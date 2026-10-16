"""Photos and videos from Home Assistant cameras for Kameraposti Turva (1.6.0).

Kameraposti publishes a capture request (customers/<id>/cameras/<camera>/capture,
QoS 1, not retained) with two signed upload URLs. A Kameraposti camera linked to
a Home Assistant camera in the options gets a still POSTed to photo_url, then --
if video is on -- an N-second recording POSTed to video_url.

The MQTT client is mocked out; uploads go through respx. PyAV (and the camera
integration's JPEG library) is not installed here, so the two camera adapters
(take a still, get the camera's stream) are patched, and FakeStream stands in for
homeassistant.components.stream.Stream: it makes the same allowlist check as
Stream.async_record and writes <path>.tmp, then <path>, as the real recorder does.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType, ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import httpx
import pytest
import respx
from freezegun.api import FrozenDateTimeFactory
from homeassistant.components.image.const import DATA_COMPONENT as IMAGE_DATA_COMPONENT
from homeassistant.const import CONF_ENTITY_ID, CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti import capture
from custom_components.kameraposti.capture import CaptureCamera, capture_cameras_from_options
from custom_components.kameraposti.const import (
    CONF_CAMERA_VIDEO,
    CONF_CAMERA_VIDEO_SECONDS,
    CONF_CAPTURE_CAMERAS,
    CONF_CAPTURE_VIDEO,
    CONF_CAPTURE_VIDEO_SECONDS,
    CONF_CUSTOMER_ID,
    CONF_EXCLUDED_CAMERAS,
    CONF_EXPORTED_CAMERAS,
    CONF_EXPORTED_ENTITIES,
    CONF_HIDDEN_CAMERAS,
    CONF_HOST,
    CONF_REMOVED_CAMERAS,
    DOMAIN,
)
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.models import CaptureRequestRejected, parse_capture_request

CUSTOMER_ID = 3
CAMERA_ID = 5
ENTITY_ID = "camera.piha"
JPEG = b"\xff\xd8\xff\xe0still-from-piha"
MP4 = b"\x00\x00\x00\x18ftypisom-ten-seconds"
# Fragments of the signed URLs that must never reach a log record.
SECRET_FRAGMENTS = ("signature=", "/turva/kamerakuva/", "cam.steels.me/turva")


def photo_url(request_id: int) -> str:
    return f"https://cam.steels.me/turva/kamerakuva/{request_id}/photo?expires=1&h=cam.steels.me&signature=p{request_id}"


def video_url(request_id: int) -> str:
    return f"https://cam.steels.me/turva/kamerakuva/{request_id}/video?expires=2&h=cam.steels.me&signature=v{request_id}"


def _topic(camera_id: int = CAMERA_ID) -> str:
    return f"customers/{CUSTOMER_ID}/cameras/{camera_id}/capture"


def _request(request_id: int = 17, camera_id: int = CAMERA_ID, **overrides: object) -> bytes:
    now = dt_util.utcnow()
    payload: dict[str, object] = {
        "request_id": request_id,
        "camera_id": camera_id,
        "reason": "alarm",
        "requested_at": now.isoformat(),
        "photo_url": photo_url(request_id),
        "photo_expires_at": (now + timedelta(seconds=120)).isoformat(),
        "video_url": video_url(request_id),
        "video_expires_at": (now + timedelta(seconds=300)).isoformat(),
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


def _link(entity_id: str = ENTITY_ID, *, video: bool = True, seconds: int = 12) -> dict[str, Any]:
    return {CONF_ENTITY_ID: entity_id, CONF_CAPTURE_VIDEO: video, CONF_CAPTURE_VIDEO_SECONDS: seconds}


class FakeStream:
    """Stands in for homeassistant.components.stream.Stream (PyAV is not installed)."""

    def __init__(self, hass: HomeAssistant, events: list[str]) -> None:
        self.hass = hass
        self.events = events
        self.records: list[tuple[str, int, int]] = []
        self.removed: list[str] = []
        self._outputs: dict[str, Any] = {}
        # What the recording does: write MP4 (default), write nothing, hang, or raise;
        # gate: wait for it before finishing.
        self.content: bytes | None = MP4
        self.hang = False
        self.error: BaseException | None = None
        self.gate: asyncio.Event | None = None
        # Stream.remove_provider stops the stream when its last output goes.
        self.stops = 0

    def outputs(self) -> MappingProxyType[str, Any]:
        return MappingProxyType(self._outputs)

    async def async_record(self, video_path: str, duration: int = 30, lookback: int = 5) -> None:
        # The same check as Stream.async_record.
        if not self.hass.config.is_allowed_path(video_path):
            raise HomeAssistantError(f"Can't write {video_path}, no access to path!")
        if "recorder" in self._outputs:
            raise HomeAssistantError(f"Stream already recording to {self._outputs['recorder'].video_path}!")
        self._outputs["recorder"] = SimpleNamespace(video_path=video_path)
        self.records.append((video_path, duration, lookback))
        request_id = Path(video_path).name.split("-")[1]
        self.events.append(f"recording {request_id}")
        # The recorder writes <path>.tmp and moves it into place when it is done.
        await self.hass.async_add_executor_job(Path(video_path + ".tmp").write_bytes, b"partial")
        if self.hang:
            await asyncio.Event().wait()
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        if self.content is not None:
            await self.hass.async_add_executor_job(Path(video_path).write_bytes, self.content)
        await self.hass.async_add_executor_job(os.remove, video_path + ".tmp")
        del self._outputs["recorder"]
        self.events.append(f"recorded {request_id}")

    async def remove_provider(self, provider: Any) -> None:
        self.removed.append(provider.video_path)
        self._outputs = {name: output for name, output in self._outputs.items() if output is not provider}
        if not self._outputs:
            self.stops += 1


@dataclass
class Harness:
    hass: HomeAssistant
    entry: MockConfigEntry
    coordinator: KameraportiCoordinator
    stream: FakeStream | None
    events: list[str]
    take_photo: AsyncMock
    camera_stream: AsyncMock
    dirs: SimpleNamespace
    sent: list[str] = field(default_factory=list)
    # 1.9.0: what each Frigate image entity holds (async_frigate_image is patched to read it).
    frigate: dict[str, bytes | None] = field(default_factory=dict)

    async def send(self, payload: bytes, topic: str | None = None) -> None:
        self.coordinator._handle_message(topic or _topic(), payload)
        await self.hass.async_block_till_done(wait_background_tasks=True)

    def capture_files(self) -> list[str]:
        """Every file left in any capture folder (config, media, www)."""
        found = []
        for root in (self.dirs.config, self.dirs.media):
            for dirpath, _, filenames in os.walk(root):
                found += [os.path.join(dirpath, name) for name in filenames]
        return found


@pytest.fixture
def mock_mqtt_client() -> AsyncGenerator[None]:
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        instance = mock_cls.return_value
        instance.async_start = AsyncMock()
        instance.async_stop = AsyncMock()
        yield


@pytest.fixture
def capture_dirs(hass: HomeAssistant, tmp_path: Path) -> SimpleNamespace:
    """A config dir and a media dir of our own, with Home Assistant's default allowlist."""
    config = tmp_path / "config"
    media = tmp_path / "media"
    (config / "www").mkdir(parents=True)
    media.mkdir()
    hass.config.config_dir = str(config)
    hass.config.media_dirs = {"local": str(media)}
    hass.config.allowlist_external_dirs = {str(config / "www"), str(media)}
    return SimpleNamespace(config=config, media=media, www=config / "www")


@pytest.fixture(autouse=True)
def no_retry_delay() -> AsyncGenerator[None]:
    with patch.object(capture, "UPLOAD_RETRY_DELAY_SECONDS", 0):
        yield


@pytest.fixture
def make_harness(
    hass: HomeAssistant, mock_mqtt_client: None, capture_dirs: SimpleNamespace
) -> AsyncGenerator[Callable[..., Any]]:
    patchers: list[Any] = []

    async def _make(
        links: dict[str, Any] | None = None,
        *,
        streaming: bool = True,
        host: str | None = "cam.steels.me",
        options: dict[str, Any] | None = None,
    ) -> Harness:
        events: list[str] = []
        stream = FakeStream(hass, events) if streaming else None

        async def _take_photo(hass_: HomeAssistant, entity_id: str) -> bytes:
            events.append(f"photo taken {entity_id}")
            return JPEG

        take_photo = AsyncMock(side_effect=_take_photo)
        camera_stream = AsyncMock(return_value=stream)
        frigate: dict[str, bytes | None] = {}
        frigate_image = AsyncMock(side_effect=lambda hass_, entity_id: frigate.get(entity_id))
        for target, mock in (
            ("async_take_photo", take_photo),
            ("async_camera_stream", camera_stream),
            ("async_frigate_image", frigate_image),
        ):
            patcher = patch.object(capture, target, mock)
            patcher.start()
            patchers.append(patcher)

        data: dict[str, Any] = {CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"}
        if host is not None:
            data[CONF_HOST] = host
        # Version 2: a version 1 entry is migrated to the default service on setup.
        entry = MockConfigEntry(
            domain=DOMAIN,
            data=data,
            options={
                CONF_CAPTURE_CAMERAS: {str(CAMERA_ID): _link()} if links is None else links,
                **(options or {}),
            },
            version=2,
        )
        entry.add_to_hass(hass)
        # 1.8.1: only a camera that exists in Home Assistant is reported (and can be asked for photos).
        hass.states.async_set(ENTITY_ID, "idle")
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        coordinator = hass.data[DOMAIN][entry.entry_id]
        return Harness(
            hass, entry, coordinator, stream, events, take_photo, camera_stream, capture_dirs, frigate=frigate
        )

    yield _make
    for patcher in patchers:
        patcher.stop()


def _routes(request_id: int = 17, *, photo: Any = 202, video: Any = 202, events: list[str] | None = None):
    """respx routes for one request's photo and video uploads."""

    def _responder(kind: str, outcome: Any) -> Callable[[httpx.Request], httpx.Response]:
        outcomes = list(outcome) if isinstance(outcome, list) else None

        def _respond(request: httpx.Request) -> httpx.Response:
            if events is not None:
                events.append(f"{kind} posted {request_id}")
            current = outcomes.pop(0) if outcomes is not None else outcome
            if isinstance(current, BaseException):
                raise current
            return httpx.Response(current)

        return _respond

    photo_route = respx.post(photo_url(request_id)).mock(side_effect=_responder("photo", photo))
    video_route = respx.post(video_url(request_id)).mock(side_effect=_responder("video", video))
    return photo_route, video_route


def _assert_no_url_logged(caplog: pytest.LogCaptureFixture) -> None:
    texts = [caplog.text]
    for record in caplog.records:
        texts.append(record.getMessage())
        texts.append(repr(record.args))
        if record.exc_info:
            texts.append(logging.Formatter().formatException(record.exc_info))
    for text in texts:
        for fragment in SECRET_FRAGMENTS:
            assert fragment not in text, text


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """The integration's own warnings (Home Assistant's loader warns about custom integrations)."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno >= logging.WARNING and r.name.startswith("custom_components.kameraposti")
    ]


async def _wait_for(condition: Callable[[], bool]) -> None:
    for _ in range(300):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


# -- the happy path ---------------------------------------------------------------


@respx.mock
async def test_photo_then_video_are_posted_with_their_types(make_harness: Callable[..., Any]) -> None:
    h = await make_harness()
    photo_route, video_route = _routes(events=h.events)

    await h.send(_request())

    assert h.events == [
        f"photo taken {ENTITY_ID}",
        "photo posted 17",
        "recording 17",
        "recorded 17",
        "video posted 17",
    ]
    h.take_photo.assert_awaited_once_with(h.hass, ENTITY_ID)
    photo = photo_route.calls.last.request
    assert photo.headers["content-type"] == "image/jpeg"
    assert photo.content == JPEG
    video = video_route.calls.last.request
    assert video.headers["content-type"] == "video/mp4"
    assert video.content == MP4
    # Recorded as the camera.record service does, for the configured length, with no lookback.
    h.camera_stream.assert_awaited_once_with(h.hass, ENTITY_ID)
    [(path, duration, lookback)] = h.stream.records
    assert (duration, lookback) == (12, 0)
    # A hidden folder in the media folder (allowlisted, served only to logged-in users) -- never www.
    assert Path(path).parent == h.dirs.media / ".kameraposti_capture"
    assert path.endswith(".mp4")
    assert h.capture_files() == []


@respx.mock
async def test_unlinked_camera_is_ignored(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    h = await make_harness()
    photo_route, video_route = _routes(request_id=18)

    await h.send(_request(request_id=18, camera_id=6), topic=_topic(6))

    h.take_photo.assert_not_awaited()
    assert not photo_route.called
    assert not video_route.called
    assert _warnings(caplog) == [
        "Kameraposti capture request_id=18 camera_id=6: no Home Assistant camera chosen for it in "
        "Kameraposti, no photo"
    ]


@respx.mock
async def test_a_request_naming_any_camera_takes_the_photo_from_it(
    make_harness: Callable[..., Any],
) -> None:
    """1.7.0: Kameraposti names the Home Assistant camera; no link by camera id is needed. 1.8.1: the
    camera need not be on any list -- not even when an older 1.7.0 list or 1.8.0 exclusion says so."""
    h = await make_harness(
        {},
        options={
            CONF_EXPORTED_CAMERAS: [ENTITY_ID],
            CONF_EXCLUDED_CAMERAS: ["camera.talli"],
            CONF_CAMERA_VIDEO: False,
            CONF_CAMERA_VIDEO_SECONDS: 10,
        },
    )
    h.hass.states.async_set("camera.talli", "idle")
    photo_route, video_route = _routes(request_id=19)

    await h.send(_request(request_id=19, camera_id=8, entity_id="camera.talli"), topic=_topic(8))

    h.take_photo.assert_awaited_once_with(h.hass, "camera.talli")
    assert photo_route.called
    assert not video_route.called


@respx.mock
async def test_only_a_hidden_or_missing_camera_is_refused_and_says_so(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    """16.10 a request ignored at debug level left steelo guessing. A camera the user never touched in
    the options works, also one added to Home Assistant after setup; a hidden one, or one that does
    not exist, never does -- nor an old server's link to a hidden camera."""
    h = await make_harness(options={CONF_HIDDEN_CAMERAS: ["camera.olohuone", ENTITY_ID]})
    h.hass.states.async_set("camera.olohuone", "idle")
    h.hass.states.async_set("camera.talli", "idle")
    for request_id in (30, 31, 32, 33, 34):
        _routes(request_id=request_id)

    with caplog.at_level(logging.WARNING):
        await h.send(_request(request_id=30, entity_id="camera.talli"))
        h.hass.states.async_set("camera.uusi", "idle")
        await h.send(_request(request_id=31, entity_id="camera.uusi"))
        await h.send(_request(request_id=32, entity_id="camera.olohuone"))
        await h.send(_request(request_id=33, entity_id="camera.ei_ole"))
        # No entity_id (server before V60): camera 5 is linked to camera.piha, which is hidden.
        await h.send(_request(request_id=34))

    assert [c.args[1] for c in h.take_photo.await_args_list] == ["camera.talli", "camera.uusi"]
    assert _warnings(caplog) == [
        "Kameraposti capture request_id=32 camera_id=5: camera.olohuone is hidden under "
        "Configure -> Hide cameras from Kameraposti (or does not exist in Home Assistant), no photo",
        "Kameraposti capture request_id=33 camera_id=5: camera.ei_ole is hidden under "
        "Configure -> Hide cameras from Kameraposti (or does not exist in Home Assistant), no photo",
        "Kameraposti capture request_id=34 camera_id=5: camera.piha is hidden under "
        "Configure -> Hide cameras from Kameraposti (or does not exist in Home Assistant), no photo",
    ]
    _assert_no_url_logged(caplog)


@respx.mock
async def test_a_fresh_entry_takes_photos_from_every_camera(make_harness: Callable[..., Any]) -> None:
    """Cameras never configured -- every camera in Home Assistant is reported."""
    h = await make_harness({})
    h.hass.states.async_set("camera.talli", "idle")
    photo_route, _ = _routes(request_id=35)

    await h.send(_request(request_id=35, entity_id="camera.talli"))

    h.take_photo.assert_awaited_once_with(h.hass, "camera.talli")
    assert photo_route.called


@respx.mock
async def test_an_invalid_entity_id_rejects_the_request(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    """Rejected while parsing (debug), not taken for an unticked camera (warning)."""
    h = await make_harness()
    with caplog.at_level(logging.WARNING):
        for request_id, entity_id in ((22, "sensor.ovi"), (23, 7), (24, "camera.Piha")):
            _routes(request_id=request_id)
            await h.send(_request(request_id=request_id, entity_id=entity_id))
    h.take_photo.assert_not_awaited()
    assert _warnings(caplog) == []


def test_the_entity_id_is_parsed_and_validated() -> None:
    parsed = parse_capture_request(
        _topic(),
        _request(entity_id="camera.talli"),
        expected_customer_id=CUSTOMER_ID,
        expected_host="cam.steels.me",
    )
    assert parsed.entity_id == "camera.talli"
    assert (
        parse_capture_request(
            _topic(), _request(), expected_customer_id=CUSTOMER_ID, expected_host="cam.steels.me"
        )
    ).entity_id is None
    for entity_id in ("sensor.ovi", 7, "camera.Piha", "camera." + "a" * 201):
        with pytest.raises(CaptureRequestRejected, match="entity_id"):
            parse_capture_request(
                _topic(),
                _request(entity_id=entity_id),
                expected_customer_id=CUSTOMER_ID,
                expected_host="cam.steels.me",
            )


@respx.mock
async def test_video_only_when_it_is_on_for_the_camera(make_harness: Callable[..., Any]) -> None:
    h = await make_harness({str(CAMERA_ID): _link(video=False)})
    photo_route, video_route = _routes()

    await h.send(_request())

    assert photo_route.call_count == 1
    h.camera_stream.assert_not_awaited()
    assert h.stream.records == []
    assert not video_route.called


@respx.mock
async def test_each_request_id_is_handled_once(make_harness: Callable[..., Any]) -> None:
    """QoS 1 is at-least-once: a redelivered request (now, or after the first is done) is ignored."""
    h = await make_harness()
    photo_route, video_route = _routes()

    h.coordinator._handle_message(_topic(), _request())
    h.coordinator._handle_message(_topic(), _request())
    await h.hass.async_block_till_done(wait_background_tasks=True)
    await h.send(_request())

    assert h.take_photo.await_count == 1
    assert photo_route.call_count == 1
    assert video_route.call_count == 1
    assert len(h.stream.records) == 1


@respx.mock
async def test_two_requests_for_one_camera_record_one_after_the_other(
    make_harness: Callable[..., Any],
) -> None:
    """A stream records to one file at a time: the second recording waits for the first
    instead of failing with "already recording"; both photos go out at once."""
    h = await make_harness()
    _routes(17, events=h.events)
    _routes(18, events=h.events)
    h.stream.gate = asyncio.Event()

    h.coordinator._handle_message(_topic(), _request(17))
    h.coordinator._handle_message(_topic(), _request(18))
    # While the first request records, the second one's photo goes out -- and its
    # recording waits.
    await _wait_for(lambda: "recording 17" in h.events and "photo posted 18" in h.events)
    assert "recording 18" not in h.events
    h.stream.gate.set()
    await h.hass.async_block_till_done(wait_background_tasks=True)

    events = h.events
    assert events.index("photo posted 17") < events.index("recording 17")
    assert events.index("photo posted 18") < events.index("recorded 17") < events.index("recording 18")
    assert events.index("recording 18") < events.index("video posted 18")
    assert len(h.stream.records) == 2
    assert all(route.call_count == 1 for route in respx.routes)
    assert h.capture_files() == []


@respx.mock
async def test_a_video_link_that_expires_while_waiting_for_the_camera_is_not_used(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    """The second request waits for the first one's recording; its video link is checked
    again once the camera is free, and nothing is recorded with an expired link."""
    h = await make_harness()
    _routes(17, events=h.events)
    photo_18, video_18 = _routes(18, events=h.events)
    h.stream.gate = asyncio.Event()

    h.coordinator._handle_message(_topic(), _request(17))
    soon = (dt_util.utcnow() + timedelta(seconds=0.5)).isoformat()
    h.coordinator._handle_message(_topic(), _request(18, video_expires_at=soon))
    await _wait_for(lambda: "recording 17" in h.events and "photo posted 18" in h.events)
    await asyncio.sleep(0.6)
    h.stream.gate.set()
    await h.hass.async_block_till_done(wait_background_tasks=True)

    assert photo_18.call_count == 1
    assert [Path(path).name.split("-")[1] for path, _, _ in h.stream.records] == ["17"]
    assert not video_18.called
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert "request_id=18" in warnings[0]
    assert "link expired" in warnings[0]
    assert h.capture_files() == []


# -- validation and expiry ------------------------------------------------------------


@respx.mock
@pytest.mark.parametrize(
    "payload",
    [
        b"{not json",
        b"",
        _request(photo_url=None),
        _request(video_expires_at=None),
        _request(photo_url=photo_url(17).replace("https:", "http:")),
        _request(video_url=video_url(17).replace("https:", "http:")),
        _request(photo_url=photo_url(17).replace("cam.steels.me/", "192.168.10.1/")),
        _request(video_url=video_url(17).replace("cam.steels.me/", "cam.steels.me:8443/")),
        _request(camera_id=6),
        _request(request_id="17"),
    ],
)
async def test_invalid_requests_are_ignored_without_logging_a_url(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture, payload: bytes
) -> None:
    caplog.set_level(logging.DEBUG)
    h = await make_harness()
    photo_route, video_route = _routes()

    await h.send(payload)

    h.take_photo.assert_not_awaited()
    assert not photo_route.called
    assert not video_route.called
    assert any("invalid Kameraposti capture request" in r.getMessage() for r in caplog.records)
    _assert_no_url_logged(caplog)


@respx.mock
@pytest.mark.parametrize(
    ("photo_expired", "video_expired"),
    [(True, False), (False, True), (True, True)],
)
async def test_an_expired_link_is_not_used(
    make_harness: Callable[..., Any],
    caplog: pytest.LogCaptureFixture,
    photo_expired: bool,
    video_expired: bool,
) -> None:
    h = await make_harness()
    photo_route, video_route = _routes()
    past = (dt_util.utcnow() - timedelta(seconds=1)).isoformat()
    overrides = {}
    if photo_expired:
        overrides["photo_expires_at"] = past
    if video_expired:
        overrides["video_expires_at"] = past

    await h.send(_request(**overrides))

    assert photo_route.called is not photo_expired
    assert (h.take_photo.await_count == 1) is not photo_expired
    assert video_route.called is not video_expired
    assert (len(h.stream.records) == 1) is not video_expired
    warnings = _warnings(caplog)
    assert len(warnings) == photo_expired + video_expired
    assert all("request_id=17" in w and "link expired" in w for w in warnings)
    assert h.capture_files() == []


# -- the response rules ----------------------------------------------------------------


@respx.mock
@pytest.mark.parametrize(
    ("responses", "calls", "delivered"),
    [
        ([202], 1, True),
        ([403], 1, False),
        ([404], 1, False),
        ([409], 1, False),
        ([413], 1, False),
        ([415], 1, False),
        ([429], 1, False),
        ([400], 1, False),
        ([302], 1, False),
        ([500, 500], 2, False),
        ([502, 502], 2, False),
        ([503, 202], 2, True),
        ([httpx.ConnectError("refused"), httpx.ConnectError("refused")], 2, False),
        ([httpx.ReadTimeout("slow"), 202], 2, True),
        ([httpx.RemoteProtocolError("dropped"), 409], 2, False),
    ],
)
async def test_photo_upload_retries_once_only_after_a_transient_failure(
    make_harness: Callable[..., Any],
    caplog: pytest.LogCaptureFixture,
    responses: list[Any],
    calls: int,
    delivered: bool,
) -> None:
    """202 = delivered; 403/404/409/413/415/429 (and other 4xx) are final; a 5xx, a
    timeout or a connection error gets one retry."""
    caplog.set_level(logging.INFO, logger="custom_components.kameraposti")
    h = await make_harness({str(CAMERA_ID): _link(video=False)})
    photo_route, _ = _routes(photo=responses)

    await h.send(_request())

    assert photo_route.call_count == calls
    lines = [r for r in caplog.records if "request_id=17" in r.getMessage()]
    if delivered:
        assert [r.levelno for r in lines] == [logging.INFO]
        assert "delivered" in lines[0].getMessage()
    elif responses[-1] == 409:
        # Another Home Assistant on the account was first: nothing to warn about.
        assert [r.levelno for r in lines] == [logging.INFO]
        assert "already" in lines[0].getMessage()
    else:
        assert [r.levelno for r in lines] == [logging.WARNING]
        assert "camera_id=5" in lines[0].getMessage()


@respx.mock
async def test_video_upload_follows_the_same_rules(make_harness: Callable[..., Any]) -> None:
    h = await make_harness()
    _, video_route = _routes(video=[503, 503])

    await h.send(_request())

    assert video_route.call_count == 2
    assert h.capture_files() == []


@respx.mock
async def test_a_redirect_is_final_and_never_followed(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    """A signed upload URL never redirects: the file is never sent to another address."""
    h = await make_harness({str(CAMERA_ID): _link(video=False)})
    photo_route = respx.post(photo_url(17)).respond(307, headers={"Location": "https://evil.example/collect"})
    elsewhere = respx.route(host="evil.example").respond(202)

    await h.send(_request())

    assert photo_route.call_count == 1
    assert not elsewhere.called
    assert any("HTTP 307" in warning for warning in _warnings(caplog))


@respx.mock
async def test_no_retry_once_the_link_has_expired(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory
) -> None:
    freezer.move_to("2026-10-15T13:00:00+00:00")
    h = await make_harness({str(CAMERA_ID): _link(video=False)})

    def _slow_failure(request: httpx.Request) -> httpx.Response:
        freezer.tick(timedelta(seconds=121))
        return httpx.Response(503)

    route = respx.post(photo_url(17)).mock(side_effect=_slow_failure)

    await h.send(_request())

    assert route.call_count == 1


# -- the video's temporary file -----------------------------------------------------------


@respx.mock
@pytest.mark.parametrize(
    "failure",
    ["none", "upload_refused", "upload_transient", "record_error", "record_unexpected", "nothing_recorded"],
)
async def test_the_temporary_video_is_always_deleted(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture, failure: str
) -> None:
    h = await make_harness()
    video: Any = {"upload_refused": 413, "upload_transient": [500, 500]}.get(failure, 202)
    photo_route, video_route = _routes(video=video)
    if failure == "record_error":
        h.stream.error = HomeAssistantError("Recording failed")
    elif failure == "record_unexpected":
        h.stream.error = OSError("disk full")
    elif failure == "nothing_recorded":
        h.stream.content = None

    await h.send(_request())

    assert photo_route.call_count == 1
    assert len(h.stream.records) == 1
    assert video_route.called is (failure in ("none", "upload_refused", "upload_transient"))
    assert h.capture_files() == []
    if failure in ("record_error", "record_unexpected", "nothing_recorded"):
        warnings = _warnings(caplog)
        assert len(warnings) == 1
        assert "request_id=17" in warnings[0]
        assert "no video" in warnings[0]


@respx.mock
async def test_a_recording_that_never_finishes_is_abandoned_and_deleted(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    """Stream.async_record waits for its first segment with no timeout: the wait is
    bounded, and the abandoned recorder is detached from the camera's stream (else
    its next recording fails with "already recording")."""
    h = await make_harness()
    photo_route, video_route = _routes()
    h.stream.hang = True

    with patch.object(capture, "recording_timeout", return_value=0.05):
        await h.send(_request())

    assert photo_route.call_count == 1
    assert not video_route.called
    [(path, _, _)] = h.stream.records
    assert h.stream.removed == [path]
    assert h.stream.outputs() == {}
    assert h.stream.stops == 1
    assert h.capture_files() == []
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert "did not finish" in warnings[0]


@respx.mock
async def test_camera_without_streaming_sends_the_photo_and_warns_once(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    h = await make_harness(streaming=False)
    photo_route, video_route = _routes()

    await h.send(_request())

    assert photo_route.call_count == 1
    assert not video_route.called
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert ENTITY_ID in warnings[0]
    assert "streaming" in warnings[0]
    assert h.capture_files() == []


@respx.mock
@pytest.mark.parametrize(
    "error",
    [
        HomeAssistantError("Camera is off"),
        OSError("unreachable"),
        aiohttp.ClientError("reset"),
        TimeoutError(),
    ],
)
async def test_a_failed_photo_still_lets_the_video_go(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture, error: Exception
) -> None:
    """Whatever the camera's own snapshot code raises, the video is still recorded and sent."""
    h = await make_harness()
    photo_route, video_route = _routes()
    h.take_photo.side_effect = error

    await h.send(_request())

    assert not photo_route.called
    assert video_route.call_count == 1
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert "no photo" in warnings[0]
    assert "request_id=17" in warnings[0]
    if not isinstance(error, HomeAssistantError):
        assert type(error).__name__ in warnings[0]
    assert h.capture_files() == []


@respx.mock
@pytest.mark.parametrize("host", ["kameraposti.fi", None])
async def test_links_to_another_host_than_the_entrys_service_are_refused(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture, host: str | None
) -> None:
    """The links must point at the service the entry connects to (None = an entry from
    before the service choice: kameraposti.fi)."""
    caplog.set_level(logging.DEBUG)
    h = await make_harness(host=host)
    photo_route, video_route = _routes()

    await h.send(_request())

    h.take_photo.assert_not_awaited()
    assert not photo_route.called
    assert not video_route.called
    assert any("is not the service host 'kameraposti.fi'" in r.getMessage() for r in caplog.records)
    _assert_no_url_logged(caplog)


@respx.mock
async def test_a_video_too_large_for_kameraposti_is_not_sent(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    h = await make_harness()
    _, video_route = _routes()
    h.stream.content = MP4 * 10

    with patch.object(capture, "VIDEO_MAX_BYTES", len(MP4) * 5):
        await h.send(_request())

    assert not video_route.called
    assert h.capture_files() == []
    assert any("larger than" in warning for warning in _warnings(caplog))


@respx.mock
async def test_config_folder_is_used_when_home_assistant_allows_it(make_harness: Callable[..., Any]) -> None:
    h = await make_harness()
    h.hass.config.allowlist_external_dirs.add(str(h.dirs.config / ".kameraposti_capture"))
    _routes()

    await h.send(_request())

    [(path, _, _)] = h.stream.records
    assert Path(path).parent == h.dirs.config / ".kameraposti_capture"
    assert h.capture_files() == []


@respx.mock
@pytest.mark.parametrize("setup", ["no_media_allowed", "media_in_www"])
async def test_no_video_without_a_private_folder_home_assistant_allows(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture, setup: str
) -> None:
    """config/www is served to anyone at /local/: a recording never goes there, even when
    it is the only folder Home Assistant allows (or a media folder lies inside it)."""
    h = await make_harness()
    if setup == "no_media_allowed":
        h.hass.config.allowlist_external_dirs = {str(h.dirs.www)}
    else:
        h.hass.config.media_dirs = {"local": str(h.dirs.www / "media")}
        h.hass.config.allowlist_external_dirs = {str(h.dirs.www)}
    photo_route, video_route = _routes()

    await h.send(_request())

    assert photo_route.call_count == 1
    assert h.stream.records == []
    assert not video_route.called
    assert h.capture_files() == []
    assert not any(h.dirs.www.rglob("*"))
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert "no video" in warnings[0]


async def test_leftovers_of_an_interrupted_run_are_swept(
    hass: HomeAssistant, capture_dirs: SimpleNamespace
) -> None:
    folder = capture_dirs.media / ".kameraposti_capture"
    folder.mkdir()
    old = folder / "capture-1-5-aaaa.mp4.tmp"
    old.write_bytes(b"x")
    os.utime(old, (time.time() - 3600, time.time() - 3600))
    fresh = folder / "capture-2-5-bbbb.mp4"
    fresh.write_bytes(b"x")
    other = folder / "notes.txt"
    other.write_bytes(b"x")
    os.utime(other, (time.time() - 3600, time.time() - 3600))

    path = await hass.async_add_executor_job(capture.video_path, hass, 3, 5)

    assert path is not None
    assert Path(path).parent == folder
    assert not old.exists()
    assert fresh.exists()
    assert other.exists()


# -- logging --------------------------------------------------------------------------


@respx.mock
async def test_the_signed_urls_never_reach_the_log(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    """Across failures of every kind, the log names request_id, the camera and the outcome only."""
    logging.getLogger("httpx").setLevel(logging.WARNING)  # as Home Assistant's bootstrap does
    caplog.set_level(logging.DEBUG)
    h = await make_harness()
    _routes(17, photo=403, video=[503, httpx.ConnectError("refused")])
    _routes(18, photo=[httpx.ReadTimeout("slow"), httpx.ReadTimeout("slow")], video=202)
    _routes(19, photo=[httpx.InvalidURL("bad"), 202], video=202)
    _routes(20, photo=202, video=202)
    _routes(21, photo=202, video=202)
    past = (dt_util.utcnow() - timedelta(seconds=1)).isoformat()

    await h.send(_request(17))
    h.stream.error = HomeAssistantError("Recording failed")
    await h.send(_request(18))
    h.stream.error = None
    await h.send(_request(19))
    await h.send(_request(20, photo_expires_at=past, video_expires_at=past))
    h.take_photo.side_effect = HomeAssistantError("Camera is off")
    await h.send(_request(21))
    await h.send(_request(22, photo_url=photo_url(22).replace("https:", "http:")))

    warnings = _warnings(caplog)
    assert len(warnings) >= 6
    for warning in warnings:
        assert "request_id=" in warning
        assert "camera_id=5" in warning
    _assert_no_url_logged(caplog)


# -- unload ----------------------------------------------------------------------------


@respx.mock
async def test_unload_cancels_a_capture_in_flight_and_deletes_its_file(
    make_harness: Callable[..., Any],
) -> None:
    h = await make_harness()
    photo_route, video_route = _routes()
    h.stream.hang = True

    h.coordinator._handle_message(_topic(), _request())
    await _wait_for(lambda: bool(h.stream.records))
    assert photo_route.call_count == 1

    assert await h.hass.config_entries.async_unload(h.entry.entry_id)
    await h.hass.async_block_till_done(wait_background_tasks=True)

    [(path, _, _)] = h.stream.records
    assert not video_route.called
    assert h.stream.removed == [path]
    assert h.capture_files() == []
    # Nothing is left running, and a late message does nothing.
    assert not h.entry._background_tasks
    h.coordinator._handle_message(_topic(), _request(18))
    await h.hass.async_block_till_done(wait_background_tasks=True)
    assert h.take_photo.await_count == 1


# -- Frigate's last recognized picture (1.9.0) -------------------------------------------
#
# steelo 2026-10-16 "otetaan frigatesta last recognize pic": Turva V63 asks for a photo on every
# motion ("motion"); a Frigate camera sends Frigate's newest last-detection image instead of a live
# still when it is fresh, and nothing at all on motion without a recognition.

FRIGATE_CAMERA = "camera.kamera1"
NOW = datetime(2026, 10, 16, 18, 0, tzinfo=UTC)
PERSON_JPEG = b"\xff\xd8\xff\xe0frigate-person"
CAR_JPEG = b"\xff\xd8\xff\xe0frigate-car"


def _frigate_camera(hass: HomeAssistant, labels: tuple[str, ...] = ("person", "car")) -> None:
    """camera.kamera1 and its image.kamera1_<label> entities on one Frigate device, as the Frigate
    integration registers them -- plus an image of another integration on the same device."""
    config_entry = MockConfigEntry(domain="frigate")
    config_entry.add_to_hass(hass)
    device = dr.async_get(hass).async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("frigate", "kamera1")}, name="Kamera1"
    )
    registry = er.async_get(hass)
    for domain, platform, object_id in (
        ("camera", "frigate", "kamera1"),
        *(("image", "frigate", f"kamera1_{label}") for label in labels),
        ("image", "other", "kamera1_muu"),
    ):
        registry.async_get_or_create(
            domain,
            platform,
            object_id,
            suggested_object_id=object_id,
            device_id=device.id,
            config_entry=config_entry,
        )
    hass.states.async_set(FRIGATE_CAMERA, "idle")


def _frigate_updated(h: Harness, label: str, when: datetime, content: bytes | None) -> None:
    """Frigate's image of `label` updated at `when` (an image entity's state is that time)."""
    h.frigate[f"image.kamera1_{label}"] = content
    h.hass.states.async_set(f"image.kamera1_{label}", when.isoformat())


def _frigate_request(request_id: int, reason: str, requested_at: datetime) -> bytes:
    return _request(
        request_id, entity_id=FRIGATE_CAMERA, reason=reason, requested_at=requested_at.isoformat()
    )


async def _frigate_harness(
    make_harness: Callable[..., Any],
    freezer: FrozenDateTimeFactory,
    labels: tuple[str, ...] = ("person", "car"),
) -> Harness:
    freezer.move_to(NOW)
    h = await make_harness({}, options={CONF_CAMERA_VIDEO: False})
    _frigate_camera(h.hass, labels)
    # Another integration's image on the same device, newer than any Frigate one: never used.
    h.frigate["image.kamera1_muu"] = b"\xff\xd8\xff\xe0not-frigate"
    h.hass.states.async_set("image.kamera1_muu", NOW.isoformat())
    return h


def _info(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and r.name.startswith("custom_components.kameraposti.capture")
    ]


@pytest.mark.parametrize("reason", ["motion", "alarm"])
@respx.mock
async def test_motion_and_alarm_send_frigates_newest_recognition(
    make_harness: Callable[..., Any],
    freezer: FrozenDateTimeFactory,
    caplog: pytest.LogCaptureFixture,
    reason: str,
) -> None:
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=2), PERSON_JPEG)
    _frigate_updated(h, "car", NOW - timedelta(seconds=1), CAR_JPEG)
    photo_route, _ = _routes(request_id=40)

    with caplog.at_level(logging.INFO, logger="custom_components.kameraposti"):
        await h.send(_frigate_request(40, reason, NOW - timedelta(seconds=3)))

    h.take_photo.assert_not_awaited()
    assert photo_route.calls.last.request.content == CAR_JPEG
    assert photo_route.calls.last.request.headers["content-type"] == "image/jpeg"
    assert "Kameraposti capture request_id=40 camera_id=5: photo from image.kamera1_car delivered" in _info(
        caplog
    )
    _assert_no_url_logged(caplog)


@respx.mock
async def test_manual_always_takes_a_live_still(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory
) -> None:
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=1), PERSON_JPEG)
    photo_route, _ = _routes(request_id=41)

    await h.send(_frigate_request(41, "manual", NOW - timedelta(seconds=3)))

    h.take_photo.assert_awaited_once_with(h.hass, FRIGATE_CAMERA)
    assert photo_route.calls.last.request.content == JPEG


@pytest.mark.parametrize(("age", "used"), [(120, True), (121, False)])
@respx.mock
async def test_a_recognition_counts_for_120_seconds(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory, age: int, used: bool
) -> None:
    """Without a fresh recognition, motion sends nothing and an alarm the live still."""
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=age), PERSON_JPEG)
    # Requested before the image was updated: no wait, the 120 s rule alone decides.
    requested_at = NOW - timedelta(seconds=age + 1)
    motion_route, _ = _routes(request_id=42)
    alarm_route, _ = _routes(request_id=43)

    await h.send(_frigate_request(42, "motion", requested_at))
    await h.send(_frigate_request(43, "alarm", requested_at))

    if used:
        h.take_photo.assert_not_awaited()
        assert motion_route.calls.last.request.content == PERSON_JPEG
        assert alarm_route.calls.last.request.content == PERSON_JPEG
    else:
        assert not motion_route.called
        h.take_photo.assert_awaited_once_with(h.hass, FRIGATE_CAMERA)
        assert alarm_route.calls.last.request.content == JPEG


@respx.mock
async def test_motion_without_a_recognition_sends_no_photo_and_says_so(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=300), PERSON_JPEG)
    photo_route, _ = _routes(request_id=44)

    with caplog.at_level(logging.INFO, logger="custom_components.kameraposti"):
        await h.send(_frigate_request(44, "motion", NOW - timedelta(seconds=301)))

    assert not photo_route.called
    h.take_photo.assert_not_awaited()
    assert (
        "Kameraposti capture request_id=44 camera_id=5: no fresh Frigate detection for camera.kamera1, "
        "no photo (motion)"
    ) in _info(caplog)
    assert _warnings(caplog) == []


@pytest.mark.parametrize("content", [b"\x89PNG\r\n\x1a\nnot-a-jpeg", None, b""])
@respx.mock
async def test_a_frigate_image_that_is_no_jpeg_is_not_used(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory, content: bytes | None
) -> None:
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=1), content)
    motion_route, _ = _routes(request_id=45)
    alarm_route, _ = _routes(request_id=46)

    await h.send(_frigate_request(45, "motion", NOW - timedelta(seconds=2)))
    await h.send(_frigate_request(46, "alarm", NOW - timedelta(seconds=2)))

    assert not motion_route.called
    h.take_photo.assert_awaited_once_with(h.hass, FRIGATE_CAMERA)
    assert alarm_route.calls.last.request.content == JPEG


@pytest.mark.parametrize("reason", ["motion", "alarm", "manual"])
@respx.mock
async def test_a_camera_without_frigate_takes_a_live_still_as_before(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory, reason: str
) -> None:
    """E.g. the Shelly: no Frigate image on its device -- 1.8.1 behaviour for every reason."""
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=1), PERSON_JPEG)
    photo_route, _ = _routes(request_id=47)

    await h.send(_request(47, entity_id=ENTITY_ID, reason=reason))

    h.take_photo.assert_awaited_once_with(h.hass, ENTITY_ID)
    assert photo_route.calls.last.request.content == JPEG


@respx.mock
async def test_a_request_before_frigates_image_waits_for_it(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory
) -> None:
    """Turva asks as soon as the sensor turns on, which can be before Frigate updates its image."""
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=30), PERSON_JPEG)
    photo_route, _ = _routes(request_id=48)

    h.coordinator._handle_message(_topic(), _frigate_request(48, "motion", NOW))
    await h.hass.async_block_till_done()
    assert not photo_route.called
    # Home Assistant refreshes an image entity's access token now and then: no new image, keep waiting.
    h.hass.states.async_set(
        "image.kamera1_person", (NOW - timedelta(seconds=30)).isoformat(), {"access_token": "uusi"}
    )
    # Let the capture task run as far as it would (an upload takes a few turns of the loop).
    for _ in range(50):
        await asyncio.sleep(0)
    assert not photo_route.called

    freezer.tick(timedelta(seconds=2))
    _frigate_updated(h, "car", dt_util.utcnow(), CAR_JPEG)
    await h.hass.async_block_till_done(wait_background_tasks=True)

    assert photo_route.calls.last.request.content == CAR_JPEG
    h.take_photo.assert_not_awaited()


@pytest.mark.parametrize(("last_age", "sent"), [(30, PERSON_JPEG), (300, None)])
@respx.mock
async def test_without_a_new_image_in_6_seconds_the_rules_decide(
    make_harness: Callable[..., Any],
    freezer: FrozenDateTimeFactory,
    last_age: int,
    sent: bytes | None,
) -> None:
    """After the wait, an image from within 120 s is still used; an older one means no photo on
    motion."""
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=last_age), PERSON_JPEG)
    photo_route, _ = _routes(request_id=49)

    h.coordinator._handle_message(_topic(), _frigate_request(49, "motion", NOW))
    await h.hass.async_block_till_done()
    freezer.tick(timedelta(seconds=5))
    async_fire_time_changed(h.hass)
    await h.hass.async_block_till_done()
    assert not photo_route.called

    freezer.tick(timedelta(seconds=1))
    async_fire_time_changed(h.hass)
    await h.hass.async_block_till_done(wait_background_tasks=True)

    if sent is None:
        assert not photo_route.called
    else:
        assert photo_route.calls.last.request.content == sent
    h.take_photo.assert_not_awaited()


@respx.mock
async def test_unload_cancels_a_capture_waiting_for_frigate(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory
) -> None:
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=30), PERSON_JPEG)
    photo_route, _ = _routes(request_id=50)

    h.coordinator._handle_message(_topic(), _frigate_request(50, "alarm", NOW))
    await h.hass.async_block_till_done()
    assert await h.hass.config_entries.async_unload(h.entry.entry_id)
    await h.hass.async_block_till_done(wait_background_tasks=True)
    freezer.tick(timedelta(seconds=10))
    async_fire_time_changed(h.hass)
    await h.hass.async_block_till_done()

    assert not photo_route.called
    h.take_photo.assert_not_awaited()
    assert not h.entry._background_tasks


@pytest.mark.parametrize(("reason", "video"), [("motion", False), ("alarm", True), ("manual", True)])
@respx.mock
async def test_motion_never_records_a_video(
    make_harness: Callable[..., Any], caplog: pytest.LogCaptureFixture, reason: str, video: bool
) -> None:
    """Turva's video link for a motion has already expired: no recording, and no "link expired"
    warning on every motion. Alarm and manual keep their video."""
    h = await make_harness()
    photo_route, video_route = _routes(request_id=60)
    overrides: dict[str, object] = {"reason": reason}
    if reason == "motion":
        overrides["video_expires_at"] = (dt_util.utcnow() - timedelta(seconds=1)).isoformat()

    await h.send(_request(60, **overrides))

    assert photo_route.called
    assert video_route.called is video
    assert len(h.stream.records) == (1 if video else 0)
    assert h.camera_stream.await_count == (1 if video else 0)
    assert _warnings(caplog) == []


@respx.mock
async def test_motion_sends_one_frigate_recognition_once(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """Two motions with no new recognition in between send the picture once; a new one is sent."""
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=1), PERSON_JPEG)
    first, _ = _routes(request_id=61)
    second, _ = _routes(request_id=62)
    third, _ = _routes(request_id=63)

    with caplog.at_level(logging.INFO, logger="custom_components.kameraposti"):
        await h.send(_frigate_request(61, "motion", NOW - timedelta(seconds=2)))
        await h.send(_frigate_request(62, "motion", NOW - timedelta(seconds=2)))
        _frigate_updated(h, "car", NOW, CAR_JPEG)
        await h.send(_frigate_request(63, "motion", NOW - timedelta(seconds=2)))

    assert first.calls.last.request.content == PERSON_JPEG
    assert not second.called
    assert third.calls.last.request.content == CAR_JPEG
    assert (
        "Kameraposti capture request_id=62 camera_id=5: Frigate detection already sent "
        "(image.kamera1_person), no photo (motion)"
    ) in _info(caplog)
    h.take_photo.assert_not_awaited()


@respx.mock
async def test_an_alarm_sends_a_recognition_already_sent(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory
) -> None:
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=1), PERSON_JPEG)
    motion, _ = _routes(request_id=64)
    alarm, _ = _routes(request_id=65)
    after, _ = _routes(request_id=66)

    await h.send(_frigate_request(64, "motion", NOW - timedelta(seconds=2)))
    await h.send(_frigate_request(65, "alarm", NOW - timedelta(seconds=2)))
    await h.send(_frigate_request(66, "motion", NOW - timedelta(seconds=2)))

    assert motion.calls.last.request.content == PERSON_JPEG
    assert alarm.calls.last.request.content == PERSON_JPEG
    assert not after.called


@pytest.mark.parametrize("refusal", [403, 500])
@respx.mock
async def test_a_recognition_not_delivered_is_tried_again_on_the_next_motion(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory, refusal: int
) -> None:
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=1), PERSON_JPEG)
    failed, _ = _routes(request_id=67, photo=refusal)
    again, _ = _routes(request_id=68)

    await h.send(_frigate_request(67, "motion", NOW - timedelta(seconds=2)))
    await h.send(_frigate_request(68, "motion", NOW - timedelta(seconds=2)))

    assert failed.called
    assert again.calls.last.request.content == PERSON_JPEG


@respx.mock
async def test_two_motions_waiting_for_the_same_recognition_send_it_once(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory
) -> None:
    """Two sensors linked to one camera turn on together: both requests wait for Frigate's picture."""
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=30), PERSON_JPEG)

    async def _slow_upload(request: httpx.Request) -> httpx.Response:
        # An upload takes a while: the other request runs meanwhile.
        for _ in range(5):
            await asyncio.sleep(0)
        return httpx.Response(202)

    one = respx.post(photo_url(69)).mock(side_effect=_slow_upload)
    other = respx.post(photo_url(70)).mock(side_effect=_slow_upload)

    h.coordinator._handle_message(_topic(), _frigate_request(69, "motion", NOW))
    h.coordinator._handle_message(_topic(), _frigate_request(70, "motion", NOW))
    await h.hass.async_block_till_done()
    freezer.tick(timedelta(seconds=1))
    _frigate_updated(h, "car", dt_util.utcnow(), CAR_JPEG)
    await h.hass.async_block_till_done(wait_background_tasks=True)

    assert one.call_count + other.call_count == 1
    assert [route.calls.last.request.content for route in (one, other) if route.called] == [CAR_JPEG]


@respx.mock
async def test_each_camera_remembers_its_own_recognition(
    make_harness: Callable[..., Any], freezer: FrozenDateTimeFactory
) -> None:
    """The same update time on another camera's Frigate picture is another picture."""
    h = await _frigate_harness(make_harness, freezer)
    _frigate_updated(h, "person", NOW - timedelta(seconds=1), PERSON_JPEG)
    config_entry = MockConfigEntry(domain="frigate")
    config_entry.add_to_hass(h.hass)
    device = dr.async_get(h.hass).async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("frigate", "kamera2")}, name="Kamera2"
    )
    for domain, object_id in (("camera", "kamera2"), ("image", "kamera2_person")):
        er.async_get(h.hass).async_get_or_create(
            domain, "frigate", object_id, suggested_object_id=object_id, device_id=device.id
        )
    h.hass.states.async_set("camera.kamera2", "idle")
    h.frigate["image.kamera2_person"] = CAR_JPEG
    h.hass.states.async_set("image.kamera2_person", (NOW - timedelta(seconds=1)).isoformat())
    first, _ = _routes(request_id=71)
    second, _ = _routes(request_id=72)

    await h.send(_frigate_request(71, "motion", NOW - timedelta(seconds=2)))
    await h.send(
        _request(
            72,
            entity_id="camera.kamera2",
            reason="motion",
            requested_at=(NOW - timedelta(seconds=2)).isoformat(),
        )
    )

    assert first.calls.last.request.content == PERSON_JPEG
    assert second.calls.last.request.content == CAR_JPEG


async def test_frigate_images_are_read_from_the_image_integration(hass: HomeAssistant) -> None:
    """ImageEntity.async_image through the image integration's EntityComponent (hass.data["image"])."""
    entity = SimpleNamespace(async_image=AsyncMock(return_value=PERSON_JPEG))
    hass.data["image"] = SimpleNamespace(
        get_entity=lambda entity_id: entity if entity_id.endswith("n") else None
    )

    assert await capture.async_frigate_image(hass, "image.kamera1_person") == PERSON_JPEG
    assert await capture.async_frigate_image(hass, "image.kamera1_car") is None
    del hass.data["image"]
    assert await capture.async_frigate_image(hass, "image.kamera1_person") is None


async def test_the_image_integration_keeps_its_entities_under_image(
    hass: HomeAssistant, make_harness: Callable[..., Any]
) -> None:
    """The key async_frigate_image reads is the image integration's own (a HassKey equal to "image")."""
    await make_harness()
    assert hass.data.get("image") is hass.data[IMAGE_DATA_COMPONENT]


def test_frigate_images_are_only_frigates_on_the_same_device(hass: HomeAssistant) -> None:
    _frigate_camera(hass)
    hass.states.async_set("camera.muu", "idle")
    assert sorted(capture.frigate_images(hass, FRIGATE_CAMERA)) == [
        "image.kamera1_car",
        "image.kamera1_person",
    ]
    assert capture.frigate_images(hass, "camera.muu") == []


def test_the_newest_frigate_image_wins_and_junk_states_are_skipped(hass: HomeAssistant) -> None:
    hass.states.async_set("image.a", (NOW - timedelta(seconds=5)).isoformat())
    hass.states.async_set("image.b", NOW.isoformat())
    hass.states.async_set("image.c", "unknown")
    hass.states.async_set("image.d", "2026-10-16T18:00:01")  # no time zone
    assert capture.newest_frigate_image(hass, ["image.a", "image.b", "image.c", "image.d", "image.e"]) == (
        "image.b",
        NOW,
    )
    assert capture.newest_frigate_image(hass, ["image.c", "image.e"]) is None


# -- options and the camera adapters ---------------------------------------------------


def test_linked_cameras_are_read_from_the_options_and_junk_is_skipped() -> None:
    options = {
        CONF_CAPTURE_CAMERAS: {
            "5": _link(seconds=12),
            "6": {CONF_ENTITY_ID: "camera.ovi"},
            "7": _link("camera.talli", seconds=99),
            "8": _link("camera.aitta", seconds=1),
            "9": _link("light.not_a_camera"),
            "x": _link(),
            "10": "camera.not_a_dict",
            "11": {
                CONF_ENTITY_ID: "camera.halli",
                CONF_CAPTURE_VIDEO: "yes",
                CONF_CAPTURE_VIDEO_SECONDS: 7.0,
            },
        }
    }

    assert capture_cameras_from_options(options) == {
        5: CaptureCamera(ENTITY_ID, video=True, video_seconds=12),
        6: CaptureCamera("camera.ovi", video=False, video_seconds=10),
        7: CaptureCamera("camera.talli", video=True, video_seconds=30),
        8: CaptureCamera("camera.aitta", video=True, video_seconds=5),
        11: CaptureCamera("camera.halli", video=False, video_seconds=7),
    }
    assert capture_cameras_from_options({}) == {}
    assert capture_cameras_from_options({CONF_CAPTURE_CAMERAS: ["camera.x"]}) == {}


async def test_camera_adapters_use_home_assistants_camera_api(hass: HomeAssistant) -> None:
    """The still comes from camera.async_get_image with a 10 s timeout, the stream from the
    camera entity's own async_create_stream -- the same as the camera.record service."""
    camera_module = ModuleType("homeassistant.components.camera")
    camera_module.async_get_image = AsyncMock(
        return_value=SimpleNamespace(content=JPEG, content_type="image/jpeg")
    )
    helper_module = ModuleType("homeassistant.components.camera.helper")
    stream = object()
    entity = MagicMock()
    entity.async_create_stream = AsyncMock(return_value=stream)
    helper_module.get_camera_from_entity_id = MagicMock(return_value=entity)
    camera_module.helper = helper_module

    hass.config.components.add("camera")
    with patch.dict(
        sys.modules,
        {
            "homeassistant.components.camera": camera_module,
            "homeassistant.components.camera.helper": helper_module,
        },
    ):
        assert await capture.async_take_photo(hass, ENTITY_ID) == JPEG
        assert await capture.async_camera_stream(hass, ENTITY_ID) is stream

    camera_module.async_get_image.assert_awaited_once_with(hass, ENTITY_ID, timeout=10)
    helper_module.get_camera_from_entity_id.assert_called_once_with(hass, ENTITY_ID)
    entity.async_create_stream.assert_awaited_once_with()


@pytest.mark.parametrize(
    ("entity", "error"), [("on", None), ("off", "Camera is off"), (None, "Camera not found")]
)
async def test_camera_stream_without_the_camera_helper_module(
    hass: HomeAssistant, entity: str | None, error: str | None
) -> None:
    """Home Assistant versions without camera.helper (hacs.json allows 2024.12): the camera
    entity is looked up in the camera component directly, as the helper does."""
    stream = object()
    camera_entity = SimpleNamespace(is_on=entity == "on", async_create_stream=AsyncMock(return_value=stream))
    hass.data["camera"] = SimpleNamespace(get_entity=lambda entity_id: camera_entity if entity else None)
    hass.config.components.add("camera")
    camera_module = ModuleType("homeassistant.components.camera")

    with patch.dict(
        sys.modules,
        {"homeassistant.components.camera": camera_module, "homeassistant.components.camera.helper": None},
    ):
        if error is None:
            assert await capture.async_camera_stream(hass, ENTITY_ID) is stream
        else:
            with pytest.raises(HomeAssistantError, match=error):
                await capture.async_camera_stream(hass, ENTITY_ID)


async def test_camera_adapters_need_the_camera_integration(hass: HomeAssistant) -> None:
    """Without Home Assistant's camera integration there is no camera to ask (and nothing is
    imported on the event loop): the capture gets one clear error instead."""
    assert "camera" not in hass.config.components
    with patch.dict(sys.modules, {"homeassistant.components.camera": None}):
        with pytest.raises(HomeAssistantError, match="camera integration"):
            await capture.async_take_photo(hass, ENTITY_ID)
        with pytest.raises(HomeAssistantError, match="camera integration"):
            await capture.async_camera_stream(hass, ENTITY_ID)


# -- the options flow ------------------------------------------------------------------


def _roster(*cameras: tuple[int, str]) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "generated_at": "2026-10-15T12:00:00Z",
            "cameras": [{"camera_id": camera_id, "name": name} for camera_id, name in cameras],
        }
    ).encode()


async def _open_camera_options(hass: HomeAssistant, entry: MockConfigEntry) -> dict:
    menu = await hass.config_entries.options.async_init(entry.entry_id)
    assert menu["type"] is FlowResultType.MENU
    assert set(menu["menu_options"]) == {"sensors", "cameras"}
    return await hass.config_entries.options.async_configure(menu["flow_id"], {"next_step_id": "cameras"})


def _defaults(form: dict) -> dict[str, object]:
    return {key.schema: key.default() for key in form["data_schema"].schema}


def _entry_with(hass: HomeAssistant, options: dict[str, Any]) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
        options=options,
    )
    entry.add_to_hass(hass)
    return entry


def _cameras_in_home_assistant(hass: HomeAssistant, *object_ids: str) -> None:
    for object_id in object_ids:
        hass.states.async_set(f"camera.{object_id}", "idle")


async def test_options_flow_hides_cameras_with_one_video_setting(
    hass: HomeAssistant, mock_mqtt_client: None
) -> None:
    """1.8.1 (steelo 2026-10-16 "maalaisjärjellä, pari klikkausta"): every camera is reported and
    chosen in Kameraposti; the form only hides cameras. A hidden camera is described as removed."""
    _cameras_in_home_assistant(hass, "piha", "talli")
    entry = _entry_with(hass, {CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"]})
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    handler = hass.data[DOMAIN][entry.entry_id]._capture
    assert tuple(handler._exported()) == (ENTITY_ID, "camera.talli")

    form = await _open_camera_options(hass, entry)
    assert form["type"] is FlowResultType.FORM
    assert form["step_id"] == "cameras"
    assert form["description_placeholders"] == {"max": "50"}
    assert _defaults(form) == {
        CONF_HIDDEN_CAMERAS: [],
        CONF_CAMERA_VIDEO: False,
        CONF_CAMERA_VIDEO_SECONDS: 10,
    }

    saved = await hass.config_entries.options.async_configure(
        form["flow_id"],
        {
            CONF_HIDDEN_CAMERAS: ["camera.talli"],
            CONF_CAMERA_VIDEO: True,
            CONF_CAMERA_VIDEO_SECONDS: 15,
        },
    )
    await hass.async_block_till_done()

    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {
        CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"],
        CONF_HIDDEN_CAMERAS: ["camera.talli"],
        CONF_REMOVED_CAMERAS: ["camera.talli"],
        CONF_CAMERA_VIDEO: True,
        CONF_CAMERA_VIDEO_SECONDS: 15,
    }
    # Saving reloads the entry: the new coordinator takes photos from every camera but the hidden one.
    handler = hass.data[DOMAIN][entry.entry_id]._capture
    assert tuple(handler._exported()) == (ENTITY_ID,)
    assert (handler._video, handler._video_seconds) == (True, 15)

    form = await _open_camera_options(hass, entry)
    assert _defaults(form) == {
        CONF_HIDDEN_CAMERAS: ["camera.talli"],
        CONF_CAMERA_VIDEO: True,
        CONF_CAMERA_VIDEO_SECONDS: 15,
    }
    # Shown again: no longer on the removal list.
    await hass.config_entries.options.async_configure(form["flow_id"], {CONF_HIDDEN_CAMERAS: []})
    await hass.async_block_till_done()
    assert entry.options[CONF_HIDDEN_CAMERAS] == []
    assert entry.options[CONF_REMOVED_CAMERAS] == []


@pytest.mark.parametrize(
    "old",
    [
        # 1.8.0 with a camera on its "must not use" list (as steelo's Shelly 16.10).
        {CONF_EXCLUDED_CAMERAS: ["camera.olohuone"], CONF_CAMERA_VIDEO: True, CONF_CAMERA_VIDEO_SECONDS: 12},
        # 1.7.0 ticks.
        {CONF_EXPORTED_CAMERAS: [ENTITY_ID], CONF_CAMERA_VIDEO: True, CONF_CAMERA_VIDEO_SECONDS: 12},
        # 1.6.0 links.
        {CONF_CAPTURE_CAMERAS: {"5": _link(), "6": _link("camera.talli", video=False)}},
    ],
)
async def test_upgrade_reports_every_camera_and_the_form_opens_empty(
    hass: HomeAssistant, mock_mqtt_client: None, old: dict[str, Any]
) -> None:
    """1.8.1: nothing an earlier version saved hides a camera, and the hide list is never pre-filled
    from it. Saving drops the old keys; the 1.6.0 links stay for their camera_id hints."""
    _cameras_in_home_assistant(hass, "piha", "talli", "olohuone")
    entry = _entry_with(hass, old)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    every = ("camera.olohuone", ENTITY_ID, "camera.talli")
    assert tuple(hass.data[DOMAIN][entry.entry_id]._capture._exported()) == every

    form = await _open_camera_options(hass, entry)
    assert _defaults(form) == {
        CONF_HIDDEN_CAMERAS: [],
        CONF_CAMERA_VIDEO: True,
        CONF_CAMERA_VIDEO_SECONDS: 12,
    }
    await hass.config_entries.options.async_configure(
        form["flow_id"], {CONF_HIDDEN_CAMERAS: [], CONF_CAMERA_VIDEO: True}
    )
    await hass.async_block_till_done()

    assert CONF_EXCLUDED_CAMERAS not in entry.options
    assert CONF_EXPORTED_CAMERAS not in entry.options
    assert entry.options[CONF_HIDDEN_CAMERAS] == []
    assert entry.options[CONF_REMOVED_CAMERAS] == []
    assert entry.options.get(CONF_CAPTURE_CAMERAS) == old.get(CONF_CAPTURE_CAMERAS)
    assert tuple(hass.data[DOMAIN][entry.entry_id]._capture._exported()) == every


async def test_an_old_removal_of_a_camera_that_is_reported_is_never_sent(
    hass: HomeAssistant, mock_mqtt_client: None
) -> None:
    """A 1.7.0/1.8.0 removal for a camera that 1.8.1 reports would contradict its description."""
    _cameras_in_home_assistant(hass, "piha")
    entry = _entry_with(hass, {CONF_EXPORTED_CAMERAS: [], CONF_REMOVED_CAMERAS: [ENTITY_ID]})
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    coordinator = hass.data[DOMAIN][entry.entry_id]
    sent: list[dict[str, Any]] = []
    coordinator._camera_exporter._publish = lambda topic, payload, retain: (
        sent.append(json.loads(payload)) or True
    )
    coordinator._camera_exporter.publish_all()
    assert sent == [{"kind": "camera", "entity_id": ENTITY_ID, "name": "piha", "mac": None}]


async def test_options_flow_refuses_a_video_length_out_of_range_and_a_non_camera(
    hass: HomeAssistant, mock_mqtt_client: None
) -> None:
    entry = _entry_with(hass, {CONF_CAPTURE_CAMERAS: {"5": _link()}})
    form = await _open_camera_options(hass, entry)

    for seconds in (4, 31):
        with pytest.raises(Exception):  # noqa: B017 - the selector's own vol.Invalid / InvalidData
            await hass.config_entries.options.async_configure(
                form["flow_id"],
                {
                    CONF_HIDDEN_CAMERAS: [],
                    CONF_CAMERA_VIDEO: True,
                    CONF_CAMERA_VIDEO_SECONDS: seconds,
                },
            )
    # Kameraposti accepts object ids of at most 200 characters (Home Assistant allows longer).
    result = await hass.config_entries.options.async_configure(
        form["flow_id"],
        {CONF_HIDDEN_CAMERAS: ["camera." + "a" * 201], CONF_CAMERA_VIDEO: False},
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_HIDDEN_CAMERAS: "invalid_camera"}
    assert entry.options == {CONF_CAPTURE_CAMERAS: {"5": _link()}}

    # Hiding more cameras than can be reported is fine: there is no limit on hiding.
    many = [f"camera.c{i}" for i in range(60)]
    saved = await hass.config_entries.options.async_configure(
        form["flow_id"], {CONF_HIDDEN_CAMERAS: many, CONF_CAMERA_VIDEO: False}
    )
    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_HIDDEN_CAMERAS] == many


async def test_saving_the_sensors_keeps_the_camera_links(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
        options={CONF_CAPTURE_CAMERAS: {"5": _link()}},
    )
    entry.add_to_hass(hass)
    menu = await hass.config_entries.options.async_init(entry.entry_id)
    form = await hass.config_entries.options.async_configure(menu["flow_id"], {"next_step_id": "sensors"})

    saved = await hass.config_entries.options.async_configure(form["flow_id"], {CONF_EXPORTED_ENTITIES: []})

    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {CONF_EXPORTED_ENTITIES: [], CONF_CAPTURE_CAMERAS: {"5": _link()}}
