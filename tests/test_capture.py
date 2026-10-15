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
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType, ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx
from freezegun.api import FrozenDateTimeFactory
from homeassistant.const import CONF_ENTITY_ID, CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti import capture
from custom_components.kameraposti.capture import CaptureCamera, capture_cameras_from_options
from custom_components.kameraposti.const import (
    CONF_CAPTURE_CAMERAS,
    CONF_CAPTURE_VIDEO,
    CONF_CAPTURE_VIDEO_SECONDS,
    CONF_CUSTOMER_ID,
    CONF_EXPORTED_ENTITIES,
    DOMAIN,
)
from custom_components.kameraposti.coordinator import KameraportiCoordinator

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
        # What the recording does: write MP4 (default), write nothing, hang, or raise.
        self.content: bytes | None = MP4
        self.hang = False
        self.error: BaseException | None = None

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
        self.events.append("recording")
        # The recorder writes <path>.tmp and moves it into place when it is done.
        await self.hass.async_add_executor_job(Path(video_path + ".tmp").write_bytes, b"partial")
        if self.hang:
            await asyncio.Event().wait()
        if self.error is not None:
            raise self.error
        if self.content is not None:
            await self.hass.async_add_executor_job(Path(video_path).write_bytes, self.content)
        await self.hass.async_add_executor_job(os.remove, video_path + ".tmp")
        del self._outputs["recorder"]

    async def remove_provider(self, provider: Any) -> None:
        self.removed.append(provider.video_path)
        self._outputs = {name: output for name, output in self._outputs.items() if output is not provider}


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

    async def _make(links: dict[str, Any] | None = None, *, streaming: bool = True) -> Harness:
        events: list[str] = []
        stream = FakeStream(hass, events) if streaming else None

        async def _take_photo(hass_: HomeAssistant, entity_id: str) -> bytes:
            events.append(f"photo taken {entity_id}")
            return JPEG

        take_photo = AsyncMock(side_effect=_take_photo)
        camera_stream = AsyncMock(return_value=stream)
        for target, mock in (("async_take_photo", take_photo), ("async_camera_stream", camera_stream)):
            patcher = patch.object(capture, target, mock)
            patcher.start()
            patchers.append(patcher)

        entry = MockConfigEntry(
            domain=DOMAIN,
            data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
            options={CONF_CAPTURE_CAMERAS: {str(CAMERA_ID): _link()} if links is None else links},
        )
        entry.add_to_hass(hass)
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        coordinator = hass.data[DOMAIN][entry.entry_id]
        return Harness(hass, entry, coordinator, stream, events, take_photo, camera_stream, capture_dirs)

    yield _make
    for patcher in patchers:
        patcher.stop()


def _routes(request_id: int = 17, *, photo: Any = 202, video: Any = 202, events: list[str] | None = None):
    """respx routes for one request's photo and video uploads."""

    def _responder(kind: str, outcome: Any) -> Callable[[httpx.Request], httpx.Response]:
        outcomes = list(outcome) if isinstance(outcome, list) else None

        def _respond(request: httpx.Request) -> httpx.Response:
            if events is not None:
                events.append(f"{kind} posted")
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


# -- the happy path ---------------------------------------------------------------


@respx.mock
async def test_photo_then_video_are_posted_with_their_types(make_harness: Callable[..., Any]) -> None:
    h = await make_harness()
    photo_route, video_route = _routes(events=h.events)

    await h.send(_request())

    assert h.events == [f"photo taken {ENTITY_ID}", "photo posted", "recording", "video posted"]
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
async def test_unlinked_camera_is_ignored(make_harness: Callable[..., Any]) -> None:
    h = await make_harness()
    photo_route, video_route = _routes(request_id=18)

    await h.send(_request(request_id=18, camera_id=6), topic=_topic(6))

    h.take_photo.assert_not_awaited()
    assert not photo_route.called
    assert not video_route.called


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
    _routes(17)
    _routes(18)

    h.coordinator._handle_message(_topic(), _request(17))
    h.coordinator._handle_message(_topic(), _request(18))
    await h.hass.async_block_till_done(wait_background_tasks=True)

    assert len(h.stream.records) == 2
    assert all(route.call_count == 1 for route in respx.routes)
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
    for _ in range(50):
        await asyncio.sleep(0.01)
        if h.stream.records:
            break
    assert h.stream.records, "the recording should have started"
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


async def test_options_flow_links_a_kameraposti_camera_to_a_home_assistant_camera(
    hass: HomeAssistant, mock_mqtt_client: None
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
        options={CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"]},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    hass.data[DOMAIN][entry.entry_id]._handle_message(
        f"customers/{CUSTOMER_ID}/cameras", _roster((CAMERA_ID, "Piha"), (6, "Talli"))
    )
    await hass.async_block_till_done()

    choose = await _open_camera_options(hass, entry)
    assert choose["type"] is FlowResultType.FORM
    assert choose["step_id"] == "cameras"
    [field_] = choose["data_schema"].schema
    offered = {
        option["value"]: option["label"] for option in choose["data_schema"].schema[field_].config["options"]
    }
    assert offered == {"5": "Piha", "6": "Talli"}

    form = await hass.config_entries.options.async_configure(choose["flow_id"], {"camera": "5"})
    assert form["type"] is FlowResultType.FORM
    assert form["step_id"] == "camera"
    assert form["description_placeholders"] == {"camera": "Piha"}
    saved = await hass.config_entries.options.async_configure(
        form["flow_id"], {CONF_ENTITY_ID: ENTITY_ID, CONF_CAPTURE_VIDEO: True, CONF_CAPTURE_VIDEO_SECONDS: 15}
    )
    await hass.async_block_till_done()

    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {
        CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"],
        CONF_CAPTURE_CAMERAS: {
            "5": {CONF_ENTITY_ID: ENTITY_ID, CONF_CAPTURE_VIDEO: True, CONF_CAPTURE_VIDEO_SECONDS: 15}
        },
    }
    # Saving reloads the entry, and the new coordinator answers for the linked camera.
    coordinator = hass.data[DOMAIN][entry.entry_id]
    assert coordinator._capture._cameras == {5: CaptureCamera(ENTITY_ID, video=True, video_seconds=15)}

    # The list shows the link (once the reloaded entry has the retained roster again);
    # the form opens with it.
    coordinator._handle_message(
        f"customers/{CUSTOMER_ID}/cameras", _roster((CAMERA_ID, "Piha"), (6, "Talli"))
    )
    await hass.async_block_till_done()
    choose = await _open_camera_options(hass, entry)
    [field_] = choose["data_schema"].schema
    labels = [option["label"] for option in choose["data_schema"].schema[field_].config["options"]]
    assert labels == [f"Piha → {ENTITY_ID} · video 15 s", "Talli"]
    form = await hass.config_entries.options.async_configure(choose["flow_id"], {"camera": "5"})
    defaults = {str(key): key.description for key in form["data_schema"].schema}
    assert defaults[CONF_ENTITY_ID] == {"suggested_value": ENTITY_ID}
    assert {
        key.schema: key.default() for key in form["data_schema"].schema if key.schema != CONF_ENTITY_ID
    } == {
        CONF_CAPTURE_VIDEO: True,
        CONF_CAPTURE_VIDEO_SECONDS: 15,
    }


async def test_options_flow_unlinks_a_camera_and_keeps_the_others(
    hass: HomeAssistant, mock_mqtt_client: None
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
        options={CONF_CAPTURE_CAMERAS: {"5": _link(), "6": _link("camera.talli", video=False)}},
    )
    entry.add_to_hass(hass)
    # Not set up: the cameras linked earlier are still listed (no roster to show).
    choose = await _open_camera_options(hass, entry)
    [field_] = choose["data_schema"].schema
    offered = [option["value"] for option in choose["data_schema"].schema[field_].config["options"]]
    assert offered == ["5", "6"]

    form = await hass.config_entries.options.async_configure(choose["flow_id"], {"camera": "5"})
    saved = await hass.config_entries.options.async_configure(
        form["flow_id"], {CONF_CAPTURE_VIDEO: True, CONF_CAPTURE_VIDEO_SECONDS: 10}
    )

    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_CAPTURE_CAMERAS] == {"6": _link("camera.talli", video=False)}


async def test_options_flow_refuses_a_video_length_out_of_range(
    hass: HomeAssistant, mock_mqtt_client: None
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
        options={CONF_CAPTURE_CAMERAS: {"5": _link()}},
    )
    entry.add_to_hass(hass)
    choose = await _open_camera_options(hass, entry)
    form = await hass.config_entries.options.async_configure(choose["flow_id"], {"camera": "5"})

    for seconds in (4, 31):
        with pytest.raises(Exception):  # noqa: B017 - the selector's own vol.Invalid / InvalidData
            await hass.config_entries.options.async_configure(
                form["flow_id"],
                {CONF_ENTITY_ID: ENTITY_ID, CONF_CAPTURE_VIDEO: True, CONF_CAPTURE_VIDEO_SECONDS: seconds},
            )
    assert entry.options[CONF_CAPTURE_CAMERAS] == {"5": _link()}


async def test_options_flow_without_cameras_says_so(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
    )
    entry.add_to_hass(hass)

    result = await _open_camera_options(hass, entry)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "no_cameras"


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
