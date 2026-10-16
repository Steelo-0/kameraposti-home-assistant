"""Photos and videos from Home Assistant cameras for Kameraposti Turva (1.6.0).

Kameraposti Turva asks for a photo when a sensor linked to one of its cameras
alarms, or when the user presses "Ota kuva Home Assistantista" on the camera's
page: it publishes a capture request (customers/<id>/cameras/<camera>/capture,
QoS 1, not retained) with two signed, expiring upload URLs. 1.7.0: the request
names the Home Assistant camera (entity_id), which must be one the integration
reports to Kameraposti (camera_export.py; 1.8.1: every camera not hidden under
"Hide cameras from Kameraposti", worked out when the request arrives); a request from an
older server names none, and the 1.6.0 option linking its Kameraposti camera
decides (its Home Assistant camera must be reported too). For that camera this module

1. takes a still with camera.async_get_image (10 s) and POSTs it to photo_url
   as image/jpeg;
2. then, if video is on for that camera, records N seconds (5-30, default 10)
   with Home Assistant's stream integration -- the same mechanism as the
   camera.record service: camera.async_create_stream() and
   stream.async_record() -- and POSTs the MP4 to video_url as video/mp4.

1.9.0 (steelo 2026-10-16 "otetaan frigatesta last recognize pic"): Turva V63 asks
for a photo on every motion of a sensor linked to the camera (reason "motion"), and
a live still on each would be noise. For a "motion" or "alarm" request, a camera
whose device has Frigate's last-detection image entities (image.<camera>_<object>,
platform frigate) sends the newest of those instead -- if it was updated within
FRIGATE_DETECTION_MAX_AGE_SECONDS and is a JPEG. The request comes as soon as the
sensor turns on, possibly before Frigate's image: when the newest image is older
than requested_at, the capture waits up to FRIGATE_WAIT_SECONDS for one of them to
update. Without a fresh recognition a motion request sends no photo, and an alarm
the live still (an alarm always brings a picture). A camera without Frigate images,
and a "manual" request, always send the live still, as before.

Both URLs must point at the service the entry connects to (its host, default
port, no userinfo), or the request is refused. The photo always goes first;
the video never delays it, and a failed photo does not stop it. Each
request_id is handled once. 202 (any 2xx) = delivered; 403/404/409/413/415/429 and any other
4xx are final; a 5xx, a timeout or a connection error gets one retry, and only
while the URL is still valid.

The URLs are signed capabilities: they are never logged, not even on errors.
The log gets request_id, the camera and the outcome only, and an httpx error by
its type name only (its message can carry the URL).

The recording goes to a temporary file in a folder Home Assistant allows
(stream.async_record checks hass.config.is_allowed_path itself, and so does
video_path() before creating anything) that is not served without logging in:
<config>/.kameraposti_capture if the allowlist permits it, else
.kameraposti_capture in a media folder (served only to logged-in users, and
hidden from the media browser as a dot folder). Never under config/www, which
is served to anyone at /local/. The file is deleted when the request is done,
whatever happened, and leftovers of an interrupted run are swept.

The work runs in tasks tied to the config entry: the MQTT thread and the event
loop never wait for it (file access goes to the executor), and unloading
cancels what is in flight.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ENTITY_ID
from homeassistant.core import Event, EventStateChangedData, HassJob, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_call_later, async_track_state_change_event
from homeassistant.helpers.httpx_client import get_async_client
from homeassistant.util import dt as dt_util

from .const import (
    CAPTURE_VIDEO_SECONDS_DEFAULT,
    CAPTURE_VIDEO_SECONDS_MAX,
    CAPTURE_VIDEO_SECONDS_MIN,
    CONF_CAPTURE_CAMERAS,
    CONF_CAPTURE_VIDEO,
    CONF_CAPTURE_VIDEO_SECONDS,
)
from .dedup import EventDedupCache
from .models import CaptureRequest, CaptureRequestRejected, parse_capture_request

_LOGGER = logging.getLogger(__name__)

PHOTO_TIMEOUT_SECONDS = 10
UPLOAD_TIMEOUT_SECONDS = 30
UPLOAD_RETRY_DELAY_SECONDS = 2
# On top of the video's own length: starting the stream (connecting to the camera,
# the first keyframe) and finishing the file.
RECORD_MARGIN_SECONDS = 30
# How long unloading waits for cancelled captures to clean up.
STOP_TIMEOUT_SECONDS = 10
# Kameraposti's limit for a video (25 MB): a bigger one is not uploaded just to be refused.
VIDEO_MAX_BYTES = 25 * 1024 * 1024
CAPTURE_DIR_NAME = ".kameraposti_capture"
FILE_PREFIX = "capture-"
# A recording older than this in the capture folder is a leftover of an interrupted run.
STALE_FILE_SECONDS = 600
# 1.9.0 (steelo 2026-10-16 "otetaan frigatesta last recognize pic"): the reasons that send Frigate's
# last recognized picture instead of a live still, how recent it must be, and how long a request
# waits for Frigate to update it when the request came first.
FRIGATE_REASONS = frozenset({"motion", "alarm"})
FRIGATE_DETECTION_MAX_AGE_SECONDS = 120
FRIGATE_WAIT_SECONDS = 6
_JPEG_MAGIC = b"\xff\xd8\xff"

_DELIVERED = "delivered"
_ALREADY_RECEIVED = "HTTP 409"


@dataclass(frozen=True, slots=True)
class CaptureCamera:
    """The Home Assistant camera linked to one Kameraposti camera (options, 1.6.0)."""

    entity_id: str
    video: bool = False
    video_seconds: int = CAPTURE_VIDEO_SECONDS_DEFAULT


def capture_cameras_from_options(options: Mapping[str, Any]) -> dict[int, CaptureCamera]:
    """{camera_id: CaptureCamera} from the entry options. Malformed links are skipped."""
    raw = options.get(CONF_CAPTURE_CAMERAS)
    cameras: dict[int, CaptureCamera] = {}
    if not isinstance(raw, Mapping):
        return cameras
    for key, link in raw.items():
        try:
            camera_id = int(key)
        except (TypeError, ValueError):
            continue
        if not isinstance(link, Mapping):
            continue
        entity_id = link.get(CONF_ENTITY_ID)
        if not isinstance(entity_id, str) or not entity_id.startswith("camera."):
            continue
        seconds = link.get(CONF_CAPTURE_VIDEO_SECONDS)
        if isinstance(seconds, bool) or not isinstance(seconds, int | float):
            seconds = CAPTURE_VIDEO_SECONDS_DEFAULT
        seconds = min(max(int(seconds), CAPTURE_VIDEO_SECONDS_MIN), CAPTURE_VIDEO_SECONDS_MAX)
        cameras[camera_id] = CaptureCamera(
            entity_id=entity_id, video=link.get(CONF_CAPTURE_VIDEO) is True, video_seconds=seconds
        )
    return cameras


def recording_timeout(seconds: int) -> float:
    """How long a recording of `seconds` may take, start-up and finishing included."""
    return seconds + RECORD_MARGIN_SECONDS


# -- Home Assistant's camera API ------------------------------------------------------
#
# Imported where used, not at module level: the camera integration (and its JPEG
# library) is needed only for a capture, so a Home Assistant without it still runs
# everything else of Kameraposti. manifest.json lists camera in after_dependencies, so
# its requirements are installed and it is set up first when it is in use.


def _require_camera_integration(hass: HomeAssistant) -> None:
    # Checked before the import: without it the camera package would be loaded on the
    # event loop, only to find no camera.
    if "camera" not in hass.config.components:
        raise HomeAssistantError("Home Assistant's camera integration is not set up")


async def async_take_photo(hass: HomeAssistant, entity_id: str) -> bytes:
    """A still from the camera (camera.async_get_image). Raises HomeAssistantError."""
    _require_camera_integration(hass)
    from homeassistant.components.camera import async_get_image  # noqa: PLC0415

    image = await async_get_image(hass, entity_id, timeout=PHOTO_TIMEOUT_SECONDS)
    return image.content


async def async_camera_stream(hass: HomeAssistant, entity_id: str) -> Any | None:
    """The camera's stream, as the camera.record service gets it. None = no streaming."""
    _require_camera_integration(hass)
    return await _camera_entity(hass, entity_id).async_create_stream()


def frigate_images(hass: HomeAssistant, entity_id: str) -> list[str]:
    """Frigate's last-detection image entities on the camera's own device (1.9.0), e.g.
    image.kamera1_person and image.kamera1_car for camera.kamera1. Empty for any other camera."""
    registry = er.async_get(hass)
    entry = registry.async_get(entity_id)
    if entry is None or entry.device_id is None:
        return []
    return [
        other.entity_id
        for other in er.async_entries_for_device(registry, entry.device_id)
        if other.domain == "image" and other.platform == "frigate"
    ]


def newest_frigate_image(hass: HomeAssistant, image_ids: list[str]) -> tuple[str, datetime] | None:
    """The image entity updated last and when: an image entity's state is its last update time."""
    newest: tuple[str, datetime] | None = None
    for image_id in image_ids:
        state = hass.states.get(image_id)
        updated = dt_util.parse_datetime(state.state) if state is not None else None
        if updated is None or updated.tzinfo is None:
            continue
        if newest is None or updated > newest[1]:
            newest = (image_id, updated)
    return newest


async def async_frigate_image(hass: HomeAssistant, entity_id: str) -> bytes | None:
    """The image entity's current image (ImageEntity.async_image), None when there is none.

    Looked up in the image integration's EntityComponent (hass.data["image"]), as its own
    image proxy view does; no import, so a Home Assistant without the image integration just
    has no Frigate image.
    """
    component = hass.data.get("image")
    entity = component.get_entity(entity_id) if component is not None else None
    if entity is None:
        return None
    async with asyncio.timeout(PHOTO_TIMEOUT_SECONDS):
        return await entity.async_image()


def _camera_entity(hass: HomeAssistant, entity_id: str) -> Any:
    """The camera entity, as camera.helper.get_camera_from_entity_id finds it.

    That helper is not in every Home Assistant hacs.json allows (2024.12): without it,
    the same lookup in the camera component (hass.data["camera"], an EntityComponent).
    """
    try:
        from homeassistant.components.camera.helper import get_camera_from_entity_id  # noqa: PLC0415
    except ImportError:
        component = hass.data.get("camera")
        camera = component.get_entity(entity_id) if component is not None else None
        if camera is None:
            raise HomeAssistantError("Camera not found") from None
        if not camera.is_on:
            raise HomeAssistantError("Camera is off") from None
        return camera
    return get_camera_from_entity_id(hass, entity_id)


# -- the recording's temporary file (executor only) ---------------------------------------


def video_path(hass: HomeAssistant, request_id: int, camera_id: int) -> str | None:
    """A new temporary file for a recording, in the first folder that may hold it. Executor only.

    <config>/.kameraposti_capture when Home Assistant's allowlist permits it, else
    .kameraposti_capture in each media folder in turn. A folder under config/www (served
    to anyone at /local/) is never used. Checked with hass.config.is_allowed_path --
    the check stream.async_record makes -- before anything is created. None = no such
    folder.
    """
    www = Path(hass.config.path("www")).resolve()
    name = f"{FILE_PREFIX}{request_id}-{camera_id}-{secrets.token_hex(4)}.mp4"
    folders = [
        hass.config.path(CAPTURE_DIR_NAME),
        *(os.path.join(media, CAPTURE_DIR_NAME) for media in hass.config.media_dirs.values()),
    ]
    for folder in folders:
        resolved = Path(folder).resolve()
        if resolved == www or www in resolved.parents:
            continue
        path = os.path.join(folder, name)
        if not hass.config.is_allowed_path(path):
            continue
        try:
            os.makedirs(folder, mode=0o700, exist_ok=True)
        except OSError:
            continue
        _sweep_stale(folder)
        return path
    return None


def _sweep_stale(folder: str) -> None:
    """Delete recordings left behind by an interrupted run (a cancel races the recorder)."""
    cutoff = time.time() - STALE_FILE_SECONDS
    try:
        entries = list(os.scandir(folder))
    except OSError:
        return
    for entry in entries:
        if not entry.name.startswith(FILE_PREFIX):
            continue
        try:
            if entry.is_file(follow_symlinks=False) and entry.stat(follow_symlinks=False).st_mtime < cutoff:
                os.remove(entry.path)
        except OSError:
            continue


def _remove_recording(path: str) -> None:
    """Delete the recording and the recorder's own <path>.tmp."""
    for name in (path, path + ".tmp"):
        try:
            os.remove(name)
        except FileNotFoundError:
            pass
        except OSError as err:
            _LOGGER.warning("Could not delete the temporary Kameraposti recording %s: %s", name, err)


def _read_recording(path: str, max_bytes: int) -> bytes | None:
    """The recording's bytes; None when it is bigger than max_bytes."""
    if os.path.getsize(path) > max_bytes:
        return None
    with open(path, "rb") as file:
        return file.read()


async def _async_detach_recorder(stream: Any, path: str) -> None:
    """Remove our recorder from the camera's stream after a timeout, an error or a cancel.

    stream.async_record leaves its recorder attached when it is interrupted, and the
    camera's next recording (ours or the camera.record service's) would then fail with
    "already recording". Only the recorder writing to our file is removed.
    """
    for output in list(stream.outputs().values()):
        if getattr(output, "video_path", None) != path:
            continue
        try:
            await stream.remove_provider(output)
        except Exception:  # noqa: BLE001 - clean-up must not hide the outcome
            _LOGGER.debug("Could not detach an interrupted Kameraposti recording", exc_info=True)


def _expired(expires_at: datetime) -> bool:
    return dt_util.utcnow() >= expires_at


class KameraportiCaptureHandler:
    """Answers Kameraposti Turva's capture requests for one config entry."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        *,
        host: str,
        customer_id: int,
        cameras: Mapping[int, CaptureCamera],
        exported: Callable[[], Collection[str]],
        video: bool,
        video_seconds: int,
    ) -> None:
        self._hass = hass
        self._entry = entry
        # The service the entry connects to: the only host the upload links may point at.
        self._host = host
        self._customer_id = customer_id
        # 1.6.0 links {camera_id: camera}: only for requests that name no entity_id.
        self._cameras = dict(cameras)
        # The reported cameras -- the only ones a photo is ever taken from -- and their video. 1.8.0
        # (steelo 2026-10-16 "maalaisjärjellä, pari klikkausta"): asked for at each request, so a
        # camera added to Home Assistant since setup works without a reload.
        self._exported = exported
        self._video = video
        self._video_seconds = video_seconds
        self._dedup = EventDedupCache()
        self._tasks: set[asyncio.Task[None]] = set()
        # One recording at a time per Home Assistant camera: a stream records to one file.
        self._record_locks: dict[str, asyncio.Lock] = {}
        self._stopping = False

    @callback
    def async_handle_message(self, topic: str, payload: bytes) -> None:
        """Runs on the event loop: validate, dedupe, start the work in a task. Never waits."""
        if self._stopping:
            return
        try:
            request = parse_capture_request(
                topic, payload, expected_customer_id=self._customer_id, expected_host=self._host
            )
        except CaptureRequestRejected as err:
            _LOGGER.debug("Ignoring invalid Kameraposti capture request on %s: %s", topic, err)
            return
        entity_id = self._entity_for(request)
        if entity_id is None or entity_id not in self._exported():
            # 1.7.0: a warning, not debug -- 16.10 a request ignored at debug level left the user
            # guessing why no photo came.
            _LOGGER.warning(
                "Kameraposti capture request_id=%s camera_id=%s: %s, no photo",
                request.request_id,
                request.camera_id,
                f"{entity_id} is hidden under Configure -> Hide cameras from Kameraposti "
                "(or does not exist in Home Assistant)"
                if entity_id is not None
                else "no Home Assistant camera chosen for it in Kameraposti",
            )
            return
        camera = CaptureCamera(entity_id=entity_id, video=self._video, video_seconds=self._video_seconds)
        if self._dedup.seen(str(request.request_id)):
            _LOGGER.debug("Kameraposti capture request_id=%s: duplicate, ignored", request.request_id)
            return
        _LOGGER.debug(
            "Kameraposti capture request_id=%s camera_id=%s reason=%s: %s, video %s",
            request.request_id,
            request.camera_id,
            request.reason,
            camera.entity_id,
            f"{camera.video_seconds} s" if camera.video else "off",
        )
        task = self._entry.async_create_background_task(
            self._hass,
            self._async_capture(request, camera),
            f"kameraposti capture {request.request_id}",
            eager_start=False,
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _entity_for(self, request: CaptureRequest) -> str | None:
        """The camera the request names, or (server before V60) the one its camera is linked to in 1.6.0."""
        if request.entity_id is not None:
            return request.entity_id
        linked = self._cameras.get(request.camera_id)
        return linked.entity_id if linked is not None else None

    async def async_stop(self) -> None:
        """Ignore new requests, cancel those in flight and wait (bounded) for their clean-up."""
        self._stopping = True
        tasks = [task for task in self._tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=STOP_TIMEOUT_SECONDS)
            if pending:
                _LOGGER.debug("%d Kameraposti captures did not stop in time", len(pending))

    async def _async_capture(self, request: CaptureRequest, camera: CaptureCamera) -> None:
        try:
            await self._async_photo(request, camera)
            # Also after a cancel that the camera's own code swallowed (async_get_image does).
            if camera.video and not self._stopping:
                await self._async_video(request, camera)
        except Exception as err:  # noqa: BLE001 - never its message: it can carry a signed URL
            _LOGGER.warning(
                "Kameraposti capture request_id=%s camera_id=%s failed: %s",
                request.request_id,
                request.camera_id,
                type(err).__name__,
            )

    async def _async_photo(self, request: CaptureRequest, camera: CaptureCamera) -> None:
        if _expired(request.photo_expires_at):
            self._log_outcome(request, camera, "photo", "link expired")
            return
        if request.reason in FRIGATE_REASONS and (images := frigate_images(self._hass, camera.entity_id)):
            frigate = await self._async_frigate_photo(request, images)
            if frigate is not None:
                image_id, content = frigate
                outcome = await self._async_upload(
                    request, "photo", request.photo_url, request.photo_expires_at, content, "image/jpeg"
                )
                self._log_outcome(request, camera, "photo", outcome, source=image_id)
                return
            if request.reason != "alarm":
                # Plain motion without a recognition never brings a picture: that was the noise.
                _LOGGER.info(
                    "Kameraposti capture request_id=%s camera_id=%s: no fresh Frigate detection for %s, "
                    "no photo (%s)",
                    request.request_id,
                    request.camera_id,
                    camera.entity_id,
                    request.reason,
                )
                return
            # An alarm always brings a picture: the live still.
        try:
            content = await async_take_photo(self._hass, camera.entity_id)
        except (HomeAssistantError, ImportError) as err:
            self._warn_no_photo(request, camera, str(err))
            return
        except Exception as err:  # noqa: BLE001 - a camera's own snapshot code may raise anything
            # Its type only: a camera's error can carry the camera's own address. The video
            # still goes. CancelledError is no Exception and stops the capture.
            self._warn_no_photo(request, camera, type(err).__name__)
            return
        outcome = await self._async_upload(
            request, "photo", request.photo_url, request.photo_expires_at, content, "image/jpeg"
        )
        self._log_outcome(request, camera, "photo", outcome)

    async def _async_frigate_photo(
        self, request: CaptureRequest, images: list[str]
    ) -> tuple[str, bytes] | None:
        """Frigate's newest recognized picture of the camera, if fresh and a JPEG (1.9.0).

        Turva sends the request as soon as the sensor turns on, possibly before Frigate updates its
        image: then wait (up to FRIGATE_WAIT_SECONDS) for one of the camera's images to update.
        """
        newest = newest_frigate_image(self._hass, images)
        if newest is None or newest[1] < request.requested_at:
            await self._async_wait_for_frigate(request, images)
            newest = newest_frigate_image(self._hass, images)
        if newest is None:
            return None
        image_id, updated = newest
        age = (dt_util.utcnow() - updated).total_seconds()
        if not 0 <= age <= FRIGATE_DETECTION_MAX_AGE_SECONDS:
            _LOGGER.debug(
                "Kameraposti capture request_id=%s: Frigate's %s is %.0f s old, not used",
                request.request_id,
                image_id,
                age,
            )
            return None
        try:
            content = await async_frigate_image(self._hass, image_id)
        except Exception as err:  # noqa: BLE001 - an image entity's own code may raise anything
            _LOGGER.warning(
                "Kameraposti capture request_id=%s: could not read Frigate's %s: %s",
                request.request_id,
                image_id,
                type(err).__name__,
            )
            return None
        if not content or not content.startswith(_JPEG_MAGIC):
            _LOGGER.warning(
                "Kameraposti capture request_id=%s: Frigate's %s has %s, not used",
                request.request_id,
                image_id,
                "no image" if not content else "no JPEG image",
            )
            return None
        return image_id, content

    async def _async_wait_for_frigate(self, request: CaptureRequest, images: list[str]) -> None:
        """Until one of the images is updated at or after requested_at, at most FRIGATE_WAIT_SECONDS.

        Waits on state changes and a timer, never polls; runs in the capture's own task, so unloading
        cancels it like the rest of the capture.
        """
        done = asyncio.Event()

        @callback
        def _updated(_event: Event[EventStateChangedData]) -> None:
            newest = newest_frigate_image(self._hass, images)
            if newest is not None and newest[1] >= request.requested_at:
                done.set()

        @callback
        def _timeout(_now: datetime) -> None:
            done.set()

        unsub_state = async_track_state_change_event(self._hass, images, _updated)
        unsub_timer = async_call_later(self._hass, FRIGATE_WAIT_SECONDS, HassJob(_timeout))
        try:
            await done.wait()
        finally:
            unsub_state()
            unsub_timer()

    async def _async_video(self, request: CaptureRequest, camera: CaptureCamera) -> None:
        if _expired(request.video_expires_at):
            self._log_outcome(request, camera, "video", "link expired")
            return
        path: str | None = None
        try:
            async with self._record_locks.setdefault(camera.entity_id, asyncio.Lock()):
                # Checked again: the camera's previous recording may have taken a while.
                if _expired(request.video_expires_at):
                    self._log_outcome(request, camera, "video", "link expired")
                    return
                path = await self._hass.async_add_executor_job(
                    video_path, self._hass, request.request_id, request.camera_id
                )
                if path is None:
                    self._warn_no_video(
                        request,
                        camera,
                        "Home Assistant allows no private folder to record into (a media folder, or "
                        f"{self._hass.config.path(CAPTURE_DIR_NAME)} in allowlist_external_dirs)",
                    )
                    return
                if not await self._async_record(request, camera, path):
                    return
            content = await self._hass.async_add_executor_job(_read_recording, path, VIDEO_MAX_BYTES)
            if content is None:
                self._log_outcome(
                    request, camera, "video", f"larger than {VIDEO_MAX_BYTES // (1024 * 1024)} MB, not sent"
                )
                return
            outcome = await self._async_upload(
                request, "video", request.video_url, request.video_expires_at, content, "video/mp4"
            )
            self._log_outcome(request, camera, "video", outcome)
        finally:
            if path is not None:
                await self._hass.async_add_executor_job(_remove_recording, path)

    async def _async_record(self, request: CaptureRequest, camera: CaptureCamera, path: str) -> bool:
        """Record into path; False (after one warning) when there is no video."""
        try:
            stream = await async_camera_stream(self._hass, camera.entity_id)
        except (HomeAssistantError, ImportError) as err:
            self._warn_no_video(request, camera, str(err))
            return False
        except Exception as err:  # noqa: BLE001 - a camera's own stream_source() may raise anything
            self._warn_no_video(request, camera, type(err).__name__)
            return False
        if stream is None:
            self._warn_no_video(request, camera, "the camera does not support streaming")
            return False

        timeout = recording_timeout(camera.video_seconds)
        finished = False
        try:
            async with asyncio.timeout(timeout):
                await stream.async_record(path, duration=camera.video_seconds, lookback=0)
            finished = True
        except TimeoutError:
            self._warn_no_video(request, camera, f"the recording did not finish within {timeout:.0f} s")
            return False
        except HomeAssistantError as err:
            self._warn_no_video(request, camera, str(err))
            return False
        except Exception as err:  # noqa: BLE001 - e.g. PyAV missing, the disk full
            self._warn_no_video(request, camera, type(err).__name__)
            return False
        finally:
            if not finished:
                await _async_detach_recorder(stream, path)

        if not await self._hass.async_add_executor_job(os.path.isfile, path):
            self._warn_no_video(request, camera, "the recording captured nothing")
            return False
        return True

    async def _async_upload(
        self,
        request: CaptureRequest,
        kind: str,
        url: str,
        expires_at: datetime,
        content: bytes,
        content_type: str,
    ) -> str:
        """POST content to its signed URL: "delivered", or why not. Never the URL itself.

        One retry after a 5xx, a timeout or a connection error, while the URL is valid.
        """
        outcome = "link expired"
        for attempt in (1, 2):
            if attempt == 2:
                _LOGGER.debug(
                    "Kameraposti capture request_id=%s %s: %s, retrying once",
                    request.request_id,
                    kind,
                    outcome,
                )
                await asyncio.sleep(UPLOAD_RETRY_DELAY_SECONDS)
            if _expired(expires_at):
                return outcome if attempt == 1 else f"{outcome}; the link expired before a retry"
            transient, outcome = await self._async_post(url, content, content_type)
            if not transient:
                return outcome
        return f"{outcome} (after one retry)"

    async def _async_post(self, url: str, content: bytes, content_type: str) -> tuple[bool, str]:
        """One POST: (transient, outcome). Never the URL, never an httpx message."""
        client = get_async_client(self._hass)
        try:
            response = await client.post(
                url,
                content=content,
                headers={"Content-Type": content_type},
                timeout=UPLOAD_TIMEOUT_SECONDS,
                # A signed URL never redirects: never send the file anywhere else.
                follow_redirects=False,
            )
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as err:
            return True, type(err).__name__
        except (httpx.HTTPError, httpx.InvalidURL) as err:
            return False, type(err).__name__
        if response.is_success:
            return False, _DELIVERED
        return response.status_code >= 500, f"HTTP {response.status_code}"

    def _log_outcome(
        self,
        request: CaptureRequest,
        camera: CaptureCamera,
        kind: str,
        outcome: str,
        *,
        source: str | None = None,
    ) -> None:
        # source: where the photo came from when not the camera itself (1.9.0: Frigate's image).
        if outcome == _DELIVERED:
            _LOGGER.info(
                "Kameraposti capture request_id=%s camera_id=%s: %s from %s delivered",
                request.request_id,
                request.camera_id,
                kind,
                source or camera.entity_id,
            )
        elif outcome == _ALREADY_RECEIVED:
            # Another Home Assistant on the account was first, or an earlier attempt got through.
            _LOGGER.info(
                "Kameraposti capture request_id=%s camera_id=%s: %s already received by Kameraposti",
                request.request_id,
                request.camera_id,
                kind,
            )
        else:
            _LOGGER.warning(
                "Kameraposti capture request_id=%s camera_id=%s: %s from %s not delivered: %s",
                request.request_id,
                request.camera_id,
                kind,
                source or camera.entity_id,
                outcome,
            )

    def _warn_no_photo(self, request: CaptureRequest, camera: CaptureCamera, why: str) -> None:
        _LOGGER.warning(
            "Kameraposti capture request_id=%s camera_id=%s: no photo from %s: %s",
            request.request_id,
            request.camera_id,
            camera.entity_id,
            why,
        )

    def _warn_no_video(self, request: CaptureRequest, camera: CaptureCamera, why: str) -> None:
        _LOGGER.warning(
            "Kameraposti capture request_id=%s camera_id=%s: no video from %s: %s",
            request.request_id,
            request.camera_id,
            camera.entity_id,
            why,
        )
