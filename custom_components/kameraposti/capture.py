"""Photos and videos from Home Assistant cameras for Kameraposti Turva (1.6.0).

Kameraposti Turva asks for a photo when a sensor linked to one of its cameras
alarms, or when the user presses "Ota kuva Home Assistantista" on the camera's
page: it publishes a capture request (customers/<id>/cameras/<camera>/capture,
QoS 1, not retained) with two signed, expiring upload URLs. 1.7.0: the request
names the Home Assistant camera (entity_id), which must be one the integration
reports to Kameraposti (camera_export.py; 1.10.0: every camera, worked out when the
request arrives); a request from an
older server names none, and the 1.6.0 option linking its Kameraposti camera
decides (its Home Assistant camera must be reported too). For that camera this module

1. takes a still with camera.async_get_image (10 s) and POSTs it to photo_url
   as image/jpeg;
2. then, unless Kameraposti's video link has already expired (a motion's has; 1.10.0: no
   video setting in Home Assistant), records 10 seconds -- including the last few seconds
   before the request when the camera's stream is already running ("Preload stream") --
   with Home Assistant's stream integration, the same mechanism as the
   camera.record service: camera.async_create_stream() and
   stream.async_record() -- and POSTs the MP4 to video_url as video/mp4.

1.11.1 (steelo 2026-10-17 "frigatelta tulee tunnistus jonka pitää laukaista video", "ei vanhaa vaan
heti kun tunnistus tapahtuu"): on a Frigate camera (platform frigate) only Frigate's detection brings
anything. A "motion" or "alarm" request is answered only when Frigate detected something on that camera
at most DETECTION_BEFORE_SECONDS before the request reached Home Assistant (frigate_events.py hears it
and sends it to Kameraposti, whose request follows within a second or two): the photo is Frigate's own
picture of that detection, fetched from Frigate by its id, then the video as for any camera. Without
such a detection -- or when its picture already went for an earlier request -- nothing is sent: no
photo, no video, never an older picture, never a live still. (1.9.0-1.11.0 sent Frigate's newest
last-detection image up to 120 s old, and an alarm without one the live still: 17.10 Frigate's pixel
motion sensors brought a live still with every alarm while nothing happened.)
Any other camera, and a "manual" request (the user's own "Ota kuva"), take the live still as before. A
motion request never records a video; a camera other than Frigate's sends at most one live still per
MOTION_LIVE_MIN_SECONDS for motion (several motion sensors on one camera each ask).

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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ENTITY_ID
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
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
from .frigate_events import KameraportiFrigateDetections, async_detection_snapshot, is_frigate_camera
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
# 1.11.1: the reasons a Frigate camera answers only with Frigate's detection ("manual" takes the live
# still), and how long before the request reached Home Assistant that detection may be: Kameraposti's
# request for it follows within a second or two.
FRIGATE_REASONS = frozenset({"motion", "alarm"})
DETECTION_BEFORE_SECONDS = 10
# At most one live still per camera this often for motion: Turva limits motion per sensor, and two
# PIRs on one camera would otherwise bring two stills a minute.
MOTION_LIVE_MIN_SECONDS = 60
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


def _unmark(marks: dict[str, datetime], key: str, mark: datetime, previous: datetime | None) -> None:
    """Undo `mark` (nothing was delivered after all), unless another request has marked since."""
    if marks.get(key) != mark:
        return
    if previous is None:
        marks.pop(key, None)
    else:
        marks[key] = previous


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
        detections: KameraportiFrigateDetections | None = None,
    ) -> None:
        self._hass = hass
        # 1.11.1: Frigate's detections as they happen: a Frigate camera answers only them.
        self._detections = detections
        # camera entity_id -> the detection whose picture went last: one detection, one photo and video.
        # In memory: after a restart Frigate's next detection is new anyway.
        self._detection_sent: dict[str, str] = {}
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
        self._no_stream_warned: set[str] = set()
        self._dedup = EventDedupCache()
        self._tasks: set[asyncio.Task[None]] = set()
        # One recording at a time per Home Assistant camera: a stream records to one file.
        self._record_locks: dict[str, asyncio.Lock] = {}
        # camera entity_id -> when a live still for motion was last sent (MOTION_LIVE_MIN_SECONDS).
        self._motion_live_sent: dict[str, datetime] = {}
        self._stopping = False

    @callback
    def async_handle_message(self, topic: str, payload: bytes) -> None:
        """Runs on the event loop: validate, dedupe, start the work in a task. Never waits."""
        if self._stopping:
            return
        # 1.11.1: on Home Assistant's own clock, like Frigate's detections (Kameraposti's may differ).
        received = dt_util.utcnow()
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
                f"{entity_id} does not exist in Home Assistant (or is disabled)"
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
            self._async_capture(request, camera, received),
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

    async def _async_capture(
        self, request: CaptureRequest, camera: CaptureCamera, received: datetime
    ) -> None:
        try:
            if request.reason in FRIGATE_REASONS and is_frigate_camera(self._hass, camera.entity_id):
                # 1.11.1: a Frigate camera answers only Frigate's detection -- else nothing, no video either.
                if not await self._async_detection_photo(request, camera, received):
                    return
            else:
                await self._async_photo(request, camera)
            # Also after a cancel that the camera's own code swallowed (async_get_image does).
            if camera.video and not self._stopping:
                if request.reason == "motion" or _expired(request.video_expires_at):
                    # 1.9.0: Turva wants no video of a motion (its video link has already expired);
                    # trying it only put a "link expired" warning in the log on every motion. 1.10.0:
                    # Kameraposti decides the video -- a link already expired here means none is wanted
                    # (the photo step cannot outlast a 5-minute link).
                    _LOGGER.debug("Kameraposti capture request_id=%s: no video wanted", request.request_id)
                else:
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
        # Marked before the still is taken, so another sensor's motion handled meanwhile does not
        # send one too; undone if nothing is delivered.
        marked: datetime | None = None
        previous: datetime | None = None
        if request.reason == "motion":
            now = dt_util.utcnow()
            previous = self._motion_live_sent.get(camera.entity_id)
            if previous is not None and (now - previous).total_seconds() < MOTION_LIVE_MIN_SECONDS:
                _LOGGER.info(
                    "Kameraposti capture request_id=%s camera_id=%s: a live still from %s went less than "
                    "%d s ago, no photo (motion)",
                    request.request_id,
                    request.camera_id,
                    camera.entity_id,
                    MOTION_LIVE_MIN_SECONDS,
                )
                return
            self._motion_live_sent[camera.entity_id] = marked = now
        outcome: str | None = None
        try:
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
        finally:
            if marked is not None and outcome != _DELIVERED:
                _unmark(self._motion_live_sent, camera.entity_id, marked, previous)

    async def _async_detection_photo(
        self, request: CaptureRequest, camera: CaptureCamera, received: datetime
    ) -> bool:
        """1.11.1: send Frigate's picture of the detection that set off the request. False -- and nothing
        is sent for the request, no video either -- when Frigate detected nothing on the camera within
        DETECTION_BEFORE_SECONDS before the request, or that detection was answered already."""
        since = received - timedelta(seconds=DETECTION_BEFORE_SECONDS)
        detection = self._detections.latest(camera.entity_id, since) if self._detections is not None else None
        if detection is None:
            _LOGGER.info(
                "Kameraposti capture request_id=%s camera_id=%s: no Frigate detection on %s, "
                "no photo or video (%s)",
                request.request_id,
                request.camera_id,
                camera.entity_id,
                request.reason,
            )
            return False
        if self._detection_sent.get(camera.entity_id) == detection.event_id:
            _LOGGER.info(
                "Kameraposti capture request_id=%s camera_id=%s: Frigate's detection %s on %s already "
                "answered, no photo or video (%s)",
                request.request_id,
                request.camera_id,
                detection.event_id,
                camera.entity_id,
                request.reason,
            )
            return False
        # Taken before anything is awaited: a second request handled meanwhile (another sensor of the
        # camera) gets nothing. One detection, one photo and video -- also when the photo fails.
        self._detection_sent[camera.entity_id] = detection.event_id
        source = f"Frigate's {detection.label or 'detection'} on {camera.entity_id}"
        if _expired(request.photo_expires_at):
            self._log_outcome(request, camera, "photo", "link expired", source=source)
            return True
        content, why_not = await async_detection_snapshot(self._hass, detection.event_id)
        if content is None:
            self._warn_no_photo(request, camera, f"Frigate gave no picture of its detection: {why_not}")
            return True
        outcome = await self._async_upload(
            request, "photo", request.photo_url, request.photo_expires_at, content, "image/jpeg"
        )
        self._log_outcome(request, camera, "photo", outcome, source=source)
        return True

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
            # Audit 1.10.0 M2: video is always asked for now, so a camera that cannot stream warns once
            # per run, not on every alarm.
            if camera.entity_id in self._no_stream_warned:
                _LOGGER.debug(
                    "Kameraposti capture request_id=%s: %s cannot stream, photo only",
                    request.request_id,
                    camera.entity_id,
                )
            else:
                self._no_stream_warned.add(camera.entity_id)
                self._warn_no_video(request, camera, "the camera does not support streaming (photos only)")
            return False

        timeout = recording_timeout(camera.video_seconds)
        finished = False
        try:
            async with asyncio.timeout(timeout):
                # lookback=0: when the stream is already running ("Preload stream"), Home Assistant still
                # prepends the last finished segment (a few seconds before the request); a larger lookback
                # adds nothing below one segment's length (audit 1.10.0 L1).
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
        # source: where the photo came from when not the camera itself (1.11.1: Frigate's detection).
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
