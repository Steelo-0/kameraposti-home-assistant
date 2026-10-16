"""Home Assistant cameras to Kameraposti (1.7.0, steelo 2026-10-16 "idioottivarma").

The user ticks cameras under "Cameras to Kameraposti" in the options. For each one this module
publishes, over the integration's own connection, a description under the sensor prefix:

* ``kameraposti/<id>/anturit/<name>/config`` --
  {"kind": "camera", "entity_id", "name", "mac", "camera_id"?}; Kameraposti creates a camera for
  it (or links the camera a 1.6.0 option already linked to it: ``camera_id``) and links a Shelly
  Camera device whose own id ends in the same MAC.
* the same topic with {"kind": "camera", "entity_id", "removed": true} for a camera unticked since.

``<name>`` is the sensor exporter's topic name for the entity_id; Kameraposti keys the camera by
the entity_id in the payload. Descriptions and removals go out on every (re)connect, at most once
a minute (a reconnect storm must not use the account's message budget), and every 15 minutes:
Kameraposti's listener reconnects hourly, and a message published in that gap would be lost.

Before the user saves the new list, a 1.6.0 entry's linked cameras are the ticked ones (and their
Kameraposti camera ids are sent along), so an upgrade keeps working without touching the options.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from homeassistant.core import CALLBACK_TYPE, HassJob, HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_call_later, async_track_time_interval
from homeassistant.util import dt as dt_util

from .capture import capture_cameras_from_options
from .const import (
    CAMERA_ENTITY_PATTERN,
    CAPTURE_VIDEO_SECONDS_DEFAULT,
    CAPTURE_VIDEO_SECONDS_MAX,
    CAPTURE_VIDEO_SECONDS_MIN,
    CONF_CAMERA_VIDEO,
    CONF_CAMERA_VIDEO_SECONDS,
    CONF_EXPORTED_CAMERAS,
    CONF_REMOVED_CAMERAS,
    MAX_EXPORTED_CAMERAS,
    MAX_REMOVED_CAMERAS,
    SENSOR_TOPIC_TEMPLATE,
    SNAPSHOT_MIN_INTERVAL_SECONDS,
)
from .sensor_export import mqtt_name_for

_LOGGER = logging.getLogger(__name__)

RESYNC_INTERVAL = timedelta(minutes=15)
_MAX_NAME_LENGTH = 60
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
_MAC_TAIL = re.compile(r"(?:^|[^0-9a-f])([0-9a-f]{12})$")
_SEPARATED_MAC = re.compile(r"[0-9a-f]{2}(?:[:\-][0-9a-f]{2}){5}")

PublishFn = Callable[[str, str, bool], bool]


@dataclass(frozen=True, slots=True)
class CameraSettings:
    """The ticked cameras and their video setting, from the entry options."""

    exported: tuple[str, ...]
    removed: tuple[str, ...]
    # entity_id -> the Kameraposti camera a 1.6.0 option linked it to.
    hints: Mapping[str, int]
    video: bool
    video_seconds: int


def _entity_ids(raw: Any, limit: int) -> list[str]:
    if not isinstance(raw, list | tuple):
        return []
    valid = [item for item in raw if isinstance(item, str) and CAMERA_ENTITY_PATTERN.fullmatch(item)]
    return list(dict.fromkeys(valid))[:limit]


def _seconds(raw: Any) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        return CAPTURE_VIDEO_SECONDS_DEFAULT
    return min(max(int(raw), CAPTURE_VIDEO_SECONDS_MIN), CAPTURE_VIDEO_SECONDS_MAX)


def camera_settings_from_options(options: Mapping[str, Any]) -> CameraSettings:
    """The ticked cameras; before the list is saved, the cameras 1.6.0 options linked."""
    legacy = capture_cameras_from_options(options)
    hints: dict[str, int] = {}
    for camera_id in sorted(legacy):
        hints.setdefault(legacy[camera_id].entity_id, camera_id)

    if CONF_EXPORTED_CAMERAS in options:
        exported = _entity_ids(options.get(CONF_EXPORTED_CAMERAS), MAX_EXPORTED_CAMERAS)
    else:
        exported = _entity_ids(list(hints), MAX_EXPORTED_CAMERAS)
    if CONF_CAMERA_VIDEO in options:
        video = options.get(CONF_CAMERA_VIDEO) is True
        video_seconds = _seconds(options.get(CONF_CAMERA_VIDEO_SECONDS))
    else:
        video = any(link.video for link in legacy.values())
        video_seconds = max(
            (link.video_seconds for link in legacy.values() if link.video),
            default=CAPTURE_VIDEO_SECONDS_DEFAULT,
        )
    removed = _entity_ids(options.get(CONF_REMOVED_CAMERAS), MAX_REMOVED_CAMERAS)
    return CameraSettings(
        exported=tuple(exported),
        removed=tuple(entity_id for entity_id in removed if entity_id not in exported),
        hints={entity_id: camera_id for entity_id, camera_id in hints.items() if entity_id in exported},
        video=video,
        video_seconds=video_seconds,
    )


def removed_after_save(previous: CameraSettings, selected: list[str]) -> list[str]:
    """The removal list after saving ``selected``: newly unticked ones last, re-ticked ones out."""
    removed = [entity_id for entity_id in previous.removed if entity_id not in selected]
    for entity_id in previous.exported:
        if entity_id not in selected and entity_id not in removed:
            removed.append(entity_id)
    return removed[-MAX_REMOVED_CAMERAS:]


def _clean(name: str) -> str:
    return _CONTROL_CHARS.sub("", name).strip()[:_MAX_NAME_LENGTH].strip()


def camera_name(hass: HomeAssistant, entity_id: str) -> str:
    """The name for a new Kameraposti camera: the device's name ("shellycamera-6ca61366d281") unless
    the user named the entity, else the friendly name, else the entity's object id."""
    entry = er.async_get(hass).async_get(entity_id)
    if entry is not None and entry.name is None and entry.device_id is not None:
        device = dr.async_get(hass).async_get(entry.device_id)
        if device is not None and (name := _clean(str(device.name_by_user or device.name or ""))):
            return name
    state = hass.states.get(entity_id)
    if state is not None and (name := _clean(str(state.attributes.get("friendly_name") or ""))):
        return name
    return entity_id.split(".", 1)[1][:_MAX_NAME_LENGTH]


def _mac(value: str) -> str | None:
    """12 hex digits at the end of a name or identifier, or a separated MAC (6c:a6:13:66:d2:81)."""
    value = value.strip().lower()
    if _SEPARATED_MAC.fullmatch(value):
        return re.sub(r"[:\-]", "", value)
    match = _MAC_TAIL.search(value)
    return match.group(1) if match else None


def camera_mac(hass: HomeAssistant, entity_id: str) -> str | None:
    """The camera device's MAC as 12 lowercase hex digits (a Shelly's own id ends in it), or None.

    The device registry's MAC connection; else the end of the device's name or of an identifier
    ("shellycamera-6ca61366d281")."""
    entry = er.async_get(hass).async_get(entity_id)
    if entry is None or entry.device_id is None:
        return None
    device = dr.async_get(hass).async_get(entry.device_id)
    if device is None:
        return None
    for kind, value in sorted(device.connections):
        if kind == dr.CONNECTION_NETWORK_MAC and len(mac := re.sub(r"[^0-9a-f]", "", value.lower())) == 12:
            return mac
    candidates = [str(device.name or "")] + [str(value) for _domain, value in sorted(device.identifiers)]
    for candidate in candidates:
        if mac := _mac(candidate):
            return mac
    return None


class KameraportiCameraExporter:
    """Describes the ticked cameras (and the unticked ones as removed) to Kameraposti."""

    def __init__(
        self, hass: HomeAssistant, *, customer_id: int, settings: CameraSettings, publish: PublishFn
    ) -> None:
        self._hass = hass
        self._customer_id = customer_id
        self._settings = settings
        self._publish = publish
        self._sent_at: datetime | None = None
        self._pending: CALLBACK_TYPE | None = None
        self._unsub_resync: Callable[[], None] | None = None

    def async_start(self) -> None:
        if self._unsub_resync is None and (self._settings.exported or self._settings.removed):
            self._unsub_resync = async_track_time_interval(
                self._hass, self._handle_resync, RESYNC_INTERVAL, cancel_on_shutdown=True
            )

    @callback
    def async_stop(self) -> None:
        if self._unsub_resync is not None:
            self._unsub_resync()
            self._unsub_resync = None
        if self._pending is not None:
            self._pending()
            self._pending = None

    @callback
    def async_on_connected(self) -> None:
        """(Re)connected: describe everything, at most once per SNAPSHOT_MIN_INTERVAL_SECONDS."""
        if not (self._settings.exported or self._settings.removed):
            return
        sent_at = self._sent_at
        elapsed = (dt_util.utcnow() - sent_at).total_seconds() if sent_at is not None else None
        if elapsed is None or elapsed >= SNAPSHOT_MIN_INTERVAL_SECONDS:
            self.publish_all()
        elif self._pending is None:
            self._pending = async_call_later(
                self._hass,
                SNAPSHOT_MIN_INTERVAL_SECONDS - elapsed,
                HassJob(self._send_pending, cancel_on_shutdown=True),
            )

    @callback
    def _send_pending(self, _now: datetime) -> None:
        self._pending = None
        self.publish_all()

    @callback
    def _handle_resync(self, _now: datetime) -> None:
        self.publish_all()

    @callback
    def publish_all(self) -> None:
        """Removals first, then the ticked cameras. Stops at the first message that cannot be sent."""
        sent = False
        for entity_id in self._settings.removed:
            if not self._send(entity_id, {"kind": "camera", "entity_id": entity_id, "removed": True}):
                return
            sent = True
        for entity_id in self._settings.exported:
            # A camera that no longer exists in Home Assistant is not described (Kameraposti would
            # make a camera that can never take a photo).
            if (
                er.async_get(self._hass).async_get(entity_id) is None
                and self._hass.states.get(entity_id) is None
            ):
                continue
            payload: dict[str, Any] = {
                "kind": "camera",
                "entity_id": entity_id,
                "name": camera_name(self._hass, entity_id),
                "mac": camera_mac(self._hass, entity_id),
            }
            if (camera_id := self._settings.hints.get(entity_id)) is not None:
                payload["camera_id"] = camera_id
            if not self._send(entity_id, payload):
                return
            sent = True
        if sent:
            self._sent_at = dt_util.utcnow()

    def _send(self, entity_id: str, payload: dict[str, Any]) -> bool:
        topic = SENSOR_TOPIC_TEMPLATE.format(customer_id=self._customer_id, name=mqtt_name_for(entity_id))
        return self._publish(f"{topic}/config", json.dumps(payload, ensure_ascii=False), False)
