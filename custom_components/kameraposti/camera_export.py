"""Home Assistant cameras to Kameraposti (1.7.0; 1.8.0 steelo 2026-10-16 "maalaisjärjellä, pari klikkausta").

1.8.0: Home Assistant only reports its cameras; everything is chosen in Kameraposti (Turva) -- which
reported camera becomes a Kameraposti camera, which one a camera's page uses, the Shelly by its MAC.
1.7.0 had the user tick cameras in the options, and settings in three places became a jungle.
The reported cameras (the "exported set") are worked out whenever they are needed -- at every
publish and every capture request -- so a camera added to Home Assistant later is reported at the
next resend:

a. ``excluded_cameras`` saved (the 1.8.0 options): every camera in Home Assistant except those.
b. else ``exported_cameras`` saved (the 1.7.0 ticks): exactly those, so a user who chose only some
   cameras keeps that until the 1.8.0 options are saved.
c. else the cameras linked by 1.6.0 options.
d. else (cameras never configured): every camera, as in a.

At most MAX_EXPORTED_CAMERAS: the first after sorting by entity_id, or of the saved list. For each
one this module publishes, over the integration's own connection, a description under the sensor
prefix:

* ``kameraposti/<id>/anturit/<name>/config`` --
  {"kind": "camera", "entity_id", "name", "mac", "camera_id"?}; Kameraposti lists it among the Home
  Assistant cameras the user can add or pick (``camera_id``: the camera a 1.6.0 option already
  linked to it) and attaches a Shelly Camera device whose own id ends in the same MAC. The protocol
  is the same as in 1.7.0; only what Kameraposti does with a description changed (Turva V62).
* the same topic with {"kind": "camera", "entity_id", "removed": true} for a camera that stopped
  being reported when the options were saved (and is not reported again since).

``<name>`` is the sensor exporter's topic name for the entity_id; Kameraposti keys the camera by
the entity_id in the payload. Descriptions and removals go out on every (re)connect, at most once
a minute (a reconnect storm must not use the account's message budget), and every 15 minutes:
Kameraposti's listener reconnects hourly, and a message published in that gap would be lost.

A camera a 1.6.0 option linked to a Kameraposti camera carries that camera's id along, whichever
way it is reported, so an upgrade keeps using the same Kameraposti camera.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Collection, Mapping, Sequence
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
    CONF_EXCLUDED_CAMERAS,
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
    """Which cameras are reported and their video setting, from the entry options."""

    # A list saved by 1.7.0 ticks or 1.6.0 links (cases b, c); None = every camera but `excluded`.
    allowlist: tuple[str, ...] | None
    excluded: tuple[str, ...]
    # Described as removed while they are not reported (the saved list; filtered at publish time).
    removed: tuple[str, ...]
    # entity_id -> the Kameraposti camera a 1.6.0 option linked it to.
    hints: Mapping[str, int]
    video: bool
    video_seconds: int


def _entity_ids(raw: Any, limit: int | None) -> list[str]:
    if not isinstance(raw, list | tuple):
        return []
    valid = [item for item in raw if isinstance(item, str) and CAMERA_ENTITY_PATTERN.fullmatch(item)]
    return list(dict.fromkeys(valid))[:limit]


def _seconds(raw: Any) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        return CAPTURE_VIDEO_SECONDS_DEFAULT
    return min(max(int(raw), CAPTURE_VIDEO_SECONDS_MIN), CAPTURE_VIDEO_SECONDS_MAX)


def camera_settings_from_options(options: Mapping[str, Any]) -> CameraSettings:
    """The reporting rule (a-d in the module docstring), the removals and the video setting."""
    legacy = capture_cameras_from_options(options)
    hints: dict[str, int] = {}
    for camera_id in sorted(legacy):
        hints.setdefault(legacy[camera_id].entity_id, camera_id)

    # Privacy first: a list the user saved in 1.7.0 (or linked in 1.6.0) keeps limiting what is
    # reported until the 1.8.0 options are saved; only an entry that never chose cameras reports all.
    allowlist: list[str] | None = None
    if CONF_EXCLUDED_CAMERAS not in options:
        if CONF_EXPORTED_CAMERAS in options:
            allowlist = _entity_ids(options.get(CONF_EXPORTED_CAMERAS), MAX_EXPORTED_CAMERAS)
        elif hints:
            allowlist = _entity_ids(list(hints), MAX_EXPORTED_CAMERAS)
    if CONF_CAMERA_VIDEO in options:
        video = options.get(CONF_CAMERA_VIDEO) is True
        video_seconds = _seconds(options.get(CONF_CAMERA_VIDEO_SECONDS))
    else:
        video = any(link.video for link in legacy.values())
        video_seconds = max(
            (link.video_seconds for link in legacy.values() if link.video),
            default=CAPTURE_VIDEO_SECONDS_DEFAULT,
        )
    return CameraSettings(
        allowlist=tuple(allowlist) if allowlist is not None else None,
        excluded=tuple(_entity_ids(options.get(CONF_EXCLUDED_CAMERAS), None)),
        removed=tuple(_entity_ids(options.get(CONF_REMOVED_CAMERAS), MAX_REMOVED_CAMERAS)),
        hints=hints,
        video=video,
        video_seconds=video_seconds,
    )


@callback
def home_assistant_cameras(hass: HomeAssistant) -> list[str]:
    """Every camera in Home Assistant, sorted by entity_id.

    The entity registry's cameras that are not disabled -- also one whose integration has not loaded
    yet at start -- and any camera state without a registry entry (a camera set up in YAML without a
    unique_id). Only ids Kameraposti accepts.
    """
    registry = er.async_get(hass)
    found = {
        entry.entity_id
        for entry in registry.entities.values()
        if entry.domain == "camera" and not entry.disabled
    }
    found.update(
        entity_id
        for entity_id in hass.states.async_entity_ids("camera")
        if registry.async_get(entity_id) is None
    )
    return sorted(entity_id for entity_id in found if CAMERA_ENTITY_PATTERN.fullmatch(entity_id))


@callback
def exported_cameras(hass: HomeAssistant, settings: CameraSettings) -> tuple[str, ...]:
    """The cameras reported to Kameraposti right now -- the only ones it can ask for photos."""
    if settings.allowlist is not None:
        return settings.allowlist
    excluded = set(settings.excluded)
    cameras = [entity_id for entity_id in home_assistant_cameras(hass) if entity_id not in excluded]
    return tuple(cameras[:MAX_EXPORTED_CAMERAS])


def removed_after_save(removed: Sequence[str], before: Sequence[str], after: Collection[str]) -> list[str]:
    """The removal list after a save: cameras no longer reported go last, reported-again ones out."""
    result = [entity_id for entity_id in removed if entity_id not in after]
    for entity_id in before:
        if entity_id not in after and entity_id not in result:
            result.append(entity_id)
    return result[-MAX_REMOVED_CAMERAS:]


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
    """Describes the reported cameras (and those no longer reported as removed) to Kameraposti."""

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
        self._warned_missing: set[str] = set()

    def async_start(self) -> None:
        # 1.8.0: always -- a camera added to Home Assistant later is reported at the next resend.
        if self._unsub_resync is None:
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
        # Audit L-5: a camera on a saved list missing 15 minutes after start is gone (removed or renamed
        # in Home Assistant), not just not loaded yet -- say so once.
        self.publish_all(warn_missing=True)

    @callback
    def publish_all(self, *, warn_missing: bool = False) -> None:
        """Removals first, then the reported cameras. Stops at the first message that cannot be sent.

        The reported cameras are worked out now, not at setup: a camera added to Home Assistant since
        is described, and a removed one that is reported again is not also described as removed.
        """
        exported = exported_cameras(self._hass, self._settings)
        sent = False
        for entity_id in self._settings.removed:
            if entity_id in exported:
                continue
            if not self._send(entity_id, {"kind": "camera", "entity_id": entity_id, "removed": True}):
                return
            sent = True
        for entity_id in exported:
            # A camera on a saved list that no longer exists in Home Assistant is not described
            # (Kameraposti would offer a camera that can never take a photo).
            if (
                er.async_get(self._hass).async_get(entity_id) is None
                and self._hass.states.get(entity_id) is None
            ):
                if warn_missing and entity_id not in self._warned_missing:
                    self._warned_missing.add(entity_id)
                    _LOGGER.warning(
                        "Kameraposti: %s is on the camera list saved by an earlier version but no longer "
                        "exists in Home Assistant; save Configure -> Cameras to Kameraposti to report "
                        "your current cameras",
                        entity_id,
                    )
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
