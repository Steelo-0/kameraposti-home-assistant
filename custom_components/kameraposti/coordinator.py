"""Runtime coordinator for one Kameraposti config entry.

Owns the MQTT client and the dedup cache, and is the single place that
turns a validated, non-duplicate Detection into Home Assistant state:
per-camera state dicts, dynamic device/entity discovery signals, and the
``kameraposti_detection`` event (contract section 17 -- dedup check,
parse+validate, ensure device/entities exist, update state, fire event,
all as one logical unit of work per incoming message).

1.5.0: the account's camera roster (customers/<id>/cameras) is the
authority on which cameras exist and what they are called -- listed cameras
get their device, renamed with the roster, and a camera that leaves a newer
roster loses its device (and so its entities) from the registries, including
stale devices from earlier runs. The retained roster arrives again on every
reconnect and restart, so removals happen only when its generated_at is
strictly newer than the last applied one (stored per config entry). A detection for an unlisted camera still
creates it as before (servers that publish no roster). Each camera's latest
photo (customers/<id>/cameras/<camera>/latest) is kept here for the image
platform.

Not a homeassistant.helpers.update_coordinator.DataUpdateCoordinator --
this integration is push-based (MQTT), not polling, so the usual
poll-and-refresh coordinator pattern does not apply. Sensors instead
listen for per-entry/per-camera dispatcher signals sent from here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
from dataclasses import dataclass
from datetime import datetime

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.storage import Store

from .const import (
    CONF_EXPORTED_ENTITIES,
    DEFAULT_CAMERA_NAME_TEMPLATE,
    DOMAIN,
    EVENT_DETECTION,
    LATEST_PHOTO_TOPIC_PATTERN,
    MANUFACTURER,
    MODEL,
    ROSTER_STORE_KEY_TEMPLATE,
    ROSTER_STORE_VERSION,
    ROSTER_TOPIC_PATTERN,
    SECURITY_COMMAND_TOPIC_TEMPLATE,
    SECURITY_ERRORS,
    SECURITY_MODES,
    SECURITY_RESULT_TOPIC_TEMPLATE,
    SECURITY_STATE_TOPIC_TEMPLATE,
    SIGNAL_CAMERA_PHOTO,
    SIGNAL_CAMERA_REMOVED,
    SIGNAL_CAMERA_UPDATE,
    SIGNAL_LATEST_PHOTO,
    SIGNAL_NEW_CAMERA,
    SIGNAL_SECURITY,
)
from .dedup import EventDedupCache
from .models import (
    CameraRoster,
    Detection,
    DetectionRejected,
    LatestPhoto,
    LatestPhotoRejected,
    RosterRejected,
    parse_detection,
    parse_latest_photo,
    parse_roster,
)
from .mqtt_client import ConnectionState, KameraportiMqttClient
from .sensor_export import KameraportiSensorExporter

_LOGGER = logging.getLogger(__name__)

# How long arming/disarming waits for Kameraposti's answer.
SECURITY_RESULT_TIMEOUT_SECONDS = 10

# Camera device identifier "<customer_id>:<camera_id>" (the security system's
# device is "<customer_id>:security" and never matches).
_CAMERA_IDENTIFIER = re.compile(r"(?P<customer_id>[0-9]+):(?P<camera_id>[0-9]+)")


def roster_store(hass: HomeAssistant, entry_id: str) -> Store[dict[str, str]]:
    """Where one config entry keeps the generated_at of its newest applied roster."""
    return Store(hass, ROSTER_STORE_VERSION, ROSTER_STORE_KEY_TEMPLATE.format(entry_id=entry_id))


@dataclass(slots=True)
class CameraState:
    """Latest known detection state for one camera_id (contract section 14)."""

    camera_id: int
    label: str | None = None
    confidence: float | None = None
    last_detection_time: datetime | None = None
    # 1.5.0: the camera's name on the roster (None = not named / no roster).
    name: str | None = None
    # 1.5.0: the camera's latest photo (None = no photo).
    latest_photo: LatestPhoto | None = None


class KameraportiCoordinator:
    """Owns the MQTT connection for one config entry and applies its detections."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        *,
        host: str,
        customer_id: int,
        username: str,
        password: str,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.customer_id = customer_id
        self.cameras: dict[int, CameraState] = {}
        self.connection_state: ConnectionState = ConnectionState.CONNECTING
        # steelo 2026-10-05: Kamerapostin turvajärjestelmän tila (None = ei vielä tiedossa).
        self.security_mode: str | None = None
        self._security_pending: dict[str, asyncio.Future[dict]] = {}
        self._security_state_topic = SECURITY_STATE_TOPIC_TEMPLATE.format(customer_id=customer_id)
        self._security_result_topic = SECURITY_RESULT_TOPIC_TEMPLATE.format(customer_id=customer_id)
        self._security_command_topic = SECURITY_COMMAND_TOPIC_TEMPLATE.format(customer_id=customer_id)
        # 1.5.0: camera ids on the last roster (None = no roster received), and
        # photos of cameras the roster does not list (yet) -- shown as soon as
        # the camera appears, never a reason to create the camera.
        self._roster_camera_ids: set[int] | None = None
        self._unlisted_photos: dict[int, LatestPhoto] = {}
        self._roster_store = roster_store(hass, entry.entry_id)
        # generated_at of the newest roster applied with removals (None = none yet).
        self._roster_applied_at: datetime | None = None

        self._dedup = EventDedupCache()
        self._client = KameraportiMqttClient(
            hass,
            host=host,
            customer_id=customer_id,
            username=username,
            password=password,
            on_message=self._handle_message,
            on_state_change=self._handle_state_change,
        )
        self._exporter = KameraportiSensorExporter(
            hass,
            customer_id=customer_id,
            entity_ids=list(entry.options.get(CONF_EXPORTED_ENTITIES, [])),
            publish=self._client.publish,
        )

    @property
    def signal_new_camera(self) -> str:
        """Dispatcher signal fired the first time a camera_id is ever seen."""
        return f"{SIGNAL_NEW_CAMERA}_{self.entry.entry_id}"

    @property
    def signal_security(self) -> str:
        """Dispatcher signal for the security panel (mode or availability changed)."""
        return f"{SIGNAL_SECURITY}_{self.entry.entry_id}"

    @property
    def signal_camera_removed(self) -> str:
        """Dispatcher signal fired when the roster drops a camera (its device is removed)."""
        return f"{SIGNAL_CAMERA_REMOVED}_{self.entry.entry_id}"

    def signal_camera_update(self, camera_id: int) -> str:
        """Dispatcher signal fired on every subsequent update for a known camera_id."""
        return f"{SIGNAL_CAMERA_UPDATE}_{self.entry.entry_id}_{camera_id}"

    def signal_latest_photo(self, camera_id: int) -> str:
        """Dispatcher signal fired when a known camera's latest photo changes."""
        return f"{SIGNAL_LATEST_PHOTO}_{self.entry.entry_id}_{camera_id}"

    @property
    def signal_camera_photo(self) -> str:
        """Dispatcher signal fired when a known camera gets a photo after having none.

        The image platform creates a camera's latest-photo entity only then: a
        service that publishes no photos gets no (always unavailable) entity.
        """
        return f"{SIGNAL_CAMERA_PHOTO}_{self.entry.entry_id}"

    def camera_device_identifier(self, camera_id: int) -> tuple[str, str]:
        """The camera device's identifier, shared by all of the camera's entities."""
        return (DOMAIN, f"{self.customer_id}:{camera_id}")

    def camera_name(self, camera_id: int) -> str:
        """The roster's name for the camera, else "Riistakamera <id>"."""
        state = self.cameras.get(camera_id)
        if state is not None and state.name:
            return state.name
        return DEFAULT_CAMERA_NAME_TEMPLATE.format(camera_id=camera_id)

    def camera_device_info(self, camera_id: int) -> DeviceInfo:
        """DeviceInfo for every entity of one camera (they all join the same device)."""
        return DeviceInfo(
            identifiers={self.camera_device_identifier(camera_id)},
            manufacturer=MANUFACTURER,
            model=MODEL,
            name=self.camera_name(camera_id),
        )

    async def async_start(self) -> None:
        """Start the MQTT client (contract section 21/22 counterpart is async_stop)."""
        # Before any message can arrive: the retained roster comes right after connecting.
        self._roster_applied_at = await self._async_load_roster_applied_at()
        self._exporter.async_start()
        await self._client.async_start()

    async def async_stop(self) -> None:
        """Disconnect the MQTT client and stop any pending reconnect. Idempotent."""
        self._exporter.async_stop()
        await self._client.async_stop()

    @callback
    def _handle_state_change(self, state: ConnectionState) -> None:
        """Runs on the HA event loop (marshaled by KameraportiMqttClient)."""
        if state == self.connection_state:
            # Never log/dispatch on a repeated identical state -- a
            # broker that keeps refusing auth would otherwise flood the
            # log once per retry (contract section 12).
            return
        _LOGGER.info("Kameraposti MQTT connection state: %s -> %s", self.connection_state, state)
        self.connection_state = state
        async_dispatcher_send(self.hass, self.signal_security)

        if state == ConnectionState.CONNECTED:
            # Describe the exported sensors and send their current state on
            # every (re)connect -- nothing is buffered while disconnected.
            self._exporter.publish_snapshot()

        if state == ConnectionState.AUTH_FAILURE:
            # Proactively surface Home Assistant's own reauth flow
            # (contract section 20) rather than relying on the user to
            # notice a stuck "connecting" entry. The MQTT client keeps
            # retrying with backoff regardless -- reauth just gives the
            # user a fast path to fix a rotated/typo'd password.
            self.entry.async_start_reauth(self.hass)

    async def _async_load_roster_applied_at(self) -> datetime | None:
        data = await self._roster_store.async_load()
        raw = data.get("generated_at") if isinstance(data, dict) else None
        if not isinstance(raw, str):
            return None
        try:
            applied_at = datetime.fromisoformat(raw)
        except ValueError:
            return None
        return applied_at if applied_at.tzinfo is not None else None

    @callback
    def _handle_message(self, topic: str, payload: bytes) -> None:
        """Runs on the HA event loop (marshaled by KameraportiMqttClient).

        Implements contract section 17 steps 1-2: parse+validate, then
        dedup. Any contract violation is logged at debug and otherwise
        silently ignored -- never raises, never disconnects, never
        blocks (section 7).
        """
        if topic == self._security_state_topic:
            self._apply_security_state(payload)
            return
        if topic == self._security_result_topic:
            self._apply_security_result(payload)
            return
        if ROSTER_TOPIC_PATTERN.match(topic):
            self._handle_roster(topic, payload)
            return
        if LATEST_PHOTO_TOPIC_PATTERN.match(topic):
            self._handle_latest_photo(topic, payload)
            return

        try:
            detection = parse_detection(topic, payload, expected_customer_id=self.customer_id)
        except DetectionRejected as err:
            _LOGGER.debug("Ignoring invalid Kameraposti detection message on %s: %s", topic, err)
            return

        _LOGGER.debug(
            "Kameraposti received event_id=%s camera_id=%s label=%s",
            detection.event_id,
            detection.camera_id,
            detection.label,
        )

        if self._dedup.seen(detection.event_id):
            _LOGGER.debug("Kameraposti dedup rejected event_id=%s (duplicate)", detection.event_id)
            return

        _LOGGER.debug("Kameraposti dedup accepted event_id=%s", detection.event_id)

        self._apply_detection(detection)

    @callback
    def _handle_roster(self, topic: str, payload: bytes) -> None:
        try:
            roster = parse_roster(topic, payload, expected_customer_id=self.customer_id)
        except RosterRejected as err:
            _LOGGER.debug("Ignoring invalid Kameraposti camera roster on %s: %s", topic, err)
            return
        if roster is None:
            # A cleared retained roster is "no roster", not "no cameras" (that
            # is an empty list) -- keep the cameras as they are.
            _LOGGER.debug("Kameraposti camera roster cleared on %s, keeping the cameras", topic)
            return
        self._apply_roster(roster)

    @callback
    def _apply_roster(self, roster: CameraRoster) -> None:
        """Make the camera devices match the roster: remove (newer roster only), add, rename."""
        listed = {camera.camera_id: camera.name for camera in roster.cameras}
        newer = self._roster_applied_at is None or roster.generated_at > self._roster_applied_at
        _LOGGER.debug(
            "Kameraposti camera roster generated_at=%s (%s): %s",
            roster.generated_at.isoformat(),
            "newer, applying removals" if newer else "not newer, adding and renaming only",
            sorted(listed),
        )
        self._roster_camera_ids = set(listed)
        device_registry = dr.async_get(self.hass)

        if newer:
            self._remove_unlisted_cameras(device_registry, listed)
            self._roster_applied_at = roster.generated_at
            self.hass.async_create_task(
                self._roster_store.async_save({"generated_at": roster.generated_at.isoformat()}),
                name="kameraposti save roster generated_at",
            )

        for camera_id, name in listed.items():
            state = self.cameras.get(camera_id)
            if state is None:
                # The new entities' DeviceInfo carries the roster name, which also
                # renames a device left from an earlier run.
                self.cameras[camera_id] = self._new_camera_state(camera_id, name=name)
                async_dispatcher_send(self.hass, self.signal_new_camera, camera_id)
            elif state.name != name:
                state.name = name
                self._sync_device_name(device_registry, camera_id)

    @callback
    def _remove_unlisted_cameras(
        self, device_registry: dr.DeviceRegistry, listed: dict[int, str | None]
    ) -> None:
        # Every camera device of this entry that is not listed -- also a stale
        # one from an earlier run that this run never saw. Removing the entry
        # from the device removes the device and its entities (registry and
        # live entities); a device shared with another entry only loses ours.
        for device in dr.async_entries_for_config_entry(device_registry, self.entry.entry_id):
            camera_id = self._camera_id_of_device(device)
            if camera_id is not None and camera_id not in listed:
                _LOGGER.debug("Kameraposti removing camera_id=%s (not on the roster)", camera_id)
                device_registry.async_update_device(device.id, remove_config_entry_id=self.entry.entry_id)
        for camera_id in [camera_id for camera_id in self.cameras if camera_id not in listed]:
            del self.cameras[camera_id]
            async_dispatcher_send(self.hass, self.signal_camera_removed, camera_id)

    def _new_camera_state(self, camera_id: int, *, name: str | None = None) -> CameraState:
        # A photo that arrived before the roster listed the camera shows at once.
        return CameraState(
            camera_id=camera_id, name=name, latest_photo=self._unlisted_photos.pop(camera_id, None)
        )

    @callback
    def _handle_latest_photo(self, topic: str, payload: bytes) -> None:
        try:
            camera_id, photo = parse_latest_photo(topic, payload, expected_customer_id=self.customer_id)
        except LatestPhotoRejected as err:
            _LOGGER.debug("Ignoring invalid Kameraposti latest photo message on %s: %s", topic, err)
            return
        # Never log the URL: it is a signed capability for the photo.
        _LOGGER.debug(
            "Kameraposti latest photo camera_id=%s photo_id=%s",
            camera_id,
            photo.photo_id if photo is not None else None,
        )

        state = self.cameras.get(camera_id)
        if state is None:
            if photo is None:
                self._unlisted_photos.pop(camera_id, None)
            elif self._roster_camera_ids is not None and camera_id not in self._roster_camera_ids:
                # The roster decides which cameras exist: a photo never brings back
                # a removed camera, it waits for the roster to list the camera.
                self._unlisted_photos[camera_id] = photo
            else:
                # No roster (yet): the photo makes the camera known, as a detection does.
                self.cameras[camera_id] = CameraState(camera_id=camera_id, latest_photo=photo)
                async_dispatcher_send(self.hass, self.signal_new_camera, camera_id)
            return

        if state.latest_photo == photo:
            # A redelivered or reconnect copy of the same message: nothing to do.
            return
        had_photo = state.latest_photo is not None
        state.latest_photo = photo
        if photo is not None and not had_photo:
            async_dispatcher_send(self.hass, self.signal_camera_photo, camera_id)
        async_dispatcher_send(self.hass, self.signal_latest_photo(camera_id))

    @callback
    def _sync_device_name(self, device_registry: dr.DeviceRegistry, camera_id: int) -> None:
        # Only the integration's name: a name the user gave the device in Home
        # Assistant (name_by_user) still wins in the UI.
        device = device_registry.async_get_device(identifiers={self.camera_device_identifier(camera_id)})
        name = self.camera_name(camera_id)
        if device is not None and device.name != name:
            device_registry.async_update_device(device.id, name=name)

    def _camera_id_of_device(self, device: dr.DeviceEntry) -> int | None:
        """camera_id of one of this account's camera devices; None for any other device."""
        for domain, identifier in device.identifiers:
            if domain != DOMAIN:
                continue
            match = _CAMERA_IDENTIFIER.fullmatch(identifier)
            if match is not None and int(match.group("customer_id")) == self.customer_id:
                return int(match.group("camera_id"))
        return None

    @callback
    def _apply_security_state(self, payload: bytes) -> None:
        data = _json_object(payload)
        mode = data.get("mode") if data else None
        if mode not in SECURITY_MODES:
            _LOGGER.debug("Ignoring invalid Kameraposti security state")
            return
        self.security_mode = mode
        async_dispatcher_send(self.hass, self.signal_security)

    @callback
    def _apply_security_result(self, payload: bytes) -> None:
        data = _json_object(payload)
        request_id = data.get("request_id") if data else None
        future = self._security_pending.get(request_id) if isinstance(request_id, str) else None
        if future is not None and not future.done():
            future.set_result(data)

    async def async_set_security_mode(self, mode: str, code: str | None) -> None:
        """Arm or disarm Kameraposti (disarming needs the code set in Kameraposti).

        Raises HomeAssistantError (translated) when not connected, when
        Kameraposti does not answer, or when it refuses (wrong code, locked,
        no code set).
        """
        request_id = secrets.token_hex(8)
        command: dict[str, str] = {"mode": mode, "request_id": request_id}
        if code:
            command["code"] = code
        future: asyncio.Future[dict] = self.hass.loop.create_future()
        self._security_pending[request_id] = future
        try:
            if not self._client.publish(self._security_command_topic, json.dumps(command), False):
                raise HomeAssistantError(translation_domain=DOMAIN, translation_key="not_connected")
            try:
                result = await asyncio.wait_for(future, SECURITY_RESULT_TIMEOUT_SECONDS)
            except TimeoutError as err:
                raise HomeAssistantError(translation_domain=DOMAIN, translation_key="no_response") from err
        finally:
            self._security_pending.pop(request_id, None)

        if not result.get("ok"):
            error = result.get("error")
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key=error if error in SECURITY_ERRORS else "refused",
            )
        if result.get("mode") in SECURITY_MODES:
            self.security_mode = result["mode"]
            async_dispatcher_send(self.hass, self.signal_security)

    @callback
    def _apply_detection(self, detection: Detection) -> None:
        """Contract section 17 steps 3-7: ensure entities, update state, fire event."""
        is_new_camera = detection.camera_id not in self.cameras

        _LOGGER.debug(
            "Kameraposti updating camera state event_id=%s camera_id=%s label=%s",
            detection.event_id,
            detection.camera_id,
            detection.label,
        )

        if is_new_camera:
            self.cameras[detection.camera_id] = self._new_camera_state(detection.camera_id)
        state = self.cameras[detection.camera_id]
        state.label = detection.label
        state.confidence = detection.confidence
        state.last_detection_time = detection.timestamp

        if is_new_camera:
            # The platforms create the device + entities on this signal
            # and immediately reflect the state already stored above --
            # no separate "update" signal needed for the very first event.
            async_dispatcher_send(self.hass, self.signal_new_camera, detection.camera_id)
        else:
            async_dispatcher_send(self.hass, self.signal_camera_update(detection.camera_id))

        _LOGGER.debug(
            "Kameraposti firing %s event_id=%s camera_id=%s",
            EVENT_DETECTION,
            detection.event_id,
            detection.camera_id,
        )

        self.hass.bus.async_fire(
            EVENT_DETECTION,
            {
                "event_id": detection.event_id,
                # Not part of the locked backend payload -- added here
                # only because it can be useful in automations spanning
                # multiple Kameraposti accounts (contract section 16).
                "customer_id": detection.customer_id,
                "camera_id": detection.camera_id,
                "label": detection.label,
                "confidence": detection.confidence,
                "timestamp": detection.timestamp.isoformat(),
            },
        )


def _json_object(payload: bytes) -> dict | None:
    try:
        data = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None
