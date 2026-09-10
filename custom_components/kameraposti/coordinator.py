"""Runtime coordinator for one Kameraposti config entry.

Owns the MQTT client and the dedup cache, and is the single place that
turns a validated, non-duplicate Detection into Home Assistant state:
per-camera state dicts, dynamic device/entity discovery signals, and the
``kameraposti_detection`` event (contract section 17 -- dedup check,
parse+validate, ensure device/entities exist, update state, fire event,
all as one logical unit of work per incoming message).

Not a homeassistant.helpers.update_coordinator.DataUpdateCoordinator --
this integration is push-based (MQTT), not polling, so the usual
poll-and-refresh coordinator pattern does not apply. Sensors instead
listen for per-entry/per-camera dispatcher signals sent from here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import EVENT_DETECTION, SIGNAL_CAMERA_UPDATE, SIGNAL_NEW_CAMERA
from .dedup import EventDedupCache
from .models import Detection, DetectionRejected, parse_detection
from .mqtt_client import ConnectionState, KameraportiMqttClient

_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class CameraState:
    """Latest known detection state for one camera_id (contract section 14)."""

    camera_id: int
    label: str | None = None
    confidence: float | None = None
    last_detection_time: datetime | None = None


class KameraportiCoordinator:
    """Owns the MQTT connection for one config entry and applies its detections."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        *,
        customer_id: int,
        username: str,
        password: str,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.customer_id = customer_id
        self.cameras: dict[int, CameraState] = {}
        self.connection_state: ConnectionState = ConnectionState.CONNECTING

        self._dedup = EventDedupCache()
        self._client = KameraportiMqttClient(
            hass,
            customer_id=customer_id,
            username=username,
            password=password,
            on_message=self._handle_message,
            on_state_change=self._handle_state_change,
        )

    @property
    def signal_new_camera(self) -> str:
        """Dispatcher signal fired the first time a camera_id is ever seen."""
        return f"{SIGNAL_NEW_CAMERA}_{self.entry.entry_id}"

    def signal_camera_update(self, camera_id: int) -> str:
        """Dispatcher signal fired on every subsequent update for a known camera_id."""
        return f"{SIGNAL_CAMERA_UPDATE}_{self.entry.entry_id}_{camera_id}"

    async def async_start(self) -> None:
        """Start the MQTT client (contract section 21/22 counterpart is async_stop)."""
        await self._client.async_start()

    async def async_stop(self) -> None:
        """Disconnect the MQTT client and stop any pending reconnect. Idempotent."""
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

        if state == ConnectionState.AUTH_FAILURE:
            # Proactively surface Home Assistant's own reauth flow
            # (contract section 20) rather than relying on the user to
            # notice a stuck "connecting" entry. The MQTT client keeps
            # retrying with backoff regardless -- reauth just gives the
            # user a fast path to fix a rotated/typo'd password.
            self.entry.async_start_reauth(self.hass)

    @callback
    def _handle_message(self, topic: str, payload: bytes) -> None:
        """Runs on the HA event loop (marshaled by KameraportiMqttClient).

        Implements contract section 17 steps 1-2: parse+validate, then
        dedup. Any contract violation is logged at debug and otherwise
        silently ignored -- never raises, never disconnects, never
        blocks (section 7).
        """
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
    def _apply_detection(self, detection: Detection) -> None:
        """Contract section 17 steps 3-7: ensure entities, update state, fire event."""
        is_new_camera = detection.camera_id not in self.cameras

        _LOGGER.debug(
            "Kameraposti updating camera state event_id=%s camera_id=%s label=%s",
            detection.event_id,
            detection.camera_id,
            detection.label,
        )

        state = self.cameras.setdefault(detection.camera_id, CameraState(camera_id=detection.camera_id))
        state.label = detection.label
        state.confidence = detection.confidence
        state.last_detection_time = detection.timestamp

        if is_new_camera:
            # sensor.py creates the device + 3 entities on this signal
            # and immediately reflects the state already stored above --
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
