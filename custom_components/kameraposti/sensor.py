"""Sensor platform for Kameraposti: 3 entities per detected camera.

Cameras are discovered dynamically (contract section 15) -- camera_id is
not known at config-flow time, only once the first detection for it
arrives. Unique IDs are fully deterministic
(``kameraposti:{customer_id}:{camera_id}:{kind}``), so a Home Assistant
reload never creates duplicate entities for a camera_id already seen in
an earlier run of this same config entry.

V1 is deliberately generic (contract section 14): one "last detection"
label sensor, one confidence sensor, one last-detection-time sensor.
No per-label binary_sensor -- new labels (species, etc.) must never
require a Home Assistant-side schema change.
"""

from __future__ import annotations

import logging

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, MANUFACTURER, MODEL
from .coordinator import CameraState, KameraportiCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Kameraposti sensors, adding new ones as new cameras appear."""
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][entry.entry_id]

    known_cameras: set[int] = set()

    @callback
    def _add_camera(camera_id: int) -> None:
        if camera_id in known_cameras:
            # Guards against ever creating a duplicate set of entities
            # for the same camera_id (contract section 15) -- a
            # dispatcher signal could in principle be re-sent, entity
            # creation itself must not be re-triggerable.
            return
        known_cameras.add(camera_id)
        async_add_entities(
            [
                KameraportiLastDetectionSensor(coordinator, camera_id),
                KameraportiConfidenceSensor(coordinator, camera_id),
                KameraportiLastDetectionTimeSensor(coordinator, camera_id),
            ]
        )

    entry.async_on_unload(async_dispatcher_connect(hass, coordinator.signal_new_camera, _add_camera))

    # Entities for cameras the coordinator already knows about (e.g. a
    # detection arrived between coordinator start and this platform
    # finishing setup) -- do not wait for a second event to surface them.
    for camera_id in list(coordinator.cameras):
        _add_camera(camera_id)


class _KameraportiCameraSensorBase(SensorEntity):
    """Shared device-linking + live-update behaviour for the 3 sensors."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, coordinator: KameraportiCoordinator, camera_id: int) -> None:
        self._coordinator = coordinator
        self._camera_id = camera_id
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"{coordinator.customer_id}:{camera_id}")},
            manufacturer=MANUFACTURER,
            model=MODEL,
            name=f"Kameraposti Camera {camera_id}",
        )

    @property
    def _camera_state(self) -> CameraState | None:
        return self._coordinator.cameras.get(self._camera_id)

    async def async_added_to_hass(self) -> None:
        """Subscribe to this camera's update signal for the entity's lifetime."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                self._coordinator.signal_camera_update(self._camera_id),
                self._handle_update,
            )
        )

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()


class KameraportiLastDetectionSensor(_KameraportiCameraSensorBase):
    """Most recent detection label for this camera."""

    _attr_translation_key = "last_detection"

    def __init__(self, coordinator: KameraportiCoordinator, camera_id: int) -> None:
        super().__init__(coordinator, camera_id)
        self._attr_unique_id = f"{DOMAIN}:{coordinator.customer_id}:{camera_id}:last_detection"

    @property
    def native_value(self) -> str | None:
        state = self._camera_state
        return state.label if state is not None else None


class KameraportiConfidenceSensor(_KameraportiCameraSensorBase):
    """Confidence (0.0-1.0) of the most recent detection for this camera."""

    _attr_translation_key = "detection_confidence"

    def __init__(self, coordinator: KameraportiCoordinator, camera_id: int) -> None:
        super().__init__(coordinator, camera_id)
        self._attr_unique_id = f"{DOMAIN}:{coordinator.customer_id}:{camera_id}:confidence"

    @property
    def native_value(self) -> float | None:
        state = self._camera_state
        return state.confidence if state is not None else None


class KameraportiLastDetectionTimeSensor(_KameraportiCameraSensorBase):
    """Timestamp (image captured_at) of the most recent detection for this camera."""

    _attr_translation_key = "last_detection_time"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(self, coordinator: KameraportiCoordinator, camera_id: int) -> None:
        super().__init__(coordinator, camera_id)
        self._attr_unique_id = f"{DOMAIN}:{coordinator.customer_id}:{camera_id}:last_detection_time"

    @property
    def native_value(self):
        state = self._camera_state
        return state.last_detection_time if state is not None else None
