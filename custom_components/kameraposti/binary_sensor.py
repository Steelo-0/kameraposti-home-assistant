"""Kameraposti connection (design review TOP 3, 2026-10-18).

A diagnostic connectivity sensor on the security system's device: on while the integration's own
MQTT connection to Kameraposti is up. Kameraposti notices the other side (no reports from this Home
Assistant for an hour); this one lets a Home Assistant automation react on the site itself.
"""

from __future__ import annotations

from homeassistant.components.binary_sensor import BinarySensorDeviceClass, BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, MANUFACTURER
from .coordinator import KameraportiCoordinator
from .mqtt_client import ConnectionState


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([KameraportiConnection(coordinator)])


class KameraportiConnection(BinarySensorEntity):
    """On while Home Assistant is connected to Kameraposti."""

    _attr_has_entity_name = True
    _attr_translation_key = "connection"
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_should_poll = False

    def __init__(self, coordinator: KameraportiCoordinator) -> None:
        self._coordinator = coordinator
        self._attr_unique_id = f"kameraposti:{coordinator.customer_id}:connection"
        # The same device as the security system panel (alarm_control_panel.py).
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"{coordinator.customer_id}:security")},
            manufacturer=MANUFACTURER,
            name=f"Kameraposti {coordinator.customer_id}",
        )

    async def async_added_to_hass(self) -> None:
        # The coordinator signals every connection state change on signal_security.
        self.async_on_remove(
            async_dispatcher_connect(self.hass, self._coordinator.signal_security, self._refresh)
        )

    @callback
    def _refresh(self) -> None:
        self.async_write_ha_state()

    @property
    def is_on(self) -> bool:
        return self._coordinator.connection_state == ConnectionState.CONNECTED

    @property
    def extra_state_attributes(self) -> dict[str, str]:
        return {"connection": str(self._coordinator.connection_state)}
