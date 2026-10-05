"""Security system panel for Kameraposti (steelo 2026-10-05).

Mirrors Kameraposti's mode (away / home / disarmed) and arms or disarms it
over the integration's own connection. Arming needs no code; disarming
needs the disarm code set in Kameraposti (Anturit -> Turvajärjestelmä),
which Kameraposti itself checks -- Home Assistant never stores it.
"""

from __future__ import annotations

from homeassistant.components.alarm_control_panel import (
    AlarmControlPanelEntity,
    AlarmControlPanelEntityFeature,
    AlarmControlPanelState,
    CodeFormat,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, MANUFACTURER
from .coordinator import KameraportiCoordinator
from .mqtt_client import ConnectionState

_STATES = {
    "away": AlarmControlPanelState.ARMED_AWAY,
    "home": AlarmControlPanelState.ARMED_HOME,
    "disarmed": AlarmControlPanelState.DISARMED,
}


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([KameraportiSecurityPanel(coordinator)])


class KameraportiSecurityPanel(AlarmControlPanelEntity):
    """Kameraposti's security system: Poissa / Kotona / Purettu."""

    _attr_has_entity_name = True
    _attr_translation_key = "security"
    _attr_should_poll = False
    _attr_supported_features = (
        AlarmControlPanelEntityFeature.ARM_AWAY | AlarmControlPanelEntityFeature.ARM_HOME
    )
    _attr_code_format = CodeFormat.NUMBER
    _attr_code_arm_required = False

    def __init__(self, coordinator: KameraportiCoordinator) -> None:
        self._coordinator = coordinator
        self._attr_unique_id = f"kameraposti:{coordinator.customer_id}:security"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"{coordinator.customer_id}:security")},
            manufacturer=MANUFACTURER,
            name=f"Kameraposti {coordinator.customer_id}",
        )

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(self.hass, self._coordinator.signal_security, self._refresh)
        )

    @callback
    def _refresh(self) -> None:
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return self._coordinator.connection_state == ConnectionState.CONNECTED

    @property
    def alarm_state(self) -> AlarmControlPanelState | None:
        mode = self._coordinator.security_mode
        return _STATES.get(mode) if mode is not None else None

    async def async_alarm_disarm(self, code: str | None = None) -> None:
        await self._coordinator.async_set_security_mode("disarmed", code)

    async def async_alarm_arm_away(self, code: str | None = None) -> None:
        await self._coordinator.async_set_security_mode("away", None)

    async def async_alarm_arm_home(self, code: str | None = None) -> None:
        await self._coordinator.async_set_security_mode("home", None)
