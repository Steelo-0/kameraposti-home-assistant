"""Design review TOP 3 (2026-10-18): a "Kameraposti connection" binary sensor shows in Home Assistant
whether the integration reaches Kameraposti, so an automation can react when Turva cannot hear the site.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_HOST, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.mqtt_client import ConnectionState


async def test_the_connection_sensor_follows_the_mqtt_connection(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: 3, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_start = AsyncMock()
    client.async_stop = AsyncMock()
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][entry.entry_id]

    registry = er.async_get(hass)
    entity_id = registry.async_get_entity_id("binary_sensor", DOMAIN, "kameraposti:3:connection")
    assert entity_id is not None
    assert registry.async_get(entity_id).entity_category is EntityCategory.DIAGNOSTIC
    state = hass.states.get(entity_id)
    assert state.state == "off"
    assert state.attributes["device_class"] == "connectivity"
    assert state.attributes["connection"] == "connecting"

    coordinator._handle_state_change(ConnectionState.CONNECTED)
    await hass.async_block_till_done()
    assert hass.states.get(entity_id).state == "on"

    coordinator._handle_state_change(ConnectionState.RECONNECTING)
    await hass.async_block_till_done()
    state = hass.states.get(entity_id)
    assert state.state == "off"
    assert state.attributes["connection"] == "reconnecting"
