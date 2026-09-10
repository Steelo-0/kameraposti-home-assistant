"""Setup / unload / reload lifecycle tests (contract section 21).

The MQTT client class is replaced with a fresh AsyncMock() per
construction (never the same shared instance) so that "reload leaves
exactly one active client" can be verified precisely: the OLD instance
must have been stopped, the NEW instance must be the only one still
running.
"""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, DOMAIN

DATA = {CONF_CUSTOMER_ID: 3, CONF_USERNAME: "rk-3-abc", CONF_PASSWORD: "secret"}


@pytest.fixture
def mqtt_client_instances() -> Generator[list[AsyncMock]]:
    instances: list[AsyncMock] = []

    def _construct(*args: object, **kwargs: object) -> AsyncMock:
        instance = AsyncMock()
        instances.append(instance)
        return instance

    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", side_effect=_construct):
        yield instances


async def test_setup_entry_starts_exactly_one_client(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    entry = MockConfigEntry(domain=DOMAIN, data=DATA)
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    assert len(mqtt_client_instances) == 1
    assert mqtt_client_instances[0].async_start.await_count == 1
    assert entry.entry_id in hass.data[DOMAIN]


async def test_unload_entry_stops_the_client_and_forgets_the_coordinator(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """L. Unload: disconnect, tasks/timers stopped, no lingering coordinator."""
    entry = MockConfigEntry(domain=DOMAIN, data=DATA)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.NOT_LOADED
    assert mqtt_client_instances[0].async_stop.await_count == 1
    assert entry.entry_id not in hass.data.get(DOMAIN, {})


async def test_reload_leaves_exactly_one_active_client(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """M. Reload: exactly one active MQTT connection afterwards, never two."""
    entry = MockConfigEntry(domain=DOMAIN, data=DATA)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert len(mqtt_client_instances) == 1
    first_client = mqtt_client_instances[0]

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    assert len(mqtt_client_instances) == 2
    second_client = mqtt_client_instances[1]

    # The old client was torn down exactly once...
    assert first_client.async_stop.await_count == 1
    # ...and only the new client is left running.
    assert second_client.async_start.await_count == 1
    assert second_client.async_stop.await_count == 0


async def test_updating_entry_data_triggers_a_reload_via_update_listener(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """Reauth flow updates entry.data and relies on this listener to reconnect
    with the new password -- exercise that wiring directly."""
    entry = MockConfigEntry(domain=DOMAIN, data=DATA)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    hass.config_entries.async_update_entry(entry, data={**DATA, CONF_PASSWORD: "rotated"})
    await hass.async_block_till_done()

    assert len(mqtt_client_instances) == 2
    assert mqtt_client_instances[0].async_stop.await_count == 1
    assert mqtt_client_instances[1].async_start.await_count == 1
