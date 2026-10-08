"""Setup / unload / reload lifecycle tests (contract section 21).

The MQTT client class is replaced with a fresh AsyncMock() per
construction (never the same shared instance) so that "reload leaves
exactly one active client" can be verified precisely: the OLD instance
must have been stopped, the NEW instance must be the only one still
running.
"""

from __future__ import annotations

import logging
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


async def test_version_1_entry_migrates_to_the_own_broker_and_kp_login(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """2026-10-05: v1 pointed at the removed tailscale2.steels.me with an rk- login."""
    entry = MockConfigEntry(domain=DOMAIN, data=DATA, version=1)
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.version == 2
    assert entry.data[CONF_USERNAME] == "kp-3"
    assert entry.data["host"] == "kameraposti.fi"
    assert entry.data[CONF_PASSWORD] == "secret"


ENTRY_DATA = {"host": "cam.steels.me", CONF_CUSTOMER_ID: 3, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"}


@pytest.mark.parametrize(
    ("unique_id", "username", "expected"),
    [
        ("3", "kp-3", "kp-3"),
        (" KP-3 ", "kp-3", "kp-3"),
        ("kp-3", "kp-3", "kp-3"),
        ("KP-3-2 ", "kp-3-2", "kp-3-2"),
        ("kp-3-2", "kp-3-2", "kp-3-2"),
        (None, "kp-3", "kp-3"),
    ],
)
async def test_setup_rewrites_the_unique_id_to_the_login(
    hass: HomeAssistant,
    mqtt_client_instances: list[AsyncMock],
    unique_id: str | None,
    username: str,
    expected: str,
) -> None:
    """1.5.1: entries created before 1.5.1 carry the raw setup input as their unique id."""
    entry = MockConfigEntry(
        domain=DOMAIN, data={**ENTRY_DATA, CONF_USERNAME: username}, unique_id=unique_id, version=2
    )
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.unique_id == expected
    assert entry.state is ConfigEntryState.LOADED
    assert len(mqtt_client_instances) == 1


async def test_a_version_1_entry_gets_the_login_as_unique_id(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    entry = MockConfigEntry(domain=DOMAIN, data=DATA, unique_id="3", version=1)
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.unique_id == "kp-3"


def _same_login_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
        and r.name == "custom_components.kameraposti"
        and "kp-3" in r.getMessage()
    ]


async def test_two_entries_with_one_login_are_both_kept_and_warned_about(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock], caplog: pytest.LogCaptureFixture
) -> None:
    """Nothing is deleted: the older entry gets the login as its unique id, the newer one is left
    as it is, and the log says that the two disconnect each other."""
    older = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, unique_id="3", version=2, title="Vanha")
    newer = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, unique_id=" KP-3 ", version=2, title="Uusi")
    older.add_to_hass(hass)
    newer.add_to_hass(hass)

    assert await hass.config_entries.async_setup(older.entry_id)
    await hass.async_block_till_done()

    assert older.unique_id == "kp-3"
    assert newer.unique_id == " KP-3 "
    assert len(hass.config_entries.async_entries(DOMAIN)) == 2
    assert older.state is ConfigEntryState.LOADED
    assert newer.state is ConfigEntryState.LOADED
    warnings = _same_login_warnings(caplog)
    assert len(warnings) == 1
    assert "Vanha" in warnings[0]
    assert "Uusi" in warnings[0]
    assert "extra login" in warnings[0]


async def test_a_newer_duplicate_that_already_has_the_login_keeps_it(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock], caplog: pytest.LogCaptureFixture
) -> None:
    """Two entries never get the same unique id; once the duplicate is removed, the remaining
    entry takes the login on its next setup."""
    older = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, unique_id="3", version=2, title="Vanha")
    newer = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, unique_id="kp-3", version=2, title="Uusi")
    older.add_to_hass(hass)
    newer.add_to_hass(hass)

    assert await hass.config_entries.async_setup(older.entry_id)
    await hass.async_block_till_done()

    assert older.unique_id == "3"
    assert newer.unique_id == "kp-3"
    assert len(_same_login_warnings(caplog)) == 1
    assert "already in use" not in caplog.text

    assert await hass.config_entries.async_remove(newer.entry_id)
    await hass.async_block_till_done()
    assert await hass.config_entries.async_reload(older.entry_id)
    await hass.async_block_till_done()

    assert older.unique_id == "kp-3"
    assert older.state is ConfigEntryState.LOADED
