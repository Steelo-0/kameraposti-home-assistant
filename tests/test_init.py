"""Setup / unload / reload lifecycle tests (contract section 21).

The MQTT client class is replaced with a fresh AsyncMock() per
construction (never the same shared instance) so that "reload leaves
exactly one active client" can be verified precisely: the OLD instance
must have been stopped, the NEW instance must be the only one still
running.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import WebSocketGenerator

from custom_components.kameraposti import async_migrate_entry
from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_EXPORTED_ENTITIES, CONF_HOST, DOMAIN

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


async def test_saving_the_options_reloads_the_entry_exactly_once(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """1.12.1 (2026-10-19): an options save reloaded the entry twice -- Home Assistant's own reload
    plus the integration's update listener. Each reload is one new MQTT client."""
    entry = MockConfigEntry(domain=DOMAIN, data=DATA, version=2)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert len(mqtt_client_instances) == 1

    form = await hass.config_entries.options.async_init(entry.entry_id)
    saved = await hass.config_entries.options.async_configure(form["flow_id"], {CONF_EXPORTED_ENTITIES: []})
    await hass.async_block_till_done()

    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert len(mqtt_client_instances) == 2
    assert mqtt_client_instances[0].async_stop.await_count == 1
    assert mqtt_client_instances[1].async_start.await_count == 1


async def test_reauth_reconnects_with_the_new_password_exactly_once(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """Reauth stores the password and Home Assistant reloads once (no second reload by a listener)."""
    entry = MockConfigEntry(domain=DOMAIN, data=DATA, version=2)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    result = await entry.start_reauth_flow(hass)
    with patch("custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock):
        done = await hass.config_entries.flow.async_configure(result["flow_id"], {CONF_PASSWORD: "rotated"})
        await hass.async_block_till_done()

    assert done["reason"] == "reauth_successful"
    assert entry.data[CONF_PASSWORD] == "rotated"
    assert len(mqtt_client_instances) == 2
    assert mqtt_client_instances[1].async_start.await_count == 1


async def test_entry_updates_during_setup_do_not_reload_it(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """Setup itself sets the login as unique id and drops the dead camera options: neither reloads."""
    entry = MockConfigEntry(
        domain=DOMAIN, data=DATA, version=2, unique_id="3", options={"hidden_cameras": [1]}
    )
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.unique_id != "3"
    assert "hidden_cameras" not in entry.options
    assert len(mqtt_client_instances) == 1


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


# 1.14.1 (Fable bug hunt H-M1): the unique ids were "kameraposti:<account>:..." and the device identifiers
# "<account>:...", so two entries with the same account number (the test and the production service, or
# two logins of one account) collided. They now carry the entry id; config entry version 2.2 moves the ids
# of an existing installation.

ROSTER = json.dumps(
    {
        "schema_version": 1,
        "generated_at": "2026-10-19T04:00:00+00:00",
        "cameras": [{"camera_id": 12, "name": "Pihakamera"}],
    }
).encode()


async def _set_up_with_roster(hass: HomeAssistant, entry: MockConfigEntry) -> None:
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    hass.data[DOMAIN][entry.entry_id]._handle_message("customers/3/cameras", ROSTER)
    await hass.async_block_till_done()


async def test_an_existing_installation_keeps_its_entity_ids_names_areas_and_devices(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """Every registry entry keeps its entity id (and so its history), the name the user gave it, and its
    device; the devices keep their area and name. Home Assistant creates no second entity for any of them."""
    entry = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, version=2, minor_version=1, unique_id="kp-3")
    entry.add_to_hass(hass)
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)
    area = ar.async_get(hass).async_create("Piha")
    camera = device_registry.async_get_or_create(
        config_entry_id=entry.entry_id, identifiers={(DOMAIN, "3:12")}, name="Pihakamera"
    )
    device_registry.async_update_device(camera.id, name_by_user="Oma kamera", area_id=area.id)
    security = device_registry.async_get_or_create(
        config_entry_id=entry.entry_id, identifiers={(DOMAIN, "3:security")}, name="Kameraposti 3"
    )
    entity_ids: dict[str, str] = {}
    for domain, suffix, device in (
        ("sensor", "12:last_detection", camera),
        ("sensor", "12:confidence", camera),
        ("sensor", "12:last_detection_time", camera),
        ("image", "12:latest_photo", camera),
        ("alarm_control_panel", "security", security),
        ("binary_sensor", "connection", security),
    ):
        registered = entity_registry.async_get_or_create(
            domain, DOMAIN, f"kameraposti:3:{suffix}", config_entry=entry, device_id=device.id
        )
        entity_ids[suffix] = registered.entity_id
    entity_ids["12:last_detection"] = entity_registry.async_update_entity(
        entity_ids["12:last_detection"], new_entity_id="sensor.pihan_havainto", name="Pihan havainto"
    ).entity_id

    await _set_up_with_roster(hass, entry)

    assert (entry.version, entry.minor_version) == (2, 2)
    assert {
        registry_entry.unique_id: registry_entry.entity_id
        for registry_entry in er.async_entries_for_config_entry(entity_registry, entry.entry_id)
    } == {f"kameraposti:{entry.entry_id}:{suffix}": entity_id for suffix, entity_id in entity_ids.items()}
    assert all(hass.states.get(entity_id) is not None for entity_id in entity_ids.values())
    assert entity_registry.async_get("sensor.pihan_havainto").name == "Pihan havainto"
    assert entity_registry.async_get("sensor.pihan_havainto").device_id == camera.id
    camera = device_registry.async_get(camera.id)
    assert camera.identifiers == {(DOMAIN, f"{entry.entry_id}:12")}
    assert (camera.name_by_user, camera.area_id) == ("Oma kamera", area.id)
    assert device_registry.async_get(security.id).identifiers == {(DOMAIN, f"{entry.entry_id}:security")}
    assert len(dr.async_entries_for_config_entry(device_registry, entry.entry_id)) == 2


async def test_two_entries_with_one_account_number_get_their_own_entities_and_devices(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """kp-3 on the test service and kp-3-2 on production: the second entry's security panel, connection
    sensor and camera entities were left out ("ID ... already exists - ignoring"), and one device was
    shared by both."""
    test = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, version=2, minor_version=2, unique_id="kp-3")
    prod = MockConfigEntry(
        domain=DOMAIN,
        data={**ENTRY_DATA, CONF_HOST: "turva.secsrv.fi", CONF_USERNAME: "kp-3-2"},
        version=2,
        minor_version=2,
        unique_id="kp-3-2",
    )
    for entry in (test, prod):
        entry.add_to_hass(hass)
        await _set_up_with_roster(hass, entry)

    entity_registry = er.async_get(hass)
    device_registry = dr.async_get(hass)
    suffixes = ("security", "connection", "12:last_detection", "12:confidence", "12:last_detection_time")
    for entry in (test, prod):
        entities = er.async_entries_for_config_entry(entity_registry, entry.entry_id)
        assert {e.unique_id for e in entities} == {f"kameraposti:{entry.entry_id}:{s}" for s in suffixes}
        assert all(hass.states.get(e.entity_id) is not None for e in entities)
        devices = dr.async_entries_for_config_entry(device_registry, entry.entry_id)
        assert {identifier for device in devices for identifier in device.identifiers} == {
            (DOMAIN, f"{entry.entry_id}:12"),
            (DOMAIN, f"{entry.entry_id}:security"),
        }
        assert all(device.config_entries == {entry.entry_id} for device in devices)


@pytest.mark.parametrize("prod_first", [False, True])
async def test_a_device_both_entries_shared_stays_with_the_entry_whose_entities_are_on_it(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock], prod_first: bool
) -> None:
    """As 1.14.0 left two entries with one account number: both linked to the security device, the panel
    the first entry's. That entry keeps the device and the panel; the other one only loses its link
    (removing nothing) and gets its own device and panel -- whichever entry is migrated first."""
    test = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, version=2, minor_version=1, unique_id="kp-3")
    prod = MockConfigEntry(
        domain=DOMAIN,
        data={**ENTRY_DATA, CONF_HOST: "turva.secsrv.fi", CONF_USERNAME: "kp-3-2"},
        version=2,
        minor_version=1,
        unique_id="kp-3-2",
    )
    test.add_to_hass(hass)
    prod.add_to_hass(hass)
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)
    shared = device_registry.async_get_or_create(
        config_entry_id=test.entry_id, identifiers={(DOMAIN, "3:security")}, name="Kameraposti 3"
    )
    device_registry.async_update_device(shared.id, add_config_entry_id=prod.entry_id)
    panel = entity_registry.async_get_or_create(
        "alarm_control_panel", DOMAIN, "kameraposti:3:security", config_entry=test, device_id=shared.id
    )

    for entry in (prod, test) if prod_first else (test, prod):
        assert await async_migrate_entry(hass, entry)
    assert await hass.config_entries.async_setup(test.entry_id)
    await hass.async_block_till_done()

    shared = device_registry.async_get(shared.id)
    assert shared.identifiers == {(DOMAIN, f"{test.entry_id}:security")}
    assert shared.config_entries == {test.entry_id}
    panel = entity_registry.async_get(panel.entity_id)
    assert (panel.unique_id, panel.device_id) == (f"kameraposti:{test.entry_id}:security", shared.id)
    prod_device = device_registry.async_get_device(identifiers={(DOMAIN, f"{prod.entry_id}:security")})
    assert prod_device is not None and prod_device.config_entries == {prod.entry_id}
    prod_panel = entity_registry.async_get_entity_id(
        "alarm_control_panel", DOMAIN, f"kameraposti:{prod.entry_id}:security"
    )
    assert entity_registry.async_get(prod_panel).device_id == prod_device.id
    assert hass.states.get(prod_panel) is not None


# 1.14.1 (bug hunt 2026-10-19 M-1): after 1.14.1 -> 1.14.0 -> 1.14.1 the devices 1.14.0 made with the account
# number in front ("<account>:security", "<account>:<camera>") stayed for good: the integration let the UI
# delete no device, and nothing dropped one left empty.


async def test_the_ui_deletes_a_left_over_device_but_not_one_in_use(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock], hass_ws_client: WebSocketGenerator
) -> None:
    assert await async_setup_component(hass, "config", {})
    entry = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, version=2, minor_version=2, unique_id="kp-3")
    entry.add_to_hass(hass)
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)
    left_over = device_registry.async_get_or_create(
        config_entry_id=entry.entry_id, identifiers={(DOMAIN, "3:security")}, name="Kameraposti 3"
    )
    duplicate = entity_registry.async_get_or_create(
        "alarm_control_panel", DOMAIN, "kameraposti:3:security", config_entry=entry, device_id=left_over.id
    )
    await _set_up_with_roster(hass, entry)
    security = device_registry.async_get_device(identifiers={(DOMAIN, f"{entry.entry_id}:security")})
    camera = device_registry.async_get_device(identifiers={(DOMAIN, f"{entry.entry_id}:12")})
    client = await hass_ws_client(hass)

    async def remove(device: dr.DeviceEntry) -> dict:
        await client.send_json_auto_id(
            {
                "type": "config/device_registry/remove_config_entry",
                "config_entry_id": entry.entry_id,
                "device_id": device.id,
            }
        )
        return await client.receive_json()

    for in_use in (security, camera):
        assert not (await remove(in_use))["success"]
        assert device_registry.async_get(in_use.id) is not None
    assert (await remove(left_over))["success"]
    assert device_registry.async_get(left_over.id) is None
    assert entity_registry.async_get(duplicate.entity_id) is None  # 1.14.0's duplicate went with it


async def test_setup_drops_its_link_to_a_left_over_device_without_its_entities(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """A device with the account number in front loses this entry's link once no entity of the entry is on
    it -- and goes, when it was only this entry's and no entity is on it. A device with an entity of this
    entry, or one that dropping the link would remove while another entry's entity is on it, stays."""
    entry = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, version=2, minor_version=2, unique_id="kp-3")
    other = MockConfigEntry(domain="muu")
    entry.add_to_hass(hass)
    other.add_to_hass(hass)
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)

    def left_over(identifier: str, *, shared: bool = False) -> dr.DeviceEntry:
        device = device_registry.async_get_or_create(
            config_entry_id=entry.entry_id, identifiers={(DOMAIN, identifier)}, name=identifier
        )
        if shared:
            device = device_registry.async_update_device(device.id, add_config_entry_id=other.entry_id)
        return device

    empty = left_over("3:12")
    with_ours = left_over("3:security")
    ours = entity_registry.async_get_or_create(
        "alarm_control_panel", DOMAIN, "kameraposti:3:security", config_entry=entry, device_id=with_ours.id
    )
    shared = left_over("3:7", shared=True)
    theirs = entity_registry.async_get_or_create(
        "sensor", "muu", "a", config_entry=other, device_id=shared.id
    )
    only_ours_with_theirs = left_over("3:8")
    stray = entity_registry.async_get_or_create(
        "sensor", "muu", "b", config_entry=other, device_id=only_ours_with_theirs.id
    )

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert device_registry.async_get(empty.id) is None
    assert device_registry.async_get(with_ours.id).config_entries == {entry.entry_id}
    assert entity_registry.async_get(ours.entity_id).device_id == with_ours.id
    assert device_registry.async_get(shared.id).config_entries == {other.entry_id}
    assert entity_registry.async_get(theirs.entity_id).device_id == shared.id
    assert device_registry.async_get(only_ours_with_theirs.id).config_entries == {entry.entry_id}
    assert entity_registry.async_get(stray.entity_id).device_id == only_ours_with_theirs.id


async def test_setup_drops_its_link_to_another_entrys_device_once_its_entities_moved_off(
    hass: HomeAssistant, mqtt_client_instances: list[AsyncMock]
) -> None:
    """Fable audit L-1: a device two entries with one account number shared went to one entry in the
    migration; the other entry's panel moved to its own device at setup, but its link stayed on the first
    one's device for good. It goes after setup, once none of the entry's entities is on the device."""
    test = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, version=2, minor_version=2, unique_id="kp-3")
    prod = MockConfigEntry(
        domain=DOMAIN,
        data={**ENTRY_DATA, CONF_HOST: "turva.secsrv.fi", CONF_USERNAME: "kp-3-2"},
        version=2,
        minor_version=2,
        unique_id="kp-3-2",
    )
    test.add_to_hass(hass)
    prod.add_to_hass(hass)
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)
    shared = device_registry.async_get_or_create(
        config_entry_id=test.entry_id,
        identifiers={(DOMAIN, f"{test.entry_id}:security")},
        name="Kameraposti 3",
    )
    device_registry.async_update_device(shared.id, add_config_entry_id=prod.entry_id)
    panels = {
        entry.entry_id: entity_registry.async_get_or_create(
            "alarm_control_panel",
            DOMAIN,
            f"kameraposti:{entry.entry_id}:security",
            config_entry=entry,
            device_id=shared.id,
        )
        for entry in (test, prod)
    }
    test_panel, prod_panel = panels[test.entry_id], panels[prod.entry_id]

    assert await hass.config_entries.async_setup(test.entry_id)
    await hass.async_block_till_done()

    assert device_registry.async_get(shared.id).config_entries == {test.entry_id}
    assert entity_registry.async_get(test_panel.entity_id).device_id == shared.id
    prod_device = device_registry.async_get_device(identifiers={(DOMAIN, f"{prod.entry_id}:security")})
    assert entity_registry.async_get(prod_panel.entity_id).device_id == prod_device.id
    assert hass.states.get(prod_panel.entity_id) is not None
