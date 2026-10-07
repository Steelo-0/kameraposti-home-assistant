"""Roster-driven camera devices (1.5.0, customers/<id>/cameras).

The roster is the authority on which cameras exist and what they are called:
each listed camera gets its device (named from the roster, renamed with it),
and a camera that leaves the roster loses its device and entities -- also a
stale device left over from an earlier Home Assistant run. A detection for a
camera that is not on the roster still creates the camera as before, so
servers that publish no roster keep working.

The MQTT client is mocked out (as in test_sensor.py); messages are fed to
coordinator._handle_message, exactly what the client invokes on the HA loop.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, DOMAIN, EVENT_DETECTION
from custom_components.kameraposti.coordinator import KameraportiCoordinator

CUSTOMER_ID = 3
ROSTER_TOPIC = f"customers/{CUSTOMER_ID}/cameras"


# Each roster defaults to a newer generated_at than the last, as the server's
# publisher does on every change; only a strictly newer roster removes cameras.
_GENERATED_AT = itertools.count()


def _roster(*cameras: tuple[int, str], generated_at: str | None = None) -> bytes:
    if generated_at is None:
        generated_at = (
            datetime(2026, 10, 7, 4, tzinfo=UTC) + timedelta(seconds=next(_GENERATED_AT))
        ).isoformat()
    return json.dumps(
        {
            "schema_version": 1,
            "generated_at": generated_at,
            "cameras": [{"camera_id": camera_id, "name": name} for camera_id, name in cameras],
        }
    ).encode()


def _detection(camera_id: int, event_id: str = "01A") -> tuple[str, bytes]:
    payload = {
        "schema_version": 1,
        "event_id": event_id,
        "camera_id": camera_id,
        "label": "animal",
        "confidence": 0.9,
        "timestamp": "2026-09-09T18:12:42+00:00",
    }
    return f"customers/{CUSTOMER_ID}/detections/{camera_id}", json.dumps(payload).encode()


@pytest.fixture
def mock_mqtt_client() -> AsyncGenerator[None]:
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        instance = mock_cls.return_value
        instance.async_start = AsyncMock()
        instance.async_stop = AsyncMock()
        yield


@pytest.fixture
async def setup_entry(hass: HomeAssistant, mock_mqtt_client: None) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


def _coordinator(hass: HomeAssistant, entry: MockConfigEntry) -> KameraportiCoordinator:
    return hass.data[DOMAIN][entry.entry_id]


def _camera_device(hass: HomeAssistant, camera_id: int) -> dr.DeviceEntry | None:
    return dr.async_get(hass).async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:{camera_id}")})


def _camera_entity_ids(hass: HomeAssistant, camera_id: int) -> set[str]:
    prefix = f"{DOMAIN}:{CUSTOMER_ID}:{camera_id}:"
    return {
        e.entity_id for e in er.async_get(hass).entities.values() if (e.unique_id or "").startswith(prefix)
    }


async def test_roster_creates_a_named_device_per_camera_before_any_detection(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    _coordinator(hass, setup_entry)._handle_message(
        ROSTER_TOPIC, _roster((12, "Pihakamera"), (35, "Riistapolku"))
    )
    await hass.async_block_till_done()

    piha = _camera_device(hass, 12)
    polku = _camera_device(hass, 35)
    assert piha is not None and piha.name == "Pihakamera"
    assert polku is not None and polku.name == "Riistapolku"
    assert piha.manufacturer == "Kameraposti"
    assert piha.model == "Riistakamera"
    # The detection sensors exist (no detection yet -> unknown).
    state = hass.states.get("sensor.pihakamera_last_detection")
    assert state is not None
    assert state.state == "unknown"


async def test_roster_rename_updates_the_device_name(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()

    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Navetta")))
    await hass.async_block_till_done()

    device = _camera_device(hass, 12)
    assert device is not None
    assert device.name == "Navetta"
    # The entity keeps its id; its friendly name follows the device.
    state = hass.states.get("sensor.pihakamera_last_detection")
    assert state is not None
    assert state.attributes["friendly_name"] == "Navetta Last detection"


async def test_roster_renames_a_device_left_from_an_earlier_run_and_keeps_the_users_name(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    device_registry = dr.async_get(hass)
    old = device_registry.async_get_or_create(
        config_entry_id=setup_entry.entry_id,
        identifiers={(DOMAIN, f"{CUSTOMER_ID}:12")},
        name="Riistakamera 12",
    )
    device_registry.async_update_device(old.id, name_by_user="Oma nimi")

    _coordinator(hass, setup_entry)._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()

    device = _camera_device(hass, 12)
    assert device.id == old.id
    assert device.name == "Pihakamera"
    assert device.name_by_user == "Oma nimi"


async def test_roster_names_a_camera_first_seen_in_a_detection(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(*_detection(12))
    await hass.async_block_till_done()
    assert _camera_device(hass, 12).name == "Riistakamera 12"

    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()

    assert _camera_device(hass, 12).name == "Pihakamera"
    assert hass.states.get("sensor.riistakamera_12_last_detection").state == "animal"


async def test_camera_that_leaves_the_roster_loses_its_device_and_entities(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera"), (35, "Riistapolku")))
    await hass.async_block_till_done()
    removed_entity_ids = _camera_entity_ids(hass, 35)
    assert removed_entity_ids

    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()

    assert _camera_device(hass, 35) is None
    assert _camera_entity_ids(hass, 35) == set()
    assert all(hass.states.get(entity_id) is None for entity_id in removed_entity_ids)
    assert 35 not in coordinator.cameras
    # The camera that stayed is untouched.
    assert _camera_device(hass, 12) is not None
    assert _camera_entity_ids(hass, 12)


async def test_stale_device_from_an_earlier_run_is_removed_but_the_security_device_stays(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    """The coordinator starts empty after a restart; the registry still holds the old cameras."""
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)
    stale = device_registry.async_get_or_create(
        config_entry_id=setup_entry.entry_id,
        identifiers={(DOMAIN, f"{CUSTOMER_ID}:99")},
        name="Riistakamera 99",
    )
    entity_registry.async_get_or_create(
        "sensor",
        DOMAIN,
        f"{DOMAIN}:{CUSTOMER_ID}:99:last_detection",
        config_entry=setup_entry,
        device_id=stale.id,
    )
    security = device_registry.async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:security")})
    assert security is not None

    _coordinator(hass, setup_entry)._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()

    assert _camera_device(hass, 99) is None
    assert _camera_entity_ids(hass, 99) == set()
    assert device_registry.async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:security")}) is not None
    security_entity_id = er.async_get(hass).async_get_entity_id(
        "alarm_control_panel", DOMAIN, f"{DOMAIN}:{CUSTOMER_ID}:security"
    )
    assert security_entity_id is not None
    assert hass.states.get(security_entity_id) is not None


async def test_an_empty_camera_list_removes_every_camera_device(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera"), (35, "Riistapolku")))
    await hass.async_block_till_done()

    coordinator._handle_message(ROSTER_TOPIC, _roster())
    await hass.async_block_till_done()

    assert _camera_device(hass, 12) is None
    assert _camera_device(hass, 35) is None
    assert coordinator.cameras == {}


@pytest.mark.parametrize("payload", [b"", b"{not json", b'{"schema_version": 2, "cameras": []}'])
async def test_cleared_or_invalid_roster_removes_nothing(
    hass: HomeAssistant, setup_entry: MockConfigEntry, payload: bytes
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()

    coordinator._handle_message(ROSTER_TOPIC, payload)
    await hass.async_block_till_done()

    assert _camera_device(hass, 12) is not None
    assert _camera_device(hass, 12).name == "Pihakamera"
    assert 12 in coordinator.cameras


async def test_roster_for_another_account_is_ignored(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()

    coordinator._handle_message("customers/4/cameras", _roster())
    await hass.async_block_till_done()

    assert _camera_device(hass, 12) is not None


async def test_detection_for_a_camera_not_on_the_roster_still_creates_it(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    """Backwards compatible: servers without a roster, or a roster that lags behind."""
    events = []
    hass.bus.async_listen(EVENT_DETECTION, events.append)
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()

    coordinator._handle_message(*_detection(40))
    await hass.async_block_till_done()

    device = _camera_device(hass, 40)
    assert device is not None
    assert device.name == "Riistakamera 40"
    assert hass.states.get("sensor.riistakamera_40_last_detection").state == "animal"
    assert [e.data["camera_id"] for e in events] == [40]


async def test_camera_back_on_the_roster_gets_its_entities_again(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera"), (35, "Riistapolku")))
    await hass.async_block_till_done()
    entity_count = len(_camera_entity_ids(hass, 35))

    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera")))
    await hass.async_block_till_done()
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera"), (35, "Riistapolku")))
    await hass.async_block_till_done()

    assert _camera_device(hass, 35) is not None
    assert len(_camera_entity_ids(hass, 35)) == entity_count
    assert hass.states.get("sensor.riistapolku_last_detection") is not None


# -- M-1: only a strictly newer roster removes cameras -----------------------
# The roster is retained, so the same (or, in a race, an older) roster arrives
# again on every reconnect and Home Assistant restart. Removing on that would
# delete a camera created from detections (or added while the server's roster
# job failed) on each restart, and the next detection would create it again.

T1 = "2026-10-07T09:00:00+00:00"
T2 = "2026-10-07T10:00:00+00:00"
T0 = "2026-10-07T08:00:00+00:00"


async def test_same_retained_roster_after_a_restart_keeps_a_detection_created_camera(
    hass: HomeAssistant, setup_entry: MockConfigEntry, hass_storage: dict
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera"), generated_at=T1))
    await hass.async_block_till_done()
    coordinator._handle_message(*_detection(40))
    await hass.async_block_till_done()
    assert _camera_device(hass, 40) is not None
    # The applied roster time is stored per config entry.
    assert hass_storage[f"{DOMAIN}.roster.{setup_entry.entry_id}"]["data"] == {"generated_at": T1}

    assert await hass.config_entries.async_reload(setup_entry.entry_id)
    await hass.async_block_till_done()
    reloaded = _coordinator(hass, setup_entry)
    reloaded._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera"), generated_at=T1))
    await hass.async_block_till_done()

    device = _camera_device(hass, 40)
    assert device is not None
    assert _camera_entity_ids(hass, 40)
    assert _camera_device(hass, 12) is not None


async def test_newer_roster_without_a_detection_created_camera_removes_it(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera"), generated_at=T1))
    coordinator._handle_message(*_detection(40))
    await hass.async_block_till_done()

    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Pihakamera"), generated_at=T2))
    await hass.async_block_till_done()

    assert _camera_device(hass, 40) is None
    assert 40 not in coordinator.cameras


async def test_older_or_equal_roster_never_removes_but_still_adds_and_renames(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator = _coordinator(hass, setup_entry)
    coordinator._handle_message(
        ROSTER_TOPIC, _roster((12, "Pihakamera"), (35, "Riistapolku"), generated_at=T1)
    )
    await hass.async_block_till_done()

    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Navetta"), (50, "Uusi"), generated_at=T0))
    coordinator._handle_message(ROSTER_TOPIC, _roster((12, "Navetta"), generated_at=T1))
    await hass.async_block_till_done()

    assert _camera_device(hass, 35) is not None
    assert 35 in coordinator.cameras
    assert _camera_device(hass, 12).name == "Navetta"
    assert _camera_device(hass, 50) is not None


async def test_removing_the_entry_removes_its_stored_roster_time(
    hass: HomeAssistant, setup_entry: MockConfigEntry, hass_storage: dict
) -> None:
    _coordinator(hass, setup_entry)._handle_message(
        ROSTER_TOPIC, _roster((12, "Pihakamera"), generated_at=T1)
    )
    await hass.async_block_till_done()
    key = f"{DOMAIN}.roster.{setup_entry.entry_id}"
    assert key in hass_storage

    assert await hass.config_entries.async_remove(setup_entry.entry_id)
    await hass.async_block_till_done()

    assert key not in hass_storage
