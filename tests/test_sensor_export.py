"""Sensor export tests (2026-10-05): selected Home Assistant entities become
Kameraposti sensors. The integration publishes a description to
kameraposti/<id>/anturit/<name>/config (Kameraposti creates or updates the
sensor) and each state change to kameraposti/<id>/anturit/<name> in the
simple format (leak/dry, smoke/clear, open/closed, motion, temperature).
On every (re)connect it describes the sensors again and sends their current
state (not motion), so a leak that started during a break is not lost;
Kameraposti drops repeated states itself.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.core import HassJob, HassJobType, HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.config_flow import EXPORTABLE_ENTITIES
from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_EXPORTED_ENTITIES, CONF_HOST, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.mqtt_client import ConnectionState
from custom_components.kameraposti.sensor_export import (
    KameraportiSensorExporter,
    event_for,
    kind_for,
    mqtt_name_for,
)

CUSTOMER_ID = 3


def _set(
    hass: HomeAssistant,
    entity_id: str,
    state: str,
    device_class: str | None,
    name: str,
    unit: str | None = None,
) -> None:
    attrs = {"friendly_name": name}
    if device_class is not None:
        attrs["device_class"] = device_class
    if unit is not None:
        attrs["unit_of_measurement"] = unit
    hass.states.async_set(entity_id, state, attrs)


def _recorder() -> tuple[list[tuple[str, str, bool]], object]:
    published: list[tuple[str, str, bool]] = []

    def publish(topic: str, payload: str, retain: bool) -> bool:
        published.append((topic, payload, retain))
        return True

    return published, publish


async def test_kind_follows_the_device_class(hass: HomeAssistant) -> None:
    cases = {
        ("binary_sensor.a", "moisture"): "leak",
        ("binary_sensor.b", "smoke"): "smoke",
        ("binary_sensor.c", "door"): "door",
        ("binary_sensor.d", "garage_door"): "door",
        ("binary_sensor.e", "opening"): "door",
        ("binary_sensor.f", "window"): "window",
        ("binary_sensor.g", "motion"): "motion",
        ("binary_sensor.h", "occupancy"): "motion",
        ("sensor.i", "temperature"): "temperature",
        ("binary_sensor.j", "battery"): None,
        ("sensor.k", "humidity"): None,
        ("binary_sensor.l", None): None,
    }
    for (entity_id, device_class), kind in cases.items():
        _set(hass, entity_id, "off", device_class, entity_id)
        assert kind_for(hass.states.get(entity_id)) == kind, entity_id


def test_states_map_to_simple_events() -> None:
    assert event_for("leak", "on") == "leak"
    assert event_for("leak", "off") == "dry"
    assert event_for("smoke", "on") == "smoke"
    assert event_for("smoke", "off") == "clear"
    assert event_for("door", "on") == "open"
    assert event_for("window", "off") == "closed"
    assert event_for("motion", "on") == "motion"
    assert event_for("motion", "off") is None
    assert event_for("temperature", "21.5") == "21.5"
    assert event_for("temperature", "-3") == "-3"
    assert event_for("temperature", "68", "°F") == "20.0"
    assert event_for("temperature", "21.5", "°C") == "21.5"
    assert event_for("temperature", "294", "K") is None
    for kind, state in [
        ("leak", "unavailable"),
        ("leak", "unknown"),
        ("temperature", "unknown"),
        ("temperature", "nan"),
    ]:
        assert event_for(kind, state) is None, (kind, state)


def test_topic_names_are_valid_and_bounded() -> None:
    assert mqtt_name_for("binary_sensor.kellari_vuoto") == "binary_sensor.kellari_vuoto"
    long_id = "binary_sensor." + "x" * 80
    name = mqtt_name_for(long_id)
    assert len(name) <= 64
    assert name != mqtt_name_for(long_id + "y")
    assert all(c.isalnum() or c in "_.-" for c in name)


async def test_exporter_describes_sensors_and_publishes_changes(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    _set(hass, "binary_sensor.liike", "off", "motion", "Pihan liike")
    _set(hass, "sensor.olohuone", "20.0", "temperature", "Olohuone")
    _set(hass, "binary_sensor.akku", "off", "battery", "Akku")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=["binary_sensor.kellari", "binary_sensor.liike", "sensor.olohuone", "binary_sensor.akku"],
        publish=publish,
    )
    exporter.async_start()

    exporter.publish_snapshot()
    configs = {topic: json.loads(payload) for topic, payload, _ in published if topic.endswith("/config")}
    assert configs == {
        "kameraposti/3/anturit/binary_sensor.kellari/config": {
            "name": "Kellarin vuoto",
            "kind": "leak",
            "format": "simple",
        },
        "kameraposti/3/anturit/binary_sensor.liike/config": {
            "name": "Pihan liike",
            "kind": "motion",
            "format": "simple",
        },
        "kameraposti/3/anturit/sensor.olohuone/config": {
            "name": "Olohuone",
            "kind": "temperature",
            "format": "simple",
        },
    }
    states = [(topic, payload) for topic, payload, _ in published if not topic.endswith("/config")]
    assert states == [
        ("kameraposti/3/anturit/binary_sensor.kellari", "dry"),
        ("kameraposti/3/anturit/sensor.olohuone", "20.0"),
    ]
    assert all(retain is False for _, _, retain in published)
    assert published.index(("kameraposti/3/anturit/binary_sensor.kellari", "dry", False)) > published.index(
        next(p for p in published if p[0] == "kameraposti/3/anturit/binary_sensor.kellari/config")
    )
    published.clear()

    freezer.tick(61)
    _set(hass, "binary_sensor.kellari", "on", "moisture", "Kellarin vuoto")
    _set(hass, "binary_sensor.liike", "on", "motion", "Pihan liike")
    _set(hass, "binary_sensor.liike", "off", "motion", "Pihan liike")
    _set(hass, "sensor.olohuone", "21.5", "temperature", "Olohuone")
    _set(hass, "sensor.olohuone", "unavailable", "temperature", "Olohuone")
    _set(hass, "binary_sensor.akku", "on", "battery", "Akku")
    await hass.async_block_till_done()

    assert published == [
        ("kameraposti/3/anturit/binary_sensor.kellari", "leak", False),
        ("kameraposti/3/anturit/binary_sensor.liike", "motion", False),
        ("kameraposti/3/anturit/sensor.olohuone", "21.5", False),
    ]

    exporter.async_stop()
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    await hass.async_block_till_done()
    assert len(published) == 3


async def test_temperature_is_sent_at_most_once_a_minute_and_the_latest_value_follows(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """A chatty thermometer must not use up the account's shared message budget
    (Kameraposti: 120/min per account) and crowd out a leak alarm."""
    _set(hass, "sensor.olohuone", "20.0", "temperature", "Olohuone")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=["sensor.olohuone"], publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    published.clear()

    for value in ("20.1", "20.2", "20.3"):
        freezer.tick(5)
        _set(hass, "sensor.olohuone", value, "temperature", "Olohuone")
    await hass.async_block_till_done()
    assert published == []

    freezer.tick(50)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert published == [("kameraposti/3/anturit/sensor.olohuone", "20.3", False)]

    freezer.tick(61)
    _set(hass, "sensor.olohuone", "20.4", "temperature", "Olohuone")
    await hass.async_block_till_done()
    assert published[-1] == ("kameraposti/3/anturit/sensor.olohuone", "20.4", False)
    assert len(published) == 2

    freezer.tick(1)
    _set(hass, "sensor.olohuone", "20.5", "temperature", "Olohuone")
    exporter.async_stop()
    freezer.tick(120)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert len(published) == 2


async def test_trailing_temperature_send_runs_on_the_event_loop(hass: HomeAssistant) -> None:
    """Fable M-1: a plain lambda given to async_call_later becomes an executor
    job, so the trailing send would touch hass.states and async_call_later off
    the event loop. The scheduled action must be an event-loop callback."""
    _set(hass, "sensor.olohuone", "20.0", "temperature", "Olohuone")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=["sensor.olohuone"], publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    with patch("custom_components.kameraposti.sensor_export.async_call_later") as call_later:
        _set(hass, "sensor.olohuone", "20.1", "temperature", "Olohuone")
        await hass.async_block_till_done()

    call_later.assert_called_once()
    action = call_later.call_args.args[2]
    job = action if isinstance(action, HassJob) else HassJob(action)
    assert job.job_type is HassJobType.Callback
    job.target(dt_util.utcnow())
    assert published[-1] == ("kameraposti/3/anturit/sensor.olohuone", "20.1", False)
    exporter.async_stop()


async def test_sensors_are_described_and_resent_every_15_minutes(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """Fable re-check: Kameraposti's listener reconnects hourly (and after a crash);
    a description or state published in that gap is lost. A periodic resend
    recovers it (Kameraposti drops repeated states, so no extra alarms)."""
    _set(hass, "binary_sensor.kellari", "on", "moisture", "Kellarin vuoto")
    _set(hass, "binary_sensor.liike", "on", "motion", "Pihan liike")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=["binary_sensor.kellari", "binary_sensor.liike"],
        publish=publish,
    )
    exporter.async_start()
    exporter.publish_snapshot()
    published.clear()

    freezer.tick(14 * 60)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert published == []

    freezer.tick(61)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert [t for t, _, _ in published] == [
        "kameraposti/3/anturit/binary_sensor.kellari/config",
        "kameraposti/3/anturit/binary_sensor.kellari",
        "kameraposti/3/anturit/binary_sensor.liike/config",
    ]
    assert published[1][1] == "leak"

    exporter.async_stop()
    published.clear()
    freezer.tick(16 * 60)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert published == []


async def test_entity_that_appears_later_is_described_before_its_first_state(hass: HomeAssistant) -> None:
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=["binary_sensor.ovi"], publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    assert published == []

    _set(hass, "binary_sensor.ovi", "on", "door", "Etuovi")
    _set(hass, "binary_sensor.ovi", "off", "door", "Etuovi")
    _set(hass, "binary_sensor.ovi", "off", "door", "Pääovi")
    await hass.async_block_till_done()

    assert [(t, json.loads(p) if t.endswith("/config") else p) for t, p, _ in published] == [
        (
            "kameraposti/3/anturit/binary_sensor.ovi/config",
            {"name": "Etuovi", "kind": "door", "format": "simple"},
        ),
        ("kameraposti/3/anturit/binary_sensor.ovi", "open"),
        ("kameraposti/3/anturit/binary_sensor.ovi", "closed"),
        (
            "kameraposti/3/anturit/binary_sensor.ovi/config",
            {"name": "Pääovi", "kind": "door", "format": "simple"},
        ),
    ]
    exporter.async_stop()


async def test_nothing_is_marked_described_while_disconnected(hass: HomeAssistant) -> None:
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    published: list[tuple[str, str, bool]] = []
    connected = False

    def publish(topic: str, payload: str, retain: bool) -> bool:
        if connected:
            published.append((topic, payload, retain))
        return connected

    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=["binary_sensor.kellari"], publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    connected = True
    _set(hass, "binary_sensor.kellari", "on", "moisture", "Kellarin vuoto")
    await hass.async_block_till_done()

    assert [t for t, _, _ in published] == [
        "kameraposti/3/anturit/binary_sensor.kellari/config",
        "kameraposti/3/anturit/binary_sensor.kellari",
    ]
    exporter.async_stop()


async def test_coordinator_describes_exported_sensors_when_connected(hass: HomeAssistant) -> None:
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        options={CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"]},
        version=2,
    )
    entry.add_to_hass(hass)
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        client = MagicMock()
        client.async_start = AsyncMock()
        client.async_stop = AsyncMock()
        mock_cls.return_value = client
        coordinator = KameraportiCoordinator(
            hass, entry, host="cam.steels.me", customer_id=CUSTOMER_ID, username="kp-3", password="x"
        )
        await coordinator.async_start()
        coordinator._handle_state_change(ConnectionState.CONNECTED)
        await hass.async_block_till_done()

        calls = [c.args for c in client.publish.call_args_list]
        assert [c[0] for c in calls] == [
            "kameraposti/3/anturit/binary_sensor.kellari/config",
            "kameraposti/3/anturit/binary_sensor.kellari",
        ]
        assert json.loads(calls[0][1])["kind"] == "leak"
        assert calls[1][1:] == ("dry", False)

        _set(hass, "binary_sensor.kellari", "on", "moisture", "Kellarin vuoto")
        await hass.async_block_till_done()
        assert client.publish.call_args.args == ("kameraposti/3/anturit/binary_sensor.kellari", "leak", False)
        await coordinator.async_stop()


async def test_options_flow_selects_the_entities_to_export(hass: HomeAssistant) -> None:
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)

    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        result = await hass.config_entries.options.async_init(entry.entry_id)
        assert result["type"] is FlowResultType.FORM
        result2 = await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"]}
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_EXPORTED_ENTITIES] == ["binary_sensor.kellari"]


async def test_options_flow_refuses_more_sensors_than_kameraposti_allows(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)
    for i in range(21):
        _set(hass, f"binary_sensor.vuoto_{i}", "off", "moisture", f"Vuoto {i}")

    result = await hass.config_entries.options.async_init(entry.entry_id)
    result2 = await hass.config_entries.options.async_configure(
        result["flow_id"], {CONF_EXPORTED_ENTITIES: [f"binary_sensor.vuoto_{i}" for i in range(21)]}
    )

    assert result2["type"] is FlowResultType.FORM
    assert result2["errors"] == {CONF_EXPORTED_ENTITIES: "too_many"}
    assert CONF_EXPORTED_ENTITIES not in entry.options


async def test_zwave_problem_class_leak_is_exported_as_leak(hass: HomeAssistant) -> None:
    """1.3.2 (steelo 2026-10-06 "vuoto mittaus jää pois"): Z-Wave JS UI publishes the Water Alarm as
    device_class "problem" (OK / Problem). The value part of the name decides; the device name
    ("Keitttio-Vuoto") alone does not make the general purpose sensor a leak sensor."""
    _set(
        hass,
        "binary_sensor.keitttio_vuoto_event_water_leak",
        "off",
        "problem",
        "Keitttio-Vuoto_event_water_leak",
    )
    _set(
        hass,
        "binary_sensor.keitttio_vuoto_event_general_purpose",
        "on",
        "problem",
        "Keitttio-Vuoto_event_general_purpose",
    )
    _set(hass, "binary_sensor.nodeid_6_alarm_status", "off", "problem", "nodeID_6_alarm_status")
    _set(hass, "binary_sensor.kellari", "off", "problem", "Kellari water leak")
    _set(hass, "binary_sensor.olohuone_palo", "off", "problem", "Olohuone smoke detected")

    assert kind_for(hass.states.get("binary_sensor.keitttio_vuoto_event_water_leak")) == "leak"
    assert kind_for(hass.states.get("binary_sensor.kellari")) == "leak"
    assert kind_for(hass.states.get("binary_sensor.olohuone_palo")) == "smoke"
    assert kind_for(hass.states.get("binary_sensor.keitttio_vuoto_event_general_purpose")) is None
    assert kind_for(hass.states.get("binary_sensor.nodeid_6_alarm_status")) is None
    # Problem = on = leak, OK = off = dry.
    assert event_for("leak", "on") == "leak"
    assert event_for("leak", "off") == "dry"


async def test_options_flow_names_a_sensor_whose_kind_is_unknown(hass: HomeAssistant) -> None:
    _set(
        hass,
        "binary_sensor.keitttio_vuoto_event_water_leak",
        "off",
        "problem",
        "Keitttio-Vuoto_event_water_leak",
    )
    _set(
        hass,
        "binary_sensor.keitttio_vuoto_event_general_purpose",
        "on",
        "problem",
        "Keitttio-Vuoto_event_general_purpose",
    )
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)

    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        result = await hass.config_entries.options.async_init(entry.entry_id)
        refused = await hass.config_entries.options.async_configure(
            result["flow_id"],
            {
                CONF_EXPORTED_ENTITIES: [
                    "binary_sensor.keitttio_vuoto_event_water_leak",
                    "binary_sensor.keitttio_vuoto_event_general_purpose",
                ]
            },
        )
        assert refused["type"] is FlowResultType.FORM
        assert refused["errors"] == {CONF_EXPORTED_ENTITIES: "unknown_kind"}
        assert (
            refused["description_placeholders"]["entities"]
            == "binary_sensor.keitttio_vuoto_event_general_purpose"
        )
        assert CONF_EXPORTED_ENTITIES not in entry.options

        saved = await hass.config_entries.options.async_configure(
            refused["flow_id"], {CONF_EXPORTED_ENTITIES: ["binary_sensor.keitttio_vuoto_event_water_leak"]}
        )
        await hass.async_block_till_done()

    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_EXPORTED_ENTITIES] == ["binary_sensor.keitttio_vuoto_event_water_leak"]


async def test_gas_and_carbon_monoxide_detectors_are_gas_sensors(hass: HomeAssistant) -> None:
    """1.4.0 (steelo 2026-10-06): a gas detector is its own kind, alarming like smoke; a carbon
    monoxide detector (HA "carbon_monoxide") is a gas sensor too."""
    cases = {
        ("binary_sensor.kaasu", "gas"): "gas",
        ("binary_sensor.haka", "carbon_monoxide"): "gas",
        ("sensor.kaasu", "gas"): None,
    }
    for (entity_id, device_class), kind in cases.items():
        _set(hass, entity_id, "off", device_class, entity_id)
        assert kind_for(hass.states.get(entity_id)) == kind, entity_id
    assert event_for("gas", "on") == "gas"
    assert event_for("gas", "off") == "clear"
    for state in ("unavailable", "unknown", "12"):
        assert event_for("gas", state) is None, state


async def test_zwave_problem_class_gas_is_exported_as_gas(hass: HomeAssistant) -> None:
    """Z-Wave JS UI notification sensors (device class "problem"): "gas", "combustible" or
    "carbon_monoxide" in the value part of the name makes a gas sensor. A Finnish device name
    ("Keittio-Kaasu") alone does not, and a CO2 alarm (a threshold, not a ppm reading) is not gas."""
    cases = {
        "Keittio-Kaasu_event_combustible_gas_detected": "gas",
        "Keittio-Kaasu_event_toxic_gas": "gas",
        "Eteinen_event_carbon_monoxide_detected": "gas",
        "Keittio-Kaasu_event_general_purpose": None,
        "Olohuone_event_carbon_dioxide_detected": None,
    }
    for name, kind in cases.items():
        entity_id = "binary_sensor." + name.lower().replace("-", "_")
        _set(hass, entity_id, "off", "problem", name)
        assert kind_for(hass.states.get(entity_id)) == kind, name


async def test_value_part_decides_when_the_device_name_names_another_kind(hass: HomeAssistant) -> None:
    """A combined detector's device name may name several kinds ("Smoke-Gas"); the value part,
    which follows the device name, decides -- the last kind word in the name wins."""
    cases = {
        "Smoke-Gas_event_smoke_detected": "smoke",
        "Smoke-Gas_event_combustible_gas_detected": "gas",
        "Gas-Smoke_event_smoke_detected": "smoke",
        "Smoke-CO_event_carbon_monoxide_detected": "gas",
        "Smoke-Leak_event_smoke_detected": "smoke",
        "Smoke-Leak_event_water_leak": "leak",
    }
    for name, kind in cases.items():
        entity_id = "binary_sensor." + name.lower().replace("-", "_")
        _set(hass, entity_id, "off", "problem", name)
        assert kind_for(hass.states.get(entity_id)) == kind, name


async def test_gas_alarm_is_described_and_sent_without_throttling(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    _set(hass, "binary_sensor.keittio_kaasu", "off", "gas", "Keittiön kaasu")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=["binary_sensor.keittio_kaasu"], publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    assert [(t, json.loads(p) if t.endswith("/config") else p) for t, p, _ in published] == [
        (
            "kameraposti/3/anturit/binary_sensor.keittio_kaasu/config",
            {"name": "Keittiön kaasu", "kind": "gas", "format": "simple"},
        ),
        ("kameraposti/3/anturit/binary_sensor.keittio_kaasu", "clear"),
    ]
    published.clear()

    for state in ("on", "off", "on"):
        freezer.tick(1)
        _set(hass, "binary_sensor.keittio_kaasu", state, "gas", "Keittiön kaasu")
    await hass.async_block_till_done()
    assert published == [
        ("kameraposti/3/anturit/binary_sensor.keittio_kaasu", "gas", False),
        ("kameraposti/3/anturit/binary_sensor.keittio_kaasu", "clear", False),
        ("kameraposti/3/anturit/binary_sensor.keittio_kaasu", "gas", False),
    ]
    exporter.async_stop()


def _offered_device_classes() -> set[tuple[str, str]]:
    return {
        (domain, device_class)
        for entity_filter in EXPORTABLE_ENTITIES.config["filter"]
        for domain in entity_filter["domain"]
        for device_class in entity_filter["device_class"]
    }


def test_export_list_offers_gas_and_carbon_monoxide_detectors() -> None:
    offered = _offered_device_classes()
    assert ("binary_sensor", "gas") in offered
    assert ("binary_sensor", "carbon_monoxide") in offered


def test_export_list_offers_carbon_dioxide_sensors() -> None:
    offered = _offered_device_classes()
    assert ("sensor", "carbon_dioxide") in offered
    assert ("sensor", "temperature") in offered


@pytest.mark.parametrize(
    ("entity_id", "device_class", "state", "unit"),
    [
        ("binary_sensor.keittio_kaasu", "gas", "off", None),
        ("binary_sensor.eteinen_haka", "carbon_monoxide", "off", None),
        ("sensor.olohuone_co2", "carbon_dioxide", "812", "ppm"),
    ],
)
async def test_options_flow_accepts_the_new_kinds(
    hass: HomeAssistant, entity_id: str, device_class: str, state: str, unit: str | None
) -> None:
    _set(hass, entity_id, state, device_class, entity_id, unit)
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)

    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        result = await hass.config_entries.options.async_init(entry.entry_id)
        saved = await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_EXPORTED_ENTITIES: [entity_id]}
        )
        await hass.async_block_till_done()

    assert saved["type"] is FlowResultType.CREATE_ENTRY, saved.get("errors")
    assert entry.options[CONF_EXPORTED_ENTITIES] == [entity_id]


async def test_carbon_dioxide_sensor_is_a_co2_sensor(hass: HomeAssistant) -> None:
    """1.4.0 (steelo 2026-10-06): a CO2 meter (HA sensor "carbon_dioxide", ppm) is the co2 kind."""
    cases = {
        ("sensor.olohuone_co2", "carbon_dioxide"): "co2",
        ("binary_sensor.co2_halytys", "carbon_dioxide"): None,
        ("sensor.haka_ppm", "carbon_monoxide"): None,
    }
    for (entity_id, device_class), kind in cases.items():
        _set(hass, entity_id, "812", device_class, entity_id)
        assert kind_for(hass.states.get(entity_id)) == kind, entity_id


def test_co2_reading_is_sent_as_whole_ppm_in_json() -> None:
    """Contract: {"e":"co2","v":<integer ppm>}; unit ppm or none, finite numbers only."""
    assert event_for("co2", "812") == '{"e":"co2","v":812}'
    assert event_for("co2", "812", "ppm") == '{"e":"co2","v":812}'
    assert event_for("co2", "812.6", "ppm") == '{"e":"co2","v":813}'
    assert event_for("co2", "1449.4") == '{"e":"co2","v":1449}'
    assert type(json.loads(event_for("co2", "455.0", "ppm"))["v"]) is int
    for state, unit in [
        ("812", "ppb"),
        ("812", "mg/m³"),
        ("0.08", "%"),
        ("high", "ppm"),
        ("", None),
        ("inf", "ppm"),
        ("-inf", None),
        ("nan", "ppm"),
        ("unavailable", "ppm"),
        ("unknown", None),
        ("on", None),
    ]:
        assert event_for("co2", state, unit) is None, (state, unit)


async def test_co2_is_sent_at_most_once_a_minute_and_the_latest_value_follows(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """Like temperature: a chatty CO2 meter must not use up the account's shared message budget."""
    _set(hass, "sensor.olohuone_co2", "800", "carbon_dioxide", "Olohuoneen CO2", "ppm")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=["sensor.olohuone_co2"], publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    assert [(t, json.loads(p)) for t, p, _ in published] == [
        (
            "kameraposti/3/anturit/sensor.olohuone_co2/config",
            {"name": "Olohuoneen CO2", "kind": "co2", "format": "simple"},
        ),
        ("kameraposti/3/anturit/sensor.olohuone_co2", {"e": "co2", "v": 800}),
    ]
    published.clear()

    for value in ("810", "820", "830.4"):
        freezer.tick(5)
        _set(hass, "sensor.olohuone_co2", value, "carbon_dioxide", "Olohuoneen CO2", "ppm")
    await hass.async_block_till_done()
    assert published == []

    freezer.tick(50)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert published == [("kameraposti/3/anturit/sensor.olohuone_co2", '{"e":"co2","v":830}', False)]

    freezer.tick(61)
    _set(hass, "sensor.olohuone_co2", "840", "carbon_dioxide", "Olohuoneen CO2", "ppm")
    await hass.async_block_till_done()
    assert published[-1] == ("kameraposti/3/anturit/sensor.olohuone_co2", '{"e":"co2","v":840}', False)
    assert len(published) == 2

    freezer.tick(1)
    _set(hass, "sensor.olohuone_co2", "850", "carbon_dioxide", "Olohuoneen CO2", "ppm")
    exporter.async_stop()
    freezer.tick(120)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert len(published) == 2
