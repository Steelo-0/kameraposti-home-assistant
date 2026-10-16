"""Sensor export tests (2026-10-05): selected Home Assistant entities become
Kameraposti sensors. The integration publishes a description to
kameraposti/<id>/anturit/<name>/config (Kameraposti creates or updates the
sensor) and each state change to kameraposti/<id>/anturit/<name> in the
simple format (leak/dry, smoke/clear, gas/clear, open/closed, motion,
temperature, {"e":"co2","v":<ppm>}).
On every (re)connect it describes the sensors again and sends their current
state (not motion), so a leak that started during a break is not lost;
Kameraposti drops repeated states itself.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.core import HassJob, HassJobType, HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.config_flow import exportable_entity_options
from custom_components.kameraposti.const import (
    CONF_CUSTOMER_ID,
    CONF_EXPORTED_ENTITIES,
    CONF_HOST,
    DEFAULT_SENSOR_LIMIT,
    DOMAIN,
    MAX_EXPORTED_SENSORS,
)
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.mqtt_client import ConnectionState
from custom_components.kameraposti.sensor_export import (
    KameraportiSensorExporter,
    effective_sensor_limit,
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


async def _connected_coordinator(
    hass: HomeAssistant, entity_ids: list[str]
) -> tuple[KameraportiCoordinator, MagicMock]:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        options={CONF_EXPORTED_ENTITIES: entity_ids},
        version=2,
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_start = AsyncMock()
    client.async_stop = AsyncMock()
    client.publish.return_value = True
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", return_value=client):
        coordinator = KameraportiCoordinator(
            hass, entry, host="cam.steels.me", customer_id=CUSTOMER_ID, username="kp-3", password="x"
        )
    await coordinator.async_start()
    coordinator._handle_state_change(ConnectionState.CONNECTED)
    await hass.async_block_till_done()
    return coordinator, client


def _published_topics(client: MagicMock) -> list[str]:
    return [c.args[0] for c in client.publish.call_args_list]


async def test_a_reconnect_storm_sends_the_full_snapshot_at_most_once_a_minute(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """1.5.1: two clients on one login kick each other out every few seconds. Kameraposti counts
    every message against the account's 120/min before reading it, so a full snapshot (two
    messages per sensor) on each reconnect crowded out real leak/smoke/door alarms."""
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    _set(hass, "binary_sensor.ovi", "off", "door", "Etuovi")
    coordinator, client = await _connected_coordinator(hass, ["binary_sensor.kellari", "binary_sensor.ovi"])
    snapshot = [
        "kameraposti/3/anturit/binary_sensor.kellari/config",
        "kameraposti/3/anturit/binary_sensor.kellari",
        "kameraposti/3/anturit/binary_sensor.ovi/config",
        "kameraposti/3/anturit/binary_sensor.ovi",
    ]
    assert _published_topics(client) == snapshot  # the first connect sends it at once

    for _ in range(10):
        freezer.tick(5)
        coordinator._handle_state_change(ConnectionState.RECONNECTING)
        coordinator._handle_state_change(ConnectionState.CONNECTED)
        async_fire_time_changed(hass)
        await hass.async_block_till_done()
        assert _published_topics(client) == snapshot

    # A minute after the first one, the (deferred) snapshot goes out once.
    freezer.tick(10)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert _published_topics(client) == snapshot * 2

    freezer.tick(59)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert _published_topics(client) == snapshot * 2

    # A deferred snapshot dies with the entry.
    coordinator._handle_state_change(ConnectionState.RECONNECTING)
    coordinator._handle_state_change(ConnectionState.CONNECTED)
    await coordinator.async_stop()
    freezer.tick(5)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert _published_topics(client) == snapshot * 2


async def test_an_alarm_from_a_break_goes_out_at_once_on_a_rate_limited_reconnect(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """The snapshot limit never delays an alarm: a sensor whose message could not be sent during
    the break is sent as soon as the connection is back; only the full snapshot waits."""
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    _set(hass, "binary_sensor.ovi", "off", "door", "Etuovi")
    coordinator, client = await _connected_coordinator(hass, ["binary_sensor.kellari", "binary_sensor.ovi"])
    client.publish.reset_mock()

    freezer.tick(5)
    client.publish.return_value = False  # disconnected
    coordinator._handle_state_change(ConnectionState.RECONNECTING)
    _set(hass, "binary_sensor.kellari", "on", "moisture", "Kellarin vuoto")
    await hass.async_block_till_done()
    client.publish.reset_mock()

    freezer.tick(2)
    client.publish.return_value = True
    coordinator._handle_state_change(ConnectionState.CONNECTED)
    await hass.async_block_till_done()
    assert [c.args for c in client.publish.call_args_list] == [
        ("kameraposti/3/anturit/binary_sensor.kellari", "leak", False)
    ]

    # Sent once: the next rate-limited reconnect does not repeat it.
    freezer.tick(2)
    coordinator._handle_state_change(ConnectionState.RECONNECTING)
    coordinator._handle_state_change(ConnectionState.CONNECTED)
    await hass.async_block_till_done()
    assert len(client.publish.call_args_list) == 1
    await coordinator.async_stop()


async def _open_sensor_options(hass: HomeAssistant, entry: MockConfigEntry) -> dict:
    """1.6.0: the options open on a menu (sensors / cameras); pick the sensors."""
    menu = await hass.config_entries.options.async_init(entry.entry_id)
    assert menu["type"] is FlowResultType.MENU
    result = await hass.config_entries.options.async_configure(menu["flow_id"], {"next_step_id": "sensors"})
    assert result["step_id"] == "sensors"
    return result


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
        result = await _open_sensor_options(hass, entry)
        assert result["type"] is FlowResultType.FORM
        result2 = await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"]}
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_EXPORTED_ENTITIES] == ["binary_sensor.kellari"]


async def test_options_flow_refuses_more_sensors_than_the_default_until_kameraposti_tells(
    hass: HomeAssistant,
) -> None:
    """1.8.0: without a limit from Kameraposti (the entry not running, or no roster yet) the default
    20 applies, and the error names it."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)
    for i in range(DEFAULT_SENSOR_LIMIT + 1):
        _set(hass, f"binary_sensor.vuoto_{i}", "off", "moisture", f"Vuoto {i}")

    result = await _open_sensor_options(hass, entry)
    assert result["description_placeholders"]["max"] == "20"
    result2 = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {CONF_EXPORTED_ENTITIES: [f"binary_sensor.vuoto_{i}" for i in range(DEFAULT_SENSOR_LIMIT + 1)]},
    )

    assert result2["type"] is FlowResultType.FORM
    assert result2["errors"] == {CONF_EXPORTED_ENTITIES: "too_many"}
    assert result2["description_placeholders"]["max"] == "20"
    assert CONF_EXPORTED_ENTITIES not in entry.options


def _limit_roster(sensor_limit: object = None, *, second: int = 0) -> bytes:
    payload: dict[str, object] = {
        "schema_version": 1,
        "generated_at": f"2026-10-16T12:00:{second:02d}+00:00",
        "cameras": [],
    }
    if sensor_limit is not None:
        payload["sensor_limit"] = sensor_limit
    return json.dumps(payload).encode()


async def _running_entry(hass: HomeAssistant, roster: bytes | None) -> MockConfigEntry:
    """A set-up entry whose coordinator has received ``roster`` (None = no roster yet)."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    if roster is not None:
        hass.data[DOMAIN][entry.entry_id]._handle_message(f"customers/{CUSTOMER_ID}/cameras", roster)
        await hass.async_block_till_done()
    return entry


@pytest.mark.parametrize(("sensor_limit", "effective"), [(30, 30), (200, 200), (500, 200), (7, 7)])
async def test_options_flow_takes_as_many_sensors_as_the_plan_from_kameraposti(
    hass: HomeAssistant, sensor_limit: int, effective: int
) -> None:
    """1.8.0 (steelo 2026-10-16 "maalaisjärjellä, pari klikkausta"): the limit comes from Kameraposti's
    roster, never more than 200; one more is refused with the plan's own number."""
    selected = [f"binary_sensor.ovi_{i}" for i in range(effective + 1)]
    for i, entity_id in enumerate(selected):
        _set(hass, entity_id, "off", "door", f"Ovi {i}")
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        entry = await _running_entry(hass, _limit_roster(sensor_limit))

        result = await _open_sensor_options(hass, entry)
        assert result["description_placeholders"]["max"] == str(effective)
        refused = await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_EXPORTED_ENTITIES: selected}
        )
        assert refused["type"] is FlowResultType.FORM
        assert refused["errors"] == {CONF_EXPORTED_ENTITIES: "too_many"}
        assert refused["description_placeholders"]["max"] == str(effective)

        saved = await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_EXPORTED_ENTITIES: selected[:effective]}
        )
        await hass.async_block_till_done()

    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_EXPORTED_ENTITIES] == selected[:effective]


async def test_options_flow_falls_back_to_the_default_with_an_invalid_or_missing_limit(
    hass: HomeAssistant,
) -> None:
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        entry = await _running_entry(hass, _limit_roster("30"))
        result = await _open_sensor_options(hass, entry)
    assert result["description_placeholders"]["max"] == "20"


def test_the_effective_limit_is_the_plans_up_to_200() -> None:
    assert MAX_EXPORTED_SENSORS == 200
    assert effective_sensor_limit(None) == DEFAULT_SENSOR_LIMIT == 20
    assert effective_sensor_limit(1) == 1
    assert effective_sensor_limit(30) == 30
    assert effective_sensor_limit(200) == 200
    assert effective_sensor_limit(1000) == 200


# -- the plan's sensor limit in the exporter (1.8.0) -------------------------------------


def _doors(hass: HomeAssistant, count: int) -> list[str]:
    entity_ids = [f"binary_sensor.ovi_{i:02d}" for i in range(count)]
    for i, entity_id in enumerate(entity_ids):
        _set(hass, entity_id, "off", "door", f"Ovi {i}")
    return entity_ids


def _sent_entities(published: list[tuple[str, str, bool]]) -> list[str]:
    """The entities whose state was sent, in order (descriptions left out)."""
    return [topic.split("/")[3] for topic, _, _ in published if not topic.endswith("/config")]


def _kameraposti_logs(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == level and r.name.startswith("custom_components.kameraposti")
    ]


async def test_only_the_first_chosen_sensors_up_to_the_limit_are_sent(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Saved order decides; the rest are named in ONE warning per change -- not before Kameraposti has
    told the limit (the default would claim a limit the plan may not have)."""
    entity_ids = _doors(hass, 25)
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=entity_ids, publish=publish
    )
    exporter.async_start()

    with caplog.at_level(logging.INFO):
        exporter.publish_snapshot()
        assert _sent_entities(published) == entity_ids[:20]
        assert _kameraposti_logs(caplog, logging.WARNING) == []

        # A roster without sensor_limit: the default stands, now told -- named once.
        exporter.async_set_limit(20)
        exporter.async_set_limit(20)
        assert _kameraposti_logs(caplog, logging.WARNING) == [
            "Kameraposti: your Kameraposti plan takes at most 20 sensors; not sending "
            + ", ".join(entity_ids[20:])
            + " (the last ones chosen under Configure -> Sensors to Kameraposti)"
        ]
        # The left-out ones are not sent when they change either.
        published.clear()
        _set(hass, entity_ids[24], "on", "door", "Ovi 24")
        _set(hass, entity_ids[0], "on", "door", "Ovi 0")
        await hass.async_block_till_done()
        assert _sent_entities(published) == [entity_ids[0]]
    exporter.async_stop()


async def test_a_higher_limit_sends_the_left_out_sensors_at_once_without_a_restart(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    entity_ids = _doors(hass, 25)
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=entity_ids, publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    exporter.async_set_limit(20)
    published.clear()
    caplog.clear()

    with caplog.at_level(logging.INFO):
        exporter.async_set_limit(30)
    # Described before their state, right away -- not only at the next 15-minute resend.
    assert [topic for topic, _, _ in published] == [
        topic
        for entity_id in entity_ids[20:]
        for topic in (f"kameraposti/3/anturit/{entity_id}/config", f"kameraposti/3/anturit/{entity_id}")
    ]
    assert _kameraposti_logs(caplog, logging.WARNING) == []
    assert _kameraposti_logs(caplog, logging.INFO) == [
        "Kameraposti: your Kameraposti plan takes all 25 chosen sensors"
    ]

    published.clear()
    exporter.publish_snapshot()
    assert _sent_entities(published) == entity_ids
    published.clear()
    _set(hass, entity_ids[24], "on", "door", "Ovi 24")
    await hass.async_block_till_done()
    assert _sent_entities(published) == [entity_ids[24]]
    exporter.async_stop()


async def test_a_lower_limit_stops_sending_the_extra_sensors_without_removal_messages(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    entity_ids = _doors(hass, 25)
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=entity_ids, publish=publish, limit=30
    )
    exporter.async_start()
    exporter.async_set_limit(30)
    exporter.publish_snapshot()
    published.clear()

    with caplog.at_level(logging.WARNING):
        exporter.async_set_limit(22)
        exporter.async_set_limit(22)
    assert published == []
    assert _kameraposti_logs(caplog, logging.WARNING) == [
        "Kameraposti: your Kameraposti plan takes at most 22 sensors; not sending "
        + ", ".join(entity_ids[22:])
        + " (the last ones chosen under Configure -> Sensors to Kameraposti)"
    ]
    exporter.publish_snapshot()
    assert _sent_entities(published) == entity_ids[:22]
    published.clear()
    _set(hass, entity_ids[23], "on", "door", "Ovi 23")
    await hass.async_block_till_done()
    assert published == []

    # A different set left out is a change: named again.
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        exporter.async_set_limit(24)
    assert _kameraposti_logs(caplog, logging.WARNING) == [
        "Kameraposti: your Kameraposti plan takes at most 24 sensors; not sending binary_sensor.ovi_24 "
        "(the last ones chosen under Configure -> Sensors to Kameraposti)"
    ]
    exporter.async_stop()


async def test_the_roster_sets_the_limit_and_a_roster_without_it_brings_the_default_back(
    hass: HomeAssistant,
) -> None:
    entity_ids = _doors(hass, 25)
    coordinator, client = await _connected_coordinator(hass, entity_ids)
    roster_topic = f"customers/{CUSTOMER_ID}/cameras"
    assert coordinator.sensor_limit is None
    states = [c.args for c in client.publish.call_args_list]
    assert _sent_entities(states) == entity_ids[:20]

    client.publish.reset_mock()
    coordinator._handle_message(roster_topic, _limit_roster(30, second=1))
    await hass.async_block_till_done()
    assert coordinator.sensor_limit == 30
    states = [c.args for c in client.publish.call_args_list]
    assert _sent_entities(states) == entity_ids[20:]

    # The retained roster again (a reconnect): nothing more to send.
    client.publish.reset_mock()
    coordinator._handle_message(roster_topic, _limit_roster(30, second=1))
    await hass.async_block_till_done()
    assert _published_topics(client) == []

    # An older roster changes nothing; a newer one without the field (or with junk) is the default.
    coordinator._handle_message(roster_topic, _limit_roster(5, second=0))
    assert coordinator.sensor_limit == 30
    coordinator._handle_message(roster_topic, _limit_roster(0, second=2))
    await hass.async_block_till_done()
    assert coordinator.sensor_limit is None
    client.publish.reset_mock()
    _set(hass, entity_ids[24], "on", "door", "Ovi 24")
    await hass.async_block_till_done()
    assert _published_topics(client) == []
    await coordinator.async_stop()


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
    # 1.4.1: the list only offers sensors that can be sent; a sensor chosen earlier (here the general
    # purpose one) stays in the list and is named if its kind can no longer be told.
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        options={CONF_EXPORTED_ENTITIES: ["binary_sensor.keitttio_vuoto_event_general_purpose"]},
        version=2,
    )
    entry.add_to_hass(hass)

    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        result = await _open_sensor_options(hass, entry)
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
        assert entry.options[CONF_EXPORTED_ENTITIES] == ["binary_sensor.keitttio_vuoto_event_general_purpose"]

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
    for state in ("unavailable", "unknown"):
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


async def test_export_list_offers_exactly_what_can_be_sent(hass: HomeAssistant) -> None:
    """1.4.1: the list is built from kind_for(), so a Z-Wave gas level without a device class
    (steelo 2026-10-06: "sensor_gas_carbon_monoxide" was missing) is offered, and an entity that
    cannot be sent is not."""
    _set(hass, "binary_sensor.keittio_kaasu", "off", "gas", "Keittiön kaasu")
    _set(hass, "binary_sensor.eteinen_hakavaroitin", "off", "carbon_monoxide", "Eteisen häkävaroitin")
    _set(hass, "sensor.olohuone_co2", "612", "carbon_dioxide", "Olohuone CO2", unit="ppm")
    _set(hass, "sensor.olohuone_lampo", "21.5", "temperature", "Olohuone", unit="°C")
    _set(hass, "sensor.nodeid_27_gas_carbon_monoxide", "0", None, "nodeID_27_gas_carbon_monoxide", unit="ppm")
    _set(hass, "sensor.nodeid_28_gas_carbon_dioxide", "640", None, "nodeID_28_gas_carbon_dioxide", unit="ppm")
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
    _set(hass, "sensor.nodeid_27_hardware_status", "ok", None, "nodeID_27_hardware_status")
    _set(hass, "sensor.olohuone_kosteus", "40", "humidity", "Olohuone kosteus", unit="%")
    _set(hass, "switch.nodeid_27_config_switch_2_1", "off", None, "config_switch_2_1")

    offered = {option["value"] for option in exportable_entity_options(hass, ["binary_sensor.poistettu"])}

    assert offered == {
        "binary_sensor.keittio_kaasu",
        "binary_sensor.eteinen_hakavaroitin",
        "sensor.olohuone_co2",
        "sensor.olohuone_lampo",
        "sensor.nodeid_27_gas_carbon_monoxide",
        "sensor.nodeid_28_gas_carbon_dioxide",
        "binary_sensor.keitttio_vuoto_event_water_leak",
        # Already chosen but gone right now: stays so saving the form keeps it.
        "binary_sensor.poistettu",
    }
    assert kind_for(hass.states.get("sensor.nodeid_27_gas_carbon_monoxide")) == "gas"
    assert kind_for(hass.states.get("sensor.nodeid_28_gas_carbon_dioxide")) == "co2"


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
        result = await _open_sensor_options(hass, entry)
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
        # 1.4.1: a CO meter is a gas alarm by ppm limits (test_co_meter_is_a_gas_alarm_by_ppm_limits).
        ("sensor.haka_ppm", "carbon_monoxide"): "gas",
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


async def test_entity_id_decides_before_a_renamed_friendly_name(hass: HomeAssistant) -> None:
    """Fable M-1: a friendly name renamed by the user cannot turn a leak sensor into a gas one; the
    friendly name decides only when the entity id has no kind word."""
    _set(hass, "binary_sensor.keittio_event_water_leak", "off", "problem", "Kellarin gas-varoitin")
    _set(hass, "binary_sensor.node_14_event", "off", "problem", "Varasto smoke detected")

    assert kind_for(hass.states.get("binary_sensor.keittio_event_water_leak")) == "leak"
    assert kind_for(hass.states.get("binary_sensor.node_14_event")) == "smoke"


async def test_gas_counts_only_as_a_word_of_its_own(hass: HomeAssistant) -> None:
    """Fable L-1: "gas" inside another word (Vegas, gasket, degassing) is no gas sensor."""
    for entity_id, name in (
        ("binary_sensor.las_vegas_event_general_purpose", "Las Vegas_event_general_purpose"),
        ("binary_sensor.gasket_event_general_purpose", "Gasket_event_general_purpose"),
        ("binary_sensor.degassing_tank_event_general_purpose", "Degassing tank_event_general_purpose"),
    ):
        _set(hass, entity_id, "off", "problem", name)
        assert kind_for(hass.states.get(entity_id)) is None, entity_id
    _set(hass, "binary_sensor.varasto_gas_alarm", "off", "problem", "Varasto gas alarm")
    assert kind_for(hass.states.get("binary_sensor.varasto_gas_alarm")) == "gas"


async def test_co_meter_is_a_gas_alarm_by_ppm_limits(hass: HomeAssistant) -> None:
    """1.4.1 (steelo 2026-10-06, Z-Wave "Carbon monoxide (CO) level"): a CO meter in ppm is a gas
    sensor that alarms from 50 ppm and clears below 35 ppm; nothing is sent in between."""
    _set(hass, "sensor.node_27_carbon_monoxide_co_level", "0", "carbon_monoxide", "CO level", unit="ppm")
    assert kind_for(hass.states.get("sensor.node_27_carbon_monoxide_co_level")) == "gas"

    assert event_for("gas", "0", "ppm") == "clear"
    assert event_for("gas", "34.9", "ppm") == "clear"
    assert event_for("gas", "35", "ppm") is None
    assert event_for("gas", "49.9", None) is None
    assert event_for("gas", "50", "ppm") == "gas"
    assert event_for("gas", "300", "ppm") == "gas"
    assert event_for("gas", "60", "mg/m³") is None
    assert event_for("gas", "nan", "ppm") is None
    # The detector's on/off is unchanged.
    assert event_for("gas", "on") == "gas"
    assert event_for("gas", "off") == "clear"


async def test_co_meter_sends_only_when_the_alarm_state_changes(hass: HomeAssistant) -> None:
    published, publish = _recorder()
    entity_id = "sensor.node_27_carbon_monoxide_co_level"
    _set(hass, entity_id, "0", "carbon_monoxide", "CO level", unit="ppm")
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=[entity_id], publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    await hass.async_block_till_done()
    state_topic = f"kameraposti/{CUSTOMER_ID}/anturit/{entity_id}"

    for value in ("2", "40", "55", "70", "45", "30", "1"):
        _set(hass, entity_id, value, "carbon_monoxide", "CO level", unit="ppm")
        await hass.async_block_till_done()
    exporter.async_stop()

    states = [payload for topic, payload, _ in published if topic == state_topic]
    assert states == ["clear", "gas", "clear"]
    config = [json.loads(payload) for topic, payload, _ in published if topic == f"{state_topic}/config"]
    assert config[0]["kind"] == "gas"


async def test_sensors_are_named_after_their_device(hass: HomeAssistant) -> None:
    """1.4.2 (steelo 2026-10-07 "friendly nimet, nyt yhtä sanasotkua"): the device's name instead of
    "<device> <device>_event_water_leak"; the friendly name stays when the user named the entity, when
    the entity has no device, or when two chosen entities of the same kind share the device."""
    from homeassistant.helpers import device_registry as dr
    from homeassistant.helpers import entity_registry as er

    config_entry = MockConfigEntry(domain="mqtt")
    config_entry.add_to_hass(hass)
    devices = dr.async_get(hass)
    entities = er.async_get(hass)
    kitchen = devices.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("mqtt", "zwave_30")}, name="Keitttio-Vuoto"
    )
    hall = devices.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("mqtt", "zwave_24")}, name="Takaovi"
    )
    devices.async_update_device(hall.id, name_by_user="Takaovi (eteinen)")

    def entity(domain: str, uid: str, device_id: str | None, original_name: str) -> str:
        return entities.async_get_or_create(
            domain, "mqtt", uid, device_id=device_id, suggested_object_id=uid, original_name=original_name
        ).entity_id

    leak = entity(
        "binary_sensor", "keitttio_vuoto_event_water_leak", kitchen.id, "Keitttio-Vuoto_event_water_leak"
    )
    temp_1 = entity("sensor", "keitttio_vuoto_temperature_air", kitchen.id, "temperature_air")
    temp_2 = entity("sensor", "keitttio_vuoto_temperature_air_2", kitchen.id, "temperature_air_2")
    door = entity("binary_sensor", "takaovi_door_state_simple", hall.id, "Takaovi_door_state_simple")
    named = entity("binary_sensor", "kellari_water_leak", hall.id, "water_leak")
    entities.async_update_entity(named, name="Kellarin vuoto")
    loose = entity("binary_sensor", "irrallinen_vuoto", None, "Irrallinen vuoto")

    _set(hass, leak, "off", "problem", "Keitttio-Vuoto Keitttio-Vuoto_event_water_leak")
    _set(hass, temp_1, "21.5", "temperature", "Keitttio-Vuoto temperature_air", unit="°C")
    _set(hass, temp_2, "22.1", "temperature", "Keitttio-Vuoto temperature_air_2", unit="°C")
    _set(hass, door, "off", "door", "Takaovi Takaovi_door_state_simple")
    _set(hass, named, "off", "moisture", "Takaovi (eteinen) Kellarin vuoto")
    _set(hass, loose, "off", "moisture", "Irrallinen vuoto")

    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=[leak, temp_1, temp_2, door, named, loose], publish=publish
    )
    exporter.async_start()
    exporter.publish_snapshot()
    names = {
        topic.split("/")[3]: json.loads(payload)["name"]
        for topic, payload, _ in published
        if topic.endswith("/config")
    }
    exporter.async_stop()

    assert names[leak] == "Keitttio-Vuoto"
    assert names[door] == "Takaovi (eteinen)"
    # Two thermometers on one device keep their own names.
    assert names[temp_1] == "Keitttio-Vuoto temperature_air"
    assert names[temp_2] == "Keitttio-Vuoto temperature_air_2"
    # The user named this entity: their name stays.
    assert names[named] == "Takaovi (eteinen) Kellarin vuoto"
    assert names[loose] == "Irrallinen vuoto"


async def test_export_list_rows_are_short(hass: HomeAssistant) -> None:
    """1.4.3 (steelo 2026-10-07 "liian pitkät nimet"): a row reads "<name in Kameraposti> · <kind>";
    the entity id is added only when two rows would otherwise read the same."""
    from homeassistant.helpers import device_registry as dr
    from homeassistant.helpers import entity_registry as er

    config_entry = MockConfigEntry(domain="mqtt")
    config_entry.add_to_hass(hass)
    devices = dr.async_get(hass)
    entities = er.async_get(hass)
    kitchen = devices.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("mqtt", "zwave_7")}, name="Keittiö palovaroitin"
    )
    smoke = entities.async_get_or_create(
        "binary_sensor",
        "mqtt",
        "keittio_palovaroitin_smoke_alarm",
        device_id=kitchen.id,
        suggested_object_id="keittio_palovaroitin_keittio_palovaroitin_smoke_alarm",
    ).entity_id
    _set(hass, smoke, "off", "smoke", "Keittiö palovaroitin Keittiö palovaroitin_smoke_alarm")
    _set(hass, "binary_sensor.ovi_a", "off", "door", "Ovi")
    _set(hass, "binary_sensor.ovi_b", "off", "door", "Ovi")

    hass.config.language = "fi"
    labels = {option["value"]: option["label"] for option in exportable_entity_options(hass, [])}
    assert labels[smoke] == "Keittiö palovaroitin · Savu"
    assert labels["binary_sensor.ovi_a"] == "Ovi · Ovi (ovi_a)"
    assert labels["binary_sensor.ovi_b"] == "Ovi · Ovi (ovi_b)"

    hass.config.language = "en"
    labels = {option["value"]: option["label"] for option in exportable_entity_options(hass, [])}
    assert labels[smoke] == "Keittiö palovaroitin · Smoke"


async def test_device_name_does_not_decide_and_tamper_is_no_door(hass: HomeAssistant) -> None:
    """1.4.4 (steelo 2026-10-07): "Eteinen Smoke_alarm_status" (a smoke detector's OK / Problem alarm
    status) is not a smoke alarm although the device is called "Eteinen Smoke", and a Z-Wave sensor's
    cover switch ("nodeID_11_cover_status", Closed / Open) is not a door."""
    cases = {
        (
            "binary_sensor.eteinen_smoke_eteinen_smoke_alarm_status",
            "problem",
            "Eteinen Smoke Eteinen Smoke_alarm_status",
        ): None,
        (
            "binary_sensor.keitttio_vuoto_keitttio_vuoto_event_water_leak",
            "problem",
            "Keitttio-Vuoto Keitttio-Vuoto_event_water_leak",
        ): "leak",
        (
            "binary_sensor.nodeid_11_nodeid_11_cover_status",
            "opening",
            "nodeID_11 nodeID_11_cover_status",
        ): None,
        (
            "binary_sensor.takaovi_takaovi_door_state_simple",
            "door",
            "Takaovi Takaovi_door_state_simple",
        ): "door",
        (
            "binary_sensor.eteinen_smoke_eteinen_smoke_smoke_alarm",
            "smoke",
            "Eteinen Smoke Eteinen Smoke_smoke_alarm",
        ): "smoke",
        ("binary_sensor.varaston_tamper", "tamper", "Varaston tamper"): None,
    }
    for (entity_id, device_class, name), kind in cases.items():
        _set(hass, entity_id, "off", device_class, name)
        assert kind_for(hass.states.get(entity_id)) == kind, entity_id
    offered = {option["value"] for option in exportable_entity_options(hass, [])}
    assert "binary_sensor.eteinen_smoke_eteinen_smoke_alarm_status" not in offered
    assert "binary_sensor.nodeid_11_nodeid_11_cover_status" not in offered
