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
from functools import partial
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.core import HassJob, HassJobType, HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import instance_id
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.config_flow import exportable_entity_options
from custom_components.kameraposti.const import (
    CONF_CUSTOMER_ID,
    CONF_ENTITY_KINDS,
    CONF_EXPORTED_ENTITIES,
    CONF_HOST,
    DEFAULT_SENSOR_LIMIT,
    DOMAIN,
    MAX_EXPORTED_SENSORS,
)
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.frigate_events import frigate_detection_names
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
            "kameraposti/3/anturit/_ha/sync",
        ]
        assert json.loads(calls[0][1])["kind"] == "leak"
        assert calls[1][1:] == ("dry", False)
        # 1.12.0: this Home Assistant (login + installation id), every chosen entity, after the descriptions.
        source = f"kp-3.{await instance_id.async_get(hass)}"
        assert json.loads(calls[0][1])["source"] == source
        assert json.loads(calls[2][1]) == {
            "source": source,
            "selection_saved": True,
            "entities": ["binary_sensor.kellari"],
        }
        assert calls[2][2] is False

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
        "kameraposti/3/anturit/_ha/sync",
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
    """1.10.0: the options open straight on the sensors (1.6.0-1.9.x: a menu of sensors / cameras)."""
    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] is FlowResultType.FORM
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


async def test_options_flow_saves_more_than_the_default_until_kameraposti_tells_the_limit(
    hass: HomeAssistant,
) -> None:
    """Design review TOP 8 (2026-10-18): without a limit from Kameraposti (the entry not running, or no
    roster yet) the form does not refuse at the default 20 -- a 30-sensor plan could not be saved right
    after a restart -- and says the limit is not known yet. (1.8.0 refused at 20.)"""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)
    for i in range(DEFAULT_SENSOR_LIMIT + 1):
        _set(hass, f"binary_sensor.vuoto_{i}", "off", "moisture", f"Vuoto {i}")

    result = await _open_sensor_options(hass, entry)
    assert result["description_placeholders"]["limit_note"] == (
        "Your plan's sensor limit has not arrived from Kameraposti yet; "
        "if you choose more than it takes, the rest are left out."
    )
    chosen = [f"binary_sensor.vuoto_{i}" for i in range(DEFAULT_SENSOR_LIMIT + 1)]
    result2 = await hass.config_entries.options.async_configure(
        result["flow_id"], {CONF_EXPORTED_ENTITIES: chosen}
    )

    assert result2["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_EXPORTED_ENTITIES] == chosen


async def test_before_the_limit_is_known_only_the_hard_maximum_is_refused(hass: HomeAssistant) -> None:
    """Fable audit 2026-10-18 L6: without a limit from Kameraposti, 200 is not called the plan limit."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)
    chosen = [f"binary_sensor.vuoto_{i}" for i in range(MAX_EXPORTED_SENSORS + 1)]
    for i, entity_id in enumerate(chosen):
        _set(hass, entity_id, "off", "moisture", f"Vuoto {i}")

    result = await _open_sensor_options(hass, entry)
    refused = await hass.config_entries.options.async_configure(
        result["flow_id"], {CONF_EXPORTED_ENTITIES: chosen}
    )

    assert refused["errors"] == {CONF_EXPORTED_ENTITIES: "too_many_max"}
    assert refused["description_placeholders"]["max"] == str(MAX_EXPORTED_SENSORS)


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
        assert (
            result["description_placeholders"]["limit_note"]
            == f"Your Kameraposti plan takes at most {effective}."
        )
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
    # Design review TOP 8: an invalid limit is no limit yet -- the form waits for a valid one.
    assert result["description_placeholders"]["max"] == str(MAX_EXPORTED_SENSORS)
    note = result["description_placeholders"]["limit_note"]
    assert note.startswith("Your plan's sensor limit has not arrived")


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
    """The entities whose state was sent, in order (descriptions and the 1.12.0 list left out)."""
    return [
        topic.split("/")[3]
        for topic, _, _ in published
        if not topic.endswith("/config") and not topic.endswith("/_ha/sync")
    ]


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


async def test_nothing_is_logged_while_every_chosen_sensor_is_sent(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """The roster arrives on every start and reconnect, and a plan change that leaves nothing out is
    no news: no line in the log."""
    entity_ids = _doors(hass, 25)
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=entity_ids, publish=publish, limit=30
    )
    exporter.async_start()
    with caplog.at_level(logging.INFO):
        exporter.async_set_limit(30)
        exporter.async_set_limit(40)
        exporter.async_set_limit(25)
    assert _kameraposti_logs(caplog, logging.INFO) == []
    assert _kameraposti_logs(caplog, logging.WARNING) == []
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


async def test_options_flow_asks_the_kind_of_a_sensor_whose_kind_cannot_be_read(hass: HomeAssistant) -> None:
    """1.13.0 (steelo 2026-10-19 "muuta niin että näkyy kaikki"): the general purpose sensor is offered
    and its kind is asked on the next page (1.3.2-1.12.x refused it as "unknown_kind")."""
    leak = "binary_sensor.keitttio_vuoto_event_water_leak"
    _set(hass, leak, "off", "problem", "Keitttio-Vuoto_event_water_leak")
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
        result = await _open_sensor_options(hass, entry)
        selector = result["data_schema"].schema[CONF_EXPORTED_ENTITIES]
        offered = {option["value"] for option in selector.config["options"]}
        assert "binary_sensor.keitttio_vuoto_event_general_purpose" in offered
        kinds = await hass.config_entries.options.async_configure(
            result["flow_id"],
            {
                CONF_EXPORTED_ENTITIES: [
                    "binary_sensor.keitttio_vuoto_event_water_leak",
                    "binary_sensor.keitttio_vuoto_event_general_purpose",
                ]
            },
        )
        assert kinds["type"] is FlowResultType.FORM
        assert kinds["step_id"] == "kinds"
        # Only the one whose kind cannot be read is asked.
        asked = [str(key) for key in kinds["data_schema"].schema]
        assert asked == ["binary_sensor.keitttio_vuoto_event_general_purpose"]
        saved = await hass.config_entries.options.async_configure(
            kinds["flow_id"], {"binary_sensor.keitttio_vuoto_event_general_purpose": "leak"}
        )
        await hass.async_block_till_done()

    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_EXPORTED_ENTITIES] == [
        "binary_sensor.keitttio_vuoto_event_water_leak",
        "binary_sensor.keitttio_vuoto_event_general_purpose",
    ]
    assert entry.options[CONF_ENTITY_KINDS] == {"binary_sensor.keitttio_vuoto_event_general_purpose": "leak"}


async def test_options_flow_still_names_a_chosen_entity_that_cannot_be_sent(hass: HomeAssistant) -> None:
    """A text sensor chosen before (no kind can be read or chosen) is named instead of dropped silently."""
    _set(hass, "sensor.ovi_teksti", "Window/door is open", None, "Ovi teksti")
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        options={CONF_EXPORTED_ENTITIES: ["sensor.ovi_teksti"]},
        version=2,
    )
    entry.add_to_hass(hass)

    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        result = await _open_sensor_options(hass, entry)
        refused = await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_EXPORTED_ENTITIES: ["sensor.ovi_teksti"]}
        )

    assert refused["errors"] == {CONF_EXPORTED_ENTITIES: "unknown_kind"}
    assert refused["description_placeholders"]["entities"] == "sensor.ovi_teksti"


async def test_every_binary_and_numeric_sensor_is_offered_but_not_the_integrations_own(
    hass: HomeAssistant,
) -> None:
    _set(hass, "binary_sensor.takaovi_any", "off", None, "Takaovi Any")
    _set(hass, "sensor.kasvihuone", "18.5", None, "Kasvihuone", unit="°C")
    _set(hass, "sensor.ovi_teksti", "Window/door is open", None, "Ovi teksti")
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    er.async_get(hass).async_get_or_create(
        "binary_sensor", DOMAIN, "yhteys", suggested_object_id="kameraposti_yhteys"
    )
    _set(hass, "binary_sensor.kameraposti_yhteys", "on", "connectivity", "Kameraposti yhteys")

    options = {option["value"]: option["label"] for option in exportable_entity_options(hass, [])}

    assert options["binary_sensor.takaovi_any"] == "Takaovi Any · kind chosen next"
    assert options["sensor.kasvihuone"] == "Kasvihuone · kind chosen next"
    assert options["binary_sensor.kellari"] == "Kellarin vuoto · Leak"
    assert "sensor.ovi_teksti" not in options
    assert "binary_sensor.kameraposti_yhteys" not in options
    # A chosen kind shows in the label.
    chosen = exportable_entity_options(hass, [], {"binary_sensor.takaovi_any": "door"})
    assert {o["value"]: o["label"] for o in chosen}["binary_sensor.takaovi_any"] == "Takaovi Any · Door"


async def test_a_chosen_kind_is_described_and_sent_like_a_read_one(hass: HomeAssistant) -> None:
    _set(hass, "binary_sensor.takaovi_any", "on", None, "Takaovi")
    _set(hass, "sensor.kasvihuone", "18.5", None, "Kasvihuone", unit="°C")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=["binary_sensor.takaovi_any", "sensor.kasvihuone"],
        publish=publish,
        kinds={"binary_sensor.takaovi_any": "door", "sensor.kasvihuone": "temperature"},
    )
    exporter.async_start()

    exporter.publish_snapshot()

    configs = {
        topic: json.loads(payload)["kind"] for topic, payload, _ in published if topic.endswith("/config")
    }
    assert configs == {
        "kameraposti/3/anturit/binary_sensor.takaovi_any/config": "door",
        "kameraposti/3/anturit/sensor.kasvihuone/config": "temperature",
    }
    states = [(topic, payload) for topic, payload, _ in published if not topic.endswith("/config")]
    assert ("kameraposti/3/anturit/binary_sensor.takaovi_any", "open") in states
    assert ("kameraposti/3/anturit/sensor.kasvihuone", "18.5") in states
    exporter.async_stop()

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


async def test_export_list_offers_everything_that_can_be_sent(hass: HomeAssistant) -> None:
    """1.4.1: a Z-Wave gas level without a device class (steelo 2026-10-06: "sensor_gas_carbon_monoxide" was
    missing) is offered with its kind. 1.13.0: every other binary and numeric sensor too (its kind is chosen
    next); an entity that cannot be sent at all (text sensor, switch) is not."""
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
        # 1.13.0: offered, kind chosen next.
        "binary_sensor.keitttio_vuoto_event_general_purpose",
        "sensor.olohuone_kosteus",
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
    # 1.13.0: listed (every binary sensor is), but never as smoke or door by itself: the kind is chosen next.
    labels = {option["value"]: option["label"] for option in exportable_entity_options(hass, [])}
    assert labels["binary_sensor.eteinen_smoke_eteinen_smoke_alarm_status"].endswith("· kind chosen next")
    assert labels["binary_sensor.nodeid_11_nodeid_11_cover_status"].endswith("· kind chosen next")


# -- 1.11.1: Frigate's own binary sensors are never sent ----------------------------------------
#
# steelo 2026-10-17: Frigate's pixel motion (binary_sensor.<camera>_motion) alarmed every five minutes
# while nothing happened. Frigate reaches Kameraposti only through its detections ("<camera> Frigate").


def _frigate_sensors(hass: HomeAssistant) -> None:
    """Frigate's pixel motion and person occupancy of kamera1, and a PIR of another integration."""
    registry = er.async_get(hass)
    frigate = MockConfigEntry(domain="frigate")
    frigate.add_to_hass(hass)
    for platform, unique_id, object_id, device_class, name in (
        ("frigate", "x:motion_sensor:kamera1", "kamera1_motion", "motion", "Kamera1 Motion"),
        (
            "frigate",
            "x:occupancy_sensor:kamera1_person",
            "kamera1_person_occupancy",
            "occupancy",
            "Kamera1 Person",
        ),
        ("zha", "pir-1", "piha_liike", "motion", "Pihan liike"),
    ):
        registry.async_get_or_create(
            "binary_sensor",
            platform,
            unique_id,
            suggested_object_id=object_id,
            config_entry=frigate if platform == "frigate" else None,
        )
        _set(hass, f"binary_sensor.{object_id}", "off", device_class, name)


async def test_frigates_own_sensors_are_not_sent(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    _frigate_sensors(hass)
    published, publish = _recorder()
    with caplog.at_level(logging.INFO, logger="custom_components.kameraposti"):
        exporter = KameraportiSensorExporter(
            hass,
            customer_id=CUSTOMER_ID,
            entity_ids=[
                "binary_sensor.kamera1_motion",
                "binary_sensor.kamera1_person_occupancy",
                "binary_sensor.piha_liike",
            ],
            publish=publish,
            limit=1,
        )
    exporter.async_start()

    exporter.publish_snapshot()
    for entity_id in ("binary_sensor.kamera1_motion", "binary_sensor.kamera1_person_occupancy"):
        hass.states.async_set(entity_id, "on", hass.states.get(entity_id).attributes)
    hass.states.async_set(
        "binary_sensor.piha_liike", "on", hass.states.get("binary_sensor.piha_liike").attributes
    )
    await hass.async_block_till_done()

    # Left out before the limit is counted: the plan's one sensor is the PIR.
    assert {topic for topic, _, _ in published} == {
        "kameraposti/3/anturit/binary_sensor.piha_liike/config",
        "kameraposti/3/anturit/binary_sensor.piha_liike",
    }
    assert (
        "Kameraposti: not sending Frigate's own sensors binary_sensor.kamera1_motion, "
        "binary_sensor.kamera1_person_occupancy -- Frigate's detections go to Kameraposti as "
        '"<camera> Frigate"'
    ) in _kameraposti_logs(caplog, logging.INFO)


async def test_the_options_no_longer_offer_or_keep_frigates_own_sensors(hass: HomeAssistant) -> None:
    _frigate_sensors(hass)
    offered = [option["value"] for option in exportable_entity_options(hass, [])]
    assert offered == ["binary_sensor.piha_liike"]

    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        options={CONF_EXPORTED_ENTITIES: ["binary_sensor.kamera1_motion", "binary_sensor.piha_liike"]},
        version=2,
    )
    entry.add_to_hass(hass)
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        result = await _open_sensor_options(hass, entry)
        schema = result["data_schema"].schema
        default = next(key for key in schema if key == CONF_EXPORTED_ENTITIES).default()
        assert default == ["binary_sensor.piha_liike"]
        result2 = await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_EXPORTED_ENTITIES: default}
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_EXPORTED_ENTITIES] == ["binary_sensor.piha_liike"]


async def test_the_selection_list_names_every_chosen_entity_after_the_descriptions(
    hass: HomeAssistant,
) -> None:
    """1.12.0 (steelo 2026-10-18 "HA:n valinta synkronoi ja tiputtaa muut"): Kameraposti drops the sensors
    this Home Assistant created that are not on the list. Every chosen entity is on it -- also one over
    the plan's limit (a lower plan never drops a sensor) and one with no state yet (Z-Wave still starting)."""
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    _set(hass, "binary_sensor.ovi", "off", "door", "Etuovi")
    long_id = "binary_sensor." + "pitka_nimi_" * 6
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=["binary_sensor.kellari", "binary_sensor.ei_viela", "binary_sensor.ovi", long_id],
        publish=publish,
        limit=2,
        source="kp-3-2",
        selection_saved=True,
    )
    exporter.async_start()
    exporter.publish_snapshot()

    topic, payload, retain = published[-1]
    assert topic == "kameraposti/3/anturit/_ha/sync"
    assert retain is False
    assert json.loads(payload) == {
        "source": "kp-3-2",
        "selection_saved": True,
        "entities": [
            "binary_sensor.kellari",
            "binary_sensor.ei_viela",
            "binary_sensor.ovi",
            mqtt_name_for(long_id),
        ],
    }
    assert [json.loads(p)["source"] for t, p, _ in published if t.endswith("/config")] == ["kp-3-2"]
    exporter.async_stop()


async def test_no_list_goes_out_when_a_description_could_not_be_sent(hass: HomeAssistant) -> None:
    """A list without its new sensors' descriptions would be fine, but a broken connection sends neither."""
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    published: list[str] = []

    def publish(topic: str, payload: str, retain: bool) -> bool:
        published.append(topic)
        return False

    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=["binary_sensor.kellari"],
        publish=publish,
        source="kp-3",
        selection_saved=True,
    )
    exporter.async_start()
    exporter.publish_snapshot()
    assert published == ["kameraposti/3/anturit/binary_sensor.kellari/config"]
    exporter.async_stop()


async def test_an_empty_selection_sends_the_empty_list_every_15_minutes(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """Nothing chosen = nothing of this Home Assistant stays in Kameraposti; repeated like the snapshot,
    since Kameraposti's listener may be reconnecting when one goes out."""
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass, customer_id=CUSTOMER_ID, entity_ids=[], publish=publish, source="kp-3", selection_saved=True
    )
    exporter.async_start()
    exporter.publish_snapshot()
    # Bug hunt 2026-10-19: Kameraposti takes a list without any sensor on it only with selection_saved
    # (Home Assistant 1.12-1.14.0 sent an empty one from a re-added entry).
    assert published == [
        (
            "kameraposti/3/anturit/_ha/sync",
            '{"source": "kp-3", "selection_saved": true, "entities": []}',
            False,
        )
    ]

    freezer.tick(15 * 60 + 1)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert len(published) == 2

    exporter.async_stop()
    freezer.tick(16 * 60)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert len(published) == 2


async def test_without_a_login_no_list_is_sent(hass: HomeAssistant) -> None:
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=["binary_sensor.kellari"],
        publish=publish,
        selection_saved=True,
    )
    exporter.async_start()
    exporter.publish_snapshot()
    assert [t for t, _, _ in published] == [
        "kameraposti/3/anturit/binary_sensor.kellari/config",
        "kameraposti/3/anturit/binary_sensor.kellari",
    ]
    assert "source" not in json.loads(published[0][1])
    exporter.async_stop()


async def test_a_long_list_goes_out_in_parts_under_the_brokers_limit(hass: HomeAssistant) -> None:
    """Fable audit M2: the broker drops a message over 4096 bytes without a word. 200 long names go
    out as parts of one snapshot; Kameraposti drops only once every part has come."""
    entity_ids = [f"binary_sensor.pitka_anturin_nimi_{n:03d}_x" for n in range(200)]
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=entity_ids,
        publish=publish,
        source="kp-3.abc",
        selection_saved=True,
    )
    exporter.async_start()
    exporter.publish_snapshot()

    payloads = [payload for topic, payload, _ in published if topic.endswith("/_ha/sync")]
    parts = [json.loads(payload) for payload in payloads]
    assert len(parts) > 1
    assert all(len(payload.encode()) <= 3500 for payload in payloads)
    assert {p["snapshot"] for p in parts} == {parts[0]["snapshot"]}
    assert [(p["part"], p["parts"]) for p in parts] == [(n, len(parts)) for n in range(1, len(parts) + 1)]
    assert [name for p in parts for name in p["entities"]] == entity_ids
    assert all(p["source"] == "kp-3.abc" for p in parts)
    # Every part says the list is a saved selection (Kameraposti reads it from the parts).
    assert all(
        list(p) == ["source", "selection_saved", "snapshot", "part", "parts", "entities"] for p in parts
    )
    assert all(p["selection_saved"] is True for p in parts)
    exporter.async_stop()


async def test_the_frigate_cameras_detection_sensors_are_on_the_list(hass: HomeAssistant) -> None:
    """Fable audit M3: a camera's detection sensor is the camera's; a long (hashed) name too."""
    registry = er.async_get(hass)
    registry.async_get_or_create("camera", "frigate", "frigate:camera:piha", suggested_object_id="piha")
    long_object_id = "erittain_pitka_kameran_nimi_jota_kukaan_ei_jaksa_kirjoittaa"
    registry.async_get_or_create(
        "camera", "frigate", "frigate:camera:pitka", suggested_object_id=long_object_id
    )
    registry.async_get_or_create("camera", "demo", "demo-1", suggested_object_id="muu")
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=[],
        publish=publish,
        source="kp-3.abc",
        extra_names=partial(frigate_detection_names, hass),
        selection_saved=True,
    )
    exporter.async_start()
    exporter.publish_snapshot()

    [(_, payload, _)] = published
    assert json.loads(payload)["entities"] == [
        "binary_sensor.piha_frigate",
        mqtt_name_for(f"binary_sensor.{long_object_id}_frigate"),
    ]
    exporter.async_stop()


async def test_an_unreadable_installation_id_sends_no_list_and_setup_goes_on(hass: HomeAssistant) -> None:
    """Fable audit L-D: the id's store can fail (disk full); then nothing is synced, nothing is dropped."""
    _set(hass, "binary_sensor.kellari", "off", "moisture", "Kellarin vuoto")
    with patch(
        "custom_components.kameraposti.coordinator.instance_id.async_get", side_effect=OSError("disk full")
    ):
        coordinator, client = await _connected_coordinator(hass, ["binary_sensor.kellari"])
    calls = [c.args for c in client.publish.call_args_list]
    assert [c[0] for c in calls] == [
        "kameraposti/3/anturit/binary_sensor.kellari/config",
        "kameraposti/3/anturit/binary_sensor.kellari",
    ]
    assert "source" not in json.loads(calls[0][1])
    await coordinator.async_stop()


# 1.14.1 (Fable bug hunt H-H1): a new or re-added entry has no saved selection. Its first snapshot sent an
# empty list (only the Frigate detection sensors' names on it), and Kameraposti dropped every sensor this
# Home Assistant had described before: the source (login + installation id) survives removing and adding
# the entry.


def _frigate_camera(hass: HomeAssistant) -> None:
    er.async_get(hass).async_get_or_create(
        "camera", "frigate", "frigate:camera:piha", suggested_object_id="piha"
    )


async def test_no_list_goes_out_before_a_selection_has_been_saved(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """Not on connect, not with the 15-minute resend -- also not with a Frigate camera's detection sensor
    as the only name on it."""
    _frigate_camera(hass)
    published, publish = _recorder()
    exporter = KameraportiSensorExporter(
        hass,
        customer_id=CUSTOMER_ID,
        entity_ids=[],
        publish=publish,
        source="kp-3.abc",
        extra_names=partial(frigate_detection_names, hass),
    )
    exporter.async_start()
    exporter.publish_snapshot()
    freezer.tick(15 * 60 + 1)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()

    assert published == []
    exporter.async_stop()


async def test_a_re_added_entry_drops_nothing_until_its_selection_is_saved(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """steelo 2026-10-19 removed the CAM entry and added it again. Now the new entry sends no list -- on
    connect, on the resend -- until the options are saved; saving them (here deliberately with nothing
    chosen) sends it."""
    _frigate_camera(hass)
    clients: list[MagicMock] = []

    def _client(*args: object, **kwargs: object) -> MagicMock:
        client = MagicMock()
        client.async_start = AsyncMock()
        client.async_stop = AsyncMock()
        client.publish.return_value = True
        clients.append(client)
        return client

    def _lists(client: MagicMock) -> list[dict]:
        return [
            json.loads(c.args[1]) for c in client.publish.call_args_list if c.args[0].endswith("/_ha/sync")
        ]

    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", side_effect=_client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        hass.data[DOMAIN][entry.entry_id]._handle_state_change(ConnectionState.CONNECTED)
        freezer.tick(15 * 60 + 1)
        async_fire_time_changed(hass)
        await hass.async_block_till_done()
        assert _lists(clients[0]) == []

        result = await _open_sensor_options(hass, entry)
        await hass.config_entries.options.async_configure(result["flow_id"], {CONF_EXPORTED_ENTITIES: []})
        await hass.async_block_till_done()
        assert len(clients) == 2  # saving reloaded the entry
        hass.data[DOMAIN][entry.entry_id]._handle_state_change(ConnectionState.CONNECTED)
        await hass.async_block_till_done()

    source = f"kp-3.{await instance_id.async_get(hass)}"
    assert _lists(clients[1]) == [
        {"source": source, "selection_saved": True, "entities": ["binary_sensor.piha_frigate"]}
    ]
    assert await hass.config_entries.async_unload(entry.entry_id)
