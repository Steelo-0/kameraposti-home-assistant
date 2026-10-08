"""Connection errors and recovery through a real config entry: the coordinator and the real
KameraportiMqttClient, with only paho-mqtt's Client mocked (no network I/O).

The MQTT client runs connect() on an executor thread and paho calls back on its own network
thread; everything that touches Home Assistant (state reports -> dispatcher) or the event loop
(timers) must be handed to the loop first. Home Assistant raises RuntimeError for a dispatcher
send from another thread (helpers/frame.py report_non_thread_safe_operation, ERROR for custom
integrations), and the test loop runs in asyncio debug mode, which raises for loop.call_later
from another thread. On top of those checks, ``loop_thread_calls`` records the thread of every
state report and reconnect scheduling, so these tests do not depend on either check staying on.

Exported sensors across a drop: paho accepts a QoS 1 publish (publish() returns True) long before
the broker acknowledges it, and a new paho client is built for every connection attempt, so what
was not acknowledged when the connection dropped must be sent again by the exporter.
"""

from __future__ import annotations

import itertools
import socket
import ssl
import threading
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import paho.mqtt.client as mqtt
import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_EXPORTED_ENTITIES, CONF_HOST, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.mqtt_client import ConnectionState, KameraportiMqttClient

ENTRY_DATA = {CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: 3, CONF_USERNAME: "kp-3", CONF_PASSWORD: "x"}


@pytest.fixture
def paho() -> Generator[MagicMock]:
    with patch("custom_components.kameraposti.mqtt_client.mqtt.Client") as mock_cls:
        instance = MagicMock(name="paho")
        mock_cls.return_value = instance
        yield instance


@pytest.fixture
def loop_thread_calls(hass: HomeAssistant) -> Generator[list[tuple[str, bool]]]:
    """(method, ran on the event loop thread) for every state report and reconnect scheduling."""
    calls: list[tuple[str, bool]] = []
    report_state = KameraportiMqttClient._report_state
    schedule_reconnect = KameraportiMqttClient._schedule_reconnect

    def recording_report_state(self: KameraportiMqttClient, state: ConnectionState) -> None:
        calls.append(("_report_state", threading.get_ident() == hass.loop_thread_id))
        report_state(self, state)

    def recording_schedule_reconnect(self: KameraportiMqttClient) -> None:
        calls.append(("_schedule_reconnect", threading.get_ident() == hass.loop_thread_id))
        schedule_reconnect(self)

    with (
        patch.object(KameraportiMqttClient, "_report_state", recording_report_state),
        patch.object(KameraportiMqttClient, "_schedule_reconnect", recording_schedule_reconnect),
    ):
        yield calls


async def _setup_entry(hass: HomeAssistant, options: dict | None = None) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN, data=ENTRY_DATA, options=options or {}, version=2, unique_id="kp-3"
    )
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


def _coordinator(hass: HomeAssistant, entry: MockConfigEntry) -> KameraportiCoordinator:
    return hass.data[DOMAIN][entry.entry_id]


async def _run_due_reconnect(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, seconds: float = 31
) -> None:
    """Let the pending reconnect timer (at most 30 s + 1 s jitter) fire and its attempt finish."""
    freezer.tick(seconds)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()


async def test_setup_with_the_network_down_loads_the_entry_and_keeps_retrying(
    hass: HomeAssistant,
    paho: MagicMock,
    loop_thread_calls: list[tuple[str, bool]],
    freezer: FrozenDateTimeFactory,
) -> None:
    """Home Assistant boots while DNS/the network is down: connect() raises gaierror on the
    executor. Reporting RECONNECTING from there raised RuntimeError (dispatcher send off the
    loop), so setup failed for good -- no retry, no alarms until the next restart."""
    paho.connect.side_effect = socket.gaierror(-3, "Temporary failure in name resolution")

    entry = await _setup_entry(hass)

    assert entry.state is ConfigEntryState.LOADED
    coordinator = _coordinator(hass, entry)
    assert coordinator.connection_state is ConnectionState.RECONNECTING
    assert coordinator._client._reconnect_handle is not None

    # The retry fails the same way and schedules the next one.
    await _run_due_reconnect(hass, freezer)
    assert paho.connect.call_count == 2
    assert coordinator._client._reconnect_handle is not None
    assert entry.state is ConfigEntryState.LOADED

    assert ("_schedule_reconnect", True) in loop_thread_calls
    assert all(on_loop for _, on_loop in loop_thread_calls), loop_thread_calls
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_tls_errors_during_an_outage_keep_the_reconnect_loop_alive(
    hass: HomeAssistant,
    paho: MagicMock,
    loop_thread_calls: list[tuple[str, bool]],
    freezer: FrozenDateTimeFactory,
) -> None:
    """An outage where the reconnect attempts fail in the TLS handshake (ssl.SSLError). The
    first one reported TLS_FAILURE from the executor, which raised and killed the reconnect
    task before the next attempt was scheduled: disconnected until Home Assistant restarted."""
    entry = await _setup_entry(hass)
    assert entry.state is ConfigEntryState.LOADED
    coordinator = _coordinator(hass, entry)
    paho.on_connect(paho, None, MagicMock(), 0, None)
    await hass.async_block_till_done()
    assert coordinator.connection_state is ConnectionState.CONNECTED

    paho.connect.side_effect = ssl.SSLError("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred")
    paho.on_disconnect(paho, None, MagicMock(), None, None)
    await hass.async_block_till_done()
    assert coordinator.connection_state is ConnectionState.RECONNECTING

    for attempt in (2, 3):
        await _run_due_reconnect(hass, freezer)
        assert paho.connect.call_count == attempt
        assert coordinator.connection_state is ConnectionState.TLS_FAILURE
        # The next attempt is already scheduled.
        assert coordinator._client._reconnect_handle is not None

    assert entry.state is ConfigEntryState.LOADED
    assert all(on_loop for _, on_loop in loop_thread_calls), loop_thread_calls
    assert await hass.config_entries.async_unload(entry.entry_id)


class _Broker:
    """paho's side of the publishes: publish() accepts every message with the next message id,
    ack() is the broker's PUBACK (paho calls on_publish on its network thread)."""

    def __init__(self, paho: MagicMock) -> None:
        self._paho = paho
        self._mids = itertools.count(1)
        self._acked: set[int] = set()
        self.sent: list[tuple[str, str, int]] = []
        paho.is_connected.return_value = True
        paho.publish.side_effect = self._publish

    def _publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> MagicMock:
        mid = next(self._mids)
        self.sent.append((topic, payload, mid))
        return MagicMock(rc=mqtt.MQTT_ERR_SUCCESS, mid=mid)

    def ack(self, topic: str | None = None) -> None:
        for sent_topic, _, mid in self.sent:
            if mid not in self._acked and topic in (None, sent_topic):
                self._acked.add(mid)
                self._paho.on_publish(self._paho, None, mid, ReasonCode(PacketTypes.PUBACK), None)

    def sent_since(self, count: int) -> list[tuple[str, str]]:
        return [(topic, payload) for topic, payload, _ in self.sent[count:]]


async def _drop_and_reconnect(hass: HomeAssistant, paho: MagicMock, freezer: FrozenDateTimeFactory) -> None:
    """The connection drops; the first reconnect (1 s + up to 1 s jitter) gets its CONNACK."""
    paho.on_disconnect(paho, None, MagicMock(), None, None)
    await hass.async_block_till_done()
    await _run_due_reconnect(hass, freezer, 2)
    paho.on_connect(paho, None, MagicMock(), 0, None)
    await hass.async_block_till_done()


@pytest.mark.parametrize(
    ("entity_id", "attributes", "idle", "alarm", "payload"),
    [
        ("binary_sensor.kellari", {"device_class": "moisture"}, "off", "on", "leak"),
        # A CO meter: its last sent alarm state must not stop the resend.
        ("sensor.co", {"device_class": "carbon_monoxide", "unit_of_measurement": "ppm"}, "10", "80", "gas"),
    ],
)
async def test_an_alarm_the_broker_never_acknowledged_goes_out_again_at_once(
    hass: HomeAssistant,
    paho: MagicMock,
    freezer: FrozenDateTimeFactory,
    entity_id: str,
    attributes: dict[str, str],
    idle: str,
    alarm: str,
    payload: str,
) -> None:
    """An alarm paho accepted but the broker had not acknowledged when the connection dropped was
    lost from the fast path: only publishes that returned False were remembered, and the full
    snapshot waits up to a minute after a reconnect. It now goes out as soon as the connection is
    back; a sensor whose message the broker acknowledged is not sent again."""
    hass.states.async_set(entity_id, idle, {"friendly_name": "Hälytin", **attributes})
    hass.states.async_set("binary_sensor.ovi", "off", {"friendly_name": "Etuovi", "device_class": "door"})
    broker = _Broker(paho)
    entry = await _setup_entry(hass, {CONF_EXPORTED_ENTITIES: [entity_id, "binary_sensor.ovi"]})
    paho.on_connect(paho, None, MagicMock(), 0, None)
    await hass.async_block_till_done()
    assert len(broker.sent) == 4  # the first connect describes both and sends their states
    broker.ack()

    freezer.tick(5)
    hass.states.async_set(entity_id, alarm, {"friendly_name": "Hälytin", **attributes})
    hass.states.async_set("binary_sensor.ovi", "on", {"friendly_name": "Etuovi", "device_class": "door"})
    await hass.async_block_till_done()
    broker.ack("kameraposti/3/anturit/binary_sensor.ovi")  # the alarm's PUBACK never comes
    before_drop = len(broker.sent)

    await _drop_and_reconnect(hass, paho, freezer)

    assert broker.sent_since(before_drop) == [(f"kameraposti/3/anturit/{entity_id}", payload)]
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_a_description_lost_in_a_drop_is_sent_again_before_the_state(
    hass: HomeAssistant, paho: MagicMock, freezer: FrozenDateTimeFactory
) -> None:
    """Kameraposti creates the sensor from its description: a lost one is sent again too."""
    hass.states.async_set(
        "binary_sensor.kellari", "on", {"friendly_name": "Kellari", "device_class": "moisture"}
    )
    broker = _Broker(paho)
    entry = await _setup_entry(hass, {CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"]})
    paho.on_connect(paho, None, MagicMock(), 0, None)
    await hass.async_block_till_done()
    assert [topic for topic, _ in broker.sent_since(0)] == [
        "kameraposti/3/anturit/binary_sensor.kellari/config",
        "kameraposti/3/anturit/binary_sensor.kellari",
    ]
    before_drop = len(broker.sent)  # neither acknowledged

    freezer.tick(5)
    await _drop_and_reconnect(hass, paho, freezer)

    assert broker.sent_since(before_drop) == broker.sent_since(0)[:before_drop]
    assert await hass.config_entries.async_unload(entry.entry_id)
