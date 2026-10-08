"""Connection errors and recovery through a real config entry: the coordinator and the real
KameraportiMqttClient, with only paho-mqtt's Client mocked (no network I/O).

The MQTT client runs connect() on an executor thread and paho calls back on its own network
thread; everything that touches Home Assistant (state reports -> dispatcher) or the event loop
(timers) must be handed to the loop first. Home Assistant raises RuntimeError for a dispatcher
send from another thread (helpers/frame.py report_non_thread_safe_operation, ERROR for custom
integrations), and the test loop runs in asyncio debug mode, which raises for loop.call_later
from another thread. On top of those checks, ``loop_thread_calls`` records the thread of every
state report and reconnect scheduling, so these tests do not depend on either check staying on.
"""

from __future__ import annotations

import socket
import ssl
import threading
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_HOST, DOMAIN
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


async def _run_due_reconnect(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Let the pending reconnect timer (at most 30 s + 1 s jitter) fire and its attempt finish."""
    freezer.tick(31)
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
