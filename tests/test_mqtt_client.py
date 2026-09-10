"""MQTT client tests (contract sections 9/10/12): connect/subscribe wiring,
connection-state transitions, auth-failure detection, reconnect scheduling,
clean stop. paho.mqtt.client.Client itself is mocked throughout -- no real
network I/O happens in this test file.
"""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant

from custom_components.kameraposti.mqtt_client import (
    ConnectionState,
    KameraportiMqttClient,
    _Backoff,
)

CUSTOMER_ID = 3


@pytest.fixture
def mock_paho_client() -> Generator[MagicMock]:
    with patch("custom_components.kameraposti.mqtt_client.mqtt.Client") as mock_cls:
        instance = MagicMock(name="paho_client_instance")
        mock_cls.return_value = instance
        yield instance


def _make_client(hass: HomeAssistant, states: list[ConnectionState]) -> KameraportiMqttClient:
    return KameraportiMqttClient(
        hass,
        customer_id=CUSTOMER_ID,
        username="rk-3-abc",
        password="secret",
        on_message=lambda topic, payload: None,
        on_state_change=states.append,
    )


async def test_start_connects_with_expected_transport_and_subscribes_on_success(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    states: list[ConnectionState] = []
    client = _make_client(hass, states)

    await client.async_start()
    await hass.async_block_till_done()

    mock_paho_client.connect.assert_called_once()
    host, port = mock_paho_client.connect.call_args.args[:2]
    assert host == "tailscale2.steels.me"
    assert port == 443
    mock_paho_client.ws_set_options.assert_called_once_with(path="/mqtt")
    mock_paho_client.tls_set.assert_called_once()
    mock_paho_client.loop_start.assert_called_once()
    assert states == [ConnectionState.CONNECTING]

    # Simulate the broker's successful CONNACK arriving on paho's own thread.
    on_connect = mock_paho_client.on_connect
    on_connect(mock_paho_client, None, MagicMock(), 0, None)
    await hass.async_block_till_done()

    mock_paho_client.subscribe.assert_called_once_with(f"customers/{CUSTOMER_ID}/detections/+", qos=1)
    assert states[-1] == ConnectionState.CONNECTED

    await client.async_stop()


@pytest.mark.parametrize("auth_rc", [4, 5])
async def test_auth_rejection_reason_codes_report_auth_failure_and_do_not_subscribe(
    hass: HomeAssistant, mock_paho_client: MagicMock, auth_rc: int
) -> None:
    """N. Auth failure: handled state, no crash, no subscribe."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()

    on_connect = mock_paho_client.on_connect
    on_connect(mock_paho_client, None, MagicMock(), auth_rc, None)
    await hass.async_block_till_done()

    assert ConnectionState.AUTH_FAILURE in states
    mock_paho_client.subscribe.assert_not_called()
    # A pending reconnect must still be scheduled -- auth failure is not
    # a tight loop, but it is not a permanent give-up either (contract
    # section 12: reload must be able to fix it, and a rotated password
    # applied broker-side should eventually be picked up too).
    assert client._reconnect_handle is not None

    await client.async_stop()


async def test_connect_fail_before_any_connack_is_not_treated_as_auth_failure(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """TLS/network failures (on_connect_fail) must be distinguishable from
    broker-issued auth rejections (on_connect with a bad reason code)."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()

    on_connect_fail = mock_paho_client.on_connect_fail
    on_connect_fail(mock_paho_client, None)
    await hass.async_block_till_done()

    assert ConnectionState.AUTH_FAILURE not in states
    assert ConnectionState.RECONNECTING in states

    await client.async_stop()


async def test_disconnect_after_being_connected_schedules_a_reconnect(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """K. Reconnect: a drop schedules a reconnect attempt."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()
    mock_paho_client.on_connect(mock_paho_client, None, MagicMock(), 0, None)
    await hass.async_block_till_done()

    on_disconnect = mock_paho_client.on_disconnect
    on_disconnect(mock_paho_client, None, MagicMock(), None, None)
    await hass.async_block_till_done()

    assert states[-1] == ConnectionState.RECONNECTING
    assert client._reconnect_handle is not None

    await client.async_stop()


async def test_reconnect_attempt_reconnects_and_resubscribes_on_success(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """K. Reconnect: resubscribe happens again once the retry succeeds, and
    no duplicate/second client is left active."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()
    mock_paho_client.on_connect(mock_paho_client, None, MagicMock(), 0, None)
    await hass.async_block_till_done()

    mock_paho_client.on_disconnect(mock_paho_client, None, MagicMock(), None, None)
    await hass.async_block_till_done()
    assert client._reconnect_handle is not None

    # Fire the scheduled reconnect attempt directly instead of waiting out
    # the real backoff delay -- this tests "does a reconnect attempt work",
    # not "does asyncio's own timer fire on schedule".
    client._start_reconnect_task()
    await hass.async_block_till_done()

    assert mock_paho_client.connect.call_count == 2
    mock_paho_client.on_connect(mock_paho_client, None, MagicMock(), 0, None)
    await hass.async_block_till_done()

    assert mock_paho_client.subscribe.call_count == 2
    assert states[-1] == ConnectionState.CONNECTED

    await client.async_stop()


async def test_stop_cancels_pending_reconnect_and_disconnects_cleanly(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """L. Unload: disconnect, cancel pending timers, stop reconnecting."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()

    mock_paho_client.on_disconnect(mock_paho_client, None, MagicMock(), None, None)
    await hass.async_block_till_done()
    assert client._reconnect_handle is not None

    await client.async_stop()

    assert client._reconnect_handle is None
    mock_paho_client.disconnect.assert_called_once()
    mock_paho_client.loop_stop.assert_called_once()
    assert states[-1] == ConnectionState.STOPPED


async def test_disconnect_after_stop_does_not_schedule_a_new_reconnect(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """A disconnect callback firing during/after shutdown must not restart
    the reconnect loop (contract section 22: no reconnect during shutdown)."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()

    await client.async_stop()

    # paho can still call on_disconnect asynchronously right after
    # disconnect() -- must be a no-op once we are closing/closed.
    mock_paho_client.on_disconnect(mock_paho_client, None, MagicMock(), None, None)
    await hass.async_block_till_done()

    assert client._reconnect_handle is None


class TestBackoff:
    """Contract section 9: 1s -> 2s -> 4s -> 8s -> 16s -> 30s max, with jitter."""

    def test_delays_double_up_to_the_configured_maximum(self) -> None:
        backoff = _Backoff(minimum=1, maximum=30, jitter=0)

        delays = [backoff.next_delay() for _ in range(8)]

        assert delays == [1, 2, 4, 8, 16, 30, 30, 30]

    def test_jitter_adds_a_small_bounded_extra_delay(self) -> None:
        backoff = _Backoff(minimum=1, maximum=30, jitter=1.0)

        delay = backoff.next_delay()

        assert 1.0 <= delay < 2.0

    def test_reset_returns_to_the_minimum_delay(self) -> None:
        backoff = _Backoff(minimum=1, maximum=30, jitter=0)
        backoff.next_delay()
        backoff.next_delay()
        backoff.next_delay()

        backoff.reset()

        assert backoff.next_delay() == 1
