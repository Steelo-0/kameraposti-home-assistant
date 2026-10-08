"""MQTT client tests (contract sections 9/10/12): connect/subscribe wiring,
connection-state transitions, auth-failure detection, reconnect scheduling,
clean stop. paho.mqtt.client.Client itself is mocked throughout -- no real
network I/O happens in this test file.

Reason-code regression (see TestBlockingConnectionTestReasonCodes and the
test_handle_connect_accepts_a_real_*_reasoncode_object tests below): a
production WSS smoke test found that paho-mqtt 2.x's callback API hands
on_connect/on_subscribe real `paho.mqtt.reasoncodes.ReasonCode` objects,
which have no __int__ and are unhashable -- `int(reason_code)` and
`reason_code in <frozenset of ints>` both raise. Every test above this
point only ever fed callbacks a plain int, which is why 73 passing tests
still missed a bug that broke every real connection attempt.
"""

from __future__ import annotations

import logging
import ssl
import threading
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import paho.mqtt.client as mqtt
import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.core import HomeAssistant
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from custom_components.kameraposti.mqtt_client import (
    CannotConnect,
    ConnectionState,
    InvalidAuth,
    KameraportiMqttClient,
    _Backoff,
    _blocking_test_connection,
)

CUSTOMER_ID = 3
PROBE = {"host": "cam.steels.me", "customer_id": CUSTOMER_ID, "username": "kp-3"}


@pytest.fixture
def mock_paho_client() -> Generator[MagicMock]:
    with patch("custom_components.kameraposti.mqtt_client.mqtt.Client") as mock_cls:
        instance = MagicMock(name="paho_client_instance")
        mock_cls.return_value = instance
        yield instance


def _make_client(hass: HomeAssistant, states: list[ConnectionState]) -> KameraportiMqttClient:
    return KameraportiMqttClient(
        hass,
        host="cam.steels.me",
        customer_id=CUSTOMER_ID,
        username="kp-3",
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
    assert host == "cam.steels.me"
    assert port == 443
    mock_paho_client.ws_set_options.assert_called_once_with(path="/mqtt")
    # The broker pins the login to client id == username (kp-<id>).
    with patch("custom_components.kameraposti.mqtt_client.mqtt.Client") as mock_cls:
        client._build_client()
        assert mock_cls.call_args.kwargs["client_id"] == "kp-3"
    mock_paho_client.tls_set.assert_called_once()
    mock_paho_client.loop_start.assert_called_once()
    assert states == [ConnectionState.CONNECTING]

    # Simulate the broker's successful CONNACK arriving on paho's own thread.
    on_connect = mock_paho_client.on_connect
    on_connect(mock_paho_client, None, MagicMock(), 0, None)
    await hass.async_block_till_done()

    mock_paho_client.subscribe.assert_any_call(f"customers/{CUSTOMER_ID}/detections/+", qos=1)
    assert states[-1] == ConnectionState.CONNECTED

    await client.async_stop()


async def test_connect_subscribes_detections_security_roster_and_latest_photos(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """1.5.0: the roster comes before the latest photos, so a fresh connection
    knows the account's cameras before their retained photos arrive."""
    client = _make_client(hass, [])
    await client.async_start()
    await hass.async_block_till_done()

    client._handle_connect(mock_paho_client, None, None, 0)

    topics = [c.args[0] for c in mock_paho_client.subscribe.call_args_list]
    assert topics == [
        "customers/3/detections/+",
        "customers/3/security",
        "customers/3/security/result",
        "customers/3/cameras",
        "customers/3/cameras/+/latest",
    ]
    assert all(c.kwargs == {"qos": 1} for c in mock_paho_client.subscribe.call_args_list)
    await client.async_stop()


@pytest.mark.parametrize("refused", [[], ["customers/3/cameras", "customers/3/cameras/+/latest"]])
async def test_refused_subscriptions_are_logged_once_without_changing_anything(
    hass: HomeAssistant, mock_paho_client: MagicMock, caplog: pytest.LogCaptureFixture, refused: list[str]
) -> None:
    """Fable L-4: a broker whose ACL does not (yet) allow the 1.5.0 topics refuses them
    in its SUBACK; one debug line names them, the connection carries on as before."""
    caplog.set_level(logging.DEBUG, logger="custom_components.kameraposti.mqtt_client")
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()
    mids = iter(range(1, 100))
    mock_paho_client.subscribe.side_effect = lambda topic, qos: (mqtt.MQTT_ERR_SUCCESS, next(mids))

    client._handle_connect(mock_paho_client, None, None, 0)
    await hass.async_block_till_done()
    topics = [c.args[0] for c in mock_paho_client.subscribe.call_args_list]
    for mid, topic in enumerate(topics, start=1):
        code = 135 if topic in refused else 1  # 135 = not authorized
        client._handle_subscribe(
            mock_paho_client, None, mid, [ReasonCode(PacketTypes.SUBACK, identifier=code)], None
        )
    await hass.async_block_till_done()

    lines = [r.getMessage() for r in caplog.records if "refused" in r.getMessage()]
    if refused:
        assert len(lines) == 1
        assert all(topic in lines[0] for topic in refused)
        assert "detections" not in lines[0]
    else:
        assert lines == []
    assert states[-1] == ConnectionState.CONNECTED
    mock_paho_client.disconnect.assert_not_called()
    await client.async_stop()


async def test_publish_is_qos1_and_only_while_connected(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """Sensor export: nothing is queued while disconnected (the exporter re-sends on connect)."""
    client = _make_client(hass, [])
    assert client.publish("kameraposti/3/anturit/x", "leak") is False

    await client.async_start()
    await hass.async_block_till_done()
    mock_paho_client.is_connected.return_value = False
    assert client.publish("kameraposti/3/anturit/x", "leak") is False
    mock_paho_client.publish.assert_not_called()

    mock_paho_client.is_connected.return_value = True
    mock_paho_client.publish.return_value.rc = mqtt.MQTT_ERR_SUCCESS
    assert client.publish("kameraposti/3/anturit/x", "leak") is True
    mock_paho_client.publish.assert_called_once_with("kameraposti/3/anturit/x", "leak", qos=1, retain=False)

    mock_paho_client.publish.return_value.rc = mqtt.MQTT_ERR_NO_CONN
    assert client.publish("kameraposti/3/anturit/x", "dry") is False
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


async def test_handle_connect_accepts_a_real_successful_connack_reasoncode_object(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """Regression: a real ReasonCode has no __int__ and is unhashable --
    int(reason_code) and `reason_code in frozenset(...)` both raise for
    it even though every int-based test above passes. Uses paho's own
    convert_connack_rc_to_reason_code(), exactly what a real MQTTv3.1.1
    broker's CONNACK produces under the v2 callback API."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()

    success = mqtt.convert_connack_rc_to_reason_code(0)
    mock_paho_client.on_connect(mock_paho_client, None, MagicMock(), success, None)
    await hass.async_block_till_done()

    mock_paho_client.subscribe.assert_any_call(f"customers/{CUSTOMER_ID}/detections/+", qos=1)
    assert states[-1] == ConnectionState.CONNECTED

    await client.async_stop()


@pytest.mark.parametrize("v3_rc", [4, 5])
async def test_handle_connect_accepts_a_real_auth_failure_reasoncode_object(
    hass: HomeAssistant, mock_paho_client: MagicMock, v3_rc: int
) -> None:
    """Same regression as above, for the auth-failure path: paho remaps
    the old CONNACK codes 4/5 to ReasonCode values 134/135, never as a
    plain int, in real production traffic."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()

    failure = mqtt.convert_connack_rc_to_reason_code(v3_rc)
    mock_paho_client.on_connect(mock_paho_client, None, MagicMock(), failure, None)
    await hass.async_block_till_done()

    assert ConnectionState.AUTH_FAILURE in states
    mock_paho_client.subscribe.assert_not_called()

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

    # Kaksi yhteyttä (alku + uudelleen), kumpikin tilaa havainnot + turvatilan + tuloksen
    # + kameralistan + viimeisimmät kuvat.
    assert mock_paho_client.subscribe.call_count == 10
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


@pytest.fixture
def no_jitter() -> Generator[None]:
    with patch("custom_components.kameraposti.mqtt_client.random.uniform", return_value=0.0):
        yield


async def _connack(hass: HomeAssistant, paho: MagicMock) -> None:
    paho.on_connect(paho, None, MagicMock(), 0, None)
    await hass.async_block_till_done()


async def _drop(hass: HomeAssistant, client: KameraportiMqttClient, paho: MagicMock) -> float:
    """The broker drops the connection; returns the scheduled reconnect delay and runs that reconnect."""
    paho.on_disconnect(paho, None, MagicMock(), None, None)
    await hass.async_block_till_done()
    handle = client._reconnect_handle
    assert handle is not None
    delay = round(handle.when() - hass.loop.time(), 3)
    # Run the reconnect now instead of waiting out the delay (and never twice).
    handle.cancel()
    client._start_reconnect_task()
    await hass.async_block_till_done()
    return delay


async def test_a_connection_dropped_right_after_connack_keeps_backing_off(
    hass: HomeAssistant, mock_paho_client: MagicMock, freezer: FrozenDateTimeFactory, no_jitter: None
) -> None:
    """1.5.1: a login also used by another client (the broker pins client id == login) gets its
    CONNACK and is kicked out seconds later. Resetting the backoff on CONNACK made both sides
    reconnect every 1-2 s forever; the delay keeps doubling until a connection stays up."""
    client = _make_client(hass, [])
    await client.async_start()
    await hass.async_block_till_done()

    delays = []
    for _ in range(7):
        await _connack(hass, mock_paho_client)
        freezer.tick(2)
        delays.append(await _drop(hass, client, mock_paho_client))

    assert delays == [1, 2, 4, 8, 16, 30, 30]
    await client.async_stop()


async def test_only_a_connection_that_stays_up_a_minute_resets_the_backoff(
    hass: HomeAssistant, mock_paho_client: MagicMock, freezer: FrozenDateTimeFactory, no_jitter: None
) -> None:
    client = _make_client(hass, [])
    await client.async_start()
    await hass.async_block_till_done()
    for expected in (1, 2, 4):
        await _connack(hass, mock_paho_client)
        assert await _drop(hass, client, mock_paho_client) == expected

    # Up for 59 s: not stable yet, the next drop still backs off further.
    await _connack(hass, mock_paho_client)
    freezer.tick(59)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert await _drop(hass, client, mock_paho_client) == 8

    # Up for a full minute: stable, a later drop reconnects after 1 s again.
    await _connack(hass, mock_paho_client)
    freezer.tick(60)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    freezer.tick(600)
    assert await _drop(hass, client, mock_paho_client) == 1
    await client.async_stop()


async def test_stop_cancels_the_stable_connection_timer(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    client = _make_client(hass, [])
    await client.async_start()
    await hass.async_block_till_done()
    await _connack(hass, mock_paho_client)
    handle = client._stable_handle
    assert handle is not None

    await client.async_stop()

    assert handle.cancelled()
    assert client._stable_handle is None


def _shared_login_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == "custom_components.kameraposti.mqtt_client"
    ]


async def test_repeated_drops_right_after_connecting_warn_once_about_a_shared_login(
    hass: HomeAssistant,
    mock_paho_client: MagicMock,
    freezer: FrozenDateTimeFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """1.5.1: three drops within two minutes, each soon after connecting, are the signature of a
    second Home Assistant or bridge using the same login: say so once, in a WARNING."""
    client = _make_client(hass, [])
    await client.async_start()
    await hass.async_block_till_done()

    for _ in range(2):
        await _connack(hass, mock_paho_client)
        freezer.tick(3)
        await _drop(hass, client, mock_paho_client)
    assert _shared_login_warnings(caplog) == []

    await _connack(hass, mock_paho_client)
    freezer.tick(3)
    await _drop(hass, client, mock_paho_client)
    warnings = _shared_login_warnings(caplog)
    assert len(warnings) == 1
    assert "kp-3" in warnings[0]
    assert "another Home Assistant" in warnings[0]
    assert "extra login" in warnings[0]

    for _ in range(4):
        await _connack(hass, mock_paho_client)
        freezer.tick(3)
        await _drop(hass, client, mock_paho_client)
    assert len(_shared_login_warnings(caplog)) == 1
    await client.async_stop()


async def test_occasional_or_stable_drops_do_not_warn_about_a_shared_login(
    hass: HomeAssistant,
    mock_paho_client: MagicMock,
    freezer: FrozenDateTimeFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _make_client(hass, [])
    await client.async_start()
    await hass.async_block_till_done()

    # Drops of connections that had been up for a minute or more: an ordinary network.
    for _ in range(4):
        await _connack(hass, mock_paho_client)
        freezer.tick(61)
        async_fire_time_changed(hass)
        await hass.async_block_till_done()
        await _drop(hass, client, mock_paho_client)
    # Quick drops, but more than two minutes apart.
    for _ in range(4):
        await _connack(hass, mock_paho_client)
        freezer.tick(2)
        await _drop(hass, client, mock_paho_client)
        mock_paho_client.on_connect_fail(mock_paho_client, None)
        await hass.async_block_till_done()
        client._reconnect_handle.cancel()
        client._reconnect_handle = None
        freezer.tick(70)
        client._start_reconnect_task()
        await hass.async_block_till_done()

    assert _shared_login_warnings(caplog) == []
    await client.async_stop()


def _loop_calls(paho: MagicMock) -> list[str]:
    relevant = ("connect", "loop_start", "disconnect", "loop_stop")
    return [name for name, _, _ in paho.method_calls if name in relevant]


async def test_stop_during_an_in_flight_connect_leaves_no_running_connection(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """1.5.1: unload/reload while connect() is still blocking on the executor. async_stop disconnected
    a client that was not connected yet, then connect() returned and loop_start() brought the
    connection up anyway -- a zombie with the same client id that kicks the reloaded entry."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    connecting = threading.Event()
    release = threading.Event()

    def slow_connect(*args: object, **kwargs: object) -> int:
        connecting.set()
        release.wait(5)
        return mqtt.MQTT_ERR_SUCCESS

    mock_paho_client.connect.side_effect = slow_connect
    start = hass.async_create_task(client.async_start())
    assert await hass.async_add_executor_job(connecting.wait, 5)

    await client.async_stop()
    release.set()
    await start
    await hass.async_block_till_done()

    calls = _loop_calls(mock_paho_client)
    assert "loop_start" not in calls, calls
    assert calls[-1] in ("disconnect", "loop_stop"), calls
    assert states[-1] == ConnectionState.STOPPED


async def test_a_reconnect_that_runs_after_stop_never_connects(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """The executor job of a reconnect can start after async_stop has finished."""
    client = _make_client(hass, [])
    await client.async_start()
    await hass.async_block_till_done()
    await client.async_stop()
    mock_paho_client.reset_mock()

    await hass.async_add_executor_job(client._connect_once)
    await hass.async_block_till_done()

    mock_paho_client.connect.assert_not_called()
    mock_paho_client.loop_start.assert_not_called()


async def test_a_connack_or_failure_after_stop_changes_nothing(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()
    await client.async_stop()

    client._handle_connect(mock_paho_client, None, None, 0)
    client._handle_connect(mock_paho_client, None, None, 135)
    client._handle_connect_fail(mock_paho_client, None)
    await hass.async_block_till_done()

    mock_paho_client.subscribe.assert_not_called()
    assert states[-1] == ConnectionState.STOPPED
    assert client._reconnect_handle is None
    assert client._stable_handle is None


async def test_a_refusal_handled_on_the_loop_after_stop_starts_no_reauth(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    """A CONNACK refusal that paho receives just before unload is handed to the event loop, which
    runs it while async_stop waits for the disconnect: it must not report AUTH_FAILURE (the
    coordinator would start a reauth flow for an entry that is being unloaded)."""
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    await client.async_start()
    await hass.async_block_till_done()

    client._handle_connect(mock_paho_client, None, None, 135)
    await client.async_stop()
    await hass.async_block_till_done()

    assert ConnectionState.AUTH_FAILURE not in states
    assert states[-1] == ConnectionState.STOPPED
    assert client._reconnect_handle is None


async def test_a_connect_error_after_stop_reports_nothing(
    hass: HomeAssistant, mock_paho_client: MagicMock
) -> None:
    states: list[ConnectionState] = []
    client = _make_client(hass, states)
    connecting = threading.Event()
    release = threading.Event()

    def failing_connect(*args: object, **kwargs: object) -> int:
        connecting.set()
        release.wait(5)
        raise ssl.SSLError("handshake interrupted")

    mock_paho_client.connect.side_effect = failing_connect
    start = hass.async_create_task(client.async_start())
    assert await hass.async_add_executor_job(connecting.wait, 5)
    await client.async_stop()
    release.set()
    await start
    await hass.async_block_till_done()

    assert states[-1] == ConnectionState.STOPPED
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


class TestBlockingConnectionTestReasonCodes:
    """Regression tests for the config-flow connection probe
    (`_blocking_test_connection`, used by `async_test_connection`).

    This is the exact function and line (mqtt_client.py, `on_connect`)
    where the real WSS smoke test hit `TypeError` from `int(reason_code)`
    in production -- and it had NO direct test coverage at all before
    this (only ever exercised indirectly, with `async_test_connection`
    itself mocked away in tests/test_config_flow.py). All fixtures here
    drive it with real paho.mqtt.reasoncodes.ReasonCode objects, the
    actual runtime type, not integers.
    """

    @staticmethod
    def _connect_with(mock_paho_client: MagicMock, reason_code: object) -> None:
        mock_paho_client.connect.side_effect = lambda *a, **k: mock_paho_client.on_connect(
            mock_paho_client, None, MagicMock(), reason_code, None
        )

    @staticmethod
    def _subscribe_with(mock_paho_client: MagicMock, *suback_reason_codes: object) -> None:
        mock_paho_client.subscribe.side_effect = lambda *a, **k: mock_paho_client.on_subscribe(
            mock_paho_client, None, 1, list(suback_reason_codes), None
        )

    def test_successful_connack_reasoncode_does_not_raise_and_issues_subscribe(
        self, mock_paho_client: MagicMock
    ) -> None:
        self._connect_with(mock_paho_client, mqtt.convert_connack_rc_to_reason_code(0))
        self._subscribe_with(mock_paho_client, ReasonCode(PacketTypes.SUBACK, identifier=1))

        _blocking_test_connection(**PROBE, password="secret")

        mock_paho_client.subscribe.assert_called_once()
        mock_paho_client.disconnect.assert_called_once()

    def test_probe_needs_only_the_detection_subscription(self, mock_paho_client: MagicMock) -> None:
        """1.5.0: setup must keep working against a broker that does not (yet) let the
        login read the roster / latest photos -- the probe asks only for detections."""
        self._connect_with(mock_paho_client, mqtt.convert_connack_rc_to_reason_code(0))
        self._subscribe_with(mock_paho_client, ReasonCode(PacketTypes.SUBACK, identifier=1))

        _blocking_test_connection(**PROBE, password="secret")

        mock_paho_client.subscribe.assert_called_once_with(f"customers/{CUSTOMER_ID}/detections/+", qos=1)

    @pytest.mark.parametrize("v3_rc", [4, 5])
    def test_auth_failure_reasoncode_maps_to_invalid_auth(
        self, mock_paho_client: MagicMock, v3_rc: int
    ) -> None:
        self._connect_with(mock_paho_client, mqtt.convert_connack_rc_to_reason_code(v3_rc))

        with pytest.raises(InvalidAuth):
            _blocking_test_connection(**PROBE, password="wrong")

    def test_non_auth_connect_failure_reasoncode_maps_to_cannot_connect(
        self, mock_paho_client: MagicMock
    ) -> None:
        # v3 CONNACK code 3 -> "Server unavailable", ReasonCode value 136.
        self._connect_with(mock_paho_client, mqtt.convert_connack_rc_to_reason_code(3))

        with pytest.raises(CannotConnect):
            _blocking_test_connection(**PROBE, password="secret")

    def test_suback_success_reasoncode_does_not_raise(self, mock_paho_client: MagicMock) -> None:
        self._connect_with(mock_paho_client, mqtt.convert_connack_rc_to_reason_code(0))
        self._subscribe_with(mock_paho_client, ReasonCode(PacketTypes.SUBACK, identifier=0))

        _blocking_test_connection(**PROBE, password="secret")

    def test_suback_failure_reasoncode_maps_to_cannot_connect(self, mock_paho_client: MagicMock) -> None:
        self._connect_with(mock_paho_client, mqtt.convert_connack_rc_to_reason_code(0))
        # 128 = "Unspecified error" for SUBACK -- ReasonCode.is_failure is
        # True for any value >= 0x80.
        self._subscribe_with(mock_paho_client, ReasonCode(PacketTypes.SUBACK, identifier=128))

        with pytest.raises(CannotConnect):
            _blocking_test_connection(**PROBE, password="secret")

    def test_an_unexpected_callback_exception_is_reraised_as_itself_and_logged(
        self, mock_paho_client: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Item 6: a programming/callback bug must surface as itself (and
        be logged), never be silently reported as CannotConnect."""

        class _ExplodingReasonCode:
            @property
            def value(self) -> int:
                raise ZeroDivisionError("boom")

        self._connect_with(mock_paho_client, _ExplodingReasonCode())

        with caplog.at_level(logging.ERROR, logger="custom_components.kameraposti.mqtt_client"):
            with pytest.raises(ZeroDivisionError):
                _blocking_test_connection(**PROBE, password="secret")

        assert "Unexpected error handling a Kameraposti MQTT callback" in caplog.text

    def test_callback_thread_never_raises_typeerror_from_reasoncode_conversion(
        self, mock_paho_client: MagicMock
    ) -> None:
        """Broad regression: feed every real ReasonCode this module deals
        with (success/auth-failure/other-failure CONNACK, success/failure
        SUBACK) through the probe and confirm none of them ever raise
        TypeError -- the exact class of bug int(reason_code) caused."""
        for connack_value in (0, 128, 132, 133, 134, 135, 136):
            for suback_value in (0, 1, 2, 128):
                mock_paho_client.reset_mock(side_effect=True)
                self._connect_with(
                    mock_paho_client, ReasonCode(PacketTypes.CONNACK, identifier=connack_value)
                )
                self._subscribe_with(
                    mock_paho_client, ReasonCode(PacketTypes.SUBACK, identifier=suback_value)
                )
                try:
                    _blocking_test_connection(**PROBE, password="secret")
                except (CannotConnect, InvalidAuth):
                    pass
