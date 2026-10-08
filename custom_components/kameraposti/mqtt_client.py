"""Own outbound MQTT (WSS) client for the Kameraposti backend.

Deliberately NOT homeassistant.components.mqtt -- this integration must
never register a second Home Assistant MQTT broker, and must work
identically whether or not the user has HA's own built-in MQTT
integration configured against their own broker/Zigbee2MQTT (contract
section 1). This module owns a completely separate connection to
wss://tailscale2.steels.me/mqtt.

Thread/event-loop boundary (contract sections 9/10): paho-mqtt's network
loop (loop_start()) runs in its OWN background thread and invokes
on_connect/on_message/on_disconnect/on_connect_fail FROM THAT THREAD.
None of those callbacks may touch Home Assistant state, entities, or the
event bus directly. Every callback in this module does the absolute
minimum on the MQTT thread (read arguments, maybe do lightweight parsing)
and then crosses back into the HA event loop via
``hass.loop.call_soon_threadsafe`` before calling any consumer-supplied
callback. Blocking paho-mqtt calls (connect/reconnect/disconnect/
loop_start/loop_stop) are run through ``hass.async_add_executor_job`` so
they never block the event loop either.

Reconnection is fully owned by this module (paho-mqtt's own
``reconnect_on_failure`` is disabled) so the exponential backoff +
jitter behaviour in the contract is exact and observable, not an
implementation detail of the underlying library.
"""

from __future__ import annotations

import asyncio
import logging
import random
import ssl
import threading
from collections import deque
from collections.abc import Callable
from enum import StrEnum
from typing import Any

import paho.mqtt.client as mqtt
from homeassistant.core import HomeAssistant

from .const import (
    CONNECTION_TEST_TIMEOUT_SECONDS,
    LATEST_PHOTO_SUBSCRIBE_TEMPLATE,
    MQTT_KEEPALIVE_SECONDS,
    MQTT_PORT,
    MQTT_TRANSPORT,
    MQTT_WS_PATH,
    RECONNECT_JITTER_SECONDS,
    RECONNECT_MAX_DELAY_SECONDS,
    RECONNECT_MIN_DELAY_SECONDS,
    ROSTER_TOPIC_TEMPLATE,
    SECURITY_RESULT_TOPIC_TEMPLATE,
    SECURITY_STATE_TOPIC_TEMPLATE,
    SHORT_CONNECTION_DROPS_WARN_COUNT,
    SHORT_CONNECTION_DROPS_WINDOW_SECONDS,
    STABLE_CONNECTION_SECONDS,
    TOPIC_SUBSCRIBE_TEMPLATE,
)

_LOGGER = logging.getLogger(__name__)

# MQTT CONNACK / v5 reason-code values that mean "the network round trip
# happened but the broker rejected these credentials" -- as opposed to a
# network/TLS failure, which never reaches on_connect at all (it surfaces
# via on_connect_fail instead). paho-mqtt's v2 callback API always hands
# on_connect a paho.mqtt.reasoncodes.ReasonCode, even for a plain
# MQTTv3.1.1 broker -- convert_connack_rc_to_reason_code() remaps the old
# CONNACK codes 4/5 ("bad username or password" / "not authorised") to
# the MQTTv5 numeric space (134/135), so 4/5 never actually appear here in
# production; they're kept only so a caller/test that already has a v3
# numeric code still matches.
_AUTH_FAILURE_REASON_CODES = frozenset({4, 5, 134, 135})


def _reason_code_value(reason_code: Any) -> int:
    """Numeric value of a CONNACK/SUBACK reason code.

    paho-mqtt 2.x's ReasonCode does NOT support int() -- it has no
    __int__, only a plain `.value` attribute and __eq__/__lt__ against a
    bare int. Calling int() on one raises TypeError (the exact bug this
    fixes). ReasonCode is also unhashable (it defines __eq__ without
    __hash__), so it can never be used directly as a `in <frozenset>`
    member either -- always extract the plain int first. A test double
    may still hand us a plain int/bool directly, which has no `.value`,
    so that case falls through unchanged.
    """
    return int(getattr(reason_code, "value", reason_code))


def _reason_code_is_failure(reason_code: Any) -> bool:
    """Whether a reason code represents failure, per ReasonCode.is_failure.

    Prefers the real ReasonCode.is_failure property when available;
    falls back to the same >= 0x80 threshold it uses internally for
    plain-int test doubles that have no such property.
    """
    is_failure = getattr(reason_code, "is_failure", None)
    if is_failure is not None:
        return bool(is_failure)
    return _reason_code_value(reason_code) >= 0x80


class ConnectionState(StrEnum):
    """Coarse connection lifecycle used for diagnostics/entity availability."""

    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    AUTH_FAILURE = "auth_failure"
    TLS_FAILURE = "tls_failure"
    STOPPED = "stopped"


class _Backoff:
    """Exponential backoff with jitter (contract section 9).

    1s -> 2s -> 4s -> 8s -> 16s -> 30s max, each with up to
    RECONNECT_JITTER_SECONDS of extra random delay so many clients
    reconnecting at once don't all retry in lockstep. A stable connection
    (up for STABLE_CONNECTION_SECONDS, not merely a CONNACK) resets it back
    to the minimum.
    """

    def __init__(
        self,
        minimum: float = RECONNECT_MIN_DELAY_SECONDS,
        maximum: float = RECONNECT_MAX_DELAY_SECONDS,
        jitter: float = RECONNECT_JITTER_SECONDS,
    ) -> None:
        self._minimum = minimum
        self._maximum = maximum
        self._jitter = jitter
        self._current = minimum

    def next_delay(self) -> float:
        delay = self._current + random.uniform(0, self._jitter)
        self._current = min(self._current * 2, self._maximum)
        return delay

    def reset(self) -> None:
        self._current = self._minimum


class KameraportiMqttClient:
    """Owns one persistent WSS connection + subscription to the Kameraposti broker."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        host: str,
        customer_id: int,
        username: str,
        password: str,
        on_message: Callable[[str, bytes], None],
        on_state_change: Callable[[ConnectionState], None],
    ) -> None:
        self._hass = hass
        self._host = host
        self._customer_id = customer_id
        self._username = username
        self._password = password
        self._on_message = on_message
        self._on_state_change = on_state_change

        self._client: mqtt.Client | None = None
        self._backoff = _Backoff()
        # SUBACK bookkeeping for one connection (paho's network thread only):
        # message id -> topic still waiting for its SUBACK, and refused topics.
        self._suback_pending: dict[int, str] = {}
        self._suback_refused: list[str] = []
        self._closing = False
        self._reconnect_handle: Any | None = None
        # 1.5.1: pending while the current connection is not yet stable (event loop only).
        self._stable_handle: asyncio.TimerHandle | None = None
        # loop.time() of recent drops of not-yet-stable connections, and whether the
        # shared-login warning has been logged since the last stable connection.
        self._short_drops: deque[float] = deque()
        self._shared_login_warned = False

    @property
    def topic(self) -> str:
        return TOPIC_SUBSCRIBE_TEMPLATE.format(customer_id=self._customer_id)

    @property
    def topics(self) -> list[str]:
        """Detections + the security system state and command results (2026-10-05)
        + the camera roster and each camera's latest photo (1.5.0).

        The roster is subscribed before the latest photos so that, on a fresh
        connection, the broker hands over the retained camera list before the
        retained photos. A broker whose ACL does not (yet) allow the 1.5.0
        topics just refuses those subscriptions in its SUBACK; detections and
        the security system keep working (the config-flow probe asks only for
        detections).
        """
        return [
            self.topic,
            SECURITY_STATE_TOPIC_TEMPLATE.format(customer_id=self._customer_id),
            SECURITY_RESULT_TOPIC_TEMPLATE.format(customer_id=self._customer_id),
            ROSTER_TOPIC_TEMPLATE.format(customer_id=self._customer_id),
            LATEST_PHOTO_SUBSCRIBE_TEMPLATE.format(customer_id=self._customer_id),
        ]

    async def async_start(self) -> None:
        """Start the client and attempt the first connection."""
        self._closing = False
        self._report_state(ConnectionState.CONNECTING)
        await self._hass.async_add_executor_job(self._connect_once)

    async def async_stop(self) -> None:
        """Stop reconnecting and cleanly disconnect. Idempotent."""
        self._closing = True

        if self._reconnect_handle is not None:
            self._reconnect_handle.cancel()
            self._reconnect_handle = None
        self._cancel_stable_timer()

        client = self._client
        self._client = None
        if client is not None:
            await self._hass.async_add_executor_job(self._disconnect_client, client)

        self._report_state(ConnectionState.STOPPED)

    def publish(self, topic: str, payload: str, retain: bool = False) -> bool:
        """Queue a QoS 1 publish (sensor export). False while not connected.

        paho-mqtt's publish() only queues the packet for its network thread,
        so this is safe to call from the event loop. Nothing is buffered
        while disconnected -- the exporter re-sends descriptions and current
        states on the next connect instead.
        """
        client = self._client
        if client is None or not client.is_connected():
            return False
        info = client.publish(topic, payload, qos=1, retain=retain)
        return info.rc == mqtt.MQTT_ERR_SUCCESS

    # -- setup helpers (always run on the executor thread) --------------

    def _build_client(self) -> mqtt.Client:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            transport=MQTT_TRANSPORT,
            # The broker pins the login to client id == username.
            client_id=self._username,
            # We own reconnection entirely (backoff + jitter below) --
            # paho-mqtt's own retry-on-failure would otherwise race with
            # ours and make the backoff/jitter contract unobservable.
            reconnect_on_failure=False,
        )
        client.username_pw_set(self._username, self._password)
        # TLS certificate verification is mandatory (contract section
        # 23) -- tls_set() with no arguments uses the system CA bundle
        # and verifies both the chain and the hostname by default.
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.tls_insecure_set(False)
        client.ws_set_options(path=MQTT_WS_PATH)
        client.on_connect = self._handle_connect
        client.on_disconnect = self._handle_disconnect
        client.on_connect_fail = self._handle_connect_fail
        client.on_message = self._handle_message
        client.on_subscribe = self._handle_subscribe
        return client

    def _connect_once(self) -> None:
        """Blocking connect attempt. Must run on the executor thread."""
        client = self._build_client()
        self._client = client
        try:
            client.connect(self._host, MQTT_PORT, keepalive=MQTT_KEEPALIVE_SECONDS)
        except (OSError, ssl.SSLError) as err:
            _LOGGER.debug("Kameraposti MQTT connect() raised %s: %s", type(err).__name__, err)
            self._client = None
            self._report_state(
                ConnectionState.TLS_FAILURE if isinstance(err, ssl.SSLError) else ConnectionState.RECONNECTING
            )
            self._schedule_reconnect()
            return
        client.loop_start()

    def _disconnect_client(self, client: mqtt.Client) -> None:
        try:
            client.disconnect()
        finally:
            client.loop_stop()

    # -- paho-mqtt callbacks (run on paho's network thread) --------------

    def _handle_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        connect_flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        rc = _reason_code_value(reason_code)
        if rc == 0:
            self._suback_pending = {}
            self._suback_refused = []
            for topic in self.topics:
                result = client.subscribe(topic, qos=1)
                mid = result[1] if isinstance(result, tuple) else None
                if mid is not None:
                    self._suback_pending[mid] = topic
            self._hass.loop.call_soon_threadsafe(self._on_connected)
            return

        if rc in _AUTH_FAILURE_REASON_CODES:
            self._hass.loop.call_soon_threadsafe(self._report_state, ConnectionState.AUTH_FAILURE)
        else:
            self._hass.loop.call_soon_threadsafe(self._report_state, ConnectionState.RECONNECTING)
        self._hass.loop.call_soon_threadsafe(self._schedule_reconnect)

    def _handle_connect_fail(self, client: mqtt.Client, userdata: Any) -> None:
        # TCP/TLS handshake itself never completed -- never an auth
        # failure (no CONNACK was ever received to carry that verdict).
        self._hass.loop.call_soon_threadsafe(self._report_state, ConnectionState.RECONNECTING)
        self._hass.loop.call_soon_threadsafe(self._schedule_reconnect)

    def _handle_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any = None,
        properties: Any = None,
    ) -> None:
        if self._closing:
            return
        self._hass.loop.call_soon_threadsafe(self._on_disconnected)

    def _handle_subscribe(
        self, client: mqtt.Client, userdata: Any, mid: int, reason_code_list: Any, properties: Any = None
    ) -> None:
        """Log, once per connection, the topics the broker refused (its ACL).

        Diagnostics only: a refused 1.5.0 topic (roster / latest photos) on a
        broker that does not allow it yet changes nothing else.
        """
        try:
            topic = self._suback_pending.pop(mid, None)
            if topic is None:
                return
            if any(_reason_code_is_failure(rc) for rc in reason_code_list):
                self._suback_refused.append(topic)
            if not self._suback_pending and self._suback_refused:
                _LOGGER.debug(
                    "Kameraposti broker refused the subscriptions %s (not allowed for this login)",
                    ", ".join(self._suback_refused),
                )
        except Exception:  # noqa: BLE001 - must never break paho's network thread
            _LOGGER.debug("Could not check a Kameraposti SUBACK", exc_info=True)

    def _handle_message(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
        topic = message.topic
        payload = message.payload
        self._hass.loop.call_soon_threadsafe(self._on_message, topic, payload)

    # -- state/reconnect bookkeeping (always run on the HA event loop) --

    def _on_connected(self) -> None:
        # Not self._backoff.reset(): a CONNACK alone does not make the connection stable
        # (see STABLE_CONNECTION_SECONDS). _on_stable resets it once the connection stays up.
        self._cancel_stable_timer()
        self._stable_handle = self._hass.loop.call_later(STABLE_CONNECTION_SECONDS, self._on_stable)
        self._report_state(ConnectionState.CONNECTED)

    def _on_stable(self) -> None:
        self._stable_handle = None
        self._backoff.reset()
        self._short_drops.clear()
        self._shared_login_warned = False

    def _cancel_stable_timer(self) -> None:
        if self._stable_handle is not None:
            self._stable_handle.cancel()
            self._stable_handle = None

    def _on_disconnected(self) -> None:
        if self._closing:
            return
        if self._stable_handle is not None:
            # Dropped before it became stable.
            self._cancel_stable_timer()
            self._note_short_connection()
        self._report_state(ConnectionState.RECONNECTING)
        self._schedule_reconnect()

    def _note_short_connection(self) -> None:
        """Warn once when connections keep being dropped right after connecting.

        The broker accepts one connection per login (client id == login): a second Home
        Assistant or a bridge with the same login kicks this one out, this one kicks it back.
        """
        now = self._hass.loop.time()
        self._short_drops.append(now)
        while self._short_drops and now - self._short_drops[0] > SHORT_CONNECTION_DROPS_WINDOW_SECONDS:
            self._short_drops.popleft()
        if len(self._short_drops) < SHORT_CONNECTION_DROPS_WARN_COUNT or self._shared_login_warned:
            return
        self._shared_login_warned = True
        _LOGGER.warning(
            "Kameraposti dropped the MQTT connection of login %s %d times within %d s, each time "
            "soon after connecting. The login is probably also used by another Home Assistant or a "
            "bridge: Kameraposti allows one connection per login, so the two keep disconnecting each "
            "other (reconnects now back off up to %d s). Give the other one its own extra login "
            "(%s-<n>) on Kameraposti's Sensors page.",
            self._username,
            len(self._short_drops),
            SHORT_CONNECTION_DROPS_WINDOW_SECONDS,
            RECONNECT_MAX_DELAY_SECONDS,
            f"kp-{self._customer_id}",
        )

    def _report_state(self, state: ConnectionState) -> None:
        self._on_state_change(state)

    def _schedule_reconnect(self) -> None:
        if self._closing:
            return
        if self._reconnect_handle is not None:
            # Already have one in flight -- paho can call both
            # on_disconnect and on_connect_fail in edge cases; never
            # stack duplicate reconnect timers for the same drop.
            return

        delay = self._backoff.next_delay()
        _LOGGER.debug("Kameraposti MQTT reconnecting in %.1fs", delay)
        self._reconnect_handle = self._hass.loop.call_later(delay, self._start_reconnect_task)

    def _start_reconnect_task(self) -> None:
        self._reconnect_handle = None
        if self._closing:
            return
        self._hass.async_create_task(self._async_reconnect())

    async def _async_reconnect(self) -> None:
        if self._closing:
            return
        await self._hass.async_add_executor_job(self._connect_once)


class CannotConnect(Exception):
    """The broker could not be reached (DNS/TCP/TLS/timeout)."""


class InvalidAuth(Exception):
    """The broker reached out but rejected the given credentials."""


def _blocking_test_connection(host: str, customer_id: int, username: str, password: str) -> None:
    """Fully synchronous connect + subscribe + disconnect probe.

    MUST run on an executor thread, never on the event loop. Deliberately
    does not require an actual detection event to arrive (contract
    section 18) -- a successful CONNACK plus a successful SUBACK to our
    own customer namespace already proves DNS, TLS, auth and ACL all
    work.
    """
    connected = threading.Event()
    subscribed = threading.Event()
    outcome: dict[str, Any] = {}

    def on_connect(
        client: mqtt.Client, userdata: Any, connect_flags: Any, reason_code: Any, properties: Any = None
    ) -> None:
        try:
            rc = _reason_code_value(reason_code)
            if rc == 0:
                client.subscribe(TOPIC_SUBSCRIBE_TEMPLATE.format(customer_id=customer_id), qos=1)
            elif rc in _AUTH_FAILURE_REASON_CODES:
                outcome["auth_failed"] = True
            else:
                outcome["connect_failed"] = True
        except Exception as exc:  # noqa: BLE001 - must not crash paho's thread; re-raised below
            outcome["callback_exception"] = exc
        finally:
            connected.set()

    def on_connect_fail(client: mqtt.Client, userdata: Any) -> None:
        outcome["connect_failed"] = True
        connected.set()

    def on_subscribe(
        client: mqtt.Client, userdata: Any, mid: int, reason_code_list: Any, properties: Any = None
    ) -> None:
        try:
            if any(_reason_code_is_failure(rc) for rc in reason_code_list):
                outcome["subscribe_failed"] = True
        except Exception as exc:  # noqa: BLE001
            outcome["callback_exception"] = exc
        finally:
            subscribed.set()

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        transport=MQTT_TRANSPORT,
        client_id=username,
        reconnect_on_failure=False,
    )
    client.username_pw_set(username, password)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.tls_insecure_set(False)
    client.ws_set_options(path=MQTT_WS_PATH)
    client.on_connect = on_connect
    client.on_connect_fail = on_connect_fail
    client.on_subscribe = on_subscribe

    try:
        client.connect(host, MQTT_PORT, keepalive=MQTT_KEEPALIVE_SECONDS)
    except (OSError, ssl.SSLError) as err:
        raise CannotConnect(str(err)) from err

    client.loop_start()
    try:
        if not connected.wait(CONNECTION_TEST_TIMEOUT_SECONDS):
            raise CannotConnect("timed out waiting for the broker to respond")
        callback_exception = outcome.get("callback_exception")
        if callback_exception is not None:
            # A programming/callback error is not "the broker is
            # unreachable" -- surface and log it as itself so it shows up
            # as "unknown" (with a full traceback) rather than being
            # silently misreported as cannot_connect/invalid_auth.
            _LOGGER.exception(
                "Unexpected error handling a Kameraposti MQTT callback during connection test",
                exc_info=callback_exception,
            )
            raise callback_exception
        if outcome.get("auth_failed"):
            raise InvalidAuth("broker rejected the given credentials")
        if outcome.get("connect_failed"):
            raise CannotConnect("broker refused the connection")
        if not subscribed.wait(CONNECTION_TEST_TIMEOUT_SECONDS):
            raise CannotConnect("timed out waiting for subscription confirmation")
        callback_exception = outcome.get("callback_exception")
        if callback_exception is not None:
            _LOGGER.exception(
                "Unexpected error handling a Kameraposti MQTT callback during connection test",
                exc_info=callback_exception,
            )
            raise callback_exception
        if outcome.get("subscribe_failed"):
            raise CannotConnect("broker rejected the subscription to the customer topic")
    finally:
        client.disconnect()
        client.loop_stop()


async def async_test_connection(
    hass: HomeAssistant, *, host: str, customer_id: int, username: str, password: str
) -> None:
    """Test connectivity, auth and subscription against the Kameraposti broker.

    Raises CannotConnect or InvalidAuth on failure; returns normally on
    success. Used by the config flow (contract section 18) -- an entry is
    never saved unless this passes.
    """
    await hass.async_add_executor_job(_blocking_test_connection, host, customer_id, username, password)
