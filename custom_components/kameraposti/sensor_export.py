"""Export chosen Home Assistant entities to Kameraposti as sensors.

The user picks entities in the integration's options (leak, smoke, gas,
door, window, motion binary sensors; temperature and CO2 sensors). For each
one this module publishes, over the integration's own connection:

* ``kameraposti/<id>/anturit/<name>/config`` -- {"name", "kind", "format"};
  Kameraposti creates the sensor (or updates its name/kind) automatically.
* ``kameraposti/<id>/anturit/<name>`` -- the state in Kameraposti's simple
  format: leak/dry, smoke/clear, gas/clear, open/closed, motion, the
  temperature in degrees Celsius, or {"e":"co2","v":<ppm>}.

``<name>`` is the entity_id, so it stays stable across friendly-name changes.
On every (re)connect the sensors are described again and their current state
is sent (not motion: a motion sensor that happens to be "on" at reconnect is
not a new movement), so an alarm that started during a break is not lost.
Kameraposti drops a repeated leak/dry/open/closed state on its own.
The same snapshot is repeated every 15 minutes: Kameraposti's listener
reconnects hourly (and after a crash), and a message published in that gap
would otherwise be lost until the next state change.
A reading (temperature, CO2) is sent at most once a minute per entity (the
latest value follows when the minute is up), so a chatty thermometer or CO2
meter cannot use up the account's shared message budget and crowd out a leak
alarm.

Everything here runs on the Home Assistant event loop; ``publish`` is the
MQTT client's non-blocking publish and returns False while disconnected.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
from collections.abc import Callable
from datetime import datetime, timedelta
from functools import partial

from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import (
    CALLBACK_TYPE,
    Event,
    EventStateChangedData,
    HassJob,
    HomeAssistant,
    State,
    callback,
)
from homeassistant.helpers.event import (
    async_call_later,
    async_track_state_change_event,
    async_track_time_interval,
)
from homeassistant.util import dt as dt_util

from .const import SENSOR_TOPIC_TEMPLATE

_LOGGER = logging.getLogger(__name__)

# binary_sensor device_class -> Kameraposti sensor kind.
KIND_BY_BINARY_DEVICE_CLASS: dict[str, str] = {
    "moisture": "leak",
    "smoke": "smoke",
    # 1.4.0 (steelo 2026-10-06): a gas detector is its own kind, alarming like smoke;
    # a carbon monoxide detector is a gas sensor too.
    "gas": "gas",
    "carbon_monoxide": "gas",
    "door": "door",
    "garage_door": "door",
    "opening": "door",
    "window": "window",
    "motion": "motion",
    "occupancy": "motion",
    "presence": "motion",
}

# 1.3.2 (2026-10-06): Z-Wave JS UI publishes notification sensors as device_class "problem"
# ("OK" / "Problem"), e.g. the Water Alarm as "<device>_event_water_leak". Their kind comes from
# the value part of the name; others of that class (general purpose, alarm status) stay unexported.
# The value part follows the device name, so when the name has several kind words (a combined
# "Smoke-Gas" detector) the last one decides. Only English value words: a Finnish device name
# ("Keitttio-Vuoto", "Keittio-Kaasu") never makes its general purpose sensor a leak or gas sensor.
INFERRED_DEVICE_CLASSES: tuple[str, ...] = ("problem",)
_KIND_BY_NAME: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"water_leak|leak|flood"), "leak"),
    (re.compile(r"smoke"), "smoke"),
    # 1.4.0: gas alarms (combustible / toxic gas) and carbon monoxide; not carbon_dioxide,
    # whose kind (co2) is a ppm reading, not an alarm state. "gas" only as a word of its own
    # (names use "_" between words), not inside "vegas", "gasket" or "degassing".
    (re.compile(r"combustible|(?<![a-z])gas(?![a-z])|carbon_monoxide"), "gas"),
)

# Kameraposti simple-format event for a binary sensor that is on / off.
_EVENTS_BY_KIND: dict[str, tuple[str, str | None]] = {
    "leak": ("leak", "dry"),
    "smoke": ("smoke", "clear"),
    "gas": ("gas", "clear"),
    "door": ("open", "closed"),
    "window": ("open", "closed"),
    "motion": ("motion", None),
}

# Kameraposti accepts topic names [A-Za-z0-9_.-]{1,64} and names of 1-60
# characters without control characters.
_MAX_NAME_LENGTH = 64
_MAX_DISPLAY_NAME_LENGTH = 60
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")

# Kinds that send a reading (a number), not an alarm state: sent at most once
# per READING_INTERVAL_SECONDS per entity, the latest value following.
READING_KINDS: frozenset[str] = frozenset({"temperature", "co2"})
# 1.4.1 (steelo 2026-10-06, Z-Wave "Carbon monoxide (CO) level"): a CO meter (sensor, ppm) is sent
# as a gas sensor that alarms at CO_ALARM_PPM and clears below CO_CLEAR_PPM (no message in between,
# so a reading hovering around the limit does not flap). EN 50291 alarms at 50 ppm after 60-90 min;
# alarming at once is the safe side.
CO_ALARM_PPM = 50
CO_CLEAR_PPM = 35
# Minimum interval between two readings from one entity.
READING_INTERVAL_SECONDS = 60
# Periodic resend of descriptions and current states.
RESYNC_INTERVAL = timedelta(minutes=15)

PublishFn = Callable[[str, str, bool], bool]


def kind_for(state: State | None) -> str | None:
    """Kameraposti kind for an entity, or None when it cannot be exported."""
    if state is None:
        return None
    device_class = state.attributes.get("device_class")
    if state.domain == "binary_sensor":
        kind = KIND_BY_BINARY_DEVICE_CLASS.get(device_class)
        if kind is None and device_class in INFERRED_DEVICE_CLASSES:
            kind = _kind_from_name(state)
        return kind
    if state.domain == "sensor":
        if device_class == "temperature":
            return "temperature"
        # 1.4.0: a CO2 meter (ppm) is sent as a reading, like a thermometer.
        if device_class == "carbon_dioxide":
            return "co2"
        # 1.4.1: a CO meter (ppm) is a gas sensor: alarm / clear by CO_ALARM_PPM / CO_CLEAR_PPM.
        if device_class == "carbon_monoxide":
            return "gas"
        # 1.4.1: Z-Wave JS UI publishes Multilevel Sensor gas levels without a device class (only an
        # icon), e.g. "<node>_gas_carbon_monoxide" / "<node>_gas_carbon_dioxide" in ppm.
        if device_class is None and state.attributes.get("unit_of_measurement") in (None, "ppm"):
            names = " ".join(
                re.sub(r"[\s-]+", "_", name.lower())
                for name in (state.entity_id, str(state.attributes.get("friendly_name") or ""))
            )
            if "carbon_monoxide" in names:
                return "gas"
            if "carbon_dioxide" in names:
                return "co2"
    return None


def _kind_from_name(state: State) -> str | None:
    """Kind from the entity id, else the friendly name (spaces and dashes read as underscores).

    Within a name the last kind word wins: it belongs to the value part, which follows the device
    name. The entity id decides first (it comes from the integration's own name and stays put when
    the user renames the entity), so a renamed friendly name cannot turn a leak sensor into a gas one.
    """
    for name in (state.entity_id, str(state.attributes.get("friendly_name") or "")):
        text = re.sub(r"[\s-]+", "_", name.lower())
        last: tuple[int, str] | None = None
        for pattern, kind in _KIND_BY_NAME:
            for match in pattern.finditer(text):
                if last is None or match.start() > last[0]:
                    last = (match.start(), kind)
        if last is not None:
            return last[1]
    return None


def event_for(kind: str, state: str, unit: str | None = None) -> str | None:
    """Kameraposti simple-format payload for a state, or None to send nothing."""
    if state in (STATE_UNAVAILABLE, STATE_UNKNOWN):
        return None
    if kind == "temperature":
        value = _finite_number(state)
        if value is None:
            return None
        if unit == "°F":
            return str(round((value - 32) * 5 / 9, 2))
        if unit not in (None, "°C"):
            return None
        return state
    if kind == "co2":
        # The contract's JSON form names the event; the value in whole ppm.
        value = _finite_number(state)
        if value is None or unit not in (None, "ppm"):
            return None
        return json.dumps({"e": "co2", "v": round(value)}, separators=(",", ":"))
    events = _EVENTS_BY_KIND.get(kind)
    if events is None:
        return None
    if state == "on":
        return events[0]
    if state == "off":
        return events[1]
    if kind == "gas":
        # 1.4.1: a CO meter's ppm reading; between the limits nothing is sent (Kameraposti keeps the state).
        value = _finite_number(state)
        if value is None or unit not in (None, "ppm"):
            return None
        if value >= CO_ALARM_PPM:
            return events[0]
        if value < CO_CLEAR_PPM:
            return events[1]
    return None


def _finite_number(state: str) -> float | None:
    try:
        value = float(state)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def mqtt_name_for(entity_id: str) -> str:
    """Topic name for an entity: the entity_id, shortened with a hash if needed."""
    if len(entity_id) <= _MAX_NAME_LENGTH:
        return entity_id
    digest = hashlib.sha1(entity_id.encode()).hexdigest()[:8]
    return f"{entity_id[: _MAX_NAME_LENGTH - 9]}-{digest}"


def _display_name(state: State) -> str:
    name = _CONTROL_CHARS.sub("", str(state.attributes.get("friendly_name") or "")).strip()
    return (name or state.entity_id)[:_MAX_DISPLAY_NAME_LENGTH].strip()


class KameraportiSensorExporter:
    """Publishes the chosen entities' descriptions and state changes."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        customer_id: int,
        entity_ids: list[str],
        publish: PublishFn,
    ) -> None:
        self._hass = hass
        self._customer_id = customer_id
        self._entity_ids = list(dict.fromkeys(entity_ids))
        self._publish = publish
        self._unsub: Callable[[], None] | None = None
        self._unsub_resync: Callable[[], None] | None = None
        # entity_id -> (name, kind) last described on the current connection.
        self._described: dict[str, tuple[str, str]] = {}
        # 1.4.1: last alarm state sent for a CO meter, so each ppm update does not resend "clear".
        self._co_sent: dict[str, str] = {}
        # Reading throttle: last sent time and the pending trailing send.
        self._reading_sent_at: dict[str, datetime] = {}
        self._reading_pending: dict[str, CALLBACK_TYPE] = {}

    @callback
    def async_start(self) -> None:
        if self._unsub is None and self._entity_ids:
            self._unsub = async_track_state_change_event(
                self._hass, self._entity_ids, self._handle_state_event
            )
            self._unsub_resync = async_track_time_interval(
                self._hass, self._handle_resync, RESYNC_INTERVAL, cancel_on_shutdown=True
            )

    @callback
    def async_stop(self) -> None:
        if self._unsub is not None:
            self._unsub()
            self._unsub = None
        if self._unsub_resync is not None:
            self._unsub_resync()
            self._unsub_resync = None
        for cancel in self._reading_pending.values():
            cancel()
        self._reading_pending.clear()
        self._described.clear()
        self._co_sent.clear()

    @callback
    def publish_snapshot(self) -> None:
        """On (re)connect: describe every exportable entity and send its current state."""
        self._described.clear()
        for entity_id in self._entity_ids:
            state = self._hass.states.get(entity_id)
            kind = kind_for(state)
            if state is None or kind is None:
                continue
            if not self._describe(state, kind):
                return
            if kind != "motion":
                self._publish_state(state, kind, throttle=False)

    @callback
    def _handle_resync(self, _now: datetime) -> None:
        """Periodic resend; publishes nothing while disconnected."""
        self.publish_snapshot()

    @callback
    def _handle_state_event(self, event: Event[EventStateChangedData]) -> None:
        new_state = event.data["new_state"]
        old_state = event.data["old_state"]
        kind = kind_for(new_state)
        if new_state is None or kind is None:
            return
        if self._described.get(new_state.entity_id) != (_display_name(new_state), kind):
            if not self._describe(new_state, kind):
                return
        if old_state is not None and old_state.state == new_state.state:
            return
        self._publish_state(new_state, kind)

    def _topic(self, entity_id: str) -> str:
        return SENSOR_TOPIC_TEMPLATE.format(customer_id=self._customer_id, name=mqtt_name_for(entity_id))

    def _describe(self, state: State, kind: str) -> bool:
        name = _display_name(state)
        payload = json.dumps({"name": name, "kind": kind, "format": "simple"}, ensure_ascii=False)
        if not self._publish(f"{self._topic(state.entity_id)}/config", payload, False):
            return False
        self._described[state.entity_id] = (name, kind)
        return True

    def _publish_state(self, state: State, kind: str, *, throttle: bool = True) -> None:
        payload = event_for(kind, state.state, state.attributes.get("unit_of_measurement"))
        if payload is None:
            return
        entity_id = state.entity_id
        co_meter = state.domain == "sensor" and kind == "gas"
        if co_meter and throttle and self._co_sent.get(entity_id) == payload:
            return
        if kind in READING_KINDS:
            if entity_id in self._reading_pending:
                return
            sent_at = self._reading_sent_at.get(entity_id)
            elapsed = (dt_util.utcnow() - sent_at).total_seconds() if sent_at is not None else None
            if throttle and elapsed is not None and elapsed < READING_INTERVAL_SECONDS:
                # A @callback (not a bare lambda, which Home Assistant would
                # run as an executor job off the event loop -- Fable M-1).
                self._reading_pending[entity_id] = async_call_later(
                    self._hass,
                    READING_INTERVAL_SECONDS - elapsed,
                    HassJob(partial(self._send_pending_reading, entity_id), cancel_on_shutdown=True),
                )
                return
        _LOGGER.debug("Kameraposti exporting %s -> %s", entity_id, payload)
        if self._publish(self._topic(entity_id), payload, False):
            if kind in READING_KINDS:
                self._reading_sent_at[entity_id] = dt_util.utcnow()
            if co_meter:
                self._co_sent[entity_id] = payload

    @callback
    def _send_pending_reading(self, entity_id: str, _now: datetime) -> None:
        """Trailing send: the entity's value at the end of the interval."""
        self._reading_pending.pop(entity_id, None)
        state = self._hass.states.get(entity_id)
        kind = kind_for(state)
        if state is not None and kind in READING_KINDS:
            # The interval has passed; a timer firing a hair early must not re-arm.
            self._publish_state(state, kind, throttle=False)
