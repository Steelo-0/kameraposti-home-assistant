"""Export chosen Home Assistant entities to Kameraposti as sensors.

The user picks entities in the integration's options (leak, smoke, door,
window, motion binary sensors and temperature sensors). For each one this
module publishes, over the integration's own connection:

* ``kameraposti/<id>/anturit/<name>/config`` -- {"name", "kind", "format"};
  Kameraposti creates the sensor (or updates its name/kind) automatically.
* ``kameraposti/<id>/anturit/<name>`` -- the state in Kameraposti's simple
  format: leak/dry, smoke/clear, open/closed, motion, or the temperature in
  degrees Celsius.

``<name>`` is the entity_id, so it stays stable across friendly-name changes.
On every (re)connect the sensors are described again and their current state
is sent (not motion: a motion sensor that happens to be "on" at reconnect is
not a new movement), so an alarm that started during a break is not lost.
Kameraposti drops a repeated leak/dry/open/closed state on its own.
A temperature is sent at most once a minute per entity (the latest value
follows when the minute is up), so a chatty thermometer cannot use up the
account's shared message budget and crowd out a leak alarm.

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
from datetime import datetime

from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import CALLBACK_TYPE, Event, EventStateChangedData, HomeAssistant, State, callback
from homeassistant.helpers.event import async_call_later, async_track_state_change_event
from homeassistant.util import dt as dt_util

from .const import SENSOR_TOPIC_TEMPLATE

_LOGGER = logging.getLogger(__name__)

# binary_sensor device_class -> Kameraposti sensor kind.
KIND_BY_BINARY_DEVICE_CLASS: dict[str, str] = {
    "moisture": "leak",
    "smoke": "smoke",
    "door": "door",
    "garage_door": "door",
    "opening": "door",
    "window": "window",
    "motion": "motion",
    "occupancy": "motion",
    "presence": "motion",
}

# Kameraposti simple-format event for a binary sensor that is on / off.
_EVENTS_BY_KIND: dict[str, tuple[str, str | None]] = {
    "leak": ("leak", "dry"),
    "smoke": ("smoke", "clear"),
    "door": ("open", "closed"),
    "window": ("open", "closed"),
    "motion": ("motion", None),
}

# Kameraposti accepts topic names [A-Za-z0-9_.-]{1,64} and names of 1-60
# characters without control characters.
_MAX_NAME_LENGTH = 64
_MAX_DISPLAY_NAME_LENGTH = 60
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")

# Minimum interval between two temperature messages from one entity.
TEMPERATURE_INTERVAL_SECONDS = 60

PublishFn = Callable[[str, str, bool], bool]


def kind_for(state: State | None) -> str | None:
    """Kameraposti kind for an entity, or None when it cannot be exported."""
    if state is None:
        return None
    device_class = state.attributes.get("device_class")
    if state.domain == "binary_sensor":
        return KIND_BY_BINARY_DEVICE_CLASS.get(device_class)
    if state.domain == "sensor" and device_class == "temperature":
        return "temperature"
    return None


def event_for(kind: str, state: str, unit: str | None = None) -> str | None:
    """Kameraposti simple-format payload for a state, or None to send nothing."""
    if state in (STATE_UNAVAILABLE, STATE_UNKNOWN):
        return None
    if kind == "temperature":
        try:
            value = float(state)
        except ValueError:
            return None
        if not math.isfinite(value):
            return None
        if unit == "°F":
            return str(round((value - 32) * 5 / 9, 2))
        if unit not in (None, "°C"):
            return None
        return state
    events = _EVENTS_BY_KIND.get(kind)
    if events is None:
        return None
    if state == "on":
        return events[0]
    if state == "off":
        return events[1]
    return None


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
        # entity_id -> (name, kind) last described on the current connection.
        self._described: dict[str, tuple[str, str]] = {}
        # Temperature throttle: last sent time and the pending trailing send.
        self._temperature_sent_at: dict[str, datetime] = {}
        self._temperature_pending: dict[str, CALLBACK_TYPE] = {}

    @callback
    def async_start(self) -> None:
        if self._unsub is None and self._entity_ids:
            self._unsub = async_track_state_change_event(
                self._hass, self._entity_ids, self._handle_state_event
            )

    @callback
    def async_stop(self) -> None:
        if self._unsub is not None:
            self._unsub()
            self._unsub = None
        for cancel in self._temperature_pending.values():
            cancel()
        self._temperature_pending.clear()
        self._described.clear()

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
        if kind == "temperature":
            if entity_id in self._temperature_pending:
                return
            sent_at = self._temperature_sent_at.get(entity_id)
            elapsed = (dt_util.utcnow() - sent_at).total_seconds() if sent_at is not None else None
            if throttle and elapsed is not None and elapsed < TEMPERATURE_INTERVAL_SECONDS:
                self._temperature_pending[entity_id] = async_call_later(
                    self._hass,
                    TEMPERATURE_INTERVAL_SECONDS - elapsed,
                    lambda _now: self._send_pending_temperature(entity_id),
                )
                return
        _LOGGER.debug("Kameraposti exporting %s -> %s", entity_id, payload)
        if self._publish(self._topic(entity_id), payload, False) and kind == "temperature":
            self._temperature_sent_at[entity_id] = dt_util.utcnow()

    @callback
    def _send_pending_temperature(self, entity_id: str) -> None:
        """Trailing send: the entity's value at the end of the interval."""
        self._temperature_pending.pop(entity_id, None)
        state = self._hass.states.get(entity_id)
        if state is not None and kind_for(state) == "temperature":
            self._publish_state(state, "temperature")
