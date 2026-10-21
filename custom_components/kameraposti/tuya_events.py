"""Tuya cameras' alarms to Kameraposti (1.15.0, steelo 2026-10-20 "nedis käyttää tuya pluginia HA",
"aivan liian monimutkainen laittaa").

Home Assistant's Tuya integration (Nedis SmartLife and other Tuya cameras) gives a camera no motion
binary sensor. Its alarm is an event entity on the camera's device: "Doorbell message" (Tuya's
alarm_message, which a security camera sends on motion too) and, on a doorbell, "Doorbell picture" (a
press). The sensor export takes no event entity, so a Tuya camera's motion never reached Kameraposti
(Turva 20.10: an account with two Tuya cameras had not one sensor). Like Frigate's detections
(frigate_events.py), each new event goes to Kameraposti as a motion of that camera, on a sensor
``binary_sensor.<camera>_tuya`` named "<camera> Tuya"; Kameraposti links it to ``camera.<camera>`` by
that name, and the motion brings the camera's photo (a live still, capture.py) and, armed, the alarm.
Nothing to set or tick.

An event entity's state is the time of its latest event, and Home Assistant restores it at start. Only a
time newer than the previous state, and at most RECENT_SECONDS old, can be a new event -- and only with a
message other than the entity's last one. Audit 2026-10-20 (Fable H-1, measured in Home Assistant's Tuya
code): an update without a list of changed data points -- the camera going online or offline -- fires the
event entity again with the alarm it still holds and a fresh time (models.py skip_update, event.py
_handle_state_update). That replay carries the same message; a real alarm carries a new one (the alarm's
own picture and time: the message is the decoded alarm_message or the doorbell picture's address). A
battery camera that reports its alarm as it wakes still counts, as its message is new. An alarm made while
Home Assistant was off can come once, late, at the camera's next online update, and so can a brand-new
camera's first replay (no message seen before); nothing tells them apart.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from .const import SENSOR_TOPIC_TEMPLATE
from .sensor_export import mqtt_name_for

_LOGGER = logging.getLogger(__name__)

PublishFn = Callable[[str, str, bool], bool]

TUYA_PLATFORM = "tuya"
# A doorbell press brings both of its events; Kameraposti has its own motion interval as well.
MIN_INTERVAL_SECONDS = 5
# An event at most this old is new; a restored one is older.
RECENT_SECONDS = 60
_MAX_NAME_LENGTH = 60
_SUFFIX = " Tuya"
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


@callback
def tuya_cameras(hass: HomeAssistant) -> dict[str, str]:
    """Device id -> the device's Tuya camera (the first by entity_id), for every enabled Tuya camera."""
    cameras: dict[str, str] = {}
    for entry in sorted(er.async_get(hass).entities.values(), key=lambda entry: entry.entity_id):
        if (
            entry.platform == TUYA_PLATFORM
            and entry.domain == "camera"
            and not entry.disabled
            and entry.device_id is not None
        ):
            cameras.setdefault(entry.device_id, entry.entity_id)
    return cameras


def sensor_entity_id(camera_object_id: str) -> str:
    """The name Kameraposti knows the camera's alarms by (linked by the camera's name)."""
    return f"binary_sensor.{camera_object_id}_tuya"


@callback
def tuya_alarm_names(hass: HomeAssistant) -> list[str]:
    """The alarm sensors' topic names of every Tuya camera, for the selection list (sensor_export, like
    frigate_detection_names): a camera's alarms are the camera's, never dropped while it exists."""
    return [
        mqtt_name_for(sensor_entity_id(entity_id.split(".", 1)[1]))
        for entity_id in sorted(tuya_cameras(hass).values())
    ]


def _event_time(state: str) -> datetime | None:
    """The time of an event entity's latest event, or None (unknown, unavailable)."""
    at = dt_util.parse_datetime(state)
    if at is None or at.tzinfo is None:
        return None
    return at


@callback
def _is_event_entity(data: Mapping[str, Any]) -> bool:
    return str(data.get("entity_id", "")).startswith("event.")


class KameraportiTuyaAlarms:
    """Sends each new alarm event of a Tuya camera to Kameraposti as a motion of that camera."""

    def __init__(self, hass: HomeAssistant, *, customer_id: int, publish: PublishFn) -> None:
        self._hass = hass
        self._customer_id = customer_id
        self._publish = publish
        # Who describes the alarm sensor, like the sensor export (1.12.0).
        self._source: str | None = None
        self._unsubscribe: Callable[[], None] | None = None
        # camera entity_id -> when its last motion was sent.
        self._last_sent: dict[str, float] = {}
        # Sensors described on the current connection (Kameraposti makes the sensor from the description).
        self._described: set[str] = set()
        # event entity_id -> the message of its last event seen (restored ones too): a replay has the same.
        self._last_message: dict[str, str] = {}

    @callback
    def async_start(self) -> None:
        """Listen to state changes of event entities: a Tuya camera added later is heard as well."""
        if self._unsubscribe is None:
            self._unsubscribe = self._hass.bus.async_listen(
                EVENT_STATE_CHANGED, self._handle_state_event, event_filter=_is_event_entity
            )
            for state in self._hass.states.async_all("event"):
                if self._camera_of(state.entity_id) is not None:
                    self._remember(state.entity_id, state.attributes.get("message"))

    @callback
    def async_stop(self) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        self._described.clear()

    @callback
    def async_set_source(self, source: str | None) -> None:
        """This Home Assistant's login and installation id (sensor_export's selection list)."""
        self._source = source

    @callback
    def async_on_connected(self) -> None:
        """A new connection to Kameraposti: describe each sensor again before its next motion."""
        self._described.clear()

    @callback
    def _handle_state_event(self, event: Event[EventStateChangedData]) -> None:
        new_state = event.data["new_state"]
        old_state = event.data["old_state"]
        if new_state is None:
            return
        camera = self._camera_of(new_state.entity_id)
        if camera is None:
            return
        if old_state is not None:
            self._remember(new_state.entity_id, old_state.attributes.get("message"), only_first=True)
        else:
            # No old state: the entity was just added (Home Assistant starting, the integration
            # reloading) with its restored event.
            self._remember(new_state.entity_id, new_state.attributes.get("message"))
            return
        at = _event_time(new_state.state)
        if at is None:
            return
        before = _event_time(old_state.state)
        if before is not None and at <= before:
            return
        now = dt_util.utcnow()
        if (now - at).total_seconds() > RECENT_SECONDS:
            return
        message = new_state.attributes.get("message")
        if not isinstance(message, str) or not message:
            return
        if self._last_message.get(new_state.entity_id) == message:
            _LOGGER.debug(
                "Kameraposti: %s fired again with its last alarm (online update), not a motion",
                new_state.entity_id,
            )
            return
        self._last_message[new_state.entity_id] = message
        last = self._last_sent.get(camera)
        if last is not None and now.timestamp() - last < MIN_INTERVAL_SECONDS:
            return
        object_id = camera.split(".", 1)[1]
        entity_id = sensor_entity_id(object_id)
        topic = SENSOR_TOPIC_TEMPLATE.format(customer_id=self._customer_id, name=mqtt_name_for(entity_id))
        if entity_id not in self._described:
            description = {"name": self._name(camera, object_id), "kind": "motion", "format": "simple"}
            if self._source is not None:
                description["source"] = self._source
            if not self._publish(f"{topic}/config", json.dumps(description, ensure_ascii=False), False):
                return
            self._described.add(entity_id)
        if self._publish(topic, "motion", False):
            self._last_sent[camera] = now.timestamp()
            _LOGGER.debug("Kameraposti: Tuya alarm %s -> motion of %s", new_state.entity_id, entity_id)

    def _remember(self, entity_id: str, message: Any, *, only_first: bool = False) -> None:
        if isinstance(message, str) and message and not (only_first and entity_id in self._last_message):
            self._last_message[entity_id] = message

    def _camera_of(self, entity_id: str) -> str | None:
        """The Tuya camera of a Tuya event entity on the same device, or None."""
        entry = er.async_get(self._hass).async_get(entity_id)
        if entry is None or entry.platform != TUYA_PLATFORM or entry.device_id is None or entry.disabled:
            return None
        return tuya_cameras(self._hass).get(entry.device_id)

    def _name(self, camera: str, object_id: str) -> str:
        state = self._hass.states.get(camera)
        friendly = str(state.attributes.get("friendly_name") or "") if state is not None else ""
        base = _CONTROL_CHARS.sub("", friendly).strip() or object_id
        return f"{base[: _MAX_NAME_LENGTH - len(_SUFFIX)].strip()}{_SUFFIX}"
