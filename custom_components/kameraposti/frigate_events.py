"""Frigate's detections to Kameraposti (1.11.0, steelo 2026-10-17 "frigaten tunnistukset", "frigatenotify").

Like Frigate-Notify, this listens to Frigate's own MQTT events (``<topic_prefix>/events``) through
Home Assistant's MQTT integration. Frigate publishes the first message of a tracked object (type
``new``) once it is no longer a false positive, so each one is a new detection: a person walking in,
a car driving up. It goes to Kameraposti as a motion of that camera, on a sensor
``binary_sensor.<camera>_frigate`` named "<camera> Frigate"; Kameraposti links it to the Home
Assistant camera ``camera.<camera>`` by that name, and the motion brings the camera's photo (Frigate's
picture of the detection, capture.py) and, armed, the alarm.

Nothing to set or tick. A Frigate binary sensor that stays on (a parked car, a tree in the wind)
never changes, so the sensor export never told Kameraposti anything from such a camera; a new
detection still is one.
"""

from __future__ import annotations

import json
import logging
import re
from collections import OrderedDict
from collections.abc import Callable

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from homeassistant.util import slugify

from .const import SENSOR_TOPIC_TEMPLATE
from .sensor_export import mqtt_name_for

_LOGGER = logging.getLogger(__name__)

PublishFn = Callable[[str, str, bool], bool]

DEFAULT_TOPIC_PREFIX = "frigate"
# Several objects at once (a family, a car and its driver) are one motion; Kameraposti has its own
# motion interval as well.
MIN_INTERVAL_SECONDS = 5
_SEEN_EVENTS = 500
_MAX_NAME_LENGTH = 60
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


def sensor_entity_id(camera_object_id: str) -> str:
    """The name Kameraposti knows the camera's detections by (linked by the camera's name)."""
    return f"binary_sensor.{camera_object_id}_frigate"


def frigate_topic_prefixes(hass: HomeAssistant) -> set[str]:
    """Frigate's MQTT topic prefixes: the default and those the Frigate integration read from Frigate."""
    prefixes = {DEFAULT_TOPIC_PREFIX}
    data = hass.data.get("frigate")
    if isinstance(data, dict):
        for entry_data in data.values():
            config = entry_data.get("config") if isinstance(entry_data, dict) else None
            mqtt_config = config.get("mqtt") if isinstance(config, dict) else None
            prefix = mqtt_config.get("topic_prefix") if isinstance(mqtt_config, dict) else None
            if isinstance(prefix, str) and prefix.strip() and not set(prefix) & set("#+"):
                prefixes.add(prefix.strip())
    return prefixes


def new_detection(payload: str | bytes) -> tuple[str, str, str] | None:
    """(event id, camera, label) of a new, real, moving detection in a Frigate event, else None."""
    try:
        data = json.loads(payload)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("type") != "new":
        return None
    after = data.get("after")
    if not isinstance(after, dict):
        return None
    event_id, camera, label = after.get("id"), after.get("camera"), after.get("label")
    if not (isinstance(event_id, str) and event_id and isinstance(camera, str) and camera):
        return None
    if after.get("false_positive") is True or after.get("stationary") is True:
        return None
    return event_id, camera, label if isinstance(label, str) else ""


class KameraportiFrigateDetections:
    """Sends each new Frigate detection to Kameraposti as a motion of its camera."""

    def __init__(self, hass: HomeAssistant, *, customer_id: int, publish: PublishFn) -> None:
        self._hass = hass
        self._customer_id = customer_id
        self._publish = publish
        self._unsubscribe: list[Callable[[], None]] = []
        self._stopped = False
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._last_sent: dict[str, float] = {}
        # Sensors described on the current connection (Kameraposti makes the sensor from the description).
        self._described: set[str] = set()

    async def async_start(self) -> None:
        """Subscribe to Frigate's events, if Home Assistant has MQTT (Frigate's integration needs it)."""
        self._stopped = False
        if "mqtt" not in self._hass.config.components or not await mqtt.async_wait_for_mqtt_client(
            self._hass
        ):
            _LOGGER.debug("Kameraposti: no MQTT integration in Home Assistant, Frigate's detections not read")
            return
        for prefix in sorted(frigate_topic_prefixes(self._hass)):
            if self._stopped:
                return
            try:
                self._unsubscribe.append(
                    await mqtt.async_subscribe(self._hass, f"{prefix}/events", self._handle_event, qos=0)
                )
            except Exception as err:  # noqa: BLE001 -- the rest of the integration must keep running
                _LOGGER.warning(
                    "Kameraposti could not listen to Frigate's events on %s/events: %s", prefix, err
                )
        _LOGGER.debug(
            "Kameraposti listening to Frigate's detections on %s", sorted(frigate_topic_prefixes(self._hass))
        )

    @callback
    def async_stop(self) -> None:
        self._stopped = True
        for unsubscribe in self._unsubscribe:
            unsubscribe()
        self._unsubscribe.clear()
        self._described.clear()

    @callback
    def async_on_connected(self) -> None:
        """A new connection to Kameraposti: describe each sensor again before its next motion."""
        self._described.clear()

    @callback
    def _handle_event(self, msg: mqtt.ReceiveMessage) -> None:
        detection = new_detection(msg.payload)
        if detection is None:
            return
        event_id, camera, label = detection
        if event_id in self._seen:
            return
        self._seen[event_id] = None
        if len(self._seen) > _SEEN_EVENTS:
            self._seen.popitem(last=False)
        now = dt_util.utcnow().timestamp()
        last = self._last_sent.get(camera)
        if last is not None and now - last < MIN_INTERVAL_SECONDS:
            return
        object_id, name = self._camera(camera)
        entity_id = sensor_entity_id(object_id)
        topic = SENSOR_TOPIC_TEMPLATE.format(customer_id=self._customer_id, name=mqtt_name_for(entity_id))
        if entity_id not in self._described:
            payload = json.dumps({"name": name, "kind": "motion", "format": "simple"}, ensure_ascii=False)
            if not self._publish(f"{topic}/config", payload, False):
                return
            self._described.add(entity_id)
        if self._publish(topic, "motion", False):
            self._last_sent[camera] = now
            _LOGGER.debug("Kameraposti: Frigate detected %s on %s -> motion of %s", label, camera, entity_id)

    def _camera(self, camera: str) -> tuple[str, str]:
        """The Home Assistant camera of a Frigate camera: (its object id, the sensor's name)."""
        suffix = f":camera:{camera}"
        object_id = slugify(camera)
        for entry in er.async_get(self._hass).entities.values():
            if (
                entry.platform == "frigate"
                and entry.domain == "camera"
                and (entry.unique_id or "").endswith(suffix)
            ):
                object_id = entry.entity_id.split(".", 1)[1]
                break
        state = self._hass.states.get(f"camera.{object_id}")
        friendly = str(state.attributes.get("friendly_name") or "") if state is not None else ""
        base = _CONTROL_CHARS.sub("", friendly).strip() or camera
        return object_id, f"{base[: _MAX_NAME_LENGTH - len(' Frigate')].strip()} Frigate"
