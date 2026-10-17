"""1.11.0 (steelo 2026-10-17 "frigaten tunnistukset", "frigatenotify"): each new Frigate detection
(Frigate's own MQTT event, type "new") is a motion of its camera in Kameraposti, like Frigate-Notify
reads them. A Frigate binary sensor that stays on (a parked car) never changed, so nothing was sent.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from freezegun.api import FrozenDateTimeFactory
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.frigate_events import (
    KameraportiFrigateDetections,
    frigate_topic_prefixes,
    new_detection,
)
from custom_components.kameraposti.mqtt_client import ConnectionState

CUSTOMER_ID = 2


def _event(event_id: str, camera: str = "etupiha", kind: str = "new", **after: object) -> str:
    return json.dumps(
        {
            "type": kind,
            "before": {"id": event_id, "camera": camera},
            "after": {
                "id": event_id,
                "camera": camera,
                "label": "person",
                "false_positive": False,
                "stationary": False,
            }
            | after,
        }
    )


class _Broker:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []
        self.up = True

    def publish(self, topic: str, payload: str, retain: bool) -> bool:
        assert retain is False
        if self.up:
            self.sent.append((topic, payload))
        return self.up


async def _started(hass: HomeAssistant, broker: _Broker) -> KameraportiFrigateDetections:
    detections = KameraportiFrigateDetections(hass, customer_id=CUSTOMER_ID, publish=broker.publish)
    await detections.async_start()
    await hass.async_block_till_done()
    return detections


async def test_a_new_detection_is_a_motion_of_its_camera(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    hass.states.async_set("camera.etupiha", "idle", {"friendly_name": "Etupiha"})
    broker = _Broker()
    detections = await _started(hass, broker)

    async_fire_mqtt_message(hass, "frigate/events", _event("1700000000.1-abc"))
    await hass.async_block_till_done()

    base = f"kameraposti/{CUSTOMER_ID}/anturit/binary_sensor.etupiha_frigate"
    assert broker.sent == [
        (f"{base}/config", json.dumps({"name": "Etupiha Frigate", "kind": "motion", "format": "simple"})),
        (base, "motion"),
    ]
    detections.async_stop()


async def test_updates_ends_false_positives_parked_cars_and_repeats_send_nothing(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, freezer: FrozenDateTimeFactory
) -> None:
    broker = _Broker()
    detections = await _started(hass, broker)

    for payload in (
        _event("a", kind="update"),
        _event("a", kind="end"),
        _event("b", false_positive=True),
        _event("c", stationary=True),
        "not json",
        json.dumps({"type": "new", "after": {"camera": "etupiha"}}),
        json.dumps(["new"]),
    ):
        async_fire_mqtt_message(hass, "frigate/events", payload)
    await hass.async_block_till_done()
    assert broker.sent == []

    async_fire_mqtt_message(hass, "frigate/events", _event("d"))
    async_fire_mqtt_message(hass, "frigate/events", _event("d"))
    # Another object within 5 s on the same camera is the same motion; another camera is its own.
    async_fire_mqtt_message(hass, "frigate/events", _event("e"))
    async_fire_mqtt_message(hass, "frigate/events", _event("f", camera="kamera2"))
    await hass.async_block_till_done()
    assert [payload for _, payload in broker.sent].count("motion") == 2

    freezer.tick(timedelta(seconds=6))
    # The same detection again later (a broker re-delivery) is not a new one.
    async_fire_mqtt_message(hass, "frigate/events", _event("d"))
    await hass.async_block_till_done()
    assert [payload for _, payload in broker.sent].count("motion") == 2
    async_fire_mqtt_message(hass, "frigate/events", _event("g"))
    await hass.async_block_till_done()
    states = [topic for topic, payload in broker.sent if payload == "motion"]
    assert states == [
        f"kameraposti/{CUSTOMER_ID}/anturit/binary_sensor.etupiha_frigate",
        f"kameraposti/{CUSTOMER_ID}/anturit/binary_sensor.kamera2_frigate",
        f"kameraposti/{CUSTOMER_ID}/anturit/binary_sensor.etupiha_frigate",
    ]
    # Described once per connection, again after a reconnect.
    assert sum(topic.endswith("/config") for topic, _ in broker.sent) == 2
    detections.async_on_connected()
    freezer.tick(timedelta(seconds=6))
    async_fire_mqtt_message(hass, "frigate/events", _event("h"))
    await hass.async_block_till_done()
    assert broker.sent[-2][0].endswith("binary_sensor.etupiha_frigate/config")
    detections.async_stop()


async def test_disconnected_the_detection_is_dropped_and_described_again(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    broker = _Broker()
    broker.up = False
    detections = await _started(hass, broker)
    async_fire_mqtt_message(hass, "frigate/events", _event("x"))
    await hass.async_block_till_done()
    assert broker.sent == []

    broker.up = True
    async_fire_mqtt_message(hass, "frigate/events", _event("y"))
    await hass.async_block_till_done()
    assert [payload for _, payload in broker.sent][-1] == "motion"
    assert broker.sent[0][0].endswith("/config")
    detections.async_stop()
    # Stopped: nothing more.
    async_fire_mqtt_message(hass, "frigate/events", _event("z", camera="kamera3"))
    await hass.async_block_till_done()
    assert not any("kamera3" in topic for topic, _ in broker.sent)


async def test_the_frigate_camera_entity_and_the_topic_prefix_come_from_the_frigate_integration(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    registry = er.async_get(hass)
    # Another integration's camera with a look-alike id is not Frigate's.
    registry.async_get_or_create("camera", "generic", "x:camera:Front Yard", suggested_object_id="vaara")
    registry.async_get_or_create(
        "camera", "frigate", "entry1:camera:Front Yard", suggested_object_id="piha_etu"
    )
    hass.states.async_set("camera.piha_etu", "idle", {"friendly_name": "Piha etu"})
    hass.data["frigate"] = {"entry1": {"config": {"mqtt": {"topic_prefix": "frigate2"}}}, "broken": None}
    assert frigate_topic_prefixes(hass) == {"frigate", "frigate2"}

    broker = _Broker()
    detections = await _started(hass, broker)
    async_fire_mqtt_message(hass, "frigate2/events", _event("p", camera="Front Yard"))
    await hass.async_block_till_done()

    assert broker.sent[0] == (
        f"kameraposti/{CUSTOMER_ID}/anturit/binary_sensor.piha_etu_frigate/config",
        json.dumps({"name": "Piha etu Frigate", "kind": "motion", "format": "simple"}),
    )
    detections.async_stop()


async def test_stopped_while_starting_subscribes_nothing(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    broker = _Broker()
    detections = KameraportiFrigateDetections(hass, customer_id=CUSTOMER_ID, publish=broker.publish)
    ready = asyncio.Event()

    async def _wait(_hass: HomeAssistant) -> bool:
        await ready.wait()
        return True

    with patch("custom_components.kameraposti.frigate_events.mqtt.async_wait_for_mqtt_client", _wait):
        task = hass.async_create_task(detections.async_start())
        await asyncio.sleep(0)
        detections.async_stop()
        ready.set()
        await task
    async_fire_mqtt_message(hass, "frigate/events", _event("s"))
    await hass.async_block_till_done()
    assert broker.sent == []


async def test_a_wildcard_prefix_is_not_subscribed_and_old_detections_are_forgotten(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, freezer: FrozenDateTimeFactory
) -> None:
    hass.data["frigate"] = {
        "e": {"config": {"mqtt": {"topic_prefix": "fr/#"}}},
        "f": {"config": {"mqtt": {}}},
    }
    assert frigate_topic_prefixes(hass) == {"frigate"}

    broker = _Broker()
    with patch("custom_components.kameraposti.frigate_events._SEEN_EVENTS", 2):
        detections = await _started(hass, broker)
        for event_id in ("a", "b", "c", "a"):
            async_fire_mqtt_message(hass, "frigate/events", _event(event_id))
            await hass.async_block_till_done()
            freezer.tick(timedelta(seconds=6))
    # Only the newest ids are remembered (bounded), so "a" counts again once forgotten.
    assert [payload for _, payload in broker.sent].count("motion") == 4
    detections.async_stop()


async def test_without_mqtt_in_home_assistant_nothing_is_subscribed(hass: HomeAssistant) -> None:
    broker = _Broker()
    detections = KameraportiFrigateDetections(hass, customer_id=CUSTOMER_ID, publish=broker.publish)
    await detections.async_start()
    detections.async_stop()
    assert broker.sent == []


def test_new_detection_parsing() -> None:
    assert new_detection(_event("1.2-x")) == ("1.2-x", "etupiha", "person")
    assert new_detection(_event("1.2-x", label=None)) == ("1.2-x", "etupiha", "")
    assert new_detection(b'{"type":"new","after":{"id":"i","camera":"c"}}') == ("i", "c", "")
    assert new_detection(_event("", camera="c")) is None
    assert new_detection(_event("i", camera="")) is None


async def test_the_coordinator_reads_frigate_and_describes_again_after_a_reconnect(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, freezer: FrozenDateTimeFactory
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN, data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-2", CONF_PASSWORD: "s"}
    )
    entry.add_to_hass(hass)
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        client = MagicMock()
        client.async_start = AsyncMock()
        client.async_stop = AsyncMock()
        client.publish.return_value = True
        mock_cls.return_value = client
        coordinator = KameraportiCoordinator(
            hass, entry, host="cam.steels.me", customer_id=CUSTOMER_ID, username="kp-2", password="s"
        )
        await coordinator.async_start()
        await hass.async_block_till_done()
        async_fire_mqtt_message(hass, "frigate/events", _event("k1"))
        await hass.async_block_till_done()
        coordinator._handle_state_change(ConnectionState.CONNECTED)
        freezer.tick(timedelta(seconds=6))
        async_fire_mqtt_message(hass, "frigate/events", _event("k2"))
        await hass.async_block_till_done()
        await coordinator.async_stop()
        async_fire_mqtt_message(hass, "frigate/events", _event("k3", camera="kamera3"))
        await hass.async_block_till_done()

    frigate = [c.args[0] for c in client.publish.call_args_list if "_frigate" in c.args[0]]
    base = f"kameraposti/{CUSTOMER_ID}/anturit/binary_sensor.etupiha_frigate"
    assert frigate == [f"{base}/config", base, f"{base}/config", base]


async def test_each_detection_is_remembered_before_kameraposti_hears_of_it(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, freezer: FrozenDateTimeFactory
) -> None:
    """1.11.1: the capture request a detection's motion brings must find it (capture.py), also a newer
    detection within the 5 s in which no motion is sent."""
    broker = _Broker()
    detections = await _started(hass, broker)
    at_publish: list[str | None] = []

    def publish(topic: str, payload: str, retain: bool) -> bool:
        if not topic.endswith("/config"):
            latest = detections.latest("camera.etupiha", dt_util.utcnow())
            at_publish.append(latest.event_id if latest is not None else None)
        return broker.publish(topic, payload, retain)

    detections._publish = publish
    start = dt_util.utcnow()
    async_fire_mqtt_message(hass, "frigate/events", _event("first", label="car"))
    await hass.async_block_till_done()
    freezer.tick(timedelta(seconds=2))
    async_fire_mqtt_message(hass, "frigate/events", _event("second"))
    await hass.async_block_till_done()

    assert at_publish == ["first"]
    latest = detections.latest("camera.etupiha", start)
    assert (latest.event_id, latest.label) == ("second", "person")
    assert detections.latest("camera.etupiha", dt_util.utcnow() + timedelta(seconds=1)) is None
    assert detections.latest("camera.muu", start) is None
    detections.async_stop()
