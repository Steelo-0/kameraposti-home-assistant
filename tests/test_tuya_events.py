"""1.15.0 (steelo 2026-10-20 "nedis käyttää tuya pluginia HA", "aivan liian monimutkainen laittaa"): a
Tuya camera's alarm (its event entity: "Doorbell message", "Doorbell picture") is a motion of that camera
in Kameraposti, like a Frigate detection. Turva 20.10: an account with two Tuya cameras had not one sensor.
"""

from __future__ import annotations

import json
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from freezegun.api import FrozenDateTimeFactory
from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_USERNAME, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_EXPORTED_ENTITIES, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.mqtt_client import ConnectionState
from custom_components.kameraposti.sensor_export import mqtt_name_for
from custom_components.kameraposti.tuya_events import KameraportiTuyaAlarms, tuya_alarm_names

CUSTOMER_ID = 12
BASE = f"kameraposti/{CUSTOMER_ID}/anturit/binary_sensor.ulkokamera_tuya"
ALARM = "event.ulkokamera_doorbell_message"
PICTURE = "event.ulkokamera_doorbell_picture"


class _Broker:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []
        self.up = True

    def publish(self, topic: str, payload: str, retain: bool) -> bool:
        assert retain is False
        if self.up:
            self.sent.append((topic, payload))
        return self.up

    def motions(self) -> list[str]:
        return [topic for topic, payload in self.sent if payload == "motion"]


def _tuya_camera(
    hass: HomeAssistant,
    object_id: str = "ulkokamera",
    device: str = "bf1234567890abcdefgh",
    *,
    events: tuple[str, ...] = ("alarm_message", "doorbell_pic"),
    platform: str = "tuya",
    camera: bool = True,
) -> None:
    """A Tuya camera as Home Assistant's Tuya integration registers it: the camera on the device and its
    event entities (unique_id = the device id + the data point, entity_id = <device>_<translation key>)."""
    config_entry = MockConfigEntry(domain=platform)
    config_entry.add_to_hass(hass)
    device_entry = dr.async_get(hass).async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={(platform, device)}, name=object_id.title()
    )
    registry = er.async_get(hass)
    if camera:
        registry.async_get_or_create(
            "camera",
            platform,
            device,
            suggested_object_id=object_id,
            device_id=device_entry.id,
            config_entry=config_entry,
        )
        hass.states.async_set(f"camera.{object_id}", "idle", {"friendly_name": object_id.title()})
    suffix = {"alarm_message": "doorbell_message", "doorbell_pic": "doorbell_picture"}
    for dpcode in events:
        entity_id = f"event.{object_id}_{suffix[dpcode]}"
        registry.async_get_or_create(
            "event",
            platform,
            f"{device}{dpcode}",
            suggested_object_id=entity_id.split(".", 1)[1],
            device_id=device_entry.id,
            config_entry=config_entry,
        )
        hass.states.async_set(entity_id, STATE_UNKNOWN, {"event_type": None})


_alarms = iter(range(1, 1_000_000))


def _fire(hass: HomeAssistant, entity_id: str, at=None, message: str | None = None) -> str:
    """The entity's event as EventEntity writes it: its state is the event's time, the attributes the
    alarm (a real alarm's message is its own: picture, time). Returns the message."""
    at = at or dt_util.utcnow()
    message = message if message is not None else f"alarm {next(_alarms)}"
    hass.states.async_set(
        entity_id, at.isoformat(timespec="milliseconds"), {"event_type": "triggered", "message": message}
    )
    return message


async def _started(hass: HomeAssistant, broker: _Broker) -> KameraportiTuyaAlarms:
    alarms = KameraportiTuyaAlarms(hass, customer_id=CUSTOMER_ID, publish=broker.publish)
    alarms.async_start()
    await hass.async_block_till_done()
    return alarms


async def test_a_tuya_cameras_alarm_is_a_motion_of_that_camera(hass: HomeAssistant) -> None:
    _tuya_camera(hass)
    broker = _Broker()
    alarms = await _started(hass, broker)

    _fire(hass, ALARM)
    await hass.async_block_till_done()

    assert broker.sent == [
        (f"{BASE}/config", json.dumps({"name": "Ulkokamera Tuya", "kind": "motion", "format": "simple"})),
        (BASE, "motion"),
    ]
    alarms.async_stop()


async def test_a_doorbell_press_is_one_motion_and_the_next_alarm_another(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """A press brings both of the doorbell's events; within 5 s they are one motion. Another camera is its
    own."""
    _tuya_camera(hass)
    _tuya_camera(hass, "ovikello", "bf0000000000ovikello", events=("doorbell_pic",))
    broker = _Broker()
    alarms = await _started(hass, broker)

    _fire(hass, PICTURE)
    _fire(hass, ALARM)
    freezer.tick(timedelta(seconds=2))
    _fire(hass, ALARM)
    _fire(hass, "event.ovikello_doorbell_picture")
    await hass.async_block_till_done()
    assert broker.motions() == [BASE, f"kameraposti/{CUSTOMER_ID}/anturit/binary_sensor.ovikello_tuya"]

    freezer.tick(timedelta(seconds=6))
    _fire(hass, ALARM)
    await hass.async_block_till_done()
    assert broker.motions()[-1] == BASE
    assert len(broker.motions()) == 3
    # Described once per connection, again after a reconnect.
    assert sum(topic == f"{BASE}/config" for topic, _ in broker.sent) == 1
    alarms.async_on_connected()
    freezer.tick(timedelta(seconds=6))
    _fire(hass, ALARM)
    await hass.async_block_till_done()
    assert [topic for topic, _ in broker.sent[-2:]] == [f"{BASE}/config", BASE]
    alarms.async_stop()


async def test_restored_and_old_events_are_no_motion(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """Home Assistant restores an event entity's last event at start: that is no new alarm."""
    _tuya_camera(hass)
    broker = _Broker()
    alarms = await _started(hass, broker)
    earlier = dt_util.utcnow() - timedelta(minutes=10)

    # An old event time, from unavailable.
    hass.states.async_set(ALARM, STATE_UNAVAILABLE)
    _fire(hass, ALARM, earlier)
    # Added again (integration reload): no old state at all.
    hass.states.async_remove(PICTURE)
    _fire(hass, PICTURE)
    # Unavailable, unknown and not a time are no event; nor is one without a message.
    hass.states.async_set(ALARM, STATE_UNAVAILABLE)
    hass.states.async_set(ALARM, "not a time")
    hass.states.async_set(ALARM, "2026-10-20T18:00:00")
    _fire(hass, ALARM, message="")
    await hass.async_block_till_done()
    assert broker.sent == []
    freezer.tick(timedelta(seconds=1))

    # The same event time again (only an attribute changed), or an older one, is not a new event.
    now = dt_util.utcnow()
    _fire(hass, ALARM, now)
    await hass.async_block_till_done()
    assert len(broker.motions()) == 1
    freezer.tick(timedelta(seconds=6))
    hass.states.async_set(
        ALARM, now.isoformat(timespec="milliseconds"), {"event_type": "triggered", "message": "x", "y": 1}
    )
    _fire(hass, ALARM, now - timedelta(seconds=1))
    await hass.async_block_till_done()
    assert len(broker.motions()) == 1
    alarms.async_stop()


async def test_an_online_update_replaying_the_last_alarm_is_no_motion(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """Fable audit 2026-10-20 H-1: the camera going offline and online fires its event entity again with
    the alarm it holds and a fresh time (Tuya's update without changed data points). Same message: no
    motion -- also when the last one was restored at start. A battery camera's alarm reported as it wakes
    has a new message and is one."""
    _tuya_camera(hass)
    restored = "alarm before the restart"
    hass.states.async_set(
        PICTURE, "2026-10-20T08:00:00.000+00:00", {"event_type": "triggered", "message": restored}
    )
    broker = _Broker()
    alarms = await _started(hass, broker)

    # Offline: the replay lands as unavailable. Online: the same alarm, fresh time.
    hass.states.async_set(PICTURE, STATE_UNAVAILABLE)
    _fire(hass, PICTURE, message=restored)
    first = _fire(hass, ALARM)
    await hass.async_block_till_done()
    assert broker.motions() == [BASE]

    freezer.tick(timedelta(seconds=6))
    hass.states.async_set(ALARM, STATE_UNAVAILABLE)
    _fire(hass, ALARM, message=first)
    freezer.tick(timedelta(seconds=6))
    # Online and staying online: the same again (another update without data points).
    _fire(hass, ALARM, message=first)
    await hass.async_block_till_done()
    assert broker.motions() == [BASE]

    # Woken by motion: its new alarm, from unavailable.
    freezer.tick(timedelta(seconds=6))
    hass.states.async_set(ALARM, STATE_UNAVAILABLE)
    _fire(hass, ALARM)
    await hass.async_block_till_done()
    assert broker.motions() == [BASE, BASE]
    alarms.async_stop()


async def test_only_a_tuya_cameras_own_events_count(hass: HomeAssistant) -> None:
    """Another integration's event entity, a Tuya event without a camera on its device (a wireless
    button), a disabled camera or event entity: nothing."""
    _tuya_camera(hass, "nappi", "bf00000000000000nappi", camera=False)
    _tuya_camera(hass, "muu", "muu-1", platform="demo")
    _tuya_camera(hass, "pois", "bf000000000000000pois")
    registry = er.async_get(hass)
    registry.async_update_entity("camera.pois", disabled_by=er.RegistryEntryDisabler.USER)
    _tuya_camera(hass)
    registry.async_update_entity(ALARM, disabled_by=er.RegistryEntryDisabler.USER)
    hass.states.async_set("event.irrallinen", STATE_UNKNOWN)
    broker = _Broker()
    alarms = await _started(hass, broker)

    for entity_id in (
        "event.nappi_doorbell_message",
        "event.muu_doorbell_message",
        "event.pois_doorbell_message",
        "event.irrallinen",
        ALARM,
    ):
        _fire(hass, entity_id)
    await hass.async_block_till_done()
    assert broker.sent == []

    _fire(hass, PICTURE)
    await hass.async_block_till_done()
    assert broker.motions() == [BASE]
    alarms.async_stop()


async def test_disconnected_the_alarm_is_dropped_and_described_again(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    _tuya_camera(hass)
    broker = _Broker()
    broker.up = False
    alarms = await _started(hass, broker)
    _fire(hass, ALARM)
    await hass.async_block_till_done()
    assert broker.sent == []

    broker.up = True
    freezer.tick(timedelta(seconds=1))
    _fire(hass, ALARM)
    await hass.async_block_till_done()
    assert broker.sent == [
        (f"{BASE}/config", json.dumps({"name": "Ulkokamera Tuya", "kind": "motion", "format": "simple"})),
        (BASE, "motion"),
    ]
    alarms.async_stop()
    # Stopped: nothing more.
    freezer.tick(timedelta(seconds=6))
    _fire(hass, ALARM)
    await hass.async_block_till_done()
    assert len(broker.motions()) == 1


async def test_the_alarm_sensor_is_described_as_this_home_assistants(hass: HomeAssistant) -> None:
    _tuya_camera(hass)
    broker = _Broker()
    alarms = await _started(hass, broker)
    alarms.async_set_source("kp-12.abc")

    _fire(hass, ALARM)
    await hass.async_block_till_done()

    assert json.loads(broker.sent[0][1])["source"] == "kp-12.abc"
    alarms.async_stop()


async def test_a_long_camera_name_is_cut(hass: HomeAssistant) -> None:
    object_id = "erittain_pitka_kameran_nimi_jota_kukaan_ei_jaksa_kirjoittaa_tuskin"
    _tuya_camera(hass, object_id)
    hass.states.async_set(f"camera.{object_id}", "idle", {"friendly_name": "Ä" * 80})
    broker = _Broker()
    alarms = await _started(hass, broker)

    _fire(hass, f"event.{object_id}_doorbell_message")
    await hass.async_block_till_done()

    topic, payload = broker.sent[0]
    assert (
        topic
        == f"kameraposti/{CUSTOMER_ID}/anturit/{mqtt_name_for(f'binary_sensor.{object_id}_tuya')}/config"
    )
    assert json.loads(payload)["name"] == "Ä" * 55 + " Tuya"
    alarms.async_stop()


async def test_the_selection_list_keeps_every_tuya_cameras_alarm_sensor(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """1.12.0's list drops what this Home Assistant described but did not list: the Tuya cameras' alarm
    sensors are on it (like Frigate's detection sensors), chosen or not."""
    _tuya_camera(hass)
    _tuya_camera(hass, "ovikello", "bf0000000000ovikello", events=("doorbell_pic",))
    _tuya_camera(hass, "nappi", "bf00000000000000nappi", camera=False)
    er.async_get(hass).async_get_or_create(
        "camera", "frigate", "frigate:camera:piha", suggested_object_id="piha"
    )
    assert tuya_alarm_names(hass) == ["binary_sensor.ovikello_tuya", "binary_sensor.ulkokamera_tuya"]

    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_HOST: "cam.steels.me",
            CONF_CUSTOMER_ID: CUSTOMER_ID,
            CONF_USERNAME: "kp-12",
            CONF_PASSWORD: "x",
        },
        options={CONF_EXPORTED_ENTITIES: []},
        version=2,
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_start = AsyncMock()
    client.async_stop = AsyncMock()
    client.publish.return_value = True
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", return_value=client):
        coordinator = KameraportiCoordinator(
            hass, entry, host="cam.steels.me", customer_id=CUSTOMER_ID, username="kp-12", password="x"
        )
    await coordinator.async_start()
    coordinator._handle_state_change(ConnectionState.CONNECTED)
    await hass.async_block_till_done()

    [selection] = [c.args[1] for c in client.publish.call_args_list if c.args[0].endswith("/_ha/sync")]
    assert json.loads(selection)["entities"] == [
        "binary_sensor.piha_frigate",
        "binary_sensor.ovikello_tuya",
        "binary_sensor.ulkokamera_tuya",
    ]

    # The coordinator hears the alarm, describes the sensor again after a reconnect, and stops with it.
    _fire(hass, ALARM)
    await hass.async_block_till_done()
    coordinator._handle_state_change(ConnectionState.RECONNECTING)
    coordinator._handle_state_change(ConnectionState.CONNECTED)
    freezer.tick(timedelta(seconds=6))
    _fire(hass, ALARM)
    await hass.async_block_till_done()
    await coordinator.async_stop()
    freezer.tick(timedelta(seconds=6))
    _fire(hass, ALARM)
    await hass.async_block_till_done()

    tuya = [c.args[0] for c in client.publish.call_args_list if "_tuya" in c.args[0]]
    assert tuya == [f"{BASE}/config", BASE, f"{BASE}/config", BASE]
