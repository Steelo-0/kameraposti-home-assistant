"""1.18.0 (Turva onboarding contract section 1): the hello after every connect.

kameraposti/<account>/anturit/_ha/hello, QoS 1, not retained, once after every successful connect (start and
reconnect): {"version", "alarm_candidates", "cameras"} and nothing else. Turva shows from it that Home
Assistant is connected, also before any sensor has arrived, and how many alarm sensors it found.
"""

from __future__ import annotations

import json
import re
from collections.abc import Generator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import paho.mqtt.client as mqtt
import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_EXPORTED_ENTITIES, CONF_HOST, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.mqtt_client import ConnectionState

HELLO_TOPIC = "kameraposti/3/anturit/_ha/hello"
ENTRY_DATA = {CONF_HOST: "turva.secsrv.fi", CONF_CUSTOMER_ID: 3, CONF_USERNAME: "kp-3", CONF_PASSWORD: "x"}
MANIFEST = Path(__file__).parent.parent / "custom_components" / "kameraposti" / "manifest.json"
# The service's check of the version (contract section 1).
SERVICE_VERSION_PATTERN = re.compile(r"^[0-9]{1,3}(\.[0-9]{1,3}){0,3}$")


def _version() -> str:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))["version"]


def _set_sensors(hass: HomeAssistant) -> None:
    """Two alarm sensors sent by themselves, a temperature that is not, two cameras."""
    hass.states.async_set("binary_sensor.ovi", "off", {"friendly_name": "Etuovi", "device_class": "door"})
    hass.states.async_set("binary_sensor.savu", "off", {"friendly_name": "Savu", "device_class": "smoke"})
    hass.states.async_set(
        "sensor.lampo",
        "21.5",
        {"friendly_name": "Lämpö", "device_class": "temperature", "unit_of_measurement": "°C"},
    )
    hass.states.async_set("camera.piha", "idle", {"friendly_name": "Piha"})
    hass.states.async_set("camera.talli", "idle", {"friendly_name": "Talli"})


@pytest.fixture
def paho() -> Generator[MagicMock]:
    with patch("custom_components.kameraposti.mqtt_client.mqtt.Client") as mock_cls:
        instance = MagicMock(name="paho")
        instance.is_connected.return_value = True
        instance.publish.return_value = MagicMock(rc=mqtt.MQTT_ERR_SUCCESS, mid=1)
        mock_cls.return_value = instance
        yield instance


def _hellos(paho: MagicMock) -> list:
    return [c for c in paho.publish.call_args_list if c.args[0] == HELLO_TOPIC]


async def test_every_connect_says_hello_once_with_qos_1_not_retained(
    hass: HomeAssistant, paho: MagicMock, freezer: FrozenDateTimeFactory
) -> None:
    """Through a real entry and MQTT client (only paho mocked): the start, and each reconnect -- also one
    within the snapshot's minute, which sends no new snapshot."""
    hass.config.location_name = "Mökki"
    _set_sensors(hass)
    entry = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, version=2, minor_version=2, unique_id="kp-3")
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert _hellos(paho) == []  # not before the broker's CONNACK

    paho.on_connect(paho, None, MagicMock(), 0, None)
    await hass.async_block_till_done()

    [hello] = _hellos(paho)
    assert hello.args == (HELLO_TOPIC, hello.args[1])
    assert hello.kwargs == {"qos": 1, "retain": False}
    assert json.loads(hello.args[1]) == {"version": _version(), "alarm_candidates": 2, "cameras": 2}
    assert "Mökki" not in hello.args[1]
    # First, before the descriptions.
    assert paho.publish.call_args_list[0].args[0] == HELLO_TOPIC

    for reconnects in (1, 2):
        freezer.tick(5)
        paho.on_disconnect(paho, None, MagicMock(), None, None)
        await hass.async_block_till_done()
        freezer.tick(2)  # the first reconnect: 1 s + up to 1 s jitter
        async_fire_time_changed(hass)
        await hass.async_block_till_done()
        assert len(_hellos(paho)) == reconnects  # a drop alone says nothing
        paho.on_connect(paho, None, MagicMock(), 0, None)
        await hass.async_block_till_done()
        assert len(_hellos(paho)) == reconnects + 1
        assert _hellos(paho)[-1].kwargs == {"qos": 1, "retain": False}

    assert await hass.config_entries.async_unload(entry.entry_id)


async def _hello_of(hass: HomeAssistant, options: dict) -> dict:
    entry = MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, options=options, version=2)
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_start = AsyncMock()
    client.async_stop = AsyncMock()
    client.publish.return_value = True
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", return_value=client):
        coordinator = KameraportiCoordinator(
            hass, entry, host="turva.secsrv.fi", customer_id=3, username="kp-3", password="x"
        )
    await coordinator.async_start()
    coordinator._handle_state_change(ConnectionState.CONNECTED)
    await hass.async_block_till_done()
    [hello] = [c.args for c in client.publish.call_args_list if c.args[0] == HELLO_TOPIC]
    await coordinator.async_stop()
    assert hello[2] is False  # not retained
    return json.loads(hello[1])


async def test_without_a_selection_the_candidates_are_the_alarm_sensors_found(hass: HomeAssistant) -> None:
    """auto_sensors.py: smoke, gas, leak, door, window and motion; a temperature is left to the user."""
    _set_sensors(hass)

    assert await _hello_of(hass, {}) == {"version": _version(), "alarm_candidates": 2, "cameras": 2}


async def test_with_a_saved_selection_the_candidates_are_the_selection(hass: HomeAssistant) -> None:
    _set_sensors(hass)

    hello = await _hello_of(hass, {CONF_EXPORTED_ENTITIES: ["sensor.lampo", "binary_sensor.ovi"]})
    assert hello["alarm_candidates"] == 2
    # A saved empty selection sends nothing, whatever alarm sensors there are.
    assert (await _hello_of(hass, {CONF_EXPORTED_ENTITIES: []}))["alarm_candidates"] == 0


async def test_no_sensor_and_no_camera_is_a_hello_of_zeros(hass: HomeAssistant) -> None:
    """What lets Turva tell "connected, nothing found" from "not connected"."""
    hello = await _hello_of(hass, {})

    assert hello == {"version": _version(), "alarm_candidates": 0, "cameras": 0}
    assert SERVICE_VERSION_PATTERN.fullmatch(hello["version"])
