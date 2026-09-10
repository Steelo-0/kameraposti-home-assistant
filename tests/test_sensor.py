"""Sensor entity tests: device/entity creation and shape (contract 13/14/15).

The MQTT client itself is mocked out here (async_start/async_stop are
no-ops) -- these tests are only about entity/device wiring, not about
the transport. Reconnect/backoff/auth-failure behaviour of the real
client is covered in test_mqtt_client.py, and full setup/unload/reload
lifecycle is covered in test_init.py.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator

CUSTOMER_ID = 3
CAMERA_ID = 16


def _payload(**overrides: object) -> bytes:
    base: dict[str, object] = {
        "schema_version": 1,
        "event_id": "01A",
        "camera_id": CAMERA_ID,
        "label": "animal",
        "confidence": 0.9,
        "timestamp": "2026-09-09T18:12:42+00:00",
    }
    base.update(overrides)
    return json.dumps(base).encode()


@pytest.fixture
def mock_mqtt_client() -> AsyncGenerator[None]:
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        instance = mock_cls.return_value
        instance.async_start = AsyncMock()
        instance.async_stop = AsyncMock()
        yield


@pytest.fixture
async def setup_entry(hass: HomeAssistant, mock_mqtt_client: None) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "rk-3-abc", CONF_PASSWORD: "secret"},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


async def test_first_detection_creates_one_device_and_three_entities(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][setup_entry.entry_id]

    coordinator._handle_message(f"customers/{CUSTOMER_ID}/detections/{CAMERA_ID}", _payload())
    await hass.async_block_till_done()

    device_registry = dr.async_get(hass)
    device = device_registry.async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:{CAMERA_ID}")})
    assert device is not None
    assert device.manufacturer == "Kameraposti"
    assert device.model == "Riistakamera"
    assert device.name == f"Riistakamera {CAMERA_ID}"

    entity_registry = er.async_get(hass)
    unique_ids = {e.unique_id for e in entity_registry.entities.values() if e.device_id == device.id}
    assert unique_ids == {
        f"{DOMAIN}:{CUSTOMER_ID}:{CAMERA_ID}:last_detection",
        f"{DOMAIN}:{CUSTOMER_ID}:{CAMERA_ID}:confidence",
        f"{DOMAIN}:{CUSTOMER_ID}:{CAMERA_ID}:last_detection_time",
    }

    # entity_id is derived from the device name -- "Riistakamera 16".
    last_detection = hass.states.get(f"sensor.riistakamera_{CAMERA_ID}_last_detection")
    confidence = hass.states.get(f"sensor.riistakamera_{CAMERA_ID}_detection_confidence")
    last_time = hass.states.get(f"sensor.riistakamera_{CAMERA_ID}_last_detection_time")

    assert last_detection is not None
    assert last_detection.state == "animal"
    assert confidence is not None
    assert float(confidence.state) == 0.9
    assert last_time is not None
    assert last_time.state != "unknown"


async def test_second_detection_for_the_same_camera_updates_state_without_new_entities(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][setup_entry.entry_id]
    entity_registry = er.async_get(hass)

    coordinator._handle_message(f"customers/{CUSTOMER_ID}/detections/{CAMERA_ID}", _payload(event_id="01A"))
    await hass.async_block_till_done()
    count_after_first = len(
        [e for e in entity_registry.entities.values() if e.platform == DOMAIN]
    )

    coordinator._handle_message(
        f"customers/{CUSTOMER_ID}/detections/{CAMERA_ID}",
        _payload(event_id="01B", label="person", confidence=0.5),
    )
    await hass.async_block_till_done()
    count_after_second = len(
        [e for e in entity_registry.entities.values() if e.platform == DOMAIN]
    )

    assert count_after_second == count_after_first

    last_detection = hass.states.get(f"sensor.riistakamera_{CAMERA_ID}_last_detection")
    assert last_detection.state == "person"


async def test_two_different_cameras_get_two_separate_devices(
    hass: HomeAssistant, setup_entry: MockConfigEntry
) -> None:
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][setup_entry.entry_id]

    coordinator._handle_message(f"customers/{CUSTOMER_ID}/detections/16", _payload(event_id="01A", camera_id=16))
    coordinator._handle_message(f"customers/{CUSTOMER_ID}/detections/17", _payload(event_id="01B", camera_id=17))
    await hass.async_block_till_done()

    device_registry = dr.async_get(hass)
    assert device_registry.async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:16")}) is not None
    assert device_registry.async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:17")}) is not None
