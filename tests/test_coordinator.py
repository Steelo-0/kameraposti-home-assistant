"""Coordinator-level tests (contract section 13: A, B, C + device/entity signals).

Calls coordinator._handle_message directly -- this is exactly what
KameraportiMqttClient invokes (via call_soon_threadsafe) once a message
arrives, so testing it directly exercises the real parse -> dedup ->
apply -> fire pipeline without needing a live or faked MQTT connection.
"""

from __future__ import annotations

import json

import pytest
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, DOMAIN, EVENT_DETECTION
from custom_components.kameraposti.coordinator import KameraportiCoordinator

CUSTOMER_ID = 3
CAMERA_ID = 16


@pytest.fixture
def coordinator(hass: HomeAssistant) -> KameraportiCoordinator:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "rk-3-abc", CONF_PASSWORD: "secret"},
    )
    entry.add_to_hass(hass)
    return KameraportiCoordinator(hass, entry, customer_id=CUSTOMER_ID, username="rk-3-abc", password="secret")


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


def _topic(camera_id: int = CAMERA_ID, customer_id: int = CUSTOMER_ID) -> str:
    return f"customers/{customer_id}/detections/{camera_id}"


async def test_valid_detection_updates_state_and_fires_event_once(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    """A. Valid detection: entity state updates, event fires exactly once."""
    events = []
    hass.bus.async_listen(EVENT_DETECTION, events.append)

    coordinator._handle_message(_topic(), _payload())
    await hass.async_block_till_done()

    assert len(events) == 1
    assert events[0].data["label"] == "animal"
    assert events[0].data["camera_id"] == CAMERA_ID
    assert events[0].data["event_id"] == "01A"
    assert CAMERA_ID in coordinator.cameras
    assert coordinator.cameras[CAMERA_ID].label == "animal"
    assert coordinator.cameras[CAMERA_ID].confidence == 0.9


async def test_duplicate_event_id_is_ignored(hass: HomeAssistant, coordinator: KameraportiCoordinator) -> None:
    """B. Dedup: first processed, duplicate ignored -- no second event, no state change."""
    events = []
    hass.bus.async_listen(EVENT_DETECTION, events.append)

    coordinator._handle_message(_topic(), _payload(event_id="01A", label="animal"))
    coordinator._handle_message(_topic(), _payload(event_id="01A", label="person"))
    await hass.async_block_till_done()

    assert len(events) == 1
    assert coordinator.cameras[CAMERA_ID].label == "animal"


async def test_same_content_different_event_id_both_processed(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    """C. Same content, different event_id -- must NOT be deduplicated."""
    events = []
    hass.bus.async_listen(EVENT_DETECTION, events.append)

    coordinator._handle_message(_topic(), _payload(event_id="01A"))
    coordinator._handle_message(_topic(), _payload(event_id="01B"))
    await hass.async_block_till_done()

    assert len(events) == 2
    assert {e.data["event_id"] for e in events} == {"01A", "01B"}


async def test_wrong_tenant_topic_is_ignored(hass: HomeAssistant, coordinator: KameraportiCoordinator) -> None:
    """D. Wrong tenant topic -- ignore, never raise."""
    events = []
    hass.bus.async_listen(EVENT_DETECTION, events.append)

    coordinator._handle_message(_topic(customer_id=4), _payload())
    await hass.async_block_till_done()

    assert events == []
    assert coordinator.cameras == {}


async def test_camera_id_mismatch_is_ignored(hass: HomeAssistant, coordinator: KameraportiCoordinator) -> None:
    """E. camera_id mismatch between topic and payload -- ignore."""
    events = []
    hass.bus.async_listen(EVENT_DETECTION, events.append)

    coordinator._handle_message(_topic(camera_id=16), _payload(camera_id=17))
    await hass.async_block_till_done()

    assert events == []


async def test_invalid_json_does_not_raise(hass: HomeAssistant, coordinator: KameraportiCoordinator) -> None:
    """F. Invalid JSON -- ignore, no crash."""
    events = []
    hass.bus.async_listen(EVENT_DETECTION, events.append)

    coordinator._handle_message(_topic(), b"{not valid json")
    await hass.async_block_till_done()

    assert events == []


async def test_new_camera_sends_new_camera_signal_exactly_once(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    received: list[int] = []
    async_dispatcher_connect(hass, coordinator.signal_new_camera, received.append)

    coordinator._handle_message(_topic(), _payload(event_id="01A"))
    await hass.async_block_till_done()

    assert received == [CAMERA_ID]


async def test_second_detection_sends_update_signal_not_new_camera_signal(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    new_camera_signals: list[int] = []
    update_signals: list[bool] = []
    async_dispatcher_connect(hass, coordinator.signal_new_camera, new_camera_signals.append)

    coordinator._handle_message(_topic(), _payload(event_id="01A"))
    await hass.async_block_till_done()

    async_dispatcher_connect(hass, coordinator.signal_camera_update(CAMERA_ID), lambda: update_signals.append(True))
    coordinator._handle_message(_topic(), _payload(event_id="01B"))
    await hass.async_block_till_done()

    assert new_camera_signals == [CAMERA_ID]
    assert update_signals == [True]
