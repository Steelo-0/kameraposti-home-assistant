"""The "Latest photo" image entity (1.5.0, customers/<id>/cameras/<camera>/latest).

One image entity per camera, on the camera's device. Its image_url is the
signed URL from the retained message and image_last_updated the photo's
captured_at; Home Assistant fetches the image only when the photo changes
(no polling). An empty retained message (no photo) or an expired URL makes
the entity unavailable until the next message.

The MQTT client is mocked out; image fetches go through respx.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx
from freezegun.api import FrozenDateTimeFactory
from homeassistant.components.image import async_get_image
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.httpx_client import get_async_client
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, DOMAIN, EVENT_DETECTION
from custom_components.kameraposti.coordinator import KameraportiCoordinator

CUSTOMER_ID = 3
CAMERA_ID = 12
NOW = "2026-10-07T05:00:00+00:00"
URL_1 = "https://cam.steels.me/riistakamera/ha/kuva/5501?expires=1&signature=a1"
URL_2 = "https://cam.steels.me/riistakamera/ha/kuva/5502?expires=2&signature=b2"
JPEG_1 = b"\xff\xd8\xff\xe0photo-5501"
JPEG_2 = b"\xff\xd8\xff\xe0photo-5502"
ROSTER_TOPIC = f"customers/{CUSTOMER_ID}/cameras"
LATEST_TOPIC = f"customers/{CUSTOMER_ID}/cameras/{CAMERA_ID}/latest"
UNIQUE_ID = f"{DOMAIN}:{CUSTOMER_ID}:{CAMERA_ID}:latest_photo"


def _latest(**overrides: object) -> bytes:
    payload: dict[str, object] = {
        "schema_version": 1,
        "camera_id": CAMERA_ID,
        "photo_id": 5501,
        "captured_at": "2026-10-07T04:31:10Z",
        "received_at": "2026-10-07T04:31:40Z",
        "is_video": False,
        "url": URL_1,
        "expires_at": (dt_util.utcnow() + timedelta(days=30)).isoformat(),
        "detection": {"label": "hirvi", "confidence": 0.93},
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


def _detection() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event_id": "01A",
            "camera_id": CAMERA_ID,
            "label": "animal",
            "confidence": 0.9,
            "timestamp": "2026-10-07T04:31:10+00:00",
        }
    ).encode()


# Each roster defaults to a newer generated_at than the last, as the server's
# publisher does on every change; only a strictly newer roster removes cameras.
_GENERATED_AT = itertools.count()


def _roster(*cameras: tuple[int, str], generated_at: str | None = None) -> bytes:
    if generated_at is None:
        generated_at = (
            datetime(2026, 10, 7, 4, tzinfo=UTC) + timedelta(seconds=next(_GENERATED_AT))
        ).isoformat()
    return json.dumps(
        {
            "schema_version": 1,
            "generated_at": generated_at,
            "cameras": [{"camera_id": camera_id, "name": name} for camera_id, name in cameras],
        }
    ).encode()


@pytest.fixture
def mock_mqtt_client() -> AsyncGenerator[None]:
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        instance = mock_cls.return_value
        instance.async_start = AsyncMock()
        instance.async_stop = AsyncMock()
        yield


@pytest.fixture
async def coordinator(
    hass: HomeAssistant, mock_mqtt_client: None, freezer: FrozenDateTimeFactory
) -> KameraportiCoordinator:
    freezer.move_to(NOW)
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_CUSTOMER_ID: CUSTOMER_ID, CONF_USERNAME: "kp-3", CONF_PASSWORD: "secret"},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return hass.data[DOMAIN][entry.entry_id]


async def _send(hass: HomeAssistant, coordinator: KameraportiCoordinator, topic: str, payload: bytes) -> None:
    coordinator._handle_message(topic, payload)
    await hass.async_block_till_done()


def _entity_id(hass: HomeAssistant) -> str | None:
    return er.async_get(hass).async_get_entity_id("image", DOMAIN, UNIQUE_ID)


async def test_latest_photo_entity_joins_the_camera_device_with_url_time_and_attributes(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())

    entity_id = _entity_id(hass)
    assert entity_id == "image.pihakamera_latest_photo"
    registry_entry = er.async_get(hass).async_get(entity_id)
    device = dr.async_get(hass).async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:{CAMERA_ID}")})
    assert device is not None
    assert registry_entry.device_id == device.id
    sensor_entry = er.async_get(hass).async_get("sensor.pihakamera_last_detection")
    assert sensor_entry.device_id == device.id

    state = hass.states.get(entity_id)
    assert state.state == "2026-10-07T04:31:10+00:00"  # image_last_updated = captured_at
    assert state.attributes["friendly_name"] == "Pihakamera Latest photo"
    assert state.attributes["label"] == "hirvi"
    assert state.attributes["confidence"] == 0.93
    assert state.attributes["is_video"] is False
    assert state.attributes["captured_at"] == "2026-10-07T04:31:10+00:00"
    assert state.attributes["entity_picture"].startswith(f"/api/image_proxy/{entity_id}?token=")
    # The signed URL is a capability for the photo: never an attribute.
    assert all(URL_1 not in str(value) for value in state.attributes.values())

    entity = hass.data["image"].get_entity(entity_id)
    assert entity.image_url == URL_1
    assert entity._client is get_async_client(hass, verify_ssl=True)


async def test_no_latest_photo_entity_until_the_cameras_first_photo(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    """A service that publishes no photos (kameraposti.fi today) must not leave a
    permanently unavailable entity on every camera: the entity comes with the first
    valid photo, and an empty retained message before that creates nothing."""
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    await _send(hass, coordinator, f"customers/{CUSTOMER_ID}/detections/{CAMERA_ID}", _detection())
    await _send(hass, coordinator, LATEST_TOPIC, b"")
    await _send(hass, coordinator, LATEST_TOPIC, b'{"schema_version": 2}')

    assert _entity_id(hass) is None
    assert hass.states.get("sensor.pihakamera_last_detection").state == "animal"

    await _send(hass, coordinator, LATEST_TOPIC, _latest())

    assert _entity_id(hass) == "image.pihakamera_latest_photo"
    assert hass.states.get("image.pihakamera_latest_photo").state == "2026-10-07T04:31:10+00:00"


async def test_only_cameras_that_had_a_photo_get_the_entity_and_it_comes_back_after_a_reload(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    """After a reload/restart the entity is recreated when the retained photo arrives
    again; a camera that never had a photo has no entity (or registry entry) at all."""
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera"), (7, "Navetta")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())
    navetta_unique_id = f"{DOMAIN}:{CUSTOMER_ID}:7:latest_photo"
    assert er.async_get(hass).async_get_entity_id("image", DOMAIN, navetta_unique_id) is None

    entry = coordinator.entry
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    reloaded: KameraportiCoordinator = hass.data[DOMAIN][entry.entry_id]
    entity_id = "image.pihakamera_latest_photo"

    # Retained messages arrive again on subscribe: roster first, then the photo.
    await _send(hass, reloaded, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera"), (7, "Navetta")))
    assert hass.data["image"].get_entity(entity_id) is None
    await _send(hass, reloaded, LATEST_TOPIC, _latest())

    assert _entity_id(hass) == entity_id  # same registry entry, same entity id
    assert hass.data["image"].get_entity(entity_id) is not None
    assert hass.states.get(entity_id).state == "2026-10-07T04:31:10+00:00"
    assert er.async_get(hass).async_get_entity_id("image", DOMAIN, navetta_unique_id) is None


async def test_photo_entity_is_kept_when_the_photo_goes_away(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())
    await _send(hass, coordinator, LATEST_TOPIC, b"")

    assert hass.states.get("image.pihakamera_latest_photo").state == STATE_UNAVAILABLE

    # Created once: a later photo updates the same entity, no second one.
    await _send(hass, coordinator, LATEST_TOPIC, _latest(photo_id=5502, url=URL_2))
    image_entities = [e for e in er.async_get(hass).entities.values() if e.domain == "image"]
    assert [e.entity_id for e in image_entities] == ["image.pihakamera_latest_photo"]
    assert hass.states.get("image.pihakamera_latest_photo").state == "2026-10-07T04:31:10+00:00"


@respx.mock
async def test_image_is_fetched_once_per_photo_and_again_when_the_photo_changes(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    route_1 = respx.get(URL_1).respond(200, content=JPEG_1, headers={"Content-Type": "image/jpeg"})
    route_2 = respx.get(URL_2).respond(200, content=JPEG_2, headers={"Content-Type": "image/jpeg"})
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())
    entity_id = "image.pihakamera_latest_photo"

    assert (await async_get_image(hass, entity_id)).content == JPEG_1
    assert (await async_get_image(hass, entity_id)).content == JPEG_1
    assert route_1.call_count == 1  # cached, no polling

    # The same message again (QoS 1 redelivery, a reconnect's retained copy): nothing changes.
    token_before = hass.states.get(entity_id).attributes["access_token"]
    await _send(hass, coordinator, LATEST_TOPIC, _latest())
    assert (await async_get_image(hass, entity_id)).content == JPEG_1
    assert route_1.call_count == 1
    assert hass.states.get(entity_id).attributes["access_token"] == token_before

    # A new photo (taken in the same second -- burst mode): new URL, refetched.
    await _send(hass, coordinator, LATEST_TOPIC, _latest(photo_id=5502, url=URL_2, detection=None))
    state = hass.states.get(entity_id)
    assert state.state == "2026-10-07T04:31:10+00:00"
    assert state.attributes["label"] is None
    assert state.attributes["confidence"] is None
    # The picture link changes even though captured_at did not, so the frontend reloads it.
    assert state.attributes["access_token"] != token_before
    assert (await async_get_image(hass, entity_id)).content == JPEG_2
    assert route_2.call_count == 1


@respx.mock
async def test_new_detection_for_the_same_photo_updates_attributes_without_refetching(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    route = respx.get(URL_1).respond(200, content=JPEG_1, headers={"Content-Type": "image/jpeg"})
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest(detection=None))
    entity_id = "image.pihakamera_latest_photo"
    await async_get_image(hass, entity_id)

    await _send(hass, coordinator, LATEST_TOPIC, _latest(detection={"label": "kettu", "confidence": 0.71}))

    state = hass.states.get(entity_id)
    assert state.attributes["label"] == "kettu"
    assert state.attributes["confidence"] == 0.71
    assert (await async_get_image(hass, entity_id)).content == JPEG_1
    assert route.call_count == 1


@respx.mock
async def test_new_signed_url_for_the_same_photo_is_refetched(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    """The nightly re-signing changes only the URL: Home Assistant must use the new one."""
    resigned = URL_1.replace("signature=a1", "signature=a1-renewed")
    respx.get(URL_1).respond(200, content=JPEG_1, headers={"Content-Type": "image/jpeg"})
    route = respx.get(resigned).respond(200, content=JPEG_1, headers={"Content-Type": "image/jpeg"})
    await _send(hass, coordinator, LATEST_TOPIC, _latest())
    entity_id = _entity_id(hass)
    await async_get_image(hass, entity_id)

    await _send(hass, coordinator, LATEST_TOPIC, _latest(url=resigned))

    assert hass.data["image"].get_entity(entity_id).image_url == resigned
    await async_get_image(hass, entity_id)
    assert route.call_count == 1


async def test_empty_retained_message_makes_the_entity_unavailable_until_the_next_photo(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())
    entity_id = "image.pihakamera_latest_photo"

    await _send(hass, coordinator, LATEST_TOPIC, b"")
    assert hass.states.get(entity_id).state == STATE_UNAVAILABLE
    with pytest.raises(HomeAssistantError):
        await async_get_image(hass, entity_id)

    await _send(hass, coordinator, LATEST_TOPIC, _latest(photo_id=5502, url=URL_2))
    assert hass.states.get(entity_id).state == "2026-10-07T04:31:10+00:00"
    assert hass.data["image"].get_entity(entity_id).image_url == URL_2


@respx.mock
async def test_expired_url_makes_the_entity_unavailable_and_is_never_fetched(
    hass: HomeAssistant, coordinator: KameraportiCoordinator, freezer: FrozenDateTimeFactory
) -> None:
    route = respx.get(URL_1).respond(403)
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    entity_id = "image.pihakamera_latest_photo"

    # Already expired when it arrives.
    await _send(hass, coordinator, LATEST_TOPIC, _latest(expires_at="2026-10-07T04:59:59Z"))
    assert hass.states.get(entity_id).state == STATE_UNAVAILABLE
    with pytest.raises(HomeAssistantError):
        await async_get_image(hass, entity_id)
    assert route.call_count == 0

    # Valid for one more minute: available now, unavailable right after it has passed --
    # by the entity's own timer, well before the image component's 5-minute token
    # refresh would rewrite the state anyway.
    await _send(hass, coordinator, LATEST_TOPIC, _latest(expires_at="2026-10-07T05:01:00Z"))
    assert hass.states.get(entity_id).state == "2026-10-07T04:31:10+00:00"
    freezer.move_to("2026-10-07T05:01:01+00:00")
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert hass.states.get(entity_id).state == STATE_UNAVAILABLE

    # The re-signed URL brings it back.
    await _send(hass, coordinator, LATEST_TOPIC, _latest(expires_at="2026-11-06T06:00:00Z", url=URL_2))
    assert hass.states.get(entity_id).state == "2026-10-07T04:31:10+00:00"


@pytest.mark.parametrize(
    "payload",
    [b"{not json", b'{"schema_version": 2}', b'{"schema_version": 1, "camera_id": 12}'],
)
async def test_invalid_latest_photo_message_keeps_the_current_photo(
    hass: HomeAssistant, coordinator: KameraportiCoordinator, payload: bytes
) -> None:
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())

    await _send(hass, coordinator, LATEST_TOPIC, payload)

    entity_id = "image.pihakamera_latest_photo"
    assert hass.states.get(entity_id).state == "2026-10-07T04:31:10+00:00"
    assert hass.data["image"].get_entity(entity_id).image_url == URL_1


async def test_latest_photo_creates_the_camera_when_there_is_no_roster(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    await _send(hass, coordinator, LATEST_TOPIC, _latest())

    device = dr.async_get(hass).async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:{CAMERA_ID}")})
    assert device is not None
    assert device.name == f"Riistakamera {CAMERA_ID}"
    assert (
        hass.states.get(f"image.riistakamera_{CAMERA_ID}_latest_photo").state == "2026-10-07T04:31:10+00:00"
    )
    assert hass.states.get(f"sensor.riistakamera_{CAMERA_ID}_last_detection").state == "unknown"


async def test_empty_latest_photo_for_an_unknown_camera_creates_nothing(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    await _send(hass, coordinator, LATEST_TOPIC, b"")

    assert coordinator.cameras == {}
    assert dr.async_get(hass).async_get_device(identifiers={(DOMAIN, f"{CUSTOMER_ID}:{CAMERA_ID}")}) is None


async def test_photo_of_a_camera_not_on_the_roster_waits_for_the_roster(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    """With a roster, the roster decides which cameras exist -- a photo never resurrects
    a removed camera, but a photo that arrives just before the roster lists its camera
    is shown as soon as it does."""
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((7, "Navetta")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())

    assert CAMERA_ID not in coordinator.cameras
    assert _entity_id(hass) is None

    await _send(hass, coordinator, ROSTER_TOPIC, _roster((7, "Navetta"), (CAMERA_ID, "Pihakamera")))

    assert hass.states.get("image.pihakamera_latest_photo").state == "2026-10-07T04:31:10+00:00"


async def test_camera_removed_from_the_roster_loses_its_latest_photo_entity(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera"), (7, "Navetta")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())
    assert _entity_id(hass) is not None

    await _send(hass, coordinator, ROSTER_TOPIC, _roster((7, "Navetta")))

    assert _entity_id(hass) is None
    assert hass.states.get("image.pihakamera_latest_photo") is None


async def test_detections_keep_their_sensors_and_event_next_to_the_photo(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    events = []
    hass.bus.async_listen(EVENT_DETECTION, events.append)
    await _send(hass, coordinator, ROSTER_TOPIC, _roster((CAMERA_ID, "Pihakamera")))
    await _send(hass, coordinator, LATEST_TOPIC, _latest())

    detection = {
        "schema_version": 1,
        "event_id": "01A",
        "camera_id": CAMERA_ID,
        "label": "animal",
        "confidence": 0.9,
        "timestamp": "2026-10-07T04:31:10+00:00",
    }
    await _send(
        hass, coordinator, f"customers/{CUSTOMER_ID}/detections/{CAMERA_ID}", json.dumps(detection).encode()
    )

    assert hass.states.get("sensor.pihakamera_last_detection").state == "animal"
    assert [e.data["event_id"] for e in events] == ["01A"]
    # A detection does not touch the photo.
    assert hass.states.get("image.pihakamera_latest_photo").attributes["label"] == "hirvi"


async def test_image_fetch_errors_do_not_break_the_entity(
    hass: HomeAssistant, coordinator: KameraportiCoordinator
) -> None:
    await _send(hass, coordinator, LATEST_TOPIC, _latest())
    entity_id = _entity_id(hass)

    with respx.mock:
        respx.get(URL_1).mock(side_effect=httpx.ConnectError("down"))
        with pytest.raises(HomeAssistantError):
            await async_get_image(hass, entity_id)

    assert hass.states.get(entity_id).state == "2026-10-07T04:31:10+00:00"
