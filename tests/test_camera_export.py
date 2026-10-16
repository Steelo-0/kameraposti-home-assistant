"""Home Assistant cameras to Kameraposti (1.7.0, steelo 2026-10-16 "idioottivarma").

The user ticks cameras under "Cameras to Kameraposti"; the integration describes each one to
kameraposti/<id>/anturit/<name>/config with kind "camera" (Kameraposti makes a camera of it and links
a Shelly Camera whose own id ends in the same MAC), and the unticked ones as removed. A 1.6.0 entry's
linked cameras count as ticked until the list is saved, and their Kameraposti camera ids go along.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.const import CONF_ENTITY_ID
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.camera_export import (
    CameraSettings,
    KameraportiCameraExporter,
    camera_mac,
    camera_name,
    camera_settings_from_options,
    removed_after_save,
)
from custom_components.kameraposti.const import (
    CONF_CAMERA_VIDEO,
    CONF_CAMERA_VIDEO_SECONDS,
    CONF_CAPTURE_CAMERAS,
    CONF_CAPTURE_VIDEO,
    CONF_CAPTURE_VIDEO_SECONDS,
    CONF_CUSTOMER_ID,
    CONF_EXPORTED_CAMERAS,
    CONF_HOST,
    CONF_REMOVED_CAMERAS,
    DOMAIN,
)
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.mqtt_client import ConnectionState

CUSTOMER_ID = 3
SHELLY = "camera.shellycamera_6ca61366d281_stream_0"


def _link(entity_id: str, *, video: bool = False, seconds: int = 10) -> dict[str, object]:
    return {CONF_ENTITY_ID: entity_id, CONF_CAPTURE_VIDEO: video, CONF_CAPTURE_VIDEO_SECONDS: seconds}


def _recorder(ok: bool = True) -> tuple[list[tuple[str, dict, bool]], object]:
    published: list[tuple[str, dict, bool]] = []

    def publish(topic: str, payload: str, retain: bool) -> bool:
        if not ok:
            return False
        published.append((topic, json.loads(payload), retain))
        return True

    return published, publish


def _settings(**overrides: object) -> CameraSettings:
    values: dict[str, object] = {
        "exported": (SHELLY,),
        "removed": (),
        "hints": {},
        "video": False,
        "video_seconds": 10,
    }
    values.update(overrides)
    return CameraSettings(**values)  # type: ignore[arg-type]


def _shelly_device(hass: HomeAssistant, *, name: str = "shellycamera-6ca61366d281", mac: bool = True) -> None:
    config_entry = MockConfigEntry(domain="shelly")
    config_entry.add_to_hass(hass)
    device = dr.async_get(hass).async_get_or_create(
        config_entry_id=config_entry.entry_id,
        connections={(dr.CONNECTION_NETWORK_MAC, "6c:a6:13:66:d2:81")} if mac else set(),
        identifiers={("shelly", "camera-1")},
        name=name,
    )
    er.async_get(hass).async_get_or_create(
        "camera",
        "shelly",
        "6ca61366d281-stream-0",
        suggested_object_id="shellycamera_6ca61366d281_stream_0",
        device_id=device.id,
        config_entry=config_entry,
        original_name="Stream 0",
    )
    hass.states.async_set(SHELLY, "idle", {"friendly_name": f"{name} Stream 0"})


# -- settings from the options ----------------------------------------------------------


def test_a_1_6_entry_counts_its_linked_cameras_as_ticked() -> None:
    settings = camera_settings_from_options(
        {
            CONF_CAPTURE_CAMERAS: {
                "6": _link("camera.talli", video=True, seconds=12),
                "5": _link(SHELLY),
                "7": _link(SHELLY, video=True, seconds=20),
                "x": _link("camera.roska"),
            }
        }
    )

    assert settings.exported == (SHELLY, "camera.talli")
    # The lowest Kameraposti camera id linked to each Home Assistant camera.
    assert settings.hints == {SHELLY: 5, "camera.talli": 6}
    assert settings.video is True
    assert settings.video_seconds == 20
    assert settings.removed == ()


def test_the_saved_list_decides_and_junk_is_skipped() -> None:
    settings = camera_settings_from_options(
        {
            CONF_CAPTURE_CAMERAS: {"5": _link(SHELLY, video=True), "6": _link("camera.talli")},
            CONF_EXPORTED_CAMERAS: ["camera.talli", "sensor.ovi", "camera.Iso", 7, "camera.talli"],
            CONF_REMOVED_CAMERAS: [SHELLY, "camera.talli", "camera.vanha"],
            CONF_CAMERA_VIDEO: False,
            CONF_CAMERA_VIDEO_SECONDS: 99,
        }
    )

    assert settings.exported == ("camera.talli",)
    # A hint only for a ticked camera; a ticked camera is never also "removed".
    assert settings.hints == {"camera.talli": 6}
    assert settings.removed == (SHELLY, "camera.vanha")
    assert settings.video is False
    assert settings.video_seconds == 30
    assert camera_settings_from_options({}).exported == ()


def test_unticked_cameras_go_to_the_removal_list_and_reticked_ones_leave_it() -> None:
    previous = _settings(exported=(SHELLY, "camera.talli"), removed=("camera.vanha",))

    assert removed_after_save(previous, ["camera.talli"]) == ["camera.vanha", SHELLY]
    assert removed_after_save(_settings(exported=(), removed=(SHELLY,)), [SHELLY]) == []
    many = _settings(exported=(), removed=tuple(f"camera.c{i}" for i in range(20)))
    assert removed_after_save(many, []) == [f"camera.c{i}" for i in range(20)]
    more = _settings(exported=("camera.uusi",), removed=tuple(f"camera.c{i}" for i in range(20)))
    assert removed_after_save(more, [])[-1] == "camera.uusi"
    assert len(removed_after_save(more, [])) == 20


# -- what a camera is called and its MAC -------------------------------------------------


async def test_name_and_mac_come_from_the_camera_device(hass: HomeAssistant) -> None:
    _shelly_device(hass)

    assert camera_name(hass, SHELLY) == "shellycamera-6ca61366d281"
    assert camera_mac(hass, SHELLY) == "6ca61366d281"

    # The user's own device name wins; a named entity keeps its own (friendly) name.
    device_id = er.async_get(hass).async_get(SHELLY).device_id
    dr.async_get(hass).async_update_device(device_id, name_by_user="Keittiön kamera")
    assert camera_name(hass, SHELLY) == "Keittiön kamera"
    er.async_get(hass).async_update_entity(SHELLY, name="Eteinen")
    hass.states.async_set(SHELLY, "idle", {"friendly_name": "Eteinen\x07"})
    assert camera_name(hass, SHELLY) == "Eteinen"


async def test_mac_falls_back_to_the_device_name(hass: HomeAssistant) -> None:
    _shelly_device(hass, mac=False)
    assert camera_mac(hass, SHELLY) == "6ca61366d281"

    hass.states.async_set("camera.ilman_laitetta", "idle", {})
    assert camera_mac(hass, "camera.ilman_laitetta") is None
    assert camera_name(hass, "camera.ilman_laitetta") == "ilman_laitetta"


async def test_no_mac_when_the_device_has_none(hass: HomeAssistant) -> None:
    _shelly_device(hass, name="Kuistin kamera", mac=False)
    assert camera_mac(hass, SHELLY) is None


async def test_the_mac_connection_wins_over_the_name(hass: HomeAssistant) -> None:
    """Audit L-7: a device named by its user still gives its MAC from the registry connection."""
    _shelly_device(hass, name="Kuistin kamera", mac=True)
    assert camera_mac(hass, SHELLY) == "6ca61366d281"


def test_at_most_20_ticked_cameras_are_read() -> None:
    settings = camera_settings_from_options({CONF_EXPORTED_CAMERAS: [f"camera.c{i}" for i in range(25)]})
    assert settings.exported == tuple(f"camera.c{i}" for i in range(20))


# -- the exporter ---------------------------------------------------------------------------


async def test_ticked_cameras_are_described_and_unticked_ones_removed(hass: HomeAssistant) -> None:
    _shelly_device(hass)
    hass.states.async_set("camera.talli", "idle", {"friendly_name": "Talli"})
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(
        hass,
        customer_id=CUSTOMER_ID,
        settings=_settings(
            exported=(SHELLY, "camera.talli", "camera.poistettu_hasta"),
            removed=("camera.vanha",),
            hints={SHELLY: 5},
        ),
        publish=publish,
    )

    exporter.publish_all()

    assert published == [
        (
            "kameraposti/3/anturit/camera.vanha/config",
            {"kind": "camera", "entity_id": "camera.vanha", "removed": True},
            False,
        ),
        (
            f"kameraposti/3/anturit/{SHELLY}/config",
            {
                "kind": "camera",
                "entity_id": SHELLY,
                "name": "shellycamera-6ca61366d281",
                "mac": "6ca61366d281",
                "camera_id": 5,
            },
            False,
        ),
        (
            "kameraposti/3/anturit/camera.talli/config",
            {"kind": "camera", "entity_id": "camera.talli", "name": "Talli", "mac": None},
            False,
        ),
    ]


async def test_a_long_entity_id_keeps_its_full_id_in_the_payload(hass: HomeAssistant) -> None:
    long_id = "camera." + "a" * 80
    hass.states.async_set(long_id, "idle", {})
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(
        hass, customer_id=CUSTOMER_ID, settings=_settings(exported=(long_id,)), publish=publish
    )

    exporter.publish_all()

    [(topic, payload, _)] = published
    assert len(topic.split("/")[3]) <= 64
    assert payload["entity_id"] == long_id


async def test_nothing_is_marked_sent_while_disconnected(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    hass.states.async_set(SHELLY, "idle", {})
    sent: list[str] = []
    connected = False

    def publish(topic: str, payload: str, retain: bool) -> bool:
        if connected:
            sent.append(topic)
        return connected

    exporter = KameraportiCameraExporter(hass, customer_id=CUSTOMER_ID, settings=_settings(), publish=publish)
    exporter.async_on_connected()
    assert sent == []
    connected = True
    # Nothing went out before, so the next connection sends at once.
    exporter.async_on_connected()
    assert sent == [f"kameraposti/3/anturit/{SHELLY}/config"]


async def test_a_reconnect_storm_sends_at_most_once_a_minute_and_15_minute_resend(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    hass.states.async_set(SHELLY, "idle", {})
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(hass, customer_id=CUSTOMER_ID, settings=_settings(), publish=publish)
    exporter.async_start()

    exporter.async_on_connected()
    freezer.tick(5)
    exporter.async_on_connected()
    exporter.async_on_connected()
    assert len(published) == 1

    freezer.tick(56)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert len(published) == 2

    freezer.tick(15 * 60)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert len(published) == 3

    exporter.async_stop()
    freezer.tick(16 * 60)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert len(published) == 3


async def test_a_failed_publish_stops_the_round_and_the_next_connection_resends(hass: HomeAssistant) -> None:
    hass.states.async_set("camera.eka", "idle", {})
    hass.states.async_set("camera.toka", "idle", {})
    sent: list[str] = []
    accept = [True, False]

    def publish(topic: str, payload: str, retain: bool) -> bool:
        ok = accept.pop(0) if accept else True
        if ok:
            sent.append(json.loads(payload)["entity_id"])
        return ok

    exporter = KameraportiCameraExporter(
        hass,
        customer_id=CUSTOMER_ID,
        settings=_settings(exported=("camera.eka", "camera.toka"), removed=("camera.vanha",)),
        publish=publish,
    )
    exporter.async_on_connected()
    # The removal went, the first description failed: nothing after it was tried.
    assert sent == ["camera.vanha"]
    exporter.async_on_connected()
    assert sent == ["camera.vanha", "camera.vanha", "camera.eka", "camera.toka"]


async def test_a_ticked_camera_that_no_longer_exists_is_named_once(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """Audit L-5: renamed or removed in Home Assistant -- not described, and the log says where to fix it."""
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(
        hass, customer_id=CUSTOMER_ID, settings=_settings(exported=("camera.vanha",)), publish=publish
    )
    exporter.async_start()
    with caplog.at_level(logging.WARNING):
        # Right after start the camera may just not be loaded yet: no warning.
        exporter.async_on_connected()
        for _ in range(2):
            freezer.tick(15 * 60 + 1)
            async_fire_time_changed(hass)
            await hass.async_block_till_done()
    exporter.async_stop()

    assert published == []
    warnings = [r.getMessage() for r in caplog.records if r.name.startswith("custom_components.kameraposti")]
    assert warnings == [
        "Kameraposti: camera.vanha is ticked under Configure -> Cameras to Kameraposti but no longer exists "
        "in Home Assistant; tick the camera's current entity there"
    ]


async def test_nothing_to_send_without_cameras(hass: HomeAssistant) -> None:
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(
        hass, customer_id=CUSTOMER_ID, settings=_settings(exported=()), publish=publish
    )
    exporter.async_start()
    exporter.async_on_connected()
    assert published == []
    exporter.async_stop()


async def test_coordinator_describes_ticked_cameras_when_connected(hass: HomeAssistant) -> None:
    _shelly_device(hass)
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        # A 1.6.0 entry: the linked camera is ticked and its Kameraposti camera id goes along.
        options={CONF_CAPTURE_CAMERAS: {"1": _link(SHELLY)}},
        version=2,
    )
    entry.add_to_hass(hass)
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        client = MagicMock()
        client.async_start = AsyncMock()
        client.async_stop = AsyncMock()
        client.publish.return_value = True
        mock_cls.return_value = client
        coordinator = KameraportiCoordinator(
            hass, entry, host="cam.steels.me", customer_id=CUSTOMER_ID, username="kp-3", password="x"
        )
        await coordinator.async_start()
        coordinator._handle_state_change(ConnectionState.CONNECTED)
        await hass.async_block_till_done()

        [(topic, payload, retain)] = [c.args for c in client.publish.call_args_list]
        assert topic == f"kameraposti/3/anturit/{SHELLY}/config"
        assert json.loads(payload) == {
            "kind": "camera",
            "entity_id": SHELLY,
            "name": "shellycamera-6ca61366d281",
            "mac": "6ca61366d281",
            "camera_id": 1,
        }
        assert retain is False
        assert coordinator._capture._exported == frozenset({SHELLY})
        await coordinator.async_stop()
