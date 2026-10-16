"""Home Assistant cameras to Kameraposti (1.7.0; 1.8.1 steelo 2026-10-16 "maalaisjärjellä, pari klikkausta").

The integration describes each reported camera to kameraposti/<id>/anturit/<name>/config with kind
"camera" (Kameraposti lists it for the user to add or pick, and attaches a Shelly Camera whose own id
ends in the same MAC), and the cameras hidden on a save as removed. 1.8.1: every camera in Home
Assistant is reported unless it is under hidden_cameras; nothing an earlier version saved (1.8.0
excluded_cameras, 1.7.0 exported_cameras, 1.6.0 links) limits it. The set is worked out at every
publish, so a camera added later is reported at the next resend. A camera a 1.6.0 option linked
carries its Kameraposti camera id along.
"""

from __future__ import annotations

import json
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
    exported_cameras,
    home_assistant_cameras,
)
from custom_components.kameraposti.const import (
    CONF_CAMERA_VIDEO,
    CONF_CAMERA_VIDEO_SECONDS,
    CONF_CAPTURE_CAMERAS,
    CONF_CAPTURE_VIDEO,
    CONF_CAPTURE_VIDEO_SECONDS,
    CONF_CUSTOMER_ID,
    CONF_EXCLUDED_CAMERAS,
    CONF_EXPORTED_CAMERAS,
    CONF_HIDDEN_CAMERAS,
    CONF_HOST,
    CONF_REMOVED_CAMERAS,
    DOMAIN,
    MAX_EXPORTED_CAMERAS,
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
    """Every camera in Home Assistant reported by default."""
    values: dict[str, object] = {
        "hidden": (),
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


def _camera(hass: HomeAssistant, object_id: str, *, registry: bool = True, state: bool = True) -> str:
    """A camera in Home Assistant: in the entity registry (as integrations add them) and/or a state."""
    entity_id = f"camera.{object_id}"
    if registry:
        config_entry = MockConfigEntry(domain="demo")
        config_entry.add_to_hass(hass)
        registered = er.async_get(hass).async_get_or_create(
            "camera", "demo", object_id, suggested_object_id=object_id, config_entry=config_entry
        )
        assert registered.entity_id == entity_id
    if state:
        hass.states.async_set(entity_id, "idle", {"friendly_name": object_id.capitalize()})
    return entity_id


async def _resync(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    freezer.tick(15 * 60 + 1)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()


# -- settings from the options ----------------------------------------------------------


def test_no_camera_setting_is_read_any_more_and_1_6_links_keep_their_hints() -> None:
    """1.10.0 (steelo 16.10 "tee sitä nyt jo yksinkertaisempi"): no option hides a camera, removes one or
    turns the video off -- 1.8.1's hidden list hid his Shelly when he switched the video on. The 1.6.0
    links still give camera_id hints."""
    settings = camera_settings_from_options(
        {
            CONF_CAPTURE_CAMERAS: {
                "6": _link("camera.talli", video=True, seconds=12),
                "5": _link(SHELLY),
                "7": _link(SHELLY, video=False, seconds=20),
                "x": _link("camera.roska"),
            },
            CONF_EXPORTED_CAMERAS: ["camera.talli"],
            CONF_EXCLUDED_CAMERAS: [SHELLY],
            CONF_HIDDEN_CAMERAS: [SHELLY, "camera.olohuone"],
            CONF_REMOVED_CAMERAS: [SHELLY],
            CONF_CAMERA_VIDEO: False,
            CONF_CAMERA_VIDEO_SECONDS: 30,
        }
    )
    assert settings == CameraSettings(
        hidden=(), removed=(), hints={SHELLY: 5, "camera.talli": 6}, video=True, video_seconds=10
    )
    assert camera_settings_from_options({}) == CameraSettings(
        hidden=(), removed=(), hints={}, video=True, video_seconds=10
    )


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


def test_at_most_50_cameras_are_reported_constant() -> None:
    assert MAX_EXPORTED_CAMERAS == 50


# -- which cameras are reported -----------------------------------------------------------


async def test_every_camera_in_home_assistant_is_found(hass: HomeAssistant) -> None:
    """Registry cameras that are not disabled -- also one not loaded yet -- and state-only cameras."""
    loaded = _camera(hass, "piha")
    not_loaded_yet = _camera(hass, "talli", state=False)
    yaml_camera = _camera(hass, "aitta", registry=False)
    disabled = _camera(hass, "vanha", state=False)
    er.async_get(hass).async_update_entity(disabled, disabled_by=er.RegistryEntryDisabler.USER)
    hass.states.async_set("sensor.kamera", "1")

    assert home_assistant_cameras(hass) == [yaml_camera, loaded, not_loaded_yet]


async def test_every_camera_is_reported_after_an_upgrade(
    hass: HomeAssistant,
) -> None:
    _shelly_device(hass)
    for object_id in ("talli", "olohuone"):
        _camera(hass, object_id)
    every = ("camera.olohuone", SHELLY, "camera.talli")

    # 1.8.0 options with the Shelly on its "must not use" list (steelo 16.10): the Shelly is reported.
    from_1_8_0 = camera_settings_from_options({CONF_EXCLUDED_CAMERAS: [SHELLY], CONF_CAMERA_VIDEO: False})
    assert exported_cameras(hass, from_1_8_0) == every
    # 1.7.0 ticks: no longer an allowlist.
    from_1_7 = camera_settings_from_options({CONF_EXPORTED_CAMERAS: ["camera.talli"]})
    assert exported_cameras(hass, from_1_7) == every
    # 1.6.0 links: every camera, the hint kept.
    from_1_6 = camera_settings_from_options({CONF_CAPTURE_CAMERAS: {"5": _link("camera.talli")}})
    assert exported_cameras(hass, from_1_6) == every
    assert from_1_6.hints == {"camera.talli": 5}
    # Fresh install.
    assert exported_cameras(hass, camera_settings_from_options({})) == every
    # 1.10.0: 1.8.1's hidden list hides nothing either (the Shelly hidden by mistake comes back).
    hidden = camera_settings_from_options({CONF_HIDDEN_CAMERAS: [SHELLY, "camera.olohuone"]})
    assert exported_cameras(hass, hidden) == every


async def test_at_most_50_cameras_are_reported(hass: HomeAssistant) -> None:
    for i in range(55):
        _camera(hass, f"c{i:02d}", registry=False)
    assert exported_cameras(hass, camera_settings_from_options({})) == tuple(
        f"camera.c{i:02d}" for i in range(50)
    )


# -- the exporter ---------------------------------------------------------------------------


async def test_reported_cameras_are_described_and_hidden_ones_removed(hass: HomeAssistant) -> None:
    _shelly_device(hass)
    hass.states.async_set("camera.talli", "idle", {"friendly_name": "Talli"})
    hass.states.async_set("camera.vanha", "idle", {"friendly_name": "Vanha"})
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(
        hass,
        customer_id=CUSTOMER_ID,
        settings=_settings(hidden=("camera.vanha",), removed=("camera.vanha",), hints={SHELLY: 5}),
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
    exporter = KameraportiCameraExporter(hass, customer_id=CUSTOMER_ID, settings=_settings(), publish=publish)

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
        settings=_settings(hidden=("camera.vanha",), removed=("camera.vanha",)),
        publish=publish,
    )
    exporter.async_on_connected()
    # The removal went, the first description failed: nothing after it was tried.
    assert sent == ["camera.vanha"]
    exporter.async_on_connected()
    assert sent == ["camera.vanha", "camera.vanha", "camera.eka", "camera.toka"]


async def test_nothing_to_send_without_cameras(hass: HomeAssistant) -> None:
    published, publish = _recorder()
    for settings in (_settings(), _settings(hidden=(SHELLY,))):
        exporter = KameraportiCameraExporter(
            hass, customer_id=CUSTOMER_ID, settings=settings, publish=publish
        )
        exporter.async_start()
        exporter.async_on_connected()
        exporter.async_stop()
    assert published == []


@pytest.mark.parametrize(
    ("options", "reported"),
    [
        ({}, ["camera.olohuone", "camera.piha"]),
        # 1.10.0: 1.8.1's hidden list hides nothing; 1.8.0's has hidden nothing since 1.8.1.
        ({CONF_HIDDEN_CAMERAS: ["camera.olohuone"]}, ["camera.olohuone", "camera.piha"]),
        ({CONF_EXCLUDED_CAMERAS: ["camera.olohuone"]}, ["camera.olohuone", "camera.piha"]),
    ],
)
async def test_a_camera_added_to_home_assistant_later_is_reported_at_the_resync(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    options: dict[str, object],
    reported: list[str],
) -> None:
    """The set is worked out at every publish, not once at setup."""
    _camera(hass, "piha")
    _camera(hass, "olohuone")
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(
        hass, customer_id=CUSTOMER_ID, settings=camera_settings_from_options(options), publish=publish
    )
    exporter.async_start()
    exporter.async_on_connected()
    assert [payload["entity_id"] for _, payload, _ in published] == reported

    published.clear()
    _camera(hass, "uusi", registry=False)
    await _resync(hass, freezer)
    assert [payload["entity_id"] for _, payload, _ in published] == [*reported, "camera.uusi"]
    exporter.async_stop()


async def test_a_1_7_tick_list_limits_nothing_also_for_a_camera_added_later(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """1.8.1: the 1.7.0 list is no allowlist any more -- every camera is reported."""
    _camera(hass, "talli")
    _camera(hass, "piha")
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(
        hass,
        customer_id=CUSTOMER_ID,
        settings=camera_settings_from_options({CONF_EXPORTED_CAMERAS: ["camera.talli"]}),
        publish=publish,
    )
    exporter.async_start()
    exporter.async_on_connected()
    _camera(hass, "uusi")
    await _resync(hass, freezer)
    assert [payload["entity_id"] for _, payload, _ in published] == [
        "camera.piha",
        "camera.talli",
        "camera.piha",
        "camera.talli",
        "camera.uusi",
    ]
    exporter.async_stop()


async def test_a_removed_camera_that_is_reported_again_is_not_also_described_as_removed(
    hass: HomeAssistant,
) -> None:
    _camera(hass, "piha")
    published, publish = _recorder()
    exporter = KameraportiCameraExporter(
        hass,
        customer_id=CUSTOMER_ID,
        settings=_settings(removed=("camera.piha", "camera.olohuone")),
        publish=publish,
    )
    exporter.publish_all()
    assert [(payload["entity_id"], payload.get("removed", False)) for _, payload, _ in published] == [
        ("camera.olohuone", True),
        ("camera.piha", False),
    ]


async def test_a_1_6_link_sends_its_camera_id_when_every_camera_is_reported(hass: HomeAssistant) -> None:
    """Upgrade from 1.6.0: every camera is reported, the linked one with its camera_id."""
    _shelly_device(hass)
    _camera(hass, "piha")
    published, publish = _recorder()
    settings = camera_settings_from_options({CONF_CAPTURE_CAMERAS: {"5": _link(SHELLY)}})
    exporter = KameraportiCameraExporter(hass, customer_id=CUSTOMER_ID, settings=settings, publish=publish)
    exporter.publish_all()
    assert {payload["entity_id"]: payload.get("camera_id") for _, payload, _ in published} == {
        "camera.piha": None,
        SHELLY: 5,
    }


async def test_coordinator_describes_a_1_6_entrys_linked_cameras_when_connected(hass: HomeAssistant) -> None:
    _shelly_device(hass)
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        # A 1.6.0 entry: the linked camera is reported and its Kameraposti camera id goes along.
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
        assert tuple(coordinator._capture._exported()) == (SHELLY,)
        await coordinator.async_stop()


async def test_coordinator_of_a_fresh_entry_reports_every_camera(hass: HomeAssistant) -> None:
    _camera(hass, "piha")
    _camera(hass, "talli", registry=False)
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
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

        assert [json.loads(c.args[1]) for c in client.publish.call_args_list] == [
            {"kind": "camera", "entity_id": "camera.piha", "name": "Piha", "mac": None},
            {"kind": "camera", "entity_id": "camera.talli", "name": "Talli", "mac": None},
        ]
        # A camera added after setup can be asked for photos without a reload.
        _camera(hass, "uusi")
        assert tuple(coordinator._capture._exported()) == ("camera.piha", "camera.talli", "camera.uusi")
        await coordinator.async_stop()
