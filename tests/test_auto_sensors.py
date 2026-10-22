"""1.16.0 (steelo 2026-10-21 "Hälytysanturit lähtevät itsestään"): with no saved selection, the alarm
sensors (smoke, gas, leak, door, window, motion) are sent by themselves; temperatures and the rest are chosen
by hand."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from homeassistant.const import CONF_HOST, EVENT_HOMEASSISTANT_STARTED
from homeassistant.core import CoreState, HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.kameraposti.auto_sensors import RELOAD_DELAY_SECONDS, auto_alarm_entities
from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_EXPORTED_ENTITIES, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator

CUSTOMER_ID = 3


def _set(hass: HomeAssistant, entity_id: str, device_class: str | None, unit: str | None = None) -> None:
    attrs: dict[str, str] = {"friendly_name": entity_id.split(".", 1)[1]}
    if device_class is not None:
        attrs["device_class"] = device_class
    if unit is not None:
        attrs["unit_of_measurement"] = unit
    hass.states.async_set(entity_id, "off" if entity_id.startswith("binary_sensor.") else "21", attrs)


def _home(hass: HomeAssistant) -> None:
    _set(hass, "binary_sensor.eteinen_liike", "motion")
    _set(hass, "binary_sensor.ulko_ovi", "door")
    _set(hass, "binary_sensor.keittio_vuoto", "moisture")
    _set(hass, "binary_sensor.olohuone_ikkuna", "window")
    _set(hass, "binary_sensor.palovaroitin", "smoke")
    _set(hass, "binary_sensor.kaasu", "gas")
    _set(hass, "sensor.co_mittari", "carbon_monoxide", "ppm")
    # Not alarm sensors: chosen by hand.
    _set(hass, "sensor.olohuone_lampo", "temperature", "°C")
    _set(hass, "sensor.olohuone_co2", "carbon_dioxide", "ppm")
    _set(hass, "binary_sensor.tuntematon", None)


def _entry(hass: HomeAssistant, options: dict | None = None) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        options=options or {},
        version=2,
    )
    entry.add_to_hass(hass)
    return entry


def _coordinator(hass: HomeAssistant, entry: MockConfigEntry) -> KameraportiCoordinator:
    client = MagicMock()
    client.async_start = AsyncMock()
    client.async_stop = AsyncMock()
    client.publish.return_value = True
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", return_value=client):
        return KameraportiCoordinator(
            hass, entry, host="cam.steels.me", customer_id=CUSTOMER_ID, username="kp-3", password="x"
        )


ALARMS_IN_ORDER = [
    "binary_sensor.palovaroitin",
    "binary_sensor.kaasu",
    "sensor.co_mittari",
    "binary_sensor.keittio_vuoto",
    "binary_sensor.ulko_ovi",
    "binary_sensor.olohuone_ikkuna",
    "binary_sensor.eteinen_liike",
]


async def test_the_alarm_sensors_are_chosen_most_important_first(hass: HomeAssistant) -> None:
    _home(hass)
    registry = er.async_get(hass)
    # Never: a Frigate sensor, this integration's own, a diagnostic one, a disabled or hidden one.
    for platform, object_id, extra in (
        ("frigate", "piha_motion", {}),
        (DOMAIN, "turva_ovi", {}),
        ("zwave_js", "ovi_tamper", {"entity_category": er.EntityCategory.DIAGNOSTIC}),
        ("zwave_js", "piilo_ovi", {"hidden_by": er.RegistryEntryHider.USER}),
    ):
        entry = registry.async_get_or_create(
            "binary_sensor", platform, object_id, suggested_object_id=object_id, **extra
        )
        _set(hass, entry.entity_id, "door" if "ovi" in object_id else "motion")

    assert auto_alarm_entities(hass) == ALARMS_IN_ORDER


async def test_without_a_saved_selection_the_alarm_sensors_are_sent(hass: HomeAssistant) -> None:
    _home(hass)
    coordinator = _coordinator(hass, _entry(hass))

    assert coordinator._exporter._entity_ids == ALARMS_IN_ORDER
    # Still no saved selection: no list is sent, so Kameraposti drops nothing (1.14.1 H-H1).
    assert coordinator._exporter._selection_saved is False


async def test_a_saved_selection_is_kept_even_when_empty(hass: HomeAssistant) -> None:
    _home(hass)

    assert _coordinator(hass, _entry(hass, {CONF_EXPORTED_ENTITIES: []}))._exporter._entity_ids == []
    chosen = ["sensor.olohuone_lampo"]
    assert _coordinator(hass, _entry(hass, {CONF_EXPORTED_ENTITIES: chosen}))._exporter._entity_ids == chosen


async def test_a_sensor_added_later_reloads_the_entry_once(hass: HomeAssistant) -> None:
    _home(hass)
    entry = _entry(hass)
    coordinator = _coordinator(hass, entry)
    with patch.object(hass.config_entries, "async_schedule_reload") as reload:
        await coordinator.async_start()
        await hass.async_block_till_done()
        # Home Assistant running, nothing new: no reload.
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=RELOAD_DELAY_SECONDS + 1))
        await hass.async_block_till_done()
        reload.assert_not_called()

        # A new door sensor and a thermometer appear: one reload after the delay.
        _set(hass, "binary_sensor.takaovi", "door")
        _set(hass, "sensor.sauna_lampo", "temperature", "°C")
        await hass.async_block_till_done()
        reload.assert_not_called()
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=RELOAD_DELAY_SECONDS + 1))
        await hass.async_block_till_done()
        reload.assert_called_once_with(entry.entry_id)

        # Only a thermometer: the alarm list is the same, no reload.
        reload.reset_mock()
        _set(hass, "sensor.kellari_lampo", "temperature", "°C")
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=2 * RELOAD_DELAY_SECONDS + 2))
        await hass.async_block_till_done()
    await coordinator.async_stop()


async def test_the_list_is_worked_out_again_once_home_assistant_has_started(hass: HomeAssistant) -> None:
    """At startup the other integrations' sensors may not exist yet when this entry is set up."""
    hass.set_state(CoreState.starting)
    entry = _entry(hass)
    coordinator = _coordinator(hass, entry)
    assert coordinator._exporter._entity_ids == []
    with patch.object(hass.config_entries, "async_schedule_reload") as reload:
        await coordinator.async_start()
        _home(hass)
        await hass.async_block_till_done()
        reload.assert_not_called()

        hass.set_state(CoreState.running)
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STARTED)
        await hass.async_block_till_done()
        reload.assert_called_once_with(entry.entry_id)
    await coordinator.async_stop()


async def test_a_saved_selection_never_reloads(hass: HomeAssistant) -> None:
    _home(hass)
    coordinator = _coordinator(hass, _entry(hass, {CONF_EXPORTED_ENTITIES: ["binary_sensor.ulko_ovi"]}))
    with patch.object(hass.config_entries, "async_schedule_reload") as reload:
        await coordinator.async_start()
        _set(hass, "binary_sensor.takaovi", "door")
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=RELOAD_DELAY_SECONDS + 1))
        await hass.async_block_till_done()
        reload.assert_not_called()
    await coordinator.async_stop()


async def test_the_options_open_with_the_alarm_sensors_chosen(hass: HomeAssistant) -> None:
    _home(hass)
    entry = _entry(hass)

    form = await hass.config_entries.options.async_init(entry.entry_id)

    assert (form["type"], form["step_id"]) == (FlowResultType.FORM, "sensors")
    key = next(key for key in form["data_schema"].schema if key == CONF_EXPORTED_ENTITIES)
    assert key.default() == ALARMS_IN_ORDER
    # Saving it as it is keeps them, now as a saved selection. Saving reloads (sets up) the entry: with a
    # mocked MQTT client, and done before the test ends.
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        saved = await hass.config_entries.options.async_configure(
            form["flow_id"], {CONF_EXPORTED_ENTITIES: ALARMS_IN_ORDER}
        )
        await hass.async_block_till_done()
    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_EXPORTED_ENTITIES] == ALARMS_IN_ORDER
    assert await hass.config_entries.async_unload(entry.entry_id)
