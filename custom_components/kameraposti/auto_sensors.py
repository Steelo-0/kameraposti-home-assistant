"""Alarm sensors sent by themselves (1.16.0, steelo 2026-10-21 "Hälytysanturit lähtevät itsestään. Uusi
HA-yhteys lähettää heti ovi-, ikkuna-, liike-, savu-, vuoto- ja kaasuanturit paketin rajaan asti.
Lämpötilat ja muut lisätään itse, ja kaikki voi poistaa valinnasta.").

Until a selection has been saved under Configure -> Sensors to Turva, every smoke, gas, leak, door,
window and motion sensor of this Home Assistant is sent, most important first (the plan's limit takes the
first, sensor_export). Temperatures, CO2 and sensors whose kind has to be chosen are left to the user, and so
are Frigate's own sensors (1.11.1), this integration's own entities, and hidden, disabled or diagnostic ones.
The selection is not saved by this, so no list is sent and Kameraposti drops nothing (1.14.1 H-H1); the
options form opens with these chosen, and saving it ends the automatic mode.

Other integrations may still be starting when this entry is set up, and sensors are added later: once Home
Assistant has started, and after a new sensor appears, the list is worked out again, and if it changed the
entry reloads (at most once per RELOAD_DELAY_SECONDS).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.core import (
    CALLBACK_TYPE,
    CoreState,
    Event,
    EventStateChangedData,
    HassJob,
    HomeAssistant,
    callback,
)
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.start import async_at_started

from .const import DOMAIN, MAX_EXPORTED_SENSORS
from .sensor_export import frigate_own_sensor, kind_for

_LOGGER = logging.getLogger(__name__)

# The kinds sent by themselves, most important first.
AUTO_KINDS: tuple[str, ...] = ("smoke", "gas", "leak", "door", "window", "motion")
RELOAD_DELAY_SECONDS = 60


@callback
def auto_alarm_entities(hass: HomeAssistant) -> list[str]:
    """Every alarm sensor sent without a saved selection, most important kind first."""
    registry = er.async_get(hass)
    found: list[tuple[int, str]] = []
    for state in hass.states.async_all(("binary_sensor", "sensor")):
        kind = kind_for(state)
        if kind not in AUTO_KINDS or frigate_own_sensor(hass, state.entity_id):
            continue
        entry = registry.async_get(state.entity_id)
        if entry is not None and (
            entry.platform == DOMAIN or entry.disabled or entry.hidden or entry.entity_category is not None
        ):
            continue
        found.append((AUTO_KINDS.index(kind), state.entity_id))
    return [entity_id for _, entity_id in sorted(found)][:MAX_EXPORTED_SENSORS]


@callback
def _is_new_sensor(data: Mapping[str, Any]) -> bool:
    return data.get("old_state") is None and str(data.get("entity_id", "")).startswith(
        ("binary_sensor.", "sensor.")
    )


class KameraportiAutoSensors:
    """Reloads the entry when the alarm sensors to send by themselves have changed."""

    def __init__(self, hass: HomeAssistant, entry_id: str, sent: list[str]) -> None:
        self._hass = hass
        self._entry_id = entry_id
        self._sent = list(sent)
        self._unsubs: list[Callable[[], None]] = []
        self._pending: CALLBACK_TYPE | None = None
        self._reloading = False

    @callback
    def async_start(self) -> None:
        if self._unsubs:
            return
        self._unsubs.append(
            self._hass.bus.async_listen(
                EVENT_STATE_CHANGED, self._handle_new_sensor, event_filter=_is_new_sensor
            )
        )
        self._unsubs.append(async_at_started(self._hass, self._handle_started))

    @callback
    def async_stop(self) -> None:
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()
        if self._pending is not None:
            self._pending()
            self._pending = None

    @callback
    def _handle_started(self, _hass: HomeAssistant) -> None:
        self._check()

    @callback
    def _handle_new_sensor(self, _event: Event[EventStateChangedData]) -> None:
        # At startup every entity is new; the started check covers them all at once.
        if self._hass.state is not CoreState.running or self._pending is not None:
            return
        self._pending = async_call_later(
            self._hass, RELOAD_DELAY_SECONDS, HassJob(self._handle_delay, cancel_on_shutdown=True)
        )

    @callback
    def _handle_delay(self, _now: datetime) -> None:
        self._pending = None
        self._check()

    @callback
    def _check(self) -> None:
        if self._reloading:
            return
        now = auto_alarm_entities(self._hass)
        if now == self._sent:
            return
        _LOGGER.info(
            "Kameraposti: the alarm sensors sent by themselves changed (%d -> %d); reloading",
            len(self._sent),
            len(now),
        )
        self._reloading = True
        self._hass.config_entries.async_schedule_reload(self._entry_id)
