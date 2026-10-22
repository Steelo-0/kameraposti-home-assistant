"""Config flow for the Kameraposti integration.

Version 2 (2026-10-05): Kameraposti's own broker. Asks for the service
(production or the CAM test server -- a fixed list, never a free-form host),
the account number and the account's MQTT password from Kameraposti's sensor
page. The login is always kp-<customer_id> (the broker pins it to that client
id), so the user never types a username. Port/path/transport are fixed.

Runs a REAL connection test (connect + auth + subscribe to the caller's
own customer namespace) before ever saving the entry (section 18) -- a
malformed/incorrect account never gets silently accepted.

Options (2026-10-05): the Home Assistant entities exported to Kameraposti as
sensors (sensor_export.py). 1.6.0: a menu -- the sensors, or the cameras.
1.7.0: the cameras are one list of Home Assistant cameras ticked for Kameraposti
(camera_export.py) and one video setting; Kameraposti makes a camera of each and
nothing is linked by hand (1.6.0 linked one Kameraposti camera at a time).
1.8.0: nothing to tick -- every camera is reported and chosen in Kameraposti; the
cameras step only hides cameras from it (and keeps the one video setting). 1.8.1:
the hidden list starts empty for everyone, never pre-filled from older options.
1.10.0: no cameras step at all -- the options are the sensors. Saving reloads the entry.

1.14.1 (Fable bug hunt H-L2): Reconfigure changes an entry's service and password; the login, the
options and the entities stay (moving to turva.secsrv.fi needed removing and re-adding it).

1.18.0 (Turva onboarding, steelo 2026-10-22 "Käyttöliittymä ja yhdistäminen mahdollisimman yksinkertaiseksi"):
the first form is the pairing code alone, for Turva's service. Left empty, it opens the login step: the
service, and a code or the login and password (1.17.0's form), so every service and way still connects.
"""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant, State, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .auto_sensors import auto_alarm_entities
from .const import (
    BROKER_HOSTS,
    CONF_CUSTOMER_ID,
    CONF_ENTITY_KINDS,
    CONF_EXPORTED_ENTITIES,
    CONF_HOST,
    CONF_PAIRING_CODE,
    DEFAULT_HOST,
    DOMAIN,
    SETUP_DEFAULT_HOST,
    TURVA_HOSTS,
    USERNAME_TEMPLATE,
)
from .coordinator import roster_store
from .login import entry_login, is_device_login, parse_login
from .mqtt_client import CannotConnect, InvalidAuth, async_test_connection
from .pairing import InvalidPairingCode, PairingLoginsFull, async_pair, normalize_code
from .sensor_export import (
    CHOOSABLE_NUMERIC_KINDS,
    MAX_EXPORTED_SENSORS,
    choosable_kinds,
    effective_kind,
    effective_sensor_limit,
    frigate_own_sensor,
    kameraposti_name,
    kind_for,
)

# 1.12.1 (2026-10-19: one options save reloaded the entry twice in steelo's log): Home Assistant reloads the
# entry itself after an options save (OptionsFlowWithReload) and after reauth
# (async_update_reload_and_abort). An update listener on top of that reloaded a second time -- and
# newer Home Assistant refuses update listeners together with either. Older Home Assistant without
# OptionsFlowWithReload keeps the update listener (__init__.py), as before.
try:
    from homeassistant.config_entries import OptionsFlowWithReload as _OptionsFlowBase

    OPTIONS_FLOW_RELOADS = True
except ImportError:  # pragma: no cover -- Home Assistant without OptionsFlowWithReload
    _OptionsFlowBase = OptionsFlow
    OPTIONS_FLOW_RELOADS = False

_LOGGER = logging.getLogger(__name__)

# 1.18.0: the code shown on the Turva page, for Turva's service (SETUP_DEFAULT_HOST). Optional: left empty,
# the login step follows (the field's own text says so).
STEP_USER_DATA_SCHEMA = vol.Schema({vol.Optional(CONF_PAIRING_CODE): vol.Coerce(str)})


def _login_schema(host: str) -> vol.Schema:
    """1.18.0: the service (Turva's preselected, after a failure the one just tried), a code or the login."""
    return vol.Schema(
        {
            vol.Required(CONF_HOST, default=host): SelectSelector(
                SelectSelectorConfig(options=list(BROKER_HOSTS), mode=SelectSelectorMode.DROPDOWN)
            ),
            # 1.17.0 (steelo 2026-10-21 "Paritus suositus"): the code from the service's page, or the login.
            vol.Optional(CONF_PAIRING_CODE): vol.Coerce(str),
            # 1.3.1: the login as shown on the Anturit page ("kp-2", "kp-2-2") or just the account number.
            vol.Optional(CONF_CUSTOMER_ID): vol.Coerce(str),
            vol.Optional(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
        }
    )


STEP_REAUTH_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
    }
)


def _reconfigure_schema(host: str) -> vol.Schema:
    """1.14.1: the service (the entry's own preselected) and the password there."""
    return vol.Schema(
        {
            vol.Required(CONF_HOST, default=host): SelectSelector(
                SelectSelectorConfig(options=list(BROKER_HOSTS), mode=SelectSelectorMode.DROPDOWN)
            ),
            vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
        }
    )


KIND_LABELS_FI: dict[str, str] = {
    "leak": "Vuoto",
    "smoke": "Savu",
    "gas": "Kaasu",
    "door": "Ovi",
    "window": "Ikkuna",
    "motion": "Liike",
    "temperature": "Lämpötila",
    "co2": "CO₂",
}
# 1.13.0: an entity whose kind cannot be read is listed too; its kind is asked on the next page.
CHOOSE_KIND_LABEL = {"fi": "laji valitaan seuraavaksi", "en": "kind chosen next"}
KIND_LABELS_EN: dict[str, str] = {
    "leak": "Leak",
    "smoke": "Smoke",
    "gas": "Gas",
    "door": "Door",
    "window": "Window",
    "motion": "Motion",
    "temperature": "Temperature",
    "co2": "CO₂",
}


def _limit_note(known: int | None, finnish: bool) -> str:
    """The plan's limit in the sensors form, or that it is not known yet (design review TOP 8)."""
    if known is not None:
        limit = effective_sensor_limit(known)
        if finnish:
            return f"Kamerapostin pakettisi ottaa enintään {limit}."
        return f"Your Kameraposti plan takes at most {limit}."
    if finnish:
        return (
            "Pakettisi anturirajaa ei ole vielä saatu Kamerapostista; "
            "jos valitset enemmän kuin paketti ottaa, ylimenevät jäävät pois."
        )
    return (
        "Your plan's sensor limit has not arrived from Kameraposti yet; "
        "if you choose more than it takes, the rest are left out."
    )


def _choosable_kinds_now(state: State | None) -> tuple[str, ...]:
    """choosable_kinds(), and the number kinds for a sensor that has no value right now.

    1.14.2 (bug hunt 19.10 H-L1): an unavailable or unknown sensor without a unit or state class cannot tell
    whether it reports a number; it was refused as "unknown_kind" and the whole selection with it. It is
    listed only when chosen before (exportable_entity_options), so its kind is asked like any other's.
    """
    kinds = choosable_kinds(state)
    if kinds or state is None or state.domain != "sensor":
        return kinds
    return CHOOSABLE_NUMERIC_KINDS if state.state in (STATE_UNAVAILABLE, STATE_UNKNOWN) else ()


def exportable_entity_options(
    hass: HomeAssistant, current: list[str], kinds: dict[str, str] | None = None
) -> list[SelectOptionDict]:
    """The export list: 1.13.0 (steelo 2026-10-19 "muuta niin että näkyy kaikki") every binary sensor and
    every numeric sensor, not only those whose kind can be read (1.4.1-1.12.x).

    An entity whose kind cannot be read gets "kind chosen next": the options ask it on their next page
    (choosable_kinds). Chosen entities stay listed even if they are unavailable right now, so saving the
    form does not drop them. Never offered: Frigate's own binary sensors (1.11.1) and this integration's
    own entities (they would loop back to Kameraposti).
    """
    # 1.4.2 (steelo 2026-10-07 "liian pitkät nimet"): "<name in Kameraposti> · <kind>", not
    # "<friendly name> (<entity_id>)"; the entity id is added only when two rows would read the same.
    finnish = (hass.config.language or "").startswith("fi")
    kind_labels = KIND_LABELS_FI if finnish else KIND_LABELS_EN
    choose = CHOOSE_KIND_LABEL["fi" if finnish else "en"]
    registry = er.async_get(hass)
    candidates = [
        state
        for state in hass.states.async_all(("binary_sensor", "sensor"))
        if (kind_for(state) is not None or choosable_kinds(state))
        and not frigate_own_sensor(hass, state.entity_id)
        and getattr(registry.async_get(state.entity_id), "platform", None) != DOMAIN
    ]
    candidate_ids = [state.entity_id for state in candidates]
    labels: dict[str, str] = {}
    for state in candidates:
        kind = effective_kind(state, kinds)
        kind_label = kind_labels.get(kind, kind) if kind else choose
        labels[state.entity_id] = f"{kameraposti_name(hass, state, candidate_ids)} · {kind_label}"
    seen: dict[str, int] = {}
    for label in labels.values():
        seen[label] = seen.get(label, 0) + 1
    for entity_id, label in labels.items():
        if seen[label] > 1:
            labels[entity_id] = f"{label} ({entity_id.split('.', 1)[-1]})"
    for entity_id in current:
        labels.setdefault(entity_id, entity_id)
    return [
        SelectOptionDict(value=entity_id, label=label)
        for entity_id, label in sorted(labels.items(), key=lambda item: item[1].lower())
    ]


def _entry_title(hass: HomeAssistant, host: str, customer_id: int, username: str) -> str:
    """1.18.0 (Turva onboarding, review F15): "Turva (<home name>)" on a Turva service -- the name the service
    also shows for this Home Assistant's login (pairing.py). Kameraposti keeps "Kameraposti (<login>)": the
    account number, or an extra login itself. A Turva entry without a home name gets the login too."""
    login = username if username != USERNAME_TEMPLATE.format(customer_id=customer_id) else str(customer_id)
    if host not in TURVA_HOSTS:
        return f"Kameraposti ({login})"
    return f"Turva ({(hass.config.location_name or '').strip() or login})"


async def _async_validate(hass: HomeAssistant, data: dict[str, Any]) -> None:
    """Run the real connection test. Raises CannotConnect / InvalidAuth."""
    await async_test_connection(
        hass,
        host=data.get(CONF_HOST, DEFAULT_HOST),
        customer_id=data[CONF_CUSTOMER_ID],
        username=data[CONF_USERNAME],
        password=data[CONF_PASSWORD],
    )


class KameraportiConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Kameraposti."""

    VERSION = 2
    # 1.14.1 (Fable bug hunt H-M1): unique ids and device identifiers carry the entry id (__init__.py
    # async_migrate_entry). Only the minor version, so 1.14.0 still loads a migrated entry.
    MINOR_VERSION = 2

    _reauth_entry_data: dict[str, Any] | None = None
    # Fable M-2 (21.10): a code is spent once paired; a retry with it (e.g. after a failed connection test)
    # reuses the login it gave instead of asking again. (service, code) -> (login, password), this flow only.
    _paired: dict[tuple[str, str], tuple[str, str]] | None = None

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> KameraportiOptionsFlow:
        return KameraportiOptionsFlow()

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """1.18.0: the pairing code alone, for Turva's service; an empty code opens the login step."""
        if user_input is None:
            return self._setup_form("user", SETUP_DEFAULT_HOST)
        code = str(user_input.get(CONF_PAIRING_CODE) or "").strip()
        if not code:
            return await self.async_step_login()
        return await self._async_set_up("user", {CONF_HOST: SETUP_DEFAULT_HOST, CONF_PAIRING_CODE: code})

    async def async_step_login(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """1.18.0: the service, and its pairing code or the login and password (1.17.0's first form)."""
        if user_input is None:
            return self._setup_form("login", SETUP_DEFAULT_HOST)
        return await self._async_set_up("login", user_input)

    def _setup_form(self, step_id: str, host: str, errors: dict[str, str] | None = None) -> ConfigFlowResult:
        schema = STEP_USER_DATA_SCHEMA if step_id == "user" else _login_schema(host)
        return self.async_show_form(step_id=step_id, data_schema=schema, errors=errors or {})

    async def _async_set_up(self, step_id: str, user_input: dict[str, Any]) -> ConfigFlowResult:
        """Pair or take the typed login, then test the connection and create the entry (either step)."""
        errors: dict[str, str] = {}
        host = user_input[CONF_HOST]
        if host not in BROKER_HOSTS:
            return self._setup_form(step_id, SETUP_DEFAULT_HOST, {"base": "cannot_connect"})
        # 1.17.0: a pairing code gets this Home Assistant's own login from the service (pairing.py);
        # without one the login and password are needed as before. The code is never stored.
        code = str(user_input.get(CONF_PAIRING_CODE) or "").strip()
        if code:
            paired = self._paired if self._paired is not None else {}
            self._paired = paired
            key = (host, normalize_code(code))
            try:
                if key in paired:
                    login, password = paired[key]
                else:
                    login, password = paired[key] = await async_pair(self.hass, host, code)
            except InvalidPairingCode:
                errors[CONF_PAIRING_CODE] = "invalid_code"
            except PairingLoginsFull:
                errors["base"] = "logins_full"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            if errors:
                return self._setup_form(step_id, host, errors)
            user_input = {CONF_HOST: host, CONF_CUSTOMER_ID: login, CONF_PASSWORD: password}
        elif not str(user_input.get(CONF_CUSTOMER_ID) or "").strip() or not user_input.get(CONF_PASSWORD):
            return self._setup_form(step_id, host, {"base": "code_or_login"})
        user_input = {key: value for key, value in user_input.items() if key != CONF_PAIRING_CODE}
        parsed = parse_login(user_input[CONF_CUSTOMER_ID])
        if parsed is None:
            if code:
                # Not typed by the user (the code form has no login field): the service's answer is wrong.
                _LOGGER.warning("Kameraposti: the service paired Home Assistant with the login %r", login)
                return self._setup_form(step_id, host, {"base": "unknown"})
            device = is_device_login(user_input[CONF_CUSTOMER_ID])
            return self._setup_form(
                step_id, host, {CONF_CUSTOMER_ID: "device_login" if device else "invalid_login"}
            )
        customer_id, username = parsed
        # 1.5.1: the unique id is the canonical login ("3", "kp-3" and " KP-3 " are one
        # login), checked before the test connection, which would kick a running entry with
        # the same login off the broker. Entries from before 1.5.1 that have not been set up
        # since (so still carry the raw input as unique id) are matched by their stored login.
        await self.async_set_unique_id(username)
        self._abort_if_unique_id_configured()
        if any(entry_login(entry.data) == username for entry in self._async_current_entries()):
            return self.async_abort(reason="already_configured")
        user_input = {**user_input, CONF_CUSTOMER_ID: customer_id, CONF_USERNAME: username}

        try:
            await _async_validate(self.hass, user_input)
        except InvalidAuth:
            errors["base"] = "invalid_auth"
        except CannotConnect:
            errors["base"] = "cannot_connect"
        except Exception:  # noqa: BLE001 - never let setup crash the flow
            _LOGGER.exception("Unexpected error validating Kameraposti connection")
            errors["base"] = "unknown"
        else:
            return self.async_create_entry(
                title=_entry_title(self.hass, host, customer_id, username), data=user_input
            )
        return self._setup_form(step_id, host, errors)

    async def async_step_reauth(self, entry_data: dict[str, Any]) -> ConfigFlowResult:
        """Start a reauth flow (contract section 20) when the broker reports auth failure."""
        self._reauth_entry_data = dict(entry_data)
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Ask only for a new password; customer_id/username are unchanged."""
        errors: dict[str, str] = {}

        if user_input is not None and self._reauth_entry_data is not None:
            candidate = {**self._reauth_entry_data, CONF_PASSWORD: user_input[CONF_PASSWORD]}
            try:
                await _async_validate(self.hass, candidate)
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Unexpected error validating Kameraposti reauth")
                errors["base"] = "unknown"
            else:
                return self._update_reload_and_abort(self._get_reauth_entry(), candidate, "reauth_successful")

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=STEP_REAUTH_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_reconfigure(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """1.14.1 (Fable bug hunt H-L2): another service and its password for an existing entry.

        The login (the entry's unique id), the options and every entity stay. Until 1.14.1 the service
        could only be changed by removing and re-adding the integration, which reset the chosen sensors
        and -- before the H-H1 fix -- made Kameraposti drop them. Tested for real before it is saved,
        like the setup.
        """
        entry = self._get_reconfigure_entry()
        host = entry.data.get(CONF_HOST, DEFAULT_HOST)
        errors: dict[str, str] = {}

        if user_input is not None:
            candidate = {
                **entry.data,
                CONF_HOST: user_input[CONF_HOST],
                CONF_PASSWORD: user_input[CONF_PASSWORD],
            }
            if candidate[CONF_HOST] not in BROKER_HOSTS:
                errors["base"] = "cannot_connect"
            else:
                try:
                    await _async_validate(self.hass, candidate)
                except InvalidAuth:
                    errors["base"] = "invalid_auth"
                except CannotConnect:
                    errors["base"] = "cannot_connect"
                except Exception:  # noqa: BLE001
                    _LOGGER.exception("Unexpected error validating Kameraposti reconfigure")
                    errors["base"] = "unknown"
                else:
                    if candidate[CONF_HOST] != host:
                        await _async_mark_roster_time_of(self.hass, entry, host)
                    return self._update_reload_and_abort(entry, candidate, "reconfigure_successful")
                # The form comes back with the service just tried.
                host = candidate[CONF_HOST]

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_reconfigure_schema(host),
            errors=errors,
            description_placeholders={"login": entry_login(entry.data) or ""},
        )

    def _update_reload_and_abort(
        self, entry: ConfigEntry, data: dict[str, Any], reason: str
    ) -> ConfigFlowResult:
        """Store the entry's new data and reload it once.

        1.14.2 (bug hunt 19.10 H-L3): on Home Assistant without OptionsFlowWithReload the update listener
        (__init__.py) reloads a changed entry; async_update_reload_and_abort reloaded it a second time. There
        the listener reloads, and an unchanged entry (the same password again) is reloaded here.
        """
        if OPTIONS_FLOW_RELOADS:
            return self.async_update_reload_and_abort(entry, data=data, reason=reason)
        if not self.hass.config_entries.async_update_entry(entry, data=data):
            self.hass.config_entries.async_schedule_reload(entry.entry_id)
        return self.async_abort(reason=reason)


async def _async_mark_roster_time_of(hass: HomeAssistant, entry: ConfigEntry, host: str) -> None:
    """The stored camera-list time is the old service's: the new setup ignores it (coordinator), so the new
    service's list -- maybe older, from another server -- applies and its cameras replace the old ones.

    1.14.1 (Fable audit L-3): not removed here -- the running setup could store it again before the
    reload. Since 1.14.1 it carries its service; one stored by 1.14.0 has none and gets the old one.
    """
    store = roster_store(hass, entry.entry_id)
    stored = await store.async_load()
    if isinstance(stored, dict) and "host" not in stored:
        await store.async_save({**stored, "host": host})


class KameraportiOptionsFlow(_OptionsFlowBase):
    """The entities exported to Kameraposti as sensors.

    1.10.0 (steelo 2026-10-16 "tee sitä nyt jo yksinkertaisempi"): the only setting. The cameras are
    chosen in Kameraposti and the video comes with every alarm, so there is no camera step to get wrong
    (1.8.x's "cameras Kameraposti does not see" list hid a Shelly when its video was switched on).
    """

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Straight to the sensors (1.6.0-1.9.x: a menu of sensors and cameras)."""
        return await self.async_step_sensors(user_input)

    async def async_step_sensors(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Choose the entities exported to Kameraposti as sensors."""
        errors: dict[str, str] = {}
        # 1.8.0 (steelo 2026-10-16 "maalaisjärjellä, pari klikkausta"): as many as the account's plan
        # takes -- the limit the running entry's roster told, else the default.
        coordinator = self.hass.data.get(DOMAIN, {}).get(self.config_entry.entry_id)
        known = coordinator.sensor_limit if coordinator is not None else None
        # Design review TOP 8: until Kameraposti has told the plan's limit (entry not running, no roster
        # yet) the form does not refuse at the default 20 -- a 30-sensor plan could not be saved right
        # after a restart. The exporter still sends only what the plan takes once the limit is known.
        limit = effective_sensor_limit(known) if known is not None else MAX_EXPORTED_SENSORS
        finnish = (self.hass.config.language or "").startswith("fi")
        placeholders = {"max": str(limit), "entities": "", "limit_note": _limit_note(known, finnish)}
        # 1.11.1: Frigate's own binary sensors chosen before drop out of the form; saving it forgets them.
        # 1.16.0: with no saved selection the form opens with the alarm sensors sent by themselves.
        current = (
            [
                entity_id
                for entity_id in self.config_entry.options.get(CONF_EXPORTED_ENTITIES, [])
                if not frigate_own_sensor(self.hass, entity_id)
            ]
            if CONF_EXPORTED_ENTITIES in self.config_entry.options
            else auto_alarm_entities(self.hass)[:limit]
        )

        if user_input is not None:
            selected = list(dict.fromkeys(user_input.get(CONF_EXPORTED_ENTITIES, [])))
            # 1.3.2: an entity that cannot be sent at all (no kind can be read or chosen, e.g. a text
            # sensor chosen before) would be dropped silently -- say which one instead.
            unknown = [
                entity_id
                for entity_id in selected
                if (state := self.hass.states.get(entity_id)) is not None
                and kind_for(state) is None
                and not _choosable_kinds_now(state)
            ]
            if len(selected) > limit:
                # Fable audit L6: before the plan's limit is known, the error names only the hard maximum.
                too_many = "too_many" if known is not None else "too_many_max"
                errors[CONF_EXPORTED_ENTITIES] = too_many
                current = selected
            elif unknown:
                errors[CONF_EXPORTED_ENTITIES] = "unknown_kind"
                current = selected
                placeholders["entities"] = ", ".join(unknown)
            else:
                self._selected = selected
                # 1.13.0: the kind of every chosen entity whose kind cannot be read is asked next.
                # 1.14.2 (H-L1): with the kinds it may take now -- the page is answered for what it asked,
                # also when an entity's state changes while it is open.
                self._needed = self._kinds_needed()
                if self._needed:
                    return await self.async_step_kinds()
                return self._save({})

        return self.async_show_form(
            step_id="sensors",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_EXPORTED_ENTITIES, default=current): SelectSelector(
                        SelectSelectorConfig(
                            options=exportable_entity_options(
                                self.hass, current, self.config_entry.options.get(CONF_ENTITY_KINDS, {})
                            ),
                            multiple=True,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
            errors=errors,
            description_placeholders=placeholders,
        )

    _selected: list[str]
    _needed: dict[str, tuple[str, ...]]

    def _kinds_needed(self) -> dict[str, tuple[str, ...]]:
        """Chosen entities whose kind cannot be read but can be chosen, with the kinds they may take."""
        needed: dict[str, tuple[str, ...]] = {}
        for entity_id in self._selected:
            state = self.hass.states.get(entity_id)
            if state is not None and kind_for(state) is None and (choices := _choosable_kinds_now(state)):
                needed[entity_id] = choices
        return needed

    def _save(self, kinds: dict[str, str]) -> ConfigFlowResult:
        """Keep the kinds of chosen entities only; an entity left out forgets its chosen kind."""
        earlier = self.config_entry.options.get(CONF_ENTITY_KINDS, {})
        kept = {entity_id: kind for entity_id, kind in earlier.items() if entity_id in self._selected}
        options = {**self.config_entry.options, CONF_EXPORTED_ENTITIES: self._selected}
        options.pop(CONF_ENTITY_KINDS, None)
        if chosen := {**kept, **kinds}:
            options[CONF_ENTITY_KINDS] = chosen
        return self.async_create_entry(data=options)

    async def async_step_kinds(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """1.13.0: the kind of each chosen entity whose kind cannot be read (door, window, motion ...)."""
        needed = self._needed
        earlier = self.config_entry.options.get(CONF_ENTITY_KINDS, {})
        if user_input is not None:
            kinds = {
                entity_id: user_input[entity_id]
                for entity_id, choices in needed.items()
                if user_input.get(entity_id) in choices
            }
            return self._save(kinds)

        finnish = (self.hass.config.language or "").startswith("fi")
        kind_labels = KIND_LABELS_FI if finnish else KIND_LABELS_EN
        schema: dict[Any, Any] = {}
        names: list[str] = []
        for entity_id, choices in needed.items():
            state = self.hass.states.get(entity_id)
            default = earlier.get(entity_id) if earlier.get(entity_id) in choices else choices[0]
            schema[vol.Required(entity_id, default=default)] = SelectSelector(
                SelectSelectorConfig(
                    options=[
                        SelectOptionDict(value=kind, label=kind_labels.get(kind, kind)) for kind in choices
                    ],
                    mode=SelectSelectorMode.DROPDOWN,
                )
            )
            friendly = state.attributes.get("friendly_name") if state is not None else None
            names.append(f"{entity_id} = {friendly or entity_id}")
        return self.async_show_form(
            step_id="kinds",
            data_schema=vol.Schema(schema),
            description_placeholders={"entities": "\n".join(names)},
        )
