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
cameras step only hides cameras from it (and keeps the one video setting).
Saving reloads the entry.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.selector import (
    BooleanSelector,
    EntitySelector,
    EntitySelectorConfig,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .camera_export import (
    camera_settings_from_options,
    exported_cameras,
    home_assistant_cameras,
    removed_after_save,
)
from .const import (
    BROKER_HOSTS,
    CAMERA_ENTITY_PATTERN,
    CAPTURE_VIDEO_SECONDS_DEFAULT,
    CAPTURE_VIDEO_SECONDS_MAX,
    CAPTURE_VIDEO_SECONDS_MIN,
    CONF_CAMERA_VIDEO,
    CONF_CAMERA_VIDEO_SECONDS,
    CONF_CUSTOMER_ID,
    CONF_EXCLUDED_CAMERAS,
    CONF_EXPORTED_CAMERAS,
    CONF_EXPORTED_ENTITIES,
    CONF_HOST,
    CONF_REMOVED_CAMERAS,
    DEFAULT_HOST,
    DOMAIN,
    MAX_EXPORTED_CAMERAS,
    USERNAME_TEMPLATE,
)
from .login import entry_login, is_device_login, parse_login
from .mqtt_client import CannotConnect, InvalidAuth, async_test_connection
from .sensor_export import effective_sensor_limit, kameraposti_name, kind_for

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST, default=DEFAULT_HOST): SelectSelector(
            SelectSelectorConfig(options=list(BROKER_HOSTS), mode=SelectSelectorMode.DROPDOWN)
        ),
        # 1.3.1: the login as shown on the Anturit page ("kp-2", "kp-2-2") or just the account number.
        vol.Required(CONF_CUSTOMER_ID): vol.Coerce(str),
        vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
    }
)

STEP_REAUTH_DATA_SCHEMA = vol.Schema(
    {
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


def exportable_entity_options(hass: HomeAssistant, current: list[str]) -> list[SelectOptionDict]:
    """1.4.1: the export list offers exactly the entities this integration can send.

    Built from kind_for() instead of an entity filter, because a filter only sees device classes and
    Z-Wave JS UI publishes gas levels (CO, CO2 ppm) without one. Chosen entities stay listed even if
    they are unavailable right now, so saving the form does not drop them.
    """
    # 1.4.2 (steelo 2026-10-07 "liian pitkät nimet"): "<name in Kameraposti> · <kind>", not
    # "<friendly name> (<entity_id>)"; the entity id is added only when two rows would read the same.
    kind_labels = KIND_LABELS_FI if (hass.config.language or "").startswith("fi") else KIND_LABELS_EN
    candidates = [
        state for state in hass.states.async_all(("binary_sensor", "sensor")) if kind_for(state) is not None
    ]
    candidate_ids = [state.entity_id for state in candidates]
    labels: dict[str, str] = {}
    for state in candidates:
        kind = kind_for(state)
        labels[state.entity_id] = (
            f"{kameraposti_name(hass, state, candidate_ids)} · {kind_labels.get(kind, kind)}"
        )
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

    _reauth_entry_data: dict[str, Any] | None = None

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> KameraportiOptionsFlow:
        return KameraportiOptionsFlow()

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """First (and only, for V1) step: customer_id + credentials."""
        errors: dict[str, str] = {}

        if user_input is not None:
            if user_input[CONF_HOST] not in BROKER_HOSTS:
                errors["base"] = "cannot_connect"
                return self.async_show_form(step_id="user", data_schema=STEP_USER_DATA_SCHEMA, errors=errors)
            parsed = parse_login(user_input[CONF_CUSTOMER_ID])
            if parsed is None:
                device = is_device_login(user_input[CONF_CUSTOMER_ID])
                return self.async_show_form(
                    step_id="user",
                    data_schema=STEP_USER_DATA_SCHEMA,
                    errors={CONF_CUSTOMER_ID: "device_login" if device else "invalid_login"},
                )
            customer_id, username = parsed
            is_extra_login = username != USERNAME_TEMPLATE.format(customer_id=customer_id)
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
                    title=f"Kameraposti ({username})" if is_extra_login else f"Kameraposti ({customer_id})",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

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
                reauth_entry = self._get_reauth_entry()
                return self.async_update_reload_and_abort(reauth_entry, data=candidate)

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=STEP_REAUTH_DATA_SCHEMA,
            errors=errors,
        )


class KameraportiOptionsFlow(OptionsFlow):
    """The entities exported to Kameraposti as sensors, and the Home Assistant cameras (1.7.0)."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """1.6.0: a menu -- the exported sensors, or the cameras."""
        return self.async_show_menu(step_id="init", menu_options=["sensors", "cameras"])

    async def async_step_cameras(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """1.8.0 (steelo 2026-10-16 "maalaisjärjellä, pari klikkausta"): the cameras Kameraposti must
        not use, and the video setting.

        Every other camera is reported and chosen in Kameraposti (1.7.0's ticking left settings in three
        places). An entry still on a 1.7.0 tick list or 1.6.0 links opens with every camera outside that
        list hidden, so saving without changes keeps what Kameraposti gets; the save replaces the list
        with the hidden cameras. A camera the save stops reporting is described to Kameraposti as
        removed.
        """
        errors: dict[str, str] = {}
        previous = camera_settings_from_options(self.config_entry.options)
        if previous.allowlist is None:
            excluded = list(previous.excluded)
        else:
            excluded = [
                entity_id
                for entity_id in home_assistant_cameras(self.hass)
                if entity_id not in previous.allowlist
            ]
        video = previous.video
        seconds = previous.video_seconds

        if user_input is not None:
            selected = list(dict.fromkeys(user_input.get(CONF_EXCLUDED_CAMERAS, [])))
            video = bool(user_input.get(CONF_CAMERA_VIDEO, False))
            seconds = int(user_input.get(CONF_CAMERA_VIDEO_SECONDS, CAPTURE_VIDEO_SECONDS_DEFAULT))
            if any(CAMERA_ENTITY_PATTERN.fullmatch(entity_id) is None for entity_id in selected):
                errors[CONF_EXCLUDED_CAMERAS] = "invalid_camera"
                excluded = selected
            else:
                saved = replace(previous, allowlist=None, excluded=tuple(selected))
                before = exported_cameras(self.hass, previous)
                after = exported_cameras(self.hass, saved)
                options = {
                    **self.config_entry.options,
                    CONF_EXCLUDED_CAMERAS: selected,
                    CONF_REMOVED_CAMERAS: removed_after_save(previous.removed, before, after),
                    CONF_CAMERA_VIDEO: video,
                    CONF_CAMERA_VIDEO_SECONDS: seconds,
                }
                # The 1.7.0 tick list would keep limiting what is reported; the hidden list replaces it.
                options.pop(CONF_EXPORTED_CAMERAS, None)
                return self.async_create_entry(data=options)

        return self.async_show_form(
            step_id="cameras",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_EXCLUDED_CAMERAS, default=excluded): EntitySelector(
                        EntitySelectorConfig(domain="camera", multiple=True)
                    ),
                    vol.Required(CONF_CAMERA_VIDEO, default=video): BooleanSelector(),
                    vol.Required(CONF_CAMERA_VIDEO_SECONDS, default=seconds): NumberSelector(
                        NumberSelectorConfig(
                            min=CAPTURE_VIDEO_SECONDS_MIN,
                            max=CAPTURE_VIDEO_SECONDS_MAX,
                            step=1,
                            unit_of_measurement="s",
                            mode=NumberSelectorMode.BOX,
                        )
                    ),
                }
            ),
            errors=errors,
            description_placeholders={"max": str(MAX_EXPORTED_CAMERAS)},
        )

    async def async_step_sensors(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Choose the entities exported to Kameraposti as sensors."""
        errors: dict[str, str] = {}
        # 1.8.0 (steelo 2026-10-16 "maalaisjärjellä, pari klikkausta"): as many as the account's plan
        # takes -- the limit the running entry's roster told, else the default.
        coordinator = self.hass.data.get(DOMAIN, {}).get(self.config_entry.entry_id)
        limit = effective_sensor_limit(coordinator.sensor_limit if coordinator is not None else None)
        placeholders = {"max": str(limit), "entities": ""}
        current = list(self.config_entry.options.get(CONF_EXPORTED_ENTITIES, []))

        if user_input is not None:
            selected = list(dict.fromkeys(user_input.get(CONF_EXPORTED_ENTITIES, [])))
            # 1.3.2: a "problem" sensor whose kind cannot be read from its name (e.g. general
            # purpose) would be dropped silently -- say which one instead.
            unknown = [
                entity_id
                for entity_id in selected
                if (state := self.hass.states.get(entity_id)) is not None and kind_for(state) is None
            ]
            if len(selected) > limit:
                errors[CONF_EXPORTED_ENTITIES] = "too_many"
                current = selected
            elif unknown:
                errors[CONF_EXPORTED_ENTITIES] = "unknown_kind"
                current = selected
                placeholders["entities"] = ", ".join(unknown)
            else:
                options = {**self.config_entry.options, CONF_EXPORTED_ENTITIES: selected}
                return self.async_create_entry(data=options)

        return self.async_show_form(
            step_id="sensors",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_EXPORTED_ENTITIES, default=current): SelectSelector(
                        SelectSelectorConfig(
                            options=exportable_entity_options(self.hass, current),
                            multiple=True,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
            errors=errors,
            description_placeholders=placeholders,
        )
