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
sensors (sensor_export.py). Saving them reloads the entry.
"""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.selector import (
    EntityFilterSelectorConfig,
    EntitySelector,
    EntitySelectorConfig,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
    BROKER_HOSTS,
    CONF_CUSTOMER_ID,
    CONF_EXPORTED_ENTITIES,
    CONF_HOST,
    CONF_LOGIN_NUMBER,
    DEFAULT_HOST,
    DOMAIN,
    EXTRA_USERNAME_TEMPLATE,
    MAX_EXPORTED_SENSORS,
    USERNAME_TEMPLATE,
)
from .mqtt_client import CannotConnect, InvalidAuth, async_test_connection
from .sensor_export import KIND_BY_BINARY_DEVICE_CLASS

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST, default=DEFAULT_HOST): SelectSelector(
            SelectSelectorConfig(options=list(BROKER_HOSTS), mode=SelectSelectorMode.DROPDOWN)
        ),
        vol.Required(CONF_CUSTOMER_ID): vol.All(vol.Coerce(int), vol.Range(min=1)),
        # Optional extra login kp-<id>-<n> (n >= 2); empty = the main login kp-<id>.
        vol.Optional(CONF_LOGIN_NUMBER): vol.All(vol.Coerce(int), vol.Range(min=2, max=99)),
        vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
    }
)

STEP_REAUTH_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
    }
)


EXPORTABLE_ENTITIES = EntitySelector(
    EntitySelectorConfig(
        multiple=True,
        filter=[
            EntityFilterSelectorConfig(
                domain="binary_sensor", device_class=list(KIND_BY_BINARY_DEVICE_CLASS)
            ),
            EntityFilterSelectorConfig(domain="sensor", device_class="temperature"),
        ],
    )
)


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
            await self.async_set_unique_id(str(user_input[CONF_CUSTOMER_ID]))
            self._abort_if_unique_id_configured()
            if user_input[CONF_HOST] not in BROKER_HOSTS:
                errors["base"] = "cannot_connect"
                return self.async_show_form(step_id="user", data_schema=STEP_USER_DATA_SCHEMA, errors=errors)
            login_number = user_input.get(CONF_LOGIN_NUMBER)
            user_input = {
                **user_input,
                CONF_USERNAME: (
                    EXTRA_USERNAME_TEMPLATE.format(
                        customer_id=user_input[CONF_CUSTOMER_ID], login_number=login_number
                    )
                    if login_number
                    else USERNAME_TEMPLATE.format(customer_id=user_input[CONF_CUSTOMER_ID])
                ),
            }

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
                    title=(
                        f"Kameraposti ({user_input[CONF_USERNAME]})"
                        if user_input.get(CONF_LOGIN_NUMBER)
                        else f"Kameraposti ({user_input[CONF_CUSTOMER_ID]})"
                    ),
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
    """Choose the entities exported to Kameraposti as sensors."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        current = list(self.config_entry.options.get(CONF_EXPORTED_ENTITIES, []))

        if user_input is not None:
            selected = list(dict.fromkeys(user_input.get(CONF_EXPORTED_ENTITIES, [])))
            if len(selected) > MAX_EXPORTED_SENSORS:
                errors[CONF_EXPORTED_ENTITIES] = "too_many"
                current = selected
            else:
                options = {**self.config_entry.options, CONF_EXPORTED_ENTITIES: selected}
                return self.async_create_entry(data=options)

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {vol.Optional(CONF_EXPORTED_ENTITIES, default=current): EXPORTABLE_ENTITIES}
            ),
            errors=errors,
            description_placeholders={"max": str(MAX_EXPORTED_SENSORS)},
        )
