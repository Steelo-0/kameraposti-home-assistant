"""Config flow for the Kameraposti integration.

Only asks for the three things the contract requires (customer_id, MQTT
username, MQTT password, section 3) -- broker host/port/websocket path
are fixed in const.py and never exposed in the UI (section 2/19).

Runs a REAL connection test (connect + auth + subscribe to the caller's
own customer namespace) before ever saving the entry (section 18) -- a
malformed/incorrect account never gets silently accepted.
"""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import CONF_CUSTOMER_ID, DOMAIN
from .mqtt_client import CannotConnect, InvalidAuth, async_test_connection

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_CUSTOMER_ID): vol.All(vol.Coerce(int), vol.Range(min=1)),
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
    }
)

STEP_REAUTH_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
    }
)


async def _async_validate(hass: HomeAssistant, data: dict[str, Any]) -> None:
    """Run the real connection test. Raises CannotConnect / InvalidAuth."""
    await async_test_connection(
        hass,
        customer_id=data[CONF_CUSTOMER_ID],
        username=data[CONF_USERNAME],
        password=data[CONF_PASSWORD],
    )


class KameraportiConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Kameraposti."""

    VERSION = 1

    _reauth_entry_data: dict[str, Any] | None = None

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """First (and only, for V1) step: customer_id + credentials."""
        errors: dict[str, str] = {}

        if user_input is not None:
            await self.async_set_unique_id(str(user_input[CONF_CUSTOMER_ID]))
            self._abort_if_unique_id_configured()

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
                    title=f"Kameraposti ({user_input[CONF_CUSTOMER_ID]})",
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
