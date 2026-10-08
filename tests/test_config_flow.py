"""Config flow tests (contract section 18): real-looking connection test,
proper error mapping, no entry saved on failure, reauth path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_HOST, DOMAIN
from custom_components.kameraposti.mqtt_client import CannotConnect, InvalidAuth

# Version 2 (2026-10-05): service + account number + password; the login is always kp-<id>.
USER_INPUT = {CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: 3, CONF_PASSWORD: "s3cret-pw"}
ENTRY_DATA = {**USER_INPUT, CONF_USERNAME: "kp-3"}


async def test_successful_setup_creates_entry(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection",
        new_callable=AsyncMock,
    ) as mock_test:
        result2 = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.CREATE_ENTRY
    assert result2["data"] == ENTRY_DATA
    mock_test.assert_awaited_once()
    assert mock_test.await_args.kwargs == {
        "host": "cam.steels.me",
        "customer_id": 3,
        "username": "kp-3",
        "password": "s3cret-pw",
    }


async def test_invalid_auth_shows_error_and_does_not_create_entry(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection",
        new_callable=AsyncMock,
        side_effect=InvalidAuth,
    ):
        result2 = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)

    assert result2["type"] is FlowResultType.FORM
    assert result2["errors"] == {"base": "invalid_auth"}
    assert len(hass.config_entries.async_entries(DOMAIN)) == 0


async def test_cannot_connect_shows_error_and_does_not_create_entry(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection",
        new_callable=AsyncMock,
        side_effect=CannotConnect,
    ):
        result2 = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)

    assert result2["type"] is FlowResultType.FORM
    assert result2["errors"] == {"base": "cannot_connect"}
    assert len(hass.config_entries.async_entries(DOMAIN)) == 0


async def test_same_customer_id_cannot_be_configured_twice(hass: HomeAssistant) -> None:
    existing = MockConfigEntry(
        domain=DOMAIN, data=ENTRY_DATA, unique_id=str(USER_INPUT[CONF_CUSTOMER_ID]), version=2
    )
    existing.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection",
        new_callable=AsyncMock,
    ):
        result2 = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)

    assert result2["type"] is FlowResultType.ABORT
    assert result2["reason"] == "already_configured"


async def test_reauth_flow_updates_password_on_success(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN, data=ENTRY_DATA, unique_id=str(USER_INPUT[CONF_CUSTOMER_ID]), version=2
    )
    entry.add_to_hass(hass)

    with patch(
        "custom_components.kameraposti.coordinator.KameraportiMqttClient"
    ) as mock_client_cls:
        mock_client_cls.return_value.async_start = AsyncMock()
        mock_client_cls.return_value.async_stop = AsyncMock()
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    result = await entry.start_reauth_flow(hass)
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reauth_confirm"

    with (
        patch(
            "custom_components.kameraposti.config_flow.async_test_connection",
            new_callable=AsyncMock,
        ),
        patch(
            "custom_components.kameraposti.coordinator.KameraportiMqttClient"
        ) as mock_client_cls,
    ):
        mock_client_cls.return_value.async_start = AsyncMock()
        mock_client_cls.return_value.async_stop = AsyncMock()
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_PASSWORD: "new-password"}
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.ABORT
    assert result2["reason"] == "reauth_successful"
    assert entry.data[CONF_PASSWORD] == "new-password"


async def test_reauth_flow_with_still_wrong_password_shows_error(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN, data=ENTRY_DATA, unique_id=str(USER_INPUT[CONF_CUSTOMER_ID]), version=2
    )
    entry.add_to_hass(hass)

    result = await entry.start_reauth_flow(hass)

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection",
        new_callable=AsyncMock,
        side_effect=InvalidAuth,
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_PASSWORD: "still-wrong"}
        )

    assert result2["type"] is FlowResultType.FORM
    assert result2["errors"] == {"base": "invalid_auth"}
    assert entry.data[CONF_PASSWORD] == "s3cret-pw"


async def test_the_service_must_be_one_of_the_known_hosts(hass: HomeAssistant) -> None:
    """Only the fixed services are offered -- never a free-form broker host."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock
    ) as mock_test:
        try:
            result2 = await hass.config_entries.flow.async_configure(
                result["flow_id"], {**USER_INPUT, CONF_HOST: "evil.example.com"}
            )
        except Exception:  # noqa: BLE001 - the selector itself may reject the value
            result2 = None

    assert result2 is None or result2["type"] is FlowResultType.FORM
    mock_test.assert_not_awaited()
    assert len(hass.config_entries.async_entries(DOMAIN)) == 0


async def _setup_with(hass: HomeAssistant, login: object) -> tuple[dict, AsyncMock]:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection",
        new_callable=AsyncMock,
    ) as mock_test:
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_CUSTOMER_ID: login}
        )
        await hass.async_block_till_done()
    return result2, mock_test


async def test_the_login_can_be_typed_as_kp_id_n(hass: HomeAssistant) -> None:
    """steelo 2026-10-06 "ei hyväksy kuin numeroita": a second connection uses an extra login
    kp-<id>-<n>; the field takes the login as shown on the Anturit page (or just the account number)."""
    result, mock_test = await _setup_with(hass, "kp-3-2")

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_USERNAME] == "kp-3-2"
    assert result["data"][CONF_CUSTOMER_ID] == 3
    assert result["title"] == "Kameraposti (kp-3-2)"
    assert mock_test.await_args.kwargs["username"] == "kp-3-2"
    assert mock_test.await_args.kwargs["customer_id"] == 3


async def test_the_main_login_can_be_typed_with_or_without_prefix(hass: HomeAssistant) -> None:
    result, _ = await _setup_with(hass, " KP-3 ")
    assert result["data"][CONF_USERNAME] == "kp-3"
    assert result["data"][CONF_CUSTOMER_ID] == 3


async def test_an_unknown_login_format_shows_a_field_error(hass: HomeAssistant) -> None:
    for bad in ("abc", "kp-3-1", "kp--3", "3-x", "0"):
        result, mock_test = await _setup_with(hass, bad)
        assert result["type"] is FlowResultType.FORM, bad
        assert result["errors"] == {CONF_CUSTOMER_ID: "invalid_login"}, bad
        mock_test.assert_not_awaited()



async def test_the_same_login_typed_differently_is_configured_only_once(hass: HomeAssistant) -> None:
    """1.5.1: the unique id was the raw field value, so "3", "kp-3" and " KP-3 " became three
    entries with one login -- which then kept disconnecting each other. The unique id is now the
    login itself, and a duplicate is refused before any test connection (which would kick the
    running entry off the broker)."""
    result, _ = await _setup_with(hass, "3")
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["result"].unique_id == "kp-3"

    for same in ("kp-3", " KP-3 ", "Kp-3", 3):
        result, mock_test = await _setup_with(hass, same)
        assert result["type"] is FlowResultType.ABORT, same
        assert result["reason"] == "already_configured", same
        mock_test.assert_not_awaited()
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1

    # An extra login is a different login.
    result, _ = await _setup_with(hass, " KP-3-2")
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["result"].unique_id == "kp-3-2"
    result, _ = await _setup_with(hass, "kp-3-2")
    assert result["type"] is FlowResultType.ABORT
    assert len(hass.config_entries.async_entries(DOMAIN)) == 2


async def test_an_entry_from_before_1_5_1_blocks_the_same_login(hass: HomeAssistant) -> None:
    """An entry whose unique id is still the raw input (not loaded, so not yet rewritten) is
    recognised by its stored login."""
    MockConfigEntry(domain=DOMAIN, data=ENTRY_DATA, unique_id=" KP-3 ", version=2).add_to_hass(hass)

    result, mock_test = await _setup_with(hass, "3")

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    mock_test.assert_not_awaited()
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
