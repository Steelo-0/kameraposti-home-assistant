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
