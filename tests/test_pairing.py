"""1.17.0 (steelo 2026-10-21 proposal 6, "Paritus suositus"): the setup asks for the pairing code shown on the
Turva page instead of the login and password (which stay as the fallback)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import aiohttp
from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.test_util.aiohttp import AiohttpClientMocker

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_HOST, CONF_PAIRING_CODE, DOMAIN
from custom_components.kameraposti.mqtt_client import CannotConnect

PAIR_URL = "https://turva.secsrv.fi/api/v1/ha/pair"


async def _start(hass: HomeAssistant) -> dict:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    assert (result["type"], result["step_id"]) == (FlowResultType.FORM, "user")
    return result


async def test_a_pairing_code_sets_up_the_entry_with_the_login_turva_gives(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    hass.config.location_name = "Mökki"
    aioclient_mock.post(PAIR_URL, json={"username": "kp-3-4", "password": "paritettu-salasana"})
    form = await _start(hass)

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock
    ) as test:
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: " k7q2-m9xd "}
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {
        CONF_HOST: "turva.secsrv.fi",
        CONF_CUSTOMER_ID: 3,
        CONF_USERNAME: "kp-3-4",
        CONF_PASSWORD: "paritettu-salasana",
    }
    assert result["title"] == "Kameraposti (kp-3-4)"
    assert test.await_args.kwargs["username"] == "kp-3-4"
    (_, url, body, _) = aioclient_mock.mock_calls[0]
    assert str(url) == PAIR_URL
    assert body == {"code": "k7q2-m9xd", "name": "Mökki"}


async def test_a_wrong_or_used_code_is_named_on_the_code_field(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    aioclient_mock.post(PAIR_URL, status=404, json={"error": "invalid_code"})
    form = await _start(hass)

    result = await hass.config_entries.flow.async_configure(
        form["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "ABCD-EFGH"}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_PAIRING_CODE: "invalid_code"}


async def test_a_full_account_and_an_unreachable_service_say_so(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    aioclient_mock.post(PAIR_URL, status=409, json={"error": "logins_full"})
    form = await _start(hass)
    result = await hass.config_entries.flow.async_configure(
        form["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "ABCD-EFGH"}
    )
    assert result["errors"] == {"base": "logins_full"}

    aioclient_mock.clear_requests()
    aioclient_mock.post(PAIR_URL, exc=aiohttp.ClientError())
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "ABCD-EFGH"}
    )
    assert result["errors"] == {"base": "cannot_connect"}

    aioclient_mock.clear_requests()
    aioclient_mock.post(PAIR_URL, json={"unexpected": True})
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "ABCD-EFGH"}
    )
    assert result["errors"] == {"base": "cannot_connect"}


async def test_without_a_code_the_login_and_password_are_needed(hass: HomeAssistant) -> None:
    form = await _start(hass)

    result = await hass.config_entries.flow.async_configure(form["flow_id"], {CONF_HOST: "turva.secsrv.fi"})
    assert result["errors"] == {"base": "code_or_login"}

    with patch("custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_HOST: "turva.secsrv.fi", CONF_CUSTOMER_ID: "kp-3", CONF_PASSWORD: "salasana"},
        )
        await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_USERNAME] == "kp-3"


async def test_the_code_field_comes_before_the_login(hass: HomeAssistant) -> None:
    form = await _start(hass)

    assert [str(key) for key in form["data_schema"].schema] == [
        CONF_HOST,
        CONF_PAIRING_CODE,
        CONF_CUSTOMER_ID,
        CONF_PASSWORD,
    ]


async def test_a_failed_connection_test_after_pairing_reuses_the_login_with_the_same_code(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    """Fable M-2 (21.10): the code is spent once paired; a retry with it must not ask again (a new code would
    leave an unused login behind on the Turva page each time)."""
    aioclient_mock.post(PAIR_URL, json={"username": "kp-3-4", "password": "paritettu-salasana"})
    form = await _start(hass)
    test = AsyncMock(side_effect=[CannotConnect("broker down"), None])

    with patch("custom_components.kameraposti.config_flow.async_test_connection", test):
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "K7Q2-M9XD"}
        )
        assert result["errors"] == {"base": "cannot_connect"}
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "k7q2 m9xd"}
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_USERNAME] == "kp-3-4"
    assert aioclient_mock.call_count == 1


async def test_a_service_without_pairing_is_not_a_wrong_code(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    """Fable L-4: a 404 that is not the service's invalid_code (no endpoint there) reads as no connection."""
    aioclient_mock.post(PAIR_URL, status=404, text="Not Found")
    form = await _start(hass)

    result = await hass.config_entries.flow.async_configure(
        form["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "ABCD-EFGH"}
    )

    assert result["errors"] == {"base": "cannot_connect"}


async def test_a_long_home_assistant_name_is_cut_for_the_login_label(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    hass.config.location_name = "Koti " * 60
    aioclient_mock.post(PAIR_URL, json={"username": "kp-3-4", "password": "x"})
    form = await _start(hass)

    with patch("custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock):
        await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "ABCD-EFGH"}
        )

    (_, _, body, _) = aioclient_mock.mock_calls[0]
    assert len(body["name"]) == 40
