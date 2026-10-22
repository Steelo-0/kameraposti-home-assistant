"""1.17.0 (steelo 2026-10-21 proposal 6, "Paritus suositus"): the setup asks for the pairing code shown on the
Turva page instead of the login and password (which stay as the fallback).

1.18.0 (Turva onboarding, steelo 2026-10-22): the first form is the code alone, for turva.secsrv.fi; an empty
code opens the login step, where the service is chosen and its code or the login and password are entered."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiohttp
import voluptuous as vol
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


async def _login_step(hass: HomeAssistant) -> dict:
    result = await hass.config_entries.flow.async_configure((await _start(hass))["flow_id"], {})
    assert (result["type"], result["step_id"]) == (FlowResultType.FORM, "login")
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
            form["flow_id"], {CONF_PAIRING_CODE: " k7q2-m9xd "}
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
        form["flow_id"], {CONF_PAIRING_CODE: "ABCD-EFGH"}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_PAIRING_CODE: "invalid_code"}


async def test_a_full_account_and_an_unreachable_service_say_so(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    aioclient_mock.post(PAIR_URL, status=409, json={"error": "logins_full"})
    form = await _start(hass)
    result = await hass.config_entries.flow.async_configure(
        form["flow_id"], {CONF_PAIRING_CODE: "ABCD-EFGH"}
    )
    assert result["errors"] == {"base": "logins_full"}

    aioclient_mock.clear_requests()
    aioclient_mock.post(PAIR_URL, exc=aiohttp.ClientError())
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_PAIRING_CODE: "ABCD-EFGH"}
    )
    assert result["errors"] == {"base": "cannot_connect"}

    aioclient_mock.clear_requests()
    aioclient_mock.post(PAIR_URL, json={"unexpected": True})
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_PAIRING_CODE: "ABCD-EFGH"}
    )
    assert result["errors"] == {"base": "cannot_connect"}


async def test_without_a_code_the_login_and_password_are_needed(hass: HomeAssistant) -> None:
    form = await _login_step(hass)

    result = await hass.config_entries.flow.async_configure(form["flow_id"], {CONF_HOST: "turva.secsrv.fi"})
    assert (result["step_id"], result["errors"]) == ("login", {"base": "code_or_login"})

    with patch("custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_HOST: "turva.secsrv.fi", CONF_CUSTOMER_ID: "kp-3", CONF_PASSWORD: "salasana"},
        )
        await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_USERNAME] == "kp-3"


async def test_the_first_form_is_the_code_alone_and_it_may_be_left_empty(hass: HomeAssistant) -> None:
    """1.18.0: one field; the service is Turva's (no choice shown), and an empty code opens the login step."""
    form = await _start(hass)

    [field] = list(form["data_schema"].schema)
    assert str(field) == CONF_PAIRING_CODE
    assert isinstance(field, vol.Optional)
    assert form["errors"] == {}


async def test_the_login_step_offers_every_service_turva_first_and_the_code_before_the_login(
    hass: HomeAssistant,
) -> None:
    form = await _login_step(hass)

    assert [str(key) for key in form["data_schema"].schema] == [
        CONF_HOST,
        CONF_PAIRING_CODE,
        CONF_CUSTOMER_ID,
        CONF_PASSWORD,
    ]
    host = next(key for key in form["data_schema"].schema if key == CONF_HOST)
    assert host.default() == "turva.secsrv.fi"
    assert form["data_schema"].schema[host].config["options"] == [
        "turva.secsrv.fi",
        "cam.steels.me",
        "kameraposti.fi",
    ]


async def test_a_code_from_another_service_is_paired_there_on_the_login_step(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    """The code does not carry its service: a CAM Turva (cam.steels.me) code is entered with that service."""
    aioclient_mock.post("https://cam.steels.me/api/v1/ha/pair", json={"username": "kp-7-2", "password": "pw"})
    form = await _login_step(hass)

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock
    ) as test:
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_HOST: "cam.steels.me", CONF_PAIRING_CODE: "ABCD-EFGH"}
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {
        CONF_HOST: "cam.steels.me",
        CONF_CUSTOMER_ID: 7,
        CONF_USERNAME: "kp-7-2",
        CONF_PASSWORD: "pw",
    }
    assert test.await_args.kwargs["host"] == "cam.steels.me"
    [(_, url, _, _)] = aioclient_mock.mock_calls
    assert str(url) == "https://cam.steels.me/api/v1/ha/pair"


async def test_a_kameraposti_login_connects_on_the_login_step(hass: HomeAssistant) -> None:
    """A Kameraposti (kameraposti.fi) customer has no pairing code: the service and the login."""
    form = await _login_step(hass)

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock
    ) as test:
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"],
            {CONF_HOST: "kameraposti.fi", CONF_CUSTOMER_ID: "kp-5", CONF_PASSWORD: "salasana"},
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {
        CONF_HOST: "kameraposti.fi",
        CONF_CUSTOMER_ID: 5,
        CONF_USERNAME: "kp-5",
        CONF_PASSWORD: "salasana",
    }
    assert test.await_args.kwargs["host"] == "kameraposti.fi"


async def test_a_wrong_code_on_the_login_step_keeps_the_service_just_tried(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    aioclient_mock.post("https://cam.steels.me/api/v1/ha/pair", status=404, json={"error": "invalid_code"})
    form = await _login_step(hass)

    result = await hass.config_entries.flow.async_configure(
        form["flow_id"], {CONF_HOST: "cam.steels.me", CONF_PAIRING_CODE: "ABCD-EFGH"}
    )

    assert (result["step_id"], result["errors"]) == ("login", {CONF_PAIRING_CODE: "invalid_code"})
    host = next(key for key in result["data_schema"].schema if key == CONF_HOST)
    assert host.default() == "cam.steels.me"


async def test_a_code_paired_on_the_first_form_is_reused_on_the_login_step(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    """Fable M-2 across the steps: the login a code gave is remembered for the whole flow (service + code)."""
    aioclient_mock.post(PAIR_URL, json={"username": "kp-3-4", "password": "paritettu-salasana"})
    form = await _start(hass)
    test = AsyncMock(side_effect=[CannotConnect("broker down"), None])

    with patch("custom_components.kameraposti.config_flow.async_test_connection", test):
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_PAIRING_CODE: "K7Q2-M9XD"}
        )
        assert (result["step_id"], result["errors"]) == ("user", {"base": "cannot_connect"})
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
        assert result["step_id"] == "login"
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PAIRING_CODE: "k7q2-m9xd"}
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_USERNAME] == "kp-3-4"
    assert aioclient_mock.call_count == 1


async def test_a_login_the_service_gives_that_is_not_one_is_an_unknown_error(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    """The code form has no login field to name: a broken answer is a general error, never a field error."""
    aioclient_mock.post(PAIR_URL, json={"username": "kp-3-d1", "password": "x"})
    form = await _start(hass)

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock
    ) as test:
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_PAIRING_CODE: "ABCD-EFGH"}
        )

    assert (result["step_id"], result["errors"]) == ("user", {"base": "unknown"})
    test.assert_not_awaited()


def test_both_steps_have_their_texts() -> None:
    """1.18.0: the code form's description names no button of the web page (it changes)."""
    folder = Path(__file__).parent.parent / "custom_components" / "kameraposti" / "translations"
    expected = {
        "fi": "Syötä Turva-sivulla näkyvä parituskoodi (esim. K7Q2-M9XD).",
        "en": "Enter the pairing code shown on the Turva page (e.g. K7Q2-M9XD).",
    }
    for language in ("en", "fi"):
        steps = json.loads((folder / f"{language}.json").read_text(encoding="utf-8"))["config"]["step"]
        assert steps["user"]["description"] == expected[language], language
        assert steps["user"]["data"].keys() == {CONF_PAIRING_CODE}, language
        assert steps["user"]["data_description"].keys() == {CONF_PAIRING_CODE}, language
        assert {"title", "description"} <= steps["login"].keys(), language
        login_fields = {CONF_HOST, CONF_PAIRING_CODE, CONF_CUSTOMER_ID, CONF_PASSWORD}
        assert steps["login"]["data"].keys() == login_fields, language
    assert "Näytä parituskoodi" not in json.dumps(
        json.loads((folder / "fi.json").read_text(encoding="utf-8"))["config"]["step"], ensure_ascii=False
    )


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
            form["flow_id"], {CONF_PAIRING_CODE: "K7Q2-M9XD"}
        )
        assert result["errors"] == {"base": "cannot_connect"}
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_PAIRING_CODE: "k7q2 m9xd"}
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
        form["flow_id"], {CONF_PAIRING_CODE: "ABCD-EFGH"}
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
            form["flow_id"], {CONF_PAIRING_CODE: "ABCD-EFGH"}
        )

    (_, _, body, _) = aioclient_mock.mock_calls[0]
    assert len(body["name"]) == 40
