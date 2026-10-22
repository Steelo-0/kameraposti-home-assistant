"""Config flow tests (contract section 18): real-looking connection test,
proper error mapping, no entry saved on failure, reauth path.
"""

from __future__ import annotations

import json
from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import device_registry as dr
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_EXPORTED_ENTITIES, CONF_HOST, DOMAIN
from custom_components.kameraposti.mqtt_client import CannotConnect, InvalidAuth

# Version 2 (2026-10-05): service + account number + password; the login is always kp-<id>.
USER_INPUT = {CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: 3, CONF_PASSWORD: "s3cret-pw"}
ENTRY_DATA = {**USER_INPUT, CONF_USERNAME: "kp-3"}


async def _login_form(hass: HomeAssistant) -> dict:
    """1.18.0: the first form is the pairing code alone; left empty, the login step follows."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    assert (result["type"], result["step_id"]) == (FlowResultType.FORM, "user")
    result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
    assert (result["type"], result["step_id"]) == (FlowResultType.FORM, "login")
    return result


async def test_successful_setup_creates_entry(hass: HomeAssistant) -> None:
    result = await _login_form(hass)

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


async def test_a_new_setup_offers_kameraposti_turva_first(hass: HomeAssistant) -> None:
    """Design review TOP 8 (steelo 2026-10-18): kameraposti.fi has no sensor broker, so a new setup
    defaults to Kameraposti Turva. 2026-10-19 (steelo "srv3 palveluksi"): production Turva is
    turva.secsrv.fi, cam.steels.me stays as the test service; all three stay selectable (1.18.0: on the
    login step)."""
    result = await _login_form(hass)

    host = next(key for key in result["data_schema"].schema if key == CONF_HOST)
    assert host.default() == "turva.secsrv.fi"
    options = result["data_schema"].schema[host].config["options"]
    assert options == ["turva.secsrv.fi", "cam.steels.me", "kameraposti.fi"]


async def test_a_setup_on_the_production_turva_service_connects_there(hass: HomeAssistant) -> None:
    result = await _login_form(hass)

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection",
        new_callable=AsyncMock,
    ) as mock_test:
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_HOST: "turva.secsrv.fi"}
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.CREATE_ENTRY, result2
    assert result2["data"][CONF_HOST] == "turva.secsrv.fi"
    assert mock_test.await_args.kwargs["host"] == "turva.secsrv.fi"


async def test_invalid_auth_shows_error_and_does_not_create_entry(hass: HomeAssistant) -> None:
    result = await _login_form(hass)

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
    result = await _login_form(hass)

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

    result = await _login_form(hass)

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
    result = await _login_form(hass)

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
    result = await _login_form(hass)
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
    assert result["title"] == "Turva (test home)"  # cam.steels.me is a Turva service (1.18.0)
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



async def test_a_device_login_says_it_belongs_to_the_device(hass: HomeAssistant) -> None:
    """steelo 2026-10-15: a Shelly's own login kp-<id>-d<n> was typed in and got "Unknown login".
    It is the device's login (one connection, own topics only), so the error says so and points
    to the Home Assistant login instead -- and nothing connects with it."""
    for device in ("kp-2-d10", " KP-2-D10 ", "kp\u20112\u2011d10"):
        result, mock_test = await _setup_with(hass, device)
        assert result["type"] is FlowResultType.FORM, device
        assert result["errors"] == {CONF_CUSTOMER_ID: "device_login"}, device
        mock_test.assert_not_awaited()


async def test_dashes_and_invisible_characters_from_a_copy_are_tolerated(hass: HomeAssistant) -> None:
    """A phone keyboard or a copy from a web page can bring a non-ASCII dash or a zero-width
    character; the login is still the same login."""
    typed_logins = (("kp\u20113\u20112", "kp-3-2"), ("kp-3-2\u200b", "kp-3-2"), ("\ufeffkp\u20133", "kp-3"))
    for typed, login in typed_logins:
        result, mock_test = await _setup_with(hass, typed)
        assert result["type"] is FlowResultType.CREATE_ENTRY, repr(typed)
        assert result["data"][CONF_USERNAME] == login, repr(typed)
        assert mock_test.await_args.kwargs["username"] == login
        await hass.config_entries.async_remove(result["result"].entry_id)


def test_every_setup_error_has_a_translation() -> None:
    import json
    from pathlib import Path

    folder = Path(__file__).parent.parent / "custom_components" / "kameraposti" / "translations"
    for language in ("en", "fi"):
        errors = json.loads((folder / f"{language}.json").read_text(encoding="utf-8"))["config"]["error"]
        expected = {"invalid_login", "device_login", "invalid_auth", "cannot_connect", "unknown"}
        assert expected <= errors.keys(), language


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


# 1.14.1 (Fable bug hunt H-L2): the service of an existing entry could not be changed. Removing and adding
# the integration again reset the chosen sensors -- and made Kameraposti drop them (H-H1).


def _existing_entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data=ENTRY_DATA,
        options={CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"]},
        unique_id="kp-3",
        version=2,
        minor_version=2,
    )
    entry.add_to_hass(hass)
    return entry


@pytest.fixture
def mock_mqtt_client() -> Generator[None]:
    """The entry is reloaded (set up) after a reconfigure."""
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient") as mock_cls:
        mock_cls.return_value.async_start = AsyncMock()
        mock_cls.return_value.async_stop = AsyncMock()
        yield


async def test_reconfigure_moves_the_entry_to_another_service_and_keeps_the_login_and_options(
    hass: HomeAssistant, mock_mqtt_client: None
) -> None:
    entry = _existing_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reconfigure"
    assert result["description_placeholders"] == {"login": "kp-3"}
    host = next(key for key in result["data_schema"].schema if key == CONF_HOST)
    assert host.default() == "cam.steels.me"
    assert result["data_schema"].schema[host].config["options"] == [
        "turva.secsrv.fi",
        "cam.steels.me",
        "kameraposti.fi",
    ]

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock
    ) as mock_test:
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PASSWORD: "uusi-salasana"}
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.ABORT
    assert result2["reason"] == "reconfigure_successful"
    assert mock_test.await_args.kwargs == {
        "host": "turva.secsrv.fi",
        "customer_id": 3,
        "username": "kp-3",
        "password": "uusi-salasana",
    }
    assert entry.data == {**ENTRY_DATA, CONF_HOST: "turva.secsrv.fi", CONF_PASSWORD: "uusi-salasana"}
    assert entry.unique_id == "kp-3"
    assert entry.options == {CONF_EXPORTED_ENTITIES: ["binary_sensor.kellari"]}
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1


@pytest.mark.parametrize(
    ("error", "reason"), [(InvalidAuth, "invalid_auth"), (CannotConnect, "cannot_connect")]
)
async def test_a_failed_reconfigure_changes_nothing_and_shows_why(
    hass: HomeAssistant, error: type[Exception], reason: str
) -> None:
    entry = _existing_entry(hass)
    result = await entry.start_reconfigure_flow(hass)

    with patch(
        "custom_components.kameraposti.config_flow.async_test_connection",
        new_callable=AsyncMock,
        side_effect=error,
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PASSWORD: "vaara"}
        )

    assert result2["type"] is FlowResultType.FORM
    assert result2["errors"] == {"base": reason}
    host = next(key for key in result2["data_schema"].schema if key == CONF_HOST)
    assert host.default() == "turva.secsrv.fi"  # the service just tried
    assert entry.data == ENTRY_DATA


def _roster(generated_at: str, camera_id: int, name: str) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "generated_at": generated_at,
            "cameras": [{"camera_id": camera_id, "name": name}],
        }
    ).encode()


async def test_after_a_new_service_its_camera_list_applies_even_when_it_is_older(
    hass: HomeAssistant, mock_mqtt_client: None
) -> None:
    """The entry keeps the time of the newest camera list it applied, and an older list changes nothing.
    That time was the old service's: the new service's list, made on another server, was older, so its
    cameras never came and the old service's stayed."""
    entry = _existing_entry(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    hass.data[DOMAIN][entry.entry_id]._handle_message(
        "customers/3/cameras", _roster("2026-10-19T10:00:00+00:00", 12, "Vanha palvelu")
    )
    await hass.async_block_till_done()

    result = await entry.start_reconfigure_flow(hass)
    with patch("custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock):
        await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PASSWORD: "uusi"}
        )
        await hass.async_block_till_done()
    hass.data[DOMAIN][entry.entry_id]._handle_message(
        "customers/3/cameras", _roster("2026-10-18T08:00:00+00:00", 5, "Uusi palvelu")
    )
    await hass.async_block_till_done()

    devices = dr.async_entries_for_config_entry(dr.async_get(hass), entry.entry_id)
    assert "Uusi palvelu" in {device.name for device in devices}
    assert "Vanha palvelu" not in {device.name for device in devices}
    assert await hass.config_entries.async_unload(entry.entry_id)


def test_the_reconfigure_step_has_its_texts() -> None:
    from pathlib import Path

    folder = Path(__file__).parent.parent / "custom_components" / "kameraposti" / "translations"
    for language in ("en", "fi"):
        config = json.loads((folder / f"{language}.json").read_text(encoding="utf-8"))["config"]
        step = config["step"]["reconfigure"]
        assert {"title", "description"} <= step.keys(), language
        assert "{login}" in step["description"], language
        assert step["data"].keys() == {CONF_HOST, CONF_PASSWORD}, language
        assert "reconfigure_successful" in config["abort"], language


async def test_a_camera_list_time_stored_by_1_14_0_does_not_hold_back_the_new_services_list(
    hass: HomeAssistant, mock_mqtt_client: None, hass_storage: dict
) -> None:
    """Fable audit L-3: Reconfigure no longer removes the stored time (the old setup could save it again
    before the reload); the time carries its service, and 1.14.0's, stored without one, gets the old service
    here. The new setup then ignores it, so the new service's older list applies."""
    entry = _existing_entry(hass)
    key = f"{DOMAIN}.roster.{entry.entry_id}"
    hass_storage[key] = {
        "version": 1,
        "minor_version": 1,
        "key": key,
        "data": {"generated_at": "2026-10-19T10:00:00+00:00"},
    }

    result = await entry.start_reconfigure_flow(hass)
    with patch("custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock):
        await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_HOST: "turva.secsrv.fi", CONF_PASSWORD: "uusi"}
        )
        await hass.async_block_till_done()
    hass.data[DOMAIN][entry.entry_id]._handle_message(
        "customers/3/cameras", _roster("2026-10-18T08:00:00+00:00", 5, "Uusi palvelu")
    )
    await hass.async_block_till_done()

    devices = dr.async_entries_for_config_entry(dr.async_get(hass), entry.entry_id)
    assert "Uusi palvelu" in {device.name for device in devices}
    assert hass_storage[key]["data"] == {
        "generated_at": "2026-10-18T08:00:00+00:00",
        "host": "turva.secsrv.fi",
    }
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.parametrize(
    ("host", "login", "title"),
    [
        ("turva.secsrv.fi", "kp-3", "Turva (Mökki)"),
        ("turva.secsrv.fi", "kp-3-2", "Turva (Mökki)"),
        ("cam.steels.me", "kp-3", "Turva (Mökki)"),
        ("kameraposti.fi", "kp-3", "Kameraposti (3)"),
        ("kameraposti.fi", "kp-3-2", "Kameraposti (kp-3-2)"),
    ],
)
async def test_a_turva_entry_is_called_by_the_home_and_a_kameraposti_entry_by_the_login(
    hass: HomeAssistant, host: str, login: str, title: str
) -> None:
    """1.18.0 (Turva onboarding, review F15): a Turva customer saw "Kameraposti (kp-1)"."""
    hass.config.location_name = "Mökki"
    result = await _login_form(hass)
    with patch("custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_HOST: host, CONF_CUSTOMER_ID: login}
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.CREATE_ENTRY
    assert result2["title"] == title


async def test_a_turva_entry_without_a_home_name_is_called_by_the_login(hass: HomeAssistant) -> None:
    hass.config.location_name = "  "
    result = await _login_form(hass)
    with patch("custom_components.kameraposti.config_flow.async_test_connection", new_callable=AsyncMock):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_HOST: "turva.secsrv.fi", CONF_CUSTOMER_ID: "kp-3-2"}
        )
        await hass.async_block_till_done()

    assert result2["title"] == "Turva (kp-3-2)"
