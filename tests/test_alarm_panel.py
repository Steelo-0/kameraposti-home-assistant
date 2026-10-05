"""Security system panel (steelo 2026-10-05 "voiko HA purkaa viritykset" ->
"Kyllä, purku koodilla"): an alarm_control_panel mirrors Kameraposti's mode
(customers/<id>/security, retained) and arms/disarms over the integration's
own connection (kameraposti/<id>/turva/set). Disarming needs the code set in
Kameraposti; the backend answers on customers/<id>/security/result.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kameraposti.const import CONF_CUSTOMER_ID, CONF_HOST, DOMAIN
from custom_components.kameraposti.coordinator import KameraportiCoordinator
from custom_components.kameraposti.mqtt_client import ConnectionState

CUSTOMER_ID = 3


def _entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "cam.steels.me", CONF_CUSTOMER_ID: CUSTOMER_ID, "username": "kp-3", "password": "x"},
        version=2,
    )
    entry.add_to_hass(hass)
    return entry


def _client() -> MagicMock:
    client = MagicMock()
    client.async_start = AsyncMock()
    client.async_stop = AsyncMock()
    client.publish.return_value = True
    return client


async def _coordinator(hass: HomeAssistant, client: MagicMock) -> KameraportiCoordinator:
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", return_value=client):
        coordinator = KameraportiCoordinator(
            hass, _entry(hass), host="cam.steels.me", customer_id=CUSTOMER_ID, username="kp-3", password="x"
        )
    await coordinator.async_start()
    coordinator._handle_state_change(ConnectionState.CONNECTED)
    return coordinator


def _sent_command(client: MagicMock) -> tuple[str, dict]:
    topic, payload, retain = client.publish.call_args.args
    assert retain is False
    return topic, json.loads(payload)


async def test_the_retained_state_sets_the_mode(hass: HomeAssistant) -> None:
    coordinator = await _coordinator(hass, _client())
    assert coordinator.security_mode is None

    coordinator._handle_message("customers/3/security", b'{"mode":"home","changed_at":null,"source":"web"}')
    assert coordinator.security_mode == "home"

    coordinator._handle_message("customers/3/security", b'{"mode":"party"}')
    coordinator._handle_message("customers/4/security", b'{"mode":"disarmed"}')
    assert coordinator.security_mode == "home"


async def test_disarming_sends_the_code_and_waits_for_the_result(hass: HomeAssistant) -> None:
    client = _client()
    coordinator = await _coordinator(hass, client)

    task = hass.async_create_task(coordinator.async_set_security_mode("disarmed", "1234"))
    await asyncio.sleep(0)
    topic, command = _sent_command(client)
    assert topic == "kameraposti/3/turva/set"
    assert command["mode"] == "disarmed"
    assert command["code"] == "1234"
    assert len(command["request_id"]) >= 8

    coordinator._handle_message(
        "customers/3/security/result",
        json.dumps(
            {"request_id": command["request_id"], "ok": True, "error": None, "mode": "disarmed"}
        ).encode(),
    )
    await task
    assert coordinator.security_mode == "disarmed"


async def test_arming_sends_no_code(hass: HomeAssistant) -> None:
    client = _client()
    coordinator = await _coordinator(hass, client)

    task = hass.async_create_task(coordinator.async_set_security_mode("away", None))
    await asyncio.sleep(0)
    _, command = _sent_command(client)
    assert "code" not in command
    coordinator._handle_message(
        "customers/3/security/result",
        json.dumps({"request_id": command["request_id"], "ok": True, "mode": "away"}).encode(),
    )
    await task


@pytest.mark.parametrize("error", ["invalid_code", "locked", "code_not_set"])
async def test_a_refused_disarm_raises_a_translated_error(hass: HomeAssistant, error: str) -> None:
    client = _client()
    coordinator = await _coordinator(hass, client)

    task = hass.async_create_task(coordinator.async_set_security_mode("disarmed", "0000"))
    await asyncio.sleep(0)
    _, command = _sent_command(client)
    coordinator._handle_message(
        "customers/3/security/result",
        json.dumps(
            {"request_id": command["request_id"], "ok": False, "error": error, "mode": "away"}
        ).encode(),
    )
    with pytest.raises(HomeAssistantError) as raised:
        await task
    assert raised.value.translation_key == error


async def test_no_answer_or_no_connection_raises(hass: HomeAssistant) -> None:
    client = _client()
    coordinator = await _coordinator(hass, client)

    with patch("custom_components.kameraposti.coordinator.SECURITY_RESULT_TIMEOUT_SECONDS", 0.01):
        with pytest.raises(HomeAssistantError) as raised:
            await coordinator.async_set_security_mode("home", None)
    assert raised.value.translation_key == "no_response"

    client.publish.return_value = False
    with pytest.raises(HomeAssistantError) as raised:
        await coordinator.async_set_security_mode("home", None)
    assert raised.value.translation_key == "not_connected"


async def test_the_panel_follows_the_mode_and_disarms_with_the_code(hass: HomeAssistant) -> None:
    entry = _entry(hass)
    client = _client()
    with patch("custom_components.kameraposti.coordinator.KameraportiMqttClient", return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][entry.entry_id]
    coordinator._handle_state_change(ConnectionState.CONNECTED)

    entity_id = er.async_get(hass).async_get_entity_id(
        "alarm_control_panel", DOMAIN, "kameraposti:3:security"
    )
    assert entity_id is not None
    coordinator._handle_message("customers/3/security", b'{"mode":"away"}')
    await hass.async_block_till_done()
    assert hass.states.get(entity_id).state == "armed_away"

    task = hass.async_create_task(
        hass.services.async_call(
            "alarm_control_panel", "alarm_disarm", {"entity_id": entity_id, "code": "1234"}, blocking=True
        )
    )
    for _ in range(10):
        await asyncio.sleep(0)
        if client.publish.called:
            break
    _, command = _sent_command(client)
    assert command == {"mode": "disarmed", "code": "1234", "request_id": command["request_id"]}
    coordinator._handle_message(
        "customers/3/security/result",
        json.dumps({"request_id": command["request_id"], "ok": True, "mode": "disarmed"}).encode(),
    )
    coordinator._handle_message("customers/3/security", b'{"mode":"disarmed"}')
    await task
    await hass.async_block_till_done()
    assert hass.states.get(entity_id).state == "disarmed"
