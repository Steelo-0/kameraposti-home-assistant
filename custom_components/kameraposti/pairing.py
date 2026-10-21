"""Pairing with a code (1.17.0, steelo 2026-10-21 proposal 6, "Paritus suositus").

The Turva page shows a code (8 characters, 15 minutes, single use); the setup sends it to the chosen service
(POST https://<host>/api/v1/ha/pair) and gets this Home Assistant's own login and password back. The code is
never stored. The login and password stay as the fallback.
"""

from __future__ import annotations

import asyncio
from typing import Final

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .mqtt_client import CannotConnect

PAIR_PATH: Final = "/api/v1/ha/pair"
PAIR_TIMEOUT_SECONDS: Final = 15
# The service keeps 40 characters of Home Assistant's name as the login's label (Fable L-4: a longer name
# was refused).
NAME_MAX_LENGTH: Final = 40


class InvalidPairingCode(Exception):
    """The code is wrong, expired or used (404 invalid_code)."""


class PairingLoginsFull(Exception):
    """The account has its maximum number of extra logins (409 logins_full)."""


async def async_pair(hass: HomeAssistant, host: str, code: str) -> tuple[str, str]:
    """The login and password the service gives for ``code``; CannotConnect when it cannot be asked."""
    session = async_get_clientsession(hass)
    try:
        async with asyncio.timeout(PAIR_TIMEOUT_SECONDS):
            response = await session.post(
                f"https://{host}{PAIR_PATH}",
                json={"code": code, "name": (hass.config.location_name or "")[:NAME_MAX_LENGTH]},
            )
            if response.status == 422:
                raise InvalidPairingCode
            if response.status == 404:
                # Fable L-4: only the service's own answer is a wrong code; a 404 without it (a service or
                # release without pairing) is no connection.
                body = await response.json(content_type=None)
                if isinstance(body, dict) and body.get("error") == "invalid_code":
                    raise InvalidPairingCode
                raise CannotConnect("pairing not found on the service")
            if response.status == 409:
                raise PairingLoginsFull
            if response.status != 200:
                raise CannotConnect(f"pairing answered {response.status}")
            data = await response.json(content_type=None)
    except (aiohttp.ClientError, TimeoutError, ValueError) as err:
        raise CannotConnect(str(err)) from err
    username = data.get("username") if isinstance(data, dict) else None
    password = data.get("password") if isinstance(data, dict) else None
    if not isinstance(username, str) or not username or not isinstance(password, str) or not password:
        raise CannotConnect("pairing answer without a login")
    return username, password


def normalize_code(code: str) -> str:
    """The code as the service stores it: upper case, without spaces and dashes."""
    return "".join(code.split()).replace("-", "").upper()
