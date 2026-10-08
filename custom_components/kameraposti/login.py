"""The account's MQTT login: kp-<customer_id> or an extra login kp-<customer_id>-<n>.

1.5.1: the login is also the config entry's unique id. The broker allows one
connection per login (client id == login), so two entries with the same login
can only disconnect each other; the setup field takes the login in several
spellings ("3", "kp-3", " KP-3 "), and all of them are the same login.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from homeassistant.const import CONF_USERNAME

from .const import CONF_CUSTOMER_ID, EXTRA_USERNAME_TEMPLATE, LOGIN_PATTERN, USERNAME_TEMPLATE


def parse_login(value: object) -> tuple[int, str] | None:
    """(customer_id, canonical login) of a login as typed in the setup form, None if invalid.

    "3", "kp-3", " KP-3 " -> (3, "kp-3"); "kp-3-2" -> (3, "kp-3-2").
    """
    match = LOGIN_PATTERN.match(str(value).strip())
    if match is None:
        return None
    customer_id, login_number = int(match.group(1)), match.group(2)
    if login_number:
        return customer_id, EXTRA_USERNAME_TEMPLATE.format(
            customer_id=customer_id, login_number=int(login_number)
        )
    return customer_id, USERNAME_TEMPLATE.format(customer_id=customer_id)


def entry_login(data: Mapping[str, Any]) -> str | None:
    """The canonical login a config entry connects with (from its data), None if unknown.

    A version 1 entry still has its old rk-<id>-<suffix> login; its migration
    makes that kp-<id>, so that is its login here too.
    """
    parsed = parse_login(data.get(CONF_USERNAME, ""))
    if parsed is not None:
        return parsed[1]
    customer_id = data.get(CONF_CUSTOMER_ID)
    if isinstance(customer_id, bool) or not isinstance(customer_id, int | str):
        return None
    parsed = parse_login(customer_id)
    return USERNAME_TEMPLATE.format(customer_id=parsed[0]) if parsed is not None else None
