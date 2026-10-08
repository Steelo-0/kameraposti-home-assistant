"""The Kameraposti integration.

Receives Hailo/AI detection events pushed from the Kameraposti backend
over the integration's own outbound MQTT (WSS) connection -- see
mqtt_client.py and docs/kameraposti-ha-mqtt-contract-v1.md. Does not
register or depend on Home Assistant's built-in MQTT integration.
"""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant, callback

from .const import CONF_CUSTOMER_ID, CONF_HOST, DEFAULT_HOST, DOMAIN, USERNAME_TEMPLATE
from .coordinator import KameraportiCoordinator, roster_store
from .login import entry_login

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.IMAGE, Platform.ALARM_CONTROL_PANEL]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Kameraposti from a config entry."""
    _async_set_login_unique_id(hass, entry)

    coordinator = KameraportiCoordinator(
        hass,
        entry,
        host=entry.data.get(CONF_HOST, DEFAULT_HOST),
        customer_id=entry.data[CONF_CUSTOMER_ID],
        username=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
    )

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    # Deliberately does not raise ConfigEntryNotReady on a failed first
    # attempt: this is a cloud_push integration with its own indefinite
    # backoff+reconnect (contract section 9), not a one-shot poll. A
    # broker that is briefly unreachable at Home Assistant startup keeps
    # retrying in the background instead of blocking/aborting setup.
    # Genuinely bad credentials are already rejected by the config flow's
    # own connection test before an entry can even be created.
    await coordinator.async_start()

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry: stop the MQTT client before tearing down entities."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    coordinator: KameraportiCoordinator | None = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    if coordinator is not None:
        await coordinator.async_stop()

    return unload_ok


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Forget the entry's stored camera-roster time (1.5.0)."""
    await roster_store(hass, entry.entry_id).async_remove()


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Version 1 -> 2 (2026-10-05): Kameraposti's own broker.

    Version 1 entries pointed at tailscale2.steels.me (removed) with an
    rk-<id>-<suffix> login. Version 2 uses the chosen service host and the
    account login kp-<customer_id>; the stored password belongs to the old
    login, so the broker rejects it and Home Assistant asks for the new
    password (reauth), which the user copies from Kameraposti's sensor page.
    """
    if entry.version == 1:
        data = {
            **entry.data,
            CONF_HOST: DEFAULT_HOST,
            CONF_USERNAME: USERNAME_TEMPLATE.format(customer_id=entry.data[CONF_CUSTOMER_ID]),
        }
        hass.config_entries.async_update_entry(entry, data=data, version=2)
        _LOGGER.info("Kameraposti entry migrated to version 2 (own broker, login kp-<id>)")
    return True


@callback
def _async_set_login_unique_id(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """1.5.1: the unique id is the entry's canonical login (kp-<id> / kp-<id>-<n>).

    Entries from before 1.5.1 carry the raw setup input ("3", " KP-3 ") as unique id, so
    one login could be added several times. Runs on every setup rather than once as a
    migration, so an entry that could not take its login while a duplicate existed takes
    it once the duplicate is gone. Never deletes anything: of two entries with the same
    login, the older one gets the login as unique id (unless the newer one already has it),
    the newer one is left as it is, and a warning names both.
    """
    login = entry_login(entry.data)
    if login is None:
        return
    same_login = [
        other
        for other in hass.config_entries.async_entries(DOMAIN, include_ignore=False)
        if entry_login(other.data) == login
    ]
    # Oldest first; sorted() is stable, so equal times keep the registry order.
    same_login.sort(key=lambda other: other.created_at)
    oldest = same_login[0] if same_login else entry
    if oldest.entry_id != entry.entry_id:
        _LOGGER.warning(
            "Kameraposti entries '%s' and '%s' use the same MQTT login %s. Kameraposti allows one "
            "connection per login, so while both are enabled they keep disconnecting each other. "
            "Remove one of them, or add one again with its own extra login (%s-<n>) from "
            "Kameraposti's Sensors page.",
            oldest.title,
            entry.title,
            login,
            USERNAME_TEMPLATE.format(customer_id=entry.data[CONF_CUSTOMER_ID]),
        )
        return
    if entry.unique_id == login:
        return
    holder = hass.config_entries.async_entry_for_domain_unique_id(DOMAIN, login)
    if holder is not None and holder.entry_id != entry.entry_id:
        # A newer entry with the same login already has it; the warning comes from its setup.
        return
    _LOGGER.debug("Kameraposti entry '%s' unique id %r -> %r", entry.title, entry.unique_id, login)
    hass.config_entries.async_update_entry(entry, unique_id=login)


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the entry when its data changes (e.g. a password updated via reauth)."""
    await hass.config_entries.async_reload(entry.entry_id)
