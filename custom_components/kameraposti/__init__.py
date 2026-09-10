"""The Kameraposti integration.

Receives Hailo/AI detection events pushed from the Kameraposti backend
over the integration's own outbound MQTT (WSS) connection -- see
mqtt_client.py and docs/kameraposti-ha-mqtt-contract-v1.md. Does not
register or depend on Home Assistant's built-in MQTT integration.
"""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant

from .const import CONF_CUSTOMER_ID, DOMAIN
from .coordinator import KameraportiCoordinator

PLATFORMS: list[Platform] = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Kameraposti from a config entry."""
    coordinator = KameraportiCoordinator(
        hass,
        entry,
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


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the entry when its data changes (e.g. a password updated via reauth)."""
    await hass.config_entries.async_reload(entry.entry_id)
