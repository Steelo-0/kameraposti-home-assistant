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
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er

from .config_flow import OPTIONS_FLOW_RELOADS
from .const import CONF_CUSTOMER_ID, CONF_HOST, DEAD_CAMERA_OPTIONS, DEFAULT_HOST, DOMAIN, USERNAME_TEMPLATE
from .coordinator import KameraportiCoordinator, roster_store
from .login import entry_login

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.IMAGE,
    Platform.ALARM_CONTROL_PANEL,
    Platform.BINARY_SENSOR,
]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Kameraposti from a config entry."""
    _async_set_login_unique_id(hass, entry)
    # 1.10.0 (audit M1): the camera options are gone; drop them so a downgrade cannot hide a camera again.
    if any(key in entry.options for key in DEAD_CAMERA_OPTIONS):
        hass.config_entries.async_update_entry(
            entry, options={k: v for k, v in entry.options.items() if k not in DEAD_CAMERA_OPTIONS}
        )

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

    # 1.12.1: Home Assistant reloads after an options save or reauth by itself (config_flow.py);
    # only Home Assistant without OptionsFlowWithReload still needs the listener.
    if not OPTIONS_FLOW_RELOADS:
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

    Version 2.2 (1.14.1, Fable bug hunt H-M1): the entities' unique ids and the devices' identifiers carry
    the entry id instead of the account number (_async_scope_ids_to_entry).
    """
    if entry.version == 1:
        data = {
            **entry.data,
            CONF_HOST: DEFAULT_HOST,
            CONF_USERNAME: USERNAME_TEMPLATE.format(customer_id=entry.data[CONF_CUSTOMER_ID]),
        }
        hass.config_entries.async_update_entry(entry, data=data, version=2)
        _LOGGER.info("Kameraposti entry migrated to version 2 (own broker, login kp-<id>)")
    if entry.version == 2 and entry.minor_version < 2:
        await _async_scope_ids_to_entry(hass, entry)
        hass.config_entries.async_update_entry(entry, minor_version=2)
        _LOGGER.info("Kameraposti entry '%s' migrated to version 2.2 (ids of the entry)", entry.title)
    return True


async def _async_scope_ids_to_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """1.14.1: "kameraposti:<customer_id>:..." -> "kameraposti:<entry_id>:..." for the entry's entities,
    "<customer_id>:..." -> "<entry_id>:..." for its devices.

    Until 1.14.0 two entries with the same account number (the test and the production service, two
    logins of one account) shared these ids: Home Assistant left out the second entry's colliding
    entities and linked both entries to one device. Each entity keeps its registry entry -- entity id,
    name, area, history -- and each device its id, area and the name the user gave it. Nothing is
    removed: an id already taken stays as it is (a warning names it). A device both entries were linked
    to stays with the entry whose entities are on it; an entry with none of its entities there only
    drops its link (that removes nothing) and gets its own device at setup.
    """
    customer_id = entry.data[CONF_CUSTOMER_ID]
    old_prefix, new_prefix = f"{DOMAIN}:{customer_id}:", f"{DOMAIN}:{entry.entry_id}:"
    entity_registry = er.async_get(hass)

    @callback
    def _entry_scoped(registry_entry: er.RegistryEntry) -> dict[str, str] | None:
        unique_id = registry_entry.unique_id
        if registry_entry.platform != DOMAIN or not unique_id.startswith(old_prefix):
            return None
        new_unique_id = new_prefix + unique_id[len(old_prefix) :]
        if entity_registry.async_get_entity_id(registry_entry.domain, DOMAIN, new_unique_id) is not None:
            _LOGGER.warning(
                "Kameraposti: %s keeps its unique id %s (%s is taken)",
                registry_entry.entity_id,
                unique_id,
                new_unique_id,
            )
            return None
        return {"new_unique_id": new_unique_id}

    await er.async_migrate_entries(hass, entry.entry_id, _entry_scoped)

    device_registry = dr.async_get(hass)
    old_prefix, new_prefix = f"{customer_id}:", f"{entry.entry_id}:"
    for device in dr.async_entries_for_config_entry(device_registry, entry.entry_id):
        old = {
            (domain, identifier)
            for domain, identifier in device.identifiers
            if domain == DOMAIN and identifier.startswith(old_prefix)
        }
        if not old:
            continue
        with_entities = {
            registry_entry.config_entry_id
            for registry_entry in er.async_entries_for_device(
                entity_registry, device.id, include_disabled_entities=True
            )
        }
        if entry.entry_id not in with_entities and len(device.config_entries) > 1:
            # The other entry's (its migration takes the device): dropping the link removes nothing here.
            device_registry.async_update_device(device.id, remove_config_entry_id=entry.entry_id)
            continue
        new = {(DOMAIN, new_prefix + identifier[len(old_prefix) :]) for _domain, identifier in old}
        holder = device_registry.async_get_device(identifiers=new)
        if holder is not None and holder.id != device.id:
            _LOGGER.warning(
                "Kameraposti: device '%s' keeps its identifiers %s (%s are taken)",
                device.name_by_user or device.name,
                sorted(identifier for _domain, identifier in old),
                sorted(identifier for _domain, identifier in new),
            )
            continue
        device_registry.async_update_device(device.id, new_identifiers=(device.identifiers - old) | new)
        # Whichever entry migrates first: another Kameraposti entry with none of its entities here drops
        # its link too (removing nothing), so it does not stay on this entry's device.
        for other_id in device.config_entries - with_entities - {entry.entry_id}:
            other = hass.config_entries.async_get_entry(other_id)
            if other is not None and other.domain == DOMAIN:
                device_registry.async_update_device(device.id, remove_config_entry_id=other_id)


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
    """Reload the entry when it changes -- only on Home Assistant without OptionsFlowWithReload."""
    await hass.config_entries.async_reload(entry.entry_id)
