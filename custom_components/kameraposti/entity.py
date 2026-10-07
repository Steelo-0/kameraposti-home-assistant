"""Shared per-camera entity wiring for the sensor and image platforms.

Every platform adds its entities for a camera when the coordinator first
knows it (a roster entry, a detection or a latest photo), exactly once per
camera, and forgets the camera when the roster drops it: the coordinator
removes the camera's device, which removes the entities from the registry
and from Home Assistant, and the camera gets new entities if it comes back.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import Entity
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import KameraportiCoordinator


@callback
def async_setup_camera_entities(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator: KameraportiCoordinator,
    async_add_entities: AddEntitiesCallback,
    entities_for_camera: Callable[[int], Iterable[Entity]],
) -> None:
    """Add entities_for_camera(camera_id) for every camera, now and as cameras appear."""
    known_cameras: set[int] = set()

    @callback
    def _add_camera(camera_id: int) -> None:
        if camera_id in known_cameras:
            # Guards against ever creating a duplicate set of entities
            # for the same camera_id (contract section 15) -- a
            # dispatcher signal could in principle be re-sent, entity
            # creation itself must not be re-triggerable.
            return
        known_cameras.add(camera_id)
        async_add_entities(list(entities_for_camera(camera_id)))

    @callback
    def _forget_camera(camera_id: int) -> None:
        known_cameras.discard(camera_id)

    entry.async_on_unload(async_dispatcher_connect(hass, coordinator.signal_new_camera, _add_camera))
    entry.async_on_unload(async_dispatcher_connect(hass, coordinator.signal_camera_removed, _forget_camera))

    # Entities for cameras the coordinator already knows about (e.g. a
    # message arrived between coordinator start and this platform
    # finishing setup) -- do not wait for a second message to surface them.
    for camera_id in list(coordinator.cameras):
        _add_camera(camera_id)
