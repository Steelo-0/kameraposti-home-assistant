"""Image platform for Kameraposti: each camera's latest photo (1.5.0).

One "Latest photo" entity per camera that has had a photo, on the camera's
device: it is created with the camera's first photo, so a service that
publishes no photos leaves no always-unavailable entity behind (after a
restart it comes back when the retained photo arrives again). Kameraposti
publishes the photo as a retained message with a signed, expiring URL
(customers/<id>/cameras/<camera>/latest); the entity's image_url is that URL
and image_last_updated the photo's captured_at. Home Assistant fetches the
image only when the photo (or its URL) changes -- nothing is polled -- and
the Generic Camera link copied from Kameraposti is no longer needed.

Once created the entity stays: no photo any more (an empty retained message)
or an expired URL makes it unavailable until the next message; an expired URL is never fetched. The
URL itself is never exposed: it is not an attribute and is not logged here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from homeassistant.components.image import ImageEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_track_point_in_utc_time
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .coordinator import KameraportiCoordinator
from .entity import async_setup_camera_entities
from .models import LatestPhoto


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up a latest-photo entity for each camera as soon as it has a photo."""
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][entry.entry_id]

    async_setup_camera_entities(
        hass,
        entry,
        coordinator,
        async_add_entities,
        lambda camera_id: [KameraportiLatestPhotoImage(hass, coordinator, camera_id)],
        requires_photo=True,
    )


class KameraportiLatestPhotoImage(ImageEntity):
    """The camera's latest photo (a video's poster frame), straight from Kameraposti."""

    _attr_has_entity_name = True
    _attr_translation_key = "latest_photo"
    _attr_should_poll = False

    def __init__(self, hass: HomeAssistant, coordinator: KameraportiCoordinator, camera_id: int) -> None:
        # The URL is a signed capability for the photo: only ever send it to a
        # server whose certificate checks out.
        super().__init__(hass, verify_ssl=True)
        self._coordinator = coordinator
        self._camera_id = camera_id
        self._attr_unique_id = f"{DOMAIN}:{coordinator.customer_id}:{camera_id}:latest_photo"
        self._attr_device_info = coordinator.camera_device_info(camera_id)
        # None, not UNDEFINED: this entity always serves a URL, never image().
        self._attr_image_url = None
        self._photo: LatestPhoto | None = None
        self._cancel_expiry: CALLBACK_TYPE | None = None
        self._adopt(self._coordinator_photo())

    def _coordinator_photo(self) -> LatestPhoto | None:
        state = self._coordinator.cameras.get(self._camera_id)
        return state.latest_photo if state is not None else None

    def _adopt(self, photo: LatestPhoto | None) -> bool:
        """Take the coordinator's photo; True if anything visible changed.

        Only a new photo or a new URL (re-signed) drops the cached image so
        Home Assistant fetches again; a new detection for the same photo only
        changes the attributes. The access token is rotated with a new photo,
        so the picture link changes even when two photos share captured_at
        (burst shots in the same second) and the frontend reloads it.
        """
        if photo == self._photo:
            return False
        previous = self._photo
        self._photo = photo
        if (
            photo is None
            or previous is None
            or (photo.photo_id, photo.url) != (previous.photo_id, previous.url)
        ):
            self._cached_image = None
            self._attr_image_url = photo.url if photo is not None else None
            self._attr_image_last_updated = photo.captured_at if photo is not None else None
            self.async_update_token()
        return True

    @property
    def available(self) -> bool:
        """A photo whose signed URL has not expired."""
        photo = self._photo
        return photo is not None and photo.expires_at > dt_util.utcnow()

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        photo = self._photo
        if photo is None:
            return None
        return {
            "label": photo.label,
            "confidence": photo.confidence,
            "is_video": photo.is_video,
            "captured_at": photo.captured_at.isoformat(),
        }

    async def async_image(self) -> bytes | None:
        """The photo's bytes -- never fetched without a photo or with an expired URL."""
        if not self.available:
            return None
        return await super().async_image()

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                self._coordinator.signal_latest_photo(self._camera_id),
                self._handle_latest_photo,
            )
        )
        self.async_on_remove(self._async_cancel_expiry)
        # A photo that arrived between creating this entity and adding it.
        self._adopt(self._coordinator_photo())
        self._async_schedule_expiry()

    @callback
    def _handle_latest_photo(self) -> None:
        if self._adopt(self._coordinator_photo()):
            self._async_schedule_expiry()
            self.async_write_ha_state()

    @callback
    def _async_schedule_expiry(self) -> None:
        # One timer at the URL's expiry (not polling): Kameraposti re-signs
        # well before that, so normally a new message replaces it first.
        self._async_cancel_expiry()
        photo = self._photo
        if photo is not None and photo.expires_at > dt_util.utcnow():
            self._cancel_expiry = async_track_point_in_utc_time(
                self.hass, self._async_expired, photo.expires_at
            )

    @callback
    def _async_cancel_expiry(self) -> None:
        if self._cancel_expiry is not None:
            self._cancel_expiry()
            self._cancel_expiry = None

    @callback
    def _async_expired(self, _now: datetime) -> None:
        self._cancel_expiry = None
        self._cached_image = None
        self.async_write_ha_state()
