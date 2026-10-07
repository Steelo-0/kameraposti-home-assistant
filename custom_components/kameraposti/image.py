"""Image platform for Kameraposti: each camera's latest photo (1.5.0).

One "Latest photo" entity per camera that has had a photo, on the camera's
device: it is created with the camera's first photo, so a service that
publishes no photos leaves no always-unavailable entity behind. After a
restart it is back as soon as the camera is (its registry entry shows it had
a photo), showing the photo when the retained message arrives again. Kameraposti
publishes the photo as a retained message with a signed, expiring URL
(customers/<id>/cameras/<camera>/latest); the entity's image_url is that URL
and image_last_updated the photo's captured_at. Home Assistant fetches the
image only when the photo (or its URL) changes -- nothing is polled -- and
the Generic Camera link copied from Kameraposti is no longer needed.

Once created the entity stays: no photo any more (an empty retained message),
an expired URL or a failed fetch makes it unavailable. A definitive failure
(HTTP 4xx other than 429, not an image) lasts until the next message; a
transient one (timeout, connection error, HTTP 5xx / 429) allows one new
attempt after a cooldown of 5 min, doubling per failure in a row, at most
1 h -- a single timer makes the entity available again, and the next image
request (the frontend's) tries once more. Nothing is polled.

The URL is a signed capability for the photo and is never exposed: not an
attribute, and never logged. That is why the entity fetches the image itself
instead of Home Assistant's ImageEntity URL loader, which logs the whole URL
at ERROR on every failure and fetches (and logs) again on every image
request. A failed fetch logs entity_id, photo_id and the reason only, once,
and a definitive one is not retried for the same URL.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

import httpx
from homeassistant.components.image import Image, ImageEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_call_later, async_track_point_in_utc_time
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .coordinator import KameraportiCoordinator
from .entity import async_setup_camera_entities
from .models import LatestPhoto

_LOGGER = logging.getLogger(__name__)

FETCH_TIMEOUT_SECONDS = 10
# A transient failure (timeout, connection error, HTTP 5xx / 429) allows a new
# attempt after 5 min, doubling per consecutive failure, at most 1 h.
RETRY_FIRST_COOLDOWN = timedelta(minutes=5)
RETRY_MAX_COOLDOWN = timedelta(hours=1)
# JPEG magic bytes, for a response without a Content-Type header.
_JPEG_MAGIC = b"\xff\xd8\xff"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up a latest-photo entity for each camera as soon as it has a photo."""
    coordinator: KameraportiCoordinator = hass.data[DOMAIN][entry.entry_id]
    entity_registry = er.async_get(hass)

    @callback
    def _has_or_had_photo(camera_id: int) -> bool:
        state = coordinator.cameras.get(camera_id)
        if state is not None and state.latest_photo is not None:
            return True
        # Created in an earlier run: once created it stays (unavailable without a
        # photo) instead of a restored "no longer provided" registry orphan.
        unique_id = latest_photo_unique_id(coordinator.customer_id, camera_id)
        return entity_registry.async_get_entity_id(Platform.IMAGE, DOMAIN, unique_id) is not None

    async_setup_camera_entities(
        hass,
        entry,
        coordinator,
        async_add_entities,
        lambda camera_id: [KameraportiLatestPhotoImage(hass, coordinator, camera_id)],
        include=_has_or_had_photo,
    )


def latest_photo_unique_id(customer_id: int, camera_id: int) -> str:
    return f"{DOMAIN}:{customer_id}:{camera_id}:latest_photo"


def retry_cooldown(consecutive_failures: int) -> timedelta:
    """Wait before the next attempt after this many transient failures in a row."""
    doublings = min(max(consecutive_failures - 1, 0), 10)  # 5 min * 2**10 is far past the cap
    return min(RETRY_FIRST_COOLDOWN * 2**doublings, RETRY_MAX_COOLDOWN)


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
        self._attr_unique_id = latest_photo_unique_id(coordinator.customer_id, camera_id)
        self._attr_device_info = coordinator.camera_device_info(camera_id)
        # None, not UNDEFINED: this entity always serves a URL, never image().
        self._attr_image_url = None
        self._photo: LatestPhoto | None = None
        self._cancel_expiry: CALLBACK_TYPE | None = None
        # (photo_id, url) whose fetch failed for good (4xx, not an image):
        # unavailable, not fetched again, until a message brings another one.
        self._failed_fetch: tuple[int, str] | None = None
        # Transient failures of the current photo/URL: unavailable until
        # _retry_after, then one more attempt.
        self._transient_failures = 0
        self._retry_after: datetime | None = None
        self._cancel_retry: CALLBACK_TYPE | None = None
        self._fetch_lock = asyncio.Lock()
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
            self._async_reset_retry()
            self._attr_image_url = photo.url if photo is not None else None
            self._attr_image_last_updated = photo.captured_at if photo is not None else None
            self.async_update_token()
        return True

    @property
    def available(self) -> bool:
        """A photo whose signed URL has not expired and has not failed to load."""
        photo = self._photo
        return (
            photo is not None
            and photo.expires_at > dt_util.utcnow()
            and (photo.photo_id, photo.url) != self._failed_fetch
            and (self._retry_after is None or dt_util.utcnow() >= self._retry_after)
        )

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
        """The photo's bytes: fetched once per photo/URL, never when unavailable."""
        # One fetch at a time: concurrent image requests wait for it and use its result.
        async with self._fetch_lock:
            photo = self._photo
            if photo is None or not self.available:
                return None
            if self._cached_image is not None:
                return self._cached_image.content
            result = await self._async_fetch(photo)
            current = self._photo
            if current is None or (current.photo_id, current.url) != (photo.photo_id, photo.url):
                # The photo changed during the fetch: its result is no longer ours.
                return result.content if isinstance(result, Image) else None
            if isinstance(result, Image):
                self._async_reset_retry()
                self._cached_image = result
                self._attr_content_type = result.content_type
                return result.content
            self._async_fetch_failed(photo, *result)
            return None

    async def _async_fetch(self, photo: LatestPhoto) -> Image | tuple[str, bool]:
        """GET the signed URL: the image, or (reason, transient) -- never the URL itself."""
        try:
            response = await self._client.get(photo.url, timeout=FETCH_TIMEOUT_SECONDS, follow_redirects=True)
        except (httpx.TimeoutException, httpx.NetworkError) as err:
            return type(err).__name__, True
        except (httpx.HTTPError, httpx.InvalidURL) as err:
            return type(err).__name__, False
        if not response.is_success:
            status = response.status_code
            return f"HTTP {status}", status >= 500 or status == 429
        content_type = response.headers.get("content-type")
        if content_type is None and response.content.startswith(_JPEG_MAGIC):
            content_type = "image/jpeg"
        if content_type is not None and content_type.split("/", 1)[0].strip().lower() == "image":
            return Image(content_type=content_type, content=response.content)
        return f"not an image ({content_type or 'no content type'})", False

    @callback
    def _async_fetch_failed(self, photo: LatestPhoto, reason: str, transient: bool) -> None:
        if transient:
            self._transient_failures += 1
            cooldown = retry_cooldown(self._transient_failures)
            self._retry_after = dt_util.utcnow() + cooldown
            self._async_cancel_retry()
            self._cancel_retry = async_call_later(self.hass, cooldown, self._async_retry_allowed)
            then = f"retrying in {int(cooldown.total_seconds() // 60)} min"
        else:
            self._failed_fetch = (photo.photo_id, photo.url)
            then = "waiting for the next photo or link"
        _LOGGER.warning(
            "%s: could not load the latest photo (photo_id=%s): %s; %s",
            self.entity_id,
            photo.photo_id,
            reason,
            then,
        )
        self.async_write_ha_state()

    @callback
    def _async_retry_allowed(self, _now: datetime) -> None:
        # Available again: the next image request makes one more attempt.
        self._cancel_retry = None
        self.async_write_ha_state()

    @callback
    def _async_reset_retry(self) -> None:
        self._transient_failures = 0
        self._retry_after = None
        self._async_cancel_retry()

    @callback
    def _async_cancel_retry(self) -> None:
        if self._cancel_retry is not None:
            self._cancel_retry()
            self._cancel_retry = None

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
        self.async_on_remove(self._async_cancel_retry)
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
