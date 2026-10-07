"""Payload parsing and validation for the Kameraposti MQTT contract.

Detections (V1), and since 1.5.0 the account's camera roster and each
camera's latest photo.

Deliberately pure Python (no Home Assistant imports) -- this is the one
module doing the actual contract enforcement (topic shape, payload shape,
QoS/tenant-adjacent identifier checks), so it needs to be trivially unit
testable in isolation, fast, and free of any MQTT-thread/event-loop
concerns.

Every rejection raises DetectionRejected (RosterRejected,
LatestPhotoRejected for the 1.5.0 payloads) and NOTHING else -- callers
(coordinator.py) catch exactly this one exception type, log it at
debug/warning, and continue. A malformed message is expected, routine
input, not a bug: it must never crash the integration, force a
reconnect, or block the Home Assistant event loop (contract section 7).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit

from .const import (
    LATEST_PHOTO_SCHEMA_VERSION,
    LATEST_PHOTO_TOPIC_PATTERN,
    ROSTER_SCHEMA_VERSION,
    ROSTER_TOPIC_PATTERN,
    SCHEMA_VERSION_SUPPORTED,
    TOPIC_PATTERN,
)


class DetectionRejected(Exception):
    """One incoming MQTT message failed contract validation. Ignore it."""


@dataclass(frozen=True, slots=True)
class Detection:
    """One validated, contract-compliant detection event."""

    event_id: str
    customer_id: int
    camera_id: int
    label: str
    confidence: float
    timestamp: datetime


def parse_topic(topic: str) -> tuple[int, int]:
    """Extract (customer_id, camera_id) from a topic string.

    Raises DetectionRejected if the topic does not match
    ``customers/{customer_id}/detections/{camera_id}`` exactly (contract
    section 6, steps 1/3/4).
    """
    match = TOPIC_PATTERN.match(topic)
    if match is None:
        raise DetectionRejected(
            f"topic does not match customers/{{customer_id}}/detections/{{camera_id}}: {topic!r}"
        )
    return int(match.group("customer_id")), int(match.group("camera_id"))


def parse_detection(topic: str, payload: bytes | str, *, expected_customer_id: int) -> Detection:
    """Parse and fully validate one MQTT message into a Detection.

    Implements contract sections 6 ("Topic validation") and 7 ("Payload
    validation") in full. Raises DetectionRejected for every violation
    listed there -- callers treat all of them identically (ignore this
    message, log why, keep the connection alive).

    Unknown/extra JSON fields are silently ignored (forward-compatibility
    requirement, contract section 7) -- this function only ever reads the
    six named keys it needs.
    """
    topic_customer_id, topic_camera_id = parse_topic(topic)

    if topic_customer_id != expected_customer_id:
        # Defensive check (contract section 23): the broker's dynsec ACL
        # should make this unreachable, but the client validates the
        # namespace itself too rather than trusting transport-layer auth
        # alone.
        raise DetectionRejected(
            f"topic customer_id {topic_customer_id} does not match configured "
            f"customer_id {expected_customer_id} -- cross-tenant topic, ignoring"
        )

    try:
        data = json.loads(payload)
    except (ValueError, TypeError) as err:
        raise DetectionRejected(f"invalid JSON payload: {err}") from err

    if not isinstance(data, dict):
        raise DetectionRejected(f"payload JSON is not an object: {type(data).__name__}")

    schema_version = data.get("schema_version")
    if schema_version != SCHEMA_VERSION_SUPPORTED:
        raise DetectionRejected(f"unsupported schema_version: {schema_version!r}")

    event_id = data.get("event_id")
    if not isinstance(event_id, str) or event_id == "":
        raise DetectionRejected(f"event_id missing or not a string: {event_id!r}")

    payload_camera_id = data.get("camera_id")
    if not isinstance(payload_camera_id, int) or isinstance(payload_camera_id, bool):
        raise DetectionRejected(f"camera_id missing or not an integer: {payload_camera_id!r}")

    if payload_camera_id != topic_camera_id:
        raise DetectionRejected(
            f"payload camera_id {payload_camera_id} does not match topic camera_id {topic_camera_id}"
        )

    label = data.get("label")
    if not isinstance(label, str) or label.strip() == "":
        raise DetectionRejected(f"label missing or empty: {label!r}")

    confidence = data.get("confidence")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        raise DetectionRejected(f"confidence missing or not numeric: {confidence!r}")
    confidence = float(confidence)
    if not (0.0 <= confidence <= 1.0):
        raise DetectionRejected(f"confidence out of range [0.0, 1.0]: {confidence}")

    timestamp_raw = data.get("timestamp")
    if not isinstance(timestamp_raw, str) or timestamp_raw == "":
        raise DetectionRejected(f"timestamp missing or not a string: {timestamp_raw!r}")
    try:
        # Python's fromisoformat (3.11+, which HA's minimum supported
        # runtime already exceeds) accepts a 'Z' suffix and arbitrary
        # numeric offsets -- do NOT assume Europe/Helsinki or any other
        # fixed offset, the contract explicitly forbids that assumption.
        timestamp = datetime.fromisoformat(timestamp_raw)
    except ValueError as err:
        raise DetectionRejected(f"timestamp is not valid ISO-8601: {timestamp_raw!r}") from err
    if timestamp.tzinfo is None:
        raise DetectionRejected(f"timestamp is not timezone-aware: {timestamp_raw!r}")

    return Detection(
        event_id=event_id,
        customer_id=topic_customer_id,
        camera_id=topic_camera_id,
        # Backend already normalizes to lowercase, but the integration
        # never trusts the wire blindly -- normalize defensively too
        # rather than rejecting non-lowercase input outright (contract:
        # "arbitrary lowercase label", no whitelist, no strict-case
        # rejection requirement).
        label=label.strip().lower(),
        confidence=confidence,
        timestamp=timestamp,
    )


# -- camera roster + latest photo (1.5.0) -------------------------------------


class RosterRejected(Exception):
    """A camera roster message failed validation. Ignore it (keep the cameras as they are)."""


class LatestPhotoRejected(Exception):
    """A latest-photo message failed validation. Ignore it (keep the photo as it is)."""


@dataclass(frozen=True, slots=True)
class RosterCamera:
    """One camera on the account's roster. name None = not named, use the default."""

    camera_id: int
    name: str | None


@dataclass(frozen=True, slots=True)
class CameraRoster:
    """The account's authoritative camera list (retained customers/<id>/cameras)."""

    generated_at: datetime
    cameras: tuple[RosterCamera, ...]


@dataclass(frozen=True, slots=True)
class LatestPhoto:
    """One camera's latest photo (retained customers/<id>/cameras/<camera>/latest).

    url is a signed, expiring capability for the image itself: it is never
    logged or exposed as an entity attribute.
    """

    camera_id: int
    photo_id: int
    captured_at: datetime
    is_video: bool
    url: str
    expires_at: datetime
    label: str | None
    confidence: float | None


def _is_int(value: object) -> bool:
    # bool is a subclass of int -- True/False must never pass as an id.
    return isinstance(value, int) and not isinstance(value, bool)


def _aware_datetime(value: object, field: str, rejected: type[Exception]) -> datetime:
    if not isinstance(value, str) or value == "":
        raise rejected(f"{field} missing or not a string: {value!r}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as err:
        raise rejected(f"{field} is not valid ISO-8601: {value!r}") from err
    if parsed.tzinfo is None:
        raise rejected(f"{field} is not timezone-aware: {value!r}")
    return parsed


def _json_object(payload: bytes | str, rejected: type[Exception]) -> dict:
    try:
        data = json.loads(payload)
    except (ValueError, TypeError) as err:
        raise rejected(f"invalid JSON payload: {err}") from err
    if not isinstance(data, dict):
        raise rejected(f"payload JSON is not an object: {type(data).__name__}")
    return data


def _check_schema_version(data: dict, supported: int, rejected: type[Exception]) -> None:
    schema_version = data.get("schema_version")
    if not _is_int(schema_version) or schema_version != supported:
        raise rejected(f"unsupported schema_version: {schema_version!r}")


def parse_roster(topic: str, payload: bytes | str, *, expected_customer_id: int) -> CameraRoster | None:
    """Parse the account's camera roster.

    Returns None for an empty (cleared) retained message: there is no roster,
    which is not the same as a roster with no cameras. Any invalid camera
    rejects the WHOLE roster -- the roster decides which cameras are removed,
    so a partially read list must never be acted on. Unknown fields are
    ignored (forward compatibility, as in detections).
    """
    match = ROSTER_TOPIC_PATTERN.match(topic)
    if match is None:
        raise RosterRejected(f"topic does not match customers/{{customer_id}}/cameras: {topic!r}")
    topic_customer_id = int(match.group("customer_id"))
    if topic_customer_id != expected_customer_id:
        raise RosterRejected(
            f"topic customer_id {topic_customer_id} does not match configured "
            f"customer_id {expected_customer_id} -- cross-tenant topic, ignoring"
        )

    if len(payload) == 0:
        return None

    data = _json_object(payload, RosterRejected)
    _check_schema_version(data, ROSTER_SCHEMA_VERSION, RosterRejected)
    generated_at = _aware_datetime(data.get("generated_at"), "generated_at", RosterRejected)

    raw_cameras = data.get("cameras")
    if not isinstance(raw_cameras, list):
        raise RosterRejected(f"cameras missing or not a list: {type(raw_cameras).__name__}")

    cameras: list[RosterCamera] = []
    seen: set[int] = set()
    for index, raw in enumerate(raw_cameras):
        if not isinstance(raw, dict):
            raise RosterRejected(f"cameras[{index}] is not an object: {type(raw).__name__}")
        camera_id = raw.get("camera_id")
        if not _is_int(camera_id) or camera_id < 1:
            raise RosterRejected(
                f"cameras[{index}].camera_id missing or not a positive integer: {camera_id!r}"
            )
        if camera_id in seen:
            raise RosterRejected(f"cameras[{index}].camera_id {camera_id} is a duplicate")
        seen.add(camera_id)
        name = raw.get("name")
        if not isinstance(name, str):
            raise RosterRejected(f"cameras[{index}].name missing or not a string: {name!r}")
        cameras.append(RosterCamera(camera_id=camera_id, name=name.strip() or None))

    return CameraRoster(generated_at=generated_at, cameras=tuple(cameras))


def parse_latest_photo(
    topic: str, payload: bytes | str, *, expected_customer_id: int
) -> tuple[int, LatestPhoto | None]:
    """Parse one camera's latest-photo message into (camera_id, photo).

    photo is None for an empty (cleared) retained message: the camera has no
    photo (or was removed). The image URL must be https -- Home Assistant
    fetches it, so nothing else (plain http, other schemes) is accepted.
    Unknown fields are ignored.
    """
    match = LATEST_PHOTO_TOPIC_PATTERN.match(topic)
    if match is None:
        raise LatestPhotoRejected(
            f"topic does not match customers/{{customer_id}}/cameras/{{camera_id}}/latest: {topic!r}"
        )
    topic_customer_id = int(match.group("customer_id"))
    topic_camera_id = int(match.group("camera_id"))
    if topic_customer_id != expected_customer_id:
        raise LatestPhotoRejected(
            f"topic customer_id {topic_customer_id} does not match configured "
            f"customer_id {expected_customer_id} -- cross-tenant topic, ignoring"
        )

    if len(payload) == 0:
        return topic_camera_id, None

    data = _json_object(payload, LatestPhotoRejected)
    _check_schema_version(data, LATEST_PHOTO_SCHEMA_VERSION, LatestPhotoRejected)

    camera_id = data.get("camera_id")
    if not _is_int(camera_id):
        raise LatestPhotoRejected(f"camera_id missing or not an integer: {camera_id!r}")
    if camera_id != topic_camera_id:
        raise LatestPhotoRejected(
            f"payload camera_id {camera_id} does not match topic camera_id {topic_camera_id}"
        )

    photo_id = data.get("photo_id")
    if not _is_int(photo_id) or photo_id < 1:
        raise LatestPhotoRejected(f"photo_id missing or not a positive integer: {photo_id!r}")

    captured_at = _aware_datetime(data.get("captured_at"), "captured_at", LatestPhotoRejected)
    expires_at = _aware_datetime(data.get("expires_at"), "expires_at", LatestPhotoRejected)

    is_video = data.get("is_video")
    if not isinstance(is_video, bool):
        raise LatestPhotoRejected(f"is_video missing or not a boolean: {is_video!r}")

    url = data.get("url")
    if not isinstance(url, str):
        raise LatestPhotoRejected(f"url missing or not a string: {type(url).__name__}")
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname:
        # Never echo the URL itself: it is a signed capability for the photo.
        raise LatestPhotoRejected(f"url is not an https URL (scheme {parts.scheme!r})")

    label: str | None = None
    confidence: float | None = None
    detection = data.get("detection")
    if detection is not None:
        if not isinstance(detection, dict):
            raise LatestPhotoRejected(f"detection is not an object or null: {type(detection).__name__}")
        raw_label = detection.get("label")
        if not isinstance(raw_label, str) or raw_label.strip() == "":
            raise LatestPhotoRejected(f"detection label missing or empty: {raw_label!r}")
        raw_confidence = detection.get("confidence")
        if isinstance(raw_confidence, bool) or not isinstance(raw_confidence, (int, float)):
            raise LatestPhotoRejected(f"detection confidence missing or not numeric: {raw_confidence!r}")
        if not (0.0 <= float(raw_confidence) <= 1.0):
            raise LatestPhotoRejected(f"detection confidence out of range [0.0, 1.0]: {raw_confidence}")
        label = raw_label.strip().lower()
        confidence = float(raw_confidence)

    return topic_camera_id, LatestPhoto(
        camera_id=topic_camera_id,
        photo_id=photo_id,
        captured_at=captured_at,
        is_video=is_video,
        url=url,
        expires_at=expires_at,
        label=label,
        confidence=confidence,
    )
