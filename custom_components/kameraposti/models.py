"""Detection payload parsing and validation for the Kameraposti V1 MQTT contract.

Deliberately pure Python (no Home Assistant imports) -- this is the one
module doing the actual contract enforcement (topic shape, payload shape,
QoS/tenant-adjacent identifier checks), so it needs to be trivially unit
testable in isolation, fast, and free of any MQTT-thread/event-loop
concerns.

Every rejection raises DetectionRejected and NOTHING else -- callers
(coordinator.py) catch exactly this one exception type, log it at
debug/warning, and continue. A malformed message is expected, routine
input, not a bug: it must never crash the integration, force a
reconnect, or block the Home Assistant event loop (contract section 7).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from .const import SCHEMA_VERSION_SUPPORTED, TOPIC_PATTERN


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
