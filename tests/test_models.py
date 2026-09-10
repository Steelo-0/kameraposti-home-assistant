"""Unit tests for detection payload parsing/validation (contract sections 6-7).

Pure Python, no Home Assistant fixtures needed -- these are the fast,
isolated tests for the actual contract-enforcement logic.
"""

from __future__ import annotations

import json

import pytest

from custom_components.kameraposti.models import DetectionRejected, parse_detection

VALID_PAYLOAD = {
    "schema_version": 1,
    "event_id": "01K0000000000000000000001",
    "camera_id": 16,
    "label": "animal",
    "confidence": 0.972,
    "timestamp": "2026-09-09T18:12:42+00:00",
}


def _payload(**overrides: object) -> bytes:
    return json.dumps({**VALID_PAYLOAD, **overrides}).encode()


def test_valid_detection_parses_correctly() -> None:
    detection = parse_detection(
        "customers/3/detections/16",
        _payload(),
        expected_customer_id=3,
    )

    assert detection.event_id == "01K0000000000000000000001"
    assert detection.customer_id == 3
    assert detection.camera_id == 16
    assert detection.label == "animal"
    assert detection.confidence == 0.972
    assert detection.timestamp.isoformat() == "2026-09-09T18:12:42+00:00"


def test_unknown_fields_are_ignored() -> None:
    """J. Unknown field -- message is still accepted (forward compatibility)."""
    detection = parse_detection(
        "customers/3/detections/16",
        _payload(future_field="anything", another_one={"nested": True}),
        expected_customer_id=3,
    )

    assert detection.label == "animal"


@pytest.mark.parametrize(
    "label_in,label_out",
    [("Animal", "animal"), ("PERSON", "person"), ("Moose", "moose")],
)
def test_label_is_normalised_to_lowercase_and_not_whitelisted(label_in: str, label_out: str) -> None:
    """Arbitrary lowercase labels must be accepted -- no hardcoded whitelist."""
    detection = parse_detection(
        "customers/3/detections/16",
        _payload(label=label_in),
        expected_customer_id=3,
    )

    assert detection.label == label_out


def test_wrong_tenant_topic_is_rejected() -> None:
    """D. Wrong tenant topic -- ignore."""
    with pytest.raises(DetectionRejected, match="customer_id"):
        parse_detection(
            "customers/4/detections/16",
            _payload(camera_id=16),
            expected_customer_id=3,
        )


def test_camera_id_mismatch_between_topic_and_payload_is_rejected() -> None:
    """E. camera_id mismatch -- ignore."""
    with pytest.raises(DetectionRejected, match="camera_id"):
        parse_detection(
            "customers/3/detections/16",
            _payload(camera_id=17),
            expected_customer_id=3,
        )


def test_invalid_json_is_rejected() -> None:
    """F. Invalid JSON -- ignore, never crash."""
    with pytest.raises(DetectionRejected, match="JSON"):
        parse_detection(
            "customers/3/detections/16",
            b"{not valid json",
            expected_customer_id=3,
        )


def test_unsupported_schema_version_is_rejected() -> None:
    """G. Unsupported schema -- ignore."""
    with pytest.raises(DetectionRejected, match="schema_version"):
        parse_detection(
            "customers/3/detections/16",
            _payload(schema_version=2),
            expected_customer_id=3,
        )


def test_missing_schema_version_is_rejected() -> None:
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "schema_version"}
    with pytest.raises(DetectionRejected, match="schema_version"):
        parse_detection(
            "customers/3/detections/16",
            json.dumps(payload).encode(),
            expected_customer_id=3,
        )


@pytest.mark.parametrize("bad_confidence", [-0.1, 1.1, "not-a-number", None])
def test_invalid_confidence_is_rejected(bad_confidence: object) -> None:
    """H. Invalid confidence (below 0, above 1, non-numeric) -- ignore."""
    with pytest.raises(DetectionRejected, match="confidence"):
        parse_detection(
            "customers/3/detections/16",
            _payload(confidence=bad_confidence),
            expected_customer_id=3,
        )


def test_confidence_boolean_is_rejected() -> None:
    """bool is a subclass of int in Python -- must not slip through as 0/1."""
    with pytest.raises(DetectionRejected, match="confidence"):
        parse_detection(
            "customers/3/detections/16",
            _payload(confidence=True),
            expected_customer_id=3,
        )


@pytest.mark.parametrize(
    "bad_timestamp",
    [
        "2026-09-09T18:12:42",  # naive, no timezone
        "not-a-timestamp",
        "",
    ],
)
def test_invalid_or_naive_timestamp_is_rejected(bad_timestamp: str) -> None:
    """I. Invalid timestamp / naive timestamp without timezone -- ignore."""
    with pytest.raises(DetectionRejected, match="timestamp"):
        parse_detection(
            "customers/3/detections/16",
            _payload(timestamp=bad_timestamp),
            expected_customer_id=3,
        )


@pytest.mark.parametrize(
    "good_timestamp",
    [
        "2026-09-09T18:12:42+00:00",
        "2026-09-09T21:12:42+03:00",
        "2026-09-09T18:12:42Z",
    ],
)
def test_any_valid_timezone_aware_offset_is_accepted(good_timestamp: str) -> None:
    """I. Valid timestamps with different offsets must all be accepted -- never
    assume Europe/Helsinki or any single fixed offset."""
    detection = parse_detection(
        "customers/3/detections/16",
        _payload(timestamp=good_timestamp),
        expected_customer_id=3,
    )

    assert detection.timestamp.tzinfo is not None


@pytest.mark.parametrize(
    "topic",
    [
        "customers/3/detections/16/detection",  # extra suffix
        "customers/3/detections/",  # missing camera_id
        "customers/detections/16",  # missing customer_id segment
        "customers/3/detections/abc",  # non-numeric camera_id
        "customers/abc/detections/16",  # non-numeric customer_id
        "riistakamera/3/16/detection",  # old pre-V1 topic shape
        "customers/#",
        "customers/+/detections/+",
    ],
)
def test_malformed_topics_are_rejected(topic: str) -> None:
    with pytest.raises(DetectionRejected):
        parse_detection(topic, _payload(), expected_customer_id=3)


@pytest.mark.parametrize("bad_event_id", [None, "", 123])
def test_missing_or_invalid_event_id_is_rejected(bad_event_id: object) -> None:
    with pytest.raises(DetectionRejected, match="event_id"):
        parse_detection(
            "customers/3/detections/16",
            _payload(event_id=bad_event_id),
            expected_customer_id=3,
        )


@pytest.mark.parametrize("bad_label", [None, "", "   "])
def test_missing_or_empty_label_is_rejected(bad_label: object) -> None:
    with pytest.raises(DetectionRejected, match="label"):
        parse_detection(
            "customers/3/detections/16",
            _payload(label=bad_label),
            expected_customer_id=3,
        )


def test_payload_that_is_not_a_json_object_is_rejected() -> None:
    with pytest.raises(DetectionRejected, match="object"):
        parse_detection(
            "customers/3/detections/16",
            json.dumps([1, 2, 3]).encode(),
            expected_customer_id=3,
        )
