"""Unit tests for detection payload parsing/validation (contract sections 6-7).

Pure Python, no Home Assistant fixtures needed -- these are the fast,
isolated tests for the actual contract-enforcement logic.
"""

from __future__ import annotations

import json

import pytest

from custom_components.kameraposti.models import (
    DetectionRejected,
    LatestPhotoRejected,
    RosterRejected,
    parse_detection,
    parse_latest_photo,
    parse_roster,
)

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


# -- camera roster: customers/<id>/cameras (1.5.0) ---------------------------

ROSTER_TOPIC = "customers/3/cameras"
VALID_ROSTER = {
    "schema_version": 1,
    "generated_at": "2026-10-07T09:12:00+00:00",
    "cameras": [
        {"camera_id": 12, "name": "Kamera 1"},
        {"camera_id": 35, "name": "S12HD"},
    ],
}


def _roster(**overrides: object) -> bytes:
    return json.dumps({**VALID_ROSTER, **overrides}).encode()


def test_valid_roster_parses_cameras_in_order() -> None:
    roster = parse_roster(ROSTER_TOPIC, _roster(), expected_customer_id=3)

    assert roster is not None
    assert [(c.camera_id, c.name) for c in roster.cameras] == [(12, "Kamera 1"), (35, "S12HD")]
    assert roster.generated_at.isoformat() == "2026-10-07T09:12:00+00:00"


def test_roster_tolerates_unknown_fields_and_an_empty_camera_list() -> None:
    roster = parse_roster(
        ROSTER_TOPIC,
        _roster(future={"x": 1}, cameras=[{"camera_id": 12, "name": "Piha", "model": "S12HD"}]),
        expected_customer_id=3,
    )
    assert roster is not None
    assert [(c.camera_id, c.name) for c in roster.cameras] == [(12, "Piha")]

    empty = parse_roster(ROSTER_TOPIC, _roster(cameras=[]), expected_customer_id=3)
    assert empty is not None
    assert empty.cameras == ()


def test_empty_retained_roster_payload_means_no_roster() -> None:
    assert parse_roster(ROSTER_TOPIC, b"", expected_customer_id=3) is None


def test_blank_roster_name_falls_back_to_no_name() -> None:
    roster = parse_roster(
        ROSTER_TOPIC, _roster(cameras=[{"camera_id": 12, "name": "  "}]), expected_customer_id=3
    )
    assert roster is not None
    assert roster.cameras[0].name is None


@pytest.mark.parametrize("schema_version", [2, "1", None, True])
def test_roster_with_an_unsupported_schema_version_is_rejected(schema_version: object) -> None:
    with pytest.raises(RosterRejected, match="schema_version"):
        parse_roster(ROSTER_TOPIC, _roster(schema_version=schema_version), expected_customer_id=3)


@pytest.mark.parametrize(
    "payload,reason",
    [
        (b"{not json", "JSON"),
        (json.dumps([1, 2]).encode(), "object"),
        (_roster(cameras={"camera_id": 12}), "cameras"),
        (_roster(cameras=None), "cameras"),
        (_roster(cameras=["12"]), "object"),
        (_roster(cameras=[{"name": "x"}]), "camera_id"),
        (_roster(cameras=[{"camera_id": "12", "name": "x"}]), "camera_id"),
        (_roster(cameras=[{"camera_id": True, "name": "x"}]), "camera_id"),
        (_roster(cameras=[{"camera_id": 0, "name": "x"}]), "camera_id"),
        (_roster(cameras=[{"camera_id": 12}]), "name"),
        (_roster(cameras=[{"camera_id": 12, "name": None}]), "name"),
        (_roster(cameras=[{"camera_id": 12, "name": 5}]), "name"),
        (_roster(cameras=[{"camera_id": 12, "name": "a"}, {"camera_id": 12, "name": "b"}]), "duplicate"),
        (_roster(generated_at=None), "generated_at"),
        (_roster(generated_at="2026-10-07T09:12:00"), "generated_at"),
        (_roster(generated_at="yesterday"), "generated_at"),
    ],
)
def test_junk_roster_is_rejected_as_a_whole(payload: bytes, reason: str) -> None:
    """One bad camera rejects the whole roster: a partial list would remove real cameras."""
    with pytest.raises(RosterRejected, match=reason):
        parse_roster(ROSTER_TOPIC, payload, expected_customer_id=3)


@pytest.mark.parametrize("topic", ["customers/4/cameras", "customers/3/cameras/12", "customers/x/cameras"])
def test_roster_on_a_foreign_or_malformed_topic_is_rejected(topic: str) -> None:
    with pytest.raises(RosterRejected):
        parse_roster(topic, _roster(), expected_customer_id=3)


# -- latest photo: customers/<id>/cameras/<camera>/latest (1.5.0) ------------

LATEST_TOPIC = "customers/3/cameras/12/latest"
VALID_LATEST = {
    "schema_version": 1,
    "camera_id": 12,
    "photo_id": 5501,
    "captured_at": "2026-10-07T04:31:10Z",
    "received_at": "2026-10-07T04:31:40Z",
    "is_video": False,
    "url": "https://cam.steels.me/riistakamera/ha/kuva/5501?expires=1&signature=abc",
    "expires_at": "2026-11-06T04:31:40Z",
    "detection": {"label": "Hirvi", "confidence": 0.93},
}


def _latest(**overrides: object) -> bytes:
    return json.dumps({**VALID_LATEST, **overrides}).encode()


def test_valid_latest_photo_parses() -> None:
    camera_id, photo = parse_latest_photo(LATEST_TOPIC, _latest(), expected_customer_id=3)

    assert camera_id == 12
    assert photo is not None
    assert photo.camera_id == 12
    assert photo.photo_id == 5501
    assert photo.captured_at.isoformat() == "2026-10-07T04:31:10+00:00"
    assert photo.expires_at.isoformat() == "2026-11-06T04:31:40+00:00"
    assert photo.is_video is False
    assert photo.url == VALID_LATEST["url"]
    assert photo.label == "hirvi"
    assert photo.confidence == 0.93


def test_latest_photo_without_a_detection_and_with_unknown_fields_parses() -> None:
    _, photo = parse_latest_photo(LATEST_TOPIC, _latest(detection=None, extra=[1]), expected_customer_id=3)
    assert photo is not None
    assert photo.label is None
    assert photo.confidence is None

    payload = {k: v for k, v in VALID_LATEST.items() if k != "detection"}
    _, photo = parse_latest_photo(LATEST_TOPIC, json.dumps(payload).encode(), expected_customer_id=3)
    assert photo is not None
    assert photo.label is None


def test_empty_retained_latest_payload_means_no_photo_for_that_camera() -> None:
    assert parse_latest_photo(LATEST_TOPIC, b"", expected_customer_id=3) == (12, None)


@pytest.mark.parametrize("schema_version", [2, "1", None])
def test_latest_photo_with_an_unsupported_schema_version_is_rejected(schema_version: object) -> None:
    with pytest.raises(LatestPhotoRejected, match="schema_version"):
        parse_latest_photo(LATEST_TOPIC, _latest(schema_version=schema_version), expected_customer_id=3)


@pytest.mark.parametrize(
    "payload,reason",
    [
        (b"{not json", "JSON"),
        (json.dumps("photo").encode(), "object"),
        (_latest(camera_id=13), "camera_id"),
        (_latest(camera_id="12"), "camera_id"),
        (_latest(photo_id="5501"), "photo_id"),
        (_latest(photo_id=True), "photo_id"),
        (_latest(photo_id=0), "photo_id"),
        (_latest(captured_at=None), "captured_at"),
        (_latest(captured_at="2026-10-07T04:31:10"), "captured_at"),
        (_latest(is_video=0), "is_video"),
        (_latest(is_video="false"), "is_video"),
        (_latest(url=None), "url"),
        (_latest(url="http://cam.steels.me/riistakamera/ha/kuva/5501"), "url"),
        (_latest(url="javascript:alert(1)"), "url"),
        (_latest(url="https:///riistakamera/ha/kuva/5501"), "url"),
        (_latest(expires_at=None), "expires_at"),
        (_latest(expires_at="soon"), "expires_at"),
        (_latest(detection=["hirvi", 0.9]), "detection"),
        (_latest(detection={"label": "", "confidence": 0.9}), "label"),
        (_latest(detection={"label": "hirvi"}), "confidence"),
        (_latest(detection={"label": "hirvi", "confidence": 1.5}), "confidence"),
        (_latest(detection={"label": "hirvi", "confidence": True}), "confidence"),
    ],
)
def test_junk_latest_photo_is_rejected(payload: bytes, reason: str) -> None:
    with pytest.raises(LatestPhotoRejected, match=reason):
        parse_latest_photo(LATEST_TOPIC, payload, expected_customer_id=3)


@pytest.mark.parametrize(
    "topic",
    [
        "customers/4/cameras/12/latest",  # another account
        "customers/3/cameras/abc/latest",
        "customers/3/cameras/12/latest/x",
        "customers/3/cameras/12",
        "customers/3/cameras/+/latest",
    ],
)
def test_latest_photo_on_a_foreign_or_malformed_topic_is_rejected(topic: str) -> None:
    with pytest.raises(LatestPhotoRejected):
        parse_latest_photo(topic, _latest(), expected_customer_id=3)
