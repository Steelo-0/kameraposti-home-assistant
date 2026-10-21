"""Constants for the Kameraposti integration.

Authoritative backend contract: docs/kameraposti-ha-mqtt-contract-v1.md in
the steel-cloud repo. Topic shape, payload shape, QoS, retain and tenant
semantics here MUST match that document exactly -- if it changes, this
file changes first, together with a schema_version bump on the backend
side. Do not invent a parallel schema here.
"""

from __future__ import annotations

import re
from typing import Final

DOMAIN: Final = "kameraposti"

# Kameraposti's own broker (2026-10-05: the old tailscale2.steels.me endpoint
# was removed). The user picks the service, never a free-form host. Port/path/transport are fixed:
# MQTT over WSS behind the service's HTTPS proxy.
# 2026-10-19 (steelo "srv3 palveluksi"): production Kameraposti Turva runs at turva.secsrv.fi; cam.steels.me
# stays as the test service.
DEFAULT_HOST: Final = "kameraposti.fi"
BROKER_HOSTS: Final = ("turva.secsrv.fi", "cam.steels.me", "kameraposti.fi")
# Design review TOP 8 (steelo 2026-10-18): a new setup offers Kameraposti Turva's service first --
# kameraposti.fi has no sensor broker. DEFAULT_HOST stays the fallback for entries stored without a
# host (version 1 migration), so an existing installation never changes service.
SETUP_DEFAULT_HOST: Final = "turva.secsrv.fi"
MQTT_PORT: Final = 443
MQTT_WS_PATH: Final = "/mqtt"
MQTT_TRANSPORT: Final = "websockets"
MQTT_KEEPALIVE_SECONDS: Final = 60

# Contract section 4: subscribe ONLY to this pattern, scoped to the
# configured customer_id. Never a broader wildcard -- the broker's dynsec
# ACL already enforces this, but the client validates defensively too
# (contract section 23).
TOPIC_SUBSCRIBE_TEMPLATE: Final = "customers/{customer_id}/detections/+"

# Anchored, digits-only on both segments -- naturally rejects
# customers/+/..., customers/#, non-numeric ids, and extra path segments.
TOPIC_PATTERN: Final = re.compile(r"^customers/(?P<customer_id>\d+)/detections/(?P<camera_id>\d+)$")

SCHEMA_VERSION_SUPPORTED: Final = 1

# 1.5.0: the account's camera list (retained, the authority on which cameras
# exist and what they are called) and each camera's latest photo (retained,
# a signed image URL; an empty retained message = no photo). Both payloads
# carry their own schema_version 1.
ROSTER_TOPIC_TEMPLATE: Final = "customers/{customer_id}/cameras"
ROSTER_TOPIC_PATTERN: Final = re.compile(r"^customers/(?P<customer_id>\d+)/cameras$")
LATEST_PHOTO_SUBSCRIBE_TEMPLATE: Final = "customers/{customer_id}/cameras/+/latest"
LATEST_PHOTO_TOPIC_PATTERN: Final = re.compile(
    r"^customers/(?P<customer_id>\d+)/cameras/(?P<camera_id>\d+)/latest$"
)
ROSTER_SCHEMA_VERSION: Final = 1
# Sanity caps: a broken server must not grow device names or memory without bound.
ROSTER_MAX_CAMERAS: Final = 1000
ROSTER_MAX_NAME_LENGTH: Final = 255
# 1.8.0 (steelo 2026-10-16 "maalaisjärjellä, pari klikkausta"): Turva V62 adds the account's sensor
# limit to the roster ("sensor_limit", optional). A value outside 1..ROSTER_MAX_SENSOR_LIMIT is
# ignored like a missing one -- it never rejects the roster, which decides which cameras exist.
ROSTER_MAX_SENSOR_LIMIT: Final = 1000
LATEST_PHOTO_MAX_URL_LENGTH: Final = 2048
# The newest applied roster's generated_at, stored per config entry: the roster
# is retained, so the same (or an older) one arrives again on every reconnect and
# restart -- only a strictly newer roster may remove cameras.
ROSTER_STORE_KEY_TEMPLATE: Final = DOMAIN + ".roster.{entry_id}"
ROSTER_STORE_VERSION: Final = 1
LATEST_PHOTO_SCHEMA_VERSION: Final = 1

# 1.6.0: Kameraposti Turva asks for a photo (and optionally a video) from the Home
# Assistant camera linked to one of its cameras: QoS 1, not retained (an old request
# is useless). The payload carries signed, expiring upload URLs that are never logged.
CAPTURE_SUBSCRIBE_TEMPLATE: Final = "customers/{customer_id}/cameras/+/capture"
CAPTURE_TOPIC_PATTERN: Final = re.compile(
    r"^customers/(?P<customer_id>\d+)/cameras/(?P<camera_id>\d+)/capture$"
)
CAPTURE_MAX_URL_LENGTH: Final = 2048
# reason is "alarm" or "manual" today; any short lowercase word is accepted (it is only
# logged), so a new reason on the server never costs a photo.
CAPTURE_REASON_PATTERN: Final = re.compile(r"^[a-z_]{1,32}$")

EVENT_DETECTION: Final = "kameraposti_detection"

CONF_CUSTOMER_ID: Final = "customer_id"
# 1.17.0: the pairing code of the setup form (never stored).
CONF_PAIRING_CODE: Final = "pairing_code"
CONF_HOST: Final = "host"
CONF_EXPORTED_ENTITIES: Final = "exported_entities"
# 1.13.0: the kind the user chose for an exported entity whose kind cannot be read (entity_id -> kind).
CONF_ENTITY_KINDS: Final = "entity_kinds"
# 1.6.0: Kameraposti cameras linked to Home Assistant cameras for Turva's capture requests:
# {"<camera_id>": {"entity_id": "camera.x", "video": bool, "video_seconds": int}}.
CONF_CAPTURE_CAMERAS: Final = "capture_cameras"
CONF_CAPTURE_VIDEO: Final = "video"
CONF_CAPTURE_VIDEO_SECONDS: Final = "video_seconds"
CAPTURE_VIDEO_SECONDS_DEFAULT: Final = 10
CAPTURE_VIDEO_SECONDS_MIN: Final = 5
CAPTURE_VIDEO_SECONDS_MAX: Final = 30

# Camera options of 1.7.0-1.9.x (the ticked list, 1.8.0's "must not use" list, 1.8.1's hidden list
# and its removals, the one video setting). 1.10.0 (steelo 2026-10-16 "tee sitä nyt jo
# yksinkertaisempi"): there are no camera options -- every camera is reported and chosen in
# Kameraposti, and Kameraposti's request decides the video. Setup drops these keys
# (DEAD_CAMERA_OPTIONS) so a downgrade cannot hide a camera again.
CONF_EXPORTED_CAMERAS: Final = "exported_cameras"
CONF_EXCLUDED_CAMERAS: Final = "excluded_cameras"
CONF_HIDDEN_CAMERAS: Final = "hidden_cameras"
CONF_REMOVED_CAMERAS: Final = "removed_cameras"
CONF_CAMERA_VIDEO: Final = "camera_video"
CONF_CAMERA_VIDEO_SECONDS: Final = "camera_video_seconds"
DEAD_CAMERA_OPTIONS: Final = (
    CONF_EXPORTED_CAMERAS,
    CONF_EXCLUDED_CAMERAS,
    CONF_HIDDEN_CAMERAS,
    CONF_REMOVED_CAMERAS,
    CONF_CAMERA_VIDEO,
    CONF_CAMERA_VIDEO_SECONDS,
)
# 1.8.0: at most this many cameras are reported (the first after sorting).
MAX_EXPORTED_CAMERAS: Final = 50
# Kameraposti accepts camera.<object id> of lowercase letters, digits and underscores, 1-200.
CAMERA_ENTITY_PATTERN: Final = re.compile(r"^camera\.[a-z0-9_]{1,200}$")

# Sensor export (2026-10-05): chosen Home Assistant entities become sensors in
# Kameraposti. The account login may publish only under its own
# kameraposti/<id>/anturit/ prefix (broker ACL); "<name>/config" describes the
# sensor, "<name>" carries its state in Kameraposti's simple format.
SENSOR_TOPIC_TEMPLATE: Final = "kameraposti/{customer_id}/anturit/{name}"
# 1.12.0 (steelo 2026-10-18 "HA:n valinta synkronoi ja tiputtaa muut"): every chosen entity, after each full
# snapshot; Kameraposti drops the sensors this Home Assistant created that are no longer chosen. Two levels,
# so it never matches a sensor's own topic.
SYNC_TOPIC_TEMPLATE: Final = "kameraposti/{customer_id}/anturit/_ha/sync"
# The broker's message_size_limit is 4096 bytes; a longer list goes out in parts of at most this.
SYNC_PART_MAX_BYTES: Final = 3500
# 1.7.1 (steelo 2026-10-16 "ei anturirajoja"): the most this integration ever sends; the old 20
# here stopped accounts whose limit is higher.
MAX_EXPORTED_SENSORS: Final = 200
# 1.8.0 (steelo 2026-10-16 "maalaisjärjellä, pari klikkausta"): below that, the account's plan
# decides -- the roster's sensor_limit. Until Kameraposti tells it (or on a server that does not),
# the first 20 chosen sensors are sent: the old behaviour, which the server's per-account message
# budget is sized for.
DEFAULT_SENSOR_LIMIT: Final = 20

# The account's MQTT login on Kameraposti's broker is kp-<customer_id>, and the
# broker only accepts that login with the same client id (dynsec clientid pin).
USERNAME_TEMPLATE: Final = "kp-{customer_id}"
# 2026-10-06: each login has one connection (the broker pins client id == username), so a second
# Home Assistant (or a bridge) on the same account uses an extra login kp-<id>-<n> created on the
# Kameraposti Anturit page. Same permissions as the main login.
EXTRA_USERNAME_TEMPLATE: Final = "kp-{customer_id}-{login_number}"
# 1.3.1 (steelo 2026-10-06 "ei hyväksy kuin numeroita"): the setup field takes the login exactly as the
# Anturit page shows it -- "kp-<id>", "kp-<id>-<n>" (n >= 2) -- or just the account number.
LOGIN_PATTERN: Final = re.compile(r"^(?:kp-)?([1-9]\d{0,9})(?:-([2-9]|[1-9]\d))?$", re.IGNORECASE)
# 1.6.1: a device's own login (e.g. a Shelly's kp-<id>-d<n>) is not a Home Assistant login: one
# connection and the device's own topics only. The setup form names it instead of "unknown".
DEVICE_LOGIN_PATTERN: Final = re.compile(r"^kp-[1-9]\d{0,9}-d[1-9]\d{0,9}$", re.IGNORECASE)

MANUFACTURER: Final = "Kameraposti"
# Design review TOP 10 (2026-10-18): a camera, not a trail camera (Turva cameras are home cameras).
MODEL: Final = "Kamera"
# Device name for a camera the roster has not named (or with no roster at all).
# Fable audit 2026-10-18 L5: the same word as the model ("Kamera"); a named camera keeps its own name.
DEFAULT_CAMERA_NAME_TEMPLATE: Final = "Kamera {camera_id}"

# Contract section 8: "noin 500 viimeisintä event_id:tä" -- a reasonable
# bounded size, not required to survive a HA restart.
DEDUP_CACHE_SIZE: Final = 500

# Contract section 9: 1s -> 2s -> 4s -> 8s -> 16s -> 30s max, plus jitter.
RECONNECT_MIN_DELAY_SECONDS: Final = 1
RECONNECT_MAX_DELAY_SECONDS: Final = 30
RECONNECT_JITTER_SECONDS: Final = 1.0
# 1.5.1: the backoff starts again from the minimum only after a connection has stayed up this
# long, not on CONNACK. A login also used by another client is accepted and then kicked out
# seconds later (the broker pins client id == login); resetting on CONNACK made both clients
# reconnect every 1-2 s forever.
STABLE_CONNECTION_SECONDS: Final = 60
# 1.5.1: this many drops of not-yet-stable connections within the window mean the login is
# probably used by another Home Assistant or bridge -- one WARNING says so.
SHORT_CONNECTION_DROPS_WARN_COUNT: Final = 3
SHORT_CONNECTION_DROPS_WINDOW_SECONDS: Final = 120
# 1.5.1: the full sensor snapshot (two messages per exported sensor) goes out at most this often.
# Kameraposti counts every message against the account's per-minute message budget before reading
# it (120 when this was written; Turva V62 raises the default to 300), so a reconnect storm must not
# use the budget that leak/smoke/door alarms need.
SNAPSHOT_MIN_INTERVAL_SECONDS: Final = 60
# 1.14.2 (bug hunt 19.10 H-L4): the snapshots' messages (descriptions, their states, the selection list, the
# cameras) of one account on one service go out at most this many in any SNAPSHOT_BUDGET_SECONDS (budget.py):
# half of the 300 a minute Kameraposti allows an account. A plan's 30 sensors and 50 cameras (about 111
# messages) still go out at once; an unlimited account's 200 sensors and 50 cameras (about 455) take about
# three minutes. A few seconds over Kameraposti's minute, so two batches never meet in one of its minutes.
SNAPSHOT_BUDGET_MESSAGES: Final = 150
SNAPSHOT_BUDGET_SECONDS: Final = 65

# How long the config flow's connection test waits for CONNACK+SUBACK
# before declaring the broker unreachable.
CONNECTION_TEST_TIMEOUT_SECONDS: Final = 10

# steelo 2026-10-05 "HA purku koodilla": turvajärjestelmän viritys ja purku.
# Tila säilytettynä (customers/<id>/security), komento omaan aiheeseen
# (brokerin ACL rajaa omaan tiliin), tulos request_id:llä.
SECURITY_STATE_TOPIC_TEMPLATE: Final = "customers/{customer_id}/security"
SECURITY_RESULT_TOPIC_TEMPLATE: Final = "customers/{customer_id}/security/result"
SECURITY_COMMAND_TOPIC_TEMPLATE: Final = "kameraposti/{customer_id}/turva/set"
SECURITY_MODES: Final = ("away", "home", "disarmed")
SECURITY_ERRORS: Final = ("invalid_code", "locked", "code_not_set", "code_required", "rate_limited")

SIGNAL_NEW_CAMERA: Final = f"{DOMAIN}_new_camera"
SIGNAL_CAMERA_UPDATE: Final = f"{DOMAIN}_camera_update"
SIGNAL_CAMERA_REMOVED: Final = f"{DOMAIN}_camera_removed"
SIGNAL_LATEST_PHOTO: Final = f"{DOMAIN}_latest_photo"
SIGNAL_CAMERA_PHOTO: Final = f"{DOMAIN}_camera_photo"
SIGNAL_SECURITY: Final = f"{DOMAIN}_security"
