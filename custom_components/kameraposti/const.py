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

# Fixed service endpoint (contract section 2). NOT user-configurable in the
# V1 config flow -- the user only supplies their own account credentials,
# never the broker host/port/path.
MQTT_HOST: Final = "tailscale2.steels.me"
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

EVENT_DETECTION: Final = "kameraposti_detection"

CONF_CUSTOMER_ID: Final = "customer_id"

MANUFACTURER: Final = "Kameraposti"
MODEL: Final = "Riistakamera"

# Contract section 8: "noin 500 viimeisintä event_id:tä" -- a reasonable
# bounded size, not required to survive a HA restart.
DEDUP_CACHE_SIZE: Final = 500

# Contract section 9: 1s -> 2s -> 4s -> 8s -> 16s -> 30s max, plus jitter.
RECONNECT_MIN_DELAY_SECONDS: Final = 1
RECONNECT_MAX_DELAY_SECONDS: Final = 30
RECONNECT_JITTER_SECONDS: Final = 1.0

# How long the config flow's connection test waits for CONNACK+SUBACK
# before declaring the broker unreachable.
CONNECTION_TEST_TIMEOUT_SECONDS: Final = 10

SIGNAL_NEW_CAMERA: Final = f"{DOMAIN}_new_camera"
SIGNAL_CAMERA_UPDATE: Final = f"{DOMAIN}_camera_update"
