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
# was removed). The user picks the service, never a free-form host: the
# production service or the CAM test server. Port/path/transport are fixed:
# MQTT over WSS behind the service's HTTPS proxy.
DEFAULT_HOST: Final = "kameraposti.fi"
BROKER_HOSTS: Final = ("kameraposti.fi", "cam.steels.me")
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
CONF_HOST: Final = "host"
CONF_EXPORTED_ENTITIES: Final = "exported_entities"

# Sensor export (2026-10-05): chosen Home Assistant entities become sensors in
# Kameraposti. The account login may publish only under its own
# kameraposti/<id>/anturit/ prefix (broker ACL); "<name>/config" describes the
# sensor, "<name>" carries its state in Kameraposti's simple format.
SENSOR_TOPIC_TEMPLATE: Final = "kameraposti/{customer_id}/anturit/{name}"
# Kameraposti's per-account sensor cap (kameraposti_sensors.max_per_account).
MAX_EXPORTED_SENSORS: Final = 20

# The account's MQTT login on Kameraposti's broker is kp-<customer_id>, and the
# broker only accepts that login with the same client id (dynsec clientid pin).
USERNAME_TEMPLATE: Final = "kp-{customer_id}"

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
SIGNAL_SECURITY: Final = f"{DOMAIN}_security"
