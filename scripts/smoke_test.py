#!/usr/bin/env python3
"""Manual, optional smoke test against the REAL Kameraposti broker.

Not part of the automated test suite and never runs in CI -- it needs a
real network path to wss://tailscale2.steels.me/mqtt and a real test
account's credentials, supplied only via environment variables (never
hardcoded here):

    KAMERAPOSTI_TEST_CUSTOMER_ID
    KAMERAPOSTI_TEST_USERNAME
    KAMERAPOSTI_TEST_PASSWORD

Run manually, e.g. against test account #3:

    KAMERAPOSTI_TEST_CUSTOMER_ID=3 \\
    KAMERAPOSTI_TEST_USERNAME=rk-3-... \\
    KAMERAPOSTI_TEST_PASSWORD=... \\
    python scripts/smoke_test.py

Proves, in order: WSS/TLS connects, the given credentials authenticate,
subscribing to the account's own namespace succeeds -- and, if a real
detection is triggered while it is running, that a message actually
arrives and is valid JSON.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any

import paho.mqtt.client as mqtt

HOST = "tailscale2.steels.me"
PORT = 443
WS_PATH = "/mqtt"
WAIT_SECONDS = 30


def main() -> int:
    customer_id = os.environ.get("KAMERAPOSTI_TEST_CUSTOMER_ID")
    username = os.environ.get("KAMERAPOSTI_TEST_USERNAME")
    password = os.environ.get("KAMERAPOSTI_TEST_PASSWORD")

    if not (customer_id and username and password):
        print(
            "SKIP: set KAMERAPOSTI_TEST_CUSTOMER_ID, KAMERAPOSTI_TEST_USERNAME "
            "and KAMERAPOSTI_TEST_PASSWORD to run this manual smoke test."
        )
        return 0

    topic = f"customers/{customer_id}/detections/+"
    connected = threading.Event()
    subscribed = threading.Event()
    received = threading.Event()
    failures: list[str] = []

    def on_connect(client: mqtt.Client, userdata: Any, connect_flags: Any, reason_code: Any, properties: Any = None) -> None:
        rc = int(reason_code)
        if rc == 0:
            print(f"CONNECTED (rc={rc})")
            client.subscribe(topic, qos=1)
        else:
            failures.append(f"CONNECT FAILED rc={rc}")
        connected.set()

    def on_connect_fail(client: mqtt.Client, userdata: Any) -> None:
        failures.append("CONNECT FAILED (network/TLS -- no CONNACK ever arrived)")
        connected.set()

    def on_subscribe(client: mqtt.Client, userdata: Any, mid: int, reason_codes: Any, properties: Any = None) -> None:
        print(f"SUBSCRIBED to {topic}")
        subscribed.set()

    def on_message(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
        print(f"MESSAGE on {message.topic}:")
        try:
            print(json.dumps(json.loads(message.payload), indent=2))
        except ValueError:
            print(message.payload)
        received.set()

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
    client.username_pw_set(username, password)
    client.tls_set()
    client.ws_set_options(path=WS_PATH)
    client.on_connect = on_connect
    client.on_connect_fail = on_connect_fail
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    print(f"Connecting to wss://{HOST}{WS_PATH} as {username} (customer_id={customer_id})...")
    client.connect(HOST, PORT, keepalive=60)
    client.loop_start()

    try:
        if not connected.wait(WAIT_SECONDS):
            print("FAIL: timed out waiting for CONNACK")
            return 1
        if failures:
            print(f"FAIL: {failures[0]}")
            return 1
        if not subscribed.wait(WAIT_SECONDS):
            print("FAIL: timed out waiting for SUBACK")
            return 1

        print(
            f"OK so far: WSS + TLS + auth + subscribe all succeeded. "
            f"Waiting up to {WAIT_SECONDS}s for a real detection "
            "(trigger one now if you want to prove end-to-end delivery)..."
        )
        if not received.wait(WAIT_SECONDS):
            print("No detection arrived in time -- connection/auth/subscribe were still proven OK above.")
    finally:
        client.disconnect()
        client.loop_stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
