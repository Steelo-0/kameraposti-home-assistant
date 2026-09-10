# Kameraposti for Home Assistant

Home Assistant custom integration that receives Kameraposti riistakamera
(trail camera) detection events directly from Kameraposti's own MQTT
broker, and turns them into Home Assistant sensors and an automation
trigger event.

It talks to Kameraposti's broker over its own dedicated MQTT connection —
it does **not** use, share, or modify your existing Home Assistant MQTT
integration or your own broker in any way, and it does not require any
port forwarding or inbound access on your network (the connection is
outbound-only, over WSS/TLS).

## Requirements

- Home Assistant 2024.1.0 or newer
- A Kameraposti account with MQTT access enabled, and:
  - your **customer ID**
  - an **MQTT username** and **MQTT password** issued by Kameraposti for that account

Kameraposti provides these three values when MQTT access is enabled for
your account. This integration does not use your regular Kameraposti
login.

## Installation

### Via HACS (custom repository)

1. In HACS, go to **Integrations → ⋮ → Custom repositories**.
2. Add this repository's URL, category **Integration**.
3. Install **Kameraposti**, then restart Home Assistant.

### Manual

Copy `custom_components/kameraposti` into your Home Assistant
`config/custom_components/` directory and restart Home Assistant.

## Setup

1. Go to **Settings → Devices & services → Add integration**, search for
   **Kameraposti**.
2. Enter your customer ID, MQTT username, and MQTT password.
3. The integration tests the connection (WSS/TLS + authentication +
   subscribe) before saving — if anything fails, you'll see the reason
   before the entry is created.

Each camera that has sent at least one detection appears automatically as
its own device, with no further configuration.

If Kameraposti rotates your MQTT password, Home Assistant will prompt you
to re-authenticate; enter the new password and the integration reconnects
automatically.

## What you get

For each camera (`camera_id`) that reports a detection, one device is
created with three sensors:

| Sensor | Example entity ID | Description |
|---|---|---|
| Last detection | `sensor.kameraposti_camera_16_last_detection` | Label of the most recent detection (e.g. `moose`) |
| Detection confidence | `sensor.kameraposti_camera_16_detection_confidence` | Confidence of the most recent detection, 0–1 |
| Last detection time | `sensor.kameraposti_camera_16_last_detection_time` | Timestamp of the most recent detection |

Every detection also fires a `kameraposti_detection` event, so you can
build automations that react immediately without polling a sensor's
state:

```yaml
event_data:
  customer_id: 3
  camera_id: 16
  event_id: "01J...ULID..."
  label: "moose"
  confidence: 0.92
  timestamp: "2026-09-09T18:12:42+00:00"
```

### Example automation

Notify when a camera detects something with high confidence:

```yaml
automation:
  - alias: "Kameraposti: notify on confident detection"
    trigger:
      - platform: event
        event_type: kameraposti_detection
    condition:
      - condition: template
        value_template: "{{ trigger.event.data.confidence >= 0.8 }}"
    action:
      - service: notify.mobile_app_your_phone
        data:
          title: "Riistakamera {{ trigger.event.data.camera_id }}"
          message: >
            {{ trigger.event.data.label }}
            ({{ (trigger.event.data.confidence * 100) | round(0) }}%)
```

## Known limitations

- Duplicate-detection suppression is a bounded in-memory cache (~500
  recent event IDs) — it is not persisted across a Home Assistant
  restart, so a detection retried by the broker across a restart could
  in theory be delivered twice.
- The three sensors reflect only the *most recent* detection per camera;
  historical detections are available through Home Assistant's own
  recorder/history for the sensors, and through the `kameraposti_detection`
  event stream if you log or forward it yourself (e.g. via `logbook` or
  your own automation).

## Manual smoke test (optional, not part of CI)

`scripts/smoke_test.py` connects to the real Kameraposti broker with
credentials for one test account, to verify WSS/TLS + auth + subscribe
against production end-to-end. It is not run automatically anywhere and
never reads credentials from anything but environment variables:

```bash
KAMERAPOSTI_TEST_CUSTOMER_ID=... \
KAMERAPOSTI_TEST_USERNAME=... \
KAMERAPOSTI_TEST_PASSWORD=... \
python scripts/smoke_test.py
```

## Development

```bash
pip install -e ".[test]"
pytest
```

## License

MIT, see [LICENSE](LICENSE).
