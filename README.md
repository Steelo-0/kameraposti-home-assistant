# Kameraposti for Home Assistant

Home Assistant custom integration that receives Kameraposti riistakamera
(trail camera) detection events directly from Kameraposti's own MQTT
broker, and turns them into Home Assistant sensors and an automation
trigger event. It can also send your own Home Assistant sensors (leak,
smoke, door, window, motion, temperature) to Kameraposti, which then
alarms through its own notifications.

It talks to Kameraposti's broker over its own dedicated MQTT connection —
it does **not** use, share, or modify your existing Home Assistant MQTT
integration or your own broker in any way, and it does not require any
port forwarding or inbound access on your network (the connection is
outbound-only, over WSS/TLS).

## Requirements

- Home Assistant 2024.12.0 or newer
- A Kameraposti account with sensors enabled, and:
  - your **account number** (customer ID)
  - the account's **MQTT password** from Kameraposti's sensor page
    (Cameras → Sensors → MQTT)

The MQTT login is `kp-<account number>`; you never type it. This
integration does not use your regular Kameraposti login.

Each MQTT login has one connection at a time: the broker accepts a login only
with the same client id, so a second connection with the same login (another
Home Assistant, or a Zigbee2MQTT/Mosquitto bridge to Kameraposti) disconnects
the first. For a second connection, create an **extra login**
`kp-<account number>-<n>` on Kameraposti's sensor page (Cameras → Sensors →
MQTT → Extra logins) and type that login, e.g. `kp-2-2`, in the
**MQTT login or account number** field (since 1.3.1; just the account number
means the main login). Bridges use their extra login directly.

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
2. Choose the service (`kameraposti.fi`, or `cam.steels.me` for the test
   server), then enter your account number and MQTT password.
3. The integration tests the connection (WSS/TLS + authentication +
   subscribe) before saving — if anything fails, you'll see the reason
   before the entry is created.

Each camera that has sent at least one detection appears automatically as
its own device, with no further configuration.

If Kameraposti rotates your MQTT password, Home Assistant will prompt you
to re-authenticate; enter the new password and the integration reconnects
automatically.

Upgrading from 1.0.x: the old broker address no longer exists. The entry is
migrated automatically and Home Assistant asks for the new MQTT password
(re-authenticate) — create it on Kameraposti's sensor page.

## What you get

For each camera (`camera_id`) that reports a detection, one device is
created (named "Riistakamera {camera_id}", e.g. "Riistakamera 16") with
three sensors:

| Sensor | Example entity ID | Description |
|---|---|---|
| Last detection | `sensor.riistakamera_16_last_detection` | Label of the most recent detection (e.g. `moose`) |
| Detection confidence | `sensor.riistakamera_16_detection_confidence` | Confidence of the most recent detection, 0–1 |
| Last detection time | `sensor.riistakamera_16_last_detection_time` | Timestamp of the most recent detection |

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

## Sensors to Kameraposti

**Settings → Devices & services → Kameraposti → Configure** lets you pick
Home Assistant entities to send to Kameraposti (at most 20 per account):

| Home Assistant entity | Kameraposti sensor | Sent |
|---|---|---|
| `binary_sensor`, device class `moisture` | leak | `leak` / `dry` |
| `binary_sensor`, device class `problem` with `leak`, `water_leak` or `flood` in its name (Z-Wave JS UI water alarm, shown as OK / Problem) | leak | `leak` / `dry` |
| `binary_sensor`, device class `problem` with `smoke` in its name | smoke | `smoke` / `clear` |
| `binary_sensor`, device class `smoke` | smoke | `smoke` / `clear` |
| `binary_sensor`, device class `door`, `garage_door`, `opening` | door | `open` / `closed` |
| `binary_sensor`, device class `window` | window | `open` / `closed` |
| `binary_sensor`, device class `motion`, `occupancy`, `presence` | motion | `motion` |
| `sensor`, device class `temperature` | temperature | value in °C, at most once a minute |

Kameraposti creates each sensor automatically (named after the entity's
friendly name) and handles the alarms, quiet hours and notifications
itself. The topic is `kameraposti/<account>/anturit/<entity_id>`.

On every (re)connect, and every 15 minutes, the sensors are described
again and their current state is sent, so a leak that started while Home
Assistant was offline (or a message lost during a broker-side restart) is
not lost; Kameraposti ignores repeated states, so this causes no extra
alarms. Nothing is queued while disconnected. A sensor deleted only in
Kameraposti comes back on the next connection — remove it from this list
as well.

## Security system (arm / disarm)

The integration adds an alarm panel, **Security system** ("Turvajärjestelmä"),
that shows and changes Kameraposti's mode:

| Kameraposti | Home Assistant | What alarms |
|---|---|---|
| Poissa (away) | Armed away | everything: doors, windows, motion, leak, smoke, temperature |
| Kotona (home) | Armed home | doors and windows; leak, smoke and temperature always |
| Purettu (disarmed) | Disarmed | only leak, smoke and temperature |

Arming needs no code. **Disarming needs the disarm code** you set in
Kameraposti (Sensors → Security system → Home Assistant disarm code); Kameraposti
checks it, and this integration passes it through without storing it (if you
give the panel a default code in Home Assistant's own entity settings, Home
Assistant stores that one). Without a code set, Home Assistant can arm but not
disarm. Five wrong codes lock disarming for 15 minutes, ten in a day for 24
hours, and Kameraposti notifies you and your followers (arming still works). Every change shows in Kameraposti's log as
"Home Assistant". Mode changes made in the Kameraposti app or website show up
in Home Assistant right away, so automations can react to them too.

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
end-to-end. It is not run automatically anywhere and never reads
credentials from anything but environment variables:

```bash
KAMERAPOSTI_TEST_HOST=cam.steels.me \
KAMERAPOSTI_TEST_CUSTOMER_ID=... \
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
