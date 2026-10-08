# Kameraposti for Home Assistant

Home Assistant custom integration that receives Kameraposti riistakamera
(trail camera) detection events directly from Kameraposti's own MQTT
broker, turns them into Home Assistant sensors and an automation trigger
event, and shows each camera's latest photo. It can also send your own Home
Assistant sensors (leak, smoke, gas, door, window, motion, temperature,
carbon dioxide) to Kameraposti, which then alarms through its own
notifications.

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

Since 1.5.1 a connection that keeps being dropped right after connecting backs
off up to 30 seconds between attempts (the delay starts again from 1 second
only after a connection has stayed up for a minute), and Home Assistant's log
gets one warning that the login is probably also used by another Home
Assistant or a bridge.

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

A login can be added only once, however it is typed (`3`, `kp-3` and
` KP-3 ` are the same login; since 1.5.1). If two entries from an earlier
version use the same login, both are kept and Home Assistant's log warns
about them on every start: remove one, or add it again with an extra login.

Your cameras appear automatically as devices, with no further
configuration: since 1.5.0 straight from your account's camera list, named
as in Kameraposti (on a service that does not publish the list, a camera
appears with its first detection).

If Kameraposti rotates your MQTT password, Home Assistant will prompt you
to re-authenticate; enter the new password and the integration reconnects
automatically.

Upgrading from 1.0.x: the old broker address no longer exists. The entry is
migrated automatically and Home Assistant asks for the new MQTT password
(re-authenticate) — create it on Kameraposti's sensor page.

## What you get

Each camera is one device, named as in Kameraposti ("Riistakamera
{camera_id}", e.g. "Riistakamera 16", when the service sends no name), with
three sensors, and the camera's latest photo once it has one:

| Entity | Example entity ID | Description |
|---|---|---|
| Last detection | `sensor.riistakamera_16_last_detection` | Label of the most recent detection (e.g. `moose`) |
| Detection confidence | `sensor.riistakamera_16_detection_confidence` | Confidence of the most recent detection, 0–1 |
| Last detection time | `sensor.riistakamera_16_last_detection_time` | Timestamp of the most recent detection |
| Latest photo | `image.riistakamera_16_latest_photo` | The camera's latest photo (from its first photo on), see [Latest photo](#latest-photo) |

Entity IDs follow the device name when the entities are first created (a
camera named "Pihakamera" gets `sensor.pihakamera_last_detection`); renaming
the camera in Kameraposti later renames the device, and existing entity IDs
stay as they are.

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

### Camera list (since 1.5.0)

The integration follows your account's camera list in Kameraposti: a new
camera gets its device right away, a renamed camera's device is renamed (a
name you give the device in Home Assistant still wins), and a camera you
remove in Kameraposti disappears from Home Assistant with its entities —
including old cameras left behind by earlier versions. The list holds only
active cameras: a camera that is paused, or left over your plan's camera
limit, leaves Home Assistant the same way and comes back when it is active
again. A detection from a camera that is not on the list still creates the
camera, as before; it is removed only when Kameraposti's list changes again
without it (not on every restart or reconnect).

## Latest photo

Since 1.5.0 a camera gets a **Latest photo** ("Viimeisin kuva") image
entity on its device with its first photo — on a Kameraposti service that
does not publish photos, no such entity appears. Kameraposti sends the
photo's address when a new photo arrives, and Home Assistant fetches the
photo (JPEG, long side at most 1280 px; a video shows its poster frame) only
then — nothing is polled. Show it on a dashboard with a **Picture entity**
card, or open the entity.

- **State**: the time the photo was taken (`captured_at`).
- **Attributes**: `label` and `confidence` of the photo's detection (empty
  when nothing was detected), `is_video`, `captured_at`.
- The address is a signed link to that one photo, valid for 30 days and
  renewed by Kameraposti before it expires. It is never shown as an
  attribute, and the integration never writes it to the log: if loading the
  photo fails, the log names the entity, the photo's id and the reason (e.g.
  `HTTP 403`, `ConnectError`), once.
- Once created, the entity stays: if the photo is gone (e.g. deleted in
  Kameraposti), the link has expired or Kameraposti refused it (e.g.
  `HTTP 403`), it is unavailable until the next photo or renewed link
  arrives. After a temporary failure (timeout, connection error, `HTTP 5xx`
  or `429`) it is unavailable for 5 minutes and then tries again; the wait
  doubles for each failure in a row, up to an hour. After a Home Assistant
  restart it is back as soon as the camera is, and shows the photo when
  Kameraposti's stored photo message arrives again.

To react to each new photo, trigger on the entity's state with `to: ~`
(a state trigger without `to`/`from` also fires on Home Assistant's routine
attribute updates of image entities):

```yaml
trigger:
  - platform: state
    entity_id: image.riistakamera_16_latest_photo
    to: ~
```

If you added a camera's Home Assistant image link from Kameraposti
(`…/riistakamera/ha/<id>/latest.jpg`) as a **Generic Camera**, it is no
longer needed: the Latest photo entity shows the same photo without a link
and without fetching it every 10 seconds. The old link keeps working until
you remove it (Settings → Devices & services → Generic Camera).

## Sensors to Kameraposti

**Settings → Devices & services → Kameraposti → Configure** lets you pick
Home Assistant entities to send to Kameraposti (at most 20 per account). Since 1.4.1 the list shows
exactly the entities the integration can send:

| Home Assistant entity | Kameraposti sensor | Sent |
|---|---|---|
| `binary_sensor`, device class `moisture` | leak | `leak` / `dry` |
| `binary_sensor`, device class `problem` with `leak`, `water_leak` or `flood` in its name (Z-Wave JS UI water alarm, shown as OK / Problem) | leak | `leak` / `dry` |
| `binary_sensor`, device class `problem` with `smoke` in its name | smoke | `smoke` / `clear` |
| `binary_sensor`, device class `smoke` | smoke | `smoke` / `clear` |
| `binary_sensor`, device class `gas`, `carbon_monoxide` | gas | `gas` / `clear` |
| `binary_sensor`, device class `problem` with `gas`, `combustible` or `carbon_monoxide` in its name | gas | `gas` / `clear` |
| `binary_sensor`, device class `door`, `garage_door`, `opening` | door | `open` / `closed` |
| `binary_sensor`, device class `window` | window | `open` / `closed` |
| `binary_sensor`, device class `motion`, `occupancy`, `presence` | motion | `motion` |
| `sensor`, device class `temperature` | temperature | value in °C, at most once a minute |
| `sensor`, device class `carbon_dioxide` (unit ppm or none) | co2 | `{"e":"co2","v":812}`, whole ppm, at most once a minute |
| `sensor`, device class `carbon_monoxide` (CO meter, unit ppm or none) | gas | `gas` from 50 ppm, `clear` below 35 ppm (nothing in between); sent when the alarm state changes |
| `sensor` without a device class named `…carbon_monoxide` / `…carbon_dioxide`, unit ppm or none (Z-Wave JS UI gas levels, e.g. `nodeID_27_gas_carbon_monoxide`) | gas / co2 | as the two rows above |

For a `problem` sensor the kind comes from the value part of its name, which
follows the device name: if the name has several of these words (a combined
smoke and gas detector, say), the last one decides. A carbon monoxide
detector is a gas sensor in Kameraposti.

Kameraposti creates each sensor automatically, named after its device
(since 1.4.2; the entity's friendly name when you have named the entity yourself, when it has no
device, or when two chosen sensors of the same kind share a device). A name you give the sensor in
Kameraposti stays (the server keeps it). Kameraposti handles the alarms, quiet hours and
notifications itself. The topic is `kameraposti/<account>/anturit/<entity_id>`.

On every (re)connect, and every 15 minutes, the sensors are described
again and their current state is sent, so a leak that started while Home
Assistant was offline (or a message lost during a broker-side restart) is
not lost; Kameraposti ignores repeated states, so this causes no extra
alarms. Nothing is queued while disconnected. A sensor deleted only in
Kameraposti comes back on the next connection — remove it from this list
as well.

Since 1.5.1 this full resend happens at most once a minute: Kameraposti
counts every message against the account's limit (120 a minute) before
reading it, so a connection that keeps dropping must not crowd out real
alarms. A sensor whose message could not be sent during the break is still
sent as soon as the connection is back; the full resend follows when the
minute is up.

## Security system (arm / disarm)

The integration adds an alarm panel, **Security system** ("Turvajärjestelmä"),
that shows and changes Kameraposti's mode:

| Kameraposti | Home Assistant | What alarms |
|---|---|---|
| Poissa (away) | Armed away | everything: doors, windows, motion, leak, smoke, gas, temperature, CO₂ |
| Kotona (home) | Armed home | doors and windows; leak, smoke, gas, temperature and CO₂ always |
| Purettu (disarmed) | Disarmed | only leak, smoke, gas, temperature and CO₂ |

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
- The latest photo needs a Kameraposti service that publishes it; on a
  service that does not (yet), cameras have no Latest photo entity.
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
