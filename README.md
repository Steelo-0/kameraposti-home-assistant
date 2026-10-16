# Kameraposti for Home Assistant

Home Assistant custom integration that receives Kameraposti riistakamera
(trail camera) detection events directly from Kameraposti's own MQTT
broker, turns them into Home Assistant sensors and an automation trigger
event, and shows each camera's latest photo. It can also send your own Home
Assistant sensors (leak, smoke, gas, door, window, motion, temperature,
carbon dioxide) to Kameraposti, which then alarms through its own
notifications, and send Kameraposti Turva a photo (and a short video) from a
Home Assistant camera when an alarm goes off.

It talks to Kameraposti's broker over its own dedicated MQTT connection —
it does **not** use, share, or modify your existing Home Assistant MQTT
integration or your own broker in any way, and it does not require any
port forwarding or inbound access on your network (the connection is
outbound-only, over WSS/TLS).

## Requirements

- Home Assistant 2024.12.0 or newer
- A Kameraposti account with sensors enabled, and the **service**, **MQTT
  login** and **MQTT password** shown in the **Home Assistant** section of the
  Sensors page (e.g. `cam.steels.me`, `kp-2`). The setup form asks for exactly
  these three. This integration does not use your regular Kameraposti login.

Each MQTT login has one connection at a time: the broker accepts a login only
with the same client id, so a second connection with the same login
disconnects the first. For another Home Assistant, choose **Add another Home
Assistant** in the same section and use the login it shows, e.g. `kp-2-3`.
Zigbee2MQTT/Mosquitto bridges get their own logins under the Sensors page's
advanced settings. A device's own login (e.g. a Shelly's `kp-2-d10`) is not
accepted: since 1.6.1 the setup form says so. Just the account number (`2`)
still means the main login `kp-2`.

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
2. Choose the service, then enter the MQTT login (e.g. `kp-2`) and password,
   all three as shown in the Home Assistant section of the Sensors page.
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

**Settings → Devices & services → Kameraposti → Configure → Sensors to
Kameraposti** lets you pick Home Assistant entities to send to Kameraposti, as
many as your Kameraposti plan takes (see [How many sensors](#how-many-sensors-since-180)).
Since 1.4.1 the list shows exactly the entities the integration can send:

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
counts every message against the account's message budget per minute before
reading it, so a connection that keeps dropping must not crowd out real
alarms. A sensor whose message could not be sent during the break is still
sent as soon as the connection is back; the full resend follows when the
minute is up.

### How many sensors (since 1.8.0)

Your Kameraposti plan decides how many sensors Home Assistant sends:
Kameraposti tells the number along with your camera list, and the list under
**Sensors to Kameraposti** takes at most that many (the form says how many).
Until Kameraposti has told it — for a moment after Home Assistant starts, or
on a Kameraposti service that does not tell it yet — the number is 20, as in
earlier versions. It is never more than 200.

If you have chosen more sensors than the plan takes (say, after changing to a
smaller plan), the first ones in the list are sent and Home Assistant's log
names the rest once; Kameraposti gets no message about them and keeps what it
had. When the plan takes more again, they are sent right away, without a
restart.

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

## Cameras to Kameraposti (since 1.8.1; photos since 1.6.0)

Kameraposti Turva can ask Home Assistant for a photo when a sensor linked to
one of its cameras alarms, or when you press **Ota kuva Home Assistantista**
(take a photo from Home Assistant) in Kameraposti. The photo arrives in
Kameraposti as one of the camera's own photos: it is attached to the alarm
(if the alarm has no photo yet), shows among the camera's photos and updates
its Latest photo. This is for cameras that cannot send
their photos to Kameraposti themselves (e.g. a Shelly camera, which sends
only to Shelly's cloud) but are in Home Assistant.

There is nothing to set in Home Assistant. It tells Kameraposti which cameras
it has (every camera entity that is not disabled, at most 50), and you choose
in Kameraposti which of them become cameras: add one from Kameraposti's list of
Home Assistant cameras, or pick one on a camera's page. A Shelly Camera added on
Kameraposti's Anturit page finds its Home Assistant camera by itself
(Kameraposti matches the Shelly's own id to the camera device's MAC address). A
camera you delete in Kameraposti lets go of its Home Assistant camera, so you
can add it again. A camera you add to Home Assistant later shows up in
Kameraposti within 15 minutes.

**Settings → Devices & services → Kameraposti → Configure → Hide cameras from
Kameraposti** is only for cameras Kameraposti must not see (none by default).
Kameraposti is not told about them, and a request for a photo from one of them
is refused with a warning in the log. A camera you hide later is withdrawn from
Kameraposti. The same form chooses whether a video is sent as well (5–30
seconds, default 10).

Upgrading to 1.8.1: every camera is reported, whatever was ticked (1.7),
linked (1.6) or listed as not to be used (1.8.0) before; those lists no longer
do anything, and the hide list starts empty. Kameraposti keeps using the
cameras a 1.6 link pointed at.

How the photo and the video are taken:

- **Photo**: a still from the camera (Home Assistant's own camera snapshot,
  waiting at most 10 seconds), sent to Kameraposti right away. The photo is
  always sent before the video.
- **Frigate cameras** (since 1.9.0): Kameraposti Turva also asks for a photo
  on every motion of a sensor linked to the camera. For a camera whose Home
  Assistant device has Frigate's last-detection pictures
  (`image.<camera>_person`, `image.<camera>_car`, …), a motion or alarm
  request sends Frigate's newest recognized picture instead of a live still,
  if Frigate updated it within the last 2 minutes. The request can come a
  moment before Frigate's picture, so Home Assistant waits up to 6 seconds for
  it. Motion without a recognition sends no picture; an alarm without one
  sends the live still, so an alarm always brings a picture. A motion never
  sends the same Frigate picture twice (an alarm does; after a Home Assistant
  restart one may go again). **Ota kuva Home Assistantista** always takes a
  live still, and so does every request for a camera without Frigate pictures
  (e.g. a Shelly camera).
- A motion never records a video; alarms and **Ota kuva Home Assistantista**
  do, when video is on.
- **Video** (optional): recorded after the photo with Home Assistant's
  `stream` integration, the same way as the `camera.record` action. The
  camera must support streaming (the camera's page in Home Assistant shows
  live video) and `stream` must be set up (it is with `default_config`). The
  video starts a few seconds after the alarm, not before it. A video larger
  than Kameraposti's limit (25 MB) is not sent; use a shorter video for a
  high-resolution stream. If the camera cannot stream, or the recording
  fails, the log gets one warning and only the photo is sent.
- **Temporary file**: the recording goes to a hidden folder
  `.kameraposti_capture` in Home Assistant's media folder (`/media` on Home
  Assistant OS and in containers), or to `/config/.kameraposti_capture` if
  you have added that folder to `allowlist_external_dirs`. Media files are
  served only to logged-in users, and the folder is hidden from the media
  browser. It is never written under `/config/www`, which is served to anyone
  at `/local/`. The file is deleted as soon as the request is done,
  whatever happened.
- Kameraposti's answer: a refusal (e.g. an expired or already used link) is
  final; a server error, timeout or lost connection is retried once while
  the link is still valid (2 minutes for the photo, 5 for the video).
- Each request is handled once. With several Home Assistants on the same
  account, the first one to deliver wins and the others log that
  Kameraposti already has the photo.
- The links in Kameraposti's request are one-time permissions to upload: the
  integration never writes them to the log. Log lines name the request
  (`request_id`), the camera and the outcome. Do not raise the `httpx`
  logger to `info` or `debug` in production (`logger:` in
  `configuration.yaml`): the `httpx` library itself then logs every request
  address, these links included.
- The links must point at the Kameraposti service the integration is set up
  with (`kameraposti.fi` or `cam.steels.me`, default port, no user name); a
  request with any other address is ignored, so nobody who could publish a
  request can make Home Assistant send camera images elsewhere.
- A recording that does not finish in time (the video length + 30 seconds,
  e.g. a camera that stops sending) is abandoned and its file deleted, but
  the recorder may keep one file handle open until Home Assistant restarts.
  Home Assistant's own `camera.record` action has the same limitation.
- Works on Home Assistant 2024.12 and newer, like the rest of the
  integration: where Home Assistant has no `camera.helper` module, the camera
  is looked up the same way through the camera integration itself.

A Kameraposti service that does not offer this yet refuses the subscription,
and everything else works as before.

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
