# wt901

Unattended data collection from wearable sensors and action cameras.

A worker wears a [WitMotion WT901SDCL-BT50](https://www.wit-motion.com/) IMU and
a [DJI Osmo Action](https://www.dji.com/osmo-action-5-pro) camera through their
shift. Both record to their own memory cards. At the end of the shift the
devices go into a dock — a laptop or a small PC — and everything from there on
happens on its own: the volumes are mounted, the recordings are copied and
verified, the sensor's binary log is decoded to JSON Lines, video is re-encoded
to a fraction of its size, the cards are cleared, and the volume is unmounted so
the worker can see the device is finished.

A full-screen dashboard reports what is happening in words someone standing at
the dock can act on: **DO NOT UNPLUG** or **CAN BE TAKEN**.

Built for recording hand movements during dairy work, but nothing here is
specific to that.

```
2026-09-24 12:35:56  farm-dock  wt901 cc656a1
--------------------------------------------------------------------------
   CAM01:  DONE - CAN BE TAKEN
   COW01:  COPYING 46% - DO NOT UNPLUG

STORAGE   53% [##############..............]  220.9 GB free of 467.3 GB

DEVICES
  CAM01        camera     DONE - CAN BE TAKEN   3 files, 28.9 GB in 16 min
  COW01        bracelet   COPYING 46%           5.6 GB of 12.1 GB  34 MB/s
  CAM02        camera     not connected         last sync 3 h ago

PROCESSING  re-encoding CAM01/DJI_20260923161646_0002_D.MP4  28%  1.5x real time
  720p on CPU   waiting 2 (4.1 GB)   space reclaimed 512 GB

CLOUD UPLOAD  idle, nothing to upload
  target cloud:farm-data   waiting 0 (0 B)   last success 12 min ago
```

## What it does

- **Mounts devices itself** through `udisksctl`, so a dock with nobody logged in
  still works, and a device that has already been collected is left alone until
  it is physically unplugged.
- **Copies and verifies.** Every file is read once, hashed as it is read, and
  the copy is checked before anything is removed from the card.
- **Survives a yanked cable.** An interrupted transfer keeps its partial file
  and resumes from where it stopped, after checking that the partial data really
  matches the source.
- **Decodes sensor logs.** The `WIT*.TXT` files only look like text; they hold a
  binary packet stream, which becomes JSON Lines, one sample per line.
- **Merges split recordings.** The sensor starts a new file every 12 MB, so one
  continuous recording is spread over several files; they are joined back into
  real sessions by timestamp.
- **Shrinks video.** The camera records at around 100 Mbit/s. Re-encoding to
  720p makes it roughly thirty times smaller, in the background, after the
  camera has already been released.
- **Configures the sensor over Bluetooth**, because the manufacturer's tool is
  Windows-only and the `SET.TXT` on the card is written by the device rather
  than read from it.

## Requirements

- Python 3.8+ — collection uses only the standard library
- `udisks2` on Linux, for mounting
- `ffmpeg` — only for `--transcode`
- `rclone` — only for cloud upload
- `bleak` — only for the Bluetooth configuration commands

```bash
sudo apt install -y udisks2 ffmpeg rclone
pip install bleak
```

macOS works too and mounts volumes by itself; `udisksctl` is not used there.

## Quick start

```bash
# 1. See what a plugged-in device exposes
python3 wt901.py --list

# 2. Watch one dock cycle, without deleting anything
python3 wt901.py --monitor

# 3. Once you trust it, install the background service
python3 wt901.py --install --delete --transcode
```

After step 3 there is nothing left to run. Dock a device, wait for the volume to
disappear, take it back.

On Linux `--install` writes a systemd **user** service that polls for devices,
plus an autostart entry that opens the dashboard full-screen on the dock's own
screen. It prints the two extra commands needed to keep it running with nobody
logged in. On macOS it installs a launchd agent triggered by volume mounts.

## Commands

### Collecting

| Command | Effect |
|---|---|
| `--dashboard` | full-screen live view; fine over SSH |
| `--monitor` | one-off progress view in the terminal |
| `--status` | one-shot summary of every device (`--json` for scripts) |
| `--once` | single pass, then exit |
| `--list` | show volumes and the files on them |
| `--delete` | clear the card after a verified copy |
| `--install` / `--uninstall` | run automatically on connect |
| `--volume NAME` | only touch volumes matching this name |
| `--forget DEVICE` | drop a device that will never come back |
| `--keep-raw` | keep the binary `.TXT` after decoding |

### Video

| Command | Effect |
|---|---|
| `--transcode` | re-encode video in the background |
| `--height 720` | output height (default 720) |
| `--crf 28` | quality; lower is better and bigger (default 28) |
| `--keep-original` | keep the camera original beside the small copy |
| `--benchmark FILE` | time encoder settings on your own footage |

`--benchmark` is the honest way to choose settings: it encodes a sample from a
real recording at several presets and says how long a shift would take.

```
  height preset      crf    time  size/min  smaller   speed  hours per 8 h shift
  ------------------------------------------------------------------------------
     720 ultrafast    28    3.0s     30 MB       9x    3.3x     2.4 h
     720 veryfast     28    4.8s      5 MB      55x    2.1x     3.8 h
     720 veryfast     32    4.4s      3 MB      81x    2.3x     3.5 h
```

Hardware encoding is used when the machine has a usable GPU, and the run falls
back to the CPU the first time the GPU refuses a file — 10-bit H.265 is a common
cause.

### Cloud

| Command | Effect |
|---|---|
| `--remote cloud:path` | rclone remote to upload finished sessions to |
| `--upload` | upload once, then exit |

Any rclone backend works. A session counts as uploaded only after `rclone check`
confirms the remote copy, and a session is never uploaded while it is still
being collected or re-encoded.

To exercise the whole pipeline without an account anywhere, point it at a local
folder:

```bash
rclone config create testcloud alias remote=/home/you/fake-cloud
python3 wt901.py --upload --remote testcloud:farm-data
```

### Sensor configuration over Bluetooth

| Command | Effect |
|---|---|
| `--scan` | find the sensor |
| `--verify` | read back the current settings |
| `--listen` | live data stream; measures the real sample rate |
| `--apply --rate 100` | set output rate and a matching filter bandwidth |
| `--settime` | sync the sensor clock to this computer |

The sensor's clock drifts badly — check it with `--listen` now and then. Those
timestamps are the only thing tying a recording to reality.

## Where the data goes

```
~/Projects/wt901-data/
├── collector.log
├── collected.json              what has already been fetched
├── state/                      live status, read by the dashboard
├── COW01/                      one folder per device, from the volume label
│   └── sessions/
│       └── 20260924_124104/    dock time
│           ├── session_20260924T080000.jsonl
│           └── SET.TXT         sensor config at the time of recording
└── CAM01/
    └── sessions/
        └── 20260924_124530/
            └── DJI_20260924103012_0001_D.MP4
```

One line of a sensor `.jsonl` is one sample:

```json
{"t":"2026-09-24T08:00:00.000","ax":0.293,"ay":-0.146,"az":1.025,
 "gx":305.2,"gy":-122.1,"gz":183.1,"roll":0.549,"pitch":1.099,"yaw":1.648}
```

Acceleration in g, angular velocity in deg/s, angles in degrees. At 100 Hz with
accelerometer, gyroscope and angles the sensor produces about 2.7 KB/s — roughly
80 MB per eight-hour shift.

## Several devices

Every card ships with the same label — `NO NAME` for the sensor, `SD_Card` for
the camera — which collides with any other unlabelled stick. Give each one a
real label before deploying: it becomes the folder name and part of the
deduplication key, so two devices with identically named files cannot overwrite
each other.

```bash
# Linux, FAT32 (the sensor's card)
sudo umount /dev/sdX && sudo fatlabel /dev/sdX COW01

# Linux, exFAT (the camera's card)
sudo umount /dev/sdX && sudo exfatlabel /dev/sdX CAM01

# macOS
diskutil rename "/Volumes/NO NAME" COW01
```

Up to `MAX_PARALLEL` devices are collected at once; the rest queue.

## Notes on the hardware

- The sensor starts a new file every 12 MB. `SET.TXT` is generated by the device
  and ignored when edited by hand — use `--apply` and `--settime`.
- The camera needs **File Transfer** chosen on its touchscreen before the
  computer sees it as a disk. Nothing can automate that away.
- A camera's built-in storage mounts under the same name on every unit, so it
  cannot identify a camera and is skipped. Record to the microSD card.
- `.LRF` proxy files are removed along with their video and not copied; set
  `CAMERA_KEEP_LRF` to keep them.

## Safety

- Nothing is deleted from a card until the copy has been verified.
- `SET.TXT` and the file a device is still writing to are never deleted.
- An unreadable file is retried, then skipped; the rest of the device still
  copies.
- A video is replaced by its smaller version only after the re-encode has been
  checked for length; a file ffmpeg cannot read stays at full size.
- Locks are per device and record the process holding them, so a collector
  killed mid-copy blocks nothing once it restarts.
- Everything already collected is remembered, so repeated runs are harmless.

## Configuration

Defaults live in the `SETTINGS` block at the top of `wt901.py`: output folder,
poll interval, parallelism, encoder preset and thread count, retention. The
command-line flags override them, and `--install` bakes the flags you used into
the service.

## License

MIT
