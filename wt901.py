#!/usr/bin/env python3
"""
wt901.py - everything for working with a WT901SDCL-BT50 sensor.

The sensor mounts as a USB mass-storage volume. This script notices it,
copies the recordings, decodes the binary format into JSON Lines, and
frees space on the card. It can also configure the sensor over Bluetooth.

DATA COLLECTION (pure stdlib, runs on the system Python)
    python3 wt901.py --monitor --delete    live terminal monitor
    python3 wt901.py --once                single pass, then exit
    python3 wt901.py --list                show what is on the card
    python3 wt901.py --install --delete    auto-run on device connect
    python3 wt901.py --uninstall           remove the auto-run agent

SENSOR CONFIGURATION (requires: pip install bleak)
    python3 wt901.py --scan                find the sensor over BLE
    python3 wt901.py --verify              show current settings
    python3 wt901.py --listen              live data stream
    python3 wt901.py --apply               apply 100 Hz
    python3 wt901.py --apply --rate 50     apply 50 Hz

FIRST RUN
    1. --list with the sensor plugged in, note the volume name
    2. --monitor without --delete, confirm files are copied
    3. inspect the output, then --install --delete

SAFETY
    * each file is read once; the hash is computed inline and checked
    * without --delete originals are never touched
    * SET.TXT and the currently active file are never deleted
    * an unreadable file is skipped, the rest still copy
    * already-collected files are remembered, no duplicates
"""

import argparse
import asyncio
import hashlib
import json
import os
import platform
import re
import socket
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

# ==========================================================================
# SETTINGS
# ==========================================================================

OUT_DIR = Path.home() / "Projects" / "wt901-data"

POLL_INTERVAL = 10.0      # watch-mode poll period, seconds

# Upload to cloud storage through rclone. Empty string disables uploading.
# The value is an rclone remote plus path, e.g. "cloud:farm-data".
# Set it here or pass --remote on the command line.
UPLOAD_REMOTE = ""
UPLOAD_INTERVAL = 30.0    # seconds between upload passes
DELETE_AFTER_UPLOAD = False   # free local disk once a session is verified remotely

DEVICE_RETRY = 60.0       # seconds before retrying a device that failed
MAX_PARALLEL = 4          # devices collected at the same time
READY_SHOW_MINUTES = 30   # how long the dashboard keeps saying "can be taken"
STALE_HOURS = 36          # flag a device that has not synced for this long
SETTLE_DELAY = 3.0        # settle time after a volume mounts

MIN_FILE_SIZE = 128       # smaller files are treated as junk, bytes

KEEP_RAW = False          # keep binary .TXT files after decoding
                          # True saves space: .TXT is ~4x smaller than .jsonl

COPY_ATTEMPTS = 3         # attempts per file before skipping it
RETRY_DELAY = 3.0         # delay between attempts, seconds

# Volume name filter: case-insensitive substring.
# None means "any removable volume that holds recordings".
# Find your volume name with --list and set it here for reliability.
VOLUME_FILTER = None

# Recording file extensions. Empty set = accept every file.
# The card is dedicated to data, so by default we take everything.
DATA_SUFFIXES = set()

# Files we copy but NEVER delete from the card.
# SET.TXT holds the sensor config; losing it resets factory defaults.
PROTECTED_NAMES = {"SET.TXT", "SETTING.TXT", "CONFIG.TXT"}

# How we tell a sensor apart from an unrelated USB stick.
# The WT901 volume is usually labelled "NO NAME", which collides with any
# unlabelled stick, so we match on the presence of recording files.
REQUIRED_GLOB = "WIT*.TXT"

# --- Cameras (DJI Osmo Action 5 Pro) ---
# Recordings land in DCIM/DJI_NNN/DJI_<timestamp>_<seq>_D.MP4, each with a
# low-res .LRF proxy beside it. The proxy is redundant with the MP4, so it
# is skipped unless CAMERA_KEEP_LRF is set.
CAMERA_VIDEO_EXTS = {".MP4", ".MOV"}
CAMERA_KEEP_LRF = False
# The camera's built-in storage mounts with the same label on every unit,
# so it cannot identify a camera. Record to the microSD card instead.
CAMERA_SKIP_LABELS = {"OSMOACTION"}

# OS volumes that can never be a sensor
IGNORED_NAMES = {
    "Macintosh HD", "Preboot", "Recovery", "VM", "Update", "Data",
    "com.apple.TimeMachine.localsnapshots", "home", "net",
}

AGENT_LABEL = "com.wt901.collector"

# ==========================================================================


def mount_roots():
    """Directories where the OS mounts removable media."""
    user = Path.home().name
    return [
        Path("/Volumes"),                    # macOS
        Path("/media") / user,               # Linux
        Path("/run/media") / user,           # Linux with systemd
        Path("/media"),                      # Linux, fallback
    ]


QUIET = False             # in monitor mode the log goes to file only


def log(message):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}"
    if not QUIET:
        print(line, flush=True)
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUT_DIR / "collector.log", "a") as handle:
            handle.write(line + "\n")
    except OSError:
        pass


# --------------------------------------------------------------------------
# State: which files were already collected
# --------------------------------------------------------------------------

def state_path():
    return OUT_DIR / "collected.json"


def load_state():
    try:
        with open(state_path()) as handle:
            return set(json.load(handle))
    except (OSError, ValueError):
        return set()


def save_state(seen):
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(state_path(), "w") as handle:
            json.dump(sorted(seen), handle, indent=1)
    except OSError as exc:
        log(f"  could not save state: {exc}")


# --------------------------------------------------------------------------
# Live state shared with the dashboard
# --------------------------------------------------------------------------

def state_dir():
    return OUT_DIR / "state"


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def read_state(name):
    try:
        return json.loads((state_dir() / f"{name}.json").read_text())
    except (OSError, ValueError):
        return {}


def write_state(name, **fields):
    """Merge fields into a small JSON file, written atomically.

    The collector, the uploader and the dashboard are separate processes
    or threads; replace() guarantees a reader never sees half a file.
    """
    try:
        folder = state_dir()
        folder.mkdir(parents=True, exist_ok=True)
        data = read_state(name)
        data.update(fields)
        data["updated"] = now_iso()
        tmp = folder / f".{name}.tmp"
        tmp.write_text(json.dumps(data, indent=1))
        tmp.replace(folder / f"{name}.json")
    except OSError:
        pass


def seconds_since(iso):
    if not iso:
        return None
    try:
        return (datetime.now() - datetime.fromisoformat(iso)).total_seconds()
    except ValueError:
        return None


def safe_name(text):
    """Volume name to folder name: no spaces or separators."""
    cleaned = "".join(c if c.isalnum() or c in "-_" else "_" for c in text)
    return cleaned.strip("_") or "unknown"


def file_key(path, volume=None):
    """File identity: volume, name, size, mtime.

    The volume must be part of the key: different sensors name their files
    identically (WIT0.TXT and so on) and can easily match on size and
    mtime too. Without it, one sensor's data would be silently dropped.
    """
    try:
        info = path.stat()
    except OSError:
        return None
    tag = volume if volume else path.parent.name
    return f"{tag}/{path.name}:{info.st_size}:{int(info.st_mtime)}"


# --------------------------------------------------------------------------
# Notifications and unmounting
# --------------------------------------------------------------------------

def notify(title, message):
    """Desktop notification."""
    try:
        if platform.system() == "Darwin":
            text = message.replace('"', "'")
            head = title.replace('"', "'")
            subprocess.run(
                ["osascript", "-e",
                 f'display notification "{text}" with title "{head}"'],
                capture_output=True, timeout=10)
        elif platform.system() == "Linux":
            subprocess.run(["notify-send", title, message],
                           capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        pass


def unmount(volume):
    """Unmount the volume: the icon disappearing means it is safe to unplug."""
    try:
        if platform.system() == "Darwin":
            result = subprocess.run(["diskutil", "unmount", str(volume)],
                                    capture_output=True, text=True, timeout=30)
        else:
            result = subprocess.run(["umount", str(volume)],
                                    capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            log("  volume unmounted, the sensor can be taken")
            return True
        log(f"  unmount failed: {result.stderr.strip()[:80]}")
    except (OSError, subprocess.SubprocessError) as exc:
        log(f"  unmount failed: {exc}")
    return False


# --------------------------------------------------------------------------
# Decoding binary logs into JSON Lines
# --------------------------------------------------------------------------

PACKET_HEADER = b"\x55\x61"
PACKET_SIZE = 28
SESSION_GAP = 5.0
MIN_SESSION = 100

ACC_SCALE = 16.0 / 32768
GYRO_SCALE = 2000.0 / 32768
ANGLE_SCALE = 180.0 / 32768


def decode_packet(chunk):
    if len(chunk) != PACKET_SIZE or chunk[0:2] != PACKET_HEADER:
        return None

    def word(index):
        start = 2 + index * 2
        return int.from_bytes(chunk[start:start + 2], "little", signed=True)

    year, month, day, hour, minute, second = chunk[20:26]
    ms = int.from_bytes(chunk[26:28], "little")
    try:
        stamp = datetime(2000 + year, month, day, hour, minute, second,
                         ms * 1000)
    except ValueError:
        return None

    return {
        "t": stamp.isoformat(timespec="milliseconds"),
        "ax": round(word(0) * ACC_SCALE, 4),
        "ay": round(word(1) * ACC_SCALE, 4),
        "az": round(word(2) * ACC_SCALE, 4),
        "gx": round(word(3) * GYRO_SCALE, 3),
        "gy": round(word(4) * GYRO_SCALE, 3),
        "gz": round(word(5) * GYRO_SCALE, 3),
        "roll": round(word(6) * ANGLE_SCALE, 3),
        "pitch": round(word(7) * ANGLE_SCALE, 3),
        "yaw": round(word(8) * ANGLE_SCALE, 3),
    }


def read_samples(path):
    raw = path.read_bytes()
    position = 0
    out = []
    while position < len(raw) - 1:
        found = raw.find(PACKET_HEADER, position)
        if found < 0:
            break
        sample = decode_packet(raw[found:found + PACKET_SIZE])
        if sample is None:
            position = found + 1
            continue
        out.append(sample)
        position = found + PACKET_SIZE
    return out


def split_sessions(samples):
    """The sensor appends to files, so one file can hold several recordings."""
    if not samples:
        return []
    sessions = [[samples[0]]]
    previous = datetime.fromisoformat(samples[0]["t"])
    for sample in samples[1:]:
        moment = datetime.fromisoformat(sample["t"])
        delta = (moment - previous).total_seconds()
        if delta > SESSION_GAP or delta < -1:
            sessions.append([])
        sessions[-1].append(sample)
        previous = moment
    return sessions


def convert_all(paths, out_dir):
    """Gather samples from every file and split them into real sessions.

    The sensor starts a new file every 12 MB, so a single continuous
    recording is spread across several WIT files. Here we merge them,
    sort by timestamp and split on genuine gaps instead.
    """
    everything = []
    for path in paths:
        if path.suffix.upper() != ".TXT" or not path.stem.upper().startswith("WIT"):
            continue
        try:
            everything.extend(read_samples(path))
        except OSError as exc:
            log(f"  cannot read {path.name}: {exc}")

    if not everything:
        return 0, 0

    everything.sort(key=lambda s: s["t"])

    written = 0
    files = 0
    for session in split_sessions(everything):
        if len(session) < MIN_SESSION:
            continue
        start = datetime.fromisoformat(session[0]["t"])
        target = out_dir / f"session_{start.strftime('%Y%m%dT%H%M%S')}.jsonl"
        try:
            with open(target, "w") as handle:
                for sample in session:
                    handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
        except OSError as exc:
            log(f"  cannot write {target.name}: {exc}")
            continue

        span = (datetime.fromisoformat(session[-1]["t"]) - start).total_seconds()
        rate = (len(session) - 1) / span if span > 0 else 0
        log(f"  {target.name}  {len(session)} samples, "
            f"{span / 60:.1f} min, ~{rate:.0f} Hz")

        # Small sidecar so --status never has to re-read a huge .jsonl
        try:
            meta = target.with_suffix(".meta.json")
            meta.write_text(json.dumps({
                "file": target.name,
                "samples": len(session),
                "seconds": round(span, 1),
                "rate_hz": round(rate, 1),
                "start": session[0]["t"],
                "end": session[-1]["t"],
                "bytes": target.stat().st_size,
            }, indent=1))
        except OSError:
            pass

        written += len(session)
        files += 1

    return written, files


def convert_file(path):
    """Binary log -> one .jsonl per session. Returns counters."""
    try:
        samples = read_samples(path)
    except OSError as exc:
        log(f"  cannot read {path.name}: {exc}")
        return 0, 0

    if not samples:
        return 0, 0

    written = 0
    files = 0
    for session in split_sessions(samples):
        if len(session) < MIN_SESSION:
            continue
        start = datetime.fromisoformat(session[0]["t"])
        target = path.with_name(
            f"{path.stem}_{start.strftime('%Y%m%dT%H%M%S')}.jsonl")
        try:
            with open(target, "w") as handle:
                for sample in session:
                    handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
        except OSError as exc:
            log(f"  cannot write {target.name}: {exc}")
            continue

        span = (datetime.fromisoformat(session[-1]["t"]) - start).total_seconds()
        rate = (len(session) - 1) / span if span > 0 else 0
        log(f"  {target.name}  {len(session)} samples, {span:.0f} s, ~{rate:.0f} Hz")
        written += len(session)
        files += 1

    return written, files


# --------------------------------------------------------------------------
# Finding volumes and files
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# Mounting USB media ourselves (Linux)
#
# The desktop only auto-mounts a device when it appears. Once this program
# unmounts a volume - which is how a worker sees that a device is finished -
# nothing will ever mount it again while it stays plugged in. So on Linux we
# mount removable filesystems ourselves through udisksctl, which needs no
# root, and simply remember the ones we have already finished with.
# --------------------------------------------------------------------------

MOUNT_RETRY = 30.0
_mount_attempts = {}


def usb_filesystems():
    """Removable USB filesystems, whether or not they are mounted."""
    if platform.system() != "Linux":
        return []
    try:
        result = subprocess.run(
            ["lsblk", "--json", "-o",
             "PATH,LABEL,FSTYPE,MOUNTPOINT,TRAN,UUID,TYPE"],
            capture_output=True, text=True, timeout=10)
        data = json.loads(result.stdout or "{}")
    except (OSError, ValueError, subprocess.SubprocessError):
        return []

    found = []

    def walk(nodes, transport):
        for node in nodes:
            carried = node.get("tran") or transport
            if node.get("fstype") and carried == "usb":
                found.append({
                    "path": node.get("path"),
                    "label": node.get("label") or "",
                    "uuid": node.get("uuid") or node.get("path"),
                    "mountpoint": node.get("mountpoint"),
                })
            walk(node.get("children") or [], carried)

    walk(data.get("blockdevices", []), None)
    return found


def mount_new_media(skip_uuids):
    """Mount anything removable that is not mounted and not already done."""
    for device in usb_filesystems():
        if device["mountpoint"] or not device["path"]:
            continue
        if device["uuid"] in skip_uuids:
            continue

        moment = time.monotonic()
        if moment - _mount_attempts.get(device["path"], 0) < MOUNT_RETRY:
            continue
        _mount_attempts[device["path"]] = moment

        try:
            result = subprocess.run(
                ["udisksctl", "mount", "-b", device["path"],
                 "--no-user-interaction"],
                capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.SubprocessError) as exc:
            log(f"could not mount {device['path']}: {exc}")
            continue

        name = device["label"] or device["path"]
        if result.returncode == 0:
            log(f"mounted {name}")
        else:
            message = (result.stderr or result.stdout).strip().splitlines()
            log(f"could not mount {name}: {message[-1] if message else '?'}")


def volume_uuid(volume):
    for device in usb_filesystems():
        if device["mountpoint"] == str(volume):
            return device["uuid"]
    return None


def find_volumes():
    found = []
    roots = [r for r in mount_roots() if r.is_dir()]
    # /media contains /media/<user>, which is itself a mount root on Linux,
    # not a device. Never report a root as a volume.
    root_set = {r.resolve() for r in roots}
    for root in roots:
        if not root.is_dir():
            continue
        try:
            entries = list(root.iterdir())
        except OSError:
            continue
        for entry in entries:
            if entry.name.startswith(".") or entry.name in IGNORED_NAMES:
                continue
            if not entry.is_dir():
                continue
            if entry.is_symlink():
                continue
            try:
                if entry.resolve() in root_set:
                    continue
            except OSError:
                continue
            if VOLUME_FILTER and VOLUME_FILTER.lower() not in entry.name.lower():
                continue
            if entry not in found:
                found.append(entry)
    return found


def device_kind(volume):
    """Return "bracelet", "camera" or None for a mounted volume."""
    try:
        if REQUIRED_GLOB and (any(volume.glob(REQUIRED_GLOB))
                              or any(volume.glob(REQUIRED_GLOB.lower()))):
            return "bracelet"
    except OSError:
        return None

    # A second camera's built-in storage mounts as OsmoAction1 and so on,
    # so match on the prefix rather than the exact label.
    if any(volume.name.upper().startswith(label)
           for label in CAMERA_SKIP_LABELS):
        return None
    dcim = volume / "DCIM"
    try:
        if dcim.is_dir() and any(p.is_dir() and p.name.upper().startswith("DJI")
                                 for p in dcim.iterdir()):
            return "camera"
    except OSError:
        pass
    return None


def is_sensor(volume):
    """Any volume we know how to collect from."""
    return device_kind(volume) is not None


# Directories that hold OS bookkeeping, never sensor data. On macOS these
# are hidden by a leading dot, but Linux shows their contents plainly, so
# filtering on the file name alone is not enough.
JUNK_DIRS = {
    ".spotlight-v100", ".fseventsd", ".trashes", ".temporaryitems",
    ".documentrevisions-v100", "system volume information",
    "$recycle.bin", "found.000", ".ds_store",
}


def in_junk_dir(path, volume):
    """True if any folder between the volume root and the file is OS junk."""
    try:
        parts = path.relative_to(volume).parts[:-1]
    except ValueError:
        return False
    return any(p.lower() in JUNK_DIRS or p.startswith(".") for p in parts)


def data_files(volume):
    files = []
    try:
        for path in volume.rglob("*"):
            if path.name.startswith(".") or not path.is_file():
                continue
            if path.is_symlink() or in_junk_dir(path, volume):
                continue
            if DATA_SUFFIXES and path.suffix.lower() not in DATA_SUFFIXES:
                continue
            try:
                if path.stat().st_size < MIN_FILE_SIZE:
                    continue
            except OSError:
                continue
            files.append(path)
    except OSError as exc:
        log(f"  volume unreadable: {exc}")
    return sorted(files)


def digest(path, chunk=1024 * 1024):
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


# --------------------------------------------------------------------------
# Single-instance lock
# --------------------------------------------------------------------------

def lock_path(name="collector"):
    return OUT_DIR / f"{name}.lock"


class Busy(Exception):
    """Another instance is already working on this volume."""


def holder_alive(text):
    """Is the process that wrote this lock still running?

    A lock left behind by a killed collector - a service restart during a
    copy, say - would otherwise block the device until it went stale.
    """
    match = re.search(r"pid (\d+)", text or "")
    if not match:
        return True
    try:
        os.kill(int(match.group(1)), 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def clear_stuck_state():
    """Reset devices left mid-copy by a collector that is no longer running."""
    folder = state_dir()
    if not folder.is_dir():
        return
    for path in sorted(folder.glob("*.json")):
        if path.stem == "uploader":
            continue
        if read_state(path.stem).get("phase") in ("checking", "copying",
                                                  "decoding"):
            write_state(path.stem, phase="interrupted", finished=now_iso(),
                        error="collector stopped mid-copy - "
                              "reconnect the device to resume")
            log(f"{path.stem}: was left mid-copy, marked for resume")

    for lock in sorted(OUT_DIR.glob("collector*.lock")):
        try:
            if not holder_alive(lock.read_text()):
                lock.unlink()
                log(f"removed stale lock {lock.name}")
        except OSError:
            pass


def acquire_lock(stale_after=900, name="collector"):
    """Take an exclusive lock, or raise Busy.

    The monitor and the launchd agent both react to a mount, so without
    this they race for the same files: whoever loses copies nothing and
    the card may end up not cleared. A stale lock left by a crashed run
    is ignored after stale_after seconds.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = lock_path(name)

    if path.exists():
        try:
            text = path.read_text().strip()
            age = time.time() - path.stat().st_mtime
            if age < stale_after and holder_alive(text):
                raise Busy(text or "another instance")
            path.unlink()          # dead owner, or long forgotten
        except OSError:
            pass

    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise Busy("another instance")
    with os.fdopen(fd, "w") as handle:
        handle.write(f"pid {os.getpid()}\n")
    return path


def release_lock(name="collector"):
    try:
        lock_path(name).unlink()
    except OSError:
        pass


# --------------------------------------------------------------------------
# Main workflow
# --------------------------------------------------------------------------

def copy_verified(source, target, chunk=1024 * 1024, on_progress=None):
    """Copy in a single pass, hashing the bytes as they are read.

    Important: the sensor keeps writing to the card while connected, so
    the source must not be read twice - it can grow between passes and
    the checksums would differ for no real reason. We hash exactly what
    we read, then compare it against what landed on disk.
    """
    # Stat the source before touching anything. If the device has gone
    # away, give up immediately: carrying on would compare the partial
    # file against a size of zero and throw away a good resume point.
    total = source.stat().st_size

    # Copy into a .part file and rename only once it is verified. A half
    # copied file therefore never looks finished, and an interrupted
    # transfer can pick up where it stopped instead of starting over.
    part = target.with_name(target.name + ".part")
    resume_from = 0
    source_hash = hashlib.sha256()
    if part.exists():
        done = part.stat().st_size
        if 0 < done <= total:
            with open(part, "rb") as existing:
                while True:
                    block = existing.read(chunk)
                    if not block:
                        break
                    source_hash.update(block)
            resume_from = done
        else:
            part.unlink()

    copied_bytes = resume_from
    mode = "ab" if resume_from else "wb"
    with open(source, "rb") as src, open(part, mode) as dst:
        if resume_from:
            src.seek(resume_from)
        while True:
            block = src.read(chunk)
            if not block:
                break
            dst.write(block)
            source_hash.update(block)
            copied_bytes += len(block)
            if on_progress:
                on_progress(copied_bytes, total)

    target_hash = hashlib.sha256()
    with open(part, "rb") as dst:
        while True:
            block = dst.read(chunk)
            if not block:
                break
            target_hash.update(block)

    if source_hash.hexdigest() != target_hash.hexdigest():
        part.unlink()
        raise OSError("copy does not match the bytes read")

    part.replace(target)
    return copied_bytes



def camera_files(volume):
    """Video files in DCIM, oldest first. Proxies only when requested."""
    wanted = set(CAMERA_VIDEO_EXTS)
    if CAMERA_KEEP_LRF:
        wanted.add(".LRF")
    found = []
    dcim = volume / "DCIM"
    try:
        for path in dcim.rglob("*"):
            if path.name.startswith(".") or not path.is_file():
                continue
            if path.suffix.upper() in wanted:
                found.append(path)
    except OSError as exc:
        log(f"  volume unreadable: {exc}")
    return sorted(found, key=lambda p: p.name)


def video_stamp(path):
    """DJI_20260920191737_0001_D.MP4 -> 2026-09-20T19:17:37, or None."""
    parts = path.stem.split("_")
    if len(parts) >= 2 and len(parts[1]) == 14 and parts[1].isdigit():
        try:
            return datetime.strptime(parts[1], "%Y%m%d%H%M%S").isoformat()
        except ValueError:
            return None
    return None


def open_session(device_dir):
    """Folder for this collection, reusing one that was left unfinished.

    Partial .part files live inside the session folder, so a copy that was
    cut short can only resume if the next attempt lands in the same place.
    A fresh folder every time would silently restart every transfer.
    """
    sessions = device_dir / "sessions"
    sessions.mkdir(parents=True, exist_ok=True)
    try:
        existing = sorted((p for p in sessions.iterdir() if p.is_dir()),
                          key=lambda p: p.name, reverse=True)
    except OSError:
        existing = []
    for path in existing:
        if not (path / ".complete").exists() and not (path / ".uploaded").exists():
            return path
    path = sessions / datetime.now().strftime("%Y%m%d_%H%M%S")
    path.mkdir(parents=True, exist_ok=True)
    return path


def _collect_camera(volume, delete_after, progress=None, done_bytes=None):
    seen = load_state()
    everything = camera_files(volume)
    fresh = [f for f in everything if file_key(f, volume.name) not in seen]

    if not fresh:
        log(f"camera {volume.name}: nothing new ({len(everything)} already collected)")
        return False

    total = sum(f.stat().st_size for f in fresh)
    log(f"camera {volume.name}: {len(fresh)} new videos, {human(total)}")

    session = open_session(OUT_DIR / safe_name(volume.name))

    copied, failed = [], []
    interrupted = False
    for source in fresh:
        if not volume.is_dir():
            interrupted = True
            break
        target = session / source.name
        ok, size_bytes = False, 0
        for attempt in range(1, COPY_ATTEMPTS + 1):
            try:
                hook = None
                if progress:
                    hook = lambda got, tot, n=source.name: progress(got, tot, n)
                size_bytes = copy_verified(source, target, on_progress=hook)
                if done_bytes is not None:
                    done_bytes[0] += size_bytes
                ok = True
                break
            except OSError as exc:
                if not volume.is_dir():
                    # Unplugged mid-copy. Retrying against a path that no
                    # longer exists only produces confusing errors.
                    log(f"  {source.name}: device disconnected during copy")
                    interrupted = True
                    break
                log(f"  {source.name}: attempt {attempt} failed - {exc}")
                # The .part file is deliberately kept: the next attempt,
                # or the next time the device is plugged in, resumes from it.
                if attempt < COPY_ATTEMPTS:
                    time.sleep(RETRY_DELAY)
                if not source.exists() or not volume.is_dir():
                    log(f"  {source.name}: device disconnected during copy")
                    interrupted = True
                    break

        if interrupted:
            break
        if not ok:
            log(f"  {source.name} skipped, original left on the camera")
            failed.append(source)
            continue

        seen.add(file_key(source, volume.name))
        copied.append((source, target))
        log(f"  {source.name}  {human(size_bytes)}  ok")

        try:
            target.with_suffix(".meta.json").write_text(json.dumps({
                "file": target.name,
                "kind": "video",
                "bytes": size_bytes,
                "start": video_stamp(source),
                "end": video_stamp(source),
                "samples": 0,
            }, indent=1))
        except OSError:
            pass

    save_state(seen)

    if interrupted:
        write_state(safe_name(volume.name), phase="interrupted",
                    finished=now_iso(),
                    error="unplugged during copy - will resume when reconnected")
        log("  interrupted; partial file kept, will resume on reconnect")
        return False

    if not copied:
        write_state(safe_name(volume.name), phase="error", finished=now_iso(),
                    error=f"{len(failed)} videos unreadable")
        notify("Camera download failed",
               f"{len(failed)} videos unreadable. Camera data is intact.")
        return False

    log(f"saved {len(copied)} videos to {session}")

    if delete_after:
        removed = 0
        for source, _ in copied:
            try:
                source.unlink()
                removed += 1
                # The proxy is useless without its video
                proxy = source.with_suffix(".LRF")
                if not CAMERA_KEEP_LRF and proxy.exists():
                    proxy.unlink()
                # Gallery thumbnails in MISC/THM share the video's stem
                # exactly (DJI_..._0001_D.THM / .SCR), so matching on the
                # full stem cannot touch another recording's files.
                thumbs = volume / "MISC" / "THM"
                if thumbs.is_dir():
                    for leftover in thumbs.rglob(source.stem + ".*"):
                        if leftover.suffix.upper() in (".THM", ".SCR"):
                            try:
                                leftover.unlink()
                            except OSError:
                                pass
            except OSError as exc:
                log(f"  could not delete {source.name}: {exc}")
        log(f"  removed {removed} videos from the camera")
    else:
        log("  camera not cleared (no --delete)")

    copied_bytes = sum(t.stat().st_size for _, t in copied)
    (session / ".complete").write_text(now_iso())
    write_state(safe_name(volume.name), phase="done", kind="camera",
                finished=now_iso(), last_done=now_iso(), files=len(copied),
                bytes=copied_bytes, percent=100, error=None)
    notify("Camera footage collected",
           f"{len(copied)} videos, {human(copied_bytes)}. The camera can be taken.")
    unmount(volume)
    return True


def collect(volume, delete_after, progress=None, done_bytes=None):
    """Collect new files from a volume. True if anything was copied."""
    if not is_sensor(volume):
        return False

    dev = safe_name(volume.name)
    # One lock per device, not one for the whole program: two cameras in
    # the dock must not block each other.
    lock_name = f"collector-{dev}"
    try:
        acquire_lock(name=lock_name)
    except Busy as who:
        log(f"volume {volume.name}: skipped, {who} is already collecting it")
        return False

    kind = device_kind(volume)
    try:
        files = camera_files(volume) if kind == "camera" else data_files(volume)
        seen = load_state()
        total = sum(f.stat().st_size for f in files
                    if file_key(f, volume.name) not in seen)
    except OSError:
        total = 0

    write_state(dev, phase="checking", kind=kind, started=now_iso(),
                volume=str(volume), total=total, percent=0, file=None, error=None)

    if done_bytes is None:
        done_bytes = [0]
    last_write = [0.0]
    window = []          # recent (time, bytes) pairs, for a live speed
    began = time.monotonic()

    def tracker(got, file_total, name=""):
        if progress:
            progress(got, file_total, name)
        moment = time.monotonic()
        overall = done_bytes[0] + got
        window.append((moment, overall))
        while len(window) > 2 and moment - window[0][0] > 10:
            window.pop(0)

        if moment - last_write[0] < 0.5:
            return
        last_write[0] = moment

        # Speed over the last few seconds rather than the whole transfer:
        # a resumed copy would otherwise look impossibly fast at first.
        speed = 0.0
        span = window[-1][0] - window[0][0]
        if span > 0.5:
            speed = (window[-1][1] - window[0][1]) / span
        left = max(0, total - overall)
        percent = int(100 * overall / total) if total else 0
        write_state(dev, phase="copying", file=name, copied=overall,
                    total=total, speed=speed,
                    eta=(left / speed) if speed > 1 else None,
                    percent=min(percent, 99))

    try:
        if kind == "camera":
            ok = _collect_camera(volume, delete_after, tracker, done_bytes)
        else:
            ok = _collect(volume, delete_after, tracker, done_bytes)
    except Exception as exc:
        write_state(dev, phase="error", finished=now_iso(), error=str(exc)[:200])
        raise
    finally:
        release_lock(lock_name)

    if ok:
        write_state(dev, took=time.monotonic() - began, speed=None, eta=None)
    elif read_state(dev).get("phase") in ("checking", "copying", "decoding"):
        write_state(dev, phase="nothing_new", finished=now_iso())
    return ok


def _collect(volume, delete_after, progress=None, done_bytes=None):
    seen = load_state()
    everything = data_files(volume)
    if not everything:
        return False

    fresh = [f for f in everything if file_key(f, volume.name) not in seen]
    if not fresh:
        log(f"volume {volume.name}: nothing new ({len(everything)} already collected)")
        return False

    total_mb = sum(f.stat().st_size for f in fresh) / (1024 * 1024)
    log(f"volume {volume.name}: {len(fresh)} new files, {total_mb:.1f} MB")

    # Each sensor gets its own folder, keyed on the volume label.
    # Rename a card with: diskutil rename "/Volumes/NO NAME" COW01
    session = open_session(OUT_DIR / safe_name(volume.name))

    copied = []
    failed = []
    interrupted = False
    for source in fresh:
        if not volume.is_dir():
            interrupted = True
            break
        target = session / source.name
        counter = 1
        while target.exists():
            target = session / f"{source.stem}_{counter}{source.suffix}"
            counter += 1

        ok = False
        size_bytes = 0
        for attempt in range(1, COPY_ATTEMPTS + 1):
            try:
                hook = None
                if progress:
                    hook = lambda got, tot: progress(got, tot, source.name)
                size_bytes = copy_verified(source, target, on_progress=hook)
                if done_bytes is not None:
                    done_bytes[0] += size_bytes
                ok = True
                break
            except OSError as exc:
                if not volume.is_dir():
                    log(f"  {source.name}: device disconnected during copy")
                    interrupted = True
                    break
                log(f"  {source.name}: attempt {attempt} failed - {exc}")
                # The .part file is deliberately kept: the next attempt,
                # or the next time the device is plugged in, resumes from it.
                if attempt < COPY_ATTEMPTS:
                    time.sleep(RETRY_DELAY)
                if not source.exists() or not volume.is_dir():
                    log(f"  {source.name}: device disconnected during copy")
                    interrupted = True
                    break

        if interrupted:
            break
        if not ok:
            log(f"  {source.name} skipped, original left on the card")
            failed.append(source)
            continue

        seen.add(file_key(source, volume.name))
        copied.append((source, target))
        log(f"  {source.name}  {size_bytes / (1024 * 1024):.1f} MB  ok")

    save_state(seen)

    if interrupted:
        write_state(safe_name(volume.name), phase="interrupted",
                    finished=now_iso(),
                    error="unplugged during copy - will resume when reconnected")
        log("  interrupted; partial file kept, will resume on reconnect")
        return False

    if not copied:
        log("nothing could be copied")
        write_state(safe_name(volume.name), phase="error", finished=now_iso(),
                    error=f"{len(failed)} files unreadable")
        notify("Download failed",
               f"{len(failed)} files unreadable. Card data is intact.")
        return False

    log(f"saved {len(copied)} files to {session}")
    if failed:
        names = ", ".join(f.name for f in failed)
        log(f"unreadable, left on the card: {names}")

    # Decode to JSON Lines, merging across file boundaries
    write_state(safe_name(volume.name), phase="decoding")
    raw_files = [t for _, t in copied]
    samples_total, jsonl_total = convert_all(raw_files, session)
    if jsonl_total:
        log(f"decoded {samples_total} samples into {jsonl_total} .jsonl files")

        # Originals are redundant now: everything moved into .jsonl.
        # Decoded sessions are more complete and easier to work with.
        if not KEEP_RAW:
            dropped = 0
            for path in raw_files:
                if path.suffix.upper() != ".TXT":
                    continue
                if path.stem.upper().startswith("WIT"):
                    try:
                        path.unlink()
                        dropped += 1
                    except OSError:
                        pass
            if dropped:
                log(f"  removed {dropped} raw .TXT files, kept the .jsonl")
    elif raw_files:
        log("  nothing to decode, raw .TXT files kept")

    removed = 0
    if delete_after:
        # The sensor writes while docked too. The newest file is most
        # likely still active - deleting it would discard data written
        # after our copy finished.
        active = None
        try:
            active = max((s for s, _ in copied), key=lambda p: p.stat().st_mtime)
        except (OSError, ValueError):
            pass

        for source, _ in copied:
            if source.name.upper() in PROTECTED_NAMES:
                log(f"  {source.name} kept on the card (config file)")
                continue
            if active is not None and source == active:
                log(f"  {source.name} kept: the sensor is probably still writing to it")
                continue
            try:
                source.unlink()
                removed += 1
            except OSError as exc:
                log(f"  could not delete {source.name}: {exc}")
        log(f"  removed {removed} files from the card")
    else:
        log("  card not cleared (no --delete)")

    (session / ".complete").write_text(now_iso())
    write_state(safe_name(volume.name), phase="done", kind="bracelet",
                finished=now_iso(), last_done=now_iso(), files=len(copied),
                bytes=dir_bytes(session), samples=samples_total,
                percent=100, error=None)
    notify(
        "Sensor data collected",
        f"{len(copied)} files, {total_mb:.0f} MB, "
        f"{samples_total} samples. The sensor can be taken.",
    )
    unmount(volume)
    return True


# --------------------------------------------------------------------------
# Upload to cloud storage (rclone)
# --------------------------------------------------------------------------

MARKERS = (".complete", ".uploaded")


def session_dirs():
    found = []
    if not OUT_DIR.is_dir():
        return found
    for device in sorted(OUT_DIR.iterdir()):
        sessions = device / "sessions"
        if device.is_dir() and sessions.is_dir():
            found.extend(p for p in sessions.iterdir() if p.is_dir())
    return found


def device_busy(device):
    """True while a collector is working on this device."""
    if read_state(device).get("phase") in ("checking", "copying", "decoding"):
        lock = lock_path(f"collector-{device}")
        try:
            if lock.exists() and holder_alive(lock.read_text()):
                return True
        except OSError:
            return True
    return False


def session_complete(path):
    """Is this session finished and safe to send to the cloud?

    Uploading a session that is still being written makes rclone fail
    with "source file is being updated" - and worse, could publish half
    a video. A session folder is reused while a transfer resumes, so its
    age alone says nothing.
    """
    try:
        if any(path.glob("*.part")):
            return False          # a transfer is still unfinished here
        if device_busy(path.parent.parent.name):
            return False
    except OSError:
        return False

    if (path / ".complete").exists():
        return True
    # Sessions collected before completion markers existed: trust them
    # once they have been quiet for an hour.
    try:
        return time.time() - path.stat().st_mtime > 3600 and any(path.iterdir())
    except OSError:
        return False


def dir_bytes(path):
    total = 0
    for item in path.rglob("*"):
        if item.is_file() and item.name not in MARKERS \
                and item.suffix != ".part":
            try:
                total += item.stat().st_size
            except OSError:
                pass
    return total


def pending_sessions():
    return sorted((p for p in session_dirs()
                   if not (p / ".uploaded").exists() and session_complete(p)),
                  key=lambda p: p.name)


def rclone_args():
    args = []
    for marker in MARKERS:
        args += ["--exclude", marker]
    # Never publish a partially transferred file
    args += ["--exclude", "*.part"]
    return args


def upload_session(path, rclone):
    device = path.parent.parent.name
    label = f"{device}/{path.name}"
    dest = f"{UPLOAD_REMOTE.rstrip('/')}/{device}/{path.name}"
    size = dir_bytes(path)

    write_state("uploader", phase="uploading", session=label, bytes=size,
                percent=0, started=now_iso(), error=None)
    log(f"upload {label} ({human(size)}) -> {dest}")

    cmd = [rclone, "copy", str(path), dest, "--checksum",
           "--retries", "3", "--low-level-retries", "10",
           "--stats", "2s", "--stats-one-line",
           "--stats-log-level", "NOTICE"] + rclone_args()
    last_error = ""
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            match = re.search(r"(\d+)%", line)
            if match:
                pace = re.search(r"([\d.]+\s*[KMGT]?i?B/s)", line)
                eta = re.search(r"ETA\s+(\S+)", line)
                write_state("uploader", percent=int(match.group(1)),
                            speed=pace.group(1) if pace else None,
                            eta=eta.group(1) if eta else None)
            if "ERROR" in line:
                last_error = line.strip()[-200:]
        code = proc.wait()
    except OSError as exc:
        code, last_error = -1, str(exc)

    if code != 0:
        message = last_error or f"rclone exited with code {code}"
        write_state("uploader", phase="error", error=message, failed_at=now_iso())
        log(f"  upload failed: {message}")
        return False

    # Only a remote copy that rclone has checked counts as uploaded
    check = subprocess.run([rclone, "check", str(path), dest, "--one-way"]
                           + rclone_args(), capture_output=True, text=True)
    if check.returncode != 0:
        message = "remote copy does not match local files"
        write_state("uploader", phase="error", error=message, failed_at=now_iso())
        log(f"  upload failed: {message}")
        return False

    (path / ".uploaded").write_text(json.dumps(
        {"uploaded": now_iso(), "dest": dest, "bytes": size}, indent=1))

    if DELETE_AFTER_UPLOAD:
        for item in path.iterdir():
            if item.is_file() and item.name not in MARKERS \
                    and not item.name.endswith(".meta.json"):
                try:
                    item.unlink()
                except OSError:
                    pass

    write_state("uploader", last_ok=now_iso(), last_session=label, percent=100)
    log(f"  uploaded and verified {label}")
    return True


def upload_pass():
    """Upload every finished session that is not in the cloud yet."""
    if not UPLOAD_REMOTE:
        return
    pending = pending_sessions()
    write_state("uploader", remote=UPLOAD_REMOTE, pending=len(pending),
                pending_bytes=sum(dir_bytes(p) for p in pending))

    rclone = shutil.which("rclone")
    if not rclone:
        write_state("uploader", phase="error", error="rclone is not installed")
        return
    if not pending:
        write_state("uploader", phase="idle", error=None)
        return

    try:
        acquire_lock(stale_after=6 * 3600, name="uploader")
    except Busy:
        return
    try:
        for index, path in enumerate(pending):
            if not upload_session(path, rclone):
                return
            rest = pending[index + 1:]
            write_state("uploader", pending=len(rest),
                        pending_bytes=sum(dir_bytes(p) for p in rest))
        write_state("uploader", phase="idle", error=None)
    finally:
        release_lock("uploader")


def uploader_loop():
    while True:
        try:
            upload_pass()
        except Exception as exc:
            log(f"uploader error: {exc}")
            write_state("uploader", phase="error", error=str(exc)[:200])
        time.sleep(UPLOAD_INTERVAL)


def run_once(delete_after):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    clear_stuck_state()
    time.sleep(SETTLE_DELAY)
    volumes = find_volumes()
    if not volumes:
        log("triggered on connect: no removable volumes")
        return 0

    worked = False
    for volume in volumes:
        if collect(volume, delete_after):
            worked = True

    if not worked:
        names = ", ".join(v.name for v in volumes)
        log(f"triggered on connect: no new recordings on ({names})")
    upload_pass()
    return 0


def human(size):
    for unit in ("B", "KB", "MB", "GB"):
        if abs(size) < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit != "GB" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


class Screen:
    """Live single-line output: redraws the line in place."""

    def __init__(self, enabled=True):
        self.enabled = enabled and sys.stdout.isatty()
        self.width = 0

    def line(self, text):
        if not self.enabled:
            return
        padded = text.ljust(self.width)
        self.width = max(self.width, len(text))
        print("\r" + padded, end="", flush=True)

    def done(self, text=""):
        if not self.enabled:
            if text:
                print(text, flush=True)
            return
        print("\r" + text.ljust(self.width), flush=True)
        self.width = 0


def bar(fraction, width=24):
    filled = int(round(fraction * width))
    return "█" * filled + "░" * (width - filled)


def monitor(delete_after):
    """Watch the dock and show what is happening in the terminal."""
    globals()["QUIET"] = True      # keep the log from breaking the live view
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    clear_stuck_state()
    screen = Screen()

    print()
    print("  WT901SDCL-BT50 monitor")
    print(f"  data:    {OUT_DIR}")
    print(f"  cleanup: {'on' if delete_after else 'off'}")
    print("  quit:    Ctrl+C")
    print()

    known = set()
    retry_at = {}
    spinner = "|/-\\"
    tick = 0

    while True:
        volumes = find_volumes()
        sensors = [v for v in volumes if is_sensor(v)]
        current = {v.name for v in sensors}
        known &= current
        retry_at = {k: v for k, v in retry_at.items() if k in current}

        fresh = [v for v in sensors if v.name not in known
                 and time.monotonic() >= retry_at.get(v.name, 0)]

        if not fresh:
            tick += 1
            mark = spinner[tick % len(spinner)]
            if sensors:
                screen.line(f"  {mark} sensor docked, nothing new")
            else:
                screen.line(f"  {mark} waiting for the sensor")
            time.sleep(1.0)
            continue

        for volume in fresh:
            screen.done(f"  * sensor connected: {volume.name}")
            files = [f for f in data_files(volume)
                     if file_key(f, volume.name) not in load_state()]
            total_bytes = sum(f.stat().st_size for f in files)
            print(f"    {len(files)} new files, {human(total_bytes)}")

            started = time.monotonic()
            done_bytes = [0]

            def progress(current_bytes, file_total, name=""):
                overall = done_bytes[0] + current_bytes
                elapsed = time.monotonic() - started
                speed = overall / elapsed / 1024 if elapsed > 0.5 else 0
                fraction = overall / total_bytes if total_bytes else 0
                screen.line(f"    {bar(fraction)} {fraction * 100:3.0f}%  "
                            f"{human(overall)}  {speed:.0f} KB/s  {name}")

            result = collect(volume, delete_after, progress, done_bytes)
            phase = read_state(safe_name(volume.name)).get("phase")
            if result or phase == "nothing_new":
                known.add(volume.name)
            else:
                retry_at[volume.name] = time.monotonic() + DEVICE_RETRY
                print(f"    will retry in {DEVICE_RETRY:.0f} s")
            screen.done()
            if result:
                made = sorted((OUT_DIR / "sessions").glob("*/session_*.jsonl"),
                              key=lambda p: p.stat().st_mtime, reverse=True)
                for path in made[:5]:
                    if time.time() - path.stat().st_mtime < 120:
                        rows = sum(1 for _ in open(path))
                        print(f"    + {path.name}  {rows} samples")
                print(f"    done in {time.monotonic() - started:.0f} s, "
                      f"the sensor can be taken")
            else:
                print("    no new recordings found")
            print()


def watch(delete_after):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    log(f"watching for a connection, polling every {POLL_INTERVAL:.0f} s")
    log(f"writing data to {OUT_DIR}")
    if delete_after:
        log("card cleanup enabled")
    if VOLUME_FILTER:
        log(f"volume filter: {VOLUME_FILTER!r}")
    if UPLOAD_REMOTE:
        log(f"uploading to {UPLOAD_REMOTE} every {UPLOAD_INTERVAL:.0f} s")
        threading.Thread(target=uploader_loop, daemon=True).start()
    clear_stuck_state()

    done = set()        # finished while this volume stayed mounted
    retry_at = {}       # volumes that failed, and when to try them again
    active = {}         # volumes being collected right now
    results = {}        # what each finished worker reported
    settled = set()     # uuids we finished with; do not mount them again

    def worker(volume):
        name = volume.name
        try:
            time.sleep(SETTLE_DELAY)
            if not volume.is_dir():
                results[name] = False
                return
            uuid = volume_uuid(volume)
            ok = collect(volume, delete_after)
            phase = read_state(safe_name(name)).get("phase")
            finished = bool(ok) or phase == "nothing_new"
            if finished and uuid:
                # We unmounted it on purpose; leave it alone until the
                # device is physically unplugged and put back.
                settled.add(uuid)
            results[name] = finished
        except Exception as exc:
            log(f"volume {name}: collection crashed - {exc}")
            results[name] = False

    while True:
        attached = {d["uuid"] for d in usb_filesystems()}
        settled &= attached          # forget devices that were taken away
        mount_new_media(settled)

        volumes = find_volumes()
        current = {v.name for v in volumes}
        done &= current                       # forget volumes that went away
        retry_at = {k: v for k, v in retry_at.items() if k in current}

        # Collect finished workers first so their slots free up
        for name, thread in list(active.items()):
            if thread.is_alive():
                continue
            del active[name]
            if results.pop(name, False):
                done.add(name)
                retry_at.pop(name, None)
            else:
                # A yanked cable, a busy device or an unreadable file: try
                # again shortly. Writing the device off until it is
                # unplugged would leave it stuck showing an old error.
                retry_at[name] = time.monotonic() + DEVICE_RETRY
                log(f"volume {name}: will retry in {DEVICE_RETRY:.0f} s")

        for volume in volumes:
            name = volume.name
            if name in done or name in active:
                continue
            if not device_kind(volume):
                done.add(name)          # not ours; ignore it quietly
                continue
            if time.monotonic() < retry_at.get(name, 0):
                continue
            if len(active) >= MAX_PARALLEL:
                break

            # One thread per device: a camera copying 30 GB must not hold
            # up the bracelet plugged in beside it.
            thread = threading.Thread(target=worker, args=(volume,), daemon=True)
            active[name] = thread
            thread.start()

        time.sleep(POLL_INTERVAL if not active else 1.0)


def list_volumes():
    volumes = find_volumes()
    if not volumes:
        print("\nNo removable volumes found. Is the sensor plugged in?\n")
        return
    seen = load_state()
    print()
    for volume in volumes:
        kind = device_kind(volume)
        if kind == "camera":
            files = camera_files(volume)
        elif kind == "bracelet":
            files = data_files(volume)
        else:
            print(f"  {volume}  (not a known device, ignored)")
            continue
        fresh = [f for f in files if file_key(f, volume.name) not in seen]
        size = sum(f.stat().st_size for f in files) / (1024 * 1024)
        print(f"  {volume}  [{kind}]")
        print(f"    {len(files)} files, {len(fresh)} new, {size:.1f} MB")
        for sample in files[:5]:
            print(f"      {sample.name}")
        if len(files) > 5:
            print(f"      ... and {len(files) - 5} more")
    print()


# ==========================================================================
# SENSOR CONFIGURATION OVER BLUETOOTH
#
# Needs bleak, imported lazily: USB collection runs on the system
# Python with no third-party packages, so the auto-run agent does not
# depend on any virtualenv being active.
# ==========================================================================

DEVICE_NAME_HINT = "WT"        # substring of the advertised BLE name
SCAN_SECONDS = 8.0

# WitMotion module characteristics. Falls back to auto-discovery.
WRITE_CHAR_GUESS = "0000ffe9-0000-1000-8000-00805f9a34fb"
NOTIFY_CHAR_GUESS = "0000ffe4-0000-1000-8000-00805f9a34fb"

UNLOCK = bytes([0xFF, 0xAA, 0x69, 0x88, 0xB5])
SAVE = bytes([0xFF, 0xAA, 0x00, 0x00, 0x00])

# Register, value, description.
#
# RSW (output content) is deliberately left alone: the sensor already
# sends 0x61 packets with accel, gyro, angles and time, no magnetometer.
# That is exactly what we want, and poking at the extended BLE 5.0
# format risks breaking it.
SETTINGS = [
    (0x03, 0x09, "RRATE: 100 Hz"),
    (0x1F, 0x03, "BANDWIDTH: 42 Hz"),
]

READADDR = 0x27

RATE_NAMES = {
    0x01: "0.2 Hz", 0x02: "0.5 Hz", 0x03: "1 Hz", 0x04: "2 Hz",
    0x05: "5 Hz", 0x06: "10 Hz", 0x07: "20 Hz", 0x08: "50 Hz",
    0x09: "100 Hz", 0x0B: "200 Hz",
}
BW_NAMES = {
    0x00: "256 Hz", 0x01: "188 Hz", 0x02: "98 Hz", 0x03: "42 Hz",
    0x04: "20 Hz", 0x05: "10 Hz", 0x06: "5 Hz",
}
RSW_BITS = [
    (0, "time"), (1, "acceleration"), (2, "gyroscope"), (3, "angle"),
    (4, "magnetic field"), (5, "port"), (6, "pressure"),
]


replies = []


def need_bleak():
    try:
        from bleak import BleakClient, BleakScanner
        return BleakClient, BleakScanner
    except ImportError:
        print("\nBluetooth features need the bleak package:")
        print("    pip install bleak")
        print("\nOr activate the virtualenv where it is already installed.\n")
        raise SystemExit(1)


def write_cmd(register, value):
    """FF AA <register> <low byte> <high byte>"""
    return bytes([0xFF, 0xAA, register, value & 0xFF, (value >> 8) & 0xFF])


def on_notify(_sender, data: bytearray):
    replies.append(bytes(data))


def decode_rsw(value):
    active = [name for bit, name in RSW_BITS if value & (1 << bit)]
    return ", ".join(active) if active else "nothing"


async def find_device():
    _, BleakScanner = need_bleak()
    print(f"scanning for the sensor, {SCAN_SECONDS:.0f} seconds ...")
    devices = await BleakScanner.discover(timeout=SCAN_SECONDS)

    matches = [d for d in devices
               if DEVICE_NAME_HINT.lower() in (d.name or "").lower()]
    if matches:
        return matches, devices
    return [], devices


async def pick_chars(client):
    """Locate the write and notify characteristics."""
    write_char = notify_char = None

    for service in client.services:
        for char in service.characteristics:
            props = char.properties
            uuid = char.uuid.lower()

            if uuid == WRITE_CHAR_GUESS and ("write" in props or
                                             "write-without-response" in props):
                write_char = char
            if uuid == NOTIFY_CHAR_GUESS and "notify" in props:
                notify_char = char

    if write_char and notify_char:
        return write_char, notify_char

    # Fallback: first characteristic with matching properties
    for service in client.services:
        for char in service.characteristics:
            props = char.properties
            if write_char is None and ("write" in props or
                                       "write-without-response" in props):
                write_char = char
            if notify_char is None and "notify" in props:
                notify_char = char

    return write_char, notify_char


async def read_registers(client, write_char, first_register):
    """Read 4 consecutive registers starting at the given address."""
    replies.clear()

    # The docs require unlocking before touching registers. Reads do not
    # strictly need it, but some firmwares stay silent without it.
    await client.write_gatt_char(write_char, UNLOCK, response=False)
    await asyncio.sleep(0.3)

    await client.write_gatt_char(write_char, write_cmd(READADDR, first_register),
                                 response=False)
    await asyncio.sleep(2.0)

    for packet in ble_all_packets():
        # BLE 5.0 models answer reads with packet type 0x71:
        # 55 71 <address> <REG1> <REG2> ...
        if len(packet) >= 20 and packet[0] == 0x55 and packet[1] == 0x71:
            addr = int.from_bytes(packet[2:4], "little")
            if addr != first_register:
                continue
            return [int.from_bytes(packet[4 + i * 2:6 + i * 2], "little")
                    for i in range(4)]

        # Standard format from the documentation
        if len(packet) >= 10 and packet[0] == 0x55 and packet[1] == 0x5F:
            return [int.from_bytes(packet[2 + i * 2:4 + i * 2], "little")
                    for i in range(4)]

    if replies:
        print(f"  no answer to the read of register 0x{first_register:02x}")
    return None


BLE_PACKET_SIZES = {0x61: 28, 0x71: 20}
BLE_DEFAULT_SIZE = 11


def ble_split_packets(blob):
    """One notification can carry several packets back to back."""
    out = []
    i = 0
    while i < len(blob) - 1:
        if blob[i] != 0x55:
            i += 1
            continue
        size = BLE_PACKET_SIZES.get(blob[i + 1], BLE_DEFAULT_SIZE)
        if i + size > len(blob):
            break
        out.append(bytes(blob[i:i + size]))
        i += size
    return out


def ble_all_packets():
    """All packets from the collected notifications, already split."""
    result = []
    for blob in replies:
        result.extend(ble_split_packets(blob))
    return result


def ble_decode_61(packet):
    """Packet 0x61: ACC(3) + GYRO(3) + ANGLE(3) as int16, then 8 time bytes."""
    if len(packet) < 28 or packet[0] != 0x55 or packet[1] != 0x61:
        return None

    def word(index):
        return int.from_bytes(packet[2 + index * 2:4 + index * 2],
                              "little", signed=True)

    acc = [word(i) / 32768 * 16 for i in range(3)]
    gyro = [word(i) / 32768 * 2000 for i in range(3, 6)]
    angle = [word(i) / 32768 * 180 for i in range(6, 9)]

    yy, mm, dd, hh, mn, ss = packet[20:26]
    ms = int.from_bytes(packet[26:28], "little")

    return {
        "acc": acc, "gyro": gyro, "angle": angle,
        "time": f"20{yy:02d}-{mm:02d}-{dd:02d} {hh:02d}:{mn:02d}:{ss:02d}.{ms:03d}",
        "stamp": ((hh * 60 + mn) * 60 + ss) * 1000 + ms,
    }


async def listen(client, notify_char, seconds=10.0):
    """Listen to the stream and measure the real rate from timestamps."""
    print(f"\nlistening for {seconds:.0f} seconds, move the sensor ...")
    replies.clear()
    await asyncio.sleep(seconds)

    if not replies:
        print("\nSilence. The sensor is not transmitting over BLE.")
        return

    packets = ble_all_packets()
    print(f"\nnotifications {len(replies)}, samples {len(packets)}")

    decoded = [d for d in (ble_decode_61(p) for p in packets) if d]
    if not decoded:
        print("packets are not 0x61, first three in full:")
        for packet in replies[:3]:
            print(f"  {packet.hex(' ')}")
        return

    sample = decoded[0]
    print(f"\nfirst sample  {sample['time']}")
    print("  accel  " + "  ".join(f"{v:+7.3f} g" for v in sample["acc"]))
    print("  gyro   " + "  ".join(f"{v:+7.2f} deg/s" for v in sample["gyro"]))
    print("  angle  " + "  ".join(f"{v:+7.2f} deg" for v in sample["angle"]))

    # Rate from the gap between consecutive timestamps
    deltas = [b["stamp"] - a["stamp"]
              for a, b in zip(decoded, decoded[1:])
              if 0 < b["stamp"] - a["stamp"] < 5000]
    if deltas:
        step = sorted(deltas)[len(deltas) // 2]
        print(f"\nsample interval {step} ms  ->  about "
              f"{1000 / step:.0f} Hz")
        print(f"that is roughly {1000 / step * 28 / 1024:.1f} KB per second")


async def show_settings(client, write_char):
    print("\nshow current settings:")

    values = await read_registers(client, write_char, 0x02)
    if values is None:
        print("  the device did not answer the read request")
        return False

    rsw, rrate = values[0], values[1]
    print(f"  RSW   0x{rsw:04x}  output: {decode_rsw(rsw)}")
    print(f"  RRATE 0x{rrate:04x}  rate: "
          f"{RATE_NAMES.get(rrate & 0x0F, 'unknown')}")

    values = await read_registers(client, write_char, 0x1F)
    if values:
        bw = values[0]
        print(f"  BW    0x{bw:04x}  bandwidth: "
              f"{BW_NAMES.get(bw & 0x0F, 'unknown')}")
    print()
    return True


async def apply_settings(client, write_char):
    print("\napplying settings")
    print("  unlock")
    await client.write_gatt_char(write_char, UNLOCK, response=False)
    await asyncio.sleep(0.3)

    for register, value, comment in SETTINGS:
        print(f"  0x{register:02x} = 0x{value:02x}   {comment}")
        await client.write_gatt_char(write_char, write_cmd(register, value),
                                     response=False)
        await asyncio.sleep(0.3)

    print("  save")
    await client.write_gatt_char(write_char, SAVE, response=False)
    await asyncio.sleep(1.5)
    print("done")




# Clock registers: 0x30 YYMM, 0x31 DDHH, 0x32 MMSS, 0x33 MS.
# In each one the low byte is the first field, the high byte the second.
# Verified against the examples in the WitMotion documentation.
REG_YYMM, REG_DDHH, REG_MMSS, REG_MS = 0x30, 0x31, 0x32, 0x33


def time_commands(moment):
    """Build the clock-setting commands for a given moment."""
    return [
        (REG_YYMM, (moment.month << 8) | (moment.year % 100), "year and month"),
        (REG_DDHH, (moment.hour << 8) | moment.day, "day and hour"),
        (REG_MMSS, (moment.second << 8) | moment.minute, "minute and second"),
        (REG_MS, moment.microsecond // 1000, "milliseconds"),
    ]


async def set_clock(client, write_char):
    """Set the sensor clock from the computer clock."""
    print("\nreading the current sensor time")
    values = await read_registers(client, write_char, REG_YYMM)
    if values:
        yymm, ddhh, mmss, ms = values
        try:
            was = datetime(2000 + (yymm & 0xFF), yymm >> 8, ddhh & 0xFF,
                           ddhh >> 8, mmss & 0xFF, mmss >> 8, ms * 1000)
            drift = (datetime.now() - was).total_seconds()
            print(f"  before: {was.isoformat(timespec='seconds')}")
            print(f"  drift: {drift / 86400:+.1f} days")
        except ValueError:
            print(f"  the sensor clock holds garbage: {[hex(v) for v in values]}")

    now = datetime.now()
    print(f"  setting: {now.isoformat(timespec='milliseconds')}")

    await client.write_gatt_char(write_char, UNLOCK, response=False)
    await asyncio.sleep(0.2)
    for register, value, label in time_commands(now):
        await client.write_gatt_char(write_char, write_cmd(register, value),
                                     response=False)
        await asyncio.sleep(0.15)
    await client.write_gatt_char(write_char, SAVE, response=False)
    await asyncio.sleep(1.2)

    print("\nverifying")
    values = await read_registers(client, write_char, REG_YYMM)
    if not values:
        print("  no answer from the sensor, check with --listen")
        return
    yymm, ddhh, mmss, ms = values
    try:
        now_dev = datetime(2000 + (yymm & 0xFF), yymm >> 8, ddhh & 0xFF,
                           ddhh >> 8, mmss & 0xFF, mmss >> 8, ms * 1000)
    except ValueError:
        print(f"  cannot decode the answer: {[hex(v) for v in values]}")
        return
    drift = abs((datetime.now() - now_dev).total_seconds())
    print(f"  after: {now_dev.isoformat(timespec='seconds')}")
    if drift < 5:
        print("  clock synchronised")
    else:
        print(f"  still off by {drift:.0f} s - something is wrong")



async def ble_main(mode):
    matches, everything = await find_device()

    if not matches:
        print(f"\nNo device with '{DEVICE_NAME_HINT}' in its name.")
        print("What is visible over BLE:\n")
        for device in everything[:15]:
            print(f"  {device.address}   {device.name or '(no name)'}")
        print("\nAdjust DEVICE_NAME_HINT in this script to match your sensor.")
        return 1

    if len(matches) > 1:
        print("\nseveral matches, using the first:")
        for device in matches:
            print(f"  {device.address}   {device.name}")

    target = matches[0]
    print(f"\nconnecting to {target.name} [{target.address}]")

    if mode == "scan":
        return 0

    BleakClient, _ = need_bleak()
    async with BleakClient(target, timeout=20.0) as client:
        print("connected")

        write_char, notify_char = await pick_chars(client)
        if write_char is None:
            print("no writable characteristic found")
            return 1
        print(f"write:   {write_char.uuid}")
        if notify_char:
            print(f"notify:  {notify_char.uuid}")
            await client.start_notify(notify_char, on_notify)
            await asyncio.sleep(0.5)
        else:
            print("no notify characteristic, verification unavailable")

        if mode == "settime":
            await set_clock(client, write_char)
        elif mode == "verify":
            await show_settings(client, write_char)
        elif mode == "listen":
            if notify_char:
                await listen(client, notify_char)
            else:
                print("no notify characteristic")
        elif mode == "apply":
            await show_settings(client, write_char)
            await apply_settings(client, write_char)
            print("reading back to confirm:")
            await show_settings(client, write_char)

        if notify_char:
            await client.stop_notify(notify_char)

    return 0



# --------------------------------------------------------------------------
# Status overview
# --------------------------------------------------------------------------

def device_summary():
    """One record per known sensor, built from the sidecar metadata.

    Reading .meta.json instead of the .jsonl keeps this instant even when
    a device holds gigabytes of recordings.
    """
    docked, kinds = {}, {}
    for volume in find_volumes():
        kind = device_kind(volume)
        if kind:
            docked[safe_name(volume.name)] = volume
            kinds[safe_name(volume.name)] = kind

    devices = []
    if OUT_DIR.is_dir():
        known = [p for p in OUT_DIR.iterdir()
                 if p.is_dir() and (p / "sessions").is_dir()]
    else:
        known = []

    for name in sorted({p.name for p in known} | set(docked)):
        sessions_dir = OUT_DIR / name / "sessions"
        sessions, samples, size, last, first = 0, 0, 0, None, None
        kind = kinds.get(name)

        if sessions_dir.is_dir():
            for meta_file in sessions_dir.rglob("*.meta.json"):
                try:
                    meta = json.loads(meta_file.read_text())
                except (OSError, ValueError):
                    continue
                sessions += 1
                if meta.get("kind") == "video":
                    kind = kind or "camera"
                else:
                    kind = kind or "bracelet"
                samples += meta.get("samples", 0)
                size += meta.get("bytes", 0)
                for key, keep in (("end", "last"), ("start", "first")):
                    value = meta.get(key)
                    if not value:
                        continue
                    if keep == "last" and (last is None or value > last):
                        last = value
                    if keep == "first" and (first is None or value < first):
                        first = value

            # Fall back to file times for sessions collected before metadata
            if not sessions:
                found = list(sessions_dir.rglob("*.jsonl"))
                videos = [p for p in sessions_dir.rglob("*")
                          if p.suffix.upper() in CAMERA_VIDEO_EXTS]
                if videos and not found:
                    found, kind = videos, kind or "camera"
                elif found:
                    kind = kind or "bracelet"
                sessions = len(found)
                size = sum(f.stat().st_size for f in found)

        devices.append({
            "device": name,
            "kind": kind or "?",
            "docked": name in docked,
            "volume": str(docked[name]) if name in docked else None,
            "sessions": sessions,
            "samples": samples,
            "bytes": size,
            "first": first,
            "last": last,
        })
    return devices


def collector_state():
    """Is a collection running right now, and how long has it been going?"""
    try:
        locks = sorted(OUT_DIR.glob("collector*.lock"))
    except OSError:
        return None
    for path in locks:
        try:
            age = time.time() - path.stat().st_mtime
            who = path.stem.replace("collector-", "")
            return {"holder": f"{who} ({path.read_text().strip()})",
                    "age_s": round(age)}
        except OSError:
            continue
    return None


def free_space():
    try:
        usage = shutil.disk_usage(OUT_DIR if OUT_DIR.is_dir() else Path.home())
        return {"free": usage.free, "total": usage.total}
    except OSError:
        return None


def ago(iso):
    if not iso:
        return "never"
    try:
        delta = (datetime.now() - datetime.fromisoformat(iso)).total_seconds()
    except ValueError:
        return "?"
    if delta < 0:
        return "in the future"
    for limit, div, unit in ((90, 1, "s"), (5400, 60, "min"),
                             (172800, 3600, "h")):
        if delta < limit:
            return f"{delta / div:.0f} {unit} ago"
    return f"{delta / 86400:.1f} days ago"


def show_status(as_json=False):
    devices = device_summary()
    running = collector_state()
    space = free_space()

    if as_json:
        print(json.dumps({
            "generated": datetime.now().isoformat(timespec="seconds"),
            "devices": devices,
            "collecting": running,
            "disk": space,
        }, indent=1))
        return

    print()
    print(f"  {'DEVICE':<12} {'KIND':<9} {'DOCKED':<7} {'FILES':>6} "
          f"{'SAMPLES':>12} {'SIZE':>10}  LAST RECORDING")
    print("  " + "-" * 78)

    if not devices:
        print("  no devices seen yet\n")
    for d in devices:
        mark = "yes" if d["docked"] else "-"
        samples = f"{d['samples']:,}" if d["kind"] != "camera" else "-"
        print(f"  {d['device']:<12} {d['kind']:<9} {mark:<7} {d['sessions']:>6} "
              f"{samples:>12} {human(d['bytes']):>10}  {ago(d['last'])}")

    print()
    if running:
        print(f"  collecting now: {running['holder']} "
              f"({running['age_s']} s)")
    else:
        print("  idle")

    if space:
        used = 100 * (1 - space["free"] / space["total"])
        print(f"  disk: {human(space['free'])} free, {used:.0f}% used")

    total_bytes = sum(d["bytes"] for d in devices)
    if space and total_bytes:
        days = space["free"] / (total_bytes / max(1, len(devices)) or 1)
        print(f"  data collected so far: {human(total_bytes)}")
    print()


# --------------------------------------------------------------------------
# Live dashboard
# --------------------------------------------------------------------------

C = {
    "reset": "\033[0m", "bold": "\033[1m", "dim": "\033[2m",
    "red": "\033[31m", "green": "\033[32m", "yellow": "\033[33m",
    "blue": "\033[34m", "cyan": "\033[36m",
    "on_red": "\033[41;97;1m", "on_green": "\033[42;30;1m",
    "on_yellow": "\033[43;30;1m",
}


def short_time(seconds):
    if seconds is None:
        return ""
    if seconds < 90:
        return f"{seconds:.0f} s"
    if seconds < 5400:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"


def paint(text, *styles):
    return "".join(C[s] for s in styles) + text + C["reset"]


def git_version():
    try:
        result = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parent),
             "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=3)
        if result.returncode == 0:
            return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return "-"


def tail_log(lines=8):
    path = OUT_DIR / "collector.log"
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - 16384))
            text = handle.read().decode("utf-8", errors="replace")
    except OSError:
        return []
    return text.splitlines()[-lines:]


BAD_WORDS = ("fail", "error", "unreadable", "skipped", "stalled")


def device_status(name, st, docked, summary_last):
    """Return (label, colour, detail) for one device - worded for a worker."""
    phase = st.get("phase")
    age = seconds_since(st.get("updated"))
    finished = seconds_since(st.get("finished"))

    if phase in ("checking", "copying", "decoding"):
        if age is not None and age > 120:
            return "STALLED", "on_red", "no progress for 2 min - see log"
        pct = st.get("percent") or 0
        what = {"checking": "CHECKING", "copying": f"COPYING {pct}%",
                "decoding": "PROCESSING"}[phase]
        detail = []
        if st.get("copied") and st.get("total"):
            detail.append(f"{human(st['copied'])} of {human(st['total'])}")
        if st.get("speed"):
            detail.append(f"{human(st['speed'])}/s")
        if st.get("eta"):
            detail.append(f"{short_time(st['eta'])} left")
        if st.get("file"):
            detail.append(st["file"])
        return what + " - DO NOT UNPLUG", "on_yellow", "  ".join(detail)

    # A finished device is unmounted, so "can be taken" must survive the
    # volume disappearing - that banner is the whole point of the screen.
    if phase == "done" and finished is not None \
            and finished < READY_SHOW_MINUTES * 60:
        detail = f"{st.get('files', 0)} files, {human(st.get('bytes', 0))}"
        took = st.get("took")
        if took and took > 1 and st.get("bytes"):
            detail += (f" in {short_time(took)} "
                       f"({human(st['bytes'] / took)}/s)")
        return "DONE - CAN BE TAKEN", "on_green", detail

    if docked:
        if phase == "interrupted":
            return "RESUMING", "on_yellow", "reconnected, picking up where it stopped"
        if phase == "error":
            return "ERROR", "on_red", st.get("error") or "see log"
        if phase == "nothing_new":
            return "NOTHING NEW - CAN BE TAKEN", "on_green", ""
        return "CONNECTED", "cyan", "waiting"

    # Not in the dock. A past failure is worth reporting to an engineer,
    # but never as a banner: the device it refers to is not here, and a
    # worker would read it as a warning about the one in their hand.
    if phase == "interrupted":
        return "not connected", "yellow", \
            "copy interrupted - reconnect the device to resume"

    if phase == "error" and finished is not None and finished < 24 * 3600:
        return "not connected", "red", \
            f"last attempt failed {ago(st.get('finished'))}: " \
            f"{st.get('error') or 'see log'}"

    last = st.get("last_done") or summary_last
    idle = seconds_since(last)
    if idle is not None and idle > STALE_HOURS * 3600:
        return f"NO DATA FOR {idle / 3600:.0f} h", "red", "check the device"
    return "not connected", "dim", f"last sync {ago(last)}"


def render_dashboard(version, host, summary, pending, width):
    lines = []
    rule = paint("-" * min(width, 100), "dim")

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"{stamp}  {paint(host, 'cyan', 'bold')}  "
                 f"wt901 {paint(version, 'dim')}")
    lines.append(rule)

    # Storage
    space = free_space()
    if space:
        used = 1 - space["free"] / space["total"]
        filled = int(used * 30)
        colour = "red" if used > 0.9 else "yellow" if used > 0.75 else "green"
        gauge = paint("#" * filled, colour) + paint("." * (30 - filled), "dim")
        lines.append(f"{paint('STORAGE', 'blue', 'bold'):<20} {used * 100:3.0f}% "
                     f"[{gauge}]  {human(space['free'])} free of "
                     f"{human(space['total'])}")
    lines.append("")

    # Devices
    docked = {}
    for volume in find_volumes():
        kind = device_kind(volume)
        if kind:
            docked[safe_name(volume.name)] = kind

    by_name = {d["device"]: d for d in summary}
    states = {}
    if state_dir().is_dir():
        for path in state_dir().glob("*.json"):
            if path.stem != "uploader":
                states[path.stem] = read_state(path.stem)

    pending_by_dev = {}
    for path in pending:
        dev = path.parent.parent.name
        pending_by_dev[dev] = pending_by_dev.get(dev, 0) + 1

    names = sorted(set(by_name) | set(states) | set(docked))
    banners = []
    lines.append(paint("DEVICES", "blue", "bold"))
    if not names:
        lines.append(paint("  no devices seen yet", "dim"))

    for name in names:
        d = by_name.get(name, {})
        st = states.get(name, {})
        kind = docked.get(name) or st.get("kind") or d.get("kind") or "?"
        label, colour, detail = device_status(
            name, st, name in docked, d.get("last"))

        if colour in ("on_yellow", "on_green", "on_red") \
                and (name in docked or colour == "on_green"):
            banners.append((name, label, colour))

        upload = ""
        if UPLOAD_REMOTE:
            waiting = pending_by_dev.get(name, 0)
            upload = (paint(f"cloud: {waiting} waiting", "yellow") if waiting
                      else paint("cloud: up to date", "green"))

        shown = paint(f" {label} ", colour) if colour.startswith("on_") \
            else paint(label, colour)
        lines.append(f"  {paint(name, 'bold'):<20} {kind:<9} {shown}  "
                     f"{paint(detail, 'dim')}  {upload}")
    lines.append("")

    # Uploader
    up = read_state("uploader")
    head = paint("CLOUD UPLOAD", "blue", "bold")
    if not UPLOAD_REMOTE:
        lines.append(f"{head}  {paint('off (no --remote set)', 'dim')}")
    else:
        phase = up.get("phase")
        waiting = len(pending)
        wbytes = human(sum(dir_bytes(p) for p in pending))
        if phase == "uploading" and (seconds_since(up.get("updated")) or 0) < 300:
            extra = ""
            if up.get("speed"):
                extra += f"  {up['speed']}"
            if up.get("eta"):
                extra += f"  ETA {up['eta']}"
            state = paint(f"uploading {up.get('session')}  "
                          f"{up.get('percent', 0)}%{extra}", "yellow", "bold")
        elif phase == "error":
            state = paint(f"ERROR: {up.get('error')}", "red", "bold") + \
                paint(f"  (retrying every {UPLOAD_INTERVAL:.0f} s)", "dim")
        elif waiting:
            state = paint(f"{waiting} sessions waiting", "yellow")
        else:
            state = paint("idle, nothing to upload", "green")
        lines.append(f"{head}  {state}")
        lines.append(f"  target {paint(UPLOAD_REMOTE, 'cyan')}   "
                     f"waiting {waiting} ({wbytes})   "
                     f"last success {ago(up.get('last_ok'))}")
    lines.append("")

    # Recent events
    lines.append(paint("RECENT", "blue", "bold"))
    for entry in tail_log(8):
        entry = entry[:width - 2]
        bad = any(word in entry.lower() for word in BAD_WORDS)
        lines.append("  " + (paint(entry, "red") if bad else paint(entry, "dim")))

    # Big banner for whoever is standing at the dock
    top = []
    order = {"on_red": 0, "on_yellow": 1, "on_green": 2}
    for name, label, colour in sorted(banners, key=lambda b: order[b[2]]):
        top.append(paint(f"   {name}:  {label}   ".ljust(min(width, 100)), colour))
    if top:
        top.append("")
    return lines[:2] + top + lines[2:]


def forget_device(name):
    """Remove a device the dashboard should stop showing.

    Cards renamed after their first use leave a row behind under the old
    label. This clears the stale state, and the collected data with it
    only if that data is empty.
    """
    removed = []
    state_file = state_dir() / f"{safe_name(name)}.json"
    if state_file.exists():
        state_file.unlink()
        removed.append(str(state_file))

    folder = OUT_DIR / safe_name(name)
    if folder.is_dir():
        if dir_bytes(folder) == 0:
            shutil.rmtree(folder, ignore_errors=True)
            removed.append(str(folder))
        else:
            print(f"kept {folder} - it still holds "
                  f"{human(dir_bytes(folder))} of data")

    if removed:
        print("removed:\n  " + "\n  ".join(removed))
    else:
        print(f"nothing to remove for {name!r}")
    return 0


def dashboard(refresh=1.0):
    # The service knows where it uploads; reuse that so the dashboard
    # works over SSH without repeating --remote every time.
    if not UPLOAD_REMOTE and read_state("uploader").get("remote"):
        globals()["UPLOAD_REMOTE"] = read_state("uploader")["remote"]
    version, host = git_version(), socket.gethostname()
    cache = {"at": 0.0, "summary": [], "pending": []}
    sys.stdout.write("\033[2J\033[?25l")
    try:
        while True:
            if time.time() - cache["at"] > 10:
                cache["summary"] = device_summary()
                cache["pending"] = pending_sessions() if UPLOAD_REMOTE else []
                cache["pending_bytes"] = sum(dir_bytes(p) for p in cache["pending"])
                cache["at"] = time.time()
            width = shutil.get_terminal_size((100, 30)).columns
            lines = render_dashboard(version, host, cache["summary"],
                                     cache["pending"], width)
            sys.stdout.write("\033[H" + "\n".join(l + "\033[K" for l in lines)
                             + "\n\033[J")
            sys.stdout.flush()
            time.sleep(refresh)
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write("\033[?25h\n")
        sys.stdout.flush()


# --------------------------------------------------------------------------
# Auto-run on device connect
# --------------------------------------------------------------------------

def agent_file():
    return Path.home() / "Library" / "LaunchAgents" / f"{AGENT_LABEL}.plist"


def install(delete_after):
    script = Path(__file__).resolve()
    python = Path(sys.executable).resolve()

    if platform.system() == "Darwin":
        args = [str(python), str(script), "--once"]
        if delete_after:
            args.append("--delete")
        if UPLOAD_REMOTE:
            args += ["--remote", UPLOAD_REMOTE]
        entries = "".join(f"\n        <string>{a}</string>" for a in args)

        plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{AGENT_LABEL}</string>
    <key>ProgramArguments</key>
    <array>{entries}
    </array>
    <key>StartOnMount</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/{AGENT_LABEL}.out.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/{AGENT_LABEL}.err.log</string>
</dict>
</plist>
"""
        target = agent_file()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(plist)

        subprocess.run(["launchctl", "unload", str(target)],
                       capture_output=True)
        result = subprocess.run(["launchctl", "load", str(target)],
                                capture_output=True, text=True)
        if result.returncode != 0:
            print(f"launchctl returned an error: {result.stderr.strip()}")
            return 1

        print(f"\nagent installed: {target}")
        print("the script now starts by itself when the sensor is connected")
        print(f"launch logs: /tmp/{AGENT_LABEL}.out.log")
        print(f"remove with: python3 {script} --uninstall\n")
        return 0

    if platform.system() == "Linux":
        # A systemd *user* service that polls, rather than a udev rule.
        # udev runs its hooks as root (wrong home dir, wrong /media path)
        # and fires before the desktop session has mounted the volume, so
        # a udev-triggered run usually finds nothing. Polling as the real
        # user sees exactly the mounts the user sees.
        args = f"{python} {script}"
        if delete_after:
            args += " --delete"
        if UPLOAD_REMOTE:
            args += f" --remote {UPLOAD_REMOTE}"
        unit_dir = Path.home() / ".config" / "systemd" / "user"
        unit_dir.mkdir(parents=True, exist_ok=True)
        unit = unit_dir / "wt901.service"
        unit.write_text(f"""[Unit]
Description=WT901 bracelet and camera collector

[Service]
ExecStart={args}
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
""")
        steps = [
            ["systemctl", "--user", "daemon-reload"],
            ["systemctl", "--user", "enable", "--now", "wt901.service"],
        ]
        for cmd in steps:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"{' '.join(cmd)} failed: {result.stderr.strip()}")
                return 1

        # Dashboard on the dock's own screen, for whoever takes the devices
        autostart = Path.home() / ".config" / "autostart"
        autostart.mkdir(parents=True, exist_ok=True)
        remote_arg = f" --remote {UPLOAD_REMOTE}" if UPLOAD_REMOTE else ""
        (autostart / "wt901-dashboard.desktop").write_text(f"""[Desktop Entry]
Type=Application
Name=WT901 dashboard
Exec=gnome-terminal --full-screen -- {python} {script} --dashboard{remote_arg}
X-GNOME-Autostart-enabled=true
""")

        print(f"\nservice installed: {unit}")
        print("dashboard will open full-screen after login")
        print("it polls for devices every "
              f"{POLL_INTERVAL:.0f} s and restarts itself if it crashes")
        print("\ncheck it:   systemctl --user status wt901")
        print("live log:   journalctl --user -u wt901 -f")
        print(f"remove:     python3 {script} --uninstall")
        print("\nFor an unattended dock two more things are needed:")
        print("  1. keep the service running with nobody logged in:")
        print(f"       sudo loginctl enable-linger {Path.home().name}")
        print("  2. turn on automatic login (Settings > Users), because")
        print("     USB volumes are only auto-mounted inside a desktop session")
        print("  3. keep the screen awake for the dashboard:")
        bus = "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$(id -u)/bus"
        print(f"       {bus} gsettings set org.gnome.desktop.session idle-delay 0")
        print(f"       {bus} gsettings set org.gnome.desktop.screensaver lock-enabled false\n")
        return 0

    print(f"auto-run is not supported on {platform.system()}")
    return 1


def uninstall():
    if platform.system() == "Darwin":
        target = agent_file()
        if not target.exists():
            print("no agent is installed")
            return 0
        subprocess.run(["launchctl", "unload", str(target)],
                       capture_output=True)
        target.unlink()
        print("agent removed")
        return 0

    if platform.system() == "Linux":
        unit = Path.home() / ".config" / "systemd" / "user" / "wt901.service"
        subprocess.run(["systemctl", "--user", "disable", "--now", "wt901.service"],
                       capture_output=True)
        if unit.exists():
            unit.unlink()
        kiosk = Path.home() / ".config" / "autostart" / "wt901-dashboard.desktop"
        if kiosk.exists():
            kiosk.unlink()
        subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)
        print("service removed")
        return 0

    return 1


def main():
    parser = argparse.ArgumentParser(
        description="Collect data from a WT901SDCL-BT50 over USB.",
        epilog="Start with --list while the sensor is plugged in.",
    )
    parser.add_argument("--once", action="store_true",
                        help="single pass, then exit")
    parser.add_argument("--monitor", action="store_true",
                        help="live terminal monitor")
    parser.add_argument("--list", action="store_true",
                        help="list volumes and their files")
    parser.add_argument("--delete", action="store_true",
                        help="clear the card after a verified copy")
    parser.add_argument("--install", action="store_true",
                        help="install the auto-run agent")
    parser.add_argument("--uninstall", action="store_true",
                        help="remove the auto-run agent")
    parser.add_argument("--volume", metavar="NAME",
                        help="only use volumes whose name contains this")
    parser.add_argument("--forget", metavar="DEVICE",
                        help="stop showing a device that will not come back")
    parser.add_argument("--dashboard", action="store_true",
                        help="full-screen live dashboard")
    parser.add_argument("--upload", action="store_true",
                        help="upload finished sessions once, then exit")
    parser.add_argument("--remote", metavar="REMOTE",
                        help="rclone remote for uploads, e.g. cloud:farm-data")
    parser.add_argument("--status", action="store_true",
                        help="overview of all sensors and collected data")
    parser.add_argument("--json", action="store_true",
                        help="machine-readable output for --status")
    parser.add_argument("--keep-raw", action="store_true",
                        help="keep the raw .TXT files after decoding")

    ble = parser.add_argument_group("sensor configuration over Bluetooth")
    ble.add_argument("--scan", action="store_true",
                     help="find the sensor over BLE")
    ble.add_argument("--verify", action="store_true",
                     help="show the current sensor settings")
    ble.add_argument("--listen", action="store_true",
                     help="listen to the BLE data stream")
    ble.add_argument("--apply", action="store_true",
                     help="apply output rate and filter bandwidth")
    ble.add_argument("--rate", type=int, choices=[20, 50, 100, 200],
                     metavar="HZ", help="output rate for --apply (default 100)")
    ble.add_argument("--settime", action="store_true",
                     help="sync the sensor clock with this computer")

    args = parser.parse_args()

    if args.volume:
        globals()["VOLUME_FILTER"] = args.volume

    if args.keep_raw:
        globals()["KEEP_RAW"] = True

    if args.remote:
        globals()["UPLOAD_REMOTE"] = args.remote

    if args.rate:
        codes = {20: (0x07, 0x05), 50: (0x08, 0x04),
                 100: (0x09, 0x03), 200: (0x0B, 0x02)}
        rate_code, bw_code = codes[args.rate]
        globals()["SETTINGS"] = [
            (0x03, rate_code, f"RRATE: {args.rate} Hz"),
            (0x1F, bw_code, "BANDWIDTH: matched to the rate"),
        ]

    try:
        if args.scan or args.verify or args.listen or args.apply or args.settime:
            mode = ("scan" if args.scan else "settime" if args.settime
                    else "verify" if args.verify
                    else "listen" if args.listen else "apply")
            sys.exit(asyncio.run(ble_main(mode)))
        elif args.forget:
            sys.exit(forget_device(args.forget))
        elif args.dashboard:
            dashboard()
        elif args.upload:
            if not UPLOAD_REMOTE:
                print("set a target with --remote, e.g. --remote cloud:farm-data")
                sys.exit(1)
            globals()["QUIET"] = False
            upload_pass()
            up = read_state("uploader")
            print(f"uploader: {up.get('phase')}  "
                  f"waiting {up.get('pending', 0)}  error {up.get('error')}")
        elif args.status:
            show_status(args.json)
        elif args.list:
            list_volumes()
        elif args.install:
            sys.exit(install(args.delete))
        elif args.uninstall:
            sys.exit(uninstall())
        elif args.monitor:
            monitor(args.delete)
        elif args.once:
            sys.exit(run_once(args.delete))
        else:
            watch(args.delete)
    except KeyboardInterrupt:
        print()
        log("stopped")
        sys.exit(0)


if __name__ == "__main__":
    main()
