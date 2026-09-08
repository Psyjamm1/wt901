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
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ==========================================================================
# SETTINGS
# ==========================================================================

OUT_DIR = Path.home() / "Projects" / "wt901-data"

POLL_INTERVAL = 10.0      # watch-mode poll period, seconds
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

def find_volumes():
    found = []
    for root in mount_roots():
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
            if VOLUME_FILTER and VOLUME_FILTER.lower() not in entry.name.lower():
                continue
            if entry not in found:
                found.append(entry)
    return found


def is_sensor(volume):
    """A volume counts as a sensor if it holds recording files."""
    if not REQUIRED_GLOB:
        return True
    try:
        return any(volume.glob(REQUIRED_GLOB)) or any(
            volume.glob(REQUIRED_GLOB.lower())
        )
    except OSError:
        return False


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

def lock_path():
    return OUT_DIR / "collector.lock"


class Busy(Exception):
    """Another instance is already working on this volume."""


def acquire_lock(stale_after=900):
    """Take an exclusive lock, or raise Busy.

    The monitor and the launchd agent both react to a mount, so without
    this they race for the same files: whoever loses copies nothing and
    the card may end up not cleared. A stale lock left by a crashed run
    is ignored after stale_after seconds.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = lock_path()

    if path.exists():
        try:
            age = time.time() - path.stat().st_mtime
            if age < stale_after:
                raise Busy(path.read_text().strip() or "another instance")
            path.unlink()
        except OSError:
            pass

    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise Busy("another instance")
    with os.fdopen(fd, "w") as handle:
        handle.write(f"pid {os.getpid()}\n")
    return path


def release_lock():
    try:
        lock_path().unlink()
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
    source_hash = hashlib.sha256()
    copied_bytes = 0
    try:
        total = source.stat().st_size
    except OSError:
        total = 0

    with open(source, "rb") as src, open(target, "wb") as dst:
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
    with open(target, "rb") as dst:
        while True:
            block = dst.read(chunk)
            if not block:
                break
            target_hash.update(block)

    if source_hash.hexdigest() != target_hash.hexdigest():
        raise OSError("copy does not match the bytes read")

    return copied_bytes


def collect(volume, delete_after, progress=None, done_bytes=None):
    """Collect new files from a volume. True if anything was copied."""
    if not is_sensor(volume):
        return False

    try:
        acquire_lock()
    except Busy as who:
        log(f"volume {volume.name}: skipped, {who} is already collecting")
        return False

    try:
        return _collect(volume, delete_after, progress, done_bytes)
    finally:
        release_lock()


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
    device_dir = OUT_DIR / safe_name(volume.name)
    session = device_dir / "sessions" / datetime.now().strftime("%Y%m%d_%H%M%S")
    session.mkdir(parents=True, exist_ok=True)

    copied = []
    failed = []
    for source in fresh:
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
                log(f"  {source.name}: attempt {attempt} failed - {exc}")
                try:
                    if target.exists():
                        target.unlink()      # never leave a partial copy behind
                except OSError:
                    pass
                if attempt < COPY_ATTEMPTS:
                    time.sleep(RETRY_DELAY)

        if not ok:
            log(f"  {source.name} skipped, original left on the card")
            failed.append(source)
            continue

        seen.add(file_key(source, volume.name))
        copied.append((source, target))
        log(f"  {source.name}  {size_bytes / (1024 * 1024):.1f} MB  ok")

    save_state(seen)

    if not copied:
        log("nothing could be copied")
        notify("Download failed",
               f"{len(failed)} files unreadable. Card data is intact.")
        return False

    log(f"saved {len(copied)} files to {session}")
    if failed:
        names = ", ".join(f.name for f in failed)
        log(f"unreadable, left on the card: {names}")

    # Decode to JSON Lines, merging across file boundaries
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

    notify(
        "Sensor data collected",
        f"{len(copied)} files, {total_mb:.0f} MB, "
        f"{samples_total} samples. The sensor can be taken.",
    )
    unmount(volume)
    return True


def run_once(delete_after):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
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
    screen = Screen()

    print()
    print("  WT901SDCL-BT50 monitor")
    print(f"  data:    {OUT_DIR}")
    print(f"  cleanup: {'on' if delete_after else 'off'}")
    print("  quit:    Ctrl+C")
    print()

    known = set()
    spinner = "|/-\\"
    tick = 0

    while True:
        volumes = find_volumes()
        sensors = [v for v in volumes if is_sensor(v)]
        current = {v.name for v in sensors}
        known &= current

        fresh = [v for v in sensors if v.name not in known]

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
            known.add(volume.name)
            print()


def watch(delete_after):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    log(f"watching for a connection, polling every {POLL_INTERVAL:.0f} s")
    log(f"writing data to {OUT_DIR}")
    if delete_after:
        log("card cleanup enabled")
    if VOLUME_FILTER:
        log(f"volume filter: {VOLUME_FILTER!r}")

    known = set()
    while True:
        volumes = find_volumes()
        current = {v.name for v in volumes}
        known &= current                      # forget volumes that went away

        for volume in volumes:
            if volume.name in known:
                continue
            time.sleep(SETTLE_DELAY)
            if volume.is_dir():
                collect(volume, delete_after)
            known.add(volume.name)

        time.sleep(POLL_INTERVAL)


def list_volumes():
    volumes = find_volumes()
    if not volumes:
        print("\nNo removable volumes found. Is the sensor plugged in?\n")
        return
    seen = load_state()
    print()
    for volume in volumes:
        files = data_files(volume)
        fresh = [f for f in files if file_key(f, volume.name) not in seen]
        size = sum(f.stat().st_size for f in files) / (1024 * 1024)
        print(f"  {volume}")
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
    docked = {}
    for volume in find_volumes():
        if is_sensor(volume):
            docked[safe_name(volume.name)] = volume

    devices = []
    if OUT_DIR.is_dir():
        known = [p for p in OUT_DIR.iterdir()
                 if p.is_dir() and (p / "sessions").is_dir()]
    else:
        known = []

    for name in sorted({p.name for p in known} | set(docked)):
        sessions_dir = OUT_DIR / name / "sessions"
        sessions, samples, size, last, first = 0, 0, 0, None, None

        if sessions_dir.is_dir():
            for meta_file in sessions_dir.rglob("*.meta.json"):
                try:
                    meta = json.loads(meta_file.read_text())
                except (OSError, ValueError):
                    continue
                sessions += 1
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
                sessions = len(found)
                size = sum(f.stat().st_size for f in found)

        devices.append({
            "device": name,
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
    path = lock_path()
    if not path.exists():
        return None
    try:
        age = time.time() - path.stat().st_mtime
        return {"holder": path.read_text().strip(), "age_s": round(age)}
    except OSError:
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
    print(f"  {'DEVICE':<12} {'DOCKED':<8} {'SESSIONS':>9} {'SAMPLES':>12} "
          f"{'SIZE':>9}  LAST RECORDING")
    print("  " + "-" * 72)

    if not devices:
        print("  no devices seen yet\n")
    for d in devices:
        mark = "yes" if d["docked"] else "-"
        print(f"  {d['device']:<12} {mark:<8} {d['sessions']:>9} "
              f"{d['samples']:>12,} {human(d['bytes']):>9}  {ago(d['last'])}")

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
        args = f"{python} {script} --once"
        if delete_after:
            args += " --delete"
        rule = (
            'ACTION=="add", SUBSYSTEM=="block", ENV{ID_FS_USAGE}=="filesystem", '
            f'RUN+="/usr/bin/systemd-run --no-block {args}"\n'
        )
        print("\nThe udev rule must be installed as root.")
        print("Run these two commands:\n")
        print(f"  echo '{rule.strip()}' | "
              "sudo tee /etc/udev/rules.d/99-wt901.rules")
        print("  sudo udevadm control --reload-rules\n")
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
        print("\nRun:\n")
        print("  sudo rm /etc/udev/rules.d/99-wt901.rules")
        print("  sudo udevadm control --reload-rules\n")
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
