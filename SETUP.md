# Setting up a new dock machine

Written after setting up two of them. Everything in Troubleshooting below
actually happened — none of it is hypothetical.

Takes about half an hour on Debian or Ubuntu. Replace `USER` with the account
the collector will run as.

## Before you start

- The machine needs a wired or wireless network and a way to reach it by SSH.
- Have the cards out of the devices or the devices ready to plug in.
- Know which storage the cameras record to. It must be the **microSD card**,
  not the built-in memory: internal storage mounts under the same name on every
  camera, so it cannot tell one unit from another, and the collector skips it.

## 1. SSH

On the machine itself, with a keyboard, once:

```bash
sudo apt update
sudo apt install -y openssh-server
sudo systemctl enable --now ssh
hostname -I
```

Note the address. Everything below can be done over SSH.

```bash
ssh USER@ADDRESS
ssh-copy-id USER@ADDRESS
```

Ask whoever runs the network to reserve that address for this machine. A
DHCP lease will change under you and the dock will go missing.

## 2. Packages

```bash
sudo apt update
sudo apt install -y git python3-pip udisks2 ffmpeg rclone \
                    exfatprogs dosfstools vainfo mesa-va-drivers
```

Collection itself needs only Python. The rest is for re-encoding video,
labelling cards and uploading.

## 3. The code

Servers run the `release` branch. `main` is where changes are tested.

```bash
git clone --branch release https://github.com/YOUR_NAME/wt901.git ~/wt901
python3 ~/wt901/wt901.py --help
```

**Read that help output before going further.** It must list `--label`,
`--dashboard`, `--transcode`, `--benchmark` and `--remote`. If it does not, the
`release` branch is pointing at old code — fix it on the development machine
and clone again:

```bash
git checkout main && git pull
git checkout release && git reset --hard origin/main && git push --force-with-lease
```

## 4. Mounting without a desktop

A server has no graphical session, and udisks refuses to mount for anyone who
does not have one. Without this rule, cards are never mounted and the collector
sits there seeing nothing.

```bash
sudo tee /etc/polkit-1/rules.d/50-wt901.rules > /dev/null <<EOF
polkit.addRule(function(action, subject) {
    if (subject.user == "$USER" &&
            action.id.indexOf("org.freedesktop.udisks2.filesystem-") == 0) {
        return polkit.Result.YES;
    }
});
EOF
```

No restart needed. Test it with a card plugged in:

```bash
lsblk -o NAME,LABEL,FSTYPE,SIZE,TRAN,MOUNTPOINT | grep -v loop
udisksctl mount -b /dev/sdX --no-user-interaction
```

This lets that one account mount any removable medium without a password. On a
machine whose entire job is accepting memory cards that is the point; on a
shared machine, think twice.

## 5. Hardware video encoding

Worth the ten minutes: it is the difference between an hour of CPU grinding and
a couple of minutes.

```bash
ls -la /dev/dri/
groups
```

`/dev/dri/renderD128` belongs to the `render` group. If `groups` does not list
`render`, nothing will use the GPU:

```bash
sudo usermod -aG render $USER
```

**Log out and back in** — group membership is only applied at login. Then:

```bash
vainfo 2>&1 | grep -iE "driver|H264|HEVC"
```

Look for `VAEntrypointEncSlice`. That line is hardware encoding; `VAEntrypointVLD`
alone is decoding only.

If `/dev/dri` is empty, the kernel has not brought the GPU up:

```bash
sudo apt install -y firmware-amd-graphics   # or firmware-misc-nonfree on Intel
sudo reboot
```

The collector tries hardware first and falls back to the CPU the moment it is
refused, so a machine without a working GPU still works — only slower.

## 6. Name the cards

Every card ships with the same label, so they must be named before deployment.
Plug in **one at a time**:

```bash
python3 ~/wt901/wt901.py --label CAM01
```

Then `CAM02`, `COW01` and so on. Eleven characters, letters and digits.

Check what you just named by size — the sensor's card is around 16 GB, a
camera's is 64 GB or more. Labelling a camera `COW04` is easy and confusing
later:

```bash
lsblk -o NAME,LABEL,SIZE | grep -v loop
```

Put a sticker with the same name on the device itself. The person at the dock
needs a number they can read.

## 7. First run by hand

```bash
python3 ~/wt901/wt901.py --list
python3 ~/wt901/wt901.py --monitor
```

Plug a device in. For a camera, choose **File Transfer → USB** on its
touchscreen — until you do, the computer sees a charger, not a disk. Nothing is
deleted in this mode. Watch a full cycle, then Ctrl+C.

## 8. Install the service

```bash
python3 ~/wt901/wt901.py --install --delete --transcode
sudo loginctl enable-linger $USER
systemctl --user status wt901
```

`--delete` clears a card once the copy has been verified. `enable-linger` keeps
the service running with nobody logged in — without it, it stops when your SSH
session ends.

Watch it work:

```bash
journalctl --user -u wt901 -f
```

The re-encode line says `hardware` or `software`. If it says `software` on a
machine where `vainfo` showed `EncSlice`, something is wrong with the GPU path —
collection still works, it is just slower.

## 9. Cloud upload (optional)

```bash
rclone config create azure azureblob account NAME tenant T client_id C client_secret S
rclone lsd azure:
python3 ~/wt901/wt901.py --install --delete --transcode --remote azure:CONTAINER/path
systemctl --user restart wt901
```

Any rclone backend works the same way. A session is uploaded only after it has
finished copying and re-encoding, and is marked as sent only after `rclone
check` confirms the remote copy.

## 10. The screen at the dock

```bash
python3 ~/wt901/wt901.py --dashboard
```

Works over SSH. For a monitor at the dock itself, `--install` already created an
autostart entry that opens it full-screen — that one needs automatic login,
since it is a desktop session. Keep the screen awake:

```bash
gsettings set org.gnome.desktop.session idle-delay 0
gsettings set org.gnome.desktop.screensaver lock-enabled false
```

## Checklist

- [ ] `--help` lists the current commands
- [ ] a card mounts by itself within ten seconds of being plugged in
- [ ] `vainfo` shows `VAEntrypointEncSlice`
- [ ] every card has a name, and the device has a matching sticker
- [ ] a full cycle works by hand: copy, re-encode, clear, unmount
- [ ] `systemctl --user status wt901` says `active (running)`
- [ ] it survives a reboot
- [ ] the device clocks are right — they are what ties recordings together

## Troubleshooting

| What you see | Why | Fix |
|---|---|---|
| `--help` is missing half the commands | `release` points at old code | reset `release` to `main`, clone again |
| `exfatlabel is missing` though it is installed | on Debian it lives in `/usr/sbin`, off a user's PATH | update to a version with the fix, or run with `sudo` |
| `NotAuthorizedCanObtain` when mounting | no graphical session, polkit refuses | add the rule in step 4 |
| card is in `lsblk` but never mounts | same as above, or the service is not running | check step 4, then `systemctl --user status wt901` |
| `vaGetDriverNames() failed` | user is not in `render`, or no GPU driver | step 5, then log out and back in |
| re-encode always says `software` | GPU refused the stream | works anyway; check `vainfo`, try a newer driver |
| camera does not appear at all | it is not in File Transfer mode | choose it on the camera's screen |
| a second device is ignored | it was still mounting, or it failed and is waiting to be retried | it retries by itself after a minute |
| `--benchmark` prints nonsense | the sample is longer than the clip | use a recording of at least two minutes |
| a device shows `in the future` | its clock is ahead of real time | set the clock; `--settime` for the sensor, the app for a camera |
| version shows `-dirty` | someone edited the code on the server | revert it and release the change properly instead |

## Day to day

Updating:

```bash
cd ~/wt901 && git pull
systemctl --user restart wt901
```

Do it when the dashboard is not yellow. Restarting mid-copy is recovered from
automatically, but it wastes the transfer.

Never edit code on a dock machine. Changes go through `main`, get tested, and
come back as a release — otherwise nobody can say what is running where.
