# ESPN watchdog adapter v1: inactive install and recovery

This is an exact, prepared procedure only. Do not run it without a separately
reviewed production change window. Preparation stages and validates immutable
bytes; it does not replace the live morning report or install an active cron
entry. The activation block is deliberately separate and installs cron last.

## Prepare and validate inactive bytes

```bash
set -eu
reviewed_source_root=/root/dpf-espn-release
watchdog_root=/root/watchdog
release_root=/root/watchdog/releases/espn-release-c2225657d354ee2a21f8c2e22daf5e233a25a3c0
stage_root=/root/watchdog/.espn-watchdog-v1-stage-c2225657d354ee2a21f8c2e22daf5e233a25a3c0
morning_sha256=1dee57096b30d6362dd7d542aa664ec3758b07e684a8344700db917ce2974e91
adapter_sha256=a94b6c9fd82a4fd4a5faf2040fe4da93cb768d78f02a51c3082405a1f1b747a9
patch_sha256=19198db13821f50db844a90dc5e916d06b8dc69fc8d3ba82c94d0c88a1bd773d
cron_sha256=811cf35601cb3a7210005da50b75c1b6db87b3f25852123fe2ce1c2ddbb4af70
probe_sha256=040c79abf7f6757f5dbbe1541b53711d44f2ef74578d0bbbda99dbcce278ed64
scrapers_init_sha256=cf5b2e633e20bef29d1cb0adee3355895772e47e16ee1b1556e9c5fe0a313b5f
espn_init_sha256=afd1195500e289d967d4b72d54c144ca99ea629b21b09149bcbb313f8842c2f3
layout_sha256=5bd383d1831ee8838aeb4ee1c8484d88dafc4f7ab1e9e08da9c11a7d89797684

test ! -e "$stage_root"
test ! -e "$release_root"
test ! -e "$watchdog_root/morning_report.py.pre-espn-watchdog-v1"
test ! -e "$watchdog_root/espn_watchdog_adapter_v1.py"
test ! -e /etc/cron.d/espn-rollout-observer-v1

install -d -o root -g root -m 0700 "$stage_root"
install -d -o root -g root -m 0755 \
  "$stage_root/release/scripts" "$stage_root/release/scrapers/espn"
install -o root -g root -m 0444 \
  "$reviewed_source_root/scripts/espn_rollout_probe_v1.py" \
  "$stage_root/release/scripts/espn_rollout_probe_v1.py"
install -o root -g root -m 0444 \
  "$reviewed_source_root/scrapers/__init__.py" \
  "$stage_root/release/scrapers/__init__.py"
install -o root -g root -m 0444 \
  "$reviewed_source_root/scrapers/espn/__init__.py" \
  "$stage_root/release/scrapers/espn/__init__.py"
install -o root -g root -m 0444 \
  "$reviewed_source_root/scrapers/espn/layout.py" \
  "$stage_root/release/scrapers/espn/layout.py"
install -o root -g root -m 0555 \
  "$reviewed_source_root/scripts/espn_watchdog_adapter_v1.py" \
  "$stage_root/espn_watchdog_adapter_v1.py"
install -o root -g root -m 0444 \
  "$reviewed_source_root/deploy/espn/watchdog/morning_report_espn_v1.patch" \
  "$stage_root/morning_report_espn_v1.patch"
install -o root -g root -m 0644 \
  "$reviewed_source_root/deploy/espn/watchdog/espn_rollout_observer_v1.cron" \
  "$stage_root/espn_rollout_observer_v1.cron"
cp --preserve=all "$watchdog_root/morning_report.py" \
  "$stage_root/morning_report.py"

test "$(sha256sum "$stage_root/morning_report.py" | cut -d ' ' -f 1)" = "$morning_sha256"
test "$(sha256sum "$stage_root/espn_watchdog_adapter_v1.py" | cut -d ' ' -f 1)" = "$adapter_sha256"
test "$(sha256sum "$stage_root/morning_report_espn_v1.patch" | cut -d ' ' -f 1)" = "$patch_sha256"
test "$(sha256sum "$stage_root/espn_rollout_observer_v1.cron" | cut -d ' ' -f 1)" = "$cron_sha256"
test "$(sha256sum "$stage_root/release/scripts/espn_rollout_probe_v1.py" | cut -d ' ' -f 1)" = "$probe_sha256"
test "$(sha256sum "$stage_root/release/scrapers/__init__.py" | cut -d ' ' -f 1)" = "$scrapers_init_sha256"
test "$(sha256sum "$stage_root/release/scrapers/espn/__init__.py" | cut -d ' ' -f 1)" = "$espn_init_sha256"
test "$(sha256sum "$stage_root/release/scrapers/espn/layout.py" | cut -d ' ' -f 1)" = "$layout_sha256"

patch --dry-run --forward --fuzz=0 -p0 -d "$stage_root" \
  < "$stage_root/morning_report_espn_v1.patch"
patch --forward --fuzz=0 -p0 -d "$stage_root" \
  < "$stage_root/morning_report_espn_v1.patch"
/root/.venvs/dpf-test/bin/python -B -c \
  'import ast,pathlib; root=pathlib.Path("/root/watchdog/.espn-watchdog-v1-stage-c2225657d354ee2a21f8c2e22daf5e233a25a3c0"); ast.parse((root/"morning_report.py").read_text()); ast.parse((root/"espn_watchdog_adapter_v1.py").read_text()); ast.parse((root/"release/scripts/espn_rollout_probe_v1.py").read_text())'

chmod 0555 "$stage_root/release" "$stage_root/release/scripts" \
  "$stage_root/release/scrapers" "$stage_root/release/scrapers/espn"
install -d -o root -g root -m 0755 "$watchdog_root/releases"
mv "$stage_root/release" "$release_root"
install -o root -g root -m 0555 \
  "$stage_root/espn_watchdog_adapter_v1.py" \
  "$watchdog_root/espn_watchdog_adapter_v1.py"

set +e
adapter_output=$(/root/.venvs/dpf-test/bin/python -B \
  "$watchdog_root/espn_watchdog_adapter_v1.py" \
  --release-root "$release_root" --observer morning --format lines)
adapter_status=$?
set -e
test "$adapter_status" -eq 0 || test "$adapter_status" -eq 1
test "$(printf '%s\n' "$adapter_output" | wc -l)" -eq 15
printf '%s\n' "$adapter_output"
PYTHONPATH="$watchdog_root" /usr/bin/python3 -B \
  "$stage_root/morning_report.py" --dry-run
```

Stop here. Review both read-only previews and the staged files before running
the activation block. A failing ESPN result is visible and does not bypass the
required 15-line shape check.

## Activate only after review

```bash
set -eu
watchdog_root=/root/watchdog
release_root=/root/watchdog/releases/espn-release-c2225657d354ee2a21f8c2e22daf5e233a25a3c0
stage_root=/root/watchdog/.espn-watchdog-v1-stage-c2225657d354ee2a21f8c2e22daf5e233a25a3c0
morning_sha256=1dee57096b30d6362dd7d542aa664ec3758b07e684a8344700db917ce2974e91
adapter_sha256=a94b6c9fd82a4fd4a5faf2040fe4da93cb768d78f02a51c3082405a1f1b747a9
cron_sha256=811cf35601cb3a7210005da50b75c1b6db87b3f25852123fe2ce1c2ddbb4af70
test "$(sha256sum "$watchdog_root/morning_report.py" | cut -d ' ' -f 1)" = "$morning_sha256"
test "$(sha256sum "$watchdog_root/espn_watchdog_adapter_v1.py" | cut -d ' ' -f 1)" = "$adapter_sha256"
test "$(sha256sum "$stage_root/espn_rollout_observer_v1.cron" | cut -d ' ' -f 1)" = "$cron_sha256"
test -d "$release_root"
test ! -e "$watchdog_root/morning_report.py.pre-espn-watchdog-v1"
test ! -e /etc/cron.d/espn-rollout-observer-v1
cp --preserve=all "$watchdog_root/morning_report.py" \
  "$watchdog_root/morning_report.py.pre-espn-watchdog-v1"
install -o root -g root -m 0755 "$stage_root/morning_report.py" \
  "$watchdog_root/morning_report.py"
install -o root -g root -m 0644 \
  "$stage_root/espn_rollout_observer_v1.cron" \
  /etc/cron.d/espn-rollout-observer-v1
```

## Recover preparation, activation, or a partial activation

Recovery disables every installed byte and restores the exact pre-install
morning report when activation reached the backup step.

```bash
set -eu
watchdog_root=/root/watchdog
release_root=/root/watchdog/releases/espn-release-c2225657d354ee2a21f8c2e22daf5e233a25a3c0
stage_root=/root/watchdog/.espn-watchdog-v1-stage-c2225657d354ee2a21f8c2e22daf5e233a25a3c0
if test -e /etc/cron.d/espn-rollout-observer-v1; then
  test ! -e /etc/cron.d/espn-rollout-observer-v1.disabled
  mv /etc/cron.d/espn-rollout-observer-v1 \
    /etc/cron.d/espn-rollout-observer-v1.disabled
fi
if test -e "$watchdog_root/morning_report.py.pre-espn-watchdog-v1"; then
  test ! -e "$watchdog_root/morning_report.py.espn-watchdog-v1.disabled"
  mv "$watchdog_root/morning_report.py" \
    "$watchdog_root/morning_report.py.espn-watchdog-v1.disabled"
  mv "$watchdog_root/morning_report.py.pre-espn-watchdog-v1" \
    "$watchdog_root/morning_report.py"
fi
if test -e "$watchdog_root/espn_watchdog_adapter_v1.py"; then
  test ! -e "$watchdog_root/espn_watchdog_adapter_v1.py.disabled"
  mv "$watchdog_root/espn_watchdog_adapter_v1.py" \
    "$watchdog_root/espn_watchdog_adapter_v1.py.disabled"
fi
if test -e "$release_root"; then
  test ! -e "$release_root.disabled"
  mv "$release_root" "$release_root.disabled"
fi
if test -e "$stage_root"; then
  test ! -e "$stage_root.disabled"
  mv "$stage_root" "$stage_root.disabled"
fi
```
