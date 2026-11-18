#!/bin/sh
set -eu

if test "${CLUBELO_CANARY_UPSTREAM:-}" = control; then
extension=/usr/local/libexec/whoscored/flaresolverr_extended.py
receipt=/usr/local/share/whoscored/flaresolverr-extension.sha256

test -f "$extension" && test ! -L "$extension"
test -f "$receipt" && test ! -L "$receipt"
test "$(stat -c '%u:%g:%a:%h' "$extension")" = '0:0:444:1'
test "$(stat -c '%u:%g:%a:%h' "$receipt")" = '0:0:444:1'
sha256sum --check --status "$receipt"
test -L /app/chromedriver
test "$(readlink /app/chromedriver)" = /tmp/whoscored-chromedriver
install -m 0700 /usr/local/share/whoscored/chromedriver.original \
  /tmp/whoscored-chromedriver
upstream=/usr/local/libexec/whoscored/flaresolverr_extended.py
elif test "${CLUBELO_CANARY_UPSTREAM:-}" = candidate; then
upstream=/app/flaresolverr.py
else
echo "clubelo-canary: CLUBELO_CANARY_UPSTREAM must be control or candidate" >&2
exit 1
fi

test -n "${upstream:-}"
test -f "$upstream" && test ! -L "$upstream"

exec /usr/local/bin/python -I -u -c '
import importlib.util
import os
import runpy
import stat
import sys

UPSTREAMS = {
    "control": "/usr/local/libexec/whoscored/flaresolverr_extended.py",
    "candidate": "/app/flaresolverr.py",
}

mode = os.environ.get("CLUBELO_CANARY_UPSTREAM", "")
if mode not in UPSTREAMS:
    sys.stderr.write("clubelo-canary: invalid CLUBELO_CANARY_UPSTREAM\n")
    raise SystemExit(2)
upstream = UPSTREAMS[mode]

sys.path.insert(0, "/app")

try:
    active_info = os.stat(
        "/var/lib/clubelo-canary-run/active_run.json", follow_symlinks=False
    )
except OSError:
    active_info = None
if active_info is None or not stat.S_ISREG(active_info.st_mode):
    sys.stderr.write("clubelo-canary: missing or invalid active_run.json\n")
    raise SystemExit(2)

solver_spec = importlib.util.spec_from_file_location(
    "clubelo_canary_solver_instrumentation",
    "/usr/local/libexec/clubelo-canary/solver_instrumentation.py",
)
solver_instrumentation = importlib.util.module_from_spec(solver_spec)
sys.modules[solver_spec.name] = solver_instrumentation
solver_spec.loader.exec_module(solver_instrumentation)

solver_instrumentation.install(
    state_dir="/var/lib/clubelo-canary-run", arm=mode
)

runpy.run_path(upstream, run_name="__main__")
'
