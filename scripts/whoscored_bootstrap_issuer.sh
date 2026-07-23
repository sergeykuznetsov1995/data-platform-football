#!/usr/bin/env bash
# Publish one bootstrap-mode request and synchronously start the protected
# WhoScored issuer. Installed as /usr/local/libexec/whoscored-bootstrap-issuer.
set -Eeuo pipefail
export PATH=/usr/bin:/bin

readonly SYSTEMCTL=/usr/bin/systemctl
readonly INSTALL=/usr/bin/install
readonly MKTEMP=/usr/bin/mktemp
readonly LN=/usr/bin/ln
readonly RM=/usr/bin/rm
readonly SYNC=/usr/bin/sync
readonly STAT=/usr/bin/stat
readonly READLINK=/usr/bin/readlink
readonly FLOCK=/usr/bin/flock
readonly CONTROL_DIRECTORY=/run/whoscored-daily-issuer-control
readonly MODE_REQUEST="$CONTROL_DIRECTORY/mode.env"
readonly ISSUER_SERVICE=whoscored-daily-issuer.service
readonly ISSUER_TIMER=whoscored-daily-issuer.timer

request_temp=

fail() {
  echo "WhoScored bootstrap issuer blocked: $*" >&2
  exit 78
}

cleanup_request() {
  local exit_status=$?
  trap - EXIT
  "$RM" -f -- "${request_temp:-}" "$MODE_REQUEST" || true
  exit "$exit_status"
}

assert_inactive() {
  local unit="$1"
  if "$SYSTEMCTL" is-active --quiet "$unit"; then
    fail "$unit must be inactive"
  fi
}

usage() {
  echo \
    "usage: whoscored-bootstrap-issuer publish | run RUN_ID | enable-daily" \
    >&2
  exit 64
}

test "$EUID" = 0 || fail "launcher requires root"

declare -a request_lines=()
readonly launcher_action="${1:-}"
case "${1:-}" in
  publish)
    test "$#" = 1 || usage
    request_lines=("WHOSCORED_ISSUER_MODE=bootstrap-publish")
    ;;
  run)
    test "$#" = 2 || usage
    readonly bootstrap_run_id="$2"
    [[ "$bootstrap_run_id" =~ ^scheduled__[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T10:00:00\+00:00$ ]] || \
      fail "RUN_ID must be an exact scheduled 10:00 UTC bootstrap slot"
    request_lines=(
      "WHOSCORED_ISSUER_MODE=bootstrap"
      "WHOSCORED_BOOTSTRAP_RUN_ID=$bootstrap_run_id"
    )
    ;;
  enable-daily)
    test "$#" = 1 || usage
    ;;
  *) usage ;;
esac

test ! -L "$CONTROL_DIRECTORY" || fail "control directory must not be a symlink"
test ! -e "$CONTROL_DIRECTORY" || test -d "$CONTROL_DIRECTORY" || \
  fail "control path is not a directory"
"$INSTALL" -d -o root -g root -m 0700 "$CONTROL_DIRECTORY"
test "$($READLINK -f -- "$CONTROL_DIRECTORY")" = "$CONTROL_DIRECTORY" || \
  fail "control directory is not canonical"
test "$($STAT -c '%u:%g:%a' -- "$CONTROL_DIRECTORY")" = 0:0:700 || \
  fail "control directory must be root:root 0700"

# Lock the directory inode itself; no attacker-controlled lock path is opened.
exec {control_lock_fd}<"$CONTROL_DIRECTORY"
"$FLOCK" --exclusive --nonblock "$control_lock_fd" || \
  fail "another bootstrap launcher is active"

# Every supported timer transition and bootstrap publication is now inside the
# same directory-inode lock.  Install cleanup before inspecting stale state or
# touching systemd so every post-lock failure leaves the daily path unambiguous.
trap cleanup_request EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# A stale request is removed by the EXIT trap, but is never consumed by this
# invocation. The operator must rerun after inspecting the fail-closed alert.
test ! -e "$MODE_REQUEST" && test ! -L "$MODE_REQUEST" || \
  fail "stale mode request found; it has been removed, rerun explicitly"

if test "$launcher_action" = enable-daily; then
  assert_inactive "$ISSUER_SERVICE"
  "$SYSTEMCTL" enable --now "$ISSUER_TIMER"
  "$SYSTEMCTL" is-enabled --quiet "$ISSUER_TIMER" || \
    fail "daily issuer timer did not become enabled"
  "$SYSTEMCTL" is-active --quiet "$ISSUER_TIMER" || \
    fail "daily issuer timer did not become active"
  assert_inactive "$ISSUER_SERVICE"
  test ! -e "$MODE_REQUEST" && test ! -L "$MODE_REQUEST" || \
    fail "daily timer enable observed a mode request"
  exit 0
fi

# Bootstrap authority is never published while the recurring path or another
# issuer invocation could consume it.  This transition remains under the same
# lock used by enable-daily through request cleanup and service completion.
"$SYSTEMCTL" disable --now "$ISSUER_TIMER"
assert_inactive "$ISSUER_TIMER"
assert_inactive "$ISSUER_SERVICE"

umask 0077
request_temp="$($MKTEMP "$CONTROL_DIRECTORY/.mode.env.XXXXXX")"
test -f "$request_temp" && test ! -L "$request_temp" || \
  fail "temporary mode request is not a regular file"
test "$($STAT -c '%u:%g:%a:%h' -- "$request_temp")" = 0:0:600:1 || \
  fail "temporary mode request is not private"
printf '%s\n' "${request_lines[@]}" >"$request_temp"
"$SYNC" -f "$request_temp"

# link(2) exposes the complete file at MODE_REQUEST in one atomic operation and
# fails instead of replacing a concurrently created path.
"$LN" -- "$request_temp" "$MODE_REQUEST"
"$RM" -f -- "$request_temp"
request_temp=
test -f "$MODE_REQUEST" && test ! -L "$MODE_REQUEST" || \
  fail "mode request publication failed"
test "$($STAT -c '%u:%g:%a:%h' -- "$MODE_REQUEST")" = 0:0:600:1 || \
  fail "mode request is not root-owned, private, and single-link"

# systemctl start waits for this Type=oneshot service. On dependency/start
# failures, signals, and successful completion, the EXIT trap removes the exact
# request path independently of whether systemd ran ExecStopPost.
"$SYSTEMCTL" start "$ISSUER_SERVICE"
test ! -e "$MODE_REQUEST" && test ! -L "$MODE_REQUEST" || \
  fail "issuer did not consume the one-shot mode request"
assert_inactive "$ISSUER_SERVICE"
