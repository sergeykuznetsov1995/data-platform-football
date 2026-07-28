#!/usr/bin/env python3
"""Free the SofaScore gateway's single lease slot when a lease latches stuck.

The gateway runs with ``--max-active-leases 1``: exactly one non-backfill lease
may hold the paid slot.  Its own busy predicate counts a lease as active when

    (not closed and not expired) or active_tunnels or reserved_bytes
    or global_budget_escrow_bytes

so a lease survives the slot in four distinct ways, and every one of them has
been observed to latch:

* ``accounting_uncertain`` — ``_latch_lease_accounting_uncertainty`` revokes the
  lease but deliberately retains its reservation as forensic state, and the
  reaper's conservative-settlement branch excludes ``sofascore``.  Nothing
  in-process ever clears it.
* closed or expired while ``reserved_bytes``/``global_budget_escrow_bytes``
  remain — the drained-but-not-released faces.
* neither closed nor expired, but its holder died without ``DELETE``ing it —
  the lease simply idles out its full hour while every other task is refused.
  This is what killed the #1000 history wave on 2026-07-28: lease
  ``9731de76…`` went silent at 11:52:50 UTC and only released at 12:56:36 with
  ``recovered_after_restart``, and the five leagues that ran in between each
  burned three attempts on ``HTTP 429 paid-proxy concurrency limit reached``.

The durable fix belongs in the reaper, but ``scripts/proxy_filter`` is inside
the SofaScore runtime fingerprint: changing it forces a full canary
re-measurement.  This watchdog lives in ``deploy/`` — outside the fingerprint —
so it costs nothing to deploy and buys time until that rotation happens.

It is deliberately conservative.  A restart is only ever issued when the state
on disk proves the lease is unrecoverable in-process AND proves the restart will
actually help; every other anomaly raises an alert and changes nothing.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_CONTAINER = "sofascore_gw_951"
DEFAULT_STATE_DIR = Path("/root/sofascore-runtime/gateway-state")
DEFAULT_EXPECTED_MOUNT = "/root/dpf-release-aa2ce3ab"
DEFAULT_ALERT_COMMAND = "/root/.claude/hooks/tg-send.sh"

BYTES_FILE = "bytes.json"
ALLOCATIONS_FILE = "sofascore_allocations.json"
WAL_FILE = "sofascore_allocation_claims.jsonl"

WAL_EVENT_VERSION = "sofascore-allocation-wal-v1"

# A backfill lease draws on a different provider pool and cannot consume the
# production concurrency slot, so it can never be the lease we are stuck behind.
BACKFILL_SOURCE = "transfermarkt_backfill"

# ``_periodic_dump`` rewrites bytes.json every 2s. Anything older means the dump
# loop itself is wedged — which is exactly the failure the reaper can raise into,
# and which looks identical to a healthy quiet gateway from the outside.
STALE_SNAPSHOT_SECONDS = 60.0

# A lease that latched microseconds ago may still be settling. Only act once the
# state has been stable for this long.
LATCH_GRACE_SECONDS = 30.0

# A live capture writes an endpoint event every few seconds. Total WAL silence
# for this long, with no socket open, means the holder is gone — the lease is
# idling out its hour while everyone else is refused.
SILENT_LEASE_SECONDS = 600.0

# The gateway's own ceiling; an active_claim older than this cannot belong to a
# live attempt.
MAX_LEASE_TTL_SECONDS = 3600.0

CONFIRM_SAMPLES = 3
SAMPLE_INTERVAL_SECONDS = 5.0
MIN_RESTART_INTERVAL_SECONDS = 120.0
MAX_RESTARTS_PER_DAY = 12

# Several alert conditions are permanent until a human clears them — four
# allocation claims have been orphaned since 2026-07-23. Repeating an unchanged
# alert every sample would bury the one that is new.
ALERT_REPEAT_SECONDS = 6 * 3600.0

# Above this the gateway OOM-kills itself replaying the WAL into memory before
# the HTTP server starts (256M memcg, 133MB WAL took ~500MB heap on 2026-07-24).
WAL_OOM_RISK_BYTES = 50 * 1024 * 1024

logger = logging.getLogger("sofascore-gw-watchdog")


NONE = "none"
RESTART = "restart"
ALERT = "alert"
FREEZE = "freeze"


@dataclass(frozen=True)
class Verdict:
    """What the watchdog decided from one snapshot, and why."""

    action: str
    reason: str
    lease_id: str = ""
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Snapshot:
    """Everything the decision needs, already read off disk."""

    bytes_payload: Mapping[str, Any] | None
    bytes_age_seconds: float | None
    allocations: Mapping[str, Any] | None
    wal_open_lease_ids: frozenset[str]
    wal_last_event_at: Mapping[str, float]
    wal_size_bytes: int
    now: float


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_epoch(value: Any) -> float | None:
    """Epoch seconds from either a number or an ISO-8601 timestamp.

    The two state files disagree on clock format and a single ``float()`` only
    reads one of them: ``bytes.json`` dumps lease clocks as epoch floats, but
    the durable allocation journal stamps ``active_claim.started_at`` with
    ``datetime.now(timezone.utc).isoformat()`` (``scrapers/sofascore/
    workload_plan.py:1633-1638``), which ``float()`` refuses.
    """

    number = _as_float(value)
    if number is not None:
        return number
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _holds_the_slot(lease: Mapping[str, Any]) -> bool:
    """Mirror the gateway's own busy-slot predicate (``_create_lease``).

    Anything this returns ``True`` for is refusing every other ``POST
    /v1/leases`` right now, whatever else the lease looks like.
    """

    if str(lease.get("source") or "") == BACKFILL_SOURCE:
        return False
    open_and_unexpired = not lease.get("closed") and not lease.get("expired")
    return (
        open_and_unexpired
        or int(lease.get("active_tunnels") or 0) > 0
        or int(lease.get("reserved_bytes") or 0) > 0
        or int(lease.get("global_budget_escrow_bytes") or 0) > 0
    )


def _stuck_reason(
    lease: Mapping[str, Any],
    now: float,
    wal_last_event_at: Mapping[str, float],
) -> str | None:
    """Why this slot-holding lease can never be released in-process.

    Returns ``None`` for a lease that is merely busy — a live capture holds the
    slot legitimately and must never be restarted out from under.
    """

    lease_id = str(lease.get("id") or "")
    reserved = int(lease.get("reserved_bytes") or 0)
    escrow = int(lease.get("global_budget_escrow_bytes") or 0)
    tunnels = int(lease.get("active_tunnels") or 0)

    if lease.get("accounting_uncertain"):
        # Assigned exactly once and only ever True; the reaper's settlement
        # branch excludes sofascore and _close_lease refuses to drain an
        # uncertain lease. There is no in-process path that clears this.
        return (
            f"latched accounting-uncertain while holding {reserved} reserved "
            f"byte(s), {escrow} escrow byte(s) and {tunnels} tunnel(s)"
        )

    if lease.get("closed") or lease.get("expired"):
        # The gateway is done with it and still counts it against the slot:
        # whatever it retains was never drained.
        return (
            f"is {'closed' if lease.get('closed') else 'expired'} but still "
            f"holds {reserved} reserved byte(s), {escrow} escrow byte(s) and "
            f"{tunnels} tunnel(s)"
        )

    expires_at = _as_float(lease.get("expires_at"))
    if expires_at is not None and now >= expires_at:
        # The dump can be a beat behind the ``expired`` property.
        return f"is {now - expires_at:.0f}s past its TTL and still holds the slot"

    if tunnels > 0:
        # A socket is open. Whatever else is true, someone is mid-read.
        return None

    last_event = wal_last_event_at.get(lease_id)
    if last_event is None:
        # No activity was ever recorded for it. That is a claim we cannot age,
        # so we refuse to judge it rather than guess.
        return None
    silence = now - last_event
    if silence >= SILENT_LEASE_SECONDS:
        return (
            f"has made no gateway request for {silence / 60:.0f} minutes with no "
            "socket open; its holder is gone but the slot is still taken"
        )
    return None


def _orphaned_active_claims(
    allocations: Mapping[str, Any] | None,
    live_lease_ids: Iterable[str],
    now: float,
) -> list[dict[str, Any]]:
    """Durable claims with no live lease — the HTTP 409 face of the mine.

    A restart cannot help these: the WAL replay only releases attempts whose
    ``claim_intent`` is still in the live WAL.  They need a human, so they are
    alert-only.
    """

    if not isinstance(allocations, Mapping):
        return []
    live = {str(item) for item in live_lease_ids}
    orphans: list[dict[str, Any]] = []
    for run_key, run in (allocations.get("runs") or {}).items():
        if not isinstance(run, Mapping):
            continue
        for allocation_id, allocation in (run.get("allocations") or {}).items():
            if not isinstance(allocation, Mapping):
                continue
            claim = allocation.get("active_claim")
            if not isinstance(claim, Mapping):
                continue
            # The gateway persists only the token/attempt hashes and
            # ``started_at`` into ``active_claim`` — never a lease id — so this
            # guard is future-proofing and the age below is what actually
            # decides. Reading the stamp with a plain ``float()`` made both
            # skips unreachable and reported every in-flight claim of a healthy
            # run as an orphan.
            lease_id = str(claim.get("lease_id") or "")
            if lease_id and lease_id in live:
                continue
            started = _as_epoch(claim.get("started_at"))
            if started is None:
                # An unreadable stamp is not evidence of an orphan; refuse to
                # judge it rather than page a human on a claim we cannot age.
                continue
            age = now - started
            if age < MAX_LEASE_TTL_SECONDS:
                continue
            orphans.append(
                {
                    "run": str(run_key),
                    "allocation_id": str(allocation_id),
                    "lease_id": lease_id,
                    "age_seconds": round(age, 1),
                }
            )
    return orphans


def evaluate(snapshot: Snapshot) -> Verdict:
    """Decide from one snapshot. Pure — this is what the tests drive."""

    if snapshot.bytes_payload is None:
        return Verdict(ALERT, "bytes.json is missing or unreadable")
    if (
        snapshot.bytes_age_seconds is not None
        and snapshot.bytes_age_seconds > STALE_SNAPSHOT_SECONDS
    ):
        # The dump loop calls the reaper and the dump under one try: a raise in
        # the reaper stops the file from being written at all. Silence here is
        # not health.
        return Verdict(
            ALERT,
            f"bytes.json is {snapshot.bytes_age_seconds:.0f}s stale; the dump "
            "loop is probably wedged",
        )

    leases = snapshot.bytes_payload.get("leases")
    leases = [item for item in (leases or []) if isinstance(item, Mapping)]
    live_ids = [str(item.get("id") or "") for item in leases]

    orphans = _orphaned_active_claims(snapshot.allocations, live_ids, snapshot.now)

    stuck: list[tuple[Mapping[str, Any], str]] = []
    for lease in leases:
        if not _holds_the_slot(lease):
            continue
        reason = _stuck_reason(lease, snapshot.now, snapshot.wal_last_event_at)
        if reason is not None:
            stuck.append((lease, reason))

    if not stuck:
        if orphans:
            return Verdict(
                ALERT,
                f"{len(orphans)} durable allocation claim(s) outlived their lease; "
                "a restart cannot release these",
                details={"orphans": orphans},
            )
        return Verdict(NONE, "no stuck lease")

    lease, reason = stuck[0]
    lease_id = str(lease.get("id") or "")
    created = _as_float(lease.get("created_at"))
    age = None if created is None else snapshot.now - created
    if age is not None and age < LATCH_GRACE_SECONDS:
        return Verdict(
            NONE,
            f"lease {lease_id} is {age:.0f}s old; inside the grace window",
            lease_id=lease_id,
        )

    if not lease_id:
        return Verdict(ALERT, "stuck lease has no id in the dump")

    if lease_id not in snapshot.wal_open_lease_ids:
        # Restarting now would drop this attempt's durable claim for good: the
        # WAL replay is the only thing that releases it, and it is not there.
        # This is exactly how alloc-cd08d5ed became permanently orphaned after
        # the WAL was moved aside on 2026-07-28.
        return Verdict(
            FREEZE,
            f"lease {lease_id} holds the slot but has no open claim_intent in the "
            "live WAL; a restart would orphan its allocation claim permanently",
            lease_id=lease_id,
        )

    if snapshot.wal_size_bytes > WAL_OOM_RISK_BYTES:
        return Verdict(
            FREEZE,
            f"allocation WAL is {snapshot.wal_size_bytes / 1048576:.0f}MB; a "
            "restart would OOM in recovery before the HTTP server starts",
            lease_id=lease_id,
        )

    return Verdict(
        RESTART,
        f"lease {lease_id} {reason}; the slot cannot be freed in-process",
        lease_id=lease_id,
        details={
            "allocation_id": str(lease.get("allocation_id") or ""),
            "dag_id": str(lease.get("dag_id") or ""),
            "run_id": str(lease.get("run_id") or ""),
            "task_id": str(lease.get("task_id") or ""),
            "orphans": orphans,
        },
    )


@dataclass(frozen=True)
class WalState:
    """What the WAL says about attempts a restart replay could still release."""

    open_lease_ids: frozenset[str]
    last_event_at: Mapping[str, float]
    size_bytes: int


def _parse_occurred_at(value: Any) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def read_wal_state(path: Path) -> WalState:
    """Open attempts and their last activity, from the allocation WAL.

    Mirrors ``_read_allocation_wal``: an attempt is recoverable when it has a
    ``claim_intent`` and no ``allocation_finished``.  A corrupt line is not our
    problem to interpret — we simply stop trusting the file and report nothing,
    which downgrades every restart to a freeze.
    """

    try:
        size = path.stat().st_size
    except OSError:
        size = 0
    open_ids: set[str] = set()
    finished: set[str] = set()
    last_event: dict[str, float] = {}
    try:
        with path.open("rb") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                event = json.loads(raw.decode("utf-8"))
                if (
                    not isinstance(event, dict)
                    or event.get("event_version") != WAL_EVENT_VERSION
                ):
                    return WalState(frozenset(), {}, size)
                lease_id = str(event.get("lease_id") or "").strip()
                if not lease_id:
                    return WalState(frozenset(), {}, size)
                kind = event.get("event_type")
                if kind == "claim_intent":
                    open_ids.add(lease_id)
                elif kind == "allocation_finished":
                    finished.add(lease_id)
                occurred = _parse_occurred_at(event.get("occurred_at"))
                if occurred is not None:
                    previous = last_event.get(lease_id)
                    if previous is None or occurred > previous:
                        last_event[lease_id] = occurred
    except FileNotFoundError:
        return WalState(frozenset(), {}, size)
    except (OSError, ValueError, UnicodeDecodeError):
        return WalState(frozenset(), {}, size)
    return WalState(frozenset(open_ids - finished), last_event, size)


def _read_json(path: Path) -> Mapping[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        return None
    return payload if isinstance(payload, Mapping) else None


def collect_snapshot(state_dir: Path, *, now: float | None = None) -> Snapshot:
    now = time.time() if now is None else now
    bytes_path = state_dir / BYTES_FILE
    try:
        age: float | None = now - bytes_path.stat().st_mtime
    except OSError:
        age = None
    wal = read_wal_state(state_dir / WAL_FILE)
    return Snapshot(
        bytes_payload=_read_json(bytes_path),
        bytes_age_seconds=age,
        allocations=_read_json(state_dir / ALLOCATIONS_FILE),
        wal_open_lease_ids=wal.open_lease_ids,
        wal_last_event_at=wal.last_event_at,
        wal_size_bytes=wal.size_bytes,
        now=now,
    )


def _run(command: Sequence[str], *, timeout: float = 60.0) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        list(command),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def verify_container(container: str, state_dir: Path, expected_mount: str) -> str:
    """Prove we are about to restart the gateway we are reading state from.

    ``sofascore_proxy_filter`` is a different container in a different compose
    project that answers to the same DNS alias — restarting it would do nothing
    and hide the real problem.
    """

    probe = _run(["docker", "inspect", container, "--format", "{{json .Mounts}}"])
    if probe.returncode != 0:
        raise RuntimeError(f"cannot inspect {container}: {probe.stderr.strip()}")
    try:
        mounts = json.loads(probe.stdout or "[]")
    except ValueError as exc:
        raise RuntimeError(f"cannot parse mounts of {container}") from exc
    sources = {str(item.get("Source") or "") for item in mounts if isinstance(item, dict)}
    if str(state_dir) not in sources:
        raise RuntimeError(
            f"{container} does not mount {state_dir}; refusing to restart a "
            "container whose state we are not reading"
        )
    if expected_mount and expected_mount not in sources:
        raise RuntimeError(
            f"{container} does not mount the expected release tree {expected_mount}"
        )
    return container


class AlertThrottle:
    """Deliver an unchanged alert at most once every ``ALERT_REPEAT_SECONDS``.

    A new or changed message always goes out immediately — the throttle only
    suppresses repetition, never novelty.
    """

    def __init__(self, repeat_after: float = ALERT_REPEAT_SECONDS) -> None:
        self._repeat_after = repeat_after
        self._sent_at: dict[str, float] = {}

    def should_send(self, message: str, now: float) -> bool:
        previous = self._sent_at.get(message)
        if previous is not None and now - previous < self._repeat_after:
            return False
        self._sent_at[message] = now
        return True


def alert(command: str, message: str) -> None:
    logger.warning("ALERT %s", message)
    if not command:
        return
    try:
        _run([command, f"[sofascore-gw-watchdog] {message}"], timeout=30.0)
    except (OSError, subprocess.SubprocessError):
        logger.exception("could not deliver alert")


def recovered_attempt_count(log_blob: str) -> int | None:
    """How many crash-orphaned attempts the WAL replay released, per the log.

    ``_recover_allocation_wal`` logs its count unconditionally, zero included —
    so the line proves the gateway finished booting, and the number proves the
    replay actually did the release we restarted for.
    """

    marker = "crash-orphaned SofaScore allocation attempts"
    best: int | None = None
    for line in log_blob.splitlines():
        if marker not in line:
            continue
        head = line.split(marker)[0].split()
        if not head:
            continue
        try:
            best = int(head[-1])
        except ValueError:
            continue
    return best


def restart_gateway(container: str, *, wait_seconds: float = 180.0) -> bool:
    """Restart and confirm the replay actually recovered the orphaned attempt."""

    result = _run(["docker", "restart", container], timeout=120.0)
    if result.returncode != 0:
        logger.error("docker restart failed: %s", result.stderr.strip())
        return False
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        health = _run(
            ["docker", "inspect", container, "--format", "{{.State.Health.Status}}"]
        )
        if health.stdout.strip() == "healthy":
            logs = _run(["docker", "logs", container, "--since", "5m"], timeout=60.0)
            recovered = recovered_attempt_count(f"{logs.stdout}{logs.stderr}")
            if recovered is None:
                logger.error("gateway is healthy but never logged a WAL replay")
                return False
            if recovered < 1:
                logger.error(
                    "gateway replayed the WAL but released no orphaned attempt; "
                    "the slot was held by something else"
                )
                return False
            return True
        time.sleep(3.0)
    logger.error("gateway did not become healthy within %.0fs", wait_seconds)
    return False


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--expected-mount", default=DEFAULT_EXPECTED_MOUNT)
    parser.add_argument("--alert-command", default=DEFAULT_ALERT_COMMAND)
    parser.add_argument("--interval", type=float, default=SAMPLE_INTERVAL_SECONDS)
    parser.add_argument(
        "--once",
        action="store_true",
        help="evaluate a single snapshot, report the verdict and exit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="never restart; log what would have happened",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    if args.once:
        verdict = evaluate(collect_snapshot(args.state_dir))
        print(json.dumps({
            "action": verdict.action,
            "reason": verdict.reason,
            "lease_id": verdict.lease_id,
            "details": dict(verdict.details),
        }, ensure_ascii=False))
        return 0

    consecutive = 0
    pending_lease = ""
    last_restart = 0.0
    restarts_today = 0
    day = time.gmtime().tm_yday
    frozen = False
    throttle = AlertThrottle()

    while True:
        time.sleep(args.interval)
        if time.gmtime().tm_yday != day:
            day = time.gmtime().tm_yday
            restarts_today = 0
        if frozen:
            continue

        verdict = evaluate(collect_snapshot(args.state_dir))
        if verdict.action == NONE:
            consecutive = 0
            pending_lease = ""
            continue
        if verdict.action == ALERT:
            consecutive = 0
            if throttle.should_send(verdict.reason, time.time()):
                alert(args.alert_command, verdict.reason)
            continue
        if verdict.action == FREEZE:
            frozen = True
            alert(
                args.alert_command,
                f"FROZEN, needs a human: {verdict.reason}",
            )
            continue

        # RESTART: require the same lease to survive several samples.
        if verdict.lease_id != pending_lease:
            pending_lease = verdict.lease_id
            consecutive = 1
            continue
        consecutive += 1
        if consecutive < CONFIRM_SAMPLES:
            continue

        now = time.time()
        if now - last_restart < MIN_RESTART_INTERVAL_SECONDS:
            continue
        if restarts_today >= MAX_RESTARTS_PER_DAY:
            frozen = True
            alert(
                args.alert_command,
                f"FROZEN: {restarts_today} restarts today already; the mine is "
                "firing faster than the watchdog should paper over it",
            )
            continue

        if args.dry_run:
            logger.info("DRY RUN: would restart — %s", verdict.reason)
            consecutive = 0
            pending_lease = ""
            continue

        try:
            verify_container(args.container, args.state_dir, args.expected_mount)
        except RuntimeError as exc:
            frozen = True
            alert(args.alert_command, f"FROZEN: {exc}")
            continue

        logger.warning("restarting %s — %s", args.container, verdict.reason)
        recovered = restart_gateway(args.container)
        last_restart = time.time()
        restarts_today += 1
        consecutive = 0
        pending_lease = ""
        if recovered:
            alert(
                args.alert_command,
                f"restarted {args.container} in "
                f"{time.time() - now:.0f}s — {verdict.reason}",
            )
        else:
            frozen = True
            alert(
                args.alert_command,
                f"FROZEN: restarted {args.container} but recovery was not "
                "confirmed; check for a crash loop",
            )


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
