"""One ESPN request gate per VM: origins, pace steps, lanes and auto-reset (#1500).

The gate is the only place that decides *where* and *when* an ESPN request
goes.  Its state is one JSON document under ``flock`` (``fsync`` +
``os.replace``, the pattern of ``scrapers/whoscored/source_circuit.py``), so
every process on the VM shares the same pace, origin blocks and counters.
The policy lives in ``configs/espn/transport_policy.json``.

* Origins: each cluster has a primary and at most one reserve.  A 403 closes
  the origin; after its block expires exactly one request probes it.  The
  reserve starts closed and is probed at most once a day.  When no origin of a
  cluster is open, requests fail with ``AllOriginsBlocked``: pause, then one
  probe every few minutes; the ``history`` lane freezes first and reopens
  last.
* Pace: steps S0…S3 requests/min, one permit per request spaced ``60/rate``
  seconds apart.  ``live`` takes any free slot; ``history`` only while its
  permits of the last minute stay below ``(1 - live_share)`` of the step.
* Auto-reset: a 429, ``≥ http403_count`` 403s in a window, or a 5xx+timeout
  share above ``error_share`` drops one step, cools down and freezes
  ``history``; after a quiet cooldown the step returns to the ceiling.  Two
  resets within an hour hold S0 for six hours and leave an ``alert``.
"""

from __future__ import annotations

import fcntl
import json
import logging
import math
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Optional

from .transport_contracts import (
    AllOriginsBlocked,
    DailyCapExceeded,
    LaneClosed,
    normalize_transport_origin,
)


logger = logging.getLogger(__name__)

DEFAULT_POLICY_PATH = (
    Path(__file__).resolve().parents[2] / "configs" / "espn" / "transport_policy.json"
)
GATE_STATE_ENV = "ESPN_GATE_STATE_PATH"
STEP_CEILING_ENV = "ESPN_GATE_STEP_CEILING"
LANES = ("live", "history")
STATE_VERSION = 1

_TOP_KEYS = {
    "schema_version",
    "clusters",
    "origin_block_seconds",
    "origin_probe_seconds",
    "reserve_probe_seconds",
    "all_blocked_pause_seconds",
    "all_blocked_probe_seconds",
    "steps",
    "live_share",
    "lanes",
    "reset",
    "uncompressed_warn_bytes",
}
_RESET_KEYS = {
    "http403_count",
    "http403_window_seconds",
    "error_share",
    "error_window_seconds",
    "error_min_requests",
    "cooldown_seconds",
    "double_reset_window_seconds",
    "double_reset_hold_seconds",
}
_LANE_KEYS = {"daily_requests", "daily_bytes"}


@dataclass(frozen=True)
class TransportPolicy:
    clusters: Mapping[str, tuple[str, Optional[str]]]
    origin_block_seconds: int
    origin_probe_seconds: int
    reserve_probe_seconds: int
    all_blocked_pause_seconds: int
    all_blocked_probe_seconds: int
    steps: tuple[int, ...]
    live_share: float
    lanes: Mapping[str, Mapping[str, int]]
    reset: Mapping[str, float]
    uncompressed_warn_bytes: int

    def cluster_of(self, origin: str) -> str:
        normalized = normalize_transport_origin(origin)
        for name, (primary, reserve) in self.clusters.items():
            if normalized in (primary, reserve):
                return name
        raise ValueError("ESPN origin belongs to no transport cluster")


def _positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"transport policy: {name} must be a positive integer")
    return value


def _exact_keys(value: object, keys: set[str], name: str, optional=()) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"transport policy: {name} must be an object")
    missing = keys - set(value) - set(optional)
    unknown = set(value) - keys
    if missing or unknown:
        raise ValueError(
            f"transport policy: {name} keys mismatch "
            f"(missing {sorted(missing)}, unknown {sorted(unknown)})"
        )
    return value


def load_transport_policy(path: Optional[os.PathLike | str] = None) -> TransportPolicy:
    """Load the policy; any unknown key or out-of-range value fails closed."""

    with open(path or DEFAULT_POLICY_PATH, encoding="utf-8") as handle:
        raw = json.load(handle)
    return parse_transport_policy(raw)


def parse_transport_policy(raw: object) -> TransportPolicy:
    doc = _exact_keys(raw, _TOP_KEYS, "document")
    if doc["schema_version"] != 1:
        raise ValueError("transport policy: unsupported schema_version")

    clusters_raw = doc["clusters"]
    if not isinstance(clusters_raw, dict) or not clusters_raw:
        raise ValueError("transport policy: clusters must be a non-empty object")
    clusters: dict[str, tuple[str, Optional[str]]] = {}
    seen: set[str] = set()
    for name, spec in clusters_raw.items():
        spec = _exact_keys(
            spec, {"primary", "reserve"}, f"clusters.{name}", optional=("reserve",)
        )
        primary = normalize_transport_origin(spec["primary"])
        reserve = (
            normalize_transport_origin(spec["reserve"]) if "reserve" in spec else None
        )
        for origin in filter(None, (primary, reserve)):
            if origin in seen:
                raise ValueError("transport policy: origin listed twice")
            seen.add(origin)
        clusters[name] = (primary, reserve)

    steps = doc["steps"]
    if (
        not isinstance(steps, list)
        or not steps
        or any(type(v) is not int or v <= 0 for v in steps)
        or any(b <= a for a, b in zip(steps, steps[1:]))
    ):
        raise ValueError("transport policy: steps must be strictly increasing")

    share = doc["live_share"]
    if type(share) not in (int, float) or not 0 < share < 1:
        raise ValueError("transport policy: live_share must be in (0, 1)")

    lanes_raw = doc["lanes"]
    if not isinstance(lanes_raw, dict) or set(lanes_raw) != set(LANES):
        raise ValueError("transport policy: lanes must be exactly live and history")
    lanes = {
        lane: {
            key: _positive_int(value, f"lanes.{lane}.{key}")
            for key, value in _exact_keys(spec, _LANE_KEYS, f"lanes.{lane}").items()
        }
        for lane, spec in lanes_raw.items()
    }

    reset = dict(_exact_keys(doc["reset"], _RESET_KEYS, "reset"))
    for key, value in reset.items():
        if key == "error_share":
            if type(value) not in (int, float) or not 0 < value < 1:
                raise ValueError("transport policy: reset.error_share in (0, 1)")
        else:
            _positive_int(value, f"reset.{key}")

    return TransportPolicy(
        clusters=clusters,
        origin_block_seconds=_positive_int(
            doc["origin_block_seconds"], "origin_block_seconds"
        ),
        origin_probe_seconds=_positive_int(
            doc["origin_probe_seconds"], "origin_probe_seconds"
        ),
        reserve_probe_seconds=_positive_int(
            doc["reserve_probe_seconds"], "reserve_probe_seconds"
        ),
        all_blocked_pause_seconds=_positive_int(
            doc["all_blocked_pause_seconds"], "all_blocked_pause_seconds"
        ),
        all_blocked_probe_seconds=_positive_int(
            doc["all_blocked_probe_seconds"], "all_blocked_probe_seconds"
        ),
        steps=tuple(steps),
        live_share=float(share),
        lanes=lanes,
        reset=reset,
        uncompressed_warn_bytes=_positive_int(
            doc["uncompressed_warn_bytes"], "uncompressed_warn_bytes"
        ),
    )


def default_state_path() -> Path:
    configured = os.environ.get(GATE_STATE_ENV, "").strip()
    if configured:
        return Path(configured)
    home = os.environ.get("AIRFLOW_HOME", "").strip() or "/opt/airflow"
    return Path(home) / "state" / "espn" / "gate.json"


@dataclass(frozen=True)
class Permit:
    """One request's admission: the origin to use and the pace it ran at."""

    cluster: str
    origin: str
    lane: str
    step: int
    probe: bool
    granted_at: float


@dataclass(frozen=True)
class ReportOutcome:
    origin_closed: bool
    all_blocked: bool
    reset: bool


def _fresh_state() -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "step": None,
        "cooldown_until": 0.0,
        "hold_until": 0.0,
        "history_frozen_until": 0.0,
        "next_permit_at": 0.0,
        "origins": {},
        "all_blocked": {},
        "permits": [],
        "http403": [],
        "outcomes": [],
        "resets": [],
        "daily": {},
        "alerts": [],
    }


class TransportGate:
    """VM-wide ESPN admission control backed by one locked JSON document."""

    def __init__(
        self,
        policy: Optional[TransportPolicy] = None,
        state_path: Optional[os.PathLike | str] = None,
        lane: str = "live",
        *,
        step_ceiling: Optional[int] = None,
        utcnow_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.policy = policy or load_transport_policy()
        if lane not in LANES:
            raise ValueError("ESPN gate lane must be live or history")
        self.lane = lane
        self.state_path = Path(state_path) if state_path else default_state_path()
        if step_ceiling is None:
            raw = os.environ.get(STEP_CEILING_ENV, "").strip() or "0"
            try:
                step_ceiling = int(raw)
            except ValueError:
                raise ValueError(f"{STEP_CEILING_ENV} must be an integer") from None
        if type(step_ceiling) is not int or not (
            0 <= step_ceiling < len(self.policy.steps)
        ):
            raise ValueError("ESPN gate step ceiling is outside the policy steps")
        self.step_ceiling = step_ceiling
        self.utcnow_fn = utcnow_fn
        self.sleep_fn = sleep_fn

    # ------------------------------------------------------------------ API

    def choose_origin(self, cluster: str) -> str:
        """Return the origin the next request of ``cluster`` would use."""

        with self._state() as state:
            origin, _ = self._pick_origin(state, cluster, self._now(), claim=False)
        return origin

    def acquire(self, cluster: str) -> Permit:
        """Take one permit; waits for pace, raises when the lane or cluster is shut."""

        # A permit is granted only for "now", after every check under the
        # lock; waiting never holds a permit, so a 403 or an auto-reset seen
        # by another process during the wait applies to this request too.
        while True:
            with self._state() as state:
                result = self._try_acquire(state, cluster, self._now())
            if isinstance(result, Permit):
                return result
            self.sleep_fn(result)

    def report(
        self,
        permit: Permit,
        *,
        status: Optional[int] = None,
        timeout: bool = False,
        direct_bytes: int = 0,
    ) -> ReportOutcome:
        """Account one finished request and apply origin blocks and auto-reset."""

        with self._state() as state:
            now = self._now()
            return self._report(state, permit, status, timeout, direct_bytes, now)

    def snapshot(self) -> dict[str, Any]:
        with self._state() as state:
            now = self._now()
            step = self._effective_step(state, now)
            return {
                "step": step,
                "rate_per_minute": self.policy.steps[step],
                "step_ceiling": self.step_ceiling,
                "cooldown_until": state["cooldown_until"],
                "hold_until": state["hold_until"],
                "history_frozen_until": state["history_frozen_until"],
                "origins": json.loads(json.dumps(state["origins"])),
                "all_blocked": dict(state["all_blocked"]),
                "daily": json.loads(json.dumps(state["daily"])),
                "resets": list(state["resets"]),
                "alerts": list(state["alerts"]),
            }

    # -------------------------------------------------------------- internals

    def _now(self) -> float:
        return self.utcnow_fn().timestamp()

    def _today(self) -> str:
        return self.utcnow_fn().astimezone(timezone.utc).date().isoformat()

    def _cluster(self, cluster: str) -> tuple[str, Optional[str]]:
        try:
            return self.policy.clusters[cluster]
        except KeyError:
            raise ValueError(f"unknown ESPN transport cluster {cluster!r}") from None

    def _origin_state(self, state, origin: str, reserve: bool, now: float) -> dict:
        entry = state["origins"].get(origin)
        if entry is None:
            # The reserve is known closed for our User-Agent: start closed.
            entry = {
                "blocked_until": now + self.policy.reserve_probe_seconds
                if reserve
                else 0.0,
                "closed": reserve,
                "last_status": None,
            }
            state["origins"][origin] = entry
        return entry

    def _pick_origin(self, state, cluster: str, now: float, *, claim: bool):
        primary, reserve = self._cluster(cluster)
        candidates = [
            (origin, self._origin_state(state, origin, is_reserve, now), is_reserve)
            for origin, is_reserve in ((primary, False), (reserve, True))
            if origin is not None
        ]
        # A due reserve probe goes first, so the reserve is re-checked once a
        # day even while the primary is healthy.
        due_reserve = [
            c for c in candidates if c[2] and self._probe_due(c[1], now, True)
        ]
        for origin, entry, is_reserve in due_reserve + candidates:
            if self._probe_due(entry, now, is_reserve):
                if claim:
                    # One probe: nobody else probes this origin meanwhile.
                    if entry["closed"]:
                        entry["blocked_until"] = now + self._probe_interval(
                            state, cluster, is_reserve
                        )
                    else:
                        entry["recheck_at"] = now + self.policy.reserve_probe_seconds
                return origin, True
            if not entry["closed"]:
                return origin, False
        raise AllOriginsBlocked(f"all ESPN origins of cluster {cluster!r} are blocked")

    @staticmethod
    def _probe_due(entry: dict, now: float, is_reserve: bool) -> bool:
        if entry["closed"]:
            return now >= entry["blocked_until"]
        # An open reserve is idle while the primary works: re-check it daily.
        recheck_at = entry.get("recheck_at")
        return is_reserve and recheck_at is not None and now >= recheck_at

    def _probe_interval(self, state, cluster: str, is_reserve: bool) -> int:
        if is_reserve:
            return self.policy.reserve_probe_seconds
        if state["all_blocked"].get(cluster):
            return self.policy.all_blocked_probe_seconds
        return self.policy.origin_probe_seconds

    def _effective_step(self, state, now: float) -> int:
        if now < state["hold_until"]:
            return 0
        if state["step"] is None or now >= state["cooldown_until"]:
            state["step"] = self.step_ceiling
        return min(state["step"], self.step_ceiling)

    def _history_closed(self, state, now: float) -> bool:
        return now < state["history_frozen_until"] or any(state["all_blocked"].values())

    def _daily(self, state) -> dict:
        today = self._today()
        daily = state["daily"]
        if daily.get("date") != today:
            daily.clear()
            daily["date"] = today
        return daily.setdefault(self.lane, {"requests": 0, "bytes": 0})

    def _try_acquire(self, state, cluster: str, now: float):
        self._cluster(cluster)
        if self.lane == "history" and self._history_closed(state, now):
            raise LaneClosed("ESPN history lane is frozen")
        caps = self.policy.lanes[self.lane]
        daily = self._daily(state)
        if (
            daily["requests"] >= caps["daily_requests"]
            or daily["bytes"] >= caps["daily_bytes"]
        ):
            raise DailyCapExceeded(f"ESPN {self.lane} lane daily cap reached")
        self._pick_origin(state, cluster, now, claim=False)
        if state["next_permit_at"] > now:
            return state["next_permit_at"] - now

        step = self._effective_step(state, now)
        rate = self.policy.steps[step]
        state["permits"] = [p for p in state["permits"] if p[0] > now - 60.0]
        if self.lane == "history":
            quota = math.floor(rate * (1.0 - self.policy.live_share))
            history = [p[0] for p in state["permits"] if p[1] == "history"]
            if len(history) >= quota:
                return max(0.05, history[0] + 60.0 - now)
        origin, probe = self._pick_origin(state, cluster, now, claim=True)
        state["next_permit_at"] = now + 60.0 / rate
        state["permits"].append([now, self.lane])
        daily["requests"] += 1
        return Permit(cluster, origin, self.lane, step, probe, now)

    def _report(self, state, permit, status, timeout, direct_bytes, now):
        policy = self.policy
        reset_policy = policy.reset
        self._daily(state)["bytes"] += max(0, int(direct_bytes))

        second = int(now)
        window = now - reset_policy["error_window_seconds"]
        outcomes = [o for o in state["outcomes"] if o[0] > window]
        failed = 1 if timeout or (status is not None and 500 <= status <= 599) else 0
        if outcomes and outcomes[-1][0] == second:
            outcomes[-1][1] += 1
            outcomes[-1][2] += failed
        else:
            outcomes.append([second, 1, failed])
        state["outcomes"] = outcomes

        entry = state["origins"][permit.origin]
        _, reserve = self._cluster(permit.cluster)
        is_reserve = permit.origin == reserve
        origin_closed = False
        reset_reason = None

        if status == 403:
            origin_closed = True
            entry["closed"] = True
            entry["last_status"] = 403
            if is_reserve:
                block = policy.reserve_probe_seconds
            elif state["all_blocked"].get(permit.cluster):
                block = policy.all_blocked_probe_seconds
            else:
                block = policy.origin_block_seconds
            # A late 403 of a request admitted earlier never shortens a block.
            entry["blocked_until"] = max(entry["blocked_until"], now + block)
            window403 = now - reset_policy["http403_window_seconds"]
            state["http403"] = [t for t in state["http403"] if t > window403] + [now]
            if len(state["http403"]) >= reset_policy["http403_count"]:
                reset_reason = "http403"
                state["http403"] = []
        elif status is not None and (permit.probe or not entry["closed"]):
            # Only the probe reopens a closed origin: a late answer of a
            # request admitted before the block proves nothing.
            entry["last_status"] = status
            entry["closed"] = False
            entry["blocked_until"] = 0.0
            if is_reserve and permit.probe:
                entry["recheck_at"] = now + policy.reserve_probe_seconds
            if permit.probe and state["all_blocked"].pop(permit.cluster, None):
                # Live resumes now; history reopens last, after a cooldown.
                state["history_frozen_until"] = max(
                    state["history_frozen_until"],
                    now + reset_policy["cooldown_seconds"],
                )

        all_blocked = False
        if origin_closed:
            # Every origin of the cluster closed, even if a probe is already
            # due: the cluster is blocked and the pause starts now.
            origins = [o for o in self._cluster(permit.cluster) if o is not None]
            if all(state["origins"][o]["closed"] for o in origins):
                all_blocked = True
                if not state["all_blocked"].get(permit.cluster):
                    state["all_blocked"][permit.cluster] = now
                    # The pause runs from the last closure, whichever origin
                    # closed last; the reserve keeps its daily probe.
                    primary_entry = state["origins"][self._cluster(permit.cluster)[0]]
                    primary_entry["blocked_until"] = max(
                        primary_entry["blocked_until"],
                        now + policy.all_blocked_pause_seconds,
                    )
                    logger.error(
                        "ESPN gate: all origins of cluster %s are blocked",
                        permit.cluster,
                    )

        if status == 429:
            reset_reason = "http429"
        total = sum(o[1] for o in outcomes)
        errors = sum(o[2] for o in outcomes)
        if (
            reset_reason is None
            and total >= reset_policy["error_min_requests"]
            and errors / total > reset_policy["error_share"]
        ):
            reset_reason = "error_share"
            state["outcomes"] = []
        if reset_reason is not None:
            self._auto_reset(state, reset_reason, now)
        return ReportOutcome(origin_closed, all_blocked, reset_reason is not None)

    def _auto_reset(self, state, reason: str, now: float) -> None:
        reset_policy = self.policy.reset
        step = self._effective_step(state, now)
        state["step"] = max(0, step - 1)
        cooldown = now + reset_policy["cooldown_seconds"]
        state["cooldown_until"] = cooldown
        state["history_frozen_until"] = max(state["history_frozen_until"], cooldown)
        window = now - reset_policy["double_reset_window_seconds"]
        state["resets"] = [t for t in state["resets"] if t > window] + [now]
        logger.warning(
            "ESPN gate auto-reset (%s): step S%d, cooldown until %s",
            reason,
            state["step"],
            datetime.fromtimestamp(cooldown, timezone.utc).isoformat(),
        )
        if len(state["resets"]) >= 2:
            hold = now + reset_policy["double_reset_hold_seconds"]
            state["step"] = 0
            state["hold_until"] = hold
            state["cooldown_until"] = max(state["cooldown_until"], hold)
            alert = {
                "at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                "kind": "double_reset",
                "reason": reason,
                "hold_until": datetime.fromtimestamp(hold, timezone.utc).isoformat(),
            }
            state["alerts"] = (state["alerts"] + [alert])[-20:]
            logger.error(
                "ESPN gate: two auto-resets within an hour, S0 held: %s", alert
            )

    # ------------------------------------------------------------ state file

    @contextmanager
    def _state(self) -> Iterator[dict[str, Any]]:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.state_path.with_name(self.state_path.name + ".lock")
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            state = self._read_state()
            before = json.dumps(state, sort_keys=True)
            yield state
            if json.dumps(state, sort_keys=True) != before:
                self._write_state(state)
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _read_state(self) -> dict[str, Any]:
        try:
            with open(self.state_path, encoding="utf-8") as handle:
                state = json.load(handle)
        except FileNotFoundError:
            return _fresh_state()
        if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
            raise ValueError("ESPN gate state has an unknown format")
        return state

    def _write_state(self, state: dict[str, Any]) -> None:
        temporary = self.state_path.with_name(
            f"{self.state_path.name}.tmp-{os.getpid()}"
        )
        payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(descriptor, payload.encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, self.state_path)


__all__ = [
    "DEFAULT_POLICY_PATH",
    "GATE_STATE_ENV",
    "LANES",
    "Permit",
    "ReportOutcome",
    "STEP_CEILING_ENV",
    "TransportGate",
    "TransportPolicy",
    "default_state_path",
    "load_transport_policy",
    "parse_transport_policy",
]
