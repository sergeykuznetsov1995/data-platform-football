"""Fail-closed SofaScore proxy budget derived from measured canary traffic.

Reservations are shared through a flock-protected ledger, so Airflow retries and
parallel tasks spend from one logical DAG-run allowance.  A transport passes the
reservation token to the filtering proxy; the proxy charges real bytes in both
directions before forwarding each chunk.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional

from scrapers.sofascore.workload_plan import (
    MIN_COLD_SAMPLES_PER_CLASS,
    MIN_DISTINCT_EXITS_PER_CLASS,
    WORKLOAD_ARTIFACT_SCHEMA_VERSION,
    WORKLOAD_BUDGET_DERIVATION,
    WORKLOAD_METER,
    WorkloadPolicyUnavailable,
    load_verified_workload_policy,
)


CANARY_SCHEMA_VERSION = WORKLOAD_ARTIFACT_SCHEMA_VERSION
LEGACY_CANARY_SCHEMA_VERSION = 1
LEDGER_SCHEMA_VERSION = 1
BUDGET_DERIVATION = WORKLOAD_BUDGET_DERIVATION
LEGACY_BUDGET_DERIVATION = "max_measured_total_and_per_run_endpoint_max_v1"
MIN_CANARY_RUNS = MIN_COLD_SAMPLES_PER_CLASS
MIN_DISTINCT_PROXY_EXITS = MIN_DISTINCT_EXITS_PER_CLASS
REQUIRED_BUDGET_COHORT = "25_matches_50_players"
REQUIRED_BUDGET_MODE = "cold"
REQUIRED_BENCHMARK_MODES = frozenset({
    "cold",
    "no_op",
    "offline_replay",
    "single_endpoint_resume",
})
REQUIRED_METRICS = frozenset({
    "browser_sessions",
    "navigations",
    "request_count",
    "source_request_count",
    "completed_matches",
    "completed_players",
    "matches_per_second",
    "players_per_second",
    "p50_duration_ms",
    "p95_duration_ms",
    "cache_hit_rate",
    "replay_hit_rate",
    "endpoint_completeness",
})


class ProductionBudgetUnavailable(RuntimeError):
    """The checked-in canary is insufficient to authorize paid traffic."""


class ProxyBudgetExceeded(RuntimeError):
    """The next reservation/byte chunk would cross the logical-run budget."""


class BudgetAccountingError(RuntimeError):
    """Provider traffic was missing, inconsistent, or exceeded its reservation."""


def _validate_v2_sample_status_evidence(
    class_name: str,
    raw_class: Mapping[str, object],
    sample: Mapping[str, object],
) -> None:
    evidence = sample.get("evidence")
    counts = evidence.get("endpoint_status_counts") if isinstance(
        evidence, Mapping
    ) else None
    required = raw_class.get("required_endpoints")
    if (
        not isinstance(counts, Mapping)
        or not counts
        or not isinstance(required, list)
        or not required
        or set(counts) != set(required)
    ):
        raise ProductionBudgetUnavailable(
            f"{class_name!r} sample omitted endpoint status evidence"
        )
    terminal = {"success", "legitimate_empty", "not_supported"}
    normalized: dict[str, dict[str, int]] = {}
    for endpoint, statuses in counts.items():
        if not isinstance(statuses, Mapping) or not statuses:
            raise ProductionBudgetUnavailable(
                f"{class_name!r} sample has invalid endpoint status evidence"
            )
        normalized[str(endpoint)] = {}
        for status, count in statuses.items():
            if (
                status not in terminal
                or isinstance(count, bool)
                or not isinstance(count, int)
                or count <= 0
            ):
                raise ProductionBudgetUnavailable(
                    f"{class_name!r} sample has invalid endpoint status evidence"
                )
            normalized[str(endpoint)][str(status)] = count
    planned = evidence.get("planned_endpoints")
    raw_count = evidence.get("raw_payload_count")
    if (
        isinstance(planned, bool)
        or not isinstance(planned, int)
        or planned <= 0
        or raw_count != planned
        or sum(sum(values.values()) for values in normalized.values()) != planned
    ):
        raise ProductionBudgetUnavailable(
            f"{class_name!r} sample endpoint status evidence is incomplete"
        )
    scope = raw_class.get("scope")
    if scope == "match":
        strict = set(required)
    elif scope == "player":
        strict = {"player_profile"}
    else:
        strict = {"schedule_last", "schedule_next"}
    allowed_required = {"success", "legitimate_empty"}
    if any(set(normalized[name]) - allowed_required for name in strict):
        raise ProductionBudgetUnavailable(
            f"{class_name!r} required endpoint status is not_supported"
        )
    if scope == "season" and evidence.get("season_plan_complete") is not True:
        raise ProductionBudgetUnavailable(
            f"{class_name!r} sample has no complete season-plan evidence"
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def experimental_canary_policy_id(hard_cap_bytes: int) -> str:
    """Shared identity for one explicitly capped, non-production canary.

    Both the browser-side budget ledger and proxy control plane must derive the
    same value independently.  It intentionally cannot authorize production.
    """

    if (
        isinstance(hard_cap_bytes, bool)
        or not isinstance(hard_cap_bytes, int)
        or hard_cap_bytes <= 0
    ):
        raise ValueError("experimental canary cap must be a positive integer")
    policy = {
        "policy_version": 2,
        "source": "sofascore_canary",
        "meter": WORKLOAD_METER,
        "hard_cap_bytes": hard_cap_bytes,
        "isolated_serial": True,
        "exit_probe_host": "api.ipify.org",
        "production_authorized": False,
    }
    return hashlib.sha256(
        json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class BudgetPolicy:
    artifact_id: str
    hard_run_bytes: int
    endpoint_reservation_bytes: Mapping[str, int]
    sample_count: int
    distinct_proxy_exits: int
    workload_class: Optional[str] = None
    parent_artifact_id: Optional[str] = None

    def reservation_for(self, endpoint: str) -> int:
        try:
            value = int(self.endpoint_reservation_bytes[endpoint])
        except KeyError as exc:
            raise ProductionBudgetUnavailable(
                f"verified canary has no reservation for endpoint {endpoint!r}"
            ) from exc
        if value <= 0:
            raise ProductionBudgetUnavailable(
                f"invalid canary reservation for endpoint {endpoint!r}"
            )
        return value


def _load_v2_class_policy(
    artifact_path: Path,
    raw: bytes,
    payload: Mapping[str, object],
    workload_class: Optional[str],
) -> BudgetPolicy:
    """Adapt one verified workload class to the capture-engine budget API."""

    if not isinstance(workload_class, str) or not workload_class.strip():
        raise ProductionBudgetUnavailable(
            "v2 workload artifact requires one explicit workload_class"
        )
    name = workload_class.strip()
    try:
        workload_policy = load_verified_workload_policy(artifact_path)
        measured = workload_policy.classes[name]
    except KeyError as exc:
        raise ProductionBudgetUnavailable(
            f"verified workload artifact has no class {name!r}"
        ) from exc
    except WorkloadPolicyUnavailable as exc:
        raise ProductionBudgetUnavailable(str(exc)) from exc

    raw_classes = payload.get("workload_classes")
    raw_class = raw_classes.get(name) if isinstance(raw_classes, Mapping) else None
    if not isinstance(raw_class, Mapping):
        raise ProductionBudgetUnavailable(
            f"verified workload artifact has no class {name!r}"
        )
    samples = raw_class.get("samples")
    if not isinstance(samples, list):
        raise ProductionBudgetUnavailable(f"{name!r}.samples must be an array")
    observed_endpoint_maxima: dict[str, int] = {
        endpoint: 0 for endpoint in measured.required_endpoints
    }
    for sample in samples:
        if not isinstance(sample, Mapping):
            raise ProductionBudgetUnavailable(
                f"{name!r} sample must be an object"
            )
        _validate_v2_sample_status_evidence(name, raw_class, sample)
        request_map = sample.get("endpoint_request_provider_bytes")
        if not isinstance(request_map, Mapping):
            # The workload policy validator normally catches this first.  Keep
            # the adapter fail-closed if that contract ever changes.
            raise ProductionBudgetUnavailable(
                f"{name!r} sample omitted exact request accounting"
            )
        for endpoint in measured.required_endpoints:
            observations = request_map.get(endpoint)
            if not isinstance(observations, list) or not observations:
                raise ProductionBudgetUnavailable(
                    f"{name!r} sample omitted endpoint {endpoint!r}"
                )
            observed_endpoint_maxima[endpoint] = max(
                observed_endpoint_maxima[endpoint],
                max(int(value) for value in observations),
            )
    if any(value <= 0 for value in observed_endpoint_maxima.values()):
        missing = sorted(
            endpoint
            for endpoint, value in observed_endpoint_maxima.items()
            if value <= 0
        )
        raise ProductionBudgetUnavailable(
            f"{name!r} has no positive measured request for: {', '.join(missing)}"
        )

    # A cold run always attributes browser warm-up bytes to the first missing
    # endpoint.  On endpoint resume that first endpoint can be *any* member of
    # the class, so a cold per-endpoint maximum is not a safe upper bound (for
    # example incidents may inherit warm-up although event did in every cold
    # sample).  Let each known endpoint reserve the measured class remainder;
    # SharedBudgetLedger and the filtering proxy still enforce the exact hard
    # task/allocation total before every provider byte is forwarded.
    endpoint_reservations = {
        endpoint: measured.hard_task_bytes
        for endpoint in measured.required_endpoints
    }

    parent_artifact_id = hashlib.sha256(raw).hexdigest()
    class_artifact_id = hashlib.sha256(
        f"{parent_artifact_id}\0{name}".encode("utf-8")
    ).hexdigest()
    return BudgetPolicy(
        artifact_id=class_artifact_id,
        hard_run_bytes=measured.hard_task_bytes,
        endpoint_reservation_bytes=endpoint_reservations,
        sample_count=measured.sample_count,
        distinct_proxy_exits=measured.distinct_proxy_exits,
        workload_class=name,
        parent_artifact_id=parent_artifact_id,
    )


def _load_legacy_verified_policy(raw: bytes, payload: Mapping[str, object]) -> BudgetPolicy:
    """Read v1 only for explicit offline compatibility tests/migrations.

    Production callers never reach this function unless they deliberately set
    ``allow_legacy_v1=True``.  A v1 artifact cannot describe a task class, so
    treating it as a production cap would allow one large free-form DagRun.
    """

    if payload.get("budget_derivation") != LEGACY_BUDGET_DERIVATION:
        raise ProductionBudgetUnavailable(
            f"legacy canary budget_derivation must be {LEGACY_BUDGET_DERIVATION}"
        )

    if payload.get("source") != "sofascore":
        raise ProductionBudgetUnavailable("proxy canary source must be sofascore")
    if payload.get("meter") != WORKLOAD_METER:
        raise ProductionBudgetUnavailable("canary must use provider-path accounting")
    if payload.get("verified") is not True:
        raise ProductionBudgetUnavailable(
            "SofaScore paid capture disabled: proxy canary is not verified"
        )
    samples = payload.get("samples")
    if not isinstance(samples, list):
        raise ProductionBudgetUnavailable("canary samples must be an array")
    if any(
        not isinstance(sample, dict)
        or not isinstance(sample.get("budget_eligible"), bool)
        for sample in samples
    ):
        raise ProductionBudgetUnavailable(
            "every verified canary sample needs explicit budget_eligible"
        )
    observed_modes = {
        str(sample.get("mode", ""))
        for sample in samples
        if isinstance(sample, dict)
    }
    missing_modes = sorted(REQUIRED_BENCHMARK_MODES - observed_modes)
    if missing_modes:
        raise ProductionBudgetUnavailable(
            f"verified benchmark is missing modes: {', '.join(missing_modes)}"
        )
    budget_samples = [sample for sample in samples if sample["budget_eligible"]]
    if len(budget_samples) < MIN_CANARY_RUNS:
        raise ProductionBudgetUnavailable(
            f"canary needs at least {MIN_CANARY_RUNS} budget-eligible logical runs"
        )

    totals: list[int] = []
    exits: set[str] = set()
    run_ids: set[str] = set()
    endpoint_run_maxima: dict[str, list[int]] = {}
    endpoint_sample_counts: dict[str, int] = {}
    for sample in budget_samples:
        run_id = str(sample.get("run_id", "")).strip()
        if not run_id or run_id in run_ids:
            raise ProductionBudgetUnavailable(
                "budget-eligible canary run_id values must be non-empty and unique"
            )
        run_ids.add(run_id)
        if sample.get("cohort") != REQUIRED_BUDGET_COHORT:
            raise ProductionBudgetUnavailable(
                f"budget canary cohort must be {REQUIRED_BUDGET_COHORT}"
            )
        if sample.get("mode") != REQUIRED_BUDGET_MODE:
            raise ProductionBudgetUnavailable(
                f"budget-eligible canary mode must be {REQUIRED_BUDGET_MODE}"
            )
        metrics = sample.get("metrics")
        if not isinstance(metrics, dict) or not REQUIRED_METRICS.issubset(metrics):
            missing = sorted(REQUIRED_METRICS - set(metrics or {}))
            raise ProductionBudgetUnavailable(
                f"budget canary is missing metrics: {', '.join(missing)}"
            )
        if metrics.get("completed_matches") != 25 or metrics.get("completed_players") != 50:
            raise ProductionBudgetUnavailable(
                "budget canary did not complete the fixed 25-match/50-player cohort"
            )
        if metrics.get("endpoint_completeness") != 1:
            raise ProductionBudgetUnavailable(
                "incomplete endpoint walk cannot establish a paid proxy budget"
            )
        total = sample.get("total_provider_bytes")
        exit_hash = sample.get("proxy_exit_hash")
        endpoints = sample.get("endpoint_provider_bytes")
        request_bytes = sample.get("endpoint_request_provider_bytes")
        if isinstance(total, bool) or not isinstance(total, int) or total <= 0:
            raise ProductionBudgetUnavailable("canary totals must be positive integer bytes")
        if not isinstance(exit_hash, str) or len(exit_hash) < 12:
            raise ProductionBudgetUnavailable("canary proxy exits must be anonymized hashes")
        if not isinstance(endpoints, dict) or not endpoints:
            raise ProductionBudgetUnavailable("canary needs endpoint provider-byte metrics")
        if not isinstance(request_bytes, dict) or set(request_bytes) != set(endpoints):
            raise ProductionBudgetUnavailable(
                "canary needs per-request provider bytes for every endpoint"
            )
        endpoint_total = 0
        for endpoint, value in endpoints.items():
            if not isinstance(endpoint, str) or not endpoint:
                raise ProductionBudgetUnavailable("invalid endpoint in canary")
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProductionBudgetUnavailable("endpoint bytes must be non-negative integers")
            observations = request_bytes[endpoint]
            if (
                not isinstance(observations, list)
                or not observations
                or any(
                    isinstance(item, bool) or not isinstance(item, int) or item < 0
                    for item in observations
                )
                or sum(observations) != value
            ):
                raise ProductionBudgetUnavailable(
                    f"invalid per-request provider bytes for endpoint {endpoint!r}"
                )
            endpoint_run_maxima.setdefault(endpoint, []).append(max(observations))
            endpoint_sample_counts[endpoint] = endpoint_sample_counts.get(endpoint, 0) + 1
            endpoint_total += value
        if endpoint_total != total:
            raise ProductionBudgetUnavailable(
                f"endpoint bytes do not equal total for run {sample.get('run_id')!r}"
            )
        totals.append(total)
        exits.add(exit_hash)
    if len(exits) < MIN_DISTINCT_PROXY_EXITS:
        raise ProductionBudgetUnavailable(
            f"canary needs at least {MIN_DISTINCT_PROXY_EXITS} distinct proxy exits"
        )
    # Each endpoint must have observations for every logical run; otherwise a
    # missing expensive request could make its measured maximum artificially cheap.
    incomplete = sorted(
        name for name, count in endpoint_sample_counts.items()
        if count != len(budget_samples)
    )
    if incomplete:
        raise ProductionBudgetUnavailable(
            f"canary endpoint coverage is incomplete: {', '.join(incomplete)}"
        )
    artifact_id = hashlib.sha256(raw).hexdigest()
    return BudgetPolicy(
        artifact_id=artifact_id,
        hard_run_bytes=max(totals),
        endpoint_reservation_bytes={
            endpoint: max(run_maxima)
            for endpoint, run_maxima in sorted(endpoint_run_maxima.items())
        },
        sample_count=len(budget_samples),
        distinct_proxy_exits=len(exits),
    )


def load_verified_policy(
    path: os.PathLike[str] | str,
    *,
    workload_class: Optional[str] = None,
    allow_legacy_v1: bool = False,
) -> BudgetPolicy:
    """Load exactly one measured task budget; never aggregate v2 classes.

    ``workload_class`` is mandatory for schema v2.  The returned artifact ID
    binds both the parent artifact SHA-256 and the selected class, so a lease
    cannot be replayed under another task shape.  Schema v1 is accepted only
    behind an explicit compatibility flag and therefore cannot silently
    authorize the production capture path.
    """

    artifact_path = Path(path)
    try:
        raw = artifact_path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionBudgetUnavailable(f"invalid canary artifact: {path}") from exc
    if not isinstance(payload, Mapping):
        raise ProductionBudgetUnavailable("canary artifact must be an object")
    schema_version = payload.get("schema_version")
    if schema_version == CANARY_SCHEMA_VERSION:
        return _load_v2_class_policy(
            artifact_path,
            raw,
            payload,
            workload_class,
        )
    if schema_version == LEGACY_CANARY_SCHEMA_VERSION:
        if not allow_legacy_v1:
            raise ProductionBudgetUnavailable(
                "legacy v1 canary cannot authorize production; migrate to workload v2"
            )
        if workload_class is not None:
            raise ProductionBudgetUnavailable(
                "legacy v1 canary has no workload classes"
            )
        return _load_legacy_verified_policy(raw, payload)
    raise ProductionBudgetUnavailable("unsupported proxy canary schema")


class SharedBudgetLedger:
    """Atomic reservation and real-byte accounting for one or more DAG runs."""

    def __init__(self, path: os.PathLike[str] | str, policy: BudgetPolicy) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.policy = policy

    def _locked(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+")
        os.fchmod(handle.fileno(), 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def _read(self) -> dict:
        if not self.path.exists():
            return {"schema_version": LEDGER_SCHEMA_VERSION, "runs": {}}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise BudgetAccountingError(f"corrupt budget ledger: {self.path}") from exc
        if payload.get("schema_version") != LEDGER_SCHEMA_VERSION:
            raise BudgetAccountingError("unsupported budget ledger version")
        if not isinstance(payload.get("runs"), dict):
            raise BudgetAccountingError("invalid budget ledger")
        return payload

    def _write(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(
            f"{self.path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        )
        try:
            descriptor = os.open(
                temporary,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                    stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            except BaseException:
                # fdopen owns and closes the descriptor after successful wrap.
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                raise
            os.replace(temporary, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def _run(self, payload: dict, run_id: str) -> dict:
        run_id = str(run_id).strip()
        if not run_id:
            raise ValueError("run_id must not be empty")
        run = payload["runs"].setdefault(
            run_id,
            {
                "artifact_id": self.policy.artifact_id,
                "hard_run_bytes": self.policy.hard_run_bytes,
                "spent_provider_bytes": 0,
                "reservations": {},
                "updated_at": _utc_now(),
            },
        )
        if (
            run.get("artifact_id") != self.policy.artifact_id
            or run.get("hard_run_bytes") != self.policy.hard_run_bytes
        ):
            raise BudgetAccountingError(
                "logical run already exists with a different canary policy"
            )
        return run

    @staticmethod
    def _reservation_key(token: str) -> str:
        return hashlib.sha256(str(token).encode("utf-8")).hexdigest()

    def _reservation(self, run: dict, token: str) -> tuple[str, dict]:
        # New ledgers persist only a token hash.  The raw-key fallback reads
        # version-1 files created before token redaction without changing the
        # public SharedBudgetLedger API.
        hashed = self._reservation_key(token)
        if hashed in run["reservations"]:
            return hashed, run["reservations"][hashed]
        if token in run["reservations"]:
            return token, run["reservations"][token]
        raise BudgetAccountingError("unknown proxy budget reservation")

    def reserve(self, run_id: str, endpoint: str) -> tuple[str, int]:
        measured_max = self.policy.reservation_for(endpoint)
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            outstanding = sum(
                item["reserved_bytes"] - item["consumed_bytes"]
                for item in run["reservations"].values()
            )
            remaining = (
                run["hard_run_bytes"]
                - run["spent_provider_bytes"]
                - outstanding
            )
            amount = min(measured_max, remaining)
            if amount <= 0:
                raise ProxyBudgetExceeded(
                    f"budget exhausted before endpoint {endpoint!r}: "
                    f"spent={run['spent_provider_bytes']} reserved={outstanding} "
                    f"measured_max={measured_max} limit={run['hard_run_bytes']}"
                )
            token = uuid.uuid4().hex
            run["reservations"][self._reservation_key(token)] = {
                "endpoint": endpoint,
                "reserved_bytes": amount,
                "consumed_bytes": 0,
                "created_at": _utc_now(),
            }
            run["updated_at"] = _utc_now()
            self._write(payload)
            return token, amount
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def consume(self, run_id: str, token: str, provider_bytes: int) -> None:
        """Charge bytes measured at the residential-provider path."""
        if (
            isinstance(provider_bytes, bool)
            or not isinstance(provider_bytes, int)
            or provider_bytes < 0
        ):
            raise BudgetAccountingError("provider_bytes must be a non-negative integer")
        if provider_bytes == 0:
            return
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            _, reservation = self._reservation(run, token)
            if reservation["consumed_bytes"] + provider_bytes > reservation["reserved_bytes"]:
                raise ProxyBudgetExceeded(
                    "provider chunk would exceed the endpoint's measured maximum reservation"
                )
            if run["spent_provider_bytes"] + provider_bytes > run["hard_run_bytes"]:
                raise ProxyBudgetExceeded(
                    "provider chunk would exceed the logical DAG-run hard budget"
                )
            reservation["consumed_bytes"] += provider_bytes
            run["spent_provider_bytes"] += provider_bytes
            run["updated_at"] = _utc_now()
            self._write(payload)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def claim(self, run_id: str, token: str, max_provider_bytes: int) -> int:
        """Atomically reserve the next provider read before bytes can arrive.

        Both tunnel directions can be pumped concurrently. A separate
        ``remaining`` check would race: each side could observe the same final
        allowance and read beyond it. Claiming and charging the read window in
        one lock makes the hard boundary deterministic; ``refund`` returns any
        unused tail when the socket yields fewer bytes.
        """
        if (
            isinstance(max_provider_bytes, bool)
            or not isinstance(max_provider_bytes, int)
            or max_provider_bytes <= 0
        ):
            raise BudgetAccountingError(
                "max_provider_bytes must be a positive integer"
            )
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            _, reservation = self._reservation(run, token)
            endpoint_remaining = (
                reservation["reserved_bytes"] - reservation["consumed_bytes"]
            )
            run_remaining = run["hard_run_bytes"] - run["spent_provider_bytes"]
            claimed = min(max_provider_bytes, endpoint_remaining, run_remaining)
            if claimed <= 0:
                raise ProxyBudgetExceeded(
                    "provider budget exhausted before the next tunnel read"
                )
            reservation["consumed_bytes"] += claimed
            run["spent_provider_bytes"] += claimed
            run["updated_at"] = _utc_now()
            self._write(payload)
            return claimed
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def refund(self, run_id: str, token: str, provider_bytes: int) -> None:
        """Return the unused tail of a pre-charged provider read window."""
        if (
            isinstance(provider_bytes, bool)
            or not isinstance(provider_bytes, int)
            or provider_bytes < 0
        ):
            raise BudgetAccountingError(
                "provider_bytes must be a non-negative integer"
            )
        if provider_bytes == 0:
            return
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            _, reservation = self._reservation(run, token)
            if (
                provider_bytes > reservation["consumed_bytes"]
                or provider_bytes > run["spent_provider_bytes"]
            ):
                raise BudgetAccountingError(
                    "provider refund exceeds charged traffic"
                )
            reservation["consumed_bytes"] -= provider_bytes
            run["spent_provider_bytes"] -= provider_bytes
            run["updated_at"] = _utc_now()
            self._write(payload)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def finish(
        self,
        run_id: str,
        token: str,
        *,
        reported_provider_bytes: Optional[int] = None,
    ) -> int:
        """Close a reservation, optionally charging a transport meter once.

        Proxy-integrated transports call :meth:`consume` per chunk and then pass
        the same measured total here.  A simpler metered transport may skip
        chunk calls and provide its provider total at finish; unmetered paid
        transports are rejected by the capture engine.
        """
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            reservation_key, reservation = self._reservation(run, token)
            consumed = int(reservation["consumed_bytes"])
            if reported_provider_bytes is not None:
                if (
                    isinstance(reported_provider_bytes, bool)
                    or not isinstance(reported_provider_bytes, int)
                    or reported_provider_bytes < 0
                ):
                    raise BudgetAccountingError("invalid reported provider bytes")
                if consumed == 0 and reported_provider_bytes:
                    if reported_provider_bytes > reservation["reserved_bytes"]:
                        raise ProxyBudgetExceeded(
                            "reported traffic exceeds endpoint reservation"
                        )
                    if run["spent_provider_bytes"] + reported_provider_bytes > run["hard_run_bytes"]:
                        raise ProxyBudgetExceeded("reported traffic exceeds run budget")
                    reservation["consumed_bytes"] = reported_provider_bytes
                    run["spent_provider_bytes"] += reported_provider_bytes
                    consumed = reported_provider_bytes
                elif consumed != reported_provider_bytes:
                    raise BudgetAccountingError(
                        f"provider meter mismatch: proxy={consumed}, transport={reported_provider_bytes}"
                    )
            del run["reservations"][reservation_key]
            run["updated_at"] = _utc_now()
            self._write(payload)
            return consumed
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def cancel(self, run_id: str, token: str) -> None:
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            try:
                reservation_key, reservation = self._reservation(run, token)
            except BudgetAccountingError:
                return
            if reservation["consumed_bytes"]:
                raise BudgetAccountingError("cannot cancel a reservation after provider traffic")
            del run["reservations"][reservation_key]
            run["updated_at"] = _utc_now()
            self._write(payload)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def snapshot(self, run_id: str) -> dict:
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            return json.loads(json.dumps(run))
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()


def anonymize_proxy_exit(value: str) -> str:
    """Persist no provider IPs/credentials in benchmark artifacts."""
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("proxy exit must not be empty")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _valid_exit_hash(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _exact_request_map(
    sample: Mapping[str, object],
    *,
    required_endpoints: set[str],
) -> tuple[dict[str, list[int]], int]:
    request_bytes = sample.get("endpoint_request_provider_bytes")
    if not isinstance(request_bytes, Mapping) or set(request_bytes) != required_endpoints:
        raise ValueError("canary sample endpoint request map is not exact")
    normalized: dict[str, list[int]] = {}
    total = 0
    for endpoint in sorted(required_endpoints):
        values = request_bytes[endpoint]
        if (
            not isinstance(values, list)
            or not values
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                for value in values
            )
        ):
            raise ValueError(
                f"canary sample needs exact request bytes for {endpoint!r}"
            )
        normalized[endpoint] = list(values)
        total += sum(values)
    declared = sample.get("total_provider_bytes")
    if isinstance(declared, bool) or not isinstance(declared, int) or declared != total:
        raise ValueError("canary sample total does not equal its exact request map")
    endpoint_totals = sample.get("endpoint_provider_bytes")
    if endpoint_totals is not None and (
        not isinstance(endpoint_totals, Mapping)
        or set(endpoint_totals) != required_endpoints
        or any(endpoint_totals[name] != sum(normalized[name]) for name in normalized)
    ):
        raise ValueError("canary sample endpoint totals do not equal request bytes")
    return normalized, total


def _all_run_ids(payload: Mapping[str, object]) -> set[str]:
    values: set[str] = set()
    raw_classes = payload.get("workload_classes")
    if isinstance(raw_classes, Mapping):
        for raw_class in raw_classes.values():
            if not isinstance(raw_class, Mapping):
                continue
            for sample in raw_class.get("samples", []):
                if isinstance(sample, Mapping) and isinstance(sample.get("run_id"), str):
                    values.add(sample["run_id"])
    benchmark_samples = payload.get("benchmark_samples", [])
    if not isinstance(benchmark_samples, list):
        raise ValueError("benchmark_samples must be an array")
    for sample in benchmark_samples:
        if isinstance(sample, Mapping) and isinstance(sample.get("run_id"), str):
            values.add(sample["run_id"])
    return values


def _write_artifact_atomic(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f"{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    )
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _append_v2_sample(
    payload: dict,
    sample: Mapping[str, object],
    *,
    workload_class: Optional[str],
) -> None:
    run_id = str(sample.get("run_id", "")).strip()
    if not run_id or run_id in _all_run_ids(payload):
        raise ValueError("canary run_id must be non-empty and globally unique")
    name = str(workload_class or sample.get("workload_class") or "").strip()
    mode = str(sample.get("mode", "")).strip()
    if mode in {"no_op", "offline_replay", "single_endpoint_resume"}:
        if not name:
            raise ValueError("benchmark sample needs a workload_class")
        classes = payload.get("workload_classes")
        if not isinstance(classes, Mapping) or name not in classes:
            raise ValueError(f"unknown workload class {name!r}")
        if sample.get("budget_eligible") is not False:
            raise ValueError("benchmark samples cannot establish a budget")
        if mode in {"no_op", "offline_replay"}:
            zero_fields = (
                "total_provider_bytes",
                "lease_count",
                "network_request_count",
                "allocation_bytes",
            )
            if any(sample.get(field) != 0 for field in zero_fields):
                raise ValueError(
                    "network-free benchmark must have zero lease/network/allocation"
                )
            if sample.get("proxy_exit_hash") is not None:
                raise ValueError("network-free benchmark must not claim a proxy exit")
            if sample.get("endpoint_request_provider_bytes") != {}:
                raise ValueError("network-free benchmark must have no provider requests")
            if sample.get("endpoint_provider_bytes") not in (None, {}):
                raise ValueError("network-free benchmark must have zero endpoint bytes")
        else:
            raw_class = classes[name]
            required = set(raw_class.get("required_endpoints", []))
            request_map = sample.get("endpoint_request_provider_bytes")
            if (
                not _valid_exit_hash(sample.get("proxy_exit_hash"))
                or not isinstance(request_map, Mapping)
                or len(request_map) != 1
                or not set(request_map).issubset(required)
                or sample.get("lease_count") != 1
                or sample.get("network_request_count") != 1
                or not isinstance(sample.get("allocation_bytes"), int)
                or sample.get("allocation_bytes", 0) <= 0
            ):
                raise ValueError("resume benchmark is not one exact paid request")
            _, total = _exact_request_map(
                sample,
                required_endpoints=set(request_map),
            )
            if total <= 0:
                raise ValueError("resume benchmark provider bytes must be positive")
        payload.setdefault("benchmark_samples", []).append(dict(sample))
        return

    if mode != "cold" or sample.get("budget_eligible") is not True:
        raise ValueError("workload budget samples must be eligible cold observations")
    classes = payload.get("workload_classes")
    raw_class = classes.get(name) if isinstance(classes, Mapping) else None
    if not isinstance(raw_class, dict):
        raise ValueError(f"unknown workload class {name!r}")
    if sample.get("workload_class") not in {None, name}:
        raise ValueError("sample workload_class does not match its destination")
    if sample.get("units") != raw_class.get("max_units"):
        raise ValueError("cold sample units must equal the class max_units")
    if not _valid_exit_hash(sample.get("proxy_exit_hash")):
        raise ValueError("cold sample needs a SHA-256 proxy exit hash")
    required = raw_class.get("required_endpoints")
    if not isinstance(required, list) or not required:
        raise ValueError("workload class has no required endpoints")
    observed = sample.get("endpoint_request_provider_bytes")
    expected = set(required)
    if raw_class.get("scope") == "season" and isinstance(observed, Mapping):
        # Referee IDs are not guaranteed in season schedule payloads.  The
        # endpoint remains measured whenever planned, while its absence must
        # not cause a fake request.  All other season endpoints stay exact.
        actual = set(observed)
        if not expected - {"referee_profile"} <= actual <= expected:
            raise ValueError("canary sample endpoint request map is not exact")
        expected = actual
    _, total = _exact_request_map(sample, required_endpoints=expected)
    if total <= 0:
        raise ValueError("cold sample provider bytes must be positive")
    normalized = dict(sample)
    normalized["workload_class"] = name
    raw_class.setdefault("samples", []).append(normalized)
    raw_class["hard_task_bytes"] = max(
        item["total_provider_bytes"] for item in raw_class["samples"]
    )


def append_canary_sample(
    path: os.PathLike[str] | str,
    sample: Mapping[str, object],
    *,
    workload_class: Optional[str] = None,
) -> None:
    """Atomically append one class observation and keep it unverified."""
    artifact_path = Path(path)
    lock_path = artifact_path.with_suffix(artifact_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            if payload.get("schema_version") != CANARY_SCHEMA_VERSION:
                raise ValueError("unsupported canary artifact")
            if payload.get("budget_derivation") != BUDGET_DERIVATION:
                raise ValueError("unsupported canary budget_derivation")
            if payload.get("verified") is True:
                raise ValueError("verified canary artifact is immutable")
            _append_v2_sample(
                payload,
                sample,
                workload_class=workload_class,
            )
            payload["verified"] = False
            payload["updated_at"] = _utc_now()
            _write_artifact_atomic(artifact_path, payload)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
