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
import math
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional


CANARY_SCHEMA_VERSION = 1
LEDGER_SCHEMA_VERSION = 1
MIN_CANARY_RUNS = 20
MIN_DISTINCT_PROXY_EXITS = 5
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _nearest_rank_p95(values: list[int]) -> int:
    if not values:
        raise ProductionBudgetUnavailable("canary has no provider-byte samples")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


@dataclass(frozen=True)
class BudgetPolicy:
    artifact_id: str
    hard_run_bytes: int
    endpoint_reservation_bytes: Mapping[str, int]
    sample_count: int
    distinct_proxy_exits: int

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


def load_verified_policy(path: os.PathLike[str] | str) -> BudgetPolicy:
    """Derive the hard ceiling and endpoint reservations from measured p95s.

    No multiplier or hand-written MB threshold is accepted: the logical-run
    ceiling is the nearest-rank p95 of the checked-in provider byte totals and
    every endpoint reservation is its own nearest-rank p95.
    """
    artifact_path = Path(path)
    try:
        raw = artifact_path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionBudgetUnavailable(f"invalid canary artifact: {path}") from exc
    if payload.get("schema_version") != CANARY_SCHEMA_VERSION:
        raise ProductionBudgetUnavailable("unsupported proxy canary schema")
    if payload.get("source") != "sofascore":
        raise ProductionBudgetUnavailable("proxy canary source must be sofascore")
    if payload.get("meter") != "proxy_filter_provider_path_v2":
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
    endpoint_values: dict[str, list[int]] = {}
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
        if not isinstance(total, int) or total <= 0:
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
            if not isinstance(value, int) or value < 0:
                raise ProductionBudgetUnavailable("endpoint bytes must be non-negative integers")
            observations = request_bytes[endpoint]
            if (
                not isinstance(observations, list)
                or not observations
                or any(not isinstance(item, int) or item < 0 for item in observations)
                or sum(observations) != value
            ):
                raise ProductionBudgetUnavailable(
                    f"invalid per-request provider bytes for endpoint {endpoint!r}"
                )
            endpoint_values.setdefault(endpoint, []).extend(observations)
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
    # missing expensive request could make its p95 look artificially cheap.
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
        hard_run_bytes=_nearest_rank_p95(totals),
        endpoint_reservation_bytes={
            endpoint: _nearest_rank_p95(values)
            for endpoint, values in sorted(endpoint_values.items())
        },
        sample_count=len(budget_samples),
        distinct_proxy_exits=len(exits),
    )


class SharedBudgetLedger:
    """Atomic reservation and real-byte accounting for one or more DAG runs."""

    def __init__(self, path: os.PathLike[str] | str, policy: BudgetPolicy) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.policy = policy

    def _locked(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+")
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
            temporary.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, self.path)
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

    def reserve(self, run_id: str, endpoint: str) -> tuple[str, int]:
        amount = self.policy.reservation_for(endpoint)
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            outstanding = sum(
                item["reserved_bytes"] - item["consumed_bytes"]
                for item in run["reservations"].values()
            )
            if run["spent_provider_bytes"] + outstanding + amount > run["hard_run_bytes"]:
                raise ProxyBudgetExceeded(
                    f"budget exhausted before endpoint {endpoint!r}: "
                    f"spent={run['spent_provider_bytes']} reserved={outstanding} "
                    f"next={amount} limit={run['hard_run_bytes']}"
                )
            token = uuid.uuid4().hex
            run["reservations"][token] = {
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
        if not isinstance(provider_bytes, int) or provider_bytes < 0:
            raise BudgetAccountingError("provider_bytes must be a non-negative integer")
        if provider_bytes == 0:
            return
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, run_id)
            try:
                reservation = run["reservations"][token]
            except KeyError as exc:
                raise BudgetAccountingError("unknown proxy budget reservation") from exc
            if reservation["consumed_bytes"] + provider_bytes > reservation["reserved_bytes"]:
                raise ProxyBudgetExceeded(
                    "provider chunk would exceed the endpoint's measured p95 reservation"
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
            try:
                reservation = run["reservations"][token]
            except KeyError as exc:
                raise BudgetAccountingError(
                    "unknown proxy budget reservation"
                ) from exc
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
            try:
                reservation = run["reservations"][token]
            except KeyError as exc:
                raise BudgetAccountingError(
                    "unknown proxy budget reservation"
                ) from exc
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
            try:
                reservation = run["reservations"][token]
            except KeyError as exc:
                raise BudgetAccountingError("unknown proxy budget reservation") from exc
            consumed = int(reservation["consumed_bytes"])
            if reported_provider_bytes is not None:
                if not isinstance(reported_provider_bytes, int) or reported_provider_bytes < 0:
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
            del run["reservations"][token]
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
            reservation = run["reservations"].get(token)
            if reservation is None:
                return
            if reservation["consumed_bytes"]:
                raise BudgetAccountingError("cannot cancel a reservation after provider traffic")
            del run["reservations"][token]
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


def append_canary_sample(path: os.PathLike[str] | str, sample: Mapping[str, object]) -> None:
    """Atomically append one provider-metered logical-run observation."""
    artifact_path = Path(path)
    lock_path = artifact_path.with_suffix(artifact_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            if payload.get("schema_version") != CANARY_SCHEMA_VERSION:
                raise ValueError("unsupported canary artifact")
            samples = payload.setdefault("samples", [])
            run_id = str(sample.get("run_id", "")).strip()
            if not run_id or any(item.get("run_id") == run_id for item in samples):
                raise ValueError("canary run_id must be non-empty and unique")
            if not isinstance(sample.get("budget_eligible"), bool):
                raise ValueError("canary sample needs explicit budget_eligible")
            if not str(sample.get("cohort", "")).strip():
                raise ValueError("canary sample needs a cohort")
            if not str(sample.get("mode", "")).strip():
                raise ValueError("canary sample needs a mode")
            if sample["budget_eligible"] and (
                sample["cohort"] != REQUIRED_BUDGET_COHORT
                or sample["mode"] != REQUIRED_BUDGET_MODE
            ):
                raise ValueError(
                    "budget-eligible samples must use the fixed cold canary cohort"
                )
            if len(str(sample.get("proxy_exit_hash", ""))) < 12:
                raise ValueError("canary sample needs an anonymized proxy exit hash")
            endpoints = sample.get("endpoint_provider_bytes")
            request_bytes = sample.get("endpoint_request_provider_bytes")
            total = sample.get("total_provider_bytes")
            if (
                not isinstance(endpoints, dict)
                or not endpoints
                or not isinstance(request_bytes, dict)
                or set(request_bytes) != set(endpoints)
                or not isinstance(total, int)
                or sum(endpoints.values()) != total
            ):
                raise ValueError("canary sample needs exact endpoint/provider totals")
            if any(
                not isinstance(values, list)
                or sum(values) != endpoints[endpoint]
                or any(not isinstance(value, int) or value < 0 for value in values)
                for endpoint, values in request_bytes.items()
            ):
                raise ValueError("canary sample needs exact per-request provider bytes")
            samples.append(dict(sample))
            # Collection never silently self-approves production. Reviewers set
            # verified=true only after cohort/provenance inspection.
            payload["verified"] = False
            payload["updated_at"] = _utc_now()
            temporary = artifact_path.with_name(
                f"{artifact_path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
            )
            try:
                temporary.write_text(
                    json.dumps(payload, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                os.replace(temporary, artifact_path)
            finally:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
