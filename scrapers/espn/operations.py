"""Durable orchestration contracts for ESPN native Raw/Bronze ingestion.

The module is deliberately importable without Airflow.  DAG callables use the
same pure contracts as unit tests; production persistence adapters live below
the in-memory reference implementation.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
import threading
import time
from typing import (
    Any,
    Callable,
    ContextManager,
    Iterable,
    Iterator,
    Mapping,
    Protocol,
    Sequence,
)

from .selection import current_manifest_order_key
from .transport_contracts import (
    ESPN_SITE_API_CAPTURE_ORIGINS,
    ESPN_SITE_API_PRIMARY_ORIGIN,
    normalize_transport_origin,
)


_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_SCOPE_RE = re.compile(r"[1-9][0-9]*:[1-9][0-9]*")
_MODES = frozenset({"daily", "repair", "backfill", "replay"})
SUMMARY_BATCH_SIZE = 50
MAX_SUMMARY_EVENTS = 100
# A scope can contain a full season, while each mapped network task remains
# capped at 50. This guards fan-out without reusing Task 5's per-execution cap.
MAX_SCOPE_SUMMARY_EVENTS = 1000


class OperationsError(RuntimeError):
    """Base error for orchestration contract failures."""


class LeaseConflict(OperationsError):
    """A live scope lease belongs to another immutable run identity."""


class LeaseLost(OperationsError):
    """A worker no longer owns the exact lease epoch it was issued."""


def producer_state_failures(
    states: Mapping[str, Sequence[str]],
    expected_counts: Mapping[str, int],
) -> tuple[str, ...]:
    """Evaluate exact mapped-task cardinality, including Airflow zero maps."""

    if set(states) != set(expected_counts):
        return ("producer task state set differs from signed expectation",)
    failures: list[str] = []
    for task_id in sorted(expected_counts):
        expected = expected_counts[task_id]
        if type(expected) is not int or expected < 0:
            raise ValueError("producer expected count must be non-negative")
        observed = tuple(str(item).casefold() for item in states[task_id])
        if expected == 0:
            if any(item != "skipped" for item in observed):
                failures.append(f"{task_id}: zero-map state is not skipped")
            continue
        if len(observed) != expected or any(item != "success" for item in observed):
            failures.append(
                f"{task_id}: expected {expected} success states, got {list(observed)!r}"
            )
    return tuple(failures)


def evaluate_alerts(
    snapshot: Mapping[str, Any], *, observed_at: datetime
) -> tuple[dict[str, Any], ...]:
    """Return deterministic structured hard/warning ESPN alert contracts."""

    now = _utc(observed_at, "observed_at")
    common = {
        "run_id",
        "attempt",
        "scope_id",
        "state",
        "last_complete_at",
        "direct_requests",
        "request_budget",
        "proxy_bytes",
        "lease_conflict",
        "unpromoted_current_season",
        "unresolved_discovery_diffs",
    }
    typed_identity = {
        "dag_id",
        "subject_dag_id",
        "subject_run_id",
        "identity_kind",
        "identity_sha256",
    }
    signed_plan_identity = {"plan_signature", "registry_signature"}
    extended_signed_plan = signed_plan_identity | {
        "dag_id",
        "subject_dag_id",
        "subject_run_id",
    }
    actual_keys = set(snapshot)
    if actual_keys == common | typed_identity:
        identity_sha256 = _signature(snapshot["identity_sha256"], "identity_sha256")
        if identity_sha256 == "0" * 64:
            raise ValueError("alert identity must not use a sentinel hash")
        identity = {
            "dag_id": _required(snapshot["dag_id"], "dag_id"),
            "run_id": _required(snapshot["run_id"], "run_id"),
            "attempt": _positive_int(snapshot["attempt"], "attempt"),
            "scope_id": (
                None
                if snapshot["scope_id"] is None
                else _scope_id(snapshot["scope_id"])
            ),
            "subject_dag_id": (
                None
                if snapshot["subject_dag_id"] is None
                else _required(snapshot["subject_dag_id"], "subject_dag_id")
            ),
            "subject_run_id": (
                None
                if snapshot["subject_run_id"] is None
                else _required(snapshot["subject_run_id"], "subject_run_id")
            ),
            "identity_kind": _required(snapshot["identity_kind"], "identity_kind"),
            "identity_sha256": identity_sha256,
            "observed_at": now.isoformat(),
        }
    elif (
        actual_keys == common | signed_plan_identity
        or actual_keys == common | extended_signed_plan
    ):
        plan_signature = _signature(snapshot["plan_signature"], "plan_signature")
        registry_signature = _signature(
            snapshot["registry_signature"], "registry_signature"
        )
        if plan_signature == "0" * 64 or registry_signature == "0" * 64:
            raise ValueError("alert plan identities must not use sentinel hashes")
        identity = {
            "run_id": _required(snapshot["run_id"], "run_id"),
            "attempt": _positive_int(snapshot["attempt"], "attempt"),
            "scope_id": (
                None
                if snapshot["scope_id"] is None
                else _scope_id(snapshot["scope_id"])
            ),
            "identity_kind": "scope-plan",
            "identity_sha256": plan_signature,
            "plan_signature": plan_signature,
            "registry_signature": registry_signature,
            "observed_at": now.isoformat(),
        }
        if actual_keys == common | extended_signed_plan:
            identity.update(
                {
                    "dag_id": _required(snapshot["dag_id"], "dag_id"),
                    "subject_dag_id": (
                        None
                        if snapshot["subject_dag_id"] is None
                        else _required(snapshot["subject_dag_id"], "subject_dag_id")
                    ),
                    "subject_run_id": (
                        None
                        if snapshot["subject_run_id"] is None
                        else _required(snapshot["subject_run_id"], "subject_run_id")
                    ),
                }
            )
    else:
        raise OperationsError("alert snapshot identity fields mismatch")
    conditions: list[tuple[str, str, Mapping[str, Any]]] = []
    state = _required(snapshot["state"], "state")
    if state in {"incomplete", "schema_drift", "failed"}:
        conditions.append(("hard", state, {"state": state}))
    last_complete = snapshot["last_complete_at"]
    if last_complete is None:
        conditions.append(("hard", "freshness_36h", {"age_hours": None}))
    else:
        completed_at = _utc(last_complete, "last_complete_at")
        age_hours = max(0.0, (now - completed_at).total_seconds() / 3600)
        if age_hours >= 36:
            conditions.append(("hard", "freshness_36h", {"age_hours": age_hours}))
    direct = int(snapshot["direct_requests"])
    budget = int(snapshot["request_budget"])
    if direct < 0 or budget <= 0:
        raise ValueError("request budget counters are invalid")
    ratio = direct / budget
    if ratio >= 1:
        conditions.append(("hard", "request_budget_100", {"ratio": ratio}))
    elif ratio >= 0.8:
        conditions.append(("warning", "request_budget_80", {"ratio": ratio}))
    if int(snapshot["proxy_bytes"]) > 0:
        conditions.append(
            ("hard", "proxy_usage", {"proxy_bytes": int(snapshot["proxy_bytes"])})
        )
    if bool(snapshot["lease_conflict"]):
        conditions.append(("hard", "lease_conflict", {}))
    if bool(snapshot["unpromoted_current_season"]):
        conditions.append(("hard", "unpromoted_current_season", {}))
    if bool(snapshot["unresolved_discovery_diffs"]):
        conditions.append(("warning", "unresolved_discovery_diffs", {}))
    alerts = []
    for severity, code, details in conditions:
        base = {
            "kind": "espn-alert-v1",
            "severity": severity,
            "code": code,
            **identity,
            "details": dict(details),
        }
        alerts.append(
            {
                **base,
                "alert_sha256": hashlib.sha256(_canonical_bytes(base)).hexdigest(),
            }
        )
    return tuple(sorted(alerts, key=lambda item: (item["severity"], item["code"])))


def _required(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _scope_id(value: object) -> str:
    scope_id = _required(value, "scope_id")
    if _SCOPE_RE.fullmatch(scope_id) is None:
        raise ValueError("scope_id must be '<espn_id>:<source_season_year>'")
    return scope_id


def _signature(value: object, field: str) -> str:
    signature = _required(value, field)
    if _SHA256_RE.fullmatch(signature) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return signature


def _utc(value: object, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise TypeError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _ttl(value: object) -> timedelta:
    if not isinstance(value, timedelta):
        raise TypeError("ttl must be timedelta")
    if value <= timedelta(0) or value > timedelta(hours=24):
        raise ValueError("ttl must be positive and at most 24 hours")
    return value


def _positive_int(value: object, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode()


def _exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise OperationsError(
            f"{field} fields mismatch: expected={sorted(expected)!r}, "
            f"actual={sorted(value)!r}"
        )


def seal_raw_batch_descriptor(
    *,
    endpoint: str,
    run_id: str,
    attempt: int,
    scope_id: str,
    plan_signature: str,
    batch_id: str,
    request_ids: Sequence[str],
    event_ids: Sequence[int],
) -> dict[str, Any]:
    """Sign the exact request set one mapped network task must produce."""

    if endpoint not in {"scoreboard", "summary"}:
        raise ValueError("endpoint must be scoreboard or summary")
    supplied_requests = tuple(_required(item, "request_id") for item in request_ids)
    if not supplied_requests or len(supplied_requests) != len(set(supplied_requests)):
        raise ValueError("descriptor request IDs must be non-empty and unique")
    events = tuple(sorted(_positive_int(item, "event_id") for item in event_ids))
    if len(events) != len(set(events)):
        raise ValueError("descriptor event IDs must be unique")
    if endpoint == "scoreboard" and events:
        raise ValueError("scoreboard descriptor event IDs must be empty")
    if endpoint == "summary":
        if len(events) > SUMMARY_BATCH_SIZE:
            raise ValueError("Summary descriptor cannot exceed 50 events")
        expected_requests = tuple(f"summary:{event_id}" for event_id in events)
        if set(supplied_requests) != set(expected_requests):
            raise ValueError("Summary descriptor request/event identity mismatch")
        requests = expected_requests
    else:
        requests = tuple(sorted(supplied_requests))
    base = {
        "kind": "espn-raw-batch-descriptor-v1",
        "schema_version": 1,
        "endpoint": endpoint,
        "run_id": _required(run_id, "run_id"),
        "attempt": _positive_int(attempt, "attempt"),
        "scope_id": _scope_id(scope_id),
        "plan_signature": _signature(plan_signature, "plan_signature"),
        "batch_id": _required(batch_id, "batch_id"),
        "request_ids": list(requests),
        "event_ids": list(events),
    }
    return {
        **base,
        "descriptor_sha256": hashlib.sha256(_canonical_bytes(base)).hexdigest(),
    }


def _validated_batch_descriptor(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise OperationsError("raw batch descriptor must be a mapping")
    descriptor = dict(value)
    _exact_keys(
        descriptor,
        {
            "kind",
            "schema_version",
            "endpoint",
            "run_id",
            "attempt",
            "scope_id",
            "plan_signature",
            "batch_id",
            "request_ids",
            "event_ids",
            "descriptor_sha256",
        },
        "raw batch descriptor",
    )
    expected_hash = _signature(descriptor["descriptor_sha256"], "descriptor_sha256")
    base = {key: item for key, item in descriptor.items() if key != "descriptor_sha256"}
    if hashlib.sha256(_canonical_bytes(base)).hexdigest() != expected_hash:
        raise OperationsError("raw batch descriptor signature mismatch")
    rebuilt = seal_raw_batch_descriptor(
        endpoint=descriptor["endpoint"],
        run_id=descriptor["run_id"],
        attempt=descriptor["attempt"],
        scope_id=descriptor["scope_id"],
        plan_signature=descriptor["plan_signature"],
        batch_id=descriptor["batch_id"],
        request_ids=descriptor["request_ids"],
        event_ids=descriptor["event_ids"],
    )
    if rebuilt != descriptor:
        raise OperationsError("raw batch descriptor canonical form mismatch")
    return descriptor


def plan_summary_batches(
    event_ids: Iterable[int],
    *,
    run_id: str,
    attempt: int,
    scope_id: str,
    plan_signature: str,
    max_events: int = MAX_SUMMARY_EVENTS,
) -> dict[str, Any]:
    """Create bounded, deterministic URI-safe descriptors for wave two."""

    run = _required(run_id, "run_id")
    attempt_value = _positive_int(attempt, "attempt")
    scope = _scope_id(scope_id)
    signature = _signature(plan_signature, "plan_signature")
    if type(max_events) is not int or not 1 <= max_events <= MAX_SCOPE_SUMMARY_EVENTS:
        raise ValueError(f"max_events must be between 1 and {MAX_SCOPE_SUMMARY_EVENTS}")
    normalized = tuple(sorted(_positive_int(item, "event_id") for item in event_ids))
    if len(normalized) != len(set(normalized)):
        raise ValueError("Summary event IDs must be unique")
    admitted = normalized[:max_events]
    pending = normalized[max_events:]
    batches = []
    for offset in range(0, len(admitted), SUMMARY_BATCH_SIZE):
        chunk = admitted[offset : offset + SUMMARY_BATCH_SIZE]
        identity = "\x00".join(
            (
                "espn-summary-batch-v1",
                run,
                str(attempt_value),
                scope,
                signature,
                str(offset // SUMMARY_BATCH_SIZE + 1),
                ",".join(str(item) for item in chunk),
            )
        )
        batch_id = hashlib.sha256(identity.encode()).hexdigest()
        batches.append(
            seal_raw_batch_descriptor(
                endpoint="summary",
                run_id=run,
                attempt=attempt_value,
                scope_id=scope,
                plan_signature=signature,
                batch_id=batch_id,
                request_ids=tuple(f"summary:{event_id}" for event_id in chunk),
                event_ids=chunk,
            )
        )
    return {
        "batches": batches,
        "budget_used": len(admitted),
        "budget_limit": max_events,
        "budget_exhausted": bool(pending),
        "pending_event_ids": list(pending),
    }


def _validate_raw_request(value: object, *, schema_version: int) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise OperationsError("raw request must be a mapping")
    request = dict(value)
    required = {
        "request_id",
        "scope_id",
        "endpoint",
        "event_id",
        "url_fingerprint",
        "raw_uri",
        "raw_sha256",
        "fetched_at",
        "http_status",
        "direct_bytes",
        "proxy_bytes",
        "query_start",
        "query_end",
    }
    expected = required if schema_version == 1 else {*required, "transport_origin"}
    if (
        type(schema_version) is not int
        or schema_version not in {1, 2}
        or set(request) != expected
    ):
        raise OperationsError(
            f"raw request fields mismatch for checkpoint v{schema_version}"
        )
    _required(request["request_id"], "request_id")
    _scope_id(request["scope_id"])
    if request["endpoint"] not in {"scoreboard", "summary"}:
        raise OperationsError("raw request endpoint is invalid")
    if request["endpoint"] == "summary":
        _positive_int(request["event_id"], "event_id")
        if request["query_start"] is not None or request["query_end"] is not None:
            raise OperationsError("Summary raw request cannot contain query dates")
    else:
        if request["event_id"] is not None:
            raise OperationsError("scoreboard raw request event_id must be null")
        for field in ("query_start", "query_end"):
            try:
                datetime.strptime(_required(request[field], field), "%Y-%m-%d")
            except ValueError as exc:
                raise OperationsError(f"{field} must be an ISO date") from exc
    _signature(request["url_fingerprint"], "url_fingerprint")
    _required(request["raw_uri"], "raw_uri")
    _signature(request["raw_sha256"], "raw_sha256")
    _required(request["fetched_at"], "fetched_at")
    if (
        type(request["http_status"]) is not int
        or not 200 <= request["http_status"] <= 299
    ):
        raise OperationsError("raw request status must be successful")
    for field in ("direct_bytes", "proxy_bytes"):
        if type(request[field]) is not int or request[field] < 0:
            raise OperationsError(f"raw request {field} is invalid")
    if request["proxy_bytes"] != 0:
        raise OperationsError("ESPN raw request contains proxy traffic")
    if schema_version == 2:
        try:
            origin = normalize_transport_origin(request["transport_origin"])
        except ValueError as exc:
            raise OperationsError("raw request transport_origin is invalid") from exc
        if (
            origin != request["transport_origin"]
            or origin not in ESPN_SITE_API_CAPTURE_ORIGINS
        ):
            raise OperationsError("raw request transport_origin is invalid")
    return request


def seal_raw_checkpoint(
    *,
    endpoint: str,
    run_id: str,
    attempt: int,
    scope_id: str,
    plan_signature: str,
    batch_id: str,
    requests: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Seal one immutable mapped network result; never a shared mutable file."""

    if endpoint not in {"scoreboard", "summary"}:
        raise ValueError("endpoint must be scoreboard or summary")
    if not requests:
        raise ValueError("raw checkpoint requests must be non-empty")
    versions = {
        2 if isinstance(item, Mapping) and "transport_origin" in item else 1
        for item in requests
    }
    schema_version = max(versions)
    normalized_inputs = tuple(
        (
            dict(item)
            if schema_version == 1 or "transport_origin" in item
            else {**item, "transport_origin": ESPN_SITE_API_PRIMARY_ORIGIN}
        )
        if isinstance(item, Mapping)
        else item
        for item in requests
    )
    normalized = tuple(
        _validate_raw_request(item, schema_version=schema_version)
        for item in normalized_inputs
    )
    if endpoint == "summary" and len(normalized) > SUMMARY_BATCH_SIZE:
        raise ValueError("Summary checkpoint cannot exceed 50 requests")
    if any(item["endpoint"] != endpoint for item in normalized):
        raise OperationsError("checkpoint endpoint identity drift")
    scope = _scope_id(scope_id)
    if any(item["scope_id"] != scope for item in normalized):
        raise OperationsError("checkpoint scope identity drift")
    request_ids = [item["request_id"] for item in normalized]
    if len(request_ids) != len(set(request_ids)):
        raise OperationsError("checkpoint request IDs must be unique")
    base = {
        "kind": f"espn-raw-batch-checkpoint-v{schema_version}",
        "schema_version": schema_version,
        "endpoint": endpoint,
        "run_id": _required(run_id, "run_id"),
        "attempt": _positive_int(attempt, "attempt"),
        "scope_id": scope,
        "plan_signature": _signature(plan_signature, "plan_signature"),
        "batch_id": _required(batch_id, "batch_id"),
        "requests": sorted(normalized, key=lambda item: item["request_id"]),
    }
    return {
        **base,
        "checkpoint_sha256": hashlib.sha256(_canonical_bytes(base)).hexdigest(),
    }


def _validated_checkpoint(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise OperationsError("raw checkpoint must be a mapping")
    checkpoint = dict(value)
    _exact_keys(
        checkpoint,
        {
            "kind",
            "schema_version",
            "endpoint",
            "run_id",
            "attempt",
            "scope_id",
            "plan_signature",
            "batch_id",
            "requests",
            "checkpoint_sha256",
        },
        "raw checkpoint",
    )
    expected_hash = _signature(checkpoint["checkpoint_sha256"], "checkpoint_sha256")
    if type(checkpoint["schema_version"]) is not int or checkpoint[
        "schema_version"
    ] not in {1, 2}:
        raise OperationsError("raw checkpoint schema version is unsupported")
    base = {key: item for key, item in checkpoint.items() if key != "checkpoint_sha256"}
    if hashlib.sha256(_canonical_bytes(base)).hexdigest() != expected_hash:
        raise OperationsError("raw checkpoint signature mismatch")
    rebuilt = seal_raw_checkpoint(
        endpoint=checkpoint["endpoint"],
        run_id=checkpoint["run_id"],
        attempt=checkpoint["attempt"],
        scope_id=checkpoint["scope_id"],
        plan_signature=checkpoint["plan_signature"],
        batch_id=checkpoint["batch_id"],
        requests=checkpoint["requests"],
    )
    if rebuilt != checkpoint:
        raise OperationsError("raw checkpoint canonical form mismatch")
    return checkpoint


def validate_raw_checkpoint(value: object) -> dict[str, Any]:
    """Public fail-closed validator used to resume an exact mapped batch."""

    return _validated_checkpoint(value)


def reduce_raw_checkpoints(
    *,
    run_id: str,
    attempt: int,
    mode: str,
    as_of: str,
    registry_signature: str,
    plan_signature: str,
    selected_scopes: Sequence[str],
    expected_batches: Sequence[Mapping[str, Any]],
    checkpoints: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reduce immutable mapped checkpoints into Task 5's exact raw manifest."""

    run = _required(run_id, "run_id")
    attempt_value = _positive_int(attempt, "attempt")
    if mode not in _MODES:
        raise ValueError("mode is invalid")
    try:
        datetime.strptime(_required(as_of, "as_of"), "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("as_of must be an ISO date") from exc
    registry = _signature(registry_signature, "registry_signature")
    signature = _signature(plan_signature, "plan_signature")
    scopes = tuple(sorted(_scope_id(item) for item in selected_scopes))
    if not scopes or len(scopes) != len(set(scopes)):
        raise ValueError("selected scopes must be non-empty and unique")
    normalized = tuple(_validated_checkpoint(item) for item in checkpoints)
    expected_descriptors = tuple(
        _validated_batch_descriptor(item) for item in expected_batches
    )
    expected_by_identity = {
        (item["endpoint"], item["batch_id"]): item for item in expected_descriptors
    }
    if not expected_descriptors or len(expected_by_identity) != len(
        expected_descriptors
    ):
        raise OperationsError("expected raw batches must be non-empty and unique")
    identities = {(item["endpoint"], item["batch_id"]) for item in normalized}
    if len(identities) != len(normalized):
        raise OperationsError("duplicate raw checkpoint batch identity")
    if identities != set(expected_by_identity):
        raise OperationsError(
            "raw checkpoints must exactly match signed expected batches"
        )
    for item in normalized:
        actual = (
            item["run_id"],
            item["attempt"],
            item["plan_signature"],
            item["scope_id"],
        )
        expected = (run, attempt_value, signature, item["scope_id"])
        if actual != expected or item["scope_id"] not in scopes:
            raise OperationsError("raw checkpoint identity mismatch")
        descriptor = expected_by_identity[(item["endpoint"], item["batch_id"])]
        descriptor_identity = (
            descriptor["run_id"],
            descriptor["attempt"],
            descriptor["plan_signature"],
            descriptor["scope_id"],
        )
        if descriptor_identity != actual:
            raise OperationsError("raw batch descriptor identity mismatch")
        actual_requests = sorted(request["request_id"] for request in item["requests"])
        if actual_requests != sorted(descriptor["request_ids"]):
            raise OperationsError("raw checkpoint request set mismatch")
        actual_events = sorted(
            request["event_id"]
            for request in item["requests"]
            if request["event_id"] is not None
        )
        if actual_events != descriptor["event_ids"]:
            raise OperationsError("raw checkpoint event set mismatch")
    ordered = sorted(
        normalized,
        key=lambda item: (
            0 if item["endpoint"] == "scoreboard" else 1,
            item["scope_id"],
            item["batch_id"],
        ),
    )
    seen_requests: set[str] = set()
    manifest_checkpoints = []
    for index, item in enumerate(ordered, start=1):
        request_ids = {request["request_id"] for request in item["requests"]}
        if seen_requests & request_ids:
            raise OperationsError("duplicate raw request across checkpoints")
        seen_requests.update(request_ids)
        normalized_requests = [
            (
                dict(request)
                if item["schema_version"] == 2
                else {
                    **request,
                    "transport_origin": ESPN_SITE_API_PRIMARY_ORIGIN,
                }
            )
            for request in item["requests"]
        ]
        manifest_checkpoints.append(
            {
                "checkpoint_id": index,
                "scope_id": item["scope_id"],
                "endpoint": item["endpoint"],
                "requests": normalized_requests,
            }
        )
    base = {
        "kind": "espn-raw-run-manifest-v2",
        "schema_version": 2,
        "run_id": run,
        "attempt": attempt_value,
        "mode": mode,
        "as_of": as_of,
        "registry_signature": registry,
        "plan_signature": signature,
        "selected_scopes": list(scopes),
        "checkpoints": manifest_checkpoints,
    }
    return {
        **base,
        "manifest_sha256": hashlib.sha256(_canonical_bytes(base)).hexdigest(),
    }


@dataclass(frozen=True, slots=True)
class ScopeLease:
    scope_id: str
    owner_id: str
    plan_signature: str
    epoch: int
    token_sha256: str
    acquired_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        _scope_id(self.scope_id)
        _required(self.owner_id, "owner_id")
        _signature(self.plan_signature, "plan_signature")
        if type(self.epoch) is not int or self.epoch <= 0:
            raise ValueError("lease epoch must be a positive integer")
        _signature(self.token_sha256, "token_sha256")
        acquired = _utc(self.acquired_at, "acquired_at")
        expires = _utc(self.expires_at, "expires_at")
        if expires <= acquired:
            raise ValueError("lease expiry must follow acquisition")


@dataclass(frozen=True, slots=True)
class ScopeHead:
    dag_id: str
    scope_id: str
    generation_id: str
    generation_signature: str
    manifest_sha256: str
    snapshot_uri: str
    snapshot_sha256: str
    registry_signature: str
    plan_signature: str
    run_id: str
    published_at: datetime
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        _required(self.dag_id, "dag_id")
        _scope_id(self.scope_id)
        for field in ("generation_id", "snapshot_uri", "run_id"):
            _required(getattr(self, field), field)
        for field in (
            "generation_signature",
            "manifest_sha256",
            "snapshot_sha256",
            "registry_signature",
            "plan_signature",
        ):
            _signature(getattr(self, field), field)
        _utc(self.published_at, "published_at")
        if self.completed_at is not None:
            _utc(self.completed_at, "completed_at")


@dataclass(frozen=True, slots=True)
class RunManifestEvidence:
    dag_id: str
    run_id: str
    attempt: int
    scope_id: str
    plan_signature: str
    registry_signature: str
    state: str
    evidence_uri: str
    evidence_sha256: str
    recorded_at: datetime

    def __post_init__(self) -> None:
        _required(self.dag_id, "dag_id")
        _required(self.run_id, "run_id")
        _positive_int(self.attempt, "attempt")
        _scope_id(self.scope_id)
        _signature(self.plan_signature, "plan_signature")
        _signature(self.registry_signature, "registry_signature")
        if self.state not in {"complete", "noop"}:
            raise ValueError("run manifest state must be complete or noop")
        _required(self.evidence_uri, "evidence_uri")
        _signature(self.evidence_sha256, "evidence_sha256")
        _utc(self.recorded_at, "recorded_at")


class PublicationFence:
    """Callable lease assertion plus same-transaction evidence recorder."""

    def __init__(
        self,
        assert_fn: Callable[[], None],
        record_fn: Callable[[ScopeHead | None, RunManifestEvidence], ScopeHead | None],
        clock_fn: Callable[[], datetime],
    ) -> None:
        self._assert_fn = assert_fn
        self._record_fn = record_fn
        self._clock_fn = clock_fn
        self._last_asserted_at: datetime | None = None

    def __call__(self) -> None:
        self._assert_fn()
        self._last_asserted_at = _utc(self._clock_fn(), "publication clock")

    def publication_time(self) -> datetime:
        """Return the authoritative clock while the publication fence is held."""

        self._assert_fn()
        # Refresh on every explicit read. Repository fence callbacks happen
        # immediately *before* appends; orchestration reads this value after
        # COMPLETE so freshness is not stamped with the pre-commit instant.
        self._last_asserted_at = _utc(self._clock_fn(), "publication clock")
        return self._last_asserted_at

    def record_published(
        self, head: ScopeHead, evidence: RunManifestEvidence
    ) -> ScopeHead:
        self()
        if head.scope_id != evidence.scope_id:
            raise ValueError("head/evidence scope mismatch")
        if head.run_id != evidence.run_id:
            raise ValueError("head/evidence run mismatch")
        if head.dag_id != evidence.dag_id:
            raise ValueError("head/evidence DAG mismatch")
        if head.plan_signature != evidence.plan_signature:
            raise ValueError("head/evidence plan mismatch")
        if head.registry_signature != evidence.registry_signature:
            raise ValueError("head/evidence registry mismatch")
        selected = self._record_fn(head, evidence)
        if selected is None:
            raise OperationsError("published head selection returned no head")
        return selected

    def record_evidence(self, evidence: RunManifestEvidence) -> None:
        """Record an exact no-op run without moving the current scope head."""

        self()
        if evidence.state != "noop":
            raise ValueError("evidence-only publication must be a no-op")
        self._record_fn(None, evidence)


class ScopeLeaseStore(Protocol):
    def read_run_evidence(
        self, *, dag_id: str, run_id: str, attempt: int
    ) -> tuple[RunManifestEvidence, ...]: ...

    def read_latest_run_evidence_by_scope(
        self, scope_ids: Iterable[str], *, dag_id: str
    ) -> dict[str, RunManifestEvidence]: ...

    def acquire_many(
        self,
        scope_ids: Iterable[str],
        *,
        owner_id: str,
        plan_signature: str,
        now: datetime,
        ttl: timedelta,
    ) -> tuple[ScopeLease, ...]: ...

    def reclaim_owner_many(
        self,
        expected_leases: Iterable[ScopeLease],
        *,
        owner_id: str,
        plan_signature: str,
        now: datetime,
        ttl: timedelta,
    ) -> tuple[ScopeLease, ...]: ...

    def assert_owned(self, lease: ScopeLease, *, now: datetime) -> None: ...

    def read_owner_leases(self, owner_id: str) -> tuple[ScopeLease, ...]: ...

    def bind_plans(
        self,
        leases: Iterable[ScopeLease],
        plan_signatures: Mapping[str, str],
        *,
        now: datetime,
    ) -> tuple[ScopeLease, ...]: ...

    def read_scope_heads_owned(
        self, leases: Iterable[ScopeLease], *, now: datetime
    ) -> dict[str, ScopeHead]: ...

    def hydrate_head_completed_at(
        self,
        lease: ScopeLease,
        expected: ScopeHead,
        *,
        completed_at: datetime,
        now: datetime,
    ) -> ScopeHead: ...

    def renew(
        self, lease: ScopeLease, *, now: datetime, ttl: timedelta
    ) -> ScopeLease: ...

    def release(self, lease: ScopeLease, *, now: datetime) -> None: ...

    def publication_guard(
        self, lease: ScopeLease, *, now: datetime
    ) -> ContextManager[PublicationFence]: ...


class MemoryRequestPermitStore:
    """Reference durable leaky-bucket gate shared by mapped HTTP tasks."""

    def __init__(self, *, requests_per_minute: int = 30) -> None:
        if type(requests_per_minute) is not int or not 1 <= requests_per_minute <= 30:
            raise ValueError("requests_per_minute must be between 1 and 30")
        self._interval = timedelta(seconds=60 / requests_per_minute)
        self._next_at: datetime | None = None
        self._lock = threading.Lock()

    def acquire(self, *, now: datetime) -> datetime:
        requested_at = _utc(now, "now")
        with self._lock:
            admitted_at = max(requested_at, self._next_at or requested_at)
            self._next_at = admitted_at + self._interval
            return admitted_at


def _lease_token(
    *, scope_id: str, owner_id: str, plan_signature: str, epoch: int
) -> str:
    material = "\x00".join(
        ("espn-scope-lease-v1", scope_id, owner_id, plan_signature, str(epoch))
    )
    return hashlib.sha256(material.encode()).hexdigest()


class MemoryScopeLeaseStore:
    """Thread-safe reference implementation of the durable lease semantics."""

    def __init__(self) -> None:
        self._rows: dict[str, ScopeLease] = {}
        self._epochs: dict[str, int] = {}
        self._released: set[tuple[str, str, int]] = set()
        self._heads: dict[str, ScopeHead] = {}
        self._run_evidence: dict[tuple[str, str, int, str], RunManifestEvidence] = {}
        self._lock = threading.RLock()

    def current(self, scope_id: str) -> ScopeLease | None:
        with self._lock:
            return self._rows.get(_scope_id(scope_id))

    def read_owner_leases(self, owner_id: str) -> tuple[ScopeLease, ...]:
        owner = _required(owner_id, "owner_id")
        with self._lock:
            return tuple(
                sorted(
                    (item for item in self._rows.values() if item.owner_id == owner),
                    key=lambda item: item.scope_id,
                )
            )

    def bind_plans(
        self,
        leases: Iterable[ScopeLease],
        plan_signatures: Mapping[str, str],
        *,
        now: datetime,
    ) -> tuple[ScopeLease, ...]:
        """Atomically replace admission identities with exact signed plans."""

        ordered = tuple(sorted(leases, key=lambda item: item.scope_id))
        checked_at = _utc(now, "now")
        if not ordered or len({item.scope_id for item in ordered}) != len(ordered):
            raise ValueError("plan binding requires unique non-empty leases")
        signatures = {
            _scope_id(scope_id): _signature(signature, "plan_signature")
            for scope_id, signature in plan_signatures.items()
        }
        if set(signatures) != {item.scope_id for item in ordered}:
            raise ValueError("plan binding scope set must equal lease scope set")
        with self._lock:
            for lease in ordered:
                current = self._rows.get(lease.scope_id)
                if (
                    not self._same_identity(current, lease)
                    or current.expires_at <= checked_at
                ):
                    raise LeaseLost(
                        f"ESPN scope lease lost for {lease.scope_id} "
                        f"epoch {lease.epoch}"
                    )
            rebound = []
            for lease in ordered:
                signature = signatures[lease.scope_id]
                bound = ScopeLease(
                    scope_id=lease.scope_id,
                    owner_id=lease.owner_id,
                    plan_signature=signature,
                    epoch=lease.epoch,
                    token_sha256=_lease_token(
                        scope_id=lease.scope_id,
                        owner_id=lease.owner_id,
                        plan_signature=signature,
                        epoch=lease.epoch,
                    ),
                    acquired_at=lease.acquired_at,
                    expires_at=lease.expires_at,
                )
                self._rows[lease.scope_id] = bound
                rebound.append(bound)
            return tuple(rebound)

    def read_scope_heads(self, scope_ids: Iterable[str]) -> dict[str, ScopeHead]:
        scopes = tuple(_scope_id(item) for item in scope_ids)
        with self._lock:
            return {
                scope_id: self._heads[scope_id]
                for scope_id in scopes
                if scope_id in self._heads
            }

    def read_scope_heads_owned(
        self, leases: Iterable[ScopeLease], *, now: datetime
    ) -> dict[str, ScopeHead]:
        """Read one coherent head set only while every bundle lease is live."""

        ordered = tuple(sorted(leases, key=lambda item: item.scope_id))
        checked_at = _utc(now, "now")
        if not ordered or len({item.scope_id for item in ordered}) != len(ordered):
            raise ValueError("owned head read requires unique non-empty leases")
        with self._lock:
            for lease in ordered:
                current = self._rows.get(lease.scope_id)
                if (
                    not self._same_identity(current, lease)
                    or current.expires_at <= checked_at
                ):
                    raise LeaseLost(
                        f"ESPN scope lease lost for {lease.scope_id} "
                        f"epoch {lease.epoch}"
                    )
            return {
                lease.scope_id: self._heads[lease.scope_id]
                for lease in ordered
                if lease.scope_id in self._heads
            }

    def hydrate_head_completed_at(
        self,
        lease: ScopeLease,
        expected: ScopeHead,
        *,
        completed_at: datetime,
        now: datetime,
    ) -> ScopeHead:
        """Backfill logical time only from an exact immutable snapshot."""

        checked_at = _utc(now, "now")
        logical_time = _utc(completed_at, "completed_at")
        with self._lock:
            current_lease = self._rows.get(lease.scope_id)
            current_head = self._heads.get(lease.scope_id)
            if (
                not self._same_identity(current_lease, lease)
                or current_lease.expires_at <= checked_at
            ):
                raise LeaseLost(
                    f"ESPN scope lease lost for {lease.scope_id} epoch {lease.epoch}"
                )
            if current_head != expected:
                raise OperationsError(
                    "scope head changed during logical-time hydration"
                )
            if current_head.completed_at is not None:
                if current_head.completed_at != logical_time:
                    raise OperationsError(
                        "scope head logical time conflicts with snapshot"
                    )
                return current_head
            hydrated = replace(current_head, completed_at=logical_time)
            self._heads[lease.scope_id] = hydrated
            return hydrated

    def read_run_evidence(
        self, *, dag_id: str, run_id: str, attempt: int
    ) -> tuple[RunManifestEvidence, ...]:
        dag = _required(dag_id, "dag_id")
        run = _required(run_id, "run_id")
        attempt_value = _positive_int(attempt, "attempt")
        with self._lock:
            return tuple(
                sorted(
                    (
                        evidence
                        for (
                            stored_dag,
                            stored_run,
                            stored_attempt,
                            _,
                        ), evidence in self._run_evidence.items()
                        if stored_dag == dag
                        and stored_run == run
                        and stored_attempt == attempt_value
                    ),
                    key=lambda item: item.scope_id,
                )
            )

    def read_latest_run_evidence_by_scope(
        self, scope_ids: Iterable[str], *, dag_id: str
    ) -> dict[str, RunManifestEvidence]:
        scopes = tuple(_scope_id(item) for item in scope_ids)
        if scopes != tuple(sorted(set(scopes))):
            raise ValueError("latest evidence scope set must be sorted and unique")
        dag = _required(dag_id, "dag_id")
        with self._lock:
            candidates: dict[str, list[RunManifestEvidence]] = {}
            for evidence in self._run_evidence.values():
                if evidence.scope_id not in scopes or evidence.dag_id != dag:
                    continue
                candidates.setdefault(evidence.scope_id, []).append(evidence)
            latest = {}
            for scope_id, rows in candidates.items():
                latest_at = max(item.recorded_at for item in rows)
                matches = [item for item in rows if item.recorded_at == latest_at]
                if len(matches) != 1:
                    raise OperationsError(
                        f"ambiguous latest run evidence for {scope_id}"
                    )
                latest[scope_id] = matches[0]
            return dict(sorted(latest.items()))

    def acquire_many(
        self,
        scope_ids: Iterable[str],
        *,
        owner_id: str,
        plan_signature: str,
        now: datetime,
        ttl: timedelta,
    ) -> tuple[ScopeLease, ...]:
        ordered = tuple(sorted(_scope_id(item) for item in scope_ids))
        if not ordered or len(ordered) != len(set(ordered)):
            raise ValueError("scope lease batch must be non-empty and unique")
        owner = _required(owner_id, "owner_id")
        signature = _signature(plan_signature, "plan_signature")
        acquired_at = _utc(now, "now")
        lifetime = _ttl(ttl)
        with self._lock:
            conflicts: list[str] = []
            for scope_id in ordered:
                current = self._rows.get(scope_id)
                if current is None or current.expires_at <= acquired_at:
                    continue
                if current.owner_id != owner:
                    conflicts.append(scope_id)
                elif current.plan_signature != signature:
                    raise LeaseConflict(
                        f"scope {scope_id} live lease cannot change plan identity"
                    )
            if conflicts:
                raise LeaseConflict(
                    "live ESPN scope lease conflict: " + ", ".join(conflicts)
                )

            result: list[ScopeLease] = []
            for scope_id in ordered:
                current = self._rows.get(scope_id)
                if (
                    current is not None
                    and current.expires_at > acquired_at
                    and current.owner_id == owner
                    and current.plan_signature == signature
                ):
                    result.append(current)
                    continue
                epoch = self._epochs.get(scope_id, 0) + 1
                lease = ScopeLease(
                    scope_id=scope_id,
                    owner_id=owner,
                    plan_signature=signature,
                    epoch=epoch,
                    token_sha256=_lease_token(
                        scope_id=scope_id,
                        owner_id=owner,
                        plan_signature=signature,
                        epoch=epoch,
                    ),
                    acquired_at=acquired_at,
                    expires_at=acquired_at + lifetime,
                )
                self._rows[scope_id] = lease
                self._epochs[scope_id] = epoch
                result.append(lease)
            return tuple(result)

    def assert_owned(self, lease: ScopeLease, *, now: datetime) -> None:
        if not isinstance(lease, ScopeLease):
            raise TypeError("lease must be ScopeLease")
        checked_at = _utc(now, "now")
        with self._lock:
            current = self._rows.get(lease.scope_id)
            if (
                current != lease
                or current.expires_at <= checked_at
                or current.token_sha256 != lease.token_sha256
            ):
                raise LeaseLost(
                    f"ESPN scope lease lost for {lease.scope_id} epoch {lease.epoch}"
                )

    def reclaim_owner_many(
        self,
        expected_leases: Iterable[ScopeLease],
        *,
        owner_id: str,
        plan_signature: str,
        now: datetime,
        ttl: timedelta,
    ) -> tuple[ScopeLease, ...]:
        """Fence every worker of one partial retry bundle with fresh epochs."""

        candidates = tuple(expected_leases)
        if any(not isinstance(item, ScopeLease) for item in candidates):
            raise TypeError("expected leases must be ScopeLease values")
        expected = tuple(sorted(candidates, key=lambda item: item.scope_id))
        ordered = tuple(item.scope_id for item in expected)
        if not ordered or len(ordered) != len(set(ordered)):
            raise ValueError("scope lease batch must be non-empty and unique")
        owner = _required(owner_id, "owner_id")
        if any(item.owner_id != owner for item in expected):
            raise LeaseConflict("expected retry bundle owner differs")
        signature = _signature(plan_signature, "plan_signature")
        acquired_at = _utc(now, "now")
        lifetime = _ttl(ttl)
        if not any(item.expires_at <= acquired_at for item in expected):
            raise LeaseConflict("ESPN retry bundle has no expired lease")
        with self._lock:
            current = {scope_id: self._rows.get(scope_id) for scope_id in ordered}
            expected_by_scope = {item.scope_id: item for item in expected}
            if any(
                not self._same_identity(current[scope_id], expected_by_scope[scope_id])
                for scope_id in ordered
            ):
                raise LeaseLost("ESPN retry bundle identity changed before reclaim")
            if not any(lease.expires_at <= acquired_at for lease in current.values()):
                raise LeaseConflict("ESPN retry bundle is live after locked recheck")
            result = []
            for scope_id in ordered:
                previous = current[scope_id]
                epoch = max(self._epochs.get(scope_id, 0), previous.epoch) + 1
                replacement = ScopeLease(
                    scope_id=scope_id,
                    owner_id=owner,
                    plan_signature=signature,
                    epoch=epoch,
                    token_sha256=_lease_token(
                        scope_id=scope_id,
                        owner_id=owner,
                        plan_signature=signature,
                        epoch=epoch,
                    ),
                    acquired_at=acquired_at,
                    expires_at=acquired_at + lifetime,
                )
                self._rows[scope_id] = replacement
                self._epochs[scope_id] = epoch
                result.append(replacement)
            return tuple(result)

    @staticmethod
    def _same_identity(current: ScopeLease | None, expected: ScopeLease) -> bool:
        return current is not None and (
            current.scope_id,
            current.owner_id,
            current.plan_signature,
            current.epoch,
            current.token_sha256,
        ) == (
            expected.scope_id,
            expected.owner_id,
            expected.plan_signature,
            expected.epoch,
            expected.token_sha256,
        )

    def renew(self, lease: ScopeLease, *, now: datetime, ttl: timedelta) -> ScopeLease:
        checked_at = _utc(now, "now")
        lifetime = _ttl(ttl)
        with self._lock:
            current = self._rows.get(lease.scope_id)
            if (
                not self._same_identity(current, lease)
                or current.expires_at <= checked_at
            ):
                raise LeaseLost(
                    f"ESPN scope lease lost for {lease.scope_id} epoch {lease.epoch}"
                )
            renewed = ScopeLease(
                scope_id=current.scope_id,
                owner_id=current.owner_id,
                plan_signature=current.plan_signature,
                epoch=current.epoch,
                token_sha256=current.token_sha256,
                acquired_at=current.acquired_at,
                expires_at=max(current.expires_at, checked_at + lifetime),
            )
            self._rows[lease.scope_id] = renewed
            return renewed

    def release(self, lease: ScopeLease, *, now: datetime) -> None:
        _utc(now, "now")
        released_identity = (lease.scope_id, lease.token_sha256, lease.epoch)
        with self._lock:
            current = self._rows.get(lease.scope_id)
            if current is None and released_identity in self._released:
                return
            if not self._same_identity(current, lease):
                raise LeaseLost(
                    f"ESPN scope lease lost for {lease.scope_id} epoch {lease.epoch}"
                )
            del self._rows[lease.scope_id]
            self._released.add(released_identity)

    @contextmanager
    def publication_guard(
        self, lease: ScopeLease, *, now: datetime
    ) -> Iterator[PublicationFence]:
        """Hold the same scope lock through the COMPLETE visibility append."""

        if not isinstance(lease, ScopeLease):
            raise TypeError("lease must be ScopeLease")
        checked_at = _utc(now, "now")
        with self._lock:
            current = self._rows.get(lease.scope_id)
            if current != lease or current.expires_at <= checked_at:
                raise LeaseLost(
                    f"ESPN scope lease lost for {lease.scope_id} epoch {lease.epoch}"
                )

            def assert_locked() -> None:
                current_locked = self._rows.get(lease.scope_id)
                if (
                    not self._same_identity(current_locked, lease)
                    or current_locked.expires_at <= checked_at
                ):
                    raise LeaseLost(
                        f"ESPN scope lease lost for {lease.scope_id} "
                        f"epoch {lease.epoch}"
                    )

            def record(
                head: ScopeHead | None, evidence: RunManifestEvidence
            ) -> ScopeHead | None:
                if evidence.scope_id != lease.scope_id:
                    raise LeaseLost("publication evidence scope is not leased")
                if head is not None:
                    existing_head = self._heads.get(head.scope_id)
                    if existing_head is not None:
                        same_generation = (
                            existing_head.generation_id == head.generation_id
                        )
                        if same_generation and existing_head != head:
                            raise OperationsError(
                                "scope head generation identity conflict"
                            )
                        if existing_head.completed_at is None:
                            raise OperationsError(
                                "scope head logical order is not hydrated"
                            )
                key = (
                    evidence.dag_id,
                    evidence.run_id,
                    evidence.attempt,
                    evidence.scope_id,
                )
                stored = self._run_evidence.get(key)
                if stored is not None and stored != evidence:
                    raise OperationsError("run manifest evidence conflict")
                selected = existing_head if head is not None else None
                if head is not None and (
                    existing_head is None
                    or current_manifest_order_key(head)
                    > current_manifest_order_key(existing_head)
                ):
                    self._heads[head.scope_id] = head
                    selected = head
                self._run_evidence[key] = evidence
                return selected

            yield PublicationFence(assert_locked, record, lambda: checked_at)


class PostgresEspnControlStore:
    """PostgreSQL-backed lease, publication fence and shared request gate.

    Scope rows are locked in sorted order.  ``publication_guard`` keeps the
    exact row lock and transaction open through the repository's COMPLETE
    manifest append, so reclaim cannot cross the visibility boundary.
    """

    CONTROL_SCHEMA = "espn_control"
    LEASE_TABLE = "espn_control.scope_lease_v2"
    RATE_TABLE = "espn_control.request_rate_gate_v2"
    HEAD_TABLE = "espn_control.scope_head_v2"
    RUN_TABLE = "espn_control.run_manifest_v2"

    def __init__(self, connect: Callable[[], Any]) -> None:
        if not callable(connect):
            raise TypeError("connect must be callable")
        self._connect = connect

    @classmethod
    def from_env(cls) -> "PostgresEspnControlStore":
        dsn = os.environ.get("ESPN_CONTROL_DATABASE_URL") or os.environ.get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
        )
        if not dsn:
            raise OperationsError(
                "ESPN_CONTROL_DATABASE_URL or Airflow SQL connection is required"
            )
        if dsn.startswith("postgresql+psycopg2://"):
            dsn = "postgresql://" + dsn.removeprefix("postgresql+psycopg2://")
        if not dsn.startswith(("postgresql://", "postgres://")):
            raise OperationsError("ESPN control store must use PostgreSQL")

        def connect():
            import psycopg2

            return psycopg2.connect(dsn)

        return cls(connect)

    def migrate(self) -> None:
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.CONTROL_SCHEMA}")
                    cursor.execute(
                        f"""CREATE TABLE IF NOT EXISTS {self.LEASE_TABLE} (
    scope_id text PRIMARY KEY,
    owner_id text NOT NULL,
    plan_signature char(64) NOT NULL,
    epoch bigint NOT NULL CHECK (epoch > 0),
    token_sha256 char(64) NOT NULL,
    acquired_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    CHECK (expires_at > acquired_at)
)"""
                    )
                    cursor.execute(
                        f"""CREATE TABLE IF NOT EXISTS {self.RATE_TABLE} (
    source text PRIMARY KEY,
    next_permit_at timestamptz NOT NULL
)"""
                    )
                    cursor.execute(
                        f"""CREATE TABLE IF NOT EXISTS {self.HEAD_TABLE} (
    scope_id text PRIMARY KEY,
    dag_id text NOT NULL,
    generation_id text NOT NULL,
    generation_signature char(64) NOT NULL,
    manifest_sha256 char(64) NOT NULL,
    snapshot_uri text NOT NULL,
    snapshot_sha256 char(64) NOT NULL,
    registry_signature char(64) NOT NULL,
    plan_signature char(64) NOT NULL,
    run_id text NOT NULL,
    published_at timestamptz NOT NULL,
    completed_at timestamptz
)"""
                    )
                    cursor.execute(
                        f"""CREATE TABLE IF NOT EXISTS {self.RUN_TABLE} (
    dag_id text NOT NULL,
    run_id text NOT NULL,
    attempt integer NOT NULL CHECK (attempt > 0),
    scope_id text NOT NULL,
    plan_signature char(64) NOT NULL,
    registry_signature char(64) NOT NULL,
    state text NOT NULL CHECK (state IN ('complete', 'noop')),
    evidence_uri text NOT NULL,
    evidence_sha256 char(64) NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (dag_id, run_id, attempt, scope_id)
)"""
                    )
                    # Task 6 v2 tables may already exist from a pre-review deploy.
                    # Preserve those rows under an explicitly unknown legacy
                    # namespace, while all current readers remain fail-closed on
                    # their exact DAG identity.
                    cursor.execute(
                        f"ALTER TABLE {self.HEAD_TABLE} "
                        "ADD COLUMN IF NOT EXISTS dag_id text"
                    )
                    cursor.execute(
                        f"ALTER TABLE {self.HEAD_TABLE} "
                        "ADD COLUMN IF NOT EXISTS completed_at timestamptz"
                    )
                    cursor.execute(
                        f"UPDATE {self.HEAD_TABLE} SET dag_id = "
                        "'espn-legacy-v2-unknown' WHERE dag_id IS NULL"
                    )
                    cursor.execute(
                        f"ALTER TABLE {self.HEAD_TABLE} "
                        "ALTER COLUMN dag_id SET NOT NULL"
                    )
                    cursor.execute(
                        f"ALTER TABLE {self.RUN_TABLE} "
                        "ADD COLUMN IF NOT EXISTS dag_id text"
                    )
                    cursor.execute(
                        f"UPDATE {self.RUN_TABLE} SET dag_id = "
                        "'espn-legacy-v2-unknown' WHERE dag_id IS NULL"
                    )
                    cursor.execute(
                        f"ALTER TABLE {self.RUN_TABLE} ALTER COLUMN dag_id SET NOT NULL"
                    )
                    cursor.execute(
                        f"""DO $espn_migration$
DECLARE current_definition text;
BEGIN
    SELECT pg_get_constraintdef(oid)
      INTO current_definition
      FROM pg_constraint
     WHERE conrelid = '{self.RUN_TABLE}'::regclass
       AND conname = 'run_manifest_v2_pkey';
    IF current_definition IS NULL
       OR position('dag_id' in current_definition) = 0 THEN
        ALTER TABLE {self.RUN_TABLE}
            DROP CONSTRAINT IF EXISTS run_manifest_v2_pkey;
        ALTER TABLE {self.RUN_TABLE}
            ADD CONSTRAINT run_manifest_v2_pkey
            PRIMARY KEY (dag_id, run_id, attempt, scope_id);
    END IF;
END
$espn_migration$"""
                    )
        finally:
            connection.close()

    def read_scope_heads_owned(
        self, leases: Iterable[ScopeLease], *, now: datetime
    ) -> dict[str, ScopeHead]:
        """Atomically validate the sorted lease bundle, then read its heads."""

        _utc(now, "now")
        ordered = tuple(sorted(leases, key=lambda item: item.scope_id))
        if not ordered or len({item.scope_id for item in ordered}) != len(ordered):
            raise ValueError("owned head read requires unique non-empty leases")
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    for lease in ordered:
                        self._advisory_lock(cursor, lease.scope_id)
                    database_now = self._db_now(cursor)
                    for lease in ordered:
                        current = self._select_lease(
                            cursor, lease.scope_id, for_update=True
                        )
                        if (
                            not self._same_lease(current, lease)
                            or current.expires_at <= database_now
                        ):
                            raise LeaseLost(
                                f"ESPN scope lease lost for {lease.scope_id} "
                                f"epoch {lease.epoch}"
                            )
                    cursor.execute(
                        f"SELECT dag_id, scope_id, generation_id, generation_signature, "
                        f"manifest_sha256, snapshot_uri, snapshot_sha256, "
                        f"registry_signature, plan_signature, run_id, published_at, "
                        f"completed_at "
                        f"FROM {self.HEAD_TABLE} WHERE scope_id = ANY(%s) "
                        "ORDER BY scope_id FOR UPDATE",
                        ([lease.scope_id for lease in ordered],),
                    )
                    heads = tuple(self._head_from_row(row) for row in cursor.fetchall())
                    return {head.scope_id: head for head in heads}
        finally:
            connection.close()

    def hydrate_head_completed_at(
        self,
        lease: ScopeLease,
        expected: ScopeHead,
        *,
        completed_at: datetime,
        now: datetime,
    ) -> ScopeHead:
        """Hydrate a legacy nullable head from its exact signed snapshot only."""

        _utc(now, "now")
        logical_time = _utc(completed_at, "completed_at")
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    self._advisory_lock(cursor, lease.scope_id)
                    database_now = self._db_now(cursor)
                    current_lease = self._select_lease(
                        cursor, lease.scope_id, for_update=True
                    )
                    if (
                        not self._same_lease(current_lease, lease)
                        or current_lease.expires_at <= database_now
                    ):
                        raise LeaseLost(
                            f"ESPN scope lease lost for {lease.scope_id} "
                            f"epoch {lease.epoch}"
                        )
                    cursor.execute(
                        f"SELECT dag_id, scope_id, generation_id, "
                        f"generation_signature, manifest_sha256, snapshot_uri, "
                        f"snapshot_sha256, registry_signature, plan_signature, "
                        f"run_id, published_at, completed_at FROM {self.HEAD_TABLE} "
                        "WHERE scope_id = %s FOR UPDATE",
                        (lease.scope_id,),
                    )
                    row = cursor.fetchone()
                    current = None if row is None else self._head_from_row(row)
                    if current != expected:
                        raise OperationsError(
                            "scope head changed during logical-time hydration"
                        )
                    if current.completed_at is not None:
                        if current.completed_at != logical_time:
                            raise OperationsError(
                                "scope head logical time conflicts with snapshot"
                            )
                        return current
                    cursor.execute(
                        f"UPDATE {self.HEAD_TABLE} SET completed_at = %s "
                        "WHERE scope_id = %s AND completed_at IS NULL",
                        (logical_time, lease.scope_id),
                    )
                    if cursor.rowcount != 1:
                        raise OperationsError("scope head logical-time hydration raced")
                    return replace(current, completed_at=logical_time)
        finally:
            connection.close()

    @staticmethod
    def _db_now(cursor) -> datetime:
        cursor.execute("SELECT clock_timestamp()")
        row = cursor.fetchone()
        if row is None:
            raise OperationsError("PostgreSQL clock query returned no row")
        return _utc(row[0], "database clock")

    def current_time(self) -> datetime:
        """Read PostgreSQL's wall clock for monitor/freshness decisions."""

        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    return self._db_now(cursor)
        finally:
            connection.close()

    @staticmethod
    def _advisory_lock(cursor, scope_id: str) -> None:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"espn:{scope_id}",),
        )

    @staticmethod
    def _lease_from_row(row: Sequence[Any]) -> ScopeLease:
        if len(row) != 7:
            raise OperationsError("stored ESPN scope lease row is malformed")
        return ScopeLease(
            scope_id=row[0],
            owner_id=row[1],
            plan_signature=row[2],
            epoch=int(row[3]),
            token_sha256=row[4],
            acquired_at=row[5],
            expires_at=row[6],
        )

    def _select_lease(
        self, cursor, scope_id: str, *, for_update: bool
    ) -> ScopeLease | None:
        suffix = " FOR UPDATE" if for_update else ""
        cursor.execute(
            f"SELECT scope_id, owner_id, plan_signature, epoch, token_sha256, "
            f"acquired_at, expires_at FROM {self.LEASE_TABLE} "
            f"WHERE scope_id = %s{suffix}",
            (scope_id,),
        )
        row = cursor.fetchone()
        return None if row is None else self._lease_from_row(row)

    @staticmethod
    def _head_from_row(row: Sequence[Any]) -> ScopeHead:
        if len(row) not in {11, 12}:
            raise OperationsError("stored ESPN scope head row is malformed")
        return ScopeHead(
            dag_id=row[0],
            scope_id=row[1],
            generation_id=row[2],
            generation_signature=row[3],
            manifest_sha256=row[4],
            snapshot_uri=row[5],
            snapshot_sha256=row[6],
            registry_signature=row[7],
            plan_signature=row[8],
            run_id=row[9],
            published_at=row[10],
            completed_at=None if len(row) == 11 else row[11],
        )

    @staticmethod
    def _evidence_from_row(row: Sequence[Any]) -> RunManifestEvidence:
        if len(row) != 10:
            raise OperationsError("stored ESPN run evidence row is malformed")
        return RunManifestEvidence(
            dag_id=row[0],
            run_id=row[1],
            attempt=int(row[2]),
            scope_id=row[3],
            plan_signature=row[4],
            registry_signature=row[5],
            state=row[6],
            evidence_uri=row[7],
            evidence_sha256=row[8],
            recorded_at=row[9],
        )

    def read_scope_heads(self, scope_ids: Iterable[str]) -> dict[str, ScopeHead]:
        scopes = tuple(sorted({_scope_id(item) for item in scope_ids}))
        if not scopes:
            return {}
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SELECT dag_id, scope_id, generation_id, generation_signature, "
                        f"manifest_sha256, snapshot_uri, snapshot_sha256, "
                        f"registry_signature, plan_signature, run_id, published_at, "
                        f"completed_at "
                        f"FROM {self.HEAD_TABLE} WHERE scope_id = ANY(%s)",
                        (list(scopes),),
                    )
                    heads = tuple(self._head_from_row(row) for row in cursor.fetchall())
                    return {head.scope_id: head for head in heads}
        finally:
            connection.close()

    def read_owner_leases(self, owner_id: str) -> tuple[ScopeLease, ...]:
        owner = _required(owner_id, "owner_id")
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SELECT scope_id, owner_id, plan_signature, epoch, "
                        f"token_sha256, acquired_at, expires_at "
                        f"FROM {self.LEASE_TABLE} WHERE owner_id = %s "
                        "ORDER BY scope_id",
                        (owner,),
                    )
                    return tuple(self._lease_from_row(row) for row in cursor.fetchall())
        finally:
            connection.close()

    def bind_plans(
        self,
        leases: Iterable[ScopeLease],
        plan_signatures: Mapping[str, str],
        *,
        now: datetime,
    ) -> tuple[ScopeLease, ...]:
        """Atomically fence the whole owned bundle to exact plan signatures."""

        _utc(now, "now")
        ordered = tuple(sorted(leases, key=lambda item: item.scope_id))
        if not ordered or len({item.scope_id for item in ordered}) != len(ordered):
            raise ValueError("plan binding requires unique non-empty leases")
        signatures = {
            _scope_id(scope_id): _signature(signature, "plan_signature")
            for scope_id, signature in plan_signatures.items()
        }
        if set(signatures) != {item.scope_id for item in ordered}:
            raise ValueError("plan binding scope set must equal lease scope set")
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    for lease in ordered:
                        self._advisory_lock(cursor, lease.scope_id)
                    database_now = self._db_now(cursor)
                    for lease in ordered:
                        current = self._select_lease(
                            cursor, lease.scope_id, for_update=True
                        )
                        if (
                            not self._same_lease(current, lease)
                            or current.expires_at <= database_now
                        ):
                            raise LeaseLost(
                                f"ESPN scope lease lost for {lease.scope_id} "
                                f"epoch {lease.epoch}"
                            )
                    rebound = []
                    for lease in ordered:
                        signature = signatures[lease.scope_id]
                        token = _lease_token(
                            scope_id=lease.scope_id,
                            owner_id=lease.owner_id,
                            plan_signature=signature,
                            epoch=lease.epoch,
                        )
                        cursor.execute(
                            f"UPDATE {self.LEASE_TABLE} SET "
                            "plan_signature = %s, token_sha256 = %s "
                            "WHERE scope_id = %s AND owner_id = %s "
                            "AND plan_signature = %s AND epoch = %s "
                            "AND token_sha256 = %s "
                            "RETURNING scope_id, owner_id, plan_signature, epoch, "
                            "token_sha256, acquired_at, expires_at",
                            (
                                signature,
                                token,
                                lease.scope_id,
                                lease.owner_id,
                                lease.plan_signature,
                                lease.epoch,
                                lease.token_sha256,
                            ),
                        )
                        row = cursor.fetchone()
                        if row is None:
                            raise LeaseLost(
                                f"ESPN scope lease lost for {lease.scope_id} "
                                f"epoch {lease.epoch}"
                            )
                        rebound.append(self._lease_from_row(row))
                    return tuple(rebound)
        finally:
            connection.close()

    def read_run_evidence(
        self, *, dag_id: str, run_id: str, attempt: int
    ) -> tuple[RunManifestEvidence, ...]:
        dag = _required(dag_id, "dag_id")
        run = _required(run_id, "run_id")
        attempt_value = _positive_int(attempt, "attempt")
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SELECT dag_id, run_id, attempt, scope_id, plan_signature, "
                        f"registry_signature, state, evidence_uri, "
                        f"evidence_sha256, recorded_at FROM {self.RUN_TABLE} "
                        "WHERE dag_id = %s AND run_id = %s AND attempt = %s "
                        "ORDER BY scope_id",
                        (dag, run, attempt_value),
                    )
                    return tuple(
                        self._evidence_from_row(row) for row in cursor.fetchall()
                    )
        finally:
            connection.close()

    def read_latest_run_evidence_by_scope(
        self, scope_ids: Iterable[str], *, dag_id: str
    ) -> dict[str, RunManifestEvidence]:
        scopes = tuple(_scope_id(item) for item in scope_ids)
        if scopes != tuple(sorted(set(scopes))):
            raise ValueError("latest evidence scope set must be sorted and unique")
        dag = _required(dag_id, "dag_id")
        if not scopes:
            return {}
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT dag_id, run_id, attempt, scope_id, plan_signature, "
                        "registry_signature, state, evidence_uri, evidence_sha256, "
                        "recorded_at FROM (SELECT dag_id, run_id, attempt, scope_id, "
                        "plan_signature, registry_signature, state, evidence_uri, "
                        "evidence_sha256, recorded_at, DENSE_RANK() OVER "
                        "(PARTITION BY scope_id ORDER BY recorded_at DESC) AS freshness_rank "
                        f"FROM {self.RUN_TABLE} WHERE dag_id = %s AND scope_id = ANY(%s)"
                        ") ranked WHERE freshness_rank = 1 ORDER BY scope_id",
                        (dag, list(scopes)),
                    )
                    rows = tuple(
                        self._evidence_from_row(row) for row in cursor.fetchall()
                    )
                    output = {}
                    for item in rows:
                        if item.scope_id in output:
                            raise OperationsError(
                                f"ambiguous latest run evidence for {item.scope_id}"
                            )
                        output[item.scope_id] = item
                    return output
        finally:
            connection.close()

    def acquire_many(
        self,
        scope_ids: Iterable[str],
        *,
        owner_id: str,
        plan_signature: str,
        now: datetime,
        ttl: timedelta,
    ) -> tuple[ScopeLease, ...]:
        # ``now`` is validated for API symmetry but PostgreSQL is authoritative.
        _utc(now, "now")
        ordered = tuple(sorted(_scope_id(item) for item in scope_ids))
        if not ordered or len(ordered) != len(set(ordered)):
            raise ValueError("scope lease batch must be non-empty and unique")
        owner = _required(owner_id, "owner_id")
        signature = _signature(plan_signature, "plan_signature")
        lifetime = _ttl(ttl)
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    for scope_id in ordered:
                        self._advisory_lock(cursor, scope_id)
                    database_now = self._db_now(cursor)
                    current = {
                        scope_id: self._select_lease(cursor, scope_id, for_update=True)
                        for scope_id in ordered
                    }
                    conflicts = [
                        scope_id
                        for scope_id, lease in current.items()
                        if lease is not None
                        and lease.expires_at > database_now
                        and lease.owner_id != owner
                    ]
                    if conflicts:
                        raise LeaseConflict(
                            "live ESPN scope lease conflict: " + ", ".join(conflicts)
                        )
                    for scope_id, lease in current.items():
                        if (
                            lease is not None
                            and lease.expires_at > database_now
                            and lease.owner_id == owner
                            and lease.plan_signature != signature
                        ):
                            raise LeaseConflict(
                                f"scope {scope_id} live lease cannot change plan identity"
                            )
                    result: list[ScopeLease] = []
                    for scope_id in ordered:
                        lease = current[scope_id]
                        if (
                            lease is not None
                            and lease.expires_at > database_now
                            and lease.owner_id == owner
                            and lease.plan_signature == signature
                        ):
                            result.append(lease)
                            continue
                        epoch = 1 if lease is None else lease.epoch + 1
                        replacement = ScopeLease(
                            scope_id=scope_id,
                            owner_id=owner,
                            plan_signature=signature,
                            epoch=epoch,
                            token_sha256=_lease_token(
                                scope_id=scope_id,
                                owner_id=owner,
                                plan_signature=signature,
                                epoch=epoch,
                            ),
                            acquired_at=database_now,
                            expires_at=database_now + lifetime,
                        )
                        cursor.execute(
                            f"""INSERT INTO {self.LEASE_TABLE}
(scope_id, owner_id, plan_signature, epoch, token_sha256, acquired_at, expires_at)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (scope_id) DO UPDATE SET
owner_id = EXCLUDED.owner_id,
plan_signature = EXCLUDED.plan_signature,
epoch = EXCLUDED.epoch,
token_sha256 = EXCLUDED.token_sha256,
acquired_at = EXCLUDED.acquired_at,
expires_at = EXCLUDED.expires_at""",
                            (
                                replacement.scope_id,
                                replacement.owner_id,
                                replacement.plan_signature,
                                replacement.epoch,
                                replacement.token_sha256,
                                replacement.acquired_at,
                                replacement.expires_at,
                            ),
                        )
                        result.append(replacement)
                    return tuple(result)
        finally:
            connection.close()

    def reclaim_owner_many(
        self,
        expected_leases: Iterable[ScopeLease],
        *,
        owner_id: str,
        plan_signature: str,
        now: datetime,
        ttl: timedelta,
    ) -> tuple[ScopeLease, ...]:
        """Atomically re-epoch a partial same-owner retry bundle."""

        _utc(now, "now")
        candidates = tuple(expected_leases)
        if any(not isinstance(item, ScopeLease) for item in candidates):
            raise TypeError("expected leases must be ScopeLease values")
        expected = tuple(sorted(candidates, key=lambda item: item.scope_id))
        ordered = tuple(item.scope_id for item in expected)
        if not ordered or len(ordered) != len(set(ordered)):
            raise ValueError("scope lease batch must be non-empty and unique")
        owner = _required(owner_id, "owner_id")
        if any(item.owner_id != owner for item in expected):
            raise LeaseConflict("expected retry bundle owner differs")
        signature = _signature(plan_signature, "plan_signature")
        lifetime = _ttl(ttl)
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    for scope_id in ordered:
                        self._advisory_lock(cursor, scope_id)
                    database_now = self._db_now(cursor)
                    if not any(item.expires_at <= database_now for item in expected):
                        raise LeaseConflict("ESPN retry bundle has no expired lease")
                    current = {
                        scope_id: self._select_lease(cursor, scope_id, for_update=True)
                        for scope_id in ordered
                    }
                    expected_by_scope = {item.scope_id: item for item in expected}
                    if any(
                        not self._same_lease(
                            current[scope_id], expected_by_scope[scope_id]
                        )
                        for scope_id in ordered
                    ):
                        raise LeaseLost(
                            "ESPN retry bundle identity changed before reclaim"
                        )
                    if not any(
                        lease.expires_at <= database_now for lease in current.values()
                    ):
                        raise LeaseConflict(
                            "ESPN retry bundle is live after locked recheck"
                        )
                    result = []
                    for scope_id in ordered:
                        previous = current[scope_id]
                        epoch = previous.epoch + 1
                        replacement = ScopeLease(
                            scope_id=scope_id,
                            owner_id=owner,
                            plan_signature=signature,
                            epoch=epoch,
                            token_sha256=_lease_token(
                                scope_id=scope_id,
                                owner_id=owner,
                                plan_signature=signature,
                                epoch=epoch,
                            ),
                            acquired_at=database_now,
                            expires_at=database_now + lifetime,
                        )
                        cursor.execute(
                            f"UPDATE {self.LEASE_TABLE} SET "
                            "plan_signature = %s, epoch = %s, token_sha256 = %s, "
                            "acquired_at = %s, expires_at = %s "
                            "WHERE scope_id = %s AND owner_id = %s AND epoch = %s "
                            "AND token_sha256 = %s",
                            (
                                replacement.plan_signature,
                                replacement.epoch,
                                replacement.token_sha256,
                                replacement.acquired_at,
                                replacement.expires_at,
                                scope_id,
                                owner,
                                previous.epoch,
                                previous.token_sha256,
                            ),
                        )
                        if cursor.rowcount != 1:
                            raise LeaseLost(f"ESPN retry reclaim lost scope {scope_id}")
                        result.append(replacement)
                    return tuple(result)
        finally:
            connection.close()

    @staticmethod
    def _same_lease(current: ScopeLease | None, expected: ScopeLease) -> bool:
        return current is not None and (
            current.scope_id,
            current.owner_id,
            current.plan_signature,
            current.epoch,
            current.token_sha256,
        ) == (
            expected.scope_id,
            expected.owner_id,
            expected.plan_signature,
            expected.epoch,
            expected.token_sha256,
        )

    def assert_owned(self, lease: ScopeLease, *, now: datetime) -> None:
        _utc(now, "now")
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    database_now = self._db_now(cursor)
                    current = self._select_lease(
                        cursor, lease.scope_id, for_update=False
                    )
                    if (
                        not self._same_lease(current, lease)
                        or current.expires_at <= database_now
                    ):
                        raise LeaseLost(
                            f"ESPN scope lease lost for {lease.scope_id} "
                            f"epoch {lease.epoch}"
                        )
        finally:
            connection.close()

    def renew(self, lease: ScopeLease, *, now: datetime, ttl: timedelta) -> ScopeLease:
        _utc(now, "now")
        lifetime = _ttl(ttl)
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    self._advisory_lock(cursor, lease.scope_id)
                    database_now = self._db_now(cursor)
                    current = self._select_lease(
                        cursor, lease.scope_id, for_update=True
                    )
                    if (
                        not self._same_lease(current, lease)
                        or current.expires_at <= database_now
                    ):
                        raise LeaseLost(
                            f"ESPN scope lease lost for {lease.scope_id} "
                            f"epoch {lease.epoch}"
                        )
                    cursor.execute(
                        f"UPDATE {self.LEASE_TABLE} SET expires_at = "
                        "GREATEST(expires_at, clock_timestamp() + %s) "
                        "WHERE scope_id = %s AND owner_id = %s "
                        "AND plan_signature = %s AND epoch = %s "
                        "AND token_sha256 = %s "
                        "RETURNING scope_id, owner_id, plan_signature, epoch, "
                        "token_sha256, acquired_at, expires_at",
                        (
                            lifetime,
                            lease.scope_id,
                            lease.owner_id,
                            lease.plan_signature,
                            lease.epoch,
                            lease.token_sha256,
                        ),
                    )
                    row = cursor.fetchone()
                    if row is None:
                        raise LeaseLost(
                            f"ESPN scope lease lost for {lease.scope_id} "
                            f"epoch {lease.epoch}"
                        )
                    return self._lease_from_row(row)
        finally:
            connection.close()

    def release(self, lease: ScopeLease, *, now: datetime) -> None:
        _utc(now, "now")
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    self._advisory_lock(cursor, lease.scope_id)
                    current = self._select_lease(
                        cursor, lease.scope_id, for_update=True
                    )
                    if not self._same_lease(current, lease):
                        raise LeaseLost(
                            f"ESPN scope lease lost for {lease.scope_id} "
                            f"epoch {lease.epoch}"
                        )
                    cursor.execute(
                        f"UPDATE {self.LEASE_TABLE} SET "
                        "expires_at = LEAST(expires_at, clock_timestamp()) "
                        "WHERE scope_id = %s AND owner_id = %s "
                        "AND plan_signature = %s AND epoch = %s "
                        "AND token_sha256 = %s",
                        (
                            lease.scope_id,
                            lease.owner_id,
                            lease.plan_signature,
                            lease.epoch,
                            lease.token_sha256,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise LeaseLost(
                            f"ESPN scope lease lost for {lease.scope_id} "
                            f"epoch {lease.epoch}"
                        )
        finally:
            connection.close()

    @contextmanager
    def publication_guard(
        self, lease: ScopeLease, *, now: datetime
    ) -> Iterator[PublicationFence]:
        _utc(now, "now")
        connection = self._connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    self._advisory_lock(cursor, lease.scope_id)
                    self._select_lease(cursor, lease.scope_id, for_update=True)

                    def assert_locked() -> None:
                        database_now = self._db_now(cursor)
                        locked = self._select_lease(
                            cursor, lease.scope_id, for_update=True
                        )
                        if (
                            not self._same_lease(locked, lease)
                            or locked.expires_at <= database_now
                        ):
                            raise LeaseLost(
                                f"ESPN scope lease lost for {lease.scope_id} "
                                f"epoch {lease.epoch}"
                            )

                    def record(
                        head: ScopeHead | None, evidence: RunManifestEvidence
                    ) -> ScopeHead | None:
                        current_head = None
                        if evidence.scope_id != lease.scope_id:
                            raise LeaseLost("publication evidence scope is not leased")
                        if head is not None:
                            cursor.execute(
                                f"SELECT dag_id, scope_id, generation_id, "
                                f"generation_signature, manifest_sha256, "
                                f"snapshot_uri, snapshot_sha256, "
                                f"registry_signature, plan_signature, run_id, "
                                f"published_at, completed_at FROM {self.HEAD_TABLE} "
                                "WHERE scope_id = %s FOR UPDATE",
                                (head.scope_id,),
                            )
                            row = cursor.fetchone()
                            current_head = (
                                None if row is None else self._head_from_row(row)
                            )
                            if current_head is not None:
                                same_generation = (
                                    current_head.generation_id == head.generation_id
                                )
                                if same_generation and current_head != head:
                                    raise OperationsError(
                                        "scope head generation identity conflict"
                                    )
                                if current_head.completed_at is None:
                                    raise OperationsError(
                                        "scope head logical order is not hydrated"
                                    )
                        cursor.execute(
                            f"""INSERT INTO {self.RUN_TABLE}
(dag_id, run_id, attempt, scope_id, plan_signature, registry_signature, state,
 evidence_uri, evidence_sha256, recorded_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dag_id, run_id, attempt, scope_id) DO NOTHING""",
                            (
                                evidence.dag_id,
                                evidence.run_id,
                                evidence.attempt,
                                evidence.scope_id,
                                evidence.plan_signature,
                                evidence.registry_signature,
                                evidence.state,
                                evidence.evidence_uri,
                                evidence.evidence_sha256,
                                evidence.recorded_at,
                            ),
                        )
                        cursor.execute(
                            f"SELECT dag_id, run_id, attempt, scope_id, plan_signature, "
                            f"registry_signature, state, evidence_uri, "
                            f"evidence_sha256, recorded_at FROM {self.RUN_TABLE} "
                            "WHERE dag_id = %s AND run_id = %s AND attempt = %s "
                            "AND scope_id = %s",
                            (
                                evidence.dag_id,
                                evidence.run_id,
                                evidence.attempt,
                                evidence.scope_id,
                            ),
                        )
                        evidence_row = cursor.fetchone()
                        if (
                            evidence_row is None
                            or self._evidence_from_row(evidence_row) != evidence
                        ):
                            raise OperationsError("run manifest evidence conflict")
                        selected_head = current_head
                        if head is not None and (
                            current_head is None
                            or current_manifest_order_key(head)
                            > current_manifest_order_key(current_head)
                        ):
                            cursor.execute(
                                f"""INSERT INTO {self.HEAD_TABLE}
(dag_id, scope_id, generation_id, generation_signature, manifest_sha256,
 snapshot_uri, snapshot_sha256, registry_signature, plan_signature,
 run_id, published_at, completed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (scope_id) DO UPDATE SET
dag_id = EXCLUDED.dag_id,
generation_id = EXCLUDED.generation_id,
generation_signature = EXCLUDED.generation_signature,
manifest_sha256 = EXCLUDED.manifest_sha256,
snapshot_uri = EXCLUDED.snapshot_uri,
snapshot_sha256 = EXCLUDED.snapshot_sha256,
registry_signature = EXCLUDED.registry_signature,
plan_signature = EXCLUDED.plan_signature,
run_id = EXCLUDED.run_id,
published_at = EXCLUDED.published_at,
completed_at = EXCLUDED.completed_at""",
                                (
                                    head.dag_id,
                                    head.scope_id,
                                    head.generation_id,
                                    head.generation_signature,
                                    head.manifest_sha256,
                                    head.snapshot_uri,
                                    head.snapshot_sha256,
                                    head.registry_signature,
                                    head.plan_signature,
                                    head.run_id,
                                    head.published_at,
                                    head.completed_at,
                                ),
                            )
                            selected_head = head
                        return selected_head

                    assert_locked()
                    yield PublicationFence(
                        assert_locked, record, lambda: self._db_now(cursor)
                    )
        finally:
            connection.close()

    def acquire_request_permit(
        self,
        *,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        """Reserve one direct attempt at a durable global maximum of 30 rpm."""

        if not callable(sleep_fn):
            raise TypeError("sleep_fn must be callable")
        while True:
            connection = self._connect()
            wait_seconds = 0.0
            try:
                with connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"INSERT INTO {self.RATE_TABLE} "
                            "(source, next_permit_at) "
                            "VALUES ('espn', clock_timestamp()) "
                            "ON CONFLICT (source) DO NOTHING"
                        )
                        cursor.execute(
                            f"SELECT next_permit_at, clock_timestamp() "
                            f"FROM {self.RATE_TABLE} WHERE source = 'espn' "
                            "FOR UPDATE"
                        )
                        row = cursor.fetchone()
                        if row is None:
                            raise OperationsError(
                                "ESPN request rate gate row is missing"
                            )
                        next_at = _utc(row[0], "next_permit_at")
                        database_now = _utc(row[1], "database clock")
                        if next_at > database_now:
                            wait_seconds = (next_at - database_now).total_seconds()
                        else:
                            cursor.execute(
                                f"UPDATE {self.RATE_TABLE} SET "
                                "next_permit_at = clock_timestamp() + "
                                "interval '2 seconds' WHERE source = 'espn'"
                            )
                            return
            finally:
                connection.close()
            sleep_fn(max(0.001, min(wait_seconds, 2.0)))


__all__ = [
    "LeaseConflict",
    "LeaseLost",
    "MemoryScopeLeaseStore",
    "OperationsError",
    "MAX_SUMMARY_EVENTS",
    "MAX_SCOPE_SUMMARY_EVENTS",
    "MemoryRequestPermitStore",
    "PostgresEspnControlStore",
    "PublicationFence",
    "SUMMARY_BATCH_SIZE",
    "ScopeLease",
    "ScopeLeaseStore",
    "ScopeHead",
    "RunManifestEvidence",
    "plan_summary_batches",
    "producer_state_failures",
    "reduce_raw_checkpoints",
    "seal_raw_batch_descriptor",
    "seal_raw_checkpoint",
    "validate_raw_checkpoint",
    "evaluate_alerts",
]
