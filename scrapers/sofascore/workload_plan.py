"""Deterministic, signed and restart-safe SofaScore paid workload plans.

The paid proxy budget is measured per *task shape*, not per free-form DAG run.
This module keeps that contract small and auditable:

* match IDs are partitioned into stable, source-tournament-scoped batches of
  at most 25;
* player IDs are partitioned into source-tournament-scoped batches of at most
  50, while the
  complete player universe is signed before any slicing;
* season work is measured separately for each tournament/shape pair;
* every network allocation is immutable and HMAC-signed for one DagRun;
* retries can spend only the remaining bytes of the original allocation.

There are deliberately no Airflow, browser or proxy imports here.  DAG, CLI
and backfill consumers can share this pure planning/accounting layer without
creating a second capture path.
"""

from __future__ import annotations

import fcntl
import base64
import hashlib
import hmac
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from scrapers.sofascore.runtime_fingerprint import (
    RuntimeFingerprintError,
    validate_runtime_fingerprint,
)


WORKLOAD_ARTIFACT_SCHEMA_VERSION = 2
WORKLOAD_PLAN_SCHEMA_VERSION = 2
ALLOCATION_LEDGER_SCHEMA_VERSION = 1
WORKLOAD_BUDGET_DERIVATION = "max_observed_task_bytes_per_workload_class_v2"
WORKLOAD_METER = "proxy_filter_provider_path_v2"
MIN_COLD_SAMPLES_PER_CLASS = 20
MIN_DISTINCT_EXITS_PER_CLASS = 5
MATCH_BATCH_SIZE = 25
PLAYER_BATCH_SIZE = 50
# Compatibility aliases for the existing EPL canary.  Production planning
# must call ``match_workload_class``/``player_workload_class`` with the source
# tournament ID; these names are intentionally *not* universal budgets.
MATCH_WORKLOAD_CLASS = "match_batch_25_t17"
PLAYER_WORKLOAD_CLASS = "player_batch_50_t17"
PLAYER_UNIVERSE_TASK_ID = "materialize_full_player_universe"
DQ_TASK_ID = "run_sofascore_dq"
CONTROL_TOKEN_ENV = "SOFASCORE_PROXY_CONTROL_TOKEN"
SIGNATURE_ALGORITHM = "hmac-sha256"
SEASON_WORKLOAD_SHAPE_VERSION = 1
SEASON_STATIC_ENDPOINTS = (
    "schedule_last",
    "schedule_next",
    "standings_total",
    "rounds",
    "cup_trees",
    "participants",
)
SEASON_DYNAMIC_ENDPOINTS = ("squads", "referee_profile")
WORKLOAD_FRESHNESS_SCOPES = ("season", "match", "player")


class WorkloadPolicyUnavailable(RuntimeError):
    """A measured workload class cannot safely authorize paid traffic."""


class WorkloadPlanError(ValueError):
    """A workload cannot be represented by one immutable safe plan."""


class WorkloadPlanSignatureError(WorkloadPlanError):
    """A DagRun plan was modified or signed with another control token."""


class AllocationError(RuntimeError):
    """Base class for allocation-ledger failures."""


class UnknownAllocation(AllocationError):
    """A task requested paid traffic that is absent from the signed plan."""


class DuplicateAllocation(AllocationError):
    """A plan or lease tried to reuse an already unique identifier."""


class ConcurrentAllocation(AllocationError):
    """Another attempt currently owns the allocation."""


class AllocationBudgetExceeded(AllocationError):
    """The next provider byte would cross an immutable task/run cap."""


class AllocationAccountingError(AllocationError):
    """Persisted allocation state or reported provider traffic is invalid."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise WorkloadPlanError("workload values must be canonical JSON") from exc


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WorkloadPolicyUnavailable(f"{field} must be a positive integer")
    return value


def _non_negative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AllocationAccountingError(f"{field} must be a non-negative integer")
    return value


def _required_token(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkloadPlanError(f"{field} must be a non-empty string")
    return value.strip()


def source_tournament_token(value: object) -> str:
    """Return the canonical positive SofaScore tournament ID token."""

    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise WorkloadPlanError("source_tournament_id must be a positive integer")
    token = str(value).strip()
    if not token.isdecimal() or int(token) <= 0 or token != str(int(token)):
        raise WorkloadPlanError("source_tournament_id must be a positive integer")
    return token


def match_workload_class(source_tournament_id: int | str) -> str:
    """Measured 25-match class for exactly one source tournament."""

    return f"match_batch_{MATCH_BATCH_SIZE}_t{source_tournament_token(source_tournament_id)}"


def player_workload_class(source_tournament_id: int | str) -> str:
    """Measured 50-player class for exactly one source tournament."""

    return f"player_batch_{PLAYER_BATCH_SIZE}_t{source_tournament_token(source_tournament_id)}"


def _normalize_freshness_keys(
    value: Optional[Mapping[str, object]],
) -> tuple[tuple[str, str], ...]:
    source = value or {scope: "final" for scope in WORKLOAD_FRESHNESS_SCOPES}
    if set(source) != set(WORKLOAD_FRESHNESS_SCOPES):
        raise WorkloadPlanError(
            "freshness_keys must contain exactly season, match and player"
        )
    return tuple(
        (scope, _required_token(source[scope], f"freshness_keys.{scope}"))
        for scope in WORKLOAD_FRESHNESS_SCOPES
    )


def _control_secret(value: Optional[str | bytes]) -> bytes:
    if value is None:
        value = os.environ.get(CONTROL_TOKEN_ENV, "")
    if isinstance(value, str):
        secret = value.encode("utf-8")
    elif isinstance(value, bytes):
        secret = value
    else:
        raise WorkloadPlanSignatureError("proxy control token must be bytes or text")
    if len(secret) < 32:
        raise WorkloadPlanSignatureError(
            f"{CONTROL_TOKEN_ENV} must contain at least 32 bytes"
        )
    return secret


def _stable_id_key(value: str) -> tuple[int, int | str, str]:
    if value.isdecimal():
        return (0, int(value), value)
    return (1, value, value)


def _normalize_ids(values: Sequence[int | str], field: str) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise WorkloadPlanError(f"{field} values must be integer or string IDs")
        token = str(value).strip()
        if not token:
            raise WorkloadPlanError(f"{field} values must not be empty")
        normalized.append(token)
    if len(normalized) != len(set(normalized)):
        raise WorkloadPlanError(f"{field} contains duplicate IDs")
    return tuple(sorted(normalized, key=_stable_id_key))


def stable_partitions(
    values: Sequence[int | str],
    batch_size: int,
    *,
    field: str = "ids",
) -> tuple[tuple[str, ...], ...]:
    """Return a stable exact partition; every input ID appears exactly once."""

    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size < 1
    ):
        raise WorkloadPlanError("batch_size must be a positive integer")
    normalized = _normalize_ids(values, field)
    return tuple(
        normalized[index : index + batch_size]
        for index in range(0, len(normalized), batch_size)
    )


def qualify_work_unit(partition_key: str, target_id: int | str) -> str:
    """Encode a competition/season target as one opaque signed unit."""

    partition = _required_token(partition_key, "partition_key")
    if isinstance(target_id, bool) or not isinstance(target_id, (int, str)):
        raise WorkloadPlanError("target_id must be an integer or string")
    target = _required_token(str(target_id), "target_id")
    encoded = (
        base64.urlsafe_b64encode(
            _canonical_json({"partition": partition, "target_id": target})
        )
        .decode("ascii")
        .rstrip("=")
    )
    return "q1." + encoded


def parse_qualified_work_unit(value: str) -> tuple[str, str]:
    token = _required_token(value, "qualified unit")
    if not token.startswith("q1."):
        raise WorkloadPlanError("qualified unit has an unsupported version")
    encoded = token[3:]
    try:
        raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkloadPlanError("qualified unit is corrupt") from exc
    if not isinstance(payload, Mapping) or set(payload) != {"partition", "target_id"}:
        raise WorkloadPlanError("qualified unit has invalid fields")
    return (
        _required_token(payload["partition"], "qualified unit partition"),
        _required_token(payload["target_id"], "qualified unit target_id"),
    )


def season_shape_digest(shape: Mapping[str, object]) -> str:
    if not isinstance(shape, Mapping) or not shape:
        raise WorkloadPlanError("season workload shape must be a non-empty object")
    return hashlib.sha256(_canonical_json(dict(shape))).hexdigest()


def tournament_canonical_url(slug: str, tournament_id: int | str) -> str:
    """Stable warm anchor shared by production and measured canary classes."""

    normalized_slug = _required_token(slug, "tournament slug")
    if any(
        not (character.isalnum() or character in {"-", "_"})
        for character in normalized_slug
    ):
        raise WorkloadPlanError("tournament slug contains unsafe URL characters")
    normalized_id = _required_token(str(tournament_id), "tournament_id")
    if not normalized_id.isdecimal() or int(normalized_id) <= 0:
        raise WorkloadPlanError("tournament_id must be a positive integer")
    return (
        "https://www.sofascore.com/tournament/"
        f"{normalized_slug}/{normalized_id}"
    )


def production_season_shape(
    tournament_id: int | str,
    *,
    season_format: str,
    max_pages_per_direction: int,
) -> Mapping[str, object]:
    """Canonical bounded shape shared by the DAG and the paid canary.

    Schedule chains and evidence-derived squads/referees are dynamic, so the
    class describes their deterministic algorithm and hard page bound.  If a
    future season costs more than the measured maximum, the proxy stops the
    task; operators must collect a new class sample instead of applying a
    guessed multiplier.
    """

    tournament = _required_token(str(tournament_id), "tournament_id")
    season_format = _required_token(season_format, "season_format")
    if season_format not in {"split_year", "calendar_year", "named"}:
        raise WorkloadPlanError("season_format is not capture-safe")
    if (
        isinstance(max_pages_per_direction, bool)
        or not isinstance(max_pages_per_direction, int)
        or max_pages_per_direction < 1
    ):
        raise WorkloadPlanError("max_pages_per_direction must be positive")
    return {
        "shape_version": SEASON_WORKLOAD_SHAPE_VERSION,
        "source_tournament_id": tournament,
        "season_format": season_format,
        "schedule_page_chain": {
            "directions": ["last", "next"],
            "max_pages_per_direction": max_pages_per_direction,
        },
        "static_endpoints": list(SEASON_STATIC_ENDPOINTS),
        "dynamic_endpoints": list(SEASON_DYNAMIC_ENDPOINTS),
        "dynamic_evidence": "stored_schedule_participants_squads_events_v1",
    }


def season_workload_class(
    tournament_id: int | str,
    shape: Mapping[str, object],
) -> str:
    tournament = _required_token(str(tournament_id), "tournament_id")
    return f"season_{tournament}_{season_shape_digest(shape)[:16]}"


@dataclass(frozen=True)
class WorkloadClassBudget:
    name: str
    scope: str
    max_units: int
    hard_task_bytes: int
    required_endpoints: tuple[str, ...]
    sample_count: int
    distinct_proxy_exits: int
    shape_digest: Optional[str] = None
    source_tournament_id: Optional[str] = None


@dataclass(frozen=True)
class WorkloadBudgetPolicy:
    artifact_id: str
    classes: Mapping[str, WorkloadClassBudget]

    def class_for(
        self,
        name: str,
        *,
        scope: str,
        units: int,
        source_tournament_id: int | str,
    ) -> WorkloadClassBudget:
        try:
            policy = self.classes[name]
        except KeyError as exc:
            raise WorkloadPolicyUnavailable(
                f"verified workload artifact has no class {name!r}"
            ) from exc
        if policy.scope != scope:
            raise WorkloadPolicyUnavailable(
                f"workload class {name!r} belongs to {policy.scope!r}, not {scope!r}"
            )
        tournament = source_tournament_token(source_tournament_id)
        if policy.source_tournament_id != tournament:
            raise WorkloadPolicyUnavailable(
                f"workload class {name!r} is measured for tournament "
                f"{policy.source_tournament_id!r}, not {tournament!r}"
            )
        if isinstance(units, bool) or not isinstance(units, int) or units < 1:
            raise WorkloadPlanError("allocation units must be a positive integer")
        if units > policy.max_units:
            raise WorkloadPlanError(
                f"{name!r} permits at most {policy.max_units} units, got {units}"
            )
        return policy


def _validate_request_map(
    value: object,
    *,
    required_endpoints: tuple[str, ...],
    field: str,
) -> int:
    if not isinstance(value, Mapping):
        raise WorkloadPolicyUnavailable(f"{field} must be an endpoint object")
    endpoints = set(value)
    if endpoints != set(required_endpoints):
        missing = sorted(set(required_endpoints) - endpoints)
        extra = sorted(endpoints - set(required_endpoints))
        raise WorkloadPolicyUnavailable(
            f"{field} endpoint mismatch: missing={missing} extra={extra}"
        )
    total = 0
    for endpoint in required_endpoints:
        observations = value[endpoint]
        if not isinstance(observations, list) or not observations:
            raise WorkloadPolicyUnavailable(
                f"{field}.{endpoint} must be a non-empty request-byte list"
            )
        for observation in observations:
            if (
                isinstance(observation, bool)
                or not isinstance(observation, int)
                or observation < 0
            ):
                raise WorkloadPolicyUnavailable(
                    f"{field}.{endpoint} request bytes must be non-negative integers"
                )
            total += observation
    return total


def load_verified_workload_policy(
    path: os.PathLike[str] | str,
) -> WorkloadBudgetPolicy:
    """Load v2 class budgets and derive every cap from measured cold maxima."""

    artifact_path = Path(path)
    try:
        raw = artifact_path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkloadPolicyUnavailable(
            f"invalid workload artifact: {artifact_path}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise WorkloadPolicyUnavailable("workload artifact must be an object")
    if payload.get("schema_version") != WORKLOAD_ARTIFACT_SCHEMA_VERSION:
        raise WorkloadPolicyUnavailable("workload artifact schema_version must be 2")
    if payload.get("source") != "sofascore":
        raise WorkloadPolicyUnavailable("workload artifact source must be sofascore")
    if payload.get("meter") != WORKLOAD_METER:
        raise WorkloadPolicyUnavailable("workload artifact uses an untrusted meter")
    if payload.get("budget_derivation") != WORKLOAD_BUDGET_DERIVATION:
        raise WorkloadPolicyUnavailable(
            f"budget_derivation must be {WORKLOAD_BUDGET_DERIVATION}"
        )
    if payload.get("verified") is not True:
        raise WorkloadPolicyUnavailable("workload artifact is not verified")
    try:
        current_fingerprint = validate_runtime_fingerprint(
            payload.get("runtime_fingerprint")
        )
    except RuntimeFingerprintError as exc:
        raise WorkloadPolicyUnavailable(str(exc)) from exc
    runtime_digest = current_fingerprint["digest"]
    if "budget_multiplier" in payload:
        raise WorkloadPolicyUnavailable("workload budgets cannot use a multiplier")
    raw_classes = payload.get("workload_classes")
    if not isinstance(raw_classes, Mapping) or not raw_classes:
        raise WorkloadPolicyUnavailable("workload_classes must be a non-empty object")

    classes: dict[str, WorkloadClassBudget] = {}
    for raw_name, raw_class in sorted(raw_classes.items()):
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise WorkloadPolicyUnavailable("workload class names must be non-empty")
        name = raw_name.strip()
        if not isinstance(raw_class, Mapping):
            raise WorkloadPolicyUnavailable(
                f"workload class {name!r} must be an object"
            )
        if "budget_multiplier" in raw_class:
            raise WorkloadPolicyUnavailable(
                f"workload class {name!r} cannot use a multiplier"
            )
        scope = raw_class.get("scope")
        if scope not in {"match", "player", "season"}:
            raise WorkloadPolicyUnavailable(
                f"workload class {name!r} has invalid scope"
            )
        try:
            source_tournament_id = source_tournament_token(
                raw_class.get("source_tournament_id")
            )
        except WorkloadPlanError as exc:
            raise WorkloadPolicyUnavailable(
                f"workload class {name!r} needs one source_tournament_id"
            ) from exc
        max_units = _positive_int(raw_class.get("max_units"), f"{name}.max_units")
        if scope == "match" and (
            name != match_workload_class(source_tournament_id)
            or max_units != MATCH_BATCH_SIZE
        ):
            raise WorkloadPolicyUnavailable(
                "match workload class must bind batch size and source tournament"
            )
        if scope == "player" and (
            name != player_workload_class(source_tournament_id)
            or max_units != PLAYER_BATCH_SIZE
        ):
            raise WorkloadPolicyUnavailable(
                "player workload class must bind batch size and source tournament"
            )
        if scope == "season" and max_units != 1:
            raise WorkloadPolicyUnavailable(
                "each season allocation must contain one season"
            )
        raw_endpoints = raw_class.get("required_endpoints")
        if (
            not isinstance(raw_endpoints, list)
            or not raw_endpoints
            or any(
                not isinstance(item, str) or not item.strip() for item in raw_endpoints
            )
        ):
            raise WorkloadPolicyUnavailable(
                f"{name}.required_endpoints must be a non-empty string list"
            )
        required_endpoints = tuple(sorted(item.strip() for item in raw_endpoints))
        if len(required_endpoints) != len(set(required_endpoints)):
            raise WorkloadPolicyUnavailable(
                f"{name}.required_endpoints contains duplicates"
            )
        samples = raw_class.get("samples")
        if not isinstance(samples, list) or len(samples) < MIN_COLD_SAMPLES_PER_CLASS:
            raise WorkloadPolicyUnavailable(
                f"{name!r} needs at least {MIN_COLD_SAMPLES_PER_CLASS} cold samples"
            )
        run_ids: set[str] = set()
        exits: set[str] = set()
        totals: list[int] = []
        for index, sample in enumerate(samples):
            prefix = f"{name}.samples[{index}]"
            if not isinstance(sample, Mapping):
                raise WorkloadPolicyUnavailable(f"{prefix} must be an object")
            evidence = sample.get("evidence")
            if (
                not isinstance(evidence, Mapping)
                or evidence.get("runtime_fingerprint_digest") != runtime_digest
            ):
                raise WorkloadPolicyUnavailable(
                    f"{prefix} was measured with another runtime fingerprint"
                )
            run_id = sample.get("run_id")
            if not isinstance(run_id, str) or not run_id.strip() or run_id in run_ids:
                raise WorkloadPolicyUnavailable(
                    f"{name!r} sample run_id values must be non-empty and unique"
                )
            run_ids.add(run_id)
            if sample.get("workload_class") != name:
                raise WorkloadPolicyUnavailable(
                    f"{prefix}.workload_class must equal {name!r}"
                )
            try:
                sample_tournament_id = source_tournament_token(
                    sample.get("source_tournament_id")
                )
            except WorkloadPlanError as exc:
                raise WorkloadPolicyUnavailable(
                    f"{prefix}.source_tournament_id is invalid"
                ) from exc
            if sample_tournament_id != source_tournament_id:
                raise WorkloadPolicyUnavailable(
                    f"{prefix} was measured for tournament "
                    f"{sample_tournament_id!r}, not {source_tournament_id!r}"
                )
            if (
                sample.get("mode") != "cold"
                or sample.get("budget_eligible") is not True
            ):
                raise WorkloadPolicyUnavailable(
                    f"{prefix} must be an eligible cold observation"
                )
            if sample.get("units") != max_units:
                raise WorkloadPolicyUnavailable(
                    f"{prefix}.units must equal measured max_units={max_units}"
                )
            exit_hash = sample.get("proxy_exit_hash")
            if not isinstance(exit_hash, str) or len(exit_hash) < 12:
                raise WorkloadPolicyUnavailable(
                    f"{prefix}.proxy_exit_hash must be anonymized"
                )
            exits.add(exit_hash)
            measured_total = _validate_request_map(
                sample.get("endpoint_request_provider_bytes"),
                required_endpoints=required_endpoints,
                field=f"{prefix}.endpoint_request_provider_bytes",
            )
            total = sample.get("total_provider_bytes")
            if (
                isinstance(total, bool)
                or not isinstance(total, int)
                or total <= 0
                or total != measured_total
            ):
                raise WorkloadPolicyUnavailable(
                    f"{prefix}.total_provider_bytes must equal the exact request map"
                )
            totals.append(total)
        if len(exits) < MIN_DISTINCT_EXITS_PER_CLASS:
            raise WorkloadPolicyUnavailable(
                f"{name!r} needs at least {MIN_DISTINCT_EXITS_PER_CLASS} distinct exits"
            )
        observed_max = max(totals)
        if raw_class.get("hard_task_bytes") != observed_max:
            raise WorkloadPolicyUnavailable(
                f"{name!r}.hard_task_bytes must equal max observed bytes {observed_max}"
            )
        raw_shape_digest = raw_class.get("shape_digest")
        if scope == "season":
            if (
                not isinstance(raw_shape_digest, str)
                or len(raw_shape_digest) != 64
                or any(char not in "0123456789abcdef" for char in raw_shape_digest)
                or not name.endswith(raw_shape_digest[:16])
            ):
                raise WorkloadPolicyUnavailable(
                    f"season class {name!r} needs its exact shape_digest"
                )
            raw_shape = raw_class.get("shape")
            if not isinstance(raw_shape, Mapping):
                raise WorkloadPolicyUnavailable(
                    f"season class {name!r} needs its measured shape"
                )
            try:
                shape_tournament_id = source_tournament_token(
                    raw_shape.get("source_tournament_id")
                )
            except WorkloadPlanError as exc:
                raise WorkloadPolicyUnavailable(
                    f"season class {name!r} shape has no source tournament"
                ) from exc
            if shape_tournament_id != source_tournament_id:
                raise WorkloadPolicyUnavailable(
                    f"season class {name!r} shape belongs to another tournament"
                )
            if season_shape_digest(raw_shape) != raw_shape_digest:
                raise WorkloadPolicyUnavailable(
                    f"season class {name!r} shape_digest does not match its shape"
                )
            shape_digest_value: Optional[str] = raw_shape_digest
        else:
            if raw_shape_digest is not None:
                raise WorkloadPolicyUnavailable(
                    f"non-season class {name!r} cannot declare shape_digest"
                )
            shape_digest_value = None
        classes[name] = WorkloadClassBudget(
            name=name,
            scope=scope,
            max_units=max_units,
            hard_task_bytes=observed_max,
            required_endpoints=required_endpoints,
            sample_count=len(samples),
            distinct_proxy_exits=len(exits),
            shape_digest=shape_digest_value,
            source_tournament_id=source_tournament_id,
        )
    return WorkloadBudgetPolicy(
        artifact_id=hashlib.sha256(raw).hexdigest(),
        classes=classes,
    )


@dataclass(frozen=True)
class SeasonWorkload:
    tournament_id: int | str
    season_id: int | str
    shape: Mapping[str, object]
    pending: bool = True

    @property
    def workload_class(self) -> str:
        return season_workload_class(self.tournament_id, self.shape)

    @property
    def shape_digest(self) -> str:
        return season_shape_digest(self.shape)

    @property
    def unit(self) -> str:
        tournament = _required_token(str(self.tournament_id), "tournament_id")
        season = _required_token(str(self.season_id), "season_id")
        return f"{tournament}:{season}"


@dataclass(frozen=True)
class WorkloadAllocation:
    allocation_id: str
    task_id: str
    scope: str
    workload_class: str
    batch_index: int
    units: tuple[str, ...]
    budget_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "allocation_id": self.allocation_id,
            "task_id": self.task_id,
            "scope": self.scope,
            "class": self.workload_class,
            "batch_index": self.batch_index,
            "units": list(self.units),
            "budget": self.budget_bytes,
        }


@dataclass(frozen=True)
class AllocationRequest:
    """One deterministic batch prepared from a network-free manifest snapshot."""

    task_id: str
    scope: str
    workload_class: str
    batch_index: int
    units: tuple[str, ...]
    source_tournament_id: int | str


def _allocation_from_dict(value: object) -> WorkloadAllocation:
    if not isinstance(value, Mapping):
        raise WorkloadPlanError("plan allocations must be objects")
    units = value.get("units")
    if not isinstance(units, list):
        raise WorkloadPlanError("allocation units must be an array")
    allocation = WorkloadAllocation(
        allocation_id=_required_token(value.get("allocation_id"), "allocation_id"),
        task_id=_required_token(value.get("task_id"), "task_id"),
        scope=_required_token(value.get("scope"), "scope"),
        workload_class=_required_token(value.get("class"), "class"),
        batch_index=value.get("batch_index"),  # type: ignore[arg-type]
        units=tuple(_required_token(item, "unit") for item in units),
        budget_bytes=value.get("budget"),  # type: ignore[arg-type]
    )
    if (
        isinstance(allocation.batch_index, bool)
        or not isinstance(allocation.batch_index, int)
        or allocation.batch_index < 0
    ):
        raise WorkloadPlanError("allocation batch_index must be non-negative")
    if (
        isinstance(allocation.budget_bytes, bool)
        or not isinstance(allocation.budget_bytes, int)
        or allocation.budget_bytes <= 0
    ):
        raise WorkloadPlanError("allocation budget must be positive")
    if not allocation.units or len(allocation.units) != len(set(allocation.units)):
        raise WorkloadPlanError("allocation units must be non-empty and unique")
    if allocation.scope not in {"match", "player", "season"}:
        raise WorkloadPlanError("allocation scope is invalid")
    return allocation


@dataclass(frozen=True)
class SignedDagRunPlan:
    artifact_id: str
    dag_id: str
    run_id: str
    freshness_keys: tuple[tuple[str, str], ...]
    player_universe_ids: tuple[str, ...]
    allocations: tuple[WorkloadAllocation, ...]
    run_cap_bytes: int
    dq_dependencies: tuple[str, ...]
    plan_digest: str
    signature: str

    @property
    def requires_proxy_lease(self) -> bool:
        return bool(self.allocations)

    @property
    def player_universe_digest(self) -> str:
        return hashlib.sha256(
            _canonical_json(list(self.player_universe_ids))
        ).hexdigest()

    def freshness_key(self, scope: str) -> str:
        normalized = _required_token(scope, "freshness scope")
        try:
            return dict(self.freshness_keys)[normalized]
        except KeyError as exc:
            raise WorkloadPlanError(
                f"signed workload plan has no {normalized!r} freshness key"
            ) from exc

    def unsigned_dict(self) -> dict[str, object]:
        return {
            "schema_version": WORKLOAD_PLAN_SCHEMA_VERSION,
            "workload_artifact_schema_version": WORKLOAD_ARTIFACT_SCHEMA_VERSION,
            "artifact_id": self.artifact_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "freshness_keys": dict(self.freshness_keys),
            "player_universe": {
                "task_id": PLAYER_UNIVERSE_TASK_ID,
                "ids": list(self.player_universe_ids),
                "count": len(self.player_universe_ids),
                "sha256": self.player_universe_digest,
            },
            "allocations": [allocation.to_dict() for allocation in self.allocations],
            "run_cap_bytes": self.run_cap_bytes,
            "dq": {
                "task_id": DQ_TASK_ID,
                "depends_on": list(self.dq_dependencies),
            },
            "signature_algorithm": SIGNATURE_ALGORITHM,
        }

    def to_dict(self) -> dict[str, object]:
        payload = self.unsigned_dict()
        payload["plan_digest"] = self.plan_digest
        payload["signature"] = self.signature
        return payload

    def verify(self, control_token: Optional[str | bytes] = None) -> None:
        expected_digest = hashlib.sha256(
            _canonical_json(self.unsigned_dict())
        ).hexdigest()
        if not hmac.compare_digest(self.plan_digest, expected_digest):
            raise WorkloadPlanSignatureError(
                "DagRun plan digest does not match its body"
            )
        signed = self.unsigned_dict()
        signed["plan_digest"] = self.plan_digest
        expected_signature = hmac.new(
            _control_secret(control_token),
            _canonical_json(signed),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(self.signature, expected_signature):
            raise WorkloadPlanSignatureError("DagRun plan HMAC signature is invalid")
        _validate_plan_invariants(self)

    @classmethod
    def from_dict(
        cls,
        value: object,
        *,
        control_token: Optional[str | bytes] = None,
    ) -> "SignedDagRunPlan":
        if not isinstance(value, Mapping):
            raise WorkloadPlanError("DagRun plan must be an object")
        if value.get("schema_version") != WORKLOAD_PLAN_SCHEMA_VERSION:
            raise WorkloadPlanError("unsupported DagRun workload plan schema")
        if value.get("workload_artifact_schema_version") != 2:
            raise WorkloadPlanError("DagRun plan is not tied to a v2 artifact")
        universe = value.get("player_universe")
        dq = value.get("dq")
        allocations = value.get("allocations")
        freshness_keys = value.get("freshness_keys")
        if not isinstance(universe, Mapping) or not isinstance(dq, Mapping):
            raise WorkloadPlanError("DagRun plan is missing universe/DQ barriers")
        if not isinstance(allocations, list):
            raise WorkloadPlanError("DagRun allocations must be an array")
        if not isinstance(freshness_keys, Mapping):
            raise WorkloadPlanError("DagRun plan is missing signed freshness keys")
        universe_ids = universe.get("ids")
        dependencies = dq.get("depends_on")
        if not isinstance(universe_ids, list) or not isinstance(dependencies, list):
            raise WorkloadPlanError("DagRun universe/dependencies must be arrays")
        if universe.get("task_id") != PLAYER_UNIVERSE_TASK_ID:
            raise WorkloadPlanError("unexpected player universe task")
        if dq.get("task_id") != DQ_TASK_ID:
            raise WorkloadPlanError("unexpected DQ task")
        plan = cls(
            artifact_id=_required_token(value.get("artifact_id"), "artifact_id"),
            dag_id=_required_token(value.get("dag_id"), "dag_id"),
            run_id=_required_token(value.get("run_id"), "run_id"),
            freshness_keys=_normalize_freshness_keys(freshness_keys),
            player_universe_ids=tuple(
                _required_token(item, "player_universe.id") for item in universe_ids
            ),
            allocations=tuple(_allocation_from_dict(item) for item in allocations),
            run_cap_bytes=value.get("run_cap_bytes"),  # type: ignore[arg-type]
            dq_dependencies=tuple(
                _required_token(item, "dq.depends_on") for item in dependencies
            ),
            plan_digest=_required_token(value.get("plan_digest"), "plan_digest"),
            signature=_required_token(value.get("signature"), "signature"),
        )
        if value.get("signature_algorithm") != SIGNATURE_ALGORITHM:
            raise WorkloadPlanError("unsupported plan signature algorithm")
        if universe.get("count") != len(plan.player_universe_ids):
            raise WorkloadPlanError("player universe count does not match its IDs")
        if universe.get("sha256") != plan.player_universe_digest:
            raise WorkloadPlanError("player universe digest does not match its IDs")
        plan.verify(control_token)
        return plan


def _validate_plan_invariants(plan: SignedDagRunPlan) -> None:
    if len(plan.artifact_id) != 64 or any(
        char not in "0123456789abcdef" for char in plan.artifact_id
    ):
        raise WorkloadPlanError("artifact_id must be a SHA-256 digest")
    if plan.player_universe_ids != _normalize_ids(
        plan.player_universe_ids, "player_universe_ids"
    ):
        raise WorkloadPlanError("player universe must use stable sorted IDs")
    if plan.freshness_keys != _normalize_freshness_keys(dict(plan.freshness_keys)):
        raise WorkloadPlanError("freshness keys must use canonical scope order")
    allocation_ids = [item.allocation_id for item in plan.allocations]
    task_ids = [item.task_id for item in plan.allocations]
    if len(allocation_ids) != len(set(allocation_ids)):
        raise DuplicateAllocation("allocation_id values must be unique")
    if len(task_ids) != len(set(task_ids)):
        raise DuplicateAllocation("paid task_id values must be unique")
    signed_universe = set(plan.player_universe_ids)
    unplanned_player_units = sorted(
        {
            unit
            for allocation in plan.allocations
            if allocation.scope == "player"
            for unit in allocation.units
        }
        - signed_universe,
        key=_stable_id_key,
    )
    if unplanned_player_units:
        raise WorkloadPlanError(
            "player allocation contains IDs outside the signed universe: "
            + ", ".join(unplanned_player_units[:5])
        )
    expected_cap = sum(item.budget_bytes for item in plan.allocations)
    if plan.run_cap_bytes != expected_cap:
        raise WorkloadPlanError("run cap must equal the sum of unique allocation caps")
    expected_dependencies = tuple(
        ([PLAYER_UNIVERSE_TASK_ID] if plan.player_universe_ids else []) + task_ids
    )
    if plan.dq_dependencies != expected_dependencies:
        raise WorkloadPlanError("DQ must wait for the full universe and every batch")


def _signed_plan(
    *,
    artifact_id: str,
    dag_id: str,
    run_id: str,
    freshness_keys: Optional[Mapping[str, object]] = None,
    player_universe_ids: tuple[str, ...],
    allocations: tuple[WorkloadAllocation, ...],
    control_token: Optional[str | bytes],
) -> SignedDagRunPlan:
    normalized_freshness = _normalize_freshness_keys(freshness_keys)
    dependency_tasks = tuple(
        ([PLAYER_UNIVERSE_TASK_ID] if player_universe_ids else [])
        + [allocation.task_id for allocation in allocations]
    )
    provisional = SignedDagRunPlan(
        artifact_id=artifact_id,
        dag_id=dag_id,
        run_id=run_id,
        freshness_keys=normalized_freshness,
        player_universe_ids=player_universe_ids,
        allocations=allocations,
        run_cap_bytes=sum(item.budget_bytes for item in allocations),
        dq_dependencies=dependency_tasks,
        plan_digest="pending",
        signature="pending",
    )
    digest = hashlib.sha256(_canonical_json(provisional.unsigned_dict())).hexdigest()
    signed = provisional.unsigned_dict()
    signed["plan_digest"] = digest
    signature = hmac.new(
        _control_secret(control_token),
        _canonical_json(signed),
        hashlib.sha256,
    ).hexdigest()
    result = SignedDagRunPlan(
        artifact_id=artifact_id,
        dag_id=dag_id,
        run_id=run_id,
        freshness_keys=normalized_freshness,
        player_universe_ids=player_universe_ids,
        allocations=allocations,
        run_cap_bytes=provisional.run_cap_bytes,
        dq_dependencies=dependency_tasks,
        plan_digest=digest,
        signature=signature,
    )
    result.verify(control_token)
    return result


def build_signed_allocation_plan(
    policy: WorkloadBudgetPolicy,
    *,
    dag_id: str,
    run_id: str,
    freshness_keys: Optional[Mapping[str, object]] = None,
    player_universe_ids: Sequence[int | str],
    requests: Sequence[AllocationRequest],
    control_token: Optional[str | bytes] = None,
) -> SignedDagRunPlan:
    """Sign already-grouped batches without allowing a caller-chosen budget.

    The production DAG uses this form because each competition is a separate
    Airflow task.  The planner groups IDs first, so a 25-match or 50-player
    allocation can never straddle two competition/season partitions.
    """

    dag_id = _required_token(dag_id, "dag_id")
    run_id = _required_token(run_id, "run_id")
    universe = _normalize_ids(player_universe_ids, "player_universe_ids")
    allocations: list[WorkloadAllocation] = []
    seen_tasks: set[str] = set()
    for request in requests:
        task_id = _required_token(request.task_id, "task_id")
        if task_id in seen_tasks:
            raise DuplicateAllocation(f"duplicate paid task_id {task_id!r}")
        seen_tasks.add(task_id)
        scope = _required_token(request.scope, "scope")
        workload_class = _required_token(request.workload_class, "class")
        if scope not in {"match", "player", "season"}:
            raise WorkloadPlanError(f"invalid allocation scope {scope!r}")
        if (
            isinstance(request.batch_index, bool)
            or not isinstance(request.batch_index, int)
            or request.batch_index < 0
        ):
            raise WorkloadPlanError("batch_index must be non-negative")
        units = _normalize_ids(request.units, f"{task_id}.units")
        source_tournament_id = source_tournament_token(
            request.source_tournament_id
        )
        measured = policy.class_for(
            workload_class,
            scope=scope,
            units=len(units),
            source_tournament_id=source_tournament_id,
        )
        identity = {
            "artifact_id": policy.artifact_id,
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "scope": scope,
            "class": workload_class,
            "batch_index": request.batch_index,
            "units": list(units),
            "budget": measured.hard_task_bytes,
        }
        allocations.append(
            WorkloadAllocation(
                allocation_id="alloc-"
                + hashlib.sha256(_canonical_json(identity)).hexdigest()[:32],
                task_id=task_id,
                scope=scope,
                workload_class=workload_class,
                batch_index=request.batch_index,
                units=units,
                budget_bytes=measured.hard_task_bytes,
            )
        )
    return _signed_plan(
        artifact_id=policy.artifact_id,
        dag_id=dag_id,
        run_id=run_id,
        freshness_keys=freshness_keys,
        player_universe_ids=universe,
        allocations=tuple(allocations),
        control_token=control_token,
    )


def build_signed_dagrun_plan(
    policy: WorkloadBudgetPolicy,
    *,
    dag_id: str,
    run_id: str,
    freshness_keys: Optional[Mapping[str, object]] = None,
    pending_match_ids: Sequence[int | str] = (),
    player_universe_ids: Sequence[int | str] = (),
    pending_player_ids: Optional[Sequence[int | str]] = None,
    season_workloads: Sequence[SeasonWorkload] = (),
    source_tournament_id: Optional[int | str] = None,
    control_token: Optional[str | bytes] = None,
) -> SignedDagRunPlan:
    """Build the one immutable paid-work snapshot for DAG, CLI or backfill."""

    dag_id = _required_token(dag_id, "dag_id")
    run_id = _required_token(run_id, "run_id")
    universe = _normalize_ids(player_universe_ids, "player_universe_ids")
    pending_players = _normalize_ids(
        universe if pending_player_ids is None else pending_player_ids,
        "pending_player_ids",
    )
    unknown_players = sorted(set(pending_players) - set(universe), key=_stable_id_key)
    if unknown_players:
        raise WorkloadPlanError(
            "pending players are absent from the full pre-slice universe: "
            + ", ".join(unknown_players[:5])
        )

    allocation_inputs: list[tuple[str, str, int, tuple[str, ...], int]] = []
    tournament = (
        source_tournament_token(source_tournament_id)
        if source_tournament_id is not None
        else None
    )
    if (pending_match_ids or pending_players) and tournament is None:
        raise WorkloadPlanError(
            "source_tournament_id is required for match/player allocations"
        )
    for batch_index, units in enumerate(
        stable_partitions(
            pending_match_ids, MATCH_BATCH_SIZE, field="pending_match_ids"
        )
    ):
        assert tournament is not None
        workload_class = match_workload_class(tournament)
        measured = policy.class_for(
            workload_class,
            scope="match",
            units=len(units),
            source_tournament_id=tournament,
        )
        allocation_inputs.append(
            (
                "match",
                workload_class,
                batch_index,
                units,
                measured.hard_task_bytes,
            )
        )
    for batch_index, units in enumerate(
        stable_partitions(
            pending_players, PLAYER_BATCH_SIZE, field="pending_player_ids"
        )
    ):
        assert tournament is not None
        workload_class = player_workload_class(tournament)
        measured = policy.class_for(
            workload_class,
            scope="player",
            units=len(units),
            source_tournament_id=tournament,
        )
        allocation_inputs.append(
            (
                "player",
                workload_class,
                batch_index,
                units,
                measured.hard_task_bytes,
            )
        )

    seen_seasons: set[str] = set()
    ordered_seasons = sorted(
        (item for item in season_workloads if item.pending),
        key=lambda item: _stable_id_key(item.unit),
    )
    season_index = 0
    for workload in ordered_seasons:
        if workload.unit in seen_seasons:
            raise WorkloadPlanError(f"duplicate season workload {workload.unit!r}")
        seen_seasons.add(workload.unit)
        measured = policy.class_for(
            workload.workload_class,
            scope="season",
            units=1,
            source_tournament_id=workload.tournament_id,
        )
        if measured.shape_digest != workload.shape_digest:
            raise WorkloadPolicyUnavailable(
                f"season class {workload.workload_class!r} shape is not measured"
            )
        allocation_inputs.append(
            (
                "season",
                workload.workload_class,
                season_index,
                (workload.unit,),
                measured.hard_task_bytes,
            )
        )
        season_index += 1

    allocations: list[WorkloadAllocation] = []
    scope_counts: dict[str, int] = {"match": 0, "player": 0, "season": 0}
    for scope, workload_class, batch_index, units, budget in allocation_inputs:
        scope_index = scope_counts[scope]
        scope_counts[scope] += 1
        task_id = f"capture_{scope}_batch_{scope_index:05d}"
        identity = {
            "artifact_id": policy.artifact_id,
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "scope": scope,
            "class": workload_class,
            "batch_index": batch_index,
            "units": list(units),
            "budget": budget,
        }
        allocation_id = (
            "alloc-" + hashlib.sha256(_canonical_json(identity)).hexdigest()[:32]
        )
        allocations.append(
            WorkloadAllocation(
                allocation_id=allocation_id,
                task_id=task_id,
                scope=scope,
                workload_class=workload_class,
                batch_index=batch_index,
                units=units,
                budget_bytes=budget,
            )
        )
    return _signed_plan(
        artifact_id=policy.artifact_id,
        dag_id=dag_id,
        run_id=run_id,
        freshness_keys=freshness_keys,
        player_universe_ids=universe,
        allocations=tuple(allocations),
        control_token=control_token,
    )


@dataclass(frozen=True, repr=False)
class AllocationClaim:
    artifact_id: str
    dag_id: str
    run_id: str
    plan_digest: str
    allocation_id: str
    task_id: str
    scope: str
    workload_class: str
    batch_index: int
    allocation_budget_bytes: int
    spent_provider_bytes: int
    remaining_provider_bytes: int
    claim_token: str

    def __repr__(self) -> str:
        return (
            "AllocationClaim("
            f"allocation_id={self.allocation_id!r}, task_id={self.task_id!r}, "
            f"remaining_provider_bytes={self.remaining_provider_bytes})"
        )


class AllocationLedger:
    """Atomic allocation ownership and provider accounting across retries."""

    def __init__(
        self,
        path: os.PathLike[str] | str,
        *,
        control_token: Optional[str | bytes] = None,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self._secret = _control_secret(control_token)

    def _locked(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+")
        os.fchmod(handle.fileno(), 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": ALLOCATION_LEDGER_SCHEMA_VERSION, "runs": {}}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AllocationAccountingError("allocation ledger is corrupt") from exc
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != ALLOCATION_LEDGER_SCHEMA_VERSION
            or not isinstance(value.get("runs"), dict)
        ):
            raise AllocationAccountingError("unsupported allocation ledger")
        return value

    def _write(self, value: Mapping[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(
            f"{self.path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        )
        try:
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                    stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            except BaseException:
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

    @staticmethod
    def _run_key(plan: SignedDagRunPlan) -> str:
        return hashlib.sha256(
            f"{plan.dag_id}\0{plan.run_id}".encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _token_hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _verify(self, plan: SignedDagRunPlan) -> None:
        plan.verify(self._secret)

    def _run(self, payload: dict[str, Any], plan: SignedDagRunPlan) -> dict[str, Any]:
        key = self._run_key(plan)
        existing = payload["runs"].get(key)
        if existing is None:
            allocations = {
                item.allocation_id: {
                    "task_id": item.task_id,
                    "scope": item.scope,
                    "class": item.workload_class,
                    "batch_index": item.batch_index,
                    "units": list(item.units),
                    "budget_bytes": item.budget_bytes,
                    "spent_provider_bytes": 0,
                    "completed": False,
                    "active_claim": None,
                    "attempts": 0,
                    "lease_stats": [],
                }
                for item in plan.allocations
            }
            existing = {
                "artifact_id": plan.artifact_id,
                "dag_id": plan.dag_id,
                "run_id": plan.run_id,
                "plan_digest": plan.plan_digest,
                "plan_signature": plan.signature,
                "run_cap_bytes": plan.run_cap_bytes,
                "spent_provider_bytes": 0,
                "allocations": allocations,
                "created_at": _utc_now(),
                "updated_at": _utc_now(),
            }
            payload["runs"][key] = existing
        expected = {
            "artifact_id": plan.artifact_id,
            "dag_id": plan.dag_id,
            "run_id": plan.run_id,
            "plan_digest": plan.plan_digest,
            "plan_signature": plan.signature,
            "run_cap_bytes": plan.run_cap_bytes,
        }
        if any(existing.get(field) != value for field, value in expected.items()):
            raise AllocationAccountingError(
                "DagRun already has a different immutable workload plan"
            )
        if set(existing.get("allocations", {})) != {
            item.allocation_id for item in plan.allocations
        }:
            raise AllocationAccountingError(
                "persisted allocations differ from signed plan"
            )
        return existing

    def claim(
        self,
        plan: SignedDagRunPlan,
        allocation_id: str,
        *,
        attempt_id: str,
        claim_token: Optional[str] = None,
    ) -> AllocationClaim:
        self._verify(plan)
        allocation_id = _required_token(allocation_id, "allocation_id")
        attempt_id = _required_token(attempt_id, "attempt_id")
        if claim_token is None:
            claim_token = uuid.uuid4().hex
        else:
            claim_token = _required_token(claim_token, "claim_token")
            if len(claim_token) < 32:
                raise AllocationAccountingError(
                    "caller-supplied claim_token must contain at least 32 characters"
                )
        if allocation_id not in {item.allocation_id for item in plan.allocations}:
            raise UnknownAllocation(
                f"allocation {allocation_id!r} is absent from the signed DagRun plan"
            )
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, plan)
            allocation = run["allocations"][allocation_id]
            if allocation.get("active_claim") is not None:
                raise ConcurrentAllocation(
                    f"allocation {allocation_id!r} already has an active attempt"
                )
            if allocation.get("completed") is True:
                raise DuplicateAllocation(
                    f"allocation {allocation_id!r} is already complete"
                )
            spent = int(allocation.get("spent_provider_bytes", 0))
            budget = int(allocation["budget_bytes"])
            remaining = budget - spent
            if remaining <= 0:
                raise AllocationBudgetExceeded(
                    f"allocation {allocation_id!r} has no remaining provider bytes"
                )
            allocation["active_claim"] = {
                "claim_token_hash": self._token_hash(claim_token),
                "attempt_id_hash": self._token_hash(attempt_id),
                "started_at": _utc_now(),
                "start_spent_provider_bytes": spent,
            }
            allocation["attempts"] = int(allocation.get("attempts", 0)) + 1
            run["updated_at"] = _utc_now()
            self._write(payload)
            return AllocationClaim(
                artifact_id=plan.artifact_id,
                dag_id=plan.dag_id,
                run_id=plan.run_id,
                plan_digest=plan.plan_digest,
                allocation_id=allocation_id,
                task_id=str(allocation["task_id"]),
                scope=str(allocation["scope"]),
                workload_class=str(allocation["class"]),
                batch_index=int(allocation["batch_index"]),
                allocation_budget_bytes=budget,
                spent_provider_bytes=spent,
                remaining_provider_bytes=remaining,
                claim_token=claim_token,
            )
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def resume_claim(
        self,
        plan: SignedDagRunPlan,
        allocation_id: str,
        *,
        claim_token: str,
    ) -> AllocationClaim:
        """Recover one active claim using the secret from a durable proxy WAL.

        A crashed process must not silently clear ownership and mint another
        full task allowance.  The proxy persists the raw claim token in its
        mode-0600 WAL before opening the provider connection; only that token
        can recover the same allocation/remaining balance after restart.
        """

        self._verify(plan)
        allocation_id = _required_token(allocation_id, "allocation_id")
        claim_token = _required_token(claim_token, "claim_token")
        if allocation_id not in {item.allocation_id for item in plan.allocations}:
            raise UnknownAllocation(
                f"allocation {allocation_id!r} is absent from the signed DagRun plan"
            )
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, plan)
            allocation = run["allocations"][allocation_id]
            active = allocation.get("active_claim")
            if not isinstance(active, Mapping) or active.get(
                "claim_token_hash"
            ) != self._token_hash(claim_token):
                raise AllocationAccountingError(
                    "allocation recovery token is stale or invalid"
                )
            spent = int(allocation["spent_provider_bytes"])
            budget = int(allocation["budget_bytes"])
            return AllocationClaim(
                artifact_id=plan.artifact_id,
                dag_id=plan.dag_id,
                run_id=plan.run_id,
                plan_digest=plan.plan_digest,
                allocation_id=allocation_id,
                task_id=str(allocation["task_id"]),
                scope=str(allocation["scope"]),
                workload_class=str(allocation["class"]),
                batch_index=int(allocation["batch_index"]),
                allocation_budget_bytes=budget,
                spent_provider_bytes=spent,
                remaining_provider_bytes=budget - spent,
                claim_token=claim_token,
            )
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def _active(
        self,
        run: Mapping[str, Any],
        claim: AllocationClaim,
    ) -> dict[str, Any]:
        allocations = run.get("allocations")
        if (
            not isinstance(allocations, Mapping)
            or claim.allocation_id not in allocations
        ):
            raise UnknownAllocation(
                "claim allocation is absent from the persisted plan"
            )
        allocation = allocations[claim.allocation_id]
        active = allocation.get("active_claim")
        if not isinstance(active, dict) or active.get(
            "claim_token_hash"
        ) != self._token_hash(claim.claim_token):
            raise AllocationAccountingError("allocation claim is stale or invalid")
        return allocation

    def consume(
        self, plan: SignedDagRunPlan, claim: AllocationClaim, provider_bytes: int
    ) -> None:
        self._verify(plan)
        provider_bytes = _non_negative_int(provider_bytes, "provider_bytes")
        if provider_bytes == 0:
            return
        if claim.plan_digest != plan.plan_digest:
            raise AllocationAccountingError("claim belongs to another DagRun plan")
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, plan)
            allocation = self._active(run, claim)
            allocation_spent = int(allocation["spent_provider_bytes"])
            allocation_budget = int(allocation["budget_bytes"])
            run_spent = int(run["spent_provider_bytes"])
            if allocation_spent + provider_bytes > allocation_budget:
                raise AllocationBudgetExceeded(
                    "provider chunk would exceed the signed task allocation"
                )
            if run_spent + provider_bytes > int(run["run_cap_bytes"]):
                raise AllocationBudgetExceeded(
                    "provider chunk would exceed the signed DagRun cap"
                )
            allocation["spent_provider_bytes"] = allocation_spent + provider_bytes
            run["spent_provider_bytes"] = run_spent + provider_bytes
            run["updated_at"] = _utc_now()
            self._write(payload)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def finish(
        self,
        plan: SignedDagRunPlan,
        claim: AllocationClaim,
        *,
        lease_id: str,
        endpoint_request_provider_bytes: Mapping[str, Sequence[int]],
        completed: bool,
        meter: str = WORKLOAD_METER,
        proxy_exit_hash: Optional[str] = None,
    ) -> Mapping[str, object]:
        """Persist exact lease provenance, then release this attempt atomically."""

        self._verify(plan)
        lease_id = _required_token(lease_id, "lease_id")
        if meter != WORKLOAD_METER:
            raise AllocationAccountingError("lease stats use an untrusted meter")
        if not isinstance(completed, bool):
            raise AllocationAccountingError("completed must be boolean")
        request_map: dict[str, list[int]] = {}
        reported = 0
        if not isinstance(endpoint_request_provider_bytes, Mapping):
            raise AllocationAccountingError("endpoint request stats must be an object")
        for endpoint, observations in sorted(endpoint_request_provider_bytes.items()):
            endpoint = _required_token(endpoint, "endpoint")
            if not isinstance(observations, Sequence) or isinstance(
                observations, (str, bytes, bytearray)
            ):
                raise AllocationAccountingError("endpoint request stats must be arrays")
            normalized: list[int] = []
            for value in observations:
                normalized.append(_non_negative_int(value, "request provider bytes"))
            if not normalized:
                raise AllocationAccountingError(
                    "endpoint request stats must not be empty"
                )
            request_map[endpoint] = normalized
            reported += sum(normalized)
        if proxy_exit_hash is not None and (
            not isinstance(proxy_exit_hash, str) or len(proxy_exit_hash) < 12
        ):
            raise AllocationAccountingError("proxy exit must be an anonymized hash")
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, plan)
            allocation = self._active(run, claim)
            active = allocation["active_claim"]
            attempt_spent = int(allocation["spent_provider_bytes"]) - int(
                active["start_spent_provider_bytes"]
            )
            if reported != attempt_spent:
                raise AllocationAccountingError(
                    "lease request map does not equal provider bytes charged by this attempt"
                )
            lease_hash = self._token_hash(lease_id)
            if any(
                item.get("lease_id_hash") == lease_hash
                for item in allocation.get("lease_stats", [])
            ):
                raise DuplicateAllocation("lease stats were already recorded")
            stats: dict[str, object] = {
                "artifact_id": plan.artifact_id,
                "dag_id": plan.dag_id,
                "run_id": plan.run_id,
                "plan_digest": plan.plan_digest,
                "allocation_id": claim.allocation_id,
                "task_id": claim.task_id,
                "scope": claim.scope,
                "class": claim.workload_class,
                "batch_index": claim.batch_index,
                "allocation_budget_bytes": claim.allocation_budget_bytes,
                "attempt_provider_bytes": attempt_spent,
                "allocation_spent_provider_bytes": int(
                    allocation["spent_provider_bytes"]
                ),
                "allocation_remaining_provider_bytes": int(allocation["budget_bytes"])
                - int(allocation["spent_provider_bytes"]),
                "run_cap_bytes": int(run["run_cap_bytes"]),
                "run_spent_provider_bytes": int(run["spent_provider_bytes"]),
                "meter": meter,
                "lease_id_hash": lease_hash,
                "endpoint_request_provider_bytes": request_map,
                "proxy_exit_hash": proxy_exit_hash,
                "attempt_started_at": active["started_at"],
                "finished_at": _utc_now(),
                "completed": completed,
            }
            allocation.setdefault("lease_stats", []).append(stats)
            allocation["active_claim"] = None
            if completed:
                allocation["completed"] = True
            run["updated_at"] = _utc_now()
            self._write(payload)
            return stats
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def snapshot(self, plan: SignedDagRunPlan) -> Mapping[str, object]:
        self._verify(plan)
        handle = self._locked()
        try:
            payload = self._read()
            run = self._run(payload, plan)
            # Round-trip detaches callers from mutable in-memory state.
            snapshot = json.loads(json.dumps(run, sort_keys=True))
            self._write(payload)
            return snapshot
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
