#!/usr/bin/env python3
"""Plan and apply the guarded one-time FotMob 10557/10558 purge.

The default mode is read-only and emits a short-lived, canonical JSON plan.
Applying a plan requires its exact SHA-256 and revalidates Airflow quiescence,
the FotMob publication lease, structural-female exclusion evidence, Iceberg
snapshots/counts, and raw-object reachability.  SQL rows are deleted in Phase A.
Only after every DELETE is owned and its post-state is journaled may Phase B
expire rollback snapshots and remove exclusive target manifests/raw blobs.

This tool deliberately has no competition-ID option.  It never deletes by
``run_id`` and never mutates global team/player snapshots or transfer rows.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit

try:
    from scripts import fotmob_runtime as runtime_binding
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import fotmob_runtime as runtime_binding


PLAN_SCHEMA_VERSION = "fotmob-competition-purge-plan-v2"
JOURNAL_SCHEMA_VERSION = "fotmob-competition-purge-journal-v1"
SCHEDULED_OBSERVATION_SCHEMA_VERSION = "fotmob-scheduled-observation-v1"
PURGE_COMPETITION_IDS = (10557, 10558)
PLAN_TTL = timedelta(hours=1)
MAX_EVIDENCE_AGE = timedelta(days=30)
MAX_PROTECTED_REPORT_BYTES = 2 * 1024 * 1024
WRITER_DAG_IDS = (
    "dag_orchestrate_fotmob",
    "dag_trigger_fotmob_daily",
    "dag_refresh_fotmob",
    "dag_backfill_fotmob",
    "dag_collect_fotmob_players",
    "dag_ingest_fotmob",
    "dag_transform_fotmob_silver",
    "dag_iceberg_maintenance",
    "dag_iceberg_maintenance_daily",
)
SHARED_PAUSE_STATES = {
    "dag_master_pipeline": True,
    "dag_sofascore_pipeline": True,
    "dag_ingest_fotmob": True,
    "dag_transform_fotmob_silver": True,
    "dag_iceberg_maintenance": True,
    "dag_iceberg_maintenance_daily": True,
}
SHARED_STATE_DAGS = (
    "dag_backfill_fotmob",
    "dag_collect_fotmob_players",
    "dag_ingest_fotmob",
    "dag_master_pipeline",
    "dag_orchestrate_fotmob",
    "dag_refresh_fotmob",
    "dag_sofascore_pipeline",
    "dag_transform_e3",
    "dag_transform_e4",
    "dag_transform_fbref_gold",
    "dag_transform_fotmob_silver",
    "dag_transform_xref",
    "dag_trigger_fotmob_daily",
    "dag_iceberg_maintenance",
    "dag_iceberg_maintenance_daily",
)
GLOBAL_PRESERVE_TABLES = (
    "fotmob_team_snapshots",
    "fotmob_squad_snapshots",
    "fotmob_player_snapshots",
    "fotmob_transfer_events",
)
PROTECTED_EVIDENCE_TABLES = (
    "fotmob_competitions",
    "fotmob_competition_scope_observations",
)
PHASE_A_TABLES = (
    "fotmob_competition_seasons",
    "fotmob_competition_season_history",
    "fotmob_season_stages",
    "fotmob_matches",
    "fotmob_standings",
    "fotmob_playoff_brackets",
    "fotmob_season_teams",
    "fotmob_leaderboard_categories",
    "fotmob_leaderboards",
    "fotmob_match_payloads",
    "fotmob_field_inventory",
    "fotmob_competitions",
    "fotmob_competition_scope_observations",
    "fotmob_ingest_manifest",
)
SHARED_TARGET_TYPES = frozenset({"all_leagues", "transfers_page"})
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TARGET_PATH_RE = re.compile(
    r"targets/sha256/([0-9a-f]{2})/([0-9a-f]{64})\.json"
)
_BLOB_PATH_RE = re.compile(
    r"blobs/sha256/([0-9a-f]{2})/([0-9a-f]{64})\.json\.gz"
)


class PurgeRefused(RuntimeError):
    """A fail-closed precondition rejected the purge before a new mutation."""


class PostDeleteVerificationError(RuntimeError):
    """A DELETE may have committed but ownership or post-state is unproven."""


@dataclass(frozen=True)
class ScheduledObservation:
    path: str
    sha256: str
    deployment_report_path: str
    deployment_report_sha256: str
    deployment_id: str
    git_sha: str
    scheduler_container_id: str
    generation_id: str
    publication_binding: Mapping[str, str]
    runs: Mapping[str, Mapping[str, str]]


@dataclass(frozen=True)
class ProtectedEvidence:
    competition_id: int
    catalog_batch_id: str
    evidence_batch_id: str
    profile_target_key: str
    profile_content_hash: str
    catalog_scope_decision: str
    evidence_decision: str
    source_gender: str
    policy_rule: str
    classifier_version: str
    observed_at: datetime


@dataclass(frozen=True)
class TableInspection:
    table: str
    snapshot_id: int
    total_count: int
    candidate_count: int


@dataclass(frozen=True)
class TableOperation:
    table: str
    predicate: str
    snapshot_id: int
    total_count: int
    candidate_count: int


@dataclass(frozen=True)
class DeleteReceipt:
    table: str
    parent_snapshot_id: int
    snapshot_id: int
    operation: str
    query_id: str
    snapshot_query_id: str
    deleted_count: int


@dataclass(frozen=True)
class ManifestReference:
    target_key: str
    content_hash: str | None
    target_type: str
    competition_id: int | None
    batch_id: str
    raw_uri: str | None


@dataclass(frozen=True)
class RawTargetObject:
    target_key: str
    manifest_path: str
    manifest_sha256: str
    content_hash: str
    blob_path: str
    blob_sha256: str
    canonical_url: str


class PurgeBackend(Protocol):
    def assert_quiescent(
        self, writer_dag_ids: Sequence[str], *, source: str
    ) -> None: ...

    def assert_live_scheduled_observation(
        self, observation: ScheduledObservation
    ) -> None: ...

    def acquire_apply_fence(
        self, plan_sha256: str, fence_generation_id: str
    ) -> str: ...

    def recover_apply_fence(
        self, plan_sha256: str, fence_generation_id: str | None
    ) -> str | None: ...

    def release_apply_fence(self, fence_token: str) -> None: ...

    def load_protected_evidence(
        self, competition_ids: Sequence[int]
    ) -> Mapping[int, ProtectedEvidence]: ...

    def load_global_team_ids(
        self, competition_ids: Sequence[int]
    ) -> Sequence[str]: ...

    def inspect_table(self, table: str, predicate: str) -> TableInspection: ...

    def count_matching_rows(self, table: str, predicate: str) -> int: ...

    def inspect_global_tables(
        self, team_ids: Sequence[str]
    ) -> Mapping[str, TableInspection]: ...

    def load_snapshot_ids(self, table: str) -> Sequence[int]: ...

    def load_manifest_references(self) -> Sequence[ManifestReference]: ...

    def load_raw_targets(self) -> Mapping[str, RawTargetObject]: ...

    def invalidate_raw_inventory(self) -> None: ...

    def validate_raw_target(self, target_key: str) -> None: ...

    def validate_raw_blob(self, blob_path: str, content_hash: str) -> None: ...

    def delete_table(self, operation: TableOperation) -> DeleteReceipt: ...

    def recover_delete(self, operation: TableOperation) -> DeleteReceipt | None: ...

    def raw_object_sha256(self, path: str) -> str | None: ...

    def raw_blob_ref_count(self, blob_path: str) -> int: ...

    def delete_raw_object(self, path: str, expected_sha256: str) -> None: ...

    def expire_snapshots(
        self, table: str, snapshot_ids: Sequence[int], *, current_snapshot_id: int
    ) -> None: ...


def _utc(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise PurgeRefused(f"{field} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _timestamp(value: object, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise PurgeRefused(f"{field} is not an ISO timestamp") from exc
    return _utc(parsed, field=field)


def _iso(value: datetime) -> str:
    return _utc(value, field="timestamp").isoformat().replace("+00:00", "Z")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_sha256(value: object, *, field: str) -> str:
    text = str(value or "")
    if _SHA256_RE.fullmatch(text) is None:
        raise PurgeRefused(f"{field} must be lowercase SHA-256")
    return text


def _nonempty(value: object, *, field: str) -> str:
    text = str(value or "")
    if not text or text != text.strip():
        raise PurgeRefused(f"{field} must be non-empty and canonical")
    return text


def _unique_json_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _read_protected_json_report(
    path: Path, *, label: str
) -> tuple[bytes, dict[str, Any]]:
    """Read one stable, non-writable regular report without following a link."""

    try:
        resolved = Path(path).resolve(strict=True)
        before = resolved.lstat()
        raw = resolved.read_bytes()
        after = resolved.lstat()
    except OSError as exc:
        raise PurgeRefused(f"cannot read protected {label}: {exc}") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or before.st_nlink != 1
        or before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or not raw
        or len(raw) != before.st_size
        or len(raw) > MAX_PROTECTED_REPORT_BYTES
        or stat.S_IMODE(before.st_mode) & 0o022
    ):
        raise PurgeRefused(f"protected {label} is not a stable regular file")
    try:
        payload = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_json_object
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise PurgeRefused(f"protected {label} is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise PurgeRefused(f"protected {label} must contain a JSON object")
    return raw, payload


def _validated_deployment_payload(
    raw: bytes, payload: Mapping[str, Any]
) -> tuple[str, dict[str, str], dict[str, Any]]:
    """Validate one exact active report and expose its canonical rollout edge."""

    if (
        payload.get("schema_version") != "fotmob-deploy-v2"
        or payload.get("passed") is not True
        or payload.get("activation_state") != "active"
        or payload.get("kept_paused") is not False
    ):
        raise PurgeRefused("protected deployment report is not active automatic")
    deployment_id = str(payload.get("deployment_id") or "")
    git_sha = str(payload.get("git_sha") or "")
    scheduler_container_id = str(payload.get("scheduler_container_id") or "")
    if (
        re.fullmatch(r"[0-9a-f]{32}", deployment_id) is None
        or re.fullmatch(r"[0-9a-f]{40}", git_sha) is None
        or re.fullmatch(r"[0-9a-f]{64}", scheduler_container_id) is None
    ):
        raise PurgeRefused("protected deployment report has an invalid live identity")
    admission = payload.get("automatic_catalog_admission")
    try:
        normalized_admission = runtime_binding.validate_automatic_catalog_admission(
            admission,
            now=_timestamp(payload.get("generated_at"), field="deployment.generated_at"),
        )
        canary = admission.get("canary") if isinstance(admission, Mapping) else None
        if (
            not isinstance(canary, Mapping)
            or canary.get("deployment_id") != deployment_id
            or canary.get("git_sha") != git_sha
            or canary.get("scheduler_container_id") != scheduler_container_id
        ):
            raise runtime_binding.RuntimeBindingError(
                "automatic canary identity differs from deployment"
            )
        rollout = runtime_binding.validate_automatic_rollout_activation(
            payload,
            normalized_admission,
        )
    except (PurgeRefused, runtime_binding.RuntimeBindingError) as exc:
        raise PurgeRefused(
            "protected deployment report has no canonical active automatic ceremony"
        ) from exc
    return _sha256(raw), {
        "deployment_id": deployment_id,
        "git_sha": git_sha,
        "scheduler_container_id": scheduler_container_id,
    }, rollout


def _deployment_identity(path: Path) -> tuple[str, dict[str, str]]:
    raw, payload = _read_protected_json_report(path, label="deployment report")
    digest, identity, _rollout = _validated_deployment_payload(raw, payload)
    return digest, identity


def _canonical_binding_timestamp(value: object, *, field: str) -> datetime:
    parsed = _timestamp(value, field=field)
    canonical = parsed.isoformat(timespec="microseconds")
    if str(value) != canonical:
        raise PurgeRefused(f"{field} is not an exact UTC timestamp")
    return parsed


def _scheduled_generation_id(binding: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(binding), sort_keys=True, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fotmob-publication:{payload}"))


def _scheduled_observation(
    path: Path, *, deployment_report: Path
) -> ScheduledObservation:
    deployment_sha256, deployment = _deployment_identity(deployment_report)
    raw, payload = _read_protected_json_report(
        path, label="scheduled observation report"
    )
    identity = payload.get("deployment")
    runs = payload.get("runs")
    publication = payload.get("publication")
    if (
        payload.get("schema_version") != SCHEDULED_OBSERVATION_SCHEMA_VERSION
        or payload.get("passed") is not True
        or not isinstance(identity, Mapping)
        or dict(identity) != deployment
        or not isinstance(runs, Mapping)
        or set(runs) != {"owner", "ingest", "silver", "sofascore", "finalizer"}
        or not isinstance(publication, Mapping)
    ):
        raise PurgeRefused("scheduled observation is not bound to the live deployment")
    binding = publication.get("binding")
    generation_id = str(publication.get("generation_id") or "")
    if not isinstance(binding, Mapping) or set(binding) != {
        "schema",
        "source",
        "owner",
        "data_interval_start",
        "data_interval_end",
        "runtime_fingerprint",
    }:
        raise PurgeRefused("scheduled observation publication binding is incomplete")
    start = _canonical_binding_timestamp(
        binding.get("data_interval_start"), field="publication.data_interval_start"
    )
    end = _canonical_binding_timestamp(
        binding.get("data_interval_end"), field="publication.data_interval_end"
    )
    if (
        binding.get("schema") != "fotmob-publication-v1"
        or binding.get("source") != "fotmob"
        or binding.get("owner") != "isolated"
        or binding.get("runtime_fingerprint") != deployment["git_sha"]
        or start.hour != 14
        or start.minute != 0
        or start.second != 0
        or start.microsecond != 0
        or end - start != timedelta(hours=24)
        or end.hour != 14
        or end.minute != 0
        or end.second != 0
        or end.microsecond != 0
    ):
        raise PurgeRefused(
            "scheduled observation binding is not the exact 14:00 UTC daily"
        )
    try:
        if str(uuid.UUID(generation_id)) != generation_id:
            raise ValueError("non-canonical UUID")
    except (TypeError, ValueError) as exc:
        raise PurgeRefused("scheduled observation generation ID is invalid") from exc
    if generation_id != _scheduled_generation_id(binding):
        raise PurgeRefused("scheduled observation generation does not match binding")
    if (
        publication.get("status") != "succeeded"
        or publication.get("phase") != "published"
        or publication.get("active") is not False
        or publication.get("published") is not True
        or publication.get("released") is not True
    ):
        raise PurgeRefused(
            "scheduled observation publication was not published and released"
        )

    expected_runs = {
        "owner": {
            "dag_id": "dag_orchestrate_fotmob",
            "run_type": "scheduled",
            "state": "success",
        },
        "ingest": {"dag_id": "dag_ingest_fotmob", "state": "success"},
        "silver": {"dag_id": "dag_transform_fotmob_silver", "state": "success"},
        "sofascore": {"dag_id": "dag_sofascore_pipeline", "state": "success"},
        "finalizer": {
            "dag_id": "dag_sofascore_pipeline",
            "task_id": "finalize_fotmob_publication",
            "state": "success",
        },
    }
    for name, expected in expected_runs.items():
        item = runs.get(name)
        if not isinstance(item, Mapping) or any(
            item.get(key) != value for key, value in expected.items()
        ):
            raise PurgeRefused(
                f"scheduled observation {name} did not succeed exactly"
            )
        _nonempty(item.get("run_id"), field=f"runs.{name}.run_id")
        if item.get("generation_id") != generation_id:
            raise PurgeRefused(f"scheduled observation {name} generation differs")
    if (
        runs["ingest"].get("run_id")
        != f"fotmob_orchestrated__{generation_id}"
        or runs["silver"].get("run_id") != f"fotmob_silver__{generation_id}"
        or runs["ingest"].get("owner_run_id") != runs["owner"].get("run_id")
        or runs["silver"].get("ingest_run_id") != runs["ingest"].get("run_id")
        or runs["finalizer"].get("run_id") != runs["sofascore"].get("run_id")
    ):
        raise PurgeRefused("scheduled observation run lineage is not exact")
    return ScheduledObservation(
        path=str(Path(path).resolve()),
        sha256=_sha256(raw),
        deployment_report_path=str(Path(deployment_report).resolve()),
        deployment_report_sha256=deployment_sha256,
        deployment_id=deployment["deployment_id"],
        git_sha=deployment["git_sha"],
        scheduler_container_id=deployment["scheduler_container_id"],
        generation_id=generation_id,
        publication_binding={str(key): str(value) for key, value in binding.items()},
        runs={
            str(name): {str(key): str(value) for key, value in item.items()}
            for name, item in runs.items()
            if isinstance(item, Mapping)
        },
    )


def _scheduled_observation_document(value: ScheduledObservation) -> dict[str, str]:
    return asdict(value)


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _id_predicate(competition_id: int, *, column: str = "competition_id") -> str:
    if competition_id not in PURGE_COMPETITION_IDS:
        raise AssertionError("competition ID is outside the immutable purge pair")
    spellings = ", ".join(
        _sql_literal(value) for value in (str(competition_id), f"{competition_id}.0")
    )
    return f"CAST({column} AS VARCHAR) IN ({spellings})"


def _ids_predicate(*, column: str = "competition_id") -> str:
    # ``fotmob_field_inventory.competition_id`` is VARCHAR and historical
    # pandas loads contain the exact ``10557.0``/``10558.0`` spellings.  An
    # integer cast would abort the whole Trino statement on unrelated malformed
    # values; an exact string set reaches the known legacy rows without
    # broadening ownership to arbitrary fractional IDs.
    return "(" + " OR ".join(
        _id_predicate(value, column=column) for value in PURGE_COMPETITION_IDS
    ) + ")"


def _same_purge_id(left: str, right: str) -> str:
    clauses = []
    for value in PURGE_COMPETITION_IDS:
        spellings = ", ".join(
            _sql_literal(item) for item in (str(value), f"{value}.0")
        )
        clauses.append(
            f"(CAST({left} AS VARCHAR) IN ({spellings}) "
            f"AND CAST({right} AS VARCHAR) IN ({spellings}))"
        )
    return "(" + " OR ".join(clauses) + ")"


def _optional_competition_id(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise PurgeRefused("manifest competition_id is invalid")
    text = str(value)
    match = re.fullmatch(r"([1-9][0-9]*)(?:\.0)?", text, re.ASCII)
    if match is None:
        raise PurgeRefused("manifest competition_id is not an exact integer spelling")
    return int(match.group(1))


def _team_id_sort_key(value: str) -> tuple[int, int | str]:
    return (0, int(value)) if value.isascii() and value.isdigit() else (1, value)


def _not_in(column: str, values: Sequence[str]) -> str:
    if not values:
        raise PurgeRefused(f"protected {column} set cannot be empty")
    return f"{column} NOT IN ({', '.join(_sql_literal(v) for v in values)})"


def _nullable_not_in(column: str, values: Sequence[str]) -> str:
    return f"({column} IS NULL OR {_not_in(column, values)})"


def _paired_batch_delete_predicate(
    evidence: Sequence[ProtectedEvidence],
    *,
    batch_column: str,
    batch_fields: Sequence[str],
) -> str:
    clauses: list[str] = []
    for item in evidence:
        batches = tuple(str(getattr(item, field)) for field in batch_fields)
        clauses.append(
            f"({_id_predicate(item.competition_id)} AND "
            f"{_nullable_not_in(batch_column, batches)})"
        )
    return "(" + " OR ".join(clauses) + ")"


def _validate_protected_evidence(
    evidence: Mapping[int, ProtectedEvidence], *, now: datetime
) -> tuple[ProtectedEvidence, ...]:
    expected = set(PURGE_COMPETITION_IDS)
    if set(evidence) != expected:
        raise PurgeRefused(
            "current structural-female evidence must exist exactly for both IDs"
        )
    validated: list[ProtectedEvidence] = []
    clock = _utc(now, field="now")
    for competition_id in PURGE_COMPETITION_IDS:
        item = evidence[competition_id]
        if item.competition_id != competition_id:
            raise PurgeRefused("structural-female evidence identity mismatch")
        if (
            item.catalog_scope_decision != "excluded"
            or item.evidence_decision != "excluded"
            or item.source_gender.casefold() != "female"
            or item.policy_rule != "exclude_female"
            or item.classifier_version != "fotmob-men-v1"
        ):
            raise PurgeRefused(
                f"competition {competition_id} lacks current structural-female "
                "excluded evidence"
            )
        _nonempty(item.catalog_batch_id, field="catalog_batch_id")
        _nonempty(item.evidence_batch_id, field="evidence_batch_id")
        _require_sha256(item.profile_target_key, field="profile_target_key")
        _require_sha256(item.profile_content_hash, field="profile_content_hash")
        observed = _utc(item.observed_at, field="observed_at")
        if observed > clock or clock - observed > MAX_EVIDENCE_AGE:
            raise PurgeRefused(
                f"competition {competition_id} structural-female evidence is stale"
            )
        validated.append(item)
    return tuple(validated)


def _table_predicates(
    evidence: Sequence[ProtectedEvidence],
) -> dict[str, str]:
    base = _ids_predicate()
    predicates = {table: base for table in PHASE_A_TABLES}
    predicates["fotmob_competitions"] = (
        f"{base} AND "
        + _paired_batch_delete_predicate(
            evidence,
            batch_column="_target_batch_id",
            batch_fields=("catalog_batch_id",),
        )
    )
    predicates["fotmob_competition_scope_observations"] = (
        f"{base} AND "
        + _paired_batch_delete_predicate(
            evidence,
            batch_column="_target_batch_id",
            batch_fields=("evidence_batch_id",),
        )
    )
    predicates["fotmob_ingest_manifest"] = (
        f"{base} "
        "AND (target_type IS NULL OR "
        "target_type NOT IN ('all_leagues', 'transfers_page')) "
        "AND "
        + _paired_batch_delete_predicate(
            evidence,
            batch_column="batch_id",
            batch_fields=("catalog_batch_id", "evidence_batch_id"),
        )
    )
    if any("run_id" in predicate.casefold() for predicate in predicates.values()):
        raise AssertionError("purge predicates must never use run_id")
    return predicates


def _protected_survivor_inventory(
    evidence: Sequence[ProtectedEvidence],
) -> tuple[dict[str, Any], ...]:
    items: list[dict[str, Any]] = []
    for table, batch_field in (
        ("fotmob_competitions", "catalog_batch_id"),
        ("fotmob_competition_scope_observations", "evidence_batch_id"),
    ):
        for item in evidence:
            predicate = (
                f"{_id_predicate(item.competition_id)} AND "
                f"_target_batch_id = {_sql_literal(getattr(item, batch_field))}"
            )
            items.append(
                {
                    "table": table,
                    "competition_id": item.competition_id,
                    "predicate": predicate,
                    "expected_count": 1,
                }
            )
    return tuple(items)


def _validate_live_protected_survivors(
    backend: PurgeBackend,
    inventory: Sequence[Mapping[str, Any]],
) -> None:
    for item in inventory:
        table = str(item["table"])
        predicate = str(item["predicate"])
        expected = item.get("expected_count")
        observed = backend.count_matching_rows(table, predicate)
        if expected != 1 or observed != 1:
            raise PurgeRefused(
                f"{table} must retain exactly one physical evidence row for "
                f"competition {item.get('competition_id')}"
            )


def _validate_inspection(
    inspection: TableInspection, *, table: str
) -> TableInspection:
    if inspection.table != table:
        raise PurgeRefused(f"table inspection identity mismatch for {table}")
    for field in ("snapshot_id", "total_count", "candidate_count"):
        value = getattr(inspection, field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise PurgeRefused(f"invalid {field} for {table}")
    if inspection.snapshot_id == 0:
        raise PurgeRefused(f"{table} has no authoritative Iceberg snapshot")
    if inspection.candidate_count > inspection.total_count:
        raise PurgeRefused(f"candidate count exceeds total rows for {table}")
    return inspection


def _evidence_document(item: ProtectedEvidence) -> dict[str, Any]:
    payload = asdict(item)
    payload["observed_at"] = _iso(item.observed_at)
    return payload


def _inspection_document(item: TableInspection) -> dict[str, Any]:
    return asdict(item)


def _operation_document(item: TableOperation) -> dict[str, Any]:
    return asdict(item)


def _manifest_reference_is_doomed(
    reference: ManifestReference,
    evidence: Sequence[ProtectedEvidence],
) -> bool:
    if reference.competition_id not in PURGE_COMPETITION_IDS:
        return False
    owner = next(
        (item for item in evidence if item.competition_id == reference.competition_id),
        None,
    )
    if owner is None:
        return False
    protected_batches = {owner.catalog_batch_id, owner.evidence_batch_id}
    return (
        reference.target_type not in SHARED_TARGET_TYPES
        and reference.batch_id not in protected_batches
    )


def _validate_manifest_reference(reference: ManifestReference) -> None:
    _require_sha256(reference.target_key, field="manifest target_key")
    _nonempty(reference.target_type, field="manifest target_type")
    _nonempty(reference.batch_id, field="manifest batch_id")
    # Logical manifests such as ``scope_completion`` deliberately carry a
    # coverage content_hash without a raw_uri.  Only a raw_uri asserts raw
    # object ownership; when present it must be paired with a canonical hash.
    if reference.raw_uri is not None:
        if reference.content_hash is None:
            raise PurgeRefused("manifest raw_uri lacks a content hash")
        _require_sha256(reference.content_hash, field="manifest content_hash")
        _nonempty(reference.raw_uri, field="manifest raw_uri")
        blob_path = _blob_path(reference.content_hash)
        if not (
            reference.raw_uri == blob_path
            or reference.raw_uri.endswith("/" + blob_path)
        ):
            raise PurgeRefused("manifest raw_uri does not identify its content hash")
    if reference.competition_id is not None and (
        isinstance(reference.competition_id, bool)
        or not isinstance(reference.competition_id, int)
        or reference.competition_id <= 0
    ):
        raise PurgeRefused("manifest competition_id is invalid")


def _blob_path(content_hash: str) -> str:
    normalized = _require_sha256(content_hash, field="content_hash")
    return f"blobs/sha256/{normalized[:2]}/{normalized}.json.gz"


def _validate_raw_target(item: RawTargetObject) -> None:
    target_key = _require_sha256(item.target_key, field="raw target_key")
    content_hash = _require_sha256(item.content_hash, field="raw content_hash")
    _require_sha256(item.manifest_sha256, field="raw manifest SHA-256")
    if item.blob_sha256:
        _require_sha256(item.blob_sha256, field="raw blob SHA-256")
    target_match = _TARGET_PATH_RE.fullmatch(item.manifest_path)
    blob_match = _BLOB_PATH_RE.fullmatch(item.blob_path)
    if (
        target_match is None
        or target_match.group(1) != target_key[:2]
        or target_match.group(2) != target_key
        or blob_match is None
        or blob_match.group(1) != content_hash[:2]
        or blob_match.group(2) != content_hash
    ):
        raise PurgeRefused("raw object path is not content-address canonical")
    parsed_url = urlsplit(str(item.canonical_url))
    if (
        parsed_url.scheme != "https"
        or parsed_url.hostname not in {"www.fotmob.com", "data.fotmob.com"}
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.port not in {None, 443}
    ):
        raise PurgeRefused("raw target URL is outside the canonical FotMob origin")


def canonical_plan_bytes(plan: Mapping[str, Any]) -> bytes:
    payload = dict(plan)
    payload.pop("plan_sha256", None)
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def with_plan_hash(plan: Mapping[str, Any]) -> dict[str, Any]:
    payload = json.loads(json.dumps(dict(plan), default=str))
    payload.pop("plan_sha256", None)
    payload["plan_sha256"] = _sha256(canonical_plan_bytes(payload))
    return payload


def build_plan(
    backend: PurgeBackend,
    *,
    deployment_report: Path,
    scheduled_observation_report: Path,
    now: datetime | None = None,
    ttl: timedelta = PLAN_TTL,
) -> dict[str, Any]:
    """Build a short-lived read-only purge plan from authoritative state."""

    clock = _utc(now or datetime.now(timezone.utc), field="now")
    if not isinstance(ttl, timedelta) or not timedelta(0) < ttl <= PLAN_TTL:
        raise PurgeRefused(f"plan TTL must be positive and at most {PLAN_TTL}")
    scheduled_observation = _scheduled_observation(
        scheduled_observation_report, deployment_report=deployment_report
    )
    backend.assert_live_scheduled_observation(scheduled_observation)
    backend.assert_quiescent(WRITER_DAG_IDS, source="fotmob")
    evidence = _validate_protected_evidence(
        backend.load_protected_evidence(PURGE_COMPETITION_IDS), now=clock
    )
    protected_survivors = _protected_survivor_inventory(evidence)
    _validate_live_protected_survivors(backend, protected_survivors)
    predicates = _table_predicates(evidence)
    operations: list[TableOperation] = []
    for table in PHASE_A_TABLES:
        inspection = _validate_inspection(
            backend.inspect_table(table, predicates[table]), table=table
        )
        operations.append(
            TableOperation(
                table=table,
                predicate=predicates[table],
                snapshot_id=inspection.snapshot_id,
                total_count=inspection.total_count,
                candidate_count=inspection.candidate_count,
            )
        )

    team_ids = tuple(
        sorted(
            {
                _nonempty(value, field="team_id")
                for value in (
                    str(item)
                    for item in backend.load_global_team_ids(PURGE_COMPETITION_IDS)
                )
            },
            key=_team_id_sort_key,
        )
    )
    if len(team_ids) != 23:
        raise PurgeRefused(
            f"expected exactly 23 protected global team IDs, observed {len(team_ids)}"
        )
    global_inspections = backend.inspect_global_tables(team_ids)
    if set(global_inspections) != set(GLOBAL_PRESERVE_TABLES):
        raise PurgeRefused("global snapshot preservation inventory is incomplete")
    preserved_tables: dict[str, TableInspection] = {}
    for table in GLOBAL_PRESERVE_TABLES:
        inspection = _validate_inspection(global_inspections[table], table=table)
        if inspection.candidate_count != 0:
            raise PurgeRefused(f"global table {table} unexpectedly has purge candidates")
        preserved_tables[table] = inspection
    snapshot_expirations: list[dict[str, Any]] = []
    for operation in operations:
        if operation.candidate_count == 0:
            continue
        snapshot_ids = tuple(sorted(set(backend.load_snapshot_ids(operation.table))))
        if (
            not snapshot_ids
            or operation.snapshot_id not in snapshot_ids
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in snapshot_ids
            )
        ):
            raise PurgeRefused(
                f"authoritative snapshot inventory is invalid for {operation.table}"
            )
        snapshot_expirations.append(
            {
                "table": operation.table,
                "planned_head_snapshot_id": operation.snapshot_id,
                "snapshot_ids": list(snapshot_ids),
            }
        )

    references = tuple(backend.load_manifest_references())
    for reference in references:
        _validate_manifest_reference(reference)
    raw_references = tuple(item for item in references if item.raw_uri is not None)
    references_by_target: dict[str, list[ManifestReference]] = {}
    for reference in raw_references:
        references_by_target.setdefault(reference.target_key, []).append(reference)
    doomed_target_keys = {
        target_key
        for target_key, target_refs in references_by_target.items()
        if any(_manifest_reference_is_doomed(item, evidence) for item in target_refs)
        and all(_manifest_reference_is_doomed(item, evidence) for item in target_refs)
    }
    raw_targets = dict(backend.load_raw_targets())
    if set(raw_targets) != {item.target_key for item in raw_targets.values()}:
        raise PurgeRefused("raw target inventory contains duplicate/mismatched keys")
    for item in raw_targets.values():
        _validate_raw_target(item)
    missing_raw = doomed_target_keys - set(raw_targets)
    if missing_raw:
        raise PurgeRefused(
            "competition-owned manifest rows lack raw target manifests: "
            + ", ".join(sorted(missing_raw))
        )
    for target_key in sorted(doomed_target_keys):
        backend.validate_raw_target(target_key)

    target_deletes: list[dict[str, Any]] = []
    for target_key in sorted(doomed_target_keys):
        raw = raw_targets[target_key]
        target_deletes.append(
            {
                "target_key": target_key,
                "path": raw.manifest_path,
                "sha256": raw.manifest_sha256,
                "content_hash": raw.content_hash,
                "bronze_reference_count": len(references_by_target[target_key]),
                "surviving_bronze_reference_count": 0,
            }
        )

    raw_by_blob: dict[str, list[RawTargetObject]] = {}
    for item in raw_targets.values():
        raw_by_blob.setdefault(item.blob_path, []).append(item)
    bronze_by_blob: dict[str, list[ManifestReference]] = {}
    for reference in raw_references:
        if reference.content_hash is not None:
            bronze_by_blob.setdefault(_blob_path(reference.content_hash), []).append(
                reference
            )
    candidate_blob_paths = {
        _blob_path(reference.content_hash)
        for reference in raw_references
        if reference.content_hash is not None
        and _manifest_reference_is_doomed(reference, evidence)
    }
    candidate_blob_paths.update(
        raw_targets[target_key].blob_path for target_key in doomed_target_keys
    )
    blob_deletes: list[dict[str, Any]] = []
    shared_blob_exclusions: list[dict[str, Any]] = []
    for blob_path in sorted(candidate_blob_paths):
        bronze_refs = bronze_by_blob.get(blob_path, [])
        raw_refs = raw_by_blob.get(blob_path, [])
        surviving_bronze = [
            item
            for item in bronze_refs
            if not _manifest_reference_is_doomed(item, evidence)
        ]
        surviving_targets = [
            item for item in raw_refs if item.target_key not in doomed_target_keys
        ]
        content_hash = _BLOB_PATH_RE.fullmatch(blob_path).group(2)
        backend.validate_raw_blob(blob_path, content_hash)
        object_sha = backend.raw_object_sha256(blob_path)
        if object_sha is None:
            raise PurgeRefused(f"referenced raw blob is missing: {blob_path}")
        _require_sha256(object_sha, field="raw blob SHA-256")
        if surviving_bronze or surviving_targets:
            shared_blob_exclusions.append(
                {
                    "path": blob_path,
                    "sha256": object_sha,
                    "content_hash": content_hash,
                    "bronze_reference_count": len(bronze_refs),
                    "surviving_bronze_reference_count": len(surviving_bronze),
                    "target_reference_count": len(raw_refs),
                    "surviving_target_reference_count": len(surviving_targets),
                    "reason": "surviving_bronze_or_target_manifest_reference",
                }
            )
            continue
        blob_deletes.append(
            {
                "path": blob_path,
                "sha256": object_sha,
                "content_hash": content_hash,
                "bronze_reference_count": len(bronze_refs),
                "target_reference_count": len(raw_refs),
                "surviving_reference_count": 0,
            }
        )

    protected_target_keys = {item.profile_target_key for item in evidence}
    if len(protected_target_keys) != len(PURGE_COMPETITION_IDS):
        raise PurgeRefused("protected profile target identities are not unique")
    for item in evidence:
        expected_refs = [
            reference
            for reference in references_by_target.get(item.profile_target_key, ())
            if reference.target_type == "competition_profile"
            and reference.competition_id == item.competition_id
            and reference.batch_id == item.evidence_batch_id
            and reference.content_hash == item.profile_content_hash
        ]
        raw = raw_targets.get(item.profile_target_key)
        if len(expected_refs) != 1 or raw is None:
            raise PurgeRefused(
                f"protected profile raw lineage is not exact for {item.competition_id}"
            )
        if raw.content_hash != item.profile_content_hash:
            raise PurgeRefused(
                f"protected profile raw content drifted for {item.competition_id}"
            )
    relevant_exclusions = {
        reference.target_key
        for reference in raw_references
        if reference.target_type in SHARED_TARGET_TYPES
        or reference.target_key in protected_target_keys
        or (
            reference.target_key not in doomed_target_keys
            and reference.content_hash is not None
            and _blob_path(reference.content_hash) in candidate_blob_paths
        )
        or (
            reference.target_key not in doomed_target_keys
            and any(
                _manifest_reference_is_doomed(item, evidence)
                for item in references_by_target[reference.target_key]
            )
        )
    }
    relevant_exclusions.update(protected_target_keys)
    relevant_exclusions.update(
        item.target_key
        for item in raw_targets.values()
        if item.target_key not in doomed_target_keys
        and item.blob_path in candidate_blob_paths
    )
    shared_exclusions: list[dict[str, Any]] = []
    for target_key in sorted(relevant_exclusions):
        raw = raw_targets.get(target_key)
        if raw is None:
            raise PurgeRefused(
                f"protected/shared target lacks raw manifest: {target_key}"
            )
        backend.validate_raw_target(target_key)
        types = sorted(
            {item.target_type for item in references_by_target.get(target_key, ())}
        )
        live_manifest_sha = backend.raw_object_sha256(raw.manifest_path)
        _require_sha256(
            live_manifest_sha,
            field=f"protected manifest SHA-256 for {target_key}",
        )
        if live_manifest_sha != raw.manifest_sha256:
            raise PurgeRefused(f"protected manifest hash drifted for {target_key}")
        protected_blob_sha = backend.raw_object_sha256(raw.blob_path)
        _require_sha256(
            protected_blob_sha,
            field=f"protected blob SHA-256 for {target_key}",
        )
        if raw.blob_sha256 and protected_blob_sha != raw.blob_sha256:
            raise PurgeRefused(f"protected blob hash drifted for {target_key}")
        shared_exclusions.append(
            {
                "target_key": target_key,
                "target_types": types,
                "manifest_path": raw.manifest_path,
                "manifest_sha256": raw.manifest_sha256,
                "blob_path": raw.blob_path,
                "blob_sha256": protected_blob_sha,
                "reason": (
                    "protected_excluded_evidence"
                    if target_key in protected_target_keys
                    else "shared_all_leagues_or_transfers"
                    if set(types) & SHARED_TARGET_TYPES
                    else "shared_blob_reachable"
                ),
            }
        )

    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "created_at": _iso(clock),
        "expires_at": _iso(clock + ttl),
        "competition_ids": list(PURGE_COMPETITION_IDS),
        "writer_dag_ids": list(WRITER_DAG_IDS),
        "scheduled_observation": _scheduled_observation_document(
            scheduled_observation
        ),
        "protected_evidence": [_evidence_document(item) for item in evidence],
        "global_preservation": {
            "team_ids": list(team_ids),
            "tables": {
                table: _inspection_document(preserved_tables[table])
                for table in GLOBAL_PRESERVE_TABLES
            },
        },
        "phase_a": {
            "tables": [_operation_document(item) for item in operations],
            "protected_survivors": list(protected_survivors),
            "delete_by_run_id": False,
            "validation": "owned direct-child delete snapshots and exact post-counts",
        },
        "phase_b": {
            "requires_phase_a_validated_journal": True,
            "target_manifests": target_deletes,
            "blobs": blob_deletes,
            "shared_exclusions": shared_exclusions,
            "shared_blob_exclusions": shared_blob_exclusions,
            "snapshots_to_expire": snapshot_expirations,
        },
    }
    return with_plan_hash(plan)


def _plan_evidence(plan: Mapping[str, Any]) -> tuple[ProtectedEvidence, ...]:
    raw = plan.get("protected_evidence")
    if not isinstance(raw, list) or len(raw) != len(PURGE_COMPETITION_IDS):
        raise PurgeRefused("plan protected evidence has an invalid shape")
    output: list[ProtectedEvidence] = []
    try:
        for item in raw:
            if not isinstance(item, Mapping):
                raise TypeError("evidence item is not an object")
            output.append(
                ProtectedEvidence(
                    competition_id=int(item["competition_id"]),
                    catalog_batch_id=str(item["catalog_batch_id"]),
                    evidence_batch_id=str(item["evidence_batch_id"]),
                    profile_target_key=str(item["profile_target_key"]),
                    profile_content_hash=str(item["profile_content_hash"]),
                    catalog_scope_decision=str(item["catalog_scope_decision"]),
                    evidence_decision=str(item["evidence_decision"]),
                    source_gender=str(item["source_gender"]),
                    policy_rule=str(item["policy_rule"]),
                    classifier_version=str(item["classifier_version"]),
                    observed_at=_timestamp(item["observed_at"], field="observed_at"),
                )
            )
    except (KeyError, TypeError, ValueError) as exc:
        raise PurgeRefused("plan protected evidence is malformed") from exc
    if tuple(item.competition_id for item in output) != PURGE_COMPETITION_IDS:
        raise PurgeRefused("plan evidence is not ordered for the exact purge IDs")
    return tuple(output)


def _plan_scheduled_observation(plan: Mapping[str, Any]) -> ScheduledObservation:
    value = plan.get("scheduled_observation")
    if not isinstance(value, Mapping):
        raise PurgeRefused("plan scheduled observation is absent")
    try:
        observation = ScheduledObservation(
            path=str(value["path"]),
            sha256=_require_sha256(
                value["sha256"], field="scheduled observation SHA-256"
            ),
            deployment_report_path=str(value["deployment_report_path"]),
            deployment_report_sha256=_require_sha256(
                value["deployment_report_sha256"],
                field="scheduled observation deployment report SHA-256",
            ),
            deployment_id=str(value["deployment_id"]),
            git_sha=str(value["git_sha"]),
            scheduler_container_id=str(value["scheduler_container_id"]),
            generation_id=str(value["generation_id"]),
            publication_binding={
                str(key): str(item)
                for key, item in dict(value["publication_binding"]).items()
            },
            runs={
                str(name): {
                    str(key): str(item)
                    for key, item in dict(run).items()
                }
                for name, run in dict(value["runs"]).items()
            },
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PurgeRefused("plan scheduled observation is malformed") from exc
    if (
        not Path(observation.path).is_absolute()
        or not Path(observation.deployment_report_path).is_absolute()
        or re.fullmatch(r"[0-9a-f]{32}", observation.deployment_id) is None
        or re.fullmatch(r"[0-9a-f]{40}", observation.git_sha) is None
        or re.fullmatch(r"[0-9a-f]{64}", observation.scheduler_container_id) is None
        or set(observation.publication_binding)
        != {
            "schema",
            "source",
            "owner",
            "data_interval_start",
            "data_interval_end",
            "runtime_fingerprint",
        }
        or set(observation.runs)
        != {"owner", "ingest", "silver", "sofascore", "finalizer"}
    ):
        raise PurgeRefused("plan scheduled observation identity is invalid")
    try:
        if str(uuid.UUID(observation.generation_id)) != observation.generation_id:
            raise ValueError("non-canonical")
    except (TypeError, ValueError) as exc:
        raise PurgeRefused("plan scheduled observation generation is invalid") from exc
    return observation


def _revalidate_scheduled_observation(
    plan: Mapping[str, Any],
    *,
    deployment_report: Path | None = None,
    scheduled_observation_report: Path | None = None,
) -> ScheduledObservation:
    expected = _plan_scheduled_observation(plan)
    observed = _scheduled_observation(
        scheduled_observation_report or Path(expected.path),
        deployment_report=deployment_report or Path(expected.deployment_report_path),
    )
    if observed != expected:
        raise PurgeRefused(
            "scheduled observation digest or live deployment identity drifted"
        )
    return observed


def _plan_operations(
    plan: Mapping[str, Any], evidence: Sequence[ProtectedEvidence]
) -> tuple[TableOperation, ...]:
    phase_a = plan.get("phase_a")
    if not isinstance(phase_a, Mapping) or phase_a.get("delete_by_run_id") is not False:
        raise PurgeRefused("plan Phase A contract is malformed")
    if phase_a.get("protected_survivors") != list(
        _protected_survivor_inventory(evidence)
    ):
        raise PurgeRefused("plan protected survivor inventory is not exact")
    raw = phase_a.get("tables")
    if not isinstance(raw, list) or len(raw) != len(PHASE_A_TABLES):
        raise PurgeRefused("plan does not contain the exact Phase A table set")
    expected_predicates = _table_predicates(evidence)
    output: list[TableOperation] = []
    try:
        for index, item in enumerate(raw):
            if not isinstance(item, Mapping):
                raise TypeError("table operation is not an object")
            operation = TableOperation(
                table=str(item["table"]),
                predicate=str(item["predicate"]),
                snapshot_id=int(item["snapshot_id"]),
                total_count=int(item["total_count"]),
                candidate_count=int(item["candidate_count"]),
            )
            expected_table = PHASE_A_TABLES[index]
            if operation.table != expected_table:
                raise PurgeRefused("Phase A tables are missing, duplicated, or reordered")
            if operation.predicate != expected_predicates[expected_table]:
                raise PurgeRefused(
                    f"plan predicate differs from compiled predicate for {expected_table}"
                )
            _validate_inspection(
                TableInspection(
                    table=operation.table,
                    snapshot_id=operation.snapshot_id,
                    total_count=operation.total_count,
                    candidate_count=operation.candidate_count,
                ),
                table=operation.table,
            )
            output.append(operation)
    except (KeyError, TypeError, ValueError) as exc:
        raise PurgeRefused("plan Phase A operation is malformed") from exc
    return tuple(output)


def _validate_plan(
    plan: Mapping[str, Any],
    *,
    supplied_sha256: str,
    now: datetime,
    allow_expired_recovery: bool = False,
) -> tuple[tuple[ProtectedEvidence, ...], tuple[TableOperation, ...]]:
    supplied = _require_sha256(supplied_sha256, field="supplied plan SHA-256")
    embedded = _require_sha256(plan.get("plan_sha256"), field="plan_sha256")
    canonical = _sha256(canonical_plan_bytes(plan))
    if supplied != embedded or supplied != canonical:
        raise PurgeRefused(
            "supplied plan SHA-256 does not match the exact canonical plan"
        )
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise PurgeRefused("unsupported purge plan schema")
    if plan.get("competition_ids") != list(PURGE_COMPETITION_IDS):
        raise PurgeRefused("plan competition IDs are not the immutable purge pair")
    if plan.get("writer_dag_ids") != list(WRITER_DAG_IDS):
        raise PurgeRefused("plan writer DAG inventory is incomplete")
    _plan_scheduled_observation(plan)
    created = _timestamp(plan.get("created_at"), field="created_at")
    expires = _timestamp(plan.get("expires_at"), field="expires_at")
    clock = _utc(now, field="now")
    if clock >= expires and not allow_expired_recovery:
        raise PurgeRefused("purge plan has expired")
    if expires <= created or expires - created > PLAN_TTL:
        raise PurgeRefused("plan expiry interval is invalid")
    if created > clock:
        raise PurgeRefused("purge plan was created in the future")
    evidence = _plan_evidence(plan)
    _validate_protected_evidence(
        {item.competition_id: item for item in evidence}, now=clock
    )
    operations = _plan_operations(plan, evidence)
    phase_b = plan.get("phase_b")
    if (
        not isinstance(phase_b, Mapping)
        or phase_b.get("requires_phase_a_validated_journal") is not True
    ):
        raise PurgeRefused("plan Phase B admission marker is absent")
    for collection in (
        "target_manifests",
        "blobs",
        "shared_exclusions",
        "shared_blob_exclusions",
    ):
        if not isinstance(phase_b.get(collection), list):
            raise PurgeRefused(f"plan Phase B {collection} inventory is malformed")
    expected_operations = [item for item in operations if item.candidate_count > 0]
    expiry = phase_b.get("snapshots_to_expire")
    if not isinstance(expiry, list) or len(expiry) != len(expected_operations):
        raise PurgeRefused("plan snapshot-expiration inventory drifted")
    for operation, item in zip(expected_operations, expiry):
        if not isinstance(item, Mapping):
            raise PurgeRefused("plan snapshot-expiration item is malformed")
        snapshot_ids = item.get("snapshot_ids")
        if (
            item.get("table") != operation.table
            or item.get("planned_head_snapshot_id") != operation.snapshot_id
            or not isinstance(snapshot_ids, list)
            or operation.snapshot_id not in snapshot_ids
            or snapshot_ids != sorted(set(snapshot_ids))
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in snapshot_ids
            )
        ):
            raise PurgeRefused("plan snapshot-expiration item is malformed")
    return evidence, operations


def _same_evidence(
    left: Sequence[ProtectedEvidence], right: Sequence[ProtectedEvidence]
) -> bool:
    return [_evidence_document(item) for item in left] == [
        _evidence_document(item) for item in right
    ]


def _validate_global_preservation(
    plan: Mapping[str, Any], backend: PurgeBackend
) -> None:
    expected = plan.get("global_preservation")
    if not isinstance(expected, Mapping):
        raise PurgeRefused("global preservation plan is malformed")
    raw_team_ids = expected.get("team_ids")
    if not isinstance(raw_team_ids, list) or len(raw_team_ids) != 23:
        raise PurgeRefused("global preservation plan must contain 23 team IDs")
    team_ids = tuple(str(item) for item in raw_team_ids)
    if len(set(team_ids)) != 23 or tuple(
        sorted(team_ids, key=_team_id_sort_key)
    ) != team_ids:
        raise PurgeRefused("global team ID preservation inventory is not canonical")
    live_team_ids = tuple(
        sorted(
            (
                str(item)
                for item in backend.load_global_team_ids(PURGE_COMPETITION_IDS)
            ),
            key=_team_id_sort_key,
        )
    )
    # Once Phase A removes season-team rows, the exact live source inventory is
    # unavailable.  An empty result is accepted only after table post-state is
    # independently proven; callers perform this check before Phase A as well.
    if live_team_ids and live_team_ids != team_ids:
        raise PurgeRefused("protected global team ID inventory drifted")
    live = backend.inspect_global_tables(team_ids)
    raw_tables = expected.get("tables")
    if not isinstance(raw_tables, Mapping) or set(raw_tables) != set(
        GLOBAL_PRESERVE_TABLES
    ):
        raise PurgeRefused("global table preservation plan is incomplete")
    if set(live) != set(GLOBAL_PRESERVE_TABLES):
        raise PurgeRefused("live global table inventory is incomplete")
    for table in GLOBAL_PRESERVE_TABLES:
        observed = _validate_inspection(live[table], table=table)
        expected_item = raw_tables[table]
        if not isinstance(expected_item, Mapping) or asdict(observed) != dict(
            expected_item
        ):
            raise PurgeRefused(f"global preserved table drifted: {table}")


def _initial_journal(plan_sha256: str) -> dict[str, Any]:
    return {
        "schema_version": JOURNAL_SCHEMA_VERSION,
        "plan_sha256": plan_sha256,
        "status": "phase_a",
        "phase_a_validated": False,
        "phase_a_receipts": {},
        "phase_a_intent": None,
        "apply_fence_generation_id": None,
        "deleted_raw_objects": [],
        "raw_delete_intent": None,
        "expired_snapshots": [],
        "snapshot_expiration_intent": None,
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode(
            "utf-8"
        )
        + b"\n"
    )
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _load_journal(path: Path, *, plan_sha256: str) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        journal = _initial_journal(plan_sha256)
        _atomic_json(target, journal)
        return journal
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PurgeRefused("purge journal is unreadable") from exc
    if not isinstance(payload, dict):
        raise PurgeRefused("purge journal must be a JSON object")
    if payload.get("schema_version") != JOURNAL_SCHEMA_VERSION:
        raise PurgeRefused("purge journal schema differs")
    if payload.get("plan_sha256") != plan_sha256:
        raise PurgeRefused("purge journal belongs to a different plan")
    if not isinstance(payload.get("phase_a_receipts"), dict):
        raise PurgeRefused("purge journal receipts are malformed")
    if not isinstance(payload.get("deleted_raw_objects"), list) or not isinstance(
        payload.get("expired_snapshots"), list
    ):
        raise PurgeRefused("purge journal Phase B state is malformed")
    return payload


def _validate_journal_state(
    journal: Mapping[str, Any],
    operations: Sequence[TableOperation],
    plan: Mapping[str, Any],
) -> None:
    required = {
        "schema_version",
        "plan_sha256",
        "status",
        "phase_a_validated",
        "phase_a_receipts",
        "phase_a_intent",
        "apply_fence_generation_id",
        "deleted_raw_objects",
        "raw_delete_intent",
        "expired_snapshots",
        "snapshot_expiration_intent",
    }
    allowed = required | {"completed_at"}
    if set(journal) - allowed or not required <= set(journal):
        raise PurgeRefused("purge journal contains missing or unknown state fields")
    status = journal.get("status")
    if status not in {"phase_a", "phase_b", "complete"}:
        raise PurgeRefused("purge journal status is invalid")
    fence_generation_id = journal.get("apply_fence_generation_id")
    if fence_generation_id is not None:
        try:
            normalized_fence = str(uuid.UUID(str(fence_generation_id)))
        except (AttributeError, TypeError, ValueError) as exc:
            raise PurgeRefused("purge journal apply fence ID is invalid") from exc
        if normalized_fence != fence_generation_id:
            raise PurgeRefused("purge journal apply fence ID is not canonical")
    receipts = journal.get("phase_a_receipts")
    if not isinstance(receipts, Mapping):
        raise PurgeRefused("purge journal receipts are malformed")
    keys = set(receipts)
    expected_prefix = {item.table for item in operations[: len(keys)]}
    if keys != expected_prefix:
        raise PurgeRefused("purge journal receipts are not an exact Phase A prefix")
    by_table = {item.table: item for item in operations}
    for table, value in receipts.items():
        operation = by_table[table]
        if operation.candidate_count == 0:
            expected_noop = {
                "table": table,
                "parent_snapshot_id": operation.snapshot_id,
                "snapshot_id": operation.snapshot_id,
                "operation": "noop",
                "query_id": "noop",
                "snapshot_query_id": "noop",
                "deleted_count": 0,
            }
            if value != expected_noop:
                raise PurgeRefused(f"purge journal no-op receipt drifted for {table}")
        else:
            _validate_delete_receipt(operation, _receipt_from_document(value))
    phase_a_intent = journal.get("phase_a_intent")
    if phase_a_intent is not None:
        if not isinstance(phase_a_intent, Mapping) or set(phase_a_intent) != {
            "table",
            "parent_snapshot_id",
            "candidate_count",
        }:
            raise PurgeRefused("purge journal Phase A intent is malformed")
        if len(keys) >= len(operations):
            raise PurgeRefused("purge journal has a Phase A intent after all receipts")
        intended = operations[len(keys)]
        if dict(phase_a_intent) != {
            "table": intended.table,
            "parent_snapshot_id": intended.snapshot_id,
            "candidate_count": intended.candidate_count,
        } or intended.candidate_count <= 0:
            raise PurgeRefused("purge journal Phase A intent is not the exact next DELETE")
    complete_receipts = len(receipts) == len(operations)
    validated = journal.get("phase_a_validated") is True
    if journal.get("phase_a_validated") not in {True, False}:
        raise PurgeRefused("purge journal Phase A marker is not boolean")
    if validated != (status in {"phase_b", "complete"}) or (
        validated and not complete_receipts
    ):
        raise PurgeRefused("purge journal Phase A validation state is inconsistent")
    if not validated and (
        journal.get("deleted_raw_objects") or journal.get("expired_snapshots")
    ):
        raise PurgeRefused("purge journal contains Phase B work before validation")
    if not validated and (
        journal.get("raw_delete_intent") is not None
        or journal.get("snapshot_expiration_intent") is not None
    ):
        raise PurgeRefused("purge journal contains Phase B intent before validation")
    for field in ("raw_delete_intent", "snapshot_expiration_intent"):
        value = journal.get(field)
        if value is not None and (not isinstance(value, str) or not value):
            raise PurgeRefused(f"purge journal {field} is malformed")
    if status == "complete" and "completed_at" not in journal:
        raise PurgeRefused("complete purge journal lacks completed_at")
    if status != "complete" and "completed_at" in journal:
        raise PurgeRefused("incomplete purge journal unexpectedly has completed_at")
    if status == "complete" and any(
        journal.get(field) is not None
        for field in (
            "phase_a_intent",
            "raw_delete_intent",
            "snapshot_expiration_intent",
        )
    ):
        raise PurgeRefused("complete purge journal contains an in-flight intent")

    try:
        phase_b = plan["phase_b"]
        raw_order = [
            str(item["path"])
            for item in [
                *phase_b["target_manifests"],
                *phase_b["blobs"],
            ]
        ]
        expiration_order = [
            f"{item['table']}:{_sha256(json.dumps(tuple(int(value) for value in item['snapshot_ids'])).encode())}"
            for item in phase_b["snapshots_to_expire"]
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise PurgeRefused("plan Phase B inventory is malformed") from exc
    if len(raw_order) != len(set(raw_order)) or len(expiration_order) != len(
        set(expiration_order)
    ):
        raise PurgeRefused("plan Phase B inventory contains duplicate mutations")
    deleted = journal.get("deleted_raw_objects")
    expired = journal.get("expired_snapshots")
    if (
        not isinstance(deleted, list)
        or not all(isinstance(item, str) for item in deleted)
        or deleted != raw_order[: len(deleted)]
        or not isinstance(expired, list)
        or not all(isinstance(item, str) for item in expired)
        or expired != expiration_order[: len(expired)]
    ):
        raise PurgeRefused("purge journal Phase B state is not an exact plan prefix")
    if journal.get("raw_delete_intent") is not None and journal.get(
        "raw_delete_intent"
    ) != (raw_order[len(deleted)] if len(deleted) < len(raw_order) else None):
        raise PurgeRefused("purge journal raw deletion intent is not the exact next item")
    if journal.get("snapshot_expiration_intent") is not None and journal.get(
        "snapshot_expiration_intent"
    ) != (
        expiration_order[len(expired)]
        if len(expired) < len(expiration_order)
        else None
    ):
        raise PurgeRefused(
            "purge journal snapshot intent is not the exact next item"
        )
    if (deleted or journal.get("raw_delete_intent") is not None) and expired != (
        expiration_order
    ):
        raise PurgeRefused("raw deletion began before every snapshot expiration")
    if status == "complete" and (
        deleted != raw_order or expired != expiration_order
    ):
        raise PurgeRefused("complete purge journal is missing planned Phase B work")


def _journal_has_mutation_state(journal: Mapping[str, Any]) -> bool:
    receipts = journal.get("phase_a_receipts")
    return bool(
        (
            isinstance(receipts, Mapping)
            and any(
                isinstance(value, Mapping)
                and value.get("operation") != "noop"
                for value in receipts.values()
            )
        )
        or journal.get("phase_a_intent") is not None
        or journal.get("phase_a_validated") is True
        or journal.get("deleted_raw_objects")
        or journal.get("raw_delete_intent") is not None
        or journal.get("expired_snapshots")
        or journal.get("snapshot_expiration_intent") is not None
    )


def _journal_has_committed_mutation(journal: Mapping[str, Any]) -> bool:
    receipts = journal.get("phase_a_receipts")
    return bool(
        (
            isinstance(receipts, Mapping)
            and any(
                isinstance(value, Mapping)
                and value.get("operation") == "delete"
                for value in receipts.values()
            )
        )
        or journal.get("phase_a_validated") is True
        or journal.get("deleted_raw_objects")
        or journal.get("expired_snapshots")
    )


def _receipt_from_document(value: object) -> DeleteReceipt:
    if not isinstance(value, Mapping):
        raise PurgeRefused("journal delete receipt is malformed")
    try:
        return DeleteReceipt(
            table=str(value["table"]),
            parent_snapshot_id=int(value["parent_snapshot_id"]),
            snapshot_id=int(value["snapshot_id"]),
            operation=str(value["operation"]),
            query_id=str(value["query_id"]),
            snapshot_query_id=str(value["snapshot_query_id"]),
            deleted_count=int(value["deleted_count"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PurgeRefused("journal delete receipt is malformed") from exc


def _validate_delete_receipt(
    operation: TableOperation, receipt: DeleteReceipt
) -> None:
    if (
        receipt.table != operation.table
        or receipt.parent_snapshot_id != operation.snapshot_id
        or receipt.snapshot_id == operation.snapshot_id
        or receipt.operation.casefold() != "delete"
        or not receipt.query_id
        or receipt.query_id != receipt.snapshot_query_id
        or receipt.deleted_count != operation.candidate_count
    ):
        raise PostDeleteVerificationError(
            f"{operation.table} DELETE snapshot ownership is unproven"
        )


def _expected_post_inspection(
    operation: TableOperation, receipt: DeleteReceipt | None
) -> TableInspection:
    return TableInspection(
        table=operation.table,
        snapshot_id=(receipt.snapshot_id if receipt else operation.snapshot_id),
        total_count=operation.total_count - operation.candidate_count,
        candidate_count=0,
    )


def _assert_inspection_equal(
    observed: TableInspection,
    expected: TableInspection,
    *,
    context: str,
) -> None:
    try:
        _validate_inspection(observed, table=expected.table)
    except PurgeRefused as exc:
        raise PurgeRefused(
            f"{expected.table} {context} drift: {exc}"
        ) from exc
    if observed != expected:
        raise PurgeRefused(
            f"{expected.table} {context} drift: expected={expected}, observed={observed}"
        )


def _revalidate_admission(
    plan: Mapping[str, Any],
    backend: PurgeBackend,
    planned_evidence: Sequence[ProtectedEvidence],
    *,
    now: datetime,
) -> None:
    backend.assert_quiescent(WRITER_DAG_IDS, source="fotmob")
    live_evidence = _validate_protected_evidence(
        backend.load_protected_evidence(PURGE_COMPETITION_IDS), now=now
    )
    if not _same_evidence(planned_evidence, live_evidence):
        raise PurgeRefused("current structural-female evidence drifted from the plan")
    _validate_live_protected_survivors(
        backend, plan["phase_a"]["protected_survivors"]
    )
    _validate_global_preservation(plan, backend)


def _phase_b_items(plan: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    phase_b = plan["phase_b"]
    raw = phase_b[key]
    if not isinstance(raw, list) or not all(isinstance(item, Mapping) for item in raw):
        raise PurgeRefused(f"Phase B {key} inventory is malformed")
    return list(raw)


def apply_plan(
    plan: Mapping[str, Any],
    backend: PurgeBackend,
    *,
    supplied_sha256: str,
    journal_path: Path,
    deployment_report: Path | None = None,
    scheduled_observation_report: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Apply an exact plan with crash-safe two-phase journal checkpoints."""

    clock = _utc(now or datetime.now(timezone.utc), field="now")
    evidence, operations = _validate_plan(
        plan,
        supplied_sha256=supplied_sha256,
        now=clock,
        allow_expired_recovery=True,
    )
    _revalidate_scheduled_observation(
        plan,
        deployment_report=deployment_report,
        scheduled_observation_report=scheduled_observation_report,
    )
    backend.assert_live_scheduled_observation(_plan_scheduled_observation(plan))
    plan_sha256 = str(plan["plan_sha256"])
    expired = clock >= _timestamp(plan.get("expires_at"), field="expires_at")
    if expired and not Path(journal_path).exists():
        raise PurgeRefused("purge plan has expired")
    journal = _load_journal(Path(journal_path), plan_sha256=plan_sha256)
    _validate_journal_state(journal, operations, plan)

    # Always inspect the journaled external fence, even for an otherwise empty
    # journal.  The process can die immediately after ControlStore commits the
    # lock and before the first Phase-A write-ahead intent.
    journaled_fence = journal.get("apply_fence_generation_id")
    fence_token = backend.recover_apply_fence(plan_sha256, journaled_fence)
    if journaled_fence is not None and fence_token is None:
        # The exact prior generation is terminal/released.  Clearing it permits
        # a fresh journaled generation on a safe retry or partial recovery.
        journal["apply_fence_generation_id"] = None
        _atomic_json(Path(journal_path), journal)

    def acquire_journaled_fence() -> str:
        generation_id = journal.get("apply_fence_generation_id")
        if generation_id is None:
            generation_id = str(uuid.uuid4())
            journal["apply_fence_generation_id"] = generation_id
            _atomic_json(Path(journal_path), journal)
        return backend.acquire_apply_fence(plan_sha256, str(generation_id))

    def release_fence_before_mutation() -> None:
        nonlocal fence_token
        if fence_token is None or _journal_has_mutation_state(journal):
            return
        backend.release_apply_fence(fence_token)
        fence_token = None
        journal["apply_fence_generation_id"] = None
        _atomic_json(Path(journal_path), journal)

    if expired and not _journal_has_mutation_state(journal):
        if fence_token is not None:
            backend.release_apply_fence(fence_token)
            journal["apply_fence_generation_id"] = None
            _atomic_json(Path(journal_path), journal)
        raise PurgeRefused("purge plan expired before any recoverable mutation")
    if (
        journal.get("status") != "complete"
        and _journal_has_mutation_state(journal)
        and fence_token is None
    ):
        # The exact journaled ControlStore generation is a lock understood by
        # every FotMob publication initializer.  It spans the first possible
        # DELETE through the final durable Phase-B receipt, and is deliberately
        # left active on an incomplete/ambiguous failure so writers cannot race
        # a partially applied purge.  A same-plan resume reacquires it
        # idempotently.
        fence_token = acquire_journaled_fence()

    try:
        _revalidate_admission(plan, backend, evidence, now=clock)
        predicates = _table_predicates(evidence)
        receipt_documents = journal["phase_a_receipts"]

        has_delete_receipt = any(
            operation.candidate_count > 0 and operation.table in receipt_documents
            for operation in operations
        )
        if (
            not has_delete_receipt
            and journal.get("phase_a_intent") is None
            and journal.get("phase_a_validated") is not True
        ):
            # Before the first destructive SQL, independently regenerate every
            # state-derived plan section.  A matching SHA authenticates bytes, but
            # only this live reconstruction proves the submitted plan was actually
            # derived from current Bronze/raw reachability rather than hand-forged.
            try:
                expected_observation = _plan_scheduled_observation(plan)
                fresh = build_plan(
                    backend,
                    now=clock,
                    deployment_report=deployment_report
                    or Path(expected_observation.deployment_report_path),
                    scheduled_observation_report=scheduled_observation_report
                    or Path(expected_observation.path),
                )
            except PurgeRefused as exc:
                raise PurgeRefused(
                    f"live plan reconstruction drifted: {exc}"
                ) from exc
            for section in (
                "protected_evidence",
                "global_preservation",
                "phase_a",
                "phase_b",
            ):
                if fresh.get(section) != plan.get(section):
                    raise PurgeRefused(
                        f"submitted plan {section} drifted from live reconstruction"
                    )
    except Exception:
        release_fence_before_mutation()
        raise

    phase_a_intent = journal.get("phase_a_intent")
    if phase_a_intent is not None:
        intended = operations[len(receipt_documents)]
        recovered = backend.recover_delete(intended)
        if recovered is not None:
            _validate_delete_receipt(intended, recovered)
            observed = backend.inspect_table(
                intended.table, predicates[intended.table]
            )
            try:
                _assert_inspection_equal(
                    observed,
                    _expected_post_inspection(intended, recovered),
                    context="write-ahead DELETE recovery",
                )
            except PurgeRefused as exc:
                raise PostDeleteVerificationError(str(exc)) from exc
            receipt_documents[intended.table] = asdict(recovered)
            journal["phase_a_intent"] = None
            _atomic_json(Path(journal_path), journal)
    if expired and not _journal_has_committed_mutation(journal):
        if fence_token is not None:
            backend.release_apply_fence(fence_token)
            fence_token = None
            journal["apply_fence_generation_id"] = None
            _atomic_json(Path(journal_path), journal)
        raise PurgeRefused(
            "expired purge recovery has no proven committed mutation"
        )

    # Validate every table before the first new DELETE, including receipts from
    # a prior crashed invocation.  This prevents a later-table drift from
    # discovering itself only after an earlier table was already mutated.
    try:
        for operation in operations:
            receipt_value = receipt_documents.get(operation.table)
            if receipt_value is None:
                expected = TableInspection(
                    table=operation.table,
                    snapshot_id=operation.snapshot_id,
                    total_count=operation.total_count,
                    candidate_count=operation.candidate_count,
                )
            else:
                if str(receipt_value.get("operation")) == "noop":
                    receipt = None
                else:
                    receipt = _receipt_from_document(receipt_value)
                    _validate_delete_receipt(operation, receipt)
                expected = _expected_post_inspection(operation, receipt)
            observed = backend.inspect_table(
                operation.table, predicates[operation.table]
            )
            _assert_inspection_equal(observed, expected, context="pre-apply")
    except Exception:
        release_fence_before_mutation()
        raise

    if journal.get("status") != "complete" and fence_token is None:
        fence_token = acquire_journaled_fence()
        try:
            backend.invalidate_raw_inventory()
            expected_observation = _plan_scheduled_observation(plan)
            fresh_after_fence = build_plan(
                backend,
                now=clock,
                deployment_report=deployment_report
                or Path(expected_observation.deployment_report_path),
                scheduled_observation_report=scheduled_observation_report
                or Path(expected_observation.path),
            )
            for section in (
                "protected_evidence",
                "global_preservation",
                "phase_a",
                "phase_b",
            ):
                if fresh_after_fence.get(section) != plan.get(section):
                    raise PurgeRefused(
                        f"submitted plan {section} drifted after fence acquisition"
                    )
            _revalidate_admission(plan, backend, evidence, now=clock)
            # Close the read-check/fence-acquisition race: after the singleton
            # publication lock is ours, prove every Phase-A pre-state once more.
            for operation in operations:
                receipt_value = receipt_documents.get(operation.table)
                if receipt_value is None:
                    expected = TableInspection(
                        table=operation.table,
                        snapshot_id=operation.snapshot_id,
                        total_count=operation.total_count,
                        candidate_count=operation.candidate_count,
                    )
                elif str(receipt_value.get("operation")) == "noop":
                    expected = _expected_post_inspection(operation, None)
                else:
                    receipt = _receipt_from_document(receipt_value)
                    _validate_delete_receipt(operation, receipt)
                    expected = _expected_post_inspection(operation, receipt)
                observed = backend.inspect_table(
                    operation.table, predicates[operation.table]
                )
                _assert_inspection_equal(
                    observed, expected, context="post-fence pre-apply"
                )
        except Exception:
            backend.release_apply_fence(fence_token)
            fence_token = None
            journal["apply_fence_generation_id"] = None
            _atomic_json(Path(journal_path), journal)
            raise

    for operation in operations:
        if operation.table in receipt_documents:
            continue
        if operation.candidate_count == 0:
            # A no-op has no child snapshot and therefore no expiration entry.
            receipt_documents[operation.table] = {
                "table": operation.table,
                "parent_snapshot_id": operation.snapshot_id,
                "snapshot_id": operation.snapshot_id,
                "operation": "noop",
                "query_id": "noop",
                "snapshot_query_id": "noop",
                "deleted_count": 0,
            }
            _atomic_json(Path(journal_path), journal)
            continue
        expected_intent = {
            "table": operation.table,
            "parent_snapshot_id": operation.snapshot_id,
            "candidate_count": operation.candidate_count,
        }
        if journal.get("phase_a_intent") is None:
            journal["phase_a_intent"] = expected_intent
            _atomic_json(Path(journal_path), journal)
        elif journal.get("phase_a_intent") != expected_intent:
            raise PurgeRefused("Phase A write-ahead intent drifted before DELETE")
        backend.assert_quiescent(WRITER_DAG_IDS, source="fotmob")
        try:
            receipt = backend.delete_table(operation)
        except Exception as exc:
            raise PostDeleteVerificationError(
                f"{operation.table} DELETE outcome is unknown"
            ) from exc
        _validate_delete_receipt(operation, receipt)
        observed = backend.inspect_table(
            operation.table, predicates[operation.table]
        )
        try:
            _assert_inspection_equal(
                observed,
                _expected_post_inspection(operation, receipt),
                context="post-delete",
            )
        except PurgeRefused as exc:
            raise PostDeleteVerificationError(str(exc)) from exc
        receipt_documents[operation.table] = asdict(receipt)
        journal["phase_a_intent"] = None
        _atomic_json(Path(journal_path), journal)

    # Phase A is durable only after all table post-states and the preserved
    # global/evidence state are observed together under a fresh quiescence gate.
    try:
        _revalidate_admission(plan, backend, evidence, now=clock)
        for operation in operations:
            receipt_value = receipt_documents[operation.table]
            if str(receipt_value.get("operation")) == "noop":
                receipt = None
            else:
                receipt = _receipt_from_document(receipt_value)
                _validate_delete_receipt(operation, receipt)
            observed = backend.inspect_table(
                operation.table, predicates[operation.table]
            )
            _assert_inspection_equal(
                observed,
                _expected_post_inspection(operation, receipt),
                context="Phase A validation",
            )
    except Exception:
        release_fence_before_mutation()
        raise
    if journal.get("phase_a_validated") is not True:
        journal["phase_a_validated"] = True
        journal["status"] = "phase_b"
        _atomic_json(Path(journal_path), journal)

    if journal.get("phase_a_validated") is not True:
        raise AssertionError("Phase B cannot run before durable Phase A validation")
    _revalidate_admission(plan, backend, evidence, now=clock)
    for operation in operations:
        receipt_value = receipt_documents[operation.table]
        receipt = (
            None
            if str(receipt_value.get("operation")) == "noop"
            else _receipt_from_document(receipt_value)
        )
        observed = backend.inspect_table(
            operation.table, predicates[operation.table]
        )
        _assert_inspection_equal(
            observed,
            _expected_post_inspection(operation, receipt),
            context="Phase B admission",
        )

    deleted_raw = set(str(item) for item in journal["deleted_raw_objects"])
    protected_paths: set[str] = set()
    for item in _phase_b_items(plan, "shared_exclusions"):
        for path_key, sha_key in (
            ("manifest_path", "manifest_sha256"),
            ("blob_path", "blob_sha256"),
        ):
            path = str(item[path_key])
            expected_sha = _require_sha256(item[sha_key], field=sha_key)
            if backend.raw_object_sha256(path) != expected_sha:
                raise PurgeRefused(f"protected raw object drifted or vanished: {path}")
            protected_paths.add(path)
    for item in _phase_b_items(plan, "shared_blob_exclusions"):
        path = str(item["path"])
        expected_sha = _require_sha256(item["sha256"], field="shared blob SHA-256")
        if backend.raw_object_sha256(path) != expected_sha:
            raise PurgeRefused(f"protected shared blob drifted or vanished: {path}")
        protected_paths.add(path)

    target_items = _phase_b_items(plan, "target_manifests")
    blob_items = _phase_b_items(plan, "blobs")
    allowed_raw_deletions = {
        str(item["path"]) for item in [*target_items, *blob_items]
    }
    if deleted_raw - allowed_raw_deletions:
        raise PurgeRefused("purge journal contains an unplanned raw deletion")
    raw_delete_order = [str(item["path"]) for item in [*target_items, *blob_items]]
    raw_intent = journal.get("raw_delete_intent")
    first_pending_raw = next(
        (path for path in raw_delete_order if path not in deleted_raw), None
    )
    if raw_intent is not None and raw_intent != first_pending_raw:
        raise PurgeRefused("purge journal raw deletion intent is out of order")
    for item in [*target_items, *blob_items]:
        path = str(item["path"])
        expected_sha = _require_sha256(item["sha256"], field="raw object SHA-256")
        observed_sha = backend.raw_object_sha256(path)
        if path in deleted_raw:
            if observed_sha is not None:
                raise PurgeRefused(f"journaled raw deletion reappeared: {path}")
        elif raw_intent == path and observed_sha is None:
            # The object deletion committed after its write-ahead intent, but
            # the process died before checkpointing the post-state.
            journal["deleted_raw_objects"].append(path)
            journal["raw_delete_intent"] = None
            deleted_raw.add(path)
            raw_intent = None
            _atomic_json(Path(journal_path), journal)
        elif observed_sha != expected_sha:
            raise PurgeRefused(f"raw deletion candidate drifted: {path}")
        if path in protected_paths:
            raise PurgeRefused(f"raw object is both protected and doomed: {path}")

    # Snapshot expiration is deliberately first in Phase B.  If the engine
    # cannot prove/perform exact retention work, no raw object has been removed.
    expired = set(str(item) for item in journal["expired_snapshots"])
    allowed_expiration_markers = {
        f"{item['table']}:{_sha256(json.dumps(tuple(int(value) for value in item['snapshot_ids'])).encode())}"
        for item in plan["phase_b"]["snapshots_to_expire"]
    }
    if expired - allowed_expiration_markers:
        raise PurgeRefused("purge journal contains an unplanned snapshot expiration")
    expiration_order = [
        f"{item['table']}:{_sha256(json.dumps(tuple(int(value) for value in item['snapshot_ids'])).encode())}"
        for item in plan["phase_b"]["snapshots_to_expire"]
    ]
    expiration_intent = journal.get("snapshot_expiration_intent")
    first_pending_expiration = next(
        (marker for marker in expiration_order if marker not in expired), None
    )
    if (
        expiration_intent is not None
        and expiration_intent != first_pending_expiration
    ):
        raise PurgeRefused("purge journal snapshot expiration intent is out of order")
    for item in plan["phase_b"]["snapshots_to_expire"]:
        table = str(item["table"])
        snapshot_ids = tuple(int(value) for value in item["snapshot_ids"])
        marker = f"{table}:{_sha256(json.dumps(snapshot_ids).encode())}"
        receipt = _receipt_from_document(receipt_documents[table])
        live_snapshot_ids = set(backend.load_snapshot_ids(table))
        if marker in expired:
            if live_snapshot_ids != {receipt.snapshot_id}:
                raise PurgeRefused(
                    f"{table} journaled snapshot expiration post-state drifted"
                )
            continue
        expected_before = {*snapshot_ids, receipt.snapshot_id}
        if live_snapshot_ids == {receipt.snapshot_id}:
            if expiration_intent != marker:
                raise PurgeRefused(
                    f"{table} snapshots vanished without a write-ahead intent"
                )
            # The engine committed expiration after its write-ahead intent but
            # the process crashed before checkpointing the exact post-state.
            journal["expired_snapshots"].append(marker)
            journal["snapshot_expiration_intent"] = None
            expired.add(marker)
            expiration_intent = None
            _atomic_json(Path(journal_path), journal)
            continue
        if live_snapshot_ids != expected_before:
            raise PurgeRefused(f"{table} snapshot inventory drifted before expiration")
        if expiration_intent is None:
            journal["snapshot_expiration_intent"] = marker
            expiration_intent = marker
            _atomic_json(Path(journal_path), journal)
        backend.assert_quiescent(WRITER_DAG_IDS, source="fotmob")
        try:
            backend.expire_snapshots(
                table, snapshot_ids, current_snapshot_id=receipt.snapshot_id
            )
        except Exception as exc:
            raise PostDeleteVerificationError(
                f"{table} snapshot expiration outcome is unknown"
            ) from exc
        if set(backend.load_snapshot_ids(table)) != {receipt.snapshot_id}:
            raise PurgeRefused(f"{table} snapshot expiration post-state is unproven")
        journal["expired_snapshots"].append(marker)
        journal["snapshot_expiration_intent"] = None
        expired.add(marker)
        expiration_intent = None
        _atomic_json(Path(journal_path), journal)

    for item in target_items:
        path = str(item["path"])
        if path in deleted_raw:
            continue
        expected_sha = str(item["sha256"])
        if raw_intent is None:
            journal["raw_delete_intent"] = path
            raw_intent = path
            _atomic_json(Path(journal_path), journal)
        backend.assert_quiescent(WRITER_DAG_IDS, source="fotmob")
        try:
            backend.delete_raw_object(path, expected_sha)
        except Exception as exc:
            raise PostDeleteVerificationError(
                f"raw target deletion outcome is unknown: {path}"
            ) from exc
        if backend.raw_object_sha256(path) is not None:
            raise PurgeRefused(f"raw target manifest still exists after delete: {path}")
        journal["deleted_raw_objects"].append(path)
        journal["raw_delete_intent"] = None
        deleted_raw.add(path)
        raw_intent = None
        _atomic_json(Path(journal_path), journal)

    # A target-manifest removal can commit even when its storage acknowledgement
    # is lost.  Rebuild reachability before the first blob decision so a
    # same-process recovery never counts a deleted target from an old index.
    backend.invalidate_raw_inventory()
    for item in blob_items:
        path = str(item["path"])
        if path in deleted_raw:
            continue
        if backend.raw_blob_ref_count(path) != 0:
            raise PurgeRefused(f"raw blob remains reachable and cannot be deleted: {path}")
        expected_sha = str(item["sha256"])
        if raw_intent is None:
            journal["raw_delete_intent"] = path
            raw_intent = path
            _atomic_json(Path(journal_path), journal)
        backend.assert_quiescent(WRITER_DAG_IDS, source="fotmob")
        try:
            backend.delete_raw_object(path, expected_sha)
        except Exception as exc:
            raise PostDeleteVerificationError(
                f"raw blob deletion outcome is unknown: {path}"
            ) from exc
        if backend.raw_object_sha256(path) is not None:
            raise PurgeRefused(f"raw blob still exists after delete: {path}")
        journal["deleted_raw_objects"].append(path)
        journal["raw_delete_intent"] = None
        deleted_raw.add(path)
        raw_intent = None
        _atomic_json(Path(journal_path), journal)

    if deleted_raw != allowed_raw_deletions or expired != allowed_expiration_markers:
        raise PurgeRefused("Phase B journal is incomplete after planned operations")
    if journal.get("status") != "complete":
        journal["status"] = "complete"
        journal["completed_at"] = _iso(clock)
        _atomic_json(Path(journal_path), journal)
    if fence_token is not None:
        backend.release_apply_fence(fence_token)
        journal["apply_fence_generation_id"] = None
        _atomic_json(Path(journal_path), journal)
    return journal


@dataclass(frozen=True)
class _SnapshotMetadata:
    snapshot_id: int
    parent_snapshot_id: int | None
    operation: str
    summary: Mapping[str, str]


class TrinoAirflowRawBackend:
    """Production adapter; imports external clients only when CLI execution asks."""

    def __init__(
        self,
        connection: Any,
        raw_store: Any,
        *,
        catalog: str = "iceberg",
        schema: str = "bronze",
        run: Callable[..., Any] = subprocess.run,
    ) -> None:
        if _IDENTIFIER_RE.fullmatch(catalog) is None or _IDENTIFIER_RE.fullmatch(
            schema
        ) is None:
            raise PurgeRefused("catalog and schema must be simple SQL identifiers")
        self.connection = connection
        self.raw_store = raw_store
        self.catalog = catalog
        self.schema = schema
        self._apply_fence_generation: str | None = None
        self._apply_fence_binding: dict[str, Any] | None = None
        self._raw_targets_cache: dict[str, RawTargetObject] | None = None
        self._validated_raw_targets: set[str] = set()
        self._validated_raw_blobs: set[str] = set()
        self._run = run

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "TrinoAirflowRawBackend":
        from scrapers.fotmob.raw_store import FotMobRawStore

        raw_uri = str(args.raw_store_uri or os.environ.get("FOTMOB_RAW_STORE_URI", ""))
        if not raw_uri.strip():
            raise PurgeRefused(
                "FOTMOB_RAW_STORE_URI or --raw-store-uri is required"
            )
        return cls(
            _trino_connection(catalog=args.catalog, schema=args.schema),
            FotMobRawStore.from_uri(raw_uri),
            catalog=args.catalog,
            schema=args.schema,
        )

    def close(self) -> None:
        self.connection.close()

    def _table(self, table: str, *, suffix: str = "") -> str:
        if table not in set(PHASE_A_TABLES) | set(GLOBAL_PRESERVE_TABLES):
            raise PurgeRefused(f"table is outside the immutable purge set: {table}")
        name = table + suffix
        return f'"{self.catalog}"."{self.schema}"."{name}"'

    def _query(self, sql: str) -> list[tuple[Any, ...]]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            return [tuple(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _snapshot(self, table: str) -> _SnapshotMetadata:
        rows = self._query(
            f"""
            SELECT refs.snapshot_id, snapshots.parent_id, snapshots.operation,
                   snapshots.summary
            FROM {self._table(table, suffix='$refs')} refs
            JOIN {self._table(table, suffix='$snapshots')} snapshots
              ON snapshots.snapshot_id = refs.snapshot_id
            WHERE refs.name = 'main' AND refs.type = 'BRANCH'
            """
        )
        if len(rows) != 1 or len(rows[0]) != 4 or rows[0][0] is None:
            raise PurgeRefused(f"cannot establish current main snapshot for {table}")
        summary = rows[0][3]
        if not isinstance(summary, Mapping):
            raise PurgeRefused(f"Iceberg snapshot summary is malformed for {table}")
        return _SnapshotMetadata(
            snapshot_id=int(rows[0][0]),
            parent_snapshot_id=(None if rows[0][1] is None else int(rows[0][1])),
            operation=str(rows[0][2]),
            summary={str(key): str(value) for key, value in summary.items()},
        )

    def assert_live_scheduled_observation(
        self, observation: ScheduledObservation
    ) -> None:
        """Independently prove report lineage from live scheduler/control state.

        The protected JSON is an audit artifact, not an authority: a forged
        success row cannot pass this database/control-plane readback.
        """

        marker_rows = self._query(
            'SELECT COUNT(*) FROM '
            f'"{self.catalog}"."{self.schema}"."fotmob_runtime_deployments" '
            f"WHERE deployment_id = {_sql_literal(observation.deployment_id)} "
            f"AND git_sha = {_sql_literal(observation.git_sha)} "
            "AND scheduler_container_id = "
            f"{_sql_literal(observation.scheduler_container_id)}"
        )
        if (
            len(marker_rows) != 1
            or len(marker_rows[0]) != 1
            or int(marker_rows[0][0]) != 1
        ):
            raise PurgeRefused("live automatic deployment identity differs from report")

        # The CLI host is deliberately not an Airflow authority.  The isolated
        # and shared metadata databases are separate, so execute each exact
        # query in the immutable container ID admitted by the deployment report.
        deployment_raw, deployment = _read_protected_json_report(
            Path(observation.deployment_report_path), label="deployment report"
        )
        deployment_sha256, deployment_identity, rollout = (
            _validated_deployment_payload(deployment_raw, deployment)
        )
        if (
            deployment_sha256 != observation.deployment_report_sha256
            or deployment_identity
            != {
                "deployment_id": observation.deployment_id,
                "git_sha": observation.git_sha,
                "scheduler_container_id": observation.scheduler_container_id,
            }
        ):
            raise PurgeRefused(
                "live scheduled observation deployment report drifted"
            )
        handoff = deployment.get("shared_handoff_final")
        shared_id = (
            handoff.get("shared_scheduler_container")
            if isinstance(handoff, Mapping)
            else None
        )
        if re.fullmatch(r"[0-9a-f]{64}", str(shared_id or "")) is None:
            raise PurgeRefused("deployment report has no admitted shared scheduler")
        activation_at = _timestamp(
            rollout.get("owner_at"), field="automatic rollout owner_at"
        ).isoformat()
        isolated = self._container_proof(
            observation.scheduler_container_id,
            self._isolated_observation_script(observation, activation_at=activation_at),
        )
        shared = self._container_proof(
            str(shared_id), self._shared_observation_script(observation)
        )
        if isolated.get("passed") is not True or shared.get("passed") is not True:
            raise PurgeRefused("live scheduled automatic observation is unproven")
        self._live_isolated_scheduler_id = observation.scheduler_container_id
        self._live_shared_scheduler_id = str(shared_id)

    def _container_proof(self, container_id: str, script: str) -> Mapping[str, Any]:
        """Run a read-only proof inside one exact scheduler container."""

        try:
            completed = self._run(
                ("docker", "exec", container_id, "python", "-c", script),
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception as exc:
            raise PurgeRefused(
                "cannot independently read admitted scheduler container"
            ) from exc
        prefix = "FOTMOB_PURGE_PROOF="
        for line in reversed(str(getattr(completed, "stdout", "")).splitlines()):
            if line.startswith(prefix):
                try:
                    payload = json.loads(line[len(prefix) :])
                except json.JSONDecodeError as exc:
                    raise PurgeRefused("admitted scheduler proof is invalid JSON") from exc
                if isinstance(payload, Mapping):
                    return payload
        raise PurgeRefused("admitted scheduler did not emit a proof")

    @staticmethod
    def _proof_script(expected: Mapping[str, Any], checks: str) -> str:
        # All expected values are JSON literals; the generated program performs
        # exact run-id/XCom comparisons inside the metadata DB, not host state.
        return "\n".join(
            (
                "import json, os",
                "from datetime import datetime, timezone",
                "from airflow.models import DagRun, TaskInstance, XCom",
                "from airflow.settings import Session",
                "from scrapers.fbref.control import ControlStore",
                f"expected = json.loads({json.dumps(dict(expected), sort_keys=True)!r})",
                "def state(v): return str(getattr(v, 'value', v or '')).lower()",
                "def instant(v): return v.astimezone(timezone.utc).isoformat(timespec='microseconds') if v is not None else None",
                "def run(session, dag_id, run_id):",
                "    return session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id).one_or_none()",
                "def task(session, dag_id, run_id, task_id):",
                "    return session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == run_id, TaskInstance.task_id == task_id).one_or_none()",
                "def xcom(session, dag_id, run_id, task_id):",
                "    row = session.query(XCom).filter(XCom.dag_id == dag_id, XCom.run_id == run_id, XCom.task_id == task_id, XCom.key == 'return_value').order_by(XCom.timestamp.desc()).first()",
                "    return XCom.deserialize_value(row) if row is not None else None",
                "def first_admitted_daily_owner(session, activation_at):",
                "    rows = session.query(DagRun).filter(DagRun.dag_id == 'dag_orchestrate_fotmob', DagRun.state == 'success').order_by(DagRun.start_date.asc()).all()",
                "    for candidate in rows:",
                "        attested = task(session, candidate.dag_id, candidate.run_id, 'attest_isolated_runtime')",
                "        decision = xcom(session, candidate.dag_id, candidate.run_id, 'choose_fotmob_lane')",
                "        initialized = xcom(session, candidate.dag_id, candidate.run_id, 'initialize_fotmob_publication')",
                "        if state(candidate.run_type) == 'scheduled' and attested is not None and state(attested.state) == 'success' and attested.start_date is not None and attested.start_date.isoformat() >= activation_at and isinstance(decision, dict) and decision.get('lane') == 'daily' and isinstance(initialized, dict) and initialized.get('generation_id') and isinstance(initialized.get('binding'), dict):",
                "            return candidate",
                "    return None",
                "session = Session()",
                "try:",
                "    passed = bool(" + checks + ")",
                "finally:",
                "    session.close()",
                "print('FOTMOB_PURGE_PROOF=' + json.dumps({'passed': passed}, sort_keys=True))",
            )
        )

    def _isolated_observation_script(
        self, observation: ScheduledObservation, *, activation_at: str
    ) -> str:
        expected = {
            "deployment_id": observation.deployment_id,
            "git_sha": observation.git_sha,
            "scheduler_container_id": observation.scheduler_container_id,
            "generation_id": observation.generation_id,
            "binding": dict(observation.publication_binding),
            "runs": observation.runs,
            "activation_at": activation_at,
        }
        checks = " and ".join(
            (
                "os.environ.get('FOTMOB_ISOLATED_STACK') == '1'",
                "os.environ.get('FOTMOB_DEPLOYMENT_ID') == expected['deployment_id']",
                "os.environ.get('FOTMOB_DEPLOY_GIT_SHA') == expected['git_sha']",
                "(owner := run(session, 'dag_orchestrate_fotmob', expected['runs']['owner']['run_id'])) is not None",
                "state(owner.state) == 'success' and state(owner.run_type) == 'scheduled'",
                "(attest_task := task(session, 'dag_orchestrate_fotmob', owner.run_id, 'attest_isolated_runtime')) is not None and state(attest_task.state) == 'success' and attest_task.start_date is not None and attest_task.start_date.isoformat() >= expected['activation_at']",
                "(earliest := first_admitted_daily_owner(session, expected['activation_at'])) is not None and earliest.run_id == owner.run_id",
                "(lane := xcom(session, 'dag_orchestrate_fotmob', owner.run_id, 'choose_fotmob_lane')) is not None and lane.get('lane') == 'daily'",
                "(attestation := xcom(session, 'dag_orchestrate_fotmob', owner.run_id, 'attest_isolated_runtime')) is not None and all(attestation.get(k) == expected[k] for k in ('deployment_id', 'git_sha', 'scheduler_container_id'))",
                "(initializer := xcom(session, 'dag_orchestrate_fotmob', owner.run_id, 'initialize_fotmob_publication')) is not None and initializer.get('generation_id') == expected['generation_id'] and initializer.get('binding') == expected['binding']",
                "all((child := run(session, expected['runs'][name]['dag_id'], expected['runs'][name]['run_id'])) is not None and state(child.state) == 'success' and isinstance(child.conf, dict) and child.conf.get('fotmob_publication', {}).get('generation_id') == expected['generation_id'] and child.conf.get('fotmob_publication', {}).get('binding') == expected['binding'] for name in ('ingest', 'silver'))",
                "(publication := ControlStore.from_env().get_publication_generation(expected['generation_id'], source='fotmob')) is not None and publication.get('generation_id') == expected['generation_id'] and publication.get('source') == 'fotmob' and publication.get('binding') == expected['binding'] and publication.get('status') == 'succeeded' and publication.get('phase') == 'published' and publication.get('active') is False and publication.get('lock_active') is False and publication.get('consumer') == {'dag_id': 'dag_sofascore_pipeline', 'run_id': expected['runs']['sofascore']['run_id']}",
            )
        )
        return self._proof_script(expected, checks)

    def _shared_observation_script(self, observation: ScheduledObservation) -> str:
        expected = {
            "generation_id": observation.generation_id,
            "binding": dict(observation.publication_binding),
            "runs": observation.runs,
        }
        checks = " and ".join(
            (
                "(sofa := run(session, 'dag_sofascore_pipeline', expected['runs']['sofascore']['run_id'])) is not None and state(sofa.state) == 'success' and state(sofa.run_type) == 'scheduled' and instant(sofa.logical_date) == expected['binding']['data_interval_start'] and instant(sofa.data_interval_start) == expected['binding']['data_interval_start'] and instant(sofa.data_interval_end) == expected['binding']['data_interval_end']",
                "(finalizer := task(session, 'dag_sofascore_pipeline', sofa.run_id, 'finalize_fotmob_publication')) is not None and state(finalizer.state) == 'success'",
                "(final_state := xcom(session, 'dag_sofascore_pipeline', sofa.run_id, 'finalize_fotmob_publication')) is not None and final_state.get('generation_id') == expected['generation_id'] and final_state.get('phase') == 'published' and final_state.get('released') is True",
            )
        )
        return self._proof_script(expected, checks)

    def _quiescence_script(
        self,
        dag_ids: Sequence[str],
        *,
        control: bool,
        active_dag_ids: Sequence[str] | None = None,
        expected_pause_states: Mapping[str, bool] | None = None,
    ) -> str:
        expected = {
            "dag_ids": list(dag_ids),
            "active_dag_ids": list(active_dag_ids or dag_ids),
            "expected_pause_states": dict(expected_pause_states or {
                dag_id: True for dag_id in dag_ids
            }),
            "fence_generation": self._apply_fence_generation,
            "fence_binding": self._apply_fence_binding,
        }
        control_lines = (
            (
                "    publication = ControlStore.from_env().assert_no_active_publication_generation(source='fotmob')",
                "    control_ok = publication.get('safe') is True and publication.get('active') is not True",
            )
            if self._apply_fence_generation is None
            else (
                "    publication = ControlStore.from_env().get_publication_generation(expected['fence_generation'], source='fotmob')",
                "    control_ok = isinstance(publication, dict) and publication.get('generation_id') == expected['fence_generation'] and publication.get('binding') == expected['fence_binding'] and publication.get('status') == 'running' and publication.get('phase') == 'writing' and publication.get('active') is True and publication.get('owner_dag_id') == 'fotmob_legacy_purge'",
            )
        ) if control else ("    control_ok = True",)
        return "\n".join(
            (
                "import json",
                "from airflow.models import DagModel, DagRun, TaskInstance",
                "from airflow.settings import Session",
                "from sqlalchemy import text",
                "from scrapers.fbref.control import ControlStore",
                f"expected = json.loads({json.dumps(expected, sort_keys=True)!r})",
                "session = Session()",
                "try:",
                "    session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))",
                "    dag_ids = expected['dag_ids']",
                "    models = {row.dag_id: bool(row.is_paused) for row in session.query(DagModel).filter(DagModel.dag_id.in_(dag_ids)).all()}",
                "    active = session.query(DagRun).filter(DagRun.dag_id.in_(expected['active_dag_ids']), DagRun.state.in_(('queued', 'running'))).count()",
                "    active_tasks = session.query(TaskInstance).filter(TaskInstance.dag_id.in_(expected['active_dag_ids']), TaskInstance.state.in_(('queued', 'running', 'scheduled', 'deferred', 'up_for_retry', 'up_for_reschedule', 'restarting'))).count()",
                *control_lines,
                "    passed = models == expected['expected_pause_states'] and active == 0 and active_tasks == 0 and control_ok",
                "    session.commit()",
                "except Exception:",
                "    session.rollback()",
                "    raise",
                "finally:",
                "    session.close()",
                "print('FOTMOB_PURGE_PROOF=' + json.dumps({'passed': passed}, sort_keys=True))",
            )
        )

    def assert_quiescent(
        self, writer_dag_ids: Sequence[str], *, source: str
    ) -> None:
        if tuple(writer_dag_ids) != WRITER_DAG_IDS or source != "fotmob":
            raise PurgeRefused("quiescence request differs from the FotMob writer set")
        isolated_id = getattr(self, "_live_isolated_scheduler_id", None)
        shared_id = getattr(self, "_live_shared_scheduler_id", None)
        if not isolated_id or not shared_id:
            raise PurgeRefused("quiescence has no admitted scheduler identities")
        isolated = self._container_proof(
            isolated_id, self._quiescence_script(WRITER_DAG_IDS[:6], control=True)
        )
        shared = self._container_proof(
            shared_id,
            self._quiescence_script(
                tuple(SHARED_PAUSE_STATES),
                control=False,
                active_dag_ids=SHARED_STATE_DAGS,
                expected_pause_states=SHARED_PAUSE_STATES,
            ),
        )
        if isolated.get("passed") is not True or shared.get("passed") is not True:
            raise PurgeRefused("cannot prove dual-metadata writer/publication quiescence")

    @staticmethod
    def _fence_identity(
        plan_sha256: str, fence_generation_id: str
    ) -> tuple[str, dict[str, Any]]:
        plan_hash = _require_sha256(plan_sha256, field="plan SHA-256")
        try:
            generation_id = str(uuid.UUID(str(fence_generation_id)))
        except (AttributeError, TypeError, ValueError) as exc:
            raise PurgeRefused("purge fence generation ID is invalid") from exc
        if generation_id != fence_generation_id:
            raise PurgeRefused("purge fence generation ID is not canonical")
        binding = {
            "schema_version": "fotmob-legacy-purge-fence-v1",
            "plan_sha256": plan_hash,
            "competition_ids": list(PURGE_COMPETITION_IDS),
        }
        return generation_id, binding

    def _container_fence_state(
        self,
        *,
        action: str,
        generation_id: str,
        binding: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """Mutate/read ControlStore only inside the admitted isolated runtime."""

        container_id = getattr(self, "_live_isolated_scheduler_id", None)
        if not container_id:
            raise PurgeRefused("purge fence has no admitted isolated scheduler")
        if action == "initialize":
            expression = (
                "store.initialize_publication_generation("
                "generation_id,dag_id='fotmob_legacy_purge',binding=binding,"
                "source='fotmob',ttl_seconds=14*24*60*60)"
            )
        elif action == "get":
            expression = (
                "store.get_publication_generation(generation_id,source='fotmob')"
            )
        elif action == "release":
            expression = (
                "store.fail_publication_generation("
                "generation_id,safe_to_release=True,source='fotmob')"
            )
        else:  # pragma: no cover - internal callers use the closed set above
            raise PurgeRefused("unknown purge fence action")
        marker = "FOTMOB_PURGE_FENCE="
        code = "\n".join(
            (
                "import json",
                "from scrapers.fbref.control import ControlStore",
                f"generation_id = {generation_id!r}",
                f"binding = json.loads({json.dumps(dict(binding), sort_keys=True)!r})",
                "store = ControlStore.from_env()",
                f"state = {expression}",
                f"print({marker!r} + json.dumps(state, default=str, sort_keys=True))",
            )
        )
        try:
            completed = self._run(
                ("docker", "exec", container_id, "python", "-c", code),
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception as exc:
            raise PurgeRefused(
                f"cannot {action} the FotMob purge apply fence"
            ) from exc
        for line in reversed(str(completed.stdout).splitlines()):
            if not line.startswith(marker):
                continue
            try:
                state = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise PurgeRefused("purge fence returned invalid JSON") from exc
            if state is None or isinstance(state, Mapping):
                return state
        raise PurgeRefused("purge fence returned no exact state")

    def acquire_apply_fence(
        self, plan_sha256: str, fence_generation_id: str
    ) -> str:
        """Acquire the singleton FotMob publication lock for this exact plan."""

        if self._apply_fence_generation is not None:
            raise PurgeRefused("purge backend already owns an apply fence")
        generation_id, binding = self._fence_identity(
            plan_sha256, fence_generation_id
        )
        state = self._container_fence_state(
            action="initialize",
            generation_id=generation_id,
            binding=binding,
        )
        if not isinstance(state, Mapping):
            raise PurgeRefused("FotMob purge apply fence acquisition is absent")
        if (
            state.get("generation_id") != generation_id
            or state.get("binding") != binding
            or state.get("status") != "running"
            or state.get("phase") != "writing"
            or state.get("active") is not True
            or state.get("owner_dag_id") != "fotmob_legacy_purge"
        ):
            raise PurgeRefused("FotMob purge apply fence acquisition is unproven")
        self._apply_fence_generation = generation_id
        self._apply_fence_binding = binding
        try:
            self.assert_quiescent(WRITER_DAG_IDS, source="fotmob")
        except Exception:
            # Acquisition itself is the only mutation so far.  Releasing is
            # safe and avoids an hours-long outage on a paused/active-run drift.
            self.release_apply_fence(generation_id)
            raise
        return generation_id

    def recover_apply_fence(
        self, plan_sha256: str, fence_generation_id: str | None
    ) -> str | None:
        """Recognize an exact fence left after a complete-journal crash."""

        if self._apply_fence_generation is not None:
            raise PurgeRefused("purge backend already owns an apply fence")
        if fence_generation_id is None:
            return None
        generation_id, binding = self._fence_identity(
            plan_sha256, fence_generation_id
        )
        state = self._container_fence_state(
            action="get", generation_id=generation_id, binding=binding
        )
        if state is None:
            return None
        if state.get("binding") != binding:
            raise PurgeRefused("FotMob purge fence binding differs from the plan")
        if (
            state.get("status") == "failed"
            and state.get("phase") == "failed"
            and state.get("active") is False
        ):
            return None
        if (
            state.get("generation_id") != generation_id
            or state.get("status") != "running"
            or state.get("phase") != "writing"
            or state.get("active") is not True
            or state.get("owner_dag_id") != "fotmob_legacy_purge"
        ):
            raise PurgeRefused("FotMob purge fence terminal state is ambiguous")
        self._apply_fence_generation = generation_id
        self._apply_fence_binding = binding
        self.assert_quiescent(WRITER_DAG_IDS, source="fotmob")
        return generation_id

    def release_apply_fence(self, fence_token: str) -> None:
        """Release only the exact successful purge fence."""

        if fence_token != self._apply_fence_generation:
            raise PurgeRefused("cannot release a different purge apply fence")
        binding = self._apply_fence_binding
        if not isinstance(binding, Mapping):
            raise PurgeRefused("purge apply fence binding is absent")
        state = self._container_fence_state(
            action="release", generation_id=fence_token, binding=binding
        )
        if not isinstance(state, Mapping):
            raise PurgeRefused("FotMob purge apply fence release is absent")
        if state.get("released") is not True or state.get("active") is True:
            raise PurgeRefused("FotMob purge apply fence release is unproven")
        self._apply_fence_generation = None
        self._apply_fence_binding = None

    def load_protected_evidence(
        self, competition_ids: Sequence[int]
    ) -> Mapping[int, ProtectedEvidence]:
        if tuple(competition_ids) != PURGE_COMPETITION_IDS:
            raise PurgeRefused("evidence query differs from immutable purge IDs")
        rows = self._query(
            f"""
            SELECT CAST(c.competition_id AS VARCHAR), c._target_batch_id,
                   e._target_batch_id, e.profile_target_key,
                   e.profile_content_hash, c.scope_decision, e.decision,
                   e.source_gender, e.policy_rule, e.classifier_version,
                   e.observed_at
            FROM {self._table('fotmob_competitions', suffix='_current')} c
            JOIN {self._table('fotmob_competition_scope_observations', suffix='_current')} e
              ON {_same_purge_id('e.competition_id', 'c.competition_id')}
            WHERE {_ids_predicate(column='c.competition_id')}
            ORDER BY 1
            """
        )
        if len(rows) != len(PURGE_COMPETITION_IDS) or any(
            len(row) != 11 for row in rows
        ):
            raise PurgeRefused(
                "current catalog/evidence views do not contain exactly two rows"
            )
        output: dict[int, ProtectedEvidence] = {}
        for row in rows:
            observed = row[10]
            if isinstance(observed, datetime):
                observed_at = (
                    observed.replace(tzinfo=timezone.utc)
                    if observed.tzinfo is None
                    else observed.astimezone(timezone.utc)
                )
            else:
                observed_at = _timestamp(observed, field="observed_at")
            item = ProtectedEvidence(
                competition_id=_optional_competition_id(row[0]) or 0,
                catalog_batch_id=str(row[1]),
                evidence_batch_id=str(row[2]),
                profile_target_key=str(row[3]),
                profile_content_hash=str(row[4]),
                catalog_scope_decision=str(row[5]),
                evidence_decision=str(row[6]),
                source_gender=str(row[7]),
                policy_rule=str(row[8]),
                classifier_version=str(row[9]),
                observed_at=observed_at,
            )
            if item.competition_id in output:
                raise PurgeRefused("duplicate current structural evidence row")
            output[item.competition_id] = item
        return output

    def load_global_team_ids(
        self, competition_ids: Sequence[int]
    ) -> Sequence[str]:
        if tuple(competition_ids) != PURGE_COMPETITION_IDS:
            raise PurgeRefused("team inventory query differs from immutable purge IDs")
        rows = self._query(
            f"""
            SELECT DISTINCT CAST(team_id AS VARCHAR)
            FROM {self._table('fotmob_season_teams', suffix='_current')}
            WHERE {_ids_predicate()} AND team_id IS NOT NULL
            ORDER BY 1
            """
        )
        if any(len(row) != 1 for row in rows):
            raise PurgeRefused("global team inventory query returned invalid rows")
        return tuple(str(row[0]) for row in rows)

    def inspect_table(self, table: str, predicate: str) -> TableInspection:
        snapshot = self._snapshot(table)
        rows = self._query(
            f"SELECT COUNT(*), COUNT_IF({predicate}) FROM {self._table(table)}"
        )
        if len(rows) != 1 or len(rows[0]) != 2:
            raise PurgeRefused(f"row-count query returned invalid shape for {table}")
        return TableInspection(
            table=table,
            snapshot_id=snapshot.snapshot_id,
            total_count=int(rows[0][0]),
            candidate_count=int(rows[0][1]),
        )

    def count_matching_rows(self, table: str, predicate: str) -> int:
        rows = self._query(
            f"SELECT COUNT(*) FROM {self._table(table)} WHERE {predicate}"
        )
        if len(rows) != 1 or len(rows[0]) != 1:
            raise PurgeRefused(f"row-count query returned invalid shape for {table}")
        value = rows[0][0]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise PurgeRefused(f"row-count query returned invalid value for {table}")
        return value

    def inspect_global_tables(
        self, team_ids: Sequence[str]
    ) -> Mapping[str, TableInspection]:
        if len(tuple(team_ids)) != 23:
            raise PurgeRefused("global preservation requires the exact 23-team set")
        return {
            table: self.inspect_table(table, "FALSE")
            for table in GLOBAL_PRESERVE_TABLES
        }

    def load_snapshot_ids(self, table: str) -> Sequence[int]:
        rows = self._query(
            f"SELECT snapshot_id FROM {self._table(table, suffix='$snapshots')}"
        )
        if any(len(row) != 1 or row[0] is None for row in rows):
            raise PurgeRefused(f"snapshot inventory is malformed for {table}")
        values = tuple(sorted(int(row[0]) for row in rows))
        if len(values) != len(set(values)):
            raise PurgeRefused(f"snapshot inventory is duplicated for {table}")
        return values

    def load_manifest_references(self) -> Sequence[ManifestReference]:
        rows = self._query(
            f"""
            SELECT target_key, content_hash, target_type,
                   CAST(competition_id AS VARCHAR), batch_id, raw_uri
            FROM {self._table('fotmob_ingest_manifest')}
            WHERE target_key IS NOT NULL
            """
        )
        output: list[ManifestReference] = []
        for row in rows:
            if len(row) != 6:
                raise PurgeRefused("manifest reference query returned invalid rows")
            output.append(
                ManifestReference(
                    target_key=str(row[0]),
                    content_hash=(None if row[1] is None else str(row[1])),
                    target_type=("" if row[2] is None else str(row[2])),
                    competition_id=_optional_competition_id(row[3]),
                    batch_id=("" if row[4] is None else str(row[4])),
                    raw_uri=(None if row[5] is None else str(row[5])),
                )
            )
        return tuple(output)

    def invalidate_raw_inventory(self) -> None:
        self._raw_targets_cache = None
        self._validated_raw_targets.clear()
        self._validated_raw_blobs.clear()

    def load_raw_targets(self) -> Mapping[str, RawTargetObject]:
        from pyarrow import fs
        from scrapers.fotmob.raw_store import RawJsonRecord

        if self._raw_targets_cache is not None:
            return dict(self._raw_targets_cache)

        selector = fs.FileSelector(
            self.raw_store._path("targets/sha256"),
            recursive=True,
            allow_not_found=False,
        )
        output: dict[str, RawTargetObject] = {}
        root = PurePosixPath(self.raw_store.root)
        for info in self.raw_store.filesystem.get_file_info(selector):
            if info.type != fs.FileType.File:
                continue
            try:
                relative = str(PurePosixPath(info.path).relative_to(root))
            except ValueError as exc:
                raise PurgeRefused("raw filesystem escaped its configured root") from exc
            match = _TARGET_PATH_RE.fullmatch(relative)
            if match is None:
                raise PurgeRefused(f"unexpected object under raw targets/: {relative}")
            try:
                encoded = self.raw_store._read_bytes(relative)
            except Exception as exc:
                raise PurgeRefused(
                    f"raw target manifest vanished during inventory: {relative}"
                ) from exc
            try:
                payload = json.loads(encoded.decode("utf-8"))
                record = RawJsonRecord(**payload)
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
                raise PurgeRefused(
                    f"raw target manifest is malformed: {relative}"
                ) from exc
            target_key = str(record.target_key)
            content_hash = str(record.content_hash)
            blob_path = str(record.blob_key)
            if (
                record.manifest_version != "fotmob-raw-v1"
                or record.source != "fotmob"
                or target_key != match.group(2)
                or _SHA256_RE.fullmatch(content_hash) is None
                or blob_path != _blob_path(content_hash)
                or record.raw_uri != self.raw_store._uri(blob_path)
                or record.hash_algorithm != "sha256"
                or record.compression != "gzip"
                or _sha256(str(record.canonical_url).encode("utf-8")) != target_key
                or isinstance(record.decoded_bytes, bool)
                or not isinstance(record.decoded_bytes, int)
                or record.decoded_bytes < 0
                or isinstance(record.stored_bytes, bool)
                or not isinstance(record.stored_bytes, int)
                or record.stored_bytes < 0
            ):
                raise PurgeRefused(f"raw target manifest identity drifted: {relative}")
            if target_key in output:
                raise PurgeRefused(f"duplicate raw target key: {target_key}")
            output[target_key] = RawTargetObject(
                target_key=target_key,
                manifest_path=relative,
                manifest_sha256=_sha256(encoded),
                content_hash=content_hash,
                blob_path=blob_path,
                blob_sha256="",
                canonical_url=str(record.canonical_url),
            )
        self._raw_targets_cache = output
        return dict(output)

    def validate_raw_target(self, target_key: str) -> None:
        normalized = _require_sha256(target_key, field="raw target key")
        if normalized in self._validated_raw_targets:
            return
        indexed = self.load_raw_targets().get(normalized)
        if indexed is None:
            raise PurgeRefused(f"raw target is absent from the live index: {normalized}")
        try:
            _body, record = self.raw_store.load_target_key(normalized)
        except Exception as exc:
            raise PurgeRefused(
                f"raw target/blob integrity is unproven: {normalized}"
            ) from exc
        if (
            record.content_hash != indexed.content_hash
            or record.blob_key != indexed.blob_path
            or record.canonical_url != indexed.canonical_url
            or record.compression != "gzip"
        ):
            raise PurgeRefused(f"raw target changed during validation: {normalized}")
        self._validated_raw_targets.add(normalized)
        self._validated_raw_blobs.add(indexed.blob_path)

    def validate_raw_blob(self, blob_path: str, content_hash: str) -> None:
        candidate = self._validate_raw_path(blob_path)
        expected_hash = _require_sha256(content_hash, field="raw content hash")
        if candidate != _blob_path(expected_hash):
            raise PurgeRefused("raw blob path/content hash identity differs")
        if candidate in self._validated_raw_blobs:
            return
        try:
            compressed = self.raw_store._read_bytes(candidate)
            body = gzip.decompress(compressed)
        except Exception as exc:
            raise PurgeRefused(f"raw blob is not valid gzip: {candidate}") from exc
        if _sha256(body) != expected_hash:
            raise PurgeRefused(f"raw blob content hash drifted: {candidate}")
        self._validated_raw_blobs.add(candidate)

    def delete_table(self, operation: TableOperation) -> DeleteReceipt:
        before = self._snapshot(operation.table)
        if before.snapshot_id != operation.snapshot_id:
            raise PurgeRefused(f"{operation.table} snapshot raced before DELETE")
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"DELETE FROM {self._table(operation.table)} "
                f"WHERE {operation.predicate}"
            )
            query_after_execute = getattr(cursor, "query_id", None)
            cursor.fetchall()
            query_after_fetch = getattr(cursor, "query_id", None)
            deleted_count = int(getattr(cursor, "rowcount", -1))
        finally:
            cursor.close()
        if (
            not query_after_execute
            or not query_after_fetch
            or str(query_after_execute) != str(query_after_fetch)
        ):
            raise PostDeleteVerificationError(
                f"{operation.table} DELETE query ownership is unavailable"
            )
        after = self._snapshot(operation.table)
        return DeleteReceipt(
            table=operation.table,
            parent_snapshot_id=(
                -1 if after.parent_snapshot_id is None else after.parent_snapshot_id
            ),
            snapshot_id=after.snapshot_id,
            operation=after.operation,
            query_id=str(query_after_fetch),
            snapshot_query_id=str(after.summary.get("trino_query_id") or ""),
            deleted_count=deleted_count,
        )

    def recover_delete(self, operation: TableOperation) -> DeleteReceipt | None:
        """Recover an exact direct-child DELETE after a checkpoint crash."""

        current = self._snapshot(operation.table)
        if current.snapshot_id == operation.snapshot_id:
            return None
        query_id = str(current.summary.get("trino_query_id") or "")
        if (
            current.parent_snapshot_id != operation.snapshot_id
            or current.operation.casefold() != "delete"
            or not query_id
        ):
            raise PostDeleteVerificationError(
                f"{operation.table} in-flight DELETE ownership is unproven"
            )
        return DeleteReceipt(
            table=operation.table,
            parent_snapshot_id=operation.snapshot_id,
            snapshot_id=current.snapshot_id,
            operation=current.operation,
            query_id=query_id,
            snapshot_query_id=query_id,
            deleted_count=operation.candidate_count,
        )

    @staticmethod
    def _validate_raw_path(path: str) -> str:
        candidate = str(path)
        if _TARGET_PATH_RE.fullmatch(candidate) is None and _BLOB_PATH_RE.fullmatch(
            candidate
        ) is None:
            raise PurgeRefused("raw mutation path is outside content-addressed objects")
        return candidate

    def raw_object_sha256(self, path: str) -> str | None:
        candidate = self._validate_raw_path(path)
        try:
            if not self.raw_store._exists(candidate):
                return None
            return _sha256(self.raw_store._read_bytes(candidate))
        except Exception as exc:
            raise PurgeRefused(
                f"raw object hash cannot be established: {candidate}"
            ) from exc

    def raw_blob_ref_count(self, blob_path: str) -> int:
        candidate = self._validate_raw_path(blob_path)
        match = _BLOB_PATH_RE.fullmatch(candidate)
        if match is None:
            raise PurgeRefused("raw reachability query requires a blob path")
        target_references = sum(
            item.blob_path == candidate for item in self.load_raw_targets().values()
        )
        rows = self._query(
            f"SELECT COUNT(*) FROM {self._table('fotmob_ingest_manifest')} "
            f"WHERE content_hash = {_sql_literal(match.group(2))}"
        )
        if len(rows) != 1 or len(rows[0]) != 1:
            raise PurgeRefused("Bronze raw-blob reachability query is malformed")
        return target_references + int(rows[0][0])

    def delete_raw_object(self, path: str, expected_sha256: str) -> None:
        candidate = self._validate_raw_path(path)
        expected = _require_sha256(expected_sha256, field="raw object SHA-256")
        if self.raw_object_sha256(candidate) != expected:
            raise PurgeRefused(f"raw object hash drifted before deletion: {candidate}")
        target_match = _TARGET_PATH_RE.fullmatch(candidate)
        try:
            self.raw_store.filesystem.delete_file(self.raw_store._path(candidate))
        except Exception as exc:
            raise PostDeleteVerificationError(
                f"raw object deletion outcome is unknown: {candidate}"
            ) from exc
        finally:
            if target_match is not None:
                self.invalidate_raw_inventory()

    def expire_snapshots(
        self, table: str, snapshot_ids: Sequence[int], *, current_snapshot_id: int
    ) -> None:
        before = self._snapshot(table)
        planned = tuple(sorted(set(int(value) for value in snapshot_ids)))
        if (
            before.snapshot_id != current_snapshot_id
            or current_snapshot_id in planned
            or set(self.load_snapshot_ids(table)) != {*planned, current_snapshot_id}
        ):
            raise PurgeRefused(f"{table} snapshot-expiration lineage drifted")
        self._query("SET SESSION iceberg.expire_snapshots_min_retention = '0s'")
        self._query(
            f"ALTER TABLE {self._table(table)} EXECUTE expire_snapshots("
            "retention_threshold => '0s')"
        )
        after = self._snapshot(table)
        if after.snapshot_id != current_snapshot_id:
            raise PurgeRefused(f"{table} main snapshot moved during expiration")
        if set(self.load_snapshot_ids(table)) != {current_snapshot_id}:
            raise PurgeRefused(f"{table} planned snapshots were not exactly expired")


def _trino_tls_verify() -> bool | str:
    raw = os.environ.get("TRINO_TLS_VERIFY", "true").strip().casefold()
    if raw in {"0", "false", "no"}:
        raise PurgeRefused("TLS verification cannot be disabled for the purge tool")
    if raw not in {"1", "true", "yes"}:
        raise PurgeRefused("TRINO_TLS_VERIFY must be true/yes/1")
    for key in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return True


def _trino_connection(*, catalog: str, schema: str) -> Any:
    import trino
    from trino.auth import BasicAuthentication

    host = os.environ.get("TRINO_HOST", "").strip()
    if not host:
        raise PurgeRefused("TRINO_HOST is required")
    user = os.environ.get("TRINO_USER", "airflow").strip()
    password = os.environ.get("TRINO_PASSWORD", "")
    scheme = os.environ.get("TRINO_HTTP_SCHEME", "https").strip().casefold()
    if scheme != "https":
        raise PurgeRefused("the purge tool requires authenticated HTTPS Trino")
    if not password:
        raise PurgeRefused("TRINO_PASSWORD is required")
    return trino.dbapi.connect(
        host=host,
        port=int(os.environ.get("TRINO_PORT", "8443")),
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme="https",
        auth=BasicAuthentication(user, password),
        verify=_trino_tls_verify(),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="apply an exact reviewed plan (default: read-only plan only)",
    )
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--plan-sha256")
    parser.add_argument("--journal", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("fotmob-10557-10558-purge-plan.json"),
    )
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--raw-store-uri")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--trino-env-file", type=Path, required=True)
    parser.add_argument(
        "--compose-file",
        type=Path,
        default=runtime_binding.REPOSITORY_ROOT
        / "deploy"
        / "fotmob"
        / "airflow.compose.yaml",
    )
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument(
        "--deployment-report",
        type=Path,
        required=True,
        help="Protected current FotMob deployment report",
    )
    parser.add_argument(
        "--scheduled-observation-report",
        type=Path,
        required=True,
        help="Protected first successful scheduled automatic-daily report",
    )
    return parser


def _load_purge_environment(args: argparse.Namespace) -> str:
    """Load only explicit raw/Trino settings and discard ambient authority."""

    try:
        lines = args.env_file.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise PurgeRefused(f"cannot read purge env file: {exc}") from exc
    allowed = set(runtime_binding.PURGE_RAW_ENV_KEYS) | {"ICEBERG_WAREHOUSE"}
    parsed: dict[str, str] = {}
    for line_number, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in allowed:
            continue
        value = value.strip()
        if value[:1] in {"'", '"'}:
            if len(value) < 2 or value[-1] != value[0]:
                raise PurgeRefused(
                    f"{args.env_file}:{line_number}: unterminated {key} value"
                )
            value = value[1:-1]
        parsed[key] = value
    raw_uri = str(args.raw_store_uri or parsed.get("FOTMOB_RAW_STORE_URI") or "")
    if not raw_uri:
        warehouse = parsed.get("ICEBERG_WAREHOUSE", "").strip().strip("/")
        if warehouse:
            raw_uri = f"s3://{warehouse}/raw/fotmob"
    values = {
        "FOTMOB_RAW_STORE_URI": raw_uri,
        "FOTMOB_RAW_S3_ENDPOINT": parsed.get(
            "FOTMOB_RAW_S3_ENDPOINT", "seaweedfs:8333"
        ),
        "FOTMOB_RAW_S3_SCHEME": parsed.get("FOTMOB_RAW_S3_SCHEME", "http"),
        "FOTMOB_RAW_S3_REGION": parsed.get("FOTMOB_RAW_S3_REGION", "us-east-1"),
        "S3_ACCESS_KEY": parsed.get("S3_ACCESS_KEY", ""),
        "S3_SECRET_KEY": parsed.get("S3_SECRET_KEY", ""),
    }
    if not values["FOTMOB_RAW_STORE_URI"]:
        raise PurgeRefused("purge env file does not define the raw-store URI")
    if not values["S3_ACCESS_KEY"] or not values["S3_SECRET_KEY"]:
        raise PurgeRefused("purge env file does not define S3 credentials")
    for key in runtime_binding.PURGE_RAW_ENV_KEYS:
        os.environ.pop(key, None)
    os.environ.update(values)
    try:
        runtime_binding.load_host_trino_environment(args.trino_env_file)
    except runtime_binding.RuntimeBindingError as exc:
        raise PurgeRefused(str(exc)) from exc
    return values["FOTMOB_RAW_STORE_URI"]


def validate_purge_runtime(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Re-attest both runtimes and bind explicit host data-plane settings."""

    raw_uri = _load_purge_environment(args)
    try:
        context = runtime_binding.load_deployment_context(
            args.deployment_report,
            project=args.project,
            compose_file=args.compose_file,
        )
        isolated = runtime_binding.validate_live_deployment(
            context,
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=True,
            run=run,
        )
        shared = runtime_binding.validate_live_shared_runtime(context, run=run)
        data = runtime_binding.validate_live_purge_data_bindings(
            context, raw_store_uri=raw_uri, run=run
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise PurgeRefused(str(exc)) from exc
    return {"isolated": isolated, "shared": shared, "data": data, "passed": True}


def _read_plan(path: Path) -> Mapping[str, Any]:
    def reject_duplicate_keys(
        pairs: Sequence[tuple[str, Any]],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key, value in pairs:
            if key in payload:
                raise PurgeRefused(f"plan contains duplicate JSON key: {key}")
            payload[key] = value
        return payload

    try:
        payload = json.loads(
            Path(path).read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except PurgeRefused:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PurgeRefused("plan file is unreadable") from exc
    if not isinstance(payload, Mapping):
        raise PurgeRefused("plan file must contain a JSON object")
    return payload


def main(
    argv: Sequence[str] | None = None,
    *,
    backend_factory: Callable[[argparse.Namespace], PurgeBackend] = (
        TrinoAirflowRawBackend.from_args
    ),
    runtime_preflight: Callable[[argparse.Namespace], Mapping[str, Any]] = (
        validate_purge_runtime
    ),
) -> int:
    args = build_parser().parse_args(argv)
    backend: PurgeBackend | None = None
    try:
        plan: Mapping[str, Any] | None = None
        if args.apply:
            if args.plan is None or args.plan_sha256 is None or args.journal is None:
                raise PurgeRefused(
                    "--apply requires --plan, --plan-sha256, and --journal"
                )
            plan = _read_plan(args.plan)
            # Plan bytes, expiry, hard-coded IDs and compiled predicates are
            # checked before opening Trino/Airflow/raw-store connections.
            validation_clock = datetime.now(timezone.utc)
            _evidence, operations = _validate_plan(
                plan,
                supplied_sha256=args.plan_sha256,
                now=validation_clock,
                allow_expired_recovery=True,
            )
            if validation_clock >= _timestamp(
                plan.get("expires_at"), field="expires_at"
            ):
                if not args.journal.exists():
                    raise PurgeRefused("purge plan has expired")
                journal = _load_journal(
                    args.journal, plan_sha256=str(plan["plan_sha256"])
                )
                _validate_journal_state(journal, operations, plan)
                if (
                    not _journal_has_mutation_state(journal)
                    and journal.get("apply_fence_generation_id") is None
                ):
                    raise PurgeRefused("purge plan has expired")
        elif args.plan is not None or args.plan_sha256 is not None or args.journal:
            raise PurgeRefused("plan/hash/journal arguments are valid only with --apply")
        runtime_preflight(args)
        backend = backend_factory(args)
        if args.apply:
            if plan is None:  # pragma: no cover - guarded by the apply branch
                raise AssertionError("validated apply plan is unavailable")
            result = apply_plan(
                plan,
                backend,
                supplied_sha256=args.plan_sha256,
                journal_path=args.journal,
                deployment_report=args.deployment_report,
                scheduled_observation_report=args.scheduled_observation_report,
            )
        else:
            result = build_plan(
                backend,
                deployment_report=args.deployment_report,
                scheduled_observation_report=args.scheduled_observation_report,
            )
            _atomic_json(args.output, result)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except PurgeRefused as exc:
        print(f"PURGE REFUSED: {exc}", file=sys.stderr)
        return 2
    except PostDeleteVerificationError as exc:
        print(f"POST-DELETE VERIFICATION FAILED: {exc}", file=sys.stderr)
        return 3
    finally:
        close = getattr(backend, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    raise SystemExit(main())
