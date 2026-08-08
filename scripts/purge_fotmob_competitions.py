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
import sys
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit


PLAN_SCHEMA_VERSION = "fotmob-competition-purge-plan-v1"
JOURNAL_SCHEMA_VERSION = "fotmob-competition-purge-journal-v1"
PURGE_COMPETITION_IDS = (10557, 10558)
PLAN_TTL = timedelta(hours=1)
MAX_EVIDENCE_AGE = timedelta(days=30)
WRITER_DAG_IDS = (
    "dag_orchestrate_fotmob",
    "dag_trigger_fotmob_daily",
    "dag_refresh_fotmob",
    "dag_backfill_fotmob",
    "dag_ingest_fotmob",
    "dag_transform_fotmob_silver",
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
    now: datetime | None = None,
    ttl: timedelta = PLAN_TTL,
) -> dict[str, Any]:
    """Build a short-lived read-only purge plan from authoritative state."""

    clock = _utc(now or datetime.now(timezone.utc), field="now")
    if not isinstance(ttl, timedelta) or not timedelta(0) < ttl <= PLAN_TTL:
        raise PurgeRefused(f"plan TTL must be positive and at most {PLAN_TTL}")
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
                fresh = build_plan(backend, now=clock)
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
            fresh_after_fence = build_plan(backend, now=clock)
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

    def assert_quiescent(
        self, writer_dag_ids: Sequence[str], *, source: str
    ) -> None:
        if tuple(writer_dag_ids) != WRITER_DAG_IDS or source != "fotmob":
            raise PurgeRefused("quiescence request differs from the FotMob writer set")
        try:
            from airflow.models import DagModel, DagRun
            from airflow.settings import Session
            from scrapers.fbref.control import ControlStore

            session = Session()
            try:
                active_rows = (
                    session.query(DagRun.dag_id, DagRun.run_id, DagRun.state)
                    .filter(
                        DagRun.dag_id.in_(WRITER_DAG_IDS),
                        DagRun.state.in_(("queued", "running")),
                    )
                    .all()
                )
                dag_rows = (
                    session.query(DagModel.dag_id, DagModel.is_paused)
                    .filter(DagModel.dag_id.in_(WRITER_DAG_IDS))
                    .all()
                )
            finally:
                session.close()
            dag_pause_state = {str(row[0]): bool(row[1]) for row in dag_rows}
            if set(dag_pause_state) != set(WRITER_DAG_IDS):
                missing = sorted(set(WRITER_DAG_IDS) - set(dag_pause_state))
                raise PurgeRefused(
                    "cannot prove all writer/maintenance DAGs are paused: "
                    + ", ".join(missing)
                )
            unpaused = sorted(
                dag_id for dag_id, is_paused in dag_pause_state.items() if not is_paused
            )
            if unpaused:
                raise PurgeRefused(
                    "writer/maintenance DAGs are not paused: " + ", ".join(unpaused)
                )
            if active_rows:
                active = sorted(
                    f"{row[0]}:{row[1]}:{row[2]}" for row in active_rows
                )
                raise PurgeRefused("active writer DAGs: " + ", ".join(active))
            control = ControlStore.from_env()
            if self._apply_fence_generation is None:
                publication = control.assert_no_active_publication_generation(
                    source="fotmob"
                )
            else:
                publication = control.get_publication_generation(
                    self._apply_fence_generation, source="fotmob"
                )
        except PurgeRefused:
            raise
        except Exception as exc:
            raise PurgeRefused(
                "cannot prove Airflow writer/publication quiescence"
            ) from exc
        if self._apply_fence_generation is None:
            if publication.get("safe") is not True or publication.get("active") is True:
                raise PurgeRefused("active FotMob publication lease")
        elif (
            not isinstance(publication, Mapping)
            or publication.get("generation_id") != self._apply_fence_generation
            or publication.get("binding") != self._apply_fence_binding
            or publication.get("status") != "running"
            or publication.get("phase") != "writing"
            or publication.get("active") is not True
            or publication.get("owner_dag_id") != "fotmob_legacy_purge"
        ):
            raise PurgeRefused("FotMob purge apply fence is not active and exact")

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

    def acquire_apply_fence(
        self, plan_sha256: str, fence_generation_id: str
    ) -> str:
        """Acquire the singleton FotMob publication lock for this exact plan."""

        if self._apply_fence_generation is not None:
            raise PurgeRefused("purge backend already owns an apply fence")
        generation_id, binding = self._fence_identity(
            plan_sha256, fence_generation_id
        )
        try:
            from scrapers.fbref.control import ControlStore

            state = ControlStore.from_env().initialize_publication_generation(
                generation_id,
                dag_id="fotmob_legacy_purge",
                binding=binding,
                source="fotmob",
                ttl_seconds=14 * 24 * 60 * 60,
            )
        except Exception as exc:
            raise PurgeRefused("cannot acquire the FotMob purge apply fence") from exc
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
        try:
            from scrapers.fbref.control import ControlStore

            state = ControlStore.from_env().get_publication_generation(
                generation_id, source="fotmob"
            )
        except Exception as exc:
            raise PurgeRefused("cannot inspect the FotMob purge apply fence") from exc
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
        try:
            from scrapers.fbref.control import ControlStore

            state = ControlStore.from_env().fail_publication_generation(
                fence_token,
                safe_to_release=True,
                source="fotmob",
            )
        except Exception as exc:
            raise PurgeRefused("cannot release the FotMob purge apply fence") from exc
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
    return parser


def _read_plan(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
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
        backend = backend_factory(args)
        if args.apply:
            if plan is None:  # pragma: no cover - guarded by the apply branch
                raise AssertionError("validated apply plan is unavailable")
            result = apply_plan(
                plan,
                backend,
                supplied_sha256=args.plan_sha256,
                journal_path=args.journal,
            )
        else:
            result = build_plan(backend)
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
