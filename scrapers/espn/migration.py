"""Safe, append-only migration and rollback for ESPN Native Bronze v2.

The default path only renders a machine-readable plan.  Applying a promotion
requires one exact scope, three immutable green-run bundles, a matching
physical COMPLETE manifest, an immutable legacy-or-absence fallback baseline,
and the durable scope lease held through the append-only cutover record.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import shlex
from typing import Any, Callable, ContextManager, Mapping, Protocol
from urllib.parse import unquote, urlparse

import yaml

from .daily_owner import (
    DAILY_PARENT_FIELDS,
    SCHEDULED_RUN_TYPE,
    DailyOwnerError,
    daily_child_run_id,
    resolve_daily_owner_profile,
    standard_scheduled_run_id,
)
from .operations import PostgresEspnControlStore
from .repository import (
    BASELINE_TABLE,
    CUTOVER_TABLE,
    EspnBronzeRepository,
    MANIFEST_TABLE,
    ScopeCutover,
    canonical_json,
    canonical_sha256,
    render_current_view_sql,
    render_repository_ddl,
)
from . import runner
from .registry import Registry, RegistryError, validate_registry_document


UTC = timezone.utc
LEGACY_MIGRATION_VERSION = "espn-native-bronze-v2-migration-v1"
MIGRATION_VERSION = "espn-native-bronze-v2-migration-v2"
BASELINE_VERSION = "espn-legacy-baseline-v1"
ABSENCE_BASELINE_VERSION = "espn-absence-baseline-v1"
LEGACY_PROMOTION_EVIDENCE_VERSION = "espn-v2-promotion-evidence-v2"
PROMOTION_EVIDENCE_VERSION = "espn-v2-promotion-evidence-v3"
LEGACY_ROLLBACK_PLAN_VERSION = "espn-v2-rollback-plan-v1"
ROLLBACK_PLAN_VERSION = "espn-v2-rollback-plan-v2"
_SHA_RE = re.compile(r"[0-9a-f]{64}")
_SCOPE_RE = re.compile(r"([1-9][0-9]*):([1-9][0-9]*)")
_BASELINE_COLUMNS = (
    "baseline_version",
    "scope_id",
    "legacy_league",
    "legacy_season",
    "captured_at",
    "entity_metrics_json",
    "legacy_snapshot_ids_json",
    "registry_signature",
    "durable_manifest_uri",
    "durable_manifest_sha256",
    "replay_raw_manifest_uri",
    "replay_raw_manifest_sha256",
    "trust_label",
    "baseline_sha256",
)
_MANIFEST_IDENTITY_COLUMNS = (
    "status",
    "scope_id",
    "generation_id",
    "generation_signature",
    "manifest_sha256",
    "registry_signature",
)


class MigrationError(RuntimeError):
    """Migration evidence or a production precondition failed closed."""


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _required(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MigrationError(f"{field} must be a non-empty string")
    return value.strip()


def _sha(value: object, field: str) -> str:
    raw = _required(value, field)
    if _SHA_RE.fullmatch(raw) is None:
        raise MigrationError(f"{field} must be a lowercase SHA-256")
    return raw


def _positive_int(value: object, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise MigrationError(f"{field} must be a positive integer")
    return value


def _scope(value: object) -> tuple[str, int, int]:
    raw = _required(value, "scope_id")
    match = _SCOPE_RE.fullmatch(raw)
    if match is None:
        raise MigrationError("scope_id must be '<espn_id>:<source_year>'")
    espn_id, source_year = (int(part) for part in match.groups())
    if source_year < 1800:
        raise MigrationError("scope source year is invalid")
    return raw, espn_id, source_year


def _utc(value: object, field: str) -> datetime:
    raw = _required(value, field)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise MigrationError(f"{field} must be an ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MigrationError(f"{field} must be timezone-aware")
    normalized = parsed.astimezone(UTC)
    if raw != normalized.isoformat():
        raise MigrationError(f"{field} must be canonical UTC")
    return normalized


def _immutable_uri(value: object, field: str) -> str:
    uri = _required(value, field)
    parsed = urlparse(uri)
    if parsed.scheme not in {"file", "s3"}:
        raise MigrationError(f"{field} must use file:// or s3://")
    components = tuple(
        part.casefold() for part in unquote(parsed.path).split("/") if part
    )
    if any(part == "latest" or part.startswith("latest.") for part in components):
        raise MigrationError(f"{field} must not use a mutable latest alias")
    return uri


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    uri: str
    sha256: str

    @classmethod
    def from_mapping(cls, value: object, *, field: str) -> "ArtifactRef":
        if not isinstance(value, Mapping) or set(value) != {"uri", "sha256"}:
            raise MigrationError(f"{field} schema mismatch")
        return cls(
            uri=_immutable_uri(value["uri"], f"{field}.uri"),
            sha256=_sha(value["sha256"], f"{field}.sha256"),
        )

    def to_dict(self) -> dict[str, str]:
        return {"uri": self.uri, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class FallbackDescriptor:
    """The honest route to restore when native data is deactivated."""

    kind: str
    league: str | None = None
    season: str | None = None

    def __post_init__(self) -> None:
        if self.kind == "legacy":
            _required(self.league, "fallback.league")
            _required(self.season, "fallback.season")
        elif self.kind == "absent":
            if self.league is not None or self.season is not None:
                raise MigrationError("absent fallback must not contain legacy aliases")
        else:
            raise MigrationError("fallback.kind must be legacy or absent")

    @classmethod
    def from_mapping(
        cls, value: object, *, field: str = "fallback"
    ) -> "FallbackDescriptor":
        if not isinstance(value, Mapping):
            raise MigrationError(f"{field} must be an object")
        kind = value.get("kind")
        expected = {"kind", "league", "season"} if kind == "legacy" else {"kind"}
        if set(value) != expected:
            raise MigrationError(f"{field} schema mismatch")
        try:
            return cls(
                kind=_required(kind, f"{field}.kind"),
                league=(
                    _required(value.get("league"), f"{field}.league")
                    if kind == "legacy"
                    else None
                ),
                season=(
                    _required(value.get("season"), f"{field}.season")
                    if kind == "legacy"
                    else None
                ),
            )
        except MigrationError as exc:
            raise MigrationError(f"{field} is invalid: {exc}") from exc

    def to_dict(self) -> dict[str, str]:
        if self.kind == "absent":
            return {"kind": "absent"}
        return {
            "kind": "legacy",
            "league": str(self.league),
            "season": str(self.season),
        }


@dataclass(frozen=True, slots=True)
class GreenRunEvidence:
    dag_id: str
    run_id: str
    attempt: int
    scope_id: str
    registry_signature: str
    plan_signature: str
    generation_id: str
    generation_signature: str
    manifest_sha256: str
    as_of: date
    logical_date: datetime
    data_interval_start: datetime
    data_interval_end: datetime
    parent_run_id: str
    recorded_at: datetime
    durable_manifest_ref: ArtifactRef
    run_evidence_ref: ArtifactRef
    raw_manifest_ref: ArtifactRef
    publication_ref: ArtifactRef
    generation_snapshot_ref: ArtifactRef
    published_dq_ref: ArtifactRef
    terminal_verdict_ref: ArtifactRef
    health_ref: ArtifactRef
    lease_release_ref: ArtifactRef
    success_receipt_ref: ArtifactRef
    run_registry_snapshot_ref: ArtifactRef

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "attempt": self.attempt,
            "scope_id": self.scope_id,
            "registry_signature": self.registry_signature,
            "plan_signature": self.plan_signature,
            "generation_id": self.generation_id,
            "generation_signature": self.generation_signature,
            "manifest_sha256": self.manifest_sha256,
            "as_of": self.as_of.isoformat(),
            "logical_date": self.logical_date.isoformat(),
            "data_interval_start": self.data_interval_start.isoformat(),
            "data_interval_end": self.data_interval_end.isoformat(),
            "parent_run_id": self.parent_run_id,
            "recorded_at": self.recorded_at.isoformat(),
            "durable_manifest_ref": self.durable_manifest_ref.to_dict(),
            "run_evidence_ref": self.run_evidence_ref.to_dict(),
            "raw_manifest_ref": self.raw_manifest_ref.to_dict(),
            "publication_ref": self.publication_ref.to_dict(),
            "generation_snapshot_ref": self.generation_snapshot_ref.to_dict(),
            "published_dq_ref": self.published_dq_ref.to_dict(),
            "terminal_verdict_ref": self.terminal_verdict_ref.to_dict(),
            "health_ref": self.health_ref.to_dict(),
            "lease_release_ref": self.lease_release_ref.to_dict(),
            "success_receipt_ref": self.success_receipt_ref.to_dict(),
            "run_registry_snapshot_ref": self.run_registry_snapshot_ref.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class PromotionEvidence:
    evidence_version: str
    scope_id: str
    espn_id: int
    source_season_year: int
    fallback: FallbackDescriptor
    trust_label: str
    cutover_id: str
    effective_at: datetime
    registry_snapshot_ref: ArtifactRef
    green_runs: tuple[GreenRunEvidence, ...]

    @property
    def legacy_league(self) -> str | None:
        return self.fallback.league

    @property
    def legacy_season(self) -> str | None:
        return self.fallback.season


def registry_fallback_descriptors(
    registry: Registry,
) -> dict[str, FallbackDescriptor]:
    """Derive one deterministic fallback for every enabled current scope."""

    if not isinstance(registry, Registry):
        raise TypeError("registry must be Registry")
    output: dict[str, FallbackDescriptor] = {}
    for competition in registry.promoted:
        edition = competition.current_edition
        scope_id = competition.scope_id(edition)
        if competition.legacy is None:
            fallback = FallbackDescriptor(kind="absent")
        else:
            seasons = competition.legacy.season_aliases.get(
                edition.source_season_year, ()
            )
            if not seasons:
                raise MigrationError(
                    f"registry legacy fallback has no reviewed season for {scope_id}"
                )
            fallback = FallbackDescriptor(
                kind="legacy",
                league=competition.legacy.league,
                season=seasons[0],
            )
        if scope_id in output:
            raise MigrationError(f"registry fallback scope is duplicated: {scope_id}")
        output[scope_id] = fallback
    return dict(sorted(output.items()))


ArtifactReader = Callable[[str], bytes]


def _default_artifact_reader(uri: str) -> bytes:
    try:
        return runner._read_artifact(uri)
    except Exception as exc:
        raise MigrationError(f"cannot read immutable artifact {uri}: {exc}") from exc


def _read_ref(
    ref: ArtifactRef, *, reader: ArtifactReader, field: str
) -> Mapping[str, Any]:
    try:
        body = reader(ref.uri)
    except MigrationError:
        raise
    except Exception as exc:
        raise MigrationError(f"cannot read {field}: {exc}") from exc
    if not isinstance(body, bytes):
        raise MigrationError(f"{field} reader must return bytes")
    if hashlib.sha256(body).hexdigest() != ref.sha256:
        raise MigrationError(f"{field} SHA-256 mismatch")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"{field} is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise MigrationError(f"{field} must contain an object")
    return payload


def _read_bytes_ref(ref: ArtifactRef, *, reader: ArtifactReader, field: str) -> bytes:
    try:
        body = reader(ref.uri)
    except MigrationError:
        raise
    except Exception as exc:
        raise MigrationError(f"cannot read {field}: {exc}") from exc
    if not isinstance(body, bytes):
        raise MigrationError(f"{field} reader must return bytes")
    if hashlib.sha256(body).hexdigest() != ref.sha256:
        raise MigrationError(f"{field} SHA-256 mismatch")
    return body


def _load_green_run(
    raw: object,
    *,
    scope_id: str,
    reader: ArtifactReader,
    field: str,
) -> GreenRunEvidence:
    if not isinstance(raw, Mapping) or set(raw) != {
        "durable_manifest_ref",
        "raw_manifest_ref",
        "published_dq_ref",
        "terminal_verdict_ref",
        "health_ref",
        "lease_release_ref",
        "success_receipt_ref",
    }:
        raise MigrationError(f"{field} schema mismatch")
    durable_ref = ArtifactRef.from_mapping(
        raw["durable_manifest_ref"], field=f"{field}.durable_manifest_ref"
    )
    raw_ref = ArtifactRef.from_mapping(
        raw["raw_manifest_ref"], field=f"{field}.raw_manifest_ref"
    )
    published_dq_ref = ArtifactRef.from_mapping(
        raw["published_dq_ref"], field=f"{field}.published_dq_ref"
    )
    verdict_ref = ArtifactRef.from_mapping(
        raw["terminal_verdict_ref"], field=f"{field}.terminal_verdict_ref"
    )
    health_ref = ArtifactRef.from_mapping(
        raw["health_ref"], field=f"{field}.health_ref"
    )
    release_ref = ArtifactRef.from_mapping(
        raw["lease_release_ref"], field=f"{field}.lease_release_ref"
    )
    success_ref = ArtifactRef.from_mapping(
        raw["success_receipt_ref"], field=f"{field}.success_receipt_ref"
    )
    durable = _read_ref(
        durable_ref, reader=reader, field=f"{field}.durable_manifest_ref"
    )
    if (
        durable.get("kind") != "espn-durable-run-manifest-v1"
        or durable.get("schema_version") != 1
    ):
        raise MigrationError(f"{field} durable manifest kind is invalid")
    run_id = _required(durable.get("run_id"), f"{field}.run_id")
    dag_id = _required(durable.get("dag_id"), f"{field}.dag_id")
    attempt = _positive_int(durable.get("attempt"), f"{field}.attempt")
    registry_signature = _sha(
        durable.get("registry_signature"), f"{field}.registry_signature"
    )
    scope_ids = durable.get("scope_ids")
    if not isinstance(scope_ids, list) or scope_id not in scope_ids:
        raise MigrationError(f"{field} durable manifest does not contain the scope")
    evidence_rows = durable.get("evidence")
    if not isinstance(evidence_rows, list):
        raise MigrationError(f"{field} durable evidence must be a list")
    matches = [
        row
        for row in evidence_rows
        if isinstance(row, Mapping) and row.get("scope_id") == scope_id
    ]
    if len(matches) != 1:
        raise MigrationError(f"{field} must contain one exact scope evidence row")
    summary = matches[0]
    if (
        summary.get("dag_id"),
        summary.get("run_id"),
        summary.get("attempt"),
        summary.get("registry_signature"),
        summary.get("state"),
    ) != (dag_id, run_id, attempt, registry_signature, "complete"):
        raise MigrationError(f"{field} durable scope evidence identity mismatch")
    plan_signature = _sha(summary.get("plan_signature"), f"{field}.plan_signature")
    evidence_ref = ArtifactRef(
        uri=_immutable_uri(summary.get("evidence_uri"), f"{field}.evidence_uri"),
        sha256=_sha(summary.get("evidence_sha256"), f"{field}.evidence_sha256"),
    )
    evidence = _read_ref(evidence_ref, reader=reader, field=f"{field}.run_evidence")
    required_identity = (
        evidence.get("kind"),
        evidence.get("schema_version"),
        evidence.get("dag_id"),
        evidence.get("run_id"),
        evidence.get("attempt"),
        evidence.get("scope_id"),
        evidence.get("state"),
        evidence.get("plan_signature"),
        evidence.get("registry_signature"),
    )
    if required_identity != (
        "espn-run-manifest-evidence-v1",
        1,
        dag_id,
        run_id,
        attempt,
        scope_id,
        "complete",
        plan_signature,
        registry_signature,
    ):
        raise MigrationError(f"{field} run evidence identity mismatch")
    generation_id = _required(evidence.get("generation_id"), f"{field}.generation_id")
    generation_signature = _sha(
        evidence.get("generation_signature"), f"{field}.generation_signature"
    )
    manifest_sha256 = _sha(evidence.get("manifest_sha256"), f"{field}.manifest_sha256")
    recorded_at = _utc(evidence.get("recorded_at"), f"{field}.recorded_at")
    if summary.get("recorded_at") != recorded_at.isoformat():
        raise MigrationError(f"{field} durable/run evidence timestamp mismatch")

    raw_publications = durable.get("publication_refs")
    if not isinstance(raw_publications, list):
        raise MigrationError(f"{field} durable publication_refs must be a list")
    publications: list[tuple[ArtifactRef, Mapping[str, Any]]] = []
    for index, wrapped in enumerate(raw_publications):
        publication_field = f"{field}.publication_refs[{index}]"
        if not isinstance(wrapped, Mapping) or set(wrapped) != {"publication_ref"}:
            raise MigrationError(f"{publication_field} schema mismatch")
        publication_ref = ArtifactRef.from_mapping(
            wrapped["publication_ref"], field=f"{publication_field}.publication_ref"
        )
        publication = _read_ref(publication_ref, reader=reader, field=publication_field)
        if (
            publication.get("kind") != "espn-publication-result-v1"
            or publication.get("schema_version") != 1
        ):
            raise MigrationError(f"{publication_field} kind is invalid")
        if publication.get("evidence_ref") == evidence_ref.to_dict():
            publications.append((publication_ref, publication))
    if len(publications) != 1:
        raise MigrationError(
            f"{field} must bind one exact durable publication to scope evidence"
        )
    publication_ref, publication = publications[0]
    if publication.get("state") != "complete":
        raise MigrationError(f"{field} promotion run must publish a new generation")
    if (
        type(publication.get("proxy_bytes")) is not int
        or publication["proxy_bytes"] != 0
    ):
        raise MigrationError(f"{field} publication contains proxy traffic")
    ArtifactRef.from_mapping(
        publication.get("scope_binding_ref"),
        field=f"{field}.publication.scope_binding_ref",
    )
    snapshot_ref = ArtifactRef.from_mapping(
        publication.get("snapshot_ref"), field=f"{field}.publication.snapshot_ref"
    )

    published_dq = _read_ref(
        published_dq_ref, reader=reader, field=f"{field}.published_dq_ref"
    )
    quality = published_dq.get("quality")
    if (
        published_dq.get("kind"),
        published_dq.get("schema_version"),
        published_dq.get("dag_id"),
        published_dq.get("run_id"),
        published_dq.get("attempt"),
        published_dq.get("scope_id"),
        published_dq.get("plan_signature"),
        published_dq.get("registry_signature"),
        published_dq.get("publication_ref"),
        isinstance(quality, Mapping) and quality.get("passed"),
        isinstance(quality, Mapping) and quality.get("failures"),
        isinstance(quality, Mapping) and quality.get("scope_id"),
    ) != (
        "espn-published-dq-result-v1",
        1,
        dag_id,
        run_id,
        attempt,
        scope_id,
        plan_signature,
        registry_signature,
        publication_ref.to_dict(),
        True,
        [],
        scope_id,
    ):
        raise MigrationError(f"{field} published DQ identity is not green")

    verdict = _read_ref(
        verdict_ref, reader=reader, field=f"{field}.terminal_verdict_ref"
    )
    producer_states = verdict.get("producer_states")
    expected_counts = verdict.get("expected_counts")
    scope_metrics = verdict.get("scope_metrics")
    published_states = (
        producer_states.get("published_dq")
        if isinstance(producer_states, Mapping)
        else None
    )
    if (
        verdict.get("kind"),
        verdict.get("schema_version"),
        verdict.get("dag_id"),
        verdict.get("run_id"),
        verdict.get("attempt"),
        verdict.get("status"),
        verdict.get("failures"),
        verdict.get("scope_count"),
    ) != (
        "espn-terminal-verdict-v1",
        1,
        dag_id,
        run_id,
        attempt,
        "complete",
        [],
        len(scope_ids),
    ):
        raise MigrationError(f"{field} terminal verdict is not green")
    if (
        not isinstance(published_states, list)
        or not published_states
        or any(state != "success" for state in published_states)
        or not isinstance(expected_counts, Mapping)
        or expected_counts.get("published_dq") != len(published_states)
        or not isinstance(scope_metrics, Mapping)
        or scope_id not in scope_metrics
    ):
        raise MigrationError(f"{field} terminal verdict lacks published-DQ success")

    health = _read_ref(health_ref, reader=reader, field=f"{field}.health_ref")
    alerts = health.get("alerts")
    health_metrics = health.get("scope_metrics")
    if (
        (
            health.get("kind"),
            health.get("schema_version"),
            health.get("run_id"),
            health.get("attempt"),
            health.get("status"),
            health.get("verdict_ref"),
        )
        != (
            "espn-health-result-v1",
            1,
            run_id,
            attempt,
            "complete",
            verdict_ref.to_dict(),
        )
        or not isinstance(alerts, list)
        or not isinstance(health_metrics, Mapping)
        or scope_id not in health_metrics
        or canonical_json(health_metrics.get(scope_id))
        != canonical_json(scope_metrics.get(scope_id))
    ):
        raise MigrationError(f"{field} health result identity is not green")
    if alerts:
        raise MigrationError(f"{field} health result contains an alert")

    release = _read_ref(release_ref, reader=reader, field=f"{field}.lease_release_ref")
    release_scope_ids = release.get("scope_ids")
    released_scopes = release.get("released")
    if (
        (
            release.get("kind"),
            release.get("schema_version"),
            release.get("dag_id"),
            release.get("run_id"),
            release.get("attempt"),
            release.get("failures"),
        )
        != (
            "espn-lease-release-result-v1",
            1,
            dag_id,
            run_id,
            attempt,
            [],
        )
        or not isinstance(release_scope_ids, list)
        or not isinstance(released_scopes, list)
        or scope_id not in release_scope_ids
        or scope_id not in released_scopes
    ):
        raise MigrationError(f"{field} lease cleanup result is not green")

    success = _read_ref(
        success_ref, reader=reader, field=f"{field}.success_receipt_ref"
    )
    receipt_hash = success.get("receipt_sha256")
    receipt_base = {
        key: value for key, value in success.items() if key != "receipt_sha256"
    }
    expected_receipt_hash = hashlib.sha256(
        (
            json.dumps(
                receipt_base,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    ).hexdigest()
    dq_refs = success.get("published_dq_refs")
    run_registry_ref = ArtifactRef.from_mapping(
        success.get("registry_ref"), field=f"{field}.success.registry_ref"
    )
    parent = success.get("parent")
    if not isinstance(parent, Mapping) or set(parent) != DAILY_PARENT_FIELDS:
        raise MigrationError(f"{field} success receipt parent schema mismatch")
    try:
        profile = resolve_daily_owner_profile(parent.get("owner_profile"))
    except DailyOwnerError as exc:
        raise MigrationError(f"{field} daily owner profile is unknown") from exc
    parent_run_id = _required(parent.get("parent_run_id"), f"{field}.parent_run_id")
    expected_child = daily_child_run_id(parent_run_id)
    if (
        parent.get("schema"),
        parent.get("parent_dag_id"),
        parent.get("parent_task_id"),
        parent.get("parent_run_type"),
        parent.get("child_dag_id"),
        parent.get("child_run_id"),
        run_id,
    ) != (
        profile.envelope_schema,
        profile.parent_dag_id,
        profile.trigger_task_id,
        SCHEDULED_RUN_TYPE,
        profile.child_dag_id,
        expected_child,
        expected_child,
    ):
        raise MigrationError(f"{field} daily owner identity mismatch")
    logical_date = _utc(success.get("logical_date"), f"{field}.logical_date")
    parent_logical_date = _utc(
        parent.get("logical_date"), f"{field}.parent_logical_date"
    )
    interval_start = _utc(
        parent.get("data_interval_start"), f"{field}.data_interval_start"
    )
    interval_end = _utc(parent.get("data_interval_end"), f"{field}.data_interval_end")
    if (
        parent_logical_date != logical_date
        or interval_start != logical_date
        or interval_end - interval_start != timedelta(days=1)
        or (
            interval_start.hour,
            interval_start.minute,
            interval_start.second,
            interval_start.microsecond,
        )
        != (14, 0, 0, 0)
        or parent_run_id != standard_scheduled_run_id(logical_date)
    ):
        raise MigrationError(f"{field} daily interval identity mismatch")
    matching_dq = (
        [
            item
            for item in dq_refs
            if isinstance(item, Mapping) and item.get("scope_id") == scope_id
        ]
        if isinstance(dq_refs, list)
        else []
    )
    if (
        (
            success.get("kind"),
            success.get("schema_version"),
            success.get("dag_id"),
            success.get("run_id"),
            success.get("attempt"),
            success.get("mode"),
            success.get("scope_ids"),
            success.get("registry_signature"),
            success.get("durable_manifest_ref"),
            success.get("verdict_ref"),
            success.get("health_ref"),
            success.get("lease_release_ref"),
            receipt_hash,
        )
        != (
            "espn-run-success-receipt-v1",
            1,
            dag_id,
            run_id,
            attempt,
            "daily",
            durable.get("scope_ids"),
            registry_signature,
            durable_ref.to_dict(),
            verdict_ref.to_dict(),
            health_ref.to_dict(),
            release_ref.to_dict(),
            expected_receipt_hash,
        )
        or len(matching_dq) != 1
        or matching_dq[0].get("published_dq_ref") != published_dq_ref.to_dict()
    ):
        raise MigrationError(f"{field} final success receipt identity mismatch")

    raw_manifest = _read_ref(raw_ref, reader=reader, field=f"{field}.raw_manifest_ref")
    try:
        validated_raw = runner._validate_raw_manifest(raw_manifest)
    except Exception as exc:
        raise MigrationError(f"{field} raw manifest is invalid: {exc}") from exc
    if (
        validated_raw["run_id"],
        validated_raw["attempt"],
        validated_raw["registry_signature"],
        validated_raw["plan_signature"],
    ) != (run_id, attempt, registry_signature, plan_signature):
        raise MigrationError(f"{field} raw manifest identity mismatch")
    if scope_id not in validated_raw["selected_scopes"]:
        raise MigrationError(f"{field} raw manifest scope set mismatch")
    if validated_raw.get("mode") != "daily":
        raise MigrationError(f"{field} green run must be a daily run")
    try:
        as_of = date.fromisoformat(validated_raw.get("as_of"))
    except (TypeError, ValueError) as exc:
        raise MigrationError(f"{field} raw manifest as_of is invalid") from exc
    if success.get("as_of") != as_of.isoformat():
        raise MigrationError(f"{field} success receipt/raw as_of mismatch")
    if logical_date.date() != as_of:
        raise MigrationError(f"{field} daily logical date/as_of mismatch")
    return GreenRunEvidence(
        dag_id=dag_id,
        run_id=run_id,
        attempt=attempt,
        scope_id=scope_id,
        registry_signature=registry_signature,
        plan_signature=plan_signature,
        generation_id=generation_id,
        generation_signature=generation_signature,
        manifest_sha256=manifest_sha256,
        as_of=as_of,
        logical_date=logical_date,
        data_interval_start=interval_start,
        data_interval_end=interval_end,
        parent_run_id=parent_run_id,
        recorded_at=recorded_at,
        durable_manifest_ref=durable_ref,
        run_evidence_ref=evidence_ref,
        raw_manifest_ref=raw_ref,
        publication_ref=publication_ref,
        generation_snapshot_ref=snapshot_ref,
        published_dq_ref=published_dq_ref,
        terminal_verdict_ref=verdict_ref,
        health_ref=health_ref,
        lease_release_ref=release_ref,
        success_receipt_ref=success_ref,
        run_registry_snapshot_ref=run_registry_ref,
    )


def load_promotion_evidence(
    path: str | Path,
    *,
    artifact_reader: ArtifactReader | None = None,
) -> PromotionEvidence:
    source = Path(path)
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"cannot read promotion evidence: {exc}") from exc
    common = {
        "schema_version",
        "scope_id",
        "trust_label",
        "cutover_id",
        "effective_at",
        "registry_snapshot_ref",
        "green_runs",
    }
    if not isinstance(document, Mapping):
        raise MigrationError("promotion evidence schema mismatch")
    evidence_version = document.get("schema_version")
    expected = (
        common | {"legacy_league", "legacy_season"}
        if evidence_version == LEGACY_PROMOTION_EVIDENCE_VERSION
        else common | {"fallback"}
        if evidence_version == PROMOTION_EVIDENCE_VERSION
        else set()
    )
    if not expected:
        raise MigrationError("unsupported promotion evidence schema")
    if set(document) != expected:
        raise MigrationError("promotion evidence schema mismatch")
    scope_id, espn_id, source_year = _scope(document["scope_id"])
    trust = _required(document["trust_label"], "trust_label")
    if trust == "legacy_untrusted":
        raise MigrationError("legacy_untrusted data cannot be promoted")
    if trust != "trusted":
        raise MigrationError("promotion evidence trust_label must be trusted")
    cutover_id = _required(document["cutover_id"], "cutover_id")
    root_cutover_id = f"espn-native-{espn_id}-{source_year}"
    if (
        cutover_id != root_cutover_id
        and re.fullmatch(
            re.escape(root_cutover_id) + r"-repromote-[0-9a-f]{16}", cutover_id
        )
        is None
    ):
        raise MigrationError("cutover_id is not deterministic for the exact scope")
    effective_at = _utc(document["effective_at"], "effective_at")
    raw_runs = document["green_runs"]
    if not isinstance(raw_runs, list) or len(raw_runs) != 3:
        raise MigrationError("promotion requires exactly three green runs")
    reader = artifact_reader or _default_artifact_reader
    registry_ref = ArtifactRef.from_mapping(
        document["registry_snapshot_ref"], field="registry_snapshot_ref"
    )
    runs = tuple(
        _load_green_run(
            raw,
            scope_id=scope_id,
            reader=reader,
            field=f"green_runs[{index}]",
        )
        for index, raw in enumerate(raw_runs)
    )
    if len({item.run_id for item in runs}) != 3:
        raise MigrationError("three green runs must have unique run IDs")
    if tuple(item.recorded_at for item in runs) != tuple(
        sorted(item.recorded_at for item in runs)
    ) or any(
        left.recorded_at >= right.recorded_at for left, right in zip(runs, runs[1:])
    ):
        raise MigrationError("green runs must be strictly chronological")
    if len({item.registry_signature for item in runs}) != 1:
        raise MigrationError("green runs must use one registry signature")
    if any(item.dag_id != "dag_ingest_espn" for item in runs):
        raise MigrationError("green runs must belong to the daily ESPN owner")
    if effective_at <= runs[-1].recorded_at:
        raise MigrationError("cutover effective_at must be after the third green run")
    if any(
        right.as_of != left.as_of + timedelta(days=1)
        for left, right in zip(runs, runs[1:])
    ):
        raise MigrationError("green runs must cover three consecutive daily dates")
    if any(
        right.data_interval_start != left.data_interval_end
        or right.logical_date.date() != left.logical_date.date() + timedelta(days=1)
        for left, right in zip(runs, runs[1:])
    ):
        raise MigrationError("green runs must have adjacent daily-owner data intervals")
    if (
        any(
            item.run_registry_snapshot_ref.sha256 != registry_ref.sha256
            for item in runs
        )
        or runs[-1].run_registry_snapshot_ref != registry_ref
    ):
        raise MigrationError("green runs do not use one exact registry content")

    try:
        registry_document = yaml.safe_load(
            _read_bytes_ref(
                registry_ref, reader=reader, field="registry_snapshot_ref"
            ).decode("utf-8")
        )
        registry = validate_registry_document(registry_document)
    except (UnicodeDecodeError, yaml.YAMLError, RegistryError) as exc:
        raise MigrationError("registry snapshot is invalid") from exc
    registry_signature = runs[0].registry_signature
    if registry.signature() != registry_signature:
        raise MigrationError("registry snapshot signature differs from green runs")
    competition = registry.by_id.get(espn_id)
    edition = (
        next(
            (
                item
                for item in competition.editions
                if item.source_season_year == source_year
            ),
            None,
        )
        if competition is not None
        else None
    )
    if competition is None or not competition.enabled or edition is None:
        raise MigrationError("registry does not promote the exact ESPN edition")
    fallback = (
        FallbackDescriptor(
            kind="legacy",
            league=_required(document["legacy_league"], "legacy_league"),
            season=_required(document["legacy_season"], "legacy_season"),
        )
        if evidence_version == LEGACY_PROMOTION_EVIDENCE_VERSION
        else FallbackDescriptor.from_mapping(document["fallback"])
    )
    legacy = competition.legacy
    if fallback.kind == "legacy":
        if (
            legacy is None
            or fallback.league
            not in {
                legacy.league,
                *legacy.league_aliases,
            }
            or fallback.season not in legacy.season_aliases.get(source_year, ())
        ):
            raise MigrationError("fallback legacy aliases do not match the registry")
        if source_year < 2016:
            raise MigrationError("legacy_untrusted pre-2016 data cannot be promoted")
    elif legacy is not None:
        raise MigrationError("fallback kind does not match the registry legacy route")
    return PromotionEvidence(
        evidence_version=str(evidence_version),
        scope_id=scope_id,
        espn_id=espn_id,
        source_season_year=source_year,
        fallback=fallback,
        trust_label=trust,
        cutover_id=cutover_id,
        effective_at=effective_at,
        registry_snapshot_ref=registry_ref,
        green_runs=runs,
    )


def migration_statements(
    *, catalog: str = "iceberg", schema: str = "bronze"
) -> tuple[str, ...]:
    repository_ddl = render_repository_ddl(catalog=catalog, schema=schema)
    statements = [f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"]
    statements.extend(repository_ddl.values())
    cutover = f"{catalog}.{schema}.{CUTOVER_TABLE}"
    statements.extend(
        (
            f'ALTER TABLE {cutover} ADD COLUMN IF NOT EXISTS "ancestor_cutover_sha256_json" varchar',
            f'ALTER TABLE {cutover} ADD COLUMN IF NOT EXISTS "ancestor_lineage_sha256" varchar',
        )
    )
    statements.extend(
        render_current_view_sql(entity, catalog=catalog, schema=schema)
        for entity in ("schedule", "lineup", "matchsheet")
    )
    destructive = re.compile(r"\b(?:DROP|DELETE|TRUNCATE)\b", re.IGNORECASE)
    if any(destructive.search(statement) for statement in statements):
        raise MigrationError("migration DDL contains a destructive statement")
    return tuple(statements)


def _candidate(run: GreenRunEvidence) -> dict[str, Any]:
    return run.to_dict()


def _rollback_instructions(
    output_path: Path, evidence: PromotionEvidence
) -> dict[str, Any]:
    promotion_report = output_path.resolve()
    rollback_output = promotion_report.with_name(
        promotion_report.name + ".rollback.json"
    )
    command = " ".join(
        shlex.quote(part)
        for part in (
            "python",
            "scripts/migrate_espn_native_v2.py",
            "rollback",
            "--promotion-report",
            str(promotion_report),
            "--reason",
            "emergency-native-cutover-rollback",
            "--output",
            str(rollback_output),
            "--apply",
        )
    )
    return {
        "mode": "append_only_successor",
        "scope_id": evidence.scope_id,
        "registry_snapshot_ref": evidence.registry_snapshot_ref.to_dict(),
        "commands": [command],
        "legacy_objects_retained": True,
        "v2_objects_retained": True,
    }


def build_promotion_plan(
    evidence: PromotionEvidence,
    *,
    output_path: str | Path,
    catalog: str = "iceberg",
    schema: str = "bronze",
) -> dict[str, Any]:
    if not isinstance(evidence, PromotionEvidence):
        raise TypeError("evidence must be PromotionEvidence")
    output = Path(output_path)
    result = {
        "schema_version": MIGRATION_VERSION,
        "mode": "dry_run",
        "status": "planned",
        "mutates": False,
        "scope_id": evidence.scope_id,
        "registry_snapshot_ref": evidence.registry_snapshot_ref.to_dict(),
        "fallback": evidence.fallback.to_dict(),
        "trust_label": evidence.trust_label,
        "green_runs": [item.to_dict() for item in evidence.green_runs],
        "candidate": _candidate(evidence.green_runs[-1]),
        "statements": list(migration_statements(catalog=catalog, schema=schema)),
        "baseline": {
            "table": f"{catalog}.{schema}.{BASELINE_TABLE}",
            "status": "planned",
            "kind": evidence.fallback.kind,
            "entities": ["schedule", "lineup", "matchsheet"],
            "captures_snapshot_ids": True,
        },
        "rollback": _rollback_instructions(output, evidence),
    }
    result["plan_sha256"] = canonical_sha256(result)
    return result


class MigrationBackend(Protocol):
    def ensure_objects(self) -> None: ...

    def legacy_baseline(
        self, league: str, season: str
    ) -> tuple[Mapping[str, Mapping[str, Any]], Mapping[str, int]]: ...

    def absence_baseline(self) -> Mapping[str, int]: ...

    def verify_candidate(
        self, candidate: GreenRunEvidence, registry_snapshot_ref: ArtifactRef
    ) -> None: ...

    def complete_manifest(
        self, scope_id: str, generation_id: str
    ) -> Mapping[str, Any] | None: ...

    def latest_complete_manifest(self, scope_id: str) -> Mapping[str, Any] | None: ...

    def baseline(self, scope_id: str) -> Mapping[str, Any] | None: ...

    def append_baseline(self, row: Mapping[str, Any]) -> None: ...

    def latest_cutover(self, scope_id: str) -> ScopeCutover | None: ...

    def append_cutover(self, cutover: ScopeCutover) -> None: ...


class LeaseStore(Protocol):
    def migrate(self) -> None: ...

    def acquire(
        self, scope_id: str, owner_id: str, plan_signature: str, *, now: datetime
    ) -> object: ...

    def guard(self, lease: object, *, now: datetime) -> ContextManager[None]: ...

    def release(self, lease: object, *, now: datetime) -> None: ...


def _manifest_matches(
    manifest: Mapping[str, Any] | None, run: GreenRunEvidence
) -> bool:
    return manifest is not None and (
        manifest.get("status"),
        manifest.get("scope_id"),
        manifest.get("generation_id"),
        manifest.get("generation_signature"),
        manifest.get("manifest_sha256"),
        manifest.get("registry_signature"),
    ) == (
        "complete",
        run.scope_id,
        run.generation_id,
        run.generation_signature,
        run.manifest_sha256,
        run.registry_signature,
    )


def _baseline_row(
    evidence: PromotionEvidence,
    *,
    metrics: Mapping[str, Mapping[str, Any]],
    snapshot_ids: Mapping[str, int],
    captured_at: datetime,
) -> dict[str, Any]:
    expected_entities = {"schedule", "lineup", "matchsheet"}
    normalized_metrics: dict[str, dict[str, Any]] = {}
    if evidence.fallback.kind == "legacy":
        if set(metrics) != expected_entities:
            raise MigrationError("legacy baseline metrics entity set mismatch")
        for entity in sorted(expected_entities):
            value = metrics[entity]
            if not isinstance(value, Mapping) or set(value) < {
                "row_count",
                "distinct_key_count",
                "max_ingested_at",
            }:
                raise MigrationError(f"legacy {entity} baseline metrics are incomplete")
            row_count = value["row_count"]
            key_count = value["distinct_key_count"]
            if type(row_count) is not int or row_count < 0:
                raise MigrationError(f"legacy {entity} row_count is invalid")
            if type(key_count) is not int or not 0 <= key_count <= row_count:
                raise MigrationError(f"legacy {entity} distinct key count is invalid")
            normalized_metrics[entity] = {
                key: value[key]
                for key in sorted(value)
                if key
                in {
                    "row_count",
                    "distinct_key_count",
                    "null_key_count",
                    "min_match_date",
                    "max_match_date",
                    "max_ingested_at",
                }
            }
        if normalized_metrics["schedule"]["row_count"] == 0:
            raise MigrationError("legacy schedule baseline is empty")
    elif metrics:
        raise MigrationError("absence baseline must not contain legacy metrics")
    if set(snapshot_ids) != {
        "espn_schedule",
        "espn_lineup",
        "espn_matchsheet",
    } or any(type(value) is not int or value <= 0 for value in snapshot_ids.values()):
        raise MigrationError("legacy Iceberg snapshot ID set is incomplete")
    candidate = evidence.green_runs[-1]
    base = {
        "baseline_version": (
            BASELINE_VERSION
            if evidence.fallback.kind == "legacy"
            else ABSENCE_BASELINE_VERSION
        ),
        "scope_id": evidence.scope_id,
        "legacy_league": evidence.legacy_league,
        "legacy_season": evidence.legacy_season,
        "captured_at": captured_at.astimezone(UTC),
        "entity_metrics_json": canonical_json(normalized_metrics),
        "legacy_snapshot_ids_json": canonical_json(dict(sorted(snapshot_ids.items()))),
        "registry_signature": candidate.registry_signature,
        "durable_manifest_uri": candidate.durable_manifest_ref.uri,
        "durable_manifest_sha256": candidate.durable_manifest_ref.sha256,
        "replay_raw_manifest_uri": candidate.raw_manifest_ref.uri,
        "replay_raw_manifest_sha256": candidate.raw_manifest_ref.sha256,
        "trust_label": evidence.trust_label,
    }
    return {**base, "baseline_sha256": canonical_sha256(base)}


def _validate_existing_baseline(
    row: Mapping[str, Any], evidence: PromotionEvidence
) -> dict[str, Any]:
    expected = (
        (
            BASELINE_VERSION
            if evidence.fallback.kind == "legacy"
            else ABSENCE_BASELINE_VERSION
        ),
        evidence.scope_id,
        evidence.legacy_league,
        evidence.legacy_season,
        evidence.trust_label,
    )
    actual = tuple(
        row.get(field)
        for field in (
            "baseline_version",
            "scope_id",
            "legacy_league",
            "legacy_season",
            "trust_label",
        )
    )
    if actual != expected:
        raise MigrationError(
            "existing fallback baseline conflicts with promotion evidence"
        )
    try:
        metrics = json.loads(row.get("entity_metrics_json"))
        snapshot_ids = json.loads(row.get("legacy_snapshot_ids_json"))
    except (TypeError, json.JSONDecodeError) as exc:
        raise MigrationError("existing fallback baseline payload is invalid") from exc
    if canonical_json(metrics) != row.get("entity_metrics_json"):
        raise MigrationError("existing fallback baseline metrics are not canonical")
    if canonical_json(snapshot_ids) != row.get("legacy_snapshot_ids_json"):
        raise MigrationError("existing fallback baseline snapshots are not canonical")
    if evidence.fallback.kind == "absent" and metrics != {}:
        raise MigrationError("existing absence baseline contains legacy metrics")
    if set(snapshot_ids) != {
        "espn_schedule",
        "espn_lineup",
        "espn_matchsheet",
    } or any(type(value) is not int or value <= 0 for value in snapshot_ids.values()):
        raise MigrationError("existing fallback baseline snapshot set is invalid")
    _sha(row.get("registry_signature"), "baseline.registry_signature")
    _immutable_uri(row.get("durable_manifest_uri"), "baseline.durable_manifest_uri")
    _sha(row.get("durable_manifest_sha256"), "baseline.durable_manifest_sha256")
    _immutable_uri(
        row.get("replay_raw_manifest_uri"), "baseline.replay_raw_manifest_uri"
    )
    _sha(
        row.get("replay_raw_manifest_sha256"),
        "baseline.replay_raw_manifest_sha256",
    )
    base = {key: value for key, value in row.items() if key != "baseline_sha256"}
    captured_at = base.get("captured_at")
    if isinstance(captured_at, datetime) and captured_at.tzinfo is None:
        base["captured_at"] = captured_at.replace(tzinfo=UTC)
    if canonical_sha256(base) != row.get("baseline_sha256"):
        raise MigrationError("existing legacy baseline hash is invalid")
    return {**dict(row), "captured_at": base.get("captured_at")}


def _native_cutover(
    evidence: PromotionEvidence,
    baseline: Mapping[str, Any],
    predecessor: ScopeCutover | None = None,
) -> ScopeCutover:
    candidate = evidence.green_runs[-1]
    root_id = f"espn-native-{evidence.espn_id}-{evidence.source_season_year}"
    if predecessor is None:
        expected_id = root_id
        predecessor_id = predecessor_sha = None
        ancestors: tuple[str, ...] = ()
    else:
        if predecessor.active_source != evidence.fallback.kind:
            raise MigrationError(
                "native re-promotion requires the matching fallback rollback head"
            )
        expected_id = f"{root_id}-repromote-{predecessor.cutover_sha256[:16]}"
        predecessor_id = predecessor.cutover_id
        predecessor_sha = predecessor.cutover_sha256
        ancestors = (
            *predecessor.ancestor_cutover_sha256s,
            predecessor.cutover_sha256,
        )
        if evidence.effective_at <= predecessor.effective_at:
            raise MigrationError("re-promotion must follow the rollback effective time")
    if evidence.cutover_id != expected_id:
        raise MigrationError(
            f"cutover_id must equal deterministic current-head ID {expected_id}"
        )
    return ScopeCutover(
        cutover_id=evidence.cutover_id,
        scope_id=evidence.scope_id,
        active_source="native",
        previous_source=evidence.fallback.kind,
        predecessor_cutover_id=predecessor_id,
        predecessor_cutover_sha256=predecessor_sha,
        legacy_league=evidence.legacy_league,
        legacy_season=evidence.legacy_season,
        registry_signature=candidate.registry_signature,
        effective_at=evidence.effective_at,
        native_generation_id=candidate.generation_id,
        native_generation_signature=candidate.generation_signature,
        native_manifest_sha256=candidate.manifest_sha256,
        rollback_run_id=None,
        rollback_reason=None,
        metadata={
            "migration_version": MIGRATION_VERSION,
            "fallback": evidence.fallback.to_dict(),
            "baseline_sha256": baseline["baseline_sha256"],
            "durable_manifest_ref": candidate.durable_manifest_ref.to_dict(),
            "run_evidence_ref": candidate.run_evidence_ref.to_dict(),
            "replay_raw_manifest_ref": candidate.raw_manifest_ref.to_dict(),
            "registry_snapshot_ref": evidence.registry_snapshot_ref.to_dict(),
            "generation_snapshot_ref": candidate.generation_snapshot_ref.to_dict(),
            "success_receipt_ref": candidate.success_receipt_ref.to_dict(),
            "three_green_runs": [item.to_dict() for item in evidence.green_runs],
        },
        ancestor_cutover_sha256s=ancestors,
    )


def _promotion_result(
    evidence: PromotionEvidence,
    baseline: Mapping[str, Any],
    cutover: ScopeCutover,
) -> dict[str, Any]:
    result = {
        "schema_version": MIGRATION_VERSION,
        "mode": "apply",
        "status": "promoted",
        "mutates": True,
        "scope_id": evidence.scope_id,
        "registry_snapshot_ref": evidence.registry_snapshot_ref.to_dict(),
        "fallback": evidence.fallback.to_dict(),
        "trust_label": evidence.trust_label,
        "green_runs": [item.to_dict() for item in evidence.green_runs],
        "candidate": _candidate(evidence.green_runs[-1]),
        "baseline": _jsonable(baseline),
        "cutover": {
            **cutover.to_row(),
            "effective_at": cutover.effective_at.isoformat(),
        },
    }
    result["result_sha256"] = canonical_sha256(result)
    return result


def _is_exact_committed_promotion(
    cutover: ScopeCutover,
    evidence: PromotionEvidence,
    baseline: Mapping[str, Any],
) -> bool:
    candidate = evidence.green_runs[-1]
    root_id = f"espn-native-{evidence.espn_id}-{evidence.source_season_year}"
    expected_id = (
        root_id
        if cutover.predecessor_cutover_sha256 is None
        else f"{root_id}-repromote-{cutover.predecessor_cutover_sha256[:16]}"
    )
    metadata = dict(cutover.metadata)
    fallback_metadata_matches = metadata.get("fallback") == evidence.fallback.to_dict()
    if (
        not fallback_metadata_matches
        and evidence.evidence_version == LEGACY_PROMOTION_EVIDENCE_VERSION
        and evidence.fallback.kind == "legacy"
        and metadata.get("fallback") is None
        and metadata.get("migration_version") == LEGACY_MIGRATION_VERSION
    ):
        fallback_metadata_matches = True
    return (
        (
            cutover.cutover_id,
            cutover.scope_id,
            cutover.active_source,
            cutover.previous_source,
            cutover.legacy_league,
            cutover.legacy_season,
            cutover.registry_signature,
            cutover.effective_at,
            cutover.native_generation_id,
            cutover.native_generation_signature,
            cutover.native_manifest_sha256,
            metadata.get("baseline_sha256"),
            metadata.get("durable_manifest_ref"),
            metadata.get("run_evidence_ref"),
            metadata.get("replay_raw_manifest_ref"),
            metadata.get("registry_snapshot_ref"),
            metadata.get("generation_snapshot_ref"),
            metadata.get("success_receipt_ref"),
        )
        == (
            expected_id,
            evidence.scope_id,
            "native",
            evidence.fallback.kind,
            evidence.legacy_league,
            evidence.legacy_season,
            candidate.registry_signature,
            evidence.effective_at,
            candidate.generation_id,
            candidate.generation_signature,
            candidate.manifest_sha256,
            baseline["baseline_sha256"],
            candidate.durable_manifest_ref.to_dict(),
            candidate.run_evidence_ref.to_dict(),
            candidate.raw_manifest_ref.to_dict(),
            evidence.registry_snapshot_ref.to_dict(),
            candidate.generation_snapshot_ref.to_dict(),
            candidate.success_receipt_ref.to_dict(),
        )
        and evidence.cutover_id == expected_id
        and fallback_metadata_matches
        and canonical_json(metadata.get("three_green_runs"))
        == canonical_json([item.to_dict() for item in evidence.green_runs])
    )


def apply_promotion(
    evidence: PromotionEvidence,
    *,
    backend: MigrationBackend,
    lease_store: LeaseStore,
    now: datetime,
) -> dict[str, Any]:
    if not isinstance(evidence, PromotionEvidence):
        raise TypeError("evidence must be PromotionEvidence")
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise TypeError("now must be timezone-aware")
    observed_at = now.astimezone(UTC)
    if evidence.effective_at > observed_at:
        raise MigrationError("future cutover effective_at is not yet admissible")
    candidate = evidence.green_runs[-1]
    backend.ensure_objects()
    lease_store.migrate()
    owner = f"espn-migration/{evidence.cutover_id}"
    lease = lease_store.acquire(
        evidence.scope_id,
        owner,
        candidate.plan_signature,
        now=observed_at,
    )
    try:
        with lease_store.guard(lease, now=observed_at):
            existing_baseline = backend.baseline(evidence.scope_id)
            baseline = (
                None
                if existing_baseline is None
                else _validate_existing_baseline(existing_baseline, evidence)
            )
            latest = backend.latest_cutover(evidence.scope_id)
            if latest is not None and latest.active_source == "native":
                if baseline is None or not _is_exact_committed_promotion(
                    latest, evidence, baseline
                ):
                    raise MigrationError(
                        "scope already has a different native cutover head"
                    )
                cutover = latest
                backend.append_cutover(cutover)
            else:
                predecessor = latest
                if predecessor is not None and (
                    predecessor.active_source != evidence.fallback.kind
                    or predecessor.legacy_league != evidence.fallback.league
                    or predecessor.legacy_season != evidence.fallback.season
                ):
                    raise MigrationError("scope cutover head is invalid for promotion")

                manifest = backend.complete_manifest(
                    evidence.scope_id, candidate.generation_id
                )
                if not _manifest_matches(manifest, candidate):
                    raise MigrationError(
                        "candidate is not bound to the exact physical COMPLETE manifest"
                    )
                latest_manifest = backend.latest_complete_manifest(evidence.scope_id)
                if not _manifest_matches(latest_manifest, candidate):
                    raise MigrationError(
                        "candidate is no longer the latest COMPLETE manifest under lease"
                    )
                backend.verify_candidate(candidate, evidence.registry_snapshot_ref)
                if baseline is None:
                    if evidence.fallback.kind == "legacy":
                        metrics, snapshot_ids = backend.legacy_baseline(
                            str(evidence.legacy_league),
                            str(evidence.legacy_season),
                        )
                    else:
                        metrics = {}
                        snapshot_ids = backend.absence_baseline()
                    baseline = _baseline_row(
                        evidence,
                        metrics=metrics,
                        snapshot_ids=snapshot_ids,
                        captured_at=observed_at,
                    )
                    backend.append_baseline(baseline)
                cutover = _native_cutover(evidence, baseline, predecessor)
                backend.append_cutover(cutover)
    finally:
        lease_store.release(lease, now=observed_at)
    return _promotion_result(evidence, baseline, cutover)


def build_rollback_plan(
    promotion_report: Mapping[str, Any],
    *,
    reason: str,
    output_path: str | Path,
) -> dict[str, Any]:
    if (
        not isinstance(promotion_report, Mapping)
        or promotion_report.get("schema_version")
        not in {MIGRATION_VERSION, LEGACY_MIGRATION_VERSION}
        or promotion_report.get("status") != "promoted"
    ):
        raise MigrationError("rollback requires a successful promotion report")
    report_base = {
        key: value for key, value in promotion_report.items() if key != "result_sha256"
    }
    if canonical_sha256(report_base) != promotion_report.get("result_sha256"):
        raise MigrationError("promotion report SHA-256 is invalid")
    scope_id, _, _ = _scope(promotion_report.get("scope_id"))
    cutover = promotion_report.get("cutover")
    candidate = promotion_report.get("candidate")
    baseline = promotion_report.get("baseline")
    if not all(isinstance(value, Mapping) for value in (cutover, candidate, baseline)):
        raise MigrationError("promotion report rollback binding is incomplete")
    if promotion_report.get("schema_version") == MIGRATION_VERSION:
        fallback = FallbackDescriptor.from_mapping(promotion_report.get("fallback"))
    else:
        legacy = promotion_report.get("legacy_scope")
        if not isinstance(legacy, Mapping):
            raise MigrationError(
                "legacy promotion report rollback binding is incomplete"
            )
        fallback = FallbackDescriptor(
            kind="legacy",
            league=_required(legacy.get("league"), "legacy_scope.league"),
            season=_required(legacy.get("season"), "legacy_scope.season"),
        )
    predecessor_id = _required(cutover.get("cutover_id"), "cutover.cutover_id")
    predecessor_sha = _sha(cutover.get("cutover_sha256"), "cutover.cutover_sha256")
    try:
        predecessor_ancestors = json.loads(
            _required(
                cutover.get("ancestor_cutover_sha256_json"),
                "cutover.ancestor_cutover_sha256_json",
            )
        )
    except json.JSONDecodeError as exc:
        raise MigrationError("promotion cutover ancestry is invalid") from exc
    if (
        not isinstance(predecessor_ancestors, list)
        or any(
            _SHA_RE.fullmatch(item) is None
            for item in predecessor_ancestors
            if isinstance(item, str)
        )
        or not all(isinstance(item, str) for item in predecessor_ancestors)
    ):
        raise MigrationError("promotion cutover ancestry is invalid")
    effective_at = _utc(
        cutover.get("effective_at"), "cutover.effective_at"
    ) + timedelta(microseconds=1)
    rollback_id = predecessor_id + "-rollback"
    baseline_sha256 = _sha(baseline.get("baseline_sha256"), "baseline.baseline_sha256")
    if (
        cutover.get("active_source") != "native"
        or cutover.get("previous_source") != fallback.kind
        or cutover.get("legacy_league") != fallback.league
        or cutover.get("legacy_season") != fallback.season
        or cutover.get("native_generation_id") != candidate.get("generation_id")
        or cutover.get("native_generation_signature")
        != candidate.get("generation_signature")
        or cutover.get("native_manifest_sha256") != candidate.get("manifest_sha256")
    ):
        raise MigrationError("promotion report fallback/candidate binding is invalid")
    try:
        metadata = json.loads(_required(cutover.get("metadata_json"), "metadata_json"))
    except json.JSONDecodeError as exc:
        raise MigrationError("promotion cutover metadata is invalid") from exc
    legacy_metadata_compatibility = (
        fallback.kind == "legacy"
        and metadata.get("migration_version") == LEGACY_MIGRATION_VERSION
        and metadata.get("fallback") is None
    )
    if metadata.get("baseline_sha256") != baseline_sha256 or (
        promotion_report.get("schema_version") == MIGRATION_VERSION
        and metadata.get("fallback") != fallback.to_dict()
        and not legacy_metadata_compatibility
    ):
        raise MigrationError("promotion report baseline/fallback binding is invalid")
    result = {
        "schema_version": ROLLBACK_PLAN_VERSION,
        "mode": "dry_run",
        "status": "planned",
        "mutates": False,
        "scope_id": scope_id,
        "cutover_id": rollback_id,
        "effective_at": effective_at.isoformat(),
        "predecessor_cutover_id": predecessor_id,
        "predecessor_cutover_sha256": predecessor_sha,
        "ancestor_cutover_sha256s": [*predecessor_ancestors, predecessor_sha],
        "fallback": fallback.to_dict(),
        "registry_signature": _sha(
            candidate.get("registry_signature"), "candidate.registry_signature"
        ),
        "plan_signature": _sha(
            candidate.get("plan_signature"), "candidate.plan_signature"
        ),
        "rollback_run_id": "rollback/" + rollback_id,
        "reason": _required(reason, "rollback reason"),
        "baseline_sha256": baseline_sha256,
        "output_path": str(Path(output_path).resolve()),
    }
    result["plan_sha256"] = canonical_sha256(result)
    return result


def _validate_rollback_plan(plan: Mapping[str, Any]) -> FallbackDescriptor:
    common = {
        "schema_version",
        "mode",
        "status",
        "mutates",
        "scope_id",
        "cutover_id",
        "effective_at",
        "predecessor_cutover_id",
        "predecessor_cutover_sha256",
        "ancestor_cutover_sha256s",
        "registry_signature",
        "plan_signature",
        "rollback_run_id",
        "reason",
        "baseline_sha256",
        "output_path",
        "plan_sha256",
    }
    version = plan.get("schema_version")
    expected = (
        common | {"fallback"}
        if version == ROLLBACK_PLAN_VERSION
        else common | {"legacy_league", "legacy_season"}
        if version == LEGACY_ROLLBACK_PLAN_VERSION
        else set()
    )
    if not expected or set(plan) != expected:
        raise MigrationError("rollback plan schema mismatch")
    base = {key: value for key, value in plan.items() if key != "plan_sha256"}
    if canonical_sha256(base) != plan.get("plan_sha256"):
        raise MigrationError("rollback plan SHA-256 mismatch")
    if (
        plan.get("mode") != "dry_run"
        or plan.get("status") != "planned"
        or plan.get("mutates") is not False
    ):
        raise MigrationError("rollback plan state is invalid")
    _scope(plan.get("scope_id"))
    _required(plan.get("cutover_id"), "cutover_id")
    _utc(plan.get("effective_at"), "effective_at")
    _required(plan.get("predecessor_cutover_id"), "predecessor_cutover_id")
    predecessor_sha = _sha(
        plan.get("predecessor_cutover_sha256"), "predecessor_cutover_sha256"
    )
    ancestors = plan.get("ancestor_cutover_sha256s")
    if (
        not isinstance(ancestors, list)
        or not ancestors
        or ancestors[-1] != predecessor_sha
        or any(
            not isinstance(item, str) or _SHA_RE.fullmatch(item) is None
            for item in ancestors
        )
        or len(set(ancestors)) != len(ancestors)
    ):
        raise MigrationError("rollback ancestry is invalid")
    _sha(plan.get("registry_signature"), "registry_signature")
    _sha(plan.get("plan_signature"), "plan_signature")
    _required(plan.get("rollback_run_id"), "rollback_run_id")
    _required(plan.get("reason"), "reason")
    _sha(plan.get("baseline_sha256"), "baseline_sha256")
    _required(plan.get("output_path"), "output_path")
    if version == ROLLBACK_PLAN_VERSION:
        return FallbackDescriptor.from_mapping(plan.get("fallback"))
    return FallbackDescriptor(
        kind="legacy",
        league=_required(plan.get("legacy_league"), "legacy_league"),
        season=_required(plan.get("legacy_season"), "legacy_season"),
    )


def apply_rollback(
    plan: Mapping[str, Any],
    *,
    backend: MigrationBackend,
    lease_store: LeaseStore,
    now: datetime,
) -> dict[str, Any]:
    fallback = _validate_rollback_plan(plan)
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise TypeError("now must be timezone-aware")
    observed_at = now.astimezone(UTC)
    backend.ensure_objects()
    lease_store.migrate()
    lease = lease_store.acquire(
        plan["scope_id"],
        f"espn-migration/{plan['cutover_id']}",
        plan["plan_signature"],
        now=observed_at,
    )
    try:
        with lease_store.guard(lease, now=observed_at):
            latest = backend.latest_cutover(plan["scope_id"])
            rollback = ScopeCutover(
                cutover_id=plan["cutover_id"],
                scope_id=plan["scope_id"],
                active_source=fallback.kind,
                previous_source="native",
                predecessor_cutover_id=plan["predecessor_cutover_id"],
                predecessor_cutover_sha256=plan["predecessor_cutover_sha256"],
                legacy_league=fallback.league,
                legacy_season=fallback.season,
                registry_signature=plan["registry_signature"],
                effective_at=_utc(plan["effective_at"], "effective_at"),
                native_generation_id=None,
                native_generation_signature=None,
                native_manifest_sha256=None,
                rollback_run_id=plan["rollback_run_id"],
                rollback_reason=plan["reason"],
                metadata={
                    "migration_version": (
                        LEGACY_MIGRATION_VERSION
                        if plan["schema_version"] == LEGACY_ROLLBACK_PLAN_VERSION
                        else MIGRATION_VERSION
                    ),
                    **(
                        {}
                        if plan["schema_version"] == LEGACY_ROLLBACK_PLAN_VERSION
                        else {"fallback": fallback.to_dict()}
                    ),
                    "baseline_sha256": plan["baseline_sha256"],
                    "rollback_plan_sha256": plan["plan_sha256"],
                },
                ancestor_cutover_sha256s=tuple(plan["ancestor_cutover_sha256s"]),
            )
            if latest is None:
                raise MigrationError("rollback predecessor is absent")
            if (latest.cutover_id, latest.cutover_sha256) == (
                rollback.cutover_id,
                rollback.cutover_sha256,
            ):
                pass
            elif (
                latest.cutover_id,
                latest.cutover_sha256,
                latest.active_source,
            ) != (
                plan["predecessor_cutover_id"],
                plan["predecessor_cutover_sha256"],
                "native",
            ):
                raise MigrationError("rollback predecessor is stale")
            else:
                latest_metadata = dict(latest.metadata)
                if (
                    latest.previous_source,
                    latest.legacy_league,
                    latest.legacy_season,
                    latest.registry_signature,
                    latest_metadata.get("baseline_sha256"),
                ) != (
                    fallback.kind,
                    fallback.league,
                    fallback.season,
                    plan["registry_signature"],
                    plan["baseline_sha256"],
                ):
                    raise MigrationError(
                        "rollback fallback does not match the native predecessor"
                    )
                backend.append_cutover(rollback)
    finally:
        lease_store.release(lease, now=observed_at)
    result = {
        **dict(plan),
        "mode": "apply",
        "status": "rolled_back",
        "mutates": True,
        "cutover": {
            **rollback.to_row(),
            "effective_at": rollback.effective_at.isoformat(),
        },
    }
    result["result_sha256"] = canonical_sha256(result)
    return result


class RepositoryMigrationBackend:
    """Production Trino/Iceberg adapter; no connection is opened at import."""

    _LEGACY_KEYS = {
        "schedule": "COALESCE(CAST(game_id AS varchar), game)",
        "lineup": "concat_ws('|', game, team, player)",
        "matchsheet": "concat_ws('|', game, team)",
    }

    def __init__(self, repository: EspnBronzeRepository) -> None:
        self.repository = repository

    def ensure_objects(self) -> None:
        self.repository.ensure_objects()

    def absence_baseline(self) -> Mapping[str, int]:
        snapshot_ids: dict[str, int] = {}
        for entity in self._LEGACY_KEYS:
            table = f"espn_{entity}"
            ref_rows = self.repository._execute(
                f'SELECT snapshot_id FROM {self.repository.catalog}.{self.repository.schema}."{table}$refs" '
                "WHERE name = 'main' AND type = 'BRANCH'"
            )
            if len(ref_rows) != 1:
                raise MigrationError(f"legacy {table} main branch is ambiguous")
            raw_snapshot = ref_rows[0]
            snapshot_id = (
                raw_snapshot.get("snapshot_id")
                if isinstance(raw_snapshot, Mapping)
                else raw_snapshot[0]
            )
            if type(snapshot_id) is not int or snapshot_id <= 0:
                raise MigrationError(f"legacy {table} main snapshot is invalid")
            snapshot_ids[table] = snapshot_id
        return snapshot_ids

    def legacy_baseline(
        self, league: str, season: str
    ) -> tuple[Mapping[str, Mapping[str, Any]], Mapping[str, int]]:
        output = {}
        snapshot_ids = dict(self.absence_baseline())
        for entity, key_sql in self._LEGACY_KEYS.items():
            table = f"espn_{entity}"
            snapshot_id = snapshot_ids[table]
            rows = self.repository._execute(
                f"""SELECT COUNT(*) AS row_count,
COUNT(DISTINCT {key_sql}) AS distinct_key_count,
COUNT_IF({key_sql} IS NULL) AS null_key_count,
CAST(MIN(match_date) AS varchar) AS min_match_date,
CAST(MAX(match_date) AS varchar) AS max_match_date,
CAST(MAX(_ingested_at) AS varchar) AS max_ingested_at
FROM {self.repository.catalog}.{self.repository.schema}.{table} FOR VERSION AS OF {snapshot_id}
WHERE league = ? AND CAST(season AS varchar) = ?"""
                if entity == "schedule"
                else f"""SELECT COUNT(*) AS row_count,
COUNT(DISTINCT {key_sql}) AS distinct_key_count,
COUNT_IF({key_sql} IS NULL) AS null_key_count,
CAST(NULL AS varchar) AS min_match_date,
CAST(NULL AS varchar) AS max_match_date,
CAST(MAX(_ingested_at) AS varchar) AS max_ingested_at
FROM {self.repository.catalog}.{self.repository.schema}.{table} FOR VERSION AS OF {snapshot_id}
WHERE league = ? AND CAST(season AS varchar) = ?""",
                (league, season),
            )
            if len(rows) != 1:
                raise MigrationError(f"legacy {entity} baseline query returned no row")
            row = rows[0]
            values = (
                tuple(
                    row.get(name)
                    for name in (
                        "row_count",
                        "distinct_key_count",
                        "null_key_count",
                        "min_match_date",
                        "max_match_date",
                        "max_ingested_at",
                    )
                )
                if isinstance(row, Mapping)
                else tuple(row)
            )
            if len(values) != 6:
                raise MigrationError(f"legacy {entity} baseline row is malformed")
            output[entity] = dict(
                zip(
                    (
                        "row_count",
                        "distinct_key_count",
                        "null_key_count",
                        "min_match_date",
                        "max_match_date",
                        "max_ingested_at",
                    ),
                    values,
                )
            )
        return output, snapshot_ids

    def verify_candidate(
        self, candidate: GreenRunEvidence, registry_snapshot_ref: ArtifactRef
    ) -> None:
        try:
            generation = runner.load_scope_snapshot(
                candidate.generation_snapshot_ref.uri,
                artifact_sha256=candidate.generation_snapshot_ref.sha256,
                expected_scope_id=candidate.scope_id,
            )
        except Exception as exc:
            raise MigrationError(
                f"cannot load candidate generation snapshot: {exc}"
            ) from exc
        if (
            generation.run_id,
            generation.generation_id,
            generation.generation_signature,
            generation.manifest_sha256,
            generation.registry_snapshot_uri,
            generation.registry_signature,
            generation.plan_signature,
        ) != (
            candidate.run_id,
            candidate.generation_id,
            candidate.generation_signature,
            candidate.manifest_sha256,
            registry_snapshot_ref.uri,
            candidate.registry_signature,
            candidate.plan_signature,
        ):
            raise MigrationError("candidate generation snapshot identity mismatch")
        try:
            report = self.repository.verify_published_scope(generation)
        except Exception as exc:
            raise MigrationError(
                f"physical candidate verification failed: {exc}"
            ) from exc
        if not report.passed:
            raise MigrationError(
                "physical candidate verification failed: " + "; ".join(report.failures)
            )

    def complete_manifest(
        self, scope_id: str, generation_id: str
    ) -> Mapping[str, Any] | None:
        return self.repository._existing_manifest(scope_id, generation_id)

    def latest_complete_manifest(self, scope_id: str) -> Mapping[str, Any] | None:
        rows = self.repository._execute(
            "SELECT "
            + ", ".join(f'"{name}"' for name in _MANIFEST_IDENTITY_COLUMNS)
            + f" FROM {self.repository.catalog}.{self.repository.schema}.{MANIFEST_TABLE} "
            + 'WHERE "scope_id" = ? AND "status" = \'complete\' '
            + 'ORDER BY "completed_at" DESC, "generation_id" DESC, "manifest_sha256" DESC LIMIT 1',
            (scope_id,),
        )
        if not rows:
            return None
        raw = rows[0]
        values = (
            tuple(raw.get(name) for name in _MANIFEST_IDENTITY_COLUMNS)
            if isinstance(raw, Mapping)
            else tuple(raw)
        )
        if len(values) != len(_MANIFEST_IDENTITY_COLUMNS):
            raise MigrationError("latest COMPLETE manifest row is malformed")
        return dict(zip(_MANIFEST_IDENTITY_COLUMNS, values))

    def baseline(self, scope_id: str) -> Mapping[str, Any] | None:
        rows = self.repository._execute(
            "SELECT "
            + ", ".join(f'"{name}"' for name in _BASELINE_COLUMNS)
            + f" FROM {self.repository.catalog}.{self.repository.schema}.{BASELINE_TABLE} "
            + 'WHERE "scope_id" = ?',
            (scope_id,),
        )
        if not rows:
            return None
        normalized: list[dict[str, Any]] = []
        for raw in rows:
            values = (
                tuple(raw.get(name) for name in _BASELINE_COLUMNS)
                if isinstance(raw, Mapping)
                else tuple(raw)
            )
            if len(values) != len(_BASELINE_COLUMNS):
                raise MigrationError("legacy baseline row is malformed")
            normalized.append(dict(zip(_BASELINE_COLUMNS, values)))
        hashes = {row.get("baseline_sha256") for row in normalized}
        if len(hashes) != 1:
            raise MigrationError("conflicting legacy baselines share one scope")
        return normalized[0]

    def append_baseline(self, row: Mapping[str, Any]) -> None:
        self.repository._write(BASELINE_TABLE, [row])

    def latest_cutover(self, scope_id: str) -> ScopeCutover | None:
        columns = (
            "cutover_id",
            "scope_id",
            "active_source",
            "previous_source",
            "predecessor_cutover_id",
            "predecessor_cutover_sha256",
            "legacy_league",
            "legacy_season",
            "registry_signature",
            "effective_at",
            "native_generation_id",
            "native_generation_signature",
            "native_manifest_sha256",
            "rollback_run_id",
            "rollback_reason",
            "metadata_json",
            "ancestor_cutover_sha256_json",
        )
        rows = self.repository._execute(
            "SELECT "
            + ", ".join(f'"{name}"' for name in columns)
            + f" FROM {self.repository.catalog}.{self.repository.schema}.{CUTOVER_TABLE} "
            + 'WHERE "scope_id" = ? ORDER BY "effective_at" DESC, "cutover_id" DESC, "cutover_sha256" DESC LIMIT 1',
            (scope_id,),
        )
        if not rows:
            return None
        raw = rows[0]
        values = (
            tuple(raw.get(name) for name in columns)
            if isinstance(raw, Mapping)
            else tuple(raw)
        )
        if len(values) != len(columns):
            raise MigrationError("latest cutover row is malformed")
        row = dict(zip(columns, values))
        try:
            metadata = json.loads(row.pop("metadata_json"))
            ancestry = tuple(json.loads(row.pop("ancestor_cutover_sha256_json")))
            effective_at = row.get("effective_at")
            if isinstance(effective_at, datetime) and effective_at.tzinfo is None:
                row["effective_at"] = effective_at.replace(tzinfo=UTC)
            return ScopeCutover(
                **row, metadata=metadata, ancestor_cutover_sha256s=ancestry
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise MigrationError(f"latest cutover row is invalid: {exc}") from exc

    def append_cutover(self, cutover: ScopeCutover) -> None:
        self.repository.append_cutover(cutover)


class ProductionLeaseStore:
    def __init__(self, store: PostgresEspnControlStore) -> None:
        self.store = store

    def migrate(self) -> None:
        self.store.migrate()

    def current_time(self) -> datetime:
        return self.store.current_time()

    def acquire(
        self, scope_id: str, owner_id: str, plan_signature: str, *, now: datetime
    ) -> object:
        return self.store.acquire_many(
            (scope_id,),
            owner_id=owner_id,
            plan_signature=plan_signature,
            now=self.store.current_time(),
            ttl=timedelta(minutes=30),
        )[0]

    @contextmanager
    def guard(self, lease: object, *, now: datetime):
        with self.store.publication_guard(lease, now=self.store.current_time()):
            yield

    def release(self, lease: object, *, now: datetime) -> None:
        self.store.release(lease, now=self.store.current_time())


__all__ = [
    "ABSENCE_BASELINE_VERSION",
    "BASELINE_TABLE",
    "BASELINE_VERSION",
    "FallbackDescriptor",
    "LEGACY_MIGRATION_VERSION",
    "LEGACY_PROMOTION_EVIDENCE_VERSION",
    "LEGACY_ROLLBACK_PLAN_VERSION",
    "MIGRATION_VERSION",
    "MigrationError",
    "ProductionLeaseStore",
    "PromotionEvidence",
    "PROMOTION_EVIDENCE_VERSION",
    "RepositoryMigrationBackend",
    "apply_promotion",
    "apply_rollback",
    "build_promotion_plan",
    "build_rollback_plan",
    "load_promotion_evidence",
    "migration_statements",
    "registry_fallback_descriptors",
]
