"""PostgreSQL-backed state machine for production FBref ingestion.

All claim, budget, and completion operations are transactional.  Workers may
crash at any point: an expired lease can be reclaimed, while a stale worker is
prevented from committing by the UUID token plus monotonically increasing
lease epoch.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from urllib.parse import urlsplit, urlunsplit

from scrapers.fbref.control.migrations import (
    MIGRATION_LOCK_KEY,
    MIGRATIONS,
    bootstrap_statements,
)
from scrapers.fbref.control.models import (
    BudgetReservation,
    CohortTarget,
    CompetitionRegistryEntry,
    FrontierProvenance,
    FrontierTarget,
    ObservationLease,
    SeasonAlias,
    SeasonRegistryEntry,
    TargetLease,
    ThrottleSlot,
)
from scrapers.fbref.policy import (
    DISCOVERY_SPINE_PAGE_KINDS,
    OTHER_PUBLICATION_CRITICAL_PAGE_KINDS,
    PUBLICATION_FRESHNESS_PAGE_KINDS,
)
from scrapers.fbref.settings import (
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    MIN_DOMAIN_INTERVAL_SECONDS,
)


class ControlStoreError(RuntimeError):
    """Base exception for control-plane failures."""


class ControlStoreConfigError(ControlStoreError):
    """The PostgreSQL connection configuration is missing or invalid."""


class MigrationError(ControlStoreError):
    """The installed migration history differs from this code version."""


class BudgetExceeded(ControlStoreError):
    """A fail-closed request or byte budget cannot fund a reservation."""


class LeaseLost(ControlStoreError):
    """A worker tried to mutate a target after losing its fenced lease."""


class StateConflict(ControlStoreError):
    """An idempotency key already exists with different immutable data."""


ConnectionFactory = Callable[[str], Any]
_CONTROL_URI_ENV = "FBREF_CONTROL_DB_URI"
_AIRFLOW_URI_ENV = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
_DATASET_STATES = {"pending", "succeeded", "failed", "skipped"}
_AVAILABILITY_STATES = {
    "available",
    "empty",
    "restricted",
    "not_applicable",
    "duplicate",
    "layout_only",
    "unknown",
    "error",
}
_SEASON_ALIAS_KINDS = {"source", "label", "url", "legacy", "operator"}
_PAGE_KIND_SLA_SECONDS = {
    "competition_index": 86_400,
    "schedule": 86_400,
    "match": 86_400,
    "competition": 604_800,
    "season": 604_800,
    "season_stats": 604_800,
    "standings": 604_800,
    "squad": 604_800,
    "player": 2_592_000,
    "matchlog": 2_592_000,
}
_PAGE_KIND_SLA_VALUES = ", ".join(
    f"('{page_kind}', {seconds})"
    for page_kind, seconds in sorted(_PAGE_KIND_SLA_SECONDS.items())
)
_PENDING_MATCH_SAMPLE_LIMIT = 10
_MAX_FRONTIER_DISCOVERY_TARGETS = 1000
_MAX_FRONTIER_DISCOVERY_EDGES = 5000
_FRONTIER_SCOPE_CTE = """
    WITH declared_scope AS (
        SELECT frontier.target_id, frontier.source,
               frontier.source_ids ->> 'competition_id' AS competition_id,
               frontier.source_ids ->> 'season_id' AS season_id
        FROM fbref_control.page_frontier AS frontier
        WHERE frontier.source_ids ? 'competition_id'
        UNION
        SELECT edge.child_target_id AS target_id, child.source,
               edge.carried_competition_id AS competition_id,
               edge.carried_season_id AS season_id
        FROM fbref_control.frontier_provenance AS edge
        JOIN fbref_control.page_frontier AS child
          ON child.target_id = edge.child_target_id
        WHERE edge.carried_competition_id IS NOT NULL
    ),
    canonical_scope AS (
        SELECT declared.target_id, declared.source,
               declared.competition_id,
               COALESCE(alias.season_id, declared.season_id) AS season_id
        FROM declared_scope AS declared
        LEFT JOIN fbref_control.season_alias AS alias
          ON alias.source = declared.source
         AND alias.competition_id = declared.competition_id
         AND alias.alias = declared.season_id
    ),
    scope_rollup AS (
        SELECT scoped.target_id,
               count(DISTINCT (
                   scoped.competition_id, scoped.season_id
               )) AS scope_count,
               bool_or(competition.competition_id IS NULL)
                   AS competition_missing,
               bool_or(competition.gender = 'female') AS has_female,
               bool_or(competition.gender = 'unknown') AS has_unknown,
               bool_or(
                   competition.competition_id IS NOT NULL
                   AND (
                       competition.gender <> 'male'
                       OR competition.crawl_state <> 'active'
                       OR competition.lifecycle_state NOT IN (
                           'present', 'missing_once'
                       )
                       OR NOT competition.present
                   )
               ) AS inactive_competition,
               bool_or(
                   scoped.season_id IS NOT NULL
                   AND (
                       season.season_id IS NULL
                       OR season.lifecycle_state <> 'present'
                       OR NOT season.present
                   )
               ) AS invalid_season,
               bool_or(
                   scoped.season_id IS NOT NULL
                   AND season.lifecycle_state = 'present'
                   AND season.present
                   AND season.is_current
               ) AS has_current_season,
               bool_or(scoped.season_id IS NULL)
                   AS has_competition_scope
        FROM canonical_scope AS scoped
        LEFT JOIN fbref_control.competition_registry AS competition
          ON competition.source = scoped.source
         AND competition.competition_id = scoped.competition_id
        LEFT JOIN fbref_control.season_registry AS season
          ON season.source = scoped.source
         AND season.competition_id = scoped.competition_id
         AND season.season_id = scoped.season_id
        GROUP BY scoped.target_id
    )
"""


def _postgres_dsn(uri: str) -> str:
    candidate = uri.strip()
    if not candidate:
        raise ControlStoreConfigError("FBref control database URI is empty")
    parsed = urlsplit(candidate)
    scheme = parsed.scheme.lower()
    if scheme in {"postgresql+psycopg2", "postgresql+psycopg"}:
        scheme = "postgresql"
    elif scheme == "postgres":
        scheme = "postgresql"
    if scheme != "postgresql" or not parsed.netloc:
        raise ControlStoreConfigError(
            "FBref control state requires a PostgreSQL URI"
        )
    return urlunsplit((scheme, parsed.netloc, parsed.path, parsed.query, ""))


def resolve_control_db_uri(
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """Resolve an explicit URI, then Airflow's configured metadata URI."""
    env = os.environ if environ is None else environ
    explicit = env.get(_CONTROL_URI_ENV, "").strip()
    if explicit:
        return _postgres_dsn(explicit)
    airflow_env = env.get(_AIRFLOW_URI_ENV, "").strip()
    if airflow_env:
        return _postgres_dsn(airflow_env)

    try:
        from airflow.configuration import conf

        airflow_default = conf.get("database", "sql_alchemy_conn").strip()
    except (ImportError, AttributeError, KeyError):
        airflow_default = ""
    if airflow_default:
        return _postgres_dsn(airflow_default)
    raise ControlStoreConfigError(
        f"Set {_CONTROL_URI_ENV} or {_AIRFLOW_URI_ENV} to the Airflow "
        "PostgreSQL database URI"
    )


def make_control_run_id(airflow_run_id: object, *, dag_id: object) -> str:
    """Map Airflow's free-form DAG/run identity to a stable UUID."""
    airflow_id = _text(airflow_run_id, "airflow_run_id")
    dag = _text(dag_id, "dag_id")
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fbref-control:{dag}:{airflow_id}"))


def make_logical_refresh_id(run_id: object, target_id: object) -> str:
    """Return a stable retry-safe refresh UUID for one run/target pair."""
    run = str(uuid.UUID(str(run_id)))
    target = str(target_id).strip()
    if not target:
        raise ValueError("target_id must not be empty")
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fbref:{run}:{target}"))


def make_budget_reservation_id(attempt_id: object) -> str:
    """Return the retry-safe budget idempotency key for one fetch attempt."""
    attempt = str(uuid.UUID(str(attempt_id)))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fbref-budget:{attempt}"))


def make_frontier_provenance_id(
    *,
    parent_target_id: object,
    child_target_id: object,
    relation: object,
    parent_content_hash: object,
    parser_version: object,
    carried_competition_id: Optional[object] = None,
    carried_season_id: Optional[object] = None,
) -> str:
    """Return the stable identity of one immutable discovery edge."""
    competition = (
        None
        if carried_competition_id is None
        else _text(carried_competition_id, "carried_competition_id")
    )
    season = (
        None
        if carried_season_id is None
        else _text(carried_season_id, "carried_season_id")
    )
    if season is not None and competition is None:
        raise ValueError("A carried season requires a carried competition")
    identity = json.dumps(
        [
            _text(parent_target_id, "parent_target_id"),
            _text(child_target_id, "child_target_id"),
            _text(relation, "relation"),
            competition,
            season,
            _text(parent_content_hash, "parent_content_hash"),
            _text(parser_version, "parser_version"),
        ],
        separators=(",", ":"),
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fbref-provenance:{identity}"))


def _uuid(value: object, name: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError(f"{name} must be a UUID") from exc


def _text(value: object, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _non_negative(value: object, name: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{name} must be non-negative")
    return normalized


def _json(value: Optional[Mapping[str, Any]]) -> str:
    return json.dumps(dict(value or {}), sort_keys=True, separators=(",", ":"))


def _json_mapping(value: object, name: str) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise StateConflict(f"{name} is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise StateConflict(f"{name} must be a JSON object")
    return dict(value)


def _sha256_hex(value: object, name: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{name} must be a SHA256 hex digest")
    return normalized


def _raw_baseline_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    evidence = dict(value)
    schema_version = _text(
        evidence.get("schema_version"), "raw baseline schema_version"
    )
    if not schema_version.startswith("fbref-raw-inventory-v"):
        raise ValueError("raw baseline schema_version is unsupported")
    return {
        "schema_version": schema_version,
        "raw_root_sha256": _sha256_hex(
            evidence.get("raw_root_sha256"), "raw baseline raw_root_sha256"
        ),
        "object_count": _non_negative(
            evidence.get("object_count"), "raw baseline object_count"
        ),
        "encoded_bytes": _non_negative(
            evidence.get("encoded_bytes"), "raw baseline encoded_bytes"
        ),
        "fingerprint_sha256": _sha256_hex(
            evidence.get("fingerprint_sha256"),
            "raw baseline fingerprint_sha256",
        ),
        "baseline_sha256": _sha256_hex(
            evidence.get("baseline_sha256"), "raw baseline baseline_sha256"
        ),
    }


def _attempt_snapshot(attempt_ids: Iterable[object]) -> dict[str, Any]:
    normalized_ids = sorted(_uuid(item, "attempt_id") for item in attempt_ids)
    encoded = json.dumps(
        normalized_ids, sort_keys=True, separators=(",", ":")
    ).encode("ascii")
    return {
        "schema_version": "fbref-raw-attempt-snapshot-v1",
        "successful_attempt_count": len(normalized_ids),
        "successful_attempt_ids_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _validated_attempt_snapshot(value: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = dict(value)
    if snapshot.get("schema_version") != "fbref-raw-attempt-snapshot-v1":
        raise StateConflict("raw fetch attempt snapshot schema is unsupported")
    return {
        "schema_version": "fbref-raw-attempt-snapshot-v1",
        "successful_attempt_count": _non_negative(
            snapshot.get("successful_attempt_count"),
            "successful_attempt_count",
        ),
        "successful_attempt_ids_sha256": _sha256_hex(
            snapshot.get("successful_attempt_ids_sha256"),
            "successful_attempt_ids_sha256",
        ),
    }


def _raw_audit_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    evidence = dict(value)
    if evidence.get("schema_version") != "fbref-raw-audit-anchor-v1":
        raise StateConflict("raw audit anchor schema is unsupported")
    if str(evidence.get("status") or "").casefold() != "passed":
        raise StateConflict("raw audit anchor must have passed status")
    run_type = str(evidence.get("run_type") or "").strip().casefold()
    if run_type not in {"current", "backfill", "replay"}:
        raise StateConflict("raw audit anchor run_type is unsupported")
    failure_count = _non_negative(
        evidence.get("failure_count"), "raw audit failure_count"
    )
    if failure_count:
        raise StateConflict("passed raw audit anchor has failures")
    zero_delta_required = evidence.get("zero_delta_required")
    if not isinstance(zero_delta_required, bool):
        raise StateConflict("raw audit zero_delta_required must be boolean")
    successful_attempt_count = _non_negative(
        evidence.get("successful_attempt_count"),
        "raw audit successful_attempt_count",
    )
    audited_attempt_count = _non_negative(
        evidence.get("audited_attempt_count"),
        "raw audit audited_attempt_count",
    )
    if audited_attempt_count != successful_attempt_count:
        raise StateConflict("raw audit did not audit every successful attempt")
    return {
        "schema_version": "fbref-raw-audit-anchor-v1",
        "status": "passed",
        "run_type": run_type,
        "audited_control_run_id": _uuid(
            evidence.get("audited_control_run_id"),
            "audited_control_run_id",
        ),
        "processing_control_run_id": _uuid(
            evidence.get("processing_control_run_id"),
            "processing_control_run_id",
        ),
        "successful_attempt_count": successful_attempt_count,
        "audited_attempt_count": audited_attempt_count,
        "failure_count": 0,
        "zero_delta_required": zero_delta_required,
        "attempt_snapshot_sha256": _sha256_hex(
            evidence.get("attempt_snapshot_sha256"),
            "raw audit attempt_snapshot_sha256",
        ),
        "artifact_sha256": _sha256_hex(
            evidence.get("artifact_sha256"),
            "raw audit artifact_sha256",
        ),
        "artifact": _text(evidence.get("artifact"), "raw audit artifact"),
    }


def _ordered_texts(value: object, name: str) -> list[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence")
    rendered = [_text(item, name) for item in value]
    if len(rendered) != len(set(rendered)):
        raise ValueError(f"{name} contains duplicates")
    return rendered


def _acceptance_cohort_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    evidence = dict(value)
    if evidence.get("schema_version") != "fbref-acceptance-cohort-v1":
        raise StateConflict("acceptance cohort schema is unsupported")
    if str(evidence.get("status") or "").casefold() != "frozen":
        raise StateConflict("acceptance cohort must have frozen status")
    scope = str(evidence.get("scope") or "").strip().casefold()
    if scope not in {"current", "history"}:
        raise ValueError("acceptance cohort scope must be current or history")
    target_ids = _ordered_texts(evidence.get("target_ids"), "target_ids")
    if not 1 <= len(target_ids) <= 25:
        raise ValueError("acceptance cohort must contain between 1 and 25 targets")
    cohort_size = _non_negative(evidence.get("cohort_size"), "cohort_size")
    if cohort_size != len(target_ids):
        raise StateConflict("acceptance cohort size does not match target_ids")
    encoded = json.dumps(
        target_ids, ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")
    cohort_sha256 = hashlib.sha256(encoded).hexdigest()
    supplied_hash = _sha256_hex(
        evidence.get("cohort_sha256"), "cohort_sha256"
    )
    if supplied_hash != cohort_sha256:
        raise StateConflict("acceptance cohort hash does not match target_ids")
    required_page_kinds = _ordered_texts(
        evidence.get("required_page_kinds"), "required_page_kinds"
    )
    if not required_page_kinds:
        raise ValueError("required_page_kinds must not be empty")
    required_routes = _ordered_texts(
        evidence.get("required_routes", ()), "required_routes"
    )
    slots = evidence.get("coverage_slots")
    if not isinstance(slots, Mapping) or not slots:
        raise ValueError("coverage_slots must be a non-empty mapping")
    coverage_slots = {
        _text(slot, "coverage slot"): _text(target, "coverage target")
        for slot, target in sorted(slots.items(), key=lambda item: str(item[0]))
    }
    if len(set(coverage_slots.values())) != len(coverage_slots):
        raise ValueError("coverage_slots must select distinct targets")
    if set(coverage_slots.values()) != set(target_ids):
        raise StateConflict("coverage_slots must cover the exact cohort")
    return {
        "schema_version": "fbref-acceptance-cohort-v1",
        "status": "frozen",
        "scope": scope,
        "cohort_size": cohort_size,
        "cohort_sha256": cohort_sha256,
        "target_ids": target_ids,
        "required_page_kinds": required_page_kinds,
        "required_routes": required_routes,
        "coverage_slots": coverage_slots,
    }


def _count_mapping(value: object, name: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    return {
        _text(key, name): _non_negative(count, name)
        for key, count in sorted(value.items(), key=lambda item: str(item[0]))
    }


def _bronze_acceptance_evidence(
    value: Mapping[str, Any], *, replay: bool
) -> dict[str, Any]:
    evidence = dict(value)
    schema = (
        "fbref-bronze-acceptance-replay-v1"
        if replay
        else "fbref-bronze-acceptance-v1"
    )
    if evidence.get("schema_version") != schema:
        raise StateConflict("bronze acceptance schema is unsupported")
    if str(evidence.get("status") or "").casefold() != "passed":
        raise StateConflict("bronze acceptance evidence must have passed status")
    strict_gates = evidence.get("strict_gates")
    if not isinstance(strict_gates, Mapping) or not strict_gates:
        raise ValueError("strict_gates must be a non-empty mapping")
    # JSON round-tripping rejects unserializable objects and detaches caller
    # owned dictionaries before they become immutable run metadata.
    normalized_gates = json.loads(_json(dict(strict_gates)))
    normalized = {
        "schema_version": schema,
        "status": "passed",
        "processing_control_run_id": _uuid(
            evidence.get("processing_control_run_id"),
            "processing_control_run_id",
        ),
        "scope": _text(evidence.get("scope"), "scope").casefold(),
        "cohort_size": _non_negative(
            evidence.get("cohort_size"), "cohort_size"
        ),
        "cohort_sha256": _sha256_hex(
            evidence.get("cohort_sha256"), "cohort_sha256"
        ),
        "page_kind_counts": _count_mapping(
            evidence.get("page_kind_counts"), "page_kind_counts"
        ),
        "route_counts": _count_mapping(
            evidence.get("route_counts"), "route_counts"
        ),
        "strict_gates": normalized_gates,
    }
    if replay:
        normalized["source_control_run_id"] = _uuid(
            evidence.get("source_control_run_id"), "source_control_run_id"
        )
    if normalized["scope"] not in {"current", "history"}:
        raise ValueError("bronze acceptance scope must be current or history")
    if normalized["cohort_size"] <= 0:
        raise ValueError("bronze acceptance cohort must not be empty")
    if sum(normalized["page_kind_counts"].values()) != normalized["cohort_size"]:
        raise StateConflict("page_kind_counts do not match acceptance cohort")
    if sum(normalized["route_counts"].values()) != normalized["cohort_size"]:
        raise StateConflict("route_counts do not match acceptance cohort")
    return normalized


def _row_dict(cursor: Any, row: Any) -> Optional[dict]:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    description = cursor.description or ()
    names = [getattr(item, "name", item[0]) for item in description]
    return dict(zip(names, row))


def _fetchone(cursor: Any) -> Optional[dict]:
    return _row_dict(cursor, cursor.fetchone())


def _fetchall(cursor: Any) -> list[dict]:
    return [dict(_row_dict(cursor, row) or {}) for row in cursor.fetchall()]


def _budget_from_row(row: Mapping[str, Any]) -> BudgetReservation:
    return BudgetReservation(
        reservation_id=str(row["reservation_id"]),
        run_id=str(row["run_id"]),
        logical_refresh_id=str(row["logical_refresh_id"]),
        requests_reserved=int(row["requests_reserved"]),
        bytes_reserved=int(row["bytes_reserved"]),
        status=str(row["status"]),
        requests_used=(
            None
            if row.get("requests_used") is None
            else int(row["requests_used"])
        ),
        bytes_used=(
            None if row.get("bytes_used") is None else int(row["bytes_used"])
        ),
    )


class ControlStore:
    """Transactional API over the dedicated ``fbref_control`` schema."""

    def __init__(
        self,
        db_uri: str,
        *,
        connection_factory: Optional[ConnectionFactory] = None,
    ) -> None:
        self.db_uri = _postgres_dsn(db_uri)
        self._connection_factory = connection_factory

    @classmethod
    def from_env(
        cls,
        *,
        environ: Optional[Mapping[str, str]] = None,
        connection_factory: Optional[ConnectionFactory] = None,
    ) -> "ControlStore":
        return cls(
            resolve_control_db_uri(environ),
            connection_factory=connection_factory,
        )

    def _connect(self):
        if self._connection_factory is not None:
            return self._connection_factory(self.db_uri)
        try:
            import psycopg2
        except ImportError as exc:  # pragma: no cover - production dependency
            raise ControlStoreConfigError(
                "psycopg2 is required to use the FBref control store"
            ) from exc
        return psycopg2.connect(self.db_uri)

    @staticmethod
    def _cursor(connection: Any):
        try:
            from psycopg2.extras import RealDictCursor
        except ImportError:
            return connection.cursor()
        try:
            return connection.cursor(cursor_factory=RealDictCursor)
        except TypeError:
            return connection.cursor()

    @contextmanager
    def _transaction(self, existing_cursor: Any = None):
        if existing_cursor is not None:
            yield existing_cursor
            return
        connection = self._connect()
        cursor = self._cursor(connection)
        try:
            yield cursor
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    def migrate(self) -> tuple[int, ...]:
        """Apply all pending migrations under one transaction advisory lock."""
        applied_now = []
        with self._transaction() as cursor:
            for statement in bootstrap_statements():
                cursor.execute(statement)
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_KEY,))
            cursor.execute(
                """
                SELECT version, name, checksum
                FROM fbref_control.schema_migration
                ORDER BY version
                """
            )
            installed = {int(row["version"]): row for row in _fetchall(cursor)}
            known_versions = {migration.version for migration in MIGRATIONS}
            unexpected = sorted(set(installed) - known_versions)
            if unexpected:
                raise MigrationError(
                    f"Database has unknown FBref migrations: {unexpected}"
                )
            for migration in MIGRATIONS:
                existing = installed.get(migration.version)
                if existing is not None:
                    if (
                        existing["name"] != migration.name
                        or existing["checksum"] != migration.checksum
                    ):
                        raise MigrationError(
                            "FBref migration history checksum mismatch at "
                            f"version {migration.version}"
                        )
                    continue
                for statement in migration.statements:
                    cursor.execute(statement)
                cursor.execute(
                    """
                    INSERT INTO fbref_control.schema_migration (
                        version, name, checksum
                    ) VALUES (%s, %s, %s)
                    """,
                    (migration.version, migration.name, migration.checksum),
                )
                applied_now.append(migration.version)
        return tuple(applied_now)

    def validate_migrations(self) -> dict[str, Any]:
        """Verify the complete installed migration history without mutating it.

        Runtime workers must never silently bootstrap or upgrade the control
        schema.  Deployment owns ``migrate()``; this read-only preflight proves
        that the database has exactly the versions, names, and checksums known
        to the running code before an Airflow control run is created.
        """

        with self._transaction() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(
                """
                SELECT version, name, checksum
                FROM fbref_control.schema_migration
                ORDER BY version
                """
            )
            installed = {
                int(row["version"]): row for row in _fetchall(cursor)
            }

        expected = {migration.version: migration for migration in MIGRATIONS}
        missing = sorted(set(expected) - set(installed))
        unexpected = sorted(set(installed) - set(expected))
        if missing or unexpected:
            raise MigrationError(
                "FBref control migration history is incomplete: "
                f"missing={missing}, unexpected={unexpected}"
            )
        for version, migration in expected.items():
            row = installed[version]
            if (
                row.get("name") != migration.name
                or row.get("checksum") != migration.checksum
            ):
                raise MigrationError(
                    "FBref migration history checksum mismatch at "
                    f"version {version}"
                )
        return {
            "status": "passed",
            "versions": sorted(expected),
            "checksum_verified": True,
            "read_only": True,
        }

    def create_run(
        self,
        run_type: str,
        *,
        request_limit: int,
        byte_limit: int,
        run_id: Optional[object] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> str:
        normalized_run_id = _uuid(run_id or uuid.uuid4(), "run_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.crawl_run (
                    run_id, run_type, request_limit, byte_limit, metadata
                ) VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id) DO NOTHING
                """,
                (
                    normalized_run_id,
                    _text(run_type, "run_type"),
                    _non_negative(request_limit, "request_limit"),
                    _non_negative(byte_limit, "byte_limit"),
                    _json(metadata),
                ),
            )
            cursor.execute(
                """
                SELECT run_type, request_limit, byte_limit, metadata
                FROM fbref_control.crawl_run WHERE run_id = %s
                """,
                (normalized_run_id,),
            )
            existing = _fetchone(cursor)
            if existing is None:
                raise ControlStoreError("Run insert returned no row")
            expected = (
                _text(run_type, "run_type"),
                _non_negative(request_limit, "request_limit"),
                _non_negative(byte_limit, "byte_limit"),
            )
            actual = (
                existing["run_type"],
                int(existing["request_limit"]),
                int(existing["byte_limit"]),
            )
            if actual != expected:
                raise StateConflict(
                    f"run_id {normalized_run_id} already has different limits"
                )
        return normalized_run_id

    def start_run(self, run_id: object) -> None:
        normalized = _uuid(run_id, "run_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET status = 'running',
                    started_at = COALESCE(started_at, clock_timestamp()),
                    updated_at = clock_timestamp()
                WHERE run_id = %s AND status IN ('pending', 'running')
                """,
                (normalized,),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"Run {normalized} cannot be started")

    def acquire_publication_lock(
        self,
        run_id: object,
        *,
        dag_id: object,
        source: str = "fbref",
        ttl_seconds: int = 8 * 24 * 60 * 60,
    ) -> dict:
        """Acquire the source lock spanning Bronze through master Gold."""

        normalized_run_id = _uuid(run_id, "run_id")
        normalized_dag_id = _text(dag_id, "dag_id")
        normalized_source = _text(source, "source")
        normalized_ttl = int(ttl_seconds)
        if not 60 <= normalized_ttl <= 14 * 24 * 60 * 60:
            raise ValueError("publication lock ttl_seconds must be 60..1209600")
        with self._transaction() as cursor:
            cursor.execute(
                "SELECT status FROM fbref_control.crawl_run WHERE run_id = %s",
                (normalized_run_id,),
            )
            run = _fetchone(cursor)
            if run is None or str(run["status"]) != "running":
                raise StateConflict(
                    "Publication lock owner must be an existing running run"
                )
            cursor.execute(
                """
                INSERT INTO fbref_control.publication_lock (
                    source, owner_run_id, owner_dag_id, expires_at
                ) VALUES (
                    %s, %s, %s,
                    clock_timestamp() + (%s * interval '1 second')
                )
                ON CONFLICT (source) DO NOTHING
                """,
                (
                    normalized_source,
                    normalized_run_id,
                    normalized_dag_id,
                    normalized_ttl,
                ),
            )
            inserted = cursor.rowcount == 1
            cursor.execute(
                """
                SELECT source, owner_run_id, owner_dag_id, acquired_at,
                       expires_at, released_at,
                       (released_at IS NULL
                        AND expires_at > clock_timestamp()) AS active
                FROM fbref_control.publication_lock
                WHERE source = %s
                FOR UPDATE
                """,
                (normalized_source,),
            )
            lock = _fetchone(cursor)
            if lock is None:  # pragma: no cover - guarded by INSERT/PK
                raise ControlStoreError("Publication lock row disappeared")
            current_owner = str(lock["owner_run_id"])
            if current_owner == normalized_run_id:
                if lock["released_at"] is not None or not bool(lock["active"]):
                    raise StateConflict(
                        "Released or expired publication generation cannot "
                        "be reacquired by the same run"
                    )
                return {
                    **lock,
                    "acquired": inserted,
                    "idempotent": not inserted,
                }
            if bool(lock["active"]):
                raise StateConflict(
                    "FBref publication is locked by another control run"
                )
            cursor.execute(
                """
                UPDATE fbref_control.publication_lock
                SET owner_run_id = %s,
                    owner_dag_id = %s,
                    acquired_at = clock_timestamp(),
                    expires_at = clock_timestamp()
                        + (%s * interval '1 second'),
                    released_at = NULL,
                    metadata = '{}'::jsonb
                WHERE source = %s
                RETURNING source, owner_run_id, owner_dag_id, acquired_at,
                          expires_at, released_at
                """,
                (
                    normalized_run_id,
                    normalized_dag_id,
                    normalized_ttl,
                    normalized_source,
                ),
            )
            replaced = _fetchone(cursor)
            return {**replaced, "acquired": True, "idempotent": False}

    def release_publication_lock(
        self,
        run_id: object,
        *,
        source: str = "fbref",
    ) -> dict:
        """Release only the publication generation owned by ``run_id``."""

        normalized_run_id = _uuid(run_id, "run_id")
        normalized_source = _text(source, "source")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT owner_run_id, released_at
                FROM fbref_control.publication_lock
                WHERE source = %s
                FOR UPDATE
                """,
                (normalized_source,),
            )
            lock = _fetchone(cursor)
            if lock is None:
                return {"source": normalized_source, "released": False}
            if str(lock["owner_run_id"]) != normalized_run_id:
                raise StateConflict(
                    "Cannot release another control run's publication lock"
                )
            if lock["released_at"] is not None:
                return {
                    "source": normalized_source,
                    "owner_run_id": normalized_run_id,
                    "released": True,
                    "idempotent": True,
                }
            cursor.execute(
                """
                UPDATE fbref_control.publication_lock
                SET released_at = clock_timestamp()
                WHERE source = %s AND owner_run_id = %s
                RETURNING released_at
                """,
                (normalized_source, normalized_run_id),
            )
            released = _fetchone(cursor)
            if released is None:  # pragma: no cover - row lock guards this
                raise ControlStoreError("Publication lock release lost its row")
            return {
                "source": normalized_source,
                "owner_run_id": normalized_run_id,
                "released": True,
                "idempotent": False,
                "released_at": released["released_at"],
            }

    def renew_publication_lock(
        self,
        run_id: object,
        *,
        source: str = "fbref",
        ttl_seconds: int = 8 * 24 * 60 * 60,
    ) -> dict:
        """Extend only an active exact-owner lock from the database clock."""

        normalized_run_id = _uuid(run_id, "run_id")
        normalized_source = _text(source, "source")
        normalized_ttl = int(ttl_seconds)
        if not 60 <= normalized_ttl <= 14 * 24 * 60 * 60:
            raise ValueError("publication lock ttl_seconds must be 60..1209600")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT owner_run_id, released_at,
                       expires_at > clock_timestamp() AS active
                FROM fbref_control.publication_lock
                WHERE source = %s
                FOR UPDATE
                """,
                (normalized_source,),
            )
            lock = _fetchone(cursor)
            if (
                lock is None
                or str(lock["owner_run_id"]) != normalized_run_id
                or lock["released_at"] is not None
                or not bool(lock["active"])
            ):
                raise StateConflict(
                    "Only the active publication owner can renew the lock"
                )
            cursor.execute(
                """
                UPDATE fbref_control.publication_lock
                SET expires_at = clock_timestamp()
                    + (%s * interval '1 second')
                WHERE source = %s AND owner_run_id = %s
                RETURNING source, owner_run_id, owner_dag_id, acquired_at,
                          expires_at, released_at
                """,
                (normalized_ttl, normalized_source, normalized_run_id),
            )
            renewed = _fetchone(cursor)
            if renewed is None:  # pragma: no cover - row lock guards this
                raise ControlStoreError("Publication lock renewal lost its row")
            return renewed

    def get_publication_lock(
        self, *, source: str = "fbref"
    ) -> Optional[dict]:
        """Return current lock evidence with a database-clock active flag."""

        normalized_source = _text(source, "source")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT source, owner_run_id, owner_dag_id, acquired_at,
                       expires_at, released_at,
                       (released_at IS NULL
                        AND expires_at > clock_timestamp()) AS active
                FROM fbref_control.publication_lock
                WHERE source = %s
                """,
                (normalized_source,),
            )
            return _fetchone(cursor)

    def assert_publication_lock_owner(
        self,
        run_id: object,
        *,
        source: str = "fbref",
    ) -> dict:
        """Fail unless ``run_id`` owns the active publication generation."""
        normalized_run_id = _uuid(run_id, "run_id")
        normalized_source = _text(source, "source")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT source, owner_run_id, owner_dag_id, acquired_at,
                       expires_at, released_at,
                       (released_at IS NULL
                        AND expires_at > clock_timestamp()) AS active
                FROM fbref_control.publication_lock
                WHERE source = %s
                """,
                (normalized_source,),
            )
            lock = _fetchone(cursor)
        if (
            lock is None
            or str(lock["owner_run_id"]) != normalized_run_id
            or lock["released_at"] is not None
            or not bool(lock["active"])
        ):
            raise StateConflict(
                "Active publication lock is not owned by this control run"
            )
        lock["owner_run_id"] = str(lock["owner_run_id"])
        lock["active"] = True
        return lock

    @contextmanager
    def guard_publication_lock(
        self,
        run_id: object,
        *,
        source: str = "fbref",
    ):
        """Fence one external publication while its owner is still active.

        The row lock is deliberately held across the caller's Trino/Iceberg
        writes.  A release, expiry takeover, or reassignment therefore cannot
        commit between the ownership check and the external publication.
        """

        normalized_run_id = _uuid(run_id, "run_id")
        normalized_source = _text(source, "source")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT source, owner_run_id, owner_dag_id, acquired_at,
                       expires_at, released_at,
                       (released_at IS NULL
                        AND expires_at > clock_timestamp()) AS active
                FROM fbref_control.publication_lock
                WHERE source = %s
                FOR UPDATE
                """,
                (normalized_source,),
            )
            lock = _fetchone(cursor)
            if (
                lock is None
                or str(lock["owner_run_id"]) != normalized_run_id
                or lock["released_at"] is not None
                or not bool(lock["active"])
            ):
                raise StateConflict(
                    "Active publication lock is not owned by this control run"
                )
            lock["owner_run_id"] = str(lock["owner_run_id"])
            lock["active"] = True
            yield lock

    def finish_run(self, run_id: object, *, succeeded: bool) -> None:
        normalized = _uuid(run_id, "run_id")
        status = "succeeded" if succeeded else "failed"
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET status = %s, finished_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE run_id = %s AND status = 'running'
                """,
                (status, normalized),
            )
            if cursor.rowcount != 1:
                cursor.execute(
                    "SELECT status FROM fbref_control.crawl_run WHERE run_id = %s",
                    (normalized,),
                )
                existing = _fetchone(cursor)
                if existing is None or existing["status"] != status:
                    raise StateConflict(f"Run {normalized} cannot finish as {status}")

    def get_run(self, run_id: object) -> Optional[dict]:
        with self._transaction() as cursor:
            cursor.execute(
                "SELECT * FROM fbref_control.crawl_run WHERE run_id = %s",
                (_uuid(run_id, "run_id"),),
            )
            return _fetchone(cursor)

    def record_raw_baseline(
        self,
        run_id: object,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Anchor one create-once raw inventory digest in control state."""

        run = _uuid(run_id, "run_id")
        normalized = _raw_baseline_evidence(evidence)
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status, metadata
                FROM fbref_control.crawl_run
                WHERE run_id = %s
                FOR UPDATE
                """,
                (run,),
            )
            row = _fetchone(cursor)
            if row is None:
                raise StateConflict(f"Raw baseline run {run} does not exist")
            metadata = _json_mapping(
                row.get("metadata") or {}, "crawl run metadata"
            )
            installed_value = metadata.get("raw_baseline")
            if installed_value is not None:
                installed = _raw_baseline_evidence(
                    _json_mapping(installed_value, "raw_baseline")
                )
                if installed != normalized:
                    raise StateConflict(
                        f"Run {run} already has a different raw baseline"
                    )
                return {**normalized, "idempotent": True}
            if row["status"] != "running":
                raise StateConflict(
                    f"Run {run} cannot anchor its first raw baseline after "
                    f"leaving running state ({row['status']})"
                )
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM fbref_control.fetch_attempt
                    WHERE run_id = %s
                ) AS source_started
                """,
                (run,),
            )
            progress = _fetchone(cursor) or {}
            if bool(progress.get("source_started")):
                raise StateConflict(
                    f"Run {run} cannot anchor its first raw baseline after "
                    "fetch processing started"
                )
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET metadata = metadata || %s::jsonb,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                """,
                (_json({"raw_baseline": normalized}), run),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"Raw baseline run {run} disappeared")
        return {**normalized, "idempotent": False}

    def get_raw_baseline(self, run_id: object) -> Optional[dict[str, Any]]:
        """Read the immutable control-plane baseline anchor for one run."""

        run = _uuid(run_id, "run_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT metadata -> 'raw_baseline' AS raw_baseline
                FROM fbref_control.crawl_run
                WHERE run_id = %s
                """,
                (run,),
            )
            row = _fetchone(cursor)
        if row is None:
            raise StateConflict(f"Raw baseline run {run} does not exist")
        value = row.get("raw_baseline")
        if value is None:
            return None
        return _raw_baseline_evidence(
            _json_mapping(value, "raw_baseline")
        )

    def record_raw_audit(
        self,
        run_id: object,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Anchor one passed final raw audit before run validation."""

        run = _uuid(run_id, "run_id")
        normalized = _raw_audit_evidence(evidence)
        if normalized["processing_control_run_id"] != run:
            raise StateConflict(
                "Raw audit processing_control_run_id does not match its run"
            )
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status, metadata
                FROM fbref_control.crawl_run
                WHERE run_id = %s
                FOR UPDATE
                """,
                (run,),
            )
            row = _fetchone(cursor)
            if row is None:
                raise StateConflict(f"Raw audit run {run} does not exist")
            metadata = _json_mapping(
                row.get("metadata") or {}, "crawl run metadata"
            )
            installed_value = metadata.get("raw_audit")
            if installed_value is not None:
                installed = _raw_audit_evidence(
                    _json_mapping(installed_value, "raw_audit")
                )
                if installed != normalized:
                    raise StateConflict(
                        f"Run {run} already has a different raw audit anchor"
                    )
                return {**normalized, "idempotent": True}
            if row["status"] != "running":
                raise StateConflict(
                    f"Run {run} cannot anchor its first raw audit after "
                    f"leaving running state ({row['status']})"
                )
            if metadata.get("raw_baseline") is None:
                raise StateConflict(
                    f"Run {run} cannot anchor raw audit without a baseline"
                )
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET metadata = metadata || %s::jsonb,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                """,
                (_json({"raw_audit": normalized}), run),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"Raw audit run {run} disappeared")
        return {**normalized, "idempotent": False}

    def get_raw_audit(self, run_id: object) -> Optional[dict[str, Any]]:
        """Read one create-once passed final raw-audit anchor."""

        run = _uuid(run_id, "run_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT metadata -> 'raw_audit' AS raw_audit
                FROM fbref_control.crawl_run
                WHERE run_id = %s
                """,
                (run,),
            )
            row = _fetchone(cursor)
        if row is None:
            raise StateConflict(f"Raw audit run {run} does not exist")
        value = row.get("raw_audit")
        if value is None:
            return None
        return _raw_audit_evidence(_json_mapping(value, "raw_audit"))

    def record_acceptance_cohort(
        self,
        run_id: object,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Create-once anchor for the exact ordered acceptance cohort."""

        run = _uuid(run_id, "run_id")
        normalized = _acceptance_cohort_evidence(evidence)
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status, run_type, request_limit, byte_limit, metadata
                FROM fbref_control.crawl_run
                WHERE run_id = %s
                FOR UPDATE
                """,
                (run,),
            )
            row = _fetchone(cursor)
            if row is None or row["status"] != "running":
                raise StateConflict(
                    f"Run {run} cannot anchor an acceptance cohort"
                )
            expected_scope = (
                "current" if row["run_type"] == "current" else "history"
                if row["run_type"] == "backfill"
                else None
            )
            metadata = _json_mapping(
                row.get("metadata") or {}, "crawl run metadata"
            )
            if (
                expected_scope != normalized["scope"]
                or int(row["request_limit"]) != 100
                or int(row["byte_limit"]) != 50 * 1024 * 1024
                or metadata.get("acceptance_profile") is not True
                or metadata.get("publication_eligible") is not False
                or int(metadata.get("shard_size") or 0) != 25
            ):
                raise StateConflict(
                    f"Run {run} is not the bounded nonpublishing acceptance profile"
                )
            cursor.execute(
                """
                SELECT target_id
                FROM fbref_control.run_target
                WHERE run_id = %s
                ORDER BY ordinal
                """,
                (run,),
            )
            installed_targets = [
                str(item["target_id"]) for item in _fetchall(cursor)
            ]
            if installed_targets != normalized["target_ids"]:
                raise StateConflict(
                    "acceptance cohort anchor differs from immutable run targets"
                )
            installed_value = metadata.get("acceptance_cohort")
            if installed_value is not None:
                installed = _acceptance_cohort_evidence(
                    _json_mapping(installed_value, "acceptance_cohort")
                )
                if installed != normalized:
                    raise StateConflict(
                        f"Run {run} already has different acceptance cohort evidence"
                    )
                return {**normalized, "idempotent": True}
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET metadata = metadata || %s::jsonb,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                """,
                (_json({"acceptance_cohort": normalized}), run),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"Acceptance run {run} disappeared")
        return {**normalized, "idempotent": False}

    def record_bronze_acceptance(
        self,
        run_id: object,
        evidence: Mapping[str, Any],
        *,
        replay: bool = False,
    ) -> dict[str, Any]:
        """Create-once passed strict-gate evidence before terminal success."""

        run = _uuid(run_id, "run_id")
        normalized = _bronze_acceptance_evidence(evidence, replay=replay)
        if normalized["processing_control_run_id"] != run:
            raise StateConflict(
                "bronze acceptance processing_control_run_id does not match run"
            )
        key = "bronze_acceptance_replay" if replay else "bronze_acceptance"
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status, metadata
                FROM fbref_control.crawl_run
                WHERE run_id = %s
                FOR UPDATE
                """,
                (run,),
            )
            row = _fetchone(cursor)
            if row is None or row["status"] != "running":
                raise StateConflict(
                    f"Run {run} cannot record first bronze acceptance evidence"
                )
            metadata = _json_mapping(
                row.get("metadata") or {}, "crawl run metadata"
            )
            if replay:
                raw_audit = metadata.get("raw_audit")
                if (
                    metadata.get("acceptance_replay") is not True
                    or metadata.get("publication_eligible") is not False
                    or str(
                        metadata.get("acceptance_replay_source_run_id") or ""
                    )
                    != normalized["source_control_run_id"]
                    or not isinstance(raw_audit, Mapping)
                    or str(raw_audit.get("status") or "").casefold()
                    != "passed"
                    or raw_audit.get("zero_delta_required") is not True
                ):
                    raise StateConflict(
                        f"Run {run} lacks strict acceptance replay prerequisites"
                    )
            else:
                cohort_value = metadata.get("acceptance_cohort")
                if not isinstance(cohort_value, Mapping):
                    raise StateConflict(
                        f"Run {run} has no frozen acceptance cohort"
                    )
                cohort = _acceptance_cohort_evidence(cohort_value)
                if (
                    cohort["scope"] != normalized["scope"]
                    or cohort["cohort_size"] != normalized["cohort_size"]
                    or cohort["cohort_sha256"] != normalized["cohort_sha256"]
                ):
                    raise StateConflict(
                        "bronze acceptance marker differs from frozen cohort"
                    )
            installed_value = metadata.get(key)
            if installed_value is not None:
                installed = _bronze_acceptance_evidence(
                    _json_mapping(installed_value, key), replay=replay
                )
                if installed != normalized:
                    raise StateConflict(
                        f"Run {run} already has different {key} evidence"
                    )
                return {**normalized, "idempotent": True}
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET metadata = metadata || %s::jsonb,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                """,
                (_json({key: normalized}), run),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"Acceptance run {run} disappeared")
        return {**normalized, "idempotent": False}

    def seal_raw_fetch_attempts(self, run_id: object) -> dict[str, Any]:
        """Freeze and fingerprint the successful-attempt set before audit.

        ``claim_targets`` holds the same crawl-run row lock, so no new worker
        can enter after the marker is installed.  The active-work checks make
        an in-flight completion lose this race safely: the audit retries only
        after that transaction has reached a terminal attempt state.
        """

        run = _uuid(run_id, "run_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status, metadata
                FROM fbref_control.crawl_run
                WHERE run_id = %s
                FOR UPDATE
                """,
                (run,),
            )
            row = _fetchone(cursor)
            if row is None:
                raise StateConflict(f"Raw audit run {run} does not exist")
            if row["status"] not in {
                "running",
                "succeeded",
                "failed",
                "cancelled",
            }:
                raise StateConflict(
                    f"Raw audit run {run} is not sealable ({row['status']})"
                )
            cursor.execute(
                """
                SELECT
                    (SELECT count(*)
                     FROM fbref_control.fetch_attempt
                     WHERE run_id = %s AND status = 'claimed')
                        AS claimed_attempts,
                    (SELECT count(*)
                     FROM fbref_control.page_frontier
                     WHERE lease_run_id = %s AND state = 'leased')
                        AS active_leases,
                    (SELECT count(*)
                     FROM fbref_control.budget_reservation
                     WHERE run_id = %s AND status = 'reserved')
                        AS active_reservations
                """,
                (run, run, run),
            )
            active = _fetchone(cursor) or {}
            active_counts = {
                key: int(active.get(key) or 0)
                for key in (
                    "claimed_attempts",
                    "active_leases",
                    "active_reservations",
                )
            }
            if any(active_counts.values()):
                raise StateConflict(
                    f"Raw audit run {run} still has active fetch work: "
                    f"{active_counts}"
                )
            cursor.execute(
                """
                SELECT attempt_id
                FROM fbref_control.fetch_attempt
                WHERE run_id = %s AND status = 'succeeded'
                ORDER BY attempt_id
                """,
                (run,),
            )
            snapshot = _attempt_snapshot(
                item["attempt_id"] for item in _fetchall(cursor)
            )
            metadata = _json_mapping(
                row.get("metadata") or {}, "crawl run metadata"
            )
            installed_value = metadata.get("raw_fetch_attempt_snapshot")
            if installed_value is not None:
                installed = _validated_attempt_snapshot(
                    _json_mapping(
                        installed_value, "raw_fetch_attempt_snapshot"
                    )
                )
                if installed != snapshot:
                    raise StateConflict(
                        f"Run {run} successful fetch attempts changed after seal"
                    )
                return {**snapshot, "idempotent": True}
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET metadata = metadata || %s::jsonb,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                """,
                (_json({"raw_fetch_attempt_snapshot": snapshot}), run),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"Raw audit run {run} disappeared")
        return {**snapshot, "idempotent": False}

    def reserve_budget(
        self,
        run_id: object,
        logical_refresh_id: object,
        *,
        requests: int = 1,
        bytes_: int,
        reservation_id: Optional[object] = None,
        attempt_id: Optional[object] = None,
    ) -> BudgetReservation:
        """Atomically reserve capacity before constructing a network request."""
        run = _uuid(run_id, "run_id")
        refresh = _uuid(logical_refresh_id, "logical_refresh_id")
        requested = _non_negative(requests, "requests")
        byte_count = _non_negative(bytes_, "bytes_")
        if reservation_id is None and attempt_id is None:
            raise ValueError(
                "reservation_id or attempt_id is required for idempotent budget use"
            )
        reservation = _uuid(
            reservation_id or make_budget_reservation_id(attempt_id),
            "reservation_id",
        )
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT * FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run,),
            )
            crawl_run = _fetchone(cursor)
            if crawl_run is None or crawl_run["status"] != "running":
                raise StateConflict(f"Run {run} is not running")
            cursor.execute(
                """
                SELECT * FROM fbref_control.budget_reservation
                WHERE reservation_id = %s FOR UPDATE
                """,
                (reservation,),
            )
            existing = _fetchone(cursor)
            if existing is not None:
                result = _budget_from_row(existing)
                if (
                    result.run_id != run
                    or result.logical_refresh_id != refresh
                    or result.requests_reserved != requested
                    or result.bytes_reserved != byte_count
                ):
                    raise StateConflict(
                        f"Refresh {refresh} already has a different reservation"
                    )
                return result

            projected_requests = (
                int(crawl_run["requests_used"])
                + int(crawl_run["requests_reserved"])
                + requested
            )
            projected_bytes = (
                int(crawl_run["bytes_used"])
                + int(crawl_run["bytes_reserved"])
                + byte_count
            )
            if (
                projected_requests > int(crawl_run["request_limit"])
                or projected_bytes > int(crawl_run["byte_limit"])
            ):
                raise BudgetExceeded(
                    f"Run {run} budget cannot reserve "
                    f"{requested} request(s) and {byte_count} bytes"
                )
            cursor.execute(
                """
                INSERT INTO fbref_control.budget_reservation (
                    reservation_id, run_id, logical_refresh_id,
                    requests_reserved, bytes_reserved
                ) VALUES (%s, %s, %s, %s, %s)
                RETURNING *
                """,
                (reservation, run, refresh, requested, byte_count),
            )
            row = _fetchone(cursor)
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET requests_reserved = requests_reserved + %s,
                    bytes_reserved = bytes_reserved + %s,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                """,
                (requested, byte_count, run),
            )
            if row is None:
                raise ControlStoreError("Budget reservation insert returned no row")
            return _budget_from_row(row)

    def settle_budget(
        self,
        reservation_id: object,
        *,
        requests_used: int,
        bytes_used: int,
    ) -> BudgetReservation:
        """Settle once; repeated settlement with identical usage is harmless."""
        reservation = _uuid(reservation_id, "reservation_id")
        used_requests = _non_negative(requests_used, "requests_used")
        used_bytes = _non_negative(bytes_used, "bytes_used")
        with self._transaction() as cursor:
            # Discover the owning run without taking a row lock. Every
            # mutating budget path then locks crawl_run before reservations,
            # which prevents reserve/settle/reap/abort lock inversions.
            cursor.execute(
                """
                SELECT * FROM fbref_control.budget_reservation
                WHERE reservation_id = %s
                """,
                (reservation,),
            )
            row = _fetchone(cursor)
            if row is None:
                raise StateConflict(f"Unknown budget reservation {reservation}")
            discovered = _budget_from_row(row)

            cursor.execute(
                """
                SELECT * FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (discovered.run_id,),
            )
            crawl_run = _fetchone(cursor)
            if crawl_run is None:
                raise StateConflict(f"Unknown run {discovered.run_id}")
            cursor.execute(
                """
                SELECT * FROM fbref_control.budget_reservation
                WHERE reservation_id = %s FOR UPDATE
                """,
                (reservation,),
            )
            row = _fetchone(cursor)
            if row is None:
                raise StateConflict(f"Unknown budget reservation {reservation}")
            current = _budget_from_row(row)
            if current.run_id != discovered.run_id:
                raise StateConflict(
                    f"Reservation {reservation} changed owning run"
                )
            if current.status == "settled":
                if (
                    current.requests_used != used_requests
                    or current.bytes_used != used_bytes
                ):
                    raise StateConflict(
                        f"Reservation {reservation} was already settled differently"
                    )
                return current
            cursor.execute(
                """
                UPDATE fbref_control.budget_reservation
                SET status = 'settled', requests_used = %s, bytes_used = %s,
                    settled_at = clock_timestamp()
                WHERE reservation_id = %s AND status = 'reserved'
                RETURNING *
                """,
                (used_requests, used_bytes, reservation),
            )
            settled = _fetchone(cursor)
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET requests_reserved = requests_reserved - %s,
                    bytes_reserved = bytes_reserved - %s,
                    requests_used = requests_used + %s,
                    bytes_used = bytes_used + %s,
                    budget_exceeded = budget_exceeded
                        OR requests_used + %s > request_limit
                        OR bytes_used + %s > byte_limit,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                """,
                (
                    current.requests_reserved,
                    current.bytes_reserved,
                    used_requests,
                    used_bytes,
                    used_requests,
                    used_bytes,
                    current.run_id,
                ),
            )
            if settled is None:
                raise StateConflict(f"Reservation {reservation} lost settlement race")
            return _budget_from_row(settled)

    def create_registry_snapshot(
        self,
        *,
        fetched_at: datetime,
        successful: bool,
        run_id: Optional[object] = None,
        content_hash: Optional[str] = None,
        source: str = "fbref",
        metadata: Optional[Mapping[str, Any]] = None,
        snapshot_id: Optional[object] = None,
    ) -> str:
        """Record the immutable source response used for reconciliation."""
        snapshot = _uuid(snapshot_id or uuid.uuid4(), "snapshot_id")
        normalized_run = None if run_id is None else _uuid(run_id, "run_id")
        normalized_source = _text(source, "source")
        with self._transaction() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.registry_snapshot (
                    snapshot_id, run_id, source, content_hash, successful,
                    fetched_at, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (snapshot_id) DO NOTHING
                """,
                (
                    snapshot,
                    normalized_run,
                    normalized_source,
                    content_hash,
                    bool(successful),
                    fetched_at,
                    _json(metadata),
                ),
            )
            cursor.execute(
                """
                SELECT run_id, source, content_hash, successful, fetched_at,
                       metadata
                FROM fbref_control.registry_snapshot
                WHERE snapshot_id = %s
                """,
                (snapshot,),
            )
            row = _fetchone(cursor)
            if row is None:
                raise ControlStoreError("Registry snapshot insert returned no row")
            actual_run = None if row["run_id"] is None else str(row["run_id"])
            actual_metadata = row.get("metadata") or {}
            if isinstance(actual_metadata, str):
                actual_metadata = json.loads(actual_metadata)
            if (
                actual_run != normalized_run
                or row["source"] != normalized_source
                or row["content_hash"] != content_hash
                or bool(row["successful"]) != bool(successful)
                or row["fetched_at"] != fetched_at
                or dict(actual_metadata) != dict(metadata or {})
            ):
                if bool(row["successful"]):
                    raise StateConflict(
                        f"snapshot_id {snapshot} already has different evidence"
                    )
                # A failed snapshot is evidence about our parse, not about the
                # source. Freezing it would poison this (parser, page) pair
                # forever: the retry that fixes the parser could never record
                # its result, and the target would be stuck until the parser
                # version changed. Successful snapshots stay immutable.
                cursor.execute(
                    """
                    UPDATE fbref_control.registry_snapshot
                    SET run_id = %s, source = %s, content_hash = %s,
                        successful = %s, fetched_at = %s, metadata = %s::jsonb
                    WHERE snapshot_id = %s
                    """,
                    (
                        normalized_run,
                        normalized_source,
                        content_hash,
                        bool(successful),
                        fetched_at,
                        _json(metadata),
                        snapshot,
                    ),
                )
        return snapshot

    @staticmethod
    def _validated_competitions(
        entries: Iterable[CompetitionRegistryEntry],
    ) -> list[CompetitionRegistryEntry]:
        result = []
        ids = set()
        urls = set()
        for entry in entries:
            competition_id = _text(entry.competition_id, "competition_id")
            canonical_url = _text(entry.canonical_url, "canonical_url")
            gender = _text(entry.gender, "gender").lower()
            if gender not in {"male", "female", "unknown"}:
                raise ValueError(
                    f"Unsupported gender {entry.gender!r} for {competition_id}"
                )
            if competition_id in ids or canonical_url in urls:
                raise ValueError("Competition snapshot contains duplicate IDs or URLs")
            ids.add(competition_id)
            urls.add(canonical_url)
            result.append(
                CompetitionRegistryEntry(
                    competition_id=competition_id,
                    canonical_url=canonical_url,
                    name=_text(entry.name, "name"),
                    gender=gender,
                    classification=_text(entry.classification, "classification"),
                    calendar_type=(
                        None
                        if entry.calendar_type is None
                        else _text(entry.calendar_type, "calendar_type")
                    ),
                    metadata=dict(entry.metadata),
                )
            )
        return result

    def reconcile_competitions(
        self,
        snapshot_id: object,
        entries: Iterable[CompetitionRegistryEntry],
        *,
        shrink_override_reason: Optional[object] = None,
    ) -> dict[str, int]:
        """Reconcile an accepted index snapshot without deleting history.

        Unknown-gender rows are durably stored as quarantined and then fail the
        caller, so operators can see and classify them without any downstream
        crawl. A drop of more than ten percent from the live registry fails
        closed unless an explicit, durable operator reason accompanies it.
        """
        snapshot = _uuid(snapshot_id, "snapshot_id")
        competitions = self._validated_competitions(entries)
        unknown_ids = sorted(
            entry.competition_id
            for entry in competitions
            if entry.gender == "unknown"
        )
        override_reason = (
            None
            if shrink_override_reason is None
            else _text(shrink_override_reason, "shrink_override_reason")
        )
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT * FROM fbref_control.registry_snapshot
                WHERE snapshot_id = %s FOR UPDATE
                """,
                (snapshot,),
            )
            evidence = _fetchone(cursor)
            if evidence is None or not evidence["successful"]:
                raise StateConflict(
                    "Only a successful registry snapshot may be reconciled"
                )
            source = str(evidence["source"])
            fetched_at = evidence["fetched_at"]
            cursor.execute(
                """
                SELECT max(last_seen_at) AS latest
                FROM fbref_control.competition_registry
                WHERE source = %s
                """,
                (source,),
            )
            latest = _fetchone(cursor)
            if latest and latest["latest"] and latest["latest"] > fetched_at:
                raise StateConflict("Registry snapshots must reconcile chronologically")

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM fbref_control.competition_registry
                WHERE source = %s AND lifecycle_state <> 'disappeared'
                """,
                (source,),
            )
            baseline_row = _fetchone(cursor) or {}
            baseline_count = int(baseline_row.get("count") or 0)
            severe_shrink = (
                baseline_count > 0
                and len(competitions) * 10 < baseline_count * 9
            )
            if severe_shrink and override_reason is None:
                raise StateConflict(
                    "Competition snapshot shrank by more than 10%: "
                    f"{baseline_count} -> {len(competitions)}"
                )
            if severe_shrink:
                cursor.execute(
                    """
                    INSERT INTO
                        fbref_control.registry_reconciliation_override (
                            snapshot_id, override_type, reason
                        ) VALUES (
                            %s, 'competition_snapshot_shrink', %s
                        )
                    ON CONFLICT (snapshot_id, override_type) DO NOTHING
                    """,
                    (snapshot, override_reason),
                )

            counts = {"active": 0, "skipped": 0, "quarantined": 0}
            seen_ids = []
            for entry in competitions:
                crawl_state = {
                    "male": "active",
                    "female": "skipped",
                    "unknown": "quarantined",
                }[entry.gender]
                counts[crawl_state] += 1
                seen_ids.append(entry.competition_id)
                cursor.execute(
                    """
                    INSERT INTO fbref_control.competition_registry (
                        source, competition_id, canonical_url, name, gender,
                        classification, calendar_type, lifecycle_state,
                        crawl_state, present, consecutive_misses,
                        first_seen_at, last_seen_at, first_snapshot_id,
                        last_snapshot_id, metadata
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, 'present', %s,
                        true, 0, %s, %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT (source, competition_id) DO UPDATE SET
                        canonical_url = EXCLUDED.canonical_url,
                        name = EXCLUDED.name,
                        gender = EXCLUDED.gender,
                        classification = EXCLUDED.classification,
                        calendar_type = EXCLUDED.calendar_type,
                        lifecycle_state = 'present',
                        crawl_state = EXCLUDED.crawl_state,
                        present = true,
                        consecutive_misses = 0,
                        last_seen_at = EXCLUDED.last_seen_at,
                        last_snapshot_id = EXCLUDED.last_snapshot_id,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        source,
                        entry.competition_id,
                        entry.canonical_url,
                        entry.name,
                        entry.gender,
                        entry.classification,
                        entry.calendar_type,
                        crawl_state,
                        fetched_at,
                        fetched_at,
                        snapshot,
                        snapshot,
                        _json(entry.metadata),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.snapshot_competition (
                        snapshot_id, source, competition_id
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (snapshot, source, entry.competition_id),
                )

            cursor.execute(
                """
                UPDATE fbref_control.competition_registry
                SET consecutive_misses = consecutive_misses + 1,
                    lifecycle_state = CASE
                        WHEN consecutive_misses + 1 >= 2 THEN 'disappeared'
                        ELSE 'missing_once'
                    END,
                    present = consecutive_misses + 1 < 2,
                    last_snapshot_id = %s
                WHERE source = %s
                  AND last_snapshot_id <> %s
                  AND NOT (competition_id = ANY(%s::text[]))
                """,
                (snapshot, source, snapshot, seen_ids),
            )
            counts["missing"] = cursor.rowcount
            counts["snapshot_baseline"] = baseline_count
            counts["snapshot_shrink_overridden"] = int(severe_shrink)
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier AS frontier
                SET state = 'queued', next_fetch_at = clock_timestamp(),
                    last_error_class = CASE
                        WHEN frontier.state = 'quarantined' THEN NULL
                        ELSE frontier.last_error_class
                    END,
                    last_error_message = CASE
                        WHEN frontier.state = 'quarantined' THEN NULL
                        ELSE frontier.last_error_message
                    END,
                    updated_at = clock_timestamp()
                FROM fbref_control.competition_registry AS competition
                WHERE competition.source = frontier.source
                  AND competition.competition_id =
                      frontier.source_ids ->> 'competition_id'
                  AND competition.gender = 'male'
                  AND competition.crawl_state = 'active'
                  AND competition.lifecycle_state IN (
                      'present', 'missing_once'
                  )
                  AND competition.present
                  AND (
                    frontier.state = 'skipped'
                    OR (
                      frontier.state = 'quarantined'
                      AND frontier.last_error_class = 'ScopeQuarantined'
                    )
                  )
                """
            )
            counts["frontier_scope_reopened"] = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier AS frontier
                SET state = CASE
                        WHEN competition.gender IN ('female', 'unknown')
                        THEN 'quarantined'
                        ELSE 'skipped'
                    END,
                    next_fetch_at = NULL, retry_after = NULL,
                    last_error_class = CASE
                        WHEN competition.gender IN ('female', 'unknown')
                        THEN 'ScopeQuarantined'
                        ELSE frontier.last_error_class
                    END,
                    last_error_message = CASE
                        WHEN competition.gender = 'female'
                        THEN 'female_gender'
                        WHEN competition.gender = 'unknown'
                        THEN 'unknown_gender'
                        ELSE frontier.last_error_message
                    END,
                    updated_at = clock_timestamp()
                FROM fbref_control.competition_registry AS competition
                WHERE competition.source = frontier.source
                  AND competition.competition_id =
                      frontier.source_ids ->> 'competition_id'
                  AND (
                    competition.gender <> 'male'
                    OR competition.crawl_state <> 'active'
                    OR competition.lifecycle_state NOT IN (
                        'present', 'missing_once'
                    )
                    OR NOT competition.present
                  )
                  AND frontier.state <> 'leased'
                """
            )
            counts["frontier_scope_closed"] = cursor.rowcount
        if unknown_ids:
            raise StateConflict(
                "Competition snapshot durably quarantined unknown gender: "
                + ", ".join(unknown_ids)
            )
        return counts

    def eligible_competitions(self, *, source: str = "fbref") -> list[dict]:
        """Return only current male rows eligible to create downstream targets."""
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT * FROM fbref_control.competition_registry
                WHERE source = %s AND gender = 'male'
                  AND crawl_state = 'active'
                  AND lifecycle_state IN ('present', 'missing_once')
                  AND present
                ORDER BY competition_id
                """,
                (_text(source, "source"),),
            )
            return _fetchall(cursor)

    def list_publication_scope(
        self, *, source: str = "fbref"
    ) -> list[dict]:
        """Export canonical and aliased season scope for Bronze/Silver gates."""

        with self._transaction() as cursor:
            cursor.execute(
                """
                WITH scope_token AS (
                    SELECT season.source, season.competition_id,
                           season.season_id AS canonical_season_id,
                           season.season_id AS source_season_id,
                           'canonical'::text AS scope_kind
                    FROM fbref_control.season_registry AS season
                    UNION ALL
                    SELECT alias.source, alias.competition_id,
                           alias.season_id AS canonical_season_id,
                           alias.alias AS source_season_id,
                           'alias'::text AS scope_kind
                    FROM fbref_control.season_alias AS alias
                    WHERE alias.alias <> alias.season_id
                )
                SELECT token.source,
                       token.competition_id AS source_competition_id,
                       token.source_season_id,
                       token.canonical_season_id,
                       token.scope_kind,
                       competition.name AS competition_name,
                       competition.gender,
                       competition.crawl_state AS competition_crawl_state,
                       competition.lifecycle_state
                           AS competition_lifecycle_state,
                       competition.present AS competition_present,
                       season.label AS season_label,
                       season.is_current AS season_is_current,
                       season.lifecycle_state AS season_lifecycle_state,
                       season.present AS season_present,
                       COALESCE(
                           season.metadata ->> 'direct_match_only' = 'true',
                           false
                       ) AS direct_match_only,
                       (
                           competition.gender = 'male'
                           AND competition.crawl_state = 'active'
                           AND competition.lifecycle_state IN (
                               'present', 'missing_once'
                           )
                           AND competition.present
                           AND season.lifecycle_state = 'present'
                           AND season.present
                       ) AS eligible_male
                FROM scope_token AS token
                JOIN fbref_control.competition_registry AS competition
                  ON competition.source = token.source
                 AND competition.competition_id = token.competition_id
                JOIN fbref_control.season_registry AS season
                  ON season.source = token.source
                 AND season.competition_id = token.competition_id
                 AND season.season_id = token.canonical_season_id
                WHERE token.source = %s
                ORDER BY token.competition_id, token.source_season_id,
                         token.scope_kind
                """,
                (_text(source, "source"),),
            )
            return _fetchall(cursor)

    @staticmethod
    def _validated_seasons(
        competition_id: str,
        entries: Iterable[SeasonRegistryEntry],
    ) -> list[SeasonRegistryEntry]:
        result = []
        ids = set()
        urls = set()
        for entry in entries:
            entry_competition = _text(entry.competition_id, "competition_id")
            if entry_competition != competition_id:
                raise ValueError("Season belongs to a different competition")
            season_id = _text(entry.season_id, "season_id")
            canonical_url = _text(entry.canonical_url, "canonical_url")
            if season_id in ids or canonical_url in urls:
                raise ValueError("Season snapshot contains duplicate IDs or URLs")
            ids.add(season_id)
            urls.add(canonical_url)
            result.append(
                SeasonRegistryEntry(
                    competition_id=competition_id,
                    season_id=season_id,
                    canonical_url=canonical_url,
                    label=(
                        None if entry.label is None else _text(entry.label, "label")
                    ),
                    is_current=bool(entry.is_current),
                    metadata=dict(entry.metadata),
                )
            )
        current_ids = [entry.season_id for entry in result if entry.is_current]
        if len(current_ids) > 1:
            raise ValueError(
                "Season snapshot contains more than one current season: "
                + ", ".join(sorted(current_ids))
            )
        return result

    def reconcile_seasons(
        self,
        snapshot_id: object,
        competition_id: object,
        entries: Iterable[SeasonRegistryEntry],
    ) -> dict[str, int]:
        """Reconcile source-native seasons for one eligible male competition."""
        snapshot = _uuid(snapshot_id, "snapshot_id")
        competition = _text(competition_id, "competition_id")
        seasons = self._validated_seasons(competition, entries)
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT * FROM fbref_control.registry_snapshot
                WHERE snapshot_id = %s FOR UPDATE
                """,
                (snapshot,),
            )
            evidence = _fetchone(cursor)
            if evidence is None or not evidence["successful"]:
                raise StateConflict(
                    "Only a successful registry snapshot may be reconciled"
                )
            source = str(evidence["source"])
            fetched_at = evidence["fetched_at"]
            cursor.execute(
                """
                SELECT crawl_state, lifecycle_state, present
                FROM fbref_control.competition_registry
                WHERE source = %s AND competition_id = %s
                FOR UPDATE
                """,
                (source, competition),
            )
            parent = _fetchone(cursor)
            if (
                parent is None
                or parent["crawl_state"] != "active"
                or parent["lifecycle_state"] not in {
                    "present",
                    "missing_once",
                }
                or not parent["present"]
            ):
                raise StateConflict(
                    f"Competition {competition} is not eligible for season targets"
                )
            cursor.execute(
                """
                SELECT max(last_seen_at) AS latest
                FROM fbref_control.season_registry
                WHERE source = %s AND competition_id = %s
                """,
                (source, competition),
            )
            latest = _fetchone(cursor)
            if latest and latest["latest"] and latest["latest"] > fetched_at:
                raise StateConflict("Season snapshots must reconcile chronologically")

            seen_ids = []
            current_count = 0
            incoming_current = next(
                (entry.season_id for entry in seasons if entry.is_current),
                None,
            )
            if incoming_current is not None:
                # Avoid a transient violation of the v8 partial unique index
                # while the newly published current season is upserted.
                cursor.execute(
                    """
                    UPDATE fbref_control.season_registry
                    SET is_current = false
                    WHERE source = %s AND competition_id = %s
                      AND is_current AND season_id <> %s
                    """,
                    (source, competition, incoming_current),
                )
            for entry in seasons:
                seen_ids.append(entry.season_id)
                current_count += int(entry.is_current)
                cursor.execute(
                    """
                    INSERT INTO fbref_control.season_registry (
                        source, competition_id, season_id, canonical_url,
                        label, is_current, lifecycle_state, present,
                        consecutive_misses, first_seen_at, last_seen_at,
                        first_snapshot_id, last_snapshot_id, metadata
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, 'present', true, 0,
                        %s, %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT (source, competition_id, season_id)
                    DO UPDATE SET
                        canonical_url = EXCLUDED.canonical_url,
                        label = EXCLUDED.label,
                        is_current = EXCLUDED.is_current,
                        lifecycle_state = 'present',
                        present = true,
                        consecutive_misses = 0,
                        last_seen_at = EXCLUDED.last_seen_at,
                        last_snapshot_id = EXCLUDED.last_snapshot_id,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        source,
                        competition,
                        entry.season_id,
                        entry.canonical_url,
                        entry.label,
                        entry.is_current,
                        fetched_at,
                        fetched_at,
                        snapshot,
                        snapshot,
                        _json(entry.metadata),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.snapshot_season (
                        snapshot_id, source, competition_id, season_id
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (snapshot, source, competition, entry.season_id),
                )

            cursor.execute(
                """
                UPDATE fbref_control.season_registry
                SET consecutive_misses = consecutive_misses + 1,
                    lifecycle_state = CASE
                        WHEN consecutive_misses + 1 >= 2 THEN 'disappeared'
                        ELSE 'missing_once'
                    END,
                    present = false,
                    is_current = false,
                    last_snapshot_id = %s
                WHERE source = %s AND competition_id = %s
                  AND last_snapshot_id <> %s
                  AND NOT (season_id = ANY(%s::text[]))
                """,
                (snapshot, source, competition, snapshot, seen_ids),
            )
            missing_count = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier AS frontier
                SET state = 'queued', next_fetch_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                FROM fbref_control.season_registry AS season
                WHERE season.source = frontier.source
                  AND season.competition_id =
                      frontier.source_ids ->> 'competition_id'
                  AND season.season_id =
                      frontier.source_ids ->> 'season_id'
                  AND season.lifecycle_state = 'present'
                  AND season.present AND season.is_current
                  AND frontier.state = 'skipped'
                  AND frontier.refresh_policy <> 'historical_once'
                """
            )
            reopened_count = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier AS frontier
                SET state = 'skipped', next_fetch_at = NULL,
                    retry_after = NULL, updated_at = clock_timestamp()
                FROM fbref_control.season_registry AS season
                WHERE season.source = frontier.source
                  AND season.competition_id =
                      frontier.source_ids ->> 'competition_id'
                  AND season.season_id =
                      frontier.source_ids ->> 'season_id'
                  AND (
                    season.lifecycle_state <> 'present'
                    OR NOT season.present
                    OR (
                      NOT season.is_current
                      AND frontier.refresh_policy <> 'historical_once'
                    )
                  )
                  AND frontier.state <> 'leased'
                """
            )
            return {
                "present": len(seasons),
                "current": current_count,
                "missing": missing_count,
                "frontier_scope_reopened": reopened_count,
                "frontier_scope_closed": cursor.rowcount,
            }

    def upsert_season_alias(
        self,
        alias: SeasonAlias,
        *,
        snapshot_id: Optional[object] = None,
    ) -> None:
        """Map one durable source/legacy token to a canonical season."""
        source = _text(alias.source, "source")
        competition = _text(alias.competition_id, "competition_id")
        alias_value = _text(alias.alias, "alias")
        season = _text(alias.season_id, "season_id")
        alias_kind = _text(alias.alias_kind, "alias_kind").lower()
        if alias_kind not in _SEASON_ALIAS_KINDS:
            raise ValueError(f"Unsupported season alias kind: {alias_kind}")
        snapshot = (
            None
            if snapshot_id is None
            else _uuid(snapshot_id, "snapshot_id")
        )
        with self._transaction() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.season_alias (
                    source, competition_id, alias, season_id, alias_kind,
                    first_snapshot_id, last_snapshot_id, metadata
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (source, competition_id, alias) DO UPDATE SET
                    alias_kind = EXCLUDED.alias_kind,
                    last_snapshot_id = COALESCE(
                        EXCLUDED.last_snapshot_id,
                        fbref_control.season_alias.last_snapshot_id
                    ),
                    metadata = EXCLUDED.metadata,
                    last_seen_at = clock_timestamp()
                WHERE fbref_control.season_alias.season_id = EXCLUDED.season_id
                RETURNING season_id
                """,
                (
                    source,
                    competition,
                    alias_value,
                    season,
                    alias_kind,
                    snapshot,
                    snapshot,
                    _json(alias.metadata),
                ),
            )
            row = _fetchone(cursor)
            if row is None or str(row["season_id"]) != season:
                raise StateConflict(
                    f"Season alias {competition}/{alias_value} is already "
                    "mapped to a different season"
                )

    def resolve_season_alias(
        self,
        competition_id: object,
        alias: object,
        *,
        source: str = "fbref",
    ) -> Optional[dict]:
        """Resolve an alias only through a currently published male registry."""
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT mapped.source, mapped.competition_id, mapped.alias,
                       mapped.alias_kind, mapped.season_id,
                       season.canonical_url, season.label, season.is_current,
                       mapped.metadata
                FROM fbref_control.season_alias AS mapped
                JOIN fbref_control.season_registry AS season
                  ON season.source = mapped.source
                 AND season.competition_id = mapped.competition_id
                 AND season.season_id = mapped.season_id
                JOIN fbref_control.competition_registry AS competition
                  ON competition.source = season.source
                 AND competition.competition_id = season.competition_id
                WHERE mapped.source = %s AND mapped.competition_id = %s
                  AND mapped.alias = %s
                  AND season.present
                  AND season.lifecycle_state = 'present'
                  AND competition.gender = 'male'
                  AND competition.crawl_state = 'active'
                  AND competition.present
                  AND competition.lifecycle_state IN (
                      'present', 'missing_once'
                  )
                """,
                (
                    _text(source, "source"),
                    _text(competition_id, "competition_id"),
                    _text(alias, "alias"),
                ),
            )
            return _fetchone(cursor)

    def list_seasons(
        self,
        *,
        current: Optional[bool] = None,
        source: str = "fbref",
        limit: int = 25,
        after: Optional[tuple[str, str]] = None,
    ) -> list[dict]:
        """Page source-native seasons for eligible, currently published males."""
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 25:
            raise ValueError("limit must be between 1 and 25")
        if after is None:
            after_competition = None
            after_season = None
        else:
            if len(after) != 2:
                raise ValueError("after must be (competition_id, season_id)")
            after_competition = _text(after[0], "after competition_id")
            after_season = _text(after[1], "after season_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT season.source, season.competition_id, season.season_id,
                       season.canonical_url, season.label, season.is_current,
                       season.metadata, competition.name AS competition_name,
                       competition.classification,
                       competition.calendar_type,
                       competition.canonical_url AS competition_url
                FROM fbref_control.season_registry AS season
                JOIN fbref_control.competition_registry AS competition
                  ON competition.source = season.source
                 AND competition.competition_id = season.competition_id
                WHERE season.source = %s
                  AND season.lifecycle_state = 'present' AND season.present
                  AND competition.gender = 'male'
                  AND competition.crawl_state = 'active'
                  AND competition.lifecycle_state IN (
                      'present', 'missing_once'
                  )
                  AND competition.present
                  AND (%s::boolean IS NULL OR season.is_current = %s)
                  AND (
                      %s::text IS NULL
                      OR (season.competition_id, season.season_id)
                         > (%s, %s)
                  )
                ORDER BY season.competition_id, season.season_id
                LIMIT %s
                """,
                (
                    _text(source, "source"),
                    current,
                    current,
                    after_competition,
                    after_competition,
                    after_season,
                    normalized_limit,
                ),
            )
            return _fetchall(cursor)

    def list_backfill_seasons(
        self,
        *,
        source: str = "fbref",
        limit: int = 25,
    ) -> list[dict]:
        """Return a bounded auto-resume cohort of non-current seasons.

        A completed ``historical_once`` target is deliberately absent from
        subsequent runs, while missing, queued, due-retry, and formerly
        current season targets remain eligible.  This keeps the selection
        bounded in PostgreSQL and removes the operator-managed registry
        cursor which previously requeued the first page on every DAG run.
        """

        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 25:
            raise ValueError("limit must be between 1 and 25")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT season.source, season.competition_id, season.season_id,
                       season.canonical_url, season.label, season.is_current,
                       season.metadata, competition.name AS competition_name,
                       competition.classification,
                       competition.calendar_type,
                       competition.canonical_url AS competition_url
                FROM fbref_control.season_registry AS season
                JOIN fbref_control.competition_registry AS competition
                  ON competition.source = season.source
                 AND competition.competition_id = season.competition_id
                LEFT JOIN fbref_control.page_frontier AS frontier
                  ON frontier.source = season.source
                 AND frontier.page_kind = 'season'
                 AND frontier.source_ids ->> 'competition_id' =
                     season.competition_id
                 AND frontier.source_ids ->> 'season_id' = season.season_id
                WHERE season.source = %s
                  AND season.lifecycle_state = 'present' AND season.present
                  AND NOT season.is_current
                  AND season.metadata ->> 'direct_match_only'
                      IS DISTINCT FROM 'true'
                  AND competition.gender = 'male'
                  AND competition.crawl_state = 'active'
                  AND competition.lifecycle_state IN (
                      'present', 'missing_once'
                  )
                  AND competition.present
                  AND (
                      frontier.target_id IS NULL
                      OR (
                          frontier.state IN ('queued', 'retry', 'fetched')
                          AND (
                              frontier.state <> 'retry'
                              OR frontier.retry_after IS NULL
                              OR frontier.retry_after <= clock_timestamp()
                          )
                          AND NOT (
                              frontier.refresh_policy = 'historical_once'
                              AND frontier.state = 'fetched'
                              AND frontier.next_fetch_at IS NULL
                          )
                      )
                  )
                ORDER BY season.competition_id, season.season_id
                LIMIT %s
                """,
                (_text(source, "source"), normalized_limit),
            )
            return _fetchall(cursor)

    def upsert_frontier_target(
        self,
        target: FrontierTarget,
        *,
        _cursor: Any = None,
    ) -> None:
        """Create/update one canonical identity before any network request."""
        target_id = _text(target.target_id, "target_id")
        canonical_url = _text(target.canonical_url, "canonical_url")
        if urlsplit(canonical_url).scheme not in {"http", "https"}:
            raise ValueError("canonical_url must be absolute HTTP(S)")
        with self._transaction(_cursor) as cursor:
            cursor.execute(
                """
                SELECT target_id, source, page_kind, canonical_url,
                       source_ids, state
                FROM fbref_control.page_frontier
                WHERE target_id = %s OR canonical_url = %s
                ORDER BY target_id, canonical_url
                FOR UPDATE
                """,
                (target_id, canonical_url),
            )
            rows = _fetchall(cursor)
            for row in rows:
                if row["target_id"] != target_id:
                    if str(row["state"]) == "quarantined":
                        # A quarantined, mis-classified target still holds this
                        # canonical URL (e.g. pre-#949 discovery minted the
                        # /stats/ player-standard page as a season target with
                        # season_id='stats').  Release the URL onto a dead
                        # sentinel so the correctly classified target can claim
                        # it; the quarantined row and its append-only provenance
                        # are preserved for audit.  This self-heals the mint on
                        # the next discovery pass, in acceptance and in prod.
                        cursor.execute(
                            """
                            UPDATE fbref_control.page_frontier
                            SET canonical_url =
                                    canonical_url || '#superseded:' || %s,
                                updated_at = clock_timestamp()
                            WHERE target_id = %s
                            """,
                            (target_id, row["target_id"]),
                        )
                        continue
                    raise StateConflict(
                        f"Canonical URL already belongs to {row['target_id']}"
                    )
                if row["state"] == "leased":
                    installed_source_ids = row["source_ids"]
                    if isinstance(installed_source_ids, str):
                        installed_source_ids = json.loads(installed_source_ids)
                    if (
                        row["canonical_url"] != canonical_url
                        or row["source"] != _text(target.source, "source")
                        or row["page_kind"] != _text(target.page_kind, "page_kind")
                        or dict(installed_source_ids) != dict(target.source_ids)
                    ):
                        raise StateConflict(
                            "Cannot change target identity under an active lease"
                        )
            cursor.execute(
                """
                INSERT INTO fbref_control.page_frontier (
                    target_id, source, page_kind, canonical_url, source_ids,
                    refresh_policy, priority, next_fetch_at
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                ON CONFLICT (target_id) DO UPDATE SET
                    source = EXCLUDED.source,
                    page_kind = EXCLUDED.page_kind,
                    canonical_url = EXCLUDED.canonical_url,
                    source_ids = EXCLUDED.source_ids,
                    refresh_policy = CASE
                        WHEN fbref_control.page_frontier.page_kind IN (
                                 'player', 'squad'
                             )
                         AND fbref_control.page_frontier.refresh_policy NOT IN (
                                 'historical_once', 'current_completed_once'
                             )
                         AND EXCLUDED.refresh_policy = 'historical_once'
                        THEN fbref_control.page_frontier.refresh_policy
                        ELSE EXCLUDED.refresh_policy
                    END,
                    priority = CASE
                        WHEN fbref_control.page_frontier.page_kind IN (
                                 'player', 'squad'
                             )
                         AND fbref_control.page_frontier.refresh_policy NOT IN (
                                 'historical_once', 'current_completed_once'
                             )
                         AND EXCLUDED.refresh_policy = 'historical_once'
                        THEN fbref_control.page_frontier.priority
                        ELSE EXCLUDED.priority
                    END,
                    next_fetch_at = CASE
                        WHEN fbref_control.page_frontier.refresh_policy IN (
                                 'historical_once', 'current_completed_once'
                             )
                         AND EXCLUDED.refresh_policy NOT IN (
                                 'historical_once', 'current_completed_once'
                             )
                         AND fbref_control.page_frontier.next_fetch_at IS NULL
                        THEN clock_timestamp()
                        WHEN fbref_control.page_frontier.page_kind IN (
                                 'player', 'squad'
                             )
                         AND fbref_control.page_frontier.refresh_policy NOT IN (
                                 'historical_once', 'current_completed_once'
                             )
                         AND EXCLUDED.refresh_policy = 'historical_once'
                        THEN fbref_control.page_frontier.next_fetch_at
                        WHEN (
                          fbref_control.page_frontier.page_kind = 'match'
                          AND EXCLUDED.refresh_policy = 'current_completed_once'
                          AND fbref_control.page_frontier.refresh_policy NOT IN (
                              'historical_once', 'current_completed_once'
                          )
                        ) OR (
                          fbref_control.page_frontier.page_kind = 'season'
                          AND EXCLUDED.refresh_policy = 'historical_once'
                          AND fbref_control.page_frontier.refresh_policy NOT IN (
                              'historical_once', 'current_completed_once'
                          )
                        )
                        THEN clock_timestamp()
                        ELSE COALESCE(
                            EXCLUDED.next_fetch_at,
                            fbref_control.page_frontier.next_fetch_at
                        )
                    END,
                    updated_at = clock_timestamp()
                """,
                (
                    target_id,
                    _text(target.source, "source"),
                    _text(target.page_kind, "page_kind"),
                    canonical_url,
                    _json(target.source_ids),
                    _text(target.refresh_policy, "refresh_policy"),
                    int(target.priority),
                    target.next_fetch_at,
                ),
            )

    def record_frontier_provenance(
        self,
        provenance: FrontierProvenance,
        *,
        _cursor: Any = None,
    ) -> str:
        """Append one idempotent, content-addressed discovery edge."""
        parent = _text(provenance.parent_target_id, "parent_target_id")
        child = _text(provenance.child_target_id, "child_target_id")
        if parent == child:
            raise ValueError("A frontier provenance edge cannot point to itself")
        relation = _text(provenance.relation, "relation")
        content_hash = _text(
            provenance.parent_content_hash, "parent_content_hash"
        )
        parser_version = _text(provenance.parser_version, "parser_version")
        competition = (
            None
            if provenance.carried_competition_id is None
            else _text(
                provenance.carried_competition_id,
                "carried_competition_id",
            )
        )
        season = (
            None
            if provenance.carried_season_id is None
            else _text(provenance.carried_season_id, "carried_season_id")
        )
        if season is not None and competition is None:
            raise ValueError("A carried season requires a carried competition")
        refresh = (
            None
            if provenance.logical_refresh_id is None
            else _uuid(provenance.logical_refresh_id, "logical_refresh_id")
        )
        provenance_id = make_frontier_provenance_id(
            parent_target_id=parent,
            child_target_id=child,
            relation=relation,
            carried_competition_id=competition,
            carried_season_id=season,
            parent_content_hash=content_hash,
            parser_version=parser_version,
        )
        expected_metadata = dict(provenance.metadata)
        with self._transaction(_cursor) as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.frontier_provenance (
                    provenance_id, parent_target_id, child_target_id,
                    relation, carried_competition_id, carried_season_id,
                    parent_content_hash, parser_version,
                    logical_refresh_id, metadata
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (
                    parent_target_id, child_target_id, relation,
                    carried_competition_id, carried_season_id,
                    parent_content_hash, parser_version
                ) DO NOTHING
                """,
                (
                    provenance_id,
                    parent,
                    child,
                    relation,
                    competition,
                    season,
                    content_hash,
                    parser_version,
                    refresh,
                    _json(expected_metadata),
                ),
            )
            cursor.execute(
                """
                SELECT provenance_id, carried_competition_id,
                       carried_season_id, logical_refresh_id, metadata
                FROM fbref_control.frontier_provenance
                WHERE parent_target_id = %s AND child_target_id = %s
                  AND relation = %s
                  AND carried_competition_id IS NOT DISTINCT FROM %s
                  AND carried_season_id IS NOT DISTINCT FROM %s
                  AND parent_content_hash = %s
                  AND parser_version = %s
                """,
                (
                    parent,
                    child,
                    relation,
                    competition,
                    season,
                    content_hash,
                    parser_version,
                ),
            )
            row = _fetchone(cursor)
            if row is None:
                raise ControlStoreError("Frontier provenance insert returned no row")
            installed_metadata = row.get("metadata") or {}
            if isinstance(installed_metadata, str):
                installed_metadata = json.loads(installed_metadata)
            if (
                row.get("carried_competition_id") != competition
                or row.get("carried_season_id") != season
                or dict(installed_metadata) != expected_metadata
            ):
                raise StateConflict(
                    f"Frontier provenance {parent} -> {child} already has "
                    "different immutable evidence"
                )
            return str(row["provenance_id"])

    def upsert_frontier_discovery_batch(
        self,
        *,
        targets: Sequence[FrontierTarget],
        provenance: Sequence[FrontierProvenance],
    ) -> dict[str, int]:
        """Atomically persist one bounded observation's discovery output."""
        raw_targets = list(targets)
        raw_provenance = list(provenance)
        if len(raw_targets) > _MAX_FRONTIER_DISCOVERY_TARGETS:
            raise ValueError(
                "frontier discovery target batch exceeds "
                f"{_MAX_FRONTIER_DISCOVERY_TARGETS}"
            )
        if len(raw_provenance) > _MAX_FRONTIER_DISCOVERY_EDGES:
            raise ValueError(
                "frontier discovery provenance batch exceeds "
                f"{_MAX_FRONTIER_DISCOVERY_EDGES}"
            )
        ordered_targets = sorted(
            raw_targets,
            key=lambda target: (
                _text(target.target_id, "target_id"),
                _text(target.canonical_url, "canonical_url"),
            ),
        )
        ordered_provenance = sorted(
            raw_provenance,
            key=lambda edge: make_frontier_provenance_id(
                parent_target_id=edge.parent_target_id,
                child_target_id=edge.child_target_id,
                relation=edge.relation,
                carried_competition_id=edge.carried_competition_id,
                carried_season_id=edge.carried_season_id,
                parent_content_hash=edge.parent_content_hash,
                parser_version=edge.parser_version,
            ),
        )
        observation_keys = {
            (
                _text(edge.parent_target_id, "parent_target_id"),
                _text(edge.parent_content_hash, "parent_content_hash"),
                _text(edge.parser_version, "parser_version"),
                None
                if edge.logical_refresh_id is None
                else _uuid(edge.logical_refresh_id, "logical_refresh_id"),
            )
            for edge in ordered_provenance
        }
        if len(observation_keys) > 1:
            raise ValueError(
                "frontier discovery batch must belong to one observation"
            )
        if not ordered_targets and not ordered_provenance:
            return {"target_count": 0, "provenance_count": 0}

        with self._transaction() as cursor:
            for target in ordered_targets:
                self.upsert_frontier_target(target, _cursor=cursor)
            for edge in ordered_provenance:
                self.record_frontier_provenance(edge, _cursor=cursor)
        return {
            "target_count": len(ordered_targets),
            "provenance_count": len(ordered_provenance),
        }

    def list_frontier_provenance(
        self,
        *,
        parent_target_id: Optional[object] = None,
        child_target_id: Optional[object] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Query bounded discovery ancestry in deterministic oldest-first order."""
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        parent = (
            None
            if parent_target_id is None
            else _text(parent_target_id, "parent_target_id")
        )
        child = (
            None
            if child_target_id is None
            else _text(child_target_id, "child_target_id")
        )
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT provenance_id, parent_target_id, child_target_id,
                       relation, carried_competition_id, carried_season_id,
                       parent_content_hash, parser_version,
                       logical_refresh_id, metadata, discovered_at
                FROM fbref_control.frontier_provenance
                WHERE (%s::text IS NULL OR parent_target_id = %s)
                  AND (%s::text IS NULL OR child_target_id = %s)
                ORDER BY discovered_at, provenance_id
                LIMIT %s
                """,
                (parent, parent, child, child, normalized_limit),
            )
            return _fetchall(cursor)

    def list_male_eligible_frontier_targets(
        self,
        *,
        source: str = "fbref",
        page_kinds: Optional[Sequence[str]] = None,
        after_target_id: Optional[object] = None,
        limit: int = 25,
    ) -> list[dict]:
        """Return targets whose every carried scope resolves to an active male."""
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        kinds = (
            None
            if page_kinds is None
            else sorted({_text(kind, "page_kind") for kind in page_kinds})
        )
        if kinds == []:
            return []
        after = (
            None
            if after_target_id is None
            else _text(after_target_id, "after_target_id")
        )
        with self._transaction() as cursor:
            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                SELECT frontier.*, scope.scope_count
                FROM fbref_control.page_frontier AS frontier
                JOIN scope_rollup AS scope ON scope.target_id = frontier.target_id
                WHERE frontier.source = %s
                  AND frontier.state IN ('queued', 'retry', 'fetched')
                  AND scope.scope_count > 0
                  AND NOT COALESCE(scope.competition_missing, true)
                  AND NOT COALESCE(scope.has_female, false)
                  AND NOT COALESCE(scope.has_unknown, true)
                  AND NOT COALESCE(scope.inactive_competition, true)
                  AND NOT COALESCE(scope.invalid_season, true)
                  AND (
                    COALESCE(scope.has_competition_scope, false)
                    OR COALESCE(scope.has_current_season, false)
                    OR frontier.refresh_policy = 'historical_once'
                  )
                  AND (%s::text[] IS NULL OR frontier.page_kind = ANY(%s))
                  AND (%s::text IS NULL OR frontier.target_id > %s)
                ORDER BY frontier.target_id
                LIMIT %s
                """,
                (
                    _text(source, "source"),
                    kinds,
                    kinds,
                    after,
                    after,
                    normalized_limit,
                ),
            )
            return _fetchall(cursor)

    def reconcile_frontier_scope(
        self,
        *,
        source: str = "fbref",
    ) -> dict[str, int]:
        """Fail closed on scope and reopen only our own resolved quarantine.

        This reconciliation changes frontier scheduling state only.  Immutable
        raw fetches, manifests, and Bronze datasets remain available for audit
        and parser replay.
        """
        normalized_source = _text(source, "source")
        with self._transaction() as cursor:
            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                , classified AS (
                    SELECT frontier.target_id,
                           CASE
                             WHEN scope.target_id IS NULL
                               THEN 'unresolved_scope'
                             WHEN COALESCE(scope.competition_missing, true)
                               THEN 'missing_competition'
                             WHEN COALESCE(scope.has_unknown, false)
                               THEN 'unknown_gender'
                             WHEN COALESCE(scope.has_female, false)
                               THEN 'female_gender'
                             WHEN COALESCE(scope.inactive_competition, true)
                               THEN 'inactive_competition'
                             WHEN COALESCE(scope.invalid_season, false)
                               THEN 'invalid_season'
                             WHEN NOT (
                               COALESCE(
                                 scope.has_competition_scope, false
                               )
                               OR COALESCE(scope.has_current_season, false)
                               OR frontier.refresh_policy = 'historical_once'
                             ) THEN 'noncurrent_season'
                             ELSE NULL
                           END AS reason
                    FROM fbref_control.page_frontier AS frontier
                    LEFT JOIN scope_rollup AS scope
                      ON scope.target_id = frontier.target_id
                    WHERE frontier.source = %s
                      AND frontier.page_kind <> 'competition_index'
                )
                UPDATE fbref_control.page_frontier AS frontier
                SET state = 'queued', next_fetch_at = clock_timestamp(),
                    retry_after = NULL, last_error_class = NULL,
                    last_error_message = NULL,
                    updated_at = clock_timestamp()
                FROM classified
                WHERE classified.target_id = frontier.target_id
                  AND classified.reason IS NULL
                  AND frontier.state = 'quarantined'
                  AND frontier.last_error_class = 'ScopeQuarantined'
                RETURNING frontier.target_id
                """,
                (normalized_source,),
            )
            reopened = len(_fetchall(cursor))

            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                , classified AS (
                    SELECT frontier.target_id,
                           CASE
                             WHEN scope.target_id IS NULL
                               THEN 'unresolved_scope'
                             WHEN COALESCE(scope.competition_missing, true)
                               THEN 'missing_competition'
                             WHEN COALESCE(scope.has_unknown, false)
                               THEN 'unknown_gender'
                             WHEN COALESCE(scope.has_female, false)
                               THEN 'female_gender'
                             WHEN COALESCE(scope.inactive_competition, true)
                               THEN 'inactive_competition'
                             WHEN COALESCE(scope.invalid_season, false)
                               THEN 'invalid_season'
                             WHEN NOT (
                               COALESCE(
                                 scope.has_competition_scope, false
                               )
                               OR COALESCE(scope.has_current_season, false)
                               OR frontier.refresh_policy = 'historical_once'
                             ) THEN 'noncurrent_season'
                             ELSE NULL
                           END AS reason
                    FROM fbref_control.page_frontier AS frontier
                    LEFT JOIN scope_rollup AS scope
                      ON scope.target_id = frontier.target_id
                    WHERE frontier.source = %s
                      AND frontier.page_kind <> 'competition_index'
                )
                UPDATE fbref_control.page_frontier AS frontier
                SET state = 'quarantined', next_fetch_at = NULL,
                    retry_after = NULL,
                    last_error_class = 'ScopeQuarantined',
                    last_error_message = classified.reason,
                    updated_at = clock_timestamp()
                FROM classified
                WHERE classified.target_id = frontier.target_id
                  AND classified.reason IS NOT NULL
                  AND frontier.state NOT IN ('leased', 'dead')
                  AND (
                    frontier.state <> 'quarantined'
                    OR (
                      frontier.last_error_class = 'ScopeQuarantined'
                      AND frontier.last_error_message IS DISTINCT FROM
                          classified.reason
                    )
                  )
                RETURNING classified.reason
                """,
                (normalized_source,),
            )
            counts: dict[str, int] = {"reopened": reopened, "quarantined": 0}
            for row in _fetchall(cursor):
                reason = str(row["reason"])
                counts[reason] = counts.get(reason, 0) + 1
                counts["quarantined"] += 1
            counts["total"] = counts["quarantined"]
            return counts

    def quarantine_ineligible_frontier_targets(
        self,
        *,
        source: str = "fbref",
    ) -> dict[str, int]:
        """Compatibility alias for full frontier scope reconciliation."""
        return self.reconcile_frontier_scope(source=source)

    def list_acceptance_candidates(
        self,
        *,
        scope: str,
        limit: int = 1000,
    ) -> list[dict]:
        """List deterministic, evidence-backed candidates for manual sampling.

        The query never invents scope or an evidence class.  Player and match
        classifications exist only when the newest successful raw content has
        complete successful generic/typed manifests; ambiguous or conflicting
        evidence remains ``NULL`` and therefore cannot fill a strict slot.
        """

        normalized_scope = str(scope).strip().casefold()
        if normalized_scope not in {"current", "history"}:
            raise ValueError("acceptance scope must be current or history")
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        from scrapers.fbref.page_document import PAGE_DOCUMENT_VERSION
        from scrapers.fbref.typed_bronze import TYPED_BRONZE_PARSER_VERSION

        match_datasets = [
            "typed:shot_events",
            "typed:match_events",
            "typed:lineups",
            "typed:match_team_stats",
            "typed:match_managers",
            "typed:match_officials",
            "typed:match_keeper_stats",
            "typed:match_player_stats",
        ]
        with self._transaction() as cursor:
            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                , latest_success AS MATERIALIZED (
                    SELECT DISTINCT ON (attempt.target_id)
                           attempt.target_id, attempt.content_hash
                    FROM fbref_control.fetch_attempt AS attempt
                    WHERE attempt.status = 'succeeded'
                      AND attempt.content_hash IS NOT NULL
                      AND attempt.raw_manifest_key IS NOT NULL
                    ORDER BY attempt.target_id, attempt.finished_at DESC,
                             attempt.attempt_number DESC, attempt.attempt_id DESC
                ), page_evidence AS MATERIALIZED (
                    SELECT latest.target_id,
                           bool_and(
                             manifest.availability = 'available'
                             AND manifest.row_count > 0
                           ) AS all_populated,
                           bool_and(
                             manifest.availability = 'empty'
                             AND manifest.row_count = 0
                             AND NULLIF(trim(manifest.error_message), '')
                                 IS NOT NULL
                           ) AS all_empty
                    FROM latest_success AS latest
                    JOIN fbref_control.dataset_manifest AS manifest
                      ON manifest.target_id = latest.target_id
                     AND manifest.content_hash = latest.content_hash
                     AND manifest.dataset = '__page__'
                    WHERE manifest.parse_status = 'succeeded'
                      AND manifest.persistence_status = 'succeeded'
                      AND manifest.validation_status = 'succeeded'
                      AND manifest.parser_version = %s
                    GROUP BY latest.target_id
                ), match_evidence AS MATERIALIZED (
                    SELECT latest.target_id,
                           count(DISTINCT manifest.dataset) FILTER (
                             WHERE manifest.dataset = ANY(%s::text[])
                           ) AS dataset_count,
                           bool_and(
                             manifest.parse_status = 'succeeded'
                             AND manifest.persistence_status IN (
                               'succeeded', 'skipped'
                             )
                             AND manifest.validation_status IN (
                               'succeeded', 'skipped'
                             )
                             AND manifest.availability NOT IN ('unknown', 'error')
                             AND (
                               manifest.availability NOT IN (
                                 'empty', 'restricted', 'not_applicable'
                               )
                               OR NULLIF(trim(manifest.error_message), '')
                                  IS NOT NULL
                             )
                           ) FILTER (
                             WHERE manifest.dataset = ANY(%s::text[])
                           ) AS all_safe,
                           bool_or(
                             manifest.dataset = 'typed:__complete__'
                             AND manifest.parse_status = 'succeeded'
                             AND manifest.persistence_status = 'succeeded'
                             AND manifest.validation_status = 'succeeded'
                           ) AS completed,
                           bool_and(
                             manifest.availability = 'available'
                             AND manifest.row_count > 0
                           ) FILTER (
                             WHERE manifest.dataset = 'typed:match_player_stats'
                           ) AS player_stats_populated,
                           bool_and(
                             manifest.availability IN (
                               'empty', 'restricted', 'not_applicable'
                             )
                             AND manifest.row_count = 0
                             AND NULLIF(trim(manifest.error_message), '')
                                 IS NOT NULL
                           ) FILTER (
                             WHERE manifest.dataset = 'typed:match_player_stats'
                           ) AS player_stats_sparse
                    FROM latest_success AS latest
                    JOIN fbref_control.dataset_manifest AS manifest
                      ON manifest.target_id = latest.target_id
                     AND manifest.content_hash = latest.content_hash
                    WHERE (
                      manifest.dataset = 'typed:__complete__'
                      OR manifest.dataset = ANY(%s::text[])
                    )
                      AND manifest.parser_version = %s
                    GROUP BY latest.target_id
                ), eligible AS MATERIALIZED (
                    SELECT frontier.target_id, frontier.page_kind,
                           frontier.canonical_url, frontier.source_ids,
                           frontier.refresh_policy, frontier.state,
                           selected.gender, selected.competition_id,
                           selected.season_id, selected.is_current,
                           CASE
                             WHEN frontier.page_kind = 'player'
                              AND page_evidence.all_populated
                               THEN 'populated_player'
                             WHEN frontier.page_kind = 'player'
                              AND page_evidence.all_empty
                               THEN 'empty_player'
                             WHEN frontier.page_kind = 'match'
                              AND match_evidence.dataset_count = 8
                              AND match_evidence.all_safe
                              AND match_evidence.completed
                              AND match_evidence.player_stats_populated
                               THEN 'full_match'
                             WHEN frontier.page_kind = 'match'
                              AND match_evidence.dataset_count = 8
                              AND match_evidence.all_safe
                              AND match_evidence.completed
                              AND match_evidence.player_stats_sparse
                               THEN 'sparse_match'
                             ELSE NULL
                           END AS evidence_class,
                           row_number() OVER (
                             PARTITION BY
                               selected.competition_id,
                               selected.season_id,
                               frontier.page_kind,
                               COALESCE(frontier.source_ids ->> 'stat_route', ''),
                               CASE
                                 WHEN frontier.page_kind = 'player'
                                  AND page_evidence.all_populated
                                   THEN 'populated_player'
                                 WHEN frontier.page_kind = 'player'
                                  AND page_evidence.all_empty
                                   THEN 'empty_player'
                                 WHEN frontier.page_kind = 'match'
                                  AND match_evidence.dataset_count = 8
                                  AND match_evidence.all_safe
                                  AND match_evidence.completed
                                  AND match_evidence.player_stats_populated
                                   THEN 'full_match'
                                 WHEN frontier.page_kind = 'match'
                                  AND match_evidence.dataset_count = 8
                                  AND match_evidence.all_safe
                                  AND match_evidence.completed
                                  AND match_evidence.player_stats_sparse
                                   THEN 'sparse_match'
                                 ELSE ''
                               END
                             ORDER BY frontier.priority DESC,
                                      frontier.target_id
                           ) AS bucket_rank
                    FROM fbref_control.page_frontier AS frontier
                    LEFT JOIN scope_rollup AS scope_rollup
                      ON scope_rollup.target_id = frontier.target_id
                    LEFT JOIN LATERAL (
                      SELECT competition.gender,
                             canonical.competition_id,
                             canonical.season_id,
                             season.is_current
                      FROM canonical_scope AS canonical
                      JOIN fbref_control.competition_registry AS competition
                        ON competition.source = canonical.source
                       AND competition.competition_id = canonical.competition_id
                      LEFT JOIN fbref_control.season_registry AS season
                        ON season.source = canonical.source
                       AND season.competition_id = canonical.competition_id
                       AND season.season_id = canonical.season_id
                      WHERE canonical.target_id = frontier.target_id
                        AND (
                          canonical.season_id IS NULL
                          OR (
                            season.lifecycle_state = 'present'
                            AND season.present
                            AND (
                              (%s = 'current' AND season.is_current)
                              OR (%s = 'history' AND NOT season.is_current)
                            )
                          )
                        )
                      ORDER BY COALESCE(season.is_current, false) DESC,
                               canonical.competition_id, canonical.season_id
                      LIMIT 1
                    ) AS selected ON true
                    LEFT JOIN page_evidence
                      ON page_evidence.target_id = frontier.target_id
                    LEFT JOIN match_evidence
                      ON match_evidence.target_id = frontier.target_id
                    WHERE frontier.source = 'fbref'
                      AND (
                        frontier.state IN ('queued', 'fetched')
                        OR (
                          frontier.state = 'retry'
                          AND (
                            frontier.retry_after IS NULL
                            OR frontier.retry_after <= clock_timestamp()
                          )
                        )
                      )
                      AND (
                        frontier.page_kind = 'competition_index'
                        OR (
                          selected.gender = 'male'
                          AND scope_rollup.scope_count > 0
                          AND NOT COALESCE(
                            scope_rollup.competition_missing, true
                          )
                          AND NOT COALESCE(scope_rollup.has_female, false)
                          AND NOT COALESCE(scope_rollup.has_unknown, true)
                          AND NOT COALESCE(
                            scope_rollup.inactive_competition, true
                          )
                          AND NOT COALESCE(scope_rollup.invalid_season, true)
                        )
                      )
                      AND (
                        (
                          %s = 'current'
                          AND frontier.refresh_policy <> 'historical_once'
                          AND (
                            frontier.page_kind = 'competition_index'
                            OR selected.season_id IS NULL
                            OR selected.is_current
                          )
                        )
                        OR (
                          %s = 'history'
                          AND frontier.page_kind <> 'competition_index'
                          AND frontier.refresh_policy = 'historical_once'
                          AND selected.season_id IS NOT NULL
                          AND NOT selected.is_current
                        )
                      )
                ), representatives AS (
                    SELECT eligible.*,
                           CASE
                             WHEN page_kind = 'season_stats'
                              AND source_ids ->> 'stat_route' IN (
                                'standard', 'shooting', 'playingtime',
                                'misc', 'keepers'
                              )
                               THEN 'season_stats_' ||
                                    (source_ids ->> 'stat_route')
                             WHEN evidence_class = 'populated_player'
                               THEN 'player_populated'
                             WHEN evidence_class = 'empty_player'
                               THEN 'player_empty'
                             WHEN evidence_class = 'full_match'
                               THEN 'match_full'
                             WHEN evidence_class = 'sparse_match'
                               THEN 'match_sparse'
                             WHEN page_kind IN (
                               'competition_index', 'competition', 'season',
                               'schedule', 'standings', 'squad', 'matchlog'
                             ) THEN page_kind
                             ELSE NULL
                           END AS coverage_slot,
                           row_number() OVER (
                             PARTITION BY page_kind,
                               COALESCE(source_ids ->> 'stat_route', ''),
                               COALESCE(evidence_class, '')
                             ORDER BY target_id
                           ) AS coverage_rank
                    FROM eligible
                ), history_season_coverage AS (
                    SELECT competition_id, season_id,
                           count(DISTINCT coverage_slot) AS slot_count
                    FROM representatives
                    WHERE competition_id IS NOT NULL
                      AND season_id IS NOT NULL
                      AND is_current = false
                      AND coverage_slot IS NOT NULL
                    GROUP BY competition_id, season_id
                ), ranked_history_seasons AS (
                    SELECT competition_id, season_id, slot_count,
                           row_number() OVER (
                             ORDER BY (slot_count >= 14) DESC,
                                      slot_count DESC,
                                      competition_id, season_id
                           ) AS season_rank
                    FROM history_season_coverage
                )
                SELECT target_id, page_kind, canonical_url, source_ids,
                       refresh_policy, state, gender, competition_id,
                       season_id, is_current, evidence_class
                FROM representatives
                LEFT JOIN ranked_history_seasons AS history
                  USING (competition_id, season_id)
                WHERE (%s = 'current' AND coverage_rank <= 10)
                   OR (
                     %s = 'history'
                     AND history.season_rank <= 5
                     AND bucket_rank <= 3
                   )
                ORDER BY CASE WHEN %s = 'current' THEN page_kind ELSE '' END,
                         CASE WHEN %s = 'current'
                           THEN COALESCE(source_ids ->> 'stat_route', '')
                           ELSE ''
                         END,
                         CASE WHEN %s = 'current'
                           THEN COALESCE(evidence_class, '') ELSE '' END,
                         history.season_rank NULLS FIRST,
                         competition_id NULLS FIRST, season_id NULLS FIRST,
                         page_kind,
                         COALESCE(source_ids ->> 'stat_route', ''),
                         evidence_class NULLS LAST, bucket_rank, target_id
                LIMIT %s
                """,
                (
                    PAGE_DOCUMENT_VERSION,
                    match_datasets,
                    match_datasets,
                    match_datasets,
                    TYPED_BRONZE_PARSER_VERSION,
                    normalized_scope,
                    normalized_scope,
                    normalized_scope,
                    normalized_scope,
                    normalized_scope,
                    normalized_scope,
                    normalized_scope,
                    normalized_scope,
                    normalized_scope,
                    normalized_limit,
                ),
            )
            rows = _fetchall(cursor)
        candidates = []
        for row in rows:
            source_ids = row.get("source_ids") or {}
            if isinstance(source_ids, str):
                source_ids = json.loads(source_ids)
            if not isinstance(source_ids, Mapping):
                raise StateConflict(
                    f"Acceptance candidate {row.get('target_id')} has invalid source_ids"
                )
            candidates.append(
                {
                    "target_id": str(row["target_id"]),
                    "page_kind": str(row["page_kind"]),
                    "canonical_url": str(row["canonical_url"]),
                    "source_ids": dict(source_ids),
                    "refresh_policy": str(row["refresh_policy"]),
                    "state": str(row["state"]),
                    "gender": (
                        None if row.get("gender") is None else str(row["gender"])
                    ),
                    "competition_id": (
                        None
                        if row.get("competition_id") is None
                        else str(row["competition_id"])
                    ),
                    "season_id": (
                        None
                        if row.get("season_id") is None
                        else str(row["season_id"])
                    ),
                    "is_current": (
                        None
                        if row.get("is_current") is None
                        else bool(row["is_current"])
                    ),
                    "evidence_class": (
                        None
                        if row.get("evidence_class") is None
                        else str(row["evidence_class"])
                    ),
                }
            )
        return candidates

    def create_explicit_run_cohort(
        self,
        run_id: object,
        target_ids: Sequence[object],
    ) -> list[CohortTarget]:
        """Atomically freeze an exact, ordered, already-crawlable cohort.

        Unlike due-frontier admission this method never chooses targets.  The
        caller supplies every identity and PostgreSQL either installs that
        exact sequence or installs nothing.  A retry is idempotent only when
        the complete immutable membership is byte-for-byte equivalent.
        """

        run = _uuid(run_id, "run_id")
        normalized_ids = [_text(item, "target_id") for item in target_ids]
        if not 1 <= len(normalized_ids) <= 25:
            raise ValueError("explicit cohort must contain between 1 and 25 targets")
        if len(normalized_ids) != len(set(normalized_ids)):
            raise ValueError("explicit cohort contains duplicate target IDs")
        cohort = [
            CohortTarget(
                target_id=target_id,
                logical_refresh_id=make_logical_refresh_id(run, target_id),
                ordinal=ordinal,
            )
            for ordinal, target_id in enumerate(normalized_ids)
        ]

        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status, run_type, request_limit, byte_limit, metadata
                FROM fbref_control.crawl_run
                WHERE run_id = %s
                FOR UPDATE
                """,
                (run,),
            )
            crawl_run = _fetchone(cursor)
            if crawl_run is None or crawl_run["status"] != "running":
                raise StateConflict(f"Run {run} cannot accept an explicit cohort")
            metadata = _json_mapping(
                crawl_run.get("metadata") or {}, "crawl run metadata"
            )
            scope = str(metadata.get("acceptance_scope") or "").casefold()
            if (
                scope not in {"current", "history"}
                or metadata.get("acceptance_profile") is not True
                or metadata.get("publication_eligible") is not False
                or int(metadata.get("shard_size") or 0) != 25
                or int(crawl_run["request_limit"]) != 100
                or int(crawl_run["byte_limit"]) != 50 * 1024 * 1024
                or (
                    scope == "current" and crawl_run["run_type"] != "current"
                )
                or (
                    scope == "history" and crawl_run["run_type"] != "backfill"
                )
            ):
                raise StateConflict(
                    f"Run {run} is not the bounded nonpublishing acceptance profile"
                )
            cursor.execute(
                """
                SELECT target_id, logical_refresh_id, ordinal
                FROM fbref_control.run_target
                WHERE run_id = %s
                ORDER BY ordinal
                """,
                (run,),
            )
            installed = _fetchall(cursor)
            if installed:
                installed_cohort = [
                    CohortTarget(
                        target_id=str(item["target_id"]),
                        logical_refresh_id=str(item["logical_refresh_id"]),
                        ordinal=int(item["ordinal"]),
                    )
                    for item in installed
                ]
                if installed_cohort != cohort:
                    raise StateConflict(
                        f"Run {run} already has a different immutable cohort"
                    )
                return cohort

            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                , requested(target_id, ordinal) AS (
                    SELECT requested.target_id, requested.ordinality - 1
                    FROM unnest(%s::text[]) WITH ORDINALITY
                         AS requested(target_id, ordinality)
                )
                SELECT requested.target_id
                FROM requested
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = requested.target_id
                LEFT JOIN scope_rollup AS scope
                  ON scope.target_id = frontier.target_id
                WHERE frontier.source = 'fbref'
                  AND (
                    frontier.state IN ('queued', 'fetched')
                    OR (
                      frontier.state = 'retry'
                      AND (
                        frontier.retry_after IS NULL
                        OR frontier.retry_after <= clock_timestamp()
                      )
                    )
                  )
                  AND (
                    frontier.page_kind = 'competition_index'
                    OR (
                      scope.scope_count > 0
                      AND NOT COALESCE(scope.competition_missing, true)
                      AND NOT COALESCE(scope.has_female, false)
                      AND NOT COALESCE(scope.has_unknown, true)
                      AND NOT COALESCE(scope.inactive_competition, true)
                      AND NOT COALESCE(scope.invalid_season, true)
                    )
                  )
                  AND (
                    (
                      %s = 'current'
                      AND (
                        frontier.page_kind = 'competition_index'
                        OR (
                          frontier.refresh_policy <> 'historical_once'
                          AND (
                            COALESCE(scope.has_competition_scope, false)
                            OR COALESCE(scope.has_current_season, false)
                          )
                        )
                      )
                    )
                    OR (
                      %s = 'history'
                      AND frontier.page_kind <> 'competition_index'
                      AND frontier.refresh_policy = 'historical_once'
                      AND NOT COALESCE(scope.has_current_season, false)
                      AND NOT COALESCE(scope.has_competition_scope, false)
                    )
                  )
                ORDER BY requested.ordinal
                FOR UPDATE OF frontier
                """,
                (normalized_ids, scope, scope),
            )
            crawlable_ids = [
                str(item["target_id"]) for item in _fetchall(cursor)
            ]
            if crawlable_ids != normalized_ids:
                unavailable = [
                    target_id
                    for target_id in normalized_ids
                    if target_id not in set(crawlable_ids)
                ]
                raise StateConflict(
                    "Explicit cohort contains absent, leased, out-of-scope, "
                    "or non-crawlable targets: " + ", ".join(unavailable)
                )
            cursor.execute(
                """
                SELECT outstanding.target_id, outstanding.run_id
                FROM fbref_control.run_target AS outstanding
                JOIN fbref_control.crawl_run AS outstanding_run
                  ON outstanding_run.run_id = outstanding.run_id
                WHERE outstanding.target_id = ANY(%s::text[])
                  AND outstanding.run_id <> %s
                  AND outstanding.status IN ('pending', 'leased', 'retry')
                  AND outstanding_run.status IN ('pending', 'running')
                ORDER BY outstanding.target_id
                LIMIT 1
                """,
                (normalized_ids, run),
            )
            outstanding = _fetchone(cursor)
            if outstanding is not None:
                raise StateConflict(
                    f"Target {outstanding['target_id']} already belongs to active "
                    f"run {outstanding['run_id']}"
                )
            for item in cohort:
                cursor.execute(
                    """
                    INSERT INTO fbref_control.run_target (
                        run_id, target_id, logical_refresh_id, ordinal
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (
                        run,
                        item.target_id,
                        item.logical_refresh_id,
                        item.ordinal,
                    ),
                )
                cursor.execute(
                    """
                    UPDATE fbref_control.page_frontier
                    SET state = 'queued', next_fetch_at = clock_timestamp(),
                        retry_after = NULL, updated_at = clock_timestamp()
                    WHERE target_id = %s
                    """,
                    (item.target_id,),
                )
                if cursor.rowcount != 1:
                    raise StateConflict(f"Target {item.target_id} was lost")
        return cohort

    def create_run_cohort(
        self,
        run_id: object,
        targets: Sequence[CohortTarget],
    ) -> int:
        """Insert immutable run membership; only processing status may change."""
        run = _uuid(run_id, "run_id")
        normalized = []
        seen_targets = set()
        seen_refreshes = set()
        seen_ordinals = set()
        for target in targets:
            item = CohortTarget(
                target_id=_text(target.target_id, "target_id"),
                logical_refresh_id=_uuid(
                    target.logical_refresh_id, "logical_refresh_id"
                ),
                ordinal=_non_negative(target.ordinal, "ordinal"),
            )
            if (
                item.target_id in seen_targets
                or item.logical_refresh_id in seen_refreshes
                or item.ordinal in seen_ordinals
            ):
                raise ValueError("Run cohort contains duplicate identities")
            seen_targets.add(item.target_id)
            seen_refreshes.add(item.logical_refresh_id)
            seen_ordinals.add(item.ordinal)
            normalized.append(item)

        inserted = 0
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run,),
            )
            crawl_run = _fetchone(cursor)
            if crawl_run is None or crawl_run["status"] not in {"pending", "running"}:
                raise StateConflict(f"Run {run} cannot accept a cohort")
            for item in normalized:
                cursor.execute(
                    """
                    SELECT state
                    FROM fbref_control.page_frontier
                    WHERE target_id = %s
                    FOR UPDATE
                    """,
                    (item.target_id,),
                )
                frontier = _fetchone(cursor)
                cursor.execute(
                    """
                    SELECT logical_refresh_id, ordinal
                    FROM fbref_control.run_target
                    WHERE run_id = %s AND target_id = %s
                    """,
                    (run, item.target_id),
                )
                existing = _fetchone(cursor)
                if existing is not None:
                    if (
                        str(existing["logical_refresh_id"])
                        != item.logical_refresh_id
                        or int(existing["ordinal"]) != item.ordinal
                    ):
                        raise StateConflict(
                            f"Run cohort target {item.target_id} is immutable"
                        )
                    continue
                if frontier is None or frontier["state"] in {
                    "leased", "dead", "quarantined", "skipped"
                }:
                    raise StateConflict(
                        f"Target {item.target_id} is absent or not crawlable"
                    )
                cursor.execute(
                    """
                    SELECT outstanding.run_id
                    FROM fbref_control.run_target AS outstanding
                    JOIN fbref_control.crawl_run AS outstanding_run
                      ON outstanding_run.run_id = outstanding.run_id
                    WHERE outstanding.target_id = %s
                      AND outstanding.run_id <> %s
                      AND outstanding.status IN (
                          'pending', 'leased', 'retry'
                      )
                      AND outstanding_run.status IN ('pending', 'running')
                    LIMIT 1
                    """,
                    (item.target_id, run),
                )
                outstanding = _fetchone(cursor)
                if outstanding is not None:
                    raise StateConflict(
                        f"Target {item.target_id} already belongs to active run "
                        f"{outstanding['run_id']}"
                    )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.run_target (
                        run_id, target_id, logical_refresh_id, ordinal
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (
                        run,
                        item.target_id,
                        item.logical_refresh_id,
                        item.ordinal,
                    ),
                )
                cursor.execute(
                    """
                    UPDATE fbref_control.page_frontier
                    SET state = CASE
                            WHEN state = 'fetched' THEN 'queued'
                            ELSE state
                        END,
                        updated_at = clock_timestamp()
                    WHERE target_id = %s
                    """,
                    (item.target_id,),
                )
                if cursor.rowcount != 1:
                    raise StateConflict(f"Target {item.target_id} was lost")
                inserted += 1
        return inserted

    def create_due_run_cohort(
        self,
        run_id: object,
        *,
        page_kinds: Optional[Sequence[str]] = None,
        refresh_policies: Optional[Sequence[str]] = None,
        limit: int = 25,
    ) -> list[CohortTarget]:
        """Atomically select a due shard and assign stable refresh identities."""
        run = _uuid(run_id, "run_id")
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 25:
            raise ValueError("limit must be between 1 and 25")
        kinds = None
        if page_kinds is not None:
            kinds = sorted({_text(kind, "page_kind") for kind in page_kinds})
            if not kinds:
                return []
        policies = None
        if refresh_policies is not None:
            policies = sorted(
                {_text(policy, "refresh_policy") for policy in refresh_policies}
            )
            if not policies:
                return []
        # Admission tiers keep the publication spine ahead of an arbitrarily
        # old enrichment backlog.  In particular, a newly discovered current
        # match must not need a previous fetch before it becomes critical.
        cohort = []
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run,),
            )
            crawl_run = _fetchone(cursor)
            if crawl_run is None or crawl_run["status"] not in {"pending", "running"}:
                raise StateConflict(f"Run {run} cannot accept a due cohort")
            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                , eligible AS MATERIALIZED (
                  SELECT frontier.target_id, frontier.page_kind,
                         frontier.last_fetched_at, frontier.created_at,
                         frontier.priority, frontier.next_fetch_at,
                         COALESCE(
                           frontier.retry_after,
                           frontier.next_fetch_at,
                           frontier.last_fetched_at,
                           frontier.created_at
                         ) AS due_at,
                         CASE
                           WHEN frontier.page_kind = 'competition_index'
                             THEN 0
                           WHEN frontier.page_kind = 'match'
                            AND frontier.refresh_policy <> 'historical_once'
                            AND COALESCE(scope.has_current_season, false)
                             THEN 1
                           WHEN frontier.page_kind = ANY(%s::text[])
                            AND frontier.refresh_policy <> 'historical_once'
                             THEN 2
                           WHEN frontier.page_kind = ANY(%s::text[])
                            AND frontier.refresh_policy <> 'historical_once'
                             THEN 3
                           ELSE 4
                         END AS admission_tier
                  FROM fbref_control.page_frontier AS frontier
                  LEFT JOIN scope_rollup AS scope
                    ON scope.target_id = frontier.target_id
                  WHERE (
                        frontier.state IN ('queued', 'retry')
                        OR (
                            frontier.state = 'fetched'
                            AND frontier.next_fetch_at IS NOT NULL
                            AND frontier.next_fetch_at <= clock_timestamp()
                        )
                    )
                  AND (frontier.next_fetch_at IS NULL
                       OR frontier.next_fetch_at <= clock_timestamp())
                  AND (frontier.retry_after IS NULL
                       OR frontier.retry_after <= clock_timestamp())
                  AND (%s::text[] IS NULL
                       OR frontier.page_kind = ANY(%s::text[]))
                  AND (%s::text[] IS NULL
                       OR frontier.refresh_policy = ANY(%s::text[]))
                  AND (
                    frontier.page_kind = 'competition_index'
                    OR (
                      scope.scope_count > 0
                      AND NOT COALESCE(scope.competition_missing, true)
                      AND NOT COALESCE(scope.has_female, false)
                      AND NOT COALESCE(scope.has_unknown, true)
                      AND NOT COALESCE(scope.inactive_competition, true)
                      AND NOT COALESCE(scope.invalid_season, true)
                      AND (
                        COALESCE(scope.has_competition_scope, false)
                        OR COALESCE(scope.has_current_season, false)
                        OR frontier.refresh_policy = 'historical_once'
                      )
                    )
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM fbref_control.run_target AS existing
                      WHERE existing.run_id = %s
                        AND existing.target_id = frontier.target_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM fbref_control.run_target AS outstanding
                      JOIN fbref_control.crawl_run AS outstanding_run
                        ON outstanding_run.run_id = outstanding.run_id
                      WHERE outstanding.target_id = frontier.target_id
                        AND outstanding.status IN (
                            'pending', 'leased', 'retry'
                        )
                        AND outstanding_run.status IN ('pending', 'running')
                  )
                )
                SELECT frontier.target_id
                FROM eligible
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = eligible.target_id
                ORDER BY eligible.admission_tier,
                         eligible.due_at,
                         frontier.priority DESC,
                         frontier.created_at,
                         frontier.target_id
                LIMIT %s
                FOR UPDATE OF frontier SKIP LOCKED
                """,
                (
                    list(DISCOVERY_SPINE_PAGE_KINDS),
                    list(OTHER_PUBLICATION_CRITICAL_PAGE_KINDS),
                    kinds,
                    kinds,
                    policies,
                    policies,
                    run,
                    normalized_limit,
                ),
            )
            candidates = _fetchall(cursor)
            cursor.execute(
                """
                SELECT COALESCE(max(ordinal), -1) + 1 AS next_ordinal
                FROM fbref_control.run_target WHERE run_id = %s
                """,
                (run,),
            )
            next_ordinal = int(_fetchone(cursor)["next_ordinal"])
            accepted_offset = 0
            for candidate in candidates:
                target_id = str(candidate["target_id"])
                # The candidate CTE is evaluated before its frontier row is
                # locked.  A competing run may have committed membership in
                # that narrow interval, so recheck while this transaction now
                # owns the target's frontier fence.
                cursor.execute(
                    """
                    SELECT outstanding.run_id
                    FROM fbref_control.run_target AS outstanding
                    JOIN fbref_control.crawl_run AS outstanding_run
                      ON outstanding_run.run_id = outstanding.run_id
                    WHERE outstanding.target_id = %s
                      AND outstanding.status IN (
                          'pending', 'leased', 'retry'
                      )
                      AND outstanding_run.status IN ('pending', 'running')
                    LIMIT 1
                    """,
                    (target_id,),
                )
                if _fetchone(cursor) is not None:
                    continue
                item = CohortTarget(
                    target_id=target_id,
                    logical_refresh_id=make_logical_refresh_id(run, target_id),
                    ordinal=next_ordinal + accepted_offset,
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.run_target (
                        run_id, target_id, logical_refresh_id, ordinal
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (
                        run,
                        item.target_id,
                        item.logical_refresh_id,
                        item.ordinal,
                    ),
                )
                cursor.execute(
                    """
                    UPDATE fbref_control.page_frontier
                    SET state = 'queued', updated_at = clock_timestamp()
                    WHERE target_id = %s
                    """,
                    (item.target_id,),
                )
                cohort.append(item)
                accepted_offset += 1
        return cohort

    def get_run_summary(
        self,
        run_id: object,
        *,
        parser_version: Optional[object] = None,
        typed_parser_version: Optional[object] = None,
        stateful_parser_version: Optional[object] = None,
        raw_processing_sla_seconds: int = 86_400,
    ) -> Optional[dict]:
        """Return compact budget/target/attempt/dataset counts for DAG gates.

        Supplying all parser versions scopes validation to the per-observation
        fence.  The unversioned form remains available to diagnostic and
        one-off remediation callers that predate that fence.
        """
        run = _uuid(run_id, "run_id")
        supplied_versions = (
            parser_version,
            typed_parser_version,
            stateful_parser_version,
        )
        if any(value is not None for value in supplied_versions) and not all(
            value is not None for value in supplied_versions
        ):
            raise ValueError("all parser versions must be supplied together")
        parser = (
            None
            if parser_version is None
            else _text(parser_version, "parser_version")
        )
        typed_parser = (
            None
            if typed_parser_version is None
            else _text(typed_parser_version, "typed_parser_version")
        )
        stateful_parser = (
            None
            if stateful_parser_version is None
            else _text(stateful_parser_version, "stateful_parser_version")
        )
        raw_sla = int(raw_processing_sla_seconds)
        if raw_sla <= 0:
            raise ValueError("raw_processing_sla_seconds must be positive")
        with self._transaction() as cursor:
            cursor.execute(
                "SELECT * FROM fbref_control.crawl_run WHERE run_id = %s",
                (run,),
            )
            summary = _fetchone(cursor)
            if summary is None:
                return None
            cursor.execute(
                """
                SELECT status, count(*) AS count
                FROM fbref_control.run_target
                WHERE run_id = %s GROUP BY status ORDER BY status
                """,
                (run,),
            )
            summary["target_counts"] = {
                str(row["status"]): int(row["count"])
                for row in _fetchall(cursor)
            }
            cursor.execute(
                """
                SELECT frontier.page_kind, count(*) AS count
                FROM fbref_control.run_target AS target
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = target.target_id
                WHERE target.run_id = %s
                GROUP BY frontier.page_kind
                ORDER BY frontier.page_kind
                """,
                (run,),
            )
            summary["cohort_page_kind_counts"] = {
                str(row["page_kind"]): int(row["count"])
                for row in _fetchall(cursor)
            }
            cursor.execute(
                """
                SELECT CASE
                         WHEN frontier.page_kind = 'season_stats'
                           THEN 'season_stats:' || COALESCE(
                             frontier.source_ids ->> 'stat_route', 'unknown'
                           )
                         ELSE frontier.page_kind
                       END AS route,
                       count(*) AS count
                FROM fbref_control.run_target AS target
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = target.target_id
                WHERE target.run_id = %s
                GROUP BY route
                ORDER BY route
                """,
                (run,),
            )
            summary["cohort_route_counts"] = {
                str(row["route"]): int(row["count"])
                for row in _fetchall(cursor)
            }
            cursor.execute(
                """
                SELECT
                    count(*) FILTER (
                        WHERE season.is_current
                          AND frontier.refresh_policy <> 'historical_once'
                    )
                        AS current_pending_match_count,
                    count(*) FILTER (
                        WHERE NOT season.is_current
                          AND frontier.refresh_policy = 'historical_once'
                    ) AS historical_pending_match_count
                FROM fbref_control.page_frontier AS frontier
                JOIN fbref_control.competition_registry AS competition
                  ON competition.source = frontier.source
                 AND competition.competition_id =
                     frontier.source_ids ->> 'competition_id'
                JOIN fbref_control.season_registry AS season
                  ON season.source = frontier.source
                 AND season.competition_id = competition.competition_id
                 AND season.season_id =
                     frontier.source_ids ->> 'season_id'
                WHERE frontier.source = 'fbref'
                  AND frontier.page_kind = 'match'
                  AND (
                    frontier.state IN ('queued', 'retry', 'leased')
                    OR (
                      frontier.state = 'fetched'
                      AND frontier.next_fetch_at IS NOT NULL
                      AND frontier.next_fetch_at <= clock_timestamp()
                    )
                  )
                  AND competition.gender = 'male'
                  AND competition.crawl_state = 'active'
                  AND competition.lifecycle_state IN (
                      'present', 'missing_once'
                  )
                  AND competition.present
                  AND season.lifecycle_state = 'present'
                  AND season.present
                """
            )
            pending_matches = _fetchone(cursor)
            current_pending = int(
                0
                if pending_matches is None
                else pending_matches.get("current_pending_match_count") or 0
            )
            historical_pending = int(
                0
                if pending_matches is None
                else pending_matches.get("historical_pending_match_count") or 0
            )
            # Compatibility key consumed by the existing hard gate.  It now
            # deliberately describes only the current registry snapshot.
            summary["promotion_pending_match_count"] = current_pending
            summary["current_pending_match_count"] = current_pending
            summary["historical_pending_match_count"] = historical_pending
            cursor.execute(
                """
                SELECT frontier.target_id,
                       frontier.source_ids ->> 'competition_id'
                           AS competition_id,
                       frontier.source_ids ->> 'season_id' AS season_id,
                       frontier.state, frontier.last_error_class,
                       left(frontier.last_error_message, 500)
                           AS last_error_message
                FROM fbref_control.page_frontier AS frontier
                JOIN fbref_control.competition_registry AS competition
                  ON competition.source = frontier.source
                 AND competition.competition_id =
                     frontier.source_ids ->> 'competition_id'
                JOIN fbref_control.season_registry AS season
                  ON season.source = frontier.source
                 AND season.competition_id = competition.competition_id
                 AND season.season_id =
                     frontier.source_ids ->> 'season_id'
                WHERE frontier.source = 'fbref'
                  AND frontier.page_kind = 'match'
                  AND (
                    frontier.state IN ('queued', 'retry', 'leased')
                    OR (
                      frontier.state = 'fetched'
                      AND frontier.next_fetch_at IS NOT NULL
                      AND frontier.next_fetch_at <= clock_timestamp()
                    )
                  )
                  AND competition.gender = 'male'
                  AND competition.crawl_state = 'active'
                  AND competition.lifecycle_state IN (
                      'present', 'missing_once'
                  )
                  AND competition.present
                  AND season.lifecycle_state = 'present'
                  AND season.present
                  AND season.is_current
                  AND frontier.refresh_policy <> 'historical_once'
                ORDER BY frontier.priority DESC,
                         COALESCE(
                           frontier.retry_after,
                           frontier.next_fetch_at,
                           frontier.created_at
                         ),
                         frontier.target_id
                LIMIT %s
                """,
                (_PENDING_MATCH_SAMPLE_LIMIT,),
            )
            summary["current_pending_match_sample"] = [
                {
                    "target_id": str(row["target_id"]),
                    "competition_id": str(row["competition_id"]),
                    "season_id": str(row["season_id"]),
                    "state": str(row["state"]),
                    "last_error_class": row.get("last_error_class"),
                    "last_error_message": row.get("last_error_message"),
                }
                for row in _fetchall(cursor)
            ]
            cursor.execute(
                """
                SELECT status, count(*) AS count
                FROM fbref_control.fetch_attempt
                WHERE run_id = %s GROUP BY status ORDER BY status
                """,
                (run,),
            )
            summary["attempt_counts"] = {
                str(row["status"]): int(row["count"])
                for row in _fetchall(cursor)
            }
            cursor.execute(
                """
                SELECT manifest.validation_status AS status, count(*) AS count
                FROM fbref_control.dataset_manifest AS manifest
                JOIN fbref_control.fetch_attempt AS attempt
                  ON attempt.target_id = manifest.target_id
                 AND attempt.content_hash = manifest.content_hash
                WHERE attempt.run_id = %s AND attempt.status = 'succeeded'
                  AND (
                    %s::text IS NULL
                    OR (
                      manifest.parser_version IN (%s, %s)
                      AND EXISTS (
                        SELECT 1
                        FROM fbref_control.observation_processing AS observed
                        WHERE observed.logical_refresh_id =
                              attempt.logical_refresh_id
                          AND observed.parser_version = %s
                          AND observed.typed_parser_version = %s
                          AND observed.stateful_parser_version = %s
                      )
                    )
                  )
                GROUP BY manifest.validation_status
                ORDER BY manifest.validation_status
                """,
                (
                    run,
                    parser,
                    parser,
                    typed_parser,
                    parser,
                    typed_parser,
                    stateful_parser,
                ),
            )
            summary["dataset_validation_counts"] = {
                str(row["status"]): int(row["count"])
                for row in _fetchall(cursor)
            }
            cursor.execute(
                """
                SELECT count(*) AS count
                FROM (
                    SELECT DISTINCT attempt.target_id, attempt.content_hash
                    FROM fbref_control.fetch_attempt AS attempt
                    WHERE attempt.run_id = %s
                      AND attempt.status = 'succeeded'
                      AND (
                        (
                          %s::text IS NULL
                          AND NOT EXISTS (
                            SELECT 1
                            FROM fbref_control.dataset_manifest AS manifest
                            WHERE manifest.target_id = attempt.target_id
                              AND manifest.content_hash = attempt.content_hash
                              AND manifest.dataset = '__page__'
                              AND manifest.parse_status = 'succeeded'
                              AND manifest.persistence_status = 'succeeded'
                              AND manifest.validation_status = 'succeeded'
                          )
                        )
                        OR (
                          %s::text IS NOT NULL
                          AND NOT EXISTS (
                            SELECT 1
                            FROM fbref_control.observation_processing AS observed
                            WHERE observed.logical_refresh_id =
                                  attempt.logical_refresh_id
                              AND observed.parser_version = %s
                              AND observed.typed_parser_version = %s
                              AND observed.stateful_parser_version = %s
                              AND observed.status = 'succeeded'
                              AND observed.generic_status = 'succeeded'
                              AND observed.typed_status IN (
                                  'succeeded', 'skipped'
                              )
                              AND observed.stateful_status IN (
                                  'succeeded', 'skipped'
                              )
                              AND observed.validation_status = 'succeeded'
                          )
                        )
                      )
                ) AS missing
                """,
                (
                    run,
                    parser,
                    parser,
                    parser,
                    typed_parser,
                    stateful_parser,
                ),
            )
            missing = _fetchone(cursor)
            summary["unvalidated_target_count"] = int(
                0 if missing is None else missing["count"]
            )

            classified_errors = [
                "budget_exhausted",
                "clearance_failed",
                "clearance_export_failed",
                "hard_transport_policy",
                "http_exception",
                "http_status",
                "empty_body",
                "response_too_large",
                "invalid_encoding",
                "invalid_content_type",
            ]
            cursor.execute(
                """
                SELECT frontier.page_kind,
                       COALESCE(sum(attempt.http_request_count) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                       ), 0) AS network_attempts,
                       COALESCE(sum(
                           COALESCE(cardinality(array_positions(
                               attempt.http_status_history, 200
                           )), 0)
                           + COALESCE(cardinality(array_positions(
                               attempt.http_status_history, 304
                           )), 0)
                       ) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                       ), 0) AS warm_http_successes,
                       count(*) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.status = 'failed'
                       ) AS failed_network_attempts,
                       count(*) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.status = 'failed'
                             AND (
                                 attempt.error_class IS NULL
                                 OR NOT (
                                     attempt.error_class = ANY(%s::text[])
                                     OR attempt.error_class LIKE 'page_contract_%%'
                                     OR attempt.error_class LIKE 'raw_contract_%%'
                                 )
                             )
                       ) AS unclassified_failures,
                       COALESCE(sum(
                           GREATEST(attempt.http_request_count - 1, 0)
                           + CASE WHEN attempt.attempt_number > 1
                                  THEN 1 ELSE 0 END
                       ) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                       ), 0) AS classified_retries,
                       count(*) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.attempt_number > 1
                             AND NOT EXISTS (
                                 SELECT 1
                                 FROM fbref_control.fetch_attempt AS prior
                                 WHERE prior.logical_refresh_id =
                                       attempt.logical_refresh_id
                                   AND prior.attempt_number <
                                       attempt.attempt_number
                                   AND prior.status IN ('failed', 'expired')
                                   AND prior.error_class IS NOT NULL
                             )
                       )
                       + count(*) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.http_request_count > 1
                             AND (
                               cardinality(attempt.http_status_history) <
                                   attempt.http_request_count - 1
                               OR EXISTS (
                                   SELECT 1
                                   FROM unnest(
                                       attempt.http_status_history[
                                           1:GREATEST(
                                               attempt.http_request_count - 1,
                                               0
                                           )::integer
                                       ]
                                   ) AS retry_status(status)
                                   WHERE retry_status.status NOT IN (
                                       500, 502, 503, 504
                                   )
                               )
                             )
                       ) AS duplicate_fetch_violations,
                       percentile_cont(0.5) WITHIN GROUP (
                           ORDER BY attempt.latency_ms
                       ) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.http_status IN (200, 304)
                             AND attempt.raw_manifest_key IS NOT NULL
                             AND attempt.latency_ms IS NOT NULL
                       ) AS p50_latency_ms,
                       percentile_cont(0.95) WITHIN GROUP (
                           ORDER BY attempt.latency_ms
                       ) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.http_status IN (200, 304)
                             AND attempt.raw_manifest_key IS NOT NULL
                             AND attempt.latency_ms IS NOT NULL
                       ) AS p95_latency_ms,
                       percentile_cont(0.5) WITHIN GROUP (
                           ORDER BY attempt.wire_bytes
                       ) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.http_status IN (200, 304)
                             AND attempt.raw_manifest_key IS NOT NULL
                             AND attempt.wire_bytes IS NOT NULL
                       ) AS p50_http_wire_bytes,
                       percentile_cont(0.95) WITHIN GROUP (
                           ORDER BY attempt.wire_bytes
                       ) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.http_status IN (200, 304)
                             AND attempt.raw_manifest_key IS NOT NULL
                             AND attempt.wire_bytes IS NOT NULL
                       ) AS p95_http_wire_bytes,
                       percentile_cont(0.5) WITHIN GROUP (
                           ORDER BY attempt.provider_billed_bytes
                       ) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.http_status IN (200, 304)
                             AND attempt.raw_manifest_key IS NOT NULL
                             AND attempt.provider_billed_bytes IS NOT NULL
                       ) AS p50_provider_billed_bytes,
                       percentile_cont(0.95) WITHIN GROUP (
                           ORDER BY attempt.provider_billed_bytes
                       ) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                             AND attempt.http_status IN (200, 304)
                             AND attempt.raw_manifest_key IS NOT NULL
                             AND attempt.provider_billed_bytes IS NOT NULL
                       ) AS p95_provider_billed_bytes,
                       COALESCE(sum(attempt.wire_bytes) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                       ), 0) AS http_wire_bytes,
                       COALESCE(sum(attempt.decoded_bytes) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                       ), 0)
                           AS decoded_html_bytes,
                       COALESCE(sum(attempt.compressed_bytes) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                       ), 0)
                           AS compressed_raw_bytes,
                       sum(attempt.provider_billed_bytes) FILTER (
                           WHERE attempt.reservation_id IS NOT NULL
                       )
                           AS provider_billed_bytes
                FROM fbref_control.fetch_attempt AS attempt
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = attempt.target_id
                WHERE attempt.run_id = %s
                GROUP BY frontier.page_kind
                ORDER BY frontier.page_kind
                """,
                (classified_errors, run),
            )
            traffic_by_kind = {}
            for row in _fetchall(cursor):
                attempts = int(row["network_attempts"] or 0)
                successes = int(row["warm_http_successes"] or 0)
                traffic_by_kind[str(row["page_kind"])] = {
                    **row,
                    "network_attempts": attempts,
                    "warm_http_successes": successes,
                    "warm_http_success_rate": (
                        None if attempts == 0 else successes / attempts
                    ),
                }
            summary["traffic_by_page_kind"] = traffic_by_kind
            summary["traffic_totals"] = {
                "network_attempts": sum(
                    row["network_attempts"] for row in traffic_by_kind.values()
                ),
                "warm_http_successes": sum(
                    row["warm_http_successes"]
                    for row in traffic_by_kind.values()
                ),
                "failed_network_attempts": sum(
                    int(row["failed_network_attempts"] or 0)
                    for row in traffic_by_kind.values()
                ),
                "unclassified_failures": sum(
                    int(row["unclassified_failures"] or 0)
                    for row in traffic_by_kind.values()
                ),
                "classified_retries": sum(
                    int(row["classified_retries"] or 0)
                    for row in traffic_by_kind.values()
                ),
                "duplicate_fetch_violations": sum(
                    int(row["duplicate_fetch_violations"] or 0)
                    for row in traffic_by_kind.values()
                ),
            }
            total_attempts = summary["traffic_totals"]["network_attempts"]
            total_successes = summary["traffic_totals"]["warm_http_successes"]
            summary["traffic_totals"]["warm_http_success_rate"] = (
                None
                if total_attempts == 0
                else total_successes / total_attempts
            )
            summary["traffic_totals"]["unclassified_failure_rate"] = (
                0.0
                if total_attempts == 0
                else summary["traffic_totals"]["unclassified_failures"]
                / total_attempts
            )

            cursor.execute(
                """
                SELECT count(*) AS sessions,
                       COALESCE(max(browser_bootstrap_attempts), 0)
                           AS max_bootstraps_per_session,
                       COALESCE(sum(browser_bootstrap_attempts), 0)
                           AS browser_bootstrap_attempts,
                       COALESCE(sum(browser_bootstrap_requests), 0)
                           AS browser_bootstrap_requests,
                       COALESCE(sum(browser_document_bytes), 0)
                           AS browser_document_bytes,
                       COALESCE(sum(browser_asset_bytes), 0)
                           AS browser_asset_bytes,
                       COALESCE(sum(browser_unobserved_bytes), 0)
                           AS browser_unobserved_bytes,
                       COALESCE(sum(http_requests), 0) AS http_requests,
                       COALESCE(sum(http_wire_bytes), 0) AS http_wire_bytes,
                       COALESCE(sum(decoded_html_bytes), 0)
                           AS decoded_html_bytes,
                       COALESCE(sum(compressed_raw_bytes), 0)
                           AS compressed_raw_bytes,
                       sum(provider_billed_bytes) AS provider_billed_bytes
                FROM fbref_control.clearance_session
                WHERE run_id = %s
                """,
                (run,),
            )
            summary["session_metrics"] = _fetchone(cursor) or {}

            cursor.execute(
                """
                SELECT crawl_state, lifecycle_state, count(*) AS count
                FROM fbref_control.competition_registry
                WHERE source = 'fbref'
                GROUP BY crawl_state, lifecycle_state
                ORDER BY crawl_state, lifecycle_state
                """
            )
            summary["competition_coverage"] = _fetchall(cursor)
            cursor.execute(
                """
                SELECT gender, count(*) AS count
                FROM fbref_control.competition_registry
                WHERE source = 'fbref'
                  AND lifecycle_state <> 'disappeared'
                GROUP BY gender
                ORDER BY gender
                """
            )
            registry_gender_counts = {
                "male": 0,
                "female": 0,
                "unknown": 0,
            }
            for row in _fetchall(cursor):
                registry_gender_counts[str(row["gender"])] = int(
                    row["count"] or 0
                )
            summary["registry_gender_counts"] = registry_gender_counts
            summary["unknown_gender_registry_count"] = (
                registry_gender_counts["unknown"]
            )
            cursor.execute(
                """
                SELECT page_kind, state, count(*) AS count,
                       count(*) FILTER (
                           WHERE state IN ('queued', 'retry')
                              OR (
                                  state = 'fetched'
                                  AND next_fetch_at IS NOT NULL
                                  AND next_fetch_at <= clock_timestamp()
                              )
                       ) AS due_count,
                       min(
                           CASE
                             WHEN state = 'retry' THEN retry_after
                             WHEN state = 'fetched' THEN next_fetch_at
                             WHEN state = 'queued' THEN created_at
                             ELSE NULL
                           END
                       ) AS oldest_due_at
                FROM fbref_control.page_frontier
                WHERE source = 'fbref'
                GROUP BY page_kind, state
                ORDER BY page_kind, state
                """
            )
            frontier_sla: dict[str, dict[str, Any]] = {}
            for row in _fetchall(cursor):
                kind = str(row["page_kind"])
                bucket = frontier_sla.setdefault(
                    kind,
                    {
                        "states": {},
                        "total": 0,
                        "due": 0,
                        "oldest_due_at": None,
                    },
                )
                count = int(row["count"] or 0)
                due = int(row["due_count"] or 0)
                bucket["states"][str(row["state"])] = count
                bucket["total"] += count
                bucket["due"] += due
                observed_oldest = row.get("oldest_due_at")
                if observed_oldest is not None and (
                    bucket["oldest_due_at"] is None
                    or observed_oldest < bucket["oldest_due_at"]
                ):
                    bucket["oldest_due_at"] = observed_oldest
            summary["frontier_sla_by_page_kind"] = frontier_sla

            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + f"""
                , sla(page_kind, sla_seconds) AS (
                    VALUES {_PAGE_KIND_SLA_VALUES}
                ), current_scope AS (
                    SELECT frontier.page_kind, frontier.state,
                           frontier.refresh_policy, frontier.created_at,
                           frontier.last_fetched_at, sla.sla_seconds
                    FROM fbref_control.page_frontier AS frontier
                    JOIN sla ON sla.page_kind = frontier.page_kind
                    LEFT JOIN scope_rollup AS scope
                      ON scope.target_id = frontier.target_id
                    WHERE frontier.source = 'fbref'
                      -- Current runs cannot claim historical one-shot matches.
                      AND NOT (
                        frontier.page_kind = 'match'
                        AND frontier.refresh_policy = 'historical_once'
                      )
                      AND (
                        frontier.page_kind = 'competition_index'
                        OR (
                          scope.scope_count > 0
                          AND NOT COALESCE(
                              scope.competition_missing, true
                          )
                          AND NOT COALESCE(scope.has_female, false)
                          AND NOT COALESCE(scope.has_unknown, true)
                          AND NOT COALESCE(
                              scope.inactive_competition, true
                          )
                          AND NOT COALESCE(scope.invalid_season, true)
                          AND (
                            frontier.page_kind = 'competition'
                            OR COALESCE(scope.has_current_season, false)
                          )
                        )
                      )
                ), evaluated_scope AS (
                    SELECT current_scope.*,
                           CASE
                             WHEN page_kind = 'match'
                              AND refresh_policy = 'current_completed_once'
                             THEN (
                               (
                                 state = 'fetched'
                                 AND last_fetched_at IS NOT NULL
                               )
                               OR (
                                 state IN ('queued', 'retry', 'leased')
                                 AND COALESCE(
                                   last_fetched_at, created_at
                                 ) >= clock_timestamp()
                                   - (sla_seconds * interval '1 second')
                               )
                             )
                            ELSE (
                              last_fetched_at IS NOT NULL
                              AND last_fetched_at >= clock_timestamp()
                                - (sla_seconds * interval '1 second')
                            ) OR (
                              last_fetched_at IS NULL
                              AND state IN ('queued', 'retry', 'leased')
                              AND created_at >= clock_timestamp()
                                - (sla_seconds * interval '1 second')
                            )
                           END AS within_sla
                    FROM current_scope
                )
                SELECT page_kind, max(sla_seconds) AS sla_seconds,
                       count(*) AS total_targets,
                       count(*) FILTER (
                           WHERE last_fetched_at IS NULL
                       ) AS never_fetched_targets,
                       count(*) FILTER (WHERE NOT within_sla)
                           AS stale_targets,
                       count(*) FILTER (WHERE within_sla)
                           AS fresh_targets,
                       min(last_fetched_at) AS oldest_last_fetched_at
                FROM evaluated_scope
                GROUP BY page_kind
                ORDER BY page_kind
                """
            )
            freshness_by_kind = {
                str(row["page_kind"]): {
                    "sla_seconds": int(row["sla_seconds"]),
                    "total_targets": int(row["total_targets"] or 0),
                    "fresh_targets": int(row["fresh_targets"] or 0),
                    "stale_targets": int(row["stale_targets"] or 0),
                    "never_fetched_targets": int(
                        row["never_fetched_targets"] or 0
                    ),
                    "oldest_last_fetched_at": row.get(
                        "oldest_last_fetched_at"
                    ),
                }
                for row in _fetchall(cursor)
            }
            summary["freshness_by_page_kind"] = freshness_by_kind
            freshness_totals = {
                "total_targets": sum(
                    row["total_targets"]
                    for row in freshness_by_kind.values()
                ),
                "fresh_targets": sum(
                    row["fresh_targets"]
                    for row in freshness_by_kind.values()
                ),
                "stale_targets": sum(
                    row["stale_targets"]
                    for row in freshness_by_kind.values()
                ),
                "never_fetched_targets": sum(
                    row["never_fetched_targets"]
                    for row in freshness_by_kind.values()
                ),
            }
            freshness_totals["all_within_sla"] = (
                freshness_totals["stale_targets"] == 0
            )
            summary["current_scope_freshness"] = freshness_totals
            publication_rows = [
                row
                for kind, row in freshness_by_kind.items()
                if kind in PUBLICATION_FRESHNESS_PAGE_KINDS
            ]
            publication_freshness = {
                "total_targets": sum(
                    row["total_targets"] for row in publication_rows
                ),
                "fresh_targets": sum(
                    row["fresh_targets"] for row in publication_rows
                ),
                "stale_targets": sum(
                    row["stale_targets"] for row in publication_rows
                ),
                "never_fetched_targets": sum(
                    row["never_fetched_targets"] for row in publication_rows
                ),
            }
            publication_freshness["all_within_sla"] = (
                publication_freshness["stale_targets"] == 0
            )
            summary["publication_scope_freshness"] = publication_freshness

            cursor.execute(
                """
                SELECT frontier.page_kind,
                       count(*) FILTER (
                           WHERE attempt.run_id = %s
                       ) AS run_count,
                       count(*) AS global_count,
                       count(*) FILTER (
                           WHERE COALESCE(
                               attempt.finished_at, attempt.started_at
                           ) < clock_timestamp()
                               - (%s * interval '1 second')
                       ) AS global_sla_overdue_count,
                       min(COALESCE(
                           attempt.finished_at, attempt.started_at
                       )) FILTER (
                           WHERE attempt.run_id = %s
                       ) AS run_oldest_raw_at,
                       min(COALESCE(
                           attempt.finished_at, attempt.started_at
                       )) AS global_oldest_raw_at
                FROM fbref_control.fetch_attempt AS attempt
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = attempt.target_id
                WHERE attempt.status = 'succeeded'
                  AND attempt.raw_manifest_key IS NOT NULL
                  AND attempt.content_hash IS NOT NULL
                  AND (
                    (
                      %s::text IS NULL
                      AND NOT EXISTS (
                        SELECT 1
                        FROM fbref_control.dataset_manifest AS manifest
                        WHERE manifest.target_id = attempt.target_id
                          AND manifest.content_hash = attempt.content_hash
                          AND manifest.dataset = '__page__'
                          AND manifest.parse_status = 'succeeded'
                          AND manifest.persistence_status = 'succeeded'
                          AND manifest.validation_status = 'succeeded'
                      )
                    )
                    OR (
                      %s::text IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1
                        FROM fbref_control.observation_processing AS observed
                        WHERE observed.logical_refresh_id =
                              attempt.logical_refresh_id
                          AND observed.parser_version = %s
                          AND observed.typed_parser_version = %s
                          AND observed.stateful_parser_version = %s
                          AND observed.status = 'succeeded'
                          AND observed.generic_status = 'succeeded'
                          AND observed.typed_status IN (
                              'succeeded', 'skipped'
                          )
                          AND observed.stateful_status IN (
                              'succeeded', 'skipped'
                          )
                          AND observed.validation_status = 'succeeded'
                      )
                    )
                  )
                GROUP BY frontier.page_kind
                ORDER BY frontier.page_kind
                """,
                (
                    run,
                    raw_sla,
                    run,
                    parser,
                    parser,
                    parser,
                    typed_parser,
                    stateful_parser,
                ),
            )
            raw_rows = _fetchall(cursor)
            run_raw_by_kind = {
                str(row["page_kind"]): {
                    "count": int(row["run_count"] or 0),
                    "oldest_raw_at": row.get("run_oldest_raw_at"),
                }
                for row in raw_rows
                if int(row["run_count"] or 0) > 0
            }
            global_raw_by_kind = {
                str(row["page_kind"]): {
                    "count": int(row["global_count"] or 0),
                    "sla_overdue_count": int(
                        row["global_sla_overdue_count"] or 0
                    ),
                    "oldest_raw_at": row.get("global_oldest_raw_at"),
                }
                for row in raw_rows
            }
            summary["unprocessed_raw_by_page_kind"] = run_raw_by_kind
            summary["unprocessed_raw_count"] = sum(
                row["count"] for row in run_raw_by_kind.values()
            )
            summary["global_unprocessed_raw_by_page_kind"] = (
                global_raw_by_kind
            )
            summary["global_unprocessed_raw_count"] = sum(
                row["count"] for row in global_raw_by_kind.values()
            )
            global_sla_overdue = sum(
                row["sla_overdue_count"]
                for row in global_raw_by_kind.values()
            )
            summary["global_unprocessed_raw_sla_overdue_count"] = (
                global_sla_overdue
            )
            # Compatibility for operational dashboards created before the
            # run/global split. Validation uses the explicit global key above.
            summary["unprocessed_raw_sla_overdue_count"] = global_sla_overdue
            summary["raw_processing_sla_seconds"] = raw_sla

            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                SELECT
                    CASE
                      WHEN scope.target_id IS NULL THEN 'unresolved_scope'
                      WHEN COALESCE(scope.competition_missing, true)
                        THEN 'missing_competition'
                      WHEN COALESCE(scope.has_unknown, false)
                        THEN 'unknown_gender'
                      WHEN COALESCE(scope.has_female, false)
                        THEN 'female_gender'
                      WHEN COALESCE(scope.inactive_competition, true)
                        THEN 'inactive_competition'
                      WHEN COALESCE(scope.invalid_season, false)
                        THEN 'invalid_season'
                      WHEN NOT (
                        COALESCE(scope.has_competition_scope, false)
                        OR COALESCE(scope.has_current_season, false)
                        OR frontier.refresh_policy = 'historical_once'
                      ) THEN 'noncurrent_season'
                      ELSE 'eligible_male'
                    END AS scope_status,
                    frontier.state NOT IN (
                      'skipped', 'quarantined', 'dead'
                    ) AS crawlable,
                    count(*) AS count
                FROM fbref_control.page_frontier AS frontier
                LEFT JOIN scope_rollup AS scope
                  ON scope.target_id = frontier.target_id
                WHERE frontier.source = 'fbref'
                  AND frontier.page_kind <> 'competition_index'
                GROUP BY scope_status, crawlable
                ORDER BY scope_status, crawlable
                """
            )
            scope_rows = _fetchall(cursor)
            scope_counts: dict[str, int] = {}
            crawlable_scope_counts: dict[str, int] = {}
            noncrawlable_scope_counts: dict[str, int] = {}
            for row in scope_rows:
                status = str(row["scope_status"])
                count = int(row["count"] or 0)
                scope_counts[status] = scope_counts.get(status, 0) + count
                target = (
                    crawlable_scope_counts
                    if row["crawlable"]
                    else noncrawlable_scope_counts
                )
                target[status] = target.get(status, 0) + count
            summary["frontier_scope_counts"] = scope_counts
            summary["crawlable_frontier_scope_counts"] = (
                crawlable_scope_counts
            )
            summary["noncrawlable_frontier_scope_counts"] = (
                noncrawlable_scope_counts
            )
            cursor.execute(
                """
                SELECT manifest.availability, count(*) AS count
                FROM fbref_control.dataset_manifest AS manifest
                JOIN fbref_control.fetch_attempt AS attempt
                  ON attempt.target_id = manifest.target_id
                 AND attempt.content_hash = manifest.content_hash
                WHERE attempt.run_id = %s AND manifest.dataset <> '__page__'
                  AND (
                    %s::text IS NULL
                    OR (
                      manifest.parser_version IN (%s, %s)
                      AND EXISTS (
                        SELECT 1
                        FROM fbref_control.observation_processing AS observed
                        WHERE observed.logical_refresh_id =
                              attempt.logical_refresh_id
                          AND observed.parser_version = %s
                          AND observed.typed_parser_version = %s
                          AND observed.stateful_parser_version = %s
                      )
                    )
                  )
                GROUP BY manifest.availability
                ORDER BY manifest.availability
                """,
                (
                    run,
                    parser,
                    parser,
                    typed_parser,
                    parser,
                    typed_parser,
                    stateful_parser,
                ),
            )
            summary["table_availability"] = {
                str(row["availability"]): int(row["count"])
                for row in _fetchall(cursor)
            }
            cursor.execute(
                """
                SELECT competition.gender, count(DISTINCT frontier.target_id)
                           AS count
                FROM fbref_control.page_frontier AS frontier
                JOIN fbref_control.competition_registry AS competition
                  ON competition.source = frontier.source
                 AND competition.competition_id =
                     frontier.source_ids ->> 'competition_id'
                WHERE competition.gender IN ('female', 'unknown')
                  AND frontier.state NOT IN (
                      'skipped', 'quarantined', 'dead'
                  )
                GROUP BY competition.gender
                ORDER BY competition.gender
                """
            )
            out_of_scope = {
                str(row["gender"]): int(row["count"])
                for row in _fetchall(cursor)
            }
            summary["female_downstream_targets"] = out_of_scope.get("female", 0)
            summary["unknown_gender_downstream_targets"] = out_of_scope.get(
                "unknown", 0
            )
            cursor.execute(
                """
                SELECT metadata
                FROM fbref_control.registry_snapshot
                WHERE source = 'fbref' AND successful
                  AND metadata ->> 'page_kind' = 'competition_index'
                ORDER BY fetched_at DESC LIMIT 1
                """
            )
            snapshot = _fetchone(cursor)
            summary["sentinel_coverage"] = (
                {}
                if not snapshot
                else dict(snapshot.get("metadata") or {}).get("sentinels", {})
            )
            return summary

    def get_acceptance_run_evidence(self, run_id: object) -> Optional[dict]:
        """Return stable target/dataset proof for live or no-op replay.

        A replay run owns no fetch attempts.  When it was initialized through
        the acceptance replay API, its metadata identifies the frozen source;
        target and dataset evidence therefore comes from that source while the
        returned summary (including zero traffic) remains the replay run's.
        """

        from scrapers.fbref.discovery import DISCOVERY_PARSER_VERSION
        from scrapers.fbref.page_document import PAGE_DOCUMENT_VERSION
        from scrapers.fbref.typed_bronze import TYPED_BRONZE_PARSER_VERSION

        run = _uuid(run_id, "run_id")
        summary = self.get_run_summary(
            run,
            parser_version=PAGE_DOCUMENT_VERSION,
            typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
            stateful_parser_version=DISCOVERY_PARSER_VERSION,
        )
        if summary is None:
            return None
        metadata = summary.get("metadata")
        evidence_run = run
        if (
            str(summary.get("run_type") or "").casefold() == "replay"
            and isinstance(metadata, Mapping)
            and metadata.get("acceptance_replay") is True
        ):
            evidence_run = _uuid(
                metadata.get("acceptance_replay_source_run_id"),
                "acceptance_replay_source_run_id",
            )
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT target.ordinal, target.target_id,
                       target.logical_refresh_id, target.status,
                       frontier.page_kind, frontier.canonical_url,
                       frontier.source_ids, attempt.attempt_id,
                       attempt.attempt_number, attempt.http_status,
                       attempt.raw_manifest_key, attempt.content_hash,
                       attempt.http_request_count, attempt.wire_bytes,
                       attempt.decoded_bytes, attempt.compressed_bytes,
                       attempt.provider_billed_bytes, attempt.finished_at
                FROM fbref_control.run_target AS target
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = target.target_id
                LEFT JOIN LATERAL (
                  SELECT candidate.*
                  FROM fbref_control.fetch_attempt AS candidate
                  WHERE candidate.logical_refresh_id = target.logical_refresh_id
                  ORDER BY candidate.attempt_number DESC,
                           candidate.attempt_id DESC
                  LIMIT 1
                ) AS attempt ON true
                WHERE target.run_id = %s
                ORDER BY target.ordinal
                """,
                (evidence_run,),
            )
            targets = []
            for row in _fetchall(cursor):
                source_ids = row.get("source_ids") or {}
                if isinstance(source_ids, str):
                    source_ids = json.loads(source_ids)
                targets.append(
                    {
                        "ordinal": int(row["ordinal"]),
                        "target_id": str(row["target_id"]),
                        "logical_refresh_id": str(row["logical_refresh_id"]),
                        "status": str(row["status"]),
                        "page_kind": str(row["page_kind"]),
                        "canonical_url": str(row["canonical_url"]),
                        "source_ids": dict(source_ids),
                        "attempt_id": (
                            None
                            if row.get("attempt_id") is None
                            else str(row["attempt_id"])
                        ),
                        "attempt_number": (
                            None
                            if row.get("attempt_number") is None
                            else int(row["attempt_number"])
                        ),
                        "http_status": (
                            None
                            if row.get("http_status") is None
                            else int(row["http_status"])
                        ),
                        "raw_manifest_key": row.get("raw_manifest_key"),
                        "content_hash": row.get("content_hash"),
                        "http_request_count": int(
                            row.get("http_request_count") or 0
                        ),
                        "wire_bytes": int(row.get("wire_bytes") or 0),
                        "decoded_bytes": int(row.get("decoded_bytes") or 0),
                        "compressed_bytes": int(
                            row.get("compressed_bytes") or 0
                        ),
                        "provider_billed_bytes": (
                            None
                            if row.get("provider_billed_bytes") is None
                            else int(row["provider_billed_bytes"])
                        ),
                        "finished_at": row.get("finished_at"),
                        "evidence_class": None,
                    }
                )
            cursor.execute(
                """
                WITH latest_success AS (
                  SELECT DISTINCT ON (target.logical_refresh_id)
                         target.ordinal, target.target_id,
                         frontier.page_kind, attempt.content_hash,
                         target.logical_refresh_id
                  FROM fbref_control.run_target AS target
                  JOIN fbref_control.page_frontier AS frontier
                    ON frontier.target_id = target.target_id
                  JOIN fbref_control.fetch_attempt AS attempt
                    ON attempt.logical_refresh_id = target.logical_refresh_id
                  WHERE target.run_id = %s
                    AND attempt.status = 'succeeded'
                    AND attempt.content_hash IS NOT NULL
                  ORDER BY target.logical_refresh_id,
                           attempt.attempt_number DESC, attempt.attempt_id DESC
                )
                SELECT latest.ordinal, latest.target_id, latest.page_kind,
                       manifest.parser_version, manifest.dataset,
                       manifest.availability, manifest.parse_status,
                       manifest.persistence_status,
                       manifest.validation_status, manifest.row_count,
                       manifest.manifest_key, manifest.error_class,
                       manifest.error_message, manifest.completed_at
                FROM latest_success AS latest
                JOIN fbref_control.dataset_manifest AS manifest
                  ON manifest.target_id = latest.target_id
                 AND manifest.content_hash = latest.content_hash
                WHERE manifest.parser_version IN (%s, %s)
                  AND EXISTS (
                    SELECT 1
                    FROM fbref_control.observation_processing AS observed
                    WHERE observed.logical_refresh_id = latest.logical_refresh_id
                      AND observed.parser_version = %s
                      AND observed.typed_parser_version = %s
                      AND observed.stateful_parser_version = %s
                      AND observed.status = 'succeeded'
                      AND observed.generic_status = 'succeeded'
                      AND observed.typed_status IN ('succeeded', 'skipped')
                      AND observed.stateful_status IN ('succeeded', 'skipped')
                      AND observed.validation_status = 'succeeded'
                  )
                ORDER BY latest.ordinal, manifest.parser_version,
                         manifest.dataset
                """,
                (
                    evidence_run,
                    PAGE_DOCUMENT_VERSION,
                    TYPED_BRONZE_PARSER_VERSION,
                    PAGE_DOCUMENT_VERSION,
                    TYPED_BRONZE_PARSER_VERSION,
                    DISCOVERY_PARSER_VERSION,
                ),
            )
            datasets = [
                {
                    "ordinal": int(row["ordinal"]),
                    "target_id": str(row["target_id"]),
                    "page_kind": str(row["page_kind"]),
                    "parser_version": str(row["parser_version"]),
                    "dataset": str(row["dataset"]),
                    "availability": str(row["availability"]),
                    "parse_status": str(row["parse_status"]),
                    "persistence_status": str(row["persistence_status"]),
                    "validation_status": str(row["validation_status"]),
                    "row_count": int(row["row_count"]),
                    "manifest_key": row.get("manifest_key"),
                    "error_class": row.get("error_class"),
                    "error_message": row.get("error_message"),
                    "empty_reason": (
                        row.get("error_message")
                        if str(row["availability"]) in {
                            "empty", "restricted", "not_applicable"
                        }
                        else None
                    ),
                    "completed_at": row.get("completed_at"),
                }
                for row in _fetchall(cursor)
            ]
        datasets_by_target: dict[str, list[dict]] = {}
        for dataset in datasets:
            datasets_by_target.setdefault(dataset["target_id"], []).append(
                dataset
            )
        required_match_datasets = {
            "typed:shot_events",
            "typed:match_events",
            "typed:lineups",
            "typed:match_team_stats",
            "typed:match_managers",
            "typed:match_officials",
            "typed:match_keeper_stats",
            "typed:match_player_stats",
        }
        explicit_empty = {"empty", "restricted", "not_applicable"}
        for target in targets:
            manifests = datasets_by_target.get(target["target_id"], [])
            if target["page_kind"] == "player":
                pages = [
                    item for item in manifests if item["dataset"] == "__page__"
                ]
                if pages and all(
                    item["availability"] == "available"
                    and item["row_count"] > 0
                    and item["parse_status"] == "succeeded"
                    and item["persistence_status"] == "succeeded"
                    and item["validation_status"] == "succeeded"
                    for item in pages
                ):
                    target["evidence_class"] = "populated_player"
                elif pages and all(
                    item["availability"] == "empty"
                    and item["row_count"] == 0
                    and bool(str(item.get("empty_reason") or "").strip())
                    and item["parse_status"] == "succeeded"
                    and item["persistence_status"] == "succeeded"
                    and item["validation_status"] == "succeeded"
                    for item in pages
                ):
                    target["evidence_class"] = "empty_player"
            elif target["page_kind"] == "match":
                typed = {
                    item["dataset"]: item
                    for item in manifests
                    if item["dataset"] in required_match_datasets
                }
                complete = next(
                    (
                        item
                        for item in manifests
                        if item["dataset"] == "typed:__complete__"
                    ),
                    None,
                )
                safe = (
                    set(typed) == required_match_datasets
                    and complete is not None
                    and all(
                        item["availability"] not in {"unknown", "error"}
                        and item["parse_status"] == "succeeded"
                        and item["persistence_status"]
                        in {"succeeded", "skipped"}
                        and item["validation_status"]
                        in {"succeeded", "skipped"}
                        and (
                            item["availability"] not in explicit_empty
                            or bool(
                                str(item.get("empty_reason") or "").strip()
                            )
                        )
                        for item in typed.values()
                    )
                    and complete["parse_status"] == "succeeded"
                    and complete["persistence_status"] == "succeeded"
                    and complete["validation_status"] == "succeeded"
                )
                player_stats = typed.get("typed:match_player_stats")
                if safe and player_stats is not None:
                    if (
                        player_stats["availability"] == "available"
                        and player_stats["row_count"] > 0
                    ):
                        target["evidence_class"] = "full_match"
                    elif (
                        player_stats["availability"] in explicit_empty
                        and player_stats["row_count"] == 0
                        and bool(
                            str(player_stats.get("empty_reason") or "").strip()
                        )
                    ):
                        target["evidence_class"] = "sparse_match"
        return {
            "summary": summary,
            "targets": targets,
            "datasets": datasets,
            "processing_control_run_id": run,
            "evidence_control_run_id": evidence_run,
        }

    def list_successful_fetch_attempts(
        self,
        run_id: object,
        *,
        limit: int = 250,
        after: Optional[tuple[int, int, str]] = None,
    ) -> list[dict]:
        """Page successful raw evidence in stable run-cohort order."""
        run = _uuid(run_id, "run_id")
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        if after is None:
            after_ordinal = None
            after_attempt_number = None
            after_attempt_id = None
        else:
            if len(after) != 3:
                raise ValueError(
                    "after must be (ordinal, attempt_number, attempt_id)"
                )
            after_ordinal = _non_negative(after[0], "after ordinal")
            after_attempt_number = int(after[1])
            if after_attempt_number <= 0:
                raise ValueError("after attempt_number must be positive")
            after_attempt_id = _uuid(after[2], "after attempt_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT attempt.attempt_id, attempt.run_id,
                       attempt.target_id, attempt.logical_refresh_id,
                       attempt.attempt_number, target.ordinal,
                       frontier.page_kind, frontier.canonical_url,
                       frontier.source_ids, attempt.http_status,
                       attempt.content_hash, attempt.raw_manifest_key,
                       attempt.decoded_bytes, attempt.compressed_bytes,
                       attempt.wire_bytes, attempt.provider_billed_bytes,
                       attempt.http_request_count,
                       attempt.http_status_history, attempt.etag,
                       attempt.last_modified, attempt.transport_version,
                       attempt.session_version, attempt.latency_ms,
                       attempt.started_at, attempt.finished_at
                FROM fbref_control.fetch_attempt AS attempt
                JOIN fbref_control.run_target AS target
                  ON target.run_id = attempt.run_id
                 AND target.target_id = attempt.target_id
                 AND target.logical_refresh_id = attempt.logical_refresh_id
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = attempt.target_id
                WHERE attempt.run_id = %s
                  AND attempt.status = 'succeeded'
                  AND (
                    %s::bigint IS NULL
                    OR (
                      target.ordinal, attempt.attempt_number,
                      attempt.attempt_id
                    ) > (%s::bigint, %s::integer, %s::uuid)
                  )
                ORDER BY target.ordinal, attempt.attempt_number,
                         attempt.attempt_id
                LIMIT %s
                """,
                (
                    run,
                    after_ordinal,
                    after_ordinal,
                    after_attempt_number,
                    after_attempt_id,
                    normalized_limit,
                ),
            )
            rows = _fetchall(cursor)
        for row in rows:
            for key in (
                "attempt_id",
                "run_id",
                "logical_refresh_id",
            ):
                row[key] = str(row[key])
            source_ids = row.get("source_ids") or {}
            if isinstance(source_ids, str):
                source_ids = json.loads(source_ids)
            row["source_ids"] = dict(source_ids)
        return rows

    def list_fetch_attempts_for_refresh(
        self,
        run_id: object,
        logical_refresh_id: object,
    ) -> list[dict]:
        """Return raw-evidence lineage for one run-local logical refresh.

        A zero-network ``raw-recovery`` success intentionally has no network
        counters of its own.  Raw acceptance uses this bounded lineage to
        match the immutable manifest's original attempt and audit the counters
        that were reattributed to it transactionally by ``complete_fetch``.
        """

        run = _uuid(run_id, "run_id")
        refresh = _uuid(logical_refresh_id, "logical_refresh_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT attempt_id, run_id, target_id, logical_refresh_id,
                       attempt_number, status, http_status, content_hash,
                       raw_manifest_key, decoded_bytes, compressed_bytes,
                       wire_bytes, provider_billed_bytes, http_request_count,
                       http_status_history, etag, last_modified,
                       transport_version, session_version, latency_ms,
                       started_at, finished_at
                FROM fbref_control.fetch_attempt
                WHERE run_id = %s AND logical_refresh_id = %s
                ORDER BY attempt_number, attempt_id
                """,
                (run, refresh),
            )
            rows = _fetchall(cursor)
        for row in rows:
            for key in ("attempt_id", "run_id", "logical_refresh_id"):
                row[key] = str(row[key])
        return rows

    def get_observation_cleanup_evidence(
        self,
        logical_refresh_id: object,
    ) -> Optional[dict]:
        """Return DB-clock ownership evidence for safe staging cleanup."""
        refresh = _uuid(logical_refresh_id, "logical_refresh_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT target.run_id, run.status AS run_status,
                       EXISTS (
                         SELECT 1
                         FROM fbref_control.page_frontier AS frontier
                         WHERE frontier.target_id = target.target_id
                           AND frontier.lease_run_id = target.run_id
                           AND frontier.lease_refresh_id =
                               target.logical_refresh_id
                           AND frontier.state = 'leased'
                           AND frontier.claim_token IS NOT NULL
                           AND frontier.lease_expires_at > clock_timestamp()
                       ) AS active_fetch_lease,
                       EXISTS (
                         SELECT 1
                         FROM fbref_control.budget_reservation AS reservation
                         WHERE reservation.run_id = target.run_id
                           AND reservation.logical_refresh_id =
                               target.logical_refresh_id
                           AND reservation.status = 'reserved'
                       ) AS active_budget_reservation,
                       EXISTS (
                         SELECT 1
                         FROM fbref_control.observation_processing
                              AS processing
                         WHERE processing.logical_refresh_id =
                               target.logical_refresh_id
                           AND processing.status = 'processing'
                           AND processing.claim_token IS NOT NULL
                       ) AS active_observation_processing
                FROM fbref_control.run_target AS target
                JOIN fbref_control.crawl_run AS run
                  ON run.run_id = target.run_id
                WHERE target.logical_refresh_id = %s
                """,
                (refresh,),
            )
            row = _fetchone(cursor)
        if row is not None:
            row["run_id"] = str(row["run_id"])
            for key in (
                "active_fetch_lease",
                "active_budget_reservation",
                "active_observation_processing",
            ):
                row[key] = bool(row[key])
        return row

    def list_unprocessed_fetches(
        self,
        *,
        parser_version: object,
        typed_parser_version: object,
        stateful_parser_version: object,
        source: str = "fbref",
        page_kinds: Optional[Sequence[str]] = None,
        limit: int = 25,
    ) -> list[dict]:
        """Return global raw observations missing the exact successful parse.

        Selection is intentionally independent of the source crawl-run status:
        a successful immutable raw commit remains recoverable when a later task
        made its parent run fail or get cancelled.  Oldest raw is drained first
        so repeated bounded calls cannot starve earlier observations.
        """
        parser = _text(parser_version, "parser_version")
        typed_parser = _text(typed_parser_version, "typed_parser_version")
        stateful_parser = _text(
            stateful_parser_version, "stateful_parser_version"
        )
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        kinds = (
            None
            if page_kinds is None
            else sorted({_text(kind, "page_kind") for kind in page_kinds})
        )
        if kinds == []:
            return []
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT attempt.attempt_id, attempt.run_id,
                       source_run.status AS source_run_status,
                       source_run.run_type AS source_run_type,
                       attempt.target_id, attempt.logical_refresh_id,
                       attempt.content_hash, attempt.raw_manifest_key,
                       attempt.http_status, attempt.started_at,
                       attempt.finished_at, frontier.page_kind,
                       frontier.canonical_url, frontier.source_ids
                FROM fbref_control.fetch_attempt AS attempt
                JOIN fbref_control.crawl_run AS source_run
                  ON source_run.run_id = attempt.run_id
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = attempt.target_id
                WHERE frontier.source = %s
                  AND attempt.status = 'succeeded'
                  AND attempt.raw_manifest_key IS NOT NULL
                  AND attempt.content_hash IS NOT NULL
                  AND (%s::text[] IS NULL OR frontier.page_kind = ANY(%s))
                  AND NOT EXISTS (
                    SELECT 1
                    FROM fbref_control.observation_processing AS observed
                    WHERE observed.logical_refresh_id =
                          attempt.logical_refresh_id
                      AND observed.parser_version = %s
                      AND observed.typed_parser_version = %s
                      AND observed.stateful_parser_version = %s
                      AND observed.status = 'succeeded'
                      AND observed.generic_status = 'succeeded'
                      AND observed.typed_status IN ('succeeded', 'skipped')
                      AND observed.stateful_status IN (
                          'succeeded', 'skipped'
                      )
                      AND observed.validation_status = 'succeeded'
                  )
                ORDER BY COALESCE(
                    attempt.finished_at, attempt.started_at
                ), attempt.attempt_id
                LIMIT %s
                """,
                (
                    _text(source, "source"),
                    kinds,
                    kinds,
                    parser,
                    typed_parser,
                    stateful_parser,
                    normalized_limit,
                ),
            )
            return _fetchall(cursor)

    def get_frontier_target(self, target_id: object) -> Optional[dict]:
        """Return fetch validators and latest content for conditional requests."""
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT target_id, source, page_kind, canonical_url, source_ids,
                       refresh_policy, state, priority, next_fetch_at,
                       retry_after, last_fetched_at, last_http_status,
                       last_content_hash, last_etag, last_modified,
                       last_error_class, last_error_message
                FROM fbref_control.page_frontier
                WHERE target_id = %s
                """,
                (_text(target_id, "target_id"),),
            )
            return _fetchone(cursor)

    @contextmanager
    def guard_latest_content(
        self,
        target_id: object,
        content_hash: object,
        logical_refresh_id: object,
    ):
        """Fence a typed write to the frontier's newest committed content.

        ``FOR NO KEY UPDATE`` blocks claims and fetch completions for this one
        target while allowing dataset-manifest foreign-key checks on the same
        row.  The lock therefore spans the external typed Bronze write without
        blocking its completion markers.  Both the digest and the immutable
        logical-refresh identity must match: content can repeat after a 304 or
        an A -> B -> A transition.
        """

        target = _text(target_id, "target_id")
        expected_hash = _text(content_hash, "content_hash")
        expected_refresh = _uuid(logical_refresh_id, "logical_refresh_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT state, last_content_hash
                FROM fbref_control.page_frontier
                WHERE target_id = %s
                FOR NO KEY UPDATE
                """,
                (target,),
            )
            row = _fetchone(cursor)
            if row is None or row["state"] == "leased":
                # ``None`` means retry later: absence or an in-flight refresh
                # is not evidence that this raw observation is stale.
                yield None
            else:
                cursor.execute(
                    """
                    SELECT logical_refresh_id, content_hash
                    FROM fbref_control.fetch_attempt
                    WHERE target_id = %s AND status = 'succeeded'
                    ORDER BY lease_epoch DESC, attempt_number DESC
                    LIMIT 1
                    """,
                    (target,),
                )
                latest = _fetchone(cursor)
                yield (
                    latest is not None
                    and str(latest["logical_refresh_id"]) == expected_refresh
                    and str(latest.get("content_hash") or "") == expected_hash
                    and str(row.get("last_content_hash") or "")
                    == expected_hash
                )

    def claim_observation_processing(
        self,
        *,
        logical_refresh_id: object,
        target_id: object,
        content_hash: object,
        parser_version: object,
        typed_parser_version: object,
        stateful_parser_version: object,
        lease_seconds: int = 3600,
    ) -> Optional[ObservationLease]:
        """Claim one parser-version fence for one successful observation."""

        refresh = _uuid(logical_refresh_id, "logical_refresh_id")
        target = _text(target_id, "target_id")
        digest = _text(content_hash, "content_hash")
        parser = _text(parser_version, "parser_version")
        typed_parser = _text(typed_parser_version, "typed_parser_version")
        stateful_parser = _text(
            stateful_parser_version, "stateful_parser_version"
        )
        seconds = int(lease_seconds)
        if not 1 <= seconds <= 24 * 60 * 60:
            raise ValueError("lease_seconds must be between 1 and 86400")
        token = str(uuid.uuid4())
        with self._transaction() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.observation_processing (
                    logical_refresh_id, parser_version,
                    typed_parser_version, stateful_parser_version,
                    target_id, content_hash
                )
                SELECT target.logical_refresh_id, %s, %s, %s,
                       target.target_id, attempt.content_hash
                FROM fbref_control.run_target AS target
                JOIN fbref_control.fetch_attempt AS attempt
                  ON attempt.run_id = target.run_id
                 AND attempt.target_id = target.target_id
                 AND attempt.logical_refresh_id = target.logical_refresh_id
                WHERE target.logical_refresh_id = %s
                  AND target.target_id = %s
                  AND attempt.content_hash = %s
                  AND attempt.status = 'succeeded'
                ON CONFLICT (
                    logical_refresh_id, parser_version, typed_parser_version,
                    stateful_parser_version
                ) DO NOTHING
                """,
                (
                    parser,
                    typed_parser,
                    stateful_parser,
                    refresh,
                    target,
                    digest,
                ),
            )
            cursor.execute(
                """
                SELECT *, (
                    status = 'processing'
                    AND lease_expires_at > clock_timestamp()
                ) AS active_claim
                FROM fbref_control.observation_processing
                WHERE logical_refresh_id = %s AND parser_version = %s
                  AND typed_parser_version = %s
                  AND stateful_parser_version = %s
                FOR UPDATE
                """,
                (refresh, parser, typed_parser, stateful_parser),
            )
            row = _fetchone(cursor)
            if row is None:
                raise StateConflict(
                    "Observation fence requires matching successful fetch evidence"
                )
            if row["target_id"] != target or row["content_hash"] != digest:
                raise StateConflict("Observation fence evidence is immutable")
            if row["status"] == "succeeded" or bool(row["active_claim"]):
                return None
            cursor.execute(
                """
                UPDATE fbref_control.observation_processing
                SET status = 'processing', claim_token = %s,
                    lease_expires_at = clock_timestamp()
                        + (%s * interval '1 second'),
                    error_class = NULL, error_message = NULL,
                    started_at = COALESCE(started_at, clock_timestamp()),
                    updated_at = clock_timestamp()
                WHERE logical_refresh_id = %s AND parser_version = %s
                  AND typed_parser_version = %s
                  AND stateful_parser_version = %s
                RETURNING lease_expires_at
                """,
                (
                    token,
                    seconds,
                    refresh,
                    parser,
                    typed_parser,
                    stateful_parser,
                ),
            )
            claimed = _fetchone(cursor)
            if claimed is None:
                raise LeaseLost(f"Observation claim lost for {refresh}")
            return ObservationLease(
                logical_refresh_id=refresh,
                target_id=target,
                content_hash=digest,
                parser_version=parser,
                typed_parser_version=typed_parser,
                stateful_parser_version=stateful_parser,
                claim_token=token,
                lease_expires_at=claimed["lease_expires_at"],
            )

    def complete_observation_processing(
        self,
        lease: ObservationLease,
        *,
        typed_status: str,
        stateful_status: str,
    ) -> None:
        """Close an observation only after all stateful effects succeeded."""

        typed = str(typed_status).strip().lower()
        stateful = str(stateful_status).strip().lower()
        if typed not in {"succeeded", "skipped"}:
            raise ValueError("typed_status must be succeeded or skipped")
        if stateful not in {"succeeded", "skipped"}:
            raise ValueError("stateful_status must be succeeded or skipped")
        identity = (
            lease.logical_refresh_id,
            lease.parser_version,
            lease.typed_parser_version,
            lease.stateful_parser_version,
        )
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.observation_processing
                SET status = 'succeeded', generic_status = 'succeeded',
                    typed_status = %s, stateful_status = %s,
                    validation_status = 'succeeded', claim_token = NULL,
                    lease_expires_at = NULL, error_class = NULL,
                    error_message = NULL,
                    completed_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE logical_refresh_id = %s AND parser_version = %s
                  AND typed_parser_version = %s
                  AND stateful_parser_version = %s
                  AND status = 'processing' AND claim_token = %s
                """,
                (typed, stateful, *identity, lease.claim_token),
            )
            if cursor.rowcount:
                return
            cursor.execute(
                """
                SELECT status
                FROM fbref_control.observation_processing
                WHERE logical_refresh_id = %s AND parser_version = %s
                  AND typed_parser_version = %s
                  AND stateful_parser_version = %s
                """,
                identity,
            )
            row = _fetchone(cursor)
            if row is not None and row["status"] == "succeeded":
                return
            raise LeaseLost(
                f"Observation completion lease lost for {lease.logical_refresh_id}"
            )

    def fail_observation_processing(
        self,
        lease: ObservationLease,
        *,
        error_class: object,
        error_message: object,
    ) -> None:
        """Release a failed observation claim so a later wave can retry it."""

        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.observation_processing
                SET status = 'failed', claim_token = NULL,
                    lease_expires_at = NULL, error_class = %s,
                    error_message = %s, updated_at = clock_timestamp()
                WHERE logical_refresh_id = %s AND parser_version = %s
                  AND typed_parser_version = %s
                  AND stateful_parser_version = %s
                  AND status = 'processing' AND claim_token = %s
                """,
                (
                    str(error_class)[:200],
                    str(error_message)[:2000],
                    lease.logical_refresh_id,
                    lease.parser_version,
                    lease.typed_parser_version,
                    lease.stateful_parser_version,
                    lease.claim_token,
                ),
            )
            if not cursor.rowcount:
                raise LeaseLost(
                    f"Observation failure lease lost for {lease.logical_refresh_id}"
                )

    def list_run_fetches(
        self,
        run_id: object,
        *,
        page_kinds: Optional[Sequence[str]] = None,
        only_unparsed: bool = False,
        parser_version: Optional[str] = None,
        typed_parser_version: Optional[str] = None,
        stateful_parser_version: Optional[str] = None,
        limit: int = 25,
    ) -> list[dict]:
        """Return a bounded offline-parse handoff for successful fetches.

        Pipeline work supplies both parser versions and is fenced by immutable
        ``logical_refresh_id``.  The parser-only form is retained for focused
        offline remediation jobs whose content manifest is their completion
        key.
        """
        run = _uuid(run_id, "run_id")
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 25:
            raise ValueError("limit must be between 1 and 25")
        kinds = None
        if page_kinds is not None:
            kinds = sorted({_text(kind, "page_kind") for kind in page_kinds})
            if not kinds:
                return []
        if (typed_parser_version is None) != (
            stateful_parser_version is None
        ):
            raise ValueError(
                "typed and stateful parser versions must be supplied together"
            )
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (target.ordinal)
                       attempt.target_id, frontier.page_kind,
                       frontier.canonical_url, frontier.source_ids,
                       attempt.content_hash, attempt.raw_manifest_key,
                       attempt.http_status, attempt.finished_at AS fetched_at,
                       target.logical_refresh_id, attempt.attempt_id
                FROM fbref_control.run_target AS target
                JOIN fbref_control.fetch_attempt AS attempt
                  ON attempt.run_id = target.run_id
                 AND attempt.target_id = target.target_id
                 AND attempt.logical_refresh_id = target.logical_refresh_id
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = target.target_id
                WHERE target.run_id = %s AND attempt.status = 'succeeded'
                  AND (%s::text[] IS NULL
                       OR frontier.page_kind = ANY(%s::text[]))
                  AND (
                      NOT %s
                      OR (
                        %s::text IS NOT NULL
                        AND NOT EXISTS (
                          SELECT 1
                          FROM fbref_control.observation_processing AS observed
                          WHERE observed.logical_refresh_id =
                                attempt.logical_refresh_id
                            AND observed.parser_version = %s
                            AND observed.typed_parser_version = %s
                            AND observed.stateful_parser_version = %s
                            AND (
                                observed.status = 'succeeded'
                                OR (
                                    observed.status = 'processing'
                                    AND observed.lease_expires_at >
                                        clock_timestamp()
                                )
                            )
                        )
                      )
                      OR (
                        %s::text IS NULL
                        AND NOT EXISTS (
                          SELECT 1
                          FROM fbref_control.dataset_manifest AS manifest
                          WHERE manifest.target_id = attempt.target_id
                            AND manifest.content_hash = attempt.content_hash
                            AND (%s::text IS NULL
                                 OR manifest.parser_version = %s)
                            AND manifest.dataset = '__page__'
                            AND manifest.parse_status = 'succeeded'
                            AND manifest.persistence_status = 'succeeded'
                            AND manifest.validation_status = 'succeeded'
                        )
                      )
                  )
                ORDER BY target.ordinal, attempt.attempt_number DESC
                LIMIT %s
                """,
                (
                    run,
                    kinds,
                    kinds,
                    bool(only_unparsed),
                    typed_parser_version,
                    parser_version,
                    typed_parser_version,
                    stateful_parser_version,
                    typed_parser_version,
                    parser_version,
                    parser_version,
                    normalized_limit,
                ),
            )
            return _fetchall(cursor)

    def list_replay_fetches(
        self,
        source_run_id: object,
        *,
        parser_version: object,
        typed_parser_version: Optional[object] = None,
        stateful_parser_version: Optional[object] = None,
        page_kinds: Optional[Sequence[str]] = None,
        limit: int = 25,
    ) -> list[dict]:
        """Return raw work missing either generic or typed parser evidence."""
        return self.list_run_fetches(
            source_run_id,
            page_kinds=page_kinds,
            only_unparsed=True,
            parser_version=_text(parser_version, "parser_version"),
            typed_parser_version=(
                None
                if typed_parser_version is None
                else _text(typed_parser_version, "typed_parser_version")
            ),
            stateful_parser_version=(
                None
                if stateful_parser_version is None
                else _text(
                    stateful_parser_version, "stateful_parser_version"
                )
            ),
            limit=limit,
        )

    @staticmethod
    def _settle_reserved_rows_conservatively(
        cursor: Any,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        """Charge abandoned reservations after their run rows are locked."""

        totals: dict[str, dict[str, int]] = {}
        for row in rows:
            reservation = _budget_from_row(row)
            if reservation.status != "reserved":
                continue
            cursor.execute(
                """
                UPDATE fbref_control.budget_reservation
                SET status = 'settled',
                    requests_used = requests_reserved,
                    bytes_used = bytes_reserved,
                    settled_at = clock_timestamp()
                WHERE reservation_id = %s AND status = 'reserved'
                """,
                (reservation.reservation_id,),
            )
            if cursor.rowcount != 1:
                raise StateConflict(
                    f"Reservation {reservation.reservation_id} "
                    "lost conservative settlement race"
                )
            run_totals = totals.setdefault(
                reservation.run_id,
                {"requests": 0, "bytes": 0, "count": 0},
            )
            run_totals["requests"] += reservation.requests_reserved
            run_totals["bytes"] += reservation.bytes_reserved
            run_totals["count"] += 1

        for run_id, run_totals in totals.items():
            requests = run_totals["requests"]
            bytes_ = run_totals["bytes"]
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET requests_reserved = requests_reserved - %s,
                    bytes_reserved = bytes_reserved - %s,
                    requests_used = requests_used + %s,
                    bytes_used = bytes_used + %s,
                    budget_exceeded = budget_exceeded
                        OR requests_used + %s > request_limit
                        OR bytes_used + %s > byte_limit,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                  AND requests_reserved >= %s
                  AND bytes_reserved >= %s
                """,
                (
                    requests,
                    bytes_,
                    requests,
                    bytes_,
                    requests,
                    bytes_,
                    run_id,
                    requests,
                    bytes_,
                ),
            )
            if cursor.rowcount != 1:
                raise StateConflict(
                    f"Run {run_id} cannot settle abandoned reservations"
                )
        return sum(item["count"] for item in totals.values())

    @classmethod
    def _reap_expired(
        cls,
        cursor: Any,
        *,
        run_ids: Optional[Sequence[str]] = None,
        run_rows_locked: bool = False,
    ) -> int:
        # Discover candidates without locking. The second frontier query
        # revalidates expiry after all owning run rows have been locked, so a
        # concurrent heartbeat either wins before FOR UPDATE or loses its
        # exact fenced lease after the reaper locks it.
        if run_ids is None:
            cursor.execute(
                """
                SELECT DISTINCT lease_run_id AS run_id
                FROM fbref_control.page_frontier
                WHERE state = 'leased'
                  AND lease_expires_at <= clock_timestamp()
                ORDER BY lease_run_id
                """
            )
            normalized_run_ids = sorted(
                str(row["run_id"]) for row in _fetchall(cursor)
            )
        else:
            normalized_run_ids = sorted(
                {_uuid(run_id, "run_id") for run_id in run_ids}
            )
        if not normalized_run_ids:
            return 0
        if not run_rows_locked:
            cursor.execute(
                """
                SELECT run_id
                FROM fbref_control.crawl_run
                WHERE run_id = ANY(%s::uuid[])
                ORDER BY run_id
                FOR UPDATE
                """,
                (normalized_run_ids,),
            )
            locked_run_ids = {
                str(row["run_id"]) for row in _fetchall(cursor)
            }
            if locked_run_ids != set(normalized_run_ids):
                raise StateConflict(
                    "Expired leases reference a missing crawl run"
                )
        cursor.execute(
            """
            SELECT target_id, claim_token, lease_epoch,
                   lease_run_id, lease_refresh_id
            FROM fbref_control.page_frontier
            WHERE state = 'leased'
              AND lease_run_id = ANY(%s::uuid[])
              AND lease_expires_at <= clock_timestamp()
            ORDER BY lease_run_id, target_id
            FOR UPDATE
            """,
            (normalized_run_ids,),
        )
        expired_frontiers = _fetchall(cursor)
        reaped = 0
        for frontier in expired_frontiers:
            target_id = str(frontier["target_id"])
            claim_token = str(frontier["claim_token"])
            lease_epoch = int(frontier["lease_epoch"])
            run_id = str(frontier["lease_run_id"])
            refresh_id = str(frontier["lease_refresh_id"])

            # The frontier row remains locked for the whole transaction, so
            # a heartbeat cannot revive this exact token while its budget is
            # being charged and its state transitions are committed.
            cursor.execute(
                """
                SELECT reservation.*
                FROM fbref_control.budget_reservation AS reservation
                WHERE reservation.run_id = %s
                  AND reservation.logical_refresh_id = %s
                  AND reservation.status = 'reserved'
                ORDER BY reservation.reservation_id
                FOR UPDATE
                """,
                (run_id, refresh_id),
            )
            cls._settle_reserved_rows_conservatively(
                cursor, _fetchall(cursor)
            )
            cursor.execute(
                """
                UPDATE fbref_control.fetch_attempt
                SET status = 'expired', finished_at = clock_timestamp(),
                    error_class = 'LeaseExpired',
                    error_message = 'Worker lease expired before completion'
                WHERE run_id = %s AND target_id = %s
                  AND logical_refresh_id = %s AND claim_token = %s
                  AND lease_epoch = %s AND status = 'claimed'
                """,
                (
                    run_id,
                    target_id,
                    refresh_id,
                    claim_token,
                    lease_epoch,
                ),
            )
            cursor.execute(
                """
                UPDATE fbref_control.run_target
                SET status = 'retry', updated_at = clock_timestamp()
                WHERE run_id = %s AND target_id = %s
                  AND logical_refresh_id = %s AND status = 'leased'
                """,
                (run_id, target_id, refresh_id),
            )
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier
                SET state = 'retry', retry_after = clock_timestamp(),
                    claim_token = NULL, lease_run_id = NULL,
                    lease_refresh_id = NULL, leased_by = NULL,
                    lease_expires_at = NULL, updated_at = clock_timestamp()
                WHERE target_id = %s AND state = 'leased'
                  AND claim_token = %s AND lease_epoch = %s
                  AND lease_run_id = %s AND lease_refresh_id = %s
                """,
                (
                    target_id,
                    claim_token,
                    lease_epoch,
                    run_id,
                    refresh_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateConflict(
                    f"Expired lease changed while reaping {target_id}"
                )
            reaped += 1
        return reaped

    def reap_expired_leases(self) -> int:
        with self._transaction() as cursor:
            return self._reap_expired(cursor)

    def abort_run(
        self,
        run_id: object,
        *,
        error_class: object = "RunAborted",
        error_message: object = "Control run aborted by its orchestrator",
    ) -> dict:
        """Fail a live run, settle its reservations, and release all leases.

        The operation is idempotent. A run already marked ``succeeded`` is
        never downgraded when a downstream Airflow task fails.
        """

        run = _uuid(run_id, "run_id")
        normalized_class = _text(error_class, "error_class")
        normalized_message = _text(error_message, "error_message")[:4000]
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run,),
            )
            locked = _fetchone(cursor)
            if locked is None:
                return {
                    "run_id": run,
                    "status": "missing",
                    "aborted": False,
                    "reservations_settled": 0,
                    "targets_released": 0,
                }
            if locked["status"] == "succeeded":
                return {
                    "run_id": run,
                    "status": "succeeded",
                    "aborted": False,
                    "reservations_settled": 0,
                    "targets_released": 0,
                }

            cursor.execute(
                """
                SELECT target_id
                FROM fbref_control.page_frontier
                WHERE state = 'leased' AND lease_run_id = %s
                ORDER BY target_id
                FOR UPDATE
                """,
                (run,),
            )
            _fetchall(cursor)
            cursor.execute(
                """
                SELECT * FROM fbref_control.budget_reservation
                WHERE run_id = %s AND status = 'reserved'
                ORDER BY reservation_id
                FOR UPDATE
                """,
                (run,),
            )
            reserved_rows = _fetchall(cursor)
            reservations_settled = self._settle_reserved_rows_conservatively(
                cursor, reserved_rows
            )
            cursor.execute(
                """
                UPDATE fbref_control.fetch_attempt
                SET status = 'failed', error_class = %s,
                    error_message = %s,
                    heartbeat_at = clock_timestamp(),
                    finished_at = clock_timestamp()
                WHERE run_id = %s AND status = 'claimed'
                """,
                (normalized_class, normalized_message, run),
            )
            attempts_failed = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.run_target
                SET status = 'failed', updated_at = clock_timestamp()
                WHERE run_id = %s
                  AND status IN ('pending', 'leased', 'retry')
                """,
                (run,),
            )
            targets_failed = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier
                SET state = 'retry', retry_after = clock_timestamp(),
                    last_error_class = %s, last_error_message = %s,
                    claim_token = NULL, lease_run_id = NULL,
                    lease_refresh_id = NULL, leased_by = NULL,
                    lease_expires_at = NULL, updated_at = clock_timestamp()
                WHERE state = 'leased' AND lease_run_id = %s
                """,
                (normalized_class, normalized_message, run),
            )
            targets_released = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.clearance_session
                SET status = 'failed', closed_at = clock_timestamp()
                WHERE run_id = %s AND status = 'active'
                """,
                (run,),
            )
            sessions_closed = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET status = 'failed', finished_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE run_id = %s AND status IN ('pending', 'running')
                """,
                (run,),
            )
            final_status = (
                "failed"
                if locked["status"] in {"pending", "running", "failed"}
                else str(locked["status"])
            )
            return {
                "run_id": run,
                "status": final_status,
                "aborted": True,
                "reservations_settled": reservations_settled,
                "attempts_failed": attempts_failed,
                "targets_failed": targets_failed,
                "targets_released": targets_released,
                "sessions_closed": sessions_closed,
            }

    def claim_targets(
        self,
        run_id: object,
        worker_id: object,
        *,
        limit: int = 25,
        lease_seconds: int = 300,
        page_kinds: Optional[Sequence[str]] = None,
        refresh_policies: Optional[Sequence[str]] = None,
    ) -> list[TargetLease]:
        """Claim a bounded shard with PostgreSQL ``SKIP LOCKED`` semantics."""
        run = _uuid(run_id, "run_id")
        worker = _text(worker_id, "worker_id")
        normalized_limit = int(limit)
        if not 1 <= normalized_limit <= 25:
            raise ValueError("limit must be between 1 and 25")
        normalized_lease = int(lease_seconds)
        if normalized_lease <= 0:
            raise ValueError("lease_seconds must be positive")
        kinds = None
        if page_kinds is not None:
            kinds = sorted({_text(kind, "page_kind") for kind in page_kinds})
            if not kinds:
                return []
        policies = None
        if refresh_policies is not None:
            policies = sorted(
                {_text(policy, "refresh_policy") for policy in refresh_policies}
            )
            if not policies:
                return []

        leases = []
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status, metadata FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run,),
            )
            crawl_run = _fetchone(cursor)
            if crawl_run is None or crawl_run["status"] != "running":
                return []
            run_metadata = _json_mapping(
                crawl_run.get("metadata") or {}, "crawl run metadata"
            )
            if "raw_fetch_attempt_snapshot" in run_metadata:
                return []
            self._reap_expired(
                cursor,
                run_ids=(run,),
                run_rows_locked=True,
            )
            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                SELECT target.target_id, target.logical_refresh_id,
                       frontier.canonical_url, frontier.page_kind,
                       frontier.source_ids, frontier.lease_epoch
                FROM fbref_control.run_target AS target
                JOIN fbref_control.crawl_run AS run
                  ON run.run_id = target.run_id
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = target.target_id
                LEFT JOIN scope_rollup AS scope
                  ON scope.target_id = frontier.target_id
                WHERE target.run_id = %s AND run.status = 'running'
                  AND target.status IN ('pending', 'retry')
                  AND frontier.state IN ('queued', 'retry')
                  AND (%s::text[] IS NULL
                       OR frontier.page_kind = ANY(%s::text[]))
                  AND (%s::text[] IS NULL
                       OR frontier.refresh_policy = ANY(%s::text[]))
                  AND (
                    frontier.page_kind = 'competition_index'
                    OR (
                      scope.scope_count > 0
                      AND NOT COALESCE(scope.competition_missing, true)
                      AND NOT COALESCE(scope.has_female, false)
                      AND NOT COALESCE(scope.has_unknown, true)
                      AND NOT COALESCE(scope.inactive_competition, true)
                      AND NOT COALESCE(scope.invalid_season, true)
                      AND (
                        COALESCE(scope.has_competition_scope, false)
                        OR COALESCE(scope.has_current_season, false)
                        OR frontier.refresh_policy = 'historical_once'
                      )
                    )
                  )
                  AND (frontier.next_fetch_at IS NULL
                       OR frontier.next_fetch_at <= clock_timestamp())
                  AND (frontier.retry_after IS NULL
                       OR frontier.retry_after <= clock_timestamp())
                ORDER BY frontier.priority DESC, target.ordinal
                LIMIT %s
                FOR UPDATE OF frontier SKIP LOCKED
                """,
                (run, kinds, kinds, policies, policies, normalized_limit),
            )
            candidates = _fetchall(cursor)
            for candidate in candidates:
                token = str(uuid.uuid4())
                attempt_id = str(uuid.uuid4())
                epoch = int(candidate["lease_epoch"]) + 1
                refresh = str(candidate["logical_refresh_id"])
                target_id = str(candidate["target_id"])
                cursor.execute(
                    """
                    UPDATE fbref_control.page_frontier
                    SET state = 'leased', claim_token = %s,
                        lease_epoch = %s, lease_run_id = %s,
                        lease_refresh_id = %s, leased_by = %s,
                        lease_expires_at = clock_timestamp()
                            + (%s * interval '1 second'),
                        retry_after = NULL, updated_at = clock_timestamp()
                    WHERE target_id = %s
                    RETURNING lease_expires_at
                    """,
                    (
                        token,
                        epoch,
                        run,
                        refresh,
                        worker,
                        normalized_lease,
                        target_id,
                    ),
                )
                expiry = _fetchone(cursor)
                cursor.execute(
                    """
                    UPDATE fbref_control.run_target
                    SET status = 'leased', updated_at = clock_timestamp()
                    WHERE run_id = %s AND target_id = %s
                      AND logical_refresh_id = %s
                      AND status IN ('pending', 'retry')
                    """,
                    (run, target_id, refresh),
                )
                if cursor.rowcount != 1 or expiry is None:
                    raise StateConflict(f"Could not claim target {target_id}")
                cursor.execute(
                    """
                    SELECT COALESCE(max(attempt_number), 0) + 1 AS number
                    FROM fbref_control.fetch_attempt
                    WHERE logical_refresh_id = %s
                    """,
                    (refresh,),
                )
                attempt_number = int(_fetchone(cursor)["number"])
                cursor.execute(
                    """
                    INSERT INTO fbref_control.fetch_attempt (
                        attempt_id, run_id, target_id, logical_refresh_id,
                        attempt_number, claim_token, lease_epoch, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'claimed')
                    """,
                    (
                        attempt_id,
                        run,
                        target_id,
                        refresh,
                        attempt_number,
                        token,
                        epoch,
                    ),
                )
                source_ids = candidate["source_ids"]
                if isinstance(source_ids, str):
                    source_ids = json.loads(source_ids)
                leases.append(
                    TargetLease(
                        attempt_id=attempt_id,
                        run_id=run,
                        target_id=target_id,
                        logical_refresh_id=refresh,
                        canonical_url=str(candidate["canonical_url"]),
                        page_kind=str(candidate["page_kind"]),
                        source_ids=dict(source_ids),
                        claim_token=token,
                        lease_epoch=epoch,
                        attempt_number=attempt_number,
                        leased_by=worker,
                        lease_expires_at=expiry["lease_expires_at"],
                    )
                )
        return leases

    def bind_reservation(
        self,
        lease: TargetLease,
        reservation_id: object,
    ) -> None:
        """Attach the pre-request budget reservation to its fenced attempt."""
        reservation = _uuid(reservation_id, "reservation_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT status FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (lease.run_id,),
            )
            run = _fetchone(cursor)
            if run is None or run["status"] != "running":
                raise LeaseLost(f"Run lease lost for {lease.target_id}")
            cursor.execute(
                """
                SELECT target_id FROM fbref_control.page_frontier
                WHERE target_id = %s AND state = 'leased'
                  AND claim_token = %s AND lease_epoch = %s
                  AND lease_run_id = %s AND lease_refresh_id = %s
                FOR UPDATE
                """,
                (
                    lease.target_id,
                    lease.claim_token,
                    lease.lease_epoch,
                    lease.run_id,
                    lease.logical_refresh_id,
                ),
            )
            if _fetchone(cursor) is None:
                raise LeaseLost(f"Lease lost for {lease.target_id}")
            cursor.execute(
                """
                SELECT logical_refresh_id
                FROM fbref_control.budget_reservation
                WHERE reservation_id = %s AND run_id = %s
                  AND logical_refresh_id = %s AND status = 'reserved'
                FOR UPDATE
                """,
                (
                    reservation,
                    lease.run_id,
                    lease.logical_refresh_id,
                ),
            )
            if _fetchone(cursor) is None:
                raise LeaseLost(
                    f"Budget reservation lost for {lease.target_id}"
                )
            cursor.execute(
                """
                UPDATE fbref_control.fetch_attempt
                SET reservation_id = %s
                WHERE attempt_id = %s AND claim_token = %s
                  AND lease_epoch = %s AND status = 'claimed'
                  AND run_id = %s AND target_id = %s
                  AND logical_refresh_id = %s
                """,
                (
                    reservation,
                    lease.attempt_id,
                    lease.claim_token,
                    lease.lease_epoch,
                    lease.run_id,
                    lease.target_id,
                    lease.logical_refresh_id,
                ),
            )
            if cursor.rowcount != 1:
                raise LeaseLost(f"Could not bind reservation to {lease.target_id}")

    def heartbeat(self, lease: TargetLease, *, lease_seconds: int = 300) -> datetime:
        """Extend only the exact live token/epoch; stale workers fail closed."""
        extension = int(lease_seconds)
        if extension <= 0:
            raise ValueError("lease_seconds must be positive")
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier
                SET lease_expires_at = clock_timestamp()
                        + (%s * interval '1 second'),
                    updated_at = clock_timestamp()
                WHERE target_id = %s AND state = 'leased'
                  AND claim_token = %s AND lease_epoch = %s
                  AND lease_run_id = %s
                  AND lease_expires_at > clock_timestamp()
                RETURNING lease_expires_at
                """,
                (
                    extension,
                    lease.target_id,
                    lease.claim_token,
                    lease.lease_epoch,
                    lease.run_id,
                ),
            )
            row = _fetchone(cursor)
            if row is None:
                raise LeaseLost(f"Lease lost for {lease.target_id}")
            cursor.execute(
                """
                UPDATE fbref_control.fetch_attempt
                SET heartbeat_at = clock_timestamp()
                WHERE attempt_id = %s AND status = 'claimed'
                  AND claim_token = %s AND lease_epoch = %s
                """,
                (lease.attempt_id, lease.claim_token, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise LeaseLost(f"Attempt lease lost for {lease.target_id}")
            return row["lease_expires_at"]

    def complete_fetch(
        self,
        lease: TargetLease,
        *,
        http_status: int,
        content_hash: Optional[str],
        raw_manifest_key: Optional[str],
        decoded_bytes: int,
        compressed_bytes: int,
        wire_bytes: int,
        provider_billed_bytes: Optional[int] = None,
        http_request_count: int = 1,
        http_status_history: Optional[Sequence[int]] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        transport_version: Optional[str] = None,
        session_version: Optional[str] = None,
        latency_ms: Optional[int] = None,
        recovered_from_attempt_id: Optional[object] = None,
        next_fetch_at: Optional[datetime] = None,
    ) -> None:
        """Commit fetch evidence only while the exact fenced lease is live."""
        status = int(http_status)
        decoded = _non_negative(decoded_bytes, "decoded_bytes")
        compressed = _non_negative(compressed_bytes, "compressed_bytes")
        wire = _non_negative(wire_bytes, "wire_bytes")
        billed = (
            None
            if provider_billed_bytes is None
            else _non_negative(provider_billed_bytes, "provider_billed_bytes")
        )
        latency = (
            None if latency_ms is None else _non_negative(latency_ms, "latency_ms")
        )
        request_count = _non_negative(
            http_request_count, "http_request_count"
        )
        status_history = tuple(
            int(item)
            for item in (
                (status,) if http_status_history is None else http_status_history
            )
        )
        if len(status_history) > request_count:
            raise ValueError("HTTP status history exceeds request count")
        if any(item < 100 or item > 599 for item in status_history):
            raise ValueError("HTTP status history contains an invalid status")
        recovered_attempt = (
            None
            if recovered_from_attempt_id is None
            else _uuid(recovered_from_attempt_id, "recovered_from_attempt_id")
        )
        if not 200 <= status < 400:
            raise ValueError("A successful fetch requires a 2xx/3xx HTTP status")
        if not content_hash or not raw_manifest_key:
            raise ValueError("A successful fetch requires raw content and manifest")
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier
                SET state = 'fetched', next_fetch_at = %s,
                    last_fetched_at = clock_timestamp(),
                    last_http_status = %s,
                    last_content_hash = COALESCE(%s, last_content_hash),
                    last_etag = COALESCE(%s, last_etag),
                    last_modified = COALESCE(%s, last_modified),
                    last_error_class = NULL, last_error_message = NULL,
                    claim_token = NULL, lease_run_id = NULL,
                    lease_refresh_id = NULL, leased_by = NULL,
                    lease_expires_at = NULL, retry_after = NULL,
                    updated_at = clock_timestamp()
                WHERE target_id = %s AND state = 'leased'
                  AND claim_token = %s AND lease_epoch = %s
                  AND lease_run_id = %s AND lease_refresh_id = %s
                  AND lease_expires_at > clock_timestamp()
                RETURNING target_id
                """,
                (
                    next_fetch_at,
                    status,
                    content_hash,
                    etag,
                    last_modified,
                    lease.target_id,
                    lease.claim_token,
                    lease.lease_epoch,
                    lease.run_id,
                    lease.logical_refresh_id,
                ),
            )
            if _fetchone(cursor) is None:
                raise LeaseLost(f"Lease lost for {lease.target_id}")
            cursor.execute(
                """
                UPDATE fbref_control.run_target
                SET status = 'succeeded', updated_at = clock_timestamp()
                WHERE run_id = %s AND target_id = %s
                  AND logical_refresh_id = %s AND status = 'leased'
                """,
                (lease.run_id, lease.target_id, lease.logical_refresh_id),
            )
            if cursor.rowcount != 1:
                raise LeaseLost(f"Run target lease lost for {lease.target_id}")
            cursor.execute(
                """
                UPDATE fbref_control.fetch_attempt
                SET status = 'succeeded', http_status = %s,
                    content_hash = %s, raw_manifest_key = %s,
                    decoded_bytes = %s, compressed_bytes = %s,
                    wire_bytes = %s, provider_billed_bytes = %s,
                    http_request_count = %s, http_status_history = %s,
                    etag = %s, last_modified = %s,
                    transport_version = %s, session_version = %s,
                    latency_ms = %s, heartbeat_at = clock_timestamp(),
                    finished_at = clock_timestamp()
                WHERE attempt_id = %s AND status = 'claimed'
                  AND claim_token = %s AND lease_epoch = %s
                """,
                (
                    status,
                    content_hash,
                    raw_manifest_key,
                    0 if recovered_attempt else decoded,
                    0 if recovered_attempt else compressed,
                    0 if recovered_attempt else wire,
                    None if recovered_attempt else billed,
                    0 if recovered_attempt else request_count,
                    [] if recovered_attempt else list(status_history),
                    etag,
                    last_modified,
                    (
                        "raw-recovery"
                        if recovered_attempt
                        else transport_version
                    ),
                    None if recovered_attempt else session_version,
                    0 if recovered_attempt else latency,
                    lease.attempt_id,
                    lease.claim_token,
                    lease.lease_epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise LeaseLost(f"Attempt lease lost for {lease.target_id}")
            if recovered_attempt is not None:
                source_reservation = make_budget_reservation_id(
                    recovered_attempt
                )
                cursor.execute(
                    """
                    UPDATE fbref_control.fetch_attempt
                    SET reservation_id = COALESCE(
                            reservation_id,
                            (SELECT reservation_id
                             FROM fbref_control.budget_reservation
                             WHERE reservation_id = %s)
                        ),
                        http_status = %s, content_hash = %s,
                        raw_manifest_key = %s, decoded_bytes = %s,
                        compressed_bytes = %s, wire_bytes = %s,
                        provider_billed_bytes = %s,
                        http_request_count = %s, http_status_history = %s,
                        etag = %s,
                        last_modified = %s, transport_version = %s,
                        session_version = %s, latency_ms = %s
                    WHERE attempt_id = %s AND attempt_id <> %s
                      AND run_id = %s AND logical_refresh_id = %s
                    """,
                    (
                        source_reservation,
                        status,
                        content_hash,
                        raw_manifest_key,
                        decoded,
                        compressed,
                        wire,
                        billed,
                        request_count,
                        list(status_history),
                        etag,
                        last_modified,
                        transport_version,
                        session_version,
                        latency,
                        recovered_attempt,
                        lease.attempt_id,
                        lease.run_id,
                        lease.logical_refresh_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise StateConflict(
                        f"Raw recovery source attempt {recovered_attempt} "
                        "is missing or belongs to another refresh"
                    )

    def finish_fetch(self, lease: TargetLease, **evidence: Any) -> None:
        """Compatibility name used by orchestration for successful completion."""
        self.complete_fetch(lease, **evidence)

    def requeue_unfetched_targets(self, leases: Sequence[TargetLease]) -> int:
        """Return still-leased targets to the queue, untouched by this run.

        Used when the run stops at its budget: these targets were never fetched,
        so they carry no source-failure evidence, must not back off, and must
        stay claimable by the next run.  Their claimed attempt is nevertheless
        closed as ``cancelled`` so no terminal run leaks an active attempt.
        """
        requeued = 0
        with self._transaction() as cursor:
            for lease in leases:
                cursor.execute(
                    """
                    UPDATE fbref_control.page_frontier
                    SET state = 'queued', claim_token = NULL,
                        lease_run_id = NULL, lease_refresh_id = NULL,
                        leased_by = NULL, lease_expires_at = NULL,
                        updated_at = clock_timestamp()
                    WHERE target_id = %s AND state = 'leased'
                      AND claim_token = %s AND lease_epoch = %s
                      AND lease_run_id = %s AND lease_refresh_id = %s
                    RETURNING target_id
                    """,
                    (
                        lease.target_id,
                        lease.claim_token,
                        lease.lease_epoch,
                        lease.run_id,
                        lease.logical_refresh_id,
                    ),
                )
                if _fetchone(cursor) is None:
                    continue
                cursor.execute(
                    """
                    UPDATE fbref_control.run_target
                    SET status = 'skipped', updated_at = clock_timestamp()
                    WHERE run_id = %s AND target_id = %s
                      AND logical_refresh_id = %s AND status = 'leased'
                    """,
                    (lease.run_id, lease.target_id, lease.logical_refresh_id),
                )
                if cursor.rowcount != 1:
                    raise LeaseLost(
                        f"Run target lease lost for {lease.target_id}"
                    )
                cursor.execute(
                    """
                    UPDATE fbref_control.fetch_attempt
                    SET status = 'cancelled',
                        error_class = 'UnfetchedRequeue',
                        error_message =
                            'Target returned before network activity',
                        heartbeat_at = clock_timestamp(),
                        finished_at = clock_timestamp()
                    WHERE attempt_id = %s AND status = 'claimed'
                      AND claim_token = %s AND lease_epoch = %s
                      AND run_id = %s AND target_id = %s
                      AND logical_refresh_id = %s
                    """,
                    (
                        lease.attempt_id,
                        lease.claim_token,
                        lease.lease_epoch,
                        lease.run_id,
                        lease.target_id,
                        lease.logical_refresh_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise LeaseLost(
                        f"Attempt lease lost for {lease.target_id}"
                    )
                requeued += 1
        return requeued

    def fail_fetch(
        self,
        lease: TargetLease,
        *,
        error_class: object,
        error_message: object,
        retry_delay_seconds: int = 60,
        permanent: bool = False,
        requeue: bool = False,
        http_status: Optional[int] = None,
        wire_bytes: int = 0,
        provider_billed_bytes: Optional[int] = None,
        http_request_count: int = 0,
        http_status_history: Optional[Sequence[int]] = None,
        latency_ms: Optional[int] = None,
        transport_version: Optional[str] = None,
        session_version: Optional[str] = None,
    ) -> None:
        """Record a classified failure and either back off or dead-letter it."""
        retry_delay = _non_negative(retry_delay_seconds, "retry_delay_seconds")
        wire = _non_negative(wire_bytes, "wire_bytes")
        billed = (
            None
            if provider_billed_bytes is None
            else _non_negative(provider_billed_bytes, "provider_billed_bytes")
        )
        latency = (
            None if latency_ms is None else _non_negative(latency_ms, "latency_ms")
        )
        request_count = _non_negative(
            http_request_count, "http_request_count"
        )
        status_history = tuple(
            int(item) for item in (http_status_history or ())
        )
        if len(status_history) > request_count:
            raise ValueError("HTTP status history exceeds request count")
        if any(item < 100 or item > 599 for item in status_history):
            raise ValueError("HTTP status history contains an invalid status")
        transport = (
            None
            if transport_version is None
            else _text(transport_version, "transport_version")
        )
        session = (
            None
            if session_version is None
            else _text(session_version, "session_version")
        )
        if requeue:
            # The attempt is real evidence and is recorded, but the page itself
            # was never judged — the failure was ours (a clearance the source
            # stopped honouring). It must stay immediately claimable, and this
            # run must not count it as an unfinished target.
            frontier_state, target_state = "queued", "skipped"
        elif permanent:
            frontier_state, target_state = "dead", "failed"
        else:
            frontier_state, target_state = "retry", "retry"
        normalized_class = _text(error_class, "error_class")
        normalized_message = _text(error_message, "error_message")[:4000]
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier
                SET state = %s,
                    retry_after = CASE WHEN %s THEN NULL ELSE
                        clock_timestamp() + (%s * interval '1 second') END,
                    last_http_status = %s, last_error_class = %s,
                    last_error_message = %s,
                    claim_token = NULL, lease_run_id = NULL,
                    lease_refresh_id = NULL, leased_by = NULL,
                    lease_expires_at = NULL, updated_at = clock_timestamp()
                WHERE target_id = %s AND state = 'leased'
                  AND claim_token = %s AND lease_epoch = %s
                  AND lease_run_id = %s AND lease_refresh_id = %s
                  AND lease_expires_at > clock_timestamp()
                RETURNING target_id
                """,
                (
                    frontier_state,
                    permanent or requeue,
                    retry_delay,
                    http_status,
                    normalized_class,
                    normalized_message,
                    lease.target_id,
                    lease.claim_token,
                    lease.lease_epoch,
                    lease.run_id,
                    lease.logical_refresh_id,
                ),
            )
            if _fetchone(cursor) is None:
                raise LeaseLost(f"Lease lost for {lease.target_id}")
            cursor.execute(
                """
                UPDATE fbref_control.run_target
                SET status = %s, updated_at = clock_timestamp()
                WHERE run_id = %s AND target_id = %s
                  AND logical_refresh_id = %s AND status = 'leased'
                """,
                (
                    target_state,
                    lease.run_id,
                    lease.target_id,
                    lease.logical_refresh_id,
                ),
            )
            if cursor.rowcount != 1:
                raise LeaseLost(f"Run target lease lost for {lease.target_id}")
            cursor.execute(
                """
                UPDATE fbref_control.fetch_attempt
                SET status = 'failed', http_status = %s, wire_bytes = %s,
                    provider_billed_bytes = %s,
                    http_request_count = %s, http_status_history = %s,
                    error_class = %s,
                    error_message = %s, latency_ms = %s,
                    transport_version = %s, session_version = %s,
                    heartbeat_at = clock_timestamp(),
                    finished_at = clock_timestamp()
                WHERE attempt_id = %s AND status = 'claimed'
                  AND claim_token = %s AND lease_epoch = %s
                """,
                (
                    http_status,
                    wire,
                    billed,
                    request_count,
                    list(status_history),
                    normalized_class,
                    normalized_message,
                    latency,
                    transport,
                    session,
                    lease.attempt_id,
                    lease.claim_token,
                    lease.lease_epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise LeaseLost(f"Attempt lease lost for {lease.target_id}")

    def retry_session_fetch(
        self,
        lease: TargetLease,
        *,
        error_class: object,
        error_message: object,
        http_status: Optional[int] = None,
        wire_bytes: int = 0,
        provider_billed_bytes: Optional[int] = None,
        http_request_count: int = 0,
        http_status_history: Optional[Sequence[int]] = None,
        latency_ms: Optional[int] = None,
        transport_version: Optional[str] = None,
        session_version: Optional[str] = None,
    ) -> None:
        """Close a bad session attempt and retry its target in this run.

        Budget reservation settlement is intentionally caller-owned: the
        runner has the authoritative request/byte counters.  The transition
        is immediate and preserves the immutable run target and logical
        refresh identity; ``claim_targets`` creates a fresh fenced attempt.
        """
        self.fail_fetch(
            lease,
            error_class=error_class,
            error_message=error_message,
            retry_delay_seconds=0,
            permanent=False,
            requeue=False,
            http_status=http_status,
            wire_bytes=wire_bytes,
            provider_billed_bytes=provider_billed_bytes,
            http_request_count=http_request_count,
            http_status_history=http_status_history,
            latency_ms=latency_ms,
            transport_version=transport_version,
            session_version=session_version,
        )

    def record_dataset_manifest(
        self,
        *,
        target_id: object,
        content_hash: object,
        parser_version: object,
        dataset: object,
        availability: str,
        parse_status: str,
        persistence_status: str,
        validation_status: str,
        row_count: int = 0,
        manifest_key: Optional[str] = None,
        error_class: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Upsert the independent parse/persist/validation state for a dataset."""
        identity = (
            _text(target_id, "target_id"),
            _text(content_hash, "content_hash"),
            _text(parser_version, "parser_version"),
            _text(dataset, "dataset"),
        )
        normalized_availability = str(availability).strip().lower()
        statuses = tuple(
            str(value).strip().lower()
            for value in (
                parse_status,
                persistence_status,
                validation_status,
            )
        )
        if normalized_availability not in _AVAILABILITY_STATES:
            raise ValueError(f"Unknown availability state: {availability}")
        if any(status not in _DATASET_STATES for status in statuses):
            raise ValueError("Unknown dataset completion state")
        rows = _non_negative(row_count, "row_count")
        completed = all(status in {"succeeded", "skipped"} for status in statuses)
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT availability, parse_status, persistence_status,
                       validation_status, row_count, manifest_key
                FROM fbref_control.dataset_manifest
                WHERE target_id = %s AND content_hash = %s
                  AND parser_version = %s AND dataset = %s
                FOR UPDATE
                """,
                identity,
            )
            existing = _fetchone(cursor)
            if existing is not None and all(
                existing[name] in {"succeeded", "skipped"}
                for name in (
                    "parse_status",
                    "persistence_status",
                    "validation_status",
                )
            ):
                requested = (
                    normalized_availability,
                    *statuses,
                    rows,
                    manifest_key,
                )
                installed = (
                    existing["availability"],
                    existing["parse_status"],
                    existing["persistence_status"],
                    existing["validation_status"],
                    int(existing["row_count"]),
                    existing["manifest_key"],
                )
                if requested != installed:
                    raise StateConflict(
                        "A completed dataset manifest is immutable: "
                        f"{identity[3]} of {identity[0]} "
                        f"({identity[2]}) installed={installed!r} "
                        f"requested={requested!r}"
                    )
                return
            cursor.execute(
                """
                INSERT INTO fbref_control.dataset_manifest (
                    target_id, content_hash, parser_version, dataset,
                    availability, parse_status, persistence_status,
                    validation_status, row_count, manifest_key,
                    error_class, error_message, completed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    CASE WHEN %s THEN clock_timestamp() ELSE NULL END
                )
                ON CONFLICT (
                    target_id, content_hash, parser_version, dataset
                ) DO UPDATE SET
                    availability = EXCLUDED.availability,
                    parse_status = EXCLUDED.parse_status,
                    persistence_status = EXCLUDED.persistence_status,
                    validation_status = EXCLUDED.validation_status,
                    row_count = EXCLUDED.row_count,
                    manifest_key = EXCLUDED.manifest_key,
                    error_class = EXCLUDED.error_class,
                    error_message = EXCLUDED.error_message,
                    completed_at = EXCLUDED.completed_at,
                    updated_at = clock_timestamp()
                """,
                (
                    *identity,
                    normalized_availability,
                    *statuses,
                    rows,
                    manifest_key,
                    error_class,
                    error_message,
                    completed,
                ),
            )

    def open_clearance_session(
        self,
        *,
        domain: object,
        session_version: object,
        expires_at: datetime,
        run_id: Optional[object] = None,
        session_id: Optional[object] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> str:
        session = _uuid(session_id or uuid.uuid4(), "session_id")
        run = None if run_id is None else _uuid(run_id, "run_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.clearance_session (
                    session_id, run_id, domain, session_version,
                    expires_at, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (session_id) DO NOTHING
                """,
                (
                    session,
                    run,
                    _text(domain, "domain").lower(),
                    _text(session_version, "session_version"),
                    expires_at,
                    _json(metadata),
                ),
            )
            cursor.execute(
                """
                SELECT run_id, domain, session_version, expires_at
                FROM fbref_control.clearance_session
                WHERE session_id = %s
                """,
                (session,),
            )
            row = _fetchone(cursor)
            actual_run = None if row is None or row["run_id"] is None else str(row["run_id"])
            if row is None or (
                actual_run,
                row["domain"],
                row["session_version"],
                row["expires_at"],
            ) != (
                run,
                _text(domain, "domain").lower(),
                _text(session_version, "session_version"),
                expires_at,
            ):
                raise StateConflict(f"session_id {session} has different evidence")
        return session

    def record_session_metrics(
        self,
        session_id: object,
        *,
        browser_bootstrap_attempts: int = 0,
        browser_bootstrap_requests: int = 0,
        browser_document_bytes: int = 0,
        browser_asset_bytes: int = 0,
        browser_unobserved_bytes: int = 0,
        http_requests: int = 0,
        http_wire_bytes: int = 0,
        decoded_html_bytes: int = 0,
        compressed_raw_bytes: int = 0,
        provider_billed_bytes: Optional[int] = None,
    ) -> dict:
        """Increment disjoint byte counters; billing stays NULL when unknown."""
        session = _uuid(session_id, "session_id")
        values = tuple(
            _non_negative(value, name)
            for value, name in (
                (browser_bootstrap_attempts, "browser_bootstrap_attempts"),
                (browser_bootstrap_requests, "browser_bootstrap_requests"),
                (browser_document_bytes, "browser_document_bytes"),
                (browser_asset_bytes, "browser_asset_bytes"),
                (browser_unobserved_bytes, "browser_unobserved_bytes"),
                (http_requests, "http_requests"),
                (http_wire_bytes, "http_wire_bytes"),
                (decoded_html_bytes, "decoded_html_bytes"),
                (compressed_raw_bytes, "compressed_raw_bytes"),
            )
        )
        billed = (
            None
            if provider_billed_bytes is None
            else _non_negative(provider_billed_bytes, "provider_billed_bytes")
        )
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.clearance_session
                SET browser_bootstrap_attempts =
                        browser_bootstrap_attempts + %s,
                    browser_bootstrap_requests =
                        browser_bootstrap_requests + %s,
                    browser_document_bytes = browser_document_bytes + %s,
                    browser_asset_bytes = browser_asset_bytes + %s,
                    browser_unobserved_bytes = browser_unobserved_bytes + %s,
                    http_requests = http_requests + %s,
                    http_wire_bytes = http_wire_bytes + %s,
                    decoded_html_bytes = decoded_html_bytes + %s,
                    compressed_raw_bytes = compressed_raw_bytes + %s,
                    provider_billed_bytes = CASE
                        WHEN %s IS NULL THEN provider_billed_bytes
                        ELSE COALESCE(provider_billed_bytes, 0) + %s
                    END
                WHERE session_id = %s AND status = 'active'
                RETURNING *
                """,
                (*values, billed, billed, session),
            )
            row = _fetchone(cursor)
            if row is None:
                raise StateConflict(f"Clearance session {session} is not active")
            return row

    def close_clearance_session(
        self,
        session_id: object,
        *,
        status: str = "closed",
    ) -> None:
        normalized_status = str(status).strip().lower()
        if normalized_status not in {"closed", "expired", "failed"}:
            raise ValueError("Session terminal status is invalid")
        session = _uuid(session_id, "session_id")
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.clearance_session
                SET status = %s, closed_at = clock_timestamp()
                WHERE session_id = %s AND status = 'active'
                """,
                (normalized_status, session),
            )
            if cursor.rowcount != 1:
                cursor.execute(
                    """
                    SELECT status FROM fbref_control.clearance_session
                    WHERE session_id = %s
                    """,
                    (session,),
                )
                row = _fetchone(cursor)
                if row is None or row["status"] != normalized_status:
                    raise StateConflict(f"Session {session} cannot be closed")

    def reserve_domain_slot(
        self,
        domain: object = "fbref.com",
        *,
        interval_seconds: float = DEFAULT_DOMAIN_INTERVAL_SECONDS,
    ) -> ThrottleSlot:
        """Atomically reserve one globally spaced request time for a domain."""
        normalized_domain = _text(domain, "domain").lower()
        interval = float(interval_seconds)
        if interval < MIN_DOMAIN_INTERVAL_SECONDS:
            raise ValueError(
                "interval_seconds must respect the FBref "
                f"{MIN_DOMAIN_INTERVAL_SECONDS:g}-second source minimum"
            )
        token = str(uuid.uuid4())
        with self._transaction() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.domain_throttle (
                    domain, next_request_at, lease_epoch, last_slot_token
                ) VALUES (
                    %s,
                    clock_timestamp() + (%s * interval '1 second'),
                    1,
                    %s
                )
                ON CONFLICT (domain) DO UPDATE SET
                    next_request_at = GREATEST(
                        fbref_control.domain_throttle.next_request_at,
                        clock_timestamp()
                    ) + (%s * interval '1 second'),
                    lease_epoch =
                        fbref_control.domain_throttle.lease_epoch + 1,
                    last_slot_token = EXCLUDED.last_slot_token,
                    updated_at = clock_timestamp()
                RETURNING
                    next_request_at - (%s * interval '1 second')
                        AS scheduled_at,
                    lease_epoch
                """,
                (normalized_domain, interval, token, interval, interval),
            )
            row = _fetchone(cursor)
            if row is None:
                raise ControlStoreError("Domain throttle returned no slot")
            return ThrottleSlot(
                domain=normalized_domain,
                slot_token=token,
                lease_epoch=int(row["lease_epoch"]),
                scheduled_at=row["scheduled_at"],
            )
