"""PostgreSQL-backed state machine for production FBref ingestion.

All claim, budget, and completion operations are transactional.  Workers may
crash at any point: an expired lease can be reclaimed, while a stale worker is
prevented from committing by the UUID token plus monotonically increasing
lease epoch.
"""

from __future__ import annotations

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
    FrontierTarget,
    ObservationLease,
    SeasonRegistryEntry,
    TargetLease,
    ThrottleSlot,
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
    def _transaction(self):
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
                raise StateConflict(
                    f"snapshot_id {snapshot} already has different evidence"
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
    ) -> dict[str, int]:
        """Reconcile one successful index snapshot without deleting history."""
        snapshot = _uuid(snapshot_id, "snapshot_id")
        competitions = self._validated_competitions(entries)
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
                    present = false,
                    last_snapshot_id = %s
                WHERE source = %s
                  AND last_snapshot_id <> %s
                  AND NOT (competition_id = ANY(%s::text[]))
                """,
                (snapshot, source, snapshot, seen_ids),
            )
            counts["missing"] = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier AS frontier
                SET state = 'queued', next_fetch_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                FROM fbref_control.competition_registry AS competition
                WHERE competition.source = frontier.source
                  AND competition.competition_id =
                      frontier.source_ids ->> 'competition_id'
                  AND competition.gender = 'male'
                  AND competition.crawl_state = 'active'
                  AND competition.lifecycle_state = 'present'
                  AND competition.present
                  AND frontier.state IN ('skipped', 'quarantined')
                """
            )
            counts["frontier_scope_reopened"] = cursor.rowcount
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier AS frontier
                SET state = CASE
                        WHEN competition.gender = 'unknown'
                        THEN 'quarantined'
                        ELSE 'skipped'
                    END,
                    next_fetch_at = NULL, retry_after = NULL,
                    updated_at = clock_timestamp()
                FROM fbref_control.competition_registry AS competition
                WHERE competition.source = frontier.source
                  AND competition.competition_id =
                      frontier.source_ids ->> 'competition_id'
                  AND (
                    competition.gender <> 'male'
                    OR competition.crawl_state <> 'active'
                    OR competition.lifecycle_state <> 'present'
                    OR NOT competition.present
                  )
                  AND frontier.state <> 'leased'
                """
            )
            counts["frontier_scope_closed"] = cursor.rowcount
            return counts

    def eligible_competitions(self, *, source: str = "fbref") -> list[dict]:
        """Return only current male rows eligible to create downstream targets."""
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT * FROM fbref_control.competition_registry
                WHERE source = %s AND gender = 'male'
                  AND crawl_state = 'active'
                  AND lifecycle_state = 'present' AND present
                ORDER BY competition_id
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
                or parent["lifecycle_state"] != "present"
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
                  AND competition.lifecycle_state = 'present'
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
                  AND competition.gender = 'male'
                  AND competition.crawl_state = 'active'
                  AND competition.lifecycle_state = 'present'
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

    def upsert_frontier_target(self, target: FrontierTarget) -> None:
        """Create/update one canonical identity before any network request."""
        target_id = _text(target.target_id, "target_id")
        canonical_url = _text(target.canonical_url, "canonical_url")
        if urlsplit(canonical_url).scheme not in {"http", "https"}:
            raise ValueError("canonical_url must be absolute HTTP(S)")
        with self._transaction() as cursor:
            cursor.execute(
                """
                SELECT target_id, source, page_kind, canonical_url,
                       source_ids, state
                FROM fbref_control.page_frontier
                WHERE target_id = %s OR canonical_url = %s
                FOR UPDATE
                """,
                (target_id, canonical_url),
            )
            rows = _fetchall(cursor)
            for row in rows:
                if row["target_id"] != target_id:
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
        control_quota = max(1, normalized_limit // 5)
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
                """
                WITH eligible AS MATERIALIZED (
                  SELECT frontier.target_id, frontier.page_kind,
                         frontier.last_fetched_at, frontier.created_at,
                         frontier.priority, frontier.next_fetch_at,
                         (
                           frontier.last_fetched_at IS NOT NULL
                           AND frontier.refresh_policy <> 'historical_once'
                           AND frontier.page_kind = ANY(ARRAY[
                               'competition_index', 'competition',
                               'season', 'schedule'
                           ]::text[])
                         ) AS control_lane
                  FROM fbref_control.page_frontier AS frontier
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
                    NOT (frontier.source_ids ? 'competition_id')
                    OR EXISTS (
                      SELECT 1
                      FROM fbref_control.competition_registry AS competition
                      WHERE competition.source = frontier.source
                        AND competition.competition_id =
                            frontier.source_ids ->> 'competition_id'
                        AND competition.gender = 'male'
                        AND competition.crawl_state = 'active'
                        AND competition.lifecycle_state = 'present'
                        AND competition.present
                    )
                  )
                  AND (
                    NOT (frontier.source_ids ? 'season_id')
                    OR EXISTS (
                      SELECT 1
                      FROM fbref_control.season_registry AS season
                      WHERE season.source = frontier.source
                        AND season.competition_id =
                            frontier.source_ids ->> 'competition_id'
                        AND season.season_id =
                            frontier.source_ids ->> 'season_id'
                        AND season.lifecycle_state = 'present'
                        AND season.present
                        AND (
                          season.is_current
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
                ), ranked AS (
                  SELECT eligible.*,
                         row_number() OVER (
                           PARTITION BY eligible.control_lane
                           ORDER BY eligible.last_fetched_at ASC NULLS LAST,
                                    eligible.created_at,
                                    eligible.target_id
                         ) AS control_lane_rank
                  FROM eligible
                )
                SELECT frontier.target_id
                FROM ranked
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = ranked.target_id
                -- Reserve a small lane for overdue control pages.  The rest
                -- remains FIFO: never-fetched first, then oldest refresh.
                ORDER BY CASE
                           WHEN ranked.control_lane
                            AND ranked.control_lane_rank <= %s
                           THEN 0 ELSE 1
                         END,
                         CASE
                           WHEN ranked.control_lane
                            AND ranked.control_lane_rank <= %s
                           THEN frontier.last_fetched_at
                         END ASC NULLS LAST,
                         (frontier.last_fetched_at IS NOT NULL) ASC,
                         CASE
                             WHEN frontier.last_fetched_at IS NULL
                             THEN frontier.created_at
                             ELSE frontier.last_fetched_at
                         END ASC,
                         frontier.priority DESC,
                         frontier.next_fetch_at NULLS FIRST,
                         frontier.target_id
                LIMIT %s
                FOR UPDATE OF frontier SKIP LOCKED
                """,
                (
                    kinds,
                    kinds,
                    policies,
                    policies,
                    run,
                    control_quota,
                    control_quota,
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
            for offset, candidate in enumerate(candidates):
                target_id = str(candidate["target_id"])
                item = CohortTarget(
                    target_id=target_id,
                    logical_refresh_id=make_logical_refresh_id(run, target_id),
                    ordinal=next_ordinal + offset,
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
        return cohort

    def get_run_summary(
        self,
        run_id: object,
        *,
        parser_version: Optional[object] = None,
        typed_parser_version: Optional[object] = None,
        stateful_parser_version: Optional[object] = None,
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
                SELECT count(*) AS count
                FROM fbref_control.page_frontier AS frontier
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
                  AND (
                    NOT (frontier.source_ids ? 'competition_id')
                    OR EXISTS (
                      SELECT 1
                      FROM fbref_control.competition_registry AS competition
                      WHERE competition.source = frontier.source
                        AND competition.competition_id =
                            frontier.source_ids ->> 'competition_id'
                        AND competition.gender = 'male'
                        AND competition.crawl_state = 'active'
                        AND competition.lifecycle_state = 'present'
                        AND competition.present
                    )
                  )
                  AND (
                    NOT (frontier.source_ids ? 'season_id')
                    OR EXISTS (
                      SELECT 1
                      FROM fbref_control.season_registry AS season
                      WHERE season.source = frontier.source
                        AND season.competition_id =
                            frontier.source_ids ->> 'competition_id'
                        AND season.season_id =
                            frontier.source_ids ->> 'season_id'
                        AND season.lifecycle_state = 'present'
                        AND season.present
                        AND (
                          season.is_current
                          OR frontier.refresh_policy = 'historical_once'
                        )
                    )
                  )
                """
            )
            pending_matches = _fetchone(cursor)
            summary["promotion_pending_match_count"] = int(
                0 if pending_matches is None else pending_matches["count"]
            )
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
                SELECT status FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run,),
            )
            crawl_run = _fetchone(cursor)
            if crawl_run is None or crawl_run["status"] != "running":
                return []
            self._reap_expired(
                cursor,
                run_ids=(run,),
                run_rows_locked=True,
            )
            cursor.execute(
                """
                SELECT target.target_id, target.logical_refresh_id,
                       frontier.canonical_url, frontier.page_kind,
                       frontier.source_ids, frontier.lease_epoch
                FROM fbref_control.run_target AS target
                JOIN fbref_control.crawl_run AS run
                  ON run.run_id = target.run_id
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = target.target_id
                WHERE target.run_id = %s AND run.status = 'running'
                  AND target.status IN ('pending', 'retry')
                  AND frontier.state IN ('queued', 'retry')
                  AND (%s::text[] IS NULL
                       OR frontier.page_kind = ANY(%s::text[]))
                  AND (%s::text[] IS NULL
                       OR frontier.refresh_policy = ANY(%s::text[]))
                  AND (
                    NOT (frontier.source_ids ? 'competition_id')
                    OR EXISTS (
                      SELECT 1
                      FROM fbref_control.competition_registry AS competition
                      WHERE competition.source = frontier.source
                        AND competition.competition_id =
                            frontier.source_ids ->> 'competition_id'
                        AND competition.gender = 'male'
                        AND competition.crawl_state = 'active'
                        AND competition.lifecycle_state = 'present'
                        AND competition.present
                    )
                  )
                  AND (
                    NOT (frontier.source_ids ? 'season_id')
                    OR EXISTS (
                      SELECT 1
                      FROM fbref_control.season_registry AS season
                      WHERE season.source = frontier.source
                        AND season.competition_id =
                            frontier.source_ids ->> 'competition_id'
                        AND season.season_id =
                            frontier.source_ids ->> 'season_id'
                        AND season.lifecycle_state = 'present'
                        AND season.present
                        AND (
                          season.is_current
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

    def fail_fetch(
        self,
        lease: TargetLease,
        *,
        error_class: object,
        error_message: object,
        retry_delay_seconds: int = 60,
        permanent: bool = False,
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
        frontier_state = "dead" if permanent else "retry"
        target_state = "failed" if permanent else "retry"
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
                    permanent,
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
                    raise StateConflict("A completed dataset manifest is immutable")
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
        interval_seconds: float = 3.0,
    ) -> ThrottleSlot:
        """Atomically reserve one globally spaced request time for a domain."""
        normalized_domain = _text(domain, "domain").lower()
        interval = float(interval_seconds)
        if interval <= 0:
            raise ValueError("interval_seconds must be positive")
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
