"""Append-only publication manifest for one Understat league-season scope.

The seven Bronze tables are not committed in one Iceberg transaction.  This
manifest is therefore the logical publication fence: consumers may use a
``batch_id`` only after a structurally complete :class:`ScopeAttempt` has been
appended.  Failed and upstream-empty attempts remain useful audit records but
can never make a scope look published.

The domain objects have no Trino dependency.  ``UnderstatManifestRepository``
accepts writer/query adapters and lazily creates the platform adapters only in
``from_env``/default production use, which keeps unit tests hermetic.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence

import pandas as pd


MANIFEST_VERSION = "understat-ingest-manifest-v1"
CONTRACT_VERSION = "understat-bronze-v2"
MANIFEST_SCHEMA = "ops"
MANIFEST_TABLE = "understat_ingest_manifest_v1"

UNDERSTAT_ENTITIES = (
    "understat_schedule",
    "understat_shots",
    "understat_players",
    "understat_team_match_stats",
    "understat_player_match_stats",
    "understat_player_team_season_stats",
    "understat_team_season_breakdowns",
)
SCHEDULE_ENTITY = UNDERSTAT_ENTITIES[0]

MANIFEST_COLUMNS = (
    "manifest_version",
    "contract_version",
    "parser_version",
    "league",
    "season",
    "source_league",
    "source_season_id",
    "mode",
    "batch_id",
    "run_id",
    "attempt_id",
    "attempt_no",
    "status",
    "entity_statuses_json",
    "row_counts_json",
    "natural_key_counts_json",
    "payload_hashes_json",
    "quality_json",
    "started_at",
    "completed_at",
    "error_type",
    "error_message",
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required(value: object, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _validate_identifier(value: str, name: str) -> str:
    normalized = _required(value, name)
    if not _IDENTIFIER.fullmatch(normalized):
        raise ValueError(f"{name} is not a safe SQL identifier: {value!r}")
    return normalized


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


class ManifestStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    UPSTREAM_PENDING = "upstream_pending"
    NOT_PUBLISHED = "not_published"
    RETRYABLE_FAILURE = "retryable_failure"
    CONTRACT_FAILURE = "contract_failure"
    SCHEMA_DRIFT = "schema_drift"

    @property
    def published(self) -> bool:
        return self is ManifestStatus.COMPLETE


@dataclass(frozen=True, order=True)
class ScopeKey:
    """Canonical scope plus the identifiers used by Understat itself."""

    league: str
    season: str
    source_league: Optional[str] = None
    source_season_id: Optional[str] = None

    def __post_init__(self) -> None:
        league = _required(self.league, "league")
        season = _required(self.season, "season")
        object.__setattr__(self, "league", league)
        object.__setattr__(self, "season", season)
        object.__setattr__(
            self,
            "source_league",
            _required(self.source_league or league, "source_league"),
        )
        object.__setattr__(
            self,
            "source_season_id",
            _required(self.source_season_id or season, "source_season_id"),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "league": self.league,
            "season": self.season,
            "source_league": str(self.source_league),
            "source_season_id": str(self.source_season_id),
        }


def _normalize_entity_map(
    values: Mapping[str, Any],
    name: str,
) -> dict[str, Any]:
    normalized = {str(key): value for key, value in dict(values).items()}
    expected = set(UNDERSTAT_ENTITIES)
    missing = expected - set(normalized)
    extra = set(normalized) - expected
    if missing or extra:
        raise ValueError(
            f"{name} must describe exactly the seven Understat entities; "
            f"missing={sorted(missing)}, extra={sorted(extra)}"
        )
    return {entity: normalized[entity] for entity in UNDERSTAT_ENTITIES}


@dataclass(frozen=True)
class ScopeAttempt:
    """One terminal attempt to materialize all seven entities for a scope."""

    scope: ScopeKey
    status: ManifestStatus
    batch_id: str
    run_id: str
    attempt_id: str
    mode: str
    parser_version: str
    entity_statuses: Mapping[str, ManifestStatus | str]
    row_counts: Mapping[str, int]
    natural_key_counts: Mapping[str, int]
    payload_hashes: Mapping[str, str]
    contract_version: str = CONTRACT_VERSION
    attempt_no: int = 1
    quality: Mapping[str, Any] = field(default_factory=dict)
    started_at: str = field(default_factory=utc_now_iso)
    completed_at: str = field(default_factory=utc_now_iso)
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    manifest_version: str = MANIFEST_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.scope, ScopeKey):
            object.__setattr__(self, "scope", ScopeKey(**dict(self.scope)))
        if not isinstance(self.status, ManifestStatus):
            object.__setattr__(self, "status", ManifestStatus(self.status))
        for name in (
            "batch_id",
            "run_id",
            "attempt_id",
            "mode",
            "parser_version",
            "contract_version",
            "started_at",
            "completed_at",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        if self.attempt_no < 1:
            raise ValueError("attempt_no must be >= 1")
        if self.manifest_version != MANIFEST_VERSION:
            raise ValueError(f"Unsupported manifest version: {self.manifest_version}")

        raw_statuses = _normalize_entity_map(
            self.entity_statuses, "entity_statuses"
        )
        statuses = {
            entity: (
                value if isinstance(value, ManifestStatus) else ManifestStatus(value)
            )
            for entity, value in raw_statuses.items()
        }
        rows = {
            entity: int(value)
            for entity, value in _normalize_entity_map(
                self.row_counts, "row_counts"
            ).items()
        }
        keys = {
            entity: int(value)
            for entity, value in _normalize_entity_map(
                self.natural_key_counts, "natural_key_counts"
            ).items()
        }
        hashes = {
            entity: str(value or "").strip()
            for entity, value in _normalize_entity_map(
                self.payload_hashes, "payload_hashes"
            ).items()
        }
        if any(value < 0 for value in (*rows.values(), *keys.values())):
            raise ValueError("row and natural-key counts must be non-negative")
        for entity in UNDERSTAT_ENTITIES:
            if keys[entity] > rows[entity]:
                raise ValueError(
                    f"{entity}: natural-key count cannot exceed row count"
                )
            if rows[entity] == 0 and keys[entity] != 0:
                raise ValueError(f"{entity}: an empty entity must have zero keys")

        object.__setattr__(self, "entity_statuses", statuses)
        object.__setattr__(self, "row_counts", rows)
        object.__setattr__(self, "natural_key_counts", keys)
        object.__setattr__(self, "payload_hashes", hashes)
        object.__setattr__(self, "quality", dict(self.quality))
        self._validate_status_contract()

    def _validate_status_contract(self) -> None:
        statuses = self.entity_statuses
        if self.status is ManifestStatus.COMPLETE:
            invalid = [
                entity
                for entity in UNDERSTAT_ENTITIES
                if statuses[entity] is not ManifestStatus.COMPLETE
                or self.row_counts[entity] <= 0
                or self.natural_key_counts[entity] != self.row_counts[entity]
                or not self.payload_hashes[entity]
            ]
            if invalid:
                raise ValueError(
                    "complete scope requires seven complete, non-empty, "
                    f"unique and hashed entities; invalid={invalid}"
                )
            return

        if self.status is ManifestStatus.NOT_PUBLISHED:
            if any(self.row_counts.values()) or any(
                value is not ManifestStatus.NOT_PUBLISHED
                for value in statuses.values()
            ):
                raise ValueError(
                    "not_published requires zero rows and seven not_published entities"
                )
            return

        if self.status is ManifestStatus.UPSTREAM_PENDING:
            if (
                statuses[SCHEDULE_ENTITY] is not ManifestStatus.COMPLETE
                or self.row_counts[SCHEDULE_ENTITY] <= 0
                or self.natural_key_counts[SCHEDULE_ENTITY]
                != self.row_counts[SCHEDULE_ENTITY]
                or not self.payload_hashes[SCHEDULE_ENTITY]
            ):
                raise ValueError(
                    "upstream_pending requires a complete non-empty schedule"
                )
            invalid = [
                entity
                for entity in UNDERSTAT_ENTITIES[1:]
                if statuses[entity] is not ManifestStatus.UPSTREAM_PENDING
                or self.row_counts[entity] != 0
            ]
            if invalid:
                raise ValueError(
                    "upstream_pending is the active schedule-only state; "
                    f"invalid={invalid}"
                )
            return

        if self.status not in set(statuses.values()):
            raise ValueError(
                f"{self.status.value} scope requires at least one entity with "
                "the same failure status"
            )

    @property
    def published(self) -> bool:
        return self.status.published

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope.to_dict(),
            "status": self.status.value,
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "attempt_no": self.attempt_no,
            "mode": self.mode,
            "parser_version": self.parser_version,
            "contract_version": self.contract_version,
            "entity_statuses": {
                key: value.value for key, value in self.entity_statuses.items()
            },
            "row_counts": dict(self.row_counts),
            "natural_key_counts": dict(self.natural_key_counts),
            "payload_hashes": dict(self.payload_hashes),
            "quality": dict(self.quality),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "manifest_version": self.manifest_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ScopeAttempt":
        values = dict(payload)
        scope = values.pop("scope")
        values["scope"] = scope if isinstance(scope, ScopeKey) else ScopeKey(**scope)
        return cls(**values)

    def to_row(self) -> dict[str, Any]:
        row = {
            "manifest_version": self.manifest_version,
            "contract_version": self.contract_version,
            "parser_version": self.parser_version,
            **self.scope.to_dict(),
            "mode": self.mode,
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "attempt_no": self.attempt_no,
            "status": self.status.value,
            "entity_statuses_json": _canonical_json(
                {key: value.value for key, value in self.entity_statuses.items()}
            ),
            "row_counts_json": _canonical_json(self.row_counts),
            "natural_key_counts_json": _canonical_json(
                self.natural_key_counts
            ),
            "payload_hashes_json": _canonical_json(self.payload_hashes),
            "quality_json": _canonical_json(self.quality),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_type": self.error_type or "",
            "error_message": self.error_message or "",
        }
        return {column: row[column] for column in MANIFEST_COLUMNS}

    @classmethod
    def from_row(
        cls, row: Mapping[str, Any] | Sequence[Any]
    ) -> "ScopeAttempt":
        if isinstance(row, Mapping):
            values = {column: row.get(column) for column in MANIFEST_COLUMNS}
        else:
            if len(row) != len(MANIFEST_COLUMNS):
                raise ValueError("Trino manifest row has an unexpected column count")
            values = dict(zip(MANIFEST_COLUMNS, row))
        return cls(
            scope=ScopeKey(
                league=values["league"],
                season=values["season"],
                source_league=values["source_league"],
                source_season_id=values["source_season_id"],
            ),
            status=ManifestStatus(values["status"]),
            batch_id=values["batch_id"],
            run_id=values["run_id"],
            attempt_id=values["attempt_id"],
            attempt_no=int(values["attempt_no"]),
            mode=values["mode"],
            parser_version=values["parser_version"],
            contract_version=values["contract_version"],
            entity_statuses=json.loads(values["entity_statuses_json"] or "{}"),
            row_counts=json.loads(values["row_counts_json"] or "{}"),
            natural_key_counts=json.loads(
                values["natural_key_counts_json"] or "{}"
            ),
            payload_hashes=json.loads(values["payload_hashes_json"] or "{}"),
            quality=json.loads(values["quality_json"] or "{}"),
            started_at=str(values["started_at"]),
            completed_at=str(values["completed_at"]),
            error_type=values["error_type"] or None,
            error_message=values["error_message"] or None,
            manifest_version=values["manifest_version"],
        )


class IcebergWriterProtocol(Protocol):
    def write_dataframe(self, df: pd.DataFrame, **kwargs: Any) -> str: ...


class QueryExecutorProtocol(Protocol):
    def execute_query(
        self, sql: str, params: Optional[tuple] = None
    ) -> Sequence[Any]: ...


def render_manifest_ddl(
    *,
    catalog: str = "iceberg",
    schema: str = MANIFEST_SCHEMA,
    table: str = MANIFEST_TABLE,
) -> str:
    catalog = _validate_identifier(catalog, "catalog")
    schema = _validate_identifier(schema, "schema")
    table = _validate_identifier(table, "table")
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
    manifest_version varchar,
    contract_version varchar,
    parser_version varchar,
    league varchar,
    season varchar,
    source_league varchar,
    source_season_id varchar,
    mode varchar,
    batch_id varchar,
    run_id varchar,
    attempt_id varchar,
    attempt_no bigint,
    status varchar,
    entity_statuses_json varchar,
    row_counts_json varchar,
    natural_key_counts_json varchar,
    payload_hashes_json varchar,
    quality_json varchar,
    started_at varchar,
    completed_at varchar,
    error_type varchar,
    error_message varchar
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['league', 'season']
)
""".strip()


class UnderstatManifestRepository:
    """Iceberg-backed append-only manifest with injectable storage adapters."""

    def __init__(
        self,
        *,
        writer: Optional[IcebergWriterProtocol] = None,
        query: Optional[QueryExecutorProtocol] = None,
        catalog: str = "iceberg",
        schema: str = MANIFEST_SCHEMA,
        table: str = MANIFEST_TABLE,
        ensure_table_on_write: bool = True,
    ) -> None:
        self.catalog = _validate_identifier(catalog, "catalog")
        self.schema = _validate_identifier(schema, "schema")
        self.table = _validate_identifier(table, "table")
        self.qualified = f"{self.catalog}.{self.schema}.{self.table}"
        self._writer = writer
        self._query = query
        self.ensure_table_on_write = bool(ensure_table_on_write)
        self._table_ensured = False

    @classmethod
    def from_env(cls, **kwargs: Any) -> "UnderstatManifestRepository":
        """Create a lazy production repository using platform env defaults."""
        return cls(**kwargs)

    def _get_writer(self) -> IcebergWriterProtocol:
        if self._writer is None:
            from scrapers.base.iceberg_writer import IcebergWriter

            self._writer = IcebergWriter(catalog=self.catalog)
        return self._writer

    def _get_query(self) -> QueryExecutorProtocol:
        if self._query is not None:
            return self._query
        writer = self._get_writer()
        factory = getattr(writer, "_get_trino_manager", None)
        if not callable(factory):
            raise RuntimeError(
                "a query adapter is required for manifest reads/DDL"
            )
        self._query = factory()
        return self._query

    def _execute(
        self,
        sql: str,
        *,
        params: Optional[tuple] = None,
        fetch: bool = True,
    ) -> Sequence[Any]:
        query = self._get_query()
        execute_query = getattr(query, "execute_query", None)
        if callable(execute_query):
            rows = execute_query(sql, params=params)
            return rows or []
        execute = getattr(query, "_execute", None)
        if callable(execute):
            rows = execute(sql, fetch=fetch, params=params)
            return rows or []
        if callable(query):
            rows = query(sql, params)
            return rows or []
        raise TypeError("query adapter must expose execute_query or _execute")

    def ensure_table(self) -> None:
        if self._table_ensured:
            return
        self._execute(
            f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}",
            fetch=False,
        )
        self._execute(
            render_manifest_ddl(
                catalog=self.catalog,
                schema=self.schema,
                table=self.table,
            ),
            fetch=False,
        )
        self._table_ensured = True

    def append_attempt(self, attempt: ScopeAttempt) -> str:
        if not isinstance(attempt, ScopeAttempt):
            raise TypeError("attempt must be a ScopeAttempt")
        if self.ensure_table_on_write:
            self.ensure_table()
        frame = pd.DataFrame([attempt.to_row()], columns=MANIFEST_COLUMNS)
        return self._get_writer().write_dataframe(
            frame,
            database=self.schema,
            table=self.table,
            partition_spec=[("league", "identity"), ("season", "identity")],
            mode="append",
            add_metadata=False,
            source="understat",
        )

    @staticmethod
    def _select_columns() -> str:
        return ", ".join(f'"{column}"' for column in MANIFEST_COLUMNS)

    def _latest(
        self,
        scope: ScopeKey,
        *,
        contract_version: str,
        statuses: Sequence[ManifestStatus] = (),
    ) -> Optional[ScopeAttempt]:
        if not isinstance(scope, ScopeKey):
            raise TypeError("scope must be a ScopeKey")
        contract_version = _required(contract_version, "contract_version")
        normalized_statuses = tuple(
            status if isinstance(status, ManifestStatus) else ManifestStatus(status)
            for status in statuses
        )
        if len(normalized_statuses) == 1:
            status_clause = ' AND "status" = ?'
        elif normalized_statuses:
            placeholders = ", ".join("?" for _ in normalized_statuses)
            status_clause = f' AND "status" IN ({placeholders})'
        else:
            status_clause = ""
        params: tuple[Any, ...] = (
            scope.league,
            scope.season,
            contract_version,
        )
        params += tuple(status.value for status in normalized_statuses)
        rows = self._execute(
            f"SELECT {self._select_columns()} FROM {self.qualified} "
            'WHERE "league" = ? AND "season" = ? '
            f'AND "contract_version" = ?{status_clause} '
            'ORDER BY "completed_at" DESC, "attempt_id" DESC LIMIT 1',
            params=params,
        )
        return ScopeAttempt.from_row(rows[0]) if rows else None

    def latest_attempt(
        self,
        scope: ScopeKey,
        *,
        contract_version: str = CONTRACT_VERSION,
    ) -> Optional[ScopeAttempt]:
        return self._latest(
            scope,
            contract_version=contract_version,
            statuses=(),
        )

    def latest_complete(
        self,
        scope: ScopeKey,
        *,
        contract_version: str = CONTRACT_VERSION,
    ) -> Optional[ScopeAttempt]:
        return self._latest(
            scope,
            contract_version=contract_version,
            statuses=(ManifestStatus.COMPLETE,),
        )

    def latest_data_attempt(
        self,
        scope: ScopeKey,
        *,
        contract_version: str = CONTRACT_VERSION,
    ) -> Optional[ScopeAttempt]:
        """Return the newest attempt that observed usable source data.

        ``upstream_pending`` is deliberately included: its schedule is a valid
        source observation even though the other six entities are not yet
        publishable. Remembering it prevents a later transient empty response
        from silently regressing the scope to ``not_published``.
        """

        return self._latest(
            scope,
            contract_version=contract_version,
            statuses=(ManifestStatus.COMPLETE, ManifestStatus.UPSTREAM_PENDING),
        )

    def physical_scope_row_count(self, entity: str, scope: ScopeKey) -> int:
        """Count exact physical rows without applying the manifest fence.

        This is intentionally a migration/preflight primitive, not a consumer
        read. It lets the first v2 attempt distinguish a truly unpublished
        calendar probe from legacy Bronze data that must not be hidden by a
        new ``not_published`` marker.
        """

        if entity not in UNDERSTAT_ENTITIES:
            raise ValueError(f"unknown Understat entity: {entity!r}")
        if not isinstance(scope, ScopeKey):
            raise TypeError("scope must be a ScopeKey")
        table = _validate_identifier(entity, "entity table")
        rows = self._execute(
            f'SELECT COUNT(*) AS row_count FROM {self.catalog}.bronze.{table} '
            'WHERE "league" = ? AND "season" = ?',
            params=(scope.league, scope.season),
        )
        if len(rows) != 1:
            raise RuntimeError(
                f"physical row-count query returned {len(rows)} rows for {entity}"
            )
        row = rows[0]
        value = row.get("row_count") if isinstance(row, Mapping) else row[0]
        return int(value or 0)

    def is_scope_complete(
        self,
        scope: ScopeKey,
        *,
        contract_version: str = CONTRACT_VERSION,
        verify_physical: bool = True,
    ) -> bool:
        # A later failure invalidates an older complete marker.  This matters
        # because partition replacement is table-by-table: the failed attempt
        # may already have changed a subset of physical partitions.
        attempt = self.latest_attempt(scope, contract_version=contract_version)
        if attempt is None or attempt.status is not ManifestStatus.COMPLETE:
            return False
        return not verify_physical or self.verify_physical_batch(attempt)

    is_published = is_scope_complete

    def next_incomplete(
        self,
        scopes: Iterable[ScopeKey],
        *,
        contract_version: str = CONTRACT_VERSION,
        verify_physical: bool = True,
    ) -> Optional[ScopeKey]:
        for scope in scopes:
            if not self.is_scope_complete(
                scope,
                contract_version=contract_version,
                verify_physical=verify_physical,
            ):
                return scope
        return None

    def verify_physical_batch(self, attempt: ScopeAttempt) -> bool:
        """Verify the manifest's logical commit against all Bronze partitions.

        One UNION ALL round-trip proves that every exact scope has the manifest
        row count and that every physical row belongs to the same shared batch.
        Thus a process crash between table replacements cannot leave an older
        complete manifest looking healthy.
        """
        if attempt.status is not ManifestStatus.COMPLETE:
            return False
        selects = []
        params: list[Any] = []
        for entity in UNDERSTAT_ENTITIES:
            table = _validate_identifier(entity, "entity table")
            selects.append(
                f"SELECT '{table}' AS entity, COUNT(*) AS row_count, "
                'COUNT_IF("_batch_id" = ?) AS batch_rows, '
                'COUNT(DISTINCT "_batch_id") AS batch_count '
                f"FROM {self.catalog}.bronze.{table} "
                'WHERE "league" = ? AND "season" = ?'
            )
            params.extend(
                (attempt.batch_id, attempt.scope.league, attempt.scope.season)
            )
        rows = self._execute(" UNION ALL ".join(selects), params=tuple(params))
        observed: dict[str, tuple[int, int, int]] = {}
        for row in rows:
            if isinstance(row, Mapping):
                entity = str(row.get("entity"))
                values = (
                    int(row.get("row_count") or 0),
                    int(row.get("batch_rows") or 0),
                    int(row.get("batch_count") or 0),
                )
            else:
                if len(row) < 4:
                    return False
                entity = str(row[0])
                values = (int(row[1]), int(row[2]), int(row[3]))
            if entity in observed:
                return False
            observed[entity] = values
        if set(observed) != set(UNDERSTAT_ENTITIES):
            return False
        for entity in UNDERSTAT_ENTITIES:
            row_count, batch_rows, batch_count = observed[entity]
            expected = attempt.row_counts[entity]
            if (
                row_count != expected
                or batch_rows != row_count
                or batch_count != 1
            ):
                return False
        return True


def new_attempt_id() -> str:
    return str(uuid.uuid4())


def _load_attempt_payload(
    value: str | Path | Mapping[str, Any],
) -> Mapping[str, Any]:
    if isinstance(value, (str, Path)):
        with Path(value).open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    else:
        payload = dict(value)
    for key in ("scope_attempt", "manifest", "attempt"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            return nested
    return payload


def validate_scope_attempt_result(
    value: str | Path | Mapping[str, Any],
    *,
    expected_scope: ScopeKey,
    accepted_statuses: Iterable[ManifestStatus | str] = (
        ManifestStatus.COMPLETE,
    ),
    contract_version: str = CONTRACT_VERSION,
) -> ScopeAttempt:
    """Validate a runner result without importing Airflow.

    The runner may emit the attempt directly or under ``scope_attempt``,
    ``manifest`` or ``attempt``.  Malformed/partial ``complete`` attempts fail
    while constructing :class:`ScopeAttempt`.
    """
    attempt = ScopeAttempt.from_dict(_load_attempt_payload(value))
    accepted = {
        status if isinstance(status, ManifestStatus) else ManifestStatus(status)
        for status in accepted_statuses
    }
    if attempt.status not in accepted:
        raise ValueError(
            f"Understat scope attempt status {attempt.status.value!r} is not accepted"
        )
    if (
        attempt.scope.league != expected_scope.league
        or attempt.scope.season != expected_scope.season
    ):
        raise ValueError(
            "Understat scope result does not match the requested scope: "
            f"expected={expected_scope.league}/{expected_scope.season}, "
            f"actual={attempt.scope.league}/{attempt.scope.season}"
        )
    if attempt.contract_version != contract_version:
        raise ValueError(
            "Understat scope result contract mismatch: "
            f"expected={contract_version}, actual={attempt.contract_version}"
        )
    return attempt


__all__ = [
    "CONTRACT_VERSION",
    "MANIFEST_COLUMNS",
    "MANIFEST_SCHEMA",
    "MANIFEST_TABLE",
    "MANIFEST_VERSION",
    "SCHEDULE_ENTITY",
    "UNDERSTAT_ENTITIES",
    "ManifestStatus",
    "ScopeAttempt",
    "ScopeKey",
    "UnderstatManifestRepository",
    "new_attempt_id",
    "render_manifest_ddl",
    "utc_now_iso",
    "validate_scope_attempt_result",
]
