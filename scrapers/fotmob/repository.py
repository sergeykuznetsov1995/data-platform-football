"""Canonical Iceberg persistence for the FotMob ingestion pipeline.

The legacy scraper writes nine convenient, but partly season-mislabelled,
tables directly.  The native pipeline keeps source identity intact and uses an
append-only manifest as its logical commit point:

* every physical row carries a deterministic ``_target_batch_id``;
* a successful manifest is appended only after all rows for the target were
  written;
* zero-row, not-available and excluded targets are explicit manifest states;
* raw object identity and transport counters travel with every commit.

This module intentionally contains no FotMob parsing or HTTP code.  It accepts
plain row mappings, which makes raw replay and unit testing deterministic.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence

import pandas as pd

from scrapers.base.iceberg_writer import IcebergWriter


PARSER_VERSION = "fotmob-native-v1"
MANIFEST_TABLE = "fotmob_ingest_manifest"


class ManifestStatus(str, Enum):
    """Exhaustive terminal/non-terminal result of one source target."""

    SUCCESS = "success"
    NOT_MODIFIED = "not_modified"
    NOT_AVAILABLE = "not_available"
    RETRYABLE_FAILURE = "retryable_failure"
    TERMINAL_FAILURE = "terminal_failure"
    EXCLUDED = "excluded"
    REVIEW_REQUIRED = "review_required"
    SCHEMA_DRIFT = "schema_drift"


SUCCESS_STATES = frozenset(
    {ManifestStatus.SUCCESS.value, ManifestStatus.NOT_MODIFIED.value}
)


class RepositoryWriter(Protocol):
    def write_dataframe(
        self,
        df: pd.DataFrame,
        database: str,
        table: str,
        partition_spec: Optional[list[tuple[str, str]]] = None,
        mode: str = "append",
        add_metadata: bool = True,
        source: Optional[str] = None,
        delete_filter: Optional[str] = None,
    ) -> str: ...


def utc_now() -> datetime:
    """Return a timezone-naive UTC timestamp accepted by Trino/Iceberg."""

    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def stable_json(value: Any) -> str:
    """Serialize nested source values without lossy coercion."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def deterministic_target_batch_id(
    target_key: str,
    content_hash: Optional[str],
    parser_version: str = PARSER_VERSION,
) -> str:
    """Stable identity for idempotent raw replay of the same target."""

    material = (
        f"{str(target_key).strip()}\0{content_hash or 'no-content'}\0{parser_version}"
    ).encode("utf-8")
    return "fm1-" + hashlib.sha256(material).hexdigest()


def _scalar(value: Any) -> Any:
    """Make arbitrary parser values safe for an Arrow dataframe column.

    Canonical parsers expose nested source fragments in explicit ``*_json``
    columns.  This defensive conversion also prevents a future mixed
    int/dict value from reproducing the player-details Arrow failure that the
    legacy runner used to hide.
    """

    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return stable_json(asdict(value))
    if isinstance(value, (dict, list, tuple, set)):
        return stable_json(value)
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def normalize_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return rows with homogeneous, Arrow-safe scalar values."""

    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(f"repository row must be a mapping, got {type(row)!r}")
        output.append({str(key): _scalar(value) for key, value in row.items()})
    return output


@dataclass(frozen=True)
class TableRows:
    """One physical canonical dataset produced by a source target."""

    table: str
    rows: Sequence[Mapping[str, Any]]
    entity_type: str
    partition_cols: tuple[str, ...] = ()
    distinct_key: Optional[str] = None


@dataclass(frozen=True)
class TargetCommit:
    """Logical result for one catalog/API target."""

    run_id: str
    target_type: str
    target_key: str
    status: ManifestStatus
    competition_id: Optional[str] = None
    source_season_key: Optional[str] = None
    stage_id: Optional[str] = None
    entity_id: Optional[str] = None
    content_hash: Optional[str] = None
    raw_uri: Optional[str] = None
    parser_version: str = PARSER_VERSION
    fetch_outcome: Optional[str] = None
    http_status: Optional[int] = None
    attempts: int = 0
    retries: int = 0
    cache_hit: bool = False
    stale: bool = False
    fetched_at: Optional[datetime] = None
    direct_bytes: int = 0
    proxy_bytes: int = 0
    encoded_bytes: int = 0
    decoded_bytes: int = 0
    expected_counts: Mapping[str, int] = field(default_factory=dict)
    actual_counts: Mapping[str, int] = field(default_factory=dict)
    capabilities: Mapping[str, Any] = field(default_factory=dict)
    exclusions: Sequence[Mapping[str, Any] | str] = field(default_factory=tuple)
    unknown_paths: Sequence[str] = field(default_factory=tuple)
    error_code: Optional[str] = None
    error: Optional[str] = None
    retry_after: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def batch_id(self) -> str:
        return deterministic_target_batch_id(
            self.target_key, self.content_hash, self.parser_version
        )

    def manifest_row(self) -> dict[str, Any]:
        completed_at = self.completed_at or utc_now()
        return {
            "run_id": self.run_id,
            "target_type": self.target_type,
            "target_key": self.target_key,
            "competition_id": self.competition_id,
            "source_season_key": self.source_season_key,
            "stage_id": self.stage_id,
            "entity_id": self.entity_id,
            "batch_id": self.batch_id,
            "content_hash": self.content_hash,
            "raw_uri": self.raw_uri,
            "parser_version": self.parser_version,
            "status": self.status.value,
            "fetch_outcome": self.fetch_outcome,
            "http_status": self.http_status,
            "attempts": int(self.attempts),
            "retries": int(self.retries),
            "cache_hit": bool(self.cache_hit),
            "stale": bool(self.stale),
            "fetched_at": self.fetched_at,
            "completed_at": completed_at,
            "retry_after": self.retry_after,
            "direct_bytes": int(self.direct_bytes),
            "proxy_bytes": int(self.proxy_bytes),
            "encoded_bytes": int(self.encoded_bytes),
            "decoded_bytes": int(self.decoded_bytes),
            "expected_counts_json": stable_json(dict(self.expected_counts)),
            "actual_counts_json": stable_json(dict(self.actual_counts)),
            "capabilities_json": stable_json(dict(self.capabilities)),
            "exclusions_json": stable_json(list(self.exclusions)),
            "unknown_paths_json": stable_json(sorted(set(self.unknown_paths))),
            "error_code": self.error_code,
            "error": self.error,
        }


# Physical partitioning is intentionally source-native.  In particular,
# source_season_key is VARCHAR and is never derived from a year integer.
TABLE_PARTITIONS: dict[str, tuple[str, ...]] = {
    "fotmob_competitions": ("discovery_date",),
    "fotmob_competition_seasons": ("competition_id",),
    "fotmob_competition_season_history": ("competition_id",),
    "fotmob_season_stages": ("competition_id", "source_season_key"),
    "fotmob_matches": ("competition_id", "source_season_key"),
    "fotmob_standings": ("competition_id", "source_season_key"),
    "fotmob_leaderboard_categories": ("competition_id", "source_season_key"),
    "fotmob_leaderboards": ("competition_id", "source_season_key"),
    "fotmob_match_payloads": ("competition_id", "source_season_key"),
    "fotmob_team_snapshots": ("snapshot_date",),
    "fotmob_squad_snapshots": ("snapshot_date",),
    "fotmob_player_snapshots": ("snapshot_date",),
    "fotmob_transfer_events": ("event_year",),
    "fotmob_playoff_brackets": ("competition_id", "source_season_key"),
    "fotmob_season_teams": ("competition_id", "source_season_key"),
    "fotmob_field_inventory": ("target_type",),
    MANIFEST_TABLE: ("target_type",),
}


CURRENT_VIEW_SPECS: dict[str, tuple[str, tuple[str, ...]]] = {
    "fotmob_competitions": ("all_leagues", ("competition_id",)),
    "fotmob_competition_seasons": (
        "competition_seasons",
        ("competition_id", "source_season_key"),
    ),
    "fotmob_competition_season_history": (
        "competition_seasons",
        ("competition_id", "history_season_label"),
    ),
    "fotmob_season_stages": (
        "league_season",
        ("competition_id", "source_season_key", "stage_id"),
    ),
    "fotmob_matches": (
        "league_season",
        ("competition_id", "source_season_key", "match_id"),
    ),
    "fotmob_standings": (
        "league_season",
        (
            "competition_id",
            "source_season_key",
            "table_id",
            "table_name",
            "table_type",
            "team_id",
            "position",
        ),
    ),
    "fotmob_playoff_brackets": (
        "league_season",
        (
            "competition_id",
            "source_season_key",
            "stage_id",
            "draw_order",
            "home_team_id",
            "away_team_id",
        ),
    ),
    "fotmob_season_teams": (
        "league_season",
        ("competition_id", "source_season_key", "team_id"),
    ),
    "fotmob_leaderboard_categories": (
        "league_season",
        (
            "competition_id",
            "source_season_key",
            "participant_type",
            "source_order",
        ),
    ),
    "fotmob_leaderboards": (
        "leaderboard",
        (
            "competition_id",
            "source_season_key",
            "participant_type",
            "participant_id",
            "team_id",
            "stat_name",
            "rank",
            "top_list_index",
        ),
    ),
    "fotmob_match_payloads": (
        "match",
        ("competition_id", "source_season_key", "match_id"),
    ),
    "fotmob_team_snapshots": ("team", ("team_id",)),
    "fotmob_squad_snapshots": (
        "team",
        ("team_id", "member_type", "member_id"),
    ),
    "fotmob_player_snapshots": ("player", ("player_id",)),
    "fotmob_transfer_events": ("transfers_page", ("transfer_event_id",)),
}


# These entities are complete snapshots of one logical API target.  Restrict
# their current views to the newest successful batch per target before
# deduplicating natural keys.  Otherwise a fixture removed after postponement,
# a team leaving a table, or a player leaving a squad would survive forever
# from an older batch.  Catalog/history/event tables deliberately retain older
# natural keys and apply their own tombstone/history semantics.
REPLACE_TARGET_CURRENT_TABLES = frozenset(
    {
        "fotmob_season_stages",
        "fotmob_matches",
        "fotmob_standings",
        "fotmob_playoff_brackets",
        "fotmob_season_teams",
        "fotmob_leaderboard_categories",
        "fotmob_leaderboards",
        "fotmob_match_payloads",
        "fotmob_team_snapshots",
        "fotmob_squad_snapshots",
    }
)


# A physical URL is not always the logical snapshot identity.  In particular,
# the selected league payload may first arrive through ``?id=`` and then be
# durably rebound to ``?id=&season=``.  Choose the newest successful manifest
# per source entity/scope before applying each table's natural row key.
REPLACE_TARGET_MANIFEST_IDENTITIES: dict[str, tuple[str, ...]] = {
    "fotmob_season_stages": (
        "target_type",
        "competition_id",
        "source_season_key",
    ),
    "fotmob_matches": (
        "target_type",
        "competition_id",
        "source_season_key",
    ),
    "fotmob_standings": (
        "target_type",
        "competition_id",
        "source_season_key",
    ),
    "fotmob_playoff_brackets": (
        "target_type",
        "competition_id",
        "source_season_key",
    ),
    "fotmob_season_teams": (
        "target_type",
        "competition_id",
        "source_season_key",
    ),
    "fotmob_leaderboard_categories": (
        "target_type",
        "competition_id",
        "source_season_key",
    ),
    "fotmob_leaderboards": ("target_type", "target_key"),
    "fotmob_match_payloads": ("target_type", "entity_id"),
    "fotmob_team_snapshots": ("target_type", "entity_id"),
    "fotmob_squad_snapshots": ("target_type", "entity_id"),
}


class FotMobRepository:
    """Append-only physical writes plus manifest-backed logical commits."""

    def __init__(
        self,
        *,
        writer: Optional[RepositoryWriter] = None,
        catalog: str = "iceberg",
        schema: str = "bronze",
    ) -> None:
        self.writer = writer or IcebergWriter(catalog=catalog)
        self.catalog = catalog
        self.schema = schema

    def _write(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        *,
        entity_type: str,
        partition_cols: Optional[Sequence[str]] = None,
    ) -> Optional[str]:
        normalized = normalize_rows(rows)
        if not normalized:
            return None
        frame = pd.DataFrame(normalized)
        now = utc_now()
        if "_source" not in frame:
            frame["_source"] = "fotmob"
        if "_entity_type" not in frame:
            frame["_entity_type"] = entity_type
        if "_ingested_at" not in frame:
            frame["_ingested_at"] = now
        partitions = tuple(
            partition_cols
            if partition_cols is not None
            else TABLE_PARTITIONS.get(table, ())
        )
        missing = [column for column in partitions if column not in frame.columns]
        if missing:
            raise ValueError(f"{table}: partition columns absent from rows: {missing}")
        return self.writer.write_dataframe(
            frame,
            database=self.schema,
            table=table,
            partition_spec=[(column, "identity") for column in partitions] or None,
            add_metadata=False,
            source="fotmob",
        )

    def commit(
        self,
        commit: TargetCommit,
        datasets: Sequence[TableRows] = (),
    ) -> list[str]:
        """Write target datasets, then append the logical commit manifest.

        A non-success result must not carry rows.  For success, the repository
        derives actual counts and rejects disagreement with caller-provided
        counts before touching storage.  Replays of the same content/parser
        get the same batch id; current views can therefore deduplicate them.
        """

        if commit.status not in {ManifestStatus.SUCCESS, ManifestStatus.NOT_MODIFIED}:
            if any(dataset.rows for dataset in datasets):
                raise ValueError(
                    f"{commit.status.value} target cannot commit physical rows"
                )

        derived_counts = {
            dataset.entity_type: len(dataset.rows) for dataset in datasets
        }
        expected_mismatches = {
            entity_type: (int(expected), derived_counts[entity_type])
            for entity_type, expected in commit.expected_counts.items()
            if entity_type in derived_counts
            and int(expected) != derived_counts[entity_type]
        }
        if expected_mismatches:
            raise ValueError(
                "source expected counts disagree with physical datasets: "
                f"{expected_mismatches!r}"
            )
        if commit.actual_counts and dict(commit.actual_counts) != derived_counts:
            raise ValueError(
                "manifest actual counts disagree with physical datasets: "
                f"manifest={dict(commit.actual_counts)!r}, rows={derived_counts!r}"
            )

        # Rebuild a frozen commit only when counts were intentionally omitted.
        if not commit.actual_counts and derived_counts:
            values = asdict(commit)
            values["status"] = commit.status
            values["actual_counts"] = derived_counts
            commit = TargetCommit(**values)

        table_paths: list[str] = []
        for dataset in datasets:
            rows = normalize_rows(dataset.rows)
            for row in rows:
                row.setdefault("_target_batch_id", commit.batch_id)
                row.setdefault("_payload_sha256", commit.content_hash)
                row.setdefault("_parser_version", commit.parser_version)
                row.setdefault("_raw_uri", commit.raw_uri)
                row.setdefault("_observed_at", commit.fetched_at or utc_now())
            path = self._write(
                dataset.table,
                rows,
                entity_type=dataset.entity_type,
                partition_cols=(dataset.partition_cols or None),
            )
            if path:
                table_paths.append(path)

        manifest_path = self._write(
            MANIFEST_TABLE,
            [commit.manifest_row()],
            entity_type="ingest_manifest",
        )
        if manifest_path:
            table_paths.append(manifest_path)
        return table_paths

    def record(self, commit: TargetCommit) -> str:
        """Append a target state that carries no physical rows."""

        paths = self.commit(commit)
        return paths[-1]

    def ensure_schema(self) -> None:
        """Create the stable manifest and current logical views.

        Entity tables are created by their first typed dataframe because some
        FotMob capabilities are competition-specific.  The manifest schema is
        fixed so even a completely unavailable run remains observable.
        """

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return
        trino = manager_getter()
        trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.{MANIFEST_TABLE} (
                run_id VARCHAR,
                target_type VARCHAR,
                target_key VARCHAR,
                competition_id VARCHAR,
                source_season_key VARCHAR,
                stage_id VARCHAR,
                entity_id VARCHAR,
                batch_id VARCHAR,
                content_hash VARCHAR,
                raw_uri VARCHAR,
                parser_version VARCHAR,
                status VARCHAR,
                fetch_outcome VARCHAR,
                http_status INTEGER,
                attempts INTEGER,
                retries INTEGER,
                cache_hit BOOLEAN,
                stale BOOLEAN,
                fetched_at TIMESTAMP(6),
                completed_at TIMESTAMP(6),
                retry_after TIMESTAMP(6),
                direct_bytes BIGINT,
                proxy_bytes BIGINT,
                encoded_bytes BIGINT,
                decoded_bytes BIGINT,
                expected_counts_json VARCHAR,
                actual_counts_json VARCHAR,
                capabilities_json VARCHAR,
                exclusions_json VARCHAR,
                unknown_paths_json VARCHAR,
                error_code VARCHAR,
                error VARCHAR,
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6)
            ) WITH (partitioning = ARRAY['target_type'])
            """
        )

    def ensure_current_views(self) -> list[str]:
        """Create manifest-filtered, deduplicated ``*_current`` views.

        Physical writes are append-only.  These views expose only committed
        batches and choose the newest observation per canonical natural key;
        a crash before the manifest append is therefore invisible.
        """

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return []
        trino = manager_getter()
        created: list[str] = []
        for table, (target_types, natural_key) in CURRENT_VIEW_SPECS.items():
            if not trino.table_exists(self.schema, table):
                continue
            columns = trino.get_table_columns(self.schema, table)
            available = {str(item).lower() for item in columns}
            required_metadata = {
                "_target_batch_id",
                "_observed_at",
                "_ingested_at",
            }
            missing_metadata = sorted(required_metadata - available)
            if missing_metadata:
                raise ValueError(
                    f"{table}: current view commit columns are missing: "
                    f"{missing_metadata}"
                )
            missing_keys = [key for key in natural_key if key.lower() not in available]
            if missing_keys:
                raise ValueError(
                    f"{table}: current view natural-key columns are missing: "
                    f"{missing_keys}"
                )
            keys = list(natural_key)
            quoted_columns = ", ".join(f'"{column}"' for column in columns)
            partition = ", ".join(f'r."{key}"' for key in keys)
            types = ", ".join(
                "'" + item.strip().replace("'", "''") + "'"
                for item in target_types.split(",")
            )
            view = f"{table}_current"
            if table in REPLACE_TARGET_CURRENT_TABLES:
                manifest_identity = REPLACE_TARGET_MANIFEST_IDENTITIES.get(table)
                if not manifest_identity:
                    raise ValueError(
                        f"{table}: replacement target has no manifest identity"
                    )
                manifest_partition = ", ".join(manifest_identity)
                committed_cte = f"""
                successful_targets AS (
                    SELECT batch_id, target_key,
                           ROW_NUMBER() OVER (
                               PARTITION BY {manifest_partition}
                               ORDER BY completed_at DESC, batch_id DESC
                           ) AS target_rn
                    FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
                    WHERE target_type IN ({types})
                      AND status IN ('success', 'not_modified')
                ), committed AS (
                    SELECT DISTINCT batch_id
                    FROM successful_targets
                    WHERE target_rn = 1
                )
                """
            else:
                committed_cte = f"""
                committed AS (
                    SELECT DISTINCT batch_id
                    FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
                    WHERE target_type IN ({types})
                      AND status IN ('success', 'not_modified')
                )
                """
            trino._execute(
                f"""
                CREATE OR REPLACE VIEW {self.catalog}.{self.schema}.{view} AS
                WITH {committed_cte}, ranked AS (
                    SELECT r.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY {partition}
                               ORDER BY r._observed_at DESC, r._ingested_at DESC,
                                        r._target_batch_id DESC
                           ) AS _current_rn
                    FROM {self.catalog}.{self.schema}.{table} r
                    INNER JOIN committed c
                        ON c.batch_id = r._target_batch_id
                )
                SELECT {quoted_columns}
                FROM ranked
                WHERE _current_rn = 1
                """
            )
            created.append(f"{self.catalog}.{self.schema}.{view}")
        return created

    def latest_success(self, target_key: str) -> Optional[dict[str, Any]]:
        """Return the newest successful manifest for incremental planning."""

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return None
        trino = manager_getter()
        safe = str(target_key).replace("'", "''")
        rows = trino.execute_query(
            f"""
            SELECT target_key, batch_id, content_hash, raw_uri, parser_version,
                   status, fetched_at, completed_at, actual_counts_json,
                   capabilities_json
            FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
            WHERE target_key = '{safe}'
              AND status IN ('success', 'not_modified')
            ORDER BY completed_at DESC
            LIMIT 1
            """
        )
        if not rows:
            return None
        columns = (
            "target_key",
            "batch_id",
            "content_hash",
            "raw_uri",
            "parser_version",
            "status",
            "fetched_at",
            "completed_at",
            "actual_counts_json",
            "capabilities_json",
        )
        return dict(zip(columns, rows[0]))

    def latest_entity_success(
        self, target_type: str, entity_id: str | int
    ) -> Optional[dict[str, Any]]:
        """Return the latest logical success independent of a rotating URL.

        Next.js build ids change the physical player URL even when the player
        identity does not.  Incremental snapshot planning must therefore key
        freshness by ``(target_type, entity_id)`` rather than URL hash.
        """

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return None
        trino = manager_getter()
        safe_type = str(target_type).replace("'", "''")
        safe_id = str(entity_id).replace("'", "''")
        rows = trino.execute_query(
            f"""
            SELECT target_key, batch_id, content_hash, raw_uri, parser_version,
                   status, fetched_at, completed_at, actual_counts_json,
                   capabilities_json
            FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
            WHERE target_type = '{safe_type}'
              AND entity_id = '{safe_id}'
              AND status IN ('success', 'not_modified')
            ORDER BY completed_at DESC
            LIMIT 1
            """
        )
        if not rows:
            return None
        columns = (
            "target_key",
            "batch_id",
            "content_hash",
            "raw_uri",
            "parser_version",
            "status",
            "fetched_at",
            "completed_at",
            "actual_counts_json",
            "capabilities_json",
        )
        return dict(zip(columns, rows[0]))

    def successful_season_scopes(
        self,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> set[tuple[int, str]]:
        """Load all completed canonical season scopes with one manifest query."""

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return set()
        trino = manager_getter()
        safe_version = str(parser_version).replace("'", "''")
        rows = trino.execute_query(
            f"""
            SELECT DISTINCT competition_id, source_season_key
            FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
            WHERE target_type = 'league_season'
              AND status IN ('success', 'not_modified')
              AND parser_version = '{safe_version}'
              AND competition_id IS NOT NULL
              AND source_season_key IS NOT NULL
            """
        )
        output: set[tuple[int, str]] = set()
        for competition_id, source_season_key in rows:
            try:
                output.add((int(competition_id), str(source_season_key)))
            except (TypeError, ValueError):
                continue
        return output

    def completed_scope_keys(
        self,
        plan_signature: str,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> set[tuple[int, str]]:
        """Return scopes fully completed for an exact entity/policy plan."""

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return set()
        trino = manager_getter()
        safe_signature = str(plan_signature).replace("'", "''")
        safe_version = str(parser_version).replace("'", "''")
        rows = trino.execute_query(
            f"""
            SELECT DISTINCT competition_id, source_season_key
            FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
            WHERE target_type = 'scope_completion'
              AND entity_id = '{safe_signature}'
              AND status IN ('success', 'not_modified')
              AND parser_version = '{safe_version}'
              AND competition_id IS NOT NULL
              AND source_season_key IS NOT NULL
            """
        )
        output: set[tuple[int, str]] = set()
        for competition_id, source_season_key in rows:
            try:
                output.add((int(competition_id), str(source_season_key)))
            except (TypeError, ValueError):
                continue
        return output

    def scope_completion_times(
        self,
        plan_signature: str,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> dict[tuple[int, str], datetime]:
        """Return latest completion time for DAILY oldest-first fairness."""

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return {}
        trino = manager_getter()
        safe_signature = str(plan_signature).replace("'", "''")
        safe_version = str(parser_version).replace("'", "''")
        rows = trino.execute_query(
            f"""
            SELECT competition_id, source_season_key, MAX(completed_at)
            FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
            WHERE target_type = 'scope_completion'
              AND entity_id = '{safe_signature}'
              AND status IN ('success', 'not_modified')
              AND parser_version = '{safe_version}'
              AND competition_id IS NOT NULL
              AND source_season_key IS NOT NULL
            GROUP BY competition_id, source_season_key
            """
        )
        output: dict[tuple[int, str], datetime] = {}
        for competition_id, source_season_key, completed_at in rows:
            try:
                timestamp = completed_at
                if not isinstance(timestamp, datetime):
                    timestamp = datetime.fromisoformat(str(timestamp))
                output[(int(competition_id), str(source_season_key))] = timestamp
            except (TypeError, ValueError):
                continue
        return output

    def completed_competition_ids(
        self,
        plan_signature: str,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> set[int]:
        """Return competition-wide streams completed for an exact plan."""

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return set()
        trino = manager_getter()
        safe_signature = str(plan_signature).replace("'", "''")
        safe_version = str(parser_version).replace("'", "''")
        rows = trino.execute_query(
            f"""
            SELECT DISTINCT competition_id
            FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
            WHERE target_type = 'competition_completion'
              AND entity_id = '{safe_signature}'
              AND status IN ('success', 'not_modified')
              AND parser_version = '{safe_version}'
              AND competition_id IS NOT NULL
              AND source_season_key IS NULL
            """
        )
        output: set[int] = set()
        for row in rows:
            competition_id = row[0] if isinstance(row, (tuple, list)) else row
            try:
                output.add(int(competition_id))
            except (TypeError, ValueError):
                continue
        return output

    def competition_completion_times(
        self,
        plan_signature: str,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> dict[int, datetime]:
        """Return latest competition completion time for DAILY fairness."""

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return {}
        trino = manager_getter()
        safe_signature = str(plan_signature).replace("'", "''")
        safe_version = str(parser_version).replace("'", "''")
        rows = trino.execute_query(
            f"""
            SELECT competition_id, MAX(completed_at)
            FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
            WHERE target_type = 'competition_completion'
              AND entity_id = '{safe_signature}'
              AND status IN ('success', 'not_modified')
              AND parser_version = '{safe_version}'
              AND competition_id IS NOT NULL
              AND source_season_key IS NULL
            GROUP BY competition_id
            """
        )
        output: dict[int, datetime] = {}
        for competition_id, completed_at in rows:
            try:
                timestamp = completed_at
                if not isinstance(timestamp, datetime):
                    timestamp = datetime.fromisoformat(str(timestamp))
                output[int(competition_id)] = timestamp
            except (TypeError, ValueError):
                continue
        return output

    def current_squad_player_ids(self, team_id: str | int) -> set[int]:
        """Load player IDs from the latest committed snapshot of one team."""

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return set()
        trino = manager_getter()
        table = "fotmob_squad_snapshots_current"
        if not trino.table_exists(self.schema, table):
            return set()
        safe_id = str(team_id).replace("'", "''")
        rows = trino.execute_query(
            f"""
            SELECT DISTINCT member_id
            FROM {self.catalog}.{self.schema}.{table}
            WHERE team_id = '{safe_id}'
              AND member_type = 'player'
              AND member_id IS NOT NULL
            """
        )
        output: set[int] = set()
        for row in rows:
            member_id = row[0] if isinstance(row, (tuple, list)) else row
            try:
                output.add(int(member_id))
            except (TypeError, ValueError):
                continue
        return output

    def latest_catalog_presence(self) -> dict[str, tuple[bool, bool]]:
        """Return presence in the two newest complete discovery snapshots.

        Consumers can tombstone only when both values are false.  A single
        missing snapshot therefore never removes a source competition.
        """

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return {}
        trino = manager_getter()
        if not trino.table_exists(self.schema, "fotmob_competitions"):
            return {}
        rows = trino.execute_query(
            f"""
            WITH committed AS (
                SELECT DISTINCT batch_id
                FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
                WHERE target_type = 'all_leagues'
                  AND status IN ('success', 'not_modified')
            ), runs AS (
                SELECT discovery_run_id,
                       MAX(_observed_at) AS observed_at,
                       DENSE_RANK() OVER (ORDER BY MAX(_observed_at) DESC) AS rn
                FROM {self.catalog}.{self.schema}.fotmob_competitions c
                INNER JOIN committed m
                    ON m.batch_id = c._target_batch_id
                GROUP BY discovery_run_id
            ), ids AS (
                SELECT competition_id,
                       MAX(CASE WHEN r.rn = 1 THEN 1 ELSE 0 END) AS newest,
                       MAX(CASE WHEN r.rn = 2 THEN 1 ELSE 0 END) AS previous
                FROM {self.catalog}.{self.schema}.fotmob_competitions c
                JOIN runs r USING (discovery_run_id)
                WHERE r.rn <= 2
                GROUP BY competition_id
            )
            SELECT competition_id, newest, previous FROM ids
            """
        )
        return {
            str(competition_id): (bool(newest), bool(previous))
            for competition_id, newest, previous in rows
        }

    def previous_catalog_snapshots(self, limit: int = 2) -> list[set[int]]:
        """Return newest complete catalog ID sets before the current run."""

        if limit < 1:
            return []
        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return []
        trino = manager_getter()
        if not trino.table_exists(self.schema, "fotmob_competitions"):
            return []
        rows = trino.execute_query(
            f"""
            WITH committed AS (
                SELECT DISTINCT batch_id
                FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
                WHERE target_type = 'all_leagues'
                  AND status IN ('success', 'not_modified')
            ), runs AS (
                SELECT discovery_run_id, MAX(c._observed_at) AS observed_at
                FROM {self.catalog}.{self.schema}.fotmob_competitions c
                INNER JOIN committed m
                    ON m.batch_id = c._target_batch_id
                GROUP BY discovery_run_id
                ORDER BY observed_at DESC
                LIMIT {int(limit)}
            )
            SELECT c.discovery_run_id, c.competition_id
            FROM {self.catalog}.{self.schema}.fotmob_competitions c
            INNER JOIN runs r USING (discovery_run_id)
            WHERE COALESCE(c.is_tombstoned, FALSE) = FALSE
            ORDER BY r.observed_at DESC
            """
        )
        output: list[set[int]] = []
        by_run: dict[str, set[int]] = {}
        for run_id, competition_id in rows:
            key = str(run_id)
            if key not in by_run:
                by_run[key] = set()
                output.append(by_run[key])
            try:
                by_run[key].add(int(competition_id))
            except (TypeError, ValueError):
                continue
        return output[:limit]


class MemoryFotMobRepository:
    """Small repository double used by service/replay tests and benchmarks."""

    def __init__(self) -> None:
        self.commits: list[TargetCommit] = []
        self.tables: dict[str, list[dict[str, Any]]] = {}

    def ensure_schema(self) -> None:
        return None

    def ensure_current_views(self) -> list[str]:
        return []

    def commit(
        self,
        commit: TargetCommit,
        datasets: Sequence[TableRows] = (),
    ) -> list[str]:
        if commit.status not in {ManifestStatus.SUCCESS, ManifestStatus.NOT_MODIFIED}:
            if any(dataset.rows for dataset in datasets):
                raise ValueError("failed targets cannot carry rows")
        derived = {dataset.entity_type: len(dataset.rows) for dataset in datasets}
        expected_mismatches = {
            entity_type: (int(expected), derived[entity_type])
            for entity_type, expected in commit.expected_counts.items()
            if entity_type in derived and int(expected) != derived[entity_type]
        }
        if expected_mismatches:
            raise ValueError("source expected count mismatch")
        if commit.actual_counts and dict(commit.actual_counts) != derived:
            raise ValueError("actual count mismatch")
        if not commit.actual_counts and derived:
            values = asdict(commit)
            values["status"] = commit.status
            values["actual_counts"] = derived
            commit = TargetCommit(**values)
        self.commits.append(commit)
        for dataset in datasets:
            target = self.tables.setdefault(dataset.table, [])
            for raw_row in dataset.rows:
                row = dict(raw_row)
                row.setdefault("_target_batch_id", commit.batch_id)
                row.setdefault("_payload_sha256", commit.content_hash)
                row.setdefault("_parser_version", commit.parser_version)
                row.setdefault("_raw_uri", commit.raw_uri)
                target.append(row)
        return [f"memory://{dataset.table}" for dataset in datasets] + [
            f"memory://{MANIFEST_TABLE}"
        ]

    def record(self, commit: TargetCommit) -> str:
        self.commit(commit)
        return f"memory://{MANIFEST_TABLE}"

    def latest_success(self, target_key: str) -> Optional[dict[str, Any]]:
        for commit in reversed(self.commits):
            if (
                commit.target_key == target_key
                and commit.status.value in SUCCESS_STATES
            ):
                return commit.manifest_row()
        return None

    def latest_entity_success(
        self, target_type: str, entity_id: str | int
    ) -> Optional[dict[str, Any]]:
        for commit in reversed(self.commits):
            if (
                commit.target_type == str(target_type)
                and commit.entity_id == str(entity_id)
                and commit.status.value in SUCCESS_STATES
            ):
                return commit.manifest_row()
        return None

    def successful_season_scopes(
        self,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> set[tuple[int, str]]:
        return {
            (int(commit.competition_id), str(commit.source_season_key))
            for commit in self.commits
            if commit.target_type == "league_season"
            and commit.status.value in SUCCESS_STATES
            and commit.parser_version == parser_version
            and commit.competition_id is not None
            and commit.source_season_key is not None
        }

    def completed_scope_keys(
        self,
        plan_signature: str,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> set[tuple[int, str]]:
        return {
            (int(commit.competition_id), str(commit.source_season_key))
            for commit in self.commits
            if commit.target_type == "scope_completion"
            and commit.entity_id == str(plan_signature)
            and commit.status.value in SUCCESS_STATES
            and commit.parser_version == parser_version
            and commit.competition_id is not None
            and commit.source_season_key is not None
        }

    def scope_completion_times(
        self,
        plan_signature: str,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> dict[tuple[int, str], datetime]:
        output: dict[tuple[int, str], datetime] = {}
        for commit in self.commits:
            if (
                commit.target_type != "scope_completion"
                or commit.entity_id != str(plan_signature)
                or commit.status.value not in SUCCESS_STATES
                or commit.parser_version != parser_version
                or commit.competition_id is None
                or commit.source_season_key is None
            ):
                continue
            completed_at = commit.completed_at or commit.fetched_at
            if completed_at is None:
                continue
            key = (int(commit.competition_id), str(commit.source_season_key))
            if key not in output or completed_at > output[key]:
                output[key] = completed_at
        return output

    def completed_competition_ids(
        self,
        plan_signature: str,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> set[int]:
        return {
            int(commit.competition_id)
            for commit in self.commits
            if commit.target_type == "competition_completion"
            and commit.entity_id == str(plan_signature)
            and commit.status.value in SUCCESS_STATES
            and commit.parser_version == parser_version
            and commit.competition_id is not None
            and commit.source_season_key is None
        }

    def competition_completion_times(
        self,
        plan_signature: str,
        *,
        parser_version: str = PARSER_VERSION,
    ) -> dict[int, datetime]:
        output: dict[int, datetime] = {}
        for commit in self.commits:
            if (
                commit.target_type != "competition_completion"
                or commit.entity_id != str(plan_signature)
                or commit.status.value not in SUCCESS_STATES
                or commit.parser_version != parser_version
                or commit.competition_id is None
                or commit.source_season_key is not None
            ):
                continue
            completed_at = commit.completed_at or commit.fetched_at
            if completed_at is None:
                continue
            key = int(commit.competition_id)
            if key not in output or completed_at > output[key]:
                output[key] = completed_at
        return output

    def current_squad_player_ids(self, team_id: str | int) -> set[int]:
        latest = self.latest_entity_success("team", team_id)
        if latest is None:
            return set()
        batch_id = latest.get("batch_id")
        output: set[int] = set()
        for row in self.tables.get("fotmob_squad_snapshots", []):
            if (
                str(row.get("team_id")) != str(team_id)
                or row.get("member_type") != "player"
                or row.get("_target_batch_id") != batch_id
            ):
                continue
            try:
                output.add(int(row["member_id"]))
            except (KeyError, TypeError, ValueError):
                continue
        return output

    def previous_catalog_snapshots(self, limit: int = 2) -> list[set[int]]:
        rows = self.tables.get("fotmob_competitions", [])
        by_run: dict[str, set[int]] = {}
        order: list[str] = []
        for row in reversed(rows):
            run_id = str(row.get("discovery_run_id"))
            if run_id not in by_run:
                by_run[run_id] = set()
                order.append(run_id)
            if not row.get("is_tombstoned"):
                by_run[run_id].add(int(row["competition_id"]))
        return [by_run[run_id] for run_id in order[:limit]]
