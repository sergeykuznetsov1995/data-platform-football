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
import logging
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence

import pandas as pd

from scrapers.base.iceberg_writer import IcebergWriter


logger = logging.getLogger(__name__)

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


# Field-inventory rows carry no target identity: every match of a season emits
# the same ~600 (target_type, competition, season, json_path, disposition) rows.
# Fifty buffered targets therefore staged ~30k rows of which ~600 were distinct,
# and that single table accounted for most of the run's Trino statements (#930).
DEDUP_KEYS: dict[str, tuple[str, ...]] = {
    "fotmob_field_inventory": (
        "target_type",
        "competition_id",
        "source_season_key",
        "json_path",
        "disposition",
    ),
}

# Player inventory keys carry no scope columns, so one preload would pull the
# whole ~3.8M-key population for rows the run-level dedup already collapses.
INVENTORY_PRELOAD_SKIP: frozenset[str] = frozenset({"player"})


def _dedup_key_value(value: Any) -> Optional[str]:
    """Normalize a dedup key value to its canonical string spelling.

    Live rows carry ints while the table column is VARCHAR holding both '53'
    and '53.0' spellings (pandas floats int columns in frames that mix scoped
    and scope-less rows); comparing un-normalized values would silently never
    match across that boundary.
    """

    if value is None:
        return None
    text = str(value)
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _completed_at_key(view: Mapping[str, Any]) -> str:
    """Order manifest views by completion time without assuming a dtype."""

    return str(view.get("completed_at") or "")


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

    # Manifest columns the incremental planner reads back. Buffered commits
    # must answer those reads from memory, otherwise batching would hide a
    # target that this very run already committed.
    _READ_COLUMNS = (
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

    def __init__(
        self,
        *,
        writer: Optional[RepositoryWriter] = None,
        catalog: str = "iceberg",
        schema: str = "bronze",
        batch_size: int = 1,
        max_buffered_rows: int = 20_000,
    ) -> None:
        self.writer = writer or IcebergWriter(catalog=catalog)
        self.catalog = catalog
        self.schema = schema
        # One Iceberg commit per target makes the manifest table grow one
        # single-row data file per target; commit cost then scales with the
        # file count (production: 4.3k files -> 9.5 s per one-row insert).
        # Buffering N targets into one commit per table keeps that cost flat.
        self.batch_size = max(1, int(batch_size))
        self.max_buffered_rows = max(1, int(max_buffered_rows))
        self._pending: dict[
            tuple[str, str, Optional[tuple[str, ...]]], list[dict[str, Any]]
        ] = {}
        self._pending_manifest: list[dict[str, Any]] = []
        # Dedup keys deliberately outlive flush(): inventory rows carry no
        # target identity, so a key seen once needs no second row this run —
        # matches of one season share almost every json_path, and re-emitting
        # them each flush wrote ~2.4M rows per iteration where ~200k were new.
        self._seen_keys: dict[str, set[tuple[Any, ...]]] = {}
        self._seeded_scopes: set[tuple[Any, ...]] = set()
        self._pending_targets: dict[str, dict[str, Any]] = {}
        self._pending_entities: dict[tuple[str, str], dict[str, Any]] = {}
        self._pending_rows = 0
        # Incremental planning asks "did we already ingest this target?" once
        # per target. As a Trino round-trip that was the single most expensive
        # thing the backfill did (678 of ~1900 queries per 40 min, ~7 queries
        # and ~13 s per target). Preloading the answer once makes those reads
        # free; commits keep the index current so read-your-writes still holds.
        self._manifest_index: dict[str, dict[str, Any]] = {}
        self._entity_index: dict[tuple[str, str], dict[str, Any]] = {}
        self._preloaded = False

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

        prepared: list[
            tuple[str, str, Optional[tuple[str, ...]], list[dict[str, Any]]]
        ] = []
        for dataset in datasets:
            rows = normalize_rows(dataset.rows)
            if not rows:
                continue
            for row in rows:
                row.setdefault("_target_batch_id", commit.batch_id)
                row.setdefault("_payload_sha256", commit.content_hash)
                row.setdefault("_parser_version", commit.parser_version)
                row.setdefault("_raw_uri", commit.raw_uri)
                row.setdefault("_observed_at", commit.fetched_at or utc_now())
            prepared.append(
                (
                    dataset.table,
                    dataset.entity_type,
                    tuple(dataset.partition_cols) if dataset.partition_cols else None,
                    rows,
                )
            )
        manifest_row = commit.manifest_row()

        if self.batch_size <= 1:
            table_paths: list[str] = []
            for table, entity_type, partition_cols, rows in prepared:
                path = self._write(
                    table,
                    rows,
                    entity_type=entity_type,
                    partition_cols=partition_cols,
                )
                if path:
                    table_paths.append(path)
            manifest_path = self._write(
                MANIFEST_TABLE, [manifest_row], entity_type="ingest_manifest"
            )
            if manifest_path:
                table_paths.append(manifest_path)
            self._index_committed(manifest_row)
            return table_paths

        for table, entity_type, partition_cols, rows in prepared:
            rows = self._deduplicate(table, rows)
            if not rows:
                continue
            self._pending.setdefault((table, entity_type, partition_cols), []).extend(
                rows
            )
            self._pending_rows += len(rows)
        self._pending_manifest.append(manifest_row)
        self._index_pending(manifest_row)
        if (
            len(self._pending_manifest) >= self.batch_size
            or self._pending_rows >= self.max_buffered_rows
        ):
            return self.flush()
        return []

    def _deduplicate(
        self, table: str, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Drop rows this run already emitted under the same logical key.

        Only tables whose rows carry no target identity are deduplicated (see
        ``DEDUP_KEYS``); the surviving row keeps a batch id that this run has
        already committed (or commits in the same flush), so manifest gating
        is unaffected.  The seen-set survives flush() on purpose: a failed
        flush keeps both the buffer and the keys, so a retry re-appends the
        very same rows.  Note that manifest ``actual_counts`` for these tables
        mean "observed by the parser", not "physically written" — counts are
        derived before deduplication.
        """

        key_columns = DEDUP_KEYS.get(table)
        if not key_columns:
            return rows
        seen = self._seen_keys.setdefault(table, set())
        output: list[dict[str, Any]] = []
        for row in rows:
            key = tuple(_dedup_key_value(row.get(column)) for column in key_columns)
            self._seed_scope_keys(table, key_columns, key)
            if key in seen:
                continue
            seen.add(key)
            output.append(row)
        return output

    def _seed_scope_keys(
        self,
        table: str,
        key_columns: tuple[str, ...],
        key: tuple[Optional[str], ...],
    ) -> None:
        """Lazily fold a scope's already-written dedup keys into the seen-set.

        Iterations resume mid-scope, so the json paths a season's matches
        share were usually written by an earlier run already — re-learning
        them re-writes ~150k inventory rows per scope.  One ``SELECT
        DISTINCT`` per (target_type, competition, season) replaces that.  Any
        failure degrades to run-local dedup and never blocks the write path.
        """

        scope = (table,) + key[:3]
        if key[0] in INVENTORY_PRELOAD_SKIP or scope in self._seeded_scopes:
            return
        # Mark first: a failing query must not retry once per row.
        self._seeded_scopes.add(scope)
        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return
        try:
            trino = manager_getter()
            if not trino.table_exists(self.schema, table):
                return
            conditions = []
            for column, value in zip(key_columns[:3], key[:3]):
                if value is None:
                    conditions.append(f"{column} IS NULL")
                    continue
                safe = value.replace("'", "''")
                variants = {safe}
                if safe.isdigit():
                    variants.add(f"{safe}.0")  # historical pandas spelling
                in_list = ", ".join(f"'{v}'" for v in sorted(variants))
                conditions.append(f"{column} IN ({in_list})")
            tail_columns = ", ".join(key_columns[3:])
            rows = trino.execute_query(
                f"""
                SELECT DISTINCT {tail_columns}
                FROM {self.catalog}.{self.schema}.{table}
                WHERE {" AND ".join(conditions)}
                """
            )
        except Exception as exc:
            logger.warning("Inventory key preload failed for %s: %s", scope, exc)
            return
        seen = self._seen_keys.setdefault(table, set())
        for row in rows:
            seen.add(key[:3] + tuple(_dedup_key_value(value) for value in row))

    def _index_pending(self, manifest_row: Mapping[str, Any]) -> None:
        """Make a buffered commit visible to this run's incremental reads."""

        if manifest_row.get("status") not in SUCCESS_STATES:
            return
        view = {column: manifest_row.get(column) for column in self._READ_COLUMNS}
        self._pending_targets[str(manifest_row.get("target_key"))] = view
        entity_id = manifest_row.get("entity_id")
        if entity_id is not None:
            key = (str(manifest_row.get("target_type")), str(entity_id))
            self._pending_entities[key] = view

    def _index_committed(self, manifest_row: Mapping[str, Any]) -> None:
        """Fold a durable commit into the preloaded index."""

        if not self._preloaded or manifest_row.get("status") not in SUCCESS_STATES:
            return
        view = {column: manifest_row.get(column) for column in self._READ_COLUMNS}
        self._manifest_index[str(manifest_row.get("target_key"))] = view
        entity_id = manifest_row.get("entity_id")
        if entity_id is not None:
            key = (str(manifest_row.get("target_type")), str(entity_id))
            self._entity_index[key] = view

    def preload_manifest_index(self) -> int:
        """Load every committed target once so per-target reads never query.

        The index is authoritative afterwards: a key that is absent was never
        committed, so a miss answers ``None`` without a Trino round-trip.
        Commits update it, which keeps replay/dedup decisions correct.
        """

        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return 0
        trino = manager_getter()
        if not trino.table_exists(self.schema, MANIFEST_TABLE):
            self._preloaded = True
            return 0
        columns = ", ".join(self._READ_COLUMNS)
        rows = trino.execute_query(
            f"""
            SELECT {columns}, target_type, entity_id
            FROM (
                SELECT {columns}, target_type, entity_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY target_key ORDER BY completed_at DESC
                       ) AS rn
                FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE}
                WHERE status IN ('success', 'not_modified')
            )
            WHERE rn = 1
            """
        )
        width = len(self._READ_COLUMNS)
        for row in rows:
            view = dict(zip(self._READ_COLUMNS, row[:width]))
            target_type, entity_id = row[width], row[width + 1]
            self._manifest_index[str(view["target_key"])] = view
            if entity_id is not None:
                key = (str(target_type), str(entity_id))
                previous = self._entity_index.get(key)
                # A rotating Next.js build id gives one entity several target
                # keys; freshness is keyed by identity, so keep the newest.
                if previous is None or _completed_at_key(view) >= _completed_at_key(
                    previous
                ):
                    self._entity_index[key] = view
        self._preloaded = True
        return len(self._manifest_index)

    def flush(self) -> list[str]:
        """Write every buffered target as one Iceberg commit per table.

        Physical rows go first and the manifest last, exactly as in the
        unbuffered path: a crash between the two can only lose visibility of
        rows (``*_current`` views are manifest-gated), never claim rows that
        were never written.  The buffer is cleared only after the manifest
        lands, so a failed flush stays retryable; a retry re-appends rows under
        the same deterministic ``_target_batch_id``, which the views collapse.
        """

        if not self._pending and not self._pending_manifest:
            return []
        paths: list[str] = []
        for (table, entity_type, partition_cols), rows in self._pending.items():
            path = self._write(
                table,
                rows,
                entity_type=entity_type,
                partition_cols=partition_cols,
            )
            if path:
                paths.append(path)
        manifest_path = self._write(
            MANIFEST_TABLE, self._pending_manifest, entity_type="ingest_manifest"
        )
        if manifest_path:
            paths.append(manifest_path)
        # Fold the flushed commits into the durable index: clearing the pending
        # buffer must not make a target this run already ingested look absent.
        for manifest_row in self._pending_manifest:
            self._index_committed(manifest_row)
        self._pending = {}
        self._pending_manifest = []
        self._pending_targets = {}
        self._pending_entities = {}
        self._pending_rows = 0
        return paths

    def record(self, commit: TargetCommit) -> str:
        """Append a target state that carries no physical rows."""

        paths = self.commit(commit)
        return paths[-1] if paths else ""

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

        buffered = self._pending_targets.get(str(target_key))
        if buffered is not None:
            return buffered
        if self._preloaded:
            return self._manifest_index.get(str(target_key))
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

        key = (str(target_type), str(entity_id))
        buffered = self._pending_entities.get(key)
        if buffered is not None:
            return buffered
        if self._preloaded:
            return self._entity_index.get(key)
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

    def _buffered_squad_player_ids(self, team_id: str | int) -> set[int]:
        """Player IDs of a squad this run committed but has not flushed yet."""

        wanted = str(team_id)
        output: set[int] = set()
        for (table, _, _), rows in self._pending.items():
            if table != "fotmob_squad_snapshots":
                continue
            for row in rows:
                if str(row.get("team_id")) != wanted:
                    continue
                if row.get("member_type") != "player":
                    continue
                try:
                    output.add(int(row.get("member_id")))
                except (TypeError, ValueError):
                    continue
        return output

    def current_squad_player_ids(self, team_id: str | int) -> set[int]:
        """Load player IDs from the latest committed snapshot of one team."""

        buffered = self._buffered_squad_player_ids(team_id)
        manager_getter = getattr(self.writer, "_get_trino_manager", None)
        if manager_getter is None:
            return buffered
        trino = manager_getter()
        table = "fotmob_squad_snapshots_current"
        if not trino.table_exists(self.schema, table):
            return buffered
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
        output: set[int] = set(buffered)
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
            SELECT discovery_run_id, c.competition_id
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

    def flush(self) -> list[str]:
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
