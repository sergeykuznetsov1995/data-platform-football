"""Iceberg persistence for idempotent WhoScored ingestion.

The repository deliberately treats the match manifest as the commit point.
Event and lineup rows may be appended independently, but downstream readers
only expose the batch referenced by the latest successful manifest row.  This
keeps a task crash from replacing a previously complete game with a partial
one and makes Airflow retries network-free when the raw payload is cached.
Preview rows use the same append-then-manifest protocol; a successful zero-row
manifest hides an older non-empty injury snapshot without an Iceberg DELETE.
"""

from __future__ import annotations

import hashlib
import json
import os
import pickle
import re
import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import fcntl
import pandas as pd
import pyarrow as pa

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoTableManager
from scrapers.whoscored.catalog import WhoScoredCatalog
from scrapers.whoscored.domain import WhoScoredScope
from scrapers.whoscored.parsers import MATCH_AVAILABILITY_VERSION, PARSER_VERSION
from scrapers.whoscored.profile_policy import MAX_DAILY_PROFILE_CANDIDATES
from scrapers.whoscored.runtime_contract import require_production_runtime_class


MATCH_MANIFEST_TABLE = "whoscored_match_ingest_manifest"
PREVIEW_MANIFEST_TABLE = "whoscored_preview_ingest_manifest"
PROFILE_VERSIONS_TABLE = "whoscored_player_profile_versions"
PROFILE_MANIFEST_TABLE = "whoscored_profile_ingest_manifest"
CATALOG_MANIFEST_TABLE = "whoscored_catalog_manifest"
SCOPE_MANIFEST_TABLE = "whoscored_scope_ingest_manifest"
MATCH_COMPLETION_GRACE = timedelta(hours=3)
MATCH_REFRESH_DAYS = 7
PREVIEW_REFRESH_HOURS = 6
PROFILE_REFRESH_DAYS = 90

# Parser v7 catalog manifests predate the canonical row-order fingerprint used
# by v8. Their physical rows no longer retain enough ordering information to
# recompute that legacy digest, but an explicit full-history v8 migration still
# needs the last committed catalog for loss detection. This narrow allowlist is
# accepted only when the caller opts into the migration path below; normal
# reads remain pinned to the current parser and its reproducible fingerprint.
_LEGACY_CATALOG_MIGRATION_PARSERS = frozenset({"whoscored-parser-v7"})

# Source business datasets.  Operational manifests and request telemetry are
# intentionally not counted here.
WHOSCORED_BUSINESS_TABLES = (
    "whoscored_competitions",
    "whoscored_seasons",
    "whoscored_stages",
    "whoscored_schedule",
    "whoscored_match_incidents",
    "whoscored_match_bets",
    "whoscored_stage_standings",
    "whoscored_stage_forms",
    "whoscored_stage_streaks",
    "whoscored_stage_performance",
    "whoscored_matches",
    "whoscored_events",
    "whoscored_lineups",
    "whoscored_substitutions",
    "whoscored_formations",
    "whoscored_team_match_stats",
    "whoscored_player_match_stats",
    "whoscored_team_stage_stats",
    "whoscored_player_stage_stats",
    "whoscored_referee_stage_stats",
    "whoscored_preview_lineups",
    "whoscored_missing_players",
    "whoscored_preview_sections",
    PROFILE_VERSIONS_TABLE,
    "whoscored_player_stage_participations",
)

MATCH_DATASET_TABLES = {
    "matches": "whoscored_matches",
    "events": "whoscored_events",
    "lineups": "whoscored_lineups",
    "substitutions": "whoscored_substitutions",
    "formations": "whoscored_formations",
    "team_match_stats": "whoscored_team_match_stats",
    "player_match_stats": "whoscored_player_match_stats",
}

PREVIEW_DATASET_TABLES = {
    "missing_players": "whoscored_missing_players",
    "preview_lineups": "whoscored_preview_lineups",
    "preview_sections": "whoscored_preview_sections",
}

SCOPE_DATASET_TABLES = {
    "whoscored_schedule",
    "whoscored_match_incidents",
    "whoscored_match_bets",
    "whoscored_stage_standings",
    "whoscored_stage_forms",
    "whoscored_stage_streaks",
    "whoscored_stage_performance",
    "whoscored_team_stage_stats",
    "whoscored_player_stage_stats",
    "whoscored_referee_stage_stats",
}

# Betting offers are an expiring source snapshot: WhoScored removes providers
# and prices after a match starts or finishes.  The physical Iceberg batches
# remain append-only, while the current manifest is allowed to publish the
# smaller source snapshot.  Other scope datasets retain the strict no-shrink
# completeness guard.
SCOPE_SHRINKABLE_DATASET_TABLES = {"whoscored_match_bets"}

DEFAULT_SCOPE_WRITE_CHUNK_ROWS = 20_000
MAX_SCOPE_WRITE_CHUNK_ROWS = 100_000
_SPOOL_INSERT_BATCH_ROWS = 256


def scope_write_chunk_rows_from_env(
    environ: Mapping[str, str] = os.environ,
) -> int:
    """Return the bounded number of scope rows materialised for one write."""

    raw = environ.get(
        "WHOSCORED_SCOPE_WRITE_CHUNK_ROWS",
        str(DEFAULT_SCOPE_WRITE_CHUNK_ROWS),
    )
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "WHOSCORED_SCOPE_WRITE_CHUNK_ROWS must be an integer in 1..100000"
        ) from exc
    if str(raw).strip() != str(value) or not 1 <= value <= MAX_SCOPE_WRITE_CHUNK_ROWS:
        raise ValueError(
            "WHOSCORED_SCOPE_WRITE_CHUNK_ROWS must be an integer in 1..100000"
        )
    return value


class WhoScoredScopeRowSpool:
    """Disk-backed, re-iterable set of entity-keyed scope rows.

    Stage statistics expand compact source documents into millions of long-form
    rows.  Keeping those Python dictionaries for every stage made two
    LocalExecutor tasks capable of exhausting the 16 GiB worker.  This spool
    preserves the exact row dictionaries in a private SQLite file, enforces the
    same SHA-256 entity-key deduplication as the former in-memory helper, and
    materialises only a small fetch batch while iterating.

    A stage savepoint lets the service discard a partially parsed stage if the
    source proves that its statistics UI is unavailable after an earlier feed
    batch succeeded.  The file is ephemeral staging only; raw S3 objects and
    the Iceberg manifest remain the durable sources of truth.
    """

    def __init__(
        self,
        *,
        table: str,
        league: str,
        season: str,
        directory: Optional[str] = None,
    ) -> None:
        if table not in SCOPE_DATASET_TABLES:
            raise ValueError(f"unsupported WhoScored scope spool table {table!r}")
        self.table = table
        self.league = str(league)
        self.season = str(season)
        configured_root = (
            directory or os.environ.get("WHOSCORED_SCOPE_SPOOL_DIR", "").strip() or None
        )
        if configured_root is not None:
            spool_root = Path(configured_root).expanduser()
            spool_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not spool_root.is_dir():
                raise ValueError("WhoScored scope spool root must be a directory")
            configured_root = str(spool_root)
        self._temporary = tempfile.TemporaryDirectory(
            prefix="whoscored-scope-",
            dir=configured_root,
        )
        os.chmod(self._temporary.name, 0o700)
        self.path = Path(self._temporary.name) / "rows.sqlite3"
        self._connection = sqlite3.connect(str(self.path))
        # A disk journal keeps per-stage rollback bounded by disk rather than
        # retaining the undo log in Python/SQLite heap memory.
        self._connection.execute("PRAGMA journal_mode=DELETE")
        self._connection.execute("PRAGMA synchronous=OFF")
        self._connection.execute("PRAGMA temp_store=FILE")
        self._connection.execute("PRAGMA cache_size=-2048")
        self._connection.execute(
            "CREATE TABLE rows ("
            "ordinal INTEGER PRIMARY KEY AUTOINCREMENT, "
            "entity_key TEXT NOT NULL UNIQUE, payload BLOB NOT NULL)"
        )
        self._connection.commit()
        self._count = 0
        self._columns: set[str] = set()
        self._stage_snapshot: Optional[tuple[int, set[str]]] = None
        self._closed = False

    @staticmethod
    def _entity_key(row: Mapping[str, Any]) -> str:
        payload = json.dumps(
            dict(row),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def begin_stage(self) -> None:
        if self._stage_snapshot is not None:
            raise RuntimeError(f"{self.table} already has an open stage savepoint")
        self._connection.execute("SAVEPOINT source_stage")
        self._stage_snapshot = (self._count, set(self._columns))

    def commit_stage(self) -> None:
        if self._stage_snapshot is None:
            raise RuntimeError(f"{self.table} has no open stage savepoint")
        self._connection.execute("RELEASE SAVEPOINT source_stage")
        self._stage_snapshot = None

    def rollback_stage(self) -> None:
        if self._stage_snapshot is None:
            return
        count, columns = self._stage_snapshot
        self._connection.execute("ROLLBACK TO SAVEPOINT source_stage")
        self._connection.execute("RELEASE SAVEPOINT source_stage")
        self._count = count
        self._columns = columns
        self._stage_snapshot = None

    def append_entity_rows(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Append rows with stable entity keys without retaining the iterable."""

        pending: list[tuple[str, sqlite3.Binary]] = []

        def flush() -> None:
            if not pending:
                return
            before = self._connection.total_changes
            self._connection.executemany(
                "INSERT OR IGNORE INTO rows(entity_key, payload) VALUES (?, ?)",
                pending,
            )
            self._count += self._connection.total_changes - before
            pending.clear()

        for source in rows:
            row = dict(source)
            if "entity_key" in row:
                raise ValueError(f"{self.table} source row already has entity_key")
            for column, expected in (
                ("league", self.league),
                ("season", self.season),
            ):
                value = row.get(column)
                if value is not None and str(value) != expected:
                    raise ValueError(
                        f"{self.table} contains {column}={value!r} outside {expected!r}"
                    )
            key = self._entity_key(row)
            row["entity_key"] = key
            self._columns.update(str(column) for column in row)
            pending.append(
                (
                    key,
                    sqlite3.Binary(pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)),
                )
            )
            if len(pending) >= _SPOOL_INSERT_BATCH_ROWS:
                flush()
        flush()
        if self._stage_snapshot is None:
            self._connection.commit()

    @property
    def columns(self) -> frozenset[str]:
        return frozenset(self._columns)

    @property
    def on_disk_bytes(self) -> int:
        return self.path.stat().st_size if self.path.exists() else 0

    def content_fingerprint(self) -> str:
        """Hash exact row identities without deserializing the row payloads."""

        digest = hashlib.sha256(b"whoscored-scope-spool-v1\0")
        cursor = self._connection.execute(
            "SELECT entity_key FROM rows ORDER BY ordinal"
        )
        try:
            while True:
                batch = cursor.fetchmany(_SPOOL_INSERT_BATCH_ROWS)
                if not batch:
                    return digest.hexdigest()
                for (entity_key,) in batch:
                    digest.update(str(entity_key).encode("ascii"))
                    digest.update(b"\n")
        finally:
            cursor.close()

    def __len__(self) -> int:
        return self._count

    def __enter__(self) -> "WhoScoredScopeRowSpool":
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.close()

    def __iter__(self):
        cursor = self._connection.execute("SELECT payload FROM rows ORDER BY ordinal")
        try:
            while True:
                batch = cursor.fetchmany(_SPOOL_INSERT_BATCH_ROWS)
                if not batch:
                    return
                for (payload,) in batch:
                    row = pickle.loads(payload)
                    if not isinstance(row, dict):  # pragma: no cover - internal DB
                        raise RuntimeError(f"{self.table} spool payload is invalid")
                    yield row
        finally:
            cursor.close()

    def close(self) -> None:
        if self._closed:
            return
        self.rollback_stage()
        self._connection.close()
        self._temporary.cleanup()
        self._closed = True


# The physical schema of every source business table is a source contract, not
# an observation about whichever DataFrame happened to be written first.  Keep
# operational manifests out of this mapping: their schemas are declared next
# to the commit protocol that owns them.
_METADATA_COLUMNS = {
    "_source": "VARCHAR",
    "_entity_type": "VARCHAR",
    "_ingested_at": "TIMESTAMP(6)",
    "_batch_id": "VARCHAR",
}
_SOURCE_METADATA_COLUMNS = {
    "source_raw_json": "VARCHAR",
    "source_schema_fingerprint": "VARCHAR",
}
_CATALOG_STORAGE_COLUMNS = {
    "record_key": "VARCHAR",
    "payload_json": "VARCHAR",
    "_catalog_batch_id": "VARCHAR",
    **_METADATA_COLUMNS,
}
_SCOPE_STORAGE_COLUMNS = {
    "batch_schema_fingerprint": "VARCHAR",
    "_scope_batch_id": "VARCHAR",
    "_payload_sha256": "VARCHAR",
    "_parser_version": "VARCHAR",
    **_METADATA_COLUMNS,
}
_MATCH_STORAGE_COLUMNS = {
    "batch_schema_fingerprint": "VARCHAR",
    "_game_batch_id": "VARCHAR",
    "_payload_sha256": "VARCHAR",
    "_parser_version": "VARCHAR",
    **_METADATA_COLUMNS,
}
_PREVIEW_STORAGE_COLUMNS = {
    "batch_schema_fingerprint": "VARCHAR",
    "_preview_batch_id": "VARCHAR",
    "_payload_sha256": "VARCHAR",
    "_parser_version": "VARCHAR",
    **_METADATA_COLUMNS,
}
_MATCH_IDENTITY_COLUMNS = {
    "league": "VARCHAR",
    "season": "VARCHAR",
    "game": "VARCHAR",
    "game_id": "BIGINT",
}
_STAT_LEAF_COLUMNS = {
    "category": "VARCHAR",
    "subcategory": "VARCHAR",
    "stat": "VARCHAR",
    "filter": "VARCHAR",
    "minute": "BIGINT",
    "numeric_value": "DOUBLE",
    "text_value": "VARCHAR",
    "boolean_value": "BOOLEAN",
    "value_json": "VARCHAR",
    "source_path": "VARCHAR",
}
_SEASON_TABLE_COLUMNS = {
    "entity_key": "VARCHAR",
    "league": "VARCHAR",
    "season": "VARCHAR",
    "source_season_id": "BIGINT",
    "table_index": "BIGINT",
    "block_index": "BIGINT",
    "row_index": "BIGINT",
    "table_type": "VARCHAR",
    "source_path": "VARCHAR",
    "stage_id": "BIGINT",
    "start_date": "VARCHAR",
    "end_date": "VARCHAR",
    "team_id": "BIGINT",
    "team": "VARCHAR",
    "rank": "BIGINT",
    "played": "BIGINT",
    "points": "BIGINT",
    "group_name": "VARCHAR",
    "source_values_json": "VARCHAR",
    **_SOURCE_METADATA_COLUMNS,
    "table_raw_json": "VARCHAR",
    "table_schema_fingerprint": "VARCHAR",
    **_SCOPE_STORAGE_COLUMNS,
}
_STAGE_STAT_COLUMNS = {
    "entity_key": "VARCHAR",
    "league": "VARCHAR",
    "season": "VARCHAR",
    "source_season_id": "BIGINT",
    "stage_id": "BIGINT",
    "row_index": "BIGINT",
    "entity_type": "VARCHAR",
    "source_category": "VARCHAR",
    "source_subcategory": "VARCHAR",
    "team_id": "BIGINT",
    "team": "VARCHAR",
    "player_id": "BIGINT",
    "player": "VARCHAR",
    "referee_id": "BIGINT",
    "referee": "VARCHAR",
    "rank": "BIGINT",
    **_STAT_LEAF_COLUMNS,
    "record_schema_fingerprint": "VARCHAR",
    "document_schema_fingerprint": "VARCHAR",
    **_SCOPE_STORAGE_COLUMNS,
}

WHOSCORED_BUSINESS_COLUMN_CONTRACTS: dict[str, dict[str, str]] = {
    "whoscored_competitions": {
        "competition_id": "VARCHAR",
        "region_id": "BIGINT",
        "region_name": "VARCHAR",
        "region_code": "VARCHAR",
        "region_flag": "VARCHAR",
        "tournament_id": "BIGINT",
        "tournament_name": "VARCHAR",
        "tournament_url": "VARCHAR",
        "sort_order": "BIGINT",
        "source_sex": "BIGINT",
        "eligibility": "VARCHAR",
        "classification_reason": "VARCHAR",
        "classifier_version": "VARCHAR",
        "override_version": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_CATALOG_STORAGE_COLUMNS,
    },
    "whoscored_seasons": {
        "competition_id": "VARCHAR",
        "region_id": "BIGINT",
        "tournament_id": "BIGINT",
        "season_id": "VARCHAR",
        "source_season_id": "BIGINT",
        "source_label": "VARCHAR",
        "season_format": "VARCHAR",
        "source_url": "VARCHAR",
        "start": "DATE",
        "end": "DATE",
        "source_selected": "BOOLEAN",
        "is_active": "BOOLEAN",
        "eligibility": "VARCHAR",
        "classification_reason": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_CATALOG_STORAGE_COLUMNS,
    },
    "whoscored_stages": {
        "competition_id": "VARCHAR",
        "league": "VARCHAR",
        "season": "VARCHAR",
        "region_id": "BIGINT",
        "tournament_id": "BIGINT",
        "league_id": "BIGINT",
        "source_season_id": "BIGINT",
        "season_id": "BIGINT",
        "stage_id": "BIGINT",
        "stage": "VARCHAR",
        "source_url": "VARCHAR",
        "eligibility": "VARCHAR",
        "classification_reason": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_CATALOG_STORAGE_COLUMNS,
    },
    "whoscored_schedule": {
        "league": "VARCHAR",
        "season": "VARCHAR",
        "region_id": "BIGINT",
        "region_code": "VARCHAR",
        "region_name": "VARCHAR",
        "tournament_id": "BIGINT",
        "tournament_name": "VARCHAR",
        "source_season_id": "BIGINT",
        "source_season_name": "VARCHAR",
        "source_sex": "BIGINT",
        "game": "VARCHAR",
        "game_id": "BIGINT",
        "date": "TIMESTAMP(6)",
        "home_team": "VARCHAR",
        "away_team": "VARCHAR",
        "home_team_id": "BIGINT",
        "away_team_id": "BIGINT",
        "home_score": "BIGINT",
        "away_score": "BIGINT",
        "status": "BIGINT",
        "match_is_opta": "BOOLEAN",
        "has_preview": "BOOLEAN",
        "stage_id": "BIGINT",
        "stage": "VARCHAR",
        "aggregate_winner_field": "VARCHAR",
        "extra_result_field": "VARCHAR",
        # These source fields have historically mixed numbers, empty strings
        # and textual aggregate markers.  Production Bronze already exposes
        # them as VARCHAR; keep the additive V3 contract write-compatible.
        "home_extratime_score": "VARCHAR",
        "away_extratime_score": "VARCHAR",
        "home_penalty_score": "VARCHAR",
        "away_penalty_score": "VARCHAR",
        "home_red_cards": "BIGINT",
        "away_red_cards": "BIGINT",
        "home_yellow_cards": "BIGINT",
        "away_yellow_cards": "BIGINT",
        "home_team_country_code": "VARCHAR",
        "away_team_country_code": "VARCHAR",
        "home_team_country_name": "VARCHAR",
        "away_team_country_name": "VARCHAR",
        "bets": "VARCHAR",
        "incidents": "VARCHAR",
        "comment_count": "BIGINT",
        "elapsed": "VARCHAR",
        "first_half_ended_at_utc": "VARCHAR",
        "has_incidents_summary": "BOOLEAN",
        "is_lineup_confirmed": "BOOLEAN",
        "is_stream_available": "BOOLEAN",
        "is_top_match": "BOOLEAN",
        "last_scorer": "DOUBLE",
        "period": "BIGINT",
        "score_changed_at": "VARCHAR",
        "second_half_started_at_utc": "VARCHAR",
        "start_time": "VARCHAR",
        "started_at_utc": "VARCHAR",
        "winner_field": "DOUBLE",
        **_SOURCE_METADATA_COLUMNS,
        **_SCOPE_STORAGE_COLUMNS,
    },
    "whoscored_match_incidents": {
        "entity_key": "VARCHAR",
        "league": "VARCHAR",
        "season": "VARCHAR",
        "game_id": "BIGINT",
        "game": "VARCHAR",
        "stage_id": "BIGINT",
        "stage": "VARCHAR",
        "match_is_opta": "BOOLEAN",
        "source_ordinal": "BIGINT",
        "source_path": "VARCHAR",
        "source_incident_id": "VARCHAR",
        "incident_type": "VARCHAR",
        "incident_subtype": "VARCHAR",
        "minute": "BIGINT",
        "expanded_minute": "BIGINT",
        "period": "VARCHAR",
        "field": "VARCHAR",
        "team_id": "BIGINT",
        "team": "VARCHAR",
        "player_id": "BIGINT",
        "player": "VARCHAR",
        "participating_player_id": "BIGINT",
        "participating_player": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_SCOPE_STORAGE_COLUMNS,
    },
    "whoscored_match_bets": {
        "entity_key": "VARCHAR",
        "league": "VARCHAR",
        "season": "VARCHAR",
        "game_id": "BIGINT",
        "game": "VARCHAR",
        "stage_id": "BIGINT",
        "stage": "VARCHAR",
        "source_outcome": "VARCHAR",
        "source_bet_id": "VARCHAR",
        "bet_name": "VARCHAR",
        "source_offer_ordinal": "BIGINT",
        "provider_id": "BIGINT",
        "betting_provider": "VARCHAR",
        "odds_decimal": "DOUBLE",
        "odds_fractional": "VARCHAR",
        "odds_us": "VARCHAR",
        "clickout_url": "VARCHAR",
        "source_path": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_SCOPE_STORAGE_COLUMNS,
    },
    "whoscored_stage_standings": dict(_SEASON_TABLE_COLUMNS),
    "whoscored_stage_forms": dict(_SEASON_TABLE_COLUMNS),
    "whoscored_stage_streaks": dict(_SEASON_TABLE_COLUMNS),
    "whoscored_stage_performance": dict(_SEASON_TABLE_COLUMNS),
    "whoscored_matches": {
        **_MATCH_IDENTITY_COLUMNS,
        "home_team_id": "BIGINT",
        "home_team": "VARCHAR",
        "away_team_id": "BIGINT",
        "away_team": "VARCHAR",
        "home_score": "BIGINT",
        "away_score": "BIGINT",
        "status": "VARCHAR",
        "period": "VARCHAR",
        "expanded_max_minute": "BIGINT",
        "attendance": "BIGINT",
        "venue_name": "VARCHAR",
        "referee_id": "BIGINT",
        "referee_name": "VARCHAR",
        "weather": "VARCHAR",
        "start_time": "VARCHAR",
        "home_manager": "VARCHAR",
        "away_manager": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_MATCH_STORAGE_COLUMNS,
    },
    "whoscored_events": {
        **_MATCH_IDENTITY_COLUMNS,
        "source_event_id": "BIGINT",
        "opta_event_id": "BIGINT",
        "team_event_id": "BIGINT",
        "period": "VARCHAR",
        "minute": "BIGINT",
        # Legacy Bronze inferred this nullable clock component as DOUBLE.
        # Normalise every V3 batch to the same explicit physical type.
        "second": "DOUBLE",
        "expanded_minute": "BIGINT",
        "type": "VARCHAR",
        "outcome_type": "VARCHAR",
        "team_id": "BIGINT",
        "team": "VARCHAR",
        "player_id": "BIGINT",
        "player": "VARCHAR",
        "x": "DOUBLE",
        "y": "DOUBLE",
        "end_x": "DOUBLE",
        "end_y": "DOUBLE",
        "goal_mouth_y": "DOUBLE",
        "goal_mouth_z": "DOUBLE",
        "blocked_x": "DOUBLE",
        "blocked_y": "DOUBLE",
        "qualifiers": "VARCHAR",
        "is_touch": "BOOLEAN",
        "is_shot": "BOOLEAN",
        "is_goal": "BOOLEAN",
        "card_type": "VARCHAR",
        "related_team_event_id": "BIGINT",
        "related_player_id": "BIGINT",
        "satisfied_events_types": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_MATCH_STORAGE_COLUMNS,
    },
    "whoscored_lineups": {
        **_MATCH_IDENTITY_COLUMNS,
        "team_id": "BIGINT",
        "team": "VARCHAR",
        "side": "VARCHAR",
        "player_id": "BIGINT",
        "player": "VARCHAR",
        "shirt_no": "DOUBLE",
        "position": "VARCHAR",
        "is_starter": "BOOLEAN",
        "is_man_of_the_match": "BOOLEAN",
        "subbed_in_expanded_minute": "DOUBLE",
        "subbed_out_expanded_minute": "DOUBLE",
        "minutes_played": "DOUBLE",
        "rating": "DOUBLE",
        "height": "DOUBLE",
        "weight": "DOUBLE",
        "age": "DOUBLE",
        **_SOURCE_METADATA_COLUMNS,
        **_MATCH_STORAGE_COLUMNS,
    },
    "whoscored_substitutions": {
        **_MATCH_IDENTITY_COLUMNS,
        "side": "VARCHAR",
        "team_id": "BIGINT",
        "team": "VARCHAR",
        "player_id": "BIGINT",
        "player": "VARCHAR",
        "action": "VARCHAR",
        "expanded_minute": "BIGINT",
        "related_player_id": "BIGINT",
        **_SOURCE_METADATA_COLUMNS,
        **_MATCH_STORAGE_COLUMNS,
    },
    "whoscored_formations": {
        **_MATCH_IDENTITY_COLUMNS,
        "side": "VARCHAR",
        "team_id": "BIGINT",
        "team": "VARCHAR",
        "formation_index": "BIGINT",
        "formation_id": "BIGINT",
        "formation_name": "VARCHAR",
        "start_expanded_minute": "BIGINT",
        "end_expanded_minute": "BIGINT",
        "captain_player_id": "BIGINT",
        "player_ids": "VARCHAR",
        "formation_slots": "VARCHAR",
        "formation_positions": "VARCHAR",
        "jersey_numbers": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_MATCH_STORAGE_COLUMNS,
    },
    "whoscored_team_match_stats": {
        **_MATCH_IDENTITY_COLUMNS,
        "side": "VARCHAR",
        "team_id": "BIGINT",
        "team": "VARCHAR",
        "player_id": "BIGINT",
        "player": "VARCHAR",
        **_STAT_LEAF_COLUMNS,
        "source_schema_fingerprint": "VARCHAR",
        **_MATCH_STORAGE_COLUMNS,
    },
    "whoscored_player_match_stats": {
        **_MATCH_IDENTITY_COLUMNS,
        "side": "VARCHAR",
        "team_id": "BIGINT",
        "team": "VARCHAR",
        "player_id": "BIGINT",
        "player": "VARCHAR",
        **_STAT_LEAF_COLUMNS,
        "source_schema_fingerprint": "VARCHAR",
        **_MATCH_STORAGE_COLUMNS,
    },
    "whoscored_team_stage_stats": {
        **_STAGE_STAT_COLUMNS,
        # Positional stagestatfeed rows retain their complete source tuple;
        # summary/Detailed rows share the record/document fingerprints below.
        **_SOURCE_METADATA_COLUMNS,
    },
    "whoscored_player_stage_stats": dict(_STAGE_STAT_COLUMNS),
    "whoscored_referee_stage_stats": dict(_STAGE_STAT_COLUMNS),
    "whoscored_preview_lineups": {
        **_MATCH_IDENTITY_COLUMNS,
        "side": "VARCHAR",
        "team": "VARCHAR",
        "player_id": "BIGINT",
        "player": "VARCHAR",
        "position": "VARCHAR",
        "formation": "VARCHAR",
        "rating": "DOUBLE",
        "source_path": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_PREVIEW_STORAGE_COLUMNS,
    },
    "whoscored_missing_players": {
        **_MATCH_IDENTITY_COLUMNS,
        "team": "VARCHAR",
        "player": "VARCHAR",
        "player_id": "BIGINT",
        "reason": "VARCHAR",
        "status": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_PREVIEW_STORAGE_COLUMNS,
    },
    "whoscored_preview_sections": {
        **_MATCH_IDENTITY_COLUMNS,
        "section_type": "VARCHAR",
        "source": "VARCHAR",
        "heading": "VARCHAR",
        "text": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        **_PREVIEW_STORAGE_COLUMNS,
    },
    PROFILE_VERSIONS_TABLE: {
        "player_id": "BIGINT",
        "name": "VARCHAR",
        "current_team_id": "BIGINT",
        "current_team_name": "VARCHAR",
        "shirt_number": "INTEGER",
        "age": "INTEGER",
        "date_of_birth": "DATE",
        "height_cm": "INTEGER",
        "nationality": "VARCHAR",
        "country_code": "VARCHAR",
        "positions": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        "payload_sha256": "VARCHAR",
        "raw_uri": "VARCHAR",
        "parser_version": "VARCHAR",
        "fetched_at": "TIMESTAMP(6)",
        "_profile_batch_id": "VARCHAR",
        **_METADATA_COLUMNS,
    },
    "whoscored_player_stage_participations": {
        "player_id": "BIGINT",
        "record_type": "VARCHAR",
        "region_id": "BIGINT",
        "tournament_id": "BIGINT",
        "source_season_id": "BIGINT",
        "stage_id": "BIGINT",
        "game_id": "BIGINT",
        "tournament": "VARCHAR",
        "season": "VARCHAR",
        "stage": "VARCHAR",
        "team_id": "BIGINT",
        "team": "VARCHAR",
        "position": "VARCHAR",
        "source_path": "VARCHAR",
        **_SOURCE_METADATA_COLUMNS,
        "payload_json": "VARCHAR",
        "_profile_batch_id": "VARCHAR",
        "_payload_sha256": "VARCHAR",
        "_parser_version": "VARCHAR",
        **_METADATA_COLUMNS,
    },
}

if set(WHOSCORED_BUSINESS_COLUMN_CONTRACTS) != set(WHOSCORED_BUSINESS_TABLES):
    missing = sorted(
        set(WHOSCORED_BUSINESS_TABLES) - set(WHOSCORED_BUSINESS_COLUMN_CONTRACTS)
    )
    extra = sorted(
        set(WHOSCORED_BUSINESS_COLUMN_CONTRACTS) - set(WHOSCORED_BUSINESS_TABLES)
    )
    raise RuntimeError(
        f"invalid WhoScored business schema contracts: missing={missing}, extra={extra}"
    )


class BatchConflict(RuntimeError):
    """An existing physical batch disagrees with the parsed row counts."""


def _lock_commit_sequence(method):
    """Serialize identical batch identities across Airflow task processes."""

    @wraps(method)
    def wrapped(self, commits, *args, **kwargs):
        ordered = tuple(commits)
        keys = []
        for commit in ordered:
            if hasattr(commit, "game_id"):
                identity = (
                    f"{getattr(commit, 'league', '')}:"
                    f"{getattr(commit, 'season', '')}:"
                    f"{int(commit.game_id)}"
                )
            else:
                identity = f"player:{int(commit.player_id)}"
            keys.append(f"{method.__name__}:{identity}")
        with self._commit_locks(keys):
            return method(self, ordered, *args, **kwargs)

    return wrapped


def _lock_catalog_commit(method):
    @wraps(method)
    def wrapped(self, catalog, *args, **kwargs):
        with self._commit_locks(("catalog",)):
            return method(self, catalog, *args, **kwargs)

    return wrapped


def _lock_scope_commit(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        identity = ":".join(
            str(kwargs.get(name) or "") for name in ("league", "season", "entity_group")
        )
        with self._commit_locks((f"scope:{identity}",)):
            return method(self, *args, **kwargs)

    return wrapped


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _clean_unicode(value: str) -> str:
    """Turn JSON surrogate pairs into valid Unicode and replace lone halves."""
    try:
        value.encode("utf-8")
        return value
    except UnicodeEncodeError:
        return value.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "replace")


def _clean_json_unicode(value: Any) -> Any:
    if isinstance(value, str):
        return _clean_unicode(value)
    if isinstance(value, Mapping):
        return {
            _clean_unicode(str(key)): _clean_json_unicode(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_clean_json_unicode(item) for item in value]
    return value


def canonical_catalog_rows(
    rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Return the order-independent canonical catalog wire representation."""

    canonical: dict[str, tuple[dict[str, Any], ...]] = {}
    for kind in ("competitions", "seasons", "stages"):
        materialised = [
            dict(_clean_json_unicode(dict(row))) for row in rows.get(kind, ())
        ]
        canonical[kind] = tuple(
            sorted(
                materialised,
                key=lambda row: json.dumps(
                    row,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
            )
        )
    return canonical


def catalog_payload_sha256(
    rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> str:
    """Hash all canonical catalog rows for manifest/read-back integrity."""

    payload = json.dumps(
        canonical_catalog_rows(rows),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_catalog_raw_inputs(
    raw_inputs: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    """Return the exact order-independent catalog provenance representation."""

    materialised = [dict(_clean_json_unicode(dict(item))) for item in raw_inputs]
    return tuple(
        sorted(
            materialised,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
        )
    )


def catalog_raw_provenance_sha256(
    raw_inputs: Sequence[Mapping[str, Any]],
) -> str:
    """Hash the complete canonical list of raw observations."""

    payload = json.dumps(
        canonical_catalog_raw_inputs(raw_inputs),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _catalog_manifest_as_of_date(value: Any) -> date:
    """Decode one exact manifest DATE without accepting datetime coercion."""

    if type(value) is date:
        return value
    if type(value) is str and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise BatchConflict("catalog manifest has an invalid as_of_date") from exc
        if parsed.isoformat() == value:
            return parsed
    raise BatchConflict("catalog manifest has an invalid as_of_date")


def _validate_catalog_raw_provenance(
    *,
    batch_id: str,
    raw_inputs_json: str,
    raw_provenance_sha256: str,
    as_of_date: date,
) -> None:
    """Verify direct inputs or their content-addressed provenance descriptor."""

    if re.fullmatch(r"[0-9a-f]{64}", raw_provenance_sha256) is None:
        raise BatchConflict(f"catalog {batch_id} has an invalid raw provenance sha256")
    try:
        decoded = json.loads(raw_inputs_json)
    except (TypeError, ValueError) as exc:
        raise BatchConflict(f"catalog {batch_id} has invalid raw_inputs_json") from exc
    if not isinstance(decoded, list) or not decoded:
        raise BatchConflict(
            f"catalog {batch_id} raw_inputs_json must be a non-empty list"
        )
    if any(not isinstance(item, dict) for item in decoded):
        raise BatchConflict(f"catalog {batch_id} raw_inputs_json contains a non-object")
    expected_as_of = as_of_date.isoformat()
    if any(str(item.get("as_of_date") or "") != expected_as_of for item in decoded):
        raise BatchConflict(
            f"catalog {batch_id} raw provenance is not bound to as_of_date"
        )
    if catalog_raw_provenance_sha256(decoded) == raw_provenance_sha256:
        return
    descriptors = [
        item
        for item in decoded
        if str(item.get("target_id") or "").startswith("whoscored:catalog-provenance:")
    ]
    if len(descriptors) == 1:
        descriptor = descriptors[0]
        if (
            str(descriptor.get("target_id") or "")
            == f"whoscored:catalog-provenance:{batch_id}"
            and str(descriptor.get("payload_sha256") or "") == raw_provenance_sha256
            and str(descriptor.get("raw_uri") or "")
            and type(descriptor.get("input_count")) is int
            and int(descriptor["input_count"]) > 0
        ):
            return
    raise BatchConflict(
        f"catalog {batch_id} raw provenance hash is not reconstructible "
        "from its manifest"
    )


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _coerce_bool(value: Any) -> bool:
    """Coerce source/driver booleans without treating ``"false"`` as true."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    normalised = str(value).strip().lower()
    if normalised in {"true", "1", "yes", "y", "t"}:
        return True
    if normalised in {"false", "0", "no", "n", "f", ""}:
        return False
    raise ValueError(f"invalid boolean value from WhoScored/Trino: {value!r}")


MATCH_BATCH_ID_PREFIX = "ws2-v3-"
PREVIEW_BATCH_ID_PREFIX = "wsp2-v3-"
MATCH_NOT_AVAILABLE_BATCH_ID_PREFIX = "wsna2-v3-"
PREVIEW_NOT_AVAILABLE_BATCH_ID_PREFIX = "wspna2-v3-"
PROFILE_BATCH_ID_PREFIX = "wspr2-v3-"
PROFILE_NOT_AVAILABLE_BATCH_ID_PREFIX = "wsprna2-"


def deterministic_game_batch_id(
    game_id: int,
    payload_sha256: str,
    parser_version: str = PARSER_VERSION,
    *,
    league: str,
    season: str,
) -> str:
    """Return the V3 physical identity bound to its complete logical scope."""

    value = json.dumps(
        {
            "game_id": int(game_id),
            "league": league,
            "parser_version": parser_version,
            "payload_sha256": payload_sha256,
            "season": season,
            "version": "match-v3",
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return MATCH_BATCH_ID_PREFIX + hashlib.sha256(value).hexdigest()


def deterministic_preview_batch_id(
    game_id: int,
    payload_sha256: str,
    parser_version: str = PARSER_VERSION,
    *,
    league: str,
    season: str,
) -> str:
    """Return the V3 preview identity bound to its complete logical scope."""

    value = json.dumps(
        {
            "game_id": int(game_id),
            "league": league,
            "parser_version": parser_version,
            "payload_sha256": payload_sha256,
            "season": season,
            "version": "preview-v3",
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return PREVIEW_BATCH_ID_PREFIX + hashlib.sha256(value).hexdigest()


def deterministic_profile_not_available_batch_id(
    player_id: int,
    *,
    failure_code: str,
    http_status: Optional[int],
    payload_sha256: Optional[str],
    raw_uri: Optional[str],
    parser_version: str = PARSER_VERSION,
    availability_version: str = MATCH_AVAILABILITY_VERSION,
) -> str:
    """Bind one durable source-owned profile absence proof to its player."""

    if int(player_id) <= 0:
        raise ValueError("profile not-available player_id must be positive")
    if not str(failure_code):
        raise ValueError("profile not-available failure_code is required")
    identity = json.dumps(
        {
            "availability_version": str(availability_version),
            "failure_code": str(failure_code),
            "http_status": int(http_status) if http_status is not None else None,
            "parser_version": str(parser_version),
            "payload_sha256": (
                str(payload_sha256) if payload_sha256 is not None else None
            ),
            "player_id": int(player_id),
            "raw_uri": str(raw_uri) if raw_uri is not None else None,
            "state": "not_available",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return PROFILE_NOT_AVAILABLE_BATCH_ID_PREFIX + hashlib.sha256(identity).hexdigest()


def _deterministic_game_not_available_batch_id(
    prefix: str,
    kind: str,
    game_id: int,
    *,
    league: str,
    season: str,
    failure_code: str,
    http_status: Optional[int],
    payload_sha256: Optional[str],
    raw_uri: Optional[str],
    parser_version: str,
    availability_version: str,
) -> str:
    if int(game_id) <= 0 or not league or not season or not str(failure_code):
        raise ValueError(f"{kind} not-available identity is incomplete")
    identity = json.dumps(
        {
            "availability_version": str(availability_version),
            "failure_code": str(failure_code),
            "game_id": int(game_id),
            "http_status": int(http_status) if http_status is not None else None,
            "kind": kind,
            "league": str(league),
            "parser_version": str(parser_version),
            "payload_sha256": (
                str(payload_sha256) if payload_sha256 is not None else None
            ),
            "raw_uri": str(raw_uri) if raw_uri is not None else None,
            "season": str(season),
            "state": "not_available",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return prefix + hashlib.sha256(identity).hexdigest()


def deterministic_match_not_available_batch_id(
    game_id: int,
    *,
    league: str,
    season: str,
    failure_code: str,
    http_status: Optional[int],
    payload_sha256: Optional[str],
    raw_uri: Optional[str],
    parser_version: str = PARSER_VERSION,
    availability_version: str = MATCH_AVAILABILITY_VERSION,
) -> str:
    return _deterministic_game_not_available_batch_id(
        MATCH_NOT_AVAILABLE_BATCH_ID_PREFIX,
        "match",
        game_id,
        league=league,
        season=season,
        failure_code=failure_code,
        http_status=http_status,
        payload_sha256=payload_sha256,
        raw_uri=raw_uri,
        parser_version=parser_version,
        availability_version=availability_version,
    )


def deterministic_preview_not_available_batch_id(
    game_id: int,
    *,
    league: str,
    season: str,
    failure_code: str,
    http_status: Optional[int],
    payload_sha256: Optional[str],
    raw_uri: Optional[str],
    parser_version: str = PARSER_VERSION,
    availability_version: str = MATCH_AVAILABILITY_VERSION,
) -> str:
    return _deterministic_game_not_available_batch_id(
        PREVIEW_NOT_AVAILABLE_BATCH_ID_PREFIX,
        "preview",
        game_id,
        league=league,
        season=season,
        failure_code=failure_code,
        http_status=http_status,
        payload_sha256=payload_sha256,
        raw_uri=raw_uri,
        parser_version=parser_version,
        availability_version=availability_version,
    )


@dataclass(frozen=True)
class MatchCandidate:
    game_id: int
    league: str
    season: str
    game: str
    kickoff: Optional[datetime]
    status: int
    match_is_opta: bool
    attempt_no: int = 1


@dataclass(frozen=True)
class MatchCommit:
    game_id: int
    league: str
    season: str
    game: str
    payload_sha256: str
    raw_uri: str
    events: Sequence[Mapping[str, Any]]
    lineups: Sequence[Mapping[str, Any]]
    lineups_available: bool
    transport_mode: str
    proxy_mode: str = "none"
    http_status: int = 200
    direct_bytes: int = 0
    paid_bytes: int = 0
    parser_version: str = PARSER_VERSION
    availability_version: str = MATCH_AVAILABILITY_VERSION
    kickoff: Optional[datetime] = None
    fetched_at: Optional[datetime] = None
    attempt_no: int = 1
    # Additional matchCentre datasets share the same logical commit point.
    # ``events`` and ``lineups`` remain explicit for the existing consumers.
    datasets: Mapping[str, Sequence[Mapping[str, Any]]] = field(default_factory=dict)
    dataset_statuses: Mapping[str, str] = field(default_factory=dict)
    schema_fingerprint: Optional[str] = None
    is_opta: Optional[bool] = None
    schedule_status: Optional[int] = None

    @property
    def batch_id(self) -> str:
        return deterministic_game_batch_id(
            self.game_id,
            self.payload_sha256,
            self.parser_version,
            league=self.league,
            season=self.season,
        )


@dataclass(frozen=True)
class ManifestFailure:
    game_id: int
    league: str
    season: str
    state: str
    failure_code: str
    error: str
    retry_after: Optional[datetime]
    attempt_no: int
    game: Optional[str] = None
    kickoff: Optional[datetime] = None
    is_opta: Optional[bool] = None
    transport_mode: str = "none"
    proxy_mode: str = "none"
    http_status: Optional[int] = None
    direct_bytes: int = 0
    paid_bytes: int = 0
    payload_sha256: Optional[str] = None
    raw_uri: Optional[str] = None
    parser_version: str = PARSER_VERSION
    availability_version: str = MATCH_AVAILABILITY_VERSION


@dataclass(frozen=True)
class PreviewCommit:
    game_id: int
    league: str
    season: str
    game: str
    payload_sha256: str
    raw_uri: str
    missing_players: Sequence[Mapping[str, Any]]
    transport_mode: str
    proxy_mode: str = "none"
    http_status: int = 200
    direct_bytes: int = 0
    paid_bytes: int = 0
    parser_version: str = PARSER_VERSION
    availability_version: str = MATCH_AVAILABILITY_VERSION
    kickoff: Optional[datetime] = None
    fetched_at: Optional[datetime] = None
    attempt_no: int = 1
    datasets: Mapping[str, Sequence[Mapping[str, Any]]] = field(default_factory=dict)
    dataset_statuses: Mapping[str, str] = field(default_factory=dict)
    schema_fingerprint: Optional[str] = None

    @property
    def batch_id(self) -> str:
        return deterministic_preview_batch_id(
            self.game_id,
            self.payload_sha256,
            self.parser_version,
            league=self.league,
            season=self.season,
        )


@dataclass(frozen=True)
class PreviewFailure:
    game_id: int
    league: str
    season: str
    game: str
    state: str
    failure_code: str
    error: str
    retry_after: Optional[datetime]
    attempt_no: int
    kickoff: Optional[datetime] = None
    payload_sha256: Optional[str] = None
    raw_uri: Optional[str] = None
    transport_mode: str = "none"
    proxy_mode: str = "none"
    http_status: Optional[int] = None
    direct_bytes: int = 0
    paid_bytes: int = 0
    parser_version: str = PARSER_VERSION
    availability_version: str = MATCH_AVAILABILITY_VERSION


@dataclass(frozen=True)
class ProfileCommit:
    player_id: int
    profile: Mapping[str, Any]
    payload_sha256: str
    raw_uri: str
    transport_mode: str
    proxy_mode: str = "none"
    direct_bytes: int = 0
    paid_bytes: int = 0
    fetched_at: Optional[datetime] = None
    parser_version: str = PARSER_VERSION
    availability_version: str = MATCH_AVAILABILITY_VERSION
    participations: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    participations_status: str = "empty"

    @property
    def batch_id(self) -> str:
        identity = json.dumps(
            {
                "kind": "profile",
                "parser_version": str(self.parser_version),
                "payload_sha256": str(self.payload_sha256),
                "player_id": int(self.player_id),
                "version": 3,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return PROFILE_BATCH_ID_PREFIX + hashlib.sha256(identity).hexdigest()


@dataclass(frozen=True)
class ProfileCandidateSnapshot:
    """One complete, deterministic daily profile candidate generation."""

    player_ids: tuple[int, ...]
    count: int
    payload_sha256: str


class ProfileCandidateCapacityExceeded(RuntimeError):
    """The exact due backlog cannot be completed inside one bounded task."""

    def __init__(self, *, count: int, hard_cap: int) -> None:
        self.count = int(count)
        self.hard_cap = int(hard_cap)
        super().__init__(
            "WhoScored due profile backlog exceeds the daily hard cap: "
            f"count={self.count}, hard_cap={self.hard_cap}"
        )


def entity_id_payload_sha256(entity_ids: Iterable[int]) -> str:
    """Hash one complete positive-ID set independently of source order."""

    raw_values = list(entity_ids)
    if any(type(value) is not int or value <= 0 for value in raw_values):
        raise ValueError("entity ids must be positive and unique")
    values = sorted(raw_values)
    if len(values) != len(set(values)):
        raise ValueError("entity ids must be positive and unique")
    encoded = json.dumps(values, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def profile_candidate_payload_sha256(player_ids: Iterable[int]) -> str:
    """Hash a complete candidate set independently of query priority order."""

    return entity_id_payload_sha256(player_ids)


class WhoScoredRepository:
    """Repository for schedule candidates and per-game logical commits."""

    def __init__(
        self,
        *,
        writer: Optional[IcebergWriter] = None,
        trino: Optional[TrinoTableManager] = None,
        catalog: str = "iceberg",
        schema: str = "bronze",
    ) -> None:
        require_production_runtime_class(operation="WhoScored Iceberg persistence")
        self.writer = writer or IcebergWriter(catalog=catalog)
        self.trino = trino or self.writer._get_trino_manager()
        # An injected Trino manager must also back writes.  Otherwise the
        # writer lazily opens a second connection using environment defaults.
        if writer is None and trino is not None:
            self.writer._trino_manager = trino
        self.catalog = catalog
        self.schema = schema

    @property
    def _manifest(self) -> str:
        return f"{self.catalog}.{self.schema}.{MATCH_MANIFEST_TABLE}"

    @property
    def _preview_manifest(self) -> str:
        return f"{self.catalog}.{self.schema}.{PREVIEW_MANIFEST_TABLE}"

    @property
    def _catalog_manifest(self) -> str:
        return f"{self.catalog}.{self.schema}.{CATALOG_MANIFEST_TABLE}"

    @property
    def _scope_manifest(self) -> str:
        return f"{self.catalog}.{self.schema}.{SCOPE_MANIFEST_TABLE}"

    @staticmethod
    def _normalise_json_value(value: Any) -> Any:
        if isinstance(value, (datetime,)):
            return value.isoformat()
        if hasattr(value, "isoformat") and not isinstance(value, (str, bytes)):
            try:
                return value.isoformat()
            except (TypeError, ValueError):
                pass
        return str(value)

    @classmethod
    def _canonical_json(cls, value: Any) -> str:
        return json.dumps(
            _clean_json_unicode(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=cls._normalise_json_value,
        )

    @contextmanager
    def _commit_locks(self, identities: Iterable[str]):
        """Hold sorted filesystem claims for the complete commit protocol.

        Airflow's local task processes share the mounted log directory.  The
        lock covers physical preflight, payload append and manifest publish,
        closing the same-batch race without introducing a paid/network call or
        relying on an unavailable uniqueness constraint in Iceberg.
        """

        keys = sorted({str(value) for value in identities if str(value)})
        if not keys:
            yield
            return
        root = Path(
            os.environ.get(
                "WHOSCORED_LOCK_DIR",
                str(Path(tempfile.gettempdir()) / "whoscored_commit_locks"),
            )
        )
        root.mkdir(parents=True, exist_ok=True, mode=0o750)
        handles = []
        try:
            for identity in keys:
                digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
                handle = (root / f"{digest}.lock").open("a+", encoding="utf-8")
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                handles.append(handle)
            yield
        finally:
            for handle in reversed(handles):
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()

    @staticmethod
    def _normalise_frame_types(
        frame: pd.DataFrame, *, table: Optional[str] = None
    ) -> pd.DataFrame:
        """Coerce a business frame to its declared physical column contract.

        A missing/all-null first batch must have exactly the same Arrow types as
        a populated later batch.  Unknown parser columns fail closed so a source
        drift cannot silently become an Iceberg schema mutation.
        """
        result = frame.copy()
        if table is None:
            raise ValueError("WhoScored business frame requires a table contract")
        try:
            contract = WHOSCORED_BUSINESS_COLUMN_CONTRACTS[table]
        except KeyError as exc:
            raise ValueError(f"unknown WhoScored business table {table!r}") from exc
        unknown = sorted(set(result.columns) - set(contract))
        if unknown:
            raise ValueError(
                f"{table} contains columns outside its physical contract: "
                + ", ".join(unknown)
            )

        for column in result.columns:
            data_type = contract[column]
            if data_type.startswith("TIMESTAMP"):
                converted = pd.to_datetime(result[column], errors="raise", utc=True)
                result[column] = converted.dt.tz_localize(None).astype("datetime64[us]")
            elif data_type == "DATE":
                converted = pd.to_datetime(result[column], errors="raise")
                dates = [
                    None if pd.isna(value) else pd.Timestamp(value).date()
                    for value in converted
                ]
                result[column] = pd.Series(
                    pd.array(dates, dtype=pd.ArrowDtype(pa.date32())),
                    index=result.index,
                )
            elif data_type == "BOOLEAN":
                result[column] = (
                    result[column]
                    .map(lambda value: pd.NA if pd.isna(value) else _coerce_bool(value))
                    .astype("boolean")
                )
            elif data_type == "BIGINT":
                result[column] = pd.to_numeric(result[column], errors="raise").astype(
                    "Int64"
                )
            elif data_type == "INTEGER":
                result[column] = pd.to_numeric(result[column], errors="raise").astype(
                    "Int32"
                )
            elif data_type == "DOUBLE":
                result[column] = pd.to_numeric(result[column], errors="raise").astype(
                    "Float64"
                )
            elif data_type == "VARCHAR":
                cleaned = result[column].map(
                    lambda value: (
                        value if pd.isna(value) else _clean_unicode(str(value))
                    )
                )
                result[column] = pd.Series(
                    pd.array(cleaned, dtype=pd.ArrowDtype(pa.string())),
                    index=result.index,
                )
            else:  # Every supported contract type must have an explicit coercion.
                raise ValueError(
                    f"unsupported WhoScored contract type {data_type!r} "
                    f"for {table}.{column}"
                )
        return result

    def _ensure_business_table_contract(self, table: str) -> None:
        """Create/add the exact declared columns without touching legacy extras."""
        try:
            columns = WHOSCORED_BUSINESS_COLUMN_CONTRACTS[table]
        except KeyError as exc:
            raise ValueError(f"unknown WhoScored business table {table!r}") from exc
        column_sql = ",\n                    ".join(
            f'"{name}" {data_type}' for name, data_type in columns.items()
        )
        if table in {PROFILE_VERSIONS_TABLE, "whoscored_player_stage_participations"}:
            partitioning = " WITH (partitioning = ARRAY['bucket(player_id, 32)'])"
        elif table not in {
            "whoscored_competitions",
            "whoscored_seasons",
            "whoscored_stages",
        }:
            partitioning = " WITH (partitioning = ARRAY['league', 'season'])"
        else:
            partitioning = ""
        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.{table} (
                    {column_sql}
            ){partitioning}
            """
        )
        existing_columns = self.trino.get_table_columns(self.schema, table)
        if isinstance(existing_columns, Mapping):
            existing = {str(name).lower() for name in existing_columns}
        else:
            existing = {str(name).lower() for name in existing_columns}
        # CREATE IF NOT EXISTS plus additive ALTER is race-safe and preserves
        # every pre-V3 column until the separate data migration removes it.
        for name, data_type in columns.items():
            if name.lower() not in existing:
                self.trino.add_column(self.schema, table, name, data_type)

    def _ensure_catalog_schema(self, *, create_views: bool) -> None:
        for table in (
            "whoscored_competitions",
            "whoscored_seasons",
            "whoscored_stages",
        ):
            self._ensure_business_table_contract(table)
        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._catalog_manifest} (
                batch_id VARCHAR,
                payload_sha256 VARCHAR,
                raw_uri VARCHAR,
                raw_inputs_json VARCHAR,
                raw_provenance_sha256 VARCHAR,
                discovery_mode VARCHAR,
                as_of_date DATE,
                parent_catalog_batch_id VARCHAR,
                parent_catalog_payload_sha256 VARCHAR,
                parent_catalog_raw_provenance_sha256 VARCHAR,
                parser_version VARCHAR,
                state VARCHAR,
                competitions_count BIGINT,
                seasons_count BIGINT,
                stages_count BIGINT,
                quarantined_count BIGINT,
                schema_fingerprint VARCHAR,
                started_at TIMESTAMP(6),
                completed_at TIMESTAMP(6),
                error VARCHAR,
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            )
            """
        )
        manifest_columns = {
            name.lower()
            for name in self.trino.get_table_columns(
                self.schema, CATALOG_MANIFEST_TABLE
            )
        }
        additive_columns = {
            "raw_inputs_json": "VARCHAR",
            "raw_provenance_sha256": "VARCHAR",
            "discovery_mode": "VARCHAR",
            "as_of_date": "DATE",
            "parent_catalog_batch_id": "VARCHAR",
            "parent_catalog_payload_sha256": "VARCHAR",
            "parent_catalog_raw_provenance_sha256": "VARCHAR",
        }
        for name, data_type in additive_columns.items():
            if name not in manifest_columns:
                self.trino.add_column(
                    self.schema, CATALOG_MANIFEST_TABLE, name, data_type
                )
        if not create_views:
            return
        latest = f"{self.catalog}.{self.schema}.whoscored_catalog_latest_success"
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW {latest} AS
            SELECT * FROM (
                SELECT m.*, ROW_NUMBER() OVER (
                    ORDER BY completed_at DESC, _ingested_at DESC, batch_id DESC
                ) AS _manifest_rank
                FROM {self._catalog_manifest} m
                WHERE state = 'success'
            ) WHERE _manifest_rank = 1
            """
        )
        for table in (
            "whoscored_competitions",
            "whoscored_seasons",
            "whoscored_stages",
        ):
            self.trino._execute(
                f"""
                CREATE OR REPLACE VIEW {self.catalog}.{self.schema}.{table}_current AS
                SELECT d.*
                FROM {self.catalog}.{self.schema}.{table} d
                JOIN {latest} m ON m.batch_id = d._catalog_batch_id
                """
            )

    def _ensure_scope_schema(self, *, create_views: bool) -> None:
        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._scope_manifest} (
                league VARCHAR,
                season VARCHAR,
                entity_group VARCHAR,
                batch_id VARCHAR,
                payload_sha256 VARCHAR,
                raw_uris_json VARCHAR,
                parser_version VARCHAR,
                state VARCHAR,
                entity_counts_json VARCHAR,
                dataset_states_json VARCHAR,
                schema_fingerprint VARCHAR,
                started_at TIMESTAMP(6),
                completed_at TIMESTAMP(6),
                error VARCHAR,
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            ) WITH (partitioning = ARRAY['league', 'season'])
            """
        )
        scope_manifest_columns = {
            name.lower()
            for name in self.trino.get_table_columns(self.schema, SCOPE_MANIFEST_TABLE)
        }
        if "dataset_states_json" not in scope_manifest_columns:
            self.trino.add_column(
                self.schema, SCOPE_MANIFEST_TABLE, "dataset_states_json", "VARCHAR"
            )
        for table in sorted(SCOPE_DATASET_TABLES):
            self._ensure_business_table_contract(table)
        if not create_views:
            return
        latest = f"{self.catalog}.{self.schema}.whoscored_scope_ingest_latest_success"
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW {latest} AS
            SELECT * FROM (
                SELECT m.*, ROW_NUMBER() OVER (
                    PARTITION BY league, season, entity_group
                    ORDER BY completed_at DESC, _ingested_at DESC, batch_id DESC
                ) AS _manifest_rank
                FROM {self._scope_manifest} m
                WHERE state = 'success'
            ) WHERE _manifest_rank = 1
            """
        )
        # Scope-owned datasets use this manifest. Match and preview datasets
        # are exposed by their stricter per-game manifests below.
        for table in sorted(SCOPE_DATASET_TABLES):
            if not self.trino.table_exists(self.schema, table):
                continue
            self.trino._execute(
                f"""
                CREATE OR REPLACE VIEW {self.catalog}.{self.schema}.{table}_current AS
                SELECT d.*
                FROM {self.catalog}.{self.schema}.{table} d
                JOIN {latest} m
                  ON m.league = d.league
                 AND m.season = d.season
                 AND m.batch_id = d._scope_batch_id
                UNION ALL
                SELECT d.*
                FROM {self.catalog}.{self.schema}.{table} d
                WHERE d._scope_batch_id IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM {latest} m
                      WHERE m.league = d.league
                        AND m.season = d.season
                  )
                """
            )

    def ensure_schema(self, *, create_views: bool = True) -> None:
        """Create additive V2 storage and, unless deferred, strict views."""
        self._ensure_catalog_schema(create_views=create_views)
        self._ensure_scope_schema(create_views=create_views)
        for table in MATCH_DATASET_TABLES.values():
            self._ensure_business_table_contract(table)
        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._manifest} (
                league VARCHAR,
                season VARCHAR,
                game_id BIGINT,
                game VARCHAR,
                kickoff TIMESTAMP(6),
                batch_id VARCHAR,
                payload_sha256 VARCHAR,
                raw_uri VARCHAR,
                parser_version VARCHAR,
                availability_version VARCHAR,
                state VARCHAR,
                is_final BOOLEAN,
                is_opta BOOLEAN,
                events_count BIGINT,
                lineups_count BIGINT,
                lineups_available BOOLEAN,
                transport_mode VARCHAR,
                proxy_mode VARCHAR,
                http_status INTEGER,
                failure_code VARCHAR,
                error VARCHAR,
                attempt_no INTEGER,
                retry_after TIMESTAMP(6),
                fetched_at TIMESTAMP(6),
                completed_at TIMESTAMP(6),
                direct_bytes BIGINT,
                paid_bytes BIGINT,
                entity_counts_json VARCHAR,
                dataset_statuses_json VARCHAR,
                schema_fingerprint VARCHAR,
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            ) WITH (partitioning = ARRAY['league', 'season'])
            """
        )
        manifest_columns = {
            name.lower()
            for name in self.trino.get_table_columns(self.schema, MATCH_MANIFEST_TABLE)
        }
        for name, data_type in {
            "availability_version": "VARCHAR",
            "entity_counts_json": "VARCHAR",
            "dataset_statuses_json": "VARCHAR",
            "schema_fingerprint": "VARCHAR",
        }.items():
            if name not in manifest_columns:
                self.trino.add_column(
                    self.schema, MATCH_MANIFEST_TABLE, name, data_type
                )
        self._ensure_preview_schema()
        if create_views:
            self._create_current_views()
        self._ensure_profile_schema(create_views=create_views)

    @staticmethod
    def _catalog_record_key(kind: str, row: Mapping[str, Any]) -> str:
        if kind == "competitions":
            parts = (
                row.get("region_id"),
                row.get("tournament_id"),
                row.get("competition_id"),
            )
        elif kind == "seasons":
            parts = (
                row.get("competition_id"),
                row.get("source_season_id"),
                row.get("season_id"),
            )
        elif kind == "stages":
            parts = (
                row.get("competition_id"),
                row.get("source_season_id"),
                row.get("stage_id"),
            )
        else:
            raise ValueError(f"unsupported catalog dataset: {kind!r}")
        return "|".join("" if part is None else str(part) for part in parts)

    def _read_catalog_manifest(self, batch_id: str) -> dict[str, Any]:
        rows = self.trino.execute_query(
            f"SELECT competitions_count, seasons_count, stages_count, "
            "payload_sha256, parser_version, schema_fingerprint, raw_inputs_json, "
            "raw_provenance_sha256, discovery_mode, as_of_date, "
            "parent_catalog_batch_id, parent_catalog_payload_sha256, "
            "parent_catalog_raw_provenance_sha256, COUNT(*) OVER () "
            "AS _identity_count "
            f"FROM {self._catalog_manifest} "
            f"WHERE batch_id = {_sql_string(batch_id)} AND state = 'success' "
            "ORDER BY completed_at DESC, _ingested_at DESC LIMIT 1"
        )
        if not rows:
            raise BatchConflict(f"catalog {batch_id} success manifest is missing")
        row = rows[0]
        if len(row) != 14 or int(row[13]) != 1:
            raise BatchConflict(f"catalog {batch_id} manifest shape is invalid")
        return {
            "batch_id": batch_id,
            "competitions_count": row[0],
            "seasons_count": row[1],
            "stages_count": row[2],
            "payload_sha256": str(row[3] or ""),
            "parser_version": str(row[4] or ""),
            "schema_fingerprint": str(row[5] or ""),
            "raw_inputs_json": str(row[6] or ""),
            "raw_provenance_sha256": str(row[7] or ""),
            "discovery_mode": str(row[8] or ""),
            "as_of_date": row[9],
            "parent_catalog_batch_id": str(row[10] or ""),
            "parent_catalog_payload_sha256": str(row[11] or ""),
            "parent_catalog_raw_provenance_sha256": str(row[12] or ""),
        }

    def _validate_catalog_lineage(
        self,
        manifest: Mapping[str, Any],
        *,
        allow_legacy_parser_for_full_history: bool = False,
    ) -> None:
        """Validate every exact parent edge and reject cycles fail-closed."""

        seen: set[str] = set()
        current = dict(manifest)
        first = True
        lineage_manifests: Optional[dict[str, dict[str, Any]]] = None
        while True:
            batch_id = str(current["batch_id"])
            if batch_id in seen:
                raise BatchConflict(
                    f"catalog lineage cycle detected at batch {batch_id}"
                )
            seen.add(batch_id)
            parser_version = str(current.get("parser_version") or "")
            if parser_version != PARSER_VERSION:
                if (
                    first
                    and allow_legacy_parser_for_full_history
                    and parser_version in _LEGACY_CATALOG_MIGRATION_PARSERS
                ):
                    return
                raise BatchConflict(
                    f"catalog {batch_id} lineage uses unsupported parser "
                    f"{parser_version!r}"
                )
            if (
                first
                and allow_legacy_parser_for_full_history
                and not current.get("raw_provenance_sha256")
                and current.get("as_of_date") is None
                and not current.get("parent_catalog_batch_id")
                and not current.get("parent_catalog_payload_sha256")
                and not current.get("parent_catalog_raw_provenance_sha256")
            ):
                # A reviewed full-history run may use one physically verified
                # pre-lineage snapshot only for loss detection. It is never a
                # valid incremental parent and the new snapshot has no parent.
                return
            current_as_of = _catalog_manifest_as_of_date(current.get("as_of_date"))
            _validate_catalog_raw_provenance(
                batch_id=batch_id,
                raw_inputs_json=str(current.get("raw_inputs_json") or ""),
                raw_provenance_sha256=str(current.get("raw_provenance_sha256") or ""),
                as_of_date=current_as_of,
            )
            discovery_mode = str(current.get("discovery_mode") or "")
            parent_batch_id = str(current.get("parent_catalog_batch_id") or "")
            parent_payload_sha256 = str(
                current.get("parent_catalog_payload_sha256") or ""
            )
            parent_raw_provenance_sha256 = str(
                current.get("parent_catalog_raw_provenance_sha256") or ""
            )
            parent_identity = (
                parent_batch_id,
                parent_payload_sha256,
                parent_raw_provenance_sha256,
            )
            if discovery_mode == "full_history":
                if any(parent_identity):
                    raise BatchConflict(
                        f"full-history catalog {batch_id} declares a parent"
                    )
                return
            if discovery_mode != "incremental":
                raise BatchConflict(
                    f"catalog {batch_id} has invalid discovery mode {discovery_mode!r}"
                )
            if (
                not all(parent_identity)
                or re.fullmatch(r"[0-9a-f]{64}", parent_payload_sha256) is None
                or re.fullmatch(r"[0-9a-f]{64}", parent_raw_provenance_sha256) is None
            ):
                raise BatchConflict(
                    f"incremental catalog {batch_id} has incomplete parent lineage"
                )
            if parent_batch_id in seen:
                raise BatchConflict(
                    f"catalog lineage cycle detected at batch {parent_batch_id}"
                )
            if lineage_manifests is None:
                rows = self.trino.execute_query(
                    f"SELECT batch_id, payload_sha256, parser_version, "
                    "raw_inputs_json, raw_provenance_sha256, discovery_mode, "
                    "as_of_date, parent_catalog_batch_id, "
                    "parent_catalog_payload_sha256, "
                    "parent_catalog_raw_provenance_sha256, "
                    "COUNT(*) OVER (PARTITION BY batch_id) AS _identity_count "
                    f"FROM {self._catalog_manifest} WHERE state = 'success'"
                )
                lineage_manifests = {}
                for row in rows:
                    if len(row) != 11:
                        raise BatchConflict("catalog lineage manifest shape is invalid")
                    candidate_batch_id = str(row[0] or "")
                    if (
                        not candidate_batch_id
                        or candidate_batch_id in lineage_manifests
                    ):
                        continue
                    lineage_manifests[candidate_batch_id] = {
                        "batch_id": candidate_batch_id,
                        "payload_sha256": str(row[1] or ""),
                        "parser_version": str(row[2] or ""),
                        "raw_inputs_json": str(row[3] or ""),
                        "raw_provenance_sha256": str(row[4] or ""),
                        "discovery_mode": str(row[5] or ""),
                        "as_of_date": row[6],
                        "parent_catalog_batch_id": str(row[7] or ""),
                        "parent_catalog_payload_sha256": str(row[8] or ""),
                        "parent_catalog_raw_provenance_sha256": str(row[9] or ""),
                        "identity_count": int(row[10]),
                    }
            parent = lineage_manifests.get(parent_batch_id)
            if parent is None:
                raise BatchConflict(
                    f"catalog {batch_id} parent {parent_batch_id} is missing"
                )
            if int(parent["identity_count"]) != 1:
                raise BatchConflict(
                    f"catalog {batch_id} parent {parent_batch_id} is ambiguous"
                )
            if parent_payload_sha256 != parent["payload_sha256"]:
                raise BatchConflict(
                    f"catalog {batch_id} parent payload hash does not match "
                    f"{parent_batch_id}"
                )
            if parent_raw_provenance_sha256 != parent["raw_provenance_sha256"]:
                raise BatchConflict(
                    f"catalog {batch_id} parent raw provenance hash does not "
                    f"match {parent_batch_id}"
                )
            parent_as_of = _catalog_manifest_as_of_date(parent.get("as_of_date"))
            if parent_as_of > current_as_of:
                raise BatchConflict(
                    f"catalog {batch_id} as_of_date precedes parent {parent_batch_id}"
                )
            current = parent
            first = False

    @_lock_catalog_commit
    def persist_discovered_catalog(
        self,
        catalog: WhoScoredCatalog,
        *,
        discovery_batch_id: str,
        raw_uri: str,
        payload_sha256: str,
        raw_inputs: Sequence[Mapping[str, Any]],
        raw_provenance_sha256: str,
        discovery_mode: str,
        as_of_date: date,
        parent_catalog_batch_id: Optional[str],
        parent_catalog_payload_sha256: Optional[str],
        parent_catalog_raw_provenance_sha256: Optional[str],
    ) -> str:
        """Append a complete discovery snapshot, then publish one manifest.

        All rows, including exclusions and quarantined records, are persisted.
        A retry with the same deterministic batch verifies physical counts and
        performs no duplicate writes.
        """
        if (
            not discovery_batch_id
            or not payload_sha256
            or not raw_uri
            or re.fullmatch(r"[0-9a-f]{64}", raw_provenance_sha256) is None
            or discovery_mode not in {"incremental", "full_history"}
            or type(as_of_date) is not date
        ):
            raise ValueError(
                "catalog batch id, payload/raw provenance sha256, raw_uri, "
                "discovery mode and exact as_of_date are required"
            )
        parent_identity = (
            parent_catalog_batch_id,
            parent_catalog_payload_sha256,
            parent_catalog_raw_provenance_sha256,
        )
        if discovery_mode == "full_history" and any(parent_identity):
            raise ValueError("full-history catalog cannot declare a parent")
        if discovery_mode == "incremental" and (
            not all(parent_identity)
            or re.fullmatch(r"[0-9a-f]{64}", str(parent_catalog_payload_sha256)) is None
            or re.fullmatch(r"[0-9a-f]{64}", str(parent_catalog_raw_provenance_sha256))
            is None
        ):
            raise ValueError("incremental catalog requires an exact parent identity")
        datasets = canonical_catalog_rows(catalog.to_rows())
        fingerprint = catalog_payload_sha256(datasets)
        if payload_sha256 != fingerprint:
            raise ValueError(
                "catalog payload_sha256 must identify the complete canonical output"
            )
        raw_inputs_json = self._canonical_json(canonical_catalog_raw_inputs(raw_inputs))
        _validate_catalog_raw_provenance(
            batch_id=discovery_batch_id,
            raw_inputs_json=raw_inputs_json,
            raw_provenance_sha256=raw_provenance_sha256,
            as_of_date=as_of_date,
        )
        if discovery_mode == "incremental":
            parent_manifest = self._read_catalog_manifest(str(parent_catalog_batch_id))
            self._validate_catalog_lineage(parent_manifest)
            if (
                parent_manifest["payload_sha256"] != parent_catalog_payload_sha256
                or parent_manifest["raw_provenance_sha256"]
                != parent_catalog_raw_provenance_sha256
            ):
                raise BatchConflict(
                    f"catalog {discovery_batch_id} exact parent identity differs "
                    f"from {parent_catalog_batch_id}"
                )
            parent_as_of = _catalog_manifest_as_of_date(parent_manifest["as_of_date"])
            if parent_as_of > as_of_date:
                raise BatchConflict(
                    f"catalog {discovery_batch_id} as_of_date precedes parent "
                    f"{parent_catalog_batch_id}"
                )
        expected = {
            kind: len(tuple(datasets.get(kind, ())))
            for kind in ("competitions", "seasons", "stages")
        }
        existing_manifest = self.trino.execute_query(
            f"SELECT competitions_count, seasons_count, stages_count, "
            "payload_sha256, parser_version, schema_fingerprint, raw_inputs_json, "
            "raw_provenance_sha256, discovery_mode, as_of_date, "
            "parent_catalog_batch_id, parent_catalog_payload_sha256, "
            "parent_catalog_raw_provenance_sha256 "
            f"FROM {self._catalog_manifest} "
            f"WHERE batch_id = {_sql_string(discovery_batch_id)} "
            "AND state = 'success' ORDER BY completed_at DESC LIMIT 1"
        )
        if existing_manifest:
            stored = tuple(int(value) for value in existing_manifest[0][:3])
            wanted = tuple(
                expected[kind] for kind in ("competitions", "seasons", "stages")
            )
            if stored != wanted:
                raise BatchConflict(
                    f"catalog manifest {discovery_batch_id} counts {stored}, expected {wanted}"
                )
            identity = tuple(str(value or "") for value in existing_manifest[0][3:])
            wanted_identity = (
                payload_sha256,
                PARSER_VERSION,
                fingerprint,
                raw_inputs_json,
                raw_provenance_sha256,
                discovery_mode,
                as_of_date.isoformat(),
                str(parent_catalog_batch_id or ""),
                str(parent_catalog_payload_sha256 or ""),
                str(parent_catalog_raw_provenance_sha256 or ""),
            )
            if identity != wanted_identity:
                raise BatchConflict(
                    f"catalog manifest {discovery_batch_id} identity differs from parser output"
                )
            for kind, count in expected.items():
                rows = self.trino.execute_query(
                    f"SELECT COUNT(*) FROM {self.catalog}.{self.schema}.whoscored_{kind} "
                    f"WHERE _catalog_batch_id = {_sql_string(discovery_batch_id)}"
                )
                physical = int(rows[0][0]) if rows else 0
                if physical != count:
                    raise BatchConflict(
                        f"catalog {kind} batch {discovery_batch_id}: "
                        f"physical={physical}, manifest={count}"
                    )
            return discovery_batch_id

        for kind in ("competitions", "seasons", "stages"):
            source_rows = [dict(row) for row in datasets.get(kind, ())]
            keys: dict[str, str] = {}
            materialised: list[dict[str, Any]] = []
            for source in source_rows:
                key = self._catalog_record_key(kind, source)
                payload = self._canonical_json(source)
                previous = keys.get(key)
                if previous is not None and previous != payload:
                    raise ValueError(
                        f"catalog {kind} contains conflicting duplicate key {key!r}"
                    )
                if previous is not None:
                    continue
                keys[key] = payload
                row = dict(source)
                row.update(
                    {
                        "record_key": key,
                        "payload_json": payload,
                        "_catalog_batch_id": discovery_batch_id,
                        "_entity_type": kind[:-1],
                    }
                )
                materialised.append(row)
            if len(materialised) != expected[kind]:
                raise ValueError(
                    f"catalog {kind} has duplicate records: "
                    f"raw={expected[kind]}, unique={len(materialised)}"
                )
            existing_rows = self.trino.execute_query(
                f"SELECT COUNT(*) FROM "
                f"{self.catalog}.{self.schema}.whoscored_{kind} "
                f"WHERE _catalog_batch_id = {_sql_string(discovery_batch_id)}"
            )
            existing_count = int(existing_rows[0][0]) if existing_rows else 0
            if existing_count == expected[kind]:
                continue
            if existing_count:
                raise BatchConflict(
                    f"catalog {kind} batch {discovery_batch_id} has an "
                    f"incomplete orphan batch: physical={existing_count}, "
                    f"expected={expected[kind]}"
                )
            if not materialised:
                continue
            self.writer.write_dataframe(
                self._normalise_frame_types(
                    pd.DataFrame(materialised), table=f"whoscored_{kind}"
                ),
                database=self.schema,
                table=f"whoscored_{kind}",
                source="whoscored",
            )
            rows = self.trino.execute_query(
                f"SELECT COUNT(*) FROM {self.catalog}.{self.schema}.whoscored_{kind} "
                f"WHERE _catalog_batch_id = {_sql_string(discovery_batch_id)}"
            )
            physical = int(rows[0][0]) if rows else 0
            if physical != expected[kind]:
                raise BatchConflict(
                    f"catalog {kind} batch {discovery_batch_id}: "
                    f"wrote={physical}, expected={expected[kind]}"
                )

        now = _utc_now()
        self.writer.write_dataframe(
            pd.DataFrame(
                [
                    {
                        "batch_id": discovery_batch_id,
                        "payload_sha256": payload_sha256,
                        "raw_uri": raw_uri,
                        "raw_inputs_json": raw_inputs_json,
                        "raw_provenance_sha256": raw_provenance_sha256,
                        "discovery_mode": discovery_mode,
                        "as_of_date": as_of_date,
                        "parent_catalog_batch_id": parent_catalog_batch_id,
                        "parent_catalog_payload_sha256": (
                            parent_catalog_payload_sha256
                        ),
                        "parent_catalog_raw_provenance_sha256": (
                            parent_catalog_raw_provenance_sha256
                        ),
                        "parser_version": PARSER_VERSION,
                        "state": "success",
                        "competitions_count": expected["competitions"],
                        "seasons_count": expected["seasons"],
                        "stages_count": expected["stages"],
                        "quarantined_count": len(catalog.quarantined),
                        "schema_fingerprint": fingerprint,
                        "started_at": now,
                        "completed_at": now,
                        "error": None,
                        "_entity_type": "catalog_manifest",
                    }
                ]
            ),
            database=self.schema,
            table=CATALOG_MANIFEST_TABLE,
            source="whoscored",
        )
        return discovery_batch_id

    def load_discovered_catalog(
        self,
        *,
        batch_id: Optional[str] = None,
        allow_legacy_parser_for_full_history: bool = False,
    ) -> WhoScoredCatalog:
        """Load one exact logically committed discovery snapshot.

        ``allow_legacy_parser_for_full_history`` exists only for the explicit
        v7 -> v8 full-history bootstrap. Legacy manifests still have to prove
        physical counts, valid JSON/catalog structure and their original
        internally consistent SHA-256 identity. They are never accepted by a
        normal current-catalog read.
        """
        if not self.trino.table_exists(self.schema, CATALOG_MANIFEST_TABLE):
            raise LookupError("WhoScored discovered catalog is not initialized")
        batch_filter = (
            "" if batch_id is None else f"AND batch_id = {_sql_string(batch_id)} "
        )
        manifests = self.trino.execute_query(
            f"SELECT batch_id FROM {self._catalog_manifest} "
            f"WHERE state = 'success' {batch_filter}"
            "ORDER BY completed_at DESC, _ingested_at DESC, batch_id DESC LIMIT 1"
        )
        if not manifests:
            raise LookupError("WhoScored discovered catalog has no successful snapshot")
        batch_id = str(manifests[0][0])
        manifest = self._read_catalog_manifest(batch_id)
        datasets: dict[str, list[dict[str, Any]]] = {}
        for kind in ("competitions", "seasons", "stages"):
            rows = self.trino.execute_query(
                f"SELECT payload_json FROM "
                f"{self.catalog}.{self.schema}.whoscored_{kind} "
                f"WHERE _catalog_batch_id = {_sql_string(batch_id)} ORDER BY record_key"
            )
            decoded: list[dict[str, Any]] = []
            for (payload,) in rows:
                value = json.loads(str(payload))
                if not isinstance(value, dict):
                    raise BatchConflict(
                        f"catalog {kind}/{batch_id} contains a non-object payload"
                    )
                decoded.append(value)
            datasets[kind] = decoded
        datasets = canonical_catalog_rows(datasets)
        catalog = WhoScoredCatalog.from_rows(datasets)
        actual = tuple(
            len(datasets[kind]) for kind in ("competitions", "seasons", "stages")
        )
        manifest_counts = tuple(
            int(manifest[f"{kind}_count"])
            for kind in ("competitions", "seasons", "stages")
        )
        if manifest_counts != actual:
            raise BatchConflict(
                f"catalog {batch_id} manifest/physical mismatch: "
                f"manifest={manifest_counts}, physical={actual}"
            )
        physical_fingerprint = catalog_payload_sha256(catalog.to_rows())
        manifest_payload = str(manifest["payload_sha256"] or "")
        manifest_parser = str(manifest["parser_version"] or "")
        manifest_schema = str(manifest["schema_fingerprint"] or "")
        current_identity = (
            manifest_parser == PARSER_VERSION
            and manifest_payload == physical_fingerprint
            and manifest_schema == physical_fingerprint
        )
        legacy_identity = (
            bool(allow_legacy_parser_for_full_history)
            and manifest_parser in _LEGACY_CATALOG_MIGRATION_PARSERS
            and manifest_payload == manifest_schema
            and re.fullmatch(r"[0-9a-f]{64}", manifest_payload) is not None
        )
        if not current_identity and not legacy_identity:
            raise BatchConflict(
                f"catalog {batch_id} manifest identity mismatch: "
                f"payload={manifest_payload}, schema={manifest_schema}, "
                f"parser={manifest_parser}, physical={physical_fingerprint}, "
                f"expected_parser={PARSER_VERSION}"
            )
        self._validate_catalog_lineage(
            manifest,
            allow_legacy_parser_for_full_history=(allow_legacy_parser_for_full_history),
        )
        return catalog

    def load_catalog_generation_snapshot(
        self,
        *,
        batch_id: Optional[str] = None,
        allow_legacy_parser_for_full_history: bool = False,
    ) -> tuple[dict[str, Any], WhoScoredCatalog]:
        """Bind one exact manifest generation to rows from that same batch."""
        generation = self.catalog_generation(
            batch_id=batch_id,
            allow_legacy_parser_for_full_history=(allow_legacy_parser_for_full_history),
        )
        catalog = self.load_discovered_catalog(
            batch_id=str(generation["catalog_batch_id"]),
            allow_legacy_parser_for_full_history=(allow_legacy_parser_for_full_history),
        )
        return generation, catalog

    def catalog_generation(
        self,
        *,
        batch_id: Optional[str] = None,
        allow_legacy_parser_for_full_history: bool = False,
    ) -> dict[str, Any]:
        """Return verifiable provenance for one exact or latest snapshot."""
        if not self.trino.table_exists(self.schema, CATALOG_MANIFEST_TABLE):
            raise LookupError("WhoScored discovered catalog is not initialized")
        batch_filter = (
            "" if batch_id is None else f"AND batch_id = {_sql_string(batch_id)} "
        )
        rows = self.trino.execute_query(
            f"SELECT batch_id, payload_sha256, parser_version, raw_uri, "
            "raw_inputs_json, completed_at, discovery_mode, "
            "raw_provenance_sha256, as_of_date, parent_catalog_batch_id, "
            "parent_catalog_payload_sha256, "
            "parent_catalog_raw_provenance_sha256, "
            "COUNT(*) OVER (PARTITION BY batch_id) AS _identity_count "
            f"FROM {self._catalog_manifest} WHERE state = 'success' "
            f"{batch_filter}"
            "ORDER BY completed_at DESC, _ingested_at DESC, batch_id DESC LIMIT 1"
        )
        if not rows:
            raise LookupError("WhoScored discovered catalog has no successful snapshot")
        row = rows[0]
        if len(row) != 13 or int(row[12]) != 1:
            raise BatchConflict("catalog generation manifest shape is invalid")
        raw_inputs_json = str(row[4] or "")
        parser_version = str(row[2] or "")
        pre_lineage_full_history_bootstrap = (
            allow_legacy_parser_for_full_history
            and parser_version == PARSER_VERSION
            and not row[7]
            and row[8] is None
            and not any(row[index] for index in (9, 10, 11))
        )
        if (
            allow_legacy_parser_for_full_history
            and parser_version in _LEGACY_CATALOG_MIGRATION_PARSERS
        ) or pre_lineage_full_history_bootstrap:
            catalog_as_of_date: Optional[str] = None
        else:
            catalog_as_of_date = _catalog_manifest_as_of_date(row[8]).isoformat()
        self._validate_catalog_lineage(
            {
                "batch_id": str(row[0]),
                "payload_sha256": str(row[1] or ""),
                "parser_version": parser_version,
                "raw_inputs_json": raw_inputs_json,
                "raw_provenance_sha256": str(row[7] or ""),
                "discovery_mode": str(row[6] or ""),
                "as_of_date": row[8],
                "parent_catalog_batch_id": str(row[9] or ""),
                "parent_catalog_payload_sha256": str(row[10] or ""),
                "parent_catalog_raw_provenance_sha256": str(row[11] or ""),
            },
            allow_legacy_parser_for_full_history=(allow_legacy_parser_for_full_history),
        )
        return {
            "catalog_batch_id": str(row[0]),
            "catalog_payload_sha256": str(row[1]),
            "catalog_parser_version": parser_version,
            "catalog_raw_uri": str(row[3]),
            "catalog_raw_inputs_sha256": hashlib.sha256(
                raw_inputs_json.encode("utf-8")
            ).hexdigest(),
            "catalog_completed_at": str(row[5]),
            "catalog_discovery_mode": str(row[6] or ""),
            "catalog_raw_provenance_sha256": str(row[7] or ""),
            "catalog_as_of_date": catalog_as_of_date,
            "parent_catalog_batch_id": str(row[9] or "") or None,
            "parent_catalog_payload_sha256": str(row[10] or "") or None,
            "parent_catalog_raw_provenance_sha256": (str(row[11] or "") or None),
        }

    def latest_catalog_generation(self) -> dict[str, Any]:
        """Return verifiable provenance for the current catalog snapshot."""

        return self.catalog_generation()

    def list_catalog_scopes(
        self,
        *,
        active_only: bool = True,
        include_quarantined: bool = False,
    ) -> list[WhoScoredScope]:
        catalog = self.load_discovered_catalog()
        if catalog.quarantined and not include_quarantined:
            raise ValueError(
                f"WhoScored catalog has {len(catalog.quarantined)} quarantined records"
            )
        return [
            season.scope for season in catalog.eligible_scopes(active_only=active_only)
        ]

    def _ensure_preview_schema(self) -> None:
        """Create the append-only preview payload and its logical commit log."""
        for table in PREVIEW_DATASET_TABLES.values():
            self._ensure_business_table_contract(table)

        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._preview_manifest} (
                league VARCHAR,
                season VARCHAR,
                game_id BIGINT,
                game VARCHAR,
                kickoff TIMESTAMP(6),
                batch_id VARCHAR,
                payload_sha256 VARCHAR,
                raw_uri VARCHAR,
                parser_version VARCHAR,
                availability_version VARCHAR,
                state VARCHAR,
                missing_players_count BIGINT,
                entity_counts_json VARCHAR,
                dataset_statuses_json VARCHAR,
                schema_fingerprint VARCHAR,
                transport_mode VARCHAR,
                proxy_mode VARCHAR,
                http_status INTEGER,
                failure_code VARCHAR,
                error VARCHAR,
                attempt_no INTEGER,
                retry_after TIMESTAMP(6),
                fetched_at TIMESTAMP(6),
                completed_at TIMESTAMP(6),
                direct_bytes BIGINT,
                paid_bytes BIGINT,
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            ) WITH (partitioning = ARRAY['league', 'season'])
            """
        )
        manifest_columns = {
            name.lower()
            for name in self.trino.get_table_columns(
                self.schema, PREVIEW_MANIFEST_TABLE
            )
        }
        for name, data_type in {
            "entity_counts_json": "VARCHAR",
            "dataset_statuses_json": "VARCHAR",
            "schema_fingerprint": "VARCHAR",
            "availability_version": "VARCHAR",
        }.items():
            if name not in manifest_columns:
                self.trino.add_column(
                    self.schema, PREVIEW_MANIFEST_TABLE, name, data_type
                )

    def _ensure_profile_schema(self, *, create_views: bool = True) -> None:
        versions = f"{self.catalog}.{self.schema}.{PROFILE_VERSIONS_TABLE}"
        manifest = f"{self.catalog}.{self.schema}.{PROFILE_MANIFEST_TABLE}"
        participations = (
            f"{self.catalog}.{self.schema}.whoscored_player_stage_participations"
        )
        self._ensure_business_table_contract(PROFILE_VERSIONS_TABLE)
        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {manifest} (
                player_id BIGINT,
                payload_sha256 VARCHAR,
                raw_uri VARCHAR,
                parser_version VARCHAR,
                availability_version VARCHAR,
                state VARCHAR,
                http_status INTEGER,
                failure_code VARCHAR,
                error VARCHAR,
                attempt_no INTEGER,
                retry_after TIMESTAMP(6),
                transport_mode VARCHAR,
                proxy_mode VARCHAR,
                direct_bytes BIGINT,
                paid_bytes BIGINT,
                fetched_at TIMESTAMP(6),
                completed_at TIMESTAMP(6),
                participations_count BIGINT,
                _profile_batch_id VARCHAR,
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            ) WITH (partitioning = ARRAY['bucket(player_id, 32)'])
            """
        )
        self._ensure_business_table_contract("whoscored_player_stage_participations")
        manifest_columns = {
            name.lower()
            for name in self.trino.get_table_columns(
                self.schema, PROFILE_MANIFEST_TABLE
            )
        }
        for name, data_type in {
            "_profile_batch_id": "VARCHAR",
            "participations_count": "BIGINT",
            "availability_version": "VARCHAR",
        }.items():
            if name not in manifest_columns:
                self.trino.add_column(
                    self.schema, PROFILE_MANIFEST_TABLE, name, data_type
                )
        if not create_views:
            return
        roster = f"{self.catalog}.{self.schema}.whoscored_player_roster"
        # A profile is a global source entity.  Candidate discovery must not
        # depend on a player having appeared in a confirmed lineup: substitutes,
        # stage-stat-only players and preview/injury-only players are all valid
        # profile targets.  Every input below is a manifest-filtered current
        # view, so an orphan or partial payload can never expand the roster.
        roster_sources = (
            ("whoscored_lineups", "game_id", "player_id", "team"),
            ("whoscored_player_match_stats", "game_id", "player_id", "team"),
            ("whoscored_events", "game_id", "player_id", "team"),
            ("whoscored_substitutions", "game_id", "player_id", "team"),
            ("whoscored_substitutions", "game_id", "related_player_id", "team"),
            ("whoscored_player_stage_stats", None, "player_id", "team"),
            ("whoscored_missing_players", "game_id", "player_id", "team"),
            ("whoscored_preview_lineups", "game_id", "player_id", "team"),
        )
        roster_selects: list[str] = []
        for table, game_column, player_column, team_column in roster_sources:
            if not self.trino.table_exists(self.schema, table):
                continue
            game_expr = (
                f"CAST({game_column} AS BIGINT)"
                if game_column
                else "CAST(NULL AS BIGINT)"
            )
            team_expr = (
                f"CAST({team_column} AS VARCHAR)"
                if team_column
                else "CAST(NULL AS VARCHAR)"
            )
            roster_selects.append(
                "SELECT "
                f"{game_expr} AS game_id, CAST({player_column} AS BIGINT) AS player_id, "
                f"league, season, {team_expr} AS team "
                f"FROM {self.catalog}.{self.schema}.{table}_current "
                f"WHERE {player_column} IS NOT NULL"
            )
        if roster_selects:
            self.trino._execute(
                f"CREATE OR REPLACE VIEW {roster} AS "
                "SELECT DISTINCT game_id, player_id, league, season, team FROM ("
                + " UNION ALL ".join(roster_selects)
                + ")"
            )
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW {self.catalog}.silver.whoscored_player_profile_current AS
            WITH latest AS (
                SELECT * FROM (
                    SELECT m.*, ROW_NUMBER() OVER (
                        PARTITION BY player_id
                        ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                 _profile_batch_id DESC, _batch_id DESC
                    ) AS rn
                    FROM {manifest} m
                    WHERE m.state = 'success'
                ) WHERE rn = 1
            )
            SELECT player_id, name, current_team_id, current_team_name,
                   shirt_number, age, date_of_birth, height_cm, nationality,
                   country_code, positions, payload_sha256, raw_uri,
                   parser_version, fetched_at
            FROM (
                SELECT p.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY p.player_id
                           ORDER BY p.fetched_at DESC, p._ingested_at DESC
                       ) AS physical_rn
                FROM {versions} p
                JOIN latest m
                  ON m.player_id = p.player_id
                 AND (
                      m.payload_sha256 = p.payload_sha256
                      OR (
                          m.payload_sha256 IS NULL
                          AND p.payload_sha256 IS NULL
                      )
                 )
                 AND (
                      m.parser_version = p.parser_version
                      OR (
                          m.parser_version IS NULL
                          AND p.parser_version IS NULL
                      )
                 )
                 AND (
                      m._profile_batch_id = p._profile_batch_id
                      OR (
                          m._profile_batch_id IS NULL
                          AND p._profile_batch_id IS NULL
                      )
                 )
            ) WHERE physical_rn = 1
            """
        )
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW
                {self.catalog}.{self.schema}.whoscored_player_stage_participations_current
            AS
            WITH latest AS (
                SELECT * FROM (
                    SELECT m.*, ROW_NUMBER() OVER (
                        PARTITION BY player_id
                        ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                 _profile_batch_id DESC, _batch_id DESC
                    ) AS rn
                    FROM {manifest} m
                    WHERE m.state = 'success'
                      AND m._profile_batch_id LIKE 'wspr2-%'
                ) WHERE rn = 1
            )
            SELECT p.*
            FROM {participations} p
            JOIN latest m
              ON m.player_id = p.player_id
             AND m._profile_batch_id = p._profile_batch_id
             AND m.payload_sha256 = p._payload_sha256
             AND m.parser_version = p._parser_version
            """
        )

    @staticmethod
    def _profile_candidate_query(
        *,
        catalog: str,
        schema: str,
        scopes: Sequence[WhoScoredScope],
        include_exact_count: bool,
        limit: int,
    ) -> Optional[str]:
        """Build the one canonical daily due/retry candidate predicate."""

        selected = tuple(
            dict.fromkeys((scope.competition_id, scope.season_id) for scope in scopes)
        )
        if not selected or limit == 0:
            return None
        roster = f"{catalog}.{schema}.whoscored_player_roster"
        manifest = f"{catalog}.{schema}.{PROFILE_MANIFEST_TABLE}"
        scope_filter = " OR ".join(
            f"(league = {_sql_string(league)} AND season = {_sql_string(season)})"
            for league, season in selected
        )
        count_projection = ", COUNT(*) OVER () AS exact_candidate_count"
        if not include_exact_count:
            count_projection = ""
        return f"""
            WITH latest AS (
                SELECT * FROM (
                    SELECT m.*, ROW_NUMBER() OVER (
                        PARTITION BY CAST(player_id AS BIGINT)
                        ORDER BY COALESCE(
                            completed_at, fetched_at, _ingested_at
                        ) DESC, COALESCE(_profile_batch_id, '') DESC,
                        COALESCE(_batch_id, '') DESC
                    ) AS rn
                    FROM {manifest} m
                ) WHERE rn = 1
            ), roster AS (
                SELECT DISTINCT CAST(player_id AS BIGINT) AS player_id
                FROM {roster}
                WHERE player_id IS NOT NULL AND ({scope_filter})
            ), candidates AS (
                SELECT r.player_id,
                       CASE
                           WHEN m.player_id IS NULL THEN 0
                           WHEN m._profile_batch_id IS NULL
                             OR m._profile_batch_id NOT LIKE 'wspr2-%' THEN 1
                           ELSE 2
                       END AS candidate_priority,
                       COALESCE(
                           m.fetched_at, TIMESTAMP '1970-01-01 00:00:00'
                       ) AS candidate_fetched_at
                FROM roster r
                LEFT JOIN latest m ON CAST(m.player_id AS BIGINT) = r.player_id
                WHERE m.player_id IS NULL
                   OR (
                        m.state = 'success'
                        AND (
                            m._profile_batch_id IS NULL
                            OR m._profile_batch_id NOT LIKE 'wspr2-%'
                            OR m.raw_uri IS NULL
                            OR m.payload_sha256 IS NULL
                            OR m.parser_version IS DISTINCT FROM
                                {_sql_string(PARSER_VERSION)}
                            OR COALESCE(
                                m.fetched_at, TIMESTAMP '1970-01-01 00:00:00'
                            ) <= CAST(
                                CURRENT_TIMESTAMP
                                    - INTERVAL '{PROFILE_REFRESH_DAYS}' DAY
                                AS TIMESTAMP
                            )
                        )
                   )
                   OR (
                        m.state = 'retryable'
                        AND COALESCE(
                            m.retry_after, TIMESTAMP '1970-01-01 00:00:00'
                        ) <= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
                   )
                   OR (
                        m.state = 'parse_failed'
                        AND m.parser_version IS DISTINCT FROM
                            {_sql_string(PARSER_VERSION)}
                   )
                   OR (
                        m.state = 'not_available'
                        AND (
                            m.parser_version IS DISTINCT FROM
                                {_sql_string(PARSER_VERSION)}
                            OR m.availability_version IS DISTINCT FROM
                                {_sql_string(MATCH_AVAILABILITY_VERSION)}
                            OR m.failure_code IS NULL
                            OR (
                                m.raw_uri IS NULL
                                AND COALESCE(m.http_status, 0) NOT IN (404, 410)
                            )
                        )
                   )
            )
            SELECT player_id{count_projection}
            FROM candidates
            ORDER BY candidate_priority, candidate_fetched_at, player_id
            LIMIT {limit}
            """

    def list_profile_candidates(
        self,
        *,
        scopes: Sequence[WhoScoredScope],
        limit: int = 500,
    ) -> list[int]:
        """Return a bounded prefix of the canonical daily candidate set."""

        if int(limit) < 0:
            raise ValueError("profile candidate limit must be non-negative")
        query = self._profile_candidate_query(
            catalog=self.catalog,
            schema=self.schema,
            scopes=scopes,
            include_exact_count=False,
            limit=int(limit),
        )
        if query is None:
            return []
        rows = self.trino.execute_query(query)
        return [int(row[0]) for row in rows]

    def profile_candidate_snapshot(
        self,
        *,
        scopes: Sequence[WhoScoredScope],
        hard_cap: int = MAX_DAILY_PROFILE_CANDIDATES,
    ) -> ProfileCandidateSnapshot:
        """Freeze every currently due profile or fail before source traffic.

        ``COUNT(*) OVER`` and the IDs are read by one Trino query, so the
        count cannot describe a different manifest generation from the set it
        fingerprints. The extra row is only a defensive bound on the result;
        the window count remains the exact backlog size.
        """

        if type(hard_cap) is not int or not 1 <= hard_cap <= 3_000:
            raise ValueError("profile candidate hard cap must be in 1..3000")
        query = self._profile_candidate_query(
            catalog=self.catalog,
            schema=self.schema,
            scopes=scopes,
            include_exact_count=True,
            limit=hard_cap + 1,
        )
        if query is None:
            player_ids: tuple[int, ...] = ()
            return ProfileCandidateSnapshot(
                player_ids=player_ids,
                count=0,
                payload_sha256=profile_candidate_payload_sha256(player_ids),
            )
        rows = self.trino.execute_query(query)
        if not rows:
            player_ids = ()
            return ProfileCandidateSnapshot(
                player_ids=player_ids,
                count=0,
                payload_sha256=profile_candidate_payload_sha256(player_ids),
            )
        exact_counts = {int(row[1]) for row in rows}
        if len(exact_counts) != 1:
            raise BatchConflict("profile candidate query returned inconsistent counts")
        count = exact_counts.pop()
        if count < 0 or count < len(rows):
            raise BatchConflict("profile candidate query returned an invalid count")
        if count > hard_cap:
            raise ProfileCandidateCapacityExceeded(count=count, hard_cap=hard_cap)
        if len(rows) != count:
            raise BatchConflict(
                "profile candidate query did not return its complete exact set"
            )
        player_ids = tuple(sorted(int(row[0]) for row in rows))
        return ProfileCandidateSnapshot(
            player_ids=player_ids,
            count=count,
            payload_sha256=profile_candidate_payload_sha256(player_ids),
        )

    def list_roster_player_ids(
        self,
        *,
        scopes: Sequence[WhoScoredScope],
    ) -> list[int]:
        """Freeze every roster player in the selected historical scopes.

        Unlike the daily candidate query this deliberately ignores profile
        manifest state, refresh cadence and retry_after. A historical plan
        must remain capable of repairing every current/failed/missing profile.
        """
        selected = tuple(
            dict.fromkeys((scope.competition_id, scope.season_id) for scope in scopes)
        )
        if not selected:
            return []
        roster = f"{self.catalog}.{self.schema}.whoscored_player_roster"
        scope_filter = " OR ".join(
            f"(league = {_sql_string(league)} AND season = {_sql_string(season)})"
            for league, season in selected
        )
        rows = self.trino.execute_query(
            f"""
            SELECT DISTINCT CAST(player_id AS BIGINT)
            FROM {roster}
            WHERE player_id IS NOT NULL AND ({scope_filter})
            ORDER BY CAST(player_id AS BIGINT)
            """
        )
        return [int(row[0]) for row in rows]

    def record_profile_failure(
        self,
        *,
        player_id: int,
        state: str,
        failure_code: str,
        error: str,
        retry_after: Optional[datetime],
        transport_mode: str = "none",
        proxy_mode: str = "none",
        http_status: Optional[int] = None,
        direct_bytes: int = 0,
        paid_bytes: int = 0,
        payload_sha256: Optional[str] = None,
        raw_uri: Optional[str] = None,
        attempt_no: int = 1,
        availability_version: str = MATCH_AVAILABILITY_VERSION,
    ) -> Optional[str]:
        """Persist a failed profile attempt in the current manifest schema."""
        if state not in {
            "retryable",
            "terminal",
            "parse_failed",
            "not_available",
        }:
            raise ValueError(f"unsupported profile failure state: {state}")
        if state == "retryable" and retry_after is None:
            raise ValueError("retryable profile failure requires retry_after")
        if state != "retryable" and retry_after is not None:
            raise ValueError(f"{state} profile failure cannot have retry_after")
        if int(attempt_no) < 1:
            raise ValueError("profile failure attempt_no must be positive")
        if (
            state == "not_available"
            and raw_uri is None
            and http_status not in {404, 410}
        ):
            raise ValueError(
                "not_available profile failure requires raw proof or HTTP 404/410"
            )

        outcome_batch_id = None
        if state == "not_available":
            outcome_batch_id = deterministic_profile_not_available_batch_id(
                player_id,
                failure_code=failure_code,
                http_status=http_status,
                payload_sha256=payload_sha256,
                raw_uri=raw_uri,
                availability_version=availability_version,
            )

        now = _utc_now()
        row = {
            "player_id": int(player_id),
            "payload_sha256": payload_sha256,
            "raw_uri": raw_uri,
            "parser_version": PARSER_VERSION,
            "availability_version": availability_version,
            "state": state,
            "http_status": http_status,
            "failure_code": str(failure_code),
            "error": str(error)[:4000],
            "attempt_no": int(attempt_no),
            "retry_after": retry_after,
            "transport_mode": transport_mode,
            "proxy_mode": proxy_mode,
            "direct_bytes": int(direct_bytes),
            "paid_bytes": int(paid_bytes),
            "fetched_at": now,
            "completed_at": now if state != "retryable" else None,
            "_profile_batch_id": outcome_batch_id,
            "_entity_type": "profile_manifest",
        }
        self.writer.write_dataframe(
            pd.DataFrame([row]),
            database=self.schema,
            table=PROFILE_MANIFEST_TABLE,
            partition_spec=[("player_id", "bucket(32)")],
            source="whoscored",
        )
        return outcome_batch_id

    @_lock_commit_sequence
    def commit_profiles(self, commits: Sequence[ProfileCommit]) -> tuple[str, ...]:
        """Batch profile versions and manifests without one-row Iceberg writes."""
        ordered = tuple(commits)
        if not ordered:
            return ()
        seen_players: set[int] = set()
        for commit in ordered:
            if int(commit.player_id) in seen_players:
                raise ValueError(f"duplicate profile player_id {commit.player_id}")
            seen_players.add(int(commit.player_id))
            if not commit.payload_sha256 or not commit.raw_uri:
                raise ValueError("profile raw identity is required")
            if int(commit.profile.get("player_id", commit.player_id)) != int(
                commit.player_id
            ):
                raise ValueError(f"profile identity mismatch for {commit.player_id}")
            if commit.participations_status not in {"available", "empty"}:
                raise ValueError(
                    f"profile {commit.player_id} participation dataset has invalid "
                    f"status {commit.participations_status!r}"
                )
            if bool(commit.participations) != (
                commit.participations_status == "available"
            ):
                raise ValueError(
                    f"profile {commit.player_id} participation status disagrees "
                    "with parsed rows"
                )

        batch_ids = [commit.batch_id for commit in ordered]
        expected_by_batch = {
            commit.batch_id: (
                int(commit.player_id),
                commit.payload_sha256,
                commit.parser_version,
            )
            for commit in ordered
        }
        if len(expected_by_batch) != len(ordered):
            raise BatchConflict("duplicate profile batch identity")
        quoted_batches = ",".join(_sql_string(value) for value in batch_ids)
        versions = f"{self.catalog}.{self.schema}.{PROFILE_VERSIONS_TABLE}"
        manifest = f"{self.catalog}.{self.schema}.{PROFILE_MANIFEST_TABLE}"
        physical_rows = self.trino.execute_query(
            f"SELECT _profile_batch_id, player_id, payload_sha256, "
            f"parser_version, COUNT(*) FROM {versions} "
            f"WHERE _profile_batch_id IN ({quoted_batches}) "
            "GROUP BY _profile_batch_id, player_id, payload_sha256, parser_version"
        )
        physical: dict[str, int] = {}
        for row in physical_rows:
            batch_id = str(row[0])
            identity = (int(row[1]), str(row[2]), str(row[3]))
            if expected_by_batch.get(batch_id) != identity:
                raise BatchConflict(
                    f"profile batch {batch_id} has unexpected identity {identity}"
                )
            if batch_id in physical:
                raise BatchConflict(
                    f"profile batch {batch_id} has multiple physical identities"
                )
            count = int(row[4])
            physical[batch_id] = count
            if count > 1:
                raise BatchConflict(f"profile batch {batch_id} has {count} rows")

        success_rows = self.trino.execute_query(
            f"SELECT _profile_batch_id, player_id, payload_sha256, "
            "parser_version, MAX(COALESCE(participations_count, 0)) "
            f"FROM {manifest} WHERE _profile_batch_id IN ({quoted_batches}) "
            "AND state = 'success' GROUP BY _profile_batch_id, player_id, "
            "payload_sha256, parser_version"
        )
        successes: dict[str, int] = {}
        for row in success_rows:
            batch_id = str(row[0])
            identity = (int(row[1]), str(row[2]), str(row[3]))
            if expected_by_batch.get(batch_id) != identity:
                raise BatchConflict(
                    f"profile success {batch_id} has unexpected identity {identity}"
                )
            if batch_id in successes:
                raise BatchConflict(
                    f"profile batch {batch_id} has multiple success identities"
                )
            successes[batch_id] = int(row[4])
        participation_counts: dict[str, int] = {}
        if self.trino.table_exists(
            self.schema, "whoscored_player_stage_participations"
        ):
            participation_rows_existing = self.trino.execute_query(
                f"SELECT _profile_batch_id, COUNT(*) FROM "
                f"{self.catalog}.{self.schema}.whoscored_player_stage_participations "
                f"WHERE _profile_batch_id IN ({quoted_batches}) "
                "GROUP BY _profile_batch_id"
            )
            participation_counts = {
                str(row[0]): int(row[1]) for row in participation_rows_existing
            }
        for commit in ordered:
            identity = (
                int(commit.player_id),
                commit.payload_sha256,
                commit.parser_version,
            )
            physical_count = physical.get(commit.batch_id, 0)
            participation_count = participation_counts.get(commit.batch_id, 0)
            expected_participations = len(commit.participations)
            if commit.batch_id in successes and physical_count != 1:
                raise BatchConflict(
                    f"profile {commit.player_id}/{commit.payload_sha256} is "
                    f"committed but has {physical_count} physical versions"
                )
            if (
                commit.batch_id in successes
                and participation_count != expected_participations
            ):
                raise BatchConflict(
                    f"profile {identity} is committed but has "
                    f"{participation_count}/{expected_participations} participations"
                )
            if commit.batch_id not in successes and participation_count not in {
                0,
                expected_participations,
            }:
                raise BatchConflict(
                    f"profile {identity} has partial participation batch "
                    f"{participation_count}/{expected_participations}"
                )

        version_rows: list[dict[str, Any]] = []
        participation_rows: list[dict[str, Any]] = []
        now = _utc_now()
        for commit in ordered:
            if physical.get(commit.batch_id, 0) == 0:
                row = dict(commit.profile)
                row.update(
                    {
                        "player_id": int(commit.player_id),
                        "payload_sha256": commit.payload_sha256,
                        "raw_uri": commit.raw_uri,
                        "parser_version": commit.parser_version,
                        "fetched_at": commit.fetched_at or now,
                        "_profile_batch_id": commit.batch_id,
                        "_entity_type": "player_profile",
                    }
                )
                version_rows.append(row)
            if participation_counts.get(commit.batch_id, 0):
                continue
            for participation in commit.participations:
                row = dict(participation)
                row.update(
                    {
                        "player_id": int(commit.player_id),
                        "payload_json": self._canonical_json(participation),
                        "_profile_batch_id": commit.batch_id,
                        "_payload_sha256": commit.payload_sha256,
                        "_parser_version": commit.parser_version,
                        "_entity_type": "player_stage_participation",
                    }
                )
                participation_rows.append(row)

        if version_rows:
            self.writer.write_dataframe(
                self._normalise_frame_types(
                    pd.DataFrame(version_rows), table=PROFILE_VERSIONS_TABLE
                ),
                database=self.schema,
                table=PROFILE_VERSIONS_TABLE,
                partition_spec=[("player_id", "bucket(32)")],
                source="whoscored",
            )
        if participation_rows:
            self.writer.write_dataframe(
                self._normalise_frame_types(
                    pd.DataFrame(participation_rows),
                    table="whoscored_player_stage_participations",
                ),
                database=self.schema,
                table="whoscored_player_stage_participations",
                partition_spec=[("player_id", "bucket(32)")],
                source="whoscored",
            )

        verified_rows = self.trino.execute_query(
            f"SELECT _profile_batch_id, player_id, payload_sha256, "
            f"parser_version, COUNT(*) FROM {versions} "
            f"WHERE _profile_batch_id IN ({quoted_batches}) "
            "GROUP BY _profile_batch_id, player_id, payload_sha256, parser_version"
        )
        verified: dict[str, int] = {}
        for row in verified_rows:
            batch_id = str(row[0])
            identity = (int(row[1]), str(row[2]), str(row[3]))
            if expected_by_batch.get(batch_id) != identity:
                raise BatchConflict(
                    f"profile batch {batch_id} has unexpected verified identity "
                    f"{identity}"
                )
            if batch_id in verified:
                raise BatchConflict(
                    f"profile batch {batch_id} has multiple verified identities"
                )
            verified[batch_id] = int(row[4])
        for commit in ordered:
            identity = (
                int(commit.player_id),
                commit.payload_sha256,
                commit.parser_version,
            )
            if verified.get(commit.batch_id) != 1:
                raise BatchConflict(
                    f"profile {identity} physical count is "
                    f"{verified.get(commit.batch_id, 0)}"
                )
        if self.trino.table_exists(
            self.schema, "whoscored_player_stage_participations"
        ):
            rows = self.trino.execute_query(
                f"SELECT _profile_batch_id, COUNT(*) FROM "
                f"{self.catalog}.{self.schema}.whoscored_player_stage_participations "
                f"WHERE _profile_batch_id IN ({quoted_batches}) "
                "GROUP BY _profile_batch_id"
            )
            verified_participations = {str(row[0]): int(row[1]) for row in rows}
            for commit in ordered:
                expected = len(commit.participations)
                actual = verified_participations.get(commit.batch_id, 0)
                if actual != expected:
                    raise BatchConflict(
                        f"profile {commit.player_id} participation count is "
                        f"{actual}, expected {expected}"
                    )

        # A profile version is content-addressed, but its refresh cadence is an
        # observation-level concern.  Re-fetching an unchanged payload after
        # PROFILE_REFRESH_DAYS must advance the success manifest without
        # duplicating the physical version.  It must also supersede a newer
        # retryable/parse_failed attempt which recovered to the same payload.
        # Conversely, a retry after that heartbeat must remain idempotent.
        player_ids_sql = ",".join(str(int(commit.player_id)) for commit in ordered)
        latest_rows = self.trino.execute_query(
            "SELECT player_id, payload_sha256, parser_version, state, "
            "_profile_batch_id, fetched_at FROM ("
            "SELECT m.*, ROW_NUMBER() OVER (PARTITION BY player_id "
            "ORDER BY fetched_at DESC, _ingested_at DESC, _batch_id DESC) AS rn "
            f"FROM {manifest} m WHERE player_id IN ({player_ids_sql})"
            ") WHERE rn = 1"
        )
        latest_by_player = {int(row[0]): row for row in latest_rows}
        refresh_cutoff = now - timedelta(days=PROFILE_REFRESH_DAYS)

        manifest_rows: list[dict[str, Any]] = []
        for commit in ordered:
            identity = (
                int(commit.player_id),
                commit.payload_sha256,
                commit.parser_version,
            )
            expected_participations = len(commit.participations)
            if commit.batch_id in successes:
                if successes[commit.batch_id] != expected_participations:
                    raise BatchConflict(
                        f"profile {identity} participation count: "
                        f"manifest={successes[commit.batch_id]}, "
                        f"parser={expected_participations}"
                    )
            latest = latest_by_player.get(int(commit.player_id))
            if latest is not None:
                latest_fetched_at = latest[5]
                if isinstance(latest_fetched_at, datetime):
                    if latest_fetched_at.tzinfo is not None:
                        latest_fetched_at = latest_fetched_at.astimezone(
                            timezone.utc
                        ).replace(tzinfo=None)
                else:
                    latest_fetched_at = None
                already_fresh = (
                    str(latest[3]) == "success"
                    and str(latest[1] or "") == commit.payload_sha256
                    and str(latest[2] or "") == commit.parser_version
                    and str(latest[4] or "") == commit.batch_id
                    and latest_fetched_at is not None
                    and latest_fetched_at > refresh_cutoff
                )
                if already_fresh:
                    continue
            manifest_rows.append(
                {
                    "player_id": int(commit.player_id),
                    "payload_sha256": commit.payload_sha256,
                    "raw_uri": commit.raw_uri,
                    "parser_version": commit.parser_version,
                    "availability_version": commit.availability_version,
                    "state": "success",
                    "http_status": 200,
                    "failure_code": None,
                    "error": None,
                    "attempt_no": 1,
                    "retry_after": None,
                    "transport_mode": commit.transport_mode,
                    "proxy_mode": commit.proxy_mode,
                    "direct_bytes": int(commit.direct_bytes),
                    "paid_bytes": int(commit.paid_bytes),
                    "fetched_at": commit.fetched_at or now,
                    "completed_at": now,
                    "participations_count": expected_participations,
                    "_profile_batch_id": commit.batch_id,
                    "_entity_type": "profile_manifest",
                }
            )
        if manifest_rows:
            self.writer.write_dataframe(
                pd.DataFrame(manifest_rows),
                database=self.schema,
                table=PROFILE_MANIFEST_TABLE,
                partition_spec=[("player_id", "bucket(32)")],
                source="whoscored",
            )
        return tuple(commit.batch_id for commit in ordered)

    def _create_current_views(self) -> None:
        latest = f"{self.catalog}.{self.schema}.whoscored_match_ingest_latest"
        latest_success = (
            f"{self.catalog}.{self.schema}.whoscored_match_ingest_latest_success"
        )
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW {latest} AS
            SELECT * FROM (
                SELECT m.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY league, season, game_id
                           ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                    batch_id DESC, _batch_id DESC
                       ) AS _manifest_rank
                FROM {self._manifest} m
            ) WHERE _manifest_rank = 1
            """
        )
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW {latest_success} AS
            SELECT * FROM (
                SELECT m.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY league, season, game_id
                           ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                    batch_id DESC, _batch_id DESC
                       ) AS _manifest_rank
                FROM {self._manifest} m
                WHERE state = 'success'
                  AND batch_id LIKE 'ws2-%'
                  AND raw_uri IS NOT NULL
            ) WHERE _manifest_rank = 1
            """
        )
        for table in MATCH_DATASET_TABLES.values():
            physical = f"{self.catalog}.{self.schema}.{table}"
            current = f"{self.catalog}.{self.schema}.{table}_current"
            if not self.trino.table_exists(self.schema, table):
                continue
            self.trino._execute(
                f"""
                CREATE OR REPLACE VIEW {current} AS
                SELECT d.*
                FROM {physical} d
                JOIN {latest_success} m
                  ON m.league = d.league
                 AND m.season = d.season
                 AND m.game_id = CAST(d.game_id AS BIGINT)
                 AND m.state = 'success'
                 AND m.batch_id = d._game_batch_id
                UNION ALL
                SELECT d.*
                FROM {physical} d
                WHERE d._game_batch_id IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM {latest_success} m
                      WHERE m.league = d.league
                        AND m.season = d.season
                        AND m.game_id = CAST(d.game_id AS BIGINT)
                  )
                """
            )

        preview_latest = f"{self.catalog}.{self.schema}.whoscored_preview_ingest_latest"
        preview_latest_success = (
            f"{self.catalog}.{self.schema}.whoscored_preview_ingest_latest_success"
        )
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW {preview_latest} AS
            SELECT * FROM (
                SELECT m.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY league, season, game_id
                           ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                    COALESCE(batch_id, '') DESC, _batch_id DESC
                       ) AS _manifest_rank
                FROM {self._preview_manifest} m
            ) WHERE _manifest_rank = 1
            """
        )
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW {preview_latest_success} AS
            SELECT * FROM (
                SELECT m.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY league, season, game_id
                           ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                    batch_id DESC, _batch_id DESC
                       ) AS _manifest_rank
                FROM {self._preview_manifest} m
                WHERE state = 'success'
                  AND batch_id LIKE 'wsp2-%'
                  AND raw_uri IS NOT NULL
            ) WHERE _manifest_rank = 1
            """
        )
        for table in PREVIEW_DATASET_TABLES.values():
            if not self.trino.table_exists(self.schema, table):
                continue
            physical = f"{self.catalog}.{self.schema}.{table}"
            current = f"{self.catalog}.{self.schema}.{table}_current"
            self.trino._execute(
                f"""
                CREATE OR REPLACE VIEW {current} AS
                SELECT d.*
                FROM {physical} d
                JOIN {preview_latest_success} m
                  ON m.league = d.league
                 AND m.season = d.season
                 AND m.game_id = CAST(d.game_id AS BIGINT)
                 AND m.batch_id = d._preview_batch_id
                UNION ALL
                SELECT d.*
                FROM {physical} d
                WHERE d._preview_batch_id IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM {preview_latest_success} m
                      WHERE m.league = d.league
                        AND m.season = d.season
                        AND m.game_id = CAST(d.game_id AS BIGINT)
                  )
                """
            )

    def list_match_candidates(
        self,
        league: str,
        season: str,
        *,
        match_ids: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        include_success: bool = False,
        include_failed: bool = False,
        include_all_completed: bool = False,
        kickoff_from: Optional[datetime] = None,
    ) -> list[MatchCandidate]:
        """Return completed games without a successful manifest commit.

        ``match_is_opta`` is metadata, not an availability gate.  WhoScored
        exposes valid matchCentre payloads for some rows where that schedule
        flag is false, so filtering on it silently loses finished matches.
        """
        latest = f"{self.catalog}.{self.schema}.whoscored_match_ingest_latest"
        ids = [int(value) for value in (match_ids or [])]
        if include_success and not ids and not include_all_completed:
            raise ValueError("include_success requires explicit match_ids")
        id_filter = ""
        if ids:
            id_filter = (
                " AND CAST(s.game_id AS BIGINT) IN ("
                + ",".join(str(value) for value in ids)
                + ")"
            )
        kickoff_filter = ""
        if kickoff_from is not None:
            value = kickoff_from.replace(tzinfo=None).isoformat(
                sep=" ", timespec="seconds"
            )
            kickoff_filter = f" AND s.date >= TIMESTAMP {_sql_string(value)}"
        if limit is not None and int(limit) < 0:
            raise ValueError("match candidate limit must be non-negative")
        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
        failed_filter = (
            " OR m.state IN ('terminal', 'parse_failed')" if include_failed else ""
        )
        manifest_filter = (
            "TRUE"
            if include_success or include_all_completed
            else f"""
                (
                    m.game_id IS NULL
                    OR (
                    m.state = 'success'
                    AND (
                        m.batch_id IS NULL
                        OR m.batch_id NOT LIKE 'ws2-%'
                        OR m.raw_uri IS NULL
                        OR m.payload_sha256 IS NULL
                        OR m.parser_version IS DISTINCT FROM
                            {_sql_string(PARSER_VERSION)}
                        OR (
                            m.parser_version = {_sql_string(PARSER_VERSION)}
                            AND s.date >= CAST(
                                CURRENT_TIMESTAMP - INTERVAL '30' DAY AS TIMESTAMP
                            )
                            AND COALESCE(
                                m.fetched_at,
                                TIMESTAMP '1970-01-01 00:00:00'
                            ) <= CAST(
                                CURRENT_TIMESTAMP - INTERVAL '{MATCH_REFRESH_DAYS}' DAY
                                AS TIMESTAMP
                            )
                        )
                    )
                    )
                    OR (
                        m.state = 'retryable'
                        AND COALESCE(m.retry_after, TIMESTAMP '1970-01-01 00:00:00')
                            <= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
                    )
                    OR (
                        m.state = 'parse_failed'
                        AND (
                            m.parser_version IS DISTINCT FROM
                                {_sql_string(PARSER_VERSION)}
                            OR m.availability_version IS DISTINCT FROM
                                {_sql_string(MATCH_AVAILABILITY_VERSION)}
                        )
                    )
                    {failed_filter}
                )
            """
        )
        sql = f"""
            WITH schedule AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY league, season, game_id
                    ORDER BY _ingested_at DESC
                ) AS rn
                FROM {self.catalog}.{self.schema}.whoscored_schedule_current
                WHERE league = {_sql_string(league)}
                  AND season = {_sql_string(season)}
            )
            SELECT CAST(s.game_id AS BIGINT), s.league, s.season, s.game,
                   s.date, CAST(s.status AS INTEGER), s.match_is_opta,
                   CASE
                       WHEN m.state = 'retryable'
                       THEN COALESCE(m.attempt_no, 0) + 1
                       ELSE 1
                   END AS attempt_no
            FROM schedule s
            LEFT JOIN {latest} m
              ON m.league = s.league
             AND m.season = s.season
             AND m.game_id = CAST(s.game_id AS BIGINT)
            WHERE s.rn = 1
              AND s.game_id IS NOT NULL
              AND (
                    s.status = 6
                    OR (
                        s.status = 1
                        AND s.home_score IS NOT NULL
                        AND s.away_score IS NOT NULL
                        AND s.date <= CAST(
                            CURRENT_TIMESTAMP - INTERVAL '3' HOUR AS TIMESTAMP
                        )
                    )
              )
              AND ({manifest_filter})
              {id_filter}
              {kickoff_filter}
            ORDER BY s.date, s.game_id
            {limit_sql}
        """
        rows = self.trino.execute_query(sql)
        return [
            MatchCandidate(
                game_id=int(row[0]),
                league=str(row[1]),
                season=str(row[2]),
                game=str(row[3]),
                kickoff=row[4],
                status=int(row[5]),
                match_is_opta=_coerce_bool(row[6]),
                attempt_no=int(row[7]),
            )
            for row in rows
        ]

    def list_completed_match_candidates(
        self,
        league: str,
        season: str,
        *,
        match_ids: Optional[Iterable[int]] = None,
    ) -> list[MatchCandidate]:
        """Return the complete schedule-backed historical candidate set.

        This policy intentionally ignores every manifest state/version and
        retry_after. It is used only while freezing an immutable backfill plan;
        regular daily ingestion continues to use ``list_match_candidates``.
        """
        return self.list_match_candidates(
            league,
            season,
            match_ids=match_ids,
            limit=None,
            include_all_completed=True,
        )

    def latest_source_season_id(self, league: str, season: str) -> Optional[int]:
        if not self.trino.table_exists(self.schema, "whoscored_seasons"):
            return None
        rows = self.trino.execute_query(
            f"SELECT CAST(source_season_id AS BIGINT) "
            f"FROM {self.catalog}.{self.schema}.whoscored_seasons_current "
            f"WHERE competition_id = {_sql_string(league)} "
            f"AND season_id = {_sql_string(season)} "
            "AND source_season_id IS NOT NULL ORDER BY _ingested_at DESC LIMIT 1"
        )
        return int(rows[0][0]) if rows else None

    def _scope_batch_count(
        self, table: str, *, league: str, season: str, batch_id: str
    ) -> int:
        if not self.trino.table_exists(self.schema, table):
            return 0
        rows = self.trino.execute_query(
            f"SELECT COUNT(*) FROM {self.catalog}.{self.schema}.{table} "
            f"WHERE league = {_sql_string(league)} "
            f"AND season = {_sql_string(season)} "
            f"AND _scope_batch_id = {_sql_string(batch_id)}"
        )
        return int(rows[0][0]) if rows else 0

    @_lock_scope_commit
    def commit_scope_bundle(
        self,
        *,
        league: str,
        season: str,
        entity_group: str,
        datasets: Mapping[str, Sequence[Mapping[str, Any]]],
        distinct_keys: Mapping[str, str],
        payload_sha256: str,
        raw_uris: Sequence[str],
        source_empty: Iterable[str] = (),
        source_unavailable: Iterable[str] = (),
        feed_states: Optional[Mapping[str, str]] = None,
    ) -> str:
        """Publish multiple scope datasets through one logical commit point."""
        if not datasets:
            raise ValueError("scope commit requires at least one dataset")
        if not payload_sha256 or not raw_uris:
            raise ValueError("scope commit requires raw identity")
        allowed = SCOPE_DATASET_TABLES
        explicit_empty = set(source_empty)
        unavailable = set(source_unavailable)
        unknown_states = (explicit_empty | unavailable) - set(datasets)
        if unknown_states:
            raise ValueError(
                "scope dataset states reference unknown tables: "
                + ", ".join(sorted(unknown_states))
            )
        normalized_feed_states: dict[str, str] = {}
        for raw_key, raw_status in (feed_states or {}).items():
            key = str(raw_key)
            status = str(raw_status)
            if (
                not key
                or len(key) > 200
                or re.fullmatch(r"[A-Za-z0-9:_-]+", key) is None
            ):
                raise ValueError(f"unsafe WhoScored source feed key {key!r}")
            if status not in {"available", "empty", "not_available"}:
                raise ValueError(
                    f"WhoScored source feed {key!r} has invalid status {status!r}"
                )
            normalized_feed_states[key] = status
        dataset_states: dict[str, Any] = {
            table: (
                "empty"
                if table in explicit_empty
                else "not_available"
                if table in unavailable
                else "available"
            )
            for table in datasets
        }
        if normalized_feed_states:
            dataset_states["__feeds__"] = dict(sorted(normalized_feed_states.items()))
        row_sources: dict[str, Iterable[Mapping[str, Any]]] = {}
        counts: dict[str, int] = {}
        schema_fields: dict[str, list[str]] = {}
        for table, source_rows in datasets.items():
            if table not in allowed:
                raise ValueError(f"unsupported WhoScored business table {table!r}")
            key = distinct_keys.get(table)
            if not key or not key.replace("_", "").isalnum():
                raise ValueError(f"{table} requires a safe distinct key")
            try:
                row_count = len(source_rows)
            except TypeError as exc:
                raise ValueError(
                    f"{table} rows must be bounded and re-iterable"
                ) from exc
            if not row_count and table not in explicit_empty | unavailable:
                raise ValueError(
                    f"{table} is empty without an explicit source_empty state"
                )
            if isinstance(source_rows, WhoScoredScopeRowSpool):
                if (
                    source_rows.table != table
                    or source_rows.league != str(league)
                    or source_rows.season != str(season)
                ):
                    raise ValueError(f"{table} spool identity does not match commit")
                if row_count and key != "entity_key":
                    raise ValueError(
                        f"{table} spool supports only its entity_key contract"
                    )
                columns = set(source_rows.columns)
                if row_count and key not in columns:
                    raise ValueError(f"{table} lacks distinct key {key!r}")
            else:
                columns: set[str] = set()
                distinct_values: set[str] = set()
                observed = 0
                for source in source_rows:
                    row = dict(source)
                    observed += 1
                    columns.update(str(column) for column in row)
                    for column, expected in (
                        ("league", league),
                        ("season", season),
                    ):
                        value = row.get(column)
                        if value is not None and str(value) != str(expected):
                            raise ValueError(
                                f"{table} contains {column}={value!r} outside "
                                f"{expected!r}"
                            )
                    if key not in row:
                        raise ValueError(f"{table} lacks distinct key {key!r}")
                    value = row[key]
                    if value is None:
                        raise ValueError(f"{table}.{key} contains nulls")
                    normalized = str(value)
                    if normalized in distinct_values:
                        raise ValueError(f"{table}.{key} contains duplicates")
                    distinct_values.add(normalized)
                if observed != row_count:
                    raise ValueError(
                        f"{table} changed cardinality while validating: "
                        f"len={row_count}, iterated={observed}"
                    )
            schema_fields[table] = sorted(columns)
            counts[table] = row_count
            row_sources[table] = source_rows

        identity = self._canonical_json(
            {
                "league": league,
                "season": season,
                "entity_group": entity_group,
                "payload_sha256": payload_sha256,
                "parser_version": PARSER_VERSION,
                "feed_states": normalized_feed_states,
            }
        )
        batch_id = "wss2-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()
        already = self.trino.execute_query(
            f"SELECT entity_counts_json, dataset_states_json FROM {self._scope_manifest} "
            f"WHERE league = {_sql_string(league)} "
            f"AND season = {_sql_string(season)} "
            f"AND entity_group = {_sql_string(entity_group)} "
            f"AND batch_id = {_sql_string(batch_id)} AND state = 'success' "
            "ORDER BY completed_at DESC LIMIT 1"
        )
        if already:
            stored = json.loads(str(already[0][0]))
            if stored != counts:
                raise BatchConflict(
                    f"scope batch {batch_id} counts: manifest={stored}, parser={counts}"
                )
            stored_states = json.loads(str(already[0][1]))
            if stored_states != dataset_states:
                raise BatchConflict(
                    f"scope batch {batch_id} states: manifest={stored_states}, "
                    f"parser={dataset_states}"
                )
            for table, expected in counts.items():
                physical = self._scope_batch_count(
                    table, league=league, season=season, batch_id=batch_id
                )
                if physical != expected:
                    raise BatchConflict(
                        f"scope batch {batch_id}/{table}: physical={physical}, "
                        f"manifest={expected}"
                    )
            return batch_id

        previous_rows = self.trino.execute_query(
            f"SELECT entity_counts_json FROM {self._scope_manifest} "
            f"WHERE league = {_sql_string(league)} "
            f"AND season = {_sql_string(season)} "
            f"AND entity_group = {_sql_string(entity_group)} AND state = 'success' "
            "ORDER BY completed_at DESC, _ingested_at DESC LIMIT 1"
        )
        previous = json.loads(str(previous_rows[0][0])) if previous_rows else {}
        for table, new_count in counts.items():
            old_count = int(previous.get(table, 0))
            # WhoScored scope batches are complete snapshots.  Neither an
            # apparently authoritative empty response nor a small percentage
            # drop is sufficient evidence to hide rows that were already
            # published.  Corrections that genuinely remove source rows must
            # be replayed explicitly after review instead of being accepted by
            # an unattended daily run.
            if (
                table not in SCOPE_SHRINKABLE_DATASET_TABLES
                and old_count
                and new_count < old_count
            ):
                raise ValueError(
                    f"{table} completeness guard: new={new_count}, old={old_count}, "
                    "published snapshot cannot shrink"
                )

        schema_fingerprint = hashlib.sha256(
            self._canonical_json(schema_fields).encode("utf-8")
        ).hexdigest()
        chunk_rows = scope_write_chunk_rows_from_env()
        for table, source_rows in row_sources.items():
            table_lock = (
                ("scope-table:whoscored_player_stage_stats",)
                if table == "whoscored_player_stage_stats"
                else ()
            )
            with self._commit_locks(table_lock):
                existing_count = self._scope_batch_count(
                    table, league=league, season=season, batch_id=batch_id
                )
                if existing_count == counts[table]:
                    continue
                if existing_count:
                    # No success manifest references this exact batch (the early
                    # return above already handled that case). A previous chunked
                    # attempt therefore left a hidden physical orphan. Remove only
                    # that unpublished batch and replay it from deterministic raw
                    # inputs. The player table lock also serializes this DELETE
                    # with PyIceberg appends for other concurrently running scopes.
                    self.trino._execute(
                        f"DELETE FROM {self.catalog}.{self.schema}.{table} "
                        f"WHERE league = {_sql_string(league)} "
                        f"AND season = {_sql_string(season)} "
                        f"AND _scope_batch_id = {_sql_string(batch_id)}"
                    )
                    remaining = self._scope_batch_count(
                        table, league=league, season=season, batch_id=batch_id
                    )
                    if remaining:
                        raise BatchConflict(
                            f"scope batch {batch_id}/{table} orphan cleanup left "
                            f"{remaining} rows"
                        )
                if not counts[table]:
                    continue
                pending: list[dict[str, Any]] = []

                def write_pending() -> None:
                    if not pending:
                        return
                    frame = pd.DataFrame(pending)
                    # Pandas has already extracted the column arrays. Release
                    # the Python row dictionaries before normalization,
                    # metadata copies and Arrow conversion overlap in memory.
                    pending.clear()
                    frame["batch_schema_fingerprint"] = schema_fingerprint
                    frame["_scope_batch_id"] = batch_id
                    frame["_payload_sha256"] = payload_sha256
                    frame["_parser_version"] = PARSER_VERSION
                    frame["_entity_type"] = table.removeprefix("whoscored_")
                    for column in frame.columns:
                        if (
                            frame[column]
                            .map(lambda value: isinstance(value, (dict, list)))
                            .any()
                        ):
                            frame[column] = frame[column].map(
                                lambda value: (
                                    self._canonical_json(value)
                                    if isinstance(value, (dict, list))
                                    else value
                                )
                            )
                    normalized = self._normalise_frame_types(frame, table=table)
                    del frame
                    self.writer.write_dataframe(
                        normalized,
                        database=self.schema,
                        table=table,
                        partition_spec=[
                            ("league", "identity"),
                            ("season", "identity"),
                        ],
                        source="whoscored",
                        bulk_arrow=(table == "whoscored_player_stage_stats"),
                    )

                written = 0
                for source in source_rows:
                    row = dict(source)
                    row["league"] = league
                    row["season"] = season
                    pending.append(row)
                    if len(pending) >= chunk_rows:
                        written += len(pending)
                        write_pending()
                if pending:
                    written += len(pending)
                    write_pending()
                if written != counts[table]:
                    raise BatchConflict(
                        f"scope batch {batch_id}/{table} iterator changed "
                        f"cardinality: wrote={written}, expected={counts[table]}"
                    )
                physical = self._scope_batch_count(
                    table, league=league, season=season, batch_id=batch_id
                )
                if physical != counts[table]:
                    raise BatchConflict(
                        f"scope batch {batch_id}/{table}: wrote={physical}, "
                        f"expected={counts[table]}"
                    )

        now = _utc_now()
        self.writer.write_dataframe(
            pd.DataFrame(
                [
                    {
                        "league": league,
                        "season": season,
                        "entity_group": entity_group,
                        "batch_id": batch_id,
                        "payload_sha256": payload_sha256,
                        "raw_uris_json": self._canonical_json(sorted(set(raw_uris))),
                        "parser_version": PARSER_VERSION,
                        "state": "success",
                        "entity_counts_json": self._canonical_json(counts),
                        "dataset_states_json": self._canonical_json(dataset_states),
                        "schema_fingerprint": schema_fingerprint,
                        "started_at": now,
                        "completed_at": now,
                        "error": None,
                        "_entity_type": "scope_manifest",
                    }
                ]
            ),
            database=self.schema,
            table=SCOPE_MANIFEST_TABLE,
            partition_spec=[("league", "identity"), ("season", "identity")],
            source="whoscored",
        )
        return batch_id

    def list_preview_candidates(
        self,
        league: str,
        season: str,
        *,
        limit: Optional[int] = None,
        match_ids: Optional[Iterable[int]] = None,
        force_replay: bool = False,
    ) -> list[dict[str, Any]]:
        """Return only unseen, parser-stale, or due-for-retry preview pages."""
        if limit is not None and int(limit) < 0:
            raise ValueError("preview candidate limit must be non-negative")
        limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""
        ids = sorted({int(value) for value in (match_ids or ())})
        if force_replay and not ids:
            raise ValueError("preview force_replay requires explicit match_ids")
        id_filter = (
            " AND CAST(s.game_id AS BIGINT) IN ("
            + ",".join(str(value) for value in ids)
            + ")"
            if ids
            else ""
        )
        time_filter = (
            "TRUE"
            if force_replay
            else """s.date BETWEEN CAST(
                    CURRENT_TIMESTAMP - INTERVAL '48' HOUR AS TIMESTAMP
                  ) AND CAST(
                    CURRENT_TIMESTAMP + INTERVAL '3' HOUR AS TIMESTAMP
                  )"""
        )
        candidate_filter = (
            "TRUE"
            if force_replay
            else f"""
                    m.game_id IS NULL
                    OR (
                        m.state = 'retryable'
                        AND COALESCE(
                            m.retry_after, TIMESTAMP '1970-01-01 00:00:00'
                        ) <= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
                    )
                    OR (
                        m.state = 'success'
                        AND (
                            m.parser_version IS DISTINCT FROM
                                {_sql_string(PARSER_VERSION)}
                            OR (
                                m.parser_version = {_sql_string(PARSER_VERSION)}
                                AND s.date >= CAST(
                                    CURRENT_TIMESTAMP - INTERVAL '3' HOUR
                                    AS TIMESTAMP
                                )
                                AND COALESCE(
                                    m.fetched_at,
                                    TIMESTAMP '1970-01-01 00:00:00'
                                ) <= CAST(
                                CURRENT_TIMESTAMP - INTERVAL '{PREVIEW_REFRESH_HOURS}' HOUR
                                    AS TIMESTAMP
                                )
                            )
                        )
                    )
                    OR (
                        m.state = 'parse_failed'
                        AND m.parser_version IS DISTINCT FROM
                            {_sql_string(PARSER_VERSION)}
                    )
                    OR (
                        m.state = 'not_available'
                        AND m.availability_version IS DISTINCT FROM
                            {_sql_string(MATCH_AVAILABILITY_VERSION)}
                    )
        """
        )
        latest = f"{self.catalog}.{self.schema}.whoscored_preview_ingest_latest"
        rows = self.trino.execute_query(
            f"""
            WITH schedule AS (
                SELECT s.*, ROW_NUMBER() OVER (
                    PARTITION BY league, season, game_id
                    ORDER BY _ingested_at DESC
                ) rn
                FROM {self.catalog}.{self.schema}.whoscored_schedule_current s
                WHERE league = {_sql_string(league)}
                  AND season = {_sql_string(season)}
                  AND has_preview = TRUE
            )
            SELECT CAST(s.game_id AS BIGINT), s.game, s.date,
                   s.home_team, s.away_team,
                   CASE
                       WHEN m.state = 'retryable'
                       THEN COALESCE(m.attempt_no, 0) + 1
                       ELSE 1
                   END AS attempt_no,
                   CASE
                       WHEN m.state = 'success'
                        AND m.parser_version = {_sql_string(PARSER_VERSION)}
                        AND s.date >= CAST(
                            CURRENT_TIMESTAMP - INTERVAL '3' HOUR AS TIMESTAMP
                        )
                        AND COALESCE(
                            m.fetched_at, TIMESTAMP '1970-01-01 00:00:00'
                        ) <= CAST(
                            CURRENT_TIMESTAMP - INTERVAL '{PREVIEW_REFRESH_HOURS}' HOUR AS TIMESTAMP
                        )
                       THEN TRUE ELSE FALSE
                   END AS force_refresh
            FROM schedule s
            LEFT JOIN {latest} m
              ON m.league = s.league
             AND m.season = s.season
             AND m.game_id = CAST(s.game_id AS BIGINT)
            WHERE s.rn = 1
              AND s.game_id IS NOT NULL
              AND ({time_filter})
              AND ({candidate_filter})
              {id_filter}
            ORDER BY s.date, s.game_id
            {limit_sql}
            """
        )
        return [
            {
                "game_id": int(row[0]),
                "game": str(row[1]),
                "date": row[2],
                "home_team": row[3],
                "away_team": row[4],
                "attempt_no": int(row[5]),
                "force_refresh": bool(row[6]),
            }
            for row in rows
        ]

    def record_preview_failure(self, failure: PreviewFailure) -> Optional[str]:
        if failure.state not in {
            "retryable",
            "terminal",
            "parse_failed",
            "not_available",
        }:
            raise ValueError(f"unsupported preview failure state: {failure.state}")
        if failure.state == "retryable" and failure.retry_after is None:
            raise ValueError("retryable preview failure requires retry_after")
        if failure.state != "retryable" and failure.retry_after is not None:
            raise ValueError(f"{failure.state} preview failure cannot have retry_after")
        if int(failure.attempt_no) < 1:
            raise ValueError("preview failure attempt_no must be positive")
        if (
            failure.state == "not_available"
            and failure.raw_uri is None
            and failure.http_status not in {404, 410}
        ):
            raise ValueError(
                "not_available preview failure requires raw proof or HTTP 404/410"
            )
        outcome_batch_id = None
        if failure.state == "not_available":
            outcome_batch_id = deterministic_preview_not_available_batch_id(
                failure.game_id,
                league=failure.league,
                season=failure.season,
                failure_code=failure.failure_code,
                http_status=failure.http_status,
                payload_sha256=failure.payload_sha256,
                raw_uri=failure.raw_uri,
                parser_version=failure.parser_version,
                availability_version=failure.availability_version,
            )
        now = _utc_now()
        self.writer.write_dataframe(
            pd.DataFrame(
                [
                    {
                        "league": failure.league,
                        "season": failure.season,
                        "game_id": int(failure.game_id),
                        "game": failure.game,
                        "kickoff": failure.kickoff,
                        "batch_id": outcome_batch_id,
                        "payload_sha256": failure.payload_sha256,
                        "raw_uri": failure.raw_uri,
                        "parser_version": failure.parser_version,
                        "availability_version": failure.availability_version,
                        "state": failure.state,
                        "missing_players_count": None,
                        "entity_counts_json": None,
                        "dataset_statuses_json": None,
                        "schema_fingerprint": None,
                        "transport_mode": failure.transport_mode,
                        "proxy_mode": failure.proxy_mode,
                        "http_status": failure.http_status,
                        "failure_code": failure.failure_code,
                        "error": failure.error[:4000],
                        "attempt_no": int(failure.attempt_no),
                        "retry_after": failure.retry_after,
                        "fetched_at": now,
                        "completed_at": (None if failure.state == "retryable" else now),
                        "direct_bytes": int(failure.direct_bytes),
                        "paid_bytes": int(failure.paid_bytes),
                        "_entity_type": "preview_manifest",
                    }
                ]
            ),
            database=self.schema,
            table=PREVIEW_MANIFEST_TABLE,
            partition_spec=[("league", "identity"), ("season", "identity")],
            source="whoscored",
        )
        return outcome_batch_id

    def _prepare_preview_commit(
        self, commit: PreviewCommit
    ) -> tuple[dict[str, Sequence[Mapping[str, Any]]], dict[str, int], str]:
        if not commit.payload_sha256:
            raise ValueError("payload_sha256 is required for a preview commit")
        if not commit.raw_uri:
            raise ValueError("raw_uri is required for a preview commit")
        if int(commit.attempt_no) < 1:
            raise ValueError("preview commit attempt_no must be positive")
        identities: set[tuple[str, int]] = set()
        for row in commit.missing_players:
            for column, expected in (
                ("league", commit.league),
                ("season", commit.season),
                ("game_id", int(commit.game_id)),
            ):
                value = row.get(column)
                if value is not None and str(value) != str(expected):
                    raise ValueError(
                        f"preview rows contain {column}={value!r} outside {expected!r}"
                    )
            player_id = row.get("player_id")
            if player_id is None:
                raise ValueError("preview row has null player_id")
            identity = (str(row.get("team") or ""), int(player_id))
            if identity in identities:
                raise ValueError(
                    f"preview rows contain duplicate team/player identity {identity!r}"
                )
            identities.add(identity)

        datasets: dict[str, Sequence[Mapping[str, Any]]] = {
            "missing_players": commit.missing_players,
        }
        for name, rows in commit.datasets.items():
            if name not in PREVIEW_DATASET_TABLES:
                raise ValueError(f"unsupported preview dataset {name!r}")
            if name == "missing_players":
                if self._canonical_json(list(rows)) != self._canonical_json(
                    list(commit.missing_players)
                ):
                    raise ValueError("duplicate missing_players datasets disagree")
                continue
            datasets[name] = rows
        statuses = {
            str(name): str(value) for name, value in commit.dataset_statuses.items()
        }
        if set(statuses) != set(datasets):
            raise ValueError(
                f"preview {commit.game_id} dataset statuses must cover exactly "
                f"{sorted(datasets)}; got {sorted(statuses)}"
            )
        for name, rows in datasets.items():
            status = statuses[name]
            if status not in {"available", "empty", "not_available"}:
                raise ValueError(
                    f"preview {commit.game_id}/{name} has invalid status {status!r}"
                )
            if status == "not_available":
                raise ValueError(
                    f"preview {commit.game_id}/{name} is not available; refusing "
                    "to hide the previously published snapshot"
                )
            if status == "available" and not rows:
                raise ValueError(
                    f"preview {commit.game_id}/{name} is available but has no rows"
                )
            if status == "empty" and rows:
                raise ValueError(
                    f"preview {commit.game_id}/{name} is empty but has rows"
                )
        counts = {name: len(rows) for name, rows in datasets.items()}
        schema_fingerprint = (
            commit.schema_fingerprint
            or hashlib.sha256(
                self._canonical_json(
                    {
                        name: sorted({str(key) for row in rows for key in row.keys()})
                        for name, rows in datasets.items()
                    }
                ).encode("utf-8")
            ).hexdigest()
        )
        return datasets, counts, schema_fingerprint

    def validate_preview_commit(self, commit: PreviewCommit) -> None:
        """Reject an incomplete preview before it enters a multi-game batch."""

        self._prepare_preview_commit(commit)

    def _preview_physical_counts(
        self, table: str, batch_ids: Sequence[str]
    ) -> dict[str, int]:
        if not batch_ids or not self.trino.table_exists(self.schema, table):
            return {}
        values = ",".join(_sql_string(value) for value in batch_ids)
        rows = self.trino.execute_query(
            f"SELECT _preview_batch_id, COUNT(*) FROM "
            f"{self.catalog}.{self.schema}.{table} "
            f"WHERE _preview_batch_id IN ({values}) GROUP BY _preview_batch_id"
        )
        return {str(row[0]): int(row[1]) for row in rows}

    @_lock_commit_sequence
    def commit_previews(self, commits: Sequence[PreviewCommit]) -> tuple[str, ...]:
        """Batch physical preview tables and atomically publish each game.

        Each dataset is appended at most once for the whole chunk.  The one
        manifest frame is written only after every per-game physical count has
        been verified, so complete orphan batches are resumable and partial
        batches fail closed.  A successful zero-row snapshot is represented by
        its manifest and hides an older non-empty snapshot without a DELETE.
        """
        ordered = tuple(commits)
        if not ordered:
            return ()
        seen: set[tuple[str, str, int]] = set()
        prepared: dict[
            str,
            tuple[
                PreviewCommit,
                dict[str, Sequence[Mapping[str, Any]]],
                dict[str, int],
                str,
            ],
        ] = {}
        for commit in ordered:
            identity = (commit.league, commit.season, int(commit.game_id))
            if identity in seen:
                raise ValueError(f"duplicate preview commit {identity}")
            seen.add(identity)
            datasets, counts, fingerprint = self._prepare_preview_commit(commit)
            if commit.batch_id in prepared:
                raise BatchConflict(f"preview batch id collision {commit.batch_id}")
            prepared[commit.batch_id] = (commit, datasets, counts, fingerprint)

        batch_ids = list(prepared)
        quoted = ",".join(_sql_string(value) for value in batch_ids)
        manifest_rows = self.trino.execute_query(
            "SELECT batch_id, missing_players_count, entity_counts_json, "
            f"COUNT(*) FROM {self._preview_manifest} "
            f"WHERE batch_id IN ({quoted}) AND state = 'success' "
            "GROUP BY batch_id, missing_players_count, entity_counts_json"
        )
        manifests: dict[str, dict[str, int]] = {}
        for row in manifest_rows:
            batch_id = str(row[0])
            counts = (
                json.loads(str(row[2])) if row[2] else {"missing_players": int(row[1])}
            )
            if batch_id in manifests and manifests[batch_id] != counts:
                raise BatchConflict(f"conflicting preview manifest {batch_id}")
            manifests[batch_id] = counts

        names = sorted(
            {name for _, datasets, _, _ in prepared.values() for name in datasets}
        )
        physical = {
            name: self._preview_physical_counts(PREVIEW_DATASET_TABLES[name], batch_ids)
            for name in names
        }
        frames: dict[str, list[dict[str, Any]]] = {name: [] for name in names}
        for batch_id, (commit, datasets, counts, fingerprint) in prepared.items():
            if batch_id in manifests and manifests[batch_id] != counts:
                raise BatchConflict(
                    f"preview manifest {batch_id}: stored={manifests[batch_id]}, "
                    f"parser={counts}"
                )
            for name, rows in datasets.items():
                existing = physical[name].get(batch_id, 0)
                expected = counts[name]
                if batch_id in manifests and existing != expected:
                    raise BatchConflict(
                        f"committed preview {batch_id}/{name}: "
                        f"manifest={expected}, physical={existing}, parser={expected}"
                    )
                if existing == expected:
                    continue
                if existing:
                    raise BatchConflict(
                        f"preview {batch_id}/{name} has partial physical batch "
                        f"{existing}/{expected}"
                    )
                for source in rows:
                    row = dict(source)
                    row.update(
                        {
                            "league": commit.league,
                            "season": commit.season,
                            "game": commit.game,
                            "game_id": int(commit.game_id),
                            "_preview_batch_id": batch_id,
                            "_payload_sha256": commit.payload_sha256,
                            "_parser_version": commit.parser_version,
                            "_entity_type": name,
                            "batch_schema_fingerprint": fingerprint,
                        }
                    )
                    for key, value in tuple(row.items()):
                        if isinstance(value, (dict, list)):
                            row[key] = self._canonical_json(value)
                    frames[name].append(row)

        for name, rows in frames.items():
            if not rows:
                continue
            self.writer.write_dataframe(
                self._normalise_frame_types(
                    pd.DataFrame(rows), table=PREVIEW_DATASET_TABLES[name]
                ),
                database=self.schema,
                table=PREVIEW_DATASET_TABLES[name],
                partition_spec=[("league", "identity"), ("season", "identity")],
                source="whoscored",
            )

        verified = {
            name: self._preview_physical_counts(PREVIEW_DATASET_TABLES[name], batch_ids)
            for name in names
        }
        now = _utc_now()
        # The same raw payload may be revisited intentionally after the
        # bounded preview refresh cadence.  Advance the success observation in
        # that case (or after a newer failure), but keep an immediate Airflow
        # retry idempotent instead of appending duplicate success manifests.
        historical_batches = {
            batch_id for batch_id in prepared if batch_id in manifests
        }
        latest_by_game: dict[tuple[str, str, int], Sequence[Any]] = {}
        if historical_batches:
            wanted = " OR ".join(
                "(league = "
                f"{_sql_string(commit.league)} AND season = "
                f"{_sql_string(commit.season)} AND game_id = {int(commit.game_id)})"
                for batch_id, (
                    commit,
                    _datasets,
                    _counts,
                    _fingerprint,
                ) in prepared.items()
                if batch_id in historical_batches
            )
            latest_rows = self.trino.execute_query(
                "SELECT league, season, game_id, batch_id, state, fetched_at FROM ("
                "SELECT m.*, ROW_NUMBER() OVER ("
                "PARTITION BY league, season, game_id "
                "ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC, "
                "COALESCE(batch_id, '') DESC, _batch_id DESC) AS rn "
                f"FROM {self._preview_manifest} m WHERE {wanted}"
                ") WHERE rn = 1"
            )
            latest_by_game = {
                (str(row[0]), str(row[1]), int(row[2])): row for row in latest_rows
            }
        refresh_cutoff = now - timedelta(hours=PREVIEW_REFRESH_HOURS)
        manifest_frame: list[dict[str, Any]] = []
        for batch_id, (commit, _datasets, counts, fingerprint) in prepared.items():
            for name, expected in counts.items():
                actual = verified[name].get(batch_id, 0)
                if actual != expected:
                    raise BatchConflict(
                        f"preview {batch_id}/{name}: "
                        f"physical={actual}, expected={expected}"
                    )
            latest = latest_by_game.get(
                (commit.league, commit.season, int(commit.game_id))
            )
            if latest is not None:
                latest_fetched_at = latest[5]
                if isinstance(latest_fetched_at, datetime):
                    if latest_fetched_at.tzinfo is not None:
                        latest_fetched_at = latest_fetched_at.astimezone(
                            timezone.utc
                        ).replace(tzinfo=None)
                else:
                    latest_fetched_at = None
                already_fresh = (
                    str(latest[4]) == "success"
                    and str(latest[3] or "") == batch_id
                    and latest_fetched_at is not None
                    and latest_fetched_at > refresh_cutoff
                )
                if already_fresh:
                    continue
            manifest_frame.append(
                {
                    "league": commit.league,
                    "season": commit.season,
                    "game_id": int(commit.game_id),
                    "game": commit.game,
                    "kickoff": commit.kickoff,
                    "batch_id": commit.batch_id,
                    "payload_sha256": commit.payload_sha256,
                    "raw_uri": commit.raw_uri,
                    "parser_version": commit.parser_version,
                    "availability_version": commit.availability_version,
                    "state": "success",
                    "missing_players_count": counts["missing_players"],
                    "entity_counts_json": self._canonical_json(counts),
                    "dataset_statuses_json": self._canonical_json(
                        commit.dataset_statuses
                    ),
                    "schema_fingerprint": fingerprint,
                    "transport_mode": commit.transport_mode,
                    "proxy_mode": commit.proxy_mode,
                    "http_status": int(commit.http_status),
                    "failure_code": None,
                    "error": None,
                    "attempt_no": int(commit.attempt_no),
                    "retry_after": None,
                    "fetched_at": commit.fetched_at or now,
                    "completed_at": now,
                    "direct_bytes": int(commit.direct_bytes),
                    "paid_bytes": int(commit.paid_bytes),
                    "_entity_type": "preview_manifest",
                }
            )
        if manifest_frame:
            self.writer.write_dataframe(
                pd.DataFrame(manifest_frame),
                database=self.schema,
                table=PREVIEW_MANIFEST_TABLE,
                partition_spec=[("league", "identity"), ("season", "identity")],
                source="whoscored",
            )
        return tuple(commit.batch_id for commit in ordered)

    def _prepare_match_commit(
        self, commit: MatchCommit
    ) -> tuple[dict[str, Sequence[Mapping[str, Any]]], dict[str, int], str]:
        if int(commit.attempt_no) < 1:
            raise ValueError("match commit attempt_no must be positive")
        event_ids = [row.get("source_event_id") for row in commit.events]
        if any(value is None for value in event_ids):
            raise ValueError(f"game {commit.game_id} has null source_event_id")
        if any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
            or value > 9_223_372_036_854_775_807
            for value in event_ids
        ):
            raise ValueError(
                f"game {commit.game_id} has invalid global source_event_id"
            )
        if len(event_ids) != len(set(event_ids)):
            raise ValueError(f"game {commit.game_id} has duplicate source_event_id")
        team_event_ids = [row.get("team_event_id") for row in commit.events]
        if any(value is None for value in team_event_ids):
            raise ValueError(f"game {commit.game_id} has null team_event_id")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in team_event_ids
        ):
            raise ValueError(f"game {commit.game_id} has invalid team_event_id")
        team_event_keys = [
            (row.get("team_id"), row.get("team_event_id")) for row in commit.events
        ]
        if len(team_event_keys) != len(set(team_event_keys)):
            raise ValueError(
                f"game {commit.game_id} has duplicate team-local event identity"
            )
        if bool(commit.lineups) != bool(commit.lineups_available):
            raise ValueError(
                f"game {commit.game_id} lineups_available disagrees with lineup rows"
            )

        completed = commit.schedule_status == 6 or (
            commit.schedule_status == 1
            and commit.kickoff is not None
            and commit.kickoff <= _utc_now() - MATCH_COMPLETION_GRACE
        )
        if completed and commit.is_opta is True:
            status = str(commit.dataset_statuses.get("events") or "")
            match_rows = tuple(commit.datasets.get("matches", ()))
            declared_duration = 90
            if match_rows and match_rows[0].get("expanded_max_minute") is not None:
                declared_duration = max(1, int(match_rows[0]["expanded_max_minute"]))
            event_minutes = [
                row.get("expanded_minute", row.get("minute"))
                for row in commit.events
                if row.get("expanded_minute", row.get("minute")) is not None
            ]
            max_minute = max((int(value) for value in event_minutes), default=-1)
            # Opta feeds contain on-ball actions, not merely goals/cards.  A
            # final match with a tiny prefix or no late-match event is a known
            # WhoScored partial-response failure (#915), never a valid empty.
            minimum_rows = max(20, declared_duration)
            minimum_max_minute = max(1, declared_duration - 15)
            if (
                status != "available"
                or len(commit.events) < minimum_rows
                or max_minute < minimum_max_minute
            ):
                raise ValueError(
                    f"game {commit.game_id} has incomplete final Opta events: "
                    f"status={status or 'missing'}, rows={len(commit.events)}, "
                    f"max_minute={max_minute}, declared_duration={declared_duration}"
                )

        datasets: dict[str, Sequence[Mapping[str, Any]]] = {
            "events": commit.events,
            "lineups": commit.lineups,
        }
        for name, rows in commit.datasets.items():
            if name not in MATCH_DATASET_TABLES:
                raise ValueError(f"unsupported match dataset {name!r}")
            if name in {"events", "lineups"}:
                expected_rows = datasets[name]
                if self._canonical_json(list(rows)) != self._canonical_json(
                    list(expected_rows)
                ):
                    raise ValueError(f"duplicate {name} datasets disagree")
                continue
            datasets[name] = rows
        if "matches" in datasets and len(datasets["matches"]) != 1:
            raise ValueError(
                f"game {commit.game_id} must have exactly one match metadata row"
            )
        statuses = {
            str(name): str(value) for name, value in commit.dataset_statuses.items()
        }
        if set(statuses) != set(datasets):
            raise ValueError(
                f"game {commit.game_id} dataset statuses must cover exactly "
                f"{sorted(datasets)}; got {sorted(statuses)}"
            )
        for name, rows in datasets.items():
            status = statuses[name]
            if status not in {"available", "empty", "not_available"}:
                raise ValueError(
                    f"game {commit.game_id}/{name} has invalid status {status!r}"
                )
            if status == "available" and not rows:
                raise ValueError(
                    f"game {commit.game_id}/{name} is available but has no rows"
                )
            if status in {"empty", "not_available"} and rows:
                raise ValueError(
                    f"game {commit.game_id}/{name} has status {status} but has rows"
                )
        counts = {name: len(rows) for name, rows in datasets.items()}
        schema_fingerprint = (
            commit.schema_fingerprint
            or hashlib.sha256(
                self._canonical_json(
                    {
                        name: sorted({str(key) for row in rows for key in row.keys()})
                        for name, rows in datasets.items()
                    }
                ).encode("utf-8")
            ).hexdigest()
        )
        return datasets, counts, schema_fingerprint

    def validate_match_commit(self, commit: MatchCommit) -> None:
        """Run per-game completeness checks before adding it to a write batch."""

        self._prepare_match_commit(commit)

    @staticmethod
    def _is_completed_match(commit: MatchCommit) -> bool:
        return commit.schedule_status == 6 or (
            commit.schedule_status == 1
            and commit.kickoff is not None
            and commit.kickoff <= _utc_now() - MATCH_COMPLETION_GRACE
        )

    def _current_dataset_counts(
        self, commits: Sequence[MatchCommit]
    ) -> dict[tuple[str, str, int], dict[str, int]]:
        """Read prior committed counts without scanning physical history."""

        guarded = [commit for commit in commits if self._is_completed_match(commit)]
        if not guarded:
            return {}
        values = ",".join(
            "("
            + ",".join(
                (
                    _sql_string(commit.league),
                    _sql_string(commit.season),
                    str(int(commit.game_id)),
                )
            )
            + ")"
            for commit in guarded
        )
        rows = self.trino.execute_query(
            "WITH wanted(league, season, game_id) AS (VALUES "
            f"{values}) SELECT m.league, m.season, m.game_id, "
            "m.entity_counts_json, m.events_count, m.lineups_count "
            "FROM wanted w JOIN "
            f"{self.catalog}.{self.schema}.whoscored_match_ingest_latest_success m "
            "ON m.league = w.league AND m.season = w.season "
            "AND m.game_id = w.game_id"
        )
        result: dict[tuple[str, str, int], dict[str, int]] = {}
        for league, season, game_id, payload, events_count, lineups_count in rows:
            try:
                counts = json.loads(str(payload)) if payload else {}
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise BatchConflict(
                    f"game {game_id} has invalid published entity_counts_json"
                ) from exc
            if not isinstance(counts, dict):
                raise BatchConflict(
                    f"game {game_id} has non-object published entity counts"
                )
            counts.setdefault("events", int(events_count or 0))
            counts.setdefault("lineups", int(lineups_count or 0))
            result[(str(league), str(season), int(game_id))] = {
                str(name): int(value) for name, value in counts.items()
            }
        return result

    def _match_physical_counts(
        self, table: str, batch_ids: Sequence[str]
    ) -> dict[str, int]:
        if not batch_ids or not self.trino.table_exists(self.schema, table):
            return {}
        values = ",".join(_sql_string(value) for value in batch_ids)
        rows = self.trino.execute_query(
            f"SELECT _game_batch_id, COUNT(*) FROM "
            f"{self.catalog}.{self.schema}.{table} "
            f"WHERE _game_batch_id IN ({values}) GROUP BY _game_batch_id"
        )
        return {str(row[0]): int(row[1]) for row in rows}

    @_lock_commit_sequence
    def commit_matches(self, commits: Sequence[MatchCommit]) -> tuple[str, ...]:
        """Batch physical tables and manifests while retaining per-game commits."""
        ordered = tuple(commits)
        if not ordered:
            return ()
        seen: set[tuple[str, str, int]] = set()
        prepared: dict[
            str,
            tuple[
                MatchCommit,
                dict[str, Sequence[Mapping[str, Any]]],
                dict[str, int],
                str,
            ],
        ] = {}
        for commit in ordered:
            identity = (commit.league, commit.season, int(commit.game_id))
            if identity in seen:
                raise ValueError(f"duplicate match commit {identity}")
            seen.add(identity)
            datasets, counts, fingerprint = self._prepare_match_commit(commit)
            if commit.batch_id in prepared:
                raise BatchConflict(f"match batch id collision {commit.batch_id}")
            prepared[commit.batch_id] = (commit, datasets, counts, fingerprint)

        prior_dataset_counts = self._current_dataset_counts(ordered)
        for commit, _datasets, counts, _fingerprint in prepared.values():
            previous = prior_dataset_counts.get(
                (commit.league, commit.season, int(commit.game_id)), {}
            )
            for name, published_count in previous.items():
                current_count = int(counts.get(name, 0))
                if published_count and current_count < published_count:
                    raise BatchConflict(
                        f"game {commit.game_id}/{name} completeness regression: "
                        f"new={current_count}, published={published_count}"
                    )

        batch_ids = list(prepared)
        quoted = ",".join(_sql_string(value) for value in batch_ids)
        manifest_rows = self.trino.execute_query(
            f"SELECT batch_id, events_count, lineups_count, entity_counts_json, "
            "COUNT(*) FROM "
            f"{self._manifest} WHERE batch_id IN ({quoted}) AND state = 'success' "
            "GROUP BY batch_id, events_count, lineups_count, entity_counts_json"
        )
        manifests: dict[str, dict[str, int]] = {}
        for row in manifest_rows:
            batch_id = str(row[0])
            counts = (
                json.loads(str(row[3]))
                if row[3]
                else {"events": int(row[1]), "lineups": int(row[2])}
            )
            if batch_id in manifests and manifests[batch_id] != counts:
                raise BatchConflict(f"conflicting match manifest {batch_id}")
            # Multiple identical success rows are legitimate refresh
            # heartbeats for one content-addressed physical batch.
            manifests[batch_id] = counts

        names = sorted(
            {name for _, datasets, _, _ in prepared.values() for name in datasets}
        )
        physical = {
            name: self._match_physical_counts(MATCH_DATASET_TABLES[name], batch_ids)
            for name in names
        }
        frames: dict[str, list[dict[str, Any]]] = {name: [] for name in names}
        for batch_id, (commit, datasets, counts, fingerprint) in prepared.items():
            if batch_id in manifests and manifests[batch_id] != counts:
                raise BatchConflict(
                    f"match manifest {batch_id}: stored={manifests[batch_id]}, "
                    f"parser={counts}"
                )
            for name, rows in datasets.items():
                existing = physical[name].get(batch_id, 0)
                expected = counts[name]
                if batch_id in manifests and existing != expected:
                    raise BatchConflict(
                        f"committed match {batch_id}/{name}: "
                        f"manifest={expected}, physical={existing}, parser={expected}"
                    )
                if existing == expected:
                    continue
                if existing:
                    raise BatchConflict(
                        f"match {batch_id}/{name} has partial physical batch "
                        f"{existing}/{expected}"
                    )
                for source in rows:
                    row = dict(source)
                    row.update(
                        {
                            "league": commit.league,
                            "season": commit.season,
                            "game": commit.game,
                            "game_id": int(commit.game_id),
                            "_game_batch_id": batch_id,
                            "_payload_sha256": commit.payload_sha256,
                            "_parser_version": commit.parser_version,
                            "_entity_type": name,
                            "batch_schema_fingerprint": fingerprint,
                        }
                    )
                    for key, value in tuple(row.items()):
                        if isinstance(value, (dict, list)):
                            row[key] = self._canonical_json(value)
                    frames[name].append(row)

        for name, rows in frames.items():
            if not rows:
                continue
            self.writer.write_dataframe(
                self._normalise_frame_types(
                    pd.DataFrame(rows), table=MATCH_DATASET_TABLES[name]
                ),
                database=self.schema,
                table=MATCH_DATASET_TABLES[name],
                partition_spec=[("league", "identity"), ("season", "identity")],
                source="whoscored",
            )

        verified = {
            name: self._match_physical_counts(MATCH_DATASET_TABLES[name], batch_ids)
            for name in names
        }
        new_manifests: list[dict[str, Any]] = []
        now = _utc_now()
        wanted = " OR ".join(
            "(league = "
            f"{_sql_string(commit.league)} AND season = "
            f"{_sql_string(commit.season)} AND game_id = {int(commit.game_id)})"
            for commit in ordered
        )
        latest_rows = self.trino.execute_query(
            "SELECT league, season, game_id, batch_id, state, fetched_at FROM ("
            "SELECT m.*, ROW_NUMBER() OVER ("
            "PARTITION BY league, season, game_id "
            "ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC, "
            "batch_id DESC, _batch_id DESC) AS rn "
            f"FROM {self._manifest} m WHERE {wanted}"
            ") WHERE rn = 1"
        )
        latest_by_game = {
            (str(row[0]), str(row[1]), int(row[2])): row for row in latest_rows
        }
        refresh_cutoff = now - timedelta(days=MATCH_REFRESH_DAYS)
        for batch_id, (commit, _datasets, counts, fingerprint) in prepared.items():
            for name, expected in counts.items():
                actual = verified[name].get(batch_id, 0)
                if actual != expected:
                    raise BatchConflict(
                        f"match {batch_id}/{name}: physical={actual}, expected={expected}"
                    )
            latest = latest_by_game.get(
                (commit.league, commit.season, int(commit.game_id))
            )
            if latest is not None:
                latest_fetched_at = latest[5]
                if isinstance(latest_fetched_at, datetime):
                    if latest_fetched_at.tzinfo is not None:
                        latest_fetched_at = latest_fetched_at.astimezone(
                            timezone.utc
                        ).replace(tzinfo=None)
                else:
                    latest_fetched_at = None
                already_fresh = (
                    str(latest[4]) == "success"
                    and str(latest[3] or "") == batch_id
                    and latest_fetched_at is not None
                    and latest_fetched_at > refresh_cutoff
                )
                if already_fresh:
                    continue
            new_manifests.append(
                {
                    "league": commit.league,
                    "season": commit.season,
                    "game_id": int(commit.game_id),
                    "game": commit.game,
                    "kickoff": commit.kickoff,
                    "batch_id": batch_id,
                    "payload_sha256": commit.payload_sha256,
                    "raw_uri": commit.raw_uri,
                    "parser_version": commit.parser_version,
                    "availability_version": commit.availability_version,
                    "state": "success",
                    "is_final": self._is_completed_match(commit),
                    "is_opta": commit.is_opta,
                    "events_count": len(commit.events),
                    "lineups_count": len(commit.lineups),
                    "lineups_available": bool(commit.lineups_available),
                    "entity_counts_json": self._canonical_json(counts),
                    "dataset_statuses_json": self._canonical_json(
                        commit.dataset_statuses
                    ),
                    "schema_fingerprint": fingerprint,
                    "transport_mode": commit.transport_mode,
                    "proxy_mode": commit.proxy_mode,
                    "http_status": int(commit.http_status),
                    "failure_code": None,
                    "error": None,
                    "attempt_no": int(commit.attempt_no),
                    "retry_after": None,
                    "fetched_at": commit.fetched_at or now,
                    "completed_at": now,
                    "direct_bytes": int(commit.direct_bytes),
                    "paid_bytes": int(commit.paid_bytes),
                    "_entity_type": "match_manifest",
                }
            )
        if new_manifests:
            self.writer.write_dataframe(
                pd.DataFrame(new_manifests),
                database=self.schema,
                table=MATCH_MANIFEST_TABLE,
                partition_spec=[("league", "identity"), ("season", "identity")],
                source="whoscored",
            )
        return tuple(commit.batch_id for commit in ordered)

    def record_failure(self, failure: ManifestFailure) -> Optional[str]:
        if failure.state not in {
            "retryable",
            "terminal",
            "parse_failed",
            "not_available",
        }:
            raise ValueError(f"unsupported manifest failure state: {failure.state}")
        if failure.state == "retryable" and failure.retry_after is None:
            raise ValueError("retryable match failure requires retry_after")
        if failure.state != "retryable" and failure.retry_after is not None:
            raise ValueError(f"{failure.state} match failure cannot have retry_after")
        if int(failure.attempt_no) < 1:
            raise ValueError("match failure attempt_no must be positive")
        if (
            failure.state == "not_available"
            and failure.raw_uri is None
            and failure.http_status not in {404, 410}
        ):
            raise ValueError(
                "not_available match failure requires raw proof or HTTP 404/410"
            )
        outcome_batch_id = None
        if failure.state == "not_available":
            outcome_batch_id = deterministic_match_not_available_batch_id(
                failure.game_id,
                league=failure.league,
                season=failure.season,
                failure_code=failure.failure_code,
                http_status=failure.http_status,
                payload_sha256=failure.payload_sha256,
                raw_uri=failure.raw_uri,
                parser_version=failure.parser_version,
                availability_version=failure.availability_version,
            )
        now = _utc_now()
        row = asdict(failure)
        row.update(
            {
                "game": failure.game,
                "kickoff": failure.kickoff,
                "batch_id": outcome_batch_id,
                "payload_sha256": failure.payload_sha256,
                "raw_uri": failure.raw_uri,
                "parser_version": failure.parser_version,
                "availability_version": failure.availability_version,
                "is_final": failure.state != "retryable",
                "is_opta": failure.is_opta,
                "events_count": 0,
                "lineups_count": 0,
                "lineups_available": False,
                "fetched_at": now,
                "completed_at": now if failure.state != "retryable" else None,
                "_entity_type": "match_manifest",
            }
        )
        self.writer.write_dataframe(
            pd.DataFrame([row]),
            database=self.schema,
            table=MATCH_MANIFEST_TABLE,
            partition_spec=[("league", "identity"), ("season", "identity")],
            source="whoscored",
        )
        return outcome_batch_id
