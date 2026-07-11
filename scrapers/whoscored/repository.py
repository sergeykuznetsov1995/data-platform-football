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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoTableManager
from scrapers.whoscored.domain import WhoScoredScope
from scrapers.whoscored.parsers import PARSER_VERSION


MATCH_MANIFEST_TABLE = "whoscored_match_ingest_manifest"
PREVIEW_MANIFEST_TABLE = "whoscored_preview_ingest_manifest"
PROFILE_VERSIONS_TABLE = "whoscored_player_profile_versions"
PROFILE_MANIFEST_TABLE = "whoscored_profile_ingest_manifest"


class BatchConflict(RuntimeError):
    """An existing physical batch disagrees with the parsed row counts."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def deterministic_game_batch_id(
    game_id: int, payload_sha256: str, parser_version: str = PARSER_VERSION
) -> str:
    value = f"{int(game_id)}\0{payload_sha256}\0{parser_version}".encode("utf-8")
    return "ws2-" + hashlib.sha256(value).hexdigest()


def deterministic_preview_batch_id(
    game_id: int, payload_sha256: str, parser_version: str = PARSER_VERSION
) -> str:
    value = f"preview\0{int(game_id)}\0{payload_sha256}\0{parser_version}".encode(
        "utf-8"
    )
    return "wsp2-" + hashlib.sha256(value).hexdigest()


@dataclass(frozen=True)
class MatchCandidate:
    game_id: int
    league: str
    season: str
    game: str
    kickoff: Optional[datetime]
    status: int
    match_is_opta: bool


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
    kickoff: Optional[datetime] = None
    fetched_at: Optional[datetime] = None

    @property
    def batch_id(self) -> str:
        return deterministic_game_batch_id(
            self.game_id, self.payload_sha256, self.parser_version
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
    transport_mode: str = "none"
    proxy_mode: str = "none"
    http_status: Optional[int] = None
    direct_bytes: int = 0
    paid_bytes: int = 0
    payload_sha256: Optional[str] = None
    raw_uri: Optional[str] = None
    parser_version: str = PARSER_VERSION


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
    kickoff: Optional[datetime] = None
    fetched_at: Optional[datetime] = None
    attempt_no: int = 1

    @property
    def batch_id(self) -> str:
        return deterministic_preview_batch_id(
            self.game_id, self.payload_sha256, self.parser_version
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

    def ensure_schema(self, *, create_views: bool = True) -> None:
        """Create additive V2 storage and, unless deferred, strict views."""
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
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            ) WITH (partitioning = ARRAY['league', 'season'])
            """
        )
        for table, columns in {
            "whoscored_events": {
                "source_event_id": "BIGINT",
                "_game_batch_id": "VARCHAR",
                "_payload_sha256": "VARCHAR",
                "_parser_version": "VARCHAR",
            },
            "whoscored_lineups": {
                "_game_batch_id": "VARCHAR",
                "_payload_sha256": "VARCHAR",
                "_parser_version": "VARCHAR",
            },
        }.items():
            if not self.trino.table_exists(self.schema, table):
                continue
            existing = {
                name.lower()
                for name in self.trino.get_table_columns(self.schema, table)
            }
            for name, data_type in columns.items():
                if name.lower() not in existing:
                    self.trino.add_column(self.schema, table, name, data_type)

        self._ensure_preview_schema()
        if create_views:
            self._create_current_views()
        self._ensure_profile_schema(create_views=create_views)

    def _ensure_preview_schema(self) -> None:
        """Create the append-only preview payload and its logical commit log."""
        physical = f"{self.catalog}.{self.schema}.whoscored_missing_players"
        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {physical} (
                league VARCHAR,
                season VARCHAR,
                game VARCHAR,
                game_id BIGINT,
                team VARCHAR,
                player VARCHAR,
                player_id BIGINT,
                reason VARCHAR,
                status VARCHAR,
                _preview_batch_id VARCHAR,
                _payload_sha256 VARCHAR,
                _parser_version VARCHAR,
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            ) WITH (partitioning = ARRAY['league', 'season'])
            """
        )
        existing = {
            name.lower()
            for name in self.trino.get_table_columns(
                self.schema, "whoscored_missing_players"
            )
        }
        for name, data_type in {
            "_preview_batch_id": "VARCHAR",
            "_payload_sha256": "VARCHAR",
            "_parser_version": "VARCHAR",
        }.items():
            if name.lower() not in existing:
                self.trino.add_column(
                    self.schema, "whoscored_missing_players", name, data_type
                )

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
                state VARCHAR,
                missing_players_count BIGINT,
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

    def _ensure_profile_schema(self, *, create_views: bool = True) -> None:
        versions = f"{self.catalog}.{self.schema}.{PROFILE_VERSIONS_TABLE}"
        manifest = f"{self.catalog}.{self.schema}.{PROFILE_MANIFEST_TABLE}"
        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {versions} (
                player_id BIGINT,
                name VARCHAR,
                current_team_id BIGINT,
                current_team_name VARCHAR,
                shirt_number INTEGER,
                age INTEGER,
                date_of_birth DATE,
                height_cm INTEGER,
                nationality VARCHAR,
                country_code VARCHAR,
                positions VARCHAR,
                payload_sha256 VARCHAR,
                raw_uri VARCHAR,
                parser_version VARCHAR,
                fetched_at TIMESTAMP(6),
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            ) WITH (partitioning = ARRAY['bucket(player_id, 32)'])
            """
        )
        self.trino._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {manifest} (
                player_id BIGINT,
                payload_sha256 VARCHAR,
                raw_uri VARCHAR,
                parser_version VARCHAR,
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
                _source VARCHAR,
                _entity_type VARCHAR,
                _ingested_at TIMESTAMP(6),
                _batch_id VARCHAR
            ) WITH (partitioning = ARRAY['bucket(player_id, 32)'])
            """
        )
        if not create_views:
            return
        current_lineups = f"{self.catalog}.{self.schema}.whoscored_lineups_current"
        roster = f"{self.catalog}.{self.schema}.whoscored_player_roster"
        if self.trino.table_exists(self.schema, "whoscored_lineups"):
            self.trino._execute(
                f"""
                CREATE OR REPLACE VIEW {roster} AS
                SELECT DISTINCT
                    CAST(game_id AS BIGINT) AS game_id,
                    CAST(player_id AS BIGINT) AS player_id,
                    league,
                    season,
                    team
                FROM {current_lineups}
                WHERE player_id IS NOT NULL
                """
            )
        self.trino._execute(
            f"""
            CREATE OR REPLACE VIEW {self.catalog}.silver.whoscored_player_profile_current AS
            SELECT player_id, name, current_team_id, current_team_name,
                   shirt_number, age, date_of_birth, height_cm, nationality,
                   country_code, positions, payload_sha256, raw_uri,
                   parser_version, fetched_at
            FROM (
                SELECT p.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY player_id
                           ORDER BY fetched_at DESC, _ingested_at DESC
                       ) AS rn
                FROM {versions} p
                WHERE EXISTS (
                    SELECT 1 FROM {manifest} m
                    WHERE m.player_id = p.player_id
                      AND (
                          m.payload_sha256 = p.payload_sha256
                          OR (
                              m.payload_sha256 IS NULL
                              AND p.payload_sha256 IS NULL
                          )
                      )
                      AND m.parser_version = p.parser_version
                      AND m.state = 'success'
                )
            ) WHERE rn = 1
            """
        )

    def list_profile_candidates(
        self,
        *,
        scopes: Sequence[WhoScoredScope],
        limit: int = 200,
    ) -> list[int]:
        """Return unseen or due-retry profiles from the selected rosters.

        Profile pages are global, but their candidate population is not: only
        players present in the explicitly selected competition-season scopes
        are eligible.  Successful and terminal manifests remain sticky across
        parser releases.  Re-fetching thousands of valid legacy profiles only
        to obtain a new raw/parser version would waste proxy traffic; refreshes
        must be introduced separately through an explicit TTL/force policy.
        """
        if int(limit) < 0:
            raise ValueError("profile candidate limit must be non-negative")
        selected = tuple(
            dict.fromkeys(
                (scope.competition_id, scope.season_id) for scope in scopes
            )
        )
        if not selected or int(limit) == 0:
            return []
        # Profiles can be run as an independent CLI subcommand, before the
        # match task has had a chance to initialize manifests/current views.
        self.ensure_schema()
        manifest = f"{self.catalog}.{self.schema}.{PROFILE_MANIFEST_TABLE}"
        roster = f"{self.catalog}.{self.schema}.whoscored_player_roster"
        scope_filter = " OR ".join(
            "(league = "
            f"{_sql_string(league)} AND season = {_sql_string(season)})"
            for league, season in selected
        )
        rows = self.trino.execute_query(
            f"""
            WITH latest AS (
                SELECT * FROM (
                    SELECT m.*, ROW_NUMBER() OVER (
                        PARTITION BY player_id
                        ORDER BY fetched_at DESC, _ingested_at DESC
                    ) AS rn
                    FROM {manifest} m
                ) WHERE rn = 1
            )
            SELECT r.player_id
            FROM (
                SELECT DISTINCT player_id
                FROM {roster}
                WHERE {scope_filter}
            ) r
            LEFT JOIN latest m ON m.player_id = r.player_id
            WHERE m.player_id IS NULL
               OR (
                    m.state = 'retryable'
                    AND COALESCE(m.retry_after, TIMESTAMP '1970-01-01 00:00:00')
                        <= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
               )
               OR (
                    m.state = 'parse_failed'
                    AND m.parser_version IS DISTINCT FROM
                        {_sql_string(PARSER_VERSION)}
               )
            ORDER BY CASE WHEN m.player_id IS NULL THEN 0 ELSE 1 END,
                     r.player_id
            LIMIT {int(limit)}
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
    ) -> None:
        """Persist a failed profile attempt in the current manifest schema."""
        if state not in {"retryable", "terminal", "parse_failed"}:
            raise ValueError(f"unsupported profile failure state: {state}")
        if state == "retryable" and retry_after is None:
            raise ValueError("retryable profile failure requires retry_after")
        if state != "retryable" and retry_after is not None:
            raise ValueError(f"{state} profile failure cannot have retry_after")
        if int(attempt_no) < 1:
            raise ValueError("profile failure attempt_no must be positive")

        now = _utc_now()
        row = {
            "player_id": int(player_id),
            "payload_sha256": payload_sha256,
            "raw_uri": raw_uri,
            "parser_version": PARSER_VERSION,
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
            "_entity_type": "profile_manifest",
        }
        self.writer.write_dataframe(
            pd.DataFrame([row]),
            database=self.schema,
            table=PROFILE_MANIFEST_TABLE,
            partition_spec=[("player_id", "bucket(32)")],
            source="whoscored",
        )

    def commit_profile(
        self,
        *,
        player_id: int,
        profile: Mapping[str, Any],
        payload_sha256: str,
        raw_uri: str,
        transport_mode: str,
        proxy_mode: str = "none",
        direct_bytes: int = 0,
        paid_bytes: int = 0,
    ) -> None:
        if not payload_sha256:
            raise ValueError("payload_sha256 is required for a profile commit")
        manifest = f"{self.catalog}.{self.schema}.{PROFILE_MANIFEST_TABLE}"
        now = _utc_now()
        row = dict(profile)
        row.update(
            {
                "player_id": int(player_id),
                "payload_sha256": payload_sha256,
                "raw_uri": raw_uri,
                "parser_version": PARSER_VERSION,
                "fetched_at": now,
                "_entity_type": "player_profile",
            }
        )
        versions = f"{self.catalog}.{self.schema}.{PROFILE_VERSIONS_TABLE}"
        physical = self.trino.execute_query(
            f"SELECT COUNT(*) FROM {versions} WHERE player_id = {int(player_id)} "
            f"AND payload_sha256 = {_sql_string(payload_sha256)} "
            f"AND parser_version = {_sql_string(PARSER_VERSION)}"
        )
        physical_count = int(physical[0][0]) if physical else 0
        if physical_count > 1:
            raise BatchConflict(
                f"profile {player_id}/{payload_sha256} has {physical_count} versions"
            )
        existing = self.trino.execute_query(
            f"SELECT COUNT(*) FROM {manifest} WHERE player_id = {int(player_id)} "
            f"AND payload_sha256 = {_sql_string(payload_sha256)} "
            f"AND parser_version = {_sql_string(PARSER_VERSION)} "
            "AND state = 'success'"
        )
        if existing and int(existing[0][0]) > 0:
            if physical_count != 1:
                raise BatchConflict(
                    f"profile {player_id}/{payload_sha256} is committed but has "
                    f"{physical_count} physical versions"
                )
            return
        if physical_count == 0:
            self.writer.write_dataframe(
                pd.DataFrame([row]),
                database=self.schema,
                table=PROFILE_VERSIONS_TABLE,
                partition_spec=[("player_id", "bucket(32)")],
                source="whoscored",
            )
        self.writer.write_dataframe(
            pd.DataFrame(
                [
                    {
                        "player_id": int(player_id),
                        "payload_sha256": payload_sha256,
                        "raw_uri": raw_uri,
                        "parser_version": PARSER_VERSION,
                        "state": "success",
                        "http_status": 200,
                        "failure_code": None,
                        "error": None,
                        "attempt_no": 1,
                        "retry_after": None,
                        "transport_mode": transport_mode,
                        "proxy_mode": proxy_mode,
                        "direct_bytes": int(direct_bytes),
                        "paid_bytes": int(paid_bytes),
                        "fetched_at": now,
                        "completed_at": now,
                        "_entity_type": "profile_manifest",
                    }
                ]
            ),
            database=self.schema,
            table=PROFILE_MANIFEST_TABLE,
            partition_spec=[("player_id", "bucket(32)")],
            source="whoscored",
        )

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
            ) WHERE _manifest_rank = 1
            """
        )
        for entity in ("events", "lineups"):
            physical = f"{self.catalog}.{self.schema}.whoscored_{entity}"
            current = f"{self.catalog}.{self.schema}.whoscored_{entity}_current"
            if not self.trino.table_exists(self.schema, f"whoscored_{entity}"):
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
                """
            )

        preview_latest = (
            f"{self.catalog}.{self.schema}.whoscored_preview_ingest_latest"
        )
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
            ) WHERE _manifest_rank = 1
            """
        )
        if self.trino.table_exists(self.schema, "whoscored_missing_players"):
            physical = f"{self.catalog}.{self.schema}.whoscored_missing_players"
            current = (
                f"{self.catalog}.{self.schema}.whoscored_missing_players_current"
            )
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
                """
            )

    def list_match_candidates(
        self,
        league: str,
        season: str,
        *,
        match_ids: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
    ) -> list[MatchCandidate]:
        """Return completed Opta games without a successful manifest commit."""
        latest = f"{self.catalog}.{self.schema}.whoscored_match_ingest_latest"
        ids = [int(value) for value in (match_ids or [])]
        id_filter = ""
        if ids:
            id_filter = " AND CAST(s.game_id AS BIGINT) IN (" + ",".join(
                str(value) for value in ids
            ) + ")"
        if limit is not None and int(limit) < 0:
            raise ValueError("match candidate limit must be non-negative")
        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
        sql = f"""
            WITH schedule AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY league, season, game_id
                    ORDER BY _ingested_at DESC
                ) AS rn
                FROM {self.catalog}.{self.schema}.whoscored_schedule
                WHERE league = {_sql_string(league)}
                  AND season = {_sql_string(season)}
            )
            SELECT CAST(s.game_id AS BIGINT), s.league, s.season, s.game,
                   s.date, CAST(s.status AS INTEGER), s.match_is_opta
            FROM schedule s
            LEFT JOIN {latest} m
              ON m.league = s.league
             AND m.season = s.season
             AND m.game_id = CAST(s.game_id AS BIGINT)
            WHERE s.rn = 1
              AND s.game_id IS NOT NULL
              AND s.match_is_opta = TRUE
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
              AND (
                    m.game_id IS NULL
                    OR (
                        m.state = 'retryable'
                        AND COALESCE(m.retry_after, TIMESTAMP '1970-01-01 00:00:00')
                            <= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
                    )
                    OR (
                        m.state = 'parse_failed'
                        AND m.parser_version IS DISTINCT FROM
                            {_sql_string(PARSER_VERSION)}
                    )
              )
              {id_filter}
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
                match_is_opta=bool(row[6]),
            )
            for row in rows
        ]

    def latest_source_season_id(self, league: str, season: str) -> Optional[int]:
        if not self.trino.table_exists(self.schema, "whoscored_season_stages"):
            return None
        rows = self.trino.execute_query(
            f"SELECT CAST(season_id AS BIGINT) "
            f"FROM {self.catalog}.{self.schema}.whoscored_season_stages "
            f"WHERE league = {_sql_string(league)} AND season = {_sql_string(season)} "
            "AND season_id IS NOT NULL ORDER BY _ingested_at DESC LIMIT 1"
        )
        return int(rows[0][0]) if rows else None

    def write_scope_snapshot(
        self,
        *,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        league: str,
        season: str,
        entity_type: str,
        distinct_key: str,
        min_replace_ratio: float = 0.9,
    ) -> str:
        """Replace one small scope only after an explicit completeness guard."""
        if (
            not table
            or not table[0].isalpha()
            or not table.replace("_", "").isalnum()
        ):
            raise ValueError(f"unsafe table name: {table!r}")
        if (
            not distinct_key
            or not distinct_key[0].isalpha()
            or not distinct_key.replace("_", "").isalnum()
        ):
            raise ValueError(f"unsafe distinct key: {distinct_key!r}")
        if not rows:
            raise ValueError(f"refusing an empty {table} scope snapshot")
        frame = pd.DataFrame([dict(row) for row in rows])
        for column, expected in (("league", league), ("season", season)):
            if column in frame and frame[column].notna().any():
                actual = {str(value) for value in frame[column].dropna().unique()}
                if actual != {str(expected)}:
                    raise ValueError(
                        f"{table} contains {column} values outside {expected!r}: {actual}"
                    )
            frame[column] = expected
        frame["_entity_type"] = entity_type
        if distinct_key not in frame:
            raise ValueError(f"{table} lacks distinct key {distinct_key!r}")
        if frame[distinct_key].isna().any():
            raise ValueError(f"{table}.{distinct_key} contains nulls")
        if frame[distinct_key].duplicated().any():
            raise ValueError(f"{table}.{distinct_key} contains duplicates")
        existing = 0
        if self.trino.table_exists(self.schema, table):
            result = self.trino.execute_query(
                f"SELECT COUNT(DISTINCT {distinct_key}) "
                f"FROM {self.catalog}.{self.schema}.{table} "
                f"WHERE league = {_sql_string(league)} "
                f"AND season = {_sql_string(season)}"
            )
            existing = int(result[0][0]) if result else 0
        new_count = int(frame[distinct_key].nunique())
        if existing and new_count < existing * float(min_replace_ratio):
            raise ValueError(
                f"{table} completeness guard: new={new_count}, old={existing}, "
                f"ratio={new_count / existing:.3f}"
            )
        return self.writer.write_dataframe(
            frame,
            database=self.schema,
            table=table,
            partition_spec=[("league", "identity"), ("season", "identity")],
            source="whoscored",
            delete_filter=(
                f"league = {_sql_string(league)} AND season = {_sql_string(season)}"
            ),
        )

    def list_preview_candidates(
        self,
        league: str,
        season: str,
        *,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Return only unseen, parser-stale, or due-for-retry preview pages."""
        if limit is not None and int(limit) < 0:
            raise ValueError("preview candidate limit must be non-negative")
        limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""
        latest = f"{self.catalog}.{self.schema}.whoscored_preview_ingest_latest"
        rows = self.trino.execute_query(
            f"""
            WITH schedule AS (
                SELECT s.*, ROW_NUMBER() OVER (
                    PARTITION BY league, season, game_id
                    ORDER BY _ingested_at DESC
                ) rn
                FROM {self.catalog}.{self.schema}.whoscored_schedule s
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
                            CURRENT_TIMESTAMP - INTERVAL '6' HOUR AS TIMESTAMP
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
              AND s.date BETWEEN CAST(
                    CURRENT_TIMESTAMP - INTERVAL '48' HOUR AS TIMESTAMP
                  ) AND CAST(
                    CURRENT_TIMESTAMP + INTERVAL '3' HOUR AS TIMESTAMP
                  )
              AND (
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
                                    CURRENT_TIMESTAMP - INTERVAL '6' HOUR
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
              )
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

    def record_preview_failure(self, failure: PreviewFailure) -> None:
        if failure.state not in {"retryable", "terminal", "parse_failed"}:
            raise ValueError(f"unsupported preview failure state: {failure.state}")
        if failure.state == "retryable" and failure.retry_after is None:
            raise ValueError("retryable preview failure requires retry_after")
        if failure.state != "retryable" and failure.retry_after is not None:
            raise ValueError(
                f"{failure.state} preview failure cannot have retry_after"
            )
        if int(failure.attempt_no) < 1:
            raise ValueError("preview failure attempt_no must be positive")
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
                        "batch_id": None,
                        "payload_sha256": failure.payload_sha256,
                        "raw_uri": failure.raw_uri,
                        "parser_version": failure.parser_version,
                        "state": failure.state,
                        "missing_players_count": None,
                        "transport_mode": failure.transport_mode,
                        "proxy_mode": failure.proxy_mode,
                        "http_status": failure.http_status,
                        "failure_code": failure.failure_code,
                        "error": failure.error[:4000],
                        "attempt_no": int(failure.attempt_no),
                        "retry_after": failure.retry_after,
                        "fetched_at": now,
                        "completed_at": (
                            None if failure.state == "retryable" else now
                        ),
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

    def _preview_batch_count(self, commit: PreviewCommit) -> int:
        rows = self.trino.execute_query(
            f"SELECT COUNT(*) FROM {self.catalog}.{self.schema}."
            "whoscored_missing_players "
            f"WHERE _preview_batch_id = {_sql_string(commit.batch_id)} "
            f"AND league = {_sql_string(commit.league)} "
            f"AND season = {_sql_string(commit.season)} "
            f"AND CAST(game_id AS BIGINT) = {int(commit.game_id)}"
        )
        return int(rows[0][0]) if rows else 0

    def _write_preview_batch(self, commit: PreviewCommit) -> None:
        expected = len(commit.missing_players)
        existing = self._preview_batch_count(commit)
        if existing == expected:
            return
        if existing:
            raise BatchConflict(
                f"preview batch {commit.batch_id} has {existing} rows; "
                f"parser produced {expected}"
            )
        if not commit.missing_players:
            return
        frame = pd.DataFrame([dict(row) for row in commit.missing_players])
        frame["league"] = commit.league
        frame["season"] = commit.season
        frame["game"] = commit.game
        frame["game_id"] = int(commit.game_id)
        frame["_preview_batch_id"] = commit.batch_id
        frame["_payload_sha256"] = commit.payload_sha256
        frame["_parser_version"] = commit.parser_version
        frame["_entity_type"] = "missing_players"
        for column in frame.columns:
            if frame[column].map(lambda value: isinstance(value, (dict, list))).any():
                frame[column] = frame[column].map(
                    lambda value: json.dumps(
                        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    )
                    if isinstance(value, (dict, list))
                    else value
                )
        self.writer.write_dataframe(
            frame,
            database=self.schema,
            table="whoscored_missing_players",
            partition_spec=[("league", "identity"), ("season", "identity")],
            source="whoscored",
        )
        written = self._preview_batch_count(commit)
        if written != expected:
            raise BatchConflict(
                f"preview batch {commit.batch_id}: wrote {written}, expected {expected}"
            )

    def commit_preview(self, commit: PreviewCommit) -> str:
        """Append a preview batch, then publish its manifest as commit point.

        A zero-row parsed page is a real successful snapshot: its manifest
        hides every older batch for the game without issuing an Iceberg DELETE.
        """
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

        expected = len(commit.missing_players)
        existing = self.trino.execute_query(
            f"SELECT missing_players_count FROM {self._preview_manifest} "
            f"WHERE league = {_sql_string(commit.league)} "
            f"AND season = {_sql_string(commit.season)} "
            f"AND game_id = {int(commit.game_id)} "
            f"AND batch_id = {_sql_string(commit.batch_id)} "
            "AND state = 'success' ORDER BY completed_at DESC LIMIT 1"
        )
        physical_count = self._preview_batch_count(commit)
        if existing:
            manifest_count = int(existing[0][0])
            if manifest_count != expected or physical_count != expected:
                raise BatchConflict(
                    f"preview {commit.game_id}/{commit.batch_id}: "
                    f"manifest={manifest_count}, physical={physical_count}, "
                    f"parser={expected}"
                )
        elif physical_count not in {0, expected}:
            raise BatchConflict(
                f"preview {commit.game_id}/{commit.batch_id}: orphan physical="
                f"{physical_count}, parser={expected}"
            )

        # Even when the payload hash is unchanged, publish a fresh manifest
        # row. It advances fetched_at after the bounded cadence refresh and
        # clears a newer retryable failure without duplicating payload rows.
        self._write_preview_batch(commit)
        now = _utc_now()
        fetched_at = commit.fetched_at or now
        self.writer.write_dataframe(
            pd.DataFrame(
                [
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
                        "state": "success",
                        "missing_players_count": expected,
                        "transport_mode": commit.transport_mode,
                        "proxy_mode": commit.proxy_mode,
                        "http_status": int(commit.http_status),
                        "failure_code": None,
                        "error": None,
                        "attempt_no": int(commit.attempt_no),
                        "retry_after": None,
                        "fetched_at": fetched_at,
                        "completed_at": now,
                        "direct_bytes": int(commit.direct_bytes),
                        "paid_bytes": int(commit.paid_bytes),
                        "_entity_type": "preview_manifest",
                    }
                ]
            ),
            database=self.schema,
            table=PREVIEW_MANIFEST_TABLE,
            partition_spec=[("league", "identity"), ("season", "identity")],
            source="whoscored",
        )
        return commit.batch_id

    def _batch_count(self, table: str, commit: MatchCommit) -> int:
        if not self.trino.table_exists(self.schema, table):
            return 0
        rows = self.trino.execute_query(
            f"SELECT COUNT(*) FROM {self.catalog}.{self.schema}.{table} "
            f"WHERE _game_batch_id = {_sql_string(commit.batch_id)} "
            f"AND league = {_sql_string(commit.league)} "
            f"AND season = {_sql_string(commit.season)} "
            f"AND CAST(game_id AS BIGINT) = {int(commit.game_id)}"
        )
        return int(rows[0][0]) if rows else 0

    def _write_batch_rows(
        self,
        *,
        table: str,
        entity_type: str,
        rows: Sequence[Mapping[str, Any]],
        commit: MatchCommit,
    ) -> None:
        expected = len(rows)
        existing = self._batch_count(table, commit)
        if existing == expected:
            return
        if existing:
            raise BatchConflict(
                f"{table} batch {commit.batch_id} has {existing} rows; "
                f"parser produced {expected}"
            )
        if not rows:
            return
        frame = pd.DataFrame([dict(row) for row in rows])
        frame["league"] = commit.league
        frame["season"] = commit.season
        frame["game"] = commit.game
        frame["game_id"] = int(commit.game_id)
        frame["_game_batch_id"] = commit.batch_id
        frame["_payload_sha256"] = commit.payload_sha256
        frame["_parser_version"] = commit.parser_version
        frame["_entity_type"] = entity_type
        for column in frame.columns:
            if frame[column].map(lambda value: isinstance(value, (dict, list))).any():
                frame[column] = frame[column].map(
                    lambda value: json.dumps(
                        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    )
                    if isinstance(value, (dict, list))
                    else value
                )
        self.writer.write_dataframe(
            frame,
            database=self.schema,
            table=table,
            partition_spec=[("league", "identity"), ("season", "identity")],
            source="whoscored",
        )
        written = self._batch_count(table, commit)
        if written != expected:
            raise BatchConflict(
                f"{table} batch {commit.batch_id}: wrote {written}, expected {expected}"
            )

    def commit_match(self, commit: MatchCommit) -> str:
        """Append both datasets and publish one successful manifest commit."""
        if not commit.events:
            raise ValueError(f"completed game {commit.game_id} has no events")
        event_ids = [row.get("source_event_id") for row in commit.events]
        if any(value is None for value in event_ids):
            raise ValueError(f"game {commit.game_id} has null source_event_id")
        if len(event_ids) != len(set(event_ids)):
            raise ValueError(f"game {commit.game_id} has duplicate source_event_id")
        if bool(commit.lineups) != bool(commit.lineups_available):
            raise ValueError(
                f"game {commit.game_id} lineups_available disagrees with lineup rows"
            )

        already = self.trino.execute_query(
            f"SELECT events_count, lineups_count FROM {self._manifest} "
            f"WHERE league = {_sql_string(commit.league)} "
            f"AND season = {_sql_string(commit.season)} "
            f"AND game_id = {int(commit.game_id)} "
            f"AND batch_id = {_sql_string(commit.batch_id)} AND state = 'success' "
            "ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC LIMIT 1"
        )
        if already:
            stored_counts = (int(already[0][0]), int(already[0][1]))
            expected_counts = (len(commit.events), len(commit.lineups))
            physical_counts = (
                self._batch_count("whoscored_events", commit),
                self._batch_count("whoscored_lineups", commit),
            )
            if stored_counts != expected_counts or physical_counts != expected_counts:
                raise BatchConflict(
                    f"committed batch {commit.batch_id} counts: manifest={stored_counts}, "
                    f"physical={physical_counts}, parser={expected_counts}"
                )
            return commit.batch_id

        self._write_batch_rows(
            table="whoscored_events",
            entity_type="events",
            rows=commit.events,
            commit=commit,
        )
        self._write_batch_rows(
            table="whoscored_lineups",
            entity_type="lineups",
            rows=commit.lineups,
            commit=commit,
        )

        now = _utc_now()
        row = {
            "league": commit.league,
            "season": commit.season,
            "game_id": int(commit.game_id),
            "game": commit.game,
            "kickoff": commit.kickoff,
            "batch_id": commit.batch_id,
            "payload_sha256": commit.payload_sha256,
            "raw_uri": commit.raw_uri,
            "parser_version": commit.parser_version,
            "state": "success",
            "is_final": True,
            "is_opta": True,
            "events_count": len(commit.events),
            "lineups_count": len(commit.lineups),
            "lineups_available": bool(commit.lineups_available),
            "transport_mode": commit.transport_mode,
            "proxy_mode": commit.proxy_mode,
            "http_status": int(commit.http_status),
            "failure_code": None,
            "error": None,
            "attempt_no": 1,
            "retry_after": None,
            "fetched_at": commit.fetched_at or now,
            "completed_at": now,
            "direct_bytes": int(commit.direct_bytes),
            "paid_bytes": int(commit.paid_bytes),
            "_entity_type": "match_manifest",
        }
        self.writer.write_dataframe(
            pd.DataFrame([row]),
            database=self.schema,
            table=MATCH_MANIFEST_TABLE,
            partition_spec=[("league", "identity"), ("season", "identity")],
            source="whoscored",
        )
        return commit.batch_id

    def record_failure(self, failure: ManifestFailure) -> None:
        if failure.state not in {"retryable", "terminal", "parse_failed"}:
            raise ValueError(f"unsupported manifest failure state: {failure.state}")
        now = _utc_now()
        row = asdict(failure)
        row.update(
            {
                "game": None,
                "kickoff": None,
                "batch_id": None,
                "payload_sha256": failure.payload_sha256,
                "raw_uri": failure.raw_uri,
                "parser_version": failure.parser_version,
                "is_final": failure.state != "retryable",
                "is_opta": True,
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
