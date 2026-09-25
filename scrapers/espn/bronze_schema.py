"""Bronze tables of the new ESPN contour (#1503).

Four data tables in ``iceberg.bronze``, one row per natural key, partitioned
by ``competition_slug`` and ``season_year``; no generations, no ``_current``
suffix (convention #1156).  The frozen ``*_generation_v2`` tables and the
legacy ``espn_lineup``/``espn_matchsheet`` stay untouched: master-silver reads
them until the marts are unfrozen.

``espn_lineup`` is already taken in bronze by the legacy soccerdata table that
``dags/sql/silver/espn_lineup.sql`` reads, so the lineup table of the new
contour is ``espn_match_lineup``.  ``espn_match`` and ``espn_match_events``
share their names with silver tables (different schema; #1156 landmine).

Raw bodies are not copied into the rows: ``raw_uri``/``raw_sha256`` point to
the raw store (#1500).  The tables are created by the DAG of the new contour
(#1504) through ``ensure_bronze_tables``.
"""

from __future__ import annotations

import pyarrow as pa

BRONZE_DATABASE = "bronze"

MATCH_TABLE = "espn_match"
LINEUP_TABLE = "espn_match_lineup"
TEAM_STATS_TABLE = "espn_team_stats"
EVENTS_TABLE = "espn_match_events"

PARTITION_SPEC = [("competition_slug", "identity"), ("season_year", "identity")]

# Every timestamp is naive UTC (platform convention, like the #1500 journal).
_TS = pa.timestamp("us")

LINEAGE_FIELDS = (
    pa.field("_source", pa.string()),
    # Commit time of the batch: one value for every row of a batch.
    pa.field("_ingested_at", _TS),
    pa.field("_batch_id", pa.string()),
    pa.field("_source_fetched_at", _TS),
    pa.field("raw_uri", pa.string()),
    pa.field("raw_sha256", pa.string()),
    pa.field("parser_version", pa.string()),
)

_SCOPE_FIELDS = (
    pa.field("competition_slug", pa.string()),
    pa.field("season_year", pa.int32()),
    pa.field("event_id", pa.int64()),
)

LINEUP_STAT_COLUMNS = (
    "appearances",
    "fouls_committed",
    "fouls_suffered",
    "goal_assists",
    "goals_conceded",
    "offsides",
    "own_goals",
    "red_cards",
    "saves",
    "shots_faced",
    "shots_on_target",
    "sub_ins",
    "total_goals",
    "total_shots",
    "yellow_cards",
)

TEAM_STAT_COLUMNS = (
    "accurate_crosses",
    "accurate_long_balls",
    "accurate_passes",
    "blocked_shots",
    "cross_pct",
    "effective_clearance",
    "effective_tackles",
    "fouls_committed",
    "interceptions",
    "longball_pct",
    "offsides",
    "pass_pct",
    "penalty_kick_goals",
    "penalty_kick_shots",
    "possession_pct",
    "red_cards",
    "saves",
    "shot_pct",
    "shots_on_target",
    "tackle_pct",
    "total_clearance",
    "total_crosses",
    "total_long_balls",
    "total_passes",
    "total_shots",
    "total_tackles",
    "won_corners",
    "yellow_cards",
)


def _schema(*fields: pa.Field) -> pa.Schema:
    return pa.schema([*_SCOPE_FIELDS, *fields, *LINEAGE_FIELDS])


MATCH_SCHEMA = _schema(
    pa.field("kickoff", _TS),
    pa.field("kickoff_confirmed", pa.bool_()),
    pa.field("status", pa.string()),
    pa.field("status_map_version", pa.string()),
    pa.field("terminal", pa.bool_()),
    pa.field("played_final", pa.bool_()),
    pa.field("terminal_nonplayed", pa.bool_()),
    pa.field("home_team_id", pa.int64()),
    pa.field("home_team", pa.string()),
    pa.field("away_team_id", pa.int64()),
    pa.field("away_team", pa.string()),
    # NULL until the match is played.
    pa.field("home_score", pa.int32()),
    pa.field("away_score", pa.int32()),
    pa.field("home_score_h1", pa.int32()),
    pa.field("away_score_h1", pa.int32()),
    pa.field("home_score_h2", pa.int32()),
    pa.field("away_score_h2", pa.int32()),
    pa.field("home_score_et", pa.int32()),
    pa.field("away_score_et", pa.int32()),
    pa.field("home_shootout", pa.int32()),
    pa.field("away_shootout", pa.int32()),
    pa.field("home_aggregate", pa.int32()),
    pa.field("away_aggregate", pa.int32()),
    pa.field("leg", pa.int32()),
    pa.field("advance_team_id", pa.int64()),
    pa.field("home_formation", pa.string()),
    pa.field("away_formation", pa.string()),
    pa.field("venue_id", pa.int64()),
    pa.field("venue", pa.string()),
    pa.field("attendance", pa.int64()),
    pa.field("referee", pa.string()),
    # Summary outcome (#1502); NULL while the Summary is not fetched.
    pa.field("disposition", pa.string()),
    # JSON list of lineup anomaly classes.
    pa.field("anomalies", pa.string()),
    pa.field("reason", pa.string()),
    # captured / valid_empty / malformed, or pending before the Summary.
    pa.field("lineup_state", pa.string()),
    pa.field("team_stats_state", pa.string()),
    pa.field("events_state", pa.string()),
    # Deep core statistics: pending until wave 3.
    pa.field("deep_state", pa.string()),
    pa.field("duplicate_of", pa.string()),
    pa.field("parse_state", pa.string()),
    pa.field("first_fetched_at", _TS),
    # NULL until the recheck (#1506).
    pa.field("rechecked_at", _TS),
)

LINEUP_SCHEMA = _schema(
    pa.field("team_id", pa.int64()),
    pa.field("team", pa.string()),
    pa.field("home_away", pa.string()),
    pa.field("athlete_id", pa.int64()),
    pa.field("player", pa.string()),
    pa.field("jersey", pa.string()),
    pa.field("position", pa.string()),
    pa.field("formation_place", pa.string()),
    pa.field("starter", pa.bool_()),
    pa.field("subbed_in", pa.bool_()),
    pa.field("subbed_out", pa.bool_()),
    pa.field("sub_in", pa.string()),
    pa.field("sub_out", pa.string()),
    *(pa.field(name, pa.float64()) for name in LINEUP_STAT_COLUMNS),
    pa.field("substitutions_json", pa.string()),
    pa.field("lineup_anomaly", pa.bool_()),
    # NULL until wave 3.
    pa.field("deep_stats_json", pa.string()),
)

TEAM_STATS_SCHEMA = _schema(
    pa.field("team_id", pa.int64()),
    pa.field("team", pa.string()),
    pa.field("home_away", pa.string()),
    pa.field("formation", pa.string()),
    *(pa.field(name, pa.float64()) for name in TEAM_STAT_COLUMNS),
    pa.field("team_stats_state", pa.string()),
    # NULL until wave 3.
    pa.field("deep_stats_json", pa.string()),
)

EVENTS_SCHEMA = _schema(
    pa.field("kind", pa.string()),
    # play_id for key events; sequence (else play_id) for commentary.
    pa.field("event_key", pa.string()),
    pa.field("play_id", pa.string()),
    pa.field("sequence", pa.int64()),
    pa.field("period", pa.int32()),
    pa.field("clock_value", pa.float64()),
    pa.field("clock_display", pa.string()),
    pa.field("team_id", pa.int64()),
    # JSON list of native athlete ids.
    pa.field("athlete_ids", pa.string()),
    pa.field("type_id", pa.string()),
    pa.field("type_text", pa.string()),
    pa.field("text", pa.string()),
    pa.field("x", pa.float64()),
    pa.field("y", pa.float64()),
    pa.field("scoring_play", pa.bool_()),
    pa.field("red_card", pa.bool_()),
    pa.field("yellow_card", pa.bool_()),
    pa.field("penalty_kick", pa.bool_()),
    pa.field("own_goal", pa.bool_()),
    pa.field("home_score", pa.int32()),
    pa.field("away_score", pa.int32()),
)

# Write order of a batch: match -> lineup -> team_stats -> events.
TABLES: dict[str, pa.Schema] = {
    MATCH_TABLE: MATCH_SCHEMA,
    LINEUP_TABLE: LINEUP_SCHEMA,
    TEAM_STATS_TABLE: TEAM_STATS_SCHEMA,
    EVENTS_TABLE: EVENTS_SCHEMA,
}

# Natural key of each table inside its partition.
NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    MATCH_TABLE: ("event_id",),
    LINEUP_TABLE: ("event_id", "team_id", "athlete_id"),
    TEAM_STATS_TABLE: ("event_id", "team_id"),
    EVENTS_TABLE: ("event_id", "kind", "event_key"),
}


def ensure_bronze_tables(writer) -> None:
    """Create the four tables when missing (``IcebergWriter``)."""
    for table, schema in TABLES.items():
        writer.create_table_if_not_exists(
            BRONZE_DATABASE,
            table,
            schema,
            partition_spec=PARTITION_SPEC,
        )
