"""
DuckDB-execution test for gold Variant A (issue #463):
``dags/sql/gold/fct_player_season_stats.sql`` over a multi-squad silver fixture.

After #463 silver.fbref_player_season_profile keeps one row per
(player_id, squad, league, season) — a winter transfer inside the league
yields TWO rows per player-season. The gold fct must collapse them back to
ONE row via the fb_dedup CTE: survivor = max-minutes club (design contract
docs/design/gold-star-schema.md §5.2), deterministic tiebreaker = squad ASC.

Fixture mirrors the live canary (feedback_fixture_must_mirror_live_enums):
Danny Ings 2022/23 — Aston Villa 824 min / 6 goals, West Ham United
775 min / 2 goals → gold row must carry team_id='aston_villa', goals=6.

Only the FBref spine + xref tables carry rows; the four enrichment sources
(FotMob/WhoScored/Understat/SofaScore) are empty — LEFT JOINs yield NULLs
and every COALESCE falls back to FBref.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]

# #542: gold SQL is now a source-priority template (.sql.j2) — render it from
# configs/medallion/source_priority.yaml before translating for DuckDB.
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))
os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(PROJECT_ROOT / "configs" / "medallion")
)

SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_season_stats.sql.j2"

pytestmark = pytest.mark.unit


_ICEBERG_TO_LOCAL = {
    "iceberg.silver.xref_player":                       "silver_xref_player",
    "iceberg.silver.xref_team":                         "silver_xref_team",
    "iceberg.silver.fbref_player_season_profile":       "silver_fbref_player_season_profile",
    "iceberg.silver.fotmob_player_season_profile":      "silver_fotmob_player_season_profile",
    "iceberg.silver.whoscored_player_season_aggregate": "silver_whoscored_player_season_aggregate",
    "iceberg.silver.understat_player_season_aggregate": "silver_understat_player_season_aggregate",
    "iceberg.silver.sofascore_player_season_aggregate": "silver_sofascore_player_season_aggregate",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    return sql


# ---------------------------------------------------------------------------
# Fixture schemas — only the columns the gold SQL references
# ---------------------------------------------------------------------------

_VARCHAR_COLS = {"player_id", "canonical_id", "source", "source_id", "league",
                 "season", "confidence", "player", "nation", "pos", "squad"}


def _ddl(table: str, cols: list[str]) -> str:
    defs = ", ".join(
        f'"{c}" {"VARCHAR" if c in _VARCHAR_COLS else "DOUBLE"}' for c in cols
    )
    return f"CREATE TABLE {table} ({defs})"


_FB_COLS = [
    "player_id", "league", "season", "squad", "pos",
    "mp", "minutes", "goals", "assists", "yellow_cards", "red_cards",
    "penalty_goals", "penalty_attempts", "penalties_won", "penalties_conceded",
    "shots", "shots_on_target", "interceptions", "tackles_won",
    "fouls_committed", "fouls_drawn", "offsides", "crosses", "own_goals",
    "second_yellow", "complete_matches", "starts", "subs", "unused_sub",
    "plus_minus", "points_per_match", "on_off_impact", "goals_per_shot",
]

_FM_COLS = [
    "player_id", "league", "season", "minutes_played", "matches_played",
    "goals", "assists", "yellow_cards", "red_cards",
    "penalties_won", "penalties_conceded",
    "expected_goals", "expected_assists", "expected_goals_on_target",
    "big_chances_created", "big_chances_missed", "chances_created",
    "shots_per_90", "shots_on_target_per_90", "interceptions_per_90",
    "tackles_per_90", "fouls_per_90", "clearances_per_90", "recoveries_per_90",
    "blocks_per_90", "successful_dribbles_per_90", "accurate_passes_per_90",
    "accurate_long_balls_per_90", "defensive_actions_per_90",
    "poss_won_final_third_per_90",
]

_WS_COLS = [
    "canonical_id", "league", "season", "matches_seen", "shots_total",
    "shots_on_target_proxy", "interceptions", "tackle_won", "tackle_att",
    "tackle_pct", "fouls_committed", "clearances", "ball_recoveries",
    "takeon_won", "takeon_att", "takeon_pct", "dribbles", "pass_total",
    "pass_ok", "pass_pct", "bad_touches", "touches_in_box", "avg_x", "avg_y",
]

_US_COLS = [
    "canonical_id", "league", "season", "games_played", "minutes_played",
    "goals", "assists", "yellow_cards", "red_cards", "shots", "key_passes",
    "expected_goals", "expected_assists", "non_penalty_goals",
    "non_penalty_xg", "xg_chain", "xg_buildup",
]

_SS_COLS = [
    "canonical_id", "league", "season", "rating", "expected_goals",
    "penalty_goals", "penalties_taken", "penalty_won", "penalty_conceded",
    "total_shots", "shots_on_target", "interceptions", "tackles_won",
    "tackles", "fouls", "was_fouled", "offsides", "clearances",
    "ball_recoveries", "blocks", "dribbles", "dribbles_pct", "total_passes",
    "accurate_passes", "accurate_passes_pct", "accurate_long_balls",
    "accurate_long_balls_pct", "total_long_balls", "total_crosses",
    "accurate_crosses", "accurate_crosses_pct", "key_passes",
    "tackles_won_pct", "ground_duels_won", "ground_duels_won_pct",
    "aerial_duels_won", "aerial_duels_won_pct", "total_duels_won",
    "total_duels_won_pct", "errors_lead_to_goal", "errors_lead_to_shot",
    "touches", "dispossessed", "possession_lost", "poss_won_att_third",
    "totw_appearances", "matches_started", "appearances", "dribbled_past",
    "secondary_assists", "final_third_passes", "shots_off_target",
    "shots_inside_box", "shots_outside_box", "blocked_shots", "hit_woodwork",
    "goal_conversion_pct", "goals_inside_box", "goals_outside_box",
    "headed_goals", "left_foot_goals", "right_foot_goals", "set_piece_shots",
    "free_kick_goals",
]

_TABLES = {
    "silver_xref_player": ["canonical_id", "source", "source_id", "league",
                           "season", "confidence"],
    "silver_xref_team":   ["canonical_id", "source", "source_id", "league",
                           "season", "confidence"],
    "silver_fbref_player_season_profile":       _FB_COLS,
    "silver_fotmob_player_season_profile":      _FM_COLS,
    "silver_whoscored_player_season_aggregate": _WS_COLS,
    "silver_understat_player_season_aggregate": _US_COLS,
    "silver_sofascore_player_season_aggregate": _SS_COLS,
}

LEAGUE = "ENG-Premier League"
SEASON = "2223"


@pytest.fixture()
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    for table, cols in _TABLES.items():
        con.execute(_ddl(table, cols))
    # xref spine: Danny Ings, FBref id 07802f7f → canonical fb_07802f7f
    con.execute(
        "INSERT INTO silver_xref_player VALUES "
        "('fb_07802f7f', 'fbref', '07802f7f', ?, ?, 'exact')",
        [LEAGUE, SEASON],
    )
    con.execute(
        "INSERT INTO silver_xref_team VALUES "
        "('aston_villa', 'fbref', 'Aston Villa', ?, ?, 'exact'), "
        "('west_ham_united', 'fbref', 'West Ham United', ?, ?, 'exact')",
        [LEAGUE, SEASON, LEAGUE, SEASON],
    )
    yield con
    con.close()


def _insert_profile(con, squad: str, minutes: float, goals: float):
    con.execute(
        """
        INSERT INTO silver_fbref_player_season_profile
            (player_id, league, season, squad, pos, mp, minutes, goals)
        VALUES ('07802f7f', ?, ?, ?, 'FW', 38, ?, ?)
        """,
        [LEAGUE, SEASON, squad, minutes, goals],
    )


def _run(con):
    from utils.medallion_config import render_fact_sql
    rendered = render_fact_sql(SQL_PATH, "fct_player_season_stats")
    sql = _translate(rendered)
    return con.execute(sql).fetch_df()


class TestMultiSquadCollapse:

    def test_one_gold_row_team_id_from_max_minutes_club(self, duck_conn):
        # Arrange: winter transfer — two silver rows for one player-season
        _insert_profile(duck_conn, "Aston Villa", 824, 6)
        _insert_profile(duck_conn, "West Ham United", 775, 2)

        # Act
        df = _run(duck_conn)

        # Assert: gold PK (player_id, league, season) — exactly one row,
        # whole row taken from the max-minutes club (Variant A)
        assert len(df) == 1, (
            f"multi-squad silver rows must collapse to 1 gold row, got {len(df)}"
        )
        row = df.iloc[0]
        assert row["player_id"] == "fb_07802f7f"
        assert row["team_id"] == "aston_villa", (
            "team_id must resolve to the max-minutes club (§5.2)"
        )
        assert row["minutes"] == 824
        assert row["goals"] == 6

    def test_minutes_tie_resolved_by_squad_alphabetical(self, duck_conn):
        # Arrange: equal minutes — tiebreaker = squad ASC (deterministic)
        _insert_profile(duck_conn, "West Ham United", 800, 2)
        _insert_profile(duck_conn, "Aston Villa", 800, 6)

        # Act
        df = _run(duck_conn)

        # Assert
        assert len(df) == 1
        assert df.iloc[0]["team_id"] == "aston_villa", (
            "minutes tie must resolve deterministically (squad ASC)"
        )
