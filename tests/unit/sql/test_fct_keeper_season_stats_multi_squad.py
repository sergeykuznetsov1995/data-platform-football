"""
DuckDB-execution test for gold keeper Variant B (issue #515, на базе #463):
``dags/sql/gold/fct_keeper_season_stats.sql`` over a multi-squad silver fixture.

Mirrors test_fct_player_season_stats_multi_squad but for the keeper fact and,
crucially, exercises the #515 B2 ratio re-derivation: для мульти-squad вратаря
несуммируемые ratio (save_pct / clean_sheet_pct / goals_against_per90 /
pk_save_pct) ПЕРЕСЧИТЫВАЮТСЯ из суммированных счётчиков; одноклубный вратарь
сохраняет родной FBref-ratio (0-diff к Варианту A).

The two enrichment sources (FotMob keeper / WhoScored) are empty — LEFT JOINs
yield NULLs, so every published column comes from the FBref keeper spine.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))
os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(PROJECT_ROOT / "configs" / "medallion")
)

SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_keeper_season_stats.sql.j2"

pytestmark = pytest.mark.unit


_ICEBERG_TO_LOCAL = {
    "iceberg.silver.xref_player":                       "silver_xref_player",
    "iceberg.silver.xref_team":                         "silver_xref_team",
    "iceberg.silver.fbref_keeper_profile":              "silver_fbref_keeper_profile",
    "iceberg.silver.fotmob_keeper_profile":             "silver_fotmob_keeper_profile",
    "iceberg.silver.whoscored_player_season_aggregate": "silver_whoscored_player_season_aggregate",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    return sql


_VARCHAR_COLS = {"player_id", "canonical_id", "source", "source_id", "league",
                 "season", "confidence", "squad"}


def _ddl(table: str, cols: list[str]) -> str:
    defs = ", ".join(
        f'"{c}" {"VARCHAR" if c in _VARCHAR_COLS else "DOUBLE"}' for c in cols
    )
    return f"CREATE TABLE {table} ({defs})"


_FB_COLS = [
    "player_id", "league", "season", "squad",
    "mp", "minutes", "clean_sheets", "yellow_cards", "red_cards",
    "goals_against", "shots_on_target_against", "saves",
    "wins", "draws", "losses",
    "pk_faced", "pk_allowed", "pk_saved", "pk_missed",
    "goals_against_per90", "save_pct", "clean_sheet_pct", "pk_save_pct",
]

_FM_COLS = [
    "player_id", "league", "season", "matches_played", "minutes_played",
    "clean_sheets", "yellow_cards", "red_cards", "save_percentage",
    "saves_per_90", "goals_prevented", "accurate_passes_per_90",
    "accurate_long_balls_per_90", "fotmob_rating",
]

_WS_COLS = [
    "canonical_id", "league", "season",
    "keeper_saves", "keeper_pickups", "keeper_claims",
]

_TABLES = {
    "silver_xref_player": ["canonical_id", "source", "source_id", "league",
                           "season", "confidence"],
    "silver_xref_team":   ["canonical_id", "source", "source_id", "league",
                           "season", "confidence"],
    "silver_fbref_keeper_profile":              _FB_COLS,
    "silver_fotmob_keeper_profile":             _FM_COLS,
    "silver_whoscored_player_season_aggregate": _WS_COLS,
}

LEAGUE = "ENG-Premier League"
SEASON = "2223"
KEEPER_ID = "abc12345"


@pytest.fixture()
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    for table, cols in _TABLES.items():
        con.execute(_ddl(table, cols))
    con.execute(
        "INSERT INTO silver_xref_player VALUES "
        "('fb_abc12345', 'fbref', 'abc12345', ?, ?, 'exact')",
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


def _insert_keeper(con, squad, minutes, mp, saves, sota, ga, cs,
                   pk_faced, pk_saved, save_pct, cs_pct, ga90, pk_save_pct):
    con.execute(
        """
        INSERT INTO silver_fbref_keeper_profile
            (player_id, league, season, squad, minutes, mp, saves,
             shots_on_target_against, goals_against, clean_sheets,
             pk_faced, pk_saved, save_pct, clean_sheet_pct,
             goals_against_per90, pk_save_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [KEEPER_ID, LEAGUE, SEASON, squad, minutes, mp, saves, sota, ga, cs,
         pk_faced, pk_saved, save_pct, cs_pct, ga90, pk_save_pct],
    )


def _run(con):
    from utils.medallion_config import render_fact_sql
    rendered = render_fact_sql(SQL_PATH, "fct_keeper_season_stats")
    sql = _translate(rendered)
    return con.execute(sql).fetch_df()


class TestKeeperMultiSquadVariantB:

    def test_counters_summed_and_ratios_recomputed(self, duck_conn):
        # Arrange: winter transfer — Villa (max minutes) + West Ham
        _insert_keeper(duck_conn, "Aston Villa", 2700, 30, 90, 120, 30, 10,
                       4, 2, 75.0, 33.3, 1.0, 50.0)
        _insert_keeper(duck_conn, "West Ham United", 900, 10, 20, 40, 20, 2,
                       2, 1, 50.0, 20.0, 2.0, 50.0)

        # Act
        df = _run(duck_conn)

        # Assert: one row; team_id from the max-minutes club; counters SUMmed.
        assert len(df) == 1
        row = df.iloc[0]
        assert row["team_id"] == "aston_villa"
        assert row["minutes"] == 2700 + 900
        assert row["saves"] == 90 + 20
        assert row["goals_against"] == 30 + 20
        assert row["clean_sheets"] == 10 + 2

        # #515 B2: ratios re-derived from the summed counters (multi-squad).
        assert row["save_pct"] == pytest.approx(110 / 160 * 100, abs=0.05)        # 68.8
        assert row["clean_sheet_pct"] == pytest.approx(12 / 40 * 100, abs=0.05)   # 30.0
        assert row["goals_against_per90"] == pytest.approx(50 * 90 / 3600, abs=0.01)  # 1.25
        assert row["pk_save_pct"] == pytest.approx(3 / 6 * 100, abs=0.05)         # 50.0

    def test_single_squad_keeps_native_fbref_ratio(self, duck_conn):
        # #515 surgical guard: одноклубный вратарь НЕ меняется — родной
        # FBref save_pct сохраняется (НЕ пересчитывается из сумм). Ставим
        # намеренно "несочетающийся" native ratio (71.5 != 90/120*100=75),
        # чтобы доказать, что берётся именно он, а не пересчёт.
        _insert_keeper(duck_conn, "Aston Villa", 2700, 30, 90, 120, 30, 10,
                       4, 2, 71.5, 33.3, 1.0, 50.0)

        # Act
        df = _run(duck_conn)

        # Assert
        assert len(df) == 1
        row = df.iloc[0]
        assert row["team_id"] == "aston_villa"
        assert row["saves"] == 90
        assert row["save_pct"] == pytest.approx(71.5, abs=0.001), (
            "single-squad keeper must keep its native FBref save_pct, not a "
            "recomputed value"
        )
