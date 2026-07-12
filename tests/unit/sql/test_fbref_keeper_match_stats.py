"""Typed Silver and additive Gold contract for per-match keeper stats (#870)."""

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / "dags" / "sql" / "silver" / "fbref_keeper_match_stats.sql"
GOLD = ROOT / "dags" / "sql" / "gold" / "fct_player_match.sql.j2"

pytestmark = pytest.mark.unit


@pytest.fixture()
def con():
    duckdb = pytest.importorskip("duckdb")
    db = duckdb.connect()
    db.execute(
        """CREATE TABLE bronze_keeper (
            match_id VARCHAR, player_id VARCHAR, "Player" VARCHAR,
            team VARCHAR, team_side VARCHAR, "Nation" VARCHAR, "Age" VARCHAR,
            "Min" VARCHAR, "Shot Stopping_SoTA" VARCHAR,
            "Shot Stopping_GA" VARCHAR, "Shot Stopping_Saves" VARCHAR,
            "Shot Stopping_Save%" VARCHAR, league VARCHAR, season BIGINT,
            source_season_id VARCHAR,
            _ingested_at TIMESTAMP, _batch_id VARCHAR
        )"""
    )
    db.execute(
        """CREATE TABLE player_identity (
            player_id VARCHAR, player_name VARCHAR, team_name VARCHAR,
            league VARCHAR, season VARCHAR, is_synthetic BOOLEAN,
            id_resolution VARCHAR, id_evidence_datasets VARCHAR[]
        )"""
    )
    yield db
    db.close()


def _run(con):
    sql = SILVER.read_text(encoding="utf-8").replace(
        "iceberg.bronze.fbref_match_keeper_stats", "bronze_keeper"
    )
    sql = sql.replace(
        "iceberg.silver.fbref_player_identity", "player_identity"
    )
    sql = sql.replace(", NFD)", ")").replace("normalize(", "strip_accents(")
    sql = sql.replace("REGEXP_LIKE(", "REGEXP_MATCHES(")
    return con.execute(sql).fetch_df()


def test_typed_latest_keeper_row(con):
    con.execute(
        "INSERT INTO player_identity VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("98ea5115", "David Raya", "Arsenal", "ENG-Premier League", "2526",
         False, "source_native", ["match_keeper_stats"]),
    )
    rows = [
        ("m1", "98ea5115", "David Raya", "Arsenal", "away", "es ESP", "29-336",
         "90", "7", "1", "6", "85.7", "ENG-Premier League", 2025, None,
         "2026-07-01", "b1"),
        ("m1", "98ea5115", "David Raya", "Arsenal", "away", "es ESP", "29-336",
         "90", "7", "0", "7", "100.0", "ENG-Premier League", 2025, None,
         "2026-07-02", "b2"),
    ]
    con.executemany("INSERT INTO bronze_keeper VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    out = _run(con)
    assert len(out) == 1
    row = out.iloc[0]
    assert row["player_name"] == "David Raya"
    assert row["minutes"] == 90
    assert row["shots_on_target_against"] == 7
    assert row["goals_against"] == 0
    assert row["saves"] == 7
    assert row["save_pct"] == 100.0
    assert row["season"] == "2526"


def test_blank_keeper_id_resolves_and_survives(con):
    resolved_id = "noid_keeper_raya"
    con.execute(
        "INSERT INTO player_identity VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (resolved_id, "David Raya", "Arsenal", "ENG-Premier League", "2526",
         True, "synthetic_residual", ["match_keeper_stats", "lineups"]),
    )
    con.execute(
        "INSERT INTO bronze_keeper VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("m-blank", "", "David Raya", "Arsenal", "home", "es ESP", "29-336",
         "90", "2", "0", "2", "100.0", "ENG-Premier League", 2025, None,
         "2026-07-02", "b1"),
    )

    out = _run(con)

    assert len(out) == 1
    assert out.iloc[0]["player_id"] == resolved_id
    assert out.iloc[0]["player_id_resolution"] == "synthetic_residual"
    assert bool(out.iloc[0]["player_id_is_synthetic"]) is True


def test_gold_extends_existing_player_match_grain():
    sql = GOLD.read_text(encoding="utf-8")
    assert "iceberg.silver.fbref_keeper_match_stats" in sql
    for column in (
        "gk_minutes", "gk_shots_on_target_against", "gk_goals_against",
        "gk_saves", "gk_save_pct",
    ):
        assert f"AS {column}" in sql
    assert "gk.match_id   = fb.match_id" in sql
    assert "gk.player_id  = fb.player_id" in sql
    assert "gk.team_side  = fb.team_side" in sql
