"""Blank FBref match-grain IDs resolve before Silver dedup and Gold joins."""

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
PLAYER_SQL = ROOT / "dags/sql/silver/fbref_player_match_stats.sql"
LINEUP_SQL = ROOT / "dags/sql/silver/fbref_match_lineups.sql"
GOLD_SQL = ROOT / "dags/sql/gold/fct_player_match.sql.j2"

pytestmark = pytest.mark.unit


def _translate(sql: str, bronze_name: str) -> str:
    sql = sql.replace(
        f"iceberg.bronze.{bronze_name}", bronze_name
    ).replace(
        "iceberg.silver.fbref_player_identity", "player_identity"
    )
    sql = sql.replace(", NFD)", ")").replace("normalize(", "strip_accents(")
    return sql.replace("REGEXP_LIKE(", "REGEXP_MATCHES(")


@pytest.fixture()
def con():
    duckdb = pytest.importorskip("duckdb")
    db = duckdb.connect()
    db.execute(
        """CREATE TABLE player_identity (
            player_id VARCHAR, player_name VARCHAR, team_name VARCHAR,
            league VARCHAR, season VARCHAR, is_synthetic BOOLEAN,
            id_resolution VARCHAR, id_evidence_datasets VARCHAR[]
        )"""
    )
    db.execute(
        """CREATE TABLE fbref_match_player_stats (
            match_id VARCHAR, player_id VARCHAR, player VARCHAR, team VARCHAR,
            team_side VARCHAR, nation VARCHAR, pos VARCHAR, age VARCHAR,
            min VARCHAR, gls VARCHAR, ast VARCHAR, pk VARCHAR, pkatt VARCHAR,
            sh VARCHAR, sot VARCHAR, crdy VARCHAR, crdr VARCHAR, crs VARCHAR,
            fls VARCHAR, fld VARCHAR, "off" VARCHAR, tklw VARCHAR,
            "int" VARCHAR, og VARCHAR, pkwon DOUBLE, pkcon DOUBLE,
            _ingested_at TIMESTAMP, league VARCHAR, season BIGINT,
            source_season_id VARCHAR
        )"""
    )
    db.execute(
        """CREATE TABLE fbref_lineups (
            match_id VARCHAR, team VARCHAR, player VARCHAR, player_id VARCHAR,
            is_starter BOOLEAN, position VARCHAR, number VARCHAR,
            _ingested_at TIMESTAMP, _batch_id VARCHAR, league VARCHAR,
            season BIGINT, source_season_id VARCHAR
        )"""
    )
    yield db
    db.close()


@pytest.mark.parametrize(
    ("league", "legacy_season", "source_season_id", "silver_season"),
    [
        ("ENG-Premier League", 2025, None, "2526"),
        ("Source Cup", None, "edition-final", "edition-final"),
    ],
)
def test_blank_player_and_lineup_share_resolved_gold_key(
    con,
    league,
    legacy_season,
    source_season_id,
    silver_season,
):
    resolved_id = "noid_example_keeper"
    con.execute(
        "INSERT INTO player_identity VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            resolved_id,
            "Example Keeper",
            "Example FC",
            league,
            silver_season,
            True,
            "synthetic_residual",
            ["match_player_stats", "lineups", "match_keeper_stats"],
        ),
    )
    con.execute(
        """INSERT INTO fbref_match_player_stats VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        (
            "match-1", "", "Example Keeper", "Example FC", "home", "eng ENG",
            "GK", "25-100", "90", "0", "0", "0", "0", "0", "0", "0",
            "0", "0", "0", "0", "0", "0", "0", "0", 0.0, 0.0,
            "2026-07-01", league, legacy_season, source_season_id,
        ),
    )
    con.execute(
        "INSERT INTO fbref_lineups VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "match-1", "Example FC", "Example Keeper", "", True, "GK", "1",
            "2026-07-01", "batch-1", league, legacy_season,
            source_season_id,
        ),
    )

    players = con.execute(
        _translate(PLAYER_SQL.read_text(encoding="utf-8"),
                   "fbref_match_player_stats")
    ).fetch_df()
    lineups = con.execute(
        _translate(LINEUP_SQL.read_text(encoding="utf-8"), "fbref_lineups")
    ).fetch_df()

    assert len(players) == len(lineups) == 1
    assert players.iloc[0]["player_id"] == resolved_id
    assert lineups.iloc[0]["player_id"] == resolved_id
    assert players.iloc[0]["player_id_resolution"] == "synthetic_residual"
    assert lineups.iloc[0]["player_id_resolution"] == "synthetic_residual"
    assert players.iloc[0]["season"] == silver_season
    assert lineups.iloc[0]["season"] == silver_season
    joined = players.merge(lineups, on=["match_id", "player_id"])
    assert len(joined) == 1


def test_gold_keeper_metrics_join_on_the_resolved_player_key():
    sql = GOLD_SQL.read_text(encoding="utf-8")
    assert "gk.player_id  = fb.player_id" in sql
    assert "fb.player_id = xfp.fbref_player_id" in sql


def test_every_fbref_identity_path_keeps_opaque_source_season_ids():
    paths = [
        ROOT / "dags/sql/silver/fbref_player_identity.sql",
        ROOT / "dags/sql/silver/fbref_player_match_stats.sql",
        ROOT / "dags/sql/silver/fbref_match_lineups.sql",
        ROOT / "dags/sql/silver/fbref_keeper_match_stats.sql",
        ROOT / "dags/sql/silver/fbref_player_season_profile.sql",
        ROOT / "dags/sql/silver/fbref_keeper_profile.sql",
        ROOT / "dags/sql/silver/fbref_team_season_profile.sql",
        ROOT / "dags/sql/silver/fbref_match_enriched.sql",
        ROOT / "dags/sql/silver/fbref_match_events.sql",
        ROOT / "dags/sql/silver/fbref_match_officials.sql",
        ROOT / "dags/sql/silver/xref_team.sql.j2",
        ROOT / "dags/sql/silver/xref_manager.sql.j2",
        ROOT / "dags/sql/silver/xref_match.sql",
        ROOT / "dags/sql/silver/xref_referee.sql.j2",
        ROOT / "dags/sql/gold/dim_match.sql.j2",
        ROOT / "dags/sql/gold/fct_manager_stint.sql",
    ]
    for path in paths:
        sql = path.read_text(encoding="utf-8")
        assert "source_season_id" in sql, path.name
        assert "NULLIF(TRIM(" in sql, (
            f"{path.name} must fall back to a nonblank opaque source season"
        )
