"""Executable contract tests for silver.fbref_player_identity (#916/#926)."""

from __future__ import annotations

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags" / "sql" / "silver" / "fbref_player_identity.sql"

pytestmark = pytest.mark.unit


SEASON_TABLES = (
    "fbref_player_stats",
    "fbref_player_shooting",
    "fbref_player_playingtime",
    "fbref_player_misc",
    "fbref_keeper_keeper",
)
TEAM_TABLES = (
    "fbref_match_player_stats",
    "fbref_lineups",
)


def _translated_sql() -> str:
    sql = SQL_PATH.read_text(encoding="utf-8")
    for table in (*SEASON_TABLES, *TEAM_TABLES, "fbref_match_keeper_stats"):
        sql = sql.replace(f"iceberg.bronze.{table}", table)
    sql = sql.replace(", NFD)", ")").replace("normalize(", "strip_accents(")
    sql = sql.replace("REGEXP_LIKE(", "REGEXP_MATCHES(")
    return sql


@pytest.fixture()
def con():
    duckdb = pytest.importorskip("duckdb")
    db = duckdb.connect()
    db.execute("CREATE MACRO to_utf8(x) AS (x)")
    db.execute("CREATE MACRO xxhash64(x) AS (hash(x))")
    db.execute("CREATE MACRO to_hex(x) AS (printf('%x', x))")
    for table in SEASON_TABLES:
        db.execute(
            f"""CREATE TABLE {table} (
                player_id VARCHAR, player VARCHAR, squad VARCHAR,
                league VARCHAR, season BIGINT, source_season_id VARCHAR,
                _ingested_at TIMESTAMP
            )"""
        )
    for table in TEAM_TABLES:
        db.execute(
            f"""CREATE TABLE {table} (
                player_id VARCHAR, player VARCHAR, team VARCHAR,
                league VARCHAR, season BIGINT, source_season_id VARCHAR,
                _ingested_at TIMESTAMP
            )"""
        )
    db.execute(
        """CREATE TABLE fbref_match_keeper_stats (
            player_id VARCHAR, "Player" VARCHAR, team VARCHAR,
            league VARCHAR, season BIGINT, source_season_id VARCHAR,
            _ingested_at TIMESTAMP
        )"""
    )
    yield db
    db.close()


def _season_row(
    con, table: str, player_id, player: str, squad: str, season=2016,
    source_season_id=None,
):
    con.execute(
        f"INSERT INTO {table} VALUES (?, ?, ?, 'ITA-Serie A', ?, ?, TIMESTAMP '2026-07-08')",
        [player_id, player, squad, season, source_season_id],
    )


def _match_row(
    con, table: str, player_id, player: str, team: str, season=2016,
    source_season_id=None,
):
    con.execute(
        f"INSERT INTO {table} VALUES (?, ?, ?, 'ITA-Serie A', ?, ?, TIMESTAMP '2026-07-08')",
        [player_id, player, team, season, source_season_id],
    )


def _rows(con):
    cur = con.execute(_translated_sql())
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def test_recovers_two_native_ids_and_synthesizes_only_residual(con):
    for table in SEASON_TABLES[:4]:
        _season_row(con, table, None, "Giuseppe Borello", "Crotone")
        _season_row(con, table, None, "Giorgio Spizzichino", "Lazio")
        _season_row(con, table, None, "Christian Rutjens", "Benevento", 2017)

    _match_row(con, "fbref_match_player_stats", "498df2a6", "Giuseppe Borello", "Crotone")
    _match_row(con, "fbref_lineups", "ca44da69", "Giorgio Spizzichino", "Lazio")

    by_name = {row["player_name"]: row for row in _rows(con)}
    assert by_name["Giuseppe Borello"]["player_id"] == "498df2a6"
    assert by_name["Giorgio Spizzichino"]["player_id"] == "ca44da69"
    assert by_name["Giuseppe Borello"]["id_resolution"] == "recovered_unique_native"
    assert by_name["Giorgio Spizzichino"]["id_resolution"] == "recovered_unique_native"
    assert by_name["Christian Rutjens"]["player_id"].startswith("noid_")
    assert by_name["Christian Rutjens"]["is_synthetic"] is True
    assert by_name["Christian Rutjens"]["id_resolution"] == "synthetic_residual"


def test_ambiguous_name_refines_by_exact_team_without_fuzzy_merge(con):
    _match_row(con, "fbref_match_player_stats", "11111111", "Alex Smith", "Alpha")
    _match_row(con, "fbref_match_player_stats", "22222222", "Alex Smith", "Beta")
    _season_row(con, "fbref_player_stats", None, "Alex Smith", "Beta")

    rows = [row for row in _rows(con) if row["player_name"] == "Alex Smith"]
    by_team = {(row["team_name"], row["player_id"]): row for row in rows}
    assert ("Alpha", "11111111") in by_team
    assert ("Beta", "22222222") in by_team
    assert not any(row["player_id"].startswith("noid_") for row in rows)


def test_synthetic_id_is_deterministic(con):
    _season_row(con, "fbref_player_stats", None, "Christian Rutjens", "Benevento", 2017)
    first = _rows(con)[0]["player_id"]
    second = _rows(con)[0]["player_id"]
    assert first == second
    assert first.startswith("noid_")


def test_source_single_year_season_is_not_made_split_year(con):
    _season_row(
        con,
        "fbref_player_stats",
        "11111111",
        "Tournament Player",
        "Country",
        season=2024,
        source_season_id="2024",
    )
    assert _rows(con)[0]["season"] == "2024"
