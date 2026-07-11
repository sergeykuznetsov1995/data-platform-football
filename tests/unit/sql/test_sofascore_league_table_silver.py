"""
Unit tests for Silver ``sofascore_league_table`` SQL logic (#702).

Conform-only projection of ``bronze.sofascore_league_table`` (APPEND-mode
snapshots): dedup to the latest ``_ingested_at`` per (league, season, group, team),
cast numerics to INTEGER, rename ``team`` → ``team_name`` and ``_ingested_at``
→ ``_bronze_ingested_at``, emit the season slug as-is (#404). The dedup logic
moved here from gold.fct_standings (#702 — Gold one-hop from Silver).

Trino → DuckDB transpile via sqlglot; fixture rows in-memory.
"""

from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "sofascore_league_table.sql"


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


def _translate(sql_text: str) -> str:
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    return statements[0].replace("iceberg.bronze.", "bronze.")


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("""
        CREATE TABLE bronze.sofascore_league_table (
            league VARCHAR, season VARCHAR, team VARCHAR, "group" VARCHAR,
            mp BIGINT, w BIGINT, d BIGINT, l BIGINT,
            gf BIGINT, ga BIGINT, gd BIGINT, pts BIGINT,
            _ingested_at TIMESTAMP
        )
    """)
    # 2 teams × 2 snapshots + one blank-name row (must be filtered out).
    con.execute("""
        INSERT INTO bronze.sofascore_league_table VALUES
        -- older snapshots (discarded)
        ('ENG-Premier League', '2425', 'Manchester City', '__total__', 10, 8, 1, 1, 30, 10, 20, 25,
         TIMESTAMP '2026-04-26 06:00:00'),
        ('ENG-Premier League', '2425', 'Liverpool',       '__total__', 10, 7, 2, 1, 25, 12, 13, 23,
         TIMESTAMP '2026-04-26 06:00:00'),
        -- latest snapshots (win)
        ('ENG-Premier League', '2425', 'Manchester City', '__total__', 12, 10, 1, 1, 35, 11, 24, 31,
         TIMESTAMP '2026-04-27 06:00:00'),
        ('ENG-Premier League', '2425', 'Liverpool',       '__total__', 12, 8, 2, 2, 28, 14, 14, 26,
         TIMESTAMP '2026-04-27 06:00:00'),
        -- blank team name -> filtered
        ('ENG-Premier League', '2425', '   ',             '__total__', 12, 0, 0, 12, 0, 50, -50, 0,
         TIMESTAMP '2026-04-27 06:00:00')
    """)


@pytest.fixture(scope="module")
def silver_rows():
    sql_text = SQL_PATH.read_text(encoding="utf-8")
    try:
        translated = _translate(sql_text)
    except Exception as e:
        pytest.skip(f"sqlglot Trino→DuckDB translation failed: {e}")

    con = duckdb.connect(":memory:")
    try:
        _bootstrap(con)
        rows = con.execute(translated).fetchall()
        col_names = [c[0] for c in con.description]
    except Exception as e:
        pytest.skip(f"DuckDB execution of translated silver SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


@pytest.mark.unit
class TestSofascoreLeagueTableSilver:
    def test_dedup_and_blank_filter(self, silver_rows):
        """5 input rows → 2 (latest per team; blank name dropped)."""
        assert len(silver_rows) == 2
        assert {r["team_name"] for r in silver_rows} == {"Manchester City", "Liverpool"}

    def test_latest_snapshot_wins(self, silver_rows):
        mci = next(r for r in silver_rows if r["team_name"] == "Manchester City")
        assert mci["played"] == 12 and mci["points"] == 31

    def test_columns_renamed_and_cast(self, silver_rows):
        mci = next(r for r in silver_rows if r["team_name"] == "Manchester City")
        # rename: team -> team_name, _ingested_at -> _bronze_ingested_at
        assert "team_name" in mci and "_bronze_ingested_at" in mci
        assert "team" not in mci and "_ingested_at" not in mci
        # cast to int
        assert isinstance(mci["points"], int)

    def test_season_slug_as_is(self, silver_rows):
        """#404: SofaScore bronze season is already slug — emitted unchanged."""
        assert {r["season"] for r in silver_rows} == {"2425"}

    def test_total_scope_sentinel_is_not_exposed(self, silver_rows):
        assert {r["group_id"] for r in silver_rows} == {None}
        assert {r["standing_scope"] for r in silver_rows} == {"__total__"}
