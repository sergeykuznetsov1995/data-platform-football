"""
Unit tests for Silver ``espn_matchsheet`` SQL logic (issue #735).

Conform-only venue projection of ``bronze.espn_matchsheet`` (APPEND-mode
backfill snapshots): dedup to the latest ``_ingested_at`` per
(trim(venue), league, season, game), derive ``match_date`` from the first 10
chars of ``game``, rename ``_ingested_at`` → ``_bronze_ingested_at``, emit the
season slug as-is (#404). The dedup logic moved here from gold.dim_venue
(#735 — Gold one-hop from Silver, audit #704).

Trino → DuckDB transpile via sqlglot; fixture rows in-memory.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "espn_matchsheet.sql"


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
        CREATE TABLE bronze.espn_matchsheet (
            venue VARCHAR, game VARCHAR, league VARCHAR, season VARCHAR,
            _ingested_at TIMESTAMP
        )
    """)
    # One game with two snapshots (latest _ingested_at wins) + one single game
    # + NULL/blank venue rows (must be filtered).
    con.execute("""
        INSERT INTO bronze.espn_matchsheet VALUES
        -- Etihad: older snapshot (discarded)
        ('Etihad Stadium', '2024-08-15-MCI-CHE', 'ENG-Premier League', '2425',
         TIMESTAMP '2026-04-27 06:00:00'),
        -- Etihad: latest snapshot (wins) — note padded name to prove trim()
        ('  Etihad Stadium  ', '2024-08-15-MCI-CHE', 'ENG-Premier League', '2425',
         TIMESTAMP '2026-04-27 09:00:00'),
        -- Goodison: single snapshot
        ('Goodison Park', '2024-08-22-EVE-LIV', 'ENG-Premier League', '2425',
         TIMESTAMP '2026-04-27 09:00:00'),
        -- NULL venue -> filtered
        (NULL, '2024-08-23-BRE-ARS', 'ENG-Premier League', '2425',
         TIMESTAMP '2026-04-27 09:00:00'),
        -- blank venue -> filtered
        ('   ', '2024-08-24-NEW-TOT', 'ENG-Premier League', '2425',
         TIMESTAMP '2026-04-27 09:00:00')
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
class TestEspnMatchsheetSilver:
    def test_dedup_and_blank_filter(self, silver_rows):
        """5 input rows → 2 (latest snapshot per game; NULL/blank venue dropped)."""
        assert len(silver_rows) == 2
        assert {r["venue"] for r in silver_rows} == {"Etihad Stadium", "Goodison Park"}

    def test_venue_trimmed(self, silver_rows):
        """trim(venue): the padded '  Etihad Stadium  ' snapshot collapses with
        the unpadded one and surfaces a clean name."""
        venues = {r["venue"] for r in silver_rows}
        assert "Etihad Stadium" in venues
        assert "  Etihad Stadium  " not in venues

    def test_latest_snapshot_wins(self, silver_rows):
        """Dedup keeps the row with the most recent _ingested_at."""
        etihad = next(r for r in silver_rows if r["venue"] == "Etihad Stadium")
        assert etihad["_bronze_ingested_at"] == datetime.datetime(2026, 4, 27, 9, 0, 0)

    def test_match_date_derived_from_game(self, silver_rows):
        """match_date = try_cast(substr(game, 1, 10) as date)."""
        etihad = next(r for r in silver_rows if r["venue"] == "Etihad Stadium")
        goodison = next(r for r in silver_rows if r["venue"] == "Goodison Park")
        assert etihad["match_date"] == datetime.date(2024, 8, 15)
        assert goodison["match_date"] == datetime.date(2024, 8, 22)

    def test_columns_renamed(self, silver_rows):
        """rename: _ingested_at -> _bronze_ingested_at; game NOT emitted."""
        row = silver_rows[0]
        assert "_bronze_ingested_at" in row
        assert "_ingested_at" not in row
        assert "game" not in row

    def test_season_slug_as_is(self, silver_rows):
        """#404: ESPN bronze season is already a slug — emitted unchanged."""
        assert {r["season"] for r in silver_rows} == {"2425"}

    def test_partition_keys_present(self, silver_rows):
        """league + season survive as the trailing partition keys."""
        row = silver_rows[0]
        assert row["league"] == "ENG-Premier League"
        assert "season" in row
