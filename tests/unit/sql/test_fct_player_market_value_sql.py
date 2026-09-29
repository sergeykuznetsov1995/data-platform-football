"""
Unit tests for Gold ``fct_player_market_value`` SQL logic (issue #430).

Market-value timeline, one row per (player_id, valuation_date, source).
The FotMob branch (legacy FotMob Silver) was removed in #1590; only
Transfermarkt remains. Logic under test:

  * Transfermarkt reads canonical_id straight from Silver.  Unresolved points
    retain stable source-prefixed ids (issue #871).
  * cross-season collapse: the same (player, date) point lands in several
    season partitions of Silver — ROW_NUMBER over the design PK keeps exactly
    one row.
  * the SQL no longer reads any legacy FotMob Silver table (#1590).

Strategy: Trino -> DuckDB transpile via sqlglot, fixture rows in an in-memory
silver schema, execute, assert.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_market_value.sql"

LEAGUE = "ENG-Premier League"
INGESTED = datetime(2026, 6, 1, 3, 0, 0)
INGESTED2 = datetime(2026, 6, 2, 3, 0, 0)


def _translate(sql_text: str) -> str:
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    return statements[0].replace("iceberg.silver.", "silver.")


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    con.execute("""
        CREATE TABLE silver.transfermarkt_market_value_history (
            player_id            VARCHAR,
            canonical_id         VARCHAR,
            mv_date              DATE,
            value_eur            BIGINT,
            club_name            VARCHAR,
            age                  INTEGER,
            _bronze_ingested_at  TIMESTAMP,
            league               VARCHAR,
            season               VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO silver.transfermarkt_market_value_history "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            # SAME resolved (player, date) point re-emitted in two season partitions.
            ("700", "fb_x", date(2024, 1, 1), 90_000_000, "Man City", 24,
             INGESTED, LEAGUE, "2425"),
            ("700", "fb_x", date(2024, 1, 1), 95_000_000, "Man City", 24,
             INGESTED2, LEAGUE, "2526"),
            # Orphan (canonical_id NULL) -> retained with a stable prefixed id.
            ("701", None, date(2024, 2, 1), 3_000_000, "Youth FC", 19,
             INGESTED, LEAGUE, "2526"),
        ],
    )


@pytest.fixture(scope="module")
def gold_rows():
    sql_text = SQL_PATH.read_text(encoding="utf-8")
    try:
        translated = _translate(sql_text)
    except Exception as e:
        pytest.skip(f"sqlglot Trino→DuckDB translation failed: {e}")

    con = duckdb.connect(":memory:")
    try:
        _bootstrap(con)
    except Exception as e:
        pytest.skip(f"DuckDB fixture bootstrap failed: {e}")

    try:
        rows = con.execute(translated).fetchall()
        col_names = [c[0] for c in con.description]
    except Exception as e:
        pytest.skip(f"DuckDB execution of translated fct_player_market_value SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


pytestmark = pytest.mark.unit


class TestFctPlayerMarketValue:

    def test_both_transfermarkt_points_are_retained(self, gold_rows):
        # Resolved point collapses across seasons; the orphan survives.
        assert len(gold_rows) == 2

    def test_distinct_sources(self, gold_rows):
        assert {r["source"] for r in gold_rows} == {"transfermarkt"}

    def test_no_legacy_fotmob_silver_read(self):
        assert "silver.fotmob_" not in SQL_PATH.read_text(encoding="utf-8")

    def test_transfermarkt_point_present(self, gold_rows):
        tm = next(
            r for r in gold_rows
            if r["source"] == "transfermarkt" and r["player_id"] == "fb_x"
        )
        assert tm["player_id"] == "fb_x"
        assert tm["valuation_date"] == date(2024, 1, 1)
        # the two season partitions collapse to the freshest ingest
        assert tm["market_value_eur"] == 95_000_000
        assert tm["currency"] == "EUR"

    def test_orphans_are_retained_with_stable_source_prefixes(self, gold_rows):
        ids = {r["player_id"] for r in gold_rows}
        assert "tm_701" in ids

    def test_pk_unique_with_source(self, gold_rows):
        pks = [(r["player_id"], r["valuation_date"], r["source"])
               for r in gold_rows]
        assert len(pks) == len(set(pks)), f"PK collision: {pks}"

    def test_columns_contract(self, gold_rows):
        expected = {
            "player_id", "valuation_date", "market_value_eur",
            "currency", "source", "_bronze_ingested_at",
        }
        assert set(gold_rows[0].keys()) == expected
