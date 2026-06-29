"""
Unit tests for Gold ``fct_player_market_value`` SQL logic (issue #430).

Two-source market-value timeline, one row per
(player_id, valuation_date, source). Logic under test:

  * FotMob bridged to canonical via silver.xref_player (INNER JOIN non-orphan =
    canonical-only); Transfermarkt reads canonical_id straight from Silver,
    canonical-only (WHERE canonical_id IS NOT NULL).
  * `source` in the PK keeps a FotMob and a Transfermarkt point on the same
    (player, date) as two distinct rows.
  * cross-season collapse: the same FotMob (player, date) point lands in
    several season partitions of Silver — ROW_NUMBER over the design PK keeps
    exactly one row.

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

    # xref_player — only the FotMob bridge rows matter here.
    con.execute("""
        CREATE TABLE silver.xref_player (
            canonical_id  VARCHAR,
            source        VARCHAR,
            source_id     VARCHAR,
            league        VARCHAR,
            season        VARCHAR,
            confidence    VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO silver.xref_player VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("fb_x", "fotmob", "500", LEAGUE, "2425", "exact"),
            ("fb_x", "fotmob", "500", LEAGUE, "2526", "exact"),
            # orphan bridge row -> filtered out -> fotmob '900' resolves to nothing.
            ("fm_900", "fotmob", "900", LEAGUE, "2526", "orphan"),
        ],
    )

    con.execute("""
        CREATE TABLE silver.fotmob_player_market_value_history (
            player_id            VARCHAR,
            value_date           DATE,
            market_value_eur     BIGINT,
            currency             VARCHAR,
            _bronze_ingested_at  TIMESTAMP,
            league               VARCHAR,
            season               VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO silver.fotmob_player_market_value_history VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            # SAME (player, date) point re-emitted in two season partitions.
            ("500", date(2024, 1, 1), 100_000_000, "EUR", INGESTED, LEAGUE, "2425"),
            ("500", date(2024, 1, 1), 100_000_000, "EUR", INGESTED2, LEAGUE, "2526"),
            # Orphan FotMob player -> INNER JOIN non-orphan drops it.
            ("900", date(2024, 1, 1), 5_000_000, "EUR", INGESTED, LEAGUE, "2526"),
        ],
    )

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
            # Resolved canonical — same (player, date) as FotMob, different source.
            ("700", "fb_x", date(2024, 1, 1), 95_000_000, "Man City", 24,
             INGESTED, LEAGUE, "2526"),
            # Orphan (canonical_id NULL) -> dropped (canonical-only contract).
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

    def test_exactly_two_rows(self, gold_rows):
        # fotmob (cross-season collapsed) + transfermarkt, both for fb_x@2024-01-01.
        assert len(gold_rows) == 2

    def test_distinct_sources(self, gold_rows):
        assert {r["source"] for r in gold_rows} == {"fotmob", "transfermarkt"}

    def test_fotmob_point_collapsed_cross_season(self, gold_rows):
        fm = [r for r in gold_rows if r["source"] == "fotmob"]
        assert len(fm) == 1, "the two season partitions must collapse to one row"
        assert fm[0]["player_id"] == "fb_x"
        assert fm[0]["valuation_date"] == date(2024, 1, 1)
        assert fm[0]["market_value_eur"] == 100_000_000

    def test_transfermarkt_point_present(self, gold_rows):
        tm = next(r for r in gold_rows if r["source"] == "transfermarkt")
        assert tm["player_id"] == "fb_x"
        assert tm["market_value_eur"] == 95_000_000
        assert tm["currency"] == "EUR"

    def test_orphans_dropped_canonical_only(self, gold_rows):
        # FotMob '900' (orphan bridge) and TM '701' (NULL canonical) are gone.
        assert all(r["player_id"].startswith("fb_") for r in gold_rows)

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
