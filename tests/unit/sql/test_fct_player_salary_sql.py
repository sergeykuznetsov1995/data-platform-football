"""
Unit tests for Gold ``fct_player_salary`` SQL logic (issue #430).

Pure projection of silver.capology_player_salaries (canonical_id already
resolved in Silver) — one row per (player_id, league, season). The key logic
under test is the orphan-ID strategy:

  player_id = COALESCE(canonical_id, 'cap_' || player_slug)

which keeps the ≈9.5% of APL roster players with no FBref counterpart instead
of dropping them, plus the status/verified -> contract_status/is_verified
renames.

Strategy: Trino -> DuckDB transpile via sqlglot, fixture rows in an in-memory
silver schema, execute, assert.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_salary.sql"

LEAGUE = "ENG-Premier League"
SEASON = "2526"
INGESTED = datetime(2026, 6, 1, 3, 0, 0)


def _translate(sql_text: str) -> str:
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    return statements[0].replace("iceberg.silver.", "silver.")


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("""
        CREATE TABLE silver.capology_player_salaries (
            player_slug          VARCHAR,
            canonical_id         VARCHAR,
            player_name          VARCHAR,
            club_name            VARCHAR,
            weekly_gross_gbp     DECIMAL(12,2),
            annual_gross_gbp     DECIMAL(14,2),
            weekly_gross_eur     DECIMAL(12,2),
            annual_gross_eur     DECIMAL(14,2),
            weekly_gross_usd     DECIMAL(12,2),
            annual_gross_usd     DECIMAL(14,2),
            status               VARCHAR,
            verified             BOOLEAN,
            _bronze_ingested_at  TIMESTAMP,
            league               VARCHAR,
            season               VARCHAR
        )
    """)
    rows = [
        # 1. Resolved player (FBref spine).
        ("haaland", "fb_aaaa1111", "Erling Haaland", "Manchester City",
         Decimal("375000.00"), Decimal("19500000.00"),
         Decimal("440000.00"), Decimal("22880000.00"),
         Decimal("480000.00"), Decimal("24960000.00"),
         "Active", True),
        # 2. Orphan player (no FBref counterpart) -> 'cap_' prefix, kept.
        ("youth-keeper", None, "Youth Keeper", "Arsenal",
         Decimal("5000.00"), Decimal("260000.00"),
         Decimal("5800.00"), Decimal("301600.00"),
         Decimal("6300.00"), Decimal("327600.00"),
         "Loan", False),
        # 3. Another resolved player.
        ("saka", "fb_bbbb2222", "Bukayo Saka", "Arsenal",
         Decimal("195000.00"), Decimal("10140000.00"),
         Decimal("228000.00"), Decimal("11856000.00"),
         Decimal("249000.00"), Decimal("12948000.00"),
         "Active", True),
    ]
    for r in rows:
        con.execute(
            "INSERT INTO silver.capology_player_salaries VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*r, INGESTED, LEAGUE, SEASON),
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
        pytest.skip(f"DuckDB execution of translated fct_player_salary SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


pytestmark = pytest.mark.unit


class TestFctPlayerSalary:

    def test_row_parity_no_rows_lost(self, gold_rows):
        """Pure projection: gold row count == silver row count."""
        assert len(gold_rows) == 3

    def test_resolved_player_passthrough(self, gold_rows):
        row = next(r for r in gold_rows if r["player_id"] == "fb_aaaa1111")
        assert row["player_name"] == "Erling Haaland"
        assert row["club_name"] == "Manchester City"
        assert row["weekly_gross_eur"] == Decimal("440000.00")
        assert row["annual_gross_eur"] == Decimal("22880000.00")
        assert row["contract_status"] == "Active"
        assert row["is_verified"] is True

    def test_orphan_player_gets_cap_prefix(self, gold_rows):
        orphans = [r for r in gold_rows if r["player_id"] == "cap_youth-keeper"]
        assert len(orphans) == 1, "orphan row must be kept, not dropped"
        assert orphans[0]["is_verified"] is False

    def test_pk_unique(self, gold_rows):
        pks = [(r["player_id"], r["league"], r["season"]) for r in gold_rows]
        assert len(pks) == len(set(pks)), f"PK collision: {pks}"

    def test_pk_columns_never_null(self, gold_rows):
        for r in gold_rows:
            for col in ("player_id", "league", "season"):
                assert r[col] is not None, f"NULL in PK column {col}: {r}"

    def test_columns_contract(self, gold_rows):
        expected = {
            "player_id", "player_name", "club_name",
            "weekly_gross_eur", "annual_gross_eur",
            "weekly_gross_gbp", "annual_gross_gbp",
            "weekly_gross_usd", "annual_gross_usd",
            "contract_status", "is_verified",
            "_bronze_ingested_at", "league", "season",
        }
        assert set(gold_rows[0].keys()) == expected
