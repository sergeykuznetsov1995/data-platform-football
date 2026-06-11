"""
Unit tests for Gold ``fct_transfer`` SQL logic (issue #429).

fct_transfer is a pure projection of silver.transfermarkt_transfers — no
JOINs — so the row count must exactly equal the silver row count (DoD:
no rows lost). The key logic under test is the orphan-ID strategy:
canonical ids may be NULL (≈18% of players, most clubs), and the design
says KEEP those rows with a prefixed orphan id instead of NULL:

  player_id    = COALESCE(canonical_id, 'tm_' || player_id)
  from_team_id = COALESCE(from_club_id_canonical, 'tm_' || slug(from_club_name))
  to_team_id   = COALESCE(to_club_id_canonical,   'tm_' || slug(to_club_name))

slug() is the same LOWER(REGEXP_REPLACE(.., '[^a-zA-Z0-9]+', '_')) formula
used by team_aliases — resolved clubs therefore match dim_team.team_id.

Strategy: Trino → DuckDB transpile via sqlglot, fixture rows in an
in-memory silver schema, execute, assert. Fixture mirrors live values
(canonical 'fb_'-prefixed players, slugged clubs, season slug '2526').
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_transfer.sql"

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
        CREATE TABLE silver.transfermarkt_transfers (
            canonical_id            VARCHAR,
            player_id               VARCHAR,
            transfer_date           DATE,
            from_club_id_canonical  VARCHAR,
            to_club_id_canonical    VARCHAR,
            from_club_name          VARCHAR,
            to_club_name            VARCHAR,
            fee_eur                 BIGINT,
            market_value_eur        BIGINT,
            is_upcoming             BOOLEAN,
            is_loan                 BOOLEAN,
            _bronze_ingested_at     TIMESTAMP,
            league                  VARCHAR,
            season                  VARCHAR
        )
    """)

    rows = [
        # 1. Fully resolved permanent transfer.
        ("fb_aaaa1111", "433177", date(2025, 7, 1), "chelsea", "arsenal",
         "Chelsea", "Arsenal", 30_000_000, 50_000_000, False, False),
        # 2. Orphan player on loan to an unresolved (non-APL) club.
        (None, "999001", date(2025, 8, 15), "arsenal", None,
         "Arsenal", "Real Madrid", None, 5_000_000, False, True),
        # 3. Upcoming transfer, unresolved from-club.
        ("fb_bbbb2222", "555002", date(2026, 7, 1), None, "liverpool",
         "Bayern Munich", "Liverpool", 80_000_000, 90_000_000, True, False),
        # 4+5. Same orphan player, TWO moves on the SAME date between
        # unresolved clubs (end of loan + new loan). Without the orphan
        # club slugs both rows would collapse to (tm_999001, date, NULL,
        # NULL) — the very PK collision the COALESCE exists to prevent.
        (None, "999001", date(2025, 6, 30), None, None,
         "Norwich City", "Sporting CP", None, 4_000_000, False, True),
        (None, "999001", date(2025, 6, 30), None, None,
         "Sporting CP", "Norwich City", None, 4_000_000, False, True),
    ]
    for r in rows:
        con.execute(
            "INSERT INTO silver.transfermarkt_transfers "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        pytest.skip(f"DuckDB execution of translated fct_transfer SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


pytestmark = pytest.mark.unit


class TestFctTransfer:

    def test_row_parity_no_rows_lost(self, gold_rows):
        """Pure projection: gold row count == silver row count (DoD)."""
        assert len(gold_rows) == 5

    def test_resolved_ids_pass_through(self, gold_rows):
        row = next(r for r in gold_rows if r["player_id"] == "fb_aaaa1111")
        assert row["from_team_id"] == "chelsea"
        assert row["to_team_id"] == "arsenal"
        assert row["fee_eur"] == 30_000_000
        assert row["market_value_at_transfer_eur"] == 50_000_000
        assert row["is_loan"] is False
        assert row["is_upcoming"] is False

    def test_orphan_player_gets_tm_prefix(self, gold_rows):
        orphans = [r for r in gold_rows if r["player_id"] == "tm_999001"]
        assert len(orphans) == 3, "orphan rows must be kept, not dropped"

    def test_orphan_club_gets_tm_slug(self, gold_rows):
        row = next(r for r in gold_rows if r["transfer_date"] == date(2025, 8, 15))
        assert row["from_team_id"] == "arsenal"          # resolved side intact
        assert row["to_team_id"] == "tm_real_madrid"     # orphan slug
        row3 = next(r for r in gold_rows if r["is_upcoming"] is True)
        assert row3["from_team_id"] == "tm_bayern_munich"
        assert row3["to_team_id"] == "liverpool"

    def test_pk_unique_including_same_day_orphan_pair(self, gold_rows):
        pks = [
            (r["player_id"], r["transfer_date"],
             r["from_team_id"], r["to_team_id"])
            for r in gold_rows
        ]
        assert len(pks) == len(set(pks)), f"PK collision: {pks}"

    def test_pk_columns_never_null(self, gold_rows):
        for r in gold_rows:
            for col in ("player_id", "transfer_date",
                        "from_team_id", "to_team_id"):
                assert r[col] is not None, f"NULL in PK column {col}: {r}"

    def test_loan_flags_pass_through(self, gold_rows):
        loans = [r for r in gold_rows if r["is_loan"]]
        assert len(loans) == 3

    def test_columns_contract(self, gold_rows):
        expected = {
            "player_id", "transfer_date", "from_team_id", "to_team_id",
            "from_club_name", "to_club_name", "fee_eur",
            "market_value_at_transfer_eur", "is_loan", "is_upcoming",
            "_bronze_ingested_at", "league", "season",
        }
        assert set(gold_rows[0].keys()) == expected
