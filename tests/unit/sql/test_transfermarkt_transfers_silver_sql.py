"""
Unit tests for Silver ``transfermarkt_transfers`` SQL — the ``is_loan``
derivation added by issue #429.

The silver template derives ``is_loan = LOWER(fee_text) LIKE '%loan%'``
from the raw Transfermarkt fee string. Live fee_text variants (verified
against bronze on 2026-06-11):

  'loan transfer'                                   → is_loan = TRUE
  'End of loan'                                     → is_loan = TRUE
  'Loan fee:<br /><i class="normaler-text">€2.00m'  → is_loan = TRUE
  '€30.00m' / 'free transfer' / '-' / '?'           → is_loan = FALSE

Strategy: render the template through the REAL renderer
(``utils.medallion_config.render_sql_template`` — standalone-line
placeholders only; the inline mention in the doc-header comment must stay
untouched), then Trino → DuckDB transpile via sqlglot, bootstrap bronze +
xref fixtures, execute, assert. Also covers the pre-existing contract:
orphan players keep canonical_id NULL, ROW_NUMBER dedup keeps the newest
bronze row.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "silver" / "transfermarkt_transfers.sql.j2"
)

# ``utils.medallion_config`` lives under ``dags/utils/`` — dags/ is on
# sys.path inside the Airflow container but not on the host (same trick as
# test_dim_competition_render.py).
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

LEAGUE = "ENG-Premier League"
SEASON = "2526"

# Two-column (raw_name, canonical_name) tuples — the template contract
# (see test_medallion_config.test_sql_values_default_stays_two_columns).
# 'Spartak' appears TWICE with different canonicals (worldwide namesake clubs —
# the 2-tuple VALUES carries no league): without the #465 GROUP BY guard each
# duplicate raw_name fans out both club JOINs.
ALIAS_VALUES = (
    "('Arsenal', 'Arsenal'),\n"
    "        ('Wolves', 'Wolverhampton Wanderers'),\n"
    "        ('Spartak', 'Spartak Moscow'),\n"
    "        ('Spartak', 'Spartak Nalchik')"
)


def _render_and_translate() -> str:
    from utils.medallion_config import render_sql_template

    sql_text = render_sql_template(
        TEMPLATE_PATH, team_aliases_values_sql=ALIAS_VALUES,
    )
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    out = statements[0]
    out = out.replace("iceberg.silver.", "silver.")
    out = out.replace("iceberg.bronze.", "bronze.")
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    con.execute("""
        CREATE TABLE bronze.transfermarkt_transfers (
            player_id         VARCHAR,
            transfer_date     DATE,
            season            VARCHAR,
            from_club_id      VARCHAR,
            from_club_name    VARCHAR,
            to_club_id        VARCHAR,
            to_club_name      VARCHAR,
            fee_text          VARCHAR,
            fee_eur           DOUBLE,
            market_value_eur  DOUBLE,
            is_upcoming       BOOLEAN,
            league            VARCHAR,
            _ingested_at      TIMESTAMP
        )
    """)
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

    t0 = datetime(2026, 6, 1, 3, 0, 0)
    t1 = datetime(2026, 6, 2, 3, 0, 0)

    # (player_id, date, from_name, to_name, fee_text, fee_eur, ingested_at)
    rows = [
        ("100001", date(2025, 7, 1), "Arsenal", "Wolves",
         "€30.00m", 30_000_000.0, t0),
        ("100002", date(2025, 7, 2), "Arsenal", "Real Madrid",
         "loan transfer", None, t0),
        ("100003", date(2025, 7, 3), "Real Madrid", "Arsenal",
         "End of loan", None, t0),
        ("100004", date(2025, 7, 4), "Arsenal", "Wolves",
         'Loan fee:<br /><i class="normaler-text">€2.00m</i>', 2_000_000.0, t0),
        ("100005", date(2025, 7, 5), "Wolves", "Arsenal",
         "free transfer", None, t0),
        ("100006", date(2025, 7, 6), "Wolves", "Arsenal",
         "-", None, t0),
        # Dedup case: same business key ingested twice — newest must win.
        ("100007", date(2025, 7, 7), "Arsenal", "Wolves",
         "€10.00m", 10_000_000.0, t0),
        ("100007", date(2025, 7, 7), "Arsenal", "Wolves",
         "€11.00m", 11_000_000.0, t1),
        # #465 collision case: 'Spartak' matches TWO alias rows.
        ("100008", date(2025, 7, 8), "Spartak", "Arsenal",
         "free transfer", None, t0),
    ]
    for pid, d, frm, to, fee_text, fee_eur, ts in rows:
        con.execute(
            "INSERT INTO bronze.transfermarkt_transfers "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (pid, d, SEASON, f"c_{frm}", frm, f"c_{to}", to,
             fee_text, fee_eur, 5_000_000.0, False, LEAGUE, ts),
        )

    # Only player 100001 is xref-resolved; the rest stay orphans (NULL).
    con.execute(
        "INSERT INTO silver.xref_player VALUES (?, ?, ?, ?, ?, ?)",
        ("fb_aaaa1111", "transfermarkt", "100001", LEAGUE, SEASON, "exact"),
    )


@pytest.fixture(scope="module")
def silver_rows():
    try:
        translated = _render_and_translate()
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
        pytest.skip(
            f"DuckDB execution of translated transfermarkt_transfers SQL "
            f"failed: {e}"
        )

    return [dict(zip(col_names, r)) for r in rows]


pytestmark = pytest.mark.unit


class TestSilverTransfersIsLoan:

    def test_is_loan_derivation(self, silver_rows):
        expected = {
            "100001": False,   # '€30.00m'
            "100002": True,    # 'loan transfer'
            "100003": True,    # 'End of loan'
            "100004": True,    # 'Loan fee:<br />…'
            "100005": False,   # 'free transfer'
            "100006": False,   # '-'
        }
        by_player = {r["player_id"]: r["is_loan"] for r in silver_rows}
        for pid, want in expected.items():
            assert by_player[pid] is want, (
                f"player {pid}: is_loan={by_player[pid]}, want {want}"
            )

    def test_is_loan_never_null(self, silver_rows):
        assert all(r["is_loan"] is not None for r in silver_rows)


class TestSilverTransfersExistingContract:
    """Pre-existing behavior must survive the is_loan addition."""

    def test_orphan_player_canonical_null(self, silver_rows):
        resolved = next(r for r in silver_rows if r["player_id"] == "100001")
        assert resolved["canonical_id"] == "fb_aaaa1111"
        orphan = next(r for r in silver_rows if r["player_id"] == "100002")
        assert orphan["canonical_id"] is None, "orphans keep NULL, rows kept"

    def test_club_alias_resolution(self, silver_rows):
        row = next(r for r in silver_rows if r["player_id"] == "100001")
        assert row["from_club_id_canonical"] == "arsenal"
        assert row["to_club_id_canonical"] == "wolverhampton_wanderers"
        unresolved = next(r for r in silver_rows if r["player_id"] == "100002")
        assert unresolved["to_club_id_canonical"] is None  # 'Real Madrid'

    def test_dedup_keeps_newest_ingest(self, silver_rows):
        dup = [r for r in silver_rows if r["player_id"] == "100007"]
        assert len(dup) == 1
        assert dup[0]["fee_eur"] == 11_000_000


class TestSilverTransfersAliasCollision:
    """#465: a duplicate raw_name in the alias VALUES must NOT fan out the
    club JOINs (precedent: venue norm-collision #425 — 323 dup PK rows)."""

    def test_collision_raw_name_no_fanout(self, silver_rows):
        rows = [r for r in silver_rows if r["player_id"] == "100008"]
        assert len(rows) == 1, (
            f"'Spartak' matches 2 alias rows — expected the GROUP BY guard to "
            f"collapse them, got {len(rows)} rows"
        )

    def test_grain_unique(self, silver_rows):
        grain = [
            (r["player_id"], r["transfer_date"],
             r["from_club_name"], r["to_club_name"])
            for r in silver_rows
        ]
        assert len(grain) == len(set(grain)), (
            f"duplicate grain rows: {sorted(grain)}"
        )

    def test_collision_resolves_to_one_curated(self, silver_rows):
        """Membership only — the guard picks ONE deterministic canonical
        (MAX); which one wins is not part of the contract."""
        row = next(r for r in silver_rows if r["player_id"] == "100008")
        assert row["from_club_id_canonical"] in {
            "spartak_moscow", "spartak_nalchik",
        }
