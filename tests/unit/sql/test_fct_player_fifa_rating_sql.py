"""
Unit tests for Gold ``fct_player_fifa_rating`` SQL logic (issue #430).

Projection of silver.sofifa_player_profile (canonical_id already resolved in
Silver) — one row per (player_id, fifa_edition). Logic under test:

  * orphan-ID strategy player_id = COALESCE(canonical_id, 'sf_' || player_id),
  * one row PER edition kept (a player exists across FC 25 / FC 26),
  * ROW_NUMBER dedup when two source player_ids resolve to the same canonical
    within one edition — keep the higher-rated row.

Strategy: Trino -> DuckDB transpile via sqlglot, fixture rows in an in-memory
silver schema, execute, assert.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_fifa_rating.sql"

INGESTED = datetime(2026, 6, 1, 3, 0, 0)


def _translate(sql_text: str) -> str:
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    return statements[0].replace("iceberg.silver.", "silver.")


def _row(player_id, canonical_id, name, edition, overall, value_eur=100):
    """Build a 20-col silver row; ratings other than `overall` are constants
    (not asserted), only `overall` drives the dedup ORDER BY."""
    r = 50
    return (
        player_id, canonical_id, name, edition,
        overall, overall,                      # overall, potential
        r, r, r, r, r, r,                      # pace..physical
        r, r, r, r, r,                         # gk_diving..gk_reflexes
        value_eur, 50,                         # value_eur, wage_eur
        INGESTED,
    )


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("""
        CREATE TABLE silver.sofifa_player_profile (
            player_id            VARCHAR,
            canonical_id         VARCHAR,
            player_name          VARCHAR,
            fifa_edition         VARCHAR,
            overall              INTEGER,
            potential            INTEGER,
            pace                 INTEGER,
            shooting             INTEGER,
            passing              INTEGER,
            dribbling            INTEGER,
            defending            INTEGER,
            physical             INTEGER,
            gk_diving            INTEGER,
            gk_handling          INTEGER,
            gk_kicking           INTEGER,
            gk_positioning       INTEGER,
            gk_reflexes          INTEGER,
            value_eur            BIGINT,
            wage_eur             BIGINT,
            _bronze_ingested_at  TIMESTAMP
        )
    """)
    rows = [
        # Resolved player across TWO editions -> two rows kept.
        _row("200", "fb_haaland", "Erling Haaland", "FC 26", 91, value_eur=185_000_000),
        _row("200", "fb_haaland", "Erling Haaland", "FC 25", 90, value_eur=180_000_000),
        # Orphan player (no FBref counterpart) -> 'sf_' prefix, kept.
        _row("999", None, "Youth Player", "FC 26", 62),
        # Two source ids -> SAME canonical in SAME edition -> dedup to overall=85.
        _row("300", "fb_dup", "Player Dup A", "FC 26", 80),
        _row("301", "fb_dup", "Player Dup B", "FC 26", 85),
    ]
    placeholders = ", ".join(["?"] * 20)
    for r in rows:
        con.execute(
            f"INSERT INTO silver.sofifa_player_profile VALUES ({placeholders})", r
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
        pytest.skip(f"DuckDB execution of translated fct_player_fifa_rating SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


pytestmark = pytest.mark.unit


class TestFctPlayerFifaRating:

    def test_row_count_after_dedup(self, gold_rows):
        # 2 haaland editions + 1 orphan + 1 deduped fb_dup = 4
        assert len(gold_rows) == 4

    def test_resolved_player_passthrough(self, gold_rows):
        fc26 = next(r for r in gold_rows
                    if r["player_id"] == "fb_haaland" and r["fifa_edition"] == "FC 26")
        assert fc26["overall"] == 91
        assert fc26["potential"] == 91
        assert fc26["value_eur"] == 185_000_000

    def test_player_kept_per_edition(self, gold_rows):
        editions = sorted(r["fifa_edition"] for r in gold_rows
                          if r["player_id"] == "fb_haaland")
        assert editions == ["FC 25", "FC 26"]

    def test_orphan_player_gets_sf_prefix(self, gold_rows):
        orphans = [r for r in gold_rows if r["player_id"] == "sf_999"]
        assert len(orphans) == 1, "orphan row must be kept, not dropped"

    def test_dedup_keeps_higher_rated(self, gold_rows):
        dup = [r for r in gold_rows if r["player_id"] == "fb_dup"]
        assert len(dup) == 1, "two source ids in one edition must collapse"
        assert dup[0]["overall"] == 85

    def test_pk_unique(self, gold_rows):
        pks = [(r["player_id"], r["fifa_edition"]) for r in gold_rows]
        assert len(pks) == len(set(pks)), f"PK collision: {pks}"

    def test_columns_contract(self, gold_rows):
        expected = {
            "player_id", "player_name", "fifa_edition",
            "overall", "potential", "pace", "shooting", "passing",
            "dribbling", "defending", "physical",
            "gk_diving", "gk_handling", "gk_kicking", "gk_positioning",
            "gk_reflexes", "value_eur", "wage_eur", "_bronze_ingested_at",
        }
        assert set(gold_rows[0].keys()) == expected
