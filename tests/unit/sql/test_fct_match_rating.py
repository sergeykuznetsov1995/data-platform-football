"""
Unit tests for the SofaScore → fct_match_rating pipeline (E4.5).

Pipeline under test:
  bronze.sofascore_player_ratings  ─┐
  bronze.sofascore_schedule         ├─► silver.sofascore_player_ratings
  silver.xref_player                ─┘                  ↓
  gold.dim_match (bridge target)                       gold.fct_match_rating

Synthetic dataset:
  * 5 ratings in bronze.sofascore_player_ratings:
      - 4 valid (range 0.1–10.0): 6.5 / 7.4 / 8.3 / 9.1
      - 1 outlier 11.5 (>10.0)  → silver typing maps to NULL → row preserved
        with rating IS NULL (silver does NOT drop the row, just nulls the value)
  * 3 player_id resolvable through xref_player → canonical from xref
  * 2 player_id orphan (no xref_player row) → fall-back 'ss_<raw>'

The orphan handling test verifies the COALESCE fallback in silver:
  COALESCE(xp.canonical_id, 'ss_' || st.player_id)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "sofascore_player_ratings.sql"
GOLD_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_match_rating.sql"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Translation
# ---------------------------------------------------------------------------


def _collapse_call(sql: str, fn_name: str) -> str:
    out = []
    i = 0
    n = len(sql)
    while i < n:
        if sql[i:i + len(fn_name)].lower() == fn_name.lower():
            j = i + len(fn_name)
            while j < n and sql[j] in " \t\n\r":
                j += 1
            if j < n and sql[j] == "(":
                depth = 1
                j += 1
                inner_start = j
                while j < n and depth > 0:
                    if sql[j] == "(":
                        depth += 1
                    elif sql[j] == ")":
                        depth -= 1
                    if depth == 0:
                        break
                    j += 1
                out.append(sql[inner_start:j])
                i = j + 1
                continue
        out.append(sql[i])
        i += 1
    return "".join(out)


_ICEBERG_TO_LOCAL = {
    "iceberg.bronze.sofascore_player_ratings": "bronze_sofascore_player_ratings",
    "iceberg.bronze.sofascore_schedule":       "bronze_sofascore_schedule",
    "iceberg.silver.xref_player":              "silver_xref_player",
    "iceberg.silver.sofascore_player_ratings": "silver_sofascore_player_ratings",
    "iceberg.gold.dim_match":                  "gold_dim_match",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    sql = _collapse_call(sql, "to_utf8")
    sql = _collapse_call(sql, "to_hex")
    sql = re.sub(r"\bxxhash64\b", "md5", sql, flags=re.IGNORECASE)
    sql = sql.replace("timestamp(6)", "timestamp")
    return sql


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


@pytest.fixture(autouse=True)
def _reset_schemas(duck_conn):
    for tbl in (
        "bronze_sofascore_player_ratings", "bronze_sofascore_schedule",
        "silver_xref_player", "silver_sofascore_player_ratings",
        "gold_dim_match",
    ):
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    duck_conn.execute(
        """
        CREATE TABLE bronze_sofascore_player_ratings (
            match_id      VARCHAR,
            player_id     VARCHAR,
            team_side     VARCHAR,
            rating        DOUBLE,
            position      VARCHAR,
            league        VARCHAR,
            season        VARCHAR,
            _ingested_at  TIMESTAMP
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE bronze_sofascore_schedule (
            game_id       BIGINT,
            date          TIMESTAMP,
            home_team     VARCHAR,
            away_team     VARCHAR,
            league        VARCHAR,
            season        VARCHAR,
            _ingested_at  TIMESTAMP
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE silver_xref_player (
            source        VARCHAR,
            source_id     VARCHAR,
            canonical_id  VARCHAR,
            league        VARCHAR,
            season        VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE gold_dim_match (
            match_id      VARCHAR,
            date          DATE,
            league        VARCHAR,
            season        BIGINT,
            home_team_id  VARCHAR,
            away_team_id  VARCHAR
        )
        """
    )
    yield


def _seed(duck_conn) -> None:
    """Synthetic dataset.

    bronze.sofascore_player_ratings:
      (M_SS_1, P1, home, 6.5)  — resolvable
      (M_SS_1, P2, away, 7.4)  — resolvable
      (M_SS_1, P3, home, 8.3)  — resolvable
      (M_SS_1, P4, away, 9.1)  — orphan → 'ss_P4'
      (M_SS_1, P5, home, 11.5) — outlier → silver maps rating to NULL; player orphan
    """
    duck_conn.execute(
        """
        INSERT INTO bronze_sofascore_player_ratings VALUES
          ('M_SS_1', 'P1', 'home', 6.5, 'GK', 'ENG-Premier League', '2425',
           TIMESTAMP '2026-05-08 12:00:00'),
          ('M_SS_1', 'P2', 'away', 7.4, 'DF', 'ENG-Premier League', '2425',
           TIMESTAMP '2026-05-08 12:00:00'),
          ('M_SS_1', 'P3', 'home', 8.3, 'MF', 'ENG-Premier League', '2425',
           TIMESTAMP '2026-05-08 12:00:00'),
          ('M_SS_1', 'P4', 'away', 9.1, 'FW', 'ENG-Premier League', '2425',
           TIMESTAMP '2026-05-08 12:00:00'),
          ('M_SS_1', 'P5', 'home', 11.5, 'MF', 'ENG-Premier League', '2425',
           TIMESTAMP '2026-05-08 12:00:00')
        """
    )

    # 3 players resolvable. Two seasons present for P1 to verify season+league
    # predicate prevents fan-out (two rows in xref_player → fan-out without
    # the season filter). After the predicate the JOIN must yield exactly 1
    # row in silver per (match_id, player_id).
    duck_conn.execute(
        """
        INSERT INTO silver_xref_player VALUES
          ('sofascore', 'P1', 'fb_p1', 'ENG-Premier League', '2425'),
          ('sofascore', 'P1', 'fb_p1_old', 'ENG-Premier League', '2324'),
          ('sofascore', 'P2', 'fb_p2', 'ENG-Premier League', '2425'),
          ('sofascore', 'P3', 'fb_p3', 'ENG-Premier League', '2425')
          -- P4 / P5: no row → orphan path
        """
    )

    # Schedule + dim_match for the bridge — supply M_SS_1 as bridged.
    duck_conn.execute(
        """
        INSERT INTO bronze_sofascore_schedule VALUES
          (1234, TIMESTAMP '2024-08-15 19:00:00', 'Liverpool', 'Arsenal',
           'ENG-Premier League', '2425', TIMESTAMP '2026-05-08 12:00:00')
        """
    )
    duck_conn.execute(
        """
        INSERT INTO gold_dim_match VALUES
          ('M_FBREF_HEX', DATE '2024-08-15', 'ENG-Premier League', 2024,
           'liverpool', 'arsenal')
        """
    )


def _materialize_silver(con) -> None:
    sql = _translate(SILVER_PATH.read_text(encoding="utf-8"))
    con.execute("DROP TABLE IF EXISTS silver_sofascore_player_ratings")
    con.execute(f"CREATE TABLE silver_sofascore_player_ratings AS {sql}")


def _run_gold(con) -> List[Dict[str, Any]]:
    sql = _translate(GOLD_PATH.read_text(encoding="utf-8"))
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFctMatchRatingPipeline:

    def test_total_row_count(self, duck_conn):
        """5 bronze rows → 5 silver rows → 5 gold rows. The 11.5-outlier row
        is preserved (silver only nulls the rating, not the row)."""
        _seed(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        assert len(out) == 5, f"expected 5 rows, got {len(out)}: {out}"

    def test_rating_outlier_handled(self, duck_conn):
        """11.5 → silver typing maps to NULL (CASE drops out-of-range values)."""
        _seed(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        ratings = sorted(
            (float(r["rating"]) if r["rating"] is not None else None for r in out),
            key=lambda x: (x is None, x),
        )
        # 4 valid + 1 NULL
        non_null = [v for v in ratings if v is not None]
        assert len(non_null) == 4, ratings
        for v in non_null:
            assert 0.1 <= v <= 10.0, f"out-of-range rating leaked: {v}"

    def test_xref_join_no_fanout_with_season_predicate(self, duck_conn):
        """P1 has TWO xref_player rows (different seasons). With season+league
        predicate the silver JOIN must NOT fan out — exactly 1 row per
        (match_id, player_id)."""
        _seed(duck_conn)
        _materialize_silver(duck_conn)
        # Count silver-level rows for P1 specifically.
        cnt = duck_conn.execute(
            """
            SELECT COUNT(*) FROM silver_sofascore_player_ratings
            WHERE player_id_canonical = 'fb_p1'
            """
        ).fetchone()[0]
        assert cnt == 1, (
            f"xref_player season-predicate fan-out detected: {cnt} rows for P1"
        )

    def test_orphan_player_handling(self, duck_conn):
        """P4 + P5 lack xref_player rows → fallback canonical 'ss_<raw>'."""
        _seed(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        canonicals = {r["player_id_canonical"] for r in out}
        assert "ss_P4" in canonicals, canonicals
        assert "ss_P5" in canonicals, canonicals

    def test_resolved_canonical_present(self, duck_conn):
        """P1/P2/P3 → resolved canonicals from xref_player."""
        _seed(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        canonicals = {r["player_id_canonical"] for r in out}
        assert {"fb_p1", "fb_p2", "fb_p3"} <= canonicals, canonicals

    def test_canonical_trio_populated(self, duck_conn):
        _seed(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        for r in out:
            assert r["rating_canonical"], r
            assert r["rating_source"] == "sofascore", r
            assert r["rating_version"] == "v1", r

    def test_team_side_present(self, duck_conn):
        _seed(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        for r in out:
            assert r["team_side"] in {"home", "away"}, r

    def test_pk_uniqueness(self, duck_conn):
        _seed(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        pks = [r["rating_canonical"] for r in out]
        assert len(pks) == len(set(pks)), pks
