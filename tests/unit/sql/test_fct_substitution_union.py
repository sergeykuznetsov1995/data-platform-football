"""
Unit tests for the FBref + WhoScored substitution UNION + dedup pipeline (E4.5).

Pipeline under test (cross-source assembly folded into Gold — #382):
  bronze.fbref_match_events  (event_type='substitution', player + secondary_player)
  bronze.whoscored_events    (type IN ('SubstitutionOff', 'SubstitutionOn'),
                              paired via ws_pairs CTE on game_id+minute+team_id)
  ↓
  gold.fct_substitution

We exercise ``dags/sql/gold/fct_substitution.sql`` end-to-end on DuckDB after a
small text-substitution pass. The gold SQL now inlines the former
``silver.match_substitutions`` CTE chain, so it reads bronze + xref directly.

Synthetic dataset:
  * 2 FBref subs in match M1 (player_in_canonical / player_out_canonical pairs)
  * 1 WhoScored swap pair (SubOff + SubOn at the same minute / team) in
    a WS-only fixture (no FBref counterpart) so it survives dedup.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
GOLD_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_substitution.sql"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Translation helpers (shared with sibling tests)
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
    "iceberg.bronze.fbref_match_events":     "bronze_fbref_match_events",
    "iceberg.bronze.whoscored_events":       "bronze_whoscored_events",
    "iceberg.bronze.whoscored_schedule":     "bronze_whoscored_schedule",
    "iceberg.silver.fbref_match_enriched":   "silver_fbref_match_enriched",
    "iceberg.silver.xref_match":             "silver_xref_match",
    "iceberg.silver.xref_team":              "silver_xref_team",
    "iceberg.silver.xref_player":            "silver_xref_player",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    sql = re.sub(r"\bregexp_like\s*\(", "regexp_matches(", sql, flags=re.IGNORECASE)
    sql = _collapse_call(sql, "to_utf8")
    sql = _collapse_call(sql, "to_hex")
    sql = re.sub(r"\bxxhash64\b", "md5", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bformat\s*\(", "printf(", sql, flags=re.IGNORECASE)
    sql = sql.replace("timestamp(6)", "timestamp")
    return sql


# ---------------------------------------------------------------------------
# Schema setup
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
        "bronze_fbref_match_events", "bronze_whoscored_events",
        "bronze_whoscored_schedule", "silver_fbref_match_enriched",
        "silver_xref_match", "silver_xref_team", "silver_xref_player",
    ):
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    duck_conn.execute(
        """
        CREATE TABLE bronze_fbref_match_events (
            match_id              VARCHAR,
            minute                VARCHAR,
            event_type            VARCHAR,
            player                VARCHAR,
            player_id             VARCHAR,
            secondary_player      VARCHAR,
            secondary_player_id   VARCHAR,
            team                  VARCHAR,
            league                VARCHAR,
            season                BIGINT,
            _ingested_at          TIMESTAMP
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE bronze_whoscored_events (
            game_id           DOUBLE,
            period            VARCHAR,
            minute            BIGINT,
            second            BIGINT,
            expanded_minute   BIGINT,
            type              VARCHAR,
            outcome_type      VARCHAR,
            team_id           DOUBLE,
            player_id         DOUBLE,
            qualifiers        VARCHAR,
            related_event_id  DOUBLE,
            team              VARCHAR,
            league            VARCHAR,
            season            VARCHAR,
            _ingested_at      TIMESTAMP
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE bronze_whoscored_schedule (
            game_id      BIGINT,
            date         TIMESTAMP,
            home_team    VARCHAR,
            away_team    VARCHAR,
            league       VARCHAR,
            season       VARCHAR,
            _ingested_at TIMESTAMP
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE silver_fbref_match_enriched (
            match_id  VARCHAR,
            league    VARCHAR,
            home      VARCHAR,
            away      VARCHAR,
            date      DATE
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE silver_xref_match (
            source        VARCHAR,
            source_id     VARCHAR,
            canonical_id  VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE silver_xref_team (
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
        CREATE TABLE silver_xref_player (
            source        VARCHAR,
            source_id     VARCHAR,
            canonical_id  VARCHAR,
            league        VARCHAR,
            season        VARCHAR
        )
        """
    )
    yield


def _seed(duck_conn) -> None:
    """Synthetic dataset.

    FBref subs:
      * (M1, t=Liverpool, m=60) IN=fb_in1 OFF=fb_out1
      * (M1, t=Arsenal,   m=75) IN=fb_in2 OFF=fb_out2
    WhoScored swap (game_id=1, team_id=100, m=80) — no FBref counterpart.
    """
    duck_conn.execute(
        """
        INSERT INTO bronze_fbref_match_events VALUES
          ('M1', '60', 'substitution', 'NewPlayer1', 'fb_in1',
           'OldPlayer1', 'fb_out1', 'Liverpool', 'ENG-Premier League', 2024,
           TIMESTAMP '2026-05-08 12:00:00'),
          ('M1', '75', 'substitution', 'NewPlayer2', 'fb_in2',
           'OldPlayer2', 'fb_out2', 'Arsenal', 'ENG-Premier League', 2024,
           TIMESTAMP '2026-05-08 12:00:00')
        """
    )

    # WhoScored: 1 SubOff + 1 SubOn → ws_pairs builds 1 paired row.
    duck_conn.execute(
        """
        INSERT INTO bronze_whoscored_events VALUES
          (2.0, 'SecondHalf', 80, 5, 80, 'SubstitutionOff', 'Successful',
           100.0, 5000.0, NULL, 6000.0, 'Manchester City',
           'ENG-Premier League', '2425', TIMESTAMP '2026-05-08 12:00:00'),
          (2.0, 'SecondHalf', 80, 6, 80, 'SubstitutionOn',  'Successful',
           100.0, 6000.0, NULL, 5000.0, 'Manchester City',
           'ENG-Premier League', '2425', TIMESTAMP '2026-05-08 12:00:00')
        """
    )

    # Schedule (bridge spine) — game_id=2 → Manchester City vs Newcastle 2024-09-01.
    # No FBref enriched row for this date → fallback 'whoscored_raw_2'.
    duck_conn.execute(
        """
        INSERT INTO bronze_whoscored_schedule VALUES
          (2, TIMESTAMP '2024-09-01 19:00:00', 'Manchester City', 'Newcastle',
           'ENG-Premier League', '2425', TIMESTAMP '2026-05-08 12:00:00')
        """
    )

    # FBref enriched: provides the bridge target only for M1 — so WS row falls
    # back to 'whoscored_raw_2' deliberately.
    duck_conn.execute(
        """
        INSERT INTO silver_fbref_match_enriched VALUES
          ('M1', 'ENG-Premier League', 'Liverpool', 'Arsenal',
           DATE '2024-08-15')
        """
    )

    duck_conn.execute(
        """
        INSERT INTO silver_xref_team VALUES
          ('fbref',     'Liverpool',       'liverpool',       'ENG-Premier League', '2425'),
          ('fbref',     'Arsenal',         'arsenal',         'ENG-Premier League', '2425'),
          ('whoscored', 'Manchester City', 'manchester_city', 'ENG-Premier League', '2425')
        """
    )

    duck_conn.execute(
        """
        INSERT INTO silver_xref_player VALUES
          ('fbref',     'fb_in1',  'in1_canon',  'ENG-Premier League', '2425'),
          ('fbref',     'fb_out1', 'out1_canon', 'ENG-Premier League', '2425'),
          ('fbref',     'fb_in2',  'in2_canon',  'ENG-Premier League', '2425'),
          ('fbref',     'fb_out2', 'out2_canon', 'ENG-Premier League', '2425'),
          ('whoscored', '6000',    'ws_in_canon',  'ENG-Premier League', '2425'),
          ('whoscored', '5000',    'ws_out_canon', 'ENG-Premier League', '2425')
        """
    )


def _run_gold(duck_conn) -> List[Dict[str, Any]]:
    sql = _translate(GOLD_PATH.read_text(encoding="utf-8"))
    cur = duck_conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFctSubstitutionPipeline:

    def test_total_row_count(self, duck_conn):
        """2 FBref + 1 WhoScored paired = 3 rows."""
        _seed(duck_conn)
        out = _run_gold(duck_conn)
        assert len(out) == 3, f"expected 3 rows, got {len(out)}: {out}"

    def test_paired_in_out_correctness(self, duck_conn):
        """FBref convention: player = IN, secondary_player = OFF.

        ws_pairs builds (in, out) by joining SubstitutionOn ↔ SubstitutionOff
        on (game_id, minute, team_id).
        """
        _seed(duck_conn)
        out = _run_gold(duck_conn)

        fbref_rows = [r for r in out if r["substitution_source"] == "fbref"]
        ws_rows = [r for r in out if r["substitution_source"] == "whoscored"]

        # FBref rows carry the canonical IDs we seeded
        in_outs = sorted(
            (r["player_in_canonical"], r["player_out_canonical"]) for r in fbref_rows
        )
        assert in_outs == [
            ("in1_canon", "out1_canon"),
            ("in2_canon", "out2_canon"),
        ], in_outs

        # WS pair: player_id=6000 came on (SubstitutionOn), 5000 came off
        assert len(ws_rows) == 1, ws_rows
        assert ws_rows[0]["player_in_canonical"] == "ws_in_canon"
        assert ws_rows[0]["player_out_canonical"] == "ws_out_canon"

    def test_minute_parity_across_sources(self, duck_conn):
        """FBref minutes 60/75; WhoScored minute 80 — all preserved."""
        _seed(duck_conn)
        out = _run_gold(duck_conn)
        minutes = sorted(r["minute"] for r in out)
        assert minutes == [60, 75, 80], minutes

    def test_canonical_trio_populated(self, duck_conn):
        _seed(duck_conn)
        out = _run_gold(duck_conn)
        for r in out:
            assert r["substitution_canonical"], r
            assert r["substitution_source"] in {"fbref", "whoscored"}, r
            assert r["substitution_version"] == "v1", r

    def test_source_distribution(self, duck_conn):
        _seed(duck_conn)
        out = _run_gold(duck_conn)
        from collections import Counter
        c = Counter(r["substitution_source"] for r in out)
        assert c.get("fbref") == 2, c
        assert c.get("whoscored") == 1, c

    def test_pk_uniqueness(self, duck_conn):
        _seed(duck_conn)
        out = _run_gold(duck_conn)
        pks = [r["substitution_canonical"] for r in out]
        assert len(pks) == len(set(pks)), pks
