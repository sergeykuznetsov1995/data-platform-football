"""
Unit tests for the FBref+WhoScored card UNION + dedup pipeline (E4.5).

Pipeline under test (cross-source assembly folded into Gold — #382):
  bronze.fbref_match_events  ──┐
                                 ├─► gold.fct_card
  bronze.whoscored_events    ──┘

We execute ``dags/sql/gold/fct_card.sql`` against an in-memory DuckDB after a
small text-substitution pass (Trino-only ``regexp_like`` /
``xxhash64(to_utf8(…))`` / ``timestamp(6)``). The gold SQL now inlines the
former ``silver.match_cards`` CTE chain, so it reads bronze + xref directly.

Synthetic dataset:
  * 3 FBref bronze cards: yellow, red, second_yellow.
  * 2 WhoScored bronze cards: type='Card' with qualifiers carrying displayName.
  * 1 cross-source duplicate (FBref + WS for the same player+minute) →
    must collapse to 1 FBref-priority row.
  * 1 WS-only card (no FBref counterpart) → survives.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
GOLD_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_card.sql"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Trino → DuckDB translation helpers
# ---------------------------------------------------------------------------


def _collapse_call(sql: str, fn_name: str) -> str:
    """Drop a wrapper function call (paren-balanced)."""
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
    # Trino `format('%02d%02d', mod(s,100), mod(s+1,100))` → DuckDB `printf`
    sql = re.sub(r"\bformat\s*\(", "printf(", sql, flags=re.IGNORECASE)
    sql = sql.replace("timestamp(6)", "timestamp")
    return sql


# ---------------------------------------------------------------------------
# Fixture / schema setup
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
            match_id          VARCHAR,
            minute            VARCHAR,
            event_type        VARCHAR,
            player            VARCHAR,
            player_id         VARCHAR,
            secondary_player  VARCHAR,
            secondary_player_id VARCHAR,
            team              VARCHAR,
            league            VARCHAR,
            season            BIGINT,
            _ingested_at      TIMESTAMP
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


def _ws_card_qualifiers(display: str) -> str:
    """JSON-style qualifiers literal that regexp_matches understands."""
    return f'[{{"type": {{"value": 1, "displayName": "{display}"}}}}]'


def _seed_corpus(duck_conn) -> None:
    """Synthetic dataset matching the spec.

    FBref:
      * (M1, P1, m=10) yellow
      * (M1, P2, m=20) red
      * (M1, P3, m=80) second_yellow
    WhoScored:
      * (game_id=1, P-WS-A, m=30) yellow                — WS-only, no FBref
      * (game_id=1, P1, m=10) yellow                    — DUP with FBref → drop
    """
    # FBref bronze: 3 cards
    duck_conn.execute(
        """
        INSERT INTO bronze_fbref_match_events VALUES
          ('M1', '10', 'yellow_card',         'Player1', 'fb_p1', NULL, NULL,
           'Liverpool', 'ENG-Premier League', 2024,
           TIMESTAMP '2026-05-08 12:00:00'),
          ('M1', '20', 'red_card',            'Player2', 'fb_p2', NULL, NULL,
           'Liverpool', 'ENG-Premier League', 2024,
           TIMESTAMP '2026-05-08 12:00:00'),
          ('M1', '80', 'second_yellow_card',  'Player3', 'fb_p3', NULL, NULL,
           'Liverpool', 'ENG-Premier League', 2024,
           TIMESTAMP '2026-05-08 12:00:00')
        """
    )

    # WhoScored bronze: 2 cards (1 dup with FBref, 1 WS-only)
    yellow_q = _ws_card_qualifiers("Yellow")
    duck_conn.execute(
        """
        INSERT INTO bronze_whoscored_events VALUES
          (1.0, 'FirstHalf', 30, 0, 30, 'Card', 'Successful',
           100.0, 999.0, ?,        NULL, 'Liverpool',
           'ENG-Premier League', '2425', TIMESTAMP '2026-05-08 12:00:00'),
          (1.0, 'FirstHalf', 10, 0, 10, 'Card', 'Successful',
           100.0, 1000.0, ?,       NULL, 'Liverpool',
           'ENG-Premier League', '2425', TIMESTAMP '2026-05-08 12:00:00')
        """,
        [yellow_q, yellow_q],
    )

    # WS schedule (bridge spine) — same league + same date.
    duck_conn.execute(
        """
        INSERT INTO bronze_whoscored_schedule VALUES
          (1, TIMESTAMP '2024-08-15 19:00:00', 'Liverpool', 'Arsenal',
           'ENG-Premier League', '2425', TIMESTAMP '2026-05-08 12:00:00')
        """
    )

    # FBref enriched: provides the bridge target.
    duck_conn.execute(
        """
        INSERT INTO silver_fbref_match_enriched VALUES
          ('M1', 'ENG-Premier League', 'Liverpool', 'Arsenal',
           DATE '2024-08-15')
        """
    )

    # xref_team — both sources point to the same canonical id (so the bridge
    # roundtrip works).
    duck_conn.execute(
        """
        INSERT INTO silver_xref_team VALUES
          ('fbref',     'Liverpool', 'liverpool',
           'ENG-Premier League', '2425'),
          ('fbref',     'Arsenal',   'arsenal',
           'ENG-Premier League', '2425'),
          ('whoscored', 'Liverpool', 'liverpool',
           'ENG-Premier League', '2425'),
          ('whoscored', 'Arsenal',   'arsenal',
           'ENG-Premier League', '2425')
        """
    )

    # xref_player — only resolve fb_p1 to the same canonical as ws player_id=1000
    # so the cross-source dedup branch fires for one of the duplicates.
    duck_conn.execute(
        """
        INSERT INTO silver_xref_player VALUES
          ('fbref',     'fb_p1', 'p1_canon', 'ENG-Premier League', '2425'),
          ('whoscored', '1000',  'p1_canon', 'ENG-Premier League', '2425'),
          ('fbref',     'fb_p2', 'p2_canon', 'ENG-Premier League', '2425'),
          ('fbref',     'fb_p3', 'p3_canon', 'ENG-Premier League', '2425')
        """
    )

    # xref_match: empty (COALESCE falls back to fe.match_id).


def _run_gold(duck_conn) -> List[Dict[str, Any]]:
    sql = _translate(GOLD_PATH.read_text(encoding="utf-8"))
    cur = duck_conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFctCardPipeline:

    def test_dedup_collapses_cross_source_dup(self, duck_conn):
        """Total = 4 rows (3 FBref + 1 WS-only). The (M1, P1, 10, yellow)
        duplicate collapses on the resolved canonical to FBref-priority side."""
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        assert len(out) == 4, f"expected 4 rows after dedup, got {len(out)}: {out}"

    def test_card_type_distribution(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        types = sorted(r["card_type"] for r in out)
        assert "yellow" in types
        assert "red" in types
        assert "second_yellow" in types

    def test_card_source_distribution(self, duck_conn):
        """3 FBref-source rows + 1 WhoScored-source row (WS-only fallback)."""
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        from collections import Counter
        c = Counter(r["card_source"] for r in out)
        assert c.get("fbref") == 3, c
        assert c.get("whoscored") == 1, c

    def test_canonical_trio_populated(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        for r in out:
            assert r["card_canonical"], r
            assert r["card_source"] in {"fbref", "whoscored"}, r
            assert r["card_version"] == "v1", r

    def test_pk_uniqueness(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        pks = [r["card_canonical"] for r in out]
        assert len(pks) == len(set(pks)), pks
