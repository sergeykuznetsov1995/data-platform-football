"""
Unit tests for the unified match chronicle ``gold.fct_match_timeline`` (#427).

Pipeline under test:
  silver.fbref_match_events  (primary)   ──┐
                                            ├─► gold.fct_match_timeline
  bronze.whoscored_events    (fallback)  ──┘

Per-match fallback gate: a match takes WhoScored events ONLY when it has zero
FBref events — no event-level cross-source dedup, no mixed-source matches.

We execute ``dags/sql/gold/fct_match_timeline.sql`` against an in-memory
DuckDB after a small text-substitution pass (Trino-only ``regexp_like``).

CAVEAT: DuckDB tolerates same-level SELECT-alias references inside OVER()
that Trino rejects (COLUMN_NOT_FOUND) — these tests do NOT validate that.
Run ``EXPLAIN (TYPE VALIDATE)`` against live Trino as a separate step.

Synthetic dataset:
  * M1 — FBref-rich match (Liverpool vs Arsenal): goal+assist, penalty,
    own_goal (credited side), yellow / second yellow, substitution,
    stoppage-time events ('45+2', '90+4') — PLUS overlapping WhoScored
    events for the same bridged game_id=1 that the gate must drop.
  * M2 — WhoScored-only match (Chelsea vs Fulham, game_id=2 bridged via
    schedule+enriched): goal+assist, penalty goal, penalty miss, own goal
    (Opta side flip), yellow card in first-half stoppage (cumulative
    minute 48 → 45+3), SubstitutionOff/On pair (one timeline row).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
GOLD_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_match_timeline.sql"

pytestmark = pytest.mark.unit

EVENT_TYPE_DICT = {
    "goal", "own_goal", "penalty_goal", "penalty_missed",
    "yellow_card", "second_yellow", "red_card", "substitution",
}


# ---------------------------------------------------------------------------
# Trino → DuckDB translation helpers
# ---------------------------------------------------------------------------

_ICEBERG_TO_LOCAL = {
    "iceberg.silver.fbref_match_events":     "silver_fbref_match_events",
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
        "silver_fbref_match_events", "bronze_whoscored_events",
        "bronze_whoscored_schedule", "silver_fbref_match_enriched",
        "silver_xref_match", "silver_xref_team", "silver_xref_player",
    ):
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    duck_conn.execute(
        """
        CREATE TABLE silver_fbref_match_events (
            match_id            VARCHAR,
            minute              VARCHAR,
            event_type          VARCHAR,
            player              VARCHAR,
            player_id           VARCHAR,
            team                VARCHAR,
            team_side           VARCHAR,
            secondary_player    VARCHAR,
            secondary_player_id VARCHAR,
            _bronze_ingested_at TIMESTAMP,
            league              VARCHAR,
            season              VARCHAR
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
            related_player_id DOUBLE,
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


def _ws_qualifiers(display: str) -> str:
    """JSON-style qualifiers literal that regexp_matches understands."""
    return f'[{{"type": {{"value": 1, "displayName": "{display}"}}}}]'


_TS = "TIMESTAMP '2026-06-11 12:00:00'"
_LG = "ENG-Premier League"


def _seed_corpus(duck_conn) -> None:
    # ---- M1: FBref-rich match (Liverpool home vs Arsenal away) ----
    # FBref substitution convention: player_id = ON, secondary_player_id = OFF.
    # FBref own_goal convention: team/team_side = CREDITED side, player = striker
    # from the opposite team.
    duck_conn.execute(
        f"""
        INSERT INTO silver_fbref_match_events VALUES
          ('M1', '10',   'goal',               'P1', 'fb_p1', 'Liverpool', 'home',
           'P2', 'fb_p2', {_TS}, '{_LG}', '2425'),
          ('M1', '30',   'penalty',            'P9', 'fb_p9', 'Arsenal',   'away',
           NULL, NULL,   {_TS}, '{_LG}', '2425'),
          ('M1', '45+2', 'yellow_card',        'P8', 'fb_p8', 'Liverpool', 'home',
           NULL, NULL,   {_TS}, '{_LG}', '2425'),
          ('M1', '55',   'own_goal',           'P3', 'fb_p3', 'Liverpool', 'home',
           NULL, NULL,   {_TS}, '{_LG}', '2425'),
          ('M1', '60',   'substitution',       'P4', 'fb_p4', 'Liverpool', 'home',
           'P5', 'fb_p5', {_TS}, '{_LG}', '2425'),
          ('M1', '70',   'yellow_card',        'P6', 'fb_p6', 'Arsenal',   'away',
           NULL, NULL,   {_TS}, '{_LG}', '2425'),
          ('M1', '80',   'second_yellow_card', 'P6', 'fb_p6', 'Arsenal',   'away',
           NULL, NULL,   {_TS}, '{_LG}', '2425'),
          ('M1', '90+4', 'goal',               'P7', 'fb_p7', 'Arsenal',   'away',
           NULL, NULL,   {_TS}, '{_LG}', '2425')
        """
    )

    # WhoScored events for the SAME match (game_id=1 bridges to M1) — the
    # per-match fallback gate must drop ALL of them.
    yellow_q = _ws_qualifiers("Yellow")
    duck_conn.execute(
        f"""
        INSERT INTO bronze_whoscored_events VALUES
          (1.0, 'FirstHalf', 10, 0, 10, 'Goal', 'Successful',
           100.0, 1000.0, NULL, '[]', NULL, 'Liverpool', '{_LG}', '2425', {_TS}),
          (1.0, 'FirstHalf', 30, 0, 30, 'Card', 'Successful',
           101.0, 1001.0, NULL, ?, NULL, 'Arsenal', '{_LG}', '2425', {_TS})
        """,
        [yellow_q],
    )

    # ---- M2: WhoScored-only match (Chelsea home vs Fulham away) ----
    # Opta own_goal convention: event sits on the STRIKER's team (Fulham) —
    # the credit must flip to Chelsea. Cumulative minute: FirstHalf 48 → 45+3.
    duck_conn.execute(
        f"""
        INSERT INTO bronze_whoscored_events VALUES
          (2.0, 'FirstHalf',  9, 30,  9, 'Goal', 'Successful',
           200.0, 2001.0, 2002.0, '[]', NULL, 'Chelsea', '{_LG}', '2425', {_TS}),
          (2.0, 'FirstHalf', 48,  0, 48, 'Card', 'Successful',
           201.0, 2006.0, NULL, ?, NULL, 'Fulham', '{_LG}', '2425', {_TS}),
          (2.0, 'SecondHalf', 50, 0, 50, 'Goal', 'Successful',
           201.0, 2003.0, NULL, ?, NULL, 'Fulham', '{_LG}', '2425', {_TS}),
          (2.0, 'SecondHalf', 60, 0, 60, 'SavedShot', 'Unsuccessful',
           200.0, 2004.0, NULL, ?, NULL, 'Chelsea', '{_LG}', '2425', {_TS}),
          (2.0, 'SecondHalf', 65, 0, 65, 'SubstitutionOff', 'Successful',
           200.0, 2007.0, 2008.0, '[]', NULL, 'Chelsea', '{_LG}', '2425', {_TS}),
          (2.0, 'SecondHalf', 65, 0, 65, 'SubstitutionOn', 'Successful',
           200.0, 2008.0, 2007.0, '[]', NULL, 'Chelsea', '{_LG}', '2425', {_TS}),
          (2.0, 'SecondHalf', 70, 0, 70, 'Goal', 'Successful',
           201.0, 2005.0, NULL, ?, NULL, 'Fulham', '{_LG}', '2425', {_TS})
        """,
        [
            yellow_q,
            _ws_qualifiers("Penalty"),
            _ws_qualifiers("Penalty"),
            _ws_qualifiers("OwnGoal"),
        ],
    )

    # WS schedule (bridge spine).
    duck_conn.execute(
        f"""
        INSERT INTO bronze_whoscored_schedule VALUES
          (1, TIMESTAMP '2024-08-15 19:00:00', 'Liverpool', 'Arsenal',
           '{_LG}', '2425', {_TS}),
          (2, TIMESTAMP '2024-08-16 19:00:00', 'Chelsea', 'Fulham',
           '{_LG}', '2425', {_TS})
        """
    )

    # FBref enriched: bridge targets.
    duck_conn.execute(
        f"""
        INSERT INTO silver_fbref_match_enriched VALUES
          ('M1', '{_LG}', 'Liverpool', 'Arsenal', DATE '2024-08-15'),
          ('M2', '{_LG}', 'Chelsea',   'Fulham',  DATE '2024-08-16')
        """
    )

    # xref_team — both sources resolve to the same canonical slug.
    duck_conn.execute(
        f"""
        INSERT INTO silver_xref_team VALUES
          ('fbref',     'Liverpool', 'liverpool', '{_LG}', '2425'),
          ('fbref',     'Arsenal',   'arsenal',   '{_LG}', '2425'),
          ('fbref',     'Chelsea',   'chelsea',   '{_LG}', '2425'),
          ('fbref',     'Fulham',    'fulham',    '{_LG}', '2425'),
          ('whoscored', 'Liverpool', 'liverpool', '{_LG}', '2425'),
          ('whoscored', 'Arsenal',   'arsenal',   '{_LG}', '2425'),
          ('whoscored', 'Chelsea',   'chelsea',   '{_LG}', '2425'),
          ('whoscored', 'Fulham',    'fulham',    '{_LG}', '2425')
        """
    )

    # xref_player.
    duck_conn.execute(
        f"""
        INSERT INTO silver_xref_player VALUES
          ('fbref',     'fb_p1', 'p1', '{_LG}', '2425'),
          ('fbref',     'fb_p2', 'p2', '{_LG}', '2425'),
          ('fbref',     'fb_p3', 'p3', '{_LG}', '2425'),
          ('fbref',     'fb_p4', 'p4', '{_LG}', '2425'),
          ('fbref',     'fb_p5', 'p5', '{_LG}', '2425'),
          ('fbref',     'fb_p6', 'p6', '{_LG}', '2425'),
          ('fbref',     'fb_p7', 'p7', '{_LG}', '2425'),
          ('fbref',     'fb_p8', 'p8', '{_LG}', '2425'),
          ('fbref',     'fb_p9', 'p9', '{_LG}', '2425'),
          ('whoscored', '2001', 'c2001', '{_LG}', '2425'),
          ('whoscored', '2002', 'c2002', '{_LG}', '2425'),
          ('whoscored', '2003', 'c2003', '{_LG}', '2425'),
          ('whoscored', '2004', 'c2004', '{_LG}', '2425'),
          ('whoscored', '2005', 'c2005', '{_LG}', '2425'),
          ('whoscored', '2006', 'c2006', '{_LG}', '2425'),
          ('whoscored', '2007', 'c2007', '{_LG}', '2425'),
          ('whoscored', '2008', 'c2008', '{_LG}', '2425')
        """
    )

    # xref_match: empty (COALESCE falls back to the raw match_id).


def _run_gold(duck_conn) -> List[Dict[str, Any]]:
    sql = _translate(GOLD_PATH.read_text(encoding="utf-8"))
    cur = duck_conn.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return sorted(rows, key=lambda r: (r["match_id"], r["event_seq"]))


def _match_rows(out, match_id):
    return [r for r in out if r["match_id"] == match_id]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFctMatchTimeline:

    def test_event_seq_dense_and_monotonic(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        m1, m2 = _match_rows(out, "M1"), _match_rows(out, "M2")
        assert len(m1) == 8, m1
        assert len(m2) == 6, m2
        assert [r["event_seq"] for r in m1] == list(range(1, 9))
        assert [r["event_seq"] for r in m2] == list(range(1, 7))

    def test_event_type_dictionary_and_mappings(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        types = {r["event_type"] for r in out}
        assert types <= EVENT_TYPE_DICT, types
        # source values must be remapped, never passed through raw
        assert "penalty" not in types
        assert "second_yellow_card" not in types
        assert "penalty_goal" in types
        assert "second_yellow" in types
        assert "penalty_missed" in types  # WS-only category

    def test_minute_added_and_period_parsing(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        m1 = _match_rows(out, "M1")
        stoppage_1h = next(r for r in m1 if r["event_type"] == "yellow_card"
                           and r["player_id"] == "p8")
        assert (stoppage_1h["minute"], stoppage_1h["minute_added"],
                stoppage_1h["period"]) == (45, 2, "1H")
        stoppage_2h = next(r for r in m1 if r["event_seq"] == 8)
        assert (stoppage_2h["minute"], stoppage_2h["minute_added"],
                stoppage_2h["period"]) == (90, 4, "2H")
        # WS cumulative minute: FirstHalf 48 → 45+3
        ws_yellow = next(r for r in _match_rows(out, "M2")
                         if r["event_type"] == "yellow_card")
        assert (ws_yellow["minute"], ws_yellow["minute_added"],
                ws_yellow["period"]) == (45, 3, "1H")
        plain = next(r for r in m1 if r["event_seq"] == 1)
        assert (plain["minute"], plain["minute_added"]) == (10, None)

    def test_running_score_and_own_goal_credit(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        m1 = _match_rows(out, "M1")
        scores = [(r["score_home_after"], r["score_away_after"]) for r in m1]
        # 10' goal H → 1:0; 30' pen A → 1:1; 45+2 card → 1:1;
        # 55' own_goal credited H → 2:1; 60' sub / 70' & 80' cards → 2:1;
        # 90+4 goal A → 2:2
        assert scores == [(1, 0), (1, 1), (1, 1), (2, 1),
                          (2, 1), (2, 1), (2, 1), (2, 2)], scores
        og = next(r for r in m1 if r["event_type"] == "own_goal")
        assert og["team_id"] == "liverpool"   # credited team
        assert og["player_id"] == "p3"        # actual striker (opposite team)
        assert og["related_player_id"] is None

    def test_score_carried_on_non_goal_events(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        for r in out:
            assert r["score_home_after"] is not None, r
            assert r["score_away_after"] is not None, r

    def test_ws_fallback_exclusivity(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        sources_by_match = {}
        for r in out:
            sources_by_match.setdefault(r["match_id"], set()).add(r["event_source"])
        assert sources_by_match["M1"] == {"fbref"}
        assert sources_by_match["M2"] == {"whoscored"}
        for match_id, sources in sources_by_match.items():
            assert len(sources) == 1, (match_id, sources)

    def test_substitution_semantics(self, duck_conn):
        """player_id = player going OFF (main actor), related = coming ON."""
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        fb_sub = next(r for r in _match_rows(out, "M1")
                      if r["event_type"] == "substitution")
        assert fb_sub["player_id"] == "p5"           # FBref secondary = OFF
        assert fb_sub["related_player_id"] == "p4"   # FBref player = ON
        ws_subs = [r for r in _match_rows(out, "M2")
                   if r["event_type"] == "substitution"]
        assert len(ws_subs) == 1, ws_subs  # On row must not double-count
        assert ws_subs[0]["player_id"] == "c2007"
        assert ws_subs[0]["related_player_id"] == "c2008"

    def test_ws_penalty_and_own_goal_flip(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        m2 = _match_rows(out, "M2")
        pen_goal = next(r for r in m2 if r["event_type"] == "penalty_goal")
        assert pen_goal["player_id"] == "c2003"
        pen_miss = next(r for r in m2 if r["event_type"] == "penalty_missed")
        assert pen_miss["player_id"] == "c2004"
        assert (pen_miss["score_home_after"], pen_miss["score_away_after"]) == (1, 1)
        og = next(r for r in m2 if r["event_type"] == "own_goal")
        assert og["team_id"] == "chelsea"      # credit flipped off Fulham
        assert og["player_id"] == "c2005"      # Fulham striker
        assert (og["score_home_after"], og["score_away_after"]) == (2, 1)
        scores = [(r["score_home_after"], r["score_away_after"]) for r in m2]
        assert scores == [(1, 0), (1, 0), (1, 1), (1, 1), (1, 1), (2, 1)], scores

    def test_goal_assist_related(self, duck_conn):
        _seed_corpus(duck_conn)
        out = _run_gold(duck_conn)
        fb_goal = next(r for r in _match_rows(out, "M1") if r["event_seq"] == 1)
        assert fb_goal["related_player_id"] == "p2"
        ws_goal = next(r for r in _match_rows(out, "M2") if r["event_seq"] == 1)
        assert ws_goal["related_player_id"] == "c2002"
        for r in out:
            if r["event_type"] in {"yellow_card", "second_yellow", "red_card"}:
                assert r["related_player_id"] is None, r

    def test_determinism_and_pk_uniqueness(self, duck_conn):
        _seed_corpus(duck_conn)
        first = _run_gold(duck_conn)
        second = _run_gold(duck_conn)
        assert first == second
        pks = [(r["match_id"], r["event_seq"]) for r in first]
        assert len(pks) == len(set(pks))
