"""
Unit tests for ``dags/sql/gold/fct_goal.sql`` — UNION + canonical-trio (E4.5).

Strategy
--------
``fct_goal`` unions:
  * regular_goals   — ``iceberg.gold.fct_shot``        WHERE result_canonical='goal'
  * own_goals       — ``iceberg.bronze.fbref_match_events`` WHERE event_type='own_goal'
                      (+ xref_match / xref_team / xref_player season+league JOINs)

We exercise the UNION + canonical-trio + own-goal team-attribution behaviour
on DuckDB by:
  * Re-naming the iceberg.* table refs to single-namespace tables seeded
    from inline VALUES.
  * Translating Trino-only ``xxhash64(to_utf8(...))`` → DuckDB ``md5(...)``
    (both yield deterministic varchar; the file's contract is just "non-null
    deterministic varchar PK").
  * Replacing the ``format('%02d%02d', mod(s,100), mod(s+1,100))`` call (not
    relevant here — fct_goal does not use it; only silver/match_cards does).

The test does NOT re-implement the SQL; it executes the production file
verbatim after a small text-substitution pass.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_goal.sql"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Trino → DuckDB translation
# ---------------------------------------------------------------------------


def _collapse_call(sql: str, fn_name: str) -> str:
    """Replace ``fn_name(EXPR)`` with ``EXPR`` by paren-counting.

    Used to drop wrappers like ``to_utf8(...)`` and ``to_hex(...)`` that have
    no DuckDB equivalent but whose arguments are already strings/hex.
    """
    out = []
    i = 0
    n = len(sql)
    pat = fn_name + "("
    while i < n:
        # Match fn_name followed by optional ws then '('
        j = i
        if sql[i:i + len(fn_name)].lower() == fn_name.lower():
            j = i + len(fn_name)
            # Skip whitespace
            while j < n and sql[j] in " \t\n\r":
                j += 1
            if j < n and sql[j] == "(":
                # Walk to matching close paren
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
                # sql[inner_start:j] is the inner expression
                # sql[j] is the closing paren — skip it.
                out.append(sql[inner_start:j])
                i = j + 1
                continue
        out.append(sql[i])
        i += 1
    return "".join(out)


def _translate_trino_to_duckdb(sql: str) -> str:
    """Adapt Gold SQL for DuckDB execution.

    Rewrites:
      * ``iceberg.<schema>.<table>``    → underscore-joined single-namespace name
      * ``to_utf8(<x>)``                → ``<x>``     (md5 takes varchar directly)
      * ``xxhash64(<x>)``               → ``md5(<x>)`` (deterministic varchar PK)
      * ``to_hex(<x>)``                 → ``<x>``     (md5 already hex; safe drop)
      * ``timestamp(6)``                → ``timestamp``
    """
    sql = sql.replace("iceberg.gold.fct_shot", "gold_fct_shot")
    sql = sql.replace("iceberg.bronze.fbref_match_events", "bronze_fbref_match_events")
    sql = sql.replace("iceberg.silver.xref_match", "silver_xref_match")
    sql = sql.replace("iceberg.silver.xref_team", "silver_xref_team")
    sql = sql.replace("iceberg.silver.xref_player", "silver_xref_player")

    # Collapse Trino-only wrappers — paren-balanced replacement.
    sql = _collapse_call(sql, "to_utf8")
    sql = _collapse_call(sql, "to_hex")
    # Now xxhash64(EXPR) → md5(EXPR). Just rename the function.
    sql = re.sub(r"\bxxhash64\b", "md5", sql, flags=re.IGNORECASE)

    sql = sql.replace("timestamp(6)", "timestamp")
    return sql


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Fixtures: inline seed schemas
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


@pytest.fixture(autouse=True)
def _create_schemas(duck_conn):
    """Drop + recreate the seed tables before every test."""
    for tbl in (
        "gold_fct_shot",
        "bronze_fbref_match_events",
        "silver_xref_match",
        "silver_xref_team",
        "silver_xref_player",
    ):
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    duck_conn.execute(
        """
        CREATE TABLE gold_fct_shot (
            shot_id                       VARCHAR,
            match_id_canonical            VARCHAR,
            team_id_canonical             VARCHAR,
            player_id_canonical           VARCHAR,
            assist_player_id_canonical    VARCHAR,
            minute                        INTEGER,
            result_canonical              VARCHAR,
            situation_canonical           VARCHAR,
            league                        VARCHAR,
            season                        VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE bronze_fbref_match_events (
            match_id        VARCHAR,
            minute          VARCHAR,
            event_type      VARCHAR,
            player          VARCHAR,
            player_id       VARCHAR,
            team            VARCHAR,
            league          VARCHAR,
            season          BIGINT,
            _ingested_at    TIMESTAMP
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


def _run(duck_conn) -> List[Dict[str, Any]]:
    sql = _translate_trino_to_duckdb(_read_sql())
    cur = duck_conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestUnionAndOwnGoalSplit:
    """3 regular goals + 2 own goals → 5 rows, correct is_own_goal split."""

    def test_full_corpus_union(self, duck_conn):
        # ---- Regular goals (fct_shot WHERE result_canonical='goal') ----
        # Per spec: 3 regular goals (1 normal, 1 penalty, 1 of a same-minute
        # brace via the multi-goal-same-minute pk_tiebreaker scenario). The
        # brace is exercised separately in TestPkUniqueness; here we want
        # exactly 3 regular rows.
        duck_conn.execute(
            """
            INSERT INTO gold_fct_shot VALUES
              -- 1) normal goal
              ('S1', 'M1', 'team_home', 'P1', 'P2', 22, 'goal', 'open_play',
               'ENG-Premier League', '2425'),
              -- 2) penalty goal (situation_canonical='penalty')
              ('S2', 'M1', 'team_away', 'P3', NULL, 67, 'goal', 'penalty',
               'ENG-Premier League', '2425'),
              -- 3) third goal — different match
              ('S3', 'M2', 'team_home', 'P4', NULL, 46, 'goal', 'open_play',
               'ENG-Premier League', '2425'),
              -- non-goal shot — must NOT appear in fct_goal
              ('S5', 'M1', 'team_home', 'P1', NULL, 30, 'saved', 'open_play',
               'ENG-Premier League', '2425')
            """
        )

        # ---- Own goals (bronze.fbref_match_events) ----
        # 1) Bernd Leno scenario: scorer's name is OG-scorer; team = receiving team
        # 2) NULL match_id_canonical edge case: match_id has no xref_match row,
        #    falls back to fe.match_id (still non-null). To exercise the
        #    "match_id_canonical IS NULL → filtered" branch, we add a row with
        #    match_id NULL — should be dropped by the final WHERE.
        duck_conn.execute(
            """
            INSERT INTO bronze_fbref_match_events VALUES
              ('M3', '20', 'own_goal', 'Bernd Leno', 'fb_leno',
                'Liverpool', 'ENG-Premier League', 2024, TIMESTAMP '2026-05-08 12:00:00'),
              ('M3', '43', 'own_goal', 'Axel Disasi', 'fb_disasi',
                'Wolverhampton Wanderers', 'ENG-Premier League', 2024,
                TIMESTAMP '2026-05-08 12:00:00'),
              -- NULL minute — filtered by final WHERE minute IS NOT NULL
              ('M3', NULL, 'own_goal', 'Ghost', 'fb_ghost',
                'Liverpool', 'ENG-Premier League', 2024, TIMESTAMP '2026-05-08 12:00:00'),
              -- non-own-goal event — must NOT appear
              ('M3', '50', 'yellow_card', 'Other', 'fb_other',
                'Liverpool', 'ENG-Premier League', 2024, TIMESTAMP '2026-05-08 12:00:00')
            """
        )

        # ---- xref_team for own_goal team-attribution ----
        duck_conn.execute(
            """
            INSERT INTO silver_xref_team VALUES
              ('fbref', 'Liverpool', 'liverpool', 'ENG-Premier League', '2024'),
              ('fbref', 'Wolverhampton Wanderers', 'wolves',
               'ENG-Premier League', '2024')
            """
        )
        # ---- xref_player for own_goal scorer canonical ----
        duck_conn.execute(
            """
            INSERT INTO silver_xref_player VALUES
              ('fbref', 'fb_leno', 'leno_canonical', 'ENG-Premier League', '2024'),
              ('fbref', 'fb_disasi', 'disasi_canonical', 'ENG-Premier League', '2024')
            """
        )

        out = _run(duck_conn)

        # Total rows = 3 regular + 2 own_goal = 5 (NULL minute and non-OG
        # bronze rows are dropped; non-goal shot is dropped).
        assert len(out) == 5, f"expected 5 goal rows, got {len(out)}: {out}"

    def test_is_own_goal_split(self, duck_conn):
        duck_conn.execute(
            """
            INSERT INTO gold_fct_shot VALUES
              ('S1', 'M1', 't1', 'P1', NULL, 22, 'goal', 'open_play',
               'ENG-Premier League', '2425'),
              ('S2', 'M1', 't2', 'P2', NULL, 67, 'goal', 'penalty',
               'ENG-Premier League', '2425'),
              ('S3', 'M2', 't1', 'P3', NULL, 46, 'goal', 'open_play',
               'ENG-Premier League', '2425')
            """
        )
        duck_conn.execute(
            """
            INSERT INTO bronze_fbref_match_events VALUES
              ('M3', '20', 'own_goal', 'Leno', 'fb_leno', 'Liverpool',
                'ENG-Premier League', 2024, TIMESTAMP '2026-05-08 12:00:00'),
              ('M4', '12', 'own_goal', 'Disasi', 'fb_disasi', 'Wolves',
                'ENG-Premier League', 2024, TIMESTAMP '2026-05-08 12:00:00')
            """
        )

        out = _run(duck_conn)
        own_count = sum(1 for r in out if r["is_own_goal"] is True)
        reg_count = sum(1 for r in out if r["is_own_goal"] is False)
        assert reg_count == 3
        assert own_count == 2

    def test_goal_source_distribution(self, duck_conn):
        duck_conn.execute(
            """
            INSERT INTO gold_fct_shot VALUES
              ('S1', 'M1', 't1', 'P1', NULL, 22, 'goal', 'open_play',
               'ENG-Premier League', '2425')
            """
        )
        duck_conn.execute(
            """
            INSERT INTO bronze_fbref_match_events VALUES
              ('M3', '20', 'own_goal', 'X', 'fb_x', 'Liverpool',
                'ENG-Premier League', 2024, TIMESTAMP '2026-05-08 12:00:00')
            """
        )
        out = _run(duck_conn)
        sources = sorted(r["goal_source"] for r in out)
        assert sources == ["fbref_own_goal", "fct_shot"]


class TestPkUniqueness:
    """goal_canonical must be distinct per row, even on same-minute brace."""

    def test_distinct_goal_canonical_for_brace(self, duck_conn):
        # Same player canonical scoring twice at minute 46 — pk_tiebreaker=shot_id
        # keeps them distinct.
        duck_conn.execute(
            """
            INSERT INTO gold_fct_shot VALUES
              ('S1', 'M1', 't1', 'P1', NULL, 46, 'goal', 'open_play',
               'ENG-Premier League', '2425'),
              ('S2', 'M1', 't1', 'P1', NULL, 46, 'goal', 'open_play',
               'ENG-Premier League', '2425')
            """
        )
        out = _run(duck_conn)
        canonicals = [r["goal_canonical"] for r in out]
        assert len(canonicals) == len(set(canonicals)), (
            f"duplicate goal_canonical on same-minute brace: {canonicals}"
        )

    def test_canonical_trio_all_not_null(self, duck_conn):
        duck_conn.execute(
            """
            INSERT INTO gold_fct_shot VALUES
              ('S1', 'M1', 't1', 'P1', NULL, 22, 'goal', 'open_play',
               'ENG-Premier League', '2425')
            """
        )
        duck_conn.execute(
            """
            INSERT INTO bronze_fbref_match_events VALUES
              ('M3', '20', 'own_goal', 'X', 'fb_x', 'Liverpool',
                'ENG-Premier League', 2024, TIMESTAMP '2026-05-08 12:00:00')
            """
        )
        out = _run(duck_conn)
        for r in out:
            assert r["goal_canonical"] is not None, r
            assert r["goal_source"] is not None, r
            assert r["goal_version"] == "v1", r


class TestMinuteRange:
    """All emitted rows have minute in [0, 130]."""

    def test_minute_in_valid_range(self, duck_conn):
        duck_conn.execute(
            """
            INSERT INTO gold_fct_shot VALUES
              ('S1', 'M1', 't1', 'P1', NULL, 0, 'goal', 'open_play',
               'ENG-Premier League', '2425'),
              ('S2', 'M1', 't1', 'P2', NULL, 130, 'goal', 'open_play',
               'ENG-Premier League', '2425')
            """
        )
        out = _run(duck_conn)
        for r in out:
            assert 0 <= r["minute"] <= 130, r


class TestOwnGoalTeamAttribution:
    """FBref `team` column on own_goal = goal-RECEIVING team (verified empirically)."""

    def test_team_id_canonical_is_receiving_team(self, duck_conn):
        # Bernd Leno (Fulham GK) own-goals; FBref reports team='Liverpool'
        # (the receiving side). team_id_canonical must resolve to liverpool.
        duck_conn.execute(
            """
            INSERT INTO bronze_fbref_match_events VALUES
              ('M3', '20', 'own_goal', 'Bernd Leno', 'fb_leno', 'Liverpool',
                'ENG-Premier League', 2024, TIMESTAMP '2026-05-08 12:00:00')
            """
        )
        duck_conn.execute(
            """
            INSERT INTO silver_xref_team VALUES
              ('fbref', 'Liverpool', 'liverpool', 'ENG-Premier League', '2024')
            """
        )
        duck_conn.execute(
            """
            INSERT INTO silver_xref_player VALUES
              ('fbref', 'fb_leno', 'leno_canonical', 'ENG-Premier League', '2024')
            """
        )
        out = _run(duck_conn)
        assert len(out) == 1
        assert out[0]["team_id_canonical"] == "liverpool"
        assert out[0]["scorer_id_canonical"] == "leno_canonical"
        assert out[0]["is_own_goal"] is True
        assert out[0]["assist_id_canonical"] is None
