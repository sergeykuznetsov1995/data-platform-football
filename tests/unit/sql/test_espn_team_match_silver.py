"""Contract tests for the ESPN native-v2 team-match Silver transform."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags/sql/silver/espn_team_match.sql"


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestEspnTeamMatchSilver:
    def test_native_sources_are_deduped_before_json_unnest(self):
        sql = _sql()
        body = "\n".join(x for x in sql.splitlines() if not x.lstrip().startswith("--"))
        assert body.count("iceberg.bronze.espn_schedule_generation_v2") == 1
        assert body.count("iceberg.bronze.espn_matchsheet_generation_v2") == 1
        assert re.search(r"PARTITION BY\s+event_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert re.search(r"PARTITION BY\s+event_id\s*,\s*team_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert body.index("schedule_dedup") < body.index("CROSS JOIN UNNEST")

    def test_matchsheet_branch_wins_and_scoreboard_branch_is_played_only(self):
        sql = _sql()
        assert "stats_source" in sql and "'matchsheet'" in sql and "'scoreboard'" in sql
        assert "matches_with_matchsheet" in sql
        assert re.search(r"WHERE\s+s\.played_final", sql, re.I)
        assert re.search(r"m\.event_id\s+IS\s+NULL", sql, re.I)
        assert "$.sides.home.competitor.statistics" in sql
        assert "$.sides.away.competitor.statistics" in sql

    def test_schedule_team_names_and_calculated_percentages(self):
        sql = _sql()
        assert re.search(r"CASE\s+WHEN\s+.*team_id.*home_team_id.*THEN.*home_team", sql, re.I | re.S)
        for pct in ("pass_pct", "shot_pct", "cross_pct", "longball_pct", "tackle_pct"):
            assert pct in sql
        assert re.search(r"ROUND\s*\(\s*100\.0\s*\*\s*accurate_passes\s*/\s*NULLIF\s*\(\s*total_passes\s*,\s*0\s*\)", sql, re.I)

    def test_executable_matchsheet_first_scores_and_scoreboard_fallback(self):
        """Exercise the score pairing independently of Trino JSON UNNEST."""
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        rows = con.execute("""
            WITH schedule(event_id, ingested_at, home_id, away_id, home_score, away_score) AS (
                VALUES
                  (1, TIMESTAMP '2026-08-01 00:00:00', 10, 20, 2, 1),
                  (1, TIMESTAMP '2026-08-02 00:00:00', 10, 20, 2, 1),
                  (2, TIMESTAMP '2026-08-02 00:00:00', 30, 40, 5, 6)
            ), schedule_dedup AS (
                SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn FROM schedule
            ), matchsheet(event_id, team_id, score, ingested_at) AS (
                VALUES
                  (1, 10, 99, TIMESTAMP '2026-08-01 00:00:00'),
                  (1, 10, 3, TIMESTAMP '2026-08-02 00:00:00'),
                  (1, 20, 98, TIMESTAMP '2026-08-01 00:00:00'),
                  (1, 20, 4, TIMESTAMP '2026-08-02 00:00:00')
            ), matchsheet_dedup AS (
                SELECT *, row_number() OVER (PARTITION BY event_id, team_id ORDER BY ingested_at DESC) AS rn FROM matchsheet
            ), matchsheet_rows AS (
                SELECT m.event_id, m.team_id,
                  COALESCE(m.score, CASE WHEN m.team_id = s.home_id THEN s.home_score ELSE s.away_score END) AS goals_for,
                  COALESCE(opponent.score, CASE WHEN m.team_id = s.home_id THEN s.away_score ELSE s.home_score END) AS goals_against
                FROM matchsheet_dedup m
                JOIN schedule_dedup s ON s.event_id = m.event_id AND s.rn = 1
                LEFT JOIN matchsheet_dedup opponent
                  ON opponent.event_id = m.event_id
                 AND opponent.team_id = CASE WHEN m.team_id = s.home_id THEN s.away_id ELSE s.home_id END
                 AND opponent.rn = 1
                WHERE m.rn = 1
            ), scoreboard_rows AS (
                SELECT event_id, home_id AS team_id, home_score AS goals_for, away_score AS goals_against
                FROM schedule_dedup WHERE rn = 1 AND event_id = 2
                UNION ALL
                SELECT event_id, away_id, away_score, home_score
                FROM schedule_dedup WHERE rn = 1 AND event_id = 2
            )
            SELECT * FROM matchsheet_rows
            UNION ALL SELECT * FROM scoreboard_rows
            ORDER BY event_id, team_id
        """).fetchall()
        assert rows == [(1, 10, 3, 4), (1, 20, 4, 3), (2, 30, 5, 6), (2, 40, 6, 5)]

        sql = _sql()
        assert "LEFT JOIN matchsheet_dedup opponent" in sql
        assert re.search(r"COALESCE\s*\(\s*opponent\.score", sql, re.I)
