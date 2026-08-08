"""Contract tests for the ESPN native-v2 player-match Silver transform."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags/sql/silver/espn_player_match_aggregate.sql"


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestEspnPlayerMatchAggregateSilver:
    def test_native_lineup_dedup_is_team_qualified_and_joined_to_schedule(self):
        sql = _sql()
        body = "\n".join(x for x in sql.splitlines() if not x.lstrip().startswith("--"))
        assert body.count("iceberg.bronze.espn_lineup_generation_v2") == 1
        assert body.count("iceberg.bronze.espn_schedule_generation_v2") == 1
        assert re.search(r"PARTITION BY\s+event_id\s*,\s*team_id\s*,\s*athlete_id", body, re.I)
        assert re.search(r"JOIN\s+schedule_dedup\s+s\s+ON\s+s\.event_id\s*=\s+l\.event_id", body, re.I)
        assert re.search(r"WHERE\s+s\.played_final", body, re.I)

    def test_json_jersey_position_groups_and_best_effort_minutes(self):
        sql = _sql()
        assert "$.jersey" in sql
        assert re.search(r"COALESCE\s*\(\s*json_extract_scalar\(.*\$\.jersey.*NULLIF\s*\(\s*jersey", sql, re.I | re.S)
        for token in ("Goalkeeper", "Sweeper", "Midfielder", "Forward", "'GK'", "'DF'", "'MF'", "'FW'"):
            assert token in sql
        assert re.search(r"is_starter\s+OR\s+subbed_in", sql, re.I)
        assert "STATUS_FINAL_PEN" in sql and "STATUS_FINAL_AET" in sql
        assert "GREATEST" in sql

    def test_no_plays_is_zero_and_final_pen_didscore_keeps_shootout_context(self):
        sql = _sql()
        assert re.search(r"COALESCE\s*\(\s*cardinality\s*\(\s*filter", sql, re.I)
        assert "$.didScore" in sql and "$.didAssist" in sql
        assert "STATUS_FINAL_PEN" in sql

    def test_executable_final_pen_didscore_is_counted_as_is(self):
        """Lineup plays are the only input; shootout detail is deliberately absent.

        Live calibration established that FINAL_PEN lineup didScore excludes series
        kicks, so a scoreboard cap/filter would discard valid data rather than fix it.
        """
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        rows = con.execute("""
            WITH lineup_plays(event_id, status, did_score) AS (
                VALUES (1, 'STATUS_FINAL_PEN', TRUE), (1, 'STATUS_FINAL_PEN', FALSE),
                       (2, 'STATUS_FULL_TIME', TRUE)
            ), regulation_score(event_id, goals) AS (
                VALUES (1, 1), (2, 1)
            ), shootout_details(event_id, made_kicks) AS (
                VALUES (1, 5)
            )
            SELECT p.event_id, SUM(CASE WHEN p.did_score THEN 1 ELSE 0 END) AS goals_events,
                   s.goals AS regulation_goals
            FROM lineup_plays p
            JOIN regulation_score s ON s.event_id = p.event_id
            GROUP BY p.event_id, s.goals
            ORDER BY p.event_id
        """).fetchall()
        assert rows == [(1, 1, 1), (2, 1, 1)]

        formula = _sql().split("AS goals_events", 1)[0].rsplit("COALESCE", 1)[-1]
        assert "$.didScore" in formula
        assert "shootout" not in formula.lower() and "clock" not in formula.lower()

    def test_executable_position_group_agrees_with_roster_abbreviation(self):
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        rows = con.execute("""
            WITH lineup(position, roster_position) AS (
                VALUES ('Goalkeeper', 'G'), ('Center Left Defender', 'D'),
                       ('Attacking Midfielder', 'M'), ('Center Right Forward', 'F'),
                       ('Substitute', NULL)
            )
            SELECT position,
              CASE WHEN position = 'Goalkeeper' THEN 'GK'
                   WHEN position LIKE '%Back%' OR position LIKE '%Defender%' OR position = 'Sweeper' THEN 'DF'
                   WHEN position LIKE '%Midfielder%' THEN 'MF'
                   WHEN position LIKE '%Forward%' THEN 'FW' END AS position_group,
              CASE roster_position WHEN 'G' THEN 'GK' WHEN 'D' THEN 'DF'
                                   WHEN 'M' THEN 'MF' WHEN 'F' THEN 'FW' END AS roster_group
            FROM lineup ORDER BY position
        """).fetchall()
        assert {(group, roster) for _, group, roster in rows if roster} == {
            ("GK", "GK"), ("DF", "DF"), ("MF", "MF"), ("FW", "FW")
        }
        assert "clean_position LIKE '%Defender%'" in _sql()

    def test_fixture_minutes_and_team_qualified_pk_are_executable(self):
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        rows = con.execute("""
            SELECT team_id, athlete_id,
                   CASE WHEN is_starter OR subbed_in THEN GREATEST(COALESCE(sub_out, 90) - COALESCE(sub_in, 0), 0) END AS minutes
            FROM (VALUES (10, 99, FALSE, FALSE, NULL, NULL), (20, 99, TRUE, FALSE, 0, 90))
                 AS x(team_id, athlete_id, is_starter, subbed_in, sub_in, sub_out)
        """).fetchall()
        assert rows == [(10, 99, None), (20, 99, 90)]
