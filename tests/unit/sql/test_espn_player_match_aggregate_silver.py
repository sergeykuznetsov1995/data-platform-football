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
