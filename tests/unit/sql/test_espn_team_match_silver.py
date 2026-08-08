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

    def test_fixture_math_for_scoreboard_fallback_is_executable(self):
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        row = con.execute("SELECT ROUND(100.0 * 45 / NULLIF(50, 0), 2), 100.0 * 0 / NULLIF(0, 0)").fetchone()
        assert row == (90.0, None)
