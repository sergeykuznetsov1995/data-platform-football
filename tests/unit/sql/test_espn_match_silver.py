"""Contract tests for the ESPN native-v2 match Silver transform."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags/sql/silver/espn_match.sql"


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestEspnMatchSilver:
    def test_native_source_dedup_and_pure_select(self):
        sql = _sql()
        body = "\n".join(x for x in sql.splitlines() if not x.lstrip().startswith("--"))
        assert body.count("iceberg.bronze.espn_schedule_generation_v2") == 1
        assert "bronze_src_schedule" in body
        assert re.search(r"PARTITION BY\s+event_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert "CREATE TABLE" not in body.upper()

    def test_non_played_scores_are_nullified_and_attendance_sentinel_is_null(self):
        sql = _sql()
        assert re.search(r"CASE\s+WHEN\s+played_final\s+THEN\s+home_score\s+END", sql, re.I)
        assert re.search(r"CASE\s+WHEN\s+played_final\s+THEN\s+away_score\s+END", sql, re.I)
        assert re.search(r"NULLIF\s*\(\s*attendance_value\s*,\s*0\s*\)", sql, re.I)

    def test_shootout_winner_stage_group_and_display_name_season_slug(self):
        sql = _sql()
        assert "$.sides.home.competitor.shootoutScore" in sql
        assert "$.sides.away.competitor.shootoutScore" in sql
        assert "$.sides.home.competitor.winner" in sql
        assert "$.sides.away.competitor.winner" in sql
        assert "group-stage" in sql and "league-phase" in sql
        assert "regexp_extract(alt_game_note" in sql
        assert "$.source.league.season.displayName" in sql
        assert "source_season_year" in sql

    def test_referee_is_aggregated_before_schedule_join_and_partition_keys_trail(self):
        sql = _sql()
        assert "referee_by_event" in sql
        assert re.search(r"GROUP BY\s+event_id", sql, re.I)
        final = sql.rsplit("SELECT", 1)[-1]
        assert re.search(r"competition_slug\s+AS\s+league", final, re.I)
        assert re.search(r"CAST\s*\(\s*source_season_year\s+AS\s+varchar\s*\)\s+AS\s+season", final, re.I)
