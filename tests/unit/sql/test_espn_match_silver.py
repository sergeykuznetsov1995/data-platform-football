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

    def test_executable_latest_snapshot_season_slug_and_group_parsing(self):
        """DuckDB fixture exercises the same scalar expressions as the Trino SQL.

        JSON extraction/array casts are Trino-specific, but dedup and the scalar
        regexp expressions below are executable in DuckDB.
        """
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        rows = con.execute(r"""
            WITH schedule(event_id, ingested_at, display_name, alt_game_note, source_year) AS (
                VALUES
                  (1, TIMESTAMP '2026-08-01 00:00:00', 'Premier League 2025-26', 'WCQ - AFC, Group A', 2025),
                  (1, TIMESTAMP '2026-08-02 00:00:00', 'Premier League 2026-27', 'WCQ - AFC, Group B', 2026),
                  (2, TIMESTAMP '2026-08-02 00:00:00', 'FIFA Club World Cup 2026', 'Cup, Final', 2026),
                  (3, TIMESTAMP '2026-08-02 00:00:00', 'Unknown', 'League', 2031)
            ), dedup AS (
                SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
                FROM schedule
            )
            SELECT event_id,
              CASE
                WHEN regexp_extract(display_name, '(\d{4})\s*[-/]\s*(\d{2}|\d{4})', 1) <> ''
                  THEN substr(regexp_extract(display_name, '(\d{4})\s*[-/]\s*(\d{2}|\d{4})', 1), 3, 2)
                       || right(regexp_extract(display_name, '(\d{4})\s*[-/]\s*(\d{2}|\d{4})', 2), 2)
                WHEN regexp_extract(display_name, '(\d{4})', 1) <> ''
                  THEN regexp_extract(display_name, '(\d{4})', 1)
                ELSE CAST(source_year AS varchar)
              END AS season_slug_platform,
              regexp_extract(alt_game_note, '(Group\s+[A-Z0-9]+)\s*$', 1) AS group_name
            FROM dedup WHERE rn = 1 ORDER BY event_id
        """).fetchall()
        assert rows == [(1, "2627", "Group B"), (2, "2026", ""), (3, "2031", "")]

        # Tie the executable fixture to the production expression and prevent
        # the double-backslash Trino literal regression.
        sql = _sql()
        assert r"(\d{4})\s*[-/]\s*(\d{2}|\d{4})" in sql
        assert r"(Group\s+[A-Z0-9]+)\s*$" in sql
        assert r"\\d" not in sql and r"\\s" not in sql

    def test_referee_is_aggregated_before_schedule_join_and_partition_keys_trail(self):
        sql = _sql()
        assert "referee_by_event" in sql
        assert re.search(r"GROUP BY\s+event_id", sql, re.I)
        final = sql.rsplit("SELECT", 1)[-1]
        assert re.search(r"competition_slug\s+AS\s+league", final, re.I)
        assert re.search(r"CAST\s*\(\s*source_season_year\s+AS\s+varchar\s*\)\s+AS\s+season", final, re.I)

    def test_all_native_v2_transforms_have_required_header_sections(self):
        for name in (
            "espn_match.sql",
            "espn_team_match.sql",
            "espn_player_match_aggregate.sql",
        ):
            sql = (ROOT / "dags" / "sql" / "silver" / name).read_text(encoding="utf-8")
            assert "-- Sources (native v2):" in sql
            assert "-- Notes:" in sql
            assert "-- DAG integration:" in sql
