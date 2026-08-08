"""Contract tests for the ESPN native-v2 match-events Silver transform."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
exp = sqlglot.exp


ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags/sql/silver/espn_match_events.sql"
pytestmark = pytest.mark.unit


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _tree() -> exp.Select:
    return sqlglot.parse_one(_sql(), read="trino")


class TestEspnMatchEventsSilver:
    def test_schedule_is_deduplicated_before_details_unnest_and_played_only(self):
        sql = _sql()
        body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        assert body.count("iceberg.bronze.espn_schedule_generation_v2") == 1
        assert re.search(r"PARTITION BY\s+event_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert body.index("schedule_dedup") < body.index("CROSS JOIN UNNEST")
        assert re.search(r"WHERE\s+s\.played_final", body, re.I)
        assert re.search(r"WITH\s+ORDINALITY\s+AS\s+u\s*\(\s*detail\s*,\s*seq\s*\)", body, re.I)

    def test_ast_binds_all_detail_json_paths_and_modeled_flags(self):
        tree = _tree()
        paths = {
            node.expression.sql(dialect="trino")
            for node in tree.find_all(exp.JSONExtractScalar)
        }
        assert {
            "'$.type.text'", "'$.type.id'", "'$.clock.displayValue'", "'$.clock.value'",
            "'$.team.id'", "'$.athletesInvolved[0].id'", "'$.athletesInvolved[0].displayName'",
            "'$.athletesInvolved[0].jersey'", "'$.athletesInvolved[0].position'",
            "'$.scoringPlay'", "'$.shootout'", "'$.scoreValue'", "'$.ownGoal'",
            "'$.penaltyKick'", "'$.yellowCard'", "'$.redCard'",
        } <= paths
        assert "'$.athletesInvolved[0].athlete.id'" not in paths

        is_goal = next(alias for alias in tree.find_all(exp.Alias) if alias.alias == "is_goal").this
        goal_paths = {node.expression.sql(dialect="trino") for node in is_goal.find_all(exp.JSONExtractScalar)}
        assert {"'$.scoringPlay'", "'$.shootout'"} <= goal_paths
        assert any(isinstance(node, exp.Not) for node in is_goal.walk())

        flags = {alias.alias: alias.this for alias in tree.find_all(exp.Alias)}
        for name in ("is_own_goal", "is_penalty", "is_yellow_card", "is_red_card", "is_shootout"):
            assert name in flags
            assert any(isinstance(node, exp.JSONExtractScalar) for node in flags[name].walk())

    def test_executable_latest_snapshot_ordinality_clock_and_shootout_behavior(self):
        """DuckDB covers behavior; AST assertions above bind paths to production."""
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        rows = con.execute("""
            WITH schedule(event_id, ingested_at, played_final, details) AS (
                VALUES
                  (1, TIMESTAMP '2026-08-01 00:00:00', TRUE,
                   [{'clock_display': '39''', 'scoring_play': TRUE, 'shootout': FALSE},
                    {'clock_display': '90''+1''', 'scoring_play': TRUE, 'shootout': TRUE}]),
                  (1, TIMESTAMP '2026-08-02 00:00:00', TRUE,
                   [{'clock_display': '90''+3''', 'scoring_play': TRUE, 'shootout': FALSE},
                    {'clock_display': '120''', 'scoring_play': FALSE, 'shootout': FALSE}]),
                  (2, TIMESTAMP '2026-08-02 00:00:00', FALSE,
                   [{'clock_display': '5''', 'scoring_play': TRUE, 'shootout': FALSE}])
            ), dedup AS (
                SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
                FROM schedule
            )
            SELECT event_id, ordinality AS seq,
                   CAST(regexp_extract(detail.clock_display, '^(\\d+)', 1) AS integer) AS minute,
                   TRY_CAST(regexp_extract(detail.clock_display, '\\+(\\d+)', 1) AS integer) AS plus_minute,
                   detail.scoring_play AND NOT detail.shootout AS is_goal
            FROM dedup CROSS JOIN UNNEST(details) WITH ORDINALITY AS t(detail, ordinality)
            WHERE rn = 1 AND played_final
            ORDER BY event_id, seq
        """).fetchall()
        assert rows == [(1, 1, 90, 3, True), (1, 2, 120, None, False)]

    def test_header_lineage_and_trailing_partitions(self):
        sql = _sql()
        assert "-- Sources (native v2):" in sql
        assert "-- Notes:" in sql and "-- Footguns:" in sql and "-- DAG integration:" in sql
        final = sql.rsplit("SELECT", 1)[-1]
        assert re.search(r"s\._ingested_at\s+AS\s+_bronze_ingested_at", final, re.I)
        assert re.search(r"s\.competition_slug\s+AS\s+league", final, re.I)
        assert re.search(r"CAST\s*\(\s*s\.source_season_year\s+AS\s+varchar\s*\)\s+AS\s+season", final, re.I)
