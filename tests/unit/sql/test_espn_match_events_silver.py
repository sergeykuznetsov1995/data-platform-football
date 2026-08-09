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


def _output(tree: exp.Select, name: str) -> exp.Expression:
    return next(expression for expression in tree.expressions if expression.alias_or_name == name)


def _scalar_path(expression: exp.Expression) -> exp.JSONExtractScalar:
    return next(expression.find_all(exp.JSONExtractScalar))


class TestEspnMatchEventsSilver:
    def test_schedule_is_deduplicated_before_details_unnest_and_played_only(self):
        sql = _sql()
        body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        assert body.count("iceberg.bronze.espn_schedule_generation_v2") == 1
        assert re.search(r"PARTITION BY\s+event_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert body.index("schedule_dedup") < body.index("CROSS JOIN UNNEST")
        assert re.search(r"WHERE\s+s\.played_final", body, re.I)
        assert re.search(r"WITH\s+ORDINALITY\s+AS\s+u\s*\(\s*detail\s*,\s*seq\s*\)", body, re.I)

    def test_ast_binds_each_output_to_its_exact_detail_path_and_ordinality(self):
        tree = _tree()
        for alias, path in {
            "event_type": "'$.type.text'",
            "event_type_id": "'$.type.id'",
            "clock_display": "'$.clock.displayValue'",
            "clock_seconds": "'$.clock.value'",
            "team_id": "'$.team.id'",
            "athlete_id": "'$.athletesInvolved[0].id'",
            "player_name": "'$.athletesInvolved[0].displayName'",
            "player_jersey": "'$.athletesInvolved[0].jersey'",
            "player_position": "'$.athletesInvolved[0].position'",
            "score_value": "'$.scoreValue'",
            "is_own_goal": "'$.ownGoal'",
            "is_penalty": "'$.penaltyKick'",
            "is_yellow_card": "'$.yellowCard'",
            "is_red_card": "'$.redCard'",
            "is_shootout": "'$.shootout'",
        }.items():
            scalar = _scalar_path(_output(tree, alias))
            assert scalar.expression.sql(dialect="trino") == path
            assert isinstance(scalar.this, exp.Column)
            assert (scalar.this.table, scalar.this.name) == ("s", "detail")

        is_goal = _output(tree, "is_goal")
        assert {
            node.expression.sql(dialect="trino") for node in is_goal.find_all(exp.JSONExtractScalar)
        } == {"'$.scoringPlay'", "'$.shootout'"}
        assert any(isinstance(node, exp.Not) for node in is_goal.walk())

        seq = _output(tree, "seq")
        assert isinstance(seq, exp.Column) and (seq.table, seq.name) == ("s", "seq")
        event_details = next(cte.this for cte in tree.args["with_"].expressions if cte.alias_or_name == "event_details")
        unnest = next(event_details.find_all(exp.Unnest))
        assert unnest.args["offset"].name == "seq"
        assert [column.name for column in unnest.args["alias"].columns] == ["detail"]

    def test_clock_output_aliases_bind_to_single_backslash_trino_patterns(self):
        tree = _tree()
        for alias, pattern in (("minute", r"^(\d+)"), ("plus_minute", r"\+(\d+)")):
            regexp = next(_output(tree, alias).find_all(exp.RegexpExtract))
            assert isinstance(regexp.this, exp.JSONExtractScalar)
            assert regexp.this.expression.sql(dialect="trino") == "'$.clock.displayValue'"
            assert (regexp.this.this.table, regexp.this.this.name) == ("s", "detail")
            assert regexp.expression.this == pattern
            assert "\\\\" not in regexp.expression.this
            assert regexp.args["group"].this == "1"

    def test_executable_latest_snapshot_ordinality_clock_and_shootout_behavior(self):
        """DuckDB checks scalar behavior; production nodes are bound above."""
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
