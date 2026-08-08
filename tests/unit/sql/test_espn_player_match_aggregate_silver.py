"""Contract tests for the ESPN native-v2 player-match Silver transform."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
exp = sqlglot.exp


ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags/sql/silver/espn_player_match_aggregate.sql"


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _tree() -> exp.Select:
    return sqlglot.parse_one(_sql(), read="trino")


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

    def test_schedule_partition_fields_are_aliased_before_lineup_passthrough(self):
        tree = _tree()
        lineup_played = next(
            cte.this
            for cte in tree.args["with_"].expressions
            if cte.alias_or_name == "lineup_played"
        )
        aliases = {
            expression.alias: expression.this
            for expression in lineup_played.expressions
            if isinstance(expression, exp.Alias)
        }
        assert {
            "schedule_competition_slug": ("s", "competition_slug"),
            "schedule_source_season_year": ("s", "source_season_year"),
        } == {
            name: (source.table, source.name)
            for name, source in aliases.items()
            if name.startswith("schedule_")
        }

        output = {
            expression.alias: expression.this
            for expression in tree.expressions
            if isinstance(expression, exp.Alias)
        }
        assert output["league"].name == "schedule_competition_slug"
        assert output["season"].this.name == "schedule_source_season_year"

    def test_json_jersey_position_groups_and_best_effort_minutes(self):
        tree = _tree()
        jersey_coalesce = next(
            node for node in tree.find_all(exp.Coalesce)
            if isinstance(node.this, exp.JSONExtractScalar)
            and node.this.expression.sql(dialect="trino") == "'$.jersey'"
        )
        assert isinstance(jersey_coalesce.expressions[0], exp.Nullif)
        assert jersey_coalesce.expressions[0].this.name == "jersey"

        position_group = next(
            alias for alias in tree.find_all(exp.Alias)
            if alias.alias == "position_group" and isinstance(alias.this, exp.Case)
        ).this
        assert {clause.args["true"].this for clause in position_group.args["ifs"]} == {"GK", "DF", "MF", "FW"}
        position_patterns = {
            node.expression.this for node in position_group.find_all(exp.Like)
        }
        assert {"%Back%", "%Defender%", "%Midfielder%", "%Forward%"} <= position_patterns

        minutes = next(
            alias for alias in tree.find_all(exp.Alias)
            if alias.alias == "minutes_played" and isinstance(alias.this, exp.Case)
        ).this
        assert {column.name for column in minutes.find_all(exp.Column)} >= {"is_starter", "subbed_in", "status"}
        assert {literal.this for literal in minutes.find_all(exp.Literal)} >= {"STATUS_FINAL_AET", "STATUS_FINAL_PEN", "120", "90"}

    def test_no_plays_is_zero_and_final_pen_didscore_keeps_shootout_context(self):
        tree = _tree()
        goals_events = next(
            alias for alias in tree.find_all(exp.Alias)
            if alias.alias == "goals_events" and isinstance(alias.this, exp.Coalesce)
        ).this
        assert isinstance(goals_events.this, exp.ArraySize)
        assert goals_events.expressions[0].this == "0"
        did_score_filter = goals_events.this.this
        assert isinstance(did_score_filter, exp.ArrayFilter)
        assert isinstance(did_score_filter.this, exp.Cast)
        json_extract = did_score_filter.this.this
        assert isinstance(json_extract, exp.JSONExtract)
        assert json_extract.expression.sql(dialect="trino") == "'$.plays'"
        predicate = did_score_filter.expression.this
        assert isinstance(predicate, exp.EQ)
        assert isinstance(predicate.this, exp.JSONExtractScalar)
        assert predicate.this.expression.sql(dialect="trino") == "'$.didScore'"
        assert predicate.expression.this == "true"

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

        goals_events = next(
            alias for alias in _tree().find_all(exp.Alias)
            if alias.alias == "goals_events" and isinstance(alias.this, exp.Coalesce)
        )
        assert not any(
            isinstance(node, exp.JSONExtractScalar)
            and node.expression.sql(dialect="trino") in {"'$.shootout'", "'$.clock'"}
            for node in goals_events.this.walk()
        )

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
        position_group = next(
            alias for alias in _tree().find_all(exp.Alias)
            if alias.alias == "position_group" and isinstance(alias.this, exp.Case)
        ).this
        assert any(
            isinstance(node, exp.Like)
            and node.this.name == "clean_position"
            and node.expression.this == "%Defender%"
            for node in position_group.walk()
        )

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
