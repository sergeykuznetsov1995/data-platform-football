"""Contract tests for the ESPN native-v2 substitutions Silver transform."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
exp = sqlglot.exp


ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags/sql/silver/espn_substitutions.sql"
pytestmark = pytest.mark.unit


def _template() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _sql() -> str:
    from utils.espn_season_mapping import render_espn_downstream_sql

    return render_espn_downstream_sql(_template())


def _tree() -> exp.Select:
    return sqlglot.parse_one(_sql(), read="trino")


def _output(tree: exp.Select, name: str) -> exp.Expression:
    return next(expression for expression in tree.expressions if expression.alias_or_name == name)


def _scalar_path(expression: exp.Expression) -> exp.JSONExtractScalar:
    return next(expression.find_all(exp.JSONExtractScalar))


class TestEspnSubstitutionsSilver:
    def test_native_sources_dedup_before_inbound_filter_and_schedule_played_join(self):
        sql = _sql()
        body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        assert body.count("iceberg.bronze.espn_lineup AS es_source") == 1
        assert body.count("iceberg.bronze.espn_schedule AS es_source") == 1
        assert re.search(r"PARTITION BY\s+event_id\s*,\s*team_id\s*,\s*athlete_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert re.search(r"PARTITION BY\s+event_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert body.index("lineup_dedup") < body.index("subbed_in")
        assert re.search(r"JOIN\s+schedule_dedup\s+s\s+ON\s+s\.event_id\s*=\s+l\.event_id", body, re.I)
        assert re.search(r"WHERE\s+s\.played_final\s+AND\s+l\.subbed_in", body, re.I)

    def test_ast_binds_each_inbound_output_to_the_exact_source_or_json_path(self):
        tree = _tree()
        for alias, column in {
            "event_id": "event_id", "team_id": "team_id", "team": "team",
            "player_in_id": "athlete_id", "player_in_name": "player",
        }.items():
            output = _output(tree, alias)
            source = output.this if isinstance(output, exp.Alias) else output
            assert isinstance(source, exp.Column)
            assert (source.table, source.name) == ("l", column)

        for alias, path in {
            "player_out_id": "'$.subbedInFor.athlete.id'",
            "player_out_name": "'$.subbedInFor.athlete.displayName'",
            "player_out_jersey": "'$.subbedInFor.jersey'",
        }.items():
            scalar = _scalar_path(_output(tree, alias))
            assert scalar.expression.sql(dialect="trino") == path
            assert (scalar.this.table, scalar.this.name) == ("l", "extra_json")

        player_in_jersey = _output(tree, "player_in_jersey")
        jersey_scalar = _scalar_path(player_in_jersey)
        assert jersey_scalar.expression.sql(dialect="trino") == "'$.jersey'"
        assert (jersey_scalar.this.table, jersey_scalar.this.name) == ("l", "extra_json")
        nullif = next(player_in_jersey.find_all(exp.Nullif))
        assert (nullif.this.table, nullif.this.name) == ("l", "jersey")
        assert "'$.subbedInFor.athlete.jersey'" not in {
            node.expression.sql(dialect="trino") for node in player_in_jersey.find_all(exp.JSONExtractScalar)
        }

    def test_inbound_cte_binds_played_schedule_join_predicate_and_team_qualified_pk(self):
        tree = _tree()
        inbound = next(cte.this for cte in tree.args["with_"].expressions if cte.alias_or_name == "inbound_substitutions")
        join = next(inbound.find_all(exp.Join))
        assert join.this.name == "schedule_dedup" and join.this.alias == "s"
        join_columns = {(column.table, column.name) for column in join.args["on"].find_all(exp.Column)}
        assert join_columns == {("s", "event_id"), ("l", "event_id")}
        predicate_columns = {(column.table, column.name) for column in inbound.args["where"].this.find_all(exp.Column)}
        assert predicate_columns == {("s", "played_final"), ("l", "subbed_in")}

        lineup_dedup = next(cte.this for cte in tree.args["with_"].expressions if cte.alias_or_name == "lineup_dedup")
        window = next(lineup_dedup.find_all(exp.Window))
        assert [column.name for column in window.args["partition_by"]] == ["event_id", "team_id", "athlete_id"]
        ordered = window.args["order"].expressions[0]
        assert ordered.this.name == "_ingested_at" and ordered.args["desc"] is True

    def test_schedule_platform_partitions_are_bound_before_lineup_passthrough(self):
        tree = _tree()
        inbound = next(
            cte.this
            for cte in tree.args["with_"].expressions
            if cte.alias_or_name == "inbound_substitutions"
        )
        passthrough = {
            (source.table, source.name)
            for source in inbound.expressions
            if isinstance(source, exp.Column) and source.table == "s"
        }
        assert {("s", "platform_league"), ("s", "platform_season_slug")} <= passthrough

        output = {
            expression.alias: expression.this
            for expression in tree.expressions
            if isinstance(expression, exp.Alias)
        }
        assert output["league"].this.name == "platform_league"
        assert output["season"].this.name == "platform_season_slug"

    def test_executable_dedup_inbound_only_pairing_and_json_jersey_priority(self):
        """DuckDB checks scalar behavior; AST assertions bind the production paths."""
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        rows = con.execute("""
            WITH lineup(event_id, team_id, athlete_id, ingested_at, subbed_in, sub_in, json_jersey, bronze_jersey, out_id) AS (
                VALUES
                  (1, 10, 99, TIMESTAMP '2026-08-01 00:00:00', TRUE, '65', '5', '50', 77),
                  (1, 10, 99, TIMESTAMP '2026-08-02 00:00:00', TRUE, '65', '5', '50', 77),
                  (1, 10, 77, TIMESTAMP '2026-08-02 00:00:00', FALSE, 'end', '9', '9', NULL),
                  (1, 10, 88, TIMESTAMP '2026-08-02 00:00:00', FALSE, 'end', '8', '8', NULL)
            ), dedup AS (
                SELECT *, row_number() OVER (PARTITION BY event_id, team_id, athlete_id ORDER BY ingested_at DESC) AS rn
                FROM lineup
            )
            SELECT event_id, team_id, athlete_id AS player_in_id, CAST(sub_in AS integer) AS minute,
                   COALESCE(json_jersey, NULLIF(bronze_jersey, '')) AS player_in_jersey, out_id AS player_out_id
            FROM dedup WHERE rn = 1 AND subbed_in
        """).fetchall()
        assert rows == [(1, 10, 99, 65, "5", 77)]

    def test_header_lineage_and_trailing_partitions(self):
        sql = _sql()
        assert "-- Sources (native v2):" in sql
        assert "-- Notes:" in sql and "-- Footguns:" in sql and "-- DAG integration:" in sql
        assert "outgoing-only" in sql.lower()
        final = sql.rsplit("SELECT", 1)[-1]
        assert re.search(r"l\._ingested_at\s+AS\s+_bronze_ingested_at", final, re.I)
        assert re.search(r"l\.platform_league\s+AS\s+league", final, re.I)
        assert re.search(r"l\.platform_season_slug\s+AS\s+season", final, re.I)
