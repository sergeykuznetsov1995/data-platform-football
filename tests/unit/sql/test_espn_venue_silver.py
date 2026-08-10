"""Contract tests for the ESPN native-v2 venue Silver transform."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
exp = sqlglot.exp


ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags/sql/silver/espn_venue.sql"
pytestmark = pytest.mark.unit


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _tree() -> exp.Select:
    return sqlglot.parse_one(_sql(), read="trino")


def _output(tree: exp.Select, name: str) -> exp.Expression:
    return next(expression for expression in tree.expressions if expression.alias_or_name == name)


class TestEspnVenueSilver:
    def test_schedule_then_venue_latest_dedup_and_non_null_venue_id(self):
        sql = _sql()
        body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        assert body.count("iceberg.bronze.espn_schedule_generation_v2") == 1
        assert re.search(r"PARTITION BY\s+event_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert re.search(r"PARTITION BY\s+venue_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert body.index("schedule_dedup") < body.index("venue_dedup")
        assert re.search(r"WHERE\s+venue_id\s+IS\s+NOT\s+NULL", body, re.I)

    def test_ast_binds_venue_outputs_and_both_latest_row_number_stages(self):
        tree = _tree()
        name = _output(tree, "venue_name")
        assert isinstance(name.this, exp.Trim)
        assert isinstance(name.this.this, exp.Column) and name.this.this.name == "venue"
        for alias, path in (("city", "'$.venue.address.city'"), ("country", "'$.venue.address.country'")):
            scalar = next(_output(tree, alias).find_all(exp.JSONExtractScalar))
            assert scalar.expression.sql(dialect="trino") == path
            assert isinstance(scalar.this, exp.Column) and scalar.this.name == "extra_json"

        schedule_dedup = next(cte.this for cte in tree.args["with_"].expressions if cte.alias_or_name == "schedule_dedup")
        venue_dedup = next(cte.this for cte in tree.args["with_"].expressions if cte.alias_or_name == "venue_dedup")
        for cte, partition, ordered_column in (
            (schedule_dedup, "event_id", "_ingested_at"),
            (venue_dedup, "venue_id", "_ingested_at"),
        ):
            window = next(cte.find_all(exp.Window))
            assert isinstance(window.this, exp.RowNumber)
            assert [column.name for column in window.args["partition_by"]] == [partition]
            ordered = window.args["order"].expressions[0]
            assert ordered.this.name == ordered_column and ordered.args["desc"] is True

    def test_executable_two_stage_latest_selection(self):
        """DuckDB checks two-stage latest behavior; AST binds production stages."""
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        rows = con.execute("""
            WITH schedule(event_id, venue_id, venue, ingested_at, city, country) AS (
                VALUES
                  (1, 7, ' Old Ground ', TIMESTAMP '2026-08-01 00:00:00', 'Old', 'AA'),
                  (1, 7, ' New Ground ', TIMESTAMP '2026-08-02 00:00:00', 'New', 'AA'),
                  (2, 7, ' Newest Ground ', TIMESTAMP '2026-08-03 00:00:00', 'Newest', 'BB'),
                  (3, NULL, ' Unknown ', TIMESTAMP '2026-08-04 00:00:00', 'None', 'CC')
            ), schedule_dedup AS (
                SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS event_rn FROM schedule
            ), venue_dedup AS (
                SELECT *, row_number() OVER (PARTITION BY venue_id ORDER BY ingested_at DESC) AS venue_rn
                FROM schedule_dedup WHERE event_rn = 1 AND venue_id IS NOT NULL
            )
            SELECT venue_id, trim(venue), city, country FROM venue_dedup WHERE venue_rn = 1
        """).fetchall()
        assert rows == [(7, "Newest Ground", "Newest", "BB")]

    def test_header_lineage_and_trailing_partitions(self):
        sql = _sql()
        assert "-- Sources (native v2):" in sql
        assert "-- Notes:" in sql and "-- Footguns:" in sql and "-- DAG integration:" in sql
        final = sql.rsplit("SELECT", 1)[-1]
        assert re.search(r"_ingested_at\s+AS\s+_bronze_ingested_at", final, re.I)
        assert re.search(r"competition_slug\s+AS\s+league", final, re.I)
        assert re.search(r"CAST\s*\(\s*source_season_year\s+AS\s+varchar\s*\)\s+AS\s+season", final, re.I)
