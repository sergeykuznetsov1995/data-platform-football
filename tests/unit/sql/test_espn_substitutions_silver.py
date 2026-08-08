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


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _tree() -> exp.Select:
    return sqlglot.parse_one(_sql(), read="trino")


class TestEspnSubstitutionsSilver:
    def test_native_sources_dedup_before_inbound_filter_and_schedule_played_join(self):
        sql = _sql()
        body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        assert body.count("iceberg.bronze.espn_lineup_generation_v2") == 1
        assert body.count("iceberg.bronze.espn_schedule_generation_v2") == 1
        assert re.search(r"PARTITION BY\s+event_id\s*,\s*team_id\s*,\s*athlete_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert re.search(r"PARTITION BY\s+event_id\s+ORDER BY\s+_ingested_at\s+DESC", body, re.I)
        assert body.index("lineup_dedup") < body.index("subbed_in")
        assert re.search(r"JOIN\s+schedule_dedup\s+s\s+ON\s+s\.event_id\s*=\s+l\.event_id", body, re.I)
        assert re.search(r"WHERE\s+s\.played_final\s+AND\s+l\.subbed_in", body, re.I)

    def test_ast_binds_inbound_pair_and_sibling_jersey_paths(self):
        tree = _tree()
        paths = {
            node.expression.sql(dialect="trino")
            for node in tree.find_all(exp.JSONExtractScalar)
        }
        assert {
            "'$.subbedInFor.athlete.id'", "'$.subbedInFor.athlete.displayName'",
            "'$.jersey'", "'$.subbedInFor.jersey'",
        } <= paths
        assert "'$.subbedInFor.athlete.jersey'" not in paths

        final = sqlglot.parse_one(_sql(), read="trino")
        player_in = next(alias for alias in final.find_all(exp.Alias) if alias.alias == "player_in_id")
        assert isinstance(player_in.this, exp.Column) and player_in.this.name == "athlete_id"

    def test_executable_dedup_inbound_only_pairing_and_json_jersey_priority(self):
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
        assert re.search(r"l\.competition_slug\s+AS\s+league", final, re.I)
        assert re.search(r"CAST\s*\(\s*l\.source_season_year\s+AS\s+varchar\s*\)\s+AS\s+season", final, re.I)
