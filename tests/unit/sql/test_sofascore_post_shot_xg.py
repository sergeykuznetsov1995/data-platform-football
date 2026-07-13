"""Explicit SofaScore-only team-match post-shot xG semantics (#876)."""

from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = ROOT / "dags/sql/gold/fct_sofascore_team_match_post_shot_xg.sql"


@pytest.fixture()
def con():
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE silver_sofascore_shots (
            match_id VARCHAR,
            team_id VARCHAR,
            shot_id VARCHAR,
            is_sot BOOLEAN,
            xgot DOUBLE,
            shot_source VARCHAR,
            league VARCHAR,
            season VARCHAR
        )
        """
    )
    yield connection
    connection.close()


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _run(con, rows):
    con.executemany(
        "INSERT INTO silver_sofascore_shots VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    query = _sql().replace(
        "iceberg.silver.sofascore_shots", "silver_sofascore_shots"
    )
    cursor = con.execute(query)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _row(match, team, shot, is_sot, xgot, source="sofascore_v1"):
    return (match, team, shot, is_sot, xgot, source, "ENG-Premier League", "2526")


def test_team_match_sum_and_opponent_are_explicit(con):
    rows = _run(
        con,
        [
            _row("m1", "home", "h1", True, 0.7),
            _row("m1", "home", "h2", False, None),
            _row("m1", "away", "a1", True, 0.2),
            _row("m1", "away", "a2", True, 0.1),
        ],
    )
    assert len(rows) == 2
    home = next(row for row in rows if row["team_id"] == "home")
    away = next(row for row in rows if row["team_id"] == "away")
    assert home["post_shot_xg"] == pytest.approx(0.7)
    assert home["post_shot_xg_against"] == pytest.approx(0.3)
    assert home["opponent_id"] == "away"
    assert away["post_shot_xg_against"] == pytest.approx(0.7)
    assert {row["metric_source"] for row in rows} == {"sofascore"}
    assert {row["metric_definition"] for row in rows} == {
        "sum_shotmap_xgot_on_target"
    }


def test_zero_on_target_is_real_zero_not_null(con):
    rows = _run(
        con,
        [
            _row("m2", "home", "h1", False, None),
            _row("m2", "away", "a1", True, 0.2),
        ],
    )
    home = next(row for row in rows if row["team_id"] == "home")
    assert home["shots_on_target"] == 0
    assert home["post_shot_xg"] == pytest.approx(0.0)


def test_missing_xgot_on_target_suppresses_whole_match(con):
    rows = _run(
        con,
        [
            _row("m3", "home", "h1", True, None),
            _row("m3", "away", "a1", True, 0.2),
        ],
    )
    # The complete away row is also withheld because it has no publishable
    # opponent row.  This is safer than emitting asymmetric against metrics.
    assert rows == []


def test_non_sofascore_source_and_non_two_team_shape_are_excluded(con):
    rows = _run(
        con,
        [
            _row("m4", "home", "h1", True, 0.2, "other_v1"),
            _row("m4", "away", "a1", True, 0.1, "other_v1"),
            _row("m5", "one", "1", True, 0.1),
            _row("m5", "two", "2", True, 0.2),
            _row("m5", "three", "3", True, 0.3),
        ],
    )
    assert rows == []


def test_sql_has_no_cross_source_coalesce_or_unversioned_metric():
    sql = _sql().lower()
    assert "where shot_source = 'sofascore_v1'" in sql
    assert "metric_source" in sql
    assert "metric_definition" in sql
    assert "metric_version" in sql
    assert "coalesce" not in "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def test_fact_is_registered_in_production_gold_dag():
    dag_source = (ROOT / "dags/dag_transform_e3.py").read_text(encoding="utf-8")
    assert "'fct_sofascore_team_match_post_shot_xg'" in dag_source
    assert "'dags/sql/gold/fct_sofascore_team_match_post_shot_xg.sql'" in dag_source
