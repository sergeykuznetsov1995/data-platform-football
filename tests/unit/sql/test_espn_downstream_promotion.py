"""One explicit ESPN allowlist across every downstream promotion path."""

from __future__ import annotations

import inspect
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
DAGS = ROOT / "dags"
if str(DAGS) not in sys.path:
    sys.path.insert(0, str(DAGS))

VALUES_MARKER = "__ESPN_DOWNSTREAM_SCOPE_VALUES__"
FILTER_MARKER = "__ESPN_DOWNSTREAM_SCOPE_FILTER__"

SQL_CONSUMERS = (
    ROOT / "dags/sql/silver/xref_team.sql.j2",
    ROOT / "dags/sql/silver/xref_match.sql",
    ROOT / "dags/sql/silver/espn_lineup.sql",
    ROOT / "dags/sql/silver/espn_matchsheet.sql",
    ROOT / "dags/sql/gold/fct_lineup.sql",
)


@pytest.mark.unit
@pytest.mark.parametrize("path", SQL_CONSUMERS, ids=lambda path: path.name)
def test_every_sql_consumer_uses_current_view_and_common_filter(path: Path) -> None:
    sql = path.read_text(encoding="utf-8")

    assert VALUES_MARKER in sql
    assert FILTER_MARKER in sql
    assert "espn_scope.platform_league" in sql
    assert "espn_scope.platform_season_slug" in sql
    assert "iceberg.bronze.espn_" in sql
    assert "_current" in sql


@pytest.mark.unit
def test_player_resolver_uses_the_same_rendered_filter() -> None:
    from utils import xref_player_resolver

    source = inspect.getsource(xref_player_resolver._fetch_espn_players)

    assert "iceberg.bronze.espn_lineup_current" in source
    assert VALUES_MARKER in source
    assert FILTER_MARKER in source
    assert "render_espn_downstream_sql" in source
    assert "espn_scope.platform_league" in source
    assert "espn_scope.platform_season_slug" in source


@pytest.mark.unit
@pytest.mark.parametrize(
    "path",
    (
        ROOT / "dags/sql/silver/espn_lineup.sql",
        ROOT / "dags/sql/silver/espn_matchsheet.sql",
    ),
    ids=lambda path: path.name,
)
def test_silver_outputs_preserve_native_source_identity(path: Path) -> None:
    sql = path.read_text(encoding="utf-8")

    assert "es_source.scope_id" in sql
    assert "AS source_scope_id" in sql
    assert "es_source.source_season_year" in sql
    assert "AS source_season_year" in sql


@pytest.mark.unit
def test_all_transform_runner_modes_use_one_rendered_select_loader() -> None:
    from utils import gold_tasks, silver_tasks

    for function in (
        silver_tasks.run_silver_transform,
        silver_tasks.run_silver_transform_partition_staged,
        silver_tasks.run_silver_partition_insert,
        gold_tasks.run_gold_partition_insert_wrapped,
    ):
        assert "_load_transform_select_sql" in inspect.getsource(function), function


@pytest.mark.unit
def test_e3_backfill_precheck_routes_espn_current_view_through_mapping() -> None:
    source = (ROOT / "dags" / "dag_e3_backfill.py").read_text(encoding="utf-8")

    assert "iceberg.bronze.espn_lineup_current" in source
    assert "render_espn_downstream_sql" in source
    assert VALUES_MARKER in source
    assert FILTER_MARKER in source
    assert "espn_scope.platform_season_slug = '{season_sql}'" in source
    assert "espn_scope.platform_league = '{league_sql}'" in source


@pytest.mark.unit
def test_common_filter_maps_native_and_legacy_but_excludes_wrong_rows() -> None:
    duckdb = pytest.importorskip("duckdb")
    from utils.espn_season_mapping import render_espn_downstream_sql

    template = """
WITH espn_downstream_scope (
    scope_id, espn_id, source_season_year, platform_league,
    platform_season_slug, convention, effective_start_date, effective_end_date
) AS (VALUES
__ESPN_DOWNSTREAM_SCOPE_VALUES__
)
SELECT
    es_source.label,
    espn_scope.platform_league AS league,
    espn_scope.platform_season_slug AS season,
    es_source.scope_id AS source_scope_id,
    espn_scope.source_season_year
FROM espn_source es_source
JOIN espn_downstream_scope espn_scope ON
__ESPN_DOWNSTREAM_SCOPE_FILTER__
ORDER BY es_source.label
"""
    sql = render_espn_downstream_sql(template)
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE espn_source (
            label VARCHAR,
            scope_id VARCHAR,
            competition_id BIGINT,
            source_season_year BIGINT,
            league VARCHAR,
            season VARCHAR,
            game VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO espn_source VALUES
          ('legacy-ok', NULL, NULL, NULL,
           'ENG-Premier League', '2627', '2026-08-20 Liverpool-Arsenal'),
          ('native-ok', '700:2026', 700, 2026,
           'raw-source-league', '2026', '2026-08-20 Liverpool-Arsenal'),
          ('wrong-scope', '701:2026', 701, 2026,
           'ENG-Premier League', '2627', '2026-08-20 Liverpool-Arsenal'),
          ('wrong-native-id', '700:2026', 701, 2026,
           'ENG-Premier League', '2627', '2026-08-20 Liverpool-Arsenal'),
          ('missing-map', NULL, NULL, NULL,
           'UEFA-Champions League', '2627', '2026-08-20 A-B'),
          ('outside-date', '700:2026', 700, 2026,
           'ENG-Premier League', '2627', '2026-08-01 A-B'),
          ('bad-date', '700:2026', 700, 2026,
           'ENG-Premier League', '2627', 'not-a-date A-B')
        """
    )

    rows = con.execute(sql).fetchall()

    assert rows == [
        ("legacy-ok", "ENG-Premier League", "2627", None, 2026),
        ("native-ok", "ENG-Premier League", "2627", "700:2026", 2026),
    ]
