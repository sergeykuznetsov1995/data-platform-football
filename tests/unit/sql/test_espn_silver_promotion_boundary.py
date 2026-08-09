"""Shared compact6 promotion boundary for the six native ESPN Silver models."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from sqlglot import exp, parse_one


ROOT = Path(__file__).resolve().parents[3]
SQL_DIR = ROOT / "dags" / "sql" / "silver"
VALUES_MARKER = "__ESPN_DOWNSTREAM_SCOPE_VALUES__"
FILTER_MARKER = "__ESPN_DOWNSTREAM_SCOPE_FILTER__"
EXPECTED_SOURCES = {
    "espn_match.sql": ("espn_schedule", "espn_matchsheet"),
    "espn_team_match.sql": ("espn_schedule", "espn_matchsheet"),
    "espn_player_match_aggregate.sql": ("espn_schedule", "espn_lineup"),
    "espn_match_events.sql": ("espn_schedule",),
    "espn_substitutions.sql": ("espn_schedule", "espn_lineup"),
    "espn_venue.sql": ("espn_schedule",),
}

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("name", EXPECTED_SOURCES)
def test_template_reads_only_canonical_public_views_through_exact_scope_filter(name):
    template = (SQL_DIR / name).read_text(encoding="utf-8")
    executable = "\n".join(
        line for line in template.splitlines() if not line.lstrip().startswith("--")
    )

    assert template.count(VALUES_MARKER) == 1
    assert template.count(FILTER_MARKER) == len(EXPECTED_SOURCES[name])
    assert "_generation_v2" not in executable
    assert "espn_internal" not in executable
    assert "_current" not in executable

    for source in EXPECTED_SOURCES[name]:
        assert executable.count(f"iceberg.bronze.{source} AS es_source") == 1
        assert re.search(
            rf"FROM\s+iceberg\.bronze\.{source}\s+AS\s+es_source\s+"
            rf"JOIN\s+espn_downstream_scope\s+espn_scope\s+ON\s+{FILTER_MARKER}",
            executable,
            re.I | re.S,
        )


@pytest.mark.parametrize("name", EXPECTED_SOURCES)
def test_rendered_model_contains_exact_six_mappings_and_platform_partitions(name):
    from utils.espn_season_mapping import render_espn_downstream_sql

    template = (SQL_DIR / name).read_text(encoding="utf-8")
    rendered = render_espn_downstream_sql(template)
    tree = parse_one(rendered, read="trino")

    assert "__ESPN_DOWNSTREAM" not in rendered
    scope_cte = next(
        cte for cte in tree.find_all(exp.CTE)
        if cte.alias_or_name == "espn_downstream_scope"
    )
    values = next(scope_cte.find_all(exp.Values))
    assert len(values.expressions) == 6

    outputs = {
        expression.alias: expression.this
        for expression in tree.expressions
        if isinstance(expression, exp.Alias)
    }
    assert isinstance(outputs["league"], exp.Column)
    assert outputs["league"].name == "platform_league"
    assert isinstance(outputs["season"], exp.Column)
    assert outputs["season"].name == "platform_season_slug"

    final_select = rendered.rsplit("SELECT", 1)[-1]
    assert not re.search(r"competition_slug\s+AS\s+league", final_select, re.I)
    assert not re.search(r"source_season_year[^,\n]*\s+AS\s+season", final_select, re.I)
