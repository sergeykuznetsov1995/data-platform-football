"""Fail-closed validation wiring for the FBref Silver DAG."""

import ast
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.unit


def test_silver_validation_cannot_mask_failed_transform_with_all_done():
    """A transform failure must skip validation/xref, not validate stale tables."""

    path = ROOT / "dags" / "dag_transform_fbref_silver.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    trigger_rules = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        task = kwargs.get("task_id")
        if not isinstance(task, ast.Constant):
            continue
        if task.value not in {
            "validate_silver",
            "validate_silver_quality",
            "trigger_xref_transform",
        }:
            continue
        rule = kwargs.get("trigger_rule")
        trigger_rules[task.value] = (
            rule.value if isinstance(rule, ast.Constant) else None
        )

    assert set(trigger_rules) == {
        "validate_silver",
        "validate_silver_quality",
        "trigger_xref_transform",
    }
    assert all(rule != "all_done" for rule in trigger_rules.values())


def test_silver_dq_registers_new_strict_fbref_contracts():
    source = (ROOT / "dags" / "dag_transform_fbref_silver.py").read_text(
        encoding="utf-8"
    )
    for check_name in (
        "score_event_mismatch[silver.fbref_match_enriched]",
        "scored_match_without_events[silver.fbref_match_enriched]",
        "awarded_result_override_missing[silver.fbref_match_enriched]",
        "shootout_score_parse[silver.fbref_match_enriched]",
        "restricted_match_events[silver.fbref_match_enriched]",
    ):
        assert check_name in source

    assert "event_availability, 'unknown'" in source
    assert "NOT IN ('restricted', 'not_applicable')" in source


def test_silver_adds_source_identity_columns_before_transforms():
    source = (ROOT / "dags" / "dag_transform_fbref_silver.py").read_text(
        encoding="utf-8"
    )
    assert "task_id='ensure_source_identity_columns'" in source
    assert "ensure_source_identity_columns >> transforms_group" in source
    assert '"source_season_id"' in source


def test_every_identity_resolved_silver_table_waits_for_identity():
    source = (ROOT / "dags" / "dag_transform_fbref_silver.py").read_text(
        encoding="utf-8"
    )
    dependency = source.split(
        "transform_tasks['fbref_player_identity'] >> [", 1
    )[1].split("\n        ]", 1)[0]
    for table in (
        "fbref_player_season_profile",
        "fbref_keeper_profile",
        "fbref_player_match_stats",
        "fbref_match_lineups",
        "fbref_keeper_match_stats",
    ):
        assert f"transform_tasks['{table}']" in dependency
