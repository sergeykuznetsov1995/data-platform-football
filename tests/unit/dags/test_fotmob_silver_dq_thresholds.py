"""DQ range thresholds for the FotMob Silver DAG.

A threshold that trails the competition format turns every report into noise
and hides the warnings that matter (issue #1045).
"""

import ast
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.unit

# The European league phase has been one Swiss-model table of 36 clubs since
# 2024/25, so 31-36 are real placings, not parse errors.
EUROPEAN_LEAGUE_PHASE_CLUBS = 36


def _dag_source_and_tree():
    source = (ROOT / "dags" / "dag_transform_fotmob_silver.py").read_text(
        encoding="utf-8"
    )
    return source, ast.parse(source)


def _named_check(name: str):
    source, tree = _dag_source_and_tree()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        keywords = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        name_node = keywords.get("name")
        if isinstance(name_node, ast.Constant) and name_node.value == name:
            return source, tree, node, keywords
    raise AssertionError(f"no DQ check named {name!r}")


def _constant(tree: ast.AST, name: str):
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(
            isinstance(target, ast.Name) and target.id == name
            for target in targets
        ):
            continue
        value = node.value
        if isinstance(value, ast.Constant):
            return value.value
    raise AssertionError(f"no literal assignment for {name}")


def _value_range_bounds(table: str, column: str):
    path = ROOT / "dags" / "dag_transform_fotmob_silver.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "value_range"):
            continue
        args = [a.value for a in node.args if isinstance(a, ast.Constant)]
        if args[:2] != [table, column]:
            continue
        kwargs = {
            kw.arg: kw.value.value
            for kw in node.keywords
            if kw.arg and isinstance(kw.value, ast.Constant)
        }
        return kwargs.get("min_val"), kwargs.get("max_val")
    raise AssertionError(f"no value_range check for {table}.{column}")


@pytest.mark.parametrize("table,column", [
    ("silver.fotmob_team_profile", "table_position"),
    ("silver.fotmob_team_standings", "position"),
])
def test_table_position_range_covers_the_36_club_league_phase(table, column):
    min_val, max_val = _value_range_bounds(table, column)

    assert min_val == 1
    assert max_val == EUROPEAN_LEAGUE_PHASE_CLUBS


def test_once_player_policy_has_missing_card_gate():
    _, _, call, keywords = _named_check("fotmob_squad_players_without_card")

    assert isinstance(call.func, ast.Attribute)
    assert call.func.attr == "ref_integrity"
    assert {
        key: value.value
        for key, value in keywords.items()
        if key in {
            "child",
            "parent",
            "key",
            "parent_key",
            "where",
            "warn_rate",
            "error_rate",
        }
        and isinstance(value, ast.Constant)
    } == {
        "child": "bronze.fotmob_squad_snapshots_current",
        "parent": "bronze.fotmob_player_snapshots_current",
        "key": "member_id",
        "parent_key": "player_id",
        "where": "member_type = 'player'",
        "warn_rate": 0.01,
        "error_rate": 0.03,
    }


def test_once_player_policy_has_non_blocking_age_review_gate():
    source, tree, call, keywords = _named_check(
        "fotmob_player_card_review_horizon"
    )

    assert isinstance(call.func, ast.Attribute)
    assert call.func.attr == "coverage"
    assert isinstance(call.args[0], ast.Constant)
    assert call.args[0].value == "silver.fotmob_player_profile"
    assert "card_observed_at" in ast.unparse(keywords["condition"])
    assert keywords["where"].value == "is_current_season"
    assert keywords["warn_threshold"].value == 0.95
    assert keywords["error_threshold"].value == 0.0
    assert keywords["severity"].value == "WARNING"
    assert _constant(tree, "PLAYER_CARD_REVIEW_HORIZON_DAYS") == 365
    assert "PLAYER_CARD_FRESH_HOURS" not in source
