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
