"""Runtime ownership contracts for ESPN Native Bronze v2."""

from __future__ import annotations

import ast
from pathlib import Path

from dags.utils.config import SCHEDULES


ROOT = Path(__file__).resolve().parents[3]


def test_master_is_the_only_daily_espn_schedule_owner() -> None:
    """The ESPN child is trigger-only; the master owns the daily cadence."""
    assert SCHEDULES["dag_ingest_espn"] is None
    assert SCHEDULES["dag_master_pipeline"] == "0 14 * * *"


def test_native_espn_factory_declares_a_nonempty_description() -> None:
    """All factory-built ESPN DAGs must stay useful in the Airflow UI."""
    source = (ROOT / "dags/utils/espn_dag_factory.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    dag_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "DAG"
    ]

    assert len(dag_calls) == 1
    description = next(
        (
            keyword.value
            for keyword in dag_calls[0].keywords
            if keyword.arg == "description"
        ),
        None,
    )
    assert description is not None
    assert not (isinstance(description, ast.Constant) and not description.value)
