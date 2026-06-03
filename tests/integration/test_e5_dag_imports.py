"""
Smoke integration tests for E5 (Player Availability) DAG wiring.

Goal: verify the Silver + Gold DAGs *parse* and contain the new E5 task ids.
Anything beyond presence (dependency wiring, task config) is out of scope —
covered by ``test_dag_structure.py`` and ``test_dag_parsing.py``.

This module defines its own ``dag_bag`` fixture; it requires
``apache-airflow`` on PYTHONPATH and will skip otherwise.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Re-use the project-level conftest fixtures
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DAGS_FOLDER = PROJECT_ROOT / "dags"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DAGS_FOLDER))


@pytest.fixture(scope="module")
def dag_bag():
    """Local DagBag fixture so this test module is self-sufficient."""
    os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT / "airflow_home"))
    os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_FOLDER))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///airflow.db"
    )
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("Airflow not installed")
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


@pytest.mark.integration
class TestE5DagImports:
    """Verify E5 task ids land in the right DAGs."""

    def test_silver_dag_loads_with_e5_task(self, dag_bag):
        """``dag_transform_fbref_silver`` must contain ``whoscored_player_unavailable``.

        NB: tasks live inside a ``silver_transforms`` TaskGroup so the fully
        qualified id is ``silver_transforms.whoscored_player_unavailable``. We
        check on the *suffix* to stay tolerant of task-group renames.
        """
        if dag_bag.import_errors:
            pytest.fail(
                "DAG import errors: " + "; ".join(
                    f"{k}: {v}" for k, v in dag_bag.import_errors.items()
                )
            )

        dag_id = "dag_transform_fbref_silver"
        assert dag_id in dag_bag.dags, (
            f"DAG '{dag_id}' not found. "
            f"Loaded: {sorted(dag_bag.dags.keys())}"
        )
        dag = dag_bag.dags[dag_id]
        task_ids = {t.task_id for t in dag.tasks}
        # Accept either bare task_id or a TaskGroup-prefixed one
        present = any(
            tid == "whoscored_player_unavailable"
            or tid.endswith(".whoscored_player_unavailable")
            for tid in task_ids
        )
        assert present, (
            f"E5 Silver task 'whoscored_player_unavailable' missing from {dag_id}. "
            f"Tasks: {sorted(task_ids)}"
        )

    def test_gold_dag_loads_with_fct_player_unavailable(self, dag_bag):
        """``dag_transform_fbref_gold`` must contain ``fct_player_unavailable``.

        Tolerates TaskGroup prefix (e.g. ``s3_facts.fct_player_unavailable``).
        """
        if dag_bag.import_errors:
            pytest.fail(
                "DAG import errors: " + "; ".join(
                    f"{k}: {v}" for k, v in dag_bag.import_errors.items()
                )
            )

        dag_id = "dag_transform_fbref_gold"
        assert dag_id in dag_bag.dags, (
            f"DAG '{dag_id}' not found. "
            f"Loaded: {sorted(dag_bag.dags.keys())}"
        )
        dag = dag_bag.dags[dag_id]
        task_ids = {t.task_id for t in dag.tasks}
        present = any(
            tid == "fct_player_unavailable"
            or tid.endswith(".fct_player_unavailable")
            for tid in task_ids
        )
        assert present, (
            f"E5 Gold task 'fct_player_unavailable' missing from {dag_id}. "
            f"Tasks: {sorted(task_ids)}"
        )
