"""
Smoke integration test for E7 (BI marts) DAG wiring inside
``dag_transform_fbref_gold`` (T3).

Verifies:
* the Gold DAG still loads after T3 added the s7_dashboard_marts TaskGroup,
* the 3 mart task_ids are present (mart_scouting_radar, mart_referee_dashboard,
  mart_event_heatmap), namespaced under s7_dashboard_marts.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_dags_folder() -> Path:
    env_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if env_folder and Path(env_folder).is_dir():
        return Path(env_folder)
    candidate = PROJECT_ROOT / "dags"
    if candidate.is_dir():
        return candidate
    return Path("/opt/airflow/dags")


# sys.path setup (project root + dags folder) is centralised in the root conftest.py.
DAGS_FOLDER = _resolve_dags_folder()


@pytest.fixture(scope="module")
def dag_bag():
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
class TestE7GoldDagMarts:
    DAG_ID = "dag_transform_fbref_gold"
    EXPECTED_MART_TASKS = (
        "mart_scouting_radar",
        "mart_referee_dashboard",
        "mart_event_heatmap",
    )

    def test_dag_transform_fbref_gold_imports_with_marts(self, dag_bag):
        relevant = {
            k: v for k, v in dag_bag.import_errors.items()
            if "transform_fbref_gold" in k
        }
        if relevant:
            pytest.fail(
                "Gold DAG import errors: "
                + "; ".join(f"{k}: {v}" for k, v in relevant.items())
            )
        dag = dag_bag.dags.get(self.DAG_ID)
        assert dag is not None, (
            f"DAG '{self.DAG_ID}' not found. "
            f"Loaded: {sorted(dag_bag.dags.keys())}"
        )
        task_ids = {t.task_id for t in dag.tasks}
        for mart in self.EXPECTED_MART_TASKS:
            present = (
                f"s7_dashboard_marts.{mart}" in task_ids
                or any(tid.endswith(f".{mart}") or tid == mart for tid in task_ids)
            )
            assert present, (
                f"mart task '{mart}' missing from {self.DAG_ID}. "
                f"s7_dashboard_marts children: "
                f"{sorted(t for t in task_ids if t.startswith('s7_dashboard_marts.'))}"
            )
