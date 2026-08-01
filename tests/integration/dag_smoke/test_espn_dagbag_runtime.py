"""Real-Airflow parser regression for ESPN production DAGs."""

from __future__ import annotations

from pathlib import Path
import shutil

import pytest


ROOT = Path(__file__).resolve().parents[3]
ESPN_DAG_FILES = (
    "dag_ingest_espn.py",
    "dag_repair_espn.py",
    "dag_backfill_espn.py",
    "dag_replay_espn.py",
    "dag_discover_espn_registry.py",
    "dag_monitor_espn.py",
)
ESPN_DAG_IDS = {path.removesuffix(".py") for path in ESPN_DAG_FILES}


@pytest.mark.integration
def test_real_airflow_safe_mode_loads_every_espn_dag_with_return_value_maps(
    tmp_path,
):
    try:
        import airflow
        from airflow.models import DagBag
        from airflow.models.mappedoperator import MappedOperator
        from airflow.models.xcom_arg import XComArg
        from airflow.utils.xcom import XCOM_RETURN_KEY
    except ImportError:
        pytest.skip("real Airflow is not installed in the host unit environment")
    if not isinstance(getattr(airflow, "__version__", None), str):
        pytest.skip("unit-test Airflow stubs cannot parse mapped DAGs")

    dag_folder = tmp_path / "espn-dags"
    dag_folder.mkdir()
    for filename in ESPN_DAG_FILES:
        shutil.copy2(ROOT / "dags" / filename, dag_folder / filename)

    bag = DagBag(
        dag_folder=str(dag_folder),
        include_examples=False,
        safe_mode=True,
    )

    assert bag.import_errors == {}
    assert set(bag.dags) == ESPN_DAG_IDS
    mapped_count = 0
    for dag in bag.dags.values():
        for task in dag.tasks:
            if not isinstance(task, MappedOperator):
                continue
            mapped_count += 1
            for value in task.expand_input.value.values():
                assert isinstance(value, XComArg)
                references = list(value.iter_references())
                assert references
                assert {key for _operator, key in references} == {XCOM_RETURN_KEY}
    assert mapped_count == 23
