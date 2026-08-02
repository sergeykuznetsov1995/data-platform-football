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
    "dag_trigger_espn_daily.py",
)
ESPN_DAG_IDS = {path.removesuffix(".py") for path in ESPN_DAG_FILES}
MASTER_DAG_FILE = "dag_master_pipeline.py"


def _copy_isolated_projection(dag_folder: Path) -> None:
    """Project reviewed ESPN DAGs while proving the shared master is ignored."""

    for filename in (*ESPN_DAG_FILES, MASTER_DAG_FILE):
        shutil.copy2(ROOT / "dags" / filename, dag_folder / filename)
    shutil.copy2(
        ROOT / "configs" / "espn" / "isolated.airflowignore",
        dag_folder / ".airflowignore",
    )


@pytest.mark.integration
def test_real_airflow_safe_mode_loads_every_espn_dag_with_return_value_maps(
    tmp_path, monkeypatch
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
    _copy_isolated_projection(dag_folder)
    assert (dag_folder / MASTER_DAG_FILE).is_file()

    monkeypatch.setenv("ESPN_ISOLATED_STACK", "1")

    bag = DagBag(
        dag_folder=str(dag_folder),
        include_examples=False,
        safe_mode=True,
    )

    assert bag.import_errors == {}
    assert set(bag.dags) == ESPN_DAG_IDS
    assert len(bag.dags) == 7
    assert "dag_master_pipeline" not in bag.dags
    for dag_id in (
        "dag_ingest_espn",
        "dag_repair_espn",
        "dag_backfill_espn",
        "dag_replay_espn",
    ):
        assert bag.dags[dag_id].dagrun_timeout.total_seconds() == 11 * 60 * 60
        verdict = bag.dags[dag_id].get_task("terminal_verdict")
        assert verdict.upstream_task_ids == set(verdict.op_kwargs["producer_task_ids"])
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


@pytest.mark.integration
def test_real_airflow_projection_omits_owner_without_exact_role(tmp_path, monkeypatch):
    try:
        import airflow
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("real Airflow is not installed in the host unit environment")
    if not isinstance(getattr(airflow, "__version__", None), str):
        pytest.skip("unit-test Airflow stubs cannot parse mapped DAGs")

    dag_folder = tmp_path / "espn-dags"
    dag_folder.mkdir()
    _copy_isolated_projection(dag_folder)
    assert (dag_folder / MASTER_DAG_FILE).is_file()
    monkeypatch.delenv("ESPN_ISOLATED_STACK", raising=False)

    bag = DagBag(dag_folder=str(dag_folder), include_examples=False, safe_mode=True)

    assert bag.import_errors == {}
    assert set(bag.dags) == ESPN_DAG_IDS - {"dag_trigger_espn_daily"}
    assert "dag_master_pipeline" not in bag.dags
