"""Run only against a copied runtime, on the exact consumer image, without network."""
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

assert sys.dont_write_bytecode
assert sys._whoscored_runtime_startup_root == "/opt/airflow"
sys._load_whoscored_runtime_contract("/opt/airflow")
root = Path("/opt/airflow")
plan = json.loads((root / "retirement-probe.json").read_text())
for name, expected in plan["hashes"].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name

from airflow.models import DagBag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

graphs = {}
source_paths = {"dag_ingest_fbref": "dags/utils/fbref_current_dag_factory.py",
                "dag_bootstrap_fbref": "dags/utils/fbref_current_dag_factory.py",
                "dag_backfill_fbref": "dags/dag_backfill_fbref.py",
                "dag_replay_fbref": "dags/dag_replay_fbref.py"}
for filename, expected_dags in plan["dag_files"].items():
    bag = DagBag(dag_folder=str(root / filename), include_examples=False, safe_mode=False)
    assert not bag.import_errors, bag.import_errors
    assert set(expected_dags) <= set(bag.dags), (filename, expected_dags, sorted(bag.dags))
    for dag_id, dag in bag.dags.items():
        if dag_id not in source_paths:
            continue
        changed = source_paths[dag_id] in plan["changed"]
        if changed or dag_id == "dag_bootstrap_fbref":
            assert not any(isinstance(t, TriggerDagRunOperator) for t in dag.tasks), dag_id
            finalizer = dag.get_task("release_publication_lock")
            assert type(finalizer) is PythonOperator, dag_id
            assert finalizer.python_callable.__name__ == "finalize_fbref_publication_lock"
            assert finalizer.trigger_rule == "all_done" and finalizer.retries == 0
            if dag_id != "dag_bootstrap_fbref":
                assert "export_publication_scope" in finalizer.upstream_task_ids
        else:
            assert "trigger_silver_transform" in dag.task_ids
        graphs[dag_id] = {
            "schedule": str(dag.schedule_interval), "params": dict(dag.params),
            "max_active_runs": dag.max_active_runs,
            "tasks": {t.task_id: {
                "type": type(t).__name__, "upstream": sorted(t.upstream_task_ids),
                "downstream": sorted(t.downstream_task_ids), "rule": t.trigger_rule,
                "retries": t.retries, "timeout": str(t.execution_timeout), "pool": t.pool,
                "callable": getattr(getattr(t, "python_callable", None), "__name__", None),
                "kwargs": getattr(t, "op_kwargs", None),
            } for t in dag.tasks},
        }

# Execute the actual untouched live finalizer with in-memory control evidence.
# Source requests, metabase and publication store are never contacted.
from utils import fbref_pipeline_tasks as tasks
from airflow.exceptions import AirflowException
calls = []
tasks.release_fbref_publication_lock = lambda **kw: (calls.append(kw), {"released": True})[1]
tasks._control_store = lambda: SimpleNamespace(get_run=lambda run_id: {
    "metadata": {"execution_mode": "history_nonpublishing", "publication_eligible": False}})


def check(states, *, dag_id="dag_ingest_fbref", bootstrap=False, fails=False, releases=1):
    calls.clear()
    context = SimpleNamespace(get_task_instances=lambda: [SimpleNamespace(task_id=k, state=v) for k, v in states.items()])
    try:
        result = tasks.finalize_fbref_publication_lock(
            airflow_run_id="offline_probe", dag_id=dag_id, bootstrap_only=bootstrap,
            dag_run=context, task=SimpleNamespace(upstream_task_ids={"export_publication_scope"}))
    except AirflowException:
        assert fails, states
    else:
        assert not fails, states
        assert isinstance(result, dict)
    assert len(calls) == releases, (states, calls)


check({"acquire_publication_lock": "success", "export_publication_scope": "success"})
check({"acquire_publication_lock": "success", "export_publication_scope": "failed"}, fails=True)
check({"acquire_publication_lock": "success", "export_publication_scope": "upstream_failed"}, fails=True)
check({"acquire_publication_lock": "failed"}, fails=True, releases=0)
check({"acquire_publication_lock": "skipped", "plan_backfill": "success"}, releases=0)
check({"acquire_publication_lock": "success", "validate_canary_run": "success", "release_canary_publication_lock": "success"}, releases=0)
check({"acquire_publication_lock": "success", "validate_canary_run": "failed"}, fails=True)
check({"acquire_publication_lock": "success", "choose_publication_path": "success", "export_publication_scope": "skipped", "validate_run": "success"}, dag_id="dag_backfill_fbref")
check({"acquire_publication_lock": "success", "choose_publication_path": "success", "export_publication_scope": "skipped", "validate_run": "failed"}, dag_id="dag_backfill_fbref", fails=True)
check({name: "success" for name in tasks.FBREF_BOOTSTRAP_REQUIRED_TASK_IDS}, dag_id=tasks.FBREF_BOOTSTRAP_DAG_ID, bootstrap=True)
check({name: "failed" for name in tasks.FBREF_BOOTSTRAP_REQUIRED_TASK_IDS}, dag_id=tasks.FBREF_BOOTSTRAP_DAG_ID, bootstrap=True, fails=True)
check({"acquire_publication_lock": "success", "export_publication_scope": "success"}, dag_id="dag_replay_fbref")
print(json.dumps({"startup_contract": "passed", "phase": plan["phase"], "active_dag_files": len(plan["dag_files"]),
                  "behavior_cases": 12, "graphs": graphs}, default=str), flush=True)
