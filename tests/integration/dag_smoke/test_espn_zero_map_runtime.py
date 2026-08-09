"""Real-Airflow regression for ESPN's legitimate empty Summary map."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from types import SimpleNamespace

import pytest


@pytest.mark.integration
def test_real_airflow_zero_summary_map_executes_actual_espn_reducer(
    tmp_path, monkeypatch, request
):
    airflow_home = tmp_path / "airflow"
    metadata_path = tmp_path / "airflow.db"
    monkeypatch.setenv("AIRFLOW_HOME", str(airflow_home))
    monkeypatch.setenv(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{metadata_path}"
    )
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    try:
        import airflow
        from airflow import settings
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        from airflow.utils import db
    except ImportError:
        pytest.skip("real Airflow is not installed in the host unit environment")
    if not isinstance(getattr(airflow, "__version__", None), str):
        pytest.skip("unit-test Airflow stubs cannot execute a mapped DAG")

    from dags.utils import espn_native_tasks as tasks

    metadata_uri = f"sqlite:///{metadata_path}"
    original_metadata_uri = settings.SQL_ALCHEMY_CONN
    settings.dispose_orm()
    settings.SQL_ALCHEMY_CONN = metadata_uri
    settings.configure_orm(disable_connection_pool=True)

    def restore_airflow_orm():
        settings.dispose_orm()
        settings.SQL_ALCHEMY_CONN = original_metadata_uri
        settings.configure_orm()

    request.addfinalizer(restore_airflow_orm)
    db.initdb()
    assert metadata_path.is_file(), "Airflow test metadata must stay in tmp_path"
    scope_id = "700:2024"
    scope_binding_ref = {
        "uri": (tmp_path / "scope-binding.json").as_uri(),
        "sha256": "b" * 64,
    }
    summary_index = {
        "kind": "espn-summary-wave-index-v1",
        "schema_version": 1,
        "expected_map_count": 0,
        "expected_scoreboard_map_count": 0,
        "scopes": [
            {
                "scope_id": scope_id,
                "scope_binding_ref": scope_binding_ref,
                "scoreboard_phase_ref": None,
                "summary_batch_refs": [],
                "budget_used": 0,
                "budget_limit": tasks.MAX_SCOPE_SUMMARY_EVENTS,
                "budget_exhausted": False,
                "pending_event_ids": [],
            }
        ],
    }
    summary_index_body = (
        json.dumps(summary_index, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    summary_index_path = tmp_path / "summary-wave-index.json"
    summary_index_path.write_bytes(summary_index_body)
    summary_index_ref = {
        "uri": summary_index_path.as_uri(),
        "sha256": hashlib.sha256(summary_index_body).hexdigest(),
    }
    descriptor = {
        "scope_root": (tmp_path / "run" / "scopes" / "700-2024").as_uri(),
        "raw_manifest_uri": (tmp_path / "raw-manifest.json").as_uri(),
    }
    loaded = SimpleNamespace(
        bindings={scope_id: SimpleNamespace(active=False, prior=object())}
    )
    scope = SimpleNamespace(scope_id=scope_id)
    monkeypatch.setattr(
        tasks,
        "_heartbeat_scope_binding",
        lambda ref: tasks._binding(ref),
    )
    monkeypatch.setattr(
        tasks,
        "_binding",
        lambda ref: (
            (None, descriptor, loaded, scope, None)
            if ref == scope_binding_ref
            else pytest.fail(f"unexpected binding: {ref!r}")
        ),
    )
    monkeypatch.setattr(tasks.runner, "_manifest_base", lambda *_args: {"base": True})
    monkeypatch.setattr(
        tasks.runner, "_seal_manifest", lambda manifest: {**manifest, "sealed": True}
    )

    def emit_summary_plan():
        return {
            "summary_batch_refs": [],
            "summary_index_ref": summary_index_ref,
        }

    with DAG(
        dag_id="test_espn_zero_map_runtime",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        catchup=False,
    ) as dag_instance:
        summary_plan = PythonOperator(
            task_id="plan_summary_batches",
            python_callable=emit_summary_plan,
            multiple_outputs=True,
        )
        selector = PythonOperator(
            task_id="select_summary_batches",
            python_callable=tasks.select_mapping_descriptors,
            op_kwargs={
                "source": summary_plan.output,
                "source_key": "summary_batch_refs",
                "descriptor_key": "summary_batch_ref",
                "max_items": tasks.MAX_SUMMARY_BATCH_MAP_ITEMS,
            },
        )
        summary_fetch = PythonOperator.partial(
            task_id="fetch_summary_batches",
            python_callable=tasks.fetch_summary_batch,
        ).expand(op_kwargs=selector.output)
        reducer = PythonOperator(
            task_id="reduce_raw_manifests",
            python_callable=tasks.reduce_raw_manifest_wave,
            op_kwargs={
                "summary_index_ref": summary_plan.output["summary_index_ref"],
                "summary_phase_refs": summary_fetch.output,
            },
            trigger_rule="none_failed",
        )
        summary_plan >> selector >> summary_fetch >> reducer

    dag_run = dag_instance.test(
        execution_date=datetime(2026, 8, 1, tzinfo=timezone.utc)
    )
    states = {
        (instance.task_id, instance.map_index): str(
            getattr(instance.state, "value", instance.state)
        ).lower()
        for instance in dag_run.get_task_instances()
    }

    assert any(
        task_id == "fetch_summary_batches" and state == "skipped"
        for (task_id, _), state in states.items()
    )
    assert states[("reduce_raw_manifests", -1)] == "success"
    reducer_instance = dag_run.get_task_instance(task_id="reduce_raw_manifests")
    reduced = reducer_instance.xcom_pull(
        task_ids="reduce_raw_manifests", key="return_value"
    )
    assert len(reduced) == 1
    assert set(reduced[0]) == {"raw_phase_ref"}
