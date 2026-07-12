from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _reload():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_canary_sofascore_proxy", None)
    sys.modules.pop("dags.dag_canary_sofascore_proxy", None)
    return importlib.import_module("dag_canary_sofascore_proxy")


def _dag_run(run_id: str, *, producer_state: str):
    return SimpleNamespace(
        run_id=run_id,
        get_task_instance=lambda task_id: SimpleNamespace(
            task_id=task_id,
            state=producer_state,
        ),
    )


def _write_valid_candidate(path: Path, cohort_path: str, cap: int) -> dict:
    benchmark = importlib.import_module("scripts.research.bench_sofascore_paid_canary")
    cohort = benchmark.load_fixed_cohort(cohort_path)
    payload = benchmark._artifact_template(cohort, cap)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    result = benchmark._artifact_summary(payload)
    result.update(
        {
            "status": "collected_unverified",
            "artifact": str(path),
            "experimental_cap_bytes": cap,
            "production_authorized": False,
            "blocked_workload_classes": {
                name: value["collection_blocker"]
                for name, value in payload["workload_classes"].items()
                if value.get("collection_blocker")
            },
        }
    )
    return result


def _collect_valid_candidate(module, tmp_path, monkeypatch, *, run_id="manual__one"):
    repo_cohort = (
        Path(__file__).resolve().parents[3]
        / "configs/sofascore/proxy_canary_cohort.json"
    )
    monkeypatch.setattr(module, "CANARY_RUN_ROOT", tmp_path / "runs")
    monkeypatch.setattr(module, "FIXED_COHORT", str(repo_cohort))
    benchmark = importlib.import_module("scripts.research.bench_sofascore_paid_canary")
    monkeypatch.setattr(
        benchmark,
        "collect_canary",
        lambda *, artifact_path, experimental_cap_bytes, **_kwargs: (
            _write_valid_candidate(
                Path(artifact_path),
                str(repo_cohort),
                experimental_cap_bytes,
            )
        ),
    )
    context = {
        "run_id": run_id,
        "dag_run": _dag_run(run_id, producer_state="success"),
        "params": {"experimental_cap_bytes": 123456, "target_cold_runs": 20},
    }
    producer_manifest = module.collect_fixed_cohort(**context)
    return context, producer_manifest


def test_canary_dag_is_paused_manual_serial_and_has_hard_dq_barrier():
    module = _reload()
    kwargs = module.dag._dag_kwargs
    assert module.dag.dag_id == "dag_canary_sofascore_proxy"
    assert module.dag.schedule is None
    assert kwargs["catchup"] is False
    assert kwargs["is_paused_upon_creation"] is True
    assert kwargs["max_active_runs"] == 1
    assert kwargs["max_active_tasks"] == 1

    from airflow.operators.python import PythonOperator

    tasks = {task.task_id: task for task in PythonOperator._instances}
    assert set(tasks) == {"collect_fixed_cohort", "validate_candidate_manifest"}
    assert tasks["collect_fixed_cohort"].downstream_task_ids == {
        "validate_candidate_manifest"
    }
    assert tasks["validate_candidate_manifest"].upstream_task_ids == {
        "collect_fixed_cohort"
    }
    assert all("verify" not in task_id for task_id in tasks)


def test_canary_paths_are_private_stable_and_run_scoped(tmp_path, monkeypatch):
    module = _reload()
    monkeypatch.setattr(module, "CANARY_RUN_ROOT", tmp_path / "runs")

    first = module._run_paths({"run_id": "manual__one"})
    retry = module._run_paths({"run_id": "manual__one"})
    second = module._run_paths({"run_id": "manual__two"})

    assert first == retry
    assert first[:3] != second[:3]
    assert all(str(path).startswith(str(tmp_path / "runs")) for path in first[:3])
    assert "manual__one" not in str(first[0])
    assert first[0].name == module.CANARY_ARTIFACT_NAME
    assert first[2].name == module.CANARY_PRODUCER_MANIFEST_NAME
    assert module.FIXED_COHORT == (
        "/opt/airflow/configs/sofascore/proxy_canary_cohort.json"
    )


def test_canary_dag_fails_closed_without_explicit_cap():
    module = _reload()
    with pytest.raises(Exception, match="experimental_cap_bytes"):
        module.collect_fixed_cohort(
            run_id="manual__one",
            params={"experimental_cap_bytes": 0, "target_cold_runs": 20},
        )


def test_canary_dag_passes_only_current_run_paths_and_explicit_policy(
    tmp_path, monkeypatch
):
    module = _reload()
    monkeypatch.setattr(module, "CANARY_RUN_ROOT", tmp_path / "runs")
    captured = {}

    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {
            "status": "collected_unverified",
            "verified": False,
            "production_authorized": False,
            "blocked_workload_classes": {},
            "artifact": str(kwargs["artifact_path"]),
        }

    benchmark = importlib.import_module("scripts.research.bench_sofascore_paid_canary")
    monkeypatch.setattr(benchmark, "collect_canary", fake_collect)
    monkeypatch.setattr(
        module,
        "_validate_candidate_artifact",
        lambda _path: ("a" * 64, "b" * 64, {}),
    )
    result = module.collect_fixed_cohort(
        run_id="manual__one",
        params={"experimental_cap_bytes": 123456, "target_cold_runs": 21},
    )
    artifact, workspace, manifest, run_id_hash = module._run_paths(
        {"run_id": "manual__one"}
    )
    assert captured == {
        "artifact_path": artifact,
        "experimental_cap_bytes": 123456,
        "target_cold_runs": 21,
        "cohort_path": module.FIXED_COHORT,
        "workspace": workspace,
    }
    assert result["dag_run_id_sha256"] == run_id_hash
    assert result["artifact_sha256"] == "a" * 64
    assert json.loads(manifest.read_text()) == result


def test_manifest_dq_accepts_only_successful_current_run_candidate(
    tmp_path, monkeypatch
):
    module = _reload()
    context, producer_manifest = _collect_valid_candidate(
        module, tmp_path, monkeypatch
    )

    result = module.validate_candidate_manifest(**context)

    assert result["status"] == "success"
    assert result["artifact"] == producer_manifest["artifact_path"]
    assert result["artifact_sha256"] == producer_manifest["artifact_sha256"]
    assert result["runtime_fingerprint_digest"] == producer_manifest[
        "runtime_fingerprint_digest"
    ]
    assert result["blocked_workload_classes"] == producer_manifest[
        "blocked_workload_classes"
    ]
    assert result["production_authorized"] is False


def test_manifest_dq_rejects_failed_producer_before_reading_files(
    tmp_path, monkeypatch
):
    module = _reload()
    monkeypatch.setattr(module, "CANARY_RUN_ROOT", tmp_path / "runs")
    context = {
        "run_id": "manual__failed",
        "dag_run": _dag_run("manual__failed", producer_state="failed"),
    }
    monkeypatch.setattr(
        module,
        "_validate_candidate_artifact",
        lambda _path: pytest.fail("failed producer artifact must not be read"),
    )

    with pytest.raises(Exception, match="producer did not succeed"):
        module.validate_candidate_manifest(**context)


def test_manifest_dq_rejects_artifact_changed_after_producer(
    tmp_path, monkeypatch
):
    module = _reload()
    context, producer_manifest = _collect_valid_candidate(
        module, tmp_path, monkeypatch
    )
    artifact = Path(producer_manifest["artifact_path"])
    artifact.write_bytes(artifact.read_bytes() + b"\n")

    with pytest.raises(Exception, match="changed after production"):
        module.validate_candidate_manifest(**context)


def test_manifest_dq_rejects_marker_from_another_run(tmp_path, monkeypatch):
    module = _reload()
    context, _producer_manifest = _collect_valid_candidate(
        module, tmp_path, monkeypatch
    )
    _artifact, _workspace, manifest, _run_hash = module._run_paths(context)
    payload = json.loads(manifest.read_text())
    payload["dag_run_id_sha256"] = "0" * 64
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(Exception, match="stale lineage"):
        module.validate_candidate_manifest(**context)
