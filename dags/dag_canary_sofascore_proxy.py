"""Manual-only fixed-cohort SofaScore provider-byte canary.

This DAG has no schedule, no upstream trigger and starts paused.  It collects
an unverified candidate under the writable Airflow logs volume; it never edits
the read-only checked-in production artifact and never invokes the atomic
``verify`` promotion command.  Operators must provide the exact experimental
proxy-filter cap used for ``source=sofascore_canary``.

The task is resumable at artifact granularity: every accepted cold run is
atomically appended before the next fresh raw/manifest/browser/lease run.  A
retry therefore collects only the remaining runs needed to reach the requested
target (minimum 20).  Every DagRun owns a private, deterministic directory and
an ``all_done`` validator checks both the producer state and an atomic manifest;
an old or partially written candidate can never make a failed run look healthy.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


CANARY_RUN_ROOT = Path("/opt/airflow/logs/sofascore-canary/runs")
CANARY_ARTIFACT_NAME = "proxy_budget_canary.json"
CANARY_WORKSPACE_NAME = "work"
CANARY_PRODUCER_MANIFEST_NAME = "producer_manifest.json"
FIXED_COHORT = "/opt/airflow/configs/sofascore/proxy_canary_cohort.json"
PRODUCER_TASK_ID = "collect_fixed_cohort"
MANIFEST_SCHEMA_VERSION = 1


def _context_run_id(context: Mapping[str, Any]) -> str:
    """Return Airflow's immutable DagRun id or fail closed."""

    value = context.get("run_id")
    if value is None:
        value = getattr(context.get("ti"), "run_id", None)
    if value is None:
        value = getattr(context.get("dag_run"), "run_id", None)
    run_id = str(value).strip() if value is not None else ""
    if not run_id:
        raise AirflowException("SofaScore canary requires an Airflow DagRun id")
    return run_id


def _run_paths(context: Mapping[str, Any]) -> tuple[Path, Path, Path, str]:
    """Return private artifact/workspace/manifest paths for this DagRun."""

    run_id_hash = hashlib.sha256(_context_run_id(context).encode("utf-8")).hexdigest()
    run_root = CANARY_RUN_ROOT / run_id_hash
    return (
        run_root / CANARY_ARTIFACT_NAME,
        run_root / CANARY_WORKSPACE_NAME,
        run_root / CANARY_PRODUCER_MANIFEST_NAME,
        run_id_hash,
    )


def _write_manifest_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Durably publish the producer marker only after artifact validation."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}-{uuid4().hex}")
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _validate_candidate_artifact(path: Path) -> tuple[str, str, dict[str, str]]:
    """Validate the candidate and return hashes plus explicit class blockers."""

    from scripts.research.bench_sofascore_paid_canary import (
        load_fixed_cohort,
        validate_artifact,
    )

    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AirflowException("SofaScore canary artifact is unreadable") from exc
    if not isinstance(payload, dict):
        raise AirflowException("SofaScore canary artifact must be a JSON object")
    try:
        validate_artifact(
            payload,
            cohort=load_fixed_cohort(FIXED_COHORT),
            require_verifiable=False,
        )
    except Exception as exc:
        raise AirflowException(f"SofaScore canary artifact DQ failed: {exc}") from None
    if payload.get("verified") is not False:
        raise AirflowException("Canary collection must leave verified=false")
    fingerprint = payload.get("runtime_fingerprint")
    digest = fingerprint.get("digest") if isinstance(fingerprint, dict) else None
    if not isinstance(digest, str) or len(digest) != 64:
        raise AirflowException("Canary artifact has no runtime fingerprint digest")
    classes = payload.get("workload_classes")
    assert isinstance(classes, dict)  # Guaranteed by validate_artifact above.
    blockers = {
        str(name): str(value.get("collection_blocker") or "").strip()
        for name, value in classes.items()
        if isinstance(value, dict)
        and str(value.get("collection_blocker") or "").strip()
    }
    return hashlib.sha256(raw).hexdigest(), digest, blockers


def _require_successful_producer(context: Mapping[str, Any]) -> None:
    """Reject a marker from a failed/missing producer task attempt."""

    dag_run = context.get("dag_run")
    getter = getattr(dag_run, "get_task_instance", None)
    if getter is None:
        raise AirflowException("Canary manifest DQ cannot read producer state")
    try:
        task_instance = getter(PRODUCER_TASK_ID)
    except Exception:
        task_instance = None
    state = getattr(task_instance, "state", None)
    if str(state).lower() != "success":
        raise AirflowException(
            f"Required canary producer did not succeed: {PRODUCER_TASK_ID}="
            f"{state or 'missing'}"
        )


def collect_fixed_cohort(**context):
    """Run only the unverified experimental collector."""

    params = context.get("params") or {}
    cap = params.get("experimental_cap_bytes", 0)
    target = params.get("target_cold_runs", 20)
    if isinstance(cap, bool) or not isinstance(cap, int) or cap <= 0:
        raise AirflowException(
            "experimental_cap_bytes must be explicitly set to the positive "
            "cap configured on proxy-filter"
        )
    if isinstance(target, bool) or not isinstance(target, int) or target < 20:
        raise AirflowException("target_cold_runs must be an integer >= 20")

    artifact, workspace, manifest, run_id_hash = _run_paths(context)
    # A retry may resume the current run's candidate, but it must publish a new
    # marker after it succeeds.  Never leave the previous attempt's marker.
    try:
        manifest.unlink()
    except FileNotFoundError:
        pass

    from scripts.research.bench_sofascore_paid_canary import collect_canary

    try:
        result = collect_canary(
            artifact_path=artifact,
            experimental_cap_bytes=cap,
            target_cold_runs=target,
            cohort_path=FIXED_COHORT,
            workspace=workspace,
        )
    except Exception as exc:
        # The benchmark CLI already keeps tokens and raw exits out of its
        # diagnostics.  Airflow receives only the bounded exception message.
        raise AirflowException(f"SofaScore proxy canary failed: {exc}") from None
    if (
        not isinstance(result, dict)
        or result.get("status") != "collected_unverified"
        or result.get("verified") is not False
        or result.get("production_authorized") is not False
        or result.get("artifact") != str(artifact)
    ):
        raise AirflowException("SofaScore canary producer returned an invalid result")
    artifact_sha256, runtime_digest, blockers = _validate_candidate_artifact(artifact)
    if result.get("blocked_workload_classes") != blockers:
        raise AirflowException("SofaScore canary producer hid a collection blocker")
    producer_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "source": "sofascore_canary",
        "status": "collected_unverified",
        "producer_task_id": PRODUCER_TASK_ID,
        "dag_run_id_sha256": run_id_hash,
        "artifact_path": str(artifact),
        "artifact_sha256": artifact_sha256,
        "runtime_fingerprint_digest": runtime_digest,
        "blocked_workload_classes": blockers,
        "verified": False,
        "production_authorized": False,
    }
    _write_manifest_atomic(manifest, producer_manifest)
    return producer_manifest


def validate_candidate_manifest(**context):
    """Hard DQ barrier for current-run producer state and artifact lineage."""

    _require_successful_producer(context)
    artifact, _workspace, manifest, run_id_hash = _run_paths(context)
    try:
        producer_manifest = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AirflowException("Current-run canary producer manifest is unreadable") from exc
    expected_keys = {
        "schema_version",
        "source",
        "status",
        "producer_task_id",
        "dag_run_id_sha256",
        "artifact_path",
        "artifact_sha256",
        "runtime_fingerprint_digest",
        "blocked_workload_classes",
        "verified",
        "production_authorized",
    }
    if not isinstance(producer_manifest, dict) or set(producer_manifest) != expected_keys:
        raise AirflowException("Current-run canary producer manifest is malformed")
    fixed_contract = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "source": "sofascore_canary",
        "status": "collected_unverified",
        "producer_task_id": PRODUCER_TASK_ID,
        "dag_run_id_sha256": run_id_hash,
        "artifact_path": str(artifact),
        "verified": False,
        "production_authorized": False,
    }
    if any(producer_manifest.get(key) != value for key, value in fixed_contract.items()):
        raise AirflowException("Current-run canary producer manifest has stale lineage")
    artifact_sha256, runtime_digest, blockers = _validate_candidate_artifact(artifact)
    if (
        producer_manifest.get("artifact_sha256") != artifact_sha256
        or producer_manifest.get("runtime_fingerprint_digest") != runtime_digest
        or producer_manifest.get("blocked_workload_classes") != blockers
    ):
        raise AirflowException("Current-run canary artifact changed after production")
    return {
        "status": "success",
        "artifact": str(artifact),
        "artifact_sha256": artifact_sha256,
        "runtime_fingerprint_digest": runtime_digest,
        "blocked_workload_classes": blockers,
        "production_authorized": False,
    }


with DAG(
    dag_id="dag_canary_sofascore_proxy",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    },
    description=(
        "Manual fixed 25-match/50-player provider-metered SofaScore canary; "
        "collection never authorizes production"
    ),
    schedule=None,
    start_date=datetime(2026, 7, 11),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["sofascore", "canary", "proxy", "manual-only"],
    params={
        "experimental_cap_bytes": Param(
            default=0,
            type="integer",
            minimum=0,
            title="Experimental hard cap (bytes)",
            description=(
                "Required explicit cap configured for proxy-filter's isolated "
                "sofascore_canary source. Zero fails closed."
            ),
        ),
        "target_cold_runs": Param(
            default=20,
            type="integer",
            minimum=20,
            title="Target accepted cold runs",
            description=(
                "Resumable total. Increase above 20 if five distinct exit "
                "hashes have not yet been observed."
            ),
        ),
    },
    doc_md=__doc__,
) as dag:
    collect = PythonOperator(
        task_id=PRODUCER_TASK_ID,
        python_callable=collect_fixed_cohort,
    )
    validate = PythonOperator(
        task_id="validate_candidate_manifest",
        python_callable=validate_candidate_manifest,
        trigger_rule="all_done",
    )

    collect >> validate
