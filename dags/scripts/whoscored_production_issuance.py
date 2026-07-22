"""Live predecessor-evidence replay for WhoScored daily paid issuance.

This module runs inside the already admitted scheduler container.  A promoted
rollout is allowed to receive a daily pointer only after its immutable
operational-store promotion proof has been replayed against the live Airflow
metadata DB, TaskInstances, and persisted XCom values.  The initial wave has
an explicit genesis authority and therefore has no predecessor runs to replay.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/dags/", 1)[0]
)
if _whoscored_production:
    if (
        getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None)
        != 2
    ):
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = _whoscored_bootstrap_sys._load_whoscored_runtime_contract(
    _whoscored_root
)

import re
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from dags.scripts.whoscored_ops_store import WhoScoredOpsStore
from dags.scripts.whoscored_rollout_acceptance import (
    ROLLOUT_GENESIS_PROOF_SHA256,
    idempotency_evidence,
    is_countable_scheduled_run,
    mapped_scope_dq_evidence,
    normalized_scope_plan_authority,
    promotion_acceptance_evidence,
    receipts_prefix,
    run_evidence_sha256,
    scope_plan_sha256,
    terminal_task_states_evidence,
)
from scrapers.whoscored.runtime_contract import validate_runtime_contract


_SCOPE_AUTHORITY_DIGEST_FIELDS = (
    "catalog_active_scopes_sha256",
    "cohort_sha256",
    "ranked_scope_ids_sha256",
    "promotion_acceptance_sha256",
    "promotion_terminal_receipt_sha256",
    "runtime_sha256",
    "classifier_sha256",
)
_SCOPE_AUTHORITY_FIELDS = frozenset(
    {
        "catalog_active_scope_count",
        "max_scopes",
        "require_full_active",
        "rollout_id",
        "wave_id",
        *_SCOPE_AUTHORITY_DIGEST_FIELDS,
    }
)
_WAVE_CONTRACTS = {
    "wave-20": (20, False),
    "wave-70": (70, False),
    "wave-all": (2_000, True),
}
_SOURCE_WAVES = {"wave-70": "wave-20", "wave-all": "wave-70"}
_DIGEST = re.compile(r"[0-9a-f]{64}")
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")


def _utc_iso(value: Any) -> str:
    if value is None or value.tzinfo is None:
        raise RuntimeError("WhoScored promotion DagRun time is invalid")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _airflow_components():
    """Import metadata models only inside the production scheduler runtime."""

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.xcom import XCom
    from airflow.utils.session import create_session
    from airflow.utils.xcom import XCOM_RETURN_KEY

    return DagRun, TaskInstance, XCom, create_session, XCOM_RETURN_KEY


def _verify_terminal_runs(
    terminal_runs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Replay exact immutable witnesses against the live Airflow metadata DB."""

    DagRun, TaskInstance, XCom, create_session, XCOM_RETURN_KEY = _airflow_components()
    run_ids = [run.get("run_id") for run in terminal_runs]
    if any(not isinstance(run_id, str) for run_id in run_ids) or len(run_ids) != len(
        set(run_ids)
    ):
        raise RuntimeError("WhoScored promotion DagRun identities are invalid")
    verified: list[dict[str, Any]] = []
    with create_session() as session:
        dag_runs = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "dag_ingest_whoscored",
                DagRun.run_id.in_(run_ids),
            )
            .all()
        )
        if len(dag_runs) != len(run_ids):
            raise RuntimeError("WhoScored promotion DagRun is missing from metadata DB")
        dag_run_by_id = {dag_run.run_id: dag_run for dag_run in dag_runs}
        if len(dag_run_by_id) != len(run_ids):
            raise RuntimeError("WhoScored metadata DB returned duplicate DagRuns")

        for witness in terminal_runs:
            run_id = witness["run_id"]
            dag_run = dag_run_by_id.get(run_id)
            if (
                dag_run is None
                or str(dag_run.state or "").lower().split(".")[-1] != "success"
                or _utc_iso(dag_run.execution_date) != witness.get("logical_date")
                or dag_run.end_date is None
                or dag_run.end_date < dag_run.execution_date
                or not is_countable_scheduled_run(
                    run_id=dag_run.run_id,
                    run_type=dag_run.run_type,
                    external_trigger=dag_run.external_trigger,
                    conf=dag_run.conf,
                )
            ):
                raise RuntimeError(
                    "WhoScored promotion DagRun is no longer terminal green"
                )

            task_rows = (
                session.query(
                    TaskInstance.task_id,
                    TaskInstance.map_index,
                    TaskInstance.state,
                )
                .filter(
                    TaskInstance.dag_id == "dag_ingest_whoscored",
                    TaskInstance.run_id == run_id,
                )
                .all()
            )
            task_state_values = [
                {
                    "task_id": row.task_id,
                    "map_index": row.map_index,
                    "state": str(row.state or "").lower().split(".")[-1],
                }
                for row in task_rows
            ]
            task_states = terminal_task_states_evidence(task_state_values)
            if task_states != witness.get("task_states"):
                raise RuntimeError(
                    "WhoScored promotion TaskInstance states have drifted"
                )

            xcom_rows = (
                session.query(XCom.map_index, XCom.value)
                .filter(
                    XCom.dag_id == "dag_ingest_whoscored",
                    XCom.run_id == run_id,
                    XCom.task_id == "validate_active_scope",
                    XCom.key == XCOM_RETURN_KEY,
                )
                .order_by(XCom.map_index.asc())
                .all()
            )
            expected_scope_dq = witness.get("scope_dq")
            expected_count = (
                expected_scope_dq.get("count")
                if isinstance(expected_scope_dq, Mapping)
                else None
            )
            if (
                isinstance(expected_count, bool)
                or not isinstance(expected_count, int)
                or [row.map_index for row in xcom_rows] != list(range(expected_count))
            ):
                raise RuntimeError(
                    "WhoScored promotion mapped DQ XCom set is not exact"
                )
            scope_dq_values = [XCom.deserialize_value(row) for row in xcom_rows]
            scope_dq = mapped_scope_dq_evidence(scope_dq_values)
            if scope_dq != expected_scope_dq:
                raise RuntimeError("WhoScored promotion mapped DQ evidence has drifted")

            singleton_task_ids = {
                "alert_preflight": "validate_whoscored_paid_alert_delivery",
                "catalog_dq": "validate_whoscored_catalog",
                "daily_slo": "validate_whoscored_daily_slo",
                "profile_dq": "validate_profile_refresh",
                "runtime_preflight": "validate_whoscored_runtime",
                "scope_plan": "freeze_daily_scope_plan",
                "traffic_dq": "report_whoscored_traffic",
            }
            singleton_rows = (
                session.query(XCom)
                .filter(
                    XCom.dag_id == "dag_ingest_whoscored",
                    XCom.run_id == run_id,
                    XCom.task_id.in_(tuple(singleton_task_ids.values())),
                    XCom.key == XCOM_RETURN_KEY,
                )
                .all()
            )
            by_task_id: dict[str, Any] = {}
            for row in singleton_rows:
                if row.map_index != -1 or row.task_id in by_task_id:
                    raise RuntimeError(
                        "WhoScored promotion singleton XCom set is not exact"
                    )
                by_task_id[row.task_id] = XCom.deserialize_value(row)
            if set(by_task_id) != set(singleton_task_ids.values()):
                raise RuntimeError(
                    "WhoScored promotion singleton XCom set is not exact"
                )
            singleton_values = {
                name: by_task_id[task_id]
                for name, task_id in singleton_task_ids.items()
            }
            observed_idempotency = idempotency_evidence(
                scope_dq=scope_dq_values,
                profile_dq=singleton_values["profile_dq"],
            )
            if observed_idempotency != witness.get("idempotency"):
                raise RuntimeError(
                    "WhoScored promotion idempotency evidence has drifted"
                )
            observed_scope_plan_sha256 = scope_plan_sha256(
                singleton_values["scope_plan"]
            )
            if observed_scope_plan_sha256 != witness.get("scope_plan_sha256"):
                raise RuntimeError("WhoScored promotion scope-plan XCom has drifted")
            observed_evidence_sha256 = run_evidence_sha256(
                scope_plan=singleton_values["scope_plan"],
                runtime_preflight=singleton_values["runtime_preflight"],
                catalog_dq=singleton_values["catalog_dq"],
                profile_dq=singleton_values["profile_dq"],
                traffic_dq=singleton_values["traffic_dq"],
                daily_slo=singleton_values["daily_slo"],
                alert_preflight=singleton_values["alert_preflight"],
                scope_dq=scope_dq_values,
                terminal_task_states=task_state_values,
            )
            if observed_evidence_sha256 != witness.get("evidence_sha256"):
                raise RuntimeError(
                    "WhoScored promotion green XCom evidence has drifted"
                )
            verified.append(
                {
                    "completed_at": _utc_iso(dag_run.end_date),
                    "evidence_sha256": observed_evidence_sha256,
                    "idempotency": observed_idempotency,
                    "logical_date": witness["logical_date"],
                    "run_id": run_id,
                    "scope_dq": scope_dq,
                    "scope_plan_sha256": observed_scope_plan_sha256,
                    "task_states": task_states,
                }
            )
    return verified


def _verify_newer_runs_belong_to_current_wave(
    terminal_runs: Sequence[Mapping[str, Any]],
    *,
    expected_authority: Mapping[str, Any],
) -> None:
    """Reject a stale predecessor if later scheduled runs are not this wave.

    Once promotion is active, later runs of the newly active wave are expected,
    so the predecessor cannot remain the newest DagRun forever.  Every later
    run must nevertheless be terminal and reveal an exact persisted scope plan
    for the current signed wave.  Thus a hidden later source-wave failure, or a
    queued/running run with no durable current-wave identity, fails closed.
    """

    if not terminal_runs:
        return
    terminal_logical = datetime.fromisoformat(
        str(terminal_runs[-1].get("logical_date") or "").replace("Z", "+00:00")
    )
    if terminal_logical.tzinfo is None:
        raise RuntimeError("WhoScored promotion terminal logical date is invalid")
    DagRun, _TaskInstance, XCom, create_session, XCOM_RETURN_KEY = _airflow_components()
    with create_session() as session:
        newer_runs = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "dag_ingest_whoscored",
                DagRun.run_id.like("scheduled__%"),
                DagRun.run_type == "scheduled",
                DagRun.external_trigger.is_(False),
                DagRun.execution_date > terminal_logical,
            )
            .order_by(DagRun.execution_date.asc(), DagRun.run_id.asc())
            .all()
        )
        for dag_run in newer_runs:
            state = str(dag_run.state or "").lower().split(".")[-1]
            if (
                state not in {"success", "failed"}
                or dag_run.end_date is None
                or dag_run.end_date < dag_run.execution_date
                or not is_countable_scheduled_run(
                    run_id=dag_run.run_id,
                    run_type=dag_run.run_type,
                    external_trigger=dag_run.external_trigger,
                    conf=dag_run.conf,
                )
            ):
                raise RuntimeError(
                    "WhoScored newer scheduled DagRun lacks terminal current-wave proof"
                )
            scope_rows = (
                session.query(XCom)
                .filter(
                    XCom.dag_id == "dag_ingest_whoscored",
                    XCom.run_id == dag_run.run_id,
                    XCom.task_id == "freeze_daily_scope_plan",
                    XCom.key == XCOM_RETURN_KEY,
                )
                .all()
            )
            if len(scope_rows) != 1 or scope_rows[0].map_index != -1:
                raise RuntimeError(
                    "WhoScored newer scheduled DagRun lacks exact scope-plan XCom"
                )
            scope = normalized_scope_plan_authority(
                XCom.deserialize_value(scope_rows[0])
            )
            if any(
                scope.get(field) != expected_authority[field]
                for field in _SCOPE_AUTHORITY_FIELDS
            ):
                raise RuntimeError(
                    "WhoScored newer scheduled DagRun belongs to a stale rollout wave"
                )


def verify_daily_issuance_rollout(
    *,
    rollout_id: str,
    expected_scope_authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Return live proof that the current rollout may receive today's pointer."""

    expected = dict(expected_scope_authority)
    wave_id = expected.get("wave_id")
    wave_contract = _WAVE_CONTRACTS.get(str(wave_id))
    catalog_count = expected.get("catalog_active_scope_count")
    if (
        not isinstance(rollout_id, str)
        or _TOKEN.fullmatch(rollout_id) is None
        or frozenset(expected) != _SCOPE_AUTHORITY_FIELDS
        or expected.get("rollout_id") != rollout_id
        or wave_contract is None
        or (expected.get("max_scopes"), expected.get("require_full_active"))
        != wave_contract
        or isinstance(catalog_count, bool)
        or not isinstance(catalog_count, int)
        or not 1 <= catalog_count <= 2_000
        or any(
            not isinstance(expected[field], str)
            or _DIGEST.fullmatch(expected[field]) is None
            for field in _SCOPE_AUTHORITY_DIGEST_FIELDS
        )
    ):
        raise RuntimeError("WhoScored issuance authority is invalid")
    promotion_acceptance_sha256 = expected["promotion_acceptance_sha256"]
    promotion_terminal_receipt_sha256 = expected["promotion_terminal_receipt_sha256"]
    runtime_sha256 = expected["runtime_sha256"]
    classifier_sha256 = expected["classifier_sha256"]

    runtime = validate_runtime_contract(report_schema_version=3)
    runtime_release = {
        "parser_version": runtime["parser_version"],
        "manifest_sha256": runtime["manifest_sha256"],
        "code_tree_sha256": runtime["code_tree_sha256"],
    }
    if runtime_release["code_tree_sha256"] != runtime_sha256:
        raise RuntimeError("WhoScored issuance runtime differs from signed authority")
    ops_store = WhoScoredOpsStore.from_env(optional=False)
    if ops_store is None:
        raise RuntimeError("WhoScored operational store is required")
    records = list(ops_store.iter_content_addressed_json(receipts_prefix(rollout_id)))
    if wave_id == "wave-20":
        if (
            promotion_acceptance_sha256 != ROLLOUT_GENESIS_PROOF_SHA256
            or promotion_terminal_receipt_sha256 != ROLLOUT_GENESIS_PROOF_SHA256
        ):
            raise RuntimeError("WhoScored initial issuance lacks genesis authority")
        promotion = {
            "classifier_sha256": classifier_sha256,
            "promotion_acceptance_sha256": promotion_acceptance_sha256,
            "receipt_sha256s": [],
            "release": runtime_release,
            "runtime_sha256": runtime_sha256,
            "schema_version": 1,
            "source_cohort_sha256": None,
            "source_wave_id": None,
            "terminal_receipt_sha256": promotion_terminal_receipt_sha256,
        }
        terminal_runs: Sequence[Mapping[str, Any]] = []
    else:
        source_wave_id = _SOURCE_WAVES[wave_id]
        live = promotion_acceptance_evidence(
            records,
            rollout_id=rollout_id,
            source_wave_id=source_wave_id,
            expected_terminal_receipt_sha256=promotion_terminal_receipt_sha256,
        )
        if (
            live.get("promotion_acceptance_sha256") != promotion_acceptance_sha256
            or live.get("terminal_receipt_sha256") != promotion_terminal_receipt_sha256
            or live.get("runtime_sha256") != runtime_sha256
            or live.get("classifier_sha256") != classifier_sha256
            or live.get("release") != runtime_release
        ):
            raise RuntimeError(
                "WhoScored live promotion proof differs from signed authority"
            )
        terminal_runs = live["terminal_runs"]
        expected_run_count = 2 if wave_id == "wave-70" else 4
        if len(terminal_runs) != expected_run_count:
            raise RuntimeError("WhoScored promotion predecessor run set is incomplete")
        promotion = {
            field: live[field]
            for field in (
                "classifier_sha256",
                "promotion_acceptance_sha256",
                "receipt_sha256s",
                "release",
                "runtime_sha256",
                "schema_version",
                "source_cohort_sha256",
                "source_wave_id",
                "terminal_receipt_sha256",
            )
        }

    verified_terminal_runs = (
        _verify_terminal_runs(terminal_runs) if terminal_runs else []
    )
    _verify_newer_runs_belong_to_current_wave(
        terminal_runs,
        expected_authority=expected,
    )
    return {
        "promotion": promotion,
        "rollout_id": rollout_id,
        "runtime_release": runtime_release,
        "schema_version": 1,
        "status": "live-authority-verified",
        "terminal_runs": verified_terminal_runs,
        "wave_id": wave_id,
    }
