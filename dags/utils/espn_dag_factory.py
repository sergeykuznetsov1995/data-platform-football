"""Thin production DAG factory for ESPN Native Bronze v2."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from utils import espn_native_tasks as tasks


def build_espn_ingest_dag(*, dag_id: str, mode: str) -> DAG:
    if mode not in {"daily", "repair", "backfill", "replay"}:
        raise ValueError("unsupported ESPN DAG mode")
    network = mode != "replay"
    producer_task_ids = (
        [
            "validate_registry_and_admission",
            "acquire_scope_leases",
            "build_signed_scope_plans",
        ]
        + (
            [
                "fetch_scoreboard_batches",
                "plan_summary_batches",
                "fetch_summary_batches",
                "reduce_raw_manifests",
            ]
            if network
            else ["bind_replay_raw_manifests"]
        )
        + [
            "offline_parse",
            "staging_dq",
            "publish_scopes",
            "persist_run_manifests",
            "published_dq",
        ]
    )
    with DAG(
        dag_id=dag_id,
        description=f"ESPN Native Bronze v2 {mode} ingestion",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=8),
        tags=["espn", "native", "bronze"],
        params={
            "scopes": Param(default=[], type="array"),
            "attempt": Param(default=1, type="integer", minimum=1),
            "replay_sources": Param(default={}, type="object"),
        },
    ) as dag:
        admission = PythonOperator(
            task_id="validate_registry_and_admission",
            python_callable=tasks.validate_registry_and_admission,
            op_kwargs={"mode": mode},
            retries=0,
        )
        leasing = PythonOperator(
            task_id="acquire_scope_leases",
            python_callable=tasks.acquire_scope_leases,
            op_kwargs={"admission_ref": admission.output},
            multiple_outputs=True,
            retries=0,
        )
        planning = PythonOperator(
            task_id="build_signed_scope_plans",
            python_callable=tasks.build_signed_scope_plans,
            op_kwargs={"leasing": leasing.output},
            multiple_outputs=True,
            retries=0,
        )
        admission >> leasing >> planning

        if network:
            scoreboard = PythonOperator.partial(
                task_id="fetch_scoreboard_batches",
                python_callable=tasks.fetch_scoreboard_batch,
                pool=tasks.HTTP_POOL,
                pool_slots=1,
                retries=1,
                retry_delay=timedelta(minutes=1),
            ).expand(op_kwargs=planning.output["scope_binding_refs"])
            summary_plan = PythonOperator(
                task_id="plan_summary_batches",
                python_callable=tasks.plan_summary_batch_wave,
                op_kwargs={"scoreboard_phase_refs": scoreboard.output},
                multiple_outputs=True,
                retries=0,
            )
            summary_fetch = PythonOperator.partial(
                task_id="fetch_summary_batches",
                python_callable=tasks.fetch_summary_batch,
                pool=tasks.HTTP_POOL,
                pool_slots=1,
                retries=1,
                retry_delay=timedelta(minutes=1),
            ).expand(op_kwargs=summary_plan.output["summary_batch_refs"])
            raw_source = PythonOperator(
                task_id="reduce_raw_manifests",
                python_callable=tasks.reduce_raw_manifest_wave,
                op_kwargs={
                    "summary_index_ref": summary_plan.output["summary_index_ref"],
                    "summary_phase_refs": summary_fetch.output,
                },
                trigger_rule="none_failed",
                retries=0,
            )
            planning >> scoreboard >> summary_plan >> summary_fetch >> raw_source
        else:
            raw_source = PythonOperator(
                task_id="bind_replay_raw_manifests",
                python_callable=tasks.bind_replay_raw_manifests,
                op_kwargs={"planning": planning.output},
                retries=0,
            )
            planning >> raw_source

        offline = PythonOperator.partial(
            task_id="offline_parse",
            python_callable=tasks.offline_parse_scope,
            retries=0,
        ).expand(op_kwargs=raw_source.output)
        staging_dq = PythonOperator.partial(
            task_id="staging_dq",
            python_callable=tasks.staging_dq_scope,
            retries=0,
        ).expand(op_kwargs=offline.output)
        publish = PythonOperator.partial(
            task_id="publish_scopes",
            python_callable=tasks.publish_scope,
            retries=0,
        ).expand(op_kwargs=staging_dq.output)
        persist = PythonOperator(
            task_id="persist_run_manifests",
            python_callable=tasks.persist_run_manifests,
            op_kwargs={
                "plan_index_ref": planning.output["plan_index_ref"],
                "publication_refs": publish.output,
            },
            multiple_outputs=True,
            retries=0,
        )
        published_dq = PythonOperator.partial(
            task_id="published_dq",
            python_callable=tasks.published_dq_scope,
            retries=0,
        ).expand(op_kwargs=persist.output["publication_refs"])
        raw_source >> offline >> staging_dq >> publish >> persist >> published_dq

        verdict = PythonOperator(
            task_id="terminal_verdict",
            python_callable=tasks.terminal_verdict,
            op_kwargs={
                "producer_task_ids": producer_task_ids,
            },
            trigger_rule="all_done",
            retries=0,
        )
        health = PythonOperator(
            task_id="record_health_metrics",
            python_callable=tasks.record_health_metrics,
            op_kwargs={},
            trigger_rule="all_done",
            retries=0,
        )
        release = PythonOperator(
            task_id="release_scope_leases",
            python_callable=tasks.release_scope_leases,
            op_kwargs={},
            trigger_rule="all_done",
            retries=0,
        )
        propagate = PythonOperator(
            task_id="propagate_terminal_failure",
            python_callable=tasks.propagate_terminal_failure,
            op_kwargs={
                "cleanup_task_ids": [
                    "record_health_metrics",
                    "release_scope_leases",
                ]
            },
            trigger_rule="all_done",
            retries=0,
        )
        published_dq >> verdict
        verdict >> health
        verdict >> release
        [health, release] >> propagate
    return dag


__all__ = ["build_espn_ingest_dag"]
