"""Trigger-only FotMob Silver core build with A/B slots (#2020, epic #1677)."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from utils import fotmob_silver_build as build
from utils.fotmob_silver_admission import AdmittedCursor
from utils.default_args import SILVER_ARGS

SQL_DIR = Path(__file__).resolve().parent / "sql" / "silver"
BUILD_TASKS = {
    "build_match_tables": "match",
    "build_season_tables": "season",
    "build_global_tables": "global",
}
BUILD_TIMEOUT = timedelta(hours=6)


def _config(params: Dict[str, Any]) -> build.BuildConfig:
    """Always all competitions and all 22 tables: the switch publishes the whole set.

    Narrow builds (``competitions``/``tables``) are for host-side pilots via
    :class:`fotmob_silver_build.BuildConfig` only.
    """
    batch_size = int(params.get("max_partitions_per_batch") or 10)
    if not 1 <= batch_size <= 10:
        raise ValueError("production FotMob Silver batches must contain 1–10 partitions")
    return build.BuildConfig(
        sql_dir=SQL_DIR,
        max_partitions_per_batch=batch_size,
        entity_buckets=dict(params.get("entity_buckets") or {}) or None,
        full_rebuild=bool(params.get("full_rebuild")) and params.get("mode") == "build",
    )


@contextmanager
def _cursor():
    from utils.silver_tasks import _PARTITION_STAGED_SESSION_PROPERTIES, _get_trino_connection

    conn = _get_trino_connection(
        session_properties={**_PARTITION_STAGED_SESSION_PROPERTIES, **build.SESSION_PROPERTIES}
    )
    try:
        yield AdmittedCursor(conn.cursor())
    finally:
        conn.close()


def _bootstrap_reader(**context) -> Dict[str, Any]:
    """Seed reader/build state; XCom ``{active_slot, revision, build_slot}``."""
    with _cursor() as cur:
        return build.prepare_run(cur, _config(context["params"]))


def _build(family: str, **context) -> int:
    """Rebuild what changed in one family; 0 in rollback mode or when nothing changed."""
    params = context["params"]
    if params.get("mode") == "rollback":
        return 0
    run = context["ti"].xcom_pull(task_ids="bootstrap_reader")
    with _cursor() as cur:
        return build.build_family(
            cur, _config(params), family, run["build_slot"], context["run_id"],
            expected_revision=run["revision"],
        )


def _switch_reader(**context) -> Dict[str, Any]:
    """Publish one state pointer for all 22 readers; empty input switches onto an equal set."""
    params = context["params"]
    ti = context["ti"]
    run = ti.xcom_pull(task_ids="bootstrap_reader")
    rebuilt = sum(int(value or 0) for value in ti.xcom_pull(task_ids=list(BUILD_TASKS)) or ())
    logging.getLogger(__name__).info("FotMob Silver: %s unit(s) rebuilt in slot %s", rebuilt, run["build_slot"])
    with _cursor() as cur:
        return build.switch_reader(
            cur, _config(params), mode=params.get("mode", "build"),
            expected_revision=run["revision"], actor=context["run_id"],
        )


with DAG(
    dag_id="dag_transform_fotmob_silver_v2",
    default_args=SILVER_ARGS,
    description="Build FotMob Silver core (22 tables) into an inactive A/B slot and switch readers",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["transform", "fotmob", "silver", "football", "trino"],
    params={
        "max_partitions_per_batch": Param(default=10, type="integer", minimum=1, maximum=10),
        "entity_buckets": Param(
            default={}, type="object",
            description="Override mod(key, N) buckets per global table, e.g. {\"fotmob_players\": 8}",
        ),
        "full_rebuild": Param(
            default=False, type="boolean",
            description="Emergency: clear the build slot's data and state first (template changes are detected)",
        ),
        "mode": Param(default="build", enum=["build", "rollback"]),
    },
    doc_md="""
    ## FotMob Silver core (v2)

    Builds all 22 `silver.fotmob_*` core tables for all competitions into the slot readers do not see
    (`<table>_a` / `<table>_b`, `utils.silver_slots`), then `switch_reader` updates one
    state pointer (`advance`, actor = run_id). Permanent views select that slot.
    Separate SQL queries may select different versions. Reader barriers wait for older
    transactions before slot reuse and publication; writers must be serialized.

    * Increment: a context partition `(competition_id, source_season_key)` or a global table
      is rebuilt only when the signal of its bronze `_current` views (max `_observed_at`,
      max `_ingested_at`, row count) differs from the state recorded for the build slot
      (`ops.fotmob_silver_build_state`). A changed SQL template
      (sha256 in the state) rebuilds that slot table in full. Both slots start empty, so
      the first two runs are full builds.
    * Empty input (nothing rebuilt) -> `switch_reader` still advances onto the build slot,
      which then equals bronze (an equal set). `mode=rollback` -> readers go back to the
      other slot, nothing is built; run it before the next build (refused once a build started
      on that slot), the abandoned slot is then cleared and rebuilt in full by the next build.
    * Both physical schemas remain in place. Incompatible SQL schema changes fail before data writes
      and require an explicit migration.
    * Every query is admitted only at 00:30–02:45 MSK with at most five foreign
      RUNNING queries. Busy/closed admission fails the run; completed batches
      remain available for the next nightly resume. No partial publication.
    * No schedule until #2021 authorization; not triggered by ingest.
    """,
) as dag:
    bootstrap_reader = PythonOperator(task_id="bootstrap_reader", python_callable=_bootstrap_reader)
    builds = [
        PythonOperator(
            task_id=task_id,
            python_callable=_build,
            op_kwargs={"family": family},
            execution_timeout=BUILD_TIMEOUT,
        )
        for task_id, family in BUILD_TASKS.items()
    ]
    switch_reader = PythonOperator(task_id="switch_reader", python_callable=_switch_reader)
    bootstrap_reader >> builds[0] >> builds[1] >> builds[2] >> switch_reader
