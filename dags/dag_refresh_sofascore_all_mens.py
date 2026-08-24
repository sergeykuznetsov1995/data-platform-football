"""Current-season refresh of every adult-men SofaScore tournament (lane F).

Three times a day the lane asks the source for the football events of
yesterday and today (metered discovery, ``bronze.sofascore_schedule``), then
runs the existing matches phase of the scope cycle on the ``SS-*`` partitions
holding finished games without a complete capture.  The 14 configured leagues
stay with ``dag_ingest_sofascore``; the frozen campaign state (state.json,
failures.json) is never touched by this lane.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils import sofascore_all_mens_state as state


DAG_ID = "dag_refresh_sofascore_all_mens"
SNAPSHOT_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_SNAPSHOT",
    "/opt/airflow/runtime/sofascore/all-men/snapshot.json",
)
POLICY_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_POLICY",
    "/opt/airflow/configs/sofascore/all_mens_campaign.json",
)
WORKLOAD_ARTIFACT = os.environ.get(
    "SOFASCORE_PROXY_BUDGET_ARTIFACT",
    "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
)
RESULT_DIR = (
    os.environ.get("SOFASCORE_REFRESH_RESULT_DIR", "").strip()
    or state.DEFAULT_REFRESH_RESULT_DIR
)

# Refresh lane knobs; every one is read at DAG parse and fails closed.
REFRESH_BATCH_SIZE = state.env_int(
    "SOFASCORE_REFRESH_BATCH_SIZE", state.DEFAULT_REFRESH_BATCH_SIZE, 1, 64
)
REFRESH_POOL = (
    os.environ.get("SOFASCORE_REFRESH_POOL", "").strip() or INGEST_SCRAPER_POOL
)
REFRESH_MAX_ACTIVE_TASKS = state.env_int(
    "SOFASCORE_REFRESH_MAX_ACTIVE_TASKS", 1, 1, 16
)
# Four daily lists of 1-3 MB each; the gateway meters them per DagRun.
REFRESH_DISCOVERY_BUDGET_BYTES = state.env_int(
    "SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", 16 * 1024 * 1024, 1, 1024 ** 3
)
# Forwarded to every task of the lane only when set: a gateway of its own.
REFRESH_TASK_ENV: dict[str, str] = {}
_control_url = os.environ.get("SOFASCORE_REFRESH_PROXY_CONTROL_URL", "").strip()
if _control_url:
    REFRESH_TASK_ENV["SOFASCORE_PROXY_CONTROL_URL"] = _control_url
REFRESH_TASK_IDS = frozenset({
    "fetch_daily_events",
    "plan_refresh_batch",
    "run_refresh_scope",
    "validate_refresh_scope",
})

# Finished games of campaign partitions that have no complete capture yet,
# per partition, busiest first.  ``season`` is CAST on both sides because the
# schedule stores it as the source ships it while the status table is text.
PENDING_PARTITIONS_SQL = """
SELECT s.league, CAST(s.season AS varchar) AS season,
       count(DISTINCT s.game_id) AS pending_matches
FROM iceberg.bronze.sofascore_schedule s
LEFT JOIN iceberg.bronze.sofascore_match_capture_status c
  ON c.league = s.league
 AND CAST(c.season AS varchar) = CAST(s.season AS varchar)
 AND c.match_id = CAST(s.game_id AS varchar)
 AND c.capture_complete = true
WHERE s.league LIKE 'SS-%'
  AND s.status_type = 'finished'
  AND c.match_id IS NULL
GROUP BY 1, 2
ORDER BY 3 DESC, 1, 2
"""


def _pending_refresh_partitions() -> list[tuple[str, str, int]]:
    """Query at task runtime; no Trino client is touched at DAG parse."""

    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(PENDING_PARTITIONS_SQL)
        return [
            (str(league), str(season), int(count))
            for league, season, count in cursor.fetchall()
        ]
    finally:
        conn.close()


def _configured_tournament_ids() -> frozenset[int]:
    """Source ids of the leagues the daily ingest already covers."""

    from scrapers.sofascore.catalog import SofaScoreCatalog

    return frozenset(SofaScoreCatalog.load().tournament_map(enabled_only=True).values())


def _plan_refresh_batch(**context: Any) -> list[dict[str, str]]:
    snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    return state.plan_refresh_batch(
        snapshot,
        _pending_refresh_partitions(),
        batch_size=REFRESH_BATCH_SIZE,
        exclude_tournament_ids=_configured_tournament_ids(),
        snapshot_path=SNAPSHOT_PATH,
        policy_path=POLICY_PATH,
        result_dir=RESULT_DIR,
        workload_artifact=WORKLOAD_ARTIFACT,
        dag_run_id=str(context.get("run_id") or "manual"),
        task_env=REFRESH_TASK_ENV,
    )


def _validate_refresh_scope(**environment: str) -> dict[str, Any]:
    result_path = Path(environment["SOFASCORE_SCOPE_RESULT_PATH"])
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"scope result is unreadable: {exc}") from exc
    if result.get("status") != "success":
        raise AirflowException("scope cycle did not finish successfully")
    if environment.get("SOFASCORE_CAMPAIGN_ACTION") != "refresh":
        raise AirflowException("unknown SofaScore campaign action")
    campaign_id, tournament_id, season_id = environment[
        "SOFASCORE_SCOPE_KEY"
    ].split(":")
    if (
        result.get("snapshot_id") != environment["SOFASCORE_EXPECTED_SNAPSHOT_ID"]
        or result.get("campaign_id") != campaign_id
        or int(result.get("tournament_id", 0)) != int(tournament_id)
        or int(result.get("source_season_id", 0)) != int(season_id)
    ):
        raise AirflowException("scope result provenance mismatch")
    # A refreshed scope is never "completed": the next DagRun re-plans it
    # from Bronze evidence as new games finish.
    return {"status": "refreshed", "scope_key": environment["SOFASCORE_SCOPE_KEY"]}


def _task_state(task_instance: Any) -> str:
    value = getattr(task_instance.state, "value", task_instance.state)
    return str(value or "none").casefold().split(".")[-1]


def _propagate_status(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise AirflowException("refresh watcher has no DagRun")
    failures = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id not in REFRESH_TASK_IDS:
            continue
        if _task_state(task_instance) in {"failed", "upstream_failed"}:
            failures.append(task_instance.task_id)
    if failures:
        raise AirflowException(
            "SofaScore refresh attempt failed: " + ", ".join(sorted(failures))
        )
    return {"status": "success"}


FETCH_COMMAND = """
set -euo pipefail
cd /opt/airflow
/opt/legacy-scraper-venv/bin/python \
  dags/scripts/run_sofascore_daily_events.py \
  --snapshot "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
  --budget-cap-bytes "${SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES}" \
  --dag-id "${AIRFLOW_CTX_DAG_ID}" \
  --run-id "${AIRFLOW_CTX_DAG_RUN_ID}" \
  --task-id "${AIRFLOW_CTX_TASK_ID}" \
  --output "${SOFASCORE_REFRESH_RESULT_DIR}/daily-events-{{ ts_nodash }}.json"
"""

RUN_SCOPE_COMMAND = """
set -euo pipefail
cd /opt/airflow
/opt/legacy-scraper-venv/bin/python \
  dags/scripts/run_sofascore_scope_cycle.py \
  --snapshot "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
  --tournament-id "${SOFASCORE_TOURNAMENT_ID}" \
  --source-season-id "${SOFASCORE_SOURCE_SEASON_ID}" \
  --expected-snapshot-id "${SOFASCORE_EXPECTED_SNAPSHOT_ID}" \
  --expected-campaign-id "${SOFASCORE_EXPECTED_CAMPAIGN_ID}" \
  --phase matches \
  --season-evidence bronze \
  --allow-pending-season \
  --output-dir "${SOFASCORE_SCOPE_OUTPUT_DIR}" \
  --output "${SOFASCORE_SCOPE_RESULT_PATH}" \
  --workload-artifact "${SOFASCORE_WORKLOAD_ARTIFACT}" \
  --run-id "${SOFASCORE_SCOPE_RUN_ID}"
"""


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Current season of every adult-men SofaScore tournament, 3x/day",
    # After the European evening, in the morning, after the daily ingest.
    schedule="30 0,8,15 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=REFRESH_MAX_ACTIVE_TASKS,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=5),
    render_template_as_native_obj=True,
    tags=["sofascore", "refresh", "all-men"],
) as dag:
    fetch = BashOperator(
        task_id="fetch_daily_events",
        bash_command=FETCH_COMMAND,
        env={
            **REFRESH_TASK_ENV,
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "SOFASCORE_CAMPAIGN_SNAPSHOT": SNAPSHOT_PATH,
            "SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES": str(
                REFRESH_DISCOVERY_BUDGET_BYTES
            ),
            "SOFASCORE_REFRESH_RESULT_DIR": RESULT_DIR,
        },
        append_env=True,
        pool=REFRESH_POOL,
        priority_weight=5,
        do_xcom_push=False,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=45),
    )
    plan = PythonOperator(
        task_id="plan_refresh_batch",
        python_callable=_plan_refresh_batch,
        retries=0,
    )
    run = BashOperator.partial(
        task_id="run_refresh_scope",
        bash_command=RUN_SCOPE_COMMAND,
        append_env=True,
        pool=REFRESH_POOL,
        # Above the history lane (1) in the shared pool.
        priority_weight=5,
        do_xcom_push=False,
        max_active_tis_per_dag=REFRESH_MAX_ACTIVE_TASKS,
        # One retry keeps the same run_id: the gateway reuses the signed
        # plan and a latched lease is re-claimed after the reaper grace.
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=2),
    ).expand(env=plan.output)
    validate = PythonOperator.partial(
        task_id="validate_refresh_scope",
        python_callable=_validate_refresh_scope,
        retries=0,
    ).expand(op_kwargs=plan.output)
    propagate = PythonOperator(
        task_id="propagate_refresh_status",
        python_callable=_propagate_status,
        trigger_rule="all_done",
        retries=0,
    )

    fetch >> plan >> run >> validate >> propagate


__all__ = ["DAG_ID", "dag"]
