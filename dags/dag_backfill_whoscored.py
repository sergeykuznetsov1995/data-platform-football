"""Continuous journal-led WhoScored history; activation requires owner approval."""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS

logger = logging.getLogger(__name__)

DAG_ID = "dag_backfill_whoscored"
RUNNER = "dags/scripts/run_whoscored_scraper.py"
RESULT_PATH = "/tmp/whoscored_backfill_{{ ts_nodash }}.json"
# Tree the Bash task runs from; the isolated WhoScored stack mounts it outside
# /opt/airflow and sets WHOSCORED_RUNTIME_ROOT.
RUNTIME_ROOT = os.environ.get("WHOSCORED_RUNTIME_ROOT", "/opt/airflow")

# Each history worker touches at most 25 match pages; the runner clamps the
# whoscored_history_workers Variable to 1..4 (1 = off switch, #1481).
CHUNK_MATCH_LIMIT = 25
MAX_WORKERS = 4
CHUNK_TIMEOUT = timedelta(minutes=45)

# Cooldown between continuous runs: short while draining, long while idle.
ACTIVE_COOLDOWN = timedelta(seconds=30)
IDLE_COOLDOWN = timedelta(minutes=15)

_TASK_ENV = {
    # Order matters: runtime_contract requires PYTHONPATH to be an ordered
    # subsequence of the anchored sys.path (root, root/dags).
    "PYTHONPATH": f"{RUNTIME_ROOT}:{RUNTIME_ROOT}/dags",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
    # No default: an unset pool fails the chunk instead of using the host IP.
    "WHOSCORED_PROXY_FILE": os.environ.get("WHOSCORED_PROXY_FILE", ""),
}


def finalize_chunk(**context: Any) -> None:
    """Read the chunk report, log progress and schedule the next poll.

    With parallel workers the report lists each worker's history; every one
    is bounded separately and the total must match.
    """
    result_path = context["templates_dict"]["result_path"]
    try:
        with Path(result_path).open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except (OSError, ValueError) as exc:
        raise AirflowException("history report is missing or invalid") from exc
    if (not isinstance(report, dict) or report.get("schema_version") != 3
            or report.get("command") != "history" or report.get("status") != "success"
            or report.get("errors") != [] or not isinstance(report.get("history"), dict)):
        raise AirflowException("history chunk report failed validation")
    task = context["dag_run"].get_task_instance("run_backfill_chunk")
    if task is None or task.state != "success":
        raise AirflowException("history chunk process failed")
    workers = report["history"].get("workers", [report["history"]])
    if not isinstance(workers, list) or not 1 <= len(workers) <= MAX_WORKERS:
        raise AirflowException("history chunk has an invalid worker list")
    total = 0
    for worker in workers:
        attempted = worker.get("attempted") if isinstance(worker, dict) else None
        if type(attempted) is not int or not 0 <= attempted <= CHUNK_MATCH_LIMIT:
            raise AirflowException("history chunk exceeds match limit")
        total += attempted
    if report["history"].get("attempted") != total:
        raise AirflowException("history chunk total differs from its workers")
    did_work = any(worker.get("did_work", worker["attempted"] > 0) is True
                   for worker in workers)
    logger.info("WhoScored history chunk: %s", report["history"])
    now = datetime.now(timezone.utc)
    target = now + (ACTIVE_COOLDOWN if did_work else IDLE_COOLDOWN)
    context["ti"].xcom_push(key="next_poll_at", value=target.isoformat())


def _poll_ready(**context: Any) -> bool:
    raw_target = context["ti"].xcom_pull(task_ids="finalize_chunk", key="next_poll_at")
    if not raw_target:
        return True
    try:
        target = datetime.fromisoformat(str(raw_target)).astimezone(timezone.utc)
    except ValueError as exc:
        raise AirflowException("backfill next-poll timestamp is invalid") from exc
    return datetime.now(timezone.utc) >= target


with DAG(
    dag_id=DAG_ID,
    default_args=SCRAPER_ARGS,
    schedule="@continuous",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=DAG_TAGS.get("whoscored"),
) as dag:
    run_chunk = BashOperator(
        task_id="run_backfill_chunk",
        bash_command=(
            "cd {root} && rm -f {result} && "
            "python {runner} history "
            "--split-lanes {{{{ var.value.get('whoscored_history_enabled', 'false') | lower }}}} "
            "--workers {{{{ var.value.get('whoscored_history_workers', '3') | int }}}} "
            "--transport-policy direct_only "
            "--output {result}"
        ).format(
            root=RUNTIME_ROOT, runner=RUNNER, result=RESULT_PATH
        ),
        env=_TASK_ENV,
        append_env=True,
        # The chunk holds the single ingest pool slot that current collection
        # waits for (#1481): 45 min bounds that wait. Live chunks before
        # #1481 took 5 min median, 32 min max end to end.
        execution_timeout=CHUNK_TIMEOUT,
        retries=0,
    )

    finalize = PythonOperator(
        task_id="finalize_chunk",
        python_callable=finalize_chunk,
        templates_dict={"result_path": RESULT_PATH},
        trigger_rule="all_done",
    )

    cooldown = PythonSensor(
        task_id="wait_before_next_continuous_run",
        python_callable=_poll_ready,
        mode="reschedule",
        poke_interval=60,
        timeout=int(IDLE_COOLDOWN.total_seconds()) + 600,
    )

    run_chunk >> finalize >> cooldown
