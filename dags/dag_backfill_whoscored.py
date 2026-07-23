"""Source-native WhoScored history backfill DAG.

Manual, self-draining backfill of every men's competition WhoScored exposes.
One isolated runner freezes a durable plan (kept in the on-VM ops store) over
the full-history catalog and drains it in bounded chunks: each continuous run
processes up to ``max_work_items`` schedule/match/roster/profile work items and
resumes the same ``queue_id`` until the plan is complete.

Traffic egresses through the residential proxy pool (``WHOSCORED_PROXY_FILE``);
data lands on the VM only — Bronze Iceberg via Trino plus raw blobs in
SeaweedFS.  No paid gateway, approval, pointer or off-host backup.

Paused on creation: unpause to start draining, pause to stop.  Daily current
data is a separate DAG (``dag_ingest_whoscored``).
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS

logger = logging.getLogger(__name__)

DAG_ID = "dag_backfill_whoscored"
RUNNER = "dags/scripts/run_whoscored_scraper.py"
QUEUE_ID = "whoscored-history"
RESULT_PATH = "/tmp/whoscored_backfill_{{ ts_nodash }}.json"

# Cooldown between continuous runs: short while draining, long once the plan is
# empty so an idle DAG does not hot-spin the shared scheduler.
ACTIVE_COOLDOWN = timedelta(seconds=30)
IDLE_COOLDOWN = timedelta(minutes=15)

_TASK_ENV = {
    "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
    "WHOSCORED_PROXY_FILE": os.environ.get(
        "WHOSCORED_PROXY_FILE", "/opt/airflow/proxys.txt"
    ),
}


def finalize_chunk(**context: Any) -> None:
    """Read the chunk report, log progress and schedule the next poll."""
    result_path = context["templates_dict"]["result_path"]
    try:
        with Path(result_path).open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except (OSError, ValueError):
        report = {}
    rows = int(report.get("rows") or 0)
    status = report.get("status")
    did_work = rows > 0
    logger.info(
        "WhoScored backfill chunk: status=%s rows=%d queue=%s",
        status, rows, QUEUE_ID,
    )
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
    params={
        "max_work_items": Param(100, type="integer", minimum=1, maximum=100),
    },
) as dag:
    run_chunk = BashOperator(
        task_id="run_backfill_chunk",
        bash_command=(
            "cd /opt/airflow && rm -f {result} && "
            "python {runner} backfill "
            "--all-catalog "
            "--as-of-date {{{{ ds }}}} "
            "--queue-id {queue} "
            "--max-work-items {{{{ params.max_work_items }}}} "
            "--transport-policy direct_only "
            "--output {result}"
        ).format(runner=RUNNER, queue=QUEUE_ID, result=RESULT_PATH),
        env=_TASK_ENV,
        append_env=True,
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
