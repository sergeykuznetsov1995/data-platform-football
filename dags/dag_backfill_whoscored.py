"""Manual, resumable WhoScored historical backfill.

The runner freezes exact match ids before capture and checkpoints after every
25-match chunk.  Task retry or a later manual run with the same ``queue_id``
continues that queue instead of selecting a new population.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS


RUN_ROOT = "/opt/airflow/logs/whoscored_runs"
STATE_DIR = "/opt/airflow/logs/whoscored_state"
BACKFILL_CHUNK_SIZE = 25
PAID_DAGRUN_LIMIT_BYTES = 8_000_000
BACKFILL_POOL = os.environ.get("WHOSCORED_BACKFILL_POOL", "whoscored_direct_pool")
_QUEUE_ID = re.compile(r"^[A-Za-z0-9_.-]{0,120}$")

BACKFILL_ARGS = {
    **{
        key: value
        for key, value in SCRAPER_ARGS.items()
        if key not in {"pool", "retries", "retry_delay", "execution_timeout"}
    },
    "retries": 1,
    "retry_delay": timedelta(hours=6),
    "execution_timeout": timedelta(hours=4),
}

_RUN_DIR_TEMPLATE = (
    RUN_ROOT + "/{{ dag.dag_id }}/{{ run_id | replace(':', '_') | replace('+', '_') }}"
)
_RESULT_TEMPLATE = _RUN_DIR_TEMPLATE + "/backfill.json"
_TASK_ENV = {
    "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
    "WHOSCORED_BACKFILL_SCOPES_JSON": "{{ params.scopes | tojson }}",
    "WHOSCORED_BACKFILL_GAME_IDS_JSON": "{{ params.game_ids | tojson }}",
    "WHOSCORED_BACKFILL_QUEUE_ID": "{{ params.queue_id }}",
    "WHOSCORED_REQUEST_LEDGER_PATH": (
        _RUN_DIR_TEMPLATE
        + "/requests_{{ task.task_id | replace('.', '_') }}_"
        + "try{{ ti.try_number }}.jsonl"
    ),
}


def validate_backfill_params(**context: Any) -> Dict[str, Any]:
    params = context.get("params", {})
    raw_scopes = params.get("scopes") or []
    game_ids = params.get("game_ids") or []
    all_catalog = bool(params.get("all_catalog", False))
    queue_id = str(params.get("queue_id") or "")
    if not raw_scopes and not all_catalog:
        raise AirflowException(
            "WhoScored backfill requires explicit scopes or all_catalog=true"
        )
    if raw_scopes and all_catalog:
        raise AirflowException(
            "WhoScored scopes and all_catalog are mutually exclusive"
        )
    if not _QUEUE_ID.fullmatch(queue_id):
        raise AirflowException("invalid WhoScored backfill queue_id")

    from dags.scripts.run_whoscored_scraper import RunnerScope

    try:
        scopes = [RunnerScope.parse(str(value)).spec for value in raw_scopes]
    except ValueError as exc:
        raise AirflowException(str(exc)) from exc
    if len(scopes) != len(set(scopes)):
        raise AirflowException("duplicate WhoScored backfill scopes")
    if any(isinstance(value, bool) or int(value) <= 0 for value in game_ids):
        raise AirflowException("WhoScored game_ids must be positive integers")

    date_from = params.get("date_from") or None
    date_to = params.get("date_to") or None
    if date_from and date_to and str(date_to) < str(date_from):
        raise AirflowException("date_to must not precede date_from")
    return {
        "scopes": scopes,
        "game_ids": sorted({int(value) for value in game_ids}),
        "all_catalog": all_catalog,
        "queue_id": queue_id or None,
        "chunk_size": BACKFILL_CHUNK_SIZE,
    }


def _result_path(context: Dict[str, Any]) -> Path:
    dag_id = getattr(context.get("dag"), "dag_id", "dag_backfill_whoscored")
    run_id = context.get("run_id") or context["dag_run"].run_id

    def safe(value: object) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")

    return Path(RUN_ROOT) / safe(dag_id) / safe(run_id) / "backfill.json"


def validate_backfill_result(**context: Any) -> Dict[str, Any]:
    path = _result_path(context)
    try:
        with path.open("r", encoding="utf-8") as handle:
            result = json.load(handle)
    except (OSError, ValueError) as exc:
        raise AirflowException(f"WhoScored backfill report unavailable: {exc}") from exc
    if result.get("schema_version") != 3 or result.get("command") != "backfill":
        raise AirflowException("WhoScored backfill report has an invalid contract")
    if result.get("status") != "success":
        raise AirflowException(f"WhoScored backfill failed: {result.get('errors', [])}")
    queue = result.get("queue") or {}
    if (
        queue.get("status") != "complete"
        or int(queue.get("pending_matches", -1))
        or int(queue.get("pending_profile_scopes", -1))
    ):
        raise AirflowException(f"WhoScored backfill queue is incomplete: {queue}")
    paid = int(result.get("paid_proxy_bytes") or 0)
    if paid > PAID_DAGRUN_LIMIT_BYTES:
        raise AirflowException(f"WhoScored backfill paid budget exceeded: {paid}")
    if context.get("params", {}).get("require_zero_paid", False) and paid:
        raise AirflowException(f"direct-only backfill used paid proxy: {paid}")
    return {"status": "success", "paid_proxy_bytes": paid, **queue}


def enforce_backfill_gate(**context: Any) -> Dict[str, Any]:
    dag_run = context.get("dag_run")
    ti = context.get("ti")
    if dag_run is None:
        raise AirflowException("backfill terminal gate requires dag_run context")
    current = getattr(ti, "task_id", "final_success_gate")
    failures = []
    for item in dag_run.get_task_instances():
        if item.task_id == current:
            continue
        state = str(item.state or "none").lower().split(".")[-1]
        if state != "success":
            failures.append(f"{item.task_id}={state}")
    if failures:
        raise AirflowException(
            "WhoScored backfill tasks were not successful: " + ", ".join(failures)
        )
    return {"status": "success"}


with DAG(
    dag_id="dag_backfill_whoscored",
    default_args=BACKFILL_ARGS,
    description="Explicit resumable WhoScored backfill with a frozen 25-match queue",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "scopes": Param(default=[], type="array", items={"type": "string"}),
        "game_ids": Param(default=[], type="array", items={"type": "integer"}),
        "date_from": Param(default=None, type=["null", "string"], format="date"),
        "date_to": Param(default=None, type=["null", "string"], format="date"),
        "queue_id": Param(default="", type="string", pattern="^[A-Za-z0-9_.-]{0,120}$"),
        "all_catalog": Param(default=False, type="boolean"),
        "require_zero_paid": Param(default=True, type="boolean"),
        "direct_only": Param(default=True, type="boolean"),
        "full_history_catalog": Param(default=False, type="boolean"),
    },
    tags=DAG_TAGS.get("whoscored", ["scraping", "whoscored", "backfill"]),
    doc_md="""
    Supply explicit competition-season scopes, `all_catalog=true` for every
    eligible discovered scope. The default queue chunk is fixed at 25. Reuse
    `queue_id` to resume from the durable checkpoint.
    """,
) as dag:
    validate_params = PythonOperator(
        task_id="validate_backfill_selectors",
        python_callable=validate_backfill_params,
        execution_timeout=timedelta(minutes=5),
    )
    run_backfill = BashOperator(
        task_id="run_whoscored_backfill",
        bash_command=(
            "cd /opt/airflow && "
            "python dags/scripts/run_whoscored_scraper.py backfill "
            '--scopes-json "$WHOSCORED_BACKFILL_SCOPES_JSON" '
            '--game-ids-json "$WHOSCORED_BACKFILL_GAME_IDS_JSON" '
            '--queue-id "$WHOSCORED_BACKFILL_QUEUE_ID" '
            f"--chunk-size {BACKFILL_CHUNK_SIZE} --state-dir {STATE_DIR} "
            "{% if params.all_catalog %}--all-catalog {% endif %}"
            "{% if params.date_from %}--date-from {{ params.date_from }} {% endif %}"
            "{% if params.date_to %}--date-to {{ params.date_to }} {% endif %}"
            "{% if params.direct_only %}--direct-only {% endif %}"
            "{% if params.full_history_catalog %}--full-history {% endif %}"
            f"--output {_RESULT_TEMPLATE}"
        ),
        env=_TASK_ENV,
        append_env=True,
        pool=BACKFILL_POOL,
        # The production pool has two slots. A historical backfill owns both,
        # so it cannot race a daily scope while remaining resumable itself.
        pool_slots=2,
        retries=1,
        retry_delay=timedelta(hours=6),
        execution_timeout=timedelta(hours=4),
    )
    backfill_dq = PythonOperator(
        task_id="validate_whoscored_backfill",
        python_callable=validate_backfill_result,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=10),
    )
    final_gate = PythonOperator(
        task_id="final_success_gate",
        python_callable=enforce_backfill_gate,
        trigger_rule="all_done",
    )

    validate_params >> run_backfill >> backfill_dq >> final_gate
