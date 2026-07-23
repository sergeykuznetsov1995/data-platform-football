"""Source-native WhoScored daily ingestion DAG.

One isolated runner refreshes the persisted men's-competition catalog and then
ingests every active scope (schedule, previews, matches, events, lineups and
match stats) for the current window.  Traffic egresses through the residential
proxy pool: WhoScored blocks the datacentre host IP at Cloudflare, so the
transport reads ``WHOSCORED_PROXY_FILE`` and routes the direct curl/FlareSolverr
requests through one sticky pool member (see ``WhoScoredTransport``).

Data lands on the VM only: Bronze Iceberg (``iceberg.bronze.whoscored_*`` via
Trino) plus raw blobs in SeaweedFS (``WHOSCORED_RAW_STORE_URI``).  There is no
paid gateway, approval, pointer or off-host backup on this path.

History is a separate manual DAG (``dag_backfill_whoscored``); this DAG only
keeps the current window fresh.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import DAG_TAGS, SCHEDULES
from utils.default_args import SCRAPER_ARGS

logger = logging.getLogger(__name__)

DAG_ID = "dag_ingest_whoscored"
RUNNER = "dags/scripts/run_whoscored_scraper.py"
DISCOVERY_PATH = "/tmp/whoscored_discovery_{{ ts_nodash }}.json"
RESULT_PATH = "/tmp/whoscored_result_{{ ts_nodash }}.json"

# The scraper subprocess reads the residential pool from WHOSCORED_PROXY_FILE
# (host:port:user:pass, one sticky member per task).  Deploy points this at the
# mounted WhoScored pool; the default matches the standard proxy mount.
_TASK_ENV = {
    "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
    "WHOSCORED_PROXY_FILE": os.environ.get(
        "WHOSCORED_PROXY_FILE", "/opt/airflow/proxys.txt"
    ),
}

# Global-grain Bronze tables that must keep refreshing on every daily run.
_FRESHNESS_TABLES = (
    "bronze.whoscored_schedule",
    "bronze.whoscored_matches",
    "bronze.whoscored_events",
)


def _load_report(path: str) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except (OSError, ValueError) as exc:
        raise AirflowException(
            f"WhoScored runner report {path} is unavailable — the runner died "
            f"before writing it: {exc}"
        ) from exc
    if not isinstance(report, dict) or report.get("schema_version") != 3:
        raise AirflowException(f"WhoScored report {path} is not report schema v3")
    return report


def validate_data(**context: Any) -> None:
    """Fail closed unless the runner reported a fully successful ingest."""
    result_path = context["templates_dict"]["result_path"]
    report = _load_report(result_path)
    status = report.get("status")
    scopes = report.get("scopes") or []
    failed = [
        scope.get("scope")
        for scope in scopes
        if scope.get("status") not in {"success", "pending"}
    ]
    logger.info(
        "WhoScored daily: status=%s scopes=%d rows=%s failed=%s",
        status,
        len(scopes),
        report.get("rows"),
        failed,
    )
    if status != "success":
        raise AirflowException(
            f"WhoScored daily ingest status={status!r}; "
            f"errors={report.get('errors')}; failed_scopes={failed}"
        )


def validate_bronze_freshness(**_context: Any) -> None:
    """Telegram-alert when bronze.whoscored_* stops refreshing.

    A direct MAX(_ingested_at) staleness check independent of the current run's
    JSON floors: it surfaces a scheduler/storage stall where no current rows
    were committed.  WARNING severity because the producer and result validator
    already fail hard; 48h gives one missed daily run of grace.
    """
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    checks = [
        CHECK.freshness(table, ts_col="_ingested_at", max_age_hours=48,
                        severity="WARNING")
        for table in _FRESHNESS_TABLES
    ]
    report = run_checks(checks, raise_on_error=False)
    logger.info("validate_bronze_freshness: %s", report.summary())
    telegram_dq_summary(report, header="WhoScored Bronze freshness")


with DAG(
    dag_id=DAG_ID,
    default_args=SCRAPER_ARGS,
    schedule=SCHEDULES.get(DAG_ID),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=DAG_TAGS.get("whoscored"),
) as dag:
    discover_catalog = BashOperator(
        task_id="discover_catalog",
        bash_command=(
            "cd /opt/airflow && rm -f {discovery} && "
            "python {runner} discover "
            "--as-of-date {{{{ ds }}}} "
            "--transport-policy direct_only "
            "--output {discovery}"
        ).format(runner=RUNNER, discovery=DISCOVERY_PATH),
        env=_TASK_ENV,
        append_env=True,
    )

    ingest_daily = BashOperator(
        task_id="ingest_daily",
        bash_command=(
            "cd /opt/airflow && rm -f {result} && "
            "python {runner} daily "
            "--skip-profiles "
            "--transport-policy direct_only "
            "--output {result}"
        ).format(runner=RUNNER, result=RESULT_PATH),
        env=_TASK_ENV,
        append_env=True,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        templates_dict={"result_path": RESULT_PATH},
    )

    bronze_freshness = PythonOperator(
        task_id="validate_bronze_freshness",
        python_callable=validate_bronze_freshness,
        trigger_rule="all_done",
    )

    discover_catalog >> ingest_daily >> validate >> bronze_freshness
