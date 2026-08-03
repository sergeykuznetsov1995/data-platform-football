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


# Error budget (#1053): every daily run carries a handful of source-side scope
# failures (observed steady state: ~4 of 141), so "any failure = red" would be
# permanently red — as useless as the permanently green all_done leaf it
# replaces. Red is reserved for signals someone must act on: a protected
# top-league scope failing, the failed share exceeding the budget, or a run
# that wrote nothing at all. Everything within budget passes loudly, with the
# counters in the log.
WHOSCORED_DAILY_MAX_FAILED_SCOPE_SHARE = 0.05
WHOSCORED_PROTECTED_SCOPE_PREFIXES = (
    "ENG-Premier League",
    "ESP-La Liga",
    "GER-Bundesliga",
    "ITA-Serie A",
    "FRA-Ligue 1",
)


def validate_data(**context: Any) -> None:
    """Error-budget gate over the runner report (#1053)."""
    result_path = context["templates_dict"]["result_path"]
    report = _load_report(result_path)
    status = report.get("status")
    scopes = report.get("scopes") or []
    rows = int(report.get("rows") or 0)
    failed = [
        str(scope.get("scope") or "")
        for scope in scopes
        if scope.get("status") not in {"success", "pending"}
    ]
    protected_failed = [
        scope
        for scope in failed
        if scope.startswith(WHOSCORED_PROTECTED_SCOPE_PREFIXES)
    ]
    logger.info(
        "WhoScored daily: status=%s scopes=%d rows=%s failed=%d (%s) protected_failed=%s",
        status,
        len(scopes),
        rows,
        len(failed),
        ", ".join(failed) or "-",
        protected_failed or "-",
    )
    if not scopes:
        if status == "success":
            return
        raise AirflowException(
            f"WhoScored daily ingest planned no scopes and status={status!r}; "
            f"errors={report.get('errors')}"
        )
    if protected_failed:
        raise AirflowException(
            f"WhoScored daily ingest failed protected scope(s): {protected_failed}"
        )
    share = len(failed) / len(scopes)
    if share > WHOSCORED_DAILY_MAX_FAILED_SCOPE_SHARE:
        raise AirflowException(
            f"WhoScored daily ingest failed {len(failed)}/{len(scopes)} scopes "
            f"({share:.1%} > budget {WHOSCORED_DAILY_MAX_FAILED_SCOPE_SHARE:.0%}); "
            f"errors={report.get('errors')}"
        )
    if rows <= 0:
        raise AirflowException(
            "WhoScored daily ingest wrote zero rows across "
            f"{len(scopes)} scope(s) — collection did not happen"
        )
    if failed:
        logger.warning(
            "WhoScored daily ingest is within the error budget: "
            "%d/%d failed scope(s), %d rows written",
            len(failed),
            len(scopes),
            rows,
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
        # The runner exits non-zero whenever ANY scope failed, which is the
        # steady state (#1053). A written report means the run completed and
        # the error-budget gate downstream is the judge; the task itself only
        # fails when the runner died without a report.
        bash_command=(
            "cd /opt/airflow && rm -f {result} && "
            "python {runner} daily "
            "--skip-profiles "
            "--transport-policy direct_only "
            "--output {result} "
            "|| [ -s {result} ]"
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

    # bronze_freshness stays useful on a red run (all_done), but it must not
    # be the sole leaf that colours the run green while validate_data is
    # upstream_failed (#1053) — the gate is a leaf of its own now.
    discover_catalog >> ingest_daily >> validate
    ingest_daily >> bronze_freshness
