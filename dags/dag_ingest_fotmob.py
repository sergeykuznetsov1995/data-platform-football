"""Source-native FotMob ingestion DAG.

The DAG is trigger-only: ``dag_master_pipeline`` is the single daily schedule
owner.  One isolated runner performs catalog discovery, exact-season planning,
raw-first ingestion and emits an atomic, run-specific report.  Validation is
fail-closed and Silver can only run after a complete native report.
"""

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.config import DAG_TAGS, FOTMOB_HTTP_POOL, SCHEDULES
from utils.default_args import SCRAPER_ARGS


RESULT_PATH = "/tmp/fotmob_result_{{ ts_nodash }}_{{ ti.try_number }}.json"
NATIVE_MODES = frozenset({"discover", "daily", "backfill", "replay"})


def validate_data(
    result_path: str = "/tmp/fotmob_result.json",
    **context,
) -> Dict[str, Any]:
    """Fail unless the runner published a complete, direct-only report."""

    import json
    import logging

    logger = logging.getLogger(__name__)
    try:
        with open(result_path, "r", encoding="utf-8") as stream:
            result = json.load(stream)
    except FileNotFoundError as exc:
        raise AirflowException(f"FotMob report not found: {result_path}") from exc
    except json.JSONDecodeError as exc:
        raise AirflowException(
            f"Invalid FotMob report JSON at {result_path}: {exc}"
        ) from exc

    mode = str(result.get("mode") or "")
    if mode not in NATIVE_MODES:
        raise AirflowException(
            f"Unsupported FotMob report mode {mode!r}; native mode is required"
        )
    if mode in NATIVE_MODES:
        operation_failures = []
        for operation in result.get("operations") or []:
            if (
                operation.get("errors")
                or operation.get("retryable")
                or operation.get("terminal")
                or operation.get("status") in {"failed", "retryable"}
            ):
                operation_failures.append(
                    {
                        "entity": operation.get("entity"),
                        "status": operation.get("status"),
                        "errors": operation.get("errors") or [],
                        "retryable": operation.get("retryable") or [],
                        "terminal": operation.get("terminal") or [],
                    }
                )
        transport = result.get("transport") or {}
        budget = result.get("budget") or {}
        violations = []
        required_transport = {
            "attempts",
            "direct_bytes",
            "proxy_bytes",
        }
        required_budget = {
            "requests",
            "max_requests",
            "direct_bytes",
            "max_direct_bytes",
            "proxy_bytes",
            "max_proxy_bytes",
        }
        missing_transport = sorted(required_transport - transport.keys())
        missing_budget = sorted(required_budget - budget.keys())
        if missing_transport:
            violations.append(f"missing transport metrics={missing_transport!r}")
        if missing_budget:
            violations.append(f"missing budget metrics={missing_budget!r}")
        if result.get("status") != "success" or result.get("complete") is not True:
            violations.append(
                f"status={result.get('status')!r}, complete={result.get('complete')!r}"
            )
        if result.get("errors"):
            violations.append(f"runner errors={result['errors']!r}")
        if operation_failures:
            violations.append(f"operation failures={operation_failures!r}")
        if int(transport.get("proxy_bytes") or 0) != 0:
            violations.append(
                f"proxy_bytes={transport.get('proxy_bytes')} (direct-only invariant)"
            )
        if int(budget.get("requests") or 0) > int(budget.get("max_requests") or 0):
            violations.append("request budget exceeded")
        if int(budget.get("direct_bytes") or 0) > int(
            budget.get("max_direct_bytes") or 0
        ):
            violations.append("direct-byte budget exceeded")
        if not result.get("operations"):
            violations.append("no native operations recorded")
        catalog_counts = [
            int((operation.get("counts") or {}).get("competitions") or 0)
            for operation in result.get("operations") or []
            if operation.get("entity") == "competition_catalog"
        ]
        if not catalog_counts or max(catalog_counts) <= 0:
            violations.append("complete competition catalog was not recorded")
        if violations:
            raise AirflowException(
                "Incomplete FotMob native ingest: " + "; ".join(violations)
            )
        summary = {
            "status": "success",
            "run_id": result.get("run_id"),
            "mode": mode,
            "rows": result.get("rows") or {},
            "tables": result.get("tables") or [],
            "transport": transport,
            "budget": budget,
        }
        logger.info("FotMob native validation complete: %s", summary)
        return summary


def _should_transform(mode: str) -> bool:
    """Catalog-only discovery has no season facts for Silver to consume."""

    return str(mode) != "discover"


with DAG(
    dag_id="dag_ingest_fotmob",
    default_args=SCRAPER_ARGS,
    description="Discover and ingest source-native FotMob JSON",
    schedule=SCHEDULES.get("dag_ingest_fotmob"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get("fotmob", ["scraping", "fotmob", "bronze", "http"]),
    max_active_runs=1,
    params={
        "mode": Param(
            default="daily",
            type="string",
            enum=sorted(NATIVE_MODES),
            title="Native run mode",
        ),
        "scope": Param(
            default="",
            type="string",
            title="Exact scopes",
            description="Optional comma-separated FotMob ID=season keys",
        ),
        "entities": Param(
            default="season,leaderboards,matches,teams,players,transfers",
            type="string",
            title="Native entities",
            description=(
                "Season facts are always synchronized; optional enrichments: "
                "leaderboards,matches,teams,players,transfers"
            ),
        ),
        "max_requests": Param(default=2000, type="integer", minimum=1),
        "max_direct_mib": Param(default=256, type="integer", minimum=1),
        "competition_limit": Param(default=0, type="integer", minimum=0),
        "season_limit": Param(default=0, type="integer", minimum=0),
    },
    doc_md="""
    ## FotMob native ingestion

    The runner discovers the complete ``allLeagues`` catalog, classifies every
    competition, preserves exact FotMob season strings and processes a bounded
    plan.  JSON is committed to the durable raw store before typed Bronze rows.
    Defaults are 2,000 requests, 256 MiB direct traffic, 0 proxy bytes, four
    workers and 30 requests/minute.  Use ``scope`` with numeric identities such
    as ``42=2025/2026,47=2025/2026``; names are never storage identities.

    ``discover`` writes catalog/season availability only. ``daily`` refreshes
    selected/latest seasons. ``backfill`` prioritizes required sentinels then
    active/newest and older source seasons. ``replay`` performs no network I/O.
    """,
) as dag:
    scrape_data_task = BashOperator(
        task_id="scrape_fotmob_data",
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_fotmob_scraper.py \\
    --mode "{{{{ params.mode }}}}" \\
    --scope "{{{{ params.scope }}}}" \\
    --entities "{{{{ params.entities }}}}" \\
    --max-requests "{{{{ params.max_requests }}}}" \\
    --max-direct-mib "{{{{ params.max_direct_mib }}}}" \\
    --max-proxy-mib 0 \\
    --competition-limit "{{{{ params.competition_limit }}}}" \\
    --season-limit "{{{{ params.season_limit }}}}" \\
    --requests-per-minute 30 \\
    --workers 4 \\
    --run-id "dag_ingest_fotmob-{{{{ ts_nodash }}}}-{{{{ ti.try_number }}}}" \\
    --output "{RESULT_PATH}"
""",
        env={
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
            "HOME": "/home/airflow",
        },
        append_env=True,
        pool=FOTMOB_HTTP_POOL,
    )

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        op_kwargs={"result_path": RESULT_PATH},
    )

    transform_gate = ShortCircuitOperator(
        task_id="season_data_available",
        python_callable=_should_transform,
        op_kwargs={"mode": "{{ params.mode }}"},
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="dag_transform_fotmob_silver",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    scrape_data_task >> validate_data_task >> transform_gate >> trigger_silver
