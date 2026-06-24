"""
Proxy Traffic — Daily Per-Source Report (#789 Phase 2)
======================================================

Reads ``iceberg.ops.proxy_traffic_runs`` for *yesterday* and logs one
per-source rollup line so the residential-proxy spend (``pool.proxys.io``,
~$4/GB) is visible day-by-day:

    PROXY_TRAFFIC_DAILY вчера прокси съели 1.234 GB (1263.6 MB):
        fbref 0.900 GB, transfermarkt 0.300 GB, capology 0.034 GB

The per-run rows are written passively by each ingest run (FBref via
``utils.fbref_callbacks.report_proxy_traffic``; Transfermarkt / Capology /
SofaScore via their run-script ``_write_results`` → ``record_traffic_run``).
This DAG only aggregates — it never scrapes and touches no bronze.

No Telegram / Superset at this stage (#789 acceptance) — log only.

Schedule: 02:00 UTC daily — same slot as the iceberg-maintenance sweep, well
before the 14:00 UTC master pipeline.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.default_args import SILVER_ARGS


def _report_proxy_traffic_daily(**_ctx) -> Dict[str, Any]:
    """Roll up yesterday's residential-proxy spend per source and log it.

    Reuses ``utils.proxy_traffic.daily_rollup`` (SELECT … GROUP BY source) over
    a connection from ``utils.silver_tasks._get_trino_connection``. Read-only.
    """
    import logging

    from utils.proxy_traffic import daily_rollup
    from utils.silver_tasks import _get_trino_connection

    logger = logging.getLogger(__name__)

    conn = _get_trino_connection()
    try:
        rollup = daily_rollup(conn)
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    logger.info("PROXY_TRAFFIC_DAILY %s", rollup['report'])
    return rollup


with DAG(
    dag_id='dag_proxy_traffic_report',
    default_args=SILVER_ARGS,
    description='Daily per-source residential-proxy traffic rollup (#789)',
    schedule='0 2 * * *',
    start_date=datetime(2026, 6, 24),
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'proxy', 'traffic', 'reporting', 'daily'],
    doc_md=__doc__,
) as dag:

    PythonOperator(
        task_id='report_proxy_traffic_daily',
        python_callable=_report_proxy_traffic_daily,
    )
