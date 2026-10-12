"""TM-only adapter for committing the common passive ops traffic reporter."""
from __future__ import annotations
from scrapers.transfermarkt.writer import CommittingConnection, writer_lock


def record_traffic_run(summary, dag_run_id=''):
    try:
        from utils import proxy_traffic
    except ImportError:
        from dags.utils import proxy_traffic
    connection = None
    try:
        connection = CommittingConnection(proxy_traffic._silver_tasks_module()._get_trino_connection())
        with writer_lock():
            return proxy_traffic.record_traffic_run(summary, dag_run_id=dag_run_id, conn=connection)
    finally:
        if connection is not None:
            connection.close()
