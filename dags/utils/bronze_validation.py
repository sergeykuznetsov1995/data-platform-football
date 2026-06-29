"""
Fail-closed Bronze row-count validation shared by ingest DAGs.

Extracted from ``dag_ingest_whoscored.py`` (issues #106/#110) so that
``dag_ingest_espn`` / ``dag_ingest_understat`` / ``dag_ingest_sofifa`` reuse
the same guard (issue #466). Semantics:

* missing ``MIN_ROW_THRESHOLDS`` key -> ``AirflowException`` (no silent-pass)
* Trino unreachable                  -> ``AirflowException`` (infra, not data)
* COUNT(*) failure / missing table   -> ``AirflowException``
* rows < threshold                   -> ``AirflowException``
"""

import logging
from typing import Any, Dict

import requests.exceptions as _req_exc
from airflow.exceptions import AirflowException
from trino.exceptions import TrinoConnectionError

from utils.config import MIN_ROW_THRESHOLDS

logger = logging.getLogger(__name__)


def bronze_count(table_name: str) -> int:
    """Count rows in iceberg.bronze.{table_name} via Trino."""
    from utils.silver_tasks import _get_trino_connection, _validate_identifier

    _validate_identifier(table_name, "table")
    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM iceberg.bronze.{table_name}")
            row = cur.fetchall()
            return int(row[0][0]) if row else 0
        finally:
            cur.close()
    finally:
        conn.close()


def validate_table(table_name: str, threshold_key: str) -> Dict[str, Any]:
    """Run a row-count check against MIN_ROW_THRESHOLDS for one Bronze table."""
    try:
        threshold = MIN_ROW_THRESHOLDS[threshold_key]
    except KeyError as e:
        raise AirflowException(
            f"MIN_ROW_THRESHOLDS missing key '{threshold_key}' — refusing silent-pass. "
            f"Add a threshold in dags/utils/config.py before re-running."
        ) from e

    try:
        rows = bronze_count(table_name)
    except (TrinoConnectionError, _req_exc.ConnectionError) as e:
        # Trino unreachable (container down, DNS not resolving, network) — infra
        # issue, not data. Distinct message helps on-call separate scope from a
        # missing/empty table. Airflow task retries cover the recovery window
        # once `restart: unless-stopped` brings Trino back.
        logger.error(f"Trino unreachable while counting {table_name}: {e}")
        raise AirflowException(
            f"Trino unreachable (infra issue, not data): {e}"
        ) from e
    except Exception as e:
        # If the Bronze table doesn't exist (first run, cancelled subtask), the
        # COUNT(*) raises. Surface as a hard validation failure.
        logger.error(f"COUNT(*) failed for {table_name}: {e}")
        raise AirflowException(
            f"Bronze table iceberg.bronze.{table_name} unavailable: {e}"
        ) from e

    summary = {'table': table_name, 'rows': rows, 'threshold': threshold}
    logger.info(f"Validation: {summary}")

    if rows < threshold:
        raise AirflowException(
            f"{table_name}: {rows} rows < threshold {threshold}"
        )
    return summary
