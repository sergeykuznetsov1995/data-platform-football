"""
Fail-closed Bronze row-count validation shared by ingest DAGs.

Extracted from ``dag_ingest_whoscored.py`` (issues #106/#110) so that
``dag_ingest_espn`` / ``dag_ingest_understat`` / ``dag_ingest_sofifa`` reuse
the same guard (issue #466). Semantics:

* missing ``MIN_ROW_THRESHOLDS`` key -> ``AirflowException`` (no silent-pass)
* Trino unreachable                  -> ``AirflowException`` (infra, not data)
* COUNT(*) failure / missing table   -> ``AirflowException``
* rows < threshold                   -> ``AirflowException``

#920 Phase 2: ``validate_table`` optionally checks per-league floors. With a
``leagues`` scope and a threshold key present in ``PER_LEAGUE_FLOOR_BASES``
(see utils.config), the table is counted per league and each competition is
compared against its OWN floor from competitions.yaml — a league missing from
the table can no longer hide behind the whole-table aggregate (the failure
mode that let the 104-match World Cup pass under APL-sized constants).
"""

import logging
from typing import Any, Callable, Dict, List, Optional

import requests.exceptions as _req_exc
from airflow.exceptions import AirflowException
from trino.exceptions import TrinoConnectionError

from utils.config import (
    MIN_ROW_THRESHOLDS,
    PER_LEAGUE_FLOOR_BASES,
    get_min_row_threshold,
)

logger = logging.getLogger(__name__)


def _guarded_count(fn: Callable[[], Any], table_name: str) -> Any:
    """Run a Trino count callable with the shared infra/data error wrapping."""
    try:
        return fn()
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


def bronze_count_by_league(table_name: str) -> Dict[str, int]:
    """Row counts per league in iceberg.bronze.{table_name} via Trino.

    One GROUP BY round-trip instead of N per-league queries — and no league
    literal ever enters the SQL (league values carry spaces/hyphens that
    could never pass _validate_identifier). A league absent from the result
    simply has no rows.
    """
    from utils.silver_tasks import _get_trino_connection, _validate_identifier

    _validate_identifier(table_name, "table")
    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT league, COUNT(*) FROM iceberg.bronze.{table_name} "
                f"GROUP BY league"
            )
            return {str(r[0]): int(r[1]) for r in cur.fetchall()}
        finally:
            cur.close()
    finally:
        conn.close()


def validate_table(
    table_name: str,
    threshold_key: str,
    leagues: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run a row-count check for one Bronze table.

    Without ``leagues`` (or for a threshold key with no per-league base —
    the whole-table wipe-floors) the historical semantics apply unchanged:
    whole-table COUNT(*) vs MIN_ROW_THRESHOLDS. With ``leagues`` and a key
    in PER_LEAGUE_FLOOR_BASES, every league in the scope is compared against
    its own competitions.yaml-derived floor; ALL shortfalls are reported in
    one failure so a red run shows the full damage, not the first casualty.
    """
    if leagues is None or threshold_key not in PER_LEAGUE_FLOOR_BASES:
        try:
            threshold = MIN_ROW_THRESHOLDS[threshold_key]
        except KeyError as e:
            raise AirflowException(
                f"MIN_ROW_THRESHOLDS missing key '{threshold_key}' — refusing silent-pass. "
                f"Add a threshold in dags/utils/config.py before re-running."
            ) from e

        rows = _guarded_count(lambda: bronze_count(table_name), table_name)

        summary = {'table': table_name, 'rows': rows, 'threshold': threshold}
        logger.info(f"Validation: {summary}")

        if rows < threshold:
            raise AirflowException(
                f"{table_name}: {rows} rows < threshold {threshold}"
            )
        return summary

    # Per-league path (#920 Phase 2). Floor derivation is fail-closed: an
    # unknown league / stub competition / broken YAML raises instead of
    # defaulting to 0 (the #102/#110 silent-pass class).
    from utils.medallion_config import MedallionConfigError

    try:
        floors = {lg: get_min_row_threshold(threshold_key, lg) for lg in leagues}
    except MedallionConfigError as e:
        raise AirflowException(
            f"Cannot derive per-league floor for '{threshold_key}': {e}"
        ) from e

    counts = _guarded_count(
        lambda: bronze_count_by_league(table_name), table_name
    )

    per_league = {
        lg: {'rows': counts.get(lg, 0), 'threshold': floors[lg]}
        for lg in leagues
    }
    summary = {
        'table': table_name,
        'rows': sum(v['rows'] for v in per_league.values()),
        'per_league': per_league,
    }
    logger.info(f"Validation: {summary}")

    failures = [
        f"{lg}: {v['rows']} rows < {v['threshold']}"
        for lg, v in per_league.items()
        if v['rows'] < v['threshold']
    ]
    if failures:
        raise AirflowException(
            f"{table_name}: league floors failed: {'; '.join(failures)}"
        )
    return summary
