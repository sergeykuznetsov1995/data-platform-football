"""Trigger-only orchestration for native ESPN v2 Silver transforms."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS
from utils import espn_silver_dq

SILVER_MIN_ROWS = espn_silver_dq.SILVER_MIN_ROWS


SILVER_TRANSFORMS = [
    ("match", "dags/sql/silver/espn_match.sql", "espn_match"),
    ("team_match", "dags/sql/silver/espn_team_match.sql", "espn_team_match"),
    ("player_match_aggregate", "dags/sql/silver/espn_player_match_aggregate.sql", "espn_player_match_aggregate"),
    ("match_events", "dags/sql/silver/espn_match_events.sql", "espn_match_events"),
    ("substitutions", "dags/sql/silver/espn_substitutions.sql", "espn_substitutions"),
    ("venue", "dags/sql/silver/espn_venue.sql", "espn_venue"),
]


def _require_compact6_mode() -> str:
    """Fence every runtime entry point to the post-cutover serving layout."""
    from scrapers.espn.layout import COMPACT6, LayoutError, require_layout_mode

    mode = require_layout_mode()
    if mode != COMPACT6:
        raise LayoutError(
            "ESPN Silver transforms require ESPN_BRONZE_LAYOUT_MODE=compact6"
        )
    return mode


def _validate_source_layout(**context) -> Dict[str, Any]:
    """Prove the exact public/internal compact6 inventory before any CTAS."""
    mode = _require_compact6_mode()

    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.espn.layout import catalog_inventory_sql, validate_catalog_layout

    with TrinoTableManager() as manager:
        rows = manager.execute_query(
            catalog_inventory_sql(catalog="iceberg"), params=()
        )
    return validate_catalog_layout(mode, rows or [])


def _run_transform(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Materialize one ESPN Silver table through the shared atomic runner."""
    # Repeat the observed-catalog proof here: Airflow can clear/retry one
    # transform without rerunning its already-successful upstream preflight.
    _validate_source_layout()

    from utils.silver_tasks import run_silver_transform

    return run_silver_transform(sql_file=sql_file, table_name=table_name, schema="silver")


def _validate_silver(**context) -> Dict[str, Any]:
    """Fail on a minimum-row regression before cross-table checks run."""
    _validate_source_layout()

    from airflow.exceptions import AirflowException
    from utils.silver_tasks import validate_silver_tables

    validation = validate_silver_tables(tables=SILVER_MIN_ROWS, min_rows=0)
    if validation["warnings"]:
        raise AirflowException(f"ESPN Silver validation failed: {validation['warnings']}")
    return validation


def _validate_silver_quality(**context) -> Dict[str, Any]:
    """Merge table-local and cross-table DQ into one alert/report surface."""
    _validate_source_layout()

    from airflow.exceptions import AirflowException
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import run_checks

    report = run_checks(espn_silver_dq.build_espn_silver_checks(), raise_on_error=False)
    report.results.extend(espn_silver_dq.run_espn_silver_custom_checks())
    logging.getLogger(__name__).info("ESPN Silver DQ: %s", report.summary())
    telegram_dq_summary(report, header="ESPN Silver DQ")
    if report.errors:
        raise AirflowException(
            f"ESPN Silver DQ failed: {len(report.errors)} error(s). "
            + "; ".join(f"{item.name}: {item.details or item.error}" for item in report.errors[:5])
        )
    return {
        "passed": len(report.passed), "total": len(report.results),
        "errors": [item.name for item in report.errors],
        "warnings": [item.name for item in report.warnings],
    }


with DAG(
    dag_id="dag_transform_espn_silver",
    default_args=SILVER_ARGS,
    description="Transform native ESPN v2 Bronze tables into Silver Iceberg tables",
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["transform", "espn", "silver", "football", "trino"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    validate_source_layout = PythonOperator(
        task_id="validate_source_layout", python_callable=_validate_source_layout
    )

    with TaskGroup(group_id="silver_transforms") as transforms_group:
        for task_id, sql_file, table_name in SILVER_TRANSFORMS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={"sql_file": sql_file, "table_name": table_name},
            )

    validate_silver = PythonOperator(task_id="validate_silver", python_callable=_validate_silver)
    validate_quality = PythonOperator(
        task_id="validate_silver_quality", python_callable=_validate_silver_quality
    )
    validate_source_layout >> transforms_group >> validate_silver >> validate_quality
