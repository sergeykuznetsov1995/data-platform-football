"""
FBref Inference Serving DAG  (T4.2)
====================================

Materialises ``iceberg.gold.predictions_input`` — a per-fixture feature
snapshot for upcoming matches in the next 7 days. Keeps online ML serving
on a single, schema-stable read instead of letting the model server craft
its own ad-hoc query against the Gold mart.

Architecture
------------

    ExternalTaskSensor(dag_transform_fbref_gold.validate_gold_quality)
        |  inference must never read features that failed Gold DQ
        v
    materialize_predictions_input  — CTAS gold.predictions_input from fct_match
        |
        v
    validate_predictions_input     — PK uniqueness, no_nulls, date window, freshness
        |
        v
    count_predictions_input        — observability metric (row count, date span)

Schedule
--------

Runs every 2 hours. Cadence is a trade-off:
  * Often enough that the snapshot is at most ~2 h stale for the model.
  * Cheap enough that a single CTAS over the wide mart fits comfortably
    in PREDICTIONS_ARGS.execution_timeout (15 min).

Train / inference parity
------------------------

``predictions_input`` is built from the SAME ``gold.fct_match`` mart that
``fct_match_train`` and ``fct_match_test`` consume — column names and
types match by construction. Rolling features inherit the point-in-time
mask from feat_*-tables (T3.4), so feature distribution at inference is
the same shape the model saw during training.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.default_args import PREDICTIONS_ARGS


# ---------------------------------------------------------------------------
# Task callables (lazy imports — keep DAG parse cheap, no scrapers/ on import)
# ---------------------------------------------------------------------------

def _materialize(**_ctx) -> Dict[str, Any]:
    from utils.gold_tasks import run_gold_transform

    # Partition by season — same convention as the fct_match_*/feat_* tables.
    # add_timestamp=False: SELECT from gold.fct_match already carries
    # _silver_created_at; CTAS wrapper would otherwise duplicate the column.
    return run_gold_transform(
        sql_file='dags/sql/gold/predictions_input.sql',
        table_name='predictions_input',
        partition_columns=['season'],
        add_timestamp=False,
    )


def _validate(**_ctx) -> Dict[str, Any]:
    from utils.gold_tasks import validate_predictions_input
    return validate_predictions_input()


def _count(**_ctx) -> Dict[str, Any]:
    from utils.gold_tasks import count_predictions_input
    return count_predictions_input()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_serve_predictions',
    default_args=PREDICTIONS_ARGS,
    description='Materialise gold.predictions_input snapshot for online ML serving',
    schedule='0 */2 * * *',  # every 2 hours
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=['serve', 'fbref', 'gold', 'inference', 'feature-store'],
    doc_md=__doc__,
) as dag:

    # WHY ExternalTaskSensor: Gold DAG is trigger-only (no schedule), so we
    # cannot key on execution_date. `external_task_id=None` waits for the
    # entire upstream DagRun; `execution_date_fn` returns None which makes
    # the sensor look at the LATEST successful Gold DagRun, not a same-day
    # match — exactly what an irregular trigger-only DAG needs.
    wait_for_gold = ExternalTaskSensor(
        task_id='wait_for_gold_dag',
        external_dag_id='dag_transform_fbref_gold',
        external_task_id='validate_gold_quality',  # last task in Gold DAG
        allowed_states=['success'],
        failed_states=['failed', 'upstream_failed', 'skipped'],
        mode='reschedule',          # release worker slot between polls
        poke_interval=300,           # 5 min
        timeout=60 * 60,             # 1 h cap; if Gold isn't ready by then, surface
        check_existence=True,        # fail fast if upstream DAG is missing
        # Gold has no schedule. Look for a successful run within a wide window
        # before this DAG's logical date — covers the every-2h cadence even
        # when Gold last ran 12+ hours ago (ingestion is daily / weekly).
        execution_date_fn=lambda dt: [dt - timedelta(hours=h) for h in range(0, 25)],
    )

    materialize = PythonOperator(
        task_id='materialize_predictions_input',
        python_callable=_materialize,
    )

    validate = PythonOperator(
        task_id='validate_predictions_input',
        python_callable=_validate,
    )

    count = PythonOperator(
        task_id='count_predictions_input',
        python_callable=_count,
    )

    wait_for_gold >> materialize >> validate >> count
