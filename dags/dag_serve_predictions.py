"""
FBref Inference Serving DAG  (T4.2 + E6 dual-version)
=====================================================

Materialises ``iceberg.gold.predictions_input`` (v1) and
``iceberg.gold.predictions_input_v2`` (E6) — per-fixture feature snapshots
for upcoming matches in the next 7 days. Keeps online ML serving on a
single, schema-stable read instead of letting the model server craft its
own ad-hoc query against the Gold mart.

Architecture (E6 dual-write)
----------------------------

    ExternalTaskSensor(dag_transform_fbref_gold.validate_gold_quality)
        |   inference must never read features that failed Gold DQ
        v
    +---------------------------+   +------------------------------+
    | materialize_predictions_  |   | materialize_predictions_     |
    | input        (v1)         |   | input_v2     (E6 narrow-fact)|
    +-----------+---------------+   +--------------+---------------+
                |                                  |
                v                                  v
    validate_predictions_input            validate_predictions_input_v2
                |                                  |
                v                                  v
    count_predictions_input               count_predictions_input_v2
                \\________________  __________________/
                                  \\/
                          log_active_version
                          (reads Variable
                          ``predictions_serving_active_version``;
                           consumers read v1 OR v2 explicitly)

Both tables are always materialised. The Airflow Variable
``predictions_serving_active_version`` (default ``'v1'``) is the
human-readable marker of which snapshot is "live" for ML model
consumers. Cutover is a manual:

    airflow variables set predictions_serving_active_version v2

after >=2 weeks of green DQ on v2.

Schedule
--------

Runs every 2 hours. Cadence is a trade-off:
  * Often enough that the snapshot is at most ~2 h stale for the model.
  * Cheap enough that two CTAS over the wide mart fit comfortably in
    PREDICTIONS_ARGS.execution_timeout (15 min).

Train / inference parity
------------------------

``predictions_input`` is built from the SAME ``gold.fct_match`` mart that
``fct_match_train`` and ``fct_match_test`` consume — column names and
types match by construction. ``predictions_input_v2`` adds the E4 narrow
facts and E5 unavailability features; v2 vs train/test parity is enforced
by the schema-parity DQ check inside ``validate_predictions_input_v2``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
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


def _materialize_v2(**_ctx) -> Dict[str, Any]:
    """E6: materialise gold.predictions_input_v2 with narrow-fact features.

    Mirrors :func:`_materialize` but reads the v2 SELECT (E4 cards/subs/odds
    + E5 unavailability features joined onto the same fct_match spine).
    """
    from utils.gold_tasks import run_gold_transform

    return run_gold_transform(
        sql_file='dags/sql/gold/predictions_input_v2.sql',
        table_name='predictions_input_v2',
        partition_columns=['season'],
        add_timestamp=False,
    )


def _validate(**_ctx) -> Dict[str, Any]:
    from utils.gold_tasks import validate_predictions_input
    return validate_predictions_input()


def _validate_v2(**_ctx) -> Dict[str, Any]:
    from utils.gold_tasks import validate_predictions_input_v2
    return validate_predictions_input_v2()


def _count(**_ctx) -> Dict[str, Any]:
    return _count_predictions_table('predictions_input')


def _count_v2(**_ctx) -> Dict[str, Any]:
    return _count_predictions_table('predictions_input_v2')


def _count_predictions_table(table: str) -> Dict[str, Any]:
    """Generic row-count + date-window observability metric for any
    predictions_input* table.

    Lazy-imports trino plumbing so DAG parse stays cheap. Pure metric — no
    assertions; mirrors :func:`utils.gold_tasks.count_predictions_input`
    but parameterised on table name so v1 and v2 share one code path.
    """
    import logging

    from utils.data_quality import _get_conn

    log = logging.getLogger(__name__)

    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT COUNT(*), MIN(date), MAX(date) "
                f"FROM iceberg.gold.{table}"
            )
            row = cur.fetchone() or (0, None, None)
        finally:
            cur.close()
    finally:
        conn.close()

    n, dmin, dmax = row
    log.info(
        f"{table}: {n} upcoming fixture(s) "
        f"(date range: {dmin} .. {dmax})"
    )
    return {'table': table, 'count': n,
            'date_min': str(dmin), 'date_max': str(dmax)}


def _log_active_version(**_ctx) -> str:
    """E6 fan-in sink: log which version is the live one for consumers.

    Reads Airflow Variable ``predictions_serving_active_version`` at
    runtime (NOT module-import time — that would break DagBag parsing on
    fresh environments where the Variable doesn't yet exist). Defaults to
    ``'v1'`` — same behaviour as pre-E6, so an unset Variable is a safe
    no-op.
    """
    import logging

    log = logging.getLogger(__name__)

    active = Variable.get(
        'predictions_serving_active_version',
        default_var='v1',
    )
    log.info(
        f"[E6] Active serving version: {active} "
        f"(both v1 and v2 are materialised; consumers read explicitly)."
    )
    return active


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_serve_predictions',
    default_args=PREDICTIONS_ARGS,
    description=(
        'Materialise gold.predictions_input (v1) + predictions_input_v2 '
        'snapshots for online ML serving (E6 dual-write)'
    ),
    schedule='0 */2 * * *',  # every 2 hours
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=['serve', 'fbref', 'gold', 'inference', 'feature-store', 'e6'],
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

    # --- v1 branch (legacy, current production serving target) -------------
    materialize_v1 = PythonOperator(
        task_id='materialize_predictions_input',  # PRESERVED — downstream sensors / log filters key on this id
        python_callable=_materialize,
    )

    validate_v1 = PythonOperator(
        task_id='validate_predictions_input',
        python_callable=_validate,
    )

    count_v1 = PythonOperator(
        task_id='count_predictions_input',
        python_callable=_count,
    )

    # --- v2 branch (E6 narrow-fact + unavailability features) --------------
    materialize_v2 = PythonOperator(
        task_id='materialize_predictions_input_v2',
        python_callable=_materialize_v2,
    )

    validate_v2 = PythonOperator(
        task_id='validate_predictions_input_v2',
        python_callable=_validate_v2,
    )

    count_v2 = PythonOperator(
        task_id='count_predictions_input_v2',
        python_callable=_count_v2,
    )

    # --- fan-in sink: log which version consumers should read --------------
    log_active = PythonOperator(
        task_id='log_active_version',
        python_callable=_log_active_version,
    )

    # Wiring: parallel v1 / v2 materialisation, fan-in at log_active.
    wait_for_gold >> [materialize_v1, materialize_v2]
    materialize_v1 >> validate_v1 >> count_v1 >> log_active
    materialize_v2 >> validate_v2 >> count_v2 >> log_active
