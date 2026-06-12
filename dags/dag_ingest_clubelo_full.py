"""
ClubElo Full Ingestion DAG (weekly)
===================================

Weekly companion to ``dag_ingest_clubelo`` (daily). Runs the same runner with
``--mode full`` to materialize the two HEAVY ClubElo bronze tables that the
daily DAG deliberately skips:

- ``clubelo_ratings_historical`` — weekly-sampled snapshots over the last 365
  days, written with ``replace_partitions=['rating_date']``.
- ``clubelo_team_history``       — per-team full ELO history, batched single
  save with ``replace_partitions=['team']``.

Why weekly + replace_partitions: these tables are full-state/historical. Daily
APPEND-mode ingest is exactly what caused the 2026-05-04 HDFS overflow
(clubelo_team_history hit 23 GB of Iceberg metadata). Replace semantics keep one
snapshot per partition; weekly cadence keeps the ~150 HTTP calls off the daily
path. The light daily DAG (current ratings only) stays unchanged.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.clubelo_tasks import validate_data
from utils.config import LEAGUES, SCHEDULES, DAG_TAGS
from utils.default_args import LIGHT_ARGS

leagues_str = ','.join(LEAGUES)

with DAG(
    dag_id='dag_ingest_clubelo_full',
    default_args=LIGHT_ARGS,
    description='Weekly ingest of ClubElo historical + team-history bronze tables',
    schedule=SCHEDULES.get('dag_ingest_clubelo_full', '0 4 * * 0'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('clubelo', ['scraping', 'clubelo', 'bronze', 'elo']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
    },
    doc_md="""
    ## ClubElo Full Ingestion (weekly)

    Materializes the two heavy ClubElo bronze tables (historical ratings +
    per-team ELO history) via the shared runner in `--mode full`. Both use
    `replace_partitions` so re-runs stay idempotent and do not accumulate
    Iceberg metadata (root cause of the 2026-05-04 HDFS overflow).

    The daily `dag_ingest_clubelo` continues to ingest current ratings only.
    """,
) as dag:

    scrape_full_task = BashOperator(
        task_id='scrape_full',
        bash_command=f"""
cd /opt/airflow && \
python dags/scripts/run_clubelo_scraper.py \
    --leagues "{leagues_str}" \
    --mode full \
    --output /tmp/clubelo_result.json
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=60),
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    scrape_full_task >> validate_data_task
