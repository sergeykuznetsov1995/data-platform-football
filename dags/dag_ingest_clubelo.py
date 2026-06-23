"""
ClubElo Data Ingestion DAG
==========================

Airflow DAG for scraping ELO ratings from ClubElo.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 1 PM UTC.

Two modes, selected via the UI-configurable ``mode`` param (no separate
backfill DAG — issue #716 folded the former ``dag_ingest_clubelo_full`` in
here, mirroring the MatchHistory pattern #710):

- ``daily`` (default, scheduled): current ratings only — fast, 1 HTTP call,
  writes ``bronze.clubelo_ratings`` (one partition per rating_date).
- ``full`` (manual "Trigger DAG w/ config"): also scrapes weekly-sampled
  historical ratings into ``bronze.clubelo_ratings_historical`` over the last
  ``days_back`` days. Set days_back≈3650 for the 10-season backfill (#716).

History is written with replace_partitions (NOT append) — daily APPEND is what
caused the 2026-05-04 HDFS overflow (#314).

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.clubelo_tasks import validate_data
from utils.config import LEAGUES, SCHEDULES, DAG_TAGS
from utils.default_args import LIGHT_ARGS

# Build leagues argument for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_clubelo',
    default_args=LIGHT_ARGS,
    description='Ingest ELO ratings from ClubElo',
    schedule=SCHEDULES.get('dag_ingest_clubelo', '0 13 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('clubelo', ['scraping', 'clubelo', 'bronze', 'elo']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        # mode/days_back/force_replace are UI-configurable: the daily scheduled
        # run uses the defaults (mode=daily → current ratings only). To backfill
        # historical ratings use "Trigger DAG w/ config" and set mode=full with
        # days_back (≈3650 = 10 APL seasons, #716). This replaces the former
        # dag_ingest_clubelo_full DAG (project rule: parametrize the ingest DAG,
        # don't spawn a backfill DAG — #710).
        'mode': Param(
            default='daily',
            type='string',
            enum=['daily', 'full'],
            title='Mode',
            description=(
                'daily = current ratings only (scheduled). '
                'full = also backfill weekly-sampled historical ratings.'
            ),
        ),
        'days_back': Param(
            default=365,
            type='integer',
            minimum=7,
            maximum=3700,
            title='History depth (days, mode=full only)',
            description=(
                'How far back to weekly-sample historical ratings in mode=full. '
                '365 = rolling year; ~3650 = 10 APL seasons (#716). '
                'Capped at 3700 (~10 seasons). Ignored in mode=daily.'
            ),
        ),
        'force_replace': Param(
            default=False,
            type='boolean',
            title='Force replace (bypass completeness guard)',
            description=(
                'Bypass the shrink guard on the replace_partitions save. '
                'Set True for a deliberate first backfill (#583/#716).'
            ),
        ),
    },
    doc_md="""
    ## ClubElo Data Ingestion

    This DAG scrapes ELO ratings for football clubs from ClubElo.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### ELO Rating System

    ClubElo uses a chess-like rating system adapted for football:
    - Initial rating: 1500
    - Updated after each match based on expected vs actual result
    - Higher rating = stronger team

    ### Data Collected

    - Club name and country
    - Current ELO rating
    - Rating date

    ### Modes (UI-configurable)

    - **daily** (default, scheduled): current ratings → `clubelo_ratings`.
    - **full** (manual "Trigger DAG w/ config"): also weekly-sampled history
      → `clubelo_ratings_historical` over the last `days_back` days. Set
      `days_back`≈3650 + `force_replace`=true for the 10-season backfill (#716).

    ### Notes

    - Simple, fast scraper (no rate limiting issues)
    - Data is partitioned by rating date
    - History uses replace_partitions, never append (HDFS-overflow guard #314)
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_ratings_task = BashOperator(
        task_id='scrape_current_ratings',
        # --mode/--days-back/--force-replace are rendered at runtime from params
        # (Jinja), so a historical backfill is triggered from the UI ("Trigger
        # DAG w/ config") without a separate DAG (#716, pattern #710). The
        # f-string escapes {{ }} as {{{{ }}}} so the literal Jinja tag survives.
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_clubelo_scraper.py \\
    --leagues "{leagues_str}" \\
    --mode {{{{ params.mode }}}} \\
    --days-back {{{{ params.days_back }}}} \\
    {{% if params.force_replace %}}--force-replace{{% endif %}} \\
    --output /tmp/clubelo_result.json
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
        # 60 min covers a deep mode=full backfill (~520 weekly HTTP fetches for
        # 10 seasons); the daily mode finishes in seconds.
        execution_timeout=timedelta(minutes=60),
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        
        trigger_rule='all_done',
    )

    scrape_ratings_task >> validate_data_task
