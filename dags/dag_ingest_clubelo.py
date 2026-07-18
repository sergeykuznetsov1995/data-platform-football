"""
ClubElo Data Ingestion DAG
==========================

Airflow DAG for scraping ELO ratings from ClubElo.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 1 PM UTC.

One source = one DAG (#716): the former weekly ``dag_ingest_clubelo_full`` is
folded in here as a gated branch (pattern #710 MatchHistory / #782 SofaScore —
parametrize the ingest DAG instead of spawning a second one).

Data collected:
- Current ELO ratings for all clubs (daily)
- Historical ELO ratings — weekly-sampled snapshots (weekly, gated; see below)

The historical scrape is full-state and heavy: the recurring weekly refresh
samples the last 365 days, and a one-time #716 backfill spans ~10 APL seasons
(~520 weekly snapshots). A ``ShortCircuitOperator`` gates it so it runs only on
the DAG's OWN Sunday scheduled run, or on a manual "Trigger DAG w/ config" with
``run_full=True``. It is skipped on weekday scheduled runs and whenever an
external trigger (e.g. ``dag_master_pipeline``) fires this DAG, so the daily
pipeline never waits on the heavy historical scrape.

It writes with ``replace_partitions=['rating_date']`` so re-runs stay idempotent
and do not accumulate Iceberg metadata — APPEND is what caused the 2026-05-04
HDFS overflow (#314). NEVER APPEND.

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from utils.clubelo_tasks import gate_full_ratings, validate_data
from utils.config import LEAGUES, SCHEDULES, DAG_TAGS
from utils.default_args import LIGHT_ARGS

# Leagues for the daily (current-ratings) bash command, fixed at parse time.
# The gated historical task renders leagues from params at runtime instead, so a
# manual trigger can override them (see its Jinja bash_command below).
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_clubelo',
    default_args=LIGHT_ARGS,
    description='Ingest ELO ratings from ClubElo (current daily + historical weekly)',
    schedule=SCHEDULES.get('dag_ingest_clubelo', '0 13 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('clubelo', ['scraping', 'clubelo', 'bronze', 'elo']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        # #716: force the heavy historical scrape in this run. Normally it
        # auto-runs only on the DAG's own Sunday scheduled run (see
        # gate_full_ratings); set True via "Trigger DAG w/ config" for an
        # on-demand refresh / deep backfill. Ignored on external/master triggers.
        'run_full': False,
        # Backfill depth for the historical scrape (days, weekly-sampled).
        # 365 = the recurring weekly refresh; ~3650 ≈ 10 APL seasons for the
        # one-time #716 backfill. Trigger w/ config:
        #   {"run_full": true, "days_back": 3650, "force_replace": true}
        'days_back': 365,
        # Bypass the completeness guard — for the deliberate first backfill, where
        # the new historical depth legitimately rewrites the partition set.
        'force_replace': False,
    },
    doc_md="""
    ## ClubElo Data Ingestion

    Scrapes ELO ratings for football clubs from ClubElo. ELO is a chess-like
    rating: start 1500, updated after each match, higher = stronger.

    ### Architecture

    Uses BashOperator to run the scraper in an isolated subprocess, preventing
    LocalExecutor fork memory issues.

    ### One source = one DAG (#716)

    The former weekly `dag_ingest_clubelo_full` is folded in here. The heavy
    historical scrape is gated by a `ShortCircuitOperator`:

    - auto-runs only on the DAG's **own Sunday scheduled run** (weekly cadence);
    - or on demand via **"Trigger DAG w/ config"** with `run_full=true`;
    - **skipped** on weekday scheduled runs and whenever `dag_master_pipeline`
      triggers this DAG (so the daily pipeline never waits on the heavy run).

    ### One-time deep backfill (#716)

    Trigger w/ config:

        {"run_full": true, "days_back": 3650, "force_replace": true}

    `days_back=3650` ≈ 10 APL seasons (2016/17→). `force_replace` bypasses the
    completeness guard for the deliberate first backfill. Written with
    `replace_partitions=['rating_date']` — NEVER APPEND (2026-05-04 overflow).

    ### Data Collected

    - Club name, country, current ELO rating, rank, rating date (daily)
    - Weekly-sampled historical ELO snapshots (gated)

    ### Notes

    - Data is partitioned by `rating_date` (date-only ISO).
    - Written to Parquet fallback (PyIceberg disabled for stability).
    """,
) as dag:

    scrape_ratings_task = BashOperator(
        task_id='scrape_current_ratings',
        bash_command=f"""
cd /opt/airflow && \
/opt/legacy-scraper-venv/bin/python dags/scripts/run_clubelo_scraper.py \
    --leagues "{leagues_str}" \
    --output /tmp/clubelo_result.json
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    # ---- Historical scrape (#716) — gated to Sunday / manual ----------------
    # Folded from the former weekly dag_ingest_clubelo_full. The gate
    # short-circuits the historical scrape to `skipped` on weekday runs and on
    # external/master triggers (keeps the daily pipeline fast).
    gate_full_ratings_task = ShortCircuitOperator(
        task_id='gate_full_ratings',
        python_callable=gate_full_ratings,
        trigger_rule='all_done',
    )

    # Jinja (not an f-string) so days_back / force_replace / leagues render from
    # params at runtime — a manual "Trigger DAG w/ config" overrides them.
    scrape_full_ratings_task = BashOperator(
        task_id='scrape_full_ratings',
        bash_command="""
cd /opt/airflow && \
rm -f /tmp/clubelo_full_result.json && \
/opt/legacy-scraper-venv/bin/python dags/scripts/run_clubelo_scraper.py \
    --leagues "{{ params.leagues | join(',') }}" \
    --mode full \
    --days-back {{ params.days_back }} \
    {{ '--force-replace' if params.force_replace else '' }} \
    --output /tmp/clubelo_full_result.json
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=90),
    )

    # Daily chain: current ratings → validate.
    scrape_ratings_task >> validate_data_task
    # Weekly/manual gated branch: skipped on weekday runs and master triggers.
    scrape_ratings_task >> gate_full_ratings_task >> scrape_full_ratings_task
