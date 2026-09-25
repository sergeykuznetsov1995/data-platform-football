"""
ClubElo Data Ingestion DAG
==========================

The ClubElo API is dead (#1459): the collection reads the HTML pages of
clubelo.com through ``dags/scripts/run_clubelo_scraper.py`` in the isolated
legacy runner (BashOperator, no LocalExecutor fork).

- Daily (#1463), twice a day: ``/Ranking`` + ``/Results`` →
  ``bronze.clubelo_rank_snapshot`` (the ``rating_date`` partition replaced in
  one transaction — NEVER APPEND, #314) and ``bronze.clubelo_result`` (MERGE by
  match date + both club keys). Fail-closed: a changed layout, a small
  snapshot or /Results of another rating date writes nothing parsed and the
  task goes red.
- History (#1462), manual only: club pages ``/{slug}``.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from utils.clubelo_tasks import (
    gate_daily,
    gate_history,
    validate_data,
)
from utils.config import DAG_TAGS
from utils.default_args import LIGHT_ARGS

# One result file per DAG run (run_id in the path): a file of another run is
# never read, a stale one of this run is rejected by fetched_at (R-37).
DAILY_RESULT = "/tmp/clubelo_daily_{{ run_id | replace(':', '_') }}.json"

# DAG definition
with DAG(
    dag_id='dag_ingest_clubelo',
    default_args=LIGHT_ARGS,
    description='ClubElo HTML: daily /Ranking + /Results snapshot, manual club-page history',
    # The site rebuilds the rating at ~08:50 UTC (its date lags ~2 days);
    # two runs a day until task 5 of the epic measures the rebuild time.
    schedule='30 9,21 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('clubelo', ['scraping', 'clubelo', 'bronze', 'elo']),
    max_active_runs=1,
    params={
        # #1462: collect the history from the club pages /{slug} of
        # clubelo.com (resumable batches into four append-only tables).
        # Manual only — "Trigger DAG w/ config" {"run_history": true}; the
        # daily chain is skipped in that run.
        'run_history': False,
    },
    doc_md="""
    ## ClubElo Data Ingestion (HTML of clubelo.com)

    The ClubElo API is closed; everything is read from the site pages with one
    polite transport (1 request/s, gzip, white list of paths, a block by the
    site stops the run). Every page is stored raw (gzip) in
    `bronze.clubelo_raw_page` before it is parsed.

    ### Daily snapshot (#1463) — `gate_daily >> scrape_daily >> validate_data`

    Runs at 09:30 and 21:30 UTC (the site rebuilds at ~08:50 UTC).

    1. `/Ranking` → `bronze.clubelo_rank_snapshot`: ~1741 clubs + provisional
       clubs of the country tables, partition `rating_date` (the page's own
       date), replaced in one transaction — never APPEND.
    2. `/Results` → `bronze.clubelo_result`: MERGE by
       (`match_date`, `home_key`, `away_key`); a later fetch updates the score
       and `is_final`.
    3. On a new rating date: up to 10 clubs newly linked from `/Ranking` go to
       the history tables.

    Red and nothing parsed written when: the page layout changed (h1 date,
    "Page created", eloData, vegaJson, country tables, a cell), fewer than 1500
    clubs, levels matched < 97 %, fewer than 95 % of 1741 clubs or of the
    previous rating date, `/Results` of another rating date after 3 retries
    10 min apart. The alert names the failed check. `validate_data` reads only
    this run's result file and rejects a file older than the run.

    ### Club-page history (#1462)

    Manual only: "Trigger DAG w/ config" `{"run_history": true}`. The
    `gate_history` branch is a root of its own; scheduled runs and
    `dag_master_pipeline` triggers keep it skipped. In that run `gate_daily`
    skips the daily chain. Batches of 200 clubs are resumable: a new run
    continues with the clubs not yet closed in
    `bronze.clubelo_history_manifest`. Any failed page, redirect, block or
    pending club makes the task red.
    """,
) as dag:

    # #1462: a manual history run skips the daily chain. Default
    # ignore_downstream_trigger_rules=True skips every downstream task.
    gate_daily_task = ShortCircuitOperator(
        task_id='gate_daily',
        python_callable=gate_daily,
        pool='default_pool',
    )

    scrape_daily_task = BashOperator(
        task_id='scrape_daily',
        bash_command=f"""
cd /opt/airflow && \
rm -f {DAILY_RESULT} && \
/opt/legacy-scraper-venv/bin/python dags/scripts/run_clubelo_scraper.py \
    --mode daily \
    --output {DAILY_RESULT}
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
        # /Results may be retried 3 x 10 min (M-09) plus up to 10 club pages.
        execution_timeout=timedelta(minutes=45),
        pool='default_pool',
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        op_kwargs={'results_path': DAILY_RESULT},
        pool='default_pool',
    )

    # ---- Club-page history (#1462) — manual run_history=True only ----------
    # A root of its own: not after the daily chain, no all_done, no calendar.
    # ignore_downstream_trigger_rules=False: skipping touches only the
    # direct downstream scrape_history.
    gate_history_task = ShortCircuitOperator(
        task_id='gate_history',
        python_callable=gate_history,
        ignore_downstream_trigger_rules=False,
        pool='default_pool',
    )

    scrape_history_task = BashOperator(
        task_id='scrape_history',
        bash_command="""
cd /opt/airflow && \
rm -f /tmp/clubelo_history_result.json && \
/opt/legacy-scraper-venv/bin/python dags/scripts/run_clubelo_scraper.py \
    --mode history \
    --batch-size 200 \
    --output /tmp/clubelo_history_result.json
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
        # Overrides LIGHT_ARGS' 5 min (R-57): ~500 pages at 1 req/s.
        execution_timeout=timedelta(minutes=30),
        # No Airflow retry (LIGHT_ARGS has one): a block (403/429) must stop
        # the run, and a red partial run must stay red; the next manual run
        # resumes from the manifest (Sol r1 #3).
        retries=0,
        pool='default_pool',
    )

    # Daily chain: snapshot → validate.
    gate_daily_task >> scrape_daily_task >> validate_data_task
    # Manual history branch (#1462).
    gate_history_task >> scrape_history_task
