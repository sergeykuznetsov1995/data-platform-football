"""
WhoScored Data Ingestion DAG
============================

Airflow DAG for scraping match event data from WhoScored.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Uses Selenium with Cloudflare bypass for data collection.

Schedules daily at 10 AM UTC.

IMPORTANT: WhoScored uses aggressive Cloudflare protection.
Recommended to run with headless=False for better success rate.

Data collected:
- Match events in SPADL format (passes, shots, tackles, etc.)

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SELENIUM_ARGS


# Extended timeout for WhoScored due to Cloudflare
WHOSCORED_ARGS = {
    **SELENIUM_ARGS,
    'execution_timeout': timedelta(hours=4),
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality.

    Returns:
        Validation results
    """
    import json
    import logging

    logger = logging.getLogger(__name__)

    try:
        with open('/tmp/whoscored_result.json', 'r') as f:
            scrape_result = json.load(f)
    except FileNotFoundError:
        logger.error("Results file not found - scraping may have failed")
        raise AirflowException("Results file not found - scraping failed")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in results: {e}")
        raise AirflowException(f"Invalid JSON in results: {e}")

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'total_events': scrape_result.get('rows', 0),
            'matches_scraped': scrape_result.get('matches_scraped', 0),
            'tables': scrape_result.get('tables', []),
        }
    }

    if scrape_result.get('errors'):
        validation['warnings'] = scrape_result['errors']
        validation['status'] = 'partial_success' if validation['summary']['total_events'] > 0 else 'failed'

    # WhoScored often has issues, so we're lenient with thresholds
    if validation['summary']['matches_scraped'] == 0:
        validation['warnings'].append("No matches scraped - possible Cloudflare blocking")

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_whoscored',
    default_args=WHOSCORED_ARGS,
    description='Ingest match events from WhoScored (SPADL format)',
    schedule=SCHEDULES.get('dag_ingest_whoscored', '0 10 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('whoscored', ['scraping', 'whoscored', 'bronze', 'selenium', 'spadl']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
        'match_urls': [],  # Optionally provide specific URLs
        'headless': False,  # Recommended False for WhoScored
    },
    doc_md="""
    ## WhoScored Data Ingestion

    This DAG scrapes match event data from WhoScored and converts it to SPADL format.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Important Notes

    - **Cloudflare Protection**: WhoScored uses aggressive bot protection.
      Set `headless=False` for better success rate.
    - **Rate Limiting**: Very conservative (10 requests/min) to avoid blocks.
    - **Timeout**: Extended to 4 hours due to potential Cloudflare delays.

    ### Parameters

    - `leagues`: List of leagues to scrape
    - `season`: Season year (e.g., 2024)
    - `match_urls`: Optional list of specific match URLs to scrape
    - `headless`: Whether to run browser in headless mode (default: False)

    ### SPADL Format

    Events are converted to SPADL (Soccer Player Action Description Language) format:
    - Standardized action types (pass, shot, tackle, etc.)
    - Coordinates in meters (105x68 pitch)
    - Result (success/fail/owngoal)
    - Body part used

    ### Notes

    - Uses FlareSolverr for Cloudflare bypass (more reliable than local Selenium)
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_events_task = BashOperator(
        task_id='scrape_match_events',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_whoscored_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {CURRENT_SEASON} \\
    --output /tmp/whoscored_result.json \\
    --use-flaresolverr \\
    --flaresolverr-url http://flaresolverr:8191
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        
        trigger_rule='all_done',
    )

    scrape_events_task >> validate_data_task
