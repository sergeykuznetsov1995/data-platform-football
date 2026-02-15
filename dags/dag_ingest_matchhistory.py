"""
MatchHistory Data Ingestion DAG
===============================

Airflow DAG for scraping historical match data from football-data.co.uk.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 8 AM UTC (after other scrapers).

Data collected:
- Match results (home/away goals)
- Half-time scores
- Betting odds from multiple bookmakers
- Match statistics (shots, corners, fouls, cards)

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
        with open('/tmp/matchhistory_result.json', 'r') as f:
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
            'total_rows': scrape_result.get('rows', 0),
            'leagues_scraped': len(scrape_result.get('league_details', {})),
            'league_details': scrape_result.get('league_details', {}),
            'tables': scrape_result.get('tables', []),
        }
    }

    # Check for errors
    if scrape_result.get('errors'):
        validation['warnings'] = scrape_result['errors']
        validation['status'] = 'partial_success' if validation['summary']['total_rows'] > 0 else 'failed'

    # Check minimum data thresholds
    if validation['summary']['total_rows'] < 100:
        validation['warnings'].append("Low total row count - possible scraping issue")

    # Check per-league thresholds
    for league, count in validation['summary']['league_details'].items():
        if count < 10:
            validation['warnings'].append(f"Low match count for {league}: {count}")

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


def generate_stats_report(**context) -> Dict[str, Any]:
    """
    Generate statistics report from scraped data.

    Returns:
        Statistics report
    """
    import json
    import logging

    logger = logging.getLogger(__name__)

    try:
        with open('/tmp/matchhistory_result.json', 'r') as f:
            scrape_result = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning("No data to generate report")
        return {'status': 'skipped', 'reason': 'no data'}

    if scrape_result.get('rows', 0) == 0:
        logger.warning("No data to generate report")
        return {'status': 'skipped', 'reason': 'no data'}

    report = {
        'status': 'success',
        'total_matches': scrape_result.get('rows', 0),
        'leagues_count': len(scrape_result.get('league_details', {})),
        'leagues': scrape_result.get('league_details', {}),
        'errors_count': len(scrape_result.get('errors', [])),
    }

    logger.info(f"Report generated: {report['total_matches']} matches across {report['leagues_count']} leagues")

    return report


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_matchhistory',
    default_args=SELENIUM_ARGS,
    description='Ingest historical match data from football-data.co.uk',
    schedule=SCHEDULES.get('dag_ingest_matchhistory', '0 8 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('matchhistory', ['scraping', 'matchhistory', 'bronze', 'football', 'odds']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
    doc_md="""
    ## MatchHistory Data Ingestion

    This DAG scrapes historical match data from football-data.co.uk.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Data Collected

    - **Match Results**: Home/away goals, half-time scores
    - **Betting Odds**: Odds from multiple bookmakers (Bet365, Pinnacle, etc.)
    - **Match Stats**: Shots, corners, fouls, cards

    ### Notes

    - Uses Selenium with xvfb for headless browser operation
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_match_results',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_matchhistory_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {CURRENT_SEASON} \\
    --output /tmp/matchhistory_result.json \\
    --headless \\
    --use-xvfb
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
            'DISPLAY': ':99',
        },
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        
        trigger_rule='all_done',
    )

    generate_report_task = PythonOperator(
        task_id='generate_stats_report',
        python_callable=generate_stats_report,
        
    )

    scrape_data_task >> validate_data_task >> generate_report_task
