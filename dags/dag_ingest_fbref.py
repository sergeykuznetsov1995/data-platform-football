"""
FBref Data Ingestion DAG
========================

Airflow DAG for scraping football statistics from FBref.
Uses Selenium with Cloudflare bypass for data collection.

Schedules daily at 6 AM UTC.

Data collected:
- Match schedules and results
- Team season statistics
- Player season statistics

All data is written to Iceberg Bronze layer tables.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for all tasks
default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Configuration
LEAGUES = [
    'ENG-Premier League',
    'ESP-La Liga',
    'GER-Bundesliga',
    'ITA-Serie A',
    'FRA-Ligue 1',
]

# Current season (adjust as needed)
CURRENT_SEASON = 2024


def scrape_schedule(**context) -> Dict[str, Any]:
    """
    Scrape match schedules from FBref.

    Returns:
        Dictionary with scraping results
    """
    import logging
    from scrapers.fbref_selenium_scraper import FBrefSeleniumScraper

    logger = logging.getLogger(__name__)

    leagues = context.get('params', {}).get('leagues', LEAGUES)
    season = context.get('params', {}).get('season', CURRENT_SEASON)

    logger.info(f"Starting FBref schedule scrape: leagues={leagues}, season={season}")

    results = {'tables': [], 'rows': 0, 'errors': []}

    try:
        with FBrefSeleniumScraper(
            leagues=leagues,
            seasons=[season],
            headless=True,
        ) as scraper:
            for league in leagues:
                try:
                    df = scraper.read_schedule(league, season)
                    if df is not None and not df.empty:
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='fbref_schedule',
                            partition_cols=['league', 'season'],
                        )
                        results['tables'].append(table_path)
                        results['rows'] += len(df)
                        logger.info(f"Saved {len(df)} rows for {league}")
                except Exception as e:
                    error_msg = f"Error scraping schedule for {league}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

    except Exception as e:
        logger.error(f"Failed to initialize scraper: {e}")
        results['errors'].append(str(e))
        raise

    logger.info(f"Schedule scrape complete: {results['rows']} total rows")
    return results


def scrape_team_stats(**context) -> Dict[str, Any]:
    """
    Scrape team statistics from FBref.

    Returns:
        Dictionary with scraping results
    """
    import logging
    from scrapers.fbref_selenium_scraper import FBrefSeleniumScraper

    logger = logging.getLogger(__name__)

    leagues = context.get('params', {}).get('leagues', LEAGUES)
    season = context.get('params', {}).get('season', CURRENT_SEASON)

    logger.info(f"Starting FBref team stats scrape: leagues={leagues}, season={season}")

    results = {'tables': [], 'rows': 0, 'errors': []}

    try:
        with FBrefSeleniumScraper(
            leagues=leagues,
            seasons=[season],
            headless=True,
        ) as scraper:
            for league in leagues:
                try:
                    df = scraper.read_team_season_stats('stats', league, season)
                    if df is not None and not df.empty:
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='fbref_team_stats',
                            partition_cols=['league', 'season'],
                        )
                        results['tables'].append(table_path)
                        results['rows'] += len(df)
                        logger.info(f"Saved {len(df)} team stats for {league}")
                except Exception as e:
                    error_msg = f"Error scraping team stats for {league}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

    except Exception as e:
        logger.error(f"Failed to initialize scraper: {e}")
        results['errors'].append(str(e))
        raise

    logger.info(f"Team stats scrape complete: {results['rows']} total rows")
    return results


def scrape_player_stats(**context) -> Dict[str, Any]:
    """
    Scrape player statistics from FBref.

    Returns:
        Dictionary with scraping results
    """
    import logging
    from scrapers.fbref_selenium_scraper import FBrefSeleniumScraper

    logger = logging.getLogger(__name__)

    leagues = context.get('params', {}).get('leagues', LEAGUES)
    season = context.get('params', {}).get('season', CURRENT_SEASON)

    logger.info(f"Starting FBref player stats scrape: leagues={leagues}, season={season}")

    results = {'tables': [], 'rows': 0, 'errors': []}

    try:
        with FBrefSeleniumScraper(
            leagues=leagues,
            seasons=[season],
            headless=True,
        ) as scraper:
            for league in leagues:
                try:
                    df = scraper.read_player_season_stats('stats', league, season)
                    if df is not None and not df.empty:
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='fbref_player_stats',
                            partition_cols=['league', 'season'],
                        )
                        results['tables'].append(table_path)
                        results['rows'] += len(df)
                        logger.info(f"Saved {len(df)} player stats for {league}")
                except Exception as e:
                    error_msg = f"Error scraping player stats for {league}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

    except Exception as e:
        logger.error(f"Failed to initialize scraper: {e}")
        results['errors'].append(str(e))
        raise

    logger.info(f"Player stats scrape complete: {results['rows']} total rows")
    return results


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality.

    Returns:
        Validation results
    """
    import logging
    logger = logging.getLogger(__name__)

    # Get results from upstream tasks
    ti = context['ti']
    schedule_result = ti.xcom_pull(task_ids='scrape_schedule')
    team_result = ti.xcom_pull(task_ids='scrape_team_stats')
    player_result = ti.xcom_pull(task_ids='scrape_player_stats')

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'schedule_rows': schedule_result.get('rows', 0) if schedule_result else 0,
            'team_stats_rows': team_result.get('rows', 0) if team_result else 0,
            'player_stats_rows': player_result.get('rows', 0) if player_result else 0,
        }
    }

    # Check for errors
    all_errors = []
    if schedule_result and schedule_result.get('errors'):
        all_errors.extend(schedule_result['errors'])
    if team_result and team_result.get('errors'):
        all_errors.extend(team_result['errors'])
    if player_result and player_result.get('errors'):
        all_errors.extend(player_result['errors'])

    if all_errors:
        validation['warnings'] = all_errors
        validation['status'] = 'partial_success' if validation['summary']['schedule_rows'] > 0 else 'failed'

    # Check minimum data thresholds
    if validation['summary']['schedule_rows'] < 100:
        validation['warnings'].append("Low schedule row count - possible scraping issue")

    if validation['summary']['player_stats_rows'] < 100:
        validation['warnings'].append("Low player stats row count - possible scraping issue")

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    return validation


# DAG definition
with DAG(
    dag_id='dag_ingest_fbref',
    default_args=default_args,
    description='Ingest football statistics from FBref using Selenium',
    schedule_interval='0 6 * * *',  # Daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'fbref', 'bronze', 'football'],
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
) as dag:

    # Task: Scrape schedules
    scrape_schedule_task = PythonOperator(
        task_id='scrape_schedule',
        python_callable=scrape_schedule,
        provide_context=True,
    )

    # Task: Scrape team stats
    scrape_team_stats_task = PythonOperator(
        task_id='scrape_team_stats',
        python_callable=scrape_team_stats,
        provide_context=True,
    )

    # Task: Scrape player stats
    scrape_player_stats_task = PythonOperator(
        task_id='scrape_player_stats',
        python_callable=scrape_player_stats,
        provide_context=True,
    )

    # Task: Validate data
    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
        trigger_rule='all_done',  # Run even if upstream fails
    )

    # Dependencies: sequential due to rate limiting
    scrape_schedule_task >> scrape_team_stats_task >> scrape_player_stats_task >> validate_data_task
