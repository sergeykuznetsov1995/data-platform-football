"""
MatchHistory Data Ingestion DAG
===============================

Airflow DAG for scraping historical match data from football-data.co.uk.
Uses direct HTTP requests with Selenium fallback.

Schedules daily at 8 AM UTC (after other scrapers).

Data collected:
- Match results (home/away goals)
- Half-time scores
- Betting odds from multiple bookmakers
- Match statistics (shots, corners, fouls, cards)

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
    'execution_timeout': timedelta(hours=1),
}

# Configuration
LEAGUES = [
    'ENG-Premier League',
    'ENG-Championship',
    'ESP-La Liga',
    'GER-Bundesliga',
    'ITA-Serie A',
    'FRA-Ligue 1',
]

# Current season (adjust as needed)
CURRENT_SEASON = 2024


def scrape_match_results(**context) -> Dict[str, Any]:
    """
    Scrape match results from football-data.co.uk.

    Returns:
        Dictionary with scraping results
    """
    import logging
    from scrapers.matchhistory_direct_scraper import MatchHistoryDirectScraper

    logger = logging.getLogger(__name__)

    leagues = context.get('params', {}).get('leagues', LEAGUES)
    season = context.get('params', {}).get('season', CURRENT_SEASON)

    logger.info(f"Starting MatchHistory scrape: leagues={leagues}, season={season}")

    results = {'tables': [], 'rows': 0, 'errors': [], 'league_details': {}}

    try:
        with MatchHistoryDirectScraper(
            leagues=leagues,
            seasons=[season],
            headless=True,
            use_xvfb=True,
        ) as scraper:
            all_matches = []

            for league in leagues:
                try:
                    df = scraper.read_games(league, season)
                    if df is not None and not df.empty:
                        # Calculate odds statistics
                        df = scraper.calculate_odds_stats(df)
                        all_matches.append(df)
                        results['league_details'][league] = len(df)
                        results['rows'] += len(df)
                        logger.info(f"Fetched {len(df)} matches for {league}")
                    else:
                        results['errors'].append(f"No data for {league}")
                except Exception as e:
                    error_msg = f"Error scraping {league}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

            # Save combined results
            if all_matches:
                import pandas as pd
                combined_df = pd.concat(all_matches, ignore_index=True)
                table_path = scraper.save_to_iceberg(
                    df=combined_df,
                    table_name='matchhistory_results',
                    partition_cols=['league', 'season'],
                )
                results['tables'].append(table_path)
                logger.info(f"Saved {len(combined_df)} total rows")

    except Exception as e:
        logger.error(f"Failed to initialize scraper: {e}")
        results['errors'].append(str(e))
        raise

    logger.info(f"MatchHistory scrape complete: {results['rows']} total rows")
    return results


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality.

    Returns:
        Validation results
    """
    import logging
    logger = logging.getLogger(__name__)

    # Get results from upstream task
    ti = context['ti']
    scrape_result = ti.xcom_pull(task_ids='scrape_match_results')

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'total_rows': scrape_result.get('rows', 0) if scrape_result else 0,
            'leagues_scraped': len(scrape_result.get('league_details', {})) if scrape_result else 0,
            'league_details': scrape_result.get('league_details', {}) if scrape_result else {},
        }
    }

    # Check for errors
    if scrape_result and scrape_result.get('errors'):
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

    return validation


def generate_stats_report(**context) -> Dict[str, Any]:
    """
    Generate statistics report from scraped data.

    Returns:
        Statistics report
    """
    import logging
    logger = logging.getLogger(__name__)

    ti = context['ti']
    scrape_result = ti.xcom_pull(task_ids='scrape_match_results')

    if not scrape_result or scrape_result.get('rows', 0) == 0:
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


# DAG definition
with DAG(
    dag_id='dag_ingest_matchhistory',
    default_args=default_args,
    description='Ingest historical match data from football-data.co.uk',
    schedule_interval='0 8 * * *',  # Daily at 8 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'matchhistory', 'bronze', 'football', 'odds'],
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
) as dag:

    # Task: Scrape match results
    scrape_match_results_task = PythonOperator(
        task_id='scrape_match_results',
        python_callable=scrape_match_results,
        provide_context=True,
    )

    # Task: Validate data
    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
        trigger_rule='all_done',  # Run even if upstream fails
    )

    # Task: Generate stats report
    generate_report_task = PythonOperator(
        task_id='generate_stats_report',
        python_callable=generate_stats_report,
        provide_context=True,
    )

    # Dependencies
    scrape_match_results_task >> validate_data_task >> generate_report_task
