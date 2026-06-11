"""
Default Arguments for DAGs
==========================

Standard and specialized default arguments for Airflow tasks.
"""

from datetime import timedelta
from typing import Any, Dict

from utils.alerts import telegram_on_failure

# Standard default arguments for all DAGs
DEFAULT_ARGS: Dict[str, Any] = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'on_failure_callback': telegram_on_failure,
}

# Extended arguments for Selenium-based scrapers
SELENIUM_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=4),
}

# Light arguments for simple scrapers (ClubElo, etc.)
# Reduced timeout to fail fast if OOM occurs
LIGHT_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5),
}

# Weekly scraper arguments (SoFIFA)
WEEKLY_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=3),
}

# Silver layer transform arguments (SQL-only, no browser/scraper)
SILVER_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}
