"""
Default Arguments for DAGs
==========================

Standard and specialized default arguments for Airflow tasks.
"""

from datetime import timedelta
from typing import Any, Dict

from utils.alerts import telegram_on_failure

# Airflow pool that serializes heavy ingest scrapers to a single concurrent
# task platform-wide. Two browser/FlareSolverr scrapers running at once
# exhaust VM swap → scheduler stalls → live tasks get orphan-reset (#671).
# Assigned via the scraper arg-sets below so every ingest scraper task
# (incl. FBref factory tasks) inherits it through default_args.
INGEST_SCRAPER_POOL = 'ingest_scraper_pool'

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

# Scraper variant of DEFAULT_ARGS for ingest DAGs that use the base args
# directly (fotmob, capology, transfermarkt, espn, sofascore, understat).
# Carries the pool WITHOUT touching DEFAULT_ARGS — SILVER_ARGS spreads
# DEFAULT_ARGS, so adding 'pool' there would wrongly serialize transforms.
SCRAPER_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'pool': INGEST_SCRAPER_POOL,
}

# Extended arguments for Selenium-based scrapers
SELENIUM_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=4),
    'pool': INGEST_SCRAPER_POOL,
}

# Light arguments for simple scrapers (ClubElo, etc.)
# Reduced timeout to fail fast if OOM occurs
LIGHT_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5),
    'pool': INGEST_SCRAPER_POOL,
}

# Weekly scraper arguments (SoFIFA)
WEEKLY_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=3),
    'pool': INGEST_SCRAPER_POOL,
}

# Silver layer transform arguments (SQL-only, no browser/scraper)
SILVER_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}
