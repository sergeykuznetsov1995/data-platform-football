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
    # #951 bronze backfill: match-capture materializes the whole league in one
    # end-of-task pass (many serialized single-row manifest MERGEs). On a lean
    # manifest table each MERGE is ~0.8s, but the pass re-bloats the table within
    # a run, so a big league can exceed the 2h default. Widen to 6h for the
    # isolated backfill stack; the between-run manifest maintenance keeps commits
    # fast so tasks normally finish in well under an hour.
    'execution_timeout': timedelta(hours=6),
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
# retries/retry_delay hardened (#728): heavy CTAS (e.g. whoscored_events_spadl)
# can momentarily crash+restart Trino under memory pressure; a single 2-min
# retry sometimes landed inside the restart window and the whole DAG failed.
# Two retries at 5-min spacing absorb a Trino restart (~30-60s to healthy) so a
# transient blip recovers instead of failing the run. Spill-to-disk
# (configs/trino/config.properties) is the primary fix; this is the safety net.
SILVER_ARGS: Dict[str, Any] = {
    **DEFAULT_ARGS,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}
