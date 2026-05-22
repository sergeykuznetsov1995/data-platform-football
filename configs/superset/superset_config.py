# =============================================================================
# Superset Configuration
# =============================================================================
# Mounted at /app/pythonpath/superset_config.py inside the container.
# Loaded automatically by Superset because /app/pythonpath is on PYTHONPATH.
# Reference: https://github.com/apache/superset/blob/master/docker/pythonpath_dev/superset_config.py
# =============================================================================

import os
from typing import Any
from cachelib.redis import RedisCache
from celery.schedules import crontab  # noqa: F401  (kept for users who add scheduled jobs)


# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# DATABASE_URL is provided by docker-compose: postgresql+psycopg2://superset:...@postgres:5432/superset
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://superset:superset@postgres:5432/superset",
)

# Behind a reverse proxy / TLS terminator we will enable Talisman later.
TALISMAN_ENABLED = False

WTF_CSRF_ENABLED = True
# CSRF exemption for SQL Lab and dashboard JSON endpoints handled by Superset defaults.

# Row limits to keep dashboards responsive on a single-node setup.
ROW_LIMIT = 50000
SQL_MAX_ROW = 100000
SUPERSET_WEBSERVER_TIMEOUT = 300


# -----------------------------------------------------------------------------
# Redis (cache + Celery)
# -----------------------------------------------------------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 2))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

_REDIS_AUTH = f":{REDIS_PASSWORD}@" if REDIS_PASSWORD else ""
REDIS_URL = f"redis://{_REDIS_AUTH}{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"


CACHE_CONFIG: dict[str, Any] = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 60 * 60,  # 1 hour
    "CACHE_KEY_PREFIX": "superset_cache_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_DB,
    "CACHE_REDIS_PASSWORD": REDIS_PASSWORD or None,
}

DATA_CACHE_CONFIG: dict[str, Any] = {
    **CACHE_CONFIG,
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24,  # 24 hours for chart data
    "CACHE_KEY_PREFIX": "superset_data_cache_",
}

FILTER_STATE_CACHE_CONFIG: dict[str, Any] = {
    **CACHE_CONFIG,
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24 * 7,
    "CACHE_KEY_PREFIX": "superset_filter_cache_",
}

EXPLORE_FORM_DATA_CACHE_CONFIG: dict[str, Any] = {
    **CACHE_CONFIG,
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24 * 7,
    "CACHE_KEY_PREFIX": "superset_explore_form_cache_",
}


# -----------------------------------------------------------------------------
# Async query results (SQL Lab)
# -----------------------------------------------------------------------------
class CeleryConfig:
    broker_url = REDIS_URL
    result_backend = REDIS_URL
    imports = ("superset.sql_lab", "superset.tasks.scheduler")
    worker_prefetch_multiplier = 1
    task_acks_late = True
    task_annotations = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
    }
    beat_schedule: dict[str, Any] = {
        # No scheduled tasks by default; alerts/reports register their own schedule.
    }


CELERY_CONFIG = CeleryConfig

RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST,
    port=REDIS_PORT,
    key_prefix="superset_results_",
    db=REDIS_DB,
    password=REDIS_PASSWORD or None,
)


# -----------------------------------------------------------------------------
# Feature flags
# -----------------------------------------------------------------------------
FEATURE_FLAGS: dict[str, bool] = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ALERT_REPORTS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}


# -----------------------------------------------------------------------------
# SQL Lab
# -----------------------------------------------------------------------------
SQLLAB_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
ENABLE_TIME_ROTATE = True
LOG_LEVEL = os.environ.get("SUPERSET_LOG_LEVEL", "INFO")

# -----------------------------------------------------------------------------
# Session persistence — make login survive browser close (cookie gets Expires)
# Without this, Flask default = browser-session cookie → tab close = re-login.
# PERMANENT_SESSION_LIFETIME (Superset default = 7d) only applies when permanent.
# -----------------------------------------------------------------------------
def FLASK_APP_MUTATOR(app):
    from flask import session

    @app.before_request
    def _make_session_permanent() -> None:
        session.permanent = True
