"""
Airflow DAG Utilities
=====================

Common configuration, default arguments, and validators for Airflow DAGs.
"""

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES
from utils.default_args import DEFAULT_ARGS, SELENIUM_ARGS

__all__ = [
    'LEAGUES',
    'CURRENT_SEASON',
    'SCHEDULES',
    'DEFAULT_ARGS',
    'SELENIUM_ARGS',
]
