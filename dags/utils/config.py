"""
DAG Configuration
=================

Central configuration for all Airflow DAGs.
"""

from datetime import datetime
from typing import Dict, List


def get_current_season() -> int:
    """
    Calculate the current football season year dynamically.

    Football seasons typically start in August.
    Season 2024/2025 is represented as 2024.

    Returns:
        Current season year (e.g., 2024 for 2024/2025 season)
    """
    today = datetime.now()
    # If we're past August, it's the current year's season
    # Otherwise, it's the previous year's season
    return today.year if today.month >= 8 else today.year - 1


# Supported leagues for scraping
# NOTE: Reduced to 1 league initially to prevent OOM issues
# After successful test runs, gradually add more leagues back:
# 'ESP-La Liga', 'GER-Bundesliga', 'ITA-Serie A', 'FRA-Ligue 1'
LEAGUES: List[str] = [
    'ENG-Premier League',
]

# Current season (dynamically calculated)
CURRENT_SEASON: int = get_current_season()

# SoFIFA versions (FIFA game versions)
# Valid values: "latest", "all", or list of version IDs from URL (e.g., 230034)
SOFIFA_VERSIONS: str = 'latest'

# DAG schedule configuration (cron format, UTC)
SCHEDULES: Dict[str, str] = {
    'dag_ingest_fbref': '0 6 * * *',         # 6:00 UTC daily
    'dag_ingest_fotmob': '0 7 * * *',        # 7:00 UTC daily
    'dag_ingest_matchhistory': '0 8 * * *',  # 8:00 UTC daily
    'dag_ingest_understat': '0 9 * * *',     # 9:00 UTC daily
    'dag_ingest_whoscored': '0 10 * * *',    # 10:00 UTC daily
    'dag_ingest_sofascore': '0 11 * * *',    # 11:00 UTC daily
    'dag_ingest_espn': '0 12 * * *',         # 12:00 UTC daily
    'dag_ingest_clubelo': '0 13 * * *',      # 13:00 UTC daily
    'dag_ingest_sofifa': '0 6 * * 0',        # 6:00 UTC Sunday (weekly)
    'dag_master_pipeline': '0 14 * * *',     # 14:00 UTC daily
}

# Minimum row thresholds for validation
MIN_ROW_THRESHOLDS: Dict[str, int] = {
    'schedule': 100,
    'player_stats': 100,
    'team_stats': 50,
    'shots': 500,
    'elo_ratings': 100,
}

# Tags for DAG organization
DAG_TAGS: Dict[str, List[str]] = {
    'fbref': ['scraping', 'fbref', 'bronze', 'football', 'selenium'],
    'fotmob': ['scraping', 'fotmob', 'bronze', 'football', 'selenium'],
    'matchhistory': ['scraping', 'matchhistory', 'bronze', 'football', 'odds'],
    'understat': ['scraping', 'understat', 'bronze', 'football', 'xg'],
    'whoscored': ['scraping', 'whoscored', 'bronze', 'football', 'selenium', 'spadl'],
    'sofascore': ['scraping', 'sofascore', 'bronze', 'football'],
    'espn': ['scraping', 'espn', 'bronze', 'football'],
    'clubelo': ['scraping', 'clubelo', 'bronze', 'football', 'elo'],
    'sofifa': ['scraping', 'sofifa', 'bronze', 'football', 'fifa'],
    'master': ['orchestration', 'master', 'pipeline'],
}
