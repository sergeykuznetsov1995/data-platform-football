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

# Last 5 seasons as 4-digit CSV ("2122,2223,2324,2425,2526") — WhoScored multi-season ingest.
SEASONS_STR: str = ','.join(
    f"{(CURRENT_SEASON - off) % 100:02d}{(CURRENT_SEASON - off + 1) % 100:02d}"
    for off in range(4, -1, -1)
)

# SoFIFA versions (FIFA game versions)
# Valid values: "latest", "all", or list of version IDs from URL (e.g., 230034)
SOFIFA_VERSIONS: str = 'latest'

# DAG schedule configuration (cron format, UTC)
SCHEDULES: Dict[str, str] = {
    'dag_ingest_fbref': '0 6 * * 1',         # 6:00 UTC Monday (weekly)
    'dag_ingest_fotmob': '0 7 * * *',        # 7:00 UTC daily
    'dag_ingest_matchhistory': '0 8 * * *',  # 8:00 UTC daily
    'dag_ingest_understat': '0 9 * * *',     # 9:00 UTC daily
    'dag_ingest_whoscored': '0 10 * * *',    # 10:00 UTC daily
    'dag_ingest_sofascore': '0 11 * * *',    # 11:00 UTC daily
    'dag_ingest_espn': '0 12 * * *',         # 12:00 UTC daily
    'dag_ingest_clubelo': '0 13 * * *',      # 13:00 UTC daily
    'dag_ingest_sofifa': '0 6 * * 0',        # 6:00 UTC Sunday (weekly)
    'dag_ingest_transfermarkt': '0 4 * * 1', # 4:00 UTC Monday (weekly)
    'dag_ingest_capology': '0 5 * * 1',      # 5:00 UTC Monday (weekly)
    'dag_master_pipeline': '0 14 * * *',     # 14:00 UTC daily
    'dag_transform_fbref_silver': None,     # Trigger only (after ingestion)
    'dag_transform_fotmob_silver': None,    # Trigger only (after ingestion)
}

# Minimum row thresholds for validation (per single DagRun = 1 league x 1 season).
# Consumed by validate_table() in dags/utils/bronze_validation.py (fail-closed
# on a missing key, #106/#110) via the validate tasks in the whoscored / espn /
# understat / sofifa ingest DAGs. Currently LEAGUES=['ENG-Premier League'] and
# CURRENT_SEASON is one season, so values below are sized for 1 APL season
# (floor against a wiped/empty table — whole-table COUNT(*), so they must not
# false-fail if the configured scope ever shrinks to one season).
MIN_ROW_THRESHOLDS: Dict[str, int] = {
    'schedule': 350,        # 380 APL matches/season, allow ~5-10% missing/postponed
    'player_stats': 500,    # ~600-800 unique player-season rows expected, ~25% margin
    'team_stats': 18,       # 20 APL clubs, allow ~10% missing per rare stat_type
    'shots': 8000,          # ~10k shots/season, ~20% margin
    'elo_ratings': 100,     # ClubElo not in FBref-only roadmap; left unchanged
    # WhoScored (issue #106): hidden-enabler thresholds. Without these keys,
    # validate_table() falls back to 0 and silently passes an empty schedule
    # scrape (root cause of #102). Sized for APL 1 season.
    'whoscored_schedule': 340,    # 380 fixtures/season - 5-10% margin
    'whoscored_events': 500_000,  # ~540k events/season - 7% margin
    # ESPN / Understat / SoFIFA (issue #466): same silent-fail class as #102 —
    # read_* swallowed errors and runners exited 0. Floors calibrated against
    # live Bronze counts on 2026-06-11.
    'espn_schedule': 340,                  # 380 fixtures/season - 10% margin
    'understat_schedule': 340,             # 380 fixtures/season - 10% margin
    'understat_players': 450,              # ~547 player-season rows/season - 18%
    'understat_shots': 8000,               # ~9.8k shots/season - 20% margin
    'understat_team_match_stats': 340,     # 380 team-match rows/season - 10%
    'understat_player_match_stats': 10_000,  # ~11.1k rows/season - 10% margin
    'sofifa_players': 450,                 # 546 players / APL edition - 18%
    'sofifa_teams': 18,                    # 20 APL clubs - 10%
    'sofifa_team_ratings': 18,             # 20 APL clubs - 10%
    'sofifa_versions': 15,                 # ~20 editions (FIFA 07→FC 26) on post-EA-FC homepage (#654/#670); +1/yr
    'sofifa_leagues': 1,                   # 1-row lookup (APL-only scope)
    'sofifa_player_ratings': 450,          # 546 per-player pages / edition - 18%
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
    'transfermarkt': ['scraping', 'transfermarkt', 'bronze', 'football'],
    'capology': ['scraping', 'capology', 'bronze', 'football', 'salaries'],
    'master': ['orchestration', 'master', 'pipeline'],
    'silver_fbref': ['transform', 'fbref', 'silver', 'football', 'trino'],
}
