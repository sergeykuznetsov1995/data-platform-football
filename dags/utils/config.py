"""
DAG Configuration
=================

Central configuration for all Airflow DAGs.
"""

from datetime import datetime
from typing import Dict, List, Tuple


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
    'INT-World Cup',  # #913 Phase 1 (single-year WC). Targeting 5 sources: fbref, fotmob, sofascore, espn, whoscored. Club-only sources (sofifa etc) protected in thresholds.
]

# Club-only slice of LEAGUES — the scope for sources that do not cover
# international tournaments (sofifa). #920 Phase 2: also the league set the
# sofifa validate tasks check per-league floors against.
NON_INTERNATIONAL_LEAGUES: List[str] = [
    l for l in LEAGUES if not l.startswith('INT-')
]

# WhoScored multi-league scope (#708). Deliberately independent of the global
# LEAGUES — flipping LEAGUES switches EVERY source at once (see OOM note
# above), while WhoScored scales safely per league: dag_ingest_whoscored fans
# out one sequential task per league and events skip-existing keeps the
# steady-state cost at "new matches only". First run per new league is heavy
# (~380 matches/season × 5 seasons through FlareSolverr) but resumable.
WHOSCORED_LEAGUES: List[str] = [
    'ENG-Premier League',
    'ESP-La Liga',
    'GER-Bundesliga',
    'ITA-Serie A',
    'FRA-Ligue 1',
    'INT-World Cup',  # #913 Phase 1 (5th source; may be blocked by FlareSolverr 512M)
]

# Understat multi-league scope. Independent of the global LEAGUES for the same
# reason as WHOSCORED_LEAGUES (flipping LEAGUES switches every source at once).
# Extending is a deliberate step: first run per new league backfills ~380
# match JSONs (~5 KB wire each ≈ 2 MB) + the league JSON — direct traffic, no
# proxy; steady-state is "new matches only" (persistent soccerdata_cache).
# Understat covers the top-5 leagues out of the box; RUS-Premier League needs a
# custom soccerdata league_dict.json first (see UnderstatScraper.SUPPORTED_LEAGUES).
UNDERSTAT_LEAGUES: List[str] = [
    'ENG-Premier League',
]

# MatchHistory (football-data.co.uk) multi-league scope. Independent of the
# global LEAGUES for the same reason as WHOSCORED_LEAGUES. Cheapest source on
# the platform: one season CSV ≈ 200 KB per league, fetched directly (no
# proxy), and the scraper sends conditional requests (ETag/If-Modified-Since)
# so an unchanged CSV costs a 0-byte 304. No OOM risk. Backfilling a past
# season for these leagues = manual DAG trigger with the season param.
# Scraper supports 18 league codes (MatchHistoryScraper.LEAGUE_CODES).
MATCHHISTORY_LEAGUES: List[str] = [
    'ENG-Premier League',
    'ESP-La Liga',
    'GER-Bundesliga',
    'ITA-Serie A',
    'FRA-Ligue 1',
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

# --- Per-competition bronze floors (#920 Phase 2) ---------------------------
# Historical floors were calibrated against ONE basis: APL — 20 clubs,
# 380 scheduled matches/season. A 104-match World Cup (or a 51-match Euro)
# can never satisfy those constants, and the whole-table COUNT(*) let a
# missing league hide behind the aggregate. Per-league floors scale the same
# calibrated bases by each competition's own volume from competitions.yaml.
_APL_MATCHES = 380
_APL_TEAMS = 20

# threshold_key -> (unit, base floor at the APL basis). Keys here are the
# league-aware subset of MIN_ROW_THRESHOLDS: tables whose per-league count is
# deterministic at validation time (schedule-class tables — fixtures known
# upfront — and full-snapshot per-league sources). Append-only wipe-floors and
# tables without a league column stay whole-table in MIN_ROW_THRESHOLDS below.
# unit: 'match'  — scales with the competition's scheduled match count;
#       'team'   — scales with team_count (player-volume tables track squad
#                  count, which tracks team_count);
#       'league' — constant per league (e.g. 1 lookup row).
PER_LEAGUE_FLOOR_BASES: Dict[str, Tuple[str, int]] = {
    'whoscored_schedule': ('match', 340),      # 380 fixtures/season - 10% margin
    'espn_schedule': ('match', 340),           # 380 fixtures/season - 10% margin
    'understat_schedule': ('match', 340),      # 380 fixtures/season - 10% margin
    'understat_team_match_stats': ('match', 340),   # 380 team-match rows - 10%
    'understat_shots': ('match', 8000),        # ~9.8k shots/season - 20% margin
    'understat_player_match_stats': ('match', 10_000),  # ~11.1k rows/season - 10%
    'understat_players': ('team', 450),        # ~547 player-season rows - 18%
    'sofifa_players': ('team', 450),           # 546 players / league edition - 18%
    'sofifa_teams': ('team', 18),              # 20 clubs / league - 10%
    'sofifa_team_ratings': ('team', 18),       # 20 clubs / league - 10%
    'sofifa_leagues': ('league', 1),           # 1 lookup row per league
}


def scale_floor_for_league(unit: str, base: int, league: str) -> int:
    """Integer-scale an APL-calibrated base floor to another competition.

    Pure integer arithmetic (``base * basis // apl_basis``) so a 20-team
    club league reduces to ``base`` EXACTLY — the #920 Phase 2 equivalence
    contract with the pre-per-league constants.
    """
    if unit == 'league':
        return base
    # Lazy import: utils.config is imported by every DAG and by host tests
    # without MEDALLION_CONFIG_DIR — competitions.yaml must only be read when
    # a floor is actually evaluated (same pattern as the runner bridge).
    from utils.medallion_config import get_competition_floor_basis
    matches, teams = get_competition_floor_basis(league)
    if unit == 'match':
        return base * matches // _APL_MATCHES
    if unit == 'team':
        return base * teams // _APL_TEAMS
    raise ValueError(f"unknown floor unit {unit!r}")


def get_min_row_threshold(threshold_key: str, league: str) -> int:
    """Per-league DQ floor (#920 Phase 2).

    An unknown threshold_key raises KeyError — validate_table wraps it into
    the same fail-closed AirflowException as the whole-table path (#106/#110).
    """
    unit, base = PER_LEAGUE_FLOOR_BASES[threshold_key]
    return scale_floor_for_league(unit, base, league)


# Minimum row thresholds for validation (per single DagRun = 1 league x 1 season).
# Consumed by validate_table() in dags/utils/bronze_validation.py (fail-closed
# on a missing key, #106/#110) via the validate tasks in the whoscored / espn /
# understat / sofifa ingest DAGs.
#
# #920 Phase 2: keys present in PER_LEAGUE_FLOOR_BASES are enforced per league
# via get_min_row_threshold when the DAG passes its league scope; the values
# below are the whole-table fallback (validate_table without `leagues`) and
# the wipe-floors that stay whole-table by design:
#   - whoscored_events: append-only over nonuniform history (10-season EPL vs
#     5-season others) — a per-season basis is meaningless;
#   - whoscored_player_profile: absent from the bronze schema snapshot,
#     per-league coverage unverified — revisit after #708;
#   - espn_lineup / espn_matchsheet: accumulating per-match tables sized to
#     ONE season (~28 lineup / 2 matchsheet rows per match x 340 - margin);
#     a per-league floor would false-fail early tournament days, while the
#     10-season live tables (~145k / 7.6k rows) never trip a 1-season floor;
#   - sofifa_versions / sofifa_player_ratings: no league column in bronze.
MIN_ROW_THRESHOLDS: Dict[str, int] = {
    # WhoScored (issue #106): hidden-enabler thresholds. Without these keys,
    # validate_table() falls back to 0 and silently passes an empty schedule
    # scrape (root cause of #102). schedule scales with WHOSCORED_LEAGUES
    # (every league is scraped each run, replace semantics).
    'whoscored_schedule':
        PER_LEAGUE_FLOOR_BASES['whoscored_schedule'][1] * len(WHOSCORED_LEAGUES),
    'whoscored_events': 20_000_000,  # #895: top-5×10-season backfill landed (27.9M rows, append-only); wipe-floor ~72% of live
    'whoscored_player_profile': 300,  # ~531 players/season/league (#37); raise after #708 backfill
    # ESPN / Understat / SoFIFA (issue #466): same silent-fail class as #102 —
    # read_* swallowed errors and runners exited 0. Floors calibrated against
    # live Bronze counts on 2026-06-11.
    'espn_schedule': PER_LEAGUE_FLOOR_BASES['espn_schedule'][1],
    'espn_lineup': 9000,
    'espn_matchsheet': 620,
    'understat_schedule': PER_LEAGUE_FLOOR_BASES['understat_schedule'][1],
    'understat_players': PER_LEAGUE_FLOOR_BASES['understat_players'][1],
    'understat_shots': PER_LEAGUE_FLOOR_BASES['understat_shots'][1],
    'understat_team_match_stats':
        PER_LEAGUE_FLOOR_BASES['understat_team_match_stats'][1],
    'understat_player_match_stats':
        PER_LEAGUE_FLOOR_BASES['understat_player_match_stats'][1],
    # sofifa_*: club leagues only (INT-* not covered by sofifa), scaled by the
    # club-league count so the floor does not inflate when a tournament joins
    # LEAGUES (#913).
    'sofifa_players':
        PER_LEAGUE_FLOOR_BASES['sofifa_players'][1] * len(NON_INTERNATIONAL_LEAGUES),
    'sofifa_teams':
        PER_LEAGUE_FLOOR_BASES['sofifa_teams'][1] * len(NON_INTERNATIONAL_LEAGUES),
    'sofifa_team_ratings':
        PER_LEAGUE_FLOOR_BASES['sofifa_team_ratings'][1] * len(NON_INTERNATIONAL_LEAGUES),
    'sofifa_versions': 15,                 # ~20 editions (FIFA 07→FC 26) on post-EA-FC homepage (#654/#670); +1/yr
    'sofifa_leagues': len(NON_INTERNATIONAL_LEAGUES),   # 1 lookup row per league
    'sofifa_player_ratings':
        450 * len(NON_INTERNATIONAL_LEAGUES),  # 546 per-player pages / league edition - 18%; no league column in bronze
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
