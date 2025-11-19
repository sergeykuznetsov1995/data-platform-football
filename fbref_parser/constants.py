"""
Constants and configuration for FBref parsers.

This module contains all shared constants including table patterns,
column markers, and rename mappings used across field player and goalkeeper parsers.
"""

# Unique markers for field player table identification (content-based detection)
UNIQUE_MARKERS = {
    'gca': ['gca', 'sca90', 'goal creation', 'shot creation'],
    'possession': ['touches', 'carries', 'take-ons', 'dribbles'],
    'misc': ['recov', 'aerial', 'fls', 'fld'],
    'playing_time': ['starts', 'mn/start', 'compl', 'min%'],
    'defense': ['tkl', 'tkl+int', 'blocks', 'challenges'],
    'pass_types': ['live', 'dead', 'fk', 'tb'],
    'passing': ['cmp', 'att', 'cmp%', 'totdist'],
    'shooting': ['sh', 'sot', 'sot%', 'g/sh'],
    'standard': ['gls', 'ast', 'g+a', 'pk']
}

# Goalkeeper table patterns for table identification
GK_TABLE_PATTERNS = {
    'goalkeeping': ['GA', 'Save%', 'Saves', 'SoTA', 'CS'],
    'advanced_goalkeeping': ['PSxG', 'PSxG/SoT', 'PSxG+/-', 'PKA', 'PKsv', 'PKm'],
    'standard': ['Gls', 'Ast', 'G+A', 'PK', 'PKatt'],
    'shooting': ['Sh', 'SoT', 'SoT%', 'G/Sh', 'G/SoT'],
    'passing': ['Cmp', 'Att', 'Cmp%', 'TotDist', 'PrgDist', 'PrgP'],
    'pass_types': ['Live', 'Dead', 'FK', 'TB', 'Sw', 'Crs', 'TI', 'CK'],
    'gca': ['GCA', 'SCA', 'GCA90', 'SCA90'],
    'defense': ['Tkl', 'TklW', 'Def 3rd', 'Mid 3rd', 'Att 3rd', 'Blocks', 'Int'],
    'possession': ['Touches', 'Def Pen', 'Def 3rd', 'Mid 3rd', 'Att 3rd', 'Live', 'Carries', 'Take-Ons'],
    'playing_time': ['MP', 'Starts', 'Min', '90s', 'Mn/MP', 'Min%', 'Mn/Start'],
    'miscellaneous': ['CrdY', 'CrdR', 'Fls', 'Fld', 'Recov', 'Won', 'Lost', 'Won%'],
    'match_logs': ['Date', 'Day', 'Venue', 'Result', 'Opponent']
}

# Key columns used for merging tables
KEY_COLUMNS = ['Season', 'Age', 'Squad', 'Country', 'Comp', 'LgRank']

# Extended key columns including MP (for some contexts)
KEY_COLUMNS_WITH_MP = KEY_COLUMNS + ['MP']

# Playing Time column patterns for duplicate removal
PLAYING_TIME_PATTERNS = [
    r'Playing[_ ]Time[_ ]',  # MultiIndex: "Playing Time_Starts", "Playing_Time_Starts" etc.
    r'^(MP|Starts|Min|90s|Mn/MP|Min%|Mn/Start|Compl)$',  # Exact matches
    r'Performance_(Starts|Min|90s)',  # From Goalkeeping: "Performance_Starts"
    r'Team_Success_',  # Team Success columns from Playing Time table
    r'Subs_',  # Substitution-related columns
]

# Basic column renames for field players
FIELD_PLAYER_BASIC_RENAMES = {
    'Season': 'season',
    'Age': 'age',
    'Squad': 'squad',
    'Country': 'country',
    'Comp': 'competition',
}

# Playing Time column renames for field players
FIELD_PLAYER_PLAYING_TIME_RENAMES = {
    # Basic Playing Time columns (with prefix from playing_time table)
    'MP_playing_time': 'matches_played',
    'Playing Time_MP_playing_time': 'matches_played',
    'Starts_playing_time': 'starts',
    'Playing Time_Starts_playing_time': 'starts',
    'Min_playing_time': 'minutes',
    'Playing Time_Min_playing_time': 'minutes',
    '90s_playing_time': 'minutes_90',
    'Playing Time_90s_playing_time': 'minutes_90',
    # Other Playing Time columns
    'Playing Time_Mn/MP': 'minutes_per_match',
    'Mn/MP_playing_time': 'minutes_per_match',
    'Playing Time_Mn/MP_playing_time': 'minutes_per_match',
    'Playing Time_Min%_playing_time': 'minutes_pct',
    'Min%_playing_time': 'minutes_pct',
    'Starts_Starts_playing_time': 'starts_total',
    'Starts_Mn/Start_playing_time': 'minutes_per_start',
    'Mn/Start_playing_time': 'minutes_per_start',
    'Starts_Compl': 'matches_completed',
    'Compl_playing_time': 'matches_completed',
    'Starts_Compl_playing_time': 'matches_completed',
    'Subs_Subs_playing_time': 'subs_on',
    'Subs_playing_time': 'subs_on',
    'Subs_Mn/Sub_playing_time': 'minutes_per_sub',
    'Mn/Sub_playing_time': 'minutes_per_sub',
    'Subs_unSub_playing_time': 'subs_unused',
    'unSub_playing_time': 'subs_unused',
    'Team Success_PPM_playing_time': 'team_points_per_match',
    'PPM_playing_time': 'team_points_per_match',
    'Team Success_onG_playing_time': 'team_goals_for',
    'onG_playing_time': 'team_goals_for',
    'Team Success_onGA_playing_time': 'team_goals_against',
    'onGA_playing_time': 'team_goals_against',
    'Team Success_+/-_playing_time': 'team_goal_diff',
    '+/-_playing_time': 'team_goal_diff',
    'Team Success_+/-90_playing_time': 'team_goal_diff_per90',
    '+/-90_playing_time': 'team_goal_diff_per90',
    'Team Success_On-Off_playing_time': 'team_on_off',
    'On-Off_playing_time': 'team_on_off',
    'Team Success (xG)_onxG_playing_time': 'team_xg_for',
    'onxG_playing_time': 'team_xg_for_xg',
    'Team Success (xG)_onxGA_playing_time': 'team_xg_against',
    'onxGA_playing_time': 'team_xg_against_xg',
    'Team Success (xG)_xG+/-_playing_time': 'team_xg_diff',
    'xG+/-_playing_time': 'team_xg_diff',
    'Team Success (xG)_xG+/-90_playing_time': 'team_xg_diff_per90',
    'xG+/-90_playing_time': 'team_xg_diff_per90',
    'Team Success (xG)_On-Off_playing_time': 'team_xg_on_off'
}

# Table suffix abbreviations for field players
FIELD_PLAYER_SUFFIX_MAP = {
    '_shooting': '_sh',
    '_passing': '_pass',
    '_pass_types': '_pt',
    '_defense': '_def',
    '_possession': '_poss',
    '_misc': '_misc',
    '_gca': '_gca'
}

# Goalkeeper-specific column renames
GOALKEEPER_BASIC_RENAMES = {
    # Main columns
    'Season': 'season',
    'Age': 'age',
    'Squad': 'squad',
    'Country': 'country',
    'Comp': 'competition',
    'MP': 'matches_played',
    'Starts': 'starts',
    'Min': 'minutes',
    '90s': 'minutes_90',

    # Goalkeeping
    'GA': 'goals_against',
    'GA90': 'goals_against_per90',
    'SoTA': 'shots_on_target_against',
    'Saves': 'saves',
    'Save%': 'save_pct',
    'W': 'wins',
    'D': 'draws',
    'L': 'losses',
    'CS': 'clean_sheets',
    'CS%': 'clean_sheet_pct',
    'PKA': 'penalty_kicks_attempted',
    'PKsv': 'penalty_kicks_saved',
    'PKm': 'penalty_kicks_missed',
    'PSxG': 'post_shot_expected_goals',
    'PSxG/SoT': 'psxg_per_shot_on_target',
    'PSxG+/-': 'psxg_net',
    '/90': 'per_90_minutes',

    # Passing
    'Cmp': 'passes_completed',
    'Att': 'passes_attempted',
    'Cmp%': 'pass_completion_pct',
    'TotDist': 'total_pass_distance',
    'PrgDist': 'progressive_pass_distance',
    'AvgLen': 'avg_pass_length',
    'Launched': 'long_passes_attempted',
    'Launch%': 'long_pass_pct',

    # Standard stats
    'Gls': 'goals',
    'Ast': 'assists',
    'G+A': 'goals_plus_assists',
    'G-PK': 'non_penalty_goals',
    'PK': 'penalty_kicks_made',
    'PKatt': 'penalty_kicks_attempted',
    'xG': 'expected_goals',
    'npxG': 'non_penalty_expected_goals',
    'xA': 'expected_assists',

    # Shooting
    'Sh': 'shots',
    'SoT': 'shots_on_target',
    'SoT%': 'shots_on_target_pct',
    'G/Sh': 'goals_per_shot',
    'G/SoT': 'goals_per_shot_on_target',

    # Defense
    'Tkl': 'tackles',
    'TklW': 'tackles_won',
    'Def 3rd': 'tackles_def_3rd',
    'Mid 3rd': 'tackles_mid_3rd',
    'Att 3rd': 'tackles_att_3rd',
    'Int': 'interceptions',
    'Blocks': 'blocks',

    # Possession
    'Touches': 'touches',
    'Def Pen': 'touches_def_pen_area',
    'Live': 'live_ball_touches',
    'Carries': 'carries',
    'Take-Ons': 'take_ons',

    # GCA/SCA
    'GCA': 'goal_creating_actions',
    'GCA90': 'goal_creating_actions_per90',
    'SCA': 'shot_creating_actions',
    'SCA90': 'shot_creating_actions_per90',

    # Miscellaneous
    'CrdY': 'yellow_cards',
    'CrdR': 'red_cards',
    'Fls': 'fouls_committed',
    'Fld': 'fouls_drawn',
    'Recov': 'ball_recoveries',
    'Won': 'aerial_duels_won',
    'Lost': 'aerial_duels_lost',
    'Won%': 'aerial_duels_won_pct',

    # Pass Types
    'Live': 'live_passes',
    'Dead': 'dead_passes',
    'FK': 'free_kicks',
    'TB': 'through_balls',
    'Sw': 'switches',
    'Crs': 'crosses',
    'TI': 'throw_ins',
    'CK': 'corner_kicks',

    # Goalkeeper specific
    'Opp': 'crosses_stopped',
    'Stp': 'crosses_stopped_pct',
    'Stp%': 'cross_stop_pct',
    '#OPA': 'defensive_actions_outside_penalty_area',
    'AvgDist': 'avg_distance_defensive_actions'
}

# Snake case replacements for readability
SNAKE_CASE_REPLACEMENTS = {
    'g_plus_a': 'goals_plus_assists',
    'g_minus_pk': 'goals_minus_penalties',
    'npxg_plus_xag': 'npxg_plus_xag',
    'g_plus_a_minus_pk': 'goals_plus_assists_minus_penalties',
    'per_90_minutes': 'per_90',
    'gca_types': 'gca_types',
    'sca_types': 'sca_types',
    'aerial_duels': 'aerial_duels',
    'def_3rd': 'def_third',
    'mid_3rd': 'mid_third',
    'att_3rd': 'att_third',
    'def_pen': 'def_penalty_area',
    'att_pen': 'att_penalty_area',
    'take_minus_ons': 'takeons',
    'team_success': 'team_success',
    'mn_per_mp': 'minutes_per_match',
    'min_pct': 'minutes_pct',
    'mn_per_start': 'minutes_per_start',
    'mn_per_sub': 'minutes_per_sub'
}

# User-Agent pool for rotation (prevents 403 errors from static UA)
USER_AGENT_POOL = [
    # Chrome Windows (most common)
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    # Chrome Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    # Firefox Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    # Firefox Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
    # Edge Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
]

# Default HTTP headers for requests (Updated 2025-01 for FBref compatibility)
# Enhanced headers to better mimic real browser behavior
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Cache-Control': 'max-age=0',
    'DNT': '1'
}

# Rate limiting configuration (FBref allows 10 requests per minute)
# Conservative delays for maximum safety and 24-hour ban avoidance
MIN_REQUEST_DELAY = 6.0   # Minimum delay between requests (seconds)
MAX_REQUEST_DELAY = 8.0   # Maximum delay between requests (seconds)
MAX_REQUESTS_PER_MINUTE = 10  # FBref strict rate limit (Sports Reference policy)

# Note: With jitter (10-20%), actual delays will be 6.6-9.6 seconds
# This ensures we never exceed 10 requests/minute even with timing variations

# Default output directory paths
DEFAULT_OUTPUT_DIR_FIELD_PLAYERS = "/root/data_platform/test_arsenal_players"
DEFAULT_OUTPUT_DIR_GOALKEEPERS = "/root/data_platform/test_arsenal_goalkeepers"

# Default Arsenal squad URL
DEFAULT_ARSENAL_SQUAD_URL = "https://fbref.com/en/squads/18bb7c10/2023-2024/Arsenal-Stats"

# Duplicate 90s columns to remove from field player data
DUPLICATE_90S_COLUMNS = [
    '90s_shooting', '90s_passing', '90s_pass_types',
    '90s_defense', '90s_gca', '90s_possession', '90s_misc'
]
