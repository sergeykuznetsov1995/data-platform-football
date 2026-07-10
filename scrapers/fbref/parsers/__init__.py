"""
FBref Parsers Package
=====================

Re-exports all public functions from sub-modules so that
``from scrapers.fbref.parsers import X`` works for every symbol
that was previously available in ``scrapers.fbref.html_parser``.
"""

# --- table_parser -----------------------------------------------------------
from scrapers.fbref.parsers.table_parser import (
    MULTIINDEX_PREFIXES,
    normalize_column_names,
    extract_tables_from_comments,
    diagnose_html_structure,
    drop_blank_rows,
    parse_table,
    _parse_table_element,
    _table_has_player_header,
)

# --- id_extractors ----------------------------------------------------------
from scrapers.fbref.parsers.id_extractors import (
    PLAYER_ID_PATTERN,
    TEAM_ID_PATTERN,
    MANAGER_ID_PATTERN,
    extract_player_ids_from_table,
    extract_team_ids_from_table,
    extract_match_urls_from_schedule,
)

# --- finders ----------------------------------------------------------------
from scrapers.fbref.parsers.finders import (
    find_schedule_table,
    find_team_stats_table,
    find_player_stats_table,
    parse_shots_table,
    parse_lineup_table,
    parse_events_from_scorebox,
    parse_team_match_stats_table,
    parse_player_match_stats_tables,
    parse_keeper_match_stats_tables,
    parse_match_managers,
    parse_match_officials,
)

__all__ = [
    # Constants
    'MULTIINDEX_PREFIXES',
    'PLAYER_ID_PATTERN',
    'TEAM_ID_PATTERN',
    'MANAGER_ID_PATTERN',
    # table_parser
    'normalize_column_names',
    'extract_tables_from_comments',
    'diagnose_html_structure',
    'drop_blank_rows',
    'parse_table',
    '_parse_table_element',
    '_table_has_player_header',
    # id_extractors
    'extract_player_ids_from_table',
    'extract_team_ids_from_table',
    'extract_match_urls_from_schedule',
    # finders
    'find_schedule_table',
    'find_team_stats_table',
    'find_player_stats_table',
    'parse_shots_table',
    'parse_lineup_table',
    'parse_events_from_scorebox',
    'parse_team_match_stats_table',
    'parse_player_match_stats_tables',
    'parse_keeper_match_stats_tables',
    'parse_match_managers',
    'parse_match_officials',
]
