"""
Backwards-compatible re-exports from parsers package.

All public symbols that were previously defined in this module
are now implemented in ``scrapers.fbref.parsers`` sub-modules.
This file re-exports them so that existing imports like::

    from scrapers.fbref.html_parser import parse_table

continue to work without changes.
"""

from scrapers.fbref.parsers import *  # noqa: F401,F403
from scrapers.fbref.parsers import (  # noqa: F401,F811 — compatibility re-exports
    MULTIINDEX_PREFIXES,
    PLAYER_ID_PATTERN,
    TEAM_ID_PATTERN,
    normalize_column_names,
    extract_tables_from_comments,
    diagnose_html_structure,
    drop_blank_rows,
    parse_table,
    _parse_table_element,
    _table_has_player_header,
    extract_player_ids_from_table,
    extract_team_ids_from_table,
    extract_match_urls_from_schedule,
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
