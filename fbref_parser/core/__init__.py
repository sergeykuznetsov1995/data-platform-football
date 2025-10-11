"""
Core functionality for FBref parsing

Contains modules for:
- Web scraping (scraper.py)
- Table detection and classification (table_detector.py)
- Data cleaning (data_cleaner.py)
- Column processing (column_processor.py)
"""

from .scraper import FBrefScraper, extract_all_tables
from .table_detector import (
    identify_field_player_tables,
    identify_goalkeeper_tables,
    find_tables_by_unique_markers,
    resolve_table_conflict,
    score_table_quality,
    analyze_all_tables
)
from .data_cleaner import (
    clean_dataframe,
    clean_aggregated_rows,
    clean_country_column,
    clean_competition_column,
    remove_duplicate_columns,
    remove_playing_time_duplicates
)
from .column_processor import (
    fix_column_names,
    process_multiindex_columns,
    convert_to_snake_case,
    apply_field_player_renames,
    apply_goalkeeper_renames
)

__all__ = [
    'FBrefScraper',
    'extract_all_tables',
    'identify_field_player_tables',
    'identify_goalkeeper_tables',
    'find_tables_by_unique_markers',
    'resolve_table_conflict',
    'score_table_quality',
    'analyze_all_tables',
    'clean_dataframe',
    'clean_aggregated_rows',
    'clean_country_column',
    'clean_competition_column',
    'remove_duplicate_columns',
    'remove_playing_time_duplicates',
    'fix_column_names',
    'process_multiindex_columns',
    'convert_to_snake_case',
    'apply_field_player_renames',
    'apply_goalkeeper_renames'
]
