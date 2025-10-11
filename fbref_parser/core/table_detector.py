"""
Table detection and identification for FBref statistics

This module provides functionality for identifying and classifying statistical tables
from FBref player pages using content-based analysis and unique column markers.
"""

import pandas as pd
from typing import List, Dict, Tuple, Optional

from ..constants import UNIQUE_MARKERS, GK_TABLE_PATTERNS


def identify_field_player_tables(all_page_tables: List[pd.DataFrame]) -> Dict[str, Tuple[int, pd.DataFrame]]:
    """
    Identify and classify tables for field players using content-based detection

    Analyzes table columns to determine table types (standard, shooting, passing, etc.)
    Uses column patterns from UNIQUE_MARKERS to identify table categories.

    Args:
        all_page_tables: List of all DataFrames extracted from the player page

    Returns:
        Dictionary mapping table type to (index, DataFrame) tuple
    """
    key_tables = {}

    for i, table in enumerate(all_page_tables):
        if len(table) < 10 or len(table.columns) < 10:
            continue

        cols_str = str(table.columns).lower()

        # Standard stats (main table)
        if ('season' in cols_str or 'squad' in cols_str) and 'gls' in cols_str and 'ast' in cols_str:
            if 'standard' not in key_tables:
                key_tables['standard'] = (i, table)
                print(f"✅ Найдена таблица STANDARD #{i}: {len(table)} строк, {len(table.columns)} колонок")

        # Shooting
        elif 'shooting' in cols_str or ('sh' in cols_str and 'sot' in cols_str):
            if 'shooting' not in key_tables:
                key_tables['shooting'] = (i, table)
                print(f"✅ Найдена таблица SHOOTING #{i}: {len(table)} строк, {len(table.columns)} колонок")

        # Passing
        elif 'passing' in cols_str or ('cmp' in cols_str and 'att' in cols_str):
            if 'passing' not in key_tables:
                key_tables['passing'] = (i, table)
                print(f"✅ Найдена таблица PASSING #{i}: {len(table)} строк, {len(table.columns)} колонок")

        # Pass Types
        elif 'pass types' in cols_str or 'live' in cols_str:
            if 'pass_types' not in key_tables:
                key_tables['pass_types'] = (i, table)
                print(f"✅ Найдена таблица PASS TYPES #{i}: {len(table)} строк, {len(table.columns)} колонок")

        # GCA/SCA
        elif any(marker in cols_str for marker in ['gca', 'sca', 'goal creation', 'shot creation', 'gca90', 'sca90', 'passlive', 'passdead']):
            if 'gca' not in key_tables:
                key_tables['gca'] = (i, table)
                print(f"✅ Найдена таблица GCA/SCA #{i}: {len(table)} строк, {len(table.columns)} колонок")

        # Defense
        elif 'defense' in cols_str or 'tkl' in cols_str:
            if 'defense' not in key_tables:
                key_tables['defense'] = (i, table)
                print(f"✅ Найдена таблица DEFENSE #{i}: {len(table)} строк, {len(table.columns)} колонок")

        # Playing Time (check BEFORE possession with more specific markers)
        elif any(marker in cols_str for marker in ['mn/mp', 'min%', 'team success', 'ppm']) and 'touches' not in cols_str:
            if 'playing_time' not in key_tables:
                key_tables['playing_time'] = (i, table)
                first_cols = [str(col) for col in table.columns[:5]]
                print(f"✅ Найдена таблица PLAYING TIME #{i}: {len(table)} строк, {len(table.columns)} колонок")
                print(f"   Первые колонки: {first_cols}")

        # Possession (MUST contain touches)
        elif 'touches' in cols_str and any(marker in cols_str for marker in ['def pen', 'def 3rd', 'mid 3rd', 'att 3rd', 'dribbles']):
            if 'possession' not in key_tables:
                key_tables['possession'] = (i, table)
                first_cols = [str(col) for col in table.columns[:5]]
                print(f"✅ Найдена таблица POSSESSION #{i}: {len(table)} строк, {len(table.columns)} колонок")
                print(f"   Первые колонки: {first_cols}")

        # Miscellaneous
        elif any(marker in cols_str for marker in ['misc', 'fls', 'fld', 'off', 'crs', 'tklw', 'pkwon', 'pkcon', 'og', 'recov', 'aerial', 'won', 'lost']):
            if 'misc' not in key_tables:
                key_tables['misc'] = (i, table)
                print(f"✅ Найдена таблица MISC #{i}: {len(table)} строк, {len(table.columns)} колонок")

    return key_tables


def identify_goalkeeper_tables(all_tables: List[pd.DataFrame]) -> Dict[str, List[Dict]]:
    """
    Identify and classify tables for goalkeepers (including all statistics)

    Classifies tables into goalkeeper-specific categories (goalkeeping, advanced_goalkeeping)
    as well as standard field player statistics.

    Args:
        all_tables: List of all DataFrames extracted from the goalkeeper page

    Returns:
        Dictionary mapping table type to list of table info dictionaries
    """
    identified_tables = {
        'goalkeeping': [],
        'advanced_goalkeeping': [],
        'standard': [],
        'shooting': [],
        'passing': [],
        'pass_types': [],
        'gca': [],
        'defense': [],
        'possession': [],
        'playing_time': [],
        'miscellaneous': [],
        'match_logs': []
    }

    for i, table in enumerate(all_tables):
        # Skip small tables (< 10 rows) to exclude "Last 5 Matches"
        if len(table) < 10:
            continue

        if isinstance(table.columns, pd.MultiIndex):
            columns = table.columns.get_level_values(-1).tolist()
        else:
            columns = list(table.columns)

        clean_columns = [str(col).strip() for col in columns]

        # Check each table type
        for table_type, keywords in GK_TABLE_PATTERNS.items():
            matches = sum(1 for keyword in keywords if any(keyword in col for col in clean_columns))

            # If characteristic columns found, classify table
            if matches >= 2:  # Minimum 2 matches for classification
                identified_tables[table_type].append({
                    'index': i,
                    'table': table,
                    'matches': matches,
                    'columns': clean_columns[:10]  # First 10 columns for debugging
                })

    return identified_tables


def find_tables_by_unique_markers(all_page_tables: List[pd.DataFrame], missing_tables: List[str]) -> Dict[str, Tuple[int, pd.DataFrame]]:
    """
    Find missing tables by unique column markers (fallback detection)

    Uses UNIQUE_MARKERS to find tables that weren't detected by primary content-based detection.
    Requires minimum 2 marker matches to classify a table.

    Args:
        all_page_tables: List of all tables from the page
        missing_tables: List of table types that need to be found

    Returns:
        Dictionary mapping table name to (index, DataFrame) tuple for found tables
    """
    found_tables = {}

    for table_name in missing_tables:
        if table_name not in UNIQUE_MARKERS:
            continue

        markers = UNIQUE_MARKERS[table_name]
        candidates = []

        # Search across all tables
        for i, table in enumerate(all_page_tables):
            if len(table) < 10:  # Skip small tables
                continue

            cols_str = str(table.columns).lower()

            # Check for unique markers
            marker_count = sum(1 for marker in markers if marker in cols_str)

            if marker_count >= 2:  # Need minimum 2 markers
                candidates.append((i, table, marker_count, len(table)))

        # Choose best table (more markers, more rows)
        if candidates:
            best = max(candidates, key=lambda x: (x[2], x[3]))  # sort by markers, then by size
            found_tables[table_name] = (best[0], best[1])
            print(f"✅ Найдена таблица {table_name.upper()} #{best[0]} по маркерам ({best[2]} маркеров, {best[3]} строк)")

    return found_tables


def score_table_quality(table: pd.DataFrame, table_type: str, markers: List[str]) -> float:
    """
    Evaluate table match quality for a specific type

    Scores tables based on marker matches, exact type matches, and table size.

    Args:
        table: DataFrame to evaluate
        table_type: Expected table type
        markers: List of marker strings to look for

    Returns:
        Quality score from 0 to 100
    """
    cols_str = str(table.columns).lower()
    score = 0

    # Base score from markers
    marker_matches = sum(1 for marker in markers if marker in cols_str)
    score += marker_matches * 20  # Up to 80 points for markers

    # Bonus for exact match
    if table_type == 'gca' and ('gca' in cols_str or 'sca' in cols_str):
        score += 15
    elif table_type == 'pass_types' and ('pass types' in cols_str):
        score += 15
    elif table_type == 'possession' and ('possession' in cols_str):
        score += 15

    # Bonus for table size (more rows = better)
    size_bonus = min(len(table) / 50 * 5, 10)  # Up to 10 points
    score += size_bonus

    # Penalty for very small tables
    if len(table) < 15:
        score -= 20

    return min(score, 100)


def resolve_table_conflict(existing_name: str, existing_table: pd.DataFrame,
                          new_name: str, new_table: pd.DataFrame,
                          position: int) -> Tuple[str, pd.DataFrame, str]:
    """
    Resolve conflict when two tables compete for same position

    Uses quality scoring to choose the better table.

    Args:
        existing_name: Name of existing table
        existing_table: Existing DataFrame
        new_name: Name of new competing table
        new_table: New competing DataFrame
        position: Table position/index

    Returns:
        Tuple of (chosen_name, chosen_table, rejected_name)
    """
    existing_markers = UNIQUE_MARKERS.get(existing_name, [])
    new_markers = UNIQUE_MARKERS.get(new_name, [])

    # Evaluate quality of both tables
    existing_score = score_table_quality(existing_table, existing_name, existing_markers)
    new_score = score_table_quality(new_table, new_name, new_markers)

    print(f"🔍 Конфликт позиции #{position}: {existing_name}({existing_score:.0f}) vs {new_name}({new_score:.0f})")

    # Choose table with better score
    if new_score > existing_score:
        print(f"✅ Выбрана {new_name} (лучшая оценка)")
        return new_name, new_table, existing_name
    else:
        print(f"✅ Оставлена {existing_name} (лучшая оценка)")
        return existing_name, existing_table, new_name


def analyze_all_tables(all_page_tables: List[pd.DataFrame]) -> None:
    """
    Output detailed information about all tables for diagnostics

    Prints table sizes, columns, and possible types for debugging purposes.

    Args:
        all_page_tables: List of all tables to analyze
    """
    print(f"\n🔍 ДИАГНОСТИКА: Анализ всех {len(all_page_tables)} таблиц на странице:")

    for i, table in enumerate(all_page_tables):
        if len(table) < 5:  # Skip very small tables
            continue

        print(f"\n=== Таблица #{i} ===")
        print(f"Размер: {len(table)} строк × {len(table.columns)} колонок")

        # Show first 10 columns
        cols = []
        for col in table.columns[:10]:
            if isinstance(col, tuple):
                clean_col = '_'.join([str(c) for c in col if str(c) != 'nan' and str(c).strip()])
                cols.append(clean_col)
            else:
                cols.append(str(col))

        print(f"Первые колонки: {cols}")

        # Analyze possible table type
        cols_str = str(table.columns).lower()
        possible_types = []

        if ('season' in cols_str or 'squad' in cols_str) and 'gls' in cols_str and 'ast' in cols_str:
            possible_types.append("STANDARD")
        if 'shooting' in cols_str or ('sh' in cols_str and 'sot' in cols_str):
            possible_types.append("SHOOTING")
        if 'passing' in cols_str or ('cmp' in cols_str and 'att' in cols_str):
            possible_types.append("PASSING")
        if 'pass types' in cols_str or 'live' in cols_str:
            possible_types.append("PASS_TYPES")
        if any(marker in cols_str for marker in ['gca', 'sca', 'goal creation', 'shot creation', 'gca90', 'sca90', 'passlive', 'passdead']):
            possible_types.append("GCA/SCA")
        if 'defense' in cols_str or 'tkl' in cols_str:
            possible_types.append("DEFENSE")
        if any(marker in cols_str for marker in ['possession', 'touches', 'carries', 'take-ons', 'dribbles', 'targ', 'succ', 'tkld', 'totdist', 'prgdist']):
            possible_types.append("POSSESSION")
        if 'playing time' in cols_str or 'starts' in cols_str:
            possible_types.append("PLAYING_TIME")
        if any(marker in cols_str for marker in ['misc', 'fls', 'fld', 'off', 'crs', 'tklw', 'pkwon', 'pkcon', 'og', 'recov', 'aerial', 'won', 'lost']):
            possible_types.append("MISCELLANEOUS")

        if possible_types:
            print(f"Возможный тип: {', '.join(possible_types)}")
        else:
            print("Тип: НЕОПОЗНАННАЯ")
