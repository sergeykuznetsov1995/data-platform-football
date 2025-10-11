"""
Column processing utilities for FBref statistics

This module provides functions for:
- Fixing problematic column names (Unnamed:)
- Processing MultiIndex columns
- Converting column names to snake_case
- Applying parser-specific renames
"""

import pandas as pd
import re
from typing import List

from ..constants import (
    FIELD_PLAYER_BASIC_RENAMES,
    FIELD_PLAYER_PLAYING_TIME_RENAMES,
    FIELD_PLAYER_SUFFIX_MAP,
    DUPLICATE_90S_COLUMNS,
    GOALKEEPER_BASIC_RENAMES,
    SNAKE_CASE_REPLACEMENTS
)


def fix_column_names(columns: List) -> List[str]:
    """
    Fix problematic column names by removing Unnamed: prefixes

    Handles "Unnamed: X_level_0_" patterns from pandas MultiIndex.

    Args:
        columns: List of column names

    Returns:
        List of cleaned column names
    """
    new_columns = []

    for col in columns:
        col_str = str(col)

        # Process columns with "Unnamed: X_level_0_"
        if col_str.startswith('Unnamed:') and '_level_0_' in col_str:
            # Extract last part after last underscore
            clean_name = col_str.split('_')[-1]
            new_columns.append(clean_name)
        else:
            # Keep other columns as is
            new_columns.append(col_str)

    return new_columns


def process_multiindex_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process MultiIndex columns by flattening them

    Joins tuple column names into single strings with underscores.

    Args:
        df: DataFrame with MultiIndex columns

    Returns:
        DataFrame with flattened column names
    """
    if isinstance(df.columns, pd.MultiIndex):
        new_columns = []
        for col in df.columns:
            if isinstance(col, tuple):
                # Join tuple levels
                clean_col = '_'.join([str(c) for c in col if str(c) != 'nan' and str(c).strip()])
                # Remove multiple underscores
                clean_col = re.sub(r'_+', '_', clean_col).strip('_')
            else:
                clean_col = str(col)
            new_columns.append(clean_col)
        df.columns = new_columns

    return df


def convert_to_snake_case(column_name: str) -> str:
    """
    Convert column names to full snake_case with special character replacement

    Args:
        column_name: Column name to convert

    Returns:
        Column name in snake_case
    """
    col = str(column_name)

    # Replace special characters with descriptive words
    col = col.replace('%', '_pct')
    col = col.replace('+', '_plus_')
    col = col.replace('-', '_minus_')
    col = col.replace('/', '_per_')
    col = col.replace('(', '_')
    col = col.replace(')', '_')
    col = col.replace(' ', '_')
    col = col.replace('&', '_and_')
    col = col.replace('#', '_num_')

    # Remove multiple underscores
    col = re.sub(r'_+', '_', col)

    # Remove underscores at beginning and end
    col = col.strip('_')

    # Convert to lowercase
    col = col.lower()

    # Apply special replacements for readability
    for old, new in SNAKE_CASE_REPLACEMENTS.items():
        col = col.replace(old, new)

    return col


def apply_field_player_renames(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply field player-specific column renames and transformations

    Includes:
    - Removing duplicate 90s columns
    - Basic column renames
    - Playing Time column renames
    - Table suffix abbreviations
    - Full snake_case conversion

    Args:
        df: DataFrame to process

    Returns:
        DataFrame with renamed columns
    """
    print("\n🧹 Начинаю пост-обработку данных...")

    # 1. Remove duplicate 90s columns
    print("   Удаляю дублирующиеся столбцы 90s...")
    duplicate_90s_cols = [col for col in df.columns if col in DUPLICATE_90S_COLUMNS]
    df = df.drop(columns=duplicate_90s_cols)
    print(f"   Удалено {len(duplicate_90s_cols)} дублирующихся столбцов 90s")

    # 2. Rename basic columns to snake_case
    print("   Переименовываю основные столбцы...")
    df = df.rename(columns=FIELD_PLAYER_BASIC_RENAMES)

    # 3. Rename Playing Time columns
    print("   Переименовываю Playing Time столбцы...")
    existing_pt_renames = {old: new for old, new in FIELD_PLAYER_PLAYING_TIME_RENAMES.items() if old in df.columns}
    df = df.rename(columns=existing_pt_renames)
    print(f"   Переименовано {len(existing_pt_renames)} Playing Time столбцов")

    # 4. Shorten table suffixes
    print("   Сокращаю суффиксы таблиц...")
    new_columns = []
    for col in df.columns:
        new_col = col
        for old_suffix, new_suffix in FIELD_PLAYER_SUFFIX_MAP.items():
            if col.endswith(old_suffix):
                new_col = col.replace(old_suffix, new_suffix)
                break
        new_columns.append(new_col)
    df.columns = new_columns

    # 5. Full conversion to snake_case
    print("   Конвертирую все названия столбцов в snake_case...")
    new_column_names = [convert_to_snake_case(col) for col in df.columns]
    df.columns = new_column_names
    print(f"   Конвертировано {len(df.columns)} названий столбцов в snake_case")

    print(f"✅ Пост-обработка завершена! Итоговый размер: {df.shape[0]} строк × {df.shape[1]} столбцов")
    return df


def apply_goalkeeper_renames(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply goalkeeper-specific column renames and transformations

    Includes:
    - Goalkeeper-specific column renames
    - Full snake_case conversion
    - Duplicate column removal
    - Squad name cleaning

    Args:
        df: DataFrame to process

    Returns:
        DataFrame with renamed columns
    """
    print("\n🧹 Начинаю пост-обработку данных вратаря...")

    # Apply goalkeeper-specific renames
    existing_renames = {old: new for old, new in GOALKEEPER_BASIC_RENAMES.items() if old in df.columns}
    df = df.rename(columns=existing_renames)
    print(f"   Переименовано {len(existing_renames)} столбцов")

    # Convert to snake_case for remaining columns
    new_columns = [convert_to_snake_case(col) for col in df.columns]
    df.columns = new_columns

    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    # Remove duplicate playing_time_mp if matches_played exists
    if 'matches_played' in df.columns and 'playing_time_mp' in df.columns:
        df = df.drop(columns=['playing_time_mp'])
        print(f"   Удален дубликат playing_time_mp (оставлен matches_played)")

    # Clean data in columns
    for col in df.columns:
        if df[col].dtype == 'object':
            # Remove country codes from squad names (e.g., "eng Arsenal" -> "Arsenal")
            if 'squad' in col.lower():
                df[col] = df[col].astype(str).str.replace(r'^[a-z]{2,3}\s+', '', regex=True)

    print(f"🎯 Пост-обработка завершена. Итоговых столбцов: {len(df.columns)}")
    return df
