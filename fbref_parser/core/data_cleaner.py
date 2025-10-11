"""
Data cleaning utilities for FBref statistics

This module provides functions for cleaning DataFrames including:
- Removing aggregated rows
- Removing duplicate columns
- Cleaning country and competition values
- Removing Playing Time duplicates from non-Playing Time tables
"""

import pandas as pd
import re
from typing import List

from ..constants import PLAYING_TIME_PATTERNS


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame from garbage data

    Removes:
    - Matches columns
    - Aggregated summary rows
    - Rows with column names as values
    - Empty rows

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        return df

    # Remove Matches columns
    matches_cols = [col for col in df.columns if 'Matches' in str(col) or 'matches' in str(col).lower()]
    df = df.drop(columns=matches_cols, errors='ignore')

    # Find season column
    season_cols = [col for col in df.columns if 'Season' in str(col)]
    if season_cols:
        season_col = season_cols[0]

        # Remove only obviously aggregated rows
        # DO NOT remove rows with actual tournament data
        df = df[~df[season_col].astype(str).str.contains('Season|Seasons|Club|Clubs|Total|League', na=False)]

        # DO NOT apply strict filtering by season format
        # Keep ALL rows with specific seasons and tournaments

    # Additional check to remove rows with column names
    # Find tournament/competition column
    comp_cols = [col for col in df.columns if 'Comp' in str(col) and 'Competition' not in str(col)]
    if comp_cols:
        comp_col = comp_cols[0]
        # Remove rows where tournament column contains service words
        df = df[~df[comp_col].astype(str).str.contains('Comp|Competition|Country|Squad|MP|Min', na=False)]

    # Remove completely empty rows
    df = df.dropna(how='all')

    return df


def clean_aggregated_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove aggregated rows from data (extended version)

    Identifies and removes rows where the first column is empty
    and the fourth column contains "Country" (sign of garbage row).

    Args:
        df: DataFrame to clean

    Returns:
        DataFrame with aggregated rows removed
    """
    if df.empty:
        return df

    # Get first column (usually Season or empty)
    first_col = df.iloc[:, 0]

    # Remove rows where first column is empty or contains only spaces
    # and simultaneously fourth column contains "Country" (sign of garbage row)
    if len(df.columns) >= 4:
        fourth_col = df.iloc[:, 3]  # Country column

        # Find row indices to remove
        rows_to_drop = []
        for i, (first_val, fourth_val) in enumerate(zip(first_col, fourth_col)):
            first_str = str(first_val).strip()
            fourth_str = str(fourth_val).strip()

            # If first column is empty AND fourth contains "Country"
            if (first_str == '' or first_str == 'nan') and fourth_str == 'Country':
                rows_to_drop.append(i)

        # Remove found rows
        df = df.drop(rows_to_drop)

    return df


def clean_country_column(df: pd.DataFrame, country_col: str = 'country') -> pd.DataFrame:
    """
    Clean country column values

    Removes prefixes like "eng ENG" -> "ENG"

    Args:
        df: DataFrame to clean
        country_col: Name of country column

    Returns:
        DataFrame with cleaned country column
    """
    if country_col in df.columns:
        df[country_col] = df[country_col].astype(str).str.replace(r'^[a-z]+ ', '', regex=True)
        df[country_col] = df[country_col].replace('nan', '')

    return df


def clean_competition_column(df: pd.DataFrame, comp_col: str = 'competition') -> pd.DataFrame:
    """
    Clean competition column values

    Removes league numbers like "1. Ligue 1" -> "Ligue 1"
    and performs additional cleanup.

    Args:
        df: DataFrame to clean
        comp_col: Name of competition column

    Returns:
        DataFrame with cleaned competition column
    """
    if comp_col in df.columns:
        df[comp_col] = df[comp_col].astype(str).str.replace(r'^\d+\. ', '', regex=True)
        # Additional cleanup
        df[comp_col] = df[comp_col].str.replace('Jr. PL2 — Div. 1', 'PL2 Div 1')

    return df


def remove_duplicate_columns(df: pd.DataFrame, suffix: str = '_dup') -> pd.DataFrame:
    """
    Remove duplicate columns after merging

    Removes columns with specified suffix and completely identical columns.

    Args:
        df: DataFrame to clean
        suffix: Suffix of duplicate columns to remove

    Returns:
        DataFrame with duplicates removed
    """
    # Remove columns with suffix (anywhere in column name)
    dup_columns = [col for col in df.columns if suffix in str(col)]
    if dup_columns:
        df = df.drop(columns=dup_columns, errors='ignore')
        print(f"   Удалено {len(dup_columns)} дублирующихся столбцов содержащих {suffix}")

    # Remove completely identical columns
    df = df.loc[:, ~df.columns.duplicated()]

    return df


def remove_playing_time_duplicates(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Remove Playing Time columns from non-Playing Time tables

    Uses PLAYING_TIME_PATTERNS to identify and remove duplicate columns.

    Args:
        df: DataFrame to clean
        table_name: Name of the table (e.g., 'shooting', 'passing')

    Returns:
        DataFrame with Playing Time duplicates removed
    """
    if table_name == 'playing_time':
        return df  # Don't remove from playing_time table itself

    # Find columns to drop using patterns
    cols_to_drop = []
    for col in df.columns:
        col_str = str(col)
        # Check each pattern
        if any(re.search(pattern, col_str, re.IGNORECASE) for pattern in PLAYING_TIME_PATTERNS):
            cols_to_drop.append(col)

    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        print(f"   Удалено {len(cols_to_drop)} столбцов Playing Time из {table_name}: {cols_to_drop}")

    return df
