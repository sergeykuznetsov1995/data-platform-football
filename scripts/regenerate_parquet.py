#!/usr/bin/env python3
"""
Script to regenerate Parquet files with consistent schema.

This script reads CSV files from data/premier_league/ and creates Parquet files
with explicitly defined data types to ensure schema consistency across all partitions.

Problem being solved:
- Different teams have inconsistent data types (int64 vs double) for columns like 'mp', 'age'
- This causes Trino errors when querying across partitions without filters

Solution:
- Explicitly cast all numeric columns to DOUBLE
- Cast integer columns (age, mp, starts, subs) to their correct types with NaN handling
"""

import os
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path


# Paths
BASE_DIR = Path("/root/data_platform")
CSV_DIR = BASE_DIR / "data" / "premier_league"
PARQUET_DIR = BASE_DIR / "data" / "parquet"

# Schema definition matching DDL in sql/create_tables.sql
# Columns that should be INTEGER (will be cast with NaN → 0)
INTEGER_COLUMNS = ['age']

# Columns that should be DOUBLE (can have NaN)
DOUBLE_COLUMNS = [
    'mp', 'starts_total', 'subs_on',  # These were causing issues
    # All other numeric columns are already DOUBLE in DDL
]

# All columns that exist in field player CSVs (from DDL)
ALL_NUMERIC_COLUMNS = [
    'performance_gls', 'performance_ast', 'performance_goals_plus_assists',
    'performance_goals_minus_penalties', 'performance_pk', 'performance_pkatt',
    'performance_crdy', 'performance_crdr', 'expected_xg', 'expected_npxg',
    'expected_xag', 'expected_npxg_plus_xag', 'progression_prgc', 'progression_prgp',
    'progression_prgr', 'per_90_gls', 'per_90_ast', 'per_90_goals_plus_assists',
    'per_90_goals_minus_penalties', 'per_90_goals_plus_assists_minus_pk',
    'per_90_xg', 'per_90_xag', 'per_90_xg_plus_xag', 'per_90_npxg',
    'per_90_npxg_plus_xag', 'minutes', 'minutes_per_match', 'minutes_pct',
    'minutes_90', 'minutes_per_start', 'matches_completed', 'minutes_per_sub',
    'subs_unused', 'team_points_per_match', 'team_goals_for', 'team_goals_against',
    'team_goal_diff', 'team_goal_diff_per90', 'team_on_off', 'team_xg_for',
    'team_xg_against', 'team_xg_diff', 'team_xg_diff_per90', 'team_xg_on_off',
    # Add more columns as needed
]


def normalize_team_name(team_name: str) -> str:
    """Normalize team name to match partition format."""
    return team_name.lower().replace(' ', '_').replace('-', '_')


def apply_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Apply consistent schema to DataFrame."""

    # Handle INTEGER columns (age) - fill NaN with 0, then cast to int64
    for col in INTEGER_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')

    # Handle DOUBLE columns that were causing issues - keep as float64
    for col in DOUBLE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')

    # All other numeric columns should be float64 (DOUBLE)
    for col in df.columns:
        if col not in INTEGER_COLUMNS and col not in ['season', 'squad', 'country',
                                                       'competition', 'player_name', 'team']:
            if df[col].dtype == 'object':
                # Try to convert string columns to numeric
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    pass

            # Convert any remaining int columns to float to handle NaN
            if df[col].dtype in ['int64', 'int32']:
                # Check if there might be NaN in other rows when concatenating
                df[col] = df[col].astype('float64')

    return df


def process_team(team_dir: Path, output_dir: Path) -> dict:
    """Process all CSV files for a team and create Parquet."""

    team_name = team_dir.name
    normalized_name = normalize_team_name(team_name)

    result = {
        'team': normalized_name,
        'csv_files': 0,
        'rows': 0,
        'status': 'success',
        'error': None
    }

    # CSV files are in field_players/ subdirectory
    field_players_dir = team_dir / "field_players"
    if not field_players_dir.exists():
        result['status'] = 'skipped'
        result['error'] = 'No field_players directory found'
        return result

    # Find all CSV files in field_players subdirectory
    field_player_files = list(field_players_dir.glob("*.csv"))

    if not field_player_files:
        result['status'] = 'skipped'
        result['error'] = 'No field player CSV files found'
        return result

    result['csv_files'] = len(field_player_files)

    # Read and combine all CSVs
    dfs = []
    for csv_file in field_player_files:
        try:
            df = pd.read_csv(csv_file)
            # Extract player name from filename
            player_name = csv_file.stem.replace('_', ' ').title()
            df['player_name'] = player_name
            df['team'] = normalized_name
            dfs.append(df)
        except Exception as e:
            print(f"  Warning: Failed to read {csv_file.name}: {e}")

    if not dfs:
        result['status'] = 'error'
        result['error'] = 'All CSV files failed to read'
        return result

    # Combine DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)

    # Apply schema transformations
    combined_df = apply_schema(combined_df)

    result['rows'] = len(combined_df)

    # Create output directory
    parquet_dir = output_dir / 'field_players' / f'team={normalized_name}'
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Write Parquet with explicit schema
    output_path = parquet_dir / 'data.parquet'
    combined_df.to_parquet(output_path, index=False, compression='snappy')

    print(f"  Created: {output_path}")
    print(f"  Rows: {len(combined_df)}, Columns: {len(combined_df.columns)}")

    return result


def main():
    print("=" * 60)
    print("Parquet Regeneration Script")
    print("=" * 60)
    print(f"CSV source: {CSV_DIR}")
    print(f"Parquet output: {PARQUET_DIR}")
    print()

    if not CSV_DIR.exists():
        print(f"ERROR: CSV directory not found: {CSV_DIR}")
        sys.exit(1)

    # Get all team directories
    team_dirs = [d for d in CSV_DIR.iterdir() if d.is_dir()]

    if not team_dirs:
        print("ERROR: No team directories found")
        sys.exit(1)

    print(f"Found {len(team_dirs)} teams to process")
    print()

    results = []

    for team_dir in sorted(team_dirs):
        print(f"Processing: {team_dir.name}")
        result = process_team(team_dir, PARQUET_DIR)
        results.append(result)

        if result['status'] == 'success':
            print(f"  Status: OK ({result['csv_files']} files, {result['rows']} rows)")
        else:
            print(f"  Status: {result['status']} - {result['error']}")
        print()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)

    success = len([r for r in results if r['status'] == 'success'])
    skipped = len([r for r in results if r['status'] == 'skipped'])
    errors = len([r for r in results if r['status'] == 'error'])

    print(f"Total teams: {len(results)}")
    print(f"Success: {success}")
    print(f"Skipped: {skipped}")
    print(f"Errors: {errors}")

    total_rows = sum(r['rows'] for r in results)
    print(f"Total rows: {total_rows}")

    print()
    print("Next steps:")
    print("1. Recreate Trino table (as user 'trino'):")
    print("   DROP TABLE IF EXISTS hive.premier_league.field_players;")
    print("   -- Execute CREATE TABLE from sql/create_tables.sql")
    print("2. Sync partitions:")
    print("   CALL system.sync_partition_metadata('premier_league', 'field_players', 'FULL');")
    print("3. Test query:")
    print("   SELECT * FROM hive.premier_league.field_players LIMIT 10;")


if __name__ == "__main__":
    main()
