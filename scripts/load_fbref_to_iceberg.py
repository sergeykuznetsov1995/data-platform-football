#!/usr/bin/env python3
"""
Load FBref Parquet files into Iceberg tables via Trino.
"""

import os
import subprocess
import pandas as pd
from pathlib import Path


BRONZE_DIR = Path('/data/bronze/fbref')
TRINO_CMD = ['trino', '--execute']

# Mapping of parquet files to table names
PARQUET_TO_TABLE = {
    # Player stats
    'player_stats.parquet': 'fbref_player_stats',
    'player_shooting.parquet': 'fbref_player_shooting',
    'player_passing.parquet': 'fbref_player_passing',
    'player_passing_types.parquet': 'fbref_player_passing_types',
    'player_gca.parquet': 'fbref_player_gca',
    'player_defense.parquet': 'fbref_player_defense',
    'player_possession.parquet': 'fbref_player_possession',
    'player_playingtime.parquet': 'fbref_player_playingtime',
    'player_misc.parquet': 'fbref_player_misc',
    # Team stats
    'team_stats.parquet': 'fbref_team_stats',
    'team_shooting.parquet': 'fbref_team_shooting',
    'team_passing.parquet': 'fbref_team_passing',
    'team_passing_types.parquet': 'fbref_team_passing_types',
    'team_gca.parquet': 'fbref_team_gca',
    'team_defense.parquet': 'fbref_team_defense',
    'team_possession.parquet': 'fbref_team_possession',
    'team_playingtime.parquet': 'fbref_team_playingtime',
    'team_misc.parquet': 'fbref_team_misc',
    # Keeper stats
    'keeper_keeper.parquet': 'fbref_keeper_stats',
    'keeper_keeper_adv.parquet': 'fbref_keeper_adv',
    # Schedule
    'schedule.parquet': 'fbref_schedule',
}


def get_trino_type(dtype) -> str:
    """Convert pandas dtype to Trino type."""
    dtype_str = str(dtype)
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DOUBLE'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP'
    else:
        return 'VARCHAR'


def sanitize_column_name(col: str) -> str:
    """Sanitize column name for SQL."""
    # Replace special characters
    col = col.lower()
    col = col.replace(' ', '_')
    col = col.replace('-', '_')
    col = col.replace('+', '_plus_')
    col = col.replace('/', '_per_')
    col = col.replace('%', '_pct')
    col = col.replace('.', '_')
    col = col.replace('(', '_')
    col = col.replace(')', '')
    # Ensure starts with letter
    if col[0].isdigit():
        col = 'col_' + col
    return col


def escape_value(val) -> str:
    """Escape value for SQL."""
    if pd.isna(val):
        return 'NULL'
    if isinstance(val, str):
        # Escape single quotes
        val = val.replace("'", "''")
        return f"'{val}'"
    if isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    return str(val)


def create_table_sql(table_name: str, df: pd.DataFrame) -> str:
    """Generate CREATE TABLE SQL."""
    columns = []
    for col in df.columns:
        safe_col = sanitize_column_name(col)
        col_type = get_trino_type(df[col].dtype)
        columns.append(f'    "{safe_col}" {col_type}')

    columns_sql = ',\n'.join(columns)
    return f"""
CREATE TABLE IF NOT EXISTS iceberg.bronze.{table_name} (
{columns_sql}
)
"""


def insert_values_sql(table_name: str, df: pd.DataFrame, batch_size: int = 100) -> list:
    """Generate INSERT SQL statements in batches."""
    safe_cols = [sanitize_column_name(c) for c in df.columns]
    cols_sql = ', '.join(f'"{c}"' for c in safe_cols)

    statements = []
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        values = []
        for _, row in batch.iterrows():
            row_vals = ', '.join(escape_value(v) for v in row)
            values.append(f'({row_vals})')

        values_sql = ',\n'.join(values)
        stmt = f"INSERT INTO iceberg.bronze.{table_name} ({cols_sql}) VALUES\n{values_sql}"
        statements.append(stmt)

    return statements


def run_trino(sql: str) -> bool:
    """Execute SQL via Trino CLI."""
    try:
        result = subprocess.run(
            TRINO_CMD + [sql],
            capture_output=True,
            text=True,
            timeout=300
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return False
        return True
    except Exception as e:
        print(f"Exception: {e}")
        return False


def load_parquet_to_iceberg(parquet_file: Path, table_name: str) -> bool:
    """Load a parquet file into Iceberg table."""
    print(f"\n{'='*60}")
    print(f"Loading {parquet_file.name} -> iceberg.bronze.{table_name}")
    print('='*60)

    # Read parquet
    df = pd.read_parquet(parquet_file)
    print(f"Read {len(df)} rows, {len(df.columns)} columns")

    if df.empty:
        print("Empty dataframe, skipping")
        return True

    # Drop table if exists (for clean reload)
    drop_sql = f"DROP TABLE IF EXISTS iceberg.bronze.{table_name}"
    print(f"Dropping existing table...")
    run_trino(drop_sql)

    # Create table
    create_sql = create_table_sql(table_name, df)
    print(f"Creating table...")
    if not run_trino(create_sql):
        print("Failed to create table")
        return False

    # Insert data in batches
    print(f"Inserting {len(df)} rows...")
    insert_stmts = insert_values_sql(table_name, df, batch_size=50)
    for i, stmt in enumerate(insert_stmts):
        if not run_trino(stmt):
            print(f"Failed to insert batch {i+1}/{len(insert_stmts)}")
            return False
        print(f"  Batch {i+1}/{len(insert_stmts)} complete")

    print(f"✓ Successfully loaded {len(df)} rows into {table_name}")
    return True


def main():
    """Main entry point."""
    print("FBref Parquet to Iceberg Loader")
    print("================================\n")

    if not BRONZE_DIR.exists():
        print(f"Error: Directory {BRONZE_DIR} not found")
        return 1

    # Find all parquet files
    parquet_files = list(BRONZE_DIR.glob('*.parquet'))
    print(f"Found {len(parquet_files)} parquet files\n")

    success_count = 0
    fail_count = 0

    for pf in sorted(parquet_files):
        table_name = PARQUET_TO_TABLE.get(pf.name)
        if not table_name:
            print(f"Skipping {pf.name} (no mapping)")
            continue

        if load_parquet_to_iceberg(pf, table_name):
            success_count += 1
        else:
            fail_count += 1

    print(f"\n{'='*60}")
    print(f"SUMMARY: {success_count} success, {fail_count} failed")
    print('='*60)

    return 0 if fail_count == 0 else 1


if __name__ == '__main__':
    exit(main())
