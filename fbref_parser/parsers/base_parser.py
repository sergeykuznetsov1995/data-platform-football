"""
Base parser abstract class for FBref statistics

This module provides an abstract base class that implements shared logic
for both field player and goalkeeper parsers.
"""

import pandas as pd
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

from ..core.column_processor import (
    fix_column_names,
    process_multiindex_columns
)
from ..core.data_cleaner import (
    clean_dataframe,
    clean_country_column,
    clean_competition_column,
    remove_duplicate_columns,
    remove_playing_time_duplicates
)
from ..constants import KEY_COLUMNS, KEY_COLUMNS_WITH_MP


class BaseParser(ABC):
    """
    Abstract base class for FBref parsers

    Provides shared logic for processing tables, merging data,
    and final cleanup. Subclasses must implement parser-specific methods.
    """

    def __init__(self):
        """Initialize base parser"""
        pass

    @abstractmethod
    def identify_tables(self, all_tables: List[pd.DataFrame]) -> Dict:
        """
        Identify and classify tables (must be implemented by subclass)

        Args:
            all_tables: List of all tables from page

        Returns:
            Dictionary with identified tables
        """
        pass

    @abstractmethod
    def apply_specific_renames(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply parser-specific column renames (must be implemented by subclass)

        Args:
            df: DataFrame to process

        Returns:
            DataFrame with renamed columns
        """
        pass

    @abstractmethod
    def get_no_prefix_tables(self) -> List[str]:
        """
        Get list of table types that should not receive prefixes

        Returns:
            List of table type names
        """
        pass

    def process_table_columns(self, table: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Process table columns: MultiIndex, fix names, add prefixes

        Args:
            table: DataFrame to process
            table_name: Name of the table type

        Returns:
            Processed DataFrame
        """
        # Process MultiIndex columns
        table = process_multiindex_columns(table)

        # Fix problematic column names
        table.columns = fix_column_names(table.columns)

        # Remove Playing Time duplicates from non-Playing Time tables
        table = remove_playing_time_duplicates(table, table_name)

        # Add prefix to columns (except key columns)
        no_prefix_tables = self.get_no_prefix_tables()
        new_columns = []

        for col in table.columns:
            col_str = str(col)
            # Check if column is a key column
            is_key = any(key in col_str for key in KEY_COLUMNS_WITH_MP)

            # Tables in no_prefix_tables don't get prefixes
            if is_key or table_name in no_prefix_tables:
                new_columns.append(col_str)
            else:
                new_columns.append(f"{col_str}_{table_name}")

        table.columns = new_columns

        # Clean dataframe
        table = clean_dataframe(table)

        return table

    def merge_tables(self, processed_tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Merge all processed tables using common keys

        Args:
            processed_tables: Dictionary of table_name -> DataFrame

        Returns:
            Merged DataFrame
        """
        print(f"\n🔗 Объединяю {len(processed_tables)} таблиц...")

        # Start with standard table as base (or first available)
        if 'standard' in processed_tables:
            merged_df = processed_tables['standard'].copy()
            print(f"Базовая таблица (standard): {merged_df.shape}")
        else:
            # Use first available table
            first_key = list(processed_tables.keys())[0]
            merged_df = processed_tables[first_key].copy()
            print(f"Базовая таблица ({first_key}): {merged_df.shape}")

        # Determine key columns for merging
        merge_keys = []
        for col in merged_df.columns:
            col_lower = str(col).lower()
            if any(key in col_lower for key in ['season', 'squad', 'comp']):
                merge_keys.append(col)

        if not merge_keys:
            print("⚠️ Не найдены ключевые колонки для объединения, используем индекс")
            merge_keys = None
        else:
            print(f"Ключевые колонки для объединения: {merge_keys}")

        # Merge remaining tables
        for table_name, table_df in processed_tables.items():
            if table_name == 'standard' or (table_name == list(processed_tables.keys())[0]):
                continue

            print(f"Объединяю с {table_name}: {table_df.shape}")

            if merge_keys:
                # Check which merge keys are available in both tables
                available_keys = [key for key in merge_keys if key in merged_df.columns and key in table_df.columns]

                if available_keys:
                    try:
                        merged_df = pd.merge(merged_df, table_df, on=available_keys, how='left', suffixes=('', f'_dup_{table_name}'))
                    except Exception as e:
                        print(f"⚠️ Ошибка при объединении {table_name} по ключам {available_keys}: {e}")
                        print(f"   Fallback: объединяю по индексу")
                        merged_df = pd.concat([merged_df, table_df], axis=1)
                else:
                    print(f"⚠️ Нет общих ключевых колонок с {table_name}, объединяю по индексу")
                    merged_df = pd.concat([merged_df, table_df], axis=1)
            else:
                merged_df = pd.concat([merged_df, table_df], axis=1)

            print(f"Размер после объединения: {merged_df.shape}")

        return merged_df

    def final_cleanup(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform final cleanup: remove duplicates, clean data values

        Args:
            df: DataFrame to clean

        Returns:
            Cleaned DataFrame
        """
        print("\n🧹 Финальная очистка данных...")

        # Remove duplicate columns with _dup_ suffix
        df = remove_duplicate_columns(df, suffix='_dup_')

        # Clean country column
        df = clean_country_column(df)

        # Clean competition column
        df = clean_competition_column(df)

        return df

    def save_to_csv(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Save DataFrame to CSV file

        Args:
            df: DataFrame to save
            output_path: Output file path
        """
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"\n✅ Парсинг завершен успешно!")
        print(f"📊 Результат: {df.shape[0]} строк × {df.shape[1]} столбцов")
        print(f"💾 Файл сохранен: {output_path}")
