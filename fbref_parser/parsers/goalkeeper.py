"""
Goalkeeper parser for FBref statistics

This module implements the GoalkeeperParser class for parsing goalkeeper
statistics from FBref.com, including goalkeeper-specific tables and all field player stats.
"""

import pandas as pd
import time
from io import StringIO
from typing import Dict, List, Optional

from .base_parser import BaseParser
from ..core.scraper import FBrefScraper
from ..core.table_detector import identify_goalkeeper_tables
from ..core.column_processor import apply_goalkeeper_renames
from ..utils.file_helpers import get_output_path, normalize_name
from ..utils.squad_helpers import extract_goalkeeper_links
from ..constants import DEFAULT_OUTPUT_DIR_GOALKEEPERS


class GoalkeeperParser(BaseParser):
    """
    Parser for goalkeeper statistics from FBref

    Handles parsing of individual goalkeepers or entire squads,
    including goalkeeper-specific stats (saves, clean sheets, PSxG)
    and all field player statistics.
    """

    def __init__(self):
        """Initialize goalkeeper parser"""
        super().__init__()
        self.scraper = FBrefScraper()

    def identify_tables(self, all_tables: List[pd.DataFrame]) -> Dict:
        """
        Identify goalkeeper tables including GK-specific categories

        Args:
            all_tables: List of all tables from page

        Returns:
            Dictionary mapping table type to list of table info
        """
        return identify_goalkeeper_tables(all_tables)

    def apply_specific_renames(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply goalkeeper-specific column renames

        Args:
            df: DataFrame to process

        Returns:
            DataFrame with goalkeeper renames applied
        """
        return apply_goalkeeper_renames(df)

    def get_no_prefix_tables(self) -> List[str]:
        """
        Get list of tables that should not receive column prefixes

        For goalkeepers, standard, goalkeeping, and advanced_goalkeeping
        tables don't get prefixes.

        Returns:
            List of table types without prefixes
        """
        return ['standard', 'goalkeeping', 'advanced_goalkeeping']

    def parse_goalkeeper(self, player_name: str, player_url: str,
                        output_path: str = None) -> Optional[pd.DataFrame]:
        """
        Parse individual goalkeeper statistics

        Args:
            player_name: Goalkeeper name
            player_url: URL of the goalkeeper's all_comps page
            output_path: Custom output path (optional)

        Returns:
            DataFrame with parsed statistics, or None if parsing failed
        """
        print(f"\n🥅 Парсинг {player_name}...")
        print(f"🔗 URL: {player_url}")

        try:
            response = self.scraper.fetch_page(player_url)
            time.sleep(1)  # Respectful delay for server

            # Read all tables from page
            all_tables = pd.read_html(StringIO(response.text), encoding='utf-8')
            print(f"📊 Найдено {len(all_tables)} таблиц на странице")

            # Identify goalkeeper-specific tables
            identified_tables = self.identify_tables(all_tables)

            # Output info about found tables
            for table_type, tables in identified_tables.items():
                if tables:
                    print(f"   {table_type}: {len(tables)} таблиц")

            # Merge all found data
            all_data = []

            for table_type, tables in identified_tables.items():
                if not tables:
                    continue

                # Choose best table for each type (with most matches)
                best_table = max(tables, key=lambda x: x['matches'])
                table_data = best_table['table'].copy()

                print(f"   Обрабатываю {table_type} (таблица {best_table['index']})...")

                # Process table columns
                processed_table = self.process_table_columns(table_data, table_type)

                if not processed_table.empty:
                    all_data.append(processed_table)

            if not all_data:
                print("❌ Не найдены данные для вратаря")
                return None

            # Merge all tables
            print("🔗 Объединяю таблицы...")

            # Key columns for merging
            key_columns = ['Season', 'Age', 'Squad', 'Country', 'Comp']

            # Start with first table
            merged_data = all_data[0]

            # Merge remaining tables
            for i in range(1, len(all_data)):
                try:
                    # Find common key columns
                    key_columns_present = [col for col in key_columns if col in merged_data.columns and col in all_data[i].columns]

                    if key_columns_present:
                        # Merge by key columns with automatic suffixes
                        merged_data = pd.merge(
                            merged_data,
                            all_data[i],
                            on=key_columns_present,
                            how='outer',
                            suffixes=('', '_dup')
                        )
                        print(f"   Объединил таблицу {i+1} по ключам: {key_columns_present}")
                    else:
                        # If no common keys, concatenate by indices
                        merged_data = pd.concat([merged_data, all_data[i]], axis=1)
                        print(f"   Конкатенировал таблицу {i+1} по индексам")

                except Exception as e:
                    print(f"   ⚠️ Не удалось объединить таблицу {i+1}: {e}")
                    try:
                        merged_data = pd.concat([merged_data, all_data[i]], axis=1)
                        print(f"   Использовал конкатенацию для таблицы {i+1}")
                    except Exception as e2:
                        print(f"   ❌ Полностью не удалось добавить таблицу {i+1}: {e2}")
                        continue

            # Remove duplicate columns after merging
            print("   Удаляю дублирующиеся столбцы...")

            # Remove columns with _dup suffix
            dup_columns = [col for col in merged_data.columns if str(col).endswith('_dup')]
            if dup_columns:
                merged_data = merged_data.drop(columns=dup_columns, errors='ignore')
                print(f"   Удалено {len(dup_columns)} дублирующихся столбцов с суффиксом _dup")

            # Remove completely identical columns
            merged_data = merged_data.loc[:, ~merged_data.columns.duplicated()]

            # Final data processing
            final_data = self.apply_specific_renames(merged_data)

            print(f"✅ Обработка завершена. Строк: {len(final_data)}, Столбцов: {len(final_data.columns)}")

            # Save if output path provided
            if output_path:
                final_data.to_csv(output_path, index=False, encoding='utf-8')
                print(f"💾 Данные сохранены: {output_path}")

            return final_data

        except Exception as e:
            print(f"❌ Ошибка при парсинге {player_name}: {e}")
            return None

    def parse_squad_goalkeepers(self, squad_url: str) -> int:
        """
        Parse all goalkeepers from squad

        Args:
            squad_url: URL of the squad page

        Returns:
            Number of successfully parsed goalkeepers
        """
        print("🏴󠁧󠁢󠁥󠁮󠁧󠁿 Начинаю парсинг вратарей...")

        # Get goalkeeper links
        goalkeeper_links = extract_goalkeeper_links(squad_url)

        if not goalkeeper_links:
            print("❌ Не найдены вратари для парсинга")
            return 0

        successful_parses = 0

        for i, (player_name, player_url) in enumerate(goalkeeper_links):
            print(f"\n{'='*60}")
            print(f"🥅 Вратарь {i+1}/{len(goalkeeper_links)}: {player_name}")

            # Parse goalkeeper stats
            player_data = self.parse_goalkeeper(player_name, player_url)

            if player_data is not None and not player_data.empty:
                # Save data to CSV
                normalized_name = normalize_name(player_name)
                output_path = f"{DEFAULT_OUTPUT_DIR_GOALKEEPERS}/{normalized_name}_goalkeeper_stats.csv"

                try:
                    player_data.to_csv(output_path, index=False, encoding='utf-8')
                    print(f"💾 Данные сохранены: {output_path}")
                    successful_parses += 1

                    # Output brief statistics
                    print(f"📈 Статистика: {len(player_data)} сезонов, {len(player_data.columns)} показателей")

                except Exception as e:
                    print(f"❌ Ошибка сохранения для {player_name}: {e}")

            # Delay between requests
            if i < len(goalkeeper_links) - 1:
                time.sleep(2)

        print(f"\n🎯 Парсинг завершен!")
        print(f"✅ Успешно обработано: {successful_parses}/{len(goalkeeper_links)} вратарей")
        print(f"📁 Файлы сохранены в: {DEFAULT_OUTPUT_DIR_GOALKEEPERS}")

        return successful_parses
