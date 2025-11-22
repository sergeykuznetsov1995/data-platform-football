"""
Field player parser for FBref statistics

This module implements the FieldPlayerParser class for parsing field player
statistics from FBref.com, including support for individual players and full squads.
"""

import pandas as pd
import time
import os
from io import StringIO
from typing import Dict, List, Optional

from .base_parser import BaseParser
from ..core.scraper import FBrefScraper, extract_all_tables
from ..core.table_detector import (
    identify_field_player_tables,
    find_tables_by_unique_markers,
    resolve_table_conflict,
    analyze_all_tables
)
from ..core.column_processor import apply_field_player_renames
from ..utils.url_helpers import extract_player_name_from_url
from ..utils.file_helpers import get_output_path, normalize_name
from ..utils.squad_helpers import extract_field_player_links
from .. import constants


class FieldPlayerParser(BaseParser):
    """
    Parser for field player statistics from FBref

    Handles parsing of individual field players or entire squads,
    including all statistical categories (standard, shooting, passing, etc.)
    """

    def __init__(self):
        """Initialize field player parser"""
        super().__init__()
        self.scraper = FBrefScraper()

    def identify_tables(self, all_tables: List[pd.DataFrame]) -> Dict:
        """
        Identify field player tables using content-based detection

        Args:
            all_tables: List of all tables from page

        Returns:
            Dictionary mapping table type to (index, DataFrame)
        """
        return identify_field_player_tables(all_tables)

    def apply_specific_renames(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply field player-specific column renames

        Args:
            df: DataFrame to process

        Returns:
            DataFrame with field player renames applied
        """
        return apply_field_player_renames(df)

    def get_no_prefix_tables(self) -> List[str]:
        """
        Get list of tables that should not receive column prefixes

        Returns:
            List containing 'standard'
        """
        return ['standard']

    def parse_player(self, player_url: str, player_name: str = None,
                    output_path: str = None, simple_filename: bool = False) -> Optional[pd.DataFrame]:
        """
        Parse individual field player statistics

        Args:
            player_url: URL of the player's all_comps page
            player_name: Player name (optional, extracted from URL if not provided)
            output_path: Custom output path (optional)
            simple_filename: If True, uses simple filename without suffix

        Returns:
            DataFrame with parsed statistics, or None if parsing failed
        """
        if not player_name:
            player_name = extract_player_name_from_url(player_url)

        print(f"🚀 Запуск универсального парсера для {player_name}...")

        try:
            print("📥 Загружаю страницу...")
            response = self.scraper.fetch_page(player_url)

            # Extract all tables from page
            print("🔍 Ищу все статистические таблицы на странице...")
            all_page_tables = extract_all_tables(response.content.decode('utf-8'))
            print(f"Найдено {len(all_page_tables)} таблиц на странице")

            # Identify key tables by content
            key_tables = self.identify_tables(all_page_tables)

            # Fallback mechanism for finding missing tables
            expected_tables = ['standard', 'shooting', 'passing', 'pass_types', 'gca', 'defense', 'possession', 'playing_time', 'misc']
            missing_tables = [t for t in expected_tables if t not in key_tables]

            if missing_tables:  # Always try to find missing tables
                print(f"\n⚠️ Найдено только {len(key_tables)} таблиц из {len(expected_tables)} ожидаемых")
                print(f"Недостающие таблицы: {', '.join(missing_tables)}")
                print("Пытаюсь найти недостающие таблицы по уникальным маркерам...")

                # Find by unique markers
                found_by_markers = find_tables_by_unique_markers(all_page_tables, missing_tables)

                # Add found tables with conflict resolution
                rejected_tables = []
                for table_name, (pos, table) in found_by_markers.items():
                    # Check for position conflicts
                    conflict_name = None
                    for existing_name, (existing_pos, existing_table) in key_tables.items():
                        if existing_pos == pos:
                            conflict_name = existing_name
                            break

                    if conflict_name:
                        # Resolve conflict - choose best table
                        chosen_name, chosen_table, rejected_name = resolve_table_conflict(
                            conflict_name, key_tables[conflict_name][1],
                            table_name, table,
                            pos
                        )

                        # Update key_tables with chosen table
                        if chosen_name == table_name:
                            # New table is better - replace old one
                            del key_tables[conflict_name]
                            key_tables[table_name] = (pos, chosen_table)
                            rejected_tables.append(rejected_name)
                        else:
                            # Old one is better - remember rejected new one
                            rejected_tables.append(rejected_name)
                    else:
                        # No conflict - just add
                        key_tables[table_name] = (pos, table)

                # Search for rejected tables at other positions
                if rejected_tables:
                    print(f"\n🔄 Ищу отклонённые таблицы на других позициях: {rejected_tables}")
                    for rejected_table in rejected_tables:
                        found_alternatives = find_tables_by_unique_markers(all_page_tables, [rejected_table])

                        for alt_name, (alt_pos, alt_table) in found_alternatives.items():
                            # Check that position is free
                            pos_occupied = any(existing_pos == alt_pos for existing_name, (existing_pos, existing_table) in key_tables.items())
                            if not pos_occupied:
                                key_tables[alt_name] = (alt_pos, alt_table)
                                print(f"✅ Найдена альтернативная позиция для {alt_name} #{alt_pos}")
                                break

            # Check final list of missing tables
            final_missing = [t for t in expected_tables if t not in key_tables]

            if final_missing:
                print(f"\n⚠️ Финальный список не найденных таблиц: {', '.join(final_missing)}")
                print("Запускаю диагностику всех таблиц...")
                analyze_all_tables(all_page_tables)

            # Check if we have at least the 'standard' table (mandatory)
            if 'standard' not in key_tables:
                print("❌ Не найдена обязательная таблица STANDARD")
                if not key_tables:
                    print("   Не найдено ни одной ключевой таблицы")
                return None

            # Warn about missing tables but continue parsing
            if final_missing:
                print(f"⚠️ Продолжаю парсинг с {len(key_tables)} таблицами (отсутствуют: {', '.join(final_missing)})")

            print(f"\n🔗 Найдено {len(key_tables)} ключевых таблиц для объединения")

            # Process each table
            processed_tables = {}

            for table_name, (table_idx, table) in key_tables.items():
                print(f"\n📊 Обрабатываю таблицу {table_name}...")
                processed_table = self.process_table_columns(table.copy(), table_name)
                processed_tables[table_name] = processed_table
                print(f"✅ Обработано {len(processed_table)} строк из {table_name}")

            # Merge all tables
            merged_df = self.merge_tables(processed_tables)

            # Final cleanup
            final_df = self.final_cleanup(merged_df)

            # Apply field player-specific renames
            final_df = self.apply_specific_renames(final_df)

            # Determine output path
            if not output_path:
                output_path = get_output_path(
                    player_name,
                    output_dir=constants.DEFAULT_OUTPUT_DIR_FIELD_PLAYERS if simple_filename else None,
                    simple_filename=simple_filename
                )

            # Save to CSV
            self.save_to_csv(final_df, output_path)

            # Show sample data
            print(f"\n📋 Образец данных (первые 10 строк):")
            season_col = squad_col = comp_col = None

            for col in final_df.columns:
                col_lower = str(col).lower()
                if 'season' in col_lower and season_col is None:
                    season_col = col
                elif 'squad' in col_lower and squad_col is None:
                    squad_col = col
                elif ('comp' in col_lower or 'tournament' in col_lower) and comp_col is None:
                    comp_col = col

            if season_col and squad_col and comp_col:
                sample_data = final_df[[season_col, squad_col, comp_col]].head(10)
                print(sample_data.to_string(index=False))
            else:
                print(final_df.iloc[:10, :3].to_string(index=False))
                print(f"Использованы первые 3 колонки: {list(final_df.columns[:3])}")

            return final_df

        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
            return None

    def parse_squad(self, squad_url: str, limit: int = None, delay: int = 4) -> int:
        """
        Parse all field players from squad

        Args:
            squad_url: URL of the squad page
            limit: Maximum number of players to parse (optional, for testing)
            delay: Delay between requests in seconds (default: 4)

        Returns:
            Number of successfully parsed players
        """
        print(f"🚀 Запуск парсера команды...")
        print(f"📍 URL команды: {squad_url}")

        # Extract links to all field players
        player_links = extract_field_player_links(squad_url)

        if not player_links:
            print("❌ Не найдено ни одного полевого игрока")
            return 0

        # Apply limit if specified
        if limit and limit > 0:
            player_links = player_links[:limit]
            print(f"⚠️ Ограничение: будет спаршено только {len(player_links)} игроков")

        # Get list of existing files to skip already parsed players
        existing_files = set()
        if os.path.exists(constants.DEFAULT_OUTPUT_DIR_FIELD_PLAYERS):
            existing_files = set(os.listdir(constants.DEFAULT_OUTPUT_DIR_FIELD_PLAYERS))

        successful_parses = 0
        failed_parses = 0
        skipped_players = 0

        print(f"\n🔄 Начинаю парсинг {len(player_links)} полевых игроков...")
        if existing_files:
            print(f"📂 Найдено {len(existing_files)} существующих файлов - они будут пропущены")

        for i, (player_name, player_url) in enumerate(player_links, 1):
            # Check if player file already exists
            player_filename = f"{normalize_name(player_name)}.csv"
            if player_filename in existing_files:
                print(f"\n⏭️  Игрок {i}/{len(player_links)}: {player_name} - файл уже существует, пропускаю")
                skipped_players += 1
                continue

            print(f"\n📊 Парсинг игрока {i}/{len(player_links)}: {player_name}")

            try:
                result = self.parse_player(
                    player_url=player_url,
                    player_name=player_name,
                    output_path=None,
                    simple_filename=True
                )

                if result is not None:
                    successful_parses += 1
                    print(f"✅ Успешно спаршен: {player_name}")
                else:
                    failed_parses += 1
                    print(f"❌ Ошибка при парсинге: {player_name}")

            except Exception as e:
                failed_parses += 1
                print(f"❌ Ошибка при парсинге {player_name}: {e}")

            # Delay between requests (except for last player)
            if i < len(player_links):
                print(f"⏳ Задержка {delay} секунд...")
                time.sleep(delay)

        # Final statistics
        print(f"\n🎉 Парсинг команды завершен!")
        print(f"✅ Успешно спаршено: {successful_parses} игроков")
        print(f"⏭️  Пропущено (уже существуют): {skipped_players} игроков")
        print(f"❌ Ошибок при парсинге: {failed_parses} игроков")
        print(f"📁 Результаты сохранены в: {constants.DEFAULT_OUTPUT_DIR_FIELD_PLAYERS}")

        return successful_parses
