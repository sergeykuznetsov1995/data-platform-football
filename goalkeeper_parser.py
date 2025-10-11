#!/usr/bin/env python3
"""
Goalkeeper Parser - CLI entry point for goalkeeper statistics

This script provides a command-line interface for parsing goalkeeper statistics
from FBref.com using the GoalkeeperParser class from the fbref_parser package.

Supports:
- Individual goalkeeper parsing
- Full squad goalkeeper parsing
"""

import argparse
import sys

from fbref_parser import GoalkeeperParser
from fbref_parser.utils.file_helpers import ensure_directory_exists
from fbref_parser.constants import DEFAULT_OUTPUT_DIR_GOALKEEPERS


def main():
    """Главная функция с обработкой аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description='Парсер статистики вратарей Arsenal с FBref.com',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  Парсинг всех вратарей Arsenal (по умолчанию):
    python3 goalkeeper_parser.py

  Парсинг с пользовательской ссылкой на команду:
    python3 goalkeeper_parser.py --squad-url "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats"

  Парсинг конкретного вратаря по прямой ссылке:
    python3 goalkeeper_parser.py --url "https://fbref.com/en/players/98ea5115/David-Raya"

Результат:
  - CSV файлы сохраняются в директорию /root/data_platform/test_arsenal_goalkeepers/
  - Каждый файл содержит полную статистику вратаря по всем турнирам
        """
    )

    parser.add_argument(
        '--squad-url',
        default='https://fbref.com/en/squads/18bb7c10/2025-2026/all_comps/Arsenal-Stats-All-Competitions',
        help='URL страницы команды для извлечения ссылок на вратарей'
    )

    parser.add_argument(
        '--url',
        help='URL конкретного вратаря для парсинга (вместо всей команды)'
    )

    parser.add_argument(
        '-o', '--output',
        help='Имя выходного файла (только при парсинге одного вратаря)'
    )

    args = parser.parse_args()

    # Initialize parser
    gk_parser = GoalkeeperParser()

    # Create output directory
    ensure_directory_exists(DEFAULT_OUTPUT_DIR_GOALKEEPERS)

    try:
        if args.url:
            # Parse single goalkeeper
            print("🥅 Режим парсинга одного вратаря")

            # Extract player name from URL
            if '/players/' in args.url:
                player_id_part = args.url.split('/players/')[1]
                if '/all_comps/' in player_id_part:
                    player_name_part = player_id_part.split('/all_comps/')[1]
                    player_name = player_name_part.split('-Stats')[0].replace('-', ' ')
                else:
                    # Try to extract name from other parts of URL
                    parts = player_id_part.split('/')
                    if len(parts) > 1:
                        player_name = parts[-1].replace('-', ' ').split('-Stats')[0]
                    else:
                        player_name = "Goalkeeper"
            else:
                player_name = "Goalkeeper"

            # Check that URL contains all_comps
            if '/all_comps/' not in args.url:
                print("⚠️ URL не содержит '/all_comps/' - добавляю автоматически")
                # Try to convert URL
                if '/players/' in args.url:
                    base_url = args.url.split('/players/')[0]
                    player_part = args.url.split('/players/')[1]
                    player_id = player_part.split('/')[0]
                    normalized_name = player_name.replace(' ', '-')
                    args.url = f"{base_url}/players/{player_id}/all_comps/{normalized_name}-Stats---All-Competitions"

            print(f"🎯 Парсинг: {player_name}")
            print(f"🔗 URL: {args.url}")

            # Determine output path
            if args.output:
                output_path = args.output
            else:
                from fbref_parser.utils.file_helpers import normalize_name
                normalized_name = normalize_name(player_name)
                output_path = f"/root/data_platform/{normalized_name}_goalkeeper_stats.csv"

            # Parse goalkeeper
            player_data = gk_parser.parse_goalkeeper(player_name, args.url, output_path=output_path)

            if player_data is not None and not player_data.empty:
                print(f"✅ Данные сохранены: {output_path}")
                print(f"📈 Статистика: {len(player_data)} сезонов, {len(player_data.columns)} показателей")
            else:
                print("❌ Не удалось получить данные вратаря")
                sys.exit(1)

        else:
            # Parse all Arsenal goalkeepers
            print("🏴󠁧󠁢󠁥󠁮󠁧󠁿 Режим парсинга всех вратарей Arsenal")
            result = gk_parser.parse_squad_goalkeepers(args.squad_url)

            if result > 0:
                print(f"\n🎉 Готово! Парсинг завершен. Успешно обработано {result} вратарей.")
            else:
                print("\n💥 Парсинг не удался.")
                sys.exit(1)

    except KeyboardInterrupt:
        print("\n⏹️ Парсинг прерван пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
