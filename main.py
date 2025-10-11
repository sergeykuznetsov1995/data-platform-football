#!/usr/bin/env python3
"""
Universal Parser v2.0 - CLI entry point for field player statistics

This script provides a command-line interface for parsing field player statistics
from FBref.com using the FieldPlayerParser class from the fbref_parser package.

Supports:
- Individual player parsing
- Full squad parsing
- CSV file fixing
"""

import argparse
import sys

from fbref_parser import FieldPlayerParser
from fbref_parser.core.data_cleaner import clean_aggregated_rows
from fbref_parser.core.column_processor import fix_column_names
from fbref_parser.constants import DEFAULT_ARSENAL_SQUAD_URL
import pandas as pd


def fix_existing_csv(input_file, output_file=None):
    """Исправляет названия колонок в существующем CSV файле"""
    if output_file is None:
        output_file = input_file.replace('.csv', '_fixed.csv')

    print(f"🔧 Исправление названий колонок в CSV файле: {input_file}")

    try:
        # Загружаем CSV файл
        df = pd.read_csv(input_file)
        print(f"📊 Загружен файл: {df.shape[0]} строк, {df.shape[1]} колонок")

        # Показываем примеры проблематичных названий
        problem_cols = [col for col in df.columns[:10] if 'Unnamed:' in str(col)]
        if problem_cols:
            print(f"\n🔍 Примеры проблематичных названий колонок:")
            for col in problem_cols[:5]:
                print(f"  - {col}")

        # Исправляем названия колонок
        print("\n✨ Исправляем названия колонок...")
        new_column_names = fix_column_names(df.columns)
        df.columns = new_column_names

        # Показываем исправленные названия
        print("\n✅ Новые названия колонок:")
        for new in new_column_names[:10]:
            print(f"  - {new}")

        # Очищаем агрегированные строки
        print("\n🧹 Удаляем агрегированные строки...")
        original_rows = len(df)
        df = clean_aggregated_rows(df)
        removed_rows = original_rows - len(df)

        if removed_rows > 0:
            print(f"  Удалено {removed_rows} агрегированных строк")
        else:
            print("  Агрегированные строки не найдены")

        # Сохраняем исправленный файл
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\n💾 Исправленный файл сохранен: {output_file}")
        print(f"📊 Итоговый размер: {df.shape[0]} строк, {df.shape[1]} колонок")

        # Показываем образец данных
        print(f"\n📋 Образец исправленных данных:")
        sample_cols = ['Season', 'Squad', 'Comp']
        available_cols = [col for col in sample_cols if col in df.columns]

        if available_cols:
            print(df[available_cols].head(5).to_string(index=False))
        else:
            print("Показываем первые 3 колонки:")
            print(df.iloc[:5, :3].to_string(index=False))

        print("\n🎉 Исправление завершено успешно!")
        return df

    except FileNotFoundError:
        print(f"❌ Файл не найден: {input_file}")
        return None
    except Exception as e:
        print(f"❌ Ошибка при обработке файла: {e}")
        return None


def main():
    """Основная функция с поддержкой аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description='Универсальный парсер статистики игроков с FBref',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s                              # Парсинг William Saliba (по умолчанию)
  %(prog)s --squad arsenal              # Парсинг всех полевых игроков Arsenal
  %(prog)s --squad arsenal --limit 5    # Парсинг только первых 5 игроков Arsenal
  %(prog)s --squad-url "URL"            # Парсинг команды по кастомному URL
  %(prog)s --fix file.csv               # Исправление существующего CSV файла
  %(prog)s --fix file.csv -o fixed.csv  # Исправление с указанием выходного файла
        """
    )

    parser.add_argument('--fix',
                       help='Исправить названия колонок в существующем CSV файле')
    parser.add_argument('-o', '--output',
                       help='Выходной файл для исправленного CSV (только с --fix)')

    # Аргументы для парсинга команды
    parser.add_argument('--squad-url',
                       help='URL страницы команды для парсинга всех полевых игроков')
    parser.add_argument('--squad',
                       choices=['arsenal'],
                       help='Парсинг предустановленной команды (arsenal)')
    parser.add_argument('--limit',
                       type=int,
                       help='Ограничить количество парсинга игроков (для тестирования)')
    parser.add_argument('--delay',
                       type=int,
                       default=4,
                       help='Задержка между запросами в секундах (по умолчанию: 4)')

    args = parser.parse_args()

    if args.fix:
        # Режим исправления существующего CSV
        if args.output:
            result = fix_existing_csv(args.fix, args.output)
        else:
            result = fix_existing_csv(args.fix)

        if result is not None:
            print(f"\n🎉 Исправление файла {args.fix} завершено!")
        else:
            print(f"\n💥 Исправление файла {args.fix} не удалось.")
            sys.exit(1)

    elif args.squad or args.squad_url:
        # Режим парсинга команды
        squad_url = None

        if args.squad == 'arsenal':
            squad_url = DEFAULT_ARSENAL_SQUAD_URL
        elif args.squad_url:
            squad_url = args.squad_url

        if squad_url:
            # Use FieldPlayerParser for squad parsing
            field_parser = FieldPlayerParser()
            result = field_parser.parse_squad(
                squad_url=squad_url,
                limit=args.limit,
                delay=args.delay
            )

            if result > 0:
                print(f"\n🎉 Готово! Парсинг команды завершен. Успешно обработано {result} игроков.")
            else:
                print("\n💥 Парсинг команды не удался.")
                sys.exit(1)
        else:
            print("❌ Не указан URL команды")
            sys.exit(1)

    else:
        # Режим парсинга с FBref (по умолчанию - William Saliba)
        player_url = "https://fbref.com/en/players/972aeb2a/all_comps/William-Saliba-Stats---All-Competitions"
        field_parser = FieldPlayerParser()
        result = field_parser.parse_player(player_url=player_url)

        if result is not None:
            print("\n🎉 Готово! Чистый CSV файл создан.")
        else:
            print("\n💥 Парсинг не удался.")
            sys.exit(1)


if __name__ == "__main__":
    main()
