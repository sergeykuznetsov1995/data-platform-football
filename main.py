#!/usr/bin/env python3
"""
Единый оптимизированный парсер для всех статистических данных William Saliba с FBref
Исправляет проблемы с дублирующими колонками и агрегированными строками
Поддерживает два режима работы: парсинг с сайта и исправление существующих CSV файлов
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import numpy as np
from io import StringIO
import argparse
import sys
import os

def fix_column_names(columns):
    """Исправляет названия колонок, убирая проблематичные Unnamed: префиксы"""
    new_columns = []

    for col in columns:
        col_str = str(col)

        # Обрабатываем колонки с "Unnamed: X_level_0_"
        if col_str.startswith('Unnamed:') and '_level_0_' in col_str:
            # Извлекаем последнюю часть после последнего подчеркивания
            clean_name = col_str.split('_')[-1]
            new_columns.append(clean_name)
        else:
            # Оставляем остальные колонки как есть
            new_columns.append(col_str)

    return new_columns

def clean_dataframe(df):
    """Очистка DataFrame от мусорных данных"""
    if df.empty:
        return df

    # Удаляем колонки с названием Matches
    matches_cols = [col for col in df.columns if 'Matches' in str(col) or 'matches' in str(col).lower()]
    df = df.drop(columns=matches_cols, errors='ignore')

    # Ищем колонку с сезонами
    season_cols = [col for col in df.columns if 'Season' in str(col)]
    if season_cols:
        season_col = season_cols[0]

        # Удаляем только очевидно агрегированные строки
        # НЕ удаляем строки с реальными турнирами
        df = df[~df[season_col].astype(str).str.contains('Season|Seasons|Club|Clubs|Total|League', na=False)]

        # НЕ применяем строгую фильтрацию по формату сезона
        # Оставляем ВСЕ строки с конкретными сезонами и турнирами

    # Дополнительная проверка для удаления строк с названиями колонок
    # Ищем колонку с турнирами/соревнованиями
    comp_cols = [col for col in df.columns if 'Comp' in str(col) and 'Competition' not in str(col)]
    if comp_cols:
        comp_col = comp_cols[0]
        # Удаляем строки где колонка турниров содержит служебные слова
        df = df[~df[comp_col].astype(str).str.contains('Comp|Competition|Country|Squad|MP|Min', na=False)]

    # Удаляем полностью пустые строки
    df = df.dropna(how='all')

    return df

def clean_aggregated_rows(df):
    """Удаляет агрегированные строки из данных (расширенная версия)"""
    if df.empty:
        return df

    # Получаем первую колонку (обычно Season или пустая)
    first_col = df.iloc[:, 0]

    # Удаляем строки где первая колонка пустая или содержит только пробелы
    # и одновременно четвертая колонка содержит "Country" (признак мусорной строки)
    if len(df.columns) >= 4:
        fourth_col = df.iloc[:, 3]  # Country колонка

        # Находим индексы строк для удаления
        rows_to_drop = []
        for i, (first_val, fourth_val) in enumerate(zip(first_col, fourth_col)):
            first_str = str(first_val).strip()
            fourth_str = str(fourth_val).strip()

            # Если первая колонка пустая И четвертая содержит "Country"
            if (first_str == '' or first_str == 'nan') and fourth_str == 'Country':
                rows_to_drop.append(i)

        # Удаляем найденные строки
        df = df.drop(rows_to_drop)

    return df

def clean_final_dataframe(df):
    """Пост-обработка DataFrame для очистки и унификации названий столбцов и данных"""
    print("\n🧹 Начинаю пост-обработку данных...")

    # 1. Удаляем дублирующиеся столбцы 90s
    print("   Удаляю дублирующиеся столбцы 90s...")
    duplicate_90s_cols = [col for col in df.columns if col in [
        '90s_shooting', '90s_passing', '90s_pass_types',
        '90s_defense', '90s_gca', '90s_possession', '90s_misc'
    ]]
    df = df.drop(columns=duplicate_90s_cols)
    print(f"   Удалено {len(duplicate_90s_cols)} дублирующихся столбцов 90s")

    # 2. Переименовываем основные столбцы в snake_case
    print("   Переименовываю основные столбцы...")
    basic_renames = {
        'Season': 'season',
        'Age': 'age',
        'Squad': 'squad',
        'Country': 'country',
        'Comp': 'competition',
    }
    df = df.rename(columns=basic_renames)

    # 3. Переименовываем остальные Playing Time столбцы
    print("   Переименовываю Playing Time столбцы...")
    pt_renames = {
        # Базовые Playing Time столбцы (теперь приходят из playing_time таблицы с префиксом)
        'MP_playing_time': 'matches_played',
        'Playing Time_MP_playing_time': 'matches_played',
        'Starts_playing_time': 'starts',
        'Playing Time_Starts_playing_time': 'starts',
        'Min_playing_time': 'minutes',
        'Playing Time_Min_playing_time': 'minutes',
        '90s_playing_time': 'minutes_90',
        'Playing Time_90s_playing_time': 'minutes_90',
        # Остальные Playing Time столбцы
        'Playing Time_Mn/MP': 'minutes_per_match',
        'Mn/MP_playing_time': 'minutes_per_match',
        'Playing Time_Mn/MP_playing_time': 'minutes_per_match',
        'Playing Time_Min%_playing_time': 'minutes_pct',
        'Min%_playing_time': 'minutes_pct',
        'Starts_Starts_playing_time': 'starts_total',
        'Starts_Mn/Start_playing_time': 'minutes_per_start',
        'Mn/Start_playing_time': 'minutes_per_start',
        'Starts_Compl': 'matches_completed',
        'Compl_playing_time': 'matches_completed',
        'Starts_Compl_playing_time': 'matches_completed',
        'Subs_Subs_playing_time': 'subs_on',
        'Subs_playing_time': 'subs_on',
        'Subs_Mn/Sub_playing_time': 'minutes_per_sub',
        'Mn/Sub_playing_time': 'minutes_per_sub',
        'Subs_unSub_playing_time': 'subs_unused',
        'unSub_playing_time': 'subs_unused',
        'Team Success_PPM_playing_time': 'team_points_per_match',
        'PPM_playing_time': 'team_points_per_match',
        'Team Success_onG_playing_time': 'team_goals_for',
        'onG_playing_time': 'team_goals_for',
        'Team Success_onGA_playing_time': 'team_goals_against',
        'onGA_playing_time': 'team_goals_against',
        'Team Success_+/-_playing_time': 'team_goal_diff',
        '+/-_playing_time': 'team_goal_diff',
        'Team Success_+/-90_playing_time': 'team_goal_diff_per90',
        '+/-90_playing_time': 'team_goal_diff_per90',
        'Team Success_On-Off_playing_time': 'team_on_off',
        'On-Off_playing_time': 'team_on_off',
        'Team Success (xG)_onxG_playing_time': 'team_xg_for',
        'onxG_playing_time': 'team_xg_for_xg',
        'Team Success (xG)_onxGA_playing_time': 'team_xg_against',
        'onxGA_playing_time': 'team_xg_against_xg',
        'Team Success (xG)_xG+/-_playing_time': 'team_xg_diff',
        'xG+/-_playing_time': 'team_xg_diff',
        'Team Success (xG)_xG+/-90_playing_time': 'team_xg_diff_per90',
        'xG+/-90_playing_time': 'team_xg_diff_per90',
        'Team Success (xG)_On-Off_playing_time': 'team_xg_on_off'
    }

    # Применяем только те переименования, которые существуют в DataFrame
    existing_pt_renames = {old: new for old, new in pt_renames.items() if old in df.columns}
    df = df.rename(columns=existing_pt_renames)
    print(f"   Переименовано {len(existing_pt_renames)} Playing Time столбцов")

    # 4. Сокращаем суффиксы таблиц
    print("   Сокращаю суффиксы таблиц...")
    suffix_map = {
        '_shooting': '_sh',
        '_passing': '_pass',
        '_pass_types': '_pt',
        '_defense': '_def',
        '_possession': '_poss',
        '_misc': '_misc',
        '_gca': '_gca'
    }

    new_columns = []
    for col in df.columns:
        new_col = col
        for old_suffix, new_suffix in suffix_map.items():
            if col.endswith(old_suffix):
                new_col = col.replace(old_suffix, new_suffix)
                break
        new_columns.append(new_col)

    df.columns = new_columns

    # 5. Полная конвертация в snake_case и замена специальных символов
    print("   Конвертирую все названия столбцов в snake_case...")

    def convert_to_snake_case(column_name):
        """Конвертирует названия столбцов в полный snake_case с заменой специальных символов"""
        # Сначала заменяем специальные символы описательными словами
        col = str(column_name)

        # Замена специальных символов
        col = col.replace('%', '_pct')
        col = col.replace('+', '_plus_')
        col = col.replace('-', '_minus_')
        col = col.replace('/', '_per_')
        col = col.replace('(', '_')
        col = col.replace(')', '_')
        col = col.replace(' ', '_')
        col = col.replace('&', '_and_')
        col = col.replace('#', '_num_')

        # Убираем множественные подчеркивания
        col = re.sub(r'_+', '_', col)

        # Убираем подчеркивания в начале и конце
        col = col.strip('_')

        # Конвертируем в lowercase
        col = col.lower()

        # Специальные замены для читаемости
        replacements = {
            'g_plus_a': 'goals_plus_assists',
            'g_minus_pk': 'goals_minus_penalties',
            'npxg_plus_xag': 'npxg_plus_xag',
            'g_plus_a_minus_pk': 'goals_plus_assists_minus_penalties',
            'per_90_minutes': 'per_90',
            'gca_types': 'gca_types',
            'sca_types': 'sca_types',
            'aerial_duels': 'aerial_duels',
            'def_3rd': 'def_third',
            'mid_3rd': 'mid_third',
            'att_3rd': 'att_third',
            'def_pen': 'def_penalty_area',
            'att_pen': 'att_penalty_area',
            'take_minus_ons': 'takeons',
            'team_success': 'team_success',
            'mn_per_mp': 'minutes_per_match',
            'min_pct': 'minutes_pct',
            'mn_per_start': 'minutes_per_start',
            'mn_per_sub': 'minutes_per_sub'
        }

        for old, new in replacements.items():
            col = col.replace(old, new)

        return col

    # Применяем конвертацию ко всем столбцам
    new_column_names = [convert_to_snake_case(col) for col in df.columns]
    df.columns = new_column_names

    print(f"   Конвертировано {len(df.columns)} названий столбцов в snake_case")

    # 6. Очищаем значения данных
    print("   Очищаю значения данных...")

    # Очищаем Country (убираем префиксы типа "eng ENG" -> "ENG")
    if 'country' in df.columns:
        df['country'] = df['country'].astype(str).str.replace(r'^[a-z]+ ', '', regex=True)
        df['country'] = df['country'].replace('nan', '')

    # Очищаем Competition (убираем номера лиг типа "1. Ligue 1" -> "Ligue 1")
    if 'competition' in df.columns:
        df['competition'] = df['competition'].astype(str).str.replace(r'^\d+\. ', '', regex=True)
        # Дополнительная очистка
        df['competition'] = df['competition'].str.replace('Jr. PL2 — Div. 1', 'PL2 Div 1')

    print(f"✅ Пост-обработка завершена! Итоговый размер: {df.shape[0]} строк × {df.shape[1]} столбцов")
    return df

def find_tables_by_unique_markers(all_page_tables, missing_tables):
    """
    Поиск недостающих таблиц по уникальным маркерам колонок
    Возвращает найденные таблицы с их индексами
    """

    # Уникальные маркеры для каждого типа таблицы (улучшенная специфичность)
    UNIQUE_MARKERS = {
        'gca': ['gca', 'sca90', 'goal creation', 'shot creation'],  # Более специфичные маркеры
        'possession': ['touches', 'carries', 'take-ons', 'dribbles'],
        'misc': ['recov', 'aerial', 'fls', 'fld'],
        'playing_time': ['starts', 'mn/start', 'compl', 'min%'],
        'defense': ['tkl', 'tkl+int', 'blocks', 'challenges'],
        'pass_types': ['live', 'dead', 'fk', 'tb'],  # Конкретные колонки Pass Types
        'passing': ['cmp', 'att', 'cmp%', 'totdist'],
        'shooting': ['sh', 'sot', 'sot%', 'g/sh'],
        'standard': ['gls', 'ast', 'g+a', 'pk']
    }

    found_tables = {}

    for table_name in missing_tables:
        if table_name not in UNIQUE_MARKERS:
            continue

        markers = UNIQUE_MARKERS[table_name]
        candidates = []

        # Ищем по всем таблицам
        for i, table in enumerate(all_page_tables):
            if len(table) < 10:  # Пропускаем маленькие таблицы
                continue

            cols_str = str(table.columns).lower()

            # Проверяем наличие уникальных маркеров
            marker_count = sum(1 for marker in markers if marker in cols_str)

            if marker_count >= 2:  # Нужно минимум 2 маркера
                candidates.append((i, table, marker_count, len(table)))

        # Выбираем лучшую таблицу (больше маркеров, больше строк)
        if candidates:
            best = max(candidates, key=lambda x: (x[2], x[3]))  # сортируем по маркерам, потом по размеру
            found_tables[table_name] = (best[0], best[1])
            print(f"✅ Найдена таблица {table_name.upper()} #{best[0]} по маркерам ({best[2]} маркеров, {best[3]} строк)")

    return found_tables

def score_table_quality(table, table_type, markers):
    """
    Оценивает качество соответствия таблицы определённому типу
    Возвращает оценку от 0 до 100
    """
    cols_str = str(table.columns).lower()
    score = 0

    # Базовая оценка по маркерам
    marker_matches = sum(1 for marker in markers if marker in cols_str)
    score += marker_matches * 20  # До 80 баллов за маркеры

    # Бонус за точное соответствие
    if table_type == 'gca' and ('gca' in cols_str or 'sca' in cols_str):
        score += 15
    elif table_type == 'pass_types' and ('pass types' in cols_str):
        score += 15
    elif table_type == 'possession' and ('possession' in cols_str):
        score += 15

    # Бонус за размер таблицы (больше строк = лучше)
    size_bonus = min(len(table) / 50 * 5, 10)  # До 10 баллов
    score += size_bonus

    # Штраф за слишком маленькие таблицы
    if len(table) < 15:
        score -= 20

    return min(score, 100)

def resolve_table_conflict(existing_name, existing_table, new_name, new_table, position):
    """
    Разрешает конфликт когда две таблицы претендуют на одну позицию
    Возвращает (выбранное_имя, выбранная_таблица, отклонённое_имя)
    """

    # Получаем маркеры для обеих таблиц (синхронизированы с find_tables_by_unique_markers)
    UNIQUE_MARKERS = {
        'gca': ['gca', 'sca90', 'goal creation', 'shot creation'],  # Более специфичные маркеры
        'possession': ['touches', 'carries', 'take-ons', 'dribbles'],
        'misc': ['recov', 'aerial', 'fls', 'fld'],
        'playing_time': ['starts', 'mn/start', 'compl', 'min%'],
        'defense': ['tkl', 'tkl+int', 'blocks', 'challenges'],
        'pass_types': ['live', 'dead', 'fk', 'tb'],  # Конкретные колонки Pass Types
        'passing': ['cmp', 'att', 'cmp%', 'totdist'],
        'shooting': ['sh', 'sot', 'sot%', 'g/sh'],
        'standard': ['gls', 'ast', 'g+a', 'pk']
    }

    existing_markers = UNIQUE_MARKERS.get(existing_name, [])
    new_markers = UNIQUE_MARKERS.get(new_name, [])

    # Оцениваем качество обеих таблиц
    existing_score = score_table_quality(existing_table, existing_name, existing_markers)
    new_score = score_table_quality(new_table, new_name, new_markers)

    print(f"🔍 Конфликт позиции #{position}: {existing_name}({existing_score:.0f}) vs {new_name}({new_score:.0f})")

    # Выбираем таблицу с лучшей оценкой
    if new_score > existing_score:
        print(f"✅ Выбрана {new_name} (лучшая оценка)")
        return new_name, new_table, existing_name
    else:
        print(f"✅ Оставлена {existing_name} (лучшая оценка)")
        return existing_name, existing_table, new_name

def analyze_all_tables(all_page_tables):
    """Выводит детальную информацию о всех таблицах для диагностики"""
    print(f"\n🔍 ДИАГНОСТИКА: Анализ всех {len(all_page_tables)} таблиц на странице:")

    for i, table in enumerate(all_page_tables):
        if len(table) < 5:  # Пропускаем очень маленькие таблицы
            continue

        print(f"\n=== Таблица #{i} ===")
        print(f"Размер: {len(table)} строк × {len(table.columns)} колонок")

        # Выводим первые 10 колонок
        cols = []
        for col in table.columns[:10]:
            if isinstance(col, tuple):
                clean_col = '_'.join([str(c) for c in col if str(c) != 'nan' and str(c).strip()])
                cols.append(clean_col)
            else:
                cols.append(str(col))

        print(f"Первые колонки: {cols}")

        # Анализируем возможный тип таблицы
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

def scrape_all_competitions_table(soup, table_id, table_name):
    """Парсинг таблицы со всеми турнирами"""
    print(f"Парсинг {table_name} (ID: {table_id})...")

    table = soup.find('table', {'id': table_id})
    if not table:
        print(f"Таблица {table_id} не найдена")
        return pd.DataFrame()

    try:
        # Парсим таблицу
        tables = pd.read_html(StringIO(str(table)), header=[0,1])
        if not tables:
            return pd.DataFrame()

        df = tables[0]

        # Обрабатываем многоуровневые заголовки
        if isinstance(df.columns, pd.MultiIndex):
            new_columns = []
            for col in df.columns:
                if isinstance(col, tuple):
                    # Объединяем уровни заголовков
                    clean_col = '_'.join([str(c) for c in col if str(c) != 'nan' and str(c).strip()])
                    # Убираем лишние подчеркивания
                    clean_col = re.sub(r'_+', '_', clean_col).strip('_')
                else:
                    clean_col = str(col)
                new_columns.append(clean_col)
            df.columns = new_columns

        # Исправляем проблематичные названия колонок с "Unnamed:"
        df.columns = fix_column_names(df.columns)

        # Добавляем префикс к колонкам (кроме ключевых)
        key_columns = ['Season', 'Age', 'Squad', 'Country', 'Comp', 'LgRank', 'MP']
        new_columns = []

        for col in df.columns:
            col_str = str(col)
            # Проверяем, является ли колонка ключевой
            is_key = any(key in col_str for key in key_columns)

            if is_key or table_name == 'standard':
                new_columns.append(col_str)
            else:
                new_columns.append(f"{col_str}_{table_name}")

        df.columns = new_columns

        # Очищаем DataFrame
        df = clean_dataframe(df)

        print(f"Успешно спарсено {len(df)} строк из {table_name}")
        return df

    except Exception as e:
        print(f"Ошибка при парсинге {table_name}: {e}")
        return pd.DataFrame()

def extract_player_name_from_url(url):
    """Извлекает имя игрока из URL FBref"""
    match = re.search(r'/([^/]+)-Stats', url)
    if match:
        return match.group(1).replace('-', ' ')
    return "unknown_player"

def normalize_name(name):
    """Нормализует имя для использования в файле"""
    clean_name = re.sub(r'[^\w\s-]', '', name.strip())
    clean_name = re.sub(r'[\s-]+', '_', clean_name)
    return clean_name.lower()

def build_player_url(player_id, player_name=None):
    """Создает URL для страницы игрока на FBref"""
    if not player_name:
        player_name = "Player"
    url_name = player_name.replace(' ', '-')
    return f"https://fbref.com/en/players/{player_id}/all_comps/{url_name}-Stats---All-Competitions"

def extract_player_links_from_squad(squad_url):
    """Извлекает ссылки на всех полевых игроков со страницы команды"""
    print(f"🔍 Извлекаю ссылки на полевых игроков с: {squad_url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(squad_url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Ищем все таблицы на странице
        all_tables = soup.find_all('table')

        # Ищем таблицу со стандартной статистикой
        standard_stats_table = soup.find('table', {'id': 'all_stats_standard'})

        # Если не нашли по точному ID, пробуем альтернативные варианты
        if not standard_stats_table:
            # Пробуем другие возможные ID - начинаем с более вероятных
            alternative_ids = ['stats_standard_9', 'stats_standard', 'stats_standard_combined']
            for alt_id in alternative_ids:
                standard_stats_table = soup.find('table', {'id': alt_id})
                if standard_stats_table:
                    break

            # Если всё ещё не нашли, пробуем поиск по содержимому заголовков
            if not standard_stats_table:
                for table in all_tables:
                    headers = table.find_all(['th', 'td'])
                    header_text = ' '.join([h.get_text().strip() for h in headers[:10]])
                    if any(keyword in header_text.lower() for keyword in ['player', 'nation', 'pos', 'age', 'mp', 'starts']):
                        standard_stats_table = table
                        break

        if not standard_stats_table:
            print("❌ Не найдена таблица стандартной статистики")
            return []

        player_links = []

        # Пробуем искать полевых игроков во всех таблицах стандартной статистики
        tables_to_check = [standard_stats_table]

        # Если в основной таблице только вратари, проверим ВСЕ таблицы
        # Добавляем все таблицы для проверки
        for table in all_tables:
            table_id = table.get('id', '')
            if table_id and table_id not in [t.get('id', '') for t in tables_to_check]:
                if any(keyword in table_id for keyword in ['stats_', 'standard', 'shooting', 'passing', 'defense']):
                    tables_to_check.append(table)

        for table_idx, table in enumerate(tables_to_check):
            # Извлекаем строки с игроками из tbody
            tbody = table.find('tbody')
            if not tbody:
                continue

            found_in_table = 0

            for row in tbody.find_all('tr'):
                # Пропускаем строки с заголовками
                if 'thead' in row.get('class', []):
                    continue

                cells = row.find_all(['td', 'th'])
                if len(cells) < 4:  # Минимум колонок: Игрок, Нация, Позиция, Возраст
                    continue

                # Первая ячейка содержит имя игрока и ссылку
                player_cell = cells[0]

                # Позиция обычно в 3-й колонке (индекс 2)
                position_cell = cells[2] if len(cells) > 2 else None
                position = position_cell.get_text(strip=True) if position_cell else ""

                # Пропускаем вратарей
                if 'GK' in position.upper():
                    continue

                # Ищем ссылку на игрока
                player_link = player_cell.find('a')
                if player_link and player_link.get('href'):
                    href = player_link.get('href')

                    # Проверяем что это ссылка на страницу игрока
                    if '/players/' in href:
                        player_name = player_cell.get_text(strip=True)

                        # Проверяем на дубликаты
                        if any(existing_name == player_name for existing_name, _ in player_links):
                            continue

                        full_url = f"https://fbref.com{href}"

                        # Конвертируем в URL всех турниров
                        if not '/all_comps/' in full_url:
                            # Заменяем обычный URL на URL всех турниров
                            parts = href.split('/')
                            if len(parts) >= 4:
                                player_id = parts[3]
                                player_url_name = parts[4] if len(parts) > 4 else player_name.replace(' ', '-')
                                full_url = f"https://fbref.com/en/players/{player_id}/all_comps/{player_url_name}-Stats---All-Competitions"

                        player_links.append((player_name, full_url))
                        found_in_table += 1

            # Если нашли достаточно игроков в этой таблице, можно остановиться
            if len(player_links) >= 25:  # Ограничиваем разумным количеством
                break

        print(f"\n📊 Найдено {len(player_links)} полевых игроков")
        return player_links

    except Exception as e:
        print(f"❌ Ошибка при извлечении ссылок на игроков: {e}")
        return []

def parse_arsenal_squad(squad_url=None, limit=None, delay=4):
    """Парсит всех полевых игроков команды Arsenal"""
    if squad_url is None:
        squad_url = "https://fbref.com/en/squads/18bb7c10/2023-2024/Arsenal-Stats"

    print(f"🚀 Запуск парсера команды Arsenal...")
    print(f"📍 URL команды: {squad_url}")

    # Извлекаем ссылки на всех полевых игроков
    player_links = extract_player_links_from_squad(squad_url)

    if not player_links:
        print("❌ Не найдено ни одного полевого игрока")
        return None

    # Применяем лимит если указан
    if limit and limit > 0:
        player_links = player_links[:limit]
        print(f"⚠️ Ограничение: будет спаршено только {len(player_links)} игроков из {len(player_links)}")

    # Создаем папку для результатов
    output_dir = "/root/data_platform/test_arsenal_players"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Создана папка: {output_dir}")

    successful_parses = 0
    failed_parses = 0

    print(f"\n🔄 Начинаю парсинг {len(player_links)} полевых игроков...")

    for i, (player_name, player_url) in enumerate(player_links, 1):
        print(f"\n📊 Парсинг игрока {i}/{len(player_links)}: {player_name}")

        try:
            # Используем существующую функцию parse_from_fbref с модификациями
            result = parse_from_fbref(
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

        # Задержка между запросами (кроме последнего игрока)
        if i < len(player_links):
            print(f"⏳ Задержка {delay} секунд...")
            time.sleep(delay)

    # Итоговая статистика
    print(f"\n🎉 Парсинг команды Arsenal завершен!")
    print(f"✅ Успешно спаршено: {successful_parses} игроков")
    print(f"❌ Ошибок при парсинге: {failed_parses} игроков")
    print(f"📁 Результаты сохранены в: {output_dir}")

    return successful_parses

def parse_from_fbref(player_url=None, player_name=None, output_path=None, simple_filename=False):
    """Парсинг данных с FBref"""

    if not player_name and player_url:
        player_name = extract_player_name_from_url(player_url)
    if not player_name:
        player_name = "William Saliba"

    if not player_url:
        player_url = "https://fbref.com/en/players/972aeb2a/all_comps/William-Saliba-Stats---All-Competitions"

    print(f"🚀 Запуск универсального парсера для {player_name}...")

    # URL страницы со всеми турнирами
    url = player_url

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        print("📥 Загружаю страницу...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Новый подход: используем pandas.read_html для автоматического поиска таблиц
        print("🔍 Ищу все статистические таблицы на странице...")

        try:
            # Извлекаем все таблицы с страницы
            all_page_tables = pd.read_html(StringIO(response.content.decode('utf-8')), header=[0,1])

            print(f"Найдено {len(all_page_tables)} таблиц на странице")

            # Определяем ключевые таблицы по их содержимому
            key_tables = {}

            for i, table in enumerate(all_page_tables):
                if len(table) < 10 or len(table.columns) < 10:  # Пропускаем слишком маленькие таблицы
                    continue

                # Анализируем колонки для определения типа таблицы
                cols_str = str(table.columns).lower()

                # Стандартная статистика (основная таблица)
                if ('season' in cols_str or 'squad' in cols_str) and 'gls' in cols_str and 'ast' in cols_str:
                    if 'standard' not in key_tables:
                        key_tables['standard'] = (i, table)
                        print(f"✅ Найдена таблица STANDARD #{i}: {len(table)} строк, {len(table.columns)} колонок")

                # Удары
                elif 'shooting' in cols_str or ('sh' in cols_str and 'sot' in cols_str):
                    if 'shooting' not in key_tables:
                        key_tables['shooting'] = (i, table)
                        print(f"✅ Найдена таблица SHOOTING #{i}: {len(table)} строк, {len(table.columns)} колонок")

                # Передачи
                elif 'passing' in cols_str or ('cmp' in cols_str and 'att' in cols_str):
                    if 'passing' not in key_tables:
                        key_tables['passing'] = (i, table)
                        print(f"✅ Найдена таблица PASSING #{i}: {len(table)} строк, {len(table.columns)} колонок")

                # Типы передач
                elif 'pass types' in cols_str or 'live' in cols_str:
                    if 'pass_types' not in key_tables:
                        key_tables['pass_types'] = (i, table)
                        print(f"✅ Найдена таблица PASS TYPES #{i}: {len(table)} строк, {len(table.columns)} колонок")

                # Создание голов и ударов
                elif any(marker in cols_str for marker in ['gca', 'sca', 'goal creation', 'shot creation', 'gca90', 'sca90', 'passlive', 'passdead']):
                    if 'gca' not in key_tables:
                        key_tables['gca'] = (i, table)
                        print(f"✅ Найдена таблица GCA/SCA #{i}: {len(table)} строк, {len(table.columns)} колонок")

                # Защита
                elif 'defense' in cols_str or 'tkl' in cols_str:
                    if 'defense' not in key_tables:
                        key_tables['defense'] = (i, table)
                        print(f"✅ Найдена таблица DEFENSE #{i}: {len(table)} строк, {len(table.columns)} колонок")

                # Игровое время (проверяем РАНЬШЕ possession с более специфичными маркерами)
                elif any(marker in cols_str for marker in ['mn/mp', 'min%', 'team success', 'ppm']) and 'touches' not in cols_str:
                    if 'playing_time' not in key_tables:
                        key_tables['playing_time'] = (i, table)
                        first_cols = [str(col) for col in table.columns[:5]]
                        print(f"✅ Найдена таблица PLAYING TIME #{i}: {len(table)} строк, {len(table.columns)} колонок")
                        print(f"   Первые колонки: {first_cols}")

                # Владение мячом (ОБЯЗАТЕЛЬНО должна содержать touches)
                elif 'touches' in cols_str and any(marker in cols_str for marker in ['def pen', 'def 3rd', 'mid 3rd', 'att 3rd', 'dribbles']):
                    if 'possession' not in key_tables:
                        key_tables['possession'] = (i, table)
                        first_cols = [str(col) for col in table.columns[:5]]
                        print(f"✅ Найдена таблица POSSESSION #{i}: {len(table)} строк, {len(table.columns)} колонок")
                        print(f"   Первые колонки: {first_cols}")

                # Разное
                elif any(marker in cols_str for marker in ['misc', 'fls', 'fld', 'off', 'crs', 'tklw', 'pkwon', 'pkcon', 'og', 'recov', 'aerial', 'won', 'lost']):
                    if 'misc' not in key_tables:
                        key_tables['misc'] = (i, table)
                        print(f"✅ Найдена таблица MISC #{i}: {len(table)} строк, {len(table.columns)} колонок")

            # Fallback-механизм для поиска недостающих таблиц
            expected_tables = ['standard', 'shooting', 'passing', 'pass_types', 'gca', 'defense', 'possession', 'playing_time', 'misc']
            missing_tables = [t for t in expected_tables if t not in key_tables]

            if missing_tables and len(key_tables) >= 6:  # Если найдено хотя бы 6 таблиц
                print(f"\n⚠️ Найдено только {len(key_tables)} таблиц из {len(expected_tables)} ожидаемых")
                print(f"Недостающие таблицы: {', '.join(missing_tables)}")
                print("Пытаюсь найти недостающие таблицы по уникальным маркерам...")

                # Поиск по уникальным маркерам колонок
                found_by_markers = find_tables_by_unique_markers(all_page_tables, missing_tables)

                # Добавляем найденные таблицы с разрешением конфликтов
                rejected_tables = []  # Собираем отклонённые таблицы для повторного поиска
                for table_name, (pos, table) in found_by_markers.items():
                    # Проверяем конфликты позиций
                    conflict_name = None
                    for existing_name, (existing_pos, existing_table) in key_tables.items():
                        if existing_pos == pos:
                            conflict_name = existing_name
                            break

                    if conflict_name:
                        # Разрешаем конфликт - выбираем лучшую таблицу
                        chosen_name, chosen_table, rejected_name = resolve_table_conflict(
                            conflict_name, key_tables[conflict_name][1],
                            table_name, table,
                            pos
                        )

                        # Обновляем key_tables с выбранной таблицей
                        if chosen_name == table_name:
                            # Новая таблица лучше - заменяем старую
                            del key_tables[conflict_name]
                            key_tables[table_name] = (pos, chosen_table)
                            rejected_tables.append(rejected_name)  # Запоминаем отклонённую
                        else:
                            # Старая лучше - запоминаем отклонённую новую
                            rejected_tables.append(rejected_name)
                    else:
                        # Нет конфликта - просто добавляем
                        key_tables[table_name] = (pos, table)

                # Поиск отклонённых таблиц на других позициях
                if rejected_tables:
                    print(f"\n🔄 Ищу отклонённые таблицы на других позициях: {rejected_tables}")
                    for rejected_table in rejected_tables:
                        found_alternatives = find_tables_by_unique_markers(all_page_tables, [rejected_table])

                        for alt_name, (alt_pos, alt_table) in found_alternatives.items():
                            # Проверяем, что позиция свободна
                            pos_occupied = any(existing_pos == alt_pos for existing_name, (existing_pos, existing_table) in key_tables.items())
                            if not pos_occupied:
                                key_tables[alt_name] = (alt_pos, alt_table)
                                print(f"✅ Найдена альтернативная позиция для {alt_name} #{alt_pos}")
                                break

            # Проверяем финальный список недостающих таблиц
            final_missing = [t for t in expected_tables if t not in key_tables]

            if final_missing:
                print(f"\n⚠️ Финальный список не найденных таблиц: {', '.join(final_missing)}")
                print("Запускаю диагностику всех таблиц...")
                analyze_all_tables(all_page_tables)

            if not key_tables:
                print("❌ Не найдено ни одной ключевой таблицы")
                return None

            print(f"\n🔗 Найдено {len(key_tables)} ключевых таблиц для объединения")

            # Обрабатываем каждую таблицу
            processed_tables = {}

            for table_name, (table_idx, table) in key_tables.items():
                print(f"\n📊 Обрабатываю таблицу {table_name}...")

                # Обрабатываем многоуровневые заголовки
                if isinstance(table.columns, pd.MultiIndex):
                    new_columns = []
                    for col in table.columns:
                        if isinstance(col, tuple):
                            clean_col = '_'.join([str(c) for c in col if str(c) != 'nan' and str(c).strip()])
                            clean_col = re.sub(r'_+', '_', clean_col).strip('_')
                        else:
                            clean_col = str(col)
                        new_columns.append(clean_col)
                    table.columns = new_columns

                # Исправляем проблематичные названия колонок с "Unnamed:"
                table.columns = fix_column_names(table.columns)

                # Удаляем столбцы Playing Time из других таблиц (оставляем только из playing_time)
                if table_name != 'playing_time':
                    # Паттерны для Playing Time столбцов в MultiIndex и обычных таблицах
                    playing_time_patterns = [
                        r'Playing[_ ]Time[_ ]',  # MultiIndex: "Playing Time_Starts", "Playing_Time_Starts" etc.
                        r'^(MP|Starts|Min|90s|Mn/MP|Min%|Mn/Start|Compl)$',  # Точные совпадения
                        r'Team_Success_',  # Team Success столбцы из Playing Time таблицы
                        r'Subs_',  # Substitution-related columns
                    ]

                    # Ищем столбцы для удаления по паттернам
                    cols_to_drop = []
                    for col in table.columns:
                        col_str = str(col)
                        # Проверяем каждый паттерн
                        if any(re.search(pattern, col_str, re.IGNORECASE) for pattern in playing_time_patterns):
                            cols_to_drop.append(col)

                    if cols_to_drop:
                        table = table.drop(columns=cols_to_drop)
                        print(f"   Удалено {len(cols_to_drop)} столбцов Playing Time из {table_name}: {cols_to_drop}")

                # Добавляем префикс к колонкам (кроме ключевых)
                key_columns = ['Season', 'Age', 'Squad', 'Country', 'Comp', 'LgRank']
                new_columns = []

                for col in table.columns:
                    col_str = str(col)
                    # Проверяем, является ли колонка ключевой
                    is_key = any(key in col_str for key in key_columns)

                    if is_key or table_name == 'standard':
                        new_columns.append(col_str)
                    else:
                        new_columns.append(f"{col_str}_{table_name}")

                table.columns = new_columns

                # Очищаем таблицу
                table = clean_dataframe(table)
                processed_tables[table_name] = table

                print(f"✅ Обработано {len(table)} строк из {table_name}")

            # Объединяем все таблицы
            print(f"\n🔗 Объединяю {len(processed_tables)} таблиц...")

            # Начинаем со стандартной таблицы как основы
            if 'standard' in processed_tables:
                merged_df = processed_tables['standard'].copy()
                print(f"Базовая таблица (standard): {merged_df.shape}")

                # Определяем ключевые колонки для объединения
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

                # Объединяем остальные таблицы
                for table_name, table_df in processed_tables.items():
                    if table_name == 'standard':
                        continue

                    print(f"Объединяю с {table_name}: {table_df.shape}")

                    if merge_keys:
                        try:
                            merged_df = pd.merge(merged_df, table_df, on=merge_keys, how='left', suffixes=('', f'_dup_{table_name}'))
                        except Exception as e:
                            print(f"⚠️ Ошибка при объединении {table_name}: {e}")
                            # Пробуем объединить по индексу
                            merged_df = pd.concat([merged_df, table_df], axis=1)
                    else:
                        merged_df = pd.concat([merged_df, table_df], axis=1)

                    print(f"Размер после объединения: {merged_df.shape}")

                all_dataframes = {'all_competitions': merged_df}
            else:
                print("❌ Не найдена стандартная таблица для использования как основа")
                return None

        except Exception as e:
            print(f"❌ Ошибка при автоматическом поиске таблиц: {e}")
            return None

        if not all_dataframes:
            print("❌ Не удалось спарсить ни одной таблицы")
            return None

        print(f"\n📊 Используем единую таблицу со всеми турнирами...")

        # Используем единую таблицу со всеми турнирами
        final_df = all_dataframes['all_competitions'].copy()
        print(f"Итоговая таблица: {final_df.shape[0]} строк, {final_df.shape[1]} колонок")

        # Показываем первые колонки для диагностики
        print(f"Колонки таблицы: {list(final_df.columns[:10])}")

        # Проверяем есть ли колонка с сезонами и турнирами
        season_cols = [col for col in final_df.columns if 'season' in col.lower() or any(word in col.lower() for word in ['season', 'year'])]
        comp_cols = [col for col in final_df.columns if 'comp' in col.lower() or 'tournament' in col.lower()]

        print(f"Найденные колонки сезонов: {season_cols}")
        print(f"Найденные колонки турниров: {comp_cols}")

        # Финальная очистка
        print("\n🧹 Финальная очистка данных...")

        # Удаляем дублирующиеся колонки с суффиксами _dup_
        dup_cols = [col for col in final_df.columns if '_dup_' in col]
        final_df = final_df.drop(columns=dup_cols)

        # Финальная очистка агрегированных строк
        final_df = clean_dataframe(final_df)

        # Пост-обработка данных для очистки и унификации
        final_df = clean_final_dataframe(final_df)

        # Сохраняем результат
        if not output_path:
            if simple_filename:
                # Для парсинга команды - используем простое имя в папке test_arsenal_players
                normalized_name = normalize_name(player_name)
                output_file = f'/root/data_platform/test_arsenal_players/{normalized_name}.csv'
            else:
                # Обычный режим - используем старый формат
                normalized_name = normalize_name(player_name)
                output_file = f'/root/data_platform/{normalized_name}_all_competitions.csv'
        else:
            output_file = output_path

        final_df.to_csv(output_file, index=False, encoding='utf-8')

        print(f"\n✅ Парсинг завершен успешно!")
        print(f"📊 Результат: {final_df.shape[0]} строк × {final_df.shape[1]} колонок")
        print(f"💾 Файл сохранен: {output_file}")

        # Показываем пример данных
        print(f"\n📋 Образец данных (первые 10 строк):")

        # Ищем колонки с сезонами и турнирами более гибко
        season_col = None
        squad_col = None
        comp_col = None

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
            # Если не нашли специфичные колонки, показываем первые 3 колонки
            print(final_df.iloc[:10, :3].to_string(index=False))
            print(f"Использованы первые 3 колонки: {list(final_df.columns[:3])}")

        return final_df

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
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
            squad_url = "https://fbref.com/en/squads/18bb7c10/2023-2024/Arsenal-Stats"
        elif args.squad_url:
            squad_url = args.squad_url

        if squad_url:
            result = parse_arsenal_squad(
                squad_url=squad_url,
                limit=args.limit,
                delay=args.delay
            )

            if result is not None and result > 0:
                print(f"\n🎉 Готово! Парсинг команды завершен. Успешно обработано {result} игроков.")
            else:
                print("\n💥 Парсинг команды не удался.")
                sys.exit(1)
        else:
            print("❌ Не указан URL команды")
            sys.exit(1)

    else:
        # Режим парсинга с FBref (по умолчанию)
        result = parse_from_fbref()
        if result is not None:
            print("\n🎉 Готово! Чистый CSV файл создан.")
        else:
            print("\n💥 Парсинг не удался.")
            sys.exit(1)

if __name__ == "__main__":
    main()