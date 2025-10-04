#!/usr/bin/env python3
"""
Парсер для вратарей Arsenal с FBref.com
Извлекает специфичную для вратарей статистику с индивидуальных страниц игроков
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import numpy as np
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
        df = df[~df[season_col].astype(str).str.contains('Season|Seasons|Club|Clubs|Total|League', na=False)]

    # Дополнительная проверка для удаления строк с названиями колонок
    comp_cols = [col for col in df.columns if 'Comp' in str(col) and 'Competition' not in str(col)]
    if comp_cols:
        comp_col = comp_cols[0]
        # Удаляем строки где колонка турниров содержит служебные слова
        df = df[~df[comp_col].astype(str).str.contains('Comp|Competition|Country|Squad|MP|Min', na=False)]

    # Удаляем полностью пустые строки
    df = df.dropna(how='all')

    return df

def clean_final_dataframe_gk(df):
    """Пост-обработка DataFrame для очистки и унификации названий столбцов специфично для вратарей"""
    print("\n🧹 Начинаю пост-обработку данных вратаря...")

    # Расширенные переименования для всех типов статистики
    basic_renames = {
        # Основные столбцы
        'Season': 'season',
        'Age': 'age',
        'Squad': 'squad',
        'Country': 'country',
        'Comp': 'competition',
        'MP': 'matches_played',
        'Starts': 'starts',
        'Min': 'minutes',
        '90s': 'minutes_90',

        # Goalkeeping
        'GA': 'goals_against',
        'GA90': 'goals_against_per90',
        'SoTA': 'shots_on_target_against',
        'Saves': 'saves',
        'Save%': 'save_pct',
        'W': 'wins',
        'D': 'draws',
        'L': 'losses',
        'CS': 'clean_sheets',
        'CS%': 'clean_sheet_pct',
        'PKA': 'penalty_kicks_attempted',
        'PKsv': 'penalty_kicks_saved',
        'PKm': 'penalty_kicks_missed',
        'PSxG': 'post_shot_expected_goals',
        'PSxG/SoT': 'psxg_per_shot_on_target',
        'PSxG+/-': 'psxg_net',
        '/90': 'per_90_minutes',

        # Passing
        'Cmp': 'passes_completed',
        'Att': 'passes_attempted',
        'Cmp%': 'pass_completion_pct',
        'TotDist': 'total_pass_distance',
        'PrgDist': 'progressive_pass_distance',
        'AvgLen': 'avg_pass_length',
        'Launched': 'long_passes_attempted',
        'Launch%': 'long_pass_pct',

        # Standard stats
        'Gls': 'goals',
        'Ast': 'assists',
        'G+A': 'goals_plus_assists',
        'G-PK': 'non_penalty_goals',
        'PK': 'penalty_kicks_made',
        'PKatt': 'penalty_kicks_attempted',
        'xG': 'expected_goals',
        'npxG': 'non_penalty_expected_goals',
        'xA': 'expected_assists',

        # Shooting
        'Sh': 'shots',
        'SoT': 'shots_on_target',
        'SoT%': 'shots_on_target_pct',
        'G/Sh': 'goals_per_shot',
        'G/SoT': 'goals_per_shot_on_target',

        # Defense
        'Tkl': 'tackles',
        'TklW': 'tackles_won',
        'Def 3rd': 'tackles_def_3rd',
        'Mid 3rd': 'tackles_mid_3rd',
        'Att 3rd': 'tackles_att_3rd',
        'Int': 'interceptions',
        'Blocks': 'blocks',

        # Possession
        'Touches': 'touches',
        'Def Pen': 'touches_def_pen_area',
        'Live': 'live_ball_touches',
        'Carries': 'carries',
        'Take-Ons': 'take_ons',

        # GCA/SCA
        'GCA': 'goal_creating_actions',
        'GCA90': 'goal_creating_actions_per90',
        'SCA': 'shot_creating_actions',
        'SCA90': 'shot_creating_actions_per90',

        # Miscellaneous
        'CrdY': 'yellow_cards',
        'CrdR': 'red_cards',
        'Fls': 'fouls_committed',
        'Fld': 'fouls_drawn',
        'Recov': 'ball_recoveries',
        'Won': 'aerial_duels_won',
        'Lost': 'aerial_duels_lost',
        'Won%': 'aerial_duels_won_pct',

        # Pass Types
        'Live': 'live_passes',
        'Dead': 'dead_passes',
        'FK': 'free_kicks',
        'TB': 'through_balls',
        'Sw': 'switches',
        'Crs': 'crosses',
        'TI': 'throw_ins',
        'CK': 'corner_kicks',

        # Goalkeeper specific
        'Opp': 'crosses_stopped',
        'Stp': 'crosses_stopped_pct',
        'Stp%': 'cross_stop_pct',
        '#OPA': 'defensive_actions_outside_penalty_area',
        'AvgDist': 'avg_distance_defensive_actions'
    }

    # Применяем только те переименования, которые существуют в DataFrame
    existing_renames = {old: new for old, new in basic_renames.items() if old in df.columns}
    df = df.rename(columns=existing_renames)
    print(f"   Переименовано {len(existing_renames)} столбцов")

    # Конвертация в snake_case для остальных столбцов
    def convert_to_snake_case(column_name):
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

        return col

    # Применяем конвертацию ко всем столбцам
    new_columns = [convert_to_snake_case(col) for col in df.columns]
    df.columns = new_columns

    # Удаляем дублирующиеся столбцы
    df = df.loc[:, ~df.columns.duplicated()]

    # Удаляем дублирующиеся столбцы MP (playing_time_mp если есть matches_played)
    if 'matches_played' in df.columns and 'playing_time_mp' in df.columns:
        df = df.drop(columns=['playing_time_mp'])
        print(f"   Удален дубликат playing_time_mp (оставлен matches_played)")

    # Очистка данных в столбцах
    for col in df.columns:
        if df[col].dtype == 'object':
            # Удаляем коды стран из названий команд (например, "eng Arsenal" -> "Arsenal")
            if 'squad' in col.lower():
                df[col] = df[col].astype(str).str.replace(r'^[a-z]{2,3}\s+', '', regex=True)

    # Очищаем Country (убираем префиксы типа "eng ENG" -> "ENG")
    if 'country' in df.columns:
        df['country'] = df['country'].astype(str).str.replace(r'^[a-z]+ ', '', regex=True)
        df['country'] = df['country'].replace('nan', '')

    # Очищаем Competition (убираем номера лиг типа "2. Championship" -> "Championship")
    if 'competition' in df.columns:
        df['competition'] = df['competition'].astype(str).str.replace(r'^\d+\. ', '', regex=True)
        # Дополнительная очистка
        df['competition'] = df['competition'].str.replace('Jr. PL2 — Div. 1', 'PL2 Div 1')

    print(f"🎯 Пост-обработка завершена. Итоговых столбцов: {len(df.columns)}")
    return df

def normalize_name(name):
    """Нормализует имя игрока для создания имени файла"""
    # Убираем все неалфавитные символы и заменяем пробелы на подчеркивания
    normalized = re.sub(r'[^a-zA-Z\s]', '', name)
    normalized = re.sub(r'\s+', '_', normalized.strip())
    return normalized.lower()

def extract_goalkeeper_links(squad_url):
    """Извлекает ссылки на всех вратарей со страницы команды"""
    print(f"🥅 Извлекаю ссылки на вратарей с: {squad_url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(squad_url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Ищем таблицу со стандартной статистикой
        standard_stats_table = soup.find('table', {'id': 'stats_standard_9'})
        if not standard_stats_table:
            standard_stats_table = soup.find('table', {'id': 'stats_standard_combined'})

        # Если не нашли по точному ID, пробуем альтернативные варианты
        if not standard_stats_table:
            all_tables = soup.find_all('table')
            alternative_ids = ['all_stats_standard', 'stats_standard']
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

        goalkeeper_links = []

        # Извлекаем строки с игроками из tbody
        tbody = standard_stats_table.find('tbody')
        if not tbody:
            print("❌ Не найден tbody в таблице")
            return []

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

            # Оставляем ТОЛЬКО вратарей
            if 'GK' not in position.upper():
                continue

            # Ищем ссылку на игрока
            player_link = player_cell.find('a')
            if player_link and player_link.get('href'):
                href = player_link.get('href')

                # Проверяем что это ссылка на страницу игрока
                if '/players/' in href:
                    player_name = player_cell.get_text(strip=True)

                    # Проверяем на дубликаты
                    if any(existing_name == player_name for existing_name, _ in goalkeeper_links):
                        continue

                    # Конвертируем в URL всех турниров
                    if '/all_comps/' not in href:
                        # Заменяем часть URL для получения статистики по всем турнирам
                        href = re.sub(r'(/players/[^/]+/)\d{4}-\d{4}/', r'\1all_comps/', href)
                        href = re.sub(r'/[^/]*-Stats$', r'Stats---All-Competitions', href)
                        if not href.endswith('Stats---All-Competitions'):
                            # Если замена не сработала, формируем URL заново
                            player_id = href.split('/players/')[1].split('/')[0]
                            normalized_name = player_name.replace(' ', '-')
                            href = f"/en/players/{player_id}/all_comps/{normalized_name}-Stats---All-Competitions"

                    full_url = f"https://fbref.com{href}"
                    goalkeeper_links.append((player_name, full_url))

        print(f"✅ Найдено {len(goalkeeper_links)} вратарей:")
        for name, _ in goalkeeper_links:
            print(f"   - {name}")

        return goalkeeper_links

    except Exception as e:
        print(f"❌ Ошибка при извлечении ссылок на вратарей: {e}")
        return []

def identify_gk_tables(all_tables):
    """Определяет и классифицирует таблицы для вратарей (включая всю статистику)"""

    # Ключевые слова для определения типов таблиц (расширенная версия)
    gk_table_patterns = {
        'goalkeeping': ['GA', 'Save%', 'Saves', 'SoTA', 'CS'],
        'advanced_goalkeeping': ['PSxG', 'PSxG/SoT', 'PSxG+/-', 'PKA', 'PKsv', 'PKm'],
        'standard': ['Gls', 'Ast', 'G+A', 'PK', 'PKatt'],
        'shooting': ['Sh', 'SoT', 'SoT%', 'G/Sh', 'G/SoT'],
        'passing': ['Cmp', 'Att', 'Cmp%', 'TotDist', 'PrgDist', 'PrgP'],
        'pass_types': ['Live', 'Dead', 'FK', 'TB', 'Sw', 'Crs', 'TI', 'CK'],
        'gca': ['GCA', 'SCA', 'GCA90', 'SCA90'],
        'defense': ['Tkl', 'TklW', 'Def 3rd', 'Mid 3rd', 'Att 3rd', 'Blocks', 'Int'],
        'possession': ['Touches', 'Def Pen', 'Def 3rd', 'Mid 3rd', 'Att 3rd', 'Live', 'Carries', 'Take-Ons'],
        'playing_time': ['MP', 'Starts', 'Min', '90s', 'Mn/MP', 'Min%', 'Mn/Start'],
        'miscellaneous': ['CrdY', 'CrdR', 'Fls', 'Fld', 'Recov', 'Won', 'Lost', 'Won%'],
        'match_logs': ['Date', 'Day', 'Venue', 'Result', 'Opponent']
    }

    identified_tables = {
        'goalkeeping': [],
        'advanced_goalkeeping': [],
        'standard': [],
        'shooting': [],
        'passing': [],
        'pass_types': [],
        'gca': [],
        'defense': [],
        'possession': [],
        'playing_time': [],
        'miscellaneous': [],
        'match_logs': []
    }

    for i, table in enumerate(all_tables):
        # Пропускаем маленькие таблицы (< 10 строк), чтобы исключить "Last 5 Matches"
        # Это предотвращает дублирование match logs столбцов в других таблицах
        if len(table) < 10:
            continue

        if isinstance(table.columns, pd.MultiIndex):
            columns = table.columns.get_level_values(-1).tolist()
        else:
            columns = list(table.columns)

        # Чистим названия столбцов
        clean_columns = [str(col).strip() for col in columns]

        # Проверяем каждый тип таблицы
        for table_type, keywords in gk_table_patterns.items():
            matches = sum(1 for keyword in keywords if any(keyword in col for col in clean_columns))

            # Если найдены характерные столбцы, классифицируем таблицу
            if matches >= 2:  # Минимум 2 совпадения для классификации
                identified_tables[table_type].append({
                    'index': i,
                    'table': table,
                    'matches': matches,
                    'columns': clean_columns[:10]  # Первые 10 столбцов для отладки
                })

    return identified_tables

def parse_goalkeeper_stats(player_name, player_url):
    """Парсит статистику вратаря со страницы FBref"""
    print(f"\n🥅 Парсинг {player_name}...")
    print(f"🔗 URL: {player_url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(player_url, headers=headers)
        response.raise_for_status()
        time.sleep(1)  # Задержка для уважительного отношения к серверу

        # Читаем все таблицы со страницы
        all_tables = pd.read_html(response.text, encoding='utf-8')
        print(f"📊 Найдено {len(all_tables)} таблиц на странице")

        # Идентифицируем таблицы специфичные для вратарей
        identified_tables = identify_gk_tables(all_tables)

        # Выводим информацию о найденных таблицах
        for table_type, tables in identified_tables.items():
            if tables:
                print(f"   {table_type}: {len(tables)} таблиц")

        # Объединяем все найденные данные
        all_data = []

        for table_type, tables in identified_tables.items():
            if not tables:
                continue

            # Выбираем лучшую таблицу для каждого типа (с наибольшим количеством совпадений)
            best_table = max(tables, key=lambda x: x['matches'])
            table_data = best_table['table'].copy()

            print(f"   Обрабатываю {table_type} (таблица {best_table['index']})...")

            # Обрабатываем MultiIndex колонки если есть (как в main.py)
            if isinstance(table_data.columns, pd.MultiIndex):
                # Объединяем ОБА уровня, чтобы избежать потери дубликатов
                new_columns = []
                for col in table_data.columns:
                    if isinstance(col, tuple):
                        # Объединяем уровни заголовков через подчеркивание
                        clean_col = '_'.join([str(c) for c in col if str(c) != 'nan' and str(c).strip()])
                        # Убираем множественные подчеркивания
                        clean_col = re.sub(r'_+', '_', clean_col).strip('_')
                    else:
                        clean_col = str(col)
                    new_columns.append(clean_col)
                table_data.columns = new_columns

                # Применяем fix_column_names для очистки "Unnamed:"
                table_data.columns = fix_column_names(table_data.columns)
            else:
                table_data.columns = fix_column_names(table_data.columns)

            # Удаляем столбцы Playing Time из других таблиц (оставляем только из playing_time)
            if table_type != 'playing_time':
                # Паттерны для Playing Time столбцов в MultiIndex и обычных таблицах
                playing_time_patterns = [
                    r'Playing[_ ]Time[_ ]',  # MultiIndex: "Playing Time_Starts", "Playing_Time_Starts" etc.
                    r'^(MP|Starts|Min|90s|Mn/MP|Min%|Mn/Start|Compl)$',  # Точные совпадения (добавлен MP)
                    r'Performance_(Starts|Min|90s)',  # Из Goalkeeping: "Performance_Starts"
                    r'Team_Success_',  # Team Success столбцы из Playing Time таблицы
                    r'Subs_',  # Substitution-related columns
                ]

                # Ищем столбцы для удаления по паттернам
                cols_to_drop = []
                for col in table_data.columns:
                    col_str = str(col)
                    # Проверяем каждый паттерн
                    if any(re.search(pattern, col_str, re.IGNORECASE) for pattern in playing_time_patterns):
                        cols_to_drop.append(col)

                if cols_to_drop:
                    table_data = table_data.drop(columns=cols_to_drop)
                    print(f"   Удалено {len(cols_to_drop)} столбцов Playing Time из {table_type}: {cols_to_drop}")

            # Добавляем префикс к колонкам (кроме ключевых), как в main.py
            key_columns = ['Season', 'Age', 'Squad', 'Country', 'Comp', 'LgRank', 'MP']
            new_columns = []

            for col in table_data.columns:
                col_str = str(col)
                # Проверяем, является ли колонка ключевой
                is_key = any(key in col_str for key in key_columns)

                # Standard таблица не получает префиксы (как в main.py)
                # Для вратарей также не добавляем префиксы к goalkeeping и advanced_goalkeeping
                if is_key or table_type in ['standard', 'goalkeeping', 'advanced_goalkeeping']:
                    new_columns.append(col_str)
                else:
                    new_columns.append(f"{col_str}_{table_type}")

            table_data.columns = new_columns

            # Очищаем данные от агрегированных строк
            table_data = clean_dataframe(table_data)

            if not table_data.empty:
                # Добавляем таблицу в список для объединения
                all_data.append(table_data)

        if not all_data:
            print("❌ Не найдены данные для вратаря")
            return None

        # Объединяем все таблицы (как в main.py)
        print("🔗 Объединяю таблицы...")

        # Ключевые столбцы для объединения
        key_columns = ['Season', 'Age', 'Squad', 'Country', 'Comp']

        # Начинаем с первой таблицы
        merged_data = all_data[0]

        # Объединяем остальные таблицы
        for i in range(1, len(all_data)):
            try:
                # Находим общие ключевые столбцы
                key_columns_present = [col for col in key_columns if col in merged_data.columns and col in all_data[i].columns]

                if key_columns_present:
                    # Объединяем по ключевым столбцам с автоматическими суффиксами
                    merged_data = pd.merge(
                        merged_data,
                        all_data[i],
                        on=key_columns_present,
                        how='outer',
                        suffixes=('', '_dup')
                    )
                    print(f"   Объединил таблицу {i+1} по ключам: {key_columns_present}")
                else:
                    # Если нет общих ключей, конкатенируем по индексам
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

        # Удаляем дублирующиеся столбцы после объединения
        print("   Удаляю дублирующиеся столбцы...")

        # Удаляем столбцы с суффиксом _dup (дубликаты ключевых столбцов)
        dup_columns = [col for col in merged_data.columns if str(col).endswith('_dup')]
        if dup_columns:
            merged_data = merged_data.drop(columns=dup_columns, errors='ignore')
            print(f"   Удалено {len(dup_columns)} дублирующихся столбцов с суффиксом _dup")

        # Удаляем полностью идентичные столбцы
        merged_data = merged_data.loc[:, ~merged_data.columns.duplicated()]

        # Финальная обработка данных
        final_data = clean_final_dataframe_gk(merged_data)

        print(f"✅ Обработка завершена. Строк: {len(final_data)}, Столбцов: {len(final_data.columns)}")

        return final_data

    except Exception as e:
        print(f"❌ Ошибка при парсинге {player_name}: {e}")
        return None

def parse_arsenal_goalkeepers(squad_url="https://fbref.com/en/squads/18bb7c10/2025-2026/all_comps/Arsenal-Stats-All-Competitions"):
    """Парсит всех вратарей Arsenal"""
    print("🏴󠁧󠁢󠁥󠁮󠁧󠁿 Начинаю парсинг вратарей Arsenal...")

    # Создаем директорию для результатов если её нет
    output_dir = "/root/data_platform/test_arsenal_goalkeepers"
    os.makedirs(output_dir, exist_ok=True)

    # Получаем ссылки на вратарей
    goalkeeper_links = extract_goalkeeper_links(squad_url)

    if not goalkeeper_links:
        print("❌ Не найдены вратари для парсинга")
        return

    successful_parses = 0

    for i, (player_name, player_url) in enumerate(goalkeeper_links):
        print(f"\n{'='*60}")
        print(f"🥅 Вратарь {i+1}/{len(goalkeeper_links)}: {player_name}")

        # Парсим статистику вратаря
        player_data = parse_goalkeeper_stats(player_name, player_url)

        if player_data is not None and not player_data.empty:
            # Сохраняем данные в CSV
            normalized_name = normalize_name(player_name)
            output_path = os.path.join(output_dir, f"{normalized_name}_goalkeeper_stats.csv")

            try:
                player_data.to_csv(output_path, index=False, encoding='utf-8')
                print(f"💾 Данные сохранены: {output_path}")
                successful_parses += 1

                # Выводим краткую статистику
                print(f"📈 Статистика: {len(player_data)} сезонов, {len(player_data.columns)} показателей")

            except Exception as e:
                print(f"❌ Ошибка сохранения для {player_name}: {e}")

        # Задержка между запросами
        if i < len(goalkeeper_links) - 1:
            time.sleep(2)

    print(f"\n🎯 Парсинг завершен!")
    print(f"✅ Успешно обработано: {successful_parses}/{len(goalkeeper_links)} вратарей")
    print(f"📁 Файлы сохранены в: {output_dir}")

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

    try:
        if args.url:
            # Парсим одного конкретного вратаря
            print("🥅 Режим парсинга одного вратаря")

            # Извлекаем имя игрока из URL
            if '/players/' in args.url:
                player_id_part = args.url.split('/players/')[1]
                if '/all_comps/' in player_id_part:
                    player_name_part = player_id_part.split('/all_comps/')[1]
                    player_name = player_name_part.split('-Stats')[0].replace('-', ' ')
                else:
                    # Пробуем извлечь имя из других частей URL
                    parts = player_id_part.split('/')
                    if len(parts) > 1:
                        player_name = parts[-1].replace('-', ' ').split('-Stats')[0]
                    else:
                        player_name = "Goalkeeper"
            else:
                player_name = "Goalkeeper"

            # Проверяем, что URL содержит all_comps
            if '/all_comps/' not in args.url:
                print("⚠️ URL не содержит '/all_comps/' - добавляю автоматически")
                # Пытаемся преобразовать URL
                if '/players/' in args.url:
                    base_url = args.url.split('/players/')[0]
                    player_part = args.url.split('/players/')[1]
                    player_id = player_part.split('/')[0]
                    normalized_name = player_name.replace(' ', '-')
                    args.url = f"{base_url}/players/{player_id}/all_comps/{normalized_name}-Stats---All-Competitions"

            print(f"🎯 Парсинг: {player_name}")
            print(f"🔗 URL: {args.url}")

            # Парсим вратаря
            player_data = parse_goalkeeper_stats(player_name, args.url)

            if player_data is not None and not player_data.empty:
                # Определяем имя файла
                if args.output:
                    output_path = args.output
                else:
                    normalized_name = normalize_name(player_name)
                    output_path = f"/root/data_platform/{normalized_name}_goalkeeper_stats.csv"

                # Сохраняем данные
                player_data.to_csv(output_path, index=False, encoding='utf-8')
                print(f"✅ Данные сохранены: {output_path}")
                print(f"📈 Статистика: {len(player_data)} сезонов, {len(player_data.columns)} показателей")

            else:
                print("❌ Не удалось получить данные вратаря")
                sys.exit(1)

        else:
            # Парсим всех вратарей Arsenal
            print("🏴󠁧󠁢󠁥󠁮󠁧󠁿 Режим парсинга всех вратарей Arsenal")
            parse_arsenal_goalkeepers(args.squad_url)

    except KeyboardInterrupt:
        print("\n⏹️ Парсинг прерван пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
