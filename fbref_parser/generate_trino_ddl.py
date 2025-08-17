#!/usr/bin/env python3
"""
Генератор SQL DDL для Trino на основе реальной структуры Parquet файла William Saliba
"""
import pandas as pd
import os
import re

def generate_trino_ddl():
    """Генерирует SQL DDL для создания таблицы в Trino"""
    
    # Проверяем наличие файла (сначала новый, потом старый)
    parquet_files = ["william_saliba_combined.parquet", "william_saliba.parquet"]
    df = None
    used_file = None
    
    for file_name in parquet_files:
        if os.path.exists(file_name):
            df = pd.read_parquet(file_name)
            used_file = file_name
            break
    
    if df is None:
        print("❌ Не найден ни один из файлов:")
        for file_name in parquet_files:
            print(f"   - {file_name}")
        print("💡 Сначала запустите парсер: python3 main.py")
        return
    columns = df.columns.tolist()
    
    print(f"📊 Анализ структуры данных William Saliba")
    print(f"Используемый файл: {used_file}")
    print(f"Всего колонок: {len(columns)}")
    print(f"Строк данных: {len(df)}")
    print()
    
    # Определяем имя таблицы на основе файла
    table_name = "william_saliba_combined" if "combined" in used_file else "william_saliba"
    file_pattern = "*combined.parquet" if "combined" in used_file else "*.parquet"
    
    # Генерируем SQL DDL
    sql_ddl = f"""-- Автоматически сгенерированный SQL DDL для William Saliba
-- Основан на реальной структуре Parquet файла: {used_file}
-- Запуск: docker exec -i trino trino < auto_create_saliba_table.sql

-- Создаем схему для William Saliba
CREATE SCHEMA IF NOT EXISTS fbref.saliba_data
WITH (location = 'hdfs://namenode:9000/data/silver/fbref/william_saliba/');

-- Удаляем таблицу если существует (для пересоздания)
DROP TABLE IF EXISTS fbref.saliba_data.{table_name};

-- Создаем таблицу William Saliba с автоматически определенной структурой
CREATE TABLE fbref.saliba_data.{table_name} (
"""
    
    # Добавляем колонки (безопасное экранирование идентификаторов)
    def is_safe_identifier(name: str) -> bool:
        return re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name) is not None

    def quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    for i, col in enumerate(columns):
        col_identifier = col if is_safe_identifier(col) else quote_identifier(col)
        sql_ddl += f"    {col_identifier} VARCHAR"
        if i < len(columns) - 1:
            sql_ddl += ","
        sql_ddl += "\n"
    
    sql_ddl += """)
WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/data/silver/fbref/william_saliba/'
);

-- Проверяем результат
SHOW TABLES FROM fbref.saliba_data;

-- Базовая информация о William Saliba (все сезоны и турниры)
SELECT 
    "Unnamed: 0_level_0_Season" as Season,
    "Unnamed: 1_level_0_Age" as Age,
    "Unnamed: 2_level_0_Squad" as Squad,
    "Unnamed: 4_level_0_Comp" as Competition,
    player_name,
    player_position,
    player_height,
    player_nationality,
    "standard_Performance_Gls" as goals,
    "standard_Performance_Ast" as assists,
    "standard_Unnamed: 5_level_0_MP" as matches_played,
    team,
    parsed_at
FROM fbref.saliba_data.{table_name}
ORDER BY "Unnamed: 0_level_0_Season", "Unnamed: 4_level_0_Comp";

-- Оборонительная статистика по сезонам
SELECT 
    "Unnamed: 0_level_0_Season" as Season,
    "Unnamed: 4_level_0_Comp" as Competition,
    "defense_Tackles_Tkl" as tackles,
    "defense_Tackles_TklW" as tackles_won,
    "defense_Unnamed: 18_level_0_Int" as interceptions,
    "defense_Blocks_Blocks" as blocks,
    "misc_Performance_CrdY" as yellow_cards,
    "misc_Performance_CrdR" as red_cards,
    "misc_Aerial Duels_Won%" as aerial_success_rate
FROM fbref.saliba_data.{table_name}
WHERE "defense_Tackles_Tkl" IS NOT NULL
  AND "defense_Tackles_Tkl" != ''
ORDER BY "Unnamed: 0_level_0_Season", "Unnamed: 4_level_0_Comp";

-- Статистика передач - все сезоны
SELECT 
    "Unnamed: 0_level_0_Season" as Season,
    "Unnamed: 4_level_0_Comp" as Competition,
    "passing_Total_Cmp" as passes_completed,
    "passing_Total_Att" as passes_attempted,
    "passing_Total_Cmp%" as pass_accuracy,
    "passing_Total_TotDist" as total_distance,
    "passing_Total_PrgDist" as progressive_distance,
    "passing_Long_Cmp%" as long_pass_accuracy
FROM fbref.saliba_data.{table_name}
WHERE "passing_Total_Att" IS NOT NULL
  AND "passing_Total_Att" != ''
ORDER BY "Unnamed: 0_level_0_Season", "Unnamed: 4_level_0_Comp";

-- Статистика владения мячом
SELECT 
    "Unnamed: 0_level_0_Season" as Season,
    "Unnamed: 4_level_0_Comp" as Competition,
    "possession_Touches_Touches" as total_touches,
    "possession_Carries_Carries" as carries,
    "possession_Carries_TotDist" as carry_distance,
    "possession_Take-Ons_Succ%" as takeOn_success_rate
FROM fbref.saliba_data.{table_name}
WHERE "possession_Touches_Touches" IS NOT NULL
  AND "possession_Touches_Touches" != ''
ORDER BY "Unnamed: 0_level_0_Season", "Unnamed: 4_level_0_Comp";

-- Сводная статистика по всем турнирам
SELECT 
    "Unnamed: 4_level_0_Comp" as competition,
    COUNT(*) as seasons_played,
    SUM(CAST("standard_Unnamed: 5_level_0_MP" AS INTEGER)) as total_matches,
    SUM(CAST("standard_Performance_Gls" AS INTEGER)) as total_goals,
    AVG(CAST("passing_Total_Cmp%" AS DOUBLE)) as avg_pass_accuracy,
    AVG(CAST("defense_Tackles_Tkl" AS INTEGER)) as avg_tackles
FROM fbref.saliba_data.{table_name}
WHERE "standard_Unnamed: 5_level_0_MP" IS NOT NULL
  AND "standard_Unnamed: 5_level_0_MP" != ''
  AND "Unnamed: 4_level_0_Comp" IS NOT NULL
GROUP BY "Unnamed: 4_level_0_Comp"
ORDER BY total_matches DESC;
"""
    
    # Сохраняем в файл
    output_file = "../auto_create_saliba_table.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(sql_ddl)
    
    print(f"✅ SQL DDL сгенерирован: {output_file}")
    print()
    print("💡 Для создания таблицы выполните:")
    print(f"   cd .. && docker exec -i trino trino < auto_create_saliba_table.sql")
    print()
    print("📋 Структура колонок:")
    print("=" * 60)
    
    # Группируем колонки по секциям (новая структура с префиксами)
    sections = {}
    for col in columns:
        if '_' in col and col not in ['player_name', 'player_position', 'player_height', 'player_weight', 'player_nationality', 'player_birth_date', 'team', 'parsed_at', 'data_source_section']:
            # Новая структура: section_Column_Name
            section = col.split('_')[0]
            if section not in sections:
                sections[section] = []
            sections[section].append(col)
        else:
            # Базовые колонки
            if 'basic' not in sections:
                sections['basic'] = []
            sections['basic'].append(col)
    
    for section, cols in sections.items():
        print(f"📊 {section.upper()}: {len(cols)} полей")
        if len(cols) <= 5:  # Показываем все если мало
            for col in cols:
                print(f"   - {col}")
        else:  # Показываем только первые 3
            for col in cols[:3]:
                print(f"   - {col}")
            print(f"   ... и еще {len(cols)-3} полей")
        print()

if __name__ == "__main__":
    generate_trino_ddl()
