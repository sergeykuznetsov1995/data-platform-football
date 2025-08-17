# William Saliba Playwright Parser

Продвинутый FBRef парсер на основе Playwright для демонстрации платформы данных.
Парсит все секции статистики William Saliba (Arsenal) в одну объединенную таблицу.

## 📊 Структура директории

```
fbref_parser/
├── main.py           # Минимальный парсер William Saliba
├── Dockerfile        # Docker образ для Airflow
├── requirements.txt  # Python зависимости
├── build_and_run.sh  # Скрипт сборки и запуска
└── README.md        # Эта документация
```

## 🎯 Что парсится

**William Saliba (Arsenal) - ВСЕ КАРЬЕРНЫЕ ДАННЫЕ В ОДНОЙ ТАБЛИЦЕ:**

### 👤 Базовые данные
- ✅ **name**: William Saliba
- ✅ **position**: DF (CB, right)  
- ✅ **height**: 193cm
- ✅ **weight**: 76kg
- ✅ **nationality**: France fr
- ✅ **birth_date**: March 24, 2001

### 📊 Все статистические секции (ВСЕ СЕЗОНЫ И ТУРНИРЫ)
- ✅ **📊 Standard Stats** - Основная статистика (голы, ассисты, матчи)
- ✅ **🎯 Shooting Stats** - Статистика ударов (удары, точность, xG)
- ✅ **🎾 Passing Stats** - Статистика передач (точность, дистанция)
- ✅ **📈 Pass Types** - Типы передач (длинные, короткие, средние)
- ✅ **🎨 Goal Creation** - Создание голевых моментов (SCA, GCA)
- ✅ **🛡️ Defensive Actions** - Оборонительные действия (отборы, перехваты)
- ✅ **⚽ Possession** - Владение мячом (касания, дриблинг)
- ✅ **⏱️ Playing Time** - Время на поле (минуты, процент)
- ✅ **📋 Miscellaneous** - Дополнительная статистика (карточки, фолы, воздушные дуэли)

### ⚡ Новые возможности Playwright
- ✅ **🔗 Объединенная таблица** - Все секции в одном DataFrame
- ✅ **🧹 Очистка названий лиг** - "1. Premier League" → "Premier League"
- ✅ **🚫 Исключение matchlogs** - Не парсим секцию last_5_matchlogs
- ✅ **📊 Множественные сезоны** - Данные по всем сезонам карьеры
- ✅ **🏆 Все турниры** - Premier League, Ligue 1, Europa League и т.д.

### 📈 Объем данных
- **📊 Строк**: ~40-50 (по сезон/турнир)
- **📊 Колонок**: ~100+ (все метрики из всех секций)
- **📁 Размер данных**: ~200KB+

## 🚀 Запуск

### 1. Локальный запуск

```bash
cd fbref_parser
python3 main.py
```

### 2. Через Docker

```bash
# Сборка образа
docker build -t fbref-simple-parser:latest .

# Запуск (локально)
docker run --rm fbref-simple-parser:latest

# Запуск (с HDFS)
docker run --rm \
  -e HDFS_WEB_URL="http://namenode:9870" \
  -e HDFS_USER="airflow" \
  --network data-platform_data-platform \
  fbref-simple-parser:latest
```

### 3. Через Airflow

1. Соберите Docker образ:
   ```bash
   cd fbref_parser
   ./build_and_run.sh  # Или вручную: docker build -t fbref-simple-parser:latest .
   ```

2. Запустите DAG в Airflow UI:
   - Откройте http://localhost:8082
   - Найдите DAG: `william_saliba_parser`
   - Запустите вручную

### 4. Быстрый старт

```bash
cd fbref_parser
./build_and_run.sh

# Полный workflow до Trino анализа:
python3 main.py                    # Парсинг данных
python3 generate_trino_ddl.py     # Генерация SQL схемы
./upload_to_hdfs.sh               # Загрузка в HDFS
docker exec -i trino trino < auto_create_saliba_table.sql  # Создание таблиц
```

## 💾 Выходные данные

### Локально
Файл: `william_saliba_combined.parquet`

### HDFS
Путь: `/fbref/william_saliba/william_saliba_combined.parquet`

### Формат данных (объединенная таблица)
```python
# Пример строки из объединенной таблицы
{
    # Общие колонки (во всех строках)
    'Season': '2024-25',
    'Age': 23,
    'Squad': 'Arsenal',
    'Country': 'eng ENG',
    'Comp': 'Premier League',  # Очищено от "1. "
    'LgRank': '2nd',
    '90s': 33.8,
    
    # Статистика по секциям (с префиксами)
    'standard_MP': 35,
    'standard_Gls': 2,
    'standard_Ast': 0,
    'shooting_Sh': 6,
    'shooting_SoT': 2,
    'shooting_SoT%': 33.3,
    'passing_Cmp': 2413,
    'passing_Att': 2559,
    'passing_Cmp%': 94.3,
    'defense_Tkl': 62,
    'defense_TklW': 37,
    'defense_Int': 83,
    'possession_Touches': 2824,
    'misc_CrdY': 5,
    'misc_CrdR': 0,
    
    # Мета-информация
    'player_name': 'William Saliba',
    'player_position': 'DF (CB, right)',
    'player_height': '193cm',
    'player_nationality': 'France',
    'team': 'arsenal',
    'data_source_section': 'standard',
    'parsed_at': '2025-08-03T15:30:00.123456'
}
```

## ⚙️ Архитектура (очищено от дублирования)

**Разделение ответственности:**
- **`main.py`** - только парсинг данных и сохранение
- **`generate_trino_ddl.py`** - только генерация SQL схем
- **`upload_to_hdfs.sh`** - только загрузка в HDFS
- **`build_and_run.sh`** - только сборка Docker образа

**Убрано:**
- ❌ Дублирование DDL логики в main.py
- ❌ Неиспользуемые переменные (excluded_sections)
- ❌ Неиспользуемые импорты (subprocess, requests)
- ❌ Тестовые файлы (test_parser_local.py, check_results.py)

## 🔧 Настройка Trino

### Автоматическое создание таблиц:
```bash
# Генерация SQL DDL на основе реальной структуры данных:
python3 generate_trino_ddl.py
docker exec -i trino trino < auto_create_saliba_table.sql
```

### Примеры SQL запросов:

#### Базовая информация о всех сезонах:
```sql
SELECT Season, Age, Squad, Comp, 
       player_name, player_position, player_height, player_nationality,
       standard_MP as matches, standard_Gls as goals, standard_Ast as assists
FROM fbref.saliba_data.william_saliba_combined
ORDER BY Season;
-- Показывает прогрессию по сезонам
```

#### Оборонительная статистика по турнирам:
```sql
SELECT Season, Comp, 
       defense_Tkl as tackles,
       defense_TklW as tackles_won,
       defense_Int as interceptions,
       defense_Blocks as blocks
FROM fbref.saliba_data.william_saliba_combined
WHERE defense_Tkl IS NOT NULL
ORDER BY Season, Comp;
-- Сравнение оборонительных действий по турнирам
```

#### Статистика передач - лучшие сезоны:
```sql  
SELECT Season, Comp,
       passing_Cmp as passes_completed,
       passing_Att as passes_attempted,
       "passing_Cmp%" as pass_accuracy,
       passing_TotDist as total_distance
FROM fbref.saliba_data.william_saliba_combined
WHERE passing_Att IS NOT NULL
ORDER BY "passing_Cmp%" DESC;
-- Ранжирование сезонов по точности передач
```

#### Сводная статистика по лигам:
```sql
SELECT Comp,
       COUNT(*) as seasons_played,
       SUM(standard_MP) as total_matches,
       SUM(standard_Gls) as total_goals,
       AVG("passing_Cmp%") as avg_pass_accuracy,
       AVG(defense_Tkl) as avg_tackles_per_season
FROM fbref.saliba_data.william_saliba_combined
WHERE standard_MP IS NOT NULL
GROUP BY Comp
ORDER BY total_matches DESC;
-- Агрегированная статистика по турнирам
```

## ⚙️ Переменные окружения

- `HDFS_WEB_URL`: URL HDFS WebHDFS API (по умолчанию: `http://namenode:9870`)
- `HDFS_USER`: Пользователь HDFS (по умолчанию: `airflow`)

## 📝 Детальные логи

Парсер теперь выводит очень подробные логи для отладки:

### 🎯 Начальная информация
```
🎯 WILLIAM SALIBA PARSER - DETAILED LOGS
👤 Target Player: William Saliba (Arsenal)
🔗 Main URL: https://fbref.com/en/players/972aeb2a/William-Saliba
📊 Scouting Report: https://fbref.com/en/players/972aeb2a/scout/365_m1/William-Saliba-Scouting-Report
⚽ Season: 2024/2025
```

### 📋 Структура извлеченных данных

Парсер теперь логирует данные из каждой секции отдельно:

#### 🎯 Shooting Stats Section
```
🔗 Section URL: https://fbref.com/en/players/972aeb2a/William-Saliba#stats_shooting_dom_lg
📊 Table Shape: (19, 25)
📊 CSV Headers:
Season,Age,Squad,Country,Comp,LgRank,90s,Gls,Sh,SoT,SoT%,Sh/90,SoT/90,G/Sh,G/SoT,Dist,FK,PK,PKatt,xG,npxG,npxG/Sh,G-xG,np:G-xG,Matches

📊 CSV Values (2024-2025):
2024-2025,23,Arsenal,eng ENG,1. Premier League,2nd,33.8,2,6,2,33.3,0.18,0.06,0.33,1.00,5.4,0,0,0,2.3,2.3,0.38,-0.3,-0.3,Matches
```

#### 🛡️ Defensive Actions Section  
```
🔗 Section URL: https://fbref.com/en/players/972aeb2a/William-Saliba#stats_defense_dom_lg
📊 Table Shape: (19, 24)
📊 CSV Values (2024-2025):
2024-2025,23,Arsenal,eng ENG,1. Premier League,2nd,33.8,62,37,37,23,2,21,28,75.0,7,25,15,10,21,83,119,7,Matches
```

#### 📊 Итоговая объединенная структура (251 поле)
```
📊 DataFrame Shape: (1, 251)
📊 Total fields extracted: 251
📁 File size: 175454 bytes
```

### 🎉 Итоговый отчет
```
🎉 WILLIAM SALIBA PARSER - FINAL REPORT
⏱️ Total Duration: 0:00:03.403735
👤 Player: William Saliba
⚽ Team: ARSENAL
📅 Season: 2024-2025
💾 Data saved to HDFS: ❌ NO (local fallback)
📊 Total fields extracted: 251
📋 Tables found: 11
📁 File size: ~175KB

🔗 DETAILED SECTION LINKS:
   🎯 Scouting Report: https://fbref.com/en/players/972aeb2a/William-Saliba#all_scout_summary
   📊 Standard Stats: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_standard
   🎯 Shooting Stats: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_shooting
   🎾 Passing Stats: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_passing
   📈 Pass Types: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_pass_types
   🎨 Goal Creation: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_gca
   🛡️ Defensive Actions: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_defense
   ⚽ Possession: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_possession
   ⏱️ Playing Time: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_playing_time
   📋 Miscellaneous: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_misc
```

## 🛡️ Playwright преимущества

- ✅ **Реальный браузер** - Полная поддержка JavaScript и динамического контента
- ✅ **Headless режим** - Быстрая работа без GUI
- ✅ **Стабильность** - Ожидание загрузки страницы (networkidle)
- ✅ **Селекторы** - Точные CSS/XPath селекторы
- ✅ **Anti-detection** - Больше похоже на реального пользователя

## 🎯 Использование

Этот парсер создан для демонстрации работы платформы данных:
- Извлечение данных из внешних источников (FBRef.com)
- Детальное логирование процесса и результатов
- Сохранение в HDFS с fallback на локальное хранение
- Интеграция с Airflow
- Анализ через Trino

### 📊 Дополнительные источники данных

Парсер автоматически находит и логирует ссылки на все секции:

**Прямые ссылки на секции страницы:**
- `🎯 Scouting Report`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_scout_summary
- `📊 Standard Stats`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_standard  
- `🎯 Shooting Stats`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_shooting
- `🎾 Passing Stats`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_passing
- `📈 Pass Types`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_pass_types
- `🎨 Goal Creation`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_gca
- `🛡️ Defensive Actions`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_defense
- `⚽ Possession`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_possession
- `⏱️ Playing Time`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_playing_time
- `📋 Miscellaneous`: https://fbref.com/en/players/972aeb2a/William-Saliba#all_stats_misc

**Дополнительные ресурсы:**
- **Scouting Report**: Детальная статистика с процентилями vs других центр-беков
- **Arsenal Squad**: Командная статистика за сезон  
- **Player Page**: Полная карьерная статистика

### 🚀 Production готовность

**Текущие возможности (готово к использованию):**
- ✅ Парсинг всех 11 секций страницы игрока
- ✅ Извлечение 251 поля данных за сезон 2024/2025
- ✅ Детальное логирование структуры каждой таблицы
- ✅ Прямые ссылки на каждую секцию с якорными ссылками
- ✅ CSV-формат логирования для каждой секции
- ✅ Anti-blocking механизмы
- ✅ Fallback на локальное сохранение при недоступности HDFS
- ✅ Docker контейнеризация
- ✅ Интеграция с Airflow

**Для production расширения рекомендуется добавить:**
- 🔄 Больше игроков/команд (парсинг состава Arsenal)
- 📊 Парсинг исторических данных (предыдущие сезоны)
- ⚡ Инкрементальные обновления
- 📱 Мониторинг и алерты
- 🔄 Retry логику для HDFS
- 🚦 Обработка rate limiting
- 📈 Парсинг матчевых данных
- 🏆 Парсинг турнирных статистик
