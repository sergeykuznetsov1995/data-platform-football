# FBRef: парсинг игрока (пример: William Saliba)

## Цели
- Профиль игрока: био и постоянные атрибуты → `silver_fbref_player_profile`
- Сезонные статистики по всем соревнованиям и по каждому соревнованию → `silver_fbref_player_season_stats`

## Источник
- URL (All Competitions): `https://fbref.com/en/players/972aeb2a/all_comps/William-Saliba-Stats---All-Competitions`
- Идентификатор игрока: `fbref_id=972aeb2a`

## Таблицы/секции и селекторы
- Стандартная статистика: `#all_stats_standard`
- Удары: `#all_stats_shooting`
- Пасы: `#all_stats_passing`
- Типы пасов: `#all_stats_pass_types`
- Создание моментов: `#all_stats_gca`
- Оборона: `#all_stats_defense`
- Владение: `#all_stats_possession`
- Время на поле: `#all_stats_playing_time`
- Прочее: `#all_stats_misc`

## Нормализация
- `Comp`: удалить порядковый префикс (`^\d+\.\s+`) → чистое имя турнира
- Единицы: привести дистанции к метрам; проценты → доли/проценты по согласованной схеме
- Поля дат: `season` как `YYYY-YYYY`, `ingest_date` как `YYYY-MM-DD`
- Консолидация: одна строка = `(season, comp_name, squad)`; для All competitions `comp_name = "All Competitions"`

## Пример профиля (NDJSON)
```json
{"source":"fbref","fbref_id":"972aeb2a","player_slug":"William-Saliba","full_name":"William Alain André Gabriel Saliba","known_as":"William Saliba","birth_date":"2001-03-24","age":24,"nationalities":["FRA"],"height_cm":192,"weight_kg":85,"foot":"Right","positions":["DF","CB"],"current_club":"Arsenal","shirt_number":2,"source_url":"https://fbref.com/en/players/972aeb2a/all_comps/William-Saliba-Stats---All-Competitions","ingest_ts":"2025-08-11T17:40:00Z"}
```

## Пример сезонной строки (NDJSON)
```json
{"source":"fbref","fbref_id":"972aeb2a","season":"2023-2024","squad":"Arsenal","league_country":"ENG","comp_name":"All Competitions","position":"DF","age_season":23,"minutes":4100,"games_played":46,"games_starts":46,"minutes_per_90s":45.6,"standard_goals":2,"standard_assists":1,"standard_cards_yellow":5,"standard_cards_red":0,"standard_xg":2.1,"standard_xa":1.2,"def_tackles":55,"def_interceptions":35,"def_clearances":160,"poss_touches":3600,"passing_cmp":3200,"passing_att":3500,"passing_cmp_pct":91.4,"ingest_date":"2025-08-11","ingest_ts":"2025-08-11T17:40:00Z","source_url":"https://fbref.com/en/players/972aeb2a/all_comps/William-Saliba-Stats---All-Competitions"}
```

## Контроль качества
- Валидация NDJSON файлов против `schemas/fbref_*.schema.json`
- Идемпотентность по `(fbref_id, season, comp_name, squad, ingest_date)`

## Пути хранения
- RAW HTML (gzip): `/data/raw/fbref/player=972aeb2a/ingest_date=YYYY-MM-DD/*.html.gz`
- SILVER Parquet: `/data/silver/fbref/player_season_stats/season=YYYY-YYYY/comp=.../*.parquet`
