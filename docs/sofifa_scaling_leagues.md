# SoFIFA: масштабирование на другие лиги

> Итог ревью парсинга SoFIFA (2026-07). Чек-лист добавления лиг и связанные
> механизмы: инкрементальный скип, пороги, кэш, guard'ы.

## Стоимость лиги

Одна лига ≈ 23 bootstrap-запроса (homepage + `leagues.json` + ~21 team-страница)
плюс ~546 страниц игроков за edition → **~2 часа серийно** (≈13 с/страницу:
FlareSolverr + ротация сессии каждые 4 запроса из-за краша Chromium-таба).
`execution_timeout` BashOperator'а поднят до 12h — хватает на Big-5.

Благодаря инкрементальному скипу по `version_id` полный прогон случается
**только в недели roster-апдейта** (раз в ~2–4 недели): если Bronze уже несёт
последний `version_id`, ран сводится к 2 запросам (homepage + leagues.json).
Принудительный полный прогон: `run_sofifa_scraper.py --force-full`.

## Чек-лист: добавить лигу Big-5 (ESP / GER / ITA / FRA)

1. Добавить лигу в `LEAGUES` (`dags/utils/config.py`) — маппинг для Big-5 уже
   есть в soccerdata `LEAGUE_DICT` (ключ `SoFIFA`).
2. Больше ничего: пороги `MIN_ROW_THRESHOLDS['sofifa_*']` и warning-пороги
   `validate_data` вычисляются от `len(LEAGUES)` автоматически.
3. Первый ран после добавления: партиция `fifa_edition` вырастет — replace-guard
   (`_MIN_REPLACE_RATIO=0.9`) на рост не срабатывает. **Удаление** лиги из
   конфига guard заблокирует (exit 3) — это ожидаемо; осознанный шринк
   пропускать через `--force-replace`.

## Чек-лист: лига вне Big-5 (RPL, Championship, …)

soccerdata знает только Big-5 для SoFIFA. Прочие лиги подключаются через
кастомный `league_dict.json` в конфиге soccerdata:

1. Узнать точное имя лиги у sofifa: `https://sofifa.com/api/league` →
   `"[<nationName>] <value>"` (например `"[Russia] Premier League"`).
2. Положить файл в `/home/airflow/soccerdata/config/league_dict.json`
   (каталог живёт на named volume `soccerdata_cache` — переживает redeploy):

   ```json
   {
     "RUS-Premier League": {
       "SoFIFA": "[Russia] Premier League"
     }
   }
   ```

   Формат ключа — платформенный код лиги (`XXX-League Name`), как в `LEAGUES`.
3. Добавить лигу в `LEAGUES` (`dags/utils/config.py`).
4. Проверить floor'ы: `MIN_ROW_THRESHOLDS` считает ~546 игроков/20 клубов на
   лигу — для лиг с 16/18 командами floor может оказаться завышенным,
   скорректировать множитель при необходимости.

## Кэш страниц

`/home/airflow/soccerdata` вынесен на named volume `soccerdata_cache`
(`compose.yaml`): retry после крэша на середине длинного прогона и redeploy
контейнера переиспользуют уже скачанные страницы. Файлы кэша версионированы
`version_id` в имени (`player_<pid>_<vid>.html`) — по содержимому не
протухают, но объём растёт (~1 ГБ на roster-апдейт при Big-5); периодическая
чистка файлов старых `version_id` — вручную или followup.

## Прокси

DAG работает **без прокси** (решение #616): FlareSolverr решает Turnstile сам,
residential-МБ = 0. Если CF начнёт резать проксилес-запросы — аварийный
fallback `PROXY_FILTER_URL=http://proxy_filter:8899` (ad-tech фильтр, #652).
С прокси цена ~13 МБ/страницу игрока: инкрементальный скип тогда критичен.
