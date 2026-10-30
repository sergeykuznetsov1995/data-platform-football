# Understat production ingestion

Status: production acceptance (epic #1424), document checked against master
2026-10-30. Owner: data-platform.

## Production boundary

Understat is read directly through its AJAX endpoints by the native client in
`scrapers/understat/`; there is no soccerdata dependency. The source-owned
league registry (`scrapers/understat/catalog.py`) holds all six competitions:

| Canonical league | Source league | Source id |
|---|---|---:|
| ENG-Premier League | EPL | 1 |
| ESP-La Liga | La_liga | 4 |
| GER-Bundesliga | Bundesliga | 3 |
| ITA-Serie A | Serie_A | 2 |
| FRA-Ligue 1 | Ligue_1 | 5 |
| RUS-Premier League | RFPL | 6 |

`UNDERSTAT_LEAGUES` in `dags/utils/config.py` is only a filter: both DAGs call
`validate_understat_leagues`, which fails loudly if it disagrees with the
catalog. RFPL is a Bronze/DQ scope; Silver/Gold football facts stay top-five
because their identity spine is FBref-backed.

One exact `(league, season)` extraction produces seven normalized Bronze
tables:

1. `understat_schedule`
2. `understat_shots`
3. `understat_players`
4. `understat_team_match_stats`
5. `understat_player_match_stats`
6. `understat_player_team_season_stats`
7. `understat_team_season_breakdowns`

The final two come from `getTeamData`. A match response is parsed into both
shots and player-match rows. Two optional HTML tables are described in
[Match HTML enrichment](#match-html-enrichment-1439).

Column types are explicit in `scrapers/understat/contracts.py` (`date`
`timestamp(6)`, xG/forecast `double`, ids/counts `bigint`, flags `boolean`).
`forecast_*` became `double` on 2026-10-29 through
`python -m scrapers.understat.migrate_1440` (dry-run by default; apply and
rollback require `--publication-paused`; the previous table is kept as
`bronze.understat_schedule_1440_backup`). The manifest DDL runs once through
`run_understat_scraper.py --bootstrap-manifest`, not on every run. Manifest
`started_at`/`completed_at` are still `varchar`.

## Current data DAG

`dag_ingest_understat` is the sole current-data owner and runs daily at 09:00
UTC (12:00 MSK). `plan_current_scopes` discovers the window at runtime; one
mapped process ingests one exact scope; results travel through XCom
(`--output -`), not `/tmp` files.

Window (`catalog.py`): source seasons start in July. The rolling window holds
two seasons and never drops below `current - 1`, so a running season survives
July. The next season is probed only while the last match of the latest
schedule lies between 14 days ahead and 90 days ago; probe results are cached
for 1 day (published) or 7 days (not published). After three transport
failures of discovery the plan falls back to calendar scopes.

A season is **closed** when every schedule row is a result, the last match is
more than 2 days old, the scope is `quality_ready` and no quarantine is open.
Closed seasons leave the daily plan for good (#1431); every Monday one
`getLeagueData` request per closed scope (`closed_check.py`) compares hashes of
the parsed schedule/players/team-match frames and re-ingests only on change.
Closed checks are queued after all current scopes.

Season start needs no manual step (#1428). Scope states:

- `not_published`: an undiscovered season has an empty schedule; nothing in
  Bronze is replaced. A discovered current scope with an empty schedule is an
  error instead;
- `upstream_pending`: schedule exists but no match is played; only the
  schedule partition is replaced;
- `complete`: all seven entities pass DQ and are published. Team pages are
  requested only for teams that have played.

An empty closed season is always `contract_failure`.

### Daily increment from cache (#1436)

A match is re-downloaded only when it has no validated cache entry, its
`dates[]` fingerprint changed, the cached payload is incomplete, or it was
played within the last 2 days; on top of that a small rotation re-checks the
oldest cached matches of open seasons. Team pages are fetched only for teams
whose row or match hashes changed. Budget (`cache_state.py`): 500 HTTP
requests per day (`DAILY_LIMIT`), of which up to 72 for rotation and 60
reserved for HTML enrichment. The limit is soft: exceeding it logs a warning,
mandatory work is never dropped. The per-day ledger is
`daily-http-YYYY-MM-DD.json` in the cache under `flock`; validated cache
entries are confirmed only after publication.

### Empty-match quarantine (#1433)

In a current season an empty played match does not block the league: it is
`waiting_data` for 7 days and then `source_empty`, recorded in the manifest
`quality_json`, and keeps the season open (`quarantine_until`). The whole scope
fails only on mass emptiness: more than `min(5, max(3, ceil(5 % of played)))`
empty matches. One match with an invalid forecast is a warning; more are a
contract failure. Closed seasons only carry existing quarantine records.
DQ failures go to `iceberg.ops.understat_ingest_failures_v1`, not to the
manifest.

Exact-ID exceptions in `scrapers/understat/coverage.py` (verified 2026-07-27
to 07-29; `getMatchData` returns HTTP 200 with empty `shots` and `rosters`):

| Scope | Match IDs | Allowed missing entities |
|---|---|---|
| FRA-Ligue 1 / 1617 | 4238 | `understat_shots`, `understat_player_match_stats` |
| GER-Bundesliga / 2425 | 27930 | same |
| RUS-Premier League / 1920 | 11214, 11222, 11249, 11260 | same |
| RUS-Premier League / 2021 (source 2020) | 14229, 14235 | same |

These are the technical results described below. Any other missing game in a
closed season blocks publication.

## Historical DAG

`dag_backfill_understat` (#1427) is created paused and, once unpaused, runs on
demand at 12:00 UTC (`0 12 * * *`). Its plan is one manifest query (no site
request) selecting up to 12 closed scopes, oldest first, whose latest attempt
for the current contract is not `complete`; with no work the mapped runner is
skipped. The manifest is the durable cursor. Current data has priority 100 and
history 10 in the same one-slot scraper pool. Do not add Airflow catchup or a
hand-written season list.

## Downstream

`dag_master_pipeline` (14:00 UTC) still waits for the exact 09:00 Understat
run through `wait_for_scheduled_understat`; it never launches a crawl. The
shared master pipeline itself is paused at platform level; Silver/Gold are not
fed from this DAG while it stays paused.

## Publication and DQ fence

`iceberg.ops.understat_ingest_manifest_v1` is append-only. Every
publication-affecting attempt stores the exact source identity,
contract/parser version, status, row/key counts, content hashes and errors for
all seven entities. A complete attempt requires:

- exact league, season and source-season identity in every row;
- required parser columns and unique, non-null natural keys;
- schedule-to-fact game coverage and player/team reference coverage;
- valid xG/coordinate/forecast domains (three-way forecast total within the
  inclusive `1.0 +/- 0.02`);
- no previously populated entity disappearing;
- one shared `_batch_id` across all seven physical partitions;
- physical row counts matching the proposed manifest before `complete`.

The runner appends an `in_progress` marker before its first write. Consumers
use the latest manifest attempt and the exact complete `_batch_id`, so a hard
worker exit or newer failure hides the scope until a retry completes all seven
tables. The 90 % partition-size guard (`MIN_REPLACE_RATIO`) stays enabled;
`--force-replace` bypasses only that size check. The legacy cut-over guard
(`LegacyCutoverBlocked`) still refuses to supersede legacy rows without a
complete v2 batch.

A new unknown source field is a warning (`unknown_source_fields`, #1437), not a
stop; an unknown non-null value of a known field is still an error.

## Transport and cache

The native client bootstraps cookies, sends `X-Requested-With:
XMLHttpRequest`, paces evenly at 30 requests/minute and retries 408/425/429/5xx
up to 5 attempts with exponential backoff and jitter; `Retry-After` is honoured
up to 120 s. Every retry is logged (`Understat retry: reason=...`) and the
scope result carries `request_count`/`retry_count`.

The cache lives in `UNDERSTAT_CACHE_DIR` or
`~/soccerdata/data/UnderstatNative` on the `soccerdata_cache` volume, with
`confirmed/`, `validated/`, `schema_drift/` and `html_enrichment/`. The runner
keeps `--reparse` (bypass the payload cache for an operator run).

## Match HTML enrichment (#1439)

The optional `enrich_matches` task (after the JSON scopes, `all_done`,
retries 0, never reddens the DagRun; JSON failures surface through
`json_collection_failed`) parses the match page for two fields JSON lacks:

- `bronze.understat_match_enrichment`: `whoscored_game_id`;
- `bronze.understat_player_match_enrichment`: `yellow_card_minutes`,
  `red_card_minutes`.

The first run (2026-10-30) only recorded a baseline of 24 969 played matches;
only matches played after it are fetched, at most 60 HTML requests per day and
one per match per day, up to 3 failures before `unavailable`. Pages are cached
separately in `html_enrichment/`. The tables appear with the first new match.

## Snapshot maintenance (#1444)

The shared daily sweep (`HIGH_CHURN_BRONZE` in `dags/utils/maintenance_tasks.py`)
covers schedule, shots, players, team-match and player-match stats. The last
task of `dag_ingest_understat`, `maintain_snapshots`, covers the rest with 3-day
retention: expire + orphan removal for the two season tables and expire only
for the ops manifest. Any error fails the task.

## Delivery (#1432)

`deploy/understat/auto_deliver.sh` (host copy `/root/understat-auto-deliver.sh`,
cron hourly at :10) delivers only Understat files into the live tree: shared
modules must equal master except the pinned allowed lag
(`tests/unit/deploy/test_understat_shared_lag.py`). It refuses while an
Understat task or runner is alive or the next run is less than 30 minutes away,
accepts only after both DAGs re-parse without import errors, keeps one attempt
per commit, and supports `--check`, `--rollback` and the
`understat-auto-deliver.off` switch. Journal:
`/root/understat-deliveries/journal.log`.

## Data caveats

- **`date`** is stored as the source string without time-zone conversion.
  Since 20/21 it matches UTC (26/27: 318 of 322 played matches equal FotMob to
  the minute); history is mixed (15/16 about +4 h, EPL 17/18 +1 h in summer).
  Allow ±1 h when comparing.
- **Technical results** (the eight exception matches above) carry placeholder
  values: score 3:0/0:3, xG 0.0/0.0, forecast 0.33/0.33/0.33; 27930 has
  xG 2.0/2.0 and forecast 0/1/0. Matches awarded after being played keep the
  field score without a flag (#902); the award flag belongs in xref.

## Operator checks

```bash
/root/.venvs/dpf-test/bin/pytest -q tests/unit -k understat
```

The suite covers the native client, parsers, manifest, increment, closed
checks, season start, quarantine, HTML enrichment, migration, anomalies, DAGs,
delivery and the shared-module pins (`tests/unit/{scrapers,dags,deploy,sql,configs}`).
Daily acceptance: 6/6 scopes `success`, requests ≤ 500, current scopes done by
09:15 UTC; inspect the mapped result `scope_attempt`, the latest manifest
status and the shared physical `_batch_id` before downstream use.

## Review history

- 2026-06-04 — production audit (outside git).
- 2026-07-02 — traffic review (outside git).
- 2026-09-23 — independent review: registry R-01…R-41 in
  [#1424](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1424#issuecomment-5797642651),
  roadmap #1426–#1445.

## Неблокирующая диагностика источника (#1438)

`quality.py` добавляет предупреждения `status=warning`: одинаковые непустые
составы/наборы ударов разных матчей, несколько исходных `roster_entry_ids`,
повтор содержания удара при другом ID, голы ударов ≠ счёту и участие игрока
за команду только в одном доступном матче сезона. Последнее может означать
дебют или трансфер; неполный сезонный снимок не доказывает ошибку состава.
Кратность строк сохраняется. Автогол засчитывается противоположной стороне;
отсутствующие удары/счёт дают отдельное предупреждение. Данные и блокирующие
schema/scope/natural-key/completeness gates не меняются, дедупликации нет.

Межлиговое сравнение выполняет `criterion.source_diagnostic_summary` по
снимкам всех шести лиг. SELECT использует latest-attempt fence Understat
(`complete`, точный `_batch_id`) и опубликованный `fotmob_matches_current`.
Сверка FotMob показывает число сыгранных, разность, расхождения счетов с ID
матчей, число несопоставленных/неоднозначных строк и времена снимков.
Ключ — лига, сезон, дата UTC, домашняя и гостевая команды; используются точные
имена без учёта регистра и явно переданные проверенные `team_aliases`.
Непокрытые имена (в том числе РПЛ) остаются несопоставленными. Нет данных или
SELECT недоступен — `n/a`; это не подтверждение отсутствия аномалий.
Отсутствующие scope перечисляются отдельно; общая разность при неполном
покрытии шести лиг — `n/a`, по доступным лигам показаны отдельные числа.
Исторический SQL метрики «≤24 ч» остаётся прежним.

Реестр наблюдений (история, не утверждение о текущем состоянии сайта):

| Наблюдение | Дата доказательств / ограничения |
| --- | --- |
| 31948 PSG–Rennes был клоном 30539 Rostov–CSKA, счёт 0–0 вместо 2–2 | Ревью 23.09.2026, R-17/R-18, issue #1438. Предоставленный кэш **28.10.2026 уже различается**: 31948 — 32 игрока / 28 ударов, 30539 — 31 / 27; прежний клон на нём не воспроизводится. |
| Дубли состава/ударов 23028, 29482, 30804, 28374 | Ревью 23.09.2026, issue #1438. Кэш 28.10 подтверждает несколько roster IDs для 30804; остальные исторические ID этим replay не проверены. |
| 19 исторических матчей 2014/15: голы ударов меньше счёта | Ревью 23.09.2026, issue #1438; исходного списка ID и тел в этой копии нет, текущий статус не проверен. |

Кэш replay хранится в `tests/fixtures/understat/anomalies_1438/`, происхождение —
`PROVENANCE.txt` и `README.md`. Компактная выжимка реальных 168 сыгранных
матчей Ligue 1 и РПЛ 2026 проверяет внедрение содержимого 30539 в 31948:
оба сигнала клона с обеими идентичностями и участие 31 игрока РПЛ в Ligue 1.
Тот же сезон без подмены служит отрицательным контролем. Это согласованная
модель исторического клона, не архив его исходного тела.
30804 проверен на реальном теле с отрицательным контролем roster IDs;
сезон La Liga не предоставлен, сезонный replay 30804 остаётся неподтверждённым.

Утренняя сводка (внешний `/root/watchdog/morning_report.py`, функция
`understat_diagnostics`) запускает `source_diagnostic_summary` в общем
планировщике (pandas, код = бой Understat, адаптер — `trino.dbapi` с
`enable_dynamic_filtering=false`) и показывает одну короткую строку: число
сыгранных против FotMob с разностью по лигам и счётчики предупреждений
(⚠️ при разности, клоне или «голы ударов ≠ счёт»; ✅ иначе). Расчёт ≈ 90 с,
только SELECT; без HTTP к источникам, таблиц и влияния на бюджет сбора.
Имена команд РПЛ и часть остальных не сопоставлены с FotMob (на 30.10:
348 из 500), поэтому расхождения счетов видны только по сопоставленным.
