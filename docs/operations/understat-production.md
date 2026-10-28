# Understat production ingestion

Status: active implementation, 2026-07-27. Owner: data-platform.

## Production boundary

Understat is read directly through its AJAX endpoints; the ingestion has no
soccerdata dependency. The source-owned league registry contains all six
competitions exposed by Understat:

| Canonical league | Source league | Source id |
|---|---|---:|
| ENG-Premier League | EPL | 1 |
| ESP-La Liga | La_liga | 4 |
| GER-Bundesliga | Bundesliga | 3 |
| ITA-Serie A | Serie_A | 2 |
| FRA-Ligue 1 | Ligue_1 | 5 |
| RUS-Premier League | RFPL | 6 |

RFPL is a Bronze/DQ source scope. Existing Silver/Gold football facts remain
explicitly top-five because their identity spine is FBref-backed.

One exact `(league, season)` extraction produces seven normalized Bronze
tables:

1. `understat_schedule`
2. `understat_shots`
3. `understat_players`
4. `understat_team_match_stats`
5. `understat_player_match_stats`
6. `understat_player_team_season_stats`
7. `understat_team_season_breakdowns`

The final two tables come from `getTeamData`: exact player/team season splits
and source-faithful long-form category breakdowns. A match response is fetched
once and parsed into both shots and player-match rows.

## Current data DAG

`dag_ingest_understat` is the sole current-data owner and runs daily at 09:00
UTC. Runtime source discovery plans one mapped process per exact scope for the
rolling previous/current source seasons. During the June/July boundary it also
probes the new calendar-year source id until Understat publishes it. For the
2026-07-27 production generation, those canonical seasons are `2526` and
`2627`; no code/config edit is required when `2627` begins publishing.

Expected future-season states are explicit:

- `not_published`: an undiscovered active calendar probe is empty and neither
  v2 manifest data nor legacy physical schedule rows exist; nothing in Bronze
  is replaced;
- `upstream_pending`: schedule exists but no match is complete; only the
  schedule partition is replaced;
- `complete`: all seven non-empty entities pass DQ and are published.

An empty closed season is always `contract_failure`, never expected absence.
An empty scope already advertised by `getStatData` is also a hard failure: a
transient 404 cannot create a newer marker that hides valid Bronze data.
The 14:00 master pipeline waits for the exact successful 09:00 DAG generation;
it does not launch a duplicate crawl.

## Historical DAG

`dag_backfill_understat` is created paused and scheduled daily at 12:00 UTC
(`0 12 * * *`, after the 09:00 daily DAG). The plan runs one manifest query
(no site request, no physical scope verification) and selects up to 12 closed
scopes, oldest first, whose latest attempt for the current contract is not
`complete`. With no work the mapped runner is skipped and the shared scraper
pool is not used. The manifest is the durable cursor, so scheduler restarts do
not reset progress.

Current data has priority 100 and history priority 10 in the same one-slot
scraper pool. Do not add Airflow date catchup or a hand-written season list:
history admission is manifest state.

## Publication and DQ fence

`iceberg.ops.understat_ingest_manifest_v1` is append-only. Every
publication-affecting attempt stores the exact source identity,
contract/parser version, status, row/key counts, content hashes and errors for
all seven entities. Failures before `in_progress` remain in the Airflow task
result/log and do not supersede legacy rows or the last complete batch. A
complete attempt requires:

- exact league, season and source-season identity in every row;
- required parser columns and unique, non-null natural keys;
- schedule-to-fact game coverage and player/team reference coverage;
- valid xG/coordinate/forecast domains;
- no previously populated entity disappearing;
- one shared `_batch_id` across all seven physical partitions;
- physical row counts matching the proposed manifest before `complete` is
  appended.

A complete three-way forecast total is valid within the inclusive
`1.0 +/- 0.02` interval. In particular, the source values
`0.34 + 0.26 + 0.38 = 0.98` remain valid when nullable `Float64` subtraction
represents the boundary infinitesimally above `0.02`; totals outside the
inclusive interval remain contract failures.

Because Iceberg replacements are table-by-table, the runner appends an
`in_progress` marker before its first write. Consumers use the latest manifest
attempt and exact complete `_batch_id`; a hard worker exit or newer failure
therefore hides the scope until a retry completes all seven tables. The 90%
partition-size guard remains enabled; the manual `--force-replace` flag
bypasses only that size check, not DQ or the physical batch fence.

The history runner rechecks the latest manifest and its seven physical
partitions after reacquiring the shared pool. If a higher-priority current task
completed the scope after planning, history returns that existing attempt and
does not append `in_progress`. The terminal watcher runs after cooldown and
propagates mapped runner/validator failures to the DagRun status.

Eight match-level source exceptions were verified live. The FRA and GER
payloads were verified on 2026-07-27; the four RFPL 1920 payloads were verified
on 2026-07-28; and the two RFPL 2021 payloads were verified on 2026-07-29.
Every `getMatchData` call returns HTTP 200 but empty inner `shots` and
`rosters`:

| Scope | Match IDs | Allowed missing entities | Verification date |
|---|---|---|---|
| FRA-Ligue 1 / 1617 | 4238 (SC Bastia–Lyon) | `understat_shots`, `understat_player_match_stats` | 2026-07-27 |
| GER-Bundesliga / 2425 | 27930 (Holstein Kiel–Bochum) | `understat_shots`, `understat_player_match_stats` | 2026-07-27 |
| RUS-Premier League / 1920 | 11214, 11222, 11249, 11260 | `understat_shots`, `understat_player_match_stats` | 2026-07-28 |
| RUS-Premier League / 2021 (RFPL source season 2020) | 14229, 14235 | `understat_shots`, `understat_player_match_stats` | 2026-07-29 |

They are exact-ID exceptions in `scrapers/understat/coverage.py`. No league- or
season-wide percentage waiver exists; any new missing game blocks publication.

## Transport and cache

The native client performs cookie bootstrap and sends
`X-Requested-With: XMLHttpRequest`. It is paced evenly at 30 requests/minute,
honours `Retry-After`, retries retryable responses with exponential backoff and
jitter, and stores atomic JSON cache entries in the persistent scraper cache
volume. Current runs refresh league/team scope payloads. Every non-complete
history run also refreshes from the source, so a partial cached HTTP-200 payload
cannot trap the self-draining DAG on one scope. The standalone runner retains
an explicit `--reparse` override for operator-driven runs.

## Operator checks

Before unpausing history or accepting a deployment:

```bash
python -m pytest -q tests/unit/scrapers/test_understat_native.py \
  tests/unit/scrapers/test_understat_manifest.py \
  tests/unit/scrapers/test_run_understat_scraper.py \
  tests/unit/dags/test_dag_ingest_understat.py \
  tests/unit/dags/test_dag_backfill_understat.py
```

Inspect the mapped runner result for `scope_attempt`, then verify the latest
manifest status and shared physical `_batch_id` before downstream use. History
is finished only when `plan_history_scope` returns an empty list and all
source-discovered closed scopes pass `is_scope_complete(..., verify_physical=True)`.

Do not unpause history from a source checkout that has not been deployed into
the scheduler image/mount. Do not run both the old multi-scope runner and the
new mapped DAG; the legacy CLI has been removed.

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
`PROVENANCE.txt`; тела сохранены без изменений. Schedule в тесте синтетический,
поскольку предоставлены только match payloads. Обнаружение клона отдельно
проверяется синтетической регрессией, не выдаётся за исторический replay.
Критерий приёмки 31948 на кэше остаётся неподтверждённым: нужен архив клона
23.09 с сезонным контекстом и отрицательным контролем либо согласованное
изменение критерия. Singleton participation на одном матче его не подтверждает.

Вставка для лида во внешнюю утреннюю сводку после merge (файл cron здесь не
изменяется; `query_frame(sql)` — существующий read-only адаптер Trino,
возвращающий pandas DataFrame с именами колонок):

```python
from scrapers.understat.criterion import source_diagnostic_summary
lines.append(source_diagnostic_summary(query_frame, season_slug))
```

До подключения нужно сверить адаптер, покрытие имён команд и права SELECT.
Чтение Bronze не делает HTTP к источникам, не создаёт таблиц и не меняет бюджет
сбора. Реальная строка и runtime fingerprints проверяются лидом при доставке.
