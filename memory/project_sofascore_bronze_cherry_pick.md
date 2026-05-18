---
name: sofascore-bronze-cherry-pick-2026-05-18
description: 5 новых SofaScore Bronze-таблиц (shotmap/event_player_stats/match_stats/player_season_stats/player_profile) реализованы cherry-pick'ом из ScraperFC поверх tls_requests
metadata:
  type: project
---

# SofaScore Bronze cherry-pick (#21–#25) — 2026-05-18

Tracker [#33](https://github.com/sergeykuznetsov1995/data-platform-football/issues/33) закрывает пять wishlist'ов одним PR на ветке `feature/issue-33-sofascore-bronze-cherry-pick`. Шесть атомарных коммитов:

| Commit | Subject | Closes |
|---|---|---|
| `386cec8` | refactor: extract `_fetch_json_endpoint` helper | — |
| `77fee97` | feat: shotmap | #22 |
| `4de685b` | feat: event_player_stats | #21 |
| `16e7b52` | feat: match_stats | #25 |
| `9964c59` | feat: player_season_stats (+ season_id resolver) | #24 |
| `336e3d2` | feat: player_profile | #23 |

**Why:** все 5 endpoint'ов уже доказаны рабочими (live-probe 2026-05-18 [[issue-19]], HTTP 200 без CF/Turnstile). Цель — раскрыть SofaScore как 5-й источник для Gold-фактов (T5 / T4). Silver/Gold подключение отложено до [[issue-12]] (xref_player resolver v2).

**How to apply:** при ingestion новых SofaScore endpoints — переиспользовать `_fetch_json_endpoint(url, label, context)` и шаблон `_run_event_endpoint` (для event-grain). Player-grain endpoints (player_profile/player_season_stats) требуют отдельных `_run_*` функций, поскольку контракт параметра — `player_ids`, не `match_ids`.

## Smoke результаты (live Trino, 2026-05-18 21:29 UTC)

```
['shotmap',             127 rows /  5 matches]
['match_stats',         632 rows /  5 matches]
['event_player_stats',  120 rows /  3 matches]
['player_profile',       10 rows / 10 players]
['player_season_stats',  10 rows / 10 players]
```

Все 5 entities — 0 fallback, 0 errors, exit 0. Прогон 5×smoke внутри `airflow-webserver` контейнера занял ~6 минут (доминирует event_player_stats: 3 матча × ~40 players × 3s/request).

## Архитектурные решения

- **`_fetch_json_endpoint`** — единая обвязка proxy_rotation + rate_limit + 3-attempt retry + 403/429/404 классификация для всех 5 endpoints. `_fetch_lineup_payload` (R0.2B) остался как тонкая обёртка для обратной совместимости.
- **DAG-топология**: schedule → ratings → {shotmap, match_stats, event_player_stats, player_season_stats, player_profile} → validate_data (`trigger_rule='all_done'`).
  - shotmap/match_stats запускаются параллельно с ratings (зависят только от schedule).
  - event_player_stats/player_season_stats/player_profile зависят от ratings (нужен player_id list).
- **Smoke daily limits в DAG**: shotmap=50, match_stats=50, event_player_stats=10, player_season_stats=50, player_profile=50. Полный season backfill — отдельная задача (`backlog`), запускается раз в N дней.
- **PK / partitioning**:
  - `sofascore_event_shotmap` PK = `(match_id, shot_id)` (с composite-id fallback при отсутствии `id`)
  - `sofascore_event_player_stats` PK = `(match_id, player_id)`
  - `sofascore_match_stats` PK = `(match_id, period, stat_group, stat_name)`
  - `sofascore_player_season_stats` PK = `(player_id, season)`
  - `sofascore_player_profile` PK = `(player_id)`
  - Все 5 partitioned by `(league, season)` с `replace_partitions=True`.
- **`SOFASCORE_TOURNAMENT_MAP`** — module-level dict для 5 целевых лиг (APL=17, LaLiga=8, Bundesliga=35, Serie A=23, Ligue 1=34). `_resolve_season_id` кеширует `(ut_id, season_id)` на инстансе scraper.
- **`_camel_to_snake` + `_coerce_scalar`** — module-level helpers для auto-flatten Opta-структур (`{value: ...}` структуры разворачиваются в скаляр; dicts без `value` → None).

## DQ-результаты

`validate_data` в `dag_ingest_sofascore` расширен счётчиками всех 5 entities + R0.2B_FALLBACK классификацией. Минимальные пороги (rows < N → warning, fallback → partial_success):
- shotmap: 50
- match_stats: 100
- event_player_stats: 50
- player_season_stats: 20
- player_profile: 20

Все smoke-runs прошли пороги (см. counts выше).

## Юнит-тесты

19/19 зелёные в `tests/unit/scrapers/test_sofascore_scraper.py`:
- `TestSofaScoreScraper` (2): init, rate_limit
- `TestShotmapFlatten` (3): happy path, composite-id fallback, garbage payload
- `TestCamelToSnake` (3): basic, consecutive capitals (XGOnTarget→xg_on_target), already-snake passthrough
- `TestEventPlayerStatsFlatten` (2): happy path с `{value: ...}` unwrap, garbage
- `TestMatchStatsFlatten` (2): nested ALL+1ST periods, garbage variants
- `TestPlayerSeasonStatsFlatten` (2): happy path, garbage
- `TestSofaScoreTournamentMap` (1): canonical league→ut_id map
- `TestPlayerProfileFlatten` (4): happy path, garbage, dob_fallback, country_fallback

## Followups / known limitations

1. **Full season backfill** (без `--limit`) для event_player_stats — 22h при 20 req/min. Открыть отдельную задачу/issue для ручного триггера раз в N дней.
2. **Silver/Gold подключение** — отложено до [[issue-12]] resolver v2. Bronze ingestируется независимо.
3. **`shot_id` composite-fallback** активируется когда SofaScore не возвращает `id` (редко, но возможно). PK всё ещё уникальный.
4. **SOFASCORE_TOURNAMENT_MAP** покрывает только Big 5. Дополнительные лиги — fallback через `_fetch_json_endpoint('/unique-tournament/{ut_id}/seasons')`, но ut_id для них нужно добавить в map вручную.

## Связанные memory

- [[scraperfc-sofascore-blocked]] — ADR research [#32](https://github.com/sergeykuznetsov1995/data-platform-football/issues/32): выбран path (B) Cherry-pick из-за GPLv3 + headless Chromium на каждый JSON-вызов в ScraperFC.
- [[sofascore-public-api-no-bypass]] — все 7 целевых endpoints отдают 200 через прямой tls_requests (probe 2026-05-18 [[issue-19]]); гипотеза «SofaScore анти-bot» неверна.
- [[replace-partitions-required]] — обязательное использование `replace_partitions=True` в обеих точках (scraper + runner).
