## RX — Cross-source player unification (T1–T5 + RX2) postmortem

**Этап завершён**: 2026-05-15 (T3-T5 shipped) → 2026-05-16 (T5-ext WS+US) → 2026-05-22 (RX2 xG-verdict, 5 источников в `xref_player`)
**Postmortem написан**: 2026-06-13 — **с опозданием ~4 недели** (Step 16 целил на ≥3d green-gate после 2026-05-18). За это время Gold переформатировал эпик #478 (2026-06-12) — см. § «Что изменилось».
**Источник цифр**: гибрид — нарратив/решения/грабли из green-window записей (memory `project_t3..t5_*`, `project_xg_rx2_*`); row-counts / orphan / audit p95-p99 / CTAS — **живая ре-верификация 2026-06-13** через Trino (read-only, стек был поднят параллельной сессией).
**Reference**: `docs/decisions/RX-implementation-plan.md` Step 16, `docs/research/RX_cross_source_player_profile.md`, `docs/research/R1_cross_source_thresholds.md`.
**Commits-якоря**: `583e645` (checkpoint t3-t5), `16161fe` (t6-resolver +FotMob +SofaScore), `d3c1039` (dim_player_attributes +SofaScore +FotMob shirt), `43b8233` (multi-season dedup Understat+FotMob).

---

## Что сделано

| # | Задача | Артефакт |
|---|---|---|
| T3 | FotMob → `silver.xref_player` (резолвер cascade + orphan-учёт + DQ enum + known-pair regression) | `dags/utils/xref_player_resolver.py` (`SOURCES`+`'fotmob'`, orphan-prefix `fm_`), `dags/utils/xref_dq.py` (`evaluate_orphan_rate_per_source`, warn 10% / err 25%) |
| T4 | `silver.fotmob_player_profile` + `gold.dim_player_attributes` (per-source атрибуты, JSON-экстракция) | `dags/sql/silver/fotmob_player_profile.sql`, `dags/sql/gold/dim_player_attributes.sql` |
| T5 | `gold.fct_player_season_stats` + `gold.fct_keeper_season_stats` + `_audit` (HARD_FACT COALESCE + cross-source diff) | `dags/sql/gold/fct_{player,keeper}_season_stats.sql.j2` + `_audit.sql` |
| T5-ext | +WhoScored +Understat season-aggregates в fct + audit (2026-05-16) | `silver.{whoscored,understat}_player_season_aggregate`, +13 WS / +8 US колонок |
| RX2 | xG source-verdict (Understat primary) + SofaScore в `xref_player` → 5 источников (2026-05-22) | `silver.sofascore_player_season_aggregate`, variadic `COALESCE(fb→fm→ws→us→ss)` |

---

## Materialized counts — green-window (recorded, май) vs live (2026-06-13) + drift

| Объект | Green-window (recorded) | Live 2026-06-13 | Drift |
|---|---:|---:|---|
| `silver.xref_player` (fotmob rows) | 551 | **590** | +39 |
| `silver.xref_player` (все источники) | — | **17 468** | — |
| `silver.fotmob_player_profile` | 552 | **591** | +39 |
| `gold.dim_player_attributes` | 1 244 | **1 909** | +665 (+53%) |
| `gold.fct_player_season_stats` | **2 551** | **4 942** | **×1.94** |
| `gold.fct_keeper_season_stats` | **204** | **401** | **×1.97** |
| Сезоны (APL) | 9 | **10** (`'1617'`–`'2526'`) | +1 |

**Почему facts почти удвоились при +1 сезоне.** `fct_player_season_stats` — по-прежнему **только APL** (10 league-seasons, все `ENG-Premier League`). Удвоение не от логики (#555 миграция на `source_priority.yaml` row-count не меняет — spine = FBref), а от **данных**: per-season строк стало ~494 (было ~283 в green-window smoke). Плановый таргет был ~487/сезон для APL 2526 — то есть green-window smoke шёл на **частично забэкфилленном** spine, а последующий backfill дозаполнил его до планового уровня, +1 новый сезон.

---

## Orphan rate (FotMob)

| Замер | FotMob orphan | Состав |
|---|---|---|
| Green-window (T3 ship) | **15.25%** (84/551) | 43/84 = U21-команды вне FBref APL-покрытия; остальное — резервные вратари / ушедшие игроки |
| Live 2026-06-13 | **20.34%** (120/590) | регрессия ↑ +5.1 pp — почти весь прирост строк (+39) ушёл в orphans (+36) |

**Verdict: WARNING, не ERROR** (порог warn 10% / err 25% — `xref_dq.evaluate_orphan_rate_per_source`). Уже на green-window FotMob превышал ≤10% target (принят как WARNING). Сейчас дрейфанул дальше.

**Контекст по всем источникам (live)** — FotMob теперь худший среди match-data:

| Источник | orphan % | | Источник | orphan % |
|---|---:|---|---|---:|
| fbref (spine) | 0.00 | | sofifa | 15.26 |
| whoscored | 0.29 | | transfermarkt | 14.16 |
| sofascore | 0.38 | | capology | 14.54 |
| understat | 0.86 | | **fotmob** | **20.34** |

Резолвер v2 (`token_set_ratio` subset-fix) подтянул understat/whoscored/sofascore до <1%, но **FotMob не выиграл** — кандидат на адресный фикс (см. Followups). Высокие % у sofifa/transfermarkt/capology — finance/attribute-источники с иным набором игроков (ожидаемо).

> ⚠️ Issue #10 просил «orphan rate за последние 3 daily runs» — но RX-DAG'и (`dag_transform_xref`, `dag_transform_fbref_gold`) имеют `schedule=None` (trigger-only из master pipeline, **не** daily cron). Поэтому фиксируем стабильное текущее значение, а не «3 прогона».

---

## HARD_FACT audit diff — FBref vs FotMob baseline (live 2026-06-13)

APL, **n=430** player-пар (INNER JOIN FBref⋈FotMob), **n=40** keeper-пар. `|diff|` = `ABS(fb − fm)`. Та же таблица добавлена в `R1_cross_source_thresholds.md` как новый baseline (раньше R1 имел только FBref-vs-WhoScored и прямо отмечал *«FBref vs FotMob напрямую не сравнивали»*).

| Grain | Метрика | p95 \|diff\| | p99 \|diff\| | Сравнимых пар | Within-threshold (по сравнимым) |
|---|---|---:|---:|---:|---|
| player | matches | 0 | 9 | 430/430 | 97.21% (≤1) |
| player | minutes | 15.7 | 466 | 430/430 | 97.67% (≤90) |
| player | goals | 0 | 2 | 267/430 | 98.88% (≤1) |
| player | assists | 0 | 1 | 265/430 | 100.0% (≤1) |
| player | yellow_cards | 1.5 | 2 | 339/430 | 94.99% (≤1) |
| player | red_cards | 0 | 0 | 38/430 | 100% (=0) |
| player | penalties_won | n/a | n/a | 0/430 | не отдаётся FotMob (колонка вся NULL) |
| keeper | matches | 0 | 0 | 40/40 | ~100% |
| keeper | minutes | 1 | 2 | 40/40 | ~100% |
| keeper | clean_sheets | 1 | 1 | 30/40 | 100% (≤1) |

**Вывод**: HARD_FACT-метрики совпадают плотно (95–100% within threshold по сравнимым парам) — подтверждает recorded green-window «99.3–100%». Счётные goals/assists — p95=0 (≥95% точное совпадение). Хвост minutes (p99=466) и matches (p99=9) — игроки с mid-season transfer / partial-season, где FotMob и FBref считают по-разному.

**Coverage-находка (важно для интерпретации)**: `minutes`/`matches` diff считаются для ~100% пар, а `goals`/`assists` — только для ~62% (267/430, 265/430), `red_cards` — лишь 38/430 (~9%). FotMob отдаёт счётные «нулевые» события **разреженно** (NULL вместо 0 у незабивавших / без карточек). Наивный within-threshold по `COUNT(*)` даёт обманчивые ~61% — реальная цифра считается по `COUNT(колонки)` (non-NULL). Кандидат на `COALESCE(fm.goals, 0)` в audit (Followups).

---

## Архитектурные решения (подтверждены кодом)

- **D1 — Two-grain split**: атрибуты (time-invariant: dob/height/foot/nationality) и season-stats — два независимых артефакта (`dim_player_attributes` ⟂ `fct_*_season_stats`). Смешивать в одну таблицу нельзя — у них разные ключи и refresh-циклы.
- **D2 — Silver per-source → Gold merged через `xref_player`**. Каждый источник чистится в свой `silver.*_player_*`, объединение только в Gold по `canonical_id` (+ обязательный `(league, season)` predicate — иначе 1.5-4× fan-out).
- **D3 — Wide single-column COALESCE + ленивые суффиксы**. Overlap HARD_FACT (21 метрика: goals/assists/minutes/cards…) — одна колонка через `COALESCE(приоритет источников)`; unique-метрики (15 UNIQUE_FBREF / 16 UNIQUE_FOTMOB) — суффиксированы. MODELED overlap = 0 (FBref в Silver не отдаёт xG).
- **D4 — FBref-spine для facts**. `fct_*_season_stats` строятся от FBref-игроков. Теряем ~43-120 FotMob U21/резерв-orphans, но получаем мульти-сезонный охват (FBref = 10 сезонов, FotMob Bronze = только APL 2025/26).
- **D5 — Audit в отдельной таблице, WARNING-only** (`<table>_audit`, `error_threshold=0.0`). Cross-source diff-колонки (`<metric>_diff_<source>`) — для калибровки/дебага, не business-fct. Зерно audit = INNER JOIN на оба источника.
- **D6 — FotMob resolver v1 достаточен** (R2-followup опционален). Ожидали orphan 4-8%, получили 15-20% (U21-driven) — приемлемо для WARNING.

---

## Edge cases в JSON extraction (`player_information_json`) — обработано вручную в T4

- **Trino не поддерживает JSONPath-фильтры `[?(...)]` и correlated UNNEST** — нельзя «достать элемент массива по title». Решение (`fotmob_player_profile.sql`): map-идиома
  ```sql
  element_at(
    map_from_entries(transform(
      CAST(json_parse(d.player_information_json) AS array<json>),
      e -> ROW(json_extract_scalar(e,'$.title'), json_extract_scalar(e,'$.value.fallback'))
    )),
    'Preferred foot'
  ) AS foot
  ```
- `foot` берётся из `player_information_json` (~97%); `height_cm` (93-94%), `date_of_birth` (100%), `nationality`/`country_code` (100%) — из `bronze.fotmob_team_squad`, **не** из JSON.
- **`take_on_pct` → `takeon_pct`** (T5-ext): WhoScored-колонка без underscore. Render-тест пропустил, runtime-CTAS поймал — урок: имена колонок сверять с живой schema, не «из головы».
- **`bronze.understat_players` dups ×10** (append-mode ingest накапливал копии) → `ROW_NUMBER() OVER (PARTITION BY player_id, league, season ORDER BY _ingested_at DESC)` дедуп в Silver-CTE.
- **`season_slug` varchar `'2526'` vs `season_year` bigint `2025`**: WS/US Silver хранят сезон строкой, FBref/FotMob — числом. Spine-CTE эмитит **оба** ключа, чтобы JOIN'ы матчились.

---

## Performance (Trino CTAS wall-clock)

План: <15s на таблицу. Green-window smoke: <1s (T5), резолвер T3 — 12.26s.

**Live full DAG-run `dag_transform_fbref_gold` (2026-06-13 16:07), task-group `s2d_season_blocks`:**

| Таск | Duration |
|---|---:|
| `fct_player_season_stats` | 1.65s |
| `fct_player_season_stats_audit` | 1.03s |
| `fct_keeper_season_stats` | 0.95s |
| `fct_keeper_season_stats_audit` | 0.91s |
| `fct_team_season_stats` | 2.65s |
| `fct_team_season_stats_audit` | 1.09s |

Все <3s даже при удвоившемся row-count (4942 строк) — **×5+ запас** под планом <15s. ✅

---

## Что изменилось с green-window (эпик #478 + #555/#556)

| Изменение | PR/issue | Влияние на RX |
|---|---|---|
| Удалён derived gold tier (feat_*, fct_match*, mart_*, predictions_input*, per-source `*_team_season`) | эпик #478 (2026-06-12) | RX-core **жив**: `fct_{player,keeper}_season_stats`, `dim_player_attributes`, `xref_player`, `fotmob_player_profile` остались |
| Per-source season rollups → inline-CTE внутри `fct_team_season_stats(+audit)` | #478 | Team-уровень; player/keeper facts не затронуты |
| Season facts мигрировали на `configs/medallion/source_priority.yaml` | #555 (2026-06-12) | Приоритет источников в COALESCE теперь декларативный; spine/row-count не изменились |
| Audit-таблицы остаются inline `.sql` (не YAML), защищены `TestAuditCteSync` | #556 (2026-06-13) | `fct_*_season_stats_audit.sql` — источник live p95/p99 этого постмортема |

Итог: **звезда RX (player/keeper season-stats + attributes + xref) пережила #478 без потерь**; срезан был только производный ML-этаж (будет пересобран позже поверх чистой звезды).

---

## Followups (не блокирует)

1. **FotMob orphan-регрессия 15.25%→20.34%** при том, что understat/ws/ss подтянулись до <1%. Резолвер v2 не помог FotMob → нужен адресный разбор (U21-фильтр? `token_set_ratio` subset для FotMob-имён?). → issue **#563**.
2. **Audit goals/assists ~38% NULL coverage** — FotMob отдаёт season goals/assists разреженно. Рассмотреть `COALESCE(fm.goals,0)` в `fct_player_season_stats_audit.sql`, чтобы within-threshold не искажался NULL'ами. → issue **#564**.
3. **Blocked sources** (известные, уже в трекере): SofaScore orphan ~100% без xref-rows — #12; ESPN `player_id=NULL` upstream — #13.
4. **`penalties_won_diff_fotmob` вся NULL** — FotMob не отдаёт season penalties_won; колонку в audit можно убрать или пометить как не-сравнимую (включено в #564).

---

## Verification (как получены live-цифры)

Стек был уже поднят (`docker ps` → trino healthy). Запросы — read-only через `airflow-scheduler` (переиспользует `silver_tasks._get_trino_connection`: HTTPS:8443, BasicAuth, `verify=False`), **без** триггера DAG'ов (swap был забит на 100% — триггер = OOM-риск). DAG `dag_transform_fbref_gold` отработал сам в 16:07 → таблицы свежие.

```sql
-- orphan: SELECT source, COUNT_IF(confidence='orphan')/COUNT(*) FROM iceberg.silver.xref_player GROUP BY source
-- audit:  SELECT approx_percentile(ABS(goals_diff_fotmob),0.95), ... FROM iceberg.gold.fct_player_season_stats_audit
```
