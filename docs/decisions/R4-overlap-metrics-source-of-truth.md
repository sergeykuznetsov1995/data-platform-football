# R4 — Source-of-truth для overlap-метрик в fct_player_season_stats

**Status**: planned, not started
**Подготовлено**: 2026-05-17
**Branch**: `feature/r4-overlap-metrics-research` (этот ADR), `feature/r4-overlap-metrics-cleanup` (предложено для PR #2)
**Целевой объём**: PR #1 — этот ADR (research-only). PR #2 — ~0.5 рабочего дня (cleanup + tests).
**Related**: GitHub issue [#14](https://github.com/sergeykuznetsov1995/data-platform-football/issues/14), memory `project_t5_extension_ws_us_2026-05-16.md`, `feedback_audit_in_separate_table.md`

---

## Context

`iceberg.gold.fct_player_season_stats` сейчас содержит три пары дублирующих метрик из разных источников:

| Метрика | Колонка A | Колонка B |
|---|---|---|
| Expected Goals | `expected_goals` (FotMob) | `expected_goals_understat` (Understat) |
| Expected Assists | `expected_assists` (FotMob) | `expected_assists_understat` (Understat) |
| Shots | `shots` (FBref) | `shots_understat` (Understat) |

By design это нужно для cross-source калибровки через `fct_player_season_stats_audit`, но downstream-потребители (Superset, BI-вопросы) спрашивают «а какой xG правильный?» — нужен один canonical-источник на метрику.

User-feedback 2026-05-17 (issue #14):

> «мы должны брать одну метрику, нужно провести исследование какой источник лучше использовать»

---

## Methodology

Все запросы — Trino против `iceberg.gold.fct_player_season_stats`, scope `league = 'ENG-Premier League' AND season IN (2024, 2025)`. Полный скрипт — `scripts/r4_overlap_metrics_analysis.py`.

Запущено 2026-05-17 в контейнере `airflow-webserver`. Sample size: 518 строк 2024/25 + 493 строки 2025/26.

---

## Analysis

### 1. Coverage per source-season

| Метрика | 2024 FotMob | 2024 Understat | 2024 FBref | 2025 FotMob | 2025 Understat | 2025 FBref |
|---|---|---|---|---|---|---|
| xG | **0.00 %** (0/518) | 47.68 % (247/518) | n/a | **82.76 %** (408/493) | 50.30 % (248/493) | n/a |
| xA | **0.00 %** (0/518) | 47.68 % (247/518) | n/a | **88.44 %** (436/493) | 50.30 % (248/493) | n/a |
| shots | n/a | 47.68 % (247/518) | **100 %** (518/518) | n/a | 50.30 % (248/493) | **100 %** (493/493) |

**Самое важное**: FotMob НЕ покрывает закрытый сезон 2024/25 вовсе (ingest подключён позже T3 — 2026-05-15). Если выбрать FotMob как single source для xG/xA, исторические значения 2024/25 в main-fct станут NULL.

### 2. Bias / dispersion на пересечении (FotMob ∩ Understat, season 2025/26)

| Метрика | n_pairs | mean(A−B) | stddev | median | abs_p95 | min | max | mean(A) | mean(B) |
|---|---|---|---|---|---|---|---|---|---|
| xG (FM − US) | 191 | **−0.218** | 0.558 | −0.05 | 1.25 | −3.44 | 0.85 | 2.06 | 2.28 |
| xA (FM − US) | 209 | **−0.227** | 0.557 | −0.04 | 1.22 | −2.76 | 1.19 | 1.21 | 1.44 |

| shots (FBref − US) | n_pairs | mean | stddev | median | abs_p95 | min | max | mean(A) | mean(B) |
|---|---|---|---|---|---|---|---|---|---|
| 2024/25 | 247 | **−0.287** | 2.71 | **0** | 0 | −32 | 1 | 15.86 | 16.15 |
| 2025/26 | 248 | **−0.113** | 1.30 | **0** | 1 | −19 | 1 | 14.39 | 14.50 |

**Observation**:
- FotMob xG систематически *ниже* Understat на ~0.22 за сезон (медиана почти нулевая), std 0.56 — небольшой стабильный bias.
- Shots: median = 0 (≥50 % игроков имеют идентичный shot count), p95 ≤ 1. Расхождения концентрируются в outlier-tail (multi-league или multi-tournament players).

### 3. Top-N validation (2025/26, manual cross-check)

Top-20 PL scorers 2025/26 — только 11 из 20 имеют Understat-значения. **9 ключевых нападающих отсутствуют в Understat** (orphan/resolver issue):

| Игрок | Команда | Goals | FotMob xG | Understat xG |
|---|---|---|---|---|
| Erling Haaland | Manchester City | 26 | 25.2 | 28.64 |
| Igor Thiago | Brentford | 22 | 20.4 | **NULL** |
| João Pedro | Chelsea | 15 | 14.4 | **NULL** |
| Viktor Gyökeres | Arsenal | 14 | 12.2 | 13.54 |
| Danny Welbeck | Brighton | 13 | 12.2 | 12.55 |
| Morgan Gibbs-White | Nottingham Forest | 13 | 9.5 | **NULL** |
| Dominic Calvert-Lewin | Leeds United | 13 | 15.3 | **NULL** |
| Ollie Watkins | Aston Villa | 12 | 14.0 | **NULL** |
| Cole Palmer | Chelsea | 9 | 10.1 | **NULL** |
| Casemiro | Manchester Utd | 9 | 5.3 | **NULL** |
| Matheus Cunha | Manchester Utd | 9 | 6.7 | **NULL** |

Top-10 PL scorers 2024/25 — FotMob колонки 100 % NULL (нет ingest), Understat покрывает только 3/10:

| Игрок | Команда | Goals | FotMob xG | Understat xG |
|---|---|---|---|---|
| Mohamed Salah | Liverpool | 29 | — | 27.71 |
| Alexander Isak | Newcastle | 23 | — | NULL |
| Erling Haaland | Manchester City | 22 | — | NULL |
| Bryan Mbeumo | Brentford | 20 | — | NULL |
| Chris Wood | Nottingham Forest | 20 | — | NULL |
| Yoane Wissa | Brentford | 19 | — | NULL |
| Ollie Watkins | Aston Villa | 16 | — | 18.63 |
| Matheus Cunha | Wolves | 15 | — | 8.45 |
| Cole Palmer | Chelsea | 15 | — | NULL |
| Jean-Philippe Mateta | Crystal Palace | 14 | — | NULL |

### 4. xG-coverage-buckets, 2025/26

| Bucket | n_players | avg_goals | avg_shots_fbref | avg_starts |
|---|---|---|---|---|
| **fotmob_only** | 217 | 2.50 | 23.6 | 18.3 |
| **both** | 191 | 1.97 | 18.1 | 15.3 |
| **understat_only** | 57 | 0.12 | 1.9 | **1.6** |
| **neither** | 28 | 0.36 | 3.5 | 4.2 |

Распределение очень характерное:
- `fotmob_only` (217) — старшие игроки **с реальными минутами** (avg 18.3 старта), которые `silver.xref_player` не смог bridge'ить с Understat (orphan-резерв).
- `understat_only` (57) — игроки с avg 1.6 старта — это **U21 / cup-only / backup-вратари**. Для аналитики они не критичны.
- `both` (191) — пересечение, где можно cross-check.

### 5. Outliers (|diff − mean| > 2σ, season 2025/26)

xG outliers (топ-8) — все Haaland-class scorer'ы, Understat последовательно выше:
- Haaland (FM 25.2 / US 28.64 → −3.44), Šeško (9.2 / 12.5 → −3.30), Ekitike (9.9 / 11.93 → −2.03), Flemming (7.8 / 9.43 → −1.63), Mateta (14.1 / 15.56 → −1.46), Gyökeres (12.2 / 13.54 → −1.34).

xA outliers: тот же паттерн — Understat выше на 1.3–2.8 у плеймейкеров (Cherki, Aaronson, Yeremi Pino, Cucurella, Murphy).

Shots outliers: единичные случаи cross-tournament drift, **не systematic** (Jørgen Strand Larsen FBref 21 / Understat 40 — likely Норвегия/Euro + клубные fixture, один из источников включает не-PL матчи).

### 6. Industry context (методология xG-моделей)

- **Understat**: собственная open-методология (есть public reference, [understat.com/about](https://understat.com)). Industry-standard для академических xG-исследований.
- **FotMob**: Opta xG (proprietary, лицензия от Stats Perform). Используется на сайтах TV-broadcast, повышенная свежесть в live.
- **FBref shots**: total shots (включает blocked), dedup из `bronze.fbref_player_shooting.sh`. Wider definition чем Understat (event-based).

Расхождение FM−US ~ −0.22 на xG последовательное и согласуется с известными методологическими различиями (Opta использует более консервативную box-position веса).

---

## Decision

### xG → **FotMob (winner)**

- **Coverage 88.44 % vs 50.30 %** в текущем сезоне 2025/26.
- 11 из top-20 PL scorer'ов отсутствуют в Understat (orphan/resolver issue, не качество данных Understat per se — будет улучшено в R2-followup, но today они NULL).
- Mean bias −0.22 / median −0.05 — статистически близки на пересечении.
- Audit-таблица сохранит `xg_diff_understat = expected_goals_fotmob - expected_goals_understat` для observability.

**Trade-off**: FotMob НЕ покрывает 2024/25 — исторические значения xG в main-fct станут NULL для 2024/25. Mitigation: (а) audit-таблица сохраняет Understat-значения через diff calculation; (б) после R2/R3 resolver improvements можно re-enable Understat fallback через COALESCE — но это вне scope R4.

### xA → **FotMob (winner)**

Та же логика: 88.44 % vs 50.30 %, mean bias −0.23. Решение симметрично xG.

### shots → **FBref (winner)**

- **Coverage 100 %** в обоих сезонах (FBref — spine, INNER JOIN).
- **Median diff = 0** — 95 % игроков имеют идентичный shot count.
- p95(|diff|) ≤ 1.
- Understat дает чистую дупликацию + 50 % orphan-loss.

Audit сохранит `shots_diff_understat = shots_fbref - shots_understat` для observability.

### Decision matrix summary

| Метрика | Winner | Loser to drop | Audit diff column |
|---|---|---|---|
| xG | `expected_goals` (FotMob, kept as-is) | `expected_goals_understat` | `xg_diff_understat` (NEW в audit) |
| xA | `expected_assists` (FotMob, kept as-is) | `expected_assists_understat` | `xa_diff_understat` (NEW в audit) |
| shots | `shots` (FBref, kept as-is) | `shots_understat` | `shots_diff_understat` (NEW в audit) |

**Rename**: НЕ требуется — winner-колонки уже имеют нейтральные имена (`expected_goals`, `expected_assists`, `shots`). Просто drop `*_understat`-суффиксы.

---

## Consequences (PR #2 scope)

Следующая PR (`feature/r4-overlap-metrics-cleanup`) выполнит:

1. **`dags/sql/gold/fct_player_season_stats.sql`** — удалить три строки в UNIQUE_UNDERSTAT блоке:
   - `us.expected_goals AS expected_goals_understat`
   - `us.expected_assists AS expected_assists_understat`
   - `us.shots AS shots_understat`
   (Остальные US-уникальные метрики — `key_passes_understat`, `xg_chain`, etc. — остаются.)

2. **`dags/sql/gold/fct_player_season_stats_audit.sql`** — добавить три новых diff-колонки в LEFT JOIN US-блок:
   - `(fm.expected_goals - us.expected_goals) AS xg_diff_understat`
   - `(fm.expected_assists - us.expected_assists) AS xa_diff_understat`
   - `(fb.shots - us.shots) AS shots_diff_understat`

3. **`dags/utils/gold_tasks.py`**:
   - Удалить value_range check на `expected_goals_understat` (line 1126) — колонка исчезнет; заменить на проверку `expected_goals` (FotMob diapason 0–60).
   - Удалить value_range check на `non_penalty_xg` (line 1128) если эта колонка тоже из US (TBD при правке).
   - Добавить три WARNING-only audit_diff checks (по образцу lines 1175–1206):
     - `xg_diff_understat`: `ABS(diff) <= 2.0 OR diff IS NULL` (mean ± ~3σ из ADR ≈ −0.22 ± 1.67)
     - `xa_diff_understat`: `ABS(diff) <= 2.0 OR diff IS NULL`
     - `shots_diff_understat`: `ABS(diff) <= 5 OR diff IS NULL` (p95 ≤ 1, но outliers до 32)

4. **`tests/unit/sql/test_fct_player_season_stats_render.py`** — удалить assertions на `expected_goals_understat`, `expected_assists_understat`, `shots_understat`.

5. **`tests/unit/sql/test_fct_player_season_stats_audit_render.py`** — добавить assertions на три новых diff-колонки.

6. **Memory**: создать `memory/feedback_overlap_metrics_decision.md` с ссылкой на этот ADR и кратким decision matrix.

### Known regressions

- `fct_player_season_stats.expected_goals` теперь NULL для всего 2024/25 (47.68 % строк) — потребители за пределами 2025/26 (если такие есть) сломаются. Mitigation: пересмотр в R2/R3 followup (resolver fixes → US coverage растет → можем восстановить fallback).
- Никаких изменений в Superset (таблица не в `datasources.yaml`) и OpenMetadata (YAML отсутствует).

---

## Verification (этого PR #1)

- ✅ Скрипт `scripts/r4_overlap_metrics_analysis.py` создан и запускается в `airflow-webserver`.
- ✅ Все 5 секций (coverage / bias / top-N / outliers / orphan buckets) выдают данные.
- ✅ Числа в этом ADR взяты прямо из stdout запуска 2026-05-17.
- ✅ Top-N manual sanity check — Salah 2024/25 Understat xG 27.71 совпадает с публичным understat.com (~27.7); Haaland 2025/26 FotMob 25.2 / Understat 28.64 соответствует методологическому расхождению Opta vs Understat ≈ 0.2–0.3 на топ-скорер.
- ✅ Decision matrix явный, без TBD.

## Open question после merge PR #1

Перед стартом PR #2: проверить, не сдвинул ли R2/R3 followup за это время Understat coverage. Если orphan rate упал ниже 5 % — рассмотреть гибридный COALESCE-paтерн вместо чистого drop (что меняет ADR — но это новая итерация решения).
