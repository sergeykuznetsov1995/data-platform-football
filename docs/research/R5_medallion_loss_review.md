# R5 — Ревью потерь статистики bronze→silver→gold и приоритеты источников

**Дата:** 2026-07-03. **Метод:** статический анализ всех silver/gold SQL + live-реконсиляция
в Trino (полный набор запросов A/B/C — спайн-потери, attach-rate источников, NULL-покрытие
метрик, passthrough-равенства). Охват: все лиги/сезоны в хранилище (фактически АПЛ).

## TL;DR

Архитектура здорова: спайны целы (fct_player_match — 1 потерянная строка на 10 сезонов,
fct_team_match/fct_lineup/fct_transfer/fct_match_rating — потери 0), полевых потерь в silver
нет, приоритеты источников централизованы в `configs/medallion/source_priority.yaml`.
Найдено одно операционное повреждение (xref SofaScore в двух сезонах), несколько
подключаемых «застрявших» данных и три устаревших участка кода/доков.

## 1. Находки, ранжированные по важности

### HIGH-1. Дыра xref_player(sofascore) в сезонах 1718 и 2425 — операционная, не кодовая
В `silver.xref_player` для source='sofascore' резолвлено 48 игроков в 1718 и 182 в 2425
против ~500 в каждом другом сезоне, при полных данных в silver (602/685 уникальных игроков).
Следствие в gold (fct_player_match): покрытие `rating` 11.2% (1718) и 33.1% (2425) против
~97% в остальных сезонах; SS-блок (duels_won и др.) 9.6%/27.4% против ~85%. Потеряно
прикрепление ~12 244 (1718) + 10 351 (2425) строк SofaScore-статистики.
То же зеркалится в fct_player_season_stats (rating_sofascore 9.0%/30.5%).
**Причина:** резолвер xref отработал на неполных данных и не перезапускался после бэкфила.
**Действие:** перезапустить xref-трансформацию для сезонов 1718/2425, затем пересобрать
e3/e4/gold (порядок xref→e3→e4→gold). Код менять не нужно.

### HIGH-2. bronze.fbref_match_keeper_stats — 7 677 строк без единого читателя
Повматчевые вратарские метрики (SoTA, GA, saves, save%) пишутся продом
(`scrapers/fbref/data_readers.py:1107`), но: нет silver-трансформации, нет gold-потребителя,
таблица НЕ зарегистрирована в `docs/decisions/bronze-write-only-register.md`. При этом в
`fct_player_match` вратарских метрик нет вообще (ни из одного источника).
**Действие:** зарегистрировано в реестре (этот PR); отдельным issue — silver
`fbref_keeper_match_stats` + вратарский блок в gold.

### MEDIUM-1. fct_player_market_value отбрасывает orphan-игроков (в отличие от fct_transfer)
FotMob-ветка: из 70 655 уникальных точек (player, date) в gold доходит 37 475 (−47%);
на уровне игроков 1 191 → 726 (−39%). TM-ветка: 2 744 → 1 849 игроков (−33%).
Это молодёжь/игроки вне FBref-спайна. `fct_transfer` в той же ситуации сохраняет строки
с префиксными id ('tm_...'), `fct_player_market_value` — молча дропает.
**Действие (рекомендация):** унифицировать — сохранять orphan-строки с префиксными id,
как в fct_transfer (дизайн §5.6 «оставляем строки с orphan-ID, не выбрасываем»).

### MEDIUM-2. psxg в fct_shot = 100% NULL, SofaScore-ветка никогда не выигрывает
fct_shot: silver 116 448 (Understat) + 107 617 (SofaScore) → gold 96 408, все understat_v1,
sofascore_v1 = 0 строк. Причины (live): у SofaScore `xg` заполнен только с сезона 2223,
а Understat покрывает 1617+ полностью — фолбэк «матч без Understat» не срабатывает никогда.
Следствие: колонка `psxg` (питается от ss.xgot) пуста во всём факте, хотя в silver xgot
заполнен на 100% для 2425/2526 (и ~34% для 2223/2324). Смерджить per-shot нельзя —
у источников нет общего ключа удара (задокументировано в fct_shot.sql).
**Действие (рекомендация):** либо принять (psxg доступен аналитикам в
silver.sofascore_shots), либо отдельная таблица/матч-агрегат psxg. Не чинится COALESCE'ом.

### MEDIUM-3. xref_match(understat) 1617: 13 матчей без моста → 307 ударов потеряно
Единственная спайн-потеря fct_shot: 3.15% в 1617 (гейт 5% не сработал). Остальные сезоны 0%.
**Действие:** доразрешить 13 матчей 1617 в xref_match (вероятно, тот же перезапуск, что HIGH-1).

### MEDIUM-4. Застрявшие в silver данные без gold-потребителя
| Таблица | Строк | Что теряется |
|---|---|---|
| silver.fotmob_transfers | 800 | fee_eur, даты — независимый фид трансферов |
| silver.capology_contract_extensions | 813 | сроки/длительность контрактов |
| silver.fotmob_team_leaderboards | 5 465 | сезонные лидерборды |
| silver.sofifa_team_profile | 20 | командные FIFA-рейтинги, бюджеты |
| FotMob rating (в fotmob_player_match_aggregate) | ~94% строк с 1617 | рейтинг игрока за матч |

FotMob rating подключён этим PR (fallback в fct_player_match). Остальное — issues.

### MEDIUM-5. Умеренные дыры player-моста WhoScored
lost_player_bridge в fct_player_match: 561 (1617), 603 (1718), 530 (2021) — ~5% сезона;
в остальных сезонах ≤51. Вероятно, тот же класс проблемы, что HIGH-1 (перезапуск xref).

### LOW / гигиена
- Комментарий «WS-мост только season='2021'» в `fct_team_match.sql.j2` устарел:
  live-проверка — bronze.whoscored_schedule заполнен 1617–2526 (home_team_id 100%),
  ws-метрики (key_passes/takeon_att/ball_recoveries) в gold заполнены ~100% во всех сезонах.
  Исправлено этим PR.
- `silver.fbref_shot_events`: bronze-фид мёртв с 2026-02, таблиц физически нет; SQL-файл
  остаётся мёртвым кодом (не выполняется — гейт OPTIONAL_BRONZE_TABLES). Оставлен, помечен.
- bronze write-only: `whoscored_player_profile` (5 263) по методологии реестра НЕ write-only —
  его читает `dags/utils/xref_player_resolver.py`; `clubelo_team_history` (219 861) —
  решение «stop» уже есть (#604).
- fct_transfer: orphan-доля player_id 44.9% (13 856/30 881) против задокументированных ~18% —
  выросла после мультилиги TM (не-АПЛ игроки вне спайна). Не потеря (строки сохранены),
  но каноничность деградировала; выправится по мере мультилига-xref.
- Исторические сезоны (1011–1516): standings/shots/player-stats источников глубже 1617
  отрезаются dim_season/FBref-спайном — структурный потолок платформы, by design
  (см. решение «фикс DQ standings» 2026-07-03).

## 2. Что подтверждено здоровым (live-цифры)

| Проверка | Результат |
|---|---|
| fct_player_match спайн (B1) | потеря 1 строка / 10 сезонов, fan-out 0 |
| fct_player_season_stats спайн (B9) | потери 0, gold == bridged во всех сезонах |
| fct_team_match спайн (B3) | ровно 2 строки/матч, delta 0 |
| fct_lineup (B8) | 100% матчей покрыто; orphan ≤2.7% (гейт 3%) |
| fct_transfer passthrough (B12) | silver 30 881 == gold 30 881 |
| fct_match_rating passthrough (B11) | delta 0 во всех сезонах |
| fct_standings (B10) | все in-scope сезоны = SofaScore целиком |
| attach Understat/FotMob (B2) | потери ≤1% с 1617 |
| fct_shot Understat (B5) | 0% потерь кроме 1617 (3.15%, MEDIUM-3) |

## 3. Матрица приоритетов источников (решение ревью)

Механизм: порядок `sources:` в `configs/medallion/source_priority.yaml` = порядок COALESCE
(первый non-NULL побеждает). Расхождения значений мониторятся audit-таблицами `fct_*_audit`.

| Метрика | Приоритет | Статус |
|---|---|---|
| xG/xA/npxG (модельные) | **Understat > FotMob > SofaScore** | подтверждаем (RX2: покрытие 99.2% / 81.7% / 84.6%) |
| Счётные факты (голы, пасы, отборы…) | **FBref > SofaScore > WhoScored > Understat > FotMob** | подтверждаем (yaml) |
| Рейтинг игрока за матч | **SofaScore > FotMob** | ИЗМЕНЕНО этим PR: добавлен FotMob-fallback (был SofaScore-only; fallback закрывает дыры 1718/2425 с 11%/33% до ~95% и страхует будущие) |
| Shots (per-shot грануляция) | **Understat > SofaScore** (whole-match winner) | подтверждаем; + is_sot добавлен этим PR |
| Lineups | **FBref > SofaScore > FotMob > WhoScored > ESPN** | подтверждаем |
| Standings | **SofaScore > FotMob** (whole-season winner) | подтверждаем |
| Трансферы | **Transfermarkt only** | подтверждаем; FotMob-мердж — issue (риск дублей событий без общего ключа) |
| Market value | **оба side-by-side** (source в PK) | подтверждаем no-merge; но убрать drop орфанов (MEDIUM-1) |
| Вратарские (сезон) | **FBref > FotMob** (save_pct не мерджится) | подтверждаем |
| Повматчевые вратарские | — | источник есть (HIGH-2), потребителя нет — issue |

Правило на будущее: FBref — спайн и приоритет №1 для счётных фактов; модельные метрики —
у их «родного» источника (Understat для xG); FotMob — универсальный последний fallback;
whole-source-winner (shots/standings/lineups) вместо COALESCE там, где нет общего ключа строк.

## 4. Изменения этого PR

1. `fct_shot.sql`: колонка `is_sot` (SofaScore — прямой перенос; Understat —
   `result IN ('goal','saved')`, конвенция идентична SofaScore `shot_type IN ('goal','save')`).
2. `source_priority.yaml` + `fct_player_match.sql.j2`: rating = COALESCE(ss.rating, fm.rating).
3. `fct_team_match.sql.j2`: устаревшие комментарии про WS-мост приведены к факту.
4. `bronze-write-only-register.md`: + fbref_match_keeper_stats (вердикт (b) future).

## 5. Рекомендации (issues, не в этом PR)

1. **Перезапуск xref для 1718/2425** (+13 матчей understat 1617) и пересборка e3/e4/gold — HIGH-1/MEDIUM-3/MEDIUM-5.
2. Silver + gold для fbref_match_keeper_stats (повматчевые вратарские) — HIGH-2.
3. fct_player_market_value: сохранять orphan-строки с префиксными id — MEDIUM-1.
4. Gold-потребители: fotmob_transfers, capology_contract_extensions, sofifa_team_profile, fotmob_team_leaderboards — MEDIUM-4.
5. **Reconciliation-DQ**: WARNING-гейт «silver rows vs gold attach» по образцу запросов B1/B2
   этого ревью — сегодня потерю строк не ловит ни один гейт (audit-таблицы меряют только
   согласие значений, row floors — только грубый минимум). Дыра HIGH-1 жила незамеченной.
