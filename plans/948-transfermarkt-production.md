# План: Transfermarkt → продакшн (issue #948)

Родитель: #936 (реестр и платный контур доведены, PR #940 слит → `7860fa3`).
Здесь — автономная работа, полное покрытие мужских соревнований, DQ, техдолг.

## Цель

По расписанию запускается DAG: сам определяет актуальные соревнования, забирает
новые данные, кладёт в Bronze, проходит DQ. Человек у пульта не нужен.

## Инварианты (нарушать нельзя)

* **Платный трафик — действие в проде.** Любой прогон, который реально ходит на
  transfermarkt.com, требует явного разрешения пользователя. Всё, что можно проверить
  на тёплом кэше, dry-run или read-only Trino, проверяется без денег.
* **Не убивать TM-процессы.** Лиз прокси висит до TTL (час) и блокирует всё
  (`--max-active-leases 1`).
* **`git add -A` запрещён.** В каноническом клоне лежат незакоммиченные файлы других
  сессий. Коммитить только свои файлы поимённо.
* **Fail-closed не ослаблять.** Снятие ручного approval-ритуала (Ф1) не должно снимать
  байтовые/запросные лимиты, ledger и гейты — оно должно перенести их из разового
  ритуала в стоячую политику.

## Фазы

### Ф1. Автономный запуск: approval-ритуал → стоячая политика
DAG сейчас fail-closed по построению: без одноразовых approval-пакетов (TTL 3 ч,
argv-биндинг бит-в-бит) таск падает — `exact paid/write approval bundle is required`.
Механизм спроектирован для надзорной раскатки; для расписания непригоден.

Файлы: `dags/dag_ingest_transfermarkt.py`, `dags/utils/transfermarkt_approval.py`,
`scripts/prepare_transfermarkt_scope_approvals.py`, тесты.

- [x] Реализация
- [x] Сборка зелёная (5259 passed, 0 failed; было 5175)
- [x] Проверка живьём (рехерсал plan-таска в контейнере: scheduled+гейт → standing-env'ы
      против живого реестра; manual/гейт-off → прежняя ошибка бандлов; негативные запуски
      child с битым sha — отказ до subprocess)
- [x] Код-ревью (+ дельта-ревью доработок)
- [x] Security-ревью (инвариант подтверждён: капы/ledger'ы/гейты не тронуты; standing
      ограничен scheduled-ранами)
- [x] Коммит `cc47e89`

### Ф1.5. Автономный запуск discovery-DAG (добавлена оркестратором 2026-07-14)
DoD issue #948 требует «реестр обновляется сам», но `dag_discover_transfermarkt_registry`
имеет собственный approval-ритуал (3 пакета: paid/bronze/promotion). Решение: тот же
`StandingPolicy` из Ф1 (он генерический); отдельный вопрос — promotion-пакет argv-привязан
к CAS `expected_registry_revision` из Param → для автономии revision читается из текущего
ops-state (с сохранением CAS-семантики публикации).

Файлы: `dags/dag_discover_transfermarkt_registry.py`, `dags/scripts/run_transfermarkt_discovery.py`,
`scripts/prepare_transfermarkt_registry_approval.py` (совместимость), тесты.

- [x] Реализация (StandingPolicy для discovery; CAS-ревизия из живого ops-state с
      plan-hash сверкой apply=False↔apply=True; replay-защита fetched_at vs promoted_at;
      staging-очистка только от silver-таргетов)
- [x] Сборка зелёная (5259 passed, 46 skipped; +59 тестов: 31 DAG + 24 discovery + 4 policy-файл)
- [x] Проверка живьём (рехерсал в scheduler-контейнере: standing-режим распознан,
      живой реестр rev=2, plan-hash сошёлся; негативы: битый sha / отключённый env-гейт → отказ)
- [x] Код-ревью (свежий агент; находки перепроверены и закрыты до коммита)
- [x] Security-ревью (CAS не ослаблен: живая ревизия + plan-hash + freshness-якорь
      fetched_at внутри manifest_hash; двухключевая активация общая с Ф1)
- [x] Коммит `46ed923`

### Ф2. Пропускная способность: окно карьерных фактов и бюджет цикла
`MAX_ROSTER_WINDOW = 100` при ростере до 2199 → ~22 цикла на одну лигу-сезон только
ради стоимостей и трансферов. Байтовый предел 15 MiB — на **родительский** цикл (до 8
scope'ов), при измеренной цене scope ≈ 20 MiB. Числа не сходятся.

Файлы: `dags/scripts/run_transfermarkt_scraper.py`,
`dags/scripts/run_transfermarkt_scope_cycle.py`, `dags/dag_ingest_transfermarkt.py` (Params),
тесты.

- [x] Реализация (канон в `scrapers/transfermarkt/models.py`: SCOPE 24/22 MiB, PARENT_DAILY
      84/80 MiB, 1610/800, окно 500, cron daily; фикс единиц резерв↔settle; per-entity
      provider-грант; pre-I/O admission-гейт дневных байтов; execution_timeout выведен
      из канона; metered без гранта — fail-closed)
- [x] Сборка зелёная (5351 passed, 46 skipped; 1 fail — чужой compose-тест вне периметра)
- [x] Проверка живьём (рехерсал планировщика: батч 8 scope'ов, argv-числа = канон,
      таймауты 3600/5400, DagBag import_errors NONE, schedule '0 4 * * *'; симуляция дня:
      допуск 0/18/37/58 MiB → ADMIT, 79 MiB → REFUSE до I/O; readiness на РЕАЛЬНЫХ
      ledger-строках Trino (2 parent-цикла, капы 15728640/14680064, траты 1.29 МБ и
      0.10 МБ) → PASS)
- [x] Код-ревью (4 ревьюера + адверсариальная верификация; 2 MAJOR перепроверены
      оркестратором лично и починены: readiness equality-пин, execution_timeout)
- [x] Security-ревью (fail-closed не ослаблен; 2 подсаженные в дерево регрессии —
      агрегатная проверка scope-set против per-parent капа и floor гранта 2 байта вместо
      64 KiB — обнаружены пином литералов и устранены; в HEAD их не было)
- [x] Коммит `dbcbe6c`

### Ф3. Политика покрытия слота
`coverage_complete` требует все 9745 scope'ов → Silver/Gold и cutover недостижимы, пока
не собран весь таргет. Развилка: гейтить строго либо промоутить с зафиксированным долгом
(`career_fetches_pending` уже внутри хеша манифеста).
**Дефолт, если не решено иначе:** промоутить слот при полном покрытии scope'ов, но
блокировать cutover, если суммарный карьерный долг превышает порог; порог — в конфиге.

Файлы: `dags/utils/transfermarkt_scope_planner.py`, `dags/utils/transfermarkt_native_v2.py`,
`dags/dag_ingest_transfermarkt.py` (`validate_scope_set`), тесты.

- [x] Решение зафиксировано в этом файле (журнал п. 4, 18, 21-23)
- [x] Реализация (кумулятивный слот поперёк снапшотов реестра; CAPTURE_REVISION='v2' вместо
      selection_hash; покрытие — репорт; 3 cutover-гейта: career-debt ≤ 0.10, пол легаси,
      монотонность; свежесть — гейт, не членство; MAX_SCOPE_SET_SIZE 512→16384 + предикат
      через CTE; transform: exact-equality → subset; квота планировщика 50/50 долг/покрытие)
- [x] Сборка зелёная (5414 passed, 46 skipped; 1 fail — чужой compose-тест вне периметра)
- [x] Проверка живьём (readiness против живого ops: таргет 9745 / 684 current, reader=legacy,
      career-debt 4198/4398 = 0.9545 → гейт FAIL с 1 unstated манифестом поимённо; пол легаси
      52 пары, покрыто 2 → FAIL со списком; слот переживает ротацию снапшота; КАЖДЫЙ SQL
      провалидирован `EXPLAIN (TYPE VALIDATE)` на живом Trino — включая перепроверку
      оркестратором лично)
- [x] Код-ревью (ТРИ круга: круг 1 — 5 BLOCKER; круг 2 — 6 BLOCKER, внесённых фиксами круга 1
      (невалидный SQL `information_schema` валил transform всегда; DQ-предикат со ссылкой на
      несуществующий CTE = тихий fail-open; debt-first морил голодом новые scope'ы — ошибка
      директивы оркестратора); круг 3 — всё закрыто + вскрыт снос раздаваемых ТОП-5.
      ФИНАЛЬНЫЙ независимый круг НЕ отработал (сессионный лимит) — приёмку провёл
      оркестратор лично: живая валидация SQL, вычитка всех ключевых фиксов)
- [x] Security-ревью (fail-closed восстановлен там, где фиксы его сломали: DQ-гейт больше не
      глотает ошибку в WARNING; бюджетная эвиденция Ф2 не ослаблена — вытеснённый parent-цикл
      всё равно аудируется; capture_revision запинен к канону; standing_policy_hash подключён
      к гейту; Silver — ни native, ни раздаваемый legacy — не сужается молча)
- [x] Коммит `777fc25`

### Ф4. DQ по Bronze всего таргета
Есть DQ на scope (участники, применимость, dual-write parity) и на реестре
(`unknown_active = 0`). Нет: покрытия таргета, полноты ростеров, отсутствия сезонов,
которых у лиги не существует, дублей в append-only таблицах.

Файлы: новый `dags/utils/transfermarkt_bronze_dq.py` + гейт в
`dags/dag_transform_transfermarkt_silver.py`, тесты.

- [x] Реализация
- [x] Сборка зелёная
- [x] Проверка живьём (full-зона 70 чеков по реальному Bronze, все счётчики сверены с
      независимым базисом; scope_set-зона 52 чека PASS; ERROR ровно на 5 реальных грязных
      группах — mv_history 3 + transfers 2, DELETE-план у владельца)
- [x] Код-ревью
- [x] Security-ревью (инъекций нет, обхода гейта нет; попутно починен pre-existing
      fail-open баг маски комментариев в audit_transfermarkt_consumers)
- [x] Коммит `13d6337`

### Ф5. Дубли в `transfermarkt_player_attribute_observations`
Таблица append-only: повторный прогон одного scope кладёт полную копию ростера.

Файлы: `dags/scripts/run_transfermarkt_scraper.py` (write-spec), тесты.
**Идёт строго после Ф2** — тот же файл.

- [x] Реализация (carry-forward observed_at; проекция контента выведена из
      `_MANIFEST_COMPATIBILITY` минус identity; NULL-нормализация с обеих сторон;
      fail-closed lookup до записи; recency-гейт: переносится только штамп, остающийся
      новейшим у игрока в scope)
- [x] Сборка зелёная (5389 passed, 46 skipped; 1 fail — чужой compose-тест вне периметра)
- [x] Проверка живьём (read-only Trino: 33510 строк против 8195 реальных наблюдений =
      ×4.1; scope 2DVB/2023 — 28590 строк / 10 сканов / 2859 ключей / 5715 контентов;
      append-only только у attribute_observations — у остальных выходов есть replace_keys.
      Платный прогон НЕ выполнялся: приёмка структурная + репро на настоящих Silver-SQL)
- [x] Код-ревью (3 ревьюера + скептики; 1 MAJOR подтверждена и починена: возврат из
      аренды A→B→A оставлял покинутый клуб первым в `transfermarkt_player_attributes_v2`,
      т.к. Silver ранжирует по observed_at РАНЬШЕ _bronze_ingested_at; 6 находок опровергнуты)
- [x] Security-ревью (fail-closed lookup: «холодная таблица» = единственное допустимое
      «ничего нет»; параметризация предиката; порядок относительно платного I/O не нарушен)
- [x] Коммит `8a70344`

### Ф6. Онбординг лиг вне ТОП-5 и календарные сезоны в legacy-контуре
`configs/medallion/competitions.yaml` не знает соревнований вне ТОП-5 → `xref` пуст →
в Gold синтетические id вместо canonical.
`dags/sql/silver/transfermarkt_market_value_history.sql` выводит сезон из даты по зашитой
split-year формуле → у календарной лиги появляются несуществующие сезоны и `canonical_id`
= NULL. Ветка v2 (`market_value_points_v2.sql`) иммунна — там нет league/season.
**Дефолт:** чинить SQL, а не гасить legacy-пару, — до cutover она остаётся источником
для витрин ТОП-5.

Файлы: `configs/medallion/competitions.yaml`,
`dags/sql/silver/transfermarkt_market_value_history.sql`, тесты.

- [x] Реализация (только SQL; competitions.yaml не расширялся — см. журнал)
- [x] Сборка зелёная (16 passed в SQL-тесте; charter-аудит без новых ERROR)
- [x] Проверка живьём (TM-2DVB: DISTINCT season = только календарные годы; ТОП-5 —
      EXCEPT-дифф нового SELECT против текущего Silver = 0 строк в обе стороны)
- [x] Код-ревью
- [x] Security-ревью (fail-safe к битому реестру; строгий консенсус-гард)
- [x] Коммит `db5cc4a`

## Осталось пользователю (не автоматизируется)

- [ ] Решение по дневному лимиту трафика прокси (сейчас 100 МБ/сут → полный таргет
      больше года; 400 МБ/сут → 4–7 месяцев, дальше упор в вежливость 12 req/min).
- [ ] Строки окружения для TM в `.env` + рестарт контейнеров (инфру агенты не трогают).
      После Ф1 к списку добавился `TM_STANDING_POLICY_ENABLED=true`, и его нужно ещё
      пробросить в env-блок `x-airflow-common` в compose.yaml (там явный whitelist,
      без этого гейт не долетит до контейнеров; compose.yaml не трогали — в дереве
      чужие незакоммиченные правки).
- [ ] Чистка 5 грязных групп в legacy Bronze (Ф4-гейт красный до неё; готовые DELETE —
      в отчёте оркестратора: mv_history player 926235 mv_date 2025-06-03 value_eur=0
      mv_raw='-' в 3 сезонах; transfers player 189432 2026-07-01 fee_text='?' в 2 сезонах).
- [ ] **`PROXY_FILTER_TRANSFERMARKT_DAGRUN_BUDGET_BYTES=88080384` в `.env` + рестарт
      proxy_filter.** Без этого внешний фильтр режет TM-дагран на 15 MiB (fail-closed: отказ
      лиза, не перерасход) → сбор встанет на 1-м же scope. Код `filter_proxy.py` НЕ правили
      намеренно: правка инвалидирует отгруженную SofaScore-канарейку (её runtime_fingerprint
      хеширует весь каталог `scripts/proxy_filter`), а compose всё равно перекрывает
      константу флагом.
- [ ] **Один прогон discovery + промоция реестра (~15 MiB).** Нужен, чтобы ES1/IT1/L1/FR1
      получили `canonical_competition_id` из нового bootstrap-сида. Без этого cutover-гейт
      «пол легаси» недостижим навсегда (v2 назовёт лиги `TM-ES1`, legacy ждёт `ESP-La Liga`).
      Проверка после: `SELECT competition_id, canonical_competition_id FROM
      iceberg.silver.transfermarkt_competitions_v2 WHERE competition_id IN
      ('ES1','IT1','L1','FR1')` → 4 непустых canonical.
- [ ] **Ре-кроул 2 осиротевших scope'ов** (`2DVB:2023`, `2DVB:2024`, ~1.33 МБ суммарно):
      их манифесты несут старый `capture_revision` (= selection_hash) и в новый слот не
      войдут. Ручной прогон с явными селекторами требует одноразовых approval-пакетов
      (стоячая политика — только scheduled + пустые селекторы).
- [ ] Запуск самого бэкфилла (платный трафик).
- [ ] Переподписание `expires_at` стоячей политики раз в ~6 мес (истечение валит
      scheduled-прогоны fail-closed; сейчас до 2027-01-14).

### Followup-баги (найдены попутно, вне периметра #948)

- [ ] **`name = '38.4 %'` у ES1** в `silver.transfermarkt_competitions_v2` — парсер discovery
      затащил в поле имени ячейку с процентом. На резолв и гейты не влияет (всё идёт по
      `competition_id`), но это видимое поле реестра. → issue.
- [ ] **Тест-хелпер для живой валидации SQL:** фейковые курсоры в юнит-тестах слепы к
      невалидному SQL (см. журнал 20a-а). Нужен общий способ гонять `EXPLAIN (TYPE VALIDATE)`
      против реального Trino в CI/интеграционных тестах. → issue.
- [ ] Telegram HTML-эскейп в `alerts.py`; недетерминизм дедупа при одинаковом `_ingested_at`;
      relation-level allowlist в аудите потребителей.

## Журнал решений

_(сюда оркестратор пишет каждую развилку, решённую дефолтом)_

2026-07-14, оркестратор (дизайн-фаза, 4 Plan-агента по отчётам 3 Explore-агентов):

1. **Ф1 — механизм:** стоячая политика (`StandingPolicy` + файл в git
   `dags/configs/transfermarkt/standing_approval_policy.json` + env-гейт
   `TM_STANDING_POLICY_ENABLED`), а НЕ self-minting одноразовых пакетов из DAG:
   у self-minting TTL-гонка (3 ч < 4ч15м таймаута таска), retry-фатальность журнала
   (reissue запрещён) и самоодобрение вместо аудита. Ручной ритуал сохраняется и
   приоритетен при непустых `approval_bundles`. Капы политики обязаны РАВНЯТЬСЯ зашитым
   константам. Дефолты файла: approved_by=sergeykuznetsov1995, expires_at=+6 мес
   (2027-01-14) — истечение валит прогон fail-closed, переподписание за пользователем.
2. **Ф1.5 добавлена** (см. фазу выше): discovery-DAG в Ф1 не включён — другой файловый
   набор и отдельное решение по CAS promotion-пакета.
3. **Ф2 — периметр шире заявленного в фазе:** канон бюджетов уезжает в
   `scrapers/transfermarkt/models.py` (единый источник, import-assert'ы), правятся также
   `scrapers/transfermarkt/{client,scraper}.py` (per-entity provider-грант; фикс единиц:
   резерв был в decoded-байтах против provider-лимита), `dags/utils/config.py` (cron
   weekly→daily `0 4 * * *`), `scripts/prepare_transfermarkt_scope_approvals.py` и bench
   (числа). Новые числа: SCOPE_HARD 24 MiB / SOFT 22; PARENT_DAILY 84/80 MiB (под лимит
   100 МБ/сут, −12%); requests 1610/scope, retry 800/scope; окно карьеры 100→500.
   Подъём лимита до 400 МБ/сут = правка одной константы (решение за пользователем).
4. **Ф3 — уточнение дефолта:** «промоутить при полном покрытии» внутренне противоречиво
   (7-дневная евикция покрытия + 7-дневная freshness readiness + exact-equality в
   transform делают полный таргет недостижимым в принципе). Принято: кумулятивный слот
   (батч ∪ все complete-манифесты той же идентичности, без евикции), покрытие таргета —
   репортинг; cutover-гейты: career-debt ratio Σpending/Σroster ≤ 0.10 (константа
   `CUTOVER_MAX_CAREER_DEBT_RATIO`, не Param/env — readiness обязан совпадать из CLI и
   DAG), пол легаси (все (league,season) legacy-веток обязаны быть в слоте при первом
   cutover), монотонность (сет ⊇ approved при advance). `MAX_SCOPE_SET_SIZE` 512→16384.
5. **Ф4 — форма:** свой SQL-билдер-модуль вместо CHECK-фабрики (нужны композитные
   анти-джойны и FOR VERSION AS OF); severity: целостность = ERROR, прогресс покрытия
   таргета = WARNING (никогда не гейтит Silver — сбор занимает месяцы); гейт-таск между
   пинами и authorize_silver_writes (падает до сгорания write-approval).
6. **Ф6 — объём:** только SQL-фикс (format-aware season через join к publish-реестру с
   fallback split_year); `competitions.yaml` НЕ расширяется — онбординг не-ТОП5 лиг без
   FBref-спайна даёт пустой/orphan xref и задевает смежные контуры (clubelo, xref_team,
   DQ-знаменатели); «синтетические id в Gold для не-ТОП5» — отдельный эпик вне #948.
7. **Ф5 — семантика:** carry-forward observed_at (контентно-идемпотентный append);
   replace-партиций отвергнут (стирает SCD-историю издания), truncate-к-дате отвергнут
   (continuation через полночь под daily-cron — норма).
8. **Git:** работа в ветке `feat/948-transfermarkt-production` от HEAD
   `feat/whoscored-production-v2` (TM-файлы == origin/master; checkout master невозможен
   из-за чужого грязного дерева). Этот план-файл редактируется, но не коммитится
   (untracked-файл пользователя).

Волна 1 (исполнение, 2026-07-14):

9. **Ф4 — уточнения severity по живым данным:** физический грейн intra-batch чека
   legacy players/coaches включает клуб (все 1733 dup-группы players — мультиклубные,
   перепроверено оркестратором); дубли разделены на «конфликтующий payload» (ERROR) и
   «идентичный повтор» (WARNING — Silver-дедуп гасит детерминированно);
   orphans coach_stints→profiles = WARNING (следствие crawl-политики: stints со страниц
   истории клуба за всю историю, profiles только для действующих); cross-batch по
   memberships скоупится на scoped-строки. Остаточные ERROR на живом Bronze — 5 реально
   грязных групп (mv_history 3: player 926235, артефакт «0/'-'» рядом с реальной точкой;
   transfers 2: player 189432, fee_text '?' vs '-') — чистка за пользователем,
   DELETE-план в отчёте.
10. **Ф4 — rollback-путь:** manual_single_scope при незелёном реестре деградирует до
    YAML-only phantom-чека (WARNING) вместо жёсткого отказа — путь отката не зависит от
    native-инфры; при зелёном реестре строгий ERROR-режим.
11. **Попутный фикс (Ф4):** pre-existing fail-open баг `_mask` в
    audit_transfermarkt_consumers.py (маска съедала переводы строк докстрингов →
    комментарии маскировались по смещённым строкам, реальные v2-relations выпадали из
    инвентаря). Починен; новых нарушений фикс не вскрыл.
12. **Ф1 — по итогам ревью:** standing-режим ограничен run_type='scheduled' с пустыми
    селекторами (ручные прогоны — только через прежний ритуал); авторизационный след —
    отдельный standing-authorization.json (failure-конверт его не затирает, resume
    идемпотентно дописывает); наивные timestamp-строки отвергаются (policy_hash не
    зависит от TZ хоста); манифестная провенанс policy_hash отложена до Ф3 (валидатор
    dq_evidence отвергает неизвестные ключи, scope_state.py — зона Ф3).
13. **Ф6 — по итогам ревью:** консенсус-гард ужесточён до COUNT(*)=count_if(single_year)
    (NULL ломает консенсус — MIN/MAX были NULL-слепы).
14. **Ф2 — по итогам ревью (перепроверено оркестратором):** (а) readiness-пин леджера
    ослаблен с equality до диапазона (капы строки = капы эпохи кроула, ≤ сегодняшнего
    потолка; траты ≤ капов своей строки; writer остаётся строгим): equality сделал бы
    readiness 14 уже персистнутых прод-строк (15728640/14680064, 2 parent-цикла) вечно
    красным и превратил бы «подъём лимита = правка пары констант» в самоуничтожение
    накопленной эвиденции; (б) execution_timeout mapped-таска выведен из канона —
    худшая сумма entity-таймаутов (18000 c) превышала литеральные 4ч15м (15300 c) →
    SIGKILL посреди платного I/O без записи attempt-guard.
15. **Ф2 — filter_proxy НЕ правится кодом:** правка `scripts/proxy_filter/filter_proxy.py`
    инвалидирует отгруженную SofaScore-канарейку (её runtime_fingerprint хеширует весь
    каталог), а compose всё равно перекрывает константу флагом. Эффективный фикс —
    `PROXY_FILTER_TRANSFERMARKT_DAGRUN_BUDGET_BYTES=88080384` в `.env` (чеклист
    пользователя). Без него внешний фильтр обрежет TM-дагран на 15 MiB (fail-closed:
    отказ лиза, не перерасход).
16. **Ф2 — принято как есть:** дневной байтовый бюджет ключуется на parent run_id, а не
    на календарную дату (второй ручной dagrun в тот же день получит свежие 84 MiB) —
    ловится внешним proxy_filter (100 МБ/сут на все источники); удалённый sibling-файл
    леджера — fail-open, покрыт max() с committed-манифестами.
17. **Ф5 — recency-гейт carry-forward (по итогам ревью):** переносить observed_at можно
    только если он остаётся новейшим у игрока в scope по всем клубам. Причина: Silver
    (`transfermarkt_player_attributes_v2.sql:37-44`) ранжирует клубы игрока по
    `observed_at DESC` РАНЬШЕ `_bronze_ingested_at DESC`, поэтому наивный перенос при
    возврате из аренды (A→B→A с неизменными атрибутами) навсегда оставлял бы первым
    покинутый клуб B. Дефект был латентен (0 перемежающихся club-span'ов в живом Bronze),
    но ежедневная каденция Ф2 делает его достижимым.
18. **Ф3 — вскрытый блокер cutover (bootstrap-маппинг ТОП-5):** гейт «пол легаси»
    структурно недостижим для ESP/ITA/GER/FRA: `canonical_competition_id` проставляется
    discovery через `resolve_competition()` → `BOOTSTRAP_COMPETITIONS`, а там из ТОП-5
    была только GB1 (живьём: canonical заполнен у 6 строк из 1562). V2 называл бы эти
    лиги `TM-ES1`… и с legacy-именами (`ESP-La Liga`) не сошёлся бы никогда. Добавлены
    4 bootstrap-записи (ES1/IT1/L1/FR1) с canonical, сверенными посимвольно с
    `competitions.yaml`; `season_format=SPLIT_YEAR` запинен тестом — иначе Ф6 тихо
    пересчитала бы сезоны ТОП-5. Данные это НЕ меняет: нужен один discovery-прогон
    пользователя (~15 MiB) для переопубликации реестра.
19. **Ф6 — известное ограничение (принято):** SQL не фильтрует реестр по промоутнутому
    снапшоту (silver-реестр хранит ВСЕ опубликованные снапшоты — живьём 2 × 781 строка).
    Причина: ни одна Silver-модель не читает `iceberg.ops` — фильтр нарушил бы контракт
    слоя. Защита — консенсус-гард: расхождение снапшотов по `season_format` для одного
    canonical-ключа деградирует лигу к split_year (fail-safe, старое поведение).
20a. **Ф3 — три круга ревью, ключевые уроки:**
    (а) **Юнит-тесты с фейковым курсором СЛЕПЫ к невалидному SQL** — fetchall матчит подстроку
    и отдаёт заготовку, запрос не парсится. Так прошёл `iceberg.silver.information_schema.tables`
    (4-точечное имя → Trino отвергает), который валил бы transform ВСЕГДА. Введено правило:
    каждый SQL, реально уходящий в Trino, проверяется `EXPLAIN (TYPE VALIDATE)` на живом
    кластере. Стоит завести общий тест-хелпер для этого (followup).
    (б) **Ошибка директивы оркестратора:** «гасить карьерный долг в первую очередь» морило
    голодом новые scope'ы (у current-изданий долг возникает структурно) → покрытие не росло →
    cutover недостижим. Заменено на чередующуюся квоту 50/50 (`CAREER_DEBT_BATCH_SHARE`).
    (в) **Смертельная мина, вскрытая в круге 3:** в scope_set-режиме РАЗДАВАЕМЫЙ legacy Silver
    перестраивался `CREATE OR REPLACE` под предикат слота — первый же плановый transform со
    слотом из 2 scope'ов снёс бы ТОП-5. `_scope_legacy_sql` удалён; legacy Silver строится из
    полного legacy Bronze; страж `_assert_no_served_model_shrinks` запрещает отдавать меньше,
    чем уже отдаётся.
    (г) DQ-гейт покрытия был тихим fail-open: предикат ссылался на CTE, которого нет в запросе
    чека → исключение → severity WARNING → гейт не срабатывал никогда.
20b. **Бюджет — связывающее ограничение (не код):** 84 MiB/сут ÷ 24 MiB/scope = 3 scope/день.
    Полный таргет 9745 ≈ 8.9 года; пол легаси (50 недостающих пар) ≈ 50 дней; один проход по
    684 current-изданиям ≈ 228 дней. Гейты это честно показывают, а не маскируют. Решение по
    дневному лимиту (100→400 МБ/сут) определяет скорость, а не возможность.
21. **Ф1.5 — автономия discovery с сохранением CAS:** живая ревизия читается из
    ops-state вместо Param, компенсации: (а) plan-hash сверка plan-таска (apply=False)
    против apply=True — публикуется ровно то, что планировалось; (б) replay-защита:
    fetched_at манифеста обязан быть новее promoted_at текущей ревизии (fetched_at
    включён в manifest_hash — якорь неподделываем); (в) staging-очистка сужена до
    silver-таргетов discovery; (г) env-гейт общий с Ф1 (`TM_STANDING_POLICY_ENABLED`),
    политика отдельная: `standing_registry_policy.json` (15 MiB/1024/96/1).
