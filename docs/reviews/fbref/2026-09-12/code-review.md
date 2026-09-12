# FBref — архив ревью кода, 12.09.2026

Base: `origin/master@cff8d50c7d7859c009d00118816dbbc14d5fafa2`. Проверены FBref-изменения с 24.08, последние в этом диапазоне — 25–26 августа. Выводы основаны на чтении кода и offline-воспроизведениях; они не являются полным аудитом production rows.

Подготовлены **три исправления кода и одно исправление теста**. Доставка не выполнялась. Подготовка дальнейшей реализации остановлена пользователем; [архивный план](prior-plan-draft.md) предназначен для следующих ревью.

## 1. Immutable manifest identity — P0

В `scrapers/fbref/pipeline.py`, `_record_generic_table_results`, dataset key использовал source table ID/location без table instance identity. Сохранённый raw Atlas 2009–2010 содержит две DOM-таблицы `results2009-20103111_overall`, source ordinals 5/6, 5/6 строк. Обе попадали в `table:results2009-20103111_overall:dom`; immutable store правильно отвергал замену завершённого 5-row verdict на 6-row verdict.

Подготовленное исправление сохраняет legacy key первого экземпляра и добавляет детерминированный `table_instance_id` следующим. Реальный fixture с SHA-256 `7a79556910ea5f403efb7850d0754e60d18e270ddcb7109a5f2a32e0a6d21167` воспроизводит старый сбой. Повторный recovery идемпотентен. Восемь дополнительных вариантов покрывают DOM/comments, равные и разные таблицы и альтернативный control writer.

Residual: ранее успешно завершённые duplicates одинаковой размерности/availability могли разделять manifest без конфликта. Обычный recovery их не переигрывает. Для них нужен отдельный адресный inventory audit; глобальный reset parser version не требуется для двух остановившихся Atlas observations. Старые completed manifests должны сохранять lineage.

## 2. Неверная perspective/stat category — P1

`scrapers/fbref/parsers/finders.py` принимал hidden `stats_squads_shooting_against` для запроса `shooting` и `stats_squads_passing_types_for` для `passing`, когда точные ID не дали строк. `scrapers/fbref/typed_bronze.py`, `parse_season_stats_html`, затем маркировал результат `AVAILABLE` и requested stat type.

Воспроизведение: единственная hidden against-таблица Arsenal/Gls=99 превращалась в доступный `team_shooting`/Gls=99 без perspective. Это неверный смысл успешно разобранных данных. Исправление удаляет substring fallback и сохраняет точные поддержанные source/legacy IDs для DOM/comments. Сначала 2 новых regression tests падали; затем finder/typed/html-parser suite: **228 passed**. Tests: `tests/unit/scrapers/test_fbref_finders.py` и связанные parser suites.

Отдельный typed `against` dataset не добавлен. Наличие неверных production rows не установлено. Версия `fbref-typed-bronze-v4` и immutable completed manifests означают, что будущий корректный parse сам по себе не ремонтирует старые observations; нужны доказанный affected cohort и versioned replay/supersession.

## 3. Freshness arithmetic — P1

`scrapers/fbref/control/store.py` и consumer в `dags/utils/fbref_pipeline_tasks.py` использовали `max(stale - never_fetched, 0)`. Свежие never-fetched targets внутри SLA тоже вычитались, скрывая реально просроченные fetched копии.

Подготовленный SQL отдельно считает `aged_targets = stale AND previously_fetched` и `stale_never_fetched_targets = stale AND never_fetched`. Consumer проверяет `aged + stale_never = stale` и `stale_never <= never_fetched`; legacy payload получает консервативную, явно неточную оценку. Пять regression cases сначала падали; затем **239 focused tests passed**.

Это дефект промежуточного freshness verdict. Отдельный финальный pipeline gate существовал, поэтому публикация просроченных данных этим воспроизведением не доказана. Deployment и свежий runtime verdict не выполнены.

## 4. SIGTERM readiness — исправление теста

В `tests/unit/dags/test_run_fbref_live_waves_runner.py` дочерний процесс выдавал `READY` до входа в `try/finally`. Сигнал мог прийти раньше проверяемой защиты. `READY` перенесён внутрь protected region; задержка после readiness детерминированно показывает различие finalizers. Production signal handler не менялся.

## Неподготовленные изменения и требования

| Требование | Подтверждение | Значение |
|---|---|---|
| Абсолютный deadline | `dags/utils/fbref_pipeline_tasks.py` оценивает `pages × domain_interval + overhead`. Для 80×25 в 01:00 UTC guard дал PASS с projected_end 04:54 и deadline05:15, тогда как live task допускает 07:05 при current06:00. | Pacing — нижняя оценка, не верхняя граница fetch/parse/persist/recovery. Deadline не передаётся runner; требуется bounded admission и finalization. |
| Реальный current/history overlap | `dags/dag_backfill_fbref.py`, `dags/utils/fbref_current_dag_factory.py`, `control/store.py`: общий source lock на весь run, acquire без retries; `publish=false` не освобождает его раньше. Общий Airflow pool имеет slots=1. | Новая модель должна доказать concurrent writers, ownership/fencing, recovery и executor capacity. Простое удаление lock недостаточно. |
| Самостоятельный Bronze outcome | Current factory ждёт `trigger_silver_transform` (`wait_for_completion=True`) и освобождает lock после child. | Bronze completion/snapshot/release требуют отдельного контракта и проверки медленного/failed downstream. |
| Oversized discovery spine | `pipeline.py` делает `response_too_large` permanent независимо от page kind. Offline schedule case: `failures=[]`, `terminal_oversized_pages=1`, `permanent=true`, `requeue=false`. | Это реализованная политика, но season/schedule/index могут навсегда скрыть потомков. Нужны coverage gap и адресный recovery. |
| Typed coverage | `pipeline.py` ограничивает typed promotion schedule/season/season_stats/match. `page_document.py` разбирает таблицы прочих страниц. | Standings/squad/player/matchlog уже имеют raw/generic; отдельная domain typed promotion и внетабличные profile facts неполны. |
| Availability возврата routes | `scrapers/fbref/constants.py`, `discovery.py` исключают passing/passing_types/gca/defense/possession/keepersadv до admission без TTL/reprobe. | Текущая доступность источника не проверена заново; автоматическое обнаружение возврата данных не доказано. |
| Емкость current | `CURRENT_MAX_BATCHES=16`, shard≤25: до400 admitted targets/run. 4096 requests — аварийный circuit. | Нельзя считать 4096 обработанных страниц/день или обещать SLA из этих лимитов. Нужны фактические arrival rate и end-to-end timings. |

## Исправленные ранее и уточнённые выводы

Source-advertised seasonless current URL resolution, два exact observed redirects, target-local oversized outcomes, persistent settlement и cap16 уже есть в августовском коде. Они требуют проверки доставки и данных, а не повторной реализации. Generic parser работает и на standings/squad/player/matchlog; тезис «страницы вообще не парсятся» неверен.

Ограниченный downstream medallion allowlist не задаёт знаменатель source-discovered Bronze. `FBREF-<id>` сам по себе стабилен; source competition/season IDs уже сохраняются отдельно. Без доказанного изменения identity тезис о нестабильном fallback partition key не является отдельным bug finding.

`dags/sql/silver/fbref_team_season_profile.sql` отображает `1888-1889` и `1988-1989` как `8889`; оба сезона есть в comp9 registry. Display slug не годится как полная source identity. Silver-изменение находится в [downstream backlog](downstream-backlog.md); для Bronze остаётся проверка различимости полных source seasons. Фактическая production потеря строк не установлена.

## Проверка и пределы

Combined FBref/generated-evidence suite: **1770 passed, 1 skipped in 51.95s**, 60 тестовых файлов. Skip — real-Airflow import, Airflow отсутствовал в среде. Ruff, `git diff --check` и generated runtime evidence consistency прошли. [Лог](unit-verified.log) относится к подготовленному набору **до последующей audit utility** и не доказывает её корректность.

Сетевые source probes и delivery не выполнены. Этот результат не заменяет integration overlap в поддержанном Airflow, аудит всех production rows, targeted source recovery или production soak. Точная одно-строчная архивация исторического test fixture отдельно описана в [runtime audit](runtime-audit.md).
