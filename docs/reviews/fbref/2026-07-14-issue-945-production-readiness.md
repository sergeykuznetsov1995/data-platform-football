# FBref production readiness — issue #945

Дата ревью: 2026-07-14
База сравнения: `origin/master@7860fa3a316f`
Ветка кандидата: `fix/fbref-production-readiness-945`

## Вердикт

| Контур | Статус | Решение |
|---|---|---|
| Код и локальные проверки | **READY CANDIDATE** | Production-контур реализован; полный unit-suite зелёный, независимое ревью не оставило Critical/Important замечаний. |
| Развёрнутый источник | **NO-GO** | Кандидат ещё не развёрнут и не прошёл canary/две последовательные current-загрузки/replay на одном SHA. |
| Исторический backfill | **BLOCKED** | Не запускать до выполнения production acceptance ниже. |

Это разделяет два разных утверждения: код можно подготовить к выкладке, но источник нельзя объявить production-ready только по unit-тестам. Для GO нужны доказательства из реально развёрнутого Airflow-контура.

Дополнительный обязательный security gate: до любого live-запуска должны быть ротированы действующие proxy/control credentials, затронутые во время аудита. Значения credentials в отчёте не сохраняются.

## Проверяемый scope

- Все соревнования, которые FBref публикует в мужском разделе, без ручного allowlist лиг.
- Вся доступная история каждого найденного мужского соревнования.
- Обычные сезоны, aggregate competitions и соревнования, где выпуск ведёт прямо на один матч.
- Все таблицы на реально полученной странице в lossless generic Bronze, включая неизвестные будущие таблицы.
- Существующие typed Bronze datasets для schedule, season stats и match pages.
- Женские соревнования и неизвестный gender сохраняются в registry/scope как evidence, но не допускаются в crawl/Silver.
- Недоступные source endpoints не синтезируются и не обходятся: сохраняется явный availability status.

## Что было

Снимок production-состояния, снятый read-only во время ревью:

- Registry: 117 активных мужских соревнований, 36 женских `skipped`, 0 `unknown`; 105 current competitions. У comp `612` было два current edition с одинаковым display label.
- В frontier находилось около 11 804 non-match targets, но schedule, season stats, standings и matchlog не имели стандартного durable production-fetch пути.
- Последний исследованный production run сделал 128 запросов и получил 9 685 098 bytes для 27 успешных targets; 100 запросов приходились на browser bootstrap. Source-задачи были зелёными, но итоговый DAG становился красным из-за downstream xref.
- Raw/S3: 2 038 объектов, около 18.28 MiB. Generic Bronze содержал 992 source tables и 259 407 cells; одновременно оставались 15 stale staging tables.
- 14 raw observations для 13 targets оставались непроцессированными.
- Старый backfill обрабатывал 25 запросов примерно как два root targets на запуск; оценочный минимум — 823 ручных запуска.
- DQ показывал нулевой female leakage, хотя только по squad targets находилось не менее 319 очевидных женских targets.
- Typed Bronze исторически почти не имел source-native identity. Например: schedule — 18 176/18 176 строк без обоих source IDs; match player stats — 532 982/533 032; lineups — 731 375/731 451; несколько player/team datasets не имели IDs полностью. Поэтому немедленный строгий join только по новым IDs удалил бы почти всю легитимную мужскую историю.
- 566 относящихся к контуру тестов проходили, но не проверяли перечисленные production-blockers.

## Что стало

| Область | Было | Стало |
|---|---|---|
| Scope лиг | Частично ручной/неполный production path | Authoritative `/en/comps/` discovery, все найденные мужские competitions и вся их history. |
| Gender | Female targets могли попадать во frontier, DQ их не видел | Female и unknown сохраняются как evidence, немедленно quarantine/skip; claims и Silver fail-closed. |
| Исчезновение лиги | Одного плохого snapshot хватало для потери scope | Мужская competition исчезает только после двух последовательных успешных snapshots; первый miss остаётся crawlable. |
| Current season | Возможны несколько current rows | v8 migration детерминированно дедуплицирует и ставит unique current index; display/source aliases сохраняются. |
| Single-match cups | Исторические прямые match links не доходили до backfill | Direct editions регистрируются как `direct_match_only`; current и historical match targets получают разные refresh policies. |
| HTML discovery | Global chrome/comment links могли создавать платные targets | Header/nav/footer/sidebar/aside и их comments исключены до detach; reconciliation выполняется один раз на страницу. |
| Пустой HTML | Любой zero-table shell мог считаться single-match season | Нужны ожидаемые heading/canonical identity и history backlink; иначе semantic и typed promotion падают до замены Bronze. |
| Raw handoff | Crash мог оставить raw вне следующего run | Immutable raw сначала пишется в S3; recovery полностью дренирует непроцессированный raw до сети и проверяет no-progress. |
| Browser/proxy | До одного bootstrap на target | До 25 targets в одной warm session, максимум восемь последовательных waves; recovery/reuse не расходуют proxy. |
| Бюджет | Малый фактический throughput, нечёткий production profile | Только hard profiles: canary `100 requests / 50 MiB`, current/backfill `200 / 100 MiB`; shard `<=25`. Backfill выбирает не 25 сезонов, а вычисленную безопасную когорту: 7 для canary и 14 для production. |
| Lease failure | Airflow retry мог встретить собственный активный lease | Live fetch имеет `retries=0`; timeout/nonzero child синхронно abort/release leases и reservations до ошибки. |
| Generic Bronze | Source tables сохранялись, но контракт пропускал отдельные формы | `fbref_page_manifest`, `fbref_table_inventory`, `fbref_table_cells` сохраняют каждую material table, empty evidence и unknown schema. |
| Typed Bronze | Replace мог иметь DELETE/INSERT visibility gap | Non-empty replace выполняется одним Iceberg `MERGE`; empty replace — одним `DELETE`, один snapshot на replacement. |
| Silver scope | Legacy league/season позволяли false-zero female DQ | Versioned immutable `bronze.fbref_target_scope` привязан к exact `control_run_id` и content hash; новые строки допускаются по exact source IDs. Legacy fallback разрешён только при двух NULL IDs и совпадении одновременно мужской compatibility league и season; partial IDs fail closed. |
| Publication concurrency | Параллельные current/backfill/replay или master могли смешать поколения Bronze/Silver/Gold | v9 migration добавляет fenced singleton publication lock. Source-владелец удерживает его через Silver, master проверяет exact owner, продлевает lease и освобождает только после доказанного terminal verdict; неоднозначная/незавершённая публикация сохраняет lock fail-closed. |
| DQ | Проверки могли быть зелёными при неполном crawl | Registry, gender, freshness, raw backlog, scope, availability, traffic budget и publication topology стали hard gates. |
| Source verdict | FBref DAG зависел от xref | FBref source DAG заканчивается на FBref Silver. Xref → E3 → E4 → Gold остаются отдельной fail-closed цепочкой master DAG. |
| Alerts | Проверялся только `ALERT_ENV` | До live run обязательны `ALERT_ENV=prod`, непустые `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`. |
| Backfill | Платные волны могли стартовать при stale current | Freshness preflight идёт до recovery/fetch и повторяется перед publication. `dry_run=true` не создаёт run, не меняет state и не использует сеть. |

## Bronze coverage

Lossless слой является страховкой полноты: он сохраняет любую таблицу, встретившуюся на fetched page, даже если у неё ещё нет typed adapter. Typed-контракт продолжает публиковать существующие schedule/match datasets и доступные season datasets: player/team standard, shooting, playing time, misc и keeper tables. Passing, passing types, GCA, defense, possession и advanced keeper schemas сохранены для offline replay, но их live routes сейчас явно не fetch-ятся: аудит источника показал restricted/полностью пустые cells, поэтому платные запросы к ним не считаются production coverage до подтверждения восстановления данных у FBref.

Empty, restricted, not-applicable, duplicate, layout-only, unknown и error различаются явно. Для match page отсутствие когда-либо полученного dataset не равно доказанному empty dataset; scored match без ожидаемого events evidence является ошибкой.

## Production DQ gates

Run не может завершиться успешно, если выполняется хотя бы одно условие:

- registry пуст, нет eligible male scope или появился `gender=unknown`;
- crawlable target находится вне допустимого male registry/season scope;
- required current page просрочен: registry/schedule/final — 24 часа; season/stats/squad — 7 дней; player/matchlog — 30 дней;
- у текущего run остался unprocessed raw;
- глобальный unprocessed raw старше 24-часового processing SLA;
- parser/typed/stateful processing завершился ошибкой или availability противоречит странице;
- request/byte budget превышен либо остались незакрытые reservations/leases;
- publication scope не содержит eligible male rows;
- FBref Silver не завершился успешно.

Свежий raw параллельного run остаётся диагностическим global backlog и не роняет здоровый run; после 24 часов он становится глобальным hard-fail.

## Airflow topology

```text
current:
readiness -> init -> publication lock -> registry -> recover raw
        -> [fetch -> parse] x8 -> freshness -> validate
        -> immutable scope export -> FBref Silver
        -> source success; publication lock удерживается для master

backfill live:
readiness -> init -> freshness preflight -> publication lock
        -> select effective 7/14 seasons -> recover raw
        -> [fetch -> parse] x8 -> freshness -> validate
        -> immutable scope export -> FBref Silver -> release/final verdict

backfill dry-run:
choose mode -> exact next cohort plan    (0 requests, 0 state mutations)

replay:
readiness -> init -> publication lock -> offline parse x8 -> validate
        -> immutable scope export -> FBref Silver -> release/final verdict

master publication:
scheduled FBref sensor (параллельно остальным source DAGs)
        + source gate -> exact lock resolve/renew
        -> xref -> E3 -> E4 -> auxiliary Silver -> Gold -> report
        -> release/final verdict
```

Source DAG timeout — 18 часов. Master sensor ждёт 12 часов после своего восьмичасового schedule offset: оставшиеся 10 часов source SLA плюс 2 часа scheduler slack. Master `dagrun_timeout` вычисляется из 151-часового критического пути и 12 часов запаса, итого 163 часа; blocking publication tasks имеют `retries=0`.

## Проверки кандидата

- Финальный полный unit-suite после всех fail-closed правок: 5 250 passed, 46 skipped.
- Расширенный Airflow/control focused-suite: 678 passed, 1 skipped; независимый ревьюер дополнительно перепроверил 488 focused tests и 94 lock/cleanup/repair tests.
- Ruff и `git diff --check`: passed.
- Все девять migrations, включая v8 current-season uniqueness и v9 publication lock, применены с `ON_ERROR_STOP=1` в отдельной временной PostgreSQL 16 базе. На реальной БД проверены acquire, fenced conflict, renew, release и takeover после expiry; временная база удалена.
- 43 чтения FBref Bronze в 17 SQL files оборачиваются male publication scope; 13 non-Jinja SQL успешно разобраны в Trino dialect после rewrite.
- Регрессионные дыры из issue закрыты отдельными тестами: fetcher обязан использовать `traffic_delta`; реальный `threading.Timer` срабатывает на wedged solve; requeue завершает attempt как `cancelled`; конфликт immutable manifest не маскирует исходную parse-ошибку; падающий reservation gauge не создаёт отрицательный traffic delta.
- Live FBref/proxy run и исторический backfill намеренно не выполнялись.

Финальное независимое ревью: READY, оставшихся Critical/Important findings нет.

Известная неблокирующая уборка: 15 старых `fbref_table_cells__stg_*` остаются recovery artifacts предыдущих падений. Перед удалением нужно подтвердить, что ни один stage не относится к активному recovery; автоматическое destructive удаление в рамках этого ревью не выполнялось.

## Production acceptance и порядок запуска

1. Ротировать proxy/control credentials и подтвердить работоспособность production alert channel.
2. Развернуть один immutable candidate SHA вместе с control migrations v8 и v9; не смешивать SHA между проверками.
3. Запустить `dag_backfill_fbref` с конфигурацией `{"dry_run": true, "request_limit": 100, "byte_limit_mb": 50, "shard_size": 25}`. Сохранить exact cohort из 7 сезонов и доказательство `network_requests=0`, `state_mutations=0`.
4. Запустить current canary: `{"request_limit": 100, "byte_limit_mb": 50, "shard_size": 25}`. Проверить raw/S3, generic и typed Bronze, budgets, DQ, male scope и отсутствие female publication.
5. Получить две последовательные успешные current-загрузки на production profile `200/100/25` и том же SHA.
6. Запустить `dag_replay_fbref` для одного из успешных `source_control_run_id`; replay должен иметь budgets `0/0`, не содержать fetch path и дать тот же validation verdict на том же SHA.
7. Приложить run IDs, control summaries, S3/Bronze row counts, DQ и traffic comparison к issue #945.
8. Только после выполнения пунктов 1–7 изменить verdict на GO и разрешить bounded historical backfill. Каждый production backfill run остаётся ограничен `200 requests / 100 MiB`, effective cohort 14 сезонов, `25 targets/session`, `max_active_runs=1`.

## Итог

До выполнения production acceptance источник остаётся **NO-GO**, а backfill — **BLOCKED**. Причина не в отсутствии реализованного production path, а в отсутствии deployment/runtime evidence: canary, две current-загрузки и offline replay должны доказать поведение на реальном FBref, proxy, S3, Trino и Airflow.
