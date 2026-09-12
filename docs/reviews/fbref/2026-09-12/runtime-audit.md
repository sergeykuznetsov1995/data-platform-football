# FBref — архив runtime audit, 24.08–12.09.2026

Срез 12.09.2026 примерно 10:11–10:14 UTC. Источники: read-only Airflow/control metadata, deployed source files, существующие task logs, сохранённый raw и Iceberg snapshot metadata. Окно начинается с **фактических стартов DAG 24.08 00:00 UTC**, а не Airflow logical date. Позднее в 10:29:25 UTC отдельно выполнена проверенная архивация одной тестовой строки.

**Production NO-GO на момент среза.** Все 19 завершённых плановых current runs упали. Единственный green ingest — manual nonpublishing canary 25 августа. История остановилась 06.09 в 01:21:47 UTC. Bronze продолжает получать записи: page manifest snapshot — 12.09 10:09:42 UTC, schedule snapshot — 10:10:51 UTC. Эти timestamps не доказывают полноту или принятый current outcome.

Scope уточнён до Bronze: Silver/Gold freshness не используется как GO gate. Семидневный soak согласован, **не начат**. Подготовка реализации остановлена пользователем; следующие действия — ревью материалов. [Согласованные границы](agreed-scope.md) имеют приоритет над ранними допущениями исходного аудита.

## Запуски и причины отказов

| DAG | Success | Failed | Running при срезе |
|---|---:|---:|---:|
| `dag_ingest_fbref` | 1 manual canary | 21: 19 scheduled + 2 manual | 1, с 12.09 06:00 UTC |
| `dag_bootstrap_fbref` | 3 | 1 | 0 |
| `dag_backfill_fbref` | 40 | 15 | 0 |

FBref acceptance/replay DAG не запускался в окне. Последний green scheduled ingest в проверенной metadatabase стартовал 10 июля; более поздние green ingest были manual canaries. У 18/21 failed ingest и 10/15 failed history были сетевые запросы. Failed run мог сохранить часть Raw/Bronze, но это не доказывает успешную обработку всей работы.

Причина ниже — первая failed task каждого DAG; финализатор, сохраняющий красный итог, отдельно не посчитан.

| Причина | Current | History | Bootstrap |
|---|---:|---:|---:|
| Freshness gate после live работы | 8 | 0 | 0 |
| Publication-lock collision с историей | 2 | 0 | 0 |
| Subprocess timeout 6 часов | 3 | 6 | 0 |
| Внешний SIGTERM/SystemExit(143) | 1 | 0 | 0 |
| Конечные HTTP status failures | 3 | 0 | 0 |
| Response-size ceiling | 1 | 0 | 1 |
| Hard transport policy/metering | 2 | 3 | 0 |
| Исчерпание clearance refreshes | 1 | 2 | 0 |
| Duplicate table manifest key | 0 | 4 | 0 |
| **Итого** | **21** | **15** | **1** |

Внешний инициатор SIGTERM из task log не установлен. Hard transport/metering failures не равны исключительно source HTTP403. Некоторые продуктивные failed history runs остановились из-за timeout, transport или parser/control conflict; прежнее объяснение «сбор работает, красный только gate» не описывает эти случаи.

## Доказанный stopper истории

Два target:

- `fbref:squad:7c76bc53:a667b6f50d092c9f077c`: [Atlas 2009–2010 Liga MX](https://fbref.com/en/squads/7c76bc53/2009-2010/c31/Atlas-Stats-Liga-MX).
- `fbref:squad:7c76bc53:ca453ee5e74e027602a9`: [Atlas 2009–2010](https://fbref.com/en/squads/7c76bc53/2009-2010/Atlas-Stats).

Первый raw проверен по SHA-256 `7a79556910ea5f403efb7850d0754e60d18e270ddcb7109a5f2a32e0a6d21167`. Offline parse возвращает два `results2009-20103111_overall` в DOM, ordinals5/6, row counts5/6, разные instance IDs. Manifest key использовал только table ID/location. Попытка записать вторую таблицу конфликтовала с immutable completed verdict первой.

Исходная live wave `history_20260905T195254Z` упала 05.09 в 23:26:45 UTC. Затем `history_20260905T233916Z`, `history_20260906T001332Z`, `history_20260906T005511Z` повторяли сбой в `recover_raw_before_fetch`: по три task attempts, **ноль запросов**. Driver остановился с exit3 в 01:21:47 UTC. Restart без исправления повторяет то же наблюдение.

Кодовое исправление и replay regression подготовлены [отдельно](code-review.md). Production replay двух observations не выполнен. Ранее успешно завершённые equal-count duplicates требуют отдельной проверки полноты manifest inventory.

## Freshness и правильные популяции

`get_run_summary` около 10:14 UTC дал provenance/eligibility-backed current male scope. Он собран несколькими SELECT при работающем ingest; это **не один immutable snapshot**. Все числа этого раздела — до test-fixture archival.

| Популяция | Targets | Never fetched | Outside SLA |
|---|---:|---:|---:|
| Current male eligibility | 226 012 | 210 622 | 75 197 |
| Более узкий publication gate | 12 069 | 82 | 501 |

Publication stale: season_stats463, schedule34, season3, competition_index1. Более широкий scope добавляет stale player41 495, squad24 402, matchlog8 799. `never_fetched` и `stale` пересекаются; новые unfetched targets могут ещё находиться внутри SLA. Поэтому вычитание всех never-fetched из stale неверно; подготовленное исправление описано в [code-review.md](code-review.md).

Strict current completed-match backlog — **11** targets. Unscoped `current_completed_once=4708` включает старые/rolled-over scopes и не является текущим матчевым backlog. Малое число матчей также не доказывает completeness season_stats/squads/players/matchlogs. Global unprocessed raw вне SLA —2; current recovery lane —0, что согласуется с изоляцией двух Atlas history observations.

| Competition | Registry current season | Состояние | Source rejection |
|---|---|---|---|
| 68 USL First Division | 2009 | quarantined, последний fetch31.07 | `schedule_link_missing` |
| 76 North American Soccer League | 2017 | quarantined, последний fetch31.07 | `schedule_link_missing` |
| 79 USSF Division 2 Professional League | 2010 | quarantined, последний fetch31.07 | `schedule_link_missing` |

Все три всё ещё active/present в registry. Их исторические страницы не доказывают продолжающуюся current обязанность; lifecycle требует source evidence. Одновременно есть реальные просрочки действующих турниров: 06.09 gate сообщал 451 stale season_stats (65 never fetched), aggregate571 stale; 07.09 —88 stale schedules, 451 season_stats, aggregate544. Это отдельные более ранние срезы, их нельзя складывать с числами 12 сентября.

## Единственная применённая data cleanup

Исторический test target `fbref:test:concurrent-index:26816b472c144534a6e4348964e56687` создан 15.07, указывал на `example.invalid`, имел пустые source IDs и **не имел fetch attempts**. Scope query допускал каждый `page_kind=competition_index` без проверки canonical target identity, поэтому строка создавала постоянную ложную просрочку. Её форма соответствует concurrency fixture в `tests/integration/scrapers/test_fbref_control_admission.py`; текущий тест использует изолированную БД. Кто создал историческую строку, не установлено.

После точной backup, dependency audit, независимого SQL review и dry-run с rollback в **12.09 10:29:25 UTC** изменена ровно одна строка: `source=fbref-test-fixture`, `state=quarantined`, `last_error_class=TestFixtureArchived`. Связанный failed immutable cohort и test runs сохранены. Другой run, содержащий реальный матч, не изменён. Настоящий `fbref:competition_index:all` остался свежим и неизменным.

Операция обратима; backup и полный apply log сохранены только локально. Это устранение одной ложной просрочки, не ремонт остального current scope. Более строгий положительный source-identity contract для index eligibility остаётся предметом ревью.

## Совместная работа и runtime drift

History держит source publication lock на recovery/fetch/parse/validate даже при `publish=false`; current acquire без retry падает при пересечении. 02.09 history работала 02:37–07:28 UTC, 03.09 —00:27–06:17 UTC: обе волны перекрыли current06:00 и вызвали observed lock failures. Дополнительно общий Airflow pool имеет один slot. Нужны доказанные writer boundaries и реальное пересечение live tasks при резерве current.

Driver предполагал 180 минут истории, но волны достигали 4–6 часов. DAG timeout18h не отменяет subprocess limit6h и live-task limit6h05m. Шесть history и три current runs упёрлись в subprocess ceiling. Средняя наблюдённая history wave длительность выросла примерно с2,4h у стартов24.08 до4,9h у стартов01.09. Причина всей прибавки времени одним subsystem не установлена без timing profile.

Effective runtime читал изменённые FBref DAG/current factory/control store/pipeline из интеграционного дерева с uncommitted overlay. Один Git HEAD не идентифицировал исполняемый FBref; общий image также не фиксирует mounted source. FBref gateway разделял lifecycle общего стека. Эти факты требуют воспроизводимого release manifest и изоляции доставки; внутренние адреса, mounts и registry details в публичный архив не включены.

## Maintenance и historical frontier

`janitor_fbref_generic_stages` упал все **20/20 дней**, **60 попыток**, без сетевой работы. Последний outcome: `attention=10`, `audit_only_eligible=0`; retained stages требуют решения по writer/provenance state. Позднейшая read-only классификация уточнила состав: **8 typed stages + 2 generic batch-cell stages**, а не 10 generic observation stages. Cleanup не выполнен; позднейшая audit utility не покрыта результатом 1770 passed / 1 skipped. Сохранение stage защищает незавершённые данные. `maintain_other_high_churn_bronze` успешно выполнился все20 дней; утверждение «вся maintenance сломана» неверно.

Registry: **117 active/present male competitions** —74 club leagues,23 club cups,20 national-team competitions; **1 975 male seasons**,117 flagged current. Отдельно36 female competitions skipped. Source season labels охватывают1888–1889…2027. Registry presence не доказывает, что все соответствующие данные fetched/typed.

| Historical page kind | Pending/retry | Fetched | Quarantined |
|---|---:|---:|---:|
| match | 10 125 | 2 647 | 0 |
| matchlog | 89 539 | 5 234 | 1 |
| player | 24 738 | 3 076 | 3 |
| schedule | 707 | 69 | 23 |
| season | 1 033 | 511 | 295 |
| season_stats | 2 409 | 85 | 0 |
| squad | 41 834 | 2 854 | 1 |
| **Итого** | **170 385** | **14 476** | **323** |

Всего185 184 известных historical targets, fetched≈7,82%. Discovery продолжает добавлять потомков: это доля известной очереди, не процент всей доступной истории и не основание для даты завершения. Fetched counts не утверждают typed persistence/validation.

В сохранённом window snapshot —15 181 fetch attempts:14 515 success,595 failed,71 cancelled. Failed classes:331 HTTP403,92 warm-session connection,78 clearance,48 timeout-aborted,14 HTTP301,12 oversized HTTP200,10 warm-session timeout,5 HTTP404,5 hard transport/meter policy. Attempt count отличается от metered-request count; числа характеризуют наблюдённую эксплуатацию, а не денежный бюджет.

## Пределы выводов

Полные сканы крупных Bronze-таблиц не выполнялись; Iceberg metadata проверялось для свежести записей. Аудит не сертифицирует все строки, source gaps или полноту истории. Новых scrape/proxy requests, service changes и history restart в аудите не было; доступность провайдера вне записанных попыток не перепроверялась. Raw для воспроизведения читался из уже сохранённых материалов.

Приёмка остаётся открытой: current≤24h, устойчивый параллельный backfill, измеримый coverage, восстановление после ошибок и 7-дневный soak. История может продолжать загружаться после первого GO; её окончательная полнота имеет отдельный verdict. Денежный proxy budget не входит в задачи/GO. Полные внутренние evidence сохранены локально; [JSON-сводка](findings-verification.json) содержит выбранные проверяемые факты.
