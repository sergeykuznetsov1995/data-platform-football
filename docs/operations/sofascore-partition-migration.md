# #1366 — миграция исторических Bronze league keys SofaScore

Статус: **prepared**. Production не мигрирован, не проверен и не принят.
Issue #1366 остаётся **OPEN**. Этот PR подготавливает код, синтетические тесты
и процедуру; он не выполняет production preflight или apply.

Для любого preflight против production и для apply обязательно отдельное
разрешение владельца с указанием среды, release SHA и окна. Наличие команды
в этом документе, standing consent на модель и флаг `--writers-paused` не
являются таким разрешением. Источник остаётся **pause/OFF**; эта процедура
не разрешает его возобновлять, доставлять код или менять scheduler.

## Область и ограничения

Меняются только исторические таблицы SofaScore Bronze, входящие одновременно
в `PARTITION_SCOPED_TABLES` и контракт `configs/sofascore/endpoint_coverage.yaml`.
Natural key читается из контракта; второй список ключей в скрипте отсутствует.
Все eligible-таблицы должны присутствовать в контракте. Partition declaration
должен быть ровно `(league, season)` в этом порядке. Silver/Gold и raw store
не затрагиваются.

Legacy league преобразуется по локальному реестру, загруженному с
`enabled_only=False`. Строгие `SS-<positive integer>` сохраняются. Неизвестный
legacy key, противоречие реестра, NULL/пустая часть natural key, неизвестный
тип колонки или неподдерживаемая структура блокируют работу. Сложные типы
(ARRAY/MAP/ROW/JSON и другие неразрешённые типы) намеренно блокируются:
их безопасное сравнение и выбор победителя здесь не доказаны.
Если `_ingested_at` присутствует, требуется тип timestamp: лексикографический
порядок произвольных строк не доказывает временной приоритет.

Внутри каждой canonical natural-key группы приоритет задают `_ingested_at
DESC NULLS LAST`, затем `_batch_id DESC NULLS LAST`, если эти колонки есть.
В базовом full rewrite далее SQL использует `raw_content_hash`, если он есть,
и остальные скалярные колонки в порядке исходной схемы для воспроизводимого
результата. Batched helper использует только доступные `_ingested_at`/`_batch_id`
(`metadata_only=True`) после обязательного source/archive preflight с
`unsafe_winner_groups=0`; scalar/hash tie-break базового пути не меняется. Хеш и
лексикографический порядок **не доказывают**, какая версия верна. Preflight
отдельно находит разные полные canonical строки среди лидеров с одинаковыми
доступными `_ingested_at`/`_batch_id`. Такой конфликт запрещает apply, даже
если хеши различаются. Если метаданных нет, разные строки одной группы
также запрещают apply. Полностью совпадающие canonical строки схлопываются;
каждый их исходный экземпляр остаётся в полном архиве.

## Предпосылки будущего окна

До подключения к production зафиксировать:

1. Отдельное разрешение владельца, точный release SHA и проверенный manifest
   поставки; версии контракта и локального реестра из этого же релиза.
2. Согласованный план pause/lock. Ни один legacy/current writer, ручной job,
   backfill, retry или второй migrator не должен писать в выбранные таблицы.
   Блокировка должна действовать от первого preflight до завершения проверки
   или rollback. Скрипт блокировку не создаёт и отсутствие writers не доказывает.
3. Каталог доказательств, например `/srv/sofascore-migration-evidence/r2-20261116`,
   созданный оператором в разрешённом месте. Сохранять stdout JSON, stderr,
   exit code, время факта, release SHA, идентификатор окна и подтверждение lock.
   Примеры ниже не содержат credentials.
4. Уже существующую согласованную archive schema, права на полный CTAS archive
   и атомарный `CREATE OR REPLACE TABLE`, запас места и доступный rollback.
   Скрипт не создаёт schema. Имя архива детерминировано source table и
   migration ID: префикс таблицы плюс полный SHA-256; точное имя есть в JSON.
5. Проверенный live adapter и совместимость конкретных Trino/Iceberg/catalog
   версий с атомарной заменой таблицы. Default factory лениво создаёт
   существующий `scrapers.base.trino_manager.TrinoTableManager()` только для
   preflight/apply; конструктор сам не открывает соединение. Встраиваемый
   `run_migration(..., manager_factory=...)` поддерживает проверенный адаптер
   того же manager interface. Реальные возможности движка в данном PR не
   проверены; это обязательный допуск перед production, а не повод обходить
   отказ.
6. Исходные `SHOW CREATE TABLE` для каждой таблицы и доступные snapshot IDs
   с временем фиксации. Сохранить свойства, типы, порядок колонок и права,
   необходимые для восстановления. CTAS-архив сохраняет строки, но сам по
   себе не сохраняет весь исходный DDL. Снимки дополняют полный архив.

Raw store остаётся неизменным. Скрипт не управляет retention, snapshot expiry,
доставкой, Airflow или возобновлением источника.

## Команды базового full rewrite

Запускать из корня проверенной рабочей копии релиза. Во всех режимах обязателен
один воспроизводимый `--migration-id`: 1–48 ASCII букв, цифр, `_` или `-`.
Режимы взаимоисключающие; при отсутствии режима используется dry-run.

Без подключения к каталогу/Trino:

```bash
python -m scripts.migrate_sofascore_partition_keys \
  --dry-run --migration-id r2-20261116 \
  --contract configs/sofascore/endpoint_coverage.yaml \
  --archive-schema bronze_migration_archive \
  > /srv/sofascore-migration-evidence/r2-20261116/dry-run.json
```

Dry-run читает только локальные контракт и реестр. JSON содержит
`mode=dry_run`, `mutates=false`, спецификации, archive SQL и rewrite template.
Шаблон явно помечен `rewrite_is_template=true`: без подключения нельзя знать
все реальные колонки и их порядок. Это не готовый SQL для ручного исполнения
и не доказательство готовности. Preflight подставляет проверенную схему и
возвращает точный SQL с `rewrite_is_template=false`.

Только после отдельного разрешения на production preflight:

```bash
python -m scripts.migrate_sofascore_partition_keys \
  --preflight --migration-id r2-20261116 \
  --contract configs/sofascore/endpoint_coverage.yaml \
  --archive-schema bronze_migration_archive \
  > /srv/sofascore-migration-evidence/r2-20261116/preflight.json
```

Этот режим выполняет только чтение SELECT/SHOW и чтение схем через manager.
Он возвращает counts/DDL, а не source row payloads. `ready=false`, ошибка
схемы/partition declaration, конфликт или неизвестный ключ дают exit code 2.
`ready=true` не подтверждает внешнюю блокировку writers и не разрешает apply.

**ВНИМАНИЕ: следующая команда изменяет Bronze. Выполнять только в отдельно
одобренном окне apply, после всех допусков и при удерживаемом внешнем lock.**
Флаг `--writers-paused` подтверждает обязательство оператора; он ничего не
останавливает. В рамках этого PR команду не выполнять.

```bash
python -m scripts.migrate_sofascore_partition_keys \
  --apply --migration-id r2-20261116 --writers-paused \
  --contract configs/sofascore/endpoint_coverage.yaml \
  --archive-schema bronze_migration_archive \
  > /srv/sofascore-migration-evidence/r2-20261116/apply.json
```

При повторной проверке того же ID сохранять новый JSON под новым именем,
например `repeat-01.json`; исходные доказательства не перезаписывать.
Exit code 0 означает успех данного режима. Для dry-run это только подготовка,
для preflight — успешные read-only проверки. Ошибка после DDL имеет
`success=false`; `mutates=true` означает, что DDL уже мог исполниться, включая
потерю ответа сервера. Автоматического повтора DDL нет.

## Порядок обработки и доказательства

Сначала скрипт выполняет read-only preflight **всех** выбранных таблиц.
При первом блокере он останавливается до каких-либо изменений. Проверяются
существование, упорядоченные колонки, natural key, partition declaration,
NULL/blank keys, non-SS keys, unmapped rows, число duplicate groups до/после
canonical mapping, число distinct canonical keys и конфликты победителя.
Известные legacy ключи и безопасно разрешимые дубли сами по себе не блокеры.

В базовом full rewrite (`run_migration`) затем таблицы обрабатываются по имени.
Для каждой:

1. Сохранить preflight JSON и исходный DDL/snapshot evidence. Полный архив:
   `CREATE TABLE <archive> AS SELECT * FROM <source>`. Не фильтровать,
   не канонизировать и не дедуплицировать архив.
2. Выполнить фактический scalar `SELECT count(*)` из архива и потребовать
   `archive_rows == before_rows`. Проверить схему и профиль архива. Сравнить
   полные мультимножества source/archive через двусторонний `EXCEPT ALL`;
   различие означает остановку перед rewrite. Эта проверка не заменяет lock.
3. Атомарно выполнить `CREATE OR REPLACE TABLE <source>` с исходным comment и
   полным исходным `WITH` block, включая format, partitioning и остальные
   properties; код не подставляет усечённый список свойств. Источник SELECT —
   уже проверенный полный архив. Проекция сохраняет каждую колонку и её порядок,
   заменяя только league; `ROW_NUMBER` группирует по declared natural key уже
   после canonical mapping. Победитель должен быть доказан preflight.
4. Снова фактически прочитать source и архив. Проверить исходный comment и все
   properties, partition declaration, порядок/типы колонок, counts,
   двустороннее покрытие ключей и полное сравнение
   ожидаемых canonical winner rows с результатом. Вызвать общий
   `validate_reconciliation` с фактическими результатами; ожидаемые числа
   не подставлять вместо запросов.
5. Сохранить JSON с before/archive/after DDL и counts, статусом и фазой,
   snapshot evidence и exit code. На первой ошибке остановиться; последующие
   таблицы не изменять. Предыдущие успешные таблицы автоматически не откатываются.

Обязательны все инварианты:

- `missing_natural_keys = 0` и `extra_natural_keys = 0` между архивным canonical
  множеством и фактическим source;
- `unmapped_rows = 0`, `null_natural_keys = 0`, `non_ss_keys = 0`;
- `duplicate_keys = 0` по declared natural key;
- `payload_mismatches = 0` для полного мультимножества ожидаемых победителей;
- `after_rows = before_distinct_canonical_keys`;
- `before_rows = after_rows + collapsed_duplicate_excess`;
- `archive_rows = before_rows`.

При повторе того же migration ID существующий архив никогда не заменяется.
Базовый full rewrite проверяет его схему, профиль, counts, canonical ключи и ожидаемые полные
winner rows против текущей таблицы. Только чистый совпадающий результат даёт
`noop_verified`, без нового archive/rewrite. Архив после прерванного apply при
ещё legacy/неполном source не разрешает автоматический повтор rewrite: будет
ошибка и нужен разбор в рамках отдельно утверждённого восстановления.

Доступные Iceberg snapshots можно сохранить отдельными read-only запросами
в разрешённом окне; отсутствие этой metadata table нельзя считать снимком:

```sql
SHOW CREATE TABLE bronze.<table>;
SELECT snapshot_id, committed_at, operation
FROM bronze."<table>$snapshots"
ORDER BY committed_at DESC;
SHOW CREATE TABLE bronze_migration_archive.<archive_from_json>;
```

Это шаблоны: `<table>`/`<archive_from_json>` заменяются проверенными именами.
Сам скрипт сохраняет DDL evidence; он не выдаёт его за snapshot ID и не
обещает доступность snapshot metadata конкретной установки.

## Partition-batched staging + metadata swap

Отдельный helper `scripts/migrate_sofascore_partition_keys_batched.py` использует
путь **полный archive → stage группами → полная проверка → два metadata renames
→ postcheck**. Это отдельный путь, а не изменение истории базового full rewrite.
Все допуски выше сохраняются: разрешение владельца на точный reviewed SHA и окно,
pause/OFF, внешний writer lock, source/archive preflight, natural-key dedup и
completeness guard. Обрабатывать только одну таблицу за раз. Наличие helper
не разрешает deploy, production-запросы или возобновление источника.

1. Создать либо проверить существующий полный архив: исходные колонки и строки,
   counts и двусторонний `EXCEPT ALL` source/archive. До staging обязателен
   source/archive preflight с `unsafe_winner_groups=0`.
2. `populate_stage(..., partition_group_size=8, should_stop=None)` группирует
   до **8 canonical `(league, season)` pairs** в count/INSERT/verification
   statements вместо отдельных statements для тысяч пар. Проекция сохраняет
   все исходные колонки и declared natural key; winner ORDER BY использует
   только доступные `_ingested_at`/`_batch_id` после указанного preflight.
   Existing и missing pairs внутри группы разделяются: existing/noop subgroup
   проверяется, INSERT выполняется только для missing subgroup. Existing
   payload/count mismatch и unknown extra stage identity блокируют работу;
   INSERT поверх существующих partitions запрещён. `should_stop` проверяется
   перед каждой из этих отдельных work groups, в том числе перед INSERT после
   долгой проверки existing subgroup. Функция возвращает batches и complete.
3. После завершения всех групп выполнить полный `verify_stage`: проверить
   схему, counts, полные ожидаемые winner rows и все перечисленные выше
   инварианты reconciliation. Полнота групп сама по себе проверки не заменяет.
4. Только после полной проверки выполнить два metadata renames:
   `source → legacy`, затем `stage → source`. Выполнить полный postcheck
   результата по тем же инвариантам. Полный archive и legacy не удалять.

Approved low-memory adapter использует `iceberg.max_partitions_per_writer=1`.
Прежние archive-only full rewrite probes завершились
`ICEBERG_TOO_MANY_OPEN_PARTITIONS` при 10 open partition writers. Поэтому default
group size — **8, не 16**: до восьми разных target partitions на INSERT, без
изменения global/session memory controls. Увеличивать group size или менять
session property без нового отдельного proof/approval запрещено. Cooperative
deadline не доказывает достаточность памяти. При повторном
`ICEBERG_TOO_MANY_OPEN_PARTITIONS` или 6GB OOM остановиться и разбирать отказ,
не подбирать параметры автоматически.

`run_batched_rewrite(..., max_duration_seconds=6000,
verification_reserve_seconds=1200, clock=time.monotonic)` прекращает начинать
новые work groups при достижении границы резервного времени для проверки.
Это cooperative bound: уже выполняющийся SQL не отменяется, внешний writer-lock
watchdog не заменяется. Внешний **7200s max-hold** допустим только как отдельно
разрешённый process-local параметр конкретного окна, не новый global/default.

При нормальной ранней остановке обязательно сохранить result JSON и дать lock
выйти штатно по согласованному плану. Отчёт содержит `success=false`,
`status=stage_incomplete`, `ready=false`, `resume_required=true`, фактический
`stage_complete` и список завершённых batches. Если группы закончились, но
резерв для полной проверки истёк, сохраняется тот же `stage_incomplete` с
`stage_complete=true`. При неполном
stage не выполнять полный verify/swap/ALTER; при непроверенном stage swap/ALTER
также запрещены. Caller может вернуть exit 2: это само по себе ни повреждение,
ни успешная миграция. Разобрать report/status, source/archive counts и
двусторонний `EXCEPT ALL`, активные queries и факт release lock.

Частичный stage совместим с ручным continuation только после read-only разбора
и отдельного разрешённого продолжения с **migration ID и exact reviewed SHA,
одобренным для данного окна**. Существующие полные archive/stage не
пересоздаются. Чтобы не повторять дорогую проверку уже завершённых groups при
каждом continuation, caller передаёт предыдущий terminal result как
`previous_result`. Он обязан иметь `stage_incomplete`, `resume_required=true`,
отсутствие error/rollback_error и совпадающие migration/table/archive/stage/
legacy identity. Его batches должны образовывать точный prefix текущих work
groups; каждая checkpoint partition обязана остаться в stage с прежним group
row count. Иначе продолжение останавливается до INSERT/ALTER. Новый результат
сохраняет cumulative `batches` и число пропущенных `resumed_groups`, поэтому
следующее окно может использовать его как следующий checkpoint.

Только не обработанные ранее existing groups проверяются повторно; missing
обрабатываются, INSERT поверх existing запрещён. Полный `verify_stage` со всеми
counts, natural keys и полным payload comparison обязателен до обоих renames,
включая checkpoint-skipped groups. Уже canonical source с legacy backup
проходит full noop verification. Автоматические retry/loops и новый migration
ID для обхода блокера запрещены.

Короткий incident-пример объясняет необходимость группировки: на reviewed SHA
`1036e795` старый per-partition apply `event_participants` обработал 1,112 из
4,793 partitions за 7,200s; watchdog завершил процесс до verify/swap. При разборе
source=archive=1,209,122, `EXCEPT ALL=0`, stage=302,372, legacy absent,
active INSERT=0, lock released. Это исторический пример, не evidence текущего
состояния и не подтверждение production applied/verified/accepted нового кода.

## Независимые read-only проверки дублей

Следующий шаблон проверяет **declared natural key после canonical mapping**.
Подставить таблицу, CASE из `canonical_league_expression` в JSON и точный список
ключей из контракта. В natural key league заменить на alias `canonical_league`;
остальные компоненты сохранить. Выполнять и для архива, и для результата.
В архиве до rewrite группы допустимы и должны соответствовать отчёту; в
результате после rewrite ожидается **ноль строк**.

```sql
WITH canonical AS (
    SELECT *,
           <static_CASE_from_preflight_JSON> AS canonical_league
    FROM bronze.<table>
)
SELECT <declared_natural_key_with_canonical_league>, count(*) AS rows_per_key
FROM canonical
GROUP BY <declared_natural_key_with_canonical_league>
HAVING count(*) > 1;
```

Отдельно проверить логические дубли матчей в таблице с grain «матч», имеющей
`season`/`match_id`. Здесь league намеренно не участвует:

```sql
SELECT season, match_id, count(*) AS rows_per_match
FROM bronze.<match_grain_table>
GROUP BY season, match_id
HAVING count(*) > 1;
```

Ноль строк — ноль групп логических дублей. Natural key гарантирует уникальность
контрактного grain конкретной таблицы, а `(season, match_id)` проверяет другую
гипотезу: один логический матч независимо от league. Для событий, статистики,
составов и иных дочерних grain несколько строк на матч могут быть корректны;
второй запрос нельзя использовать как основание для их удаления. Наличие
одного match ID в разных строгих SS leagues скрипт не склеивает автоматически:
такой результат требует предметного разбора и отдельного решения владельца.

После разрешённого выполнения сравнить before/after журнал и измеритель
**по множествам match IDs**, а не только по количеству физических строк.
Разнести исходные строки архива, distinct canonical keys, collapsed excess,
match IDs и логические duplicate groups. Падение row count из-за безопасного
схлопывания не должно скрывать исчезновение match ID. Offline/FakeManager
тесты не доказывают ноль дублей в production и не заменяют эти измерения.

## Остановка и rollback

При любом отказе сохранить фазу/JSON, исходный DDL, архив и доступные snapshots;
оставить источник pause/OFF и lock согласно согласованному плану. Не продолжать
со следующей таблицей и не подбирать новый migration ID для обхода ошибки.
Не удалять архив или legacy и не применять DELETE/DROP/TRUNCATE или изменения raw store.

Incomplete/unverified stage без swap не требует восстановления source, но его
сохранность нужно доказать read-only: counts и двусторонний `EXCEPT ALL`
source/archive, состояние имён и активных queries. Orphan query или process exit
сами по себе не доказывают, что сервер прекратил либо завершил INSERT. До
readback состояния не повторять DDL; проверить завершение queries и release lock
по согласованному плану. Сохранить incomplete result JSON, не запускать retry.

Batched helper при отказе второго rename или postcheck пытается откатить имена
в обратном порядке и возвращает `rollback_error`. Это только ограниченный
автоматический rollback метаданных; он не распространяется на базовый
`run_migration`. Любой `rollback_error` требует остановки и разбора, его нельзя
игнорировать. Даже после попытки rollback сохранность подтверждается readback
имён, DDL, counts и полных мультимножеств, а не предположением. Archive и legacy
сохраняются.

Восстановление данных — отдельное разрешённое действие оператора. Из сохранённого **исходного
DDL** подготовить атомарное восстановление source с исходными свойствами,
типами и порядком колонок, читая **весь** полный архив без canonical CASE и
без дедупликации. Перед выполнением проверить поддержку операции, schema
compatibility, права и отсутствие writers. После восстановления сравнить
counts и полные мультимножества source/archive в обе стороны через
`EXCEPT ALL`, затем повторно снять DDL/snapshot evidence. Архив сохраняется.
Универсальная команда rollback здесь не дана: CTAS с текущими свойствами
не гарантирует восстановление исходного DDL. У базового full rewrite
`run_migration` автоматического rollback нет; ограниченный rollback имён batched
helper описан выше.

## Фиксация результата

`prepared` — готов код и процедура; `applied` — есть доказательство исполнения
DDL; `verified` — пройдены фактические reconciliation и предметные проверки;
`accepted` — выполнены все действующие критерии issue и оформлена приёмка.
`applied_verified` в JSON обозначает только проверки этой утилиты, а не
приёмку источника, работу scheduler или выполнение многодневных критериев.

В будущем после разрешённого окна сохранить результат в текущем source handoff
и журнале приёмок по действующей процедуре проверенной установки, с readback
и ответственным за следующий шаг. В этом PR журнал production-приёмки не
обновляется как успешная миграция, issue не закрывается и источник не запускается.
Исполнители пакетов не запускают тесты; проверки ведущего и обязательный CI
выполняются отдельно. Ни dry-run, ни offline-тесты не подтверждают production apply,
верификацию или приёмку.
