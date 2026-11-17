# #1366 — partition-batched миграция SofaScore

Статус: **prepared, не выполнено**. Этот runbook описывает новый путь для таблиц, у которых цельный `CREATE OR REPLACE` упирается в память или open-partition limits. Production apply требует отдельного разрешения владельца с точным reviewed merge SHA, writer pause и окном.

## Зачем отдельный алгоритм

На production `bronze.sofascore_event_participants` цельный rewrite дважды превысил per-node 6GB. Изменение writer settings и сортировка не помогли: `max_partitions_per_writer=1` приводит к `ICEBERG_TOO_MANY_OPEN_PARTITIONS`. Поэтому прежний runbook остаётся неприменимым к этой таблице. Нельзя повышать memory limit или повторять DDL вслепую.

На момент написания:

- исходная таблица сохранена: **1 209 122 строки**;
- полный архив `bronze_migration_archive.sofascore_event_participants_pk_b0a7c03ec15fd7499aebe594e6301cd7707e2c72a3a2c366171f358407535ffc` содержит **1 209 122 строки**;
- source и archive имеют `EXCEPT ALL = 0` в обе стороны;
- staging ещё не собран, другие таблицы не мигрированы, production `duplicates=0` не доказан;
- issue #1366 остаётся OPEN, `close_hold=true`.

## Что делает `run_batched_rewrite`

`scripts/migrate_sofascore_partition_keys_batched.py` использует тот же `TableSpec`, natural key, registry mapping и winner selection, что и исходный мигратор.

1. Проверяет source и archive, включая null/unmapped/unsafe winners. Если полного архива нет, создаёт его целиком. Существующий архив никогда не перезаписывает.
2. Требует `source ↔ archive EXCEPT ALL = 0` до начала изменений.
3. Создаёт пустую staging-таблицу с исходным comment и properties, кроме независимого `location`.
4. Читает distinct `(canonical_league, season)` и вставляет каждую партицию отдельным `INSERT INTO`. Filter находится в canonical CTE **до** `ROW_NUMBER`, поэтому ENG/SS строки одной canonical партиции дедуплицируются вместе.
5. Уже совпадающую партицию пропускает как `noop`; неожиданные или отличающиеся строки останавливают работу без перезаписи staging.
6. До swap требует фактические:
   - `duplicate_groups = 0`, `non_ss_keys = 0`, `unmapped_rows = 0`, `null_natural_keys = 0`;
   - `missing_natural_keys = 0`, `extra_natural_keys = 0`;
   - `payload_mismatches = 0` в обе стороны;
   - `after_rows = before_distinct_canonical_keys`;
   - `archive_rows = before_rows = after_rows + collapsed_duplicate_excess`.
7. Только после полной проверки выполняет:
   - `ALTER TABLE source RENAME TO <deterministic_legacy>`;
   - `ALTER TABLE stage RENAME TO source`.
8. Повторяет полную проверку после swap. Ошибка второго rename или postcheck запускает rollback rename в обратном порядке. Ошибка rollback записывается в `rollback_error` и не маскируется.
9. Повтор на уже canonical source с существующим legacy backup выполняет только проверку и возвращает `noop_verified`; повторной записи нет.

`legacy` и полный archive сохраняются. `location` у source после swap новый (staging); остальные comment/properties обязаны совпасть. Это отличие от прежнего CTAS runbook явно проверяется.

## Ограничение двух rename

Два rename — metadata operations, но **не одна атомарная транзакция для читателей**. Writer pause обязателен; читатели могут увидеть короткое отсутствие source между rename. Если это недопустимо для текущего окна, алгоритм применять нельзя. Полный archive и legacy backup позволяют восстановить исходное имя и строки, но не являются гарантией непрерывной доступности.

Не выполнять `DELETE`, `DROP`, `TRUNCATE` и автоматический retry после неоднозначного результата. Сначала проверить фактическое состояние таблиц и каталог.

## Команды — только после отдельного разрешения

Запускать из проверенной чистой рабочей копии одобренного SHA. Пример — API, не инструкция запускать сейчас:

```python
from scrapers.base.trino_manager import TrinoTableManager
from scrapers.sofascore.catalog import SofaScoreCatalog
from scripts import migrate_sofascore_partition_keys as migration
from scripts import migrate_sofascore_partition_keys_batched as batched

migration_id = "r1366-20261116-6639"
archive_schema = "bronze_migration_archive"
spec = migration.TableSpec(
    "bronze.sofascore_event_participants",
    ("league", "season", "match_id", "team_id"),
    ("league", "season"),
)
archive = migration.archive_name(spec, migration_id, archive_schema)
mapping = migration.canonical_mapping(
    SofaScoreCatalog.load().tournament_map(enabled_only=False)
)
manager = TrinoTableManager(host="127.0.0.1", port=8082)
try:
    report = batched.run_batched_rewrite(
        manager, spec, archive, mapping, migration_id, archive_schema,
    )
    if not report["success"]:
        raise RuntimeError(report)
finally:
    manager.close()
```

Для остальных таблиц specs берутся из `configs/sofascore/endpoint_coverage.yaml`; нельзя составлять второй список natural keys вручную. Внешний writer lock должен удерживаться от preflight до конца проверки или rollback. Процесс не управляет pause/OFF и не возобновляет источник.

Для широких таблиц допускаются process-local ограничения параллелизма, проверенные на текущем Trino: `task_max_writer_count=1`, `task_concurrency=1`, `scale_writers=false`, `task_scale_writers_enabled=false`, `max_writer_task_count=1`, `spill_enabled=true`. Не повышать memory limit. Не устанавливать `iceberg.max_partitions_per_writer=1`: этот вариант уже отклонён реальной ошибкой.

## Обязательные readback-проверки

Для каждой таблицы сохранить `before`, `stage_verification`, `after`, `reconciliation`, archive/stage/legacy names и `rollback_error`.

Natural key из контракта, с `league` после canonical mapping:

```sql
SELECT <declared_natural_key>, count(*) AS rows_per_key
FROM iceberg.bronze.<table>
GROUP BY <declared_natural_key>
HAVING count(*) > 1;
```

Ожидание — **ноль строк**. Дополнительно сверить фактические `source_rows`, `archive_rows`, `collapsed_duplicate_excess`, `missing/extra_natural_keys`, `payload_mismatches` и match IDs. Для match-grain таблиц отдельно проверить `(season, match_id)`; дочерние строки одного матча нельзя считать дублями по этому ключу.

Повтор `run_batched_rewrite` должен дать `noop_verified`, а не новую запись. Offline FakeManager-тесты не заменяют эти production readbacks.

## Остановка и rollback

При ошибке второго rename или postcheck helper пытается вернуть имена сам. Если `rollback_error` не NULL, оператор останавливается: сохранить JSON и фактические имена/DDL/snapshots; дальнейший ручной rollback — только отдельное разрешение владельца. Архив и legacy не удалять. Новый migration ID не использовать для обхода ошибки.

Production-доставка, issue closure, снятие `close_hold` и возобновление источника этим runbook не разрешены.
