# SofaScore: повторный разбор immutable raw в Bronze (#1367)

## Назначение и границы

DAG и worker повторно разбирают уже оплаченные immutable raw и записывают Bronze
без запросов к источнику: обязательный `source_request_count=0`.
Squads остаются raw-only; таблица squads в Bronze не создаётся.

Offline validation, merge кода, доставка и production-приёмка — отдельные факты.
Текущее поручение разрешает только offline-код и тесты; этот документ не
подтверждает выполнение тестов, merge, доставку, replay или приёмку.
Production replay требует применения #1366 с независимой проверкой,
точного доставленного SHA и отдельного разрешения владельца.
Существующие пять DAG, auto-deliver и pause controls остаются без изменений,
пока отдельное поручение не разрешит иное.
Issue #1367 остаётся OPEN до production-приёмки; Miro, таймеры и новые
операционные полномочия этим runbook не вводятся.

## Материализация и grain

| Таблица | Grain / ключ | Поля и условия |
| --- | --- | --- |
| `bronze.sofascore_lineups` | Существующий grain lineups | `player_name`; `formation` на уровне стороны; lossless canonical JSON-массивы `missing_players_json`, `support_staff_json`. На когорте приёмки все имена непустые и не NULL. |
| `bronze.sofascore_league_table` | Существующий grain league table | `position`, `promotion`, lossless `promotion_json`. |
| `bronze.sofascore_rounds` | Один раунд сезона соревнования; `[league, season, round_id]` | Lineage ниже. |
| `bronze.sofascore_cup_trees` | Один узел сетки; `[league, season, cup_tree_id, node_id]` | `parent_node_id`, lossless `children_json`. |
| `bronze.sofascore_participants` | Одна команда; `[league, season, team_id]` | Lineage ниже. |
| `bronze.sofascore_referee_profile` | Один ответ на запрошенный профиль; `[league, season, requested_referee_id]` | Фактические `referee_id` и `slug` хранятся отдельно от requested ID. |

Все четыре новые season-таблицы содержат `source_tournament_id`,
`source_season_id`, `raw_content_hash`, `raw_blob_key` и lineage `league`/`season`.
Lossless-поля сохраняют исходное содержимое; канонизация JSON не означает
отбрасывание элементов или атрибутов.

## Контракт worker

Точная форма CLI (альтернативы через `|`, необязательные части в `[]`):

```text
python dags/scripts/replay_sofascore_raw.py --mode fields|drain --scope-json '{"source_tournament_id":"17","source_season_id":"76986","freshness_key":"final"}' [--target-cap 25] [--output /tmp/sofascore_replay_result.json] [--raw-store-uri URI] [--manifest-backend trino|json] [--manifest-path PATH]
```

Это описание интерфейса, а не разрешение на production-запуск.
Scope содержит ровно три строковых поля: `source_tournament_id`,
`source_season_id`, `freshness_key`; дополнительные поля недопустимы.
`fields` — первый режим parser backfill: повторный разбор оплаченных полей
при любом статусе manifest. `drain` выбирает retryable HTTP 2xx, кроме 204,
с raw lineage и предназначен для погашения retryable-долга.
JSON manifest получает путь явно либо через `SOFASCORE_MANIFEST_PATH`;
`--manifest-path` с backend `trino` недопустим.
Неверный ввод завершает worker с кодом 1 до capture runtime.
Production использует существующий manifest backend, никогда локальный JSON.
Не добавлять live/proxy/workload/force-replace/source-retry флаги.

## DAG и bounded run/resume

`dag_replay_sofascore_raw` разрешён только в изолированном контуре SofaScore;
на общем scheduler он скрыт через `dags/.airflowignore`.
Настройки: `schedule=None`, `is_paused_upon_creation=True`, `catchup=False`,
`max_active_runs=1`, `max_active_tasks=2`, `dagrun_timeout=2h`.

| Param | Допустимое значение |
| --- | --- |
| `mode` | `fields` по умолчанию либо `drain` |
| `scope_cap` | Положительное целое, по умолчанию 3 |
| `target_cap` | Положительное целое, по умолчанию 25 |
| `source_tournament_id`, `source_season_id` | Оба null либо оба непустые строки |

Цепочка: `planner` → bounded mapped `replay_scope` для каждого scope →
`validate_summary` с `all_success`. Ошибка worker оставляет run красным;
summary её не скрывает. Пустой план — успешный no-op, не доказательство полноты.
Опциональная пара ID ограничивает план сезоном соревнования;
точный worker scope также содержит `freshness_key`.

Planner возвращает env `SOFASCORE_REPLAY_MODE`, `SOFASCORE_REPLAY_SCOPE_JSON`,
`SOFASCORE_REPLAY_TARGET_CAP`, `SOFASCORE_REPLAY_OUTPUT`.
Результат каждого scope сохраняется по пути:

```text
/opt/airflow/logs/sofascore-raw-replay/<safe-run-id>/<tournament>-<season>-<digest16>.json
```

Команда worker создаёт родительский каталог перед exec.
Summary требует пустые errors, `source_request_count=0`, нулевые
deferred/schema_error и exit codes, а также фактические соответствующие
publication receipts для каждой таблицы. Для legitimate terminal empty
результата таблица не требуется. Summary сообщает scopes/tables/rows,
но не выполняет production SELECT.

Будущая ручная процедура после отдельного разрешения:

1. Подтвердить #1366 applied + independently verified, доставленный SHA,
   разрешённый изолированный контур и границы согласованного scope.
2. Выбрать `fields` для первого backfill или `drain` для retryable-долга;
   зафиксировать caps и при необходимости точную пару ID.
3. Запустить ограниченный ручной run в согласованном контуре; сохранить
   план, результаты каждого scope, receipts, ошибки и итог DAG.
4. При сбое изучить причину, затем повторить согласованный scope с теми же
   caps и детерминированным порядком manifest. Writer lock защищает запись;
   не обходить его и не удалять lock вручную.
5. Проверить SELECT readback и остаток manifest в полном целевом scope.
   Продолжать drain в разрешённых границах до выполнения критериев.

Idempotent MERGE и raw lineage позволяют повторить выбранный упавший scope
без source retry. Caps ограничивают доказательство выбранными scopes/targets:
успех одного run не подтверждает глобальную полноту или нулевой долг.
Повторный `fields` с теми же caps сам по себе не доказывает продвижение
по всем остальным targets; охват нужно сверять по плану и результатам.

## Offline validation

Только в изолированном worktree, без production Trino/S3/source:

```bash
/root/.venvs/dpf-test/bin/pytest tests/unit/scripts/test_replay_sofascore_raw.py tests/unit/scrapers/test_sofascore_raw_fields.py tests/unit/scrapers/test_sofascore_raw_replay.py tests/unit/dags/test_dag_replay_sofascore_raw.py -q
/root/.venvs/dpf-test/bin/pytest tests/unit -q
```

Сохранить результаты существующей CI matrix `real-airflow-dag-import`
для Airflow 2.7.3/2.11.2 и локального изолированного эквивалента DagBag.
Импорт DagBag не доказывает перечитывание DAG или отсутствие import errors
в работающем scheduler; это отдельная будущая проверка после доставки.

## Ошибки, повтор и откат

Bronze MERGE предшествует фиксации success в manifest.
Ошибки writer сохраняют фактические receipts, но оставляют выбранные записи
deferred. Parser/schema errors, кроме referee, блокируют публикацию сезона.
`legitimate_empty` и `not_supported` — терминальные статусы, не deferred.
Ненулевые phase exit codes и ошибки должны быть видны в доказательствах;
повтор идемпотентен и не требует нового запроса к источнику.
Запрещены ручной success upsert и удаление истории manifest.
Откат на дерево без нового DAG совместим с delivery acceptance;
сам откат не разрешён этим документом и не доказывает приёмку данных.

## Будущий SELECT readback после отдельного разрешения

Ниже шаблоны, не production-выгрузки. Подставлять только согласованные
фильтры; `:league`, `:season`, `:tournament`, `:source_season`, `:freshness`
обозначают параметры, а не готовый синтаксис bind для Trino CLI.
`<acceptance_cohort_predicate>` — точное условие непустой когорты приёмки.

```sql
WITH cohort AS (
  SELECT player_name FROM bronze.sofascore_lineups
  WHERE league = :league AND season = :season
    AND (<acceptance_cohort_predicate>)
), counts AS (
  SELECT COUNT(*) AS total,
    COUNT(*) FILTER (WHERE player_name IS NULL OR btrim(player_name)='') AS missing
  FROM cohort
)
SELECT total, missing,
  CASE WHEN total=0 THEN '__no_rows__'
       WHEN missing=0 THEN '0' ELSE 'FAIL' END AS name_check
FROM counts;
```

`total` — результат `COUNT(*)`; нулевая когорта даёт `__no_rows__`, не PASS.
Нужны `total>0`, `missing=0` и 100% непустых имён.
Дополнительно сверить materialized fields lineups и league_table с immutable
raw: side-level formation, оба JSON-массива, position/promotion/promotion_json.

Для каждой новой таблицы отдельно проверить непустую raw lineage:

```sql
SELECT 'rounds' AS entity, COUNT(*) AS with_raw FROM bronze.sofascore_rounds
WHERE league=:league AND season=:season
  AND source_tournament_id=:tournament AND source_season_id=:source_season
  AND btrim(raw_content_hash)<>'' AND btrim(raw_blob_key)<>''
UNION ALL
SELECT 'cup_trees', COUNT(*) FROM bronze.sofascore_cup_trees
WHERE league=:league AND season=:season
  AND source_tournament_id=:tournament AND source_season_id=:source_season
  AND btrim(raw_content_hash)<>'' AND btrim(raw_blob_key)<>''
UNION ALL
SELECT 'participants', COUNT(*) FROM bronze.sofascore_participants
WHERE league=:league AND season=:season
  AND source_tournament_id=:tournament AND source_season_id=:source_season
  AND btrim(raw_content_hash)<>'' AND btrim(raw_blob_key)<>''
UNION ALL
SELECT 'referee_profile', COUNT(*) FROM bronze.sofascore_referee_profile
WHERE league=:league AND season=:season
  AND source_tournament_id=:tournament AND source_season_id=:source_season
  AND btrim(raw_content_hash)<>'' AND btrim(raw_blob_key)<>'';
```

Каждый `with_raw` должен быть >0 на согласованной когорте этих таблиц.
Примеры ключей — только placeholders: `[<league>,<season>,<round_id>]`,
`[<league>,<season>,<cup_tree_id>,<node_id>]`, `[<league>,<season>,<team_id>]`,
`[<league>,<season>,<requested_referee_id>]`; lineage —
`<source_tournament_id>/<source_season_id>/<raw_content_hash>/<raw_blob_key>`.
Сверить parent/children узлов и actual referee_id/slug с соответствующим raw.

```sql
SELECT COUNT(*) AS retryable_200
FROM iceberg.ops.sofascore_capture_manifest
WHERE source_tournament_id=:tournament AND source_season_id=:source_season
  AND freshness_key=:freshness
  AND status='retryable_failure' AND http_status=200;

SELECT status, COUNT(*) AS remaining
FROM iceberg.ops.sofascore_capture_manifest
WHERE source_tournament_id=:tournament AND source_season_id=:source_season
  AND freshness_key=:freshness
GROUP BY status ORDER BY status;
```

На завершённом полном target scope требуется `retryable_200=0`.
Повторить readback для всех входящих freshness; caps и невыбранные freshness
требуют дальнейшего drain. Этот счётчик не заменяет анализ остальных 2xx
retryable-записей и групп статусов. Нужны одновременно SELECT и per-scope results.

## Доказательства приёмки

- Точный SHA: проверенный, merged и доставленный; доказательства #1366 и разрешение.
- Offline test logs, CI matrix и изолированный DagBag import.
- Per-scope selected targets/status, scope с freshness, caps, `source_request_count=0`.
- Фактические publication receipts, errors, deferred/schema_error и phase exit codes.
- SELECT readback: непустая когорта имён, четыре таблицы с raw lineage, остатки manifest.
- Независимая проверка; после доставки — фактическое состояние изолированного scheduler.

Merge не является доставкой; доставка не является production-приёмкой.
До подтверждения всех критериев issue #1367 остаётся OPEN.
