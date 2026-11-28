# Измеритель нагрузки FotMob на Trino (#2126)

## 1. Назначение и границы комплекта

Комплект измеряет нагрузку на `iceberg.bronze.fotmob_*` под пользователем
`src_fotmob`: история запросов → минутные ряды → суточная запись → вердикт
N1–N5. Модуль выдаёт результат одного полного UTC-дня. **Три полных UTC-дня
подряд** проверяет внешний журнал приёмки; установка, запуск и синтетический
PASS сами по себе серию не начинают.

Все запросы producer — read-only. Результаты сохраняются локально, без
DDL/DML, записи в Iceberg, включения источника, изменения общего scheduler
или запуска Silver. Хранилище платформы — SeaweedFS S3 и Lakekeeper; Spark и
HDFS не используются. Установку выполняет платформенная сессия по регламенту.
Этот документ содержит предложение установки, а не свидетельство её выполнения.

Состав репозитория:

- [Модуль](../../scrapers/fotmob/load_meter.py): stdlib на уровне импорта;
  клиент `trino` загружается только внутри `poll` и `qualify`.
- [Unit-тесты](../../tests/unit/scrapers/test_fotmob_load_meter.py): полный
  день из 1440 минут создаёт детерминированный генератор `build_day`.
- [Повторы snapshot](../../tests/fixtures/fotmob/load_meter/snapshot_overlap_duplicates.jsonl):
  перекрытие окон, повторные `query_id`, конфликт полей.
- [Неизвестный SQL и чужая нагрузка](../../tests/fixtures/fotmob/load_meter/unparsed_and_foreign.jsonl).
- [Шаблон суточной записи](../../tests/fixtures/fotmob/load_meter/day_report_template.json):
  синтетический PASS с двумя таблицами. Это пример схемы, не производственное доказательство.

Большие JSONL-фикстуры в репозитории не хранятся. Платформа создаёт временный
полный день тем же тестовым генератором и отдельно передаёт синтетические
результаты S7 в приёмочный офлайн-прогон.

## 2. Форматы и время

Каталог фиксирован: `iceberg`; слой: `bronze`. Таблицы записываются полностью,
например `iceberg.bronze.fotmob_matches`. Метки — явный UTC с `Z`. Входные
query timestamps могут иметь доли секунды; выходная минута всегда имеет
секунды `00`. Неявная локальная зона и ненулевое смещение во входных JSONL
отвергаются. Интервалы полуоткрытые: `[start, end)`.

### 2.1. Минутный ряд

| Поле | Тип и значения | Смысл |
| --- | --- | --- |
| `grain` | string, `minute_table` | Формат строки |
| `minute_utc` | UTC timestamp, начало минуты | Ключ времени |
| `table` | полное имя или null | null только у gap-маркера |
| `user_attr` | `src_fotmob`, `platform`, `unknown` | Атрибуция |
| `operation` | `commit`, `scan_read`, `scan_write` | Тип события |
| `count` | целое ≥0 или null | null только у gap-маркера |
| `coverage` | `measured`, `gap` | Измеренное событие или пробел покрытия |
| `gap_reason` | null, `no_sample`, `history_lost`, `producer_down` | Причина пробела |
| `source_polls` | array[string] | Poll ID первого наблюдения запросов |

`commit` — write-statement по цели. Соответствие физическим коммитам
проверяется **ежедневно S7**, а не живой пробной записью. `scan_read` —
обращение в read-statement. `scan_write` — источник внутри write-statement;
цель UPDATE/DELETE также получает один `scan_write`. N4 использует сумму
двух видов сканов, N2 — только `scan_read` по двум poll-eligible таблицам.

Минутный файл разреженный: события с положительными счётчиками и один
gap-маркер на каждую непокрытую минуту. Нулевые события не синтезируются.
У gap-маркера `table=null`, `count=null`, `user_attr=unknown`,
`operation=scan_read`; последние два значения — технические заполнители,
не чужая операция и не скан. Если в неполной минуте часть запросов была
увидена, наблюдённые события сохраняются рядом с gap-маркером. Они не
устраняют пробел. Полнота определяется heartbeat, а не наличием событий.

### 2.2. Суточный ряд и заголовок

| Поле ряда | Тип и смысл |
| --- | --- |
| `grain`, `day_utc` | `day_table`, `YYYY-MM-DD` |
| `table`, `user_attr` | Полное имя; `src_fotmob` / `platform` / `unknown` |
| `commits` | Целое, сумма commit за сутки |
| `scan_reads`, `scan_writes` | Целые, суммы соответствующих сканов |
| `scans_total` | `scan_reads + scan_writes` |
| `peak_commits_per_min` | Максимум commit по минутам |
| `peak_polls_per_min` | Максимум scan_read по минутам для poll-eligible; иначе 0 |
| `coverage_minutes` | Целое 0…1440, покрытие producer для этой таблицы |
| `gap_intervals` | Массив `{start,end,reason}`, смежные одинаковые причины слиты |
| `completeness` | `full` iff покрыто 1440 минут и интервалов gap нет; иначе `gap` |

Один файл дня имеет `grain=day_report`, `day_utc`, `generated_at`,
`producer_variant`, `poll_interval_s`, `coverage_minutes_total`,
`gap_intervals_total`, `completeness`, `discovered_tables`,
`unparsed_queries`, `unparsed_fotmob`, `n5_measurable`,
`platform_ops_total`, `unknown_ops_total`, `foreign_load`, `tables`,
`platform_activity`, `unknown_activity`, `verdicts`, `pass`, `pass_reasons`.
Список таблиц — объединение discovery за день и фактически разобранных
полных имён. Для каждой таблицы есть ряд `src_fotmob`, даже при нулевой
нагрузке; чужие ряды появляются при наблюдённых операциях. В неполном дне
нулевые суточные суммы означают только наблюдённую сумму, не доказанный покой.

Дополнительные доказательства: `duplicate_queries`, `conflicting_query_ids`,
`peak_evidence`, `commit_checks`, `commit_check_complete`, `commit_mismatch`.
Каждая запись `commit_checks` содержит `table`, `sql_commits`,
`snapshot_commits`, `difference` (snapshots минус SQL), `match`.
Отсутствующее значение snapshots и результат сравнения — null, не ноль.
CLI также сохраняет `commit_check_evidence` с временем проверки/типами
ошибок. Сырые сообщения исключений в артефакты не попадают.

По умолчанию `generated_at` — детерминированная логическая отметка выпуска,
00:05 UTC следующих суток. Фактическое время чтения S7 хранится отдельно в
`commit_check_evidence.checked_at`. При необходимости фактическое время
создания отчёта задаётся `aggregate --generated-at <UTC>`; оно становится
явной частью входа. Один и тот же вход даёт одинаковые байты JSON.
Если в течение дня менялся интервал, `poll_interval_s` содержит наибольший,
а `poll_intervals_s` — полный список наблюдённых интервалов.

## 3. Источник и квалификация

### 3.1. Основной вариант

`query_history_poll` читает `system.runtime.queries`. Доступ, видимость
других пользователей, retention и реальные имена колонок нельзя доказать
офлайн. Снимок выполняется по времени окончания запросов, окно — последние
`2 × poll_interval_s`, по умолчанию 120 секунд. S2 выполняется каждый poll.

### 3.2. Вариант платформы

Event producer — отдельная задача платформы. Он использует CommitReport /
ScanReport и такой же формат минутных/суточных рядов и атрибуции. CommitReport
даёт `commit`, ScanReport — `scan_read`. Для SQL без CommitReport/ScanReport
применяются те же P0–P4. Время — timestamp события; heartbeat и gaps имеют
ту же семантику. Ранняя ротация с потерей событий означает `no_sample`.

Для сравнения вариантов сортировка и формат должны совпасть; технические
идентификаторы producer и polls сравниваются отдельно. Этот комплект не
устанавливает event producer и не утверждает, что event-логи уже доступны.

### 3.3. Квалификация К0–К3; К5 — платформа

Квалификация К0–К3 и передача выполняются **сразу после merge по регламенту**.
`qualify` делает только чтения, выводит JSON с ключами К0–К3 и не выполняет
пробных записей. **Живая К4 запрещена**, в том числе в изолированном контуре.
Её заменяет ежедневная read-only сверка в разделе 5.3.

| Шаг | Доказательство и условие | При провале |
| --- | --- | --- |
| К0 | `DESCRIBE system.runtime.queries`: присутствуют query_id/user/source/query/state/created/started/end/error_code; подключение к `127.0.0.1:8082`, basic auth; клиент импортируется из `/root/.venvs/dpf-test`; сессия UTC | Несовпадение колонок останавливает последующие пробы; ведущий уточняет S1 отдельной правкой, ошибки доступа эскалирует |
| К1 | S6 видит хотя бы одну чужую категорию пользователей; только src_fotmob не доказывает видимость | `n5_measurable=false`, день не PASS, эскалация платформе на вариант (b) |
| К2 | Три S5 за интервал не менее часа, например 0/30/60 минут; минимум фактического возраста ≥ `max(300, 3 × interval)` секунд; во всех пробах count меньше подтверждённого платформой лимита истории | Уточнить retention и лимит; интервал не ниже 15 секунд; при потерях S4 — history_lost, переход к (b) |
| К3 | S1 на пике <5 секунд, ниже порога slow-log источника и отсутствие соответствующей записи подтверждено оператором | Уточнить окно S1; иначе интервал до 120 секунд с новой К2 и изменением расписания платформой |
| К5 | Платформа подтверждает формат CommitReport/ScanReport и ротацию ≥36 часов | Копировать события до ротации; без этого вариант (b) не подтверждён |

`qualify` делает одну пробу за запуск. `--previous-report` переносит предыдущие
две пробы того же интервала и лимита; `--output` атомарно сохраняет новый
локальный отчёт. Это не запись в Trino. Три мгновенных запуска не проходят К2.
`--history-limit` берётся из проверенной платформенной конфигурации, а не
угадывается по S5. Без него К2 не пройдена. `--peak`,
`--slow-log-threshold-s` и `--slow-log-verified` фиксируют проверки ведущего;
программа не имеет доступа к чужим логам и не выполняет их автоматически.

Пример формы команды (значение лимита подставляет ведущий после проверки):

```text
/root/.venvs/dpf-test/bin/python -m scrapers.fotmob.load_meter qualify --history-limit <подтверждённый-лимит> --peak --slow-log-threshold-s <порог-источника> --slow-log-verified --output <локальный-отчёт.json>
```

Следующие пробы добавляют `--previous-report <локальный-отчёт.json>`.
Финальный проверенный отчёт передаётся платформе как `qualification.json`.
Producer разрешает подтверждённое покрытие только при всех пройденных
К0–К3 и совпадающем интервале; отсутствующий/неполный отчёт не даёт PASS.
Первый poll без предыдущего собственного query_id оставляет `no_sample`;
начало доказанного покрытия требует следующего poll с валидным watermark.
Квалификация сама не является стартом серии.

## 4. Read-only SQL и парсер

Литералы времени формирует модуль, сессия использует UTC. Имена целей S7
ограничены формой `iceberg.bronze.fotmob_[a-z0-9_]+`, query ID S4 — буквами,
цифрами и подчёркиванием. Произвольные SQL-фрагменты не интерполируются.

### S1. Snapshot истории

```sql
SELECT query_id, "user", source, query, state, created, started,
       "end" AS ended, error_code
FROM system.runtime.queries
WHERE "end" >= TIMESTAMP '<t_from>'
  AND "end" < TIMESTAMP '<t_to>'
```

### S2. Discovery

```sql
SELECT table_name
FROM iceberg.information_schema.tables
WHERE table_schema = 'bronze'
  AND table_name LIKE 'fotmob\_%' ESCAPE '\'
```

Подчёркивание экранировано: `fotmobX` не относится к источнику. Новая таблица
попадает в discovery следующего poll. Poll-eligible список от discovery не меняется.

### S3. Классификация statement

Это read-only выражение; код применяет эквивалентную классификацию после P0.

```sql
CASE
 WHEN regexp_like(query, '(?i)^\s*INSERT\s+INTO\b') THEN 'insert'
 WHEN regexp_like(query, '(?i)^\s*MERGE\s+INTO\b') THEN 'merge'
 WHEN regexp_like(query, '(?i)^\s*UPDATE\b') THEN 'update'
 WHEN regexp_like(query, '(?i)^\s*DELETE\s+FROM\b') THEN 'delete'
 WHEN regexp_like(query, '(?i)^\s*DROP\s+TABLE\b') THEN 'drop'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b') THEN 'view_write'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b') THEN 'ensure_table'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+TABLE\b')
  AND regexp_like(query, '(?i)\bAS\b') THEN 'ctas'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+TABLE\b') THEN 'table_ddl'
 WHEN regexp_like(query, '(?i)^\s*ALTER\s+TABLE\b') THEN 'alter_write'
 WHEN regexp_like(query, '(?i)^\s*EXPLAIN\s+ANALYZE\b') THEN 'read_exec'
 WHEN regexp_like(query, '(?i)^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN)\b')
  THEN 'read'
 ELSE 'other'
END
```

### S4. Watermark предыдущего poll

```sql
SELECT query_id, state
FROM system.runtime.queries
WHERE query_id = '<prev_poll_query_id>'
```

Отсутствие маячка при возрасте меньше подтверждённого retention —
`history_lost`. Просроченный маячок также не доказывает покрытие. Изменённый
интервал требует новой квалификации. Предыдущий query_id — ID S1, полученный
из cursor клиента, а не ID произвольного наблюдённого запроса.

### S5. Retention, только квалификация

```sql
SELECT min(created) AS oldest, max(created) AS newest, count(*) AS n
FROM system.runtime.queries
```

### S6. Видимость, только квалификация

```sql
SELECT "user", count(*) AS n
FROM system.runtime.queries
GROUP BY "user"
```

Отчёт К1 хранит категории и количества, без произвольных имён пользователей.

### S7. Ежедневная сверка физических коммитов

```sql
SELECT count(*) AS snapshots
FROM iceberg.bronze."<table>$snapshots"
WHERE committed_at >= TIMESTAMP '<начало-UTC-дня>'
  AND committed_at < TIMESTAMP '<начало-следующего-UTC-дня>'
```

`<table>` — короткое имя вроде `fotmob_matches`. Это число snapshots,
добавленных в том же дневном окне, то есть дневное приращение истории
коммитов. Проверяются все таблицы, где наблюдались write-SQL, включая
platform/unknown. Новых snapshots измеритель не создаёт. Нельзя заменять
S7 числом разобранных write-SQL: это уничтожило бы независимость сверки.

### P0–P4. Детерминированный разбор SQL

| Правило | Реализация |
| --- | --- |
| P0 | Удаление `--` и вложенных `/* */` комментариев, непрозрачные строковые литералы с `''`, отдельные quoted identifiers с `""`; нижний регистр идентификаторов |
| P1 | Кандидаты после FROM/JOIN/USING: `(?i)\b(?:FROM\|JOIN\|USING)\s+([\w$\"]+(?:\s*\.\s*[\w$\"]+){0,2})` |
| P2 | Цель после INSERT INTO / MERGE INTO / UPDATE / DELETE FROM / CREATE TABLE / ALTER TABLE / DROP TABLE получает commit; исходный regexp хранится в `P2_PATTERN` модуля |
| P3 | CTE вида WITH name AS (…) и последующие определения через запятую; односоставные ссылки на alias в его области видимости исключаются |
| P4 | Три части — только iceberg.bronze.fotmob_*; две — добавить iceberg; одна — точное сопоставление discovery, без совпадения нет операции; неоднозначность — unparsed |

Токенизатор уточняет regexp-правила: слова в literals/comments и quoted
keywords не создают ссылки; проверяются скобки, границы statement и области
видимости CTE. Нерекурсивный alias не скрывает физическую таблицу внутри
собственного определения. Ссылка на metadata-таблицу `$snapshots` не является
ссылкой на саму bronze-таблицу.

Поддержаны явные JOIN, подзапросы, MERGE USING, INSERT VALUES/SELECT,
UPDATE, DELETE, CTAS и ALTER, включая EXECUTE. Цель UPDATE/DELETE получает
scan_write; остальные источники записи — P1. Одна SQL-строка, обращающаяся
к k различным таблицам, даёт k сканов; повторное упоминание той же таблицы
не удваивается. Plain EXPLAIN относится к read; EXPLAIN ANALYZE принимается
для SELECT/WITH. Multi-statement, CALL, рекурсивные CTE, table functions,
comma joins и неподдержанная грамматика дают `unparsed=true`, без угадывания.
Один завершающий `;` допустим. Счётчик `unparsed_queries` увеличивается;
наличие имени discovered-таблицы в исходном тексте устанавливает
`unparsed_fotmob=true`, в том числе если имя было внутри литерала.

DDL контура разбирается по форме, без угадывания: `CREATE [OR REPLACE] VIEW
имя AS SELECT|WITH …`, `CREATE TABLE IF NOT EXISTS имя (колонки) [WITH (…)]`,
`CREATE TABLE имя (колонки) [WITH (…)]` и `DROP TABLE [IF EXISTS] имя`. Trino
лишь анализирует определение представления и ensure-table no-op, поэтому они
не дают ни скана, ни коммита; создание таблицы и DROP дают commit цели.
Список колонок и тело представления не разбираются как отношения. Любые
другие формы — `CASCADE`, параметры после `WITH`, `DROP VIEW`, `TRUNCATE`,
`CREATE OR REPLACE TABLE`, `CREATE TABLE IF NOT EXISTS … AS SELECT` — дают
`unparsed=true`. Эфемерный стейджинг `fotmob_*__stg_<hex>` исключён из норм
и для этих команд.

Producer никогда не исполняет SQL, который разбирает. S1/S2/S4 исключаются
по содержанию: system/information_schema дают ноль source-сканов, независимо
от пользователя. S7 читает metadata-имя, которое также не входит в нормы.

## 5. Агрегатор и вердикт

### 5.1. Сырые строки

Snapshot — JSON-объект `grain=snapshot` с полями `poll_id`, `query_id`,
`user`, `source`, `query`, `state`, `created`, `started`, `ended`,
`stmt_type`, `error_code`; producer добавляет `observed_at`.
`stmt_type` на входе не считается доказательством: агрегатор повторяет
разбор SQL. Минута = floor UTC от `coalesce(ended, started, created)`.

Heartbeat — `grain=heartbeat`, `poll_id`, `polled_at`, `window_start`,
`window_end`, `watermark_ok`, `n5_measurable`, `discovered_tables`,
`poll_interval_s`, `producer_variant`, `status` (`ok` или `error`).
Producer добавляет `query_id`, `prev_poll_query_id`, `gap_reason`, а при
ошибке — только `error_type`. Окно задаёт **фактически прочитанный S1**.
`polled_at` берётся из `SELECT current_timestamp AS now_utc`, не из местной
зоны хоста. Connection failure без heartbeat также оставляет gap.

Файлы читаются по имени; первый `query_id` побеждает во всём входе,
конфликтные поля отмечаются в `conflicting_query_ids`. Повторы не увеличивают
счётчики. Состояние FAILED не превращается в доказанный нулевой расход:
statement учитывается консервативно; несоответствие commit проверяется S7.

Измерена только **вся минута**, покрытая объединением успешных окон с
`watermark_ok=true`. Частичная минута не считается полной. Перекрывающий
успешный poll может закрыть пробел другого poll. Нет heartbeat —
`producer_down`; heartbeat есть, но окно не покрыло минуту — `no_sample`;
провален watermark — `history_lost`. Никакие нули не закрывают gap.

### 5.2. Суммы, пики и атрибуция

Группировка минут: `(minute_utc, table, user_attr, operation)`; суток:
`(day_utc, table, user_attr)`. Суммы и максимумы не усредняются.
`src_fotmob` → src_fotmob, `airflow` → platform, остальные → unknown.
Расширение `PLATFORM_USERS` и `POLL_ELIGIBLE_TABLES` требует PR.
`platform_ops_total` и `unknown_ops_total` — суммы операций, а не уникальных
query_id: один запрос с commit и scan даёт две операции.

Все измеренные операции platform и unknown сохраняются в отдельных рядах.
При любой такой активности `foreign_load=true`. N5 учитывает каждую операцию
unknown и каждую операцию platform без подтверждённой квитанции из §5.3.

### 5.3. Нормы и ежедневная сверка вместо К4

Нормы различают постоянные таблицы, эфемерный стейджинг и контрольную таблицу
манифеста. Эфемерный стейджинг — имена `fotmob_*__stg_<hex>`: промежуточные
таблицы writer'ов и C1-обновителя, живущие один прогон. Они остаются в учёте
нагрузки (суточные ряды, discovered_tables), но не участвуют в нормах N1–N4 и
в сверке S7: стойкая запись остаётся в целевой таблице (например,
`fotmob_c1_inputs`), и S7 проверяет её. Контрольная таблица
`iceberg.bronze.fotmob_ingest_manifest` — журнал волны и C1: пакетные MERGE
волны дают measured пики до 8 commit/мин, а бакетный EXCEPT ALL-дифф манифеста
у C1 — ограниченный веер опросов (≤64 корзины × 2 направления + 2
discovery-запроса ≤ 130 в минуту); для неё действуют отдельные пороги N2/N4, N3
остаётся общей. Отдельный набор **журнал-таблиц волны** (манифест,
scope-наблюдения, competition_seasons, competition_season_history,
season_stages, squad/team snapshots, transfer_events) получает контрольный
порог N1 ≤8 commit/мин: волна MERGE-ит их по разу на батч скоупа, и каденция
коммитов повторяет ритм батчей (измерено 16–19.11: пики 2–8/мин, сутки ≤167),
тогда как таблицы данных матчей остаются ≤1/мин. Расширение обоих наборов —
отдельным PR.

| Норма | Условие PASS, только src_fotmob кроме N5 |
| --- | --- |
| N1 | commits/мин: ≤1 для каждой постоянной таблицы; ≤8 для журнал-таблиц волны (см. ниже) |
| N2 | polls/мин: ≤1 для `fotmob_squad_snapshots`; ≤130 для `fotmob_ingest_manifest` |
| N3 | commits(table, UTC-day) ≤ 1440 для каждой постоянной таблицы, включая манифест |
| N4 | scans(table, UTC-day) ≤ 1440; ≤5760 для `fotmob_ingest_manifest`; scans = scan_read + scan_write |
| N5 | Число блокирующих операций равно нулю: все unknown и все platform за вычетом подтверждённого обслуживания и регламентных Iceberg-процедур platform |

Poll-eligible только `iceberg.bronze.fotmob_ingest_manifest` и
`iceberg.bronze.fotmob_squad_snapshots`. Ровно порог допустим. `evidence`
указывает худшую таблицу и минуту для пиков; выбор детерминирован: сначала
таблицы, превышающие собственный порог, затем наибольшее значение, при
равенстве — имя таблицы. Метка `threshold` называет применённый предел
(например `<=8/min (control)`).

Регламентные Iceberg-процедуры platform — `ALTER TABLE ... EXECUTE
optimize|expire_snapshots|remove_orphan_files` по fotmob-таблицам — считаются
обслуживанием хранилища и не блокируют N5: их операции попадают в поле
`maintenance_procedure_ops` (только атрибуция platform) и вычитаются из
блокирующих вместе с квитанциями; сводный вычет —
`maintenance_procedure_ops_total`. Квитанция не нужна и не выдаётся на эти
операции; вычет по квитанции того же запроса не суммируется с процедурным.
Прочие операции platform блокируют по-прежнему: интерактивная диагностика,
чужие INSERT/MERGE/UPDATE/DELETE/CTAS, любые иные ALTER. Атрибуция unknown не
освобождается никогда; unparsed SQL не квалифицируется как процедура.

Для вычета по N5 оператор ведёт отдельный реестр
`<root>/maintenance-receipts/YYYY-MM-DD.json`. Допустимы только
`compaction`, `janitor` и `morning_report`. Для каждого запроса оператор
сверяет запись конкретного запуска производителя с оригиналом отправленного
SQL и полученным производителем Trino query ID. В реестр переносятся этот ID,
UTC-день завершения, пользователь, ссылка на исходную запись запуска,
SHA-256 исходного SQL в UTF-8 (включая пробелы и комментарии) и полный набор
пар «каноническая таблица FotMob, операция». Допустимые операции —
`commit`, `scan_write`, `scan_read`.

Шаблон файла для одного запроса (значение хеша требуется заменить):

```json
{
  "day_utc": "2026-11-01",
  "receipts": [{
    "day_utc": "2026-11-01",
    "job": "morning_report",
    "evidence_ref": "producer-runs/report-17/statement-2",
    "query_id": "20261101_060000_00017_example",
    "user": "airflow",
    "sql_sha256": "<64 строчные шестнадцатеричные цифры>",
    "operations": [["iceberg.bronze.fotmob_matches", "scan_read"]]
  }]
}
```

Каталог доступен на запись только доверенному оператору или производителю
доказательств. Оператор публикует проверенный файл атомарной заменой.
Запрещено составлять реестр из raw, событий Trino или суточного отчёта.
Метка `source`, имя DAG, расписание, совпадение времени и одинаковый SQL
не доказывают происхождение. Этот комплект не устанавливает выпуск квитанций
у производителей. Пока нет независимо записанных ID и ссылок, вычет равен нулю.

Обычный `aggregate --root ROOT --day YYYY-MM-DD` сам читает реестр, в том
числе при `--offline`. Затем `verdict` и `line` с теми же root и днём
пересчитывают вычеты из сохранённой суточной записи. После исправления
реестра нужно повторить `aggregate`. Без файла и для старых отчётов N5
равна исходной сумме platform и unknown. При наличии файла каждая квитанция
должна однозначно покрывать один завершённый запрос платформы целиком.
Ошибка формата, повтор ID, конфликт снимков или несовпадение с наблюдением
отклоняет весь реестр. День получает причину `maintenance_evidence_invalid`.

Например, два запроса `airflow` с одинаковым SELECT и разными ID дают две
операции чтения. Квитанция на первый оставляет одну блокирующую операцию.
`platform_ops_total`, `unknown_ops_total`, таблицы по атрибуциям и
`foreign_load` сохраняют всю измеренную нагрузку. В `maintenance_evidence`
остаются реестр и наблюдения с атрибуцией, отпечатком и операциями,
в `maintenance_deductions` — вычтенные пары по ID. Сверка S7 учитывает
также все подтверждённые коммиты обслуживания.

Внутри обычного суточного `aggregate` после разбора сырых данных:

1. Собрать ПОСТОЯННЫЕ таблицы с наблюдённым write-SQL и число SQL-коммитов за
   день (эфемерный `fotmob_*__stg_<hex>` стейджинг исключён: S7 проверяет
   целевую таблицу, а не удалённый стейдж).
2. Под src_fotmob выполнить S7 для каждой такой таблицы за тот же полный
   UTC-день. Сверка включает все атрибуции, не только источник.
3. Сохранить обе величины. Любая абсолютная разница >0 означает
   `commit_mismatch=true` и `pass_reasons` содержит `commit_mismatch`.
4. Отсутствие доступа/таблицы/значения — `commit_check_complete=false`,
   причина `commit_check_missing`. Ошибка не заменяется нулём. При отсутствии
   write-SQL по постоянным таблицам список сверок пуст и дополнительный S7 не
   нужен.

При mismatch числовые N1/N3 остаются видимыми, но не являются достаточным
доказательством коммитов; день не PASS до разбора классификатора ведущим.
Живые пробные записи не выполняются ни в `qualify`, ни в суточном конвейере.

Итоговый PASS требует одновременно N1–N5, 1440 покрытых минут без gaps,
`unparsed_fotmob=false`, `n5_measurable=true`, полной успешной сверки коммитов.
Причины: N1…N5, `gap`, `unparsed_fotmob`, `n5_unmeasurable`,
`commit_check_missing`, `commit_mismatch`, `maintenance_evidence_invalid`. Неизвестный SQL без имени
discovered-таблицы увеличивает счётчик, но сам по себе PASS не блокирует.

Для одной таблицы 1441 commit за 1440 минут математически требует хотя бы
одной минуты с двумя commit. Поэтому синтетический тест N3=1441 одновременно
нарушает N1; невозможный сценарий N3 FAIL при N1 PASS не фабрикуется.

## 6. CLI и офлайн-проверка

В репозитории: `python -m scrapers.fotmob.load_meter <команда>`. При установке
достаточно копии единственного модуля, запускаемой по абсолютному пути.

| Команда | Действие |
| --- | --- |
| `poll` | Один S2/S4/S1, snapshot JSONL + heartbeat; локальный lock, атомарная запись, ротация |
| `discover` | Только S2 и JSON списка таблиц |
| `qualify` | Read-only К0–К3; локальный `--output` необязателен |
| `aggregate --day YYYY-MM-DD` | JSONL → minute/day; живой read-only S7 для write-таблиц |
| `aggregate --day … --offline` | Полностью офлайн; отсутствующие S7 блокируют PASS |
| `aggregate --day … --offline --snapshot-counts PATH` | Офлайн с сохранённой/синтетической сверкой S7 |
| `verdict --day …` | Пересчитать вердикт сохранённого day JSON, записать verdict JSON |
| `line --day …` | Пересчитать вердикт day JSON и вывести одну строку |

Все команды принимают `--root`; aggregate также `--raw-dir`.
Формат `--snapshot-counts`: объект с `day_utc`, `checked_at` не раньше конца
дня и `counts` — mapping полного имени таблицы на неотрицательное целое.
Файл другого дня отвергается. В рабочем cron `--offline` не применяется;
синтетические значения не являются производственным доказательством.

`poll`, `discover` и `qualify` требуют переменную
`TRINO_SRC_FOTMOB_PASSWORD`; её значение не печатается и не логируется.
Дополнительные параметры окружения: `TRINO_HOST`, `TRINO_PORT`,
`TRINO_HTTP_SCHEME`; по умолчанию 127.0.0.1, 8082, http. Пользователь, каталог
и схема фиксированы. `poll --qualification PATH` читает проверенный отчёт;
по умолчанию это `<root>/qualification.json`.

Код возврата aggregate/verdict/line равен 0 при успешном создании результата,
включая FAIL/NOT_PASS: иначе цепочка `&&` потеряла бы строку плохого дня.
Потребитель читает `pass` и `pass_reasons`. Ошибка чтения/формата/записи — 1;
непройденная квалификация — 2. Ошибка S7 создаёт полный отчёт NOT_PASS,
а не обрывает публикацию. Уже сохранённый verdict не подменяет обновлённый day.

Тесты собираются штатным `pytest tests/unit`. Команды для ведущего:

```bash
/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fotmob_load_meter.py -q
/root/.venvs/dpf-test/bin/pytest tests/unit -q
```

Тесты не подключаются к Trino: сеть заменена синтетическим DBAPI, полный
день создаётся генератором, проверяются N1–N5, границы, gap-причины,
commit_mismatch/missing, дедупликация, P0–P4, foreign attribution, CLI,
ротация и импорт в пустом окружении с запрещённым импортом trino.
Наличие этих тестов в комплекте не утверждает, что они уже выполнены.

## 7. Установка платформой

Каталог: `/root/watchdog/fotmob_meter/{raw,minute,day,verdict}/`.
Платформа копирует модуль как `/root/watchdog/fotmob_meter/load_meter.py`,
проверенную квалификацию — `qualification.json`. Raw-файлы:
`YYYYMMDD-HHMMSS-<query_id>.jsonl`; локальный watermark —
`raw/producer-state.json`; дневные файлы — `YYYY-MM-DD.json[l]`.
`poll` удаляет только собственные именованные артефакты старше 35 суток,
не трогает посторонние файлы и symlink. `.poll.lock` исключает параллельные
poll одной установки. Каталог и raw SQL доступны только ответственным
операторам; raw не отправляется провайдерам моделей.

Ниже содержимое предлагаемого `load_meter_run.sh`. Оно не установлено этим
PR. Секрет поступает от существующего механизма окружения платформы;
не помещать его в crontab, аргументы, отчёт или shell trace. Обёртка
передаёт разрешённые переменные через анонимный fd, затем выполняет `env -i`:
значение пароля не появляется в argv `env`.

```sh
#!/bin/sh
set -eu
cd /root/watchdog/fotmob_meter
exec /root/.venvs/dpf-test/bin/python - "$@" <<'PY'
import json
import os
import sys

allowed = ("TRINO_SRC_FOTMOB_PASSWORD", "TRINO_HOST", "TRINO_PORT", "TRINO_HTTP_SCHEME")
clean = {name: os.environ[name] for name in allowed if name in os.environ}
clean.update(TZ="UTC", PYTHONDONTWRITEBYTECODE="1", PATH="/usr/bin:/bin")
payload = json.dumps(clean).encode("utf-8")
if len(payload) > 4096:
    raise SystemExit("meter environment exceeds fd payload limit")
read_fd, write_fd = os.pipe()
os.set_inheritable(read_fd, True)
os.write(write_fd, payload)
os.close(write_fd)
bootstrap = """
import json, os, runpy, sys
with os.fdopen(int(sys.argv[1]), encoding='utf-8') as source:
    os.environ.update(json.load(source))
sys.argv = ['/root/watchdog/fotmob_meter/load_meter.py', *sys.argv[2:]]
runpy.run_path(sys.argv[0], run_name='__main__')
"""
os.execv("/usr/bin/env", ["env", "-i", "/root/.venvs/dpf-test/bin/python", "-c", bootstrap, str(read_fd), *sys.argv[1:]])
PY
```

Платформа проверяет доставку окружения секретов в cron своим существующим
механизмом. Эта обёртка пароль не создаёт и не извлекает. Не включать `set -x`.
Пакет не добавляет зависимостей; в выбранном venv уже должен быть python-trino.

Предлагаемые записи для интервала 60 секунд, после проверки UTC-расписания
конкретного cron платформой:

```cron
CRON_TZ=UTC
* * * * * /root/watchdog/fotmob_meter/load_meter_run.sh poll > /dev/null
5 0 * * * d=$(/usr/bin/date -u -d yesterday +\%F); /root/watchdog/fotmob_meter/load_meter_run.sh aggregate --day "$d" > /dev/null && /root/watchdog/fotmob_meter/load_meter_run.sh verdict --day "$d" > /dev/null && /root/watchdog/fotmob_meter/load_meter_run.sh line --day "$d" > /root/watchdog/fotmob_meter/line.txt
```

`TZ=UTC` процесса не заменяет проверку timezone планировщика. Изменение
интервала на 15/30/120 секунд требует согласованного расписания платформы
и новой квалификации; минутный cron не начинает автоматически опрашивать
чаще. Рабочие query timestamps и границы S1/S7 берутся в UTC.

При передаче ведущий прикладывает точные версии/хеши шести файлов, результаты
офлайн-проверок и К0–К3, шаблон и временный synthetic-day комплект с S7.
Платформа проверяет офлайн aggregate/verdict/line во временном каталоге,
затем живые raw + heartbeat и начало полного покрытия. До этих доказательств
серия не стартовала. Ведущий сохраняет отдельную приёмку и передаёт её
по действующей процедуре; этот модуль сам журнал приёмки не меняет.

Для каждого из трёх последовательных полных UTC-дней ведущий прикладывает
суточный технический PASS и отдельные эксплуатационные доказательства штатной
нагрузки FotMob. На дату дня нужны ссылки на фактические запуски и их
результаты (например, run ID штатного инкремента и соответствующую запись
manifest). Ведущий сопоставляет эти факты с действующим режимом инкремента,
history и C1. Один технический PASS возможен и в пустые сутки.
Числового минимума для подтверждения штатной нагрузки нет. Совпадение окон
запусков, продвижение history и успешность C1 не входят в расчёт N1–N5.

## 8. Строка сводки, сроки и fallback

`line` сохраняет фиксированный порядок ключей, включая плохие дни:

```text
Trino fotmob: day=2026-11-01 verdict=PASS coverage=1440/1440 tables=2 user=src_fotmob commits_day_max=720 scans_day_max=1020 peak_commit_min=1 peak_poll_min=1 platform_ops=0 unknown_ops=0 unparsed=0 gaps=0
Trino fotmob: day=2026-11-01 verdict=FAIL(N1,N5) coverage=1440/1440 tables=2 user=src_fotmob commits_day_max=720 scans_day_max=1020 peak_commit_min=2 peak_poll_min=1 platform_ops=2 unknown_ops=0 allowed_platform_ops=1 blocking_ops=1 unparsed=0 gaps=0
Trino fotmob: day=2026-11-01 verdict=NOT_PASS(gap) coverage=1410/1440 tables=2 user=src_fotmob commits_day_max=0 scans_day_max=0 peak_commit_min=0 peak_poll_min=0 platform_ops=0 unknown_ops=0 unparsed=0 gaps=1(producer_down:02:10-02:40)
```

Суточная граница в описании gap может быть `24:00`. FAIL означает нарушение
хотя бы одной N-нормы; NOT_PASS — недоказанную полноту/видимость/сверку без
измеренного нарушения N1–N5. При сочетании причин перечисляются все.
Файл строки передаётся утренней сводке по регламенту платформы.

Квалификация К0–К3 и передача: **сразу после merge по регламенту**.
Установка: **срок согласует ведущий с платформенной сессией после merge**.
Первый полный UTC-день и три дня серии определяются по реальным измерениям,
не по заранее объявленной календарной дате.

Если к согласованному сроку нет установки/ответа платформы, ведущий
проверяет условия предусмотренного fallback: самостоятельный host-cron
варианта (a) под src_fotmob допустим только с пройденными К0–К3, в частности
К1 и К2, и в ранее разрешённом регламентом объёме. Это не действие
офлайн-исполнителя. Владелец получает сообщение об ограничениях in-memory
истории и наблюдаемой видимости. При провале К1/К2 — эскалация владельцу,
нужен платформенный вариант (b); серия и Silver не стартуют. Нет тихого
перехода от `n5_measurable=false` к PASS.

## 9. Риски и действия

| Риск | Проверка | Действие |
| --- | --- | --- |
| Нет видимости чужих запросов | К1 / S6 | N5 недоказуема, не PASS, запрос варианта (b) платформе |
| Retention меньше окна или упёрся в count limit | К2 / S5, каждый S4 | Пересогласовать интервал ≥15 секунд; history_lost не закрывать нулём |
| S1 грузит координатор | К3 на пике и slow-log | Уточнить окно; до 120 секунд с повторной К2 и подходящим расписанием |
| Один write-SQL не равен одному snapshot | Ежедневный S7 внутри aggregate | `commit_mismatch`, обе величины в отчёте; N1/N3 недостаточно для PASS, разбор ведущим; пробные записи запрещены |
| S7 недоступен или snapshots успели expire | `commit_check_missing` / mismatch | День не PASS; обеспечить доступ и сохранность metadata-истории на время дневной сверки |
| SQL вне P0–P4 | unparsed counters | Имя discovered-таблицы блокирует PASS; расширение грамматики отдельным PR |
| События сервера ротируются раньше 36 часов | К5 платформы | Скопировать до ротации; иначе вариант (b) не доказан |
| Регулярная platform-компакция (#1892) | platform_activity, N5 | Сохраняется в platform_activity; Iceberg-процедуры optimize/expire_snapshots/remove_orphan_files от platform вычитаются как регламентное обслуживание; прочие операции — по квитанции на каждый запрос |
| Часы/расписание не UTC | К0 и проверка cron платформой | Границы producer по часам Trino в UTC, raw без явного UTC отвергать |
| Новая/переименованная таблица | S2 каждый poll | Динамический discovery; poll-eligible меняется только PR |
| Потеря raw/heartbeat, диск, сбой producer | Карта покрытия | producer_down; восстановление не ретуширует исторический gap |
| Повтор или конфликт query_id | Дедупликация первого, список конфликтов | Сохранить первое и конфликт для разбора, не считать повтор второй раз |
| Несовпадение layout истории | К0 DESCRIBE | Не угадывать колонки; исправление и повторная квалификация |
| Утечка пароля | Имя переменной, fd-передача, очищенное окружение | Не печатать исключения/headers, не включать shell trace, не передавать значение в argv |

## 10. Готовность к публикации и приёмке

Ведущий проверяет точный diff шести файлов, UTF-8, границы пакета,
read-only характер выполняемого SQL, отсутствие новых зависимостей и
секретов, затем запускает затронутые и полный unit suite и независимое
ревью по регламенту. Офлайн-комплект не доказывает layout, доступ, retention,
видимость, slow-log, реальную доставку или три измеренных дня. Эти факты
оформляются после merge по процедуре квалификации и передачи приёмки.
