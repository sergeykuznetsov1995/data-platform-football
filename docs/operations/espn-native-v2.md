# ESPN Native Bronze v2: runbook

Этот runbook относится только к ESPN Bronze. Legacy-таблицы остаются
read-only запасным путём: миграция не удаляет legacy и rollback всегда
append-only. Silver и Gold этим процессом не меняются.

## Перед запуском

Работать из проверенного release commit. Сначала сохранить SHA commit,
`configs/espn/registry.yaml`, входных evidence и всех полученных JSON-отчётов.
Не использовать URI с `latest`: только неизменяемые URI и SHA-256. Проверить,
что DAG-и импортируются, Airflow pool для ESPN существует, а секреты Trino,
S3 и control DB доступны только runtime. Одновременно разрешён один оператор
cutover для одного scope.

## Discovery и явное promotion registry

1. Запустить `dag_discover_espn_registry` и дождаться зелёных
   `fetch_discovery_catalog` и `write_reviewable_diff`.
2. Скачать immutable discovery snapshot и review diff. Сверить ESPN ID, slug,
   мужской пол, age class, edition, даты и три capability. Discovery никогда
   сам не меняет registry.
3. В отдельном reviewed PR применить `scrapers.espn.registry.promote_candidate`
   к точному candidate и `configs/espn/registry.yaml`. Сохранить evidence для
   gender/age class, затем проверить `validate_registry_document` и SHA-256.
4. Capability `absent` означает, что соответствующая native сущность может
   быть пустой; `unknown` не даёт права на автоматическое включение.

## Canary и три зелёных запуска

Сначала выбрать ровно один `scope_id` вида `<espn_id>:<source_year>` и запустить
его через `dag_ingest_espn`. Canary считается зелёным, только если capture,
offline parse, DQ, COMPLETE manifest, publication evidence и health artifact
связаны с одинаковыми registry/plan/generation signatures и точными URI/SHA.
Последняя задача должна записать `espn-run-success-receipt-v1`: он появляется
только после зелёного published DQ, пустого списка alerts и успешного release
lease. Одного durable manifest недостаточно — он создаётся раньше этих проверок.

Перед cutover нужны **три последовательных зелёных** запуска
`dag_ingest_espn` для того же scope и registry signature. В promotion evidence
положить exact registry snapshot URI/SHA, а для каждого запуска — durable/raw,
published-DQ, terminal-verdict, health, lease-release и final success receipt
URI/SHA. Три master data interval должны соприкасаться без пропуска; третья
COMPLETE generation становится кандидатом. Ошибка, warning, stale manifest,
пропущенный день или другой registry signature обнуляют серию.

Сначала выполнить план — команда по умолчанию dry-run и не открывает БД:

```bash
python scripts/migrate_espn_native_v2.py promote \
  --evidence /durable/espn/700-2026-promotion-evidence.json \
  --output /durable/espn/700-2026-promotion-plan.json
```

Проверить `mutates=false`, один scope, три exact green-run refs, создание V2
objects/current views, baseline и конкретную rollback command. Только после
peer review применить тот же файл:

```bash
python scripts/migrate_espn_native_v2.py promote \
  --evidence /durable/espn/700-2026-promotion-evidence.json \
  --output /durable/espn/700-2026-promotion-result.json \
  --apply
```

Успех — `status=promoted`, valid `result_sha256`, baseline всех трёх legacy
таблиц со snapshot IDs из Iceberg `main` refs и метриками, прочитанными через
`FOR VERSION AS OF` этих exact snapshots, и один native cutover, привязанный к
физически перепроверенной COMPLETE generation. Проверить current views
запросами только для этого scope.
Повтор команды должен вернуть тот же результат, а не второй переход.

## Replay

Для расследования взять `replay_raw_manifest_uri` и SHA из promotion result или
failed-run evidence. Запустить `dag_replay_espn` с exact signed replay plan и
raw manifest. Replay не ходит в ESPN: он читает только сохранённые raw bytes,
повторно проверяет SHA и публикует новую immutable generation. Никогда не
перезаписывать исходный raw manifest.

## Repair

Seed находится в `configs/espn/repair_seed.yaml`. Для полной матрицы Top-5
сезонов `1617`–`2526` сначала получить read-only input прямо из exact Iceberg
snapshots (ручной JSON не является evidence), затем выполнить validator:

```bash
python scripts/extract_espn_repair_audit.py \
  --output /durable/espn/top5-audit-input.json
```

Затем:

```bash
python scripts/audit_espn_repair.py \
  --input /durable/espn/top5-audit-input.json \
  --output /durable/espn/top5-repair-queue.json
```

Проверить все 50 scopes и причины date, duplicate, final-score и Summary
coverage. Missing scope тоже обязан попасть в очередь. Запускать выбранные
строки через `dag_repair_espn`, сохраняя failure/repair evidence. Данные до
2016 имеют метку `legacy_untrusted`: их можно исследовать и replay, но нельзя
автоматически переводить в trusted cutover.

## Rollback

При regression, stale data или hard alert взять неизменённый promotion result
и сначала построить план без `--apply`:

```bash
python scripts/migrate_espn_native_v2.py rollback \
  --promotion-report /durable/espn/700-2026-promotion-result.json \
  --reason 'canary-regression-INC-123' \
  --output /durable/espn/700-2026-rollback-plan.json
```

После review повторить с `--apply`. Rollback добавляет legacy successor,
проверяет predecessor hash и ancestry, но не меняет и не удаляет прошлые
cutovers, native generations или legacy objects. Затем проверить current views
для одного scope и запустить `dag_monitor_espn`.

После repair разрешено повторное promotion. Оно требует новую тройку зелёных
дней и новый `cutover_id` вида
`espn-native-<espn_id>-<year>-repromote-<первые 16 символов rollback SHA>`.
Новый native cutover обязан назвать rollback predecessor и продолжить ancestry;
старый root ID повторно использовать нельзя. Новый promotion result снова
служит единственным входом для следующего rollback.

## Alerts и incident response

`dag_monitor_espn` следит за 36-часовой свежестью. Health-задачи также
поднимают hard alerts для исчерпанного proxy budget, schema drift,
publication/cutover conflict, unpromoted current season и failed DQ. Warning не
разрешает cutover. Hard alert сохраняется как identity-bound JSON до падения
задачи. Оператор сохраняет alert SHA, ставит затронутый scope на паузу,
выполняет rollback при риске для current views и использует replay/repair для
диагностики. Не перезапускать capture вслепую.

## Retention

- Raw успешного COMPLETE запуска хранить минимум **90 дней**.
- Raw и evidence failed/repair запуска хранить минимум **365 дней**.
- Raw manifests, generation manifests, cutovers, baselines, health/alert и
  promotion/rollback отчёты хранить **бессрочно**.

Cleanup обязан сначала доказать terminal state и retention class. Cutover или
manifest без доступного exact replay evidence считается incident, а не
кандидатом на очистку.

## Release gates

До production apply сохранить точный вывод:

```bash
/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers tests/unit/dags -q
/root/.venvs/dpf-test/bin/pytest tests/unit -q
/root/.venvs/dpf-test/bin/pytest tests/unit/scripts/test_audit_bronze_columns.py -q
/root/.venvs/dpf-test/bin/pytest tests/unit/dags/test_dag_espn_native_v2.py -q
python scripts/audit_espn_runtime_imports.py
```

Любой non-zero gate запрещает cutover. Эти offline проверки не заменяют
отдельные live preflight, canary observation и peer review production evidence.
