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
   `fetch_discovery_catalog` и `publish_discovered_male_registry`.
2. Скачать immutable discovery snapshot и review diff. Сверить ESPN ID, slug,
   мужской пол, age class, edition, даты и три capability. Discovery никогда
   сам не меняет registry.
3. В отдельном reviewed PR применить `scrapers.espn.registry.promote_candidate`
   к точному candidate и `configs/espn/registry.yaml`. Сохранить evidence для
   gender/age class, затем проверить `validate_registry_document` и SHA-256.
4. Capability `absent` означает, что соответствующая native сущность может
   быть пустой. `CapabilityState.UNKNOWN` допускается для явно MALE кандидата,
   но не доказывает отсутствие сущности; `Gender.UNKNOWN` не даёт права на
   автоматическое включение.

## Automatic all-male rollout

Эта процедура управляет automatic all-male registry, а не ручным списком
соревнований. Политика selection должна быть ровно
`explicit-core-gender-MALE-v1`. Датированный rollout baseline:
`2026-08-02: 181 MALE / 38 FEMALE / 1 UNKNOWN`. FEMALE и UNKNOWN —
исключения; в generated registry допускаются только явно подтверждённые MALE
записи. Любой другой count, duplicate ID/slug или malformed explicit-MALE
record останавливает rollout.

Сохранять нужно immutable `male_registry_ref` (`uri` и SHA-256), candidate,
review и `discovery_state_ref`, который возвращает успешный
`publish_discovered_male_registry`. Его URI имеет run-scoped вид
`<ESPN_ARTIFACT_ROOT_URI>/discovery/<run-key>/discovery-state.json`.
`latest-state.json`
является лишь mutable discovery pointer: `latest-state.json никогда не является
registry запуска`. Он может помочь найти state, но admission, bootstrap,
canary и rollback используют frozen immutable discovery-state ref, который
сам связывает `male_registry_ref`. Reducer сначала публикует immutable state,
а затем копирует в alias те же canonical bytes; более новый weekly run меняет
только alias и не делает старый run-scoped ref нечитаемым.

Выполнять шаги строго в этом порядке.

1. Поставить на паузу daily owner и discovery; не запускать daily owner вручную:

   ```bash
   airflow dags pause dag_trigger_espn_daily
   airflow dags pause dag_discover_espn_registry
   ```

2. Deploy reviewed release. Ручной run поставленного на паузу DAG
   остаётся `queued`, поэтому для одного exact weekly discovery run
   временно снять паузу и задать явный run ID:

   ```bash
   export DISCOVERY_RUN_ID='all-male-rollout-<UTC-timestamp>'
   airflow dags unpause dag_discover_espn_registry
   airflow dags trigger dag_discover_espn_registry --run-id "$DISCOVERY_RUN_ID"
   ```

   Дождаться terminal success именно `$DISCOVERY_RUN_ID` и его
   `publish_discovered_male_registry`, затем сразу вернуть discovery
   на паузу до финальной активации:

   ```bash
   airflow dags pause dag_discover_espn_registry
   ```

   Сохранить возвращённый
   immutable `discovery_state_ref`,
   SHA-256, registry signature и count. Подтвердить baseline `181/38/1`, exact
   Core coverage и zero duplicate IDs/slugs. Сохранить immutable state ref и
   `male_registry_ref` из этого run; state ref — frozen input, связывающий
   exact generated registry. Настроить оба значения в shared Compose
   environment для `airflow-init`, `airflow-scheduler` и `airflow-webserver`;
   LocalExecutor task subprocesses наследуют их от scheduler. Затем
   recreate/restart этих Airflow services до bootstrap; неполная пара запрещена:

   ```bash
   export ESPN_DISCOVERY_STATE_REF_URI='s3://.../discovery/<run-key>/discovery-state.json'
   export ESPN_DISCOVERY_STATE_REF_SHA256='...lowercase-64-hex...'
   # compose.yaml omits both keys when unset and propagates both exact values here
   ```

   С включённой парой admission читает только этот exact state ref и никогда
   не fallback-ит к mutable pointer. Frozen ref не имеет 8-дневного срока и
   остаётся valid, пока обе переменные атомарно не удалены или не заменены
   новой полной парой; неполная замена fail-closed. Не менять пару до окончания
   rollout или rollback.

3. Запускать bounded bootstrap через `dag_backfill_espn`, не через daily
   owner. Перед первым cohort снять паузу только с manual
   `dag_backfill_espn`; его `schedule=None`, и он остаётся unpaused только
   до завершения all-scope canary:

   ```bash
   airflow dags unpause dag_backfill_espn
   ```

   После каждой exact coverage reconciliation вычислить deterministic
   `sorted(target - COMPLETE)` и передать только первые 10 отсутствующих scope
   как явный `scopes` (explicit <=10 cohort); это ten-scope bootstrap rule, а
   не разрешение расширить map/lease limit. Дождаться terminal success и
   release leases перед следующим cohort.

   ```bash
   COHORT_CONF='{"scopes":["<first-missing-scope>","..."]}'
   airflow dags trigger dag_backfill_espn --conf "$COHORT_CONF"
   ```

   Не продолжать, если admission не закрепил state ref, связанный
   `male_registry_ref` и registry signature, либо если любой selected scope
   не опубликовал exact COMPLETE head.

4. Выполнить exact coverage reconciliation. В environment с теми же
   `ESPN_ARTIFACT_ROOT_URI`, control-DB и credentials положить сохранённые
   immutable URI/SHA в переменные и выполнить команду ниже. Она доказывает
   `discovered MALE target == generated registry target == union of COMPLETE
   heads` и явно требует zero duplicates.

   ```bash
   python - <<'PY'
   from collections import Counter
   import json
   import os
   from dags.utils.espn_native_tasks import _read_ref, _verified_complete_head
   from scrapers.espn.discovery import CatalogSnapshot
   from scrapers.espn.operations import PostgresEspnControlStore
   from scrapers.espn.registry import Gender, validate_registry_document

   state_ref = {
       "uri": os.environ["ESPN_DISCOVERY_STATE_REF_URI"],
       "sha256": os.environ["ESPN_DISCOVERY_STATE_REF_SHA256"],
   }
   state = _read_ref(state_ref, kind="espn-discovery-state-v2")
   candidate_ref = state["candidate_ref"]
   registry_ref = state["male_registry_ref"]
   candidate = CatalogSnapshot.from_dict(_read_ref(candidate_ref))
   registry = validate_registry_document(_read_ref(registry_ref))
   candidate_ids = [item.espn_id for item in candidate.candidates]
   candidate_slugs = [item.slug for item in candidate.candidates]
   assert len(candidate_ids) == len(set(candidate_ids)), "duplicate candidate IDs"
   assert len(candidate_slugs) == len(set(candidate_slugs)), "duplicate candidate slugs"
   gender_counts = Counter(item.gender.value for item in candidate.candidates)
   assert gender_counts == {"MALE": 181, "FEMALE": 38, "UNKNOWN": 1}, gender_counts
   discovered_male_scope_ids = {
       f"{item.espn_id}:{item.source_season_year}"
       for item in candidate.candidates
       if item.gender is Gender.MALE
   }
   generated_scope_ids = {
       competition.scope_id(competition.current_edition)
       for competition in registry.promoted
   }
   assert len(discovered_male_scope_ids) == len(generated_scope_ids), "count drift"
   assert len(registry.by_id) == len(registry.competitions), "duplicate IDs"
   assert len(registry.by_slug) == len(registry.competitions), "duplicate slugs"
   store = PostgresEspnControlStore.from_env()
   with store._connect() as connection:
       with connection.cursor() as cursor:
           cursor.execute(
               f"SELECT scope_id FROM {store.HEAD_TABLE} "
               "WHERE registry_signature = %s",
               (registry.signature(),),
           )
           head_scope_ids = {row[0] for row in cursor.fetchall()}
   complete_scope_ids = {
       scope_id
       for scope_id, head in store.read_scope_heads(head_scope_ids).items()
       if _verified_complete_head(head)[1] == "complete"
   }
   assert discovered_male_scope_ids == generated_scope_ids, {
       "discovered_minus_generated": sorted(
           discovered_male_scope_ids - generated_scope_ids
       ),
       "generated_minus_discovered": sorted(
           generated_scope_ids - discovered_male_scope_ids
       ),
   }
   missing = sorted(generated_scope_ids - complete_scope_ids)
   extra = sorted(complete_scope_ids - generated_scope_ids)
   assert not extra, {"complete_minus_generated": extra}
   print(
       "exact coverage reconciliation: "
       f"{len(missing)} missing, 0 extra, 0 duplicate IDs/slugs"
   )
   print(json.dumps({"scopes": missing[:10]}))
   PY
   ```

   В handoff/evidence записать результат как
   `target_scope_ids - COMPLETE scope_head_v2 = empty`; не подменять exact set
   одним count. Пока разность не пуста, взять последнюю JSON-строку как
   `COHORT_CONF`, запустить ровно этот `dag_backfill_espn` cohort и повторить
   reconciliation. Если любой head не проходит physical COMPLETE verification,
   сначала выполнить repair/backfill его exact scope; простое наличие строки
   `scope_head_v2` не является COMPLETE evidence.

5. После пустой разности запустить один manual all-scope canary через
   `dag_backfill_espn`, передав в `scopes` exact frozen target array из
   saved state ref/`male_registry_ref`; zero-row scope считается успешным лишь
   с exact signed manifest/checkpoint evidence, а не из-за отсутствия mapped
   output. Summary reuse также требует exact signed prior evidence. Затем
   подтвердить zero active leases и zero alerts. Только после этого
   вернуть manual backfill на паузу, оставить repair/replay на паузе,
   снять паузу с child ingest и monitoring, затем с discovery и лишь
   после них — с daily owner. Этот порядок обязателен после
   init/recreate, когда все ESPN DAG-и могли быть поставлены на паузу:

   ```bash
   airflow dags pause dag_backfill_espn
   airflow dags pause dag_repair_espn
   airflow dags pause dag_replay_espn
   airflow dags unpause dag_ingest_espn
   airflow dags unpause dag_monitor_espn
   airflow dags unpause dag_discover_espn_registry
   airflow dags unpause dag_trigger_espn_daily
   ```

   Из-за нового registry signature обязательны три новых scheduled green
   parent/child runs. Manual bootstrap/canary не засчитываются в эти три.

### Rollback expanded registry

При regression или hard alert немедленно поставить owner на паузу. Сохранить
immutable raw/generation/reconciliation evidence; ничего из него не удалять.
Затем восстановить last reviewed release и **freeze the last good immutable
discovery-state ref, binding the last good `male_registry_ref`** в rollback
record/admission evidence. Установить сохранённые
`ESPN_DISCOVERY_STATE_REF_URI` и `ESPN_DISCOVERY_STATE_REF_SHA256` во всех
Airflow компонентах и recreate их. Не выбирать текущий `latest-state.json` и
не генерировать новый registry во время rollback: last good frozen state ref
и его `male_registry_ref` остаются неизменяемым target до отдельного reviewed
rollout.

```bash
airflow dags pause dag_trigger_espn_daily
airflow dags pause dag_discover_espn_registry
# deploy last reviewed release; retain immutable raw and generation artifacts
```

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
