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

Этот rollout выполняется только в отдельном ESPN Airflow stack из
versioned `deploy/espn/airflow.compose.yaml`, а не в shared `compose.yaml`.
Compose жёстко задаёт `ESPN_ISOLATED_STACK=1`; fresh DagBag projection
содержит ровно семь ESPN root DAG-ов и не содержит
`dag_master_pipeline`. Создать новый, не переиспользуемый projection
из exact immutable release и задать compose wrapper:

```bash
export ESPN_RELEASE_ROOT='/absolute/immutable/espn-release-<git-sha>'
export ESPN_DAGBAG_ROOT='/absolute/new/espn-dagbag-<git-sha>'
export ESPN_ENV_FILE='/protected/path/espn.env'
export ESPN_RELEASE_COMMIT='<exact-40-hex-release-commit>'
export ESPN_RELEASE_TREE_SHA256='<sha256-of-reviewed-release-tree>'
python scripts/build_espn_dagbag_projection.py \
  --release-root "$ESPN_RELEASE_ROOT" \
  --output "$ESPN_DAGBAG_ROOT"
espn_compose() {
  docker compose --env-file "$ESPN_ENV_FILE" --project-name espn-airflow \
    -f deploy/espn/airflow.compose.yaml "$@"
}
espn_airflow() {
  espn_compose exec -T airflow-scheduler airflow "$@"
}
espn_compose config --quiet
```

Protected env file обязан задать dedicated metadata
`ESPN_AIRFLOW_DATABASE_URL` с URL-encoded password и отдельный raw
`ESPN_AIRFLOW_DB_PASSWORD` для Postgres container. Оба относятся
только к dedicated `airflow-metadb`. Отдельный
`ESPN_CONTROL_DATABASE_URL` обязан указывать на shared ESPN control
DB для lease/rate/publication fences; metadata и control DSN не могут
быть одинаковыми. Перед любой migration/startup `airflow-init`
запускает `scripts/verify_espn_database_topology.py`: preflight
реально открывает оба DSN и доказывает разный connected server/database identity
по `server address + port + current_database()`. Два разных hostname,
которые ведут в одну БД, fail-closed останавливают init.

До первого шага сохранить cross-stack evidence, что shared
`dag_master_pipeline` остаётся paused и не имеет active run. Если shared
master должен быть scheduled, этот rollout заблокирован до
отдельного reviewed release, удаляющего его ESPN trigger. Не допускать
два daily owner в разных metadata DB.

1. Если isolated project уже работает, до deploy поставить на
   паузу daily owner и discovery; не запускать daily owner вручную.
   Если это первый deploy и scheduler ещё не существует, команды
   `exec` не выполнять: `DAGS_ARE_PAUSED_AT_CREATION=true` и init
   поставят все семь DAG-ов на паузу, а шаг 2 повторит
   явные pause после health/topology checks:

   ```bash
   espn_airflow dags pause dag_trigger_espn_daily
   espn_airflow dags pause dag_discover_espn_registry
   ```

   После pause и до любого `--force-recreate` fail-closed доказать
   `zero isolated active DagRuns` для всех семи isolated DAG-ов. Проверка
   выполняется one-off container через dedicated metadata DB, поэтому она
   обязательна и для первого deploy; отсутствующая/неинициализированная DB,
   ошибка запроса или хотя бы один `queued`/`running` DagRun блокируют deploy:

   ```bash
   espn_compose run --rm --no-deps airflow-scheduler python - <<'PY'
   from airflow.models import DagRun
   from airflow.utils.session import create_session

   isolated_dag_ids = {
       "dag_ingest_espn",
       "dag_repair_espn",
       "dag_backfill_espn",
       "dag_replay_espn",
       "dag_discover_espn_registry",
       "dag_monitor_espn",
       "dag_trigger_espn_daily",
   }
   with create_session() as session:
       active_runs = (
           session.query(DagRun.dag_id, DagRun.run_id, DagRun.state)
           .filter(
               DagRun.dag_id.in_(isolated_dag_ids),
               DagRun.state.in_(("queued", "running")),
           )
           .all()
       )
   if active_runs:
       raise RuntimeError(f"isolated active DagRuns block deploy: {sorted(active_runs)}")
   PY
   ```

2. Deploy reviewed release через isolated Compose и принудительно
   пересоздать Airflow services. Обычный restart не меняет
   environment и запрещён. До discovery доказать exact isolated role
   в фактическом scheduler container и проверить, что `airflow dags list`
   содержит ровно семь reviewed ESPN DAG-ов, включая owner, и не
   содержит `dag_master_pipeline`:

   ```bash
   espn_compose --profile ui up -d --wait --wait-timeout 180 --force-recreate \
     airflow-init airflow-scheduler airflow-webserver
   espn_compose exec -T airflow-scheduler airflow jobs check \
     --job-type SchedulerJob
   test "$(espn_compose exec -T airflow-scheduler \
     printenv ESPN_ISOLATED_STACK)" = '1'
   espn_compose exec -T airflow-scheduler python - <<'PY'
   from airflow.models import DagBag

   expected_dag_ids = {
       "dag_ingest_espn",
       "dag_repair_espn",
       "dag_backfill_espn",
       "dag_replay_espn",
       "dag_discover_espn_registry",
       "dag_monitor_espn",
       "dag_trigger_espn_daily",
   }
   bag = DagBag(
       dag_folder="/opt/airflow/dags",
       include_examples=False,
       safe_mode=True,
   )
   assert bag.import_errors == {}, bag.import_errors
   assert set(bag.dags) == expected_dag_ids, sorted(bag.dags)
   assert "dag_master_pipeline" not in bag.dags
   PY
   espn_airflow dags pause dag_trigger_espn_daily
   espn_airflow dags pause dag_discover_espn_registry
   ```

   Ручной run поставленного на паузу DAG
   остаётся `queued`, поэтому для одного exact weekly discovery run
   временно снять паузу и задать явный run ID:

   ```bash
   export DISCOVERY_RUN_ID='all-male-rollout-<UTC-timestamp>'
   espn_airflow dags unpause dag_discover_espn_registry
   espn_airflow dags trigger dag_discover_espn_registry --run-id "$DISCOVERY_RUN_ID"
   ```

   Дождаться terminal success именно `$DISCOVERY_RUN_ID` и его
   `publish_discovered_male_registry`, затем сразу вернуть discovery
   на паузу до финальной активации:

   ```bash
   espn_airflow dags pause dag_discover_espn_registry
   ```

   Сохранить возвращённый
   immutable `discovery_state_ref`,
   SHA-256, registry signature и count. Подтвердить baseline `181/38/1`, exact
   Core coverage и zero duplicate IDs/slugs. Сохранить immutable state ref и
   `male_registry_ref` из этого run; state ref — frozen input, связывающий
   exact generated registry. Настроить оба значения в environment
   isolated Compose для `airflow-init`, `airflow-scheduler` и
   `airflow-webserver`; LocalExecutor task subprocesses наследуют их от
   scheduler. Неполная пара запрещена. После export обязательно
   выполнить force-recreate, затем до bootstrap сравнить оба
   фактических container values с exact saved values:

   ```bash
   export ESPN_DISCOVERY_STATE_REF_URI='s3://.../discovery/<run-key>/discovery-state.json'
   export ESPN_DISCOVERY_STATE_REF_SHA256='...lowercase-64-hex...'
   espn_compose --profile ui up -d --wait --wait-timeout 180 --force-recreate \
     airflow-init airflow-scheduler airflow-webserver
   espn_compose exec -T airflow-scheduler airflow jobs check \
     --job-type SchedulerJob
   test "$(espn_compose exec -T airflow-scheduler \
     printenv ESPN_DISCOVERY_STATE_REF_URI)" = "$ESPN_DISCOVERY_STATE_REF_URI"
   test "$(espn_compose exec -T airflow-scheduler \
     printenv ESPN_DISCOVERY_STATE_REF_SHA256)" = "$ESPN_DISCOVERY_STATE_REF_SHA256"
   test "$(espn_compose exec -T airflow-scheduler \
     printenv ESPN_ISOLATED_STACK)" = '1'
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
   espn_airflow dags unpause dag_backfill_espn
   ```

   После каждой exact coverage reconciliation вычислить deterministic
   `sorted(target - COMPLETE)` и передать только первые 10 отсутствующих scope
   как явный `scopes` (explicit <=10 cohort); это ten-scope bootstrap rule, а
   не разрешение расширить map/lease limit. Дождаться terminal success и
   release leases перед следующим cohort.

   ```bash
   COHORT_CONF='{"scopes":["<first-missing-scope>","..."]}'
   espn_airflow dags trigger dag_backfill_espn --conf "$COHORT_CONF"
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

5. После deploy parser migration выполнить только manual full reconciliation
   каждого scope. Новый runtime выпускает исключительно
   `espn-native-parser-v3` / `espn-native-runtime-v4`; one-hop bridge из
   `espn-native-parser-v2` / `espn-native-runtime-v3` разрешён только внутри
   такого полного reconciliation. Partial daily и noop поверх v2 head
   fail-closed. Для точного воспроизведения прежнего v2 результата единственным
   разрешённым runtime остаётся pinned git `e12b85a`.

   До снятия паузы с scheduler daily admission должен проверить exact
   `181/181 v3/v4 heads`: полный frozen target без missing/extra scope, и каждый
   immutable COMPLETE snapshot обязан иметь parser v3/runtime v4. Смешанные
   версии строк, неизвестный parser/runtime transition или хотя бы один v2/v3
   head запрещают enablement.

6. После пустой разности и exact `181/181 v3/v4 heads` запустить один manual
   all-scope canary через `dag_backfill_espn`, передав в `scopes` exact frozen
   target array из saved state ref/`male_registry_ref`; zero-row scope считается успешным лишь
   с exact signed manifest/checkpoint evidence, а не из-за отсутствия mapped
   output. Summary reuse также требует exact signed prior evidence. Затем
   подтвердить zero active leases и zero alerts. Только после этого
   вернуть manual backfill на паузу, оставить repair/replay на паузе,
   снять паузу с child ingest и monitoring, затем с discovery и лишь
   после них — с daily owner. Этот порядок обязателен после
   init/recreate, когда все ESPN DAG-и могли быть поставлены на паузу:

   Перед trigger атомарно claim exact canary через
   `scripts.espn_canary_campaign.claim_campaign_attempt`. Identity равен
   `(ESPN_RELEASE_COMMIT, ESPN_RELEASE_TREE_SHA256, registry_signature,
   target_scope_sha256)`. Новый release начинает `ordinal001`; продолжение того
   же campaign может использовать только `ordinal002` и `ordinal003` после
   immutable failure receipt. `guard_only=True` ledger не меняет и ordinal не
   расходует. Active/successful/malformed campaign, registry/target/release-tree
   drift или четвёртая попытка блокируют trigger. Successor release обязан
   сослаться на exact predecessor failure URI/SHA и непустую remediation.
   Claim возвращает content-addressed immutable `claim_ref`. Задать только его
   точную пару как `ESPN_CANARY_CLAIM_URI` и `ESPN_CANARY_CLAIM_SHA256` до
   admission. Admission strict-read проверяет bytes/SHA, canonical
   `CampaignLedger`, latest active attempt, exact frozen scope array/target SHA
   и текущий ledger SHA. До первого control-store access admission атомарно
   создаёт immutable single-use consumption marker, связанный с exact
   `(dag_id, run_id, canonical admission identity)`, и включает claim,
   consumption ref и admission identity в signed admission и каждый signed
   plan. Retry того же exact run идемпотентен; concurrent или последовательный
   другой run с тем же claim всегда блокируется до lease/fetch/write. После
   terminal receipt `finish_campaign_attempt` публикует отдельный immutable
   deterministic `finish_ref`; он монотонно отзывает claim, поэтому даже
   восстановление старых active ledger bytes не возвращает authorization.
   Claim, consumption и finish evidence никогда не перезаписывают друг друга.
   Текущий runtime исполняет только `espn-airflow-admission-v3`; authentic v1/v2
   можно разобрать для аудита, но их execution/retry требует pinned `e12b85a` и
   fail-closed отклоняется до registry/control/network/publication.

   ```bash
   espn_airflow dags pause dag_backfill_espn
   espn_airflow dags pause dag_repair_espn
   espn_airflow dags pause dag_replay_espn
   espn_airflow dags unpause dag_ingest_espn
   espn_airflow dags unpause dag_monitor_espn
   espn_airflow dags unpause dag_discover_espn_registry
   espn_airflow dags unpause dag_trigger_espn_daily
   ```

   Из-за нового registry signature обязательны три новых scheduled green
   parent/child runs. Для v4 день считается зелёным при точной квалификации
   каждого scope в состоянии `complete` **или** `noop`; manual bootstrap/canary
   не засчитываются в эти три.

### Rollback expanded registry

При regression или hard alert немедленно поставить owner на паузу. Сохранить
immutable raw/generation/reconciliation evidence; ничего из него не удалять.
Затем восстановить last reviewed release и **freeze the last good immutable
discovery-state ref, binding the last good `male_registry_ref`** в rollback
record/admission evidence. Установить сохранённые
`ESPN_DISCOVERY_STATE_REF_URI` и `ESPN_DISCOVERY_STATE_REF_SHA256` во всех
Airflow компонентах и повторить exact `espn_compose ... up -d
--force-recreate` и container `printenv` проверки из rollout. Не выбирать
текущий `latest-state.json` и
не генерировать новый registry во время rollback: last good frozen state ref
и его `male_registry_ref` остаются неизменяемым target до отдельного reviewed
rollout.

```bash
espn_airflow dags pause dag_trigger_espn_daily
espn_airflow dags pause dag_discover_espn_registry
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

Production qualification использует parser v3/runtime v4. Для каждого
scheduled scope состояние должно быть
одинаковым в durable summary, неизменённом `espn-run-manifest-evidence-v1` и
publication: только `complete` или `noop`. `complete` публикует новую физическую
COMPLETE generation. `noop` заново проверяет уже выбранную COMPLETE generation,
но не двигает `ScopeHead.published_at` и не притворяется новой generation.

`espn-durable-run-manifest-v1` теперь содержит встроенные `release` и
`qualification`, не создавая седьмой public Bronze object. Release связывает
exact commit/tree, parser/runtime, registry, sorted target SHA и campaign ID.
Каждый scope получает outcome `complete_new` или `noop_revalidated`; fresh
no-op обязан содержать текущий signed run-evidence URI/SHA и `recorded_at`.
Schedule/entity/event dispositions используют только `captured`,
`valid_empty`, `not_applicable` и структурированные failures на каждом уровне.
Raw evidence сохраняет exact URI/SHA.

Пустой schedule не может стать зелёным только из-за `rows=0`: все подписанные
planned windows должны быть успешно получены, schema-valid и unsaturated,
competition/season ownership должна совпадать, известный nonterminal event не
может исчезнуть, а empty требует второй более свежей scheduled observation или
явной source capability metadata. `schedule=proven` с нулём строк всегда red.
Первая scheduled observation при `schedule=unknown` записывается как
non-serving `pending_empty` в существующие run-manifest/control evidence: она
связывает scheduled run ID, timestamp, exact planned windows и immutable Raw
URI/SHA, но не двигает serving head и завершает task красным. Следующий signed
plan strict-read восстанавливает эту observation из exact evidence/raw-manifest
refs; только другой более поздний scheduled run может образовать каноническую
пару и квалифицировать `valid_empty`.
Для каждого `played_final && summary_required` lineup и matchsheet либо
`captured` с обеими командами, либо evidence-backed `valid_empty` только при
partial/absent/unknown capability. Каждый nonfinal event явно
`not_applicable`; для аутентичных pre-Task2 parser-v3/runtime-v4 snapshots это
состояние безопасно выводится при qualification, даже если старый физический
snapshot ещё не содержал disposition rows.

После published DQ рядом с immutable `run-evidence.json` должен лежать
канонический sibling `qualification-attestation.json` вида
`espn-scope-qualification-attestation-v1`. Сборщик для каждого scope обязан
получить exact URI/SHA этого sibling только заменой конечного имени
`run-evidence.json`, проверить его и положить ссылку в
`qualification_attestation_ref`; `latest` и поиск по префиксу запрещены.
Attestation связывает текущую квалификацию с выбранной физической generation,
её registry/parser/runtime, signed plan, lease, publication и DQ.

`espn-run-success-receipt-v1` расширен теми же embedded `release`,
`canary_campaign` и `qualification`; `receipt_sha256` подписывает их вместе с
остальным финальным evidence. Canonical sibling attestation остаётся отдельной
immutable scope-связью и не превращается в новый public object. Старые
promotion evidence v2/v3 остаются complete-only и не могут квалифицировать
новый release campaign.
Перед verdict и ещё раз перед final receipt reducer strict-read реконструирует
полную canonical qualification каждого scope из exact publication snapshot,
current-run evidence, dispositions и Raw URI/SHA. Verdict и health обязаны
нести один и тот же validated durable-manifest ref; count/scope-only совпадение
или tamper любого nested state/Raw ref fail-closed блокирует receipt.

Перед cutover нужны **три последовательных зелёных** scheduled qualification
запуска `dag_ingest_espn` для того же scope и registry signature. В v4
разрешены последовательности из `complete|noop`, например
`complete, noop, noop` или `noop, noop, noop`, если каждый `noop` полностью
перепроверил exact current COMPLETE head. В promotion evidence положить exact
registry snapshot URI/SHA, а для каждого запуска — durable/raw, published-DQ,
qualification-attestation, terminal-verdict, health, lease-release и final
success receipt URI/SHA. Три master data interval должны соприкасаться без
пропуска. Кандидатом становится физическая COMPLETE generation, выбранная
третьей квалификацией; сам `noop` новой generation не создаёт. Ошибка, warning,
stale manifest, неподходящий parser/runtime, пропущенный день или другой
registry signature обнуляют серию.

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
