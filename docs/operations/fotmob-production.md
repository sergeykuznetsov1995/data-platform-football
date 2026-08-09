# FotMob production: automatic catalog, cleanup и rollback

Это инструкция для нового production-контура FotMob.

Коротко: список турниров не заполняется руками. Automatic catalog сам находит
мужские турниры, включая мужские товарищеские. Женские турниры исключаются.
Неизвестный или противоречивый турнир не запускается, пока классификатор не
получит однозначное доказательство.

## Что работает в Airflow

В isolated stack разрешены ровно шесть DAG:

| DAG | Schedule | Состояние после automatic activation |
| --- | --- | --- |
| `dag_orchestrate_fotmob` | `*/5 * * * *` | unpaused |
| `dag_ingest_fotmob` | `None` | unpaused |
| `dag_transform_fotmob_silver` | `None` | unpaused |
| `dag_trigger_fotmob_daily` | `None` | paused |
| `dag_refresh_fotmob` | `None` | paused |
| `dag_backfill_fotmob` | `None` | paused |

Три последних DAG — legacy. У них больше нет production schedule. Их нельзя
включать для cutover или recovery.

В shared Airflow Variable `fotmob_schedule_owner` всегда имеет значение
`isolated`. Во время activation deploy сам включает shared
`dag_sofascore_pipeline`. Не менять ownership Variable руками.

## Перед началом

Нужны:

- чистый reviewed checkout нужного commit;
- Airflow и PostgreSQL images с полным `@sha256:<64-hex>` digest;
- защищённый env-файл с Airflow, Trino, S3, Telegram и
  `FBREF_CONTROL_DB_URI`;
- durable evidence directory, доступный uid `50000`;
- shared scheduler с тем же commit и read-only mount этого evidence directory;
- `FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH`, указывающий в shared container на тот
  же `deployment.json`;
- shared `dag_master_pipeline`, `dag_sofascore_pipeline`,
  `dag_ingest_fotmob` и `dag_transform_fotmob_silver` paused, без
  `running`/`queued` FotMob runs.

Deploy сам проверяет container IDs, Git SHA, runtime files, общий control DB,
Airflow metadata и protected evidence. При несовпадении он ничего не включает.

Из корня чистого checkout один раз задайте значения:

```bash
export FOTMOB_RELEASE_ROOT="$PWD"
export FOTMOB_RELEASE_SHA="$(git rev-parse HEAD)"
export FOTMOB_ENV=/protected/path/fotmob.env
export FOTMOB_TRINO_ENV=/protected/path/fotmob-host-trino.env
export FOTMOB_EVIDENCE=/durable/path/fotmob-evidence
export FOTMOB_DEPLOY_REPORT="$FOTMOB_EVIDENCE/deployment.json"
export FOTMOB_CANARY_REPORT="$FOTMOB_EVIDENCE/automatic-canary.json"
export FOTMOB_AIRFLOW_IMAGE='registry/data-platform-airflow@sha256:<64-hex>'
export FOTMOB_POSTGRES_IMAGE='docker.io/library/postgres@sha256:<64-hex>'
export FOTMOB_SHARED_SCHEDULER=airflow-scheduler
mkdir -p "$FOTMOB_EVIDENCE"
```

Не копировать, не переименовывать и не редактировать evidence JSON между
шагами.

## 1. Подготовить automatic catalog, всё оставить paused

Запускайте именно с `--automatic-catalog --keep-paused`:

```bash
python deploy/fotmob/deploy.py \
  --release-root "$FOTMOB_RELEASE_ROOT" \
  --env-file "$FOTMOB_ENV" \
  --image "$FOTMOB_AIRFLOW_IMAGE" \
  --postgres-image "$FOTMOB_POSTGRES_IMAGE" \
  --evidence-dir "$FOTMOB_EVIDENCE" \
  --shared-scheduler-container "$FOTMOB_SHARED_SCHEDULER" \
  --automatic-catalog \
  --keep-paused \
  --report "$FOTMOB_DEPLOY_REPORT"
```

Ожидаемый результат:

- `passed=true`;
- `activation_state=kept_paused`;
- `automatic_rollout.phase=awaiting_canary`;
- все шесть DAG находятся в `paused`, а `unpaused` пуст.

Проверка:

```bash
jq '{passed, activation_state, paused, unpaused, automatic_rollout}' \
  "$FOTMOB_DEPLOY_REPORT"
```

## 2. Запустить один manual automatic canary

Canary использует dynamic `fotmob-catalog-v1`, а не статический список #930.
Он временно включает только ingest и Silver, затем снова оставляет все шесть
DAG paused.

```bash
python scripts/fotmob_backfill.py run \
  --mode automatic-canary \
  --env-file "$FOTMOB_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --expected-git-sha "$FOTMOB_RELEASE_SHA" \
  --publication-attempt 1 \
  --max-requests 10000 \
  --max-direct-mib 512 \
  --timeout-seconds 86400 \
  --execute \
  --confirm RUN_FOTMOB_AUTOMATIC_CANARY \
  --output "$FOTMOB_CANARY_REPORT"
```

Ожидаемый результат:

- `passed=true`;
- `phase=abandoned`;
- `recovery_required=false`;
- ingest и Silver завершились `success`;
- publication generation освобождена и не опубликована.

```bash
jq '{passed, phase, recovery_required, ingest_run_state, silver_run_state, final_publication}' \
  "$FOTMOB_CANARY_REPORT"
```

Если report требует recovery, не освобождайте lock и не переключайте DAG
руками. Выполните recovery с тем же deployment, attempt и лимитами:

```bash
python scripts/fotmob_backfill.py recover \
  --mode automatic-canary \
  --env-file "$FOTMOB_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --recovery-report "$FOTMOB_CANARY_REPORT" \
  --expected-git-sha "$FOTMOB_RELEASE_SHA" \
  --publication-attempt 1 \
  --max-requests 10000 \
  --max-direct-mib 512 \
  --execute \
  --confirm RECOVER_FOTMOB_AUTOMATIC_CANARY \
  --output "$FOTMOB_EVIDENCE/automatic-canary-recovery.json"

export FOTMOB_CANARY_REPORT="$FOTMOB_EVIDENCE/automatic-canary-recovery.json"
```

Для activation передавайте последний зелёный canary report.

## 3. Включить automatic owner

Activation разрешён только для сегодняшней daily boundary в окне
`13:30 <= UTC < 14:45`. Используйте тот же release, env, images, evidence,
project, shared container и `deployment.json`, что на шаге 1.

```bash
python deploy/fotmob/deploy.py \
  --release-root "$FOTMOB_RELEASE_ROOT" \
  --env-file "$FOTMOB_ENV" \
  --image "$FOTMOB_AIRFLOW_IMAGE" \
  --postgres-image "$FOTMOB_POSTGRES_IMAGE" \
  --evidence-dir "$FOTMOB_EVIDENCE" \
  --shared-scheduler-container "$FOTMOB_SHARED_SCHEDULER" \
  --automatic-catalog \
  --activate-automatic \
  --automatic-canary-report "$FOTMOB_CANARY_REPORT" \
  --report "$FOTMOB_DEPLOY_REPORT"
```

Ожидаемый результат:

- `passed=true`;
- `activation_state=active`;
- `dag_orchestrate_fotmob`, ingest и Silver unpaused;
- три legacy DAG paused;
- shared SofaScore consumer unpaused;
- owner включён последним, одним проверенным переходом.

```bash
jq '{passed, activation_state, paused, unpaused, automatic_rollout, automatic_activation}' \
  "$FOTMOB_DEPLOY_REPORT"
```

Если ошибка произошла до включения owner, deploy возвращает всё в paused и
может остановить isolated scheduler. После исправления причины повторите ту же
activation-команду.

Если report имеет `activation_state=pending_automatic` и
`recovery_required=true`, owner уже мог включиться. Ничего не останавливайте,
не ставьте DAG на паузу и не правьте JSON. Повторите ту же activation-команду:
она сначала прочитает реальное состояние и только потом безопасно завершит
report.

Если старый Sofa sensor уже истёк, повторная команда принимает только реальный
безопасный failed-граф: wait=`failed`, три transform trigger=`upstream_failed`,
finalizer=`failed`, активной publication нет. Старый interval не переигрывается;
rollout продолжится со следующей живой daily boundary.

## Если automatic generation упал после activation

Не ждите 14 дней и не освобождайте lock руками. Возьмите exact generation ID
из упавшего owner/ingest/Silver run и запустите recovery:

```bash
export FOTMOB_GENERATION_ID='<exact UUID>'
export FOTMOB_RECOVERY_REPORT="$FOTMOB_EVIDENCE/automatic-recovery-$FOTMOB_GENERATION_ID.json"

python scripts/fotmob_recover.py \
  --project fotmob-airflow \
  --compose-file deploy/fotmob/airflow.compose.yaml \
  --env-file "$FOTMOB_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --generation-id "$FOTMOB_GENERATION_ID" \
  --execute \
  --confirm RECOVER_FOTMOB_AUTOMATIC_PUBLICATION \
  --output "$FOTMOB_RECOVERY_REPORT"
```

Команда сама проверяет active deployment, оба живых scheduler и exact цепочку
owner → ingest → Silver. Затем она ставит ровно шесть isolated DAG на паузу.

- `writing` или retained `failed`: только полностью остановленная цепочка
  получает `safe_to_release=true`. Cursor остаётся без изменений, но terminal
  generation никогда не открывается повторно. Daily не повторяется в тот же
  день: он снова станет доступен на следующей календарной границе 14:00 UTC с
  новыми generation/child run ID. Refresh/backfill сохраняет ту же lane и
  повторяется на следующем допустимом пятиминутном owner interval, тоже с
  новыми ID.
- daily `ready` без consumer: lock освобождается только для exact упавшего
  scheduled Sofa run с terminal wait/downstream proof.
- refresh/backfill `ready` или уже `abandoned`: нужны exact успешные ingest и
  Silver, failed owner finalizer/cursor и пустые active-run/task proofs. Команда
  idempotent-abandon поколение и двигает fair cursor; Sofa здесь не существует.
- `consuming`: команда ничего не освобождает и пишет красный protected report.
  Это означает, что shared-таблицы могли измениться частично; нужен отдельный
  reviewed repair по exact Sofa/xref/E3/E4 lineage.

После зелёного recovery все шесть isolated DAG и новые Sofa schedules остаются
paused. Проверьте `rollout_ready` в report. Если там `false`, старый shared run
ещё спокойно заканчивает failure: не очищайте его tasks, а после terminal
состояния повторите ту же recovery-команду. Она idempotent и обновит proof.
Возвращайте traffic только когда `rollout_ready=true`, новым pristine automatic
rollout начиная с шага 1. Не включайте owner или children руками.

## Первый scheduled run

Activation — это разрешение работать, а не доказательство успешной загрузки.
Для первого daily interval должны завершиться `success`:

- scheduled `dag_orchestrate_fotmob`;
- его exact ingest child;
- exact Silver child;
- shared `dag_sofascore_pipeline`;
- task `finalize_fotmob_publication`;
- publication со статусом `published` и `released`.

Сохраните их exact run IDs, generation ID и publication binding в protected
artifact `fotmob-scheduled-observation-v1`.

После завершения первого daily запуска создайте artifact read-only командой:

```bash
export FOTMOB_SCHEDULED_OBSERVATION="$FOTMOB_EVIDENCE/first-scheduled-observation.json"

python scripts/fotmob_observe.py \
  --project fotmob-airflow \
  --compose-file deploy/fotmob/airflow.compose.yaml \
  --env-file "$FOTMOB_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --output "$FOTMOB_SCHEDULED_OBSERVATION"
```

Команда дважды читает обе Airflow metadata, проверяет живые container/runtime,
общий control DB и exact цепочку owner → ingest → Silver → Sofa → finalizer.
Она пишет только зелёный protected artifact с правами `0600`; чужой файл не
перезаписывает. Purge затем ещё раз сверяет artifact с живыми metadata и
control/data plane, поэтому одного JSON недостаточно.

## Purge только 10557/10558

Purge не является частью activation. Он жёстко ограничен ID `10557` и `10558`.
Нельзя запускать ни plan, ни apply без текущего active deployment report и
защищённого первого scheduled observation.

Перед plan поставьте на паузу isolated, shared consumer и оба shared
maintenance DAG. Команда проверяет, что никто из них не работает:

```bash
export FOTMOB_PAUSE_REPORT="$FOTMOB_EVIDENCE/rollback-pause.json"

python scripts/fotmob_rollback.py pause \
  --env-file "$FOTMOB_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --execute \
  --confirm PAUSE_FOTMOB_WRITERS \
  --output "$FOTMOB_PAUSE_REPORT"

export FOTMOB_PAUSE_REPORT_SHA="$(sha256sum "$FOTMOB_PAUSE_REPORT" | awk '{print $1}')"
```

Эта пауза останавливает automatic traffic и временно ставит на паузу две
maintenance DAG. Automatic traffic можно вернуть только новым rollout; старые
owner DAG вручную не включать. Состояние maintenance возвращается отдельной
командой ниже.

Read-only plan:

```bash
export FOTMOB_PURGE_PLAN="$FOTMOB_EVIDENCE/fotmob-10557-10558-purge-plan.json"

python scripts/purge_fotmob_competitions.py \
  --env-file "$FOTMOB_ENV" \
  --trino-env-file "$FOTMOB_TRINO_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --scheduled-observation-report "$FOTMOB_SCHEDULED_OBSERVATION" \
  --output "$FOTMOB_PURGE_PLAN"

export FOTMOB_PURGE_PLAN_SHA="$(jq -r '.plan_sha256' "$FOTMOB_PURGE_PLAN")"
```

Plan живёт один час. После отдельного review применяйте тот же файл и его
встроенный canonical SHA-256. Команда чтения отклоняет JSON с повторяющимися
ключами:

```bash
python scripts/purge_fotmob_competitions.py \
  --apply \
  --env-file "$FOTMOB_ENV" \
  --trino-env-file "$FOTMOB_TRINO_ENV" \
  --plan "$FOTMOB_PURGE_PLAN" \
  --plan-sha256 "$FOTMOB_PURGE_PLAN_SHA" \
  --journal "$FOTMOB_EVIDENCE/fotmob-10557-10558-purge-journal.json" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --scheduled-observation-report "$FOTMOB_SCHEDULED_OBSERVATION"
```

Не создавать plan заново вместо review старого файла. Apply сохраняет journal
для безопасного продолжения после прерывания.

После `status=complete` вернуть только исходные состояния двух maintenance DAG:

```bash
export FOTMOB_OPERATION_REPORT="$FOTMOB_EVIDENCE/fotmob-10557-10558-purge-journal.json"
export FOTMOB_OPERATION_REPORT_SHA="$(sha256sum "$FOTMOB_OPERATION_REPORT" | awk '{print $1}')"

python scripts/fotmob_rollback.py restore-maintenance \
  --env-file "$FOTMOB_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --pause-evidence "$FOTMOB_PAUSE_REPORT" \
  --pause-evidence-sha256 "$FOTMOB_PAUSE_REPORT_SHA" \
  --operation-report "$FOTMOB_OPERATION_REPORT" \
  --operation-report-sha256 "$FOTMOB_OPERATION_REPORT_SHA" \
  --execute \
  --confirm RESTORE_FOTMOB_MAINTENANCE \
  --output "$FOTMOB_EVIDENCE/purge-maintenance-restore.json"
```

Если plan просмотрен, но apply точно не запускался, сначала пересчитайте SHA
именно plan, затем используйте тот же вызов:

```bash
export FOTMOB_OPERATION_REPORT="$FOTMOB_PURGE_PLAN"
export FOTMOB_OPERATION_REPORT_SHA="$(sha256sum "$FOTMOB_OPERATION_REPORT" | awk '{print $1}')"
# В restore-maintenance добавить --aborted-before-mutation.
```

## Optional cleanup

Cleanup удаляет только старые staging tables и при необходимости компактит
`fotmob_field_inventory`. Он не удаляет native Bronze, raw cache,
`fotmob_competitions`, `fotmob_competitions_current`, scope observations или их
current view.

Сначала создать read-only plan:

```bash
export FOTMOB_CLEANUP_PLAN="$FOTMOB_EVIDENCE/cleanup-plan.json"

python scripts/fotmob_cleanup.py plan \
  --trino-env-file "$FOTMOB_TRINO_ENV" \
  --older-than-hours 24 \
  --output "$FOTMOB_CLEANUP_PLAN"

sha256sum "$FOTMOB_CLEANUP_PLAN"
```

Перед execute поставить на паузу все шесть isolated DAG и shared consumer:

```bash
export FOTMOB_PAUSE_REPORT="$FOTMOB_EVIDENCE/rollback-pause.json"

python scripts/fotmob_rollback.py pause \
  --env-file "$FOTMOB_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --execute \
  --confirm PAUSE_FOTMOB_WRITERS \
  --output "$FOTMOB_PAUSE_REPORT"

export FOTMOB_PAUSE_REPORT_SHA="$(sha256sum "$FOTMOB_PAUSE_REPORT" | awk '{print $1}')"
```

Pause report должен показывать все шесть isolated DAG paused, отсутствие
`running`/`queued` runs и shared consumer paused в том же admitted container.

После review exact plan:

```bash
export FOTMOB_CLEANUP_PLAN_SHA='<reviewed-64-hex-sha256>'

python scripts/fotmob_cleanup.py execute \
  --env-file "$FOTMOB_ENV" \
  --trino-env-file "$FOTMOB_TRINO_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --release-sha "$FOTMOB_RELEASE_SHA" \
  --plan "$FOTMOB_CLEANUP_PLAN" \
  --plan-sha256 "$FOTMOB_CLEANUP_PLAN_SHA" \
  --pause-evidence "$FOTMOB_PAUSE_REPORT" \
  --confirm EXECUTE_REVIEWED_FOTMOB_CLEANUP \
  --output "$FOTMOB_EVIDENCE/cleanup-result.json"
```

Cleanup оставляет scheduler и shared consumer paused. После зелёного
`cleanup-result.json` верните исходное состояние только maintenance DAG:

```bash
export FOTMOB_OPERATION_REPORT="$FOTMOB_EVIDENCE/cleanup-result.json"
export FOTMOB_OPERATION_REPORT_SHA="$(sha256sum "$FOTMOB_OPERATION_REPORT" | awk '{print $1}')"

python scripts/fotmob_rollback.py restore-maintenance \
  --env-file "$FOTMOB_ENV" \
  --deployment-report "$FOTMOB_DEPLOY_REPORT" \
  --pause-evidence "$FOTMOB_PAUSE_REPORT" \
  --pause-evidence-sha256 "$FOTMOB_PAUSE_REPORT_SHA" \
  --operation-report "$FOTMOB_OPERATION_REPORT" \
  --operation-report-sha256 "$FOTMOB_OPERATION_REPORT_SHA" \
  --execute \
  --confirm RESTORE_FOTMOB_MAINTENANCE \
  --output "$FOTMOB_EVIDENCE/cleanup-maintenance-restore.json"
```

Возврат automatic traffic — только новый rollout с шага 1.

Если cleanup plan создан до pause, но execute точно не запускался, используйте
его exact SHA и тот же `restore-maintenance` с
`--operation-report "$FOTMOB_CLEANUP_PLAN" --aborted-before-mutation`. Время
создания plan может быть раньше pause: отдельный флаг фиксирует решение
оператора об отмене сейчас.

## Rollback: только coordinator-only

Rollback не включает legacy schedule и не меняет ownership обратно. Native
Bronze, raw cache и dynamic catalog/evidence сохраняются.

1. Сохранить read-only plan:

   ```bash
   python scripts/fotmob_rollback.py plan \
     --env-file "$FOTMOB_ENV" \
     --deployment-report "$FOTMOB_DEPLOY_REPORT" \
     --output "$FOTMOB_EVIDENCE/rollback-plan.json"
   ```

2. Поставить на паузу exact six DAG и shared consumer:

   ```bash
   python scripts/fotmob_rollback.py pause \
     --env-file "$FOTMOB_ENV" \
     --deployment-report "$FOTMOB_DEPLOY_REPORT" \
     --execute \
     --confirm PAUSE_FOTMOB_WRITERS \
     --output "$FOTMOB_EVIDENCE/rollback-pause.json"

   export FOTMOB_PAUSE_REPORT_SHA="$(sha256sum "$FOTMOB_EVIDENCE/rollback-pause.json" | awk '{print $1}')"
   ```

3. Из pristine checkout reviewed consumer-revert commit выполнить
   coordinator-only deploy. Сначала развернуть тот же revert commit в shared
   Airflow, сохранив shared consumer paused и ownership `isolated`. Затем из
   корня revert checkout заново зафиксировать его путь и SHA. Использовать
   `--keep-paused` без `--automatic-catalog` и без activation flags:

   ```bash
   export FOTMOB_RELEASE_ROOT="$PWD"
   export FOTMOB_RELEASE_SHA="$(git rev-parse HEAD)"

   python deploy/fotmob/deploy.py \
     --release-root "$FOTMOB_RELEASE_ROOT" \
     --env-file "$FOTMOB_ENV" \
     --image "$FOTMOB_AIRFLOW_IMAGE" \
     --postgres-image "$FOTMOB_POSTGRES_IMAGE" \
     --evidence-dir "$FOTMOB_EVIDENCE" \
     --shared-scheduler-container "$FOTMOB_SHARED_SCHEDULER" \
     --keep-paused \
     --report "$FOTMOB_DEPLOY_REPORT"
   ```

   Ожидается `coordinator_rollout.phase=kept_paused`, все шесть DAG paused,
   shared consumer paused и `fotmob_schedule_owner=isolated`.

4. Запустить exact fenced Silver/DQ на revert commit:

   ```bash
   export FOTMOB_ROLLBACK_SHA="$FOTMOB_RELEASE_SHA"
   export FOTMOB_ROLLBACK_PUBLICATION="$FOTMOB_EVIDENCE/rollback-publication.json"

   python scripts/fotmob_rollback.py run-silver \
     --env-file "$FOTMOB_ENV" \
     --deployment-report "$FOTMOB_DEPLOY_REPORT" \
     --expected-consumer-sha "$FOTMOB_ROLLBACK_SHA" \
     --publication-attempt 1 \
     --timeout-seconds 43200 \
     --execute \
     --confirm RUN_FOTMOB_ROLLBACK_VALIDATION_SILVER \
     --output "$FOTMOB_ROLLBACK_PUBLICATION"
   ```

   При ambiguous результате не освобождать lock вручную. Использовать:

   ```bash
   python scripts/fotmob_rollback.py recover-publication \
     --env-file "$FOTMOB_ENV" \
     --deployment-report "$FOTMOB_DEPLOY_REPORT" \
     --publication-report "$FOTMOB_ROLLBACK_PUBLICATION" \
     --publication-attempt 1 \
     --execute \
     --confirm RECOVER_FOTMOB_ROLLBACK_PUBLICATION \
     --output "$FOTMOB_EVIDENCE/rollback-publication-recovery.json"

   export FOTMOB_ROLLBACK_PUBLICATION="$FOTMOB_EVIDENCE/rollback-publication-recovery.json"
   ```

   После recovery используйте последний зелёный publication report.

5. Проверить revert, Silver/DQ, abandoned generation и frozen legacy tables:

   ```bash
   python scripts/fotmob_rollback.py validate \
     --env-file "$FOTMOB_ENV" \
     --trino-env-file "$FOTMOB_TRINO_ENV" \
     --deployment-report "$FOTMOB_DEPLOY_REPORT" \
     --publication-report "$FOTMOB_ROLLBACK_PUBLICATION" \
     --expected-consumer-sha "$FOTMOB_ROLLBACK_SHA" \
     --publication-attempt 1 \
     --silver-run-id '<run-id-from-publication-report>' \
     --output "$FOTMOB_EVIDENCE/rollback-validate.json"
   ```

6. После зелёного validate вернуть исходное состояние только двух maintenance
   DAG. SHA берётся от exact проверенного файла:

   ```bash
   export FOTMOB_OPERATION_REPORT="$FOTMOB_EVIDENCE/rollback-validate.json"
   export FOTMOB_OPERATION_REPORT_SHA="$(sha256sum "$FOTMOB_OPERATION_REPORT" | awk '{print $1}')"

   python scripts/fotmob_rollback.py restore-maintenance \
     --env-file "$FOTMOB_ENV" \
     --deployment-report "$FOTMOB_DEPLOY_REPORT" \
     --pause-evidence "$FOTMOB_EVIDENCE/rollback-pause.json" \
     --pause-evidence-sha256 "$FOTMOB_PAUSE_REPORT_SHA" \
     --operation-report "$FOTMOB_OPERATION_REPORT" \
     --operation-report-sha256 "$FOTMOB_OPERATION_REPORT_SHA" \
     --execute \
     --confirm RESTORE_FOTMOB_MAINTENANCE \
     --output "$FOTMOB_EVIDENCE/rollback-maintenance-restore.json"
   ```

После validate все шесть DAG и shared consumer остаются paused. Чтобы снова
запустить production, нужен новый pristine automatic rollout с шага 1.

## Historical issue #930

Static `fotmob-daily-v1`, старый список из 158 scopes и прежние
backfill/replay процедуры сохранены только как историческое evidence. Artifact
`configs/fotmob/issue-930-scopes.txt` имеет SHA-256
`f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58`.

Этот historical contract не является исполняемой инструкцией для production,
activation или rollback: он может содержать female IDs. Не запускать legacy
schedule и не использовать static #930 scope вместо automatic catalog.
