# FotMob: production, acceptance и rollback

Этот runbook относится к source-native контуру FotMob. Контур использует
отдельный compose-проект и отдельную Airflow metadata DB. Isolated deploy не
создаёт DAG rows в shared metadata DB, но оба scheduler используют одну
production `FBREF_CONTROL_DB_URI`: через неё fenced publication generation
защищает FotMob Silver от одновременного чтения shared xref/Gold. Единственные
явные shared-изменения cutover — заранее reviewed master gate, publication
миграция и ownership Variable. Единственный FotMob producer schedule —
`0 14 * * *` в `dag_trigger_fotmob_daily`.

## Подготовка релиза

1. Создать чистый detached worktree на проверенном commit. В нём должны быть
   `dags/dag_ingest_fotmob.py`, `dags/dag_transform_fotmob_silver.py` и
   `dags/dag_trigger_fotmob_daily.py`.
2. Использовать Airflow и PostgreSQL images, закреплённые полными `sha256`
   digest. Mutable tag, включая versioned tag, admission не проходит.
3. Создать защищённые host paths для env-файла и evidence. Не копировать
   секреты в Git, compose или отчёты.
   В shared `.env` задать `FOTMOB_SHARED_ADMISSION_HOST_DIR` равным тому же
   absolute evidence directory, который будет передан isolated deploy, и
   заранее выбрать container path отчёта, например
   `FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH=/opt/airflow/fotmob-admission/deployment.json`.
   Shared compose монтирует весь каталог отдельно read-only; нельзя использовать
   file bind, потому что atomic replacement отчёта меняет inode, и нельзя
   давать task-ам второй writable mount того же каталога или его parent/child.
   Deploy сверяет inode и canonical paths всех writable bind/volume mounts;
   вложенность с logs/state fail-closed. Shared scheduler
   должен быть создан с этим mount до admission и не пересоздаваться между
   созданием report и shared fallback: report привязан к его full container ID.
   Каталог обязан существовать до `docker compose up` (`create_host_path=false`
   у scheduler mount). Deploy нормализует evidence directory и вложенный report
   directory в `0755`, в том числе при host umask `077`; секретов там быть не
   должно. Scheduler-specific `volumes` обязан явно содержать и этот mount, и
   `./configs/fotmob:/opt/airflow/configs/fotmob:ro`, потому что YAML sequence
   override не наследует common volumes. Deploy через `docker inspect` требует
   exact resolved host source, bind destination
   `/opt/airflow/fotmob-admission`, `RW=false`, а внутри exact shared container
   сверяет `FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH` с относительным путём
   `--report` под этим mount. Несовпадение останавливает admission.
4. Env-файл должен определять Airflow/Trino/S3 credentials,
   `FOTMOB_AIRFLOW_DB_PASSWORD`, непустые `TELEGRAM_BOT_TOKEN` /
   `TELEGRAM_CHAT_ID` и `FBREF_CONTROL_DB_URI`. Последняя обязана
   указывать на ту же main Airflow PostgreSQL DB, что и shared scheduler, а не
   на isolated `airflow-metadb`; пароль в URI должен быть URL-escaped. Пароль
   isolated metadata DB генерируется только из URL-safe символов
   `[A-Za-z0-9._~-]`, так как он входит в SQLAlchemy URI. Release root, image,
   Git SHA и evidence path выставляет deploy-команда.
5. Evidence path заранее создать для uid `50000`, запретить чтение посторонним и
   хранить вне release worktree.

Перед isolated deploy выполнить ownership handoff в shared Airflow:

1. Развернуть в shared scheduler тот же commit, включая gate в
   `dag_master_pipeline.py`, production consumer в
   `dag_sofascore_pipeline.py`, fenced child DAG-и
   `dag_transform_xref.py`, `dag_ingest_fotmob.py`,
   `dag_transform_fotmob_silver.py`, `dag_transform_e3.py`,
   `dag_transform_e4.py`, `dag_transform_fbref_gold.py`,
   `dags/scripts/run_fotmob_scraper.py`, `dags/utils/fotmob_publication.py` и
   актуальные ControlStore migrations. В root Compose задать точные
   `FBREF_CONTROL_DB_URI` и `FOTMOB_DEPLOY_GIT_SHA`, пересоздать Airflow
   services и убедиться, что external network `dp-backend` доступна isolated
   stack. Shared Airflow обязан иметь read-only mount
   `./configs/fotmob:/opt/airflow/configs/fotmob`; без exact
   `competitions.json` и `issue-930-scopes.txt` (SHA-256
   `f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58`)
   native runner не допускается. Scope-файл берётся
   только из того же clean release checkout и commit, что и deploy:
   не генерировать, не копировать из другой ветки и не редактировать
   его вручную. Из корня release до deploy обязательно проверить:

   ```bash
   test "$(wc -l < configs/fotmob/issue-930-scopes.txt)" -eq 158
   test "$(sha256sum configs/fotmob/issue-930-scopes.txt | awk '{print $1}')" = \
     f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58
   ```
2. Установить shared Airflow Variable `fotmob_schedule_owner=isolated`.
   Shared services не должны иметь `FOTMOB_ISOLATED_STACK`: при shared default
   файл `dag_trigger_fotmob_daily.py` не materialize-ит DAG. Если от прежнего
   parse остался stale shared DagModel `dag_trigger_fotmob_daily`, поставить его
   на паузу.
3. Зафиксировать реальное production orchestration state:
   `dag_master_pipeline=paused`, `dag_sofascore_pipeline=unpaused`, shared
   `dag_ingest_fotmob=paused` и shared
   `dag_transform_fotmob_silver=paused`. Дождаться отсутствия `running` и
   `queued` runs у master, SofaScore pipeline, shared ingest/Silver, xref, E3,
   E4, FBref Gold и stale shared `dag_trigger_fotmob_daily`.
   Deploy одним metadata snapshot повторно доказывает эти pause/run states,
   exact runtime SHA, одинаковую control DB, валидные migration checksums и
   значение Variable. Byte attestation — не частичный список файлов: report
   содержит exact SHA-256 manifest всех source/config файлов из shared bind
   roots `dags`, `scrapers`, `scripts`, `configs/medallion` и
   `configs/fotmob`, а deploy требует полного совпадения local release и
   container inventory. Сюда входят все DAG/SQL/helper/ControlStore/native
   scraper dependencies, `configs/fotmob/competitions.json` и approved
   `issue-930-scopes.txt`; omitted, extra или stale runtime input закрывает
   admission. Generated
   `__pycache__/*.pyc` не входит в manifest, остальные runtime source-файлы
   входят по exact path и bytes. Input container name один раз resolve-ится в
   полный 64-hex ID; все proofs выполняются по этому ID, а его замена между
   initial/final handoff закрывает admission.
   Serialized Sofa DAG обязан иметь
   `wait_for_fotmob_publication -> trigger_xref_transforms` и
   `[wait_for_fotmob_publication, trigger_e4_transforms] ->
   finalize_fotmob_publication` (`all_done`). В serialized xref DAG все writer
   tasks обязаны быть descendants
   `validate_fotmob_publication_consumer` с `all_success`. Serialized E3/E4 и
   FBref Gold также обязаны ставить тот же `all_success` preflight перед первым
   writer root и всеми downstream tasks. Shared isolated-daily row должен
   отсутствовать; stale row допускается только paused и без `queued`/`running`
   runs. Isolated compose, напротив, задаёт exact
   `FOTMOB_ISOLATED_STACK=1`, а fresh isolated DagBag обязан содержать scheduled
   daily DAG. Без этих доказательств isolated DAG-и не unpause.

Пример запуска из корня release worktree:

```bash
python deploy/fotmob/deploy.py \
  --release-root /absolute/path/to/clean-release \
  --env-file /protected/path/fotmob.env \
  --image registry/data-platform-airflow-scheduler@sha256:<64-hex> \
  --postgres-image docker.io/library/postgres@sha256:<64-hex> \
  --evidence-dir /durable/path/fotmob-evidence \
  --shared-scheduler-container airflow-scheduler \
  --report /durable/path/fotmob-evidence/deployment.json
```

`--report` обязан находиться внутри `--evidence-dir`: deploy передаёт его exact
container path через `FOTMOB_DEPLOYMENT_REPORT_PATH`, а scheduled preflight
читает те же durable bytes через evidence bind mount.

Deploy прекращается до unpause при любом нарушении. Для admission обязательны:

- scheduler health check проходит;
- DagBag содержит ровно `dag_ingest_fotmob`,
  `dag_transform_fotmob_silver`, `dag_trigger_fotmob_daily`;
- `airflow dags list-import-errors --output json` возвращает пустой массив;
- до admission все три DAG доказанно paused и не имеют active runs;
- shared и isolated scheduler подключены к одной migration-complete control DB;
- Telegram delivery credentials непусты и повторно проверены внутри admitted
  scheduler; report содержит только два presence boolean, не значения;
- точный deployment marker записан и повторно прочитан в целевой Trino/Iceberg
  data plane;
- release checkout, DagBag projection, полные container IDs и immutable image
  IDs повторно сверены у schedule boundary;
- ingest и Silver unpause первыми, daily trigger — строго последним;
- после admission все три DAG находятся в состоянии unpaused.

До первого production run отдельно проверить реальную доставку из admitted
container. Команда не печатает credentials; её JSON output сохранить в durable
evidence и визуально подтвердить получение exact message в целевом Telegram
chat:

```bash
set -o pipefail
docker exec <full-scheduler-container-id-from-deployment.json> python -c 'import json,secrets; from datetime import datetime,timezone; from utils.alerts import send_telegram_message; ts=datetime.now(timezone.utc).isoformat(); message=f"FotMob #930 delivery preflight | timestamp_utc={ts} | nonce={secrets.token_hex(8)}"; ok=send_telegram_message(message,level="info"); print(json.dumps({"timestamp_utc":ts,"message":message,"send_telegram_message_returned":ok},sort_keys=True)); raise SystemExit(not ok)' \
  | tee /durable/path/fotmob-evidence/telegram-delivery.json
```

`send_telegram_message_returned=true` — обязательный preflight, но не замена
delivery proof: оператор сохраняет timestamp/message evidence и подтверждение
фактически полученного сообщения до закрытия #930.

Operational команды принимают только `fotmob-deploy-v2`: report обязан
содержать оба exact shared-handoff snapshot, полный hash set и serialized
topology/quiescence proofs. Старый v1 или частичный report fail-closed.

До unpause daily trigger deploy атомарно записывает и `fsync`-ит уже полный
`deployment.json` с `activation_state=active`, exact container/image identity и
effective isolated runtime manifest. Crash до завершения этого fsync оставляет
daily paused; crash после fsync, но до unpause оставляет безопасное состояние
`active report + paused trigger`, в котором schedule не может запуститься;
crash во время или после unpause всегда имеет durable active identity.
Повторный deploy сначала quiesce-ит обнаруженный stack
и заново выполняет admission. При обычной ошибке deploy best-effort ставит
все DAG на паузу, останавливает scheduler и сохраняет красный отчёт. Для
`--keep-paused` фиксируется `activation_state=kept_paused`.
Каждый atomic replacement принудительно выставляет report mode `0444`, поэтому
файл, созданный host deploy от `root`, читается scheduler-ом с uid `50000`.
World-readable допустим только потому, что deployment report является
non-secret certificate: в нём есть IDs, hashes и два credential-presence
boolean, но нет token/password/chat-id/control URI. Секреты остаются только в
защищённом env-файле. Directory permissions всё равно должны разрешать uid
`50000` traversal/read через isolated mount.

Deploy создаёт byte-exact read-only projection из трёх versioned DAG-файлов и
`deploy/fotmob/.airflowignore`, а compose монтирует projection поверх всего
`/opt/airflow/dags`. Поэтому DAG-и, запечённые в image, не могут попасть в
DagBag. Каталоги `utils`, `sql` и `scripts` монтируются внутрь projection только
для импортов и исключены из парсинга.

## Первый daily и acceptance

Scheduled daily использует один versioned `fotmob-daily-v1` contract из
`fotmob_daily_trigger_conf()`: exact approved 158-scope artifact с SHA-256
`f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58`
динамически сворачивается в 21 competition ID (ID-set SHA-256
`664f972d5d86002131293bcc8da8382f6b7378cd43a8bd37a247c321decf689a`),
а selected/latest season берётся у source. Daily запускает все шесть entities,
включая current `transfers`, с `max_requests=10000`, `max_direct_mib=512`,
`requests_per_minute=60`, без competition/season limit. Parent trigger имеет
14-часовой timeout, SofaScore publication sensor — 16 часов. Восьмичасовой
ingest writer имеет `retries=0`: HTTP retries ограниченно выполняются внутри
runner, поэтому child не может пережить parent через унаследованные Airflow
retries. Это отдельный scheduled contract: one-time replay/backfill closure
ниже намеренно исключает transfer refresh.

Первый task каждого daily run принимает только Airflow `run_type=scheduled` и
до ControlStore initializer повторно сверяет `activation_state=active`, exact
deployment/Git/container/image identity и SHA-256 каждого фактически
смонтированного DAG/helper/SQL/scraper/script/config файла с deployment report.
Isolated writer preflight повторяет ту же byte attestation непосредственно до
writer guard, а Bronze runner делает ещё одну in-process проверку сразу перед
ControlStore guard/write. `kept_paused` принимается только writer-ами exact
coordinator namespace `issue930_(replay|backfill)_aN`: binding interval/attempt,
approved 158-scope SHA, пять closure entities, ingest/Silver run ID и generation
обязаны совпасть; daily/manual профиль в этом состоянии отклоняется. Любая
правка host bind checkout после admission останавливает run до Bronze/Silver
write; не исправлять manifest/report вручную, а выполнить новый deploy из clean
release. Manual daily run fail-closed до создания generation.

Shared-owner fallback использует свежий `kept_paused` deployment report как
read-only trust certificate; `active` report для fallback всегда отклоняется.
`shared_handoff_final.runtime_code_sha256` содержит полный exact manifest shared
bind roots, включая `dags/.airflowignore`. Initializer scheduled master, каждый
Silver writer и Bronze runner сверяют manifest, Git SHA, shared scheduler
container ID, exact report mount и ControlStore admission до guard/write, а
writer повторяет byte attestation после последней normal/salvage операции, пока
guard ещё удерживается. Drift делает run красным и не позволяет seal/publish.
Отсутствующий/stale/writable-only report, manual master run или пересозданный
shared scheduler fail-closed; требуется новый handoff/deploy, а не ручное
редактирование certificate.

Первый issue-930 closure lifecycle выполняется до включения schedule. Для этого
первый deploy запустить с `--keep-paused` и тем же стабильным runtime report
`/durable/path/fotmob-evidence/deployment.json`; все три isolated DAG должны
остаться paused. Затем
предпочтительно выполнить `replay`: он offline перепарсит уже заполненный raw
store в v2 без нового network crawl. Координатор запускает только parent ingest,
передаёт ему reviewed exact 158 scope и пять closure entities (`season`,
`leaderboards`, `matches`, `teams`, `players`), ждёт exact Silver child и
abandon-ит unclaimed candidate только после terminal/quiescence proof:

```bash
python scripts/fotmob_backfill.py run \
  --mode replay \
  --publication-attempt 1 \
  --scopes configs/fotmob/issue-930-scopes.txt \
  --scope-sha256 f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58 \
  --expected-git-sha <full-deployed-40-hex-sha> \
  --max-requests 2000 \
  --max-direct-mib 256 \
  --timeout-seconds 86400 \
  --env-file /protected/path/fotmob.env \
  --deployment-report /durable/path/fotmob-evidence/deployment.json \
  --execute --confirm RUN_FOTMOB_ISSUE_930_BACKFILL \
  --output /durable/path/fotmob-evidence/replay.json
```

Если отчёт требует recovery, не освобождать generation и не запускать новый
attempt вручную:

```bash
python scripts/fotmob_backfill.py recover \
  --publication-attempt 1 \
  --scopes configs/fotmob/issue-930-scopes.txt \
  --scope-sha256 f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58 \
  --expected-git-sha <full-deployed-40-hex-sha> \
  --env-file /protected/path/fotmob.env \
  --deployment-report /durable/path/fotmob-evidence/deployment.json \
  --recovery-report /durable/path/fotmob-evidence/replay.json \
  --execute --confirm RECOVER_FOTMOB_ISSUE_930_BACKFILL \
  --output /durable/path/fotmob-evidence/replay-recovery.json
```

Absent/non-terminal run после неоднозначного trigger сохраняет singleton
generation, пока recovery не докажет writer quiescence и точное terminal
состояние. Новый `--publication-attempt 2` разрешён только после отчёта с
доказанным terminal failure и released generation; дальше номер монотонно
увеличивается. Только если replay доказал конкретно отсутствующие raw inputs,
разрешён отдельный source-refresh запуск той же команды с `--mode backfill`,
отдельным report и attempt в backfill namespace. Backfill/replay не
пересекаются между собой и с rollback synthetic intervals. Они намеренно не
refresh-ят `transfers`: свежий all-history transfer crawl не входит в closure
задачи #930, а parity проверяет frozen legacy union и уже существующие native
transfer данные. После зелёного `phase=abandoned` принятого replay/backfill
сразу выполнить verify и parity ниже, пока исходный kept-paused
deployment остаётся запущен. Лишь после двух зелёных acceptance-
отчётов выполнить обычный deploy без `--keep-paused`: он atomically заменит тот
же `deployment.json` active certificate и только затем включит schedule.
Runtime filename не версионируется и не меняется между admission: audit archive
сохраняется в downstream `replay.json`, `verify.json`, `parity.json` и их
встроенных deployment summaries, а не в альтернативном certificate path.

Не запускать manual daily как предварительную проверку: он может создать
`ready` generation без exact scheduled consumer и удержать singleton lock.
Первым production evidence должен быть реальный scheduled run в 14:00 UTC и
соответствующий ему exact SofaScore consumer, дошедший до finalizer. Не закрывать
cutover до их terminal success; в отчёте зафиксировать release SHA, producer
run ID, Silver run ID, consumer run ID и abandoned/published generation state.

Verify использует versioned файл
`configs/fotmob/issue-930-scopes.txt` (`competition_id=source_season_key`): это
ровно утверждённые для #930 **158 scope** (124 mandatory + добор top-5 и ЧМ).
Acceptance сверяет не только count и переданный hash, но и exact identity set с
этим артефактом; SHA-256 самого артефакта также закреплён в коде. Исторический
обход полного каталога (~493 турнира) не является гейтом #930; не подменять им
acceptance scope этого cutover.

Parity имеет отдельный, исполнимый контракт: exact пять legacy-consumer лиг за
сезон 2025/26 находятся в
`deploy/fotmob/issue-930-parity-scopes.json`. JSON дополнительно содержит
`legacy_league` и `legacy_season`. Международные mandatory scope не имеют
эквивалента в замороженном legacy и поэтому не должны искусственно входить в
parity; их полноту закрывает verify 158/158.

Обе команды принимают один и тот же byte-exact lifecycle-отчёт на
все 158 scope, даже если parity сравнивает только пять лиг. Acceptance
требует `passed=true`, `phase=abandoned`, `recovery_required=false`,
exact deployment ID/Git SHA, утверждённый 158-scope artifact, пять closure
entities и abandoned/released/unpublished publication state. Plan signature,
начало ingest и native runner generation извлекаются из этого отчёта;
отдельные operator knobs для них не допускаются. Команды запускаются
на deployment host, где доступны
Docker CLI, release checkout и host-reachable Trino endpoint. `--env-file` — это
env isolated Compose, а отдельный `--trino-env-file` содержит только `TRINO_*`
с адресом, достижимым с host (Docker DNS `trino` здесь обычно непригоден).
Ambient `TRINO_*` не считаются доказательством и заменяются explicit host-env:

```bash
python scripts/fotmob_acceptance.py verify \
  --scopes configs/fotmob/issue-930-scopes.txt \
  --scope-sha256 f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58 \
  --lifecycle-report /durable/path/fotmob-evidence/replay.json \
  --env-file /protected/path/fotmob.env \
  --trino-env-file /protected/path/fotmob-host-trino.env \
  --deployment-report /durable/path/fotmob-evidence/deployment.json \
  --output /durable/path/fotmob-evidence/verify.json

python scripts/fotmob_acceptance.py parity \
  --scopes deploy/fotmob/issue-930-parity-scopes.json \
  --scope-sha256 2fceb11fd69dcd136f4879b6dad85193924b1a7d7484cf00fc9f7f4a7305568d \
  --lifecycle-report /durable/path/fotmob-evidence/replay.json \
  --env-file /protected/path/fotmob.env \
  --trino-env-file /protected/path/fotmob-host-trino.env \
  --deployment-report /durable/path/fotmob-evidence/deployment.json \
  --output /durable/path/fotmob-evidence/parity.json
```

До и после SQL обе команды повторно доказывают current Compose service IDs,
container/image/nonce/mount identity, clean release/projection, exact durable
deployment marker в Trino и exact abandoned generation в ControlStore. JSON сохраняет
SHA-256 и компактное summary lifecycle-отчёта вместе с двумя live
ControlStore proofs. Команды fail-closed и возвращают ненулевой exit code при
SQL error, отсутствующем результате или красном check. Verify проверяет:

- completion каждого target scope с точным lifecycle runner run ID, plan
  signature и timestamp,
  полным набором expected counts/identity hashes, а также совпадение
  `coverage_hash` с их каноническим содержимым;
- независимый пересчёт count и identity hash для leaderboards, finished
  matches, teams и players из точных physical batches, выбранных v2
  manifest на момент completion каждого scope;
- latest target manifests только candidate runs без
  retry/schema-drift/review-required состояний;
- нулевой `proxy_bytes` только в candidate lineage;
- отсутствие unknown/invalid/duplicate field-inventory rows в утверждённых
  scope и global snapshot targets; inventory может сохранять committed v1
  batch identity из-за run-persistent дедупликации;
- существование всех current views, уникальность natural keys и наличие
  committed v1/v2 manifest для каждой строки (v1 разрешён только как
  предусмотренный rolling fallback; scope-completion остаётся строго v2).

Parity требует exact set equality для matches, match payloads и standings;
roster покрывает минимум 90% legacy `(team_id, player_id)` и выводит явные
diff samples; каждый legacy transfer identity должен присутствовать в итоговой
Silver-таблице. JSON валиден как обычный документ, counts записываются числами.

До закрытия задачи также приложить evidence доставки тестового Telegram alert и
успешного scheduled daily. Наличие токена в окружении само по себе не является
доказательством доставки.

## Optional cleanup staging и field inventory (#994)

Cleanup не является closure gate задачи #930. Compaction field inventory и
удаление staging вынесены в follow-up #994; до его отдельного review объекты
сохраняются. Если #994 выполняется позже, writers ставятся на паузу, cleanup
исполняется только по reviewed plan, затем обычный deploy повторяет admission и
unpause, а verify/parity запускаются заново.

Планирование всегда read-only:

```bash
python scripts/fotmob_cleanup.py plan \
  --older-than-hours 24 \
  --trino-env-file /protected/path/fotmob-host-trino.env \
  --output /durable/path/fotmob-evidence/cleanup-plan.json
sha256sum /durable/path/fotmob-evidence/cleanup-plan.json
```

План содержит только точные таблицы формата `fotmob_*__stg_*`, их row count и
последний Iceberg snapshot. Неизвестные имена и свежие таблицы исключаются.
Field inventory компактизируется через отдельную shadow table и swap только
если найдены дубликаты natural key.

Перед execute остановить writers командой rollback `pause`, проверить JSON и
передать его как evidence. Execute разрешён только с byte-exact SHA-256
просмотренного плана:

```bash
python scripts/fotmob_cleanup.py execute \
  --plan /durable/path/fotmob-evidence/cleanup-plan.json \
  --plan-sha256 <reviewed-sha256> \
  --pause-evidence /durable/path/fotmob-evidence/rollback-pause.json \
  --env-file /protected/path/fotmob.env \
  --deployment-report /durable/path/fotmob-evidence/deployment.json \
  --project fotmob-airflow \
  --release-sha <full-deployed-40-hex-sha> \
  --trino-env-file /protected/path/fotmob-host-trino.env \
  --confirm EXECUTE_REVIEWED_FOTMOB_CLEANUP \
  --output /durable/path/fotmob-evidence/cleanup-result.json
```

Инструмент live повторно сверяет paused/no-active state, exact deployment
container/image/mount identity, затем останавливает isolated scheduler. Перед
первым DDL и перед зелёным завершением он через byte-exact shared runtime
вызывает read-only `assert_no_active_publication_generation(source='fotmob')`.
Активный writer или consumer publication generation блокирует cleanup; команда
никогда не освобождает чужой lease. После этого повторно сверяются schema, row
counts и snapshots. При drift операция останавливается до первого `DROP`.
Нельзя подменять plan wildcard-именами или использовать evidence старше часа.

Inventory swap двухфазный: promoted snapshot сначала сохраняется в атомарный
`fsync` journal с `phase=inventory_promoted_backup_retained`, и только затем
удаляется точный reviewed backup. После crash повтор той же команды с теми же
plan bytes/SHA проверяет journal, snapshot и содержимое compaction и безопасно
завершает backup DROP; иной backup по совпавшему имени не принимается. Scheduler
остаётся остановленным и после success, и после ошибки cleanup; обязательный
следующий шаг — обычный deploy с полным admission.

## Rollback

Native Bronze и raw cache при rollback не удаляются. Сначала сохранить план:

```bash
python scripts/fotmob_rollback.py plan \
  --env-file /protected/path/fotmob.env \
  --deployment-report /durable/path/fotmob-evidence/deployment.json \
  --output /durable/path/fotmob-evidence/rollback-plan.json
```

Далее выполнить строго в указанном порядке:

1. Поставить writers на паузу и убедиться, что active runs отсутствуют:

   ```bash
   python scripts/fotmob_rollback.py pause \
     --env-file /protected/path/fotmob.env \
     --deployment-report /durable/path/fotmob-evidence/deployment.json \
     --execute --confirm PAUSE_FOTMOB_WRITERS \
     --output /durable/path/fotmob-evidence/rollback-pause.json
   ```

   Pause helper пытается поставить на паузу все три DAG независимо от ошибки
   отдельного Airflow CLI вызова (daily всегда последний и всегда attempted),
   затем выполняет единый metadata snapshot pause/`queued`/`running` state и
   возвращает агрегированную ошибку. Частичный CLI success не является
   разрешением продолжать rollback.

2. Развернуть reviewed immutable commit, который возвращает consumers на
   frozen legacy Bronze, обязательно с `deploy.py --keep-paused` и стабильным
   `--report .../deployment.json`. Deploy atomically заменяет текущий runtime
   certificate; отдельное rollback-имя несовместимо с exact path, закреплённым
   в shared scheduler. Откат SQL должен быть выполнен до
   любых изменений native storage. Все runtime bind paths восстанавливаются из
   этого deployment report; вручную их угадывать нельзя.
3. Создать synthetic publication generation и запустить ровно один fenced
   Silver/DQ run через rollback coordinator. Команда сама записывает durable
   write-ahead identity до acquire, использует детерминированный run ID,
   временно unpause только Silver, передаёт exact publication conf и снова
   доказывает pause/no-active. После terminal success generation seal-ится и
   безопасно abandon-ится; native данные не публикуются потребителю:

   ```bash
   python scripts/fotmob_rollback.py run-silver \
     --env-file /protected/path/fotmob.env \
     --deployment-report /durable/path/fotmob-evidence/deployment.json \
     --expected-consumer-sha <full-40-hex-sha> \
     --publication-attempt 1 \
     --execute --confirm RUN_FOTMOB_ROLLBACK_VALIDATION_SILVER \
     --timeout-seconds 43200 \
     --output /durable/path/fotmob-evidence/rollback-publication.json
   ```

   Если отчёт имеет `phase=acquire_ambiguous` или
   `lock_retained_pending_terminal_proof`, не освобождать lock вручную. После
   проверки точного Silver run выполнить recovery:

   ```bash
   python scripts/fotmob_rollback.py recover-publication \
     --env-file /protected/path/fotmob.env \
     --deployment-report /durable/path/fotmob-evidence/deployment.json \
     --publication-report /durable/path/fotmob-evidence/rollback-publication.json \
     --publication-attempt 1 \
     --execute --confirm RECOVER_FOTMOB_ROLLBACK_PUBLICATION \
     --output /durable/path/fotmob-evidence/rollback-publication-recovery.json
   ```

   Non-terminal run всегда сохраняет lock. Отсутствующий exact run позволяет
   снять lock только если write-ahead phase доказывает pre-trigger окно, все
   writers paused/no-active, а control DB содержит exact active `writing`
   generation; после `silver_running` отсутствие остаётся ambiguous. Terminal
   failure позволяет безопасно освободить generation, но следующий
   `run-silver` обязан использовать увеличенный `--publication-attempt 2`
   (далее 3, 4, ...). Если pre-acquire recovery доказывает, что и generation,
   и Silver run отсутствуют (`phase=no_generation_acquired`), тот же attempt
   можно повторить. Recovery и validate всегда получают тот же attempt, что
   указан в их publication report. Только terminal success с exact candidate
   позволяет seal и abandon; для validate использовать последний зелёный
   publication report.

4. Подтвердить revision, paused writers, успешный Silver/DQ run и доступность
   всех девяти legacy-таблиц:

   ```bash
   python scripts/fotmob_rollback.py validate \
     --env-file /protected/path/fotmob.env \
     --trino-env-file /protected/path/fotmob-host-trino.env \
     --deployment-report /durable/path/fotmob-evidence/deployment.json \
     --publication-report /durable/path/fotmob-evidence/rollback-publication.json \
     --expected-consumer-sha <full-40-hex-sha> \
     --publication-attempt 1 \
     --silver-run-id <run-id-from-publication-report> \
     --output /durable/path/fotmob-evidence/rollback-validate.json
   ```

Validate принимает только Silver run со `start_date` не раньше текущего
`deployment.json.generated_at`, проверяет отсутствие и `running`, и
`queued` runs, exact deployed SHA и exact Trino deployment marker до/после
чтения legacy. Он также требует exact зелёный rollback-publication report,
совпадающий candidate digest и durable `abandoned`/inactive generation в общей
control DB. До проверки и в самом конце validate требует отсутствие active
FotMob publication writer/consumer, re-attesting shared code, SHA и одинаковый
control URI; lease при этом не освобождается. Старый зелёный или прямой manual
run без synthetic binding не является rollback evidence.

Если после pause остался running/queued run, не убивать его и не переключать
consumers: дождаться terminal state либо отдельно расследовать зависшую запись.
Возвращать ownership shared scheduler можно только после доказанного pause и
нулевых active runs isolated stack. Сначала pause isolated и дождаться
quiescence, затем непосредственно перед ownership flip выполнить новый deploy
exact release с `--keep-paused`, теми же `--evidence-dir`/`--report` и exact
shared scheduler container. Проверить, что свежий report имеет
`activation_state=kept_paused`, содержит все три exact DAG в `paused` и пустой
`unpaused`; старый `active` report не является fallback admission. Только после
этого установить shared Variable `fotmob_schedule_owner=shared`.
`FOTMOB_SHARED_ADMISSION_HOST_DIR` должен оставаться read-only mount exact
evidence directory, а report path — совпадать с
`FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH`; deploy и scheduled master проверяют это
fail-closed. После устранения инцидента
возвращать native traffic только новым deploy с теми же admission и acceptance
gates.
