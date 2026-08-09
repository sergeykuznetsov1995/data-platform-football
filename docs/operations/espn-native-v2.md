# ESPN Native Bronze v2: guarded release and compact6 runbook

Этот runbook задаёт один fail-closed порядок для release deploy, полного
181-scope обновления, compact6 и последующей проверки E3/xref/Gold. Legacy
остаётся только immutable rollback evidence: процедура не удаляет legacy,
native generations, archive manifests, journals или raw. Все изменения
append-only либо recoverable через явно зафиксированный checkpoint.

Никакая команда из документа не является разрешением на production mutation.
До Task 6 выполняются только offline tests и построение планов. Production
`apply`, Airflow triggers, compact6 и rollback требуют отдельного peer review
точных canonical bytes и назначенного окна.

## Неподвижные границы

- Работать только из immutable detached release root. Сохранить exact
  40-символьный commit, release-tree SHA-256, DagBag SHA-256, Compose SHA-256,
  image digest, registry signature, sorted target SHA и каждый URI/SHA evidence.
- Не использовать `latest` как вход. `latest-state.json` никогда не является
  registry запуска: admission использует frozen run-scoped
  `discovery_state_ref`, который связывает exact `male_registry_ref`.
- Selection policy равна `explicit-core-gender-MALE-v1`. Reviewed baseline —
  `2026-08-02: 181 MALE / 38 FEMALE / 1 UNKNOWN`; generated target содержит
  ровно 181 explicit-MALE scope, zero duplicate IDs/slugs и не содержит FEMALE
  или UNKNOWN.
- Runtime — только isolated `deploy/espn/airflow.compose.yaml` с
  `ESPN_ISOLATED_STACK=1`. Fresh projection содержит ровно семь root DAG-ов:
  `dag_ingest_espn`, `dag_monitor_espn`, `dag_discover_espn_registry`,
  `dag_trigger_espn_daily`, `dag_backfill_espn`, `dag_repair_espn` и
  `dag_replay_espn`. `dag_master_pipeline` отсутствует; shared master остаётся
  paused и не имеет active run.
- Dedicated metadata container имеет exact имя
  `espn-airflow-airflow-metadb-1`; UI слушает только `127.0.0.1:8086`.
  `ESPN_AIRFLOW_DATABASE_URL` и `ESPN_CONTROL_DATABASE_URL` обязаны указывать
  на разные connected server/database identity. `airflow-init` проверяет это
  через `scripts/verify_espn_database_topology.py` до migration.
- Перед mutation все семь DAG-ов paused и `zero isolated active DagRuns`:
  отсутствие metadata, ошибка чтения либо любой `queued`/`running` run — hard
  failure. Не использовать broad `docker compose up/down`, `restart`, ручной
  `dag_trigger_espn_daily` или shell pipe как correctness boundary.
- До compact6 runtime работает с `ESPN_BRONZE_LAYOUT_MODE=legacy14`; после
  cutover — только `compact6`. Unknown/mixed layout блокирует readers и writers.

## Release-specific deploy operator

### Подготовка immutable inputs

Создать fresh DagBag projection до планирования. Этот builder создаёт каталог;
сам deploy `plan` его не создаёт.

```bash
export ESPN_RELEASE_ROOT='/absolute/immutable/espn-release-<40hex>'
export ESPN_DAGBAG_ROOT='/absolute/new/espn-dagbag-<40hex>'
export ESPN_ENV_FILE='/protected/espn/espn.env'
export ESPN_RELEASE_COMMIT='<exact-40-hex-commit>'
export ESPN_RELEASE_TREE_SHA256='<exact-packaging-tree-sha256>'
export ESPN_AIRFLOW_IMAGE='registry/airflow@sha256:<64hex>'
export ESPN_POSTGRES_IMAGE='registry/postgres@sha256:<64hex>'
export ESPN_STACK_LOCK_ROOT='/durable/espn/deploy-stack-lock'
export ESPN_DEPLOY_STATE='/durable/espn/deploy/<transition-id>'
export ESPN_BACKUP_ROOT='/durable/espn/backups'
export ESPN_RELEASE_GUARD="$ESPN_RELEASE_ROOT/scripts/espn_release_guard_v1.py"
export ESPN_DOCKER="/usr/bin/docker"
export AIRFLOW_UID="${AIRFLOW_UID:-50000}"
export ESPN_CANARY_STATE_ROOT=/durable/espn/canary-state

# Один раз до первого deploy: source и target bind имеют этот же absolute path.
# Airflow services уже запускаются как ${AIRFLOW_UID}:0; other не получает прав.
install -d -o root -g 0 -m 0770 "$ESPN_CANARY_STATE_ROOT"

/root/.venvs/dpf-test/bin/python scripts/build_espn_dagbag_projection.py \
  --release-root "$ESPN_RELEASE_ROOT" \
  --output "$ESPN_DAGBAG_ROOT"
```

Запакованный `scripts/espn_release_guard_v1.py` — единственный reviewed
strict-read-only release guard. Он валидирует injected phase/attempt/transition/
plan identity, вызывает exact reviewed `/usr/bin/docker` без shell и внутри
digest-pinned metadata container выполняет только explicit read-only PostgreSQL
transaction. Green требует exact seven active metadata DAG IDs, все семь paused
(включая fail-closed обработку SQL `NULL`), отсутствие любого другого metadata
DAG и zero isolated `queued|running` DagRuns. Guard печатает только canonical
secret-safe JSON и весь polling/query budget bounded до 1,740 секунд, то есть
завершается раньше 1,800-секундного outer phase timeout. Deploy принимает exit
status 0 только вместе с единственным canonical report, где совпадают
phase/attempt/transition/plan identity и все четыре checks равны `true`. Его
exact argv не исполняется shell-ом; script и host Docker binary отдельно
перечисляются повторяемым `--guard-artifact`, поэтому plan и каждый spawn
повторно связывают обоих SHA-256. Mutable wrapper или полный rollout probe здесь
запрещены.

### 1. Построить и проверить plan

`deploy/espn/deploy.py plan` строго non-mutating: он не берёт locks, не делает
`mkdir`, не создаёт files/journals, не запускает guards, backup/restore или
Compose. Он читает только reviewed release/DagBag/Compose/guard bytes и печатает
canonical JSON stdout. Перенаправление stdout в новый файл выполняет оператор,
а не команда `plan`.

```bash
/root/.venvs/dpf-test/bin/python deploy/espn/deploy.py plan \
  --transition-id 'issue-1148-release-ordinal001' \
  --release-commit "$ESPN_RELEASE_COMMIT" \
  --release-tree-sha256 "$ESPN_RELEASE_TREE_SHA256" \
  --release-root "$ESPN_RELEASE_ROOT" \
  --dagbag-root "$ESPN_DAGBAG_ROOT" \
  --compose-file "$ESPN_RELEASE_ROOT/deploy/espn/airflow.compose.yaml" \
  --env-file "$ESPN_ENV_FILE" \
  --stack-lock-root "$ESPN_STACK_LOCK_ROOT" \
  --state-root "$ESPN_DEPLOY_STATE" \
  --backup-root "$ESPN_BACKUP_ROOT" \
  --airflow-image "$ESPN_AIRFLOW_IMAGE" \
  --postgres-image "$ESPN_POSTGRES_IMAGE" \
  --layout-mode legacy14 \
  --guard-argv-json \
    "[\"/root/.venvs/dpf-test/bin/python\",\"-B\",\"${ESPN_RELEASE_GUARD}\",\"guard\",\"--docker-path\",\"${ESPN_DOCKER}\",\"--poll-seconds\",\"15\",\"--max-wait-seconds\",\"1740\"]" \
  --guard-artifact "$ESPN_RELEASE_GUARD" \
  --guard-artifact "$ESPN_DOCKER" \
  > /durable/espn/review/issue-1148-release-plan.json
```

Peer review сверяет `mutates=false`, все absolute paths, exact image digests,
release/DagBag/Compose/guard/Docker/operator hashes, layout, commands, backup target,
шесть phases и canonical `plan_sha256`. `release_commit` сверяется с внешней
immutable packaging attestation для того же release root/tree; одно лишь
40-hex значение не считается provenance proof. Plan должен воспроизводиться
byte-for-byte.
`stack_lock_root` — один фиксированный owner-only root для всего Compose project
`espn-airflow`; он не меняется между transition IDs и находится вне их state
roots. Именно этот global lock сериализует любые apply/resume одного stack.
Не редактировать JSON вручную и не извлекать SHA через `tee`/pipeline.
Reviewed commands обязаны содержать `--force-recreate` для init/runtime и
bounded Compose `--wait-timeout`; обычный restart или unbounded wait запрещён.

### 2. Apply или resume exact plan

Внести в change record exact SHA из peer-reviewed plan, затем передать и plan,
и SHA. Другие runtime arguments у mutating modes отсутствуют.

```bash
export ESPN_DEPLOY_PLAN='/durable/espn/review/issue-1148-release-plan.json'
export ESPN_DEPLOY_PLAN_SHA256='<exact-plan_sha256-from-reviewed-json>'

/root/.venvs/dpf-test/bin/python deploy/espn/deploy.py apply \
  --plan "$ESPN_DEPLOY_PLAN" \
  --plan-sha256 "$ESPN_DEPLOY_PLAN_SHA256"
```

После interruption не запускать новый apply и не менять plan. Продолжить только
так:

```bash
/root/.venvs/dpf-test/bin/python deploy/espn/deploy.py resume \
  --plan "$ESPN_DEPLOY_PLAN" \
  --plan-sha256 "$ESPN_DEPLOY_PLAN_SHA256"
```

Operator выполняет guards в порядке `initial_state`, `pre_backup`,
`pre_checkpoint_mutation`, `pre_airflow_init`, `pre_recreate`, `post_deploy`.
Для каждого guard checksummed `guard-attempt-journal.json` отдельно от
checksummed transition journal хранит `started|succeeded|failed`, attempt,
duration, физический fingerprint и SHA owned regular log. Оба журнала взаимно
связаны transition/guard event SHA, transition ID и exact plan SHA. Completed
attempt identities immutable; `started-only` phase после crash запускается как
новый attempt, не переписывая старый record.

Каждый guard ограничен **1,800 секунд**, а весь deploy — **10,800 секунд**,
включая static preflight, backup, full disposable restore proof, checkpoint,
`airflow-init`, Compose recreation и final evidence. Child запускается отдельной
process group; timeout посылает SIGTERM, затем SIGKILL всей группе. stdout/stderr
никогда не являются pipe: они идут прямо в owner-only regular logs. Durable
heartbeat/ETA сохраняется с интервалом не более 60 секунд и на границах
операций. Console EPIPE не влияет на status; источники истины — checksummed
journals, checkpoint, backup/TOC/restore proof и immutable result.
Перед result operator повторно валидирует SHA/ownership каждого successful
Compose action log и связывает exact checksummed heartbeat path/SHA с result.
Post-deploy fingerprint требует digest-pinned images, exact UI bind и все шесть
release/DagBag mounts как `Type=bind`, `RW=false`; missing/RW mount блокирует
завершение.

До продолжения убедиться, что checkpoint связывает exact dump/TOC SHA и успешный
full restore в disposable `network=none` tmpfs container. Отсутствующий restore
proof, повреждённый checksum, unsafe ownership/mode, plan/hash drift или
неполная journal pair блокируют deployment.

## Read-only rollout probe и pause posture

Версионный probe не открывает writers и не делает DDL/migration. Task 6 host
adapter собирает read-only snapshot; repository probe только проверяет input и
печатает независимые результаты. Запуск из immutable release:

```bash
/root/.venvs/dpf-test/bin/python scripts/espn_rollout_probe_v1.py \
  --snapshot /protected/read-only/espn-rollout-snapshot.json \
  --observed-at '<aware-ISO8601>'
```

Ожидать `kind=espn-rollout-probe-v1`, `status=ok`, 14 независимых result codes и
`counts.fail=0`, `counts.unknown=0`. Любой `fail|unknown` — hard failure; один
зелёный check не скрывает другой красный.

Нормальная green posture — **все семь DAG-ов paused**. Единственное green
исключение — arm window `13:50–14:15 UTC`. В нём разрешён только ordered prefix
`ingest → monitor → discovery → parent`: следующий DAG можно unpause лишь после
read-only подтверждения предыдущего. После scheduler creation exact parent run
parent снова paused; первые три могут оставаться unpaused только пока его
`exact derived child` находится `queued|running`. После terminal child немедленно
вернуть все семь DAG-ов paused. Вне window, при неизвестном prerequisite,
лишнем unpaused DAG, wrong parent/child identity или failed child posture hard
red. Утренний report 07:00 этого окна не видит, поэтому Task 6 устанавливает
hourly observer; morning report даёт лишь next-day detection.

## Gated production ceremony

Ни один следующий шаг не начинается без immutable green evidence предыдущего.
Именно этот numbered order является rollout contract.

### 1. Reviewed deploy plan and restore proof

Проверить shared-owner fence, all-seven-paused и zero active runs; затем
peer-review exact deploy plan и применить его единожды. Сохранить canonical plan,
`plan_sha256`, transition journal, отдельный guard-attempt journal, heartbeat,
backup, TOC, full restore proof и result. Probe должен подтвердить exact metadb,
`127.0.0.1:8086`, scheduler/metadb healthy, exact seven-DAG inventory и
`legacy14`. Полный rollout probe ещё может быть red на campaign/receipt gates,
которые создаются следующими шагами; это ожидаемо и не превращается в green.
`airflow-init`/recreate вручную не повторять в обход operator.

### 2. Fresh campaign ordinal001

Получить fresh immutable discovery state и registry review для нового release.
Начать с green all-seven-paused/zero-active evidence, затем в bounded reviewed
maintenance interval временно unpause только `dag_discover_espn_registry`,
создать exact manual discovery run, дождаться terminal publish и немедленно
снова pause/подтвердить zero active. Probe ожидаемо hard-red только внутри этого
interval; до и после он обязан вернуть all-seven-paused green.
Сохранить `candidate_ref`, `male_registry_ref`, `discovery_state_ref`, registry
signature и exact sorted 181 target SHA. Claim campaign identity равен
`(release_commit, release_tree_sha256, registry_signature,
target_scope_sha256)` и для нового release обязан быть `ordinal001`.
`guard_only=True` не расходует ordinal; active/successful/malformed campaign,
old-release claim, drift или ordinal002 без immutable predecessor failure
блокируют rollout. Admission использует только exact single-use
`ESPN_CANARY_CLAIM_URI`/`ESPN_CANARY_CLAIM_SHA256`; consumption и finish refs
append-only и не переиспользуются. На этом шаге claim pair сохраняется как
immutable evidence, но **не устанавливается** в runtime env: non-canary
reconciliation отвергает любой присутствующий canary claim.

`deploy/espn/airflow.compose.yaml` bind-mount-ит
`/durable/espn/canary-state` в тот же путь для всех Airflow roles и передаёт
`ESPN_CANARY_STATE_ROOT`. Поэтому host operator и пересозданный scheduler читают
одинаковый absolute `file://` URI без переписывания namespace. Каталог заранее
создан `root:0` mode `0770`; mutable ledger имеет mode `0660`, evidence-каталог
— sticky mode `1770`, а immutable evidence-файлы — mode `0440`. Поэтому host
root и Airflow `${AIRFLOW_UID}:0` используют один inode namespace, scheduler
может атомарно добавить свой consumption marker, но не изменить или удалить
root-owned claim/finish evidence; other не имеет доступа. Docker не создаёт
каталог автоматически.
CLI принимает только один canonical ledger
`$ESPN_CANARY_STATE_ROOT/campaigns.json`: alternate/nested path, symlink и
non-regular leaf hard-fail, а `--guard-only` не создаёт каталогов или файлов.
Claim transition fsync-ит новый evidence-каталог в state root;
immutable evidence раньше active ledger. Existing marker при retry/recover заново
проверяется через no-follow regular-file descriptor и fsync-ится вместе с
evidence-каталогом/state root до любого ledger promotion.
После SIGKILL между любыми persistence boundaries единственный разрешённый
путь — `recover`; повторный `claim`, включая `--guard-only`, fail closed.

Из immutable release root задать exact reviewed inputs. Target file содержит
JSON array либо объект с `target_scope_ids`; state ledger не является public
Bronze object. Сначала выполнить read-only guard, затем ровно один consuming
claim. Redirect targets должны быть новыми owner-only файлами; не использовать
pipe/`tee` как correctness boundary.

```bash
umask 077
set -o noclobber
export ESPN_CANARY_TARGETS='/durable/espn/canary/exact-181-target.json'
export ESPN_CANARY_LEDGER="$ESPN_CANARY_STATE_ROOT/campaigns.json"
export ESPN_CANARY_GUARD_RESULT='/durable/espn/canary/ordinal001-guard.json'
export ESPN_CANARY_CLAIM_RESULT='/durable/espn/canary/ordinal001-claim.json'

/root/.venvs/dpf-test/bin/python -m scripts.espn_canary_campaign claim \
  --ledger-path "$ESPN_CANARY_LEDGER" \
  --release-commit "$ESPN_RELEASE_COMMIT" \
  --release-tree-sha256 "$ESPN_RELEASE_TREE_SHA256" \
  --registry-signature '<exact-registry-signature>' \
  --target-scopes "$ESPN_CANARY_TARGETS" \
  --guard-only > "$ESPN_CANARY_GUARD_RESULT"

/root/.venvs/dpf-test/bin/python -m scripts.espn_canary_campaign claim \
  --ledger-path "$ESPN_CANARY_LEDGER" \
  --release-commit "$ESPN_RELEASE_COMMIT" \
  --release-tree-sha256 "$ESPN_RELEASE_TREE_SHA256" \
  --registry-signature '<exact-registry-signature>' \
  --target-scopes "$ESPN_CANARY_TARGETS" \
  > "$ESPN_CANARY_CLAIM_RESULT"
```

Peer-review canonical claim result и exact nested `claim_ref`. Только затем
скопировать его URI/SHA в protected env как complete pair; result file или
ledger path сами по себе admission не дают. Если claim process был прерван и
terminal stdout неизвестен, не запускать claim снова. Сохранить recovery output:

```bash
export ESPN_CANARY_RECOVERY_RESULT='/durable/espn/canary/<new-unique-recovery-id>.json'
/root/.venvs/dpf-test/bin/python -m scripts.espn_canary_campaign recover \
  --ledger-path "$ESPN_CANARY_LEDGER" \
  > "$ESPN_CANARY_RECOVERY_RESULT"
```

`recover` только converges already-durable claim/finish evidence (либо создаёт
claim evidence для legacy ledger-first active state); он не выделяет следующий
ordinal. Corrupt, forked или ambiguous evidence остаётся hard failure.
Same-campaign retry после immutable failed finish использует тот же `claim`
command и получает только `ordinal002`, затем максимум `ordinal003`. Fresh
successor campaign снова получает `ordinal001`, но его claim command обязан
добавить все три reviewed arguments; partial link запрещён:

```bash
  --predecessor-failure-uri '<exact-predecessor-failure-uri>' \
  --predecessor-failure-sha256 '<exact-predecessor-failure-sha256>' \
  --remediation '<reviewed-remediation-summary>'
```

Во всех Airflow roles атомарно задать exact pair
`ESPN_DISCOVERY_STATE_REF_URI`/`ESPN_DISCOVERY_STATE_REF_SHA256`. Frozen ref
остаётся действующим, пока обе переменные атомарно не удалены или не заменены
другой reviewed pair; partial pair и mutable pointer fail closed.

Чтобы pair действительно попал в уже созданные containers, после его установки
в protected env выполнить **второй reviewed deploy transition** через тот же
`deploy/espn/deploy.py plan` → peer review → `apply`, с новым transition ID/state
root, тем же global `stack_lock_root` и exact release/images/layout. Secret-safe
manifest с URI/SHA пары включить в reviewed guard artifacts; guard проверяет
наличие exact pair без печати env values. Сохранить второй backup/restore proof
и все шесть guards. Ручной `compose recreate` запрещён: только этот operator
может перенести pair во все Airflow roles. Шаг 3 не начинается до его immutable
result и read-only подтверждения pair во всех roles.

### 3. Full 181-scope v2→v3 reconciliation

Пока все scheduled DAG-и paused, выполнить manual **full reconciliation** всех
181 frozen scopes через bounded `dag_backfill_espn` cohorts не более 10 scopes.
Перед каждым cohort сохранить green all-paused/zero-active evidence, затем в
явно одобренном maintenance interval временно unpause **только**
`dag_backfill_espn`, создать один exact manual DagRun и держать его unpaused до
terminal state. Такая posture намеренно hard-red для probe и не считается
исключением/green gate; alert override ограничен exact change record и временем.
Сразу после terminal снова pause DAG, подтвердить zero active runs и вернуть
all-seven-paused green до следующего cohort. Остальные шесть не unpause никогда.
Каждый schedule row повторно парсится из exact scoreboard Raw; unchanged Summary
сохраняет exact URI/SHA. Единственный разрешённый bridge —
`espn-native-parser-v2`/`espn-native-runtime-v3` →
`espn-native-parser-v3`/`espn-native-runtime-v4` внутри full reconciliation.
Partial daily, noop поверх v2, mixed/unknown transition запрещены. Pinned
`e12b85a` разрешён только для exact исторического v2 replay и никогда не
возвращается как serving runtime.

После каждого cohort вычислять exact set difference
`target_scope_ids - COMPLETE scope_head_v2`; брать deterministic первые 10
missing scopes, ждать terminal success и zero leases. Count без exact set,
непроверенный head или extra same-signature head не являются evidence.

### 4. Exact 181/181 v3/v4 gate

Strict-read проверить `discovered target == generated registry target == 181
physical COMPLETE heads`, без missing/extra/duplicate. Каждый head связывает
exact immutable snapshot, physical Bronze parity, parser v3/runtime v4 и тот же
registry signature. Любые 180/181, 182/181, mixed version, stale receipt или
registry/target mismatch блокируют canary и scheduler.
Evidence формулируется как `exact 181/181 v3/v4 heads`, а не только count.

### 5. All-181 canary

После exact-181 gate атомарно установить exact single-use
`ESPN_CANARY_CLAIM_URI`/`ESPN_CANARY_CLAIM_SHA256` и выполнить отдельный
**third reviewed deploy transition** с новым transition ID/state root, тем же
global lock/release/images/layout и secret-safe claim-pair manifest в guard
artifacts. Только его immutable result подтверждает, что claim pair попал во
все scheduler/worker roles; direct env injection или ручной recreate запрещены.

Запустить один manual all-scope canary с exact frozen 181-element target array
и fresh ordinal001 claim. Как и reconciliation, сначала сохранить green
all-paused/zero-active evidence, затем временно unpause только reviewed canary
entry DAG `dag_backfill_espn`, держать до terminal и немедленно снова pause.
Во время него probe обязан быть hard-red, а после — снова all-seven-paused green.
Это не per-scope canary. Успех требует для всех 181
`complete_new|noop_revalidated`, final `espn-run-success-receipt-v1`, exact
durable-manifest ref, physical head parity, zero alerts и zero active leases.

После terminal canary и sealed consumption/finish evidence атомарно удалить
**обе** `ESPN_CANARY_CLAIM_*` переменные и выполнить отдельный **fourth reviewed
deploy transition** через тот же operator/global lock. Daily non-canary runs
запрещены, пока read-only guard не подтвердит отсутствие claim pair во всех
roles, all-seven-paused и zero active runs. Partial removal hard-fails.

До удаления runtime claim pair закрыть exact attempt ровно одной из следующих
команд. `ESPN_CANARY_ATTEMPT_ID` берётся byte-for-byte из reviewed claim result,
а terminal URI/SHA указывает на immutable final receipt или failure evidence.

Успех:

```bash
/root/.venvs/dpf-test/bin/python -m scripts.espn_canary_campaign finish \
  --ledger-path "$ESPN_CANARY_LEDGER" \
  --attempt-id "$ESPN_CANARY_ATTEMPT_ID" \
  --terminal-uri "$ESPN_CANARY_TERMINAL_URI" \
  --terminal-sha256 "$ESPN_CANARY_TERMINAL_SHA256" \
  --successful > '/durable/espn/canary/ordinal001-finish-success.json'
```

Fail-closed terminal outcome:

```bash
/root/.venvs/dpf-test/bin/python -m scripts.espn_canary_campaign finish \
  --ledger-path "$ESPN_CANARY_LEDGER" \
  --attempt-id "$ESPN_CANARY_ATTEMPT_ID" \
  --terminal-uri "$ESPN_CANARY_TERMINAL_URI" \
  --terminal-sha256 "$ESPN_CANARY_TERMINAL_SHA256" \
  --failed > '/durable/espn/canary/ordinal001-finish-failed.json'
```

Interruption любого `finish` также продолжается только с теми же identity/ref
arguments и новым result filename либо `recover`; immutable finish marker
делает оба пути idempotent и не позволяет сменить terminal outcome/ref.

Отдельно проверить known scope `19425:2026` и exact Leagues Cup IDs
`401863559`, `401863560`, `401863562`, `401863563`, `401863564`. Для каждого
event/entity допускаются только `captured`, evidence-backed `valid_empty` либо
`not_applicable`: structured failure, missing ID или неизвестная disposition —
red. `played_final && summary_required` требует lineup и matchsheet с обеими
teams либо capability-backed valid-empty; nonfinal явно `not_applicable`.

`rows=0` сам по себе никогда не green. Empty schedule требует unsaturated
schema-valid coverage всех signed windows, правильную competition/season
ownership, отсутствие пропавшего known nonterminal event и вторую более свежую
scheduled observation либо explicit source capability metadata. Fresh noop
strict-read перепроверяет exact COMPLETE generation и подписывает новый
run-evidence; он не двигает `ScopeHead.published_at`.

### 6. Three scheduler-created parent/child receipts

В три соседних UTC-дня использовать только arm-window state machine выше.
Parent создаёт scheduler, не CLI/manual trigger. Сохранить exact parent receipt,
derived-child identity и child final receipt для каждого дня. Три data interval
соприкасаются без gap/overlap; первый и третий start разделены на **exact 48h**.
Все три относятся к одному release/tree/registry/target и имеют explicit
`canary_campaign=null`: после удаления claim pair scheduled daily admission не
может и не должен наследовать canary campaign identity. Ceremony evidence
отдельно связывает эти receipts с ранее sealed successful ordinal001 campaign
artifact, не возвращая `ESPN_CANARY_CLAIM_*` в runtime. Каждый receipt имеет
`complete|noop` qualification для всех 181, zero warnings/alerts/leases и один
validated durable-manifest ref. Manual bootstrap/canary не засчитываются.
После каждого child terminal вернуть все семь DAG-ов paused.

### 7. compact6 ACL and rollback proof

Сначала получить fresh transition/plan-bound run-guard evidence, где ESPN,
legacy, E3, xref и Gold paused и active runs zero. Затем capture exact legacy14
snapshots и immutable global archive manifest; plan review предшествует apply.

```bash
ESPN_BRONZE_LAYOUT_MODE=legacy14 \
/root/.venvs/dpf-test/bin/python scripts/compact_espn_bronze_v2.py plan \
  --plan /durable/espn/compact6/plan.json \
  --capture \
  --transition-id '<exact-transition-id>' \
  --registry-snapshot-uri '<immutable-registry-uri>' \
  --registry-snapshot-sha256 '<registry-sha256>' \
  --registry-signature '<registry-signature>' \
  --target-scopes /durable/espn/compact6/exact-181-target.json \
  --run-guard /durable/espn/compact6/run-guard.json
```

Review exact archive snapshot IDs/counts/multiset hashes, dispositions, six
replacements, 181 physical heads, every rendered step/postcondition,
`plan_sha256`, emergency views and logical rollback. Затем:

```bash
ESPN_BRONZE_LAYOUT_MODE=legacy14 \
/root/.venvs/dpf-test/bin/python scripts/compact_espn_bronze_v2.py apply \
  --plan /durable/espn/compact6/plan.json \
  --manifest /durable/espn/compact6/archive-manifest.json \
  --journal /durable/espn/compact6/journal.json \
  --run-guard /durable/espn/compact6/run-guard.json \
  --execute
```

После interruption использовать ту же plan/manifest/journal triple и
`scripts/compact_espn_bronze_v2.py resume --resume-operation apply --execute`.
Не создавать fork. Подтвердить **exactly six public Bronze objects**: три
explicit-column SECURITY DEFINER canonical views и три physical controls.
Обычные Airflow/analyst roles не читают `iceberg.espn_internal`; platform admin
читает archives/journal. Проверить emergency logical rollback и repromotion на
frozen archive, а не на live legacy main. После cutover задать всем runtime
`ESPN_BRONZE_LAYOUT_MODE=compact6`; e12b85a rollback запрещён.

Изменение protected env само по себе running containers не обновляет. Поэтому
до шага 8 построить новый reviewed deploy-operator plan с
`--layout-mode compact6`, новым transition ID/state root, тем же global
`stack_lock_root`, release и digest-pinned images; затем выполнить только его
`apply`. Сохранить новый backup/full-restore proof, шесть guards и immutable
result. Read-only probe должен подтвердить compact6 exact-six/parity во всех
roles; manual Compose recreate по-прежнему запрещён.

### 8. One post-cutover scheduled cycle

В следующем arm window провести ровно один scheduler-created parent/derived
child cycle уже в compact6. Probe должен показать compact6 layout, exact-six
inventory, internal/public parity, fresh all-181 final receipt, known five IDs,
valid dispositions, zero alerts/leases и затем all-seven-paused posture. До
этого цикла downstream promotion запрещён.

### 9. Six-scope E3/xref/Gold reconciliation

Продвигать только exact mapped allowlist `606:2026`, `700:2026`, `710:2026`,
`720:2026`, `730:2026`, `740:2026`; остальные 175 остаются Bronze-only.
Выполнить E3/xref/Gold reconciliation для exact platform `(league, season)`:
E3 input parity, collision-free aliases, `xref_team`, `xref_match`,
`fct_lineup`, Gold row/hash parity и pair-scoped DQ. Missing mapping, legacy
fallback для этих шести, 175-scope leakage, fanout, orphan или cap/floor из
`181 * 60000` блокируют завершение.

### 10. Rollback and secret-safe security evidence

Сделать reviewed rollback rehearsal до закрытия окна. compact6 rollback читает
только frozen global archive/disposition manifest и пишет durable journal;
никогда не выбирает live legacy main и ничего не удаляет. Сохранить rollback
SQL/hash, emergency-view parity, result и repromotion proof. Для pre-compact
release incident использовать exact deploy checkpoint/backup и отдельно
reviewed restore procedure; не угадывать команды и не менять original plan.
Rollback registry фиксирует `last good immutable discovery-state ref` и
связанный `male_registry_ref`, никогда текущий `latest-state.json`.

Security evidence должно быть `secret-safe`: записывать только имена checked
locations, commit/path, ownership/mode, digest и verdict — никогда token,
password, DSN credential или raw env value. Проверить active PostgreSQL/Trino
secrets против public Git history, backups и logs без печати значений.
Confirmed active external exposure немедленно блокирует production и требует
same-day отдельной rotation; если exposure не подтверждён, сохранить dedicated
tracker и coordinated rotation deadline не позднее семи дней.

## Replay и repair

Replay использует exact `replay_raw_manifest_uri`/SHA из failed или successful
evidence и `dag_replay_espn`. Он не ходит в ESPN, повторно проверяет immutable
Raw bytes и публикует новую generation, не перезаписывая original manifest.
Authentic v1/v2 artifact можно читать для audit, но current execution допускает
только admission v3; исторический v2 replay выполняется pinned e12b85a.

Для Top-5 repair сначала получить read-only input из exact Iceberg snapshots,
затем валидировать полную 50-scope matrix:

```bash
/root/.venvs/dpf-test/bin/python scripts/extract_espn_repair_audit.py \
  --output /durable/espn/top5-audit-input.json
/root/.venvs/dpf-test/bin/python scripts/audit_espn_repair.py \
  --input /durable/espn/top5-audit-input.json \
  --output /durable/espn/top5-repair-queue.json
```

Missing scope тоже входит в queue. Pre-2016 `legacy_untrusted` можно исследовать
и replay, но нельзя автоматически повышать до trusted serving. Старый
`scripts/migrate_espn_native_v2.py` сохраняется для чтения исторических
promotion/rollback evidence, но не заменяет all-181 + compact6 ceremony.

## Alerts и incident response

`dag_monitor_espn` и versioned rollout probe сообщают каждый failure class
отдельно: topology/health, inventory, posture, parent-child identity/state,
receipt, registry/target, physical versions, dispositions, freshness, leases,
known events и serving parity. Unknown — hard, не green. При regression сначала
все семь DAG-ов paused, затем сохраняются exact alert/evidence SHA; replay,
repair или reviewed logical rollback выбирается по фактам. Не перезапускать
capture вслепую и не вызывать writer из probe.

## Retention

- Raw успешного COMPLETE запуска хранить минимум **90 дней**.
- Raw и evidence failed/repair запуска хранить минимум **365 дней**.
- Raw manifests, generation manifests, receipts, claims/consumption/finish,
  deploy/guard/compact journals, archives, cutovers, baselines, health/alert и
  rollback/security reports хранить **бессрочно**.

Cleanup обязан доказать terminal state и retention class. Missing exact replay,
restore или rollback evidence — incident, а не кандидат на очистку.

## Release gates

До production apply сохранить полный вывод без secret values:

```bash
/root/.venvs/dpf-test/bin/pytest tests/unit/scripts/test_espn_release_deploy.py -q
/root/.venvs/dpf-test/bin/pytest tests/unit/scripts/test_espn_rollout_probe_v1.py -q
/root/.venvs/dpf-test/bin/pytest tests/unit/scripts/test_espn_native_runbook.py -q
/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers tests/unit/dags -q
/root/.venvs/dpf-test/bin/pytest tests/unit -k espn -q
/root/.venvs/dpf-test/bin/python scripts/audit_espn_runtime_imports.py
```

Любой non-zero запрещает cutover. Offline gates не заменяют live read-only
probe, exact plan review, restore/rollback rehearsal и peer-reviewed production
evidence.
