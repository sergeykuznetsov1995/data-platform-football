# WhoScored — ежедневный сбор через платный резидентный прокси (Path A, #954)

Этот runbook описывает, как включить **ежедневный** WhoScored-сбор (`dag_ingest_whoscored`)
через платный резидентный прокси, оставив **исторический бэкфилл (`dag_backfill_whoscored`)
жёстко закрытым** («детский замок»). Решение владельца: «Путь А — гнать daily через прокси»
(direct-IP хосту с чистой CF-репутацией добыть не удалось; резидентный прокси CF проходит,
что подтверждено GREEN-канарейкой 2026-07-18).

> ⚠️ Платный трафик = реальные деньги провайдера. Ни один шаг ниже не выполняется
> автоматически: включение — за явным решением владельца, свежей квитанцией и ревью-релизом.

---

## 1. Что изменено в коде (ревью-релиз #954)

Раньше `dag_ingest_whoscored` физически не мог потратить ни байта через платный прокси:
единственный кодовый сентинел `WHOSCORED_FULL_PAID_CRAWL_AVAILABLE = False`
(`scrapers/whoscored/proxy_campaign.py`) отклонял любой non-canary approval в **трёх** местах.

Введён **отдельный третий гейт** для ежедневного сбора — по образцу «у канарейки и полного
crawl намеренно раздельные гейты»:

- `scrapers/whoscored/proxy_campaign.py`
  - `WHOSCORED_DAILY_INGEST_PAID_CRAWL_AVAILABLE = True` (новый code-owned сентинел);
  - `WHOSCORED_DAILY_INGEST_PAID_CRAWL_DAG_IDS = frozenset({WHOSCORED_INGEST_DAG_ID})`
    (whitelist — только `dag_ingest_whoscored`);
  - хелпер `daily_ingest_paid_crawl_allowed(dag_id)` — единый источник логики гейта;
  - `WHOSCORED_FULL_PAID_CRAWL_AVAILABLE` **остаётся `False`** → `dag_backfill_whoscored`
    по-прежнему заблокирован.
- Три места-энфорсера теперь пропускают ingest **или** full-crawl:
  - `_assert_paid_release_gates()` (runner-transport authority),
  - `dags/scripts/whoscored_proxy_runtime.py::resolve_paid_runtime()` (scheduler resolver),
  - `scripts/proxy_filter/filter_proxy.py::create_lease()` (filtering-proxy lease).

Семантика гейта на каждом сайте:

```
if approval == exact canary:      exact measurement-canary contract
elif ingest допущен (новый гейт)  ИЛИ full-crawl доступен:   пропустить
else:                             raise "full paid crawl is disabled …"
```

**Итог по коду:** `dag_ingest_whoscored` допущен к платному пути; `dag_backfill_whoscored`
остаётся закрыт до отдельного решения (флип `WHOSCORED_FULL_PAID_CRAWL_AVAILABLE`).
Тесты: `test_daily_ingest_paid_crawl_gate_admits_ingest_but_not_backfill`
(`tests/unit/scrapers/test_whoscored_proxy_campaign.py`),
`test_daily_ingest_paid_crawl_is_admitted_by_code` и
`test_backfill_paid_crawl_gate_is_code_owned` (`tests/unit/dags/test_whoscored_proxy_runtime.py`),
плюс boundary-проверки в `tests/unit/scripts/test_filter_proxy.py`.

> Код-флип сам по себе **не гонит трафик**: он лишь снимает запрет. Реальный маршрут через
> прокси включается только полным комплектом «канареечного класса» ниже (approval + gateway +
> runtime pins); scheduled run получает authority только через exact pointer, а manual run —
> через explicit DagRun conf.

---

## 1a. Автоматический плановый сбор (issuer + pointer, #954)

До этого изменения даже допущенный к платному пути `dag_ingest_whoscored` мог тратить
прокси **только при ручном триггере** DagRun с `conf {transport_policy: direct_then_paid, …}`:
`resolve_transport_policy` читает политику исключительно из `dag_run.conf`, а плановый
(`scheduled`) запуск идёт с пустым conf → `direct_only`. Владельцу нужен полный автомат
(«даг сам каждый день всё обновляет»). Реализовано без ослабления ни одного wire-гейта
(не более 24 ч validity и строго внутри policy/charter window, привязка к одному
`run_id`, release-пины, замок бэкфилла):

- **Суточный issuer** — `scripts/whoscored_proxy_campaign.py issue-daily-ingest`. По
  owner-signed **charter-v5** (месячный «стоячий ордер»: `order_id`, окно целиком внутри
  provider policy и `valid_until ≤ 62 дн`, exact decimal-byte caps, rollout/wave contract,
  файл 0600) он раз в
  сутки строит и **подписывает** обычный ≤24 ч
  approval на `dag_ingest_whoscored` для КОНКРЕТНОГО планового `run_id`, с аллокациями
  `catalog-discovery` (phase discovery) + по одной `scope-<sha>` на каждый скоуп точного
  signed wave + одной `profiles-daily` (phase capture). Rollout manifest schema-v4 содержит
  полный frozen heavy-first active scope universe и exact
  `{rollout_id, wave_id, max_scopes, require_full_active}`. Допустимы только три
  code-owned контракта: `wave-20/20/false`, `wave-70/70/false`,
  `wave-all/2000/true`; 2000 у `wave-all` — верхняя граница кода, а не ожидаемый размер
  каталога. Charter повторяет эти четыре поля и подписывает SHA всего rollout manifest.
  Planner фиксирует родительский catalog batch, exact due match/preview/profile identities,
  все schedule targets и pagination headroom **для полного active universe**.
  При создании rollout отдельная команда `create-rollout` один раз сортирует exact scope
  workloads по `paid_target_count DESC, scope ASC` и замораживает полный
  `ranked_scope_ids` + `ranked_scope_ids_sha256` вместе с исходным workload evidence.
  Ежедневный planner обновляет exact due targets/`paid_target_count`, но не меняет этот
  порядок: каждая волна берёт фиксированный кумулятивный prefix. Поэтому две scheduled
  попытки одной волны имеют стабильную scope identity даже при изменении backlog.
  Plan schema-v4 несёт стабильный `ranked_scope_ids_sha256`, отдельный изменяемый
  `ranked_workload_sha256`, число и SHA полного sorted active каталога и выбранный prefix.
  Manifest и charter также навсегда фиксируют `runtime_sha256` и `classifier_sha256`, а
  promoted waves — SHA exact content-addressed acceptance chain и terminal receipt предыдущей
  волны. Offline signer заново проверяет подпись charter, его связь с policy/rollout, полный frozen
  ranked universe и exact prefix; planner не задаёт authority. Issuance ledger запрещает
  повторно использовать один `rollout_id` с другим ranking/release pin, начать не с wave-20,
  перескочить волну, откатиться или реактивировать предыдущую волну.
  Перед planner/signer root wrapper передаёт exact текущий `rollout_id` через
  `verify-running --issuance-rollout-id`: admission заново читает content-addressed receipts из ops store и
  воспроизводит metadata-DB/TI/XCom evidence. Для `wave-20` допустим только exact genesis
  без predecessor runs; `wave-70` требует живую пару accepted `wave-20`, а `wave-all` —
  exact цепочку из двух `wave-20` и двух `wave-70` runs. Возвращённые proof, terminal
  receipt и authority обязаны побайтно совпасть с активными manifest/charter до любого spend.
  Более новый scheduled run не может скрыть устаревшую predecessor-пару: каждый такой run
  должен быть terminal и иметь singleton scope-plan XCom именно текущей подписанной волны;
  queued/running run, отсутствие XCom или evidence прежней волны блокируют issuance. После
  начала новой волны её собственные terminal runs закономерно новее predecessor, поэтому
  проверка остаётся wave-aware, а не требует навсегда считать predecessor последним DagRun.
  `max_scopes` и byte budget не приходят из env/CLI: signer выводит их из подписанного
  charter, а effective budget не может превысить `charter.daily_cap_bytes` и
  receipt-bound provider order cap. Gross authority не больше 1 GB, а 5% всегда
  остаются недоступным safety reserve.
- **Pointer-резолюция** — `dags/scripts/whoscored_proxy_runtime.py`. Issuer кладёт рядом с
  approval маленький pointer `POINTER_ROOT/<sha256(run_id)>.json` (0600) с
  `{dag_id, run_id, approval_id, approval_sha256}`. На запуске
  `_scheduled_paid_pins` подхватывает его **только** если `run_type == "scheduled"` И
  `daily_ingest_paid_crawl_allowed(dag_id)` (т.е. только ingest — бэкфилл **никогда**, замок
  цел) И conf **точно пуст**. В production `WHOSCORED_SCHEDULED_PAID_MODE=required`:
  отсутствующий root/pointer, любой непустой DagRun conf, неверный владелец/режим, симлинк
  или mismatch `run_id`/`dag_id` останавливает DagRun **до source work**. Перехода в
  `direct_only` в required mode нет. Ручной DagRun не считается scheduled и сохраняет свой
  отдельный explicit-conf контракт.
- **Устойчивость к дрейфу каталога.** Paid DagRun исполняет только frozen signed wave.
  `deferred_scopes` содержит только не вошедший в текущий prefix остаток подписанного полного
  active universe. Новый active scope вне этого universe меняет exact count/SHA каталога и
  блокирует запуск до выпуска нового rollout; через direct fallback он не запускается.
  Если подписанный scope исчез, стал ineligible или quarantined, DAG сначала
  пишет create-once `quarantine-disappearance.audit` (0600, с SHA), затем fail-loud уходит в
  DAG-level alert. Exact due identities повторно сверяются после discovery и до сетевой
  работы скоупа.
- **Pagination/profile bounds.** Для каждого stage план резервирует страницы 2..100 у 30
  player feeds (page 1 уже входит в structured targets), но обрезает headroom так, чтобы
  полный scope не превысил 5 000 lease targets. При `profile_target_count=0` подписанная
  allocation остаётся минимальной (`lease_limit=1`, `request_limit=2`) для валидного
  fail-closed envelope, а runner получает exact `--profiles-limit 0` и не делает profile
  fetch. Work item профилей — стабильный `profiles-daily`.

**Развёртывание автомата (за владельцем, в деплой-окно):**
1. Scheduler получает только `WHOSCORED_PROXY_APPROVAL_ROOT`, read-only
   `WHOSCORED_SCHEDULED_PAID_POINTER_ROOT` и
   `WHOSCORED_SCHEDULED_PAID_MODE=required`. Статического approval selector нет.
   HMAC-секреты видны только host issuer'у и isolated filter/gateway, но не
   scheduler-раннеру.
2. Установить checked-in `whoscored-daily-issuer.service/.timer`,
   `scripts/whoscored_daily_issuer.sh` и
   `scripts/whoscored_bootstrap_issuer.sh` строго по
   [`whoscored-production.md`](whoscored-production.md). Timer срабатывает в
   09:15 UTC и не делает catch-up. Wrapper допускает запуск только в
   09:00–09:30 UTC, сначала заново аттестует все пять running/healthy сервисов,
   затем planner-контейнером **всегда заново** строит план fixed heavy-first wave во fresh
   root-only RuntimeDirectory. Wrapper замораживает exact schema-v4 plan root-owned копией,
   проверяет строгую матрицу `wave-20/70/all`, code ceiling 2000, exact selected/ranked counts
   и передаёт signer-контейнеру без сети frozen plan вместе с read-only rollout manifest.
   В wrapper нет `MAX_SCOPES`, `TOTAL_BYTES`, `--max-scopes` или `--total-bytes`: offline
   signer выводит wave bound и budget только из hash-bound rollout + подписанного charter
   до публикации approval/pointer/issuance-ledger.
   Поскольку каждый task заново требует ≥6 часов оставшейся validity, ранний
   `valid_until` обязан покрыть от планового старта 10:00 UTC сам 6-hour DagRun,
   ещё 6 часов остатка и 5 минут clock/task margin; иначе signer падает до любой
   публикации.
   Общий UID-50000 plan directory authority не является; signing keys в planner не
   монтируются.
3. **run_id.** Плановый `run_id = "scheduled__" + data_interval_start.isoformat()` (для cron
   `0 10 * * *` это (D−1) 10:00 UTC у запуска, стартующего в D 10:00). Если issuer посчитает
   run_id неверно — pointer ляжет под чужой ключ, а production required-run жёстко упадёт до
   source work (**fail-closed, без ошибочного расхода**). Точную дату-смещение подтверждаем на смоуке
   (сверить фактический `run_id` первого планового прогона с тем, что писал issuer).

---

## 2. Сколько покупать (провайдерский тариф)

Замер основан на GREEN-канарейке (`canary-curlcffi-20260718T062208Z.json`): один матч ≈
матч-центр 1.26 MB + доля schedule ≈ **1.5–2.5 MB** (direct-first curl_cffi, HTML без
JS-подресурсов; FlareSolverr-браузер только для периодического `cf_clearance`).

| Режим | Матчей/день | Трафик/день | Тариф |
|---|---|---|---|
| Полный активный каталог (~200–300 in-season соревнований) | ~150–300 (пики выходных 400–600) | ~0.6–0.8 GB (пики ~1.5–2.5 GB) | **~50 GB/мес** |
| Скромный старт (топ-5…15 лиг) | ~30–60 | ~0.1–0.3 GB | **~20–25 GB/мес** |

Текущий PROXYS.IO order 38950 с gross quota 1 GB используется для ограниченного
production-bootstrap: код навсегда оставляет 5% reserve, поэтому smoke и шесть
ускоренных запусков вместе могут получить не более 950 MB. Перед каждой issuance
issuer суммирует уже выданные ceilings и полный ceiling следующего запуска; до
overspend он останавливается без публикации нового approval и без частичного
расширения authority. Этот заказ не считается
автоматическим разрешением на последующий постоянный daily: его бюджет выбирается
отдельно после фактического billed-byte evidence.

Полный `all_catalog` бэкфилл — **отдельный порядок величины**: ~1–5 TB одноразово
(центр ~2–3 TB, при коде-потолке ≤30 дней / ≤172 800 request-units/сут). Не запускать вместе
с daily; сначала стабилизировать ежедневный сбор.

---

## 3. Порядок включения (за владельцем)

1. **Одобрить код-изменение** как ревью-релиз (оно авторизует провайдерский расход для
   ежедневного ingest; сентинелы code-owned — ни env, ни conf, ни поле approval их не включают).
2. **Подтвердить текущий заказ** и его точные `order_id`/plan tier. Для этого
   bootstrap не покупать и не расширять тариф автоматически: используется только
   receipt-bound gross 1 GB с обязательным 5% reserve. Если шесть frozen планов не
   помещаются в 950 MB, rollout остаётся закрыт до отдельного решения владельца.
3. **Пересобрать образ WhoScored** после код-изменения → build-provenance attestation
   регенерируется как `ready-v1` (content-addressed); затем регенерировать deployment
   attestation (`scripts/generate_whoscored_deployment_attestation.py`, `status=ready-v1`).
   Без пересборки стартовый gate (`whoscored_production_gate.py`) отклонит образ (дерево изменилось).
4. **Свежая (<24 ч) provider quota receipt** текущего заказа, screenshot-bound (order id,
   plan tier, `status=active`, точные десятичные quota/remaining, без прокси-кредов). Прогнать
   deploy-admission: `scripts/whoscored_production_admission.py verify-rendered` → `post-create`
   по всем 5 protected-сервисам (`airflow-scheduler`, `flaresolverr`,
   `flaresolverr_whoscored_paid`, `whoscored_paid_gateway`, `whoscored_proxy_filter`).
   Старый screenshot/receipt повторно не используется, даже если order id и quota
   не изменились.
5. **Подготовить rollout и подписать provider-policy/charter.** Выполнить `create-rollout`
   на точном active catalog: команда вычислит initial exact paid demand, heavy-first порядок
   и его стабильный SHA. Для волн `20 → 70 → all` переиспользовать без изменений те же
   `ranked_scope_ids`/SHA и один `rollout_id`; менять разрешено только wave contract и
   соответствующий `cohort_id`/manifest SHA. Policy
   HMAC-связан с точным SHA свежей
   квитанции и фиксирует provider/order/plan и decimal-byte caps; charter связан
   с policy/rollout, повторяет `rollout_id`, `wave_id`, `max_scopes`,
   `require_full_active`, `ranked_scope_ids_sha256`, immutable release pins и promotion
   proof/terminal receipt SHA, а также ограничивает автоматические суточные
   выдачи. Ручной
   NON-canary approval по-прежнему допустим только с точными release pins.
6. **Deployment env + сервисы:** approval/pointer roots,
   `WHOSCORED_PAID_GATEWAY_URL=http://whoscored_paid_gateway:8898`,
   `WHOSCORED_PAID_GATEWAY_TOKEN` (≥32 символа), `WHOSCORED_PROXY_APPROVAL_HMAC_SECRET`,
   `PROXY_POOL_JSON`, exact `WHOSCORED_PROVIDER_ORDER_ID`,
   `WHOSCORED_PROVIDER_POLICY_SHA256`, exact gross
   `WHOSCORED_PROVIDER_ORDER_CAP_BYTES`, `--daily-budget-bytes` и
   `--max-lease-bytes`;
   изолированный paid gateway + filter-proxy должны идти с совпадающим `WHOSCORED_PROXY_RUNTIME_SHA256`.
   Paid trio и все четыре paid-сети принадлежат только проекту `whoscored-gw`;
   общий project использует только external paid API network. Cutover выполняется
   exact-label ceremony без shared `down`/`--remove-orphans`.
7. **Плановый smoke DagRun** должен иметь exact пустой conf и получает paid authority только
   из точного run-ID pointer. В `required` mode любой conf и отсутствующий/повреждённый
   pointer блокируют запуск до source work. Отдельный manual DagRun по-прежнему можно явно
   запустить с
   `conf {transport_policy: direct_then_paid, paid_approval_id: <id>, paid_approval_sha256: <lowercase hex>}`;
   он не подменяет проверку scheduled автомата.

### Ускоренный bootstrap из шести запусков

Rollout и каждый wave-specific charter повторяют owner-signed
`acceptance_mode=accelerated-bootstrap-v1`, exact шесть consecutive backdated
10:00 UTC slots, `capacity_receipt_sha256` и `provider_order_cap_bytes`. Все шесть
слотов должны уже находиться в прошлом в момент создания rollout. Порядок
неизменяем: `20, 20, 70, 70, all, all`; manual run IDs и другие ISO-формы не
принимаются.

До включения timetable один раз публикуется стабильная projection:

```bash
/usr/local/libexec/whoscored-bootstrap-issuer publish
```

`pointer_root/bootstrap.json` содержит stable rollout/provider fields, canonical
content SHA и HMAC. Он намеренно не содержит wave-specific `charter_sha256`, поэтому
не меняется при promotion; каждый run pointer и approval отдельно связывают текущий
charter. Stable projection создаётся один раз как `root:root` mode `0440`; это не
меняет контракт per-run pointers и approvals, которые остаются UID `50000`, GID `0`,
mode `0600`.

Coordinator выдаёт конкретный слот вне daily-окна отдельным путём:

```bash
RUN_ID='scheduled__YYYY-MM-DDT10:00:00+00:00'
/usr/local/libexec/whoscored-bootstrap-issuer run "$RUN_ID"
```

Root-launcher сериализует все bootstrap-запуски и включение daily timer
одним directory-inode `flock`. Под этой блокировкой bootstrap-путь
сначала отключает timer, проверяет timer и service как inactive,
атомарно публикует root-owned one-shot
request и удаляет его своим `EXIT` trap при любом исходе, включая отказ
dependency до старта unit. `ExecStopPost` остаётся второй линией защиты. После
успешного `systemctl start` launcher отдельно требует отсутствия request. Поэтому
следующий timer без нового request всегда возвращается к default `daily`. Включать
его разрешено только через
`/usr/local/libexec/whoscored-bootstrap-issuer enable-daily`: эта команда держит
тот же lock во время проверок отсутствия request и `systemctl enable --now`.
Прямое включение timer и запуск wrapper напрямую
не является поддерживаемым production-путём, потому что `RuntimeDirectory` и
три `LoadCredential` создаёт только unit. Timer обязан оставаться disabled, а
oneshot service — inactive до каждого запуска launcher.

`issue-bootstrap-ingest` принимает только текущий exact slot/wave и только в
последовательном порядке. Следующий slot запрещено выдавать заранее:
каждый bootstrap DagRun сам ставит DAG на паузу и публикует новое catalog
generation; план N+1 создаётся только после terminal-green N и связывается
с его новым exact parent. Duplicate issuance возвращает тот же immutable artifact.
Обычный `issue-daily-ingest` остаётся отдельным, требует ожидаемый daily run ID и
окно 09:00–09:30 UTC.

Scheduled approval сохраняет schema-v3, но его ephemeral `scheduled_authority`
намеренно стал строже: старый v3 без bootstrap/order fields отклоняется. Manual
measurement approval остаётся отдельным schema-v2 и не меняет wire contract.

### Безопасная смена волн

`create-rollout` вызывается ровно один раз, для первой волны. Переходы выполняются только
`promote-rollout`: команда полностью валидирует исходный schema-v4 manifest, не подключается
к live catalog и переносит `rollout_id`, frozen `ranked_scope_ids`, stable SHA и исходный
workload basis без изменений. Разрешены только соседние переходы:

```bash
python scripts/whoscored_proxy_campaign.py create-rollout \
  --rollout-id production-2026-07 \
  --cohort-id production-2026-07-wave-20 \
  --wave-id wave-20 \
  --runtime-sha256 "$WHOSCORED_RUNTIME_SHA256" \
  --classifier-sha256 "$WHOSCORED_CLASSIFIER_SHA256" \
  --capacity-receipt-sha256 "$CAPACITY_RECEIPT_SHA256" \
  --provider-order-cap-bytes 1000000000 \
  --bootstrap-start-logical-date 'YYYY-MM-DDT10:00:00Z' \
  --output rollout-wave-20.json

python scripts/whoscored_proxy_campaign.py promote-rollout \
  --input rollout-wave-20.json \
  --cohort-id production-2026-07-wave-70 \
  --wave-id wave-70 \
  --acceptance-receipt "$WAVE20_RUN1_RECEIPT" \
  --acceptance-receipt "$WAVE20_RUN2_ACCEPTED_RECEIPT" \
  --output rollout-wave-70.json

python scripts/whoscored_proxy_campaign.py promote-rollout \
  --input rollout-wave-70.json \
  --cohort-id production-2026-07-wave-all \
  --wave-id wave-all \
  --acceptance-receipt "$WAVE20_RUN1_RECEIPT" \
  --acceptance-receipt "$WAVE20_RUN2_ACCEPTED_RECEIPT" \
  --acceptance-receipt "$WAVE70_RUN1_RECEIPT" \
  --acceptance-receipt "$WAVE70_RUN2_ACCEPTED_RECEIPT" \
  --output rollout-wave-all.json
```

Каждый receipt input — root-owned/private файл с именем `<content-sha256>.json` из exact
immutable acceptance prefix этого `rollout_id`. Команда требует ровно два receipt для
20→70 и ровно четыре для 70→all, полностью replay-ит accepted chain, сверяет source
cohort/rank/catalog/release и записывает новый `promotion_acceptance_sha256` вместе с exact
`promotion_terminal_receipt_sha256`; live catalog она не читает.

Локальные файлы нужны только для построения promoted manifest и сами по себе не разрешают
provider spend. На каждой ежедневной выдаче admission независимо выбирает ту же exact
цепочку по подписанному terminal digest из живого ops store, пересчитывает тот же promotion
proof общим helper и сверяет все run witnesses с Airflow metadata DB/XCom. Отсутствующий,
подменённый или только локально существующий receipt останавливает issuer до planner/signer.

Команда печатает новый canonical `cohort_sha256`; ровно его, release/proof pins и поля новой волны нужно
перенести в unsigned charter-v5 перед owner-signing. На issuer-хосте
`WHOSCORED_ROLLOUT_FILE` должен указывать на promoted manifest, а
`WHOSCORED_CHARTER_FILE` — на charter, подписанный для его exact SHA. Менять эту пару нужно
в остановленном deployment window: при промежуточном mismatch issuer только завершится
fail-closed. Повторный `create-rollout` для wave-70/all запрещён, потому что он заново прочтёт
live demand и может изменить acceptance identity.

### Последовательность rollout

1. Выпустить charter для `wave-20`, провести два подряд полностью зелёных scheduled DagRun.
2. После отдельного operator-approved promotion выпустить новый manifest/charter для
   `wave-70`; снова требуются два подряд зелёных scheduled DagRun. Manual runs в серию не
   входят, любой failed/DQ/traffic failure её сбрасывает.
3. Аналогично перейти на `wave-all`. Для её приёмки недостаточно `max_scopes=2000`:
   выбранный набор обязан равняться полному frozen active catalog, а `deferred_count` — нулю.
4. Сумма per-run allocation ceilings всех выданных bootstrap approvals должна
   помещаться в receipt-bound spendable order cap. Если следующий frozen plan не
   помещается, issuer останавливается до подписи и публикации; уменьшать каталог,
   обходить wave contract или расходовать reserve нельзя.

`ranked_scope_ids_sha256` входит в identity двух-run acceptance и обязан быть неизменным.
`ranked_workload_sha256` — evidence конкретного запуска: он закономерно меняется вместе с
due targets и сам по себе не сбрасывает acceptance streak.

### Receipt-bound lifetime cap и общий ledger

Gross `provider_order_cap_bytes` берётся из owner-signed provider policy,
повторяется в rollout/charter/approval и не может превышать `1_000_000_000`
decimal bytes. Spendable cap вычисляется кодом как
`floor(provider_order_cap_bytes × 95 / 100)`; оставшиеся 5% недоступны для
выдачи и сохраняются для in-flight/provider billing drift.

HMAC-protected campaign ledger общий для bounded smoke и всех шести bootstrap
runs. Он суммирует spent bytes и active durable escrow по provider order без
сброса в полночь, при рестарте или смене approval. Issuance-ledger-v4 до I/O
суммирует полные per-run allocation ceilings. Duplicate run ID идемпотентен,
но request/receipt drift, повтор slot index, rollback и overspend отклоняются.
При смене заказа нужен новый пустой provider-bound state namespace; старые
counters не удаляются и не наследуются.
Authenticated state marker schema-v3 связывает order ID, policy SHA и exact
gross cap. Marker v1/v2 не мигрирует автоматически и отклоняется без удаления
или сброса существующего ledger. Для manual schema-v2 smoke lifetime cap также
равен 95% startup-bound gross cap; суточный cap остаётся отдельным и сбрасывается
только по UTC-day.

Per-request потолок 2 MB (`PaidGatewayClient`, `DEFAULT_PAID_BYTES_PER_URL`) для матч-центра
1.26 MB достаточен и не трогается.

---

## 4. Как убедиться, что daily «зелёный»

- Маршрут: платный egress срабатывает **только** последним (raw-cache → direct curl →
  direct FlareSolverr → paid gateway) и лишь когда direct классифицирован как CF-challenge.
- `paid_proxy_bytes` учтён; дневной/lease/order бюджеты не превышены; DQ-гейты по 25 датасетам
  зелёные; запись в Bronze идемпотентна (повторный DagRun не плодит дублей).
- **Child-lock регресс:** триггер `dag_backfill_whoscored` c `direct_then_paid` ДОЛЖЕН быть
  отклонён (`"full paid crawl is disabled pending exact reconciliation"`).

---

## 5. Честные оговорки / риски

- **CF-репутация прокси на объёме не доказана** — живьём проверен ОДИН матч. Устойчивые
  ~150–300 матчей/день через один резидентный пул могут поднять долю challenge'ей, чаще уводить
  на медленный FlareSolverr-браузер или пометить пул.
- **Постоянная плата за GB** — daily это регулярный счёт; бэкфилл (~1–5 TB) — отдельное дорогое
  решение. Во всех WhoScored-доках вывод: чистый **direct-IP** хосту дешевле, чем платить
  по-байтно. Path A рабочий, но взвесить против починки CF-репутации хоста.
- **Губернатор 546 ms** (`GLOBAL_XHR_MIN_START_INTERVAL_MS`) — ~110 браузер-XHR/мин. При частом
  уходе на FlareSolverr это узкое место (для бэкфилла может выбить из 30-дневного окна).
- **Связка с ready-v1** — код-флип меняет дерево, поэтому образ пересобрать в `ready-v1` и
  переадмитить; пины approval'а обязаны указывать на НОВЫЙ билд, иначе `_verify_release_pins` /
  стартовый gate fail-closed'нутся.
- **Общий стек** — включение трогает protected-сервисы и общий scheduler; выполнять в тихое
  окно по [`SHARED-STACK-PROTOCOL`](../../../SHARED-STACK-PROTOCOL.md), не голым `docker compose up`.
