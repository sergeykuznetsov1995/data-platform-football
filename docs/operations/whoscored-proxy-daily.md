# WhoScored — ежедневный сбор через платный резидентный прокси (Path A, #954)

Этот runbook описывает, как включить **ежедневный** WhoScored-сбор (`dag_ingest_whoscored`)
через платный резидентный прокси, оставив **исторический бэкфилл (`dag_backfill_whoscored`)
жёстко закрытым** («детский замок»). Решение владельца: «Путь А — гнать daily через прокси»
(direct-IP хосту с чистой CF-репутацией добыть не удалось; резидентный прокси CF проходит,
что подтверждено GREEN-канарейкой 2026-07-18).

> ⚠️ Платный трафик = реальные деньги провайдера. Ни один шаг ниже не выполняется
> автоматически: включение — за явным решением владельца, свежей квитанцией и ревью-релизом.

---

## 1. Что изменено в коде (ревью-PR, ветка `ops/ws954-production`)

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
  owner-signed **charter-v4** (месячный «стоячий ордер»: `order_id`, окно целиком внутри
  provider policy и `valid_until ≤ 62 дн`, exact decimal-byte caps, rollout/wave contract,
  файл 0600) он раз в
  сутки строит и **подписывает** обычный ≤24 ч
  approval на `dag_ingest_whoscored` для КОНКРЕТНОГО планового `run_id`, с аллокациями
  `catalog-discovery` (phase discovery) + по одной `scope-<sha>` на каждый скоуп точного
  signed wave + одной `profiles-daily` (phase capture). Rollout manifest schema-v3 содержит
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
  Plan schema-v3 несёт стабильный `ranked_scope_ids_sha256`, отдельный изменяемый
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
  charter, а effective budget не может превысить `charter.daily_cap_bytes` и действующий
  code-owned 300 MB provider ceiling.
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
2. Установить checked-in `whoscored-daily-issuer.service/.timer` и
   `scripts/whoscored_daily_issuer.sh` строго по
   [`whoscored-production.md`](whoscored-production.md). Timer срабатывает в
   09:15 UTC и не делает catch-up. Wrapper допускает запуск только в
   09:00–09:30 UTC, сначала заново аттестует все пять running/healthy сервисов,
   затем planner-контейнером **всегда заново** строит план fixed heavy-first wave во fresh
   root-only RuntimeDirectory. Wrapper замораживает exact schema-v3 plan root-owned копией,
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

Текущий заказ PROXYS.IO order 38950 имеет provider quota 1 GB и предназначен
**только для проб/канареек**; на постоянный daily он не годится.

Полный `all_catalog` бэкфилл — **отдельный порядок величины**: ~1–5 TB одноразово
(центр ~2–3 TB, при коде-потолке ≤30 дней / ≤172 800 request-units/сут). Не запускать вместе
с daily; сначала стабилизировать ежедневный сбор.

---

## 3. Порядок включения (за владельцем)

1. **Одобрить код-изменение** как ревью-релиз (оно авторизует провайдерский расход для
   ежедневного ingest; сентинелы code-owned — ни env, ни conf, ни поле approval их не включают).
2. **Купить тариф** нужного размера (см. §2) и подтвердить order id/plan tier.
3. **Пересобрать образ WhoScored** после код-изменения → build-provenance attestation
   регенерируется как `ready-v1` (content-addressed); затем регенерировать deployment
   attestation (`scripts/generate_whoscored_deployment_attestation.py`, `status=ready-v1`).
   Без пересборки стартовый gate (`whoscored_production_gate.py`) отклонит образ (дерево изменилось).
4. **Свежая (<24 ч) provider quota receipt** под НОВЫЙ заказ, screenshot-bound (order id,
   plan tier, `status=active`, точные десятичные quota/remaining, без прокси-кредов). Прогнать
   deploy-admission: `scripts/whoscored_production_admission.py verify-rendered` → `post-create`
   по всем 5 protected-сервисам (`airflow-scheduler`, `flaresolverr`,
   `flaresolverr_whoscored_paid`, `whoscored_paid_gateway`, `whoscored_proxy_filter`).
   Квитанция со старой provider quota 1 GB НЕ подойдёт к новому тарифу.
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
   `WHOSCORED_PROVIDER_POLICY_SHA256`, `--daily-budget-bytes` и
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

### Безопасная смена волн

`create-rollout` вызывается ровно один раз, для первой волны. Переходы выполняются только
`promote-rollout`: команда полностью валидирует исходный schema-v3 manifest, не подключается
к live catalog и переносит `rollout_id`, frozen `ranked_scope_ids`, stable SHA и исходный
workload basis без изменений. Разрешены только соседние переходы:

```bash
python scripts/whoscored_proxy_campaign.py create-rollout \
  --rollout-id production-2026-07 \
  --cohort-id production-2026-07-wave-20 \
  --wave-id wave-20 \
  --runtime-sha256 "$WHOSCORED_RUNTIME_SHA256" \
  --classifier-sha256 "$WHOSCORED_CLASSIFIER_SHA256" \
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
перенести в unsigned charter-v4 перед owner-signing. На issuer-хосте
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
4. Текущий 300 MB lifetime provider cap остаётся неизменным. Если план волны не помещается
   в подписанные caps/floors, issuer обязан остановиться; расширять cap или обходить wave
   contract нельзя. `wave-70`/`wave-all` требуют отдельного cap-release, если фактический
   план превышает текущие 300 MB.

`ranked_scope_ids_sha256` входит в identity двух-run acceptance и обязан быть неизменным.
`ranked_workload_sha256` — evidence конкретного запуска: он закономерно меняется вместе с
due targets и сам по себе не сбрасывает acceptance streak.

### Действующий lifetime hard cap (не менять в этом релизе)

`WHOSCORED_PROVIDER_ORDER_SAFETY_CAP_BYTES = 300000000` decimal bytes
(`scripts/proxy_filter/filter_proxy.py`)
— это **пожизненный** потолок расхода на ЗАКАЗ: `order_remaining = cap − exposure_provider_bytes`,
где `exposure` копится за всё время текущего заказа (сбрасывается при переходе на новый
заказ). Ни provider quota, ни policy/charter/approval не могут поднять этот кодовый предел:
действующий effective lifetime hard cap равен ровно `300000000` decimal bytes.

**Осознанно оставлен 300 MB в этом релизе** (НЕ поднят «на будущее»): provider quota
текущего order 38950 равна 1 GB, но executable cap для смоука и daily остаётся меньшим
независимым backstop реальных денег. Поэтому:

- Смоук идёт на 300000000-byte backstop'е + policy/charter/approval caps — три
  независимых бортика.
- Issuer и filtering proxy fail-closed'нутся при исчерпании этого lifetime cap даже если
  provider quota или подписанные caps больше. Пики полного каталога (~1.5–2.5 GB/сут)
  заведомо не помещаются в текущий релиз.
- Любое будущее увеличение требует отдельного review/rebuild вместе с новой provider
  receipt и согласованными owner-signed policy caps; текущая процедура его не разрешает.
  До покупки большего заказа или изменения любого code/policy/charter cap proposal
  обязан закрепить неизменяемую выборку фактически тарифицированных provider bytes:
  окно и горизонт измерения, ordered receipt SHA-256, `order_id`, provider counter,
  decimal-byte units и алгоритм p95. Единственный допустимый forecast равен
  `forecast_bytes = ceil(p95_billed_bytes * 1.25)`. Округление выполняется вверх до
  целого decimal byte. Proposal и новый потолок должны быть явно подписаны
  `sergeykuznetsov1995`; ни покупка, ни более высокий provider quota сами по себе не
  являются разрешением, а executable cap не может превышать одобренный forecast и
  остальные более низкие пределы. Нужны новый reviewed release и отдельное явное
  owner approval — текущий GO-артефакт не может расширить бюджет.
  В текущем коде намеренно нет CLI/schema для такого widening: формула является
  обязательным acceptance contract для будущего изменения, которое должно
  реализовать и протестировать эту проверку до review/rebuild.
- При смене заказа создать отдельный пустой schema-v2 state namespace, связанный
  с новым `order_id` и `provider_policy_sha256`; старые counters не удалять и не наследовать.

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
