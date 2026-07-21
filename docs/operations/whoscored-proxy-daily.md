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
  owner-signed **charter** (месячный «стоячий ордер»: `order_id`, окно целиком внутри
  provider policy и `valid_until ≤ 62 дн`, exact decimal-byte caps, файл 0600) он раз в
  сутки строит и **подписывает** обычный ≤24 ч
  approval на `dag_ingest_whoscored` для КОНКРЕТНОГО планового `run_id`, с аллокациями
  `catalog-discovery` (phase discovery) + по одной `scope-<sha>` на каждый скоуп точного
  подписанного cohort prefix (не больше трёх) + одной `profiles-daily` (phase capture).
  Planner фиксирует родительский catalog batch, exact due match/preview/profile identities,
  все schedule targets и pagination headroom. Offline signer сам перечитывает exact cohort,
  считает SHA канонического manifest и требует, чтобы plan schema v2 содержал ровно
  доверенный `max_scopes` и exact ordered cohort prefix; planner не задаёт authority.
  Без валидного charter'а issuer не подписывает; запрошенный бюджет не может превысить
  `charter.daily_cap_bytes`.
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
- **Устойчивость к дрейфу каталога.** Paid DagRun исполняет только frozen signed cohort.
  Новые active scopes из дочернего discovery-каталога записываются как `deferred_scopes`
  с count/SHA и откладываются до следующего подписанного плана; через direct fallback они
  не запускаются. Если подписанный scope исчез, стал ineligible или quarantined, DAG сначала
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
   затем planner-контейнером **всегда заново** строит план максимум на три скоупа во fresh
   root-only RuntimeDirectory. Wrapper замораживает exact schema-v2 plan root-owned копией,
   проверяет `.max_scopes == 3` и `.scope_workloads|length <= 3`, и передаёт signer-контейнеру
   без сети frozen plan вместе с read-only cohort. Signer повторно выводит exact ordered
   prefix из cohort и trusted `--max-scopes 3` до публикации approval/pointer/issuance-ledger.
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
5. **Подписать provider-policy и charter.** Policy HMAC-связан с точным SHA свежей
   квитанции и фиксирует provider/order/plan и decimal-byte caps; charter связан
   с policy/cohort и ограничивает автоматические суточные выдачи. Ручной
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
