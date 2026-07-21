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
if approval == exact canary:      exact-1GB-контракт
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
> env + DagRun conf).

---

## 1a. Автоматический плановый сбор (issuer + pointer, #954)

До этого изменения даже допущенный к платному пути `dag_ingest_whoscored` мог тратить
прокси **только при ручном триггере** DagRun с `conf {transport_policy: direct_then_paid, …}`:
`resolve_transport_policy` читает политику исключительно из `dag_run.conf`, а плановый
(`scheduled`) запуск идёт с пустым conf → `direct_only`. Владельцу нужен полный автомат
(«даг сам каждый день всё обновляет»). Реализовано без ослабления ни одного wire-гейта
(24 ч validity, привязка к одному `run_id`, release-пины, замок бэкфилла):

- **Суточный issuer** — `scripts/whoscored_proxy_campaign.py issue-daily-ingest`. По
  owner-signed **charter** (месячный «стоячий ордер»: `order_id`, `valid_until ≤ 62 дн`,
  `daily_mb`, `monthly_mb`, файл 0600) он раз в сутки строит и **подписывает** обычный ≤24 ч
  approval на `dag_ingest_whoscored` для КОНКРЕТНОГО планового `run_id`, с аллокациями
  `catalog-discovery` (phase discovery) + по одной `scope-<sha>` на активный скоуп + одной
  `profiles-daily` (phase capture). Без валидного charter'а issuer не подписывает; запрошенный
  бюджет не может превысить `charter.daily_mb`.
- **Pointer-резолюция** — `dags/scripts/whoscored_proxy_runtime.py`. Issuer кладёт рядом с
  approval маленький pointer `POINTER_ROOT/<sha256(run_id)>.json` (0600) с
  `{dag_id, run_id, approval_id, approval_sha256}`. На запуске
  `_scheduled_paid_pins` подхватывает его **только** если `run_type == "scheduled"` И
  `daily_ingest_paid_crawl_allowed(dag_id)` (т.е. только ingest — бэкфилл **никогда**, замок
  цел) И conf пуст. Явный conf всегда приоритетнее (ручной флоу не меняется). Отсутствие
  pointer'а → `direct_only` (fail-closed, день просто идёт бесплатно). Испорченный/
  несовпадающий pointer (неверный владелец, режим ≠ 0600, симлинк, mismatch run_id/dag_id) →
  ошибка (fail loud).
- **Устойчивость к дрейфу каталога.** Скоуп, который discovery открыл ВНУТРИ запуска (после
  выпуска approval), не имеет подписанной аллокации → идёт `direct_only` через одну этот
  скоуп (`PaidRuntime.for_allocation(missing_ok=True)`), обычно challenge'ится → retryable →
  накрывается approval'ом СЛЕДУЮЩЕГО дня (лаг 1 день). Work item профилей теперь **константа**
  `profiles-daily` (catalog batch выводится внутри рана и не может быть предсказан заранее —
  это чинит блокер даже ручного платного флоу).

**Развёртывание автомата (за владельцем, в деплой-окно):**
1. env на scheduler: `WHOSCORED_PROXY_APPROVAL_ROOT` (где лежат approval'ы),
   `WHOSCORED_SCHEDULED_PAID_POINTER_ROOT` (где лежат pointer'ы), плюс существующие
   gateway/approval-path env из §3.6. HMAC-секрет approval'а виден issuer'у/filter'у/gateway,
   но **не** scheduler-раннеру (он в `_RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES`).
2. host systemd-timer, 09:00–09:30 UTC (schedule DAG'а `0 10 * * *` → до задач остаётся ≥ 6 ч
   минимума validity). Timer делает `docker exec <scheduler>`:
   - получить список активных скоупов (`--scopes-file`): читается из персистентного каталога
     (тот же запрос, что `_active_scope_specs`; для смоука — hand-authored файл из 3–5 лиг);
   - `python scripts/whoscored_proxy_campaign.py issue-daily-ingest --run-id
     scheduled__<data_interval_start ISO+00:00> --scopes-file … --charter … --runtime-sha256 …
     --classifier-sha256 … --total-mb … --secret-file … --approval-root … --pointer-root …`.
   Пины (`runtime_sha256`/`classifier_sha256`) меняются только при пересборке образа — деплой
   пишет их в конфиг, который читает wrapper.
3. **run_id.** Плановый `run_id = "scheduled__" + data_interval_start.isoformat()` (для cron
   `0 10 * * *` это (D−1) 10:00 UTC у запуска, стартующего в D 10:00). Если issuer посчитает
   run_id неверно — pointer ляжет под чужой ключ и запуск просто пойдёт `direct_only`
   (**fail-closed, без ошибочного расхода**). Точную дату-смещение подтверждаем на смоуке
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

Текущий заказ (PROXYS.IO order 38950, 1 GB) — **только для проб/канареек**, на постоянный
daily не годится.

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
   Квитанция на старый 1 GB заказ НЕ подойдёт к новому тарифу.
5. **Подписать NON-canary approval**, привязанный к `allowed_dag_ids=['dag_ingest_whoscored']`,
   validity ≥ 6 ч, `caps.total_provider_bytes`/`daily_provider_bytes` под размер тарифа (фазы
   суммируются в total, daily ≤ total), `runtime_sha256`/`classifier_sha256` = пины НОВОГО
   пересобранного релиза. (Для non-canary требования «ровно 1 GB» нет — это канареечная константа.)
6. **Deployment env + сервисы:** `WHOSCORED_PROXY_APPROVAL_PATH` / `_ROOT`,
   `WHOSCORED_PAID_GATEWAY_URL=http://whoscored_paid_gateway:8898`,
   `WHOSCORED_PAID_GATEWAY_TOKEN` (≥32 символа), `WHOSCORED_PROXY_APPROVAL_HMAC_SECRET`,
   `PROXY_POOL_JSON`; запустить filter-proxy с `--daily-budget-mb` / `--max-lease-mb` под тариф;
   изолированный paid gateway + filter-proxy должны идти с совпадающим `WHOSCORED_PROXY_RUNTIME_SHA256`.
   Шлюз изолирован в своём `-p whoscored-gw` (`deploy/whoscored/gateway.compose.yaml`).
7. **Триггер daily DagRun** с
   `conf {transport_policy: direct_then_paid, paid_approval_id: <id>, paid_approval_sha256: <lowercase hex>}`.
   Плановый/обычный запуск остаётся `direct_only` и не тратит ничего (fail-closed по умолчанию).

### Условный код-шаг: поднять потолок под тариф (в паре, на пересборке образа)

`WHOSCORED_PROVIDER_ORDER_SAFETY_CAP_BYTES = 850 MiB` (`scripts/proxy_filter/filter_proxy.py`)
— это **пожизненный** потолок расхода на ЗАКАЗ: `order_remaining = cap − exposure_provider_bytes`,
где `exposure` копится за всё время текущего заказа (сбрасывается при переходе на новый
заказ). Эффективный потолок = `min(DAILY_BUDGET_BYTES, cap) − exposure`.

**Осознанно оставлен 850 MiB в этом PR** (НЕ поднят «на будущее»): на текущем 1 GB заказе
(order 38950) и во время смоука это независимый backstop реальных денег. Поднять его заранее
до размера тарифа = снять этот backstop с живого 1 GB смоука. Поэтому:

- Смоук идёт на 850 MiB backstop'е + низком `--daily-budget-mb` (≈300) + caps approval'а — три
  независимых бортика.
- При покупке тарифа (§3.2) поднять **в паре, в тот же rebuild** (пересборка образа под новый
  receipt всё равно требуется — это НЕ лишняя пересборка):
  `WHOSCORED_PROVIDER_ORDER_SAFETY_CAP_BYTES` (код) ≈ размер заказа (напр. ~48 GiB под 50 GB)
  **и** `WHOSCORED_PROXY_FILTER_DAILY_BUDGET_MB` (env, default 850). Держать их согласованными:
  пики выходных полного каталога (~1.5–2.5 GB/сут) превысят 850 MiB и fail-closed'нутся.
- При смене заказа сбросить order-exposure state-marker/ledger фильтра (новый заказ = новый
  пожизненный счётчик).

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
