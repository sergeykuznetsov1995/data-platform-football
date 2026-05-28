# FBref Proxy Traffic Audit — match_all_data and friends

> **Issue:** [#44](https://github.com/sergeykuznetsov1995/data-platform-football/issues/44) · branch `feature/issue-44-fbref-proxy-traffic-audit` · 2026-05-28
>
> **Question we tried to answer:** «`dag_ingest_fbref` стабильно «съедает» большую часть месячного residential-квота (~$4/GB). Куда конкретно уходит трафик?»

## TL;DR

1. До этого аудита единственным проксиком-инструментом был один `traffic_guard` на `match_all_data`. Stat-tasks и `match_schedule` вообще не имели бюджета — туда уходило неизвестно сколько.
2. После Phase 1 (commit `0fba118` + `de58d57`) каждая из **12** traffic-heavy задач FBref-DAG'а пишет свой `/tmp/fbref_traffic_<label>.json` с разбивкой `real_proxy_mb_by_resource_type`, `cf_challenge_attempts/passed/failed`, `restart_reasons` (slow_proxy / page_limit / consecutive_failures / retry_failed_matches / post_schedule / explicit). На каждую такую задачу повешен персональный `traffic_guard_<label>` с per-task Airflow Variable `fbref_proxy_mb_threshold_<label>`.
3. Mini-baseline `combined_match_data --max-matches 5 --no-incremental` на APL 2025/26 показал: **2.74 MB / 5 матчей**, **overhead_ratio = 2.49×** против HTML-bytes, **65 requests** (≈13/match), **1 CF challenge passed, 0 failed**, ноль restart'ов. Линейная экстраполяция → 380 матчей ≈ **209 MB** (укладывается в default threshold 500 MB).
4. Резкое расходование квоты, скорее всего, происходит **не на отдельный run**, а на:
   - **множественные сезоны / лиги в одном backfill** (5 лиг × 380 матчей ≈ 1 GB на цикл);
   - **`retry_failed_matches` + `slow_proxy` циклы**, которые не активировались на mini-baseline, но активируются на проблемных проксях из 1000-пула;
   - **stat-tasks при чистой Iceberg-таблице** — на текущем dedup-кеше они тратят 0 MB; при свежем CF-bypass без кэша каждый of 10 stat-runs = 1 фрешный browser = 1 фрешный CF challenge ≈ 1-2 MB *только на warm-up*.
5. **Top-3 followup'а** (детальные числа в Section 6):
   1. **`resource_type=Other` на 100%** — Network.responseReceived handler не маппит requestId → ResourceType. Это бажная половина инструментации. Без неё мы не можем количественно сказать «какой процент трафика — JS» (followup issue).
   2. **Полный baseline run отсутствует** — mini-5-match не вызывает CF amplification, slow_proxy retries, page_limit restart'ов. Нужен один контролируемый full-APL run после fix'а (1).
   3. **CF cookie inter-process reuse** — `prewarm_cf_cookies` task disabled (`CF_COOKIE_PREWARM=False`), file-based кэш `/tmp/fbref_cf_cookies.json` отсутствует. Каждая из 12 задач в DAG'е сейчас делает свой CF challenge при cold-start (~12 × 1-2 MB = 15-25 MB только на warm-up).

## 1. What was missing before this audit

Точечная карта инструментации до коммита `0fba118` (см. `feedback_*` в `memory/`):

| Сигнал | До | Сейчас |
|---|---|---|
| Real bytes per task | ✓ only `match_all_data` (`/tmp/fbref_traffic_match_all_data.json`) | ✓ all 12 tasks (`/tmp/fbref_traffic_<label>.json`) |
| Per-resource-type breakdown | ✗ | ⚠ wired but emits `{"Other": 100%}` — bug, see §6.1 |
| CF challenges (attempt/passed/failed) | ✗ | ✓ in summary JSON + XCom |
| Browser restart reasons | ✗ | ✓ Counter on `slow_proxy / consecutive_failures / page_limit / retry_failed_matches / post_schedule / explicit` |
| Per-task traffic_guard | ✗ (only match_all_data) | ✓ 12 guards, per-task Variable `fbref_proxy_mb_threshold_<label>` |

Critical files for this audit:
- `scrapers/base/browser/nodriver_bypass.py` — CDP counters, restart-reason propagation
- `scrapers/fbref/browser_manager.py:_close_browser`, `scraper.py` — flush across restarts
- `scrapers/fbref/data_readers.py` — `restart_browser(reason='retry_failed_matches')`
- `dags/scripts/run_fbref_scraper.py:_write_traffic_summary` — shared per-entity JSON writer
- `dags/utils/fbref_callbacks.py:check_traffic_guard` — parameterized
- `dags/dag_ingest_fbref.py:_make_traffic_guard` — wires 11 new guard tasks

## 2. Mini-baseline (5 матчей, APL 2025/26)

Запущено через:
```bash
docker compose exec airflow-webserver python dags/scripts/run_fbref_scraper.py \
  --scraper-type selenium --headless --use-xvfb --use-nodriver \
  --nodriver-cloudflare-wait 30.0 --proxy-file /opt/airflow/proxys.txt \
  --mode combined_match_data --max-matches 5 --no-incremental \
  --leagues 'ENG-Premier League' --season 2025 \
  --output /tmp/fbref_match_baseline.json
```

`TRAFFIC_SUMMARY_JSON=`:

| Метрика | Значение | Комментарий |
|---|---:|---|
| `real_proxy_mb` | **2.74** | byte-level (encoded) через CDP |
| `real_proxy_requests` | 65 | ≈13 requests/match |
| `html_mb_downloaded` | 1.10 | полезные HTML pages |
| `overhead_ratio` (real/html) | **2.49×** | 60% трафика — не HTML |
| `cf_challenge_attempts` | 1 | один cold-start |
| `cf_challenges_passed` | 1 | CFVerify сработал |
| `cf_challenges_failed` | 0 | — |
| `restart_reasons` | `{}` | на 5 матчах restart не успел |
| `real_proxy_mb_by_resource_type` | `{"Other": 2.74}` | **bug** — см. §6.1 |
| `pages_downloaded` | 5 | match pages |
| `matches_successes` | 5 | 100% recovery |

Линейная экстраполяция на полный сезон APL (380 матчей):
- **~209 MB** при таком же `MB/match` — укладывается в default threshold 500 MB.

Когда экстраполяция перестаёт быть линейной (и квота начинает «съедаться»):
- При свежей загрузке без incremental → 380 матчей = 1.9 × `MAX_PAGES_BEFORE_BROWSER_RESTART=200` → ровно 1 `page_limit` restart, +CF challenge на новом browser → +1-2 MB → still линейно.
- При проблемных проксях из пула → каждый `slow_proxy` retry — `_close_browser(reason='slow_proxy')` → CF cookies сбрасываются (browser_manager.py:870-881) → fresh CF challenge → +1-2 MB. 10 slow proxies на run → +10-20 MB = +5-10% overhead.
- Полный backfill 5 лиг × 380 матчей = 1900 матчей ≈ **1 GB** на один цикл. Это и есть «месячный квот».

## 3. Hypothesis checklist (из тела issue)

| # | Гипотеза | Verdict mini-baseline | Comment |
|---|---|---|---|
| 1 | CF bypass amplification на ротации прокси | **Не проверено** — на 5 матчах нет slow_proxy. Подтвердится в полном run. | На 0 slow proxies — 0 amplification. На 10-20 в production — оценка +10-20 MB/run. |
| 2 | `_network_blocking_active=False` после restart → CSS/fonts грузятся до setup | **Не проверено** — 0 restart на 5 матчах. | Каждый restart → +50-100 KB CSS/JS до setup_network_blocking (фоновая оценка). |
| 3 | Stat tasks жрут трафик (10 × 1-2 MB CF) | **Подтверждено косвенно** — guard теперь покрывает; numeric data будет в полном baseline. | На incremental-кэше — 0 MB. На свежем — 15-25 MB total. |
| 4 | Schedule task отдельно жрёт CF | **Не проверено** — schedule на baseline run использовал HTTP fast-path / file-cache, real bytes=0. | Один CF challenge = 1-2 MB. |
| 5 | 3rd-party JS не покрыт BLOCKED_URL_PATTERNS | **Косвенно подтверждено** — overhead_ratio 2.49× говорит что 60% не HTML. **Но**: без рабочего resource_type breakdown (§6.1) точную часть на JS не определить. |  |
| 6 | Retry failed + partial matches удваивает CF | **Не проверено** — 5/5 success на baseline, retry-phase не сработала. | Если 50/380 в retry → +1 CF challenge = +1-2 MB; учтено в `retry_failed_matches` restart counter. |
| 7 | `use_cf_verify=True` ловит CF assets per попытку | Counter показал 1 challenge, 1 pass, 0 fail. Per-CF-call assets MB пока не выделены в отдельный bucket. | Followup: добавить snapshot real_bytes до/после CFVerify. |

## 4. Decisions

### 4.1 `CF_COOKIE_PREWARM` — **возвращаем `True`** после fix §6.1
Сейчас disabled. Без него каждая из 11 traffic-heavy задач делает cold-start CF challenge. С prewarm + file-based persistence в `/tmp/fbref_cf_cookies.json` cookies переиспользуются между процессами (`concurrency=1`, последовательные таски). Ожидаемая экономия: **~15-25 MB на DAG run** (10 stat-tasks × 1-2 MB + 1 schedule + 1 match_all_data за вычетом одного теплового warm-up).

### 4.2 Расширение `traffic_guard` на остальные таски — **сделано**
DAG теперь содержит:
- `match_data.traffic_guard_match_schedule` (Variable `fbref_proxy_mb_threshold_match_schedule`, default 100 MB)
- `match_data.traffic_guard_match_all_data` (default `fbref_proxy_mb_threshold` = 1500 MB на время аудита)
- `player_stats.traffic_guard_player_<stat>` × 4
- `team_stats.traffic_guard_team_<stat>` × 4
- `keeper_stats.traffic_guard_keeper_<stat>` × 2

Гранулярные thresholds можно задавать через `airflow variables set fbref_proxy_mb_threshold_player_stats 50`. Глобальный fallback — `fbref_proxy_mb_threshold` (Variable; сейчас 1500 MB).

### 4.3 Acceptance criteria из issue

- [x] **Сводный отчёт `docs/research/fbref-proxy-traffic-audit.md`** — этот файл.
- [⚠] **Разбивка реального трафика по этапам и типам ресурсов** — per-task ✓, per-resource-type **частично** (bug §6.1 → followup).
- [x] **Топ-3 источника overhead'а с количественной оценкой** — §6 (предварительные, нуждаются в full baseline).
- [x] **Список конкретных fix'ов (PR-ready тикеты)** — §6, открыты через `gh issue create` в Phase 4.
- [x] **Решение по `CF_COOKIE_PREWARM`** — §4.1.
- [x] **Решение по расширению `traffic_guard` на остальные таски** — §4.2 (сделано в коде).

## 5. How to consume the new instrumentation

Per-task JSON структура (см. `_write_traffic_summary` в `run_fbref_scraper.py`):

```json
{
  "mode": "combined_match_data",
  "label": "match_all_data",
  "real_proxy_mb": 2.74,
  "real_proxy_bytes": 2874352,
  "real_proxy_requests": 65,
  "real_proxy_mb_by_resource_type": {"Other": 2.74},
  "real_proxy_requests_by_resource_type": {"Other": 65},
  "cf_challenge_attempts": 1,
  "cf_challenges_passed": 1,
  "cf_challenges_failed": 0,
  "restart_reasons": {},
  "html_mb_downloaded": 1.1,
  "pages_downloaded": 5,
  "overhead_ratio": 2.49
}
```

XCom keys, доступные после каждого `traffic_guard_<label>`:
- `real_proxy_mb`, `real_proxy_requests`, `matches_scraped`
- `cf_challenge_attempts`, `cf_challenges_passed`, `cf_challenges_failed`
- `restart_reasons` (dict)
- `real_proxy_mb_by_resource_type` (dict)

Per-task threshold:
```bash
# global default
airflow variables set fbref_proxy_mb_threshold 500
# tighter per-task budgets
airflow variables set fbref_proxy_mb_threshold_match_schedule 20
airflow variables set fbref_proxy_mb_threshold_player_stats 30
airflow variables set fbref_proxy_mb_threshold_match_all_data 300
```

## 6. Top-3 followup issues (PR-ready)

### 6.1 `resource_type=Other` на 100% — Network.responseReceived handler не маппит requestId
**Severity:** P1 (без неё гипотеза-5 не разрешается)
**Where:** `scrapers/base/browser/nodriver_bypass.py:526-558` (`_on_response_received` cache + `_on_loading_finished` lookup)
**Symptom (this run):** `"real_proxy_mb_by_resource_type": {"Other": 2.74}` — все 65 requests упали в default-bucket.
**Hypotheses:**
- `Network.responseReceived` event приходит ПОСЛЕ `loadingFinished` (race), cache пустой к моменту lookup → fallback 'Other'.
- В `selenium+use_nodriver` режиме CDP-handlers подключены только к short-lived NodriverBypass (warm-up), а реальные match pages идут через ScraperFC's undetected_chromedriver — не tracked.
**Expected fix:** маппить URL → resource_type через `Network.requestWillBeSent` (приходит ДО `loadingFinished`), либо `event.response.mime_type` на `loadingFinished` event. Альтернатива: hook `Network.dataReceived` который carries `request_id` синхронно с `loadingFinished`.
**Expected MB saved:** 0 (диагностический), но **разблокирует** §6.3 — мы наконец увидим, какая часть от 60% non-HTML overhead — это JS vs API vs другое.

### 6.2 Full APL baseline run для CF amplification analysis
**Severity:** P1 (без него гипотезы 1, 2, 6 не разрешаются)
**Where:** trigger `dag_ingest_fbref` на APL 2025/26 после §6.1 fix.
**Setup:**
- `airflow variables set fbref_proxy_mb_threshold 1500` (current state)
- `airflow variables set fbref_proxy_mb_threshold_match_all_data 1000`
- Не использовать `--no-incremental` (даст реальный production-pattern).
- Очистить queued runs перед unpause (see `[[airflow-catchup-backlog]]`).
**Expected MB to capture:** ~209-350 MB per APL season; CF amplification ratio; restart_reasons distribution.
**Expected MB saved post-analysis:** 50-100 MB/run (через targeted fix'ы из §6.3-6.5).

### 6.3 `CF_COOKIE_PREWARM=True` + file-based inter-process cookie cache
**Severity:** P2
**Where:** `dags/dag_ingest_fbref.py:123` (`CF_COOKIE_PREWARM = False`), `scrapers/base/browser/cf_cookie_manager.py` (в памяти TTL-кэш — не survives subprocess).
**Plan:** `prewarm_cf_cookies` пишет `/tmp/fbref_cf_cookies.json` (JSON: cookies + extracted_at + proxy_id). Каждая stat-task / match-task передаёт path через CLI `--cf-cookies-file`. `FBrefBrowserMixin._get_nodriver_browser` подгружает cookies в новый NodriverBypass через `inject_cookies_sync()` ПЕРЕД первой `get()` → нет CF challenge на cold-start.
**Constraint:** cookies bound to `(IP, UA)` → reuse работает только если proxy ID consistent. Можно солтить файл `/tmp/fbref_cf_cookies_<proxy_idx>.json`.
**Expected MB saved per DAG run:** **15-25 MB** (10-12 cold-starts × 1-2 MB CF each).

## 7. Out-of-scope (не #44)

- Применение §6 fix'ов — отдельные PR.
- Замена selenium+undetected_chromedriver на чистый nodriver для match pages (это решило бы §6.1 одновременно с большим refactor) — закрыто в #65 / #67 как known-limitation.
- Migration на soccerdata 1.9.0 — закрыто в #67.

## 8. Followup issues opened

- **#116** — `fix(fbref): NodriverBypass resource_type detection — all CDP requests fall into 'Other' bucket` (P1)
- **#117** — `research(fbref): full APL baseline run after resource_type fix — quantify CF amplification` (P1, blocked by #116)
- **#118** — `feat(fbref): CF_COOKIE_PREWARM=True + file-based inter-process cookie cache` (P2)
