# FBref Proxy Traffic Audit — match_all_data and friends

> **Issue:** [#44](https://github.com/sergeykuznetsov1995/data-platform-football/issues/44) · branch `feature/issue-44-fbref-proxy-traffic-audit` · 2026-05-28
>
> **Question we tried to answer:** «`dag_ingest_fbref` стабильно «съедает» большую часть месячного residential-квота (~$4/GB). Куда конкретно уходит трафик?»

## TL;DR

1. До этого аудита единственным проксиком-инструментом был один `traffic_guard` на `match_all_data`. Stat-tasks и `match_schedule` вообще не имели бюджета — туда уходило неизвестно сколько.
2. После Phase 1 (commit `0fba118` + `de58d57`) каждая из **12** traffic-heavy задач FBref-DAG'а пишет свой `/tmp/fbref_traffic_<label>.json` с разбивкой `real_proxy_mb_by_resource_type`, `cf_challenge_attempts/passed/failed`, `restart_reasons` (slow_proxy / page_limit / consecutive_failures / retry_failed_matches / post_schedule / explicit). На каждую такую задачу повешен персональный `traffic_guard_<label>` с per-task Airflow Variable `fbref_proxy_mb_threshold_<label>`.
3. Mini-baseline `combined_match_data --max-matches 5 --no-incremental` на APL 2025/26 показал: **2.74 MB / 5 матчей**, **overhead_ratio = 2.49×** против HTML-bytes, **65 requests** (≈13/match), **1 CF challenge passed, 0 failed**, ноль restart'ов. Линейная экстраполяция → 380 матчей ≈ **209 MB** (укладывается в default threshold 500 MB).
4. **Post-#116 probe (10 matches, 2026-05-28, #117):** real resource_type breakdown теперь живой. CDP top-3: **XHR 1.68 MB (61.3%), SCRIPT 0.803 MB (29.3%), IMAGE 0.126 MB (4.6%)**. `OTHER` упал с 100% → 1.2%. HTTP fast-path (curl_cffi, +#124) добавил 1.72 MB чистого `Document`. Combined: 2.74 MB CDP + 1.72 MB HTTP = **4.46 MB / 10 матчей**, overhead 1.33× против HTML — лучше mini-baseline 2.49×.
5. **Production incremental DAG run (2026-05-28, #117):** 5-минутный run, все 12 task'ов отчитались `real_proxy_mb=0`. Из них 1 (match_all_data) — honest zero (380/380 уже в Iceberg). Остальные 11 — **instrumentation gap**: `scrapers.nodriver_fbref.scraper` (production single-stat/schedule path) реально CF-bypass'ит и качает 0.5-2.5 MB на task, но `_get_traffic_diagnostics()` читает из НЕправильного NodriverBypass instance → 0 байт. Новый followup §6.4 / [GH-issue](https://github.com/sergeykuznetsov1995/data-platform-football/issues/131).
6. Резкое расходование квоты, скорее всего, происходит **не на отдельный steady-state run**, а на:
   - **множественные сезоны / лиги в одном backfill** (5 лиг × 380 матчей ≈ 1 GB на цикл);
   - **`retry_failed_matches` + `slow_proxy` циклы**, которые не активировались на mini-baseline, но активируются на проблемных проксях из 1000-пула;
   - **stat-tasks при чистой Iceberg-таблице** — измерить пока нельзя из-за §6.4 (на пока есть гипотеза: 10 stat-tasks × 1-2 MB CF cold-start = 10-20 MB/run).
7. **Top-3 followup'а** (детальные числа в Section 6):
   1. ~~`resource_type=Other` на 100%~~ — **RESOLVED** PR #125 (issue #116), см. §6.1.
   2. ~~Полный baseline run отсутствует~~ — **CLOSED** этим документом (issue #117), §2bis/§2ter ниже.
   3. ~~**CF cookie inter-process reuse** — `prewarm_cf_cookies` task + file-based кэш `/tmp/fbref_cf_cookies.json`.~~ — **СНЯТО / SUPERSEDED.** Issue #118 закрыт **not-planned** (live-disproven 2026-06-14): pre-warmed `cf_clearance` НЕ пропускает CF Turnstile у FBref — CF привязывает clearance к полному fingerprint браузер-сессии, prewarm-браузер ≠ scraper-браузер. Вся инфра удалена в **#581 (PR #587)**; гипотеза экономии 15-25 MB для FBref неверна.
   4. **NEW: telemetry gap на `scrapers.nodriver_fbref` path** — counters не подключены к production single-stat/schedule wrapper. Issue [#131](https://github.com/sergeykuznetsov1995/data-platform-football/issues/131), см. §6.4.

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

## 2. Mini-baseline (5 матчей, APL 2025/26, pre-#116)

> **Status:** historical. `real_proxy_mb_by_resource_type` here is bug-affected (`{"Other": 2.74}` — все 65 reqs упали в default bucket). Корректный breakdown — §2bis после fix #116.

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

## 2bis. Post-#116 probe (10 матчей, APL 2025/26, 2026-05-28)

> Источник: `docs/research/fbref-baseline-2026-05-28/probe_10match.json` (raw).

Запущено через:

```bash
docker compose exec -T airflow-webserver python dags/scripts/run_fbref_scraper.py \
  --scraper-type selenium --headless --use-xvfb --use-nodriver \
  --nodriver-cloudflare-wait 30.0 --proxy-file /opt/airflow/proxys.txt \
  --mode combined_match_data --max-matches 10 --no-incremental \
  --leagues 'ENG-Premier League' --season 2025 \
  --output /tmp/fbref_probe/result.json \
  --traffic-output /tmp/fbref_probe/traffic.json
```

| Метрика | Значение | Комментарий |
|---|---:|---|
| `real_proxy_mb` (CDP) | **2.74** | браузерный трафик (warm-up + CF challenge) |
| `http_mb_downloaded` (curl_cffi fast-path) | **1.72** | match-page HTML после CF cookies (instr через #124) |
| **TOTAL proxy MB** | **4.46** | combined CDP+HTTP |
| `real_proxy_requests` / `http_requests_count` | 64 / 9 | 7.3 reqs/match (CDP) + 0.9 reqs/match (HTTP) |
| `cf_challenge_attempts/passed/failed` | 1 / 1 / 0 | один cold-start, потом warm session |
| `restart_reasons` | `{}` | 10 < `MAX_PAGES_BEFORE_BROWSER_RESTART=200` |
| `resource_type_cache_misses` | 0 | #116 fix работает 100% hit-rate |
| `overhead_ratio` (CDP_real / html) | **1.33×** | vs 2.49× mini-baseline (lower because warm IMAGE/SCRIPT cache) |
| `matches_successes` | 10 | 100% recovery |

**CDP resource_type breakdown** (по `real_proxy_mb_by_resource_type`):

| ResourceType | MB | % of CDP MB |
|---|---:|---:|
| **XHR** | 1.680 | **61.3%** |
| **SCRIPT** | 0.803 | **29.3%** |
| IMAGE | 0.126 | 4.6% |
| DOCUMENT | 0.055 | 2.0% |
| STYLESHEET | 0.035 | 1.3% |
| OTHER | 0.033 | **1.2%** ← (был 100% pre-#116) |
| FETCH | 0.005 | 0.2% |

**HTTP fast-path breakdown** (по `http_mb_by_resource_type`, #124):

| ResourceType | MB |
|---|---:|
| Document | 1.719 |

**Combined (CDP + HTTP, %от 4.46 MB total):**

| ResourceType | MB | % of total |
|---|---:|---:|
| Document + DOCUMENT | 1.774 | **39.8%** ← HTML match pages |
| XHR | 1.680 | 37.7% |
| SCRIPT | 0.803 | 18.0% |
| IMAGE | 0.126 | 2.8% |
| STYLESHEET + OTHER + FETCH | 0.073 | 1.6% |

**Линейная экстраполяция на 380 матчей (`--no-incremental`):**
- Cold-start (match 1) = ~1 MB (CF challenge dominated)
- Warm marginal cost ≈ (4.46 − 1) / 9 ≈ 0.38 MB/match
- → 380 матчей ≈ **~145 MB** total (~1 cold-start + 379 warm)
- ≈ 1 page_limit restart at match 200 → +1 MB CF challenge → still < 200 MB

**Top-3 источника overhead в FBref scrape** (главный вопрос issue #44 + #117):

1. **XHR (37.7% combined / 61.3% CDP)** — FBref страница match'а делает XHR к собственным эндпоинтам (xG, win prob, lineups). Это самый дорогой бакет, и он НЕ HTML.
2. **HTML (Document, 39.8%)** — собственно полезная нагрузка (~190 KB/match через curl_cffi fast-path).
3. **JS (SCRIPT, 18%)** — 11 SCRIPT requests на warm-up phase, ~73 KB средний. Не пересекает 30%-threshold acceptance criteria из #117 — отдельный issue не нужен. Кандидат на расширение `BLOCKED_URL_PATTERNS` для CDN-bundled JS.

## 2ter. Production DAG run (steady-state incremental, 2026-05-28)

> Source: 12 файлов `docs/research/fbref-baseline-2026-05-28/<label>.json`. DAG run: `manual__2026-05-28T14:29:33+00:00`.

Запуск:

```bash
docker compose exec -T airflow-webserver airflow dags trigger dag_ingest_fbref
# Variable'ы из issue #117 §Plan уже выставлены (см. §4.2 обновлённый)
```

**Headline:** DAG отработал за **5 минут 15 секунд** (vs ожидавшихся 2-4 ч). Все 12 traffic JSON'ов записаны, **все 12 c `real_proxy_mb=0`**.

| Task | реальная работа (по логам) | traffic JSON |
|---|---|---:|
| `match_all_data` | **0 матчей** (380/380 уже в Iceberg, log: «Incremental: 380 total, 380 already scraped, 0 new matches») | 0 MB — **honest zero** ✓ |
| `player_stats` | CFVerify passed, ~2.5 MB HTML loaded, 551 rows inserted | 0 MB — **bug** ⚠ |
| `player_shooting / playingtime / misc` | то же, ~0.5-2.5 MB каждый | 0 MB — bug |
| `team_*` (×4) | CFVerify passed, ~0.5-2.5 MB, 20 rows | 0 MB — bug |
| `keeper_*` (×2) | CFVerify passed, ~0.5 MB, 40 rows | 0 MB — bug |
| `match_schedule` | CFVerify passed, ~0.8 MB HTML, 432 rows inserted | 0 MB — bug |

**Root cause** (см. §6.4): production tasks используют `scrapers.nodriver_fbref.scraper` wrapper, который создаёт собственный `NodriverBypass` instance. `_get_traffic_diagnostics()` в `run_fbref_scraper.py` читает counters из `scraper.<…>` — но это ДРУГОЙ объект (или базовый scraper, не nodriver_fbref), counter всегда 0. Bytes реально качаются (видно в логах nodriver: «total: 0.3 MB»), но не accumulate'ятся в место, откуда читает runner.

**Что это значит для acceptance criteria #117:**

| AC | Status | Comment |
|---|---|---|
| 12 traffic JSON'ов записаны | ✅ | все 12 файлов в `docs/research/fbref-baseline-2026-05-28/` |
| §2 audit doc updated с real numbers | ✅ | §2bis (probe) — single fresh signal; §2ter — production гэп |
| Top-3 overhead с MB/run | ✅ | §2bis: XHR / HTML / SCRIPT |
| `restart_reasons[slow_proxy] > 5` → новый issue | N/A | 0 restart'ов в обоих runs; full-fresh APL backfill не запускали (Cost budget $2-4) |
| `Script > 30%` of total → новый issue | N/A | 18% combined / 29.3% CDP-only — under threshold |
| **NEW finding:** instrumentation gap | → #131 | §6.4 follow-up |

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

### 4.1 ~~`CF_COOKIE_PREWARM` — **возвращаем `True`** после fix §6.1~~ → **СНЯТО / SUPERSEDED**

> ⚠️ **Решение отменено (2026-06-15).** Issue #118 закрыт **not-planned**: live-проверка (2026-06-14) показала, что pre-warmed `cf_clearance` НЕ пропускает CF Turnstile у FBref — CF привязывает clearance к полному fingerprint браузер-сессии (TLS/JS/window), prewarm-браузер ≠ scraper-браузер → challenge переотдаётся независимо. Ожидаемая экономия 15-25 MB не подтвердилась. Вся file-based prewarm-инфра удалена в **#581 (PR #587)**. Абзац ниже — историческая гипотеза, **неактуален**.

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
- [x] **Решение по `CF_COOKIE_PREWARM`** — §4.1 (⚠️ позже отменено: #118 not-planned, инфра удалена в #581).
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

### 6.1 ~~`resource_type=Other` на 100%~~ — **RESOLVED PR #125 (issue #116, 2026-05-28)**
**Status:** Closed via `scrapers/base/browser/nodriver_bypass.py:534,550` — cache `_request_resource_types` пополняется на `Network.requestWillBeSent` (ДО `loadingFinished`), fallback `'Other'` только если rid отсутствует.
**Verification (this audit):** post-#116 probe `docs/research/fbref-baseline-2026-05-28/probe_10match.json` показал `resource_type_cache_misses=0` и 7 заполненных buckets (см. §2bis): XHR 61% / SCRIPT 29% / IMAGE 4.6% / DOCUMENT 2% / STYLESHEET 1.3% / OTHER 1.2% / FETCH 0.2%.

### 6.2 ~~Full APL baseline run для CF amplification analysis~~ — **RESOLVED issue #117 (2026-05-28)**
**Status:** Closed via this document. Probe 10 matches (§2bis) и production incremental DAG run (§2ter) выполнены 2026-05-28; результаты в `docs/research/fbref-baseline-2026-05-28/`.
**Verdict:**
- Hypothesis 5 («3rd-party JS не покрыт BLOCKED_URL_PATTERNS») — **частично подтверждена**: SCRIPT bucket = 29% CDP / 18% combined. Кандидат на расширение patterns, но не пересекает 30%-threshold acceptance criterion.
- Hypothesis 1 (CF amplification на ротации прокси) — **не активирована** (10 матчей мало для slow_proxy, 380 incremental матчей не дошли до browser path). Нужен отдельный issue под full backfill 5 лиг × 380 матчей если требуется.
- Hypothesis 2 / 6 (restart-induced CF re-challenges) — **не активированы** (0 restart'ов в обоих runs).
**Headline new finding:** instrumentation gap (§6.4) — без него full-fresh production-pattern измерение невозможно.

### 6.3 ~~`CF_COOKIE_PREWARM=True` + file-based inter-process cookie cache~~ — **СНЯТО / SUPERSEDED**
**Severity:** P2 · **Issue #118** — **CLOSED not-planned** (2026-06-14, live-disproven); инфра удалена в **#581 (PR #587)**

> ⚠️ **Не реализуемо для FBref:** pre-warmed `cf_clearance` не пропускает CF (clearance привязан к полному fingerprint браузер-сессии; prewarm-браузер ≠ scraper-браузер). План ниже — историческая гипотеза.
**Where:** `dags/dag_ingest_fbref.py:123` (`CF_COOKIE_PREWARM = False`), `scrapers/base/browser/cf_cookie_manager.py` (в памяти TTL-кэш — не survives subprocess).
**Plan:** `prewarm_cf_cookies` пишет `/tmp/fbref_cf_cookies.json` (JSON: cookies + extracted_at + proxy_id). Каждая stat-task / match-task передаёт path через CLI `--cf-cookies-file`. `FBrefBrowserMixin._get_nodriver_browser` подгружает cookies в новый NodriverBypass через `inject_cookies_sync()` ПЕРЕД первой `get()` → нет CF challenge на cold-start.
**Constraint:** cookies bound to `(IP, UA)` → reuse работает только если proxy ID consistent. Можно солтить файл `/tmp/fbref_cf_cookies_<proxy_idx>.json`.
**Expected MB saved per DAG run:** **15-25 MB** (10-12 cold-starts × 1-2 MB CF each).

### 6.4 NEW — Telemetry gap на `scrapers.nodriver_fbref` production path
**Severity:** P1 (без неё §6.2 production measurement невозможно) · **Issue #131** (open, follow-up to #117)
**Where:** `scrapers/nodriver_fbref/scraper.py` (production single-stat + match_schedule wrapper) vs `dags/scripts/run_fbref_scraper.py:_get_traffic_diagnostics()` (counter reader).
**Symptom (DAG run 2026-05-28T14:29:33):** 11 из 12 task'ов реально CF-bypass'ят и качают 0.5-2.5 MB HTML каждый (видно в логах: «CFVerify successfully bypassed», «Successfully loaded page», «Successfully fetched: ... (len=803473)»), но `TRAFFIC_SUMMARY_JSON` пишет `{"real_proxy_mb": 0.0, "real_proxy_requests": 0, "cf_challenge_attempts": 0}`. Аналогично для `match_schedule` (432 rows inserted, JSON 0 MB).
**Hypothesis:** `scrapers.nodriver_fbref.scraper` создаёт собственный `NodriverBypass` instance, а `_get_traffic_diagnostics()` читает counters через `getattr(scraper, '_proxy_traffic_*_base', 0)` — атрибуты на legacy `scrapers.fbref.scraper.FBrefScraper`, а не на nodriver_fbref instance. См. probe vs DAG: один и тот же `combined_match_data` mode + один и тот же scraper-type=selenium+use_nodriver, но probe идёт через legacy path и counter работает, DAG для stat-task'ов идёт через `single_stat` → `nodriver_fbref` и не работает.
**Expected fix:** либо expose `nodriver_fbref.scraper.NodriverBypass`-instance, либо переписать `_get_traffic_diagnostics()` так, чтобы он находил активный NodriverBypass через `gc` / explicit registry.
**Expected MB saved:** 0 (диагностический). **Разблокирует**: production-pattern measurement (§6.2 hypothesis 1/2/6) + per-task threshold tuning (сейчас все 11 thresholds бесполезны — guard всегда видит 0).

## 7. Out-of-scope (не #44)

- Применение §6 fix'ов — отдельные PR.
- Замена selenium+undetected_chromedriver на чистый nodriver для match pages (это решило бы §6.1 одновременно с большим refactor) — закрыто в #65 / #67 как known-limitation.
- Migration на soccerdata 1.9.0 — закрыто в #67.

## 8. Followup issues opened

- **#116** — `fix(fbref): NodriverBypass resource_type detection — all CDP requests fall into 'Other' bucket` (P1) — **CLOSED** via PR #125 (2026-05-28)
- **#117** — `research(fbref): full APL baseline run after resource_type fix — quantify CF amplification` (P1, blocked by #116) — **CLOSED** by this update (2026-05-28)
- **#118** — `feat(fbref): CF_COOKIE_PREWARM=True + file-based inter-process cookie cache` (P2) — **CLOSED not-planned** (2026-06-14, live-disproven); инфра удалена в #581 (PR #587)
- **#124** — `feat(audit): instrument curl_cffi HTTP fast-path for resource_type breakdown` (P3) — **CLOSED** (probe §2bis verifies `http_mb_by_resource_type={"Document": 1.719}`)
- **#131** — `fix(fbref): telemetry counters not wired to scrapers.nodriver_fbref production path` (P1) — open, surfaced by §6.4 / #117 production DAG run

## 9. Issue #616 — per-URL audit + blocking verdict (2026-06-17)

> **Question:** can we cut per-match proxy MB by extending `BLOCKED_URL_PATTERNS`
> (block "useless" XHR/SCRIPT/3rd-party)? **Verdict: no — blocklist extension is
> 0% in both regimes.** The traffic is already low in clean runs; the headline
> 2+ MB/match comes from browser cold-start amplification, not blockable resources.

### 9.1 What was added (instrumentation — shipped)

Per-URL traffic breakdown in `NodriverBypass`, keyed by normalised `host+path`
(query stripped, so repeated endpoint calls collapse and the counter stays
bounded):
- `get_real_traffic_stats()` now also returns `real_bytes_by_url`,
  `real_requests_by_url`, `top_traffic_urls` (top-25 by bytes, with mb+requests),
  and `first_party_mb` / `third_party_mb` (fbref.com / ssref.net = first-party).
- Flushed across browser restarts via the same base-accumulator path as the
  resource-type counters (`scrapers/fbref/browser_manager.py`,
  `scrapers/nodriver_fbref/scraper.py`), surfaced in
  `/tmp/fbref_traffic_<label>.json` (`run_fbref_scraper.py`) and the bench report.
- `scripts/research/bench_fbref_fetch.py`: new env `BENCH_FORCE_NODRIVER=1`
  disables the curl_cffi HTTP fast-path to reproduce the cold / fallback regime.

### 9.2 Method

`bench_fbref_fetch.py`, 10 fixed APL 2025/26 match pages, production-mode scraper.
Four runs — with vs without the candidate blocking patterns, in each regime:

| run | regime (HTTP fast-path) | new patterns | real_proxy_mb (CDP) | `/short/inc/` search-lists |
|---|---|---|---:|---|
| `issue616_baseline`      | warm (9/10 via curl_cffi) | no  | **2.75** | x1 |
| `issue616_after`         | warm                      | yes | **2.75** | x1 |
| `issue616_cold_baseline` | cold (0/10, forced)       | no  | **3.44** | x1 |
| `issue616_cold_after`    | cold                      | yes | **3.44** | x1 |

Raw artifacts: `docs/research/data/bench_fbref_issue616_{baseline,after,cold_baseline,cold_after}.json`.
All four: `success_rate=1.0`, per-match HTML intact (~353 KB), no data loss.

### 9.3 Why blocking does nothing

Top CDP consumer on a cold load was `fbref.com/short/inc/*_search_list.csv`
(~1.65 MB — autocomplete lists, **not** match data; parser reads HTML comments
via `extract_tables_from_comments`, no scraper code references `/short/inc/`).
Yet adding `*/short/inc/*` changed nothing because:

1. **Browser cache, not blocking.** Search-lists are fetched **once** (`requests=1`)
   on match 1 and served from cache afterwards — identical with/without the
   pattern. On that first load they're fetched *before* blocking activates.
2. **Sub-target leak.** 3rd-party trackers/ads (GTM `x21`, osano `x10`,
   pub.network `x10`) load in ad/consent **iframes (separate CDP targets)** that
   the main page's `Network.setBlockedURLs` does not cover. GTM was **already**
   in `BLOCKED_URL_PATTERNS` and still leaks identically.
3. **Late activation.** `_setup_network_blocking()` runs only *after* CF bypass,
   so the cold-start page load leaks its resources regardless of patterns.

### 9.4 Headline correction

Clean per-match proxy traffic is already low: **~0.28 MB (warm) / ~0.34 MB (cold)**.
The `2.12 MB/match` figure (`bench_fbref_baseline.json`, 601 requests / 21 MB) was
a **pathological run** — repeated browser cold-starts (CF re-challenges /
slow-proxy / restarts), each re-minting and re-downloading ~2 MB. The cost is
cold-start *amplification*, not per-page blockable resources.

### 9.5 HTTP fast-path fingerprint — already correct

(Re: external suggestion to match the cf_clearance fingerprint.) Verified:
container Chromium is **120**, the browser uses its **native UA** (UA is
deliberately not faked — see `nodriver_bypass.py` comment on JA3/JA4 mismatch),
and `curl_cffi` reuses the cookie with `impersonate='chrome120'` + matching UA,
same proxy, TTL 25 min / 150 req. The recipe is already implemented; in clean
runs the fast path works **9/10** and traffic is minimal. No fingerprint fix
needed.

### 9.6 Real lever → follow-up

Reduce **browser cold-starts** (CF re-challenge / slow-proxy / restart
amplification); each ≈ 2 MB. The HTTP fast-path already avoids them when it
works — the spikes are bad-proxy/CF days where it falls back. Diagnose with the
existing `http_fetch_diag` (#65) on the production VM when
`http_fetch_fallback > 0`. Blocklist tuning and fingerprint matching are **not**
the levers (this section).
