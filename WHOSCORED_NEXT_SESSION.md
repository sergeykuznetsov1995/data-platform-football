# WhoScored Events — Next Session (CF bypass)

> Сессия 2026-04-29 — попытка nodriver + per-match proxy rotation (failed).
> Сессия 2026-04-30 — миграция nodriver → **FlareSolverr** (код готов, тесты
> зелёные, smoke на «холодном» IP работает; полный prod-run **не проходит** —
> Cloudflare блокирует FS после ~5–10 запросов с одного IP).
> Этот файл — cold-start для следующей сессии (CF-bypass на массовом scrape).

## TL;DR

`dag_ingest_whoscored` **paused**. Iceberg `bronze.whoscored_events` стоит на
**85 551 rows / 58 games** (+4857 / +3 от smoke без прокси через FS). Цель
порога `validate_events` = **500 k rows / 330+ games**.

**Что готово (этой сессией, в коммите):** миграция кода с nodriver на
FlareSolverr, новый `FlareSolverrClient`, refactor `scrape_events`, compose
service `flaresolverr` (image `v3.3.21` пиннинг, security_opt по паритету
с tor/redis), DAG bash_command + CLI `--flaresolverr-url`, 49 unit-тестов
зелёные (24 FS-client + 25 WhoScored), полный scrapers suite **736 passed**.

**Что не идёт:** полный prod scrape 348 матчей с одного VM IP. FS успешно
решает CF на «холодном» запросе (smoke 3 матча → 4857 rows, ~3 сек), но при
массовых запросах WhoScored Cloudflare метит сессию после 5–10 запросов и
дальше каждый вызов = 60–120 s challenge timeout, новые сессии тоже падают.

**Корневая причина** — не FS, не код, а CF-блокировка VM IP при mass-scrape
(плюс residential proxy pool массово забанен за 2026-04-29). Код-миграция
nodriver → FS ценна сама по себе и закоммичена отдельно.

## Что в коде

### Что было сделано (готово)

| Файл | Что |
|------|-----|
| `scrapers/base/flaresolverr_client.py` (NEW, 200 LOC) | HTTP-клиент к FS REST API: `create_session/destroy_session/get/health/list_sessions` + context-manager. Exceptions: `FlareSolverrError/Timeout/CFChallengeFailed`. HTTP non-2xx с keyword `challenge/cloudflare/turnstile` → `CFChallengeFailed` (нужно для recycle-сессии). |
| `scrapers/whoscored/events_fetcher.py` | Удалена `fetch_match_events_via_nodriver`; добавлена `fetch_match_events_via_flaresolverr(client, match_id, session_id, max_timeout_ms=120_000)`. Regex-парсер `_extract_matchcentre_from_html` не тронут. |
| `scrapers/whoscored/scraper.py` | `scrape_events`: убраны `_save_cookies` / `_new_browser` / `_try_rotate_proxy` / periodic-restart. Добавлены FlareSolverrClient lifecycle, локальный `_recycle_session()`, retry-маппинг. Constants: `EVENTS_BROWSER_RESTART_EVERY` удалена; `EVENTS_SESSION_RECREATE_EVERY = 10` (эмпирически после ~10 запросов FS-сессия деградирует на CF); `EVENTS_MAX_PROXY_RETRIES = 3`. ctor: `flaresolverr_url: Optional[str] = None`. |
| `compose.yaml` | service `flaresolverr` (`ghcr.io/flaresolverr/flaresolverr:v3.3.21`, port 127.0.0.1:8191, healthcheck `/health`, mem 512M, shm_size 1g, security_opt по паритету с tor/redis). |
| `dags/dag_ingest_whoscored.py` | bash_command добавлен `--flaresolverr-url http://flaresolverr:8191`, `--proxy-file ""` (residential pool забанен), env `DISPLAY=:99` удалён (Xvfb для nodriver больше не нужен). |
| `dags/scripts/run_whoscored_scraper.py` | CLI: новый `--flaresolverr-url`. Удалён chromium SIGTERM cleanup и nodriver logger suppress (FS изолирует Chromium в свой контейнер). |
| `.env.example` | `FLARESOLVERR_URL=http://flaresolverr:8191`. |
| `tests/unit/scrapers/test_flaresolverr_client.py` (NEW, 24 теста) | Mock `requests.Session.post`, all error paths, context-manager, payload checks. |
| `tests/unit/scrapers/test_whoscored_scraper.py` | Удалены классы `TestWhoScoredScrapeEventsViaNodriver` + `TestWhoScoredProxyRotation` (obsolete). Добавлены `TestWhoScoredScrapeEventsViaFlaresolverr` (7 тестов) + `TestFetchMatchEventsViaFlaresolverr` (3 теста). |

### Эмпирические находки

- **Прокси с FS не нужны** — наоборот, residential pool массово забанен CF, любая FS-сессия с residential proxy сразу получает challenge timeout. На VM IP без прокси FS пробивает CF cold-start ~3 сек.
- **`maxTimeout` 60 → 120 s не помогает** — FS возвращает «Error solving the challenge. Timeout after 120.0 seconds» так же стабильно, как и за 60 с. Это не deadline, а CF-черная-метка на cookie/IP.
- **CF метит после 5–10 запросов** — даже на свежей сессии. Recycle every 10 не спасает: новая сессия с того же IP тоже идёт в CF blocklist.
- **soccerdata WhoScored docs** не дают встроенного CF-bypass: только `proxy=callable/list/"tor"` + `headless=False` (Selenium <4.13). Tor exit-ноды также в blocklist.

## Что делать в следующей сессии

### **A. Byparr swap-in** (наименьшая правка, рекомендую первым)

Byparr — drop-in replacement для FS, использует **Camoufox** (фронт Firefox) вместо Chrome — другой TLS-fingerprint, другой User-Agent ranking у CF. Контракт `/v1` совместим.

```yaml
# compose.yaml — заменить только image:
  flaresolverr:
    image: ghcr.io/byparr/byparr:latest   # was: ghcr.io/flaresolverr/flaresolverr:v3.3.21
    # rest unchanged
```

Шаги:
1. swap image в `compose.yaml`
2. `docker compose up -d flaresolverr` → wait healthcheck
3. smoke 5 matches: `docker exec airflow-scheduler bash -lc 'cd /opt/airflow && python -u dags/scripts/run_whoscored_scraper.py --leagues "ENG-Premier League" --seasons 2025 --events-only --max-matches 5 --proxy-file "" --flaresolverr-url http://flaresolverr:8191 --output /tmp/byparr_smoke.json'`
4. если smoke ОК → smoke 30 matches → если стабильно (нет CF-метки после 10–15) → full prod 345

Эстимейт: 30–60 мин.

### B. Pre-warm CF + curl_cffi (option Y из исходного плана)

Один nodriver/FS bootstrap → extract `cf_clearance` через CDP `Network.responseReceivedExtraInfo` → 348 матчей через `curl_cffi` с inject cookie.

**Плюсы:** super-fast после bootstrap. **Минусы:** требует CDP-работу, TLS fingerprint matching, refresh cookie на 30-min boundaries.

Эстимейт: 3–4 ч R&D.

### C. Tail-end альтернативы

- **Платный Cloudflare-bypass API** (Bright Data, ScrapingBee, ZenRows) — изолирует CF-bypass за third-party (стоит денег / per-request).
- **Проксирование через VPN/proxy с чистой репутацией** — ~$50/мес residential ISP-pinned.
- **Альтернативный data source** — Understat (xG, shots) уже в Silver, даёт частичный overlap; полный Opta events недоступны без платного API.

## Iceberg state на момент паузы

```
iceberg.bronze.whoscored_schedule         : 1900 rows / 5 sezones APL ✓
iceberg.bronze.whoscored_missing_players  : присутствует ✓
iceberg.bronze.whoscored_season_stages    : присутствует ✓
iceberg.bronze.whoscored_events           : 85 551 rows / 58 games (нужно 500 k+ / 330+)
```

`schedule` контейнер — 380 game_ids в `2526` сезоне, из них 35 уже в events
(skip_existing). Дотянуть ~345 матчей.

## Файлы / точки входа для следующей сессии

```
scrapers/base/flaresolverr_client.py                 # HTTP-клиент (для byparr НЕ менять — контракт совместим)
scrapers/whoscored/scraper.py                        # scrape_events с FS lifecycle
scrapers/whoscored/events_fetcher.py                 # fetch_match_events_via_flaresolverr + regex parser
dags/scripts/run_whoscored_scraper.py                # CLI
dags/dag_ingest_whoscored.py                         # DAG (paused)
compose.yaml                                          # service flaresolverr (image swap point)
tests/unit/scrapers/test_flaresolverr_client.py      # 24 unit tests
tests/unit/scrapers/test_whoscored_scraper.py        # 25 unit tests (включая FS lifecycle)
```

Memory:
- `project_whoscored_cloudflare.md` — детальная архитектура
- `project_vm_oom_event_2026-04-29.md` — про OOM и docker compose stop list

## Команды для cold-start

```bash
# 1. Verify clean state
docker compose ps | grep -E "flaresolverr|airflow"
docker exec airflow-scheduler bash -lc 'pgrep -f run_whoscored_scraper && echo running || echo clean'

# 2. Поднять FS (или byparr после swap)
docker compose up -d flaresolverr
until curl -fsS http://127.0.0.1:8191/health 2>/dev/null; do sleep 5; done

# 3. Smoke 5 matches (cold start)
docker exec airflow-scheduler bash -lc '
cd /opt/airflow && python -u dags/scripts/run_whoscored_scraper.py \
  --leagues "ENG-Premier League" --seasons 2025 \
  --events-only --max-matches 5 \
  --proxy-file "" \
  --flaresolverr-url http://flaresolverr:8191 \
  --output /tmp/smoke.json'

# 4. Trino COUNT
docker exec airflow-scheduler bash -lc '
cd /opt/airflow && python -c "
import trino, os, urllib3
urllib3.disable_warnings()
conn = trino.dbapi.connect(host=\"trino\", port=int(os.environ.get(\"TRINO_PORT\",8443)),
    http_scheme=\"https\", verify=False, user=\"trino\", catalog=\"iceberg\",
    auth=trino.auth.BasicAuthentication(\"trino\", os.environ[\"TRINO_PASSWORD\"]))
cur = conn.cursor()
cur.execute(\"SELECT count(*), count(distinct game_id) FROM iceberg.bronze.whoscored_events\")
print(cur.fetchall())
"'

# 5. После работающего fix — full prod run
docker exec airflow-scheduler airflow dags unpause dag_ingest_whoscored
docker exec airflow-scheduler airflow tasks clear dag_ingest_whoscored \
  -t scrape_whoscored --yes
# validate_events должен пройти на 500 k+
```

## Открытые риски

- **WhoScored CF tightening продолжается** — за время простоя CF может стать ещё жёстче, byparr тоже может не выдержать. План B → option Y или платный API.
- **Proxy pool** — 999 residential проксей в `/opt/airflow/proxys.txt` массово забанены. Нужен refresh пула либо переход на ISP-pinned proxy.
- **execution_timeout=6h** в `WHOSCORED_ARGS` — оставлен. Любой рабочий вариант должен укладываться: byparr smoke → ~10 min, прод 345 → ~30–60 min при стабильной работе.
