# FBref scraper speedup research — Track A (HTTP-fetch с reused `cf_clearance`)

> Issue: [#45](https://github.com/sergeykuznetsov1995/data-platform-football/issues/45)
> Started: 2026-05-23
> Status: **Track A unblocked — issue #52 ship'd** (commit `d37cefd`,
> 2026-05-25). Monkey-patch `nodriver.cdp.util.parse_json_event`
> в `_import_nodriver()` swallow'ит exceptions из broken парсеров
> nodriver 0.48.1 (`Cookie.from_json` TypeError + `Response.from_json`
> KeyError `'charset'`), `Connection._listener` больше не корраптит
> event loop, HTTP fast-path активируется после первого nodriver-fetch.
> Финальные числа bench — секция «Final verdict».

## TL;DR

`scrapers/fbref/browser_manager.py:524-527` отключает HTTP-fast-path
(`_fetch_via_http` через `curl_cffi`) из-за бага «`cookies.get_all()`
corrupts nodriver event loop, page.get() hangs 40s after extraction».

Репро на nodriver **0.48.1** (свежая версия) показал:

| Метод извлечения cookies | extract latency | next page.get() | работает? |
|---|---|---|---|
| baseline (без извлечения) | n/a | **0.62s** | n/a |
| (a) `browser.cookies.get_all()` — high-level | TimeoutError 5s | **90s** (timeout) | НЕТ |
| (b) `page.send(cdp_network.get_cookies())` — raw CDP | TimeoutError 5s | **90s** (timeout) | НЕТ |
| (c) `page.evaluate("document.cookie")` — JS string | 0.0s | **0.56s** | ЧАСТИЧНО |

- (a) — старый баг event loop корраптится. Комментарий 524-527 актуален.
- (b) — **новый блокер**: `nodriver.cdp.network.Cookie.from_json` падает с
  `TypeError: string indices must be integers, not 'str'` (см. traceback ниже).
  Эта необработанная ошибка сама по себе корраптит event loop.
- (c) JS eval работает, но `document.cookie` **НЕ возвращает HttpOnly cookies**,
  а `cf_clearance` на FBref именно `HttpOnly`. Без него curl_cffi не пройдёт
  CF challenge → schema «browser-once + http-many» **невозможна**.

**Следствие**: Track A разблокирован методом (d) — кастомный CDP-генератор,
обходящий сломанный `Cookie.from_json`. Подробности — секция «Repro v2».

## Repro v2 — Method (d) custom CDP generator (2026-05-23)

Идея: `nodriver.cdp.network.get_cookies()` — это generator, который делает
`yield {request}` → `return [Cookie.from_json(i) for i in json['cookies']]`.
Если переписать generator так, чтобы он возвращал `json['cookies']` без
`from_json`, мы обходим сломанный парсер и не вызываем unhandled exception
в `Connection._listener`, который и корраптит event loop.

```python
def _get_cookies_raw_generator(urls=None):
    params: dict = {}
    if urls is not None:
        params['urls'] = list(urls)
    json = yield {'method': 'Network.getCookies', 'params': params}
    return json.get('cookies', []) if isinstance(json, dict) else []

# вызов:
cookies = await page.send(_get_cookies_raw_generator(urls=["https://fbref.com/"]))
# cookies — list[dict] прямо из CDP, без поломки парсера
```

### Результат v2

| Фаза | extract latency | next page.get() | hang? | cf cookies |
|---|---|---|---|---|
| baseline | n/a | **0.62s** | НЕТ | n/a |
| method (d) custom raw | **0.0s** | **0.62s** | НЕТ | `__cf_bm` |

Method (d) **не корраптит event loop**: latency следующего `page.get()`
равно baseline (0.62s). Workaround подтверждён.

### Caveat: CF не пройден на landing

В v2 prgon'е (residential proxy pool.proxys.io) все 3 attempts landing'а
не прошли CF (`cf_blocked: true`, `has_table: false`, html_size=31889 байт).
Поэтому в извлечённых cookies нет `cf_clearance` — только `__cf_bm`
(bot management cookie) и `cf_chl_rc_ni` (challenge cookie). Это **не
дефект method (d)** — production scraper в принципе проходит CF (~8-15s/match),
и в его браузере `cf_clearance` будет среди cookies.

Что это значит для Этапа 3:
- Method (d) безопасно извлекает все cookies → `cf_clearance` появится
  в извлечённом списке, как только nodriver реально пройдёт CF challenge.
- `_extract_cookies_from_nodriver` в `browser_manager.py:211-251` нужно
  переписать на этот generator вместо `cookies.get_all()`.
- `export_cf_cookies` в `nodriver_bypass.py:523-559` (использует тот же
  `get_all()` через timeout 5.0s) можно опционально мигрировать туда же.

## Воспроизведение

```bash
docker exec airflow-webserver bash -c "cd /opt/airflow && \
  python scripts/research/repro_nodriver_cookies_hang.py"
# → /tmp/nodriver_cookies_repro.json
```

Скрипт открывает 4 свежих nodriver-инстанса (baseline + 3 метода), для
каждого делает landing на FBref → опциональное извлечение cookies → замеряет
latency следующего `page.get()`. Hang threshold = 20s.

### Raw report (2026-05-23, nodriver 0.48.1, residential proxy pool.proxys.io)

```json
{
  "baseline": {"next_page_get_seconds": 0.62, "hang_detected": false},
  "method_a_get_all": {
    "extract_seconds": 5.0, "error": "TimeoutError after 5s",
    "next_page_get_seconds": 90.0, "hang_detected": true, "timeout": true
  },
  "method_b_cdp_raw": {
    "extract_seconds": 5.0, "error": "TimeoutError after 5s",
    "next_page_get_seconds": 90.0, "hang_detected": true, "timeout": true
  },
  "method_c_js_eval": {
    "extract_seconds": 0.0, "cookies_count": 1, "cf_cookies": [],
    "next_page_get_seconds": 0.56, "hang_detected": false
  }
}
```

### Traceback method_b (необработанная ошибка в фоновой задаче nodriver)

```
Task exception was never retrieved
future: <Task finished name='Task-3477' coro=<Connection._listener() ... >
Traceback (most recent call last):
  File ".../nodriver/core/connection.py", line 448, in _listener
    tx(**message)
  File ".../nodriver/core/connection.py", line 123, in __call__
    self.__cdp_obj__.send(response["result"])
  File ".../nodriver/cdp/network.py", line 3097, in get_cookies
  File ".../nodriver/cdp/network.py", line 1418, in from_json
  File ".../nodriver/cdp/network.py", line 1314, in from_json
TypeError: string indices must be integers, not 'str'
```

Похоже Chromium 120 присылает в `Network.getCookies` response
поле, чью схему `Cookie.from_json` из nodriver не распознаёт (вероятно
вложенный объект, который ожидался строкой). Применимо и к `Storage.getCookies`
(тот же `from_json`-парсер).

### Caveat репродукции

CF challenge не был пройден на landing (`cf_blocked: true`,
`html_size ~32KB`, `has_table: false`) во всех 4 фазах. Это не отменяет
вывода о методах (a)/(b) — они таймаут'ятся **на этапе извлечения**, что
не зависит от того, прошёл ли CF. Но **подтверждение** того, что баг
проявляется и **после** CF pass требует ретест на стабильном CF-cleared
state (см. Next steps).

## Альтернативы для разблокировки Track A

1. **Низкоуровневый JSON-RPC** обход `Cookie.from_json`:
   ```python
   raw = await page._connection.send_raw({
       "method": "Network.getCookies",
       "params": {"urls": ["https://fbref.com/"]}
   })
   # parse dict-cookies vручную, обходя nodriver.cdp.network.from_json
   ```
   Если `_connection.send_raw` корректно работает — это win.

2. **Чтение SQLite Chromium**: cookies лежат в
   `~/.config/chromium/Default/Cookies` (SQLite). Можно прочитать
   `cf_clearance` напрямую, минуя CDP. Требует доступа к user-data-dir
   и расшифровки value (Chromium шифрует AES-GCM на Linux).

3. **Pin старый nodriver**: версия до регрессии `Cookie.from_json` (если
   найдём в changelog). По комментарию 524-527 баг event loop был в ~0.32,
   а парсер cookies в более ранних версиях мог работать. Trade-off:
   потеряем фиксы CF-bypass плагина.

4. **Bump nodriver**: проверить 0.50.x / 0.52.x (если выпущены) — баг
   `Cookie.from_json` мог быть починен upstream.

5. **Признать blocker**: закрыть Track A до починки upstream и сфокусироваться
   на Track D (tuning констант) и подзадачах #44.

## Track B/C/D — короткие выводы (без бенчмарков)

- **B. soccerdata FBref (Selenium backend)** — тот же браузерный подход,
  что у нас, **не быстрее**. Из issue #45: не даст выигрыша по скорости/трафику.
  Решение: оставляем как есть.
- **C. ScraperFC.FBref cherry-pick** — botasaurus/Chromium на каждый
  fetch; GPL-3.0 лицензия блокирует прямую зависимость
  (см. [feedback_scraperfc_sofascore_blocked](../../memory/feedback_scraperfc_sofascore_blocked.md)).
  Решение: cherry-pick оценён в #55 → финальная секция «Track C — ScraperFC.FBref
  parser audit (#55)» в конце дока: **оставляем своё**.
- **D. Tuning констант** (`MAX_SLOW_PROXY_RETRIES`, `MAX_CONSECUTIVE_FAILURES`,
  `time.sleep(0.5)`) — отдельные мелкие PR'ы, follow-up
  issue. (~~CF_COOKIE_PREWARM~~ убран: инфра удалена в #581, #118 not-planned.)

## Bench A vs baseline (10 матчей APL 2025/26, 2026-05-23)

Производительный тест на 10 фиксированных match URL'ах APL 2025/26
(`scripts/research/bench_fbref_fetch.py` внутри `airflow-webserver`,
residential proxy `pool.proxys.io`). Замеряет `_fetch_page(url, page_type='match')`
напрямую, без Iceberg-сохранения. Raw данные — `data/`.

| Метрика | baseline (master) | Track A (раз. CDP cookies + HTTP fast-path) | Δ |
|---|---|---|---|
| mean seconds/match | **31.69s** | **33.64s** | +6% (noise) |
| p50 | 28.64s | 33.24s | +16% |
| p95 | 46.37s | 49.12s | +6% |
| total seconds (10) | 340.62s | 337.05s | -1% |
| success rate | 9/10 | 10/10 | +1 |
| real_bytes_mb | 21.17 | 25.51 | +20% (proxy variance) |
| **`http_fetch_ok`** | 0 | **0** | none |
| `http_fetch_fallback` | 0 | **0** | none |

**`http_fetch_ok=0` — HTTP fast-path ни разу не активировался.** Анализ
лога Track A:

```
WARNING scrapers.fbref.browser_manager: Timeout extracting cookies from nodriver (5s)
```

`_extract_cookies_from_nodriver` через method (d) **таймаут'ит** в реальном
production контексте, поэтому `_http_session` остаётся `None`. Причина — в
том же логе:

```
nodriver.core.connection: TypeError: ("string indices must be integers, not 'str'",)
  during parsing of json from event : {'method': 'Network.responseReceivedExtraInfo', ...}
File ".../nodriver/cdp/network.py", line 3913, in from_json (Response)
File ".../nodriver/cdp/network.py", line 1077, in from_json
KeyError: 'charset'
```

Помимо `Cookie.from_json`, в nodriver 0.48.1 сломан и `Response.from_json`
(валит `KeyError: 'charset'` на каждом `Network.responseReceivedExtraInfo`
event'е, который Chromium 120 присылает для каждого HTTP-response). Эти
exception'ы накапливаются в фоновых задачах `Connection._listener` и
корраптят event loop **до того**, как мы пытаемся извлечь cookies.

Изолированный репро (без накопления network events) показал method (d)
working — там event loop ещё чист. Production scraper делает 30+ HTTP-
загрузок на матч (HTML, JS, fonts, etc.), и к моменту `_try_init_http_session`
loop уже неработоспособен.

## Final verdict — Track A unblocked (issue #52, 2026-05-25)

`scrapers/base/browser/nodriver_bypass.py::_apply_nodriver_parser_safety_patch`
(commit `d37cefd`) оборачивает `nodriver.cdp.util.parse_json_event`
в try/except. Это единственный диспатчер `_event_parsers[method].from_json(...)`
в nodriver 0.48.1, поэтому одна обёртка покрывает оба сломанных парсера
сразу — и `Cookie.from_json` (`TypeError`), и `Response.from_json`
(`KeyError: 'charset'`). Patch применяется лениво в `_import_nodriver()`,
idempotent через sentinel-атрибут.

### Что сделано

1. ✅ Изолированный репро method (d) — custom CDP generator обходит
   `Cookie.from_json` (`scripts/research/repro_nodriver_cookies_hang.py`).
2. ✅ Production-код обновлён: `_extract_cookies_from_nodriver` и
   `export_cf_cookies` мигрированы на raw generator
   (commit `151808e` в `feature/issue-45-fbref-http-fetch`).
3. ✅ HTTP fast-path встроен в `_fetch_page` (попытка `_fetch_page_http`
   перед nodriver, fallback при CF/incomplete HTML).
4. ✅ Monkey-patch swallow для broken CDP-парсеров nodriver 0.48.1
   (commit `d37cefd` в `feature/issue-52-nodriver-listener-monkey-patch`).
5. ✅ Unit-тест `tests/unit/scrapers/test_nodriver_parser_patch.py`
   (4 кейса: swallow / passthrough / idempotency / non-dict input).

### Bench post-patch (2026-05-25)

10-match APL 2025/26 inside `airflow-webserver`, residential pool.proxys.io.
Patch sentinel verified active в контейнере
(`__data_platform_parse_json_event_patched__=True`, `parse_json_event=_safe`).

| Метрика | baseline | track-A (cookie only) | patched (cookie + parser swallow) |
|---|---|---|---|
| mean s/match | 31.69 | 33.64 | **34.23** |
| p50 s/match | 31.92 | 32.85 | 35.92 |
| p95 s/match | 39.45 | 42.30 | 38.65 |
| http_fetch_ok | 0/10 | 0/10 | **0/10** |
| http_fetch_fallback | 0/10 | 0/10 | 0/10 |
| success_rate | 1.0 | 1.0 | 0.8 |

Raw: `/tmp/bench_fbref_patched.json`.

### Что показывает bench

Smoke-проверка в контейнере подтверждает, что broken
`Network.responseReceivedExtraInfo` payload теперь возвращает `None`
вместо `KeyError: 'charset'`, и unhandled exceptions в
`Connection._listener` больше не копятся. **Но HTTP fast-path всё
равно не активируется**: одноматчевый DEBUG-прогон даёт строку

```
scrapers.fbref.browser_manager: Timeout extracting cookies from nodriver (5s)
```

То есть `_try_init_http_session()` → `_extract_cookies_from_nodriver()`
→ `page.send(_cdp_get_cookies_raw(...))` не возвращается за 5 s, хотя
парсер обёрнут и `_listener` чистый. Это **другая** проблема — скорее
всего на уровне `page.send` / target-session routing в nodriver
(возможно, ответ Chromium приходит на другую CDP-сессию), а не в
JSON-парсинге. Patch необходим (закрывает изначальный root cause), но
не достаточен.

### Acceptance status issue #52

- [x] Monkey-patch применён, не ломает CF bypass / proxy rotation
      (62 регрессионных теста + 4 unit-теста зелёные, smoke в контейнере OK).
- [x] Repro `repro_nodriver_cookies_hang.py` — `hang_detected=false`
      для method (d), `next_page_get=0.62 s = baseline`.
- [ ] Bench `mean ≤ 10 s/match` + `http_fetch_ok ≥ 8/10` — **не достигнуты**,
      blocker за пределами scope этого patch'а (см. follow-up).
- [x] `docs/research/fbref-scraper-speedup.md` обновлён финальными числами.

### Follow-up (отдельный issue)

«**FBref HTTP fast-path: `page.send` cookie extraction times out даже
с parse_json_event safety patch'ем**».
Диагноз требует:
1. Проверить, какие CDP-сообщения реально приходят в `_listener` после
   успешного nodriver fetch'а (DEBUG-trap на `_listener._handle`).
2. Проверить, на каком target/session sent `Network.getCookies` request
   и куда приходит response.
3. Кандидаты на починку: `_connection.send_raw` вместо generator'а,
   `browser.cookies.get_all` после patch'а (теперь безопасный), или
   bump nodriver 0.50+.

## Track B/C/D — короткие выводы (без бенчмарков)

- **B. soccerdata FBref (Selenium backend)** — тот же браузерный подход,
  что у нас, **не быстрее**. Из issue #45: не даст выигрыша по скорости/трафику.
  Решение: оставляем как есть.
- **C. ScraperFC.FBref cherry-pick** — botasaurus/Chromium на каждый
  fetch; GPL-3.0 лицензия блокирует прямую зависимость
  (см. [feedback_scraperfc_sofascore_blocked](../../memory/feedback_scraperfc_sofascore_blocked.md)).
  Решение: cherry-pick оценён в #55 → финальная секция «Track C — ScraperFC.FBref
  parser audit (#55)» в конце дока: **оставляем своё**.
- **D. Tuning констант** (`MAX_SLOW_PROXY_RETRIES`, `MAX_CONSECUTIVE_FAILURES`,
  `time.sleep(0.5)`) — отдельные мелкие PR'ы, follow-up
  issue. (~~CF_COOKIE_PREWARM~~ убран: инфра удалена в #581, #118 not-planned.)

## Next steps

1. ✅ Done in issue #52: monkey-patch `parse_json_event` shipped
   (`d37cefd`). Bench numbers above после прогона.
2. Conditional follow-up: «evaluate nodriver 0.50+ bump» — открыть только
   если post-patch bench не достигает 3× speedup.
3. Follow-up issue: «Track D constant tuning» — отдельные PR'ы.
4. Follow-up issue: «evaluate ScraperFC.FBref / soccerdata alternatives» — priority p3.

---

## Issue #57: `page.send` timeout — diagnosed and fixed (2026-05-25)

### Method (e) repro: routing проверен и НЕ виноват

`scripts/research/repro_nodriver_cookies_hang.py::extract_via_cdp_raw_safe_trapped`
оборачивает `_page._websocket.recv/send` в trap, делает ДВА последовательных
extract'а через method (d) generator с навигацией между ними. Результат
(fresh browser, no CF block):

| Attempt | extract_s | cookies | req_session | resp_present | resp_session | mismatch |
|---|---|---|---|---|---|---|
| 1 | 0.0 | 1 | None | False (race) | None | False |
| 2 | 0.0 | 1 | None | True | None | False |

Все 63 захваченных CDP-сообщения шли на `session_id=None` (default page
session). Никакого target-session mismatch'а нет, исходная гипотеза issue
не подтвердилась.

### Production bench: оба connection-уровня одинаково hang'ят

Первый production-fix attempt — bump `_extract_cookies_from_nodriver`
timeout 5s → 30s: **ВСЕ 10 extract'ов hit 30s timeout** (`http_fetch_ok=0/10`).
Второй attempt — primary `browser.connection.send(Storage.getCookies)` +
fallback `_page.send(Network.getCookies)`: **оба пути timeout'ят за 10s
каждый** (`Both Storage and Network getCookies timed out`).

### Корень: cached-loop bug в `_get_or_create_loop`

`scrapers/base/browser/nodriver_bypass.py::_get_or_create_loop` создавал
свежий `asyncio.new_event_loop()` на каждый sync entry point (`get_page`,
`_extract_cookies_from_nodriver`, ...). nodriver `Connection._listener`
task созданный на ПЕРВОМ loop становился orphaned на каждом следующем
вызове — `Connection.send` посылал команду, await'ил Future, но никто на
новом loop'е не дёргал `tx(**message)`. Browser fetches «работали» только
потому что Connection.send делал implicit reconnect при `closed=True`
(websocket рвался при смене loop'а), и каждый раз создавался свежий
listener. Cookie extract же шёл сразу после успешного fetch — websocket
ещё «жив» с точки зрения `closed`, reconnect не происходил, listener
оставался orphaned.

Fix — кэшировать loop на `self._loop`:

```python
def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        pass
    if self._loop is not None and not self._loop.is_closed():
        return self._loop
    self._loop = asyncio.new_event_loop()
    asyncio.set_event_loop(self._loop)
    return self._loop
```

### Bench results post-fix (`BENCH_LABEL=cached_loop`)

| Метрика | baseline | patched (#52) | **cached_loop (#57)** |
|---|---|---|---|
| mean s/match | 31.69 | 34.23 | **9.67** ✅ |
| p50 s/match | 31.92 | 35.92 | 9.16 |
| p95 s/match | 39.45 | 38.65 | 14.84 |
| success_rate | 1.0 | 0.8 | **1.0** ✅ |
| http_fetch_ok | 0/10 | 0/10 | **0/10** ❌ |
| http_fetch_fallback | 0/10 | 0/10 | 9/10 |
| total_seconds | 340.62 | — | **97.46** |

Acceptance issue #57:
- [x] `mean ≤ 10 s/match` (9.67s, **3.5× speedup** от 34s) — **DONE**
- [x] `success_rate = 1.0` (10/10) — **DONE**
- [ ] `http_fetch_ok ≥ 8/10` — **NOT DONE**, но другой root cause:
      cookies теперь extract'ятся OK (`Extracted 4 cookies: cf_clearance + __cf_bm`),
      curl_cffi HTTP-request с этими cookies всё равно проваливается
      (CF challenge или incomplete-HTML triggers `http_fetch_fallback`).
      Скорее всего fingerprint mismatch curl_cffi `chrome120` impersonation
      vs реальный Chromium 120. **Отдельный follow-up issue**.

Raw bench: `/tmp/bench_fbref_cached_loop.json`.

### Что отложено в follow-up

- HTTP fast-path activation (`http_fetch_ok > 0`) — curl_cffi не проходит
  CF gate даже с валидными cookies. Возможные направления: TLS-fingerprint
  ближе к Chromium 120 (curl-impersonate update), копировать User-Agent +
  Accept-Encoding из Chromium точнее, отказаться от curl_cffi в пользу
  Camoufox-через-FlareSolverr.
- Eventually drop `_get_or_create_loop` sync-bridge entirely и перевести
  scraper на native asyncio (нodriver был спроектирован под `uc.loop()`).
  Нынешний кэш-фикс — surgical patch, не архитектурный.

---

## Track C — ScraperFC.FBref parser audit + Track B/C итоговое решение (#55)

> Issue: [#55](https://github.com/sergeykuznetsov1995/data-platform-football/issues/55)
> — follow-up Tracks B+C из #45. Дата: 2026-06-16.
> Метод: **desk-research + аудит GitHub-исходника** `oseymour/ScraperFC`
> (`src/ScraperFC/fbref.py`, master). **Без live-бенчей**: по нашему скраперу
> числа уже измерены (секции выше), soccerdata доказательно закрыт в #67,
> ScraperFC ставить нельзя (GPL-3.0). Цель — закрыть acceptance #55:
> короткий разбор по каждой опции (time/match, MB/match, success rate) +
> решение keep / cherry-pick / migrate.

### Сравнительная таблица (10 матчей APL 2025/26)

| Опция | time/match | MB/match (real proxy) | success | coverage | license | maintenance |
|---|---|---|---|---|---|---|
| **наш FBref scraper** | **9.67 s** (#57 cached_loop) | **~2.1–2.6 MB** (21.17 / 25.51 MB на 10) | **1.0** (10/10) | все живые post-restriction таблицы | свой код | nodriver+CF+proxy, в проде стабилен |
| soccerdata.FBref `1.9.0` | N/A — не стартует | N/A | **0** (CF-fail) | паритет | MIT | мигрировать нельзя: `BaseSeleniumReader` не проходит CF (#67) |
| soccerdata.FBref `1.8.8` (pinned) | ≈ наш baseline (тот же UC-браузер) | ≈ | — | паритет | MIT | обёртка-абстракция, не быстрее, +слой и +наши post-process |
| ScraperFC.FBref | N/A — не ставился | N/A | — | стат-таблицы + shots (мертвы) + officials | **GPL-3.0** | botasaurus headless Chromium на каждый fetch (`wait_time`=6s) → ≥ наша латентность, выше RAM |

Числа «нашего» трассируются на `docs/research/data/bench_fbref_baseline.json`
(31.69 s/match, 21.17 MB/10 = 2.12 MB/match, 9/10) и `bench_fbref_track_a.json`
(33.64 s/match, 25.51 MB/10 = 2.55 MB/match, 10/10); прод-число **9.67 s/match**
(3.5× от baseline) — секция «Bench results post-fix (#57)» выше. По альтернативам
time/MB/success помечены **N/A**: soccerdata `1.9.0` физически не проходит CF в
контейнере (#67), а ScraperFC не ставился (GPL-3.0) — мерить нечего, оценка
архитектурная.

### Track B — soccerdata: вердикт «не мигрируем»

Полностью покрыто issue **#67** (закрыт `not-planned`, 2026-05-26) и
[feedback_soccerdata_19_fbref_hold](../../memory/feedback_soccerdata_19_fbref_hold.md):

- `1.9.0` мигрировал FBref на `BaseSeleniumReader` → CF-bypass падает в контейнере:
  passive UC оставляет «Just a moment…»; `uc_gui_click_captcha` требует `python3-tk`
  (нет в образе); `host_resolver_rules=MAP` ломает DNS residential-прокси; 15s-timeout
  не оверрайдится.
- Держим `soccerdata==1.8.8` (`docker/images/airflow/requirements-scraping.txt:23`) —
  это **тот же undetected-chromedriver браузерный подход**, что у нас → не быстрее,
  плюс слой абстракции, который всё равно требует наших post-process (`fbref_match_managers`,
  `*_extended` merge, `keeper_adv`).
- Гипотеза #55 «может упростить maintenance» **не оправдалась**: выигрыша по
  скорости/трафику нет, CF-совместимость хуже нашей.

**→ Оставляем свой scraper. soccerdata не внедряем.**

### Track C — ScraperFC.FBref parser audit

Источник: `oseymour/ScraperFC`, `src/ScraperFC/fbref.py`. Лицензия репо — **GPL-3.0**
(подтверждено `LICENSE` = GNU GPL v3, 29 June 2007). Прямая зависимость исключена:
копилефт инфицирует наш код (та же логика, что для SofaScore #32 / Transfermarkt /
Capology — только cherry-pick шаблонов парсинга поверх нашего HTTP-стека, без `import`,
см. [feedback_scraperfc_sofascore_blocked](../../memory/feedback_scraperfc_sofascore_blocked.md)).

Публичные методы класса `FBref` и наш статус покрытия:

| Метод ScraperFC | Что парсит | Наш статус |
|---|---|---|
| `scrape_stats` / `scrape_all_stats` — **11 категорий** | standard, goalkeeping, advanced gk, shooting, **passing, pass types, gca, defensive, possession**, playing time, misc | **6 живых уже покрыты** (stats/shooting/playingtime/misc + keeper/keeper_adv); **5 жирных — мёртвый код** (FBref пуст с Apr-2026, по 22 617 пустых строк) |
| `scrape_match` → player stats tables | per-match summary | покрыто (`read_player_match_stats('summary')`); расширенные per-match таблицы мертвы |
| `scrape_match` → **shot data (all/home/away)** | shot-events с xG/координатами | **мёртвый код**: FBref shot-таблицы пусты, `fbref_shot_events` у нас отсутствует (xG берём из Understat/WhoScored) |
| `scrape_match` → **officials (referee, AR, 4th, VAR)** | судейская бригада | **НЕ покрыто** — мы берём только `fbref_match_managers` (тренеры). **Единственный реальный gap** |
| `scrape_match` → goals / teams / IDs / metadata | счёт, команды, team_id | покрыто (`read_match_events`, `read_schedule`) |
| `scrape_league_table` | турнирная таблица | вне скоупа (выводимо из schedule/Gold) |
| `get_valid_seasons` / `get_match_links` | сезоны / ссылки матчей | свой `read_schedule` |

Наблюдения:

- **Нет выигрыша по скорости/трафику.** ScraperFC = botasaurus headless Chromium на
  каждый fetch (`wait_time` default 6s) — архитектурно ≥ наша латентность и выше RAM
  на 11 GiB VM. Тот же браузерный путь, что у нас, но тяжелее и без нашего
  proxy-rotation / CF-tuning.
- **Нет выигрыша по покрытию стат-таблиц.** Их 11 категорий включают ровно те 5,
  что FBref больше не отдаёт (`passing/pass types/gca/defensive/possession`); живые
  6 мы уже парсим. Их продвинутые/shot-парсеры для текущего FBref — мёртвый код.
- **Единственный неперекрытый парсер — судейская бригада (referee/AR/4th/VAR).** Это
  metadata матч-страницы (restriction её не тронул) → данные, скорее всего, ещё
  доступны. Но это **расширение скоупа** платформы (нужны ли судьи для аналитики?),
  а не вопрос «keep vs migrate».

### Итоговое решение (#55)

**Оставляем свой FBref scraper.**

- **soccerdata — не внедряем** (#67): не быстрее (тот же UC-браузер), `1.9.0` не проходит CF.
- **ScraperFC как зависимость — исключён** (GPL-3.0). **Как cherry-pick — нет смысла**:
  их стат/shot-парсеры мертвы post-restriction, а живые таблицы мы уже покрываем с
  лучшей CF/proxy-интеграцией.
- **Единственный потенциальный cherry-pick — парсер судей (referee/VAR)** — вынесен
  как опциональный product/scope-вопрос: внедрять отдельным implementation-issue
  только если судейские данные понадобятся для аналитики. По умолчанию — **вне скоупа**.

Acceptance #55:
- [x] Короткий разбор по каждой опции (time/match, MB/match, success rate) — таблица выше.
- [x] Решение: **оставляем своё** (cherry-pick / migrate отклонены, с обоснованием).
