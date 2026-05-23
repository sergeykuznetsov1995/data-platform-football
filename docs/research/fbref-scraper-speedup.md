# FBref scraper speedup research — Track A (HTTP-fetch с reused `cf_clearance`)

> Issue: [#45](https://github.com/sergeykuznetsov1995/data-platform-football/issues/45)
> Started: 2026-05-23
> Status: **Track A не разблокирован одним только cookie-API workaround'ом.**
> Метод (d) — custom CDP generator — работает в **изолированном** репро
> (next page.get() = 0.62s = baseline). Но в production scraper'е event loop
> корраптится **другими** broken парсерами nodriver 0.48.1
> (`Response.from_json` валит `KeyError: 'charset'` на каждом HTTP-response),
> и наш `_extract_cookies_from_nodriver` тоже таймаут'ит 5s. 10-match bench:
> 0 ускорения, `http_fetch_ok=0`. См. секцию «Bench A vs baseline».

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
  Решение: оценить только cherry-pick парсеров в follow-up issue.
- **D. Tuning констант** (`MAX_SLOW_PROXY_RETRIES`, `MAX_CONSECUTIVE_FAILURES`,
  `time.sleep(0.5)`, CF_COOKIE_PREWARM) — отдельные мелкие PR'ы, follow-up
  issue.

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

## Final verdict

**Track A не разблокирован одним только cookie-API workaround'ом.**

### Что сделано

1. ✅ Изолированный репро method (d) — custom CDP generator обходит
   `Cookie.from_json` (`scripts/research/repro_nodriver_cookies_hang.py`).
2. ✅ Production-код обновлён: `_extract_cookies_from_nodriver` и
   `export_cf_cookies` мигрированы на raw generator
   (commit `151808e` в `feature/issue-45-fbref-http-fetch`).
3. ✅ HTTP fast-path встроен в `_fetch_page` (попытка `_fetch_page_http`
   перед nodriver, fallback при CF/incomplete HTML).
4. ✅ Baseline + Track A бенчмарки на 10 матчах APL 2025/26.
5. ❌ Speedup = 0. `Response.from_json` blocker не позволяет init.

### Что нужно

Полный fix Track A требует **дополнительной работы**:

1. **Bump nodriver** на 0.50+/0.52+ (если есть upstream-fix для
   `Response.from_json`). Самый цельный путь, но риск регрессий
   в CF-bypass.
2. **Monkey-patch `Connection._listener`** или
   `nodriver.cdp.util.parse_json_event`, чтобы swallow parser exceptions
   (~5 строк, точечный фикс). Не зависит от upstream-релиза.
3. **Pin старый nodriver** (до регрессий). Trade-off: потеряем фиксы
   CF-bypass плагина.

Рекомендация: попробовать (2) — monkey-patch — в follow-up issue. Это
unblocks Track A с минимальными изменениями. Если monkey-patch не
работает или ломает что-то — fallback на (1).

## Track B/C/D — короткие выводы (без бенчмарков)

- **B. soccerdata FBref (Selenium backend)** — тот же браузерный подход,
  что у нас, **не быстрее**. Из issue #45: не даст выигрыша по скорости/трафику.
  Решение: оставляем как есть.
- **C. ScraperFC.FBref cherry-pick** — botasaurus/Chromium на каждый
  fetch; GPL-3.0 лицензия блокирует прямую зависимость
  (см. [feedback_scraperfc_sofascore_blocked](../../memory/feedback_scraperfc_sofascore_blocked.md)).
  Решение: оценить только cherry-pick парсеров в follow-up issue.
- **D. Tuning констант** (`MAX_SLOW_PROXY_RETRIES`, `MAX_CONSECUTIVE_FAILURES`,
  `time.sleep(0.5)`, CF_COOKIE_PREWARM) — отдельные мелкие PR'ы, follow-up
  issue.

## Next steps

1. Follow-up issue: «monkey-patch `Connection._listener` против nodriver
   0.48.1 parser regressions» — детали выше, priority p1.
2. Follow-up issue: «evaluate nodriver 0.50+ bump» — backup опция.
3. Follow-up issue: «Track D constant tuning» — отдельные PR'ы.
4. Follow-up issue: «evaluate ScraperFC.FBref / soccerdata alternatives» — priority p3.
