# FlareSolverr Proxy Traffic Audit — WhoScored + SoFIFA

> **Issue:** [#616](https://github.com/sergeykuznetsov1995/data-platform-football/issues/616) · branch `feature/issue-616-cut-browser-proxy-traffic`
>
> **Question:** «Сколько дорогого residential-proxy трафика (~$4/ГБ) тратят два
> браузерных скрапера на FlareSolverr — WhoScored и SoFIFA — на матч / на игрока,
> и есть ли что снижать?»
>
> Сиблинг по FBref: `fbref-proxy-traffic-audit.md` (§9 — там доказано, что для
> FBref blocklist не помогает; реальный рычаг — cold-start, вынесен в #624).

## TL;DR

1. До этой работы у WhoScored и SoFIFA **не было никакого учёта трафика** — в
   отличие от FBref (nodriver/CDP `loadingFinished`). FlareSolverr — это
   Camoufox (Firefox), CDP Network events недоступны.
2. Добавлен in-process учёт в единственной общей точке —
   `FlareSolverrClient` (через неё идут ВСЕ пути: WhoScored events + schedule,
   SoFIFA). Считаем: `fs_response_bytes`, `requests`, per-URL разбивку,
   `sessions_created`, `cf_challenge_failures`. Surfaced в
   `WhoScoredScraper.get_traffic_stats()` / `SoFIFAScraper.get_traffic_stats()`
   и в output JSON обоих run-скриптов (ключ `traffic`).
3. **Важный кавеат честности (урок #131): `fs_response_*` — это НЕ proxy-МБ.**
   Это размер HTML, который FlareSolverr вернул нам. Camoufox качает
   картинки/CSS/JS/XHR через прокси и отдаёт нам только отрендеренный HTML, так
   что `fs_response_bytes` — **нижняя граница** реального proxy-трафика и
   **не чувствует** блокировку под-ресурсов.
4. Настоящий per-match proxy-МБ меряется **на VM** на уровне контейнера
   (`/proc/net/dev` дельта контейнера `flaresolverr`), обрамляя прогон
   bench-харнесса `scripts/research/bench_flaresolverr_fetch.py`.
5. Гипотеза главного драйвера трафика: **CF cold-start на каждую ротацию
   сессии** (FlareSolverr пересоздаёт сессию: SoFIFA — каждые 4 запроса,
   WhoScored events — каждые 10, schedule — каждые 8; каждая пересоздача =
   свежий Turnstile-challenge). `sessions_created` это и измеряет.
6. **Блокировка ресурсов отложена:** FlareSolverr v3.4.6 API не поддерживает
   request interception (нет `requestInterception` / `blockingPatterns`), так
   что «резать» можно только на уровне прокси (mitmproxy/filtering proxy) — это
   отдельный тикет, по реальным числам.

## 1. Что добавлено (инструментирование — shipped)

| Компонент | Файл | Что |
|---|---|---|
| Счётчики + `get_traffic_stats()` | `scrapers/base/flaresolverr_client.py` | `fs_response_bytes`, `requests`, `_bytes_by_url`/`_requests_by_url` (host+path, query отброшен), `sessions_created`, `cf_challenge_failures` |
| WhoScored surface | `scrapers/whoscored/scraper.py` | `get_traffic_stats()` → `{events, schedule}` (две независимые FS-сессии); events-снимок копится в `_last_events_traffic` |
| SoFIFA surface | `scrapers/sofifa/scraper.py` | `get_traffic_stats()` → статы reader'а (одна сессия) |
| Run-скрипты | `dags/scripts/run_{whoscored,sofifa}_scraper.py` | ключ `traffic` в output JSON |
| Bench | `scripts/research/bench_flaresolverr_fetch.py` | фикс. последовательный прогон, без Iceberg-записи; 3 source: `whoscored` (events), `whoscored-schedule` (разовый `read_schedule`), `sofifa` (player_ratings) |
| Тесты | `tests/unit/scrapers/test_flaresolverr_traffic_audit.py` | счётчики, per-URL, CF, session-recreate, shape, surface |

`get_traffic_stats()` shape:

```json
{
  "fs_response_bytes": 0,
  "fs_response_mb": 0.0,
  "requests": 0,
  "sessions_created": 0,
  "cf_challenge_failures": 0,
  "top_traffic_urls": [{"url": "host/path", "bytes": 0, "mb": 0.0, "requests": 0}]
}
```

## 2. Почему `fs_response_*` — нижняя граница, а не proxy-МБ

`FlareSolverrClient.get()` возвращает `{"html": <rendered>, ...}`. Через прокси
Camoufox скачивает полную страницу (HTML + картинки + CSS + JS + XHR), но нам
отдаёт только финальный HTML. Значит:

- `fs_response_bytes` ≈ размер HTML-ответов (+ JSON-обёртка FlareSolverr).
- Реальные proxy-байты (то, что биллится) **больше** и нам не видны in-process.
- Блокировка картинок/CSS НЕ уменьшит `fs_response_bytes` (их и так нет в HTML)
  — поэтому in-process счётчик нельзя использовать как «до/после» для блокировки;
  он полезен как (а) нижняя граница, (б) счётчик запросов и (в) **счётчик
  cold-start'ов** (`sessions_created`), который и есть главный драйвер.

## 3. Метод замера настоящего proxy-МБ (runbook на VM)

Bench гоняет фиксированный последовательный набор (без параллельных сессий — они
«отравляют» атрибуцию; это та проблема, в которую упёрся быстрый пробинг в
issue #616) и НЕ пишет в Iceberg. Обрамляем его дельтой сетевых счётчиков
контейнера `flaresolverr`:

Суммарные RX+TX байты контейнера сворачиваем в одно число (awk по
`/proc/net/dev`, чтобы не вычитать колонки руками):

```bash
docker exec flaresolverr awk 'NR>2{rx+=$2; tx+=$10} END{print rx+tx}' /proc/net/dev
```

Обрамляем **каждый** source отдельно (числа per-source не должны перекрываться):

```bash
# --- WhoScored events (n=10) ---
docker exec flaresolverr awk 'NR>2{rx+=$2;tx+=$10}END{print rx+tx}' /proc/net/dev   # BEFORE
docker exec -e BENCH_LABEL=baseline airflow-webserver bash -c \
  'cd /opt/airflow && python scripts/research/bench_flaresolverr_fetch.py --source whoscored'
docker exec flaresolverr awk 'NR>2{rx+=$2;tx+=$10}END{print rx+tx}' /proc/net/dev   # AFTER

# --- WhoScored schedule (разовый read_schedule, без Iceberg-записи) ---
#   ... тот же bracketing, --source whoscored-schedule

# --- SoFIFA player_ratings (n=10) ---
#   ... тот же bracketing, --source sofifa
```

`Δcontainer = AFTER − BEFORE` (байты за прогон одного source).
`true_proxy_bytes ≈ Δcontainer − fs_response_bytes`; делим на n матчей/игроков
(для schedule — это разовая стоимость на (лигу, сезон)) → **per-match / per-player
proxy-МБ**. Если `bronze.whoscored_schedule` пуст для events —
`BENCH_WS_MATCH_IDS="<id,id,...>"`.

`true_proxy_bytes ≈ Δcontainer − fs_response_bytes` (вычитаем обратный трафик
FlareSolverr → airflow, который и есть `fs_response_bytes`). Делим на число
матчей/игроков → **per-match / per-player proxy-МБ**.

Точный (но тяжёлый) вариант — counting forward-proxy (mitmproxy `--mode upstream`)
между FlareSolverr и residential-прокси: считает байты CONNECT-туннеля = ровно
то, что биллится, и даёт per-match атрибуцию. Здесь НЕ строим; это фундамент под
будущую блокировку (фильтрующий прокси — единственный способ «резать» для FS).

## 4. Результаты (прогон на VM, 2026-06-18)

> Прогнано через `bench_flaresolverr_fetch.py` + дельту контейнера
> (`docker exec flaresolverr awk 'NR>2{rx+=$2;tx+=$10}END{printf "%.0f",rx+tx}' /proc/net/dev`,
> BEFORE/AFTER каждого source). Артефакты: `bench_whoscored_baseline.json` (events),
> `bench_whoscored_schedule_baseline.json` (schedule) в `docs/research/data/`.
>
> **Предусловие:** замер был невозможен, пока `FlareSolverrClient` передавал
> прокси с кредами в URL → Chromium `ERR_NO_SUPPORTED_PROXIES` → весь FS-скрап
> молча возвращал 246 КБ браузер-ошибку со `status=200` (0/10 success). Починено
> в **#647** (split proxy auth) — после фикса WhoScored events 10/10.

| источник | n | requests | sessions_created | cf_fail | fs_response МБ (floor) | Δcontainer МБ | **proxy МБ / шт** |
|---|---:|---:|---:|---:|---:|---:|---:|
| WhoScored events | 10 | 10 | 1 | 0 | 18.12 | 127.15 | **~10.9 / матч** |
| WhoScored schedule | — | 14 | 2 | 0 | 5.93 | 78.14 | **~72.2 / (лига,сезон)** |
| SoFIFA player_ratings | — | — | — | — | — | — | **BLOCKED → #650** |

`proxy МБ/шт = (Δcontainer − fs_response) / n`. SoFIFA отложен: после #647 homepage
SoFIFA грузится реально, но soccerdata `read_versions` падает (`version_id` KeyError,
DOM-drift FIFA→EA FC) ещё в конструкторе reader'а — отдельный баг **#650**.

**Вывод — для WhoScored доминирует НЕ cold-start, а вес самой страницы.** На 10
матчей был всего 1 `sessions_created` (1 CF cold-start), но per-match держится
~10.9 МБ ровно (fs_response ~1.8 МБ HTML/матч стабилен по всем 10). Значит ~9 МБ
на матч — это сабресурсы (JS/CSS/картинки/реклама/XHR), которые Camoufox грузит
через прокси при рендере полной SPA-страницы и нам не возвращает. Это **зеркально
противоположно FBref** (~0.3 МБ warm, §9.4): FBref режет сабресурсы
(`BLOCKED_URL_PATTERNS`) + тянет только HTML через curl_cffi fast-path, а
FlareSolverr v3.4.6 **не умеет** request interception → платит за весь вес
страницы каждый раз. WhoScored per-match (~10.9 МБ) ≈ 36× FBref warm.

**Рычаг снижения** (для отдельного тикета) = фильтрующий **proxy-level** прокси
(mitmproxy `--mode upstream` между FlareSolverr и residential-прокси, режет
images/ads/trackers) — НЕ тюнинг `SESSION_RECREATE_EVERY` (cold-start здесь
второстепенен). Реже ротация сессии сэкономит лишь ~1 CF cold-start на пачку.

## 5. Acceptance (#616, часть WhoScored/SoFIFA)

- [x] Инструментирование FlareSolverr (учёта не было) + тесты.
- [x] Bench-харнесс + VM-runbook для per-match proxy-МБ.
- [x] **Baseline-числа** (§4): WhoScored events ~10.9 МБ/матч, schedule ~72 МБ/сезон.
      SoFIFA отложен до #650 (reader init падает на `read_versions`).
- [x] Предусловие — прокси-авторизация FS (#647, split creds) починена; без неё
      замер был невозможен (silent 0/10).
- [ ] Решение по снижению — отдельный тикет (proxy-level фильтр mitmproxy upstream;
      ротация сессии — второстепенно, см. вывод §4).
- Регрессия данных: **не затрагивается** — добавлены только пассивные счётчики,
  путь скрапа/парсинга не менялся; сторож — DQ-проверки
  (`tests/unit/dq/test_e3_dq.py`, `tests/integration/test_e2_dims_smoke.py`).

## 6. Cross-refs

- #616 (этот аудит), #647 (фикс прокси-авторизации FS — precondition замера),
  #650 (SoFIFA `read_versions` DOM-drift — блокирует SoFIFA baseline),
  #624 (FBref cold-start), #131 (telemetry gap — закрыт),
  #44 / #117 (FBref traffic instrumentation).
- `docs/research/fbref-proxy-traffic-audit.md` — сиблинг по FBref.
