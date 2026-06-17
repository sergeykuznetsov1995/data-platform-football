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
| Bench | `scripts/research/bench_flaresolverr_fetch.py` | фикс. последовательный прогон, без Iceberg-записи |
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

```bash
# 1) снять счётчики ДО (сумма RX+TX по всем интерфейсам контейнера)
docker exec flaresolverr cat /proc/net/dev

# 2) прогнать bench внутри airflow-контейнера
docker exec -e BENCH_LABEL=baseline airflow-webserver bash -c \
  'cd /opt/airflow && python scripts/research/bench_flaresolverr_fetch.py --source whoscored'
docker exec -e BENCH_LABEL=baseline airflow-webserver bash -c \
  'cd /opt/airflow && python scripts/research/bench_flaresolverr_fetch.py --source sofifa'

# 3) снять счётчики ПОСЛЕ → ΔRX+ΔTX = весь трафик контейнера за прогон
docker exec flaresolverr cat /proc/net/dev
```

`true_proxy_bytes ≈ Δcontainer − fs_response_bytes` (вычитаем обратный трафик
FlareSolverr → airflow, который и есть `fs_response_bytes`). Делим на число
матчей/игроков → **per-match / per-player proxy-МБ**.

Точный (но тяжёлый) вариант — counting forward-proxy (mitmproxy `--mode upstream`)
между FlareSolverr и residential-прокси: считает байты CONNECT-туннеля = ровно
то, что биллится, и даёт per-match атрибуцию. Здесь НЕ строим; это фундамент под
будущую блокировку (фильтрующий прокси — единственный способ «резать» для FS).

## 4. Результаты (заполнить после прогона на VM)

> Прогнать `bench_flaresolverr_fetch.py` + дельту контейнера, вписать числа.

| источник | n | requests | sessions_created | cf_fail | fs_response МБ (floor) | Δcontainer МБ | **proxy МБ / шт** |
|---|---:|---:|---:|---:|---:|---:|---:|
| WhoScored events | 10 | TBD | TBD | TBD | TBD | TBD | **TBD** |
| WhoScored schedule | — | TBD | TBD | TBD | TBD | TBD | **TBD** |
| SoFIFA player_ratings | 10 | TBD | TBD | TBD | TBD | TBD | **TBD** |

Сравнить per-match с FBref (~0.3 МБ warm, §9.4 в `fbref-proxy-traffic-audit.md`).
Ключевой вопрос: доминирует ли `sessions_created × CF-cold-start`? Если да —
рычаг — реже пересоздавать сессию (тюнить `SESSION_RECREATE_EVERY`), а не
блокировать ресурсы.

## 5. Acceptance (#616, часть WhoScored/SoFIFA)

- [x] Инструментирование FlareSolverr (учёта не было) + тесты.
- [x] Bench-харнесс + VM-runbook для per-match proxy-МБ.
- [ ] **Baseline-числа** (заполнить §4 после прогона на VM).
- [ ] Решение по снижению — отдельный тикет, по реальным числам (блокировка для
      FS = только proxy-level фильтр; либо реже ротация сессии).
- Регрессия данных: **не затрагивается** — добавлены только пассивные счётчики,
  путь скрапа/парсинга не менялся; сторож — DQ-проверки
  (`tests/unit/dq/test_e3_dq.py`, `tests/integration/test_e2_dims_smoke.py`).

## 6. Cross-refs

- #616 (этот аудит), #624 (FBref cold-start), #131 (telemetry gap — закрыт),
  #44 / #117 (FBref traffic instrumentation).
- `docs/research/fbref-proxy-traffic-audit.md` — сиблинг по FBref.
