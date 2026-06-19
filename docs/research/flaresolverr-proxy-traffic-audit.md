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
| SoFIFA player_ratings | 10 | 33 | 9 | 0 | 56.22 | 439.55 | **~38.3 / игрок** |

`proxy МБ/шт = (Δcontainer − fs_response) / n`. SoFIFA снят после фикса #650
(`read_versions` override под post-EA-FC DOM) — 10/10 success, 0 CF-fail. Артефакт:
`bench_sofifa_baseline.json`. **Числа SoFIFA сильно раздуты bootstrap'ом `read_players`:**
из 33 запросов 23 — разовый bootstrap (1 homepage + 1 `leagues.json` + **21 team-squad**
страница, которыми `read_players` энумерирует весь состав лиги), и лишь 10 —
player-rating. Маржинальная player-страница ≈ **13 МБ** proxy (fs_response ~1.66 МБ ×
~7.8 общий Δ/fs-множитель); bootstrap `read_players` ≈ **310 МБ один раз / (лига, edition)**
и амортизируется на ~500 игроков лиги. Ротация частая (`SESSION_RECREATE_EVERY=4` против
WhoScored=8, чтобы обходить tab-crash Chromium 142) → 9 cold-start на 33 запроса, но
вес страницы всё равно доминирует (33 × ~14 МБ ≈ весь Δ).

> ⚠️ **Operational gotcha (отдельно от замера):** первый прогон упал, переиспользовав
> **отравленный кеш** — `index.html` от эпохи бага #647 (246 КБ Chromium-error-page
> `ERR_NO_SUPPORTED_PROXIES`), который `read_versions(max_age=1)` взял из кеша вместо
> живого фетча. Production SoFIFA DAG так же может застрять на отравленном `index.html`
> до истечения `max_age`. Лечится чисткой `~/soccerdata/data/SoFIFA/` (followup-issue).

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
- [x] **Baseline-числа** (§4): WhoScored events ~10.9 МБ/матч, schedule ~72 МБ/сезон,
      SoFIFA ~38.3 МБ/игрок (n=10, bootstrap `read_players` доминирует) — #650 починен.
- [x] Предусловие — прокси-авторизация FS (#647, split creds) починена; без неё
      замер был невозможен (silent 0/10).
- [ ] Решение по снижению — **#652** (proxy-level фильтр mitmproxy upstream между FS и
      residential; ротация сессии — второстепенно, см. вывод §4). Рычаг общий для
      WhoScored и SoFIFA: оба платят за вес страницы.
- Регрессия данных: **не затрагивается** — добавлены только пассивные счётчики,
  путь скрапа/парсинга не менялся; сторож — DQ-проверки
  (`tests/unit/dq/test_e3_dq.py`, `tests/integration/test_e2_dims_smoke.py`).

## 5b. Prod-проводка фильтра (#652)

Реализация снижения трафика (ветка `feature/issue-652-flaresolverr-proxy-filter`):

- **compose-сервис `proxy_filter`** (`compose.yaml`) — long-running asyncio-прокси
  (`scripts/proxy_filter/filter_proxy.py`) на `backend`-сети, слушает `0.0.0.0:8899`,
  грузит `configs/proxy_filter/blocklist.txt`. Режет ad-tech CONNECT-хосты (403, без
  дозвона upstream → байты не биллятся); MITM нет, TLS-fingerprint Chrome цел.
- **Ротация upstream — внутри фильтра, idle-refresh.** `filter_proxy` держит
  `ProxyManager` и выбирает свежий residential-exit (`_acquire_upstream`) **только
  когда нет открытых туннелей** (`_active == 0`), а не пинит один при старте и не
  меняет на каждое соединение. Так все туннели одной FlareSolverr-сессии идут с
  одного exit-IP (страница и её Turnstile-challenge на одном IP = CF-safe), а
  следующая сессия (она рвёт вкладку → закрывает все туннели через нас) берёт новый
  exit. Это и есть ротация на уровне FS-сессии — попутно ловит реактивную ротацию
  скрапера (на CF-fail он пересоздаёт сессию).
- **Тоггл `PROXY_FILTER_URL`** (opt-in, по умолчанию выкл). Задан → WhoScored
  (events `_pick_proxy_url` **и** schedule-reader) и SoFIFA
  (`FlareSolverrSoFIFAReader._proxy_url`) отдают FlareSolverr статичный
  `http://proxy_filter:8899` (без креды; фильтр держит creds). Не задан → прежнее
  поведение (прямой residential).

Перемер: §3 runbook с `PROXY_FILTER_URL=http://proxy_filter:8899`; биллинговый трафик
читать из `/tmp/filter_bytes.json` фильтра (`total_mb / n`). Observe-замер (PR #656)
дал ~77% (WhoScored) и ~50% (SoFIFA) на like-for-like наборе страниц при целых данных.

| источник | proxy МБ / шт (baseline §4) | proxy МБ / шт (filter) | Δ% |
|---|---|---|---|
| WhoScored events | ~10.9 | **~0.54** (n=3) | **~−95%** |
| SoFIFA player_ratings | ~38.3 | TBD (не замерян live) | — |

> **Live-замер WhoScored (2026-06-18, n=3 матча через прод-фильтр).** Биллинг по
> точному счёту фильтра (`filter_bytes.json` = байты CONNECT-туннеля к residential):
> **1.63 МБ на 3 матча = 0.54 МБ/матч**, 3/3 успешно, **0 CF-фейлов**, idle-refresh =
> 1 exit на сессию, заблокировано 53 ad-tech-попытки (8 хостов). Like-for-like
> observe-прогон (тот же фильтр БЕЗ блок-листа, те же 3 game_id) **завис**: без
> блокировки ad-tech держит соединения открытыми и загрузка не доходит до idle —
> накопил **11.7 МБ уже на 1 матче** (сходится с baseline ~10.9). Вывод: фильтр режет
> ~95% И убирает зависание. Оговорки: выборка мала (n=3); baseline §4 мерян методом
> `/proc/net/dev` (шумит loopback'ом Chromium) — точное число — счёт самого фильтра.
> SoFIFA вживую не мерян (обвязка покрыта юнит-тестами; тот же фильтр-путь).

## 5c. Решение: proxy-less для WS+SoFIFA (2026-06-18)

Прогнали bench (`bench_flaresolverr_fetch.py`, `--proxy-file` на несуществующий
файл = proxy-less; FlareSolverr решает Cloudflare сам):

| источник | proxy-less | CF-фейлы | вывод |
|---|---|---|---|
| **WhoScored** | 30/30 ✅ | **0** | работает уверенно; прокси давал только скорость (~2с vs ~5–16с) |
| **SoFIFA** | 30/30 (через ретраи) ✅ | **5 / 30** ⚠️ | проходит, но CF начинает упираться; с прокси было 0/10 |

**Решение (подтверждено владельцем):** скорость не важна → **работаем без прокси**
для обоих источников. Это уже дефолтное состояние прод-DAG'ов
(`dag_ingest_whoscored.py` шлёт `--proxy-file ""`, `dag_ingest_sofifa.py` — без
proxy-флага; `PROXY_FILTER_URL` пуст), теперь закреплено **явно**: комментарии в
DAG'ах + лог `proxy mode: PROXY-LESS` при старте FlareSolverr-сессии
(`describe_proxy_mode()` в `scrapers/base/flaresolverr_client.py`) — случайное
включение прокси теперь видно в логах.

**Runbook (вернуть прокси как fallback):** если SoFIFA на полном прогоне (~545
страниц) начнёт стабильно ловить CF-фейлы — выставить
`PROXY_FILTER_URL=http://proxy_filter:8899` (включить ad-tech фильтр #652) или
вернуть непустой `--proxy-file`. Логи покажут смену режима.

**Оговорки:** прогоны короткие (30 страниц); полный SoFIFA (545) без прокси не
гоняли — поведение на объёме на 100% не проверено.

## 6. Cross-refs

- #616 (этот аудит), #647 (фикс прокси-авторизации FS — precondition замера),
  #650 (SoFIFA `read_versions` DOM-drift — починен, SoFIFA baseline разблокирован),
  #652 (снижение трафика FS — proxy-level фильтр), #624 (FBref cold-start),
  #131 (telemetry gap — закрыт), #44 / #117 (FBref traffic instrumentation).
- `docs/research/fbref-proxy-traffic-audit.md` — сиблинг по FBref.
