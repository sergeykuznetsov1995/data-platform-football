# Scraping-stack audit — оценка OSS-библиотек (кейс SofaScore)

**Status**: research-only, no production code changed
**Date**: 2026-07-16
**Closes investigation**: [#969](https://github.com/sergeykuznetsov1995/data-platform-football/issues/969)
**Method**: baseline из verified-артефакта #951 (0 платных байт) + живой PoC Pydoll через
резидентный прокси (Tier A datacenter — 0 байт; Tier B residential — ~5.5 MiB платных) +
zero-network микробенч Selectolax на 10 сохранённых FBref-страницах + desk-review 7 библиотек
(лицензии/зависимости проверены по живым репозиториям 2026-07-16). PoC-код — вне
runtime-fingerprint allowlist, verified-бюджет SofaScore не тронут.

---

## TL;DR

1. 🟡 **Pydoll на SofaScore — неожиданно сильный, но REJECT для замены.** На резидентном IP
   Chrome/Pydoll **снял Turnstile** и — главное — его **in-page `fetch('/api/v1/...')` вернул
   200** для всех 5 production-эндпоинтов, тогда как наш camoufox/Firefox упирается в 403-на-fetch
   и вынужден делать по одной навигации на каждый JSON (126 навигаций на 25 матчей). Но выигрыша
   нет: fetch-подход требует загрузки тяжёлого SPA (~2.34 MiB даже с блокировкой картинок) →
   **дороже по платным байтам** (≈2.95 vs 1.2 MiB на 25 матчей), а скорость fetch не помогает,
   потому что throughput ограничен **rate-limit'ом эджа 20 req/min**, а не браузером.
2. 🟢 **Selectolax — реальный 8.1× на парсинге FBref, но узкая ниша.** На реальных сохранённых
   страницах selectolax разобрал таблицы **в 8.1× быстрее** `BeautifulSoup(html.parser)` при
   **полной структурной эквивалентности** (70 таблиц / 949 строк / 9177 ячеек — идентично). Но
   парсинг у нас **не на критическом пути** (всё прокси-bound); выигрыш материален только в
   офлайн-реплее ~18k страниц (`dags/dag_replay_fbref.py`). Самый дешёвый шаг вообще — сменить
   backend `bs4` с `html.parser` на `lxml` (**1.32× бесплатно, одна строка**), без переписывания на CSS.
3. 🔴 **Остальные 7 библиотек — все REJECT.** katana, Trafilatura, Firecrawl, Crawl4AI,
   ScrapeGraphAI, Scrapy, Crawlee — ни одна не берёт планку «ценность > цена» (детали в §4).
4. ⚠️ **Доминирующая цена любой правки SofaScore-пути — пересбор verified-бюджета.** Новая
   зависимость в `requirements-scraping.txt` или правка любого из 37 allowlist-файлов
   (`runtime_fingerprint.py`) обнуляет verified-артефакт → **~50–60 MiB платных canary-замеров +
   verify + rebuild образов** (факт из #951: два пересбора 49,5 + 59 MiB). Это, а не «поменял
   импорт», — реальная стоимость перехода на любой новый движок SofaScore.

---

## 1. Question

Могут ли перечисленные в #969 OSS-библиотеки (Pydoll, katana, Selectolax, Trafilatura,
Firecrawl, Crawl4AI, ScrapeGraphAI, Scrapy, Crawlee) улучшить наш скрапинг-стек — быстрее,
дешевле, надёжнее или проще, — и какова честная цена перехода, включая эффект на verified
proxy-бюджет SofaScore? Первый предметный кейс — SofaScore (платный резидентный прокси + Turnstile).

## 2. Method

- **Baseline SofaScore** взят из verified-артефакта #951
  (`candidate-8a82c9dde71fb3a2-VERIFIED.json`) и `docs/research/sofascore-proxy-budget.md` —
  **без** новых платных замеров.
- **PoC Pydoll**: `scripts/research/poc_pydoll_sofascore.py` (throwaway-venv
  `/root/.venvs/dpf-pydoll`, pydoll-python 2.23.1, Chrome 150). Tier A — datacenter egress
  (0 байт); Tier B — резидентный прокси из пула (exit 89.182.201.125, Германия, ISP htp GmbH,
  `hosting=false` — настоящий резидентный IP), замороженная EPL-когорта
  (`configs/sofascore/proxy_canary_cohort.json`), 2 прогона ≈ **5,5 MiB** платных.
- **Selectolax**: `scripts/research/bench_fbref_replay_selectolax.py` — zero-network, 10
  сохранённых FBref-страниц (`tests/fixtures/fbref/matches/*.html.gz`, ~230K символов каждая),
  15 итераций, проверка структурной эквивалентности.
- **Guardrail**: runtime fingerprint SofaScore сверялся до/после
  (`3ed8f36…` неизменен); оба PoC-скрипта — вне allowlist (каталог `scripts/research/` не
  хешируется, кроме одного `bench_sofascore_paid_canary.py`); зависимости — только в изолированном
  venv, не в `requirements-scraping.txt`.

## 3. Findings

### 3.1 SofaScore baseline (camoufox 0.4.11 + playwright 1.59.0)

| Метрика (класс `match_batch_25`) | Значение |
|---|---|
| Транспорт | JSON-API `/api/v1` (не HTML) |
| Провайдер-байты (hard_task_bytes) | 1 320 332 B (1,2 MiB) на 25 матчей |
| Браузерные сессии / навигации | 1 сессия / 126 навигаций |
| Латентность | p50 2,99 с / p95 3,11 с на endpoint |
| Throughput | 0,073 matches/s, rate-limit **20 req / 60 s** (эдж) |
| Per-endpoint байты | lineups 515K, event 292K, incidents 208K, shotmap 201K, statistics 104K |
| Архитектурное ограничение | edge отдаёт JSON как **документ** (200 на навигацию), но **403 на fetch/XHR** в той же сессии (`camoufox_capture.py:494-503`) → 1 навигация на каждый JSON |

Прочие классы: player_batch_50 = 334 058 B; season EPL = 462 355 B; season WC = 332 253 B.
Хрупкость пинов: playwright обязан быть `<1.60` (camoufox#617), soccerdata держится на `1.8.8`.

### 3.2 PoC Pydoll (Chrome/CDP) — главный вопрос: fetch-vs-navigation

**Tier A (datacenter egress, 0 байт).** Pydoll стартует за ~1 с (RSS ~1 ГБ). Все три метода к
`/api/v1/event/{id}` — navigation, in-page fetch, curl_cffi `chrome131` — вернули **идентичный 403**
`{"error":{"code":403,"reason":"Forbidden"}}`. Даже клиент с идеальным Chrome-TLS отклонён →
блок на уровне **IP-репутации**, не браузера. Crux без резидентного IP неразрешим.

**Tier B (резидентный прокси, ~5,5 MiB платных).** Результат неожиданный:

| Проба | camoufox/Firefox (baseline) | Pydoll/Chrome (PoC) |
|---|---|---|
| Turnstile на резидентном IP | снимается (заточен, #757) | **снят** (auto-solve, is_challenge=false, SPA загрузилась); выборка мала |
| navigation `/api/v1` | 200 (документ) | 200 (документ) |
| **in-page `fetch('/api/v1')`** | **403** (вынуждает навигацию) | **200 + валидный JSON, все 5 эндпоинтов** |
| fetch-латентность | — | event 0,008 с / incidents 0,299 / lineups 0,111 / shotmap 0,005 / statistics 0,061 |

То есть барьер «403-на-fetch», из-за которого camoufox делает 126 навигаций, на Chrome-сессии
**не воспроизводится**. Но это **не бесплатная победа** и, вероятно, **не про сам браузер**:

- **Честный caveat.** Мой PoC сначала полностью загрузил SPA (получил `cf_clearance` cookie +
  сессию), затем делал fetch с `credentials:'include'`. Camoufox сознательно **не** грузит SPA
  (навигирует прямо на JSON ради экономии байт, #842) — поэтому его fetch без clearance-cookie
  → 403. Fetch «чинится» загрузкой SPA, а **не** сменой Firefox→Chrome.
- **Байты (encodedDataLength по проводу, приближение).** Landing SPA = **2,34 MiB** даже с
  блокировкой картинок (тяжёлые JS-бандлы); data-фаза дёшева — ~46 KB gzip на 9 запросов
  (~5 KB/endpoint). Амортизированная оценка Pydoll+fetch на 25 матчей ≈ 2,34 + 0,6 ≈ **2,95 MiB
  против 1,2 MiB** у camoufox — **в ~2,5× дороже** по главному ресурсу (платный трафик).
- **Скорость fetch не конвертируется в throughput.** Rate-limit 20 req/min — ограничение
  **эджа**, не браузера. 125 запросов / 20-в-минуту ≈ 6 мин минимум независимо от скорости
  fetch. Навигация camoufox (~3 с) уже сбалансирована с интервалом rate-limit (3 с/запрос) —
  навигация **не** узкое место.

Плюсы Pydoll (не решающие): снимает pin-хрупкость camoufox (чистый CDP, без firefox-бинаря и
пары camoufox↔playwright); fetch-стратегия — запасной путь на случай, если rate-limit перестанет
доминировать.

### 3.3 Selectolax vs BeautifulSoup (FBref, zero-network)

| Движок | с/страницу | Ускорение | Структурная эквивалентность |
|---|---|---|---|
| `bs4` + html.parser (текущий) | 0,0822 | 1,00× | 70 таблиц / 949 строк / 9177 ячеек |
| `bs4` + lxml | 0,0622 | **1,32×** | ✅ идентично |
| **selectolax (lexbor)** | 0,0101 | **8,12×** | ✅ идентично (checksum совпал) |

8,1× — реальное и безопасное ускорение (результат идентичен). Но: (а) парсинг FBref **не** на
критическом пути обычного ingest — он прокси-bound (fetch занимает минуты, парс — 0,08 с);
выигрыш материален только в офлайн-реплее ~18k сохранённых страниц (`dags/dag_replay_fbref.py`,
~24 мин парса → ~3 мин); (б) selectolax работает на CSS-селекторах, а продакшн-парсер FBref — на
`bs4` find_all + Comment-хаках + `pd.read_html` → полное переписывание нетривиально и рисково;
мой бенч мерил репрезентативный обход, а не весь продакшн-парсер. FBref-путь **не** в
SofaScore-fingerprint, поэтому его замена SofaScore-бюджет не трогает.

## 4. Decision — триаж 9 библиотек

| Библиотека | Класс | Лицензия | Вердикт | Killer-аргумент / цена перехода |
|---|---|---|---|---|
| **Pydoll** | браузер/CDP | MIT | **reject** (замену), находка ценна | Дороже по байтам (SPA), throughput rate-limit-bound; замена движка = пересбор бюджета ~50–60 MiB |
| **Selectolax** | HTML-парсер | MIT | **cherry-pick** (узко) | 8,1× только в офлайн-реплее FBref; переписывание bs4→CSS нетривиально; вне SofaScore-бюджета |
| **katana** | endpoint-discovery (Go) | MIT | **reject** | Находит URL, не данные; discovery уже дёшев (tls-client, 0×403); Go-HTTP хуже против CF |
| **Trafilatura** | извлечение прозы (NLP) | Apache-2.0 (≥1.8.0) | **reject** | Обратная задача — срезает «нетекст», а наши данные это и есть таблицы; нет источников-статей |
| **Firecrawl** | crawl→Markdown/LLM | **AGPL-3.0** | **reject** | AGPL жёстче GPLv3 (прецедент ScraperFC); anti-bot только в SaaS, self-host = голый Playwright |
| **Crawl4AI** | crawl→AI Markdown | Apache-2.0 + attribution | **reject** | Обязательный Chromium на HTTP-дешёвых источниках (анти-паттерн ScraperFC); Turnstile не обходит |
| **ScrapeGraphAI** | LLM-граф парсинг | MIT | **reject** | Платит LLM-токенами за то, что bs4 делает детерминированно; ломает воспроизводимость bronze/DQ |
| **Scrapy** | фреймворк-оркестратор | BSD-3 | **reject** | Дублирует Airflow+capture-engine; Turnstile/TLS-fp/байт-бюджет не решает; миграция = переписать всё |
| **Crawlee** | фреймворк (Node+Py) | Apache-2.0 | **reject** (cherry-pick идею session-pool) | Py-порт незрел, fingerprint-suite только в Node; lease-бюджет пришлось бы встраивать в чужой фреймворк |

Причина системная: стек уже специализирован под три вещи, которые ни один универсальный
инструмент не покрывает — (1) Turnstile-обход держится на эмпирической связке
camoufox+Firefox+резидентный прокси; (2) lease-based байт-бюджет `proxy_filter` уникален;
(3) детерминированный парсинг стабильных схем (JSON-API + известные таблицы) делает
LLM-extraction экономически и DQ-бессмысленным. Плюс любая Python-зависимость в
`requirements-scraping.txt` стоит ~50–60 MiB платного пересбора бюджета SofaScore.

## 5. Good-enough-to-unblock

Задача #969 закрыта: есть честный триаж (что применимо, что нет, почему), по Pydoll есть
**живые цифры PoC** (Turnstile снят, fetch=200, байты/латентность/throughput-анализ), по
Selectolax — измеренные 8,1× с проверкой эквивалентности, у каждой рекомендации указана цена
перехода включая эффект на verified-бюджет. Verified-артефакт SofaScore не тронут (fingerprint
неизменен). Реализация **не** начата — это research.

## 6. Open questions

1. **Firefox-vs-Chrome или strategy?** Расхождение fetch=200 (Chrome) vs 403 (camoufox) — это
   про браузер или про «загруженный SPA с clearance-cookie vs прямая навигация»? Проверяемо
   только эквивалентным прогоном camoufox с полной загрузкой SPA (не делали — camoufox нет на хосте).
2. **Можно ли урезать landing SPA** ниже 2,34 MiB (агрессивная блокировка JS)? Если да —
   fetch-подход мог бы приблизиться к baseline по байтам. Не измеряли предел.
3. **Надёжность Turnstile у Pydoll на объёме** — выборка 2/2 прогона мала; camoufox спец-заточен (#757).
4. **Точность метра.** encodedDataLength (CDP) ≠ провайдер-байты `proxy_filter_provider_path_v2`
   один-в-один; для решения о замене нужен замер тем же метром.

## 7. Inputs to roadmap

- **SofaScore движок: не менять.** Pydoll не улучшает доминирующие метрики (байты, rate-limit);
  замена = пересбор бюджета ~50–60 MiB при отрицательном ожидаемом эффекте.
- **Дешёвый FBref-выигрыш (P3, опционально):** сменить backend `bs4` `html.parser`→`lxml`
  (1,32×, одна строка, lxml уже в зависимостях `>=5.0`). Selectolax (8,1×) — только если
  офлайн-реплей FBref станет частой/узкой операцией; отдельным тикетом, с переписыванием
  парсера на CSS и полным набором regression-тестов.
- **Прочие 7 библиотек — закрыть как не подходящие;** держать в уме katana как ad-hoc dev-рекон
  новых источников (вне репозитория) и session-pool из Crawlee как идею для capture-engine.
- **Артефакты PoC** (переиспользуемы): `scripts/research/poc_pydoll_sofascore.py`,
  `scripts/research/bench_fbref_replay_selectolax.py`.
