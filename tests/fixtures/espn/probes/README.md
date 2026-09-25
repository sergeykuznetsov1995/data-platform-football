# ESPN: записанные ответы из проб ревью 24.09.2026

Распакованные JSON-тела ответов ESPN, байт-в-байт из проб ревью
(`/root/espn-review-20260924/`, `c3/probes/`, `c7/probes/` и `recon/espn-probes/`). Сняты
24.09.2026 напрямую с VM, без прокси. Используются `tests/unit/scrapers/test_espn_probes.py`
(#1498), детали лиг `league_detail_*` — `test_espn_classify.py` и
`test_espn_catalog_core.py` (#1499). Живых запросов тесты не делают.

| Файл | Проба | URL |
|---|---|---|
| `summary_eng1_2020.json` | c3 p11 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary?event=578281 |
| `summary_ger2_2016.json` | c3 p15 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/ger.2/summary?event=456996 |
| `summary_eng1_2010.json` | c3 p10 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary?event=292828 |
| `summary_ucl_2010.json` | c3 p12 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/summary?event=307787 |
| `summary_fifaworld_2010.json` | c3 p13 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/fifa.world/summary?event=264031 |
| `summary_eng1_2005.json` | recon 26 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary?event=184188 |
| `scoreboard_eng1_day.json` | recon 08 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?dates=20260920&limit=1000 |
| `core_events_eng1_2025.json` | recon 28 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons/2025/types/1/events?lang=en&region=us&limit=1000 |
| `core_events_ucl_2010_type5.json` | c3 p06 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/5/events?limit=1000&lang=en&region=us |
| `leagues_core.json` | recon 01 | https://sports.core.api.espn.com/v2/sports/soccer/leagues?limit=500&lang=en&region=us |
| `league_detail_afc.champions_qual.json` | c7 p02 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/afc.champions_qual?lang=en&region=us |
| `league_detail_fifa.world.u20.json` | c7 p03 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world.u20?lang=en&region=us |
| `league_detail_concacaf.champions_cup.json` | c7 p04 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/concacaf.champions_cup?lang=en&region=us |
| `league_detail_sui.1.json` | c7 p07 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/sui.1?lang=en&region=us |
| `site_api_403_akamai.{hdr,body}` | c5 p01 | https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary?event=740900 |

`site_api_403_akamai.*` — заголовки и HTML-тело отказа Akamai (HTTP/2 403, `server: AkamaiGHost`,
446 байт) на `site.api.espn.com` с User-Agent парсера `data-platform-football/espn-native-v2`,
снято 24.09.2026 16:19 UTC. Используется `test_espn_transport.py` (#1500): 403 → запрос
отложен, адрес закрыт. Записанного 429 нет — в тестах он синтетический.


## Списки core, сезоны, статусы дня (#1501)

Сняты 24.09.2026 напрямую с VM, без прокси (пробы `c2/`, `c3/`, `c4/`, `v2/`, `recon/` ревью
24.09). Используются `test_espn_urls.py`, `test_espn_core_lists.py`, `test_espn_editions.py`,
`test_espn_parsers.py`. Адреса в колонке URL — ровно те, что сверяет `test_espn_urls.py`
(кроме `scoreboard_esp1_20260920.json` — проба снята без `limit`, и
`events_window_eng1_395d_400.json` — без `lang/region`).

| Файл | Проба | Байт | URL |
|---|---|---|---|
| `league_detail_uefa.champions.json` | c2 12 | 9006 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions?lang=en&region=us |
| `seasons_uefa.champions.json` | c2 01 | 3029 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons?limit=100&lang=en&region=us |
| `seasons_eng.fa_page0_limit3.json` | c2 08 | 381 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.fa/seasons?limit=3&lang=en&region=us |
| `season_uefa.champions_2010.json` | c3 p02 | 6345 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010?lang=en&region=us |
| `types_uefa.champions_2026.json` | c2 06 | 795 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2026/types?lang=en&region=us |
| `types_eng.fa_2026_empty.json` | c2 09 | 64 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.fa/seasons/2026/types?lang=en&region=us |
| `type_events_uefa.champions_2026_t1.json` | c2 07 | 17059 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2026/types/1/events?limit=1000&lang=en&region=us |
| `type_events_fifa.world_2010_t1.json` | c3 p07 | 5394 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world/seasons/2010/types/1/events?limit=1000&lang=en&region=us |
| `events_window_eng1_30d.json` | c2 13 | 3336 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events?dates=20260901-20260930&limit=1000&lang=en&region=us |
| `events_window_eng1_395d_400.json` | v2 p07 | 74 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events?dates=20250601-20260701&limit=1000 |
| `events_nodtype_404.json` | c3 p04 | 52 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/events?limit=1000&lang=en&region=us |
| `event_status_first_half.json` | c2 14 | 348 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.nations/events/401861047/competitions/401861047/status?lang=en&region=us |
| `scoreboard_range_400.json` | recon 10 | 55 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?dates=20260801-20260831&limit=1000 |
| `all_scoreboard_20260923.json` | c2 05 | 580017 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?dates=20260923&limit=1000 |
| `scoreboard_eng1_20050813_postponed.json` | recon 23 | 88715 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?dates=20050813&limit=1000 |
| `scoreboard_esp1_20260920.json` | c4 12 | 65789 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/esp.1/scoreboard?dates=20260920 |
| `all_scoreboard_event_timevalid_false.json` | c2 03 (вырезка) | 7119 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?dates=20260924 |

- `seasons_eng.fa_page0_limit3.json` — единственное записанное тело списка из нескольких
  страниц: `limit=3` → `count=25, pageSize=3, pageCount=9, pageIndex=1`. Записана только
  первая страница; страницы 2–9 в тестах синтетические той же формы (живой записи страницы
  с `pageIndex > 1` среди проб нет).
- `types_eng.fa_2026_empty.json` — `count=0, pageIndex=0, pageCount=0`: сезона 2026 у Кубка
  Англии ESPN ещё не открыл; пустой список законен.
- `events_window_eng1_395d_400.json`, `events_nodtype_404.json`, `scoreboard_range_400.json` —
  тела ошибок ESPN (400 «The dates range specified is too large», 404 сезона без type,
  400 web.api на `scoreboard?dates=A-B`).
- `scoreboard_esp1_20260920.json` — распакованный gzip пробы `c4/probes/12_sb_esp1_day.body`.
- `all_scoreboard_event_timevalid_false.json` — из `c2/probes/03_all_scoreboard_today_web.json`
  (604 КБ, 100 событий) вырезано единственное событие с `competitions[0].timeValid=false`
  (id 732409): `{"leagues": <leagues тела>, "events": [<событие 732409>]}`, JSON без
  пробелов, `ensure_ascii=False`.
- Календарная лига для перехода сезона — записанная деталь `league_detail_fifa.world.u20.json`
  (сезон 2025: 2025-01-01T05:00Z…2026-01-01T04:59Z, то есть ET-год 2025); «осень–весна» —
  `league_detail_sui.1.json` (2025-26), в тесте у неё меняется только `season` на 2026.

`../schedule_esp1_20260920_placeholder.csv` — 10 строк esp.1 из замороженного расписания
(Trino, выгрузка ревью `v2/sched_0919_0921.csv`): все матчи тура на «2026-09-20 18:00» —
заглушка тура; реальные времена того дня — в `scoreboard_esp1_20260920.json` (5 матчей, 4 разных
времени).
