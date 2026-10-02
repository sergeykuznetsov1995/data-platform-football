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
| `core_event_eng1_2026_401879276.json` | recon 15 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/401879276?lang=en&region=us |
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

## Summary: случаи C6-F1 для мягкого режима (#1502)

Сняты 24.09.2026 16:18 UTC напрямую с VM, без прокси (пробы ревью 24.09
`c6/probes/s_12…s_15_*.json`, байт-в-байт). Используются `test_espn_probes.py`: вместе с
шестью summary выше — таблица 9 тел × disposition (`captured` / `valid_empty` /
`source_malformed` / `lineup_anomaly`).

| Файл | Проба | Байт | URL | Суть случая |
|---|---|---|---|---|
| `summary_uru1_2026_ten_starters.json` | c6 s_12 | 156193 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/uru.1/summary?event=401905201 | 10 + 11 стартовых, `formation`/`formationPlace` нет, статистики команд нет → `lineup_anomaly` (`starters_not_11`), matchsheet `valid_empty` |
| `summary_jpn1_2026_ten_starters.json` | c6 s_13 | 336314 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/jpn.1/summary?event=401877180 | у FC Tokyo 10 стартовых → `lineup_anomaly` (`starters_not_11`) |
| `summary_arg2_2026_contradictory_flag.json` | c6 s_14 | 258868 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/arg.2/summary?event=401844030 | игрок 408183 одновременно `starter` и `subbedIn` → `lineup_anomaly` (`contradictory_flags`) |
| `summary_gua1_2026_no_roster.json` | c6 s_15 | 55205 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/gua.1/summary?event=401879625 | ключа `roster` нет у обеих команд, статистики нет → контроль «честно пусто» (`valid_empty`) |

- Записанных summary с серией пенальти (`shootoutScore`), суммой двух матчей
  (`aggregateScore`), `advance` и `leg` нет ни одного: эти поля берутся по ключам C6-F6 и
  проверяются в `test_espn_parsers.py` на синтетике той же формы competitor, что в записанных
  телах.
- Флагов `redCard/yellowCard/penaltyKick/ownGoal` и счёта `homeScore/awayScore` в
  `keyEvents`/`commentary` девяти тел нет — они есть только в core plays (задача 18 карты);
  колонки `MatchEventRow` читаются по этим ключам и на записанных телах пустые, тип события —
  `type_id/type_text`. У записей `commentary` команда и участники — только имена без ID:
  `team_id`/`athlete_ids` пустые, имена — в `extra_json`.


## История: списки core прошлых сезонов (#1509)

Сняты 28.09.2026 10:40–10:41 UTC (13:40–13:41 МСК) напрямую с VM, без прокси, подпись
парсера `data-platform-football/espn-native-v2`, пауза ≥ 3 с между запросами: 17 запросов,
все 200, 711 331 байт (списки — 59 629 байт, два summary — 651 702). В репозиторий положены
16 тел: второе summary (`summary?event=422664`, 333 230 байт) тестам не нужно и не добавлено.
Используются `test_espn_history.py`. Адреса — ровно те, что строит раннер истории
(`limit=100`, страницы с `page=N`).

| Файл | Байт | URL |
|---|---|---|
| `seasons_eng1_limit100.json` | 2795 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons?limit=100&lang=en&region=us |
| `season_eng1_2015.json` | 2143 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons/2015?lang=en&region=us |
| `types_eng1_2015.json` | 176 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons/2015/types?lang=en&region=us |
| `type_events_eng1_2015_t1_p1.json` | 10666 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons/2015/types/1/events?limit=100&lang=en&region=us |
| `type_events_eng1_2015_t1_p2.json` | 10666 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons/2015/types/1/events?limit=100&page=2&lang=en&region=us |
| `type_events_eng1_2015_t1_p3.json` | 10666 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons/2015/types/1/events?limit=100&page=3&lang=en&region=us |
| `type_events_eng1_2015_t1_p4.json` | 8546 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons/2015/types/1/events?limit=100&page=4&lang=en&region=us |
| `type_events_uefa.champions_2010_t1.json` | 524 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/1/events?limit=100&lang=en&region=us |
| `type_events_uefa.champions_2010_t2.json` | 3975 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/2/events?limit=100&lang=en&region=us |
| `type_events_uefa.champions_2010_t3.json` | 3515 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/3/events?limit=100&lang=en&region=us |
| `type_events_uefa.champions_2010_t4.json` | 2365 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/4/events?limit=100&lang=en&region=us |
| `type_events_uefa.champions_2010_t6.json` | 1905 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/6/events?limit=100&lang=en&region=us |
| `type_events_uefa.champions_2010_t7.json` | 984 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/7/events?limit=100&lang=en&region=us |
| `type_events_uefa.champions_2010_t8.json` | 524 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/8/events?limit=100&lang=en&region=us |
| `type_events_uefa.champions_2010_t9.json` | 179 | https://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions/seasons/2010/types/9/events?limit=100&lang=en&region=us |
| `summary_eng1_2015_422285.json` | 318472 | https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary?event=422285 |

- eng.1 2015 — один type (`types_eng1_2015.json`: `count=1`), 380 матчей на четырёх страницах
  `limit=100` (`pageCount=4`, на четвёртой 80): единственная живая запись страницы с
  `pageIndex > 1`. Сезон закрыт (`endDate` 2016-06-01), раннер читает его из raw store.
- UCL 2010 — восемь types рядом с записанной группой (`core_events_ucl_2010_type5.json`, снята
  с `limit=1000`; тест отдаёт её по адресу `limit=100`: 96 матчей, одна страница при любом
  лимите). Всего 213 матчей, пересечений между types нет — повтор внутри сезона и двойник
  между турнирами в тестах синтетические (тот же `$ref` во втором списке).
- `summary_eng1_2015_422285.json` — первый матч первой страницы; сезоны из сотен матчей в
  тестах берут это тело под каждым id списка (в `header` переписаны `id`).
- Сезоны eng.1 2005 и ЧМ 2010 (глубина: без формаций; пустые составы при полной статистике)
  в тестах — синтетические списки одного матча вокруг записанных summary
  `summary_eng1_2005.json` и `summary_fifaworld_2010.json`.
