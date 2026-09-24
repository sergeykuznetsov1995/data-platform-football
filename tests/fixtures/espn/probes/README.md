# ESPN: записанные ответы из проб ревью 24.09.2026

Распакованные JSON-тела ответов ESPN, байт-в-байт из проб ревью
(`/root/espn-review-20260924/`, `c3/probes/` и `recon/espn-probes/`). Сняты
24.09.2026 напрямую с VM, без прокси. Используются `tests/unit/scrapers/test_espn_probes.py`
(#1498). Живых запросов тесты не делают.

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

Тела со страницей > 1 (`pageCount > 1`) среди проб нет — появится в #1501.
