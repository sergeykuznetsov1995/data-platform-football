# T0 — Рекон source-ID: Euro 2028 / AFCON 2027 / Copa América 2028 (#920 Phase 3)

**Статус:** done
**Дата:** 2026-07-10
**Разблокирует:** #920 Phase 3 — онбординг трёх турниров в `competitions.yaml` + ID-мапы
источников + `configs/soccerdata/league_dict.json` (коммиты «onboard …» и «league_dict»).
**Метод:** как в WC0 — FBref и SofaScore через camoufox + residential proxy (`proxys.txt`)
внутри контейнера `airflow-scheduler` на прод-VM (прямой curl к `api.sofascore.com` — 403
даже через residential; рабочий канал — same-origin fetch `www.sofascore.com/api/v1` из
страницы); WhoScored — одна страница через FlareSolverr + разбор `allRegions` (ровно так
soccerdata `read_leagues` строит строку `region + " - " + league`); FotMob и ESPN — прямой
curl (API открытые: fotmob достаточно UA/Referer/Origin как в скрапере, x-mas не нужен);
даты — официальные анонсы UEFA/CAF/Wikipedia. Ничего не писалось в Bronze/Iceberg, Airflow
не трогался.

## Итог по источникам (все 15 ячеек = live-confirmed, кроме дат Копы — TBA)

### Euro (следующий розыгрыш: 2028, финалы 2028-06-09..2028-07-09, ENG/SCO/WAL/IRL, 24 команды, 51 матч)

| Источник | ID | Статус | Проверено |
|---|---|---|---|
| fbref | comp_id=676, slug `European-Championship` | confirmed | `/en/comps/676/2024/schedule/2024-European-Championship-Scores-and-Fixtures` → 200 без редиректа, 310 KB, h1 «2024 UEFA Euro 2024 Scores & Fixtures», таблицы `sched_2024_676_1/2` |
| sofascore | ut=1, slug `football/europe/european-championship` | confirmed | `/api/v1/unique-tournament/1` → name «EURO», category europe; `/seasons`: EURO 2024 id=56953, «Euro Cup 2020» year=**2021** id=26542, …1960; nav-URL SPA-редирект не-404 |
| fotmob | id=50 | confirmed | `/api/data/leagues?id=50` → 200, `details {"id":50,"name":"EURO","country":"INT"}`, сезоны `['2024','2020','2016',…]` (single-year строки), групповые таблицы `table.data.tables` (6 групп + best-3rd) |
| espn | slug `uefa.euro`, league id=781 | confirmed | scoreboard `dates=20240614` → 200, name «UEFA European Championship»; `/teams` → 24 команды; **calendar = stage-дикты** |
| whoscored | `International - European Championship` (t_id=124) | confirmed | FlareSolverr → `allRegions`: region 247 International, `/regions/247/tournaments/124/international-european-championship`; есть и в builtin LEAGUE_DICT 1.8.8 |

### AFCON (следующий розыгрыш: 2027, финалы 2027-06-19..2027-07-17, KEN/UGA/TAN, 24 команды; официальный анонс CAF)

| Источник | ID | Статус | Проверено |
|---|---|---|---|
| fbref | comp_id=656, slug `Africa-Cup-of-Nations` | confirmed | `/en/comps/656/2023/schedule/…` → 398 KB, редирект показывает `/2024/`, но таблицы `sched_2023_656_1/2` (см. Находку 2); history: сезоны 2010–2023 |
| sofascore | ut=270, slug `football/africa/africa-cup-of-nations` | confirmed | name «Africa Cup of Nations»; `/seasons`: 2025 id=71636, 2023 id=56021, 2021 id=38181, … |
| fotmob | id=289 | confirmed | `details {"id":289,"name":"Africa Cup of Nations"}`, сезоны `['2025','2023',…]`, selectedSeason=2025. **id=290 — это Asian Cup** (гипотеза 290 отброшена) |
| espn | slug `caf.nations`, league id=3908 | confirmed | scoreboard `dates=20240113` → «Guinea-Bissau at Ivory Coast»; `dates=20251226` → 4 события розыгрыша-2025; `/teams` → 24; **calendar = stage-дикты**, кросс-годовое окно 2025-12..2026-12 покрыто одним entry |
| whoscored | `International - Africa Cup of Nations` (t_id=104) | confirmed | `allRegions` → `/regions/247/tournaments/104/international-africa-cup-of-nations`; в builtin LEAGUE_DICT ОТСУТСТВУЕТ → нужна custom-запись |

### Copa América (следующий розыгрыш: 2028; хозяин/даты/формат НЕ объявлены — Эквадор по ротации vs США vs ARG/PAR/URU; 16 команд/32 матча = экстраполяция от 2024, официально TBA)

| Источник | ID | Статус | Проверено |
|---|---|---|---|
| fbref | comp_id=685, slug `Copa-America` | confirmed | `/en/comps/685/2024/schedule/…` → 238 KB, редирект на безгодовую каноническую форму (= текущий розыгрыш), h1 «2024 Copa América Scores & Fixtures», таблицы `sched_2024_685_1/2` |
| sofascore | ut=133, slug `football/south-america/copa-america` | confirmed | name «Copa América»; `/seasons`: 2024 id=57114, 2021 id=26681, 2019, 2016, 2015, 2011, 2007 |
| fotmob | id=44 | confirmed | `details {"id":44,"name":"Copa America","country":"INT"}`, сезоны `['2024','2021','2019',…]` |
| espn | slug `conmebol.america`, league id=780 | confirmed | scoreboard `dates=20240620` → 200, name «Copa América»; `/teams` → 16 команд; **calendar = stage-дикты** (без Rd of 16, с 3rd-Place Match) |
| whoscored | `International - Copa America` (t_id=94, латиница без «é») | confirmed | `allRegions` → `/regions/247/tournaments/94/international-copa-america` |

## Находки

1. **ESPN-календари всех трёх турниров — stage-дикты** (`calendar[0].entries = [{label: 'Group'|'Rd of 16'|…, startDate, endDate}]`), та же форма, что у `fifa.world` из WC0 → guard `_seed_single_year_cup_calendar` в espn-скрапере обязан обобщиться с литерала `INT-World Cup` на все single_year-турниры, иначе soccerdata падает `strptime() argument 1 must be str, not dict`.
2. **AFCON: path-год fbref = официальный год розыгрыша, display-год = год проведения.** `/en/comps/656/2023/…` отдаёт нужный контент (таблицы `sched_2023_656_*`), но h1/redirect показывают «2024» (турнир игрался янв-фев 2024). Сезонный ключ = path-год (2023). Для нашего онбординга AFCON-2027 играется в июне-июле 2027 — расхождения не будет, но при бэкфиле 2023 это критично. Аналогично AFCON-2025 (дек-2025..янв-2026): на fbref history он вообще ещё НЕ заведён (покрытие 2010–2023).
3. **SofaScore: маппинг сезона — только через season_id, не через «официальный год».** У «Euro Cup 2020» `year="2021"`. Рантайм-резолв season_id в скрапере (через `/seasons`) это уже учитывает; сезоны будущих розыгрышей (Euro 2028, AFCON 2027, Copa 2028) в API ещё не заведены — появятся ближе к турниру, резолв подхватит.
4. **soccerdata builtin LEAGUE_DICT (1.8.8 и 1.9.0): AFCON и Copa América отсутствуют полностью; у `INT-World Cup` и `INT-European Championship` нет ESPN-ключа.** ESPN-ЧМ в проде работал только благодаря ручному неверсионированному `/home/airflow/soccerdata/config/league_dict.json` в volume `soccerdata_cache` (дамп снят: 6 записей — 5 клубных лиг с полным набором ключей, вкл. ESPN и WhoScored `Spain - LaLiga`, + `INT-World Cup` = builtin + `"ESPN": "fifa.world"`). Отличие 1.9.0 от 1.8.8: у INT-записей выпали FotMob-ключи (нам не важно — fotmob идёт через собственный LEAGUE_IDS).
5. **FotMob подтверждён без токенов** (достаточно UA/Referer/Origin, как в скрапере; x-mas не нужен). Формат сезона single-year — строка `'2024'`/`'2025'`; групповые таблицы у всех трёх (`table.data.tables`: Euro/AFCON — 6 групп + «Best 3rd placed teams», Copa — 4 группы) → ветки group-таблиц в fotmob-скрапере, захардкоженные на `INT-World Cup`, обязаны обобщиться.
6. **Copa América 2028: НЕ выдумывать даты.** Хозяин не объявлен (Эквадор по ротации / США по слухам / тройная заявка), формат TBA. Сезон в `competitions.yaml` заводится без `start`/`end` → `get_active_single_year_season` его пропускает → перманентный no-op до анонса CONMEBOL (фиктивное окно могло бы открыть live-scrape — класс инцидента 2026-07-09).

## Что меняется в плане Phase 3

1. Канонические id: `INT-European Championship` (= builtin-ключ soccerdata), `INT-Africa Cup of Nations`, `INT-Copa America` (ASCII — partition key/task_id; акцент только в display-имени).
2. `configs/soccerdata/league_dict.json` (коммитится): полные записи для 4 турниров; WC-запись = побайтово прод-патч (builtin + `fifa.world`); механизм установки — merge, сохраняющий чужие ключи (5 клубных записей прод-патча должны пережить установку).
3. ESPN calendar-seed, fotmob group-ветки, fbref/fotmob `format_season` — обобщение с литерала `INT-World Cup` на config-driven предикаты (см. Находки 1, 5).
4. match_count для DQ-floors (#920 Phase 2): Euro 51, AFCON 52 (формат 24, как 2023/2025), Copa 32 (провизорно, формат TBA — сезон инертен).

## Артефакты пробников (не коммитятся)

Локально в скретчпаде сессии: `probe_{ws,sofa,fbref,fbref_hist}_920.py`, `fotmob_*.json`,
`espn_*.json`; на VM/в контейнере: `/tmp/sofa_920_out.log`, `/tmp/fbref_920_out.log`,
`/tmp/ws_all_regions_920.json`, `/tmp/sofa_920_api.json`.
