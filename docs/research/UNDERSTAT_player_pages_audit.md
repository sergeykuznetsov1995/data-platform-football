# UNDERSTAT — /player/{id} pages: биографические атрибуты (probe)

**Status**: research-only, no code changes
**Date**: 2026-06-20
**Closes investigation**: [#38](https://github.com/sergeykuznetsov1995/data-platform-football/issues/38)
**Method**: live read-only probe 5 APL player-страниц `understat.com/player/{id}` + AJAX-backend `getPlayerData/{id}`. Транспорт — `tls_requests` (как прод-стек), без браузера/прокси. Никаких изменений в scrapers/DAG/SQL/Iceberg.

---

## TL;DR

1. **Вердикт — NEGATIVE.** Understat НЕ отдаёт ни одного static-биографического атрибута. На `/player/{id}` (и в его AJAX-бэкенде `getPlayerData/{id}`) нет `date_of_birth` / `height` / `weight` / `preferred_foot` / `nationality` — ни в HTML, ни в JSON. BIO-скан по объединённому payload: `birth=0, dob=0, height=0, weight=0, nation=0, country=0`.
2. **Что реально есть:** `name`, `favorite_position` (производная зона удара, напр. `FWR`), и `lastMatch = {league, team, season}` (текущий клуб). Всё остальное — xG/shots/matches-статистика, ради которой Understat и заведён.
3. **Ложные срабатывания** грубого grep'а разобраны (§3): `height`→рекламный виджет, `foot`→`shotType`="RightFoot"/"LeftFoot" (нога УДАРА, не игрока) + "football"/`<footer>`, `nation`→"pagi**nation**", `country`→гео-таргет рекламы adlook.
4. **Gap уже закрыт без Understat** (§5): `dob`/`nationality` покрыты 5 источниками, `height`/`foot` — 4 (FBref/FotMob/SofaScore/Transfermarkt/SoFIFA). Понятие "gap" по static-attrs для Understat пустое.
5. **Next step:** закрыть #38 как `wontfix` (research выполнен, реализации не будет). Follow-up issue НЕ создаём.

---

## 1. Probe setup — игроки и id

player_id разрешены не хардкодом, а из публичного `getLeagueData/EPL/2024` (поле `players[].id` × `player_name`). Затем пробита каждая `/player/{id}` + её AJAX `getPlayerData/{id}`.

| Player | understat id | `/player/{id}` | `getPlayerData/{id}` |
|---|---|---|---|
| Bukayo Saka | 7322 | HTTP 200, 22 295 B | HTTP 200, 323 228 B |
| Mohamed Salah | 1250 | HTTP 200, 22 253 B | HTTP 200 |
| Erling Haaland | 8260 | HTTP 200, 20 639 B | HTTP 200 |
| Rodri | 2496 | HTTP 200, 21 452 B | HTTP 200 |
| Cole Palmer | 8497 | HTTP 200, 21 098 B | HTTP 200 |

Все 5 страниц отдаются без Cloudflare/captcha (см. §4).

## 2. Page anatomy — где живут данные

`/player/{id}` — это **AJAX-shell**: сам HTML встраивает ровно один полезный JSON-блок, остальное подгружает `js/player.min.js` через бэкенд-endpoint.

**Единственный встроенный `var` в HTML** (DOM-вырезка, страница Saka):

```js
var player = JSON.parse('\x7B\x22id\x22\x3A\x227322\x22,\x22name\x22\x3A\x22Bukayo\x20Saka\x22\x7D');
// → {"id": "7322", "name": "Bukayo Saka"}
```

То есть в самом HTML — только `id` + `name`. (Раньше league/player-страницы встраивали `playersData`/`shotsData` прямо в HTML; теперь HTML — оболочка, данные едут отдельным XHR — поэтому soccerdata и ходит в `getLeagueData`/`getMatchData`.)

**Реальный backend — `GET /getPlayerData/{id}`** (header `X-Requested-With: XMLHttpRequest`, 323 KB для Saka). Top-level ключи и их природа:

| key | тип | содержимое | bio? |
|---|---|---|---|
| `player` | dict | `{id, name, favorite_position}` | имя + зона удара, **нет bio** |
| `matches` | list[226] | per-match xG/goals/shots/xA/position/team/date | нет |
| `groups` | dict | агрегаты `season`/`position`/`situation`/`shotZones`/`shotTypes` | нет |
| `positionsList` | list[17] | коды зон поля (`GK`,`DL`,`MC`,`FWR`…) | нет |
| `minMaxPlayerStats` | dict | min/max по зонам (`AMC`,`AML`,`DL`…) | нет |
| `shots` | list[506] | shot-события (X,Y,xG,minute,result,shotType) | нет |
| `lastMatch` | dict | `{league, team, season}` | только текущий клуб |

DOM/JSON-вырезки (Saka):

```json
"player":    {"id": "7322", "name": "Bukayo Saka", "favorite_position": "FWR"}
"lastMatch": {"league": "EPL", "team": "Arsenal", "season": "2025"}
"groups.season[0]": {"position":"FWR","games":"31","goals":"7","shots":"71",
                     "time":"2239","xG":"8.70…","team":"Arsenal","season":"2025", …}
```

## 3. Attribute coverage matrix

Цель issue — `dob / height / foot / nationality / team`. Проверено по сырому HTML **и** по полному `getPlayerData` JSON.

| Атрибут | Present? | Доказательство |
|---|:--:|---|
| `date_of_birth` / age | **N** | `birth`/`dob`/`age` = 0 совпадений в HTML и в `getPlayerData`. |
| `height` | **N** | 0 в `getPlayerData`. HTML-совпадения = рекламный виджет (`data-height="400"`, `height: 225`, `mobileStickyHeight`), не игрок. |
| `weight` | **N** | 0 совпадений нигде. |
| `preferred_foot` | **N** | В `getPlayerData` "foot" = только `shotType` ∈ {`RightFoot`,`LeftFoot`,`Head`} — нога/часть тела УДАРА, на матч/удар, не атрибут игрока. На странице Saka: `LeftFoot=366, RightFoot=115, Head=25`. HTML-совпадения = "football"/`<footer>`. |
| `nationality` / country | **N** | `nation`/`country`/`citizen` = 0 в `getPlayerData`. HTML "nation" = "pagi**nation**"; "country" = гео-таргет рекламы adlook (`"country":{"name":"Russia…"}`). |
| `team` (current) | **partial** | Есть `lastMatch.team` (+ per-match/`groups` team_title). Текущий клуб — да; история/трансферы — нет. |
| `position` | **partial/derived** | `player.favorite_position` (напр. `FWR`) и per-row `position` — это **зоны удара** (DL/MC/AMR/FWR…), производные от shotmap, а не справочное амплуа (GK/DF/MF/FW). |

Итог: из пяти целевых атрибутов **0 static-bio** (dob/height/foot/nationality), `team` — только текущий, `position` — только производная shot-зона.

> Side-note: теоретически "рабочую ногу" можно ЭВРИСТИЧЕСКИ вывести из распределения `shotType` (Saka: 366 left vs 115 right → левша). Но это derivation поверх shot-данных, а не заявленный атрибут; для `dim_player_attributes` бесполезно (§5) и рискованно (вратари/малое n).

## 4. HTTP access notes

- **Нет Cloudflare** — `tls_requests`/обычный GET отдаёт 200 на всех endpoint'ах; ни captcha, ни "Just a moment", ни `cf-*` (совпадает с integration-тестом `tests/integration/scrapers/test_real_requests.py`, бьющим understat обычным `requests.get`).
- `getPlayerData/{id}` требует header `X-Requested-With: XMLHttpRequest` (как `getLeagueData`/`getMatchData` в soccerdata) и работающих cookies (`GET https://understat.com` первым).
- `getPlayer/{id}` → 404; данные именно под `getPlayerData/{id}`.
- Без прокси/браузера. То есть **техническая** скрейпабельность есть — но скрейпить нечего (§3).

## 5. Gap vs `gold.dim_player_attributes`

`dags/sql/gold/dim_player_attributes.sql` уже собирает целевые static-attrs из существующих источников (source-suffix колонки, без winning-value logic):

| Атрибут | FBref | FotMob | SofaScore | Transfermarkt | SoFIFA | Источников |
|---|:--:|:--:|:--:|:--:|:--:|:--:|
| `date_of_birth` | ✓ (born_year) | ✓ | ✓ | ✓ | ✓ | **5** |
| `nationality` | ✓ | ✓ | ✓ | ✓ | ✓ | **5** |
| `height_cm` | — | ✓ | ✓ | ✓ (primary) | ✓ | **4** |
| `foot` | — | ✓ | ✓ | ✓ | ✓ | **4** |
| `weight_kg` | — | — | — | — | ✓ | 1 |

Каждый целевой для #38 атрибут (`dob`/`height`/`foot`/`nationality`) уже покрыт ≥4 источниками. Understat не дал бы **ни нового атрибута, ни нового покрытия** — он чистый xG/shots-источник по дизайну.

---

## Verdict — NEGATIVE (close `wontfix`)

Understat `/player/{id}` биографию не отдаёт. Killer-факты:

1. Полный backend `getPlayerData/{id}` (богаче самой страницы) содержит `birth=0, dob=0, height=0, weight=0, nation=0, country=0` — модель данных Understat = matches/shots/xG, без identity-полей.
2. Единственные не-статистические поля — `name`, `favorite_position` (shot-зона), `lastMatch.team` — все уже есть из xref/league-данных в Bronze (`bronze.understat_players`).
3. Целевые static-attrs уже покрыты 4-5 источниками в `dim_player_attributes` (§5) — добавочной ценности нет даже гипотетически.

## Next steps

- **Закрыть #38 как `wontfix`** со ссылкой на этот документ. Реализация (Bronze `understat_player_profile` → Silver → JOIN в `dim_player_attributes`) **не нужна** — нечего грузить.
- Follow-up issue НЕ создаём (positive-ветка acceptance criteria не сработала).
- Документ — единственный артефакт; код не трогаем.
