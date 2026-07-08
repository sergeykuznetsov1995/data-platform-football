# WhoScored Events Backfill — Known-Missing Register (#878)

> Status: **active** · Created 2026-07-08 · Owner: data-platform
> Scope: `iceberg.bronze.whoscored_events` completeness floor after the #878
> top-5 × 10-season events backfill.
> Origin: issue [#895] (DQ validation of the #878 backfill). Refs: #878,
> PR #885 (fast schedule path), PR #890 (`player_id` parser fix).
> Enforced by: `dags/utils/e3_dq.py::WHOSCORED_KNOWN_MISSING_GAME_IDS` +
> `completeness_check_events*`. **This file is the source of truth for the 49
> (§2); keep the code constant in sync with it.**

## Summary

Full events backfill of top-5 leagues × 10 seasons (2016–2025).
`bronze.whoscored_events` = **27,922,944 rows** across **17,901 matches**.
Coverage **17901 / 17950** scheduled fixtures (**99.73%**); **49** scheduled
fixtures have no events — **all sanctioned** (§2). Validated 2026-07-08 against
the live table (local Trino); the live schedule↔events missing set equals this
register exactly, and cross-checks 1:1 against the backfill run logs
(`/root/ws878_backfill/state/unit_events_*.log`).

DQ verdict: completeness ✓ (49 sanctioned floor), parse-integrity ✓ with two
minor flagged findings (§3), lineups ✓ (§4).

## §1 Completeness (schedule ↔ events)

`COUNT(DISTINCT schedule.game_id)` vs `COUNT(DISTINCT events.game_id)` per
`(league, season)`, `game_id IS NOT NULL`. Only the 11 season-slices with a gap
are listed; the other **39** `(league, season)` slices are **100%** complete.

| League | Season | Scheduled | Loaded | Missing |
|---|---|---|---|---|
| ENG-Premier League | 1920 | 380 | 379 | 1 |
| ENG-Premier League | 2122 | 380 | 377 | 3 |
| ESP-La Liga | 2122 | 380 | 379 | 1 |
| FRA-Ligue 1 | 1617 | 381 | 379 | 2 |
| FRA-Ligue 1 | 1920 | 300 | 279 | 21 |
| FRA-Ligue 1 | 2122 | 380 | 375 | 5 |
| FRA-Ligue 1 | 2223 | 380 | 379 | 1 |
| FRA-Ligue 1 | 2324 | 306 | 305 | 1 |
| GER-Bundesliga | 2223 | 306 | 304 | 2 |
| ITA-Serie A | 2122 | 380 | 374 | 6 |
| ITA-Serie A | 2223 | 381 | 375 | 6 |
| **Total (all 50 slices)** | | **17950** | **17901** | **49** |

Reconciliation query (equivalent to the in-scraper `skip_existing` residual):

```sql
SELECT s.game_id, s.league, s.season
FROM iceberg.bronze.whoscored_schedule s
LEFT JOIN (SELECT DISTINCT game_id FROM iceberg.bronze.whoscored_events) e
       ON e.game_id = s.game_id
WHERE e.game_id IS NULL AND s.game_id IS NOT NULL;
```

## §2 Sanctioned known-missing register (49)

Three sanctioned reasons. None is a scraper defect; the completeness gate
excludes these and reddens only on a **new** gap.

### 2a. FRA-Ligue 1 1920 — COVID cancellation (21)

Ligue 1 was suspended mid-March 2020 and the season later cancelled; these
fixtures (2020-03-13 … 2020-03-22) were **never played** (the schedule itself
holds 300, not 380, fixtures for the truncated season).

| game_id | Fixture |
|---|---|
| 1376707 | 2020-03-18 Strasbourg–Paris Saint-Germain |
| 1376716 | 2020-03-15 Paris Saint-Germain–Nice |
| 1376717 | 2020-03-13 Lyon–Reims |
| 1376718 | 2020-03-14 Nantes–Nîmes |
| 1376719 | 2020-03-14 Amiens–Angers |
| 1376720 | 2020-03-14 Toulouse–Metz |
| 1376721 | 2020-03-14 Brest–Lille |
| 1376722 | 2020-03-15 Bordeaux–Rennes |
| 1376723 | 2020-03-15 Monaco–Saint-Étienne |
| 1376724 | 2020-03-14 Strasbourg–Dijon |
| 1376725 | 2020-03-14 Montpellier–Marseille |
| 1376726 | 2020-03-21 Nice–Montpellier |
| 1376727 | 2020-03-21 Metz–Brest |
| 1376728 | 2020-03-20 Lille–Monaco |
| 1376729 | 2020-03-22 Reims–Nantes |
| 1376730 | 2020-03-22 Marseille–Paris Saint-Germain |
| 1376731 | 2020-03-22 Saint-Étienne–Strasbourg |
| 1376732 | 2020-03-21 Nîmes–Bordeaux |
| 1376733 | 2020-03-21 Angers–Toulouse |
| 1376734 | 2020-03-21 Rennes–Lyon |
| 1376735 | 2020-03-21 Dijon–Amiens |

### 2b. FRA-Ligue 1 1617 — abandoned matches (2)

| game_id | Fixture | Note |
|---|---|---|
| 1076372 | 2017-04-16 SC Bastia–Lyon | abandoned (pitch invasion) |
| 1351262 | 2016-12-03 Metz–Lyon | abandoned |

### 2c. Played fixtures WhoScored serves no `matchCentreData` for (26)

Real, played matches; WhoScored simply does not return `matchCentreData` for
them. Re-attempted across 3 backfill retry rounds (~12 attempts/match), all
re-confirmed absent (`/root/ws878_backfill/queue.txt` Round-3 scope).

| League | Season | game_id(s) |
|---|---|---|
| ENG-Premier League | 1920 | 1376255 (Burnley–Sheffield United) |
| ENG-Premier League | 2122 | 1549586, 1549627, 1549733 |
| ESP-La Liga | 2122 | 1559829 (Athletic Club–Barcelona) |
| FRA-Ligue 1 | 2122 | 1558343, 1558457, 1558484, 1558514, 1558548 |
| FRA-Ligue 1 | 2223 | 1643925 |
| FRA-Ligue 1 | 2324 | 1741059 |
| GER-Bundesliga | 2223 | 1643097, 1643214 |
| ITA-Serie A | 2122 | 1575817, 1575876, 1575881, 1575889, 1575891, 1575896 |
| ITA-Serie A | 2223 | 1651493, 1651573, 1651673, 1651695, 1651772, 1651789 |

## §3 Parse integrity (2026-07-08, whole table)

| Check | Result | Verdict |
|---|---|---|
| `player` float-strings (e.g. `'3.55401E5'`, regression #890) | **0** / 27.9M | ✓ PASS |
| `player` NOT NULL but `player_id` NULL | **0** | ✓ PASS |
| `qualifiers` not valid JSON (`TRY(json_parse)` NULL) | **0** | ✓ PASS |
| `type`/`outcome_type`/`period` dict-strings (not flat displayName) | **0** | ✓ PASS |
| events/match distribution | median 1560, p01 1325, p99 1804, avg 1560 | ✓ PASS |
| exact content-key duplicate rows | **39** (across 37 matches, ≤2 each) | ⚠ NOTE (harmless) |
| truncated matches (<1000 events) | **10** (cut off minute 0–54) | ⚠ FLAG (followup) |

**On `eventId`:** Bronze has **no** native `eventId` — the raw Opta id is
dropped by the parser (`events_fetcher.py`), and `event_id` is *synthesised* in
Silver (`whoscored_events_spadl.sql`). The issue's "0 duplicated `eventId`"
check is therefore run on the full **content key** (the same natural key Silver
de-dups on), not on a persisted id.

**39 content-key dups (harmless):** 37 matches carry 1–2 exact-duplicate event
rows each (not a wholesale double-append — `max` events/match = 2038, no match
is doubled). Silver's `ROW_NUMBER() OVER (PARTITION BY <content key> …) WHERE
rn=1` de-dup (`dags/sql/silver/whoscored_events_spadl.sql`) collapses each to a
single row, so **Gold is unaffected**. Bronze-hygiene footnote only.

**10 truncated matches (flagged — followup):** real, played matches whose event
stream is cut off early (last minute 0–54 instead of ~90+):

| game_id | Fixture | Events | Last min |
|---|---|---|---|
| 1903413 | 2026-05-04 Everton–Manchester City | 2 | 0 |
| 1834391 | 2025-02-09 Holstein Kiel–Bochum | 22 | 0 |
| 1821121 | 2025-03-15 Southampton–Wolves | 57 | 2 |
| 1911525 | 2026-05-17 Nantes–Toulouse | 410 | 21 |
| 1903171 | 2025-09-20 West Ham–Crystal Palace | 420 | 21 |
| 1821203 | 2024-10-26 Everton–Fulham | 580 | 30 |
| 1549615 | 2021-10-17 Newcastle–Tottenham | 725 | 53 |
| 1821065 | 2024-08-24 Manchester City–Ipswich | 841 | 54 |
| 1485441 | 2021-03-02 Manchester City–Wolves | 853 | 46 |
| 1903144 | 2025-08-30 Wolves–Everton | 888 | 52 |

Root cause: `scrape_events(skip_existing=True)` marks a fixture "done" once it
has **≥1 event + a lineup row**, so a truncated payload is never retried. These
pass completeness (they *have* events) but flow into Silver/Gold under-populated.
Recommended followup (**ingest scope, out of this DQ task**): targeted
`force_replace` re-scrape of these `game_id`s, and tighten `skip_existing` to a
minimum-event threshold. Tracked separately.

## §4 Lineups reconciliation (#895 §3)

`bronze.whoscored_lineups` is written from the same `matchCentreData` as events.

| events games | lineups games | in events ∖ lineups | in lineups ∖ events |
|---|---|---|---|
| 17901 | 17901 | 0 | 0 |

Perfect parity — lineups cover exactly the same 17901 matches as events.

## §5 Changelog

| Date | Change | Ref |
|---|---|---|
| 2026-07-08 | Register created. 49 known-missing sanctioned (COVID 21 / abandoned 2 / no-matchCentreData 26). Completeness gate `completeness_check_events*` + `WHOSCORED_KNOWN_MISSING_GAME_IDS` added to `dags/utils/e3_dq.py` (wired into `dag_transform_e3` + `dag_e3_backfill`); `whoscored_events` wipe-floor raised 500k→20M in `config.py`. Parse-integrity ✓ (0 float-strings, 0 dict-strings, 0 bad JSON); 39 bronze content-dups (Silver-deduped, harmless) + 10 truncated matches flagged for re-scrape followup. Lineups parity 17901=17901. | #895 |
