# Bronze Write-Only Register

> Status: **active** · Created 2026-06-15 · Owner: data-platform
> Scope: `iceberg.bronze.*` tables that have an active producer but **no** Silver/Gold reader.
> Origin: issue [#476] (Trino write-only inventory, 2026-06-11). Companion: `scripts/inventory_bronze_orphans.py`.

A **write-only** Bronze table is the opposite of an *orphan*: it has a live producer
(scraper/DAG writes it), but **nothing reads it** — no `dags/sql/silver/*.sql[.j2]`, no
`dags/sql/gold/*.sql`, no `dags/utils/*.py`. It costs scrape time, HDFS and Iceberg
snapshots for zero downstream value (yet).

`inventory_bronze_orphans.py` does **not** catch these — its KEEP-set is every table with a
producer, so a produced-but-unread table is silently kept. This register is the manual
backstop until a `produced ∧ 0-readers` detector exists (deferred, see §4).

Per-table verdict vocabulary (from #476):

- **CONSUMED** — re-verification found a live reader; no longer write-only.
- **(b) future** — keep ingesting, consumption is planned; **must** carry a tracking issue.
- **(b) keep** — keep ingesting, but it is a **free by-product** (no marginal scrape cost) of
  an already-needed request; documented, no issue (stopping would save nothing meaningful).
- **(c) stop** — no plan and real scrape cost → recommend retiring the scrape (own issue).

---

## 1. Register (2 live write-only tables, audit 2026-06-15; 4 SoFIFA via #601 + 4 FotMob via #600 + 3 Capology via #603 CONSUMED, 2026-06-17)

Cost class = cost of *stopping* the scrape. **FREE** = by-product of a request made anyway
(removing it saves only HDFS/snapshots, not HTTP); **CHEAP** = 1 HTTP; **EXPENSIVE** =
per-item/per-season HTTP.

| Table | Cost | Data-quality note | Verdict | Tracking issue |
|---|---|---|---|---|
| `whoscored_season_stages` | FREE (same session as `scrape_schedule`, soccerdata cache) | ⚠ `stage` all-NULL; 6 rows | (b) keep | — (§3) |
| `clubelo_team_history` | MODERATE (per-team histories) | no `rank`/`league`; **219,861 rows** (largest unread) | **(c) stop** | [#604] |

### Resolved since the 2026-06-11 inventory

| Table | Was | Now | Evidence |
|---|---|---|---|
| `clubelo_ratings_historical` | write-only | **CONSUMED** | `dags/sql/gold/fct_team_elo.sql:52` (UNION with `clubelo_ratings`) + `dags/sql/silver/xref_team.sql.j2:186`; landed via #431 / #593. |
| `sofascore_event_shotmap` | write-only (EXPENSIVE ~380/season) | **CONSUMED** | `dags/sql/silver/sofascore_shots.sql` (shot×match projection) → `dags/sql/gold/fct_shot_audit.sql` (cross-source xG/SoT validation vs Understat `fct_shot`); landed via #602. |
| `sofifa_teams` | write-only (EXPENSIVE N HTTP) | **CONSUMED** | `dags/sql/silver/sofifa_team_profile.sql` (team projection) + `sofifa` source branch in `dags/sql/silver/xref_team.sql.j2`; landed via #601. |
| `sofifa_team_ratings` | write-only (CHEAP; 15 cols 100% NULL) | **CONSUMED** | `dags/sql/silver/sofifa_team_profile.sql` (8 live ratings). The 15 dead FC-26 cols removed from the parser (`scrapers/sofifa/flaresolverr_reader.read_team_ratings`) + Bronze (`scripts/drop_sofifa_team_ratings_dead_columns.py`); landed via #601. |
| `sofifa_leagues` | write-only (CHEAP reference) | **CONSUMED** | `dags/sql/silver/sofifa_league_lookup.sql`; landed via #601. |
| `sofifa_versions` | write-only (CHEAP reference) | **CONSUMED** | `dags/sql/silver/sofifa_edition_lookup.sql`; landed via #601. |
| `fotmob_team_profile` | write-only (FREE by-product) | **CONSUMED** | `dags/sql/silver/fotmob_team_profile.sql` (team×season conform); landed via #600. |
| `fotmob_team_stats` | write-only (FREE by-product) | **CONSUMED** | `dags/sql/silver/fotmob_team_standings.sql` (team×season standings conform); landed via #600. |
| `fotmob_team_leaderboards` | write-only (FREE by-product) | **CONSUMED** | `dags/sql/silver/fotmob_team_leaderboards.sql` (long-form team×stat conform); landed via #600. |
| `fotmob_transfers` | write-only (CHEAP; 1 HTTP) | **CONSUMED** | `dags/sql/silver/fotmob_transfers.sql` (event-grain conform); landed via #600. |
| `capology_team_payrolls` | write-only (EXPENSIVE 1 page/season) | **CONSUMED** | `dags/sql/silver/capology_team_payrolls.sql` (declared club payroll, club×season + `canonical_id` via existing `xref_team` capology branch); landed via #603. |
| `capology_transfer_window` | write-only (EXPENSIVE) | **CONSUMED** | `dags/sql/silver/capology_transfer_window.sql` (net transfer balance, club×season); landed via #603. |
| `capology_contract_extensions` | write-only (EXPENSIVE) | **CONSUMED** | `dags/sql/silver/capology_contract_extensions.sql` (player contract snapshot, player×season + `canonical_id` via `xref_player`); landed via #603. |

> The #476 body lists 16 tables but the title says "15". The discrepancy is
> `clubelo_ratings_historical` (now consumed), which left 15. Consuming
> `sofascore_event_shotmap` (#602) left 14; promoting the 4 SoFIFA tables
> (#601) left 10; consuming the 4 FotMob team/transfers tables (#600) left 6;
> promoting the 3 Capology team-finance tables (#603) leaves **3** live
> write-only tables. The register above is the corrected set.

### Non-prod-producer orphans — stopped + dropped via #614 (not part of the write-only set)

Distinct from the §1 register (which is *prod-producer* ∧ 0-reader): these three FBref
tables had a producer **only** in the non-prod selenium path `FBrefScraper.scrape_all()`
(runner `--scraper-type selenium --mode full`), which the production DAG never invokes — it
runs `single_stat` + `combined_match_data`. They were never in the parser contract
(`scripts/audit_bronze_columns.py::EXPECTED_TABLES`) and had no Silver/Gold reader, so
`inventory_bronze_orphans.py` flagged them **droppable**. Frozen at small stale row counts.

| Table | live rows | Verdict | Superseded by |
|---|---|---|---|
| `fbref_player_stats_extended` | 551 | **(c) stop + drop** | per-stat-type `fbref_player_{stats,shooting,playingtime,misc}` + Silver `fbref_player_season_profile.sql` join |
| `fbref_team_stats_extended` | 20 | **(c) stop + drop** | per-stat-type `fbref_team_*` + `fbref_team_season_profile.sql` |
| `fbref_keeper_stats` | 40 | **(c) stop + drop** | `fbref_keeper_keeper` (+ `fbref_keeper_keeper_adv`, itself stopping via #606) |

Executed in #614 (mirrors #604): removed the producer sections from selenium `scrape_all()`
(plus the now-orphan `_merge_{team,player,keeper}_stats` / `_find_join_column` in
`data_mergers.py`, the runner full-mode flags, and their tests), then dropped the tables via
`inventory_bronze_orphans.py --drop`. No `EXPECTED_TABLES` change — they were never in it.

---

## 2. Methodology (how each table was re-verified)

For every candidate, grep for `iceberg.bronze.<table>` (and the bare name) across:

- `dags/sql/silver/*.sql` and `*.sql.j2`
- `dags/sql/gold/*.sql` and `*.sql.j2`
- `dags/utils/*.py` (silver/gold task builders, resolvers)

A hit counts as a **reader** only if it is a real `FROM`/`JOIN` in a `SELECT`. Mentions in
SQL comments do **not** count — the #476 body itself flagged `clubelo_team_history` in
`xref_team.sql.j2` as "just a comment", and that held up: it appears only in explanatory
comments (`fct_team_elo.sql:18`, `xref_team.sql.j2:61`) and the Iceberg-maintenance list
(`dags/utils/maintenance_tasks.py:55`), never in a `SELECT`.

Capology cross-check (the 3 team-finance tables, CONSUMED via #603): they are now read by
their own Silver projections (`dags/sql/silver/capology_{team_payrolls,transfer_window,
contract_extensions}.sql`). NOTE: the Gold team **wage bill** (`fct_team_season_stats.sql.j2`
`cap_finance` CTE, #192) STILL aggregates `silver.capology_player_salaries` (player salaries),
NOT `capology_team_payrolls` — #603 deliberately left Gold untouched (Silver-only). Whether the
direct team payroll should supplement/replace the salary-sum wage bill is a tracked followup
(see §4 / the #603 PR body).

---

## 3. Free by-product kept without an issue

One table is a **free by-product** of a request made for another, needed table, so "stop
scraping" would remove a parse+save but **not** the HTTP call — near-zero saving. Kept and
documented here; no tracking issue (avoids p3 issue-spam):

- **`whoscored_season_stages`** — `scrape_season_stages()` calls `read_season_stages()` in the
  **same** scraper process as `scrape_schedule()` (`run_whoscored_scraper.py:157,173`); the
  soccerdata reader caches the tournament page, so no extra HTTP in practice. The `stage`
  column is all-NULL for the in-scope single-stage leagues. Free; kept for the day a
  multi-stage competition enters scope.

> **Correction (2026-06-15):** `fbref_keeper_keeper_adv` was initially placed here as a "free
> by-product of `keeper`". That was **wrong** — `KEEPER_STAT_TYPES = ['keeper','keeper_adv']`
> (`scrapers/fbref/constants.py:42-45`) makes each a **separate** `create_single_stat_task`
> → separate `BashOperator` → separate nodriver+CF scrape of a distinct URL (`/keepersadv/`).
> It is an EXPENSIVE FBref request whose 23 advanced columns are all-NULL and whose core
> columns duplicate the (consumed) `fbref_keeper_keeper`. Re-classified **(c) stop** → [#606].

---

## 4. Deferred

- **Auto-detector** `scripts/audit_bronze_write_only.py` (`produced ∧ 0 Silver/Gold readers`)
  — would keep this register fresh and close the `inventory_bronze_orphans.py` gap. Not built
  in this pass (out of scope for #476, which chose doc + issues). Nice-to-have.
- **OpenMetadata** `configs/openmetadata/descriptions/bronze_*.yaml` have no status field;
  a `future-consumption (#N)` note in each description text would mirror this register in the
  catalog. Deferred to keep the #476 PR surgical.

---

## 5. Gold tables reading Bronze directly — one-hop audit (#704)

> **Distinct category from the §1 write-only register.** These Bronze tables ARE
> read (by Gold). #704 asked about **one-hop purity** (Gold = one-hop from
> Silver), not write-only status. Full scan:
> `grep -n 'iceberg\.bronze\.' dags/sql/gold/*.sql[.j2]` (non-comment FROM/JOIN).

Verdict vocabulary: **norm** (sanctioned bridge or source-without-Silver — keep);
**done** (already reads Silver); **fixed #704** (Silver lifted in this PR);
**followup** (tracked issue).

| Gold table | Bronze read | Type | Verdict |
|---|---|---|---|
| `fct_lineup` | `espn_schedule` | `*_schedule` bridge | norm |
| `fct_match_timeline` | `whoscored_schedule` | `*_schedule` bridge | norm |
| `fct_team_match` (`.j2`/`_audit`) | `whoscored_schedule` | `*_schedule` bridge | norm |
| `fct_team_season_stats` (`.j2`/`_audit`) | `whoscored_schedule` | `*_schedule` bridge | norm |
| `dim_match` | `fbref_match_managers` | source w/o Silver | norm |
| `fct_manager_stint` | `fbref_match_managers` | source w/o Silver | norm |
| `fct_team_elo` | `clubelo_ratings` (+`_historical`) | source w/o Silver | norm |
| `fct_standings` | — | reads Silver | done (#702) |
| `fct_event` | `whoscored_events` | `team_id→team_name` bridge (NOT event data; ~2 rows/fixture out; ADR-2 inline) | norm (sanctioned bridge) |
| `fct_shot` | `understat_shots`, `understat_players` | shot DATA + assist dict | **fixed #704** |
| `dim_venue` | `espn_matchsheet` | venue-name DATA | followup [#735] |
| `fct_match_timeline` | `whoscored_events` | raw card/sub/goal DATA (fallback) | followup [#736] |
| `fct_team_season_stats.j2` | `whoscored_events` | penalty DATA (#161) | followup [#736] |

**`fct_event` (norm, not data):** the issue flagged it priority-#1 ("709k rows,
high volume"), but the `bronze.whoscored_events` read is the `event_team_names`
CTE — a `(game_id, team_id) → team_name` bridge feeding `xref_team`, NOT a read
of SPADL event data (which comes from `silver.whoscored_events_spadl`). The 709k
is fct_event's output grain, not the bronze scan result (~2 rows/fixture out).
Documented as a sanctioned name-bridge (analogous to the `*_schedule` bridges);
SQL unchanged. Optional future cleanup: carry `team` into
`silver.whoscored_events_spadl` to drop the bridge entirely — low priority.

**`fct_shot` fix (#704):** new `silver.understat_shots` (mirrors
`silver.sofascore_shots`) conforms the Understat shotmap + resolves
team/player/assist via `xref_*` with the `(league, season)` predicate;
`gold.fct_shot` now reads it. The only bronze read kept in `fct_shot` is
`bronze.understat_schedule` — the understat→fbref `*_schedule` match bridge —
because `xref_match` has no `source='understat'` rows yet (Phase B), so the
cross-source match-id bridge stays a Gold job by charter §5.

Row-level `EXCEPT` diff (old vs new `gold.fct_shot`, 58580 rows each, PK-paired
1:1 — no row added/dropped): **167 rows (0.285%) differ, all in canonical
resolution only** — `team_id`, `xg`, coords, `result`, `minute` are identical.
The shift is `player_id` (60) + `assist_player_id` (107), driven by the mandatory
`(league, season)` xref predicate: the old Gold path joined `xref_player` on
`source_id` alone (`ARBITRARY` across seasons), the new path joins the full key
(charter R3, like `sofascore_shots`). 53 of these are `xref_player` season-coverage
gaps the old cross-season borrow masked (now orphan-tolerant NULL); 7 are
season-drift canonicals the new path resolves season-correctly. `null_player`
225 vs ~172 — well within the `shot_orphan_player_rate ≤ 3290` DQ gate. Coverage
gap tracked as followup [#738].

---

## 6. Changelog

| Date | Change | Ref |
|---|---|---|
| 2026-06-15 | Register created; 2026-06-11 inventory re-verified (15 live, not 16 — `clubelo_ratings_historical` now CONSUMED); per-table verdicts + tracking issues [#600]–[#604] filed. | #476 |
| 2026-06-15 | Correction after line-level re-check: `fbref_keeper_keeper_adv` is a **separate** FBref scrape (not a `keeper` by-product), re-classified (b) keep → **(c) stop**, issue [#606] filed; NULL counts (keeper_adv 23, sofifa_team_ratings 15) confirmed against `audit_bronze_columns.py` comments. | #476 |
| 2026-06-16 | Live `audit_bronze_columns.py` run: confirmed `sofifa_team_ratings`=15 and `fbref_keeper_keeper_adv`=26 cols 100% NULL, all 15 tables non-empty with 0 ERROR, `clubelo_team_history`=219,861 rows (largest unread). Cost/NULL notes refined. | #476 |
| 2026-06-17 | 4 SoFIFA tables promoted to Silver → **CONSUMED**: `sofifa_team_profile.sql` (+ `sofifa` source branch in `xref_team.sql.j2`), `sofifa_league_lookup.sql`, `sofifa_edition_lookup.sql`. `sofifa_team_ratings` 15 dead FC-26 cols removed (parser override + `drop_sofifa_team_ratings_dead_columns.py`, dropped from `EXPECTED_NULL`). 14 → 10 live write-only. | #601 |
| 2026-06-17 | 4 FotMob team/transfers tables promoted to Silver → **CONSUMED** (conform-only, canonical_id resolution deferred to Gold): `fotmob_team_profile.sql`, `fotmob_team_standings.sql` (from `fotmob_team_stats`), `fotmob_team_leaderboards.sql`, `fotmob_transfers.sql`; registered in `dag_transform_fotmob_silver.py` + bronze schemas added to fixture. 10 → 6 live write-only. | #600 |
| 2026-06-17 | 3 Capology team-finance tables promoted to Silver → **CONSUMED**: `capology_team_payrolls.sql` (declared club payroll), `capology_transfer_window.sql` (net transfer balance), `capology_contract_extensions.sql` (player contract snapshot); registered in `dag_transform_capology_silver.py` + 3 render-tests. `canonical_id` resolved in Silver via existing `xref_team`/`xref_player` capology branches (team 239/240, contract 680/810 live). Gold wage bill untouched (Silver-only); replace/supplement = followup. 6 → 3 live write-only. | #603 |
| 2026-06-20 | 3 FBref non-prod-producer orphans stopped + dropped (`fbref_player_stats_extended` 551, `fbref_team_stats_extended` 20, `fbref_keeper_stats` 40): producer existed only in selenium `scrape_all` (`--mode full`, never run by prod), not in the parser contract, 0 Silver/Gold readers, superseded by per-stat-type tables + Silver joins. Removed producer + orphan merge helpers/tests; dropped tables. Mirrors #604. Not part of the §1 write-only set. | #614 |
| 2026-06-20 | `fbref_keeper_keeper_adv` scrape **stopped** + table **dropped**: removed `'keeper_adv'` from `KEEPER_STAT_TYPES` (DAG no longer creates the `keeper_keeper_adv` task), cleaned dormant url-mapping/schema/docstrings, dropped the 3 `audit_bronze_columns.py` entries (`EXPECTED_NULL`/`EXPECTED_CONSTANT`/contract), removed the OM description YAML, `DROP TABLE` via `scripts/drop_fbref_keeper_keeper_adv.sql`. 26 cols 100% NULL since FBref Feb-2026; core cols duplicate the consumed `fbref_keeper_keeper`. 3 → 2 live write-only. | #606 |

| 2026-06-20 | §5 added — Gold→Bronze one-hop audit (#704): `fct_shot` lifted to `silver.understat_shots` (was direct `bronze.understat_shots` + `understat_players`; only the `understat_schedule` match bridge kept in Gold); `fct_event` documented as a sanctioned `team_id→name` bridge (SQL unchanged); `fct_standings` already Silver (#702); `dim_venue` (#735) + `whoscored_events` data reads in `fct_match_timeline`/`fct_team_season_stats` (#736) filed as followups. | #704 |

[#476]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/476
[#600]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/600
[#601]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/601
[#602]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/602
[#603]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/603
[#604]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/604
[#606]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/606
[#614]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/614
[#704]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/704
[#735]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/735
[#736]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/736
[#738]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/738
