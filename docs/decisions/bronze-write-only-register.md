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

## 1. Register (15 live write-only tables, audit 2026-06-15)

Cost class = cost of *stopping* the scrape. **FREE** = by-product of a request made anyway
(removing it saves only HDFS/snapshots, not HTTP); **CHEAP** = 1 HTTP; **EXPENSIVE** =
per-item/per-season HTTP.

| Table | Cost | Data-quality note | Verdict | Tracking issue |
|---|---|---|---|---|
| `capology_team_payrolls` | EXPENSIVE (1 page/season) | OK | (b) future | [#603] |
| `capology_contract_extensions` | EXPENSIVE | OK | (b) future | [#603] |
| `capology_transfer_window` | EXPENSIVE | OK | (b) future | [#603] |
| `fotmob_team_stats` | FREE (shared `/api/data/leagues`) | OK | (b) future | [#600] |
| `fotmob_team_profile` | FREE (shared `/api/data/leagues`) | OK | (b) future | [#600] |
| `fotmob_team_leaderboards` | FREE (shared `/api/data/leagues`) | OK | (b) future | [#600] |
| `fotmob_transfers` | CHEAP (1 HTTP) | OK | (b) future | [#600] |
| `sofifa_teams` | EXPENSIVE (N HTTP) | OK | (b) future | [#601] |
| `sofifa_team_ratings` | CHEAP | ⚠ 15 cols 100% NULL (upstream; live-confirmed 2026-06-16) | (b) future | [#601] |
| `sofifa_leagues` | CHEAP | OK (reference) | (b) future | [#601] |
| `sofifa_versions` | CHEAP | OK (reference) | (b) future | [#601] |
| `sofascore_event_shotmap` | EXPENSIVE (~380/season) | OK | (b) future | [#602] |
| `fbref_keeper_keeper_adv` | EXPENSIVE (separate `/keepersadv/` page + CF bypass ~9.67s) | ⚠ 26 cols 100% NULL live (incl. 23 advanced GK, FBref Feb-2026); core dups `keeper` | **(c) stop** | [#606] |
| `whoscored_season_stages` | FREE (same session as `scrape_schedule`, soccerdata cache) | ⚠ `stage` all-NULL; 6 rows | (b) keep | — (§3) |
| `clubelo_team_history` | MODERATE (per-team histories) | no `rank`/`league`; **219,861 rows** (largest unread) | **(c) stop** | [#604] |

### Resolved since the 2026-06-11 inventory

| Table | Was | Now | Evidence |
|---|---|---|---|
| `clubelo_ratings_historical` | write-only | **CONSUMED** | `dags/sql/gold/fct_team_elo.sql:52` (UNION with `clubelo_ratings`) + `dags/sql/silver/xref_team.sql.j2:186`; landed via #431 / #593. |

> The #476 body lists 16 tables but the title says "15". The discrepancy is exactly
> `clubelo_ratings_historical`: now consumed, it drops out, leaving **15** live write-only
> tables. The register above is the corrected set.

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

Capology cross-check (why the 3 team tables stay write-only even though Capology *is* read):
`gold.fct_team_season_stats.sql.j2:187` reads `silver.capology_player_salaries` (the
promoted **player** table), and the team wage bill (#192) is an **aggregate of player
salaries**, not `capology_team_payrolls`. The 3 team-finance tables are read nowhere.

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

## 5. Changelog

| Date | Change | Ref |
|---|---|---|
| 2026-06-15 | Register created; 2026-06-11 inventory re-verified (15 live, not 16 — `clubelo_ratings_historical` now CONSUMED); per-table verdicts + tracking issues [#600]–[#604] filed. | #476 |
| 2026-06-15 | Correction after line-level re-check: `fbref_keeper_keeper_adv` is a **separate** FBref scrape (not a `keeper` by-product), re-classified (b) keep → **(c) stop**, issue [#606] filed; NULL counts (keeper_adv 23, sofifa_team_ratings 15) confirmed against `audit_bronze_columns.py` comments. | #476 |
| 2026-06-16 | Live `audit_bronze_columns.py` run: confirmed `sofifa_team_ratings`=15 and `fbref_keeper_keeper_adv`=26 cols 100% NULL, all 15 tables non-empty with 0 ERROR, `clubelo_team_history`=219,861 rows (largest unread). Cost/NULL notes refined. | #476 |

[#476]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/476
[#600]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/600
[#601]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/601
[#602]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/602
[#603]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/603
[#604]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/604
[#606]: https://github.com/sergeykuznetsov1995/data-platform-football/issues/606
