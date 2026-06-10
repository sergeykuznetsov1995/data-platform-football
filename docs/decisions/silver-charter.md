# Silver Layer Charter

> Status: **active** · Created 2026-06-07 · Owner: data-platform
> Scope: `iceberg.silver.*` (46 tables, 9 sources + xref subsystem)
> Companion checker: `scripts/audit_silver_charter.py` · Cheat-sheet: `dags/sql/silver/README.md`

This is the **normative contract** for what belongs in the Silver layer. It does not
re-document the footguns already in [CLAUDE.md](../../CLAUDE.md) (xref `(league, season)`
predicate, `replace_partitions`, double-cast IDs, hash-PK tiebreaker) — those still apply.
It adds the one thing CLAUDE.md leaves implicit: **the Silver/Gold boundary**.

---

## 1. Purpose & Scope

Silver is the **clean, conformed, source-faithful** layer. One Bronze fact → one Silver
fact, typed and deduplicated, with stable identity attached where cheap. Silver is **not**
where analysis happens.

- **Audience:** Gold builders. ML features (`feat_*`) and BI marts (`mart_*`, `fct_*`)
  consume Silver — they are not built here.
- **Mental model:** if a human analyst would call the output "a derived metric" or "a
  feature", it is Gold. If they would call it "the same data, just clean", it is Silver.
- **Grain rule (the load-bearing one):** a Silver table **keeps the grain of its Bronze
  source**. Changing grain upward (match → season) is a Gold fact, by definition.

> Reality check (2026-06-07 audit): several live tables violate this. They are **not**
> rewritten yet — see §7 Violators Register. This charter defines the target; the register
> tracks the gap.

---

## 2. The Charter — Allowed / Forbidden

### Allowed in Silver

| Operation | Example |
|---|---|
| Type conform | `TRY_CAST(min AS INTEGER) AS minutes`; double-cast numeric IDs `CAST(CAST(x AS BIGINT) AS varchar)`; `TRY_CAST(date ...)` |
| Season normalize | Bronze BIGINT `2024` → Silver varchar slug `'2425'` |
| Deduplicate | canonical `ROW_NUMBER()` window (see §4) |
| Column rename | `gls → goals`, `min → minutes` (to project naming standard) |
| xref enrichment | add `*_id_canonical` via JOIN on `silver.xref_*` **with `(league, season)` predicate** |
| Wide single-source conform | JOIN several Bronze tables of **one source** into one wide row of **the same entity & grain** (e.g. `fbref_player_season_profile` = 4 FBref stat tables → one player×season row) |
| Vocabulary normalization | `whoscored_events_spadl` — map raw events → SPADL actions, grain 1:1 |
| Within-row rate | `accurate_passes_pct = 100.0 * accurate / total` computed **inside a single Bronze row** |

### Forbidden in Silver (→ belongs in Gold)

| Anti-pattern | Why it's Gold | Live example |
|---|---|---|
| **Grain rollup** | `SUM/AVG/COUNT ... GROUP BY` that turns match-grain into season-grain is a fact aggregation | `whoscored_player_season_aggregate` (the 4 `*_team_season` rollups migrated to Gold — #370) |
| **Cross-entity / cross-source business JOIN** | assembling an analytical row from ≥2 different sources/entities (xref excepted) is fact assembly | (resolved: was `sofascore_team_match`; now COMPLIANT — #367) |
| **PIVOT + join + rollup as a mart** | building a presentation table, not conforming one entity | (resolved: was `sofascore_team_match`; now COMPLIANT — #367) |
| **ML features / cross-row derivations** | rolling form, rest-days, ELO-derived, ratios between matches | (none in Silver — keep it that way) |
| **Population-changing business filters** | `WHERE has_stats_json` that changes counters is fact semantics, not conform | see `fotmob_team_season` incomplete-season caveat |

### The thin line (state it explicitly)

A rate computed **within one row** is conform (allowed). The **same** rate as `AVG` over
many rows is a derivation (forbidden). `100*acc/total` on a match-row = OK;
`ROUND(AVG(possession_pct))` over a season = Gold.

---

## 3. Grain & Naming Standard

- **Table name:** `{source}_{entity}_{grain}`, `grain ∈ {match, season, profile, events, history}`.
- **Known suffix drift** (`_season` vs `_season_profile` vs `_season_aggregate`): flagged by
  the checker (R5, WARN). **Not mass-renamed** — renaming is a breaking change for Gold
  consumers; do it per-table behind an issue, not in bulk.
- **xref tables** (`xref_{player,team,match,manager,referee}`) are an identity class, exempt
  from the `{source}_` prefix.

---

## 4. Mandatory Schema Contract

Every Silver table MUST have:

- **Partition keys** (last in SELECT): `league` (varchar), `season` (varchar slug `'2425'`).
  Partitioning `ARRAY['league','season']` is applied by `run_silver_transform` — do **not**
  write it in SQL.
  - **No exceptions (#404).** `season` is the slug everywhere. Year-start bronze sources
    (FBref / FotMob / MatchHistory) are converted to slug **once in Silver** (`LPAD(MOD…)`),
    so every Silver table — and every Gold JOIN — uses the slug directly (`season = season`).
    Checker rule **S2 is a hard ERROR** for any non-varchar `season`.
- **Lineage:** `_bronze_ingested_at` (alias of Bronze `_ingested_at`). `_silver_created_at`
  is added automatically by `run_silver_transform` — do **not** write it in SQL.
- **Natural key** columns first, under an `-- Identity` header.
- **Pure SELECT:** no `CREATE TABLE` / `INSERT` in the SQL file (DDL is the runner's job).
  Exception: the Python-materialized `xref_player` resolver.

### Dedup canon

```sql
ROW_NUMBER() OVER (PARTITION BY <natural_key> ORDER BY _ingested_at DESC) AS rn
... WHERE rn = 1
```

The post-dedup PK must equal `<natural_key>` and be covered by `CHECK.no_duplicates`
in `_build_silver_checks()` (`dags/utils/silver_tasks.py`).

### Key naming

- Resolved canonical id → **`{entity}_id_canonical`** (`player_id_canonical`, `team_id_canonical`).
- Raw passthrough id → **`{entity}_id_raw`** (precedent: `whoscored_events_spadl`).
- Bare `*_id` is ambiguous (raw or canonical?) → checker S4 warns.
- Reserved-word / digit-leading column names → double-quoted (`"int"`, `"off"`, `"2crdy"`).

---

## 5. xref Enrichment Rules

- xref JOINs are the **only** sanctioned cross-table reference in Silver.
- Every JOIN on `silver.xref_*` MUST carry `AND xref.league = e.league AND xref.season =
  e.season` (CLAUDE.md footgun: 1.5–4× row fan-out without it). Checker R3 = ERROR.
- Storing the **raw** source id and deferring resolution to Gold is **also valid** — many
  clean tables do this. The charter allows both; it only mandates the `(league, season)`
  predicate when xref *is* used.

---

## 6. Audit & Compliance

The checker `scripts/audit_silver_charter.py` runs two layers:

- **Layer A (static, no Trino):** regex over `dags/sql/silver/*.sql[.j2]`.
  - `R1 ROLLUP` (ERROR) — `GROUP BY` + aggregates that change grain **upward** (season from
    match). Same-grain aggregation (player→team within a match) and PIVOT-CTEs are **not**
    flagged.
  - `R2 SILVER_ON_SILVER` (WARN) — `FROM iceberg.silver.` where the source is not `xref_*`
    and not a same-entity sibling.
  - `R3 XREF_PREDICATE` (ERROR) — `xref_*` JOIN missing `(league AND season)`.
  - `R4 CROSS_SOURCE_JOIN` (WARN) — ≥2 distinct `{source}_` prefixes in FROM/JOIN (xref excepted).
  - `R5 NAMING` (WARN) — file name not matching `{source}_{entity}_{grain}`.
  - `R6 NO_DDL` (ERROR) — file contains `CREATE TABLE`/`INSERT`.
- **Layer B (schema, Trino `DESCRIBE`):** presence of `_silver_created_at` / `league` /
  `season`; `season` is varchar not bigint; bare `*_id` naming drift.

**Verdicts:** `COMPLIANT` · `EXCEPTION` (rule-violating but sanctioned in §7, stays) ·
`VIOLATOR` (issue filed for Gold migration, not done yet).

Report format: `docs/silver_charter_audit_{YYYY-MM-DD}.md` (same markdown shape as the
bronze audit), with a per-table classifier table.

---

## 7. Violators Register

Snapshot from the 2026-06-07 audit. `EXCEPTION` = stays for now (feeds a live Gold block,
rewriting risks regression); `VIOLATOR` = issue filed, migrate to Gold later. **Nothing
here is rewritten in this pass** — this charter + checker land first.

| Table | Verdict | Rules | Rationale / Action |
|---|---|---|---|
| ~~`fotmob_team_season`~~ | MIGRATED | — | #370 team-wave: rollup moved to `gold.fotmob_team_season`, silver SQL deleted. |
| ~~`understat_team_season`~~ | MIGRATED | — | #370 team-wave: moved to `gold.understat_team_season`. |
| ~~`whoscored_team_season`~~ | MIGRATED | — | #370 team-wave: moved to `gold.whoscored_team_season`. |
| ~~`sofascore_team_season`~~ | MIGRATED | — | #370 team-wave: moved to `gold.sofascore_team_season`. |
| `fbref_team_season_profile` | COMPLIANT | — | #370: season-from-season conform (reads season-grain `bronze.fbref_team_*` + intra-season GK agg), NOT a match→season rollup. R1 detector refined to require a match/event source. |
| `whoscored_player_season_aggregate` | EXCEPTION | R1 | player season-rollup (reads `silver.whoscored_events_spadl`); feeds Gold. Migration #370 PR2. |
| `fotmob_player_season_profile` | EXCEPTION | — | PIVOT of season-grain Bronze; no longer trips R1 after the #370 detector refinement. Removed from registry in #370 PR2. |
| ~~`match_cards`~~ | MIGRATED | — | #382: cross-source assembly folded into `gold.fct_card` (reads bronze+xref directly); silver SQL deleted. |
| ~~`match_substitutions`~~ | MIGRATED | — | #382: cross-source assembly folded into `gold.fct_substitution` (reads bronze+xref directly); silver SQL deleted. |
| `whoscored_team_match` | REVIEW | R2 | aggregates `silver.whoscored_events_spadl` to team×match (same-source) — manual review; likely conform. |

> #370 team-wave (DONE): the 4 `*_team_season` rollups moved to `gold.*_team_season`
> (built by `dag_transform_fbref_gold`, Stage 1.5) and `fbref_team_season_profile` was
> reclassified COMPLIANT. The 2 player-side tables migrate in #370 PR2.
> #382 (DONE): the 2 cross-source EXCEPTION facts (`match_cards`, `match_substitutions`) were
> folded into `gold.fct_card` / `gold.fct_substitution` — the assembly (FBref+WhoScored union,
> bridge, FBref-priority dedup) now lives in the Gold SQL reading bronze+xref directly, and the
> silver SQL was deleted.
> `sofifa_player_profile_empty` — RESOLVED (#369): not a standalone table, it is the empty
> fallback for `silver.sofifa_player_profile` (issue #180; spine `xref_player WHERE 1=0`,
> active + tested). Fallback `*_empty.sql` files are now excluded from the checker scan, so
> the phantom "dead stub" no longer surfaces.

### ~~Sanctioned year-start `season`~~ — RETIRED (#404)

The 13 tables that used to store `season` as year-start are now slug ('2425') like every
other table. The year-start→slug conversion moved from the Gold boundary **into Silver**
(one `LPAD(MOD…)` per table), every Gold JOIN became a direct `season = season`, the
`SEASON_YEAR_START_OK` allowlist was removed, and S2 is now a hard ERROR for any non-varchar
`season`. xref_team/xref_match/xref_manager/xref_referee also emit slug for their FBref /
FotMob / MatchHistory branches.

> `sofascore_player_season_aggregate` was initially suspected a rollup but is **COMPLIANT**:
> its source `bronze.sofascore_player_season_stats` is already season-grain (SofaScore serves
> season stats directly), so the table is conform + xref enrichment, not an aggregation.

> Note: Gold **already** has a gold-on-gold tier doing its own rollups (CLAUDE.md §Project map).
> Migrating these is removing duplicated logic, not adding new — which is also why it is *not*
> done casually: the live `fct_*` tables depend on current shapes.

---

## 8. Changelog

| Date | Change | Ref |
|---|---|---|
| 2026-06-07 | Charter created; checker + first audit. | feature/silver-charter-audit |
| 2026-06-08 | Layer B S2: sanction year-start `season` (13 tables) as WARN; §4/§7 updated. | #373 |
| 2026-06-08 | `sofascore_team_match` R2 resolved (#367): the cross-entity `minutes`/`assists` rollup from `silver.sofascore_player_match_aggregate` was dropped (it never matched on team_id — always NULL); columns kept as NULL placeholders to preserve the downstream schema. Table is now COMPLIANT single-source conform. | #367 |
| 2026-06-08 | `match_cards` / `match_substitutions` moved REVIEW→EXCEPTION (cross-source E3/E4 facts feeding thin `gold.fct_card` / `fct_substitution`); Gold migration tracked separately. | #368 / #382 |
| 2026-06-08 | #370 team-wave: 4 `*_team_season` rollups migrated Silver→Gold (`gold.*_team_season`, Stage 1.5 of `dag_transform_fbref_gold`); `fct_team_season_stats` + audit repointed; `fbref_team_season_profile` reclassified COMPLIANT; R1 detector tightened to require a match/event source. | #370 |
