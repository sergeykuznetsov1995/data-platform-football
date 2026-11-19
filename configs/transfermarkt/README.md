# Transfermarkt configuration

## Denominator sources (`denominator.tsv` and promoted registry, #1390)

The current default remains `denominator.tsv` (`TRANSFERMARKT_DENOMINATOR_SOURCE=tsv`).
It classifies every competition of the historical registry snapshot
(`tm-discovery-71d704b010cdbd222b4fe27c`, 16.07.2026, 803 competitions). Loader:
`scrapers/transfermarkt/denominator.py` (`load_denominator()`,
`denominator_ids()`; env `TRANSFERMARKT_DENOMINATOR_PATH` overrides the path).
Any format error fails closed. The history lane keeps this legacy contract.

The milestone-1 denominator is `live = 1` and `class` in `core_club`,
`core_national`. The current lane (`transfermarkt_scope_planner`, mode
`current_only`) plans those first, then `youth`/`reserve` and any competition
the file does not know yet; `amateur` and `archive` are not planned. The
registry's own crawl gate (`classification_status = eligible`) still applies
first, and today it excludes every youth and reserve competition, so the tail
holds only competitions the file does not know until that gate admits them
(follow-up).

Known debt: `current_saison_id` of EURO, AFCN and AFAC is still the registry's
future edition, not the last played one — to be set from tmapi
`competition/{id}/regulation` once the Transfermarkt gateway reaches tmapi.

### Opt-in versioned registry denominator

`dags/utils/transfermarkt_denominator_registry.py` derives the same current
`Denominator` contract from rows returned by `build_promoted_registry_query`.
Set `TRANSFERMARKT_DENOMINATOR_SOURCE=registry` only in an explicit offline test
or a separately authorized rollout. Registry errors never fall back to TSV;
`tsv` is the rollback and has no registry dependency.

`universe_version` is the exact `registry_snapshot_id`, not a file hash or the
number of published competitions/editions. The adapter requires one snapshot
and one unambiguous current edition per competition. Registry `eligible` is only
the first gate: catalogue tier and the live rule below still apply. Youth and
reserve exclusions include name evidence; multi-sport Games AG18/CACG stay
`youth`, NPAM stays an amateur German state cup. Registry team type, not the
legacy TSV national override, determines `core_national` and the national live
threshold. Unknown geography or an unknown live/tier decision cannot silently
enter core. Known non-live records become `archive`; youth/reserve subtypes are
retained in audit metadata.

BRRD (Campeonato Rondoniense) is a live Brazilian regional championship. The
promoted catalogue row supplies the region Americas. The competition profile
names Brazil in its keywords, and its archived SHA-256
`2e1ef6ee432b81fe84130c0b255a73f4355f2cca5afbea9e1788f64fcc3d2d19` matches
the current edition's source body. The Brazilian catalogue also links to BRRD
(body SHA-256 `b31ecdc538dc2307427f550987013970669c5d02fbd9c1007ed805253567653d`).
The adapter applies the existing regional and live rules with the verified
country when the competition ID, profile URL, source hash and catalogue region
match. A changed source body with continental geography requires fresh evidence.
[Competition profile](https://www.transfermarkt.com/campeonato-rondoniense/startseite/pokalwettbewerb/BRRD)
· [Brazilian catalogue](https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/26)

### Offline migration check and rollback (#2331)

Archive the exact promoted current rows, original discovery manifest and TSV.
Record SHA-256 for all three. Supply the expected promoted snapshot. The
manifest must identify that same snapshot and explicitly contain
`quarantined_competition_ids`, `discovery_report.discovery_quarantined` and
`discovery_report.title_only_competition_ids`. Each quarantine ID needs its own
reason. The report keeps every ID and reason, including an explicitly empty
inventory, and reports title-only evidence independently of membership.

A missing legacy core ID blocks the migration even when quarantine is empty.
A published/quarantined collision, mismatched snapshot, incomplete manifest
or unresolved changed classification also blocks the check. Inspect the exit
code, `complete`, `migration_clean`, `unexplained` and the per-ID delta.

The following command renders the two read-only queries and their bindings
for the 18 November 2026 Moscow reporting day. Use the original
`journal-snapshots.json`, which maps the fully qualified checks and signals
table names to positive integer Iceberg snapshot IDs.

```bash
python dags/scripts/transfermarkt_daily_report.py \
  --day 2026-11-18 --as-of 2026-11-19T19:47:43.279618Z \
  --registry-snapshot-id tm-discovery-556131e8d5f3a7c9831da251 \
  --registry-json inputs/promoted-current.json \
  --denominator configs/transfermarkt/denominator.tsv \
  --discovery-manifest-json inputs/discovery-manifest.json \
  --journal-snapshots-json inputs/journal-snapshots.json --denominator-dual-run
```

`sql_only` contains the plan. Execute its SQL with the original bindings on a
read-only Trino connection for the daily measurements. Alternatively the
`--execute` dual-run reads the specified promoted snapshot and executes both
queries. Without a supplied journal map it reads both Iceberg main references
once and records them. All three scans (checks, current signals, superseded
signals) use those physical versions. Preserve the exact row export for
replay after subsequent publications.

To verify rollback, repeat the example with `--denominator-source registry`
and then `--denominator-source tsv` in place of `--denominator-dual-run`.
TSV can omit the discovery manifest. Compare the SQL, bindings, core IDs,
version, definition hash and measured results with the matching dual side.
The TSV version is the SHA-256 of the bytes actually parsed.

### Three daily observations after an authorized switch

Before the window opens, record one expected registry snapshot, the first
Moscow reporting day, the deployed release and the rollout authorization.
Take three consecutive scheduled days. For each day retain the following
evidence with its timestamp and release from the delivery receipt.

| Producer | Evidence to record for that Moscow day |
|---|---|
| Current planner | Current portion's persisted report, cycle ID, `denominator_source` and `universe_version`. Check all current portions used that day. |
| Daily measurement | Executed daily report, `report_day`, `as_of`, `denominator_source` and `denominator_version`. Keep ordinary coverage and blockers. |
| Daily Bronze DQ | First completed scheduled daily report, `day_msk`, `cutoff_at`, `denominator_source` and `universe_version`. Keep its original checks and status. A cached report retains its original evidence. |
| Discovery | Record new canonical publications and times separately from the versions actually used. |

Count a day only when all three producers report registry mode and the
predeclared version on the verified release and the existing current/DQ
criteria pass. Missing provenance, a cached TSV result or mixed snapshots
leave that day unqualified. Archive every observation. If a consumer changes
snapshot, retain the prior window and explicitly pin a new version and first
day for a new series. The offline result alone authorizes no production switch.

### TSV format

Tab-separated, UTF-8, sorted by `id`. Columns:

| Column | Meaning |
|---|---|
| `id` | Transfermarkt competition id (registry `competition_id`) |
| `name` | name the competition page states (analytics name, same on both routes) |
| `country` | country the competition page states; `International` when none |
| `confederation` | registry confederation |
| `tier` | from the catalogue section: `1`…`6`, `regional`, `playoff`, `cup`, `supercup`, `international`, `youth`, `reserve` |
| `class` | `core_club`, `core_national`, `youth`, `reserve`, `amateur`, `archive` |
| `route` | `wettbewerb` (league page) or `pokal` (`/pokalwettbewerb/`, script-rendered) |
| `live` | `1` when the current edition opened in 2025 or later (national tournaments: 2022 or later, one 4-year cycle); `0` exactly for `archive` |
| `current_saison_id` | `saison_id` of the current edition by the one rule of `scrapers/transfermarkt/season.py`; for a national tournament whose registered edition is still ahead — the last played one |
| `step2_saison_ids` | `saison_id`s of the registry editions in the 10-season window (2015/16…2024/25, calendar 2015…2024); empty for cups — the registry lists one edition of them |
| `fotmob_id`, `sofascore_id` | set only when country + name match exactly one competition there (FotMob export of 23.09, `configs/sofascore/denominator.tsv` core by name) |
| `fbref_id` | empty: no id source yet |
| `reason` | `rule:<rule>` or `manual:<text>`, then `archive: …` when not live |

### Legacy TSV class rules (in this order)

1. `reserve` — registry `team_type = reserve` or catalogue section «Reserve league».
2. `youth` — `age_category = uxx`, a U-xx/youth name, the multi-sport Games (`AG18`, `CACG`).
3. `core_national` — registry `national_team`, or one of the national-team
   tournaments the registry files as club cups (review 23.09, C9-F4). This
   override is historical TSV data and is not imported into registry mode.
4. `amateur` — tiers 5–6, German state cups (`Landespokal …`), regional
   championships outside Brazil. Brazilian state championships stay `core_club`.
5. `core_club` — everything else.
6. `archive` overrides any class when the competition is not live.
