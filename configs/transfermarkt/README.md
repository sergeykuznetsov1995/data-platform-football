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

`title_only` is an independent evidence flag, never positive admission or a
blanket exclusion. Quarantine is a separate snapshot-bound inventory with all
IDs/reasons, not synthetic registry rows. A published/quarantine collision or a
legacy-core disappearance into quarantine prevents a clean migration result.

For one offline comparison, use exported current rows and the matching
discovery manifest:

```bash
python dags/scripts/transfermarkt_daily_report.py \
  --day 2026-11-16 --as-of 2026-11-17T00:00:00+00:00 \
  --registry-json inputs/promoted-current.json \
  --denominator configs/transfermarkt/denominator.tsv \
  --denominator-dual-run \
  --discovery-manifest-json inputs/discovery-manifest.json
```

Add `--execute --registry-snapshot-id tm-discovery-ea6e9c4edb2a29303527f0d3`
instead of `--registry-json` to run both read-only daily SELECTs against one
journal. Every added/removed/changed row must have an explanation. `complete`
means the diff has no unexplained rows and no legacy-core quarantine
disappearance; `migration_clean` additionally excludes unresolved delta
classes. Neither flag is a production acceptance. The 1824/25754 publication
counts are not the coverage denominator, and production switching is separate.

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
