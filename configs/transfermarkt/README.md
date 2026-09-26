# Transfermarkt configuration

## Denominator registry (`denominator.tsv`, #1390)

`denominator.tsv` is the one file that says which Transfermarkt competitions
count as ours. Every competition of the canonical registry snapshot
(`tm-discovery-71d704b010cdbd222b4fe27c`, 16.07.2026, 803 competitions) has a
row. Loader: `scrapers/transfermarkt/denominator.py` (`load_denominator()`,
`denominator_ids()`; env `TRANSFERMARKT_DENOMINATOR_PATH` overrides the path).
Any format error fails closed.

The milestone-1 denominator is `live = 1` and `class` in `core_club`,
`core_national`. The current lane (`transfermarkt_scope_planner`, mode
`current_only`) plans those first, then `youth`/`reserve` and any competition
the file does not know yet; `amateur` and `archive` are not planned. The
registry's own crawl gate (`classification_status = eligible`) still applies
first, and today it excludes every youth and reserve competition, so the tail
holds only competitions the file does not know until that gate admits them
(follow-up). The history lane takes the same live core.

Known debt: `current_saison_id` of EURO, AFCN and AFAC is still the registry's
future edition, not the last played one — to be set from tmapi
`competition/{id}/regulation` once the Transfermarkt gateway reaches tmapi.

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

### Class rules (in this order)

1. `reserve` — registry `team_type = reserve` or catalogue section «Reserve league».
2. `youth` — `age_category = uxx`, a U-xx/youth name, the multi-sport Games (`AG18`, `CACG`).
3. `core_national` — registry `national_team`, or one of the national-team
   tournaments the registry files as club cups (review 23.09, C9-F4).
4. `amateur` — tiers 5–6, German state cups (`Landespokal …`), regional
   championships outside Brazil. Brazilian state championships stay `core_club`.
5. `core_club` — everything else.
6. `archive` overrides any class when the competition is not live.
