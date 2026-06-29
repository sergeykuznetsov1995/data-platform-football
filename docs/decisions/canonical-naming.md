# Column naming: Gold uses plain ids, Silver keeps `_canonical`

> Status: **active** · Created 2026-06-20 · Owner: data-platform
> Scope: all `gold.*` facts/dims + the four `silver.*` xref-resolving tables
> Decides: issue #696 (followup of #428/#438/#426)

## Decision

- **Gold layer = plain ids.** Identity columns are `player_id`, `match_id`, `team_id`,
  `player_name`; synthetic xxhash64 surrogate keys are `<entity>_id` (`rating_id`, `odds_id`,
  mirroring `dim_match.match_id` / `dim_venue.venue_id`). No `_canonical` suffix anywhere in Gold.
  #696 finished the rename for the tables #428/#438 left behind: `dim_player_attributes`,
  `fct_player_market_value`, `fct_event`, `fct_match_rating`, `fct_match_odds`, and the three
  audit siblings (`fct_player_season_stats_audit`, `fct_keeper_season_stats_audit`,
  `fct_team_season_stats_audit`). Audit tables are now plain too — aligned with
  `fct_player_match_audit`, which already used plain ids.

- **Silver layer keeps `_canonical`.** This is deliberate, not an oversight.

## Why Silver keeps `_canonical`

At Silver the suffix is **load-bearing**: it distinguishes the xref-resolved canonical id from
the raw source id that lives in the *same row*. Several Silver tables carry both side by side:

| Silver table | raw column | resolved column |
|---|---|---|
| `understat_team_match` | `team_id` (Understat numeric) | `team_id_canonical` |
| `sofascore_player_ratings` | (raw dropped) | `match_id_canonical`, `player_id_canonical` |
| `whoscored_events_spadl` | `match_id` (raw game_id) | `action_canonical` (SPADL enum) |

`understat_team_match` is the clinching case: renaming `team_id_canonical → team_id` collides
head-on with the raw `team_id` already in the row. Dropping the suffix at Silver would force a
second, semantic rename of the raw column — out of scope and against the "rename without
behavior change" rule of #696.

Gold drops the raw id (it carries only the resolved one), so there the `_canonical` suffix is
pure redundancy — which is why Gold drops it and Silver does not.

## How Gold reads Silver

Gold SELECTs alias the Silver column at read time and never touch Silver:

```sql
SELECT s.match_id_canonical AS match_id,        -- read Silver name, emit plain
       s.player_id_canonical AS player_id
FROM iceberg.silver.sofascore_player_ratings s
```

xxhash64 PK inputs keep referencing the Silver `_canonical` columns verbatim, so surrogate-key
values (`rating_id`, `odds_id`) are byte-identical before and after #696 — no PK churn.
