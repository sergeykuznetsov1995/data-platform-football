# Transfermarkt native v2: production regression checklist

This is the executable checklist for the closed `source:transfermarkt` cards in
GitHub Project #2. A closed card is historical context, not production proof.
The last column names the current offline evidence; live Airflow/Trino evidence
is recorded only after a separately approved production cycle.

## Source-to-consumer matrix

| Source capture | Parser entity | Bronze contract | Silver contract(s) | Gold / canonical consumer |
|---|---|---|---|---|
| Official competition catalogs + profiles | `competition_registry`, `competition_editions` | `transfermarkt_competitions`, `transfermarkt_competition_editions` | `transfermarkt_competitions_v2`, `transfermarkt_competition_editions_v2` | scope planner, manifests, cutover control |
| One listing plus reused squad responses | `squad_memberships` | `transfermarkt_squad_memberships` | `transfermarkt_squad_memberships_v2`, `transfermarkt_player_team_season_assignment_v2`, `transfermarkt_player_xref_global_v2` | canonical `transfermarkt_players`, team-season market value |
| Same reused squad responses | `player_attribute_observations` | `transfermarkt_player_attribute_observations` | `transfermarkt_player_attribute_observations_v2`, `transfermarkt_player_attributes_v2` | canonical players, `dim_player_attributes` |
| Same reused squad responses | `player_contract_observations` | `transfermarkt_player_contract_observations` | `transfermarkt_player_contract_observations_v2`, `transfermarkt_player_attributes_v2` | canonical players; national-team scopes are explicit `not_applicable` |
| Globally deduplicated player endpoint | `market_value_points` | `transfermarkt_market_value_points` | `transfermarkt_market_value_points_v2` | `fct_player_market_value_v2`, `transfermarkt_team_season_market_value_v2` |
| Globally deduplicated player endpoint | `transfer_events` | `transfermarkt_transfer_events` | `transfermarkt_transfer_events_v2` | `fct_transfer_v2` |
| Reused team scope plus coach history/profile | `coach_profiles` | `transfermarkt_coach_profiles` | `transfermarkt_coach_profiles_v2` | `dim_manager_v2` |
| Reused team scope plus coach history/profile | `coach_stints` | `transfermarkt_coach_stints` | `transfermarkt_coach_stints_v2` | `dim_manager_v2`, canonical coaches |

Fixtures, stages, results, awards and achievements are not advertised as
supported entities: there is no production table contract for them. Awards and
achievements remain roadmap-only until source grain, tables and DQ are added.

Every concrete table is also registered in
`dags/utils/transfermarkt_native_v2.py::TABLE_CONTRACTS` with grain, natural
key, dedup order, lineage, DQ, Airflow task, OpenMetadata file and consumers.

## Closed-card regressions

| Card | Invariant retained by v2 | Offline evidence |
|---|---|---|
| #48 | full anchor capture, blocking row/key/null/freshness DQ | scope manifest/DQ tests; live scheduler run still required |
| #59 | Transfermarkt stays in global player xref and conflict checks | `test_xref_player_resolver*`, `test_xref_dq.py` |
| #60 | typed player attributes and canonical projection | native Silver SQL alignment/execution tests |
| #61 | lossless dated market-value timeline | SQL suite and market-value history execution tests |
| #62 | stable transfer-event key plus player/team xref | transfer SQL and Gold DQ tests |
| #64 | ingest freezes one exact scope set; a freshly approved transform builds it once | ingest/Silver/master DAG tests |
| #74 | Transfermarkt attributes feed canonical player attributes | Gold SQL and dashboard consumer audit |
| #285 | all declared Bronze tables/columns are audited | `test_audit_bronze_columns.py` |
| #335 | every native Bronze table has table/column descriptions | table-contract test plus OpenMetadata dry-run |
| #457 | consecutive endpoint failures abort, never return a partial frame | `test_transfermarkt_scraper.py` failure-cap tests |
| #484 | intermittent partial success fails the completeness ratio | partial-scrape and replace-guard tests |
| #486 | a bounded smoke/career window cannot replace a full anchor scope | runner replace/upsert tests |
| #493 | canonical coverage is a blocking current-scope signal | Silver/xref DQ tests; thresholds are not weakened |
| #500 | Savinho/Sávio alias remains deterministic | medallion config and resolver tests |
| #512 | CLI errors are hard failure, never fallback exit 2 | runner argparse tests |
| #619 | dated coach history and curated aliases feed managers | coach parser/render/manager alias tests |
| #620 | bounded global player windows accumulate with checkpoints | roster rotation/cache/checkpoint tests |
| #717 | exact editions support historical backfill without overwriting peers | scope planner and partition-contract tests |
| #788 | stable player IDs are resolved across seasons without canonical fanout | historical xref resolver/DQ tests |
| #793 | repeated bounded runs resume toward full roster; coach history is parsed | scope-cycle/checkpoint and coach fixture tests |
| #797 | coach profiles/stints are rebuilt for exact editions | coach history and scope manifest tests |
| #800 | membership team name and ID come from the same season squad | scraper club identity regression test |
| #835 | market-value points deduplicate globally by `(player_id, mv_date)` | market-value Silver execution tests |
| #836 | current-scope xref health remains visible and conflict-safe | resolver suffix normalization and current-scope DQ tests |

Related active/merged work is treated separately: #708 remains an open
multi-source epic; #871 is fixed here by stable `tm_`/`fm_` orphan IDs; #851 is
the traffic regression baseline; #789/#795, #790, #803/#814 and #847 are
enforced by provider-metered manifests, fail-closed empty statuses,
scope-aware DQ and resumable/batched writes.

## Current career windows

Current refreshes of transfers and market-value history admit at most 500
players and stop before the next player once decoded response bodies reach
75% of the entity's existing decoded cap. Each admitted player's entire career
still passes semantic and completeness checks; the 90% success ratio includes
failed attempts within that admitted window. Hard decoded/provider/request
caps remain blocking, including an unexpectedly large single response.

The runner reports `career_window` counts and the stop reason. Its
`roster_coverage.selected` counts admitted players and `pending` includes the
deferred tail; the scope manifest hashes those coverage values. Only committed
keys receive successful checkpoints, so deferred careers retain their previous
age and precede newly refreshed careers in the next current run. Historical
and force refreshes retain their original window behavior and checkpoint
identity; current checkpoint identity pins `decoded-soft-stop-75-v1`.

Offline evidence: the BRA4/2025 synthetic byte replay, whole-player boundary and
hard-cap tests in `test_transfermarkt_traffic.py`, commit/checkpoint ordering and
next-run selection in `test_run_transfermarkt_scraper.py`, and scope coverage
evidence in `test_run_transfermarkt_scope_cycle.py`. Scheduled production ingest
after automatic delivery remains the runtime acceptance check.

## Current full-roster continuation

The current ingest `players` runner may reuse verified `squad` pages from a
previous child cycle for at most 48 hours from their original response time.
The next cycle fetches participant listings again and uses only squads named
by that listing. URL, scope, raw body/hash and the immutable attempt chain
must agree. Cache hits never extend response age. Existing 24-hour entries
can be reused within this physical age bound without rewriting their proofs.

Prior-cycle envelopes are reported as `cache_sources`, separately from this
cycle's `raw_attempts` and paid traffic. Rows retain the original `fetched_at`;
schemas and natural keys are unchanged. A budget failure still blocks writes
and successful completion; the saved pages let a later scheduled run finish
the full roster under the same decoded, provider and request caps. Historical,
force, other entities and default client behavior retain strict cycle binding.
Current ingest checkpoints pin `verified-squad-48h-v1`.

Offline evidence: `test_transfermarkt_squad_resume.py` replays BRC/2025's exact
16,933,131-byte / 105-request failure, then completes all 126 clubs with 101
cache hits and 27 new requests. It also checks lineage, expiry, scope/URL
binding, changed participants, full bio fields and multi-club memberships.
Runtime acceptance remains a complete scheduled roster capture and green
parent validation after automatic delivery; a cold attempt may remain failed
until its scheduled continuation succeeds.
