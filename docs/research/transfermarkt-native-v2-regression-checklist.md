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
