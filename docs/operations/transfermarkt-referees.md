# Transfermarkt referee profiles and season aggregates (#2292)

## Scope

Independently written parsers for saved referee directory, profile and competition-history HTML, plus raw-first capture, native writer receipts and paused-by-default current/history DAGs. Parser modules have no HTTP client. Delivery, scheduled captures and production acceptance require the bounded E2E and evidence below; this implementation alone does not complete #2292.

The source-contract inputs are the independent directory/profile/history page shapes below; they have not been live-validated by this package:

```text
/{league}/schiedsrichter/{wettbewerb|pokalwettbewerb}/{competition_id}/saison_id/{edition_id}/plus/1[/page/{n}]
/-/profil/schiedsrichter/{referee_id}
/-/profil/schiedsrichter/{referee_id}/plus/0?funktion=1&saison_id=&wettbewerb_id={competition_id}
```

Match appointments, match events, match IDs, match-page discovery and reconstruction of matches from aggregates are forbidden. `matches_officiated` is a published season count, not a match-level row.

## Offline interfaces

```python
parse_referee_profile(html, *, referee_id) -> RefereeProfile
parse_referee_directory_page(html, *, competition_id, edition_id) -> RefereeDirectoryResult
reconcile_referee_directory_pages(
    pages, *, competition_id, edition_id, expected_page_count,
) -> RefereeDirectoryResult
parse_referee_season_aggregates(html, *, referee_id, competition_id) -> RefereeAggregateResult
confirmed_empty_referee_directory(
    *, competition_id, edition_id, completeness_proof,
) -> RefereeDirectoryResult
confirmed_empty_referee_season_aggregates(
    *, referee_id, competition_id, completeness_proof,
) -> RefereeAggregateResult
```

Parser/schema revisions are `referees-v2` / `1`. Referee IDs are canonical positive decimal strings; competition IDs are uppercase; edition IDs are the season start year. `24/25` becomes `2024`, `1999/00` becomes `1999`, and calendar-year `2026` remains `2026`. Two-part seasons must name consecutive years. Every history row's visible competition link must match the requested competition.

## Grain, fields and metrics

Profile fields are `referee_id`, `name`, optional `nationality` and `birth_date`. A directory entry additionally has optional `debut_date`. Missing optional data never discards a referee.

Each aggregate is a long metric row with `referee_id`, `competition_id`, `edition_id`, `metric`, Decimal `value` or NULL, `observation_status`, `status_reason`, `raw_value`, `source_row` and `source_column`. The future physical key adds `observed_at`:

```text
(referee_id, competition_id, edition_id, metric, observed_at)
```

Published count metrics are `matches_officiated`, `yellow_cards`, `second_yellow_cards`, `direct_red_cards` and `penalties_awarded`. Detailed tables can additionally publish `average_yellow_cards`, `average_second_yellow_cards`, `average_direct_red_cards` and `average_penalties_awarded`. Compact tables never receive parser-calculated averages.

`as_dict()` renders Decimal values as canonical strings and missing values as JSON null. Result ordering and `canonical_json()` are deterministic.

## Observation semantics and fail-closed reconciliation

- Numeric values, including explicit zero, are `observed_present` / `numeric`.
- `-` is `explicit_none` / `dash`; an empty cell is `observed_empty` / `empty_cell`.
- Malformed, negative or fractional count values fail the capture; they are not converted to zero or a conclusive observation.
- `unknown`, `not_applicable`, `unavailable` and `error` require capture-lane evidence and are not inferred from malformed HTML.
- Missing/ambiguous tables, empty bodies, changed detailed headers, invalid identities, competition conflicts and invalid seasons fail closed.
- Identical duplicate referee rows or metric rows are reconciled and counted; conflicting duplicates fail.
- Directory reconciliation requires exactly `expected_page_count` supplied pages. Zero, missing or excess pages fail with `directory_pagination_incomplete`; the parser never claims complete discovery from a stopped crawl.
- `observed_empty` results require a separate nonempty completeness proof. A missing table or empty `tbody` is not such proof.

The committed fixtures are synthetic compact/detailed directory, profile and history examples. They test parser behavior only and prove neither live source coverage nor production freshness.

## Capture lineage and production boundary

The capture lane binds `observed_at`, source URL/capture/hash, captured/extracted clocks, parser/schema/scope/run/universe/batch and storage metadata to every physical row. Fixture content never supplies real lineage. The native writer uses only the two referee Bronze tables and `transfermarkt_referee_receipts_v1`, with immutable plans, physical readback, idempotent replay and receipt recovery.

`dag_ingest_transfermarkt_referees` runs weekly directory aggregates and due monthly profiles; `dag_backfill_transfermarkt_referees` separately resumes historical directory editions and emits history coverage. Both are paused upon creation. No match route, appointment, event or match discovery is used.

Remaining evidence: authorized real captures and source-layout validation, versioned full-universe coverage, bounded E2E with receipt + physical readback + DQ on one Git SHA, scheduled continuation and next-run evidence, completed history report and three green DQ days. Merge, delivery and acceptance are separate stages.
