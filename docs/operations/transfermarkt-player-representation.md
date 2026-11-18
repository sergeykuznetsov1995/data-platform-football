# Transfermarkt player representation observations (#2282)

## Scope

Offline extraction from immutable current tmapi `/players` captures. This package does not fetch source pages, change the current lane, create a DAG/pipeline, grant tables or write production. Live three-scope canary and scheduled acceptance require a separate owner assignment after the current and 3+3 windows.

## Observation and natural key

The global grain is player × `relation_type` × representative/status × source capture. In schema `representation-v1` / `1`, `relation_type=consultant_agency`; this is not a person-agent entity or T3 agency profile.

Physical natural key: `(player_id, relation_type, representative_key, source_capture_id)`. All key fields are non-NULL. `representative_key=agency:<canonical-positive-id>` for assigned and `status:<observation_status>` otherwise. `representative_id` is a canonical decimal VARCHAR only for assigned and NULL otherwise. This mapping implements the issue's `representative_id/status` component while satisfying the existing writer's non-NULL merge-key requirement.

For real packets `source_capture_id=raw_capture_id`. Missing packets use `missing:<sha256(assessment_manifest_id|player_id|relation_type)>`; no raw ID/hash/source time is invented. `assessment_manifest_id` hashes the canonical sorted expected global-player/universe inventory and selected capture IDs, excluding extraction clocks, run IDs and storage locations.

## Statuses and source evidence

- `assigned`: positive JSON integer or ASCII digit-string agency ID, normalized losslessly; status_reason=`positive_id`.
- `unknown`: integer/string zero, null or missing field, with reasons `zero`, `null`, `missing_field`. It is not explicit none.
- `explicit_none`: reserved for typed proof from a separately saved player-profile capture with its own lineage. `/players` never emits it.
- `missing_packet`: the completed capture assessment proves that no selected packet was expected to contain the expected global player. It is unresolved and captureless.
- `error`: invalid agency value/type or capture-associated packet/schema/reconciliation failure. Known corrupt or unavailable captures remain assessment errors; no captureless error row is fabricated.

Bool, floats (including 1.0), negative/signed/whitespace/Unicode-digit/nonnumeric strings, containers and other types are errors. There is no arbitrary fixed-width maximum source ID.

Multiple independently evidenced representatives have separate agency keys and never overwrite. The scalar field alone does not prove independent support. Club assignments, roster scopes and memberships never multiply the global observation. Validate the entire `(player_id, relation_type, source_capture_id)` group for contradictions in addition to natural-key uniqueness.

## Time, lineage and replay

Packet rows preserve `source_url`, `raw_capture_id`, `source_body_hash`, `source_endpoint`, `scope_id`, raw `cycle_id`, `captured_at`, `observed_at`, parser/schema, assessment/universe/run identity, evidence type/locator and `_source/_entity_type/_batch_id/_ingested_at`. `captured_at=observed_at=raw.fetched_at`; extraction `run_id` and `extracted_at` are separate. Raw lineage and both source clocks are required for every capture-associated row, including errors. Only missing_packet is captureless and has NULL raw lineage/clocks.

Identical immutable inputs and extraction clock produce identical plan bytes. A later replay may change only assessment/universe/run/extracted/batch/ingested association metadata; source business facts and clocks cannot change. An older extraction cannot overwrite a newer association. Replay never advances freshness. Immutable plans and receipts, rather than current table tags alone, establish assessment membership.

## DQ contract

The expected denominator `E` is distinct global players in the frozen universe, independently of extracted rows or memberships. Expected groups `G` come from selected actual request URLs plus assessment-bound missing expectations. Report observation rows, distinct players and player/capture groups separately.

- Packet coverage: expected players with fully reconciled selected packet / `E`; also report group completeness.
- Verified-write coverage: expected players whose complete planned groups have physical receipts / `E`.
- Conclusive coverage: expected players whose complete selected groups are assigned or source-proven explicit_none / `E`.
- Fresh/conclusive coverage: conclusive players whose required source clocks pass the explicitly supplied evaluation time/policy / `E`. Without those inputs, freshness is `not_evaluated`.
- Unresolved counts/reasons at player and group level. Unknown/error/missing remain unresolved even when physically written.

Successful captures require 100% actual request-URL ID reconciliation, no unexpected/missing/duplicate source identities, zero conflicting groups and zero NULL captured lineage. Multiple representatives never inflate player coverage. Empty denominators produce zero counts and undefined rates, not automatic success. Production ≥95% fresh conclusive and ≤5% unresolved per significant segment, 100% source-target reconciliation and three scheduled + three DQ days remain future acceptance gates.

The committed real fixture set has 11 captures, 1032 unique players, 55 clubs and 25 multi-club rows (947 positive and 85 zero values). It proves parser breadth and replay only; it is not a complete club roster universe or production SLO measurement.

## Writer receipt, readback and recovery

The isolated future targets are `iceberg.bronze.transfermarkt_player_representation_observations` and `iceberg.ops.transfermarkt_player_representation_receipts_v1`. The default policy is not changed and therefore fails closed. Tests use temporary policy and in-memory DBAPI only.

One writer holds the Transfermarkt writer lock across existing-group conflict checks, native merge, full physical readback and receipt write/readback. There is no DELETE. Physical verification compares complete ordered keys and every business/lineage column, including NULLs, not only COUNT(*).

The stable plan/receipt binds target, complete natural-key set, expected players/groups, row/status counts, raw capture/body identities, assessment/run/universe, parser/schema, full physical hash, committed batch and committed_at. `verified=true` proves only that physical target/plan verification, not source conclusive coverage or freshness.

A matching receipt is reused only after its data passes verification. If business commit succeeded but receipt was absent/failed, the same immutable plan is reconciled and the receipt is safely completed. Missing/extra/duplicate/corrupt rows, contradictory groups, stale plans, missing raw/run/universe evidence and uncertain newer commit state fail without blind overwrite or DELETE. Live execution is outside this package.
