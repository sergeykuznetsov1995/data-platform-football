# Transfermarkt club profile and stadium observations

The writer consumes at most two captured tmapi responses per club. The profile comes from `GET https://tmapi.transfermarkt.technology/club/{club_id}`, and the stadium comes from the same URL with `/stadium` appended. Both URLs must match exactly, including the club ID. The parser and writer work offline from those captures. HTML traversal, crest downloads, address lookups and additional country requests are outside the contract.

The donor field mapping derives from Felipe Allegretti’s MIT-licensed Transfermarkt work. Copyright (c) 2023 Felipe Allegretti. The offline parser defines the stadium fields and accepted payloads. [Offline parser and full MIT notice](transfermarkt-club-stadiums.md#provenance-and-mit-license) · [Contract tests](../../tests/unit/scrapers/test_transfermarkt_club_stadiums.py)

## Profile fields

Both parsers take the complete decoded response envelope. A profile requires literal `success: true`, an object in `data`, a reconciled `data.id` and a nonblank `data.name`. Missing optional fields yield SQL NULL. A present field of the wrong type fails the observation.

| Field under `data` | Profile column | Rule |
| --- | --- | --- |
| `id` | `club_id` | Reconcile with the requested club |
| `relativeUrl`, `name` | `url`, `name` | Preserve the source values |
| `baseDetails.shortName`, `baseDetails.abbreviation` | `short_name`, `abbreviation` | Optional strings |
| `preferences.clubCode` | `club_code` | Optional string |
| `baseDetails.isNationalTeam` | `is_national_team` | Preserve a supplied boolean, including false |
| `baseDetails.superiorClub.name` | `official_name` | NULL for a national team |
| `crestUrl` | `image` | Store the supplied URL |
| `baseDetails.superiorClub.location.street` | `address_line_1` | Trim surrounding whitespace |
| `baseDetails.superiorClub.location.postcode`, `city` | `address_line_2` | Trim both parts and join nonempty parts with one space |
| `baseDetails.superiorClub.location.countryId` | `country_id`, `address_line_3` | Keep the ID and use the supplied static country mapping for the name. An unmapped ID yields a NULL name |
| `baseDetails.superiorClub.colors` | `colors` | Keep nonempty color strings in mapping order, skipping false and empty values. The frozen observation holds a tuple and Bronze stores its JSON array |

## Raw evidence and observation grain

The profile requires an OK outcome and raw HTTP 200. A present stadium has the same requirements. For each endpoint the writer reloads the durable capture and checks its capture ID, body SHA-256, URL, scope, cycle, status code and fetch time. It recomputes the digest from the actual body bytes. For HTTP 200 it decodes UTF-8 JSON and compares the complete value with the outcome before extracting fields.

Only a durable stadium HTTP 404 paired with a successfully parsed profile yields `observed_empty`. The receipt stores the typed proof containing both capture IDs and both HTTP status codes, alongside their hashes and clocks. This plan writes one profile and expects zero stadium and relation rows. Other HTTP errors, absent captures, mismatching manifests and malformed success bodies fail the write.

| Table | Natural key in merge order | Grain |
| --- | --- | --- |
| `iceberg.bronze.transfermarkt_club_profile_observations` | `club_id, observed_at` | One profile per club and profile capture time |
| `iceberg.bronze.transfermarkt_stadium_observations` | `stadium_observation_key, club_id, observed_at` | One observed venue for the club at stadium capture time |
| `iceberg.bronze.transfermarkt_club_stadium_relations` | `club_id, stadium_observation_key, observed_at` | One club-to-venue observation at the same stadium capture time |
| `iceberg.ops.transfermarkt_club_stadium_receipts_v1` | `receipt_id` | One verified paired capture plan |

A source stadium ID yields `stadium:<source-id>`. For a present stadium without an ID the key is `club:<club-id>:unidentified` and `stadium_id` is NULL. Two clubs using the same stadium retain separate observations even at the same instant. Relation `valid_from` and `valid_to` are NULL because the endpoint supplies an observation time only.

Each business row carries the source URL, raw capture ID and body hash, plus both endpoints’ capture IDs, hashes and clocks. The source capture time supplies `captured_at` and `observed_at`. The later of the two clocks supplies `extracted_at` and `_ingested_at`. SQL clocks use UTC `timestamp(6)`. Club and stadium identifiers, text and canonical JSON use `varchar`, integer measures use `bigint`, coordinates use `double` and flags use `boolean`.

The business rows also retain parser and schema revisions, cycle, scope ID and scope JSON, source competition and edition IDs, run ID, universe version, `_source`, `_entity_type` and `_batch_id`. The batch digest binds the parser and schema revisions, club, and both raw IDs and hashes. Only the receipt’s `committed_at` uses the writer’s wall clock. [Ordered table schemas and writer](../../dags/utils/transfermarkt_club_stadiums.py)

## Write and recovery

The writer holds the Transfermarkt writer lock and observes the current writer deadline through authorization, raw checks, table creation, merges, physical reads and receipt verification. The current preflight and standing policy must authorize the operation, including all four tables. The writer reloads authorization before each merge, including the receipt merge.

Before writing, structural checks require exactly one profile and either one matching stadium and relation or a proven empty pair. Every key and required lineage field must be populated. Present rows must agree on club, stadium identity, observation clock and status. Each endpoint clock is checked against that club’s history, including partial writes and previously confirmed absences. An older capture or a different body hash at the same endpoint clock fails the plan.

Native merges append observations at new keys. A retry can reuse an identical row at an occupied key. A different batch or payload occupying the same key fails the write so earlier receipts continue to identify their original rows. No DELETE is part of this operation.

For each business table the writer reads all planned columns back by batch and verifies each populated natural key. Empty plans require an explicit zero-row read for both stadium and relation. The receipt binds the key maps, profile/stadium/relation counts, present/empty counts, three physical hashes, plan SHA-256, batch and lineage. The writer merges the receipt last and compares every stored receipt column before returning `verified=True`.

A failed merge or readback yields no acknowledgement. Retry the same immutable inputs after an interruption. The writer verifies any rows already present, fills missing rows and issues the receipt after physical verification. If a receipt already exists, it rechecks the plan, all three physical results and every receipt column. A mismatch leaves the evidence available for investigation.

## Monthly capture and DQ integration

The current runner keeps an additive 30-day cursor and a durable per-scope `stadium_sweep`. It uses the already verified `listing_clubs`/`snapshot` universe and the static `dags/configs/transfermarkt/countries.tsv` mapping. Each club is fetched through the raw-first client at the two tmapi URLs above. The raw store admits only canonical positive-ID profile/stadium paths without query parameters.

A fresh monthly cursor performs no club/stadium HTTP or writes. Partial sweeps resume only pending clubs; per-club parse/write/receipt failures remain deferred, while deadline, lease and traffic-budget failures stop before the next club. Season close preserves unfinished sweep evidence. A completed sweep stores its expected club map and conclusive present/empty receipt summary. `observed_empty` is conclusive only for the paired raw 404 proof.

`dags/scripts/transfermarkt_club_stadium_dq.py` reads current journals and physical tables without production writes. It checks duplicate natural keys, NULL lineage, orphan relations and receipt/physical counts, then reports fresh present/empty coverage and unresolved shares by scope. Unattempted or incompletely listed registry scopes make enumeration incomplete and block PASS even when every written club is healthy. Green requires complete enumeration, at least 95% fresh conclusive clubs, no more than 5% unresolved per scope and zero physical/reconciliation errors.

The committed standing policy still has no club/stadium targets and no DQ schedule is enabled by this package. Delivery therefore does not authorize a write. A manually bounded E2E on the accepted Git SHA must first produce real receipts, physical readbacks and a DQ report. Standing grants and scheduled DQ are a separate activation change. Full monthly acceptance still requires 100% enumeration, source-target reconciliation, one complete scheduled monthly capture, the following scheduled no-op and three green DQ days.
