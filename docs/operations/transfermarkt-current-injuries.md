# Transfermarkt current injury observations (#2284)

## Scope

Offline structured parser for saved `current_injury` HTML captures. It reuses the existing current lane's endpoint and detector contract but does not change that lane, affected-club refresh, DAG, schedule, writer, grants or production state. It does not complete #2284 or unblock production #2285 by itself.

## Parser contract

`parse_current_injuries(html, *, competition_id, edition_id)` reads one `table.items` with explicit Player, Club, Injury, Since and Expected return columns. Every row must reconcile: canonical player/club IDs, nonempty injury text, optional ISO since/expected return and competition/edition context. One malformed row fails the whole capture.

Multiple injuries for one player remain distinct. Identical duplicate episodes are counted; conflicting duplicate `(competition,edition,player,injury)` identities fail. Replay is deterministic through `canonical_json()`.

`observed_present` is the only direct HTML success with rows. A missing table, changed headers, malformed no-entries row or empty tbody is not empty evidence. `confirmed_empty_current_injuries(...)` requires a nonempty external completeness proof and stores it in the result; no production empty layout is invented by this package. `not_applicable_current_injuries(...)` likewise requires explicit participant/registry applicability proof.

Parser episode columns are `CURRENT_INJURY_COLUMNS`. The future physical observation key adds `observed_at`; capture IDs, source hashes, raw/capture clocks, universe and run/batch lineage are attached by the capture lane.

## Tests

Synthetic structured fixtures cover all fields, multiple injuries per player, unknown dates, replay, duplicate reconciliation, missing/changed tables and headers, unproven empty layouts, malformed identities/reasons/dates, explicit empty and national not-applicable proofs.

## Remaining work

Real current injury captures including a proven empty layout, integration with the existing detector without second HTTP fetch, versioned national applicability proof, raw lineage, safe writer/receipts/readback, affected-club refresh evidence, edition universe, separately authorized bounded E2E and three scheduled daily captures plus three DQ days.
