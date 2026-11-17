# FotMob #2256 completion audit

This directory contains the executable SELECT templates for
[`../audit_fotmob_completion.py`](../audit_fotmob_completion.py). The local CLI
uses only Python's standard library. Trino and source credentials are supplied
by the existing `airflow-scheduler` container. Implementation alone supplies no
current measurements, query-budget evidence, or replay certification.

## Authorized execution procedure

Run from an isolated checkout after publication and independent checks. Choose
a new, empty local run directory outside the report directory. Keep the run
directory: the report intentionally references, rather than commits, the full
per-ID evidence. The historical input is read without rewriting its bytes.

```bash
python3 -B scripts/research/audit_fotmob_completion.py plan \
  --registry docs/research/fotmob-scope-2026-11-14/scope-registry.csv \
  --historical docs/research/fotmob-completion-audit-2026-11-16/historical-1294-input.csv \
  --run-dir /absolute/local/task/2256-run --required-scopes 5540

python3 -B scripts/research/audit_fotmob_completion.py collect \
  --run-dir /absolute/local/task/2256-run --pass-id calibration --calibrate

python3 -B scripts/research/audit_fotmob_completion.py collect \
  --run-dir /absolute/local/task/2256-run --pass-id pass1

python3 -B scripts/research/audit_fotmob_completion.py collect \
  --run-dir /absolute/local/task/2256-run --pass-id pass2

python3 -B scripts/research/audit_fotmob_completion.py verify \
  --run-dir /absolute/local/task/2256-run --pass-a pass1 --pass-b pass2 \
  --report-dir /absolute/checkout/docs/research/fotmob-completion-audit-2026-11-16
```

`build --run-dir ... --pass-id pass1 --report-dir ...` can render a complete
single pass, explicitly labelled `replay_status=NOT_RUN`. Only `verify` certifies
two passes. Calibration is never accepted as either final pass. Reusing a pass
name, changing inputs/code/SQL, changing a snapshot or frozen chunk plan, and
using the same pass twice are errors. The optional `plan --bucket-count N`
sets the global manifest bucket count before freezing; the default is 16.

The planning expectation is 452 included competitions and 5,540 required
competition/season pairs. The CLI checks the required pair count; it does not
claim those expectations were observed. Required labels are split only on `|`.
Spaces and pre-2010 EURO/World Cup season labels remain exact strings. Registry
decisions `include`/`included` are included. Historical input must have 18 unique
keys and old gaps summing to 1,762. Its season key is accepted as
`source_season_key`, `season_key`, or `season`; gap aliases are enumerated in
`load_historical`. No year derivation or heuristic season matching is performed.

## SQL and transport contract

The SQL contracts address these base tables in `iceberg.bronze`:

| Work | Base table | Pin |
| --- | --- | --- |
| Calendar rows | `fotmob_matches` | `matches_snapshot` |
| Payload rows | `fotmob_match_payloads` | `payloads_snapshot` |
| Targets, markers, failure and scope-gap evidence | `fotmob_ingest_manifest` | `manifest_snapshot` |
| C1 input and checkpoint rows | `fotmob_c1_inputs` | `c1_snapshot` |

`snapshots.sql` reads the four `$history` metadata tables once, including
`iceberg.bronze."fotmob_ingest_manifest$history"`. All subsequent
base-table reads use `FOR VERSION AS OF` with positive integer snapshot IDs.
There is no fallback to latest snapshots if retention expires a pin. An expired
snapshot or incompatible SQL schema fails the run; it must not be represented
as an empty scope. SQL templates are executable after `.format` substitution
with validated integers and generated predicates. Their hashes are frozen too.

`league_manifest.sql` consumes `{manifest_scope_predicate}` with VARCHAR
competition IDs quoted by Python. Physical match and payload templates consume
`{scope_predicate}` with numeric competition IDs.

Physical metadata columns are `_parser_version`, `_observed_at`, `_ingested_at`,
and `_target_batch_id`. Calendar flags are `finished`, `cancelled`, `postponed`,
`awarded`; `utc_time` is compared to the calendar snapshot's `made_current_at`,
never the collector's wall clock. Payload fragments are `match_facts_json`,
`stats_json`, `player_stats_json`, `lineup_json`, `shotmap_json`, `momentum_json`;
the available mask is `available_sections_json` (a JSON array of section names
or an object of section booleans). Unsupported masks are unknown, not empty.
SQL schema compatibility and the exact marker/proof shape must be checked by
the lead against the mirrored repository before production collection; no
schema discovery or production query was performed during implementation.

Every query is a sequential invocation of:

```text
docker exec --workdir /tmp airflow-scheduler python -B -W ignore -c REMOTE_SCRIPT SQL RUN
```

The remote script uses `src_fotmob`, `TRINO_SRC_FOTMOB_PASSWORD`,
`TRINO_HOST` (default `trino`), `TRINO_PORT` (default `8443`), HTTPS BasicAuth,
`verify=False`, and `source=fotmob:2256:<run>`. It sets
`request_timeout=(3,30)`, `max_attempts=1`, `enable_dynamic_filtering=false`,
and `query_max_run_time=25s`. It consumes the cursor, obtains public
`cursor.query_id`, and closes cursor and connection. It creates no remote file.
Only structured error classes and query IDs are returned on failure, never
exception messages, environment variables, passwords or stderr.

The local process timeout is 35 seconds. A statement exceeding 25 seconds of
measured local wall time fails even if the server returned successfully. A pass
exceeding 3,600 seconds fails. SQL text is capped at 128 KiB UTF-8. The guard
rejects multiple statements, comments, write/administrative tokens,
`EXPLAIN ANALYZE`, current views and unversioned base-table scans. SQL literal
contents are tokenized separately, so an exact season containing punctuation
cannot inject a statement.

## Current-equivalent reconstruction

1. For each sorted scope chunk (initially at most 25), read eligible league
   candidates and bounded physical nonempty batch evidence for that chunk.
   Only exact parser versions `fotmob-native-v2` and `fotmob-native-v1` are
   relevant. V2 success/not_modified/not_available takes precedence over v1
   success/not_modified. Calendar and match winner ordering is exactly
   `CASE WHEN parser_version='fotmob-native-v2' THEN 1 ELSE 0 END DESC,
   COALESCE(TRY(CAST(from_iso8601_timestamp(json_extract_scalar(capabilities_json,'$.source_observed_at')) AS timestamp(6))), fetched_at, completed_at) DESC,
   completed_at DESC, batch_id DESC`.
   Invalid source timestamps fall back; malformed capability JSON is unknown.
   Empty successful candidates are skipped. A v2 tombstone selects no calendar.
2. Read rows only for selected successful calendar batches. Enforce the selected
   batch per scope again on the client, then deduplicate the scoped natural key
   by parser priority, physical observation/ingestion time and batch ID.
   Deleted rows from older calendar batches cannot leak back into current.
3. Read one global latest eligible match winner per entity in fixed CRC buckets.
   Trino's `crc32(varbinary)` already returns an unsigned nonnegative BIGINT.
   The bucket predicate is exactly
   `mod(crc32(to_utf8(entity_id)), {bucket_count}) = {bucket}`.
   Ranking is global per varchar entity ID, followed by explicit integer
   normalization. Latest-attempt status/time consider only the two exact parser
   versions above. The same query retains malformed eligible capability flags
   without fetching full successful histories.
4. Fetch payload rows in deterministic ID batches of at most 5,000.
   `current_payloads.sql` returns metadata, the available mask, validity and
   six typed/raw section booleans for marker/historical section scopes; it does
   not return bulk `payload_json`. `current_payload_ids.sql` returns only
   `competition_id`, `source_season_key`, `match_id`, `parser_version`,
   `_observed_at`, `_ingested_at` and `_target_batch_id` outside those scopes
   to reduce I/O. Both templates pin the payload snapshot and include exact
   scope partitions, IDs and selected global batch IDs, with deterministic
   ordering matching `current_matches.sql`. All known calendar IDs are covered,
   including unfinished rows; this preserves complete ID sets and section
   coverage where applicable. The finished subset supplies literal #1294 counts.
   A payload is current only if its scoped natural key is in the selected
   calendar and its batch is the global winner.
5. Fetch full manifest chronology only for absent scoped payloads and section
   uncertainties, including missing matchFacts requiring parser evidence.
   Every batch is bounded and becomes part of the frozen chunk plan.

Calibration may split failing scope, payload-ID or evidence-ID chunks only for
size/time failures. Failed attempts remain in its ledger. Global bucket count
does not adapt: choose another count in a new plan if calibration cannot meet
that budget. Generic SQL/schema errors and expired snapshots do not trigger
splitting. Final passes never split, repin or reuse partial output.

## Evidence and adjudication

Presence of a current scoped payload is positive physical evidence. A later
retryable/terminal failure is a separate flag and does not delete last-good
content. Absent content with latest terminal transport failure is `fetch_failed`;
schema drift or explicit parser/schema error codes are `parse_failed`.

Successful/not_modified global winners require `match_payloads=1` in expected
counts or positive physical selected-batch evidence to establish `writer_failed`
when scoped current content is missing. An empty `actual_counts` object can be
deterministic reuse and proves no failure. A raw URI alone is insufficient.

One `not_available/source_data_not_found` observation is unknown. Absence requires
two distinct run IDs at least 7,200 seconds apart, or three distinct runs, after
the latest success. Explicit scope-gap proofs require an enumerated ID set,
matching count, matching deterministic hash, exact scope, v2 not_available
manifest and completion time; later success supersedes a proof. The supported
proof object is `capabilities_json.scope_source_gap` with `competition_id`,
`source_season_key`, `match_ids`, `count`, `id_set_hash`. Unsupported proof shapes
are retained in `candidate-anomalies.json` as unresolved; they never silently
certify source absence. Evidence batch/run IDs and evidence hashes are retained.

Literal gaps include finished cancelled rows. Actionable gaps exclude explicit
cancelled/postponed/future cases. Awarded unfinished rows without content or
applicability evidence remain unknown.

Latest `fotmob-native-v2` marker grain is `target_key`, ordered by
`completed_at DESC, run_id DESC, batch_id DESC`, including an interrupted latest
row. `markers.sql` retains all evidence columns. `source_gaps.sql` selects actual
pinned v2 `scope_source_gap` rows with status `not_available`.
Family lists in `capabilities_json.coverage.scope_entities` use the marker's own
scope; a dictionary `matches` list can enumerate explicit scoped entities.
Missing matches family means not applicable; malformed evidence means unknown.
Even malformed markers retain a usable physical scope in the work plan.
Interrupted marker status is unknown. Otherwise the primary category is
`writer_failed > parse_failed > fetch_failed > source_unavailable > not_applicable
> confirmed_content`; any unknown overrides all of them. All underlying reason
counts remain available, including section reasons.

For each current payload in marker/historical scopes, compare six raw booleans,
six typed booleans and the available mask. JSON `null`, `{}` and `[]` are absent;
false, zero and the empty string are present. Use `content` as the raw root only
when it is a JSON object. No h2h check is included. Raw present/typed absent is
writer failure; the reverse or unresolved JSON is unknown. Omitted optional
sections are observation-scoped source unavailable (`stored_source_omission`).
Finished matchFacts omission is parse failure only with schema-drift evidence;
otherwise a valid stored raw omission is source unavailable.

The full payload template retains doubled braces (`'{{}}'`) for literal empty
JSON objects so Python `.format` produces the SQL literal `'{}'`.

C1 is a separate limited cohort; `c1_inputs.sql` selects actual rows at its
pinned snapshot. `first_ingested`/`first_ingested_at` establishes
ever-ingested evidence at its own snapshot, not current content. Control rows
are excluded. The reconciliation preserves exact C1 no-payload IDs, current
additions, ever-ingested-but-not-current IDs, and IDs outside the audit calendar.
No equality across different snapshots or whole-history coverage is asserted.

## Artifacts and validation

The run directory contains sealed input/code/SQL hashes, exact snapshot pins,
initial query ledger, markers, source gaps, C1 rows, scope cohorts and chunks.
Each pass has a durable query ledger, completion status, timing and content hashes.
Failed passes retain status and query evidence and cannot build a report.

| Artifact | Meaning |
| --- | --- |
| `current-id-evidence.csv` | Every current scoped calendar row, including matching IDs; full local evidence |
| `mismatched-ids.csv` | Literal missing payloads, all nonconfirmed rows and late failures |
| `section-mismatches.csv` | All six comparisons, including agreements, to prove coverage |
| `marker-results.csv` | Every latest target, primary category, complete reason counts |
| `scope-summary.csv` | One row per physical scope with independent cohort flags and ID hashes |
| `historical-1294-results.csv` | Exact 18 old keys/gaps plus current snapshot recheck |
| `c1-reconciliation.json` | Limited cohort counts, exact correction sets, both snapshot identities |
| `candidate-anomalies.json` | Empty/malformed calendar candidates and unresolved scope-gap proofs |
| `query-ledger.csv` | Step/chunk/bucket, query ID, SQL SHA256, row count, wall time and outcome |
| `content-hashes.json` | SHA256 for every deterministic content artifact |

CSV field order is fixed, line endings are LF, JSON keys are sorted with stable
indentation. Scoped IDs are JSON arrays `[competition_id,exact_season,match_id]`.
Set hashes are SHA256 of UTF-8 NUL-joined lexicographically sorted unique
canonical IDs; empty sets hash empty bytes. Global hashes use decimal ID strings.

Validation requires every required/union scope, latest marker and historical row
exactly once; scoped arithmetic and full ID hashes must reconcile. All applicable
payloads must have six section comparisons. Bucket/chunk/query coverage, query
success and both time budgets are checked. Data-driven unknowns are counted and
excluded from content confirmation; they do not masquerade as missing queries.
`verify` compares bytes of every deterministic content artifact, ignoring only
query IDs, wall times and pass labels. It writes per-file hashes and exits nonzero
on mismatch, invalid coverage or unfinished passes.

Tests are synthetic and use an injected `execute(sql)` transport. The lead runs
the targeted and required full unit suites after publication. This implementation
session did not execute tests or production reads.
