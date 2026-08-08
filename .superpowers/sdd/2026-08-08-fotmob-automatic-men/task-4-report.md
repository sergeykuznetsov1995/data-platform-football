# Task 4 report: automatic catalog contract and durable scope planning

## Outcome

Implemented the opt-in `fotmob-catalog-v1` Bronze runner profile. It derives its
competition set only from the Task 3 structural classifier, binds work to the
immutable catalog observation and exact canonical scope universe, persists the
latest scheduler outcome for every scope in the ingest manifest, and emits
evidence consumable by a separate versioned acceptance command.

The historical `fotmob-daily-v1` cohort, scope artifact, hashes, and loader are
unchanged. The only legacy acceptance edit adds the Task 3 profile-evidence
current view to `CURRENT_VIEW_KEYS`, keeping the shared view validation in sync
with `CURRENT_VIEW_SPECS` without changing issue-930 workload semantics.

## Implemented contract

- Added immutable `CatalogContract` and `build_catalog_contract()`.
- Canonical IDs are numerically sorted and newline-hashed.
- Exact `ID=source season` tokens are sorted by `(competition_id, season)` and
  newline-hashed without normalizing spaces or season spelling.
- The plan signature binds classifier version, parser version, normalized
  entity policy, included-ID hash, and exact-scope hash.
- The report also carries the exact allLeagues target batch ID and content hash.
- Deserialization recomputes every field, count, hash, and signature and rejects
  duplicates, noncanonical order, unknown fields, malformed types, or scopes
  outside included IDs.

## Durable planning and execution

- Added disjoint `current` and `history` lanes. Current means source-selected or
  latest; history means every other exact source season.
- Removed hardcoded mandatory-ID priority from automatic ordering. The legacy
  constant remains only for historical audit/legacy behavior.
- Added `ScopeAttemptState` and manifest-backed `scope_attempt` commits for
  `success`, `retryable`, `terminal`, `source_gap`, and `deferred`.
- Attempt evidence is keyed by exact scope plus plan signature and read across
  every manifest status, including failures. HTTP `TargetCommit.attempts` is not
  overloaded.
- Same-generation task retries do not increment the logical attempt count.
- A not-yet-due retry is skipped while later ready scopes remain eligible.
- Competition-root discovery uses durable oldest-first manifest timestamps, so
  a request-limited numeric prefix cannot starve later catalog IDs.
- Current success/source-gap evidence cools down for 48 hours, keeping refresh
  evidence inside the 72-hour acceptance ceiling without hot-looping one scope.
- Season limits, exhausted request/byte budgets, and cooperative deadlines write
  explicit deferred/retry evidence and return `partial_success` with exit 0.
- Parser/schema, target commit, flush, and current-view failures remain hard.
- A source-advertised finished match payload can close as `source_gap` only
  after two distinct successful run identities are retained in evidence.

## Runner, DAG, and acceptance

- Added `--catalog-contract fotmob-catalog-v1` and `--deadline`.
- The automatic path rejects explicit scopes, competition limits, replay, and
  every legacy daily-contract field before service construction. It never calls
  the issue-930 scope loader.
- Reports emit `scope_lane`, `catalog_ids`, full Task 3 `catalog_decisions`, the
  exact `catalog_contract`, planned scopes, and latest durable scope attempts.
- Added the `catalog_contract` and `deadline` parent-DAG handoff and automatic
  runtime validation. Soft, fully evidenced partial runs may rebuild Silver;
  hard schema/commit failures may not.
- Added `fotmob_catalog_trigger_conf()` without issue-930 fields. Existing
  schedule owners are not switched by this task; activation can use this
  versioned opt-in profile while legacy daily remains available.
- Added `scripts/fotmob_catalog_acceptance.py`. It independently recomputes the
  contract, checks one decision per catalog ID, requires structural male evidence
  for every included ID, rejects female/youth/reserve/show planning, checks
  current terminal freshness, enforces explicit retry/source-gap evidence, and
  enforces proxy zero.

## TDD and verification

Initial focused tests failed at collection because the contract module and
scope-attempt interfaces did not exist. Repository attempt tests then failed on
the missing durable methods. Implementation followed those failures.

Fresh verification on the final worktree:

```text
Task 4 focused suite: 212 passed in 0.98s
Legacy fotmob_acceptance suite: 40 passed in 1.28s
Ruff check: All checks passed
py_compile: passed
git diff --check: passed
```

No production deployment, production data mutation, issue-930 scope artifact
change, or proxy enablement was performed.

## Independent review

An independent task review found three execution/acceptance hazards. Before the
final verification, the implementation was tightened to rotate budget-limited
competition discovery from durable manifest timestamps, reject `terminal`
schema/commit outcomes in automatic acceptance, and require the prior attempt
to be a qualifying successful missing-match observation before `source_gap`
closure. The reviewer also flagged the one-line legacy view-key sync; that edit
was retained after explicit controller direction because it changes neither the
issue-930 cohort nor its artifact/hash semantics and restores the frozen suite.
