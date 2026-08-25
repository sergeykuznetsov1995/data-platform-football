# FBref current throughput: 2026-08-25

## Root cause

The 25-page current runner had `CURRENT_MAX_BATCHES=80` and a 6 h 05 min
task timeout.  Live batches 1--6 had 288.7--299.8 second post-parse gaps.
That gap is not attributable to `reconcile_frontier_scope()` alone: the next
fetch cycle also performs unchanged full `_FRONTIER_SCOPE_CTE` evaluations for
the empty claim, due-cohort creation, and claim path.  It is evidence that
the full current-wave cadence cannot safely support 80 waves, not a timing
measurement for any one SQL statement.

`parse_wave()` currently defers its per-page reconciles only until the end of
that one wave.  Each `reconcile_frontier_scope()` call runs two full
`_FRONTIER_SCOPE_CTE`-backed `UPDATE` classifications (reopen and quarantine),
so repeating those two state-repair scans in every current wave is unnecessary
work.

## Design

For `run_type="current"`, `run_live_waves()` will defer those existing
per-page/per-wave scope-reconcile requests across the entire live-run loop and
perform exactly one full reconciliation in its `finally` path when any request
was made.  The final sweep still reopens only `ScopeQuarantined` targets and
still applies the unchanged full `_FRONTIER_SCOPE_CTE` classification.

This is safe because `claim_targets()` independently evaluates the same scope
CTE under its frontier lock before every network lease.  A newly seeded
ineligible target therefore cannot be fetched while repair is deferred.
Eligible newly queued targets remain claimable in the next wave; an already
quarantined target whose registry scope became eligible can wait until the
final repair (and, if needed, the next scheduled current run).  Historical
runs retain their existing per-wave reconciliation cadence.

If the final repair reopens a previously quarantined target after the last
zero-claim wave, the runner clears `frontier_closed`.  This prevents the
downstream validator from treating that reopened work as a false terminal
closure.

The current DAG cap returns to the prior 16 waves / 400 pages.  The
conservative observed full-wave cadence is about 20.35 minutes, giving about
5 h 26 min for 16 waves and about 39 minutes before the 6 h 05 min task
timeout.  The trade-off is bounded daily progress instead of attempting 2,000
pages in one task; the task returns normally and later scheduled runs continue
from durable frontier state.  Deferring the two reconciliation `UPDATE` scans
can improve throughput, but the exact speedup remains unproven until a live
canary measures the unchanged claim/cohort/claim scope work as well.

## Regression coverage

The tests require one final current-lane reconciliation after successful and
failed multi-wave loops, preserve per-wave reconciliation for history, and
pin the 16-wave DAG cap and 6 h 05 min timeout.
