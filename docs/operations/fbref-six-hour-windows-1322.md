# FBref current refresh every six hours (#1322, step B)

Step A raised the per-run cap to 20 batches and passed production acceptance
on 2026-10-01. Step B adds three smaller refreshes per day. Development,
merge, deployment, and live acceptance are separate: this change does not
close #1322 or authorize delivery, history restart, or bootstrap activation.

## Scheduled profiles

| Window (UTC) | Maximum batches | Maximum targets at shard 25 | Live-loop budget |
| --- | ---: | ---: | ---: |
| 00:00 | 9 | 225 | 3 hours |
| 06:00 | 20 | 500 | 4.5 hours |
| 12:00 | 9 | 225 | 3 hours |
| 18:00 | 9 | 225 | 3 hours |

The cron is `0 0,6,12,18 * * *`. Select the profile using the UTC hour of
`data_interval_end`: Airflow's scheduled run ID/logical date labels the
**start** of the preceding interval, not the window being executed. A delayed
06:00 run retains the large profile even if it starts after noon. UTC windows
do not move with Berlin's daylight-saving changes.

Explicit DagRun `conf` overrides remain available. `max_batches` selects
1–80 batches; `wave_deadline_seconds` selects the loop budget, with zero
explicitly disabling it. Automatic selection must not be replaced by a
fixed UI parameter default. A missing interval uses the small profile.
Overrides can exceed the reserved window and require operational judgment;
the scheduled profiles describe default runs.

Production safety limits remain 4096 requests / 2048 MiB, with shard size 25.
The existing explicit 100 / 50 canary profile remains non-publishing.
Small scheduled runs use the production profile and the normal publication
gates. No changes to frontier priority, parser, raw storage, or SQL pins are
needed. The separate manual bootstrap stays paused, with 20 batches and its
previous 5.5-hour loop budget.

## Why the large budget is 4.5 hours

The owner approved reducing the proposed 5.5-hour large-window budget to
4.5 hours on 2026-10-01 after inspecting step A's actual task timings:

- Ingest started just after 06:00 UTC and finished at 12:00:23.747 UTC.
- Live waves ran 06:03:33–11:47:43; the last batch alone took 64m14s.
- Scope reconciliation after that batch took about 4m06s; remaining DAG
  tasks took another 12m41s. Setup before live waves took 3m33s.
- A five-hour loop budget would still have allowed that last batch to start.
  A 4.5-hour budget would have stopped this observed run after batch 18.

The deadline is checked **between** batches. It is neither a hard task
completion limit nor the total DAG budget. A 90-minute reservation allowance
covers the observed batch completion, reconciliation, setup, and downstream
tasks (84m34s rounded up). It is an estimate, not a worst-case guarantee.
`max_active_runs=1` serializes ingest; a slow run can still delay its successor.
The cap is a maximum, not a promise of 500 pages in the large run. Speed
acceptance (400 pages within three hours) is tracked separately in #1606.

## History guard and publication

The DAG history guard protects all four default reservations, including an
already active interval and the previous day's interval at date boundaries.
The next-window test keeps its existing 45-minute margin and projected
history duration. Shared profile definitions prevent the scheduler and guard
from drifting apart.

The existing history duration projection does not account for parsing and is
not an accurate hard ceiling. Correcting that model, queueing on the lock,
covering all writes with the pool, and replacing the legacy controller belong
to #1328. `/root/fbref_history_backfill/driver.sh` still knows only 06:00 UTC;
it remains stopped and is not changed or restarted by step B. No absence of
collisions during paused history is evidence that concurrent history is safe.

Existing freshness/run validation accepts partial progress while still
checking run health and scope evidence. Export remains immutable and keyed
by control-run UUID. Silver is triggered after lock release with that UUID,
without waiting. Four triggers per day do not introduce a daily identifier
collision, but green Bronze does not prove green Silver. Before this change,
the five latest Silver runs were already failed (25–32 minutes each, observed
2026-10-01). Record Silver outcomes separately; fixing them is outside B.

The #1327 watchdog's two consecutive red runs can now be adjacent windows
six hours apart, rather than daily runs 24 hours apart.

## Acceptance after authorized deployment

Keep #1322 open until the issue's live criteria are measured over three days:

1. For active-season schedules, median time between successful fetches is
   at most 6.5 hours. Use fetch timestamps, not DAG start intervals. Report
   sample count and schedules with no/repeatedly missed fetches, so missing
   schedules cannot disappear from the result.
2. On a peak day, every played match is fetched within 24 hours. Use the
   #1323 measurement if ready, otherwise a direct read-only SQL measurement
   with the same eligible men's current-season scope and a fixed cutoff.
3. No `StateConflict` publication-lock collisions. State explicitly whether
   history was paused during observation; restart requires its own approval
   and #1328 prerequisites.

For the 12 scheduled windows, record planned and actual starts/ends, selected
profile, progress/batch count, `deadline_reached`, scope debt, errors, lock
release, immutable publication generation and Silver trigger/run outcome.
Check delay of the noon window after the large run and any Silver queue
growth. A partial run may be healthy without meeting freshness acceptance.
Do not trigger, clear, or unpause live DAGs to manufacture acceptance.

The separate delivery script must copy these three runtime modules together:
`dags/utils/fbref_current_windows.py`, `dags/utils/fbref_current_dag_factory.py`,
and `dags/utils/fbref_pipeline_tasks.py`. The new shared module is required
by both existing modules. Compare the reviewed diff and keep rollback copies
as one matching set. Production edits remain forbidden during development.
