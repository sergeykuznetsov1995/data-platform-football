# FBref durable history controller (#1328)

Implementation, delivery, launch and seven-day acceptance are separate steps.
This release does not launch history or satisfy the live milestone. Current
refresh remains at 00/06/12/18 UTC. Silver and host cron are unchanged.

## Ownership and progress

`dag_fbref_history_controller` is the only live history controller. It is a new
DAG identity, initially paused, and requires the deployment switch
`FBREF_HISTORY_CONTROLLER_ENABLED=1`. The default is `0`. DagRun conf cannot
set this switch. The old `dag_backfill_fbref` remains a manual dry-run entry;
its live initialization and runner are rejected. Do not restart the stopped
`/root/fbref_history_backfill/driver.sh` alongside the controller.

The schedule is `30 4,16,22 * * *` UTC: three slices at 04:30, 16:30 and
22:30 UTC (07:30, 19:30 and 01:30 МСК), each starting where a current
reservation ends (#1958, owner decision 29.10.2026). Both scheduled and manual
history admission must occur inside one of 04:30–05:00, 16:30–17:00 or
22:30–23:00 UTC. Delayed/queued/restarted runs recheck the window before lock
acquisition and before any live runner starts. All four current reservations,
including the prior day's midnight tail, remain protected: each slice must end
by 05:15, 17:15 or 23:15 UTC (next current window minus 45 minutes). The single
policy place is `dags/utils/fbref_current_windows.py`.

Migration 12 adds `history_campaign_season`, `history_timing` and
`publication_waiter`. Campaign `adult-men-2026-v1` uses the complete adult men's
registry (112 in the accepted source inventory), excluding Big5 aggregates,
youth, women and reserves. This history inventory is distinct from the active
current denominator; source-proven discontinued tournaments retain history
eligibility without reopening their current schedules.

Seasons advance by start year: 2026/27 through 2017/18 across every adult
competition, followed by deeper source-discovered editions. Calendar-year
2026 belongs to the 2026 wave. One run pins one season in `crawl_run.metadata`.
A missing edition stays `missing` and blocks progression into an older year.
The controller relies on ordinary current discovery to fill these gaps. It
does not infer that a missing registry row proves absence at FBref. Source
catalog first/last-season bounds can establish external `unavailable` ranges.
A completed authoritative competition snapshot also proves interior gap years
for periodic tournaments (for example World Cup 2026/2022/2018). The proof joins
immutable raw, exact successful generic/typed/stateful processing and the
snapshot edition set, including the latest advertised edition; its ID is
stored as `catalog_snapshot_id`. An advertised edition missing or unhealthy in
registry remains debt. Calendar type and bounds alone cannot prove these
interior gaps. Direct-match-only editions seed and validate a match root
instead of inventing a season HTML page. Current seasons
stay `current_owned` and are handled by current refresh. A source-proven discontinued
tournament's last edition belongs to history even if the old registry still
marks that edition `is_current=true`. Superseded duplicate rows fold into their
canonical edition only when a completed catalog snapshot proves the alias;
unknown aliases remain missing debt. Parked frontier URLs are excluded from
history admission and closure, while their immutable audit records remain.

A historical season closes only when its root and every discovered in-scope
page, including provenance-carried scopes and season aliases, have successful
fetch and exact current-parser generic, typed, stateful and validation proof.
Quarantines, failed parsing and unfinished descendants remain debt. Completed
roots are not requeued merely to resume their descendants. A later discovered
page reopens the season. After SIGTERM or worker restart, the next run derives
progress from committed database/raw facts and recovers unprocessed immutable
raw for the pinned season before another paid fetch. Parser versions and
logical-refresh/batch identity formats are unchanged.

`prepare_history_campaign` and the all-done lock finalizer return a durable
by-year summary: `closed`, `in_progress`, `pending`, `missing`, `unavailable`
and `current_owned`. Inspect XCom or query the checkpoint read-only:

```sql
SELECT year,
       count(*) FILTER (WHERE state='closed') AS closed,
       count(*) FILTER (WHERE state='in_progress') AS in_progress,
       count(*) FILTER (WHERE state IN ('pending','in_progress','missing')) AS remaining,
       count(*) FILTER (WHERE state='missing') AS missing,
       count(*) FILTER (WHERE state='unavailable') AS unavailable,
       count(*) FILTER (WHERE state='current_owned') AS current_owned
FROM fbref_control.history_campaign_season
WHERE campaign_id='adult-men-2026-v1'
GROUP BY year ORDER BY year DESC;
```

## Lock, pool and duration

All FBref writer tasks inherit the one-slot `fbref_scraper_pool`: current,
bootstrap, history, replay, live acceptance and acceptance replay. Only the
FBref janitor task in the shared maintenance DAG receives this same pool;
other sources' tasks, pools and cron are unchanged. Lock sensors also use this
pool, then reschedule every 30 seconds while waiting, releasing their worker
and pool slots. The FBref janitor also reschedules on a busy probe or lost atomic
acquisition race; its 90-minute sensor wait releases the pool so the current
tail can finish. Successful cleanup retains its result through PokeReturnValue
and its 120-minute execution timeout. Other tasks in the shared maintenance
DAG retain their operator classes and settings. Current tasks and queue waiters have absolute priority 100;
history/replay/acceptance use 10. Publication acquisition commits a durable
queue entry and returns a busy result instead of throwing a busy-owner
`StateConflict`. Terminal control runs cannot block the queue. Atomic acquisition also prevents legacy noncurrent callers (including the
sealed janitor) from bypassing a queued current waiter during different sensor
poke clocks. Existing owner,
expiry and publication-generation fencing still fail closed for invalid state.

TTL is at most the remaining DagRun duration at acquisition: current 18 hours,
bootstrap 8 hours, history 45 minutes, replay 18 hours or acceptance/replay
3 hours. No FBref task uses the former eight-day default. The janitor retains
its existing one-hour lease and exact writer fence. The FBref-only wrapper
keeps the previous bounded wait as the default for standalone callers; this
DAG selects rescheduling to avoid a pool/lock handoff deadlock. It does not receive an unrelated new cleanup/lock implementation.

Admission uses observed fetch **plus parse** duration, never just domain
throttle. Initial evidence comes from successful committed fetch-through-parse
observations; later history samples record actual wave `wall_ms`. Unknown or
invalid/failed-only timing refuses admission. The conservative floor is
180 seconds per page, plus ten minutes setup/finalization and a 45-minute
margin before the next current window. Admission picks `admitted_batches`: the
largest live batch count (one page each, at most `max_batches` ≤ 12) whose
projection — setup + (batches + 1 recovery page) × measured page time — still
ends by the slice deadline; zero refuses the run before any paid request. The
check repeats when `run_live_waves` starts, which runs only the batches admitted
at that moment and stops new batches at the slice deadline. The live task has a
35-minute wave deadline and a 40-minute hard timeout; the full history run/lock
ceiling is 45 minutes. The guard and hard limits complement one another: an
observed estimate is not a worst-case guarantee.

Throughput at the 180-second floor: **about ten new live pages per slice, ~30 per
day**, often less when current work, slow measured pages, missing catalog
coverage or queue delay shrink the slice. A season may still take many slices.
Increasing shard/batch size, the 12-batch ceiling or opening additional slices
requires measured complete fetch+parse/setup/tail costs, a guard that fits the
four current reservations, regression checks and a reviewed PR. There is no
automatic increase that can bypass these bounds.

## Isolated validation

Unit checks cover the new DAG identity, disabled/legacy/conf-bypass fences,
rescheduling, remaining-duration TTL, four current reservations, measured cost
and complete 112-adult ordering. Real disposable PostgreSQL tests exercise
SIGTERM and a fresh worker, descendant typed/generic proof, season-scoped raw
recovery and due admission, unknown-coverage barriers, discontinued-only history,
current-before-history queue and terminal waiter removal from eligibility.
These tests do not fetch FBref or write production data. Real Airflow imports
must also pass from the reviewed release in an isolated network-none runtime.
Offline tests cannot prove seven days of non-displacement or scheduler reparse
in the actual production metabase.

## Deployment after separate authorization

1. Require the reviewed PR head, mandatory green CI and explicit merge/delivery
   permission. Keep #1328 open for live acceptance. Confirm current prerequisites
   (#1322/#1323 milestones) independently; delivery is not launch permission.
2. Re-read shared-stack rules and actual mounts/configs. Wait for no active FBref
   run/task, publication writer, FBref janitor or competing source delivery.
   Do not stop/clear runs or release another owner's lock to create a window.
3. The changed `control/store.py`, `control/migrations.py` and paid filter belong
   to the WhoScored runtime closure. This package requires the coordinated
   **regen → integrated image build → attestation → delivery** procedure with
   current runtime-contract lock and all three trust-root files. A four-file
   mounted-code copy like #1643 is insufficient. Verify the integration image
   against every source's contract; never copy new pinned bytes onto the old
   image or edit a production lock/trust root directly.
4. Prepare a matching schema-12 rollback image before delivery. Preserve
   migration 11 (#1323) as well as 12. Snapshot exact target hashes/modes,
   image/provenance, mounts, pause states, pool slots, schema checksums and
   publication/run state. Unknown drift blocks delivery.
5. In the authorized quiet window, apply the reviewed append-only migration
   12 (and separately authorized 11 if not yet installed), then deliver the
   exact coordinated image/runtime set. Keep the new controller paused and
   `FBREF_HISTORY_CONTROLLER_ENABLED=0`. Verify source readiness accepts
   migrations 1..12 with exact checksums before normal current work resumes.
6. Confirm effective one-slot pool exists; do not change slots merely to make
   checks pass. Verify delivered hashes and image attestation, real scheduler
   parse/serialization after the final cut, no new own import errors, current
   schedule unchanged, both history gates closed, other-source settings preserved.
7. Only after prerequisites and separate launch approval, enable the deployment
   switch through the approved integration runtime configuration and unpause
   the new controller. Do not trigger the legacy DAG/driver, activate Silver,
   manufacture a current acceptance window or duplicate the existing observer.

## Enabling at ~30 pages a day (#1958)

Owner steps on production, after the reviewed release is delivered:

1. Keep `dag_fbref_history_controller` paused. Deliver the FBref release through
   the normal coordinated procedure (step 3 above; `compose.yaml` and the
   WhoScored admission script are sealed, so the delivery carries the fresh
   runtime-contract lock and trust roots).
2. Set `FBREF_HISTORY_CONTROLLER_ENABLED=1` in the production compose
   environment (`.env` or the approved override). Only `0` and `1` are admitted.
3. Recreate only the scheduler (`docker compose ... up -d --no-deps
   airflow-scheduler`) in a quiet window: no active FBref run, not within
   15 minutes of a current window (00/06/12/18 UTC) and not inside a history
   admission window. Follow `/root/SHARED-STACK-PROTOCOL.md`.
4. Unpause the controller between 22:30 and 23:00 UTC (01:30–02:00 МСК). No
   manual trigger is needed: the first scheduled run after unpause is the
   canary by rule. While `fbref_control.history_timing` has no row with
   measured pages, `initialize_run` and `run_live_waves` cap the run at
   min(param, 100 requests) and min(param, 50 MiB), whatever conf or params
   say, and log `canary_first_run=True`. From the next run on, the normal
   limits apply.
5. After that run, check the `run_live_waves` XCom (`canary_first_run`,
   `request_limit`, `byte_limit_mb`, `admitted_batches`, pages) and that its
   measurement landed in `history_timing`. To stop, pause the controller.
6. Never launch the old driver alongside this DAG.

## Rollback

Rollback requires the same quiet-window authorization and exact ownership/drift
checks. Disable and pause the new history controller through the approved
procedure, allow an active slice to terminate naturally and release its exact
lock, then restore the prepared schema-12-compatible runtime/image bundle.
Keep all other-source changes and existing pauses. Verify hashes, attestation,
readiness, scheduler reparse and current behavior after the final cut.

Migration 12 is additive; preserve its tables/checkpoints. Do not drop them or
remove migration records/checksums to make old validation pass. A plain old
schema-10/11 image rejects installed 11/12 and is not a valid rollback. The
rollback bundle must retain the identical migration definitions/checksums and
support schema 12, while restoring the prior operational behavior. Restoring
old eight-day immediate-conflict locking would restore that known risk; state
this explicitly if selected. Never delete or steal a live publication lock.

## Acceptance after authorized launch

Observe seven consecutive days of history alongside current, without a
publication-lock collision killing current or deterioration in the direct
current freshness/SLA meter. Record by-year closed/in-progress/remaining,
missing/unavailable/current-owned counts, request/byte spend, actual timing,
queue wait, lock acquisition/release/TTL and the completed/recovered page IDs.
Include nights with zero work and their reason. A paused controller, one passing
slice or a successful SIGTERM copy test does not satisfy this live milestone.
Use the existing lead/observer context; do not reset the #1322 window.
