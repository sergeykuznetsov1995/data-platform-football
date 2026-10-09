# FBref daily milestone report (#1323)

This PR prepares collection evidence and the daily host report. It does not
merge, deploy, modify cron, install the watchdog patch, trigger DAGs, send
messages, restart history, or touch Silver. #1323 stays open until three
automatic morning reports after separately authorized deployment.

## Measurement

The primary 24-hour SLA starts at the **first observed completed report link**
and ends at successful generic and typed Bronze parse/persistence/validation.
HTTP success alone is not readiness. `first_seen_at` separately retains the
earliest observation of the link, including a preview before the final score.
Migration 11 stores both timestamps and the exact raw fetch-manifest keys.
Repeated polls/retries use atomic minimum timestamps; older replay can move a
timestamp earlier and moves its raw proof with it. Kickoff uses the newest
source read, so postponed fixtures move to the corrected UTC day; older replay
cannot undo a correction. A newer read without explicit UTC makes kickoff
unknown and retains its source-read fence. Conflicting epochs at the same source-read time are
unknown. No legacy timestamp is
inferred from frontier creation, parser execution time, or deployment time.

Both source cohorts use **UTC kickoff dates**. FotMob has explicit `utc_time`;
its timezone field is commonly empty. FBref uses an explicit epoch/offset
advertised in the schedule HTML (`data-venue-epoch`). A FBref fixture without
that timestamp remains visible by its source fixture date, but its date is
unconfirmed and the cohort cannot pass acceptance. Never treat venue-local
schedule time as UTC. These UTC cohorts are not directly interchangeable with
the old #1322 source-calendar/assumed-time SQL.

Source lag is the first observed completed link minus explicit kickoff plus
two hours. Report median and continuous 95th percentile, sample count, and
unknown count. This is an **upper-bound observation estimate**, not the exact
publication time or final whistle. A negative estimate is unknown, not zero.
Also report match-to-first-fetch and match-to-Bronze readiness distributions.

The adult universe comes from the registry before joining schedules. Exclude
women/youth/reserves by source section/class/name and explicitly exclude
850–853 and `Big5`. The five independent European leagues remain eligible.
112 is the observed registry baseline, not a hardcoded SQL population. Quarantined or otherwise non-active adults stay explicit blockers unless
`skipped` plus `current_scope_lifecycle=discontinued` and a reason proves their
retirement. Missing
current seasons, missing/stale/unvalidated schedules, and source read failures
remain explicit and block cohort completion. Non-current/history lanes are
not altered. Administrative awards, cancelled/postponed games and previews
without a real score are not played fixtures. A played fixture without a
report is counted and remains unknown.

`configs/fbref/fotmob-report-map.json` records explicit, reviewed source IDs.
The included FotMob Bronze catalog was read on 2026-10-08. Seven registry
entries have no proven matching ID; their gaps are explicit. UEFA Nations
League spans four source IDs. Compare the same normalized season and UTC day,
deduplicate native fixture IDs, and require a fresh successful `league_season`
manifest. No fuzzy runtime name join, Silver xref, or live source request.
Unavailable/stale/unsupported comparator coverage is unknown, not zero.
Played FotMob rows with NULL/invalid UTC are retained and block comparison;
the SQL date filter cannot silently discard them. Supporting first-fetch
is the earliest successful source read, including reads before link discovery;
its minimum is separate from the Bronze readiness proof.
Mismatching source counts or missing comparator evidence cannot produce a
successful milestone day. The report does not repair either source's data.

## Use and output

```bash
python3 -B scripts/report_fbref_daily.py \
  --host-read-only --date 2026-10-07 --as-of 2026-10-08T05:00:00Z \
  --output-dir /root/data-platform-football/.local/reports/fbref/daily
```

The host uses the established `docker exec ... psql` reader in `BEGIN READ
ONLY`, with `ON_ERROR_STOP` and a statement timeout, and `trino-ro.sh`. It does
not initialize schemas. A saved snapshot can be replayed with `--input` and
the same `--date`/`--as-of`. JSON, Markdown, and the redacted source snapshot
are retained under timestamped names; the cutoff is fixed once per execution.
The snapshot is not a time-travel reconstruction of overwritten Bronze.
Driver failures are reduced to source labels without exposing credentials.

The morning output contains yesterday's N/M/%/FotMob, pending and unknown
counts, the latest final cohort within the lookback, and the consecutive
successful-day count. The default lookback is 14 days (maximum 31). Dates with
unknown evidence, pending deadlines, or no games do not confirm a successful
day. Even already-collected games do not finalize their cohort before every
known 24-hour deadline has elapsed. `--require-final` exits 2 for a provisional
cohort. The normal morning report prints the incomplete state and exits 0.

## Separate deployment and #1322 acceptance

Control migration/store are sealed by the shared WhoScored runtime contract.
The PR regenerates only the checked-in lock/trust evidence. Deployment needs a
matching image/runtime set, migration 11 **before** the new pipeline code, and
normal runtime attestation. Copying these files into the old live runtime is
unsafe. Do not apply the migration or install the watchdog patch during PR work.

The prepared adapter defaults to a future stable release directory under
`/root/data-platform-football/.local/releases/fbref-1323`; an authorized delivery
must materialize that release or set `FBREF_DAILY_REPORT_ROOT`. The adapter uses
the existing morning invocation; it creates no cron. Test the provided patch
against the actual watchdog version before installation. Missing release,
timeout or malformed output yields an explicit warning rather than success.

Preserve `/root/handoffs/fbref-latest.md` and the existing #1643 observer. The
handoff's 2026-10-08 delivery was verified; a **new 72-hour #1322 acceptance
window had not started**. The previous 108/112 cannot be reused as full-scope
acceptance. Only the #1322 lead can record a new start after full adult scope,
zero missing schedules and the first successful load of 82. Keep checks of
repeat schedule fetches/median interval <=6.5h, peak-day first-fetch <=24h, and
publication-lock conflicts. This report supplements those checks; it does not
restart their window, restart history, close #1322/#1326 or manufacture runs.
