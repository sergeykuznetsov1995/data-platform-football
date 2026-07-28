# Understat production ingestion

Status: active implementation, 2026-07-27. Owner: data-platform.

## Production boundary

Understat is read directly through its AJAX endpoints; the ingestion has no
soccerdata dependency. The source-owned league registry contains all six
competitions exposed by Understat:

| Canonical league | Source league | Source id |
|---|---|---:|
| ENG-Premier League | EPL | 1 |
| ESP-La Liga | La_liga | 4 |
| GER-Bundesliga | Bundesliga | 3 |
| ITA-Serie A | Serie_A | 2 |
| FRA-Ligue 1 | Ligue_1 | 5 |
| RUS-Premier League | RFPL | 6 |

RFPL is a Bronze/DQ source scope. Existing Silver/Gold football facts remain
explicitly top-five because their identity spine is FBref-backed.

One exact `(league, season)` extraction produces seven normalized Bronze
tables:

1. `understat_schedule`
2. `understat_shots`
3. `understat_players`
4. `understat_team_match_stats`
5. `understat_player_match_stats`
6. `understat_player_team_season_stats`
7. `understat_team_season_breakdowns`

The final two tables come from `getTeamData`: exact player/team season splits
and source-faithful long-form category breakdowns. A match response is fetched
once and parsed into both shots and player-match rows.

## Current data DAG

`dag_ingest_understat` is the sole current-data owner and runs daily at 09:00
UTC. Runtime source discovery plans one mapped process per exact scope for the
rolling previous/current source seasons. During the June/July boundary it also
probes the new calendar-year source id until Understat publishes it. For the
2026-07-27 production generation, those canonical seasons are `2526` and
`2627`; no code/config edit is required when `2627` begins publishing.

Expected future-season states are explicit:

- `not_published`: an undiscovered active calendar probe is empty and neither
  v2 manifest data nor legacy physical schedule rows exist; nothing in Bronze
  is replaced;
- `upstream_pending`: schedule exists but no match is complete; only the
  schedule partition is replaced;
- `complete`: all seven non-empty entities pass DQ and are published.

An empty closed season is always `contract_failure`, never expected absence.
An empty scope already advertised by `getStatData` is also a hard failure: a
transient 404 cannot create a newer marker that hides valid Bronze data.
The 14:00 master pipeline waits for the exact successful 09:00 DAG generation;
it does not launch a duplicate crawl.

## Historical DAG

`dag_backfill_understat` is created paused and scheduled `@continuous`. After
it is unpaused, every run discovers source seasons, selects the oldest closed
scope that is not complete for the current contract, processes at most one
scope, then cools down for five minutes (30 minutes when idle). The manifest is
the durable cursor, so scheduler restarts do not reset progress.

Current data has priority 100 and history priority 10 in the same one-slot
scraper pool. Do not add Airflow date catchup or a hand-written season list:
history admission is source discovery plus manifest state.

## Publication and DQ fence

`iceberg.ops.understat_ingest_manifest_v1` is append-only. Every
publication-affecting attempt stores the exact source identity,
contract/parser version, status, row/key counts, content hashes and errors for
all seven entities. Failures before `in_progress` remain in the Airflow task
result/log and do not supersede legacy rows or the last complete batch. A
complete attempt requires:

- exact league, season and source-season identity in every row;
- required parser columns and unique, non-null natural keys;
- schedule-to-fact game coverage and player/team reference coverage;
- valid xG/coordinate/forecast domains;
- no previously populated entity disappearing;
- one shared `_batch_id` across all seven physical partitions;
- physical row counts matching the proposed manifest before `complete` is
  appended.

Because Iceberg replacements are table-by-table, the runner appends an
`in_progress` marker before its first write. Consumers use the latest manifest
attempt and exact complete `_batch_id`; a hard worker exit or newer failure
therefore hides the scope until a retry completes all seven tables. The 90%
partition-size guard remains enabled; the manual `--force-replace` flag
bypasses only that size check, not DQ or the physical batch fence.

The history runner rechecks the latest manifest and its seven physical
partitions after reacquiring the shared pool. If a higher-priority current task
completed the scope after planning, history returns that existing attempt and
does not append `in_progress`. The terminal watcher runs after cooldown and
propagates mapped runner/validator failures to the DagRun status.

Two match-level source exceptions were verified live on 2026-07-27. Both
`getMatchData` calls return HTTP 200 but empty inner `shots` and `rosters`:

| Scope | Match | Allowed missing entities |
|---|---:|---|
| FRA-Ligue 1 / 1617 | 4238 (SC Bastia–Lyon) | shots, player-match |
| GER-Bundesliga / 2425 | 27930 (Holstein Kiel–Bochum) | shots, player-match |

They are exact-ID exceptions in `scrapers/understat/coverage.py`. No league- or
season-wide percentage waiver exists; any new missing game blocks publication.

## Transport and cache

The native client performs cookie bootstrap and sends
`X-Requested-With: XMLHttpRequest`. It is paced evenly at 30 requests/minute,
honours `Retry-After`, retries retryable responses with exponential backoff and
jitter, and stores atomic JSON cache entries in the persistent scraper cache
volume. Current runs refresh league/team scope payloads. Every non-complete
history run also refreshes from the source, so a partial cached HTTP-200 payload
cannot trap the self-draining DAG on one scope. The standalone runner retains
an explicit `--reparse` override for operator-driven runs.

## Operator checks

Before unpausing history or accepting a deployment:

```bash
python -m pytest -q tests/unit/scrapers/test_understat_native.py \
  tests/unit/scrapers/test_understat_manifest.py \
  tests/unit/scrapers/test_run_understat_scraper.py \
  tests/unit/dags/test_dag_ingest_understat.py \
  tests/unit/dags/test_dag_backfill_understat.py
```

Inspect the mapped runner result for `scope_attempt`, then verify the latest
manifest status and shared physical `_batch_id` before downstream use. History
is finished only when `plan_history_scope` returns an empty list and all
source-discovered closed scopes pass `is_scope_complete(..., verify_physical=True)`.

Do not unpause history from a source checkout that has not been deployed into
the scheduler image/mount. Do not run both the old multi-scope runner and the
new mapped DAG; the legacy CLI has been removed.
