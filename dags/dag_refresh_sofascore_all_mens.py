"""Current-season refresh of every adult-men SofaScore tournament (lane F).

Three times a day the lane walks SEASON PAGES of the campaign's tournaments
(metered discovery, ``bronze.sofascore_schedule``) — the source has no by-date
event list, which a live probe settled on 2026-08-25 — then runs the existing
matches phase of the scope cycle on the ``SS-*`` partitions holding finished
games without a complete capture.  The sweep splits its targets into the
seasons playing now, the known ones and the never-seeded, each walking its own
cursor; ``run_sofascore_schedule_refresh.py`` carries the details.  The 14
configured leagues stay with ``dag_ingest_sofascore``; the frozen campaign
state (state.json, failures.json) is never touched by this lane.
After that, ``enrich_season_metadata`` validates the next pending seasons of
the campaign snapshot by queue priority (#1354), so the history lane keeps
getting ready seasons without a serialized metadata wave of its own.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from scrapers.sofascore.match_deadline import match_deadline_sql
from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils.sofascore_dq import summarize_bronze_dq_reports
from utils import sofascore_all_mens_state as state


DAG_ID = "dag_refresh_sofascore_all_mens"
SNAPSHOT_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_SNAPSHOT",
    "/opt/airflow/runtime/sofascore/all-men/snapshot.json",
)
POLICY_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_POLICY",
    "/opt/airflow/configs/sofascore/all_mens_campaign.json",
)
WORKLOAD_ARTIFACT = os.environ.get(
    "SOFASCORE_PROXY_BUDGET_ARTIFACT",
    "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
)
RESULT_DIR = (
    os.environ.get("SOFASCORE_REFRESH_RESULT_DIR", "").strip()
    or state.DEFAULT_REFRESH_RESULT_DIR
)

# Refresh lane knobs; every one is read at DAG parse and fails closed.
# The lane's own timings; the DagRun window has to hold all of them because the
# batch runs SERIALLY on one pool slot (``max_active_tasks`` is 1).
REFRESH_DAGRUN_TIMEOUT = timedelta(hours=7)
REFRESH_FETCH_TIMEOUT = timedelta(minutes=150)
# #2107: the sweep retries once after this delay.  The retry shares the first
# try's window (counted from the DagRun start) instead of getting its own, so
# the sweep phase costs at most REFRESH_FETCH_TIMEOUT + this delay.
REFRESH_FETCH_RETRY_DELAY = timedelta(minutes=10)
# #2118: a scope gets two retries, 10 min apart.  A proxy exit that is down
# is usually still down 2 min later; 10 min later a new lease mostly lands on
# another exit.  The third try is for a transport-only loss only
# (RUN_SCOPE_COMMAND); a waiting scope does not hold the pool slot.
REFRESH_SCOPE_RETRIES = 2
REFRESH_SCOPE_RETRY_DELAY = timedelta(minutes=10)
# #2160: a refill scope that lost only its transport is planned once more
# after REFRESH_SCOPE_RETRY_DELAY, under a new round (new plan identity and
# lease); a second loss leaves it for the next run.
REFILL_SCOPE_RETRIES = 1
# Ceiling of one scope task; its real timeout is its own estimate x 1.5.
REFRESH_SCOPE_TIMEOUT = timedelta(seconds=state.REFRESH_MAX_SCOPE_TIMEOUT_SECONDS)
# Season metadata enrichment (#1354) runs after the scopes inside the same
# window: 170 seasons plus their tournaments' identity checks are <= 340
# requests, ~17 min at the lane's 20/min.
METADATA_TIMEOUT = timedelta(minutes=25)
# #1358: the scopes of one run share ONE window instead of each being sized
# for the worst case of the slowest one.  The old arithmetic (sweep + batch x
# 2 h x 2 attempts <= DagRun) admitted exactly one scope a run, while a scope
# really takes 10-60 min — most of the window went unused, and the backlog run
# still turned red on a scope that could not finish in 2 h.  Now the scopes
# share REFRESH_SCOPE_BUDGET by their own estimates (pending matches x
# SECONDS_PER_MATCH + warm-up, ``state.plan_refresh_batch``), every task gets
# a timeout from its own estimate, and a scope that hits its time or byte
# ceiling stops taking allocations and ends ``partial`` — the rest of it is
# the next run's.  The other half of the old window is the reserve for the
# retries (REFRESH_SCOPE_RETRIES: the workload plan supports a resumed retry).
# #1368: 30 minutes moved from this shared budget to the seasonal fields
# renewal below, so the serial DagRun still fits its 7 h exactly.
REFRESH_SCOPE_BUDGET = timedelta(minutes=90)
# #1751: transport-only scope losses in one run before it is red.
MAX_INITIAL_TRANSIENT_FAILURES = 3
# 15 min short of the (now 90 min) scope budget: they pay for the sweep's
# retry delay (#2107) and for one scope retry delay after the window
# deadline (#2118: a scope that lost its transport right at the deadline is
# retried 10 min later and its third try ends at once past the deadline).
REFRESH_RETRY_RESERVE = timedelta(hours=1, minutes=45)
# #2134: the refill task's own timeout.  A census of the remaining work takes
# 1-12 min (Trino), so the refill stops starting scopes REFILL_CENSUS_RESERVE
# short of it and ends with its own stop reason instead of being killed.
REFILL_TIMEOUT = REFRESH_SCOPE_BUDGET + timedelta(minutes=5)
REFILL_CENSUS_RESERVE = timedelta(minutes=15)
# #1368: the seasonal fields renewal window — re-buying standings_total,
# rounds, cup_trees and participants (4 requests per partition) for ongoing
# core partitions whose league table is older than the renewal interval.
# The lane is serial, so this reservation is real time taken from the scope
# budget above, and a scope stops being admitted in time to publish, flush
# the manifest and write the report inside the task timeout.
SEASON_FIELDS_TIMEOUT = timedelta(minutes=30)
SEASON_FIELDS_SCOPE_TIMEOUT = timedelta(minutes=10)
# Cap per run; the window (wall clock) decides how many actually go.
SEASON_FIELDS_MAX_SCOPES = state.env_int(
    "SOFASCORE_SEASON_FIELDS_MAX_SCOPES", 40, 1, 500
)
# Admission age in days, strictly below the 7-day contract so a rolling
# renewal can never breach it (допущение а задачи #1368). The cap is 6 for
# the same reason: 7 would only start admitting once the freshness limit is
# already reached, and the 3x/day cadence adds further delay (#1368 r1).
SEASON_FIELDS_RENEWAL_DAYS = state.env_int(
    "SOFASCORE_SEASON_FIELDS_RENEWAL_DAYS", 6, 1, 6
)
# A failed or no-credit scope waits this long before consuming a slot again.
SEASON_FIELDS_BACKOFF_HOURS = state.env_int(
    "SOFASCORE_SEASON_FIELDS_BACKOFF_HOURS", 6, 1, 168
)
if (
    REFRESH_FETCH_TIMEOUT
    + REFRESH_FETCH_RETRY_DELAY
    + REFRESH_SCOPE_BUDGET
    + REFRESH_SCOPE_RETRY_DELAY
    + REFRESH_RETRY_RESERVE
    + SEASON_FIELDS_TIMEOUT
    + METADATA_TIMEOUT
    > REFRESH_DAGRUN_TIMEOUT
):
    raise AirflowException(
        "refresh window does not fit the DagRun: sweep + its retry delay + "
        "scope budget + scope retry delay + retry reserve + season fields + "
        "metadata > dagrun_timeout"
    )
# Pace of the match phase, re-measured 29-30.09 on the refresh lane (~12 s a
# match, ``state.DEFAULT_REFRESH_SECONDS_PER_MATCH``); an env override retunes
# the estimate without a release.
REFRESH_SECONDS_PER_MATCH = state.env_int(
    "SOFASCORE_REFRESH_SECONDS_PER_MATCH",
    state.DEFAULT_REFRESH_SECONDS_PER_MATCH,
    1,
    600,
)
# Only an upper bound on the NUMBER of scopes now; the window decides how many
# actually go.  A cut by this bound is logged by the planner.
REFRESH_BATCH_SIZE = state.env_int(
    "SOFASCORE_REFRESH_BATCH_SIZE", state.DEFAULT_REFRESH_BATCH_SIZE, 1, 128
)
REFRESH_POOL = (
    os.environ.get("SOFASCORE_REFRESH_POOL", "").strip() or INGEST_SCRAPER_POOL
)
REFRESH_MAX_ACTIVE_TASKS = state.env_int(
    "SOFASCORE_REFRESH_MAX_ACTIVE_TASKS", 1, 1, 16
)
# #1358: the sweep is sized by PAGES, not by fixed class caps.  Its budget is
# what the fetch window holds at the lane's pace with a 20 % margin:
# 150 min x 20 requests/min x 0.8 = 2 400 pages.  The ``due`` class takes its
# share first, then the seed slice, then ``stale`` from what is left — stale
# shrinks instead of blocking the start.  The byte cap of the run is derived
# from the planned pages by ``worst_case_bytes`` (page size + lease warm-ups);
# a non-zero ``SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES`` is an operator
# override, and the run refuses a plan that does not fit it (Sol round 6,
# finding 5).
REFRESH_PAGES_PER_MINUTE = 20
REFRESH_PAGE_BUDGET = int(
    REFRESH_FETCH_TIMEOUT.total_seconds() / 60 * REFRESH_PAGES_PER_MINUTE * 0.8
)
REFRESH_DISCOVERY_BUDGET_BYTES = state.env_int(
    "SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", 0, 0, 1024 ** 3
)
# Seasons playing in the window get their tail pages on every run — all of
# them: the class limit is the number of due seasons of the run, bounded by
# the page budget.  This env value is only an emergency upper bound.  That is
# what keeps the "match finished -> row in Bronze" lag inside the interval.
REFRESH_MAX_DUE = state.env_int("SOFASCORE_REFRESH_MAX_DUE", 4096, 1, 4096)
# Known seasons outside the window get their tail page a slice at a time: a
# league playing once a week is outside it most of the time, and nothing else
# would ever ask for its next round.  Three runs a day take the slice around
# the ~1.4k campaign seasons in about 2.3 days — that is the lag ceiling for a
# season that is not playing right now.
REFRESH_MAX_STALE = state.env_int("SOFASCORE_REFRESH_MAX_STALE", 200, 1, 4096)
# A tail visit walks back while the page it read is entirely newer than what
# Bronze has: more than one page of matches can finish between two visits.
REFRESH_CHASE_PAGES = state.env_int("SOFASCORE_REFRESH_CHASE_PAGES", 3, 1, 32)
# Seasons Bronze has never seen are seeded a slice at a time, whole page chain.
REFRESH_MAX_SEED = state.env_int("SOFASCORE_REFRESH_MAX_SEED", 40, 1, 4096)
REFRESH_SEED_PAGES = state.env_int("SOFASCORE_REFRESH_SEED_PAGES", 12, 1, 64)
REFRESH_WINDOW_HOURS = state.env_int("SOFASCORE_REFRESH_WINDOW_HOURS", 36, 1, 168)
# The lease ceiling the gateway hands out.  It is forwarded because the sweep's
# preflight counts one browser warm-up per lease: a smaller ceiling means more
# of them, and an estimate that silently assumes 8 MiB would understate the run
# (Sol round 9, finding 4).
REFRESH_PER_LEASE_MAX_BYTES = state.env_int(
    "SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES", 8 * 1024 * 1024, 1, 1024 ** 3
)
# Forwarded to every task of the lane only when set: a gateway of its own.
REFRESH_TASK_ENV: dict[str, str] = {}
_control_url = os.environ.get("SOFASCORE_REFRESH_PROXY_CONTROL_URL", "").strip()
if _control_url:
    REFRESH_TASK_ENV["SOFASCORE_PROXY_CONTROL_URL"] = _control_url
# Season metadata enrichment (#1354): after the refresh work, the lane
# validates the next N pending seasons of the snapshot by queue priority, so
# the history lane always has ready seasons to plan.  0 switches it off.  The
# task is NOT in REFRESH_TASK_IDS: a failed enrichment is a red task, never a
# red DagRun.
METADATA_SEASONS_PER_RUN = state.env_int(
    "SOFASCORE_METADATA_SEASONS_PER_RUN", 170, 0, 10000
)
METADATA_BUDGET_BYTES = 16 * 1024 * 1024
METADATA_RESULT_DIR = str(Path(
    os.environ.get(
        "SOFASCORE_ALL_MENS_RESULT_DIR",
        "/opt/airflow/runtime/sofascore/all-men/results",
    )
).parent / "metadata-results")
ENRICH_SCRIPT = "/opt/airflow/scripts/enrich_sofascore_all_mens_snapshot.py"
REFRESH_TASK_IDS = frozenset({
    "refresh_season_schedules",
    "plan_refresh_batch",
    "run_refresh_scope",
    "validate_refresh_scope",
    "refill_refresh_window",
})

# Finished games of campaign partitions that have no complete capture yet,
# per partition.  ``season`` is CAST on both sides because the
# schedule stores it as the source ships it while the status table is text.
# #1359: per season also the nearest deadline that has NOT passed yet (end of
# the match + 24 h, the rule of the milestone-1 meter, ``match_deadline``) and
# how many pending matches still have an open deadline.
PENDING_PARTITIONS_SQL = """
SELECT p.league, p.season,
       count(*) AS pending_matches,
       min(CASE WHEN p.deadline >= to_unixtime(current_timestamp)
                THEN CAST(p.deadline AS bigint) END) AS min_open_deadline_ts,
       count_if(p.deadline >= to_unixtime(current_timestamp))
           AS open_deadline_matches, p.source_season_id,
       max(CASE WHEN p.deadline < to_unixtime(current_timestamp)
                THEN CAST(p.deadline AS bigint) END) AS max_overdue_deadline_ts
FROM (
    SELECT DISTINCT s.league, CAST(s.season AS varchar) AS season, s.game_id,
           CAST(s.season_id AS bigint) AS source_season_id,
           """ + match_deadline_sql("s.start_timestamp", "s.changes_change_timestamp") + """
               AS deadline
    FROM iceberg.bronze.sofascore_schedule s
    LEFT JOIN iceberg.bronze.sofascore_match_capture_status c
      ON c.league = s.league
     AND CAST(c.season AS varchar) = CAST(s.season AS varchar)
     AND c.match_id = CAST(s.game_id AS varchar)
     AND c.capture_complete = true
    WHERE s.league LIKE 'SS-%'
      AND s.status_type = 'finished'
      AND c.match_id IS NULL
      -- #1778: SofaScore moved the match to another season; its stale
      -- schedule row is no longer this partition's debt.
      AND NOT EXISTS (
          SELECT 1 FROM iceberg.bronze.sofascore_rejected_rows r
          WHERE r.league = s.league
            AND r.season = CAST(s.season AS varchar)
            AND r.reason_code = 'season_mismatch'
            AND r.natural_key = CAST(CAST(s.season_id AS bigint) AS varchar)
                || '|' || CAST(s.game_id AS varchar))
) p
GROUP BY 1, 2, 6
"""


def _pending_refresh_partitions() -> list[tuple[str, str, int, int | None, int, int, int | None]]:
    """Query at task runtime; no Trino client is touched at DAG parse."""

    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(PENDING_PARTITIONS_SQL)
        return [
            (
                str(league),
                str(season),
                int(count),
                int(deadline) if deadline is not None else None,
                int(open_matches or 0),
                int(source_season_id),
                int(overdue) if overdue is not None else None,
            )
            for league, season, count, deadline, open_matches, source_season_id, overdue
            in cursor.fetchall()
        ]
    finally:
        conn.close()


# #1368: the seasonal fields census. "Ongoing" reuses the sweep's due
# predicate verbatim (a latest schedule row kicking off inside the window);
# freshness is the league table's own max(_ingested_at) — a recent
# participants write must not mask stale standings, and a missing table
# partition is stale (допущение а задачи: критерий по таблицам лиг).
SEASON_FIELDS_CENSUS_SQL = """
WITH ranked AS (
    SELECT league, CAST(season AS varchar) AS season, game_id, start_timestamp,
           row_number() OVER (
               PARTITION BY league, season, game_id ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.sofascore_schedule
), ongoing AS (
    SELECT DISTINCT league, season
    FROM ranked
    WHERE rn = 1
      AND start_timestamp >= to_unixtime(current_timestamp) - {hours} * 3600
      AND start_timestamp <= to_unixtime(current_timestamp) + 6 * 3600
), table_age AS (
    SELECT league, CAST(season AS varchar) AS season, max(_ingested_at) AS last_ingested
    FROM iceberg.bronze.sofascore_league_table
    GROUP BY 1, 2
)
SELECT o.league, o.season, t.last_ingested
FROM ongoing o
LEFT JOIN table_age t ON t.league = o.league AND t.season = o.season
"""


def _season_fields_census() -> list[tuple[str, str, Any]]:
    """Ongoing partitions with their league-table age; query at task runtime."""

    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(SEASON_FIELDS_CENSUS_SQL.format(hours=int(REFRESH_WINDOW_HOURS)))
        return [(str(league), str(season), last) for league, season, last in cursor.fetchall()]
    finally:
        conn.close()


def _season_fields_admission(
    census: list[tuple[str, str, Any]],
    *,
    as_of: datetime,
    core_keys: frozenset[str],
) -> dict[str, Any]:
    """One shared definition for the acceptance ratio and the work selection.

    Count ongoing core seasons where a standings table applies. Keep all
    core entries available for the seasonal workload, including cup brackets.
    """
    from scrapers.sofascore.denominator import STANDINGS_NOT_APPLICABLE

    entries: list[dict[str, Any]] = []
    for league, season, last_ingested in census:
        if league not in core_keys:
            continue
        age_days: float | None
        if last_ingested is None or str(last_ingested) == "":
            age_days = None
        else:
            if not isinstance(last_ingested, datetime):
                stamp = datetime.fromisoformat(str(last_ingested))
            else:
                stamp = last_ingested if last_ingested.tzinfo else last_ingested.replace(
                    tzinfo=timezone.utc
                )
            age_days = (as_of - stamp).total_seconds() / 86400
        entries.append({
            "league": league,
            "season": season,
            "age_days": age_days,
            "fresh": age_days is not None and age_days <= 7,
        })
    standings = [
        item for item in entries
        if (item["league"], item["season"]) not in STANDINGS_NOT_APPLICABLE
    ]
    core_ongoing = len(standings)
    fresh_7d = sum(1 for item in standings if item["fresh"])
    return {
        "as_of": as_of.isoformat(),
        "core_ongoing": core_ongoing,
        "standings_not_applicable": len(entries) - core_ongoing,
        "fresh_7d": fresh_7d,
        "ratio": (fresh_7d / core_ongoing) if core_ongoing else None,
        "entries": entries,
    }


def _season_fields_backoff_path() -> Path:
    return Path(RESULT_DIR) / "season-fields-backoff.json"


def _load_season_fields_backoff() -> dict[str, str]:
    try:
        payload = json.loads(_season_fields_backoff_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _select_season_fields_work(
    admission: dict[str, Any],
    *,
    as_of: datetime,
    backoff: Mapping[str, str],
    max_scopes: int,
    renewal_days: int,
) -> tuple[list[tuple[str, str]], list[str]]:
    """Pick this run's renewal scopes and the admission reasons it skipped."""

    selected: list[tuple[float, str, str]] = []
    skipped: list[str] = []
    for item in admission["entries"]:
        league = item["league"]
        if not league.startswith("SS-"):
            # Lane ownership: the configured ingest leagues buy their own
            # season fields daily; they stay counted, never double-purchased.
            continue
        key = f"{league}|{item['season']}"
        not_before = backoff.get(key)
        if not_before:
            try:
                threshold = datetime.fromisoformat(not_before)
            except ValueError:
                threshold = None
            if threshold is not None and as_of < threshold:
                skipped.append(f"{key}:backoff")
                continue
        age = item["age_days"]
        if age is not None and age < renewal_days:
            skipped.append(f"{key}:not_due")
            continue
        # Missing tables first, then the OLDEST table (its freshness deadline
        # is the earliest), stable by identity (#1368 review r1: ascending
        # age would buy the youngest due table first and starve the stalest
        # partitions under the scope/time cap).
        selected.append((age if age is not None else -1.0, league, item["season"]))
    selected.sort(
        key=lambda item: (
            item[0] >= 0,   # missing (age < 0) before any known age
            -item[0],       # among known ages, the largest first
            item[1],
            item[2],
        )
    )
    pairs = [(league, season) for _age, league, season in selected[:max_scopes]]
    return pairs, skipped


def _execute_season_fields_scope(env: dict[str, str]) -> dict[str, Any]:
    """Run one fields scope cycle under its own run id and timeout."""

    command = [
        "/opt/legacy-scraper-venv/bin/python",
        "/opt/airflow/dags/scripts/run_sofascore_scope_cycle.py",
        "--snapshot", env["SOFASCORE_CAMPAIGN_SNAPSHOT"],
        "--tournament-id", env["SOFASCORE_TOURNAMENT_ID"],
        "--source-season-id", env["SOFASCORE_SOURCE_SEASON_ID"],
        "--expected-snapshot-id", env["SOFASCORE_EXPECTED_SNAPSHOT_ID"],
        "--expected-campaign-id", env["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
        "--phase", "season_fields", "--allow-pending-season",
        "--registry-overlay",
        "--output-dir", env["SOFASCORE_SCOPE_OUTPUT_DIR"],
        "--output", env["SOFASCORE_SCOPE_RESULT_PATH"],
        "--workload-artifact", env["SOFASCORE_WORKLOAD_ARTIFACT"],
        "--run-id", env["SOFASCORE_SCOPE_RUN_ID"],
    ]
    started = time.monotonic()
    try:
        child = subprocess.run(
            command,
            cwd="/opt/airflow",
            env={**os.environ, **env, "AIRFLOW_CTX_DAG_ID": DAG_ID,
                 "AIRFLOW_CTX_TASK_ID": "refresh_season_fields"},
            timeout=max(60, int(env["SOFASCORE_SCOPE_TIMEOUT_S"])),
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        # run() kills the child before raising; the timeout is one scope's
        # failed outcome, never a reason to lose the backoff persistence,
        # the remaining scopes and the report (#1368 review r1).
        return {
            "scope_key": env["SOFASCORE_SCOPE_KEY"],
            "run_id": env["SOFASCORE_SCOPE_RUN_ID"],
            "exit_code": None,
            "status": "timeout",
            "errors": ["scope exceeded SOFASCORE_SCOPE_TIMEOUT_S"],
            "elapsed_s": round(time.monotonic() - started, 1),
        }
    result: Any = None
    try:
        result = json.loads(
            Path(env["SOFASCORE_SCOPE_RESULT_PATH"]).read_text(encoding="utf-8")
        )
    except (OSError, ValueError):
        result = None
    outcome: dict[str, Any] = {
        "scope_key": env["SOFASCORE_SCOPE_KEY"],
        "run_id": env["SOFASCORE_SCOPE_RUN_ID"],
        "exit_code": child.returncode,
        "elapsed_s": round(time.monotonic() - started, 1),
    }
    if isinstance(result, dict):
        phases = result.get("phases") or []
        for field in ("status", "errors", "bytes"):
            if field in result:
                outcome[field] = result[field]
        if phases and isinstance(phases[0], dict):
            for field in ("league_table_rows", "rounds_rows", "cup_trees_rows",
                          "participants_rows", "source_request_count"):
                if field in phases[0]:
                    outcome[field] = phases[0][field]
    if child.returncode != 0:
        outcome["stderr_tail"] = child.stderr.splitlines()[-5:]
    return outcome


def _season_fields_report_path(run_id: str) -> Path:
    safe_run = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in run_id)
    return Path(RESULT_DIR) / f"season-fields-{safe_run}.json"


def _refresh_season_fields(**context: Any) -> dict[str, Any]:
    """#1368: renew the season fields of ongoing core SS partitions.

    One census before (work selection and the freshness line share it), the
    bounded renewal loop, one census after.  Failures of individual scopes
    back off instead of monopolizing the cap; the task itself fails only on
    the census or the snapshot identity.
    """

    from dags.scripts.run_sofascore_scope_cycle import _atomic_json
    from scrapers.sofascore.denominator import load_denominator

    run_id = str(context.get("run_id") or "manual")
    as_of = datetime.now(timezone.utc)
    deadline = time.monotonic() + (
        SEASON_FIELDS_TIMEOUT - SEASON_FIELDS_SCOPE_TIMEOUT - timedelta(minutes=2)
    ).total_seconds()
    try:
        census = _season_fields_census()
    except Exception as exc:
        raise AirflowException(f"season fields census failed: {exc}") from exc
    core_keys = load_denominator().core_capture_keys()
    admission = _season_fields_admission(census, as_of=as_of, core_keys=core_keys)
    backoff = _load_season_fields_backoff()
    pairs, skipped = _select_season_fields_work(
        admission,
        as_of=as_of,
        backoff=backoff,
        max_scopes=SEASON_FIELDS_MAX_SCOPES,
        renewal_days=SEASON_FIELDS_RENEWAL_DAYS,
    )
    snapshot = state.read_refresh_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    envs, unresolved = state.plan_season_fields_batch(
        snapshot,
        pairs,
        scope_timeout_s=int(SEASON_FIELDS_SCOPE_TIMEOUT.total_seconds()),
        snapshot_path=SNAPSHOT_PATH,
        policy_path=POLICY_PATH,
        result_dir=RESULT_DIR,
        workload_artifact=WORKLOAD_ARTIFACT,
        dag_run_id=run_id,
        task_env=REFRESH_TASK_ENV,
    )
    outcomes: list[dict[str, Any]] = []
    stop_reason = "completed"
    for env in envs:
        if time.monotonic() >= deadline:
            stop_reason = "time_budget"
            break
        outcome = _execute_season_fields_scope(env)
        outcomes.append(outcome)
        key = (
            f"{env['SOFASCORE_CAPTURE_KEY']}"
            f"|{env['SOFASCORE_CANONICAL_SEASON']}"
        )
        if outcome.get("exit_code") != 0:
            backoff[key] = (
                as_of + timedelta(hours=SEASON_FIELDS_BACKOFF_HOURS)
            ).isoformat()
        elif not outcome.get("league_table_rows"):
            # A terminal empty standings answer earns no freshness credit
            # (the table stays missing); it must not burn a slot every run.
            backoff[key] = (
                as_of + timedelta(days=SEASON_FIELDS_RENEWAL_DAYS)
            ).isoformat()
        else:
            backoff.pop(key, None)
    try:
        _atomic_json(_season_fields_backoff_path(), backoff)
    except OSError as exc:
        raise AirflowException(f"season fields backoff write failed: {exc}") from exc
    try:
        after_census = _season_fields_census()
    except Exception as exc:
        raise AirflowException(f"season fields post-census failed: {exc}") from exc
    after = _season_fields_admission(
        after_census, as_of=datetime.now(timezone.utc), core_keys=core_keys
    )
    freshness_fields = ("core_ongoing", "standings_not_applicable", "fresh_7d", "ratio")
    summary = {
        "run_id": run_id,
        "before": {
            name: admission[name]
            for name in ("as_of", *freshness_fields)
        },
        "after": {
            name: after[name]
            for name in ("as_of", *freshness_fields)
        },
        "season_fields_freshness": {name: after[name] for name in freshness_fields},
        "selected": len(pairs),
        "executed": len(outcomes),
        "failed": sum(1 for item in outcomes if item.get("exit_code") != 0),
        "no_credit": sum(
            1
            for item in outcomes
            if item.get("exit_code") == 0 and not item.get("league_table_rows")
        ),
        "unresolved": [f"{league}|{season}" for league, season in unresolved],
        "skipped": len(skipped),
        "stop_reason": stop_reason,
        "outcomes": outcomes,
    }
    try:
        _atomic_json(_season_fields_report_path(run_id), summary)
    except OSError as exc:
        raise AirflowException(f"season fields report write failed: {exc}") from exc
    return {
        name: summary[name]
        for name in (
            "run_id", "before", "after", "season_fields_freshness", "selected",
            "executed", "failed", "no_credit", "unresolved", "skipped",
            "stop_reason",
        )
    }


def _dag_run_type_name(dag_run: Any) -> str:
    """Normalize Airflow's enum and string DagRun type representations."""

    try:
        run_type = dag_run.run_type
    except AttributeError as exc:
        raise AirflowException("refresh DagRun has no run_type") from exc
    return str(getattr(run_type, "value", run_type)).casefold().split(".")[-1]


def _refresh_queue_mode(context: dict[str, Any]) -> str:
    """One queue for every slot (#1359); a manual run may ask for the debt only.

    Scheduled 00:30, 08:30 and 15:30 runs all plan by deadline: open deadlines
    first, the debt fills the rest of the window.  ``conf.queue_mode=backlog``
    on a manual run plans the debt tier alone.
    """

    dag_run = context.get("dag_run")
    run_type = _dag_run_type_name(dag_run)
    if run_type in {"scheduled", "backfill"}:
        return "deadline"
    if run_type != "manual":
        raise AirflowException(f"unsupported refresh run_type: {run_type!r}")
    conf = getattr(dag_run, "conf", {}) or {}
    if not isinstance(conf, dict):
        raise AirflowException(
            "manual DagRun queue_mode configuration must be a mapping"
        )
    queue_mode = conf.get("queue_mode", "deadline")
    if queue_mode not in state.REFRESH_QUEUE_MODES:
        raise AirflowException(
            "queue_mode must be one of " + ", ".join(sorted(state.REFRESH_QUEUE_MODES))
        )
    return queue_mode


def _capture_refresh_evidence(context, snapshot, rows, planned, stage, observed_at, capture_window=None):
    """Optional recorder of the SAME planning read; never issues another query.

    Enabling it and pinning the delivered SHA are separate source installation
    actions. Until then this function has no filesystem or collection effect.
    """
    directory = os.environ.get("SOFASCORE_REFRESH_EVIDENCE_DIR", "").strip()
    if not directory:
        return
    import hashlib
    from scrapers.sofascore.denominator import denominator_path, load_denominator
    from scrapers.sofascore.refresh_evidence import planning_snapshot, write_immutable

    revision = os.environ.get("SOFASCORE_EVIDENCE_CODE_REVISION", "").strip()
    if len(revision) != 40 or any(c not in "0123456789abcdef" for c in revision):
        raise AirflowException("refresh evidence needs a qualified delivered code revision")
    denominator = load_denominator()
    native = {(int(str(r[0])[3:]), int(r[5])): r for r in rows}
    selected = []
    for env in planned:
        key = int(env["SOFASCORE_TOURNAMENT_ID"]), int(env["SOFASCORE_SOURCE_SEASON_ID"])
        selected.append({"scope_key": env["SOFASCORE_SCOPE_KEY"],
                         "attempt_id": env.get("SOFASCORE_SCOPE_RUN_ID", env["SOFASCORE_SCOPE_KEY"]),
                         "tournament_id": key[0], "source_season_id": key[1],
                         "phase": "debt" if env.get("SOFASCORE_SCOPE_DEBT_ONLY") == "1"
                         or native[key][4] == 0 else "open", "environment": dict(env)})
    dag_run = context["dag_run"]
    identity = {"run_id": str(context["run_id"]), "code_revision": revision,
                "artifact_id": hashlib.sha256(Path(WORKLOAD_ARTIFACT).read_bytes()).hexdigest(),
                "denominator_sha256": hashlib.sha256(denominator_path().read_bytes()).hexdigest(),
                "snapshot_id": snapshot["snapshot_id"], "campaign_id": snapshot["campaign_id"],
                "logical_date": dag_run.logical_date.isoformat(),
                "started_at": dag_run.start_date.isoformat()}
    # #1751: the evidence keeps its six-column census shape.
    document = planning_snapshot(identity, [tuple(r[:6]) for r in rows], selected,
        core_ids=[k for k, r in denominator.rows.items() if r.queue_priority == 1],
        excluded_ids=[k for k, r in denominator.rows.items() if r.queue_priority == 0],
        disputed_ids=[k for k, r in denominator.rows.items() if r.queue_priority == 9],
        input_complete=True, observed_at=observed_at)
    document.update(stage=stage, task_try_number=int(getattr(context.get("ti"), "try_number", 1)))
    if capture_window is not None:
        document["capture_window"] = capture_window
    # The snapshot digest covers census/selection, while stage is its archive key.
    from scrapers.sofascore.refresh_evidence import _digest
    document["digest"] = _digest({k: v for k, v in document.items() if k != "digest"})
    run_key = hashlib.sha256(identity["run_id"].encode()).hexdigest()[:20]
    write_immutable(Path(directory) / run_key / f"try-{document['task_try_number']}-{stage}.json", document)


def _plan_refresh_batch(**context: Any) -> list[dict[str, Any]]:
    queue_mode = _refresh_queue_mode(context)
    snapshot = state.read_refresh_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    rows = _pending_refresh_partitions()
    observed_at = datetime.now(timezone.utc).isoformat()
    planned = state.plan_refresh_batch(
        snapshot,
        rows,
        batch_size=REFRESH_BATCH_SIZE,
        queue_mode=queue_mode,
        snapshot_path=SNAPSHOT_PATH,
        policy_path=POLICY_PATH,
        result_dir=RESULT_DIR,
        workload_artifact=WORKLOAD_ARTIFACT,
        dag_run_id=str(context.get("run_id") or "manual"),
        task_env=REFRESH_TASK_ENV,
        scope_budget_s=int(REFRESH_SCOPE_BUDGET.total_seconds()),
        seconds_per_match=REFRESH_SECONDS_PER_MATCH,
        # #2117: fresh debt keeps a quarter of the window under an open flood.
        debt_reserve_s=state.REFRESH_DEBT_RESERVE_SECONDS,
    )
    # The whole batch, retries included, must stop inside the window plus its
    # retry reserve: each task's own timeout restarts on a retry, so the scope
    # cycle stops at whichever deadline comes first (Astra #1358, finding 3).
    window_started = time.time()
    window_deadline = int(
        window_started
        + (REFRESH_SCOPE_BUDGET + REFRESH_RETRY_RESERVE).total_seconds()
    )
    for env in planned:
        env["SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH"] = str(window_deadline)
    # #2117: the reserve the planner applied (0 without fresh debt).
    debt_reserve_s = max(
        (int(env.get("SOFASCORE_REFRESH_DEBT_RESERVE_S", 0)) for env in planned),
        default=0,
    )
    _capture_refresh_evidence(context, snapshot, rows, planned, "initial", observed_at,
        capture_window={"started_at": datetime.fromtimestamp(window_started, timezone.utc).isoformat(),
                        "deadline_at": datetime.fromtimestamp(window_deadline, timezone.utc).isoformat(),
                        # The evidence contract lets the debt tier follow an
                        # unfinished open tier only in a plan that declares it.
                        "scope_budget_s": int(REFRESH_SCOPE_BUDGET.total_seconds()),
                        "debt_reserve_s": debt_reserve_s})
    # One mapped task per scope, each with the timeout of its OWN estimate
    # (``expand_kwargs`` below): the timeout is part of the mapped kwargs.
    return [
        {
            "env": env,
            "execution_timeout": timedelta(
                seconds=int(env["SOFASCORE_SCOPE_TIMEOUT_S"])
            ),
        }
        for env in planned
    ]


def _bronze_dq_report_passed(report: Any) -> bool:
    """#1371: a finished scope must ship a structurally clean Bronze DQ report."""

    checks = report.get("checks") if isinstance(report, dict) else None
    return (
        isinstance(report, dict)
        and report.get("status") == "success"
        and isinstance(checks, int)
        and not isinstance(checks, bool)
        and checks > 0
        and not report.get("violations")
        and not report.get("errors")
    )


def _validate_refresh_scope(
    env: dict[str, str], execution_timeout: Any = None, ti: Any = None
) -> dict[str, Any]:
    environment = env
    result_path = Path(environment["SOFASCORE_SCOPE_RESULT_PATH"])
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"scope result is unreadable: {exc}") from exc
    status = result.get("status")
    dq_report = result.get("bronze_dq")

    def _retain_bronze_dq() -> None:
        # #1371: a red validation still leaves its DQ evidence behind for the
        # window summary instead of dying with the task.
        if ti is not None and isinstance(dq_report, dict):
            ti.xcom_push(key="bronze_dq", value=dq_report)

    # #1751: the scope command lets a transport-only loss through on its last
    # try; it is not refreshed, the next run re-plans it.
    if status == "failed" and _transient_scope_failure(environment):
        return {"status": "transient_failure", "scope_key": environment["SOFASCORE_SCOPE_KEY"],
                "elapsed_s": result.get("elapsed_s"), "errors": result.get("errors")}
    # #1358: ``partial`` is a scope that stopped at its own time/byte/match
    # ceiling with the rest left for the next run — a finished task, not red.
    if status not in {"success", "partial"}:
        _retain_bronze_dq()
        raise AirflowException("scope cycle did not finish successfully")
    if environment.get("SOFASCORE_CAMPAIGN_ACTION") != "refresh":
        _retain_bronze_dq()
        raise AirflowException("unknown SofaScore campaign action")
    campaign_id, tournament_id, season_id = environment[
        "SOFASCORE_SCOPE_KEY"
    ].split(":")
    # The result's snapshot_id is the revision the scope actually ran on; the
    # metadata enrichment may have advanced it after planning (#1354), which
    # leaves the campaign identity and this exact scope intact.
    if (
        result.get("campaign_id") != campaign_id
        or int(result.get("tournament_id", 0)) != int(tournament_id)
        or int(result.get("source_season_id", 0)) != int(season_id)
    ):
        _retain_bronze_dq()
        raise AirflowException("scope result provenance mismatch")
    # #1371: a finished capture must prove its Bronze DQ — a failed or
    # malformed report is red here, and the mapped validator, which always
    # holds the task context, also refuses a result that ships no report at
    # all. A transport-only loss above never reaches this gate (#1751).
    if (isinstance(dq_report, dict) or ti is not None) and not _bronze_dq_report_passed(
        dq_report
    ):
        _retain_bronze_dq()
        raise AirflowException(
            "scope bronze data-quality report is failed, missing or malformed"
        )
    # A refreshed scope is never "completed": the next DagRun re-plans it
    # from Bronze evidence as new games finish.
    outcome: dict[str, Any] = {
        "status": "partial" if status == "partial" else "refreshed",
        "scope_key": environment["SOFASCORE_SCOPE_KEY"],
        "elapsed_s": result.get("elapsed_s"),
    }
    if isinstance(dq_report, dict):
        outcome["bronze_dq"] = dq_report
    if status == "partial":
        nested = result.get("partial")
        nested = nested if isinstance(nested, dict) else {}
        for field in ("remaining_matches", "stop_reason", "bytes"):
            outcome[field] = result.get(field, nested.get(field))
    return outcome


def _transient_scope_failure(env: dict[str, str]) -> bool:
    from utils.sofascore_refresh_window import transient_scope_failure

    return transient_scope_failure(env["SOFASCORE_SCOPE_RESULT_PATH"], env.get("SOFASCORE_SCOPE_RUN_ID", ""))


def _execute_refill_scope(env: dict[str, str]) -> dict[str, Any]:
    """Execute an independently signed continuation under the original deadline."""
    deadline = float(env["SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH"])
    remaining = min(float(env["SOFASCORE_SCOPE_TIMEOUT_S"]), deadline - time.time())
    if remaining <= 0:
        return {"status": "partial", "stop_reason": "time_budget", "elapsed_s": 0,
                "scope_key": env["SOFASCORE_SCOPE_KEY"]}
    command = [
        "/opt/legacy-scraper-venv/bin/python",
        "/opt/airflow/dags/scripts/run_sofascore_scope_cycle.py",
        "--snapshot", env["SOFASCORE_CAMPAIGN_SNAPSHOT"],
        "--tournament-id", env["SOFASCORE_TOURNAMENT_ID"],
        "--source-season-id", env["SOFASCORE_SOURCE_SEASON_ID"],
        "--expected-snapshot-id", env["SOFASCORE_EXPECTED_SNAPSHOT_ID"],
        "--expected-campaign-id", env["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
        "--phase", "matches", "--season-evidence", "bronze", "--allow-pending-season",
        "--registry-overlay",
        "--output-dir", env["SOFASCORE_SCOPE_OUTPUT_DIR"],
        "--output", env["SOFASCORE_SCOPE_RESULT_PATH"],
        "--workload-artifact", env["SOFASCORE_WORKLOAD_ARTIFACT"],
        "--run-id", env["SOFASCORE_SCOPE_RUN_ID"],
    ]
    started = time.monotonic()
    child = subprocess.Popen(
        command, cwd="/opt/airflow", start_new_session=True,
        env={**os.environ, **env, "AIRFLOW_CTX_DAG_ID": DAG_ID,
             "AIRFLOW_CTX_TASK_ID": "refill_refresh_window"},
    )
    try:
        code = child.wait(timeout=remaining)
    except BaseException:
        # Also clean up on Airflow SIGTERM/task timeout, not just child timeout.
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        finally:
            # The parent may exit on TERM while a browser grandchild ignores
            # it. Always reap the whole private group, not only a live parent.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()
        raise
    if code:
        return {"status": "transient_failure" if _transient_scope_failure(env) else "failed",
                "errors": [f"scope cycle exit_code={code}"],
                "elapsed_s": time.monotonic() - started,
                "scope_key": env["SOFASCORE_SCOPE_KEY"]}
    return _validate_refresh_scope(env)


def _refill_report_path(run_id: str) -> Path:
    safe_run = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in run_id)
    return Path(RESULT_DIR) / f"refill-{safe_run}.json"


def _failed_initial_report(context: dict[str, Any]) -> dict[str, Any]:
    """Retain paid attempts on a failed path without another census or refill."""
    import math
    from dags.scripts.run_sofascore_scope_cycle import _atomic_json

    run_id = str(context.get("run_id") or "manual")
    ti = context.get("ti")
    items = ti.xcom_pull(task_ids="plan_refresh_batch") if ti is not None else []
    environments = [item["env"] for item in (items or [])]
    expected = {env.get("SOFASCORE_SCOPE_RUN_ID"): env for env in environments}
    attempts = {}
    gaps = []
    directory = os.environ.get("SOFASCORE_REFRESH_EVIDENCE_DIR", "").strip()
    paths = sorted((Path(directory) / "scope-attempts").glob("*.json")) if directory else []
    # Latest output is a fallback only. It may overwrite a physical retry;
    # never describe it as a complete retry archive.
    if not directory:
        gaps.append("physical_attempt_archive_disabled")
    latest_paths = {Path(env["SOFASCORE_SCOPE_RESULT_PATH"]) for env in environments
                    if env.get("SOFASCORE_SCOPE_RESULT_PATH")}
    paths.extend(sorted(latest_paths))
    for path in paths:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            if path in latest_paths:
                gaps.append(f"unreadable_scope_result:{path}")
            continue
        env = expected.get(value.get("run_id")) if isinstance(value, dict) else None
        if env is None:
            continue
        identity = ":".join(str(value.get(key)) for key in
                            ("campaign_id", "tournament_id", "source_season_id"))
        if identity != env.get("SOFASCORE_SCOPE_KEY"):
            gaps.append(f"scope_provenance_mismatch:{path}")
            continue
        elapsed = value.get("elapsed_s")
        if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed) or elapsed < 0:
            gaps.append(f"invalid_elapsed:{path}")
            continue
        key = (value["run_id"], value.get("started_at"))
        attempts[key] = {**value, "scope_key": identity}
    seen = {value["run_id"] for value in attempts.values()}
    gaps.extend(f"missing_scope_attempt:{run}" for run in expected if run not in seen)
    if not environments:
        gaps.append("initial_plan_unavailable")
    report = {"run_id": run_id, "status": "partial", "stop_reason": "upstream_failure",
              "outcomes": [], "initial_outcomes": list(attempts.values()),
              "expected_scope_run_ids": list(expected), "evidence_gaps": sorted(set(gaps)),
              "final_census_status": "unavailable_upstream_failure",
              "window_budget_s": REFRESH_SCOPE_BUDGET.total_seconds()}
    _atomic_json(_refill_report_path(run_id), report)
    if directory:
        import hashlib
        from scrapers.sofascore.refresh_evidence import write_immutable
        run_key = hashlib.sha256(run_id.encode()).hexdigest()[:20]
        number = int(getattr(ti, "try_number", 1))
        write_immutable(Path(directory) / run_key / f"try-{number}-failed-initial.json", report)
    return report


def _refill_refresh_window(**context: Any) -> dict[str, Any]:
    task_started = time.time()
    from utils.sofascore_refresh_window import refill_deadline, refill_window
    from dags.scripts.run_sofascore_scope_cycle import _atomic_json

    dag_run = context["dag_run"]
    initial_tasks = REFRESH_TASK_IDS - {"refill_refresh_window"}
    if any(ti.task_id in initial_tasks and _task_state(ti) in {"failed", "upstream_failed"}
           for ti in dag_run.get_task_instances()):
        return _failed_initial_report(context)
    items = context["ti"].xcom_pull(task_ids="plan_refresh_batch") or []
    if not items:
        return {"status": "success", "stop_reason": "empty_initial_plan", "outcomes": []}
    outcomes = [_validate_refresh_scope(item["env"]) for item in items]
    deadline = min(float(item["env"]["SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH"])
                   for item in items)
    deadline = refill_deadline(deadline, getattr(dag_run, "start_date", None))
    deadline = min(deadline, task_started + (REFILL_TIMEOUT - REFILL_CENSUS_RESERVE).total_seconds())
    run_id = str(context.get("run_id") or "manual")
    queue_mode = _refresh_queue_mode(context)
    report_path = _refill_report_path(run_id)
    # A manual retry/clear must not silently recreate round 1 with changed
    # Bronze evidence under its already signed identity.
    if report_path.exists():
        raise AirflowException("refill already attempted for this run; wait for the next scheduled run")

    pending_state = {"number": 0, "rows": None, "observed_at": None, "snapshot": None}

    def pending():
        rows = _pending_refresh_partitions()
        pending_state.update(number=pending_state["number"] + 1, rows=rows,
                             observed_at=datetime.now(timezone.utc).isoformat(),
                             snapshot=state.read_refresh_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH))
        _capture_refresh_evidence(context, pending_state["snapshot"], rows, [],
            f"pending-{pending_state['number']}", pending_state["observed_at"])
        return rows

    def plan(rows, budget, round_number):
        snapshot = state.read_refresh_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
        planned = state.plan_refresh_batch(
            snapshot, rows, batch_size=1, queue_mode=queue_mode,
            snapshot_path=SNAPSHOT_PATH, policy_path=POLICY_PATH,
            result_dir=RESULT_DIR, workload_artifact=WORKLOAD_ARTIFACT,
            dag_run_id=f"{run_id}:refill:{round_number}", task_env=REFRESH_TASK_ENV,
            scope_budget_s=max(1, int(budget)), seconds_per_match=REFRESH_SECONDS_PER_MATCH,
        )
        _capture_refresh_evidence(context, snapshot, pending_state["rows"] or [], planned,
            f"refill-{round_number}", pending_state["observed_at"])
        if not planned:
            return None
        env = planned[0]
        env["SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH"] = str(int(deadline))
        # Active allowance, too, includes the execution itself, not only its estimate.
        env["SOFASCORE_SCOPE_TIMEOUT_S"] = str(min(int(env["SOFASCORE_SCOPE_TIMEOUT_S"]), int(budget)))
        return env

    _atomic_json(report_path, {"status": "running", "outcomes": [], "run_id": run_id})
    try:
        report = refill_window(
            initial_elapsed_s=sum(float(item["elapsed_s"]) for item in outcomes),
            deadline_epoch=deadline, pending=pending if os.environ.get("SOFASCORE_REFRESH_EVIDENCE_DIR", "").strip()
            else _pending_refresh_partitions,
            plan=plan, execute=_execute_refill_scope,
            budget_s=REFRESH_SCOPE_BUDGET.total_seconds(), now=time.time,
            max_scope_retries=REFILL_SCOPE_RETRIES,
            retry_pause_s=REFRESH_SCOPE_RETRY_DELAY.total_seconds(), sleep=time.sleep,
            persist=lambda value: _atomic_json(report_path, {**value, "run_id": run_id}),
        )
    # #2134: also Airflow's task timeout, which is not an Exception.
    except BaseException as exc:
        previous = json.loads(report_path.read_text())
        _atomic_json(report_path, {**previous, "status": "failed", "errors": [str(exc)]})
        raise
    if report["status"] == "failed":
        raise AirflowException(f"refresh refill failed; see {report_path}")
    if pending_state["rows"] is not None:
        _capture_refresh_evidence(context, pending_state["snapshot"], pending_state["rows"], [],
            "final", pending_state["observed_at"])
    return report


def _enrich_season_metadata(**context: Any) -> dict[str, Any]:
    if not METADATA_SEASONS_PER_RUN:
        raise AirflowSkipException("SOFASCORE_METADATA_SEASONS_PER_RUN is 0")
    snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    run_id = str(context.get("run_id") or "manual")
    result_dir = Path(METADATA_RESULT_DIR)
    result_dir.mkdir(parents=True, exist_ok=True)
    report_path = result_dir / (
        "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in run_id)
        + ".json"
    )
    command = [
        sys.executable, ENRICH_SCRIPT,
        "--snapshot", SNAPSHOT_PATH,
        "--policy", POLICY_PATH,
        "--output", SNAPSHOT_PATH,
        "--select", "priority",
        "--max-seasons", str(METADATA_SEASONS_PER_RUN),
        "--expected-snapshot-id", str(snapshot["snapshot_id"]),
        "--dag-id", DAG_ID,
        # A logical run of its own: the gateway meters discovery bytes per
        # (dag_id, run_id), and the schedule sweep of this DagRun may already
        # have spent that cap.  The UTC-day cap of the lane stays shared.
        "--run-id", f"{run_id}:metadata",
        "--task-id", "enrich_season_metadata",
        "--budget-cap-bytes", str(METADATA_BUDGET_BYTES),
        "--report", str(report_path),
    ]
    env = {
        **os.environ,
        **REFRESH_TASK_ENV,
        "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
    }
    completed = subprocess.run(command, env=env, cwd="/opt/airflow", check=False)
    if completed.returncode != 0:
        raise AirflowException(
            f"season metadata enrichment failed (exit {completed.returncode}); "
            f"report: {report_path}"
        )
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"metadata report is unreadable: {exc}") from exc
    traffic = report.get("traffic") or {}
    return {
        "campaign_id": snapshot.get("campaign_id"),
        "ready_wave_scopes": report.get("ready_wave_scopes"),
        "source_requests": report.get("source_requests"),
        "paid_proxy_bytes": traffic.get("paid_proxy_bytes"),
    }


def _task_state(task_instance: Any) -> str:
    value = getattr(task_instance.state, "value", task_instance.state)
    return str(value or "none").casefold().split(".")[-1]


def _propagate_status(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise AirflowException("refresh watcher has no DagRun")
    failures = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id not in REFRESH_TASK_IDS:
            continue
        if _task_state(task_instance) in {"failed", "upstream_failed"}:
            failures.append(task_instance.task_id)
    summary = _window_summary(context)
    print(f"SofaScore refresh window: {json.dumps(summary, sort_keys=True)}")
    # One provider blip costs a scope; this many means the transport is down.
    if summary["initial_transient_failures"] >= MAX_INITIAL_TRANSIENT_FAILURES:
        failures.append(f"run_refresh_scope ({summary['initial_transient_failures']} transport failures)")
    if failures:
        raise AirflowException(
            "SofaScore refresh attempt failed: " + ", ".join(sorted(failures))
        )
    return {"status": "success", **summary}


def _window_summary(
    context: dict[str, Any] | None = None, **keywords: Any
) -> dict[str, Any]:
    """#1358: how many scopes ended partial and how much of the window went."""

    if context is None:
        context = keywords
    elif keywords:
        context = {**context, **keywords}
    outcomes: list[Any] = []
    dq_reports: list[Any] = []
    refill_reason = None
    refill = None
    season_fields = None
    ti = context.get("ti")
    if ti is not None:
        try:
            pulled = ti.xcom_pull(task_ids="validate_refresh_scope")
        except Exception as exc:  # the verdict above must not hinge on it
            print(f"refresh window summary unavailable: {exc}")
            pulled = None
        if isinstance(pulled, dict):
            pulled = [pulled]
        elif not isinstance(pulled, list):
            pulled = []
        outcomes = [item for item in pulled if isinstance(item, dict)]
        try:
            # #1371: failed validators push their DQ report instead of
            # returning it; read it alongside the return values.
            explicit_dq = ti.xcom_pull(
                task_ids="validate_refresh_scope", key="bronze_dq"
            )
        except Exception as exc:
            print(f"refresh bronze DQ summary unavailable: {exc}")
            explicit_dq = None
        if isinstance(explicit_dq, dict):
            explicit_dq = [explicit_dq]
        elif not isinstance(explicit_dq, list):
            explicit_dq = []
        # Align the two forms by mapped index so one scope's report is never
        # counted twice, while a report that exists in only one of them —
        # typically a failed validator's — still reaches the summary.
        for index, item in enumerate(pulled):
            report = item.get("bronze_dq") if isinstance(item, dict) else None
            if not isinstance(report, dict) and index < len(explicit_dq):
                report = explicit_dq[index]
            if isinstance(report, dict):
                dq_reports.append(report)
        dq_reports.extend(
            report for report in explicit_dq[len(pulled):]
            if isinstance(report, dict)
        )
        try:
            refill = ti.xcom_pull(task_ids="refill_refresh_window")
        except Exception as exc:
            print(f"refresh refill summary unavailable: {exc}")
        try:
            # #1368: the seasonal renewal's own line — its post-run census
            # ratio, not a claim about this window's match work.
            season_fields = ti.xcom_pull(task_ids="refresh_season_fields")
        except Exception as exc:
            print(f"season fields summary unavailable: {exc}")
            season_fields = None
    # Failed tasks do not return an XCom; their persisted report still records
    # spent time. Only accept this run's report, including after filename sanitization.
    run_id = context.get("run_id")
    if not isinstance(refill, dict) and run_id:
        try:
            persisted = json.loads(_refill_report_path(str(run_id)).read_text())
        except FileNotFoundError:
            pass
        except (OSError, ValueError) as exc:
            print(f"refresh refill report unavailable: {exc}")
        else:
            if isinstance(persisted, dict) and persisted.get("run_id") == str(run_id):
                refill = persisted
    initial_transient = sum(1 for item in outcomes if item.get("status") == "transient_failure")
    if isinstance(refill, dict):
        if "initial_outcomes" in refill:
            # A failed mapped validation may yield no XCom at all. Physical
            # attempt receipts retain retries and replace, rather than double
            # count, the successful mapped-task XComs; their DQ reports ride
            # the same replacement (#1371).
            outcomes = list(refill["initial_outcomes"])
            dq_reports = [
                item["bronze_dq"] for item in outcomes
                if isinstance(item, dict) and isinstance(item.get("bronze_dq"), dict)
            ]
        refill_outcomes = refill.get("outcomes") or []
        outcomes.extend(refill_outcomes)
        dq_reports.extend(
            item["bronze_dq"] for item in refill_outcomes
            if isinstance(item, dict) and isinstance(item.get("bronze_dq"), dict)
        )
        refill_reason = refill.get("stop_reason")
    used = sum(
        float(item.get("elapsed_s") or 0)
        for item in outcomes
    )
    budget = REFRESH_SCOPE_BUDGET.total_seconds()
    return {
        "scopes": len(outcomes),
        "partial": sum(1 for item in outcomes if item.get("status") == "partial"),
        "partial_scopes": [
            {
                key: item.get(key)
                for key in ("scope_key", "stop_reason", "remaining_matches")
            }
            for item in outcomes
            if item.get("status") == "partial"
        ],
        "window_used_s": round(used, 1),
        "window_budget_s": budget,
        "window_use": round(used / budget, 3),
        "refill_stop_reason": refill_reason,
        "initial_transient_failures": initial_transient,
        # #1371: the run's merged Bronze DQ verdict. No reports at all is
        # "empty" (zero checks), never a data-quality PASS.
        "bronze_dq": summarize_bronze_dq_reports(dq_reports),
        # #1368: the freshness line of the seasonal renewal (допущение а) —
        # present only when the fields task ran and reported.
        **({"season_fields": season_fields} if isinstance(season_fields, dict) else {}),
    }


FETCH_COMMAND = """
set -euo pipefail
cd /opt/airflow
page_budget="${SOFASCORE_REFRESH_PAGE_BUDGET}"
run_limit=""
if [ "{{ ti.try_number }}" -gt 1 ]; then
  # #2107: the retry walks only what is left of the sweep window counted from
  # the DagRun start, so the scopes keep theirs: pages cut to that share and a
  # hard stop at its end.
  left=$(( {{ dag_run.start_date.timestamp() | int }} + SOFASCORE_REFRESH_FETCH_WINDOW_S - $(date +%s) ))
  if [ "${left}" -lt 600 ]; then
    echo "SofaScore sweep retry: ${left}s left of the sweep window, too little to walk (#2107)"
    exit 1
  fi
  page_budget=$(( SOFASCORE_REFRESH_PAGE_BUDGET * left / SOFASCORE_REFRESH_FETCH_WINDOW_S ))
  run_limit="timeout ${left}"
fi
${run_limit} /opt/legacy-scraper-venv/bin/python \
  dags/scripts/run_sofascore_schedule_refresh.py \
  --snapshot "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
  --budget-cap-bytes "${SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES}" \
  --page-budget "${page_budget}" \
  --max-due "${SOFASCORE_REFRESH_MAX_DUE}" \
  --max-stale "${SOFASCORE_REFRESH_MAX_STALE}" \
  --chase-pages "${SOFASCORE_REFRESH_CHASE_PAGES}" \
  --max-seed "${SOFASCORE_REFRESH_MAX_SEED}" \
  --seed-pages "${SOFASCORE_REFRESH_SEED_PAGES}" \
  --window-hours "${SOFASCORE_REFRESH_WINDOW_HOURS}" \
  --per-lease-max-bytes "${SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES}" \
  --cursor "${SOFASCORE_REFRESH_RESULT_DIR}/schedule-sweep-cursor.json" \
  --incomplete "${SOFASCORE_REFRESH_RESULT_DIR}/schedule-sweep-incomplete.json" \
  --dag-id "${AIRFLOW_CTX_DAG_ID}" \
  --run-id "${AIRFLOW_CTX_DAG_RUN_ID}" \
  --task-id "${AIRFLOW_CTX_TASK_ID}" \
  --output "${SOFASCORE_REFRESH_RESULT_DIR}/schedule-refresh-{{ ts_nodash }}{{ '-try%d' % ti.try_number if ti.try_number > 1 else '' }}.json"
"""

RUN_SCOPE_COMMAND = """
set -euo pipefail
cd /opt/airflow
# #2118: the third try (REFRESH_SCOPE_RETRIES) is only for a scope whose
# previous try lost only its transport; any other failure stays red at once.
# It is the last try of the round (a clear keeps try_number and sets
# max_tries = try_number + retries) and never try 2 (TIs planned before
# #2118 keep max_tries=1).  Past the window deadline it would only overwrite
# that transport result with a time-budget stop, so the scope waits for the
# next run as on #1751.  Accepted residue: a killed second try leaves the
# first try's result behind, which grants one extra try inside the window.
if [ "{{ ti.try_number }}" -gt "{{ ti.max_tries }}" ] && [ "{{ ti.try_number }}" -gt 2 ]; then
  if ! /opt/legacy-scraper-venv/bin/python -c 'import sys; sys.path.insert(0, "dags"); from utils.sofascore_refresh_window import transient_scope_failure as t; sys.exit(0 if t(sys.argv[1], sys.argv[2]) else 1)' \
      "${SOFASCORE_SCOPE_RESULT_PATH}" "${SOFASCORE_SCOPE_RUN_ID}"; then
    echo "SofaScore scope did not fail on transport only; no third try (#2118)"
    exit 1
  fi
  if [ "$(date +%s)" -ge "${SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH}" ]; then
    echo "SofaScore scope lost only its transport and the window is over; it waits for the next run (#1751, #2118)"
    exit 0
  fi
fi
code=0
started=$(date +%s)
/opt/legacy-scraper-venv/bin/python \
  dags/scripts/run_sofascore_scope_cycle.py \
  --snapshot "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
  --tournament-id "${SOFASCORE_TOURNAMENT_ID}" \
  --source-season-id "${SOFASCORE_SOURCE_SEASON_ID}" \
  --expected-snapshot-id "${SOFASCORE_EXPECTED_SNAPSHOT_ID}" \
  --expected-campaign-id "${SOFASCORE_EXPECTED_CAMPAIGN_ID}" \
  --phase matches \
  --season-evidence bronze \
  --allow-pending-season \
  --registry-overlay \
  --output-dir "${SOFASCORE_SCOPE_OUTPUT_DIR}" \
  --output "${SOFASCORE_SCOPE_RESULT_PATH}" \
  --workload-artifact "${SOFASCORE_WORKLOAD_ARTIFACT}" \
  --run-id "${SOFASCORE_SCOPE_RUN_ID}" || code=$?
# #1751: a transport-only loss (provider 502/522, captcha) with exact byte
# accounting fails the first try so Airflow retries it; on the last try the
# scope waits for the next run instead of failing the whole DagRun.
if [ "${code}" -ne 0 ] && [ "{{ ti.try_number }}" -gt "{{ ti.max_tries }}" ] \
    && /opt/legacy-scraper-venv/bin/python -c 'import sys; sys.path.insert(0, "dags"); from utils.sofascore_refresh_window import transient_scope_failure as t; sys.exit(0 if t(sys.argv[1], sys.argv[2], float(sys.argv[3])) else 1)' \
        "${SOFASCORE_SCOPE_RESULT_PATH}" "${SOFASCORE_SCOPE_RUN_ID}" "${started}"; then
  echo "SofaScore scope lost only its transport on the last try; it waits for the next run (#1751)"
  exit 0
fi
exit "${code}"
"""


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Current season of every adult-men SofaScore tournament, 3x/day",
    # After the European evening, in the morning, after the daily ingest.
    schedule="30 0,8,15 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=REFRESH_MAX_ACTIVE_TASKS,
    is_paused_upon_creation=True,
    # Runs are 8 h apart: the window has to fit the sweep plus the
    # serial match batch (one pool slot) without ever overlapping the next.
    dagrun_timeout=REFRESH_DAGRUN_TIMEOUT,
    # No native rendering: ``env`` is a template field, and a native render turns
    # the numeric budget string back into an int, which Popen refuses to encode.
    tags=["sofascore", "refresh", "all-men"],
) as dag:
    fetch = BashOperator(
        task_id="refresh_season_schedules",
        bash_command=FETCH_COMMAND,
        env={
            **REFRESH_TASK_ENV,
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "SOFASCORE_CAMPAIGN_SNAPSHOT": SNAPSHOT_PATH,
            "SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES": str(
                REFRESH_DISCOVERY_BUDGET_BYTES
            ),
            "SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES": str(REFRESH_PER_LEASE_MAX_BYTES),
            "SOFASCORE_REFRESH_PAGE_BUDGET": str(REFRESH_PAGE_BUDGET),
            "SOFASCORE_REFRESH_FETCH_WINDOW_S": str(
                int(REFRESH_FETCH_TIMEOUT.total_seconds())
            ),
            "SOFASCORE_REFRESH_MAX_DUE": str(REFRESH_MAX_DUE),
            "SOFASCORE_REFRESH_MAX_STALE": str(REFRESH_MAX_STALE),
            "SOFASCORE_REFRESH_CHASE_PAGES": str(REFRESH_CHASE_PAGES),
            "SOFASCORE_REFRESH_MAX_SEED": str(REFRESH_MAX_SEED),
            "SOFASCORE_REFRESH_SEED_PAGES": str(REFRESH_SEED_PAGES),
            "SOFASCORE_REFRESH_WINDOW_HOURS": str(REFRESH_WINDOW_HOURS),
            "SOFASCORE_REFRESH_RESULT_DIR": RESULT_DIR,
        },
        append_env=True,
        pool=REFRESH_POOL,
        priority_weight=5,
        do_xcom_push=False,
        # #2107: ONE retry, 10 min later.  A short contour failure (lease
        # meter, a lease without an exit) used to cost the whole 8 h until the
        # next run.  Every class commits its own cursor and unfinished chains
        # as it finishes, so the retry resumes where the first try stopped.
        # Lesson #7 (#1044: retries ate the DagRun budget) does not bite here:
        # the gateway gives discovery leases 64 MiB per DagRun and a full sweep
        # costs <= 10 MB.  ``plan_refresh_batch`` (all_done) waits for the
        # retry; its report gets a ``-tryN`` suffix so the first one survives.
        # The retry fits the first try's window (FETCH_COMMAND), not its own.
        retries=1,
        retry_delay=REFRESH_FETCH_RETRY_DELAY,
        # The class slices are cut to REFRESH_PAGE_BUDGET worst-case pages
        # (the fixture page of every stale and seeded season and the step-back
        # allowance count too): at the lane's 20/min that is 80 % of this
        # window, the rest is room for the source being slow (#1358).
        execution_timeout=REFRESH_FETCH_TIMEOUT,
    )
    plan = PythonOperator(
        task_id="plan_refresh_batch",
        python_callable=_plan_refresh_batch,
        # Existing Bronze schedules remain usable after a failed sweep.
        trigger_rule="all_done",
        retries=0,
    )
    run = BashOperator.partial(
        task_id="run_refresh_scope",
        bash_command=RUN_SCOPE_COMMAND,
        append_env=True,
        pool=REFRESH_POOL,
        # Current partitions run before historical fallback, above the history
        # lane (1) in the shared pool.
        priority_weight=5,
        do_xcom_push=False,
        max_active_tis_per_dag=REFRESH_MAX_ACTIVE_TASKS,
        # Retries keep the same run_id: the gateway reuses the signed plan
        # and a latched lease is re-claimed after the reaper grace.  The
        # third try runs only after a transport-only loss (#2118).
        retries=REFRESH_SCOPE_RETRIES,
        retry_delay=REFRESH_SCOPE_RETRY_DELAY,
        # ``execution_timeout`` comes from each item of the plan (#1358):
        # the scope's own estimate x 1.5, at most REFRESH_SCOPE_TIMEOUT.
        # ``strict=False`` lets the mapped value replace the default_args one.
    ).expand_kwargs(plan.output, strict=False)
    validate = PythonOperator.partial(
        task_id="validate_refresh_scope",
        python_callable=_validate_refresh_scope,
        retries=0,
    ).expand(op_kwargs=plan.output)
    refill = PythonOperator(
        task_id="refill_refresh_window", python_callable=_refill_refresh_window,
        trigger_rule="all_done", pool=REFRESH_POOL, priority_weight=5,
        retries=0, execution_timeout=REFILL_TIMEOUT,
    )
    season_fields = PythonOperator(
        task_id="refresh_season_fields",
        python_callable=_refresh_season_fields,
        # The renewal works whatever the match lane did; its reservation is
        # SEASON_FIELDS_TIMEOUT minutes of the same serial window (#1368).
        trigger_rule="all_done",
        pool=REFRESH_POOL,
        priority_weight=1,
        # NO retry: a re-run would re-census and re-walk the same paid
        # selection on the run's remaining time; the next scheduled run
        # continues from the backoff state.
        retries=0,
        execution_timeout=SEASON_FIELDS_TIMEOUT,
    )
    enrich = PythonOperator(
        task_id="enrich_season_metadata",
        python_callable=_enrich_season_metadata,
        # Runs whatever the refresh work did: metadata is independent of it.
        trigger_rule="all_done",
        pool=REFRESH_POOL,
        # Below the refresh scopes (5) in the shared pool.
        priority_weight=1,
        # NO retry, like the schedule sweep above: a second attempt of the same
        # DagRun would walk the same paid selection again on what is left of
        # the run budget; the next scheduled run continues from the snapshot.
        retries=0,
        execution_timeout=METADATA_TIMEOUT,
    )
    propagate = PythonOperator(
        task_id="propagate_refresh_status",
        python_callable=_propagate_status,
        trigger_rule="all_done",
        retries=0,
    )

    fetch >> plan >> run >> validate >> enrich >> propagate
    run >> refill
    validate >> refill
    run >> enrich
    refill >> enrich
    # #1368: the seasonal renewal runs after the match work and the status
    # propagation waits for it alongside the metadata enrichment.
    validate >> season_fields
    refill >> season_fields
    season_fields >> propagate


__all__ = ["DAG_ID", "dag"]
