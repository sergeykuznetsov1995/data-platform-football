"""Player profiles of every closed adult-men SofaScore scope (lane P, #1374).

Four times a day the lane takes tournament-seasons the history lane has
already CLOSED — every match committed to Bronze, which is exactly what the
players phase of the scope cycle demands — and buys their player profiles on
the lane's own gateway (``sofascore_gw_players``) and its own Airflow pool
(``sofascore_players_pool``).  The queue is the history lane's ``state.json``,
read-only here; the lane keeps its own ``players-state.json`` and
``players-failures.json`` next to it, so a scope parked by the history lane
is not parked here and vice versa.

The registry leagues of the daily ingest are NOT excluded by a knob: the
denominator (#1353) decides — a ``queue_priority 0`` tournament is never
planned and every core scope of every year ranks before any disputed one
(``state.plan_players_batch``).

The cadence is spread so one run rarely meets the next; delivery is dynamic
now and handled outside this DAG, so no fixed delivery window is wired here.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils import sofascore_all_mens_state as state
from utils.default_args import DEFAULT_ARGS


DAG_ID = "dag_players_sofascore_all_mens"
SNAPSHOT_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_SNAPSHOT",
    "/opt/airflow/runtime/sofascore/all-men/snapshot.json",
)
POLICY_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_POLICY",
    "/opt/airflow/configs/sofascore/all_mens_campaign.json",
)
# The history lane's memory is the QUEUE of this lane, read-only here.
HISTORY_STATE_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_STATE",
    "/opt/airflow/runtime/sofascore/all-men/state.json",
)
STATE_PATH = os.environ.get(
    "SOFASCORE_PLAYERS_STATE",
    "/opt/airflow/runtime/sofascore/all-men/players-state.json",
)
FAILURES_PATH = str(Path(STATE_PATH).with_name("players-failures.json"))
RESULT_DIR = (
    os.environ.get("SOFASCORE_PLAYERS_RESULT_DIR", "").strip()
    or state.DEFAULT_PLAYERS_RESULT_DIR
)
WORKLOAD_ARTIFACT = os.environ.get(
    "SOFASCORE_PROXY_BUDGET_ARTIFACT",
    "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
)

# The lane's own timings.  Runs start at 07:00, 12:00, 17:00 and 22:00 UTC and
# may last 4.5 h; the old fixed 03:30-06:00 delivery-window avoidance is gone
# (delivery is dynamic now), so the cadence only has to keep consecutive runs
# from overlapping each other.
PLAYERS_SCHEDULE = "0 7,12,17,22 * * *"
PLAYERS_DAGRUN_TIMEOUT = timedelta(hours=4, minutes=30)
# The batch runs SERIALLY on one pool slot, and a scope may be attempted twice
# (``retries=1``), so the window has to hold BOTH attempts (lesson of Sol
# round 12 on the refresh lane: counting one attempt made the arithmetic
# decorative).  Live measurement 2026-09-04: the largest league took 34 min
# end to end; campaign tournaments are smaller.  40 min, not 45: at 45 the
# worst case (3 scopes x 2 attempts) spends the whole 270-minute DagRun and
# ``validate`` and ``finalize`` never run, so a scope whose capture SUCCEEDED
# is not booked and gets bought again next run.  40 leaves 30 min of slack
# and keeps PLAYERS_BATCH_FITS at 3.
PLAYERS_SCOPE_TIMEOUT = timedelta(minutes=40)
PLAYERS_SCOPE_ATTEMPTS = 2
# What validate/finalize may keep of the window in the worst case: a batch
# that eats it books nothing and re-buys its scopes next run.
PLAYERS_BOOKING_RESERVE = timedelta(minutes=20)
PLAYERS_BATCH_FITS = int(
    PLAYERS_DAGRUN_TIMEOUT / (PLAYERS_SCOPE_TIMEOUT * PLAYERS_SCOPE_ATTEMPTS)
)
PLAYERS_BATCH_SIZE = min(
    state.env_int(
        "SOFASCORE_PLAYERS_BATCH_SIZE", state.DEFAULT_PLAYERS_BATCH_SIZE, 1, 64
    ),
    PLAYERS_BATCH_FITS,
)
if (
    PLAYERS_DAGRUN_TIMEOUT
    - PLAYERS_BATCH_SIZE * PLAYERS_SCOPE_TIMEOUT * PLAYERS_SCOPE_ATTEMPTS
    < PLAYERS_BOOKING_RESERVE
):
    raise AirflowException(
        "players window does not keep the booking reserve: batch x timeout x "
        "attempts leaves less than "
        f"{PLAYERS_BOOKING_RESERVE} for validate and finalize"
    )
# A literal default, NOT ``INGEST_SCRAPER_POOL``: a silent fall back into the
# shared pool would bring back exactly the 429 ``concurrency_limited`` storm
# the three lanes of #1244 were separated to end.  An empty override is not
# an invitation to share the daily ingest's pool either.
PLAYERS_POOL = (
    os.environ.get("SOFASCORE_PLAYERS_POOL", "").strip() or "sofascore_players_pool"
)
PLAYERS_MAX_ACTIVE_TASKS = state.env_int(
    "SOFASCORE_PLAYERS_MAX_ACTIVE_TASKS", 1, 1, 16
)
PLAYERS_MAX_SCOPE_ATTEMPTS = state.env_int(
    "SOFASCORE_PLAYERS_MAX_SCOPE_ATTEMPTS", state.DEFAULT_MAX_SCOPE_ATTEMPTS, 1, 100
)
# Forwarded into every planned task.  The freshness key lives HERE and never in
# the scheduler environment: there it would freeze the daily ingest's weekly
# profile rotation.  "final" is the campaign's key for a closed season, so a
# retry or a resumed scope replays from raw for free instead of paying twice.
PLAYERS_TASK_ENV: dict[str, str] = {"SOFASCORE_PLAYER_FRESHNESS_KEY": "final"}
_control_url = os.environ.get("SOFASCORE_PLAYERS_PROXY_CONTROL_URL", "").strip()
if _control_url:
    PLAYERS_TASK_ENV["SOFASCORE_PROXY_CONTROL_URL"] = _control_url
PLAYERS_TASK_IDS = frozenset({
    "plan_players_batch",
    "run_players_scope",
    "validate_players_scope",
    "finalize_players_run",
})
# ``run_sofascore_scraper`` stamps this fallback reason into players.json when
# a season simply has not started; kept as a literal because importing the
# runner at DAG parse would drag the whole scraper stack in.  The scope cycle
# stamps the same text as its waiting reason.
UNSTARTED_SEASON_REASON = "season_has_no_finished_matches"


def _plan_players_batch(**context: Any) -> list[dict[str, str]]:
    snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    campaign_id = str(snapshot.get("campaign_id") or "")
    # No ``exclude_tournament_ids``: the denominator decides which tournaments
    # belong to the daily ingest and which queue tier the rest sit in.
    return state.plan_players_batch(
        snapshot,
        completed=state.read_completed(HISTORY_STATE_PATH, campaign_id=campaign_id),
        players_completed=state.read_completed(STATE_PATH, campaign_id=campaign_id),
        failures=state.read_failures(FAILURES_PATH, campaign_id=campaign_id),
        max_scope_attempts=PLAYERS_MAX_SCOPE_ATTEMPTS,
        batch_size=PLAYERS_BATCH_SIZE,
        snapshot_path=SNAPSHOT_PATH,
        policy_path=POLICY_PATH,
        result_dir=RESULT_DIR,
        workload_artifact=WORKLOAD_ARTIFACT,
        dag_run_id=str(context.get("run_id") or "manual"),
        task_env=PLAYERS_TASK_ENV,
    )


def _validate_players_scope(**environment: str) -> dict[str, Any]:
    result_path = Path(environment["SOFASCORE_SCOPE_RESULT_PATH"])
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"scope result is unreadable: {exc}") from exc
    if environment.get("SOFASCORE_CAMPAIGN_ACTION") != "players":
        raise AirflowException("unknown SofaScore campaign action")
    campaign_id, tournament_id, season_id = environment[
        "SOFASCORE_SCOPE_KEY"
    ].split(":")
    # The result's snapshot_id is the revision the scope actually ran on; the
    # refresh lane's metadata enrichment may have advanced it after planning
    # (#1354), which leaves the campaign identity and this exact scope intact.
    # The RUN identity is strict: the gateway holds one immutable signed plan
    # per run_id, so a result of another attempt must never book this one.
    if (
        result.get("campaign_id") != campaign_id
        or int(result.get("tournament_id", 0)) != int(tournament_id)
        or int(result.get("source_season_id", 0)) != int(season_id)
        or result.get("run_id") != environment.get("SOFASCORE_SCOPE_RUN_ID")
    ):
        raise AirflowException("scope result provenance mismatch")
    if result.get("status") == "deferred":
        # The match evidence of this scope is not all in Bronze yet.  No paid
        # traffic happened (the runner defers before planning), so the task
        # stays green.  A routine evidence wait is not an incident: the branch
        # opts out of the no-traffic streak, so the scope is never
        # quarantined until a new release — the failure memory only counts
        # attempts and parks the scope for a cooldown window, which brings it
        # back to the head of the queue on its own.
        state.mark_failed(
            FAILURES_PATH,
            campaign_id=campaign_id,
            scope_key=environment["SOFASCORE_SCOPE_KEY"],
            run_id=str(result.get("run_id") or "manual"),
            reason=str(result.get("deferral_reason") or ""),
            source_requests=0,
            release=state.current_release(),
            quarantine_on_no_traffic=False,
        )
        return {
            "status": "deferred",
            "scope_key": environment["SOFASCORE_SCOPE_KEY"],
            "reason": str(result.get("deferral_reason") or ""),
        }
    if result.get("status") != "success":
        raise AirflowException("scope cycle did not finish successfully")
    players_path = (
        Path(environment["SOFASCORE_SCOPE_OUTPUT_DIR"]) / "players.json"
    )
    try:
        players = json.loads(players_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"players result is unreadable: {exc}") from exc
    # "Green but empty" guard on the UNIVERSE, not on written rows: a scope that
    # only had to top up legitimately writes zero rows, and a row threshold
    # would park a genuinely finished scope forever.  A season with nothing
    # played says so explicitly through the runner's fallback reason.
    universe = int(players.get("universe_players") or 0)
    if universe <= 0 and players.get("fallback_reason") != UNSTARTED_SEASON_REASON:
        raise AirflowException("players capture committed no player universe")
    state.mark_completed(
        STATE_PATH,
        campaign_id=campaign_id,
        scope_key=environment["SOFASCORE_SCOPE_KEY"],
    )
    state.clear_failed(
        FAILURES_PATH,
        campaign_id=campaign_id,
        scope_key=environment["SOFASCORE_SCOPE_KEY"],
    )
    return {"status": "complete", "scope_key": environment["SOFASCORE_SCOPE_KEY"]}


def _task_state(task_instance: Any) -> str:
    value = getattr(task_instance.state, "value", task_instance.state)
    return str(value or "none").casefold().split(".")[-1]


def _finalize_players_run(**context: Any) -> dict[str, Any]:
    planned = context["ti"].xcom_pull(task_ids="plan_players_batch") or []
    dag_run = context.get("dag_run")
    release = state.current_release()
    failed = 0
    for index, environment in enumerate(planned):
        scope_key = environment.get("SOFASCORE_SCOPE_KEY")
        if not scope_key or dag_run is None:
            continue
        # BOTH mapped tasks, not just the bash one (lesson of PR #1216): a scope
        # whose capture succeeded but whose validation failed would be neither
        # completed nor failed, come back at the head of the queue and be paid
        # for in full again, every single day.
        states = set()
        for task_id in ("run_players_scope", "validate_players_scope"):
            task_instance = dag_run.get_task_instance(task_id, map_index=index)
            if task_instance is not None:
                states.add(_task_state(task_instance))
        if not states & {"failed", "upstream_failed"}:
            continue
        failed += 1
        # #1351: quarantine needs the class of the failure, whether the attempt
        # reached the source at all, and which release ran it.
        reason, source_requests = state.read_scope_outcome(
            environment.get("SOFASCORE_SCOPE_RESULT_PATH")
        )
        state.mark_failed(
            FAILURES_PATH,
            campaign_id=environment["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
            scope_key=scope_key,
            run_id=str(context.get("run_id") or "manual"),
            reason=reason,
            source_requests=source_requests,
            release=release,
            season_identity=environment.get("SOFASCORE_SEASON_ALIGNMENT_IDENTITY"),
        )
    return {"planned": len(planned), "failed": failed}


def _propagate_status(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise AirflowException("players watcher has no DagRun")
    failures = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id not in PLAYERS_TASK_IDS:
            continue
        if _task_state(task_instance) in {"failed", "upstream_failed"}:
            failures.append(task_instance.task_id)
    if failures:
        raise AirflowException(
            "SofaScore players attempt failed: " + ", ".join(sorted(failures))
        )
    return {"status": "success"}


RUN_SCOPE_COMMAND = """
set -euo pipefail
cd /opt/airflow
/opt/legacy-scraper-venv/bin/python \
  dags/scripts/run_sofascore_scope_cycle.py \
  --snapshot "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
  --tournament-id "${SOFASCORE_TOURNAMENT_ID}" \
  --source-season-id "${SOFASCORE_SOURCE_SEASON_ID}" \
  --expected-snapshot-id "${SOFASCORE_EXPECTED_SNAPSHOT_ID}" \
  --expected-campaign-id "${SOFASCORE_EXPECTED_CAMPAIGN_ID}" \
  --phase players \
  --output-dir "${SOFASCORE_SCOPE_OUTPUT_DIR}" \
  --output "${SOFASCORE_SCOPE_RESULT_PATH}" \
  --workload-artifact "${SOFASCORE_WORKLOAD_ARTIFACT}" \
  --run-id "${SOFASCORE_SCOPE_RUN_ID}"
"""


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Player profiles of every closed adult-men SofaScore scope",
    schedule=PLAYERS_SCHEDULE,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=PLAYERS_MAX_ACTIVE_TASKS,
    is_paused_upon_creation=True,
    dagrun_timeout=PLAYERS_DAGRUN_TIMEOUT,
    # No native rendering: ``env`` is a template field, and a native render
    # turns the numeric strings back into ints, which Popen refuses to encode.
    tags=["sofascore", "players", "all-men"],
) as dag:
    plan = PythonOperator(
        task_id="plan_players_batch",
        python_callable=_plan_players_batch,
        retries=0,
    )
    run = BashOperator.partial(
        task_id="run_players_scope",
        bash_command=RUN_SCOPE_COMMAND,
        append_env=True,
        pool=PLAYERS_POOL,
        # Below the current-season lanes: profiles of closed seasons wait.
        priority_weight=1,
        do_xcom_push=False,
        max_active_tis_per_dag=PLAYERS_MAX_ACTIVE_TASKS,
        # One retry keeps the same run_id: the gateway reuses the signed plan
        # and finished allocations replay from raw for free.
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=PLAYERS_SCOPE_TIMEOUT,
    ).expand(env=plan.output)
    validate = PythonOperator.partial(
        task_id="validate_players_scope",
        python_callable=_validate_players_scope,
        # A batch is a batch of independent scopes (lesson of the history
        # lane): with the default ``all_success`` one failed ``run[i]`` marks
        # EVERY validation upstream_failed, and finalize then books two paid,
        # healthy scopes as failures.  ``all_done`` lets each validation reach
        # its own check and decide on its own result.
        trigger_rule="all_done",
        retries=0,
    ).expand(op_kwargs=plan.output)
    finalize = PythonOperator(
        task_id="finalize_players_run",
        python_callable=_finalize_players_run,
        trigger_rule="all_done",
        retries=0,
    )
    propagate = PythonOperator(
        task_id="propagate_players_status",
        python_callable=_propagate_status,
        trigger_rule="all_done",
        retries=0,
    )

    plan >> run >> validate
    [plan, validate] >> finalize >> propagate


__all__ = ["DAG_ID", "dag"]
