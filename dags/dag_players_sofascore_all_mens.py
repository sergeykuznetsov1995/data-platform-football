"""Player profiles of every closed adult-men SofaScore scope (lane P).

Four times a day the lane takes tournament-seasons the history lane has
already CLOSED — every match committed to Bronze, which is exactly what the
players phase of ``prepare_sofascore_workload`` demands — and buys the two
player endpoints for them on its own gateway and its own pool.  The 15
configured leagues stay with ``dag_ingest_sofascore``: the profile manifest is
keyed by (tournament, season), so this lane would otherwise outrun the daily
rotation and pay for its profiles twice.

The schedule deliberately leaves 02:30-07:00 UTC empty: the nightly delivery
automaton recreates the scheduler inside 03:30-06:00 UTC under LocalExecutor
and would kill a scope mid-flight, losing traffic already paid for.
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
# may last 4.5 h, so the last one ends at 02:30 — an hour before the delivery
# window opens (03:30 UTC, 04:45 on Sundays) and an hour after it closes.
# @continuous was rejected for exactly this: it would meet the delivery every
# single night.
PLAYERS_SCHEDULE = "0 7,12,17,22 * * *"
PLAYERS_RUN_HOURS_UTC = (7, 12, 17, 22)
PLAYERS_DAGRUN_TIMEOUT = timedelta(hours=4, minutes=30)
# The batch runs SERIALLY on one pool slot, and a scope may be attempted twice
# (``retries=1``), so the window has to hold BOTH attempts (lesson of Sol round
# 12 on the refresh lane: counting one attempt made the arithmetic decorative).
# Live measurement 2026-09-04: the largest league took 34 min end to end;
# campaign tournaments are smaller.
PLAYERS_SCOPE_TIMEOUT = timedelta(minutes=45)
PLAYERS_SCOPE_ATTEMPTS = 2
PLAYERS_BATCH_FITS = int(
    PLAYERS_DAGRUN_TIMEOUT / (PLAYERS_SCOPE_TIMEOUT * PLAYERS_SCOPE_ATTEMPTS)
)
PLAYERS_BATCH_SIZE = min(
    state.env_int(
        "SOFASCORE_PLAYERS_BATCH_SIZE", state.DEFAULT_PLAYERS_BATCH_SIZE, 1, 64
    ),
    PLAYERS_BATCH_FITS,
)
# A literal default, NOT ``INGEST_SCRAPER_POOL``: a silent fall back into the
# shared pool would bring back exactly the 429 ``concurrency_limited`` storm
# the three lanes of #1244 were separated to end.
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
# ``run_sofascore_scraper.UNSTARTED_SEASON_REASON``; kept as a literal because
# importing the runner at DAG parse would drag the whole scraper stack in.
UNSTARTED_SEASON_REASON = "season_has_no_finished_matches"


def _configured_tournament_ids() -> frozenset[int]:
    """Source ids of the leagues the daily ingest already covers."""

    from scrapers.sofascore.catalog import SofaScoreCatalog

    return frozenset(SofaScoreCatalog.load().tournament_map(enabled_only=True).values())


def _plan_players_batch(**context: Any) -> list[dict[str, str]]:
    snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    campaign_id = str(snapshot.get("campaign_id") or "")
    return state.plan_players_batch(
        snapshot,
        completed=state.read_completed(HISTORY_STATE_PATH, campaign_id=campaign_id),
        players_completed=state.read_completed(STATE_PATH, campaign_id=campaign_id),
        failures=state.read_failures(FAILURES_PATH, campaign_id=campaign_id),
        max_scope_attempts=PLAYERS_MAX_SCOPE_ATTEMPTS,
        batch_size=PLAYERS_BATCH_SIZE,
        exclude_tournament_ids=_configured_tournament_ids(),
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
    if (
        result.get("snapshot_id") != environment["SOFASCORE_EXPECTED_SNAPSHOT_ID"]
        or result.get("campaign_id") != campaign_id
        or int(result.get("tournament_id", 0)) != int(tournament_id)
        or int(result.get("source_season_id", 0)) != int(season_id)
    ):
        raise AirflowException("scope result provenance mismatch")
    if result.get("status") == "deferred":
        # The matches of this scope are not all in Bronze yet.  No paid traffic
        # happened, so the task stays green; the failure memory parks the scope
        # after a few tries so it cannot hold the head of the queue forever.
        state.mark_failed(
            FAILURES_PATH,
            campaign_id=campaign_id,
            scope_key=environment["SOFASCORE_SCOPE_KEY"],
            run_id=str(result.get("run_id") or "manual"),
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
    # would park a genuinely finished scope forever.
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
        state.mark_failed(
            FAILURES_PATH,
            campaign_id=environment["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
            scope_key=scope_key,
            run_id=str(context.get("run_id") or "manual"),
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
