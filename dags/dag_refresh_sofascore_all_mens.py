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

from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
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
REFRESH_SCOPE_TIMEOUT = timedelta(hours=2)
# Whatever the campaign-wide default (8) or an operator's override says, the
# batch is capped by what actually fits: sweep + batch * scope * attempts <=
# DagRun.  A batch of 8 would need 18 h in a 7 h window and the run would be
# killed halfway through, losing the scopes it was in the middle of (Sol round
# 5, finding 8).  With the sweep at 2.5 h and a scope at 2 h x 2 attempts the
# honest answer is ONE scope per run — three a day.  The lane's job is the
# schedule sweep; the bulk of the match phase belongs to the campaign DAG,
# which has the whole day for it.
# A scope may be attempted twice (``retries=1`` below, which the workload plan
# supports on purpose — the retry resumes on the remainder of its allocation),
# so the window has to hold both attempts.  Counting one attempt per scope made
# the "fits" arithmetic decorative: 2.5 h + 2 x 2 h looked like 6.5 h of a 7 h
# window, while a single retried scope already ran to 8.5 h and the DagRun
# timeout killed the batch mid-scope — the very thing the cap exists to prevent
# (Sol round 12, finding 2).
REFRESH_SCOPE_ATTEMPTS = 2
REFRESH_BATCH_FITS = int(
    (REFRESH_DAGRUN_TIMEOUT - REFRESH_FETCH_TIMEOUT)
    / (REFRESH_SCOPE_TIMEOUT * REFRESH_SCOPE_ATTEMPTS)
)
REFRESH_BATCH_SIZE = min(
    state.env_int(
        "SOFASCORE_REFRESH_BATCH_SIZE", state.DEFAULT_REFRESH_BATCH_SIZE, 1, 64
    ),
    REFRESH_BATCH_FITS,
)
REFRESH_POOL = (
    os.environ.get("SOFASCORE_REFRESH_POOL", "").strip() or INGEST_SCRAPER_POOL
)
REFRESH_MAX_ACTIVE_TASKS = state.env_int(
    "SOFASCORE_REFRESH_MAX_ACTIVE_TASKS", 1, 1, 16
)
# One season page is ~20-27 KB on the campaign's own accounting, plus ~75-80 KB
# per lease for the browser warm-up; the gateway meters them per DagRun.  The
# knobs below admit 150 * 3 + 200 * (3 + 1) + 40 * (12 + 3 + 2) = 1930 pages
# ~ 50 MB with warm-ups when every tail visit has to chase, every stale and
# seeded season takes its fixture page and every resumed chain steps back —
# ~95 minutes at the gateway's pace, inside both this cap and the fetch timeout.
# The run computes that worst case itself and refuses to start when an override
# pushes it over the cap (Sol round 6, finding 5).
REFRESH_DISCOVERY_BUDGET_BYTES = state.env_int(
    "SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", 64 * 1024 * 1024, 1, 1024 ** 3
)
# Seasons playing in the window get their tail pages on every run — up to this
# many; beyond the cap the class rotates on its cursor like the others.  That
# is what keeps the "match finished -> row in Bronze" lag inside the interval.
REFRESH_MAX_DUE = state.env_int("SOFASCORE_REFRESH_MAX_DUE", 150, 1, 4096)
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
REFRESH_TASK_IDS = frozenset({
    "refresh_season_schedules",
    "plan_refresh_batch",
    "run_refresh_scope",
    "validate_refresh_scope",
})

# Finished games of campaign partitions that have no complete capture yet,
# per partition, busiest first.  ``season`` is CAST on both sides because the
# schedule stores it as the source ships it while the status table is text.
PENDING_PARTITIONS_SQL = """
SELECT s.league, CAST(s.season AS varchar) AS season,
       count(DISTINCT s.game_id) AS pending_matches
FROM iceberg.bronze.sofascore_schedule s
LEFT JOIN iceberg.bronze.sofascore_match_capture_status c
  ON c.league = s.league
 AND CAST(c.season AS varchar) = CAST(s.season AS varchar)
 AND c.match_id = CAST(s.game_id AS varchar)
 AND c.capture_complete = true
WHERE s.league LIKE 'SS-%'
  AND s.status_type = 'finished'
  AND c.match_id IS NULL
GROUP BY 1, 2
ORDER BY 3 DESC, 1, 2
"""


def _pending_refresh_partitions() -> list[tuple[str, str, int]]:
    """Query at task runtime; no Trino client is touched at DAG parse."""

    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(PENDING_PARTITIONS_SQL)
        return [
            (str(league), str(season), int(count))
            for league, season, count in cursor.fetchall()
        ]
    finally:
        conn.close()


def _configured_tournament_ids() -> frozenset[int]:
    """Source ids of the leagues the daily ingest already covers."""

    from scrapers.sofascore.catalog import SofaScoreCatalog

    return frozenset(SofaScoreCatalog.load().tournament_map(enabled_only=True).values())


def _plan_refresh_batch(**context: Any) -> list[dict[str, str]]:
    snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    return state.plan_refresh_batch(
        snapshot,
        _pending_refresh_partitions(),
        batch_size=REFRESH_BATCH_SIZE,
        exclude_tournament_ids=_configured_tournament_ids(),
        snapshot_path=SNAPSHOT_PATH,
        policy_path=POLICY_PATH,
        result_dir=RESULT_DIR,
        workload_artifact=WORKLOAD_ARTIFACT,
        dag_run_id=str(context.get("run_id") or "manual"),
        task_env=REFRESH_TASK_ENV,
    )


def _validate_refresh_scope(**environment: str) -> dict[str, Any]:
    result_path = Path(environment["SOFASCORE_SCOPE_RESULT_PATH"])
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"scope result is unreadable: {exc}") from exc
    if result.get("status") != "success":
        raise AirflowException("scope cycle did not finish successfully")
    if environment.get("SOFASCORE_CAMPAIGN_ACTION") != "refresh":
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
    # A refreshed scope is never "completed": the next DagRun re-plans it
    # from Bronze evidence as new games finish.
    return {"status": "refreshed", "scope_key": environment["SOFASCORE_SCOPE_KEY"]}


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
    if failures:
        raise AirflowException(
            "SofaScore refresh attempt failed: " + ", ".join(sorted(failures))
        )
    return {"status": "success"}


FETCH_COMMAND = """
set -euo pipefail
cd /opt/airflow
/opt/legacy-scraper-venv/bin/python \
  dags/scripts/run_sofascore_schedule_refresh.py \
  --snapshot "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
  --budget-cap-bytes "${SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES}" \
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
  --output "${SOFASCORE_REFRESH_RESULT_DIR}/schedule-refresh-{{ ts_nodash }}.json"
"""

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
  --phase matches \
  --season-evidence bronze \
  --allow-pending-season \
  --output-dir "${SOFASCORE_SCOPE_OUTPUT_DIR}" \
  --output "${SOFASCORE_SCOPE_RESULT_PATH}" \
  --workload-artifact "${SOFASCORE_WORKLOAD_ARTIFACT}" \
  --run-id "${SOFASCORE_SCOPE_RUN_ID}"
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
        # NO retry, unlike every other task here.  A second attempt keeps the
        # same DagRun, so it walks up to the same paid plan again while the
        # gateway hands it only what the first attempt left of the run budget —
        # the anti-pattern of lesson #7 (#1044: three retries of one task ate
        # the DagRun budget and killed the wave without a single source hole).
        # The lane does not need it: every class commits its own cursor and its
        # own unfinished chains as it finishes, so the next scheduled run picks
        # up exactly where this one stopped, 8 h later at worst.
        retries=0,
        # Worst case is max_due * chase_pages + max_stale * (chase_pages + 1)
        # + max_seed * (seed_pages + backtrack + overlap + 1) = 450 + 800 + 680 = 1930
        # requests (the fixture page of every stale and seeded season and the
        # step-back allowance count too); at the lane's 20/min that is ~95 min,
        # so the task window clears it with room for the source being slow.
        execution_timeout=REFRESH_FETCH_TIMEOUT,
    )
    plan = PythonOperator(
        task_id="plan_refresh_batch",
        python_callable=_plan_refresh_batch,
        retries=0,
    )
    run = BashOperator.partial(
        task_id="run_refresh_scope",
        bash_command=RUN_SCOPE_COMMAND,
        append_env=True,
        pool=REFRESH_POOL,
        # Above the history lane (1) in the shared pool.
        priority_weight=5,
        do_xcom_push=False,
        max_active_tis_per_dag=REFRESH_MAX_ACTIVE_TASKS,
        # One retry keeps the same run_id: the gateway reuses the signed
        # plan and a latched lease is re-claimed after the reaper grace.
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=REFRESH_SCOPE_TIMEOUT,
    ).expand(env=plan.output)
    validate = PythonOperator.partial(
        task_id="validate_refresh_scope",
        python_callable=_validate_refresh_scope,
        retries=0,
    ).expand(op_kwargs=plan.output)
    propagate = PythonOperator(
        task_id="propagate_refresh_status",
        python_callable=_propagate_status,
        trigger_rule="all_done",
        retries=0,
    )

    fetch >> plan >> run >> validate >> propagate


__all__ = ["DAG_ID", "dag"]
