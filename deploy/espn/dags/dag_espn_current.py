"""ESPN current-data waves: 00/06/12/18 UTC, bounded Bronze publications.

Lives in ``deploy/espn/dags/`` — the DAG folder of the ``espn-airflow``
project (mounted by #1507), not ``dags/``: a new file there changes the
directory the WhoScored runtime guard pins and the shared stack does not
collect ESPN (roadmap assumption 8).  Paused on creation all the same.

prepare -> plan_wave -> run_tournament (mapped over groups of tournament-seasons)
-> wave_summary (all_done).  The logic is in ``wave`` and ``grouped_wave``; this file
only wires Airflow, the transport and Trino.  ``wave_summary`` writes the wave
log (``scrapers.espn.wave_log``, #1505) before it turns a red wave red.
``prepare`` also creates the recheck journal (``scrapers.espn.recheck``, #1506).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DAG_ID = "dag_espn_current"
SCHEDULE = "0 0,6,12,18 * * *"
# Pool of the live lane; created by the espn-airflow delivery (#1507).
LIVE_POOL = "espn_live"
RUN_TASK_ID = "run_tournament"
PLAN_TASK_ID = "plan_wave"
OUTCOME_KEY = "outcome"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# Every statement waits in the per-table limiter (scrapers/espn/trino_budget.py)
# while another process commits to the same relation, and for INTERVAL +
# WRITE_TIMEOUT of scrapers/whoscored/trino_budget.py after a crashed holder.
# A task budget below that fence turns the wait into a failed wave: 22.11.2026
# plan_wave spent three 20-min attempts inside the fence, 25.11.2026 three
# groups waited 23 min behind one replay MERGE and their retries no longer
# fitted into 55 min (#2472).
LIMITER_FENCE = timedelta(minutes=46)
# A task waits the fence out and still does its work (plan <= 6 min,
# a group <= 11 min observed) instead of burning its attempts inside the wait.
TASK_TIMEOUT = LIMITER_FENCE + timedelta(minutes=14)
# The run absorbs the fence once and still gives the slowest task its budget;
# waves are six hours apart, so the longer run keeps a delivery window free.
DAGRUN_TIMEOUT = LIMITER_FENCE + TASK_TIMEOUT + timedelta(minutes=14)


def _trino():
    # Without dynamic filtering: the tombstone MERGE loses NULL rows (#1557).
    from scrapers.espn.trino_manager import EspnTrinoTableManager

    return EspnTrinoTableManager()


def _client():
    from scrapers.espn.gate import TransportGate
    from scrapers.espn.transport import EspnHttpClient

    # One gate per task process; the pace is shared through its flock file,
    # the step comes from ESPN_GATE_STEP_CEILING (S0 by default).
    return EspnHttpClient(gate=TransportGate(lane="live"))


def _gate_closed(exc: BaseException) -> bool:
    from scrapers.espn.transport_contracts import AllOriginsBlocked, LaneClosed

    return isinstance(exc, (AllOriginsBlocked, LaneClosed))


def _no_retry(exc: BaseException) -> bool:
    from scrapers.espn.grouped_wave import ambiguous_write
    from scrapers.espn.wave import WavePlanError

    return _gate_closed(exc) or isinstance(exc, WavePlanError) or ambiguous_write(exc)


def _task_name(ti) -> str:
    map_index = getattr(ti, "map_index", -1)
    return f"{ti.task_id}[{map_index}]" if map_index is not None and map_index >= 0 else ti.task_id


def _window(context: dict[str, Any]) -> tuple[date | None, date | None]:
    """``since``/``until`` of a manual run's conf (#1508); ``None`` without them."""

    conf = getattr(context.get("dag_run"), "conf", None) or {}
    since, until = (
        # A key present with any value is parsed: "" or 0 is an error, not "no window".
        date.fromisoformat(str(conf[key])) if key in conf else None
        for key in ("since", "until")
    )
    return since, until


def _window_note(context: dict[str, Any]) -> str | None:
    try:
        since, until = _window(context)
    except ValueError:
        return None
    return f"window {since}..{until}" if since and until else None


def prepare(**_: Any) -> None:
    from scrapers.espn.trino_manager import EspnIcebergWriter
    from scrapers.espn.bronze_schema import ensure_bronze_tables
    from scrapers.espn.journal import ensure_journal_table
    from scrapers.espn.recheck import ensure_recheck_table
    from scrapers.espn.wave_log import ensure_wave_log_table

    ensure_bronze_tables(EspnIcebergWriter())
    connection = _trino().connection
    ensure_journal_table(connection)
    ensure_wave_log_table(connection)
    ensure_recheck_table(connection)


def plan_wave(**context: Any) -> list[dict[str, Any]]:
    from scrapers.espn import editions_store, grouped_wave, wave
    from scrapers.espn.freshness_source import CORE_SCOPE_KEY
    from scrapers.espn.denominator import load_denominator

    now = datetime.now(timezone.utc)
    boundary = context.get("data_interval_end") or now
    midnight = boundary.astimezone(timezone.utc).hour == 0
    # A manual run with conf {"since", "until"} catches up that window (#1508).
    try:
        since, until = _window(context)
        wave.check_window(since, until, now.date())
    except ValueError as exc:
        raise AirflowFailException(f"ESPN catch-up window: {exc}") from exc
    trino = _trino()
    client = _client()
    failure: BaseException | None = None
    try:
        plan = wave.plan_wave(
            client=client,
            trino=trino,
            rows=wave.live_rows(load_denominator()),
            state_path=editions_store.default_state_path(),
            now=now,
            # Once a day, in the 00 wave: stuck POSTPONED/SUSPENDED matches
            # and the core event list against bronze (#1505, R-09).
            check_stale=midnight,
            check_core=midnight,
            # Once a day too: rechecks at kickoff + 7…10 days and the 5 %
            # sample at +24/72 h, under the "not worse" rule (#1506).
            check_recheck=midnight,
            since=since,
            until=until,
        )
    except BaseException as exc:
        failure = exc
        if _gate_closed(exc):
            raise AirflowFailException(f"ESPN gate closed: {type(exc).__name__}: {exc}") from exc
        if isinstance(exc, wave.WavePlanError):
            raise AirflowFailException(f"ESPN wave plan: {exc}") from exc
        raise
    finally:
        try:
            wave.finish_requests(
                client,
                trino.connection,
                run_id=str(context.get("run_id") or "manual"),
                task_id=PLAN_TASK_ID,
            )
        except Exception:
            if failure is None:
                raise
            logger.exception("ESPN plan request journal not written")
    try:
        groups = grouped_wave.group_works(plan.works)
    except ValueError as exc:
        raise AirflowFailException(f"ESPN invalid wave plan: {exc}") from exc
    if midnight and since is None and str(context.get("run_id", "")).startswith("scheduled__"):
        context["ti"].xcom_push(key=CORE_SCOPE_KEY, value={
            "day": now.date().isoformat(), "out_of_season": list(plan.out_of_season),
        })
    return [{"works": [work.to_xcom() for work in group]} for group in groups]


def _edition_key(row):
    if isinstance(row, dict):
        slug, year = row.get("slug"), row.get("season_year")
        if isinstance(slug, str) and slug.strip() and str(year).isdecimal() and int(year) > 0:
            return slug.strip().lower(), int(year)
    return None


def _failed_member(key, cause):
    from scrapers.espn import wave

    return wave.TournamentOutcome(*key, wave.RED, 0, {}, cause).as_dict()


def run_tournament(work=None, works=None, **context: Any) -> dict[str, Any]:
    from scrapers.espn import grouped_wave, wave

    ti = context["ti"]
    if works is not None:
        try:
            members = [wave.TournamentWork.from_xcom(row) for row in works]
            trino = _trino()
            outcomes = grouped_wave.run_tournament_group(
                members, client=_client(), trino=trino, conn=trino.connection,
                child_trino_factory=_trino, run_id=str(context.get("run_id") or "manual"),
                task_id=_task_name(ti),
            )
        except grouped_wave.GroupRunError as exc:
            ti.xcom_push(key=OUTCOME_KEY, value={"outcomes": [o.as_dict() for o in exc.outcomes]})
            if exc.no_retry:
                raise AirflowFailException(f"ESPN {_task_name(ti)}: {exc}") from exc
            raise
        except Exception as exc:
            cause = f"{type(exc).__name__}: {exc}"
            keys = dict.fromkeys(key for row in works if (key := _edition_key(row)) is not None)
            ti.xcom_push(key=OUTCOME_KEY, value={"outcomes": [_failed_member(k, cause) for k in keys]})
            if _no_retry(exc):
                raise AirflowFailException(f"ESPN {_task_name(ti)}: {cause}") from exc
            raise
        result = {"outcomes": [o.as_dict() for o in outcomes]}
        ti.xcom_push(key=OUTCOME_KEY, value=result)
        return result

    item = wave.TournamentWork.from_xcom(work)
    try:
        trino = _trino()
        outcome = wave.run_tournament(
            item,
            client=_client(),
            trino=trino,
            conn=trino.connection,
            run_id=str(context.get("run_id") or "manual"),
            task_id=_task_name(ti),
        )
    except Exception as exc:
        ti.xcom_push(key=OUTCOME_KEY, value=wave.failed_outcome(item, exc).as_dict())
        if _no_retry(exc):
            raise AirflowFailException(f"ESPN {item.slug}: {type(exc).__name__}: {exc}") from exc
        raise
    result = outcome.as_dict()
    ti.xcom_push(key=OUTCOME_KEY, value=result)
    return result


def _state(ti) -> str:
    state = getattr(ti.state, "value", ti.state)
    return str(state or "none").casefold().split(".")[-1]


def _planned_keys(entry):
    if isinstance(entry, dict):
        entry = entry.get("works", [entry.get("work", entry)])
    if not isinstance(entry, (list, tuple)):
        return {}
    return dict.fromkeys(key for row in entry if (key := _edition_key(row)) is not None)


def _map_outcomes(result, expected, state):
    from scrapers.espn import wave

    grouped = isinstance(result, dict) and "outcomes" in result
    rows = result["outcomes"] if grouped else [result]
    proved = {}
    if isinstance(rows, list) and (grouped or expected is None or len(expected) == 1):
        for row in rows:
            key = _edition_key(row)
            if key is None or (expected is not None and key not in expected):
                continue
            counts = row.get("dispositions")
            if (
                key in proved or row.get("state") not in (wave.GREEN, wave.RED)
                or not isinstance(row.get("matches"), int) or not isinstance(counts, dict)
                or not all(isinstance(n, int) for n in counts.values())
            ):
                proved[key] = None
            elif not grouped and state != "success" and row["state"] != wave.RED:
                proved[key] = {**row, "state": wave.RED, "first_error": f"task {state}"}
            else:
                proved[key] = row
    return {
        key: proved.get(key) or _failed_member(key, f"task {state}: missing or invalid outcome")
        for key in (proved if expected is None else expected)
    }


def _wave_outcomes(ti, instances):
    # The unmapped plan's return value is the full op_kwargs list for this DagRun.
    plan = ti.xcom_pull(task_ids=PLAN_TASK_ID, map_indexes=-1, key="return_value")
    if isinstance(plan, dict):
        plan = [plan]
    mapped = {i.map_index: i for i in instances if i.task_id == RUN_TASK_ID}
    outcomes = {}
    failed = []
    for index in (mapped if plan is None else range(len(plan))):
        instance = mapped.get(index)
        state = _state(instance) if instance is not None else "missing"
        if index < 0:
            if state not in {"skipped", "removed"}:
                failed.append(f"{RUN_TASK_ID}={state}")
            continue
        if plan is None and state in {"skipped", "removed"}:
            continue
        expected = _planned_keys(plan[index]) if plan is not None else None
        result = ti.xcom_pull(task_ids=RUN_TASK_ID, map_indexes=index, key=OUTCOME_KEY)
        rows = _map_outcomes(result, expected, state)
        if rows:
            for key in outcomes.keys() & rows.keys():
                rows[key] = _failed_member(key, "duplicate edition across mapped tasks")
            outcomes.update(rows)
        else:
            failed.append(f"{RUN_TASK_ID}[{index}]={state}")
    return list(outcomes.values()), failed


def wave_summary(**context: Any) -> dict[str, Any]:
    from scrapers.espn import wave, wave_log

    dag_run = context["dag_run"]
    ti = context["ti"]
    instances = list(dag_run.get_task_instances())
    plan_error = next(
        (_state(i) for i in instances if i.task_id == PLAN_TASK_ID and _state(i) != "success"),
        None,
    )
    outcomes, failed = _wave_outcomes(ti, instances)
    started = getattr(dag_run, "start_date", None)
    finished = datetime.now(timezone.utc)
    duration = (finished - started).total_seconds() if started is not None else None
    summary = wave.summarize_wave(
        outcomes, failed, plan_error=plan_error, duration_s=duration
    )
    window = _window_note(context)
    if window is not None:
        logger.info("ESPN wave: manual catch-up, %s", window)
    # The log goes first: a red wave leaves its trace too (#1505).
    try:
        wave_log.write_wave_log(
            _trino().connection,
            wave_log.wave_log_rows(
                outcomes,
                failed,
                summary,
                run_id=str(context.get("run_id") or "manual"),
                started_at=started,
                finished_at=finished,
                window=window,
            ),
        )
    except Exception:
        if not summary.red:
            raise
        logger.exception("ESPN wave log not written")
    for line in summary.table:
        logger.info("ESPN wave: %s", line)
    for warning in summary.warnings:
        logger.warning("ESPN wave: %s", warning)
    logger.info(
        "ESPN wave: %d tournament(s), %d red, withdrawn %d, moved %d, duration %s s",
        summary.tournaments,
        summary.red_tournaments,
        summary.withdrawn_count,
        summary.moved_count,
        None if duration is None else round(duration),
    )
    if summary.red:
        raise AirflowException(f"ESPN wave red: {summary.reason}")
    return summary.as_dict()


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="ESPN current data: four waves a day, bounded groups of tournament-seasons",
    schedule=SCHEDULE,
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    is_paused_upon_creation=True,
    dagrun_timeout=DAGRUN_TIMEOUT,
    tags=["espn", "bronze", "scraping"],
    doc_md=__doc__,
) as dag:
    prepare_task = PythonOperator(
        task_id="prepare",
        python_callable=prepare,
        retries=2,
        execution_timeout=TASK_TIMEOUT,
    )
    plan_task = PythonOperator(
        task_id=PLAN_TASK_ID,
        python_callable=plan_wave,
        pool=LIVE_POOL,
        retries=2,
        execution_timeout=TASK_TIMEOUT,
    )
    run_task = PythonOperator.partial(
        task_id=RUN_TASK_ID,
        python_callable=run_tournament,
        pool=LIVE_POOL,
        retries=2,
        retry_delay=timedelta(minutes=3),
        max_active_tis_per_dag=4,
        execution_timeout=TASK_TIMEOUT,
    ).expand(op_kwargs=plan_task.output)
    summary_task = PythonOperator(
        task_id="wave_summary",
        python_callable=wave_summary,
        trigger_rule="all_done",
        retries=0,
        execution_timeout=TASK_TIMEOUT,
    )

    prepare_task >> plan_task >> run_task >> summary_task
