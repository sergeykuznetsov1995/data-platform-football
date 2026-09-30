"""Source-native WhoScored daily ingestion DAG.

Runs twice a day (10:00 and 22:00, #1474).  One isolated runner refreshes the
persisted men's-competition catalog once a week (Monday gate inside the runner,
a short-circuit otherwise) and then ingests the explicit daily denominator
(``scrapers/whoscored/catalog.py`` denominator: class-A tournaments plus probe scopes):
schedule and matches (events, lineups, match stats) in ``ingest_matches``,
the weekly stage-statistics feeds in a separate ``ingest_stages`` task whose
failure never fails the matches.  Traffic egresses through the residential
proxy pool: WhoScored blocks the datacentre host IP at Cloudflare, so the
transport reads ``WHOSCORED_PROXY_FILE`` and routes the direct curl/FlareSolverr
requests through one sticky pool member (see ``WhoScoredTransport``).

Data lands on the VM only: Bronze Iceberg (``iceberg.bronze.whoscored_*`` via
Trino) plus raw blobs in SeaweedFS (``WHOSCORED_RAW_STORE_URI``).  There is no
paid gateway, approval, pointer or off-host backup on this path.

History is a separate manual DAG (``dag_backfill_whoscored``); this DAG only
keeps the current window fresh.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS

logger = logging.getLogger(__name__)

DAG_ID = "dag_ingest_whoscored"
RUNNER = "dags/scripts/run_whoscored_scraper.py"
DISCOVERY_PATH = "/tmp/whoscored_discovery_{{ ts_nodash }}.json"
RESULT_PATH = "/tmp/whoscored_result_{{ ts_nodash }}.json"
STAGES_RESULT_PATH = "/tmp/whoscored_stages_result_{{ ts_nodash }}.json"
# Two runs a day (#1474).  Set here, not in dags/utils/config.py (locked).
SCHEDULE = "0 10,22 * * *"
# Tree the Bash tasks run from; the isolated WhoScored stack mounts it outside
# /opt/airflow and sets WHOSCORED_RUNTIME_ROOT.
RUNTIME_ROOT = os.environ.get("WHOSCORED_RUNTIME_ROOT", "/opt/airflow")

# The scraper subprocess reads the residential pool from WHOSCORED_PROXY_FILE
# (host:port:user:pass, one sticky member per task).  No default: an unset pool
# fails discover instead of egressing from the host IP.
_TASK_ENV = {
    # Order matters: runtime_contract requires PYTHONPATH to be an ordered
    # subsequence of the anchored sys.path (root, root/dags).
    "PYTHONPATH": f"{RUNTIME_ROOT}:{RUNTIME_ROOT}/dags",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
    "WHOSCORED_PROXY_FILE": os.environ.get("WHOSCORED_PROXY_FILE", ""),
}

def _load_report(path: str) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except (OSError, ValueError) as exc:
        raise AirflowException(
            f"WhoScored runner report {path} is unavailable — the runner died "
            f"before writing it: {exc}"
        ) from exc
    if not isinstance(report, dict) or report.get("schema_version") != 3:
        raise AirflowException(f"WhoScored report {path} is not report schema v3")
    return report


# Honest colour (#1476).  Red means someone must act:
#   (a) a denominator scope (class-A tournaments; probe scopes excluded) is not
#       ``success`` - failed, retryable, still running or never started;
#   (b) outside the denominator (probe scopes) more than 5 % did not succeed;
#   (c) the run had no successful network answer or the pool was dead
#       (report status ``source_unavailable``);
#   (d) the reference SQL finds a denominator game past its deadline
#       (kickoff + 26 h) without a success and without a proven "not at the
#       source";
#   (e) the schedule of an active denominator scope is older than 48 h, or
#       a denominator scope (active or finished) has no schedule at all; a
#       finished season is not re-read daily, so its age is not a signal.
# The message lists every rule that fired and its scopes.
WHOSCORED_DAILY_MAX_FAILED_SCOPE_SHARE = 0.05
_LISTED = 10


def _trino_query(sql: str) -> list[tuple[Any, ...]]:
    from utils.data_quality import _get_conn

    conn = _get_conn()
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return [tuple(row) for row in cursor.fetchall()]
        finally:
            cursor.close()
    finally:
        conn.close()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _listed(items: list[str]) -> str:
    shown = ", ".join(items[:_LISTED])
    extra = len(items) - _LISTED
    return f"{shown} (+{extra} more)" if extra > 0 else shown


def _schedule_rule_text(older: list[str], absent: list[str], max_age: int) -> str:
    parts = []
    if older:
        parts.append(
            f"{len(older)} active denominator schedule(s) older than "
            f"{max_age} h: {_listed(older)}"
        )
    if absent:
        parts.append(
            f"{len(absent)} denominator schedule(s) absent: {_listed(absent)}"
        )
    return "; ".join(parts)


def validate_data(**context: Any) -> None:
    """Red when a rule (a)-(e) above fires; the message names the rules."""
    from dags.scripts.whoscored_criterion import (
        SCHEDULE_MAX_AGE_HOURS,
        denominator_partitions,
        inactive_partitions,
        render_overdue_sql,
        stale_schedule_partitions,
    )
    from scrapers.whoscored.catalog import PROBE_SCOPE_SPECS

    result_path = context["templates_dict"]["result_path"]
    report = _load_report(result_path)
    now = _utc_now()
    status = report.get("status")
    scopes = report.get("scopes") or []
    probes = set(PROBE_SCOPE_SPECS)
    denominator = [s for s in scopes if str(s.get("scope") or "") not in probes]
    outside = [s for s in scopes if str(s.get("scope") or "") in probes]
    fired: list[str] = []

    not_success = [
        f"{s.get('scope')}:{s.get('status')}"
        for s in denominator
        if s.get("status") != "success"
    ]
    if not denominator:
        fired.append("(a) the run planned no denominator scope")
    elif not_success:
        fired.append(
            f"(a) {len(not_success)}/{len(denominator)} denominator scope(s) "
            f"not success: {_listed(not_success)}"
        )
    outside_failed = [
        f"{s.get('scope')}:{s.get('status')}"
        for s in outside
        if s.get("status") != "success"
    ]
    if outside and (
        len(outside_failed) / len(outside) > WHOSCORED_DAILY_MAX_FAILED_SCOPE_SHARE
    ):
        fired.append(
            f"(b) {len(outside_failed)}/{len(outside)} probe scope(s) failed "
            f"(> {WHOSCORED_DAILY_MAX_FAILED_SCOPE_SHARE:.0%}): "
            f"{_listed(outside_failed)}"
        )
    route_successes = (report.get("traffic") or {}).get("route_successes") or {}
    successes = sum(int(value) for value in route_successes.values())
    if status == "source_unavailable":
        detail = report.get("source_unavailable") or {}
        fired.append(
            "(c) source unavailable (dead residential pool) at "
            f"{detail.get('scope') or 'start'}: {detail.get('message')}"
        )
    elif successes == 0:
        fired.append("(c) zero successful network answers in the run")

    partitions = denominator_partitions(report)
    try:
        overdue = _trino_query(render_overdue_sql(now))
    except Exception as exc:
        fired.append(f"(d) overdue query failed: {type(exc).__name__}: {exc}")
    else:
        if overdue:
            games = [f"{row[0]}={row[1]}#{row[2]}" for row in overdue]
            fired.append(
                f"(d) {len(overdue)} denominator game(s) past the 24 h deadline "
                f"without a success: {_listed(games)}"
            )
    if partitions:
        try:
            older, absent = stale_schedule_partitions(
                _trino_query, partitions, now, inactive_partitions(report)
            )
        except Exception as exc:
            fired.append(f"(e) schedule freshness query failed: {type(exc).__name__}: {exc}")
        else:
            if older or absent:
                fired.append(
                    "(e) " + _schedule_rule_text(older, absent, SCHEDULE_MAX_AGE_HOURS)
                )

    logger.info(
        "WhoScored daily: status=%s scopes=%d denominator=%d probes=%d "
        "rows=%s route_successes=%d rules_fired=%d",
        status,
        len(scopes),
        len(denominator),
        len(outside),
        report.get("rows"),
        successes,
        len(fired),
    )
    if fired:
        raise AirflowException("WhoScored daily is red: " + " | ".join(fired))


def validate_bronze_freshness(**context: Any) -> None:
    """ERROR freshness over the denominator partitions (league, season).

    Schedule: every active denominator partition refreshed within 48 h, a
    finished one (catalog ``is_active`` False) has schedule rows.  Matches and
    events: the newest write over the denominator partitions within 48 h.
    Telegram summary, then red on any ERROR.
    """
    from dags.scripts.whoscored_criterion import (
        CONTENT_MAX_AGE_HOURS,
        CONTENT_TABLES,
        SCHEDULE_MAX_AGE_HOURS,
        content_age_hours,
        denominator_partitions,
        inactive_partitions,
        stale_schedule_partitions,
    )
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CheckResult, RunReport

    dq = RunReport()
    now = _utc_now()

    def _add(name: str, passed: bool, details: str = "", error: str = "") -> None:
        dq.results.append(
            CheckResult(
                name=name,
                kind="freshness",
                severity="ERROR",
                passed=passed,
                details=details,
                error=error or None,
            )
        )

    try:
        report = _load_report(context["templates_dict"]["result_path"])
        partitions = denominator_partitions(report)
        if not partitions:
            raise AirflowException("the run report has no denominator scope")
    except AirflowException as exc:
        _add("freshness[denominator]", False, error=str(exc))
    else:
        name = f"freshness[whoscored_schedule per partition, max {SCHEDULE_MAX_AGE_HOURS}h]"
        try:
            older, absent = stale_schedule_partitions(
                _trino_query, partitions, now, inactive_partitions(report)
            )
        except Exception as exc:
            _add(name, False, error=f"{type(exc).__name__}: {exc}")
        else:
            stale = len(older) + len(absent)
            _add(
                name,
                not stale,
                details=(
                    f"{stale}/{len(partitions)} stale: "
                    + _schedule_rule_text(older, absent, SCHEDULE_MAX_AGE_HOURS)
                    if stale
                    else f"{len(partitions)} partitions fresh"
                ),
            )
        for table in CONTENT_TABLES:
            name = f"freshness[{table} over denominator, max {CONTENT_MAX_AGE_HOURS}h]"
            try:
                age = content_age_hours(_trino_query, table, partitions, now)
            except Exception as exc:
                _add(name, False, error=f"{type(exc).__name__}: {exc}")
                continue
            _add(
                name,
                age is not None and age <= CONTENT_MAX_AGE_HOURS,
                details=f"age={'never' if age is None else f'{age:.0f}h'}",
            )
    logger.info("validate_bronze_freshness: %s", dq.summary())
    telegram_dq_summary(dq, header="WhoScored Bronze freshness")
    if dq.errors:
        raise AirflowException(
            "WhoScored Bronze freshness: "
            + "; ".join(f"{r.name}: {r.details or r.error}" for r in dq.errors)
        )


with DAG(
    dag_id=DAG_ID,
    default_args=SCRAPER_ARGS,
    schedule=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=DAG_TAGS.get("whoscored"),
) as dag:
    discover_catalog = BashOperator(
        task_id="discover_catalog",
        bash_command=(
            "cd {root} && rm -f {discovery} && "
            "python {runner} discover "
            "--as-of-date {{{{ ds }}}} "
            "--weekly-gate {{{{ data_interval_end.isoformat() }}}} "
            "--transport-policy direct_only "
            "--output {discovery}"
        ).format(root=RUNTIME_ROOT, runner=RUNNER, discovery=DISCOVERY_PATH),
        env=_TASK_ENV,
        append_env=True,
    )

    ingest_matches = BashOperator(
        task_id="ingest_matches",
        # The runner exits non-zero whenever ANY scope failed or the pool was
        # dead (exit 3, #1476). A written report means the run completed and
        # validate_data downstream is the judge; the task itself only fails
        # when the runner died without a report.
        bash_command=(
            "cd {root} && rm -f {result} && "
            "python {runner} daily "
            "--skip-profiles "
            "--transport-policy direct_only "
            "--output {result} "
            "|| [ -s {result} ]"
        ).format(root=RUNTIME_ROOT, runner=RUNNER, result=RESULT_PATH),
        env=_TASK_ENV,
        append_env=True,
        # SCRAPER_ARGS gives 2h; one daily over the denominator needs more.
        execution_timeout=timedelta(hours=8),
    )

    ingest_stages = BashOperator(
        task_id="ingest_stages",
        # Stage-statistics feeds, once a week: the runner gates on the run
        # slot (data_interval_end = 10:00 Monday), not on the start time.
        # all_done: runs after ingest_matches whatever its state.  The task
        # has no report gate, so any failed stage scope turns it red; that
        # red never reaches ingest_matches or validate_data.
        bash_command=(
            "cd {root} && rm -f {result} && "
            "python {runner} daily "
            "--daily-part stages "
            "--skip-profiles "
            "--weekly-gate {{{{ data_interval_end.isoformat() }}}} "
            "--transport-policy direct_only "
            "--output {result}"
        ).format(root=RUNTIME_ROOT, runner=RUNNER, result=STAGES_RESULT_PATH),
        env=_TASK_ENV,
        append_env=True,
        execution_timeout=timedelta(hours=8),
        trigger_rule="all_done",
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        templates_dict={"result_path": RESULT_PATH},
    )

    bronze_freshness = PythonOperator(
        task_id="validate_bronze_freshness",
        python_callable=validate_bronze_freshness,
        templates_dict={"result_path": RESULT_PATH},
        trigger_rule="all_done",
    )

    # bronze_freshness stays useful on a red run (all_done), but it must not
    # be the sole leaf that colours the run green while validate_data is
    # upstream_failed (#1053) — the gate is a leaf of its own now.
    discover_catalog >> ingest_matches >> validate
    ingest_matches >> bronze_freshness
    ingest_matches >> ingest_stages
