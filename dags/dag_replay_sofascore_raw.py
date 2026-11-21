"""Paused on-demand offline SofaScore raw replay (#1367).

The planner selects up to ``scope_cap`` replayable manifest scopes with one
manifest query, one mapped BashOperator replays each exact scope through the
offline worker with zero source traffic, and ``validate_summary`` fails
closed on any violation in the workers' JSON reports.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.default_args import DEFAULT_ARGS as BASE_DEFAULT_ARGS
from utils.default_args import INGEST_SCRAPER_POOL


logger = logging.getLogger(__name__)

DAG_ID = "dag_replay_sofascore_raw"

REPLAY_MODES = ("fields", "drain")
DEFAULT_REPLAY_MODE = "fields"
DEFAULT_SCOPE_CAP = 3
DEFAULT_TARGET_CAP = 25

REPLAY_OUTPUT_ROOT = "/opt/airflow/logs/sofascore-raw-replay"
REPLAY_WORKER_SCRIPT = "/opt/airflow/dags/scripts/replay_sofascore_raw.py"

DEFAULT_ARGS: dict[str, Any] = {**BASE_DEFAULT_ARGS, "retries": 0}

PARAMS: dict[str, Any] = {
    "mode": Param(DEFAULT_REPLAY_MODE, enum=list(REPLAY_MODES)),
    "scope_cap": Param(DEFAULT_SCOPE_CAP, type="integer", minimum=1),
    "target_cap": Param(DEFAULT_TARGET_CAP, type="integer", minimum=1),
    "source_tournament_id": Param(None, type=["null", "string"]),
    "source_season_id": Param(None, type=["null", "string"]),
}


def _safe_path_component(value: object) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip(".")
    return safe or "_"


def plan_replay_scopes(**context: Any) -> list[dict[str, str]]:
    """Return one mapped worker env per replayable scope, bounded by caps."""
    params = context.get("params") or {}
    mode = params.get("mode", DEFAULT_REPLAY_MODE)
    scope_cap = params.get("scope_cap", DEFAULT_SCOPE_CAP)
    target_cap = params.get("target_cap", DEFAULT_TARGET_CAP)
    source_tournament_id = params.get("source_tournament_id")
    source_season_id = params.get("source_season_id")

    if mode not in REPLAY_MODES:
        raise ValueError(f"mode must be one of {REPLAY_MODES}, got {mode!r}")
    for name, value in (("scope_cap", scope_cap), ("target_cap", target_cap)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(
                f"{name} must be a positive integer, got {value!r}"
            )
    exact_scope = None
    if source_tournament_id is not None or source_season_id is not None:
        if (
            not isinstance(source_tournament_id, str)
            or not source_tournament_id.strip()
            or not isinstance(source_season_id, str)
            or not source_season_id.strip()
        ):
            raise ValueError(
                "source_tournament_id and source_season_id must both be "
                "null or both be nonempty strings, got "
                f"{source_tournament_id!r} and {source_season_id!r}"
            )
        exact_scope = (
            source_tournament_id.strip(),
            source_season_id.strip(),
        )

    from dags.scripts.replay_sofascore_raw import _replay_manager
    from scrapers.sofascore.pipeline import build_capture_runtime
    from scrapers.sofascore.raw_replay import list_replay_scope_keys

    run_id = str(context.get("run_id") or "manual")
    capture_runtime = build_capture_runtime(run_id=run_id, task_id="planner")
    manager = _replay_manager(
        getattr(capture_runtime, "manifest_store", None)
    )
    keys = list_replay_scope_keys(
        manager, mode=mode, exact_scope=exact_scope, limit=scope_cap
    )
    if not keys:
        logger.info(
            "SofaScore raw replay drained: no replayable scope for mode=%s",
            mode,
        )
        return []

    environments: list[dict[str, str]] = []
    seen_outputs: set[str] = set()
    for key in keys:
        scope = {
            "source_tournament_id": key.source_tournament_id,
            "source_season_id": key.source_season_id,
            "freshness_key": key.freshness_key,
        }
        scope_json = json.dumps(scope, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(scope_json.encode("utf-8")).hexdigest()[:16]
        output_path = os.path.join(
            REPLAY_OUTPUT_ROOT,
            _safe_path_component(run_id),
            "{}-{}-{}.json".format(
                _safe_path_component(key.source_tournament_id),
                _safe_path_component(key.source_season_id),
                digest,
            ),
        )
        if output_path in seen_outputs:
            continue
        seen_outputs.add(output_path)
        logger.info("SofaScore raw replay selected scope: %s", scope_json)
        environments.append(
            {
                "SOFASCORE_REPLAY_MODE": mode,
                "SOFASCORE_REPLAY_SCOPE_JSON": scope_json,
                "SOFASCORE_REPLAY_TARGET_CAP": str(target_cap),
                "SOFASCORE_REPLAY_OUTPUT": output_path,
            }
        )
    return environments


RUN_REPLAY_SCOPE_COMMAND = f"""
set -euo pipefail
cd /opt/airflow
mkdir -p "$(dirname "${{SOFASCORE_REPLAY_OUTPUT}}")"
exec /opt/legacy-scraper-venv/bin/python {REPLAY_WORKER_SCRIPT} \\
    --mode "${{SOFASCORE_REPLAY_MODE}}" \\
    --scope-json "${{SOFASCORE_REPLAY_SCOPE_JSON}}" \\
    --target-cap "${{SOFASCORE_REPLAY_TARGET_CAP}}" \\
    --output "${{SOFASCORE_REPLAY_OUTPUT}}"
"""


def _load_replay_report(output_path: str) -> dict[str, Any]:
    try:
        with open(output_path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError) as exc:
        raise AirflowException(
            f"cannot read replay output {output_path!r}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise AirflowException(
            f"replay output {output_path!r} is not a JSON object"
        )
    return payload


def _validate_replay_report(
    output_path: str, report: Mapping[str, Any]
) -> tuple[int, int]:
    errors = report.get("errors")
    if not isinstance(errors, list) or errors:
        raise AirflowException(
            f"replay output {output_path!r}: errors must be empty, got "
            f"{errors!r}"
        )
    source_request_count = report.get("source_request_count")
    if (
        isinstance(source_request_count, bool)
        or not isinstance(source_request_count, int)
        or source_request_count != 0
    ):
        raise AirflowException(
            f"replay output {output_path!r}: source_request_count must be "
            f"0, got {source_request_count!r}"
        )
    status_counts = report.get("status_counts")
    if not isinstance(status_counts, Mapping):
        raise AirflowException(
            f"replay output {output_path!r}: 'status_counts' must be an "
            "object"
        )
    for name in ("deferred", "schema_error"):
        value = status_counts.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value != 0
        ):
            raise AirflowException(
                f"replay output {output_path!r}: status_counts.{name} must "
                f"be 0, got {value!r}"
            )
    phases = report.get("phases")
    if not isinstance(phases, Mapping):
        raise AirflowException(
            f"replay output {output_path!r}: 'phases' must be an object"
        )
    for phase_name, phase in phases.items():
        if not isinstance(phase, Mapping):
            raise AirflowException(
                f"replay output {output_path!r}: phase {phase_name!r} must "
                "be an object"
            )
        if not phase.get("ran"):
            continue
        exit_code = phase.get("exit_code", 0)
        if (
            isinstance(exit_code, bool)
            or not isinstance(exit_code, int)
            or exit_code != 0
        ):
            raise AirflowException(
                f"replay output {output_path!r}: phase {phase_name!r} "
                f"exited {exit_code!r}"
            )
    tables = report.get("tables")
    if not isinstance(tables, list):
        raise AirflowException(
            f"replay output {output_path!r}: 'tables' must be a list"
        )
    receipts = report.get("publication_receipts")
    if not isinstance(receipts, list):
        raise AirflowException(
            f"replay output {output_path!r}: 'publication_receipts' must "
            "be a list"
        )
    receipt_dicts = [
        receipt for receipt in receipts if isinstance(receipt, Mapping)
    ]
    for location in tables:
        if not isinstance(location, str) or not location:
            raise AirflowException(
                f"replay output {output_path!r}: table location must be a "
                f"nonempty string, got {location!r}"
            )
        table_name = re.split(r"[./]", location.rstrip("/"))[-1]
        matching = [
            receipt
            for receipt in receipt_dicts
            if receipt.get("location") == location
        ]
        if not matching:
            raise AirflowException(
                f"replay output {output_path!r}: table location "
                f"{location!r} has no publication receipt"
            )
        receipt = matching[0]
        if receipt.get("table") != table_name:
            raise AirflowException(
                f"replay output {output_path!r}: receipt table "
                f"{receipt.get('table')!r} does not match the last location "
                f"component {table_name!r} of {location!r}"
            )
        receipt_rows = receipt.get("rows")
        if (
            isinstance(receipt_rows, bool)
            or not isinstance(receipt_rows, int)
            or receipt_rows < 0
        ):
            raise AirflowException(
                f"replay output {output_path!r}: receipt rows for table "
                f"{table_name!r} must be a nonnegative integer, got "
                f"{receipt_rows!r}"
            )
    rows = report.get("rows")
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
        raise AirflowException(
            f"replay output {output_path!r}: 'rows' must be a nonnegative "
            f"integer, got {rows!r}"
        )
    return len(tables), rows


def validate_replay_summary(**context: Any) -> dict[str, Any]:
    """Fail closed on any replay violation and return the run summary."""
    environments = context["ti"].xcom_pull(task_ids="planner") or []
    if not environments:
        logger.info("SofaScore raw replay: empty plan, nothing to validate")
        return {"scopes": 0, "tables": 0, "rows": 0, "source_request_count": 0}

    total_tables = 0
    total_rows = 0
    for environment in environments:
        output_path = (
            environment.get("SOFASCORE_REPLAY_OUTPUT")
            if isinstance(environment, Mapping)
            else None
        )
        if not isinstance(output_path, str) or not output_path:
            raise AirflowException(
                "replay plan entry misses a SOFASCORE_REPLAY_OUTPUT string"
            )
        report = _load_replay_report(output_path)
        tables, rows = _validate_replay_report(output_path, report)
        total_tables += tables
        total_rows += rows
    return {
        "scopes": len(environments),
        "tables": total_tables,
        "rows": total_rows,
        "source_request_count": 0,
    }


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Offline SofaScore raw replay of captured manifest scopes",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=2),
    render_template_as_native_obj=True,
    params=PARAMS,
    tags=["sofascore", "replay"],
) as dag:
    planner = PythonOperator(
        task_id="planner",
        python_callable=plan_replay_scopes,
        execution_timeout=timedelta(minutes=5),
    )

    replay_scope = BashOperator.partial(
        task_id="replay_scope",
        bash_command=RUN_REPLAY_SCOPE_COMMAND,
        append_env=True,
        cwd="/opt/airflow",
        pool=INGEST_SCRAPER_POOL,
        execution_timeout=timedelta(minutes=30),
    ).expand(env=planner.output)

    validate_summary = PythonOperator(
        task_id="validate_summary",
        python_callable=validate_replay_summary,
        execution_timeout=timedelta(minutes=5),
        trigger_rule='all_success',
    )

    planner >> replay_scope >> validate_summary


__all__ = [
    "dag",
    "plan_replay_scopes",
    "validate_replay_summary",
]
