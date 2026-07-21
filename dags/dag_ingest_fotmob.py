"""Source-native FotMob ingestion DAG.

The DAG is trigger-only: ``dag_master_pipeline`` is the single daily schedule
owner.  One isolated runner performs catalog discovery, exact-season planning,
raw-first ingestion and emits an atomic, run-specific report.  Validation is
fail-closed and Silver can only run after a complete native report.

Every production run must be launched by one schedule owner with an exact
``fotmob_publication`` binding.  An ad-hoc direct trigger has no durable writer
lock and therefore fails before touching Bronze.
"""

import hashlib
import re
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.config import DAG_TAGS, FOTMOB_HTTP_POOL, SCHEDULES
from utils.default_args import SCRAPER_ARGS
from utils.fotmob_publication import (
    FOTMOB_DAILY_COMPETITION_COUNT,
    FOTMOB_DAILY_COMPETITION_IDS,
    FOTMOB_DAILY_COMPETITION_IDS_SHA256,
    FOTMOB_DAILY_CONTRACT_SCHEMA,
    FOTMOB_DAILY_ENTITIES,
    FOTMOB_DAILY_MAX_DIRECT_MIB,
    FOTMOB_DAILY_MAX_REQUESTS,
    FOTMOB_DAILY_REQUESTS_PER_MINUTE,
    FOTMOB_DAILY_SCOPE_COUNT,
    FOTMOB_DAILY_SCOPE_FILE,
    FOTMOB_DAILY_SCOPE_SHA256,
    fail_unsealed_fotmob_publication,
    seal_fotmob_publication,
    validate_fotmob_writer_fence,
)


RESULT_PATH = "/tmp/fotmob_result_{{ ts_nodash }}_{{ ti.try_number }}.json"
NATIVE_MODES = frozenset({"discover", "daily", "backfill", "replay"})
PUBLICATION_GENERATION_TEMPLATE = (
    "{{ dag_run.conf['fotmob_publication']['generation_id'] }}"
)
PUBLICATION_BINDING_TEMPLATE = {
    "schema": "{{ dag_run.conf['fotmob_publication']['binding']['schema'] }}",
    "source": "{{ dag_run.conf['fotmob_publication']['binding']['source'] }}",
    "owner": "{{ dag_run.conf['fotmob_publication']['binding']['owner'] }}",
    "data_interval_start": (
        "{{ dag_run.conf['fotmob_publication']['binding']"
        "['data_interval_start'] }}"
    ),
    "data_interval_end": (
        "{{ dag_run.conf['fotmob_publication']['binding']"
        "['data_interval_end'] }}"
    ),
    "runtime_fingerprint": (
        "{{ dag_run.conf['fotmob_publication']['binding']"
        "['runtime_fingerprint'] }}"
    ),
}


def _validate_daily_selection(
    *,
    result: Dict[str, Any],
    selection: Dict[str, Any],
    entities: list[str],
    raw_scopes: list[str],
    budget: Dict[str, Any],
) -> tuple[list[str], Dict[str, Any]]:
    """Validate the exact all-entity dynamic-current production workload."""

    violations: list[str] = []
    expected_ids = list(FOTMOB_DAILY_COMPETITION_IDS)
    expected_scope = {
        "schema": FOTMOB_DAILY_CONTRACT_SCHEMA,
        "scope_file": FOTMOB_DAILY_SCOPE_FILE,
        "scope_sha256": FOTMOB_DAILY_SCOPE_SHA256,
        "scope_count": FOTMOB_DAILY_SCOPE_COUNT,
        "competition_ids": expected_ids,
        "competition_ids_sha256": FOTMOB_DAILY_COMPETITION_IDS_SHA256,
        "competition_count": FOTMOB_DAILY_COMPETITION_COUNT,
    }
    competition_scope = selection.get("competition_scope")
    planned_scopes = selection.get("planned_scopes")
    completed_scopes = selection.get("completed_scopes")
    completed_transfer_ids = selection.get(
        "completed_transfer_competition_ids"
    )

    if selection.get("daily_contract") != FOTMOB_DAILY_CONTRACT_SCHEMA:
        violations.append("daily contract schema mismatch")
    if competition_scope != expected_scope:
        violations.append("daily competition scope mismatch")
    if raw_scopes:
        violations.append("daily exact season scope must be empty")
    if entities != sorted(FOTMOB_DAILY_ENTITIES):
        violations.append("daily entity set mismatch")
    if (
        selection.get("competition_limit") != 0
        or selection.get("season_limit") != 0
    ):
        violations.append("daily planner limits must be zero")
    if (
        selection.get("requests_per_minute")
        != FOTMOB_DAILY_REQUESTS_PER_MINUTE
    ):
        violations.append("daily request rate mismatch")
    if (
        budget.get("max_requests") != FOTMOB_DAILY_MAX_REQUESTS
        or budget.get("max_direct_bytes")
        != FOTMOB_DAILY_MAX_DIRECT_MIB * 1024 * 1024
        or budget.get("max_proxy_bytes") != 0
    ):
        violations.append("daily transport budget mismatch")

    planned_pairs: list[tuple[int, str]] = []
    if not isinstance(planned_scopes, list) or not planned_scopes:
        violations.append("missing daily planned scopes")
    else:
        for scope in planned_scopes:
            match = (
                re.fullmatch(r"([1-9][0-9]*)=(\S+)", scope)
                if isinstance(scope, str)
                else None
            )
            if match is None:
                violations.append("invalid daily planned scope evidence")
                break
            planned_pairs.append((int(match.group(1)), match.group(2)))
        if len(planned_pairs) != len(set(planned_pairs)):
            violations.append("duplicate daily planned scopes")
        if {
            competition_id for competition_id, _season in planned_pairs
        } != set(expected_ids):
            violations.append("daily planned scopes do not cover exact cohort")
    if completed_scopes != planned_scopes:
        violations.append("daily completed scopes differ from exact plan")
    valid_transfer_ids = (
        isinstance(completed_transfer_ids, list)
        and all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in completed_transfer_ids
        )
    )
    if not valid_transfer_ids or (
        len(completed_transfer_ids) != len(set(completed_transfer_ids))
        or set(completed_transfer_ids) != set(expected_ids)
    ):
        violations.append("daily transfer completions differ from exact cohort")

    operation_scopes = [
        (operation.get("metadata") or {}).get("scope")
        for operation in result.get("operations") or []
        if operation.get("entity") == "scope_completion"
        and operation.get("status") == "success"
    ]
    operation_transfer_ids = []
    for operation in result.get("operations") or []:
        if (
            operation.get("entity") != "competition_completion"
            or operation.get("status") != "success"
        ):
            continue
        raw_id = (operation.get("metadata") or {}).get("competition_id")
        try:
            operation_transfer_ids.append(int(raw_id))
        except (TypeError, ValueError):
            violations.append("invalid daily transfer completion operation")
    if operation_scopes != completed_scopes:
        violations.append("daily scope completion operations mismatch")
    if operation_transfer_ids != completed_transfer_ids:
        violations.append("daily transfer completion operations mismatch")

    return violations, {
        "daily_contract": selection.get("daily_contract"),
        "competition_scope": competition_scope,
        "planned_scopes": planned_scopes,
        "completed_scopes": completed_scopes,
        "completed_transfer_competition_ids": completed_transfer_ids,
        "requests_per_minute": selection.get("requests_per_minute"),
    }


def validate_data(
    result_path: str = "/tmp/fotmob_result.json",
    **context,
) -> Dict[str, Any]:
    """Fail unless the runner published a complete, direct-only report."""

    import json
    import logging

    logger = logging.getLogger(__name__)
    try:
        with open(result_path, "r", encoding="utf-8") as stream:
            result = json.load(stream)
    except FileNotFoundError as exc:
        raise AirflowException(f"FotMob report not found: {result_path}") from exc
    except json.JSONDecodeError as exc:
        raise AirflowException(
            f"Invalid FotMob report JSON at {result_path}: {exc}"
        ) from exc

    mode = str(result.get("mode") or "")
    if mode not in NATIVE_MODES:
        raise AirflowException(
            f"Unsupported FotMob report mode {mode!r}; native mode is required"
        )
    if mode in NATIVE_MODES:
        operation_failures = []
        for operation in result.get("operations") or []:
            if (
                operation.get("errors")
                or operation.get("retryable")
                or operation.get("terminal")
                or operation.get("status") in {"failed", "retryable"}
            ):
                operation_failures.append(
                    {
                        "entity": operation.get("entity"),
                        "status": operation.get("status"),
                        "errors": operation.get("errors") or [],
                        "retryable": operation.get("retryable") or [],
                        "terminal": operation.get("terminal") or [],
                    }
                )
        transport = result.get("transport") or {}
        budget = result.get("budget") or {}
        violations = []
        required_transport = {
            "attempts",
            "direct_bytes",
            "proxy_bytes",
        }
        required_budget = {
            "requests",
            "max_requests",
            "direct_bytes",
            "max_direct_bytes",
            "proxy_bytes",
            "max_proxy_bytes",
        }
        missing_transport = sorted(required_transport - transport.keys())
        missing_budget = sorted(required_budget - budget.keys())
        if missing_transport:
            violations.append(f"missing transport metrics={missing_transport!r}")
        if missing_budget:
            violations.append(f"missing budget metrics={missing_budget!r}")
        if result.get("status") != "success" or result.get("complete") is not True:
            violations.append(
                f"status={result.get('status')!r}, complete={result.get('complete')!r}"
            )
        if result.get("errors"):
            violations.append(f"runner errors={result['errors']!r}")
        if operation_failures:
            violations.append(f"operation failures={operation_failures!r}")
        if int(transport.get("proxy_bytes") or 0) != 0:
            violations.append(
                f"proxy_bytes={transport.get('proxy_bytes')} (direct-only invariant)"
            )
        if int(budget.get("proxy_bytes") or 0) != 0:
            violations.append(
                "budget proxy_bytes="
                f"{budget.get('proxy_bytes')} (direct-only invariant)"
            )
        if int(budget.get("requests") or 0) > int(budget.get("max_requests") or 0):
            violations.append("request budget exceeded")
        if int(budget.get("direct_bytes") or 0) > int(
            budget.get("max_direct_bytes") or 0
        ):
            violations.append("direct-byte budget exceeded")
        if int(budget.get("proxy_bytes") or 0) > int(
            budget.get("max_proxy_bytes") or 0
        ):
            violations.append("proxy-byte budget exceeded")
        if not result.get("operations"):
            violations.append("no native operations recorded")
        catalog_counts = [
            int((operation.get("counts") or {}).get("competitions") or 0)
            for operation in result.get("operations") or []
            if operation.get("entity") == "competition_catalog"
        ]
        if not catalog_counts or max(catalog_counts) <= 0:
            violations.append("complete competition catalog was not recorded")
        selection = result.get("selection")
        selection_summary = None
        if mode != "discover":
            if not isinstance(selection, dict):
                violations.append("missing exact native selection evidence")
            else:
                raw_scopes = selection.get("explicit_scopes")
                entities = selection.get("entities")
                signature = str(selection.get("scope_plan_signature") or "")
                if (
                    not isinstance(raw_scopes, list)
                    or any(
                        not isinstance(scope, str)
                        or re.fullmatch(r"[1-9][0-9]*=\S+", scope) is None
                        for scope in raw_scopes
                    )
                    or len(raw_scopes) != len(set(raw_scopes))
                ):
                    violations.append("invalid exact scope selection evidence")
                elif (
                    not isinstance(entities, list)
                    or any(not isinstance(entity, str) or not entity for entity in entities)
                    or entities != sorted(set(entities))
                ):
                    violations.append("invalid native entity selection evidence")
                elif re.fullmatch(r"fmplan1-[0-9a-f]{64}", signature) is None:
                    violations.append("invalid native scope plan signature")
                else:
                    scope_bytes = (
                        ("\n".join(raw_scopes) + "\n").encode("utf-8")
                        if raw_scopes
                        else b""
                    )
                    selection_summary = {
                        "entities": entities,
                        "explicit_scope_count": len(raw_scopes),
                        "explicit_scope_sha256": hashlib.sha256(
                            scope_bytes
                        ).hexdigest(),
                        "scope_plan_signature": signature,
                        "competition_limit": selection.get("competition_limit"),
                        "season_limit": selection.get("season_limit"),
                    }
                    if mode == "daily":
                        daily_violations, daily_summary = (
                            _validate_daily_selection(
                                result=result,
                                selection=selection,
                                entities=entities,
                                raw_scopes=raw_scopes,
                                budget=budget,
                            )
                        )
                        violations.extend(daily_violations)
                        selection_summary.update(daily_summary)
        if violations:
            raise AirflowException(
                "Incomplete FotMob native ingest: " + "; ".join(violations)
            )
        summary = {
            "status": "success",
            "run_id": result.get("run_id"),
            "mode": mode,
            "rows": result.get("rows") or {},
            "tables": result.get("tables") or [],
            "transport": transport,
            "budget": budget,
            "selection": selection_summary,
        }
        logger.info("FotMob native validation complete: %s", summary)
        return summary


def _should_transform(mode: str) -> bool:
    """Catalog-only discovery has no season facts for Silver to consume."""

    return str(mode) != "discover"


with DAG(
    dag_id="dag_ingest_fotmob",
    default_args=SCRAPER_ARGS,
    description="Discover and ingest source-native FotMob JSON",
    schedule=SCHEDULES.get("dag_ingest_fotmob"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get("fotmob", ["scraping", "fotmob", "bronze", "http"]),
    max_active_runs=1,
    params={
        "mode": Param(
            default="daily",
            type="string",
            enum=sorted(NATIVE_MODES),
            title="Native run mode",
        ),
        "scope": Param(
            default="",
            type="string",
            title="Exact scopes",
            description="Optional comma-separated FotMob ID=season keys",
        ),
        "daily_contract": Param(default="", type="string"),
        "competition_scope_file": Param(default="", type="string"),
        "competition_scope_sha256": Param(default="", type="string"),
        "competition_ids_sha256": Param(default="", type="string"),
        "entities": Param(
            default="season,leaderboards,matches,teams,players,transfers",
            type="string",
            title="Native entities",
            description=(
                "Season facts are always synchronized; optional enrichments: "
                "leaderboards,matches,teams,players,transfers"
            ),
        ),
        "max_requests": Param(default=2000, type="integer", minimum=1),
        "max_direct_mib": Param(default=256, type="integer", minimum=1),
        "competition_limit": Param(default=0, type="integer", minimum=0),
        "season_limit": Param(default=0, type="integer", minimum=0),
        "requests_per_minute": Param(
            default=30,
            type="integer",
            minimum=1,
            maximum=FOTMOB_DAILY_REQUESTS_PER_MINUTE,
        ),
    },
    doc_md="""
    ## FotMob native ingestion

    The runner discovers the complete ``allLeagues`` catalog, classifies every
    competition, preserves exact FotMob season strings and processes a bounded
    plan.  JSON is committed to the durable raw store before typed Bronze rows.
    Defaults are 2,000 requests, 256 MiB direct traffic, 0 proxy bytes, four
    workers and 30 requests/minute.  Use ``scope`` with numeric identities such
    as ``42=2025/2026,47=2025/2026``; names are never storage identities.

    ``discover`` writes catalog/season availability only. ``daily`` refreshes
    selected/latest seasons. ``backfill`` prioritizes required sentinels then
    active/newest and older source seasons. ``replay`` performs no network I/O.

    Production runs are parent-only. The shared master or isolated daily owner
    supplies the exact interval/release/owner publication generation; direct
    CLI/UI triggers are intentionally rejected before the Bronze writer.
    """,
) as dag:
    publication_preflight = PythonOperator(
        task_id="validate_publication_writer_fence",
        python_callable=validate_fotmob_writer_fence,
        retries=0,
    )

    scrape_data_task = BashOperator(
        task_id="scrape_fotmob_data",
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_fotmob_scraper.py \\
    --publication-generation-id "{PUBLICATION_GENERATION_TEMPLATE}" \\
    --publication-schema "{PUBLICATION_BINDING_TEMPLATE['schema']}" \\
    --publication-source "{PUBLICATION_BINDING_TEMPLATE['source']}" \\
    --publication-owner "{PUBLICATION_BINDING_TEMPLATE['owner']}" \\
    --publication-data-interval-start "{PUBLICATION_BINDING_TEMPLATE['data_interval_start']}" \\
    --publication-data-interval-end "{PUBLICATION_BINDING_TEMPLATE['data_interval_end']}" \\
    --publication-runtime-fingerprint "{PUBLICATION_BINDING_TEMPLATE['runtime_fingerprint']}" \\
    --mode "{{{{ params.mode }}}}" \\
    --scope "{{{{ params.scope }}}}" \\
    --daily-contract "{{{{ params.daily_contract }}}}" \\
    --competition-scope-file "{{{{ params.competition_scope_file }}}}" \\
    --competition-scope-sha256 "{{{{ params.competition_scope_sha256 }}}}" \\
    --competition-ids-sha256 "{{{{ params.competition_ids_sha256 }}}}" \\
    --entities "{{{{ params.entities }}}}" \\
    --max-requests "{{{{ params.max_requests }}}}" \\
    --max-direct-mib "{{{{ params.max_direct_mib }}}}" \\
    --max-proxy-mib 0 \\
    --competition-limit "{{{{ params.competition_limit }}}}" \\
    --season-limit "{{{{ params.season_limit }}}}" \\
    --requests-per-minute "{{{{ params.requests_per_minute }}}}" \\
    --workers 4 \\
    --output "{RESULT_PATH}"
""",
        env={
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
            "HOME": "/home/airflow",
        },
        append_env=True,
        pool=FOTMOB_HTTP_POOL,
        execution_timeout=timedelta(hours=8),
        retries=0,
    )

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        op_kwargs={"result_path": RESULT_PATH},
    )

    transform_gate = ShortCircuitOperator(
        task_id="season_data_available",
        python_callable=_should_transform,
        op_kwargs={"mode": "{{ params.mode }}"},
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="dag_transform_fotmob_silver",
        trigger_run_id=(
            "fotmob_silver__"
            + PUBLICATION_GENERATION_TEMPLATE
        ),
        logical_date="{{ logical_date.isoformat() }}",
        conf={
            "fotmob_publication": {
                "generation_id": PUBLICATION_GENERATION_TEMPLATE,
                "binding": PUBLICATION_BINDING_TEMPLATE,
            }
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        execution_timeout=timedelta(hours=4),
        retries=0,
    )

    seal_publication = PythonOperator(
        task_id="seal_fotmob_publication_ready",
        python_callable=seal_fotmob_publication,
        retries=0,
    )

    finalize_publication = PythonOperator(
        task_id="finalize_fotmob_publication",
        python_callable=fail_unsealed_fotmob_publication,
        op_kwargs={
            "success_task_id": "seal_fotmob_publication_ready",
            "writer_task_ids": [
                "scrape_fotmob_data",
                "trigger_silver_transform",
            ],
        },
        trigger_rule="all_done",
        retries=0,
    )

    (
        publication_preflight
        >> scrape_data_task
        >> validate_data_task
        >> transform_gate
        >> trigger_silver
        >> seal_publication
        >> finalize_publication
    )
