"""Source-native FotMob ingestion DAG.

The DAG is trigger-only: ``dag_master_pipeline`` is the single daily schedule
owner.  One isolated runner performs catalog discovery, exact-season planning,
raw-first ingestion and emits an atomic, run-specific report.  Validation is
fail-closed and Silver can only run after a complete native report.

With a deployed publication ceremony every production run must be launched by
one schedule owner with an exact ``fotmob_publication`` binding; an ad-hoc
direct trigger has no durable writer lock and therefore fails before touching
Bronze.  Without a deployment report the ceremony is disabled and the fence
degrades to the plain ``FOTMOB_ISOLATED_STACK`` gate
(``utils.fotmob_publication.fotmob_ceremony_configured``).
"""

import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.config import DAG_TAGS, FOTMOB_HTTP_POOL, SCHEDULES
from utils.default_args import SCRAPER_ARGS
from utils.fotmob_publication import (
    FOTMOB_CATALOG_CONTRACT_SCHEMA,
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
    record_fotmob_bronze_only_candidate,
    seal_fotmob_publication,
    validate_fotmob_writer_fence,
)
from scrapers.fotmob.scope_codec import (
    format_scope_token,
    parse_scope_token,
    validate_scope_tokens,
)
from scrapers.fotmob.source_refresh import (
    PLAYER_SOURCE_REFRESH_ARTIFACT,
    PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB,
    PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
    PLAYER_SOURCE_REFRESH_PROFILE,
    PLAYER_SOURCE_REFRESH_SHA256,
    PLAYER_SOURCE_REFRESH_TARGET_COUNT,
    REPLAY_MISSING_INPUT_PROOF_SCHEMA as REPLAY_MISSING_INPUT_SCHEMA,
    REPLAY_MISSING_INPUT_PROOF_TASK_ID,
    load_player_source_refresh_contract,
)


RESULT_PATH = "/tmp/fotmob_result_{{ ts_nodash }}.json"
NATIVE_MODES = frozenset({"discover", "daily", "backfill", "replay", "refresh"})
FOTMOB_SILVER_BRONZE_INPUTS = frozenset(
    {
        "iceberg.bronze.fotmob_competition_seasons",
        "iceberg.bronze.fotmob_season_teams",
        "iceberg.bronze.fotmob_matches",
        "iceberg.bronze.fotmob_match_payloads",
        "iceberg.bronze.fotmob_standings",
        "iceberg.bronze.fotmob_leaderboards",
        "iceberg.bronze.fotmob_squad_snapshots",
        "iceberg.bronze.fotmob_player_snapshots",
        "iceberg.bronze.fotmob_team_snapshots",
        "iceberg.bronze.fotmob_transfer_events",
    }
)
ISSUE_930_REPLAY_ENTITIES = [
    "leaderboards",
    "matches",
    "players",
    "season",
    "teams",
]
# The conf may legitimately lack ``fotmob_publication`` when the publication
# ceremony is not deployed (``utils.fotmob_publication
# .fotmob_ceremony_configured``), so the templates render empty strings
# instead of failing.  The runner itself decides what the rendered arguments
# mean: a full canonical set is validated cryptographically, an all-empty or
# ceremony-disabled stub set runs without the ControlStore guard, and a
# partial set still fails.  Keeping that decision in the runner (not in
# Jinja) keeps it unit-testable.
PUBLICATION_CONF_EXPR = "(dag_run.conf.get('fotmob_publication') or {})"
PUBLICATION_GENERATION_TEMPLATE = (
    "{{ " + PUBLICATION_CONF_EXPR + ".get('generation_id', '') }}"
)
PUBLICATION_BINDING_TEMPLATE = {
    field: (
        "{{ ("
        + PUBLICATION_CONF_EXPR
        + ".get('binding') or {}).get('"
        + field
        + "', '') }}"
    )
    for field in (
        "schema",
        "source",
        "owner",
        "data_interval_start",
        "data_interval_end",
        "runtime_fingerprint",
    )
}
# Without a publication the Silver child still needs a unique, retry-stable
# run id; the ingest DagRun's own run_id is exactly that.
SILVER_TRIGGER_RUN_ID_TEMPLATE = (
    "{{ 'fotmob_silver__' ~ ("
    + PUBLICATION_CONF_EXPR
    + ".get('generation_id') or run_id) }}"
)


def _validated_exact_scope_evidence(value: Any) -> tuple[str, ...] | None:
    """Return canonical unique scope evidence, or ``None`` when it is unsafe."""

    if not isinstance(value, list):
        return None
    try:
        tokens = validate_scope_tokens(value)
    except ValueError:
        return None
    return tokens if tuple(value) == tokens else None


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
    completed_transfer_ids = selection.get("completed_transfer_competition_ids")

    if selection.get("daily_contract") != FOTMOB_DAILY_CONTRACT_SCHEMA:
        violations.append("daily contract schema mismatch")
    if competition_scope != expected_scope:
        violations.append("daily competition scope mismatch")
    if raw_scopes:
        violations.append("daily exact season scope must be empty")
    if entities != sorted(FOTMOB_DAILY_ENTITIES):
        violations.append("daily entity set mismatch")
    if selection.get("competition_limit") != 0 or selection.get("season_limit") != 0:
        violations.append("daily planner limits must be zero")
    if selection.get("requests_per_minute") != FOTMOB_DAILY_REQUESTS_PER_MINUTE:
        violations.append("daily request rate mismatch")
    if (
        budget.get("max_requests") != FOTMOB_DAILY_MAX_REQUESTS
        or budget.get("max_direct_bytes") != FOTMOB_DAILY_MAX_DIRECT_MIB * 1024 * 1024
        or budget.get("max_proxy_bytes") != 0
    ):
        violations.append("daily transport budget mismatch")

    planned_tokens = _validated_exact_scope_evidence(planned_scopes)
    planned_pairs: list[tuple[int, str]] = []
    if planned_tokens is None:
        violations.append("invalid daily planned scope evidence")
    elif not planned_tokens:
        violations.append("missing daily planned scopes")
    else:
        planned_pairs = [parse_scope_token(scope) for scope in planned_tokens]
        if {competition_id for competition_id, _season in planned_pairs} != set(
            expected_ids
        ):
            violations.append("daily planned scopes do not cover exact cohort")
    if completed_scopes != planned_scopes:
        violations.append("daily completed scopes differ from exact plan")
    valid_transfer_ids = isinstance(completed_transfer_ids, list) and all(
        isinstance(value, int) and not isinstance(value, bool)
        for value in completed_transfer_ids
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


def _source_refresh_contract() -> Dict[str, Any]:
    return load_player_source_refresh_contract(
        Path(__file__).resolve().parents[1] / PLAYER_SOURCE_REFRESH_ARTIFACT
    )


def _validate_source_refresh_selection(
    *,
    result: Dict[str, Any],
    selection: Dict[str, Any],
    entities: list[str],
    raw_scopes: list[str],
    budget: Dict[str, Any],
) -> tuple[list[str], Dict[str, Any]]:
    """Validate the one seven-player network exception byte for byte."""

    violations: list[str] = []
    contract = _source_refresh_contract()
    expected_source = {
        key: contract[key]
        for key in (
            "profile",
            "artifact",
            "sha256",
            "target_count",
            "targets",
            "plan_signature",
        )
    }
    outcomes = selection.get("target_outcomes")
    valid_outcomes = (
        isinstance(outcomes, list)
        and len(outcomes) == PLAYER_SOURCE_REFRESH_TARGET_COUNT
        and all(
            isinstance(item, dict)
            and set(item)
            == {
                "competition_id",
                "source_season_key",
                "team_id",
                "player_id",
                "status",
            }
            and item.get("status") in {"success", "not_available"}
            for item in outcomes
        )
    )
    if valid_outcomes:
        observed_targets = [
            {key: item[key] for key in contract["targets"][0]} for item in outcomes
        ]
        valid_outcomes = observed_targets == contract["targets"]

    operations = result.get("operations") or []
    operation_entities = [
        item.get("entity") for item in operations if isinstance(item, dict)
    ]
    expected_operation_entities = {
        "player_snapshots",
        "player_source_refresh_contract",
        "commit_flush",
        "current_views",
    }
    if (
        len(operation_entities) != len(expected_operation_entities)
        or set(operation_entities) != expected_operation_entities
    ):
        violations.append("source refresh performed work outside seven players")

    player_operations = [
        item for item in operations if item.get("entity") == "player_snapshots"
    ]
    contract_operations = [
        item
        for item in operations
        if item.get("entity") == "player_source_refresh_contract"
    ]
    if len(player_operations) != 1:
        violations.append("missing exact source-refresh player operation")
    else:
        operation = player_operations[0]
        unavailable = sum(
            isinstance(item, dict) and item.get("status") == "not_available"
            for item in outcomes or []
        )
        if (
            operation.get("status") != "success"
            or operation.get("attempted") != PLAYER_SOURCE_REFRESH_TARGET_COUNT
            or operation.get("skipped") != 0
            or operation.get("not_available") != unavailable
            or int(operation.get("succeeded") or 0) + unavailable
            != PLAYER_SOURCE_REFRESH_TARGET_COUNT
        ):
            violations.append("source refresh lacks seven terminal player outcomes")
    if len(contract_operations) != 1:
        violations.append("missing source-refresh contract operation")
    else:
        operation = contract_operations[0]
        metadata = operation.get("metadata") or {}
        if (
            operation.get("status") != "success"
            or operation.get("attempted") != PLAYER_SOURCE_REFRESH_TARGET_COUNT
            or operation.get("succeeded") != PLAYER_SOURCE_REFRESH_TARGET_COUNT
            or (operation.get("counts") or {}).get("terminal_targets")
            != PLAYER_SOURCE_REFRESH_TARGET_COUNT
            or metadata.get("profile") != PLAYER_SOURCE_REFRESH_PROFILE
            or metadata.get("targets_sha256") != PLAYER_SOURCE_REFRESH_SHA256
            or metadata.get("target_outcomes") != outcomes
        ):
            violations.append("source-refresh contract operation differs")

    if selection.get("profile") != PLAYER_SOURCE_REFRESH_PROFILE:
        violations.append("source-refresh profile mismatch")
    if selection.get("source_refresh") != expected_source:
        violations.append("source-refresh artifact binding mismatch")
    if not valid_outcomes:
        violations.append("source refresh did not prove exactly seven targets")
    if raw_scopes:
        violations.append("source refresh must not execute catalog scopes")
    if entities != ["players"]:
        violations.append("source refresh entities must be exactly players")
    if (
        selection.get("competition_limit") != 0
        or selection.get("season_limit") != 0
        or selection.get("planned_scopes") != []
        or selection.get("completed_scopes") != []
        or selection.get("completed_transfer_competition_ids") != []
    ):
        violations.append("source refresh planner surface is not empty")
    if selection.get("requests_per_minute") != 30:
        violations.append("source refresh request rate mismatch")
    if selection.get("scope_plan_signature") != contract["plan_signature"]:
        violations.append("source refresh plan signature mismatch")
    if (
        budget.get("max_requests") != PLAYER_SOURCE_REFRESH_MAX_REQUESTS
        or budget.get("max_direct_bytes")
        != PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB * 1024 * 1024
        or budget.get("max_proxy_bytes") != 0
    ):
        violations.append("source refresh transport budget mismatch")

    return violations, {
        "profile": selection.get("profile"),
        "source_refresh": selection.get("source_refresh"),
        "target_outcomes": outcomes,
        "planned_scopes": selection.get("planned_scopes"),
        "completed_scopes": selection.get("completed_scopes"),
        "requests_per_minute": selection.get("requests_per_minute"),
    }


def prove_replay_missing_player_inputs(
    result_path: str = "/tmp/fotmob_result.json",
    **context,
) -> Dict[str, Any] | None:
    """Return an authorizing proof only for the exact gap-only first replay."""

    import json

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                raise AirflowException(f"duplicate FotMob replay JSON key: {key!r}")
            output[key] = value
        return output

    dag_run = context.get("dag_run")
    task_instance_getter = getattr(dag_run, "get_task_instance", None)
    if not callable(task_instance_getter):
        raise AirflowException("FotMob replay proof lacks exact task-state access")

    def task_state(task_id: str) -> str:
        task_instance = task_instance_getter(task_id=task_id)
        state = getattr(task_instance, "state", None)
        return str(getattr(state, "value", state) or "").casefold()

    def task_try_number(task_id: str) -> int:
        task_instance = task_instance_getter(task_id=task_id)
        try:
            return int(getattr(task_instance, "try_number"))
        except (TypeError, ValueError) as exc:
            raise AirflowException(
                "FotMob replay proof has invalid task attempt evidence"
            ) from exc

    current_task_instance = context.get("ti")
    if (
        task_state("validate_publication_writer_fence") != "success"
        or task_state("scrape_fotmob_data") != "failed"
        or task_try_number("validate_publication_writer_fence") != 1
        or task_try_number("scrape_fotmob_data") != 1
        or getattr(current_task_instance, "task_id", None)
        != REPLAY_MISSING_INPUT_PROOF_TASK_ID
        or int(getattr(current_task_instance, "try_number", 0) or 0) != 1
    ):
        return None

    path = Path(result_path)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise AirflowException(
            f"FotMob replay report not found: {result_path}"
        ) from exc
    if not raw or len(raw) > 64 * 1024 * 1024:
        raise AirflowException("FotMob replay report has an unsafe byte size")
    try:
        result = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AirflowException(
            "FotMob replay report is not unique-key UTF-8 JSON"
        ) from exc
    if not isinstance(result, Mapping):
        raise AirflowException("FotMob replay report must be a JSON object")
    if result.get("mode") != "replay":
        return None

    conf = getattr(dag_run, "conf", None)
    publication = conf.get("fotmob_publication") if isinstance(conf, Mapping) else None
    generation_id = (
        publication.get("generation_id") if isinstance(publication, Mapping) else None
    )
    ingest_run_id = str(getattr(dag_run, "run_id", "") or "")
    if (
        not isinstance(generation_id, str)
        or re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", generation_id)
        is None
        or result.get("run_id") != generation_id
    ):
        raise AirflowException("FotMob replay gap proof is not generation-bound")
    if ingest_run_id != f"issue930_replay_a1__{generation_id.replace('-', '')}":
        return None
    if result.get("status") == "success" and result.get("complete") is True:
        return None

    contract = _source_refresh_contract()
    from scrapers.fotmob.planner import deterministic_plan_signature
    expected_replay_plan = deterministic_plan_signature(
        ISSUE_930_REPLAY_ENTITIES,
        policy={
            "match_policy": "finished_only",
            "leaderboard_policy": "all_advertised",
            "team_policy": "global_observed_snapshot",
            "player_policy": "global_observed_snapshot",
        },
    )

    selection = result.get("selection")
    transport = result.get("transport")
    budget = result.get("budget")
    if not all(isinstance(value, Mapping) for value in (selection, transport, budget)):
        raise AirflowException("FotMob replay gap proof lacks typed runner evidence")
    scopes = selection.get("explicit_scopes")
    scope_tokens = _validated_exact_scope_evidence(scopes)
    entities = selection.get("entities")
    plan_signature = selection.get("scope_plan_signature")
    typed = selection.get("replay_missing_player_inputs")
    required_transport = {"attempts", "direct_bytes", "proxy_bytes"}
    required_budget = {
        "requests",
        "max_requests",
        "direct_bytes",
        "max_direct_bytes",
        "proxy_bytes",
        "max_proxy_bytes",
    }
    if (
        result.get("status") != "incomplete"
        or result.get("complete") is not False
        or not isinstance(result.get("errors"), list)
        or not result.get("errors")
        or not scope_tokens
        or entities != ISSUE_930_REPLAY_ENTITIES
        or selection.get("competition_limit") != 0
        or selection.get("season_limit") != 0
        or plan_signature != expected_replay_plan
        or not required_transport.issubset(transport)
        or any(type(transport[key]) is not int for key in required_transport)
        or any(transport[key] != 0 for key in required_transport)
        or not required_budget.issubset(budget)
        or any(type(budget[key]) is not int for key in required_budget)
        or any(budget[key] != 0 for key in ("requests", "direct_bytes", "proxy_bytes"))
        or budget.get("max_requests") != 2_000
        or budget.get("max_direct_bytes") != 256 * 1024 * 1024
        or budget.get("max_proxy_bytes") != 0
        or not isinstance(typed, Mapping)
        or set(typed)
        != {
            "schema",
            "failure_class",
            "missing_player_ids",
            "affected_scopes",
        }
        or typed.get("schema") != REPLAY_MISSING_INPUT_SCHEMA
        or typed.get("failure_class") != "missing_player_raw_inputs_only"
    ):
        raise AirflowException("FotMob replay failure is not a typed offline raw gap")

    contract = _source_refresh_contract()
    affected_scopes = sorted(
        {
            format_scope_token(
                target["competition_id"], target["source_season_key"]
            )
            for target in contract["targets"]
        }
    )
    if (
        typed.get("missing_player_ids") != contract["player_ids"]
        or typed.get("affected_scopes") != affected_scopes
    ):
        raise AirflowException(
            "FotMob replay missing inputs differ from the reviewed seven targets"
        )
    planned = selection.get("planned_scopes")
    completed = selection.get("completed_scopes")
    if (
        not isinstance(planned, list)
        or not isinstance(completed, list)
        or len(planned) != len(set(planned))
        or len(completed) != len(set(completed))
        or set(planned) != set(scope_tokens)
        or set(completed).intersection(affected_scopes)
        or set(completed).union(affected_scopes) != set(planned)
        or len(completed) + len(affected_scopes) != len(planned)
    ):
        raise AirflowException(
            "FotMob replay gap scopes do not partition the exact plan"
        )

    scope_bytes = ("\n".join(scopes) + "\n").encode("utf-8")
    scope_sha256 = hashlib.sha256(scope_bytes).hexdigest()
    if (
        len(scopes) != FOTMOB_DAILY_SCOPE_COUNT
        or scope_sha256 != FOTMOB_DAILY_SCOPE_SHA256
    ):
        raise AirflowException(
            "FotMob replay gap proof is outside exact issue-930 scope"
        )
    return {
        "schema_version": REPLAY_MISSING_INPUT_SCHEMA,
        "status": "source_refresh_required",
        "runner_result_sha256": hashlib.sha256(raw).hexdigest(),
        "run_id": generation_id,
        "mode": "replay",
        "scope_count": len(scopes),
        "scope_sha256": scope_sha256,
        "entities": list(entities),
        "plan_signature": plan_signature,
        "artifact_sha256": contract["sha256"],
        "target_count": contract["target_count"],
        "targets": contract["targets"],
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
        raw_result = Path(result_path).read_bytes()
        if not raw_result or len(raw_result) > 32 * 1024 * 1024:
            raise AirflowException("FotMob report has an unsafe byte size")
        result = json.loads(raw_result.decode("utf-8"))
    except FileNotFoundError as exc:
        raise AirflowException(f"FotMob report not found: {result_path}") from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AirflowException(
            f"Invalid FotMob report JSON at {result_path}: {exc}"
        ) from exc

    mode = str(result.get("mode") or "")
    if mode not in NATIVE_MODES:
        raise AirflowException(
            f"Unsupported FotMob report mode {mode!r}; native mode is required"
        )
    if mode in NATIVE_MODES:
        selection = result.get("selection")
        contract_evidence = (
            selection.get("catalog_contract")
            if isinstance(selection, dict)
            else None
        )
        automatic_catalog = (
            isinstance(contract_evidence, dict)
            and contract_evidence.get("schema") == FOTMOB_CATALOG_CONTRACT_SCHEMA
        )
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
        if automatic_catalog:
            valid_status_shape = (
                result.get("status") == "success"
                and result.get("complete") is True
            ) or (
                result.get("status") == "partial_success"
                and result.get("complete") is False
            )
            if not valid_status_shape:
                violations.append(
                    "automatic status/complete evidence is inconsistent"
                )
        elif result.get("status") != "success" or result.get("complete") is not True:
            violations.append(
                f"status={result.get('status')!r}, complete={result.get('complete')!r}"
            )
        if result.get("errors") and not automatic_catalog:
            violations.append(f"runner errors={result['errors']!r}")
        if operation_failures and not automatic_catalog:
            violations.append(f"operation failures={operation_failures!r}")
        if automatic_catalog:
            for failure in operation_failures:
                hard_errors = [
                    error
                    for error in failure["errors"]
                    if "request budget" not in str(error).casefold()
                    and "direct-byte budget" not in str(error).casefold()
                ]
                if hard_errors or failure["terminal"]:
                    violations.append(
                        "automatic hard operation failure="
                        f"{failure['entity']!r}: errors={hard_errors!r}, "
                        f"terminal={failure['terminal']!r}"
                    )
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
        selection_profile = (
            str(selection.get("profile") or "").strip()
            if isinstance(selection, dict)
            else ""
        )
        if not selection_profile:
            catalog_counts = [
                int((operation.get("counts") or {}).get("competitions") or 0)
                for operation in result.get("operations") or []
                if operation.get("entity") == "competition_catalog"
            ]
            if not catalog_counts or max(catalog_counts) <= 0:
                violations.append("complete competition catalog was not recorded")
        selection_summary = None
        if mode != "discover":
            if not isinstance(selection, dict):
                violations.append("missing exact native selection evidence")
            else:
                raw_scopes = selection.get("explicit_scopes")
                scope_tokens = _validated_exact_scope_evidence(raw_scopes)
                entities = selection.get("entities")
                signature = str(selection.get("scope_plan_signature") or "")
                if scope_tokens is None:
                    violations.append("invalid exact scope selection evidence")
                elif (
                    not isinstance(entities, list)
                    or any(
                        not isinstance(entity, str) or not entity for entity in entities
                    )
                    or entities != sorted(set(entities))
                ):
                    violations.append("invalid native entity selection evidence")
                elif re.fullmatch(r"fmplan1-[0-9a-f]{64}", signature) is None:
                    violations.append("invalid native scope plan signature")
                else:
                    scope_bytes = (
                        ("\n".join(scope_tokens) + "\n").encode("utf-8")
                        if scope_tokens
                        else b""
                    )
                    selection_summary = {
                        "entities": entities,
                        "explicit_scope_count": len(scope_tokens),
                        "explicit_scope_sha256": hashlib.sha256(
                            scope_bytes
                        ).hexdigest(),
                        "scope_plan_signature": signature,
                        "competition_limit": selection.get("competition_limit"),
                        "season_limit": selection.get("season_limit"),
                    }
                    if selection_profile:
                        source_violations, source_summary = (
                            _validate_source_refresh_selection(
                                result=result,
                                selection=selection,
                                entities=entities,
                                raw_scopes=list(scope_tokens),
                                budget=budget,
                            )
                        )
                        if mode != "backfill":
                            source_violations.append(
                                "source refresh mode must be backfill"
                            )
                        violations.extend(source_violations)
                        selection_summary.update(source_summary)
                    elif mode == "daily" and not automatic_catalog:
                        daily_violations, daily_summary = _validate_daily_selection(
                            result=result,
                            selection=selection,
                            entities=entities,
                            raw_scopes=list(scope_tokens),
                            budget=budget,
                        )
                        violations.extend(daily_violations)
                        selection_summary.update(daily_summary)
        if automatic_catalog:
            from scripts.fotmob_catalog_acceptance import validate_report

            # Полоса автоматического каталога — непрерывный обход ~450 скоупов
            # под временным бюджетом, а не кампания с обязательством закрыть
            # каталог за один ран. Скоуп, отложенный на повтор, здесь штатное
            # промежуточное состояние. Строгий режим остаётся ПО УМОЛЧАНИЮ —
            # его получает CLI приёмки (scripts/fotmob_catalog_acceptance.py),
            # которым проверяют кампанию и канарейку.
            acceptance = validate_report(result, require_full_completion=False)
            acceptance_errors = list(acceptance.errors)
            if acceptance_errors:
                violations.append(
                    "automatic catalog evidence failed: "
                    + "; ".join(acceptance_errors)
                )
        if violations:
            raise AirflowException(
                "Incomplete FotMob native ingest: " + "; ".join(violations)
            )
        raw_tables = result.get("tables")
        if raw_tables is None:
            raw_tables = []
        if not isinstance(raw_tables, list) or any(
            not isinstance(table, str) for table in raw_tables
        ):
            raise AirflowException("FotMob committed table evidence is invalid")
        bronze_inputs_changed = sorted(
            {
                table.strip().casefold()
                for table in raw_tables
                if table.strip().casefold().startswith("iceberg.bronze.")
            }
        )
        summary = {
            "status": result.get("status"),
            "run_id": result.get("run_id"),
            "mode": mode,
            "rows": result.get("rows") or {},
            "tables": raw_tables,
            "bronze_inputs_changed": bronze_inputs_changed,
            "transport": transport,
            "budget": budget,
            "selection": selection_summary,
            # The kept-paused automatic-canary coordinator reads this exact
            # file from the admitted scheduler and re-hashes it before it can
            # become rollout evidence.  XCom carries only the locator/digest,
            # never an unbounded copy of the full catalog report.
            "runner_report_path": str(Path(result_path)),
            "runner_report_sha256": hashlib.sha256(raw_result).hexdigest(),
            "runner_report_bytes": len(raw_result),
        }
        logger.info("FotMob native validation complete: %s", summary)
        return summary


def _should_transform(**context: Any) -> bool:
    """Run Silver only when validated committed Bronze inputs changed."""

    ti = context.get("ti")
    validation = ti.xcom_pull(task_ids="validate_data") if ti is not None else None
    if not isinstance(validation, Mapping):
        raise AirflowException("FotMob validate_data XCom is missing")
    changed = validation.get("bronze_inputs_changed")
    if not isinstance(changed, list) or any(
        not isinstance(table, str) for table in changed
    ):
        raise AirflowException("FotMob changed Bronze input evidence is invalid")
    normalized = {table.strip().casefold() for table in changed}
    return bool(normalized & FOTMOB_SILVER_BRONZE_INPUTS)


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
        "catalog_contract": Param(
            default="",
            type="string",
            enum=["", FOTMOB_CATALOG_CONTRACT_SCHEMA],
        ),
        "daily_contract": Param(default="", type="string"),
        "competition_scope_file": Param(default="", type="string"),
        "competition_scope_sha256": Param(default="", type="string"),
        "competition_ids_sha256": Param(default="", type="string"),
        "source_refresh_profile": Param(
            default="", type="string", enum=["", PLAYER_SOURCE_REFRESH_PROFILE]
        ),
        "source_refresh_targets_sha256": Param(
            default="", type="string", enum=["", PLAYER_SOURCE_REFRESH_SHA256]
        ),
        "source_refresh_target_count": Param(
            default=0,
            type="integer",
            enum=[0, PLAYER_SOURCE_REFRESH_TARGET_COUNT],
        ),
        "deadline": Param(default="", type="string"),
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
        "match_limit": Param(default=0, type="integer", minimum=0),
        "team_limit": Param(default=0, type="integer", minimum=0),
        "player_limit": Param(default=0, type="integer", minimum=0),
        "max_proxy_mib": Param(default=0, type="integer", minimum=0, maximum=0),
        "max_attempts": Param(default=4, type="integer", enum=[4]),
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

    With a deployed publication ceremony production runs are parent-only: the
    shared master or an isolated schedule owner supplies the exact
    interval/release/owner publication generation and direct CLI/UI triggers
    are rejected before the Bronze writer.  Without a deployment report the
    ceremony is disabled and only the isolated-stack gate remains.
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
/usr/bin/rm -f -- "{RESULT_PATH}" && \\
python dags/scripts/run_fotmob_scraper.py \\
    --publication-generation-id "{PUBLICATION_GENERATION_TEMPLATE}" \\
    --publication-schema "{PUBLICATION_BINDING_TEMPLATE["schema"]}" \\
    --publication-source "{PUBLICATION_BINDING_TEMPLATE["source"]}" \\
    --publication-owner "{PUBLICATION_BINDING_TEMPLATE["owner"]}" \\
    --publication-data-interval-start "{PUBLICATION_BINDING_TEMPLATE["data_interval_start"]}" \\
    --publication-data-interval-end "{PUBLICATION_BINDING_TEMPLATE["data_interval_end"]}" \\
    --publication-runtime-fingerprint "{PUBLICATION_BINDING_TEMPLATE["runtime_fingerprint"]}" \\
    --mode "{{{{ params.mode }}}}" \\
    --catalog-contract "{{{{ params.catalog_contract }}}}" \\
    --daily-contract "{{{{ params.daily_contract }}}}" \\
    --competition-scope-file "{{{{ params.competition_scope_file }}}}" \\
    --competition-scope-sha256 "{{{{ params.competition_scope_sha256 }}}}" \\
    --competition-ids-sha256 "{{{{ params.competition_ids_sha256 }}}}" \\
    --source-refresh-profile "{{{{ params.source_refresh_profile }}}}" \\
    --source-refresh-targets-sha256 "{{{{ params.source_refresh_targets_sha256 }}}}" \\
    --entities "{{{{ params.entities }}}}" \\
    --max-requests "{{{{ params.max_requests }}}}" \\
    --max-direct-mib "{{{{ params.max_direct_mib }}}}" \\
    --max-proxy-mib "{{{{ params.max_proxy_mib }}}}" \\
    --competition-limit "{{{{ params.competition_limit }}}}" \\
    --season-limit "{{{{ params.season_limit }}}}" \\
    --match-limit "{{{{ params.match_limit }}}}" \\
    --team-limit "{{{{ params.team_limit }}}}" \\
    --player-limit "{{{{ params.player_limit }}}}" \\
    --max-attempts "{{{{ params.max_attempts }}}}" \\
    --next-build-id "" \\
    --requests-per-minute "{{{{ params.requests_per_minute }}}}" \\
    --deadline "{{{{ params.deadline }}}}" \\
    --workers 4 \\
    --output "{RESULT_PATH}"
""",
        env={
            "FOTMOB_SCOPE_JSON": "{{ params.scope | tojson }}",
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

    replay_missing_inputs_proof = PythonOperator(
        task_id=REPLAY_MISSING_INPUT_PROOF_TASK_ID,
        python_callable=prove_replay_missing_player_inputs,
        op_kwargs={"result_path": RESULT_PATH},
        trigger_rule="all_done",
        retries=0,
    )

    transform_gate = ShortCircuitOperator(
        task_id="season_data_available",
        python_callable=_should_transform,
        ignore_downstream_trigger_rules=False,
    )

    record_bronze_only_candidate = PythonOperator(
        task_id="record_bronze_only_publication_candidate",
        python_callable=record_fotmob_bronze_only_candidate,
        op_kwargs={
            "validation_task_id": "validate_data",
            "silver_input_tables": sorted(FOTMOB_SILVER_BRONZE_INPUTS),
        },
        retries=0,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="dag_transform_fotmob_silver",
        trigger_run_id=SILVER_TRIGGER_RUN_ID_TEMPLATE,
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
        trigger_rule="none_failed_min_one_success",
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

    publication_preflight >> scrape_data_task >> validate_data_task
    validate_data_task >> record_bronze_only_candidate
    validate_data_task >> transform_gate
    transform_gate >> trigger_silver
    [record_bronze_only_candidate, trigger_silver] >> seal_publication
    seal_publication >> finalize_publication
    scrape_data_task >> replay_missing_inputs_proof >> finalize_publication
