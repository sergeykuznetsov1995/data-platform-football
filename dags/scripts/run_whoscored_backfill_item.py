#!/usr/bin/env python3
"""Execute one immutable WhoScored backfill work item.

The Airflow controller creates at most 100 mapped invocations per DagRun.
Each invocation owns either one schedule freeze, one 25-match chunk, or one
200-player profile page.  Success is committed as a new S3 receipt; retries
never update a shared queue file.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from types import SimpleNamespace
from typing import Any, Mapping, Optional, Sequence

from dags.scripts import run_whoscored_scraper as runner
from dags.scripts.whoscored_ops_store import (
    schedule_request_units,
    WhoScoredBackfillState,
    WhoScoredOpsStoreError,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one WhoScored backfill item")
    parser.add_argument("--queue-id", required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--work-item", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--direct-only", action="store_true")
    return parser


def _decode_work_item(value: str) -> dict[str, Any]:
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.urlsafe_b64decode((value + padding).encode("ascii"))
        item = json.loads(decoded.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise WhoScoredOpsStoreError("invalid encoded backfill work item") from exc
    if not isinstance(item, dict):
        raise WhoScoredOpsStoreError("backfill work item must be an object")
    return item


def _airflow_identity() -> dict[str, Any]:
    return {
        "dag_id": os.environ.get("AIRFLOW_CTX_DAG_ID"),
        "dag_run_id": os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"),
        "task_id": os.environ.get("AIRFLOW_CTX_TASK_ID"),
        "try_number": os.environ.get("AIRFLOW_CTX_TRY_NUMBER"),
        "map_index": os.environ.get("AIRFLOW_CTX_MAP_INDEX"),
    }


def _operation_args(*, game_ids: Optional[list[int]] = None) -> SimpleNamespace:
    return SimpleNamespace(
        command="backfill",
        _match_ids=game_ids,
        _force_replay=True,
        _historical_replay=True,
        max_matches=None,
        profiles_limit=200,
    )


def _record_unscoped_error(report: dict[str, Any], exc: BaseException) -> None:
    report["errors"].append(f"backfill-work: {exc}")
    report["error_details"].append(
        {
            "scope": None,
            "entity": "backfill-work",
            "type": type(exc).__name__,
            "message": str(exc),
            "retryable": runner._is_retryable(exc),
        }
    )


def _filtered_candidates(
    service: Any,
    plan: Mapping[str, Any],
    scope: runner.RunnerScope,
) -> list[int]:
    selector = plan.get("selector", {})
    requested_ids = selector.get("game_ids") or None
    candidates = service.repository.list_completed_match_candidates(
        scope.competition_id,
        scope.season_id,
        match_ids=requested_ids,
    )
    date_from = selector.get("date_from")
    date_to = selector.get("date_to")
    result: list[int] = []
    for candidate in candidates:
        if candidate.kickoff is None and (date_from or date_to):
            continue
        kickoff_date = (
            candidate.kickoff.date().isoformat() if candidate.kickoff else None
        )
        if date_from and kickoff_date and kickoff_date < str(date_from):
            continue
        if date_to and kickoff_date and kickoff_date > str(date_to):
            continue
        result.append(int(candidate.game_id))
    frozen = sorted(set(result))
    if requested_ids is not None:
        requested = sorted({int(value) for value in requested_ids})
        missing = sorted(set(requested) - set(frozen))
        if missing:
            raise WhoScoredOpsStoreError(
                f"explicit game_ids are not completed matches in {scope.spec}: {missing}"
            )
        if frozen != requested:
            raise WhoScoredOpsStoreError(
                f"explicit game_ids did not freeze exactly in {scope.spec}"
            )
    return frozen


def _frozen_profile_ids(service: Any) -> list[int]:
    player_ids = service.repository.list_roster_player_ids(scopes=[service.scope])
    frozen = sorted({int(value) for value in player_ids})
    if any(value <= 0 for value in frozen):
        raise WhoScoredOpsStoreError("roster contains invalid profile player_ids")
    return frozen


def _frozen_preview_ids(
    service: Any,
    scope: runner.RunnerScope,
    candidate_game_ids: Sequence[int],
) -> list[int]:
    if not candidate_game_ids:
        return []
    candidates = service.repository.list_preview_candidates(
        scope.competition_id,
        scope.season_id,
        match_ids=list(candidate_game_ids),
        force_replay=True,
    )
    frozen = sorted({int(item["game_id"]) for item in candidates})
    if not set(frozen) <= set(candidate_game_ids):
        raise WhoScoredOpsStoreError(
            "preview candidates escaped completed match freeze"
        )
    return frozen


def _source_request_attempts(traffic: Mapping[str, Any]) -> int:
    """Count source-facing attempts without charging raw-cache replays."""

    total = 0
    for field, exclude_raw_cache in (("route_requests", True), ("failures", False)):
        counters = traffic.get(field, {})
        if not isinstance(counters, Mapping):
            raise WhoScoredOpsStoreError(f"traffic {field} is not a counter map")
        for key, raw_value in counters.items():
            if type(raw_value) is not int or raw_value < 0:
                raise WhoScoredOpsStoreError(f"traffic {field} has invalid counters")
            if exclude_raw_cache and str(key) == "raw_cache":
                continue
            total += raw_value
    return total


def _schedule_request_accounting(
    item: Mapping[str, Any],
    result: Any,
    traffic: Mapping[str, Any],
) -> dict[str, Any]:
    metadata = getattr(result, "metadata", None)
    if not isinstance(metadata, Mapping):
        raise WhoScoredOpsStoreError("schedule result has no source-stage metadata")
    source_stage_ids = metadata.get("source_stage_ids")
    if (
        not isinstance(source_stage_ids, list)
        or any(type(stage_id) is not int or stage_id <= 0 for stage_id in source_stage_ids)
        or source_stage_ids != sorted(set(source_stage_ids))
        or metadata.get("source_stage_count") != len(source_stage_ids)
    ):
        raise WhoScoredOpsStoreError("schedule result source-stage metadata is invalid")
    estimated_units = WhoScoredBackfillState.request_units(item)
    source_attempts = _source_request_attempts(traffic)
    actual_units = max(
        estimated_units,
        schedule_request_units(len(source_stage_ids)),
        source_attempts,
    )
    return {
        "source_stage_ids": list(source_stage_ids),
        "source_request_attempts": source_attempts,
        "estimated_request_units": estimated_units,
        "actual_request_units": actual_units,
    }


def _run_work_item(
    *,
    state: WhoScoredBackfillState,
    queue_id: str,
    plan_id: str,
    item: Mapping[str, Any],
    output: str,
) -> int:
    plan = state.load_plan(queue_id, plan_id)
    state.validate_work_item(plan, item)
    scope = runner.RunnerScope.parse(str(item["scope"]))
    report = runner._new_report("backfill", [scope])
    report["backfill_work"] = dict(item)
    report["direct_only"] = not bool(os.environ.get("WHOSCORED_PAID_PROXY_URL"))
    record = runner._scope_record(report, scope)
    outcome: dict[str, Any] = {}
    schedule_result: Any = None

    # A worker can die after the S3 receipt commit but before Airflow records
    # task success. The retry must consume the receipt instead of repeating
    # source traffic for the same immutable work item.
    if state.work_completed(
        queue_id,
        plan_id,
        str(item["work_id"]),
        plan=plan,
    ):
        record["status"] = "success"
        record["resumed_completed_work"] = True
        return runner._finish(report, output)

    try:
        repository = runner._new_repository()
        if os.environ.get("WHOSCORED_SCHEMA_READY") != "1":
            repository.ensure_schema()
            os.environ["WHOSCORED_SCHEMA_READY"] = "1"
        catalog = repository.load_discovered_catalog(
            batch_id=str(plan["provenance"]["catalog_batch_id"])
        )
        selected = runner._select_catalog_snapshot_scopes(
            catalog,
            [scope],
            active_only=False,
        )
        runtime_scope = selected[0][1]
        service_cls = runner._load_runtime()
        record["status"] = "running"
        with runner._transport_scope_environment(scope, str(item["kind"])):
            with service_cls(runtime_scope, repository=repository) as service:
                if item["kind"] == "schedule":
                    result = service.sync_schedule()
                    schedule_result = result
                    runner._merge_result(report, result, scope_record=record)
                    runner._record_result_state(
                        report, record, scope, "schedule", result
                    )
                    if not record["errors"]:
                        candidate_game_ids = _filtered_candidates(
                            service,
                            plan,
                            scope,
                        )
                        outcome["candidate_game_ids"] = candidate_game_ids
                        outcome["preview_game_ids"] = _frozen_preview_ids(
                            service,
                            scope,
                            candidate_game_ids,
                        )
                elif item["kind"] == "matches":
                    args = _operation_args(game_ids=list(item["game_ids"]))
                    result = runner._invoke(service, "matches", args)
                    runner._merge_result(report, result, scope_record=record)
                    runner._record_result_state(
                        report, record, scope, "matches", result
                    )
                    if int(getattr(result, "attempted", -1)) != len(item["game_ids"]):
                        raise WhoScoredOpsStoreError(
                            "frozen match work did not attempt every game_id"
                        )
                    preview_ids = list(item["preview_game_ids"])
                    if not record["errors"] and preview_ids:
                        preview_args = _operation_args(game_ids=preview_ids)
                        preview = runner._invoke(service, "previews", preview_args)
                        runner._merge_result(report, preview, scope_record=record)
                        runner._record_result_state(
                            report, record, scope, "previews", preview
                        )
                        if int(getattr(preview, "attempted", -1)) != len(
                            preview_ids
                        ) or int(getattr(preview, "succeeded", -1)) != len(preview_ids):
                            raise WhoScoredOpsStoreError(
                                "frozen preview work did not prove every game_id"
                            )
                    outcome["game_ids"] = list(item["game_ids"])
                elif item["kind"] == "roster":
                    # This work item is deliberately sequenced after every
                    # frozen match/preview chunk. It captures players first
                    # revealed by lineups, substitutions, and injury data.
                    outcome["profile_player_ids"] = _frozen_profile_ids(service)
                elif item["kind"] == "profiles":
                    player_ids = list(item["player_ids"])
                    args = _operation_args()
                    args.profiles_limit = len(player_ids)
                    result = runner._invoke(
                        service,
                        "profiles",
                        args,
                        profile_scopes=[runtime_scope],
                        profile_player_ids=player_ids,
                    )
                    runner._merge_result(report, result, scope_record=record)
                    runner._record_result_state(
                        report, record, scope, "profiles", result
                    )
                    attempted = int(getattr(result, "attempted", 0))
                    succeeded = int(
                        getattr(
                            result,
                            "succeeded",
                            sum(int(value) for value in result.counts.values()),
                        )
                    )
                    outcome.update(
                        {
                            "player_ids": player_ids,
                            "attempted": attempted,
                            "succeeded": succeeded,
                        }
                    )
                    if attempted != len(player_ids) or succeeded != len(player_ids):
                        raise WhoScoredOpsStoreError(
                            "frozen profile work did not successfully ingest every player_id"
                        )
                else:  # pragma: no cover - validate_work_item rejects this
                    raise AssertionError(f"unknown work kind {item['kind']!r}")
                runner._collect_traffic(report, scope, service)
                if item["kind"] == "schedule" and not record["errors"]:
                    outcome.update(
                        _schedule_request_accounting(
                            item,
                            schedule_result,
                            report["traffic_by_scope"].get(scope.spec, {}),
                        )
                    )
        runner._set_scope_status(record)
        if not report["error_details"]:
            receipt = state.append_receipt(
                queue_id=queue_id,
                plan_id=plan_id,
                work_item=item,
                outcome=outcome,
                airflow=_airflow_identity(),
            )
            report["backfill_receipt"] = receipt["artifact"]
    except Exception as exc:
        if record["status"] in {"pending", "running"}:
            runner._record_error(
                report,
                record,
                scope,
                str(item.get("kind") or "backfill-work"),
                exc,
            )
            runner._set_scope_status(record)
        elif not report["error_details"]:
            _record_unscoped_error(report, exc)
    return runner._finish(report, output)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    # Backfill work is permanently direct-only even when this internal worker
    # is invoked outside Airflow. The flag remains accepted for CLI/log
    # compatibility, but cannot enable paid transport by omission.
    os.environ["WHOSCORED_PAID_PROXY_URL"] = ""
    try:
        from scrapers.whoscored.runtime_contract import validate_runtime_contract

        validate_runtime_contract(report_schema_version=runner.REPORT_SCHEMA_VERSION)
        state = WhoScoredBackfillState.from_env()
        item = _decode_work_item(args.work_item)
    except Exception as exc:
        report = runner._new_report("backfill", ())
        _record_unscoped_error(report, exc)
        return runner._finish(report, args.output)
    return _run_work_item(
        state=state,
        queue_id=args.queue_id,
        plan_id=args.plan_id,
        item=item,
        output=args.output,
    )


if __name__ == "__main__":
    sys.exit(main())
