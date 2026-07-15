#!/usr/bin/env python3
"""Production orchestration for manifest-backed WhoScored ingestion.

The public commands are deliberately workflow-shaped:

``discover``
    Refresh and atomically publish the complete source-owned catalog.
``daily``
    Read active scopes from that persisted catalog and incrementally ingest
    them.  There is no fallback to the historical six-league allow-list.
``backfill``
    Freeze an immutable S3 plan, append one receipt per 25-match/200-profile
    work item, and resume from those receipts after a task/process failure.
``replay``
    Re-parse explicitly selected match raw objects through the normal
    raw-cache-first service path.

Only these workflow commands are public.  Entity-level compatibility commands
were removed after the resumable backfill/replay paths superseded them.
"""

from __future__ import annotations

import argparse
import datetime as datetime_lib
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


REPORT_SCHEMA_VERSION = 3
PUBLIC_COMMANDS = ("discover", "daily", "backfill", "replay")
COMMANDS = PUBLIC_COMMANDS
DEFAULT_BACKFILL_CHUNK_SIZE = 25
_SAFE_QUEUE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
_PLAN_ID = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_SEASON_RE = re.compile(
    r"^[0-9]{4}(?:-(?:single|split|multi)-ws[1-9][0-9]*)?$"
)
TABLE_NAME_BY_ENTITY = {
    "competitions": "whoscored_competitions",
    "seasons": "whoscored_seasons",
    "stages": "whoscored_stages",
    "schedule": "whoscored_schedule",
    "missing_players": "whoscored_missing_players",
    "events": "whoscored_events",
    "lineups": "whoscored_lineups",
    "player_profile": "whoscored_player_profile_versions",
}


class RetryableWork(RuntimeError):
    """The service committed progress but retained retryable entity ids."""

    retryable = True


@dataclass(frozen=True, order=True)
class RunnerScope:
    """Syntax-checked scope used before the runtime catalog is imported."""

    competition_id: str
    season_id: str

    @property
    def spec(self) -> str:
        return f"{self.competition_id}={self.season_id}"

    @classmethod
    def parse(cls, value: str) -> "RunnerScope":
        competition_id, separator, season_id = value.rpartition("=")
        competition_id = competition_id.strip()
        season_id = season_id.strip()
        if not separator or not competition_id or not season_id:
            raise ValueError(
                f"Invalid scope {value!r}; expected COMPETITION=CANONICAL_SEASON"
            )
        if _CANONICAL_SEASON_RE.fullmatch(season_id) is None:
            raise ValueError(
                f"Invalid canonical season in {value!r}; expected four digits "
                "or a strict collision identity such as "
                "2021-single-ws8534"
            )
        return cls(competition_id=competition_id, season_id=season_id)


def _utc_now_iso() -> str:
    return datetime_lib.datetime.now(datetime_lib.timezone.utc).isoformat()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run WhoScored ingestion")
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument(
        "--scope",
        action="append",
        default=[],
        metavar="COMPETITION=SEASON",
        help=(
            "Explicit persisted catalog scope; repeat for multiple scopes. "
            "daily discovers active scopes when omitted"
        ),
    )
    parser.add_argument(
        "--scopes-json",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--game-id",
        action="append",
        default=[],
        type=int,
        help="Explicit match id for replay/backfill; repeat as needed",
    )
    parser.add_argument("--game-ids-json", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--date-from", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--date-to", default=None, metavar="YYYY-MM-DD")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_BACKFILL_CHUNK_SIZE,
        help="Checkpointed match chunk size (backfill default: 25)",
    )
    parser.add_argument("--queue-id", default=None)
    parser.add_argument(
        "--plan-id",
        default=None,
        help="Resume one exact durable backfill plan (requires --queue-id)",
    )
    parser.add_argument(
        "--state-dir",
        default=None,
        help=(
            "Removed local-checkpoint option retained only for an explicit "
            "migration error; configure WHOSCORED_RAW_STORE_URI instead"
        ),
    )
    parser.add_argument("--max-work-items", type=int, default=100)
    parser.add_argument(
        "--all-catalog",
        action="store_true",
        help="Backfill every eligible persisted catalog scope",
    )
    parser.add_argument(
        "--skip-profiles",
        action="store_true",
        help="Daily mapped-task mode; profiles run once in a separate task",
    )
    parser.add_argument(
        "--profiles-only",
        action="store_true",
        help="Daily global profile refresh using every active catalog scope",
    )
    parser.add_argument(
        "--direct-only",
        dest="direct_only",
        action="store_true",
        default=True,
        help="Disable paid proxy fallback (the default)",
    )
    parser.add_argument(
        "--allow-paid-proxy",
        dest="direct_only",
        action="store_false",
        help=(
            "Explicit manual opt-in for approved replay/discovery only; scheduled "
            "and backfill workflows remain direct-only"
        ),
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Discover every historical stage (discover/backfill only)",
    )
    parser.add_argument(
        "--catalog-batch-id",
        default=None,
        help="Pin a daily worker to one immutable discovered-catalog generation",
    )
    parser.add_argument("--profiles-limit", type=int, default=500)
    parser.add_argument(
        "--expected-profile-candidate-count",
        type=int,
        default=None,
        help="Exact due-profile count frozen by the daily Airflow planner",
    )
    parser.add_argument(
        "--expected-profile-candidate-sha256",
        default=None,
        help="SHA-256 of the exact due-profile player-id set",
    )
    parser.add_argument("--max-matches", type=int, default=None)
    parser.add_argument("--output", default="/tmp/whoscored_result.json")
    return parser


def _merge_json_selectors(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    for attribute, raw, converter in (
        ("scope", args.scopes_json, str),
        ("game_id", args.game_ids_json, int),
    ):
        if raw is None:
            continue
        try:
            values = json.loads(raw)
            if not isinstance(values, list):
                raise TypeError("expected a JSON array")
            converted = [converter(value) for value in values]
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            parser.error(f"--{attribute.replace('_', '-')}s-json is invalid: {exc}")
        getattr(args, attribute).extend(converted)


def _resolve_scopes(
    parser: argparse.ArgumentParser, values: Sequence[str]
) -> list[RunnerScope]:
    try:
        scopes = [RunnerScope.parse(raw) for raw in values]
    except ValueError as exc:
        parser.error(str(exc))
    if len({scope.spec for scope in scopes}) != len(scopes):
        parser.error("Duplicate --scope values are not allowed")
    return scopes


def _validate_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> list[RunnerScope]:
    scopes = _resolve_scopes(parser, args.scope)
    if args.chunk_size <= 0:
        parser.error("--chunk-size must be positive")
    if args.command == "backfill" and args.chunk_size != DEFAULT_BACKFILL_CHUNK_SIZE:
        parser.error("--chunk-size is fixed at 25 by the durable plan contract")
    if not 1 <= args.max_work_items <= 100:
        parser.error("--max-work-items must be in 1..100")
    if args.command == "backfill" and args.state_dir is not None:
        parser.error(
            "--state-dir local checkpoints were removed; configure the "
            "WhoScored S3 raw/ops store"
        )
    if args.command in {"daily", "backfill"} and not args.direct_only:
        parser.error(f"{args.command} is permanently direct-only")
    if args.profiles_limit < 0:
        parser.error("--profiles-limit must be non-negative")
    profile_contract_values = (
        args.expected_profile_candidate_count,
        args.expected_profile_candidate_sha256,
    )
    runs_profiles = args.command == "daily" and not args.skip_profiles
    if runs_profiles:
        from scrapers.whoscored.profile_policy import (
            daily_profile_candidate_hard_cap,
        )

        try:
            profile_hard_cap = daily_profile_candidate_hard_cap()
        except ValueError as exc:
            parser.error(str(exc))
        if any(value is None for value in profile_contract_values):
            parser.error(
                "daily profile work requires both exact profile candidate identity "
                "arguments"
            )
        if not 0 <= args.expected_profile_candidate_count <= profile_hard_cap:
            parser.error(
                "expected profile candidate count must be in 0..configured hard cap"
            )
        if args.profiles_limit != args.expected_profile_candidate_count:
            parser.error(
                "--profiles-limit must cover the exact expected profile backlog"
            )
        if re.fullmatch(
            r"[0-9a-f]{64}", str(args.expected_profile_candidate_sha256)
        ) is None:
            parser.error("expected profile candidate SHA-256 must be 64 lowercase hex")
    elif any(value is not None for value in profile_contract_values):
        parser.error(
            "profile candidate identity arguments are valid only for daily profile work"
        )
    if args.max_matches is not None and args.max_matches < 0:
        parser.error("--max-matches must be non-negative")
    if args.queue_id and not _SAFE_QUEUE_ID.fullmatch(args.queue_id):
        parser.error(
            "--queue-id must contain only letters, digits, dot, underscore, "
            "or dash (maximum 120 characters)"
        )
    if args.plan_id:
        if args.command != "backfill" or _PLAN_ID.fullmatch(args.plan_id) is None:
            parser.error("--plan-id must be a 64-hex backfill plan id")
        if not args.queue_id:
            parser.error("--plan-id requires --queue-id")
        if (
            scopes
            or args.game_id
            or args.all_catalog
            or args.full_history
            or args.date_from
            or args.date_to
        ):
            parser.error("--plan-id resume does not accept mutable backfill selectors")
    if (
        args.command == "backfill"
        and not args.plan_id
        and not scopes
        and not args.all_catalog
    ):
        parser.error("backfill requires --scope or --all-catalog")
    if args.command != "backfill" and args.all_catalog:
        parser.error("--all-catalog is valid only for backfill")
    if scopes and args.all_catalog:
        parser.error("--scope and --all-catalog are mutually exclusive")
    if args.command == "backfill" and args.game_id and len(scopes) != 1:
        parser.error("backfill --game-id requires exactly one --scope")
    if args.command == "replay" and (not scopes or not args.game_id):
        parser.error("replay requires both --scope and --game-id")
    if args.command != "daily" and (args.skip_profiles or args.profiles_only):
        parser.error("--skip-profiles/--profiles-only are valid only for daily")
    if args.skip_profiles and args.profiles_only:
        parser.error("--skip-profiles and --profiles-only are mutually exclusive")
    if args.full_history and args.command not in {"discover", "backfill"}:
        parser.error("--full-history is valid only for discover or backfill")
    if args.catalog_batch_id:
        if args.command != "daily" or not _SAFE_QUEUE_ID.fullmatch(
            str(args.catalog_batch_id)
        ):
            parser.error("--catalog-batch-id is a safe daily-only generation id")
    for attribute in ("date_from", "date_to"):
        value = getattr(args, attribute)
        if value is None:
            continue
        try:
            setattr(args, attribute, datetime_lib.date.fromisoformat(value))
        except ValueError:
            parser.error(f"--{attribute.replace('_', '-')} must be YYYY-MM-DD")
    if args.date_from and args.date_to and args.date_to < args.date_from:
        parser.error("--date-to must not precede --date-from")
    if args.command != "backfill" and (args.date_from or args.date_to):
        parser.error("date selectors are valid only for backfill")
    if args.command not in {"backfill", "replay"} and args.game_id:
        parser.error("match selectors are valid only for backfill or replay")
    return scopes


def _load_runtime() -> Any:
    """Import storage-heavy runtime classes only after CLI validation."""
    from scrapers.whoscored.service import WhoScoredIngestService

    return WhoScoredIngestService


def _new_repository() -> Any:
    """Construct the persisted-catalog repository lazily.

    Keeping this import out of DAG parsing is important: Trino and PyArrow are
    installed in the Airflow image, not in lightweight DAG inspection tools.
    """
    from scrapers.whoscored.repository import WhoScoredRepository

    return WhoScoredRepository()


def _scope_value(value: Any) -> RunnerScope:
    """Project a domain/catalog scope into the runner's stable wire type."""
    scope = getattr(value, "scope", value)
    competition_id = getattr(scope, "competition_id", None)
    season_id = getattr(scope, "season_id", None)
    if competition_id is None or season_id is None:
        raise TypeError(f"catalog returned an invalid scope object: {value!r}")
    return RunnerScope(str(competition_id), str(season_id))


def _persisted_scope_index(
    repository: Any, *, active_only: bool
) -> dict[str, tuple[RunnerScope, Any]]:
    """Read eligible scopes from Bronze and fail closed on bad catalog data."""
    values = repository.list_catalog_scopes(
        active_only=active_only,
        include_quarantined=False,
    )
    index: dict[str, tuple[RunnerScope, Any]] = {}
    for value in values:
        wire = _scope_value(value)
        if wire.spec in index:
            raise RuntimeError(
                f"persisted WhoScored catalog contains duplicate scope {wire.spec}"
            )
        index[wire.spec] = (wire, value)
    if not index:
        qualifier = "active " if active_only else ""
        raise RuntimeError(
            f"persisted WhoScored catalog has no eligible {qualifier}scopes; "
            "run discover and resolve quarantined rows"
        )
    return index


def resolve_daily_scope_specs() -> list[str]:
    """Public DAG helper: return deterministic active persisted scopes.

    This function is intentionally called by an Airflow *task*, never while a
    DAG file is imported.  A missing catalog therefore produces a visible task
    failure instead of an Airflow import error or a silent six-league fallback.
    """
    repository = _new_repository()
    return sorted(_persisted_scope_index(repository, active_only=True))


def _select_persisted_scopes(
    repository: Any,
    requested: Sequence[RunnerScope],
    *,
    active_only: bool,
) -> list[tuple[RunnerScope, Any]]:
    index = _persisted_scope_index(repository, active_only=active_only)
    if not requested:
        return [index[key] for key in sorted(index)]
    selected: list[tuple[RunnerScope, Any]] = []
    for scope in requested:
        try:
            selected.append(index[scope.spec])
        except KeyError as exc:
            qualifier = "active " if active_only else ""
            raise ValueError(
                f"scope {scope.spec!r} is not an eligible {qualifier}scope in "
                "the persisted WhoScored catalog"
            ) from exc
    return selected


def _select_catalog_snapshot_scopes(
    catalog: Any,
    requested: Sequence[RunnerScope],
    *,
    active_only: bool,
) -> list[tuple[RunnerScope, Any]]:
    """Select scopes from an already generation-pinned catalog object."""
    values = catalog.eligible_scopes(active_only=active_only)
    index: dict[str, tuple[RunnerScope, Any]] = {}
    for value in values:
        wire = _scope_value(value)
        if wire.spec in index:
            raise RuntimeError(f"catalog snapshot contains duplicate scope {wire.spec}")
        index[wire.spec] = (wire, value)
    if not index:
        raise RuntimeError("catalog snapshot has no eligible scopes")
    if not requested:
        return [index[key] for key in sorted(index)]
    selected: list[tuple[RunnerScope, Any]] = []
    for scope in requested:
        try:
            selected.append(index[scope.spec])
        except KeyError as exc:
            raise ValueError(
                f"scope {scope.spec!r} is not eligible in the pinned catalog snapshot"
            ) from exc
    return selected


@contextmanager
def _transport_scope_environment(scope: RunnerScope, entity: str):
    """Attach scope/entity identity before a transport reads Airflow context."""
    previous_scope = os.environ.get("WHOSCORED_SCOPE")
    previous_entity = os.environ.get("WHOSCORED_ENTITY")
    os.environ["WHOSCORED_SCOPE"] = scope.spec
    os.environ["WHOSCORED_ENTITY"] = entity
    try:
        yield
    finally:
        if previous_scope is None:
            os.environ.pop("WHOSCORED_SCOPE", None)
        else:
            os.environ["WHOSCORED_SCOPE"] = previous_scope
        if previous_entity is None:
            os.environ.pop("WHOSCORED_ENTITY", None)
        else:
            os.environ["WHOSCORED_ENTITY"] = previous_entity


def _new_report(command: str, scopes: Iterable[RunnerScope]) -> dict[str, Any]:
    started = _utc_now_iso()
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": uuid.uuid4().hex,
        "airflow": {
            "dag_id": os.environ.get("AIRFLOW_CTX_DAG_ID"),
            "dag_run_id": os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"),
            "task_id": os.environ.get("AIRFLOW_CTX_TASK_ID"),
            "try_number": os.environ.get("AIRFLOW_CTX_TRY_NUMBER"),
            "map_index": os.environ.get("AIRFLOW_CTX_MAP_INDEX"),
        },
        "status": "running",
        "command": command,
        "started_at": started,
        "finished_at": None,
        "scopes": [
            {
                "scope": scope.spec,
                "competition_id": scope.competition_id,
                "season_id": scope.season_id,
                "status": "pending",
                "entities": {},
                "errors": [],
            }
            for scope in scopes
        ],
        "entities": {},
        "rows": 0,
        "row_counts_complete": True,
        "errors": [],
        "error_details": [],
        "tables": [],
        "tables_by_entity": {},
        "traffic": {},
        "traffic_by_scope": {},
        "paid_proxy_bytes": 0,
        "queue": None,
        "catalog_batch_id": None,
        "profile_candidates": None,
    }


def _scope_record(report: Mapping[str, Any], scope: RunnerScope) -> dict[str, Any]:
    return next(item for item in report["scopes"] if item["scope"] == scope.spec)


def _is_retryable(exc: BaseException) -> bool:
    explicit = getattr(exc, "retryable", None)
    if explicit is not None:
        return bool(explicit)
    name = type(exc).__name__.lower()
    return any(
        marker in name
        for marker in ("timeout", "connection", "cloudflare", "temporary")
    )


def _record_error(
    report: dict[str, Any],
    scope_record: dict[str, Any],
    scope: RunnerScope,
    entity: str,
    exc: BaseException,
) -> None:
    retryable = _is_retryable(exc)
    message = f"{entity} [{scope.spec}]: {exc}"
    detail = {
        "scope": scope.spec,
        "entity": entity,
        "type": type(exc).__name__,
        "message": str(exc),
        "retryable": retryable,
    }
    report["errors"].append(message)
    report["error_details"].append(detail)
    scope_record["errors"].append(detail)
    logger.error(
        "WhoScored %s failed for %s: %s",
        entity,
        scope.spec,
        exc,
        exc_info=not isinstance(exc, RetryableWork),
    )


def _table_for_entity(entity: str, tables: Sequence[str]) -> Optional[str]:
    expected = TABLE_NAME_BY_ENTITY.get(entity)
    if expected is None:
        return None
    return next(
        (table for table in tables if table.rsplit(".", 1)[-1] == expected),
        None,
    )


def _merge_result(
    report: dict[str, Any],
    result: Any,
    *,
    scope_record: dict[str, Any],
) -> None:
    """Merge a typed service result into the stable report-v2 projection."""
    tables = list(dict.fromkeys(str(table) for table in result.tables))
    for table in tables:
        if table not in report["tables"]:
            report["tables"].append(table)

    for entity, raw_rows in result.counts.items():
        rows = int(raw_rows)
        table = _table_for_entity(str(entity), tables)
        current = report["entities"].setdefault(
            str(entity),
            {"table": table, "rows_written": 0, "counts_complete": True},
        )
        if table:
            current["table"] = table
            report["tables_by_entity"][str(entity)] = table
        current["rows_written"] += rows
        report["rows"] += rows
        scope_record["entities"][str(entity)] = {
            "table": table,
            "rows_written": rows,
        }


def _record_result_state(
    report: dict[str, Any],
    scope_record: dict[str, Any],
    scope: RunnerScope,
    operation: str,
    result: Any,
) -> None:
    if result.errors:
        _record_error(
            report,
            scope_record,
            scope,
            operation,
            RuntimeError("; ".join(str(item) for item in result.errors)),
        )
    elif result.retryable:
        _record_error(
            report,
            scope_record,
            scope,
            operation,
            RetryableWork(
                f"{result.entity} retryable ids: "
                + ", ".join(str(item) for item in result.retryable)
            ),
        )
    elif getattr(result, "terminal", None):
        _record_error(
            report,
            scope_record,
            scope,
            operation,
            RuntimeError(
                f"{result.entity} terminal ids: "
                + ", ".join(str(item) for item in result.terminal)
            ),
        )


def _merge_traffic(target: MutableMapping[str, Any], source: Mapping[str, Any]) -> None:
    """Add counters recursively while preserving non-additive diagnostics."""
    for key, value in source.items():
        if isinstance(value, Mapping):
            child = target.setdefault(key, {})
            if isinstance(child, MutableMapping):
                _merge_traffic(child, value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            target[key] = target.get(key, 0) + value
        elif isinstance(value, list):
            target.setdefault(key, []).extend(value)
        else:
            target[key] = value


def _paid_proxy_bytes(traffic: Mapping[str, Any]) -> int:
    """Read the canonical paid byte counter without double-counting children."""
    for key in ("paid_proxy_bytes", "paid_bytes"):
        value = traffic.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return max(0, int(value))
    routes = traffic.get("routes")
    if isinstance(routes, Mapping):
        total = 0
        for route, counters in routes.items():
            if not str(route).startswith("paid_") or not isinstance(counters, Mapping):
                continue
            value = counters.get("wire_bytes", counters.get("response_bytes", 0))
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                total += max(0, int(value))
        return total
    return 0


def _operations(command: str) -> tuple[str, ...]:
    if command == "daily":
        return ("schedule", "previews", "matches")
    raise AssertionError(f"workflow {command!r} has no implicit entity sequence")


def _invoke(
    service: Any,
    operation: str,
    args: argparse.Namespace,
    *,
    profile_scopes: Optional[Sequence[Any]] = None,
    profile_player_ids: Optional[Sequence[int]] = None,
) -> Any:
    if operation == "schedule":
        return service.sync_schedule()
    if operation == "previews":
        return service.sync_previews(
            match_ids=getattr(args, "_match_ids", None),
            force_replay=bool(getattr(args, "_force_replay", False)),
        )
    if operation == "matches":
        daily_incremental = args.command == "daily" and not getattr(
            args, "_match_ids", None
        )
        match_kwargs: dict[str, Any] = {
            "match_ids": getattr(args, "_match_ids", None),
            "limit": (
                args.max_matches
                if args.max_matches is not None
                else 100
                if daily_incremental
                else None
            ),
            "force_replay": bool(getattr(args, "_force_replay", False)),
            "kickoff_from": (
                datetime_lib.datetime.now(datetime_lib.timezone.utc)
                - datetime_lib.timedelta(days=7)
                if daily_incremental
                else None
            ),
        }
        if bool(getattr(args, "_historical_replay", False)):
            match_kwargs["historical_replay"] = True
        return service.sync_matches(**match_kwargs)
    if operation == "profiles":
        kwargs: dict[str, Any] = {
            "limit": int(args.profiles_limit),
            "candidate_scopes": profile_scopes,
        }
        if profile_player_ids is not None:
            kwargs["player_ids"] = list(profile_player_ids)
        return service.sync_profiles(
            **kwargs,
        )
    raise AssertionError(f"Unknown WhoScored operation: {operation}")


def _set_scope_status(scope_record: dict[str, Any]) -> None:
    errors = scope_record["errors"]
    if not errors:
        scope_record["status"] = "success"
    elif all(error["retryable"] for error in errors):
        scope_record["status"] = "retryable"
    else:
        scope_record["status"] = "failed"


def _collect_traffic(report: dict[str, Any], scope: RunnerScope, service: Any) -> None:
    try:
        traffic = service.traffic_stats() or {}
    except Exception as exc:
        logger.warning("traffic_stats failed for %s: %s", scope.spec, exc)
        traffic = {}
    report["traffic_by_scope"][scope.spec] = traffic
    if isinstance(traffic, Mapping):
        _merge_traffic(report["traffic"], traffic)


def _write_report(path: str, report: Mapping[str, Any]) -> None:
    """Atomically publish the JSON result so callbacks never read half a file."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{output.name}.", dir=str(output.parent))
    try:
        # mkstemp intentionally starts at 0600. Reports contain operational
        # metadata, not secrets, and must also be readable by the shared
        # root-group on the host/Airflow log volume.
        os.fchmod(fd, 0o640)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _finish(report: dict[str, Any], output: str) -> int:
    retryable_errors = [item for item in report["error_details"] if item["retryable"]]
    fatal_errors = [item for item in report["error_details"] if not item["retryable"]]
    if fatal_errors:
        report["status"] = "failed"
        exit_code = 1
    elif retryable_errors:
        report["status"] = "retryable"
        exit_code = 2
    else:
        report["status"] = "success"
        exit_code = 0
    report["finished_at"] = _utc_now_iso()
    report["paid_proxy_bytes"] = _paid_proxy_bytes(report.get("traffic", {}))
    _write_report(output, report)
    logger.info(
        "WhoScored run complete: status=%s scopes=%d rows=%d errors=%d",
        report["status"],
        len(report["scopes"]),
        report["rows"],
        len(report["errors"]),
    )
    print(json.dumps(report, ensure_ascii=False))
    return exit_code


def _merge_unscoped_result(report: dict[str, Any], result: Any) -> None:
    """Merge discovery output, which intentionally has no competition scope."""
    tables = list(dict.fromkeys(str(table) for table in result.tables))
    for table in tables:
        if table not in report["tables"]:
            report["tables"].append(table)
    for entity, raw_rows in result.counts.items():
        rows = int(raw_rows)
        table = _table_for_entity(str(entity), tables)
        report["entities"][str(entity)] = {
            "table": table,
            "rows_written": rows,
            "counts_complete": True,
        }
        report["rows"] += rows
    for message in getattr(result, "errors", ()):
        report["errors"].append(f"discover: {message}")
        report["error_details"].append(
            {
                "scope": None,
                "entity": "discover",
                "type": "RuntimeError",
                "message": str(message),
                "retryable": False,
            }
        )
    retryable = list(getattr(result, "retryable", ()))
    if retryable:
        message = "discovery retryable ids: " + ", ".join(map(str, retryable))
        report["errors"].append(message)
        report["error_details"].append(
            {
                "scope": None,
                "entity": "discover",
                "type": "RetryableWork",
                "message": message,
                "retryable": True,
            }
        )
    terminal = list(getattr(result, "terminal", ()))
    if terminal:
        message = "discovery terminal ids: " + ", ".join(map(str, terminal))
        report["errors"].append(message)
        report["error_details"].append(
            {
                "scope": None,
                "entity": "discover",
                "type": "RuntimeError",
                "message": message,
                "retryable": False,
            }
        )
    traffic = getattr(result, "traffic", None)
    if isinstance(traffic, Mapping):
        report["traffic_by_scope"]["catalog"] = dict(traffic)
        _merge_traffic(report["traffic"], traffic)


def _run_discover(
    service_cls: Any,
    repository: Any,
    report: dict[str, Any],
    *,
    full_history: bool = False,
) -> Any:
    result = service_cls.discover_catalog(
        repository=repository,
        full_history=bool(full_history),
    )
    _merge_unscoped_result(report, result)
    return result


def _run_service_operations(
    *,
    service_cls: Any,
    selected: Sequence[tuple[RunnerScope, Any]],
    repository: Any,
    report: dict[str, Any],
    args: argparse.Namespace,
    operations: Sequence[str],
) -> None:
    for scope, runtime_scope in selected:
        scope_record = _scope_record(report, scope)
        scope_record["status"] = "running"
        try:
            with _transport_scope_environment(scope, "+".join(operations)):
                service_context = service_cls(runtime_scope, repository=repository)
                with service_context as service:
                    for operation in operations:
                        try:
                            result = _invoke(service, operation, args)
                            _merge_result(report, result, scope_record=scope_record)
                            _record_result_state(
                                report, scope_record, scope, operation, result
                            )
                            if scope_record["errors"]:
                                # Schedule is a hard prerequisite for preview
                                # and match candidate selection.  Continuing
                                # after any entity failure can scrape stale
                                # candidates and spend paid budget needlessly.
                                break
                        except Exception as exc:
                            _record_error(report, scope_record, scope, operation, exc)
                            break
                    _collect_traffic(report, scope, service)
        except Exception as exc:
            _record_error(report, scope_record, scope, "service", exc)
        _set_scope_status(scope_record)


def _run_global_profiles(
    *,
    service_cls: Any,
    selected: Sequence[tuple[RunnerScope, Any]],
    repository: Any,
    report: dict[str, Any],
    args: argparse.Namespace,
    player_ids: Sequence[int],
) -> None:
    if not selected:
        return
    owner, owner_runtime_scope = selected[0]
    owner_record = _scope_record(report, owner)
    if owner_record["status"] == "pending":
        owner_record["status"] = "running"
    try:
        with _transport_scope_environment(owner, "profiles"):
            service_context = service_cls(
                owner_runtime_scope,
                repository=repository,
            )
            with service_context as service:
                profile_args = argparse.Namespace(**vars(args))
                result = _invoke(
                    service,
                    "profiles",
                    profile_args,
                    profile_scopes=[item[1] for item in selected],
                    profile_player_ids=player_ids,
                )
                profile_snapshot = report.get("profile_candidates")
                if isinstance(profile_snapshot, MutableMapping):
                    profile_snapshot["attempted"] = int(result.attempted)
                _merge_result(report, result, scope_record=owner_record)
                _record_result_state(
                    report, owner_record, owner, "player_profile", result
                )
                _collect_traffic(report, owner, service)
    except Exception as exc:
        _record_error(report, owner_record, owner, "profiles", exc)
    _set_scope_status(owner_record)
    for scope, _ in selected[1:]:
        record = _scope_record(report, scope)
        if record["status"] == "pending":
            record["status"] = owner_record["status"]
            record["delegated_to"] = owner.spec


def _freeze_daily_profile_candidates(
    *,
    repository: Any,
    selected: Sequence[tuple[RunnerScope, Any]],
    expected_count: int,
    expected_sha256: str,
) -> tuple[int, ...]:
    """Verify the Airflow plan before constructing any source transport."""

    from scrapers.whoscored.profile_policy import daily_profile_candidate_hard_cap
    from scrapers.whoscored.repository import profile_candidate_payload_sha256

    candidate_scopes = tuple(
        getattr(runtime_scope, "scope", runtime_scope)
        for _wire_scope, runtime_scope in selected
    )
    snapshot = repository.profile_candidate_snapshot(
        scopes=candidate_scopes,
        hard_cap=daily_profile_candidate_hard_cap(),
    )
    player_ids = tuple(snapshot.player_ids)
    actual_sha256 = profile_candidate_payload_sha256(player_ids)
    if len(player_ids) != snapshot.count or actual_sha256 != snapshot.payload_sha256:
        raise RuntimeError("WhoScored repository returned an invalid profile snapshot")
    if snapshot.count != expected_count or snapshot.payload_sha256 != expected_sha256:
        raise RuntimeError(
            "WhoScored profile candidate snapshot changed before source work: "
            f"expected_count={expected_count}, actual_count={snapshot.count}, "
            f"expected_sha256={expected_sha256}, "
            f"actual_sha256={snapshot.payload_sha256}"
        )
    return player_ids


def _selector_identity(
    scopes: Sequence[RunnerScope],
    game_ids: Sequence[int],
    all_catalog: bool = False,
    date_from: Optional[datetime_lib.date] = None,
    date_to: Optional[datetime_lib.date] = None,
) -> tuple[str, str]:
    selector = {
        "scopes": sorted(scope.spec for scope in scopes),
        "game_ids": sorted({int(value) for value in game_ids}),
        "all_catalog": bool(all_catalog),
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
    }
    encoded = json.dumps(selector, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(encoded).hexdigest()
    return digest, f"bf-{digest[:20]}"


def _validate_cli_backfill_deadline(plan: Mapping[str, Any]) -> None:
    provenance = plan.get("provenance")
    if not isinstance(provenance, Mapping):
        raise ValueError("backfill plan has no immutable timing provenance")
    try:
        started = datetime_lib.datetime.fromisoformat(
            str(provenance["backfill_started_at"]).replace("Z", "+00:00")
        )
        deadline = datetime_lib.datetime.fromisoformat(
            str(provenance["backfill_deadline_at"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as exc:
        raise ValueError("backfill plan has invalid timing provenance") from exc
    if started.tzinfo is None:
        started = started.replace(tzinfo=datetime_lib.timezone.utc)
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=datetime_lib.timezone.utc)
    started = started.astimezone(datetime_lib.timezone.utc)
    deadline = deadline.astimezone(datetime_lib.timezone.utc)
    now = datetime_lib.datetime.now(datetime_lib.timezone.utc)
    if (
        not datetime_lib.timedelta(0)
        < deadline - started
        <= datetime_lib.timedelta(days=30)
    ):
        raise ValueError("backfill plan deadline must be within 30 days")
    if now > deadline:
        raise ValueError("backfill plan deadline has expired")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _merge_json_selectors(parser, args)
    scopes = _validate_args(parser, args)
    if args.direct_only:
        # The service reads this only while constructing its transport.  An
        # empty endpoint makes paid fallback structurally unavailable without
        # exposing or rewriting the deployment secret.
        os.environ["WHOSCORED_PAID_PROXY_URL"] = ""
    report = _new_report(args.command, scopes)
    report["direct_only"] = bool(args.direct_only)
    report["catalog_batch_id"] = args.catalog_batch_id

    try:
        from scrapers.whoscored.runtime_contract import validate_runtime_contract

        # Recheck inside every mapped Bash process. A scheduler preflight alone
        # cannot protect mutable bind mounts from deployment between tasks.
        validate_runtime_contract(report_schema_version=REPORT_SCHEMA_VERSION)
        service_cls = _load_runtime()
    except Exception as exc:
        for scope in scopes:
            record = _scope_record(report, scope)
            _record_error(report, record, scope, "runtime", exc)
            _set_scope_status(record)
        if not scopes:
            report["errors"].append(f"runtime: {exc}")
            report["error_details"].append(
                {
                    "scope": None,
                    "entity": "runtime",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "retryable": _is_retryable(exc),
                }
            )
        return _finish(report, args.output)

    resumed_backfill_plan: Optional[dict[str, Any]] = None
    if args.command == "backfill" and args.plan_id:
        try:
            from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

            resumed_backfill_plan = WhoScoredBackfillState.from_env().load_plan(
                str(args.queue_id),
                str(args.plan_id),
            )
            _validate_cli_backfill_deadline(resumed_backfill_plan)
            scopes = [
                RunnerScope.parse(str(value))
                for value in resumed_backfill_plan["scopes"]
            ]
        except Exception as exc:
            report["errors"].append(f"backfill-resume: {exc}")
            report["error_details"].append(
                {
                    "scope": None,
                    "entity": "backfill-resume",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "retryable": False,
                }
            )
            return _finish(report, args.output)

    if args.command == "discover":
        try:
            repository = _new_repository()
            repository.ensure_schema()
            _run_discover(
                service_cls,
                repository,
                report,
                full_history=args.full_history,
            )
        except Exception as exc:
            report["errors"].append(f"discover: {exc}")
            report["error_details"].append(
                {
                    "scope": None,
                    "entity": "discover",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "retryable": _is_retryable(exc),
                }
            )
        return _finish(report, args.output)

    if args.command in {"daily", "backfill", "replay"}:
        backfill_discovery_result = None
        try:
            repository = _new_repository()
            if os.environ.get("WHOSCORED_SCHEMA_READY") != "1":
                repository.ensure_schema()
                os.environ["WHOSCORED_SCHEMA_READY"] = "1"
            if args.command == "backfill" and (args.full_history or args.all_catalog):
                discovery_report = _new_report("discover", ())
                backfill_discovery_result = _run_discover(
                    service_cls,
                    repository,
                    discovery_report,
                    full_history=True,
                )
                if getattr(backfill_discovery_result, "errors", None):
                    raise RuntimeError("; ".join(backfill_discovery_result.errors))
            if args.command == "backfill":
                if resumed_backfill_plan is not None:
                    catalog_snapshot = repository.load_discovered_catalog(
                        batch_id=str(
                            resumed_backfill_plan["provenance"]["catalog_batch_id"]
                        )
                    )
                else:
                    _generation, catalog_snapshot = (
                        repository.load_catalog_generation_snapshot()
                    )
                selected = _select_catalog_snapshot_scopes(
                    catalog_snapshot,
                    scopes,
                    active_only=False,
                )
            elif args.command == "daily" and args.catalog_batch_id:
                catalog_snapshot = repository.load_discovered_catalog(
                    batch_id=str(args.catalog_batch_id)
                )
                selected = _select_catalog_snapshot_scopes(
                    catalog_snapshot,
                    scopes,
                    active_only=True,
                )
            else:
                selected = _select_persisted_scopes(
                    repository,
                    scopes,
                    active_only=(args.command == "daily"),
                )
        except Exception as exc:
            if scopes:
                for scope in scopes:
                    record = _scope_record(report, scope)
                    _record_error(report, record, scope, "scope", exc)
                    _set_scope_status(record)
            else:
                report["errors"].append(f"catalog: {exc}")
                report["error_details"].append(
                    {
                        "scope": None,
                        "entity": "catalog",
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "retryable": _is_retryable(exc),
                    }
                )
            return _finish(report, args.output)

        report = _new_report(args.command, [item[0] for item in selected])
        report["direct_only"] = bool(args.direct_only)
        report["catalog_batch_id"] = args.catalog_batch_id
        if backfill_discovery_result is not None:
            _merge_unscoped_result(report, backfill_discovery_result)
        logger.info(
            "Starting WhoScored workflow: command=%s scopes=%s",
            args.command,
            [scope.spec for scope, _ in selected],
        )

        if args.command == "daily":
            profile_player_ids: tuple[int, ...] = ()
            if not args.skip_profiles:
                try:
                    profile_player_ids = _freeze_daily_profile_candidates(
                        repository=repository,
                        selected=selected,
                        expected_count=int(args.expected_profile_candidate_count),
                        expected_sha256=str(
                            args.expected_profile_candidate_sha256
                        ),
                    )
                except Exception as exc:
                    owner = selected[0][0]
                    owner_record = _scope_record(report, owner)
                    _record_error(
                        report,
                        owner_record,
                        owner,
                        "profile-candidate-preflight",
                        exc,
                    )
                    _set_scope_status(owner_record)
                    for delegated, _runtime in selected[1:]:
                        record = _scope_record(report, delegated)
                        record["status"] = owner_record["status"]
                        record["delegated_to"] = owner.spec
                    return _finish(report, args.output)
                report["profile_candidates"] = {
                    "schema_version": 1,
                    "count": len(profile_player_ids),
                    "payload_sha256": str(
                        args.expected_profile_candidate_sha256
                    ),
                    "attempted": None,
                }
            if not args.profiles_only:
                _run_service_operations(
                    service_cls=service_cls,
                    selected=selected,
                    repository=repository,
                    report=report,
                    args=args,
                    operations=_operations("daily"),
                )
            if not args.skip_profiles and not report["error_details"]:
                _run_global_profiles(
                    service_cls=service_cls,
                    selected=selected,
                    repository=repository,
                    report=report,
                    args=args,
                    player_ids=profile_player_ids,
                )
            return _finish(report, args.output)

        if args.command == "replay":
            args._match_ids = sorted({int(value) for value in args.game_id})
            args._force_replay = True
            args.max_matches = None
            _run_service_operations(
                service_cls=service_cls,
                selected=selected,
                repository=repository,
                report=report,
                args=args,
                operations=("matches",),
            )
            return _finish(report, args.output)

        _selector_hash, default_queue_id = _selector_identity(
            [item[0] for item in selected],
            args.game_id,
            args.all_catalog,
            args.date_from,
            args.date_to,
        )
        queue_id = args.queue_id or default_queue_id
        if not _SAFE_QUEUE_ID.fullmatch(queue_id):
            owner = selected[0][0]
            record = _scope_record(report, owner)
            _record_error(
                report,
                record,
                owner,
                "backfill",
                ValueError(f"unsafe queue id {queue_id!r}"),
            )
            _set_scope_status(record)
            return _finish(report, args.output)
        try:
            from dags.scripts.run_whoscored_backfill_item import _run_work_item
            from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

            state = WhoScoredBackfillState.from_env()
            if resumed_backfill_plan is not None:
                plan = resumed_backfill_plan
            else:
                selector = {
                    "requested_scopes": sorted(scope.spec for scope in scopes),
                    "game_ids": sorted({int(value) for value in args.game_id}),
                    "all_catalog": bool(args.all_catalog),
                    "date_from": (
                        args.date_from.isoformat() if args.date_from else None
                    ),
                    "date_to": args.date_to.isoformat() if args.date_to else None,
                    "full_history_catalog": bool(args.full_history or args.all_catalog),
                }
                catalog_scopes = [
                    scope.spec
                    for scope, _runtime in _select_catalog_snapshot_scopes(
                        catalog_snapshot,
                        [],
                        active_only=False,
                    )
                ]
                started_at = datetime_lib.datetime.now(datetime_lib.timezone.utc)
                catalog_discovery_mode = str(
                    _generation.get("catalog_discovery_mode") or ""
                )
                requires_full_history = bool(args.full_history or args.all_catalog)
                if requires_full_history and catalog_discovery_mode != "full_history":
                    raise ValueError(
                        "full-history catalog generation has no exact manifest proof"
                    )
                provenance = {
                    **_generation,
                    "full_history_discovery": (
                        catalog_discovery_mode == "full_history"
                    ),
                    "catalog_eligible_scope_count": len(catalog_scopes),
                    "catalog_eligible_scopes_sha256": hashlib.sha256(
                        ("\n".join(sorted(catalog_scopes)) + "\n").encode("utf-8")
                    ).hexdigest(),
                    "backfill_started_at": started_at.isoformat(),
                    "backfill_deadline_at": (
                        started_at + datetime_lib.timedelta(days=30)
                    ).isoformat(),
                }
                plan = state.create_plan(
                    queue_id=queue_id,
                    selector=selector,
                    scopes=[scope.spec for scope, _runtime in selected],
                    provenance=provenance,
                    schedule_stage_ids={
                        scope.spec: sorted(
                            {
                                int(stage_id)
                                for stage_id in getattr(runtime, "stage_ids", ())
                            }
                        )
                        for scope, runtime in selected
                    },
                )
            _validate_cli_backfill_deadline(plan)
            plan_id = str(plan["plan_id"])
            work_count = 0
            output_dir = Path(args.output).parent
            while work_count < int(args.max_work_items):
                batch_id = f"cli-{report['run_id']}-{work_count:06d}"
                batch = state.create_batch(
                    queue_id,
                    plan_id,
                    batch_id=batch_id,
                    limit=min(100, int(args.max_work_items) - work_count),
                    request_unit_limit=10000,
                )
                pending = batch["work_items"]
                if not pending:
                    break
                failed = False
                for item in pending:
                    child_output = output_dir / f"backfill_{item['work_id']}.json"
                    rc = _run_work_item(
                        state=state,
                        queue_id=queue_id,
                        plan_id=plan_id,
                        item=item,
                        output=str(child_output),
                    )
                    work_count += 1
                    try:
                        with child_output.open("r", encoding="utf-8") as handle:
                            child = json.load(handle)
                    except (OSError, ValueError):
                        child = {}
                    report["rows"] += int(child.get("rows") or 0)
                    for entity, metadata in (child.get("entities") or {}).items():
                        if not isinstance(metadata, Mapping):
                            continue
                        target = report["entities"].setdefault(entity, {})
                        target["rows_written"] = int(
                            target.get("rows_written") or 0
                        ) + int(metadata.get("rows_written") or 0)
                        for key in ("table", "counts_complete"):
                            if key in metadata:
                                target[key] = metadata[key]
                    report["tables"] = sorted(
                        {*report["tables"], *(child.get("tables") or [])}
                    )
                    report["tables_by_entity"].update(
                        child.get("tables_by_entity") or {}
                    )
                    child_traffic = child.get("traffic")
                    if isinstance(child_traffic, Mapping):
                        _merge_traffic(report["traffic"], child_traffic)
                    if rc:
                        owner = RunnerScope.parse(str(item["scope"]))
                        record = _scope_record(report, owner)
                        error: BaseException = RuntimeError(
                            f"work item {item['work_id']} failed with exit code {rc}"
                        )
                        if rc == 2:
                            error = RetryableWork(str(error))
                        _record_error(report, record, owner, str(item["kind"]), error)
                        _set_scope_status(record)
                        failed = True
                        break
                if failed:
                    break
                batch_progress = state.advance_batch(
                    queue_id,
                    plan_id,
                    batch_id=batch_id,
                )
                if batch_progress["status"] == "complete":
                    break
            report["queue"] = {
                **state.checkpoint_progress(queue_id, plan_id),
                "processed_work_items": work_count,
            }
            for scope, _runtime in selected:
                record = _scope_record(report, scope)
                if record["status"] == "pending":
                    record["status"] = "success"
            if report["queue"]["status"] != "complete" and not report["error_details"]:
                owner = selected[0][0]
                record = _scope_record(report, owner)
                _record_error(
                    report,
                    record,
                    owner,
                    "backfill",
                    RetryableWork(
                        "bounded backfill batch is incomplete; resume with "
                        f"--queue-id {queue_id} --plan-id {plan_id}"
                    ),
                )
                _set_scope_status(record)
        except Exception as exc:
            if not report["error_details"]:
                owner = selected[0][0]
                record = _scope_record(report, owner)
                _record_error(report, record, owner, "backfill", exc)
                _set_scope_status(record)
        return _finish(report, args.output)

    raise AssertionError(f"unhandled WhoScored workflow command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
