#!/usr/bin/env python3
"""Production orchestration for manifest-backed WhoScored ingestion.

The public commands are deliberately workflow-shaped:

``discover``
    Refresh and atomically publish the complete source-owned catalog.
``daily``
    Read active scopes from that persisted catalog and incrementally ingest
    them.  There is no fallback to the historical six-league allow-list.
``backfill``
    Freeze an explicit candidate queue, checkpoint after every 25-match chunk,
    and resume the exact same queue after a task/process failure.
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
DEFAULT_STATE_DIR = "/opt/airflow/logs/whoscored_state"
_QUEUE_SCHEMA_VERSION = 1
_SAFE_QUEUE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
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
        "--state-dir",
        default=os.environ.get("WHOSCORED_STATE_DIR", DEFAULT_STATE_DIR),
    )
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
        action="store_true",
        help="Disable paid proxy fallback for a canary/replay run",
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Discover every historical stage (discover/backfill only)",
    )
    parser.add_argument("--profiles-limit", type=int, default=500)
    parser.add_argument("--max-matches", type=int, default=None)
    # Internal operation namespaces use ``limit`` for the profile service;
    # the public workflow knob is the unambiguous ``--profiles-limit``.
    parser.set_defaults(limit=None)
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
    if args.profiles_limit < 0:
        parser.error("--profiles-limit must be non-negative")
    if args.max_matches is not None and args.max_matches < 0:
        parser.error("--max-matches must be non-negative")
    if args.queue_id and not _SAFE_QUEUE_ID.fullmatch(args.queue_id):
        parser.error(
            "--queue-id must contain only letters, digits, dot, underscore, "
            "or dash (maximum 120 characters)"
        )
    if args.command == "backfill" and not scopes and not args.all_catalog:
        parser.error("backfill requires --scope or --all-catalog")
    if args.command != "backfill" and args.all_catalog:
        parser.error("--all-catalog is valid only for backfill")
    if scopes and args.all_catalog:
        parser.error("--scope and --all-catalog are mutually exclusive")
    if args.command == "replay" and (not scopes or not args.game_id):
        parser.error("replay requires both --scope and --game-id")
    if args.command != "daily" and (args.skip_profiles or args.profiles_only):
        parser.error("--skip-profiles/--profiles-only are valid only for daily")
    if args.skip_profiles and args.profiles_only:
        parser.error("--skip-profiles and --profiles-only are mutually exclusive")
    if args.full_history and args.command not in {"discover", "backfill"}:
        parser.error("--full-history is valid only for discover or backfill")
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
        return service.sync_matches(
            match_ids=getattr(args, "_match_ids", None),
            limit=(
                args.max_matches
                if args.max_matches is not None
                else 100
                if daily_incremental
                else None
            ),
            force_replay=bool(getattr(args, "_force_replay", False)),
            kickoff_from=(
                datetime_lib.datetime.now(datetime_lib.timezone.utc)
                - datetime_lib.timedelta(days=7)
                if daily_incremental
                else None
            ),
        )
    if operation == "profiles":
        return service.sync_profiles(
            limit=int(args.profiles_limit),
            candidate_scopes=profile_scopes,
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
                profile_args.limit = args.profiles_limit
                result = _invoke(
                    service,
                    "profiles",
                    profile_args,
                    profile_scopes=[item[1] for item in selected],
                )
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


def _queue_file(state_dir: str, queue_id: str) -> Path:
    if not _SAFE_QUEUE_ID.fullmatch(queue_id):
        raise ValueError(f"unsafe queue id {queue_id!r}")
    return Path(state_dir) / f"{queue_id}.json"


def _load_queue(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if value.get("schema_version") != _QUEUE_SCHEMA_VERSION:
        raise RuntimeError(f"unsupported backfill queue schema in {path}")
    return value


def _save_queue(path: Path, queue: MutableMapping[str, Any]) -> None:
    queue["updated_at"] = _utc_now_iso()
    _write_report(str(path), queue)


def _create_backfill_queue(
    *,
    path: Path,
    queue_id: str,
    selector_hash: str,
    selected: Sequence[tuple[RunnerScope, Any]],
    service_cls: Any,
    repository: Any,
    report: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    queue_scopes: list[dict[str, Any]] = []
    for scope, runtime_scope in selected:
        record = _scope_record(report, scope)
        record["status"] = "running"
        try:
            with _transport_scope_environment(scope, "schedule+queue"):
                service_context = service_cls(runtime_scope, repository=repository)
                with service_context as service:
                    schedule_result = service.sync_schedule()
                    _merge_result(report, schedule_result, scope_record=record)
                    _record_result_state(
                        report, record, scope, "schedule", schedule_result
                    )
                    if record["errors"]:
                        _collect_traffic(report, scope, service)
                        continue
                    # Daily ingestion backs off terminal/parser-stale records,
                    # but an explicit historical backfill must freeze them
                    # into its checkpoint queue. Otherwise a prior failure
                    # silently disappears and can never satisfy completeness.
                    candidates = service.repository.list_match_candidates(
                        scope.competition_id,
                        scope.season_id,
                        match_ids=args.game_id or None,
                        limit=None,
                        include_failed=True,
                    )
                    if args.date_from or args.date_to:
                        candidates = [
                            candidate
                            for candidate in candidates
                            if candidate.kickoff is not None
                            and (
                                args.date_from is None
                                or candidate.kickoff.date() >= args.date_from
                            )
                            and (
                                args.date_to is None
                                or candidate.kickoff.date() <= args.date_to
                            )
                        ]
                    queue_scopes.append(
                        {
                            "scope": scope.spec,
                            "pending_game_ids": [
                                int(candidate.game_id) for candidate in candidates
                            ],
                            "completed_game_ids": [],
                            "completed_profiles": 0,
                            "profiles_complete": False,
                            "blocked_until": None,
                        }
                    )
                    _collect_traffic(report, scope, service)
        except Exception as exc:
            _record_error(report, record, scope, "queue", exc)
        _set_scope_status(record)
    if report["error_details"]:
        raise RuntimeError("backfill queue was not frozen because planning failed")
    queue: dict[str, Any] = {
        "schema_version": _QUEUE_SCHEMA_VERSION,
        "queue_id": queue_id,
        "selector_hash": selector_hash,
        "status": "running",
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "chunk_size": int(args.chunk_size),
        "scopes": queue_scopes,
    }
    _save_queue(path, queue)
    return queue


def _process_backfill_queue(
    *,
    path: Path,
    queue: dict[str, Any],
    selected: Sequence[tuple[RunnerScope, Any]],
    service_cls: Any,
    repository: Any,
    report: dict[str, Any],
    args: argparse.Namespace,
) -> None:
    selected_by_spec = {scope.spec: (scope, value) for scope, value in selected}
    chunk_size = int(queue["chunk_size"])
    for queue_scope in queue["scopes"]:
        scope, runtime_scope = selected_by_spec[queue_scope["scope"]]
        record = _scope_record(report, scope)
        record["status"] = "running"
        blocked_until = queue_scope.get("blocked_until")
        if blocked_until:
            due = datetime_lib.datetime.fromisoformat(blocked_until)
            if due > datetime_lib.datetime.now(datetime_lib.timezone.utc):
                _record_error(
                    report,
                    record,
                    scope,
                    "backfill",
                    RetryableWork(f"queue blocked until {blocked_until}"),
                )
                _set_scope_status(record)
                return
            queue_scope["blocked_until"] = None
        while queue_scope["pending_game_ids"]:
            chunk = list(queue_scope["pending_game_ids"][:chunk_size])
            try:
                with _transport_scope_environment(scope, "matches"):
                    service_context = service_cls(
                        runtime_scope,
                        repository=repository,
                    )
                    with service_context as service:
                        chunk_args = argparse.Namespace(**vars(args))
                        chunk_args._match_ids = chunk
                        # A pending queue entry remains authoritative even if
                        # the prior attempt managed to append a failure (or a
                        # success before crashing ahead of the checkpoint).
                        # Explicit replay makes the frozen chunk converge via
                        # raw cache instead of allowing manifest state to drop
                        # it silently on resume.
                        chunk_args._force_replay = True
                        chunk_args.max_matches = None
                        result = _invoke(service, "matches", chunk_args)
                        _merge_result(report, result, scope_record=record)
                        _record_result_state(report, record, scope, "matches", result)
                        if not record["errors"]:
                            chunk_args._force_replay = True
                            preview_result = _invoke(service, "previews", chunk_args)
                            _merge_result(
                                report,
                                preview_result,
                                scope_record=record,
                            )
                            _record_result_state(
                                report,
                                record,
                                scope,
                                "previews",
                                preview_result,
                            )
                        _collect_traffic(report, scope, service)
            except Exception as exc:
                _record_error(report, record, scope, "matches", exc)
            if record["errors"]:
                if all(error["retryable"] for error in record["errors"]):
                    queue_scope["blocked_until"] = (
                        datetime_lib.datetime.now(datetime_lib.timezone.utc)
                        + datetime_lib.timedelta(hours=6)
                    ).isoformat()
                _save_queue(path, queue)
                _set_scope_status(record)
                return
            queue_scope["completed_game_ids"].extend(chunk)
            del queue_scope["pending_game_ids"][: len(chunk)]
            _save_queue(path, queue)
        while not queue_scope.get("profiles_complete", False):
            try:
                with _transport_scope_environment(scope, "profiles"):
                    service_context = service_cls(
                        runtime_scope,
                        repository=repository,
                    )
                    with service_context as service:
                        profile_args = argparse.Namespace(**vars(args))
                        profile_args.limit = 200
                        profile_result = _invoke(
                            service,
                            "profiles",
                            profile_args,
                            profile_scopes=[runtime_scope],
                        )
                        _merge_result(report, profile_result, scope_record=record)
                        _record_result_state(
                            report,
                            record,
                            scope,
                            "profiles",
                            profile_result,
                        )
                        _collect_traffic(report, scope, service)
            except Exception as exc:
                _record_error(report, record, scope, "profiles", exc)
            if record["errors"]:
                _save_queue(path, queue)
                _set_scope_status(record)
                return
            queue_scope["completed_profiles"] = int(
                queue_scope.get("completed_profiles", 0)
            ) + int(
                getattr(
                    profile_result,
                    "succeeded",
                    sum(int(value) for value in profile_result.counts.values()),
                )
            )
            if int(getattr(profile_result, "attempted", 0)) == 0:
                queue_scope["profiles_complete"] = True
            _save_queue(path, queue)
        _set_scope_status(record)
    queue["status"] = "complete"
    _save_queue(path, queue)


def _queue_progress(queue: Mapping[str, Any], path: Path) -> dict[str, Any]:
    pending = sum(len(item["pending_game_ids"]) for item in queue["scopes"])
    completed = sum(len(item["completed_game_ids"]) for item in queue["scopes"])
    completed_profiles = sum(
        int(item.get("completed_profiles", 0)) for item in queue["scopes"]
    )
    pending_profile_scopes = sum(
        not bool(item.get("profiles_complete", False)) for item in queue["scopes"]
    )
    return {
        "queue_id": queue["queue_id"],
        "path": str(path),
        "status": queue["status"],
        "chunk_size": int(queue["chunk_size"]),
        "pending_matches": pending,
        "completed_matches": completed,
        "completed_profiles": completed_profiles,
        "pending_profile_scopes": pending_profile_scopes,
    }


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

    try:
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
            if args.command == "backfill" and args.full_history:
                discovery_report = _new_report("discover", ())
                backfill_discovery_result = _run_discover(
                    service_cls,
                    repository,
                    discovery_report,
                    full_history=True,
                )
                if getattr(backfill_discovery_result, "errors", None):
                    raise RuntimeError("; ".join(backfill_discovery_result.errors))
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
        if backfill_discovery_result is not None:
            _merge_unscoped_result(report, backfill_discovery_result)
        logger.info(
            "Starting WhoScored workflow: command=%s scopes=%s",
            args.command,
            [scope.spec for scope, _ in selected],
        )

        if args.command == "daily":
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

        selector_hash, default_queue_id = _selector_identity(
            [item[0] for item in selected],
            args.game_id,
            args.all_catalog,
            args.date_from,
            args.date_to,
        )
        queue_id = args.queue_id or default_queue_id
        path = _queue_file(args.state_dir, queue_id)
        try:
            if path.exists():
                queue = _load_queue(path)
                if queue.get("selector_hash") != selector_hash:
                    raise RuntimeError(
                        f"queue {queue_id!r} belongs to different selectors"
                    )
            else:
                queue = _create_backfill_queue(
                    path=path,
                    queue_id=queue_id,
                    selector_hash=selector_hash,
                    selected=selected,
                    service_cls=service_cls,
                    repository=repository,
                    report=report,
                    args=args,
                )
            if queue["status"] != "complete":
                _process_backfill_queue(
                    path=path,
                    queue=queue,
                    selected=selected,
                    service_cls=service_cls,
                    repository=repository,
                    report=report,
                    args=args,
                )
            else:
                for scope, _ in selected:
                    record = _scope_record(report, scope)
                    record["status"] = "success"
                    record["resumed_completed_queue"] = True
            report["queue"] = _queue_progress(queue, path)
        except Exception as exc:
            if not report["error_details"]:
                owner = selected[0][0]
                record = _scope_record(report, owner)
                _record_error(report, record, owner, "backfill", exc)
                _set_scope_status(record)
            if path.exists():
                try:
                    queue = _load_queue(path)
                    report["queue"] = _queue_progress(queue, path)
                except Exception:
                    pass
        return _finish(report, args.output)

    raise AssertionError(f"unhandled WhoScored workflow command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
