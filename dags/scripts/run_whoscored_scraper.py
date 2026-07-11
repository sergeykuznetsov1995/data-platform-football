#!/usr/bin/env python3
"""Run the manifest-backed WhoScored ingestion service.

Every invocation uses explicit canonical competition-season scopes.  This
prevents the old league/season Cartesian product and keeps discovery, raw
storage, retries, and proxy budgets inside :class:`WhoScoredIngestService`.

Commands:

``schedule``
    Refresh schedule and season-stage metadata.
``previews``
    Refresh targeted missing-player previews.
``matches``
    Fetch eligible match events and lineups.
``profiles``
    Fetch one globally capped union of the selected active rosters.
``all``
    Run schedule, previews, and matches. Profiles stay separately budgeted.
"""

from __future__ import annotations

import argparse
import datetime as datetime_lib
import json
import logging
import os
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


REPORT_SCHEMA_VERSION = 2
COMMANDS = ("schedule", "previews", "matches", "profiles", "all")
TABLE_NAME_BY_ENTITY = {
    "schedule": "whoscored_schedule",
    "season_stages": "whoscored_season_stages",
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
        if len(season_id) != 4 or not season_id.isdigit():
            raise ValueError(
                f"Invalid canonical season in {value!r}; expected four digits "
                "such as 2526 or 2026"
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
        required=True,
        metavar="COMPETITION=SEASON",
        help=(
            "Canonical scope; repeat for multiple independent scopes, e.g. "
            "--scope 'ENG-Premier League=2526'"
        ),
    )
    parser.add_argument("--max-matches", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output", default="/tmp/whoscored_result.json")
    return parser


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


def _load_runtime() -> tuple[Any, Any]:
    """Import storage-heavy runtime classes only after CLI validation."""
    from scrapers.whoscored.catalog import WhoScoredCatalog
    from scrapers.whoscored.service import WhoScoredIngestService

    return WhoScoredCatalog, WhoScoredIngestService


def _new_report(command: str, scopes: Iterable[RunnerScope]) -> dict[str, Any]:
    started = _utc_now_iso()
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": uuid.uuid4().hex,
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


def _merge_traffic(
    target: MutableMapping[str, Any], source: Mapping[str, Any]
) -> None:
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


def _operations(command: str) -> tuple[str, ...]:
    if command == "all":
        return ("schedule", "previews", "matches")
    return (command,)


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
        return service.sync_previews()
    if operation == "matches":
        return service.sync_matches(limit=args.max_matches)
    if operation == "profiles":
        return service.sync_profiles(
            limit=200 if args.limit is None else args.limit,
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


def _collect_traffic(
    report: dict[str, Any], scope: RunnerScope, service: Any
) -> None:
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
    retryable_errors = [
        item for item in report["error_details"] if item["retryable"]
    ]
    fatal_errors = [
        item for item in report["error_details"] if not item["retryable"]
    ]
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


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    scopes = _resolve_scopes(parser, args.scope)
    report = _new_report(args.command, scopes)

    try:
        catalog_cls, service_cls = _load_runtime()
        catalog = catalog_cls.from_file()
    except Exception as exc:
        for scope in scopes:
            record = _scope_record(report, scope)
            _record_error(report, record, scope, "runtime", exc)
            _set_scope_status(record)
        return _finish(report, args.output)

    resolved: list[tuple[RunnerScope, Any]] = []
    for scope in scopes:
        record = _scope_record(report, scope)
        try:
            catalog_scope = catalog.resolve_scope(
                scope.competition_id, scope.season_id
            )
            competition = catalog.competition(scope.competition_id)
            if not competition.whoscored_enabled:
                raise ValueError(
                    f"WhoScored is not enabled for {scope.competition_id}"
                )
            resolved.append((scope, catalog_scope))
        except Exception as exc:
            _record_error(report, record, scope, "scope", exc)
            _set_scope_status(record)

    logger.info(
        "Starting WhoScored runner: command=%s scopes=%s operations=%s",
        args.command,
        [scope.spec for scope, _ in resolved],
        _operations(args.command),
    )

    if args.command == "profiles" and len(resolved) != len(scopes):
        # A profile run has one global candidate union and one global budget.
        # Running a subset after one selector failed validation would silently
        # change that population, so every otherwise-valid selector fails with
        # the same run instead of remaining in a misleading pending state.
        for selected_scope, _ in resolved:
            selected_record = _scope_record(report, selected_scope)
            selected_record["status"] = "failed"
            selected_record["blocked_by_scope_validation"] = True
    elif args.command == "profiles" and resolved:
        owner, owner_catalog_scope = resolved[0]
        owner_record = _scope_record(report, owner)
        owner_record["status"] = "running"
        try:
            with service_cls(owner_catalog_scope, catalog=catalog) as service:
                result = _invoke(
                    service,
                    "profiles",
                    args,
                    profile_scopes=[item[1] for item in resolved],
                )
                _merge_result(report, result, scope_record=owner_record)
                _record_result_state(
                    report, owner_record, owner, "player_profile", result
                )
                _collect_traffic(report, owner, service)
        except Exception as exc:
            _record_error(report, owner_record, owner, "profiles", exc)
        _set_scope_status(owner_record)
        for selected_scope, _ in resolved[1:]:
            selected_record = _scope_record(report, selected_scope)
            selected_record["status"] = owner_record["status"]
            selected_record["delegated_to"] = owner.spec
    elif args.command != "profiles":
        for scope, catalog_scope in resolved:
            scope_record = _scope_record(report, scope)
            scope_record["status"] = "running"
            try:
                with service_cls(catalog_scope, catalog=catalog) as service:
                    for operation in _operations(args.command):
                        try:
                            result = _invoke(service, operation, args)
                            _merge_result(
                                report, result, scope_record=scope_record
                            )
                            _record_result_state(
                                report,
                                scope_record,
                                scope,
                                operation,
                                result,
                            )
                        except Exception as exc:
                            _record_error(
                                report, scope_record, scope, operation, exc
                            )
                    _collect_traffic(report, scope, service)
            except Exception as exc:
                _record_error(report, scope_record, scope, "service", exc)
            _set_scope_status(scope_record)

    return _finish(report, args.output)


if __name__ == "__main__":
    sys.exit(main())
