#!/usr/bin/env python3
"""Run the WhoScored ingestion service.

The v2 CLI addresses one important modelling bug in the old runner: a list of
leagues and a list of seasons were passed to ``soccerdata`` as a Cartesian
product.  That is invalid for competitions with a calendar-year season (for
example ``INT-World Cup=2026``).  New callers therefore pass one or more
explicit, canonical scopes::

    run_whoscored_scraper.py matches \
        --scope "ENG-Premier League=2526" \
        --scope "INT-World Cup=2026"

The runner deliberately opens one scraper per scope until the scraper service
itself accepts ``WhoScoredScope`` objects.  Besides preventing cross-products,
this isolates caches, browser sessions and failures by competition.

Commands:

``schedule``
    Refresh the schedule and season-stage metadata.
``previews``
    Refresh the targeted missing-player preview set.
``matches``
    Fetch eligible match events and lineups.
``profiles``
    Fetch player profiles (kept separate because it has its own budget/TTL).
``all``
    Run schedule, previews and matches.  Profiles remain an explicit command.

The legacy ``--leagues/--seasons`` and ``--skip-*`` interface remains as a
deprecated bridge for manual backfills.  Production DAGs use only the v2
scope/subcommand interface.
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
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


REPORT_SCHEMA_VERSION = 2
COMMANDS = ("schedule", "previews", "matches", "profiles", "all")


@dataclass(frozen=True, order=True)
class RunnerScope:
    """Canonical competition/season pair used by runner orchestration."""

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
    """UTC timestamp isolated from the legacy tests' patched ``datetime``."""
    return datetime_lib.datetime.now(datetime_lib.timezone.utc).isoformat()


def _parse_seasons(args: argparse.Namespace) -> List[int]:
    """Parse the deprecated season flags.

    This helper is intentionally retained for one compatibility release.  New
    callers must use ``--scope`` and canonical season IDs.
    """
    if args.seasons:
        return [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    return [int(args.season)]


def _season_to_bronze_str(season: Any) -> str:
    """Legacy season-token conversion used only by deprecated CLI flags."""
    s = str(season)
    if len(s) != 4 or not s.isdigit():
        raise ValueError(f"Unrecognized season token: {season!r}")
    if (int(s[:2]) + 1) % 100 == int(s[2:]):
        return s
    if s[2:] == "99":
        return "9900"
    return s[-2:] + f"{(int(s[-2:]) + 1) % 100:02d}"


def _season_start_year(season: Any) -> int:
    """Return a start year for a legacy split-year season token."""
    s = str(season)
    if len(s) != 4 or not s.isdigit():
        raise ValueError(f"Unrecognized season token: {season!r}")
    if s == "9900":
        return 1999
    if (int(s[:2]) + 1) % 100 == int(s[2:]):
        return 2000 + int(s[:2])
    return int(s)


def _trino_connect():
    """Open a Trino connection for the deprecated skip-existing probe."""
    try:
        import trino
        import trino.auth as trino_auth
    except ImportError as exc:
        logger.error("trino client unavailable: %s", exc)
        return None

    user = os.environ.get("TRINO_USER", "airflow")
    password = os.environ.get("TRINO_PASSWORD")
    if password:
        return trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", 8443)),
            user=user,
            catalog="iceberg",
            http_scheme="https",
            auth=trino_auth.BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", 8080)),
        user=user,
        catalog="iceberg",
    )


def _completed_schedule_pairs(leagues: List[str], season_strs: List[str]) -> set:
    """Return legacy schedule scopes that meet both completeness floors."""
    sched_floor = int(os.environ.get("WHOSCORED_SCHEDULE_MIN_ROWS", "270"))
    stages_floor = int(os.environ.get("WHOSCORED_STAGES_MIN_ROWS", "1"))
    conn = _trino_connect()
    if conn is None:
        return set()
    try:
        cur = conn.cursor()
        leagues_ph = ", ".join("?" for _ in leagues)
        seasons_ph = ", ".join("?" for _ in season_strs)

        def _counts(table: str) -> dict:
            sql = (
                "SELECT league, season, COUNT(*) "
                f"FROM iceberg.bronze.{table} "
                f"WHERE league IN ({leagues_ph}) AND season IN ({seasons_ph}) "
                "GROUP BY league, season"
            )
            cur.execute(sql, (*leagues, *season_strs))
            return {
                (row[0], row[1]): row[2]
                for row in cur.fetchall()
                if row and row[0] is not None and row[1] is not None
            }

        schedule = _counts("whoscored_schedule")
        stages = _counts("whoscored_season_stages")
        complete = {
            pair
            for pair, count in schedule.items()
            if count >= sched_floor and stages.get(pair, 0) >= stages_floor
        }
        logger.info(
            "legacy skip-existing probe: schedule=%s stages=%s complete=%s",
            schedule,
            stages,
            sorted(complete),
        )
        return complete
    except Exception as exc:
        logger.warning(
            "skip-existing probe failed (%s); all requested scopes remain due",
            exc,
        )
        return set()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run WhoScored scraper")
    parser.add_argument("command", nargs="?", choices=COMMANDS)
    parser.add_argument(
        "--scope",
        action="append",
        default=[],
        metavar="COMPETITION=SEASON",
        help=(
            "Canonical scope; repeat for multiple independent scopes, e.g. "
            "--scope 'ENG-Premier League=2526'"
        ),
    )

    # Deprecated compatibility flags.  They are intentionally not used by the
    # production DAG, but keep old backfill commands operable during rollout.
    legacy = parser.add_argument_group("deprecated legacy interface")
    legacy.add_argument("--leagues", default="ENG-Premier League")
    legacy.add_argument("--seasons", default="")
    legacy.add_argument("--season", type=int, default=2024)
    legacy.add_argument("--skip-events", action="store_true")
    legacy.add_argument("--skip-missing-players", action="store_true")
    legacy.add_argument("--skip-existing", action="store_true")
    legacy.add_argument("--events-only", action="store_true")
    legacy.add_argument("--player-profile", action="store_true")
    legacy.add_argument("--headless", action="store_true", default=True)
    legacy.add_argument(
        "--proxy-file",
        default="",
        help=(
            "Deprecated compatibility option. Empty by default: production "
            "never silently enables the raw residential proxy list."
        ),
    )

    parser.add_argument("--max-matches", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--flaresolverr-url",
        default=os.environ.get("FLARESOLVERR_URL", "http://flaresolverr:8191"),
    )
    parser.add_argument("--output", default="/tmp/whoscored_result.json")
    return parser


def _resolve_scopes(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> tuple[List[RunnerScope], bool]:
    """Resolve canonical scopes and report whether legacy flags were used."""
    if args.scope:
        try:
            scopes = [RunnerScope.parse(raw) for raw in args.scope]
        except ValueError as exc:
            parser.error(str(exc))
        if len({scope.spec for scope in scopes}) != len(scopes):
            parser.error("Duplicate --scope values are not allowed")
        return scopes, False

    if args.command:
        parser.error(f"the {args.command!r} command requires at least one --scope")

    leagues = [item.strip() for item in args.leagues.split(",") if item.strip()]
    seasons = _parse_seasons(args)
    logger.warning(
        "--leagues/--seasons is deprecated; use a repeatable canonical --scope"
    )

    scopes: List[RunnerScope] = []
    for league in leagues:
        league_seasons = seasons
        if league == "INT-World Cup":
            # Preserve the old active-window bridge without mixing its
            # calendar-year season into club scopes.
            try:
                from utils.medallion_config import get_active_season

                active = get_active_season(league)
            except Exception as exc:
                logger.warning("Could not resolve World Cup window: %s", exc)
                active = None
            if active is None:
                logger.info("INT-World Cup is outside its configured window")
                continue
            league_seasons = [active]
        for season in league_seasons:
            season_id = (
                str(season)
                if league == "INT-World Cup"
                else _season_to_bronze_str(season)
            )
            scopes.append(RunnerScope(league, season_id))
    return scopes, True


def _resolve_command(args: argparse.Namespace, legacy_mode: bool) -> str:
    if args.command:
        return args.command
    if not legacy_mode:
        return "all"
    if args.player_profile:
        return "profiles"
    if args.events_only:
        return "matches"
    return "legacy" if legacy_mode else "all"


def _new_report(command: str, scopes: Iterable[RunnerScope]) -> dict:
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
        # Compatibility projections; new consumers should use ``entities``.
        "tables": [],
        "tables_by_entity": {},
        "traffic": {},
        "traffic_by_scope": {},
    }


def _zero_schedule_traffic() -> dict:
    return {
        "fs_response_bytes": 0,
        "fs_response_mb": 0.0,
        "requests": 0,
        "sessions_created": 0,
        "cf_challenge_failures": 0,
        "top_traffic_urls": [],
    }


def _scope_record(report: dict, scope: RunnerScope) -> dict:
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
    report: dict,
    scope_record: dict,
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
        exc_info=True,
    )


def _extract_entity_result(value: Any) -> tuple[Optional[str], Optional[int]]:
    """Extract a table path/count from both legacy and v2 scraper results."""
    if isinstance(value, str):
        return value, None
    if isinstance(value, Mapping):
        table = value.get("table") or value.get("path")
        rows = value.get("rows_written", value.get("rows"))
        try:
            rows = int(rows) if rows is not None else None
        except (TypeError, ValueError):
            rows = None
        return table, rows
    return None, None


def _merge(
    report: MutableMapping[str, Any],
    entity_to_result: Mapping[str, Any],
    *,
    scope_record: Optional[MutableMapping[str, Any]] = None,
) -> None:
    """Merge legacy ``{entity: table}`` or v2 entity results into a report."""
    if not isinstance(entity_to_result, Mapping):
        return

    # A future service can return a nested v2 result without changing the
    # runner/report contract.
    if isinstance(entity_to_result.get("entities"), Mapping):
        entity_to_result = entity_to_result["entities"]

    for entity, value in entity_to_result.items():
        table, rows = _extract_entity_result(value)
        if not table and rows is None:
            continue

        current = report["entities"].setdefault(
            entity, {"table": table, "rows_written": 0, "counts_complete": True}
        )
        if table:
            current["table"] = table
            report["tables_by_entity"][entity] = table
            if table not in report["tables"]:
                report["tables"].append(table)
        if rows is None:
            current["counts_complete"] = False
            report["row_counts_complete"] = False
        else:
            current["rows_written"] += rows
            report["rows"] += rows

        if scope_record is not None:
            scope_record["entities"][entity] = {
                "table": table,
                "rows_written": rows,
            }


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


def _operations(command: str, args: argparse.Namespace) -> List[str]:
    if command == "schedule":
        return ["schedule", "season_stages"]
    if command == "previews":
        return ["missing_players"]
    if command == "matches":
        return ["events"]
    if command == "profiles":
        return ["player_profile"]
    if command == "all":
        return ["schedule", "missing_players", "season_stages", "events"]

    # Deprecated default path.  Preserve the old flags exactly for one release.
    operations = ["schedule"]
    if not args.skip_missing_players:
        operations.append("missing_players")
    operations.append("season_stages")
    if not args.skip_events:
        operations.append("events")
    return operations


def _invoke(scraper: Any, operation: str, args: argparse.Namespace) -> Mapping[str, Any]:
    if operation == "schedule":
        return scraper.scrape_schedule() or {}
    if operation == "missing_players":
        return scraper.scrape_missing_players() or {}
    if operation == "season_stages":
        return scraper.scrape_season_stages() or {}
    if operation == "events":
        return scraper.scrape_events(max_matches=args.max_matches) or {}
    if operation == "player_profile":
        return scraper.scrape_player_profile(limit=args.limit) or {}
    raise AssertionError(f"Unknown WhoScored operation: {operation}")


def _apply_legacy_skip_existing(
    report: dict,
    scopes: List[RunnerScope],
    args: argparse.Namespace,
    command: str,
) -> List[RunnerScope]:
    if not args.skip_existing:
        return scopes
    if command != "legacy" or not (args.skip_events and args.skip_missing_players):
        logger.warning(
            "--skip-existing is only supported by the deprecated fast schedule path"
        )
        return scopes

    now = datetime.now()
    current_start = now.year if now.month >= 8 else now.year - 1
    past = [scope for scope in scopes if _season_start_year(scope.season_id) < current_start]
    done = (
        _completed_schedule_pairs(
            sorted({scope.competition_id for scope in past}),
            sorted({scope.season_id for scope in past}),
        )
        if past
        else set()
    )
    skipped = [
        scope for scope in scopes if scope in past and (scope.competition_id, scope.season_id) in done
    ]
    if skipped:
        report["skipped_pairs"] = [
            [scope.competition_id, scope.season_id] for scope in sorted(skipped)
        ]
        skipped_specs = {scope.spec for scope in skipped}
        for item in report["scopes"]:
            if item["scope"] in skipped_specs:
                item["status"] = "up_to_date"
        logger.info("legacy skip-existing skipped %s", sorted(skipped_specs))
    return [scope for scope in scopes if scope not in skipped]


def _write_report(path: str, report: Mapping[str, Any]) -> None:
    """Atomically publish the JSON result so callbacks never read half a file."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{output.name}.", dir=str(output.parent))
    try:
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


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    scopes, legacy_mode = _resolve_scopes(parser, args)
    command = _resolve_command(args, legacy_mode)
    report = _new_report(command, scopes)

    scopes_to_run = _apply_legacy_skip_existing(report, scopes, args, command)
    if not scopes_to_run:
        report["status"] = "up_to_date" if scopes else "out_of_window"
        if scopes:
            report["skip_existing"] = True
        report["traffic"] = {
            "events": {},
            "schedule": _zero_schedule_traffic(),
        }
        report["finished_at"] = _utc_now_iso()
        _write_report(args.output, report)
        print(json.dumps(report, ensure_ascii=False))
        return 0

    operations = _operations(command, args)
    logger.info(
        "Starting WhoScored v2 runner: command=%s scopes=%s operations=%s",
        command,
        [scope.spec for scope in scopes_to_run],
        operations,
    )

    try:
        # Lazy import keeps --help/report parsing free of browser dependencies.
        from scrapers.whoscored import WhoScoredScraper
    except Exception as exc:
        for scope in scopes_to_run:
            scope_record = _scope_record(report, scope)
            _record_error(
                report,
                scope_record,
                scope,
                "scraper_import",
                exc,
            )
            scope_record["status"] = "failed"
        report["status"] = "failed"
        report["finished_at"] = _utc_now_iso()
        _write_report(args.output, report)
        return 1

    for scope in scopes_to_run:
        scope_record = _scope_record(report, scope)
        scope_record["status"] = "running"
        try:
            with WhoScoredScraper(
                leagues=[scope.competition_id],
                seasons=[int(scope.season_id)],
                headless=args.headless,
                proxy_file=args.proxy_file,
                flaresolverr_url=args.flaresolverr_url,
                use_v2=not legacy_mode,
            ) as scraper:
                for operation in operations:
                    try:
                        result = _invoke(scraper, operation, args)
                        _merge(report, result, scope_record=scope_record)
                    except Exception as exc:
                        _record_error(
                            report, scope_record, scope, operation, exc
                        )

                try:
                    traffic = scraper.get_traffic_stats() or {}
                except Exception as exc:
                    logger.warning(
                        "get_traffic_stats failed for %s: %s", scope.spec, exc
                    )
                    traffic = {}
                report["traffic_by_scope"][scope.spec] = traffic
                if isinstance(traffic, Mapping):
                    _merge_traffic(report["traffic"], traffic)
        except Exception as exc:
            _record_error(report, scope_record, scope, "scraper", exc)

        if scope_record["errors"]:
            scope_record["status"] = (
                "retryable"
                if all(error["retryable"] for error in scope_record["errors"])
                else "failed"
            )
        else:
            scope_record["status"] = "success"

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
    _write_report(args.output, report)
    logger.info(
        "WhoScored run complete: status=%s scopes=%d rows=%d "
        "row_counts_complete=%s errors=%d",
        report["status"],
        len(scopes_to_run),
        report["rows"],
        report["row_counts_complete"],
        len(report["errors"]),
    )
    print(json.dumps(report, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
