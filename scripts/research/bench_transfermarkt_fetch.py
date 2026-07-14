#!/usr/bin/env python3
"""GET-only Transfermarkt traffic benchmark using the production parser.

The benchmark never calls an Iceberg writer and never stores response bodies.
It exercises one full league roster, a bounded sample of both career endpoints,
and a bounded coach-history sample through the same proxy-only transport used by
Airflow.  A single scraper instance keeps the TLS connection/rate limiter and
the decoded-body budget shared across every phase.

Example (from the repository root)::

    PROXY_FILE=/opt/airflow/proxys.txt \
      python scripts/research/bench_transfermarkt_fetch.py \
        --season 2025 --players 20 --coach-clubs 3 \
        --budget-profile production --cycle-budget-bytes 25165824

The production cycle estimate scales only the sampled per-player/per-club
phases.  It is an estimate, while ``actual_*`` fields are measured counters.
``decoded_response_body_mb`` is not claimed to be provider billing; see the
traffic audit document for the distinction.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scrapers.transfermarkt.models import (  # noqa: E402
    MAX_ROSTER_WINDOW,
    PRODUCTION_ENTITY_BUDGETS,
    SCOPE_HARD_PROVIDER_BYTE_CAP,
)

LOG = logging.getLogger("bench_transfermarkt")
MIB = 1024 * 1024
PRODUCTION_CYCLE_BUDGET_BYTES = SCOPE_HARD_PROVIDER_BYTE_CAP
# Benchmark phases map one-to-one onto the production entity budgets, so the
# benchmark can never disagree with the runner/scraper about the caps.
_PHASE_ENTITY = {
    "squads": "players",
    "market_value_points": "market_value_history",
    "transfer_events": "transfers",
    "coaches": "coaches",
}
PRODUCTION_PHASE_BUDGETS: Dict[str, Dict[str, int]] = {
    phase: {
        "decoded_body_bytes": int(
            PRODUCTION_ENTITY_BUDGETS[entity]["decoded_mb"] * MIB
        ),
        "request_attempts": int(PRODUCTION_ENTITY_BUDGETS[entity]["requests"]),
    }
    for phase, entity in _PHASE_ENTITY.items()
}
MIN_COACH_COMPLETENESS = 0.9
# One production scope cycle buys at most MAX_ROSTER_WINDOW careers per
# career entity under the daily cadence; the projection scales samples to it.
CYCLE_PLAYER_TARGET = MAX_ROSTER_WINDOW
TYPICAL_LEAGUE_CLUBS = 20


def _counter(stats: Mapping[str, Any], key: str) -> int:
    return int(stats.get(key, 0) or 0)


def _phase_delta(
    name: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    rows: Optional[Mapping[str, int]] = None,
) -> Dict[str, Any]:
    """Return counters attributable to one sequential benchmark phase."""

    decoded = (
        _counter(after, "decoded_response_body_bytes")
        - _counter(before, "decoded_response_body_bytes")
    )
    attempts = _counter(after, "request_attempts") - _counter(
        before, "request_attempts"
    )
    retries = _counter(after, "retries") - _counter(before, "retries")
    failures = _counter(after, "failed_attempts") - _counter(
        before, "failed_attempts"
    )
    return {
        "phase": name,
        "decoded_response_body_bytes": decoded,
        "decoded_response_body_mb": round(decoded / MIB, 4),
        "request_attempts": attempts,
        "retries": retries,
        "failed_attempts": failures,
        "rows": dict(rows or {}),
    }


def _append_checked_phase(
    report: Dict[str, Any],
    name: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    rows: Optional[Mapping[str, int]] = None,
) -> Dict[str, Any]:
    """Append a measured phase and reject even a one-byte/attempt breach."""

    from scrapers.transfermarkt.models import TrafficBudgetExceeded

    phase = _phase_delta(name, before, after, rows=rows)
    limits = PRODUCTION_PHASE_BUDGETS[name]
    phase["decoded_body_budget_bytes"] = limits["decoded_body_bytes"]
    phase["request_attempt_budget"] = limits["request_attempts"]
    phase["within_budget"] = (
        phase["decoded_response_body_bytes"] <= limits["decoded_body_bytes"]
        and phase["request_attempts"] <= limits["request_attempts"]
    )
    report["phases"].append(phase)
    if phase["decoded_response_body_bytes"] > limits["decoded_body_bytes"]:
        raise TrafficBudgetExceeded(
            f"{name} decoded-body budget exceeded "
            f"({phase['decoded_response_body_bytes']}/"
            f"{limits['decoded_body_bytes']} bytes)"
        )
    if phase["request_attempts"] > limits["request_attempts"]:
        raise TrafficBudgetExceeded(
            f"{name} request-attempt budget exceeded "
            f"({phase['request_attempts']}/{limits['request_attempts']})"
        )
    return phase


def _cycle_budget_bytes(args: argparse.Namespace) -> int:
    """Resolve the exact byte cap without an MB-rounding conversion."""

    return int(args.cycle_budget_bytes)


def _scaled_bytes(phase: Mapping[str, Any], actual_units: int, target_units: int) -> int:
    if actual_units <= 0:
        return 0
    return int(
        math.ceil(
            int(phase["decoded_response_body_bytes"])
            * target_units
            / actual_units
        )
    )


def _cycle_projection(
    phases: list[Dict[str, Any]],
    *,
    sampled_players: int,
    sampled_coach_clubs: int,
) -> Dict[str, Any]:
    by_name = {phase["phase"]: phase for phase in phases}
    roster = int(by_name.get("squads", {}).get("decoded_response_body_bytes", 0))
    mv = _scaled_bytes(
        by_name.get("market_value_points", {}),
        sampled_players,
        CYCLE_PLAYER_TARGET,
    )
    transfers = _scaled_bytes(
        by_name.get("transfer_events", {}),
        sampled_players,
        CYCLE_PLAYER_TARGET,
    )
    coaches = _scaled_bytes(
        by_name.get("coaches", {}),
        sampled_coach_clubs,
        TYPICAL_LEAGUE_CLUBS,
    )
    total = roster + mv + transfers + coaches
    return {
        "method": (
            "linear sample scaling; roster fixed, careers to "
            f"{CYCLE_PLAYER_TARGET} players, coaches to "
            f"{TYPICAL_LEAGUE_CLUBS} clubs"
        ),
        "decoded_response_body_bytes": total,
        "decoded_response_body_mb": round(total / MIB, 4),
        "components_mb": {
            "squads": round(roster / MIB, 4),
            "market_value_points_window_players": round(mv / MIB, 4),
            "transfer_events_window_players": round(transfers / MIB, 4),
            "coaches_20_clubs": round(coaches / MIB, 4),
        },
    }


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _restore_env(previous: Mapping[str, Optional[str]]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def run(args: argparse.Namespace) -> tuple[int, Dict[str, Any]]:
    """Execute the benchmark and return ``(exit_code, report)``."""

    proxy_path = Path(args.proxy_file)
    if not proxy_path.is_file() or proxy_path.stat().st_size == 0:
        return 2, {
            "status": "configuration_error",
            "error": "proxy file is missing or empty",
            "proxy_file": proxy_path.name,
        }

    from scrapers.transfermarkt import TransfermarktScraper
    from scrapers.transfermarkt.client import redact_sensitive
    from scrapers.transfermarkt.models import TrafficBudgetExceeded

    cycle_budget_bytes = _cycle_budget_bytes(args)
    if not 0 < cycle_budget_bytes <= PRODUCTION_CYCLE_BUDGET_BYTES:
        return 2, {
            "status": "configuration_error",
            "error": (
                "cycle budget must be between 1 and "
                f"{PRODUCTION_CYCLE_BUDGET_BYTES} bytes"
            ),
            "cycle_budget_bytes": cycle_budget_bytes,
        }

    # The production scraper reads these legacy aggregate overrides.  Remove
    # them for this process so its exact per-operation decoded/attempt scopes
    # (the production entity budget canon) remain active; the client gets an
    # independent shared cycle ceiling below.
    previous_env = {
        key: os.environ.get(key)
        for key in ("TM_DECODED_BODY_BUDGET_MB", "TM_REQUEST_BUDGET")
    }
    os.environ.pop("TM_DECODED_BODY_BUDGET_MB", None)
    os.environ.pop("TM_REQUEST_BUDGET", None)

    started_at = time.time()
    report: Dict[str, Any] = {
        "status": "running",
        "league": args.league,
        "season": args.season,
        "sampled_players": args.players,
        "sampled_coach_clubs": 0 if args.skip_coaches else args.coach_clubs,
        "budget_profile": args.budget_profile,
        "cycle_budget_bytes": cycle_budget_bytes,
        "cycle_budget_mib": cycle_budget_bytes / MIB,
        "phase_budgets": PRODUCTION_PHASE_BUDGETS,
        "proxy_required": True,
        "writes_iceberg": False,
        "phases": [],
    }

    scraper = None
    try:
        with TransfermarktScraper(
            leagues=[args.league],
            seasons=[args.season],
            proxy_file=str(proxy_path),
        ) as scraper:
            http_client = getattr(scraper, "_http_client", None)
            set_cycle_budget = getattr(
                http_client, "set_cycle_decoded_body_budget", None,
            )
            if callable(set_cycle_budget):
                set_cycle_budget(cycle_budget_bytes)

            before = dict(scraper.get_traffic_stats())
            squad = scraper.read_squad_data(args.league, args.season)
            after = dict(scraper.get_traffic_stats())
            memberships = squad["memberships"]
            observations = squad["attribute_observations"]
            _append_checked_phase(
                report,
                "squads",
                before,
                after,
                rows={
                    "memberships": len(memberships),
                    "attribute_observations": len(observations),
                },
            )
            if memberships.empty:
                raise RuntimeError("squad parser returned zero memberships")

            player_ids = memberships["player_id"].dropna().astype(str).unique().tolist()
            player_ids = player_ids[: args.players]
            if len(player_ids) < args.players:
                raise RuntimeError(
                    f"only {len(player_ids)} player ids available for sample {args.players}"
                )

            before = after
            points = scraper.read_market_value_points(
                args.league,
                args.season,
                player_ids=player_ids,
                limit=len(player_ids),
            )
            after = dict(scraper.get_traffic_stats())
            _append_checked_phase(
                report,
                "market_value_points",
                before,
                after,
                rows={
                    "rows": len(points),
                    "players": points["player_id"].nunique() if not points.empty else 0,
                },
            )

            before = after
            events = scraper.read_transfer_events(
                args.league,
                args.season,
                player_ids=player_ids,
                limit=len(player_ids),
            )
            after = dict(scraper.get_traffic_stats())
            _append_checked_phase(
                report,
                "transfer_events",
                before,
                after,
                rows={
                    "rows": len(events),
                    "players": events["player_id"].nunique() if not events.empty else 0,
                },
            )

            if not args.skip_coaches:
                before = after
                coach_data = scraper.read_coach_data(
                    args.league,
                    args.season,
                    limit=args.coach_clubs,
                    memberships=memberships,
                    # Explicit public cold-cache mode: benchmark GET traffic
                    # only and never depend on Trino state.
                    coach_profile_cache={},
                )
                after = dict(scraper.get_traffic_stats())
                profiles = coach_data["profiles"]
                stints = coach_data["stints"]
                if not stints.empty and "club_id" in stints.columns:
                    successful_coach_clubs = int(stints["club_id"].nunique())
                else:
                    successful_coach_clubs = min(
                        args.coach_clubs, len(profiles), len(stints),
                    )
                _append_checked_phase(
                    report,
                    "coaches",
                    before,
                    after,
                    rows={
                        "profiles": len(profiles),
                        "stints": len(stints),
                        "successful_clubs": successful_coach_clubs,
                    },
                )
                coach_completeness = successful_coach_clubs / args.coach_clubs
                report["coach_completeness"] = {
                    "successful_clubs": successful_coach_clubs,
                    "sampled_clubs": args.coach_clubs,
                    "ratio": round(coach_completeness, 4),
                    "minimum_ratio": MIN_COACH_COMPLETENESS,
                }
                if coach_completeness < MIN_COACH_COMPLETENESS:
                    raise RuntimeError(
                        "coach-history completeness below 90% "
                        f"({successful_coach_clubs}/{args.coach_clubs})"
                    )

            final_stats = dict(scraper.get_traffic_stats())
            report["traffic"] = final_stats
            actual_bytes = int(final_stats["decoded_response_body_bytes"])
            actual_mb = round(actual_bytes / MIB, 4)
            report["actual_decoded_response_body_bytes"] = actual_bytes
            report["actual_decoded_response_body_mb"] = actual_mb
            report["cycle_projection"] = _cycle_projection(
                report["phases"],
                sampled_players=len(player_ids),
                sampled_coach_clubs=(0 if args.skip_coaches else args.coach_clubs),
            )
            report["status"] = (
                "passed"
                if actual_bytes <= cycle_budget_bytes
                else "budget_exceeded"
            )
            code = 0 if report["status"] == "passed" else 3
    except TrafficBudgetExceeded as exc:
        report["status"] = "budget_exceeded"
        report["error"] = redact_sensitive(exc)
        if scraper is not None:
            report["traffic"] = scraper.get_traffic_stats()
            actual_bytes = report["traffic"].get("decoded_response_body_bytes")
            report["actual_decoded_response_body_bytes"] = actual_bytes
            report["actual_decoded_response_body_mb"] = (
                round(int(actual_bytes) / MIB, 4)
                if actual_bytes is not None else None
            )
        code = 3
    except Exception as exc:  # noqa: BLE001 - benchmark must leave a report
        report["status"] = "failed"
        report["error"] = redact_sensitive(f"{type(exc).__name__}: {exc}")
        if scraper is not None:
            report["traffic"] = scraper.get_traffic_stats()
            actual_bytes = report["traffic"].get("decoded_response_body_bytes")
            report["actual_decoded_response_body_bytes"] = actual_bytes
            report["actual_decoded_response_body_mb"] = (
                round(int(actual_bytes) / MIB, 4)
                if actual_bytes is not None else None
            )
        code = 2
    finally:
        _restore_env(previous_env)

    report["elapsed_seconds"] = round(time.time() - started_at, 2)
    return code, report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", default="ENG-Premier League")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--players", type=int, default=20)
    parser.add_argument("--coach-clubs", type=int, default=3)
    parser.add_argument("--skip-coaches", action="store_true")
    parser.add_argument(
        "--proxy-file",
        default=os.environ.get("PROXY_FILE", "/opt/airflow/proxys.txt"),
    )
    parser.add_argument(
        "--budget-profile",
        choices=("production",),
        default="production",
        help="Use immutable production per-phase byte/attempt limits",
    )
    parser.add_argument(
        "--cycle-budget-bytes",
        type=int,
        default=PRODUCTION_CYCLE_BUDGET_BYTES,
        help=(
            "Shared decoded-body cap; cannot exceed the production "
            f"scope cap ({PRODUCTION_CYCLE_BUDGET_BYTES} bytes)"
        ),
    )
    parser.add_argument(
        "--output",
        default=f"/tmp/bench_transfermarkt_{time.strftime('%Y%m%d_%H%M%S')}.json",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    if not 1 <= args.players <= CYCLE_PLAYER_TARGET:
        parser.error(f"--players must be in [1, {CYCLE_PLAYER_TARGET}]")
    if not 1 <= args.coach_clubs <= TYPICAL_LEAGUE_CLUBS:
        parser.error(f"--coach-clubs must be in [1, {TYPICAL_LEAGUE_CLUBS}]")
    if not 0 < args.cycle_budget_bytes <= PRODUCTION_CYCLE_BUDGET_BYTES:
        parser.error(
            "--cycle-budget-bytes must be in "
            f"[1, {PRODUCTION_CYCLE_BUDGET_BYTES}]"
        )

    code, report = run(args)
    output = Path(args.output)
    _write_report(output, report)
    summary = {
        "status": report.get("status"),
        "actual_decoded_response_body_mb": report.get(
            "actual_decoded_response_body_mb"
        ),
        "actual_decoded_response_body_bytes": report.get(
            "actual_decoded_response_body_bytes"
        ),
        "cycle_projection_mb": (
            report.get("cycle_projection") or {}
        ).get("decoded_response_body_mb"),
        "request_attempts": (report.get("traffic") or {}).get("request_attempts"),
        "elapsed_seconds": report.get("elapsed_seconds"),
        "report": str(output),
    }
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
