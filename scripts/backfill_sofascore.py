#!/usr/bin/env python3
"""Historical SofaScore backfill through the production capture engine only."""

from __future__ import annotations

import argparse
import hashlib
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dags.scripts.prepare_sofascore_workload import (
    CompetitionSeason,
    prepare_workload_plan,
)
from dags.scripts.run_sofascore_scraper import main as run_capture


def _new_invocation_run_id() -> str:
    """Return a collision-resistant logical run id for immutable phase plans."""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"manual__sofascore-backfill__{timestamp}__{uuid.uuid4().hex}"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", default="ENG-Premier League")
    parser.add_argument(
        "--seasons", required=True, help="Comma-separated canonical/start years"
    )
    parser.add_argument("--output-dir", default="/tmp/sofascore-backfill")
    parser.add_argument("--offline-replay", action="store_true")
    parser.add_argument("--force-replace", action="store_true")
    parser.add_argument(
        "--run-id",
        help=(
            "Unique logical invocation id. Omit to generate one; never reuse "
            "an id for a new or force-replace run."
        ),
    )
    parser.add_argument("--raw-store-uri")
    parser.add_argument(
        "--workload-artifact",
        default=os.environ.get("SOFASCORE_PROXY_BUDGET_ARTIFACT"),
    )
    args = parser.parse_args(argv)

    invocation_run_id = (args.run_id or _new_invocation_run_id()).strip()
    if not invocation_run_id or "::" in invocation_run_id:
        parser.error("--run-id must be non-empty and cannot contain '::'")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for token in (value.strip() for value in args.seasons.split(",")):
        if not token:
            continue
        season = token
        safe_season = "".join(
            character if character.isalnum() else "-" for character in season
        ).strip("-")
        if not safe_season:
            parser.error(f"invalid season token: {token!r}")
        common = [
            "--league",
            args.league,
            "--season",
            season,
            "--allow-inactive-season",
            "--manifest-backend",
            "trino",
        ]
        if args.offline_replay:
            common.append("--offline-replay")
        if args.force_replace:
            common.append("--force-replace")
        if args.raw_store_uri:
            common.extend(["--raw-store-uri", args.raw_store_uri])
        if not args.offline_replay and not args.workload_artifact:
            parser.error(
                "--workload-artifact or SOFASCORE_PROXY_BUDGET_ARTIFACT "
                "is required for live backfill"
            )

        base_run_id = f"{invocation_run_id}--{args.league}--{season}"
        plan_tag = hashlib.sha256(base_run_id.encode("utf-8")).hexdigest()[:16]
        previous_run_id = os.environ.get("SOFASCORE_RUN_ID")
        os.environ["SOFASCORE_RUN_ID"] = base_run_id
        season_plan = None
        target_plan = None
        player_plan = None

        # Historical backfill first materializes the full paginated schedule
        # through the same raw/manifest engine. Match capture then resolves only
        # finished event IDs from that Bronze partition; it never falls back to
        # a standalone schedule scraper.
        try:
            if not args.offline_replay:
                season_plan = prepare_workload_plan(
                    dag_id="dag_ingest_sofascore",
                    base_run_id=base_run_id,
                    phase="season",
                    competition_seasons=[CompetitionSeason(args.league, season)],
                    artifact_path=args.workload_artifact,
                    output_path=(
                        output_dir / f"{safe_season}-{plan_tag}-season-plan.json"
                    ),
                    raw_store_uri=args.raw_store_uri,
                    force_replace=args.force_replace,
                    allow_inactive_season=True,
                )
            season_args = (
                [] if season_plan is None else ["--workload-plan", str(season_plan)]
            )
            result = run_capture(
                [
                    "--entity",
                    "all",
                    *common,
                    *season_args,
                    "--output",
                    str(output_dir / f"{safe_season}-season.json"),
                ]
            )
            if result != 0:
                return int(result)
            if not args.offline_replay:
                target_plan = prepare_workload_plan(
                    dag_id="dag_ingest_sofascore",
                    base_run_id=base_run_id,
                    phase="targets",
                    competition_seasons=[CompetitionSeason(args.league, season)],
                    artifact_path=args.workload_artifact,
                    output_path=(
                        output_dir / f"{safe_season}-{plan_tag}-target-plan.json"
                    ),
                    raw_store_uri=args.raw_store_uri,
                    force_replace=args.force_replace,
                    allow_inactive_season=True,
                )
            target_args = (
                [] if target_plan is None else ["--workload-plan", str(target_plan)]
            )
            result = run_capture(
                [
                    "--entity",
                    "match_capture",
                    *common,
                    *target_args,
                    "--output",
                    str(output_dir / f"{safe_season}-matches.json"),
                ]
            )
            if result != 0:
                return int(result)

            # Match Bronze now contains the current run's lineup/incidents.
            # Snapshot squads + that fresh evidence only after match capture,
            # then sign a distinct player phase under the same parent run.
            if not args.offline_replay:
                player_plan = prepare_workload_plan(
                    dag_id="dag_ingest_sofascore",
                    base_run_id=base_run_id,
                    phase="players",
                    competition_seasons=[CompetitionSeason(args.league, season)],
                    artifact_path=args.workload_artifact,
                    output_path=(
                        output_dir / f"{safe_season}-{plan_tag}-player-plan.json"
                    ),
                    raw_store_uri=args.raw_store_uri,
                    force_replace=args.force_replace,
                    allow_inactive_season=True,
                )
            player_args = (
                [] if player_plan is None else ["--workload-plan", str(player_plan)]
            )
            result = run_capture(
                [
                    "--entity",
                    "player_capture",
                    *common,
                    *player_args,
                    "--output",
                    str(output_dir / f"{safe_season}-players.json"),
                ]
            )
            if result != 0:
                return int(result)
        finally:
            if previous_run_id is None:
                os.environ.pop("SOFASCORE_RUN_ID", None)
            else:
                os.environ["SOFASCORE_RUN_ID"] = previous_run_id
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
