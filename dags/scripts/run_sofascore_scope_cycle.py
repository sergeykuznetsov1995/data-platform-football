#!/usr/bin/env python3
"""Run one immutable source-native SofaScore tournament-season scope."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(ROOT / "dags"))

from scrapers.sofascore.all_mens_campaign import (  # noqa: E402
    ScopeOverlayPaths,
    load_exact_scope,
    render_scope_overlays,
)


# The two mappings ARE the list of runnable phases: a phase exists exactly when
# it names both a signed plan phase and the capture entity that spends it.
PLAN_PHASES = {"season": "season", "matches": "targets", "players": "players"}
CAPTURE_ENTITIES = {
    "season": "all",
    "matches": "match_capture",
    "players": "player_capture",
}


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def run_phase(
    phase: str,
    scope: Mapping[str, Any],
    *,
    output_dir: str | Path,
    workload_artifact: str | Path,
) -> dict[str, Any]:
    """Prepare and execute one signed existing capture phase."""

    from dags.scripts.prepare_sofascore_workload import (
        CompetitionSeason,
        prepare_workload_plan,
    )
    from dags.scripts.run_sofascore_scraper import main as run_capture

    if phase not in PLAN_PHASES:
        raise ValueError(f"phase must be one of {sorted(PLAN_PHASES)}")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    capture_key = str(scope["capture_key"])
    canonical = str(scope["canonical_season"])
    base_run_id = str(scope["run_id"])
    plan_phase = PLAN_PHASES[phase]
    entity = CAPTURE_ENTITIES[phase]
    dag_id = str(scope.get("dag_id") or "dag_backfill_sofascore_all_mens")
    plan = prepare_workload_plan(
        dag_id=dag_id,
        base_run_id=base_run_id,
        phase=plan_phase,
        competition_seasons=[CompetitionSeason(capture_key, canonical)],
        artifact_path=workload_artifact,
        output_path=destination / f"{plan_phase}-plan.json",
        raw_store_uri=scope.get("raw_store_uri"),
        manifest_backend=str(scope.get("manifest_backend") or "trino"),
        force_replace=bool(scope.get("force_replace")),
        allow_inactive_season=True,
        # The players lane's queue IS its rotation: one tournament-season per
        # signed plan, planned only after its matches closed.  Without the
        # force flag the weekly cohort of the daily ingest would silently drop
        # the scope from the plan and the capture would fail on the missing
        # partition instead.
        players_force=True,
        season_freshness_key="final",
        season_evidence=str(scope.get("season_evidence") or "pages"),
    )
    argv = [
        "--entity", entity,
        "--league", capture_key,
        "--season", canonical,
        "--allow-inactive-season",
        "--manifest-backend", str(scope.get("manifest_backend") or "trino"),
        "--workload-plan", str(plan),
        "--output", str(destination / f"{phase}.json"),
    ]
    if scope.get("raw_store_uri"):
        argv.extend(["--raw-store-uri", str(scope["raw_store_uri"])])
    if scope.get("force_replace"):
        argv.append("--force-replace")
    exit_code = int(run_capture(argv))
    return {
        "phase": phase,
        "status": "success" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "plan": str(plan),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--tournament-id", required=True, type=int)
    parser.add_argument("--source-season-id", required=True, type=int)
    parser.add_argument("--expected-snapshot-id", required=True)
    parser.add_argument("--expected-campaign-id", required=True)
    parser.add_argument(
        "--phase",
        choices=("metadata", "season", "matches", "players", "all"),
        default="all",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--workload-artifact", required=True)
    parser.add_argument("--raw-store-uri")
    parser.add_argument("--manifest-backend", default="trino", choices=("trino",))
    parser.add_argument("--force-replace", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--season-evidence",
        choices=("pages", "bronze"),
        default="pages",
        help=(
            "bronze: plan the matches phase from finished games already in "
            "bronze.sofascore_schedule instead of season pages (refresh lane; "
            "--phase matches only)."
        ),
    )
    parser.add_argument(
        "--allow-pending-season",
        action="store_true",
        help="Accept a snapshot season whose metadata is still pending.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    from dags.scripts.prepare_sofascore_workload import PlayerEvidenceNotReady

    parser = _parser()
    args = parser.parse_args(argv)
    if args.season_evidence == "bronze" and args.phase != "matches":
        parser.error("--season-evidence bronze requires --phase matches")
    if args.phase == "players" and args.allow_pending_season:
        # Profiles need every match of the season committed to Bronze, and a
        # season whose metadata is still pending cannot have that.  Refuse here
        # instead of paying for a Trino round trip to learn the same thing.
        parser.error("--phase players cannot use --allow-pending-season")
    output_dir = Path(args.output_dir).resolve()
    output = Path(args.output).resolve()
    result: dict[str, Any] = {"status": "running", "phases": [], "errors": []}
    previous_registry = os.environ.get("SOFASCORE_REGISTRY_PATH")
    previous_medallion = os.environ.get("MEDALLION_CONFIG_DIR")
    previous_run_id = os.environ.get("SOFASCORE_RUN_ID")
    try:
        scope = load_exact_scope(
            args.snapshot,
            tournament_id=args.tournament_id,
            source_season_id=args.source_season_id,
            expected_snapshot_id=args.expected_snapshot_id,
            expected_campaign_id=args.expected_campaign_id,
            allow_pending_season=bool(args.allow_pending_season),
        )
        paths = render_scope_overlays(scope, output_dir / "scope")
        run_id = args.run_id or (
            "manual__sofascore-all-men__"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + "__"
            + str(scope["scope_digest"])[:16]
        )
        scope.update({
            "run_id": run_id,
            "raw_store_uri": args.raw_store_uri,
            "manifest_backend": args.manifest_backend,
            "force_replace": bool(args.force_replace),
            "season_evidence": args.season_evidence,
            "dag_id": (
                os.environ.get("AIRFLOW_CTX_DAG_ID")
                or "dag_backfill_sofascore_all_mens"
            ),
        })
        os.environ["SOFASCORE_REGISTRY_PATH"] = str(paths.registry_path)
        os.environ["MEDALLION_CONFIG_DIR"] = str(paths.competitions_path.parent)
        os.environ["SOFASCORE_RUN_ID"] = run_id
        result.update({
            "snapshot_id": scope["snapshot_id"],
            "campaign_id": scope["campaign_id"],
            "scope_digest": scope["scope_digest"],
            "tournament_id": scope.get("tournament_id", args.tournament_id),
            "source_season_id": scope.get(
                "source_season_id", args.source_season_id
            ),
            "capture_key": scope["capture_key"],
            "canonical_season": scope["canonical_season"],
            "run_id": run_id,
        })
        phases = {
            "metadata": (),
            "season": ("season",),
            "matches": ("matches",),
            "players": ("players",),
            # "all" deliberately stops at matches: player evidence is only
            # plannable once every match of the season has committed Bronze,
            # which is exactly what the matches phase is still doing.
            "all": ("season", "matches"),
        }[args.phase]
        for phase in phases:
            phase_result = run_phase(
                phase,
                scope,
                output_dir=output_dir,
                workload_artifact=args.workload_artifact,
            )
            result["phases"].append(phase_result)
            if phase_result.get("status") != "success":
                result["status"] = "failed"
                _atomic_json(output, result)
                return 1
        result["status"] = "success"
        _atomic_json(output, result)
        return 0
    except PlayerEvidenceNotReady as exc:
        # "Too early", not "broken": no paid traffic happened, so the scope is
        # deferred and the task stays green.  The lane parks it in its failure
        # memory and comes back after the cooldown.
        result["status"] = "deferred"
        result["deferral_reason"] = f"{type(exc).__name__}: {exc}"
        _atomic_json(output, result)
        return 0
    except Exception as exc:
        result["status"] = "failed"
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        _atomic_json(output, result)
        return 1
    finally:
        for key, previous in (
            ("SOFASCORE_REGISTRY_PATH", previous_registry),
            ("MEDALLION_CONFIG_DIR", previous_medallion),
            ("SOFASCORE_RUN_ID", previous_run_id),
        ):
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ScopeOverlayPaths", "main", "run_phase"]
