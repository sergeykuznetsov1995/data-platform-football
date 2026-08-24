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

    if phase not in {"season", "matches"}:
        raise ValueError("phase must be season or matches")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    capture_key = str(scope["capture_key"])
    canonical = str(scope["canonical_season"])
    base_run_id = str(scope["run_id"])
    plan_phase = "season" if phase == "season" else "targets"
    entity = "all" if phase == "season" else "match_capture"
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
        season_freshness_key="final",
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
        "--phase", choices=("metadata", "season", "matches", "all"), default="all"
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--workload-artifact", required=True)
    parser.add_argument("--raw-store-uri")
    parser.add_argument("--manifest-backend", default="trino", choices=("trino",))
    parser.add_argument("--force-replace", action="store_true")
    parser.add_argument("--run-id")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
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
