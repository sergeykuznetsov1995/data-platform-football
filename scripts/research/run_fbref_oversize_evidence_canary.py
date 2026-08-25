"""Run one exact, bounded, nonpublishing FBref oversize diagnostic cohort."""

from __future__ import annotations

import argparse
import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from scrapers.fbref.control import make_control_run_id
from scrapers.fbref.pipeline import FBrefPipeline, PipelineSettings


CANARY_DAG_ID = "fbref_oversize_evidence_canary"
CANARY_PAGE_KINDS = ("season_stats",)
CANARY_LOCK_TTL_SECONDS = 4 * 60 * 60
_RUN_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,159}\Z")
_TARGET_ID = re.compile(
    r"fbref:season_stats:[^:\s]{1,64}:[^:\s]{1,64}:"
    r"(?:keepers|misc|playingtime|shooting|standard)\Z"
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class CanaryConfigurationError(ValueError):
    """The requested diagnostic cannot satisfy its fixed safety contract."""


class CanaryExecutionError(RuntimeError):
    """Redacted failure which preserves only the failed lifecycle stage."""

    def __init__(self, *, stage: str, run_id: str) -> None:
        super().__init__(f"FBref oversize diagnostic failed during {stage}")
        self.stage = stage
        self.run_id = run_id


@dataclass(frozen=True)
class CanaryConfig:
    logical_run_label: str
    proxy_file: Path
    reviewed_source_run_id: str
    reviewed_terminal_snapshot_sha256: str
    target_ids: Sequence[str]

    def __post_init__(self) -> None:
        label = str(self.logical_run_label).strip()
        if not _RUN_LABEL.fullmatch(label):
            raise CanaryConfigurationError("logical run label is invalid")
        try:
            source_run_id = str(uuid.UUID(str(self.reviewed_source_run_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise CanaryConfigurationError(
                "reviewed source run id is invalid"
            ) from exc
        snapshot_sha256 = str(
            self.reviewed_terminal_snapshot_sha256
        ).strip()
        if not _SHA256.fullmatch(snapshot_sha256):
            raise CanaryConfigurationError(
                "reviewed terminal snapshot SHA256 is invalid"
            )
        target_ids = tuple(str(item).strip() for item in self.target_ids)
        if not 1 <= len(target_ids) <= 25:
            raise CanaryConfigurationError(
                "exact cohort must contain between 1 and 25 targets"
            )
        if len(target_ids) != len(set(target_ids)):
            raise CanaryConfigurationError("exact cohort contains duplicates")
        if any(not _TARGET_ID.fullmatch(item) for item in target_ids):
            raise CanaryConfigurationError(
                "exact cohort contains a non-oversize season-stat target"
            )
        proxy_file = Path(self.proxy_file)
        try:
            valid_proxy = (
                proxy_file.is_absolute()
                and proxy_file.is_file()
                and proxy_file.stat().st_size > 0
            )
        except OSError:
            valid_proxy = False
        if not valid_proxy:
            raise CanaryConfigurationError(
                "proxy file must be absolute, readable, and non-empty"
            )
        object.__setattr__(self, "logical_run_label", label)
        object.__setattr__(self, "proxy_file", proxy_file)
        object.__setattr__(self, "reviewed_source_run_id", source_run_id)
        object.__setattr__(
            self,
            "reviewed_terminal_snapshot_sha256",
            snapshot_sha256,
        )
        object.__setattr__(self, "target_ids", target_ids)


def _finish_and_release(pipeline: Any, run_id: str, *, succeeded: bool) -> None:
    """Close the control run and writer fence; preserve the caller's verdict."""

    try:
        pipeline.control.finish_run(run_id, succeeded=succeeded)
    finally:
        pipeline.control.release_publication_lock(run_id)


def run_canary(
    config: CanaryConfig,
    *,
    pipeline: Optional[Any] = None,
) -> dict[str, object]:
    """Install exactly the reviewed cohort and execute one fetch-only wave."""

    run_id = make_control_run_id(
        config.logical_run_label,
        dag_id=CANARY_DAG_ID,
    )
    active_pipeline = pipeline or FBrefPipeline.from_env()
    settings = PipelineSettings.acceptance(
        scope="current",
        proxy_file=str(config.proxy_file),
    )
    stage = "initialize"
    lock_acquired = False
    run_initialized = False
    try:
        initialized = active_pipeline.initialize_acceptance_run(
            airflow_run_id=config.logical_run_label,
            dag_id=CANARY_DAG_ID,
            settings=settings,
            execution_metadata={
                "reviewed_source_run_id": config.reviewed_source_run_id,
                "reviewed_terminal_snapshot_sha256": (
                    config.reviewed_terminal_snapshot_sha256
                ),
            },
        )
        if initialized != run_id:
            raise RuntimeError("unexpected control run identity")
        run_initialized = True

        stage = "acquire_publication_lock"
        active_pipeline.control.acquire_publication_lock(
            run_id,
            dag_id=CANARY_DAG_ID,
            ttl_seconds=CANARY_LOCK_TTL_SECONDS,
        )
        lock_acquired = True

        stage = "seed_exact_cohort"
        routes = tuple(
            sorted({target_id.rsplit(":", 1)[-1] for target_id in config.target_ids})
        )
        frozen = active_pipeline.seed_acceptance_cohort(
            run_id,
            config.target_ids,
            settings=settings,
            required_page_kinds=CANARY_PAGE_KINDS,
            required_routes=routes,
            coverage_slots={
                f"oversize:{index}": target_id
                for index, target_id in enumerate(config.target_ids)
            },
        )
        if tuple(frozen.get("target_ids") or ()) != tuple(config.target_ids):
            raise RuntimeError("frozen cohort differs from requested cohort")

        stage = "fetch"
        active_pipeline.fetch_wave(
            run_id,
            worker_id=f"oversize-evidence:{run_id}",
            page_kinds=CANARY_PAGE_KINDS,
            settings=settings,
        )

        stage = "finish"
        _finish_and_release(active_pipeline, run_id, succeeded=True)
        lock_acquired = False
    except Exception:  # noqa: BLE001 - return a redacted hard failure
        if run_initialized:
            try:
                if lock_acquired:
                    _finish_and_release(active_pipeline, run_id, succeeded=False)
                else:
                    active_pipeline.control.finish_run(run_id, succeeded=False)
            except Exception:  # noqa: BLE001 - original stage remains primary
                pass
        raise CanaryExecutionError(stage=stage, run_id=run_id) from None

    return {
        "status": "succeeded",
        "run_id": run_id,
        "target_ids": list(config.target_ids),
        "request_limit": settings.request_limit,
        "byte_limit": settings.byte_limit,
        "shard_size": settings.shard_size,
        "publication_eligible": False,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-label", required=True)
    parser.add_argument("--proxy-file", type=Path, required=True)
    parser.add_argument("--reviewed-source-run-id", required=True)
    parser.add_argument("--reviewed-terminal-snapshot-sha256", required=True)
    parser.add_argument("--target-id", action="append", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = CanaryConfig(
            logical_run_label=args.run_label,
            proxy_file=args.proxy_file,
            reviewed_source_run_id=args.reviewed_source_run_id,
            reviewed_terminal_snapshot_sha256=(
                args.reviewed_terminal_snapshot_sha256
            ),
            target_ids=args.target_id,
        )
        result = run_canary(config)
    except CanaryConfigurationError:
        print(json.dumps({"status": "failed", "stage": "configuration"}))
        return 2
    except CanaryExecutionError as exc:
        print(json.dumps({
            "status": "failed",
            "stage": exc.stage,
            "run_id": exc.run_id,
            "publication_eligible": False,
        }, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
