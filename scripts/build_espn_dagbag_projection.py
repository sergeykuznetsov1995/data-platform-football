#!/usr/bin/env python3
"""Build a fresh, exact DagBag root for the isolated ESPN Airflow stack."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil


ESPN_DAG_FILES = (
    "dag_backfill_espn.py",
    "dag_discover_espn_registry.py",
    "dag_ingest_espn.py",
    "dag_monitor_espn.py",
    "dag_repair_espn.py",
    "dag_replay_espn.py",
    "dag_trigger_espn_daily.py",
)
SUPPORT_DIRECTORIES = ("scripts", "utils")
CONTAINER_DAG_ROOT = Path("/opt/espn-source/dags")


class ProjectionError(ValueError):
    """The requested projection is incomplete or would overwrite a path."""


def build_projection(*, release_root: Path, output: Path) -> None:
    """Create an exact projection without mutating or following release files."""

    if not release_root.is_absolute() or not output.is_absolute():
        raise ProjectionError("release root and output must be absolute paths")
    release_root = release_root.resolve(strict=True)
    if output.exists() or output.is_symlink():
        raise ProjectionError("projection output must be a new path")

    source_dags = release_root / "dags"
    airflowignore = release_root / "configs/espn/isolated.airflowignore"
    for name in ESPN_DAG_FILES:
        source = source_dags / name
        if not source.is_file() or source.is_symlink():
            raise ProjectionError(f"release is missing regular ESPN DAG {name}")
    for name in SUPPORT_DIRECTORIES:
        source = source_dags / name
        if not source.is_dir() or source.is_symlink():
            raise ProjectionError(f"release is missing ESPN support directory {name}")
    if not airflowignore.is_file() or airflowignore.is_symlink():
        raise ProjectionError("release is missing isolated.airflowignore")

    output.mkdir(mode=0o750, parents=True, exist_ok=False)
    shutil.copyfile(airflowignore, output / ".airflowignore")
    (output / ".airflowignore").chmod(0o644)
    for name in (*ESPN_DAG_FILES, *SUPPORT_DIRECTORIES):
        (output / name).symlink_to(CONTAINER_DAG_ROOT / name)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a non-overwriting isolated ESPN Airflow DagBag projection"
    )
    parser.add_argument("--release-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        build_projection(release_root=args.release_root, output=args.output)
    except (OSError, ProjectionError) as exc:
        parser.error(str(exc))
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
