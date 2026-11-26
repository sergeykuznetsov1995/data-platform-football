"""Manual replay of a named, local Transfermarkt fixture package through the core CLI."""

from __future__ import annotations

from collections import Counter
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import re
import shutil
import stat
from tempfile import TemporaryDirectory


@contextmanager
def _directories(parts, parent=None):
    """Pin every path component so a renamed directory cannot redirect the replay."""
    with ExitStack() as stack:
        for name in parts:
            parent = os.open(
                name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
            )
            stack.callback(os.close, parent)
        yield parent


@contextmanager
def _file(package_fd, path):
    with _directories(path.parts[:-1], package_fd) as directory:
        fd = os.open(
            path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError(f"rehearsal input must be a regular file: {path}")
            yield stream


def _copy_member(directory, path, dest):
    with _file(directory, path) as src:
        target = dest / path
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as dst:
            shutil.copyfileobj(src, dst)


def _snapshot(package_fd, target):
    with _file(package_fd, Path("manifest.json")) as src:
        manifest = json.load(src)
    if not isinstance(manifest, dict) or not isinstance(manifest.get("fixtures"), list):
        raise ValueError("invalid rehearsal manifest")

    members = set()
    for item in manifest["fixtures"]:
        if not isinstance(item, dict):
            raise ValueError("invalid rehearsal fixture")
        endpoint, capture_id, blob = (
            item.get(field) for field in ("endpoint", "capture_id", "blob_file")
        )
        if not isinstance(endpoint, str) or not re.fullmatch(
            r"[a-z][a-z0-9_]*", endpoint
        ):
            raise ValueError("invalid fixture endpoint")
        if not isinstance(capture_id, str) or not re.fullmatch(
            r"[a-f0-9]{64}", capture_id
        ):
            raise ValueError("invalid fixture capture ID")
        capture = Path("captures", endpoint, f"{capture_id}.json")
        if item.get("capture_file", capture.as_posix()) != capture.as_posix():
            raise ValueError("invalid fixture capture path")
        if (
            not isinstance(blob, str)
            or not blob.endswith(".gz")
            or not all(
                re.fullmatch(r"[a-zA-Z0-9_-][a-zA-Z0-9._-]*", name)
                for name in blob.split("/")
            )
        ):
            raise ValueError("invalid fixture blob path")
        members.update((capture, Path(blob)))

    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    for path in sorted(members):
        _copy_member(package_fd, path, target)
    try:
        _copy_member(package_fd, Path("plan.json"), target)
    except FileNotFoundError:
        pass


def run_rehearsal(root: Path, package: str, league: str, season: str) -> dict:
    """Return counts for a complete offline replay, raising on an incomplete or invalid package."""
    from dags.scripts import run_transfermarkt_core

    if not isinstance(package, str) or not re.fullmatch(
        r"[a-zA-Z0-9][a-zA-Z0-9_-]*", package
    ):
        raise ValueError("invalid rehearsal package name")
    if not isinstance(league, str) or not re.fullmatch(r"[A-Z0-9]{2,6}", league):
        raise ValueError("invalid rehearsal league")
    if not isinstance(season, str) or not re.fullmatch(r"[0-9]{4}", season):
        raise ValueError("invalid rehearsal season")

    location = Path(root).absolute() / package
    if ".." in location.parts:
        raise ValueError("invalid rehearsal root")
    with TemporaryDirectory(prefix="tm-core-replay-") as temp:
        work = Path(temp)
        snapshot = work / "input"
        with _directories(location.parts) as package_fd:
            _snapshot(package_fd, snapshot)
        report_path = work / "report.json"
        argv = [
            "offline",
            "--require-complete",
            "--fixtures-root",
            str(snapshot),
            "--league",
            league,
            "--season",
            season,
            "--report-out",
            str(report_path),
        ]
        if (snapshot / "plan.json").is_file():
            argv.extend(["--section-plan", str(snapshot / "plan.json")])
        try:
            status = run_transfermarkt_core.main(argv)
        except SystemExit as exc:
            raise RuntimeError(
                f"incomplete rehearsal for {package}: CLI exit {exc.code}"
            ) from exc
        report = (
            json.loads(report_path.read_text(encoding="utf-8"))
            if report_path.exists()
            else {}
        )
        completion = report.get("completion") or {}
        if status != 0 or completion.get("passed") is not True:
            reason = completion.get("error") or f"CLI exit {status}"
            raise RuntimeError(f"incomplete rehearsal for {package}: {reason}")
        entities = Counter()
        for item in report["outcomes"]:
            entities[item["entity"]] += item["rows"]
        return {
            "package": package,
            "league": league,
            "season": season,
            "pages": report["fetched"],
            "outcomes": len(report["outcomes"]),
            "rows": sum(entities.values()),
            "entities": dict(sorted(entities.items())),
        }
