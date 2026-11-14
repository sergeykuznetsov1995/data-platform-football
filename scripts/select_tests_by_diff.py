#!/usr/bin/env python3
"""Select the unit tests affected by a diff (issue #2244).

Deterministic "diff -> affected test files" selector used by the focused-test
rule in AGENTS.md ("Проверка и сдача") and TESTING.md. Conservative by design:
any doubt escalates to a FULL verdict (run the whole local unit suite).

Mapping convention (basename stem search across all of tests/unit, because
test layout is cross-directory, e.g. dags/scripts/run_sofascore_scraper.py ->
tests/unit/scrapers/test_run_sofascore_scraper.py):

    scrapers/<src>/<module>.py -> test_<src>_<module>.py / test_<module>.py
    dags/dag_<name>.py         -> tests/unit/dags/test_dag_<name>.py
    dags/utils/<module>.py     -> tests/unit/**/test_<module>*.py
    dags/scripts/<x>.py        -> tests/unit/**/test_<x>*.py
    scripts/<x>.py             -> tests/unit/scripts/test_<x>*.py
    dags/sql/**/<x>.sql[.j2]   -> tests/unit/sql/test_<x>_*.py

CLI contract:
    --base REF    explicit diff base (three-dot diff semantics)
    --head REF    diff head (default HEAD)
    --worktree    diff <base> against the working tree (staged+unstaged);
                  untracked files are NOT visible (use `git add -N .` first)
    --files ...   explicit changed-path list; git is not invoked at all
    --json        machine-readable verdict (status/tests/full_triggers/base/head)
    --pytest-args print arguments for pytest; a FULL verdict prints "tests/unit"

Exit codes: 0 verdict emitted (FULL is a verdict, not an error);
2 usage error; 3 git/diff failure (not a repo, bad ref, no merge-base).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TESTS_UNIT = "tests/unit"

# FULL escalation reasons (stable identifiers; tests assert on them).
R_CONFTEST = "conftest"
R_PYTEST_CONFIG = "pytest-config"
R_REQUIREMENTS = "requirements"
R_SCRAPER_BASE = "scraper-base"
R_SCRAPER_SHARED_UTILS = "scraper-shared-utils"
R_PACKAGE_INIT = "package-init"
R_FIXTURES = "fixtures"
R_CONFIGS = "configs"
R_DAG_PLUGINS = "dag-plugins"
R_TESTS_NON_UNIT = "tests-non-unit"
R_SHORT_STEM = "short-stem"
R_NO_TEST_MAPPING = "no-test-mapping"
R_UNMAPPED_PATH = "unmapped-path"


def derive_stem(basename: str) -> str | None:
    """Strip known code suffixes; None when the basename is not code."""
    name = basename
    if name.endswith(".sql.j2"):
        name = name[: -len(".sql.j2")]
    elif name.endswith(".sql"):
        name = name[: -len(".sql")]
    elif name.endswith(".py"):
        name = name[: -len(".py")]
    elif name.endswith(".j2"):
        name = name[: -len(".j2")]
    else:
        return None
    return name


def _parts(path: str) -> list[str]:
    return [p for p in path.split("/") if p and p != "."]


def classify(path: str) -> tuple[str, str]:
    """Map one changed path to a verdict component.

    Returns one of:
        ("full", reason)  escalate to the full local suite
        ("test", path)    the changed path is itself a unit test file
        ("map", stem)     search tests by basename stem
        ("none", "")      documentation/process change; contributes nothing
    """
    parts = _parts(path)
    basename = parts[-1] if parts else path

    if basename == "conftest.py":
        return "full", R_CONFTEST
    if path == "pyproject.toml":
        return "full", R_PYTEST_CONFIG
    if basename.startswith("requirements") and (
        basename.endswith(".txt") or basename.endswith(".in")
    ):
        return "full", R_REQUIREMENTS
    if parts and parts[0] == "requirements":
        return "full", R_REQUIREMENTS

    def under(prefix: str) -> bool:
        return len(parts) > 1 and parts[0] == prefix

    if under("scrapers") and "base" in parts[1:-1]:
        return "full", R_SCRAPER_BASE
    if under("scrapers") and "utils" in parts[1:-1]:
        return "full", R_SCRAPER_SHARED_UTILS
    if basename == "__init__.py" and parts and parts[0] in (
        "scrapers",
        "dags",
        "scripts",
        "tests",
    ):
        return "full", R_PACKAGE_INIT
    if "fixtures" in parts:
        return "full", R_FIXTURES
    if parts and parts[0] == "configs":
        return "full", R_CONFIGS
    if under("dags") and "plugins" in parts[1:-1]:
        return "full", R_DAG_PLUGINS

    if (
        len(parts) > 1
        and parts[0] == "tests"
        and parts[1] == "unit"
        and basename.startswith("test_")
        and basename.endswith(".py")
    ):
        return "test", path
    if parts and parts[0] == "tests":
        return "full", R_TESTS_NON_UNIT

    stem = derive_stem(basename)
    if stem is not None and (under("scrapers") or under("dags") or under("scripts")):
        # dags/sql/** is covered by under("dags"); a stem shorter than 4
        # characters matches too broadly (e.g. "x", "abc") -> FULL.
        if len(stem) < 4:
            return "full", R_SHORT_STEM
        return "map", stem

    if parts and parts[0] in ("docs", ".github"):
        return "none", ""
    if basename.endswith(".md") or path in ("LICENSE", ".gitignore"):
        return "none", ""

    return "full", R_UNMAPPED_PATH


def _match_pattern(stem: str) -> re.Pattern[str]:
    return re.compile(r"(?:^|_)" + re.escape(stem) + r"(?:_|$)")


def test_body(test_path: str) -> str:
    """`tests/unit/scrapers/test_fotmob_parsers.py` -> `fotmob_parsers`."""
    basename = test_path.rsplit("/", 1)[-1]
    if basename.startswith("test_") and basename.endswith(".py"):
        return basename[len("test_") : -len(".py")]
    return basename


def select_tests(changed: list[str], available: list[str]):
    """Pure verdict over changed paths given the available test files.

    ("list", sorted_paths)                focused set (possibly empty)
    ("full", [(path, reason), ...])       escalation triggers, sorted by path
    """
    triggers: list[tuple[str, str]] = []
    selected: set[str] = set()
    for path in changed:
        kind, value = classify(path)
        if kind == "full":
            triggers.append((path, value))
        elif kind == "test":
            if path in available:
                selected.add(path)
        elif kind == "map":
            pattern = _match_pattern(value)
            matches = [t for t in available if pattern.search(test_body(t))]
            if matches:
                selected.update(matches)
            else:
                triggers.append((path, R_NO_TEST_MAPPING))
    if triggers:
        return "full", sorted(triggers)
    return "list", sorted(selected)


def scan_available_tests(repo_root: Path) -> list[str]:
    """All tests/unit/**/test_*.py as sorted relative posix paths."""
    unit = repo_root / TESTS_UNIT
    if not unit.is_dir():
        return []
    return sorted(
        p.relative_to(repo_root).as_posix()
        for p in unit.rglob("test_*.py")
        if p.is_file()
    )


def _git(repo_root: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo_root), "--no-pager", *args],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or "git failed"
        raise RuntimeError(detail)
    return proc.stdout


def resolve_diff(
    repo_root: Path, base: str | None, head: str | None, worktree: bool
) -> tuple[list[str], str | None, str | None]:
    """Changed paths plus resolved base/head SHAs (None when not applicable)."""
    head_ref = head or "HEAD"
    if base is None:
        base = _git(repo_root, "merge-base", head_ref, "origin/master").strip()
        if not base:
            raise RuntimeError(f"no merge-base for {head_ref} and origin/master")
    if worktree:
        out = _git(repo_root, "diff", "--no-renames", "--name-only", base)
        head_sha = None
    else:
        out = _git(
            repo_root,
            "diff",
            "--no-renames",
            "--name-only",
            f"{base}...{head_ref}",
        )
        head_sha = _git(repo_root, "rev-parse", head_ref).strip() or None
    base_sha = _git(repo_root, "rev-parse", f"{base}^{{commit}}").strip() or None
    paths = [line for line in out.splitlines() if line.strip()]
    return paths, base_sha, head_sha


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deterministic diff -> affected tests/unit selector (#2244)."
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--worktree", action="store_true")
    source.add_argument("--files", nargs="+", metavar="PATH")
    parser.add_argument("--base", metavar="REF")
    parser.add_argument("--head", metavar="REF")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--pytest-args", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    repo_root = REPO_ROOT
    try:
        if args.files is not None:
            changed = args.files
            base_sha = None
            head_sha = None
        else:
            changed, base_sha, head_sha = resolve_diff(
                repo_root, args.base, args.head, args.worktree
            )
        available = scan_available_tests(repo_root)
        verdict, payload = select_tests(changed, available)
    except RuntimeError as exc:  # git/diff failure
        print(f"select_tests_by_diff: {exc}", file=sys.stderr)
        return 3

    if args.json:
        print(
            json.dumps(
                {
                    "status": verdict,
                    "tests": payload if verdict == "list" else [],
                    "full_triggers": [
                        {"path": p, "reason": r} for p, r in (payload or [])
                    ]
                    if verdict == "full"
                    else [],
                    "base": base_sha,
                    "head": head_sha,
                },
                ensure_ascii=False,
            )
        )
        return 0

    if verdict == "full":
        if args.pytest_args:
            print(TESTS_UNIT)
        else:
            path, reason = payload[0]
            print(f"FULL {reason}: {path}")
        return 0

    for path in payload:
        print(path)
    if not payload:
        print(
            "select_tests_by_diff: no affected unit tests for this diff",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
