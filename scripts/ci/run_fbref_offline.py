#!/usr/bin/env python3
"""Run the complete import-light FBref suite in the current Python environment.

Dependency preparation is explicit: this runner never installs packages or
enters a container. Both Make and the FBref CI job use this selection.
"""

from importlib.util import find_spec
import os
from pathlib import Path
import subprocess
import sys


EXTRA_TESTS = (
    "tests/unit/dags/test_dag_iceberg_maintenance_daily.py",
    "tests/unit/dags/test_maintenance_tasks.py",
    "tests/unit/scripts/test_filter_proxy.py",
    "tests/unit/scrapers/test_proxy_manager.py",
    "tests/unit/scrapers/test_scrapers_lazy_import.py",
)


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    tests = sorted(
        str(path.relative_to(root))
        for path in (root / "tests/unit").rglob("*fbref*.py")
        if path.is_file() and not path.is_symlink()
    )
    if not tests:
        print("No offline FBref tests found under tests/unit", file=sys.stderr)
        return 1
    if find_spec("pytest") is None:
        print(
            "pytest is missing; prepare the FBref test venv from "
            "requirements/test/fbref-unit-py311.lock",
            file=sys.stderr,
        )
        return 1
    env = os.environ.copy()
    env.pop("PYTEST_ADDOPTS", None)
    env.pop("PYTEST_PLUGINS", None)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", *tests, *EXTRA_TESTS],
        cwd=root,
        env=env,
        check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
