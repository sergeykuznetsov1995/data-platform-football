"""Abort FBref control runs whose worker died without running its failure path.

Same call the Airflow failure callback makes (``ControlStore.abort_run``):
releases the run's fenced leases, settles its budget reservations, and marks
the run failed.  Needed when a runner is killed from outside (a hung Camoufox
launch, an OOM), because a run left in 'running' keeps its targets attached
and no later cohort may claim them.

Usage:
    python scripts/research/abort_fbref_runs.py <run_id> [<run_id> ...]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scrapers.fbref.control import ControlStore  # noqa: E402


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: abort_fbref_runs.py <run_id> [<run_id> ...]", file=sys.stderr)
        return 2
    store = ControlStore.from_env()
    for run_id in argv:
        result = store.abort_run(
            run_id,
            error_class="HungClearance",
            error_message="Camoufox launch hung; runner was killed externally",
        )
        print(json.dumps({"run_id": run_id, **result}, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
