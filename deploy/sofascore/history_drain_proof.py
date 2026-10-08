"""Read-only accounting proof for a drained history slot run. Stdlib, host-safe."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def accounting_status(runtime: Path, run_id: str) -> str:
    """t: every claimed attempt accounted; f: known incomplete; unknown: no proof.

    Never derive accounting from the colour of worker map0. Unissued queue entries
    were not attempted and do not need failure accounting.
    """
    try:
        document = json.loads((runtime / "history-controller.json").read_text())
        run = document["run"]
        if run.get("run_id") != run_id or run.get("mode") != "slots":
            return "unknown"
        ledger = {key: value for key, value in run.items() if key != "slot_digest"}
        if run.get("slot_digest") != hashlib.sha256(json.dumps(ledger, sort_keys=True).encode()).hexdigest():
            return "unknown"
        plan = run["plan"]
        expected = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
        if run.get("plan_digest") != expected:
            return "unknown"
        cursor, items, slots = run["cursor"], run["items"], run["slots"]
        if (isinstance(cursor, bool) or not isinstance(cursor, int) or not isinstance(plan, list)
                or not 0 <= cursor <= len(plan) or not isinstance(items, dict)
                or not isinstance(slots, dict)
                or set(items) != {str(index) for index in range(cursor)}):
            return "unknown"
        plan_indexes = [item.get("plan_index") if isinstance(item, dict) else None for item in items.values()]
        if (any(isinstance(index, bool) or not isinstance(index, int)
                or not 0 <= index < len(plan) for index in plan_indexes)
                or len(set(plan_indexes)) != cursor):
            return "unknown"
        if run.get("finalized") is not True or any(value is not None for value in slots.values()):
            return "f"
        for item in items.values():
            if (item.get("accounted") is not True or not item.get("finished_at")
                    or not item.get("started_at") or not isinstance(item.get("outcome"), dict)
                    or item["outcome"].get("status") not in ("success", "failed", "not_started")):
                return "f"
            if item["outcome"]["status"] == "not_started" and (isinstance(item.get("attempts"), bool) or item.get("attempts") != 0):
                return "unknown"
        return "t"
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return "unknown"


if __name__ == "__main__":
    print(accounting_status(Path(sys.argv[1]), sys.argv[2]))
