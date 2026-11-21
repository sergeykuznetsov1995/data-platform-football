"""Replay consumer SELECTs against legacy and candidate Transfermarkt relations."""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import closing
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dags.utils import transfermarkt_native_v2 as control
    from dags.utils.transfermarkt_native_replay import QueryPair, compare_many
except ModuleNotFoundError as exc:
    if exc.name not in {"dags", "dags.utils"}:
        raise
    from utils import transfermarkt_native_v2 as control
    from utils.transfermarkt_native_replay import QueryPair, compare_many


def _read_pairs(path: Path) -> tuple[list[QueryPair], dict]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(spec, dict) or not isinstance(spec.get("queries"), list):
        raise ValueError("expected a queries array")
    pairs = [QueryPair(**entry) for entry in spec["queries"]]
    if not pairs or len({pair.name for pair in pairs}) != len(pairs):
        raise ValueError("expected at least one query and unique names")
    metadata = spec.get("metadata")
    if not isinstance(metadata, dict) or any(
        not metadata.get(key)
        for key in ("legacy_snapshot_ids", "native_snapshot_ids", "common_watermark")
    ):
        raise ValueError("snapshot ids and a common watermark are required")
    return pairs, metadata


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--code-sha", required=True)
    parser.add_argument("--candidate-slot", required=True, choices=("a", "b"))
    parser.add_argument("--expected-reader-revision", required=True, type=int)
    args = parser.parse_args(argv)
    try:
        pairs, input_metadata = _read_pairs(args.input)
    except (OSError, ValueError, TypeError):
        print("Invalid replay input.", file=sys.stderr)
        return 1

    report = {
        "code_sha": args.code_sha,
        "candidate_slot": args.candidate_slot,
        "expected_reader_revision": args.expected_reader_revision,
        "input_metadata": input_metadata,
        "passed": False,
        "queries": [],
    }
    try:
        with closing(control.connect()) as conn, closing(conn.cursor()) as cur:
            state = control.read_reader_state(cur)
            report["reader_state"] = state.to_dict()
            if (
                state.revision == args.expected_reader_revision
                and state.active_version == "legacy"
                and state.legacy_writers_disabled_at is None
            ):
                report.update(compare_many(cur, pairs))
            else:
                report["reason"] = "pre-cutover reader state mismatch"
    except Exception:
        # Driver errors can contain SQL text and connection details.
        report["reason"] = "replay driver failure"
    try:
        args.output.write_text(
            json.dumps(report, default=str, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    except OSError:
        print("Unable to write replay report.", file=sys.stderr)
        return 1
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
