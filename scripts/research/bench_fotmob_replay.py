"""Replay saved FotMob league payloads without network or production writes.

The default is five iterations over artifacts produced by
``bench_fotmob_fetch.py``.  The decoded JSON bytes are kept in memory, while
JSON decoding and domain parsing are timed on every iteration.  This keeps the
benchmark independent of disk cache noise and guarantees proxy traffic is
zero.

Parser selection is lazy:

* ``--parser auto`` uses ``scrapers.fotmob.parsers.parse_season_bundle`` when
  that refactored pure parser exists, otherwise it uses the standalone source
  summary from the fetch harness.
* ``--parser standalone`` always uses the baseline summary.
* ``--parser package.module:function`` loads an explicit callable.  The
  callable receives ``payload`` and, if it declares a ``scope`` parameter,
  ``scope=None``.  It must return a mapping, dataclass or object containing
  sequence attributes such as ``matches`` and ``standings``.

Example::

    python scripts/research/bench_fotmob_replay.py \
      --artifact-dir /tmp/fotmob-benchmark/baseline
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import importlib
import inspect
import json
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[2]
for import_root in (REPO_ROOT, SCRIPT_DIR):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from bench_fotmob_fetch import (  # noqa: E402
    atomic_write_json,
    canonical_json_bytes,
    json_schema_paths,
    sha256_bytes,
    summarize_payload,
    utc_now,
)


AUTO_PARSER = "scrapers.fotmob.parsers:parse_season_bundle"
COLLECTION_FIELDS = (
    "matches",
    "standings",
    "stages",
    "playoffs",
    "teams",
    "player_categories",
    "team_categories",
    "categories",
    "leaderboards",
    "issues",
)
MAPPING_FIELDS = ("details", "capabilities")


@dataclass(frozen=True)
class RawCase:
    key: str
    competition_id: int
    source_season_key: str
    artifact_path: Path
    decoded_body: bytes
    payload_sha256: str


@dataclass(frozen=True)
class ParserAdapter:
    name: str
    function: Callable[..., Any]
    standalone: bool = False

    def __call__(self, payload: Any) -> Any:
        if self.standalone:
            return {
                "row_counts": summarize_payload(payload),
                "json_paths": json_schema_paths(payload),
            }
        signature = inspect.signature(self.function)
        if "scope" in signature.parameters:
            return self.function(payload, scope=None)
        return self.function(payload)


def standalone_parser() -> ParserAdapter:
    return ParserAdapter(
        name="standalone_source_summary",
        function=summarize_payload,
        standalone=True,
    )


def _load_dotted_callable(spec: str) -> Callable[..., Any]:
    if ":" not in spec:
        raise ValueError("parser must be 'auto', 'standalone', or module:function")
    module_name, function_name = spec.split(":", 1)
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    if not callable(function):
        raise TypeError(f"{spec} is not callable")
    return function


def load_parser(spec: str) -> ParserAdapter:
    """Resolve a parser only when replay starts, never at module import time."""

    if spec == "standalone":
        return standalone_parser()
    if spec == "auto":
        try:
            function = _load_dotted_callable(AUTO_PARSER)
        except (ImportError, AttributeError):
            return standalone_parser()
        return ParserAdapter(name=AUTO_PARSER, function=function)
    return ParserAdapter(name=spec, function=_load_dotted_callable(spec))


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return {
            field.name: _jsonable(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _jsonable(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(child) for child in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable(child) for child in value), key=repr)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def summarize_parser_result(result: Any) -> dict[str, Any]:
    """Reduce parser output to stable row counts and content hashes."""

    row_counts: dict[str, int] = {}
    collection_hashes: dict[str, str] = {}
    mapping_hashes: dict[str, str] = {}
    json_paths: list[str] = []

    if isinstance(result, Mapping):
        supplied_counts = result.get("row_counts")
        if isinstance(supplied_counts, Mapping):
            row_counts.update(
                {str(key): int(value) for key, value in supplied_counts.items()}
            )
        supplied_paths = result.get("json_paths")
        if isinstance(supplied_paths, (list, tuple, set, frozenset)):
            json_paths = sorted(str(path) for path in supplied_paths)
    else:
        supplied_paths = getattr(result, "json_paths", None)
        if isinstance(supplied_paths, (list, tuple, set, frozenset)):
            json_paths = sorted(str(path) for path in supplied_paths)

    for name in COLLECTION_FIELDS:
        if isinstance(result, Mapping):
            value = result.get(name)
        else:
            value = getattr(result, name, None)
        if isinstance(value, (list, tuple, set, frozenset)):
            rendered = _jsonable(value)
            row_counts[name] = len(value)
            collection_hashes[name] = sha256_bytes(canonical_json_bytes(rendered))

    for name in MAPPING_FIELDS:
        if isinstance(result, Mapping):
            value = result.get(name)
        else:
            value = getattr(result, name, None)
        if isinstance(value, Mapping):
            mapping_hashes[name] = sha256_bytes(canonical_json_bytes(_jsonable(value)))

    if not row_counts:
        # An explicit parser may return a single mapping row.  Recording it as
        # one output is safer than silently reporting zero rows.
        if isinstance(result, Mapping):
            row_counts["parser_result"] = 1
        elif result is None:
            row_counts["parser_result"] = 0
        else:
            row_counts["parser_result"] = 1

    normalized = {
        "row_counts": dict(sorted(row_counts.items())),
        "collection_hashes": dict(sorted(collection_hashes.items())),
        "mapping_hashes": dict(sorted(mapping_hashes.items())),
        "json_path_count": len(json_paths),
        "json_paths_sha256": (
            sha256_bytes(canonical_json_bytes(json_paths)) if json_paths else None
        ),
    }
    normalized["summary_sha256"] = sha256_bytes(canonical_json_bytes(normalized))
    return normalized


def _resolve_artifact_path(
    report: Mapping[str, Any], report_path: Path, value: str
) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    root_value = report.get("artifact_dir")
    root = Path(str(root_value)) if root_value else report_path.parent
    return root / candidate


def load_corpus(report_path: Path) -> list[RawCase]:
    """Load and verify every successful raw artifact in a fetch report."""

    report = json.loads(report_path.read_text(encoding="utf-8"))
    cases: list[RawCase] = []
    missing: list[str] = []
    corrupt: list[str] = []
    failed: list[str] = []
    for target in report.get("targets") or []:
        if target.get("ok") is False:
            failed.append(str(target.get("key", "unknown")))
            continue
        artifact_value = target.get("artifact_path")
        if not artifact_value:
            missing.append(str(target.get("key", "unknown")))
            continue
        path = _resolve_artifact_path(report, report_path, str(artifact_value))
        if not path.is_file():
            missing.append(f"{target.get('key', 'unknown')}:{path}")
            continue
        try:
            body = gzip.decompress(path.read_bytes())
            payload = json.loads(body.decode("utf-8-sig"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            corrupt.append(f"{target.get('key', 'unknown')}:{type(exc).__name__}")
            continue

        expected_body_hash = target.get("decoded_body_sha256")
        if expected_body_hash and sha256_bytes(body) != expected_body_hash:
            corrupt.append(f"{target.get('key', 'unknown')}:decoded_body_sha256")
            continue
        payload_hash = sha256_bytes(canonical_json_bytes(payload))
        expected_payload_hash = target.get("payload_sha256")
        if expected_payload_hash and payload_hash != expected_payload_hash:
            corrupt.append(f"{target.get('key', 'unknown')}:payload_sha256")
            continue

        cases.append(
            RawCase(
                key=str(target["key"]),
                competition_id=int(target["competition_id"]),
                source_season_key=str(target["source_season_key"]),
                artifact_path=path,
                decoded_body=body,
                payload_sha256=payload_hash,
            )
        )

    if failed or missing or corrupt:
        raise ValueError(
            "raw corpus is incomplete: "
            f"failed={failed or []}, missing={missing or []}, corrupt={corrupt or []}"
        )
    if not cases:
        raise ValueError(f"no raw FotMob artifacts referenced by {report_path}")
    return cases


def nearest_rank(values: Sequence[float], percentile: float) -> float:
    """Return an observed percentile using the nearest-rank definition."""

    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    rank = max(1, int((percentile * len(ordered) + 0.999999999)))
    return ordered[min(len(ordered), rank) - 1]


def run_replay(
    corpus: Sequence[RawCase],
    *,
    parser: ParserAdapter,
    iterations: int = 5,
    clock: Callable[[], float] = time.perf_counter,
) -> dict[str, Any]:
    if not corpus:
        raise ValueError("replay corpus is empty")
    iterations = max(1, int(iterations))
    durations: list[float] = []
    iteration_details: list[dict[str, Any]] = []
    reference_hash: Optional[str] = None
    final_targets: dict[str, Any] = {}

    for iteration in range(1, iterations + 1):
        started = clock()
        targets: dict[str, Any] = {}
        for case in corpus:
            payload = json.loads(case.decoded_body.decode("utf-8-sig"))
            result = parser(payload)
            targets[case.key] = {
                "competition_id": case.competition_id,
                "source_season_key": case.source_season_key,
                "payload_sha256": case.payload_sha256,
                **summarize_parser_result(result),
            }
        elapsed = max(0.0, clock() - started)
        result_hash = sha256_bytes(canonical_json_bytes(targets))
        if reference_hash is None:
            reference_hash = result_hash
        elif result_hash != reference_hash:
            raise RuntimeError(
                "parser output is non-deterministic: "
                f"iteration 1={reference_hash}, iteration {iteration}={result_hash}"
            )
        durations.append(elapsed)
        iteration_details.append(
            {
                "iteration": iteration,
                "seconds": round(elapsed, 9),
                "result_sha256": result_hash,
            }
        )
        final_targets = targets

    total_rows: Counter[str] = Counter()
    for target in final_targets.values():
        total_rows.update(target["row_counts"])
    decoded_bytes = sum(len(case.decoded_body) for case in corpus)
    p50 = statistics.median(durations)
    p95 = nearest_rank(durations, 0.95)
    return {
        "schema_version": "fotmob.replay-benchmark.v1",
        "mode": "offline_no_network_no_production_writes",
        "generated_at": utc_now(),
        "parser": parser.name,
        "payloads": len(corpus),
        "iterations": iterations,
        "metrics": {
            "mean_total_seconds": round(statistics.mean(durations), 9),
            "p50_total_seconds": round(p50, 9),
            "p95_total_seconds": round(p95, 9),
            "p50_seconds_per_payload": round(p50 / len(corpus), 9),
            "p95_seconds_per_payload": round(p95 / len(corpus), 9),
            "corpus_decoded_bytes": decoded_bytes,
            "corpus_decoded_mb": round(decoded_bytes / 1024 / 1024, 6),
            "network_logical_targets": 0,
            "network_attempts": 0,
            "encoded_direct_bytes": 0,
            "decoded_direct_bytes": 0,
            "encoded_proxy_bytes": 0,
            "decoded_proxy_bytes": 0,
            "row_counts": dict(sorted(total_rows.items())),
        },
        "deterministic_result_sha256": reference_hash,
        "payload_hashes": {case.key: case.payload_sha256 for case in corpus},
        "targets": final_targets,
        "iteration_details": iteration_details,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fetch-report", type=Path)
    source.add_argument(
        "--artifact-dir",
        type=Path,
        help="Directory containing fetch-report.json",
    )
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--parser", default="auto")
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    report_path = args.fetch_report or args.artifact_dir / "fetch-report.json"
    output_path = args.output or report_path.parent / "replay-report.json"
    corpus = load_corpus(report_path)
    parser = load_parser(args.parser)
    report = run_replay(
        corpus,
        parser=parser,
        iterations=max(1, args.iterations),
    )
    report["source_fetch_report"] = str(report_path.resolve())
    atomic_write_json(output_path, report)
    summary = {
        "schema_version": report["schema_version"],
        "parser": report["parser"],
        "payloads": report["payloads"],
        "iterations": report["iterations"],
        "metrics": report["metrics"],
        "deterministic_result_sha256": report["deterministic_result_sha256"],
        "report_path": str(output_path),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
