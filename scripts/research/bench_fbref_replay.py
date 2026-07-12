"""Benchmark FBref match parsing from saved HTML without network traffic."""

from __future__ import annotations

import argparse
import gzip
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scrapers.fbref.match_parser import DatasetStatus, parse_match_html  # noqa: E402
from scrapers.fbref.raw_store import RawPageStore, match_page_target  # noqa: E402
from scrapers.fbref.typed_bronze import (  # noqa: E402
    TypedSourceContext,
    parse_match_html as parse_typed_match_html,
)


def _load_pages(html_dir: Path) -> dict[str, str]:
    pages = {}
    for path in sorted(html_dir.glob("*.html.gz")):
        match_id = path.name.split(".", 1)[0]
        with gzip.open(path, "rt") as fh:
            pages[match_id] = fh.read()
    if not pages:
        raise SystemExit(f"No *.html.gz files found in {html_dir}")
    return pages


def _parse_once(
    store: RawPageStore,
    pages: dict[str, str],
) -> tuple[dict, int, int, int]:
    row_counts = {}
    contracts_passed = 0
    raw_writes = 0
    raw_hits = 0
    for match_id, captured_html in pages.items():
        target = match_page_target(match_id)
        html, record, cache_hit = store.get_or_fetch(
            target, lambda _url, payload=captured_html: payload
        )
        raw_hits += int(cache_hit)
        raw_writes += int(not cache_hit)
        result = parse_match_html(
            html,
            match_id=match_id,
            league="ENG-Premier League",
            season=2025,
        )
        store.write_parse_manifests(record, result)
        contracts_passed += (
            result.datasets["match_player_stats"].status
            == DatasetStatus.AVAILABLE
        )
        for name, dataset in result.datasets.items():
            row_counts[name] = row_counts.get(name, 0) + dataset.row_count
    return row_counts, contracts_passed, raw_writes, raw_hits


def _compatibility_benchmark(
    pages: dict[str, str], iterations: int
) -> tuple[float, float]:
    """Compare the typed production adapter with its unchanged parser baseline."""

    context = TypedSourceContext(
        source_competition_id="9",
        source_season_id="2025-2026",
        competition_name="Premier League",
        season_label="2025-2026",
    )

    def baseline_pass() -> None:
        for match_id, html in pages.items():
            parse_match_html(
                html,
                match_id=match_id,
                league="ENG-Premier League",
                season=2025,
            )

    def typed_pass() -> None:
        for match_id, html in pages.items():
            parse_typed_match_html(
                html,
                match_id=match_id,
                context=context,
                require_player_contract=False,
            )

    # Remove one-time imports/parser caches from the measurement.
    baseline_pass()
    typed_pass()
    baseline_durations = []
    typed_durations = []
    for iteration in range(iterations):
        ordered = (
            ((baseline_pass, baseline_durations), (typed_pass, typed_durations))
            if iteration % 2 == 0
            else ((typed_pass, typed_durations), (baseline_pass, baseline_durations))
        )
        for callback, durations in ordered:
            started = time.perf_counter()
            callback()
            durations.append(time.perf_counter() - started)
    divisor = len(pages)
    return (
        statistics.mean(baseline_durations) / divisor,
        statistics.mean(typed_durations) / divisor,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--html-dir", type=Path, required=True)
    parser.add_argument(
        "--raw-store-uri",
        help="file:// or s3:// URI; default is a temporary local store",
    )
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument(
        "--max-regression-percent",
        type=float,
        default=10.0,
        help="Fail when typed offline parsing is slower than baseline by more than this",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    pages = _load_pages(args.html_dir)
    temporary = None
    if args.raw_store_uri:
        store = RawPageStore.from_uri(args.raw_store_uri)
    else:
        temporary = tempfile.TemporaryDirectory(prefix="fbref-raw-replay-")
        store = RawPageStore.from_uri(Path(temporary.name).as_uri())
    durations = []
    row_counts: dict[str, int] = {}
    contracts_passed = 0
    raw_pages_written = 0
    raw_page_hits = 0
    for _ in range(max(1, args.iterations)):
        started = time.perf_counter()
        (
            row_counts,
            contracts_passed,
            written,
            hits,
        ) = _parse_once(store, pages)
        raw_pages_written += written
        raw_page_hits += hits
        durations.append(time.perf_counter() - started)

    benchmark_iterations = max(1, args.iterations)
    baseline_seconds, typed_seconds = _compatibility_benchmark(
        pages, benchmark_iterations
    )
    regression_percent = (typed_seconds / baseline_seconds - 1.0) * 100.0
    performance_gate_passed = regression_percent <= args.max_regression_percent

    report = {
        "matches": len(pages),
        "iterations": len(durations),
        "mean_total_seconds": round(statistics.mean(durations), 4),
        "mean_seconds_per_match": round(
            statistics.mean(durations) / len(pages), 4
        ),
        "player_contracts_passed": contracts_passed,
        "row_counts": row_counts,
        "raw_pages_written": raw_pages_written,
        "raw_page_hits": raw_page_hits,
        "proxy_bytes": 0,
        "compatibility_baseline_seconds_per_match": round(
            baseline_seconds, 4
        ),
        "typed_seconds_per_match": round(typed_seconds, 4),
        "typed_regression_percent": round(regression_percent, 2),
        "max_regression_percent": args.max_regression_percent,
        "performance_gate_passed": performance_gate_passed,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)
    if temporary is not None:
        temporary.cleanup()
    if not performance_gate_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
