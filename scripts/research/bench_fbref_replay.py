"""Benchmark FBref match parsing from saved HTML without network traffic."""

from __future__ import annotations

import argparse
import gzip
import json
import statistics
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scrapers.fbref.data_readers import FBrefDataReaderMixin  # noqa: E402


class _ReplayParser(FBrefDataReaderMixin):
    def __init__(self, pages: dict[str, str]) -> None:
        self._pages = pages

    def _fetch_page(self, url: str, **_: object) -> str | None:
        return self._pages.get(url.rstrip("/").rsplit("/", 1)[-1])

    @staticmethod
    def _add_metadata(df, _entity_type: str):
        return df


def _load_pages(html_dir: Path) -> dict[str, str]:
    pages = {}
    for path in sorted(html_dir.glob("*.html.gz")):
        match_id = path.name.split(".", 1)[0]
        with gzip.open(path, "rt") as fh:
            pages[match_id] = fh.read()
    if not pages:
        raise SystemExit(f"No *.html.gz files found in {html_dir}")
    return pages


def _parse_once(parser: _ReplayParser, match_ids: list[str]) -> tuple[dict, int]:
    buffers = {
        name: []
        for name in (
            "shot_events",
            "match_events",
            "lineups",
            "match_team_stats",
            "match_player_stats",
            "match_managers",
            "match_officials",
            "match_keeper_stats",
        )
    }
    contracts_passed = 0
    for match_id in match_ids:
        got = parser._process_single_match(
            match_id=match_id,
            league="ENG-Premier League",
            season=2025,
            all_shot_events=buffers["shot_events"],
            all_match_events=buffers["match_events"],
            all_lineups=buffers["lineups"],
            all_match_team_stats=buffers["match_team_stats"],
            all_match_player_stats=buffers["match_player_stats"],
            all_match_managers=buffers["match_managers"],
            all_match_officials=buffers["match_officials"],
            all_match_keeper_stats=buffers["match_keeper_stats"],
        )
        contracts_passed += "match_player_stats" in got

    row_counts = {
        name: sum(len(frame) for frame in frames)
        for name, frames in buffers.items()
    }
    return row_counts, contracts_passed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--html-dir", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    pages = _load_pages(args.html_dir)
    replay = _ReplayParser(pages)
    durations = []
    row_counts: dict[str, int] = {}
    contracts_passed = 0
    for _ in range(max(1, args.iterations)):
        started = time.perf_counter()
        row_counts, contracts_passed = _parse_once(replay, list(pages))
        durations.append(time.perf_counter() - started)

    report = {
        "matches": len(pages),
        "iterations": len(durations),
        "mean_total_seconds": round(statistics.mean(durations), 4),
        "mean_seconds_per_match": round(
            statistics.mean(durations) / len(pages), 4
        ),
        "player_contracts_passed": contracts_passed,
        "row_counts": row_counts,
        "proxy_bytes": 0,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
