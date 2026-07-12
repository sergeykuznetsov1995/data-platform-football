"""Benchmark SoFIFA FlareSolverr response traffic (issue #616).

This research harness is intentionally limited to SoFIFA. WhoScored uses its
own direct-first transport, raw cache, paid-proxy leases, and byte ceilings;
the pre-V2 benchmark path bypassed those controls and was removed.

Run inside an Airflow container::

    python scripts/research/bench_flaresolverr_fetch.py --source sofifa

Set ``BENCH_SOFIFA_PLAYER_IDS`` to a comma-separated fixed player set. The
in-process counters cover HTML returned by FlareSolverr, not every browser
asset, so container network counters remain the source for total proxy bytes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
import time
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bench_fs")

sys.path.insert(0, "/opt/airflow")


def _pick_proxy_url(proxy_file: str):
    """Return one configured proxy, or ``None`` for a direct benchmark."""
    from scrapers.utils.proxy_manager import ProxyManager

    manager = ProxyManager(rotation_strategy="random")
    try:
        count = manager.load_from_file_custom_format(proxy_file)
    except Exception as exc:  # noqa: BLE001 - research harness diagnostics
        log.warning("proxy file %s unreadable (%s); running direct", proxy_file, exc)
        return None
    if count <= 0:
        log.warning("no proxies in %s; running direct", proxy_file)
        return None
    proxy = manager.get_proxy()
    return proxy.url if proxy else None


def _bench_sofifa(
    n: int,
    proxy_file: str,
    flaresolverr_url: str,
    league: str,
    versions: str,
):
    from scrapers.sofifa.flaresolverr_reader import FlareSolverrSoFIFAReader

    reader = FlareSolverrSoFIFAReader(
        flaresolverr_url=flaresolverr_url,
        proxy=_pick_proxy_url(proxy_file),
        versions=versions,
        leagues=[league],
    )
    try:
        configured = os.environ.get("BENCH_SOFIFA_PLAYER_IDS", "").strip()
        if configured:
            player_ids = [
                int(value)
                for value in configured.split(",")
                if value.strip()
            ][:n]
        else:
            players = reader.read_players().index.unique().tolist()
            player_ids = [int(player_id) for player_id in players][:n]
        if not player_ids:
            raise SystemExit("SoFIFA bench: no player ids resolved")

        started = time.monotonic()
        reader.read_player_ratings(player=player_ids)
        elapsed = round(time.monotonic() - started, 2)
        stats = reader._fs_client.get_traffic_stats()
        details = [
            {
                "player_ids": player_ids,
                "count": len(player_ids),
                "seconds": elapsed,
                "success": True,
            }
        ]
        return stats, details, len(player_ids), len(player_ids)
    finally:
        reader.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Bench SoFIFA FlareSolverr traffic")
    parser.add_argument("--source", choices=["sofifa"], required=True)
    parser.add_argument("--n", type=int, default=10)
    parser.add_argument("--league", default="ENG-Premier League")
    parser.add_argument("--versions", default="latest")
    parser.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    parser.add_argument(
        "--flaresolverr-url",
        default=os.environ.get("FLARESOLVERR_URL", "http://flaresolverr:8191"),
    )
    args = parser.parse_args()

    label = os.environ.get("BENCH_LABEL", "unlabeled")
    report_path = f"/tmp/bench_sofifa_{label}.json"
    started = time.monotonic()
    stats, details, attempted, succeeded = _bench_sofifa(
        args.n,
        args.proxy_file,
        args.flaresolverr_url,
        args.league,
        args.versions,
    )
    elapsed = round(time.monotonic() - started, 2)
    durations = [item["seconds"] for item in details if item.get("success")]
    report = {
        "label": label,
        "source": args.source,
        "matches_attempted": attempted,
        "matches_succeeded": succeeded,
        "success_rate": round(succeeded / attempted, 2) if attempted else None,
        "total_seconds": elapsed,
        "mean_seconds": round(statistics.mean(durations), 2) if durations else None,
        "fs_response_mb": stats.get("fs_response_mb", 0.0),
        "fs_response_bytes": stats.get("fs_response_bytes", 0),
        "requests": stats.get("requests", 0),
        "sessions_created": stats.get("sessions_created", 0),
        "cf_challenge_failures": stats.get("cf_challenge_failures", 0),
        "top_traffic_urls": stats.get("top_traffic_urls", []),
        "per_match": details,
    }
    Path(report_path).write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )
    print(json.dumps(report, indent=2, default=str))
    print(f"Full report: {report_path}")


if __name__ == "__main__":
    main()
