"""
Bench: time-per-fetch для FBref match pages, baseline vs Track A.

Запускается внутри airflow-webserver. Берёт 10 fixed match URLs APL 2025/26,
создаёт FBrefScraper в production-mode и вызывает _fetch_page() по очереди.
Замеры:
- time-per-match (sec)
- success rate
- per-fetch path: HTTP fast-path vs nodriver
- accumulated proxy traffic (MB / requests)

Run:
  docker exec -e BENCH_LABEL=baseline airflow-webserver \
    bash -c 'cd /opt/airflow && python scripts/research/bench_fbref_fetch.py'

Output:
  /tmp/bench_fbref_<label>.json
"""
from __future__ import annotations

import gzip
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
log = logging.getLogger("bench")

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

LABEL = os.environ.get("BENCH_LABEL", "unlabeled")
REPORT_PATH = os.environ.get(
    "FBREF_BENCH_REPORT_PATH", f"/tmp/bench_fbref_{LABEL}.json"
)
PROXY_FILE = os.environ.get(
    "FBREF_BENCH_PROXY_FILE", "/opt/airflow/proxys.txt"
)
HTML_DIR = os.environ.get("FBREF_BENCH_HTML_DIR")

MATCH_PATHS = [
    "/en/matches/a071faa8/Liverpool-Bournemouth-August-15-2025-Premier-League",
    "/en/matches/bbdf4739/Aston-Villa-Newcastle-United-August-16-2025-Premier-League",
    "/en/matches/e1058522/Sunderland-West-Ham-United-August-16-2025-Premier-League",
    "/en/matches/57c49bae/Brighton-and-Hove-Albion-Fulham-August-16-2025-Premier-League",
    "/en/matches/d41bb8b3/Tottenham-Hotspur-Burnley-August-16-2025-Premier-League",
    "/en/matches/8613020d/Wolverhampton-Wanderers-Manchester-City-August-16-2025-Premier-League",
    "/en/matches/2185fc64/Chelsea-Crystal-Palace-August-17-2025-Premier-League",
    "/en/matches/855f8b5e/Nottingham-Forest-Brentford-August-17-2025-Premier-League",
    "/en/matches/643d26fd/Manchester-United-Arsenal-August-17-2025-Premier-League",
    "/en/matches/0701e218/Leeds-United-Everton-August-18-2025-Premier-League",
]


def main() -> None:
    from scrapers.fbref import FBrefScraper

    match_limit = max(
        1,
        min(
            len(MATCH_PATHS),
            int(os.environ.get("FBREF_BENCH_MATCH_LIMIT", len(MATCH_PATHS))),
        ),
    )
    match_paths = MATCH_PATHS[:match_limit]
    log.info(f"BENCH_LABEL={LABEL}, fetching {len(match_paths)} matches")

    html_dir = Path(HTML_DIR) if HTML_DIR else None
    if html_dir is not None:
        html_dir.mkdir(parents=True, exist_ok=True)

    per_match = []
    bench_t0 = time.monotonic()

    with FBrefScraper(
        leagues=["ENG-Premier League"],
        seasons=[2025],
        headless=True,
        use_xvfb=True,
        proxy_file=PROXY_FILE,
        use_nodriver=True,
        nodriver_cloudflare_wait=90,
    ) as scraper:
        # Issue #616: BENCH_FORCE_NODRIVER=1 disables the curl_cffi HTTP
        # fast-path so EVERY match goes through the nodriver browser. This
        # reproduces the cold / HTTP-fallback regime (the one behind the
        # 2.12 MB/match headline) where CDP network-blocking actually applies
        # to matches 2..N — letting us measure the BLOCKED_URL_PATTERNS effect.
        if os.environ.get("BENCH_FORCE_NODRIVER") == "1":
            scraper._fetch_page_http = lambda *a, **k: None
            log.info("BENCH_FORCE_NODRIVER=1 — HTTP fast-path disabled (cold regime)")
        for i, path in enumerate(match_paths, 1):
            url = "https://fbref.com" + path
            t0 = time.monotonic()
            html = None
            err = None
            try:
                html = scraper._fetch_page(url, use_cache=False, page_type="match")
            except Exception as e:  # noqa: BLE001
                err = f"{type(e).__name__}: {e}"
            elapsed = round(time.monotonic() - t0, 2)
            stats = scraper._stats
            entry = {
                "i": i,
                "path": path,
                "seconds": elapsed,
                "bytes": len(html) if html else 0,
                "success": html is not None and len(html) > 50000,
                "http_fetch_ok_cumul": stats.get("http_fetch_ok", 0),
                "http_fetch_fallback_cumul": stats.get("http_fetch_fallback", 0),
                "successes_cumul": stats.get("successes", 0),
                "failures_cumul": stats.get("failures", 0),
            }
            if err:
                entry["error"] = err
            per_match.append(entry)
            if html_dir is not None and html:
                match_id = path.split("/", 4)[3]
                with gzip.open(html_dir / f"{match_id}.html.gz", "wt") as fh:
                    fh.write(html)
            log.info(
                f"  [{i:2d}/{len(match_paths)}] {elapsed:6.2f}s "
                f"bytes={entry['bytes']:>7,} success={entry['success']} "
                f"http_ok={entry['http_fetch_ok_cumul']} "
                f"http_fb={entry['http_fetch_fallback_cumul']}"
            )

        final_stats = dict(scraper._stats)

    bench_total = round(time.monotonic() - bench_t0, 2)
    durations = [m["seconds"] for m in per_match if m["success"]]
    browser_bytes = int(final_stats.get("real_bytes_downloaded", 0) or 0)
    browser_requests = int(final_stats.get("real_requests_count", 0) or 0)
    http_bytes = int(final_stats.get("http_bytes_downloaded", 0) or 0)
    http_requests = int(final_stats.get("http_requests_count", 0) or 0)
    total_proxy_bytes = browser_bytes + http_bytes
    total_proxy_requests = browser_requests + http_requests
    report = {
        "label": LABEL,
        "transport": os.environ.get("FBREF_TRANSPORT", "nodriver"),
        "matches_attempted": len(match_paths),
        "matches_succeeded": sum(1 for m in per_match if m["success"]),
        "total_seconds": bench_total,
        "mean_seconds": round(statistics.mean(durations), 2) if durations else None,
        "p50_seconds": round(statistics.median(durations), 2) if durations else None,
        "p95_seconds": round(sorted(durations)[int(len(durations) * 0.95)], 2)
        if len(durations) >= 3 else None,
        "success_rate": round(sum(1 for m in per_match if m["success"]) / len(match_paths), 2),
        "http_fetch_ok_total": final_stats.get("http_fetch_ok", 0),
        "http_fetch_fallback_total": final_stats.get("http_fetch_fallback", 0),
        "real_bytes_mb": round(browser_bytes / 1024 / 1024, 3),
        "real_requests": browser_requests,
        "http_bytes_mb": round(http_bytes / 1024 / 1024, 3),
        "http_requests": http_requests,
        "total_proxy_bytes": total_proxy_bytes,
        "total_proxy_mb": round(total_proxy_bytes / 1024 / 1024, 3),
        "total_proxy_requests": total_proxy_requests,
        "cf_challenge_attempts": final_stats.get("cf_challenge_attempts", 0),
        "cf_challenges_passed": final_stats.get("cf_challenges_passed", 0),
        "cf_challenges_failed": final_stats.get("cf_challenges_failed", 0),
        "scraper_failures": final_stats.get("failures", 0),
        # Issue #616 — per-URL audit: top consumers + first/third-party split.
        "top_traffic_urls": final_stats.get("top_traffic_urls", []),
        "first_party_mb": final_stats.get("first_party_mb", 0.0),
        "third_party_mb": final_stats.get("third_party_mb", 0.0),
        "real_bytes_by_url": dict(final_stats.get("real_bytes_by_url", {}) or {}),
        "http_fetch_diag": final_stats.get("http_fetch_diag", []),
        "per_match": per_match,
    }

    Path(REPORT_PATH).write_text(json.dumps(report, indent=2, default=str))
    print("\n========= BENCH SUMMARY =========")
    print(json.dumps(
        {k: v for k, v in report.items() if k not in ("per_match", "http_fetch_diag")},
        indent=2, default=str,
    ))
    diag_count = len(report.get("http_fetch_diag", []))
    print(f"\nhttp_fetch_diag records: {diag_count} (see JSON for details)")
    print(f"Full report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
