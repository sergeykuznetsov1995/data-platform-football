"""Bounded benchmark for the standalone FBref production fetcher.

Runs inside the Airflow image against ten fixed APL 2025/26 match URLs. One
``FBrefFetcher`` owns the Camoufox clearance lease and all target pages use its
warm HTTP session. The report keeps the historical JSON keys so old benchmark
comparisons remain readable, and adds exact browser-document, browser-asset,
HTTP-wire, decoded-HTML, and provider-billing fields.

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
from typing import Any

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


def _p95(values: list[float]) -> float | None:
    if len(values) < 3:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * 0.95))
    return round(ordered[index], 2)


def _empty_totals() -> dict[str, int]:
    return {
        "browser_document_bytes": 0,
        "browser_asset_bytes": 0,
        "browser_requests": 0,
        "http_wire_bytes": 0,
        "http_requests": 0,
        "decoded_html_bytes": 0,
        "provider_billed_bytes": 0,
        "provider_billed_observations": 0,
        "http_fetch_ok": 0,
        "failures": 0,
    }


def _add_traffic(
    totals: dict[str, int],
    *,
    browser_document_bytes: int = 0,
    browser_asset_bytes: int = 0,
    browser_requests: int = 0,
    http_wire_bytes: int = 0,
    http_requests: int = 0,
    decoded_html_bytes: int = 0,
    provider_billed_bytes: int | None = None,
) -> None:
    totals["browser_document_bytes"] += int(browser_document_bytes or 0)
    totals["browser_asset_bytes"] += int(browser_asset_bytes or 0)
    totals["browser_requests"] += int(browser_requests or 0)
    totals["http_wire_bytes"] += int(http_wire_bytes or 0)
    totals["http_requests"] += int(http_requests or 0)
    totals["decoded_html_bytes"] += int(decoded_html_bytes or 0)
    if provider_billed_bytes is not None:
        totals["provider_billed_bytes"] += int(provider_billed_bytes)
        totals["provider_billed_observations"] += 1


def main() -> None:
    from scrapers.fbref.fetcher import (
        FETCHER_VERSION,
        FBrefFetcher,
        FetchError,
    )

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
    http_fetch_diag: list[dict[str, Any]] = []
    totals = _empty_totals()
    bench_t0 = time.monotonic()

    with FBrefFetcher(proxy_file=PROXY_FILE or None) as fetcher:
        for i, path in enumerate(match_paths, 1):
            url = "https://fbref.com" + path
            t0 = time.monotonic()
            body = b""
            err = None
            error_class = None
            response = None
            try:
                response = fetcher.fetch(url, page_kind="match")
                body = response.body
                _add_traffic(
                    totals,
                    browser_document_bytes=response.browser_document_bytes,
                    browser_asset_bytes=response.browser_asset_bytes,
                    browser_requests=response.browser_requests,
                    http_wire_bytes=response.http_wire_bytes,
                    http_requests=response.http_requests,
                    decoded_html_bytes=response.decoded_html_bytes,
                    provider_billed_bytes=response.provider_billed_bytes,
                )
                totals["http_fetch_ok"] += 1
            except FetchError as exc:
                err = f"{type(exc).__name__}: {exc}"
                error_class = exc.error_class
                _add_traffic(
                    totals,
                    browser_document_bytes=exc.browser_document_bytes,
                    browser_asset_bytes=exc.browser_asset_bytes,
                    browser_requests=exc.browser_requests,
                    http_wire_bytes=exc.wire_bytes,
                    http_requests=exc.target_requests,
                    provider_billed_bytes=exc.provider_billed_bytes,
                )
                totals["failures"] += 1
                http_fetch_diag.append({
                    "path": path,
                    "error_class": exc.error_class,
                    "http_status": exc.http_status,
                    "wire_bytes": exc.wire_bytes,
                })
            except Exception as exc:  # noqa: BLE001
                err = f"{type(exc).__name__}: {exc}"
                error_class = type(exc).__name__
                totals["failures"] += 1
                http_fetch_diag.append({
                    "path": path,
                    "error_class": error_class,
                })
            elapsed = round(time.monotonic() - t0, 2)
            success = len(body) > 50000
            entry = {
                "i": i,
                "path": path,
                "seconds": elapsed,
                "bytes": len(body),
                "success": success,
                "http_fetch_ok_cumul": totals["http_fetch_ok"],
                # The standalone path has no target-page browser fallback.
                "http_fetch_fallback_cumul": 0,
                "successes_cumul": totals["http_fetch_ok"],
                "failures_cumul": totals["failures"],
                "browser_document_bytes": (
                    0 if response is None else response.browser_document_bytes
                ),
                "browser_asset_bytes": (
                    0 if response is None else response.browser_asset_bytes
                ),
                "browser_requests": (
                    0 if response is None else response.browser_requests
                ),
                "http_wire_bytes": (
                    0 if response is None else response.http_wire_bytes
                ),
                "http_requests": (
                    0 if response is None else response.http_requests
                ),
                "decoded_html_bytes": (
                    0 if response is None else response.decoded_html_bytes
                ),
                "provider_billed_bytes": (
                    None if response is None else response.provider_billed_bytes
                ),
                "http_latency_ms": (
                    None if response is None else response.latency_ms
                ),
            }
            if err:
                entry["error"] = err
                entry["error_class"] = error_class
            per_match.append(entry)
            if html_dir is not None and body:
                match_id = path.split("/", 4)[3]
                with gzip.open(html_dir / f"{match_id}.html.gz", "wb") as fh:
                    fh.write(body)
            log.info(
                f"  [{i:2d}/{len(match_paths)}] {elapsed:6.2f}s "
                f"bytes={entry['bytes']:>7,} success={entry['success']} "
                f"http_ok={entry['http_fetch_ok_cumul']} "
                f"http_fb={entry['http_fetch_fallback_cumul']}"
            )

    bench_total = round(time.monotonic() - bench_t0, 2)
    durations = [m["seconds"] for m in per_match if m["success"]]
    browser_document_bytes = totals["browser_document_bytes"]
    browser_asset_bytes = totals["browser_asset_bytes"]
    browser_bytes = browser_document_bytes + browser_asset_bytes
    browser_requests = totals["browser_requests"]
    http_bytes = totals["http_wire_bytes"]
    http_requests = totals["http_requests"]
    total_proxy_bytes = browser_bytes + http_bytes
    total_proxy_requests = browser_requests + http_requests
    provider_billed_bytes = (
        totals["provider_billed_bytes"]
        if totals["provider_billed_observations"]
        else None
    )
    matches_succeeded = sum(1 for match in per_match if match["success"])
    report = {
        "label": LABEL,
        "transport": FETCHER_VERSION,
        "matches_attempted": len(match_paths),
        "matches_succeeded": matches_succeeded,
        "total_seconds": bench_total,
        "mean_seconds": round(statistics.mean(durations), 2) if durations else None,
        "p50_seconds": round(statistics.median(durations), 2) if durations else None,
        "p95_seconds": _p95(durations),
        "success_rate": round(matches_succeeded / len(match_paths), 2),
        "http_fetch_ok_total": totals["http_fetch_ok"],
        "http_fetch_fallback_total": 0,
        "real_bytes_mb": round(browser_bytes / 1024 / 1024, 3),
        "real_requests": browser_requests,
        "http_bytes_mb": round(http_bytes / 1024 / 1024, 3),
        "http_requests": http_requests,
        "browser_document_bytes": browser_document_bytes,
        "browser_document_mb": round(
            browser_document_bytes / 1024 / 1024, 3
        ),
        "browser_asset_bytes": browser_asset_bytes,
        "browser_asset_mb": round(browser_asset_bytes / 1024 / 1024, 3),
        "http_wire_bytes": http_bytes,
        "decoded_html_bytes": totals["decoded_html_bytes"],
        "provider_billed_bytes": provider_billed_bytes,
        "provider_billed_mb": (
            None
            if provider_billed_bytes is None
            else round(provider_billed_bytes / 1024 / 1024, 3)
        ),
        "provider_billed_observations": totals[
            "provider_billed_observations"
        ],
        "total_proxy_bytes": total_proxy_bytes,
        "total_proxy_mb": round(total_proxy_bytes / 1024 / 1024, 3),
        "total_proxy_requests": total_proxy_requests,
        # FetchResponse intentionally does not infer challenge counters or host
        # attribution from aggregate browser traffic.
        "cf_challenge_attempts": None,
        "cf_challenges_passed": None,
        "cf_challenges_failed": None,
        "scraper_failures": totals["failures"],
        "top_traffic_urls": [],
        "first_party_mb": 0.0,
        "third_party_mb": 0.0,
        "real_bytes_by_url": {},
        "host_attribution_available": False,
        "http_fetch_diag": http_fetch_diag,
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
