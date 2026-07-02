"""
Bench: per-match FlareSolverr proxy-traffic for WhoScored events / SoFIFA
player ratings (issue #616).

WhoScored + SoFIFA run on FlareSolverr / Camoufox, which exposes no CDP Network
events, so the in-process counter (``FlareSolverrClient.get_traffic_stats``)
reports ``fs_response_*`` — the rendered HTML FlareSolverr returns to us. That
is a LOWER BOUND on the residential-proxy bytes, NOT the proxy MB itself:
Camoufox downloads images / CSS / JS / XHR through the proxy and returns only
the rendered HTML. The dominant cost driver is ``sessions_created`` — each new
FlareSolverr session re-solves the Cloudflare challenge (a cold-start).

The TRUE per-match proxy MB is measured at the container level on the VM by
bracketing this run with the FlareSolverr container's network counters::

    docker exec flaresolverr cat /proc/net/dev          # before (sum RX+TX)
    <this script>
    docker exec flaresolverr cat /proc/net/dev          # after  → ΔRX+TX
    true_proxy_bytes ≈ Δcontainer − fs_response_bytes    # subtract the FS→airflow leg

See ``docs/research/flaresolverr-proxy-traffic-audit.md`` for the full method.

Runs a FIXED, SEQUENTIAL set (parallel sessions poison per-match attribution)
with NO Iceberg writes, then dumps ``get_traffic_stats()`` + per-match timings.

Run (inside the airflow container, on the VM):
  docker exec -e BENCH_LABEL=baseline airflow-webserver bash -c \\
    'cd /opt/airflow && python scripts/research/bench_flaresolverr_fetch.py --source whoscored'
  docker exec -e BENCH_LABEL=baseline airflow-webserver bash -c \\
    'cd /opt/airflow && python scripts/research/bench_flaresolverr_fetch.py --source whoscored-schedule'
  docker exec -e BENCH_LABEL=baseline airflow-webserver bash -c \\
    'cd /opt/airflow && python scripts/research/bench_flaresolverr_fetch.py --source sofifa'

Override the fixed set (else WhoScored pulls game_ids from
``bronze.whoscored_schedule`` and SoFIFA takes the first N players of the edition):
  BENCH_WS_MATCH_IDS="1903158,1903159,..."     # WhoScored game_ids
  BENCH_SOFIFA_PLAYER_IDS="158023,20801,..."   # SoFIFA player ids

Output: /tmp/bench_<source>_<label>.json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
import time
import uuid
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bench_fs")

sys.path.insert(0, "/opt/airflow")


def _pick_proxy_url(proxy_file: str):
    """Build a ProxyManager from the proxy file and return (url, manager).

    Mirrors ``BaseScraper`` proxy wiring (random rotation). Returns (None, mgr)
    when the file is empty / missing so the bench still runs proxy-less locally.
    """
    from scrapers.utils.proxy_manager import ProxyManager

    mgr = ProxyManager(rotation_strategy="random")
    try:
        count = mgr.load_from_file_custom_format(proxy_file)
    except Exception as e:  # noqa: BLE001
        log.warning(f"proxy file {proxy_file} unreadable ({e}); running proxy-less")
        return None, mgr
    if count <= 0:
        log.warning(f"no proxies in {proxy_file}; running proxy-less")
        return None, mgr
    proxy = mgr.get_proxy()
    return (proxy.url if proxy else None), mgr


def _bench_whoscored(n: int, proxy_file: str, fs_url: str, league: str, season: int):
    """Fetch N WhoScored matchCentreData pages sequentially (no Iceberg write)."""
    from scrapers.base.flaresolverr_client import FlareSolverrClient, FlareSolverrError
    from scrapers.whoscored.events_fetcher import fetch_match_events_via_flaresolverr
    from scrapers.whoscored.scraper import WhoScoredScraper

    proxy_url, mgr = _pick_proxy_url(proxy_file)

    env_ids = os.environ.get("BENCH_WS_MATCH_IDS", "").strip()
    if env_ids:
        ids = [int(x) for x in env_ids.split(",") if x.strip()][:n]
    else:
        # Pull a fixed set from the already-populated bronze schedule.
        scraper = WhoScoredScraper(
            leagues=[league], seasons=[season],
            proxy_file=proxy_file, flaresolverr_url=fs_url,
        )
        # The scraper filters by its configured seasons=[season] itself; the
        # old target_season parameter was removed with the all-seasons change.
        meta = scraper._read_events_metadata_from_bronze()
        ids = [m[0] for m in meta][:n]
    if not ids:
        raise SystemExit(
            "WhoScored bench: no game_ids — set BENCH_WS_MATCH_IDS or populate "
            "bronze.whoscored_schedule first."
        )

    recreate_every = WhoScoredScraper.EVENTS_SESSION_RECREATE_EVERY
    client = FlareSolverrClient(url=fs_url)
    session_id = f"bench-ws-{uuid.uuid4().hex[:8]}"
    client.create_session(session_id, proxy_url=proxy_url)

    def _recycle():
        nonlocal session_id, proxy_url
        try:
            client.destroy_session(session_id)
        except FlareSolverrError:
            pass
        if mgr.total_count > 0:
            p = mgr.get_proxy()
            proxy_url = p.url if p else proxy_url
        session_id = f"bench-ws-{uuid.uuid4().hex[:8]}"
        client.create_session(session_id, proxy_url=proxy_url)

    per_match = []
    prev_bytes = 0
    for i, mid in enumerate(ids, 1):
        if i > 1 and (i - 1) % recreate_every == 0:
            log.info(f"  recycling FS session at match {i}/{len(ids)}")
            _recycle()
        t0 = time.monotonic()
        ok = False
        err = None
        for attempt in range(2):  # one reactive recreate+retry, like production
            try:
                data = fetch_match_events_via_flaresolverr(client, mid, session_id)
                ok = bool(data and "events" in data)
                break
            except Exception as e:  # noqa: BLE001
                err = f"{type(e).__name__}: {e}"
                if attempt == 0:
                    log.warning(f"  match {mid} failed ({err}); recycle+retry")
                    _recycle()
        elapsed = round(time.monotonic() - t0, 2)
        cur = client._fs_response_bytes
        per_match.append({
            "i": i, "match_id": mid, "seconds": elapsed,
            "html_bytes": cur - prev_bytes, "success": ok,
            **({"error": err} if (err and not ok) else {}),
        })
        prev_bytes = cur
        log.info(
            f"  [{i:2d}/{len(ids)}] {elapsed:6.2f}s "
            f"html_bytes={per_match[-1]['html_bytes']:>8,} ok={ok}"
        )

    try:
        client.destroy_session(session_id)
    except FlareSolverrError:
        pass

    succeeded = sum(1 for m in per_match if m["success"])
    return client.get_traffic_stats(), per_match, len(ids), succeeded


def _bench_whoscored_schedule(proxy_file: str, fs_url: str, league: str, season: int):
    """Fetch the WhoScored schedule once (no Iceberg write).

    Schedule is a once-per-(league, season) operation, not per-match, so this
    runs a single ``read_schedule()`` through the same FlareSolverr reader the
    production schedule path uses (``SESSION_RECREATE_EVERY=8``). Mirrors
    ``_bench_sofifa``: build the reader directly with the picked proxy and read
    its ``_fs_client`` traffic stats — ``_save()`` / Iceberg is deliberately
    skipped so the bench measures fetch traffic only.
    """
    from scrapers.whoscored.flaresolverr_reader import FlareSolverrWhoScoredReader

    proxy_url, _ = _pick_proxy_url(proxy_file)
    reader = FlareSolverrWhoScoredReader(
        flaresolverr_url=fs_url, proxy=proxy_url,
        leagues=[league], seasons=[season],
    )
    try:
        t0 = time.monotonic()
        df = reader.read_schedule()
        elapsed = round(time.monotonic() - t0, 2)
        rows = 0 if df is None else len(df)
        stats = reader._fs_client.get_traffic_stats()
        per_match = [{"i": 1, "rows": rows, "seconds": elapsed, "success": rows > 0}]
        log.info(f"  schedule rows={rows} in {elapsed:.2f}s")
        return stats, per_match, 1, (1 if rows > 0 else 0)
    finally:
        reader.close()


def _bench_sofifa(n: int, proxy_file: str, fs_url: str, league: str, versions: str):
    """Fetch N SoFIFA player-rating pages sequentially (no Iceberg write)."""
    from scrapers.sofifa.flaresolverr_reader import FlareSolverrSoFIFAReader

    proxy_url, _ = _pick_proxy_url(proxy_file)
    reader = FlareSolverrSoFIFAReader(
        flaresolverr_url=fs_url, proxy=proxy_url,
        versions=versions, leagues=[league],
    )
    try:
        env_ids = os.environ.get("BENCH_SOFIFA_PLAYER_IDS", "").strip()
        if env_ids:
            ids = [int(x) for x in env_ids.split(",") if x.strip()][:n]
        else:
            # First N players of the edition (read_players itself fetches the
            # league/team listing pages — that traffic is part of the cost).
            players = reader.read_players().index.unique().tolist()
            ids = [int(p) for p in players][:n]
        if not ids:
            raise SystemExit("SoFIFA bench: no player ids resolved.")

        t0 = time.monotonic()
        reader.read_player_ratings(player=ids)
        elapsed = round(time.monotonic() - t0, 2)

        stats = reader._fs_client.get_traffic_stats()
        # One summary row — per-page detail lives in stats['top_traffic_urls'].
        per_match = [{
            "player_ids": ids, "count": len(ids), "seconds": elapsed,
            "success": True,
        }]
        return stats, per_match, len(ids), len(ids)
    finally:
        reader.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Bench FlareSolverr proxy traffic")
    ap.add_argument(
        "--source",
        choices=["whoscored", "whoscored-schedule", "sofifa"],
        required=True,
    )
    ap.add_argument("--n", type=int, default=10, help="Number of pages to fetch")
    ap.add_argument("--league", default="ENG-Premier League")
    ap.add_argument("--season", type=int, default=2025, help="[whoscored] season start year")
    ap.add_argument("--versions", default="latest", help="[sofifa] FIFA version(s)")
    ap.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    ap.add_argument(
        "--flaresolverr-url",
        default=os.environ.get("FLARESOLVERR_URL", "http://flaresolverr:8191"),
    )
    args = ap.parse_args()

    label = os.environ.get("BENCH_LABEL", "unlabeled")
    report_path = f"/tmp/bench_{args.source.replace('-', '_')}_{label}.json"

    log.info("=" * 64)
    log.info(
        f"BENCH START source={args.source} label={label} n={args.n} "
        f"@ {time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    log.info(
        "For TRUE proxy MB, bracket this run with "
        "`docker exec flaresolverr cat /proc/net/dev` (ΔRX+TX)."
    )
    log.info("=" * 64)

    t0 = time.monotonic()
    if args.source == "whoscored":
        stats, per_match, attempted, succeeded = _bench_whoscored(
            args.n, args.proxy_file, args.flaresolverr_url, args.league, args.season
        )
    elif args.source == "whoscored-schedule":
        stats, per_match, attempted, succeeded = _bench_whoscored_schedule(
            args.proxy_file, args.flaresolverr_url, args.league, args.season
        )
    else:
        stats, per_match, attempted, succeeded = _bench_sofifa(
            args.n, args.proxy_file, args.flaresolverr_url, args.league, args.versions
        )
    total = round(time.monotonic() - t0, 2)

    durations = [m["seconds"] for m in per_match if m.get("success")]
    report = {
        "label": label,
        "source": args.source,
        "matches_attempted": attempted,
        "matches_succeeded": succeeded,
        "success_rate": round(succeeded / attempted, 2) if attempted else None,
        "total_seconds": total,
        "mean_seconds": round(statistics.mean(durations), 2) if durations else None,
        # In-process audit (LOWER BOUND — see docstring). NOT the proxy MB.
        "fs_response_mb": stats.get("fs_response_mb", 0.0),
        "fs_response_bytes": stats.get("fs_response_bytes", 0),
        "requests": stats.get("requests", 0),
        "sessions_created": stats.get("sessions_created", 0),
        "cf_challenge_failures": stats.get("cf_challenge_failures", 0),
        "top_traffic_urls": stats.get("top_traffic_urls", []),
        "per_match": per_match,
    }

    Path(report_path).write_text(json.dumps(report, indent=2, default=str))
    log.info("=" * 64)
    log.info(f"BENCH END @ {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n========= BENCH SUMMARY =========")
    print(json.dumps(
        {k: v for k, v in report.items() if k not in ("per_match", "top_traffic_urls")},
        indent=2, default=str,
    ))
    print(
        "\nNOTE: fs_response_mb is HTML returned to us — a LOWER BOUND, NOT the "
        "residential-proxy MB. Use the docker /proc/net/dev delta for the true "
        "number (see docstring)."
    )
    print(f"Full report: {report_path}")


if __name__ == "__main__":
    main()
