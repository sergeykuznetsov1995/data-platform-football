"""Fetch one FBref page through the production FBrefFetcher (not the raw transport).

Diagnostic for the wave that hangs under Airflow: same fetcher, same default
browser caps, but a clean process. Prints the signal disposition too, because a
forked Airflow worker can hand its subprocess a SIGCHLD it never expected.
"""
import signal
import sys
import time

sys.path.insert(0, "/opt/airflow")

from scrapers.fbref.fetcher import FBrefFetcher  # noqa: E402

print("SIGCHLD:", signal.getsignal(signal.SIGCHLD), flush=True)

t0 = time.time()
with FBrefFetcher(proxy_file="/opt/airflow/proxys.txt") as fetcher:
    response = fetcher.fetch(
        "https://fbref.com/en/comps/", page_kind="competition_index"
    )
    print(
        f"fetched in {time.time() - t0:.1f}s: status={response.status} "
        f"bytes={len(response.body)}",
        flush=True,
    )
    stats = getattr(fetcher, "_bootstrap_stats", None) or {}
    for key in sorted(stats):
        print(f"  {key}: {stats[key]}", flush=True)
