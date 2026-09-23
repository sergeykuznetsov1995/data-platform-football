#!/usr/bin/env python3
"""Read-only latency probe for a SofaScore gateway control channel (#1350).

Runs inside a gateway container (stdlib only) while a real batch is leasing:

    docker exec -i sofascore_gw_history python3 - \
        --base-url http://127.0.0.1:8899 --seconds 60 --rate 5 \
        [--stats-lease-id <lease id>] < deploy/sofascore/gateway_probe_load.py

It sends ``GET /health`` and, when a lease id is given,
``GET /v1/leases/<id>/stats`` at ``--rate`` requests per second each — every
endpoint from its own thread on its own schedule, so a stalled answer on one
route does not hide the stall from the other — and prints one JSON document
with ``count, p50_ms, p95_ms, max_ms, errors, timeouts`` per endpoint.  No
request starts after ``--seconds``; the run ends at most one request timeout
(5 s) later.  The control token is taken from the environment
(``PROXY_FILTER_CONTROL_TOKEN``, the gateway's own variable) and is never
printed.  The lease bearer token is private to the task holding the lease, so
without ``SOFASCORE_PROBE_LEASE_TOKEN`` the stats route answers 401 after the
control-token check, from the same event loop — the loop stall this probe
measures; the authorized handler is covered by the clients' ``ReadTimeout``
count.  Status codes are reported in ``status_counts``.  Nothing is mutated:
both routes are GET and idempotent.
"""

from __future__ import annotations

import argparse
import http.client
import json
import math
import os
import socket
import sys
import threading
import time
from typing import Iterable, Mapping, Optional, Sequence
from urllib.parse import quote, urlsplit

REQUEST_TIMEOUT_SECONDS = 5.0


def percentile(values: Sequence[float], fraction: float) -> Optional[float]:
    """Nearest-rank percentile; ``None`` for an empty sample."""

    if not values:
        return None
    if not 0.0 < fraction <= 1.0:
        raise ValueError("fraction must be in (0, 1]")
    ordered = sorted(values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[rank - 1]


def summarize(
    latencies_ms: Sequence[float],
    *,
    errors: int,
    timeouts: int,
    status_counts: Mapping[str, int],
) -> dict[str, object]:
    def rounded(value: Optional[float]) -> Optional[float]:
        return None if value is None else round(value, 1)

    return {
        "count": len(latencies_ms) + timeouts,
        "p50_ms": rounded(percentile(latencies_ms, 0.50)),
        "p95_ms": rounded(percentile(latencies_ms, 0.95)),
        "max_ms": rounded(max(latencies_ms) if latencies_ms else None),
        "errors": errors,
        "timeouts": timeouts,
        "status_counts": dict(sorted(status_counts.items())),
    }


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--stats-lease-id", default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    split = urlsplit(args.base_url)
    if split.scheme != "http" or not split.hostname:
        parser.error("--base-url must be http://host[:port]")
    if args.seconds <= 0 or args.rate <= 0:
        parser.error("--seconds and --rate must be positive")
    return args


class _Endpoint:
    def __init__(self, name: str, path: str, headers: Mapping[str, str]) -> None:
        self.name = name
        self.path = path
        self.headers = dict(headers)
        self.latencies_ms: list[float] = []
        self.errors = 0
        self.timeouts = 0
        self.status_counts: dict[str, int] = {}

    def hit(self, host: str, port: int) -> None:
        started = time.monotonic()
        connection = http.client.HTTPConnection(
            host, port, timeout=REQUEST_TIMEOUT_SECONDS
        )
        try:
            connection.request("GET", self.path, headers=self.headers)
            response = connection.getresponse()
            response.read()
            status = str(response.status)
        except (socket.timeout, TimeoutError):
            self.timeouts += 1
            return
        except (OSError, http.client.HTTPException):
            self.errors += 1
            return
        finally:
            connection.close()
        self.latencies_ms.append((time.monotonic() - started) * 1000.0)
        self.status_counts[status] = self.status_counts.get(status, 0) + 1
        if status.startswith("5"):
            self.errors += 1

    def report(self) -> dict[str, object]:
        return summarize(
            self.latencies_ms,
            errors=self.errors,
            timeouts=self.timeouts,
            status_counts=self.status_counts,
        )


def run(args: argparse.Namespace) -> dict[str, object]:
    split = urlsplit(args.base_url)
    host, port = split.hostname or "", split.port or 80
    endpoints = [_Endpoint("health", "/health", {})]
    if args.stats_lease_id:
        headers = {
            "X-Proxy-Control-Token": os.environ.get("PROXY_FILTER_CONTROL_TOKEN", "")
        }
        bearer = os.environ.get("SOFASCORE_PROBE_LEASE_TOKEN", "")
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        endpoints.append(
            _Endpoint(
                "stats",
                f"/v1/leases/{quote(args.stats_lease_id, safe='')}/stats",
                headers,
            )
        )
    interval = 1.0 / args.rate
    deadline = time.monotonic() + args.seconds

    def drive(endpoint: _Endpoint) -> None:
        next_at = time.monotonic()
        while True:
            now = time.monotonic()
            if now >= deadline:
                return
            if next_at > now:
                time.sleep(min(next_at, deadline) - now)
                continue
            endpoint.hit(host, port)
            # A slow answer skips the missed slots instead of bursting.
            next_at = max(next_at + interval, time.monotonic())

    threads = [
        threading.Thread(target=drive, args=(endpoint,), daemon=True)
        for endpoint in endpoints
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return {
        "base_url": args.base_url,
        "seconds": args.seconds,
        "rate_per_endpoint": args.rate,
        "endpoints": {endpoint.name: endpoint.report() for endpoint in endpoints},
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    report = run(parse_args(argv))
    json.dump(report, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
