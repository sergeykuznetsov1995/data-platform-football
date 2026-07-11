#!/usr/bin/env python3
"""Filtering passthrough proxy for FlareSolverr residential traffic (#652).

Chain: Chrome (FlareSolverr) -> THIS proxy -> residential upstream -> internet.

The only lever FlareSolverr (Chromium) leaves us to cut residential-proxy traffic is
domain-level blocking: we cannot MITM the TLS (that swaps Chrome's TLS fingerprint on
the proxy->Cloudflare leg and breaks the CF bypass), so this proxy does a pure TCP
tunnel for CONNECT and only inspects the CONNECT *host*. Hosts on the ad-tech blocklist
are refused (403, never dialed upstream) so their bytes are never billed; everything
else (the target site, Cloudflare challenge, fonts) tunnels through unchanged.

The observe run (no --blocklist) showed ~90% of WhoScored and ~60% of SoFIFA
residential bytes are third-party ad-tech — see docs/research/flaresolverr-proxy-traffic-audit.md.

Run inside airflow-webserver (reaches pool.proxys.io and the flaresolverr container):
    python scripts/proxy_filter/filter_proxy.py \
        --listen 0.0.0.0:8899 --blocklist configs/proxy_filter/blocklist.txt --out /tmp/filter_bytes.json
Point a FlareSolverr session at http://<this-host>:8899 (NO auth — this proxy holds the
residential creds). With no --blocklist it is a pure observe/counting proxy.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import signal
import sys
from collections import defaultdict
from urllib.parse import urlsplit

sys.path.insert(0, "/opt/airflow")

from scripts.proxy_filter.budget import (  # noqa: E402 - standalone script path above
    SharedBudgetLedger,
    load_verified_policy,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s filter_proxy: %(message)s")
log = logging.getLogger("filter_proxy")

# billable residential bytes, per target host (sum of both tunnel directions)
up_bytes: dict[str, int] = defaultdict(int)
down_bytes: dict[str, int] = defaultdict(int)
conn_count: dict[str, int] = defaultdict(int)
blocked_count: dict[str, int] = defaultdict(int)

BLOCKLIST: set[str] = set()
provider_budget_guard = None
provider_budget_endpoint: str | None = None

# Idle-refresh rotation state (#652). The residential exit is refreshed only when
# no tunnel is currently open (``_active == 0``), so one exit IP serves a whole
# FlareSolverr/CF session (the page and its Turnstile challenge share an IP =
# CF-safe) and each new session — which closes all tunnels first — draws a fresh
# exit. Picking per-CONNECT instead would split a page and its CF challenge across
# different IPs and re-trigger the challenge.
_current_up: tuple[str, int, str, str] | None = None
_active = 0


def _load_blocklist(path: str | None) -> set[str]:
    out: set[str] = set()
    if not path:
        return out
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                out.add(line.lower())
    return out


def _is_blocked(host: str) -> bool:
    h = host.lower()
    return any(h == b or h.endswith("." + b) for b in BLOCKLIST)


def _residential_manager(proxy_file: str):
    """A ProxyManager loaded from the custom-format pool file (host:port:user:pass).

    The upstream is refreshed per FlareSolverr session (idle-refresh, see
    ``_acquire_upstream``) so each session lands on a different residential exit —
    keeping IP diversity that a pin-one-proxy startup would have thrown away, while
    holding one exit per CF session so the page and its Turnstile challenge stay on
    a single IP (#652)."""
    from scrapers.utils.proxy_manager import ProxyManager

    mgr = ProxyManager(rotation_strategy="random")
    n = mgr.load_from_file_custom_format(proxy_file)
    if n <= 0:
        raise SystemExit(f"no proxies in {proxy_file}")
    return mgr


def _pick_upstream(mgr):
    """(host, port, user, pass) of one residential proxy from the pool."""
    u = urlsplit(mgr.get_proxy().url)  # http://user:pass@host:port
    return u.hostname, u.port, u.username, u.password


def _acquire_upstream(mgr):
    """Return the residential upstream for a new tunnel, refreshing it only when idle.

    A fresh random exit is drawn only when no tunnel is currently open
    (``_active == 0``): so all tunnels within one FlareSolverr session reuse the
    same exit IP (the page + its Turnstile challenge — CF-safe), and the next
    session (which destroys the tab, closing every tunnel through us) draws a new
    one. Callers MUST ``_active += 1`` right after and ``-= 1`` when the tunnel
    closes. Safe without a lock: asyncio is cooperative and this runs with no
    ``await`` between the check and the caller's increment."""
    global _current_up
    if _active == 0 or _current_up is None:
        _current_up = _pick_upstream(mgr)
        log.info("residential upstream → %s:%s (user=%s)", *_current_up[:3])
    return _current_up


async def _pump(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    host: str,
    counter: dict[str, int],
    budget_guard=None,
) -> None:
    try:
        while True:
            metered_read = getattr(budget_guard, "read_metered", None)
            precharged = callable(metered_read)
            if precharged:
                # Claim the provider window before reading. This is atomic
                # across both tunnel directions and therefore cannot race over
                # the final bytes of the logical-run budget.
                chunk = await metered_read(reader, 65536)
            else:
                chunk = await reader.read(65536)
            if not chunk:
                break
            # Charge the bytes at the real upstream-provider path before they
            # are forwarded.  When the measured reservation is exhausted the
            # tunnel closes here, rather than completing an over-budget retry.
            if budget_guard is not None and not precharged:
                budget_guard.consume(len(chunk))
            counter[host] += len(chunk)
            writer.write(chunk)
            await writer.drain()
    except Exception:  # noqa: BLE001 — proxy must never crash a flow
        pass
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


async def _read_headers(reader: asyncio.StreamReader) -> None:
    while True:
        h = await reader.readline()
        if h in (b"\r\n", b"\n", b""):
            return


async def handle(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter, mgr) -> None:
    global _active
    try:
        first = await client_r.readline()
        if not first:
            client_w.close()
            return
        parts = first.decode("latin1").split()
        if len(parts) < 2:
            client_w.close()
            return
        method, target = parts[0].upper(), parts[1]
        host = target.rsplit(":", 1)[0] if method == "CONNECT" else (urlsplit(target).hostname or target)

        if _is_blocked(host):
            blocked_count[host] += 1
            # refuse without dialing the residential upstream → bytes never billed
            client_w.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return

        # Acquire the residential exit (idle-refresh) and mark a tunnel open so the
        # exit stays pinned for the rest of this CF session. _active must wrap the
        # whole tunnel so the next session only refreshes once every tunnel closed.
        up_host, up_port, up_user, up_pass = _acquire_upstream(mgr)
        auth = base64.b64encode(f"{up_user}:{up_pass}".encode()).decode()
        _active += 1
        try:
            if method == "CONNECT":
                conn_count[host] += 1
                await _read_headers(client_r)
                srv_r, srv_w = await asyncio.open_connection(up_host, up_port)
                srv_w.write(
                    f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n"
                    f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
                )
                await srv_w.drain()
                status = await srv_r.readline()
                await _read_headers(srv_r)
                if b"200" not in status:
                    client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    await client_w.drain()
                    client_w.close()
                    srv_w.close()
                    return
                client_w.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await client_w.drain()
                await asyncio.gather(
                    _pump(client_r, srv_w, host, up_bytes, provider_budget_guard),
                    _pump(srv_r, client_w, host, down_bytes, provider_budget_guard),
                )
            else:
                conn_count[host] += 1
                srv_r, srv_w = await asyncio.open_connection(up_host, up_port)
                srv_w.write(first)
                srv_w.write(f"Proxy-Authorization: Basic {auth}\r\n".encode())
                await srv_w.drain()
                await asyncio.gather(
                    _pump(client_r, srv_w, host, up_bytes, provider_budget_guard),
                    _pump(srv_r, client_w, host, down_bytes, provider_budget_guard),
                )
        finally:
            _active -= 1
    except Exception:  # noqa: BLE001
        try:
            client_w.close()
        except Exception:  # noqa: BLE001
            pass


def _dump(out_path: str, quiet: bool = False) -> None:
    hosts = sorted(set(up_bytes) | set(down_bytes), key=lambda h: -(up_bytes[h] + down_bytes[h]))
    rows = [
        {
            "host": h,
            "conns": conn_count.get(h, 0),
            "down_mb": round(down_bytes[h] / 1048576, 3),
            "up_mb": round(up_bytes[h] / 1048576, 3),
            "total_mb": round((up_bytes[h] + down_bytes[h]) / 1048576, 3),
        }
        for h in hosts
    ]
    total = sum(up_bytes.values()) + sum(down_bytes.values())
    report = {
        "total_mb": round(total / 1048576, 3),
        "allowed_hosts": rows,
        "blocked_hosts": sorted(
            ({"host": h, "attempts": c} for h, c in blocked_count.items()),
            key=lambda r: -r["attempts"],
        ),
    }
    if provider_budget_guard is not None:
        # Exact bytes, not rounded MB or HTTP body length, are the canary input.
        report["total_provider_bytes"] = total
        report["endpoint_provider_bytes"] = {
            provider_budget_endpoint: total,
        }
        report["endpoint_request_provider_bytes"] = {
            provider_budget_endpoint: [total],
        }
    tmp = out_path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(report, fh, indent=2)
    os.replace(tmp, out_path)
    if not quiet:
        log.info("=== allowed (total %.2f MB) ===", total / 1048576)
        for r in rows[:30]:
            log.info("  %8.3f MB  (%dx)  %s", r["total_mb"], r["conns"], r["host"])
        log.info("=== blocked attempts: %d hosts ===", len(blocked_count))
        log.info("wrote %s", out_path)


async def _periodic_dump(out_path: str, interval: float = 2.0) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            _dump(out_path, quiet=True)
        except Exception:  # noqa: BLE001
            pass


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0:8899")
    ap.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    ap.add_argument("--blocklist", default=None, help="domain blocklist file (omit = observe only)")
    ap.add_argument("--out", default="/tmp/filter_bytes.json")
    ap.add_argument("--pidfile", default="/tmp/filter_proxy.pid")
    ap.add_argument("--budget-artifact")
    ap.add_argument("--budget-ledger")
    ap.add_argument("--budget-run-id")
    ap.add_argument("--budget-reservation-token")
    ap.add_argument("--budget-endpoint")
    args = ap.parse_args()

    with open(args.pidfile, "w") as fh:
        fh.write(str(os.getpid()))

    global BLOCKLIST, provider_budget_guard, provider_budget_endpoint
    BLOCKLIST = _load_blocklist(args.blocklist)
    log.info("blocklist: %d domains from %s", len(BLOCKLIST), args.blocklist or "(none — observe mode)")

    mgr = _residential_manager(args.proxy_file)
    log.info("residential pool = %d proxies (idle-refresh per FS session)", mgr.total_count)
    host, port = args.listen.rsplit(":", 1)

    budget_args = (
        args.budget_artifact,
        args.budget_ledger,
        args.budget_run_id,
        args.budget_reservation_token,
        args.budget_endpoint,
    )
    if any(budget_args) and not all(budget_args):
        raise SystemExit("all --budget-* arguments are required together")
    if all(budget_args):
        policy = load_verified_policy(args.budget_artifact)
        ledger = SharedBudgetLedger(args.budget_ledger, policy)

        class _Guard:
            async def read_metered(
                self,
                reader: asyncio.StreamReader,
                max_bytes: int,
            ) -> bytes:
                claimed = ledger.claim(
                    args.budget_run_id,
                    args.budget_reservation_token,
                    max_bytes,
                )
                try:
                    chunk = await reader.read(claimed)
                except BaseException:
                    ledger.refund(
                        args.budget_run_id,
                        args.budget_reservation_token,
                        claimed,
                    )
                    raise
                unused = claimed - len(chunk)
                if unused:
                    ledger.refund(
                        args.budget_run_id,
                        args.budget_reservation_token,
                        unused,
                    )
                return chunk

            def consume(self, amount: int) -> None:
                ledger.consume(
                    args.budget_run_id,
                    args.budget_reservation_token,
                    amount,
                )

        provider_budget_guard = _Guard()
        provider_budget_endpoint = args.budget_endpoint
        log.info(
            "hard provider budget active run=%s endpoint=%s artifact=%s",
            args.budget_run_id,
            args.budget_endpoint,
            policy.artifact_id,
        )

    server = await asyncio.start_server(lambda r, w: handle(r, w, mgr), host, int(port))
    log.info("listening on %s (no auth — point FlareSolverr proxy here)", args.listen)

    asyncio.ensure_future(_periodic_dump(args.out))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async with server:
        await stop.wait()
    _dump(args.out)


if __name__ == "__main__":
    asyncio.run(main())
