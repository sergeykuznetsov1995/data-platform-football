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
import argparse
import asyncio
import base64
import json
import logging
import os
import secrets
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

sys.path.insert(0, "/opt/airflow")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s filter_proxy: %(message)s")
log = logging.getLogger("filter_proxy")

# billable residential bytes, per target host (sum of both tunnel directions)
up_bytes: dict[str, int] = defaultdict(int)
down_bytes: dict[str, int] = defaultdict(int)
conn_count: dict[str, int] = defaultdict(int)
blocked_count: dict[str, int] = defaultdict(int)

BLOCKLIST: set[str] = set()

# The explicit lease path is used by WhoScored.  A lease pins exactly one pool
# entry, has a one-time bearer token, and is accounted independently.  This
# replaces the unreliable ``_active == 0`` proxy selection heuristic for all
# callers which authenticate as ``lease:<token>``.  Credential-less clients are
# still accepted through the legacy path so existing SoFIFA deployments are not
# broken during migration.
DEFAULT_LEASE_BYTES = 8 * 1024 * 1024
MAX_LEASE_BYTES = 24 * 1024 * 1024
DEFAULT_LEASE_TTL_SECONDS = 60
MAX_LEASE_TTL_SECONDS = 300
DAILY_BUDGET_BYTES = 100 * 1024 * 1024
LEASE_PROXY_URL = "http://proxy_filter:8900"


@dataclass
class Lease:
    lease_id: str
    token: str
    upstream: tuple[str, int, str, str]
    created_at: float
    expires_at: float
    max_bytes: int
    up_bytes: int = 0
    down_bytes: int = 0
    reserved_bytes: int = 0
    active_tunnels: int = 0
    closed: bool = False
    budget_exceeded: bool = False
    hosts: dict[str, dict[str, int]] = field(default_factory=dict)
    tunnel_writers: set[Any] = field(default_factory=set, repr=False)

    @property
    def total_bytes(self) -> int:
        return self.up_bytes + self.down_bytes

    @property
    def expired(self) -> bool:
        return time.time() >= self.expires_at

    @property
    def usable(self) -> bool:
        return not self.closed and not self.expired and not self.budget_exceeded

    def report(self) -> dict[str, Any]:
        return {
            "id": self.lease_id,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "max_bytes": self.max_bytes,
            "up_bytes": self.up_bytes,
            "down_bytes": self.down_bytes,
            "total_bytes": self.total_bytes,
            "active_tunnels": self.active_tunnels,
            "closed": self.closed,
            "expired": self.expired,
            "budget_exceeded": self.budget_exceeded,
            "hosts": self.hosts,
        }


LEASES: dict[str, Lease] = {}
LEASE_TOKENS: dict[str, str] = {}
_daily_day = ""
_daily_up_bytes = 0
_daily_down_bytes = 0
_daily_reserved_bytes = 0

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


def _utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _refresh_daily_counter() -> None:
    global _daily_day, _daily_up_bytes, _daily_down_bytes, _daily_reserved_bytes
    day = _utc_day()
    if day != _daily_day:
        _daily_day = day
        _daily_up_bytes = 0
        _daily_down_bytes = 0
        _daily_reserved_bytes = 0


def _daily_total_bytes() -> int:
    _refresh_daily_counter()
    return _daily_up_bytes + _daily_down_bytes


def _restore_daily_counter(out_path: str) -> None:
    """Restore today's hard budget after a proxy-filter restart.

    Active leases/tunnels cannot survive a process restart, but billed bytes
    already consumed today must.  The periodic report is atomically replaced,
    so accepting it as the checkpoint cannot read a half-written value.
    """
    global _daily_day, _daily_up_bytes, _daily_down_bytes, _daily_reserved_bytes
    try:
        with open(out_path, encoding="utf-8") as fh:
            daily = (json.load(fh) or {}).get("daily") or {}
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError):
        return
    if daily.get("day") != _utc_day():
        return
    _daily_day = str(daily["day"])
    _daily_up_bytes = max(0, int(daily.get("up_bytes", 0)))
    _daily_down_bytes = max(0, int(daily.get("down_bytes", 0)))
    if not (_daily_up_bytes or _daily_down_bytes):
        # Compatibility with reports written before directional counters were
        # added. Treat all historical bytes as downstream; only the sum gates.
        _daily_down_bytes = max(0, int(daily.get("total_bytes", 0)))
    _daily_reserved_bytes = 0


def _create_lease(mgr, *, max_bytes: int, ttl_seconds: int) -> Lease:
    """Create one explicit, byte-bounded sticky residential lease."""
    _refresh_daily_counter()
    if max_bytes <= 0 or max_bytes > MAX_LEASE_BYTES:
        raise ValueError(f"max_bytes must be in 1..{MAX_LEASE_BYTES}")
    if ttl_seconds <= 0 or ttl_seconds > MAX_LEASE_TTL_SECONDS:
        raise ValueError(f"ttl_seconds must be in 1..{MAX_LEASE_TTL_SECONDS}")
    if _daily_total_bytes() >= DAILY_BUDGET_BYTES:
        raise RuntimeError("daily paid-proxy budget exhausted")
    now = time.time()
    lease = Lease(
        lease_id=uuid_hex(12),
        token=secrets.token_urlsafe(24),
        upstream=_pick_upstream(mgr),
        created_at=now,
        expires_at=now + ttl_seconds,
        max_bytes=min(max_bytes, DAILY_BUDGET_BYTES - _daily_total_bytes()),
    )
    LEASES[lease.lease_id] = lease
    LEASE_TOKENS[lease.token] = lease.lease_id
    log.info(
        "lease %s created: upstream=%s:%s max_bytes=%d ttl=%ds",
        lease.lease_id,
        lease.upstream[0],
        lease.upstream[1],
        lease.max_bytes,
        ttl_seconds,
    )
    return lease


def uuid_hex(length: int) -> str:
    """Random identifier helper kept separate for deterministic unit tests."""
    return secrets.token_hex((length + 1) // 2)[:length]


def _lease_from_proxy_authorization(value: str | None) -> Lease | None:
    if not value or not value.lower().startswith("basic "):
        return None
    try:
        decoded = base64.b64decode(value.split(None, 1)[1]).decode("utf-8")
        username, token = decoded.split(":", 1)
    except (ValueError, UnicodeDecodeError):
        return None
    if username != "lease":
        return None
    lease_id = LEASE_TOKENS.get(token)
    return LEASES.get(lease_id or "")


def _authorized_control_lease(lease_id: str, authorization: str | None) -> Lease | None:
    lease = LEASES.get(lease_id)
    if lease is None or not authorization or not authorization.lower().startswith("bearer "):
        return None
    token = authorization.split(None, 1)[1]
    return lease if secrets.compare_digest(token, lease.token) else None


def _lease_remaining(lease: Lease) -> int:
    if not lease.usable:
        return 0
    daily_remaining = max(
        0, DAILY_BUDGET_BYTES - _daily_total_bytes() - _daily_reserved_bytes
    )
    return min(
        max(0, lease.max_bytes - lease.total_bytes - lease.reserved_bytes),
        daily_remaining,
    )


def _reserve_lease_bytes(lease: Lease, wanted: int) -> int:
    """Atomically reserve allowance before an async read/write yields control."""
    global _daily_reserved_bytes
    count = min(max(0, wanted), _lease_remaining(lease))
    lease.reserved_bytes += count
    _daily_reserved_bytes += count
    return count


def _release_lease_reservation(lease: Lease, count: int) -> None:
    global _daily_reserved_bytes
    lease.reserved_bytes = max(0, lease.reserved_bytes - count)
    _daily_reserved_bytes = max(0, _daily_reserved_bytes - count)


def _account_lease_bytes(lease: Lease, host: str, direction: str, count: int) -> None:
    """Account bytes that were actually written across the paid upstream leg."""
    global _daily_up_bytes, _daily_down_bytes
    if count <= 0:
        return
    _refresh_daily_counter()
    host_stats = lease.hosts.setdefault(host, {"up_bytes": 0, "down_bytes": 0})
    if direction == "up":
        lease.up_bytes += count
        _daily_up_bytes += count
        host_stats["up_bytes"] += count
        up_bytes[host] += count
    else:
        lease.down_bytes += count
        _daily_down_bytes += count
        host_stats["down_bytes"] += count
        down_bytes[host] += count
    if lease.total_bytes >= lease.max_bytes or _daily_total_bytes() >= DAILY_BUDGET_BYTES:
        lease.budget_exceeded = True


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
    *,
    lease: Lease | None = None,
    direction: str | None = None,
) -> None:
    try:
        while True:
            read_size = 65536
            reservation = 0
            if lease is not None:
                reservation = _reserve_lease_bytes(lease, read_size)
                if reservation <= 0:
                    lease.budget_exceeded = lease.total_bytes >= lease.max_bytes
                    break
                read_size = reservation
            try:
                chunk = await reader.read(read_size)
            except Exception:
                if lease is not None:
                    _release_lease_reservation(lease, reservation)
                raise
            if lease is not None:
                # No await between release and accounting: asyncio cannot let
                # a sibling tunnel steal this allowance in the middle.
                _release_lease_reservation(lease, reservation)
                if chunk:
                    assert direction in ("up", "down")
                    _account_lease_bytes(lease, host, direction, len(chunk))
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
            if lease is None:
                counter[host] += len(chunk)
    except Exception:  # noqa: BLE001 — proxy must never crash a flow
        pass
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


async def _read_headers(reader: asyncio.StreamReader) -> list[bytes]:
    lines: list[bytes] = []
    while True:
        h = await reader.readline()
        if h in (b"\r\n", b"\n", b""):
            return lines
        lines.append(h)


def _header_map(lines: list[bytes]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in lines:
        try:
            name, value = line.decode("latin1").split(":", 1)
        except ValueError:
            continue
        headers[name.strip().lower()] = value.strip()
    return headers


async def _send_json(
    writer: asyncio.StreamWriter, status: int, payload: dict[str, Any]
) -> None:
    reason = {
        200: "OK",
        201: "Created",
        400: "Bad Request",
        401: "Unauthorized",
        404: "Not Found",
        409: "Conflict",
        429: "Too Many Requests",
        500: "Internal Server Error",
    }.get(status, "Error")
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
        + body
    )
    await writer.drain()
    writer.close()


async def _close_lease(lease: Lease) -> dict[str, Any]:
    """Stop all tunnels and wait until byte counters are final."""
    lease.closed = True
    for tunnel_writer in tuple(lease.tunnel_writers):
        try:
            tunnel_writer.close()
        except Exception:  # noqa: BLE001
            pass
    deadline = time.monotonic() + 2.0
    while lease.active_tunnels and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    return lease.report()


async def _handle_control(
    method: str,
    target: str,
    headers: dict[str, str],
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    mgr,
) -> bool:
    """Serve the lease API on the proxy listener; return False for proxy traffic."""
    path = urlsplit(target).path
    if method == "GET" and path == "/health":
        await _send_json(
            writer,
            200,
            {
                "status": "ok",
                "daily_total_bytes": _daily_total_bytes(),
                "daily_budget_bytes": DAILY_BUDGET_BYTES,
            },
        )
        return True
    if not path.startswith("/v1/leases"):
        return False
    if method == "POST" and path == "/v1/leases":
        try:
            length = int(headers.get("content-length", "0"))
            body = await reader.readexactly(length) if length else b"{}"
            request = json.loads(body)
            lease = _create_lease(
                mgr,
                max_bytes=int(request.get("max_bytes", DEFAULT_LEASE_BYTES)),
                ttl_seconds=int(
                    request.get("ttl_seconds", DEFAULT_LEASE_TTL_SECONDS)
                ),
            )
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            await _send_json(writer, 400, {"error": str(exc)})
            return True
        except RuntimeError as exc:
            await _send_json(writer, 429, {"error": str(exc)})
            return True
        await _send_json(
            writer,
            201,
            {
                "id": lease.lease_id,
                "token": lease.token,
                "max_bytes": lease.max_bytes,
                "expires_at": lease.expires_at,
                "proxy_url": LEASE_PROXY_URL,
            },
        )
        return True

    parts = path.strip("/").split("/")
    if len(parts) != 4 or parts[:2] != ["v1", "leases"]:
        await _send_json(writer, 404, {"error": "unknown lease endpoint"})
        return True
    lease_id, action = parts[2], parts[3]
    lease = _authorized_control_lease(lease_id, headers.get("authorization"))
    if lease is None:
        await _send_json(writer, 401, {"error": "invalid lease token"})
        return True
    if method == "GET" and action == "stats":
        report = lease.report()
        report["daily_total_bytes"] = _daily_total_bytes()
        report["daily_budget_bytes"] = DAILY_BUDGET_BYTES
        await _send_json(writer, 200, report)
        return True
    if method == "DELETE" and action == "close":
        report = await _close_lease(lease)
        await _send_json(writer, 200, report)
        return True
    # Convenience: DELETE /v1/leases/{id} is represented by three path parts,
    # handled below before declaring the route unknown.
    await _send_json(writer, 404, {"error": "unknown lease endpoint"})
    return True


async def _handle_control_delete_short(
    method: str,
    target: str,
    headers: dict[str, str],
    writer: asyncio.StreamWriter,
) -> bool:
    path = urlsplit(target).path
    parts = path.strip("/").split("/")
    if method != "DELETE" or len(parts) != 3 or parts[:2] != ["v1", "leases"]:
        return False
    lease = _authorized_control_lease(parts[2], headers.get("authorization"))
    if lease is None:
        await _send_json(writer, 401, {"error": "invalid lease token"})
        return True
    await _send_json(writer, 200, await _close_lease(lease))
    return True


async def _write_upstream(
    writer: asyncio.StreamWriter,
    payload: bytes,
    *,
    lease: Lease | None,
    host: str,
    direction: str,
) -> bool:
    reservation = 0
    if lease is not None:
        reservation = _reserve_lease_bytes(lease, len(payload))
        if reservation != len(payload):
            _release_lease_reservation(lease, reservation)
            lease.budget_exceeded = True
            return False
    writer.write(payload)
    if lease is not None:
        _release_lease_reservation(lease, reservation)
        _account_lease_bytes(lease, host, direction, len(payload))
    await writer.drain()
    return True


async def handle(
    client_r: asyncio.StreamReader,
    client_w: asyncio.StreamWriter,
    mgr,
    *,
    require_lease: bool = False,
) -> None:
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
        raw_headers = await _read_headers(client_r)
        headers = _header_map(raw_headers)
        if await _handle_control_delete_short(method, target, headers, client_w):
            return
        if await _handle_control(method, target, headers, client_r, client_w, mgr):
            return
        host = target.rsplit(":", 1)[0] if method == "CONNECT" else (urlsplit(target).hostname or target)

        if _is_blocked(host):
            blocked_count[host] += 1
            # refuse without dialing the residential upstream → bytes never billed
            client_w.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return

        proxy_authorization = headers.get("proxy-authorization")
        lease = _lease_from_proxy_authorization(proxy_authorization)
        if (require_lease and lease is None) or (proxy_authorization and lease is None):
            client_w.write(
                b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                b"Proxy-Authenticate: Basic realm=lease\r\n\r\n"
            )
            await client_w.drain()
            client_w.close()
            return
        if lease is not None and not lease.usable:
            client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return

        # Explicit callers use the immutable lease upstream.  Credential-less
        # callers retain the old idle-refresh path until their transports are
        # migrated, which preserves compatibility without weakening lease
        # isolation.
        if lease is not None:
            up_host, up_port, up_user, up_pass = lease.upstream
            lease.active_tunnels += 1
            lease.tunnel_writers.add(client_w)
        else:
            up_host, up_port, up_user, up_pass = _acquire_upstream(mgr)
            _active += 1
        auth = base64.b64encode(f"{up_user}:{up_pass}".encode()).decode()
        srv_w = None
        try:
            if method == "CONNECT":
                conn_count[host] += 1
                srv_r, srv_w = await asyncio.open_connection(up_host, up_port)
                if lease is not None:
                    lease.tunnel_writers.add(srv_w)
                connect_request = (
                    f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n"
                    f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
                )
                if not await _write_upstream(
                    srv_w, connect_request, lease=lease, host=host, direction="up"
                ):
                    client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
                    await client_w.drain()
                    return
                status = await srv_r.readline()
                response_headers = await _read_headers(srv_r)
                upstream_response = status + b"".join(response_headers) + b"\r\n"
                if lease is not None:
                    _account_lease_bytes(lease, host, "down", len(upstream_response))
                if b"200" not in status:
                    client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    await client_w.drain()
                    client_w.close()
                    srv_w.close()
                    return
                client_w.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await client_w.drain()
                await asyncio.gather(
                    _pump(
                        client_r,
                        srv_w,
                        host,
                        up_bytes,
                        lease=lease,
                        direction="up",
                    ),
                    _pump(
                        srv_r,
                        client_w,
                        host,
                        down_bytes,
                        lease=lease,
                        direction="down",
                    ),
                )
            else:
                conn_count[host] += 1
                srv_r, srv_w = await asyncio.open_connection(up_host, up_port)
                if lease is not None:
                    lease.tunnel_writers.add(srv_w)
                forwarded_headers = b"".join(
                    line
                    for line in raw_headers
                    if not line.lower().startswith(b"proxy-authorization:")
                )
                request_head = (
                    first
                    + forwarded_headers
                    + f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
                )
                if not await _write_upstream(
                    srv_w, request_head, lease=lease, host=host, direction="up"
                ):
                    client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
                    await client_w.drain()
                    return
                await asyncio.gather(
                    _pump(
                        client_r,
                        srv_w,
                        host,
                        up_bytes,
                        lease=lease,
                        direction="up",
                    ),
                    _pump(
                        srv_r,
                        client_w,
                        host,
                        down_bytes,
                        lease=lease,
                        direction="down",
                    ),
                )
        finally:
            if lease is not None:
                lease.tunnel_writers.discard(client_w)
                if srv_w is not None:
                    lease.tunnel_writers.discard(srv_w)
                lease.active_tunnels = max(0, lease.active_tunnels - 1)
            else:
                _active -= 1
    except Exception:  # noqa: BLE001
        try:
            client_w.close()
        except Exception:  # noqa: BLE001
            pass


def _dump(out_path: str, quiet: bool = False) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
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
        "daily": {
            "day": _daily_day or _utc_day(),
            "up_bytes": _daily_up_bytes,
            "down_bytes": _daily_down_bytes,
            "total_bytes": _daily_total_bytes(),
            "budget_bytes": DAILY_BUDGET_BYTES,
        },
        "leases": [lease.report() for lease in LEASES.values()],
        "allowed_hosts": rows,
        "blocked_hosts": sorted(
            ({"host": h, "attempts": c} for h, c in blocked_count.items()),
            key=lambda r: -r["attempts"],
        ),
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
    global BLOCKLIST, DAILY_BUDGET_BYTES, MAX_LEASE_BYTES, LEASE_PROXY_URL
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0:8899")
    ap.add_argument(
        "--lease-listen",
        default="0.0.0.0:8900",
        help="authenticated proxy listener used by explicit leases",
    )
    ap.add_argument(
        "--lease-proxy-url",
        default=os.environ.get("PROXY_FILTER_LEASE_URL", "http://proxy_filter:8900"),
        help="lease proxy URL returned by POST /v1/leases",
    )
    ap.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    ap.add_argument("--blocklist", default=None, help="domain blocklist file (omit = observe only)")
    ap.add_argument("--out", default="/tmp/filter_bytes.json")
    ap.add_argument("--pidfile", default="/tmp/filter_proxy.pid")
    ap.add_argument("--daily-budget-mb", type=float, default=100.0)
    ap.add_argument("--max-lease-mb", type=float, default=24.0)
    args = ap.parse_args()

    if args.daily_budget_mb <= 0 or args.max_lease_mb <= 0:
        raise SystemExit("proxy byte budgets must be positive")
    DAILY_BUDGET_BYTES = int(args.daily_budget_mb * 1024 * 1024)
    MAX_LEASE_BYTES = int(args.max_lease_mb * 1024 * 1024)
    LEASE_PROXY_URL = args.lease_proxy_url.rstrip("/")
    _restore_daily_counter(args.out)

    with open(args.pidfile, "w") as fh:
        fh.write(str(os.getpid()))

    BLOCKLIST = _load_blocklist(args.blocklist)
    log.info("blocklist: %d domains from %s", len(BLOCKLIST), args.blocklist or "(none — observe mode)")

    mgr = _residential_manager(args.proxy_file)
    log.info(
        "residential pool = %d proxies (explicit sticky leases; legacy idle-refresh enabled)",
        mgr.total_count,
    )
    host, port = args.listen.rsplit(":", 1)
    lease_host, lease_port = args.lease_listen.rsplit(":", 1)

    server = await asyncio.start_server(lambda r, w: handle(r, w, mgr), host, int(port))
    lease_server = await asyncio.start_server(
        lambda r, w: handle(r, w, mgr, require_lease=True),
        lease_host,
        int(lease_port),
    )
    log.info(
        "listening on %s (lease API + authenticated proxy; legacy no-auth enabled)",
        args.listen,
    )
    log.info(
        "authenticated lease proxy listening on %s (advertised as %s)",
        args.lease_listen,
        LEASE_PROXY_URL,
    )

    asyncio.ensure_future(_periodic_dump(args.out))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async with server, lease_server:
        await stop.wait()
    _dump(args.out)


if __name__ == "__main__":
    asyncio.run(main())
