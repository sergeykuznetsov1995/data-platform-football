"""Unit tests for scripts/proxy_filter/filter_proxy.py (#652).

``filter_proxy`` is a standalone script (not a package). Its only container-only
import (``scrapers.utils.proxy_manager``) is lazy — inside ``_residential`` — so the
module loads on the host with no stubbing and no network.

What we cover (the safety-critical, pure logic):
  - ``_is_blocked``: dot-boundary suffix matching, case-insensitivity, and the
    invariant that the Cloudflare challenge + target sites are NEVER blocked.
  - ``_load_blocklist``: comment/blank stripping, lowercasing, None -> empty.
  - ``_dump``: report shape (total_mb / allowed_hosts / blocked_hosts).
  - the SHIPPED ``configs/proxy_filter/blocklist.txt`` does not footgun CF/the sites.
"""
from __future__ import annotations

import importlib.util
import json
import base64
import time
from collections import defaultdict
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / "scripts" / "proxy_filter" / "filter_proxy.py"
_BLOCKLIST_PATH = REPO_ROOT / "configs" / "proxy_filter" / "blocklist.txt"


def _load_module():
    spec = importlib.util.spec_from_file_location("filter_proxy", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f"cannot load {_SCRIPT_PATH}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return _load_module()


# --- _is_blocked --------------------------------------------------------------

def test_blocks_exact_domain(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("doubleclick.net") is True


def test_blocks_subdomain_via_dot_suffix(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert — a real ad subdomain must be caught by the suffix rule
    assert mod._is_blocked("securepubads.g.doubleclick.net") is True


def test_does_not_block_lookalike_without_dot_boundary(mod):
    # Arrange — "notdoubleclick.net" merely ends with the string, not ".doubleclick.net"
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("notdoubleclick.net") is False


def test_matching_is_case_insensitive(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("SecurePubAds.G.DoubleClick.NET") is True


@pytest.mark.parametrize(
    "host",
    [
        "challenges.cloudflare.com",  # CF Turnstile — blocking it breaks the bypass
        "sofifa.com",
        "cdn.sofifa.net",
        "www.whoscored.com",
        "cdn.whoscored.com",
    ],
)
def test_never_blocks_cf_or_target_sites(mod, host):
    # Arrange — even with a broad ad blocklist, these must always pass
    mod.BLOCKLIST = {"doubleclick.net", "googletagmanager.com", "adnxs.com"}
    # Act / Assert
    assert mod._is_blocked(host) is False


def test_empty_blocklist_blocks_nothing(mod):
    # Arrange — observe mode: no blocklist
    mod.BLOCKLIST = set()
    # Act / Assert
    assert mod._is_blocked("securepubads.g.doubleclick.net") is False


# --- _load_blocklist ----------------------------------------------------------

def test_load_blocklist_none_returns_empty(mod):
    assert mod._load_blocklist(None) == set()


def test_load_blocklist_strips_comments_blanks_and_lowercases(mod, tmp_path):
    # Arrange
    f = tmp_path / "bl.txt"
    f.write_text("# header comment\n\nDoubleClick.net\n  adnxs.com  \n# trailing\n")
    # Act
    result = mod._load_blocklist(str(f))
    # Assert
    assert result == {"doubleclick.net", "adnxs.com"}


# --- _dump --------------------------------------------------------------------

def test_dump_writes_expected_report_shape(mod, tmp_path):
    # Arrange — populate the module-level byte counters
    mod.up_bytes = defaultdict(int, {"sofifa.com": 1000})
    mod.down_bytes = defaultdict(int, {"sofifa.com": 1_048_576})  # 1 MiB down
    mod.conn_count = defaultdict(int, {"sofifa.com": 2})
    mod.blocked_count = defaultdict(int, {"doubleclick.net": 5})
    out = tmp_path / "report.json"
    # Act
    mod._dump(str(out), quiet=True)
    # Assert
    report = json.loads(out.read_text())
    assert set(report) == {
        "total_mb", "daily", "leases", "allowed_hosts", "blocked_hosts"
    }
    assert report["allowed_hosts"][0]["host"] == "sofifa.com"
    assert report["allowed_hosts"][0]["down_mb"] == pytest.approx(1.0, abs=0.01)
    assert report["blocked_hosts"] == [{"host": "doubleclick.net", "attempts": 5}]


def test_daily_budget_is_restored_from_atomic_report(mod, tmp_path):
    out = tmp_path / "report.json"
    today = mod._utc_day()
    out.write_text(json.dumps({
        "daily": {"day": today, "up_bytes": 123, "down_bytes": 456}
    }))
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = mod._daily_reserved_bytes = 0

    mod._restore_daily_counter(str(out))

    assert mod._daily_day == today
    assert mod._daily_up_bytes == 123
    assert mod._daily_down_bytes == 456
    assert mod._daily_total_bytes() == 579


# --- _pick_upstream / _acquire_upstream (idle-refresh rotation) ---------------

class _FakeProxy:
    def __init__(self, url):
        self.url = url


class _FakeManager:
    """Stand-in for ProxyManager: hands out a different proxy on each get_proxy()."""

    def __init__(self, urls):
        self._urls = list(urls)
        self.calls = 0

    def get_proxy(self):
        url = self._urls[self.calls % len(self._urls)]
        self.calls += 1
        return _FakeProxy(url)


def test_pick_upstream_parses_creds_from_url(mod):
    # Arrange
    mgr = _FakeManager(["http://user:pass@pool.proxys.io:10000"])
    # Act
    host, port, user, pw = mod._pick_upstream(mgr)
    # Assert
    assert (host, port, user, pw) == ("pool.proxys.io", 10000, "user", "pass")


def test_acquire_upstream_draws_fresh_exit_when_idle(mod):
    # Arrange — pool of two exits, no tunnel open
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    mod._current_up, mod._active = None, 0
    # Act / Assert — idle → draw the first exit
    assert mod._acquire_upstream(mgr)[1] == 10000
    assert mgr.calls == 1


def test_acquire_upstream_reuses_exit_while_a_tunnel_is_open(mod):
    # Arrange — the page's tunnel is open on the first exit (_active == 1)
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    mod._current_up, mod._active = None, 0
    first = mod._acquire_upstream(mgr)  # mgr.calls -> 1
    mod._active = 1                     # that tunnel stays open
    # Act — a sibling CONNECT in the SAME CF session asks for an upstream
    second = mod._acquire_upstream(mgr)
    # Assert — same exit IP (page + Turnstile on one IP = CF-safe), no new draw
    assert second == first
    assert mgr.calls == 1


def test_acquire_upstream_refreshes_for_next_session_once_idle(mod):
    # Arrange — session 1 ran and every tunnel closed (back to idle)
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    mod._current_up, mod._active = None, 0
    mod._acquire_upstream(mgr)  # session 1 -> 10000, mgr.calls -> 1
    mod._active = 0             # all tunnels closed
    # Act — the next FlareSolverr session opens its first tunnel
    nxt = mod._acquire_upstream(mgr)
    # Assert — a fresh exit is drawn for the new session (#652 idle-refresh)
    assert nxt[1] == 10001
    assert mgr.calls == 2


# --- explicit sticky leases ---------------------------------------------------

def test_create_lease_pins_one_upstream_and_has_hard_limits(mod):
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0

    lease = mod._create_lease(mgr, max_bytes=4096, ttl_seconds=30)

    assert lease.upstream[1] == 10000
    assert lease.max_bytes == 4096
    assert lease.expires_at > lease.created_at
    assert mod.LEASES[lease.lease_id] is lease
    assert mod.LEASE_TOKENS[lease.token] == lease.lease_id
    # Re-reading a lease never asks the pool for a different exit.
    assert lease.upstream[1] == 10000
    assert mgr.calls == 1


def test_proxy_basic_auth_resolves_only_the_matching_lease(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    lease = mod._create_lease(mgr, max_bytes=4096, ttl_seconds=30)
    encoded = base64.b64encode(f"lease:{lease.token}".encode()).decode()

    assert mod._lease_from_proxy_authorization(f"Basic {encoded}") is lease
    assert mod._lease_from_proxy_authorization("Basic bm9wZTpub3Bl") is None
    assert mod._lease_from_proxy_authorization(None) is None


def test_lease_accounting_is_exact_and_budget_is_fail_closed(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.up_bytes = defaultdict(int)
    mod.down_bytes = defaultdict(int)
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    lease = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30)

    mod._account_lease_bytes(lease, "www.whoscored.com", "up", 125)
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 875)

    assert lease.report()["up_bytes"] == 125
    assert lease.report()["down_bytes"] == 875
    assert lease.report()["total_bytes"] == 1000
    assert lease.report()["hosts"]["www.whoscored.com"] == {
        "up_bytes": 125,
        "down_bytes": 875,
    }
    assert lease.budget_exceeded is True
    assert mod._lease_remaining(lease) == 0


def test_closed_or_expired_lease_cannot_open_another_tunnel(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    lease = mod._create_lease(mgr, max_bytes=4096, ttl_seconds=30)

    lease.closed = True
    assert lease.usable is False
    assert mod._lease_remaining(lease) == 0
    lease.closed = False
    lease.expires_at = time.time() - 1
    assert lease.usable is False


# --- shipped blocklist safety -------------------------------------------------

def test_shipped_blocklist_blocks_adtech_but_not_cf_or_sites(mod):
    # Arrange — load the real config the filter ships with
    mod.BLOCKLIST = mod._load_blocklist(str(_BLOCKLIST_PATH))
    assert mod.BLOCKLIST, "shipped blocklist must not be empty"
    # Assert — must keep CF + the scraped sites alive
    for keep in (
        "challenges.cloudflare.com",
        "sofifa.com",
        "cdn.sofifa.net",
        "www.whoscored.com",
        "cdn.whoscored.com",
        "fonts.gstatic.com",
    ):
        assert mod._is_blocked(keep) is False, f"{keep} must NOT be blocked"
    # Assert — must drop the heavy ad-tech seen in the observe run
    for drop in (
        "securepubads.g.doubleclick.net",
        "www.googletagmanager.com",
        "ib.adnxs.com",
        "connect.facebook.net",
        "cdn.intergient.com",
    ):
        assert mod._is_blocked(drop) is True, f"{drop} must be blocked"
