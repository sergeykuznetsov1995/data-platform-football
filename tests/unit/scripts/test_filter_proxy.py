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
import asyncio
import json
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

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
    assert set(report) == {"total_mb", "allowed_hosts", "blocked_hosts"}
    assert report["allowed_hosts"][0]["host"] == "sofifa.com"
    assert report["allowed_hosts"][0]["down_mb"] == pytest.approx(1.0, abs=0.01)
    assert report["blocked_hosts"] == [{"host": "doubleclick.net", "attempts": 5}]


def test_budgeted_dump_exposes_exact_provider_bytes_for_canary(mod, tmp_path):
    mod.up_bytes = defaultdict(int, {"www.sofascore.com": 19})
    mod.down_bytes = defaultdict(int, {"www.sofascore.com": 23})
    mod.provider_budget_guard = object()
    mod.provider_budget_endpoint = "event"
    out = tmp_path / "provider.json"
    mod._dump(str(out), quiet=True)
    report = json.loads(out.read_text())
    assert report["total_provider_bytes"] == 42
    assert report["endpoint_provider_bytes"] == {"event": 42}
    assert report["endpoint_request_provider_bytes"] == {"event": [42]}


def test_pump_charges_provider_guard_before_forwarding(mod):
    class Reader:
        def __init__(self):
            self.chunks = [b"provider-bytes", b""]

        async def read(self, size):
            return self.chunks.pop(0)

    class Writer:
        def __init__(self):
            self.writes = []
            self.closed = False

        def write(self, chunk):
            self.writes.append(chunk)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    class Guard:
        def __init__(self):
            self.charges = []

        def consume(self, amount):
            self.charges.append(amount)

    writer = Writer()
    guard = Guard()
    counter = defaultdict(int)
    asyncio.run(mod._pump(Reader(), writer, "www.sofascore.com", counter, guard))
    assert guard.charges == [len(b"provider-bytes")]
    assert counter["www.sofascore.com"] == len(b"provider-bytes")
    assert writer.writes == [b"provider-bytes"]
    assert writer.closed is True


def test_pump_does_not_double_charge_a_preclaimed_provider_read(mod):
    class Reader:
        def __init__(self):
            self.chunks = [b"preclaimed", b""]

        async def read(self, size):
            return self.chunks.pop(0)

    class Writer:
        def __init__(self):
            self.writes = []

        def write(self, chunk):
            self.writes.append(chunk)

        async def drain(self):
            return None

        def close(self):
            return None

    class Guard:
        def __init__(self):
            self.claimed = []

        async def read_metered(self, reader, max_bytes):
            chunk = await reader.read(max_bytes)
            self.claimed.append(len(chunk))
            return chunk

        def consume(self, amount):
            raise AssertionError("preclaimed bytes must not be charged twice")

    writer = Writer()
    guard = Guard()
    counter = defaultdict(int)
    asyncio.run(mod._pump(Reader(), writer, "www.sofascore.com", counter, guard))
    assert guard.claimed == [len(b"preclaimed"), 0]
    assert counter["www.sofascore.com"] == len(b"preclaimed")
    assert writer.writes == [b"preclaimed"]


def _initialize_real_metered_guard(mod, monkeypatch, tmp_path):
    """Run only filter_proxy's budget initialization and return its real guard."""
    from scripts.proxy_filter.budget import SharedBudgetLedger
    from tests.unit.scripts.test_sofascore_proxy_budget import _artifact

    artifact = _artifact(tmp_path / "canary.json")
    policy = mod.load_verified_policy(artifact)
    ledger_path = tmp_path / "ledger.json"
    ledger = SharedBudgetLedger(ledger_path, policy)
    token, limit = ledger.reserve("logical-run", "event")
    args = SimpleNamespace(
        listen="127.0.0.1:0",
        proxy_file=str(tmp_path / "unused-proxies.txt"),
        blocklist=None,
        out=str(tmp_path / "meter.json"),
        pidfile=str(tmp_path / "filter.pid"),
        budget_artifact=str(artifact),
        budget_ledger=str(ledger_path),
        budget_run_id="logical-run",
        budget_reservation_token=token,
        budget_endpoint="event",
    )
    monkeypatch.setattr(mod.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(
        mod,
        "_residential_manager",
        lambda path: SimpleNamespace(total_count=1),
    )

    class Server:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    async def start_server(*args, **kwargs):
        return Server()

    class StopEvent:
        def set(self):
            return None

        async def wait(self):
            return None

    monkeypatch.setattr(mod.asyncio, "start_server", start_server)
    monkeypatch.setattr(mod.asyncio, "Event", StopEvent)
    monkeypatch.setattr(
        mod.asyncio,
        "get_running_loop",
        lambda: SimpleNamespace(add_signal_handler=lambda *args: None),
    )

    def discard_background(coro):
        coro.close()
        return None

    monkeypatch.setattr(mod.asyncio, "ensure_future", discard_background)
    asyncio.run(mod.main())
    assert mod.provider_budget_guard is not None
    return mod.provider_budget_guard, ledger, token, limit


def test_real_metered_read_refunds_short_socket_reads_without_double_charge(
    mod,
    monkeypatch,
    tmp_path,
):
    guard, ledger, token, _ = _initialize_real_metered_guard(
        mod, monkeypatch, tmp_path
    )

    class ShortReader:
        def __init__(self):
            self.chunks = [b"short-read", b""]

        async def read(self, size):
            chunk = self.chunks.pop(0)
            assert len(chunk) <= size
            return chunk

    reader = ShortReader()
    first = asyncio.run(guard.read_metered(reader, 65536))
    assert first == b"short-read"
    assert ledger.snapshot("logical-run")["spent_provider_bytes"] == len(first)

    # EOF is also a short read: its entire preclaim must be refunded.
    assert asyncio.run(guard.read_metered(reader, 65536)) == b""
    assert ledger.snapshot("logical-run")["spent_provider_bytes"] == len(first)

    # finish validates the provider report against already-claimed bytes; it
    # must not add the same traffic a second time.
    assert ledger.finish(
        "logical-run", token, reported_provider_bytes=len(first)
    ) == len(first)
    assert ledger.snapshot("logical-run")["spent_provider_bytes"] == len(first)


def test_pump_forwards_only_the_atomic_final_provider_chunk(
    mod,
    monkeypatch,
    tmp_path,
):
    guard, ledger, _, limit = _initialize_real_metered_guard(
        mod, monkeypatch, tmp_path
    )

    class Reader:
        def __init__(self):
            self.payload = b"x" * (limit + 23)

        async def read(self, size):
            chunk, self.payload = self.payload[:size], self.payload[size:]
            return chunk

    class Writer:
        def __init__(self):
            self.writes = []
            self.closed = False

        def write(self, chunk):
            self.writes.append(chunk)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    writer = Writer()
    counter = defaultdict(int)
    asyncio.run(
        mod._pump(
            Reader(),
            writer,
            "www.sofascore.com",
            counter,
            guard,
        )
    )

    # The second read is refused before bytes move. _pump must not call
    # consume after a precharged read and cannot forward the 23-byte tail.
    assert sum(map(len, writer.writes)) == limit
    assert counter["www.sofascore.com"] == limit
    assert ledger.snapshot("logical-run")["spent_provider_bytes"] == limit
    assert writer.closed is True


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
