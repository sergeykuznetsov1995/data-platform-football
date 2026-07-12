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
import base64
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.sofascore.workload_plan import (
    MATCH_WORKLOAD_CLASS,
    PLAYER_WORKLOAD_CLASS,
    WorkloadAllocation,
    _signed_plan,
)

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
def mod(tmp_path):
    loaded = _load_module()
    loaded.LEDGER_PATH = str(tmp_path / "paid_requests.jsonl")
    loaded.CONTROL_TOKEN = "c" * 32
    loaded.SOFASCORE_BUDGET_ARTIFACT_ID = "a" * 64
    loaded.SOFASCORE_ALLOCATION_LEDGER_PATH = str(tmp_path / "allocations.json")
    loaded.SOFASCORE_ALLOCATION_WAL_PATH = str(tmp_path / "allocation-wal.jsonl")
    loaded.SOFASCORE_ALLOCATION_LEDGER = None
    loaded._SOFASCORE_ALLOCATION_LEDGER_KEY = None
    loaded.SOFASCORE_PARENT_ENVELOPE_PATH = str(tmp_path / "parent-envelopes.json")
    loaded.SOFASCORE_PARENT_ENVELOPE_LEDGER = None
    loaded._SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH = ""
    return loaded


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
    assert {
        "total_mb",
        "daily",
        "leases",
        "dagruns",
        "allowed_hosts",
        "blocked_hosts",
    }.issubset(report)
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
    policy = mod.load_verified_policy(artifact, workload_class=MATCH_WORKLOAD_CLASS)
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
        budget_workload_class=MATCH_WORKLOAD_CLASS,
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
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", mod.CONTROL_TOKEN)
    asyncio.run(mod.main())
    assert mod.provider_budget_guard is not None
    return mod.provider_budget_guard, ledger, token, limit


def test_real_metered_read_refunds_short_socket_reads_without_double_charge(
    mod,
    monkeypatch,
    tmp_path,
):
    guard, ledger, token, _ = _initialize_real_metered_guard(mod, monkeypatch, tmp_path)

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
    guard, ledger, _, limit = _initialize_real_metered_guard(mod, monkeypatch, tmp_path)

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
    mod._active = 1  # that tunnel stays open
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
    mod._active = 0  # all tunnels closed
    # Act — the next FlareSolverr session opens its first tunnel
    nxt = mod._acquire_upstream(mgr)
    # Assert — a fresh exit is drawn for the new session (#652 idle-refresh)
    assert nxt[1] == 10001
    assert mgr.calls == 2


# --- authenticated production leases -----------------------------------------


def _sofascore_context(**values):
    budget = int(values.pop("budget", 4096))
    artifact_id = str(values.pop("artifact_id", "a" * 64))
    context = {
        "source": "sofascore",
        "dag_id": "dag_ingest_sofascore",
        "run_id": "scheduled__2026-07-11::season",
        "task_id": "capture_match_batch_00000",
        "canonical_url": "https://www.sofascore.com/",
        "scope": "match",
        "capture_scope": "competition-season",
        "entity": "17/76986",
    }
    context.update(values)
    phase = context["run_id"].rsplit("::", 1)[-1]
    allocation_scope = (
        "season" if phase == "season" else "player" if phase == "players" else "match"
    )
    workload_class = (
        "season_test_shape"
        if phase == "season"
        else PLAYER_WORKLOAD_CLASS
        if phase == "players"
        else MATCH_WORKLOAD_CLASS
    )
    identity = (
        f"{context['dag_id']}\0{context['run_id']}\0{context['task_id']}\0{budget}"
    )
    allocation = WorkloadAllocation(
        allocation_id="alloc-" + hashlib.sha256(identity.encode()).hexdigest()[:32],
        task_id=context["task_id"],
        scope=allocation_scope,
        workload_class=workload_class,
        batch_index=0,
        units=("1",),
        budget_bytes=budget,
    )
    plan = _signed_plan(
        artifact_id=artifact_id,
        dag_id=context["dag_id"],
        run_id=context["run_id"],
        player_universe_ids=(("1",) if phase == "players" else ()),
        allocations=(allocation,),
        control_token="c" * 32,
    )
    context.update(
        scope=allocation_scope,
        workload_plan=plan.to_dict(),
        allocation_id=allocation.allocation_id,
        allocation=allocation.to_dict(),
        attempt_id="1",
    )
    return context


def _sofascore_canary_context(**values):
    context = {
        "source": "sofascore_canary",
        "dag_id": "dag_canary_sofascore_proxy",
        "run_id": "manual__cold-canary-01",
        "task_id": "capture_fixed_cohort",
        "canonical_url": "https://www.sofascore.com/",
        "scope": "25_matches_50_players",
        "entity": "cold",
    }
    context.update(values)
    return context


def test_sofascore_lease_is_disabled_without_verified_canary_budget(mod):
    mgr = _FakeManager(["http://provider-user:provider-pass@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 0

    with pytest.raises(RuntimeError, match="verified canary required"):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=_sofascore_context(),
            require_context=True,
        )

    # Fail before selecting or opening any paid upstream.
    assert mgr.calls == 0


def test_production_rejects_missing_or_tampered_signed_plan_before_upstream(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    missing = _sofascore_context()
    missing.pop("workload_plan")
    with pytest.raises(mod.WorkloadPlanError):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=missing,
            require_context=True,
        )
    tampered = _sofascore_context()
    tampered["workload_plan"]["signature"] = "0" * 64
    with pytest.raises(mod.WorkloadPlanError):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=tampered,
            require_context=True,
        )
    assert mgr.calls == 0


def test_signed_allocation_is_concurrent_safe_and_retry_uses_remaining(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    with pytest.raises(RuntimeError, match="concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={**context, "attempt_id": "concurrent"},
            require_context=True,
        )
    boundary = mod._begin_endpoint_request(first, "event")
    mod._account_lease_bytes(first, "www.sofascore.com", "down", 125)
    mod._finish_endpoint_request(first, boundary)
    report = asyncio.run(
        mod._close_lease(
            first,
            completed=False,
            endpoint_request_provider_bytes={"event": [125]},
        )
    )
    assert report["plan_digest"] == context["workload_plan"]["plan_digest"]
    assert report["allocation_id"] == context["allocation_id"]
    assert report["endpoint_request_provider_bytes"] == {"event": [125]}
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-2", "try_number": 2},
        require_context=True,
    )
    assert retry.max_bytes == 875
    assert mgr.calls == 2


def test_ttl_reaps_abandoned_claim_and_retry_needs_no_sidecar_restart(
    mod, monkeypatch
):
    clock = [1_000.0]
    monkeypatch.setattr(mod, "_wall_time", lambda: clock[0])
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )

    clock[0] = 1_031.0
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-after-ttl"},
        require_context=True,
    )

    assert first.expired is True
    assert first.closed is True
    assert first.allocation_finished is True
    assert first.close_recorded is True
    assert retry.max_bytes == 1000
    assert retry.allocation_claim.spent_provider_bytes == 0
    assert mgr.calls == 2
    wal = [
        json.loads(line)
        for line in Path(mod.SOFASCORE_ALLOCATION_WAL_PATH).read_text().splitlines()
    ]
    expired = [
        event
        for event in wal
        if event["event_type"] == "allocation_finished"
        and event.get("expired") is True
    ]
    assert len(expired) == 1
    assert expired[0]["lease_id"] == first.lease_id
    assert expired[0]["completed"] is False


def test_ttl_preserves_open_endpoint_bytes_and_retries_only_remainder(
    mod, monkeypatch
):
    clock = [2_000.0]
    monkeypatch.setattr(mod, "_wall_time", lambda: clock[0])
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    mod._begin_endpoint_request(first, "lineups")
    mod._account_lease_bytes(first, "www.sofascore.com", "down", 125)

    clock[0] = 2_031.0
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-open-endpoint-after-ttl"},
        require_context=True,
    )

    assert first.current_request_id == ""
    assert first.endpoint_request_provider_bytes == {"lineups": [125]}
    assert retry.max_bytes == 875
    assert retry.allocation_claim.spent_provider_bytes == 125
    assert mod._run_total_bytes(first.dagrun_key) == 125
    plan = mod.SignedDagRunPlan.from_dict(
        context["workload_plan"], control_token=mod.CONTROL_TOKEN
    )
    allocation = mod._allocation_ledger().snapshot(plan)["allocations"][
        context["allocation_id"]
    ]
    assert allocation["spent_provider_bytes"] == 125
    assert allocation["active_claim"]["attempt_id_hash"] == hashlib.sha256(
        b"retry-open-endpoint-after-ttl"
    ).hexdigest()
    assert allocation["lease_stats"][-1]["endpoint_request_provider_bytes"] == {
        "lineups": [125]
    }
    assert allocation["lease_stats"][-1]["completed"] is False
    parent = json.loads(Path(mod.SOFASCORE_PARENT_ENVELOPE_PATH).read_text())
    parent_run = next(iter(parent["runs"].values()))
    assert parent_run["spent_provider_bytes"] == 125


def test_ttl_does_not_release_claim_until_tunnels_and_reservations_drain(
    mod, monkeypatch
):
    clock = [3_000.0]
    monkeypatch.setattr(mod, "_wall_time", lambda: clock[0])
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    first.active_tunnels = 1
    first.reserved_bytes = 1
    clock[0] = 3_031.0

    with pytest.raises(RuntimeError, match="concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={**context, "attempt_id": "retry-before-drain"},
            require_context=True,
        )
    assert first.allocation_finished is False

    first.active_tunnels = 0
    first.reserved_bytes = 0
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-after-drain"},
        require_context=True,
    )
    assert first.allocation_finished is True
    assert retry.max_bytes == 1000


def test_restart_recovers_endpoint_provenance_without_minting_bytes(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    lease = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "lineups")
    mod._account_lease_bytes(lease, "www.sofascore.com", "down", 100)

    # Provider sockets vanish on process restart.  The private WAL retains the
    # claim token and active endpoint, while the allocation ledger retains bytes.
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.SOFASCORE_ALLOCATION_LEDGER = None
    mod._SOFASCORE_ALLOCATION_LEDGER_KEY = None
    assert mod._recover_allocation_wal() == 1

    plan = mod.SignedDagRunPlan.from_dict(
        context["workload_plan"], control_token=mod.CONTROL_TOKEN
    )
    snapshot = mod._allocation_ledger().snapshot(plan)
    allocation = snapshot["allocations"][context["allocation_id"]]
    assert allocation["active_claim"] is None
    assert allocation["spent_provider_bytes"] == 100
    assert allocation["lease_stats"][-1]["endpoint_request_provider_bytes"] == {
        "lineups": [100]
    }
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-after-restart"},
        require_context=True,
    )
    assert retry.max_bytes == 900


def test_parent_envelope_sums_three_phases_and_stops_before_crossing(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    base = "scheduled__parent-envelope"
    season_context = _sofascore_context(run_id=f"{base}::season", budget=300)
    season = mod._create_lease(
        mgr,
        max_bytes=300,
        ttl_seconds=30,
        metadata=season_context,
        require_context=True,
    )
    season_boundary = mod._begin_endpoint_request(season, "schedule")
    mod._account_lease_bytes(season, "www.sofascore.com", "down", 300)
    mod._finish_endpoint_request(season, season_boundary)
    asyncio.run(
        mod._close_lease(
            season,
            completed=False,
            endpoint_request_provider_bytes={"schedule": [300]},
        )
    )

    target_context = _sofascore_context(run_id=f"{base}::targets", budget=400)
    target = mod._create_lease(
        mgr,
        max_bytes=400,
        ttl_seconds=30,
        metadata=target_context,
        require_context=True,
    )
    assert target.parent_run_cap_bytes == 700
    assert target.parent_run_spent_provider_bytes == 300
    target_boundary = mod._begin_endpoint_request(target, "event")
    mod._account_lease_bytes(target, "www.sofascore.com", "down", 400)
    mod._finish_endpoint_request(target, target_boundary)
    asyncio.run(
        mod._close_lease(
            target,
            completed=False,
            endpoint_request_provider_bytes={"event": [400]},
        )
    )

    player_context = _sofascore_context(run_id=f"{base}::players", budget=300)
    player = mod._create_lease(
        mgr,
        max_bytes=300,
        ttl_seconds=30,
        metadata=player_context,
        require_context=True,
    )
    assert player.parent_run_cap_bytes == 1000
    assert player.parent_run_spent_provider_bytes == 700
    player_boundary = mod._begin_endpoint_request(player, "player_profile")
    mod._account_lease_bytes(player, "www.sofascore.com", "down", 299)
    reserved = mod._reserve_lease_bytes(player, 10)
    assert reserved == 1
    mod._release_lease_reservation(player, reserved)
    with pytest.raises(mod.ParentEnvelopeBudgetExceeded):
        mod._account_lease_bytes(player, "www.sofascore.com", "down", 2)
    mod._finish_endpoint_request(player, player_boundary)
    asyncio.run(
        mod._close_lease(
            player,
            completed=False,
            endpoint_request_provider_bytes={"player_profile": [299]},
        )
    )

    retry = mod._create_lease(
        mgr,
        max_bytes=300,
        ttl_seconds=30,
        metadata={**player_context, "attempt_id": "retry"},
        require_context=True,
    )
    assert retry.max_bytes == 1
    assert retry.parent_run_cap_bytes == 1000
    assert retry.parent_run_spent_provider_bytes == 999
    asyncio.run(mod._close_lease(retry))

    changed_player = _sofascore_context(run_id=f"{base}::players", budget=301)
    with pytest.raises(mod.ParentEnvelopeError, match="immutable players plan"):
        mod._create_lease(
            mgr,
            max_bytes=301,
            ttl_seconds=30,
            metadata=changed_player,
            require_context=True,
        )
    assert mgr.calls == 4


def test_target_first_noop_season_is_allowed_but_late_season_cannot_expand(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 500
    base = "manual__target-first"
    target_context = _sofascore_context(run_id=f"{base}::targets", budget=500)
    target = mod._create_lease(
        mgr,
        max_bytes=500,
        ttl_seconds=30,
        metadata=target_context,
        require_context=True,
    )
    assert target.parent_run_cap_bytes == 500
    asyncio.run(mod._close_lease(target))

    with pytest.raises(mod.ParentEnvelopeError, match="target-first"):
        mod._create_lease(
            mgr,
            max_bytes=100,
            ttl_seconds=30,
            metadata=_sofascore_context(run_id=f"{base}::season", budget=100),
            require_context=True,
        )
    assert mgr.calls == 1


def test_later_player_phase_cannot_be_followed_by_a_new_match_phase(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 200
    base = "manual__player-first"
    player = mod._create_lease(
        mgr,
        max_bytes=100,
        ttl_seconds=30,
        metadata=_sofascore_context(
            run_id=f"{base}::players",
            budget=100,
        ),
        require_context=True,
    )
    asyncio.run(mod._close_lease(player))

    with pytest.raises(mod.ParentEnvelopeError, match="cannot expand"):
        mod._create_lease(
            mgr,
            max_bytes=100,
            ttl_seconds=30,
            metadata=_sofascore_context(
                run_id=f"{base}::targets",
                budget=100,
            ),
            require_context=True,
        )
    assert mgr.calls == 1


@pytest.mark.parametrize(
    "phase,bad_scope",
    [("season", "match"), ("targets", "player"), ("players", "match")],
)
def test_parent_envelope_rejects_mislabeled_phase_allocations(mod, phase, bad_scope):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 100
    context = _sofascore_context(
        run_id=f"manual__bad-phase-{phase}::{phase}", budget=100
    )
    original = context["allocation"]
    allocation = WorkloadAllocation(
        allocation_id=original["allocation_id"],
        task_id=original["task_id"],
        scope=bad_scope,
        workload_class=original["class"],
        batch_index=original["batch_index"],
        units=tuple(original["units"]),
        budget_bytes=original["budget"],
    )
    plan = _signed_plan(
        artifact_id="a" * 64,
        dag_id=context["dag_id"],
        run_id=context["run_id"],
        player_universe_ids=(("1",) if bad_scope == "player" else ()),
        allocations=(allocation,),
        control_token="c" * 32,
    )
    context.update(
        scope=bad_scope,
        workload_plan=plan.to_dict(),
        allocation=allocation.to_dict(),
    )
    with pytest.raises(mod.ParentEnvelopeError, match="phase plan"):
        mod._create_lease(
            mgr,
            max_bytes=100,
            ttl_seconds=30,
            metadata=context,
            require_context=True,
        )
    assert mgr.calls == 0


def test_sofascore_source_cannot_bypass_budget_with_another_dag_id(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096

    with pytest.raises(ValueError, match="source does not match dag_id"):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=_sofascore_context(dag_id="dag_ingest_whoscored"),
            require_context=True,
        )

    assert mgr.calls == 0


def test_explicit_canary_bootstraps_artifact_but_never_authorizes_production(
    mod,
    tmp_path,
):
    from tests.unit.scripts.test_sofascore_proxy_budget import _artifact

    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 0
    mod.SOFASCORE_BUDGET_ARTIFACT_ID = ""
    mod.SOFASCORE_CANARY_HARD_CAP_BYTES = 4096
    mod.SOFASCORE_CANARY_POLICY_ID = mod._canary_policy_id(4096)

    canary = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_canary_context(),
        require_context=True,
    )
    assert canary.source == "sofascore_canary"
    assert canary.report()["budget_artifact_id"] == mod.SOFASCORE_CANARY_POLICY_ID
    assert len(mod.SOFASCORE_CANARY_POLICY_ID) == 64
    assert mod.SOFASCORE_CANARY_POLICY_ID in Path(mod.LEDGER_PATH).read_text()
    canary.closed = True

    with pytest.raises(RuntimeError, match="verified canary required"):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=_sofascore_context(),
            require_context=True,
        )

    # Twenty complete cold observations produce the independent reviewed
    # artifact which, and only which, unlocks the production DAG.
    artifact_path = _artifact(tmp_path / "canary.json", runs=20)
    policy = mod.load_verified_workload_policy(artifact_path)
    match_policy = policy.classes[MATCH_WORKLOAD_CLASS]
    assert match_policy.sample_count == 20
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = match_policy.hard_task_bytes
    mod.SOFASCORE_BUDGET_ARTIFACT_ID = policy.artifact_id
    production = mod._create_lease(
        mgr,
        max_bytes=match_policy.hard_task_bytes,
        ttl_seconds=30,
        metadata=_sofascore_context(
            budget=match_policy.hard_task_bytes,
            artifact_id=policy.artifact_id,
        ),
        require_context=True,
    )
    assert production.source == "sofascore"
    assert production.report()["budget_artifact_id"] == policy.artifact_id
    assert policy.artifact_id != mod.SOFASCORE_CANARY_POLICY_ID


def test_sofascore_lease_pins_upstream_and_uses_basic_token_auth(mod, caplog):
    mgr = _FakeManager(
        [
            "http://provider-user:provider-pass@pool.invalid:10000",
            "http://provider-user:provider-pass@pool.invalid:10001",
        ]
    )
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    lease = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    encoded = base64.b64encode(f"lease:{lease.token}".encode()).decode()

    assert lease.upstream[1] == 10000
    assert mod._lease_from_proxy_authorization(f"Basic {encoded}") is lease
    assert mod._lease_from_proxy_authorization("Basic bm9wZTpub3Bl") is None
    assert mgr.calls == 1
    assert lease.report()["source"] == "sofascore"
    assert lease.report()["upstream_fingerprint"]
    assert lease.token not in repr(lease)
    assert "provider-user" not in repr(lease)
    assert "provider-pass" not in repr(lease)
    assert "provider-user" not in caplog.text
    assert "provider-pass" not in caplog.text
    assert lease.token not in caplog.text
    report_path = Path(mod.LEDGER_PATH).with_name("report.json")
    mod._dump(str(report_path), quiet=True)
    serialized_report = report_path.read_text()
    assert "provider-user" not in serialized_report
    assert "provider-pass" not in serialized_report
    assert lease.token not in serialized_report


def test_v1_lease_control_contract_returns_token_and_authenticated_stats(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    mod.SOFASCORE_BUDGET_ARTIFACT_ID = "a" * 64
    request = json.dumps(
        {
            **_sofascore_context(),
            "max_bytes": 4096,
            "ttl_seconds": 30,
        }
    ).encode()

    class Reader:
        async def readexactly(self, length):
            assert length == len(request)
            return request

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    created_writer = Writer()
    handled = asyncio.run(
        mod._handle_control(
            "POST",
            "/v1/leases",
            {
                "content-length": str(len(request)),
                "x-proxy-control-token": mod.CONTROL_TOKEN,
            },
            Reader(),
            created_writer,
            mgr,
        )
    )
    created_head, created_body = bytes(created_writer.payload).split(b"\r\n\r\n", 1)
    created = json.loads(created_body)
    assert handled is True
    assert b"201 Created" in created_head
    assert created["proxy_url"] == "http://proxy_filter:8900"
    assert created["token"]
    assert "upstream" not in created

    stats_writer = Writer()
    asyncio.run(
        mod._handle_control(
            "GET",
            f"/v1/leases/{created['id']}/stats",
            {
                "authorization": f"Bearer {created['token']}",
                "x-proxy-control-token": mod.CONTROL_TOKEN,
            },
            Reader(),
            stats_writer,
            mgr,
        )
    )
    _, stats_body = bytes(stats_writer.payload).split(b"\r\n\r\n", 1)
    stats = json.loads(stats_body)
    assert stats["source"] == "sofascore"
    assert stats["total_bytes"] == 0
    assert stats["dagrun_budget_bytes"] == 4096
    assert stats["budget_artifact_id"] == "a" * 64
    assert created["token"] not in stats_body.decode()


def test_lease_creation_rejects_missing_control_token_without_state(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])

    class Reader:
        async def readexactly(self, length):
            raise AssertionError("unauthorized body must not be read")

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    asyncio.run(
        mod._handle_control(
            "POST", "/v1/leases", {"content-length": "1"}, Reader(), writer, mgr
        )
    )

    assert b"401 Unauthorized" in writer.payload
    assert mod.LEASES == {}
    assert mgr.calls == 0


def test_sofascore_lease_host_scope_is_fail_closed(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    production = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )

    assert mod._lease_host_allowed(production, "www.sofascore.com") is True
    assert mod._lease_host_allowed(production, "api.sofascore.com") is True
    assert mod._lease_host_allowed(production, "challenges.cloudflare.com") is True
    assert mod._lease_host_allowed(production, "evil.example") is False
    assert mod._lease_host_allowed(production, "api.ipify.org") is False


def test_authenticated_proxy_listener_rejects_missing_lease_before_dial(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])

    class Reader:
        def __init__(self):
            self.lines = [
                b"CONNECT www.sofascore.com:443 HTTP/1.1\r\n",
                b"Host: www.sofascore.com:443\r\n",
                b"\r\n",
            ]

        async def readline(self):
            return self.lines.pop(0)

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    asyncio.run(mod.handle(Reader(), writer, mgr, require_lease=True))

    assert b"407 Proxy Authentication Required" in writer.payload
    assert mgr.calls == 0


def test_non_sofascore_leases_can_run_concurrently_up_to_configured_limit(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
            "http://u:p@pool.invalid:10002",
        ]
    )
    mod.MAX_ACTIVE_LEASES = 2
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={
            "dag_id": "dag_ingest_whoscored",
            "run_id": "run-a",
            "task_id": "task-a",
            "canonical_url": "https://www.whoscored.com/a",
        },
        require_context=True,
    )
    second = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={
            "dag_id": "dag_ingest_transfermarkt",
            "run_id": "run-b",
            "task_id": "task-b",
            "canonical_url": "https://www.transfermarkt.com/b",
        },
        require_context=True,
    )

    assert first.source == "whoscored"
    assert second.source == "transfermarkt"
    assert mgr.calls == 2
    with pytest.raises(RuntimeError, match="concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={
                "dag_id": "other",
                "run_id": "run-c",
                "task_id": "task-c",
                "canonical_url": "https://example.invalid/c",
            },
            require_context=True,
        )


def test_sofascore_production_and_canary_are_each_serial(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.MAX_ACTIVE_LEASES = 4
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    first = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    with pytest.raises(RuntimeError, match="SofaScore paid-proxy concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=_sofascore_context(run_id="another-run::season"),
            require_context=True,
        )
    first.closed = True

    mod.SOFASCORE_CANARY_HARD_CAP_BYTES = 4096
    mod.SOFASCORE_CANARY_POLICY_ID = mod._canary_policy_id(4096)
    canary = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_canary_context(),
        require_context=True,
    )
    with pytest.raises(RuntimeError, match="isolated serial"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={
                "dag_id": "other",
                "run_id": "run",
                "task_id": "task",
                "canonical_url": "https://example.invalid/",
            },
            require_context=True,
        )
    assert canary.source == "sofascore_canary"


def test_exit_probe_host_is_available_only_inside_canary_lease(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    production = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    production.closed = True
    mod.SOFASCORE_CANARY_HARD_CAP_BYTES = 4096
    mod.SOFASCORE_CANARY_POLICY_ID = mod._canary_policy_id(4096)
    canary = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_canary_context(),
        require_context=True,
    )

    assert mod._lease_host_allowed(canary, "api.ipify.org") is True
    assert mod._lease_host_allowed(production, "api.ipify.org") is False
    assert mod._lease_host_allowed(None, "api.ipify.org") is False
    assert mod._lease_host_allowed(production, "www.sofascore.com") is True


def test_sofascore_dagrun_budget_is_shared_without_legacy_url_truncation(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    mod.URL_BUDGET_BYTES = 100  # legacy per-page cap must not cut warmed capture
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    assert first.max_bytes == 1000
    assert first.report()["url_budget_bytes"] == 1000
    first_request = mod._begin_endpoint_request(first, "event")
    mod._account_lease_bytes(first, "www.sofascore.com", "down", 600)
    mod._finish_endpoint_request(first, first_request)
    asyncio.run(
        mod._close_lease(
            first,
            endpoint_request_provider_bytes={"event": [600]},
            completed=False,
        )
    )

    retry_context = {**context, "attempt_id": "2", "try_number": 2}
    second = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=retry_context,
        require_context=True,
    )
    assert second.max_bytes == 400
    second_request = mod._begin_endpoint_request(second, "event")
    mod._account_lease_bytes(second, "www.sofascore.com", "up", 400)
    mod._finish_endpoint_request(second, second_request)
    asyncio.run(
        mod._close_lease(
            second,
            endpoint_request_provider_bytes={"event": [400]},
            completed=False,
        )
    )

    with pytest.raises(RuntimeError, match="budget exhausted"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata={**context, "attempt_id": "3", "try_number": 3},
            require_context=True,
        )


def test_close_ack_waits_for_tunnels_reservations_and_durable_ledger(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    lease = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    lease.active_tunnels = 1
    lease.reserved_bytes = 10

    class Tunnel:
        def close(self):
            lease.active_tunnels = 0

    lease.tunnel_writers.add(Tunnel())

    pending = asyncio.run(mod._close_lease(lease))

    assert pending["closed"] is True
    assert pending["close_complete"] is False
    assert pending["active_tunnels"] == 0
    assert pending["reserved_bytes"] == 10
    assert lease.close_recorded is False

    lease.active_tunnels = 0
    lease.reserved_bytes = 0
    complete = asyncio.run(mod._close_lease(lease))

    assert complete["close_complete"] is True
    assert complete["active_tunnels"] == 0
    assert complete["reserved_bytes"] == 0
    assert lease.close_recorded is True


def test_lease_pump_pre_reads_only_the_remaining_provider_window(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 12
    lease = mod._create_lease(
        mgr,
        max_bytes=12,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "event")

    class Reader:
        def __init__(self):
            self.payload = b"x" * 20
            self.read_sizes = []

        async def read(self, size):
            self.read_sizes.append(size)
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

    reader = Reader()
    writer = Writer()
    asyncio.run(
        mod._pump(
            reader,
            writer,
            "www.sofascore.com",
            defaultdict(int),
            lease=lease,
            direction="down",
        )
    )

    assert reader.read_sizes == [12]
    assert len(reader.payload) == 8
    assert b"".join(writer.writes) == b"x" * 12
    assert lease.down_bytes == 12
    assert lease.total_bytes == lease.max_bytes
    assert lease.budget_exceeded is True
    assert writer.closed is True


def test_provider_connect_head_is_bounded_before_read_and_counted_exactly(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 8
    lease = mod._create_lease(
        mgr,
        max_bytes=8,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "event")

    class Reader:
        def __init__(self):
            self.payload = bytearray(b"HTTP/1.1 200 OK\r\n\r\n")
            self.reads = 0

        async def read(self, size):
            self.reads += 1
            chunk = bytes(self.payload[:size])
            del self.payload[:size]
            return chunk

    reader = Reader()
    with pytest.raises(RuntimeError, match="over-budget"):
        asyncio.run(
            mod._read_metered_provider_head(
                reader,
                lease,
                "www.sofascore.com",
            )
        )

    assert reader.reads == 8
    assert lease.down_bytes == 8
    assert lease.budget_exceeded is True


def test_durable_lease_ledger_restores_daily_and_dagrun_exact_bytes(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    lease = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "event")
    mod._account_lease_bytes(lease, "www.sofascore.com", "up", 125)
    mod._account_lease_bytes(lease, "www.sofascore.com", "down", 875)

    mod._run_up_bytes.clear()
    mod._run_down_bytes.clear()
    mod._url_up_bytes.clear()
    mod._url_down_bytes.clear()
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    restored = mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=True)

    assert restored == 2
    assert mod._run_total_bytes(lease.dagrun_key) == 1000
    assert mod._daily_total_bytes() == 1000
    assert Path(mod.LEDGER_PATH).stat().st_mode & 0o777 == 0o600
    ledger = Path(mod.LEDGER_PATH).read_text()
    assert lease.token not in ledger
    assert "u:p" not in ledger


def test_daily_budget_caps_lease_and_blocks_followup_before_upstream_pick(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.DAILY_BUDGET_BYTES = 10
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 100
    lease = mod._create_lease(
        mgr,
        max_bytes=100,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "event")
    assert lease.max_bytes == 10
    mod._account_lease_bytes(lease, "www.sofascore.com", "down", 10)
    lease.closed = True

    with pytest.raises(RuntimeError, match="daily paid-proxy budget exhausted"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=_sofascore_context(task_id="retry"),
            require_context=True,
        )
    assert mgr.calls == 1


def test_corrupt_durable_lease_ledger_fails_closed(mod):
    Path(mod.LEDGER_PATH).write_text("{broken\n")

    with pytest.raises(RuntimeError, match="line 1"):
        mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=True)


def test_sensitive_query_values_are_redacted_before_report_and_ledger(mod):
    canonical = mod._canonical_url(
        "https://www.sofascore.com/api/v1/x?token=secret&a=1&api_key=also-secret"
    )

    assert canonical == (
        "https://www.sofascore.com/api/v1/x?"
        "a=1&api_key=%5BREDACTED%5D&token=%5BREDACTED%5D"
    )
    assert "secret" not in canonical


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
