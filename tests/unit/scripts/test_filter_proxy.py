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
import base64
import json
import time
from collections import defaultdict
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / "scripts" / "proxy_filter" / "filter_proxy.py"
_BLOCKLIST_PATH = REPO_ROOT / "configs" / "proxy_filter" / "blocklist.txt"
_COMPOSE_PATH = REPO_ROOT / "compose.yaml"


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


# --- production proxy-pool secret --------------------------------------------

def _pool_json(**overrides):
    entry = {
        "host": "Pool.Example.COM",
        "port": 10000,
        "username": "account-zone-production",
        "password": "test-only:p@ssword",
    }
    entry.update(overrides)
    return json.dumps([entry])


def test_proxy_pool_json_is_strictly_parsed_and_normalised(mod):
    records = mod._parse_proxy_pool_json(_pool_json())

    assert records == (
        {
            "host": "pool.example.com",
            "port": 10000,
            "username": "account-zone-production",
            "password": "test-only:p@ssword",
        },
    )


@pytest.mark.parametrize(
    "payload, expected_field",
    [
        ("", "PROXY_POOL_JSON"),
        ("not-json", "PROXY_POOL_JSON"),
        ("{}", "PROXY_POOL_JSON"),
        ("[]", "PROXY_POOL_JSON"),
        (json.dumps(["not-an-object"]), "entry"),
        (json.dumps([{"host": "pool.example"}]), "fields"),
        (_pool_json(extra="not-allowed"), "fields"),
        (_pool_json(host=" bad.example"), "host"),
        (_pool_json(host="bad_host.example"), "host"),
        (_pool_json(port=True), "port"),
        (_pool_json(port=0), "port"),
        (_pool_json(username="bad:name"), "username"),
        (_pool_json(password="bad\npassword"), "password"),
        (_pool_json(password="\ud800"), "password"),
    ],
)
def test_proxy_pool_json_rejects_invalid_shapes_without_echoing_values(
    mod, payload, expected_field
):
    with pytest.raises(mod.ProxyPoolConfigurationError) as caught:
        mod._parse_proxy_pool_json(payload)

    message = str(caught.value)
    assert expected_field in message
    assert "not-allowed" not in message
    assert "bad:name" not in message
    assert "bad password" not in message


def test_proxy_pool_json_rejects_duplicate_json_fields_without_echoing_secret(mod):
    payload = (
        '[{"host":"pool.example","port":10000,"username":"user",'
        '"password":"test-secret-one","password":"test-secret-two"}]'
    )

    with pytest.raises(mod.ProxyPoolConfigurationError) as caught:
        mod._parse_proxy_pool_json(payload)

    assert "duplicate object field" in str(caught.value)
    assert "test-secret" not in str(caught.value)


def test_proxy_pool_json_rejects_duplicate_endpoint_identity(mod):
    first = json.loads(_pool_json())[0]
    payload = json.dumps([first, {**first, "password": "different-test-password"}])

    with pytest.raises(mod.ProxyPoolConfigurationError, match="duplicates"):
        mod._parse_proxy_pool_json(payload)


def test_residential_manager_loads_env_secret_without_file_access(mod, tmp_path):
    mgr, source = mod._residential_manager(
        proxy_pool_json=_pool_json(),
        proxy_file=str(tmp_path / "does-not-exist"),
        allow_file_fallback=True,
    )

    assert source == "PROXY_POOL_JSON"
    assert mgr.total_count == 1
    assert mod._pick_upstream(mgr) == (
        "pool.example.com",
        10000,
        "account-zone-production",
        "test-only:p@ssword",
    )


def test_residential_manager_fails_closed_when_env_is_missing(mod, tmp_path):
    fallback = tmp_path / "proxys.txt"
    fallback.write_text("pool.example:10000:user:test-only-password\n")

    with pytest.raises(mod.ProxyPoolConfigurationError, match="fallback is disabled"):
        mod._residential_manager(
            proxy_pool_json=None,
            proxy_file=str(fallback),
            allow_file_fallback=False,
        )


def test_residential_manager_uses_file_only_with_explicit_opt_in(mod, tmp_path):
    fallback = tmp_path / "proxys.txt"
    fallback.write_text("pool.example:10000:user:test-only-password\n")

    mgr, source = mod._residential_manager(
        proxy_pool_json=None,
        proxy_file=str(fallback),
        allow_file_fallback=True,
    )

    assert source == "explicit file fallback"
    assert mgr.total_count == 1


def test_malformed_env_never_silently_falls_back_to_file(mod, tmp_path):
    fallback = tmp_path / "proxys.txt"
    fallback.write_text("pool.example:10000:user:test-only-password\n")

    with pytest.raises(mod.ProxyPoolConfigurationError, match="valid JSON"):
        mod._residential_manager(
            proxy_pool_json="malformed-test-secret",
            proxy_file=str(fallback),
            allow_file_fallback=True,
        )


def test_proxy_filter_compose_is_env_only_by_default():
    compose = _COMPOSE_PATH.read_text()
    service = compose.split("  proxy_filter:\n", 1)[1].split("\n  caddy:\n", 1)[0]

    assert "PROXY_POOL_JSON: ${PROXY_POOL_JSON:-}" in service
    assert "PROXY_FILTER_ALLOW_FILE_FALLBACK" in service
    assert "proxys.txt:/opt/airflow/proxys.txt" not in service


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
        "total_mb",
        "daily",
        "leases",
        "dagruns",
        "allowed_hosts",
        "blocked_hosts",
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


def test_only_one_paid_lease_can_be_active(mod):
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    first = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30)

    with pytest.raises(RuntimeError, match="concurrency"):
        mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30)

    first.closed = True
    second = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30)
    assert second.upstream[1] == 10001


def test_control_plane_paid_lease_requires_airflow_identity(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])

    with pytest.raises(ValueError, match="dag_id, run_id, task_id"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={"canonical_url": "https://www.whoscored.com/x"},
            require_context=True,
        )


def test_canonical_paid_url_keeps_and_sorts_full_query(mod):
    assert mod._canonical_url(
        "HTTPS://WWW.WHOSCORED.COM/Matches/1/Live?z=2&a=&a=1#ignored"
    ) == "https://www.whoscored.com/Matches/1/Live?a=&a=1&z=2"


def test_dagrun_and_canonical_url_budgets_are_shared_across_leases(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.DAGRUN_BUDGET_BYTES = 1000
    mod.URL_BUDGET_BYTES = 600
    metadata = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "task-a",
        "canonical_url": "https://www.whoscored.com/Matches/1/Live?x=1",
    }
    first = mod._create_lease(
        mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata
    )
    assert first.max_bytes == 600
    mod._account_lease_bytes(first, "www.whoscored.com", "down", 600)
    first.closed = True

    with pytest.raises(RuntimeError, match="budget exhausted"):
        mod._create_lease(
            mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata
        )

    second = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={
            **metadata,
            "task_id": "task-b",
            "canonical_url": "https://www.whoscored.com/Matches/2/Live",
        },
    )
    assert second.max_bytes == 400


@pytest.mark.parametrize(
    "dag_id",
    [
        "dag_ingest_transfermarkt",
        "dag_discover_transfermarkt_registry",
    ],
)
def test_transfermarkt_dagruns_have_a_separate_15_mib_cap(mod, dag_id):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.URL_BUDGET_BYTES = 24 * 1024 * 1024
    metadata = {
        "dag_id": dag_id,
        "run_id": "run",
        "task_id": "task",
        "canonical_url": "https://www.transfermarkt.com/x",
    }

    lease = mod._create_lease(
        mgr,
        max_bytes=15_728_640,
        ttl_seconds=3600,
        metadata=metadata,
    )

    assert mod.DAGRUN_BUDGET_BYTES == 8_000_000
    assert mod._dagrun_budget_bytes("dag_ingest_whoscored") == 8_000_000
    assert mod._dagrun_budget_bytes(dag_id) == 15_728_640
    assert lease.max_bytes == 15_728_640
    assert lease.report()["dagrun_budget_bytes"] == 15_728_640


def test_paid_lease_rejects_ttl_above_configured_hour(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])

    lease = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=3600)
    assert lease.expires_at > time.time() + 3500
    lease.closed = True

    with pytest.raises(ValueError, match="ttl_seconds must be in 1..3600"):
        mod._create_lease(mgr, max_bytes=1000, ttl_seconds=3601)


def test_durable_byte_ledger_restores_shared_run_and_url_usage(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    metadata = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "task",
        "canonical_url": "https://www.whoscored.com/Matches/1/Live",
    }
    lease = mod._create_lease(
        mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata
    )
    mod._account_lease_bytes(lease, "www.whoscored.com", "up", 125)
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 375)

    mod._run_up_bytes.clear()
    mod._run_down_bytes.clear()
    mod._url_up_bytes.clear()
    mod._url_down_bytes.clear()
    restored = mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)

    assert restored == 2
    assert mod._run_total_bytes("dag/run") == 500
    assert mod._url_total_bytes(
        "dag/run", "https://www.whoscored.com/Matches/1/Live"
    ) == 500


def test_corrupt_paid_byte_ledger_fails_closed_on_restore(mod):
    Path(mod.LEDGER_PATH).write_text("{broken\n")

    with pytest.raises(RuntimeError, match="line 1"):
        mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)


def test_oversized_paid_byte_ledger_event_fails_closed_on_restore(mod):
    mod.MAX_LEDGER_EVENT_BYTES = 32
    Path(mod.LEDGER_PATH).write_bytes(b'{"value":"' + b"x" * 80)

    with pytest.raises(RuntimeError, match="line 1"):
        mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)


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
