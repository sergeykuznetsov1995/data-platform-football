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
import time
from collections import defaultdict, deque
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.sofascore.workload_plan import (
    WorkloadAllocation,
    _signed_plan,
    match_workload_class,
    player_workload_class,
)

# The class names are now derived from the measured production workload shape
# rather than hard-coded, but every assertion below still means "the class this
# deployment actually signs".
MATCH_WORKLOAD_CLASS = match_workload_class()
PLAYER_WORKLOAD_CLASS = player_workload_class()

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / "scripts" / "proxy_filter" / "filter_proxy.py"
_BLOCKLIST_PATH = REPO_ROOT / "configs" / "proxy_filter" / "blocklist.txt"
_COMPOSE_PATH = REPO_ROOT / "compose.yaml"
# #951 (инцидент 2026-07-17): выделенный SofaScore-шлюз вынесен в СВОЙ
# compose-проект, чтобы чужой `docker compose up` его не пересоздавал.
_SOFASCORE_GATEWAY_COMPOSE_PATH = (
    REPO_ROOT / "deploy/sofascore/gateway.compose.yaml"
)
_FBREF_ACCEPTANCE_COMPOSE_PATH = (
    REPO_ROOT / "deploy/fbref/acceptance.compose.yaml"
)
_ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"
_SCHEDULER_DOCKERFILE_PATH = (
    REPO_ROOT / "docker/images/airflow/Dockerfile.scheduler-runtime"
)
_ACCEPTANCE_DOCKERFILE_PATH = (
    REPO_ROOT / "docker/images/airflow/Dockerfile.fbref-acceptance"
)
_ACCEPTANCE_BUILD_SCRIPT_PATH = (
    REPO_ROOT / "scripts/build_fbref_acceptance_image.sh"
)


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
    # The dedicated sofascore_proxy_filter service (#951) was moved into its own
    # compose project; a breadcrumb comment now marks its old spot between the
    # shared proxy_filter and caddy — bound the slice on that breadcrumb.
    service = compose.split("  proxy_filter:\n", 1)[1].split(
        "\n  # sofascore_proxy_filter ВЫНЕСЕН", 1
    )[0]

    assert "PROXY_POOL_JSON: ${PROXY_POOL_JSON:-}" in service
    assert "PROXY_FILTER_ALLOW_FILE_FALLBACK" in service
    assert "proxys.txt:/opt/airflow/proxys.txt" not in service
    # The lease concurrency limit is operator-tunable; the serial guarantees
    # that matter are per source (SofaScore production/canary), not global.
    assert "${PROXY_FILTER_MAX_ACTIVE_LEASES:-4}" in service


def test_sofascore_has_a_dedicated_production_metered_proxy_service():
    # The gateway lives in its OWN compose project (#951, инцидент 2026-07-17):
    # a foreign `docker compose up` on the shared project must not recreate it.
    gateway = _SOFASCORE_GATEWAY_COMPOSE_PATH.read_text()
    service = gateway.split("  sofascore_proxy_filter:\n", 1)[1].split(
        "\nnetworks:\n", 1
    )[0]

    # Dedicated pool secret + file fallback until the purchased pool lands.
    assert "PROXY_POOL_JSON: ${SOFASCORE_PROXY_POOL_JSON:-}" in service
    assert 'PROXY_FILTER_ALLOW_FILE_FALLBACK: "true"' in service
    assert "./proxys.txt:/opt/airflow/proxys.txt:ro" in service
    assert "http://sofascore_proxy_filter:8900" in service
    # hard-cap 0 => production signer (a >0 cap is the never-authorized canary).
    assert '\n      - --sofascore-canary-hard-cap-bytes\n      - "0"' in service
    # One active SofaScore lease at a time.
    assert '\n      - --max-active-leases\n      - "1"' in service
    # Ledger/WAL on the persistent log root, isolated from the shared gateway.
    assert (
        "/logs/sofascore_proxy_filter/sofascore_allocation_claims.jsonl"
        in service
    )
    # Isolation contract: joins the shared dp-backend network as EXTERNAL (own
    # project) and is ABSENT from the shared compose.yaml, so foreign deploys
    # can't sweep it.
    assert "external: true" in gateway
    assert "name: dp-backend" in gateway
    assert "\n  sofascore_proxy_filter:\n" not in _COMPOSE_PATH.read_text()
    assert (
        "SOFASCORE_PROXY_BUDGET_ARTIFACT:"
        "-/opt/airflow/configs/sofascore/proxy_budget_canary.json"
    ) in service


def test_fbref_has_an_isolated_metered_proxy_service():
    compose = _COMPOSE_PATH.read_text()
    service = compose.split("  fbref_proxy_filter:\n", 1)[1].split(
        "\n  proxy_filter:\n", 1
    )[0]

    assert "PROXY_POOL_JSON: \"\"" in service
    assert "PROXY_FILTER_ALLOW_FILE_FALLBACK: \"true\"" in service
    assert (
        "${FBREF_PROXY_POOL_FILE:-./proxys.txt}:"
        "/opt/airflow/proxys.txt:ro"
    ) in service
    assert "PROXY_FILTER_CONTROL_TOKEN: ${FBREF_PROXY_CONTROL_TOKEN:-}" in service
    assert "SOFASCORE_PROXY_CONTROL_TOKEN" not in service
    assert "http://fbref_proxy_filter:8900" in service
    assert "${FBREF_PROXY_DAGRUN_BUDGET_BYTES:-104857600}" in service
    assert "${FBREF_PROXY_URL_BUDGET_BYTES:-104857600}" in service
    assert '\n      - "1"' in service
    assert "/logs/fbref/proxy_filter/unused_sofascore_claims.jsonl" in service
    assert "/logs/proxy_filter/sofascore_allocation_claims.jsonl" not in service


def test_fbref_control_secret_is_explicit_in_airflow_and_example_env():
    compose = _COMPOSE_PATH.read_text()
    common = compose.split("x-airflow-common: &airflow-common", 1)[1].split(
        "services:", 1
    )[0]
    assert "FBREF_PROXY_CONTROL_TOKEN: ${FBREF_PROXY_CONTROL_TOKEN:-}" in common
    assert "FBREF_PROXY_CONTROL_TOKEN: ${SOFASCORE_PROXY_CONTROL_TOKEN:-}" not in (
        compose
    )

    example = _ENV_EXAMPLE_PATH.read_text()
    assert "\nFBREF_PROXY_CONTROL_TOKEN=\n" in example
    assert (
        "FBREF_PROXY_POOL_FILE=/root/fbref-949-runtime/proxys.txt" in example
    )


def test_fbref_acceptance_compose_is_a_separate_project_scoped_stack():
    acceptance = _FBREF_ACCEPTANCE_COMPOSE_PATH.read_text()
    proxy = acceptance.split("  fbref_acceptance_proxy_filter:\n", 1)[1].split(
        "\n  fbref_acceptance_runner:\n", 1
    )[0]
    runner = acceptance.split("  fbref_acceptance_runner:\n", 1)[1].split(
        "\nnetworks:\n", 1
    )[0]

    assert "-p fbref-acceptance-949" in acceptance
    assert "container_name:" not in acceptance
    assert "ports:" not in acceptance
    assert "build:" not in acceptance
    assert acceptance.count(
        "image: ${FBREF_ACCEPTANCE_AIRFLOW_IMAGE:?"
    ) == 2
    assert acceptance.count(
        "/opt/airflow/scripts/fbref_acceptance_entrypoint.sh"
    ) == 2
    assert acceptance.count("user: ${AIRFLOW_UID:-50000}:0") == 2
    assert "FBREF_EXPECTED_GIT_SHA: ${FBREF_ACCEPTANCE_GIT_SHA:?" in proxy
    assert (
        "FBREF_EXPECTED_IMAGE_DIGEST: ${FBREF_ACCEPTANCE_AIRFLOW_IMAGE:?"
        in proxy
    )
    assert "FBREF_IMAGE_DIGEST: ${FBREF_ACCEPTANCE_AIRFLOW_IMAGE:?" in runner
    assert (
        "FBREF_ACCEPTANCE_OUTPUT_ROOT: /opt/airflow/logs/fbref_acceptance"
        in runner
    )
    assert acceptance.count(
        ":/opt/airflow/logs/fbref_acceptance"
    ) == 2
    assert (
        "PROXY_FILTER_CONTROL_TOKEN: ${FBREF_PROXY_CONTROL_TOKEN:?" in proxy
    )
    assert "SOFASCORE_PROXY_CONTROL_TOKEN" not in acceptance
    assert "${FBREF_PROXY_POOL_FILE:?" in proxy
    assert ":/run/secrets/fbref-proxys.txt:ro" in proxy
    assert "./proxys.txt" not in proxy
    assert "http://fbref_acceptance_proxy_filter:8900" in proxy
    assert "\n      - acceptance_proxy\n" in proxy
    assert "production_backend" not in proxy
    assert "production_storage" not in proxy
    assert "FBREF_PROXY_CONTROL_URL: http://fbref_acceptance_proxy_filter:8899" in (
        runner
    )
    assert "\n      - production_backend\n" in runner
    assert "\n      - production_storage\n" in runner
    assert "/var/lib/postgresql/data:rw,noexec,nosuid,size=1g" in acceptance
    assert "name: dp-backend" in acceptance
    assert "name: dp-storage" in acceptance


def test_fbref_acceptance_image_is_built_from_one_exact_git_archive():
    dockerfile = _ACCEPTANCE_DOCKERFILE_PATH.read_text()
    builder = _ACCEPTANCE_BUILD_SCRIPT_PATH.read_text()

    assert "COPY source.tar /tmp/fbref-acceptance-source.tar" in dockerfile
    assert "sha256sum -c -" in dockerfile
    assert "rm -rf /opt/airflow/dags /opt/airflow/scrapers" in dockerfile
    assert "verify_fbref_acceptance_image.py" in dockerfile
    assert "filter_proxy.py --help" in dockerfile
    assert "org.opencontainers.image.revision" in dockerfile
    assert "COPY dags" not in dockerfile
    assert "git -C \"$repo_root\" archive --format=tar \"$git_sha\"" in builder
    assert "dags scrapers scripts configs" in builder
    assert "${git_sha}:docker/images/airflow/Dockerfile.fbref-acceptance" in (
        builder
    )
    assert "runtime_base_id=" in builder
    assert "local/fbref-acceptance-base:" in builder
    assert "docker image tag \"$runtime_base_id\" \"$base_build_ref\"" in builder
    assert "docker image rm \"$base_build_ref\"" in builder
    assert "docker image inspect" in builder
    assert "FBREF_ACCEPTANCE_AIRFLOW_IMAGE=%s" in builder


def test_fbref_scheduler_image_requires_the_pinned_fontconfig():
    dockerfile = _SCHEDULER_DOCKERFILE_PATH.read_text()
    assert (
        "test -r /opt/fbref-camoufox/fontconfig/windows/fonts.conf"
        in dockerfile
    )


def test_fbref_lease_is_scoped_metered_and_host_restricted(mod):
    assert mod.FBREF_DAG_IDS == frozenset(
        {
            "dag_ingest_fbref",
            "dag_bootstrap_fbref",
            "dag_backfill_fbref",
            "dag_accept_fbref_bronze",
        }
    )
    assert mod._source_for_dag("dag_ingest_fbref") == "fbref"
    assert mod._source_for_dag("dag_bootstrap_fbref") == "fbref"
    assert mod._source_for_dag("dag_backfill_fbref") == "fbref"
    assert mod._source_for_dag("dag_accept_fbref_bronze") == "fbref"
    assert mod._source_for_dag("dag_replay_fbref_bronze") == ""
    lease = mod.Lease(
        lease_id="fbref-lease",
        token="secret",
        upstream=("proxy.example", 10000, "user", "password"),
        created_at=0.0,
        expires_at=9999999999.0,
        max_bytes=1000,
        dag_id="dag_ingest_fbref",
        run_id="run",
        source="fbref",
    )

    assert lease.report()["meter"] == "proxy_filter_provider_path_v2"
    assert mod._lease_host_allowed(lease, "fbref.com") is True
    assert mod._lease_host_allowed(lease, "www.fbref.com") is True
    assert mod._lease_host_allowed(lease, "challenges.cloudflare.com") is True
    assert mod._lease_host_allowed(lease, "api.ipify.org") is True
    assert mod._lease_host_allowed(lease, "ipinfo.io") is False
    assert mod._lease_host_allowed(lease, "example.com") is False
    assert mod._lease_url_budget_bytes(lease) == mod.DAGRUN_BUDGET_BYTES


def test_production_airflow_enables_safe_fbref_stage_janitor():
    compose = _COMPOSE_PATH.read_text()
    common = compose.split("x-airflow-common:", 1)[1].split("\nservices:", 1)[0]

    assert (
        "FBREF_STAGE_JANITOR_MODE: ${FBREF_STAGE_JANITOR_MODE:-apply}" in common
    )


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


def test_daily_budget_is_restored_from_atomic_report(mod, tmp_path):
    out = tmp_path / "report.json"
    today = mod._utc_day()
    out.write_text(
        json.dumps({"daily": {"day": today, "up_bytes": 123, "down_bytes": 456}})
    )
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = mod._daily_reserved_bytes = 0

    mod._restore_daily_counter(str(out))

    assert mod._daily_day == today
    assert mod._daily_up_bytes == 123
    assert mod._daily_down_bytes == 456
    assert mod._daily_total_bytes() == 579


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
        lambda **kwargs: (SimpleNamespace(total_count=1), "test pool"),
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

    @property
    def total_count(self):
        return len(self._urls)


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


# --- explicit sticky leases (legacy, credential-less callers) -----------------


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
    # The shipped default is now a configurable pool of parallel leases (with
    # per-source serialization); pinning it back to one proves the global
    # concurrency ceiling is still enforced and still fails closed.
    mod.MAX_ACTIVE_LEASES = 1
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
    assert (
        mod._canonical_url("HTTPS://WWW.WHOSCORED.COM/Matches/1/Live?z=2&a=&a=1#ignored")
        == "https://www.whoscored.com/Matches/1/Live?a=&a=1&z=2"
    )


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
    first = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata)
    assert first.max_bytes == 600
    mod._account_lease_bytes(first, "www.whoscored.com", "down", 600)
    first.closed = True

    with pytest.raises(RuntimeError, match="budget exhausted"):
        mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata)

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
    lease = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata)
    mod._account_lease_bytes(lease, "www.whoscored.com", "up", 125)
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 375)

    mod._run_up_bytes.clear()
    mod._run_down_bytes.clear()
    mod._url_up_bytes.clear()
    mod._url_down_bytes.clear()
    restored = mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)

    assert restored == 2
    assert mod._run_total_bytes("dag/run") == 500
    assert (
        mod._url_total_bytes("dag/run", "https://www.whoscored.com/Matches/1/Live")
        == 500
    )


def test_corrupt_paid_byte_ledger_fails_closed_on_restore(mod):
    Path(mod.LEDGER_PATH).write_text("{broken\n")

    with pytest.raises(RuntimeError, match="line 1"):
        mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)


def test_oversized_paid_byte_ledger_event_fails_closed_on_restore(mod):
    mod.MAX_LEDGER_EVENT_BYTES = 32
    Path(mod.LEDGER_PATH).write_bytes(b'{"value":"' + b"x" * 80)

    with pytest.raises(RuntimeError, match="line 1"):
        mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)


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


def test_fbref_auth_check_proves_meter_config_without_paid_lease(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.DAILY_BUDGET_BYTES = 300_000_000
    mod.DAGRUN_BUDGET_BYTES = 104_857_600
    mod.URL_BUDGET_BYTES = 104_857_600
    mod.MAX_LEASE_BYTES = 104_857_600
    mod.MAX_LEASE_TTL_SECONDS = 7200
    mod.MAX_ACTIVE_LEASES = 1
    mod.LEASE_PROXY_URL = "http://fbref_proxy_filter:8900"

    class Reader:
        async def readexactly(self, _length):
            raise AssertionError("auth check must not read a request body")

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
    handled = asyncio.run(
        mod._handle_control(
            "GET",
            "/v1/auth-check",
            {"x-proxy-control-token": mod.CONTROL_TOKEN},
            Reader(),
            writer,
            mgr,
        )
    )
    head, body = bytes(writer.payload).split(b"\r\n\r\n", 1)
    report = json.loads(body)

    assert handled is True
    assert b"200 OK" in head
    assert report["meter"] == "proxy_filter_provider_path_v2"
    assert report["fbref_source_ready"] is True
    assert report["configured_pool_count"] == 2
    assert report["max_active_leases"] == 1
    assert mod.LEASES == {}
    assert mgr.calls == 0


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
    # Production leases MUST reach the geoip exit-probe host (#951): Camoufox's
    # geoip=True resolves the residential exit IP via api.ipify.org at browser
    # startup; blocking it aborted every production capture with InvalidProxy
    # before any data flowed. The canary lease already reaches it, so the
    # measured budget already carries the probe cost.
    assert mod._lease_host_allowed(production, "api.ipify.org") is True
    # Scope stays fail-closed: only the single exit-probe host is opened — the
    # other IP-echo fallbacks and arbitrary hosts remain blocked.
    assert mod._lease_host_allowed(production, "ipinfo.io") is False
    assert mod._lease_host_allowed(production, "checkip.amazonaws.com") is False


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


def test_exit_probe_host_is_available_to_sofascore_leases_not_anonymous(mod):
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

    # The geoip exit-probe host is reachable by BOTH sofascore lease kinds
    # (#951): canary measured it, and production needs it for Camoufox
    # geoip=True at browser startup. An anonymous (no-lease) caller stays blocked.
    assert mod._lease_host_allowed(canary, "api.ipify.org") is True
    assert mod._lease_host_allowed(production, "api.ipify.org") is True
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


# --- dead residential exit failover (#946) -----------------------------------
#
# A dead exit accepts the TCP CONNECT and then goes silent.  Before #946 the
# lease's only tunnel hung forever inside ``_read_metered_provider_head`` (no
# timeout), never draining ``active_tunnels``; the single SofaScore slot latched
# and every follow-up 429'd.  These tests drive the whole ``handle`` CONNECT path
# through a silent upstream and assert bounded failover with exact metering.


class _FakeUpstreamReader:
    """Minimal StreamReader: serves ``read(n)`` from a buffer, optionally
    blocking forever once the buffer drains (a dead but connected exit)."""

    def __init__(self, data=b"", *, block_when_empty=False):
        self.buf = bytearray(data)
        self.block_when_empty = block_when_empty

    async def read(self, size):
        if not self.buf:
            if self.block_when_empty:
                await asyncio.Event().wait()
            return b""
        chunk = bytes(self.buf[:size])
        del self.buf[:size]
        return chunk


class _FakeUpstreamWriter:
    def __init__(self):
        self.data = bytearray()
        self.closed = False

    def write(self, chunk):
        self.data.extend(chunk)

    async def drain(self):
        return None

    def close(self):
        self.closed = True


class _ClientConnectReader:
    """Client leg of one CONNECT: hands back the request head, then EOF for the
    (empty) client->upstream tunnel payload."""

    def __init__(self, header_lines):
        self.lines = deque(header_lines)

    async def readline(self):
        return self.lines.popleft() if self.lines else b""

    async def read(self, size):
        return b""


class _ClientWriter:
    def __init__(self):
        self.payload = bytearray()
        self.closed = False

    def write(self, chunk):
        self.payload.extend(chunk)

    async def drain(self):
        return None

    def close(self):
        self.closed = True


def _make_sofascore_lease(mod, mgr, *, budget=4096, endpoint="event"):
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.up_bytes = defaultdict(int)
    mod.down_bytes = defaultdict(int)
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = budget
    lease = mod._create_lease(
        mgr,
        max_bytes=budget,
        ttl_seconds=30,
        metadata=_sofascore_context(budget=budget),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, endpoint)
    return lease


def _connect_header_lines(lease):
    encoded = base64.b64encode(f"lease:{lease.token}".encode()).decode()
    return [
        b"CONNECT www.sofascore.com:443 HTTP/1.1\r\n",
        b"Host: www.sofascore.com:443\r\n",
        f"Proxy-Authorization: Basic {encoded}\r\n".encode(),
        b"\r\n",
    ]


def _expected_connect_head():
    auth = base64.b64encode(b"u:p").decode()
    return (
        b"CONNECT www.sofascore.com:443 HTTP/1.1\r\n"
        b"Host: www.sofascore.com:443\r\n"
        + f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
    )


def _shrink_failover_timeouts(mod, monkeypatch):
    monkeypatch.setattr(
        mod, "LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS", 0.02, raising=False
    )
    monkeypatch.setattr(
        mod, "LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS", 0.02, raising=False
    )


def _patch_upstream_opener(mod, monkeypatch, fake_open):
    # ``_open_upstream_connection`` is the #946 test seam; also patch the raw
    # asyncio symbol so the pre-#946 code (which lacks the seam) still exercises
    # the hang and fails by TimeoutError rather than skipping the dial.
    monkeypatch.setattr(mod, "_open_upstream_connection", fake_open, raising=False)
    monkeypatch.setattr(mod.asyncio, "open_connection", fake_open)


def test_lease_connect_failover_repins_silent_upstream_and_tunnels(mod, monkeypatch):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = _make_sofascore_lease(mod, mgr)
    expires_before = lease.expires_at
    _shrink_failover_timeouts(mod, monkeypatch)

    dead_writer = _FakeUpstreamWriter()
    live_writer = _FakeUpstreamWriter()
    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    tunnel = b"hello-tunnel"
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            return _FakeUpstreamReader(b"", block_when_empty=True), dead_writer
        return _FakeUpstreamReader(live_head + tunnel), live_writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    head = _expected_connect_head()
    assert b"200 Connection established" in bytes(client_writer.payload)
    assert bytes(client_writer.payload).endswith(tunnel)
    assert lease.upstream == ("pool.invalid", 10001, "u", "p")
    assert lease.upstream_repins == 1
    assert dead_writer.data == head
    assert live_writer.data == head
    assert dead_writer.closed is True
    # up == both CONNECT heads (one billed to the dead exit, one to the live
    # exit); down == the live exit's response head + tunnel payload only.
    assert lease.up_bytes == 2 * len(head)
    assert lease.down_bytes == len(live_head) + len(tunnel)
    assert lease.expires_at == expires_before
    assert lease.report()["upstream_repins"] == 1
    assert lease.active_tunnels == 0
    # M1: no failed attempt may leave its writer behind in tunnel_writers.
    assert lease.tunnel_writers == set()


def test_fbref_never_repins_after_paid_connect_upload(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10001"])
    now = mod._wall_time()
    lease = mod.Lease(
        lease_id="fbref-one-attempt",
        token="secret",
        upstream=("pool.invalid", 10000, "u", "p"),
        created_at=now,
        expires_at=now + 30,
        max_bytes=4096,
        dag_id="dag_ingest_fbref",
        run_id="run",
        source="fbref",
    )
    _shrink_failover_timeouts(mod, monkeypatch)
    dead_writer = _FakeUpstreamWriter()

    async def fake_open(host, port):
        return _FakeUpstreamReader(b"", block_when_empty=True), dead_writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    with pytest.raises(mod.UpstreamHeadTimeout):
        asyncio.run(
            mod._open_lease_upstream_tunnel(
                lease,
                mgr,
                target="www.fbref.com:443",
                host="www.fbref.com",
            )
        )

    auth = base64.b64encode(b"u:p").decode()
    expected = (
        b"CONNECT www.fbref.com:443 HTTP/1.1\r\n"
        b"Host: www.fbref.com:443\r\n"
        + f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
    )
    assert dead_writer.data == expected
    assert dead_writer.closed is True
    assert lease.up_bytes == len(expected)
    assert lease.down_bytes == 0
    assert lease.upstream_repins == 0
    assert lease.upstream == ("pool.invalid", 10000, "u", "p")
    assert mgr.calls == 0


def test_fbref_never_retries_zero_byte_tcp_dial_failure(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10001"])
    now = mod._wall_time()
    lease = mod.Lease(
        lease_id="fbref-one-dial",
        token="secret",
        upstream=("pool.invalid", 10000, "u", "p"),
        created_at=now,
        expires_at=now + 30,
        max_bytes=4096,
        dag_id="dag_ingest_fbref",
        run_id="run",
        source="fbref",
    )
    _shrink_failover_timeouts(mod, monkeypatch)
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        raise OSError("TCP dial failed")

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    with pytest.raises(OSError, match="TCP dial failed"):
        asyncio.run(
            mod._open_lease_upstream_tunnel(
                lease,
                mgr,
                target="www.fbref.com:443",
                host="www.fbref.com",
            )
        )

    assert opens == [("pool.invalid", 10000)]
    assert lease.total_bytes == 0
    assert lease.upstream_repins == 0
    assert lease.upstream == ("pool.invalid", 10000, "u", "p")
    assert mgr.calls == 0


def test_lease_connect_failover_is_refused_after_first_provider_payload_byte(
    mod, monkeypatch
):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = _make_sofascore_lease(mod, mgr)
    # A single down byte already arrived on the pinned exit: the exit is proven,
    # so a later silent read must fail closed (502) rather than silently re-pin.
    mod._account_lease_bytes(lease, "www.sofascore.com", "down", 1)
    _shrink_failover_timeouts(mod, monkeypatch)

    async def fake_open(host, port):
        return _FakeUpstreamReader(b"", block_when_empty=True), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    assert b"502 Bad Gateway" in bytes(client_writer.payload)
    assert lease.upstream_repins == 0
    assert lease.upstream == ("pool.invalid", 10000, "u", "p")
    assert lease.usable is True
    assert lease.active_tunnels == 0
    assert lease.tunnel_writers == set()
    assert mgr.calls == 1


def test_failover_redraws_past_the_exit_that_just_failed(mod, monkeypatch):
    # The pool draw is random, so a re-pin can hand back the exact exit that
    # just went silent.  The failover must re-draw (bounded) until the
    # replacement differs from the failed one.
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",  # initial pin (dies)
            "http://u:p@pool.invalid:10000",  # first re-draw: same dead exit
            "http://u:p@pool.invalid:10001",  # second re-draw: fresh exit
        ]
    )
    lease = _make_sofascore_lease(mod, mgr)
    _shrink_failover_timeouts(mod, monkeypatch)

    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            return _FakeUpstreamReader(b"", block_when_empty=True), _FakeUpstreamWriter()
        return _FakeUpstreamReader(live_head), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    assert b"200 Connection established" in bytes(client_writer.payload)
    assert lease.upstream == ("pool.invalid", 10001, "u", "p")
    assert lease.upstream_repins == 1  # one failover, even with the extra draw
    assert mgr.calls == 3
    assert opens[-1] == ("pool.invalid", 10001)


def test_provider_head_timeout_accounts_partial_bytes_exactly(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_sofascore_lease(mod, mgr)

    class Reader:
        def __init__(self):
            self.remaining = bytearray(b"HELLO")

        async def read(self, size):
            if self.remaining:
                chunk = bytes(self.remaining[:1])
                del self.remaining[:1]
                return chunk
            await asyncio.Event().wait()

    with pytest.raises(mod.UpstreamHeadTimeout):
        asyncio.run(
            asyncio.wait_for(
                mod._read_metered_provider_head(
                    Reader(),
                    lease,
                    "www.sofascore.com",
                    timeout_seconds=0.02,
                ),
                2.0,
            )
        )

    assert lease.down_bytes == 5
    assert lease.reserved_bytes == 0


def test_upstream_eof_before_any_head_byte_triggers_failover(mod, monkeypatch):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = _make_sofascore_lease(mod, mgr)
    _shrink_failover_timeouts(mod, monkeypatch)

    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            # Immediate EOF before any head byte: a closed/reset exit.
            return _FakeUpstreamReader(b""), _FakeUpstreamWriter()
        return _FakeUpstreamReader(live_head), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    assert b"200 Connection established" in bytes(client_writer.payload)
    assert lease.upstream_repins == 1
    assert lease.upstream == ("pool.invalid", 10001, "u", "p")


def test_lease_failover_does_not_mint_second_lease_or_bypass_serial_limit(
    mod, monkeypatch
):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = _make_sofascore_lease(mod, mgr)
    _shrink_failover_timeouts(mod, monkeypatch)

    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    opens = []
    observed = {}

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            observed["leases_during"] = len(mod.LEASES)
            try:
                mod._create_lease(
                    mgr,
                    max_bytes=4096,
                    ttl_seconds=30,
                    metadata=_sofascore_context(run_id="concurrent__x::season"),
                    require_context=True,
                )
                observed["second"] = "MINTED"
            except RuntimeError as exc:
                observed["second"] = str(exc)
            return _FakeUpstreamReader(b"", block_when_empty=True), _FakeUpstreamWriter()
        return _FakeUpstreamReader(live_head), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    assert observed["leases_during"] == 1
    assert "concurrency" in observed["second"]
    assert len(mod.LEASES) == 1
    assert lease.upstream_repins == 1


# --- metered SofaScore registry discovery (#946) ------------------------------


def _discovery_context(run_id="discovery__20260714T000000Z"):
    return {
        "dag_id": "dag_discover_sofascore_registry",
        "run_id": run_id,
        "task_id": "discover_sofascore_registry",
        "canonical_url": "https://api.sofascore.com/",
        "source": "sofascore_discovery",
        "scope": "discovery",
    }


def test_discovery_lease_is_refused_until_a_cap_is_authorized(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])

    assert mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES == 0
    assert mod._source_for_dag("dag_discover_sofascore_registry") == (
        "sofascore_discovery"
    )
    with pytest.raises(RuntimeError, match="discovery lease unavailable"):
        mod._create_lease(
            mgr,
            max_bytes=1_000_000,
            ttl_seconds=3600,
            metadata=_discovery_context(),
            require_context=True,
        )


def test_authorized_discovery_lease_is_capped_by_its_dagrun_budget(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 12 * 1024 * 1024

    lease = mod._create_lease(
        mgr,
        max_bytes=8 * 1024 * 1024,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )

    assert lease.source == "sofascore_discovery"
    assert lease.max_bytes == 8 * 1024 * 1024
    report = lease.report()
    assert report["dagrun_budget_bytes"] == 12 * 1024 * 1024
    # Discovery carries no signed plan, no allocation and no canary artifact.
    assert report["plan_digest"] == ""
    assert report["allocation_id"] == ""
    assert report["budget_artifact_id"] == ""
    # WhoScored and the other legacy sources keep the shared per-DagRun cap.
    assert mod._dagrun_budget_bytes("dag_ingest_whoscored") == 8_000_000


def test_discovery_lease_is_not_truncated_by_the_2mb_per_url_ceiling(mod):
    # Every discovery request hits one canonical API origin, so the legacy
    # per-URL ceiling would strangle a scan into 2 MB no matter its DagRun cap.
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.URL_BUDGET_BYTES = 2_000_000
    mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 16 * 1024 * 1024

    lease = mod._create_lease(
        mgr,
        max_bytes=8 * 1024 * 1024,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )

    assert lease.max_bytes == 8 * 1024 * 1024 > 2_000_000
    assert mod._lease_url_budget_bytes(lease) == 16 * 1024 * 1024
    assert lease.report()["url_budget_bytes"] == 16 * 1024 * 1024
    # And the whole-scan ceiling still applies across consecutive leases.
    mod._account_lease_bytes(lease, "api.sofascore.com", "down", 8 * 1024 * 1024)
    lease.closed = True
    second = mod._create_lease(
        mgr,
        max_bytes=8 * 1024 * 1024,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )
    assert second.max_bytes == 8 * 1024 * 1024
    mod._account_lease_bytes(second, "api.sofascore.com", "down", 8 * 1024 * 1024)
    second.closed = True
    with pytest.raises(RuntimeError, match="budget exhausted"):
        mod._create_lease(
            mgr,
            max_bytes=1024,
            ttl_seconds=3600,
            metadata=_discovery_context(),
            require_context=True,
        )


def test_discovery_scan_is_serial(mod):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    mod.MAX_ACTIVE_LEASES = 4
    mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 12 * 1024 * 1024

    first = mod._create_lease(
        mgr,
        max_bytes=1_000_000,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )
    with pytest.raises(RuntimeError, match="discovery paid-proxy concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=1_000_000,
            ttl_seconds=3600,
            metadata=_discovery_context(run_id="discovery__other"),
            require_context=True,
        )

    first.closed = True
    rotated = mod._create_lease(
        mgr,
        max_bytes=1_000_000,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )
    # The next lease in the same scan is pinned to a fresh residential exit.
    assert rotated.upstream[1] == 10001


def test_discovery_source_cannot_be_claimed_by_another_dag(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 12 * 1024 * 1024

    with pytest.raises(ValueError, match="source does not match dag_id"):
        mod._create_lease(
            mgr,
            max_bytes=1_000_000,
            ttl_seconds=3600,
            metadata={
                **_discovery_context(),
                "dag_id": "dag_ingest_sofascore",
            },
            require_context=True,
        )


def test_discovery_budget_is_reported_by_health_and_defaults_to_disabled():
    compose = _COMPOSE_PATH.read_text()
    service = compose.split("  proxy_filter:\n", 1)[1].split("\n  caddy:\n", 1)[0]
    source = _SCRIPT_PATH.read_text()

    assert "--sofascore-discovery-dagrun-budget-bytes" in service
    assert (
        "${PROXY_FILTER_SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES:-0}" in service
    )
    assert '"sofascore_discovery_enabled"' in source
    assert '"sofascore_discovery_dagrun_budget_bytes"' in source
    env_example = (REPO_ROOT / ".env.example").read_text()
    assert "PROXY_FILTER_SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES=0" in env_example


# --- FBref browser-phase lease cap extension ---------------------------------


def _fbref_context(**values):
    context = {
        "source": "fbref",
        "dag_id": "dag_ingest_fbref",
        "run_id": "manual__fbref-cap",
        "task_id": "run_live_waves",
        "canonical_url": "https://fbref.com/en/",
    }
    context.update(values)
    return context


def _make_fbref_lease(mod, mgr, *, max_bytes=1000):
    mod.DAILY_BUDGET_BYTES = 5000
    mod.DAGRUN_BUDGET_BYTES = 5000
    mod.URL_BUDGET_BYTES = 5000
    mod.MAX_LEASE_BYTES = 5000
    return mod._create_lease(
        mgr,
        max_bytes=max_bytes,
        ttl_seconds=30,
        metadata=_fbref_context(),
        require_context=True,
    )


def test_fbref_absolute_http_replaces_browser_lease_auth_with_provider_auth(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    client_auth = base64.b64encode(f"lease:{lease.token}".encode()).decode()
    provider_auth = base64.b64encode(b"u:p").decode()
    target = "http://www.fbref.com/en/"
    upstream_reader = _FakeUpstreamReader(
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"
    )
    upstream_writer = _FakeUpstreamWriter()

    async def fake_open(host, port):
        assert (host, port) == ("pool.invalid", 10000)
        return upstream_reader, upstream_writer

    monkeypatch.setattr(mod, "_open_upstream_connection", fake_open)
    client_writer = _ClientWriter()
    asyncio.run(
        mod.handle(
            _ClientConnectReader(
                [
                    f"GET {target} HTTP/1.1\r\n".encode(),
                    b"Host: www.fbref.com\r\n",
                    b"User-Agent: browser-test\r\n",
                    f"Proxy-Authorization: Basic {client_auth}\r\n".encode(),
                    b"\r\n",
                ]
            ),
            client_writer,
            mgr,
            require_lease=True,
        )
    )

    forwarded = bytes(upstream_writer.data)
    assert forwarded == (
        f"GET {target} HTTP/1.1\r\n".encode()
        + b"Host: www.fbref.com\r\n"
        + b"User-Agent: browser-test\r\n"
        + f"Proxy-Authorization: Basic {provider_auth}\r\n\r\n".encode()
    )
    assert forwarded.count(b"Proxy-Authorization:") == 1
    assert client_auth.encode() not in forwarded
    assert lease.token.encode() not in forwarded
    assert b"lease:" not in forwarded
    assert lease.active_tunnels == 0
    assert lease.tunnel_writers == set()


def test_fbref_drained_lease_extension_is_durable_before_cap_mutation(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    mod._account_lease_bytes(lease, "www.fbref.com", "down", 200)

    report = mod._extend_fbref_lease(lease, 3000)

    assert lease.max_bytes == 3000
    assert report["max_bytes"] == 3000
    events = [json.loads(line) for line in Path(mod.LEDGER_PATH).read_text().splitlines()]
    extended = events[-1]
    assert extended["event_type"] == "lease_extended"
    assert extended["previous_max_bytes"] == 1000
    assert extended["max_bytes"] == 3000
    assert extended["lease_total_bytes"] == 200


@pytest.mark.parametrize(
    "state",
    [
        "active_tunnels",
        "reserved_bytes",
        "current_request_id",
        "current_endpoint",
        "closed",
        "expired",
    ],
)
def test_fbref_lease_extension_refuses_non_idle_or_closed_state(mod, state):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    if state == "expired":
        lease.expires_at = 0
    elif state in {"current_request_id", "current_endpoint"}:
        setattr(lease, state, "request-1")
    else:
        setattr(lease, state, 1 if state != "closed" else True)

    with pytest.raises(RuntimeError):
        mod._extend_fbref_lease(lease, 2000)

    assert lease.max_bytes == 1000


def test_fbref_lease_extension_rejects_shared_budget_overcommit(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    mod._account_lease_bytes(lease, "www.fbref.com", "down", 200)
    mod._run_down_bytes[lease.dagrun_key] += 3500

    with pytest.raises(RuntimeError, match="remaining shared budget"):
        mod._extend_fbref_lease(lease, 1600)

    assert mod._fbref_lease_extension_ceiling(lease) == 1500
    assert lease.max_bytes == 1000


def test_fbref_lease_extension_ledger_failure_leaves_old_hard_cap(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)

    def fail_ledger(*_args, **_kwargs):
        raise OSError("fsync failed")

    monkeypatch.setattr(mod, "_append_budget_event", fail_ledger)

    with pytest.raises(OSError, match="fsync failed"):
        mod._extend_fbref_lease(lease, 2000)

    assert lease.max_bytes == 1000


def test_fbref_extend_control_requires_both_control_and_lease_tokens(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    request = json.dumps({"max_bytes": 2000}).encode()

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

    for headers in (
        {"authorization": f"Bearer {lease.token}"},
        {"x-proxy-control-token": mod.CONTROL_TOKEN, "authorization": "Bearer bad"},
    ):
        writer = Writer()
        asyncio.run(
            mod._handle_control(
                "POST",
                f"/v1/leases/{lease.lease_id}/extend",
                {"content-length": str(len(request)), **headers},
                Reader(),
                writer,
                mgr,
            )
        )
        assert b"401 Unauthorized" in writer.payload
        assert lease.max_bytes == 1000

    writer = Writer()
    asyncio.run(
        mod._handle_control(
            "POST",
            f"/v1/leases/{lease.lease_id}/extend",
            {
                "content-length": str(len(request)),
                "x-proxy-control-token": mod.CONTROL_TOKEN,
                "authorization": f"Bearer {lease.token}",
            },
            Reader(),
            writer,
            mgr,
        )
    )
    head, body = bytes(writer.payload).split(b"\r\n\r\n", 1)
    assert b"200 OK" in head
    assert json.loads(body)["max_bytes"] == 2000
    assert lease.max_bytes == 2000


def test_fbref_proxy_hard_stops_an_oversized_browser_phase_transfer(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr, max_bytes=12)

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
            "www.fbref.com",
            defaultdict(int),
            lease=lease,
            direction="down",
        )
    )

    assert reader.read_sizes == [12]
    assert reader.payload == b"x" * 8
    assert b"".join(writer.writes) == b"x" * 12
    assert lease.total_bytes == lease.max_bytes == 12
    assert lease.budget_exceeded is True
    assert writer.closed is True
