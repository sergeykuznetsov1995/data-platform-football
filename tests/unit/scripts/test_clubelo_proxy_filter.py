"""Offline ClubElo admission checks for the filter identified in the host brief."""

import asyncio
import json
from pathlib import Path

import pytest
import yaml

from tests.unit.scripts.test_filter_proxy import _FakeManager, _load_module, _prepared_module


@pytest.fixture()
def mod(tmp_path):
    loaded = _prepared_module(_load_module(), tmp_path)
    loaded.CLUBELO_CONTROL_TOKEN = "e" * 32
    loaded.CLUBELO_DAGRUN_BUDGET_BYTES = 1_048_576
    loaded.CLUBELO_HISTORY_DAGRUN_BUDGET_BYTES = 134_217_728
    loaded.SOURCE_MODE = "shared-no-whoscored"
    return loaded


def _context(task_id="scrape_daily"):
    return {
        "source": "clubelo",
        "dag_id": "dag_ingest_clubelo",
        "run_id": "manual__clubelo",
        "task_id": task_id,
        "canonical_url": "https://clubelo.com/Ranking",
    }


def _lease(mod, **context):
    return mod._create_lease(
        _FakeManager(["http://u:p@pool.invalid:10000"]),
        max_bytes=mod.MAX_LEASE_BYTES,
        ttl_seconds=30,
        metadata={**_context(), **context},
        require_context=True,
    )


def test_clubelo_admission_and_mode_caps(mod):
    lease = _lease(mod)
    assert lease.source == "clubelo"
    assert lease.max_bytes == 1_048_576
    assert lease.report()["budget_namespace"] == "clubelo_dagrun"
    assert lease.report()["dagrun_budget_bytes"] == 1_048_576
    assert mod._source_for_dag("dag_ingest_clubelo_typo") == ""
    lease.closed = True
    history = _lease(mod, task_id="scrape_history")
    assert history.report()["dagrun_budget_bytes"] == 134_217_728
    assert history.max_bytes == mod.URL_BUDGET_BYTES == 2_000_000
    assert mod.DAGRUN_BUDGET_BYTES == 8_000_000
    assert mod.MAX_LEASE_BYTES == 24 * 1024 * 1024
    assert mod._dagrun_budget_bytes("dag_ingest_transfermarkt") == 0


@pytest.mark.parametrize("host,port,allowed", [
    ("clubelo.com", 443, True),
    ("CLUBELO.COM", 443, True),
    ("clubelo.com", 80, False),
    ("clubelo.com", 444, False),
    ("www.clubelo.com", 443, False),
    ("clubelo.com.evil.example", 443, False),
    ("evilclubelo.com", 443, False),
    ("challenges.cloudflare.com", 443, False),
    ("api.ipify.org", 443, False),
    ("www.transfermarkt.com", 443, False),
])
def test_clubelo_exact_https_allowlist(mod, host, port, allowed):
    assert mod._lease_host_allowed(_lease(mod), host, port) is allowed


@pytest.mark.parametrize("mode", ["fbref-only", "whoscored-only"])
def test_clubelo_rejected_by_dedicated_services(mod, mode):
    mod.SOURCE_MODE = mode
    with pytest.raises(ValueError, match="dedicated"):
        _lease(mod)


@pytest.mark.parametrize("task_id", ["scrape_daily", "scrape_history", "check_watch"])
def test_clubelo_disabled_mode_fails_closed(mod, task_id):
    mod.CLUBELO_DAGRUN_BUDGET_BYTES = 0
    mod.CLUBELO_HISTORY_DAGRUN_BUDGET_BYTES = 0
    with pytest.raises(RuntimeError, match="explicit mode cap"):
        _lease(mod, task_id=task_id)
    assert mod.LEASES == {}


def test_clubelo_unknown_task_fails_closed(mod):
    with pytest.raises(ValueError, match="task_id"):
        _lease(mod, task_id="scrape_history_typo")


@pytest.mark.parametrize("task_id", ["scrape_daily", "scrape_history"])
def test_clubelo_budget_survives_lease_rotation_and_ledger_reload(mod, task_id):
    mod.CLUBELO_DAGRUN_BUDGET_BYTES = 900
    mod.CLUBELO_HISTORY_DAGRUN_BUDGET_BYTES = 1200
    cap = 1200 if task_id == "scrape_history" else 900
    first = _lease(mod, task_id=task_id)
    mod._account_lease_bytes(first, "clubelo.com", "down", cap - 100)
    first.closed = True
    second = _lease(mod, task_id=task_id, canonical_url="https://clubelo.com/Results")
    assert second.max_bytes == 100
    mod._account_lease_bytes(second, "clubelo.com", "down", 100)
    second.closed = True
    restored = _prepared_module(_load_module(), Path(mod.LEDGER_PATH).parent)
    restored.CLUBELO_DAGRUN_BUDGET_BYTES = 900
    restored.CLUBELO_HISTORY_DAGRUN_BUDGET_BYTES = 1200
    restored._restore_budget_ledger(restored.LEDGER_PATH, restore_daily=True)
    assert restored._run_total_bytes(first.dagrun_key) == cap
    with pytest.raises(RuntimeError, match="budget exhausted"):
        _lease(restored, task_id=task_id, canonical_url="https://clubelo.com/Arsenal")


@pytest.mark.parametrize("token,status", [("e" * 32, 201), ("c" * 32, 401),
                                         ("t" * 32, 401), ("wrong", 401)])
def test_clubelo_control_plane_uses_dedicated_token(mod, token, status):
    body = json.dumps({**_context(), "max_bytes": 1000, "ttl_seconds": 30}).encode()
    class Reader:
        async def readexactly(self, length):
            assert length == len(body)
            return body

    reader = Reader()

    class Writer:
        payload = b""

        def write(self, data):
            self.payload += data

        async def drain(self):
            pass

        def close(self):
            pass

    writer = Writer()
    asyncio.run(mod._handle_control(
        "POST", "/v1/leases",
        {"x-proxy-control-token": token, "content-length": str(len(body))},
        reader, writer, _FakeManager(["http://u:p@pool.invalid:10000"]),
    ))
    assert int(writer.payload.split()[1]) == status
    assert len(mod.LEASES) == (1 if status == 201 else 0)


@pytest.mark.parametrize("token,daily,history,message", [
    ("", "1", "0", "at least 32"),
    ("short", "0", "1", "at least 32"),
    ("c" * 32, "1", "0", "distinct"),
    ("t" * 32, "1", "0", "distinct"),
    ("b" * 32, "1", "0", "distinct"),
    ("e" * 32, "-1", "0", "non-negative"),
])
def test_clubelo_invalid_startup_config_fails_before_network(
    mod, monkeypatch, token, daily, history, message,
):
    monkeypatch.setattr(mod.sys, "argv", ["filter_proxy.py"])
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", "c" * 32)
    monkeypatch.setenv("TM_PROXY_CONTROL_TOKEN", "t" * 32)
    monkeypatch.setenv("TM_BACKFILL_PROXY_CONTROL_TOKEN", "b" * 32)
    monkeypatch.setenv("CLUBELO_PROXY_CONTROL_TOKEN", token)
    monkeypatch.setenv("PROXY_FILTER_CLUBELO_DAGRUN_BUDGET_BYTES", daily)
    monkeypatch.setenv("PROXY_FILTER_CLUBELO_HISTORY_DAGRUN_BUDGET_BYTES", history)
    with pytest.raises(SystemExit, match=message):
        asyncio.run(mod.main())


def test_clubelo_compose_environment_wiring():
    compose = yaml.safe_load(Path("compose.yaml").read_text())
    for service in ("proxy_filter", "airflow-scheduler"):
        assert "CLUBELO_PROXY_CONTROL_TOKEN" not in compose["services"][service]["environment"]
    gateway = yaml.safe_load(Path("deploy/clubelo/gateway.compose.yaml").read_text())
    assert gateway["name"] == "clubelo-gw"
    service = gateway["services"]["clubelo_proxy_filter"]
    assert service["environment"]["PROXY_FILTER_ALLOW_FILE_FALLBACK"] == "false"
    assert "--allow-legacy-noauth" not in service["command"]
    assert "--allow-proxy-file-fallback" not in service["command"]
    assert service["working_dir"] == "/opt/clubelo-repo"
    assert service["volumes"][0]["read_only"] is True
    assert service["networks"] == ["dp-backend"]
    assert gateway["networks"]["dp-backend"]["external"] is True


def test_gateway_caps_cover_solver_and_history(mod):
    gateway = yaml.safe_load(Path("deploy/clubelo/gateway.compose.yaml").read_text())
    service = gateway["services"]["clubelo_proxy_filter"]
    def default(value):
        return int(value.rsplit(":-", 1)[1].rstrip("}"))
    command = service["command"]
    mod.MAX_LEASE_BYTES = default(command[command.index("--max-lease-mb") + 1]) * 1024 * 1024
    mod.URL_BUDGET_BYTES = default(command[command.index("--url-budget-bytes") + 1])
    mod.CLUBELO_DAGRUN_BUDGET_BYTES = default(service["environment"]["PROXY_FILTER_CLUBELO_DAGRUN_BUDGET_BYTES"])
    mod.CLUBELO_HISTORY_DAGRUN_BUDGET_BYTES = default(service["environment"]["PROXY_FILTER_CLUBELO_HISTORY_DAGRUN_BUDGET_BYTES"])
    lease = _lease(mod, task_id="scrape_history")
    assert lease.max_bytes >= 100 * 1024 * 1024
    solver_bytes = 2 * 1024 * 1024
    http_bytes = 85 * 1024 * 1024
    mod._account_lease_bytes(lease, "clubelo.com", "down", solver_bytes)
    mod._account_lease_bytes(lease, "clubelo.com", "down", http_bytes)
    assert lease.total_bytes == solver_bytes + http_bytes
    mod._account_lease_bytes(lease, "clubelo.com", "down", lease.max_bytes - lease.total_bytes)
    lease.closed = True
    with pytest.raises(RuntimeError, match="budget exhausted"):
        _lease(mod, task_id="scrape_history")
