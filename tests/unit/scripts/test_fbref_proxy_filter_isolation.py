"""Cross-source guards for the dedicated FBref proxy runtime."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import yaml

from scrapers.sofascore.runtime_fingerprint import runtime_files


ROOT = Path(__file__).resolve().parents[3]
SHARED_FILTER = ROOT / "scripts/proxy_filter/filter_proxy.py"
FBREF_FILTER = ROOT / "scripts/fbref_proxy/filter_proxy.py"
SHARED_FILTER_SHA256 = (
    # Bumped for the static workload policy that replaced the paid canary
    # (#1245).  FBref keeps its own filter and stays out of ``runtime_files`` —
    # both asserted below.
    "9cfd47e56b87824c1f39f40982a53a44eeb50dcfb4f7e879929c73f5a9171634"
)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fbref_runtime_is_separate_from_paid_sofascore_filter_bytes():
    assert hashlib.sha256(SHARED_FILTER.read_bytes()).hexdigest() == (
        SHARED_FILTER_SHA256
    )
    assert FBREF_FILTER.is_file()
    assert "scripts/fbref_proxy/filter_proxy.py" not in runtime_files(ROOT)


def test_fbref_runtime_is_still_sealed_by_the_internal_release_contract():
    lock = json.loads(
        (ROOT / "scrapers/whoscored/runtime_contract.lock").read_text(
            encoding="utf-8"
        )
    )
    assert lock["files"]["scripts/fbref_proxy/filter_proxy.py"] == (
        hashlib.sha256(FBREF_FILTER.read_bytes()).hexdigest()
    )


def test_only_fbref_services_execute_the_fbref_specific_runtime():
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    command = compose["services"]["fbref_proxy_filter"]["command"]
    assert "/opt/airflow/scripts/fbref_proxy/filter_proxy.py" in command
    assert command[command.index("--source-mode") + 1] == "fbref-only"

    acceptance = yaml.safe_load(
        (ROOT / "deploy/fbref/acceptance.compose.yaml").read_text(encoding="utf-8")
    )
    acceptance_command = acceptance["services"][
        "fbref_acceptance_proxy_filter"
    ]["command"]
    assert "/opt/airflow/scripts/fbref_proxy/filter_proxy.py" in (
        acceptance_command
    )
    assert acceptance_command[acceptance_command.index("--source-mode") + 1] == (
        "fbref-only"
    )

    for service_name in ("proxy_filter",):
        shared_command = compose["services"][service_name]["command"]
        assert "/opt/airflow/scripts/proxy_filter/filter_proxy.py" in (
            shared_command
        )
        assert "/opt/airflow/scripts/fbref_proxy/filter_proxy.py" not in (
            shared_command
        )


def test_fbref_task_switches_are_scheduler_only_and_explicitly_admitted():
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    controls = {
        "FBREF_BATCH_PERSIST",
        "FBREF_BATCH_PERSIST_MATCHES",
        "FBREF_BATCH_PERSIST_MAX_CELLS",
        "FBREF_PERSISTENT_HTTP_SESSION",
    }
    scheduler_environment = compose["services"]["airflow-scheduler"][
        "environment"
    ]
    assert controls <= set(scheduler_environment)
    for service_name, service in compose["services"].items():
        if not service_name.startswith("airflow-") or service_name == (
            "airflow-scheduler"
        ):
            continue
        environment = service.get("environment", {})
        assert controls.isdisjoint(environment), service_name


def test_fbref_service_rejects_non_fbref_context_before_selecting_an_upstream():
    fbref = _load(FBREF_FILTER, "fbref_filter_source_guard")
    fbref.SOURCE_MODE = "fbref-only"
    fbref.MAX_LEASE_BYTES = 1000
    fbref.MAX_LEASE_TTL_SECONDS = 60

    class Manager:
        calls = 0

        def get_proxy(self):
            self.calls += 1
            raise AssertionError("non-FBref lease reached provider selection")

    manager = Manager()
    try:
        fbref._create_lease(
            manager,
            max_bytes=100,
            ttl_seconds=30,
            metadata={
                "dag_id": "dag_ingest_sofascore",
                "run_id": "run-a",
                "task_id": "task-a",
                "canonical_url": "https://www.sofascore.com/",
            },
            require_context=True,
        )
    except ValueError as exc:
        assert "rejects every other source" in str(exc)
    else:
        raise AssertionError("FBref-only runtime accepted another source")
    assert manager.calls == 0


def test_fbref_copy_preserves_non_fbref_selection_and_host_contracts():
    shared = _load(SHARED_FILTER, "shared_filter_runtime")
    fbref = _load(FBREF_FILTER, "fbref_filter_runtime")
    upstreams = (
        ("pool.invalid", 10000, "session-a", "password-a"),
        ("pool.invalid", 10000, "session-b", "password-b"),
    )
    for upstream in upstreams:
        assert fbref._upstream_fingerprint(upstream) == (
            shared._upstream_fingerprint(upstream)
        )
    for source, host, port in (
        ("sofascore", "api.sofascore.com", 443),
        ("whoscored", "www.whoscored.com", 443),
        ("transfermarkt", "www.transfermarkt.com", 443),
    ):
        lease = type("Lease", (), {"source": source})()
        assert fbref._lease_host_allowed(lease, host, port) == (
            shared._lease_host_allowed(lease, host, port)
        )
