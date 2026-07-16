"""CLI behavior for SofaScore discovery: direct by default, metered on opt-in."""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest


@pytest.fixture
def module():
    mod = importlib.import_module("dags.scripts.run_sofascore_discovery")
    return importlib.reload(mod)


@pytest.fixture
def registry(tmp_path):
    source = Path("configs/sofascore/tournaments.json")
    target = tmp_path / "tournaments.json"
    target.write_bytes(source.read_bytes())
    return target


class _Client:
    def __init__(self):
        self.closed = False

    @property
    def stats(self):
        return {
            "requests": 2,
            "direct_response_bytes": 123,
            "paid_proxy_bytes": 0,
            "browser_sessions": 0,
        }

    def close(self):
        self.closed = True


def _changed_document(existing):
    changed = deepcopy(existing)
    changed["tournaments"][0]["name"] += " refreshed"
    return changed


def _patch_success(module, monkeypatch, *, changed):
    client = _Client()
    monkeypatch.setattr(module, "DirectSofaScoreClient", lambda: client)

    def discover(existing, actual_client):
        assert actual_client is client
        document = _changed_document(existing) if changed else deepcopy(existing)
        return document, {
            "new_tournaments": 0,
            "updated_tournaments": int(changed),
            "unchanged_tournaments": 9 - int(changed),
            "total_tournaments": 9,
            "total_seasons": 3,
            "catalog_tournaments": 9,
            "changed": changed,
            "traffic": client.stats,
        }

    monkeypatch.setattr(module, "discover_registry", discover)
    return client


@pytest.mark.unit
def test_script_entrypoint_is_standalone_and_executable(tmp_path):
    script = (
        Path(__file__).resolve().parents[3]
        / "dags"
        / "scripts"
        / "run_sofascore_discovery.py"
    )
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)

    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "direct JSON by default" in result.stdout
    # The metered transport exists, but it is never the default.
    assert "--transport {direct,lease-proxy}" in result.stdout
    assert os.access(script, os.X_OK)


@pytest.mark.unit
def test_make_one_shots_use_a_distinct_registry_mount():
    root = Path(__file__).resolve().parents[3]
    makefile = (root / "Makefile").read_text(encoding="utf-8")
    core_requirements = (
        root / "docker/images/airflow/requirements.txt"
    ).read_text(encoding="utf-8")

    # Compose keeps the inherited /opt/airflow registry mount read-only even
    # when a run-level mount reuses that target. A distinct path is required
    # for the explicitly writable discovery one-shot.
    assert "configs/sofascore:/work/sofascore:rw" in makefile
    assert "configs/sofascore:/work/sofascore:ro" in makefile
    # direct write, direct check, and the metered opt-in one-shot.
    assert makefile.count("--registry /work/sofascore/tournaments.json") == 3
    discovery_targets = makefile.split("sofascore-discovery:\n", 1)[1].split(
        "\n# Show Web UI URLs", 1
    )[0]
    assert discovery_targets.count("airflow-webserver") == 3
    assert "airflow-scheduler" not in discovery_targets
    assert core_requirements.splitlines().count(
        "tls-client-python==1.15.1"
    ) == 1


@pytest.mark.unit
def test_metered_make_target_cannot_run_without_an_explicit_byte_cap():
    makefile = (
        Path(__file__).resolve().parents[3] / "Makefile"
    ).read_text(encoding="utf-8")
    target = makefile.split("sofascore-discovery-lease:\n", 1)[1].split(
        "\nsofascore-discovery-check:", 1
    )[0]

    assert "--transport lease-proxy" in target
    assert "--budget-cap-bytes $(BUDGET_CAP_BYTES)" in target
    assert 'test -n "$(BUDGET_CAP_BYTES)"' in target
    # The zero-paid-byte targets stay direct.
    assert "--transport" not in makefile.split("sofascore-discovery:\n", 1)[1].split(
        "\n# Metered variant", 1
    )[0]


@pytest.mark.unit
def test_normal_run_atomically_updates_registry_and_report(
    module, monkeypatch, registry, tmp_path,
):
    report = tmp_path / "report.json"
    client = _patch_success(module, monkeypatch, changed=True)

    assert module.main([
        "--registry", str(registry), "--output", str(report),
    ]) == 0

    written = json.loads(registry.read_text())
    result = json.loads(report.read_text())
    assert written["tournaments"][0]["name"].endswith(" refreshed")
    assert result["status"] == "success"
    assert result["changed"] is True
    assert result["written"] is True
    assert result["traffic"]["paid_proxy_bytes"] == 0
    assert client.closed is True


@pytest.mark.unit
def test_dry_run_reports_drift_without_writing(
    module, monkeypatch, registry, tmp_path,
):
    before = registry.read_bytes()
    report = tmp_path / "report.json"
    _patch_success(module, monkeypatch, changed=True)

    assert module.main([
        "--registry", str(registry), "--output", str(report), "--dry-run",
    ]) == 0

    assert registry.read_bytes() == before
    result = json.loads(report.read_text())
    assert result["status"] == "success"
    assert result["changed"] is True
    assert result["written"] is False


@pytest.mark.unit
@pytest.mark.parametrize("changed, expected", [(False, 0), (True, 2)])
def test_check_mode_is_ci_friendly(
    module, monkeypatch, registry, tmp_path, changed, expected,
):
    before = registry.read_bytes()
    report = tmp_path / "report.json"
    _patch_success(module, monkeypatch, changed=changed)

    assert module.main([
        "--registry", str(registry), "--output", str(report), "--check",
    ]) == expected

    assert registry.read_bytes() == before
    result = json.loads(report.read_text())
    assert result["status"] == (
        "changes_detected" if changed else "success"
    )


@pytest.mark.unit
def test_403_failure_keeps_registry_and_reports_exact_zero_paid_bytes(
    module, monkeypatch, registry, tmp_path,
):
    from scrapers.sofascore.discovery import DiscoveryHTTPError

    before = registry.read_bytes()
    report = tmp_path / "report.json"
    client = _Client()
    monkeypatch.setattr(module, "DirectSofaScoreClient", lambda: client)

    def blocked(existing, actual_client):
        raise DiscoveryHTTPError("HTTP 403 direct", status_code=403)

    monkeypatch.setattr(module, "discover_registry", blocked)

    assert module.main([
        "--registry", str(registry), "--output", str(report),
    ]) == 1

    assert registry.read_bytes() == before
    result = json.loads(report.read_text())
    assert result["status"] == "failed"
    assert "403" in result["errors"][0]
    assert result["traffic"]["paid_proxy_bytes"] == 0
    assert result["traffic"]["browser_sessions"] == 0


@pytest.mark.unit
def test_discovery_error_redacts_a_lease_proxy_token(
    module, monkeypatch, registry, tmp_path,
):
    # Security: a metered lease proxy URL is http://lease:<token>@host:port. If a
    # TLS transport folds it into an error, the token must not reach the run
    # report (or the Airflow log).
    from scrapers.sofascore.discovery import DiscoveryHTTPError

    token = "s3cr3t-lease-token"
    report = tmp_path / "report.json"
    client = _Client()
    monkeypatch.setattr(module, "DirectSofaScoreClient", lambda: client)

    def leaky(existing, actual_client):
        raise DiscoveryHTTPError(
            "metered SofaScore request failed after 3 attempts: /x: Cannot "
            f"connect to proxy http://lease:{token}@residential.example:8899"
        )

    monkeypatch.setattr(module, "discover_registry", leaky)

    assert module.main([
        "--registry", str(registry), "--output", str(report),
    ]) == 1

    result = json.loads(report.read_text())
    assert result["status"] == "failed"
    assert token not in json.dumps(result)
    assert "****:****@residential.example:8899" in result["errors"][0]


@pytest.mark.unit
def test_concurrent_activation_is_preserved_and_run_fails_for_retry(
    module, monkeypatch, registry, tmp_path,
):
    report = tmp_path / "report.json"
    client = _Client()
    monkeypatch.setattr(module, "DirectSofaScoreClient", lambda: client)

    def discover(existing, actual_client):
        assert actual_client is client
        concurrent = deepcopy(existing)
        laliga = next(
            item for item in concurrent["tournaments"]
            if item["unique_tournament_id"] == 8
        )
        laliga["enabled"] = True
        registry.write_text(json.dumps(concurrent), encoding="utf-8")
        return _changed_document(existing), {
            "changed": True,
            "traffic": client.stats,
        }

    monkeypatch.setattr(module, "discover_registry", discover)

    assert module.main([
        "--registry", str(registry), "--output", str(report),
    ]) == 1

    current = json.loads(registry.read_text(encoding="utf-8"))
    laliga = next(
        item for item in current["tournaments"]
        if item["unique_tournament_id"] == 8
    )
    result = json.loads(report.read_text(encoding="utf-8"))
    assert laliga["enabled"] is True
    assert result["status"] == "failed"
    assert "changed during discovery" in result["errors"][0]


@pytest.mark.unit
def test_invalid_registry_fails_before_creating_transport(
    module, monkeypatch, registry, tmp_path,
):
    registry.write_text("not json")
    report = tmp_path / "report.json"
    monkeypatch.setattr(
        module,
        "DirectSofaScoreClient",
        lambda: pytest.fail("transport must not open for invalid registry"),
    )

    assert module.main([
        "--registry", str(registry), "--output", str(report),
    ]) == 1
    result = json.loads(report.read_text())
    assert result["status"] == "failed"
    assert result["traffic"]["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_output_cannot_replace_registry(module, registry):
    assert module.main([
        "--registry", str(registry), "--output", str(registry),
    ]) == 1
    # The registry remains a registry; the failure report was not allowed to
    # overwrite it because the paths are validated before source access.
    assert json.loads(registry.read_text())["schema_version"] == 2


# --- transport opt-in and the conditional zero-paid-byte gate (#946) ----------


class _MeteredClient:
    """Stand-in for LeaseProxySofaScoreClient: bytes are billed on close."""

    def __init__(self, paid_bytes=1024, close_error=None):
        self._paid_bytes = paid_bytes
        self._close_error = close_error
        self.closed = False

    @property
    def stats(self):
        return {
            "requests": 5,
            "direct_response_bytes": 0,
            "paid_proxy_bytes": self._paid_bytes if self.closed else 0,
            "browser_sessions": 0,
            "browser_navigations": 0,
            "lease_count": 2,
            "upstream_repins": 1,
        }

    def close(self):
        self.closed = True
        if self._close_error is not None:
            raise RuntimeError(self._close_error)


@pytest.mark.unit
def test_report_schema_records_the_transport_and_its_ceiling(
    module, monkeypatch, registry, tmp_path,
):
    report = tmp_path / "report.json"
    _patch_success(module, monkeypatch, changed=False)

    assert module.main([
        "--registry", str(registry), "--output", str(report), "--dry-run",
    ]) == 0

    result = json.loads(report.read_text())
    assert result["schema_version"] == 2
    assert result["transport"] == "direct"
    assert result["budget_cap_bytes"] == 0
    assert result["traffic"]["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_default_transport_never_constructs_a_metered_client(
    module, monkeypatch, registry, tmp_path,
):
    monkeypatch.setattr(
        module,
        "LeaseProxySofaScoreClient",
        lambda **kwargs: pytest.fail("direct discovery must not lease a proxy"),
    )
    _patch_success(module, monkeypatch, changed=False)

    assert module.main([
        "--registry", str(registry),
        "--output", str(tmp_path / "report.json"),
    ]) == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "argv, expected",
    [
        (["--transport", "lease-proxy"], "requires a positive --budget-cap-bytes"),
        (
            ["--transport", "lease-proxy", "--budget-cap-bytes", "0"],
            "requires a positive --budget-cap-bytes",
        ),
        (
            ["--budget-cap-bytes", "1000"],
            "only meaningful for --transport lease-proxy",
        ),
        (["--scope", "targeted"], "requires at least one --tournament-id"),
        (["--tournament-id", "8"], "--tournament-id requires --scope targeted"),
    ],
)
def test_unauthorized_or_incoherent_runs_fail_before_any_transport(
    module, monkeypatch, registry, tmp_path, argv, expected,
):
    monkeypatch.setattr(
        module,
        "LeaseProxySofaScoreClient",
        lambda **kwargs: pytest.fail("no transport may open for a rejected run"),
    )
    monkeypatch.setattr(
        module,
        "DirectSofaScoreClient",
        lambda: pytest.fail("no transport may open for a rejected run"),
    )
    report = tmp_path / "report.json"

    assert module.main([
        "--registry", str(registry), "--output", str(report), *argv,
    ]) == 1

    result = json.loads(report.read_text())
    assert result["status"] == "failed"
    assert expected in result["errors"][0]
    assert result["traffic"]["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_lease_transport_reports_actual_paid_bytes_and_lease_metrics(
    module, monkeypatch, registry, tmp_path,
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_URL", "http://proxy_filter:8899")
    client = _MeteredClient(paid_bytes=900_000)
    captured = {}

    def build(**kwargs):
        captured.update(kwargs)
        return client

    monkeypatch.setattr(module, "LeaseProxySofaScoreClient", build)
    monkeypatch.setattr(
        module,
        "DirectSofaScoreClient",
        lambda: pytest.fail("lease-proxy transport must not fall back to direct"),
    )

    def discover(existing, actual_client, **kwargs):
        assert actual_client is client
        return deepcopy(existing), {"changed": False, "traffic": client.stats}

    monkeypatch.setattr(module, "discover_registry", discover)
    report = tmp_path / "report.json"

    assert module.main([
        "--registry", str(registry), "--output", str(report), "--dry-run",
        "--transport", "lease-proxy", "--budget-cap-bytes", "1000000",
    ]) == 0

    result = json.loads(report.read_text())
    assert captured["control_url"] == "http://proxy_filter:8899"
    assert captured["budget_cap_bytes"] == 1_000_000
    assert result["transport"] == "lease-proxy"
    assert result["budget_cap_bytes"] == 1_000_000
    assert result["status"] == "success"
    # Traffic is read after the transport is closed, which is what bills the
    # final lease; the zero-paid-byte force is direct-only.
    assert client.closed is True
    assert result["traffic"]["paid_proxy_bytes"] == 900_000
    assert result["traffic"]["lease_count"] == 2
    assert result["traffic"]["upstream_repins"] == 1
    assert result["traffic"]["browser_sessions"] == 0
    assert result["traffic"]["browser_navigations"] == 0


@pytest.mark.unit
def test_run_fails_when_metered_discovery_overshoots_its_cap(
    module, monkeypatch, registry, tmp_path,
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_URL", "http://proxy_filter:8899")
    client = _MeteredClient(paid_bytes=2_000_000)
    monkeypatch.setattr(module, "LeaseProxySofaScoreClient", lambda **kwargs: client)

    def discover(existing, actual_client, **kwargs):
        return deepcopy(existing), {"changed": False, "traffic": client.stats}

    monkeypatch.setattr(module, "discover_registry", discover)
    report = tmp_path / "report.json"

    assert module.main([
        "--registry", str(registry), "--output", str(report), "--dry-run",
        "--transport", "lease-proxy", "--budget-cap-bytes", "1000000",
    ]) == 1

    result = json.loads(report.read_text())
    assert result["status"] == "failed"
    assert "exceeded the 1000000-byte cap" in result["errors"][-1]
    assert result["traffic"]["paid_proxy_bytes"] == 2_000_000


@pytest.mark.unit
def test_unclosable_metered_transport_fails_the_run(
    module, monkeypatch, registry, tmp_path,
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_URL", "http://proxy_filter:8899")
    client = _MeteredClient(paid_bytes=10, close_error="meter unavailable")
    monkeypatch.setattr(module, "LeaseProxySofaScoreClient", lambda **kwargs: client)

    def discover(existing, actual_client, **kwargs):
        return deepcopy(existing), {"changed": False, "traffic": client.stats}

    monkeypatch.setattr(module, "discover_registry", discover)
    report = tmp_path / "report.json"

    assert module.main([
        "--registry", str(registry), "--output", str(report), "--dry-run",
        "--transport", "lease-proxy", "--budget-cap-bytes", "1000000",
    ]) == 1

    result = json.loads(report.read_text())
    assert result["status"] == "failed"
    assert "meter unavailable" in result["errors"][-1]


@pytest.mark.unit
def test_targeted_scope_passes_the_operator_ids_through(
    module, monkeypatch, registry, tmp_path,
):
    client = _Client()
    monkeypatch.setattr(module, "DirectSofaScoreClient", lambda: client)
    captured = {}

    def discover(existing, actual_client, *, scope, target_tournament_ids):
        captured.update(scope=scope, ids=target_tournament_ids)
        return _changed_document(existing), {
            "changed": True,
            "scope": scope,
            "traffic": client.stats,
        }

    monkeypatch.setattr(module, "discover_registry", discover)
    report = tmp_path / "report.json"

    assert module.main([
        "--registry", str(registry), "--output", str(report),
        "--scope", "targeted",
        "--tournament-id", "8",
        "--tournament-id", "23",
        "--tournament-id", "34",
        "--tournament-id", "35",
    ]) == 0

    assert captured == {"scope": "targeted", "ids": [8, 23, 34, 35]}
    result = json.loads(report.read_text())
    assert result["status"] == "success"
    assert result["written"] is True
    # A targeted detail pass still spends zero paid bytes on the direct default.
    assert result["traffic"]["paid_proxy_bytes"] == 0
