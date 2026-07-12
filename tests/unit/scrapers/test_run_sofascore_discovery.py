"""CLI behavior for direct-only SofaScore discovery."""

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
    assert "without browser or proxy" in result.stdout
    assert os.access(script, os.X_OK)


@pytest.mark.unit
def test_make_one_shots_use_a_distinct_registry_mount():
    makefile = (
        Path(__file__).resolve().parents[3] / "Makefile"
    ).read_text(encoding="utf-8")

    # Compose keeps the inherited /opt/airflow registry mount read-only even
    # when a run-level mount reuses that target. A distinct path is required
    # for the explicitly writable discovery one-shot.
    assert "configs/sofascore:/work/sofascore:rw" in makefile
    assert "configs/sofascore:/work/sofascore:ro" in makefile
    assert makefile.count("--registry /work/sofascore/tournaments.json") == 2


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
