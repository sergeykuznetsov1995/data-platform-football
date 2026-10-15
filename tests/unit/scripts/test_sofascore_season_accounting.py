"""Atomic saved-input CLI; source/SQL services are forbidden by fixtures."""
import json
import os
import socket
import subprocess
import sys
from pathlib import Path

from scripts import sofascore_season_accounting as cli
from scrapers.sofascore.season_accounting import PUBLICATION_DEFINITION, digest


def saved_input():
    inventory = {"observed_at": "2026-10-15T00:00:00Z", "scopes": [
        {"tournament_id": 1, "season_id": 2, "finished": 5, "closed": 2,
         "schedule_complete": True, "ongoing": False}]}
    return {"observations": [{"observation_id": "baseline", "identity": {
        "campaign_id": "campaign", "snapshot_id": "snapshot"}, "release": "release-a",
        "roster": [{"tournament_id": 1, "season_id": 2, "group": "2"}], "roster_complete": True,
        "observed_at": inventory["observed_at"], "inventory": inventory,
        "publication_evidence": {"definition": PUBLICATION_DEFINITION,
                                 "inventory_digest": digest(inventory)}}]}


def arguments(tmp_path):
    return ["--input", str(tmp_path / "input.json"), "--journal", str(tmp_path / "journal.json"),
            "--results", str(tmp_path), "--output", str(tmp_path / "output.json")]


def test_cli_durable_replay_offline_and_corruption_refusal(tmp_path, monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("offline CLI attempted network access")
    monkeypatch.setattr(socket, "socket", no_network)
    (tmp_path / "input.json").write_text(json.dumps(saved_input()))
    assert cli.main(arguments(tmp_path)) == 0
    raw = (tmp_path / "journal.json").read_bytes()
    assert cli.main(arguments(tmp_path)) == 0
    assert (tmp_path / "journal.json").read_bytes() == raw
    report = json.loads((tmp_path / "output.json").read_text())
    assert report["observations"] == 1
    assert report["series"]["verdict"] == "UNKNOWN"
    assert report["source_requests"] == 0
    (tmp_path / "journal.json").write_text(raw.decode().replace('"open"', '"closed"'))
    corrupted = (tmp_path / "journal.json").read_bytes()
    assert cli.main(arguments(tmp_path)) == 2
    assert (tmp_path / "journal.json").read_bytes() == corrupted


def test_failed_atomic_replace_preserves_prior_journal_and_cleans_temp(tmp_path, monkeypatch):
    (tmp_path / "input.json").write_text(json.dumps(saved_input()))
    assert cli.main(arguments(tmp_path)) == 0
    before = (tmp_path / "journal.json").read_bytes()
    def fail_replace(*args):
        raise OSError("injected interrupted save")
    monkeypatch.setattr(cli.os, "replace", fail_replace)
    assert cli.main(arguments(tmp_path)) == 2
    assert (tmp_path / "journal.json").read_bytes() == before
    assert sorted(p.name for p in tmp_path.glob("journal.json.*")) == ["journal.json.lock"]


def test_direct_cli_works_in_fresh_process_without_pytest_import_paths(tmp_path):
    (tmp_path / "input.json").write_text(json.dumps(saved_input()))
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    env.pop("PYTHONPATH", None)
    script = Path(cli.__file__).resolve()
    result = subprocess.run([sys.executable, str(script), *arguments(tmp_path)],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "output.json").read_text())["observations"] == 1
