"""Offline host adapter contracts; fixtures never consult the live host."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scrapers.sofascore import history_groups_report as report

pytestmark = pytest.mark.unit
NOW = datetime(2026, 10, 15, 13, 15, tzinfo=timezone.utc)
RELEASE = "b1be619f4b7ba9aa4a908370ae98c0e5339f8551"
ROOT = Path(__file__).resolve().parents[3]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def checkpoint(mode="slots"):
    rows = {g: dict(finished=100, closed=98, remaining=2, unknown_scopes=0,
                    scopes=10, pending=1, mapping_required=1, parked=1,
                    quarantined=1, deferred=0, percent=98.0, gate_passed=False)
            for g in report.GROUPS}
    rows["1"]["unknown_scopes"] = 2
    rows["july"].update(finished=0, closed=0, remaining=0, percent=None,
                        unknown_scopes=5)
    run = dict(run_id="scheduled__fixture", plan=[], plan_digest=digest([]))
    if mode == "slots":
        run.update(mode="slots", slots={}, items={})
        run["slot_digest"] = digest(run)
    return dict(schema_version=1, campaign_id="fixture-campaign", positions={},
                observed_at=NOW.isoformat(), groups=rows, run=run)


@pytest.fixture
def inputs(tmp_path):
    release = tmp_path / "release-b1be619f"
    target = release / "scrapers/sofascore/history_controller.py"
    target.parent.mkdir(parents=True)
    target.write_bytes((ROOT / "scrapers/sofascore/history_controller.py").read_bytes())
    (release / ".git").mkdir()
    (release / ".git/HEAD").write_text(RELEASE + "\n")
    path = tmp_path / "checkpoint.json"
    path.write_text(json.dumps(checkpoint()))
    return dict(checkpoint_path=path, campaign_id="fixture-campaign",
                release_root=release, release_id=RELEASE,
                controller_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
                checked_at=NOW, max_age_seconds=3600)


def test_all_six_groups_preserve_blocked_and_unknown(inputs):
    result = report.build_report(**inputs)
    assert result["status"] == "UNKNOWN"
    assert tuple(result["groups"]) == report.GROUPS
    assert result["groups"]["1"]["status"] == "UNKNOWN"
    assert result["groups"]["2"]["status"] == "BLOCKED"
    assert "0/0" in result["summary"] and "неизвестно 5" in result["summary"]
    assert "pending 2" in result["summary"]
    assert result["observed_at"] == NOW.isoformat()
    assert result["checked_at"] == NOW.isoformat()
    assert result["release_id"] == RELEASE


def test_legacy_batch_and_qualified_empty(inputs):
    value = checkpoint("batch")
    value["groups"]["july"].update(unknown_scopes=0, gate_passed=True)
    inputs["checkpoint_path"].write_text(json.dumps(value))
    result = report.build_report(**inputs)
    assert result["groups"]["july"]["status"] == "EMPTY"
    assert result["mode"] == "batch"


@pytest.mark.parametrize("mutation", [
    lambda d: d.update(schema_version=True),
    lambda d: d.update(campaign_id="another"),
    lambda d: d.update(run=None),
    lambda d: d["run"].update(plan_digest="0" * 64),
    lambda d: d["run"].update(slot_digest="0" * 64),
    lambda d: d["run"].pop("slot_digest"),
    lambda d: d["groups"].pop("4"),
    lambda d: d["groups"]["1"].update(closed=101),
    lambda d: d["groups"]["1"].update(finished=True),
    lambda d: d["groups"]["1"].update(unknown_scopes=-1),
    lambda d: d["groups"]["1"].update(remaining=0),
    lambda d: d["groups"]["1"].update(percent=99.0),
    lambda d: d["groups"]["1"].update(gate_passed=True),
    lambda d: d["groups"]["1"].update(pending=20),
    lambda d: d["groups"]["1"].pop("mapping_required"),
    lambda d: d.update(observed_at="bad-time"),
    lambda d: d.update(observed_at="2026-10-15T13:15:00"),
    lambda d: d.update(observed_at=(NOW + timedelta(seconds=1)).isoformat()),
    lambda d: d.update(observed_at=(NOW - timedelta(seconds=3601)).isoformat()),
])
def test_bad_checkpoint_is_explicit_unknown_in_every_group(inputs, mutation):
    value = checkpoint()
    mutation(value)
    inputs["checkpoint_path"].write_text(json.dumps(value))
    result = report.build_report(**inputs)
    assert result["status"] == "UNKNOWN"
    assert result["reason"]
    assert all(row["status"] == "UNKNOWN" for row in result["groups"].values())
    assert all(f"{g} " in result["summary"] for g in report.GROUPS)
    assert "PASS" not in result["summary"]


@pytest.mark.parametrize("raw", [None, "{", "[]", '{"schema_version":1,"schema_version":1}', "NaN"])
def test_missing_corrupt_and_duplicate_json_are_unknown(inputs, raw):
    if raw is None:
        inputs["checkpoint_path"].unlink()
    else:
        inputs["checkpoint_path"].write_text(raw)
    result = report.build_report(**inputs)
    assert result["status"] == "UNKNOWN" and result["reason"]


@pytest.mark.parametrize("key,value", [
    ("release_id", "wrong"), ("release_id", "0" * 40),
    ("release_id", RELEASE[:-1] + "0"),
    ("controller_sha256", "0" * 64), ("max_age_seconds", 0),
])
def test_release_and_freshness_pins_fail_closed(inputs, key, value):
    inputs[key] = value
    assert report.build_report(**inputs)["reason"]


def test_host_function_uses_only_release_root_line(inputs, tmp_path):
    env = tmp_path / "source.env"
    env.write_text(f"SECRET_TOKEN=do-not-print\nSOFASCORE_RELEASE_ROOT='{inputs['release_root']}'\n")
    expected = report.build_report(**inputs)["summary"]
    options = dict(inputs)
    options.pop("release_root")
    assert report.host_history_groups(**options, env_path=env) == [expected]
    env.write_text("SOFASCORE_RELEASE_ROOT=/wrong\nSOFASCORE_RELEASE_ROOT=/another\n")
    lines = report.host_history_groups(**options, env_path=env)
    assert "UNKNOWN" in lines[0] and "do-not-print" not in lines[0]


def test_cli_fresh_process_has_no_network_or_child_collection(inputs, tmp_path):
    script = ROOT / "scripts/sofascore_history_groups_report.py"
    arguments = ["--checkpoint", str(inputs["checkpoint_path"]), "--campaign-id", inputs["campaign_id"],
                 "--release-root", str(inputs["release_root"]), "--release-id", RELEASE,
                 "--controller-sha256", inputs["controller_sha256"], "--checked-at", NOW.isoformat(), "--json"]
    # The launched CLI is allowed; collection subprocesses and sockets inside it
    # are forbidden before any producer module is imported.
    guard = """
import runpy, sys
def audit(event, args):
    if event.startswith(('socket.', 'subprocess.')) or event in ('os.system', 'os.posix_spawn', 'os.fork'):
        raise RuntimeError('forbidden collection: ' + event)
sys.addaudithook(audit)
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name='__main__')
"""
    outcome = subprocess.run([sys.executable, "-B", "-c", guard, str(script), *arguments],
                             cwd=tmp_path, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                             capture_output=True, text=True, timeout=15)
    assert outcome.returncode == 2, outcome.stderr
    assert json.loads(outcome.stdout)["groups"]["1"]["unknown_scopes"] == 2
    assert not outcome.stderr


def test_valid_groups_can_pass_without_unknown(inputs):
    value = checkpoint()
    for row in value["groups"].values():
        row.update(closed=row["finished"], remaining=0, unknown_scopes=0,
                   percent=100.0 if row["finished"] else None, gate_passed=True)
    inputs["checkpoint_path"].write_text(json.dumps(value))
    result = report.build_report(**inputs)
    assert result["status"] == "PASS"
    assert result["groups"]["july"]["status"] == "EMPTY"
