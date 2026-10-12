from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from deploy.fbref_retirement import prepare as p, render_commit as r


@pytest.fixture
def candidate(tmp_path):
    name = "fbref_silver__dag_ingest_fbref__source"
    snapshot = {"checked_at": "2026-10-12T12:00:00Z",
                "runs": [{"id": 1, "dag_id": p.SILVER, "run_id": name, "state": "queued", "start_date": None, "end_date": None, "clear_number": 0, "creating_job_id": None}],
                "tasks": [{"dag_id": p.SILVER, "run_id": name, "task_id": "a", "map_index": -1, **{k: None for k in p.TASK_UNSET}, "try_number": 0}],
                "parents": [{"dag_id": "dag_ingest_fbref", "run_id": "source", "state": "success"}]}
    data = p.canonical(snapshot); (tmp_path / "snapshot.json").write_bytes(data)
    manifest = {"schema": 1, "kind": "fbref-unstarted-silver-rehearsal", "snapshot_sha256": p.digest(data),
                "tool_sha256": p.digest(Path(p.__file__).read_bytes()), "run_ids": [1], "run_count": 1, "task_count": 1}
    data = p.canonical(manifest); (tmp_path / "manifest.json").write_bytes(data)
    approval = {"action": r.ACTION, "manifest_sha256": p.digest(data), "renderer_sha256": p.digest(Path(r.__file__).read_bytes()),
                "window_start": "2026-10-12T12:00:00Z", "window_end": "2026-10-12T12:30:00Z", "owner_evidence": "isolated test only"}
    return tmp_path, approval


def test_exact_approval_renders_commit_without_execution(candidate):
    bundle, approval = candidate
    sql = r.render(bundle, approval, datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc))
    assert sql.endswith("COMMIT;\n") and not sql.endswith("ROLLBACK;\n")
    assert sql.count("DO $window$") == 2


@pytest.mark.parametrize("key,value", [
    ("action", "another-action"), ("manifest_sha256", "0"*64), ("renderer_sha256", "0"*64),
    ("owner_evidence", ""), ("window_end", "2026-10-12T11:59:00Z"),
    ("window_end", "2026-10-12T14:30:00Z"), ("window_start", "2026-10-12T12:00:00"),
])
def test_rejects_wrong_approval_or_window(candidate, key, value):
    bundle, approval = candidate; approval[key] = value
    with pytest.raises(ValueError):
        r.render(bundle, approval, datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc))


def test_rejects_stale_snapshot(candidate):
    bundle, approval = candidate
    with pytest.raises(ValueError, match="fresh"):
        r.render(bundle, approval, datetime(2026, 10, 12, 12, 6, tzinfo=timezone.utc))


def test_rejects_snapshot_hash_drift(candidate):
    bundle, approval = candidate; (bundle / "snapshot.json").write_bytes(b"{}")
    with pytest.raises(ValueError, match="hash drift"):
        r.render(bundle, approval, datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc))


def test_rejects_manifest_ids_drift_even_with_matching_approval(candidate):
    bundle, approval = candidate
    manifest = json.loads((bundle / "manifest.json").read_bytes()); manifest["run_ids"] = [2]
    data = p.canonical(manifest); (bundle / "manifest.json").write_bytes(data); approval["manifest_sha256"] = p.digest(data)
    with pytest.raises(ValueError, match="ids drift"):
        r.render(bundle, approval, datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc))
