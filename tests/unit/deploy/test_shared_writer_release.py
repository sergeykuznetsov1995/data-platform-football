"""Filesystem transaction tests; production paths/Docker/Git network never used."""
import fcntl
import json
import os
from pathlib import Path
import time
from unittest.mock import Mock

import pytest

from deploy.shared_writer import release as r


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    root = tmp_path / "runtime"
    for directory in ("scrapers/base", "dags", "scripts"):
        (root / directory).mkdir(parents=True)
    source = Path(__file__).resolve().parents[3] / r.WRITER
    old = source.read_bytes().replace(b'"dpf.operation":', b'"operation":')
    new = old.replace(b'"operation": "replace-identity-partition-batch"',
                      b'"dpf.operation": "replace-identity-partition-batch"')
    assert r.digest(old) == r.BEFORE and r.digest(new) == r.AFTER
    (root / r.WRITER).write_bytes(old)
    (root / "scrapers/base/runtime_contract.lock").write_bytes(b"original sealed lock\n")
    neighbor = root / "dags/neighbor.py"
    neighbor.write_bytes(b"# adjacent source\n")
    host = Mock()
    host.snapshot.return_value = {"identity": {"consumers": {"scheduler": {"image": "sha256:test"}}}}
    host.preflight.return_value = {"quiet": True}
    host.rollback_preflight.return_value = {"quiet": True}
    host.postflight.return_value = {"parsed": True}
    host.writer_hashes.side_effect = lambda expected: {"scheduler": expected}
    monkeypatch.setattr(r, "payload", lambda repo, commit: new)
    monkeypatch.setattr(r, "merged", Mock())
    monkeypatch.setattr(r, "tool_hashes", lambda: {"test": "exact tool"})
    bundle = tmp_path / "bundle"
    approval = r.prepare(bundle, root, tmp_path, "a" * 40, time.time() - 5, time.time() + 600, host)
    r.save(bundle / "rehearsal.json", r.canonical({"manifest": approval, "passed": True}))
    locks = tmp_path / "locks"
    locks.mkdir()
    for name in r.LOCKS:
        (locks / name).touch()
    return bundle, root, approval, host, locks


def phase(bundle):
    return json.loads((bundle / "state.json").read_bytes())["phase"]


def run(p, **kwargs):
    bundle, _, approval, host, locks = p
    return r.execute(bundle, approval, host, state=locks, **kwargs)


def test_atomic_apply_neighbors_metadata_and_guarded_rollback(prepared, monkeypatch):
    bundle, root, approval, host, _ = prepared
    before = r.closure(root)
    inode = (root / r.WRITER).stat().st_ino
    old_handle = (root / r.WRITER).open("rb")
    real_replace = os.replace
    observations = []

    def observe(src, dst):
        if Path(dst) == root / r.WRITER:
            observations.append(r.digest((root / r.WRITER).read_bytes()))
            assert Path(src).parent == bundle  # no staging file inside watched trees
            real_replace(src, dst)
            observations.append(r.digest((root / r.WRITER).read_bytes()))
        else:
            real_replace(src, dst)

    monkeypatch.setattr(os, "replace", observe)
    run(prepared)
    assert phase(bundle) == "accepted"
    assert observations == [r.BEFORE, r.AFTER]
    assert r.digest(old_handle.read()) == r.BEFORE
    old_handle.close()
    assert (root / r.WRITER).stat().st_ino != inode
    after = r.closure(root)
    before[r.WRITER]["sha256"] = r.AFTER
    assert after == before
    host.postflight.assert_called_once()
    run(prepared, recover=True)
    assert phase(bundle) == "rolled_back"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE
    assert observations == [r.BEFORE, r.AFTER, r.AFTER, r.BEFORE]


def test_postflight_failure_restores_and_checks_old_code(prepared):
    bundle, root, _, host, _ = prepared
    host.postflight.side_effect = [RuntimeError("parse failed"), None]
    with pytest.raises(RuntimeError, match="parse failed"):
        run(prepared)
    assert phase(bundle) == "rolled_back"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE
    assert host.postflight.call_count == 2


@pytest.mark.parametrize("point", ["intent", "rename"])
def test_process_death_is_recoverable(prepared, monkeypatch, point):
    bundle, root, _, _, _ = prepared
    real = r.replace

    def die(*args):
        if point == "rename":
            real(*args)
        raise SystemExit("simulated SIGKILL boundary")

    monkeypatch.setattr(r, "replace", die)
    with pytest.raises(SystemExit):
        run(prepared)
    assert phase(bundle) == "inflight"
    assert r.digest((root / r.WRITER).read_bytes()) == (r.AFTER if point == "rename" else r.BEFORE)
    monkeypatch.setattr(r, "replace", real)
    run(prepared, recover=True)
    assert phase(bundle) == "rolled_back"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE


def test_rollback_failure_does_not_claim_acceptance(prepared):
    bundle, root, _, host, _ = prepared
    host.postflight.side_effect = RuntimeError("parse failed both times")
    with pytest.raises(RuntimeError):
        run(prepared)
    assert phase(bundle) == "blocked"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE
    host.postflight.side_effect = None
    run(prepared, recover=True)
    assert phase(bundle) == "rolled_back"


@pytest.mark.parametrize("drift", ["writer", "neighbor", "mode", "newfile", "lock"])
def test_foreign_changes_are_never_overwritten(prepared, drift):
    bundle, root, _, host, _ = prepared

    def fail(*args):
        if drift == "writer":
            (root / r.WRITER).write_bytes(b"third party change")
        elif drift == "mode":
            (root / "dags/neighbor.py").chmod(0o600)
        else:
            file = {"neighbor": "dags/neighbor.py", "newfile": "scripts/new.py",
                    "lock": "scrapers/base/runtime_contract.lock"}[drift]
            (root / file).write_bytes(b"third party change")
        raise RuntimeError("foreign drift")

    host.postflight.side_effect = fail
    with pytest.raises(RuntimeError, match="foreign drift"):
        run(prepared)
    assert phase(bundle) == "blocked"
    if drift == "writer":
        assert (root / r.WRITER).read_bytes() == b"third party change"
    else:
        assert r.digest((root / r.WRITER).read_bytes()) == r.AFTER


@pytest.mark.parametrize("problem", ["approval", "payload", "backup", "receipt", "closure", "window", "preflight", "merge", "used", "inflight"])
def test_apply_fails_closed_before_mutation(prepared, monkeypatch, problem):
    bundle, root, approval, host, locks = prepared
    if problem == "approval":
        prepared = bundle, root, "b" * 64, host, locks
    elif problem in ("payload", "backup"):
        (bundle / ("after.py" if problem == "payload" else "before.py")).write_bytes(b"corrupt")
    elif problem == "receipt":
        (bundle / "rehearsal.json").unlink()
    elif problem == "closure":
        (root / "dags/neighbor.py").write_bytes(b"drift")
    elif problem == "window":
        monkeypatch.setattr(r.time, "time", lambda: 1)
    elif problem == "preflight":
        host.preflight.side_effect = RuntimeError("busy")
    elif problem == "merge":
        monkeypatch.setattr(r, "merged", Mock(side_effect=RuntimeError("unmerged")))
    elif problem == "used":
        r.journal(bundle, approval, "accepted")
    elif problem == "inflight":
        (locks / r.MARKERS[0]).touch()
    with pytest.raises((RuntimeError, FileNotFoundError)):
        run(prepared)
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE
    host.postflight.assert_not_called()


def test_lock_contention_prevents_apply_and_preserves_lock_bytes(prepared):
    bundle, root, _, _, locks = prepared
    lock = locks / r.LOCKS[1]
    lock.write_bytes(b"another owner metadata")
    with lock.open("rb") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            run(prepared)
    assert lock.read_bytes() == b"another owner metadata"
    assert phase(bundle) == "prepared"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "parent_symlink"])
def test_unsafe_target_refused(prepared, kind):
    bundle, root, _, _, _ = prepared
    target = root / r.WRITER
    other = root / "original"
    if kind == "parent_symlink":
        (root / "scrapers/base").rename(root / "base-real")
        (root / "scrapers/base").symlink_to(root / "base-real", target_is_directory=True)
    else:
        target.rename(other)
        if kind == "symlink":
            target.symlink_to(other)
        else:
            os.link(other, target)
    with pytest.raises(RuntimeError):
        run(prepared)
    assert r.digest(target.read_bytes()) == r.BEFORE
    assert phase(bundle) == "prepared"


def test_rehearsal_uses_only_copy_and_saves_receipt(prepared, monkeypatch):
    bundle, root, approval, _, _ = prepared
    before = r.closure(root)
    (bundle / "rehearsal.json").unlink()
    calls = []

    def fake_run(args, **kwargs):
        if args[1] == "rm":
            return Mock(returncode=0)
        calls.append(args)
        assert "--network" in args and args[args.index("--network") + 1] == "none"
        assert f"src={root}," not in " ".join(args)
        return Mock(returncode=0, stdout=b"passed", stderr=b"")

    monkeypatch.setattr(r.subprocess, "run", fake_run)
    r.rehearse(bundle, approval)
    assert len(calls) == 3
    assert r.closure(root) == before
    assert r.closure(bundle / "rehearsal-runtime") == before
    assert json.loads((bundle / "rehearsal.json").read_bytes()) == {"manifest": approval, "passed": True}


def test_rehearsal_failure_never_creates_receipt(prepared, monkeypatch):
    bundle, root, approval, _, _ = prepared
    (bundle / "rehearsal.json").unlink()
    monkeypatch.setattr(r.subprocess, "run", lambda *a, **kw: Mock(returncode=78, stdout=b"", stderr=b"anchor failed"))
    with pytest.raises(RuntimeError, match="isolated startup failed"):
        r.rehearse(bundle, approval)
    assert not (bundle / "rehearsal.json").exists()
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE


@pytest.mark.parametrize("after_rename", [False, True])
def test_publication_io_error_restores_original(prepared, monkeypatch, after_rename):
    bundle, root, _, _, _ = prepared
    original = os.replace
    attempted = False

    def fail_once(source, target):
        nonlocal attempted
        if Path(target) == root / r.WRITER and not attempted:
            attempted = True
            if after_rename:
                original(source, target)
            raise OSError("simulated publication I/O failure")
        return original(source, target)

    monkeypatch.setattr(os, "replace", fail_once)
    with pytest.raises(OSError, match="publication I/O failure"):
        run(prepared)
    assert phase(bundle) == "rolled_back"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE
    records = [json.loads(line)["phase"] for line in (bundle / "journal.jsonl").read_text().splitlines()]
    assert records == ["prepared", "inflight", "rolling_back", "rolled_back"]


def test_window_expiry_during_preflight_prevents_publication(prepared, monkeypatch):
    bundle, root, _, host, _ = prepared
    end = json.loads((bundle / "manifest.json").read_bytes())["end"]

    def expire(baseline):
        monkeypatch.setattr(r.time, "time", lambda: end + 1)
        return {"quiet": True}

    host.preflight.side_effect = expire
    with pytest.raises(RuntimeError, match="window expired"):
        run(prepared)
    assert phase(bundle) == "prepared"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE


def test_missing_real_consumer_hash_prevents_acceptance(prepared):
    bundle, root, _, host, _ = prepared
    host.writer_hashes.side_effect = [{"before": r.BEFORE}, RuntimeError("consumer mismatch"), {"rollback": r.BEFORE}]
    with pytest.raises(RuntimeError, match="consumer mismatch"):
        run(prepared)
    assert phase(bundle) == "rolled_back"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE


def test_recovery_never_claims_success_after_new_neighbor_drift(prepared):
    bundle, root, _, host, _ = prepared
    run(prepared)

    def drift(*args):
        (root / "dags/neighbor.py").write_bytes(b"foreign concurrent edit")
        return {"parsed": True}

    host.postflight.side_effect = drift
    with pytest.raises(RuntimeError, match="drift"):
        run(prepared, recover=True)
    assert phase(bundle) == "blocked"
    assert (root / "dags/neighbor.py").read_bytes() == b"foreign concurrent edit"


def test_foreign_interrupted_shared_release_blocks_new_bundle(prepared):
    bundle, root, _, _, locks = prepared
    marker = locks / "shared-writer-inflight.json"
    marker.write_bytes(r.canonical({"manifest": "f" * 64}))
    with pytest.raises(RuntimeError, match="another shared release needs recovery"):
        run(prepared)
    assert phase(bundle) == "prepared"
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE
    assert json.loads(marker.read_bytes())["manifest"] == "f" * 64


def test_crash_after_global_marker_before_local_intent_is_recoverable(prepared, monkeypatch):
    bundle, root, _, _, locks = prepared
    original = r.journal
    monkeypatch.setattr(r, "journal", Mock(side_effect=SystemExit("power loss before local intent")))
    with pytest.raises(SystemExit):
        run(prepared)
    assert phase(bundle) == "prepared"
    assert (locks / "shared-writer-inflight.json").exists()
    monkeypatch.setattr(r, "journal", original)
    run(prepared, recover=True)
    assert phase(bundle) == "rolled_back"
    assert not (locks / "shared-writer-inflight.json").exists()
    assert r.digest((root / r.WRITER).read_bytes()) == r.BEFORE
