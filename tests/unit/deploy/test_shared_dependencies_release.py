"""Two-file publication/recovery tests use only disposable local runtime trees."""
import fcntl
import json
import os
from pathlib import Path
import time
from unittest.mock import Mock

import pytest

from deploy.shared_dependencies import release as r


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    root = tmp_path / "runtime"
    data = {name: (f"# old {name}\n".encode(), f"# new {name}\n".encode()) for name in r.PINS}
    monkeypatch.setattr(r, "PINS", {name: tuple(r.digest(v) for v in versions) for name, versions in data.items()})
    for name, versions in data.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(versions[0])
    for name in (*r.CONFIGS, r.shared.WRITER, "scrapers/whoscored/runtime_contract.lock", "dags/neighbor.py"):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# original neighbor\n")
    (root / "scripts").mkdir()
    host = Mock()
    host.snapshot.return_value = {"identity": {"consumers": {"scheduler": {"image": "sha256:test"}}},
                                  "dags": {"neighbor": {"fileloc": "/opt/airflow/dags/neighbor.py"}}}
    host.preflight.return_value = {"quiet": True}
    host.rollback_preflight.return_value = {"quiet": True}
    host.postflight.return_value = {"parsed": True}
    host.dependency_hashes.side_effect = lambda target: {"scheduler": {p: hashes[target] for p, hashes in r.PINS.items()}}
    monkeypatch.setattr(r, "payload", lambda repo, commit: {p: v[1] for p, v in data.items()})
    monkeypatch.setattr(r, "merged", Mock())
    bundle = tmp_path / "bundle"
    repo = tmp_path / "repository"
    repo.mkdir()
    approval = r.prepare(bundle, root, repo, "a" * 40, time.time() - 5, time.time() + 600, host)
    r.save(bundle / "rehearsal.json", r.canonical({"manifest": approval, "passed": True}))
    locks = tmp_path / "locks"
    locks.mkdir()
    for name in r.shared.LOCKS:
        (locks / name).write_bytes(b"lock owner metadata")
    return bundle, root, approval, host, locks


def phase(bundle):
    return json.loads((bundle / "state.json").read_text())["phase"]


def run(p, **kwargs):
    bundle, _, approval, host, locks = p
    r.execute(bundle, approval, host, state=locks, **kwargs)


def test_pair_publication_and_reverse_rollback_preserve_neighbors(prepared, monkeypatch):
    bundle, root, approval, host, locks = prepared
    before = r.closure(root)
    handles = {name: (root / name).open("rb") for name in r.PINS}
    real = r.os.replace
    events = []

    def observe(source, target):
        if Path(target).is_relative_to(root) and str(Path(target).relative_to(root)) in r.PINS:
            assert Path(source).parent == bundle
            events.append(str(Path(target).relative_to(root)))
        real(source, target)

    monkeypatch.setattr(r.os, "replace", observe)
    run(prepared)
    assert phase(bundle) == "accepted"
    m, _ = r.load(bundle, approval)
    r.check_files(m, 1)
    for name, handle in handles.items():
        assert r.digest(handle.read()) == r.PINS[name][0]
        handle.close()
    run(prepared, recover=True)
    assert phase(bundle) == "rolled_back"
    assert r.closure(root) == before
    assert events == list(r.PINS) + list(reversed(r.PINS))
    assert all(p.read_bytes() == b"lock owner metadata" for p in locks.iterdir())


@pytest.mark.parametrize("published", [0, 1, 2])
def test_crash_between_any_publications_recovers_exact_originals(prepared, monkeypatch, published):
    bundle, root, _, _, locks = prepared
    original = r.replace_one
    calls = 0

    def die(*args):
        nonlocal calls
        if calls == published:
            raise SystemExit("power failure")
        original(*args)
        calls += 1
        if calls == published:
            raise SystemExit("power failure")

    monkeypatch.setattr(r, "replace_one", die)
    with pytest.raises(SystemExit):
        run(prepared)
    assert (locks / "shared-writer-inflight.json").exists()
    assert phase(bundle) == "inflight"
    assert sum(r.digest((root / p).read_bytes()) == h[1] for p, h in r.PINS.items()) == published
    monkeypatch.setattr(r, "replace_one", original)
    run(prepared, recover=True)
    assert phase(bundle) == "rolled_back"
    assert not (locks / "shared-writer-inflight.json").exists()


def test_crash_before_local_intent_preserves_recovery_marker(prepared, monkeypatch):
    bundle, _, _, _, locks = prepared
    original = r.journal
    monkeypatch.setattr(r, "journal", Mock(side_effect=SystemExit("power failure")))
    with pytest.raises(SystemExit):
        run(prepared)
    assert phase(bundle) == "prepared"
    assert (locks / "shared-writer-inflight.json").exists()
    monkeypatch.setattr(r, "journal", original)
    run(prepared, recover=True)
    assert phase(bundle) == "rolled_back"


@pytest.mark.parametrize("unavailable_io", [False, True])
def test_accepted_marker_cleanup_failure_never_starts_unfenced_rollback(prepared, monkeypatch, unavailable_io):
    bundle, root, _, host, locks = prepared
    real_sync = r.shared.sync_dir
    failed = False

    def fail_after_unlink(directory):
        nonlocal failed
        if Path(directory) == locks and not (locks / "shared-writer-inflight.json").exists() and not failed:
            failed = True
            raise OSError("marker directory fsync failed")
        if unavailable_io and failed:
            raise OSError("persistent filesystem failure")
        real_sync(directory)

    monkeypatch.setattr(r.shared, "sync_dir", fail_after_unlink)
    with pytest.raises(OSError):
        run(prepared)
    assert phase(bundle) == "accepted"
    assert all(r.digest((root / p).read_bytes()) == h[1] for p, h in r.PINS.items())
    host.rollback_preflight.assert_not_called()
    assert (locks / "shared-writer-inflight.json").exists()
    monkeypatch.setattr(r.shared, "sync_dir", real_sync)
    run(prepared, recover=True)
    assert phase(bundle) == "rolled_back"


def test_rolled_back_marker_cleanup_failure_keeps_verified_old_runtime(prepared, monkeypatch):
    bundle, root, _, host, locks = prepared
    host.postflight.side_effect = [RuntimeError("parse failed"), {"parsed": True}]
    real_sync = r.shared.sync_dir

    def fail_after_unlink(directory):
        if Path(directory) == locks and not (locks / "shared-writer-inflight.json").exists():
            raise OSError("marker directory fsync failed")
        real_sync(directory)

    monkeypatch.setattr(r.shared, "sync_dir", fail_after_unlink)
    with pytest.raises(OSError, match="marker directory"):
        run(prepared)
    assert phase(bundle) == "rolled_back"
    assert all(r.digest((root / p).read_bytes()) == h[0] for p, h in r.PINS.items())
    assert (locks / "shared-writer-inflight.json").exists()


@pytest.mark.parametrize("ancestor", [False, True])
def test_repository_must_not_overlap_production_tree(prepared, ancestor):
    bundle, root, _, host, _ = prepared
    repo = root.parent if ancestor else root
    with pytest.raises(RuntimeError, match="repository overlaps runtime"):
        r.prepare(bundle.parent / "other-bundle", root, repo, "a" * 40, time.time(), time.time() + 600, host)


def test_load_rejects_manifest_pointing_git_at_production(prepared):
    bundle, root, _, _, _ = prepared
    m = json.loads((bundle / "manifest.json").read_text())
    m["repo"] = str(root)
    raw = r.canonical(m)
    r.save(bundle / "manifest.json", raw)
    with pytest.raises(RuntimeError, match="repository overlaps runtime"):
        r.load(bundle, r.digest(raw))


@pytest.mark.parametrize("after_rename", [False, True])
def test_second_publication_io_failure_rolls_back_mixed_pair(prepared, monkeypatch, after_rename):
    bundle, root, _, _, _ = prepared
    real = r.os.replace
    second = root / list(r.PINS)[1]
    attempted = False

    def fail(source, target):
        nonlocal attempted
        if Path(target) == second and not attempted:
            attempted = True
            if after_rename:
                real(source, target)
            raise OSError("publication failed")
        real(source, target)

    monkeypatch.setattr(r.os, "replace", fail)
    with pytest.raises(OSError, match="publication failed"):
        run(prepared)
    assert phase(bundle) == "rolled_back"
    assert all(r.digest((root / p).read_bytes()) == h[0] for p, h in r.PINS.items())


@pytest.mark.parametrize("drift", ["dependency", "catalog", "neighbor", "lock", "mode", "newfile"])
def test_foreign_drift_blocks_recovery_without_overwriting(prepared, drift):
    bundle, root, _, host, _ = prepared

    def change(*args):
        name = {"dependency": list(r.PINS)[1], "catalog": r.CONFIGS[0], "neighbor": "dags/neighbor.py",
                "lock": "scrapers/whoscored/runtime_contract.lock", "mode": "dags/neighbor.py", "newfile": "scripts/foreign.py"}[drift]
        if drift == "mode":
            (root / name).chmod(0o600)
        else:
            (root / name).write_bytes(b"foreign change")
        raise RuntimeError("foreign change")

    host.postflight.side_effect = change
    with pytest.raises(RuntimeError, match="foreign change"):
        run(prepared)
    assert phase(bundle) == "blocked"
    assert r.digest((root / list(r.PINS)[0]).read_bytes()) == r.PINS[list(r.PINS)[0]][1]


def test_postflight_failure_recovers_but_returns_failure(prepared):
    bundle, root, _, host, _ = prepared
    host.postflight.side_effect = [RuntimeError("parse failed"), {"parsed": True}]
    with pytest.raises(RuntimeError, match="parse failed"):
        run(prepared)
    assert phase(bundle) == "rolled_back"
    assert all(r.digest((root / p).read_bytes()) == h[0] for p, h in r.PINS.items())


@pytest.mark.parametrize("problem", ["approval", "backup", "payload", "receipt", "preflight", "merged", "window", "marker", "used", "catalog"])
def test_refusal_before_runtime_mutation(prepared, monkeypatch, problem):
    bundle, root, approval, host, locks = prepared
    if problem == "approval":
        prepared = bundle, root, "f" * 64, host, locks
    elif problem in ("backup", "payload"):
        (bundle / ("before-0.py" if problem == "backup" else "after-0.py")).write_bytes(b"corrupt")
    elif problem == "receipt":
        (bundle / "rehearsal.json").unlink()
    elif problem == "preflight":
        host.preflight.side_effect = RuntimeError("active work")
    elif problem == "merged":
        monkeypatch.setattr(r, "merged", Mock(side_effect=RuntimeError("unmerged")))
    elif problem == "window":
        monkeypatch.setattr(r.time, "time", lambda: 0)
    elif problem == "marker":
        (locks / "shared-writer-inflight.json").write_bytes(r.canonical({"manifest": "f" * 64}))
    elif problem == "used":
        r.journal(bundle, approval, "accepted")
    elif problem == "catalog":
        (root / r.CONFIGS[0]).write_bytes(b"catalog changed")
    with pytest.raises((RuntimeError, FileNotFoundError)):
        run(prepared)
    assert all(r.digest((root / p).read_bytes()) == h[0] for p, h in r.PINS.items())
    host.postflight.assert_not_called()


def test_shared_writer_lock_prevents_pair_release(prepared):
    bundle, root, _, _, locks = prepared
    with (locks / r.shared.LOCKS[0]).open("rb") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            run(prepared)
    assert phase(bundle) == "prepared"
    assert all(r.digest((root / p).read_bytes()) == h[0] for p, h in r.PINS.items())


@pytest.mark.parametrize("kind", ["symlink", "hardlink"])
def test_unsafe_target_refused(prepared, kind):
    bundle, root, _, _, _ = prepared
    target = root / list(r.PINS)[0]
    original = root / "original"
    target.rename(original)
    if kind == "symlink":
        target.symlink_to(original)
    else:
        os.link(original, target)
    with pytest.raises(RuntimeError):
        run(prepared)
    assert phase(bundle) == "prepared"


def test_rehearsal_covers_mixed_forward_and_reverse_states_on_copies(prepared, monkeypatch):
    bundle, root, approval, _, _ = prepared
    before = r.closure(root)
    (bundle / "rehearsal.json").unlink()
    observations = []

    def fake_run(args, **kwargs):
        if args[1] == "rm":
            return Mock(returncode=0)
        assert args[args.index("--network") + 1] == "none"
        assert "--read-only" in args
        assert f"src={root}," not in " ".join(args)
        observations.append(json.loads((bundle / "rehearsal-runtime/dependency-probe.json").read_text()))
        return Mock(returncode=0, stdout=b"passed", stderr=b"")

    monkeypatch.setattr(r.subprocess, "run", fake_run)
    r.rehearse(bundle, approval)
    assert [(p["medallion_new"], p["proxy_new"]) for p in observations] == [
        (False, False), (True, False), (True, True), (True, False), (False, False)]
    assert r.closure(root) == before
    assert r.closure(bundle / "rehearsal-runtime") == before
    assert json.loads((bundle / "rehearsal.json").read_text()) == {"manifest": approval, "passed": True}


def test_failed_rehearsal_leaves_no_receipt(prepared, monkeypatch):
    bundle, root, approval, _, _ = prepared
    (bundle / "rehearsal.json").unlink()
    monkeypatch.setattr(r.subprocess, "run", lambda *a, **kw: Mock(returncode=78, stdout=b"", stderr=b"anchor failed"))
    with pytest.raises(RuntimeError, match="isolated startup"):
        r.rehearse(bundle, approval)
    assert not (bundle / "rehearsal.json").exists()
    assert all(r.digest((root / p).read_bytes()) == h[0] for p, h in r.PINS.items())


def test_mounted_observation_checks_both_files_on_every_consumer(monkeypatch):
    host = r.Host(Path("/unused"))
    calls = []

    def observe(args):
        calls.append(args)
        name = args[-1].removeprefix("/opt/airflow/")
        return r.PINS[name][1] + "  " + args[-1]

    monkeypatch.setattr(host, "_run", observe)
    result = host.dependency_hashes(1)
    assert set(result) == set(r.CONSUMERS)
    assert len(calls) == 2 * len(r.CONSUMERS)
    monkeypatch.setattr(host, "_run", lambda args: "foreign " + args[-1])
    with pytest.raises(RuntimeError, match="mounted dependency mismatch"):
        host.dependency_hashes(1)


@pytest.mark.parametrize("action", ["rehearse", "check", "apply", "recover"])
def test_existing_bundle_policy_cannot_be_overridden(monkeypatch, tmp_path, action):
    import sys
    monkeypatch.setattr(sys, "argv", ["release", action, "--bundle", str(tmp_path),
                                     "--import-error-policy", "whoscored-legacy-20261003"])
    with pytest.raises(RuntimeError, match="policy pinned"):
        r.main()


def test_scope_and_tool_closure_do_not_expand_old_writer_release():
    assert set(r.PINS) == {"dags/utils/medallion_config.py", "scrapers/utils/proxy_manager.py"}
    assert r.shared.WRITER not in r.PINS
    assert {"shared_writer/work.py", "shared_writer/processes.py", "shared_writer/legacy_errors.py",
            "shared_dependencies/release.py", "shared_dependencies/probe.py"} <= r.tool_hashes().keys()
