"""The approved legacy error policy never learns a new exception from live state."""
from copy import deepcopy
import hashlib

import pytest

from deploy.shared_writer import legacy_errors as policy


@pytest.fixture
def snapshot():
    return {
        "import_error_policy": policy.POLICY,
        "import_error_protected": dict(policy.PROTECTED),
        "import_error_details": [{"filename": name, "sha256": value} for name, value in policy.ERRORS.items()],
        "dags": {"clubelo": {"fileloc": "/opt/airflow/dags/dag_ingest_clubelo.py"}},
    }


def test_only_explicit_exact_baseline_is_accepted(snapshot):
    policy.validate(snapshot)
    snapshot["import_error_policy"] = "none"
    with pytest.raises(RuntimeError, match="unapproved"):
        policy.validate(snapshot)


@pytest.mark.parametrize("drift", ["new", "missing", "duplicate", "fingerprint", "filename", "protected", "active"])
def test_legacy_policy_rejects_every_changed_baseline(snapshot, drift):
    if drift == "new":
        snapshot["import_error_details"].append({"filename": "/opt/airflow/dags/new.py", "sha256": "a" * 64})
    elif drift == "missing":
        snapshot["import_error_details"].pop()
    elif drift == "duplicate":
        snapshot["import_error_details"][-1] = deepcopy(snapshot["import_error_details"][0])
    elif drift in ("fingerprint", "filename"):
        snapshot["import_error_details"][0]["sha256" if drift == "fingerprint" else "filename"] = "changed"
    elif drift == "protected":
        snapshot["import_error_protected"]["dags/.airflowignore"] = "changed"
    else:
        snapshot["dags"]["legacy-now-active"] = {"fileloc": next(iter(policy.ERRORS))}
    with pytest.raises(RuntimeError):
        policy.validate(snapshot)


def test_default_zero_errors_never_reads_legacy_paths(tmp_path):
    assert policy.protected_hashes(tmp_path, "none") == {}
    policy.validate({"import_error_details": []})


@pytest.mark.parametrize("stage", ["initial", "changed", "missing", "symlink", "parent_symlink"])
def test_protected_file_is_actually_read_and_checked(tmp_path, monkeypatch, stage):
    folder = tmp_path / "code"
    folder.mkdir()
    file = folder / "lock"
    file.write_bytes(b"sealed")
    pins = {"code/lock": hashlib.sha256(b"sealed").hexdigest()}
    monkeypatch.setattr(policy, "PROTECTED", pins)
    if stage == "changed":
        file.write_bytes(b"foreign")
    elif stage == "missing":
        file.unlink()
    elif stage == "symlink":
        file.rename(folder / "original")
        file.symlink_to(folder / "original")
    elif stage == "parent_symlink":
        folder.rename(tmp_path / "original")
        folder.symlink_to(tmp_path / "original", target_is_directory=True)
    if stage == "initial":
        assert policy.protected_hashes(tmp_path, policy.POLICY) == pins
    else:
        with pytest.raises(RuntimeError):
            policy.protected_hashes(tmp_path, policy.POLICY)


def test_unknown_policy_fails_closed(snapshot, tmp_path):
    snapshot["import_error_policy"] = "accept-any-errors"
    with pytest.raises(RuntimeError, match="unknown"):
        policy.validate(snapshot)
    with pytest.raises(RuntimeError, match="unknown"):
        policy.protected_hashes(tmp_path, "accept-any-errors")
