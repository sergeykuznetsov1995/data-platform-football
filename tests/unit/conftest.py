"""Private host resources for WhoScored tests; production defaults stay intact."""

import os
from pathlib import Path
import subprocess
import tempfile

import pytest

# Host-seal checks of the dormant WhoScored ceremony: they assert root-owned
# path chains of the production VM (or run as root), so they can only pass
# when run as root. CI ignores these modules (.github/workflows/ci.yml); they
# go away with the ceremony removal (#1487).
HOST_SEAL_MODULES = frozenset({
    "test_bench_whoscored_capacity.py",
    "test_whoscored_production_admission.py",
    "test_whoscored_go_decision.py",
    "test_whoscored_capacity_container_runtime.py",
})
# Mostly portable modules: only these tests need a root-owned parent directory.
HOST_SEAL_TESTS = frozenset({
    "test_generate_whoscored_deployment_attestation.py::test_published_document_is_accepted_by_existing_validator",
    "test_generate_whoscored_deployment_attestation.py::test_publish_is_create_once_and_never_follows_existing_symlink",
    "test_generate_whoscored_deployment_attestation.py::test_cli_runtime_rejects_an_isolated_non_system_interpreter",
    "test_generate_whoscored_deployment_attestation.py::test_generate_validates_twice_before_create_once_publish",
    "test_generate_whoscored_deployment_attestation.py::test_generate_publishes_nothing_when_ready_evidence_changes",
    "test_validate_whoscored_build_provenance.py::test_ready_manifest_and_external_deployment_binding_are_accepted",
    "test_validate_whoscored_build_provenance.py::test_external_deployment_result_reuses_exact_fd_pinned_bytes",
    "test_validate_whoscored_build_provenance.py::test_deployment_attestation_requires_named_registry_digest",
})


def pytest_collection_modifyitems(config, items):
    if os.geteuid() == 0:
        return
    skip = pytest.mark.skip(
        reason="WhoScored host-seal test needs root-owned production paths (#1487)"
    )
    for item in items:
        name = item.path.name
        if name in HOST_SEAL_MODULES or f"{name}::{item.originalname}" in HOST_SEAL_TESTS:
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def whoscored_test_locks(request, monkeypatch):
    if "whoscored" not in request.node.path.name:
        return
    root = request.getfixturevalue("tmp_path_factory").mktemp("whoscored-locks")
    for variable, name in (
        ("WHOSCORED_LOCK_DIR", "commit-locks"),
        ("WHOSCORED_RAW_LOCK_DIR", "raw-locks"),
    ):
        directory = root / name
        directory.mkdir(mode=0o700)
        monkeypatch.setenv(variable, str(directory))


@pytest.fixture(autouse=True)
def transfermarkt_test_writer_lock(request, monkeypatch):
    """Keep the Transfermarkt writer lock off the production log volume.

    The writer defaults to /opt/airflow/logs, which unprivileged hosts cannot
    write; tests that need a specific lock still override this per test.
    """
    if "transfermarkt" not in request.node.path.name:
        return
    lock = request.getfixturevalue("tmp_path_factory").mktemp("tm-lock") / "writer.lock"
    monkeypatch.setenv("TM_WRITER_LOCK_PATH", str(lock))


@pytest.fixture(scope="session")
def protected_whoscored_checkout():
    """Copy tracked bytes, retaining Git identity without altering the checkout.

    Do not regenerate evidence: stale hashes must still fail. Explicit /tmp
    avoids writable checkout ancestors used by TMPDIR in some runners.
    """
    source = Path(__file__).resolve().parents[2]
    names = subprocess.check_output(
        ["git", "ls-files", "-z"], cwd=source
    ).decode().split("\0")
    with tempfile.TemporaryDirectory(prefix="whoscored-checkout-test-", dir="/tmp") as name:
        root = Path(name)
        for relative in filter(None, names):
            original = source / relative
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # mkdir(parents=True) uses the umask for intermediate directories.
            for parent in target.parents:
                if parent == root:
                    break
                parent.chmod(0o700)
            target.write_bytes(original.read_bytes())
            target.chmod(0o700 if original.stat().st_mode & 0o111 else 0o600)
        git_dir = subprocess.check_output(
            ["git", "rev-parse", "--absolute-git-dir"], cwd=source, text=True
        ).strip()
        (root / ".git").write_text(f"gitdir: {git_dir}\n")
        (root / ".git").chmod(0o600)
        yield root
