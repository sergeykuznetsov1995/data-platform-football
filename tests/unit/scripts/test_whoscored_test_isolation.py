"""Test resources must retain real lock sharing and exact runtime bytes."""

import os
from pathlib import Path
import stat


def test_locks_belong_to_this_test(tmp_path):
    roots = [Path(os.environ[key]) for key in (
        "WHOSCORED_LOCK_DIR", "WHOSCORED_RAW_LOCK_DIR",
    )]
    assert len(set(roots)) == 2
    for root in roots:
        assert root.parent != tmp_path
        assert root.parent.name.startswith("whoscored-locks")
        assert stat.S_IMODE(root.stat().st_mode) == 0o700


def test_protected_checkout_keeps_exact_runtime_and_evidence_bytes(
    protected_whoscored_checkout,
):
    source = Path(__file__).resolve().parents[3]
    for relative in (
        "scripts/validate_whoscored_build_provenance.py",
        "scripts/whoscored_production_admission.py",
        "scrapers/whoscored/runtime_contract.lock",
        "docker/images/airflow/whoscored-build-provenance-attestation.json",
        "docker/images/airflow/whoscored-build-provenance-manifest.json",
    ):
        copied = protected_whoscored_checkout / relative
        assert copied.read_bytes() == (source / relative).read_bytes()
        assert copied.stat().st_mode & 0o022 == 0
        for parent in copied.parents:
            assert parent.stat().st_mode & 0o022 == 0
            if parent == protected_whoscored_checkout:
                break
