from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
GATE_PATH = ROOT / "docker/images/airflow/whoscored_production_gate.py"
CHECKED_ATTESTATION = (
    ROOT / "docker/images/airflow/whoscored-build-provenance-attestation.json"
)
CHECKED_MANIFEST = (
    ROOT / "docker/images/airflow/whoscored-build-provenance-manifest.json"
)
LAUNCHER_PATH = ROOT / "docker/images/airflow/whoscored-production-gate"
ENTRYPOINT_PATH = ROOT / "docker/images/airflow/whoscored-production-entrypoint"
PYTHON_LAUNCHER_PATH = ROOT / "docker/images/airflow/whoscored-production-python"


def _load_gate():
    spec = importlib.util.spec_from_file_location(
        "whoscored_production_gate", GATE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _canonical(value):
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode()


def _write_evidence(path: Path, payload: bytes) -> None:
    path.write_bytes(payload)
    path.chmod(0o444)


def _ready_manifest():
    digest = "1" * 64
    return {
        "schema_version": 1,
        "generated_at": "2026-07-15T00:00:00Z",
        "source_revision": "2" * 40,
        "source_tree_sha256": "3" * 64,
        "closure_report_sha256": "4" * 64,
        "base_images": [
            {
                "dockerfile": "docker/images/airflow/Dockerfile",
                "stage": "airflow-base",
                "image": "apache/airflow:2.11.2@sha256:" + digest,
            }
        ],
        "apt_snapshots": [
            {
                "url": "https://snapshot.debian.org/archive/debian/20260715T000000Z/",
                "release_sha256": "5" * 64,
            }
        ],
        "apt_packages": [{"name": "curl", "version": "7.88.1-10+deb12u14"}],
        "downloaded_artifacts": [
            {
                "name": "apache-spark",
                "url": "https://archive.apache.org/spark.tgz",
                "sha256": "6" * 64,
                "size": 1,
            }
        ],
        "python_locks": [
            {
                "interpreter": "airflow",
                "python_abi": "cp311",
                "path": "docker/images/airflow/requirements.lock",
                "sha256": "7" * 64,
                "require_hashes": True,
            },
            {
                "interpreter": "legacy-scraper",
                "python_abi": "cp311",
                "path": "docker/images/airflow/requirements-runner.lock",
                "sha256": "8" * 64,
                "require_hashes": True,
            },
        ],
        "github_actions": [
            {
                "workflow": ".github/workflows/whoscored-ci.yml",
                "uses": "actions/checkout",
                "commit": "9" * 40,
            }
        ],
        "compose_images": [
            {
                "service": "postgres",
                "image": "postgres:16@sha256:" + "a" * 64,
            }
        ],
        "local_images": [
            {
                "service": "airflow-scheduler",
                "context": "docker/images/airflow",
                "dockerfile": "docker/images/airflow/Dockerfile",
                "target": "airflow-scheduler",
                "payload_target": "airflow-scheduler-payload",
                "context_sha256": "b" * 64,
                "base_image_sha256": "c" * 64,
                "payload_image_id": "sha256:" + "d" * 64,
            }
        ],
    }


@pytest.mark.unit
def test_checked_in_production_evidence_matches_declared_state(tmp_path):
    gate = _load_gate()
    payload = json.loads(CHECKED_ATTESTATION.read_text(encoding="utf-8"))
    image_attestation = tmp_path / "attestation.json"
    image_manifest = tmp_path / "manifest.json"
    _write_evidence(image_attestation, CHECKED_ATTESTATION.read_bytes())
    _write_evidence(image_manifest, CHECKED_MANIFEST.read_bytes())
    if payload.get("status") == "blocked-v1":
        assert payload == {
            "schema_version": 1,
            "status": "blocked-v1",
            "provenance_manifest_sha256": "",
        }
        assert CHECKED_MANIFEST.read_bytes() == (
            b'{"schema_version":1,"status":"blocked-v1"}\n'
        )
        with pytest.raises(gate.ProductionGateError, match="not promoted"):
            gate.validate_production_attestation(
                image_attestation,
                image_manifest,
                expected_uid=image_attestation.stat().st_uid,
                enforce_immutable_parents=False,
            )
    elif payload.get("status") == "ready-v1":
        assert gate.validate_production_attestation(
            image_attestation,
            image_manifest,
            expected_uid=image_attestation.stat().st_uid,
            enforce_immutable_parents=False,
        ) == hashlib.sha256(CHECKED_MANIFEST.read_bytes()).hexdigest()
    else:
        pytest.fail("checked-in production evidence has an unsupported state")


@pytest.mark.unit
def test_production_launchers_have_no_runtime_override():
    gate_launcher = LAUNCHER_PATH.read_text(encoding="utf-8")
    entrypoint = ENTRYPOINT_PATH.read_text(encoding="utf-8")
    python_launcher = PYTHON_LAUNCHER_PATH.read_text(encoding="utf-8")

    assert LAUNCHER_PATH.stat().st_mode & 0o111
    assert ENTRYPOINT_PATH.stat().st_mode & 0o111
    assert gate_launcher == (
        "#!/bin/sh\n"
        "set -eu\n"
        "exec /usr/local/libexec/whoscored-python-real -I -S "
        "/usr/local/libexec/whoscored_production_gate.py\n"
    )
    assert entrypoint == (
        '#!/bin/sh\nset -eu\n/usr/local/bin/whoscored-production-gate\nexec "$@"\n'
    )
    assert "WHOSCORED_" not in gate_launcher
    assert "WHOSCORED_" not in entrypoint
    assert python_launcher == (
        "#!/bin/sh\n"
        "set -eu\n"
        "\n"
        "/usr/local/bin/whoscored-production-gate\n"
        'exec /usr/local/libexec/whoscored-python-real "$@"\n'
    )
    assert "WHOSCORED_" not in python_launcher
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert (
        "!docker/images/airflow/whoscored-build-provenance-attestation.json"
        in gitignore
    )
    assert (
        "!docker/images/airflow/whoscored-build-provenance-manifest.json" in gitignore
    )


@pytest.mark.unit
def test_ready_attestation_requires_exact_root_owned_manifest_bytes(tmp_path):
    gate = _load_gate()
    manifest = tmp_path / "manifest.json"
    manifest_raw = _canonical(_ready_manifest())
    _write_evidence(manifest, manifest_raw)
    attestation = tmp_path / "attestation.json"
    digest = hashlib.sha256(manifest_raw).hexdigest()
    _write_evidence(
        attestation,
        _canonical(
            {
                "schema_version": 1,
                "status": "ready-v1",
                "provenance_manifest_sha256": digest,
            }
        ),
    )

    assert (
        gate.validate_production_attestation(
            attestation,
            manifest,
            expected_uid=tmp_path.stat().st_uid,
            enforce_immutable_parents=False,
        )
        == digest
    )

    manifest.chmod(0o644)
    with pytest.raises(gate.ProductionGateError, match="owner, mode"):
        gate.validate_production_attestation(
            attestation,
            manifest,
            expected_uid=tmp_path.stat().st_uid,
            enforce_immutable_parents=False,
        )


@pytest.mark.unit
def test_gate_rejects_symlink_noncanonical_and_digest_drift(tmp_path):
    gate = _load_gate()
    manifest = tmp_path / "manifest.json"
    manifest_raw = _canonical(_ready_manifest())
    _write_evidence(manifest, manifest_raw)
    attestation = tmp_path / "attestation.json"
    _write_evidence(
        attestation,
        _canonical(
            {
                "schema_version": 1,
                "status": "ready-v1",
                "provenance_manifest_sha256": "0" * 64,
            }
        ),
    )

    with pytest.raises(gate.ProductionGateError, match="digest differs"):
        gate.validate_production_attestation(
            attestation,
            manifest,
            expected_uid=tmp_path.stat().st_uid,
            enforce_immutable_parents=False,
        )

    attestation.chmod(0o644)
    attestation.write_text(
        '{"status":"ready-v1", "schema_version":1, '
        '"provenance_manifest_sha256":"'
        + hashlib.sha256(manifest_raw).hexdigest()
        + '"}\n',
        encoding="utf-8",
    )
    attestation.chmod(0o444)
    with pytest.raises(gate.ProductionGateError, match="bytes are not canonical"):
        gate.validate_production_attestation(
            attestation,
            manifest,
            expected_uid=tmp_path.stat().st_uid,
            enforce_immutable_parents=False,
        )

    target = tmp_path / "real-attestation.json"
    attestation.rename(target)
    attestation.symlink_to(target.name)
    with pytest.raises(gate.ProductionGateError, match="unavailable"):
        gate.validate_production_attestation(
            attestation,
            manifest,
            expected_uid=tmp_path.stat().st_uid,
            enforce_immutable_parents=False,
        )


@pytest.mark.unit
def test_ready_attestation_cannot_bind_an_empty_inventory(tmp_path):
    gate = _load_gate()
    payload = _ready_manifest()
    payload["python_locks"] = []
    manifest_raw = _canonical(payload)
    manifest = tmp_path / "manifest.json"
    _write_evidence(manifest, manifest_raw)
    attestation = tmp_path / "attestation.json"
    _write_evidence(
        attestation,
        _canonical(
            {
                "schema_version": 1,
                "status": "ready-v1",
                "provenance_manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
            }
        ),
    )

    with pytest.raises(gate.ProductionGateError, match="non-empty"):
        gate.validate_production_attestation(
            attestation,
            manifest,
            expected_uid=tmp_path.stat().st_uid,
            enforce_immutable_parents=False,
        )
