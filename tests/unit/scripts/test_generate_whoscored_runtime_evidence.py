from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import generate_whoscored_runtime_evidence as evidence


ROOT = Path(__file__).resolve().parents[3]


def _runtime_fixture(root: Path) -> None:
    source = root / evidence.SOURCE_RELATIVE
    source.parent.mkdir(parents=True)
    source.write_text(
        "EXPECTED_RUNTIME_FILES = (\n"
        '    "dags/scripts/run_whoscored_scraper.py",\n'
        '    "payload.py",\n'
        '    "scrapers/whoscored/parsers.py",\n'
        '    "scrapers/whoscored/repository.py",\n'
        '    "scrapers/whoscored/runtime_contract.py",\n'
        ")\n",
        encoding="utf-8",
    )
    (root / "payload.py").write_text("VALUE = 1\n", encoding="utf-8")
    metadata_sources = {
        evidence.PARSER_RELATIVE: 'PARSER_VERSION = "parser-test-v1"\n',
        evidence.REPORT_RELATIVE: "REPORT_SCHEMA_VERSION = 7\n",
        evidence.REPOSITORY_RELATIVE: (
            "WHOSCORED_BUSINESS_TABLES = (\n"
            '    "one",\n'
            '    "two",\n'
            ")\n"
        ),
    }
    for relative, payload in metadata_sources.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
    outputs = (evidence.LOCK_RELATIVE, *(path for path, _ in evidence.TRUST_ROOTS))
    for relative in outputs:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("stale\n", encoding="ascii")


@pytest.mark.unit
def test_rendered_runtime_evidence_is_deterministic_and_self_bound(tmp_path):
    _runtime_fixture(tmp_path)

    first = evidence.render_evidence(tmp_path)
    second = evidence.render_evidence(tmp_path)

    assert first == second
    assert tuple(first) == (
        evidence.LOCK_RELATIVE,
        *(relative for relative, _ in evidence.TRUST_ROOTS),
    )
    lock = json.loads(first[evidence.LOCK_RELATIVE])
    assert lock["schema_version"] == evidence.LOCK_SCHEMA_VERSION
    assert lock["parser_version"] == "parser-test-v1"
    assert lock["report_schema_version"] == 7
    assert lock["business_dataset_count"] == 2
    assert lock["files"] == {
        relative.as_posix(): hashlib.sha256((tmp_path / relative).read_bytes()).hexdigest()
        for relative in (
            evidence.REPORT_RELATIVE,
            evidence.PurePosixPath("payload.py"),
            evidence.PARSER_RELATIVE,
            evidence.REPOSITORY_RELATIVE,
            evidence.SOURCE_RELATIVE,
        )
    }
    lock_sha256 = hashlib.sha256(first[evidence.LOCK_RELATIVE]).hexdigest()
    for relative, runtime_class in evidence.TRUST_ROOTS:
        values = dict(
            line.split("=", 1)
            for line in first[relative].decode("ascii").splitlines()
        )
        assert values["runtime_class"] == runtime_class
        assert values["runtime_contract_lock_sha256"] == lock_sha256


@pytest.mark.unit
def test_write_repairs_only_the_four_outputs_and_check_is_idempotent(tmp_path):
    _runtime_fixture(tmp_path)
    rendered = evidence.render_evidence(tmp_path)
    payload_before = (tmp_path / "payload.py").read_bytes()

    assert evidence.check_evidence(tmp_path, rendered) is False
    evidence.write_evidence(tmp_path, rendered)
    assert evidence.check_evidence(tmp_path, rendered) is True
    assert (tmp_path / "payload.py").read_bytes() == payload_before
    metadata_before = {
        relative: (tmp_path / relative).stat().st_mtime_ns for relative in rendered
    }
    assert evidence.check_evidence(tmp_path, rendered) is True
    assert {
        relative: (tmp_path / relative).stat().st_mtime_ns for relative in rendered
    } == metadata_before


@pytest.mark.unit
def test_runtime_allowlist_rejects_duplicates_unsorted_and_dynamic_values():
    with pytest.raises(evidence.EvidenceError, match="sorted and unique"):
        evidence._expected_runtime_files(
            b'EXPECTED_RUNTIME_FILES = ("z.py", "z.py")\n'
        )
    with pytest.raises(evidence.EvidenceError, match="sorted and unique"):
        evidence._expected_runtime_files(
            b'EXPECTED_RUNTIME_FILES = ("z.py", "a.py")\n'
        )
    with pytest.raises(evidence.EvidenceError, match="literal tuple"):
        evidence._expected_runtime_files(
            b'EXPECTED_RUNTIME_FILES = tuple(["a.py"])\n'
        )


@pytest.mark.unit
def test_runtime_evidence_rejects_a_symlinked_member(tmp_path):
    _runtime_fixture(tmp_path)
    payload = tmp_path / "payload.py"
    payload.unlink()
    payload.symlink_to(tmp_path / evidence.SOURCE_RELATIVE)

    with pytest.raises(evidence.EvidenceError, match="missing or unreadable"):
        evidence.render_evidence(tmp_path)


@pytest.mark.unit
def test_runtime_evidence_publishes_lock_before_all_trust_roots(
    monkeypatch, tmp_path
):
    _runtime_fixture(tmp_path)
    rendered = evidence.render_evidence(tmp_path)
    published = []
    monkeypatch.setattr(
        evidence,
        "_replace_relative",
        lambda _root_fd, relative, _payload: published.append(relative),
    )

    evidence.write_evidence(tmp_path, rendered)

    assert published == [
        evidence.LOCK_RELATIVE,
        *(relative for relative, _ in evidence.TRUST_ROOTS),
    ]


@pytest.mark.unit
def test_interruption_keeps_production_trust_root_at_the_old_decision(
    monkeypatch, tmp_path
):
    _runtime_fixture(tmp_path)
    old = evidence.render_evidence(tmp_path)
    evidence.write_evidence(tmp_path, old)
    (tmp_path / "payload.py").write_text("VALUE = 2\n", encoding="utf-8")
    new = evidence.render_evidence(tmp_path)
    production_relative = next(
        relative
        for relative, runtime_class in evidence.TRUST_ROOTS
        if runtime_class == "production-v1"
    )
    original = evidence._replace_relative

    for fail_before in range(4):
        root_fd = evidence._open_root(tmp_path)
        try:
            for relative, payload in old.items():
                original(root_fd, relative, payload)
        finally:
            os.close(root_fd)
        calls = 0

        def interrupted(root_fd, relative, payload):
            nonlocal calls
            if calls == fail_before:
                raise evidence.EvidenceError("simulated interruption")
            calls += 1
            original(root_fd, relative, payload)

        monkeypatch.setattr(evidence, "_replace_relative", interrupted)
        with pytest.raises(evidence.EvidenceError, match="simulated"):
            evidence.write_evidence(tmp_path, new)
        assert (tmp_path / production_relative).read_bytes() == old[
            production_relative
        ]


@pytest.mark.unit
def test_checked_in_runtime_evidence_matches_generator():
    rendered = evidence.render_evidence(ROOT)

    assert evidence.check_evidence(ROOT, rendered)


@pytest.mark.unit
def test_command_refuses_a_nonisolated_interpreter():
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/generate_whoscored_runtime_evidence.py"),
            "--root",
            str(ROOT),
            "--check",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == evidence.EXIT_CONFIG
    assert "requires Python -I -S" in completed.stderr
