from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path

import pytest

from scrapers.espn.migration import (
    ABSENCE_BASELINE_VERSION,
    BASELINE_TABLE,
    LEGACY_MIGRATION_VERSION,
    LEGACY_PROMOTION_EVIDENCE_VERSION,
    LEGACY_ROLLBACK_PLAN_VERSION,
    PROMOTION_EVIDENCE_VERSION,
    ArtifactRef,
    FallbackDescriptor,
    GreenRunEvidence,
    MigrationError,
    RepositoryMigrationBackend,
    apply_promotion,
    apply_rollback,
    build_promotion_plan,
    build_rollback_plan,
    load_promotion_evidence,
    migration_statements,
    registry_fallback_descriptors,
)
from scrapers.espn.repository import MANIFEST_TABLE, canonical_json, canonical_sha256
from scrapers.espn.discovery import CatalogSnapshot
from scrapers.espn.registry import (
    build_discovered_male_registry,
    load_registry,
    validate_registry_document,
)
from scripts.migrate_espn_native_v2 import main


UTC = timezone.utc
NOW = datetime(2026, 8, 1, 9, tzinfo=UTC)


def _write_json(path: Path, payload: object) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    path.write_bytes(body)
    return {"uri": path.as_uri(), "sha256": hashlib.sha256(body).hexdigest()}


def _raw_manifest(
    run_id: str,
    attempt: int,
    plan_signature: str,
    registry_signature: str,
    as_of: str,
) -> dict:
    base = {
        "kind": "espn-raw-run-manifest-v1",
        "schema_version": 1,
        "run_id": run_id,
        "attempt": attempt,
        "mode": "daily",
        "as_of": as_of,
        "registry_signature": registry_signature,
        "plan_signature": plan_signature,
        "selected_scopes": ["700:2026"],
        "checkpoints": [
            {
                "checkpoint_id": 1,
                "scope_id": "700:2026",
                "endpoint": "scoreboard",
                "requests": [
                    {
                        "request_id": f"scoreboard:{'e' * 64}",
                        "scope_id": "700:2026",
                        "endpoint": "scoreboard",
                        "event_id": None,
                        "url_fingerprint": "e" * 64,
                        "raw_uri": "file:///immutable/raw.json.gz",
                        "raw_sha256": "f" * 64,
                        "fetched_at": "2026-07-31T08:00:00+00:00",
                        "http_status": 200,
                        "direct_bytes": 100,
                        "proxy_bytes": 0,
                        "query_start": "2026-06-01",
                        "query_end": "2027-06-01",
                    }
                ],
            }
        ],
    }
    body = json.dumps(base, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    return {**base, "manifest_sha256": hashlib.sha256(body).hexdigest()}


def _evidence(
    tmp_path: Path, *, fallback_kind: str = "legacy", evidence_v3: bool = False
) -> tuple[Path, list[dict]]:
    registry_document = json.loads(load_registry().canonical_json())
    if fallback_kind == "absent":
        next(
            item for item in registry_document["competitions"] if item["espn_id"] == 700
        )["legacy"] = None
    registry = validate_registry_document(registry_document)
    registry_ref = _write_json(
        tmp_path / "registry.json", json.loads(registry.canonical_json())
    )
    registry_signature = registry.signature()
    green_runs = []
    for attempt in range(1, 4):
        as_of = f"2026-07-{28 + attempt:02d}"
        parent_run_id = f"scheduled__{as_of}T14:00:00+00:00"
        run_id = f"espn_daily__dag_trigger_espn_daily__{parent_run_id}"
        plan_signature = str(attempt) * 64
        generation_signature = chr(97 + attempt) * 64
        generation_manifest_sha256 = ("def0"[attempt]) * 64
        evidence_payload = {
            "kind": "espn-run-manifest-evidence-v1",
            "schema_version": 1,
            "dag_id": "dag_ingest_espn",
            "run_id": run_id,
            "attempt": attempt,
            "scope_id": "700:2026",
            "state": "complete",
            "plan_signature": plan_signature,
            "registry_signature": registry_signature,
            "generation_id": f"generation-{attempt}",
            "generation_signature": generation_signature,
            "manifest_sha256": generation_manifest_sha256,
            "lease_epoch": attempt,
            "publication_intent_ref": {
                "uri": f"file:///intent-{attempt}.json",
                "sha256": "9" * 64,
            },
            "recorded_at": f"2026-07-{28 + attempt:02d}T08:00:00+00:00",
        }
        evidence_ref = _write_json(
            tmp_path / f"run-{attempt}" / "run-evidence.json", evidence_payload
        )
        snapshot_ref = _write_json(
            tmp_path / f"run-{attempt}" / "generation.json",
            {"kind": "fixture-generation", "generation_id": f"generation-{attempt}"},
        )
        publication_payload = {
            "kind": "espn-publication-result-v1",
            "schema_version": 1,
            "scope_binding_ref": {
                "uri": (tmp_path / f"run-{attempt}" / "binding.json").as_uri(),
                "sha256": "8" * 64,
            },
            "state": "complete",
            "snapshot_ref": snapshot_ref,
            "evidence_ref": evidence_ref,
            "publication_intent_ref": evidence_payload["publication_intent_ref"],
            "direct_requests": 1,
            "proxy_bytes": 0,
        }
        publication_ref = _write_json(
            tmp_path / f"run-{attempt}" / "publication.json", publication_payload
        )
        durable_payload = {
            "kind": "espn-durable-run-manifest-v1",
            "schema_version": 1,
            "dag_id": "dag_ingest_espn",
            "run_id": run_id,
            "attempt": attempt,
            "registry_signature": registry_signature,
            "scope_ids": ["700:2026"],
            "evidence": [
                {
                    "dag_id": "dag_ingest_espn",
                    "run_id": run_id,
                    "attempt": attempt,
                    "scope_id": "700:2026",
                    "plan_signature": plan_signature,
                    "registry_signature": registry_signature,
                    "state": "complete",
                    "evidence_uri": evidence_ref["uri"],
                    "evidence_sha256": evidence_ref["sha256"],
                    "recorded_at": evidence_payload["recorded_at"],
                }
            ],
            "publication_refs": [{"publication_ref": publication_ref}],
        }
        durable_ref = _write_json(
            tmp_path / f"run-{attempt}" / "durable.json", durable_payload
        )
        raw_ref = _write_json(
            tmp_path / f"run-{attempt}" / "raw.json",
            _raw_manifest(
                run_id,
                attempt,
                plan_signature,
                registry_signature,
                as_of,
            ),
        )
        quality = {
            "scope_id": "700:2026",
            "passed": True,
            "failures": [],
            "row_counts": {"schedule": 380, "lineup": 8000, "matchsheet": 760},
            "row_hashes": {
                "schedule": "1" * 64,
                "lineup": "2" * 64,
                "matchsheet": "3" * 64,
            },
            "ledger_count": 1,
            "ledger_hash": "4" * 64,
        }
        dq_ref = _write_json(
            tmp_path / f"run-{attempt}" / "published-dq.json",
            {
                "kind": "espn-published-dq-result-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "scope_id": "700:2026",
                "run_id": run_id,
                "attempt": attempt,
                "plan_signature": plan_signature,
                "registry_signature": registry_signature,
                "publication_ref": publication_ref,
                "quality": quality,
            },
        )
        verdict_ref = _write_json(
            tmp_path / f"run-{attempt}" / "terminal-verdict.json",
            {
                "kind": "espn-terminal-verdict-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "run_id": run_id,
                "attempt": attempt,
                "status": "complete",
                "failures": [],
                "scope_count": 1,
                "producer_states": {"published_dq": ["success"]},
                "expected_counts": {"published_dq": 1},
                "scope_metrics": {"700:2026": {"proxy_bytes": 0}},
            },
        )
        health_ref = _write_json(
            tmp_path / f"run-{attempt}" / "health.json",
            {
                "kind": "espn-health-result-v1",
                "schema_version": 1,
                "run_id": run_id,
                "attempt": attempt,
                "status": "complete",
                "verdict_ref": verdict_ref,
                "alerts": [],
                "scope_metrics": {"700:2026": {"proxy_bytes": 0}},
            },
        )
        release_ref = _write_json(
            tmp_path / f"run-{attempt}" / "lease-release.json",
            {
                "kind": "espn-lease-release-result-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "run_id": run_id,
                "attempt": attempt,
                "scope_ids": ["700:2026"],
                "released": ["700:2026"],
                "failures": [],
            },
        )
        logical_date = f"{as_of}T14:00:00+00:00"
        interval_end = (
            datetime.fromisoformat(logical_date) + timedelta(days=1)
        ).isoformat()
        parent = {
            "schema": "espn-daily-parent-v2",
            "owner_profile": "espn-isolated-v1",
            "parent_dag_id": "dag_trigger_espn_daily",
            "parent_task_id": "trigger_espn_ingest",
            "parent_run_id": parent_run_id,
            "parent_run_type": "scheduled",
            "logical_date": logical_date,
            "data_interval_start": logical_date,
            "data_interval_end": interval_end,
            "child_dag_id": "dag_ingest_espn",
            "child_run_id": run_id,
        }
        receipt = {
            "kind": "espn-run-success-receipt-v1",
            "schema_version": 1,
            "dag_id": "dag_ingest_espn",
            "run_id": run_id,
            "attempt": attempt,
            "mode": "daily",
            "as_of": as_of,
            "logical_date": logical_date,
            "parent": parent,
            "scope_ids": ["700:2026"],
            "registry_ref": registry_ref,
            "registry_signature": registry_signature,
            "admission_ref": {
                "uri": f"file:///admission-{attempt}.json",
                "sha256": "5" * 64,
            },
            "plan_index_ref": {
                "uri": f"file:///index-{attempt}.json",
                "sha256": "6" * 64,
            },
            "durable_manifest_ref": durable_ref,
            "published_dq_refs": [{"scope_id": "700:2026", "published_dq_ref": dq_ref}],
            "verdict_ref": verdict_ref,
            "health_ref": health_ref,
            "lease_release_ref": release_ref,
        }
        receipt["receipt_sha256"] = hashlib.sha256(
            (
                json.dumps(
                    receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                + "\n"
            ).encode()
        ).hexdigest()
        success_ref = _write_json(
            tmp_path / f"run-{attempt}" / "run-success.json", receipt
        )
        green_runs.append(
            {
                "durable_manifest_ref": durable_ref,
                "raw_manifest_ref": raw_ref,
                "published_dq_ref": dq_ref,
                "terminal_verdict_ref": verdict_ref,
                "health_ref": health_ref,
                "lease_release_ref": release_ref,
                "success_receipt_ref": success_ref,
            }
        )
    document = {
        "schema_version": (
            PROMOTION_EVIDENCE_VERSION
            if evidence_v3 or fallback_kind == "absent"
            else LEGACY_PROMOTION_EVIDENCE_VERSION
        ),
        "scope_id": "700:2026",
        "trust_label": "trusted",
        "cutover_id": "espn-native-700-2026",
        "effective_at": "2026-08-01T09:00:00+00:00",
        "registry_snapshot_ref": registry_ref,
        "green_runs": green_runs,
    }
    if document["schema_version"] == LEGACY_PROMOTION_EVIDENCE_VERSION:
        document.update(
            {
                "legacy_league": "ENG-Premier League",
                "legacy_season": "2627",
            }
        )
    else:
        document["fallback"] = (
            {"kind": "absent"}
            if fallback_kind == "absent"
            else {
                "kind": "legacy",
                "league": "ENG-Premier League",
                "season": "2627",
            }
        )
    evidence_path = tmp_path / "promotion-evidence.json"
    evidence_path.write_text(json.dumps(document), encoding="utf-8")
    return evidence_path, green_runs


def _rewrite_success_parent(
    evidence_path: Path,
    green_runs: list[dict],
    *,
    index: int,
    parent: dict,
) -> None:
    document = json.loads(evidence_path.read_text())
    success_path = Path(
        green_runs[index]["success_receipt_ref"]["uri"].removeprefix("file://")
    )
    success = json.loads(success_path.read_text())
    success["parent"] = parent
    success.pop("receipt_sha256")
    success["receipt_sha256"] = hashlib.sha256(
        (
            json.dumps(
                success,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
    ).hexdigest()
    success_ref = _write_json(
        success_path.with_name(f"run-success-rewritten-{index}.json"), success
    )
    document["green_runs"][index]["success_receipt_ref"] = success_ref
    evidence_path.write_text(json.dumps(document), encoding="utf-8")


class FakeBackend:
    def __init__(self, candidate):
        self.candidate = candidate
        self.latest_manifest_candidate = candidate
        self.actions: list[str] = []
        self.baselines: dict[str, dict] = {}
        self.cutovers: dict[str, object] = {}
        self.latest = None

    def ensure_objects(self):
        self.actions.append("ensure_objects")

    def legacy_baseline(self, league, season):
        self.actions.append("capture_baseline")
        assert (league, season) == ("ENG-Premier League", "2627")
        metrics = {
            entity: {
                "row_count": count,
                "distinct_key_count": count,
                "max_ingested_at": "2026-07-30T00:00:00+00:00",
            }
            for entity, count in {
                "schedule": 380,
                "lineup": 8000,
                "matchsheet": 760,
            }.items()
        }
        self.actions.append("capture_snapshot_ids")
        snapshots = {
            "espn_schedule": 101,
            "espn_lineup": 102,
            "espn_matchsheet": 103,
        }
        return metrics, snapshots

    def absence_baseline(self):
        self.actions.append("capture_absence_baseline")
        return {
            "espn_schedule": 101,
            "espn_lineup": 102,
            "espn_matchsheet": 103,
        }

    def verify_candidate(self, candidate, registry_snapshot_ref):
        self.actions.append("verify_physical_candidate")
        assert candidate.scope_id == "700:2026"
        assert registry_snapshot_ref.uri.endswith("registry.json")

    def complete_manifest(self, scope_id, generation_id):
        self.actions.append("verify_manifest")
        if (scope_id, generation_id) != (
            "700:2026",
            self.candidate.generation_id,
        ):
            return None
        return {
            "status": "complete",
            "scope_id": scope_id,
            "generation_id": generation_id,
            "generation_signature": self.candidate.generation_signature,
            "manifest_sha256": self.candidate.manifest_sha256,
            "registry_signature": self.candidate.registry_signature,
        }

    def latest_complete_manifest(self, scope_id):
        self.actions.append("verify_latest_manifest")
        candidate = self.latest_manifest_candidate
        return {
            "status": "complete",
            "scope_id": scope_id,
            "generation_id": candidate.generation_id,
            "generation_signature": candidate.generation_signature,
            "manifest_sha256": candidate.manifest_sha256,
            "registry_signature": candidate.registry_signature,
        }

    def baseline(self, scope_id):
        return self.baselines.get(scope_id)

    def append_baseline(self, row):
        self.actions.append("append_baseline")
        self.baselines[row["scope_id"]] = dict(row)

    def append_cutover(self, cutover):
        self.actions.append("append_cutover")
        existing = self.cutovers.get(cutover.cutover_id)
        if existing is not None and existing.cutover_sha256 != cutover.cutover_sha256:
            raise MigrationError("conflicting cutover")
        self.cutovers[cutover.cutover_id] = cutover
        self.latest = cutover

    def latest_cutover(self, scope_id):
        return self.latest if self.latest and self.latest.scope_id == scope_id else None


class FakeLeaseStore:
    def __init__(self):
        self.actions = []

    def migrate(self):
        self.actions.append("migrate")

    def acquire(self, scope_id, owner_id, plan_signature, *, now):
        self.actions.append(("acquire", scope_id, owner_id, plan_signature, now))
        return object()

    @contextmanager
    def guard(self, lease, *, now):
        self.actions.append(("guard", lease, now))
        yield

    def release(self, lease, *, now):
        self.actions.append(("release", lease, now))


def test_dry_run_is_single_scope_non_mutating_and_binds_exact_artifacts(tmp_path):
    evidence_path, refs = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)

    report = build_promotion_plan(
        evidence,
        output_path=tmp_path / "promotion-result.json",
    )

    assert report["mode"] == "dry_run"
    assert report["mutates"] is False
    assert report["scope_id"] == "700:2026"
    assert (
        report["candidate"]["durable_manifest_ref"] == refs[-1]["durable_manifest_ref"]
    )
    assert report["candidate"]["raw_manifest_ref"] == refs[-1]["raw_manifest_ref"]
    assert len(report["green_runs"]) == 3
    assert BASELINE_TABLE in "\n".join(report["statements"])
    rendered = "\n".join(report["statements"] + report["rollback"]["commands"])
    assert "DROP " not in rendered.upper()
    assert str((tmp_path / "promotion-result.json").resolve()) in rendered


def test_migration_statements_never_mutate_legacy_objects():
    rendered = "\n".join(migration_statements()).upper()
    assert "DROP " not in rendered
    assert "DELETE " not in rendered
    assert "TRUNCATE " not in rendered
    assert "ALTER TABLE ICEBERG.BRONZE.ESPN_SCHEDULE " not in rendered
    assert "ALTER TABLE ICEBERG.BRONZE.ESPN_LINEUP " not in rendered
    assert "ALTER TABLE ICEBERG.BRONZE.ESPN_MATCHSHEET " not in rendered


def test_legacy_v2_v3_green_run_serializer_keeps_reviewed_bytes_and_hash():
    def ref(name: str, digit: str) -> ArtifactRef:
        return ArtifactRef(
            uri=f"file:///golden/{name}.json",
            sha256=digit * 64,
        )

    run = GreenRunEvidence(
        dag_id="dag_ingest_espn",
        run_id="espn_daily__golden",
        attempt=3,
        scope_id="700:2026",
        registry_signature="a" * 64,
        plan_signature="b" * 64,
        generation_id="generation-golden",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
        as_of=datetime(2026, 7, 31, tzinfo=UTC).date(),
        logical_date=datetime(2026, 7, 31, 14, tzinfo=UTC),
        data_interval_start=datetime(2026, 7, 31, 14, tzinfo=UTC),
        data_interval_end=datetime(2026, 8, 1, 14, tzinfo=UTC),
        parent_run_id="scheduled__2026-07-31T14:00:00+00:00",
        recorded_at=datetime(2026, 8, 1, 15, 16, 17, tzinfo=UTC),
        durable_manifest_ref=ref("durable", "0"),
        run_evidence_ref=ref("run-evidence", "1"),
        raw_manifest_ref=ref("raw", "2"),
        publication_ref=ref("publication", "3"),
        generation_snapshot_ref=ref("generation", "4"),
        published_dq_ref=ref("published-dq", "5"),
        terminal_verdict_ref=ref("terminal-verdict", "6"),
        health_ref=ref("health", "7"),
        lease_release_ref=ref("lease-release", "8"),
        success_receipt_ref=ref("success-receipt", "9"),
        run_registry_snapshot_ref=ref("registry", "a"),
    )

    # Frozen from c6927c8 in a detached worktree. Do not regenerate this golden
    # from the current serializer: v2/v3 evidence hashes are a public contract.
    expected_bytes = (
        b'{"as_of":"2026-07-31","attempt":3,"dag_id":"dag_ingest_espn",'
        b'"data_interval_end":"2026-08-01T14:00:00+00:00",'
        b'"data_interval_start":"2026-07-31T14:00:00+00:00",'
        b'"durable_manifest_ref":{"sha256":"00000000000000000000000000000000'
        b'00000000000000000000000000000000","uri":"file:///golden/durable.json"},'
        b'"generation_id":"generation-golden","generation_signature":"cccccccc'
        b'cccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
        b'"generation_snapshot_ref":{"sha256":"44444444444444444444444444444444'
        b'44444444444444444444444444444444","uri":"file:///golden/generation.json"},'
        b'"health_ref":{"sha256":"7777777777777777777777777777777777777777'
        b'777777777777777777777777","uri":"file:///golden/health.json"},'
        b'"lease_release_ref":{"sha256":"88888888888888888888888888888888'
        b'88888888888888888888888888888888","uri":"file:///golden/lease-release.json"},'
        b'"logical_date":"2026-07-31T14:00:00+00:00","manifest_sha256":"dddddddd'
        b'dddddddddddddddddddddddddddddddddddddddddddddddddddddddd",'
        b'"parent_run_id":"scheduled__2026-07-31T14:00:00+00:00",'
        b'"plan_signature":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
        b'bbbbbbbb","publication_ref":{"sha256":"33333333333333333333333333333333'
        b'33333333333333333333333333333333","uri":"file:///golden/publication.json"},'
        b'"published_dq_ref":{"sha256":"55555555555555555555555555555555'
        b'55555555555555555555555555555555","uri":"file:///golden/published-dq.json"},'
        b'"raw_manifest_ref":{"sha256":"22222222222222222222222222222222'
        b'22222222222222222222222222222222","uri":"file:///golden/raw.json"},'
        b'"recorded_at":"2026-08-01T15:16:17+00:00","registry_signature":"aaaaaaaa'
        b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"run_evidence_ref":{"sha256":"11111111111111111111111111111111'
        b'11111111111111111111111111111111","uri":"file:///golden/run-evidence.json"},'
        b'"run_id":"espn_daily__golden","run_registry_snapshot_ref":{"sha256":'
        b'"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"uri":"file:///golden/registry.json"},"scope_id":"700:2026",'
        b'"success_receipt_ref":{"sha256":"99999999999999999999999999999999'
        b'99999999999999999999999999999999","uri":"file:///golden/success-receipt.json"},'
        b'"terminal_verdict_ref":{"sha256":"66666666666666666666666666666666'
        b'66666666666666666666666666666666","uri":"file:///golden/terminal-verdict.json"}}'
    )
    expected_sha256 = "f32d9c8f2a8103857dee9a39b8abb8cb479fd73aae739e235e48c638b96738c4"

    payload = run.to_dict()
    assert canonical_json(payload).encode("utf-8") == expected_bytes
    assert hashlib.sha256(expected_bytes).hexdigest() == expected_sha256
    assert canonical_sha256(payload) == expected_sha256


def test_cli_defaults_to_dry_run_and_does_not_construct_live_adapters(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    output = tmp_path / "migration.json"

    def bomb():
        raise AssertionError("dry-run opened a live adapter")

    assert (
        main(
            [
                "promote",
                "--evidence",
                str(evidence_path),
                "--output",
                str(output),
            ],
            backend_factory=bomb,
            lease_factory=bomb,
        )
        == 0
    )
    result = json.loads(output.read_text())
    assert result["mode"] == "dry_run"
    assert result["mutates"] is False
    assert result["scope_id"] == "700:2026"
    script = Path(__file__).resolve().parents[3] / "scripts/migrate_espn_native_v2.py"
    assert os.access(script, os.X_OK)


def test_apply_cli_emits_honest_machine_failure_when_live_adapter_breaks(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    output = tmp_path / "migration-failure.json"

    def broken_backend():
        raise RuntimeError("database unavailable")

    exit_code = main(
        [
            "promote",
            "--evidence",
            str(evidence_path),
            "--output",
            str(output),
            "--apply",
        ],
        backend_factory=broken_backend,
        lease_factory=lambda: FakeLeaseStore(),
    )

    report = json.loads(output.read_text())
    assert exit_code == 2
    assert report["status"] == "failed"
    assert report["mutates"] is True
    assert report["error_type"] == "RuntimeError"


def test_apply_captures_baseline_before_one_cutover_and_retry_is_idempotent(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])
    leases = FakeLeaseStore()

    first = apply_promotion(evidence, backend=backend, lease_store=leases, now=NOW)
    second = apply_promotion(evidence, backend=backend, lease_store=leases, now=NOW)

    assert first["status"] == second["status"] == "promoted"
    assert backend.actions.index("append_baseline") < backend.actions.index(
        "append_cutover"
    )
    assert len(backend.baselines) == 1
    assert len(backend.cutovers) == 1
    baseline = backend.baselines["700:2026"]
    assert baseline["trust_label"] == "trusted"
    assert baseline["entity_metrics_json"].startswith("{")
    assert first["candidate"]["generation_id"] == evidence.green_runs[-1].generation_id
    assert leases.actions[-1][0] == "release"


def test_stale_or_missing_complete_manifest_never_appends_cutover(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])
    backend.candidate = replace(evidence.green_runs[-1], manifest_sha256="1" * 64)

    with pytest.raises(MigrationError, match="COMPLETE manifest"):
        apply_promotion(
            evidence,
            backend=backend,
            lease_store=FakeLeaseStore(),
            now=NOW,
        )

    assert "append_cutover" not in backend.actions


def test_candidate_must_still_be_latest_complete_under_the_scope_lease(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])
    backend.latest_manifest_candidate = replace(
        evidence.green_runs[-1],
        generation_id="generation-newer",
        generation_signature="9" * 64,
        manifest_sha256="8" * 64,
    )

    with pytest.raises(MigrationError, match="latest COMPLETE"):
        apply_promotion(
            evidence,
            backend=backend,
            lease_store=FakeLeaseStore(),
            now=NOW,
        )

    assert "append_baseline" not in backend.actions
    assert "append_cutover" not in backend.actions


def test_future_effective_cutover_fails_before_any_database_action(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])

    with pytest.raises(MigrationError, match="future"):
        apply_promotion(
            evidence,
            backend=backend,
            lease_store=FakeLeaseStore(),
            now=NOW.replace(hour=8),
        )

    assert backend.actions == []


def test_pre_2016_or_multi_scope_evidence_fails_closed(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    document = json.loads(evidence_path.read_text())
    document["scope_id"] = "700:2015"
    document["trust_label"] = "legacy_untrusted"
    bad = tmp_path / "untrusted.json"
    bad.write_text(json.dumps(document))
    with pytest.raises(MigrationError, match="legacy_untrusted"):
        load_promotion_evidence(bad)

    document = json.loads(evidence_path.read_text())
    document["scope_ids"] = ["700:2026", "740:2026"]
    bad.write_text(json.dumps(document))
    with pytest.raises(MigrationError, match="schema"):
        load_promotion_evidence(bad)


def test_cutover_time_must_follow_all_three_green_runs(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    document = json.loads(evidence_path.read_text())
    document["effective_at"] = "2026-07-30T07:59:59+00:00"
    evidence_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(MigrationError, match="after the third green run"):
        load_promotion_evidence(evidence_path)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("owner_profile", "unknown-v9", "profile"),
        ("schema", "espn-master-parent-v1", "identity"),
        ("parent_dag_id", "dag_master_pipeline", "identity"),
        ("parent_task_id", "ingestion_triggers.trigger_espn", "identity"),
        ("parent_run_type", "manual", "identity"),
        ("child_dag_id", "dag_repair_espn", "identity"),
    ],
)
def test_promotion_rejects_nonisolated_parent_v2_evidence(
    tmp_path, field, value, message
):
    evidence_path, green = _evidence(tmp_path)
    success_path = Path(green[0]["success_receipt_ref"]["uri"].removeprefix("file://"))
    parent = json.loads(success_path.read_text())["parent"]
    parent[field] = value
    _rewrite_success_parent(evidence_path, green, index=0, parent=parent)

    with pytest.raises(MigrationError, match=message):
        load_promotion_evidence(evidence_path)


def test_promotion_rejects_old_master_v1_envelope(tmp_path):
    evidence_path, green = _evidence(tmp_path)
    success_path = Path(green[0]["success_receipt_ref"]["uri"].removeprefix("file://"))
    receipt = json.loads(success_path.read_text())
    v2 = receipt["parent"]
    old_parent = {
        "schema": "espn-master-parent-v1",
        "parent_dag_id": "dag_master_pipeline",
        "parent_run_id": v2["parent_run_id"],
        "logical_date": v2["logical_date"],
        "data_interval_start": v2["data_interval_start"],
        "data_interval_end": v2["data_interval_end"],
        "child_run_id": v2["child_run_id"],
    }
    _rewrite_success_parent(evidence_path, green, index=0, parent=old_parent)

    with pytest.raises(MigrationError, match="parent schema"):
        load_promotion_evidence(evidence_path)


def test_promotion_requires_exact_adjacent_daily_owner_intervals(tmp_path):
    evidence_path, green = _evidence(tmp_path)
    success_path = Path(green[1]["success_receipt_ref"]["uri"].removeprefix("file://"))
    parent = json.loads(success_path.read_text())["parent"]
    parent["data_interval_start"] = (
        datetime.fromisoformat(parent["data_interval_start"]) + timedelta(minutes=1)
    ).isoformat()
    _rewrite_success_parent(evidence_path, green, index=1, parent=parent)

    with pytest.raises(MigrationError, match="daily interval"):
        load_promotion_evidence(evidence_path)


def test_rollback_is_a_successor_bound_to_exact_promotion(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])
    promotion = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    rollback_plan = build_rollback_plan(
        promotion,
        reason="canary regression",
        output_path=tmp_path / "rollback-result.json",
    )

    result = apply_rollback(
        rollback_plan,
        backend=backend,
        lease_store=FakeLeaseStore(),
        now=NOW,
    )

    rollback = backend.latest
    assert result["status"] == "rolled_back"
    assert rollback.active_source == "legacy"
    assert rollback.predecessor_cutover_id == promotion["cutover"]["cutover_id"]
    assert rollback.predecessor_cutover_sha256 == promotion["cutover"]["cutover_sha256"]
    assert (
        rollback.ancestor_cutover_sha256s[-1] == promotion["cutover"]["cutover_sha256"]
    )
    result_base = {
        key: value for key, value in result.items() if key != "result_sha256"
    }
    assert result["result_sha256"] == canonical_sha256(result_base)


def test_repository_adapter_accepts_positional_trino_rows_and_normalizes_time():
    baseline_values = (
        "espn-legacy-baseline-v1",
        "700:2026",
        "ENG-Premier League",
        "2627",
        NOW.replace(tzinfo=None),
        "{}",
        "{}",
        "a" * 64,
        "file:///durable.json",
        "b" * 64,
        "file:///raw.json",
        "c" * 64,
        "trusted",
        "d" * 64,
    )
    cutover_values = (
        "espn-native-700-2026",
        "700:2026",
        "native",
        "legacy",
        None,
        None,
        "ENG-Premier League",
        "2627",
        "a" * 64,
        NOW.replace(tzinfo=None),
        "generation-3",
        "e" * 64,
        "f" * 64,
        None,
        None,
        '{"migration_version":"test"}',
        "[]",
    )

    class FakeRepository:
        catalog = "iceberg"
        schema = "bronze"

        def _execute(self, sql, params=()):
            assert params == ("700:2026",)
            if BASELINE_TABLE in sql:
                assert "SELECT *" not in sql.upper()
                return [baseline_values]
            if MANIFEST_TABLE in sql:
                assert "ORDER BY" in sql and "LIMIT 1" in sql
                return [
                    (
                        "complete",
                        "700:2026",
                        "generation-3",
                        "e" * 64,
                        "f" * 64,
                        "a" * 64,
                    )
                ]
            return [cutover_values]

    backend = RepositoryMigrationBackend(FakeRepository())

    baseline = backend.baseline("700:2026")
    manifest = backend.latest_complete_manifest("700:2026")
    cutover = backend.latest_cutover("700:2026")

    assert baseline["baseline_version"] == "espn-legacy-baseline-v1"
    assert baseline["baseline_sha256"] == "d" * 64
    assert manifest["generation_id"] == "generation-3"
    assert cutover.effective_at == NOW


def test_repository_adapter_accepts_nullable_native_only_fallback_aliases():
    cutover_values = (
        "espn-native-700-2026",
        "700:2026",
        "native",
        "absent",
        None,
        None,
        None,
        None,
        "a" * 64,
        NOW.replace(tzinfo=None),
        "generation-3",
        "e" * 64,
        "f" * 64,
        None,
        None,
        '{"fallback":{"kind":"absent"},"migration_version":"test"}',
        "[]",
    )

    class Repository:
        catalog = "iceberg"
        schema = "bronze"

        def _execute(self, sql, params=()):
            assert params == ("700:2026",)
            return [cutover_values]

    cutover = RepositoryMigrationBackend(Repository()).latest_cutover("700:2026")

    assert cutover.active_source == "native"
    assert cutover.previous_source == "absent"
    assert cutover.legacy_league is None
    assert cutover.legacy_season is None


def test_post_cutover_retry_recovers_even_if_ingestion_head_advanced(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])

    first = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    backend.latest_manifest_candidate = replace(
        evidence.green_runs[-1],
        generation_id="generation-after-report-loss",
        generation_signature="8" * 64,
        manifest_sha256="9" * 64,
    )

    recovered = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )

    assert recovered["cutover"]["cutover_sha256"] == first["cutover"]["cutover_sha256"]


def test_physical_generation_verification_is_inside_promotion_gate(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])

    def reject(*_args):
        backend.actions.append("verify_physical_candidate")
        raise MigrationError("physical candidate verification failed")

    backend.verify_candidate = reject
    with pytest.raises(MigrationError, match="physical candidate"):
        apply_promotion(
            evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
        )

    assert "verify_physical_candidate" in backend.actions
    assert "append_baseline" not in backend.actions
    assert "append_cutover" not in backend.actions


def test_registry_aliases_are_bound_to_exact_promoted_edition(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    document = json.loads(evidence_path.read_text())
    document["legacy_league"] = "ESP-La Liga"
    evidence_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(MigrationError, match="aliases do not match"):
        load_promotion_evidence(evidence_path)


def test_warning_health_cannot_be_present_in_a_green_run(tmp_path):
    evidence_path, green = _evidence(tmp_path)
    document = json.loads(evidence_path.read_text())
    first = green[0]
    health_path = Path(first["health_ref"]["uri"].removeprefix("file://"))
    health = json.loads(health_path.read_text())
    health["alerts"] = [{"severity": "warning", "code": "stale"}]
    health_ref = _write_json(health_path.with_name("health-warning.json"), health)
    receipt_path = Path(first["success_receipt_ref"]["uri"].removeprefix("file://"))
    receipt = json.loads(receipt_path.read_text())
    receipt["health_ref"] = health_ref
    receipt.pop("receipt_sha256")
    receipt["receipt_sha256"] = hashlib.sha256(
        (
            json.dumps(
                receipt,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
    ).hexdigest()
    success_ref = _write_json(
        receipt_path.with_name("run-success-warning.json"), receipt
    )
    document["green_runs"][0]["health_ref"] = health_ref
    document["green_runs"][0]["success_receipt_ref"] = success_ref
    evidence_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(MigrationError, match="contains an alert"):
        load_promotion_evidence(evidence_path)


def test_legacy_baseline_uses_main_refs_and_exact_time_travel_snapshots():
    statements = []
    snapshot_by_table = {
        "espn_schedule": 101,
        "espn_lineup": 102,
        "espn_matchsheet": 103,
    }

    class Repository:
        catalog = "iceberg"
        schema = "bronze"

        def _execute(self, sql, params=()):
            statements.append((sql, params))
            table = next(name for name in snapshot_by_table if name in sql)
            if "$refs" in sql:
                assert params == ()
                return [(snapshot_by_table[table],)]
            assert f"FOR VERSION AS OF {snapshot_by_table[table]}" in sql
            assert params == ("ENG-Premier League", "2627")
            return [(10, 10, 0, None, None, "2026-07-31T00:00:00+00:00")]

    metrics, snapshots = RepositoryMigrationBackend(Repository()).legacy_baseline(
        "ENG-Premier League", "2627"
    )

    assert set(metrics) == {"schedule", "lineup", "matchsheet"}
    assert snapshots == snapshot_by_table
    rendered = "\n".join(sql for sql, _ in statements).upper()
    assert "$REFS" in rendered
    assert "FOR VERSION AS OF" in rendered
    assert "MAX(SNAPSHOT_ID)" not in rendered


def test_absence_baseline_captures_only_exact_main_snapshot_ids():
    statements = []
    snapshot_by_table = {
        "espn_schedule": 201,
        "espn_lineup": 202,
        "espn_matchsheet": 203,
    }

    class Repository:
        catalog = "iceberg"
        schema = "bronze"

        def _execute(self, sql, params=()):
            statements.append((sql, params))
            assert "$refs" in sql
            assert params == ()
            table = next(name for name in snapshot_by_table if name in sql)
            return [(snapshot_by_table[table],)]

    snapshots = RepositoryMigrationBackend(Repository()).absence_baseline()

    assert snapshots == snapshot_by_table
    assert len(statements) == 3
    assert all("name = 'main' AND type = 'BRANCH'" in sql for sql, _ in statements)


def test_rollback_repair_and_repromotion_extend_append_only_ancestry(tmp_path):
    first_path, _ = _evidence(tmp_path / "first")
    first_evidence = load_promotion_evidence(first_path)
    backend = FakeBackend(first_evidence.green_runs[-1])
    promotion = apply_promotion(
        first_evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    rollback_plan = build_rollback_plan(
        promotion, reason="repair needed", output_path=tmp_path / "rollback.json"
    )
    apply_rollback(
        rollback_plan, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    rollback = backend.latest

    second_path, _ = _evidence(tmp_path / "second")
    document = json.loads(second_path.read_text())
    document["cutover_id"] = (
        "espn-native-700-2026-repromote-" + rollback.cutover_sha256[:16]
    )
    document["effective_at"] = "2026-08-01T09:00:01+00:00"
    second_path.write_text(json.dumps(document), encoding="utf-8")
    second_evidence = load_promotion_evidence(second_path)
    backend.candidate = second_evidence.green_runs[-1]
    backend.latest_manifest_candidate = second_evidence.green_runs[-1]

    repromotion = apply_promotion(
        second_evidence,
        backend=backend,
        lease_store=FakeLeaseStore(),
        now=NOW + timedelta(seconds=2),
    )

    assert repromotion["cutover"]["predecessor_cutover_id"] == rollback.cutover_id
    assert json.loads(repromotion["cutover"]["ancestor_cutover_sha256_json"])[-1] == (
        rollback.cutover_sha256
    )


def test_frozen_181_registry_has_exact_honest_fallback_descriptors():
    fixture = (
        Path(__file__).resolve().parents[2] / "fixtures/espn/catalog_2026-07-31.json"
    )
    snapshot = CatalogSnapshot.from_dict(
        json.loads(fixture.read_text(encoding="utf-8"))
    )
    registry = build_discovered_male_registry(
        snapshot,
        legacy_registry=load_registry(),
    )

    fallbacks = registry_fallback_descriptors(registry)

    assert tuple(fallbacks) == tuple(sorted(fallbacks))
    assert len(fallbacks) == 181
    assert sum(item.kind == "legacy" for item in fallbacks.values()) == 9
    assert sum(item.kind == "absent" for item in fallbacks.values()) == 172
    for scope_id, fallback in fallbacks.items():
        competition = registry.by_id[int(scope_id.split(":", 1)[0])]
        if competition.legacy is None:
            assert fallback == FallbackDescriptor(kind="absent")
        else:
            assert fallback.kind == "legacy"
            assert fallback.league == competition.legacy.league
            assert (
                fallback.season
                in competition.legacy.season_aliases[int(scope_id.split(":", 1)[1])]
            )


@pytest.mark.parametrize(
    "fallback",
    [
        {"kind": "absent", "league": "must-not-be-here"},
        {"kind": "legacy", "league": "ENG-Premier League"},
        {"kind": "unknown"},
    ],
)
def test_v3_promotion_evidence_rejects_malformed_fallback_descriptor(
    tmp_path, fallback
):
    evidence_path, _ = _evidence(tmp_path, evidence_v3=True)
    document = json.loads(evidence_path.read_text())
    document["fallback"] = fallback
    evidence_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(MigrationError, match="fallback"):
        load_promotion_evidence(evidence_path)


def test_promotion_fallback_must_match_registry_route_kind(tmp_path):
    legacy_path, _ = _evidence(tmp_path / "legacy", evidence_v3=True)
    legacy_document = json.loads(legacy_path.read_text())
    legacy_document["fallback"] = {"kind": "absent"}
    legacy_path.write_text(json.dumps(legacy_document), encoding="utf-8")
    with pytest.raises(MigrationError, match="fallback.*registry"):
        load_promotion_evidence(legacy_path)

    absent_path, _ = _evidence(tmp_path / "absent", fallback_kind="absent")
    absent_document = json.loads(absent_path.read_text())
    absent_document["fallback"] = {
        "kind": "legacy",
        "league": "ENG-Premier League",
        "season": "2627",
    }
    absent_path.write_text(json.dumps(absent_document), encoding="utf-8")
    with pytest.raises(MigrationError, match="fallback.*registry"):
        load_promotion_evidence(absent_path)


def test_native_only_promotion_is_idempotent_and_rolls_back_to_absent(tmp_path):
    evidence_path, _ = _evidence(tmp_path, fallback_kind="absent")
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])

    first = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    second = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )

    assert first["cutover"]["cutover_sha256"] == second["cutover"]["cutover_sha256"]
    assert first["fallback"] == {"kind": "absent"}
    assert "capture_baseline" not in backend.actions
    assert backend.actions.count("capture_absence_baseline") == 1
    baseline = backend.baselines["700:2026"]
    assert baseline["baseline_version"] == ABSENCE_BASELINE_VERSION
    assert baseline["legacy_league"] is None
    assert baseline["legacy_season"] is None
    assert json.loads(baseline["entity_metrics_json"]) == {}
    assert backend.latest.active_source == "native"
    assert backend.latest.previous_source == "absent"
    assert backend.latest.legacy_league is None
    assert backend.latest.legacy_season is None

    rollback_plan = build_rollback_plan(
        first,
        reason="native-only canary regression",
        output_path=tmp_path / "rollback.json",
    )
    assert rollback_plan["fallback"] == {"kind": "absent"}
    result = apply_rollback(
        rollback_plan,
        backend=backend,
        lease_store=FakeLeaseStore(),
        now=NOW,
    )
    retry = apply_rollback(
        rollback_plan,
        backend=backend,
        lease_store=FakeLeaseStore(),
        now=NOW,
    )

    assert result["status"] == retry["status"] == "rolled_back"
    assert result["cutover"]["cutover_sha256"] == retry["cutover"]["cutover_sha256"]
    assert len(backend.cutovers) == 2
    assert backend.latest.active_source == "absent"
    assert backend.latest.previous_source == "native"
    assert backend.latest.legacy_league is None
    assert backend.latest.legacy_season is None


def test_v2_legacy_evidence_keeps_strict_legacy_baseline_and_rollback(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])

    promotion = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    rollback_plan = build_rollback_plan(
        promotion,
        reason="legacy canary regression",
        output_path=tmp_path / "rollback.json",
    )
    apply_rollback(
        rollback_plan,
        backend=backend,
        lease_store=FakeLeaseStore(),
        now=NOW,
    )

    assert evidence.fallback == FallbackDescriptor(
        kind="legacy", league="ENG-Premier League", season="2627"
    )
    assert "capture_baseline" in backend.actions
    assert promotion["fallback"] == {
        "kind": "legacy",
        "league": "ENG-Premier League",
        "season": "2627",
    }
    assert backend.latest.active_source == "legacy"
    assert backend.latest.legacy_league == "ENG-Premier League"
    assert backend.latest.legacy_season == "2627"


def test_v2_legacy_retry_recognizes_exact_v1_committed_cutover(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])
    apply_promotion(evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW)
    current = backend.latest
    legacy_metadata = dict(current.metadata)
    legacy_metadata.pop("fallback")
    legacy_metadata["migration_version"] = LEGACY_MIGRATION_VERSION
    legacy_cutover = type(current)(
        **{
            **current.constructor_values(),
            "metadata": legacy_metadata,
        }
    )
    backend.latest = legacy_cutover
    backend.cutovers = {legacy_cutover.cutover_id: legacy_cutover}

    recovered = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    rollback = build_rollback_plan(
        recovered,
        reason="v1 compatibility",
        output_path=tmp_path / "rollback.json",
    )

    assert recovered["cutover"]["cutover_sha256"] == legacy_cutover.cutover_sha256
    assert rollback["fallback"] == {
        "kind": "legacy",
        "league": "ENG-Premier League",
        "season": "2627",
    }


def test_v3_legacy_evidence_is_readable_with_exact_reviewed_aliases(tmp_path):
    evidence_path, _ = _evidence(tmp_path, evidence_v3=True)

    evidence = load_promotion_evidence(evidence_path)

    assert evidence.evidence_version == PROMOTION_EVIDENCE_VERSION
    assert evidence.fallback == FallbackDescriptor(
        kind="legacy", league="ENG-Premier League", season="2627"
    )


def test_rehashed_nonempty_absence_baseline_is_rejected_on_retry(tmp_path):
    evidence_path, _ = _evidence(tmp_path, fallback_kind="absent")
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])
    apply_promotion(evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW)
    baseline = backend.baselines[evidence.scope_id]
    baseline["entity_metrics_json"] = '{"schedule":{"row_count":1}}'
    baseline["baseline_sha256"] = canonical_sha256(
        {key: value for key, value in baseline.items() if key != "baseline_sha256"}
    )

    with pytest.raises(MigrationError, match="absence baseline"):
        apply_promotion(
            evidence,
            backend=backend,
            lease_store=FakeLeaseStore(),
            now=NOW,
        )


def test_rollback_rejects_rehashed_fallback_that_differs_from_native_parent(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])
    promotion = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    plan = build_rollback_plan(
        promotion,
        reason="tamper test",
        output_path=tmp_path / "rollback.json",
    )
    plan["fallback"] = {"kind": "absent"}
    plan["plan_sha256"] = canonical_sha256(
        {key: value for key, value in plan.items() if key != "plan_sha256"}
    )

    with pytest.raises(MigrationError, match="fallback.*native predecessor"):
        apply_rollback(
            plan,
            backend=backend,
            lease_store=FakeLeaseStore(),
            now=NOW,
        )

    assert backend.latest.active_source == "native"
    assert len(backend.cutovers) == 1


def test_legacy_v1_rollback_plan_remains_readable_and_strict(tmp_path):
    evidence_path, _ = _evidence(tmp_path)
    evidence = load_promotion_evidence(evidence_path)
    backend = FakeBackend(evidence.green_runs[-1])
    promotion = apply_promotion(
        evidence, backend=backend, lease_store=FakeLeaseStore(), now=NOW
    )
    plan = build_rollback_plan(
        promotion,
        reason="legacy plan compatibility",
        output_path=tmp_path / "rollback.json",
    )
    plan["schema_version"] = LEGACY_ROLLBACK_PLAN_VERSION
    plan.pop("fallback")
    plan["legacy_league"] = "ENG-Premier League"
    plan["legacy_season"] = "2627"
    plan["plan_sha256"] = canonical_sha256(
        {key: value for key, value in plan.items() if key != "plan_sha256"}
    )

    result = apply_rollback(
        plan,
        backend=backend,
        lease_store=FakeLeaseStore(),
        now=NOW,
    )

    assert result["status"] == "rolled_back"
    assert backend.latest.active_source == "legacy"
    assert backend.latest.legacy_league == "ENG-Premier League"
    assert backend.latest.legacy_season == "2627"
