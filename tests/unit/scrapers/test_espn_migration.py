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
    BASELINE_TABLE,
    MigrationError,
    RepositoryMigrationBackend,
    apply_promotion,
    apply_rollback,
    build_promotion_plan,
    build_rollback_plan,
    load_promotion_evidence,
    migration_statements,
)
from scrapers.espn.repository import MANIFEST_TABLE, canonical_sha256
from scrapers.espn.registry import load_registry
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


def _evidence(tmp_path: Path) -> tuple[Path, list[dict]]:
    registry = load_registry()
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
        "schema_version": "espn-v2-promotion-evidence-v2",
        "scope_id": "700:2026",
        "legacy_league": "ENG-Premier League",
        "legacy_season": "2627",
        "trust_label": "trusted",
        "cutover_id": "espn-native-700-2026",
        "effective_at": "2026-08-01T09:00:00+00:00",
        "registry_snapshot_ref": registry_ref,
        "green_runs": green_runs,
    }
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
