from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import pytest

from scrapers.espn import runner
from scrapers.espn.discovery import CatalogSnapshot
from scrapers.espn.migration import (
    MigrationError,
    PROMOTION_EVIDENCE_VERSION,
    V4_PROMOTION_EVIDENCE_VERSION,
    apply_promotion,
    load_promotion_evidence,
)
from scrapers.espn.models import DispositionState, IngestPlan, ScopePlan
from scrapers.espn.parser_contracts import PARSER_VERSION
from scrapers.espn.registry import build_discovered_male_registry, load_registry
from scrapers.espn.repository import (
    RawLedgerRecord,
    ScopeGeneration,
    validate_scope_generation,
)


UTC = timezone.utc
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "espn"


def _write(path: Path, payload: object) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    path.write_bytes(body)
    return {"uri": path.as_uri(), "sha256": hashlib.sha256(body).hexdigest()}


def _receipt(payload: dict) -> dict:
    payload = dict(payload)
    payload["receipt_sha256"] = hashlib.sha256(
        (
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
    ).hexdigest()
    return payload


def _frozen_registry():
    catalog = CatalogSnapshot.from_dict(
        json.loads((FIXTURES / "catalog_2026-07-31.json").read_text())
    )
    return build_discovered_male_registry(catalog, legacy_registry=load_registry())


def _scope(registry) -> ScopePlan:
    competition = registry.by_id[700]
    edition = competition.current_edition
    return ScopePlan(
        scope_id=competition.scope_id(edition),
        espn_id=competition.espn_id,
        slug=competition.slug,
        source_season_year=edition.source_season_year,
        start_date=edition.start_date,
        end_date=edition.end_date,
        capabilities=edition.capabilities,
    )


def _physical_generation(
    *, scope: ScopePlan, registry_ref: dict[str, str], registry_signature: str
) -> ScopeGeneration:
    ledger = RawLedgerRecord(
        request_id="scoreboard:physical-v3",
        endpoint="scoreboard",
        event_id=None,
        disposition=DispositionState.CAPTURED,
        raw_uri="file:///immutable/physical-scoreboard.json.gz",
        raw_sha256="a" * 64,
        fetched_at=datetime(2026, 7, 27, 8, tzinfo=UTC),
        direct_bytes=100,
        proxy_bytes=0,
        event_ids=(),
    )
    generation = ScopeGeneration(
        plan=scope,
        run_id="physical-complete-run-v3",
        generation_id="physical-generation-v3",
        registry_snapshot_uri=registry_ref["uri"],
        registry_signature=registry_signature,
        plan_signature="f" * 64,
        parser_version=PARSER_VERSION,
        runtime_version="espn-native-runtime-v3",
        ingested_at=datetime(2026, 7, 27, 9, tzinfo=UTC),
        batch_id="physical-batch-v3",
        schedule=(),
        lineup=(),
        matchsheet=(),
        planned_request_ids=(ledger.request_id,),
        raw_ledger=(ledger,),
        dispositions=(),
    )
    assert validate_scope_generation(generation).passed
    return generation


def _signed_plan(
    root: Path,
    *,
    scope: ScopePlan,
    registry_ref: dict[str, str],
    registry_signature: str,
    run_id: str,
    attempt: int,
    as_of: date,
    prior: dict[str, str],
) -> tuple[dict[str, str], str]:
    binding = {
        "active": False,
        "initial_capture": False,
        "scoreboard_max_range_days": runner.SCOREBOARD_MAX_RANGE_DAYS,
        "generation_id": f"qualification-generation-{attempt}",
        "batch_id": hashlib.sha256(f"batch-{attempt}".encode()).hexdigest(),
        "ingested_at": datetime.combine(as_of, datetime.min.time(), tzinfo=UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "generation_snapshot_uri": (root / "generation.json").as_uri(),
        "known_nonterminal_events": [],
        "prior": prior,
    }
    plan = IngestPlan(
        schema_version=1,
        run_id=run_id,
        as_of=as_of,
        registry_signature=registry_signature,
        scopes=(scope,),
        metadata={
            "runtime": {
                "mode": "daily",
                "attempt": attempt,
                "registry_snapshot_uri": registry_ref["uri"],
                "raw_manifest_uri": (root / "raw.json").as_uri(),
                "output_uri": (root / "runner-result.json").as_uri(),
                "raw_store_uri": (root / "raw-store").as_uri(),
                "max_events": 100,
                "selected_scopes": [scope.scope_id],
                "scope_bindings": {scope.scope_id: binding},
                "replay_source": None,
            }
        },
    )
    return _write(
        root / "plan.json",
        {
            "kind": runner.PLAN_KIND,
            "plan": plan.to_dict(),
            "signature": plan.signature(),
        },
    ), plan.signature()


def _raw_manifest(
    *, run_id: str, attempt: int, scope_id: str, registry: str, plan: str, as_of: date
) -> dict:
    base = {
        "kind": "espn-raw-run-manifest-v1",
        "schema_version": 1,
        "run_id": run_id,
        "attempt": attempt,
        "mode": "daily",
        "as_of": as_of.isoformat(),
        "registry_signature": registry,
        "plan_signature": plan,
        "selected_scopes": [scope_id],
        "checkpoints": [],
    }
    body = json.dumps(base, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    return {**base, "manifest_sha256": hashlib.sha256(body).hexdigest()}


def _v4_evidence(tmp_path: Path, states: tuple[str, str, str]) -> Path:
    registry = _frozen_registry()
    assert len(registry.promoted) == 181
    registry_ref = _write(
        tmp_path / "registry.json", json.loads(registry.canonical_json())
    )
    registry_signature = registry.signature()
    scope = _scope(registry)
    physical = _physical_generation(
        scope=scope,
        registry_ref=registry_ref,
        registry_signature=registry_signature,
    )
    snapshot_path = tmp_path / "physical" / "generation.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_body = runner.scope_snapshot_bytes(physical)
    snapshot_path.write_bytes(snapshot_body)
    snapshot_ref = {
        "uri": snapshot_path.as_uri(),
        "sha256": hashlib.sha256(snapshot_body).hexdigest(),
    }
    prior = {
        "uri": snapshot_ref["uri"],
        "artifact_sha256": snapshot_ref["sha256"],
        "scope_id": scope.scope_id,
        "generation_id": physical.generation_id,
        "generation_signature": physical.generation_signature,
        "manifest_sha256": physical.manifest_sha256,
    }
    selected_head = {
        "dag_id": "dag_ingest_espn",
        "scope_id": scope.scope_id,
        "generation_id": physical.generation_id,
        "generation_signature": physical.generation_signature,
        "manifest_sha256": physical.manifest_sha256,
        "snapshot_uri": snapshot_ref["uri"],
        "snapshot_sha256": snapshot_ref["sha256"],
        "registry_signature": registry_signature,
        "plan_signature": physical.plan_signature,
        "run_id": physical.run_id,
        "published_at": "2026-07-27T09:01:00+00:00",
        "completed_at": physical.ingested_at.isoformat(),
    }
    green_runs = []
    for index, state in enumerate(states, start=1):
        as_of = date(2026, 7, 27) + timedelta(days=index)
        logical_date = datetime.combine(as_of, datetime.min.time(), tzinfo=UTC).replace(
            hour=14
        )
        parent_run_id = f"scheduled__{logical_date.isoformat()}"
        run_id = f"espn_daily__dag_trigger_espn_daily__{parent_run_id}"
        root = tmp_path / f"run-{index}"
        plan_ref, plan_signature = _signed_plan(
            root,
            scope=scope,
            registry_ref=registry_ref,
            registry_signature=registry_signature,
            run_id=run_id,
            attempt=index,
            as_of=as_of,
            prior=prior,
        )
        if state == "complete":
            physical = replace(
                physical,
                run_id=run_id,
                generation_id=f"qualification-generation-{index}",
                plan_signature=plan_signature,
                ingested_at=datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
                batch_id=hashlib.sha256(f"batch-{index}".encode()).hexdigest(),
            )
            snapshot_path = root / "generation.json"
            snapshot_body = runner.scope_snapshot_bytes(physical)
            snapshot_path.write_bytes(snapshot_body)
            snapshot_ref = {
                "uri": snapshot_path.as_uri(),
                "sha256": hashlib.sha256(snapshot_body).hexdigest(),
            }
            prior = {
                "uri": snapshot_ref["uri"],
                "artifact_sha256": snapshot_ref["sha256"],
                "scope_id": scope.scope_id,
                "generation_id": physical.generation_id,
                "generation_signature": physical.generation_signature,
                "manifest_sha256": physical.manifest_sha256,
            }
            selected_head = {
                **selected_head,
                "generation_id": physical.generation_id,
                "generation_signature": physical.generation_signature,
                "manifest_sha256": physical.manifest_sha256,
                "snapshot_uri": snapshot_ref["uri"],
                "snapshot_sha256": snapshot_ref["sha256"],
                "plan_signature": physical.plan_signature,
                "run_id": physical.run_id,
                "published_at": (logical_date + timedelta(minutes=30)).isoformat(),
                "completed_at": physical.ingested_at.isoformat(),
            }
        descriptor_ref = _write(
            root / "scope-plan-descriptor.json",
            {
                "kind": "espn-scope-plan-descriptor-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "run_id": run_id,
                "attempt": index,
                "mode": "daily",
                "scope_id": scope.scope_id,
                "plan_ref": plan_ref,
                "plan_signature": plan_signature,
                "raw_manifest_uri": (root / "raw.json").as_uri(),
                "raw_store_uri": (root / "raw-store").as_uri(),
                "generation_snapshot_uri": (root / "generation.json").as_uri(),
                "expected_scoreboard_batch": None,
                "scoreboard_checkpoint_uri": None,
                "scope_root": root.as_uri(),
            },
        )
        lease_epoch = index
        binding_ref = _write(
            root / "lease-binding.json",
            {
                "kind": "espn-scope-lease-binding-v1",
                "schema_version": 1,
                "scope_plan_ref": descriptor_ref,
                "lease": {
                    "scope_id": scope.scope_id,
                    "owner_id": f"dag_ingest_espn/{run_id}/{index}",
                    "plan_signature": plan_signature,
                    "epoch": lease_epoch,
                    "token_sha256": str(index) * 64,
                    "acquired_at": (logical_date - timedelta(minutes=5)).isoformat(),
                    "expires_at": (logical_date + timedelta(hours=1)).isoformat(),
                },
            },
        )
        intent_ref = None
        if state == "complete":
            intent_ref = _write(
                root / "publication-intent.json",
                {
                    "kind": "espn-publication-intent-v1",
                    "schema_version": 1,
                    "dag_id": "dag_ingest_espn",
                    "run_id": run_id,
                    "attempt": index,
                    "scope_id": scope.scope_id,
                    "plan_signature": plan_signature,
                    "registry_signature": registry_signature,
                    "generation_id": physical.generation_id,
                    "generation_signature": physical.generation_signature,
                    "manifest_sha256": physical.manifest_sha256,
                    "snapshot_ref": snapshot_ref,
                    "prepared_at": (logical_date + timedelta(minutes=20)).isoformat(),
                },
            )
        evidence_payload = {
            "kind": "espn-run-manifest-evidence-v1",
            "schema_version": 1,
            "dag_id": "dag_ingest_espn",
            "run_id": run_id,
            "attempt": index,
            "scope_id": scope.scope_id,
            "state": state,
            "plan_signature": plan_signature,
            "registry_signature": registry_signature,
            "generation_id": physical.generation_id,
            "generation_signature": physical.generation_signature,
            "manifest_sha256": physical.manifest_sha256,
            "lease_epoch": lease_epoch,
            "publication_intent_ref": intent_ref,
            "recorded_at": (logical_date + timedelta(minutes=30)).isoformat(),
        }
        evidence_ref = _write(root / "run-evidence.json", evidence_payload)
        publication = {
            "kind": "espn-publication-result-v1",
            "schema_version": 1,
            "scope_binding_ref": binding_ref,
            "state": state,
            "snapshot_ref": snapshot_ref,
            "evidence_ref": evidence_ref,
            "publication_intent_ref": evidence_payload["publication_intent_ref"],
            "selected_head": selected_head,
            "direct_requests": 0,
            "proxy_bytes": 0,
        }
        publication_ref = _write(root / "publication.json", publication)
        quality = {
            "scope_id": scope.scope_id,
            "passed": True,
            "failures": [],
            "row_counts": {"schedule": 0, "lineup": 0, "matchsheet": 0},
            "row_hashes": {
                "schedule": "1" * 64,
                "lineup": "2" * 64,
                "matchsheet": "3" * 64,
            },
            "ledger_count": 1,
            "ledger_hash": "4" * 64,
        }
        dq_ref = _write(
            root / "published-dq.json",
            {
                "kind": "espn-published-dq-result-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "scope_id": scope.scope_id,
                "run_id": run_id,
                "attempt": index,
                "plan_signature": plan_signature,
                "registry_signature": registry_signature,
                "publication_ref": publication_ref,
                "current_selection": selected_head,
                "quality": quality,
            },
        )
        attestation_ref = _write(
            root / "qualification-attestation.json",
            {
                "kind": "espn-scope-qualification-attestation-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "run_id": run_id,
                "attempt": index,
                "scope_id": scope.scope_id,
                "state": state,
                "registry_signature": registry_signature,
                "plan_signature": plan_signature,
                "lease_epoch": lease_epoch,
                "recorded_at": evidence_payload["recorded_at"],
                "scope_binding_ref": binding_ref,
                "run_evidence_ref": evidence_ref,
                "publication_ref": publication_ref,
                "published_dq_ref": dq_ref,
                "snapshot_ref": snapshot_ref,
                "selected_head": selected_head,
                "physical_generation": {
                    "run_id": physical.run_id,
                    "plan_signature": physical.plan_signature,
                    "generation_id": physical.generation_id,
                    "generation_signature": physical.generation_signature,
                    "manifest_sha256": physical.manifest_sha256,
                    "registry_signature": physical.registry_signature,
                    "parser_version": physical.parser_version,
                    "runtime_version": physical.runtime_version,
                },
            },
        )
        durable_ref = _write(
            root / "durable.json",
            {
                "kind": "espn-durable-run-manifest-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "run_id": run_id,
                "attempt": index,
                "registry_signature": registry_signature,
                "scope_ids": [scope.scope_id],
                "evidence": [
                    {
                        "dag_id": "dag_ingest_espn",
                        "run_id": run_id,
                        "attempt": index,
                        "scope_id": scope.scope_id,
                        "plan_signature": plan_signature,
                        "registry_signature": registry_signature,
                        "state": state,
                        "evidence_uri": evidence_ref["uri"],
                        "evidence_sha256": evidence_ref["sha256"],
                        "recorded_at": evidence_payload["recorded_at"],
                    }
                ],
                "publication_refs": [{"publication_ref": publication_ref}],
            },
        )
        raw_ref = _write(
            root / "raw.json",
            _raw_manifest(
                run_id=run_id,
                attempt=index,
                scope_id=scope.scope_id,
                registry=registry_signature,
                plan=plan_signature,
                as_of=as_of,
            ),
        )
        verdict_ref = _write(
            root / "terminal-verdict.json",
            {
                "kind": "espn-terminal-verdict-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "run_id": run_id,
                "attempt": index,
                "status": "complete",
                "failures": [],
                "scope_count": 1,
                "producer_states": {"published_dq": ["success"]},
                "expected_counts": {"published_dq": 1},
                "scope_metrics": {scope.scope_id: {"proxy_bytes": 0}},
            },
        )
        health_ref = _write(
            root / "health.json",
            {
                "kind": "espn-health-result-v1",
                "schema_version": 1,
                "run_id": run_id,
                "attempt": index,
                "status": "complete",
                "verdict_ref": verdict_ref,
                "alerts": [],
                "scope_metrics": {scope.scope_id: {"proxy_bytes": 0}},
            },
        )
        release_ref = _write(
            root / "lease-release.json",
            {
                "kind": "espn-lease-release-result-v1",
                "schema_version": 1,
                "dag_id": "dag_ingest_espn",
                "run_id": run_id,
                "attempt": index,
                "scope_ids": [scope.scope_id],
                "released": [scope.scope_id],
                "failures": [],
            },
        )
        parent = {
            "schema": "espn-daily-parent-v2",
            "owner_profile": "espn-isolated-v1",
            "parent_dag_id": "dag_trigger_espn_daily",
            "parent_task_id": "trigger_espn_ingest",
            "parent_run_id": parent_run_id,
            "parent_run_type": "scheduled",
            "logical_date": logical_date.isoformat(),
            "data_interval_start": logical_date.isoformat(),
            "data_interval_end": (logical_date + timedelta(days=1)).isoformat(),
            "child_dag_id": "dag_ingest_espn",
            "child_run_id": run_id,
        }
        success_ref = _write(
            root / "success.json",
            _receipt(
                {
                    "kind": "espn-run-success-receipt-v1",
                    "schema_version": 1,
                    "dag_id": "dag_ingest_espn",
                    "run_id": run_id,
                    "attempt": index,
                    "mode": "daily",
                    "as_of": as_of.isoformat(),
                    "logical_date": logical_date.isoformat(),
                    "parent": parent,
                    "scope_ids": [scope.scope_id],
                    "registry_ref": registry_ref,
                    "registry_signature": registry_signature,
                    "admission_ref": {
                        "uri": f"file:///admission-{index}.json",
                        "sha256": "5" * 64,
                    },
                    "plan_index_ref": {
                        "uri": f"file:///index-{index}.json",
                        "sha256": "6" * 64,
                    },
                    "durable_manifest_ref": durable_ref,
                    "published_dq_refs": [
                        {"scope_id": scope.scope_id, "published_dq_ref": dq_ref}
                    ],
                    "verdict_ref": verdict_ref,
                    "health_ref": health_ref,
                    "lease_release_ref": release_ref,
                }
            ),
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
                "qualification_attestation_ref": attestation_ref,
            }
        )
    document = {
        "schema_version": V4_PROMOTION_EVIDENCE_VERSION,
        "scope_id": scope.scope_id,
        "trust_label": "trusted",
        "cutover_id": f"espn-native-{scope.espn_id}-{scope.source_season_year}",
        "effective_at": "2026-08-01T09:00:00+00:00",
        "registry_snapshot_ref": registry_ref,
        "green_runs": green_runs,
        "fallback": {
            "kind": "legacy",
            "league": "ENG-Premier League",
            "season": "2627",
        },
    }
    path = tmp_path / "promotion-v4.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "states", [("complete", "noop", "noop"), ("noop", "noop", "noop")]
)
def test_v4_noop_sequences_qualify_against_one_physical_complete(tmp_path, states):
    evidence = load_promotion_evidence(_v4_evidence(tmp_path, states))

    assert [run.state for run in evidence.green_runs] == list(states)
    assert len({run.physical_run_id for run in evidence.green_runs}) == 1
    assert all(
        run.physical_runtime_version == "espn-native-runtime-v3"
        for run in evidence.green_runs
    )


def test_same_noop_chain_is_rejected_under_v3(tmp_path):
    path = _v4_evidence(tmp_path, ("noop", "noop", "noop"))
    document = json.loads(path.read_text())
    document["schema_version"] = PROMOTION_EVIDENCE_VERSION
    for run in document["green_runs"]:
        run.pop("qualification_attestation_ref")
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(MigrationError):
        load_promotion_evidence(path)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("run_id",), "tampered-run"),
        (("attempt",), 99),
        (("scope_id",), "740:2026"),
        (("registry_signature",), "1" * 64),
        (("plan_signature",), "2" * 64),
        (("state",), "failed"),
        (("selected_head", "manifest_sha256"), "3" * 64),
        (("physical_generation", "run_id"), "tampered-physical-run"),
        (("physical_generation", "plan_signature"), "4" * 64),
        (("physical_generation", "parser_version"), "espn-native-parser-v1"),
        (("physical_generation", "runtime_version"), "espn-native-runtime-v2"),
    ],
)
def test_v4_attestation_tampering_is_rejected(tmp_path, path, value):
    evidence_path = _v4_evidence(tmp_path, ("noop", "noop", "noop"))
    document = json.loads(evidence_path.read_text())
    ref = document["green_runs"][0]["qualification_attestation_ref"]
    artifact = Path(ref["uri"].removeprefix("file://"))
    payload = json.loads(artifact.read_text())
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    document["green_runs"][0]["qualification_attestation_ref"] = _write(
        artifact.with_name("tampered-attestation.json"), payload
    )
    evidence_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(MigrationError):
        load_promotion_evidence(evidence_path)


@pytest.mark.parametrize(
    ("artifact_key", "path", "value"),
    [
        ("scope_binding_ref", ("kind",), "tampered-binding"),
        ("published_dq_ref", ("current_selection", "run_id"), "tampered-head"),
        ("published_dq_ref", ("quality", "passed"), False),
        ("snapshot_ref", ("runtime_version",), "espn-native-runtime-v2"),
    ],
)
def test_v4_rejects_tampered_linked_artifact(tmp_path, artifact_key, path, value):
    evidence_path = _v4_evidence(tmp_path, ("noop", "noop", "noop"))
    document = json.loads(evidence_path.read_text())
    attestation_ref = document["green_runs"][0]["qualification_attestation_ref"]
    attestation = json.loads(
        Path(attestation_ref["uri"].removeprefix("file://")).read_text()
    )
    ref = attestation[artifact_key]
    artifact = Path(ref["uri"].removeprefix("file://"))
    payload = json.loads(artifact.read_text())
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    artifact.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(MigrationError):
        load_promotion_evidence(evidence_path)


def test_v4_rejects_tampered_complete_publication_intent(tmp_path):
    evidence_path = _v4_evidence(tmp_path, ("complete", "noop", "noop"))
    document = json.loads(evidence_path.read_text())
    attestation_ref = document["green_runs"][0]["qualification_attestation_ref"]
    attestation = json.loads(
        Path(attestation_ref["uri"].removeprefix("file://")).read_text()
    )
    publication_ref = attestation["publication_ref"]
    publication = json.loads(
        Path(publication_ref["uri"].removeprefix("file://")).read_text()
    )
    intent_ref = publication["publication_intent_ref"]
    intent_path = Path(intent_ref["uri"].removeprefix("file://"))
    intent = json.loads(intent_path.read_text())
    intent["plan_signature"] = "0" * 64
    intent_path.write_text(json.dumps(intent), encoding="utf-8")

    with pytest.raises(MigrationError):
        load_promotion_evidence(evidence_path)


class _V4ApplyBackend:
    def __init__(self, candidate, *, stale_latest: bool = False):
        self.candidate = candidate
        self.stale_latest = stale_latest
        self.actions: list[str] = []

    def ensure_objects(self):
        self.actions.append("ensure_objects")

    def legacy_baseline(self, league, season):
        self.actions.append("capture_baseline")
        return (
            {
                entity: {
                    "row_count": 0,
                    "distinct_key_count": 0,
                    "max_ingested_at": None,
                }
                for entity in ("schedule", "lineup", "matchsheet")
            },
            {
                "espn_schedule": 1,
                "espn_lineup": 2,
                "espn_matchsheet": 3,
            },
        )

    def verify_candidate(self, candidate, registry_snapshot_ref):
        self.actions.append("verify_candidate")

    def _manifest(self):
        physical = self.candidate.physical_generation
        return {
            "status": "complete",
            "scope_id": self.candidate.scope_id,
            "run_id": physical.run_id,
            "generation_id": physical.generation_id,
            "generation_signature": physical.generation_signature,
            "manifest_sha256": physical.manifest_sha256,
            "registry_snapshot_uri": physical.registry_snapshot_ref.uri,
            "registry_signature": physical.registry_signature,
            "plan_signature": physical.plan_signature,
            "parser_version": physical.parser_version,
            "runtime_version": physical.runtime_version,
            "completed_at": physical.completed_at,
        }

    def complete_manifest(self, scope_id, generation_id):
        self.actions.append("complete_manifest")
        return self._manifest()

    def latest_complete_manifest(self, scope_id):
        self.actions.append("latest_complete_manifest")
        return self._manifest()

    def latest_complete_manifest_v4(self, scope_id):
        self.actions.append("latest_complete_manifest_v4")
        manifest = self._manifest()
        if self.stale_latest:
            manifest["generation_id"] = "newer-generation"
        return manifest

    def baseline(self, scope_id):
        return None

    def append_baseline(self, row):
        self.actions.append("append_baseline")

    def latest_cutover(self, scope_id):
        return None

    def append_cutover(self, cutover):
        self.actions.append("append_cutover")


class _V4LeaseStore:
    def __init__(self, head):
        self.head = head

    def migrate(self):
        pass

    def acquire(self, scope_id, owner_id, plan_signature, *, now):
        return object()

    @contextmanager
    def guard(self, lease, *, now):
        yield

    def release(self, lease, *, now):
        pass

    def current_scope_head(self, scope_id):
        return self.head


@pytest.mark.parametrize("race", ["head", "latest"])
def test_v4_apply_rejects_control_or_manifest_race_before_append(tmp_path, race):
    evidence = load_promotion_evidence(_v4_evidence(tmp_path, ("noop", "noop", "noop")))
    candidate = evidence.green_runs[-1]
    head = dict(candidate.selected_head)
    if race == "head":
        head["manifest_sha256"] = "0" * 64
    backend = _V4ApplyBackend(candidate, stale_latest=race == "latest")

    with pytest.raises(MigrationError):
        apply_promotion(
            evidence,
            backend=backend,
            lease_store=_V4LeaseStore(head),
            now=datetime(2026, 8, 2, tzinfo=UTC),
        )

    assert "append_baseline" not in backend.actions
    assert "append_cutover" not in backend.actions
