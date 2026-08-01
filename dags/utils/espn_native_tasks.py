"""Executable Airflow task callables for ESPN Native Bronze v2.

Every cross-task value is a bounded URI/SHA descriptor.  Raw bodies and row
sets stay in the immutable raw/artifact stores; PostgreSQL owns leases, the
shared source rate gate, current heads and exact current-run evidence.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Sequence

from scrapers.espn.models import IngestPlan, ScopePlan
from scrapers.espn.operations import (
    LeaseConflict,
    OperationsError,
    PostgresEspnControlStore,
    RunManifestEvidence,
    MAX_SCOPE_SUMMARY_EVENTS,
    ScopeHead,
    ScopeLease,
    evaluate_alerts,
    plan_summary_batches as make_summary_batches,
    producer_state_failures,
    reduce_raw_checkpoints,
    seal_raw_batch_descriptor,
    seal_raw_checkpoint,
    validate_raw_checkpoint,
)
from scrapers.espn.raw_store import EspnRawStore
from scrapers.espn.registry import Registry, load_registry
from scrapers.espn.repository import (
    EspnBronzeRepository,
    ScopePublicationState,
    validate_scope_generation,
)
from scrapers.espn import runner
from scrapers.espn.transport import EspnHttpClient
from scrapers.espn.transport_contracts import (
    DEFAULT_MAX_REQUESTS,
    DEFAULT_MAX_TASK_BYTES,
    EndpointType,
    TaskBudget,
)


UTC = timezone.utc
HTTP_POOL = "espn_http_pool"
REGISTRY_ENV = "ESPN_REGISTRY_PATH"
ARTIFACT_ROOT_ENV = "ESPN_ARTIFACT_ROOT_URI"
RAW_STORE_ENV = "ESPN_RAW_STORE_URI"
LEASE_TTL = timedelta(hours=6)
_SHA_RE = re.compile(r"[0-9a-f]{64}")
_MANUAL_MODES = frozenset({"repair", "backfill", "replay"})
_PRODUCER_SUCCESS = frozenset({"success"})


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode()


def _required(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OperationsError(f"{field} must be a non-empty string")
    return value


def _sha(value: object, field: str) -> str:
    result = _required(value, field)
    if _SHA_RE.fullmatch(result) is None:
        raise OperationsError(f"{field} must be a lowercase SHA-256")
    return result


def _artifact_root() -> str:
    return _required(os.environ.get(ARTIFACT_ROOT_ENV), ARTIFACT_ROOT_ENV).rstrip("/")


def _raw_store_uri() -> str:
    return _required(os.environ.get(RAW_STORE_ENV), RAW_STORE_ENV).rstrip("/")


def _run_key(dag_id: str, run_id: str) -> str:
    return hashlib.sha256(
        f"espn-airflow-v1\x00{dag_id}\x00{run_id}".encode()
    ).hexdigest()


def _join_uri(root: str, *parts: str) -> str:
    clean = [part.strip("/") for part in parts]
    if any(not part or "/" in part or part in {".", ".."} for part in clean):
        raise OperationsError("artifact path segment is unsafe")
    return root.rstrip("/") + "/" + "/".join(clean)


def _write_payload(uri: str, payload: Mapping[str, Any], *, immutable: bool = True):
    body = _canonical_bytes(payload)
    runner._write_artifact(uri, body, immutable=immutable)
    return {"uri": uri, "sha256": hashlib.sha256(body).hexdigest()}


def _read_ref(ref: object, *, kind: str | None = None) -> dict[str, Any]:
    if not isinstance(ref, Mapping) or set(ref) != {"uri", "sha256"}:
        raise OperationsError("artifact reference must contain only uri and sha256")
    uri = _required(ref["uri"], "artifact uri")
    expected = _sha(ref["sha256"], "artifact sha256")
    body = runner._read_artifact(uri)
    if hashlib.sha256(body).hexdigest() != expected:
        raise OperationsError("artifact reference hash mismatch")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("artifact reference is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise OperationsError("artifact payload must be an object")
    if kind is not None and payload.get("kind") != kind:
        raise OperationsError(f"artifact kind must be {kind}")
    return payload


def _ref_for_uri(uri: str) -> dict[str, str]:
    body = runner._read_artifact(uri)
    return {"uri": uri, "sha256": hashlib.sha256(body).hexdigest()}


def _logical_date(context: Mapping[str, Any]) -> datetime:
    value = context.get("logical_date")
    if value is None:
        dag_run = context.get("dag_run")
        value = getattr(dag_run, "logical_date", None)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise OperationsError("Airflow logical_date must be timezone-aware")
    return value.astimezone(UTC)


def _attempt(context: Mapping[str, Any]) -> int:
    params = context.get("params") or {}
    value = params.get("attempt", 1)
    if type(value) is not int or value <= 0:
        raise OperationsError("ESPN attempt must be a positive integer")
    return value


def _run_identity(context: Mapping[str, Any]) -> tuple[str, str, datetime]:
    dag = context.get("dag")
    dag_id = _required(getattr(dag, "dag_id", None), "dag_id")
    run_id = _required(context.get("run_id"), "run_id")
    return dag_id, run_id, _logical_date(context)


def _scope_plan(registry: Registry, scope_id: str) -> ScopePlan:
    for competition in registry.competitions:
        for edition in competition.editions:
            if competition.scope_id(edition) == scope_id:
                return ScopePlan(
                    scope_id=scope_id,
                    espn_id=competition.espn_id,
                    slug=competition.slug,
                    source_season_year=edition.source_season_year,
                    start_date=edition.start_date,
                    end_date=edition.end_date,
                    capabilities=edition.capabilities,
                )
    raise OperationsError(f"scope {scope_id} is absent from promoted registry")


def _selected_scopes(registry: Registry, mode: str, params: Mapping[str, Any]):
    current = tuple(
        sorted(
            competition.scope_id(competition.current_edition)
            for competition in registry.competitions
            if competition.enabled
        )
    )
    all_promoted = tuple(
        sorted(
            competition.scope_id(edition)
            for competition in registry.competitions
            if competition.enabled
            for edition in competition.editions
        )
    )
    requested = tuple(params.get("scopes") or ())
    if requested:
        requested = tuple(sorted(str(item) for item in requested))
        if len(requested) != len(set(requested)):
            raise OperationsError("duplicate ESPN scope selector")
        admitted = all_promoted if mode in _MANUAL_MODES else current
        unknown = sorted(set(requested) - set(admitted))
        if unknown:
            raise OperationsError(f"unpromoted ESPN scopes requested: {unknown}")
        return requested
    if mode in _MANUAL_MODES:
        raise OperationsError(f"manual ESPN {mode} requires explicit scopes")
    return current


def _iso_utc(value: object, field: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise OperationsError(f"{field} must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _exact_parent_run(parent_run_id: str):
    """Load exactly one master DagRun from Airflow metadata."""

    from airflow.models import DagRun

    matches = DagRun.find(
        dag_id="dag_master_pipeline",
        run_id=_required(parent_run_id, "parent_run_id"),
    )
    if len(matches) != 1:
        raise OperationsError("daily ESPN exact master parent was not found")
    return matches[0]


def _daily_parent(context: Mapping[str, Any]) -> dict[str, str]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    parent = conf.get("espn_parent")
    expected_keys = {
        "schema",
        "parent_dag_id",
        "parent_run_id",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
        "child_run_id",
    }
    if not isinstance(parent, Mapping) or set(parent) != expected_keys:
        raise OperationsError("daily ESPN admission requires exact master parent")
    if parent["schema"] != "espn-master-parent-v1":
        raise OperationsError("daily ESPN parent schema mismatch")
    if parent["parent_dag_id"] != "dag_master_pipeline":
        raise OperationsError("daily ESPN parent DAG mismatch")
    parent_run_id = _required(parent["parent_run_id"], "parent_run_id")
    expected_child_run_id = f"espn_daily__dag_master_pipeline__{parent_run_id}"
    if parent["child_run_id"] != expected_child_run_id:
        raise OperationsError("daily ESPN deterministic child identity mismatch")
    if parent["child_run_id"] != context.get("run_id"):
        raise OperationsError("daily ESPN child run identity mismatch")
    if parent["logical_date"] != _logical_date(context).isoformat():
        raise OperationsError("daily ESPN logical-date binding mismatch")
    master_run = _exact_parent_run(parent_run_id)
    if getattr(master_run, "dag_id", None) != parent["parent_dag_id"]:
        raise OperationsError("daily ESPN metadata parent DAG mismatch")
    if getattr(master_run, "run_id", None) != parent_run_id:
        raise OperationsError("daily ESPN metadata parent run mismatch")
    exact_dates = {
        "logical_date": _iso_utc(
            getattr(master_run, "logical_date", None), "parent logical_date"
        ),
        "data_interval_start": _iso_utc(
            getattr(master_run, "data_interval_start", None),
            "parent data_interval_start",
        ),
        "data_interval_end": _iso_utc(
            getattr(master_run, "data_interval_end", None),
            "parent data_interval_end",
        ),
    }
    for field, actual in exact_dates.items():
        if parent[field] != actual:
            raise OperationsError(f"daily ESPN parent {field} binding mismatch")
    trigger_instance = master_run.get_task_instance(
        task_id="ingestion_triggers.trigger_espn"
    )
    trigger_state = getattr(trigger_instance, "state", None)
    trigger_state = getattr(trigger_state, "value", trigger_state)
    if str(trigger_state or "").lower() not in {
        "running",
        "deferred",
        "up_for_reschedule",
        "success",
    }:
        raise OperationsError("daily ESPN master trigger task is not active")
    if getattr(trigger_instance, "run_id", parent_run_id) != parent_run_id:
        raise OperationsError("daily ESPN master trigger task run mismatch")
    return {key: str(parent[key]) for key in sorted(parent)}


def validate_registry_and_admission(*, mode: str, **context) -> dict[str, str]:
    """Fail-closed admission and immutable registry snapshot creation."""

    if mode not in {"daily", *_MANUAL_MODES}:
        raise OperationsError("unsupported ESPN orchestration mode")
    dag_id, run_id, logical_date = _run_identity(context)
    parent = _daily_parent(context) if mode == "daily" else None
    if mode != "daily":
        dag_run = context.get("dag_run")
        if (getattr(dag_run, "conf", None) or {}).get("espn_parent") is not None:
            raise OperationsError("manual ESPN mode forbids master daily parent")
    registry_path = Path(
        os.environ.get(REGISTRY_ENV, "/opt/airflow/configs/espn/registry.yaml")
    )
    registry = load_registry(registry_path)
    scopes = _selected_scopes(registry, mode, context.get("params") or {})
    if not scopes:
        raise OperationsError("ESPN registry has no admitted scopes")
    root = _join_uri(_artifact_root(), "runs", _run_key(dag_id, run_id))
    registry_uri = _join_uri(root, "registry.json")
    registry_ref = _write_payload(
        registry_uri,
        json.loads(registry.canonical_json()),
        immutable=True,
    )
    payload = {
        "kind": "espn-airflow-admission-v1",
        "schema_version": 1,
        "dag_id": dag_id,
        "run_id": run_id,
        "attempt": _attempt(context),
        "mode": mode,
        "as_of": logical_date.date().isoformat(),
        "logical_date": logical_date.isoformat(),
        "parent": parent,
        "registry_ref": registry_ref,
        "registry_signature": registry.signature(),
        "scope_ids": list(scopes),
        "artifact_root": root,
        "raw_store_uri": _raw_store_uri(),
        "replay_sources": (
            (context.get("params") or {}).get("replay_sources", {})
            if mode == "replay"
            else {}
        ),
    }
    return _write_payload(_join_uri(root, "admission.json"), payload)


def _load_registry_ref(admission: Mapping[str, Any]) -> Registry:
    payload = _read_ref(admission["registry_ref"])
    from scrapers.espn.registry import validate_registry_document

    registry = validate_registry_document(payload)
    if registry.signature() != admission["registry_signature"]:
        raise OperationsError("admitted registry signature mismatch")
    return registry


def _scope_binding(
    *,
    head: ScopeHead | None,
    scope: ScopePlan,
    run_id: str,
    attempt: int,
    mode: str,
    root: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    identity = hashlib.sha256(
        f"espn-generation-v1\x00{run_id}\x00{attempt}\x00{scope.scope_id}\x00{mode}".encode()
    ).hexdigest()
    prior = None
    known = []
    if head is not None:
        generation = runner.load_scope_snapshot(
            head.snapshot_uri,
            artifact_sha256=head.snapshot_sha256,
            expected_scope_id=scope.scope_id,
        )
        if generation.plan != scope:
            raise OperationsError("prior scope plan differs from promoted registry")
        known = [
            {"event_id": row.event_id, "event_date": row.kickoff.date().isoformat()}
            for row in generation.schedule
            if not row.terminal
        ]
        prior = {
            "uri": head.snapshot_uri,
            "artifact_sha256": head.snapshot_sha256,
            "scope_id": head.scope_id,
            "generation_id": head.generation_id,
            "generation_signature": head.generation_signature,
            "manifest_sha256": head.manifest_sha256,
        }
    scope_key = scope.scope_id.replace(":", "-")
    return {
        "active": True,
        "initial_capture": head is None,
        "generation_id": identity,
        "batch_id": hashlib.sha256(f"espn-batch-v1\x00{identity}".encode()).hexdigest(),
        "ingested_at": ingested_at.isoformat().replace("+00:00", "Z"),
        "generation_snapshot_uri": _join_uri(
            root, "scopes", scope_key, "generation.json"
        ),
        "known_nonterminal_events": sorted(known, key=lambda item: item["event_id"]),
        "prior": prior,
    }


def _replay_binding(admission: Mapping[str, Any], scope_id: str):
    source = admission.get("replay_sources", {}).get(scope_id)
    if not isinstance(source, Mapping) or set(source) != {"uri", "sha256"}:
        raise OperationsError(
            f"replay scope {scope_id} requires exact raw manifest uri/sha256"
        )
    uri = _required(source["uri"], "replay raw manifest uri")
    expected = _sha(source["sha256"], "replay raw manifest sha256")
    body = runner._read_artifact(uri)
    if hashlib.sha256(body).hexdigest() != expected:
        raise OperationsError("replay raw manifest artifact hash mismatch")
    try:
        manifest = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("replay raw manifest is not valid JSON") from exc
    validated = runner._validate_raw_manifest(manifest)
    if scope_id not in validated["selected_scopes"]:
        raise OperationsError("replay raw manifest does not contain scope")
    source_plan_uri = uri.rsplit("/", 1)[0] + "/plan.json"
    source_plan = runner._load_signed_plan(source_plan_uri)
    source_scopes = {item.scope_id: item for item in source_plan.plan.scopes}
    source_scope = source_scopes.get(scope_id)
    if source_scope is None:
        raise OperationsError("replay source plan does not contain scope")
    expected_source = (
        source_plan.plan.run_id,
        source_plan.attempt,
        source_plan.mode,
        source_plan.plan.as_of.isoformat(),
        source_plan.plan.registry_signature,
        source_plan.signature,
        source_plan.raw_manifest_uri,
    )
    actual_source = (
        validated["run_id"],
        validated["attempt"],
        validated["mode"],
        validated["as_of"],
        validated["registry_signature"],
        validated["plan_signature"],
        uri,
    )
    if source_plan.mode == "replay" or expected_source != actual_source:
        raise OperationsError("replay source plan/raw identity mismatch")
    return (
        uri,
        {
            "mode": validated["mode"],
            "run_id": validated["run_id"],
            "attempt": validated["attempt"],
            "plan_signature": validated["plan_signature"],
            "raw_manifest_sha256": expected,
        },
        source_plan,
        source_scope,
    )


def _replay_scope_binding(
    *,
    source_binding,
    scope: ScopePlan,
    run_id: str,
    attempt: int,
    root: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    identity = hashlib.sha256(
        f"espn-generation-v1\x00{run_id}\x00{attempt}\x00{scope.scope_id}\x00replay".encode()
    ).hexdigest()
    prior = source_binding.prior
    return {
        "active": source_binding.active,
        "initial_capture": source_binding.initial_capture,
        "generation_id": identity,
        "batch_id": hashlib.sha256(f"espn-batch-v1\x00{identity}".encode()).hexdigest(),
        "ingested_at": ingested_at.isoformat().replace("+00:00", "Z"),
        "generation_snapshot_uri": _join_uri(
            root,
            "scopes",
            scope.scope_id.replace(":", "-"),
            "generation.json",
        ),
        "known_nonterminal_events": [
            {"event_id": item.event_id, "event_date": item.event_date.isoformat()}
            for item in source_binding.known_nonterminal_events
        ],
        "prior": (
            None
            if prior is None
            else {
                "uri": prior.uri,
                "artifact_sha256": prior.artifact_sha256,
                "scope_id": prior.scope_id,
                "generation_id": prior.generation_id,
                "generation_signature": prior.generation_signature,
                "manifest_sha256": prior.manifest_sha256,
            }
        ),
    }


def acquire_scope_leases(
    *, admission_ref: Mapping[str, str], **context
) -> dict[str, Any]:
    """Atomically own the admitted scope bundle before reading any head."""

    admission = _read_ref(admission_ref, kind="espn-airflow-admission-v1")
    dag_id, run_id, _ = _run_identity(context)
    if run_id != admission["run_id"]:
        raise OperationsError("lease owner run differs from admission")
    admission_signature = _sha(admission_ref["sha256"], "admission sha256")
    store = PostgresEspnControlStore.from_env()
    store.migrate()
    owner_id = f"{dag_id}/{run_id}/{admission['attempt']}"
    checked_at = datetime.now(UTC)
    try:
        leases = store.read_owner_leases(owner_id)
        if leases:
            if {item.scope_id for item in leases} != set(admission["scope_ids"]):
                raise OperationsError("existing owner lease scope set is partial")
            for lease in leases:
                store.assert_owned(lease, now=checked_at)
        else:
            leases = store.acquire_many(
                admission["scope_ids"],
                owner_id=owner_id,
                plan_signature=admission_signature,
                now=checked_at,
                ttl=LEASE_TTL,
            )
    except LeaseConflict as exc:
        conflict = {
            "kind": "espn-lease-conflict-v1",
            "schema_version": 1,
            "run_id": run_id,
            "attempt": admission["attempt"],
            "owner_id": owner_id,
            "scope_ids": admission["scope_ids"],
            "observed_at": admission["logical_date"],
            "detail": str(exc),
        }
        _write_payload(
            _join_uri(admission["artifact_root"], "lease-conflict.json"), conflict
        )
        raise
    acquisition = {
        "kind": "espn-lease-acquisition-v1",
        "schema_version": 1,
        "admission_ref": admission_ref,
        "owner_id": owner_id,
        "admission_signature": admission_signature,
        "scope_ids": sorted(item.scope_id for item in leases),
    }
    acquisition_ref = _write_payload(
        _join_uri(admission["artifact_root"], "lease-acquisition.json"),
        acquisition,
    )
    return {"lease_acquisition_ref": acquisition_ref}


def build_signed_scope_plans(
    *, leasing: Mapping[str, Any], **_context
) -> dict[str, Any]:
    """Read heads under exact leases, then seal immutable scope plans."""

    acquisition_ref = leasing["lease_acquisition_ref"]
    acquisition = _read_ref(acquisition_ref, kind="espn-lease-acquisition-v1")
    admission = _read_ref(
        acquisition["admission_ref"], kind="espn-airflow-admission-v1"
    )
    registry = _load_registry_ref(admission)
    store = PostgresEspnControlStore.from_env()
    expected_owner = (
        f"{admission['dag_id']}/{admission['run_id']}/{admission['attempt']}"
    )
    if acquisition["owner_id"] != expected_owner:
        raise OperationsError("lease acquisition owner identity mismatch")
    if acquisition["admission_signature"] != acquisition["admission_ref"]["sha256"]:
        raise OperationsError("lease acquisition admission identity mismatch")
    leases = store.read_owner_leases(expected_owner)
    if tuple(item.scope_id for item in leases) != tuple(admission["scope_ids"]):
        raise OperationsError("lease bundle scope set differs from admission")
    heads = store.read_scope_heads_owned(leases, now=datetime.now(UTC))
    scope_refs = []
    descriptors_by_scope = {}
    plan_signatures: list[str] = []
    plan_registry_signatures: set[str] = set()
    ingested_at = datetime.fromisoformat(admission["logical_date"])
    for scope_id in admission["scope_ids"]:
        scope = _scope_plan(registry, scope_id)
        scope_key = scope.scope_id.replace(":", "-")
        scope_root = _join_uri(admission["artifact_root"], "scopes", scope_key)
        replay_source = None
        raw_manifest_uri = _join_uri(scope_root, "raw-manifest.json")
        registry_snapshot_uri = admission["registry_ref"]["uri"]
        registry_signature = admission["registry_signature"]
        plan_as_of = date.fromisoformat(admission["as_of"])
        raw_store_uri = admission["raw_store_uri"]
        if admission["mode"] == "replay":
            (
                raw_manifest_uri,
                replay_source,
                source_plan,
                scope,
            ) = _replay_binding(admission, scope_id)
            source_binding = source_plan.bindings[scope_id]
            binding = _replay_scope_binding(
                source_binding=source_binding,
                scope=scope,
                run_id=admission["run_id"],
                attempt=admission["attempt"],
                root=admission["artifact_root"],
                ingested_at=ingested_at,
            )
            registry_snapshot_uri = source_plan.registry_snapshot_uri
            registry_signature = source_plan.plan.registry_signature
            plan_as_of = source_plan.plan.as_of
            raw_store_uri = source_plan.raw_store_uri
        else:
            binding = _scope_binding(
                head=heads.get(scope_id),
                scope=scope,
                run_id=admission["run_id"],
                attempt=admission["attempt"],
                mode=admission["mode"],
                root=admission["artifact_root"],
                ingested_at=ingested_at,
            )
        output_uri = _join_uri(scope_root, "runner-result.json")
        runtime = {
            "mode": admission["mode"],
            "attempt": admission["attempt"],
            "registry_snapshot_uri": registry_snapshot_uri,
            "raw_manifest_uri": raw_manifest_uri,
            "output_uri": output_uri,
            "raw_store_uri": raw_store_uri,
            "max_events": 100,
            "selected_scopes": [scope_id],
            "scope_bindings": {scope_id: binding},
            "replay_source": replay_source,
        }
        plan = IngestPlan(
            schema_version=1,
            run_id=admission["run_id"],
            as_of=plan_as_of,
            registry_signature=registry_signature,
            scopes=(scope,),
            metadata={"runtime": runtime},
        )
        envelope = {
            "kind": runner.PLAN_KIND,
            "plan": plan.to_dict(),
            "signature": plan.signature(),
        }
        plan_ref = _write_payload(
            _join_uri(scope_root, "plan.json"), envelope, immutable=True
        )
        expected_scoreboard = None
        scoreboard_checkpoint_uri = None
        if admission["mode"] != "replay":
            loaded = runner._load_signed_plan(plan_ref["uri"])
            requests = runner._scoreboard_requests(
                scope,
                loaded.bindings[scope_id],
                as_of=plan.as_of,
                mode=runner._effective_mode(loaded),
            )
            if not requests:
                raise OperationsError("active scope produced no scoreboard plan")
            request_ids = tuple(item.request_id for item in requests)
            batch_id = hashlib.sha256(
                _canonical_bytes(
                    {
                        "kind": "espn-scoreboard-batch-id-v1",
                        "scope_id": scope_id,
                        "plan_signature": plan.signature(),
                        "request_ids": request_ids,
                    }
                )
            ).hexdigest()
            expected_scoreboard = seal_raw_batch_descriptor(
                endpoint="scoreboard",
                run_id=plan.run_id,
                attempt=admission["attempt"],
                scope_id=scope_id,
                plan_signature=plan.signature(),
                batch_id=batch_id,
                request_ids=request_ids,
                event_ids=(),
            )
            scoreboard_checkpoint_uri = _join_uri(
                scope_root, "raw", f"scoreboard-{batch_id}.json"
            )
        descriptor = {
            "kind": "espn-scope-plan-descriptor-v1",
            "schema_version": 1,
            "dag_id": admission["dag_id"],
            "run_id": plan.run_id,
            "attempt": admission["attempt"],
            "mode": admission["mode"],
            "scope_id": scope_id,
            "plan_ref": plan_ref,
            "plan_signature": plan.signature(),
            "raw_manifest_uri": raw_manifest_uri,
            "raw_store_uri": raw_store_uri,
            "generation_snapshot_uri": binding["generation_snapshot_uri"],
            "expected_scoreboard_batch": expected_scoreboard,
            "scoreboard_checkpoint_uri": scoreboard_checkpoint_uri,
            "scope_root": scope_root,
        }
        descriptor_ref = _write_payload(
            _join_uri(scope_root, "scope-plan-descriptor.json"), descriptor
        )
        scope_refs.append({"scope_plan_ref": descriptor_ref})
        descriptors_by_scope[scope_id] = (scope_root, descriptor_ref)
        plan_signatures.append(plan.signature())
        plan_registry_signatures.add(plan.registry_signature)
    if len(plan_registry_signatures) != 1:
        raise OperationsError("scope plan registry signatures must be identical")
    bundle_signature = hashlib.sha256(
        _canonical_bytes(sorted(plan_signatures))
    ).hexdigest()
    index = {
        "kind": "espn-plan-index-v1",
        "schema_version": 1,
        "dag_id": admission["dag_id"],
        "run_id": admission["run_id"],
        "attempt": admission["attempt"],
        "mode": admission["mode"],
        "registry_signature": next(iter(plan_registry_signatures)),
        "bundle_signature": bundle_signature,
        "scope_ids": admission["scope_ids"],
        "scope_plan_refs": [item["scope_plan_ref"] for item in scope_refs],
    }
    index_ref = _write_payload(
        _join_uri(admission["artifact_root"], "plan-index.json"), index
    )
    bound_leases = store.bind_plans(
        leases,
        {
            scope_id: _read_ref(descriptor_ref, kind="espn-scope-plan-descriptor-v1")[
                "plan_signature"
            ]
            for scope_id, (_, descriptor_ref) in descriptors_by_scope.items()
        },
        now=datetime.now(UTC),
    )
    binding_refs = []
    for lease in bound_leases:
        scope_root, descriptor_ref = descriptors_by_scope[lease.scope_id]
        lease_binding = {
            "kind": "espn-scope-lease-binding-v1",
            "schema_version": 1,
            "scope_plan_ref": descriptor_ref,
            "lease": _lease_to_dict(lease),
        }
        binding_ref = _write_payload(
            _join_uri(scope_root, "lease-binding.json"), lease_binding
        )
        binding_refs.append({"scope_binding_ref": binding_ref})
    return {
        "lease_acquisition_ref": acquisition_ref,
        "plan_index_ref": index_ref,
        "scope_binding_refs": binding_refs,
    }


def _lease_to_dict(lease: ScopeLease) -> dict[str, Any]:
    return {
        "scope_id": lease.scope_id,
        "owner_id": lease.owner_id,
        "plan_signature": lease.plan_signature,
        "epoch": lease.epoch,
        "token_sha256": lease.token_sha256,
        "acquired_at": lease.acquired_at.isoformat(),
        "expires_at": lease.expires_at.isoformat(),
    }


def _lease_from_dict(value: Mapping[str, Any]) -> ScopeLease:
    return ScopeLease(
        scope_id=value["scope_id"],
        owner_id=value["owner_id"],
        plan_signature=value["plan_signature"],
        epoch=value["epoch"],
        token_sha256=value["token_sha256"],
        acquired_at=datetime.fromisoformat(value["acquired_at"]),
        expires_at=datetime.fromisoformat(value["expires_at"]),
    )


def _binding(ref: Mapping[str, str]):
    binding = _read_ref(ref, kind="espn-scope-lease-binding-v1")
    descriptor = _read_ref(
        binding["scope_plan_ref"], kind="espn-scope-plan-descriptor-v1"
    )
    plan_envelope = _read_ref(descriptor["plan_ref"], kind=runner.PLAN_KIND)
    loaded = runner._load_signed_plan(descriptor["plan_ref"]["uri"])
    if (
        loaded.signature,
        plan_envelope["signature"],
    ) != (descriptor["plan_signature"], descriptor["plan_signature"]):
        raise OperationsError("scope descriptor plan signature mismatch")
    if len(loaded.plan.scopes) != 1 or loaded.selected_scopes != (
        descriptor["scope_id"],
    ):
        raise OperationsError("scope descriptor must bind one selected scope")
    scope = loaded.plan.scopes[0]
    if (
        scope.scope_id,
        loaded.plan.run_id,
        loaded.attempt,
        loaded.mode,
        loaded.raw_manifest_uri,
        loaded.raw_store_uri,
        loaded.bindings[scope.scope_id].generation_snapshot_uri,
    ) != (
        descriptor["scope_id"],
        descriptor["run_id"],
        descriptor["attempt"],
        descriptor["mode"],
        descriptor["raw_manifest_uri"],
        descriptor["raw_store_uri"],
        descriptor["generation_snapshot_uri"],
    ):
        raise OperationsError("scope descriptor identity mismatch")
    lease = _lease_from_dict(binding["lease"])
    if lease.scope_id != scope.scope_id:
        raise OperationsError("lease/scope identity mismatch")
    if lease.plan_signature != loaded.signature:
        raise OperationsError("lease is not fenced to the exact signed scope plan")
    return binding, descriptor, loaded, scope, lease


def _http_client(
    raw_store: EspnRawStore,
    *,
    max_summary_events: int,
) -> EspnHttpClient:
    control = PostgresEspnControlStore.from_env()
    budget = TaskBudget(
        max_competitions=1,
        max_summary_events=max(1, max_summary_events),
        max_requests=DEFAULT_MAX_REQUESTS,
        max_bytes=DEFAULT_MAX_TASK_BYTES,
    )
    return EspnHttpClient(
        raw_store,
        budget=budget,
        request_permit=control.acquire_request_permit,
    )


def _resume_checkpoint(uri: str, expected: Mapping[str, Any]) -> dict[str, str] | None:
    """Reuse only the exact immutable signed batch checkpoint, never an alias."""

    try:
        body = runner._read_artifact(uri)
    except FileNotFoundError:
        return None
    try:
        checkpoint = validate_raw_checkpoint(json.loads(body.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("existing raw checkpoint is invalid") from exc
    identity = (
        checkpoint["endpoint"],
        checkpoint["run_id"],
        checkpoint["attempt"],
        checkpoint["scope_id"],
        checkpoint["plan_signature"],
        checkpoint["batch_id"],
        sorted(item["request_id"] for item in checkpoint["requests"]),
    )
    expected_identity = (
        expected["endpoint"],
        expected["run_id"],
        expected["attempt"],
        expected["scope_id"],
        expected["plan_signature"],
        expected["batch_id"],
        expected["request_ids"],
    )
    if identity != expected_identity:
        raise OperationsError("existing raw checkpoint identity mismatch")
    return {"uri": uri, "sha256": hashlib.sha256(body).hexdigest()}


def _request_checkpoint_uri(batch_uri: str, request_id: str) -> str:
    """Return a URI-safe immutable leaf under one exact signed batch."""

    request_key = hashlib.sha256(
        _canonical_bytes(
            {"kind": "espn-request-checkpoint-key-v1", "request_id": request_id}
        )
    ).hexdigest()
    prefix = batch_uri.removesuffix(".json")
    return f"{prefix}-requests/{request_key}.json"


def _resume_request_checkpoint(
    *, batch_uri: str, expected: Mapping[str, Any], request_id: str
) -> dict[str, Any] | None:
    """Read one successful request only when every batch identity still matches."""

    uri = _request_checkpoint_uri(batch_uri, request_id)
    try:
        body = runner._read_artifact(uri)
    except FileNotFoundError:
        return None
    try:
        checkpoint = validate_raw_checkpoint(json.loads(body.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("existing raw request checkpoint is invalid") from exc
    identity = (
        checkpoint["endpoint"],
        checkpoint["run_id"],
        checkpoint["attempt"],
        checkpoint["scope_id"],
        checkpoint["plan_signature"],
        checkpoint["batch_id"],
        [item["request_id"] for item in checkpoint["requests"]],
    )
    expected_identity = (
        expected["endpoint"],
        expected["run_id"],
        expected["attempt"],
        expected["scope_id"],
        expected["plan_signature"],
        expected["batch_id"],
        [request_id],
    )
    if identity != expected_identity:
        raise OperationsError("existing raw request checkpoint identity mismatch")
    return checkpoint["requests"][0]


def _persist_request_checkpoint(
    *, batch_uri: str, expected: Mapping[str, Any], request: Mapping[str, Any]
) -> dict[str, Any]:
    request_id = _required(request.get("request_id"), "request_id")
    if request_id not in expected["request_ids"]:
        raise OperationsError("raw request is absent from signed batch")
    checkpoint = seal_raw_checkpoint(
        endpoint=expected["endpoint"],
        run_id=expected["run_id"],
        attempt=expected["attempt"],
        scope_id=expected["scope_id"],
        plan_signature=expected["plan_signature"],
        batch_id=expected["batch_id"],
        requests=(request,),
    )
    _write_payload(_request_checkpoint_uri(batch_uri, request_id), checkpoint)
    return dict(request)


def _seal_completed_batch(
    *,
    batch_uri: str,
    expected: Mapping[str, Any],
    requests: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    if sorted(item["request_id"] for item in requests) != expected["request_ids"]:
        raise OperationsError("raw request checkpoints do not match signed batch")
    checkpoint = seal_raw_checkpoint(
        endpoint=expected["endpoint"],
        run_id=expected["run_id"],
        attempt=expected["attempt"],
        scope_id=expected["scope_id"],
        plan_signature=expected["plan_signature"],
        batch_id=expected["batch_id"],
        requests=requests,
    )
    return _write_payload(batch_uri, checkpoint)


def fetch_scoreboard_batch(
    *, scope_binding_ref: Mapping[str, str], **_context
) -> dict[str, str]:
    """Mapped network wave one: immutable scoreboard raw only."""

    binding, descriptor, loaded, scope, _ = _binding(scope_binding_ref)
    if loaded.mode == "replay":
        raise OperationsError("replay must not instantiate a network operator")
    raw_store = EspnRawStore.from_uri(descriptor["raw_store_uri"])
    client = _http_client(raw_store, max_summary_events=1)
    requests = runner._scoreboard_requests(
        scope,
        loaded.bindings[scope.scope_id],
        as_of=loaded.plan.as_of,
        mode=runner._effective_mode(loaded),
    )
    expected = descriptor["expected_scoreboard_batch"]
    if [item.request_id for item in requests] != expected["request_ids"]:
        raise OperationsError("scoreboard request plan drift")
    checkpoint_ref = _resume_checkpoint(
        descriptor["scoreboard_checkpoint_uri"], expected
    )
    if checkpoint_ref is None:
        records = []
        for request in requests:
            resumed = _resume_request_checkpoint(
                batch_uri=descriptor["scoreboard_checkpoint_uri"],
                expected=expected,
                request_id=request.request_id,
            )
            if resumed is not None:
                records.append(resumed)
                continue
            fetched = client.fetch_json(
                request.url,
                EndpointType.SCOREBOARD,
                request.params,
                competition_id=scope.espn_id,
                force_refresh=True,
            )
            record = runner._raw_request_from_fetch(
                request_id=request.request_id,
                scope_id=scope.scope_id,
                endpoint="scoreboard",
                event_id=None,
                result=fetched,
                query_start=request.query_start,
                query_end=request.query_end,
            )
            records.append(
                _persist_request_checkpoint(
                    batch_uri=descriptor["scoreboard_checkpoint_uri"],
                    expected=expected,
                    request=record,
                )
            )
        checkpoint_ref = _seal_completed_batch(
            batch_uri=descriptor["scoreboard_checkpoint_uri"],
            expected=expected,
            requests=records,
        )
    phase = {
        "kind": "espn-scoreboard-phase-result-v1",
        "schema_version": 1,
        "scope_binding_ref": scope_binding_ref,
        "expected_batch": expected,
        "checkpoint_ref": checkpoint_ref,
    }
    phase_ref = _write_payload(
        _join_uri(descriptor["scope_root"], "scoreboard-phase.json"), phase
    )
    return {"scoreboard_phase_ref": phase_ref}


def _registry_scope(loaded, scope):
    registry = runner._load_registry(
        loaded.registry_snapshot_uri, loaded.plan.registry_signature
    )
    return runner._registry_scope(registry, scope)


def plan_summary_batch_wave(
    *, scoreboard_phase_refs: Sequence[Mapping[str, Any]], **_context
) -> dict[str, Any]:
    """Offline enumerate wave-two IDs and persist each <=50 descriptor."""

    batch_refs = []
    scope_indexes = []
    for wrapped in scoreboard_phase_refs:
        phase_ref = wrapped["scoreboard_phase_ref"]
        phase = _read_ref(phase_ref, kind="espn-scoreboard-phase-result-v1")
        binding, descriptor, loaded, scope, _ = _binding(phase["scope_binding_ref"])
        checkpoint = _read_ref(phase["checkpoint_ref"])
        competition, edition = _registry_scope(loaded, scope)
        request_plan = {
            item.request_id: item
            for item in runner._scoreboard_requests(
                scope,
                loaded.bindings[scope.scope_id],
                as_of=loaded.plan.as_of,
                mode=runner._effective_mode(loaded),
            )
        }
        fetched = {}
        raw_store = EspnRawStore.from_uri(descriptor["raw_store_uri"])
        for record in checkpoint["requests"]:
            request = request_plan.get(record["request_id"])
            if request is None:
                raise OperationsError("scoreboard checkpoint has unplanned request")
            body = raw_store.load_exact(record["raw_uri"], record["raw_sha256"])
            rows = runner.parse_scoreboards(
                body,
                competition=competition,
                edition=edition,
                query_start=request.query_start,
                query_end=request.query_end,
            )
            for row in rows:
                existing = fetched.get(row.event_id)
                if existing is not None and existing != row:
                    raise OperationsError("conflicting scoreboard event")
                fetched[row.event_id] = row
        missing_known = sorted(
            item.event_id
            for item in loaded.bindings[scope.scope_id].known_nonterminal_events
            if item.event_id not in fetched
        )
        if missing_known:
            raise OperationsError(
                f"known non-terminal events absent from scoreboard: {missing_known}"
            )
        event_ids = tuple(
            sorted(row.event_id for row in fetched.values() if row.summary_required)
        )
        planned = make_summary_batches(
            event_ids,
            run_id=loaded.plan.run_id,
            attempt=loaded.attempt,
            scope_id=scope.scope_id,
            plan_signature=loaded.signature,
            max_events=MAX_SCOPE_SUMMARY_EVENTS,
        )
        scope_batch_refs = []
        for batch in planned["batches"]:
            batch_uri = _join_uri(
                descriptor["scope_root"],
                "raw",
                f"summary-plan-{batch['batch_id']}.json",
            )
            checkpoint_uri = _join_uri(
                descriptor["scope_root"],
                "raw",
                f"summary-{batch['batch_id']}.json",
            )
            batch_payload = {
                "kind": "espn-summary-batch-plan-v1",
                "schema_version": 1,
                "scope_binding_ref": phase["scope_binding_ref"],
                "expected_batch": batch,
                "checkpoint_uri": checkpoint_uri,
            }
            batch_ref = _write_payload(batch_uri, batch_payload)
            wrapped_ref = {"summary_batch_ref": batch_ref}
            batch_refs.append(wrapped_ref)
            scope_batch_refs.append(batch_ref)
        scope_indexes.append(
            {
                "scope_id": scope.scope_id,
                "scope_binding_ref": phase["scope_binding_ref"],
                "scoreboard_phase_ref": phase_ref,
                "summary_batch_refs": scope_batch_refs,
                "budget_used": planned["budget_used"],
                "budget_limit": planned["budget_limit"],
                "budget_exhausted": planned["budget_exhausted"],
                "pending_event_ids": planned["pending_event_ids"],
            }
        )
    index_payload = {
        "kind": "espn-summary-wave-index-v1",
        "schema_version": 1,
        "expected_map_count": len(batch_refs),
        "scopes": sorted(scope_indexes, key=lambda item: item["scope_id"]),
    }
    first_phase = _read_ref(
        scoreboard_phase_refs[0]["scoreboard_phase_ref"],
        kind="espn-scoreboard-phase-result-v1",
    )
    _, descriptor, _, _, _ = _binding(first_phase["scope_binding_ref"])
    root = descriptor["scope_root"].rsplit("/scopes/", 1)[0]
    index_ref = _write_payload(
        _join_uri(root, "summary-wave-index.json"), index_payload
    )
    return {"summary_batch_refs": batch_refs, "summary_index_ref": index_ref}


def fetch_summary_batch(
    *, summary_batch_ref: Mapping[str, str], **_context
) -> dict[str, str]:
    """Mapped network wave two: at most 50 immutable Summary responses."""

    batch = _read_ref(summary_batch_ref, kind="espn-summary-batch-plan-v1")
    _, descriptor, loaded, scope, _ = _binding(batch["scope_binding_ref"])
    expected = batch["expected_batch"]
    if len(expected["event_ids"]) > 50:
        raise OperationsError("Summary network batch exceeds 50 events")
    raw_store = EspnRawStore.from_uri(descriptor["raw_store_uri"])
    client = _http_client(
        raw_store, max_summary_events=max(1, len(expected["event_ids"]))
    )
    checkpoint_ref = _resume_checkpoint(batch["checkpoint_uri"], expected)
    if checkpoint_ref is None:
        records = []
        for event_id in expected["event_ids"]:
            request_id = f"summary:{event_id}"
            resumed = _resume_request_checkpoint(
                batch_uri=batch["checkpoint_uri"],
                expected=expected,
                request_id=request_id,
            )
            if resumed is not None:
                records.append(resumed)
                continue
            url = runner.SUMMARY_URL.format(slug=scope.slug)
            params = {"event": event_id}
            fetched = client.fetch_json(
                url,
                EndpointType.SUMMARY,
                params,
                competition_id=scope.espn_id,
                event_id=event_id,
                force_refresh=True,
            )
            record = runner._raw_request_from_fetch(
                request_id=request_id,
                scope_id=scope.scope_id,
                endpoint="summary",
                event_id=event_id,
                result=fetched,
                query_start=None,
                query_end=None,
            )
            records.append(
                _persist_request_checkpoint(
                    batch_uri=batch["checkpoint_uri"],
                    expected=expected,
                    request=record,
                )
            )
        checkpoint_ref = _seal_completed_batch(
            batch_uri=batch["checkpoint_uri"],
            expected=expected,
            requests=records,
        )
    phase = {
        "kind": "espn-summary-phase-result-v1",
        "schema_version": 1,
        "scope_binding_ref": batch["scope_binding_ref"],
        "expected_batch": expected,
        "checkpoint_ref": checkpoint_ref,
    }
    phase_ref = _write_payload(batch["checkpoint_uri"] + ".phase.json", phase)
    return {"summary_phase_ref": phase_ref}


def reduce_raw_manifest_wave(
    *,
    summary_index_ref: Mapping[str, str],
    summary_phase_refs: Sequence[Mapping[str, Any]],
    **_context,
) -> list[dict[str, Any]]:
    """Deterministically reduce exact signed batch set; no shared mutable map."""

    index = _read_ref(summary_index_ref, kind="espn-summary-wave-index-v1")
    summary_phases = {}
    for wrapped in summary_phase_refs:
        phase_ref = wrapped["summary_phase_ref"]
        phase = _read_ref(phase_ref, kind="espn-summary-phase-result-v1")
        key = phase["expected_batch"]["batch_id"]
        if key in summary_phases:
            raise OperationsError("duplicate Summary phase batch")
        summary_phases[key] = (phase_ref, phase)
    outputs = []
    consumed: set[str] = set()
    for scope_index in index["scopes"]:
        if scope_index["budget_exhausted"]:
            raise OperationsError(
                f"scope {scope_index['scope_id']} exhausted 100% Summary budget"
            )
        _, descriptor, loaded, scope, _ = _binding(scope_index["scope_binding_ref"])
        scoreboard_phase = _read_ref(
            scope_index["scoreboard_phase_ref"],
            kind="espn-scoreboard-phase-result-v1",
        )
        expected_batches = [scoreboard_phase["expected_batch"]]
        checkpoints = [_read_ref(scoreboard_phase["checkpoint_ref"])]
        for batch_ref in scope_index["summary_batch_refs"]:
            batch = _read_ref(batch_ref, kind="espn-summary-batch-plan-v1")
            batch_id = batch["expected_batch"]["batch_id"]
            if batch_id not in summary_phases:
                raise OperationsError("missing Summary phase batch")
            phase_ref, phase = summary_phases[batch_id]
            consumed.add(batch_id)
            if phase["scope_binding_ref"] != scope_index["scope_binding_ref"]:
                raise OperationsError("Summary phase scope binding drift")
            expected_batches.append(batch["expected_batch"])
            checkpoints.append(_read_ref(phase["checkpoint_ref"]))
        manifest = reduce_raw_checkpoints(
            run_id=loaded.plan.run_id,
            attempt=loaded.attempt,
            mode=loaded.mode,
            as_of=loaded.plan.as_of.isoformat(),
            registry_signature=loaded.plan.registry_signature,
            plan_signature=loaded.signature,
            selected_scopes=(scope.scope_id,),
            expected_batches=expected_batches,
            checkpoints=checkpoints,
        )
        raw_ref = _write_payload(descriptor["raw_manifest_uri"], manifest)
        phase_payload = {
            "kind": "espn-raw-reduction-result-v1",
            "schema_version": 1,
            "scope_binding_ref": scope_index["scope_binding_ref"],
            "raw_manifest_ref": raw_ref,
        }
        phase_ref = _write_payload(
            _join_uri(descriptor["scope_root"], "raw-reduction-phase.json"),
            phase_payload,
        )
        outputs.append({"raw_phase_ref": phase_ref})
    if consumed != set(summary_phases):
        raise OperationsError("unplanned extra Summary phase batch")
    return outputs


def bind_replay_raw_manifests(
    *, planning: Mapping[str, Any], **_context
) -> list[dict[str, Any]]:
    """Bind replay plans to exact historical Raw without creating HTTP work."""

    outputs = []
    for wrapped in planning["scope_binding_refs"]:
        scope_binding_ref = wrapped["scope_binding_ref"]
        _, descriptor, loaded, scope, _ = _binding(scope_binding_ref)
        if loaded.mode != "replay" or loaded.replay_source is None:
            raise OperationsError("replay raw binding requires replay plan identity")
        body = runner._read_artifact(descriptor["raw_manifest_uri"])
        digest = hashlib.sha256(body).hexdigest()
        if digest != loaded.replay_source.raw_manifest_sha256:
            raise OperationsError("replay raw manifest hash mismatch")
        manifest = runner._validate_raw_manifest(json.loads(body.decode("utf-8")))
        if scope.scope_id not in manifest["selected_scopes"]:
            raise OperationsError("replay raw manifest scope mismatch")
        phase = {
            "kind": "espn-raw-reduction-result-v1",
            "schema_version": 1,
            "scope_binding_ref": scope_binding_ref,
            "raw_manifest_ref": {
                "uri": descriptor["raw_manifest_uri"],
                "sha256": digest,
            },
        }
        phase_ref = _write_payload(
            _join_uri(descriptor["scope_root"], "replay-raw-binding.json"),
            phase,
        )
        outputs.append({"raw_phase_ref": phase_ref})
    return outputs


def _execution_options(descriptor, loaded, scope) -> runner.ExecutionOptions:
    return runner.ExecutionOptions(
        mode=loaded.mode,
        scopes=(scope.scope_id,),
        as_of=loaded.plan.as_of,
        run_id=loaded.plan.run_id,
        attempt=loaded.attempt,
        plan_uri=descriptor["plan_ref"]["uri"],
        raw_manifest_uri=descriptor["raw_manifest_uri"],
        output_uri=loaded.output_uri,
        raw_store_uri=descriptor["raw_store_uri"],
        max_events=loaded.max_events,
    )


def offline_parse_scope(
    *, raw_phase_ref: Mapping[str, str], **_context
) -> dict[str, str]:
    """Mapped zero-network parse from one exact raw manifest."""

    raw_phase = _read_ref(raw_phase_ref, kind="espn-raw-reduction-result-v1")
    _, descriptor, loaded, scope, _ = _binding(raw_phase["scope_binding_ref"])
    raw_ref = raw_phase["raw_manifest_ref"]
    raw_payload = _read_ref(raw_ref, kind=runner.RAW_MANIFEST_KIND)
    if raw_ref["uri"] != descriptor["raw_manifest_uri"]:
        raise OperationsError("offline parse raw URI differs from signed plan")
    if raw_payload["selected_scopes"] != [scope.scope_id]:
        raise OperationsError("offline parse raw scope set mismatch")
    staged = runner.stage(
        _execution_options(descriptor, loaded, scope),
        raw_store=EspnRawStore.from_uri(descriptor["raw_store_uri"]),
    )
    phase = {
        "kind": "espn-offline-parse-result-v1",
        "schema_version": 1,
        "scope_binding_ref": raw_phase["scope_binding_ref"],
        "raw_manifest_ref": raw_ref,
        "staging": staged.payload,
    }
    phase_ref = _write_payload(
        _join_uri(descriptor["scope_root"], "offline-parse.json"), phase
    )
    if staged.exit_code != 0 or staged.payload["state"] == "incomplete":
        raise OperationsError(f"offline ESPN parse incomplete for {scope.scope_id}")
    return {"offline_ref": phase_ref}


def _quality_payload(report) -> dict[str, Any]:
    return {
        "scope_id": report.scope_id,
        "passed": report.passed,
        "failures": list(report.failures),
        "row_counts": dict(report.row_counts),
        "row_hashes": dict(report.row_hashes),
        "ledger_count": report.ledger_count,
        "ledger_hash": report.ledger_hash,
    }


def _assert_generation_binding(*, generation, loaded, scope, state: str) -> None:
    """Bind a snapshot to the exact current plan or its signed no-op prior."""

    binding = loaded.bindings[scope.scope_id]
    if generation.plan != scope:
        raise OperationsError("generation scope plan differs from signed plan")
    if state == "staged":
        expected = (
            loaded.plan.run_id,
            loaded.registry_snapshot_uri,
            loaded.plan.registry_signature,
            loaded.signature,
            binding.generation_id,
            binding.batch_id,
            binding.ingested_at,
        )
        actual = (
            generation.run_id,
            generation.registry_snapshot_uri,
            generation.registry_signature,
            generation.plan_signature,
            generation.generation_id,
            generation.batch_id,
            generation.ingested_at,
        )
        if actual != expected:
            raise OperationsError(
                "generation identity differs from current signed plan"
            )
        return
    if state != "noop" or binding.prior is None:
        raise OperationsError("generation state lacks an exact signed identity")
    prior = binding.prior
    if (
        generation.generation_id,
        generation.generation_signature,
        generation.manifest_sha256,
    ) != (
        prior.generation_id,
        prior.generation_signature,
        prior.manifest_sha256,
    ):
        raise OperationsError("no-op generation differs from signed prior identity")


def staging_dq_scope(*, offline_ref: Mapping[str, str], **_context) -> dict[str, str]:
    """Run the full Task 4 scope validator before any append path exists."""

    phase = _read_ref(offline_ref, kind="espn-offline-parse-result-v1")
    _, descriptor, loaded, scope, _ = _binding(phase["scope_binding_ref"])
    staging = phase["staging"]
    if staging["run_id"] != loaded.plan.run_id:
        raise OperationsError("staging run identity mismatch")
    if staging["plan_signature"] != loaded.signature:
        raise OperationsError("staging plan identity mismatch")
    if staging["registry_signature"] != loaded.plan.registry_signature:
        raise OperationsError("staging registry identity mismatch")
    scope_result = staging["scopes"]
    if len(scope_result) != 1 or scope_result[0]["scope_id"] != scope.scope_id:
        raise OperationsError("staging scope result set mismatch")
    state = scope_result[0]["state"]
    snapshot_ref = None
    quality = None
    raw_manifest = _read_ref(phase["raw_manifest_ref"], kind=runner.RAW_MANIFEST_KIND)
    current_requests = [
        request
        for checkpoint in raw_manifest["checkpoints"]
        for request in checkpoint["requests"]
    ]
    proxy_bytes = sum(int(item["proxy_bytes"]) for item in current_requests)
    direct_requests = len(current_requests)
    if state == "staged":
        snapshot_ref = {
            "uri": scope_result[0]["generation_snapshot_uri"],
            "sha256": scope_result[0]["generation_snapshot_sha256"],
        }
        generation = runner.load_scope_snapshot(
            snapshot_ref["uri"],
            artifact_sha256=snapshot_ref["sha256"],
            expected_scope_id=scope.scope_id,
        )
        _assert_generation_binding(
            generation=generation, loaded=loaded, scope=scope, state=state
        )
        quality_report = validate_scope_generation(generation)
        if not quality_report.passed:
            raise OperationsError(
                "staging DQ failed: " + "; ".join(quality_report.failures)
            )
        quality = _quality_payload(quality_report)
    elif state == "noop":
        prior = loaded.bindings[scope.scope_id].prior
        if prior is None:
            raise OperationsError("staging no-op lacks signed prior identity")
        snapshot_ref = {
            "uri": prior.uri,
            "sha256": prior.artifact_sha256,
        }
        prior_generation = runner.load_scope_snapshot(
            prior.uri,
            artifact_sha256=prior.artifact_sha256,
            expected_scope_id=scope.scope_id,
        )
        _assert_generation_binding(
            generation=prior_generation, loaded=loaded, scope=scope, state=state
        )
        quality_report = validate_scope_generation(prior_generation)
        if not quality_report.passed:
            raise OperationsError("signed no-op prior failed DQ")
        quality = _quality_payload(quality_report)
    else:
        raise OperationsError("staging result is not publishable")
    dq = {
        "kind": "espn-staging-dq-result-v1",
        "schema_version": 1,
        "scope_binding_ref": phase["scope_binding_ref"],
        "state": state,
        "snapshot_ref": snapshot_ref,
        "quality": quality,
        "direct_requests": direct_requests,
        "proxy_bytes": proxy_bytes,
    }
    dq_ref = _write_payload(_join_uri(descriptor["scope_root"], "staging-dq.json"), dq)
    return {"staging_dq_ref": dq_ref}


def _evidence_payload(
    *,
    dag_id: str,
    loaded,
    scope,
    state: str,
    generation,
    lease: ScopeLease,
    recorded_at: datetime,
    publication_intent_ref: Mapping[str, str] | None,
) -> dict[str, Any]:
    if recorded_at.tzinfo is None:
        raise OperationsError("run evidence recorded_at must be timezone-aware")
    return {
        "kind": "espn-run-manifest-evidence-v1",
        "schema_version": 1,
        "dag_id": _required(dag_id, "dag_id"),
        "run_id": loaded.plan.run_id,
        "attempt": loaded.attempt,
        "scope_id": scope.scope_id,
        "state": state,
        "plan_signature": loaded.signature,
        "registry_signature": loaded.plan.registry_signature,
        "generation_id": generation.generation_id,
        "generation_signature": generation.generation_signature,
        "manifest_sha256": generation.manifest_sha256,
        "lease_epoch": lease.epoch,
        "publication_intent_ref": publication_intent_ref,
        "recorded_at": recorded_at.astimezone(UTC).isoformat(),
    }


def _publication_intent_identity(
    *, dag_id: str, loaded, scope, generation, snapshot_ref: Mapping[str, str]
) -> dict[str, Any]:
    return {
        "dag_id": _required(dag_id, "dag_id"),
        "run_id": _required(loaded.plan.run_id, "run_id"),
        "attempt": loaded.attempt,
        "scope_id": _required(scope.scope_id, "scope_id"),
        "plan_signature": _sha(loaded.signature, "plan_signature"),
        "registry_signature": _sha(
            loaded.plan.registry_signature, "registry_signature"
        ),
        "generation_id": _required(generation.generation_id, "generation_id"),
        "generation_signature": _sha(
            generation.generation_signature, "generation_signature"
        ),
        "manifest_sha256": _sha(generation.manifest_sha256, "manifest_sha256"),
        "snapshot_ref": {
            "uri": _required(snapshot_ref["uri"], "snapshot uri"),
            "sha256": _sha(snapshot_ref["sha256"], "snapshot sha256"),
        },
    }


def _load_or_create_publication_intent(
    *,
    uri: str,
    dag_id: str,
    loaded,
    scope,
    generation,
    snapshot_ref: Mapping[str, str],
    prepared_at: datetime,
    complete_exists: Callable[[], bool],
) -> tuple[dict[str, Any], dict[str, str], bool]:
    """Durably prepare exact publication recovery before COMPLETE is appended."""

    identity = _publication_intent_identity(
        dag_id=dag_id,
        loaded=loaded,
        scope=scope,
        generation=generation,
        snapshot_ref=snapshot_ref,
    )
    try:
        body = runner._read_artifact(uri)
    except FileNotFoundError:
        if not callable(complete_exists):
            raise TypeError("complete_exists must be callable")
        if complete_exists():
            raise OperationsError("exact COMPLETE predates intent")
        payload = {
            "kind": "espn-publication-intent-v1",
            "schema_version": 1,
            **identity,
            "prepared_at": _iso_utc(prepared_at, "publication prepared_at"),
        }
        return payload, _write_payload(uri, payload), True
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("publication intent is not valid JSON") from exc
    _validate_publication_intent_payload(payload, identity)
    return (
        payload,
        {"uri": uri, "sha256": hashlib.sha256(body).hexdigest()},
        False,
    )


def _validate_publication_intent_payload(
    payload: object, identity: Mapping[str, Any]
) -> datetime:
    expected_keys = {
        "kind",
        "schema_version",
        *identity,
        "prepared_at",
    }
    if not isinstance(payload, dict) or set(payload) != expected_keys:
        raise OperationsError("publication intent schema mismatch")
    if (
        payload["kind"],
        payload["schema_version"],
        {field: payload[field] for field in identity},
    ) != ("espn-publication-intent-v1", 1, identity):
        raise OperationsError("publication intent identity mismatch")
    try:
        recovered_at = datetime.fromisoformat(payload["prepared_at"])
    except (TypeError, ValueError) as exc:
        raise OperationsError("publication intent prepared_at is invalid") from exc
    if (
        recovered_at.tzinfo is None
        or payload["prepared_at"] != recovered_at.astimezone(UTC).isoformat()
    ):
        raise OperationsError("publication intent prepared_at must be canonical UTC")
    return recovered_at


def _validate_publication_intent_ref(
    ref: Mapping[str, str],
    *,
    dag_id: str,
    loaded,
    scope,
    generation,
    snapshot_ref: Mapping[str, str],
) -> dict[str, Any]:
    payload = _read_ref(ref, kind="espn-publication-intent-v1")
    identity = _publication_intent_identity(
        dag_id=dag_id,
        loaded=loaded,
        scope=scope,
        generation=generation,
        snapshot_ref=snapshot_ref,
    )
    _validate_publication_intent_payload(payload, identity)
    return payload


def _validate_publication_intent_for_result(
    publication: Mapping[str, Any],
    *,
    dag_id: str,
    loaded,
    scope,
    generation,
) -> None:
    intent_ref = publication.get("publication_intent_ref")
    if publication.get("state") == "noop":
        if intent_ref is not None:
            raise OperationsError("no-op publication must not have an intent")
        return
    if publication.get("state") != "complete" or not isinstance(intent_ref, Mapping):
        raise OperationsError("complete publication requires an exact intent")
    _validate_publication_intent_ref(
        intent_ref,
        dag_id=dag_id,
        loaded=loaded,
        scope=scope,
        generation=generation,
        snapshot_ref=publication["snapshot_ref"],
    )


def _load_existing_publication_evidence(
    *,
    uri: str,
    dag_id: str,
    loaded,
    scope,
    state: str,
    generation,
    lease: ScopeLease,
    publication_intent_ref: Mapping[str, str] | None,
) -> tuple[dict[str, Any], dict[str, str], datetime] | None:
    """Recover an exact post-COMPLETE evidence artifact after a DB crash."""

    try:
        body = runner._read_artifact(uri)
    except FileNotFoundError:
        return None
    try:
        payload = json.loads(body.decode("utf-8"))
        recorded_at = datetime.fromisoformat(payload["recorded_at"])
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        raise OperationsError("existing publication evidence is invalid") from exc
    expected = _evidence_payload(
        dag_id=dag_id,
        loaded=loaded,
        scope=scope,
        state=state,
        generation=generation,
        lease=lease,
        recorded_at=recorded_at,
        publication_intent_ref=publication_intent_ref,
    )
    if payload != expected:
        raise OperationsError("existing publication evidence identity mismatch")
    return (
        payload,
        {"uri": uri, "sha256": hashlib.sha256(body).hexdigest()},
        recorded_at,
    )


def publish_scope(*, staging_dq_ref: Mapping[str, str], **_context) -> dict[str, str]:
    """Publish under a row-lock fence held through the COMPLETE manifest."""

    dq = _read_ref(staging_dq_ref, kind="espn-staging-dq-result-v1")
    _, descriptor, loaded, scope, lease = _binding(dq["scope_binding_ref"])
    generation = runner.load_scope_snapshot(
        dq["snapshot_ref"]["uri"],
        artifact_sha256=dq["snapshot_ref"]["sha256"],
        expected_scope_id=scope.scope_id,
    )
    _assert_generation_binding(
        generation=generation, loaded=loaded, scope=scope, state=dq["state"]
    )
    quality = validate_scope_generation(generation)
    if not quality.passed or dq["quality"] != _quality_payload(quality):
        raise OperationsError("publication input differs from exact staging DQ")
    if int(dq["proxy_bytes"]) != 0:
        raise OperationsError("publication input contains proxy traffic")
    store = PostgresEspnControlStore.from_env()
    renewed = store.renew(lease, now=datetime.now(UTC), ttl=LEASE_TTL)
    existing_head = store.read_scope_heads_owned((renewed,), now=datetime.now(UTC)).get(
        scope.scope_id
    )
    state = "noop" if dq["state"] == "noop" else "complete"
    evidence_uri = _join_uri(descriptor["scope_root"], "run-evidence.json")
    repository = EspnBronzeRepository()
    with store.publication_guard(renewed, now=datetime.now(UTC)) as fence:
        intent_payload = None
        intent_ref = None
        intent_created = False
        if state == "complete":
            intent_payload, intent_ref, intent_created = (
                _load_or_create_publication_intent(
                    uri=_join_uri(descriptor["scope_root"], "publication-intent.json"),
                    dag_id=descriptor["dag_id"],
                    loaded=loaded,
                    scope=scope,
                    generation=generation,
                    snapshot_ref=dq["snapshot_ref"],
                    prepared_at=fence.publication_time(),
                    complete_exists=lambda: repository.exact_complete_exists(
                        generation
                    ),
                )
            )
            publication = repository.publish_scope(generation, publication_fence=fence)
            if publication.state not in {
                ScopePublicationState.PUBLISHED,
                ScopePublicationState.IDEMPOTENT,
            }:
                raise OperationsError("scope publication did not reach COMPLETE")
            if publication.manifest_sha256 != generation.manifest_sha256:
                raise OperationsError("published scope manifest identity mismatch")
        recovered_evidence = _load_existing_publication_evidence(
            uri=evidence_uri,
            dag_id=descriptor["dag_id"],
            loaded=loaded,
            scope=scope,
            state=state,
            generation=generation,
            lease=renewed,
            publication_intent_ref=intent_ref,
        )
        if state == "noop" and recovered_evidence is not None:
            recorded_at = recovered_evidence[2]
        elif (
            state == "complete"
            and publication.state is ScopePublicationState.IDEMPOTENT
        ):
            exact_existing_head = existing_head is not None and (
                existing_head.dag_id,
                existing_head.run_id,
                existing_head.generation_id,
                existing_head.generation_signature,
                existing_head.manifest_sha256,
                existing_head.snapshot_sha256,
            ) == (
                descriptor["dag_id"],
                loaded.plan.run_id,
                generation.generation_id,
                generation.generation_signature,
                generation.manifest_sha256,
                dq["snapshot_ref"]["sha256"],
            )
            if exact_existing_head:
                recorded_at = existing_head.published_at
            elif recovered_evidence is not None:
                recorded_at = recovered_evidence[2]
            elif intent_payload is not None and not intent_created:
                recorded_at = datetime.fromisoformat(intent_payload["prepared_at"])
            else:
                raise OperationsError(
                    "idempotent COMPLETE lacks exact durable publication time"
                )
        else:
            recorded_at = fence.publication_time()
        if recovered_evidence is not None:
            evidence_payload, evidence_ref, evidence_recorded_at = recovered_evidence
            if evidence_recorded_at != recorded_at:
                raise OperationsError(
                    "durable publication timestamp conflicts with existing evidence"
                )
        else:
            evidence_payload = _evidence_payload(
                dag_id=descriptor["dag_id"],
                loaded=loaded,
                scope=scope,
                state=state,
                generation=generation,
                lease=renewed,
                recorded_at=recorded_at,
                publication_intent_ref=intent_ref,
            )
            evidence_ref = _write_payload(evidence_uri, evidence_payload)
        evidence = RunManifestEvidence(
            dag_id=descriptor["dag_id"],
            run_id=loaded.plan.run_id,
            attempt=loaded.attempt,
            scope_id=scope.scope_id,
            plan_signature=loaded.signature,
            registry_signature=loaded.plan.registry_signature,
            state=state,
            evidence_uri=evidence_ref["uri"],
            evidence_sha256=evidence_ref["sha256"],
            recorded_at=recorded_at,
        )
        if state == "complete":
            head = ScopeHead(
                dag_id=descriptor["dag_id"],
                scope_id=scope.scope_id,
                generation_id=generation.generation_id,
                generation_signature=generation.generation_signature,
                manifest_sha256=generation.manifest_sha256,
                snapshot_uri=dq["snapshot_ref"]["uri"],
                snapshot_sha256=dq["snapshot_ref"]["sha256"],
                registry_signature=loaded.plan.registry_signature,
                plan_signature=loaded.signature,
                run_id=loaded.plan.run_id,
                published_at=recorded_at,
            )
            fence.record_published(head, evidence)
        else:
            fence.record_evidence(evidence)
    result = {
        "kind": "espn-publication-result-v1",
        "schema_version": 1,
        "scope_binding_ref": dq["scope_binding_ref"],
        "state": state,
        "snapshot_ref": dq["snapshot_ref"],
        "evidence_ref": evidence_ref,
        "publication_intent_ref": intent_ref,
        "direct_requests": dq["direct_requests"],
        "proxy_bytes": dq["proxy_bytes"],
    }
    result_ref = _write_payload(
        _join_uri(descriptor["scope_root"], "publication-result.json"), result
    )
    return {"publication_ref": result_ref}


def _evidence_dict(item: RunManifestEvidence) -> dict[str, Any]:
    return {
        "dag_id": item.dag_id,
        "run_id": item.run_id,
        "attempt": item.attempt,
        "scope_id": item.scope_id,
        "plan_signature": item.plan_signature,
        "registry_signature": item.registry_signature,
        "state": item.state,
        "evidence_uri": item.evidence_uri,
        "evidence_sha256": item.evidence_sha256,
        "recorded_at": item.recorded_at.isoformat(),
    }


def persist_run_manifests(
    *,
    plan_index_ref: Mapping[str, str],
    publication_refs: Sequence[Mapping[str, Any]],
    **_context,
) -> dict[str, Any]:
    """Seal exact current-run durable evidence; never consult a latest pointer."""

    index = _read_ref(plan_index_ref, kind="espn-plan-index-v1")
    indexed_plans = {}
    for descriptor_ref in index["scope_plan_refs"]:
        descriptor = _read_ref(descriptor_ref, kind="espn-scope-plan-descriptor-v1")
        if descriptor["scope_id"] in indexed_plans:
            raise OperationsError("plan index duplicates a scope")
        if (
            descriptor["dag_id"],
            descriptor["run_id"],
            descriptor["attempt"],
        ) != (index["dag_id"], index["run_id"], index["attempt"]):
            raise OperationsError("scope descriptor differs from plan index")
        indexed_plans[descriptor["scope_id"]] = descriptor["plan_signature"]
    if set(indexed_plans) != set(index["scope_ids"]):
        raise OperationsError("plan index scope descriptor set is incomplete")
    by_scope = {}
    normalized_refs = []
    for wrapped in publication_refs:
        publication_ref = wrapped["publication_ref"]
        publication = _read_ref(publication_ref, kind="espn-publication-result-v1")
        _, _, loaded, scope, lease = _binding(publication["scope_binding_ref"])
        if loaded.plan.run_id != index["run_id"] or loaded.attempt != index["attempt"]:
            raise OperationsError("publication run identity differs from plan index")
        if (
            loaded.signature,
            loaded.plan.registry_signature,
        ) != (
            indexed_plans.get(scope.scope_id),
            index["registry_signature"],
        ):
            raise OperationsError("publication plan differs from plan index")
        if publication["state"] not in {"complete", "noop"}:
            raise OperationsError("publication result state is invalid")
        generation = runner.load_scope_snapshot(
            publication["snapshot_ref"]["uri"],
            artifact_sha256=publication["snapshot_ref"]["sha256"],
            expected_scope_id=scope.scope_id,
        )
        _validate_publication_intent_for_result(
            publication,
            dag_id=index["dag_id"],
            loaded=loaded,
            scope=scope,
            generation=generation,
        )
        generation_state = (
            "staged" if publication["state"] == "complete" else publication["state"]
        )
        _assert_generation_binding(
            generation=generation,
            loaded=loaded,
            scope=scope,
            state=generation_state,
        )
        evidence_payload = _read_ref(
            publication["evidence_ref"], kind="espn-run-manifest-evidence-v1"
        )
        expected_evidence = _evidence_payload(
            dag_id=index["dag_id"],
            loaded=loaded,
            scope=scope,
            state=publication["state"],
            generation=generation,
            lease=lease,
            recorded_at=datetime.fromisoformat(evidence_payload["recorded_at"]),
            publication_intent_ref=publication["publication_intent_ref"],
        )
        if evidence_payload != expected_evidence:
            raise OperationsError("publication evidence identity mismatch")
        if int(publication["proxy_bytes"]) != 0:
            raise OperationsError("publication result contains proxy traffic")
        if scope.scope_id in by_scope:
            raise OperationsError("duplicate scope publication result")
        by_scope[scope.scope_id] = publication
        normalized_refs.append({"publication_ref": publication_ref})
    if set(by_scope) != set(index["scope_ids"]):
        raise OperationsError("publication result scope set is incomplete")
    durable = PostgresEspnControlStore.from_env().read_run_evidence(
        dag_id=index["dag_id"], run_id=index["run_id"], attempt=index["attempt"]
    )
    if {item.scope_id for item in durable} != set(index["scope_ids"]):
        raise OperationsError("durable current-run evidence scope set is incomplete")
    for item in durable:
        publication = by_scope[item.scope_id]
        if publication["evidence_ref"] != {
            "uri": item.evidence_uri,
            "sha256": item.evidence_sha256,
        }:
            raise OperationsError("durable run evidence reference mismatch")
        evidence_payload = _read_ref(
            publication["evidence_ref"], kind="espn-run-manifest-evidence-v1"
        )
        if (
            evidence_payload["run_id"],
            evidence_payload["dag_id"],
            evidence_payload["attempt"],
            evidence_payload["scope_id"],
            evidence_payload["plan_signature"],
            evidence_payload["registry_signature"],
            evidence_payload["state"],
        ) != (
            item.run_id,
            item.dag_id,
            item.attempt,
            item.scope_id,
            item.plan_signature,
            item.registry_signature,
            item.state,
        ):
            raise OperationsError("durable evidence payload identity mismatch")
    root = _read_ref(index["scope_plan_refs"][0], kind="espn-scope-plan-descriptor-v1")[
        "scope_root"
    ].rsplit("/scopes/", 1)[0]
    manifest = {
        "kind": "espn-durable-run-manifest-v1",
        "schema_version": 1,
        "dag_id": index["dag_id"],
        "run_id": index["run_id"],
        "attempt": index["attempt"],
        "registry_signature": index["registry_signature"],
        "scope_ids": index["scope_ids"],
        "evidence": [_evidence_dict(item) for item in durable],
        "publication_refs": sorted(
            normalized_refs,
            key=lambda item: _read_ref(item["publication_ref"])["scope_binding_ref"][
                "uri"
            ],
        ),
    }
    manifest_ref = _write_payload(
        _join_uri(root, "durable-run-manifest.json"), manifest
    )
    return {
        "run_manifest_ref": manifest_ref,
        "publication_refs": normalized_refs,
    }


def published_dq_scope(
    *, publication_ref: Mapping[str, str], **_context
) -> dict[str, str]:
    """Read-only exact-generation DQ after the COMPLETE boundary."""

    publication = _read_ref(publication_ref, kind="espn-publication-result-v1")
    _, descriptor, loaded, scope, lease = _binding(publication["scope_binding_ref"])
    generation = runner.load_scope_snapshot(
        publication["snapshot_ref"]["uri"],
        artifact_sha256=publication["snapshot_ref"]["sha256"],
        expected_scope_id=scope.scope_id,
    )
    _validate_publication_intent_for_result(
        publication,
        dag_id=descriptor["dag_id"],
        loaded=loaded,
        scope=scope,
        generation=generation,
    )
    if publication["state"] == "complete":
        _assert_generation_binding(
            generation=generation, loaded=loaded, scope=scope, state="staged"
        )
        report = EspnBronzeRepository().verify_published_scope(generation)
    elif publication["state"] == "noop":
        _assert_generation_binding(
            generation=generation, loaded=loaded, scope=scope, state="noop"
        )
        heads = PostgresEspnControlStore.from_env().read_scope_heads((scope.scope_id,))
        head = heads.get(scope.scope_id)
        prior = loaded.bindings[scope.scope_id].prior
        if prior is None or head is None:
            raise OperationsError("published no-op lacks current COMPLETE head")
        if (head.generation_id, head.manifest_sha256, head.snapshot_sha256) != (
            prior.generation_id,
            prior.manifest_sha256,
            prior.artifact_sha256,
        ):
            raise OperationsError(
                "published no-op current head differs from signed prior"
            )
        report = EspnBronzeRepository().verify_published_scope(generation)
    else:
        raise OperationsError("publication state is invalid")
    if not report.passed:
        raise OperationsError("published DQ failed: " + "; ".join(report.failures))
    evidence = _read_ref(
        publication["evidence_ref"], kind="espn-run-manifest-evidence-v1"
    )
    expected_evidence = _evidence_payload(
        dag_id=descriptor["dag_id"],
        loaded=loaded,
        scope=scope,
        state=publication["state"],
        generation=generation,
        lease=lease,
        recorded_at=datetime.fromisoformat(evidence["recorded_at"]),
        publication_intent_ref=publication["publication_intent_ref"],
    )
    if evidence != expected_evidence:
        raise OperationsError("published DQ evidence identity mismatch")
    result = {
        "kind": "espn-published-dq-result-v1",
        "schema_version": 1,
        "dag_id": descriptor["dag_id"],
        "scope_id": scope.scope_id,
        "run_id": loaded.plan.run_id,
        "attempt": loaded.attempt,
        "plan_signature": loaded.signature,
        "registry_signature": loaded.plan.registry_signature,
        "publication_ref": publication_ref,
        "quality": _quality_payload(report),
    }
    result_ref = _write_payload(
        _join_uri(descriptor["scope_root"], "published-dq.json"), result
    )
    return {"published_dq_ref": result_ref}


def _observed_task_states(context: Mapping[str, Any], task_ids: Sequence[str]):
    dag_run = context.get("dag_run")
    instances = getattr(dag_run, "get_task_instances", lambda: ())()

    def normalized(item) -> str:
        state = getattr(item, "state", "none")
        state = getattr(state, "value", state)
        return str(state or "none").lower().split(".")[-1]

    return {
        task_id: tuple(
            normalized(item)
            for item in instances
            if getattr(item, "task_id", None) == task_id
        )
        for task_id in task_ids
    }


def _scope_operational_metrics(
    index: Mapping[str, Any],
    summary_index: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """Best-effort counters from exact current-run immutable artifacts."""

    metrics = {
        scope_id: {
            "network_requests": 0,
            "proxy_bytes": 0,
            "budget_used": 0,
            "budget_limit": MAX_SCOPE_SUMMARY_EVENTS,
            "schema_drift": False,
        }
        for scope_id in index["scope_ids"]
    }
    if summary_index is not None:
        for item in summary_index.get("scopes", ()):
            scope_id = item.get("scope_id")
            if scope_id in metrics:
                metrics[scope_id]["budget_used"] = int(item["budget_used"])
                metrics[scope_id]["budget_limit"] = int(item["budget_limit"])
    summary_scopes = {
        item.get("scope_id"): item for item in (summary_index or {}).get("scopes", ())
    }
    for descriptor_ref in index["scope_plan_refs"]:
        try:
            descriptor = _read_ref(descriptor_ref, kind="espn-scope-plan-descriptor-v1")
            scope_id = descriptor["scope_id"]
            if scope_id not in metrics or index["mode"] == "replay":
                continue
            try:
                body = runner._read_artifact(descriptor["raw_manifest_uri"])
                document = json.loads(body.decode("utf-8"))
                requests = [
                    request
                    for checkpoint in document.get("checkpoints", ())
                    for request in checkpoint.get("requests", ())
                ]
            except FileNotFoundError:
                checkpoint_uris = [descriptor["scoreboard_checkpoint_uri"]]
                for batch_ref in summary_scopes.get(scope_id, {}).get(
                    "summary_batch_refs", ()
                ):
                    batch = _read_ref(batch_ref, kind="espn-summary-batch-plan-v1")
                    checkpoint_uris.append(batch["checkpoint_uri"])
                requests = []
                for checkpoint_uri in checkpoint_uris:
                    checkpoint = json.loads(
                        runner._read_artifact(checkpoint_uri).decode("utf-8")
                    )
                    requests.extend(checkpoint.get("requests", ()))
            metrics[scope_id]["network_requests"] = len(requests)
            metrics[scope_id]["proxy_bytes"] = sum(
                int(request.get("proxy_bytes", 0)) for request in requests
            )
        except FileNotFoundError:
            continue
        except Exception:
            if isinstance(descriptor_ref, Mapping):
                try:
                    descriptor = _read_ref(descriptor_ref)
                    scope_id = descriptor.get("scope_id")
                    if scope_id in metrics:
                        metrics[scope_id]["schema_drift"] = True
                except Exception:
                    pass
    return dict(sorted(metrics.items()))


def terminal_verdict(
    *,
    producer_task_ids: Sequence[str],
    plan_index_ref: Mapping[str, str] | None = None,
    run_manifest_ref: Mapping[str, str] | None = None,
    published_dq_refs: Sequence[Mapping[str, Any]] | None = None,
    summary_index_ref: Mapping[str, str] | None = None,
    **context,
) -> dict[str, str]:
    """Persist one exact verdict from task states plus current-run evidence."""

    dag_id, run_id, _ = _run_identity(context)
    attempt = _attempt(context)
    root = _join_uri(_artifact_root(), "runs", _run_key(dag_id, run_id))
    failures: list[str] = []
    states = _observed_task_states(context, producer_task_ids)
    expected_counts = {task_id: 1 for task_id in producer_task_ids}
    scope_count = 0
    index = None
    scope_metrics: dict[str, dict[str, Any]] = {}
    try:
        if plan_index_ref is None:
            plan_index_ref = _ref_for_uri(_join_uri(root, "plan-index.json"))
        index = _read_ref(plan_index_ref, kind="espn-plan-index-v1")
        if (index["dag_id"], index["run_id"], index["attempt"]) != (
            dag_id,
            run_id,
            attempt,
        ):
            raise OperationsError("terminal plan identity mismatch")
        scope_count = len(index["scope_ids"])
        plan_identities = {}
        for descriptor_ref in index["scope_plan_refs"]:
            descriptor = _read_ref(descriptor_ref, kind="espn-scope-plan-descriptor-v1")
            loaded_plan = runner._load_signed_plan(descriptor["plan_ref"]["uri"])
            scope_id = descriptor["scope_id"]
            if scope_id in plan_identities:
                raise OperationsError("terminal plan index duplicates a scope")
            if len(loaded_plan.plan.scopes) != 1:
                raise OperationsError("terminal scope plan must contain one scope")
            if (
                descriptor["dag_id"],
                loaded_plan.plan.run_id,
                loaded_plan.attempt,
                loaded_plan.plan.registry_signature,
                loaded_plan.signature,
                loaded_plan.plan.scopes[0].scope_id,
            ) != (
                dag_id,
                run_id,
                attempt,
                index["registry_signature"],
                descriptor["plan_signature"],
                scope_id,
            ):
                raise OperationsError("terminal signed scope plan identity mismatch")
            plan_identities[scope_id] = loaded_plan.signature
        if set(plan_identities) != set(index["scope_ids"]):
            raise OperationsError("terminal signed scope plan set is incomplete")
        for task_id in (
            "fetch_scoreboard_batches",
            "offline_parse",
            "staging_dq",
            "publish_scopes",
            "published_dq",
        ):
            if task_id in expected_counts:
                expected_counts[task_id] = scope_count
        if "fetch_summary_batches" in expected_counts:
            if summary_index_ref is None:
                summary_index_ref = _ref_for_uri(
                    _join_uri(root, "summary-wave-index.json")
                )
            summary_index = _read_ref(
                summary_index_ref, kind="espn-summary-wave-index-v1"
            )
            expected_counts["fetch_summary_batches"] = summary_index[
                "expected_map_count"
            ]
        else:
            summary_index = None
        scope_metrics = _scope_operational_metrics(index, summary_index)
        failures.extend(producer_state_failures(states, expected_counts))
        if run_manifest_ref is None:
            run_manifest_ref = _ref_for_uri(
                _join_uri(root, "durable-run-manifest.json")
            )
        durable_manifest = _read_ref(
            run_manifest_ref, kind="espn-durable-run-manifest-v1"
        )
        if (
            durable_manifest["dag_id"],
            durable_manifest["run_id"],
            durable_manifest["attempt"],
            durable_manifest["registry_signature"],
            durable_manifest["scope_ids"],
        ) != (
            dag_id,
            run_id,
            attempt,
            index["registry_signature"],
            index["scope_ids"],
        ):
            raise OperationsError("durable run manifest identity mismatch")
        publications_by_scope = {}
        for wrapped in durable_manifest["publication_refs"]:
            publication_ref = wrapped["publication_ref"]
            publication = _read_ref(publication_ref, kind="espn-publication-result-v1")
            _, _, loaded_plan, scope, _ = _binding(publication["scope_binding_ref"])
            if scope.scope_id in publications_by_scope:
                raise OperationsError("durable manifest duplicates a publication")
            if (
                publication["state"] in {"complete", "noop"},
                loaded_plan.plan.run_id,
                loaded_plan.attempt,
                loaded_plan.signature,
                loaded_plan.plan.registry_signature,
            ) != (
                True,
                run_id,
                attempt,
                plan_identities.get(scope.scope_id),
                index["registry_signature"],
            ):
                raise OperationsError("durable publication identity mismatch")
            generation = runner.load_scope_snapshot(
                publication["snapshot_ref"]["uri"],
                artifact_sha256=publication["snapshot_ref"]["sha256"],
                expected_scope_id=scope.scope_id,
            )
            _validate_publication_intent_for_result(
                publication,
                dag_id=dag_id,
                loaded=loaded_plan,
                scope=scope,
                generation=generation,
            )
            publications_by_scope[scope.scope_id] = (publication_ref, publication)
        if set(publications_by_scope) != set(index["scope_ids"]):
            raise OperationsError("durable publication scope set is incomplete")
        durable = PostgresEspnControlStore.from_env().read_run_evidence(
            dag_id=dag_id, run_id=run_id, attempt=attempt
        )
        if [_evidence_dict(item) for item in durable] != durable_manifest["evidence"]:
            raise OperationsError("database current-run evidence differs from manifest")
        for evidence in durable:
            if (
                evidence.dag_id,
                evidence.plan_signature,
                evidence.registry_signature,
            ) != (
                dag_id,
                plan_identities.get(evidence.scope_id),
                index["registry_signature"],
            ):
                raise OperationsError("database evidence plan identity mismatch")
            publication = publications_by_scope[evidence.scope_id][1]
            if publication["evidence_ref"] != {
                "uri": evidence.evidence_uri,
                "sha256": evidence.evidence_sha256,
            }:
                raise OperationsError("database evidence artifact reference mismatch")
            artifact_evidence = _read_ref(
                publication["evidence_ref"], kind="espn-run-manifest-evidence-v1"
            )
            if (
                artifact_evidence["dag_id"],
                artifact_evidence["run_id"],
                artifact_evidence["attempt"],
                artifact_evidence["scope_id"],
                artifact_evidence["plan_signature"],
                artifact_evidence["registry_signature"],
                artifact_evidence["state"],
            ) != (
                evidence.dag_id,
                evidence.run_id,
                evidence.attempt,
                evidence.scope_id,
                evidence.plan_signature,
                evidence.registry_signature,
                evidence.state,
            ):
                raise OperationsError("database evidence artifact identity mismatch")
        if published_dq_refs is None:
            published_dq_refs = []
            for descriptor_ref in index["scope_plan_refs"]:
                descriptor = _read_ref(
                    descriptor_ref, kind="espn-scope-plan-descriptor-v1"
                )
                published_dq_refs.append(
                    {
                        "published_dq_ref": _ref_for_uri(
                            _join_uri(descriptor["scope_root"], "published-dq.json")
                        )
                    }
                )
        dq_by_scope = {}
        for wrapped in published_dq_refs:
            dq = _read_ref(
                wrapped["published_dq_ref"], kind="espn-published-dq-result-v1"
            )
            if dq["scope_id"] in dq_by_scope:
                raise OperationsError("published DQ duplicates a scope")
            dq_by_scope[dq["scope_id"]] = dq
        if set(dq_by_scope) != set(index["scope_ids"]):
            raise OperationsError("published DQ scope set is incomplete")
        for scope_id, dq in dq_by_scope.items():
            if (
                dq["dag_id"],
                dq["run_id"],
                dq["attempt"],
                dq["plan_signature"],
                dq["registry_signature"],
                dq["publication_ref"],
                dq["quality"]["passed"],
            ) != (
                dag_id,
                run_id,
                attempt,
                plan_identities[scope_id],
                index["registry_signature"],
                publications_by_scope[scope_id][0],
                True,
            ):
                raise OperationsError("published DQ exact identity mismatch")
    except Exception as exc:
        failures.append(f"{type(exc).__name__}: {exc}")
    if index is not None and not scope_metrics:
        scope_metrics = _scope_operational_metrics(index, None)
    verdict = {
        "kind": "espn-terminal-verdict-v1",
        "schema_version": 1,
        "dag_id": dag_id,
        "run_id": run_id,
        "attempt": attempt,
        "status": "failed" if failures else "complete",
        "failures": sorted(set(failures)),
        "scope_count": scope_count,
        "producer_states": {key: list(value) for key, value in sorted(states.items())},
        "expected_counts": dict(sorted(expected_counts.items())),
        "scope_metrics": scope_metrics,
    }
    verdict_ref = _write_payload(_join_uri(root, "terminal-verdict.json"), verdict)
    if failures:
        from airflow.exceptions import AirflowException

        raise AirflowException(
            "ESPN terminal verdict failed: " + "; ".join(verdict["failures"])
        )
    return {"verdict_ref": verdict_ref}


def _current_verdict_ref(context: Mapping[str, Any]) -> dict[str, str]:
    dag_id, run_id, _ = _run_identity(context)
    uri = _join_uri(
        _artifact_root(),
        "runs",
        _run_key(dag_id, run_id),
        "terminal-verdict.json",
    )
    return _ref_for_uri(uri)


def _optional_payload(uri: str, *, kind: str | None = None):
    try:
        return _read_ref(_ref_for_uri(uri), kind=kind)
    except FileNotFoundError:
        return None


def _latest_discovery_flags(scope_id: str) -> tuple[bool, bool]:
    state = _optional_payload(
        _join_uri(_artifact_root(), "discovery", "latest-state.json"),
        kind="espn-discovery-state-v1",
    )
    if state is None:
        return False, False
    review = _read_ref(state["review_ref"], kind="espn-discovery-review-v1")
    competition_id = scope_id.split(":", 1)[0]
    rollover = any(
        str(item).split(":", 1)[0] == competition_id
        for item in review["quarantined_scopes"]
    )
    return rollover, bool(review["unresolved_discovery_diffs"])


def _head_identity_sha256(head: ScopeHead) -> str:
    return hashlib.sha256(
        _canonical_bytes(
            {
                "kind": "espn-scope-head-identity-v1",
                "dag_id": head.dag_id,
                "run_id": head.run_id,
                "scope_id": head.scope_id,
                "generation_id": head.generation_id,
                "generation_signature": head.generation_signature,
                "manifest_sha256": head.manifest_sha256,
                "snapshot_uri": head.snapshot_uri,
                "snapshot_sha256": head.snapshot_sha256,
                "registry_signature": head.registry_signature,
                "plan_signature": head.plan_signature,
                "published_at": head.published_at.isoformat(),
            }
        )
    ).hexdigest()


def _verified_complete_head(head: ScopeHead | None) -> tuple[ScopeHead | None, str]:
    """Accept freshness only after exact snapshot, COMPLETE and physical parity."""

    if head is None:
        return None, "incomplete"
    try:
        generation = runner.load_scope_snapshot(
            head.snapshot_uri,
            artifact_sha256=head.snapshot_sha256,
            expected_scope_id=head.scope_id,
        )
        identity = (
            generation.plan.scope_id,
            generation.generation_id,
            generation.generation_signature,
            generation.manifest_sha256,
            generation.run_id,
            generation.registry_signature,
            generation.plan_signature,
        )
        expected = (
            head.scope_id,
            head.generation_id,
            head.generation_signature,
            head.manifest_sha256,
            head.run_id,
            head.registry_signature,
            head.plan_signature,
        )
        if identity != expected:
            raise OperationsError("scope head snapshot identity mismatch")
        report = EspnBronzeRepository().verify_published_scope(generation)
        if not report.passed:
            raise OperationsError("scope head COMPLETE physical parity failed")
    except Exception:
        return None, "incomplete"
    return head, "complete"


def _subject_identity(
    *, dag_id: str, run_id: str, scope_id: str, registry_signature: str
) -> str:
    return hashlib.sha256(
        _canonical_bytes(
            {
                "kind": "espn-monitor-subject-v1",
                "dag_id": dag_id,
                "run_id": run_id,
                "scope_id": scope_id,
                "registry_signature": registry_signature,
            }
        )
    ).hexdigest()


def record_health_metrics(
    *,
    plan_index_ref: Mapping[str, str] | None = None,
    verdict_ref: Mapping[str, str] | None = None,
    **context,
) -> dict[str, str]:
    """Always persist health/alerts, preserving a failed verdict as red."""

    verdict_ref = verdict_ref or _current_verdict_ref(context)
    verdict = _read_ref(verdict_ref, kind="espn-terminal-verdict-v1")
    alerts = []
    heads = {}
    if plan_index_ref is None:
        try:
            dag_id, run_id, _ = _run_identity(context)
            plan_index_ref = _ref_for_uri(
                _join_uri(
                    _artifact_root(),
                    "runs",
                    _run_key(dag_id, run_id),
                    "plan-index.json",
                )
            )
        except FileNotFoundError:
            plan_index_ref = None
    plan_signatures: dict[str, str] = {}
    observer_dag_id, observer_run_id, _ = _run_identity(context)
    fallback_identity = None
    if plan_index_ref is not None:
        index = _read_ref(plan_index_ref, kind="espn-plan-index-v1")
        plan_signatures = {
            descriptor["scope_id"]: descriptor["plan_signature"]
            for descriptor in (
                _read_ref(ref, kind="espn-scope-plan-descriptor-v1")
                for ref in index["scope_plan_refs"]
            )
        }
    else:
        dag_id, run_id, _ = _run_identity(context)
        admission = _optional_payload(
            _join_uri(
                _artifact_root(),
                "runs",
                _run_key(dag_id, run_id),
                "admission.json",
            ),
            kind="espn-airflow-admission-v1",
        )
        index = (
            None
            if admission is None
            else {
                "dag_id": admission["dag_id"],
                "run_id": admission["run_id"],
                "scope_ids": admission["scope_ids"],
                "registry_signature": admission["registry_signature"],
            }
        )
        if admission is not None:
            fallback_identity = _ref_for_uri(
                _join_uri(
                    _artifact_root(),
                    "runs",
                    _run_key(observer_dag_id, observer_run_id),
                    "admission.json",
                )
            )["sha256"]
    if index is not None:
        store = PostgresEspnControlStore.from_env()
        store.migrate()
        heads = store.read_scope_heads(index["scope_ids"])
        now = store.current_time()
        conflict = _optional_payload(
            _join_uri(
                _artifact_root(),
                "runs",
                _run_key(verdict["dag_id"], verdict["run_id"]),
                "lease-conflict.json",
            ),
            kind="espn-lease-conflict-v1",
        )
        for scope_id in index["scope_ids"]:
            raw_head = heads.get(scope_id)
            head, head_state = _verified_complete_head(raw_head)
            metric = verdict.get("scope_metrics", {}).get(scope_id, {})
            rollover, unresolved = _latest_discovery_flags(scope_id)
            state = (
                "schema_drift"
                if metric.get("schema_drift")
                else "incomplete"
                if head_state != "complete"
                else verdict["status"]
            )
            if scope_id in plan_signatures:
                alert_identity = {
                    "dag_id": observer_dag_id,
                    "subject_dag_id": observer_dag_id,
                    "subject_run_id": verdict["run_id"],
                    "plan_signature": plan_signatures[scope_id],
                    "registry_signature": index["registry_signature"],
                }
            else:
                alert_identity = {
                    "dag_id": observer_dag_id,
                    "subject_dag_id": None,
                    "subject_run_id": None,
                    "identity_kind": "admission",
                    "identity_sha256": fallback_identity
                    or _subject_identity(
                        dag_id=observer_dag_id,
                        run_id=observer_run_id,
                        scope_id=scope_id,
                        registry_signature=index["registry_signature"],
                    ),
                }
            alerts.extend(
                evaluate_alerts(
                    {
                        **alert_identity,
                        "run_id": verdict["run_id"],
                        "attempt": verdict["attempt"],
                        "scope_id": scope_id,
                        "state": state,
                        "last_complete_at": (
                            head.published_at if head is not None else None
                        ),
                        "direct_requests": metric.get("budget_used", 0),
                        "request_budget": metric.get(
                            "budget_limit", MAX_SCOPE_SUMMARY_EVENTS
                        ),
                        "proxy_bytes": metric.get("proxy_bytes", 0),
                        "lease_conflict": conflict is not None,
                        "unpromoted_current_season": rollover,
                        "unresolved_discovery_diffs": unresolved,
                    },
                    observed_at=now,
                )
            )
    else:
        failure_text = " ".join(verdict["failures"]).casefold()
        failure_identity = hashlib.sha256(
            _canonical_bytes(
                {
                    "kind": "espn-pre-admission-run-v1",
                    "dag_id": observer_dag_id,
                    "run_id": observer_run_id,
                    "attempt": verdict["attempt"],
                }
            )
        ).hexdigest()
        alerts.extend(
            evaluate_alerts(
                {
                    "dag_id": observer_dag_id,
                    "run_id": verdict["run_id"],
                    "attempt": verdict["attempt"],
                    "scope_id": None,
                    "subject_dag_id": None,
                    "subject_run_id": None,
                    "identity_kind": "pre-admission-run",
                    "identity_sha256": failure_identity,
                    "state": (
                        "schema_drift"
                        if "schema" in failure_text or "registry" in failure_text
                        else "failed"
                    ),
                    "last_complete_at": None,
                    "direct_requests": 0,
                    "request_budget": MAX_SCOPE_SUMMARY_EVENTS,
                    "proxy_bytes": 0,
                    "lease_conflict": False,
                    "unpromoted_current_season": False,
                    "unresolved_discovery_diffs": False,
                },
                observed_at=PostgresEspnControlStore.from_env().current_time(),
            )
        )
    health = {
        "kind": "espn-health-result-v1",
        "schema_version": 1,
        "run_id": verdict["run_id"],
        "attempt": verdict["attempt"],
        "status": verdict["status"],
        "verdict_ref": verdict_ref,
        "alerts": alerts,
        "scope_metrics": verdict.get("scope_metrics", {}),
    }
    root = _join_uri(
        _artifact_root(),
        "runs",
        _run_key(verdict["dag_id"], verdict["run_id"]),
    )
    health_ref = _write_payload(_join_uri(root, "health.json"), health)
    if any(item["severity"] == "hard" for item in alerts):
        from airflow.exceptions import AirflowException

        raise AirflowException("ESPN health recorder found hard alerts")
    return {"health_ref": health_ref}


def release_scope_leases(
    *, lease_acquisition_ref: Mapping[str, str] | None = None, **context
) -> dict[str, Any]:
    """Release only the exact acquired owner/epoch/token identities."""

    store = PostgresEspnControlStore.from_env()
    store.migrate()
    dag_id, run_id, _ = _run_identity(context)
    owner_id = f"{dag_id}/{run_id}/{_attempt(context)}"
    leases = store.read_owner_leases(owner_id)
    acquisition = None
    if lease_acquisition_ref is not None:
        acquisition = _read_ref(lease_acquisition_ref, kind="espn-lease-acquisition-v1")
        if acquisition["owner_id"] != owner_id or {
            item.scope_id for item in leases
        } != set(acquisition["scope_ids"]):
            raise OperationsError("current owner leases differ from acquisition")
    released = []
    failures = []
    for lease in leases:
        try:
            store.release(lease, now=datetime.now(UTC))
        except Exception as exc:
            failures.append(f"{lease.scope_id}: {type(exc).__name__}: {exc}")
            continue
        released.append(lease.scope_id)
    scope_ids = sorted(
        acquisition["scope_ids"]
        if acquisition is not None
        else {lease.scope_id for lease in leases}
    )
    result = {
        "kind": "espn-lease-release-result-v1",
        "schema_version": 1,
        "dag_id": dag_id,
        "run_id": run_id,
        "attempt": _attempt(context),
        "scope_ids": scope_ids,
        "released": sorted(released),
        "failures": sorted(failures),
    }
    root = _join_uri(_artifact_root(), "runs", _run_key(dag_id, run_id))
    release_ref = _write_payload(_join_uri(root, "lease-release.json"), result)
    if failures:
        raise OperationsError("lease cleanup failures: " + "; ".join(failures))
    return {"released": sorted(released), "release_ref": release_ref}


def propagate_terminal_failure(
    *,
    verdict_ref: Mapping[str, str] | None = None,
    cleanup_task_ids: Sequence[str] = (),
    **context,
) -> dict[str, str]:
    """Restore failure or seal the only post-health/post-cleanup success receipt."""

    verdict_ref = verdict_ref or _current_verdict_ref(context)
    verdict = _read_ref(verdict_ref, kind="espn-terminal-verdict-v1")
    if verdict["status"] != "complete":
        from airflow.exceptions import AirflowException

        raise AirflowException(
            "ESPN terminal verdict failed: " + "; ".join(verdict["failures"])
        )
    cleanup_states = _observed_task_states(context, cleanup_task_ids)
    failures = producer_state_failures(
        cleanup_states, {task_id: 1 for task_id in cleanup_task_ids}
    )
    if failures:
        from airflow.exceptions import AirflowException

        raise AirflowException("ESPN cleanup/health failed: " + "; ".join(failures))

    dag_id, run_id, _ = _run_identity(context)
    attempt = _attempt(context)
    root = _join_uri(_artifact_root(), "runs", _run_key(dag_id, run_id))
    admission_ref = _ref_for_uri(_join_uri(root, "admission.json"))
    admission = _read_ref(admission_ref, kind="espn-airflow-admission-v1")
    plan_index_ref = _ref_for_uri(_join_uri(root, "plan-index.json"))
    index = _read_ref(plan_index_ref, kind="espn-plan-index-v1")
    durable_ref = _ref_for_uri(_join_uri(root, "durable-run-manifest.json"))
    durable = _read_ref(durable_ref, kind="espn-durable-run-manifest-v1")
    health_ref = _ref_for_uri(_join_uri(root, "health.json"))
    health = _read_ref(health_ref, kind="espn-health-result-v1")
    release_ref = _ref_for_uri(_join_uri(root, "lease-release.json"))
    release = _read_ref(release_ref, kind="espn-lease-release-result-v1")
    identity = (dag_id, run_id, attempt)
    if (
        (verdict["dag_id"], verdict["run_id"], verdict["attempt"]),
        (admission["dag_id"], admission["run_id"], admission["attempt"]),
        (index["dag_id"], index["run_id"], index["attempt"]),
        (durable["dag_id"], durable["run_id"], durable["attempt"]),
        (release["dag_id"], release["run_id"], release["attempt"]),
    ) != (identity, identity, identity, identity, identity):
        raise OperationsError("success receipt inputs have different run identities")
    if (
        (health["run_id"], health["attempt"]) != (run_id, attempt)
        or health["status"] != "complete"
        or health["verdict_ref"] != verdict_ref
        or health["alerts"]
        or release["failures"]
        or sorted(release["released"]) != sorted(index["scope_ids"])
    ):
        raise OperationsError("success receipt inputs are not fully green")
    dq_refs = []
    for descriptor_ref in index["scope_plan_refs"]:
        descriptor = _read_ref(descriptor_ref, kind="espn-scope-plan-descriptor-v1")
        dq_refs.append(
            {
                "scope_id": descriptor["scope_id"],
                "published_dq_ref": _ref_for_uri(
                    _join_uri(descriptor["scope_root"], "published-dq.json")
                ),
            }
        )
    receipt = {
        "kind": "espn-run-success-receipt-v1",
        "schema_version": 1,
        "dag_id": dag_id,
        "run_id": run_id,
        "attempt": attempt,
        "mode": admission["mode"],
        "as_of": admission["as_of"],
        "logical_date": admission["logical_date"],
        "parent": admission["parent"],
        "scope_ids": index["scope_ids"],
        "registry_ref": admission["registry_ref"],
        "registry_signature": index["registry_signature"],
        "admission_ref": admission_ref,
        "plan_index_ref": plan_index_ref,
        "durable_manifest_ref": durable_ref,
        "published_dq_refs": sorted(dq_refs, key=lambda item: item["scope_id"]),
        "verdict_ref": verdict_ref,
        "health_ref": health_ref,
        "lease_release_ref": release_ref,
    }
    receipt["receipt_sha256"] = hashlib.sha256(_canonical_bytes(receipt)).hexdigest()
    success_ref = _write_payload(_join_uri(root, "run-success.json"), receipt)
    return {"success_receipt_ref": success_ref}


def fetch_discovery_catalog(**context) -> dict[str, str]:
    """Weekly bounded direct fetch of the ESPN soccer catalog JSON."""

    url = os.environ.get(
        "ESPN_DISCOVERY_CATALOG_URL",
        "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
    )
    PostgresEspnControlStore.from_env().migrate()
    raw_store = EspnRawStore.from_uri(_raw_store_uri())
    fetched = _http_client(raw_store, max_summary_events=1).fetch_json(
        url,
        EndpointType.CATALOG,
        force_refresh=True,
    )
    dag_id, run_id, logical_date = _run_identity(context)
    root = _join_uri(_artifact_root(), "discovery", _run_key(dag_id, run_id))
    payload = {
        "kind": "espn-discovery-raw-v1",
        "schema_version": 1,
        "captured_at": logical_date.isoformat(),
        "raw_uri": fetched.raw_uri,
        "raw_sha256": fetched.content_hash,
        "direct_bytes": fetched.direct_bytes,
        "proxy_bytes": fetched.proxy_bytes,
    }
    return {
        "discovery_raw_ref": _write_payload(
            _join_uri(root, "discovery-raw.json"), payload
        )
    }


def _discovery_snapshot(document: Mapping[str, Any], *, captured_at: str):
    """Normalize an enriched snapshot or dropdown/detail bundle."""

    from scrapers.espn.discovery import CatalogSnapshot, discover_catalog

    if isinstance(document.get("candidates"), list):
        parsed = CatalogSnapshot.from_dict(document)
        return CatalogSnapshot(
            captured_at=captured_at,
            candidates=parsed.candidates,
            source=parsed.source,
        )
    dropdown = document.get("dropdown", document)

    def nested_leagues(value):
        if isinstance(value, Mapping):
            if isinstance(value.get("leagues"), list):
                return value["leagues"]
            for child in value.values():
                found = nested_leagues(child)
                if found is not None:
                    return found
        elif isinstance(value, list):
            for child in value:
                found = nested_leagues(child)
                if found is not None:
                    return found
        return None

    if isinstance(dropdown, Mapping) and not any(
        isinstance(dropdown.get(key), list) for key in ("leagues", "groups")
    ):
        found_leagues = nested_leagues(dropdown)
        if found_leagues is not None:
            dropdown = {"leagues": found_leagues}
    if isinstance(dropdown, Mapping) and isinstance(dropdown.get("leagues"), list):
        dropdown = {
            "leagues": [
                {
                    **dict(row),
                    "name": row.get("name") or row.get("displayName"),
                }
                if isinstance(row, Mapping)
                else row
                for row in dropdown["leagues"]
            ]
        }
    explicit_details = document.get("details_by_slug", {})
    if not isinstance(explicit_details, Mapping):
        raise OperationsError("discovery details_by_slug must be an object")
    details = dict(explicit_details)
    leagues = dropdown.get("leagues", ()) if isinstance(dropdown, Mapping) else ()
    for row in leagues:
        if not isinstance(row, Mapping):
            continue
        slug = str(row.get("slug") or "").strip()
        season = row.get("season")
        if not slug or not isinstance(season, Mapping):
            continue
        details.setdefault(slug, row)
    return discover_catalog(
        dropdown,
        details_by_slug=details,
        captured_at=captured_at,
    )


def _promoted_registry_baseline(current, registry):
    """Acknowledge the first observed catalog but pin promoted identities."""

    from scrapers.espn.discovery import CatalogCandidate, CatalogSnapshot

    promoted_ids = set(registry.by_id)
    candidates = [
        item for item in current.candidates if item.espn_id not in promoted_ids
    ]
    for source_order, competition in enumerate(registry.competitions):
        edition = competition.current_edition
        candidates.append(
            CatalogCandidate(
                espn_id=competition.espn_id,
                slug=competition.slug,
                name=competition.name,
                group="promoted-registry",
                source_order=source_order,
                gender=competition.gender,
                age_class=competition.age_class,
                source_season_year=edition.source_season_year,
                edition_display_name=edition.display_name,
                start_date=edition.start_date.isoformat(),
                end_date=edition.end_date.isoformat(),
                capabilities=edition.capabilities,
                gender_evidence=competition.gender_evidence,
            )
        )
    return CatalogSnapshot(
        captured_at=current.captured_at,
        candidates=tuple(candidates),
        source="promoted ESPN registry plus acknowledged first observation",
    )


def write_reviewable_discovery_diff(
    *, discovery_raw_ref: Mapping[str, str], **context
) -> dict[str, str]:
    """Persist candidate/diff artifacts; this function has no promotion path."""

    from scrapers.espn.discovery import (
        CatalogSnapshot,
        diff_catalogs,
        quarantine_new_editions,
    )

    raw = _read_ref(discovery_raw_ref, kind="espn-discovery-raw-v1")
    body = EspnRawStore.from_uri(_raw_store_uri()).load_exact(
        raw["raw_uri"], raw["raw_sha256"]
    )
    try:
        dropdown = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("discovery catalog is not valid JSON") from exc
    current = _discovery_snapshot(dropdown, captured_at=raw["captured_at"])
    latest_state_uri = _join_uri(_artifact_root(), "discovery", "latest-state.json")
    previous_state = _optional_payload(latest_state_uri, kind="espn-discovery-state-v1")
    previous_uri = os.environ.get("ESPN_DISCOVERY_PREVIOUS_URI")
    registry_path = Path(
        os.environ.get(REGISTRY_ENV, "/opt/airflow/configs/espn/registry.yaml")
    )
    registry = load_registry(registry_path)
    if previous_uri:
        previous_document = json.loads(
            runner._read_artifact(previous_uri).decode("utf-8")
        )
        previous = CatalogSnapshot.from_dict(previous_document)
    elif previous_state is not None:
        previous = CatalogSnapshot.from_dict(_read_ref(previous_state["candidate_ref"]))
    else:
        previous = _promoted_registry_baseline(current, registry)
    diff = diff_catalogs(previous, current)
    dag_id, run_id, _ = _run_identity(context)
    root = _join_uri(_artifact_root(), "discovery", _run_key(dag_id, run_id))
    candidate_ref = _write_payload(
        _join_uri(root, "candidate.json"), json.loads(current.canonical_json())
    )
    candidate_identity = hashlib.sha256(
        _canonical_bytes([item.to_dict() for item in current.candidates])
    ).hexdigest()
    quarantined = sorted(quarantine_new_editions(current, registry))
    unresolved = bool(diff.changes)
    if (
        previous_state is not None
        and previous_state["candidate_identity"] == candidate_identity
        and previous_state["registry_signature"] == registry.signature()
    ):
        unresolved = bool(previous_state["unresolved_discovery_diffs"])
    dag_id, run_id, _ = _run_identity(context)
    observed_at = PostgresEspnControlStore.from_env().current_time()
    alert_scopes = (
        quarantined
        or [
            competition.scope_id(competition.current_edition)
            for competition in registry.competitions
            if competition.enabled
        ][:1]
    )
    alerts = []
    for alert_scope in alert_scopes:
        alerts.extend(
            evaluate_alerts(
                {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "attempt": _attempt(context),
                    "scope_id": alert_scope,
                    "subject_dag_id": None,
                    "subject_run_id": None,
                    "identity_kind": "discovery-candidate",
                    "identity_sha256": candidate_identity,
                    "state": "complete",
                    "last_complete_at": observed_at,
                    "direct_requests": 0,
                    "request_budget": MAX_SCOPE_SUMMARY_EVENTS,
                    "proxy_bytes": raw["proxy_bytes"],
                    "lease_conflict": False,
                    "unpromoted_current_season": alert_scope in quarantined,
                    "unresolved_discovery_diffs": unresolved,
                },
                observed_at=observed_at,
            )
        )
    review = {
        "kind": "espn-discovery-review-v1",
        "schema_version": 1,
        "candidate_ref": candidate_ref,
        "registry_signature": registry.signature(),
        "quarantined_scopes": quarantined,
        "changes": [json.loads(item.canonical_json()) for item in diff.changes],
        "change_count": len(diff.changes),
        "unresolved_discovery_diffs": unresolved,
        "alerts": alerts,
        "promotion_performed": False,
    }
    review_ref = _write_payload(_join_uri(root, "reviewable-diff.json"), review)
    state = {
        "kind": "espn-discovery-state-v1",
        "schema_version": 1,
        "candidate_ref": candidate_ref,
        "review_ref": review_ref,
        "candidate_identity": candidate_identity,
        "registry_signature": registry.signature(),
        "quarantined_scopes": quarantined,
        "unresolved_discovery_diffs": unresolved,
        "observed_at": observed_at.isoformat(),
    }
    _write_payload(latest_state_uri, state, immutable=False)
    if quarantined:
        from airflow.exceptions import AirflowException

        raise AirflowException(
            "ESPN discovery quarantined unpromoted current seasons: "
            + ", ".join(quarantined)
        )
    return {"discovery_review_ref": review_ref}


def check_36h_freshness_and_alerts(**context) -> dict[str, str]:
    """Network-free six-hour monitor over durable COMPLETE scope heads."""

    registry = load_registry(
        Path(os.environ.get(REGISTRY_ENV, "/opt/airflow/configs/espn/registry.yaml"))
    )
    scope_ids = _selected_scopes(registry, "daily", {})
    store = PostgresEspnControlStore.from_env()
    store.migrate()
    heads = store.read_scope_heads(scope_ids)
    dag_id, run_id, _ = _run_identity(context)
    now = store.current_time()
    alerts = []
    for scope_id in scope_ids:
        raw_head = heads.get(scope_id)
        head, state = _verified_complete_head(raw_head)
        rollover, unresolved = _latest_discovery_flags(scope_id)
        if raw_head is None:
            alert_identity = {
                "identity_kind": "monitor-subject",
                "identity_sha256": _subject_identity(
                    dag_id=dag_id,
                    run_id=run_id,
                    scope_id=scope_id,
                    registry_signature=registry.signature(),
                ),
            }
        else:
            alert_identity = {
                "identity_kind": "scope-head",
                "identity_sha256": _head_identity_sha256(raw_head),
            }
        alerts.extend(
            evaluate_alerts(
                {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "attempt": _attempt(context),
                    "scope_id": scope_id,
                    "subject_dag_id": raw_head.dag_id if raw_head else None,
                    "subject_run_id": raw_head.run_id if raw_head else None,
                    **alert_identity,
                    "state": state,
                    "last_complete_at": head.published_at if head else None,
                    "direct_requests": 0,
                    "request_budget": DEFAULT_MAX_REQUESTS,
                    "proxy_bytes": 0,
                    "lease_conflict": False,
                    "unpromoted_current_season": rollover,
                    "unresolved_discovery_diffs": unresolved,
                },
                observed_at=now,
            )
        )
    payload = {
        "kind": "espn-monitor-result-v1",
        "schema_version": 1,
        "run_id": run_id,
        "observed_at": now.isoformat(),
        "scope_ids": list(scope_ids),
        "alerts": alerts,
    }
    root = _join_uri(_artifact_root(), "monitor", _run_key(dag_id, run_id))
    result_ref = _write_payload(_join_uri(root, "monitor.json"), payload)
    if any(item["severity"] == "hard" for item in alerts):
        from airflow.exceptions import AirflowException

        raise AirflowException("ESPN freshness monitor found hard alerts")
    return {"monitor_ref": result_ref}
