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
    LeaseLost,
    OperationsError,
    PostgresEspnControlStore,
    RunManifestEvidence,
    MAX_SCOPE_SUMMARY_EVENTS,
    SUMMARY_BATCH_SIZE,
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
from scrapers.espn.selection import current_manifest_order_key
from scrapers.espn import runner
from scrapers.espn.transport import EspnHttpClient
from scrapers.espn.transport_contracts import (
    DEFAULT_MAX_ATTEMPTS,
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
LEASE_TTL = timedelta(hours=9)
DISCOVERY_DETAIL_BATCH_SIZE = 20
MAX_DISCOVERY_COMPETITIONS = 300
DISCOVERY_REQUESTS_PER_COMPETITION = 3
MAX_DISCOVERY_DETAIL_REQUESTS = (
    MAX_DISCOVERY_COMPETITIONS * DISCOVERY_REQUESTS_PER_COMPETITION
)
# Airflow 2.11's production ``core.max_map_length`` default is 1024.
MAX_AIRFLOW_DYNAMIC_MAP_ITEMS = 1024
MAX_MAPPING_DESCRIPTOR_URI_BYTES = 4096
MAX_INGEST_SCOPE_MAP_ITEMS = MAX_DISCOVERY_COMPETITIONS
MAX_SUMMARY_BATCH_MAP_ITEMS = min(
    MAX_AIRFLOW_DYNAMIC_MAP_ITEMS,
    MAX_INGEST_SCOPE_MAP_ITEMS
    * ((MAX_SCOPE_SUMMARY_EVENTS + SUMMARY_BATCH_SIZE - 1) // SUMMARY_BATCH_SIZE),
)
MAX_DISCOVERY_DETAIL_BATCH_MAP_ITEMS = (
    MAX_DISCOVERY_COMPETITIONS + DISCOVERY_DETAIL_BATCH_SIZE - 1
) // DISCOVERY_DETAIL_BATCH_SIZE
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


def select_mapping_descriptors(
    *,
    source: object,
    source_key: str,
    descriptor_key: str,
    max_items: int,
) -> list[dict[str, dict[str, str]]]:
    """Return one bounded map list through Airflow's ``return_value`` XCom."""

    source_field = _required(source_key, "mapping selector source_key")
    descriptor_field = _required(descriptor_key, "mapping selector descriptor_key")
    if (
        isinstance(max_items, bool)
        or not isinstance(max_items, int)
        or not 1 <= max_items <= MAX_SUMMARY_BATCH_MAP_ITEMS
    ):
        raise OperationsError("mapping selector max_items is outside its static bound")
    if not isinstance(source, dict):
        raise OperationsError("mapping selector source must be an object")
    if source_field not in source:
        raise OperationsError(f"mapping selector source lacks {source_field!r}")
    rows = source[source_field]
    if not isinstance(rows, list):
        raise OperationsError(f"mapping selector {source_field!r} must be a list")
    if len(rows) > max_items:
        raise OperationsError(
            f"mapping selector static bound exceeded: {len(rows)} > {max_items}"
        )
    selected: list[dict[str, dict[str, str]]] = []
    identities: set[tuple[str, str]] = set()
    for index, row in enumerate(rows):
        item_field = f"mapping selector {source_field}[{index}]"
        if not isinstance(row, dict) or set(row) != {descriptor_field}:
            raise OperationsError(
                f"{item_field} must contain only {descriptor_field!r}"
            )
        descriptor = row[descriptor_field]
        if not isinstance(descriptor, dict) or set(descriptor) != {
            "uri",
            "sha256",
        }:
            raise OperationsError(f"{item_field}.{descriptor_field} is not a URI/SHA")
        uri = _required(descriptor["uri"], f"{item_field}.{descriptor_field}.uri")
        if len(uri.encode("utf-8")) > MAX_MAPPING_DESCRIPTOR_URI_BYTES:
            raise OperationsError(
                f"{item_field}.{descriptor_field}.uri exceeds its static bound"
            )
        digest = _sha(descriptor["sha256"], f"{item_field}.{descriptor_field}.sha256")
        identity = (uri, digest)
        if identity in identities:
            raise OperationsError(f"{item_field} duplicates a URI/SHA descriptor")
        identities.add(identity)
        selected.append({descriptor_field: {"uri": uri, "sha256": digest}})
    return selected


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
    as_of: date,
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
    binding = {
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
    typed = runner._scope_binding(binding, scope.scope_id)
    requests = runner._scoreboard_requests(
        scope,
        typed,
        as_of=as_of,
        mode=mode,
    )
    if not requests and prior is not None:
        binding["active"] = False
    return binding


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
            try:
                for lease in leases:
                    store.assert_owned(lease, now=checked_at)
            except LeaseLost:
                leases = store.reclaim_owner_many(
                    leases,
                    owner_id=owner_id,
                    plan_signature=admission_signature,
                    now=checked_at,
                    ttl=LEASE_TTL,
                )
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
        "kind": "espn-lease-acquisition-v2",
        "schema_version": 2,
        "admission_ref": admission_ref,
        "owner_id": owner_id,
        "admission_signature": admission_signature,
        "scope_ids": sorted(item.scope_id for item in leases),
        "leases": [_lease_to_dict(item) for item in leases],
    }
    acquisition_key = hashlib.sha256(_canonical_bytes(acquisition)).hexdigest()
    acquisition_ref = _write_payload(
        _join_uri(
            admission["artifact_root"],
            "lease-acquisitions",
            acquisition_key,
            "lease-acquisition.json",
        ),
        acquisition,
    )
    return {"lease_acquisition_ref": acquisition_ref}


def build_signed_scope_plans(
    *, leasing: Mapping[str, Any], **_context
) -> dict[str, Any]:
    """Read heads under exact leases, then seal immutable scope plans."""

    acquisition_ref = leasing["lease_acquisition_ref"]
    acquisition = _read_ref(acquisition_ref)
    if acquisition.get("kind") not in {
        "espn-lease-acquisition-v1",
        "espn-lease-acquisition-v2",
    }:
        raise OperationsError("lease acquisition artifact kind is invalid")
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
    if acquisition.get("kind") == "espn-lease-acquisition-v2":
        acquired_leases = tuple(
            _lease_from_dict(item) for item in acquisition["leases"]
        )
        if tuple(_lease_to_dict(item) for item in leases) != tuple(
            _lease_to_dict(item) for item in acquired_leases
        ):
            raise OperationsError("lease bundle identity differs from acquisition")
    heads = store.read_scope_heads_owned(leases, now=datetime.now(UTC))
    for lease in leases:
        head = heads.get(lease.scope_id)
        hydrated = _hydrate_legacy_scope_head(
            store=store,
            lease=lease,
            head=head,
        )
        if hydrated is not None:
            heads[lease.scope_id] = hydrated
    scope_refs = []
    network_scope_ids = []
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
                as_of=plan_as_of,
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
            if requests:
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
                network_scope_ids.append(scope_id)
            elif binding["active"]:
                raise OperationsError(
                    "active initial scope produced no scoreboard request"
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
        "network_scope_ids": network_scope_ids,
        "expected_scoreboard_map_count": len(network_scope_ids),
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
    network_binding_refs = []
    for lease in bound_leases:
        scope_root, descriptor_ref = descriptors_by_scope[lease.scope_id]
        lease_binding = {
            "kind": "espn-scope-lease-binding-v1",
            "schema_version": 1,
            "scope_plan_ref": descriptor_ref,
            "lease": _lease_to_dict(lease),
        }
        binding_ref = _write_payload(
            _join_uri(
                scope_root,
                "lease-bindings",
                f"{lease.epoch}-{lease.token_sha256}",
                "lease-binding.json",
            ),
            lease_binding,
        )
        wrapped_binding = {"scope_binding_ref": binding_ref}
        binding_refs.append(wrapped_binding)
        if lease.scope_id in network_scope_ids:
            network_binding_refs.append(wrapped_binding)
    bound_bundle = {
        "kind": "espn-bound-lease-bundle-v1",
        "schema_version": 1,
        "owner_id": expected_owner,
        "scope_ids": sorted(item.scope_id for item in bound_leases),
        "leases": [_lease_to_dict(item) for item in bound_leases],
    }
    bound_key = hashlib.sha256(_canonical_bytes(bound_bundle)).hexdigest()
    bound_bundle_ref = _write_payload(
        _join_uri(
            admission["artifact_root"],
            "lease-bundles",
            bound_key,
            "bound-lease-bundle.json",
        ),
        bound_bundle,
    )
    return {
        "lease_acquisition_ref": acquisition_ref,
        "lease_bundle_ref": bound_bundle_ref,
        "plan_index_ref": index_ref,
        "scope_binding_refs": binding_refs,
        "network_scope_binding_refs": network_binding_refs,
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


def _head_to_dict(head: ScopeHead) -> dict[str, Any]:
    if head.completed_at is None:
        raise OperationsError("selected scope head lacks logical completion time")
    return {
        "dag_id": head.dag_id,
        "scope_id": head.scope_id,
        "generation_id": head.generation_id,
        "generation_signature": head.generation_signature,
        "manifest_sha256": head.manifest_sha256,
        "snapshot_uri": head.snapshot_uri,
        "snapshot_sha256": head.snapshot_sha256,
        "registry_signature": head.registry_signature,
        "plan_signature": head.plan_signature,
        "run_id": head.run_id,
        "published_at": head.published_at.isoformat(),
        "completed_at": head.completed_at.isoformat(),
    }


def _load_exact_scope_head_snapshot(head: ScopeHead):
    """Load and validate the immutable snapshot named by one control head."""

    generation = runner.load_scope_snapshot(
        head.snapshot_uri,
        artifact_sha256=head.snapshot_sha256,
        expected_scope_id=head.scope_id,
    )
    if (
        generation.generation_id,
        generation.generation_signature,
        generation.manifest_sha256,
        generation.run_id,
        generation.registry_signature,
        generation.plan_signature,
    ) != (
        head.generation_id,
        head.generation_signature,
        head.manifest_sha256,
        head.run_id,
        head.registry_signature,
        head.plan_signature,
    ):
        raise OperationsError("legacy scope head snapshot identity mismatch")
    return generation


def _hydrate_legacy_scope_head(
    *,
    store,
    lease: ScopeLease,
    head: ScopeHead | None,
    repository: EspnBronzeRepository | None = None,
) -> ScopeHead | None:
    """Hydrate a nullable rollout head only when real current views agree."""

    if head is None or head.completed_at is not None:
        return head
    generation = _load_exact_scope_head_snapshot(head)
    verifier = repository if repository is not None else EspnBronzeRepository()
    try:
        verifier.verify_current_scope_selection(generation)
    except Exception as exc:
        raise OperationsError(
            "legacy scope head differs from the real current views; repair the "
            "control head before retrying"
        ) from exc
    return store.hydrate_head_completed_at(
        lease,
        head,
        completed_at=generation.ingested_at,
        now=datetime.now(UTC),
    )


def _scope_head_matches_generation(
    head: ScopeHead | None,
    *,
    dag_id: str,
    generation,
    snapshot_ref: Mapping[str, str],
) -> bool:
    if head is None:
        return False
    return (
        head.dag_id,
        head.scope_id,
        head.generation_id,
        head.generation_signature,
        head.manifest_sha256,
        head.snapshot_uri,
        head.snapshot_sha256,
        head.registry_signature,
        head.plan_signature,
        head.run_id,
    ) == (
        dag_id,
        generation.plan.scope_id,
        generation.generation_id,
        generation.generation_signature,
        generation.manifest_sha256,
        snapshot_ref["uri"],
        snapshot_ref["sha256"],
        generation.registry_signature,
        generation.plan_signature,
        generation.run_id,
    )


def _prepare_scope_head_for_publication(
    *,
    store,
    lease: ScopeLease,
    head: ScopeHead | None,
    repository: EspnBronzeRepository,
    state: str,
    dag_id: str,
    generation,
    snapshot_ref: Mapping[str, str],
) -> tuple[ScopeHead | None, bool]:
    """Hydrate legacy order and detect a provable post-COMPLETE recovery."""

    candidate_is_current = False
    if state == "complete":
        try:
            repository.verify_current_scope_selection(generation)
        except Exception:
            pass
        else:
            candidate_is_current = True
    if head is None:
        return None, candidate_is_current
    if head.completed_at is not None:
        return head, candidate_is_current

    previous_generation = _load_exact_scope_head_snapshot(head)
    try:
        previous_complete_exists = repository.exact_complete_exists(previous_generation)
    except Exception as exc:
        raise OperationsError(
            "legacy scope head exact COMPLETE cannot be verified; repair the "
            "control head before retrying"
        ) from exc
    if not previous_complete_exists:
        raise OperationsError(
            "legacy scope head exact COMPLETE is missing; repair the control "
            "head before retrying"
        )
    previous_is_current = False
    if (
        _scope_head_matches_generation(
            head,
            dag_id=dag_id,
            generation=generation,
            snapshot_ref=snapshot_ref,
        )
        and candidate_is_current
    ):
        previous_is_current = True
    else:
        try:
            repository.verify_current_scope_selection(previous_generation)
        except Exception as exc:
            if not candidate_is_current:
                raise OperationsError(
                    "legacy scope head and publication candidate both differ from "
                    "the real current views; repair the control head before retrying"
                ) from exc
        else:
            previous_is_current = True
    if candidate_is_current and not previous_is_current:
        previous_order = {
            "completed_at": previous_generation.ingested_at,
            "generation_id": previous_generation.generation_id,
            "manifest_sha256": previous_generation.manifest_sha256,
        }
        candidate_order = {
            "completed_at": generation.ingested_at,
            "generation_id": generation.generation_id,
            "manifest_sha256": generation.manifest_sha256,
        }
        if current_manifest_order_key(candidate_order) <= current_manifest_order_key(
            previous_order
        ):
            raise OperationsError(
                "current COMPLETE recovery conflicts with canonical logical order"
            )
    if not previous_is_current and not candidate_is_current:
        raise OperationsError(
            "legacy scope head differs from the real current views; repair the "
            "control head before retrying"
        )
    hydrated = store.hydrate_head_completed_at(
        lease,
        head,
        completed_at=previous_generation.ingested_at,
        now=datetime.now(UTC),
    )
    return hydrated, candidate_is_current


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


def _heartbeat_scope_binding(ref: Mapping[str, str]) -> ScopeLease:
    """Extend one exact bound lease before a potentially long task boundary."""

    _, _, _, _, lease = _binding(ref)
    return PostgresEspnControlStore.from_env().renew(
        lease,
        now=datetime.now(UTC),
        ttl=LEASE_TTL,
    )


def _http_client(
    raw_store: EspnRawStore,
    *,
    max_summary_events: int,
    max_competitions: int = 1,
    max_requests: int = DEFAULT_MAX_REQUESTS,
) -> EspnHttpClient:
    control = PostgresEspnControlStore.from_env()
    budget = TaskBudget(
        max_competitions=max(1, max_competitions),
        max_summary_events=max(1, max_summary_events),
        max_requests=max_requests,
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

    _heartbeat_scope_binding(scope_binding_ref)
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
    *,
    scoreboard_phase_refs: Sequence[Mapping[str, Any]],
    scope_binding_refs: Sequence[Mapping[str, Any]],
    **_context,
) -> dict[str, Any]:
    """Offline enumerate wave-two IDs and persist each <=50 descriptor."""

    batch_refs = []
    scope_indexes = []
    phases_by_scope = {}
    for wrapped in scoreboard_phase_refs:
        phase_ref = wrapped["scoreboard_phase_ref"]
        phase = _read_ref(phase_ref, kind="espn-scoreboard-phase-result-v1")
        _, _, _, phase_scope, _ = _binding(phase["scope_binding_ref"])
        if phase_scope.scope_id in phases_by_scope:
            raise OperationsError("duplicate scoreboard phase scope")
        phases_by_scope[phase_scope.scope_id] = (phase_ref, phase)
    roots = set()
    seen_scopes = set()
    for wrapped in scope_binding_refs:
        scope_binding_ref = wrapped["scope_binding_ref"]
        _heartbeat_scope_binding(scope_binding_ref)
        binding, descriptor, loaded, scope, _ = _binding(scope_binding_ref)
        if scope.scope_id in seen_scopes:
            raise OperationsError("duplicate summary scope binding")
        seen_scopes.add(scope.scope_id)
        roots.add(descriptor["scope_root"].rsplit("/scopes/", 1)[0])
        typed_binding = loaded.bindings[scope.scope_id]
        phase_entry = phases_by_scope.pop(scope.scope_id, None)
        if not typed_binding.active:
            if phase_entry is not None or typed_binding.prior is None:
                raise OperationsError("inactive scope scoreboard phase contract drift")
            scope_indexes.append(
                {
                    "scope_id": scope.scope_id,
                    "scope_binding_ref": scope_binding_ref,
                    "scoreboard_phase_ref": None,
                    "summary_batch_refs": [],
                    "budget_used": 0,
                    "budget_limit": MAX_SCOPE_SUMMARY_EVENTS,
                    "budget_exhausted": False,
                    "pending_event_ids": [],
                }
            )
            continue
        if phase_entry is None:
            raise OperationsError("active scope is missing its scoreboard phase")
        phase_ref, phase = phase_entry
        if phase["scope_binding_ref"] != scope_binding_ref:
            raise OperationsError("scoreboard phase scope binding drift")
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
                "scope_binding_ref": scope_binding_ref,
                "scoreboard_phase_ref": phase_ref,
                "summary_batch_refs": scope_batch_refs,
                "budget_used": planned["budget_used"],
                "budget_limit": planned["budget_limit"],
                "budget_exhausted": planned["budget_exhausted"],
                "pending_event_ids": planned["pending_event_ids"],
            }
        )
    if phases_by_scope:
        raise OperationsError("unplanned extra scoreboard phase scope")
    if len(roots) != 1:
        raise OperationsError("summary scope bindings must share one run root")
    index_payload = {
        "kind": "espn-summary-wave-index-v1",
        "schema_version": 1,
        "expected_map_count": len(batch_refs),
        "expected_scoreboard_map_count": len(scoreboard_phase_refs),
        "scopes": sorted(scope_indexes, key=lambda item: item["scope_id"]),
    }
    index_ref = _write_payload(
        _join_uri(next(iter(roots)), "summary-wave-index.json"), index_payload
    )
    return {"summary_batch_refs": batch_refs, "summary_index_ref": index_ref}


def fetch_summary_batch(
    *, summary_batch_ref: Mapping[str, str], **_context
) -> dict[str, str]:
    """Mapped network wave two: at most 50 immutable Summary responses."""

    batch = _read_ref(summary_batch_ref, kind="espn-summary-batch-plan-v1")
    _heartbeat_scope_binding(batch["scope_binding_ref"])
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
        _heartbeat_scope_binding(scope_index["scope_binding_ref"])
        _, descriptor, loaded, scope, _ = _binding(scope_index["scope_binding_ref"])
        typed_binding = loaded.bindings[scope.scope_id]
        expected_batches = []
        checkpoints = []
        if not typed_binding.active:
            if (
                typed_binding.prior is None
                or scope_index["scoreboard_phase_ref"] is not None
                or scope_index["summary_batch_refs"]
            ):
                raise OperationsError("inactive scope raw reduction contract drift")
        else:
            scoreboard_phase = _read_ref(
                scope_index["scoreboard_phase_ref"],
                kind="espn-scoreboard-phase-result-v1",
            )
            expected_batches.append(scoreboard_phase["expected_batch"])
            checkpoints.append(_read_ref(scoreboard_phase["checkpoint_ref"]))
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
        if typed_binding.active:
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
        else:
            manifest = runner._seal_manifest(runner._manifest_base(loaded, (scope,)))
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
        _heartbeat_scope_binding(scope_binding_ref)
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
    _heartbeat_scope_binding(raw_phase["scope_binding_ref"])
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
    _heartbeat_scope_binding(phase["scope_binding_ref"])
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


def _validate_evidence_payload(
    payload: Mapping[str, Any],
    *,
    dag_id: str,
    loaded,
    scope,
    state: str,
    generation,
    lease: ScopeLease,
    recorded_at: datetime,
    publication_intent_ref: Mapping[str, str] | None,
) -> None:
    """Accept exact evidence from this or an earlier fenced retry epoch."""

    evidence_epoch = payload.get("lease_epoch")
    if (
        type(evidence_epoch) is not int
        or evidence_epoch <= 0
        or evidence_epoch > lease.epoch
    ):
        raise OperationsError("run evidence lease epoch is not recoverable")
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
    expected["lease_epoch"] = evidence_epoch
    if payload != expected:
        raise OperationsError("run evidence identity mismatch")


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
        complete_already_exists = complete_exists()
        if complete_already_exists:
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
    if not isinstance(payload, dict):
        raise OperationsError("publication intent schema mismatch")
    if set(payload) != expected_keys:
        raise OperationsError("publication intent schema mismatch")
    if (
        payload.get("kind") != "espn-publication-intent-v1"
        or {field: payload[field] for field in identity} != identity
    ):
        raise OperationsError("publication intent identity mismatch")
    if payload.get("schema_version") != 1:
        raise OperationsError("publication intent schema mismatch")
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
    _validate_evidence_payload(
        payload,
        dag_id=dag_id,
        loaded=loaded,
        scope=scope,
        state=state,
        generation=generation,
        lease=lease,
        recorded_at=recorded_at,
        publication_intent_ref=publication_intent_ref,
    )
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
    state = "noop" if dq["state"] == "noop" else "complete"
    store = PostgresEspnControlStore.from_env()
    renewed = store.renew(lease, now=datetime.now(UTC), ttl=LEASE_TTL)
    existing_head = store.read_scope_heads_owned((renewed,), now=datetime.now(UTC)).get(
        scope.scope_id
    )
    repository = EspnBronzeRepository()
    existing_head, allow_current_reconciliation = _prepare_scope_head_for_publication(
        store=store,
        lease=renewed,
        head=existing_head,
        repository=repository,
        state=state,
        dag_id=descriptor["dag_id"],
        generation=generation,
        snapshot_ref=dq["snapshot_ref"],
    )
    evidence_uri = _join_uri(descriptor["scope_root"], "run-evidence.json")
    with store.publication_guard(renewed, now=datetime.now(UTC)) as fence:
        selected_head = existing_head
        intent_payload = None
        intent_ref = None
        intent_created = False
        if state == "complete":
            if allow_current_reconciliation:
                repository.verify_current_scope_selection(generation)
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
            exact_existing_head = _scope_head_matches_generation(
                existing_head,
                dag_id=descriptor["dag_id"],
                generation=generation,
                snapshot_ref=dq["snapshot_ref"],
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
                completed_at=generation.ingested_at,
            )
            selected_head = fence.record_published(head, evidence)
            if allow_current_reconciliation and not _scope_head_matches_generation(
                selected_head,
                dag_id=descriptor["dag_id"],
                generation=generation,
                snapshot_ref=dq["snapshot_ref"],
            ):
                raise OperationsError(
                    "current COMPLETE recovery did not reconcile the control head"
                )
        else:
            fence.record_evidence(evidence)
        if selected_head is None:
            raise OperationsError("publication produced no selected COMPLETE head")
    result = {
        "kind": "espn-publication-result-v1",
        "schema_version": 1,
        "scope_binding_ref": dq["scope_binding_ref"],
        "state": state,
        "snapshot_ref": dq["snapshot_ref"],
        "evidence_ref": evidence_ref,
        "publication_intent_ref": intent_ref,
        "selected_head": _head_to_dict(selected_head),
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
        _validate_evidence_payload(
            evidence_payload,
            dag_id=index["dag_id"],
            loaded=loaded,
            scope=scope,
            state=publication["state"],
            generation=generation,
            lease=lease,
            recorded_at=datetime.fromisoformat(evidence_payload["recorded_at"]),
            publication_intent_ref=publication["publication_intent_ref"],
        )
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
    store = PostgresEspnControlStore.from_env()
    head = store.read_scope_heads((scope.scope_id,)).get(scope.scope_id)
    if head is None:
        raise OperationsError("published DQ lacks a selected COMPLETE head")
    selected_head = _head_to_dict(head)
    recorded_selection = publication.get("selected_head", selected_head)
    if recorded_selection != selected_head:
        raise OperationsError("published DQ control head changed from publication")
    selected_generation = runner.load_scope_snapshot(
        head.snapshot_uri,
        artifact_sha256=head.snapshot_sha256,
        expected_scope_id=scope.scope_id,
    )
    if (
        selected_generation.generation_id,
        selected_generation.generation_signature,
        selected_generation.manifest_sha256,
        selected_generation.run_id,
        selected_generation.registry_signature,
        selected_generation.plan_signature,
        selected_generation.ingested_at,
    ) != (
        head.generation_id,
        head.generation_signature,
        head.manifest_sha256,
        head.run_id,
        head.registry_signature,
        head.plan_signature,
        head.completed_at,
    ):
        raise OperationsError("selected scope head snapshot identity mismatch")
    repository = EspnBronzeRepository()
    if publication["state"] == "complete":
        _assert_generation_binding(
            generation=generation, loaded=loaded, scope=scope, state="staged"
        )
        repository.verify_published_scope(generation)
    elif publication["state"] == "noop":
        _assert_generation_binding(
            generation=generation, loaded=loaded, scope=scope, state="noop"
        )
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
        repository.verify_published_scope(generation)
    else:
        raise OperationsError("publication state is invalid")
    report = repository.verify_published_scope(selected_generation)
    repository.verify_current_scope_selection(selected_generation)
    if not report.passed:
        raise OperationsError("published DQ failed: " + "; ".join(report.failures))
    evidence = _read_ref(
        publication["evidence_ref"], kind="espn-run-manifest-evidence-v1"
    )
    _validate_evidence_payload(
        evidence,
        dag_id=descriptor["dag_id"],
        loaded=loaded,
        scope=scope,
        state=publication["state"],
        generation=generation,
        lease=lease,
        recorded_at=datetime.fromisoformat(evidence["recorded_at"]),
        publication_intent_ref=publication["publication_intent_ref"],
    )
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
        "current_selection": selected_head,
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
            "offline_parse",
            "staging_dq",
            "publish_scopes",
            "published_dq",
        ):
            if task_id in expected_counts:
                expected_counts[task_id] = scope_count
        if "fetch_scoreboard_batches" in expected_counts:
            expected_counts["fetch_scoreboard_batches"] = index.get(
                "expected_scoreboard_map_count", scope_count
            )
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
            publication = publications_by_scope[scope_id][1]
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
            if dq.get("current_selection") != publication.get("selected_head"):
                raise OperationsError(
                    "terminal current selection differs from publication"
                )
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
    if hasattr(store, "migrate"):
        store.migrate()
    dag_id, run_id, _ = _run_identity(context)
    owner_id = f"{dag_id}/{run_id}/{_attempt(context)}"
    acquisition = None
    if lease_acquisition_ref is not None:
        acquisition = _read_ref(lease_acquisition_ref)
        if acquisition.get("kind") not in {
            "espn-lease-acquisition-v1",
            "espn-lease-acquisition-v2",
            "espn-bound-lease-bundle-v1",
        }:
            raise OperationsError("lease cleanup artifact kind is invalid")
        if acquisition["owner_id"] != owner_id:
            raise OperationsError("lease cleanup owner identity mismatch")
        if "leases" in acquisition:
            issued_leases = tuple(
                _lease_from_dict(item) for item in acquisition["leases"]
            )
            if {item.scope_id for item in issued_leases} != set(
                acquisition["scope_ids"]
            ):
                raise OperationsError("lease cleanup exact scope set mismatch")
            if acquisition["kind"] == "espn-lease-acquisition-v2":
                current_by_scope = {
                    item.scope_id: item for item in store.read_owner_leases(owner_id)
                }
                leases = tuple(
                    current_by_scope.get(issued.scope_id)
                    if current_by_scope.get(issued.scope_id) is not None
                    and current_by_scope[issued.scope_id].epoch == issued.epoch
                    else issued
                    for issued in issued_leases
                )
            else:
                leases = issued_leases
        else:
            leases = store.read_owner_leases(owner_id)
            if {item.scope_id for item in leases} != set(acquisition["scope_ids"]):
                raise OperationsError("current owner leases differ from acquisition")
    else:
        leases = store.read_owner_leases(owner_id)
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

    dag_id, run_id, logical_date = _run_identity(context)
    root = _join_uri(_artifact_root(), "discovery", _run_key(dag_id, run_id))
    artifact_uri = _join_uri(root, "discovery-raw.json")
    raw_store = EspnRawStore.from_uri(_raw_store_uri())
    existing = _optional_payload(artifact_uri, kind="espn-discovery-raw-v1")
    if existing is not None:
        if (
            set(existing)
            != {
                "kind",
                "schema_version",
                "captured_at",
                "raw_uri",
                "raw_sha256",
                "direct_bytes",
                "proxy_bytes",
            }
            or existing.get("schema_version") != 1
            or existing.get("captured_at") != logical_date.isoformat()
        ):
            raise OperationsError("existing discovery catalog checkpoint mismatch")
        raw_store.load_exact(existing["raw_uri"], existing["raw_sha256"])
        return {"discovery_raw_ref": _ref_for_uri(artifact_uri)}
    url = os.environ.get(
        "ESPN_DISCOVERY_CATALOG_URL",
        "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
    )
    PostgresEspnControlStore.from_env().migrate()
    fetched = _http_client(raw_store, max_summary_events=1).fetch_json(
        url,
        EndpointType.CATALOG,
        force_refresh=True,
    )
    payload = {
        "kind": "espn-discovery-raw-v1",
        "schema_version": 1,
        "captured_at": logical_date.isoformat(),
        "raw_uri": fetched.raw_uri,
        "raw_sha256": fetched.content_hash,
        "direct_bytes": fetched.direct_bytes,
        "proxy_bytes": fetched.proxy_bytes,
    }
    return {"discovery_raw_ref": _write_payload(artifact_uri, payload)}


def _normalize_discovery_dropdown(document: object) -> Mapping[str, Any]:
    """Expose one supported dropdown shape to both planning and review."""

    if not isinstance(document, Mapping):
        raise OperationsError("discovery catalog must be an object")

    def find(value: object):
        if isinstance(value, Mapping):
            for key in ("leagues", "groups"):
                if isinstance(value.get(key), list):
                    return key, value[key]
            if isinstance(value.get("leagueTeams"), Mapping):
                return "leagueTeams", value["leagueTeams"]
            for child in value.values():
                found = find(child)
                if found is not None:
                    return found
        elif isinstance(value, list):
            for child in value:
                found = find(child)
                if found is not None:
                    return found
        return None

    found = find(document)
    if found is None:
        raise OperationsError("discovery catalog contains no supported dropdown")
    key, rows = found
    if key == "leagueTeams":
        return rows
    if key == "groups":
        return {"groups": rows}
    return {
        "leagues": [
            {
                **dict(row),
                "name": row.get("name") or row.get("displayName"),
            }
            if isinstance(row, Mapping)
            else row
            for row in rows
        ]
    }


def plan_discovery_detail_batches(
    *, discovery_raw_ref: Mapping[str, str], **context
) -> dict[str, Any]:
    """Offline-plan deterministic unique competition detail batches of at most 20."""

    from scrapers.espn.discovery import parse_soccer_dropdown

    raw = _read_ref(discovery_raw_ref, kind="espn-discovery-raw-v1")
    body = EspnRawStore.from_uri(_raw_store_uri()).load_exact(
        raw["raw_uri"], raw["raw_sha256"]
    )
    try:
        dropdown = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("discovery catalog is not valid JSON") from exc
    dropdown = _normalize_discovery_dropdown(dropdown)
    unique = {}
    slugs = {}
    for candidate in parse_soccer_dropdown(dropdown):
        identity = (candidate.espn_id, candidate.slug, candidate.name)
        key = (
            ("id", candidate.espn_id)
            if candidate.espn_id is not None
            else ("slug", candidate.slug)
        )
        if key in unique and unique[key] != identity:
            raise OperationsError("discovery dropdown has conflicting identities")
        if candidate.slug in slugs and slugs[candidate.slug] != identity:
            raise OperationsError("discovery dropdown has conflicting slugs")
        unique.setdefault(key, identity)
        slugs.setdefault(candidate.slug, identity)
    competitions = list(unique.values())
    if not competitions:
        raise OperationsError("discovery catalog contains no competitions")
    if len(competitions) > MAX_DISCOVERY_COMPETITIONS:
        raise OperationsError(
            "discovery competition cap exceeded: "
            f"{len(competitions)} > {MAX_DISCOVERY_COMPETITIONS}"
        )
    dag_id, run_id, _ = _run_identity(context)
    root = _join_uri(_artifact_root(), "discovery", _run_key(dag_id, run_id))
    batch_refs = []
    batch_ids = []
    for offset in range(0, len(competitions), DISCOVERY_DETAIL_BATCH_SIZE):
        rows = competitions[offset : offset + DISCOVERY_DETAIL_BATCH_SIZE]
        batch_id = hashlib.sha256(
            _canonical_bytes(
                {
                    "kind": "espn-discovery-detail-batch-id-v1",
                    "discovery_raw_ref": discovery_raw_ref,
                    "competitions": rows,
                }
            )
        ).hexdigest()
        batch = {
            "kind": "espn-discovery-detail-batch-v1",
            "schema_version": 1,
            "batch_id": batch_id,
            "discovery_raw_ref": discovery_raw_ref,
            "competitions": [
                {"espn_id": espn_id, "slug": slug, "name": name}
                for espn_id, slug, name in rows
            ],
        }
        batch_ref = _write_payload(
            _join_uri(root, "details", f"batch-{batch_id}.json"), batch
        )
        batch_refs.append({"discovery_detail_batch_ref": batch_ref})
        batch_ids.append(batch_id)
    index = {
        "kind": "espn-discovery-detail-index-v1",
        "schema_version": 1,
        "discovery_raw_ref": discovery_raw_ref,
        "captured_at": raw["captured_at"],
        "batch_ids": batch_ids,
        "batch_refs": [item["discovery_detail_batch_ref"] for item in batch_refs],
        "competition_count": len(competitions),
        "competition_cap": MAX_DISCOVERY_COMPETITIONS,
        "detail_request_cap": MAX_DISCOVERY_DETAIL_REQUESTS,
    }
    index_ref = _write_payload(_join_uri(root, "discovery-detail-index.json"), index)
    return {
        "discovery_detail_batch_refs": batch_refs,
        "discovery_detail_index_ref": index_ref,
    }


def _validate_discovery_detail_record(
    detail: object,
    *,
    expected: Mapping[str, Any],
    raw_store: EspnRawStore,
) -> dict[str, Any]:
    required_keys = {
        "espn_id",
        "slug",
        "name",
        "metadata_raw_uri",
        "metadata_raw_sha256",
        "scoreboard_raw_uri",
        "scoreboard_raw_sha256",
        "summary_raw_uri",
        "summary_raw_sha256",
        "summary_event_id",
        "request_count",
        "direct_bytes",
        "proxy_bytes",
    }
    if not isinstance(detail, dict) or set(detail) != required_keys:
        raise OperationsError("discovery detail checkpoint schema mismatch")
    if tuple(detail[key] for key in ("espn_id", "slug", "name")) != tuple(
        expected[key] for key in ("espn_id", "slug", "name")
    ):
        raise OperationsError("discovery detail checkpoint identity mismatch")
    for prefix in ("metadata", "scoreboard"):
        uri = _required(detail[f"{prefix}_raw_uri"], f"discovery {prefix} raw uri")
        digest = _sha(
            detail[f"{prefix}_raw_sha256"],
            f"discovery {prefix} raw sha256",
        )
        raw_store.load_exact(uri, digest)
    summary_uri = detail["summary_raw_uri"]
    summary_sha256 = detail["summary_raw_sha256"]
    summary_event_id = detail["summary_event_id"]
    if summary_uri is None:
        if summary_sha256 is not None or summary_event_id is not None:
            raise OperationsError("discovery Summary checkpoint is incomplete")
    else:
        uri = _required(summary_uri, "discovery Summary raw uri")
        digest = _sha(summary_sha256, "discovery Summary raw sha256")
        if (
            not isinstance(summary_event_id, str)
            or not summary_event_id.isdecimal()
            or summary_event_id.startswith("0")
        ):
            raise OperationsError("discovery Summary checkpoint event id is invalid")
        raw_store.load_exact(uri, digest)
    expected_requests = 2 + int(summary_uri is not None)
    if detail["request_count"] != expected_requests:
        raise OperationsError("discovery detail checkpoint request count mismatch")
    for field in ("direct_bytes", "proxy_bytes"):
        if type(detail[field]) is not int or detail[field] < 0:
            raise OperationsError(
                f"discovery detail checkpoint {field} must be non-negative"
            )
    return detail


def _validate_discovery_detail_phase(
    phase: object,
    *,
    batch: Mapping[str, Any],
    batch_ref: Mapping[str, str],
    raw_store: EspnRawStore,
) -> dict[str, Any]:
    if (
        not isinstance(phase, dict)
        or set(phase)
        != {
            "kind",
            "schema_version",
            "batch_id",
            "discovery_detail_batch_ref",
            "details",
        }
        or phase.get("kind") != "espn-discovery-detail-phase-v1"
        or phase.get("schema_version") != 1
        or phase.get("batch_id") != batch["batch_id"]
        or phase.get("discovery_detail_batch_ref") != batch_ref
        or not isinstance(phase.get("details"), list)
        or len(phase["details"]) != len(batch["competitions"])
    ):
        raise OperationsError("discovery detail phase checkpoint mismatch")
    expected_by_identity = {
        (item["espn_id"], item["slug"], item["name"]): item
        for item in batch["competitions"]
    }
    seen = set()
    for detail in phase["details"]:
        if not isinstance(detail, Mapping):
            raise OperationsError("discovery detail phase row must be an object")
        identity = tuple(detail.get(key) for key in ("espn_id", "slug", "name"))
        if identity in seen or identity not in expected_by_identity:
            raise OperationsError("discovery detail phase coverage mismatch")
        seen.add(identity)
        _validate_discovery_detail_record(
            detail,
            expected=expected_by_identity[identity],
            raw_store=raw_store,
        )
    if seen != set(expected_by_identity):
        raise OperationsError("discovery detail phase coverage mismatch")
    return phase


def fetch_discovery_detail_batch(
    *, discovery_detail_batch_ref: Mapping[str, str], **_context
) -> dict[str, str]:
    """Fetch bounded identity, schedule and Summary evidence for one batch."""

    batch = _read_ref(discovery_detail_batch_ref, kind="espn-discovery-detail-batch-v1")
    competitions = batch["competitions"]
    if not 1 <= len(competitions) <= DISCOVERY_DETAIL_BATCH_SIZE:
        raise OperationsError(
            "discovery detail batch exceeds the static run-level batch size"
        )
    raw_store = EspnRawStore.from_uri(_raw_store_uri())
    phase_uri = discovery_detail_batch_ref["uri"] + ".phase.json"
    existing_phase = _optional_payload(phase_uri, kind="espn-discovery-detail-phase-v1")
    if existing_phase is not None:
        _validate_discovery_detail_phase(
            existing_phase,
            batch=batch,
            batch_ref=discovery_detail_batch_ref,
            raw_store=raw_store,
        )
        return {"discovery_detail_phase_ref": _ref_for_uri(phase_uri)}
    client = _http_client(
        raw_store,
        max_summary_events=len(competitions),
        max_competitions=len(competitions),
        max_requests=(
            DISCOVERY_REQUESTS_PER_COMPETITION
            * len(competitions)
            * DEFAULT_MAX_ATTEMPTS
        ),
    )
    metadata_template = os.environ.get(
        "ESPN_DISCOVERY_METADATA_URL_TEMPLATE",
        "https://sports.core.api.espn.com/v2/sports/soccer/leagues/{slug}",
    )
    scoreboard_template = os.environ.get(
        "ESPN_DISCOVERY_DETAIL_URL_TEMPLATE",
        "https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard",
    )
    summary_template = os.environ.get(
        "ESPN_DISCOVERY_SUMMARY_URL_TEMPLATE",
        "https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/summary",
    )
    details = []
    for item in competitions:
        item_key = hashlib.sha256(
            _canonical_bytes(
                {
                    "kind": "espn-discovery-detail-item-id-v1",
                    "batch_id": batch["batch_id"],
                    "identity": item,
                }
            )
        ).hexdigest()
        item_uri = discovery_detail_batch_ref["uri"] + f".item-{item_key}.json"
        checkpoint = _optional_payload(item_uri, kind="espn-discovery-detail-item-v1")
        if checkpoint is not None:
            if (
                set(checkpoint)
                != {
                    "kind",
                    "schema_version",
                    "batch_id",
                    "discovery_detail_batch_ref",
                    "detail",
                }
                or checkpoint.get("schema_version") != 1
                or checkpoint.get("batch_id") != batch["batch_id"]
                or checkpoint.get("discovery_detail_batch_ref")
                != discovery_detail_batch_ref
            ):
                raise OperationsError("discovery detail item checkpoint mismatch")
            details.append(
                _validate_discovery_detail_record(
                    checkpoint["detail"],
                    expected=item,
                    raw_store=raw_store,
                )
            )
            continue
        identity = item["espn_id"] or f"slug:{item['slug']}"
        metadata = client.fetch_json(
            metadata_template.format(slug=item["slug"], espn_id=item["espn_id"]),
            EndpointType.CATALOG,
            {"lang": "en", "region": "us"},
            competition_id=identity,
            force_refresh=True,
        )
        scoreboard = client.fetch_json(
            scoreboard_template.format(slug=item["slug"], espn_id=item["espn_id"]),
            EndpointType.SCOREBOARD,
            {"limit": 1},
            competition_id=identity,
            force_refresh=True,
        )
        resolved_espn_id = _discovery_metadata_id(
            metadata.json_data,
            espn_id=item["espn_id"],
            slug=item["slug"],
        )
        event_id = _first_discovery_event_id(
            scoreboard.json_data,
            espn_id=resolved_espn_id,
            slug=item["slug"],
        )
        summary = None
        if event_id is not None:
            summary = client.fetch_json(
                summary_template.format(
                    slug=item["slug"], espn_id=item["espn_id"], event_id=event_id
                ),
                EndpointType.SUMMARY,
                {"event": event_id},
                competition_id=identity,
                event_id=event_id,
                force_refresh=True,
            )
        fetched_items = (metadata, scoreboard) + (() if summary is None else (summary,))
        detail = {
            **item,
            "metadata_raw_uri": metadata.raw_uri,
            "metadata_raw_sha256": metadata.content_hash,
            "scoreboard_raw_uri": scoreboard.raw_uri,
            "scoreboard_raw_sha256": scoreboard.content_hash,
            "summary_raw_uri": None if summary is None else summary.raw_uri,
            "summary_raw_sha256": None if summary is None else summary.content_hash,
            "summary_event_id": event_id,
            "request_count": len(fetched_items),
            "direct_bytes": sum(item.direct_bytes for item in fetched_items),
            "proxy_bytes": sum(item.proxy_bytes for item in fetched_items),
        }
        item_checkpoint = {
            "kind": "espn-discovery-detail-item-v1",
            "schema_version": 1,
            "batch_id": batch["batch_id"],
            "discovery_detail_batch_ref": discovery_detail_batch_ref,
            "detail": detail,
        }
        _write_payload(item_uri, item_checkpoint)
        details.append(detail)
    phase = {
        "kind": "espn-discovery-detail-phase-v1",
        "schema_version": 1,
        "batch_id": batch["batch_id"],
        "discovery_detail_batch_ref": discovery_detail_batch_ref,
        "details": details,
    }
    phase_ref = _write_payload(phase_uri, phase)
    return {"discovery_detail_phase_ref": phase_ref}


def _first_discovery_event_id(
    document: object, *, espn_id: int | None, slug: str
) -> str | None:
    """Validate one exact league response, then select its first event id."""

    if not isinstance(document, Mapping):
        raise OperationsError("discovery scoreboard must be an object")
    leagues = document.get("leagues")
    scoreboard_id = None
    if (
        isinstance(leagues, list)
        and len(leagues) == 1
        and isinstance(leagues[0], Mapping)
    ):
        scoreboard_id = _discovery_optional_id(
            leagues[0].get("id"), label="scoreboard league id"
        )
    if (
        not isinstance(leagues, list)
        or len(leagues) != 1
        or not isinstance(leagues[0], Mapping)
        or str(leagues[0].get("slug") or "").strip() != slug
        or (
            espn_id is not None
            and scoreboard_id is not None
            and scoreboard_id != espn_id
        )
    ):
        raise OperationsError(f"discovery scoreboard must own exactly {espn_id}:{slug}")
    events = document.get("events")
    if not isinstance(events, list):
        raise OperationsError("discovery scoreboard events must be a list")
    if not events:
        return None
    event = events[0]
    if not isinstance(event, Mapping):
        raise OperationsError("discovery scoreboard event must be an object")
    event_id = str(event.get("id") or "").strip()
    if not event_id or not event_id.isdecimal() or event_id.startswith("0"):
        raise OperationsError("discovery scoreboard event id is invalid")
    return event_id


def _discovery_optional_id(value: object, *, label: str) -> int | None:
    if value is None:
        return None
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
        return int(value)
    raise OperationsError(f"discovery {label} must be a positive integer or null")


def _discovery_metadata_id(
    document: object, *, espn_id: int | None, slug: str
) -> int | None:
    """Resolve and bind the core identity, including dropdown rows without IDs."""

    if not isinstance(document, Mapping):
        raise OperationsError("discovery metadata must be an object")
    if str(document.get("slug") or "").strip() != slug:
        raise OperationsError(
            f"discovery metadata must identify exactly {espn_id}:{slug}"
        )
    metadata_id = _discovery_optional_id(document.get("id"), label="metadata id")
    if espn_id is not None and metadata_id is not None and metadata_id != espn_id:
        raise OperationsError(
            f"discovery metadata must identify exactly {espn_id}:{slug}"
        )
    return metadata_id if metadata_id is not None else espn_id


def _discovery_parser_capabilities(
    *,
    scoreboard_document: Mapping[str, Any],
    summary_document: Mapping[str, Any] | None,
    resolved_espn_id: int | None,
    slug: str,
    name: str,
    season: Mapping[str, Any],
    scoreboard_event_id: str | None,
) -> dict[str, str]:
    """Use the production parsers before emitting registry-admissible evidence."""

    unknown = {"schedule": "unknown", "lineup": "unknown", "matchsheet": "unknown"}
    if resolved_espn_id is None:
        return unknown
    raw_year = season.get("year")
    if type(raw_year) is int and raw_year >= 1800:
        source_year = raw_year
    elif isinstance(raw_year, str) and re.fullmatch(r"[1-9][0-9]{3}", raw_year):
        source_year = int(raw_year)
    else:
        return unknown

    from scrapers.espn.models import (
        AgeClass,
        CapabilityState,
        Competition,
        Edition,
        EntityCapabilities,
        Gender,
    )
    from scrapers.espn.parser_common import EspnParseError
    from scrapers.espn.parser_contracts import EntityParseState
    from scrapers.espn.schedule_parser import parse_scoreboards
    from scrapers.espn.summary_parser import parse_summary

    edition = Edition(
        source_season_year=source_year,
        display_name=str(season.get("displayName") or name),
        start_date=date(1800, 1, 1),
        end_date=date(9999, 12, 31),
        current=True,
        capabilities=EntityCapabilities(
            CapabilityState.PROVEN,
            CapabilityState.PARTIAL,
            CapabilityState.PARTIAL,
        ),
    )
    competition = Competition(
        espn_id=resolved_espn_id,
        slug=slug,
        name=name,
        gender=Gender.UNKNOWN,
        age_class=AgeClass.UNKNOWN,
        enabled=False,
        editions=(edition,),
    )
    try:
        schedule = parse_scoreboards(
            _canonical_bytes(scoreboard_document),
            competition=competition,
            edition=edition,
            query_start=date(1800, 1, 1),
            query_end=date(9999, 12, 31),
        )
    except (EspnParseError, TypeError, ValueError):
        return unknown
    result = {**unknown, "schedule": "proven"}
    if summary_document is None or scoreboard_event_id is None:
        return result
    event = next(
        (item for item in schedule if item.event_id == int(scoreboard_event_id)),
        None,
    )
    if event is None:
        return result
    try:
        summary = parse_summary(
            _canonical_bytes(summary_document),
            competition=competition,
            edition=edition,
            event=event,
        )
    except (EspnParseError, TypeError, ValueError):
        return result
    if summary.lineup_state is EntityParseState.CAPTURED and summary.lineup:
        result["lineup"] = "partial"
    if summary.matchsheet_state is EntityParseState.CAPTURED and summary.matchsheet:
        result["matchsheet"] = "partial"
    return result


def _competition_detail_document(
    metadata_document: Mapping[str, Any],
    scoreboard_document: Mapping[str, Any],
    summary_document: Mapping[str, Any] | None,
    *,
    espn_id: int | None,
    slug: str,
    name: str,
) -> Mapping[str, Any]:
    """Normalize only fields proven by exact saved ESPN responses."""

    resolved_espn_id = _discovery_metadata_id(
        metadata_document,
        espn_id=espn_id,
        slug=slug,
    )
    detail = dict(metadata_document)
    detail["slug"] = slug
    detail["name"] = _required(name, "discovery competition name")
    if resolved_espn_id is not None:
        detail["id"] = resolved_espn_id
    season = detail.get("season") or {}
    if not isinstance(season, Mapping):
        raise OperationsError("discovery detail season must be an object")
    normalized_season = dict(season)
    event_years = {
        event["season"]["year"]
        for event in scoreboard_document.get("events", ())
        if isinstance(event, Mapping)
        and isinstance(event.get("season"), Mapping)
        and event["season"].get("year") is not None
    }
    if normalized_season.get("year") is None and len(event_years) == 1:
        normalized_season["year"] = next(iter(event_years))
    if normalized_season:
        detail["season"] = normalized_season
    gender = str(detail.get("gender") or "").strip()
    if gender:
        detail["genderEvidence"] = [f"core-detail.gender={gender.upper()}"]

    scoreboard_event_id = _first_discovery_event_id(
        scoreboard_document,
        espn_id=resolved_espn_id,
        slug=slug,
    )
    detail["capabilities"] = _discovery_parser_capabilities(
        scoreboard_document=scoreboard_document,
        summary_document=summary_document,
        resolved_espn_id=resolved_espn_id,
        slug=slug,
        name=name,
        season=normalized_season,
        scoreboard_event_id=scoreboard_event_id,
    )
    return detail


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
    dropdown = _normalize_discovery_dropdown(document.get("dropdown", document))
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


def _discovery_artifact_ref(value: object, *, label: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"uri", "sha256"}:
        raise OperationsError(f"{label} must contain only uri and sha256")
    return {
        "uri": _required(value["uri"], f"{label} uri"),
        "sha256": _sha(value["sha256"], f"{label} sha256"),
    }


def _discovery_phase_input_refs(
    values: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    refs = []
    for wrapped in values:
        if not isinstance(wrapped, Mapping) or set(wrapped) != {
            "discovery_detail_phase_ref"
        }:
            raise OperationsError("discovery detail phase wrapper mismatch")
        refs.append(
            _discovery_artifact_ref(
                wrapped["discovery_detail_phase_ref"],
                label="discovery detail phase reference",
            )
        )
    refs.sort(key=lambda ref: (ref["uri"], ref["sha256"]))
    if len({(ref["uri"], ref["sha256"]) for ref in refs}) != len(refs):
        raise OperationsError("duplicate discovery detail phase reference")
    return refs


def _discovery_review_state(
    review: Mapping[str, Any], *, review_ref: Mapping[str, str]
) -> dict[str, Any]:
    return {
        "kind": "espn-discovery-state-v1",
        "schema_version": 1,
        "candidate_ref": review["candidate_ref"],
        "review_ref": dict(review_ref),
        "candidate_identity": review["candidate_identity"],
        "registry_signature": review["registry_signature"],
        "quarantined_scopes": review["quarantined_scopes"],
        "unresolved_discovery_diffs": review["unresolved_discovery_diffs"],
        "observed_at": review["observed_at"],
    }


def _discovery_observed_at(value: object, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_required(value, label))
    except ValueError as exc:
        raise OperationsError(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise OperationsError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _validate_discovery_review_checkpoint(
    review: object,
    *,
    discovery_detail_index_ref: Mapping[str, str],
    discovery_detail_phase_refs: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    required_keys = {
        "kind",
        "schema_version",
        "discovery_detail_index_ref",
        "discovery_detail_phase_refs",
        "candidate_ref",
        "candidate_identity",
        "registry_signature",
        "quarantined_scopes",
        "changes",
        "change_count",
        "unresolved_discovery_diffs",
        "alerts",
        "promotion_performed",
        "observed_at",
    }
    if (
        not isinstance(review, dict)
        or set(review) != required_keys
        or review.get("kind") != "espn-discovery-review-v1"
        or review.get("schema_version") != 2
        or review.get("discovery_detail_index_ref") != discovery_detail_index_ref
        or review.get("discovery_detail_phase_refs")
        != list(discovery_detail_phase_refs)
        or review.get("promotion_performed") is not False
        or type(review.get("unresolved_discovery_diffs")) is not bool
        or not isinstance(review.get("quarantined_scopes"), list)
        or not isinstance(review.get("changes"), list)
        or type(review.get("change_count")) is not int
        or review["change_count"] != len(review["changes"])
        or not isinstance(review.get("alerts"), list)
    ):
        raise OperationsError("existing discovery review checkpoint mismatch")
    candidate_ref = _discovery_artifact_ref(
        review["candidate_ref"], label="discovery candidate reference"
    )
    candidate = _read_ref(candidate_ref)
    candidates = candidate.get("candidates")
    if not isinstance(candidates, list):
        raise OperationsError("existing discovery candidate checkpoint mismatch")
    candidate_identity = hashlib.sha256(_canonical_bytes(candidates)).hexdigest()
    if review.get("candidate_identity") != candidate_identity:
        raise OperationsError("existing discovery candidate identity mismatch")
    _sha(review.get("registry_signature"), "discovery registry signature")
    _discovery_observed_at(review.get("observed_at"), label="discovery observed_at")
    return review


def _publish_latest_discovery_state(
    latest_state_uri: str, state: Mapping[str, Any]
) -> None:
    incoming_time = _discovery_observed_at(
        state.get("observed_at"), label="discovery state observed_at"
    )
    existing = _optional_payload(latest_state_uri, kind="espn-discovery-state-v1")
    if existing is not None:
        existing_time = _discovery_observed_at(
            existing.get("observed_at"), label="existing discovery state observed_at"
        )
        if existing_time > incoming_time:
            return
        if existing_time == incoming_time:
            if existing == state:
                return
            raise OperationsError(
                "discovery latest state has a different identity at the same time"
            )
    _write_payload(latest_state_uri, state, immutable=False)


def write_reviewable_discovery_diff(
    *,
    discovery_detail_index_ref: Mapping[str, str],
    discovery_detail_phase_refs: Sequence[Mapping[str, Any]],
    **context,
) -> dict[str, str]:
    """Persist candidate/diff artifacts; this function has no promotion path."""

    from scrapers.espn.discovery import (
        CatalogSnapshot,
        diff_catalogs,
        quarantine_new_editions,
    )

    dag_id, run_id, _ = _run_identity(context)
    root = _join_uri(_artifact_root(), "discovery", _run_key(dag_id, run_id))
    review_uri = _join_uri(root, "reviewable-diff.json")
    latest_state_uri = _join_uri(_artifact_root(), "discovery", "latest-state.json")
    canonical_index_ref = _discovery_artifact_ref(
        discovery_detail_index_ref,
        label="discovery detail index reference",
    )
    canonical_phase_refs = _discovery_phase_input_refs(discovery_detail_phase_refs)
    existing_review = _optional_payload(review_uri, kind="espn-discovery-review-v1")
    if existing_review is not None:
        review = _validate_discovery_review_checkpoint(
            existing_review,
            discovery_detail_index_ref=canonical_index_ref,
            discovery_detail_phase_refs=canonical_phase_refs,
        )
        review_ref = _ref_for_uri(review_uri)
        _publish_latest_discovery_state(
            latest_state_uri,
            _discovery_review_state(review, review_ref=review_ref),
        )
        if review["quarantined_scopes"]:
            from airflow.exceptions import AirflowException

            raise AirflowException(
                "ESPN discovery quarantined unpromoted current seasons: "
                + ", ".join(review["quarantined_scopes"])
            )
        return {"discovery_review_ref": review_ref}

    detail_index = _read_ref(canonical_index_ref, kind="espn-discovery-detail-index-v1")
    competition_count = detail_index.get("competition_count")
    if (
        detail_index.get("competition_cap") != MAX_DISCOVERY_COMPETITIONS
        or detail_index.get("detail_request_cap") != MAX_DISCOVERY_DETAIL_REQUESTS
        or type(competition_count) is not int
        or not 0 <= competition_count <= MAX_DISCOVERY_COMPETITIONS
    ):
        raise OperationsError("discovery detail index violates the static request cap")
    raw = _read_ref(detail_index["discovery_raw_ref"], kind="espn-discovery-raw-v1")
    body = EspnRawStore.from_uri(_raw_store_uri()).load_exact(
        raw["raw_uri"], raw["raw_sha256"]
    )
    try:
        dropdown = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("discovery catalog is not valid JSON") from exc
    phases = {}
    for phase_ref in canonical_phase_refs:
        phase = _read_ref(
            phase_ref,
            kind="espn-discovery-detail-phase-v1",
        )
        if phase["batch_id"] in phases:
            raise OperationsError("duplicate discovery detail phase")
        phases[phase["batch_id"]] = phase
    if set(phases) != set(detail_index["batch_ids"]):
        raise OperationsError("discovery detail phase set differs from signed index")
    indexed_batches = {}
    for batch_ref in detail_index["batch_refs"]:
        batch = _read_ref(batch_ref, kind="espn-discovery-detail-batch-v1")
        if batch["batch_id"] in indexed_batches:
            raise OperationsError("discovery detail index duplicates a batch")
        if batch["discovery_raw_ref"] != detail_index["discovery_raw_ref"]:
            raise OperationsError("discovery detail batch raw identity mismatch")
        indexed_batches[batch["batch_id"]] = (batch_ref, batch)
    if set(indexed_batches) != set(detail_index["batch_ids"]):
        raise OperationsError("discovery detail index batch references are incomplete")
    details_by_slug = {}
    seen_ids = set()
    detail_request_count = 0
    detail_proxy_bytes = 0
    raw_store = EspnRawStore.from_uri(_raw_store_uri())
    for batch_id in detail_index["batch_ids"]:
        phase = phases[batch_id]
        indexed_batch_ref, batch = indexed_batches[batch_id]
        if phase["discovery_detail_batch_ref"] != indexed_batch_ref:
            raise OperationsError("discovery detail phase batch reference mismatch")
        if batch["batch_id"] != batch_id:
            raise OperationsError("discovery detail batch identity mismatch")
        expected = {
            (item["espn_id"], item["slug"], item["name"])
            for item in batch["competitions"]
        }
        actual = {
            (item["espn_id"], item["slug"], item["name"]) for item in phase["details"]
        }
        if actual != expected or len(actual) != len(phase["details"]):
            raise OperationsError("discovery detail phase coverage mismatch")
        for item in phase["details"]:
            identity = (item["espn_id"], item["slug"], item["name"])
            if identity in seen_ids:
                raise OperationsError("discovery detail identity was fetched twice")
            seen_ids.add(identity)
            summary_uri = item.get("summary_raw_uri")
            summary_sha256 = item.get("summary_raw_sha256")
            summary_event_id = item.get("summary_event_id")
            if (summary_uri is None) != (summary_sha256 is None) or (
                summary_uri is None and summary_event_id is not None
            ):
                raise OperationsError("discovery Summary evidence is incomplete")
            expected_request_count = 2 + int(summary_uri is not None)
            if item.get("request_count") != expected_request_count:
                raise OperationsError("discovery detail request accounting mismatch")
            detail_request_count += expected_request_count
            detail_proxy_bytes += int(item["proxy_bytes"])

            def load_json(uri_field: str, sha_field: str, label: str):
                detail_body = raw_store.load_exact(item[uri_field], item[sha_field])
                try:
                    document = json.loads(detail_body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise OperationsError(
                        f"discovery {label} is not valid JSON"
                    ) from exc
                if not isinstance(document, Mapping):
                    raise OperationsError(f"discovery {label} must be an object")
                return document

            metadata_document = load_json(
                "metadata_raw_uri", "metadata_raw_sha256", "metadata"
            )
            scoreboard_document = load_json(
                "scoreboard_raw_uri", "scoreboard_raw_sha256", "scoreboard"
            )
            resolved_espn_id = _discovery_metadata_id(
                metadata_document,
                espn_id=item["espn_id"],
                slug=item["slug"],
            )
            exact_event_id = _first_discovery_event_id(
                scoreboard_document,
                espn_id=resolved_espn_id,
                slug=item["slug"],
            )
            if exact_event_id != summary_event_id:
                raise OperationsError("discovery Summary event identity mismatch")
            summary_document = None
            if summary_uri is not None:
                summary_document = load_json(
                    "summary_raw_uri", "summary_raw_sha256", "Summary"
                )
                header = summary_document.get("header")
                if not isinstance(header, Mapping) or str(
                    header.get("id") or ""
                ).strip() != str(summary_event_id):
                    raise OperationsError("discovery Summary payload identity mismatch")
            details_by_slug[item["slug"]] = _competition_detail_document(
                metadata_document,
                scoreboard_document,
                summary_document,
                espn_id=item["espn_id"],
                slug=item["slug"],
                name=item["name"],
            )
    if len(seen_ids) != detail_index["competition_count"]:
        raise OperationsError("discovery detail competition count mismatch")
    if detail_request_count > detail_index["detail_request_cap"]:
        raise OperationsError("discovery detail request cap exceeded")
    current = _discovery_snapshot(
        {"dropdown": dropdown, "details_by_slug": details_by_slug},
        captured_at=raw["captured_at"],
    )
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
                    "direct_requests": 1 + detail_request_count,
                    "request_budget": 1 + MAX_DISCOVERY_DETAIL_REQUESTS,
                    "proxy_bytes": raw["proxy_bytes"] + detail_proxy_bytes,
                    "lease_conflict": False,
                    "unpromoted_current_season": alert_scope in quarantined,
                    "unresolved_discovery_diffs": unresolved,
                },
                observed_at=observed_at,
            )
        )
    review = {
        "kind": "espn-discovery-review-v1",
        "schema_version": 2,
        "discovery_detail_index_ref": canonical_index_ref,
        "discovery_detail_phase_refs": canonical_phase_refs,
        "candidate_ref": candidate_ref,
        "candidate_identity": candidate_identity,
        "registry_signature": registry.signature(),
        "quarantined_scopes": quarantined,
        "changes": [json.loads(item.canonical_json()) for item in diff.changes],
        "change_count": len(diff.changes),
        "unresolved_discovery_diffs": unresolved,
        "alerts": alerts,
        "promotion_performed": False,
        "observed_at": observed_at.isoformat(),
    }
    review_ref = _write_payload(review_uri, review)
    _publish_latest_discovery_state(
        latest_state_uri,
        _discovery_review_state(review, review_ref=review_ref),
    )
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
