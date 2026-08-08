"""Executable Airflow task callables for ESPN Native Bronze v2.

Every cross-task value is a bounded URI/SHA descriptor.  Raw bodies and row
sets stay in the immutable raw/artifact stores; PostgreSQL owns leases, the
shared source rate gate, current heads and exact current-run evidence.
"""

from __future__ import annotations

from collections.abc import Sequence as SequenceCollection
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from scrapers.espn.models import Gender, IngestPlan, ScopePlan
from scrapers.espn.daily_owner import (
    ACTIVE_TRIGGER_STATES,
    DAILY_PARENT_FIELDS,
    ESPN_ISOLATED_V1,
    SCHEDULED_RUN_TYPE,
    DailyOwnerError,
    daily_child_run_id,
    resolve_daily_owner_profile,
    standard_scheduled_run_id,
)
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
from scrapers.espn.registry import (
    Registry,
    build_discovered_male_registry,
    load_registry,
    validate_registry_document,
)
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
    HttpStatusError,
    TaskBudget,
)


UTC = timezone.utc
HTTP_POOL = "espn_http_pool"
REPOSITORY_POOL = "espn_repository_pool"
REPOSITORY_POOL_SLOTS = 16
REGISTRY_ENV = "ESPN_REGISTRY_PATH"
ARTIFACT_ROOT_ENV = "ESPN_ARTIFACT_ROOT_URI"
RAW_STORE_ENV = "ESPN_RAW_STORE_URI"
DISCOVERY_STATE_REF_URI_ENV = "ESPN_DISCOVERY_STATE_REF_URI"
DISCOVERY_STATE_REF_SHA256_ENV = "ESPN_DISCOVERY_STATE_REF_SHA256"
LEASE_TTL = timedelta(hours=12)
DAILY_BOOTSTRAP_SCOPE_LIMIT = 10
DISCOVERY_MAX_AGE = timedelta(days=8)
DISCOVERY_FUTURE_TOLERANCE = timedelta(minutes=5)
DISCOVERY_DETAIL_BATCH_SIZE = 20
DISCOVERY_SELECTION_POLICY = "explicit-core-gender-MALE-v1"
DISCOVERY_CATALOG_URL = (
    "https://sports.core.api.espn.com/v2/sports/soccer/leagues"
    "?limit=500&lang=en&region=us"
)
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


def _exact_mapped_results(
    values: object,
    *,
    expected_count: object,
    label: str,
) -> list[Mapping[str, Any]]:
    """Normalize Airflow's zero-map ``None`` behind an exact durable count."""

    if type(expected_count) is not int or expected_count < 0:
        raise OperationsError(f"{label} expected map count is invalid")
    if values is None:
        if expected_count == 0:
            return []
        raise OperationsError(f"{label} mapped output is missing")
    if isinstance(values, (str, bytes, bytearray, Mapping)) or not isinstance(
        values, SequenceCollection
    ):
        raise OperationsError(f"{label} mapped output must be a sequence")
    rows = list(values)
    if len(rows) != expected_count:
        raise OperationsError(
            f"{label} mapped output count differs from signed plan: "
            f"{len(rows)} != {expected_count}"
        )
    if any(not isinstance(row, Mapping) for row in rows):
        raise OperationsError(f"{label} mapped output row must be an object")
    return rows


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


def _payload_ref(uri: str, payload: Mapping[str, Any]) -> dict[str, str]:
    return {"uri": uri, "sha256": hashlib.sha256(_canonical_bytes(payload)).hexdigest()}


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
        if len(requested) > MAX_INGEST_SCOPE_MAP_ITEMS:
            raise OperationsError("ESPN scope selector exceeds its static bound")
        admitted = all_promoted if mode in _MANUAL_MODES else current
        unknown = sorted(set(requested) - set(admitted))
        if unknown:
            raise OperationsError(f"unpromoted ESPN scopes requested: {unknown}")
        return requested
    if mode in _MANUAL_MODES:
        raise OperationsError(f"manual ESPN {mode} requires explicit scopes")
    return current


def _bounded_daily_scopes(
    registry: Registry, heads: Mapping[str, ScopeHead]
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Admit daily scheduling only after the manual all-scope migration."""

    target = tuple(
        sorted(
            competition.scope_id(competition.current_edition)
            for competition in registry.promoted
        )
    )
    if len(target) > MAX_INGEST_SCOPE_MAP_ITEMS:
        raise OperationsError("daily ESPN target exceeds its static map bound")
    if len(target) != 181:
        raise OperationsError("daily ESPN enablement requires the frozen 181 scopes")
    missing = tuple(scope for scope in target if scope not in heads)
    extra = tuple(sorted(set(heads) - set(target)))
    if missing or extra:
        raise OperationsError(
            "daily ESPN enablement requires 181/181 COMPLETE heads; "
            f"missing={list(missing)}, extra={list(extra)}"
        )
    return target, target, ()


def _require_scheduler_runtime_heads(
    target: Sequence[str], heads: Mapping[str, ScopeHead]
) -> None:
    """Prove every scheduled head is an exact parser-v3/runtime-v4 snapshot."""

    if len(target) != 181 or tuple(sorted(set(target))) != tuple(target):
        raise OperationsError(
            "scheduler runtime target must be 181 sorted unique scopes"
        )
    if set(heads) != set(target):
        raise OperationsError("scheduler runtime gate requires exact 181/181 heads")
    mismatches = []
    for scope_id in target:
        head = heads[scope_id]
        try:
            generation = runner.load_scope_snapshot(
                head.snapshot_uri,
                artifact_sha256=head.snapshot_sha256,
                expected_scope_id=scope_id,
            )
        except Exception as exc:
            raise OperationsError(
                f"scheduler runtime head {scope_id} is not exact COMPLETE: {exc}"
            ) from exc
        identity = (generation.parser_version, generation.runtime_version)
        if identity != (runner.PARSER_VERSION, runner.RUNTIME_VERSION):
            mismatches.append((scope_id, *identity))
    if mismatches:
        raise OperationsError(
            "scheduler enablement requires 181/181 parser-v3/runtime-v4 heads; "
            f"mismatches={mismatches}"
        )


def _frozen_discovery_state_ref() -> dict[str, str] | None:
    """Return the optional release-pinned discovery state, fail-closed."""

    uri = os.environ.get(DISCOVERY_STATE_REF_URI_ENV)
    sha256 = os.environ.get(DISCOVERY_STATE_REF_SHA256_ENV)
    if (uri is None) != (sha256 is None):
        raise OperationsError(
            f"{DISCOVERY_STATE_REF_URI_ENV} and "
            f"{DISCOVERY_STATE_REF_SHA256_ENV} must be configured together"
        )
    if uri is None:
        return None
    return {
        "uri": _required(uri, DISCOVERY_STATE_REF_URI_ENV),
        "sha256": _sha(sha256, DISCOVERY_STATE_REF_SHA256_ENV),
    }


def _load_discovered_registry(*, now: datetime) -> tuple[Registry, dict[str, Any]]:
    """Pin and validate the exact fresh all-male discovery projection."""

    if not isinstance(now, datetime) or now.tzinfo is None:
        raise OperationsError("discovery freshness clock must be timezone-aware")
    database_now = now.astimezone(UTC)
    try:
        frozen_state_ref = _frozen_discovery_state_ref()
        state_ref = frozen_state_ref
        if state_ref is None:
            latest_uri = _join_uri(_artifact_root(), "discovery", "latest-state.json")
            state_ref = _ref_for_uri(latest_uri)
        state = _read_ref(state_ref, kind="espn-discovery-state-v2")
        _candidate, registry, _review = _load_discovery_state_v2(state)
    except OperationsError:
        raise
    except (
        FileNotFoundError,
        OSError,
        KeyError,
        TypeError,
        ValueError,
        runner.RunnerError,
    ) as exc:
        raise OperationsError("discovered registry checkpoint is invalid") from exc
    observed_at = _discovery_observed_at(
        state["observed_at"], label="discovery state observed_at"
    )
    if frozen_state_ref is None and database_now - observed_at > DISCOVERY_MAX_AGE:
        raise OperationsError("discovery state is older than eight days")
    if observed_at - database_now > DISCOVERY_FUTURE_TOLERANCE:
        raise OperationsError("discovery state is more than five minutes in the future")
    if any(competition.gender is not Gender.MALE for competition in registry.promoted):
        raise OperationsError("discovery registry contains an enabled non-MALE row")
    if len(registry.promoted) != state["male_scope_count"]:
        raise OperationsError("discovery state male registry count mismatch")
    return registry, {**state, "discovery_state_ref": state_ref}


def _admission_payload(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OperationsError("admission artifact must be an object")
    expected = {
        "espn-airflow-admission-v1": 1,
        "espn-airflow-admission-v2": 2,
    }
    kind = value.get("kind")
    if kind not in expected or expected[kind] != value.get("schema_version"):
        raise OperationsError("admission artifact schema is unsupported")
    common = {
        "kind",
        "schema_version",
        "dag_id",
        "run_id",
        "attempt",
        "mode",
        "as_of",
        "logical_date",
        "parent",
        "registry_ref",
        "registry_signature",
        "scope_ids",
        "artifact_root",
        "raw_store_uri",
        "replay_sources",
    }
    v2 = {
        "target_scope_ids",
        "bootstrap_scope_ids",
        "discovery_state_ref",
        "candidate_ref",
        "selection_policy",
        "male_scope_count",
    }
    if set(value) != common | (v2 if expected[kind] == 2 else set()):
        raise OperationsError("admission artifact fields mismatch its schema")
    for field in ("dag_id", "run_id", "artifact_root", "raw_store_uri"):
        _required(value[field], f"admission {field}")
    if type(value["attempt"]) is not int or value["attempt"] < 1:
        raise OperationsError("admission attempt must be positive")
    if value["mode"] not in {"daily", *_MANUAL_MODES}:
        raise OperationsError("admission mode is unsupported")
    try:
        date.fromisoformat(_required(value["as_of"], "admission as_of"))
        logical_date = datetime.fromisoformat(
            _required(value["logical_date"], "admission logical_date")
        )
    except ValueError as exc:
        raise OperationsError("admission dates are invalid") from exc
    if logical_date.tzinfo is None:
        raise OperationsError("admission logical_date must be timezone-aware")
    if value["parent"] is not None and not isinstance(value["parent"], Mapping):
        raise OperationsError("admission parent must be an object or null")
    _discovery_artifact_ref(value["registry_ref"], label="admission registry reference")
    _sha(value["registry_signature"], "admission registry signature")
    if not isinstance(value["replay_sources"], Mapping):
        raise OperationsError("admission replay_sources must be an object")

    def scopes(field: str, *, allow_empty: bool) -> tuple[str, ...]:
        raw = value[field]
        if not isinstance(raw, list) or not all(
            isinstance(item, str) and item for item in raw
        ):
            raise OperationsError(f"admission {field} must be a string list")
        result = tuple(raw)
        if (not allow_empty and not result) or tuple(sorted(set(result))) != result:
            raise OperationsError(f"admission {field} must be sorted and unique")
        if len(result) > MAX_INGEST_SCOPE_MAP_ITEMS:
            raise OperationsError(f"admission {field} exceeds its static bound")
        return result

    selected = scopes("scope_ids", allow_empty=False)
    if expected[kind] == 2:
        target = scopes("target_scope_ids", allow_empty=False)
        bootstrap = scopes("bootstrap_scope_ids", allow_empty=True)
        _discovery_artifact_ref(
            value["discovery_state_ref"], label="admission discovery state reference"
        )
        _discovery_artifact_ref(
            value["candidate_ref"], label="admission candidate reference"
        )
        if value["selection_policy"] != DISCOVERY_SELECTION_POLICY:
            raise OperationsError("admission selection policy mismatch")
        if type(value["male_scope_count"]) is not int or value[
            "male_scope_count"
        ] != len(target):
            raise OperationsError("admission male scope count mismatch")
        if (
            len(bootstrap) > DAILY_BOOTSTRAP_SCOPE_LIMIT
            or not set(bootstrap).issubset(selected)
            or not set(bootstrap).issubset(target)
        ):
            raise OperationsError("admission bootstrap coverage is invalid")
        if value["mode"] == "daily" and not set(selected).issubset(target):
            raise OperationsError("daily admission selects a non-target scope")
        if value["mode"] in _MANUAL_MODES and bootstrap:
            raise OperationsError("manual admission cannot contain bootstrap scopes")
    return value


def _read_admission_ref(ref: Mapping[str, str]) -> Mapping[str, Any]:
    """Read current v2 and in-flight v1 admissions through one version gate."""

    return _admission_payload(_read_ref(ref))


def _replay_existing_admission(
    *,
    admission_uri: str,
    mode: str,
    dag_id: str,
    run_id: str,
    logical_date: datetime,
    parent: Mapping[str, str] | None,
    context: Mapping[str, Any],
) -> dict[str, str] | None:
    try:
        admission_ref = _ref_for_uri(admission_uri)
    except FileNotFoundError:
        return None
    admission = _read_admission_ref(admission_ref)
    if admission["kind"] == "espn-airflow-admission-v2":
        registry = _load_registry_ref(admission)
        target = tuple(
            sorted(
                competition.scope_id(competition.current_edition)
                for competition in registry.promoted
            )
        )
        admitted = {
            competition.scope_id(edition)
            for competition in registry.promoted
            for edition in competition.editions
        }
        if (
            any(item.gender is not Gender.MALE for item in registry.promoted)
            or len(registry.promoted) != admission["male_scope_count"]
            or target != tuple(admission["target_scope_ids"])
            or not set(admission["scope_ids"]).issubset(admitted)
        ):
            raise OperationsError("sealed admission registry coverage mismatch")
    params = context.get("params") or {}
    attempt = _attempt(context)
    expected_identity = (
        dag_id,
        run_id,
        attempt,
        mode,
        logical_date.date().isoformat(),
        logical_date.isoformat(),
        parent,
        admission_uri.rsplit("/", 1)[0],
    )
    actual_identity = (
        admission.get("dag_id"),
        admission.get("run_id"),
        admission.get("attempt"),
        admission.get("mode"),
        admission.get("as_of"),
        admission.get("logical_date"),
        admission.get("parent"),
        admission.get("artifact_root"),
    )
    if actual_identity != expected_identity:
        raise OperationsError("existing admission identity differs from retry")
    requested = tuple(sorted(str(item) for item in (params.get("scopes") or ())))
    if mode in _MANUAL_MODES and tuple(admission.get("scope_ids") or ()) != requested:
        raise OperationsError("existing admission scopes differ from retry")
    replay_sources = params.get("replay_sources", {}) if mode == "replay" else {}
    if admission.get("replay_sources") != replay_sources:
        raise OperationsError("existing admission replay sources differ from retry")
    return dict(admission_ref)


def _iso_utc(value: object, field: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise OperationsError(f"{field} must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _exact_parent_run(profile: object, parent_run_id: str):
    """Load exactly one frozen-profile DagRun from Airflow metadata."""

    from airflow.models import DagRun

    matches = DagRun.find(
        dag_id=getattr(profile, "parent_dag_id", None),
        run_id=_required(parent_run_id, "parent_run_id"),
    )
    if len(matches) != 1:
        raise OperationsError("daily ESPN exact isolated parent was not found")
    return matches[0]


def _daily_parent_envelope(context: Mapping[str, Any]) -> dict[str, str]:
    """Bind only the child runtime/conf envelope without metadata DB reads."""

    dag_run = context.get("dag_run")
    context_run_id = context.get("run_id")
    if (
        getattr(dag_run, "dag_id", None) != ESPN_ISOLATED_V1.child_dag_id
        or getattr(dag_run, "run_id", None) != context_run_id
    ):
        raise OperationsError("daily ESPN child DagRun identity mismatch")
    conf = getattr(dag_run, "conf", None) or {}
    parent = conf.get("espn_parent")
    if not isinstance(parent, Mapping) or set(parent) != DAILY_PARENT_FIELDS:
        raise OperationsError("daily ESPN admission requires exact isolated parent")
    return {key: str(parent[key]) for key in sorted(parent)}


def _daily_parent(context: Mapping[str, Any]) -> dict[str, str]:
    parent = _daily_parent_envelope(context)
    context_run_id = context.get("run_id")
    try:
        profile = resolve_daily_owner_profile(parent.get("owner_profile"))
    except DailyOwnerError as exc:
        raise OperationsError(f"daily ESPN owner profile rejected: {exc}") from exc
    if parent["schema"] != profile.envelope_schema:
        raise OperationsError("daily ESPN parent schema mismatch")
    if parent["parent_dag_id"] != profile.parent_dag_id:
        raise OperationsError("daily ESPN parent DAG mismatch")
    if parent["parent_task_id"] != profile.trigger_task_id:
        raise OperationsError("daily ESPN parent task mismatch")
    if parent["parent_run_type"] != SCHEDULED_RUN_TYPE:
        raise OperationsError("daily ESPN parent run type mismatch")
    if parent["child_dag_id"] != profile.child_dag_id:
        raise OperationsError("daily ESPN child DAG mismatch")
    child_dag = context.get("dag")
    if getattr(child_dag, "dag_id", None) != profile.child_dag_id:
        raise OperationsError("daily ESPN runtime child DAG mismatch")
    parent_run_id = _required(parent["parent_run_id"], "parent_run_id")
    expected_child_run_id = daily_child_run_id(parent_run_id)
    if parent["child_run_id"] != expected_child_run_id:
        raise OperationsError("daily ESPN deterministic child identity mismatch")
    if parent["child_run_id"] != context_run_id:
        raise OperationsError("daily ESPN child run identity mismatch")
    if parent["logical_date"] != _logical_date(context).isoformat():
        raise OperationsError("daily ESPN logical-date binding mismatch")
    parent_run = _exact_parent_run(profile, parent_run_id)
    if getattr(parent_run, "dag_id", None) != profile.parent_dag_id:
        raise OperationsError("daily ESPN metadata parent DAG mismatch")
    if getattr(parent_run, "run_id", None) != parent_run_id:
        raise OperationsError("daily ESPN metadata parent run ID mismatch")
    metadata_run_type = getattr(
        getattr(parent_run, "run_type", None),
        "value",
        getattr(parent_run, "run_type", None),
    )
    if str(metadata_run_type or "").lower().split(".")[-1] != SCHEDULED_RUN_TYPE:
        raise OperationsError("daily ESPN metadata parent is not scheduled")
    parent_state = getattr(
        getattr(parent_run, "state", None),
        "value",
        getattr(parent_run, "state", None),
    )
    if str(parent_state or "").lower().split(".")[-1] != "running":
        raise OperationsError("daily ESPN metadata parent run is not active")
    exact_dates = {
        "logical_date": _iso_utc(
            getattr(parent_run, "logical_date", None), "parent logical_date"
        ),
        "data_interval_start": _iso_utc(
            getattr(parent_run, "data_interval_start", None),
            "parent data_interval_start",
        ),
        "data_interval_end": _iso_utc(
            getattr(parent_run, "data_interval_end", None),
            "parent data_interval_end",
        ),
    }
    for field, actual in exact_dates.items():
        if parent[field] != actual:
            raise OperationsError(f"daily ESPN parent {field} binding mismatch")
    metadata_logical = datetime.fromisoformat(exact_dates["logical_date"])
    metadata_start = datetime.fromisoformat(exact_dates["data_interval_start"])
    metadata_end = datetime.fromisoformat(exact_dates["data_interval_end"])
    if (
        parent_run_id != standard_scheduled_run_id(metadata_logical)
        or metadata_logical != metadata_start
        or metadata_end - metadata_start != timedelta(days=1)
        or (
            metadata_start.hour,
            metadata_start.minute,
            metadata_start.second,
            metadata_start.microsecond,
        )
        != (14, 0, 0, 0)
    ):
        raise OperationsError("daily ESPN metadata parent run ID/interval mismatch")
    trigger_instance = parent_run.get_task_instance(task_id=profile.trigger_task_id)
    if (
        getattr(trigger_instance, "dag_id", None) != profile.parent_dag_id
        or getattr(trigger_instance, "task_id", None) != profile.trigger_task_id
        or getattr(trigger_instance, "run_id", None) != parent_run_id
    ):
        raise OperationsError("daily ESPN parent trigger task identity mismatch")
    trigger_state = getattr(trigger_instance, "state", None)
    trigger_state = getattr(trigger_state, "value", trigger_state)
    if str(trigger_state or "").lower().split(".")[-1] not in ACTIVE_TRIGGER_STATES:
        raise OperationsError("daily ESPN owner trigger task is not active")
    return {key: str(parent[key]) for key in sorted(parent)}


def validate_registry_and_admission(*, mode: str, **context) -> dict[str, str]:
    """Fail-closed admission and immutable registry snapshot creation."""

    if mode not in {"daily", *_MANUAL_MODES}:
        raise OperationsError("unsupported ESPN orchestration mode")
    dag_id, run_id, logical_date = _run_identity(context)
    retry_parent = _daily_parent_envelope(context) if mode == "daily" else None
    if mode != "daily":
        dag_run = context.get("dag_run")
        if (getattr(dag_run, "conf", None) or {}).get("espn_parent") is not None:
            raise OperationsError("manual ESPN mode forbids isolated daily parent")
    root = _join_uri(_artifact_root(), "runs", _run_key(dag_id, run_id))
    admission_uri = _join_uri(root, "admission.json")
    existing_ref = _replay_existing_admission(
        admission_uri=admission_uri,
        mode=mode,
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        parent=retry_parent,
        context=context,
    )
    if existing_ref is not None:
        return existing_ref
    parent = _daily_parent(context) if mode == "daily" else None
    store = PostgresEspnControlStore.from_env()
    store.migrate()
    registry, discovery = _load_discovered_registry(now=store.current_time())
    target_scopes = tuple(
        sorted(
            competition.scope_id(competition.current_edition)
            for competition in registry.promoted
        )
    )
    bootstrap_scopes: tuple[str, ...] = ()
    if mode == "daily":
        heads = store.read_scope_heads(target_scopes)
        target_scopes, scopes, bootstrap_scopes = _bounded_daily_scopes(registry, heads)
        _require_scheduler_runtime_heads(target_scopes, heads)
    else:
        scopes = _selected_scopes(registry, mode, context.get("params") or {})
    if not scopes:
        raise OperationsError("ESPN registry has no admitted scopes")
    payload = {
        "kind": "espn-airflow-admission-v2",
        "schema_version": 2,
        "dag_id": dag_id,
        "run_id": run_id,
        "attempt": _attempt(context),
        "mode": mode,
        "as_of": logical_date.date().isoformat(),
        "logical_date": logical_date.isoformat(),
        "parent": parent,
        "registry_ref": discovery["male_registry_ref"],
        "registry_signature": registry.signature(),
        "target_scope_ids": list(target_scopes),
        "scope_ids": list(scopes),
        "bootstrap_scope_ids": list(bootstrap_scopes),
        "discovery_state_ref": discovery["discovery_state_ref"],
        "candidate_ref": discovery["candidate_ref"],
        "selection_policy": discovery["selection_policy"],
        "male_scope_count": discovery["male_scope_count"],
        "artifact_root": root,
        "raw_store_uri": _raw_store_uri(),
        "replay_sources": (
            (context.get("params") or {}).get("replay_sources", {})
            if mode == "replay"
            else {}
        ),
    }
    return _write_payload(admission_uri, payload, immutable=True)


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
    prior_generation = None
    known = []
    if head is not None:
        generation = runner.load_scope_snapshot(
            head.snapshot_uri,
            artifact_sha256=head.snapshot_sha256,
            expected_scope_id=scope.scope_id,
        )
        if generation.plan != scope:
            raise OperationsError("prior scope plan differs from promoted registry")
        prior_generation = generation
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
        "scoreboard_max_range_days": runner.SCOREBOARD_MAX_RANGE_DAYS,
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
    runner._prior_parser_transition(
        prior_generation,
        full=runner._full_strategy(scope, typed, mode, as_of),
    )
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
        "scoreboard_max_range_days": source_binding.scoreboard_max_range_days,
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

    admission = _read_admission_ref(admission_ref)
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
    admission = _read_admission_ref(acquisition["admission_ref"])
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
        "acquired_at": lease.acquired_at.astimezone(UTC).isoformat(),
        "expires_at": lease.expires_at.astimezone(UTC).isoformat(),
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


_SCOPE_LEASE_BINDING_FIELDS = {
    "kind",
    "schema_version",
    "scope_plan_ref",
    "lease",
}
_SCOPE_LEASE_FIELDS = {
    "scope_id",
    "owner_id",
    "plan_signature",
    "epoch",
    "token_sha256",
    "acquired_at",
    "expires_at",
}
_SCOPE_PLAN_DESCRIPTOR_FIELDS = {
    "kind",
    "schema_version",
    "dag_id",
    "run_id",
    "attempt",
    "mode",
    "scope_id",
    "plan_ref",
    "plan_signature",
    "raw_manifest_uri",
    "raw_store_uri",
    "generation_snapshot_uri",
    "expected_scoreboard_batch",
    "scoreboard_checkpoint_uri",
    "scope_root",
}


def _binding(ref: Mapping[str, str]):
    binding = _read_ref(ref, kind="espn-scope-lease-binding-v1")
    raw_lease = binding.get("lease")
    if (
        set(binding) != _SCOPE_LEASE_BINDING_FIELDS
        or type(binding.get("schema_version")) is not int
        or binding.get("schema_version") != 1
        or not isinstance(raw_lease, Mapping)
        or set(raw_lease) != _SCOPE_LEASE_FIELDS
    ):
        raise OperationsError("scope lease binding schema mismatch")
    try:
        lease = _lease_from_dict(raw_lease)
    except (KeyError, TypeError, ValueError) as exc:
        raise OperationsError("scope lease binding lease is invalid") from exc
    canonical_lease = {
        **_lease_to_dict(lease),
        "acquired_at": lease.acquired_at.astimezone(UTC).isoformat(),
        "expires_at": lease.expires_at.astimezone(UTC).isoformat(),
    }
    if canonical_lease != dict(raw_lease):
        raise OperationsError("scope lease binding lease is not canonical")
    descriptor = _read_ref(
        binding["scope_plan_ref"], kind="espn-scope-plan-descriptor-v1"
    )
    if (
        set(descriptor) != _SCOPE_PLAN_DESCRIPTOR_FIELDS
        or type(descriptor.get("schema_version")) is not int
        or descriptor.get("schema_version") != 1
        or type(descriptor.get("attempt")) is not int
        or descriptor.get("attempt", 0) <= 0
    ):
        raise OperationsError("scope plan descriptor schema mismatch")
    for field in (
        "dag_id",
        "run_id",
        "mode",
        "scope_id",
        "plan_signature",
        "raw_manifest_uri",
        "raw_store_uri",
        "generation_snapshot_uri",
        "scope_root",
    ):
        _required(descriptor[field], f"scope descriptor {field}")
    scoreboard_batch = descriptor["expected_scoreboard_batch"]
    scoreboard_checkpoint_uri = descriptor["scoreboard_checkpoint_uri"]
    if (scoreboard_batch is None) != (scoreboard_checkpoint_uri is None) or (
        scoreboard_batch is not None
        and (
            not isinstance(scoreboard_batch, Mapping)
            or not isinstance(scoreboard_checkpoint_uri, str)
            or not scoreboard_checkpoint_uri
        )
    ):
        raise OperationsError("scope descriptor scoreboard contract is invalid")
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
    allow_site_origin_failover: bool = False,
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
        allow_site_origin_failover=allow_site_origin_failover,
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
        sorted(expected["request_ids"]),
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
    if sorted(item["request_id"] for item in requests) != sorted(
        expected["request_ids"]
    ):
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
    client = _http_client(
        raw_store,
        max_summary_events=1,
        allow_site_origin_failover=True,
    )
    requests = runner._scoreboard_requests(
        scope,
        loaded.bindings[scope.scope_id],
        as_of=loaded.plan.as_of,
        mode=runner._effective_mode(loaded),
    )
    expected = descriptor["expected_scoreboard_batch"]
    if sorted(item.request_id for item in requests) != sorted(expected["request_ids"]):
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
    plan_index_ref: Mapping[str, str],
    scoreboard_phase_refs: Sequence[Mapping[str, Any]] | None,
    scope_binding_refs: Sequence[Mapping[str, Any]],
    **_context,
) -> dict[str, Any]:
    """Offline enumerate wave-two IDs and persist each <=50 descriptor."""

    plan_index = _read_ref(plan_index_ref, kind="espn-plan-index-v1")
    indexed_scope_ids = plan_index.get("scope_ids")
    network_scope_ids = plan_index.get("network_scope_ids")
    expected_scoreboard_count = plan_index.get("expected_scoreboard_map_count")
    if (
        not isinstance(indexed_scope_ids, list)
        or not indexed_scope_ids
        or not all(
            isinstance(scope_id, str) and scope_id for scope_id in indexed_scope_ids
        )
        or len(indexed_scope_ids) != len(set(indexed_scope_ids))
        or not isinstance(network_scope_ids, list)
        or not all(
            isinstance(scope_id, str) and scope_id for scope_id in network_scope_ids
        )
        or len(network_scope_ids) != len(set(network_scope_ids))
        or not set(network_scope_ids).issubset(indexed_scope_ids)
        or type(expected_scoreboard_count) is not int
        or expected_scoreboard_count != len(network_scope_ids)
        or expected_scoreboard_count > MAX_INGEST_SCOPE_MAP_ITEMS
    ):
        raise OperationsError("signed plan index scoreboard map contract is invalid")
    scoreboard_phase_refs = _exact_mapped_results(
        scoreboard_phase_refs,
        expected_count=expected_scoreboard_count,
        label="scoreboard phase",
    )
    batch_refs = []
    scope_indexes = []
    phases_by_scope = {}
    for wrapped in scoreboard_phase_refs:
        if set(wrapped) != {"scoreboard_phase_ref"}:
            raise OperationsError("scoreboard phase wrapper mismatch")
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
        parsed_pages = []
        raw_store = EspnRawStore.from_uri(descriptor["raw_store_uri"])
        for record in checkpoint["requests"]:
            request = request_plan.get(record["request_id"])
            if request is None:
                raise OperationsError("scoreboard checkpoint has unplanned request")
            body = raw_store.load_exact(record["raw_uri"], record["raw_sha256"])
            rows = runner._parse_scoreboard_response(
                body,
                request=request,
                competition=competition,
                edition=edition,
            )
            parsed_pages.append((request, rows))
        fetched, _winner = runner._merge_scoreboard_pages(parsed_pages)
        missing_known = sorted(
            item.event_id
            for item in loaded.bindings[scope.scope_id].known_nonterminal_events
            if item.event_id not in fetched
        )
        if missing_known:
            raise OperationsError(
                f"known non-terminal events absent from scoreboard: {missing_known}"
            )
        prior = (
            runner._load_prior(typed_binding.prior, scope)
            if typed_binding.prior is not None
            else None
        )
        full = runner._full_strategy(
            scope,
            typed_binding,
            runner._effective_mode(loaded),
            loaded.plan.as_of,
        )
        transition = runner._prior_parser_transition(prior, full=full)
        effective_prior = (
            runner._bridge_v2_prior(prior)
            if transition == "v2-to-v3" and prior is not None
            else prior
        )
        event_ids = runner.summary_refresh_event_ids(
            fetched.values(), effective_prior
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
    if seen_scopes != set(indexed_scope_ids):
        raise OperationsError("summary scope bindings differ from signed plan index")
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
        raw_store,
        max_summary_events=max(1, len(expected["event_ids"])),
        allow_site_origin_failover=True,
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
    summary_phase_refs: Sequence[Mapping[str, Any]] | None,
    **_context,
) -> list[dict[str, Any]]:
    """Deterministically reduce exact signed batch set; no shared mutable map."""

    index = _read_ref(summary_index_ref, kind="espn-summary-wave-index-v1")
    scope_indexes = index.get("scopes")
    expected_map_count = index.get("expected_map_count")
    if not isinstance(scope_indexes, list) or not scope_indexes:
        raise OperationsError("Summary wave index scopes are invalid")
    indexed_scope_ids = []
    indexed_summary_count = 0
    for scope_index in scope_indexes:
        if not isinstance(scope_index, Mapping):
            raise OperationsError("Summary wave index scope must be an object")
        scope_id = scope_index.get("scope_id")
        summary_batch_refs = scope_index.get("summary_batch_refs")
        if (
            not isinstance(scope_id, str)
            or not scope_id
            or not isinstance(summary_batch_refs, list)
        ):
            raise OperationsError("Summary wave index scope contract is invalid")
        indexed_scope_ids.append(scope_id)
        indexed_summary_count += len(summary_batch_refs)
    if (
        len(indexed_scope_ids) != len(set(indexed_scope_ids))
        or type(expected_map_count) is not int
        or expected_map_count != indexed_summary_count
        or expected_map_count > MAX_SUMMARY_BATCH_MAP_ITEMS
    ):
        raise OperationsError("Summary wave index map contract is invalid")
    summary_phase_refs = _exact_mapped_results(
        summary_phase_refs,
        expected_count=expected_map_count,
        label="Summary phase",
    )
    summary_phases = {}
    for wrapped in summary_phase_refs:
        if set(wrapped) != {"summary_phase_ref"}:
            raise OperationsError("Summary phase wrapper mismatch")
        phase_ref = wrapped["summary_phase_ref"]
        phase = _read_ref(phase_ref, kind="espn-summary-phase-result-v1")
        key = phase["expected_batch"]["batch_id"]
        if key in summary_phases:
            raise OperationsError("duplicate Summary phase batch")
        summary_phases[key] = (phase_ref, phase)
    outputs = []
    consumed: set[str] = set()
    for scope_index in scope_indexes:
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
    raw_payload = runner._validate_raw_manifest(_read_ref(raw_ref))
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
    raw_manifest = runner._validate_raw_manifest(_read_ref(phase["raw_manifest_ref"]))
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


def _qualification_state(value: object) -> str:
    if type(value) is not str or value not in {"complete", "noop"}:
        raise OperationsError("publication result state is invalid")
    return value


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


def _repository_pool_slots() -> int:
    from airflow.models.pool import Pool

    pool = Pool.get_pool(REPOSITORY_POOL)
    if pool is None or type(pool.slots) is not int:
        raise OperationsError("ESPN repository pool is missing or malformed")
    return pool.slots


def ensure_repository_objects(**_context) -> dict[str, str]:
    """Create/evolve shared ESPN objects once before parallel publication."""

    if _repository_pool_slots() != REPOSITORY_POOL_SLOTS:
        raise OperationsError(
            f"ESPN repository pool must have exactly {REPOSITORY_POOL_SLOTS} slots"
        )
    repository = EspnBronzeRepository()
    repository.ensure_objects()
    return {"state": "ready"}


def _publication_repository() -> EspnBronzeRepository:
    """Return a writer that trusts the exclusive upstream DDL preflight."""

    return EspnBronzeRepository(ensure_objects_on_write=False)


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
    repository = _publication_repository()
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
    publication_refs: Sequence[Mapping[str, Any]] | None,
    **_context,
) -> dict[str, Any]:
    """Seal exact current-run durable evidence; never consult a latest pointer."""

    index = _read_ref(plan_index_ref, kind="espn-plan-index-v1")
    scope_ids = index.get("scope_ids")
    if (
        not isinstance(scope_ids, list)
        or not scope_ids
        or not all(isinstance(scope_id, str) and scope_id for scope_id in scope_ids)
        or len(scope_ids) != len(set(scope_ids))
    ):
        raise OperationsError("plan index scope set is invalid")
    publication_refs = _exact_mapped_results(
        publication_refs,
        expected_count=len(scope_ids),
        label="publication",
    )
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
        _qualification_state(publication["state"])
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
    current_route = repository.current_scope_route(scope.scope_id)
    if current_route == "native":
        repository.verify_current_scope_selection(selected_generation)
    else:
        repository.verify_current_scope_absence(scope.scope_id)
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
    binding_lease = _lease_to_dict(lease)
    attestation = {
        "kind": "espn-scope-qualification-attestation-v1",
        "schema_version": 1,
        "qualification": {
            "dag_id": descriptor["dag_id"],
            "run_id": loaded.plan.run_id,
            "attempt": loaded.attempt,
            "scope_id": scope.scope_id,
            "state": publication["state"],
            "registry_signature": loaded.plan.registry_signature,
            "plan_signature": loaded.signature,
            "recorded_at": evidence["recorded_at"],
            "lease": {
                "scope_id": binding_lease["scope_id"],
                "owner_id": binding_lease["owner_id"],
                "plan_signature": binding_lease["plan_signature"],
                "binding_epoch": binding_lease["epoch"],
                "evidence_epoch": evidence["lease_epoch"],
                "token_sha256": binding_lease["token_sha256"],
                "acquired_at": binding_lease["acquired_at"],
                "expires_at": binding_lease["expires_at"],
            },
        },
        "physical_generation": {
            "dag_id": head.dag_id,
            "run_id": selected_generation.run_id,
            "scope_id": scope.scope_id,
            "generation_id": selected_generation.generation_id,
            "generation_signature": selected_generation.generation_signature,
            "manifest_sha256": selected_generation.manifest_sha256,
            "snapshot_ref": publication["snapshot_ref"],
            "registry_snapshot_ref": _ref_for_uri(
                selected_generation.registry_snapshot_uri
            ),
            "registry_signature": selected_generation.registry_signature,
            "plan_signature": selected_generation.plan_signature,
            "parser_version": selected_generation.parser_version,
            "runtime_version": selected_generation.runtime_version,
            "published_at": head.published_at.isoformat(),
            "completed_at": head.completed_at.isoformat(),
        },
        "selected_head": selected_head,
        "scope_binding_ref": publication["scope_binding_ref"],
        "run_evidence_ref": publication["evidence_ref"],
        "publication_ref": publication_ref,
        "published_dq_ref": result_ref,
    }
    attestation_ref = _write_payload(
        _join_uri(descriptor["scope_root"], "qualification-attestation.json"),
        attestation,
    )
    return {
        "published_dq_ref": result_ref,
        "qualification_attestation_ref": attestation_ref,
    }


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
        publication_context_by_scope = {}
        for wrapped in durable_manifest["publication_refs"]:
            publication_ref = wrapped["publication_ref"]
            publication = _read_ref(publication_ref, kind="espn-publication-result-v1")
            _, _, loaded_plan, scope, lease = _binding(publication["scope_binding_ref"])
            if scope.scope_id in publications_by_scope:
                raise OperationsError("durable manifest duplicates a publication")
            if (
                _qualification_state(publication["state"]),
                loaded_plan.plan.run_id,
                loaded_plan.attempt,
                loaded_plan.signature,
                loaded_plan.plan.registry_signature,
            ) != (
                publication["state"],
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
            publication_context_by_scope[scope.scope_id] = (
                loaded_plan,
                scope,
                lease,
                generation,
            )
        if set(publications_by_scope) != set(index["scope_ids"]):
            raise OperationsError("durable publication scope set is incomplete")
        durable = PostgresEspnControlStore.from_env().read_run_evidence(
            dag_id=dag_id, run_id=run_id, attempt=attempt
        )
        if [_evidence_dict(item) for item in durable] != durable_manifest["evidence"]:
            raise OperationsError("database current-run evidence differs from manifest")
        evidence_artifacts_by_scope = {}
        evidence_rows_by_scope = {}
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
            evidence_artifacts_by_scope[evidence.scope_id] = artifact_evidence
            evidence_rows_by_scope[evidence.scope_id] = evidence
        dq_by_scope = {}
        for descriptor_ref in index["scope_plan_refs"]:
            descriptor = _read_ref(descriptor_ref, kind="espn-scope-plan-descriptor-v1")
            wrapped = {
                "published_dq_ref": _ref_for_uri(
                    _join_uri(descriptor["scope_root"], "published-dq.json")
                ),
                "qualification_attestation_ref": _ref_for_uri(
                    _join_uri(
                        descriptor["scope_root"],
                        "qualification-attestation.json",
                    )
                ),
            }
            dq = _read_ref(
                wrapped["published_dq_ref"], kind="espn-published-dq-result-v1"
            )
            attestation = _read_ref(
                wrapped["qualification_attestation_ref"],
                kind="espn-scope-qualification-attestation-v1",
            )
            if dq["scope_id"] in dq_by_scope:
                raise OperationsError("published DQ duplicates a scope")
            qualification, _, physical = _parse_qualification_attestation(attestation)
            scope_id = dq["scope_id"]
            publication_ref, publication = publications_by_scope[scope_id]
            loaded_plan, scope, lease, generation = publication_context_by_scope[
                scope_id
            ]
            artifact_evidence = evidence_artifacts_by_scope[scope_id]
            evidence_row = evidence_rows_by_scope[scope_id]
            selected_head = publication["selected_head"]
            binding_lease = _lease_to_dict(lease)
            expected_physical = {
                "dag_id": selected_head["dag_id"],
                "run_id": generation.run_id,
                "scope_id": scope.scope_id,
                "generation_id": generation.generation_id,
                "generation_signature": generation.generation_signature,
                "manifest_sha256": generation.manifest_sha256,
                "snapshot_ref": publication["snapshot_ref"],
                "registry_snapshot_ref": _ref_for_uri(generation.registry_snapshot_uri),
                "registry_signature": generation.registry_signature,
                "plan_signature": generation.plan_signature,
                "parser_version": generation.parser_version,
                "runtime_version": generation.runtime_version,
                "published_at": selected_head["published_at"],
                "completed_at": selected_head["completed_at"],
            }
            if (
                attestation.get("published_dq_ref") != wrapped["published_dq_ref"]
                or attestation.get("publication_ref") != dq.get("publication_ref")
                or qualification.get("dag_id") != dq.get("dag_id")
                or qualification.get("run_id") != dq.get("run_id")
                or qualification.get("attempt") != dq.get("attempt")
                or qualification.get("scope_id") != dq.get("scope_id")
                or qualification.get("registry_signature")
                != dq.get("registry_signature")
                or qualification.get("plan_signature") != dq.get("plan_signature")
                or qualification.get("state") != publication.get("state")
                or qualification.get("recorded_at")
                != evidence_row.recorded_at.isoformat()
                or qualification.get("lease")
                != {
                    "scope_id": binding_lease["scope_id"],
                    "owner_id": binding_lease["owner_id"],
                    "plan_signature": binding_lease["plan_signature"],
                    "binding_epoch": binding_lease["epoch"],
                    "evidence_epoch": artifact_evidence["lease_epoch"],
                    "token_sha256": binding_lease["token_sha256"],
                    "acquired_at": binding_lease["acquired_at"],
                    "expires_at": binding_lease["expires_at"],
                }
                or attestation.get("scope_binding_ref")
                != publication.get("scope_binding_ref")
                or attestation.get("run_evidence_ref")
                != publication.get("evidence_ref")
                or attestation.get("selected_head") != dq.get("current_selection")
                or physical != expected_physical
            ):
                raise OperationsError(
                    "qualification attestation differs from published DQ"
                )
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
    )
    if state is None:
        return False, False
    if state.get("kind") == "espn-discovery-state-v1":
        review = _read_ref(state["review_ref"], kind="espn-discovery-review-v1")
        competition_id = scope_id.split(":", 1)[0]
        rollover = any(
            str(item).split(":", 1)[0] == competition_id
            for item in review["quarantined_scopes"]
        )
        return rollover, bool(review["unresolved_discovery_diffs"])
    if state.get("kind") == "espn-discovery-state-v2":
        _, _, review = _load_discovery_state_v2(state)
        return False, bool(review["unresolved_discovery_diffs"])
    raise OperationsError("existing discovery state schema is unsupported")


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


_QUALIFICATION_ATTESTATION_FIELDS = {
    "kind",
    "schema_version",
    "qualification",
    "physical_generation",
    "selected_head",
    "scope_binding_ref",
    "run_evidence_ref",
    "publication_ref",
    "published_dq_ref",
}
_QUALIFICATION_FIELDS = {
    "dag_id",
    "run_id",
    "attempt",
    "scope_id",
    "state",
    "registry_signature",
    "plan_signature",
    "recorded_at",
    "lease",
}
_QUALIFICATION_LEASE_FIELDS = {
    "scope_id",
    "owner_id",
    "plan_signature",
    "binding_epoch",
    "evidence_epoch",
    "token_sha256",
    "acquired_at",
    "expires_at",
}
_PHYSICAL_GENERATION_FIELDS = {
    "dag_id",
    "run_id",
    "scope_id",
    "generation_id",
    "generation_signature",
    "manifest_sha256",
    "snapshot_ref",
    "registry_snapshot_ref",
    "registry_signature",
    "plan_signature",
    "parser_version",
    "runtime_version",
    "published_at",
    "completed_at",
}
_SELECTED_HEAD_FIELDS = {
    "dag_id",
    "scope_id",
    "generation_id",
    "generation_signature",
    "manifest_sha256",
    "snapshot_uri",
    "snapshot_sha256",
    "registry_signature",
    "plan_signature",
    "run_id",
    "published_at",
    "completed_at",
}


def _exact_artifact_ref(value: object, *, label: str) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"uri", "sha256"}:
        raise OperationsError(f"{label} must contain only uri and sha256")
    _required(value["uri"], f"{label} uri")
    _sha(value["sha256"], f"{label} sha256")
    return value


def _parse_qualification_attestation(
    attestation: object,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]:
    """Parse the exact v1 qualification schema shared by both consumers."""

    if (
        not isinstance(attestation, Mapping)
        or set(attestation) != _QUALIFICATION_ATTESTATION_FIELDS
        or (attestation.get("kind"), attestation.get("schema_version"))
        != ("espn-scope-qualification-attestation-v1", 1)
    ):
        raise OperationsError("qualification attestation schema mismatch")
    qualification = attestation.get("qualification")
    lease = qualification.get("lease") if isinstance(qualification, Mapping) else None
    physical = attestation.get("physical_generation")
    selected_head = attestation.get("selected_head")
    if (
        not isinstance(qualification, Mapping)
        or set(qualification) != _QUALIFICATION_FIELDS
        or not isinstance(lease, Mapping)
        or set(lease) != _QUALIFICATION_LEASE_FIELDS
        or not isinstance(physical, Mapping)
        or set(physical) != _PHYSICAL_GENERATION_FIELDS
        or not isinstance(selected_head, Mapping)
        or set(selected_head) != _SELECTED_HEAD_FIELDS
    ):
        raise OperationsError("qualification attestation nested schema mismatch")
    for field in (
        "scope_binding_ref",
        "run_evidence_ref",
        "publication_ref",
        "published_dq_ref",
    ):
        _exact_artifact_ref(attestation[field], label=f"attestation {field}")
    _exact_artifact_ref(physical["snapshot_ref"], label="physical snapshot reference")
    _exact_artifact_ref(
        physical["registry_snapshot_ref"], label="physical registry reference"
    )
    return qualification, lease, physical


def _verified_complete_head(
    head: ScopeHead | None,
) -> tuple[ScopeHead | None, str]:
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
        if (
            head.completed_at is None
            or generation.ingested_at != head.completed_at
            or head.completed_at > head.published_at
        ):
            raise OperationsError("scope head completion timestamp mismatch")
        report = EspnBronzeRepository().verify_published_scope(generation)
        if not report.passed:
            raise OperationsError("scope head COMPLETE physical parity failed")
    except Exception:
        return None, "incomplete"
    return head, "complete"


def _qualified_freshness_at(
    head: ScopeHead | None,
    evidence: RunManifestEvidence | None,
    *,
    expected_registry_ref: Mapping[str, str],
    expected_registry_signature: str,
    observed_at: datetime,
) -> tuple[ScopeHead | None, str, datetime | None]:
    """Derive freshness without mutating the physical COMPLETE control head."""

    verified, state = _verified_complete_head(head)
    if verified is None:
        return None, state, None
    fallback = verified.published_at
    if evidence is None:
        return verified, state, fallback
    try:
        if (
            evidence.dag_id,
            evidence.scope_id,
            evidence.registry_signature,
            evidence.state,
        ) != (
            "dag_ingest_espn",
            verified.scope_id,
            expected_registry_signature,
            "noop",
        ):
            raise OperationsError("freshness evidence identity mismatch")
        if not (
            verified.published_at <= evidence.recorded_at <= observed_at.astimezone(UTC)
        ):
            raise OperationsError("freshness evidence timestamp is not current")
        evidence_ref = {
            "uri": evidence.evidence_uri,
            "sha256": evidence.evidence_sha256,
        }
        evidence_payload = _read_ref(evidence_ref, kind="espn-run-manifest-evidence-v1")
        if (
            evidence_payload.get("dag_id"),
            evidence_payload.get("run_id"),
            evidence_payload.get("attempt"),
            evidence_payload.get("scope_id"),
            evidence_payload.get("plan_signature"),
            evidence_payload.get("registry_signature"),
            evidence_payload.get("state"),
            evidence_payload.get("recorded_at"),
        ) != (
            evidence.dag_id,
            evidence.run_id,
            evidence.attempt,
            evidence.scope_id,
            evidence.plan_signature,
            evidence.registry_signature,
            evidence.state,
            evidence.recorded_at.isoformat(),
        ):
            raise OperationsError(
                "freshness evidence artifact differs from durable row"
            )
        parsed_evidence_uri = urlsplit(evidence.evidence_uri)
        if (
            parsed_evidence_uri.query
            or parsed_evidence_uri.fragment
            or parsed_evidence_uri.path.rsplit("/", 1)[-1] != "run-evidence.json"
        ):
            raise OperationsError("freshness evidence URI is not canonical")
        root = evidence.evidence_uri.rsplit("/", 1)[0]
        attestation = _read_ref(
            _ref_for_uri(_join_uri(root, "qualification-attestation.json")),
            kind="espn-scope-qualification-attestation-v1",
        )
        qualification, qualification_lease, physical = _parse_qualification_attestation(
            attestation
        )
        current_registry_ref = _exact_artifact_ref(
            expected_registry_ref, label="current registry reference"
        )
        publication_ref = attestation.get("publication_ref")
        published_dq_ref = attestation.get("published_dq_ref")
        publication = _read_ref(publication_ref, kind="espn-publication-result-v1")
        published_dq = _read_ref(published_dq_ref, kind="espn-published-dq-result-v1")
        selected_head = _head_to_dict(verified)
        _, descriptor, loaded, scope, lease = _binding(attestation["scope_binding_ref"])
        evidence_epoch = evidence_payload.get("lease_epoch")
        binding_lease = _lease_to_dict(lease)
        if (
            descriptor.get("dag_id"),
            loaded.plan.run_id,
            loaded.attempt,
            scope.scope_id,
            loaded.plan.registry_signature,
            loaded.signature,
            loaded.mode,
            loaded.replay_source,
            loaded.registry_snapshot_uri,
            lease.scope_id,
            lease.owner_id,
            lease.plan_signature,
            type(evidence_epoch) is int and 1 <= evidence_epoch <= lease.epoch,
        ) != (
            evidence.dag_id,
            evidence.run_id,
            evidence.attempt,
            evidence.scope_id,
            evidence.registry_signature,
            evidence.plan_signature,
            "daily",
            None,
            current_registry_ref["uri"],
            evidence.scope_id,
            f"{evidence.dag_id}/{evidence.run_id}/{evidence.attempt}",
            evidence.plan_signature,
            True,
        ):
            raise OperationsError("freshness signed binding identity mismatch")
        if (
            qualification.get("dag_id"),
            qualification.get("run_id"),
            qualification.get("attempt"),
            qualification.get("scope_id"),
            qualification.get("state"),
            qualification.get("registry_signature"),
            qualification.get("plan_signature"),
            qualification.get("recorded_at"),
            qualification_lease.get("scope_id"),
            qualification_lease.get("owner_id"),
            qualification_lease.get("plan_signature"),
            qualification_lease.get("binding_epoch"),
            qualification_lease.get("evidence_epoch"),
            qualification_lease.get("token_sha256"),
            qualification_lease.get("acquired_at"),
            qualification_lease.get("expires_at"),
            attestation.get("run_evidence_ref"),
            attestation.get("scope_binding_ref"),
            attestation.get("selected_head"),
            publication.get("evidence_ref"),
            publication.get("scope_binding_ref"),
            publication.get("state"),
            publication.get("snapshot_ref"),
            publication.get("selected_head"),
            published_dq.get("publication_ref"),
            published_dq.get("dag_id"),
            published_dq.get("run_id"),
            published_dq.get("attempt"),
            published_dq.get("scope_id"),
            published_dq.get("plan_signature"),
            published_dq.get("registry_signature"),
            published_dq.get("current_selection"),
            published_dq.get("quality", {}).get("passed"),
            published_dq.get("quality", {}).get("failures"),
        ) != (
            evidence.dag_id,
            evidence.run_id,
            evidence.attempt,
            evidence.scope_id,
            evidence.state,
            evidence.registry_signature,
            evidence.plan_signature,
            evidence.recorded_at.isoformat(),
            binding_lease["scope_id"],
            binding_lease["owner_id"],
            binding_lease["plan_signature"],
            binding_lease["epoch"],
            evidence_epoch,
            binding_lease["token_sha256"],
            binding_lease["acquired_at"],
            binding_lease["expires_at"],
            evidence_ref,
            attestation.get("scope_binding_ref"),
            selected_head,
            evidence_ref,
            attestation.get("scope_binding_ref"),
            evidence.state,
            {"uri": verified.snapshot_uri, "sha256": verified.snapshot_sha256},
            selected_head,
            publication_ref,
            evidence.dag_id,
            evidence.run_id,
            evidence.attempt,
            evidence.scope_id,
            evidence.plan_signature,
            evidence.registry_signature,
            selected_head,
            True,
            [],
        ):
            raise OperationsError("freshness qualification chain mismatch")
        if (
            evidence_payload.get("publication_intent_ref")
            != publication.get("publication_intent_ref")
            or publication.get("publication_intent_ref") is not None
        ):
            raise OperationsError("freshness no-op contains publication intent")
        prior = loaded.bindings[evidence.scope_id].prior
        if prior is None or (
            prior.uri,
            prior.artifact_sha256,
            prior.scope_id,
            prior.generation_id,
            prior.generation_signature,
            prior.manifest_sha256,
        ) != (
            verified.snapshot_uri,
            verified.snapshot_sha256,
            verified.scope_id,
            verified.generation_id,
            verified.generation_signature,
            verified.manifest_sha256,
        ):
            raise OperationsError("freshness signed prior differs from physical head")
        generation = runner.load_scope_snapshot(
            verified.snapshot_uri,
            artifact_sha256=verified.snapshot_sha256,
            expected_scope_id=verified.scope_id,
        )
        if generation.plan != scope:
            raise OperationsError(
                "freshness physical scope plan differs from signed plan"
            )
        _validate_evidence_payload(
            evidence_payload,
            dag_id=evidence.dag_id,
            loaded=loaded,
            scope=scope,
            state="noop",
            generation=generation,
            lease=lease,
            recorded_at=evidence.recorded_at,
            publication_intent_ref=None,
        )
        snapshot_ref = {
            "uri": verified.snapshot_uri,
            "sha256": verified.snapshot_sha256,
        }
        registry_ref = _ref_for_uri(generation.registry_snapshot_uri)
        if (
            evidence_payload.get("generation_id"),
            evidence_payload.get("generation_signature"),
            evidence_payload.get("manifest_sha256"),
            physical.get("run_id"),
            physical.get("plan_signature"),
            physical.get("generation_id"),
            physical.get("generation_signature"),
            physical.get("manifest_sha256"),
            physical.get("registry_signature"),
            physical.get("parser_version"),
            physical.get("runtime_version"),
            physical.get("dag_id"),
            physical.get("scope_id"),
            physical.get("snapshot_ref"),
            physical.get("registry_snapshot_ref"),
            physical.get("published_at"),
            physical.get("completed_at"),
            generation.registry_snapshot_uri,
            generation.registry_signature,
            generation.parser_version,
            generation.runtime_version,
            generation.ingested_at,
            loaded.registry_snapshot_uri,
            registry_ref,
        ) != (
            verified.generation_id,
            verified.generation_signature,
            verified.manifest_sha256,
            generation.run_id,
            generation.plan_signature,
            generation.generation_id,
            generation.generation_signature,
            generation.manifest_sha256,
            generation.registry_signature,
            runner.PARSER_VERSION,
            runner.RUNTIME_VERSION,
            verified.dag_id,
            verified.scope_id,
            snapshot_ref,
            current_registry_ref,
            verified.published_at.isoformat(),
            verified.completed_at.isoformat(),
            current_registry_ref["uri"],
            expected_registry_signature,
            runner.PARSER_VERSION,
            runner.RUNTIME_VERSION,
            verified.completed_at,
            current_registry_ref["uri"],
            current_registry_ref,
        ):
            raise OperationsError("freshness physical generation mismatch")
    except Exception:
        return verified, state, fallback
    return verified, state, evidence.recorded_at


def _latest_daily_evidence_or_empty(
    store: Any, scope_ids: tuple[str, ...]
) -> dict[str, RunManifestEvidence]:
    """Read one unambiguous latest daily row per already-frozen scope."""

    reader = getattr(store, "read_latest_run_evidence_by_scope", None)
    if not callable(reader):
        return {}
    try:
        return reader(scope_ids, dag_id="dag_ingest_espn")
    except OperationsError:
        return {}


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
        )
        if admission is not None:
            admission = _admission_payload(admission)
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
        scope_ids = tuple(index["scope_ids"])
        if scope_ids != tuple(sorted(set(scope_ids))):
            raise OperationsError("health scope set must be sorted and unique")
        heads = store.read_scope_heads(scope_ids)
        latest_daily_evidence = _latest_daily_evidence_or_empty(store, scope_ids)
        admission_ref = _ref_for_uri(
            _join_uri(
                _artifact_root(),
                "runs",
                _run_key(verdict["dag_id"], verdict["run_id"]),
                "admission.json",
            )
        )
        admission = _read_admission_ref(admission_ref)
        if (
            admission["dag_id"],
            admission["run_id"],
            admission["attempt"],
            admission["scope_ids"],
            admission["registry_signature"],
        ) != (
            verdict["dag_id"],
            verdict["run_id"],
            verdict["attempt"],
            list(scope_ids),
            index["registry_signature"],
        ):
            raise OperationsError("health admission identity mismatch")
        registry_ref = _exact_artifact_ref(
            admission["registry_ref"], label="health registry reference"
        )
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
            head, head_state, freshness_at = _qualified_freshness_at(
                raw_head,
                latest_daily_evidence.get(scope_id),
                expected_registry_ref=registry_ref,
                expected_registry_signature=index["registry_signature"],
                observed_at=now,
            )
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
                        "last_complete_at": freshness_at,
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
    admission = _read_admission_ref(admission_ref)
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
        DISCOVERY_CATALOG_URL,
    )
    if url != DISCOVERY_CATALOG_URL:
        raise OperationsError(
            "ESPN discovery catalog URL must be the authoritative Core catalog"
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


def _normalize_discovery_dropdown(
    document: object, *, allow_normalized: bool = False
) -> Mapping[str, Any]:
    """Normalize the authoritative Core shape, or an explicit internal bundle."""

    if not isinstance(document, Mapping):
        raise OperationsError("discovery catalog must be an object")
    core_keys = {"count", "pageCount", "items"}
    if core_keys.intersection(document):
        count = document.get("count")
        page_count = document.get("pageCount")
        items = document.get("items")
        if (
            type(count) is not int
            or not isinstance(items, list)
            or count != len(items)
            or count < 1
            or type(page_count) is not int
            or page_count != 1
        ):
            raise OperationsError(
                "discovery Core catalog must provide one complete page"
            )
        leagues = []
        references = set()
        slugs = set()
        for item in items:
            if not isinstance(item, Mapping) or set(item) != {"$ref"}:
                raise OperationsError("discovery Core catalog item must be a $ref")
            reference = item["$ref"]
            if not isinstance(reference, str) or reference in references:
                raise OperationsError(
                    "discovery Core catalog references must be unique URLs"
                )
            try:
                parsed = urlsplit(reference)
                port = parsed.port
            except ValueError as exc:
                raise OperationsError(
                    "discovery Core catalog reference is malformed"
                ) from exc
            if (
                parsed.scheme not in {"http", "https"}
                or parsed.hostname != "sports.core.api.espn.com"
                or parsed.username is not None
                or parsed.password is not None
                or port is not None
                or parsed.fragment
            ):
                raise OperationsError("discovery Core catalog reference is not ESPN")
            match = re.fullmatch(
                r"/v2/sports/soccer/leagues/([A-Za-z0-9._-]+)", parsed.path
            )
            if match is None:
                raise OperationsError(
                    "discovery Core catalog reference has no exact league slug"
                )
            slug = match.group(1)
            if slug in slugs:
                raise OperationsError("discovery Core catalog has duplicate slugs")
            references.add(reference)
            slugs.add(slug)
            leagues.append({"id": None, "slug": slug, "name": slug})
        return {"leagues": leagues}
    if (
        allow_normalized
        and set(document) == {"leagues"}
        and isinstance(document.get("leagues"), list)
    ):
        return document
    raise OperationsError(
        "discovery production policy requires an authoritative Core catalog"
    )


def _normalized_discovery_competitions(
    document: object,
) -> tuple[tuple[int | None, str, str], ...]:
    """Return the exact unique identities represented by one Core catalog."""

    from scrapers.espn.discovery import parse_soccer_dropdown

    dropdown = _normalize_discovery_dropdown(document)
    competitions = tuple(
        (candidate.espn_id, candidate.slug, candidate.name)
        for candidate in parse_soccer_dropdown(dropdown)
    )
    if not competitions:
        raise OperationsError("discovery catalog contains no competitions")
    slugs = tuple(identity[1] for identity in competitions)
    if len(competitions) != len(set(competitions)) or len(slugs) != len(set(slugs)):
        raise OperationsError("discovery Core catalog identities must be unique")
    if len(competitions) > MAX_DISCOVERY_COMPETITIONS:
        raise OperationsError(
            "discovery competition cap exceeded: "
            f"{len(competitions)} > {MAX_DISCOVERY_COMPETITIONS}"
        )
    return competitions


def plan_discovery_detail_batches(
    *, discovery_raw_ref: Mapping[str, str], **context
) -> dict[str, Any]:
    """Offline-plan deterministic unique competition detail batches of at most 20."""

    raw = _read_ref(discovery_raw_ref, kind="espn-discovery-raw-v1")
    body = EspnRawStore.from_uri(_raw_store_uri()).load_exact(
        raw["raw_uri"], raw["raw_sha256"]
    )
    try:
        dropdown = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("discovery catalog is not valid JSON") from exc
    competitions = _normalized_discovery_competitions(dropdown)
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
    expected_requests = _discovery_detail_request_count(
        summary_uri=summary_uri,
        summary_sha256=summary_sha256,
        summary_event_id=summary_event_id,
    )
    if summary_uri is not None:
        uri = _required(summary_uri, "discovery Summary raw uri")
        digest = _sha(summary_sha256, "discovery Summary raw sha256")
        raw_store.load_exact(uri, digest)
    if detail["request_count"] != expected_requests:
        raise OperationsError("discovery detail checkpoint request count mismatch")
    for field in ("direct_bytes", "proxy_bytes"):
        if type(detail[field]) is not int or detail[field] < 0:
            raise OperationsError(
                f"discovery detail checkpoint {field} must be non-negative"
            )
    return detail


def _discovery_detail_request_count(
    *, summary_uri: object, summary_sha256: object, summary_event_id: object
) -> int:
    """Validate the three backward-compatible Summary evidence states."""

    if (summary_uri is None) != (summary_sha256 is None):
        raise OperationsError("discovery Summary checkpoint is incomplete")
    if summary_event_id is None:
        if summary_uri is not None:
            raise OperationsError("discovery Summary checkpoint is incomplete")
        return 2
    if (
        not isinstance(summary_event_id, str)
        or not summary_event_id.isdecimal()
        or summary_event_id.startswith("0")
    ):
        raise OperationsError("discovery Summary checkpoint event id is invalid")
    return 3


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
        missing_summary_request = None
        if event_id is not None:
            try:
                summary = client.fetch_json(
                    summary_template.format(
                        slug=item["slug"],
                        espn_id=item["espn_id"],
                        event_id=event_id,
                    ),
                    EndpointType.SUMMARY,
                    {"event": event_id},
                    competition_id=identity,
                    event_id=event_id,
                    force_refresh=True,
                )
            except HttpStatusError as exc:
                if exc.status != 404:
                    raise
                if exc.ledger_entry is None:
                    raise OperationsError(
                        "missing discovery Summary 404 request ledger"
                    ) from exc
                missing_summary_request = exc.ledger_entry
        fetched_items = (metadata, scoreboard) + (
            (summary,)
            if summary is not None
            else (() if missing_summary_request is None else (missing_summary_request,))
        )
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
    authoritative_name = _required(
        metadata_document.get("name") or metadata_document.get("displayName"),
        "discovery metadata name",
    )
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
        name=authoritative_name,
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
    dropdown = _normalize_discovery_dropdown(
        document.get("dropdown", document), allow_normalized=True
    )
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
        "kind": "espn-discovery-state-v2",
        "schema_version": 2,
        "candidate_ref": review["candidate_ref"],
        "candidate_signature": review["candidate_signature"],
        "review_ref": dict(review_ref),
        "male_registry_ref": review["male_registry_ref"],
        "male_registry_signature": review["male_registry_signature"],
        "male_scope_count": review["male_scope_count"],
        "selection_policy": review["selection_policy"],
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


def _load_discovery_state_v2(
    state: object,
) -> tuple[Mapping[str, Any], Registry, Mapping[str, Any]]:
    required_keys = {
        "kind",
        "schema_version",
        "candidate_ref",
        "candidate_signature",
        "review_ref",
        "male_registry_ref",
        "male_registry_signature",
        "male_scope_count",
        "selection_policy",
        "observed_at",
    }
    if (
        not isinstance(state, Mapping)
        or set(state) != required_keys
        or state.get("kind") != "espn-discovery-state-v2"
        or state.get("schema_version") != 2
        or state.get("selection_policy") != DISCOVERY_SELECTION_POLICY
        or type(state.get("male_scope_count")) is not int
        or state["male_scope_count"] < 1
    ):
        raise OperationsError("discovery state v2 checkpoint mismatch")
    candidate_ref = _discovery_artifact_ref(
        state["candidate_ref"], label="discovery state candidate reference"
    )
    review_ref = _discovery_artifact_ref(
        state["review_ref"], label="discovery state review reference"
    )
    male_registry_ref = _discovery_artifact_ref(
        state["male_registry_ref"], label="discovery state male registry reference"
    )
    candidate_signature = _sha(
        state["candidate_signature"], "discovery state candidate signature"
    )
    male_registry_signature = _sha(
        state["male_registry_signature"],
        "discovery state male registry signature",
    )
    _discovery_observed_at(state["observed_at"], label="discovery state observed_at")

    review = _read_ref(review_ref, kind="espn-discovery-review-v2")
    projection_fields = {
        "candidate_ref",
        "candidate_signature",
        "male_registry_ref",
        "male_registry_signature",
        "male_scope_count",
        "selection_policy",
        "observed_at",
    }
    if any(state[field] != review.get(field) for field in projection_fields):
        raise OperationsError("discovery state/review projection mismatch")
    review_index_ref = review.get("discovery_detail_index_ref")
    review_phase_refs = review.get("discovery_detail_phase_refs")
    if not isinstance(review_index_ref, Mapping) or not isinstance(
        review_phase_refs, list
    ):
        raise OperationsError("discovery state review input references are invalid")
    _validate_discovery_review_checkpoint(
        review,
        discovery_detail_index_ref=review_index_ref,
        discovery_detail_phase_refs=review_phase_refs,
    )

    from scrapers.espn.discovery import CatalogSnapshot

    candidate = _read_ref(candidate_ref)
    if CatalogSnapshot.from_dict(candidate).signature() != candidate_signature:
        raise OperationsError("discovery state candidate signature mismatch")
    male_registry = validate_registry_document(_read_ref(male_registry_ref))
    if male_registry.signature() != male_registry_signature:
        raise OperationsError("discovery state male registry signature mismatch")
    if len(male_registry.promoted) != state["male_scope_count"]:
        raise OperationsError("discovery state male registry count mismatch")
    return candidate, male_registry, review


def _validate_discovery_review_checkpoint(
    review: object,
    *,
    discovery_detail_index_ref: Mapping[str, str],
    discovery_detail_phase_refs: Sequence[Mapping[str, str]],
    candidate_payload: Mapping[str, Any] | None = None,
    male_registry_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    required_keys = {
        "kind",
        "schema_version",
        "discovery_detail_index_ref",
        "discovery_detail_phase_refs",
        "candidate_ref",
        "candidate_signature",
        "male_registry_ref",
        "male_registry_signature",
        "male_scope_count",
        "selection_policy",
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
        or review.get("kind") != "espn-discovery-review-v2"
        or review.get("schema_version") != 2
        or review.get("discovery_detail_index_ref") != discovery_detail_index_ref
        or review.get("discovery_detail_phase_refs")
        != list(discovery_detail_phase_refs)
        or review.get("promotion_performed") is not False
        or review.get("unresolved_discovery_diffs") is not False
        or review.get("quarantined_scopes") != []
        or review.get("selection_policy") != DISCOVERY_SELECTION_POLICY
        or type(review.get("male_scope_count")) is not int
        or review["male_scope_count"] < 1
        or not isinstance(review.get("changes"), list)
        or type(review.get("change_count")) is not int
        or review["change_count"] != len(review["changes"])
        or not isinstance(review.get("alerts"), list)
    ):
        raise OperationsError("existing discovery review checkpoint mismatch")
    candidate_ref = _discovery_artifact_ref(
        review["candidate_ref"], label="discovery candidate reference"
    )
    candidate = (
        _read_ref(candidate_ref) if candidate_payload is None else candidate_payload
    )
    if (
        candidate_payload is not None
        and _payload_ref(candidate_ref["uri"], candidate) != candidate_ref
    ):
        raise OperationsError("sealed discovery candidate reference mismatch")
    candidates = candidate.get("candidates")
    if not isinstance(candidates, list):
        raise OperationsError("existing discovery candidate checkpoint mismatch")
    from scrapers.espn.discovery import CatalogSnapshot

    candidate_signature = CatalogSnapshot.from_dict(candidate).signature()
    if review.get("candidate_signature") != candidate_signature:
        raise OperationsError("existing discovery candidate signature mismatch")
    male_registry_ref = _discovery_artifact_ref(
        review["male_registry_ref"], label="discovered male registry reference"
    )
    male_registry = validate_registry_document(
        _read_ref(male_registry_ref)
        if male_registry_payload is None
        else male_registry_payload
    )
    if (
        male_registry_payload is not None
        and _payload_ref(male_registry_ref["uri"], male_registry_payload)
        != male_registry_ref
    ):
        raise OperationsError("sealed discovered male registry reference mismatch")
    if review.get("male_registry_signature") != male_registry.signature():
        raise OperationsError("existing discovered male registry signature mismatch")
    if review["male_scope_count"] != len(male_registry.promoted):
        raise OperationsError("existing discovered male registry scope count mismatch")
    _discovery_observed_at(review.get("observed_at"), label="discovery observed_at")
    return review


def _validate_discovery_reducer_checkpoint(
    checkpoint: object,
    *,
    discovery_detail_index_ref: Mapping[str, str],
    discovery_detail_phase_refs: Sequence[Mapping[str, str]],
    candidate_uri: str,
    male_registry_uri: str,
    review_uri: str,
    discovery_state_uri: str,
) -> dict[str, Any]:
    v1_keys = {
        "kind",
        "schema_version",
        "discovery_detail_index_ref",
        "discovery_detail_phase_refs",
        "candidate_payload",
        "candidate_ref",
        "candidate_signature",
        "male_registry_payload",
        "male_registry_ref",
        "male_registry_signature",
        "male_scope_count",
        "review_payload",
        "review_ref",
        "observed_at",
    }
    v2_keys = v1_keys | {"discovery_state_payload", "discovery_state_ref"}
    if not isinstance(checkpoint, dict):
        raise OperationsError("discovery reducer checkpoint mismatch")
    version = (checkpoint.get("kind"), checkpoint.get("schema_version"))
    if version == ("espn-discovery-reducer-checkpoint-v1", 1):
        required_keys = v1_keys
    elif version == ("espn-discovery-reducer-checkpoint-v2", 2):
        required_keys = v2_keys
    else:
        raise OperationsError("discovery reducer checkpoint mismatch")
    if (
        set(checkpoint) != required_keys
        or checkpoint.get("discovery_detail_index_ref") != discovery_detail_index_ref
        or checkpoint.get("discovery_detail_phase_refs")
        != list(discovery_detail_phase_refs)
        or not isinstance(checkpoint.get("candidate_payload"), Mapping)
        or not isinstance(checkpoint.get("male_registry_payload"), Mapping)
        or not isinstance(checkpoint.get("review_payload"), Mapping)
    ):
        raise OperationsError("discovery reducer checkpoint mismatch")
    candidate_ref = _discovery_artifact_ref(
        checkpoint["candidate_ref"], label="sealed candidate reference"
    )
    male_registry_ref = _discovery_artifact_ref(
        checkpoint["male_registry_ref"], label="sealed male registry reference"
    )
    review_ref = _discovery_artifact_ref(
        checkpoint["review_ref"], label="sealed review reference"
    )
    if (
        candidate_ref["uri"] != candidate_uri
        or male_registry_ref["uri"] != male_registry_uri
        or review_ref["uri"] != review_uri
        or _payload_ref(candidate_uri, checkpoint["candidate_payload"]) != candidate_ref
        or _payload_ref(male_registry_uri, checkpoint["male_registry_payload"])
        != male_registry_ref
        or _payload_ref(review_uri, checkpoint["review_payload"]) != review_ref
    ):
        raise OperationsError("discovery reducer checkpoint artifact binding mismatch")
    review = _validate_discovery_review_checkpoint(
        checkpoint["review_payload"],
        discovery_detail_index_ref=discovery_detail_index_ref,
        discovery_detail_phase_refs=discovery_detail_phase_refs,
        candidate_payload=checkpoint["candidate_payload"],
        male_registry_payload=checkpoint["male_registry_payload"],
    )
    if (
        checkpoint["candidate_ref"] != review["candidate_ref"]
        or checkpoint["candidate_signature"] != review["candidate_signature"]
        or checkpoint["male_registry_ref"] != review["male_registry_ref"]
        or checkpoint["male_registry_signature"] != review["male_registry_signature"]
        or checkpoint["male_scope_count"] != review["male_scope_count"]
        or checkpoint["observed_at"] != review["observed_at"]
    ):
        raise OperationsError("discovery reducer checkpoint review binding mismatch")
    expected_state = _discovery_review_state(review, review_ref=review_ref)
    normalized = dict(checkpoint)
    if version == ("espn-discovery-reducer-checkpoint-v2", 2):
        state_ref = _discovery_artifact_ref(
            checkpoint["discovery_state_ref"],
            label="sealed discovery state reference",
        )
        if (
            not isinstance(checkpoint.get("discovery_state_payload"), Mapping)
            or checkpoint["discovery_state_payload"] != expected_state
            or state_ref["uri"] != discovery_state_uri
            or _payload_ref(discovery_state_uri, checkpoint["discovery_state_payload"])
            != state_ref
        ):
            raise OperationsError("discovery reducer checkpoint state binding mismatch")
    else:
        # A pre-v2 reducer checkpoint sealed the complete review projection.
        # Derive the state deterministically so interrupted rollout runs remain
        # replayable while every new checkpoint seals the state explicitly.
        normalized["discovery_state_payload"] = expected_state
        normalized["discovery_state_ref"] = _payload_ref(
            discovery_state_uri, expected_state
        )
    return normalized


def _replay_discovery_reducer_checkpoint(
    checkpoint: object,
    *,
    discovery_detail_index_ref: Mapping[str, str],
    discovery_detail_phase_refs: Sequence[Mapping[str, str]],
    candidate_uri: str,
    male_registry_uri: str,
    review_uri: str,
    discovery_state_uri: str,
    latest_state_uri: str,
) -> dict[str, Any]:
    sealed = _validate_discovery_reducer_checkpoint(
        checkpoint,
        discovery_detail_index_ref=discovery_detail_index_ref,
        discovery_detail_phase_refs=discovery_detail_phase_refs,
        candidate_uri=candidate_uri,
        male_registry_uri=male_registry_uri,
        review_uri=review_uri,
        discovery_state_uri=discovery_state_uri,
    )
    candidate_ref = _write_payload(candidate_uri, sealed["candidate_payload"])
    male_registry_ref = _write_payload(
        male_registry_uri, sealed["male_registry_payload"]
    )
    review_ref = _write_payload(review_uri, sealed["review_payload"])
    discovery_state_ref = _write_payload(
        discovery_state_uri, sealed["discovery_state_payload"]
    )
    if (
        candidate_ref != sealed["candidate_ref"]
        or male_registry_ref != sealed["male_registry_ref"]
        or review_ref != sealed["review_ref"]
        or discovery_state_ref != sealed["discovery_state_ref"]
    ):
        raise OperationsError("discovery reducer replay reference mismatch")
    _publish_latest_discovery_state(
        latest_state_uri,
        sealed["discovery_state_payload"],
        state_ref=discovery_state_ref,
    )
    return {
        "discovery_state_ref": discovery_state_ref,
        "discovery_review_ref": review_ref,
        "male_registry_ref": male_registry_ref,
    }


def _publish_latest_discovery_state(
    latest_state_uri: str,
    state: Mapping[str, Any],
    *,
    state_ref: Mapping[str, str],
) -> None:
    sealed_ref = _discovery_artifact_ref(
        state_ref, label="immutable discovery state reference"
    )
    if sealed_ref["sha256"] != hashlib.sha256(_canonical_bytes(state)).hexdigest():
        raise OperationsError("immutable discovery state payload hash mismatch")
    incoming_time = _discovery_observed_at(
        state.get("observed_at"), label="discovery state observed_at"
    )
    existing = _optional_payload(latest_state_uri)
    if existing is not None:
        if existing.get("kind") not in {
            "espn-discovery-state-v1",
            "espn-discovery-state-v2",
        }:
            raise OperationsError("existing discovery state schema is unsupported")
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


def publish_discovered_male_registry(
    *,
    discovery_detail_index_ref: Mapping[str, str],
    discovery_detail_phase_refs: Sequence[Mapping[str, Any]] | None,
    **context,
) -> dict[str, str]:
    """Persist the exact candidate, generated MALE registry and review pointer."""

    from scrapers.espn.discovery import (
        CatalogSnapshot,
        diff_catalogs,
    )

    dag_id, run_id, _ = _run_identity(context)
    root = _join_uri(_artifact_root(), "discovery", _run_key(dag_id, run_id))
    candidate_uri = _join_uri(root, "candidate.json")
    male_registry_uri = _join_uri(root, "male-registry.json")
    review_uri = _join_uri(root, "reviewable-diff.json")
    discovery_state_uri = _join_uri(root, "discovery-state.json")
    checkpoint_uri = _join_uri(root, "reducer-checkpoint.json")
    latest_state_uri = _join_uri(_artifact_root(), "discovery", "latest-state.json")
    canonical_index_ref = _discovery_artifact_ref(
        discovery_detail_index_ref,
        label="discovery detail index reference",
    )
    existing_checkpoint = _optional_payload(checkpoint_uri)
    if existing_checkpoint is not None:
        committed_phase_refs = existing_checkpoint.get("discovery_detail_phase_refs")
        if not isinstance(committed_phase_refs, list) or not committed_phase_refs:
            raise OperationsError("existing discovery reducer phase set is invalid")
        discovery_detail_phase_refs = _exact_mapped_results(
            discovery_detail_phase_refs,
            expected_count=len(committed_phase_refs),
            label="discovery detail phase",
        )
        canonical_phase_refs = _discovery_phase_input_refs(discovery_detail_phase_refs)
        return _replay_discovery_reducer_checkpoint(
            existing_checkpoint,
            discovery_detail_index_ref=canonical_index_ref,
            discovery_detail_phase_refs=canonical_phase_refs,
            candidate_uri=candidate_uri,
            male_registry_uri=male_registry_uri,
            review_uri=review_uri,
            discovery_state_uri=discovery_state_uri,
            latest_state_uri=latest_state_uri,
        )
    existing_review = _optional_payload(review_uri)
    if existing_review is not None:
        committed_phase_refs = existing_review.get("discovery_detail_phase_refs")
        if not isinstance(committed_phase_refs, list) or not committed_phase_refs:
            raise OperationsError("existing discovery review phase set is invalid")
        discovery_detail_phase_refs = _exact_mapped_results(
            discovery_detail_phase_refs,
            expected_count=len(committed_phase_refs),
            label="discovery detail phase",
        )
        canonical_phase_refs = _discovery_phase_input_refs(discovery_detail_phase_refs)
        review = _validate_discovery_review_checkpoint(
            existing_review,
            discovery_detail_index_ref=canonical_index_ref,
            discovery_detail_phase_refs=canonical_phase_refs,
        )
        review_ref = _ref_for_uri(review_uri)
        state = _discovery_review_state(review, review_ref=review_ref)
        discovery_state_ref = _write_payload(discovery_state_uri, state)
        _publish_latest_discovery_state(
            latest_state_uri,
            state,
            state_ref=discovery_state_ref,
        )
        return {
            "discovery_state_ref": discovery_state_ref,
            "discovery_review_ref": review_ref,
            "male_registry_ref": review["male_registry_ref"],
        }

    detail_index = _read_ref(canonical_index_ref, kind="espn-discovery-detail-index-v1")
    competition_count = detail_index.get("competition_count")
    batch_ids = detail_index.get("batch_ids")
    batch_refs = detail_index.get("batch_refs")
    expected_batch_count = (
        (competition_count + DISCOVERY_DETAIL_BATCH_SIZE - 1)
        // DISCOVERY_DETAIL_BATCH_SIZE
        if type(competition_count) is int and competition_count > 0
        else 0
    )
    if (
        detail_index.get("competition_cap") != MAX_DISCOVERY_COMPETITIONS
        or detail_index.get("detail_request_cap") != MAX_DISCOVERY_DETAIL_REQUESTS
        or type(competition_count) is not int
        or not 1 <= competition_count <= MAX_DISCOVERY_COMPETITIONS
        or not isinstance(batch_ids, list)
        or not isinstance(batch_refs, list)
        or not all(isinstance(batch_id, str) and batch_id for batch_id in batch_ids)
        or len(batch_ids) != expected_batch_count
        or len(batch_refs) != expected_batch_count
        or len(batch_ids) != len(set(batch_ids))
    ):
        raise OperationsError("discovery detail index violates the static request cap")
    discovery_detail_phase_refs = _exact_mapped_results(
        discovery_detail_phase_refs,
        expected_count=expected_batch_count,
        label="discovery detail phase",
    )
    canonical_phase_refs = _discovery_phase_input_refs(discovery_detail_phase_refs)
    raw = _read_ref(detail_index["discovery_raw_ref"], kind="espn-discovery-raw-v1")
    body = EspnRawStore.from_uri(_raw_store_uri()).load_exact(
        raw["raw_uri"], raw["raw_sha256"]
    )
    try:
        dropdown = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsError("discovery catalog is not valid JSON") from exc
    catalog_competitions = _normalized_discovery_competitions(dropdown)
    indexed_batches = {}
    indexed_competitions = []
    for batch_ref in detail_index["batch_refs"]:
        batch = _read_ref(batch_ref, kind="espn-discovery-detail-batch-v1")
        batch_id = batch.get("batch_id")
        batch_competitions = batch.get("competitions")
        if not isinstance(batch_id, str) or not batch_id:
            raise OperationsError("discovery detail batch identity is invalid")
        if batch_id in indexed_batches:
            raise OperationsError("discovery detail index duplicates a batch")
        if batch.get("discovery_raw_ref") != detail_index["discovery_raw_ref"]:
            raise OperationsError("discovery detail batch raw identity mismatch")
        if (
            not isinstance(batch_competitions, list)
            or not 1 <= len(batch_competitions) <= DISCOVERY_DETAIL_BATCH_SIZE
        ):
            raise OperationsError("discovery detail batch coverage is invalid")
        for item in batch_competitions:
            if (
                not isinstance(item, Mapping)
                or set(item) != {"espn_id", "slug", "name"}
                or (
                    item["espn_id"] is not None
                    and (type(item["espn_id"]) is not int or item["espn_id"] <= 0)
                )
                or not isinstance(item["slug"], str)
                or not item["slug"]
                or not isinstance(item["name"], str)
                or not item["name"]
            ):
                raise OperationsError("discovery detail batch identity is invalid")
            indexed_competitions.append((item["espn_id"], item["slug"], item["name"]))
        indexed_batches[batch_id] = (batch_ref, batch)
    if set(indexed_batches) != set(detail_index["batch_ids"]):
        raise OperationsError("discovery detail index batch references are incomplete")
    catalog_slugs = {identity[1] for identity in catalog_competitions}
    indexed_slugs = {identity[1] for identity in indexed_competitions}
    if (
        detail_index["competition_count"] != len(catalog_competitions)
        or len(indexed_competitions) != len(catalog_competitions)
        or len(indexed_competitions) != len(set(indexed_competitions))
        or set(indexed_competitions) != set(catalog_competitions)
        or indexed_slugs != catalog_slugs
        or len(indexed_slugs) != len(indexed_competitions)
    ):
        raise OperationsError(
            "discovery raw catalog coverage differs from signed detail index"
        )
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
            if item["slug"] in details_by_slug:
                raise OperationsError("discovery detail slug was fetched twice")
            summary_uri = item.get("summary_raw_uri")
            summary_sha256 = item.get("summary_raw_sha256")
            summary_event_id = item.get("summary_event_id")
            expected_request_count = _discovery_detail_request_count(
                summary_uri=summary_uri,
                summary_sha256=summary_sha256,
                summary_event_id=summary_event_id,
            )
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
    if len(seen_ids) != len(catalog_competitions) or len(details_by_slug) != len(
        catalog_competitions
    ):
        raise OperationsError("discovery detail competition count mismatch")
    if detail_request_count > detail_index["detail_request_cap"]:
        raise OperationsError("discovery detail request cap exceeded")
    current = _discovery_snapshot(
        {"dropdown": dropdown, "details_by_slug": details_by_slug},
        captured_at=raw["captured_at"],
    )
    previous_state = _optional_payload(latest_state_uri)
    if previous_state is not None and previous_state.get("kind") not in {
        "espn-discovery-state-v1",
        "espn-discovery-state-v2",
    }:
        raise OperationsError("existing discovery state schema is unsupported")
    previous_uri = os.environ.get("ESPN_DISCOVERY_PREVIOUS_URI")
    registry_path = Path(
        os.environ.get(REGISTRY_ENV, "/opt/airflow/configs/espn/registry.yaml")
    )
    legacy_registry = load_registry(registry_path)
    previous_registry = None
    previous_candidate = None
    if previous_state is not None and previous_state.get("kind") == (
        "espn-discovery-state-v2"
    ):
        previous_candidate, previous_registry, _ = _load_discovery_state_v2(
            previous_state
        )
    if previous_uri:
        previous_document = json.loads(
            runner._read_artifact(previous_uri).decode("utf-8")
        )
        previous = CatalogSnapshot.from_dict(previous_document)
    elif previous_candidate is not None:
        previous = CatalogSnapshot.from_dict(previous_candidate)
    elif previous_state is not None:
        previous = CatalogSnapshot.from_dict(_read_ref(previous_state["candidate_ref"]))
    else:
        previous = _promoted_registry_baseline(current, legacy_registry)
    diff = diff_catalogs(previous, current)
    candidate_payload = json.loads(current.canonical_json())
    candidate_ref = _payload_ref(candidate_uri, candidate_payload)
    candidate_signature = current.signature()
    male_registry = build_discovered_male_registry(
        current,
        legacy_registry=legacy_registry,
        previous_registry=previous_registry,
    )
    male_registry_payload = json.loads(male_registry.canonical_json())
    male_registry_ref = _payload_ref(male_registry_uri, male_registry_payload)
    male_registry_signature = male_registry.signature()
    male_scope_count = len(male_registry.promoted)
    observed_at = PostgresEspnControlStore.from_env().current_time()
    alert_scopes = [
        competition.scope_id(competition.current_edition)
        for competition in male_registry.promoted[:1]
    ]
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
                    "identity_sha256": candidate_signature,
                    "state": "complete",
                    "last_complete_at": observed_at,
                    "direct_requests": 1 + detail_request_count,
                    "request_budget": 1 + MAX_DISCOVERY_DETAIL_REQUESTS,
                    "proxy_bytes": raw["proxy_bytes"] + detail_proxy_bytes,
                    "lease_conflict": False,
                    "unpromoted_current_season": False,
                    "unresolved_discovery_diffs": False,
                },
                observed_at=observed_at,
            )
        )
    review = {
        "kind": "espn-discovery-review-v2",
        "schema_version": 2,
        "discovery_detail_index_ref": canonical_index_ref,
        "discovery_detail_phase_refs": canonical_phase_refs,
        "candidate_ref": candidate_ref,
        "candidate_signature": candidate_signature,
        "male_registry_ref": male_registry_ref,
        "male_registry_signature": male_registry_signature,
        "male_scope_count": male_scope_count,
        "selection_policy": DISCOVERY_SELECTION_POLICY,
        "quarantined_scopes": [],
        "changes": [json.loads(item.canonical_json()) for item in diff.changes],
        "change_count": len(diff.changes),
        "unresolved_discovery_diffs": False,
        "alerts": alerts,
        "promotion_performed": False,
        "observed_at": observed_at.isoformat(),
    }
    review_ref = _payload_ref(review_uri, review)
    discovery_state = _discovery_review_state(review, review_ref=review_ref)
    discovery_state_ref = _payload_ref(discovery_state_uri, discovery_state)
    checkpoint = {
        "kind": "espn-discovery-reducer-checkpoint-v2",
        "schema_version": 2,
        "discovery_detail_index_ref": canonical_index_ref,
        "discovery_detail_phase_refs": canonical_phase_refs,
        "candidate_payload": candidate_payload,
        "candidate_ref": candidate_ref,
        "candidate_signature": candidate_signature,
        "male_registry_payload": male_registry_payload,
        "male_registry_ref": male_registry_ref,
        "male_registry_signature": male_registry_signature,
        "male_scope_count": male_scope_count,
        "review_payload": review,
        "review_ref": review_ref,
        "discovery_state_payload": discovery_state,
        "discovery_state_ref": discovery_state_ref,
        "observed_at": observed_at.isoformat(),
    }
    _write_payload(checkpoint_uri, checkpoint)
    return _replay_discovery_reducer_checkpoint(
        checkpoint,
        discovery_detail_index_ref=canonical_index_ref,
        discovery_detail_phase_refs=canonical_phase_refs,
        candidate_uri=candidate_uri,
        male_registry_uri=male_registry_uri,
        review_uri=review_uri,
        discovery_state_uri=discovery_state_uri,
        latest_state_uri=latest_state_uri,
    )


# Release compatibility for callers importing the old observational reducer name.
write_reviewable_discovery_diff = publish_discovered_male_registry


def check_36h_freshness_and_alerts(**context) -> dict[str, str]:
    """Network-free six-hour monitor over durable COMPLETE scope heads."""

    store = PostgresEspnControlStore.from_env()
    store.migrate()
    now = store.current_time()
    frozen_state_ref = _frozen_discovery_state_ref()
    if frozen_state_ref is None:
        raise OperationsError(
            "ESPN monitor requires a frozen discovery state reference"
        )
    registry, discovery = _load_discovered_registry(now=now)
    discovery_state_ref = _discovery_artifact_ref(
        discovery.get("discovery_state_ref"),
        label="monitor discovery state reference",
    )
    if discovery_state_ref != frozen_state_ref:
        raise OperationsError(
            "monitor discovery state reference differs from configured frozen target"
        )
    male_registry_ref = _discovery_artifact_ref(
        discovery.get("male_registry_ref"),
        label="monitor male registry reference",
    )
    registry_signature = _sha(registry.signature(), "monitor registry signature")
    discovered_registry_signature = _sha(
        discovery.get("male_registry_signature"),
        "monitor discovered registry signature",
    )
    if discovered_registry_signature != registry_signature:
        raise OperationsError(
            "monitor registry signature differs from frozen discovery state"
        )
    if discovery.get("selection_policy") != DISCOVERY_SELECTION_POLICY:
        raise OperationsError("monitor selection policy differs from frozen target")
    scope_ids = tuple(
        sorted(
            competition.scope_id(competition.current_edition)
            for competition in registry.promoted
        )
    )
    if not scope_ids or tuple(sorted(set(scope_ids))) != scope_ids:
        raise OperationsError("monitor frozen target scope set is invalid")
    male_scope_count = discovery.get("male_scope_count")
    if type(male_scope_count) is not int or male_scope_count != len(scope_ids):
        raise OperationsError("monitor scope count differs from frozen target")
    heads = store.read_scope_heads(scope_ids)
    latest_evidence = _latest_daily_evidence_or_empty(store, scope_ids)
    unexpected_heads = sorted(set(heads) - set(scope_ids))
    if unexpected_heads:
        raise OperationsError(
            f"monitor scope heads contain non-target scopes: {unexpected_heads}"
        )
    dag_id, run_id, _ = _run_identity(context)
    alerts = []
    # The exact v2 discovery checkpoint is admitted only after its review has
    # no quarantined scopes or unresolved diffs.  Do not mix mutable
    # ``latest-state.json`` flags into this frozen monitor run.
    rollover, unresolved = False, False
    for scope_id in scope_ids:
        raw_head = heads.get(scope_id)
        head, state, freshness_at = _qualified_freshness_at(
            raw_head,
            latest_evidence.get(scope_id),
            expected_registry_ref=male_registry_ref,
            expected_registry_signature=registry_signature,
            observed_at=now,
        )
        if raw_head is None:
            alert_identity = {
                "identity_kind": "monitor-subject",
                "identity_sha256": _subject_identity(
                    dag_id=dag_id,
                    run_id=run_id,
                    scope_id=scope_id,
                    registry_signature=registry_signature,
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
                    "last_complete_at": freshness_at,
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
        "kind": "espn-monitor-result-v2",
        "schema_version": 2,
        "dag_id": dag_id,
        "run_id": run_id,
        "observed_at": now.isoformat(),
        "discovery_state_ref": discovery_state_ref,
        "male_registry_ref": male_registry_ref,
        "registry_signature": registry_signature,
        "selection_policy": DISCOVERY_SELECTION_POLICY,
        "male_scope_count": male_scope_count,
        "scope_ids": list(scope_ids),
        "alerts": alerts,
    }
    root = _join_uri(_artifact_root(), "monitor", _run_key(dag_id, run_id))
    result_ref = _write_payload(_join_uri(root, "monitor.json"), payload)
    if any(item["severity"] == "hard" for item in alerts):
        from airflow.exceptions import AirflowException

        raise AirflowException("ESPN freshness monitor found hard alerts")
    return {"monitor_ref": result_ref}
