"""Plan-driven ESPN Native Raw/Bronze execution.

All source selection and publication identities come from an immutable signed
plan.  Network capture is raw-first; resume and replay read exact immutable
blob URI/SHA pairs from the durable raw checkpoint rather than mutable target
aliases.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any, Iterable, Mapping, Protocol, Sequence
from urllib.parse import urlparse
import uuid

from pyarrow import fs
import yaml

from .models import (
    CapabilityState,
    Competition,
    DispositionState,
    Edition,
    EntityCapabilities,
    IngestPlan,
    RequestDisposition,
    ScopePlan,
)
from .parser_contracts import (
    EntityParseState,
    LineupRow,
    MatchsheetRow,
    PARSER_VERSION,
    ScheduleRow,
)
from .parsers import parse_scoreboards, parse_summary
from .raw_store import EspnRawStore
from .registry import Registry, RegistryError, validate_registry_document
from .repository import (
    EspnBronzeRepository,
    RawLedgerRecord,
    ScopeGeneration,
    ScopePublicationState,
    canonical_json,
    validate_scope_generation,
)
from .transport import EspnHttpClient
from .transport_contracts import (
    DEFAULT_MAX_COMPETITIONS,
    DEFAULT_MAX_REQUESTS,
    DEFAULT_MAX_SUMMARY_EVENTS,
    DEFAULT_MAX_TASK_BYTES,
    EndpointType,
    FetchResult,
    TaskBudget,
    canonicalize_target,
)


RUNTIME_VERSION = "espn-native-runtime-v2"
PLAN_KIND = "espn-ingest-plan-v1"
RAW_MANIFEST_KIND = "espn-raw-run-manifest-v1"
SCOPE_SNAPSHOT_KIND = "espn-scope-generation-snapshot-v1"
ARTIFACT_POINTER_KIND = "espn-artifact-pointer-v1"
SUMMARY_CHECKPOINT_SIZE = 50
SCOREBOARD_LIMIT = 1000
SCOREBOARD_MAX_RANGE_DAYS = 365
SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard"
)
SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/summary"
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_MODES = frozenset({"daily", "repair", "backfill", "replay"})


class RunnerError(RuntimeError):
    """Base error for deterministic ESPN execution."""


class RunnerConfigurationError(RunnerError):
    """A signed identity or artifact binding is invalid."""


class ScopeIncompleteError(RunnerError):
    """One scope cannot produce a COMPLETE generation."""


class ArtifactConflictError(RunnerConfigurationError):
    """An immutable artifact identity already contains different bytes."""


class RepositoryProtocol(Protocol):
    def publish_scope(self, generation: ScopeGeneration): ...


@dataclass(frozen=True, slots=True)
class ExecutionOptions:
    mode: str
    scopes: tuple[str, ...]
    as_of: date
    run_id: str
    attempt: int
    plan_uri: str
    raw_manifest_uri: str
    output_uri: str
    raw_store_uri: str
    max_events: int = DEFAULT_MAX_SUMMARY_EVENTS

    def __post_init__(self) -> None:
        if self.mode not in _MODES:
            raise ValueError(f"mode must be one of {sorted(_MODES)}")
        if type(self.as_of) is not date:
            raise TypeError("as_of must be a date")
        if type(self.attempt) is not int or self.attempt <= 0:
            raise ValueError("attempt must be a positive integer")
        if (
            type(self.max_events) is not int
            or self.max_events <= 0
            or self.max_events > DEFAULT_MAX_SUMMARY_EVENTS
        ):
            raise ValueError(
                f"max_events must be between 1 and {DEFAULT_MAX_SUMMARY_EVENTS}"
            )
        if not isinstance(self.scopes, (tuple, list)) or not all(
            isinstance(item, str) and item for item in self.scopes
        ):
            raise ValueError("scopes must contain exact scope IDs")
        scopes = tuple(self.scopes)
        if len(scopes) != len(set(scopes)):
            raise ValueError("duplicate --scope values are forbidden")
        object.__setattr__(self, "scopes", scopes)
        for field_name in (
            "run_id",
            "plan_uri",
            "raw_manifest_uri",
            "output_uri",
            "raw_store_uri",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if self.output_uri == "/tmp/espn_result.json":
            raise ValueError("shared /tmp/espn_result.json is forbidden")


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    exit_code: int
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class StagingResult:
    """Offline parser output before the manifest-gated repository boundary."""

    exit_code: int
    payload: Mapping[str, Any]
    generations: Mapping[str, ScopeGeneration]


@dataclass(frozen=True, slots=True)
class KnownNonterminalEvent:
    event_id: int
    event_date: date


@dataclass(frozen=True, slots=True)
class PriorBinding:
    uri: str
    artifact_sha256: str
    scope_id: str
    generation_id: str
    generation_signature: str
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class ScopeBinding:
    active: bool
    initial_capture: bool
    generation_id: str
    batch_id: str
    ingested_at: datetime
    generation_snapshot_uri: str
    known_nonterminal_events: tuple[KnownNonterminalEvent, ...]
    prior: PriorBinding | None


@dataclass(frozen=True, slots=True)
class ReplaySource:
    mode: str
    run_id: str
    attempt: int
    plan_signature: str
    raw_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class LoadedPlan:
    plan: IngestPlan
    signature: str
    registry_snapshot_uri: str
    raw_manifest_uri: str
    output_uri: str
    raw_store_uri: str
    max_events: int
    mode: str
    attempt: int
    selected_scopes: tuple[str, ...]
    bindings: Mapping[str, ScopeBinding]
    replay_source: ReplaySource | None


@dataclass(frozen=True, slots=True)
class ScoreboardRequest:
    scope_id: str
    url: str
    params: Mapping[str, Any]
    query_start: date
    query_end: date
    request_id: str


def _required_string(value: Any, field_name: str) -> str:
    if type(value) is not str or not value.strip():
        raise RunnerConfigurationError(f"{field_name} must be a non-empty string")
    return value


def _positive_int(value: Any, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise RunnerConfigurationError(f"{field_name} must be a positive integer")
    return value


def _max_events(value: Any, field_name: str) -> int:
    parsed = _positive_int(value, field_name)
    if parsed > DEFAULT_MAX_SUMMARY_EVENTS:
        raise RunnerConfigurationError(
            f"{field_name} cannot exceed {DEFAULT_MAX_SUMMARY_EVENTS}"
        )
    return parsed


def _sha256(value: Any, field_name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise RunnerConfigurationError(f"{field_name} must be a lowercase SHA-256")
    return value


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RunnerConfigurationError(f"{field_name} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise RunnerConfigurationError(f"{field_name} keys must be strings")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], field_name: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise RunnerConfigurationError(
            f"{field_name} keys mismatch; missing={missing}, unknown={extra}"
        )


def _parse_date(value: Any, field_name: str) -> date:
    if type(value) is not str:
        raise RunnerConfigurationError(f"{field_name} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RunnerConfigurationError(f"{field_name} must be an ISO date") from exc


def _parse_datetime(value: Any, field_name: str) -> datetime:
    if type(value) is not str:
        raise RunnerConfigurationError(f"{field_name} must be an ISO UTC timestamp")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise RunnerConfigurationError(
            f"{field_name} must be an ISO UTC timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RunnerConfigurationError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _capabilities(value: Any, field_name: str) -> EntityCapabilities:
    raw = _mapping(value, field_name)
    _exact_keys(raw, {"schedule", "lineup", "matchsheet"}, field_name)
    try:
        return EntityCapabilities(
            schedule=CapabilityState(raw["schedule"]),
            lineup=CapabilityState(raw["lineup"]),
            matchsheet=CapabilityState(raw["matchsheet"]),
        )
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"{field_name} is invalid") from exc


def _scope_plan(value: Any, field_name: str) -> ScopePlan:
    raw = _mapping(value, field_name)
    _exact_keys(
        raw,
        {
            "scope_id",
            "espn_id",
            "slug",
            "source_season_year",
            "start_date",
            "end_date",
            "capabilities",
        },
        field_name,
    )
    try:
        return ScopePlan(
            scope_id=raw["scope_id"],
            espn_id=raw["espn_id"],
            slug=raw["slug"],
            source_season_year=raw["source_season_year"],
            start_date=_parse_date(raw["start_date"], f"{field_name}.start_date"),
            end_date=_parse_date(raw["end_date"], f"{field_name}.end_date"),
            capabilities=_capabilities(
                raw["capabilities"], f"{field_name}.capabilities"
            ),
        )
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"{field_name} is invalid: {exc}") from exc


def _ingest_plan(value: Any) -> IngestPlan:
    raw = _mapping(value, "plan")
    _exact_keys(
        raw,
        {
            "schema_version",
            "run_id",
            "as_of",
            "registry_signature",
            "scopes",
            "metadata",
        },
        "plan",
    )
    raw_scopes = raw["scopes"]
    if not isinstance(raw_scopes, list):
        raise RunnerConfigurationError("plan.scopes must be a list")
    try:
        return IngestPlan(
            schema_version=raw["schema_version"],
            run_id=raw["run_id"],
            as_of=_parse_date(raw["as_of"], "plan.as_of"),
            registry_signature=raw["registry_signature"],
            scopes=tuple(
                _scope_plan(item, f"plan.scopes[{index}]")
                for index, item in enumerate(raw_scopes)
            ),
            metadata=raw["metadata"],
        )
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"invalid plan: {exc}") from exc


def _prior_binding(value: Any, field_name: str) -> PriorBinding | None:
    if value is None:
        return None
    raw = _mapping(value, field_name)
    _exact_keys(
        raw,
        {
            "uri",
            "artifact_sha256",
            "scope_id",
            "generation_id",
            "generation_signature",
            "manifest_sha256",
        },
        field_name,
    )
    return PriorBinding(
        uri=_required_string(raw["uri"], f"{field_name}.uri"),
        artifact_sha256=_sha256(
            raw["artifact_sha256"], f"{field_name}.artifact_sha256"
        ),
        scope_id=_required_string(raw["scope_id"], f"{field_name}.scope_id"),
        generation_id=_required_string(
            raw["generation_id"], f"{field_name}.generation_id"
        ),
        generation_signature=_sha256(
            raw["generation_signature"], f"{field_name}.generation_signature"
        ),
        manifest_sha256=_sha256(
            raw["manifest_sha256"], f"{field_name}.manifest_sha256"
        ),
    )


def _scope_binding(value: Any, scope_id: str) -> ScopeBinding:
    field_name = f"plan.metadata.runtime.scope_bindings[{scope_id!r}]"
    raw = _mapping(value, field_name)
    _exact_keys(
        raw,
        {
            "active",
            "initial_capture",
            "generation_id",
            "batch_id",
            "ingested_at",
            "generation_snapshot_uri",
            "known_nonterminal_events",
            "prior",
        },
        field_name,
    )
    for boolean in ("active", "initial_capture"):
        if type(raw[boolean]) is not bool:
            raise RunnerConfigurationError(f"{field_name}.{boolean} must be boolean")
    known_raw = raw["known_nonterminal_events"]
    if not isinstance(known_raw, (list, tuple)):
        raise RunnerConfigurationError(
            f"{field_name}.known_nonterminal_events must be a list"
        )
    known: list[KnownNonterminalEvent] = []
    seen_events: set[int] = set()
    for index, item in enumerate(known_raw):
        event_field = f"{field_name}.known_nonterminal_events[{index}]"
        event = _mapping(item, event_field)
        _exact_keys(event, {"event_id", "event_date"}, event_field)
        event_id = _positive_int(event["event_id"], f"{event_field}.event_id")
        if event_id in seen_events:
            raise RunnerConfigurationError(f"{field_name} duplicates event {event_id}")
        seen_events.add(event_id)
        known.append(
            KnownNonterminalEvent(
                event_id=event_id,
                event_date=_parse_date(
                    event["event_date"], f"{event_field}.event_date"
                ),
            )
        )
    prior = _prior_binding(raw["prior"], f"{field_name}.prior")
    if (not raw["active"] or not raw["initial_capture"]) and prior is None:
        raise RunnerConfigurationError(
            f"scope {scope_id} clean no-op/non-initial execution requires prior COMPLETE identity"
        )
    if prior is not None and prior.scope_id != scope_id:
        raise RunnerConfigurationError(f"scope {scope_id} prior identity drift")
    return ScopeBinding(
        active=raw["active"],
        initial_capture=raw["initial_capture"],
        generation_id=_required_string(
            raw["generation_id"], f"{field_name}.generation_id"
        ),
        batch_id=_required_string(raw["batch_id"], f"{field_name}.batch_id"),
        ingested_at=_parse_datetime(raw["ingested_at"], f"{field_name}.ingested_at"),
        generation_snapshot_uri=_required_string(
            raw["generation_snapshot_uri"],
            f"{field_name}.generation_snapshot_uri",
        ),
        known_nonterminal_events=tuple(
            sorted(known, key=lambda item: (item.event_date, item.event_id))
        ),
        prior=prior,
    )


def _replay_source(value: Any) -> ReplaySource | None:
    if value is None:
        return None
    raw = _mapping(value, "plan.metadata.runtime.replay_source")
    _exact_keys(
        raw,
        {"mode", "run_id", "attempt", "plan_signature", "raw_manifest_sha256"},
        "plan.metadata.runtime.replay_source",
    )
    mode = _required_string(raw["mode"], "replay_source.mode")
    if mode not in {"daily", "repair", "backfill"}:
        raise RunnerConfigurationError("replay_source.mode is invalid")
    return ReplaySource(
        mode=mode,
        run_id=_required_string(raw["run_id"], "replay_source.run_id"),
        attempt=_positive_int(raw["attempt"], "replay_source.attempt"),
        plan_signature=_sha256(raw["plan_signature"], "replay_source.plan_signature"),
        raw_manifest_sha256=_sha256(
            raw["raw_manifest_sha256"], "replay_source.raw_manifest_sha256"
        ),
    )


def _signed_scope_selection(value: Any, scope_ids: set[str]) -> tuple[str, ...]:
    field_name = "plan.metadata.runtime.selected_scopes"
    if not isinstance(value, (list, tuple)) or not all(
        type(scope_id) is str and scope_id for scope_id in value
    ):
        raise RunnerConfigurationError(f"{field_name} must be a sequence of scope IDs")
    selected = tuple(value)
    if not selected:
        raise RunnerConfigurationError(f"{field_name} must not be empty")
    if len(selected) != len(set(selected)):
        raise RunnerConfigurationError(f"{field_name} must contain unique scope IDs")
    if selected != tuple(sorted(selected)):
        raise RunnerConfigurationError(f"{field_name} must be sorted")
    unknown = sorted(set(selected) - scope_ids)
    if unknown:
        raise RunnerConfigurationError(
            f"{field_name} contains scopes outside the signed plan: {unknown}"
        )
    return selected


def _load_signed_plan(uri: str) -> LoadedPlan:
    try:
        envelope = json.loads(_read_artifact(uri).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, OSError) as exc:
        raise RunnerConfigurationError(f"cannot decode signed plan {uri}") from exc
    envelope = _mapping(envelope, "plan envelope")
    _exact_keys(envelope, {"kind", "plan", "signature"}, "plan envelope")
    if envelope["kind"] != PLAN_KIND:
        raise RunnerConfigurationError("unsupported ESPN plan envelope kind")
    plan = _ingest_plan(envelope["plan"])
    signature = _sha256(envelope["signature"], "plan envelope.signature")
    if signature != plan.signature():
        raise RunnerConfigurationError("signed ESPN plan signature mismatch")
    metadata = _mapping(plan.metadata, "plan.metadata")
    _exact_keys(metadata, {"runtime"}, "plan.metadata")
    runtime = _mapping(metadata["runtime"], "plan.metadata.runtime")
    _exact_keys(
        runtime,
        {
            "mode",
            "attempt",
            "registry_snapshot_uri",
            "raw_manifest_uri",
            "output_uri",
            "raw_store_uri",
            "max_events",
            "selected_scopes",
            "scope_bindings",
            "replay_source",
        },
        "plan.metadata.runtime",
    )
    mode = _required_string(runtime["mode"], "plan.metadata.runtime.mode")
    if mode not in _MODES:
        raise RunnerConfigurationError("plan runtime mode is invalid")
    raw_bindings = _mapping(
        runtime["scope_bindings"], "plan.metadata.runtime.scope_bindings"
    )
    scope_ids = {scope.scope_id for scope in plan.scopes}
    selected_scopes = _signed_scope_selection(runtime["selected_scopes"], scope_ids)
    if set(raw_bindings) != scope_ids:
        raise RunnerConfigurationError(
            "plan scope bindings must exactly match signed plan scopes"
        )
    bindings = {
        scope_id: _scope_binding(raw_bindings[scope_id], scope_id)
        for scope_id in sorted(scope_ids)
    }
    replay = _replay_source(runtime["replay_source"])
    if (mode == "replay") != (replay is not None):
        raise RunnerConfigurationError(
            "replay mode requires replay_source and other modes forbid it"
        )
    return LoadedPlan(
        plan=plan,
        signature=signature,
        registry_snapshot_uri=_required_string(
            runtime["registry_snapshot_uri"], "runtime.registry_snapshot_uri"
        ),
        raw_manifest_uri=_required_string(
            runtime["raw_manifest_uri"], "runtime.raw_manifest_uri"
        ),
        output_uri=_required_string(runtime["output_uri"], "runtime.output_uri"),
        raw_store_uri=_required_string(
            runtime["raw_store_uri"], "runtime.raw_store_uri"
        ),
        max_events=_max_events(runtime["max_events"], "runtime.max_events"),
        mode=mode,
        attempt=_positive_int(runtime["attempt"], "runtime.attempt"),
        selected_scopes=selected_scopes,
        bindings=bindings,
        replay_source=replay,
    )


@lru_cache(maxsize=16)
def _s3_filesystem(
    process_id: int,
    access_key: str | None,
    secret_key: str | None,
    endpoint_override: str,
    scheme: str,
    region: str,
) -> fs.S3FileSystem:
    """Reuse Arrow's expensive S3 client only inside one exact process."""

    if process_id <= 0:
        raise RunnerConfigurationError("artifact S3 process identity is invalid")
    return fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=endpoint_override,
        scheme=scheme,
        region=region,
        background_writes=False,
    )


def _resolve_uri(uri: str) -> tuple[fs.FileSystem, str]:
    candidate = _required_string(uri, "artifact URI")
    parsed = urlparse(candidate)
    if parsed.scheme == "s3":
        if not parsed.netloc:
            raise RunnerConfigurationError("S3 artifact URI must contain a bucket")
        filesystem = _s3_filesystem(
            os.getpid(),
            os.environ.get("S3_ACCESS_KEY"),
            os.environ.get("S3_SECRET_KEY"),
            os.environ.get(
                "ESPN_ARTIFACT_S3_ENDPOINT",
                os.environ.get("ESPN_RAW_S3_ENDPOINT", "seaweedfs:8333"),
            ),
            os.environ.get(
                "ESPN_ARTIFACT_S3_SCHEME",
                os.environ.get("ESPN_RAW_S3_SCHEME", "http"),
            ),
            os.environ.get(
                "ESPN_ARTIFACT_S3_REGION",
                os.environ.get("ESPN_RAW_S3_REGION", "us-east-1"),
            ),
        )
        return filesystem, f"{parsed.netloc}/{parsed.path.lstrip('/')}"
    try:
        filesystem, path = fs.FileSystem.from_uri(candidate)
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"invalid artifact URI {candidate!r}") from exc
    if not path:
        raise RunnerConfigurationError("artifact URI must identify an object")
    return filesystem, path


def _read_direct(uri: str) -> bytes:
    filesystem, path = _resolve_uri(uri)
    info = filesystem.get_file_info(path)
    if info.type == fs.FileType.NotFound:
        raise FileNotFoundError(uri)
    if info.type != fs.FileType.File:
        raise RunnerConfigurationError(f"artifact URI is not a file: {uri}")
    with filesystem.open_input_file(path) as stream:
        return stream.read()


def _read_artifact(uri: str) -> bytes:
    payload = _read_direct(uri)
    try:
        pointer = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return payload
    if not isinstance(pointer, Mapping) or pointer.get("kind") != ARTIFACT_POINTER_KIND:
        return payload
    _exact_keys(pointer, {"kind", "artifact_uri", "sha256"}, "artifact pointer")
    expected = _sha256(pointer["sha256"], "artifact pointer.sha256")
    artifact_uri = _required_string(pointer["artifact_uri"], "artifact pointer.uri")
    artifact = _read_direct(artifact_uri)
    if hashlib.sha256(artifact).hexdigest() != expected:
        raise RunnerConfigurationError("artifact pointer content hash mismatch")
    return artifact


def _local_path(uri: str) -> Path | None:
    filesystem, path = _resolve_uri(uri)
    if isinstance(filesystem, fs.LocalFileSystem):
        return Path(path)
    return None


def _write_local_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _remote_content_uri(uri: str, digest: str) -> str:
    return f"{uri}.objects/sha256/{digest[:2]}/{digest}.json"


def _write_remote_once(uri: str, payload: bytes) -> None:
    filesystem, path = _resolve_uri(uri)
    parent = str(PurePosixPath(path).parent)
    filesystem.create_dir(parent, recursive=True)
    info = filesystem.get_file_info(path)
    if info.type != fs.FileType.NotFound:
        if _read_direct(uri) != payload:
            raise ArtifactConflictError(f"immutable artifact conflict at {uri}")
        return
    with filesystem.open_output_stream(path, compression=None) as stream:
        stream.write(payload)
    if _read_direct(uri) != payload:
        raise RunnerConfigurationError(f"artifact verification failed at {uri}")


def _write_artifact(uri: str, payload: bytes, *, immutable: bool) -> None:
    local = _local_path(uri)
    if local is not None:
        if local.exists():
            existing = local.read_bytes()
            if existing == payload:
                return
            if immutable:
                raise ArtifactConflictError(f"immutable artifact conflict at {uri}")
        _write_local_atomic(local, payload)
        return

    digest = hashlib.sha256(payload).hexdigest()
    artifact_uri = _remote_content_uri(uri, digest)
    _write_remote_once(artifact_uri, payload)
    pointer = (
        json.dumps(
            {
                "kind": ARTIFACT_POINTER_KIND,
                "artifact_uri": artifact_uri,
                "sha256": digest,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    try:
        existing = _read_artifact(uri)
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if existing == payload:
            return
        if immutable:
            raise ArtifactConflictError(f"immutable artifact conflict at {uri}")
    filesystem, path = _resolve_uri(uri)
    filesystem.create_dir(str(PurePosixPath(path).parent), recursive=True)
    # Object-store PUT becomes visible as one complete pointer object; content
    # was committed and hash-verified first.
    with filesystem.open_output_stream(path, compression=None) as stream:
        stream.write(pointer)
    if _read_artifact(uri) != payload:
        raise RunnerConfigurationError(f"artifact pointer verification failed at {uri}")


def _assert_immutable_target(uri: str, payload: bytes) -> None:
    try:
        existing = _read_artifact(uri)
    except FileNotFoundError:
        return
    if existing != payload:
        raise ArtifactConflictError(f"immutable artifact conflict at {uri}")


def _canonical_bytes(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def _row_payload(row: Any) -> dict[str, Any]:
    return json.loads(canonical_json(row))


def scope_snapshot_bytes(generation: ScopeGeneration) -> bytes:
    """Serialize one validated COMPLETE generation for the next signed plan."""

    report = validate_scope_generation(generation)
    if not report.passed:
        raise ValueError(
            "cannot snapshot incomplete generation: " + "; ".join(report.failures)
        )
    payload = {
        "kind": SCOPE_SNAPSHOT_KIND,
        "status": "complete",
        "scope_id": generation.plan.scope_id,
        "generation_id": generation.generation_id,
        "generation_signature": generation.generation_signature,
        "manifest_sha256": generation.manifest_sha256,
        "generation": json.loads(canonical_json(generation)),
    }
    return _canonical_bytes(payload)


def load_scope_snapshot(
    uri: str,
    *,
    artifact_sha256: str,
    expected_scope_id: str | None = None,
) -> ScopeGeneration:
    """Load one exact staged/prior generation snapshot for DQ/publication."""

    expected_hash = _sha256(artifact_sha256, "snapshot artifact_sha256")
    payload = _read_artifact(uri)
    if hashlib.sha256(payload).hexdigest() != expected_hash:
        raise RunnerConfigurationError("scope snapshot artifact hash mismatch")
    try:
        document = _mapping(json.loads(payload.decode("utf-8")), "scope snapshot")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunnerConfigurationError("scope snapshot is not valid JSON") from exc
    _exact_keys(
        document,
        {
            "kind",
            "status",
            "scope_id",
            "generation_id",
            "generation_signature",
            "manifest_sha256",
            "generation",
        },
        "scope snapshot",
    )
    if document["kind"] != SCOPE_SNAPSHOT_KIND or document["status"] != "complete":
        raise RunnerConfigurationError("scope snapshot is not COMPLETE")
    generation = _scope_generation(document["generation"])
    if expected_scope_id is not None and generation.plan.scope_id != expected_scope_id:
        raise RunnerConfigurationError("scope snapshot scope identity mismatch")
    expected = (
        generation.plan.scope_id,
        generation.generation_id,
        generation.generation_signature,
        generation.manifest_sha256,
    )
    actual = (
        document["scope_id"],
        document["generation_id"],
        document["generation_signature"],
        document["manifest_sha256"],
    )
    if actual != expected:
        raise RunnerConfigurationError("scope snapshot generation identity mismatch")
    report = validate_scope_generation(generation)
    if not report.passed:
        raise RunnerConfigurationError(
            "scope snapshot DQ failed: " + "; ".join(report.failures)
        )
    return generation


def _schedule_row(value: Any, field_name: str) -> ScheduleRow:
    raw = dict(_mapping(value, field_name))
    expected = {field.name for field in fields(ScheduleRow)}
    _exact_keys(raw, expected, field_name)
    for name in ("kickoff", "date", "match_date"):
        raw[name] = _parse_datetime(raw[name], f"{field_name}.{name}")
    try:
        return ScheduleRow(**raw)
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"invalid {field_name}: {exc}") from exc


def _lineup_row(value: Any, field_name: str) -> LineupRow:
    raw = dict(_mapping(value, field_name))
    _exact_keys(raw, {field.name for field in fields(LineupRow)}, field_name)
    try:
        return LineupRow(**raw)
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"invalid {field_name}: {exc}") from exc


def _matchsheet_row(value: Any, field_name: str) -> MatchsheetRow:
    raw = dict(_mapping(value, field_name))
    _exact_keys(raw, {field.name for field in fields(MatchsheetRow)}, field_name)
    try:
        return MatchsheetRow(**raw)
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"invalid {field_name}: {exc}") from exc


def _raw_ledger_record(value: Any, field_name: str) -> RawLedgerRecord:
    raw = dict(_mapping(value, field_name))
    _exact_keys(raw, {field.name for field in fields(RawLedgerRecord)}, field_name)
    raw["disposition"] = DispositionState(raw["disposition"])
    if raw["fetched_at"] is not None:
        raw["fetched_at"] = _parse_datetime(
            raw["fetched_at"], f"{field_name}.fetched_at"
        )
    raw["event_ids"] = tuple(raw["event_ids"])
    try:
        return RawLedgerRecord(**raw)
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"invalid {field_name}: {exc}") from exc


def _request_disposition(value: Any, field_name: str) -> RequestDisposition:
    raw = dict(_mapping(value, field_name))
    _exact_keys(raw, {field.name for field in fields(RequestDisposition)}, field_name)
    try:
        return RequestDisposition(
            endpoint=raw["endpoint"],
            state=DispositionState(raw["state"]),
            detail=raw["detail"],
            event_id=raw["event_id"],
        )
    except (TypeError, ValueError) as exc:
        raise RunnerConfigurationError(f"invalid {field_name}: {exc}") from exc


def _scope_generation(value: Any) -> ScopeGeneration:
    raw = _mapping(value, "scope snapshot.generation")
    _exact_keys(raw, {field.name for field in fields(ScopeGeneration)}, "generation")
    try:
        return ScopeGeneration(
            plan=_scope_plan(raw["plan"], "generation.plan"),
            run_id=raw["run_id"],
            generation_id=raw["generation_id"],
            registry_snapshot_uri=raw["registry_snapshot_uri"],
            registry_signature=raw["registry_signature"],
            plan_signature=raw["plan_signature"],
            parser_version=raw["parser_version"],
            runtime_version=raw["runtime_version"],
            ingested_at=_parse_datetime(raw["ingested_at"], "generation.ingested_at"),
            batch_id=raw["batch_id"],
            schedule=tuple(
                _schedule_row(item, f"generation.schedule[{index}]")
                for index, item in enumerate(raw["schedule"])
            ),
            lineup=tuple(
                _lineup_row(item, f"generation.lineup[{index}]")
                for index, item in enumerate(raw["lineup"])
            ),
            matchsheet=tuple(
                _matchsheet_row(item, f"generation.matchsheet[{index}]")
                for index, item in enumerate(raw["matchsheet"])
            ),
            planned_request_ids=tuple(raw["planned_request_ids"]),
            raw_ledger=tuple(
                _raw_ledger_record(item, f"generation.raw_ledger[{index}]")
                for index, item in enumerate(raw["raw_ledger"])
            ),
            dispositions=tuple(
                _request_disposition(item, f"generation.dispositions[{index}]")
                for index, item in enumerate(raw["dispositions"])
            ),
        )
    except (TypeError, ValueError, KeyError) as exc:
        raise RunnerConfigurationError(f"invalid scope generation: {exc}") from exc


def _load_prior(binding: PriorBinding, expected_scope: ScopePlan) -> ScopeGeneration:
    try:
        payload = _read_artifact(binding.uri)
    except (FileNotFoundError, OSError) as exc:
        raise RunnerConfigurationError(
            f"prior COMPLETE snapshot not found for {expected_scope.scope_id}"
        ) from exc
    if hashlib.sha256(payload).hexdigest() != binding.artifact_sha256:
        raise RunnerConfigurationError(
            f"prior snapshot hash drift for {expected_scope.scope_id}"
        )
    try:
        envelope = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunnerConfigurationError("prior snapshot is not valid JSON") from exc
    envelope = _mapping(envelope, "scope snapshot")
    _exact_keys(
        envelope,
        {
            "kind",
            "status",
            "scope_id",
            "generation_id",
            "generation_signature",
            "manifest_sha256",
            "generation",
        },
        "scope snapshot",
    )
    if envelope["kind"] != SCOPE_SNAPSHOT_KIND or envelope["status"] != "complete":
        raise RunnerConfigurationError("prior snapshot is not COMPLETE")
    generation = _scope_generation(envelope["generation"])
    expected = (
        binding.scope_id,
        binding.generation_id,
        binding.generation_signature,
        binding.manifest_sha256,
    )
    observed = (
        envelope["scope_id"],
        envelope["generation_id"],
        envelope["generation_signature"],
        envelope["manifest_sha256"],
    )
    actual = (
        generation.plan.scope_id,
        generation.generation_id,
        generation.generation_signature,
        generation.manifest_sha256,
    )
    if observed != expected or actual != expected:
        raise RunnerConfigurationError(
            f"prior generation identity drift for {expected_scope.scope_id}"
        )
    if generation.plan != expected_scope:
        raise RunnerConfigurationError(
            f"prior scope contract drift for {expected_scope.scope_id}"
        )
    report = validate_scope_generation(generation)
    if not report.passed:
        raise RunnerConfigurationError(
            "prior snapshot is not a validated COMPLETE generation: "
            + "; ".join(report.failures)
        )
    return generation


def _load_registry(uri: str, expected_signature: str) -> Registry:
    try:
        document = yaml.safe_load(_read_artifact(uri).decode("utf-8"))
        registry = validate_registry_document(document)
    except (
        FileNotFoundError,
        UnicodeDecodeError,
        yaml.YAMLError,
        RegistryError,
    ) as exc:
        raise RunnerConfigurationError(
            f"cannot load exact registry snapshot {uri}"
        ) from exc
    if registry.signature() != expected_signature:
        raise RunnerConfigurationError("registry snapshot signature mismatch")
    return registry


def _registry_scope(
    registry: Registry, scope: ScopePlan
) -> tuple[Competition, Edition]:
    competition = registry.by_id.get(scope.espn_id)
    if competition is None or competition.slug != scope.slug or not competition.enabled:
        raise RunnerConfigurationError(
            f"registry does not promote exact scope {scope.scope_id}"
        )
    edition = next(
        (
            item
            for item in competition.editions
            if item.source_season_year == scope.source_season_year
        ),
        None,
    )
    if edition is None:
        raise RunnerConfigurationError(
            f"registry edition missing for exact scope {scope.scope_id}"
        )
    if (
        edition.start_date != scope.start_date
        or edition.end_date != scope.end_date
        or edition.capabilities != scope.capabilities
    ):
        raise RunnerConfigurationError(
            f"registry edition contract drift for {scope.scope_id}"
        )
    return competition, edition


def _validate_options(
    options: ExecutionOptions, loaded: LoadedPlan
) -> tuple[ScopePlan, ...]:
    plan = loaded.plan
    mismatches = []
    if options.mode != loaded.mode:
        mismatches.append("mode")
    if options.run_id != plan.run_id:
        mismatches.append("run_id")
    if options.attempt != loaded.attempt:
        mismatches.append("attempt")
    if options.as_of != plan.as_of:
        mismatches.append("as_of")
    if options.raw_manifest_uri != loaded.raw_manifest_uri:
        mismatches.append("raw_manifest_uri")
    if options.output_uri != loaded.output_uri:
        mismatches.append("output_uri")
    if options.raw_store_uri != loaded.raw_store_uri:
        mismatches.append("raw_store_uri")
    if options.max_events != loaded.max_events:
        mismatches.append("max_events")
    if mismatches:
        raise RunnerConfigurationError(
            "CLI identity mismatches signed plan: " + ", ".join(mismatches)
        )
    by_scope = {scope.scope_id: scope for scope in plan.scopes}
    selected_ids = loaded.selected_scopes
    if options.scopes and tuple(sorted(options.scopes)) != selected_ids:
        raise RunnerConfigurationError("CLI scopes do not match signed scope selection")
    if len(selected_ids) > DEFAULT_MAX_COMPETITIONS:
        raise RunnerConfigurationError(
            f"selected scopes exceed task limit {DEFAULT_MAX_COMPETITIONS}"
        )
    return tuple(by_scope[scope_id] for scope_id in selected_ids)


def _canonical_artifact_identity(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme in {"", "file"}:
        raw_path = parsed.path if parsed.scheme else uri
        return f"file://{Path(raw_path).resolve()}"
    normalized_path = "/" + str(PurePosixPath("/" + parsed.path.lstrip("/")))
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{normalized_path}"


def _preflight_artifact_uris(
    options: ExecutionOptions,
    loaded: LoadedPlan,
    selected: Sequence[ScopePlan],
) -> None:
    named = {
        "plan": options.plan_uri,
        "registry": loaded.registry_snapshot_uri,
        "raw_manifest": loaded.raw_manifest_uri,
        "output": loaded.output_uri,
    }
    for scope in selected:
        binding = loaded.bindings[scope.scope_id]
        named[f"snapshot:{scope.scope_id}"] = binding.generation_snapshot_uri
        if binding.prior is not None:
            named[f"prior:{scope.scope_id}"] = binding.prior.uri
    reverse: dict[str, str] = {}
    for name, uri in named.items():
        identity = _canonical_artifact_identity(uri)
        previous = reverse.get(identity)
        if previous is not None:
            raise RunnerConfigurationError(
                f"artifact URI collision between {previous} and {name}"
            )
        reverse[identity] = name


def is_full_reconciliation_day(scope_id: str, as_of: date) -> bool:
    """Stable seven-day shard: one full reconciliation per scope/week."""

    if type(scope_id) is not str or type(as_of) is not date:
        raise TypeError("scope_id and as_of date are required")
    shard = int(hashlib.sha256(scope_id.encode("utf-8")).hexdigest(), 16) % 7
    return shard == as_of.toordinal() % 7


def _effective_mode(loaded: LoadedPlan) -> str:
    return loaded.replay_source.mode if loaded.replay_source else loaded.mode


def _scoreboard_requests(
    scope: ScopePlan,
    binding: ScopeBinding,
    *,
    as_of: date,
    mode: str,
) -> tuple[ScoreboardRequest, ...]:
    full = (
        binding.initial_capture
        or mode in {"repair", "backfill"}
        or (mode == "daily" and is_full_reconciliation_day(scope.scope_id, as_of))
    )
    ranges: set[tuple[date, date]] = set()
    if full:
        ranges.add((scope.start_date, scope.end_date))
    else:
        start = max(scope.start_date, as_of - timedelta(days=3))
        end = min(scope.end_date, as_of + timedelta(days=14))
        if start <= end:
            ranges.add((start, end))
        for event in binding.known_nonterminal_events:
            if not scope.start_date <= event.event_date <= scope.end_date:
                raise RunnerConfigurationError(
                    f"known non-terminal event {event.event_id} is outside {scope.scope_id}"
                )
            if not any(start <= event.event_date <= end for start, end in ranges):
                ranges.add((event.event_date, event.event_date))
    bounded_ranges = []
    for start, end in sorted(ranges):
        cursor = start
        while cursor <= end:
            chunk_end = min(
                end,
                cursor + timedelta(days=SCOREBOARD_MAX_RANGE_DAYS - 1),
            )
            bounded_ranges.append((cursor, chunk_end))
            cursor = chunk_end + timedelta(days=1)

    requests: list[ScoreboardRequest] = []
    url = SCOREBOARD_URL.format(slug=scope.slug)
    for start, end in bounded_ranges:
        dates = start.strftime("%Y%m%d")
        if start != end:
            dates += "-" + end.strftime("%Y%m%d")
        params = {"dates": dates, "limit": SCOREBOARD_LIMIT}
        target = canonicalize_target(url, params)
        requests.append(
            ScoreboardRequest(
                scope_id=scope.scope_id,
                url=url,
                params=params,
                query_start=start,
                query_end=end,
                request_id=f"scoreboard:{target.url_fingerprint}",
            )
        )
    return tuple(requests)


def _manifest_base(loaded: LoadedPlan, selected: Sequence[ScopePlan]) -> dict[str, Any]:
    return {
        "kind": RAW_MANIFEST_KIND,
        "schema_version": 1,
        "run_id": loaded.plan.run_id,
        "attempt": loaded.attempt,
        "mode": loaded.mode,
        "as_of": loaded.plan.as_of.isoformat(),
        "registry_signature": loaded.plan.registry_signature,
        "plan_signature": loaded.signature,
        "selected_scopes": sorted(scope.scope_id for scope in selected),
        "checkpoints": [],
    }


def _seal_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    base = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    return {
        **base,
        "manifest_sha256": hashlib.sha256(_canonical_bytes(base)).hexdigest(),
    }


def _validate_raw_request(value: Any, field_name: str) -> dict[str, Any]:
    raw = dict(_mapping(value, field_name))
    expected = {
        "request_id",
        "scope_id",
        "endpoint",
        "event_id",
        "url_fingerprint",
        "raw_uri",
        "raw_sha256",
        "fetched_at",
        "http_status",
        "direct_bytes",
        "proxy_bytes",
        "query_start",
        "query_end",
    }
    _exact_keys(raw, expected, field_name)
    _required_string(raw["request_id"], f"{field_name}.request_id")
    _required_string(raw["scope_id"], f"{field_name}.scope_id")
    if raw["endpoint"] not in {"scoreboard", "summary"}:
        raise RunnerConfigurationError(f"{field_name}.endpoint is invalid")
    if raw["endpoint"] == "summary":
        _positive_int(raw["event_id"], f"{field_name}.event_id")
    elif raw["event_id"] is not None:
        raise RunnerConfigurationError(f"{field_name}.event_id must be null")
    _sha256(raw["url_fingerprint"], f"{field_name}.url_fingerprint")
    _required_string(raw["raw_uri"], f"{field_name}.raw_uri")
    _sha256(raw["raw_sha256"], f"{field_name}.raw_sha256")
    _parse_datetime(raw["fetched_at"], f"{field_name}.fetched_at")
    if type(raw["http_status"]) is not int or not 200 <= raw["http_status"] <= 299:
        raise RunnerConfigurationError(f"{field_name}.http_status must be successful")
    for name in ("direct_bytes", "proxy_bytes"):
        if type(raw[name]) is not int or raw[name] < 0:
            raise RunnerConfigurationError(f"{field_name}.{name} is invalid")
    if raw["proxy_bytes"] != 0:
        raise RunnerConfigurationError("ESPN raw manifest contains proxy traffic")
    if raw["endpoint"] == "scoreboard":
        start = _parse_date(raw["query_start"], f"{field_name}.query_start")
        end = _parse_date(raw["query_end"], f"{field_name}.query_end")
        if start > end:
            raise RunnerConfigurationError(f"{field_name} query range is invalid")
    elif raw["query_start"] is not None or raw["query_end"] is not None:
        raise RunnerConfigurationError(f"{field_name} Summary query dates must be null")
    return raw


def _validate_raw_manifest(manifest: Any) -> dict[str, Any]:
    raw = dict(_mapping(manifest, "raw manifest"))
    expected = {
        "kind",
        "schema_version",
        "run_id",
        "attempt",
        "mode",
        "as_of",
        "registry_signature",
        "plan_signature",
        "selected_scopes",
        "checkpoints",
        "manifest_sha256",
    }
    _exact_keys(raw, expected, "raw manifest")
    if raw["kind"] != RAW_MANIFEST_KIND or raw["schema_version"] != 1:
        raise RunnerConfigurationError("unsupported raw manifest")
    expected_hash = _sha256(raw["manifest_sha256"], "raw manifest.manifest_sha256")
    base = {key: value for key, value in raw.items() if key != "manifest_sha256"}
    if hashlib.sha256(_canonical_bytes(base)).hexdigest() != expected_hash:
        raise RunnerConfigurationError("raw manifest self-signature mismatch")
    selected_scopes = raw["selected_scopes"]
    if not isinstance(selected_scopes, list) or not all(
        type(scope_id) is str and scope_id for scope_id in selected_scopes
    ):
        raise RunnerConfigurationError("raw manifest selected scopes are invalid")
    if len(set(selected_scopes)) != len(selected_scopes):
        raise RunnerConfigurationError("raw manifest selected scopes are not unique")
    if selected_scopes != sorted(selected_scopes):
        raise RunnerConfigurationError("raw manifest selected scopes are not sorted")
    checkpoints = raw["checkpoints"]
    if not isinstance(checkpoints, list):
        raise RunnerConfigurationError("raw manifest checkpoints must be a list")
    seen: set[str] = set()
    for index, checkpoint_value in enumerate(checkpoints):
        field_name = f"raw manifest.checkpoints[{index}]"
        checkpoint = _mapping(checkpoint_value, field_name)
        _exact_keys(
            checkpoint,
            {"checkpoint_id", "scope_id", "endpoint", "requests"},
            field_name,
        )
        if checkpoint["checkpoint_id"] != index + 1:
            raise RunnerConfigurationError("raw checkpoint sequence is not contiguous")
        endpoint = checkpoint["endpoint"]
        if endpoint not in {"scoreboard", "summary"}:
            raise RunnerConfigurationError(f"{field_name}.endpoint is invalid")
        requests = checkpoint["requests"]
        if not isinstance(requests, list) or not requests:
            raise RunnerConfigurationError(f"{field_name}.requests must be non-empty")
        if endpoint == "summary" and len(requests) > SUMMARY_CHECKPOINT_SIZE:
            raise RunnerConfigurationError("Summary checkpoint exceeds 50 events")
        for request_index, request in enumerate(requests):
            validated = _validate_raw_request(
                request, f"{field_name}.requests[{request_index}]"
            )
            if validated["endpoint"] != endpoint:
                raise RunnerConfigurationError("raw checkpoint endpoint drift")
            if validated["scope_id"] != checkpoint["scope_id"]:
                raise RunnerConfigurationError("raw checkpoint scope drift")
            if validated["request_id"] in seen:
                raise RunnerConfigurationError("duplicate raw request identity")
            seen.add(validated["request_id"])
    return raw


def _load_raw_manifest(
    loaded: LoadedPlan, selected: Sequence[ScopePlan]
) -> tuple[dict[str, Any], bool]:
    try:
        payload = _read_artifact(loaded.raw_manifest_uri)
    except FileNotFoundError:
        if loaded.mode == "replay":
            raise RunnerConfigurationError("replay raw manifest is missing")
        return _seal_manifest(_manifest_base(loaded, selected)), False
    if loaded.mode == "replay":
        replay = loaded.replay_source
        assert replay is not None
        if hashlib.sha256(payload).hexdigest() != replay.raw_manifest_sha256:
            raise RunnerConfigurationError("replay raw manifest hash mismatch")
    try:
        manifest = _validate_raw_manifest(json.loads(payload.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunnerConfigurationError("raw manifest is not valid JSON") from exc
    expected_scopes = sorted(scope.scope_id for scope in selected)
    manifest_scopes = manifest["selected_scopes"]
    if loaded.mode == "replay":
        if not set(expected_scopes).issubset(manifest_scopes):
            raise RunnerConfigurationError("raw manifest scope selection mismatch")
    elif manifest_scopes != expected_scopes:
        raise RunnerConfigurationError("raw manifest scope selection mismatch")
    if manifest["as_of"] != loaded.plan.as_of.isoformat():
        raise RunnerConfigurationError("raw manifest as_of mismatch")
    if manifest["registry_signature"] != loaded.plan.registry_signature:
        raise RunnerConfigurationError("raw manifest registry mismatch")
    if loaded.mode == "replay":
        replay = loaded.replay_source
        assert replay is not None
        expected = (
            replay.run_id,
            replay.attempt,
            replay.mode,
            replay.plan_signature,
        )
        actual = (
            manifest["run_id"],
            manifest["attempt"],
            manifest["mode"],
            manifest["plan_signature"],
        )
        if actual != expected:
            raise RunnerConfigurationError("replay raw manifest identity mismatch")
    else:
        expected = (
            loaded.plan.run_id,
            loaded.attempt,
            loaded.mode,
            loaded.signature,
        )
        actual = (
            manifest["run_id"],
            manifest["attempt"],
            manifest["mode"],
            manifest["plan_signature"],
        )
        if actual != expected:
            raise RunnerConfigurationError("resume raw manifest identity mismatch")
    return manifest, True


def _raw_index(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        request["request_id"]: request
        for checkpoint in manifest["checkpoints"]
        for request in checkpoint["requests"]
    }


def _append_checkpoint(
    manifest: dict[str, Any],
    *,
    scope_id: str,
    endpoint: str,
    requests: Sequence[Mapping[str, Any]],
) -> None:
    if not requests:
        return
    if endpoint == "summary" and len(requests) > SUMMARY_CHECKPOINT_SIZE:
        raise AssertionError("Summary checkpoint exceeds hard limit")
    checkpoints = manifest["checkpoints"]
    checkpoints.append(
        {
            "checkpoint_id": len(checkpoints) + 1,
            "scope_id": scope_id,
            "endpoint": endpoint,
            "requests": [dict(item) for item in requests],
        }
    )
    sealed = _seal_manifest(manifest)
    manifest.clear()
    manifest.update(sealed)


def _persist_raw_manifest(uri: str, manifest: Mapping[str, Any]) -> None:
    _write_artifact(uri, _canonical_bytes(manifest), immutable=False)


def _raw_request_from_fetch(
    *,
    request_id: str,
    scope_id: str,
    endpoint: str,
    event_id: int | None,
    result: FetchResult,
    query_start: date | None,
    query_end: date | None,
) -> dict[str, Any]:
    if result.proxy_bytes != 0 or not 200 <= result.status <= 299:
        raise ScopeIncompleteError("ESPN capture did not produce direct successful raw")
    return {
        "request_id": request_id,
        "scope_id": scope_id,
        "endpoint": endpoint,
        "event_id": event_id,
        "url_fingerprint": result.target.url_fingerprint,
        "raw_uri": result.raw_uri,
        "raw_sha256": result.content_hash,
        "fetched_at": result.fetched_at,
        "http_status": result.status,
        "direct_bytes": result.direct_bytes,
        "proxy_bytes": result.proxy_bytes,
        "query_start": query_start.isoformat() if query_start else None,
        "query_end": query_end.isoformat() if query_end else None,
    }


def _load_exact_raw(raw_store: EspnRawStore, record: Mapping[str, Any]) -> bytes:
    return raw_store.load_exact(record["raw_uri"], record["raw_sha256"])


def _scoreboard_capture(
    request: ScoreboardRequest,
    *,
    scope: ScopePlan,
    raw_store: EspnRawStore,
    raw_index: dict[str, Mapping[str, Any]],
    http_client: Any | None,
    replay: bool,
) -> tuple[bytes, Mapping[str, Any], bool]:
    existing = raw_index.get(request.request_id)
    if existing is not None:
        expected_fingerprint = canonicalize_target(
            request.url, request.params
        ).url_fingerprint
        if (
            existing["scope_id"] != scope.scope_id
            or existing["endpoint"] != "scoreboard"
            or existing["url_fingerprint"] != expected_fingerprint
            or existing["query_start"] != request.query_start.isoformat()
            or existing["query_end"] != request.query_end.isoformat()
        ):
            raise RunnerConfigurationError("scoreboard raw request identity drift")
        return _load_exact_raw(raw_store, existing), existing, False
    if replay:
        raise ScopeIncompleteError(
            f"replay missing scoreboard raw {request.request_id}"
        )
    if http_client is None:
        raise AssertionError("network mode requires HTTP client")
    result = http_client.fetch_json(
        request.url,
        EndpointType.SCOREBOARD,
        request.params,
        competition_id=scope.espn_id,
        force_refresh=True,
    )
    record = _raw_request_from_fetch(
        request_id=request.request_id,
        scope_id=scope.scope_id,
        endpoint="scoreboard",
        event_id=None,
        result=result,
        query_start=request.query_start,
        query_end=request.query_end,
    )
    return result.body, record, True


def _summary_request_id(event_id: int) -> str:
    return f"summary:{event_id}"


def summary_refresh_event_ids(
    events: Iterable[ScheduleRow], prior: ScopeGeneration | None
) -> tuple[int, ...]:
    """Return played finals whose exact terminal Summary evidence is not reusable."""

    prior_schedule = (
        {row.event_id: row for row in prior.schedule} if prior is not None else {}
    )
    prior_summary_states: dict[int, list[DispositionState]] = {}
    prior_dispositions: dict[tuple[str, int], list[DispositionState]] = {}
    if prior is not None:
        for record in prior.raw_ledger:
            if record.endpoint == "summary" and record.event_id is not None:
                prior_summary_states.setdefault(record.event_id, []).append(
                    record.disposition
                )
        for disposition in prior.dispositions:
            if (
                disposition.endpoint in {"lineup", "matchsheet"}
                and disposition.event_id is not None
            ):
                prior_dispositions.setdefault(
                    (disposition.endpoint, disposition.event_id), []
                ).append(disposition.state)

    terminal = {DispositionState.CAPTURED, DispositionState.VALID_EMPTY}
    refresh: set[int] = set()
    for event in events:
        if not event.played_final:
            continue
        event_id = event.event_id
        prior_event = prior_schedule.get(event_id)
        dispositions_are_terminal = all(
            len(states := prior_dispositions.get((entity, event_id), [])) == 1
            and states[0] in terminal
            for entity in ("lineup", "matchsheet")
        )
        if (
            prior_event is None
            or prior_event != event
            or prior_summary_states.get(event_id) != [DispositionState.CAPTURED]
            or not dispositions_are_terminal
        ):
            refresh.add(event_id)
    return tuple(sorted(refresh))


def _summary_capture(
    event: ScheduleRow,
    *,
    scope: ScopePlan,
    raw_store: EspnRawStore,
    raw_index: dict[str, Mapping[str, Any]],
    http_client: Any | None,
    replay: bool,
) -> tuple[bytes, Mapping[str, Any], bool]:
    request_id = _summary_request_id(event.event_id)
    existing = raw_index.get(request_id)
    if existing is not None:
        url = SUMMARY_URL.format(slug=scope.slug)
        expected_fingerprint = canonicalize_target(
            url, {"event": event.event_id}
        ).url_fingerprint
        if (
            existing["scope_id"] != scope.scope_id
            or existing["endpoint"] != "summary"
            or existing["event_id"] != event.event_id
            or existing["url_fingerprint"] != expected_fingerprint
        ):
            raise RunnerConfigurationError("Summary raw request identity drift")
        return _load_exact_raw(raw_store, existing), existing, False
    if replay:
        raise ScopeIncompleteError(f"replay missing Summary raw for {event.event_id}")
    if http_client is None:
        raise AssertionError("network mode requires HTTP client")
    url = SUMMARY_URL.format(slug=scope.slug)
    params = {"event": event.event_id}
    result = http_client.fetch_json(
        url,
        EndpointType.SUMMARY,
        params,
        competition_id=scope.espn_id,
        event_id=event.event_id,
        force_refresh=True,
    )
    record = _raw_request_from_fetch(
        request_id=request_id,
        scope_id=scope.scope_id,
        endpoint="summary",
        event_id=event.event_id,
        result=result,
        query_start=None,
        query_end=None,
    )
    return result.body, record, True


def _ledger_from_raw(
    record: Mapping[str, Any], *, event_ids: tuple[int, ...] = ()
) -> RawLedgerRecord:
    return RawLedgerRecord(
        request_id=record["request_id"],
        endpoint=record["endpoint"],
        event_id=record["event_id"],
        disposition=DispositionState.CAPTURED,
        raw_uri=record["raw_uri"],
        raw_sha256=record["raw_sha256"],
        fetched_at=_parse_datetime(record["fetched_at"], "raw fetched_at"),
        direct_bytes=record["direct_bytes"],
        proxy_bytes=record["proxy_bytes"],
        event_ids=event_ids,
    )


def _merge_scoreboard_ledger(
    prior: ScopeGeneration | None,
    *,
    full: bool,
    fetched_event_ids: set[int],
    records: Sequence[Mapping[str, Any]],
    winner: Mapping[int, str],
) -> list[RawLedgerRecord]:
    output: list[RawLedgerRecord] = []
    if prior is not None and not full:
        new_request_ids = {record["request_id"] for record in records}
        for item in prior.raw_ledger:
            if item.endpoint != "scoreboard":
                continue
            retained = tuple(
                event_id
                for event_id in item.event_ids
                if event_id not in fetched_event_ids
            )
            if not retained:
                continue
            request_id = item.request_id
            if request_id in new_request_ids:
                request_id = f"{request_id}:retained:{(item.raw_sha256 or '')[:12]}"
            output.append(
                RawLedgerRecord(
                    **{
                        **item.constructor_values(),
                        "request_id": request_id,
                        "event_ids": retained,
                    }
                )
            )
    for record in records:
        bound = tuple(
            sorted(
                event_id
                for event_id, request_id in winner.items()
                if request_id == record["request_id"]
            )
        )
        output.append(_ledger_from_raw(record, event_ids=bound))
    return output


def _full_strategy(
    scope: ScopePlan, binding: ScopeBinding, mode: str, as_of: date
) -> bool:
    return (
        binding.initial_capture
        or mode in {"repair", "backfill"}
        or (mode == "daily" and is_full_reconciliation_day(scope.scope_id, as_of))
    )


def _disposition(
    *,
    entity: str,
    state: EntityParseState,
    event_id: int,
    capability: CapabilityState,
) -> RequestDisposition:
    disposition_state = DispositionState(state.value)
    if disposition_state is DispositionState.VALID_EMPTY and capability not in {
        CapabilityState.PARTIAL,
        CapabilityState.ABSENT,
        CapabilityState.UNKNOWN,
    }:
        raise ScopeIncompleteError(
            f"final event {event_id} has unresolved proven {entity}"
        )
    return RequestDisposition(
        endpoint=entity,
        state=disposition_state,
        detail=(
            "Summary parsed both native sides"
            if disposition_state is DispositionState.CAPTURED
            else f"successful Summary explicitly permits empty {entity}"
        ),
        event_id=event_id,
    )


@dataclass(slots=True)
class _BudgetState:
    max_events: int
    captured_events: int = 0

    @property
    def remaining(self) -> int:
        return self.max_events - self.captured_events


def _execute_scope(
    *,
    loaded: LoadedPlan,
    scope: ScopePlan,
    competition: Competition,
    edition: Edition,
    binding: ScopeBinding,
    prior: ScopeGeneration | None,
    raw_manifest: dict[str, Any],
    raw_store: EspnRawStore,
    http_client: Any | None,
    repository: RepositoryProtocol | None,
    budget_state: _BudgetState,
    publish: bool = True,
    staged_generations: dict[str, ScopeGeneration] | None = None,
) -> dict[str, Any]:
    if not binding.active:
        if prior is None:
            raise RunnerConfigurationError(
                f"scope {scope.scope_id} no-op requires prior COMPLETE identity"
            )
        return {
            "scope_id": scope.scope_id,
            "state": "noop",
            "generation_id": prior.generation_id,
            "manifest_sha256": prior.manifest_sha256,
            "row_counts": {
                "schedule": len(prior.schedule),
                "lineup": len(prior.lineup),
                "matchsheet": len(prior.matchsheet),
            },
        }

    mode = _effective_mode(loaded)
    requests = _scoreboard_requests(scope, binding, as_of=loaded.plan.as_of, mode=mode)
    if not requests:
        if prior is None:
            raise ScopeIncompleteError("active scope has no calendar query or prior")
        return {
            "scope_id": scope.scope_id,
            "state": "noop",
            "generation_id": prior.generation_id,
            "manifest_sha256": prior.manifest_sha256,
            "row_counts": {
                "schedule": len(prior.schedule),
                "lineup": len(prior.lineup),
                "matchsheet": len(prior.matchsheet),
            },
        }
    replay = loaded.mode == "replay"
    raw_index = _raw_index(raw_manifest)
    scoreboard_payloads: list[tuple[ScoreboardRequest, bytes, Mapping[str, Any]]] = []
    new_scoreboard_records: list[Mapping[str, Any]] = []
    try:
        for request in requests:
            body, record, is_new = _scoreboard_capture(
                request,
                scope=scope,
                raw_store=raw_store,
                raw_index=raw_index,
                http_client=http_client,
                replay=replay,
            )
            scoreboard_payloads.append((request, body, record))
            if is_new:
                new_scoreboard_records.append(record)
                raw_index[record["request_id"]] = record
    finally:
        if new_scoreboard_records:
            _append_checkpoint(
                raw_manifest,
                scope_id=scope.scope_id,
                endpoint="scoreboard",
                requests=new_scoreboard_records,
            )
            _persist_raw_manifest(loaded.raw_manifest_uri, raw_manifest)

    fetched_by_event: dict[int, ScheduleRow] = {}
    winner: dict[int, str] = {}
    scoreboard_records: list[Mapping[str, Any]] = []
    for request, body, record in scoreboard_payloads:
        rows = parse_scoreboards(
            body,
            competition=competition,
            edition=edition,
            query_start=request.query_start,
            query_end=request.query_end,
        )
        for row in rows:
            existing = fetched_by_event.get(row.event_id)
            if existing is not None and existing != row:
                raise ScopeIncompleteError(
                    f"conflicting scoreboard event {row.event_id}"
                )
            fetched_by_event[row.event_id] = row
            winner[row.event_id] = request.request_id
        scoreboard_records.append(record)
    missing_known = sorted(
        event.event_id
        for event in binding.known_nonterminal_events
        if event.event_id not in fetched_by_event
    )
    if missing_known:
        raise ScopeIncompleteError(
            f"known non-terminal events absent from scoreboard: {missing_known}"
        )
    full = _full_strategy(scope, binding, mode, loaded.plan.as_of)
    if not fetched_by_event and not full:
        assert prior is not None
        return {
            "scope_id": scope.scope_id,
            "state": "noop",
            "generation_id": prior.generation_id,
            "manifest_sha256": prior.manifest_sha256,
            "row_counts": {
                "schedule": len(prior.schedule),
                "lineup": len(prior.lineup),
                "matchsheet": len(prior.matchsheet),
            },
        }
    if full:
        schedule_by_event = dict(fetched_by_event)
    else:
        schedule_by_event = (
            {row.event_id: row for row in prior.schedule} if prior is not None else {}
        )
        schedule_by_event.update(fetched_by_event)
    schedule = tuple(
        sorted(schedule_by_event.values(), key=lambda row: (row.kickoff, row.event_id))
    )

    refresh_event_ids = set(
        summary_refresh_event_ids(fetched_by_event.values(), prior)
    )
    eligible = tuple(
        sorted(
            (
                row
                for row in fetched_by_event.values()
                if row.event_id in refresh_event_ids
            ),
            key=lambda row: row.event_id,
        )
    )
    missing = [
        event
        for event in eligible
        if _summary_request_id(event.event_id) not in raw_index
    ]
    allowed = budget_state.remaining
    capture_now = missing[:allowed]
    pending = missing[allowed:]
    for offset in range(0, len(capture_now), SUMMARY_CHECKPOINT_SIZE):
        chunk = capture_now[offset : offset + SUMMARY_CHECKPOINT_SIZE]
        new_records: list[Mapping[str, Any]] = []
        try:
            for event in chunk:
                _, record, is_new = _summary_capture(
                    event,
                    scope=scope,
                    raw_store=raw_store,
                    raw_index=raw_index,
                    http_client=http_client,
                    replay=replay,
                )
                if is_new:
                    new_records.append(record)
                    raw_index[record["request_id"]] = record
                    budget_state.captured_events += 1
        finally:
            if new_records:
                _append_checkpoint(
                    raw_manifest,
                    scope_id=scope.scope_id,
                    endpoint="summary",
                    requests=new_records,
                )
                _persist_raw_manifest(loaded.raw_manifest_uri, raw_manifest)
    if pending:
        raise ScopeIncompleteError(
            f"checkpoint pending {len(pending)} Summary events after max-events={budget_state.max_events}"
        )

    fetched_event_ids = set(fetched_by_event)
    prior_lineup = prior.lineup if prior is not None else ()
    prior_matchsheet = prior.matchsheet if prior is not None else ()
    prior_dispositions = prior.dispositions if prior is not None else ()
    retained_summary_event_ids = {
        event_id
        for event_id, event in schedule_by_event.items()
        if event_id not in refresh_event_ids
        and (event_id not in fetched_event_ids or event.played_final)
    }
    lineup = [
        row
        for row in prior_lineup
        if row.event_id in retained_summary_event_ids
    ]
    matchsheet = [
        row
        for row in prior_matchsheet
        if row.event_id in retained_summary_event_ids
    ]
    dispositions = [
        item
        for item in prior_dispositions
        if item.event_id in retained_summary_event_ids
    ]
    summary_ledger: list[RawLedgerRecord] = []
    if prior is not None:
        summary_ledger.extend(
            item
            for item in prior.raw_ledger
            if item.endpoint == "summary"
            and item.event_id in retained_summary_event_ids
        )
    for event in eligible:
        record = raw_index[_summary_request_id(event.event_id)]
        body = _load_exact_raw(raw_store, record)
        parsed = parse_summary(
            body,
            competition=competition,
            edition=edition,
            event=event,
        )
        if parsed.event_id != event.event_id or parsed.parser_version != PARSER_VERSION:
            raise ScopeIncompleteError(
                f"Summary parser identity drift for {event.event_id}"
            )
        lineup.extend(parsed.lineup)
        matchsheet.extend(parsed.matchsheet)
        dispositions.extend(
            (
                _disposition(
                    entity="lineup",
                    state=parsed.lineup_state,
                    event_id=event.event_id,
                    capability=scope.capabilities.lineup,
                ),
                _disposition(
                    entity="matchsheet",
                    state=parsed.matchsheet_state,
                    event_id=event.event_id,
                    capability=scope.capabilities.matchsheet,
                ),
            )
        )
        summary_ledger.append(_ledger_from_raw(record))

    scoreboard_ledger = _merge_scoreboard_ledger(
        prior,
        full=full,
        fetched_event_ids=set(fetched_by_event),
        records=scoreboard_records,
        winner=winner,
    )
    raw_ledger = tuple(
        sorted((*scoreboard_ledger, *summary_ledger), key=lambda item: item.request_id)
    )
    request_ids = tuple(item.request_id for item in raw_ledger)
    if len(request_ids) != len(set(request_ids)):
        raise ScopeIncompleteError("merged raw ledger request identity collision")
    generation = ScopeGeneration(
        plan=scope,
        run_id=loaded.plan.run_id,
        generation_id=binding.generation_id,
        registry_snapshot_uri=loaded.registry_snapshot_uri,
        registry_signature=loaded.plan.registry_signature,
        plan_signature=loaded.signature,
        parser_version=PARSER_VERSION,
        runtime_version=RUNTIME_VERSION,
        ingested_at=binding.ingested_at,
        batch_id=binding.batch_id,
        schedule=schedule,
        lineup=tuple(
            sorted(lineup, key=lambda row: (row.event_id, row.team_id, row.athlete_id))
        ),
        matchsheet=tuple(
            sorted(matchsheet, key=lambda row: (row.event_id, row.team_id))
        ),
        planned_request_ids=request_ids,
        raw_ledger=raw_ledger,
        dispositions=tuple(
            sorted(dispositions, key=lambda item: (item.event_id or 0, item.endpoint))
        ),
    )
    report = validate_scope_generation(generation)
    if not report.passed:
        raise ScopeIncompleteError("; ".join(report.failures))
    snapshot = scope_snapshot_bytes(generation)
    _assert_immutable_target(binding.generation_snapshot_uri, snapshot)
    _write_artifact(binding.generation_snapshot_uri, snapshot, immutable=True)
    if staged_generations is not None:
        staged_generations[scope.scope_id] = generation
    if not publish:
        return {
            "scope_id": scope.scope_id,
            "state": "staged",
            "generation_id": generation.generation_id,
            "generation_signature": generation.generation_signature,
            "manifest_sha256": generation.manifest_sha256,
            "generation_snapshot_uri": binding.generation_snapshot_uri,
            "generation_snapshot_sha256": hashlib.sha256(snapshot).hexdigest(),
            "row_counts": {
                "schedule": len(generation.schedule),
                "lineup": len(generation.lineup),
                "matchsheet": len(generation.matchsheet),
            },
        }
    if repository is None:
        raise AssertionError("publication requires a repository")
    publication = repository.publish_scope(generation)
    if publication.state not in {
        ScopePublicationState.PUBLISHED,
        ScopePublicationState.IDEMPOTENT,
    }:
        raise ScopeIncompleteError(
            publication.error or "repository did not complete scope publication"
        )
    if publication.manifest_sha256 != generation.manifest_sha256:
        raise ScopeIncompleteError("repository manifest identity mismatch")
    return {
        "scope_id": scope.scope_id,
        "state": "complete",
        "generation_id": generation.generation_id,
        "generation_signature": generation.generation_signature,
        "manifest_sha256": generation.manifest_sha256,
        "generation_snapshot_uri": binding.generation_snapshot_uri,
        "generation_snapshot_sha256": hashlib.sha256(snapshot).hexdigest(),
        "row_counts": {
            "schedule": len(generation.schedule),
            "lineup": len(generation.lineup),
            "matchsheet": len(generation.matchsheet),
        },
    }


def _default_http_client(raw_store: EspnRawStore, selected_count: int, max_events: int):
    budget = TaskBudget(
        max_competitions=min(selected_count, DEFAULT_MAX_COMPETITIONS),
        max_summary_events=max_events,
        max_requests=DEFAULT_MAX_REQUESTS,
        max_bytes=DEFAULT_MAX_TASK_BYTES,
    )
    return EspnHttpClient(raw_store, budget=budget)


def stage(
    options: ExecutionOptions,
    *,
    raw_store: EspnRawStore | None = None,
) -> StagingResult:
    """Build validated generation snapshots from exact Raw with zero HTTP.

    This is the Task 6 phase seam.  It deliberately has no HTTP-client or
    repository argument: missing Raw evidence makes the scope incomplete, and
    publication can happen only after a separate staging-DQ task.
    """

    if not isinstance(options, ExecutionOptions):
        raise TypeError("options must be ExecutionOptions")
    loaded = _load_signed_plan(options.plan_uri)
    selected = _validate_options(options, loaded)
    _preflight_artifact_uris(options, loaded, selected)
    registry = _load_registry(
        loaded.registry_snapshot_uri, loaded.plan.registry_signature
    )
    registry_scopes = {
        scope.scope_id: _registry_scope(registry, scope) for scope in selected
    }
    priors: dict[str, ScopeGeneration | None] = {}
    for scope in selected:
        prior_binding = loaded.bindings[scope.scope_id].prior
        priors[scope.scope_id] = (
            _load_prior(prior_binding, scope) if prior_binding is not None else None
        )
    raw_manifest, existed = _load_raw_manifest(loaded, selected)
    if not existed:
        return StagingResult(
            exit_code=1,
            payload={
                "kind": "espn-native-staging-result-v1",
                "schema_version": 1,
                "run_id": loaded.plan.run_id,
                "attempt": loaded.attempt,
                "plan_signature": loaded.signature,
                "state": "incomplete",
                "scopes": [
                    {
                        "scope_id": scope.scope_id,
                        "state": "incomplete",
                        "error": "exact raw manifest is missing",
                    }
                    for scope in selected
                ],
            },
            generations={},
        )
    active = [scope for scope in selected if loaded.bindings[scope.scope_id].active]
    if active and raw_store is None:
        raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    budget_state = _BudgetState(options.max_events)
    generations: dict[str, ScopeGeneration] = {}
    scope_results: list[dict[str, Any]] = []
    for scope in selected:
        competition, edition = registry_scopes[scope.scope_id]
        try:
            result = _execute_scope(
                loaded=loaded,
                scope=scope,
                competition=competition,
                edition=edition,
                binding=loaded.bindings[scope.scope_id],
                prior=priors[scope.scope_id],
                raw_manifest=raw_manifest,
                raw_store=raw_store,
                http_client=None,
                repository=None,
                budget_state=budget_state,
                publish=False,
                staged_generations=generations,
            )
        except RunnerConfigurationError:
            raise
        except Exception as exc:
            result = {
                "scope_id": scope.scope_id,
                "state": "incomplete",
                "error": f"{type(exc).__name__}: {exc}",
            }
        scope_results.append(result)
    incomplete = any(item["state"] == "incomplete" for item in scope_results)
    staged = any(item["state"] == "staged" for item in scope_results)
    state = "incomplete" if incomplete else ("staged" if staged else "noop")
    payload_base = {
        "kind": "espn-native-staging-result-v1",
        "schema_version": 1,
        "run_id": loaded.plan.run_id,
        "attempt": loaded.attempt,
        "mode": loaded.mode,
        "as_of": loaded.plan.as_of.isoformat(),
        "plan_signature": loaded.signature,
        "registry_signature": loaded.plan.registry_signature,
        "raw_manifest_uri": loaded.raw_manifest_uri,
        "raw_manifest_sha256": hashlib.sha256(
            _canonical_bytes(raw_manifest)
        ).hexdigest(),
        "state": state,
        "scopes": scope_results,
    }
    payload = {
        **payload_base,
        "staging_result_sha256": hashlib.sha256(
            _canonical_bytes(payload_base)
        ).hexdigest(),
    }
    return StagingResult(
        exit_code=1 if incomplete else 0,
        payload=payload,
        generations=generations,
    )


def execute(
    options: ExecutionOptions,
    *,
    repository: RepositoryProtocol | None = None,
    raw_store: EspnRawStore | None = None,
    http_client: Any | None = None,
) -> ExecutionResult:
    """Execute one signed plan selection and atomically emit its verdict."""

    if not isinstance(options, ExecutionOptions):
        raise TypeError("options must be ExecutionOptions")
    loaded = _load_signed_plan(options.plan_uri)
    selected = _validate_options(options, loaded)
    _preflight_artifact_uris(options, loaded, selected)
    registry = _load_registry(
        loaded.registry_snapshot_uri, loaded.plan.registry_signature
    )
    registry_scopes = {
        scope.scope_id: _registry_scope(registry, scope) for scope in selected
    }
    priors: dict[str, ScopeGeneration | None] = {}
    for scope in selected:
        prior_binding = loaded.bindings[scope.scope_id].prior
        priors[scope.scope_id] = (
            _load_prior(prior_binding, scope) if prior_binding is not None else None
        )
    raw_manifest, existed = _load_raw_manifest(loaded, selected)
    if loaded.mode != "replay" and not existed:
        _persist_raw_manifest(loaded.raw_manifest_uri, raw_manifest)
    active = [scope for scope in selected if loaded.bindings[scope.scope_id].active]
    if loaded.mode == "replay" and http_client is not None:
        raise RunnerConfigurationError("replay forbids an HTTP client")
    if active and raw_store is None:
        raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    if loaded.mode != "replay" and active and http_client is None:
        assert raw_store is not None
        http_client = _default_http_client(raw_store, len(selected), options.max_events)
    if active and repository is None:
        repository = EspnBronzeRepository()
    budget_state = _BudgetState(options.max_events)
    scope_results: list[dict[str, Any]] = []
    for scope in selected:
        competition, edition = registry_scopes[scope.scope_id]
        try:
            result = _execute_scope(
                loaded=loaded,
                scope=scope,
                competition=competition,
                edition=edition,
                binding=loaded.bindings[scope.scope_id],
                prior=priors[scope.scope_id],
                raw_manifest=raw_manifest,
                raw_store=raw_store,
                http_client=http_client,
                repository=repository,
                budget_state=budget_state,
            )
        except ArtifactConflictError as exc:
            result = {
                "scope_id": scope.scope_id,
                "state": "incomplete",
                "error": f"{type(exc).__name__}: {exc}",
            }
        except RunnerConfigurationError:
            raise
        except Exception as exc:
            result = {
                "scope_id": scope.scope_id,
                "state": "incomplete",
                "error": f"{type(exc).__name__}: {exc}",
            }
        scope_results.append(result)
    incomplete = any(item["state"] == "incomplete" for item in scope_results)
    complete = any(item["state"] == "complete" for item in scope_results)
    state = "incomplete" if incomplete else ("complete" if complete else "noop")
    raw_manifest_sha256 = hashlib.sha256(_canonical_bytes(raw_manifest)).hexdigest()
    payload_base = {
        "kind": "espn-native-run-result-v1",
        "schema_version": 1,
        "run_id": loaded.plan.run_id,
        "attempt": loaded.attempt,
        "mode": loaded.mode,
        "as_of": loaded.plan.as_of.isoformat(),
        "plan_signature": loaded.signature,
        "registry_snapshot_uri": loaded.registry_snapshot_uri,
        "registry_signature": loaded.plan.registry_signature,
        "raw_manifest_uri": loaded.raw_manifest_uri,
        "raw_manifest_sha256": raw_manifest_sha256,
        "state": state,
        "scopes": scope_results,
    }
    payload = {
        **payload_base,
        "result_sha256": hashlib.sha256(_canonical_bytes(payload_base)).hexdigest(),
    }
    result_bytes = _canonical_bytes(payload)
    try:
        existing_result = _read_artifact(loaded.output_uri)
    except FileNotFoundError:
        existing_result = None
    if existing_result is not None and existing_result != result_bytes:
        try:
            existing_payload = json.loads(existing_result.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ArtifactConflictError(
                f"existing run result is corrupt at {loaded.output_uri}"
            ) from exc
        same_identity = all(
            existing_payload.get(key) == payload.get(key)
            for key in ("run_id", "attempt", "mode", "plan_signature")
        )
        if not same_identity or existing_payload.get("state") != "incomplete":
            raise ArtifactConflictError(
                f"terminal run result conflict at {loaded.output_uri}"
            )
    _write_artifact(loaded.output_uri, result_bytes, immutable=False)
    return ExecutionResult(exit_code=1 if incomplete else 0, payload=payload)


__all__ = [
    "ArtifactConflictError",
    "ExecutionOptions",
    "ExecutionResult",
    "RUNTIME_VERSION",
    "RunnerConfigurationError",
    "RunnerError",
    "ScopeIncompleteError",
    "StagingResult",
    "execute",
    "is_full_reconciliation_day",
    "load_scope_snapshot",
    "scope_snapshot_bytes",
    "stage",
]
