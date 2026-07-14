#!/usr/bin/env python3
"""Provider-metered, fixed-cohort SofaScore paid-proxy canary.

The collector is deliberately separate from production capture authorization:

* every cold observation uses a fresh local raw store, manifest, budget ledger,
  proxy lease and browser session;
* the proxy lease source is always ``sofascore_canary`` and requires an
  operator-supplied experimental byte cap;
* only hashes of validated proxy exits are retained; lease tokens, raw exits
  and raw response bodies never enter the benchmark artifact;
* collection always leaves ``verified=false``;
* the separate ``verify`` command atomically flips that one flag only after
  the complete >=20-run / >=5-exit policy and fixed-cohort evidence validates.

Which classes exist, and which tournaments each one is measured on, is declared
by ``configs/sofascore/proxy_canary_classes.json`` alone; this module recomputes
every class name and shape digest from the production code shapes, so a class can
never be re-pointed at another league by editing a config.  Cold runs are spread
over a class' collection targets in round-robin, and ``verify`` rejects a class
whose samples are skewed below the even floor - a class that generalizes must be
evidence of a shape, not of one league.  A verified artifact is immutable: the
``extend`` command opens the next candidate from it (carrying the cold samples
over only while the runtime fingerprint is byte-for-byte identical).

The zero-network ``no_op`` and ``offline_replay`` observations and the paid
``single_endpoint_resume`` observation are benchmark-only.  They are required
evidence but never participate in production budget derivation.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import ipaddress
import json
import math
import os
import re
import sys
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "dags"))
sys.path.insert(0, str(ROOT))

from pyarrow import fs  # noqa: E402

from scrapers.sofascore.capture_engine import (  # noqa: E402
    HttpPayload,
    NullCaptureSink,
    ProviderBudgetToken,
    RetryPolicy,
    SofaScoreCaptureEngine,
    TransportError,
)
from scrapers.sofascore.live_capture import (  # noqa: E402
    LeaseBackedCamoufoxTransport,
    capture_live_dynamic_specs,
    capture_live_specs,
)
from scrapers.sofascore.manifest import (  # noqa: E402
    InMemoryManifestStore,
    JsonFileManifestStore,
    ManifestStatus,
)
from scrapers.sofascore.pipeline import (  # noqa: E402
    EVENT_PATHS,
    PLAYER_PATHS,
    build_event_spec,
    build_player_spec,
)
from scrapers.sofascore.raw_store import (  # noqa: E402
    PayloadTarget,
    RawPayloadStore,
)
from scrapers.sofascore.season_pipeline import (  # noqa: E402
    plan_season_partition,
)
from scrapers.sofascore.workload_plan import (  # noqa: E402
    MIN_MEASURED_TOURNAMENTS_FOR_TRANSFER,
    SEASON_DYNAMIC_ENDPOINTS,
    SEASON_STATIC_ENDPOINTS,
    WORKLOAD_ARTIFACT_SCHEMA_VERSION,
    WorkloadPlanError,
    production_match_shape,
    production_player_shape,
    production_season_shape,
    tournament_canonical_url,
    workload_class_name,
    workload_shape_digest,
)
from scrapers.sofascore.lease_client import redact_sensitive  # noqa: E402
from scrapers.sofascore.runtime_fingerprint import (  # noqa: E402
    RuntimeFingerprintError,
    runtime_fingerprint,
    validate_runtime_fingerprint,
)
from scrapers.utils.rate_limiter import RATE_LIMITS, get_rate_limiter  # noqa: E402
from scripts.proxy_filter.budget import (  # noqa: E402
    BUDGET_DERIVATION,
    MIN_CANARY_RUNS,
    MIN_DISTINCT_PROXY_EXITS,
    REQUIRED_BENCHMARK_MODES,
    REQUIRED_METRICS,
    BudgetPolicy,
    ProductionBudgetUnavailable,
    SharedBudgetLedger,
    append_canary_sample,
    experimental_canary_policy_id,
    load_verified_policy,
)


COLLECTOR_VERSION = "sofascore-paid-canary-v3"
METER = "proxy_filter_provider_path_v2"
EXPECTED_MATCHES = 25
EXPECTED_PLAYERS = 50
CANARY_DAG_ID = "dag_canary_sofascore_proxy"
CANARY_SOURCE = "sofascore_canary"
CONFIG_DIR = ROOT / "configs" / "sofascore"
DEFAULT_COHORT_PATH = CONFIG_DIR / "proxy_canary_cohort.json"
DEFAULT_CLASS_MANIFEST_PATH = CONFIG_DIR / "proxy_canary_classes.json"
DEFAULT_ARTIFACT_PATH = CONFIG_DIR / "proxy_budget_canary.json"
DEFAULT_WORKSPACE = Path("/tmp/sofascore-paid-canary")
BENCHMARK_ONLY_MODES = frozenset({"no_op", "offline_replay", "single_endpoint_resume"})
CLASS_MANIFEST_SCHEMA_VERSION = 1
SEASON_ENDPOINTS = tuple(SEASON_STATIC_ENDPOINTS + SEASON_DYNAMIC_ENDPOINTS)
# Units of one full measured task; they are code constants of the production
# capture, never a manifest choice.
SCOPE_MAX_UNITS = {"match": EXPECTED_MATCHES, "player": EXPECTED_PLAYERS, "season": 1}
_SEASON_SHAPE_PARAMS = frozenset(
    {"season_format", "team_count_band", "max_pages_per_direction"}
)
# Everything a class declares before its first paid byte.  Collection may only
# ever append samples and raise ``hard_task_bytes``.
IMMUTABLE_CLASS_FIELDS = (
    "scope",
    "max_units",
    "required_endpoints",
    "shape",
    "shape_digest",
    "measured_tournament_ids",
    "cohorts",
    "representative_season_ids",
    "collection_blocker",
)
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_IPV4_RE = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")
_FORBIDDEN_EVIDENCE_KEYS = frozenset(
    {
        "authorization",
        "body",
        "headers",
        "password",
        "proxy_url",
        "raw_ip",
        "raw_payload",
        "response_body",
        "secret",
        "token",
    }
)


class CanaryPolicyError(RuntimeError):
    """The canary cannot be collected or promoted safely."""


@dataclass(frozen=True)
class CanaryCohort:
    payload: Mapping[str, Any]
    digest: str
    source_tournament_id: int
    source_season_id: int
    canonical_competition: str
    canonical_season: str
    match_ids: tuple[str, ...]
    player_ids: tuple[str, ...]


@dataclass(frozen=True)
class CollectionTarget:
    """One tournament a class is measured on, with its frozen source evidence."""

    source_tournament_id: int
    tournament_slug: str
    cohort_path: Path
    cohort_name: str
    representative_season_id: Optional[int] = None

    @property
    def canonical_url(self) -> str:
        return tournament_canonical_url(
            self.tournament_slug, self.source_tournament_id
        )


@dataclass(frozen=True)
class ClassSpec:
    """One measured workload class: exactly one (scope, byte-driving shape)."""

    key: str
    name: str
    scope: str
    shape: Mapping[str, Any]
    shape_digest: str
    max_units: int
    required_endpoints: tuple[str, ...]
    targets: tuple[CollectionTarget, ...]

    @property
    def measured_tournament_ids(self) -> tuple[int, ...]:
        return tuple(target.source_tournament_id for target in self.targets)

    def target_for(self, source_tournament_id: object) -> CollectionTarget:
        for target in self.targets:
            if target.source_tournament_id == source_tournament_id:
                return target
        raise CanaryPolicyError(
            f"workload class {self.name!r} is not measured on tournament "
            f"{source_tournament_id!r}"
        )


@dataclass(frozen=True)
class ClassManifest:
    """The declared classes; names and digests are always recomputed here."""

    path: Path
    digest: str
    classes: Mapping[str, ClassSpec]

    @property
    def required_workload_classes(self) -> tuple[str, ...]:
        return tuple(self.classes)

    def spec(self, workload_class: object) -> ClassSpec:
        try:
            return self.classes[workload_class]  # type: ignore[index]
        except (KeyError, TypeError) as exc:
            raise CanaryPolicyError(
                f"unsupported workload class {workload_class!r}"
            ) from exc


@dataclass
class ColdRunState:
    run_id: str
    class_spec: ClassSpec
    target: CollectionTarget
    cohort: CanaryCohort
    experimental_cap_bytes: int
    specs: tuple[Any, ...]
    raw_store: "CountingRawPayloadStore"
    manifest_store: JsonFileManifestStore
    results: tuple[Any, ...]
    sample: dict[str, Any]
    root: Path

    @property
    def workload_class(self) -> str:
        return self.class_spec.name


class CountingRawPayloadStore(RawPayloadStore):
    """Raw store used to prove each target pointer was committed exactly once."""

    def __init__(self, filesystem: fs.FileSystem, root: str) -> None:
        super().__init__(filesystem, root)
        self.write_counts: dict[PayloadTarget, int] = {}

    def store_bytes(self, target: PayloadTarget, body: bytes, **kwargs):
        record = super().store_bytes(target, body, **kwargs)
        self.write_counts[target] = self.write_counts.get(target, 0) + 1
        return record

    def reset_write_counts(self) -> None:
        self.write_counts.clear()


class NetworkForbiddenTransport:
    """A tripwire proving replay and no-op modes never touch a source."""

    def request(self, url: str, *, provider_budget: Optional[ProviderBudgetToken]):
        raise AssertionError(
            f"network-disabled benchmark attempted source access: {url}"
        )


class RecordingCanaryTransport:
    """Record exact provider bytes for every live transport request.

    The wrapped production adapter still owns the lease, browser, pacing and
    control-plane meter.  This wrapper records only endpoint names and integer
    deltas already anonymized by that adapter.
    """

    paces_requests = True

    def __init__(
        self, wrapped: LeaseBackedCamoufoxTransport, expected_cap: int
    ) -> None:
        self.wrapped = wrapped
        self.expected_cap = expected_cap
        self.request_observations: dict[str, list[int]] = {}

    def __enter__(self) -> "RecordingCanaryTransport":
        self.wrapped.__enter__()
        snapshot = self.wrapped.provider_snapshot()
        if int(snapshot["provider_total_bytes"]) != 0:
            self.wrapped.close()
            raise CanaryPolicyError(
                "fresh canary lease already contains provider bytes"
            )
        if int(snapshot["provider_budget_bytes"]) != self.expected_cap:
            self.wrapped.close()
            raise CanaryPolicyError(
                "proxy-filter canary cap does not match the operator-supplied cap"
            )
        return self

    def _record(self, endpoint: str, provider_bytes: object) -> None:
        if (
            isinstance(provider_bytes, bool)
            or not isinstance(provider_bytes, int)
            or provider_bytes < 0
        ):
            raise CanaryPolicyError("live request omitted exact provider bytes")
        self.request_observations.setdefault(endpoint, []).append(provider_bytes)

    def request(
        self,
        url: str,
        *,
        provider_budget: Optional[ProviderBudgetToken],
    ) -> HttpPayload:
        if provider_budget is None:
            raise CanaryPolicyError("canary request has no endpoint reservation")
        endpoint = provider_budget.endpoint
        try:
            payload = self.wrapped.request(url, provider_budget=provider_budget)
        except TransportError as exc:
            self._record(endpoint, exc.provider_bytes)
            raise
        self._record(endpoint, payload.provider_bytes)
        return payload

    def provider_snapshot(self):
        return self.wrapped.provider_snapshot()

    def close(self):
        return self.wrapped.close()

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        return self.wrapped.__exit__(exc_type, exc_val, exc_tb)


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise CanaryPolicyError(f"{label} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise CanaryPolicyError(f"{label} must be a positive integer") from exc
    if parsed <= 0 or str(value).strip() != str(parsed):
        raise CanaryPolicyError(f"{label} must be a positive integer")
    return parsed


def _canonical_digest(payload: Mapping[str, Any]) -> str:
    rendered = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _collection_target(
    entry: object,
    *,
    key: str,
    scope: str,
    config_dir: Path,
) -> CollectionTarget:
    if not isinstance(entry, Mapping):
        raise CanaryPolicyError(f"class {key!r} has a malformed collection target")
    tournament_id = _positive_int(
        entry.get("source_tournament_id"), f"{key}.source_tournament_id"
    )
    slug = str(entry.get("tournament_slug") or "").strip()
    cohort_name = str(entry.get("cohort_name") or "").strip()
    raw_cohort_path = str(entry.get("cohort_path") or "").strip()
    if not slug or not cohort_name or not raw_cohort_path:
        raise CanaryPolicyError(
            f"class {key!r} target {tournament_id} needs a slug, cohort_path "
            "and cohort_name"
        )
    cohort_path = Path(raw_cohort_path)
    if cohort_path.is_absolute() or ".." in cohort_path.parts:
        raise CanaryPolicyError(
            f"class {key!r} cohort_path must stay inside the manifest directory"
        )
    try:
        tournament_canonical_url(slug, tournament_id)
    except WorkloadPlanError as exc:
        raise CanaryPolicyError(f"class {key!r} has an unsafe warm anchor") from exc
    season_id = entry.get("representative_season_id")
    if scope == "season":
        season_id = _positive_int(season_id, f"{key}.representative_season_id")
    elif season_id is not None:
        raise CanaryPolicyError(
            f"{scope} class {key!r} must not pin a representative season"
        )
    return CollectionTarget(
        source_tournament_id=tournament_id,
        tournament_slug=slug,
        cohort_path=config_dir / cohort_path,
        cohort_name=cohort_name,
        representative_season_id=season_id if scope == "season" else None,
    )


def _class_spec(entry: object, *, config_dir: Path) -> ClassSpec:
    if not isinstance(entry, Mapping):
        raise CanaryPolicyError("canary class manifest entries must be objects")
    key = str(entry.get("key") or "").strip()
    scope = str(entry.get("scope") or "").strip()
    if not key:
        raise CanaryPolicyError("canary class manifest entry needs a key")
    if scope not in SCOPE_MAX_UNITS:
        raise CanaryPolicyError(f"class {key!r} has an unsupported scope {scope!r}")
    raw_params = entry.get("shape_params")
    try:
        if scope == "season":
            if not isinstance(raw_params, Mapping) or set(raw_params) != (
                _SEASON_SHAPE_PARAMS
            ):
                raise CanaryPolicyError(
                    f"season class {key!r} needs exactly the shape parameters "
                    + ", ".join(sorted(_SEASON_SHAPE_PARAMS))
                )
            shape = production_season_shape(
                season_format=str(raw_params.get("season_format") or ""),
                team_count_band=str(raw_params.get("team_count_band") or ""),
                max_pages_per_direction=_positive_int(
                    raw_params.get("max_pages_per_direction"),
                    f"{key}.max_pages_per_direction",
                ),
            )
            required_endpoints = tuple(sorted(SEASON_ENDPOINTS))
        else:
            if raw_params is not None:
                raise CanaryPolicyError(
                    f"{scope} class {key!r} is measured with the production code "
                    "shape and cannot declare shape parameters"
                )
            shape = (
                production_match_shape()
                if scope == "match"
                else production_player_shape()
            )
            required_endpoints = tuple(
                sorted(EVENT_PATHS if scope == "match" else PLAYER_PATHS)
            )
        shape_digest = workload_shape_digest(shape)
        name = workload_class_name(scope, shape_digest)
    except WorkloadPlanError as exc:
        raise CanaryPolicyError(
            f"class {key!r} does not describe a measurable shape: {exc}"
        ) from exc
    raw_targets = entry.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise CanaryPolicyError(f"class {key!r} declares no collection target")
    targets = tuple(
        sorted(
            (
                _collection_target(
                    item, key=key, scope=scope, config_dir=config_dir
                )
                for item in raw_targets
            ),
            key=lambda target: target.source_tournament_id,
        )
    )
    if len({target.source_tournament_id for target in targets}) != len(targets):
        raise CanaryPolicyError(f"class {key!r} declares one tournament twice")
    return ClassSpec(
        key=key,
        name=name,
        scope=scope,
        shape=dict(shape),
        shape_digest=shape_digest,
        max_units=SCOPE_MAX_UNITS[scope],
        required_endpoints=required_endpoints,
        targets=targets,
    )


def load_class_manifest(
    path: os.PathLike[str] | str = DEFAULT_CLASS_MANIFEST_PATH,
) -> ClassManifest:
    """Load the class declaration; the collector never hardcodes a tournament."""

    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanaryPolicyError("canary class manifest is unreadable") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != CLASS_MANIFEST_SCHEMA_VERSION
    ):
        raise CanaryPolicyError("unsupported canary class manifest")
    entries = payload.get("classes")
    if not isinstance(entries, list) or not entries:
        raise CanaryPolicyError("canary class manifest declares no class")
    classes: dict[str, ClassSpec] = {}
    keys: set[str] = set()
    for entry in entries:
        spec = _class_spec(entry, config_dir=manifest_path.parent)
        if spec.key in keys:
            raise CanaryPolicyError(
                f"canary class manifest declares key {spec.key!r} twice"
            )
        keys.add(spec.key)
        if spec.name in classes:
            raise CanaryPolicyError(
                f"canary class manifest declares the same {spec.scope} shape "
                f"in {classes[spec.name].key!r} and {spec.key!r}"
            )
        classes[spec.name] = spec
    return ClassManifest(
        path=manifest_path,
        digest=_canonical_digest(payload),
        classes=dict(sorted(classes.items())),
    )


CLASS_MANIFEST = load_class_manifest()
REQUIRED_WORKLOAD_CLASSES = CLASS_MANIFEST.required_workload_classes


def load_fixed_cohort(
    path: os.PathLike[str] | str = DEFAULT_COHORT_PATH,
) -> CanaryCohort:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanaryPolicyError("fixed canary cohort is unreadable") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise CanaryPolicyError("unsupported fixed canary cohort")
    tournament_id = _positive_int(
        payload.get("source_tournament_id"), "cohort source_tournament_id"
    )
    season_id = _positive_int(
        payload.get("source_season_id"), "cohort source_season_id"
    )
    for name in ("cohort", "canonical_competition", "canonical_season"):
        if not isinstance(payload.get(name), str) or not payload[name].strip():
            raise CanaryPolicyError(f"fixed canary cohort has invalid {name}")
    match_ids = payload.get("match_ids")
    player_ids = payload.get("player_ids")
    if (
        not isinstance(match_ids, list)
        or len(match_ids) not in {0, EXPECTED_MATCHES}
        or len(set(match_ids)) != len(match_ids)
        or any(not isinstance(value, str) or not value.isdigit() for value in match_ids)
    ):
        raise CanaryPolicyError(
            "fixed canary cohort must contain zero or 25 unique match IDs"
        )
    if not match_ids and not str(payload.get("match_collection_blocker") or "").strip():
        raise CanaryPolicyError("empty match cohort needs a collection blocker")
    if (
        not isinstance(player_ids, list)
        or len(player_ids) not in {0, EXPECTED_PLAYERS}
        or len(set(player_ids)) != len(player_ids)
        or any(
            not isinstance(value, str) or not value.isdigit() for value in player_ids
        )
    ):
        raise CanaryPolicyError(
            "fixed canary cohort must contain zero or 50 unique player IDs"
        )
    if not player_ids and not str(
        payload.get("player_collection_blocker") or ""
    ).strip():
        raise CanaryPolicyError("empty player cohort needs a collection blocker")
    return CanaryCohort(
        payload=payload,
        digest=_canonical_digest(payload),
        source_tournament_id=tournament_id,
        source_season_id=season_id,
        canonical_competition=str(payload["canonical_competition"]).strip(),
        canonical_season=str(payload["canonical_season"]).strip(),
        match_ids=tuple(match_ids),
        player_ids=tuple(player_ids),
    )


def load_target_cohort(target: CollectionTarget) -> CanaryCohort:
    """Load the frozen cohort of one collection target and pin its tournament."""

    cohort = load_fixed_cohort(target.cohort_path)
    if cohort.source_tournament_id != target.source_tournament_id:
        raise CanaryPolicyError(
            f"cohort {target.cohort_path.name!r} belongs to tournament "
            f"{cohort.source_tournament_id}, not {target.source_tournament_id}"
        )
    if (
        target.representative_season_id is not None
        and cohort.source_season_id != target.representative_season_id
    ):
        raise CanaryPolicyError(
            f"cohort {target.cohort_path.name!r} does not carry the "
            "representative season of its season class"
        )
    return cohort


def build_fixed_specs(cohort: CanaryCohort) -> tuple[Any, ...]:
    freshness_key = f"canary-{cohort.digest[:16]}"
    specs = []
    for match_id in cohort.match_ids:
        for endpoint in EVENT_PATHS:
            specs.append(
                build_event_spec(
                    source_tournament_id=cohort.source_tournament_id,
                    source_season_id=cohort.source_season_id,
                    target_id=match_id,
                    endpoint=endpoint,
                    freshness_key=freshness_key,
                    paid_proxy=True,
                )
            )
    for player_id in cohort.player_ids:
        for endpoint in PLAYER_PATHS:
            specs.append(
                build_player_spec(
                    source_tournament_id=cohort.source_tournament_id,
                    source_season_id=cohort.source_season_id,
                    target_id=player_id,
                    endpoint=endpoint,
                    freshness_key=freshness_key,
                    paid_proxy=True,
                )
            )
    keys = [spec.key for spec in specs]
    expected = len(cohort.match_ids) * len(EVENT_PATHS) + len(
        cohort.player_ids
    ) * len(PLAYER_PATHS)
    if len(specs) != expected or len(keys) != len(set(keys)):
        raise CanaryPolicyError(
            f"fixed cohort must produce exactly {expected} unique specs"
        )
    return tuple(specs)


def _production_rate_limiter():
    limiter = get_rate_limiter("sofascore")
    expected = RATE_LIMITS["sofascore"]
    observed = limiter.config
    if (
        observed.max_requests != expected.max_requests
        or observed.window_seconds != expected.window_seconds
        or observed.burst_size != expected.burst_size
    ):
        raise CanaryPolicyError("canary must use the production SofaScore rate limit")
    return limiter


def _target_blocker(class_spec: ClassSpec, cohort: CanaryCohort) -> str:
    """Return the frozen source-evidence gap that blocks one target, if any."""

    if class_spec.scope == "match" and len(cohort.match_ids) != EXPECTED_MATCHES:
        return (
            str(cohort.payload.get("match_collection_blocker") or "").strip()
            or "fixed cohort has no 25 match IDs"
        )
    if class_spec.scope == "player" and len(cohort.player_ids) != EXPECTED_PLAYERS:
        return (
            str(cohort.payload.get("player_collection_blocker") or "").strip()
            or "fixed cohort has no 50 player IDs"
        )
    return ""


def _class_specs(class_spec: ClassSpec, cohort: CanaryCohort) -> tuple[Any, ...]:
    """Return the fixed batch specs of one class/target pair."""

    blocker = _target_blocker(class_spec, cohort)
    if blocker:
        raise CanaryPolicyError(
            f"{class_spec.name!r} collection is blocked: {blocker}"
        )
    specs = build_fixed_specs(cohort)
    if class_spec.scope == "match":
        return tuple(spec for spec in specs if spec.key.target_type == "event")
    if class_spec.scope == "player":
        return tuple(spec for spec in specs if spec.key.endpoint in PLAYER_PATHS)
    raise CanaryPolicyError(
        f"season class {class_spec.name!r} uses the dynamic season planner"
    )


def _experimental_policy(cap: int, class_spec: ClassSpec) -> BudgetPolicy:
    identity = experimental_canary_policy_id(cap)
    return BudgetPolicy(
        artifact_id=identity,
        hard_run_bytes=cap,
        endpoint_reservation_bytes={
            endpoint: cap for endpoint in class_spec.required_endpoints
        },
        sample_count=0,
        distinct_proxy_exits=0,
        workload_class=class_spec.name,
    )


def _engine(
    *,
    raw_store: RawPayloadStore,
    manifest_store,
    run_id: str,
    budget: Optional[SharedBudgetLedger] = None,
) -> SofaScoreCaptureEngine:
    return SofaScoreCaptureEngine(
        raw_store=raw_store,
        manifest_store=manifest_store,
        transport=NetworkForbiddenTransport(),
        run_id=run_id,
        task_id="fixed_cohort_canary",
        sink=NullCaptureSink(),
        budget=budget,
        rate_limiter=_production_rate_limiter(),
        retry_policy=RetryPolicy(),
        max_workers=4,
    )


@contextmanager
def _canary_environment(cap: int):
    updates = {
        "AIRFLOW_CTX_DAG_ID": CANARY_DAG_ID,
        "PROXY_FILTER_SOFASCORE_CANARY_HARD_CAP_BYTES": str(cap),
    }
    previous = {name: os.environ.get(name) for name in updates}
    os.environ.update(updates)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _transport_factory(
    engine: SofaScoreCaptureEngine,
    *,
    cap: int,
    holder: list[RecordingCanaryTransport],
):
    def create(_engine, **kwargs):
        if _engine is not engine or holder:
            raise CanaryPolicyError("canary must use one transport per logical run")
        wrapped = LeaseBackedCamoufoxTransport(
            engine,
            **kwargs,
            mode="canary",
            exit_probe_enabled=True,
        )
        recording = RecordingCanaryTransport(wrapped, cap)
        holder.append(recording)
        return recording

    return create


def _provider_maps(
    observations: Mapping[str, Sequence[int]],
) -> tuple[dict[str, int], dict[str, list[int]]]:
    request_bytes = {
        endpoint: [int(value) for value in values]
        for endpoint, values in sorted(observations.items())
    }
    endpoint_bytes = {
        endpoint: sum(values) for endpoint, values in request_bytes.items()
    }
    return endpoint_bytes, request_bytes


def _require_complete_season_plan(plan: Any) -> None:
    if plan.complete:
        return
    details = "; ".join(plan.player_universe_evidence_gaps)
    raise CanaryPolicyError(
        "season canary plan is incomplete" + (f": {details}" if details else "")
    )


def _metrics(traffic: Mapping[str, Any]) -> dict[str, Any]:
    metrics = {name: traffic[name] for name in sorted(REQUIRED_METRICS)}
    metrics.update(
        {
            "provider_up_bytes": int(traffic.get("provider_up_bytes", 0)),
            "provider_down_bytes": int(traffic.get("provider_down_bytes", 0)),
            "provider_total_bytes": int(traffic.get("provider_total_bytes", 0)),
        }
    )
    return metrics


def _result_evidence(
    specs: Sequence[Any],
    results: Sequence[Any],
    raw_store: RawPayloadStore,
    *,
    scope: str,
) -> tuple[int, int, int, dict[str, dict[str, int]]]:
    if not specs or len(results) != len(specs):
        raise CanaryPolicyError("canary did not return every planned endpoint result")
    by_key = {result.manifest.key: result for result in results}
    if len(by_key) != len(specs) or set(by_key) != {spec.key for spec in specs}:
        raise CanaryPolicyError(
            "canary endpoint result set differs from the fixed plan"
        )
    if scope == "match":
        strict_endpoints = frozenset(EVENT_PATHS)
    elif scope == "player":
        # A player's tournament-season statistics can genuinely be unsupported,
        # but the profile is the required identity record for this class.
        strict_endpoints = frozenset({"player_profile"})
    elif scope == "season":
        strict_endpoints = frozenset({"schedule_last", "schedule_next"})
    else:
        raise CanaryPolicyError(f"unsupported workload scope {scope!r}")
    accepted_required = {
        ManifestStatus.SUCCESS,
        ManifestStatus.LEGITIMATE_EMPTY,
    }
    status_counts: dict[str, dict[str, int]] = {}
    raw_count = 0
    for spec in specs:
        body, raw = raw_store.load_bytes(spec.raw_target)
        manifest = by_key[spec.key].manifest
        if not manifest.status.terminal:
            raise CanaryPolicyError(
                f"canary endpoint {spec.key.endpoint!r} is not terminal"
            )
        if (
            spec.key.endpoint in strict_endpoints
            and manifest.status not in accepted_required
        ):
            raise CanaryPolicyError(
                f"required canary endpoint {spec.key.endpoint!r} has "
                f"disallowed status {manifest.status.value!r}"
            )
        if not body and raw.http_status not in spec.legitimate_empty_http_statuses:
            raise CanaryPolicyError("stored canary payload is unexpectedly empty")
        if (
            manifest.raw_content_hash != raw.content_hash
            or manifest.raw_blob_key != raw.blob_key
        ):
            raise CanaryPolicyError("canary manifest/raw lineage mismatch")
        endpoint_counts = status_counts.setdefault(spec.key.endpoint, {})
        status = manifest.status.value
        endpoint_counts[status] = endpoint_counts.get(status, 0) + 1
        raw_count += 1
    event_success = sum(
        result.manifest.key.endpoint == "event"
        and result.manifest.status == ManifestStatus.SUCCESS
        for result in results
    )
    profile_success = sum(
        result.manifest.key.endpoint == "player_profile"
        and result.manifest.status == ManifestStatus.SUCCESS
        for result in results
    )
    return raw_count, event_success, profile_success, {
        endpoint: dict(sorted(counts.items()))
        for endpoint, counts in sorted(status_counts.items())
    }


def _sample(
    *,
    run_id: str,
    mode: str,
    class_spec: ClassSpec,
    target: CollectionTarget,
    cohort: CanaryCohort,
    cap: int,
    traffic: Mapping[str, Any],
    endpoint_bytes: Mapping[str, int],
    request_bytes: Mapping[str, Sequence[int]],
    raw_count: int,
    raw_write_count: int,
    event_success: int,
    profile_success: int,
    endpoint_status_counts: Mapping[str, Mapping[str, int]],
    proxy_exit_hash: Optional[str],
    season_plan_complete: Optional[bool] = None,
) -> dict[str, Any]:
    live = mode in {"cold", "single_endpoint_resume"}
    fingerprint = runtime_fingerprint()
    return {
        "run_id": run_id,
        "workload_class": class_spec.name,
        "source_tournament_id": target.source_tournament_id,
        "units": class_spec.max_units,
        "budget_eligible": mode == "cold",
        "cohort": target.cohort_name,
        "mode": mode,
        "proxy_exit_hash": proxy_exit_hash,
        "total_provider_bytes": int(traffic.get("provider_total_bytes", 0)),
        "lease_count": int(traffic.get("browser_sessions", 0)),
        "network_request_count": int(traffic.get("request_count", 0)),
        "allocation_bytes": cap if live else 0,
        "endpoint_provider_bytes": dict(endpoint_bytes),
        "endpoint_request_provider_bytes": {
            endpoint: list(values) for endpoint, values in request_bytes.items()
        },
        "metrics": _metrics(traffic),
        "evidence": {
            "cohort_sha256": cohort.digest,
            "runtime_fingerprint_digest": fingerprint["digest"],
            "experimental_cap_bytes": cap,
            "planned_endpoints": raw_count,
            "raw_payload_count": raw_count,
            "raw_payload_write_count": raw_write_count,
            "successful_event_bases": event_success,
            "successful_player_profiles": profile_success,
            "endpoint_status_counts": {
                endpoint: dict(counts)
                for endpoint, counts in endpoint_status_counts.items()
            },
            "season_plan_complete": season_plan_complete,
            "transport_source": (
                CANARY_SOURCE if live else "none"
            ),
        },
    }


def _zero_network_traffic(engine: SofaScoreCaptureEngine) -> dict[str, Any]:
    traffic = engine.metrics.snapshot()
    traffic.update(
        {
            "provider_up_bytes": 0,
            "provider_down_bytes": 0,
            "provider_total_bytes": 0,
            "proxy_exit_hash": None,
        }
    )
    return traffic


def execute_cold_run(
    class_spec: ClassSpec,
    target: CollectionTarget,
    *,
    experimental_cap_bytes: int,
    root: Path,
) -> ColdRunState:
    cap = _positive_int(experimental_cap_bytes, "experimental canary cap")
    if CLASS_MANIFEST.spec(class_spec.name) != class_spec:
        raise CanaryPolicyError(
            f"workload class {class_spec.name!r} is not declared by the manifest"
        )
    if class_spec.target_for(target.source_tournament_id) != target:
        raise CanaryPolicyError(
            f"workload class {class_spec.name!r} has another collection target for "
            f"tournament {target.source_tournament_id}"
        )
    cohort = load_target_cohort(target)
    run_id = f"canary-{class_spec.name}-cold-{uuid.uuid4().hex}"
    raw_store = CountingRawPayloadStore(fs.LocalFileSystem(), str(root / "raw"))
    manifest_store = JsonFileManifestStore(root / "manifest.json")
    budget = SharedBudgetLedger(
        root / "experimental-budget-ledger.json",
        _experimental_policy(cap, class_spec),
    )
    engine = _engine(
        raw_store=raw_store,
        manifest_store=manifest_store,
        run_id=run_id,
        budget=budget,
    )
    holder: list[RecordingCanaryTransport] = []
    with _canary_environment(cap):
        if class_spec.scope in {"match", "player"}:
            specs = _class_specs(class_spec, cohort)
            results, traffic = capture_live_specs(
                SimpleNamespace(engine=engine),
                specs,
                canonical_url=target.canonical_url,
                scope=(
                    f"{cohort.canonical_competition}:{cohort.canonical_season}:"
                    f"{class_spec.name}"
                ),
                entity=class_spec.name,
                transport_factory=_transport_factory(engine, cap=cap, holder=holder),
            )
        else:
            tournament_id = target.source_tournament_id
            season_id = target.representative_season_id
            max_pages = int(
                class_spec.shape["schedule_page_chain"]["max_pages_per_direction"]
            )

            def planner():
                return plan_season_partition(
                    raw_store,
                    manifest_store,
                    source_tournament_id=tournament_id,
                    source_season_id=season_id,
                    freshness_key=f"canary-{class_spec.name}-{tournament_id}",
                    event_freshness_key="final",
                    paid_proxy=True,
                    max_pages=max_pages,
                )

            results, final_plan, traffic = capture_live_dynamic_specs(
                SimpleNamespace(engine=engine),
                planner,
                canonical_url=target.canonical_url,
                scope=class_spec.name,
                entity="season_capture",
                transport_factory=_transport_factory(engine, cap=cap, holder=holder),
            )
            specs = final_plan.specs
    if len(holder) != 1:
        raise CanaryPolicyError("cold run did not create exactly one lease transport")
    endpoint_bytes, request_bytes = _provider_maps(holder[0].request_observations)
    season_plan_complete: Optional[bool] = None
    if class_spec.scope == "season":
        _require_complete_season_plan(final_plan)
        season_plan_complete = True
    raw_count, event_success, profile_success, status_counts = _result_evidence(
        specs, results, raw_store, scope=class_spec.scope
    )
    raw_writes = sum(raw_store.write_counts.values())
    sample = _sample(
        run_id=run_id,
        mode="cold",
        class_spec=class_spec,
        target=target,
        cohort=cohort,
        cap=cap,
        traffic=traffic,
        endpoint_bytes=endpoint_bytes,
        request_bytes=request_bytes,
        raw_count=raw_count,
        raw_write_count=raw_writes,
        event_success=event_success,
        profile_success=profile_success,
        endpoint_status_counts=status_counts,
        proxy_exit_hash=traffic.get("proxy_exit_hash"),
        season_plan_complete=season_plan_complete,
    )
    validate_sample(sample, cohort=cohort, cap=cap)
    return ColdRunState(
        run_id=run_id,
        class_spec=class_spec,
        target=target,
        cohort=cohort,
        experimental_cap_bytes=cap,
        specs=specs,
        raw_store=raw_store,
        manifest_store=manifest_store,
        results=tuple(results),
        sample=sample,
        root=root,
    )


def execute_no_op(state: ColdRunState) -> dict[str, Any]:
    run_id = f"canary-no-op-{uuid.uuid4().hex}"
    before = sum(state.raw_store.write_counts.values())
    engine = _engine(
        raw_store=state.raw_store,
        manifest_store=state.manifest_store,
        run_id=run_id,
    )
    results = engine.capture_many(state.specs)
    traffic = _zero_network_traffic(engine)
    raw_count, event_success, profile_success, status_counts = _result_evidence(
        state.specs,
        results,
        state.raw_store,
        scope=state.class_spec.scope,
    )
    sample = _sample(
        run_id=run_id,
        mode="no_op",
        class_spec=state.class_spec,
        target=state.target,
        cohort=state.cohort,
        cap=state.experimental_cap_bytes,
        traffic=traffic,
        endpoint_bytes={},
        request_bytes={},
        raw_count=raw_count,
        raw_write_count=sum(state.raw_store.write_counts.values()) - before,
        event_success=event_success,
        profile_success=profile_success,
        endpoint_status_counts=status_counts,
        proxy_exit_hash=None,
    )
    validate_sample(sample, cohort=state.cohort, cap=state.experimental_cap_bytes)
    return sample


def execute_offline_replay(state: ColdRunState) -> dict[str, Any]:
    run_id = f"canary-offline-replay-{uuid.uuid4().hex}"
    before = sum(state.raw_store.write_counts.values())
    engine = _engine(
        raw_store=state.raw_store,
        manifest_store=InMemoryManifestStore(),
        run_id=run_id,
    )
    results = engine.capture_many(state.specs, offline=True, force_replay=True)
    traffic = _zero_network_traffic(engine)
    raw_count, event_success, profile_success, status_counts = _result_evidence(
        state.specs,
        results,
        state.raw_store,
        scope=state.class_spec.scope,
    )
    sample = _sample(
        run_id=run_id,
        mode="offline_replay",
        class_spec=state.class_spec,
        target=state.target,
        cohort=state.cohort,
        cap=state.experimental_cap_bytes,
        traffic=traffic,
        endpoint_bytes={},
        request_bytes={},
        raw_count=raw_count,
        raw_write_count=sum(state.raw_store.write_counts.values()) - before,
        event_success=event_success,
        profile_success=profile_success,
        endpoint_status_counts=status_counts,
        proxy_exit_hash=None,
    )
    validate_sample(sample, cohort=state.cohort, cap=state.experimental_cap_bytes)
    return sample


def _copy_raw_without(
    source: RawPayloadStore,
    destination: CountingRawPayloadStore,
    specs: Sequence[Any],
    missing_key,
) -> None:
    for spec in specs:
        if spec.key == missing_key:
            continue
        body, record = source.load_bytes(spec.raw_target)
        destination.store_bytes(
            spec.raw_target,
            body,
            request_url=record.request_url,
            http_status=record.http_status,
            response_headers=record.response_headers,
            fetched_at=record.fetched_at,
            fetcher_version=record.fetcher_version,
        )
    destination.reset_write_counts()


def execute_single_endpoint_resume(state: ColdRunState) -> dict[str, Any]:
    run_id = f"canary-single-resume-{uuid.uuid4().hex}"
    resume_root = state.root / "single-endpoint-resume"
    resume_raw = CountingRawPayloadStore(fs.LocalFileSystem(), str(resume_root / "raw"))
    missing = next(
        (
            spec
            for spec in state.specs
            if state.class_spec.scope == "match"
            and spec.key.target_id == state.cohort.match_ids[0]
            and spec.key.endpoint == "incidents"
        ),
        state.specs[0],
    )
    _copy_raw_without(state.raw_store, resume_raw, state.specs, missing.key)
    existing_records = [
        result.manifest
        for result in state.results
        if result.manifest.key != missing.key
    ]
    manifest = InMemoryManifestStore(existing_records)
    budget = SharedBudgetLedger(
        resume_root / "experimental-budget-ledger.json",
        _experimental_policy(state.experimental_cap_bytes, state.class_spec),
    )
    engine = _engine(
        raw_store=resume_raw,
        manifest_store=manifest,
        run_id=run_id,
        budget=budget,
    )
    holder: list[RecordingCanaryTransport] = []
    with _canary_environment(state.experimental_cap_bytes):
        results, traffic = capture_live_specs(
            SimpleNamespace(engine=engine),
            state.specs,
            canonical_url=state.target.canonical_url,
            scope=(
                f"{state.cohort.canonical_competition}:"
                f"{state.cohort.canonical_season}:resume-canary"
            ),
            entity="single_endpoint_resume",
            transport_factory=_transport_factory(
                engine,
                cap=state.experimental_cap_bytes,
                holder=holder,
            ),
        )
    if len(holder) != 1:
        raise CanaryPolicyError("resume did not create exactly one lease transport")
    endpoint_bytes, request_bytes = _provider_maps(holder[0].request_observations)
    raw_count, event_success, profile_success, status_counts = _result_evidence(
        state.specs,
        results,
        resume_raw,
        scope=state.class_spec.scope,
    )
    sample = _sample(
        run_id=run_id,
        mode="single_endpoint_resume",
        class_spec=state.class_spec,
        target=state.target,
        cohort=state.cohort,
        cap=state.experimental_cap_bytes,
        traffic=traffic,
        endpoint_bytes=endpoint_bytes,
        request_bytes=request_bytes,
        raw_count=raw_count,
        raw_write_count=sum(resume_raw.write_counts.values()),
        event_success=event_success,
        profile_success=profile_success,
        endpoint_status_counts=status_counts,
        proxy_exit_hash=traffic.get("proxy_exit_hash"),
    )
    validate_sample(sample, cohort=state.cohort, cap=state.experimental_cap_bytes)
    return sample


def _is_hash(value: object) -> bool:
    return isinstance(value, str) and bool(_HEX64_RE.fullmatch(value))


def _finite_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) >= 0
    )


def _validate_endpoint_status_evidence(
    evidence: Mapping[str, Any],
    *,
    scope: str,
    planned: int,
) -> None:
    raw_counts = evidence.get("endpoint_status_counts")
    if not isinstance(raw_counts, Mapping) or not raw_counts:
        raise CanaryPolicyError("sample lacks endpoint status evidence")
    terminal = {
        ManifestStatus.SUCCESS.value,
        ManifestStatus.LEGITIMATE_EMPTY.value,
        ManifestStatus.NOT_SUPPORTED.value,
    }
    normalized: dict[str, dict[str, int]] = {}
    for endpoint, statuses in raw_counts.items():
        if not isinstance(endpoint, str) or not endpoint or not isinstance(
            statuses, Mapping
        ) or not statuses:
            raise CanaryPolicyError("sample endpoint status evidence is invalid")
        normalized[endpoint] = {}
        for status, count in statuses.items():
            if (
                status not in terminal
                or isinstance(count, bool)
                or not isinstance(count, int)
                or count <= 0
            ):
                raise CanaryPolicyError("sample endpoint status evidence is invalid")
            normalized[endpoint][str(status)] = count
    if sum(sum(counts.values()) for counts in normalized.values()) != planned:
        raise CanaryPolicyError("sample endpoint status evidence is incomplete")

    if scope == "match":
        expected = {endpoint: EXPECTED_MATCHES for endpoint in EVENT_PATHS}
        strict = set(EVENT_PATHS)
    elif scope == "player":
        expected = {endpoint: EXPECTED_PLAYERS for endpoint in PLAYER_PATHS}
        strict = {"player_profile"}
    else:
        expected = None
        strict = {"schedule_last", "schedule_next"}
    if expected is not None and (
        set(normalized) != set(expected)
        or any(sum(normalized[name].values()) != count for name, count in expected.items())
    ):
        raise CanaryPolicyError("sample endpoint status evidence has the wrong shape")
    allowed_required = {
        ManifestStatus.SUCCESS.value,
        ManifestStatus.LEGITIMATE_EMPTY.value,
    }
    if any(set(normalized[endpoint]) - allowed_required for endpoint in strict):
        raise CanaryPolicyError(
            "required endpoint status evidence contains not_supported"
        )


def _validate_no_sensitive_values(value: object, *, key: str = "") -> None:
    if key.casefold() in _FORBIDDEN_EVIDENCE_KEYS:
        raise CanaryPolicyError(f"canary artifact contains forbidden field {key!r}")
    if isinstance(value, Mapping):
        for child_key, child in value.items():
            _validate_no_sensitive_values(child, key=str(child_key))
        return
    if isinstance(value, list):
        for child in value:
            _validate_no_sensitive_values(child, key=key)
        return
    if not isinstance(value, str):
        return
    if re.search(r"(?i)\b(?:bearer|proxy-authorization)\s+\S+", value):
        raise CanaryPolicyError("canary artifact contains an authorization value")
    parsed = urlsplit(value) if "://" in value else None
    if parsed is not None and (
        parsed.username is not None or parsed.password is not None
    ):
        raise CanaryPolicyError("canary artifact contains URL credentials")
    for candidate in _IPV4_RE.findall(value):
        try:
            ipaddress.ip_address(candidate)
        except ValueError:
            continue
        raise CanaryPolicyError("canary artifact contains a raw IP address")


def validate_sample(
    sample: Mapping[str, Any], *, cohort: CanaryCohort, cap: int
) -> None:
    _validate_no_sensitive_values(sample)
    mode = str(sample.get("mode") or "")
    if mode not in REQUIRED_BENCHMARK_MODES:
        raise CanaryPolicyError(f"unsupported canary benchmark mode {mode!r}")
    workload_class = str(sample.get("workload_class") or "")
    if workload_class not in REQUIRED_WORKLOAD_CLASSES:
        raise CanaryPolicyError("sample has an unknown workload_class")
    class_spec = CLASS_MANIFEST.spec(workload_class)
    tournament_id = sample.get("source_tournament_id")
    try:
        target = class_spec.target_for(tournament_id)
    except CanaryPolicyError as exc:
        raise CanaryPolicyError(
            "sample source tournament is not a measured target of its "
            "workload class"
        ) from exc
    if cohort.source_tournament_id != target.source_tournament_id:
        raise CanaryPolicyError(
            "sample source tournament does not match its cohort"
        )
    if sample.get("cohort") != target.cohort_name:
        raise CanaryPolicyError("sample cohort does not match its collection target")
    if sample.get("units") != class_spec.max_units:
        raise CanaryPolicyError("sample units do not match the full measured class")
    if not str(sample.get("run_id") or "").strip():
        raise CanaryPolicyError("sample run_id is empty")
    if sample.get("budget_eligible") is not (mode == "cold"):
        raise CanaryPolicyError("only cold canary samples may be budget-eligible")
    evidence = sample.get("evidence")
    metrics = sample.get("metrics")
    if not isinstance(evidence, dict) or not isinstance(metrics, dict):
        raise CanaryPolicyError("sample lacks compact metrics/evidence")
    expected_evidence = {
        "cohort_sha256": cohort.digest,
        "runtime_fingerprint_digest": runtime_fingerprint()["digest"],
        "experimental_cap_bytes": cap,
    }
    for name, expected in expected_evidence.items():
        if evidence.get(name) != expected:
            raise CanaryPolicyError(f"sample has invalid {name}")
    if not REQUIRED_METRICS.issubset(metrics):
        raise CanaryPolicyError("sample is missing production benchmark metrics")
    for name in REQUIRED_METRICS:
        if not _finite_number(metrics[name]):
            raise CanaryPolicyError(f"sample metric {name!r} is invalid")
    for name in ("provider_up_bytes", "provider_down_bytes", "provider_total_bytes"):
        if (
            isinstance(metrics.get(name), bool)
            or not isinstance(metrics.get(name), int)
            or metrics[name] < 0
        ):
            raise CanaryPolicyError(f"sample metric {name!r} is invalid")
    total = sample.get("total_provider_bytes")
    endpoints = sample.get("endpoint_provider_bytes")
    observations = sample.get("endpoint_request_provider_bytes")
    if (
        isinstance(total, bool)
        or not isinstance(total, int)
        or not isinstance(endpoints, dict)
        or not isinstance(observations, dict)
        or set(endpoints) != set(observations)
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in endpoints.values()
        )
        or any(
            not isinstance(values, list)
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in values
            )
            or sum(values) != endpoints[endpoint]
            for endpoint, values in observations.items()
        )
        or sum(endpoints.values()) != total
        or metrics["provider_total_bytes"] != total
        or metrics["provider_up_bytes"] + metrics["provider_down_bytes"] != total
    ):
        raise CanaryPolicyError("sample provider-byte accounting is inconsistent")
    if metrics.get("endpoint_completeness") != 1:
        raise CanaryPolicyError("sample endpoint walk is incomplete")
    planned = evidence.get("planned_endpoints")
    raw_count = evidence.get("raw_payload_count")
    if (
        isinstance(planned, bool)
        or not isinstance(planned, int)
        or planned < 1
        or raw_count != planned
    ):
        raise CanaryPolicyError("sample does not retain every planned raw payload")
    _validate_endpoint_status_evidence(
        evidence,
        scope=class_spec.scope,
        planned=planned,
    )

    if mode == "cold":
        if class_spec.scope == "match":
            expected_request_counts = {
                endpoint: EXPECTED_MATCHES for endpoint in EVENT_PATHS
            }
            expected_events, expected_profiles = EXPECTED_MATCHES, 0
        elif class_spec.scope == "player":
            expected_request_counts = {
                endpoint: EXPECTED_PLAYERS for endpoint in PLAYER_PATHS
            }
            expected_events, expected_profiles = 0, EXPECTED_PLAYERS
        else:
            expected_request_counts = None
            expected_events, expected_profiles = 0, 0
        if class_spec.scope == "season":
            # Dynamic referee requests exist only when schedule payloads expose
            # referee IDs.  Require every actually planned endpoint and every
            # static season endpoint; do not invent a network request merely
            # to make an optional dynamic endpoint appear in the sample.
            status_endpoints = set(
                evidence.get("endpoint_status_counts") or {}
            )
            exact_shape = (
                set(observations) == status_endpoints
                and set(SEASON_STATIC_ENDPOINTS) <= status_endpoints
                and status_endpoints <= set(class_spec.required_endpoints)
            )
        else:
            exact_shape = set(observations) == set(class_spec.required_endpoints)
        if expected_request_counts is None:
            exact_request_count = all(
                values and all(value > 0 for value in values)
                for values in observations.values()
            )
        else:
            exact_request_count = all(
                len(observations.get(endpoint, [])) == expected_count
                and all(value > 0 for value in observations[endpoint])
                for endpoint, expected_count in expected_request_counts.items()
            )
        if (
            evidence.get("transport_source") != CANARY_SOURCE
            or evidence.get("raw_payload_write_count") != planned
            or evidence.get("successful_event_bases") != expected_events
            or evidence.get("successful_player_profiles") != expected_profiles
            or (
                class_spec.scope == "season"
                and evidence.get("season_plan_complete") is not True
            )
            or not _is_hash(sample.get("proxy_exit_hash"))
            or not 0 < total <= cap
            or metrics.get("request_count") != sum(
                len(values) for values in observations.values()
            )
            or metrics.get("source_request_count")
            != metrics.get("request_count") + 2
            or metrics.get("browser_sessions") != 1
            or metrics.get("navigations") != metrics.get("request_count") + 1
            or sample.get("lease_count") != 1
            or sample.get("network_request_count") != metrics.get("request_count")
            or sample.get("allocation_bytes") != cap
            or not exact_shape
            or not exact_request_count
        ):
            raise CanaryPolicyError("cold sample failed workload-class acceptance")
        return

    if mode in {"no_op", "offline_replay"}:
        expected_replay = 1 if mode == "offline_replay" else 0
        if (
            evidence.get("transport_source") != "none"
            or evidence.get("raw_payload_write_count") != 0
            or sample.get("proxy_exit_hash") is not None
            or total != 0
            or endpoints
            or observations
            or metrics.get("request_count") != 0
            or metrics.get("source_request_count") != 0
            or metrics.get("browser_sessions") != 0
            or metrics.get("navigations") != 0
            or metrics.get("cache_hit_rate") != 1
            or metrics.get("replay_hit_rate") != expected_replay
            or sample.get("lease_count") != 0
            or sample.get("network_request_count") != 0
            or sample.get("allocation_bytes") != 0
        ):
            raise CanaryPolicyError(f"{mode} is not a zero-network benchmark")
        return

    if (
        evidence.get("transport_source") != CANARY_SOURCE
        or evidence.get("raw_payload_write_count") != 1
        or not _is_hash(sample.get("proxy_exit_hash"))
        or not 0 < total <= cap
        or metrics.get("request_count") != 1
        or metrics.get("source_request_count") != 3
        or metrics.get("browser_sessions") != 1
        or metrics.get("navigations") != 2
        or len(observations) != 1
        or sum(len(values) for values in observations.values()) != 1
        or any(value <= 0 for values in observations.values() for value in values)
        or sample.get("lease_count") != 1
        or sample.get("network_request_count") != 1
        or sample.get("allocation_bytes") != cap
    ):
        raise CanaryPolicyError(
            "single-endpoint resume did not resume exactly one request"
        )


def _manifest_workload_classes() -> dict[str, dict[str, Any]]:
    """Materialize the declared classes; the manifest is the only source."""

    cohorts: dict[Path, CanaryCohort] = {}
    workload_classes: dict[str, dict[str, Any]] = {}
    for name, class_spec in CLASS_MANIFEST.classes.items():
        class_cohorts: dict[str, dict[str, str]] = {}
        representative_season_ids: dict[str, int] = {}
        blockers: list[str] = []
        for target in class_spec.targets:
            if target.cohort_path not in cohorts:
                cohorts[target.cohort_path] = load_target_cohort(target)
            cohort = cohorts[target.cohort_path]
            tournament = str(target.source_tournament_id)
            class_cohorts[tournament] = {
                "name": target.cohort_name,
                "cohort_sha256": cohort.digest,
            }
            if class_spec.scope == "season":
                representative_season_ids[tournament] = int(
                    target.representative_season_id
                )
            blocker = _target_blocker(class_spec, cohort)
            if blocker:
                blockers.append(f"tournament {tournament}: {blocker}")
        entry: dict[str, Any] = {
            "scope": class_spec.scope,
            "max_units": class_spec.max_units,
            "required_endpoints": list(class_spec.required_endpoints),
            "shape": dict(class_spec.shape),
            "shape_digest": class_spec.shape_digest,
            "measured_tournament_ids": list(class_spec.measured_tournament_ids),
            "cohorts": class_cohorts,
            "collection_blocker": "; ".join(blockers),
            "hard_task_bytes": None,
            "samples": [],
        }
        if class_spec.scope == "season":
            entry["representative_season_ids"] = representative_season_ids
        workload_classes[name] = entry
    return workload_classes


def _artifact_template(cohort: CanaryCohort, cap: int) -> dict[str, Any]:
    rate = RATE_LIMITS["sofascore"]
    try:
        shipped = json.loads(DEFAULT_ARTIFACT_PATH.read_text(encoding="utf-8"))
        historical = shipped.get("historical_observations", [])
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        historical = []
    return {
        "schema_version": WORKLOAD_ARTIFACT_SCHEMA_VERSION,
        "source": "sofascore",
        "collection_source": CANARY_SOURCE,
        "collector_version": COLLECTOR_VERSION,
        "runtime_fingerprint": runtime_fingerprint(),
        "meter": METER,
        "budget_derivation": BUDGET_DERIVATION,
        "experimental_hard_cap_bytes": cap,
        "bootstrap_authorizes_production": False,
        "cohort_sha256": cohort.digest,
        "class_manifest_sha256": CLASS_MANIFEST.digest,
        "rate_limit_policy": {
            "source": "sofascore",
            "max_requests": rate.max_requests,
            "window_seconds": rate.window_seconds,
            "burst_size": rate.burst_size,
        },
        "requirements": {
            "minimum_distinct_proxy_exits_per_class": MIN_DISTINCT_PROXY_EXITS,
            "minimum_cold_samples_per_class": MIN_CANARY_RUNS,
            "minimum_distinct_tournaments_for_transfer": (
                MIN_MEASURED_TOURNAMENTS_FOR_TRANSFER
            ),
            "required_workload_classes": list(REQUIRED_WORKLOAD_CLASSES),
            "required_benchmark_modes": sorted(BENCHMARK_ONLY_MODES),
        },
        "historical_observations": historical,
        "workload_classes": _manifest_workload_classes(),
        "benchmark_samples": [],
        "verified": False,
        "verification_blocker": (
            "Collection is experimental and cannot authorize production until "
            "the separate verify command validates every policy gate."
        ),
    }


def _write_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_artifact(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanaryPolicyError("canary artifact is unreadable") from exc
    if not isinstance(payload, dict):
        raise CanaryPolicyError("canary artifact must be an object")
    return payload


def bootstrap_artifact(
    path: os.PathLike[str] | str,
    *,
    cohort: CanaryCohort,
    experimental_cap_bytes: int,
) -> dict[str, Any]:
    cap = _positive_int(experimental_cap_bytes, "experimental canary cap")
    artifact_path = Path(path)
    lock_path = artifact_path.with_suffix(artifact_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            if not artifact_path.exists():
                payload = _artifact_template(cohort, cap)
                _write_atomic(artifact_path, payload)
                return payload
            payload = _read_artifact(artifact_path)
            if payload.get("verified") is True:
                raise CanaryPolicyError(
                    "verified artifact is immutable; start a new candidate"
                )
            expected = _artifact_template(cohort, cap)
            classes_before_bootstrap = payload.get("workload_classes")
            has_existing_evidence = bool(payload.get("benchmark_samples")) or (
                isinstance(classes_before_bootstrap, Mapping)
                and any(
                    isinstance(raw_class, Mapping) and raw_class.get("samples")
                    for raw_class in classes_before_bootstrap.values()
                )
            )
            if (
                "runtime_fingerprint" not in payload
                and has_existing_evidence
            ):
                raise CanaryPolicyError(
                    "existing canary evidence has no runtime fingerprint; "
                    "start a new empty candidate"
                )
            changed = False
            for name in (
                "collection_source",
                "collector_version",
                "runtime_fingerprint",
                "experimental_hard_cap_bytes",
                "bootstrap_authorizes_production",
                "cohort_sha256",
                "class_manifest_sha256",
                "rate_limit_policy",
            ):
                if name in payload and payload[name] != expected[name]:
                    raise CanaryPolicyError(f"artifact bootstrap mismatch for {name}")
                if name not in payload:
                    payload[name] = expected[name]
                    changed = True
            for name in ("schema_version", "source", "meter", "budget_derivation"):
                if payload.get(name) != expected[name]:
                    raise CanaryPolicyError(f"artifact contract mismatch for {name}")
            classes = payload.get("workload_classes")
            if not isinstance(classes, dict) or set(classes) != set(
                expected["workload_classes"]
            ):
                raise CanaryPolicyError(
                    "artifact workload classes do not match the class manifest"
                )
            for class_name, expected_class in expected["workload_classes"].items():
                candidate = classes[class_name]
                if not isinstance(candidate, dict) or not isinstance(
                    candidate.get("samples"), list
                ):
                    raise CanaryPolicyError(
                        f"artifact class {class_name!r} is malformed"
                    )
                if any(
                    candidate.get(field) != expected_class.get(field)
                    for field in IMMUTABLE_CLASS_FIELDS
                ):
                    raise CanaryPolicyError(
                        f"artifact class {class_name!r} shape changed"
                    )
            if not isinstance(payload.get("benchmark_samples"), list):
                raise CanaryPolicyError("artifact benchmark_samples must be an array")
            if changed:
                payload["verified"] = False
                _write_atomic(artifact_path, payload)
            return payload
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _sample_cohort(
    sample: Mapping[str, Any],
    *,
    cache: dict[Path, CanaryCohort],
) -> CanaryCohort:
    """Resolve a sample's frozen cohort from its own tournament provenance."""

    class_spec = CLASS_MANIFEST.spec(str(sample.get("workload_class") or ""))
    target = class_spec.target_for(sample.get("source_tournament_id"))
    if target.cohort_path not in cache:
        cache[target.cohort_path] = load_target_cohort(target)
    return cache[target.cohort_path]


def validate_artifact(
    payload: Mapping[str, Any],
    *,
    cohort: CanaryCohort,
    require_verifiable: bool,
) -> None:
    _validate_no_sensitive_values(payload)
    try:
        validate_runtime_fingerprint(payload.get("runtime_fingerprint"))
    except RuntimeFingerprintError as exc:
        raise CanaryPolicyError(str(exc)) from exc
    if (
        payload.get("schema_version") != WORKLOAD_ARTIFACT_SCHEMA_VERSION
        or payload.get("source") != "sofascore"
        or payload.get("collection_source") != CANARY_SOURCE
        or payload.get("collector_version") != COLLECTOR_VERSION
        or payload.get("meter") != METER
        or payload.get("budget_derivation") != BUDGET_DERIVATION
        or payload.get("bootstrap_authorizes_production") is not False
        or payload.get("cohort_sha256") != cohort.digest
        or payload.get("class_manifest_sha256") != CLASS_MANIFEST.digest
    ):
        raise CanaryPolicyError("canary artifact provenance is invalid")
    cap = _positive_int(
        payload.get("experimental_hard_cap_bytes"), "experimental canary cap"
    )
    expected = _artifact_template(cohort, cap)
    if payload.get("rate_limit_policy") != expected["rate_limit_policy"]:
        raise CanaryPolicyError(
            "canary artifact does not pin the production rate limit"
        )
    classes = payload.get("workload_classes")
    if not isinstance(classes, dict) or set(classes) != set(REQUIRED_WORKLOAD_CLASSES):
        raise CanaryPolicyError("canary workload classes are incomplete")
    benchmark_samples = payload.get("benchmark_samples")
    if not isinstance(benchmark_samples, list):
        raise CanaryPolicyError("canary benchmark_samples must be an array")
    cohorts: dict[Path, CanaryCohort] = {}
    run_ids: set[str] = set()
    for class_name in REQUIRED_WORKLOAD_CLASSES:
        class_spec = CLASS_MANIFEST.spec(class_name)
        raw_class = classes[class_name]
        expected_class = expected["workload_classes"][class_name]
        if not isinstance(raw_class, dict):
            raise CanaryPolicyError(f"workload class {class_name!r} is malformed")
        for field in IMMUTABLE_CLASS_FIELDS:
            if raw_class.get(field) != expected_class.get(field):
                raise CanaryPolicyError(
                    f"workload class {class_name!r} changed {field}"
                )
        samples = raw_class.get("samples")
        if not isinstance(samples, list):
            raise CanaryPolicyError(f"{class_name!r}.samples must be an array")
        by_tournament = {
            target.source_tournament_id: 0 for target in class_spec.targets
        }
        for sample in samples:
            if not isinstance(sample, dict):
                raise CanaryPolicyError("canary sample must be an object")
            if sample.get("workload_class") != class_name:
                raise CanaryPolicyError("sample is stored under the wrong class")
            validate_sample(
                sample,
                cohort=_sample_cohort(sample, cache=cohorts),
                cap=cap,
            )
            by_tournament[sample["source_tournament_id"]] += 1
            run_id = sample["run_id"]
            if run_id in run_ids:
                raise CanaryPolicyError("canary run IDs must be globally unique")
            run_ids.add(run_id)
        expected_max = max(
            (sample["total_provider_bytes"] for sample in samples),
            default=None,
        )
        if raw_class.get("hard_task_bytes") != expected_max:
            raise CanaryPolicyError(
                f"{class_name!r}.hard_task_bytes is not the observed maximum"
            )
        if require_verifiable:
            if str(raw_class.get("collection_blocker") or "").strip():
                raise CanaryPolicyError(
                    f"{class_name!r} still has a collection blocker"
                )
            if len(samples) < MIN_CANARY_RUNS:
                raise CanaryPolicyError(
                    f"{class_name!r} needs at least {MIN_CANARY_RUNS} cold runs"
                )
            exits = {sample["proxy_exit_hash"] for sample in samples}
            if len(exits) < MIN_DISTINCT_PROXY_EXITS:
                raise CanaryPolicyError(
                    f"{class_name!r} needs at least "
                    f"{MIN_DISTINCT_PROXY_EXITS} distinct exit hashes"
                )
            # A shape is only evidence of a shape if every declared tournament
            # carried its even share of the cold runs; one league must never
            # dominate a class that is meant to generalize.  The floor is
            # anchored on the fixed class minimum, never on ``len(samples)``:
            # otherwise a class legitimately collected past the minimum (e.g. an
            # ``extend`` that adds a second target to an already-full class)
            # would raise the bar above what ``collect`` targeted and dead-lock.
            floor = MIN_CANARY_RUNS // len(class_spec.targets)
            for tournament_id, count in sorted(by_tournament.items()):
                if count == 0:
                    raise CanaryPolicyError(
                        f"{class_name!r} has no cold sample for measured "
                        f"tournament {tournament_id}"
                    )
                if count < floor:
                    raise CanaryPolicyError(
                        f"{class_name!r} cold samples are skewed: tournament "
                        f"{tournament_id} has {count} of at least {floor}"
                    )
    modes_by_class: dict[str, set[str]] = {
        name: set() for name in REQUIRED_WORKLOAD_CLASSES
    }
    for sample in benchmark_samples:
        if not isinstance(sample, dict):
            raise CanaryPolicyError("benchmark sample must be an object")
        validate_sample(
            sample,
            cohort=_sample_cohort(sample, cache=cohorts),
            cap=cap,
        )
        run_id = sample["run_id"]
        if run_id in run_ids:
            raise CanaryPolicyError("canary run IDs must be globally unique")
        run_ids.add(run_id)
        modes_by_class[sample["workload_class"]].add(sample["mode"])
    if require_verifiable:
        for class_name, modes in modes_by_class.items():
            missing = sorted(BENCHMARK_ONLY_MODES - modes)
            if missing:
                raise CanaryPolicyError(
                    f"{class_name!r} is missing benchmark modes: "
                    + ", ".join(missing)
                )


def _artifact_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    classes = payload.get("workload_classes") or {}
    benchmarks = payload.get("benchmark_samples") or []
    class_summary = {}
    for name in REQUIRED_WORKLOAD_CLASSES:
        samples = classes.get(name, {}).get("samples", [])
        by_tournament = {
            str(target.source_tournament_id): 0
            for target in CLASS_MANIFEST.spec(name).targets
        }
        for sample in samples:
            tournament = str(sample.get("source_tournament_id"))
            by_tournament[tournament] = by_tournament.get(tournament, 0) + 1
        class_summary[name] = {
            "cold_samples": len(samples),
            "cold_samples_by_tournament": by_tournament,
            "distinct_exit_hashes": len(
                {sample.get("proxy_exit_hash") for sample in samples}
            ),
            "hard_task_bytes": classes.get(name, {}).get("hard_task_bytes"),
            "modes": sorted(
                {
                    sample.get("mode")
                    for sample in benchmarks
                    if sample.get("workload_class") == name
                }
            ),
        }
    return {
        "verified": payload.get("verified") is True,
        "workload_classes": class_summary,
    }


def collect_canary(
    *,
    artifact_path: os.PathLike[str] | str,
    experimental_cap_bytes: int,
    target_cold_runs: int = MIN_CANARY_RUNS,
    cohort_path: os.PathLike[str] | str = DEFAULT_COHORT_PATH,
    workspace: os.PathLike[str] | str = DEFAULT_WORKSPACE,
) -> dict[str, Any]:
    cap = _positive_int(experimental_cap_bytes, "experimental canary cap")
    target = _positive_int(target_cold_runs, "target cold runs")
    if target < MIN_CANARY_RUNS:
        raise CanaryPolicyError(f"target cold runs must be >= {MIN_CANARY_RUNS}")
    cohort = load_fixed_cohort(cohort_path)
    artifact = Path(artifact_path)
    payload = bootstrap_artifact(
        artifact,
        cohort=cohort,
        experimental_cap_bytes=cap,
    )
    validate_artifact(payload, cohort=cohort, require_verifiable=False)
    if payload.get("verified") is True:
        raise CanaryPolicyError("collection cannot mutate a verified artifact")
    workspace_path = Path(workspace)
    workspace_path.mkdir(parents=True, exist_ok=True)
    os.chmod(workspace_path, 0o700)

    blocked_classes: dict[str, str] = {}
    for workload_class in REQUIRED_WORKLOAD_CLASSES:
        class_spec = CLASS_MANIFEST.spec(workload_class)
        payload = _read_artifact(artifact)
        validate_artifact(payload, cohort=cohort, require_verifiable=False)
        raw_class = payload["workload_classes"][workload_class]
        blocker = str(raw_class.get("collection_blocker") or "").strip()
        if blocker:
            # One source-evidence gap must not discard independent evidence for
            # later classes.  The blocker stays in the artifact, is reported to
            # the operator and remains a hard failure in ``verify_artifact``.
            blocked_classes[workload_class] = blocker
            continue
        # Every declared tournament must carry its even share of the cold runs,
        # otherwise the class measures one league instead of one shape.  The
        # floor uses the same fixed class minimum as ``verify_artifact`` and the
        # production loader, so a completed collection always verifies.
        floor = MIN_CANARY_RUNS // len(class_spec.targets)
        while True:
            payload = _read_artifact(artifact)
            validate_artifact(payload, cohort=cohort, require_verifiable=False)
            class_summary = _artifact_summary(payload)["workload_classes"][
                workload_class
            ]
            by_tournament = class_summary["cold_samples_by_tournament"]
            missing_modes = BENCHMARK_ONLY_MODES - set(class_summary["modes"])
            enough_cold = class_summary["cold_samples"] >= target and all(
                by_tournament[str(target_spec.source_tournament_id)] >= floor
                for target_spec in class_spec.targets
            )
            if enough_cold and not missing_modes:
                break
            next_target = min(
                class_spec.targets,
                key=lambda item: (
                    by_tournament[str(item.source_tournament_id)],
                    item.source_tournament_id,
                ),
            )
            with tempfile.TemporaryDirectory(
                prefix=f"{workload_class}-",
                dir=workspace_path,
            ) as directory:
                state = execute_cold_run(
                    class_spec,
                    next_target,
                    experimental_cap_bytes=cap,
                    root=Path(directory),
                )
                append_canary_sample(
                    artifact,
                    state.sample,
                    workload_class=workload_class,
                )
                for mode, runner in (
                    ("no_op", execute_no_op),
                    ("offline_replay", execute_offline_replay),
                    ("single_endpoint_resume", execute_single_endpoint_resume),
                ):
                    if mode not in missing_modes:
                        continue
                    append_canary_sample(
                        artifact,
                        runner(state),
                        workload_class=workload_class,
                    )

    final = _read_artifact(artifact)
    validate_artifact(final, cohort=cohort, require_verifiable=False)
    result = _artifact_summary(final)
    result.update(
        {
            "status": "collected_unverified",
            "artifact": str(artifact),
            "experimental_cap_bytes": cap,
            "production_authorized": False,
            "blocked_workload_classes": blocked_classes,
        }
    )
    return result


def verify_artifact(
    path: os.PathLike[str] | str,
    *,
    cohort_path: os.PathLike[str] | str = DEFAULT_COHORT_PATH,
) -> dict[str, Any]:
    """Atomically promote only after custom evidence and budget policy agree."""

    cohort = load_fixed_cohort(cohort_path)
    artifact = Path(path)
    lock_path = artifact.with_suffix(artifact.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            payload = _read_artifact(artifact)
            validate_artifact(payload, cohort=cohort, require_verifiable=True)
            policies = {}
            if payload.get("verified") is True:
                policies = {
                    name: load_verified_policy(artifact, workload_class=name)
                    for name in REQUIRED_WORKLOAD_CLASSES
                }
            else:
                promoted = dict(payload)
                promoted["verified"] = True
                temporary = artifact.with_name(
                    f"{artifact.name}.verify-{os.getpid()}-{uuid.uuid4().hex}"
                )
                try:
                    _write_atomic(temporary, promoted)
                    policies = {
                        name: load_verified_policy(
                            temporary,
                            workload_class=name,
                        )
                        for name in REQUIRED_WORKLOAD_CLASSES
                    }
                    os.replace(temporary, artifact)
                    directory = os.open(artifact.parent, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                finally:
                    try:
                        temporary.unlink()
                    except FileNotFoundError:
                        pass
            return {
                "status": "verified",
                "artifact": str(artifact),
                "workload_classes": {
                    name: {
                        "artifact_id": policy.artifact_id,
                        "hard_task_bytes": policy.hard_run_bytes,
                        "sample_count": policy.sample_count,
                        "distinct_proxy_exits": policy.distinct_proxy_exits,
                    }
                    for name, policy in policies.items()
                },
            }
        except ProductionBudgetUnavailable as exc:
            raise CanaryPolicyError(
                f"production budget validation failed: {exc}"
            ) from exc
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def extend_artifact(
    path: os.PathLike[str] | str,
    *,
    destination: os.PathLike[str] | str,
    experimental_cap_bytes: Optional[int] = None,
    cohort_path: os.PathLike[str] | str = DEFAULT_COHORT_PATH,
) -> dict[str, Any]:
    """Open a new unverified candidate from a verified artifact.

    A verified artifact is immutable, so a new tournament, a new class or extra
    samples can only be measured in a fresh candidate.  Carrying the existing
    cold samples over is honest evidence *only* while the runtime that produced
    them is byte-for-byte the current one, so an identical runtime fingerprint
    digest is the hard precondition; any drift forces a full re-measurement.
    """

    source_path = Path(path)
    destination_path = Path(destination)
    if destination_path == source_path:
        raise CanaryPolicyError("extend must write a separate candidate artifact")
    if destination_path.exists():
        raise CanaryPolicyError(
            f"extend destination already exists: {destination_path}"
        )
    source = _read_artifact(source_path)
    if source.get("verified") is not True:
        raise CanaryPolicyError(
            "extend needs a verified artifact; keep collecting the candidate"
        )
    fingerprint = source.get("runtime_fingerprint")
    if (
        not isinstance(fingerprint, Mapping)
        or fingerprint.get("digest") != runtime_fingerprint()["digest"]
    ):
        raise CanaryPolicyError(
            "extend cannot carry samples across a changed runtime fingerprint"
        )
    cohort = load_fixed_cohort(cohort_path)
    cap = _positive_int(
        source.get("experimental_hard_cap_bytes")
        if experimental_cap_bytes is None
        else experimental_cap_bytes,
        "experimental canary cap",
    )
    source_classes = source.get("workload_classes")
    benchmarks = source.get("benchmark_samples")
    if not isinstance(source_classes, Mapping) or not isinstance(benchmarks, list):
        raise CanaryPolicyError("verified artifact is malformed")
    payload = _artifact_template(cohort, cap)
    carried: dict[str, int] = {}
    for name, raw_class in payload["workload_classes"].items():
        measured = set(CLASS_MANIFEST.spec(name).measured_tournament_ids)
        previous = source_classes.get(name)
        samples = previous.get("samples") if isinstance(previous, Mapping) else []
        if not isinstance(samples, list):
            raise CanaryPolicyError(f"{name!r}.samples must be an array")
        kept = [
            dict(sample)
            for sample in samples
            if isinstance(sample, Mapping)
            and sample.get("source_tournament_id") in measured
        ]
        raw_class["samples"] = kept
        raw_class["hard_task_bytes"] = max(
            (sample["total_provider_bytes"] for sample in kept),
            default=None,
        )
        carried[name] = len(kept)
    payload["benchmark_samples"] = [
        dict(sample)
        for sample in benchmarks
        if isinstance(sample, Mapping)
        and sample.get("workload_class") in payload["workload_classes"]
        and sample.get("source_tournament_id")
        in set(
            CLASS_MANIFEST.spec(sample["workload_class"]).measured_tournament_ids
        )
    ]
    payload["verified"] = False
    validate_artifact(payload, cohort=cohort, require_verifiable=False)
    _write_atomic(destination_path, payload)
    return {
        "status": "extended_unverified",
        "artifact": str(destination_path),
        "source_artifact": str(source_path),
        "experimental_cap_bytes": cap,
        "carried_cold_samples": carried,
        "verified": False,
        "production_authorized": False,
    }


def _safe_error(exc: BaseException) -> str:
    value = redact_sensitive(exc)
    return _IPV4_RE.sub("[REDACTED_IP]", value)


def _arg_positive(value: str) -> int:
    try:
        return _positive_int(value, "value")
    except CanaryPolicyError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect or verify the fixed SofaScore paid-proxy canary",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("bootstrap", "collect"):
        command = subparsers.add_parser(name)
        command.add_argument("--artifact", default=str(DEFAULT_ARTIFACT_PATH))
        command.add_argument(
            "--experimental-cap-bytes",
            required=True,
            type=_arg_positive,
            help="explicit non-production hard cap configured on proxy-filter",
        )
        if name == "collect":
            command.add_argument(
                "--target-cold-runs",
                default=MIN_CANARY_RUNS,
                type=_arg_positive,
            )
            command.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    verify = subparsers.add_parser("verify")
    verify.add_argument("--artifact", default=str(DEFAULT_ARTIFACT_PATH))
    extend = subparsers.add_parser(
        "extend",
        help=(
            "open a new unverified candidate from a verified artifact, keeping "
            "its cold samples (requires an identical runtime fingerprint)"
        ),
    )
    extend.add_argument("--artifact", default=str(DEFAULT_ARTIFACT_PATH))
    extend.add_argument("--destination", required=True)
    extend.add_argument(
        "--experimental-cap-bytes",
        default=None,
        type=_arg_positive,
        help="optional new cap; defaults to the verified artifact's cap",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "bootstrap":
            cohort = load_fixed_cohort()
            payload = bootstrap_artifact(
                args.artifact,
                cohort=cohort,
                experimental_cap_bytes=args.experimental_cap_bytes,
            )
            result = {
                "status": "bootstrapped_unverified",
                "artifact": str(args.artifact),
                "experimental_cap_bytes": payload["experimental_hard_cap_bytes"],
                "collection_source": payload["collection_source"],
                "production_authorized": False,
            }
        elif args.command == "collect":
            result = collect_canary(
                artifact_path=args.artifact,
                experimental_cap_bytes=args.experimental_cap_bytes,
                target_cold_runs=args.target_cold_runs,
                workspace=args.workspace,
            )
        elif args.command == "extend":
            result = extend_artifact(
                args.artifact,
                destination=args.destination,
                experimental_cap_bytes=args.experimental_cap_bytes,
            )
        else:
            result = verify_artifact(args.artifact)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI fail-closed boundary
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": _safe_error(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
