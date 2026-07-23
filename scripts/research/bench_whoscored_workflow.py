#!/usr/bin/env python3
"""Benchmark one complete WhoScored scope without publishing any data.

The production capacity mode replays one exact, content-addressed seed through
the production match, preview, profile and multi-stage parsers and the
in-memory repository.  A process-wide socket guard makes any attempted network
or paid-route escape fatal.  The direct-first three-phase workflow remains
available only as an explicit diagnostic mode.  Neither mode can write
Bronze/Trino tables or execute DDL.

The diagnostic workflow is run as:

* ``cold``: the temporary raw store is empty;
* ``warm``: every source object must be replayed from that raw store;
* ``incremental``: exactly one match target is invalidated before the run.

Any paid-route use, partial entity result, unexpected warm network request, or
incremental fetch of more than the invalidated target fails the process.
"""

# ruff: noqa: E402 -- production attestation must precede every later import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored capacity workflow requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_WHOSCORED_RUNTIME_CONTRACT = None
if _whoscored_production:
    if (
        getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None)
        != 2
    ):
        raise RuntimeError("image-baked WhoScored startup anchor is required")
    _whoscored_runtime_loader = getattr(
        _whoscored_bootstrap_sys, "_load_whoscored_runtime_contract", None
    )
    if not callable(_whoscored_runtime_loader):
        raise RuntimeError("image-owned WhoScored runtime loader is required")
    _WHOSCORED_RUNTIME_CONTRACT = _whoscored_runtime_loader("/opt/airflow")

import argparse
from contextlib import ExitStack
import hashlib
import json
import logging
import os
from collections import Counter
from datetime import date, datetime
from pathlib import Path
import re
import signal
import socket
import stat
import sys
from tempfile import TemporaryDirectory
import time
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence


_SEALED_BUNDLE_PATH = os.environ.pop("WHOSCORED_CAPACITY_BUNDLE_PATH", "")
_SEALED_SITE_PACKAGES = os.environ.pop(
    "WHOSCORED_CAPACITY_SITE_PACKAGES", ""
)
if _SEALED_BUNDLE_PATH:
    if (
        not sys.flags.isolated
        or not sys.flags.no_site
        or not sys.flags.ignore_environment
        or not sys.dont_write_bytecode
        or re.fullmatch(r"/proc/self/fd/[0-9]+", _SEALED_BUNDLE_PATH) is None
        or not _SEALED_SITE_PACKAGES.startswith("/")
        or Path(os.path.abspath(_SEALED_SITE_PACKAGES))
        != Path(_SEALED_SITE_PACKAGES)
    ):
        raise RuntimeError("capacity sealed-runtime bootstrap is not isolated")
    trusted_stdlib = [
        value
        for value in sys.path
        if isinstance(value, str)
        and value.startswith("/")
        and value != _SEALED_BUNDLE_PATH
    ]
    sys.path[:] = [
        _SEALED_BUNDLE_PATH,
        *trusted_stdlib,
        _SEALED_SITE_PACKAGES,
    ]
    REPO_ROOT = Path(_SEALED_BUNDLE_PATH)
else:
    if _SEALED_SITE_PACKAGES:
        raise RuntimeError("capacity site path requires a sealed runtime bundle")
    REPO_ROOT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

from scrapers.whoscored.catalog import (  # noqa: E402
    DEFAULT_COMPETITIONS_PATH,
    CatalogSeason,
    WhoScoredCatalog,
)
from scrapers.whoscored.domain import WhoScoredScope  # noqa: E402
from scrapers.whoscored.parsers import (  # noqa: E402
    DatasetStatus,
    parse_matchcentre_data,
    parse_preview_bundle,
    parse_profile_bundle,
    parse_team_stage_statistics,
)
from scrapers.whoscored.raw_store import (  # noqa: E402
    RawTarget,
    WhoScoredRawStore,
    match_page_target,
    preview_page_target,
)
from scrapers.whoscored.repository import (  # noqa: E402
    ManifestFailure,
    MatchCandidate,
    MatchCommit,
    PreviewCommit,
    PreviewFailure,
    ProfileCommit,
    WhoScoredScopeRowSpool,
)
from scrapers.whoscored.service import (  # noqa: E402
    DEFAULT_STRUCTURED_REQUESTS_PER_MINUTE,
    MAX_STRUCTURED_REQUESTS_PER_MINUTE,
    PLAYER_DETAILED_STAT_TABS,
    PLAYER_STAGE_STAT_TABS,
    TEAM_DETAILED_STAT_TABS,
    TEAM_STAGE_STAT_TABS,
    STRUCTURED_REQUEST_BURST_SIZE,
    WhoScoredIngestService,
)
from scrapers.whoscored.stage_feeds import STAGE_TEAM_FEED_CATALOG  # noqa: E402
from scrapers.whoscored.transport import (  # noqa: E402
    capacity_browser_session_prefix,
    TransportContext,
    WhoScoredTransport,
)
from scrapers.base.flaresolverr_client import MAX_XHR_BATCH_URLS  # noqa: E402


LOG = logging.getLogger("bench_whoscored_workflow")
MIB = 1024 * 1024
BENCHMARK_VERSION = "whoscored-workflow-benchmark-v2"
CACHE_CAPACITY_SCHEMA_VERSION = 1
CACHE_CAPACITY_MODE = "cache-capacity-v1"
DIRECT_DIAGNOSTIC_MODE = "direct-diagnostic-v1"
DEFAULT_SCOPE = "INT-World Cup=2026"
DEFAULT_MATCH_LIMIT = 3
DEFAULT_PROFILE_LIMIT = 3
MAX_MATCH_LIMIT = 10
MAX_PROFILE_LIMIT = 20
PREVIEW_CANDIDATE_POOL_MULTIPLIER = 3
MAX_PREVIEW_CANDIDATE_POOL = MAX_MATCH_LIMIT * PREVIEW_CANDIDATE_POOL_MULTIPLIER
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ROUTE_COUNTER_FIELDS = (
    "route_requests",
    "route_wire_bytes",
    "failures",
    "paid_proxy_bytes_by_url",
)
_SCALAR_COUNTER_FIELDS = (
    "cache_hits",
    "cache_invalid",
    "browser_sessions",
    "browser_batches",
    "browser_batch_items",
    "paid_urls",
    "paid_proxy_up_bytes",
    "paid_proxy_down_bytes",
    "paid_proxy_bytes",
)
_PAID_ROUTES = {"paid_http", "paid_flaresolverr"}
_DIRECT_ROUTES = {"direct_http", "direct_flaresolverr"}
_FEED_STATUSES = frozenset({"available", "empty", "not_available"})
_CAPACITY_CONTROL_SCHEMA_VERSION = 1
_CAPACITY_CONTROL_READ_LIMIT = 512
_CAPACITY_FLARESOLVERR_ENDPOINT = "http://127.0.0.1:8191"
_CACHE_SEED_MAX_BYTES = 64 * 1024
_CACHE_SOURCE_UNITS = 5


# This deliberately small corpus is source-shaped rather than output-shaped:
# every worker must invoke the real parsers.  Canonical JSON and the pinned
# digest make changing even one source scalar an explicit contract update.
_CACHE_CAPACITY_SEED: Mapping[str, Any] = {
    "schema_version": CACHE_CAPACITY_SCHEMA_VERSION,
    "game_id": 1903117,
    "player_id": 11,
    "match": {
        "expandedMaxMinute": 94,
        "playerIdNameDictionary": {"11": "Starter", "21": "Visitor"},
        "events": [
            {
                "id": 4100001,
                "eventId": 41,
                "minute": 10,
                "second": 2,
                "expandedMinute": 10,
                "period": {"displayName": "FirstHalf"},
                "type": {"displayName": "Pass"},
                "outcomeType": {"displayName": "Successful"},
                "teamId": 26,
                "playerId": 11,
                "x": 25,
                "y": 50,
                "qualifiers": [
                    {"type": {"value": 2, "displayName": "Cross"}}
                ],
            }
        ],
        "home": {
            "teamId": 26,
            "name": "Alpha FC",
            "scores": {"fulltime": 2},
            "stats": {"shotsTotal": 10, "possessionPct": 55.0},
            "players": [
                {
                    "playerId": 11,
                    "name": "Starter",
                    "shirtNo": 4,
                    "position": "DC",
                    "isFirstEleven": True,
                    "stats": {"ratings": {"45": 6.9, "90": 7.2}},
                }
            ],
        },
        "away": {
            "teamId": 167,
            "name": "Beta FC",
            "scores": {"fulltime": 1},
            "stats": {"shotsTotal": 8, "possessionPct": 45.0},
            "players": [
                {
                    "playerId": 21,
                    "name": "Visitor",
                    "shirtNo": 9,
                    "position": "FW",
                    "isFirstEleven": True,
                    "stats": {"ratings": {"45": 6.5, "90": 6.8}},
                }
            ],
        },
        "homeScore": 2,
        "awayScore": 1,
        "status": {"displayName": "FullTime"},
    },
    "preview_html": (
        "<script>var predictedLineups = {home:{formation:'4-3-3',players:["
        "{playerId:11,playerName:'Starter',position:{displayName:'DC'},"
        "rating:7.2}]},away:{formation:'4-4-2',players:[{playerId:21,"
        "playerName:'Visitor',position:{displayName:'FW'},rating:6.8}]}};"
        "var matchHeaderJson = {venueName:'Seed Ground',predictedScore:'2-1'};"
        "</script><div id='missing-players'><div><table><tbody><tr>"
        "<td class='pn'><a href='/Players/12/Show/Reserve'>Reserve</a></td>"
        "<td class='reason'>Injury</td><td class='confirmed'>Confirmed</td>"
        "</tr></tbody></table></div></div>"
    ),
    "profile_html": (
        "<div><span class='info-label'>Name: </span>Starter</div>"
        "<div><span class='info-label'>Current Team: </span>"
        "<a href='/Teams/26/Show/Alpha'>Alpha FC</a></div>"
        "<div><span class='info-label'>Positions: </span>Defender</div>"
        "<script>var currentParticipations = [{tournamentId:36,seasonId:10498,"
        "stageId:23752,teamId:26,teamName:'Alpha FC',"
        "position:{displayName:'DC'}}];</script>"
    ),
    "stages": [
        {
            "stage_id": 23752,
            "source_season_id": 10498,
            "payload": {
                "teamTableStats": [
                    {
                        "teamId": 26,
                        "teamName": "Alpha FC",
                        "apps": 3,
                        "rating": 7.2,
                    }
                ],
                "paging": {
                    "currentPage": 0,
                    "totalPages": 0,
                    "resultsPerPage": 0,
                    "totalResults": 0,
                    "firstRecordIndex": 0,
                    "lastRecordIndex": 0,
                },
            },
        },
        {
            "stage_id": 23753,
            "source_season_id": 10498,
            "payload": {
                "teamTableStats": [
                    {
                        "teamId": 167,
                        "teamName": "Beta FC",
                        "apps": 2,
                        "rating": 6.8,
                    }
                ],
                "paging": {
                    "currentPage": 1,
                    "totalPages": 0,
                    "resultsPerPage": 0,
                    "totalResults": 0,
                    "firstRecordIndex": 0,
                    "lastRecordIndex": 0,
                },
            },
        },
    ],
}


def _canonical_seed_bytes(document: Mapping[str, Any]) -> bytes:
    return json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


_EMBEDDED_CACHE_SEED_BYTES = _canonical_seed_bytes(_CACHE_CAPACITY_SEED)
EXPECTED_CACHE_SEED_SHA256 = (
    "eb3739448a42a00f3a5812c20a0885ea691306634b2922cda1dd2d7c8490fb4a"
)
if hashlib.sha256(_EMBEDDED_CACHE_SEED_BYTES).hexdigest() != (
    EXPECTED_CACHE_SEED_SHA256
):  # pragma: no cover - import-time source review guard
    raise RuntimeError("embedded cache-capacity-v1 seed digest drifted")


def _expected_stage_feed_keys(stage_ids: Iterable[int]) -> frozenset[str]:
    suffixes = {
        *(
            f"team:{category}:{subcategory}"
            for category, subcategory, *_ in TEAM_STAGE_STAT_TABS
        ),
        *(
            f"team-detailed:{spec.category}:{spec.subcategory}"
            for spec in TEAM_DETAILED_STAT_TABS
        ),
        *(
            f"player:{category}:{subcategory}"
            for category, subcategory, _inc_pens in PLAYER_STAGE_STAT_TABS
        ),
        *(
            f"player-detailed:{spec.category}:{spec.subcategory}"
            for spec in PLAYER_DETAILED_STAT_TABS
        ),
        *(f"stagestatfeed:{spec.type_id}" for spec in STAGE_TEAM_FEED_CATALOG),
        "referee:summary",
    }
    if len(suffixes) != 68:
        raise BenchmarkFailure(
            f"source feed catalog drifted: expected 68 feeds per stage, got {len(suffixes)}"
        )
    return frozenset(
        f"{int(stage_id)}:{suffix}" for stage_id in stage_ids for suffix in suffixes
    )


class BenchmarkFailure(RuntimeError):
    """A benchmark invariant failed after setup."""


class _NoNetworkGuard:
    """Fail and count any Python socket/DNS escape during cache replay."""

    _SOCKET_METHODS = (
        "connect",
        "connect_ex",
        "send",
        "sendall",
        "sendto",
        "sendmsg",
    )

    def __init__(self) -> None:
        self.attempts = 0
        self._saved_socket_methods: dict[str, Any] = {}
        self._saved_module_functions: dict[str, Any] = {}

    def _blocked(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        self.attempts += 1
        raise BenchmarkFailure("cache-capacity-v1 attempted network access")

    def __enter__(self) -> "_NoNetworkGuard":
        for name in self._SOCKET_METHODS:
            candidate = getattr(socket.socket, name, None)
            if candidate is not None:
                self._saved_socket_methods[name] = candidate
                setattr(socket.socket, name, self._blocked)
        for name in (
            "create_connection",
            "getaddrinfo",
            "gethostbyaddr",
            "gethostbyname",
            "gethostbyname_ex",
        ):
            candidate = getattr(socket, name, None)
            if candidate is not None:
                self._saved_module_functions[name] = candidate
                setattr(socket, name, self._blocked)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        del exc_type, exc, tb
        for name, candidate in self._saved_socket_methods.items():
            setattr(socket.socket, name, candidate)
        for name, candidate in self._saved_module_functions.items():
            setattr(socket, name, candidate)
        self._saved_socket_methods.clear()
        self._saved_module_functions.clear()
        return False


def _load_cache_capacity_seed(path: Optional[Path]) -> dict[str, Any]:
    """Load only the reviewed seed bytes and reject absence or substitution."""

    if path is None:
        payload = _EMBEDDED_CACHE_SEED_BYTES
    else:
        try:
            metadata = path.stat()
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("cache seed is not a regular file")
            if metadata.st_size <= 0 or metadata.st_size > _CACHE_SEED_MAX_BYTES:
                raise ValueError("cache seed size is invalid")
            payload = path.read_bytes()
        except OSError as exc:
            raise ValueError("cache seed is missing or unreadable") from exc
    digest = hashlib.sha256(payload).hexdigest()
    if digest != EXPECTED_CACHE_SEED_SHA256:
        raise ValueError("cache seed digest does not match the reviewed corpus")
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("cache seed is not canonical JSON") from exc
    if not isinstance(document, Mapping):
        raise ValueError("cache seed root must be an object")
    if _canonical_seed_bytes(document) != payload:
        raise ValueError("cache seed bytes are not canonical")
    if set(document) != {
        "schema_version",
        "game_id",
        "player_id",
        "match",
        "preview_html",
        "profile_html",
        "stages",
    }:
        raise ValueError("cache seed shape does not match cache-capacity-v1")
    if document.get("schema_version") != CACHE_CAPACITY_SCHEMA_VERSION:
        raise ValueError("cache seed schema version is invalid")
    stages = document.get("stages")
    if not isinstance(stages, list) or len(stages) != 2:
        raise ValueError("cache seed must contain exactly two stages")
    return dict(document)


class MemoryRequestLedger:
    """Retain transport audit events for per-phase target cardinality checks."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def append(self, event: Mapping[str, Any]) -> None:
        self.events.append(dict(event))


def _normal_table(name: str) -> str:
    token = str(name)
    return token if token.startswith("whoscored_") else f"whoscored_{token}"


def _json_fingerprint(value: Any) -> str:
    # Benchmark inputs can contain lone UTF-16 surrogates copied verbatim from
    # source HTML.  Production canonicalization repairs them before an Iceberg
    # write; keep the non-publishing fingerprint total as well by escaping all
    # non-ASCII code points before UTF-8 encoding.
    encoder = json.JSONEncoder(
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256()
    for token in encoder.iterencode(value):
        digest.update(token.encode("utf-8"))
    return digest.hexdigest()


def _dataset_fingerprint(
    datasets: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    metadata: Optional[Mapping[str, Any]] = None,
) -> str:
    """Fingerprint row collections without building one giant JSON document."""

    digest = hashlib.sha256(b"whoscored-benchmark-datasets-v2\0")
    for name in sorted(datasets):
        digest.update(json.dumps(str(name), ensure_ascii=True).encode("utf-8"))
        digest.update(b"\0")
        rows = datasets[name]
        if isinstance(rows, WhoScoredScopeRowSpool):
            digest.update(rows.content_fingerprint().encode("ascii"))
            observed = len(rows)
        else:
            observed = 0
            for row in rows:
                digest.update(_json_fingerprint(row).encode("ascii"))
                digest.update(b"\n")
                observed += 1
        if observed != len(rows):
            raise ValueError(f"benchmark dataset {name} changed while fingerprinting")
        digest.update(str(observed).encode("ascii"))
        digest.update(b"\0")
    digest.update(_json_fingerprint(metadata or {}).encode("ascii"))
    return digest.hexdigest()


def _optional_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    converter = getattr(value, "to_pydatetime", None)
    if callable(converter):
        return _optional_datetime(converter())
    token = str(value).strip().replace("Z", "+00:00")
    if not token:
        return None
    parsed = datetime.fromisoformat(token)
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "t"}


class InMemoryBenchmarkRepository:
    """Atomic, idempotent sink implementing only the service repository seam.

    It deliberately has no writer or Trino client.  Batch fingerprints are
    retained so warm replay measures accepted rows while proving that no new
    logical batch or duplicate current row was created.
    """

    def __init__(self, scope: CatalogSeason | WhoScoredScope) -> None:
        self.scope = scope.scope if isinstance(scope, CatalogSeason) else scope
        self._scope_datasets: dict[str, tuple[dict[str, Any], ...]] = {}
        self._scope_counts: dict[str, int] = {}
        self._match_commits: dict[int, MatchCommit] = {}
        self._preview_commits: dict[int, PreviewCommit] = {}
        self._profile_commits: dict[int, ProfileCommit] = {}
        self._batch_fingerprints: dict[tuple[str, str, str], str] = {}
        self._accepted_rows: Counter[str] = Counter()
        self._new_batch_rows: Counter[str] = Counter()
        self._idempotent_rows: Counter[str] = Counter()
        self._commit_calls: Counter[str] = Counter()
        self.failures: list[Any] = []

    @staticmethod
    def ensure_schema(*args: Any, **kwargs: Any) -> None:
        """No-op by construction: the benchmark never owns a physical schema."""

    @staticmethod
    def latest_source_season_id(league: str, season: str) -> None:
        return None

    @staticmethod
    def _validate_hash_and_raw(payload_sha256: str, raw_uri: str) -> None:
        if _HASH_RE.fullmatch(str(payload_sha256)) is None:
            raise ValueError("commit payload_sha256 is not a SHA-256 digest")
        if not str(raw_uri).strip():
            raise ValueError("commit lacks a raw URI")

    @staticmethod
    def _dataset_counts(
        datasets: Mapping[str, Sequence[Mapping[str, Any]]],
    ) -> dict[str, int]:
        return {_normal_table(name): len(rows) for name, rows in datasets.items()}

    def _record_batch(
        self,
        *,
        kind: str,
        identity: str,
        batch_id: str,
        counts: Mapping[str, int],
        fingerprint: str,
    ) -> None:
        key = (kind, identity, batch_id)
        previous = self._batch_fingerprints.get(key)
        if previous is not None and previous != fingerprint:
            raise ValueError(f"in-memory batch conflict for {kind}/{identity}")
        for table, count in counts.items():
            self._accepted_rows[table] += int(count)
            target = (
                self._idempotent_rows if previous is not None else self._new_batch_rows
            )
            target[table] += int(count)
        self._batch_fingerprints[key] = fingerprint
        self._commit_calls[kind] += 1

    @staticmethod
    def _validate_distinct(
        table: str,
        rows: Sequence[Mapping[str, Any]],
        distinct_key: str,
    ) -> None:
        if isinstance(rows, WhoScoredScopeRowSpool):
            if distinct_key != "entity_key" or (
                len(rows) and distinct_key not in rows.columns
            ):
                raise ValueError(f"{table}: invalid spool distinct-key contract")
            return
        values: set[str] = set()
        for row in rows:
            if row.get(distinct_key) is None:
                raise ValueError(f"{table}: missing distinct key {distinct_key}")
            value = str(row[distinct_key])
            if value in values:
                raise ValueError(f"{table}: duplicate {distinct_key} values")
            values.add(value)

    def commit_scope_bundle(
        self,
        *,
        league: str,
        season: str,
        entity_group: str,
        datasets: Mapping[str, Sequence[Mapping[str, Any]]],
        distinct_keys: Mapping[str, str],
        payload_sha256: str,
        raw_uris: Sequence[str],
        source_empty: Iterable[str] = (),
        source_unavailable: Iterable[str] = (),
        feed_states: Optional[Mapping[str, str]] = None,
        **kwargs: Any,
    ) -> str:
        del kwargs
        if league != self.scope.competition_id or season != self.scope.season_id:
            raise ValueError("scope commit does not match benchmark scope")
        self._validate_hash_and_raw(payload_sha256, next(iter(raw_uris), ""))
        empty = set(source_empty)
        unavailable = set(source_unavailable)
        if empty & unavailable:
            raise ValueError("a scope dataset cannot be empty and unavailable")
        schedule_rows: tuple[dict[str, Any], ...] = ()
        for name, source_rows in datasets.items():
            if name not in distinct_keys:
                raise ValueError(f"{name}: no distinct-key contract")
            self._validate_distinct(name, source_rows, distinct_keys[name])
            if name == "whoscored_schedule":
                # Only schedule drives later benchmark candidate selection.
                # Statistics stay in their production disk spool; retaining a
                # second dict copy here was benchmark-only multi-GiB overhead.
                schedule_rows = tuple(dict(row) for row in source_rows)
        if not schedule_rows:
            raise ValueError("scope commit contains no schedule rows")
        schedule_stage_ids = {
            int(row["stage_id"])
            for row in schedule_rows
            if row.get("stage_id") is not None
        }
        normalized_feed_states = dict(sorted((feed_states or {}).items()))
        if schedule_stage_ids or normalized_feed_states:
            actual_feed_states = frozenset(normalized_feed_states)
            try:
                feed_stage_ids = {
                    int(key.split(":", 1)[0]) for key in actual_feed_states
                }
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "scope feed-state contract has an invalid stage prefix"
                ) from exc
            if any(stage_id <= 0 for stage_id in feed_stage_ids):
                raise ValueError(
                    "scope feed-state contract has a non-positive stage prefix"
                )
            uncovered_schedule_stages = schedule_stage_ids - feed_stage_ids
            if uncovered_schedule_stages:
                raise ValueError(
                    "scope feed-state contract does not cover schedule stages: "
                    + ", ".join(map(str, sorted(uncovered_schedule_stages)))
                )
            expected_feed_states = _expected_stage_feed_keys(feed_stage_ids)
            if actual_feed_states != expected_feed_states:
                raise ValueError(
                    "scope feed-state contract mismatch: "
                    f"missing={len(expected_feed_states - actual_feed_states)} "
                    f"extra={len(actual_feed_states - expected_feed_states)}"
                )
            invalid_statuses = {
                status
                for status in normalized_feed_states.values()
                if status not in _FEED_STATUSES
            }
            if invalid_statuses:
                raise ValueError(
                    f"scope feed-state contract has invalid statuses {invalid_statuses}"
                )
        fingerprint = _dataset_fingerprint(
            datasets,
            metadata={"feed_states": normalized_feed_states},
        )
        batch_id = (
            "ws-scope-"
            + hashlib.sha256(
                f"{league}\0{season}\0{entity_group}\0{payload_sha256}".encode()
            ).hexdigest()
        )
        counts = self._dataset_counts(datasets)
        self._record_batch(
            kind="scope",
            identity=f"{league}={season}",
            batch_id=batch_id,
            counts=counts,
            fingerprint=fingerprint,
        )
        self._scope_datasets = {"whoscored_schedule": schedule_rows}
        self._scope_counts = counts
        return batch_id

    def _schedule_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._scope_datasets.get("whoscored_schedule", ())]

    @staticmethod
    def _completed(row: Mapping[str, Any]) -> bool:
        try:
            status = int(row.get("status") or 0)
        except (TypeError, ValueError):
            return False
        return status == 6 or (
            status == 1
            and row.get("home_score") is not None
            and row.get("away_score") is not None
        )

    def benchmark_match_ids(self, limit: int) -> list[int]:
        eligible = [
            row
            for row in self._schedule_rows()
            if self._completed(row)
            and _as_bool(row.get("has_preview"))
            and row.get("game_id") is not None
        ]
        eligible.sort(
            key=lambda row: (
                _optional_datetime(row.get("date")) or datetime.min,
                int(row["game_id"]),
            )
        )
        return [int(row["game_id"]) for row in eligible[: int(limit)]]

    def list_match_candidates(
        self,
        league: str,
        season: str,
        *,
        match_ids: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        include_success: bool = False,
        kickoff_from: Optional[datetime] = None,
    ) -> list[MatchCandidate]:
        del include_success
        wanted = {int(value) for value in (match_ids or ())}
        rows = [row for row in self._schedule_rows() if self._completed(row)]
        if wanted:
            rows = [row for row in rows if int(row["game_id"]) in wanted]
        if kickoff_from is not None:
            rows = [
                row
                for row in rows
                if (_optional_datetime(row.get("date")) or datetime.min)
                >= kickoff_from.replace(tzinfo=None)
            ]
        rows.sort(
            key=lambda row: (
                _optional_datetime(row.get("date")) or datetime.min,
                int(row["game_id"]),
            )
        )
        if limit is not None:
            rows = rows[: int(limit)]
        return [
            MatchCandidate(
                game_id=int(row["game_id"]),
                league=league,
                season=season,
                game=str(
                    row.get("game")
                    or f"{row.get('home_team', '')} - {row.get('away_team', '')}"
                ),
                kickoff=_optional_datetime(row.get("date")),
                status=int(row.get("status") or 0),
                match_is_opta=_as_bool(row.get("match_is_opta")),
            )
            for row in rows
        ]

    def validate_match_commit(self, commit: MatchCommit) -> None:
        self._validate_hash_and_raw(commit.payload_sha256, commit.raw_uri)
        legal = {status.value for status in DatasetStatus}
        if not commit.dataset_statuses:
            raise ValueError("match commit lacks dataset statuses")
        if set(commit.dataset_statuses.values()) - legal:
            raise ValueError("match commit has an invalid dataset status")
        if commit.schedule_status == 6 and commit.is_opta:
            if commit.dataset_statuses.get("events") != DatasetStatus.AVAILABLE.value:
                raise ValueError("completed Opta match has no available events")
            if not commit.events:
                raise ValueError("completed Opta match has no events")

    @staticmethod
    def _match_datasets(commit: MatchCommit) -> dict[str, Sequence[Mapping[str, Any]]]:
        datasets = {
            "events": commit.events,
            "lineups": commit.lineups,
            **dict(commit.datasets),
        }
        return {_normal_table(name): rows for name, rows in datasets.items()}

    def commit_matches(self, commits: Sequence[MatchCommit]) -> tuple[str, ...]:
        for commit in commits:
            self.validate_match_commit(commit)
        for commit in commits:
            datasets = self._match_datasets(commit)
            counts = {name: len(rows) for name, rows in datasets.items()}
            fingerprint = _json_fingerprint(datasets)
            self._record_batch(
                kind="matches",
                identity=str(commit.game_id),
                batch_id=commit.batch_id,
                counts=counts,
                fingerprint=fingerprint,
            )
            self._match_commits[int(commit.game_id)] = commit
        return tuple(commit.batch_id for commit in commits)

    def record_failure(self, failure: ManifestFailure) -> None:
        self.failures.append(failure)

    def list_preview_candidates(
        self,
        league: str,
        season: str,
        *,
        limit: Optional[int] = None,
        match_ids: Optional[Iterable[int]] = None,
        force_replay: bool = False,
    ) -> list[dict[str, Any]]:
        del force_replay
        wanted = {int(value) for value in (match_ids or ())}
        rows = [
            row
            for row in self._schedule_rows()
            if _as_bool(row.get("has_preview"))
            and (not wanted or int(row["game_id"]) in wanted)
        ]
        rows.sort(
            key=lambda row: (
                _optional_datetime(row.get("date")) or datetime.min,
                int(row["game_id"]),
            )
        )
        if limit is not None:
            rows = rows[: int(limit)]
        return [
            {
                "game_id": int(row["game_id"]),
                "game": str(
                    row.get("game")
                    or f"{row.get('home_team', '')} - {row.get('away_team', '')}"
                ),
                "date": _optional_datetime(row.get("date")),
                "home_team": row.get("home_team"),
                "away_team": row.get("away_team"),
                "attempt_no": 1,
                "force_refresh": False,
            }
            for row in rows
        ]

    def validate_preview_commit(self, commit: PreviewCommit) -> None:
        self._validate_hash_and_raw(commit.payload_sha256, commit.raw_uri)
        legal = {status.value for status in DatasetStatus}
        if not commit.dataset_statuses:
            raise ValueError("preview commit lacks dataset statuses")
        if set(commit.dataset_statuses.values()) - legal:
            raise ValueError("preview commit has an invalid dataset status")
        if DatasetStatus.NOT_AVAILABLE.value in commit.dataset_statuses.values():
            raise ValueError("preview structure is not available")

    @staticmethod
    def _preview_datasets(
        commit: PreviewCommit,
    ) -> dict[str, Sequence[Mapping[str, Any]]]:
        datasets = {"missing_players": commit.missing_players, **dict(commit.datasets)}
        return {_normal_table(name): rows for name, rows in datasets.items()}

    def commit_previews(self, commits: Sequence[PreviewCommit]) -> tuple[str, ...]:
        for commit in commits:
            self.validate_preview_commit(commit)
        for commit in commits:
            datasets = self._preview_datasets(commit)
            counts = {name: len(rows) for name, rows in datasets.items()}
            fingerprint = _json_fingerprint(datasets)
            self._record_batch(
                kind="previews",
                identity=str(commit.game_id),
                batch_id=commit.batch_id,
                counts=counts,
                fingerprint=fingerprint,
            )
            self._preview_commits[int(commit.game_id)] = commit
        return tuple(commit.batch_id for commit in commits)

    def record_preview_failure(self, failure: PreviewFailure) -> None:
        self.failures.append(failure)

    def benchmark_profile_ids(self, limit: int) -> list[int]:
        player_ids: set[int] = set()
        for commit in self._match_commits.values():
            for rows in self._match_datasets(commit).values():
                for row in rows:
                    value = row.get("player_id")
                    try:
                        player_id = int(value)
                    except (TypeError, ValueError):
                        continue
                    if player_id > 0:
                        player_ids.add(player_id)
        return sorted(player_ids)[: int(limit)]

    def list_profile_candidates(
        self,
        *,
        scopes: Sequence[WhoScoredScope],
        limit: int = 500,
    ) -> list[int]:
        if {scope.spec for scope in scopes} != {self.scope.spec}:
            raise ValueError("profile candidates escaped the benchmark scope")
        return self.benchmark_profile_ids(limit)

    def commit_profiles(self, commits: Sequence[ProfileCommit]) -> tuple[str, ...]:
        for commit in commits:
            self._validate_hash_and_raw(commit.payload_sha256, commit.raw_uri)
            if int(commit.player_id) <= 0 or not commit.profile.get("name"):
                raise ValueError("profile commit lacks a valid player identity")
        for commit in commits:
            datasets: dict[str, Sequence[Mapping[str, Any]]] = {
                "whoscored_player_profile_versions": (dict(commit.profile),),
                "whoscored_player_stage_participations": commit.participations,
            }
            counts = {name: len(rows) for name, rows in datasets.items()}
            fingerprint = _json_fingerprint(datasets)
            self._record_batch(
                kind="profiles",
                identity=str(commit.player_id),
                batch_id=commit.batch_id,
                counts=counts,
                fingerprint=fingerprint,
            )
            self._profile_commits[int(commit.player_id)] = commit
        return tuple(commit.batch_id for commit in commits)

    def record_profile_failure(self, **failure: Any) -> None:
        self.failures.append(dict(failure))

    def _logical_current_rows(self) -> dict[str, int]:
        current: Counter[str] = Counter(self._scope_counts)
        for commit in self._match_commits.values():
            for name, rows in self._match_datasets(commit).items():
                current[name] += len(rows)
        for commit in self._preview_commits.values():
            for name, rows in self._preview_datasets(commit).items():
                current[name] += len(rows)
        for commit in self._profile_commits.values():
            current["whoscored_player_profile_versions"] += 1
            current["whoscored_player_stage_participations"] += len(
                commit.participations
            )
        return dict(sorted(current.items()))

    def metrics_snapshot(self) -> dict[str, Any]:
        return {
            "accepted_rows": dict(self._accepted_rows),
            "new_batch_rows": dict(self._new_batch_rows),
            "idempotent_rows": dict(self._idempotent_rows),
            "logical_current_rows": self._logical_current_rows(),
            "commit_calls": dict(self._commit_calls),
            "failure_records": len(self.failures),
        }


class BenchmarkFactories:
    """Dependency seams used by unit tests; production defaults stay explicit."""

    def __init__(
        self,
        *,
        load_catalog: Callable[[Path], Any],
        create_raw_store: Callable[[str], Any],
        create_transport: Callable[[str, MemoryRequestLedger, argparse.Namespace], Any],
        create_repository: Callable[[Any], Any],
        create_service: Callable[..., Any],
        parse_preview: Callable[..., Any] = parse_preview_bundle,
    ) -> None:
        self.load_catalog = load_catalog
        self.create_raw_store = create_raw_store
        self.create_transport = create_transport
        self.create_repository = create_repository
        self.create_service = create_service
        self.parse_preview = parse_preview


def _default_factories() -> BenchmarkFactories:
    def create_transport(
        scope_spec: str,
        ledger: MemoryRequestLedger,
        args: argparse.Namespace,
    ) -> WhoScoredTransport:
        return WhoScoredTransport(
            flaresolverr_url=str(args.flaresolverr_url),
            paid_proxy_url=None,
            proxy_control_url=None,
            context=TransportContext.from_env().request_context(
                scope=scope_spec,
                entity="workflow_benchmark",
            ),
            request_ledger=ledger,
            browser_session_owner=getattr(args, "browser_session_owner", None),
        )

    def create_service(**kwargs: Any) -> WhoScoredIngestService:
        return WhoScoredIngestService(**kwargs)

    return BenchmarkFactories(
        load_catalog=WhoScoredCatalog.from_file,
        create_raw_store=WhoScoredRawStore.from_uri,
        create_transport=create_transport,
        create_repository=InMemoryBenchmarkRepository,
        create_service=create_service,
        parse_preview=parse_preview_bundle,
    )


def _map_delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, int]:
    keys = set(before) | set(after)
    result = {
        str(key): int(after.get(key, 0) or 0) - int(before.get(key, 0) or 0)
        for key in keys
    }
    if any(value < 0 for value in result.values()):
        raise BenchmarkFailure("a cumulative counter moved backwards")
    return dict(sorted((key, value) for key, value in result.items() if value))


def _traffic_delta(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    delta: dict[str, Any] = {}
    for field in _ROUTE_COUNTER_FIELDS:
        delta[field] = _map_delta(
            before.get(field, {}) or {}, after.get(field, {}) or {}
        )
    for field in _SCALAR_COUNTER_FIELDS:
        value = int(after.get(field, 0) or 0) - int(before.get(field, 0) or 0)
        if value < 0:
            raise BenchmarkFailure(f"traffic counter {field} moved backwards")
        delta[field] = value

    requests = delta["route_requests"]
    wire_bytes = delta["route_wire_bytes"]
    delta["source_request_attempts"] = sum(
        value for route, value in requests.items() if route != "raw_cache"
    )
    delta["direct_wire_bytes"] = sum(
        wire_bytes.get(route, 0) for route in _DIRECT_ROUTES
    )
    delta["direct_mb"] = round(delta["direct_wire_bytes"] / MIB, 6)
    delta["paid_mb"] = round(delta["paid_proxy_bytes"] / MIB, 6)
    delta["paid_route_requests"] = sum(requests.get(route, 0) for route in _PAID_ROUTES)
    source_events = [
        event for event in events if str(event.get("route") or "") != "raw_cache"
    ]
    delta["source_targets"] = sorted(
        {
            str(event.get("cache_key"))
            for event in source_events
            if event.get("cache_key")
        }
    )
    delta["successful_source_targets"] = sorted(
        {
            str(event.get("cache_key"))
            for event in source_events
            if event.get("cache_key") and event.get("status") == "success"
        }
    )
    # Capacity is charged in completed logical page units. Multiple physical
    # attempts or route fallbacks for one target must not inflate the useful
    # throughput projection.
    delta["successful_page_units"] = len(delta["successful_source_targets"])
    delta["source_urls"] = sorted(
        {str(event.get("url")) for event in source_events if event.get("url")}
    )
    delta["raw_cache_misses"] = sum(
        1
        for event in events
        if event.get("route") == "raw_cache" and event.get("status") == "miss"
    )
    delta["ledger_events"] = len(events)
    return delta


def _metrics_delta(
    before: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, Any]:
    accepted = _map_delta(
        before.get("accepted_rows", {}) or {}, after.get("accepted_rows", {}) or {}
    )
    new_batch = _map_delta(
        before.get("new_batch_rows", {}) or {},
        after.get("new_batch_rows", {}) or {},
    )
    idempotent = _map_delta(
        before.get("idempotent_rows", {}) or {},
        after.get("idempotent_rows", {}) or {},
    )
    commit_calls = _map_delta(
        before.get("commit_calls", {}) or {}, after.get("commit_calls", {}) or {}
    )
    failures = int(after.get("failure_records", 0) or 0) - int(
        before.get("failure_records", 0) or 0
    )
    return {
        "accepted_by_dataset": accepted,
        "accepted_total": sum(accepted.values()),
        "new_batch_by_dataset": new_batch,
        "new_batch_total": sum(new_batch.values()),
        "idempotent_by_dataset": idempotent,
        "idempotent_total": sum(idempotent.values()),
        "logical_current_by_dataset": dict(
            sorted((after.get("logical_current_rows", {}) or {}).items())
        ),
        "logical_current_total": sum(
            int(value)
            for value in (after.get("logical_current_rows", {}) or {}).values()
        ),
        "commit_calls": commit_calls,
        "failure_records": failures,
    }


def _result_document(result: Any) -> dict[str, Any]:
    renderer = getattr(result, "as_dict", None)
    if not callable(renderer):
        raise BenchmarkFailure("service returned a result without as_dict()")
    document = dict(renderer())
    return json.loads(json.dumps(document, default=str))


def _require_complete(result: Any, *, entity: str, expected: int) -> None:
    if str(getattr(result, "entity", "")) != entity:
        raise BenchmarkFailure(f"expected {entity} result")
    errors = list(getattr(result, "errors", ()) or ())
    retryable = list(getattr(result, "retryable", ()) or ())
    terminal = list(getattr(result, "terminal", ()) or ())
    attempted = int(getattr(result, "attempted", -1))
    succeeded = int(getattr(result, "succeeded", -1))
    if errors or retryable or terminal:
        raise BenchmarkFailure(
            f"{entity} was partial: errors={errors}, retryable={retryable}, "
            f"terminal={terminal}"
        )
    if attempted != expected or succeeded != expected:
        raise BenchmarkFailure(
            f"{entity} completeness is {succeeded}/{attempted}, expected "
            f"{expected}/{expected}"
        )


def _parsed_rows(results: Sequence[Any]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    for result in results:
        for name, value in (getattr(result, "counts", {}) or {}).items():
            counts[_normal_table(str(name))] += int(value)
    return {"by_dataset": dict(sorted(counts.items())), "total": sum(counts.values())}


def _select_complete_preview_match_ids(
    *,
    service: Any,
    repository: Any,
    match_limit: int,
    preview_parser: Callable[..., Any],
) -> tuple[list[int], dict[str, Any]]:
    """Select a bounded deterministic sample with complete preview structures.

    The probe uses the production fetch/cache path and parser.  An explicitly
    parsed ``not_available`` dataset makes only that candidate ineligible;
    transport and parser exceptions remain fatal.  Running this inside every
    phase makes cold probe requests part of throughput evidence and replays
    every probed raw object during warm/incremental phases.
    """

    pool_limit = min(
        MAX_PREVIEW_CANDIDATE_POOL,
        int(match_limit) * PREVIEW_CANDIDATE_POOL_MULTIPLIER,
    )
    pool_ids = list(repository.benchmark_match_ids(pool_limit))
    if len(pool_ids) != len(set(pool_ids)) or any(
        type(game_id) is not int or game_id <= 0 for game_id in pool_ids
    ):
        raise BenchmarkFailure("preview candidate pool has invalid match identities")
    candidates = repository.list_preview_candidates(
        service.scope.competition_id,
        service.scope.season_id,
        match_ids=pool_ids,
        limit=pool_limit,
        force_replay=True,
    )
    by_id: dict[int, Mapping[str, Any]] = {}
    for candidate in candidates:
        game_id = int(candidate["game_id"])
        if game_id in by_id:
            raise BenchmarkFailure("preview candidate pool contains duplicate matches")
        by_id[game_id] = candidate
    if set(by_id) != set(pool_ids):
        raise BenchmarkFailure("preview candidate pool changed during selection")

    expected_datasets = {
        "missing_players",
        "preview_lineups",
        "preview_sections",
    }
    selected: list[int] = []
    probed: list[int] = []
    rejected_not_available = 0
    for game_id in pool_ids:
        candidate = by_id[game_id]
        target = preview_page_target(game_id)
        parsed_holder: dict[str, Any] = {}

        def validate(
            response: Any,
            *,
            current_candidate: Mapping[str, Any] = candidate,
            current_game_id: int = game_id,
        ) -> bool:
            parsed_holder["preview"] = preview_parser(
                response.text,
                scope=service.scope,
                game_id=current_game_id,
                game=current_candidate["game"],
                home_team=current_candidate["home_team"],
                away_team=current_candidate["away_team"],
            )
            return True

        service._fetch(target, validator=validate, allow_cache=True)
        parsed = parsed_holder.get("preview")
        if parsed is None:
            raise BenchmarkFailure("preview probe parser returned no result")
        datasets = dict(getattr(parsed, "datasets", {}) or {})
        if set(datasets) != expected_datasets:
            raise BenchmarkFailure("preview probe dataset contract drifted")
        statuses = [dataset.status for dataset in datasets.values()]
        if any(type(status) is not DatasetStatus for status in statuses):
            raise BenchmarkFailure("preview probe returned an invalid dataset status")
        probed.append(game_id)
        if DatasetStatus.NOT_AVAILABLE in statuses:
            rejected_not_available += 1
            continue
        selected.append(game_id)
        if len(selected) == int(match_limit):
            break

    if len(selected) != int(match_limit):
        raise BenchmarkFailure(
            f"scope has only {len(selected)} complete previews in the first "
            f"{len(probed)} bounded candidates; {match_limit} required"
        )
    return selected, {
        "candidate_pool_limit": pool_limit,
        "candidate_count": len(pool_ids),
        "probed_match_ids": probed,
        "rejected_not_available": rejected_not_available,
        "selected_match_ids": list(selected),
    }


def _execute_phase(
    name: str,
    *,
    service: Any,
    repository: Any,
    transport: Any,
    ledger: MemoryRequestLedger,
    match_limit: int,
    profile_limit: int,
    preview_parser: Callable[..., Any],
) -> dict[str, Any]:
    traffic_before = dict(transport.get_traffic_stats())
    metrics_before = dict(repository.metrics_snapshot())
    ledger_offset = len(ledger.events)
    started = time.monotonic()
    results: list[Any] = []
    selected_match_ids: list[int] = []
    selected_profile_ids: list[int] = []
    preview_probe: dict[str, Any] = {}
    try:
        schedule = service.sync_schedule()
        results.append(schedule)
        _require_complete(schedule, entity="schedule", expected=1)

        selected_match_ids, preview_probe = _select_complete_preview_match_ids(
            service=service,
            repository=repository,
            match_limit=match_limit,
            preview_parser=preview_parser,
        )
        matches = service.sync_matches(
            match_ids=selected_match_ids,
            limit=match_limit,
            force_replay=True,
        )
        results.append(matches)
        _require_complete(matches, entity="matches", expected=match_limit)

        previews = service.sync_previews(
            match_ids=selected_match_ids,
            limit=match_limit,
            force_replay=True,
        )
        results.append(previews)
        _require_complete(previews, entity="previews", expected=match_limit)

        selected_profile_ids = repository.benchmark_profile_ids(profile_limit)
        if len(selected_profile_ids) != profile_limit:
            raise BenchmarkFailure(
                f"sampled matches expose only {len(selected_profile_ids)} players; "
                f"{profile_limit} required"
            )
        profiles = service.sync_profiles(
            limit=profile_limit,
            candidate_scopes=(service.scope,),
        )
        results.append(profiles)
        _require_complete(profiles, entity="profiles", expected=profile_limit)
        status = "success"
        error = None
    except Exception as exc:
        status = "failed"
        error = f"{type(exc).__name__}: {exc}"

    elapsed = time.monotonic() - started
    traffic_after = dict(transport.get_traffic_stats())
    metrics_after = dict(repository.metrics_snapshot())
    traffic = _traffic_delta(
        traffic_before, traffic_after, ledger.events[ledger_offset:]
    )
    committed = _metrics_delta(metrics_before, metrics_after)
    return {
        "name": name,
        "status": status,
        "elapsed_seconds": round(elapsed, 3),
        "selected_match_ids": selected_match_ids,
        "selected_profile_ids": selected_profile_ids,
        "preview_probe": preview_probe,
        "results": [_result_document(result) for result in results],
        "parsed_rows": _parsed_rows(results),
        "committed_rows": committed,
        "traffic": traffic,
        "error": error,
    }


def _require_phase_success(phase: Mapping[str, Any]) -> None:
    if phase.get("status") != "success":
        raise BenchmarkFailure(str(phase.get("error") or "phase failed"))
    traffic = phase["traffic"]
    if int(traffic.get("paid_proxy_bytes", 0)) != 0:
        raise BenchmarkFailure(f"{phase['name']} used paid proxy bytes")
    if int(traffic.get("paid_route_requests", 0)) != 0:
        raise BenchmarkFailure(f"{phase['name']} used a paid transport route")
    if int(phase["committed_rows"].get("failure_records", 0)) != 0:
        raise BenchmarkFailure(f"{phase['name']} recorded repository failures")


def _validate_args(args: argparse.Namespace) -> Optional[str]:
    if getattr(args, "capacity_control_fd", None) is not None:
        return "capacity control fd was not resolved"
    mode = getattr(args, "mode", DIRECT_DIAGNOSTIC_MODE)
    if mode not in {CACHE_CAPACITY_MODE, DIRECT_DIAGNOSTIC_MODE}:
        return "workflow mode is invalid"
    if mode == CACHE_CAPACITY_MODE and (
        getattr(args, "browser_session_owner", None) is not None
        or getattr(args, "flaresolverr_url", None) is not None
    ):
        return "cache-capacity-v1 forbids browser and network configuration"
    if mode == DIRECT_DIAGNOSTIC_MODE and getattr(
        args, "cache_seed_file", None
    ) is not None:
        return "cache seed is valid only in cache-capacity-v1"
    scope = str(getattr(args, "scope", ""))
    if scope.count("=") != 1 or not all(part.strip() for part in scope.split("=", 1)):
        return "scope must have the form '<competition>=<season-id>'"
    match_limit = int(getattr(args, "match_limit", 0))
    profile_limit = int(getattr(args, "profile_limit", 0))
    if not 1 <= match_limit <= MAX_MATCH_LIMIT:
        return f"match limit must be in 1..{MAX_MATCH_LIMIT}"
    if not 1 <= profile_limit <= MAX_PROFILE_LIMIT:
        return f"profile limit must be in 1..{MAX_PROFILE_LIMIT}"
    browser_session_owner = getattr(args, "browser_session_owner", None)
    if browser_session_owner is not None:
        try:
            capacity_browser_session_prefix(browser_session_owner)
        except ValueError:
            return "browser session owner is invalid"
    return None


def _apply_capacity_control(args: argparse.Namespace) -> argparse.Namespace:
    """Consume one protected inherited pipe without exposing controls in argv."""

    control_fd = getattr(args, "capacity_control_fd", None)
    mode = getattr(args, "mode", DIRECT_DIAGNOSTIC_MODE)
    if control_fd is None:
        if mode == CACHE_CAPACITY_MODE:
            if (
                getattr(args, "browser_session_owner", None) is not None
                or getattr(args, "flaresolverr_url", None) is not None
            ):
                raise ValueError("cache capacity network configuration conflicts")
            return args
        if getattr(args, "flaresolverr_url", None) is None:
            args.flaresolverr_url = os.environ.get(
                "FLARESOLVERR_URL", "http://flaresolverr:8191"
            )
        return args
    if mode == CACHE_CAPACITY_MODE:
        try:
            os.close(control_fd)
        except (OSError, TypeError):
            pass
        raise ValueError("cache capacity does not accept browser control")
    if type(control_fd) is not int or control_fd < 3:
        raise ValueError("capacity control arguments conflict")
    if (
        getattr(args, "browser_session_owner", None) is not None
        or getattr(args, "flaresolverr_url", None) is not None
    ):
        try:
            os.close(control_fd)
        except OSError:
            pass
        raise ValueError("capacity control arguments conflict")
    try:
        metadata = os.fstat(control_fd)
        if not stat.S_ISFIFO(metadata.st_mode):
            raise ValueError("capacity control descriptor is not a pipe")
        os.set_blocking(control_fd, False)
        payload = os.read(control_fd, _CAPACITY_CONTROL_READ_LIMIT)
    except (OSError, ValueError) as exc:
        raise ValueError("capacity control is unavailable") from exc
    finally:
        try:
            os.close(control_fd)
        except OSError:
            pass
    if not payload or len(payload) >= _CAPACITY_CONTROL_READ_LIMIT:
        raise ValueError("capacity control payload size is invalid")
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("capacity control payload is invalid") from exc
    if not isinstance(document, Mapping) or set(document) != {
        "schema_version",
        "owner",
        "flaresolverr_endpoint",
    }:
        raise ValueError("capacity control payload shape is invalid")
    if (
        type(document["schema_version"]) is not int
        or document["schema_version"] != _CAPACITY_CONTROL_SCHEMA_VERSION
    ):
        raise ValueError("capacity control schema version is invalid")
    owner = document["owner"]
    endpoint = document["flaresolverr_endpoint"]
    try:
        capacity_browser_session_prefix(owner)
    except ValueError as exc:
        raise ValueError("capacity control owner is invalid") from exc
    if type(endpoint) is not str or endpoint != _CAPACITY_FLARESOLVERR_ENDPOINT:
        raise ValueError("capacity control endpoint is invalid")
    args.browser_session_owner = owner
    args.flaresolverr_url = endpoint
    args.capacity_control_fd = None
    return args


def _dataset_statuses(datasets: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(name): str(dataset.status.value)
        for name, dataset in datasets.items()
    }


def _cache_capacity_phase(
    *,
    seed: Mapping[str, Any],
    seed_uri: str,
    catalog_season: CatalogSeason,
    repository: InMemoryBenchmarkRepository,
) -> dict[str, Any]:
    """Parse and idempotently commit the exact representative cache corpus."""

    scope = catalog_season.scope
    game_id = int(seed["game_id"])
    player_id = int(seed["player_id"])
    game = "Alpha FC - Beta FC"
    seed_sha256 = EXPECTED_CACHE_SEED_SHA256

    stage_rows: list[dict[str, Any]] = []
    stage_ids: list[int] = []
    for raw_stage in seed["stages"]:
        if not isinstance(raw_stage, Mapping):
            raise BenchmarkFailure("cache seed stage is not an object")
        stage_id = int(raw_stage["stage_id"])
        stage_ids.append(stage_id)
        parsed_stage = parse_team_stage_statistics(
            raw_stage["payload"],
            scope=scope,
            stage_id=stage_id,
            source_season_id=int(raw_stage["source_season_id"]),
            source_category="summaryteam",
            source_subcategory="all",
        )
        if parsed_stage.status is not DatasetStatus.AVAILABLE:
            raise BenchmarkFailure(f"seed stage {stage_id} did not parse as available")
        for row_index, row in enumerate(parsed_stage.rows):
            stage_rows.append(
                {
                    **dict(row),
                    "entity_key": hashlib.sha256(
                        (
                            f"{stage_id}\0{row_index}\0"
                            f"{row.get('source_path')}"
                        ).encode("utf-8")
                    ).hexdigest(),
                }
            )
    if len(set(stage_ids)) != 2:
        raise BenchmarkFailure("cache seed did not exercise two distinct stages")

    schedule_rows = tuple(
        {
            "game_id": game_id + offset,
            "stage_id": stage_id,
            "status": 6,
            "home_score": 2,
            "away_score": 1,
            "has_preview": offset == 0,
            "match_is_opta": True,
            "home_team": "Alpha FC",
            "away_team": "Beta FC",
            "game": game,
            "date": f"2026-06-{11 + offset:02d}T19:00:00Z",
        }
        for offset, stage_id in enumerate(stage_ids)
    )
    feed_states = {
        key: "not_available" for key in _expected_stage_feed_keys(stage_ids)
    }
    for stage_id in stage_ids:
        available_key = f"{stage_id}:team:summaryteam:all"
        if available_key not in feed_states:
            raise BenchmarkFailure("stage feed contract omitted representative feed")
        feed_states[available_key] = "available"
    scope_commit = {
        "league": scope.competition_id,
        "season": scope.season_id,
        "entity_group": CACHE_CAPACITY_MODE,
        "datasets": {
            "whoscored_schedule": schedule_rows,
            "whoscored_team_stage_stats": tuple(stage_rows),
        },
        "distinct_keys": {
            "whoscored_schedule": "game_id",
            "whoscored_team_stage_stats": "entity_key",
        },
        "payload_sha256": seed_sha256,
        "raw_uris": (seed_uri,),
        "feed_states": feed_states,
    }
    repository.commit_scope_bundle(**scope_commit)

    match = parse_matchcentre_data(
        seed["match"], scope=scope, game_id=game_id, game=game
    )
    if any(
        match.datasets[name].status is not DatasetStatus.AVAILABLE
        for name in (
            "matches",
            "events",
            "lineups",
            "team_match_stats",
            "player_match_stats",
        )
    ):
        raise BenchmarkFailure("cache seed match coverage is incomplete")
    match_commit = MatchCommit(
        game_id=game_id,
        league=scope.competition_id,
        season=scope.season_id,
        game=game,
        payload_sha256=seed_sha256,
        raw_uri=seed_uri,
        events=match.events.rows,
        lineups=match.lineups.rows,
        lineups_available=True,
        transport_mode="raw_cache",
        proxy_mode="none",
        direct_bytes=0,
        paid_bytes=0,
        parser_version=match.parser_version,
        datasets={
            name: dataset.rows
            for name, dataset in match.datasets.items()
            if name not in {"events", "lineups"}
        },
        dataset_statuses=_dataset_statuses(match.datasets),
        schema_fingerprint=match.matches.rows[0]["source_schema_fingerprint"],
        is_opta=True,
        schedule_status=6,
    )
    repository.commit_matches((match_commit,))

    preview = parse_preview_bundle(
        str(seed["preview_html"]),
        scope=scope,
        game_id=game_id,
        game=game,
        home_team="Alpha FC",
        away_team="Beta FC",
    )
    if any(
        dataset.status is not DatasetStatus.AVAILABLE
        for dataset in preview.datasets.values()
    ):
        raise BenchmarkFailure("cache seed preview coverage is incomplete")
    preview_commit = PreviewCommit(
        game_id=game_id,
        league=scope.competition_id,
        season=scope.season_id,
        game=game,
        payload_sha256=seed_sha256,
        raw_uri=seed_uri,
        missing_players=preview.missing_players.rows,
        transport_mode="raw_cache",
        proxy_mode="none",
        direct_bytes=0,
        paid_bytes=0,
        parser_version=preview.parser_version,
        datasets={
            name: dataset.rows
            for name, dataset in preview.datasets.items()
            if name != "missing_players"
        },
        dataset_statuses=_dataset_statuses(preview.datasets),
    )
    repository.commit_previews((preview_commit,))

    profile = parse_profile_bundle(str(seed["profile_html"]), player_id=player_id)
    if any(
        dataset.status is not DatasetStatus.AVAILABLE
        for dataset in profile.datasets.values()
    ):
        raise BenchmarkFailure("cache seed profile coverage is incomplete")
    profile_commit = ProfileCommit(
        player_id=player_id,
        profile=profile.profiles.rows[0],
        payload_sha256=seed_sha256,
        raw_uri=seed_uri,
        transport_mode="raw_cache",
        proxy_mode="none",
        direct_bytes=0,
        paid_bytes=0,
        parser_version=profile.parser_version,
        participations=profile.participations.rows,
        participations_status=profile.participations.status.value,
    )
    repository.commit_profiles((profile_commit,))

    # A second pass through every repository commit proves deterministic batch
    # identity and exercises the replay/idempotency path without source I/O.
    repository.commit_scope_bundle(**scope_commit)
    repository.commit_matches((match_commit,))
    repository.commit_previews((preview_commit,))
    repository.commit_profiles((profile_commit,))
    metrics = repository.metrics_snapshot()
    if int(metrics.get("failure_records", 0)) != 0:
        raise BenchmarkFailure("cache replay recorded repository failures")
    if sum((metrics.get("idempotent_rows") or {}).values()) <= 0:
        raise BenchmarkFailure("cache replay did not prove idempotent commits")

    return {
        "name": "cache_replay",
        "status": "success",
        "results": [
            {"entity": "matches", "dataset_statuses": _dataset_statuses(match.datasets)},
            {
                "entity": "previews",
                "dataset_statuses": _dataset_statuses(preview.datasets),
            },
            {
                "entity": "profiles",
                "dataset_statuses": _dataset_statuses(profile.datasets),
            },
            {
                "entity": "multistage",
                "metadata": {
                    "source_stage_count": len(stage_ids),
                    "source_stage_ids": sorted(stage_ids),
                    "feed_states_per_stage": 68,
                },
            },
        ],
        "committed_rows": {
            "accepted_total": sum((metrics.get("accepted_rows") or {}).values()),
            "idempotent_total": sum(
                (metrics.get("idempotent_rows") or {}).values()
            ),
            "logical_current_total": sum(
                (metrics.get("logical_current_rows") or {}).values()
            ),
            "failure_records": int(metrics.get("failure_records", 0)),
        },
        "traffic": {
            "cache_work_units_attempted": _CACHE_SOURCE_UNITS,
            "successful_page_units": _CACHE_SOURCE_UNITS,
            "source_request_attempts": 0,
            "network_requests": 0,
            "paid_proxy_bytes": 0,
            "paid_route_requests": 0,
        },
    }


def _run_cache_capacity(
    args: argparse.Namespace,
    *,
    dependencies: BenchmarkFactories,
) -> tuple[int, dict[str, Any]]:
    started = time.monotonic()
    cleanup: dict[str, Any] = {
        "status": "failed",
        "temporary_workspace_removed": False,
    }
    guard = _NoNetworkGuard()
    report: dict[str, Any] = {
        "benchmark_version": BENCHMARK_VERSION,
        "schema_version": CACHE_CAPACITY_SCHEMA_VERSION,
        "mode": CACHE_CAPACITY_MODE,
        "status": "running",
        "scope": str(args.scope),
        "seed_sha256": EXPECTED_CACHE_SEED_SHA256,
        "publishes": False,
        "writes_bronze": False,
        "executes_ddl": False,
        "paid_route_configured": False,
        "transport_policy": "content-addressed raw cache; network forbidden",
        "raw_store": {
            "kind": "content_addressed_temporary_local",
            "retained": False,
        },
        "stage_statistics_contract": {
            "expected_feed_states_per_stage": 68,
        },
        "phases": [],
    }
    workspace_path: Optional[Path] = None
    try:
        with guard:
            seed_path = getattr(args, "cache_seed_file", None)
            seed = _load_cache_capacity_seed(
                Path(seed_path) if seed_path is not None else None
            )
            raw_catalog_path = str(args.catalog)
            if _SEALED_BUNDLE_PATH:
                if re.fullmatch(r"/proc/self/fd/[0-9]+", raw_catalog_path) is None:
                    raise ValueError("sealed capacity catalog must be fd-backed")
                catalog_path = Path(raw_catalog_path)
            else:
                catalog_path = Path(raw_catalog_path).resolve()
            catalog = dependencies.load_catalog(catalog_path)
            catalog_season = catalog.parse_scope_spec(str(args.scope))
            competition = catalog.competition(catalog_season.scope.competition_id)
            if not competition.whoscored_enabled:
                raise ValueError(
                    f"WhoScored is not enabled for {catalog_season.scope.spec}"
                )
            with TemporaryDirectory(prefix="whoscored-cache-capacity-") as raw_root:
                workspace_path = Path(raw_root).resolve()
                seed_object = workspace_path / f"{EXPECTED_CACHE_SEED_SHA256}.json"
                seed_object.write_bytes(_EMBEDDED_CACHE_SEED_BYTES)
                seed_object.chmod(0o400)
                repository = dependencies.create_repository(catalog_season)
                phase = _cache_capacity_phase(
                    seed=seed,
                    seed_uri=seed_object.as_uri(),
                    catalog_season=catalog_season,
                    repository=repository,
                )
                report["phases"].append(phase)
        cleanup = {
            "status": "success" if not workspace_path.exists() else "failed",
            "temporary_workspace_removed": not workspace_path.exists(),
        }
        if guard.attempts:
            raise BenchmarkFailure("cache replay observed a network attempt")
        if cleanup["status"] != "success":
            raise BenchmarkFailure("cache replay workspace cleanup failed")
        report["status"] = "success"
    except ValueError as exc:
        report["status"] = "configuration_error"
        report["error"] = f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if workspace_path is not None and not workspace_path.exists():
            cleanup = {
                "status": "success",
                "temporary_workspace_removed": True,
            }
        report["cleanup"] = cleanup
        report["network_requests"] = guard.attempts
        report["paid_proxy_bytes"] = 0
        report["paid_route_requests"] = 0
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
    code = 0 if report["status"] == "success" else (
        2 if report["status"] == "configuration_error" else 1
    )
    return code, json.loads(json.dumps(report, sort_keys=True, default=str))


def run(
    args: argparse.Namespace,
    *,
    factories: Optional[BenchmarkFactories] = None,
) -> tuple[int, dict[str, Any]]:
    """Execute the benchmark and return an exit code plus JSON-safe report."""
    if _WHOSCORED_RUNTIME_CONTRACT is not None:
        _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
            operation="WhoScored capacity workflow"
        )
    validation_error = _validate_args(args)
    if validation_error:
        return 2, {
            "benchmark_version": BENCHMARK_VERSION,
            "status": "configuration_error",
            "error": validation_error,
            "publishes": False,
        }

    dependencies = factories or _default_factories()
    if getattr(args, "mode", DIRECT_DIAGNOSTIC_MODE) == CACHE_CAPACITY_MODE:
        return _run_cache_capacity(args, dependencies=dependencies)
    try:
        raw_catalog_path = str(args.catalog)
        if _SEALED_BUNDLE_PATH:
            if re.fullmatch(r"/proc/self/fd/[0-9]+", raw_catalog_path) is None:
                raise ValueError("sealed capacity catalog must be fd-backed")
            catalog_path = Path(raw_catalog_path)
        else:
            catalog_path = Path(raw_catalog_path).resolve()
        catalog = dependencies.load_catalog(catalog_path)
        catalog_season = catalog.parse_scope_spec(str(args.scope))
        competition = catalog.competition(catalog_season.scope.competition_id)
        if not competition.whoscored_enabled:
            raise ValueError(
                f"WhoScored is not enabled for {catalog_season.scope.spec}"
            )
        if competition.region_id is None or competition.tournament_id is None:
            raise ValueError(f"{catalog_season.scope.spec} lacks WhoScored source ids")
    except Exception as exc:
        return 2, {
            "benchmark_version": BENCHMARK_VERSION,
            "status": "configuration_error",
            "error": f"{type(exc).__name__}: {exc}",
            "scope": str(args.scope),
            "publishes": False,
        }

    report: dict[str, Any] = {
        "benchmark_version": BENCHMARK_VERSION,
        "schema_version": CACHE_CAPACITY_SCHEMA_VERSION,
        "mode": DIRECT_DIAGNOSTIC_MODE,
        "status": "running",
        "scope": catalog_season.scope.spec,
        "match_limit": int(args.match_limit),
        "preview_limit": int(args.match_limit),
        "profile_limit": int(args.profile_limit),
        "publishes": False,
        "writes_bronze": False,
        "executes_ddl": False,
        "raw_store": {"kind": "temporary_local", "retained": False},
        "transport_policy": "raw_cache -> direct_http -> direct_flaresolverr",
        "structured_transport": {
            "max_urls_per_browser_batch": MAX_XHR_BATCH_URLS,
            "fixed_browser_concurrency": STRUCTURED_REQUEST_BURST_SIZE,
            "default_requests_per_minute_per_task": (
                DEFAULT_STRUCTURED_REQUESTS_PER_MINUTE
            ),
            "hard_max_requests_per_minute_per_task": (
                MAX_STRUCTURED_REQUESTS_PER_MINUTE
            ),
            "rate_token_grain": "one_per_source_url",
        },
        "paid_route_configured": False,
        "stage_statistics_contract": {
            "team_query_defaults": {
                "page": "",
                "numberOfTeamsToPick": "",
                "incPens": "",
                "against": "",
            },
            "team_xg_query_filters": {
                "sortAscending": "false",
                "incPens": "true",
                "against": "false",
            },
            "team_zero_paging_sentinel": (
                "nonempty_rows_zero_totals_and_sizes_current_page_zero_or_one"
            ),
            "team_tabs": [
                {
                    "category": category,
                    "subcategory": subcategory,
                    "sort_by": sort_by,
                    "sort_ascending": sort_ascending,
                    "inc_pens": inc_pens,
                    "against": against,
                }
                for (
                    category,
                    subcategory,
                    sort_by,
                    sort_ascending,
                    inc_pens,
                    against,
                ) in TEAM_STAGE_STAT_TABS
            ],
            "player_tabs": [
                {
                    "category": category,
                    "subcategory": subcategory,
                    "inc_pens": inc_pens,
                }
                for category, subcategory, inc_pens in PLAYER_STAGE_STAT_TABS
            ],
            "team_detailed_tabs": len(TEAM_DETAILED_STAT_TABS),
            "player_detailed_tabs": len(PLAYER_DETAILED_STAT_TABS),
            "positional_stage_feeds": len(STAGE_TEAM_FEED_CATALOG),
            "expected_feed_states_per_stage": 68,
        },
        "phases": [],
    }

    transport: Any = None
    transport_close_error: Optional[str] = None

    def close_transport() -> None:
        """Close network resources once, before temporary raw files are removed."""

        nonlocal transport, transport_close_error
        current = transport
        if current is None:
            return
        try:
            current.close()
        except Exception as exc:
            transport_close_error = (
                f"transport close failed: {type(exc).__name__}: {exc}"
            )
            transport = None
        except BaseException:
            # A termination signal can arrive inside the first close attempt.
            # The CLI handler is one-shot, so retry synchronously while the
            # ExitStack still owns the temporary directory, then preserve the
            # original process exit.  A failed retry keeps the reference for
            # the outer fallback after ExitStack finishes unwinding.
            try:
                current.close()
            except Exception as exc:
                transport_close_error = (
                    f"transport close failed: {type(exc).__name__}: {exc}"
                )
            except BaseException:
                pass
            else:
                transport = None
            raise
        else:
            transport = None

    started = time.monotonic()
    try:
        with ExitStack() as resources:
            raw_root = resources.enter_context(
                TemporaryDirectory(prefix="whoscored-workflow-bench-")
            )
            raw_store = dependencies.create_raw_store(Path(raw_root).resolve().as_uri())
            ledger = MemoryRequestLedger()
            repository = dependencies.create_repository(catalog_season)
            # Registered after the temporary directory, so LIFO cleanup closes
            # the browser transport first.  This gives the supervisor's fixed
            # SIGTERM grace window to destroy its FlareSolverr session.
            resources.callback(close_transport)
            transport = dependencies.create_transport(
                catalog_season.scope.spec, ledger, args
            )
            service = dependencies.create_service(
                scope=catalog_season,
                catalog=catalog,
                repository=repository,
                transport=transport,
                raw_store=raw_store,
            )

            cold = _execute_phase(
                "cold",
                service=service,
                repository=repository,
                transport=transport,
                ledger=ledger,
                match_limit=int(args.match_limit),
                profile_limit=int(args.profile_limit),
                preview_parser=dependencies.parse_preview,
            )
            report["phases"].append(cold)
            _require_phase_success(cold)
            if int(cold["traffic"]["source_request_attempts"]) <= 0:
                raise BenchmarkFailure("cold phase made no source requests")

            warm = _execute_phase(
                "warm",
                service=service,
                repository=repository,
                transport=transport,
                ledger=ledger,
                match_limit=int(args.match_limit),
                profile_limit=int(args.profile_limit),
                preview_parser=dependencies.parse_preview,
            )
            report["phases"].append(warm)
            _require_phase_success(warm)
            if int(warm["traffic"]["source_request_attempts"]) != 0:
                raise BenchmarkFailure("warm phase escaped raw cache")
            if int(warm["traffic"]["cache_hits"]) <= 0:
                raise BenchmarkFailure("warm phase recorded no raw-cache hits")
            if int(warm["committed_rows"]["new_batch_total"]) != 0:
                raise BenchmarkFailure("warm replay created new logical batches")
            if (
                cold["committed_rows"]["logical_current_by_dataset"]
                != warm["committed_rows"]["logical_current_by_dataset"]
            ):
                raise BenchmarkFailure("warm replay changed logical current row counts")

            match_ids = list(warm["selected_match_ids"])
            if not match_ids:
                raise BenchmarkFailure("there is no match raw target to invalidate")
            target: RawTarget = match_page_target(match_ids[0])
            if not raw_store.has(target):
                raise BenchmarkFailure(
                    f"warm phase did not persist raw target {target.target_id}"
                )
            _, invalidated_record = raw_store.load_bytes(target)
            quarantine_key = raw_store.quarantine(
                target,
                reason="workflow benchmark incremental invalidation",
                record=invalidated_record,
            )
            if not quarantine_key or raw_store.has(target):
                raise BenchmarkFailure(
                    f"could not invalidate exactly one raw target {target.target_id}"
                )
            report["incremental_invalidation"] = {
                "target_id": target.target_id,
                "page_kind": target.page_kind,
                "manifest_quarantined": True,
            }

            incremental = _execute_phase(
                "incremental",
                service=service,
                repository=repository,
                transport=transport,
                ledger=ledger,
                match_limit=int(args.match_limit),
                profile_limit=int(args.profile_limit),
                preview_parser=dependencies.parse_preview,
            )
            report["phases"].append(incremental)
            _require_phase_success(incremental)
            source_targets = set(incremental["traffic"]["source_targets"])
            if source_targets != {target.target_id}:
                raise BenchmarkFailure(
                    "incremental phase fetched unexpected source targets: "
                    f"{sorted(source_targets)}"
                )
            if int(incremental["traffic"]["raw_cache_misses"]) != 1:
                raise BenchmarkFailure(
                    "incremental phase must have exactly one raw-cache miss"
                )
            if (
                cold["selected_match_ids"] != warm["selected_match_ids"]
                or cold["selected_match_ids"] != incremental["selected_match_ids"]
            ):
                raise BenchmarkFailure("match sample changed between phases")
            if (
                cold["selected_profile_ids"] != warm["selected_profile_ids"]
                or cold["selected_profile_ids"] != incremental["selected_profile_ids"]
            ):
                raise BenchmarkFailure("profile sample changed between phases")
            if (
                cold["preview_probe"] != warm["preview_probe"]
                or cold["preview_probe"] != incremental["preview_probe"]
            ):
                raise BenchmarkFailure("preview probe changed between phases")

            total_traffic = dict(transport.get_traffic_stats())
            if int(total_traffic.get("paid_proxy_bytes", 0) or 0) != 0:
                raise BenchmarkFailure("benchmark total includes paid proxy traffic")
            report["traffic_total"] = json.loads(
                json.dumps(total_traffic, sort_keys=True, default=str)
            )
            report["paid_proxy_mb"] = 0.0
            report["status"] = "success"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        # Safe fallback for failures before the ExitStack callback is active.
        close_transport()
        if transport_close_error is not None:
            # Preserve the original fail-closed precedence: an inability to
            # release network resources overrides an earlier workflow error.
            report["status"] = "failed"
            report["error"] = transport_close_error
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)

    # Prove JSON serializability before returning success to the shell.
    try:
        report = json.loads(json.dumps(report, sort_keys=True, default=str))
    except Exception as exc:  # pragma: no cover - defensive serialization guard
        return 1, {
            "benchmark_version": BENCHMARK_VERSION,
            "status": "failed",
            "error": f"report serialization failed: {type(exc).__name__}: {exc}",
            "publishes": False,
        }
    return (0 if report.get("status") == "success" else 1), report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=(CACHE_CAPACITY_MODE, DIRECT_DIAGNOSTIC_MODE),
        default=DIRECT_DIAGNOSTIC_MODE,
    )
    parser.add_argument("--scope", default=DEFAULT_SCOPE)
    parser.add_argument("--match-limit", type=int, default=DEFAULT_MATCH_LIMIT)
    parser.add_argument("--profile-limit", type=int, default=DEFAULT_PROFILE_LIMIT)
    parser.add_argument(
        "--catalog",
        default=str(DEFAULT_COMPETITIONS_PATH),
        help="static catalog used only to resolve canonical source ids",
    )
    parser.add_argument(
        "--flaresolverr-url",
        default=None,
    )
    parser.add_argument(
        "--cache-seed-file",
        type=Path,
        default=None,
        help=(
            "optional exact cache seed override used for integrity testing; "
            "bytes must match the reviewed embedded corpus"
        ),
    )
    parser.add_argument(
        "--browser-session-owner",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--capacity-control-fd",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
    )
    return parser


def _install_cli_termination_handlers() -> dict[int, Any]:
    """Turn host termination into an unwind through ``run`` cleanup."""

    previous: dict[int, Any] = {}
    termination_started = False

    def terminate(signum: int, _frame: Any) -> None:
        nonlocal termination_started
        if termination_started:
            return
        termination_started = True
        raise SystemExit(128 + signum)

    for signum in (signal.SIGTERM, signal.SIGHUP):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, terminate)
    return previous


def _restore_cli_termination_handlers(previous: Mapping[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    args = _parser().parse_args()
    try:
        args = _apply_capacity_control(args)
    except ValueError:
        print(
            json.dumps(
                {
                    "benchmark_version": BENCHMARK_VERSION,
                    "status": "configuration_error",
                    "error": "capacity control is invalid",
                    "publishes": False,
                },
                sort_keys=True,
            )
        )
        return 2
    previous_handlers = _install_cli_termination_handlers()
    try:
        code, report = run(args)
    finally:
        _restore_cli_termination_handlers(previous_handlers)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
