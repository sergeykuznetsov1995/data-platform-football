"""Durable raw-first orchestration for production FBref ingestion.

This module is deliberately independent from Airflow.  DAG tasks call these
bounded methods, while unit tests inject in-memory control/raw/writer objects.
Only :meth:`FBrefPipeline.fetch_wave` is allowed to construct a transport;
every parsing and replay method consumes an immutable raw manifest.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Mapping, Optional, Sequence

from scrapers.fbref.bronze import (
    FBrefGenericBronzeWriter,
    GenericPagePersistItem,
    GenericPersistenceError,
)
from scrapers.fbref.control import (
    BudgetExceeded,
    CompetitionRegistryEntry,
    ControlStore,
    FrontierProvenance,
    FrontierTarget,
    SeasonAlias,
    SeasonRegistryEntry,
    StateConflict,
    make_control_run_id,
    make_logical_refresh_id,
)
from scrapers.fbref.control.models import CohortTarget, ObservationLease
from scrapers.fbref.discovery import (
    DISCOVERY_PARSER_VERSION,
    CalendarType,
    CompetitionFormat,
    CompetitionGender,
    CompetitionRef,
    DiscoveredPageLink,
    ParticipantType,
    SeasonRef,
    competition_eligibility,
    discover_page_links,
    normalize_page_source_ids,
    parse_competition_html,
    parse_competition_index_html,
    parse_schedule_html,
    parse_season_html,
    season_page_is_complete_without_schedule,
    sentinel_coverage,
    url_addresses_archived_edition,
)
from scrapers.fbref.fetcher import (
    FETCHER_VERSION,
    MAX_TARGET_HTTP_ATTEMPTS,
    FBrefFetcher,
    FetchError,
)
from scrapers.fbref.page_document import (
    PAGE_DOCUMENT_VERSION,
    Availability,
    PageDocument,
    parse_page_document,
    response_owns_target_page,
)
from scrapers.fbref.raw_store import (
    PageTarget,
    RawFetchRecord,
    RawPageStore,
    competition_index_target,
    competition_page_target,
    match_page_target,
    schedule_page_target,
    season_page_target,
)
from scrapers.fbref.settings import (
    DEFAULT_BROWSER_BYTE_LIMIT_BYTES,
    DEFAULT_BROWSER_REQUESTS_PER_SOLVE,
    DEFAULT_BYTE_LIMIT,
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_REQUEST_LIMIT,
    DEFAULT_REQUEST_RESERVATION_BYTES,
    DEFAULT_SHARD_SIZE,
    FBREF_PERSISTENT_HTTP_SESSION,
    MAX_CLEARANCE_SOLVE_ATTEMPTS,
    MAX_SHARD_SIZE,
    MIN_DOMAIN_INTERVAL_SECONDS,
    MIB,
    bootstrap_byte_reservation_for,
    bootstrap_reservation_for,
)
from scrapers.fbref.typed_bronze import (
    TYPED_BRONZE_PARSER_VERSION,
    FBrefTypedBronzeAdapter,
    FBrefTypedBronzeWriter,
    TypedMatchPersistItem,
    TypedBronzeError,
    TypedSourceContext,
    parse_match_html as parse_typed_match_html,
    parse_schedule_html as parse_typed_schedule_html,
    parse_season_stats_html as parse_typed_season_stats_html,
    typed_result_requires_persistence,
)
from scrapers.fbref.match_parser import MatchParseResult


SENTINEL_COMPETITIONS = (
    "Premier League",
    "Champions League",
    "Nations League",
    "Africa Cup of Nations",
    "World Cup",
    "European Championship",
    "Copa América",
)

# One Camoufox target may consume four 90s navigations plus four 45s solve
# windows, restart overhead, and a bounded 60s throttle wait. Keep a full-hour
# fence and renew all outstanding sequential leases before every target.
FETCH_LEASE_SECONDS = 60 * 60
PROCESSING_LEASE_SECONDS = 60 * 60
# Above this count, and only when retirements also dominate their wave, a
# source contract rejection stops looking like a few unusable archived pages
# and starts looking like FBref having changed its markup.
MAX_ROUTINE_CONTRACT_QUARANTINES = 5
# Kinds whose retirement loses nothing but the page itself.  A spine page is
# excluded on purpose: match targets exist only because a schedule page was
# parsed (generic discovery drops match links), and a season/competition page
# carries the campaign's own frontier, so retiring one would silently amputate
# the subtree it alone discovers.  ``player`` is excluded too, and for a
# different reason: a player identity is one frontier row for every season and
# both lanes (``_PAGE_SOURCE_ID_KEYS["player"] == ("player_id",)``), so a verdict
# earned in the archive would bury the live row -- and its own legitimate
# table-free shape is already accepted upstream by
# ``_has_verified_zero_table_player_profile``, so nothing is lost by excluding it.
_CONTRACT_ISOLATABLE_PAGE_KINDS = frozenset({"squad"})
_LEGACY_INVALID_MATCHLOG_TARGET_ID = (
    "fbref:matchlog:matchlogs:b201bf4bc9476c3f0cc8"
)
_LEGACY_INVALID_MATCHLOG_URL = (
    "https://fbref.com/en/players//matchlogs/2016-2017/misc/"
    "Yan-Kaye-Match-Logs"
)
_LEGACY_INVALID_MATCHLOG_SOURCE_IDS = {
    "player_id": "matchlogs",
    "matchlog_season_id": "2016-2017",
    "matchlog_discriminator": "2016-2017/misc",
}
REPLAY_SOURCE_REQUEST_LIMIT = DEFAULT_REQUEST_LIMIT
REPLAY_SOURCE_BYTE_LIMIT = DEFAULT_BYTE_LIMIT
ACCEPTANCE_REQUEST_LIMIT = 100
ACCEPTANCE_BYTE_LIMIT = 50 * MIB
ACCEPTANCE_SHARD_SIZE = 25
ACCEPTANCE_EXECUTION_MODE = "acceptance_nonpublishing"

# Statuses Cloudflare returns when it no longer honours a cf_clearance for the
# warm HTTP session. They say nothing about the target page — only that this
# clearance is dead — so the wave re-solves instead of failing every remaining
# target against it.
CLEARANCE_REJECTED_STATUSES = frozenset({401, 403, 429})
# Each consecutive refresh costs one browser solve, so a source that rejects
# fresh clearances outright must still fail the wave rather than launch
# browsers in a loop. A productive warm session resets this streak: later
# expiry is independent transport churn, not evidence that the source rejects
# every fresh clearance.
MAX_CONSECUTIVE_CLEARANCE_REFRESHES = 2
# One production parse batch may consume the accepted 20 seconds per match for
# all 25 targets (500s). Add 100s for scheduler/strict-close variance, and
# finalize before entering that offline gap whenever the 115-minute local
# session deadline is this close.
PERSISTENT_PARSE_GUARD_SECONDS = 10 * 60

logger = logging.getLogger(__name__)


def _bounded_int(
    name: str, *, default: int, lower: int, upper: int
) -> int:
    """Read an integer environment setting and reject unsafe bounds."""

    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not lower <= value <= upper:
        raise ValueError(f"{name} must be between {lower} and {upper}")
    return value


def _normalize_live_batch_count(value: object) -> int:
    """Accept only an integer or a templated decimal integer string."""

    if type(value) is int:
        normalized = value
    elif isinstance(value, str) and value.strip().isdecimal():
        normalized = int(value.strip())
    else:
        raise ValueError("max_batches must be an integer")
    if not 1 <= normalized <= 80:
        raise ValueError("max_batches must be between 1 and 80")
    return normalized


FBREF_BATCH_PERSIST = os.environ.get("FBREF_BATCH_PERSIST", "0") == "1"
FBREF_BATCH_PERSIST_MATCHES = _bounded_int(
    "FBREF_BATCH_PERSIST_MATCHES", default=8, lower=2, upper=25
)
FBREF_BATCH_PERSIST_MAX_CELLS = _bounded_int(
    "FBREF_BATCH_PERSIST_MAX_CELLS",
    default=150000,
    lower=1000,
    upper=500000,
)


@contextmanager
def _captured_exit_stack(errors: list[Exception]):
    """Unwind with the real exception, then defer it to lease fencing."""

    try:
        # The exception must cross ExitStack.__exit__ before it is captured so
        # database guard contexts see the real exc_info and roll back rather
        # than treating a failed second lock entry as a successful body.
        with ExitStack() as stack:
            yield stack
    except Exception as exc:
        errors.append(exc)


class PipelineError(RuntimeError):
    """Base error for a fail-closed FBref pipeline task."""


class FetchWaveError(PipelineError):
    """At least one target in a bounded fetch wave did not complete."""


class ParseWaveError(PipelineError):
    """At least one raw page did not parse, persist, and validate."""


class RunValidationError(PipelineError):
    """Run-level completeness or traffic gates failed."""


class TypedPromotionDeferred(PipelineError):
    """An active target refresh prevents an atomic typed promotion."""


class SourceContractRejected(ParseWaveError):
    """One page's own published shape can never satisfy its parser contract.

    A ``ParseWaveError`` subclass so every caller outside the parse wave keeps
    failing closed.  Inside the wave it is the one failure isolated to its own
    target: retrying the same immutable bytes cannot change the verdict, so the
    target is retired instead of stalling every other page behind it.
    """

    def __init__(
        self, message: str, *, target_id: str, content_hash: str, reason: str
    ) -> None:
        super().__init__(message)
        self.target_id = target_id
        self.content_hash = content_hash
        self.reason = reason


@dataclass(frozen=True)
class PipelineSettings:
    run_type: str = "current"
    request_limit: int = DEFAULT_REQUEST_LIMIT
    byte_limit: int = DEFAULT_BYTE_LIMIT
    shard_size: int = DEFAULT_SHARD_SIZE
    request_reservation_bytes: int = DEFAULT_REQUEST_RESERVATION_BYTES
    domain_interval_seconds: float = DEFAULT_DOMAIN_INTERVAL_SECONDS
    bootstrap_request_reservation: Optional[int] = None
    bootstrap_byte_reservation: Optional[int] = None
    target_request_reservation: int = MAX_TARGET_HTTP_ATTEMPTS
    proxy_file: Optional[str] = None
    persistent_http_session: bool = FBREF_PERSISTENT_HTTP_SESSION

    @classmethod
    def acceptance(
        cls,
        *,
        scope: str = "current",
        proxy_file: Optional[str] = None,
        domain_interval_seconds: float = DEFAULT_DOMAIN_INTERVAL_SECONDS,
    ) -> "PipelineSettings":
        """Build the one supported bounded live-acceptance profile.

        ``scope`` describes product intent rather than exposing arbitrary run
        settings: current acceptance is a current run, while the bounded
        historical sample uses the existing backfill processing semantics.
        """

        normalized_scope = str(scope).strip().casefold()
        run_types = {"current": "current", "history": "backfill"}
        if normalized_scope not in run_types:
            raise ValueError("acceptance scope must be current or history")
        return cls(
            run_type=run_types[normalized_scope],
            request_limit=ACCEPTANCE_REQUEST_LIMIT,
            byte_limit=ACCEPTANCE_BYTE_LIMIT,
            shard_size=ACCEPTANCE_SHARD_SIZE,
            proxy_file=proxy_file,
            domain_interval_seconds=domain_interval_seconds,
        )

    @classmethod
    def acceptance_replay(
        cls,
        *,
        shard_size: int = ACCEPTANCE_SHARD_SIZE,
    ) -> "PipelineSettings":
        """Build the physically zero-network acceptance replay profile."""

        if int(shard_size) != ACCEPTANCE_SHARD_SIZE:
            raise ValueError("acceptance replay shard_size must be exactly 25")
        return cls(
            run_type="replay",
            request_limit=0,
            byte_limit=0,
            shard_size=ACCEPTANCE_SHARD_SIZE,
            # Replay never constructs a transport, but the generic settings
            # invariant intentionally keeps reservations positive.
            bootstrap_request_reservation=1,
            bootstrap_byte_reservation=1,
            persistent_http_session=False,
        )

    def __post_init__(self) -> None:
        if self.bootstrap_request_reservation is None:
            # Derived from the run's own budget, so the fetch wave's subprocess
            # (which rebuilds settings from the command line) spends exactly
            # what this run reserved for its browser.
            object.__setattr__(
                self,
                "bootstrap_request_reservation",
                bootstrap_reservation_for(self.request_limit),
            )
        if self.bootstrap_byte_reservation is None:
            object.__setattr__(
                self,
                "bootstrap_byte_reservation",
                bootstrap_byte_reservation_for(self.request_limit),
            )
        if self.run_type not in {"current", "backfill", "replay"}:
            raise ValueError("run_type must be current, backfill, or replay")
        if self.request_limit < 0 or self.byte_limit < 0:
            raise ValueError("run budgets must be non-negative")
        if not 1 <= self.shard_size <= MAX_SHARD_SIZE:
            raise ValueError("shard_size must be between 1 and 25")
        if self.request_reservation_bytes <= 0:
            raise ValueError("request_reservation_bytes must be positive")
        if self.domain_interval_seconds < MIN_DOMAIN_INTERVAL_SECONDS:
            raise ValueError(
                "domain_interval_seconds must respect the FBref "
                f"{MIN_DOMAIN_INTERVAL_SECONDS:g}-second source minimum"
            )
        if self.bootstrap_request_reservation < 1:
            raise ValueError("bootstrap_request_reservation must be positive")
        if self.bootstrap_byte_reservation < 1:
            raise ValueError("bootstrap_byte_reservation must be positive")
        if self.target_request_reservation != MAX_TARGET_HTTP_ATTEMPTS:
            raise ValueError(
                "target_request_reservation must cover both HTTP attempts"
            )
        if not isinstance(self.persistent_http_session, bool):
            raise ValueError("persistent_http_session must be boolean")
        if self.run_type == "replay" and self.persistent_http_session:
            raise ValueError("Replay cannot enable persistent HTTP metering")


BACKFILL_SEASON_COHORT_RESERVATION_BYTES = 7 * MIB


def backfill_season_cohort_capacity(
    settings: PipelineSettings,
    *,
    request_remaining: Optional[int] = None,
    byte_remaining: Optional[int] = None,
) -> int:
    """Bound historical season roots independently from warm page batches.

    A season root expands into schedules, squads, players, matchlogs, and
    matches.  Its admission contract therefore remains the production-tested
    conservative 7 MiB aggregate allowance instead of pretending that one
    season is one 3 MiB HTTP target.  This preserves deterministic 7/14
    canary/production dry-run cohorts while child pages are still fetched
    sequentially by the warm runner under the real shared budget.
    """

    available = (
        settings.byte_limit
        if byte_remaining is None
        else max(0, int(byte_remaining))
    )
    requests = (
        settings.request_limit
        if request_remaining is None
        else max(0, int(request_remaining))
    )
    request_capacity = max(
        0, requests - settings.bootstrap_request_reservation
    ) // settings.target_request_reservation
    return min(
        settings.shard_size,
        request_capacity,
        available // BACKFILL_SEASON_COHORT_RESERVATION_BYTES,
    )


def wave_target_capacity(
    settings: PipelineSettings,
    *,
    request_remaining: Optional[int] = None,
    byte_remaining: Optional[int] = None,
    bootstrap_required: bool = True,
) -> int:
    """Return the exact cohort that the current request/byte budget can fund."""

    requests = (
        settings.request_limit
        if request_remaining is None
        else max(0, int(request_remaining))
    )
    bytes_available = (
        settings.byte_limit
        if byte_remaining is None
        else max(0, int(byte_remaining))
    )
    bootstrap_requests = (
        settings.bootstrap_request_reservation if bootstrap_required else 0
    )
    bootstrap_bytes = (
        settings.bootstrap_byte_reservation if bootstrap_required else 0
    )
    tail_bytes = (
        settings.request_reservation_bytes
        if bootstrap_required and settings.persistent_http_session
        else 0
    )
    byte_capacity = max(
        0,
        bytes_available - bootstrap_bytes - tail_bytes,
    ) // settings.request_reservation_bytes
    request_capacity = (
        max(0, requests - bootstrap_requests)
        // settings.target_request_reservation
    )
    return min(settings.shard_size, request_capacity, byte_capacity)


def live_wave_target_capacity(
    settings: PipelineSettings,
    *,
    request_remaining: Optional[int] = None,
    byte_remaining: Optional[int] = None,
    bootstrap_required: bool = True,
) -> int:
    """Admit sequential warm-session pages without double-counting bytes.

    A live wave reserves one target at a time and settles that reservation
    before the next target.  The old capacity calculation divided the whole
    run's byte budget by the per-target safety reservation, so a 50 MiB
    canary admitted only seven pages per warm session despite a 25-page shard.
    The byte guard still rejects a target when less than one reservation
    remains; the control store enforces the real cumulative byte limit.
    """

    requests = (
        settings.request_limit
        if request_remaining is None
        else max(0, int(request_remaining))
    )
    bytes_available = (
        settings.byte_limit
        if byte_remaining is None
        else max(0, int(byte_remaining))
    )
    initial_bytes = settings.request_reservation_bytes + (
        settings.bootstrap_byte_reservation if bootstrap_required else 0
    )
    if bootstrap_required and settings.persistent_http_session:
        initial_bytes += settings.request_reservation_bytes
    if bytes_available < initial_bytes:
        return 0
    request_capacity = max(
        0,
        requests
        - (
            settings.bootstrap_request_reservation
            if bootstrap_required
            else 0
        ),
    )
    return min(settings.shard_size, request_capacity)


def affordable_clearance_reservation(
    settings: PipelineSettings,
    *,
    request_remaining: int,
    byte_remaining: int,
) -> Optional[tuple[int, int]]:
    """Return the largest clearance reservation the rest of a run can fund.

    The full bootstrap reservation buys every proxy rotation the transport is
    allowed to try — four solves for a production run.  Demanding all
    four before a mid-run re-solve is what left ~40 % of the request budget
    unspent: a warm-session failure late in a run could not book
    80 + 2, so the wave handed its remaining targets back even though a solve
    measures ~19 requests.  Offer the retry the largest whole number of
    rotations that still fits alongside one target; the browser is then capped
    at exactly what was reserved, so a shrunken allowance still cannot
    overspend the run.  ``None`` means not even a single solve fits, which is
    the same clean budget boundary as before.
    """

    full_requests = int(settings.bootstrap_request_reservation)
    full_bytes = int(settings.bootstrap_byte_reservation)
    rotations = max(1, full_requests // DEFAULT_BROWSER_REQUESTS_PER_SOLVE)
    for count in range(rotations, 0, -1):
        requests = min(
            full_requests, count * DEFAULT_BROWSER_REQUESTS_PER_SOLVE
        )
        bytes_ = min(full_bytes, count * DEFAULT_BROWSER_BYTE_LIMIT_BYTES)
        tail_bytes = (
            settings.request_reservation_bytes
            if settings.persistent_http_session
            else 0
        )
        fits = (
            request_remaining - requests
            >= settings.target_request_reservation
            and byte_remaining - bytes_
            >= settings.request_reservation_bytes + tail_bytes
        )
        if fits:
            return requests, bytes_
    return None


def _uses_production_safety_circuit(settings: "PipelineSettings") -> bool:
    return (
        settings.run_type in {"current", "backfill"}
        and settings.request_limit == DEFAULT_REQUEST_LIMIT
        and settings.byte_limit == DEFAULT_BYTE_LIMIT
    )


@dataclass(frozen=True)
class _FrontierSeedCandidate:
    link: DiscoveredPageLink
    historical: bool
    refresh_policy: Optional[str] = None


# Spine pages whose frontier target is season-independent or shared with the
# live current-season lane.  ``fbref:competition:<id>`` discovered from a 1930
# season page is the very row the daily run must refresh weekly, so a
# historical seed must never relabel it ``historical_once``.  The frontier
# upsert guards only ``player``/``squad`` (control/store.py, sealed), so the
# guard lives here instead.
_HISTORICAL_DOWNGRADE_GUARDED_KINDS = frozenset(
    {
        "competition",
        "competition_index",
        "season",
        "schedule",
        "standings",
        "season_stats",
    }
)

# Mirrors _MAX_FRONTIER_DISCOVERY_TARGETS / _MAX_FRONTIER_DISCOVERY_EDGES in the
# sealed control store: a batch above either ceiling is rejected, not truncated.
_DISCOVERY_TARGET_BATCH_LIMIT = 1000
_DISCOVERY_EDGE_BATCH_LIMIT = 5000


@dataclass
class WaveResult:
    cohort_size: int = 0
    claimed: int = 0
    fetched: int = 0
    recovered_from_raw: int = 0
    parsed: int = 0
    typed_promoted: int = 0
    stale_typed_observations_skipped: int = 0
    seeded: int = 0
    skipped_ineligible: int = 0
    requests: int = 0
    wire_bytes: int = 0
    decoded_html_bytes: int = 0
    browser_document_bytes: int = 0
    browser_asset_bytes: int = 0
    browser_bootstraps: int = 0
    budget_exhausted: bool = False
    requeued_at_budget: int = 0
    requeued_dead_clearance: int = 0
    requeued_session_exhaustion: int = 0
    contract_quarantined: int = 0
    failures: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class _ProcessedObservation:
    """Per-lease outcome which can be aggregated without stopping a cohort."""

    parsed: int = 0
    typed_promoted: int = 0
    stale_typed_observations_skipped: int = 0
    seeded: int = 0
    skipped_ineligible: int = 0
    contract_quarantined: int = 0
    failures: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ClaimedMatchObservation:
    run_id: str
    html: str
    record: RawFetchRecord
    observation_lease: ObservationLease
    page: PageDocument
    typed_match: MatchParseResult
    typed_context: TypedSourceContext
    match_id: str
    stateful_run_id: str
    stateful_run_type: str

    def sequential_args(self, *, generic_persisted: bool = False) -> dict:
        return {
            "run_id": self.run_id,
            "html": self.html,
            "record": self.record,
            "observation_lease": self.observation_lease,
            "page": self.page,
            "typed_match": self.typed_match,
            "typed_context": self.typed_context,
            "match_id": self.match_id,
            "generic_persisted": generic_persisted,
            "stateful_run_id": self.stateful_run_id,
            "stateful_run_type": self.stateful_run_type,
        }


@dataclass(frozen=True)
class _LivePageSettlement:
    provider_billed_bytes: int
    budget_exceeded: bool = False


@dataclass(frozen=True)
class _AcceptanceReplayMatch:
    """Prepared frozen match with no global observation-fence mutation."""

    record: RawFetchRecord
    page: PageDocument
    typed_match: MatchParseResult
    typed_context: TypedSourceContext
    match_id: str


@dataclass
class LiveRunResult:
    batches: int = 0
    frontier_closed: bool = False
    fetch: WaveResult = field(default_factory=WaveResult)
    parse: WaveResult = field(default_factory=WaveResult)

    def as_dict(self) -> dict:
        return {
            "batches": self.batches,
            "frontier_closed": self.frontier_closed,
            "fetch": self.fetch.as_dict(),
            "parse": self.parse.as_dict(),
        }


@dataclass
class _LiveFetchSession:
    stack: ExitStack = field(default_factory=ExitStack)
    fetcher: Optional[object] = None
    session_id: Optional[str] = None
    consecutive_clearance_refreshes: int = 0
    needs_clearance: bool = True
    # What the next solve may spend, once the run's remaining budget can no
    # longer fund the full reservation.  ``None`` means the settings default.
    clearance_requests: Optional[int] = None
    clearance_bytes: Optional[int] = None
    persistent_enabled: bool = False
    state: str = "idle"
    receipt: Optional[object] = None
    tail_settlement: Optional[dict] = None
    tail_reserved: bool = False
    page_budget_latched: bool = False
    finalization_guard: Callable[[], object] = nullcontext

    def clearance_reservation(self, settings) -> tuple[int, int]:
        return (
            settings.bootstrap_request_reservation
            if self.clearance_requests is None
            else self.clearance_requests,
            settings.bootstrap_byte_reservation
            if self.clearance_bytes is None
            else self.clearance_bytes,
        )

    def attach_control_session(self, session_id: str) -> None:
        """Install a newly opened control session before metering can start."""

        if self.state not in {"idle", "control_closed"}:
            raise PipelineError(
                "Previous persistent clearance session is not finalized"
            )
        self.session_id = str(session_id)
        self.state = "control_open"
        self.receipt = None
        self.tail_settlement = None
        self.tail_reserved = False
        self.page_budget_latched = False

    def begin_persistent(
        self,
        control,
        *,
        run_id: str,
        tail_bytes: int,
    ) -> None:
        if not self.persistent_enabled or self.fetcher is None or self.session_id is None:
            raise PipelineError("Persistent clearance session is incomplete")
        if self.state != "control_open":
            raise PipelineError(
                "Persistent control session is not ready for metering"
            )
        begin = getattr(self.fetcher, "begin_metered_session", None)
        if not callable(begin):
            raise PipelineError(
                "FBref persistent fetcher must expose begin_metered_session"
            )
        baseline = int(begin(self.session_id))
        self.state = "meter_started"
        self.receipt = None
        self.tail_settlement = None
        self.tail_reserved = False
        control.reserve_clearance_session_tail(
            run_id,
            self.session_id,
            bytes_reserved=tail_bytes,
            baseline_provider_bytes=baseline,
        )
        self.tail_reserved = True
        self.state = "active"

    def rollover_if_due(
        self, control, *, within_seconds: float = 0.0
    ) -> bool:
        """Finalize one exact session before its paid lease can expire."""

        if (
            not self.persistent_enabled
            or self.fetcher is None
            or self.session_id is None
            or self.state != "active"
        ):
            return False
        due = getattr(
            self.fetcher, "persistent_session_rollover_due", None
        )
        if not callable(due):
            raise PipelineError(
                "FBref persistent fetcher must expose rollover deadline"
            )
        if not bool(due(within_seconds=within_seconds)):
            return False

        # This ordering is the paid-traffic fence: strict provider close,
        # durable tail settlement, and control close all finish before reset
        # can prepare a browser that may ask for the next provider lease.
        self.finalize(control, status="closed")
        reset = getattr(self.fetcher, "reset_clearance", None)
        if not callable(reset):
            raise PipelineError(
                "FBref persistent fetcher must reset after rollover"
            )
        reset()
        return True

    def finalize(self, control, *, status: str) -> None:
        with self.finalization_guard():
            self._finalize(control, status=status)

    def _finalize(self, control, *, status: str) -> None:
        if not self.persistent_enabled:
            try:
                self.stack.close()
            finally:
                self.fetcher = None
                if self.session_id is not None:
                    try:
                        control.close_clearance_session(
                            self.session_id, status=status
                        )
                    finally:
                        self.session_id = None
                self.needs_clearance = True
            return
        if self.session_id is None:
            return
        if self.fetcher is None:
            raise FetchWaveError(
                "hard_transport_policy: persistent fetcher ownership was lost"
            )
        try:
            if self.state == "control_open":
                # Metering never started, so no provider or typed tail exists.
                control.close_clearance_session(
                    self.session_id, status="failed"
                )
                self.state = "control_closed"
                self.needs_clearance = True
                return
            if self.state in {"meter_started", "active"}:
                finalizer = getattr(
                    self.fetcher, "finalize_metered_session", None
                )
                if not callable(finalizer):
                    raise PipelineError(
                        "FBref persistent fetcher must expose finalizer"
                    )
                self.receipt = finalizer()
                self.state = "provider_finalized"
            if self.state == "provider_finalized":
                if self.tail_reserved:
                    self.tail_settlement = control.settle_clearance_session_tail(
                        self.session_id, self.receipt
                    )
                    self.state = "tail_settled"
                else:
                    # begin_metered_session succeeded, but the durable tail
                    # reservation did not. No socket admission was possible;
                    # close the empty control session and let the already
                    # reserved target budget be recovered by abort_run.
                    control.close_clearance_session(
                        self.session_id, status="failed"
                    )
                    self.state = "control_closed"
                    self.needs_clearance = True
                    return
            if self.state == "tail_settled":
                tail_result = self.tail_settlement or {}
                explicit_tail_overrun = bool(
                    tail_result.get("budget_exceeded_by_tail")
                    or tail_result.get("tail_over_reservation")
                )
                terminal = explicit_tail_overrun or bool(
                    tail_result.get("terminal")
                    and not self.page_budget_latched
                )
                control.close_clearance_session(
                    self.session_id,
                    status="failed" if terminal else status,
                )
                self.state = "control_closed"
                if terminal:
                    raise FetchWaveError(
                        "hard_transport_policy: persistent tail exceeded "
                        "the run safety circuit"
                    )
        except Exception as exc:
            if isinstance(exc, FetchWaveError):
                raise
            raise FetchWaveError(
                "hard_transport_policy: persistent session finalizer failed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        self.needs_clearance = True

    def close(self, control, *, status: str) -> None:
        with self.finalization_guard():
            self._close(control, status=status)

    def _close(self, control, *, status: str) -> None:
        finalize_error = None
        try:
            self._finalize(control, status=status)
        except Exception as exc:  # noqa: BLE001 - release only after closure
            finalize_error = exc
        releasable = self.state == "control_closed" or (
            self.state == "idle" and self.session_id is None
        )
        if self.persistent_enabled and releasable:
            try:
                self.stack.close()
            except Exception as exc:  # noqa: BLE001 - retain finalizer error
                finalize_error = finalize_error or exc
            self.fetcher = None
            self.session_id = None
        if finalize_error is not None:
            raise finalize_error


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        rendered = value
    else:
        rendered = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if rendered.tzinfo is None:
        return rendered.replace(tzinfo=timezone.utc)
    return rendered.astimezone(timezone.utc)


def _mapping(value: object) -> dict:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, Mapping):
            return dict(parsed)
    raise ValueError("Expected a mapping")


def _target_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:20]


def _registry_snapshot_id(record: RawFetchRecord) -> str:
    """Return one retry-stable identity for a single raw observation."""

    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            (
                "fbref-registry-snapshot:"
                f"{DISCOVERY_PARSER_VERSION}:"
                f"{record.logical_refresh_id}:{record.target_id}:"
                f"{record.content_hash}"
            ),
        )
    )


def _session_failure(exc: FetchError) -> bool:
    """True when the failure belongs to the clearance, not the target page."""

    return exc.error_class in {
        "clearance_failed",
        "clearance_export_failed",
        "raw_contract_cloudflare_challenge",
    } or (
        exc.error_class == "http_status"
        and exc.http_status in CLEARANCE_REJECTED_STATUSES
    ) or exc.error_class.startswith("warm_session_")


def _sentinel_gate_errors(coverage: object) -> list[str]:
    """Validate expected published men's competitions without seeding scope."""

    source = coverage if isinstance(coverage, Mapping) else {}
    missing: list[str] = []
    ineligible: list[str] = []
    for name in SENTINEL_COMPETITIONS:
        item = source.get(name)
        if not isinstance(item, Mapping) or not bool(item.get("published")):
            missing.append(name)
            continue
        if str(item.get("eligibility") or "") != "eligible":
            ineligible.append(name)

    errors = []
    if missing:
        errors.append("sentinel_coverage_missing=" + ",".join(missing))
    if ineligible:
        errors.append("sentinel_coverage_ineligible=" + ",".join(ineligible))
    return errors


def _require_acceptance_settings(settings: PipelineSettings) -> None:
    if settings.run_type not in {"current", "backfill"}:
        raise ValueError("acceptance run_type must be current or backfill")
    if (
        settings.request_limit != ACCEPTANCE_REQUEST_LIMIT
        or settings.byte_limit != ACCEPTANCE_BYTE_LIMIT
        or settings.shard_size != ACCEPTANCE_SHARD_SIZE
    ):
        raise ValueError(
            "acceptance profile must be exactly 100 requests / 50 MiB / "
            "shard 25"
        )


def _acceptance_run_profile_error(
    run: object,
    *,
    expected_status: str,
) -> Optional[str]:
    if not isinstance(run, Mapping):
        return "run_not_found"
    status = str(run.get("status") or "unknown").casefold()
    if status != expected_status:
        return f"run_status={status}"
    run_type = str(run.get("run_type") or "unknown").casefold()
    if run_type not in {"current", "backfill"}:
        return f"run_type={run_type}"
    try:
        request_limit = int(run.get("request_limit"))
        byte_limit = int(run.get("byte_limit"))
    except (TypeError, ValueError):
        return "profile_missing"
    if (
        request_limit != ACCEPTANCE_REQUEST_LIMIT
        or byte_limit != ACCEPTANCE_BYTE_LIMIT
    ):
        return "profile_not_100_requests_50_mib"
    metadata = run.get("metadata")
    if not isinstance(metadata, Mapping):
        return "metadata_missing"
    expected_scope = "current" if run_type == "current" else "history"
    if (
        metadata.get("acceptance_profile") is not True
        or str(metadata.get("execution_mode") or "").casefold()
        != ACCEPTANCE_EXECUTION_MODE
        or metadata.get("publication_eligible") is not False
        or metadata.get("bootstrap_only") is not False
        or str(metadata.get("acceptance_scope") or "").casefold()
        != expected_scope
        or int(metadata.get("shard_size") or 0) != ACCEPTANCE_SHARD_SIZE
    ):
        return "metadata_not_nonpublishing_acceptance"
    return None


def _accepted_raw_audit_error(run_id: object, run: Mapping) -> Optional[str]:
    metadata = run.get("metadata")
    if not isinstance(metadata, Mapping):
        return "raw_audit_missing"
    raw_audit = metadata.get("raw_audit")
    if not isinstance(raw_audit, Mapping):
        return "raw_audit_missing"
    try:
        successful = int(raw_audit.get("successful_attempt_count"))
        audited = int(raw_audit.get("audited_attempt_count"))
    except (TypeError, ValueError):
        return "raw_audit_not_accepted"
    if (
        raw_audit.get("schema_version") != "fbref-raw-audit-anchor-v1"
        or str(raw_audit.get("status") or "").casefold() != "passed"
        or str(raw_audit.get("run_type") or "").casefold()
        != str(run.get("run_type") or "").casefold()
        or raw_audit.get("zero_delta_required") is not False
        or successful <= 0
        or audited != successful
        or str(raw_audit.get("audited_control_run_id") or "") != str(run_id)
        or str(raw_audit.get("processing_control_run_id") or "")
        != str(run_id)
    ):
        return "raw_audit_not_accepted"
    return None


def _accepted_replay_raw_audit_error(
    processing_run_id: object,
    source_run_id: object,
    run: Mapping,
) -> Optional[str]:
    metadata = run.get("metadata")
    raw_audit = metadata.get("raw_audit") if isinstance(metadata, Mapping) else None
    if not isinstance(raw_audit, Mapping):
        return "acceptance_replay_raw_audit_missing"
    try:
        successful = int(raw_audit.get("successful_attempt_count"))
        audited = int(raw_audit.get("audited_attempt_count"))
    except (TypeError, ValueError):
        return "acceptance_replay_raw_audit_not_accepted"
    if (
        raw_audit.get("schema_version") != "fbref-raw-audit-anchor-v1"
        or str(raw_audit.get("status") or "").casefold() != "passed"
        or str(raw_audit.get("run_type") or "").casefold() != "replay"
        or raw_audit.get("zero_delta_required") is not True
        or successful <= 0
        or audited != successful
        or str(raw_audit.get("audited_control_run_id") or "")
        != str(source_run_id)
        or str(raw_audit.get("processing_control_run_id") or "")
        != str(processing_run_id)
    ):
        return "acceptance_replay_raw_audit_not_accepted"
    return None


def _acceptance_summary_errors(summary: object) -> list[str]:
    """Return only immutable, run-owned strict acceptance gate failures."""

    if not isinstance(summary, Mapping):
        return ["summary_missing"]
    errors: list[str] = []
    status = str(summary.get("status") or "unknown").casefold()
    if status not in {"running", "succeeded"}:
        errors.append(f"run_status={status}")
    profile_error = _acceptance_run_profile_error(
        summary, expected_status=status
    )
    # The summary is normally still running during validation and succeeded
    # when it is inspected as a replay source.  The profile helper's caller
    # supplies that same state so only profile/metadata are checked here.
    if profile_error is not None:
        errors.append(profile_error)
    target_counts = summary.get("target_counts")
    if not isinstance(target_counts, Mapping):
        errors.append("target_counts_missing")
    else:
        total_targets = sum(int(value or 0) for value in target_counts.values())
        if total_targets <= 0:
            errors.append("cohort_empty")
        nonsuccess = {
            str(status): int(count or 0)
            for status, count in target_counts.items()
            if status != "succeeded" and int(count or 0) > 0
        }
        if nonsuccess:
            errors.append(f"cohort_targets_not_succeeded={nonsuccess}")
    dataset_counts = summary.get("dataset_validation_counts")
    if not isinstance(dataset_counts, Mapping):
        errors.append("dataset_validation_counts_missing")
    elif any(
        int(count or 0) > 0
        for status, count in dataset_counts.items()
        if status not in {"succeeded", "skipped"}
    ):
        errors.append("dataset_validation_failed")
    if int(summary.get("unvalidated_target_count") or 0) != 0:
        errors.append(
            "unvalidated_target_count="
            f"{int(summary.get('unvalidated_target_count') or 0)}"
        )
    if int(summary.get("unprocessed_raw_count") or 0) != 0:
        errors.append(
            "unprocessed_raw_count="
            f"{int(summary.get('unprocessed_raw_count') or 0)}"
        )
    if bool(summary.get("budget_exceeded")):
        errors.append("budget_exceeded=true")
    if int(summary.get("requests_reserved") or 0) != 0:
        errors.append(
            "requests_reserved="
            f"{int(summary.get('requests_reserved') or 0)}"
        )
    if int(summary.get("bytes_reserved") or 0) != 0:
        errors.append(
            "bytes_reserved=" f"{int(summary.get('bytes_reserved') or 0)}"
        )
    try:
        if int(summary.get("requests_used") or 0) > ACCEPTANCE_REQUEST_LIMIT:
            errors.append("request_limit_exceeded")
        if int(summary.get("bytes_used") or 0) > ACCEPTANCE_BYTE_LIMIT:
            errors.append("byte_limit_exceeded")
    except (TypeError, ValueError):
        errors.append("budget_counters_invalid")
    traffic = summary.get("traffic_totals")
    if not isinstance(traffic, Mapping):
        errors.append("traffic_totals_missing")
    else:
        attempts = int(traffic.get("network_attempts") or 0)
        successes = int(traffic.get("warm_http_successes") or 0)
        success_rate = traffic.get("warm_http_success_rate")
        if attempts > 0 and (
            successes != attempts
            or success_rate is None
            or float(success_rate) != 1.0
        ):
            errors.append(
                f"warm_http_successes={successes}!={attempts};"
                f"rate={success_rate!r}"
            )
        if int(traffic.get("unclassified_failures") or 0) != 0:
            errors.append(
                "unclassified_failures="
                f"{int(traffic.get('unclassified_failures') or 0)}"
            )
        if int(traffic.get("duplicate_fetch_violations") or 0) != 0:
            errors.append(
                "duplicate_fetch_violations="
                f"{int(traffic.get('duplicate_fetch_violations') or 0)}"
            )
    availability = summary.get("table_availability")
    if not isinstance(availability, Mapping):
        errors.append("table_availability_missing")
    else:
        unsafe = {
            state: int(availability.get(state) or 0)
            for state in ("unknown", "error")
            if int(availability.get(state) or 0) > 0
        }
        if unsafe:
            errors.append(f"unsafe_table_availability={unsafe}")
    return errors


def _acceptance_coverage_errors(
    cohort_anchor: Mapping[str, object], evidence: object
) -> list[str]:
    """Re-prove frozen slots from the just-fetched immutable observations."""

    if not isinstance(evidence, Mapping):
        return ["acceptance_evidence_missing"]
    targets = evidence.get("targets")
    datasets = evidence.get("datasets")
    if not isinstance(targets, Sequence) or not isinstance(datasets, Sequence):
        return ["acceptance_evidence_incomplete"]
    expected_ids = [str(item) for item in cohort_anchor.get("target_ids") or ()]
    actual_ids = [str(item.get("target_id") or "") for item in targets]
    errors: list[str] = []
    if actual_ids != expected_ids:
        errors.append("acceptance_target_order_mismatch")
    target_by_id = {
        str(item.get("target_id") or ""): item
        for item in targets
        if isinstance(item, Mapping)
    }
    invalid_targets = [
        target_id
        for target_id, item in target_by_id.items()
        if str(item.get("status") or "").casefold() != "succeeded"
        or int(item.get("http_status") or 0) not in {200, 304}
        or not item.get("raw_manifest_key")
        or not item.get("content_hash")
    ]
    if invalid_targets:
        errors.append(
            "acceptance_target_evidence_invalid=" + ",".join(invalid_targets)
        )
    if not datasets:
        errors.append("acceptance_dataset_evidence_empty")
    unsafe_datasets = []
    for item in datasets:
        if not isinstance(item, Mapping):
            unsafe_datasets.append("non_mapping")
            continue
        if str(item.get("dataset") or "").startswith("table:"):
            # Raw per-table inventory is diagnostic, not the typed bronze
            # contract (#949).  Typed completeness is enforced by the per-target
            # __page__/typed:* manifest checks below.  Skip inventory here so an
            # unclassified auxiliary table (availability='unknown' + reason)
            # cannot fail the acceptance dataset gate.
            continue
        availability = str(item.get("availability") or "").casefold()
        statuses = {
            str(item.get(name) or "").casefold()
            for name in (
                "parse_status",
                "persistence_status",
                "validation_status",
            )
        }
        if (
            availability in {"unknown", "error"}
            or statuses - {"succeeded", "skipped"}
            or (
                availability in {"empty", "restricted", "not_applicable"}
                and not str(item.get("empty_reason") or "").strip()
            )
        ):
            unsafe_datasets.append(
                f"{item.get('target_id')}:{item.get('dataset')}"
            )
    if unsafe_datasets:
        errors.append(
            "acceptance_dataset_evidence_invalid="
            + ",".join(unsafe_datasets[:25])
        )
    manifests_by_target: dict[str, dict[str, Mapping[str, object]]] = {}
    for item in datasets:
        if not isinstance(item, Mapping):
            continue
        target_key = str(item.get("target_id") or "")
        dataset_key = str(item.get("dataset") or "")
        if target_key and dataset_key:
            manifests_by_target.setdefault(target_key, {})[dataset_key] = item

    def _manifest_ok(
        target_id: str, dataset_name: str, *, allow_skipped: bool = False
    ) -> bool:
        item = manifests_by_target.get(target_id, {}).get(dataset_name)
        if item is None:
            return False
        allowed = {"succeeded"}
        if allow_skipped:
            allowed.add("skipped")
        return (
            str(item.get("parse_status") or "").casefold() == "succeeded"
            and str(item.get("persistence_status") or "").casefold()
            in allowed
            and str(item.get("validation_status") or "").casefold()
            in allowed
        )

    missing_target_manifests: list[str] = []
    for target in targets:
        if not isinstance(target, Mapping):
            continue
        target_id = str(target.get("target_id") or "")
        page_kind = str(target.get("page_kind") or "").casefold()
        if not _manifest_ok(target_id, "__page__"):
            missing_target_manifests.append(f"{target_id}:__page__")
            continue
        required_typed: set[str] = set()
        if page_kind == "schedule":
            required_typed = {"typed:schedule"}
        elif page_kind == "season":
            required_typed = {"typed:player_stats", "typed:team_stats"}
        elif page_kind == "season_stats":
            route = str(
                (target.get("source_ids") or {}).get("stat_route") or ""
            ).casefold()
            required_typed = {
                "standard": {"typed:player_stats", "typed:team_stats"},
                "shooting": {
                    "typed:player_shooting", "typed:team_shooting"
                },
                "playingtime": {
                    "typed:player_playingtime", "typed:team_playingtime"
                },
                "misc": {"typed:player_misc", "typed:team_misc"},
                "keepers": {"typed:keeper_keeper"},
            }.get(route, set())
        elif page_kind == "match":
            required_typed = {
                "typed:shot_events", "typed:match_events", "typed:lineups",
                "typed:match_team_stats", "typed:match_managers",
                "typed:match_officials", "typed:match_keeper_stats",
                "typed:match_player_stats", "typed:__complete__",
            }
        for dataset_name in sorted(required_typed):
            if not _manifest_ok(
                target_id,
                dataset_name,
                allow_skipped=dataset_name != "typed:__complete__",
            ):
                missing_target_manifests.append(f"{target_id}:{dataset_name}")
    if missing_target_manifests:
        errors.append(
            "acceptance_target_manifests_missing="
            + ",".join(missing_target_manifests[:25])
        )
    datasets_by_target: dict[str, list[Mapping[str, object]]] = {}
    unexpected_dataset_targets = set()
    for item in datasets:
        if not isinstance(item, Mapping):
            continue
        target_id = str(item.get("target_id") or "")
        if target_id not in target_by_id:
            unexpected_dataset_targets.add(target_id or "<missing>")
            continue
        datasets_by_target.setdefault(target_id, []).append(item)
    if unexpected_dataset_targets:
        errors.append(
            "acceptance_dataset_target_mismatch="
            + ",".join(sorted(unexpected_dataset_targets))
        )

    # A successful observation is proved per target, not merely by having at
    # least one manifest somewhere in the run.  ``__page__`` is written only
    # after generic persistence and validation complete.  Typed page kinds
    # additionally require the final completion marker, so a stale-observation
    # marker or a partial set of typed datasets cannot satisfy acceptance.
    typed_page_kinds = {"schedule", "season", "season_stats", "match"}
    missing_page_completions = []
    missing_typed_completions = []
    for target_id in expected_ids:
        target = target_by_id.get(target_id)
        target_datasets = datasets_by_target.get(target_id, [])
        page_completions = [
            item
            for item in target_datasets
            if str(item.get("dataset") or "") == "__page__"
        ]
        if len(page_completions) != 1 or any(
            str(page_completions[0].get(name) or "").casefold()
            != "succeeded"
            for name in (
                "parse_status",
                "persistence_status",
                "validation_status",
            )
        ):
            missing_page_completions.append(target_id)
        page_kind = (
            str(target.get("page_kind") or "").casefold()
            if isinstance(target, Mapping)
            else ""
        )
        if page_kind not in typed_page_kinds:
            continue
        typed_completions = [
            item
            for item in target_datasets
            if str(item.get("dataset") or "") == "typed:__complete__"
        ]
        if (
            len(typed_completions) != 1
            or str(
                typed_completions[0].get("availability") or ""
            ).casefold()
            != "available"
            or any(
                str(typed_completions[0].get(name) or "").casefold()
                != "succeeded"
                for name in (
                    "parse_status",
                    "persistence_status",
                    "validation_status",
                )
            )
        ):
            missing_typed_completions.append(target_id)
    if missing_page_completions:
        errors.append(
            "acceptance_page_completion_missing="
            + ",".join(missing_page_completions)
        )
    if missing_typed_completions:
        errors.append(
            "acceptance_typed_completion_missing="
            + ",".join(missing_typed_completions)
        )
    slots = cohort_anchor.get("coverage_slots")
    if not isinstance(slots, Mapping):
        errors.append("acceptance_coverage_slots_missing")
        return errors
    for slot, target_id_value in slots.items():
        slot_name = str(slot)
        target_id = str(target_id_value)
        target = target_by_id.get(target_id)
        if target is None:
            errors.append(f"acceptance_slot_target_missing={slot_name}")
            continue
        page_kind = str(target.get("page_kind") or "").casefold()
        source_ids = target.get("source_ids")
        source_ids = source_ids if isinstance(source_ids, Mapping) else {}
        expected_class = {
            "player_populated": "populated_player",
            "player_empty": "empty_player",
            "match_full": "full_match",
            "match_sparse": "sparse_match",
        }.get(slot_name)
        if expected_class is not None:
            if str(target.get("evidence_class") or "") != expected_class:
                errors.append(f"acceptance_slot_reclassified={slot_name}")
        elif slot_name.startswith("season_stats_"):
            route = slot_name.removeprefix("season_stats_")
            if page_kind != "season_stats" or (
                str(source_ids.get("stat_route") or "").casefold() != route
            ):
                errors.append(f"acceptance_slot_route_mismatch={slot_name}")
        elif page_kind != slot_name:
            errors.append(f"acceptance_slot_page_kind_mismatch={slot_name}")
    actual_page_kinds = {
        str(item.get("page_kind") or "").casefold() for item in targets
    }
    required_page_kinds = {
        str(item).casefold()
        for item in cohort_anchor.get("required_page_kinds") or ()
    }
    missing_kinds = sorted(required_page_kinds - actual_page_kinds)
    if missing_kinds:
        errors.append("acceptance_page_kinds_missing=" + ",".join(missing_kinds))
    actual_routes = {
        str((item.get("source_ids") or {}).get("stat_route") or "").casefold()
        for item in targets
        if isinstance(item.get("source_ids"), Mapping)
        and str(item.get("page_kind") or "").casefold() == "season_stats"
    }
    required_routes = {
        str(item).casefold()
        for item in cohort_anchor.get("required_routes") or ()
    }
    missing_routes = sorted(required_routes - actual_routes)
    if missing_routes:
        errors.append("acceptance_routes_missing=" + ",".join(missing_routes))
    return errors


def _bronze_acceptance_marker(
    run_id: str,
    summary: Mapping[str, object],
    *,
    cohort_anchor: Mapping[str, object],
) -> dict:
    traffic = summary.get("traffic_totals")
    traffic = traffic if isinstance(traffic, Mapping) else {}
    return {
        "schema_version": "fbref-bronze-acceptance-v1",
        "status": "passed",
        "processing_control_run_id": str(run_id),
        "scope": str(cohort_anchor["scope"]),
        "cohort_size": int(cohort_anchor["cohort_size"]),
        "cohort_sha256": str(cohort_anchor["cohort_sha256"]),
        "page_kind_counts": dict(summary.get("cohort_page_kind_counts") or {}),
        "route_counts": dict(summary.get("cohort_route_counts") or {}),
        "strict_gates": {
            "all_cohort_targets_succeeded": True,
            "network_attempts": int(traffic.get("network_attempts") or 0),
            "warm_http_successes": int(
                traffic.get("warm_http_successes") or 0
            ),
            "warm_http_success_rate": traffic.get("warm_http_success_rate"),
            "unclassified_failures": int(
                traffic.get("unclassified_failures") or 0
            ),
            "duplicate_fetch_violations": int(
                traffic.get("duplicate_fetch_violations") or 0
            ),
            "budget_exceeded": bool(summary.get("budget_exceeded")),
            "requests_used": int(summary.get("requests_used") or 0),
            "bytes_used": int(summary.get("bytes_used") or 0),
            "requests_reserved": int(
                summary.get("requests_reserved") or 0
            ),
            "bytes_reserved": int(summary.get("bytes_reserved") or 0),
            "unvalidated_target_count": int(
                summary.get("unvalidated_target_count") or 0
            ),
            "unprocessed_raw_count": int(
                summary.get("unprocessed_raw_count") or 0
            ),
            "table_availability": dict(
                summary.get("table_availability") or {}
            ),
        },
    }


def _bronze_acceptance_replay_marker(
    run_id: str,
    summary: Mapping[str, object],
    *,
    source_run_id: str,
    source_marker: Mapping[str, object],
) -> dict:
    traffic = summary.get("traffic_totals")
    traffic = traffic if isinstance(traffic, Mapping) else {}
    metadata = summary.get("metadata")
    raw_audit = (
        metadata.get("raw_audit") if isinstance(metadata, Mapping) else {}
    )
    raw_audit = raw_audit if isinstance(raw_audit, Mapping) else {}
    pipeline_metrics = (
        metadata.get("pipeline_run_metrics")
        if isinstance(metadata, Mapping)
        else {}
    )
    pipeline_metrics = (
        pipeline_metrics if isinstance(pipeline_metrics, Mapping) else {}
    )
    return {
        "schema_version": "fbref-bronze-acceptance-replay-v1",
        "status": "passed",
        "processing_control_run_id": str(run_id),
        "source_control_run_id": str(source_run_id),
        "scope": str(source_marker["scope"]),
        "cohort_size": int(source_marker["cohort_size"]),
        "cohort_sha256": str(source_marker["cohort_sha256"]),
        "page_kind_counts": dict(source_marker.get("page_kind_counts") or {}),
        "route_counts": dict(source_marker.get("route_counts") or {}),
        "strict_gates": {
            "source_acceptance_status": "passed",
            "network_attempts": int(traffic.get("network_attempts") or 0),
            "requests_used": int(summary.get("requests_used") or 0),
            "bytes_used": int(summary.get("bytes_used") or 0),
            "request_limit": int(summary.get("request_limit") or 0),
            "byte_limit": int(summary.get("byte_limit") or 0),
            "replay_candidates_remaining": 0,
            "raw_audit_status": raw_audit.get("status"),
            "raw_zero_delta_required": raw_audit.get("zero_delta_required"),
            "raw_audited_attempt_count": int(
                raw_audit.get("audited_attempt_count") or 0
            ),
            "raw_audit_artifact_sha256": raw_audit.get("artifact_sha256"),
            "pipeline_metrics_artifact_sha256": pipeline_metrics.get(
                "artifact_sha256"
            ),
        },
    }


def page_target_from_link(link: DiscoveredPageLink) -> PageTarget:
    """Build a stable target from an exact source-provided canonical URL."""

    source_ids = normalize_page_source_ids(
        link.page_kind, link.source_ids
    )
    if link.page_kind == "match":
        base = match_page_target(link.canonical_url)
        return PageTarget(
            source=base.source,
            page_kind=base.page_kind,
            target_id=base.target_id,
            canonical_url=base.canonical_url,
            source_ids={**source_ids, **base.source_ids},
        )
    if link.page_kind == "competition":
        competition_id = source_ids.get("competition_id")
        if competition_id:
            return competition_page_target(competition_id, link.canonical_url)
    if link.page_kind == "season":
        competition_id = source_ids.get("competition_id")
        season_id = source_ids.get("season_id")
        if competition_id and season_id:
            return season_page_target(
                competition_id, season_id, link.canonical_url
            )
    if link.page_kind == "schedule":
        competition_id = source_ids.get("competition_id")
        season_id = source_ids.get("season_id")
        if competition_id and season_id:
            return schedule_page_target(
                competition_id, season_id, link.canonical_url
            )

    stable_components: list[str] = []
    if link.page_kind == "player" and source_ids.get("player_id"):
        stable_components = [source_ids["player_id"]]
    elif (
        link.page_kind == "matchlog"
        and source_ids.get("player_id")
        and source_ids.get("matchlog_discriminator")
    ):
        stable_components = [
            source_ids["player_id"],
            _target_hash(source_ids["matchlog_discriminator"]),
        ]
    elif link.page_kind == "squad" and source_ids.get("squad_id"):
        discriminator = source_ids.get("squad_discriminator") or _target_hash(
            link.canonical_url
        )
        source_ids["squad_discriminator"] = discriminator
        stable_components = [source_ids["squad_id"], discriminator]
    elif link.page_kind == "season_stats":
        stable_components = [
            source_ids.get("competition_id", "unknown"),
            source_ids.get("season_id", "unknown"),
            source_ids.get("stat_route", _target_hash(link.canonical_url)),
        ]

    # Prefer source entity/route IDs; hash the canonical URL only when the
    # source exposes no sufficient discriminator.
    return PageTarget(
        source="fbref",
        page_kind=link.page_kind,
        target_id=(
            f"fbref:{link.page_kind}:"
            + ":".join(stable_components or [_target_hash(link.canonical_url)])
        ),
        canonical_url=link.canonical_url,
        source_ids=source_ids,
    )


def _is_mass_contract_rejection(result: "WaveResult") -> bool:
    """Tell a few unusable archived pages from the source changing shape."""

    retired = result.contract_quarantined
    return (
        retired > MAX_ROUTINE_CONTRACT_QUARANTINES
        and retired * 2 > result.cohort_size
    )


def _frontier_policy(page_kind: str, *, historical: bool) -> tuple[str, int]:
    if historical:
        return "historical_once", 10
    policies = {
        "competition_index": ("daily", 100),
        "competition": ("weekly", 90),
        "season": ("daily", 80),
        "season_stats": ("daily", 75),
        "schedule": ("six_hourly", 85),
        "standings": ("daily", 70),
        "squad": ("weekly", 60),
        "player": ("monthly", 40),
        "matchlog": ("monthly", 20),
        "match": ("daily", 65),
    }
    return policies.get(page_kind, ("weekly", 30))


def frontier_target(
    target: PageTarget,
    *,
    historical: bool = False,
) -> FrontierTarget:
    policy, priority = _frontier_policy(
        target.page_kind, historical=historical
    )
    return FrontierTarget(
        target_id=target.target_id,
        page_kind=target.page_kind,
        canonical_url=target.canonical_url,
        source_ids=target.source_ids,
        refresh_policy=policy,
        priority=priority,
    )


def _next_fetch_at(page_kind: str, *, historical: bool) -> Optional[datetime]:
    if historical:
        return None
    delays = {
        # Small cadence slack keeps a 06:00 scheduled run due even when the
        # previous task finished a few minutes after its nominal slot.
        "competition_index": timedelta(hours=23),
        "competition": timedelta(days=6, hours=23),
        "schedule": timedelta(hours=5),
        "season": timedelta(hours=23),
        "season_stats": timedelta(hours=23),
        "standings": timedelta(hours=23),
        "squad": timedelta(days=6, hours=23),
        "player": timedelta(days=29, hours=23),
        "matchlog": timedelta(days=29, hours=23),
        "match": timedelta(hours=23),
    }
    return _utcnow() + delays.get(page_kind, timedelta(days=7))


def _competition_metadata(item: CompetitionRef) -> dict:
    return {
        "format": item.format.value,
        "participants": item.participants.value,
        "source_section": item.source_section,
        "country": item.country,
        "governing_body": item.governing_body,
        "tier": item.tier,
        "first_season": item.first_season,
        "last_season": item.last_season,
    }


def _registry_entry(item: CompetitionRef) -> CompetitionRegistryEntry:
    gender = {
        CompetitionGender.MALE: "male",
        CompetitionGender.FEMALE: "female",
        CompetitionGender.UNKNOWN: "unknown",
    }[item.gender]
    return CompetitionRegistryEntry(
        competition_id=item.competition_id,
        canonical_url=item.history_url,
        name=item.name,
        gender=gender,
        classification=f"{item.format.value}:{item.participants.value}",
        metadata=_competition_metadata(item),
    )


def _competition_from_registry(row: Mapping[str, object]) -> CompetitionRef:
    metadata = _mapping(row.get("metadata") or {})
    classification = str(row.get("classification") or "other:unknown")
    format_name, _, participants_name = classification.partition(":")
    gender_name = str(row.get("gender") or "unknown").lower()
    return CompetitionRef(
        comp_id=str(row["competition_id"]),
        name=str(row.get("name") or row["competition_id"]),
        format=CompetitionFormat(
            format_name if format_name in {item.value for item in CompetitionFormat}
            else CompetitionFormat.OTHER.value
        ),
        participants=ParticipantType(
            participants_name
            if participants_name in {item.value for item in ParticipantType}
            else ParticipantType.UNKNOWN.value
        ),
        gender={
            "male": CompetitionGender.MALE,
            "female": CompetitionGender.FEMALE,
        }.get(gender_name, CompetitionGender.UNKNOWN),
        source_section=str(metadata.get("source_section") or "Unknown"),
        country=metadata.get("country"),
        governing_body=metadata.get("governing_body"),
        tier=metadata.get("tier"),
        first_season=metadata.get("first_season"),
        last_season=metadata.get("last_season"),
        history_url=str(row["canonical_url"]),
    )


class FBrefPipeline:
    """Bounded orchestration around a durable control and raw store."""

    def __init__(
        self,
        control: ControlStore,
        raw_store: RawPageStore,
        *,
        generic_writer=None,
        typed_adapter=None,
        fetcher_factory: Optional[Callable[..., object]] = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = _utcnow,
        finalization_guard: Callable[[], object] = nullcontext,
    ) -> None:
        self.control = control
        self.raw_store = raw_store
        self.generic_writer = generic_writer or FBrefGenericBronzeWriter()
        self.typed_adapter = typed_adapter or FBrefTypedBronzeAdapter()
        self.fetcher_factory = fetcher_factory or (
            lambda proxy_file, max_browser_requests, max_browser_bytes: FBrefFetcher(
                proxy_file=proxy_file,
                max_browser_requests=max_browser_requests,
                max_browser_bytes=max_browser_bytes,
            )
        )
        self.sleep = sleep
        self.clock = clock
        self.finalization_guard = finalization_guard
        # Instance fields make the default-off rollout and bounded cohort
        # policy directly inspectable and overridable in deterministic tests.
        self.batch_persist_enabled = FBREF_BATCH_PERSIST
        self.batch_persist_matches = FBREF_BATCH_PERSIST_MATCHES
        self.batch_persist_max_cells = FBREF_BATCH_PERSIST_MAX_CELLS

    @classmethod
    def from_env(cls) -> "FBrefPipeline":
        from scrapers.base.trino_manager import TrinoTableManager

        manager = TrinoTableManager()
        return cls(
            ControlStore.from_env(),
            RawPageStore.from_env(optional=False),
            generic_writer=FBrefGenericBronzeWriter(manager),
            typed_adapter=FBrefTypedBronzeAdapter(
                FBrefTypedBronzeWriter(manager)
            ),
        )

    def _assert_persistent_profile(
        self, run_id: str, settings: PipelineSettings
    ) -> None:
        run = self.control.get_run(run_id)
        if run is None:
            raise PipelineError(f"Unknown control run {run_id}")
        metadata = run.get("metadata") or {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        if not isinstance(metadata, Mapping):
            raise PipelineError("FBref control run metadata is invalid")
        marker = metadata.get("persistent_http_session")
        if marker is None:
            if settings.persistent_http_session:
                raise PipelineError(
                    "Existing run has no persistent HTTP profile marker"
                )
            return
        if not isinstance(marker, bool) or marker != settings.persistent_http_session:
            raise PipelineError(
                "Stored persistent HTTP profile differs from this worker"
            )

    def initialize_run(
        self,
        *,
        airflow_run_id: object,
        dag_id: object,
        settings: PipelineSettings,
        execution_metadata: Optional[Mapping[str, object]] = None,
    ) -> str:
        self.control.migrate()
        # A worker that dies mid-wave (OOM, kill, hung browser) leaves fenced
        # leases behind.  claim_targets only reaps its own run's leases, so
        # without a global reap here those targets stay 'leased' forever: they
        # drop out of the crawl and keep promotion_pending_match_count above
        # zero, which fails every later run's validation.
        reaped = self.control.reap_expired_leases()
        if reaped:
            logger.warning(
                "Reaped %d expired FBref lease(s) left by earlier runs", reaped
            )
        run_id = make_control_run_id(airflow_run_id, dag_id=dag_id)
        base_metadata = {
            "airflow_run_id": str(airflow_run_id),
            "dag_id": str(dag_id),
            "shard_size": settings.shard_size,
            "request_reservation_bytes": (
                settings.request_reservation_bytes
            ),
            "target_request_reservation": (
                settings.target_request_reservation
            ),
            "persistent_http_session": settings.persistent_http_session,
        }
        extra_metadata = dict(execution_metadata or {})
        reserved = sorted(set(base_metadata) & set(extra_metadata))
        if reserved:
            raise ValueError(
                "FBref execution metadata cannot replace reserved keys: "
                + ", ".join(reserved)
            )
        self.control.create_run(
            settings.run_type,
            run_id=run_id,
            request_limit=settings.request_limit,
            byte_limit=settings.byte_limit,
            metadata={**base_metadata, **extra_metadata},
        )
        self.control.start_run(run_id)
        installed = self.control.get_run(run_id)
        installed_metadata = (
            installed.get("metadata") if isinstance(installed, Mapping) else None
        )
        if isinstance(installed_metadata, str):
            installed_metadata = json.loads(installed_metadata)
        marker = (
            installed_metadata.get("persistent_http_session")
            if isinstance(installed_metadata, Mapping)
            else None
        )
        if marker is None:
            if settings.persistent_http_session:
                raise PipelineError(
                    "Existing run has no persistent HTTP profile marker"
                )
        elif marker is not settings.persistent_http_session:
            raise PipelineError(
                "Stored persistent HTTP profile differs from this worker"
            )
        return run_id

    def initialize_acceptance_run(
        self,
        *,
        airflow_run_id: object,
        dag_id: object,
        settings: PipelineSettings,
        execution_metadata: Optional[Mapping[str, object]] = None,
    ) -> str:
        """Initialize an explicitly nonpublishing bounded acceptance run."""

        _require_acceptance_settings(settings)
        supplied = dict(execution_metadata or {})
        protected = {
            "execution_mode",
            "acceptance_profile",
            "acceptance_scope",
            "bootstrap_only",
            "publication_eligible",
        }
        collisions = sorted(protected & set(supplied))
        if collisions:
            raise ValueError(
                "FBref acceptance metadata cannot replace protected keys: "
                + ", ".join(collisions)
            )
        scope = "current" if settings.run_type == "current" else "history"
        return self.initialize_run(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            settings=settings,
            execution_metadata={
                **supplied,
                "execution_mode": ACCEPTANCE_EXECUTION_MODE,
                "acceptance_profile": True,
                "acceptance_scope": scope,
                "bootstrap_only": False,
                "publication_eligible": False,
            },
        )

    def initialize_acceptance_replay_run(
        self,
        *,
        airflow_run_id: object,
        dag_id: object,
        source_control_run_id: object,
        settings: PipelineSettings,
        execution_metadata: Optional[Mapping[str, object]] = None,
    ) -> str:
        """Initialize a zero-budget replay of one passed acceptance source."""

        if (
            settings.run_type != "replay"
            or settings.request_limit != 0
            or settings.byte_limit != 0
            or settings.shard_size != ACCEPTANCE_SHARD_SIZE
        ):
            raise ValueError(
                "acceptance replay profile must be exactly 0 requests / "
                "0 bytes / shard 25"
            )
        source_id = str(uuid.UUID(str(source_control_run_id).strip()))
        source_error = self._acceptance_replay_source_error(source_id)
        if source_error is not None:
            raise PipelineError(source_error)
        supplied = dict(execution_metadata or {})
        protected = {
            "execution_mode",
            "acceptance_replay",
            "acceptance_replay_source_run_id",
            "bootstrap_only",
            "publication_eligible",
        }
        collisions = sorted(protected & set(supplied))
        if collisions:
            raise ValueError(
                "FBref acceptance replay metadata cannot replace protected "
                "keys: " + ", ".join(collisions)
            )
        return self.initialize_run(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            settings=settings,
            execution_metadata={
                **supplied,
                "execution_mode": "acceptance_replay_nonpublishing",
                "acceptance_replay": True,
                "acceptance_replay_source_run_id": source_id,
                "bootstrap_only": False,
                "publication_eligible": False,
            },
        )

    def seed_acceptance_cohort(
        self,
        run_id: str,
        target_ids: Sequence[object],
        *,
        settings: PipelineSettings,
        required_page_kinds: Sequence[str],
        required_routes: Sequence[str] = (),
        coverage_slots: Optional[Mapping[str, object]] = None,
    ) -> dict:
        """Freeze one operator-selected acceptance cohort in exact order."""

        _require_acceptance_settings(settings)
        run = self.control.get_run(run_id)
        error = _acceptance_run_profile_error(run, expected_status="running")
        if error is not None:
            raise PipelineError(error)
        expected_scope = (
            "current" if settings.run_type == "current" else "history"
        )
        metadata = run.get("metadata") if isinstance(run, Mapping) else None
        if not isinstance(metadata, Mapping) or (
            str(metadata.get("acceptance_scope") or "").casefold()
            != expected_scope
        ):
            raise PipelineError("acceptance_run_scope_mismatch")
        requested_ids = [str(item).strip() for item in target_ids]
        required_kinds = [str(item).strip() for item in required_page_kinds]
        required_route_names = [str(item).strip() for item in required_routes]
        slots = {
            str(slot).strip(): str(target).strip()
            for slot, target in dict(coverage_slots or {}).items()
        }
        if not required_kinds or any(not item for item in required_kinds):
            raise ValueError("required_page_kinds must not be empty")
        if not slots or set(slots.values()) != set(requested_ids):
            raise ValueError("coverage_slots must cover the exact target_ids")
        cohort = self.control.create_explicit_run_cohort(run_id, target_ids)
        ordered_ids = [item.target_id for item in cohort]
        encoded = json.dumps(
            ordered_ids, ensure_ascii=True, separators=(",", ":")
        ).encode("ascii")
        result = {
            "cohort_size": len(ordered_ids),
            "target_ids": ordered_ids,
            "cohort_sha256": hashlib.sha256(encoded).hexdigest(),
        }
        anchored = self.control.record_acceptance_cohort(
            run_id,
            {
                "schema_version": "fbref-acceptance-cohort-v1",
                "status": "frozen",
                "scope": expected_scope,
                **result,
                "required_page_kinds": required_kinds,
                "required_routes": required_route_names,
                "coverage_slots": slots,
            },
        )
        return {**result, "acceptance_cohort": anchored}

    def seed_competition_index(self) -> str:
        target = competition_index_target()
        self.control.upsert_frontier_target(frontier_target(target))
        return target.target_id

    def _replay_source_error(self, source_run_id: Optional[str]) -> Optional[str]:
        """Require one fully accepted production source run for replay."""

        if not source_run_id:
            return "replay_source_run_id_missing"
        try:
            source_run = self.control.get_run(source_run_id)
        except (TypeError, ValueError):
            return "replay_source_run_id_invalid"
        if source_run is None:
            return f"replay_source_run_not_found={source_run_id}"
        run_type = str(source_run.get("run_type") or "unknown").lower()
        if run_type not in {"current", "backfill"}:
            return f"replay_source_run_type_forbidden={run_type}"
        status = str(source_run.get("status") or "unknown").lower()
        if status != "succeeded":
            return f"replay_source_run_not_succeeded={status}"
        try:
            source_request_limit = int(source_run.get("request_limit"))
            source_byte_limit = int(source_run.get("byte_limit"))
        except (TypeError, ValueError):
            return "replay_source_run_not_production_profile"
        if source_request_limit != REPLAY_SOURCE_REQUEST_LIMIT or (
            source_byte_limit != REPLAY_SOURCE_BYTE_LIMIT
        ):
            return "replay_source_run_not_production_profile"
        metadata = source_run.get("metadata")
        if not isinstance(metadata, Mapping):
            return "replay_source_raw_audit_missing"
        execution_mode = str(
            metadata.get("execution_mode") or ""
        ).casefold()
        explicitly_ineligible = (
            "publication_eligible" in metadata
            and metadata.get("publication_eligible") is not True
        )
        if (
            explicitly_ineligible
            or metadata.get("bootstrap_only") is True
            or execution_mode in {
                "bootstrap_only",
                "canary_nonpublishing",
            }
        ):
            return "replay_source_run_not_publication_eligible"
        raw_audit = metadata.get("raw_audit")
        if not isinstance(raw_audit, Mapping):
            return "replay_source_raw_audit_missing"
        try:
            successful_attempt_count = int(
                raw_audit.get("successful_attempt_count")
            )
            audited_attempt_count = int(raw_audit.get("audited_attempt_count"))
        except (TypeError, ValueError):
            return "replay_source_raw_audit_not_accepted"
        if (
            str(raw_audit.get("schema_version") or "")
            != "fbref-raw-audit-anchor-v1"
            or str(raw_audit.get("status") or "").casefold() != "passed"
            or str(raw_audit.get("run_type") or "").casefold() != run_type
            or raw_audit.get("zero_delta_required") is not False
            or successful_attempt_count <= 0
            or audited_attempt_count <= 0
            or str(raw_audit.get("audited_control_run_id") or "")
            != str(source_run_id)
            or str(raw_audit.get("processing_control_run_id") or "")
            != str(source_run_id)
        ):
            return "replay_source_raw_audit_not_accepted"
        return None

    def _acceptance_replay_source_error(
        self, source_run_id: Optional[str]
    ) -> Optional[str]:
        """Require a passed bounded nonpublishing acceptance source.

        This deliberately does not relax :meth:`_replay_source_error`; callers
        must opt into the separate acceptance replay path end to end.
        """

        if not source_run_id:
            return "acceptance_replay_source_run_id_missing"
        try:
            source_run = self.control.get_run(source_run_id)
        except (TypeError, ValueError):
            return "acceptance_replay_source_run_id_invalid"
        profile_error = _acceptance_run_profile_error(
            source_run, expected_status="succeeded"
        )
        if profile_error is not None:
            return f"acceptance_replay_source_{profile_error}"
        raw_error = _accepted_raw_audit_error(source_run_id, source_run)
        if raw_error is not None:
            return f"acceptance_replay_source_{raw_error}"
        metadata = source_run.get("metadata")
        marker = (
            metadata.get("bronze_acceptance")
            if isinstance(metadata, Mapping)
            else None
        )
        cohort = (
            metadata.get("acceptance_cohort")
            if isinstance(metadata, Mapping)
            else None
        )
        if not isinstance(marker, Mapping) or not isinstance(cohort, Mapping):
            return "acceptance_replay_source_strict_evidence_missing"
        if (
            marker.get("schema_version") != "fbref-bronze-acceptance-v1"
            or str(marker.get("status") or "").casefold() != "passed"
            or str(marker.get("processing_control_run_id") or "")
            != str(source_run_id)
            or int(marker.get("cohort_size") or 0) <= 0
            or str(marker.get("cohort_sha256") or "")
            != str(cohort.get("cohort_sha256") or "")
            or marker.get("strict_gates") is None
        ):
            return "acceptance_replay_source_strict_evidence_invalid"
        return None

    def seed_historical_seasons(
        self,
        *,
        run_id: str,
        settings: PipelineSettings,
        limit: int = MAX_SHARD_SIZE,
    ) -> dict:
        """Seed the next bounded, unfinished historical registry cohort."""

        if settings.run_type != "backfill":
            raise ValueError("Historical season seeding requires backfill mode")
        run = self.control.get_run(run_id)
        if run is None:
            raise PipelineError(f"Unknown control run {run_id}")
        request_remaining = max(
            0,
            int(run["request_limit"])
            - int(run.get("requests_used") or 0)
            - int(run.get("requests_reserved") or 0),
        )
        byte_remaining = max(
            0,
            int(run["byte_limit"])
            - int(run.get("bytes_used") or 0)
            - int(run.get("bytes_reserved") or 0),
        )
        safe_limit = min(
            int(limit),
            backfill_season_cohort_capacity(
                settings,
                request_remaining=request_remaining,
                byte_remaining=byte_remaining,
            ),
        )
        rows = (
            []
            if safe_limit <= 0
            else self.control.list_backfill_seasons(limit=safe_limit)
        )
        cohort = []
        for ordinal, row in enumerate(rows):
            target = season_page_target(
                row["competition_id"],
                row["season_id"],
                row["canonical_url"],
            )
            base = frontier_target(target, historical=True)
            self.control.upsert_frontier_target(
                FrontierTarget(
                    target_id=base.target_id,
                    page_kind=base.page_kind,
                    canonical_url=base.canonical_url,
                    source_ids=base.source_ids,
                    refresh_policy=base.refresh_policy,
                    priority=base.priority,
                    next_fetch_at=_as_utc(self.clock()),
                )
            )
            cohort.append(
                CohortTarget(
                    target_id=target.target_id,
                    logical_refresh_id=make_logical_refresh_id(
                        run_id, target.target_id
                    ),
                    ordinal=ordinal,
                )
            )
        if cohort:
            # Historical selection may adopt a season parked only because it
            # was outside the current-season crawl.  The upserts above change
            # those selected targets to historical_once; the existing scope
            # reconciler can now reopen only its own resolved quarantines
            # before create_run_cohort enforces crawlable membership.
            self._reconcile_frontier_scope()
        self.control.create_run_cohort(run_id, cohort)
        return {"seeded": len(rows), "auto_resume": True}

    def _remaining_wave_limit(
        self,
        run_id: str,
        settings: PipelineSettings,
        *,
        bootstrap_required: bool = True,
    ) -> int:
        run = self.control.get_run(run_id)
        if run is None:
            raise PipelineError(f"Unknown control run {run_id}")
        request_remaining = max(
            0,
            int(run["request_limit"])
            - int(run.get("requests_used") or 0)
            - int(run.get("requests_reserved") or 0),
        )
        byte_remaining = max(
            0,
            int(run["byte_limit"])
            - int(run.get("bytes_used") or 0)
            - int(run.get("bytes_reserved") or 0),
        )
        capacity = (
            live_wave_target_capacity
            if settings.run_type == "current"
            else wave_target_capacity
        )
        return capacity(
            settings,
            request_remaining=request_remaining,
            byte_remaining=byte_remaining,
            bootstrap_required=bootstrap_required,
        )

    def _wait_for_slot(self, scheduled_at: datetime) -> None:
        wait_seconds = max(
            0.0, (_as_utc(scheduled_at) - _as_utc(self.clock())).total_seconds()
        )
        if wait_seconds > 60:
            raise PipelineError(
                f"Domain throttle returned an unsafe {wait_seconds:.1f}s wait"
            )
        if wait_seconds:
            self.sleep(wait_seconds)

    @staticmethod
    def _page_target_for_lease(lease) -> PageTarget:
        return PageTarget(
            source="fbref",
            page_kind=lease.page_kind,
            target_id=lease.target_id,
            canonical_url=lease.canonical_url,
            source_ids=dict(lease.source_ids),
        )

    def _complete_from_record(
        self,
        lease,
        record: RawFetchRecord,
        *,
        historical: bool,
    ) -> None:
        frontier = self.control.get_frontier_target(lease.target_id) or {}
        one_shot = frontier.get("refresh_policy") in {
            "historical_once",
            "current_completed_once",
        }
        self.control.complete_fetch(
            lease,
            http_status=record.http_status,
            content_hash=record.content_hash,
            raw_manifest_key=self.raw_store.fetch_manifest_key(
                record.logical_refresh_id
            ),
            decoded_bytes=record.decoded_bytes,
            compressed_bytes=record.encoded_bytes,
            wire_bytes=int(record.wire_bytes or 0),
            provider_billed_bytes=record.provider_billed_bytes,
            http_request_count=record.http_requests,
            http_status_history=record.http_status_history,
            etag=record.etag,
            last_modified=record.last_modified,
            transport_version=record.transport_version,
            session_version=record.session_version,
            latency_ms=record.latency_ms,
            recovered_from_attempt_id=(
                record.attempt_id
                if record.attempt_id and record.attempt_id != lease.attempt_id
                else None
            ),
            next_fetch_at=_next_fetch_at(
                lease.page_kind, historical=(historical or one_shot)
            ),
        )

    def _settle_live_page_evidence(
        self,
        *,
        settings: PipelineSettings,
        live_session: _LiveFetchSession,
        lease,
        reservation,
        response,
        compressed_raw_bytes: int,
    ) -> _LivePageSettlement:
        provider_value = getattr(response, "provider_billed_bytes", None)
        http_wire_bytes = max(
            0,
            int(
                getattr(
                    response,
                    "http_wire_bytes",
                    getattr(response, "wire_bytes", 0),
                )
            ),
        )
        requests_used = int(response.http_requests) + int(
            response.browser_requests
        )
        if settings.persistent_http_session:
            if live_session.session_id is None or provider_value is None:
                raise FetchWaveError(
                    "hard_transport_policy: persistent page has no exact "
                    "provider counter"
                )
            settled = self.control.settle_clearance_session_page(
                live_session.session_id,
                reservation.reservation_id,
                attempt_id=lease.attempt_id,
                requests_used=requests_used,
                provider_billed_bytes=int(provider_value),
                browser_bootstrap_attempts=(
                    response.browser_bootstrap_attempts
                ),
                browser_bootstrap_requests=response.browser_requests,
                browser_document_bytes=response.browser_document_bytes,
                browser_asset_bytes=response.browser_asset_bytes,
                browser_unobserved_bytes=response.browser_unobserved_bytes,
                http_requests=response.http_requests,
                http_wire_bytes=http_wire_bytes,
                decoded_html_bytes=max(
                    0, int(getattr(response, "decoded_html_bytes", 0))
                ),
                compressed_raw_bytes=max(0, int(compressed_raw_bytes)),
            )
            budget_exceeded = bool(settled.get("budget_exceeded"))
            if budget_exceeded:
                live_session.page_budget_latched = True
            return _LivePageSettlement(
                provider_billed_bytes=int(provider_value),
                budget_exceeded=budget_exceeded,
            )

        billed = (
            int(provider_value)
            if provider_value is not None
            else http_wire_bytes
            + max(0, int(response.browser_document_bytes))
            + max(0, int(response.browser_asset_bytes))
            + max(0, int(response.browser_unobserved_bytes))
        )
        self.control.settle_budget(
            reservation.reservation_id,
            requests_used=requests_used,
            bytes_used=billed,
        )
        if live_session.session_id is not None:
            self.control.record_session_metrics(
                live_session.session_id,
                browser_bootstrap_requests=response.browser_requests,
                browser_bootstrap_attempts=(
                    response.browser_bootstrap_attempts
                ),
                browser_unobserved_bytes=response.browser_unobserved_bytes,
                browser_document_bytes=response.browser_document_bytes,
                browser_asset_bytes=response.browser_asset_bytes,
                http_requests=response.http_requests,
                http_wire_bytes=http_wire_bytes,
                decoded_html_bytes=max(
                    0, int(getattr(response, "decoded_html_bytes", 0))
                ),
                compressed_raw_bytes=max(0, int(compressed_raw_bytes)),
                provider_billed_bytes=provider_value,
            )
        return _LivePageSettlement(provider_billed_bytes=billed)

    def _record_live_budget_stop(
        self,
        *,
        result: WaveResult,
        settings: PipelineSettings,
        leases: Sequence[object],
        start_index: int,
        reason: object,
        already_requeued: int = 0,
    ) -> None:
        untouched = leases[start_index:]
        returned = self.control.requeue_unfetched_targets(untouched)
        result.requeued_at_budget += int(already_requeued) + int(returned)
        result.budget_exhausted = True
        if _uses_production_safety_circuit(settings):
            result.failures.append("production_safety_circuit_exhausted")
        logger.warning(
            "FBref run budget exhausted (%s) — %d target(s) returned "
            "for the next run",
            reason,
            int(already_requeued) + int(returned),
        )

    def fetch_wave(
        self,
        run_id: str,
        *,
        worker_id: str,
        page_kinds: Sequence[str],
        settings: PipelineSettings,
        _live_session: Optional[_LiveFetchSession] = None,
    ) -> WaveResult:
        """Fetch one bounded cohort and commit raw before control success."""

        if settings.run_type == "replay":
            raise PipelineError("Replay mode cannot execute a fetch wave")
        self._assert_persistent_profile(run_id, settings)
        result = WaveResult()
        historical = settings.run_type == "backfill"
        policies = (
            ["historical_once"]
            if historical
            else sorted(
                {
                    _frontier_policy(kind, historical=False)[0]
                    for kind in page_kinds
                }
                | (
                    {"current_completed_once"}
                    if "match" in page_kinds
                    else set()
                )
            )
        )
        leases = self.control.claim_targets(
            run_id,
            worker_id,
            limit=settings.shard_size,
            lease_seconds=FETCH_LEASE_SECONDS,
            page_kinds=page_kinds,
            refresh_policies=policies,
        )
        if not leases:
            summary = self.control.get_run_summary(run_id) or {}
            target_counts = summary.get("target_counts") or {}
            # 'skipped' is a target this run handed back to the queue when it
            # stopped at its budget. Counting it as unfinished made the wave
            # after the budget stop raise instead of no-opping, so a run that
            # spent its budget still went red.
            # 'failed' is unreachable for this run: reanimation (#1102) has
            # already returned every claimable aborted target to 'retry', so
            # what remains 'failed' has no living frontier page behind it —
            # typically a permanent dead-letter that NO run will ever refetch.
            # Counting it as unfinished deadlocked every resumed run; instead
            # the loss is logged loudly below and run quality stays with the
            # traffic gates in validate_and_finish.
            unfinished = sum(
                int(count)
                for status, count in target_counts.items()
                if status not in {"succeeded", "skipped", "failed"}
            )
            terminally_failed = int(target_counts.get("failed") or 0)
            if terminally_failed:
                logger.warning(
                    "Run %s ends its waves with %d terminally failed "
                    "target(s); this run will not refetch them — each page "
                    "is either dead-lettered, backed off, or already handled "
                    "by another run (frontier state is authoritative)",
                    run_id,
                    terminally_failed,
                )
            if unfinished:
                raise FetchWaveError(
                    f"Run has {unfinished} unfinished target(s) that are not claimable"
                )
            limit = self._remaining_wave_limit(
                run_id,
                settings,
                bootstrap_required=(
                    _live_session is None
                    or _live_session.needs_clearance
                ),
            )
            if limit <= 0:
                return result
            cohort = self.control.create_due_run_cohort(
                run_id,
                page_kinds=page_kinds,
                refresh_policies=policies,
                limit=limit,
            )
            result.cohort_size = len(cohort)
            if not cohort:
                return result
            leases = self.control.claim_targets(
                run_id,
                worker_id,
                limit=len(cohort),
                lease_seconds=FETCH_LEASE_SECONDS,
                page_kinds=page_kinds,
                refresh_policies=policies,
            )
        else:
            result.cohort_size = len(leases)
        result.claimed = len(leases)
        if len(leases) != result.cohort_size:
            raise FetchWaveError(
                f"Claimed {len(leases)} of {result.cohort_size} cohort targets"
            )
        owns_session = _live_session is None
        live_session = _live_session or _LiveFetchSession(
            persistent_enabled=settings.persistent_http_session,
            finalization_guard=self.finalization_guard,
        )
        if live_session.persistent_enabled != settings.persistent_http_session:
            raise PipelineError("Live session persistent profile changed mid-run")
        try:
            for lease_index, lease in enumerate(leases):
                # A wave owns the whole shard but processes it sequentially.
                # Renew every still-waiting fence before each target so a
                # later member cannot expire behind earlier network work.
                for outstanding in leases[lease_index:]:
                    self.control.heartbeat(
                        outstanding,
                        lease_seconds=FETCH_LEASE_SECONDS,
                    )
                target = self._page_target_for_lease(lease)
                reservation = None
                response = None
                record = None
                budget_settled = False
                try:
                    if live_session.rollover_if_due(self.control):
                        logger.info(
                            "FBref persistent session reached its safe local "
                            "lifetime; exact tail settled before rollover"
                        )
                    # Exact logical-refresh crash recovery is always safe.
                    # Historical targets are immutable by contract, so they
                    # may additionally adopt the latest verified raw-v2 (or
                    # legacy raw-v1) observation across control runs.  Current
                    # targets still require an exact refresh: an older page may
                    # predate a match final or a season rollover.
                    frontier = self.control.get_frontier_target(
                        lease.target_id
                    ) or {}
                    recoverable = (
                        self.raw_store.has_fetch(lease.logical_refresh_id)
                        or frontier.get("refresh_policy") == "historical_once"
                    )
                    if recoverable:
                        record = self.raw_store.import_fetch_from_available_raw(
                            target,
                            logical_refresh_id=lease.logical_refresh_id,
                            attempt_id=lease.attempt_id,
                        )
                        if record is not None:
                            self._complete_from_record(
                                lease, record, historical=historical
                            )
                            result.recovered_from_raw += 1
                            continue

                    (
                        clearance_requests,
                        clearance_bytes,
                    ) = live_session.clearance_reservation(settings)
                    reserved_requests = settings.target_request_reservation + (
                        clearance_requests
                        if live_session.needs_clearance
                        else 0
                    )
                    reserved_bytes = settings.request_reservation_bytes + (
                        clearance_bytes
                        if live_session.needs_clearance
                        else 0
                    )
                    reservation = self.control.reserve_budget(
                        run_id,
                        lease.logical_refresh_id,
                        attempt_id=lease.attempt_id,
                        requests=reserved_requests,
                        bytes_=reserved_bytes,
                    )
                    self.control.bind_reservation(
                        lease, reservation.reservation_id
                    )
                    slot = self.control.reserve_domain_slot(
                        "fbref.com",
                        interval_seconds=settings.domain_interval_seconds,
                    )
                    self._wait_for_slot(slot.scheduled_at)

                    if live_session.needs_clearance:
                        if live_session.fetcher is None:
                            live_session.fetcher = (
                                live_session.stack.enter_context(
                                    self.fetcher_factory(
                                        settings.proxy_file,
                                        clearance_requests,
                                        clearance_bytes,
                                    )
                                )
                            )
                        fetcher_profile = getattr(
                            live_session.fetcher,
                            "persistent_http_session",
                            False,
                        )
                        if (
                            settings.persistent_http_session
                            and fetcher_profile is not True
                        ):
                            raise PipelineError(
                                "FBref fetcher does not match the persistent "
                                "HTTP run profile"
                            )
                        opened_session_id = (
                            self.control.open_clearance_session(
                                domain="fbref.com",
                                session_version=FETCHER_VERSION,
                                expires_at=(
                                    _as_utc(self.clock())
                                    + timedelta(hours=1)
                                ),
                                run_id=run_id,
                                metadata={"worker_id": worker_id},
                            )
                        )
                        if settings.persistent_http_session:
                            live_session.attach_control_session(
                                opened_session_id
                            )
                        else:
                            live_session.session_id = opened_session_id
                        if settings.persistent_http_session:
                            live_session.begin_persistent(
                                self.control,
                                run_id=run_id,
                                tail_bytes=settings.request_reservation_bytes,
                            )
                        prepare_clearance = getattr(
                            live_session.fetcher,
                            "ensure_clearance",
                            None,
                        )
                        if not callable(prepare_clearance):
                            raise PipelineError(
                                "FBref fetcher must expose ensure_clearance"
                            )
                        browser_navigated = bool(prepare_clearance())
                        live_session.needs_clearance = False
                        if browser_navigated:
                            # The first slot admitted the browser bootstrap.
                            # Advance the durable throttle immediately after
                            # the browser closes, then reserve the real target
                            # slot. This keeps the cooldown visible to another
                            # worker while this process is waiting.
                            self.control.reserve_domain_slot(
                                "fbref.com",
                                interval_seconds=settings.domain_interval_seconds,
                            )
                            target_slot = self.control.reserve_domain_slot(
                                "fbref.com",
                                interval_seconds=(
                                    settings.domain_interval_seconds
                                ),
                            )
                            self._wait_for_slot(target_slot.scheduled_at)
                    response = live_session.fetcher.fetch(
                        lease.canonical_url,
                        page_kind=lease.page_kind,
                        etag=frontier.get("last_etag"),
                        last_modified=frontier.get("last_modified"),
                    )
                    record = self.raw_store.commit_fetch(
                        target,
                        response.body,
                        logical_refresh_id=lease.logical_refresh_id,
                        attempt_id=lease.attempt_id,
                        http_status=response.status_code,
                        fetcher_version=FETCHER_VERSION,
                        headers=response.headers,
                        wire_bytes=response.http_wire_bytes,
                        provider_billed_bytes=response.provider_billed_bytes,
                        latency_ms=response.latency_ms,
                        http_requests=response.http_requests,
                        http_status_history=response.http_status_history,
                        browser_bootstrap_attempts=(
                            response.browser_bootstrap_attempts
                        ),
                        browser_unobserved_bytes=(
                            response.browser_unobserved_bytes
                        ),
                        base_content_hash=frontier.get("last_content_hash"),
                        transport_version=FETCHER_VERSION,
                        session_version=live_session.session_id,
                    )
                    page_settlement = self._settle_live_page_evidence(
                        settings=settings,
                        live_session=live_session,
                        lease=lease,
                        reservation=reservation,
                        response=response,
                        compressed_raw_bytes=record.encoded_bytes,
                    )
                    budget_settled = True
                    self._complete_from_record(
                        lease, record, historical=historical
                    )
                    live_session.consecutive_clearance_refreshes = 0
                    result.fetched += 1
                    result.requests += (
                        response.http_requests + response.browser_requests
                    )
                    result.wire_bytes += response.http_wire_bytes
                    result.decoded_html_bytes += response.decoded_html_bytes
                    result.browser_document_bytes += (
                        response.browser_document_bytes
                    )
                    result.browser_asset_bytes += response.browser_asset_bytes
                    result.browser_bootstraps += (
                        response.browser_bootstrap_attempts
                    )
                    if page_settlement.budget_exceeded:
                        self._record_live_budget_stop(
                            result=result,
                            settings=settings,
                            leases=leases,
                            start_index=lease_index + 1,
                            reason=(
                                "persistent page crossed the run safety circuit"
                            ),
                        )
                        break
                except BudgetExceeded as exc:
                    # Canary exhaustion is its expected bounded stop. Reaching
                    # the much larger production circuit means the run is
                    # incomplete/runaway and must fail loudly after returning
                    # every untouched claim to the durable queue.
                    self._record_live_budget_stop(
                        result=result,
                        settings=settings,
                        leases=leases,
                        start_index=lease_index,
                        reason=exc,
                    )
                    break
                except FetchError as exc:
                    page_settlement = None
                    if (
                        reservation is not None
                        and not budget_settled
                        and (
                            not settings.persistent_http_session
                            or exc.provider_billed_bytes is not None
                        )
                    ):
                        page_settlement = self._settle_live_page_evidence(
                            settings=settings,
                            live_session=live_session,
                            lease=lease,
                            reservation=reservation,
                            response=exc,
                            compressed_raw_bytes=0,
                        )
                        budget_settled = True
                    if (
                        exc.browser_requests > 0
                        or exc.browser_bootstrap_attempts > 0
                    ):
                        # The browser may have navigated successfully and then
                        # failed while exporting cookies, finalising, or
                        # extending the paid lease. The original throttle slot
                        # is stale by then. Reserve an intentionally empty
                        # slot so the next solve (in this or the next run) is
                        # durably delayed by the source minimum.
                        self.control.reserve_domain_slot(
                            "fbref.com",
                            interval_seconds=settings.domain_interval_seconds,
                        )
                    result.requests += (
                        exc.http_requests + exc.browser_requests
                    )
                    result.wire_bytes += max(0, int(exc.wire_bytes))
                    result.browser_document_bytes += max(
                        0, int(exc.browser_document_bytes)
                    )
                    result.browser_asset_bytes += max(
                        0, int(exc.browser_asset_bytes)
                    )
                    result.browser_bootstraps += (
                        exc.browser_bootstrap_attempts
                    )
                    if (
                        page_settlement is not None
                        and page_settlement.budget_exceeded
                        and exc.error_class != "hard_transport_policy"
                    ):
                        if settings.persistent_http_session:
                            live_session.finalize(
                                self.control, status="failed"
                            )
                        self.control.fail_fetch(
                            lease,
                            error_class=exc.error_class,
                            error_message=str(exc),
                            retry_delay_seconds=0,
                            permanent=False,
                            requeue=True,
                            http_status=exc.http_status,
                            http_request_count=exc.http_requests,
                            http_status_history=exc.http_status_history,
                            wire_bytes=exc.wire_bytes,
                            provider_billed_bytes=exc.provider_billed_bytes,
                            latency_ms=exc.latency_ms,
                            transport_version=FETCHER_VERSION,
                            session_version=live_session.session_id,
                        )
                        if _session_failure(exc):
                            result.requeued_dead_clearance += 1
                        self._record_live_budget_stop(
                            result=result,
                            settings=settings,
                            leases=leases,
                            start_index=lease_index + 1,
                            already_requeued=1,
                            reason=(
                                "persistent failed page crossed the run "
                                "safety circuit"
                            ),
                        )
                        break
                    if exc.error_class == "hard_transport_policy":
                        # This is a run-level paid-transport invariant, not a
                        # bad target or a clearance that may be re-solved. Save
                        # the exact failed attempt, leave untouched targets
                        # claimable, and stop before a new transport/lease can
                        # spend again.
                        self.control.fail_fetch(
                            lease,
                            error_class=exc.error_class,
                            error_message=str(exc),
                            retry_delay_seconds=60,
                            permanent=False,
                            requeue=False,
                            http_status=exc.http_status,
                            http_request_count=exc.http_requests,
                            http_status_history=exc.http_status_history,
                            wire_bytes=exc.wire_bytes,
                            provider_billed_bytes=exc.provider_billed_bytes,
                            latency_ms=exc.latency_ms,
                            transport_version=FETCHER_VERSION,
                            session_version=live_session.session_id,
                        )
                        untouched = leases[lease_index + 1:]
                        result.requeued_session_exhaustion += (
                            self.control.requeue_unfetched_targets(untouched)
                        )
                        result.failures.append(
                            f"{lease.target_id}:hard_transport_policy"
                        )
                        break
                    if _session_failure(exc):
                        # The attempt is real traffic evidence, but the page
                        # itself was never judged. Retry the same immutable
                        # logical refresh when a full browser+HTTP reservation
                        # still fits; otherwise return it at the clean budget
                        # boundary before another paid session can start.
                        retry_evidence = dict(
                            error_class=exc.error_class,
                            error_message=str(exc),
                            http_status=exc.http_status,
                            http_request_count=exc.http_requests,
                            http_status_history=exc.http_status_history,
                            wire_bytes=exc.wire_bytes,
                            provider_billed_bytes=exc.provider_billed_bytes,
                            latency_ms=exc.latency_ms,
                            transport_version=FETCHER_VERSION,
                            session_version=live_session.session_id,
                        )
                        live_session.consecutive_clearance_refreshes += 1
                        result.requeued_dead_clearance += 1
                        if settings.persistent_http_session:
                            # Page/failure evidence is durable above.  Close the
                            # exact provider tail before budget math, reset, or
                            # any attempt to acquire another paid lease.
                            live_session.finalize(
                                self.control, status="failed"
                            )
                        run_after_failure = self.control.get_run(run_id)
                        if run_after_failure is None:
                            raise PipelineError(
                                f"Unknown control run {run_id}"
                            )
                        request_remaining = max(
                            0,
                            int(run_after_failure["request_limit"])
                            - int(
                                run_after_failure.get("requests_used") or 0
                            )
                            - int(
                                run_after_failure.get("requests_reserved")
                                or 0
                            ),
                        )
                        byte_remaining = max(
                            0,
                            int(run_after_failure["byte_limit"])
                            - int(run_after_failure.get("bytes_used") or 0)
                            - int(
                                run_after_failure.get("bytes_reserved") or 0
                            ),
                        )
                        retry_reservation = affordable_clearance_reservation(
                            settings,
                            request_remaining=request_remaining,
                            byte_remaining=byte_remaining,
                        )
                        retry_fits_budget = retry_reservation is not None
                        if retry_fits_budget:
                            self.control.retry_session_fetch(
                                lease,
                                **retry_evidence,
                            )
                        else:
                            # The rejected request is real evidence, but a new
                            # browser+HTTP reservation no longer fits. This is
                            # the same clean ceiling as reserve_budget raising
                            # on an untouched target: return the page now,
                            # before another proxy or browser can start.
                            self.control.fail_fetch(
                                lease,
                                retry_delay_seconds=0,
                                permanent=False,
                                requeue=True,
                                **retry_evidence,
                            )
                        if (
                            not settings.persistent_http_session
                            and live_session.session_id is not None
                        ):
                            self.control.close_clearance_session(
                                live_session.session_id,
                                status="failed",
                            )
                            live_session.session_id = None
                        live_session.needs_clearance = True
                        if not retry_fits_budget:
                            untouched = leases[lease_index + 1:]
                            returned = self.control.requeue_unfetched_targets(
                                untouched
                            )
                            result.requeued_at_budget += 1 + returned
                            result.budget_exhausted = True
                            if _uses_production_safety_circuit(settings):
                                result.failures.append(
                                    "production_safety_circuit_exhausted"
                                )
                            logger.warning(
                                "FBref clearance failed (%s, HTTP %s), but "
                                "the remaining run budget cannot fund another "
                                "browser solve — %d target(s) returned for the "
                                "next run",
                                exc.error_class,
                                exc.http_status,
                                1 + returned,
                            )
                            break
                        (
                            allowed_requests,
                            allowed_bytes,
                        ) = retry_reservation
                        (
                            previous_requests,
                            previous_bytes,
                        ) = live_session.clearance_reservation(settings)
                        clearance_downgraded = (
                            allowed_requests < previous_requests
                            or allowed_bytes < previous_bytes
                        )
                        live_session.clearance_requests = allowed_requests
                        live_session.clearance_bytes = allowed_bytes
                        logger.warning(
                            "FBref clearance failed (%s, HTTP %s) — "
                            "%s stays in this run and the session is being "
                            "re-solved on a fresh proxy "
                            "(consecutive refresh %d/%d)",
                            exc.error_class,
                            exc.http_status,
                            lease.target_id,
                            live_session.consecutive_clearance_refreshes,
                            MAX_CONSECUTIVE_CLEARANCE_REFRESHES,
                        )
                        if clearance_downgraded:
                            logger.warning(
                                "FBref re-solve reserves %d request(s) and "
                                "%d byte(s) instead of %d/%d — the rest of "
                                "the run can no longer fund every proxy "
                                "rotation",
                                allowed_requests,
                                allowed_bytes,
                                previous_requests,
                                previous_bytes,
                            )
                        if (
                            live_session.consecutive_clearance_refreshes
                            > MAX_CONSECUTIVE_CLEARANCE_REFRESHES
                        ):
                            untouched = leases[lease_index + 1:]
                            result.requeued_session_exhaustion += (
                                self.control.requeue_unfetched_targets(
                                    untouched
                                )
                            )
                            result.failures.append(
                                "clearance_session_refreshes_exhausted="
                                f"{MAX_CONSECUTIVE_CLEARANCE_REFRESHES}"
                            )
                            break

                        reset = getattr(
                            live_session.fetcher,
                            "reset_clearance",
                            None,
                        )
                        reconfigure = getattr(
                            live_session.fetcher,
                            "reconfigure_clearance_limits",
                            None,
                        )
                        if clearance_downgraded and callable(reconfigure):
                            # Preserve the fetcher's cumulative authenticated
                            # provider counter while rebuilding Camoufox with
                            # the smaller browser allowance.
                            reconfigure(
                                max_browser_requests=allowed_requests,
                                max_browser_bytes=allowed_bytes,
                            )
                        elif callable(reset) and not clearance_downgraded:
                            reset()
                        elif (
                            settings.persistent_http_session
                            and clearance_downgraded
                        ):
                            raise PipelineError(
                                "FBref persistent fetcher cannot preserve "
                                "provider spend across browser reconfigure"
                            )
                        else:
                            # A transport built for the old allowance would
                            # still spend it. Rebuild it against the smaller
                            # reservation the run just booked.
                            live_session.stack.close()
                            live_session.stack = ExitStack()
                            live_session.fetcher = None

                        retry_leases = self.control.claim_targets(
                            run_id,
                            worker_id,
                            limit=1,
                            lease_seconds=FETCH_LEASE_SECONDS,
                            page_kinds=page_kinds,
                            refresh_policies=policies,
                        )
                        if (
                            len(retry_leases) != 1
                            or retry_leases[0].logical_refresh_id
                            != lease.logical_refresh_id
                        ):
                            if retry_leases:
                                self.control.requeue_unfetched_targets(
                                    retry_leases
                                )
                            untouched = leases[lease_index + 1:]
                            result.requeued_session_exhaustion += (
                                self.control.requeue_unfetched_targets(
                                    untouched
                                )
                            )
                            result.failures.append(
                                "clearance_retry_claim_mismatch"
                            )
                            break
                        leases.insert(lease_index + 1, retry_leases[0])
                    else:
                        self.control.fail_fetch(
                            lease,
                            error_class=exc.error_class,
                            error_message=str(exc),
                            retry_delay_seconds=60,
                            permanent=(
                                exc.error_class == "response_too_large"
                            ),
                            requeue=False,
                            http_status=exc.http_status,
                            http_request_count=exc.http_requests,
                            http_status_history=exc.http_status_history,
                            wire_bytes=exc.wire_bytes,
                            provider_billed_bytes=exc.provider_billed_bytes,
                            latency_ms=exc.latency_ms,
                            transport_version=FETCHER_VERSION,
                            session_version=live_session.session_id,
                        )
                        if settings.persistent_http_session:
                            live_session.finalize(
                                self.control, status="failed"
                            )
                            reset = getattr(
                                live_session.fetcher,
                                "reset_clearance",
                                None,
                            )
                            if callable(reset):
                                reset()
                            else:
                                live_session.stack.close()
                                live_session.stack = ExitStack()
                                live_session.fetcher = None
                            live_session.needs_clearance = True
                        result.failures.append(
                            f"{lease.target_id}:{exc.error_class}"
                        )
                except Exception as exc:
                    if reservation is not None and not budget_settled:
                        if (
                            response is not None
                            and (
                                not settings.persistent_http_session
                                or response.provider_billed_bytes is not None
                            )
                        ):
                            # The response crossed the paid meter even when raw
                            # storage raised. Install authoritative settlement
                            # (and typed page evidence in persistent mode)
                            # before recording the failed fetch attempt.
                            self._settle_live_page_evidence(
                                settings=settings,
                                live_session=live_session,
                                lease=lease,
                                reservation=reservation,
                                response=response,
                                compressed_raw_bytes=(
                                    0
                                    if record is None
                                    else record.encoded_bytes
                                ),
                            )
                            budget_settled = True
                        elif not settings.persistent_http_session:
                            self.control.settle_budget(
                                reservation.reservation_id,
                                requests_used=(
                                    0
                                    if response is None
                                    else response.http_requests
                                    + response.browser_requests
                                ),
                                bytes_used=(
                                    0
                                    if response is None
                                    else response.http_wire_bytes
                                    + response.browser_document_bytes
                                    + response.browser_asset_bytes
                                    + response.browser_unobserved_bytes
                                ),
                            )
                            budget_settled = True
                    self.control.fail_fetch(
                        lease,
                        error_class=type(exc).__name__,
                        error_message=str(exc),
                        retry_delay_seconds=60,
                        http_status=(
                            None if response is None else response.status_code
                        ),
                        http_request_count=(
                            0 if response is None else response.http_requests
                        ),
                        http_status_history=(
                            ()
                            if response is None
                            else response.http_status_history
                        ),
                        wire_bytes=(
                            0 if response is None else response.http_wire_bytes
                        ),
                        provider_billed_bytes=(
                            None
                            if response is None
                            else response.provider_billed_bytes
                        ),
                        latency_ms=(
                            None if response is None else response.latency_ms
                        ),
                        transport_version=(
                            None if response is None else FETCHER_VERSION
                        ),
                        session_version=(
                            None
                            if response is None
                            else live_session.session_id
                        ),
                    )
                    if (
                        settings.persistent_http_session
                        and live_session.session_id is not None
                        and live_session.state != "control_closed"
                    ):
                        live_session.finalize(
                            self.control, status="failed"
                        )
                    result.failures.append(
                        f"{lease.target_id}:{type(exc).__name__}"
                    )
                    if settings.persistent_http_session:
                        # Any unclassified failure may have left only
                        # conservative target evidence. Stop before a second
                        # paid session and let abort_run settle that reserve.
                        break
        finally:
            if owns_session:
                live_session.close(
                    self.control,
                    status="failed" if result.failures else "closed",
                )
                if settings.persistent_http_session and not result.failures:
                    self.control.assert_persistent_metering_reconciled(run_id)
        if result.failures:
            raise FetchWaveError("; ".join(result.failures))
        return result

    @staticmethod
    def _merge_wave_result(target: WaveResult, source: WaveResult) -> None:
        for name in WaveResult.__dataclass_fields__:
            value = getattr(source, name)
            if name == "failures":
                target.failures.extend(value)
            elif isinstance(value, bool):
                setattr(target, name, bool(getattr(target, name)) or value)
            else:
                setattr(target, name, int(getattr(target, name)) + int(value))

    def run_live_waves(
        self,
        run_id: str,
        *,
        worker_id: str,
        page_kinds: Sequence[str],
        settings: PipelineSettings,
        max_batches: int = 80,
    ) -> LiveRunResult:
        """Fetch raw and parse offline in one warm, bounded process.

        The fetcher and its proxy quarantine live across batches. Each batch
        still commits every raw object before the parser receives its manifest,
        and parser discovery becomes eligible for the next batch.
        """

        if settings.run_type == "replay":
            raise PipelineError("Replay mode cannot execute live waves")
        normalized_batches = _normalize_live_batch_count(max_batches)

        run = self.control.get_run(run_id)
        self._assert_persistent_profile(run_id, settings)
        if run is not None and str(run.get("status") or "") == "failed":
            if settings.persistent_http_session:
                # An aborted/uncertain persistent session is conservative,
                # not exact.  Reanimating that DagRun would allow a second
                # paid lease before the old byte ledger can be reconciled.
                self.control.assert_persistent_metering_reconciled(run_id)
            # ``airflow tasks clear -t run_live_waves`` re-runs only this
            # task, never initialize_run, so the #1102 reanimation must live
            # on the path every resume actually takes.  start_run itself
            # refuses terminal runs (succeeded/publication/sealed).
            self.control.start_run(run_id)

        aggregate = LiveRunResult()
        live_session = _LiveFetchSession(
            persistent_enabled=settings.persistent_http_session,
            finalization_guard=self.finalization_guard,
        )
        failed = True
        try:
            for batch in range(1, normalized_batches + 1):
                fetched = self.fetch_wave(
                    run_id,
                    worker_id=f"{worker_id}:batch-{batch:02d}",
                    page_kinds=page_kinds,
                    settings=settings,
                    _live_session=live_session,
                )
                live_session.rollover_if_due(
                    self.control,
                    within_seconds=PERSISTENT_PARSE_GUARD_SECONDS,
                )
                parsed = self.parse_wave(
                    run_id,
                    page_kinds=page_kinds,
                    settings=settings,
                )
                aggregate.batches = batch
                self._merge_wave_result(aggregate.fetch, fetched)
                self._merge_wave_result(aggregate.parse, parsed)

                if fetched.budget_exhausted:
                    break
                if fetched.claimed == 0 and parsed.cohort_size == 0:
                    aggregate.frontier_closed = True
                    break
            failed = False
            return aggregate
        finally:
            live_session.close(
                self.control,
                status="failed" if failed else "closed",
            )
            if settings.persistent_http_session and not failed:
                self.control.assert_persistent_metering_reconciled(run_id)

    def _eligible_competitions(self) -> dict[str, dict]:
        return {
            str(row["competition_id"]): row
            for row in self.control.eligible_competitions()
        }

    def _seed_links(
        self,
        links: Iterable[DiscoveredPageLink],
        *,
        historical: bool,
        refresh_policy: Optional[str] = None,
        parent_record: Optional[RawFetchRecord] = None,
        reconcile_after: bool = True,
    ) -> tuple[int, int]:
        return self._seed_link_candidates(
            (
                _FrontierSeedCandidate(
                    link=link,
                    historical=historical,
                    refresh_policy=refresh_policy,
                )
                for link in links
            ),
            parent_record=parent_record,
            reconcile_after=reconcile_after,
        )

    def _preserve_live_policy(self, prepared: FrontierTarget) -> FrontierTarget:
        """Keep a live current-lane policy when a historical seed touches it."""

        if prepared.page_kind not in _HISTORICAL_DOWNGRADE_GUARDED_KINDS:
            return prepared
        getter = getattr(self.control, "get_frontier_target", None)
        if getter is None:
            return prepared
        existing = getter(prepared.target_id)
        if not existing:
            return prepared
        policy = str(existing.get("refresh_policy") or "")
        if policy in {"", "historical_once", "current_completed_once"}:
            return prepared
        logger.info(
            "FBref historical seed kept live policy %s for %s",
            policy,
            prepared.target_id,
        )
        return FrontierTarget(
            target_id=prepared.target_id,
            page_kind=prepared.page_kind,
            canonical_url=prepared.canonical_url,
            source_ids=prepared.source_ids,
            refresh_policy=policy,
            priority=int(existing.get("priority") or prepared.priority),
            next_fetch_at=existing.get("next_fetch_at"),
            source=prepared.source,
        )

    def _seed_link_candidates(
        self,
        candidates: Iterable[_FrontierSeedCandidate],
        *,
        parent_record: Optional[RawFetchRecord] = None,
        reconcile_after: bool = True,
        eligible_competitions: Optional[Mapping[str, dict]] = None,
    ) -> tuple[int, int]:
        eligible = set(
            self._eligible_competitions()
            if eligible_competitions is None
            else eligible_competitions
        )
        seeded_targets: set[str] = set()
        skipped_targets: set[str] = set()
        prepared_targets: dict[str, FrontierTarget] = {}
        provenance_edges: list[FrontierProvenance] = []

        parent_scopes: set[tuple[Optional[str], Optional[str]]] = set()
        if parent_record is not None:
            parent_competition = parent_record.source_ids.get(
                "competition_id"
            )
            parent_season = parent_record.source_ids.get("season_id")
            if parent_competition:
                parent_scopes.add((
                    str(parent_competition),
                    None if parent_season is None else str(parent_season),
                ))
            else:
                list_provenance = getattr(
                    self.control, "list_frontier_provenance", None
                )
                if list_provenance is not None:
                    for edge in list_provenance(
                        child_target_id=parent_record.target_id,
                        limit=1000,
                    ):
                        competition_id = edge.get(
                            "carried_competition_id"
                        )
                        season_id = edge.get("carried_season_id")
                        parent_scopes.add((
                            None
                            if competition_id is None
                            else str(competition_id),
                            None if season_id is None else str(season_id),
                        ))
        for candidate in candidates:
            link = candidate.link
            source_ids = dict(link.source_ids)
            target = page_target_from_link(link)
            prepared = frontier_target(
                target, historical=candidate.historical
            )
            if candidate.refresh_policy is not None:
                prepared = FrontierTarget(
                    target_id=prepared.target_id,
                    page_kind=prepared.page_kind,
                    canonical_url=prepared.canonical_url,
                    source_ids=prepared.source_ids,
                    refresh_policy=candidate.refresh_policy,
                    priority=prepared.priority,
                    next_fetch_at=prepared.next_fetch_at,
                    source=prepared.source,
                )
            if candidate.historical and candidate.refresh_policy is None:
                prepared = self._preserve_live_policy(prepared)
            existing = prepared_targets.get(target.target_id)
            if existing is None:
                prepared_targets[target.target_id] = prepared
            elif (
                existing.refresh_policy,
                existing.priority,
                existing.next_fetch_at,
            ) != (
                prepared.refresh_policy,
                prepared.priority,
                prepared.next_fetch_at,
            ):
                raise StateConflict(
                    f"Target {target.target_id} has conflicting seed policies"
                )

            link_competition = source_ids.get("competition_id")
            link_season = source_ids.get("season_id")
            scopes = (
                {(
                    str(link_competition),
                    None if link_season is None else str(link_season),
                )}
                if link_competition is not None
                else parent_scopes or {(None, None)}
            )
            male_scope = any(
                competition_id is not None
                and competition_id in eligible
                for competition_id, _ in scopes
            )
            if male_scope:
                seeded_targets.add(target.target_id)
            else:
                skipped_targets.add(target.target_id)

            if parent_record is not None:
                for competition_id, season_id in sorted(
                    scopes,
                    key=lambda scope: (
                        scope[0] or "", scope[1] or ""
                    ),
                ):
                    provenance_edges.append(FrontierProvenance(
                        parent_target_id=parent_record.target_id,
                        child_target_id=target.target_id,
                        relation=f"page_link:{link.page_kind}",
                        carried_competition_id=competition_id,
                        carried_season_id=season_id,
                        parent_content_hash=parent_record.content_hash,
                        parser_version=DISCOVERY_PARSER_VERSION,
                        logical_refresh_id=(
                            parent_record.logical_refresh_id
                        ),
                        metadata={
                            "child_page_kind": link.page_kind,
                        },
                    ))
        if prepared_targets or provenance_edges:
            ordered_targets = [
                prepared_targets[target_id]
                for target_id in sorted(prepared_targets)
            ]
            if (
                len(ordered_targets) <= _DISCOVERY_TARGET_BATCH_LIMIT
                and len(provenance_edges) <= _DISCOVERY_EDGE_BATCH_LIMIT
            ):
                self.control.upsert_frontier_discovery_batch(
                    targets=ordered_targets,
                    provenance=provenance_edges,
                )
            else:
                # A season-stats page of a large competition links more than a
                # thousand players, and the sealed control store refuses such a
                # batch outright -- the whole page then fails to parse forever,
                # taking the daily recovery wave down with it.  Split it, targets
                # first: a provenance edge has a foreign key to its child target.
                for start in range(
                    0, len(ordered_targets), _DISCOVERY_TARGET_BATCH_LIMIT
                ):
                    self.control.upsert_frontier_discovery_batch(
                        targets=ordered_targets[
                            start:start + _DISCOVERY_TARGET_BATCH_LIMIT
                        ],
                        provenance=(),
                    )
                for start in range(
                    0, len(provenance_edges), _DISCOVERY_EDGE_BATCH_LIMIT
                ):
                    self.control.upsert_frontier_discovery_batch(
                        targets=(),
                        provenance=provenance_edges[
                            start:start + _DISCOVERY_EDGE_BATCH_LIMIT
                        ],
                    )
        if parent_record is not None and reconcile_after:
            self._reconcile_frontier_scope()
        return len(seeded_targets), len(skipped_targets)

    def _reconcile_frontier_scope(self) -> None:
        reconcile_scope = getattr(
            self.control, "reconcile_frontier_scope", None
        )
        if reconcile_scope is not None:
            reconcile_scope(source="fbref")

    def _parse_competition_index(
        self,
        run_id: str,
        html: str,
        record: RawFetchRecord,
    ) -> tuple[int, int]:
        parsed = parse_competition_index_html(html)
        competitions = parsed.datasets["competitions"].records
        snapshot_id = self.control.create_registry_snapshot(
            snapshot_id=_registry_snapshot_id(record),
            run_id=run_id,
            fetched_at=_as_utc(record.fetched_at),
            successful=not parsed.has_errors,
            content_hash=record.content_hash,
            metadata={
                "page_kind": "competition_index",
                "sentinels": sentinel_coverage(
                    competitions, SENTINEL_COMPETITIONS
                ),
            },
        )
        if parsed.has_errors:
            raise ParseWaveError("Competition index discovery contract failed")
        self.control.reconcile_competitions(
            snapshot_id, [_registry_entry(item) for item in competitions]
        )
        links: list[DiscoveredPageLink] = []
        skipped = 0
        for competition in competitions:
            if competition_eligibility(competition).value != "eligible":
                skipped += 1
                continue
            links.append(DiscoveredPageLink(
                page_kind="competition",
                canonical_url=competition.history_url,
                source_ids={
                    "competition_id": competition.competition_id,
                },
            ))
        seeded, rejected = self._seed_links(
            links, historical=False, parent_record=record
        )
        return seeded, skipped + rejected

    def _parse_competition(
        self,
        run_id: str,
        html: str,
        record: RawFetchRecord,
        *,
        run_type: str,
    ) -> tuple[int, int]:
        competition_id = str(record.source_ids["competition_id"])
        registry = self._eligible_competitions()
        row = registry.get(competition_id)
        if row is None:
            raise ParseWaveError(
                f"Competition {competition_id} is not eligible for children"
            )
        competition = _competition_from_registry(row)
        parsed = parse_competition_html(html, competition)
        seasons = parsed.datasets["seasons"].records
        direct_matches = parsed.datasets["matches"].records
        snapshot_id = self.control.create_registry_snapshot(
            snapshot_id=_registry_snapshot_id(record),
            run_id=run_id,
            fetched_at=_as_utc(record.fetched_at),
            successful=not parsed.has_errors,
            content_hash=record.content_hash,
            metadata={
                "page_kind": "competition",
                "competition_id": competition_id,
            },
        )
        if parsed.has_errors:
            raise ParseWaveError(
                f"Season discovery failed for competition {competition_id}"
            )
        current_label = competition.last_season
        current_candidates = [
            index
            for index, season in enumerate(seasons)
            if current_label and season.label == current_label
        ]
        if current_candidates:
            # FBref occasionally publishes two history URLs with the same
            # display label (competition 612 did so for "2025"). A current
            # edition is singular: prefer the source ID that exactly matches
            # the advertised label, then the first/newest history row.
            canonical_current = min(
                current_candidates,
                key=lambda index: (
                    seasons[index].season_id != current_label,
                    index,
                ),
            )
            current_season_id = seasons[canonical_current].season_id
        elif current_label and any(
            match.season_id == current_label for match in direct_matches
        ):
            current_season_id = current_label
        else:
            current_season_id = (
                seasons[0].season_id
                if seasons
                else direct_matches[0].season_id
                if direct_matches
                else None
            )

        entries = [
            SeasonRegistryEntry(
                competition_id=competition_id,
                season_id=season.season_id,
                canonical_url=season.season_url,
                label=season.label,
                is_current=season.season_id == current_season_id,
                metadata={"calendar_type": season.calendar_type.value},
            )
            for season in seasons
        ]
        registered_season_ids = {entry.season_id for entry in entries}
        for match in direct_matches:
            if match.season_id in registered_season_ids:
                continue
            # Some competition histories link an edition straight to its only
            # match report.  The edition still belongs in the registry: scope
            # reconciliation must be able to prove that the carried season is
            # an active male edition even though no season page exists.
            entries.append(SeasonRegistryEntry(
                competition_id=competition_id,
                season_id=match.season_id,
                canonical_url=match.canonical_url,
                label=match.season_id,
                is_current=match.season_id == current_season_id,
                metadata={
                    "calendar_type": CalendarType.TOURNAMENT.value,
                    "direct_match_only": True,
                },
            ))
            registered_season_ids.add(match.season_id)
        self.control.reconcile_seasons(snapshot_id, competition_id, entries)
        upsert_alias = getattr(self.control, "upsert_season_alias", None)
        if upsert_alias is not None:
            for entry in entries:
                upsert_alias(SeasonAlias(
                    competition_id=competition_id,
                    alias=entry.season_id,
                    season_id=entry.season_id,
                    alias_kind="source",
                ), snapshot_id=snapshot_id)
            by_label: dict[str, list[SeasonRegistryEntry]] = {}
            for entry in entries:
                if entry.label:
                    by_label.setdefault(str(entry.label), []).append(entry)
            for label, candidates in by_label.items():
                canonical = min(
                    candidates,
                    key=lambda entry: (
                        not entry.is_current,
                        entry.season_id != label,
                        entry.season_id,
                    ),
                )
                # Source IDs and display labels share one alias namespace.
                # Never let a shifted label reinterpret another canonical
                # source season (for example Club World Cup 2021/"2022").
                if (
                    label in registered_season_ids
                    and canonical.season_id != label
                ):
                    continue
                upsert_alias(SeasonAlias(
                    competition_id=competition_id,
                    alias=label,
                    season_id=canonical.season_id,
                    alias_kind="label",
                ), snapshot_id=snapshot_id)
        candidates: list[_FrontierSeedCandidate] = []
        for season in seasons:
            is_current = season.season_id == current_season_id
            if run_type == "current" and not is_current:
                continue
            if run_type == "backfill" and is_current:
                continue
            candidates.append(_FrontierSeedCandidate(
                link=DiscoveredPageLink(
                    page_kind="season",
                    canonical_url=season.season_url,
                    source_ids={
                        "competition_id": competition_id,
                        "season_id": season.season_id,
                    },
                ),
                historical=not is_current,
            ))
        for match in direct_matches:
            is_current = match.season_id == current_season_id
            # Inventory every direct edition while the authoritative history
            # page is in hand. Current fetch waves claim only
            # ``current_completed_once``; backfill waves claim only
            # ``historical_once``, so recording both policies here makes old
            # one-match finals reachable without charging the current crawl.
            candidates.append(_FrontierSeedCandidate(
                link=DiscoveredPageLink(
                    page_kind="match",
                    canonical_url=match.canonical_url,
                    source_ids={
                        "competition_id": match.comp_id,
                        "season_id": match.season_id,
                        "match_id": match.match_id,
                    },
                ),
                historical=not is_current,
                refresh_policy=(
                    "current_completed_once" if is_current
                    else "historical_once"
                ),
            ))
        return self._seed_link_candidates(
            candidates,
            parent_record=record,
            eligible_competitions=registry,
        )

    @staticmethod
    def _season_ref(record: RawFetchRecord) -> SeasonRef:
        season_id = str(record.source_ids["season_id"])
        return SeasonRef(
            comp_id=str(record.source_ids["competition_id"]),
            season_id=season_id,
            label=season_id,
            calendar_type=CalendarType.OPAQUE,
            season_url=record.canonical_url,
        )

    def _parse_discovery_children(
        self,
        html: str,
        record: RawFetchRecord,
        *,
        historical: bool,
    ) -> tuple[int, int]:
        candidates: list[_FrontierSeedCandidate] = []
        if record.page_kind == "season":
            parsed = parse_season_html(
                html,
                self._season_ref(record),
                historical=historical,
            )
            if parsed.has_errors:
                raise ParseWaveError(
                    f"Schedule discovery failed for {record.target_id}"
                )
            for schedule in parsed.datasets["schedules"].records:
                candidates.append(_FrontierSeedCandidate(
                    link=DiscoveredPageLink(
                        page_kind="schedule",
                        canonical_url=schedule.schedule_url,
                        source_ids={
                            "competition_id": schedule.competition_id,
                            "season_id": schedule.season_id,
                        },
                    ),
                    historical=historical,
                ))
        elif record.page_kind == "schedule":
            parsed = parse_schedule_html(html, self._season_ref(record))
            if parsed.has_errors:
                raise ParseWaveError(
                    f"Schedule parse failed for {record.target_id}"
                )
            completed_urls = {
                str(row.get("match_url"))
                for row in parsed.datasets["schedule_rows"].records
                if row.get("match_url")
                and str(row.get("score") or "").strip()
            }
            for match in parsed.datasets["matches"].records:
                match_link = DiscoveredPageLink(
                    page_kind="match",
                    canonical_url=match.canonical_url,
                    source_ids={
                        "competition_id": match.competition_id,
                        "season_id": match.season_id,
                        "match_id": match.match_id,
                    },
                )
                candidates.append(_FrontierSeedCandidate(
                    link=match_link,
                    historical=(
                        historical
                    ),
                    refresh_policy=(
                        None
                        if historical or match.canonical_url not in completed_urls
                        else "current_completed_once"
                    ),
                ))
        discovered = discover_page_links(
            html,
            parent_source_ids=record.source_ids,
            parent_url=record.canonical_url,
        )
        # A match inherits competition/season identity only from a parsed
        # schedule row.  Generic navigation links on player or other pages are
        # not authoritative and must never create paid match targets.
        discovered = [
            link for link in discovered if link.page_kind != "match"
        ]
        discovered = [
            link
            for link in discovered
            if page_target_from_link(link).target_id != record.target_id
        ]
        candidates.extend(
            _FrontierSeedCandidate(link=link, historical=historical)
            for link in discovered
        )
        return self._seed_link_candidates(
            candidates,
            parent_record=record,
        )

    def _persist_generic(
        self,
        run_id: str,
        html: str,
        record: RawFetchRecord,
    ):
        page = self._parse_generic(html, record)
        self._persist_generic_page(
            run_id, page, record, record_failure=True
        )
        return page

    @staticmethod
    def _parse_generic(html: str, record: RawFetchRecord) -> PageDocument:
        page = parse_page_document(
            html,
            target_id=record.target_id,
            page_kind=record.page_kind,
            source_ids=record.source_ids,
            content_hash=record.content_hash,
        )
        if (
            page.target_id != record.target_id
            or page.page_kind != record.page_kind
            or page.content_hash != record.content_hash
            or page.parser_version != PAGE_DOCUMENT_VERSION
        ):
            raise ParseWaveError(
                f"Generic parser identity mismatch for {record.target_id}"
            )
        return page

    def _persist_generic_page(
        self,
        run_id: str,
        page: PageDocument,
        record: RawFetchRecord,
        *,
        record_failure: bool,
    ) -> None:
        try:
            self.generic_writer.persist_page(
                page,
                canonical_url=record.canonical_url,
                run_id=run_id,
                staging_identity=record.logical_refresh_id,
            )
        except Exception as exc:
            if record_failure:
                try:
                    self._record_page_completion(
                        record,
                        page,
                        succeeded=False,
                        error=exc,
                        parse_succeeded=not page.errors,
                    )
                except StateConflict:
                    # These exact bytes already have a completed manifest from
                    # an earlier retry; immutable completion evidence stands.
                    logger.warning(
                        "Failure manifest for %s not recorded: the generic "
                        "manifest is already completed",
                        record.target_id,
                    )
            raise
        self._record_generic_table_results(record, page)

    def _record_generic_table_results(
        self,
        record: RawFetchRecord,
        page: PageDocument,
        *,
        control=None,
    ) -> None:
        control_store = self.control if control is None else control
        for table in page.tables:
            control_store.record_dataset_manifest(
                target_id=record.target_id,
                content_hash=record.content_hash,
                parser_version=page.parser_version,
                dataset=f"table:{table.table_id}:{table.source_location}",
                availability=table.availability.value,
                parse_status="succeeded",
                persistence_status="succeeded",
                validation_status="succeeded",
                row_count=table.row_count,
                error_message=table.reason,
            )

    def _typed_context(
        self, record: RawFetchRecord
    ) -> Optional[TypedSourceContext]:
        competition_id = record.source_ids.get("competition_id")
        season_id = record.source_ids.get("season_id")
        if not competition_id or not season_id:
            return None
        registry = self._eligible_competitions().get(str(competition_id), {})
        return TypedSourceContext(
            source_competition_id=str(competition_id),
            source_season_id=str(season_id),
            competition_name=(
                None if not registry else str(registry.get("name") or "")
            ),
            season_label=str(season_id),
        )

    def _parse_typed_match(
        self, html: str, record: RawFetchRecord
    ) -> tuple[MatchParseResult, str, TypedSourceContext]:
        context = self._typed_context(record)
        if context is None:
            raise TypedBronzeError(
                "Typed page requires source competition_id and season_id"
            )
        match_id = str(record.source_ids.get("match_id") or "").strip()
        if not match_id:
            raise TypedBronzeError("Match target has no source match_id")
        parsed = parse_typed_match_html(
            html,
            match_id=match_id,
            context=context,
            require_player_contract=False,
        )
        if not isinstance(parsed, MatchParseResult):
            raise TypedBronzeError("Typed match parser returned invalid result")
        return parsed, match_id, context

    def _persist_preparsed_typed_match(
        self,
        run_id: str,
        record: RawFetchRecord,
        parsed: MatchParseResult,
        *,
        match_id: str,
        context: TypedSourceContext,
    ) -> None:
        counts: Optional[Mapping[str, int]] = None
        try:
            if parsed.has_errors:
                raise TypedBronzeError("Typed match parser failed")
            counts = self.typed_adapter.writer.persist_match(
                parsed,
                match_id=match_id,
                context=context,
                run_id=run_id,
                target_identity=record.logical_refresh_id,
            )
        except Exception:
            self._record_typed_results(
                record, parsed.datasets, persisted=None
            )
            raise
        self._record_typed_results(record, parsed.datasets, persisted=counts)
        self._record_typed_completion(record)

    def _record_typed_results(
        self,
        record: RawFetchRecord,
        parsed: Mapping[str, object],
        *,
        persisted: Optional[Mapping[str, int]],
        control=None,
    ) -> None:
        control_store = self.control if control is None else control
        for name, dataset in parsed.items():
            status = str(getattr(dataset.status, "value", dataset.status))
            requires_persistence = typed_result_requires_persistence(dataset)
            failed = status == "error" or (
                requires_persistence and persisted is None
            )
            was_persisted = bool(persisted is not None and name in persisted)
            control_store.record_dataset_manifest(
                target_id=record.target_id,
                content_hash=record.content_hash,
                parser_version=TYPED_BRONZE_PARSER_VERSION,
                dataset=f"typed:{name}",
                availability=status,
                parse_status="failed" if status == "error" else "succeeded",
                persistence_status=(
                    "failed"
                    if failed
                    else "succeeded" if was_persisted else "skipped"
                ),
                validation_status=(
                    "failed"
                    if failed
                    else "succeeded"
                    if (was_persisted or not requires_persistence)
                    else "skipped"
                ),
                row_count=int(getattr(dataset, "row_count", 0) or 0),
                error_class=getattr(dataset, "error_type", None),
                error_message=(
                    getattr(dataset, "error_message", None)
                    or getattr(dataset, "reason", None)
                ),
            )

    def _record_typed_completion(
        self,
        record: RawFetchRecord,
        *,
        control=None,
    ) -> None:
        """Cache typed success only after every dataset result."""

        control_store = self.control if control is None else control
        control_store.record_dataset_manifest(
            target_id=record.target_id,
            content_hash=record.content_hash,
            parser_version=TYPED_BRONZE_PARSER_VERSION,
            dataset="typed:__complete__",
            availability=Availability.AVAILABLE.value,
            parse_status="succeeded",
            persistence_status="succeeded",
            validation_status="succeeded",
            row_count=0,
        )

    def _persist_typed(
        self,
        run_id: str,
        html: str,
        record: RawFetchRecord,
    ) -> None:
        context = self._typed_context(record)
        if context is None:
            raise TypedBronzeError(
                "Typed page requires source competition_id and season_id"
            )
        parsed: Mapping[str, object]
        counts: Optional[Mapping[str, int]] = None
        try:
            if record.page_kind == "schedule":
                schedule = parse_typed_schedule_html(html, context=context)
                parsed = {"schedule": schedule}
                if schedule.status.value == "error":
                    raise TypedBronzeError(
                        schedule.error_message or schedule.reason or "schedule parse"
                    )
                counts = self.typed_adapter.writer.persist_schedule(
                    schedule,
                    context=context,
                    run_id=run_id,
                    target_identity=record.logical_refresh_id,
                )
            elif record.page_kind in {"season", "season_stats"}:
                stat_route = (
                    "standard"
                    if record.page_kind == "season"
                    else str(record.source_ids.get("stat_route") or "")
                )
                parsed = parse_typed_season_stats_html(
                    html, context=context, stat_route=stat_route
                )
                if any(item.status.value == "error" for item in parsed.values()):
                    raise TypedBronzeError("Typed season parser failed")
                counts = self.typed_adapter.writer.persist_season_stats(
                    parsed,
                    context=context,
                    run_id=run_id,
                    target_identity=record.logical_refresh_id,
                )
            elif record.page_kind == "match":
                match_id = record.source_ids.get("match_id")
                if not match_id:
                    raise TypedBronzeError("Match target has no source match_id")
                match = parse_typed_match_html(
                    html,
                    match_id=str(match_id),
                    context=context,
                    require_player_contract=False,
                )
                parsed = match.datasets
                if match.has_errors:
                    raise TypedBronzeError("Typed match parser failed")
                counts = self.typed_adapter.writer.persist_match(
                    match,
                    match_id=str(match_id),
                    context=context,
                    run_id=run_id,
                    target_identity=record.logical_refresh_id,
                )
            else:
                return
        except Exception:
            if "parsed" in locals():
                self._record_typed_results(
                    record, parsed, persisted=None
                )
            raise
        self._record_typed_results(record, parsed, persisted=counts)
        # This must remain the final typed manifest.  A crash while recording
        # any individual result therefore leaves replay work discoverable.
        self._record_typed_completion(record)

    def _persist_guarded_typed(
        self,
        run_id: str,
        html: str,
        record: RawFetchRecord,
    ) -> bool:
        """Promote typed data only from the newest fenced raw observation."""

        if record.page_kind not in {
            "schedule",
            "season",
            "season_stats",
            "match",
        }:
            return False
        if self._typed_context(record) is None:
            raise TypedBronzeError(
                "Typed page requires source competition_id and season_id"
            )
        with self.control.guard_latest_content(
            record.target_id,
            record.content_hash,
            record.logical_refresh_id,
        ) as is_latest:
            if is_latest is None:
                raise TypedPromotionDeferred(
                    f"Typed promotion deferred for active target {record.target_id}"
                )
            if is_latest:
                self._persist_typed(run_id, html, record)
                return True
        self._record_stale_typed_observation(record)
        return False

    def _record_stale_typed_observation(
        self, record: RawFetchRecord
    ) -> None:
        """Record why this observation intentionally skipped typed output."""

        self.control.record_dataset_manifest(
            target_id=record.target_id,
            content_hash=record.content_hash,
            parser_version=TYPED_BRONZE_PARSER_VERSION,
            dataset="typed:__stale_observation__",
            availability=Availability.DUPLICATE.value,
            parse_status="succeeded",
            persistence_status="skipped",
            validation_status="skipped",
            row_count=0,
            error_class="StaleTypedObservationSkipped",
            error_message=(
                "Typed promotion skipped because frontier points to newer raw"
            ),
        )
        # Stale is completed only in the per-observation fence.  A content-keyed
        # completion here could incorrectly close a later A -> B -> A refresh.

    def _record_page_completion(
        self,
        record: RawFetchRecord,
        page,
        *,
        succeeded: bool,
        error: Optional[Exception] = None,
        parse_succeeded: bool = False,
        control=None,
    ) -> None:
        control_store = self.control if control is None else control
        control_store.record_dataset_manifest(
            target_id=record.target_id,
            content_hash=record.content_hash,
            parser_version=PAGE_DOCUMENT_VERSION,
            dataset="__page__",
            availability=(
                Availability.ERROR.value
                if not succeeded
                else Availability.AVAILABLE.value
                if page is not None and page.tables
                else Availability.EMPTY.value
            ),
            parse_status=(
                "succeeded" if succeeded or parse_succeeded else "failed"
            ),
            persistence_status="succeeded" if succeeded else "failed",
            validation_status="succeeded" if succeeded else "failed",
            row_count=(
                0
                if page is None
                else sum(table.row_count for table in page.tables)
            ),
            error_class=None if error is None else type(error).__name__,
            error_message=(
                str(error)
                if error is not None
                else "verified_zero_table_page"
                if page is not None and not page.tables
                else None
            ),
        )

    def _apply_stateful_effects(
        self,
        run_id: str,
        html: str,
        record: RawFetchRecord,
        *,
        run_type: str,
        historical: bool,
    ) -> tuple[int, int]:
        """Apply discovery/reconciliation while the raw frontier is fenced."""

        if record.page_kind == "competition_index":
            return self._parse_competition_index(run_id, html, record)
        if record.page_kind == "competition":
            return self._parse_competition(
                run_id, html, record, run_type=run_type
            )
        return self._parse_discovery_children(
            html, record, historical=historical
        )

    def _target_is_historical(
        self,
        record: RawFetchRecord,
        *,
        run_type: str,
    ) -> bool:
        """Use durable target policy before the run that supplied its raw.

        Cross-run recovery may parse historical target bytes captured by a
        formerly-current run.  The frontier policy records the target's
        present lifecycle and therefore outranks that source-run provenance.
        """

        frontier = self.control.get_frontier_target(record.target_id) or {}
        refresh_policy = str(frontier.get("refresh_policy") or "").strip()
        if refresh_policy:
            return refresh_policy == "historical_once"
        return run_type == "backfill"

    def _validate_pre_promotion_contract(
        self,
        html: str,
        record: RawFetchRecord,
        *,
        historical: bool,
    ) -> None:
        """Reject ambiguous source shells before replacing typed Bronze data."""

        if record.page_kind != "season":
            return
        parsed = parse_season_html(
            html,
            self._season_ref(record),
            historical=historical,
        )
        if not parsed.has_errors:
            return
        reason = ",".join(
            sorted(
                {
                    str(dataset.reason or dataset.error_type or "unknown")
                    for dataset in parsed.datasets.values()
                    if dataset.status.value == "error"
                }
            )
        )
        # Only a shape the source itself proves finished may retire its target.
        # Every other contract failure -- a table-free shell that cannot prove
        # its identity, a link the parser could not canonicalize -- stays a loud
        # wave failure, because a retry of fresher bytes can still succeed.
        if reason == "schedule_link_missing" and (
            season_page_is_complete_without_schedule(
                html, competition_id=str(record.source_ids["competition_id"])
            )
        ):
            raise SourceContractRejected(
                f"Season source contract failed for {record.target_id}",
                target_id=record.target_id,
                content_hash=record.content_hash,
                reason=reason,
            )
        if reason == "schedule_season_mismatch" and (
            historical
            or url_addresses_archived_edition(record.canonical_url)
        ):
            raise SourceContractRejected(
                f"Season source contract failed for {record.target_id}",
                target_id=record.target_id,
                content_hash=record.content_hash,
                reason=reason,
            )
        raise ParseWaveError(
            f"Season source contract failed for {record.target_id}"
        )

    def _historical_contract_rejection(
        self,
        html: str,
        record: RawFetchRecord,
        page: PageDocument,
        *,
        recover_cross_run: bool = False,
    ) -> Optional[SourceContractRejected]:
        """Isolate a page shape only the archive can publish, and only there.

        FBref serves finished-but-table-free pages for old seasons: the 1938
        Austria squad page is a full 200 response whose whole content is a note
        about a withdrawn World Cup tie.  The generic contract is right to
        reject it, but a single such page must not end a run -- the target is
        immutable, its bytes are already committed as error evidence, and every
        later run would fetch and reject the very same bytes again.

        Retirement is terminal, so it is granted only on positive evidence, the
        same rule ``_validate_pre_promotion_contract`` states 30 lines above and
        ``season_page_is_complete_without_schedule`` enforces for seasons: a
        table-free response that cannot prove it is this target's own page stays
        a loud failure, because a foreign 200 shell has the very same shape and
        a retry of fresher bytes can still succeed.

        The one structural exception is a legacy matchlog target whose URL has
        no player ID.  Its stored ``player_id=matchlogs`` came from discovery
        collapsing the empty path segment, so the address can never identify a
        real page; historical recovery may retire it without trusting the
        response body.

        Five gates, each of which keeps a real breakage loud:

        - ``page_contract:`` verdicts only -- a parser that crashed on a
          malformed table is a bug of ours in either lane;
        - ``historical_once`` targets only -- on a live target a table-free page
          is how source drift announces itself;
        - ``_CONTRACT_ISOLATABLE_PAGE_KINDS`` only -- retiring a spine page
          would silently drop the subtree it alone discovers (a schedule page is
          the only source of a season's match targets), and retiring a player
          would bury the one row both lanes share;
        - a dated address only -- a season-less squad URL is the club's living
          row, which the current-season lane keeps refreshing;
        - the response must advertise this target's own canonical address.
        """

        if not page.errors:
            return None
        if not all(
            error.startswith("page_contract:") for error in page.errors
        ):
            return None
        frontier = self.control.get_frontier_target(record.target_id) or {}
        if frontier.get("refresh_policy") != "historical_once":
            return None
        malformed_matchlog = (
            recover_cross_run
            and record.page_kind == "matchlog"
            and record.target_id == _LEGACY_INVALID_MATCHLOG_TARGET_ID
            and record.canonical_url == _LEGACY_INVALID_MATCHLOG_URL
            and dict(record.source_ids)
            == _LEGACY_INVALID_MATCHLOG_SOURCE_IDS
        )
        if malformed_matchlog:
            return SourceContractRejected(
                f"Invalid matchlog route for {record.target_id}",
                target_id=record.target_id,
                content_hash=record.content_hash,
                reason="invalid_matchlog_route",
            )
        if record.page_kind not in _CONTRACT_ISOLATABLE_PAGE_KINDS:
            return None
        if not url_addresses_archived_edition(record.canonical_url):
            return None
        if not response_owns_target_page(
            html, canonical_url=record.canonical_url
        ):
            return None
        return SourceContractRejected(
            f"Generic source contract failed for {record.target_id}",
            target_id=record.target_id,
            content_hash=record.content_hash,
            reason=",".join(sorted(set(page.errors))),
        )

    def _without_retired_targets(
        self, fetches: Sequence[Mapping[str, object]]
    ) -> list[Mapping[str, object]]:
        """Remove targets retired after the same-run SQL cohort was selected.

        Ordinary parsing excludes quarantined targets before its bounded query,
        but a target can still be retired between that selection and processing.
        Replays deliberately bypass this guard because a newer parser may need
        to revisit committed retired bytes.
        """

        kept = [
            item
            for item in fetches
            if (
                self.control.get_frontier_target(str(item["target_id"])) or {}
            ).get("state")
            != "quarantined"
        ]
        if len(kept) != len(fetches):
            logger.info(
                "FBref parse cohort skipped %s already retired target(s)",
                len(fetches) - len(kept),
            )
        return kept

    def _failed_claimed_observation(
        self,
        *,
        record: RawFetchRecord,
        page: Optional[PageDocument],
        observation_lease: ObservationLease,
        exc: Exception,
        parse_succeeded: bool = False,
        typed_promoted: int = 0,
        stale_typed_observations_skipped: int = 0,
    ) -> _ProcessedObservation:
        failures: list[str] = []
        if not isinstance(exc, TypedPromotionDeferred):
            try:
                self._record_page_completion(
                    record,
                    page,
                    succeeded=False,
                    error=exc,
                    parse_succeeded=parse_succeeded,
                )
            except StateConflict:
                logger.warning(
                    "Failure completion marker for %s already exists",
                    record.target_id,
                )
            except Exception as manifest_exc:
                failures.append(
                    f"{record.target_id}:manifest:"
                    f"{type(manifest_exc).__name__}:{manifest_exc}"
                )
        try:
            self.control.fail_observation_processing(
                observation_lease,
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
        except Exception as fence_exc:
            failures.append(
                f"{record.target_id}:observation_fence:"
                f"{type(fence_exc).__name__}:{fence_exc}"
            )
        if isinstance(exc, SourceContractRejected):
            retired = False
            try:
                retired = self.control.quarantine_contract_rejected_target(
                    exc.target_id,
                    content_hash=exc.content_hash,
                    reason=exc.reason,
                )
            except Exception as quarantine_exc:
                failures.append(
                    f"{record.target_id}:contract_quarantine:"
                    f"{type(quarantine_exc).__name__}:{quarantine_exc}"
                )
            if retired:
                logger.warning(
                    "Quarantined %s after source contract rejection: %s",
                    exc.target_id,
                    exc.reason,
                )
                return _ProcessedObservation(
                    typed_promoted=typed_promoted,
                    stale_typed_observations_skipped=(
                        stale_typed_observations_skipped
                    ),
                    contract_quarantined=1,
                    failures=tuple(failures),
                )
        failures.append(f"{record.target_id}:{type(exc).__name__}:{exc}")
        return _ProcessedObservation(
            typed_promoted=typed_promoted,
            stale_typed_observations_skipped=(
                stale_typed_observations_skipped
            ),
            failures=tuple(failures),
        )

    def _finish_claimed_observation(
        self,
        *,
        run_id: str,
        html: str,
        record: RawFetchRecord,
        observation_lease: ObservationLease,
        page: PageDocument,
        typed_match: Optional[MatchParseResult],
        typed_context: Optional[TypedSourceContext],
        match_id: Optional[str],
        is_latest: Optional[bool],
        stateful_run_id: str,
        stateful_run_type: str,
        typed_batch_counts: Optional[Mapping[str, int]] = None,
        progress: Optional[dict[str, int]] = None,
    ) -> _ProcessedObservation:
        """Finish one supplied lease under an already-held frontier verdict."""

        typed_page = record.page_kind in {
            "schedule",
            "season",
            "season_stats",
            "match",
        }
        if is_latest is None:
            raise TypedPromotionDeferred(
                f"Stateful promotion deferred for active target {record.target_id}"
            )
        if is_latest:
            historical = self._target_is_historical(
                record,
                run_type=stateful_run_type,
            )
            self._validate_pre_promotion_contract(
                html,
                record,
                historical=historical,
            )
            typed_promoted = 0
            if typed_page:
                if record.page_kind == "match" and typed_match is not None:
                    if typed_context is None or not match_id:
                        raise TypedBronzeError(
                            "Prepared match is missing typed identity"
                        )
                    if typed_batch_counts is None:
                        self._persist_preparsed_typed_match(
                            run_id,
                            record,
                            typed_match,
                            match_id=match_id,
                            context=typed_context,
                        )
                    else:
                        self._record_typed_results(
                            record,
                            typed_match.datasets,
                            persisted=typed_batch_counts,
                        )
                        self._record_typed_completion(record)
                else:
                    if self._typed_context(record) is None:
                        raise TypedBronzeError(
                            "Typed page requires source competition_id and "
                            "season_id"
                        )
                    self._persist_typed(run_id, html, record)
                typed_promoted = 1
                if progress is not None:
                    progress["typed_promoted"] = 1
                typed_status = "succeeded"
            else:
                typed_status = "skipped"
            seeded, skipped = self._apply_stateful_effects(
                stateful_run_id,
                html,
                record,
                run_type=stateful_run_type,
                historical=historical,
            )
            stateful_status = "succeeded"
            stale = 0
        else:
            seeded, skipped = 0, 0
            typed_promoted = 0
            stale = int(typed_page)
            stateful_status = "skipped"
            typed_status = "skipped"
            if typed_page:
                self._record_stale_typed_observation(record)
                if progress is not None:
                    progress["stale_typed_observations_skipped"] = 1
        self._record_page_completion(record, page, succeeded=True)
        self.control.complete_observation_processing(
            observation_lease,
            typed_status=typed_status,
            stateful_status=stateful_status,
        )
        return _ProcessedObservation(
            parsed=1,
            typed_promoted=typed_promoted,
            stale_typed_observations_skipped=stale,
            seeded=seeded,
            skipped_ineligible=skipped,
        )

    def _process_claimed_observation(
        self,
        *,
        run_id: str,
        html: str,
        record: RawFetchRecord,
        observation_lease: ObservationLease,
        page: Optional[PageDocument],
        typed_match: Optional[MatchParseResult],
        stateful_run_id: str,
        stateful_run_type: str,
        typed_context: Optional[TypedSourceContext] = None,
        match_id: Optional[str] = None,
        generic_persisted: bool = False,
        recover_cross_run: bool = False,
    ) -> _ProcessedObservation:
        """Persist and finish an already-claimed observation without I/O."""

        prepared_page = page
        try:
            if prepared_page is None:
                prepared_page = self._parse_generic(html, record)
        except Exception as exc:
            return self._failed_claimed_observation(
                record=record,
                page=prepared_page,
                observation_lease=observation_lease,
                exc=exc,
            )
        try:
            if not generic_persisted:
                self._persist_generic_page(
                    run_id,
                    prepared_page,
                    record,
                    record_failure=False,
                )
        except Exception as exc:
            if isinstance(exc, GenericPersistenceError):
                exc = (
                    self._historical_contract_rejection(
                        html,
                        record,
                        prepared_page,
                        recover_cross_run=recover_cross_run,
                    )
                    or exc
                )
            return self._failed_claimed_observation(
                record=record,
                page=prepared_page,
                observation_lease=observation_lease,
                exc=exc,
                parse_succeeded=not prepared_page.errors,
            )
        try:
            progress: dict[str, int] = {}
            with self.control.guard_latest_content(
                record.target_id,
                record.content_hash,
                record.logical_refresh_id,
            ) as is_latest:
                return self._finish_claimed_observation(
                    run_id=run_id,
                    html=html,
                    record=record,
                    observation_lease=observation_lease,
                    page=prepared_page,
                    typed_match=typed_match,
                    typed_context=typed_context,
                    match_id=match_id,
                    is_latest=is_latest,
                    stateful_run_id=stateful_run_id,
                    stateful_run_type=stateful_run_type,
                    progress=progress,
                )
        except Exception as exc:
            return self._failed_claimed_observation(
                record=record,
                page=prepared_page,
                observation_lease=observation_lease,
                exc=exc,
                typed_promoted=progress.get("typed_promoted", 0),
                stale_typed_observations_skipped=progress.get(
                    "stale_typed_observations_skipped", 0
                ),
            )

    @staticmethod
    def _batch_has_duplicate_identity(
        items: Sequence[_ClaimedMatchObservation],
    ) -> bool:
        target_ids = [item.record.target_id for item in items]
        match_ids = [item.match_id for item in items]
        return len(target_ids) != len(set(target_ids)) or len(match_ids) != len(
            set(match_ids)
        )

    @staticmethod
    def _match_item_cells(item: _ClaimedMatchObservation) -> int:
        # Bound both lossless generic cells and materialized typed dataframe
        # cells. The latter is conservative but prevents a small HTML table
        # inventory from hiding a very wide typed cohort.
        return len(item.page.cell_records()) + sum(
            int(dataset.frame.size)
            for dataset in item.typed_match.datasets.values()
            if dataset.frame is not None
        )

    def _match_batch_cells(
        self, items: Sequence[_ClaimedMatchObservation]
    ) -> int:
        return sum(self._match_item_cells(item) for item in items)

    def _process_claimed_match_batch(
        self, items: Sequence[_ClaimedMatchObservation]
    ) -> list[_ProcessedObservation]:
        materialized = tuple(items)
        if not materialized:
            return []
        if self._batch_has_duplicate_identity(materialized):
            return [
                self._process_claimed_observation(**item.sequential_args())
                for item in materialized
            ]
        outcomes: list[_ProcessedObservation] = []
        cohort: list[_ClaimedMatchObservation] = []
        cohort_cells = 0

        def flush_cohort() -> None:
            nonlocal cohort, cohort_cells
            if cohort:
                outcomes.extend(self._persist_and_finish_match_batch(cohort))
                cohort = []
                cohort_cells = 0

        for item in materialized:
            item_cells = self._match_item_cells(item)
            if item.page.errors or item.typed_match.has_errors:
                flush_cohort()
                outcomes.append(
                    self._process_claimed_observation(
                        **item.sequential_args()
                    )
                )
                continue
            if item_cells > self.batch_persist_max_cells:
                flush_cohort()
                outcomes.append(
                    self._process_claimed_observation(
                        **item.sequential_args()
                    )
                )
                continue
            if cohort and (
                len(cohort) == self.batch_persist_matches
                or cohort_cells + item_cells
                > self.batch_persist_max_cells
            ):
                flush_cohort()
            cohort.append(item)
            cohort_cells += item_cells
        flush_cohort()
        return outcomes

    def _persist_and_finish_match_batch(
        self, items: Sequence[_ClaimedMatchObservation]
    ) -> list[_ProcessedObservation]:
        try:
            generic_counts = self.generic_writer.persist_pages(
                [
                    GenericPagePersistItem(
                        page=item.page,
                        canonical_url=item.record.canonical_url,
                        run_id=item.run_id,
                        staging_identity=item.record.logical_refresh_id,
                    )
                    for item in items
                ]
            )
            if len(generic_counts) != len(items):
                raise ParseWaveError(
                    "Generic batch returned misaligned item counts"
                )
        except Exception:
            # The generic writer may have committed a prefix. Re-run each
            # parsed page idempotently, using the same lease and immutable raw.
            return [
                self._process_claimed_observation(**item.sequential_args())
                for item in items
            ]

        outcomes: dict[str, _ProcessedObservation] = {}
        active_items = []
        for item in items:
            try:
                self._record_generic_table_results(item.record, item.page)
            except Exception as exc:
                outcomes[item.record.logical_refresh_id] = (
                    self._failed_claimed_observation(
                        record=item.record,
                        page=item.page,
                        observation_lease=item.observation_lease,
                        exc=exc,
                    )
                )
            else:
                active_items.append(item)
        finish_failures: list[
            tuple[_ClaimedMatchObservation, Exception, dict[str, int]]
        ] = []
        lock_errors: list[Exception] = []
        with _captured_exit_stack(lock_errors) as stack:
            verdicts = {
                target_id: stack.enter_context(
                    self.control.guard_latest_content(
                        item.record.target_id,
                        item.record.content_hash,
                        item.record.logical_refresh_id,
                    )
                )
                for target_id, item in sorted(
                    (
                        (item.record.target_id, item)
                        for item in active_items
                    ),
                    key=lambda pair: pair[0],
                )
            }
            eligible = [
                item
                for item in active_items
                if verdicts[item.record.target_id] is True
            ]
            counts_by_refresh: dict[str, Mapping[str, int]] = {}
            batch_error: Optional[Exception] = None
            if eligible:
                try:
                    batch_counts = self.typed_adapter.writer.persist_matches(
                        [
                            TypedMatchPersistItem(
                                parsed=item.typed_match,
                                match_id=item.match_id,
                                context=item.typed_context,
                                run_id=item.run_id,
                                target_identity=(
                                    item.record.logical_refresh_id
                                ),
                            )
                            for item in eligible
                        ]
                    )
                    if len(batch_counts) != len(eligible):
                        raise TypedBronzeError(
                            "Typed batch returned misaligned item counts"
                        )
                    counts_by_refresh = {
                        item.record.logical_refresh_id: count
                        for item, count in zip(eligible, batch_counts)
                    }
                except Exception as exc:
                    batch_error = exc

            for item in active_items:
                verdict = verdicts[item.record.target_id]
                if verdict is None:
                    finish_failures.append((
                        item,
                        TypedPromotionDeferred(
                            "Stateful promotion deferred for active target "
                            f"{item.record.target_id}"
                        ),
                        {},
                    ))
                    continue
                try:
                    # A typed batch error is repaired under the same captured
                    # verdict and locks. Re-entering the guard would deadlock;
                    # releasing first would let a newer raw commit race the
                    # repair of partially replaced old typed tables.
                    typed_counts = (
                        None
                        if batch_error is not None
                        else counts_by_refresh.get(
                            item.record.logical_refresh_id
                        )
                    )
                    progress: dict[str, int] = {}
                    outcomes[item.record.logical_refresh_id] = (
                        self._finish_claimed_observation(
                            run_id=item.run_id,
                            html=item.html,
                            record=item.record,
                            observation_lease=item.observation_lease,
                            page=item.page,
                            typed_match=item.typed_match,
                            typed_context=item.typed_context,
                            match_id=item.match_id,
                            is_latest=verdict,
                            stateful_run_id=item.stateful_run_id,
                            stateful_run_type=item.stateful_run_type,
                            typed_batch_counts=typed_counts,
                            progress=progress,
                        )
                    )
                except Exception as exc:
                    finish_failures.append((item, exc, progress))

        if lock_errors:
            scheduled = {
                item.record.logical_refresh_id
                for item, _exc, _progress in finish_failures
            }
            unfinished = [
                item
                for item in active_items
                if item.record.logical_refresh_id not in outcomes
                and item.record.logical_refresh_id not in scheduled
            ]
            for item in unfinished:
                finish_failures.append((item, lock_errors[0], {}))
            if not unfinished and outcomes:
                # An exit failure after durable completion cannot safely turn
                # a succeeded lease back into failed. Keep its state, but make
                # the wave report the frontier-fence failure loudly.
                first_key = next(iter(outcomes))
                first = outcomes[first_key]
                lock_error = lock_errors[0]
                outcomes[first_key] = replace(
                    first,
                    failures=first.failures
                    + (
                        f"{items[0].record.target_id}:content_guard:"
                        f"{type(lock_error).__name__}:{lock_error}",
                    ),
                )

        # Failure fencing uses its own control transaction, so it must happen
        # only after every frontier lock is gone. This includes ordinary
        # per-item failures as well as deferred latest verdicts.
        for item, exc, progress in finish_failures:
            outcomes[item.record.logical_refresh_id] = (
                self._failed_claimed_observation(
                    record=item.record,
                    page=item.page,
                    observation_lease=item.observation_lease,
                    exc=exc,
                    typed_promoted=progress.get("typed_promoted", 0),
                    stale_typed_observations_skipped=progress.get(
                        "stale_typed_observations_skipped", 0
                    ),
                )
            )
        return [outcomes[item.record.logical_refresh_id] for item in items]

    def _load_and_claim_observation(
        self, item: Mapping[str, object]
    ) -> Optional[tuple[str, RawFetchRecord, ObservationLease]]:
        logical_refresh_id = str(item["logical_refresh_id"])
        html, record = self.raw_store.load_fetch_html(logical_refresh_id)
        if record.logical_refresh_id != logical_refresh_id:
            raise ParseWaveError(
                f"Raw/control refresh mismatch for {logical_refresh_id}"
            )
        if record.target_id != str(item["target_id"]):
            raise ParseWaveError(
                f"Raw/control target mismatch for {logical_refresh_id}"
            )
        if item.get("content_hash") and record.content_hash != str(
            item["content_hash"]
        ):
            raise ParseWaveError(
                f"Raw/control content mismatch for {logical_refresh_id}"
            )
        lease = self.control.claim_observation_processing(
            logical_refresh_id=logical_refresh_id,
            target_id=record.target_id,
            content_hash=record.content_hash,
            parser_version=PAGE_DOCUMENT_VERSION,
            typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
            stateful_parser_version=DISCOVERY_PARSER_VERSION,
            lease_seconds=PROCESSING_LEASE_SECONDS,
        )
        if lease is None:
            return None
        return html, record, lease

    @staticmethod
    def _merge_processed_result(
        result: WaveResult, outcome: _ProcessedObservation
    ) -> None:
        result.parsed += outcome.parsed
        result.typed_promoted += outcome.typed_promoted
        result.stale_typed_observations_skipped += (
            outcome.stale_typed_observations_skipped
        )
        result.seeded += outcome.seeded
        result.skipped_ineligible += outcome.skipped_ineligible
        result.contract_quarantined += outcome.contract_quarantined
        result.failures.extend(outcome.failures)

    def _prepare_acceptance_replay_match(
        self, item: Mapping[str, object]
    ) -> _AcceptanceReplayMatch:
        """Load and parse frozen raw without touching global parse fences."""

        logical_refresh_id = str(item["logical_refresh_id"])
        html, record = self.raw_store.load_fetch_html(logical_refresh_id)
        if (
            record.logical_refresh_id != logical_refresh_id
            or record.target_id != str(item["target_id"])
            or record.page_kind != "match"
            or (
                item.get("content_hash")
                and record.content_hash != str(item["content_hash"])
            )
        ):
            raise ParseWaveError(
                f"Acceptance replay raw/control mismatch for {logical_refresh_id}"
            )
        page = self._parse_generic(html, record)
        typed_match, match_id, typed_context = self._parse_typed_match(
            html, record
        )
        if (
            page.errors
            or typed_match.has_errors
            or record.target_id != f"fbref:match:{match_id}"
        ):
            raise ParseWaveError(
                f"Acceptance replay match contract failed for {record.target_id}"
            )
        return _AcceptanceReplayMatch(
            record=record,
            page=page,
            typed_match=typed_match,
            typed_context=typed_context,
            match_id=match_id,
        )

    def _persist_acceptance_replay_match(
        self,
        run_id: str,
        item: _AcceptanceReplayMatch,
        *,
        replay_control,
    ) -> None:
        with replay_control.guard_latest_content(
            item.record.target_id,
            item.record.content_hash,
            item.record.logical_refresh_id,
        ) as latest:
            if latest is not True:
                raise ParseWaveError(
                    "acceptance_replay_control_latest_guard_failed"
                )
            self.generic_writer.persist_page(
                item.page,
                canonical_url=item.record.canonical_url,
                run_id=run_id,
                staging_identity=item.record.logical_refresh_id,
            )
            self._record_generic_table_results(
                item.record, item.page, control=replay_control
            )
            try:
                typed_counts = self.typed_adapter.writer.persist_match(
                    item.typed_match,
                    match_id=item.match_id,
                    context=item.typed_context,
                    run_id=run_id,
                    target_identity=item.record.logical_refresh_id,
                )
            except Exception:
                self._record_typed_results(
                    item.record,
                    item.typed_match.datasets,
                    persisted=None,
                    control=replay_control,
                )
                raise
            self._finish_acceptance_replay_control(
                item, typed_counts, replay_control=replay_control
            )

    def _finish_acceptance_replay_control(
        self,
        item: _AcceptanceReplayMatch,
        typed_counts: Mapping[str, int],
        *,
        replay_control,
    ) -> None:
        self._record_typed_results(
            item.record,
            item.typed_match.datasets,
            persisted=typed_counts,
            control=replay_control,
        )
        self._record_typed_completion(
            item.record, control=replay_control
        )
        self._record_page_completion(
            item.record,
            item.page,
            succeeded=True,
            control=replay_control,
        )
        replay_control.complete_observation_processing(
            replay_control.observation_lease(
                item.record.logical_refresh_id
            ),
            typed_status="succeeded",
            stateful_status="skipped",
        )

    def _persist_acceptance_replay_match_batch(
        self,
        run_id: str,
        items: Sequence[_AcceptanceReplayMatch],
        *,
        replay_control,
    ) -> None:
        lock_errors: list[Exception] = []
        with _captured_exit_stack(lock_errors) as stack:
            verdicts = {
                item.record.target_id: stack.enter_context(
                    replay_control.guard_latest_content(
                        item.record.target_id,
                        item.record.content_hash,
                        item.record.logical_refresh_id,
                    )
                )
                for item in sorted(
                    items, key=lambda value: value.record.target_id
                )
            }
            if any(verdict is not True for verdict in verdicts.values()):
                raise ParseWaveError(
                    "acceptance_replay_control_latest_guard_failed"
                )
            generic_counts = self.generic_writer.persist_pages(
                [
                    GenericPagePersistItem(
                        page=item.page,
                        canonical_url=item.record.canonical_url,
                        run_id=run_id,
                        staging_identity=item.record.logical_refresh_id,
                    )
                    for item in items
                ]
            )
            if len(generic_counts) != len(items):
                raise ParseWaveError(
                    "Acceptance generic batch returned misaligned item counts"
                )
            for item in items:
                self._record_generic_table_results(
                    item.record, item.page, control=replay_control
                )
            try:
                typed_counts = self.typed_adapter.writer.persist_matches(
                    [
                        TypedMatchPersistItem(
                            parsed=item.typed_match,
                            match_id=item.match_id,
                            context=item.typed_context,
                            run_id=run_id,
                            target_identity=(
                                item.record.logical_refresh_id
                            ),
                        )
                        for item in items
                    ]
                )
            except Exception:
                for item in items:
                    self._record_typed_results(
                        item.record,
                        item.typed_match.datasets,
                        persisted=None,
                        control=replay_control,
                    )
                raise
            if len(typed_counts) != len(items):
                raise TypedBronzeError(
                    "Acceptance typed batch returned misaligned item counts"
                )
            for item, counts in zip(items, typed_counts):
                self._finish_acceptance_replay_control(
                    item, counts, replay_control=replay_control
                )
        if lock_errors:
            raise lock_errors[0]

    def replay_acceptance_matches(
        self,
        run_id: str,
        *,
        source_run_id: str,
        settings: PipelineSettings,
    ) -> WaveResult:
        """Force one isolated zero-network replay of the frozen match cohort.

        The accepted source already owns successful global observation fences.
        Reusing the ordinary replay selector would therefore select nothing.
        This path deliberately reads every frozen successful match attempt and
        writes only the isolated Trino outputs under ``run_id``.  Real control
        methods run on replay-scoped clones in a rollback-only transaction, so
        source observations, manifests, discovery, and frontier stay unchanged.
        """

        if (
            settings.run_type != "replay"
            or settings.request_limit != 0
            or settings.byte_limit != 0
            or settings.shard_size != ACCEPTANCE_SHARD_SIZE
        ):
            raise ParseWaveError("acceptance_replay_profile_invalid")
        source_error = self._acceptance_replay_source_error(source_run_id)
        if source_error is not None:
            raise ParseWaveError(source_error)
        source_run = self.control.get_run(source_run_id)
        metadata = source_run.get("metadata") or {}
        marker = metadata.get("bronze_acceptance") or {}
        page_kind_counts = marker.get("page_kind_counts") or {}
        try:
            expected_matches = int(page_kind_counts.get("match"))
        except (TypeError, ValueError):
            expected_matches = 0
        if not 1 <= expected_matches <= settings.shard_size:
            raise ParseWaveError("acceptance_replay_match_cohort_invalid")
        cohort = metadata.get("acceptance_cohort") or {}
        coverage_slots = (
            cohort.get("coverage_slots")
            if isinstance(cohort, Mapping)
            else None
        )
        if not isinstance(coverage_slots, Mapping):
            raise ParseWaveError(
                "acceptance_replay_match_classification_missing"
            )
        match_evidence_classes: dict[str, str] = {}
        for slot, evidence_class in (
            ("match_full", "full_match"),
            ("match_sparse", "sparse_match"),
        ):
            target_id = str(coverage_slots.get(slot) or "").strip()
            if not target_id or target_id in match_evidence_classes:
                raise ParseWaveError(
                    "acceptance_replay_match_classification_invalid"
                )
            match_evidence_classes[target_id] = evidence_class

        with self.control.guard_publication_lock(run_id, source="fbref"):
            fetches = self.control.list_run_fetches(
                source_run_id,
                page_kinds=["match"],
                only_unparsed=False,
                limit=settings.shard_size,
            )
            if len(fetches) != expected_matches:
                raise ParseWaveError(
                    "acceptance_replay_match_cohort_mismatch"
                )
            prepared = [
                self._prepare_acceptance_replay_match(item)
                for item in fetches
            ]
            target_ids = [item.record.target_id for item in prepared]
            match_ids = [item.match_id for item in prepared]
            if (
                len(target_ids) != len(set(target_ids))
                or len(match_ids) != len(set(match_ids))
            ):
                raise ParseWaveError(
                    "acceptance_replay_match_identity_duplicate"
                )
            if set(target_ids) != set(match_evidence_classes):
                raise ParseWaveError(
                    "acceptance_replay_match_classification_mismatch"
                )

            mode = (
                "batch" if self.batch_persist_enabled else "sequential"
            )
            control_targets = [
                {
                    "ordinal": ordinal,
                    "target_id": item.record.target_id,
                    "logical_refresh_id": item.record.logical_refresh_id,
                    "page_kind": item.record.page_kind,
                    "source_ids": dict(item.record.source_ids),
                    "canonical_url": item.record.canonical_url,
                    "content_hash": item.record.content_hash,
                    "parser_version": PAGE_DOCUMENT_VERSION,
                    "typed_parser_version": TYPED_BRONZE_PARSER_VERSION,
                    "stateful_parser_version": DISCOVERY_PARSER_VERSION,
                    "evidence_class": match_evidence_classes[
                        item.record.target_id
                    ],
                }
                for ordinal, item in enumerate(prepared)
            ]
            with self.control.replay_control_transaction(
                replay_run_id=run_id,
                source_run_id=source_run_id,
                mode=mode,
                targets=control_targets,
            ) as replay_control:
                if not self.batch_persist_enabled:
                    for item in prepared:
                        self._persist_acceptance_replay_match(
                            run_id, item, replay_control=replay_control
                        )
                else:
                    batch: list[_AcceptanceReplayMatch] = []
                    batch_cells = 0

                    def flush() -> None:
                        nonlocal batch, batch_cells
                        if batch:
                            self._persist_acceptance_replay_match_batch(
                                run_id,
                                batch,
                                replay_control=replay_control,
                            )
                            batch = []
                            batch_cells = 0

                    for item in prepared:
                        item_cells = len(item.page.cell_records()) + sum(
                            int(dataset.frame.size)
                            for dataset in item.typed_match.datasets.values()
                            if dataset.frame is not None
                        )
                        if item_cells > self.batch_persist_max_cells:
                            flush()
                            self._persist_acceptance_replay_match(
                                run_id,
                                item,
                                replay_control=replay_control,
                            )
                            continue
                        if batch and (
                            len(batch) == self.batch_persist_matches
                            or batch_cells + item_cells
                            > self.batch_persist_max_cells
                        ):
                            flush()
                        batch.append(item)
                        batch_cells += item_cells
                    flush()

        return WaveResult(
            cohort_size=len(prepared),
            claimed=len(prepared),
            parsed=len(prepared),
            typed_promoted=len(prepared),
        )

    def parse_wave(
        self,
        run_id: str,
        *,
        page_kinds: Sequence[str],
        settings: PipelineSettings,
        source_run_id: Optional[str] = None,
        acceptance_replay: bool = False,
        _recover_cross_run: bool = False,
    ) -> WaveResult:
        """Parse raw under a database-held publication-generation fence."""

        with self.control.guard_publication_lock(run_id, source="fbref"):
            return self._parse_wave_under_publication_guard(
                run_id,
                page_kinds=page_kinds,
                settings=settings,
                source_run_id=source_run_id,
                acceptance_replay=acceptance_replay,
                _recover_cross_run=_recover_cross_run,
            )

    def _parse_wave_under_publication_guard(
        self,
        run_id: str,
        *,
        page_kinds: Sequence[str],
        settings: PipelineSettings,
        source_run_id: Optional[str] = None,
        acceptance_replay: bool = False,
        _recover_cross_run: bool = False,
    ) -> WaveResult:
        """Parse and persist a bounded handoff using raw storage only."""

        result = WaveResult()
        stateful_run_id = run_id
        stateful_run_type = settings.run_type
        if acceptance_replay and settings.run_type != "replay":
            raise ParseWaveError("acceptance_replay_requires_replay_mode")
        if settings.run_type == "replay":
            source_error = (
                self._acceptance_replay_source_error(source_run_id)
                if acceptance_replay
                else self._replay_source_error(source_run_id)
            )
            if source_error:
                raise ParseWaveError(source_error)
            source_run = self.control.get_run(source_run_id)
            stateful_run_id = str(source_run_id)
            stateful_run_type = str(source_run["run_type"])
        if _recover_cross_run:
            if settings.run_type == "replay" or source_run_id is not None:
                raise ParseWaveError(
                    "Cross-run recovery is not a replay source selector"
                )
            fetches = self.control.list_unprocessed_fetches(
                parser_version=PAGE_DOCUMENT_VERSION,
                typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
                stateful_parser_version=DISCOVERY_PARSER_VERSION,
                page_kinds=page_kinds,
                limit=settings.shard_size,
            )
        elif source_run_id:
            fetches = self.control.list_replay_fetches(
                source_run_id,
                parser_version=PAGE_DOCUMENT_VERSION,
                typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
                stateful_parser_version=DISCOVERY_PARSER_VERSION,
                page_kinds=page_kinds,
                limit=settings.shard_size,
            )
        else:
            fetches = self.control.list_run_fetches(
                run_id,
                page_kinds=page_kinds,
                only_unparsed=True,
                parser_version=PAGE_DOCUMENT_VERSION,
                typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
                stateful_parser_version=DISCOVERY_PARSER_VERSION,
                include_quarantined=False,
                limit=settings.shard_size,
            )
            # SQL selection skips retired targets before the cohort limit. This
            # guard still protects against a retirement racing selection.
            fetches = self._without_retired_targets(fetches)
        result.cohort_size = len(fetches)

        def stateful_identity(item):
            item_stateful_run_id = stateful_run_id
            item_stateful_run_type = stateful_run_type
            if _recover_cross_run:
                item_stateful_run_id = str(item["run_id"])
                item_stateful_run_type = str(
                    item.get("source_run_type")
                    or (
                        self.control.get_run(item_stateful_run_id) or {}
                    ).get("run_type")
                    or "current"
                )
            return item_stateful_run_id, item_stateful_run_type

        def process_sequential_item(item):
            try:
                claimed = self._load_and_claim_observation(item)
            except Exception as exc:
                result.failures.append(
                    f"{item['target_id']}:{type(exc).__name__}:{exc}"
                )
                return
            if claimed is None:
                return
            result.claimed += 1
            html, record, observation_lease = claimed
            item_run_id, item_run_type = stateful_identity(item)
            outcome = self._process_claimed_observation(
                run_id=run_id,
                html=html,
                record=record,
                observation_lease=observation_lease,
                page=None,
                typed_match=None,
                stateful_run_id=item_run_id,
                stateful_run_type=item_run_type,
                recover_cross_run=_recover_cross_run,
            )
            self._merge_processed_result(result, outcome)

        def flush_match_items(buffer):
            prepared: list[_ClaimedMatchObservation] = []
            for item in buffer:
                try:
                    claimed = self._load_and_claim_observation(item)
                except Exception as exc:
                    result.failures.append(
                        f"{item['target_id']}:{type(exc).__name__}:{exc}"
                    )
                    continue
                if claimed is None:
                    continue
                result.claimed += 1
                html, record, observation_lease = claimed
                item_run_id, item_run_type = stateful_identity(item)
                page: Optional[PageDocument] = None
                try:
                    page = self._parse_generic(html, record)
                    typed_match, match_id, typed_context = (
                        self._parse_typed_match(html, record)
                    )
                except Exception:
                    # Preparation validation failed. Preserve the legacy
                    # sequential evidence path using this same lease/raw.
                    outcome = self._process_claimed_observation(
                        run_id=run_id,
                        html=html,
                        record=record,
                        observation_lease=observation_lease,
                        page=page,
                        typed_match=None,
                        stateful_run_id=item_run_id,
                        stateful_run_type=item_run_type,
                        recover_cross_run=_recover_cross_run,
                    )
                    self._merge_processed_result(result, outcome)
                    continue
                prepared.append(_ClaimedMatchObservation(
                    run_id=run_id,
                    html=html,
                    record=record,
                    observation_lease=observation_lease,
                    page=page,
                    typed_match=typed_match,
                    typed_context=typed_context,
                    match_id=match_id,
                    stateful_run_id=item_run_id,
                    stateful_run_type=item_run_type,
                ))
            for outcome in self._process_claimed_match_batch(prepared):
                self._merge_processed_result(result, outcome)

        if not self.batch_persist_enabled:
            for item in fetches:
                process_sequential_item(item)
        else:
            match_buffer = []
            for item in fetches:
                if str(item.get("page_kind") or "") == "match":
                    match_buffer.append(item)
                    if len(match_buffer) == self.batch_persist_matches:
                        flush_match_items(match_buffer)
                        match_buffer = []
                    continue
                if match_buffer:
                    flush_match_items(match_buffer)
                    match_buffer = []
                process_sequential_item(item)
            if match_buffer:
                flush_match_items(match_buffer)
        if result.failures:
            raise ParseWaveError("; ".join(result.failures))
        if _is_mass_contract_rejection(result):
            # Retiring a handful of archived editions is routine.  Retiring the
            # bulk of a live cohort is not a property of those pages -- it is
            # the source's markup having moved under the parser -- and silently
            # shrinking the crawl scope is the one outcome worse than stopping.
            raise ParseWaveError(
                "Mass source contract rejection: "
                f"{result.contract_quarantined} of {result.cohort_size} "
                "targets retired in one wave"
            )
        return result

    def recover_unprocessed_wave(
        self,
        run_id: str,
        *,
        page_kinds: Sequence[str],
        settings: PipelineSettings,
    ) -> WaveResult:
        """Drain committed raw left unprocessed by any earlier source run.

        This is deliberately invoked before a current/backfill fetch wave so
        parse failure can never strand immutable S3 raw behind a terminal
        parent run or trigger a needless paid re-fetch.
        """

        return self.parse_wave(
            run_id,
            page_kinds=page_kinds,
            settings=settings,
            _recover_cross_run=True,
        )

    def validate_and_finish(
        self,
        run_id: str,
        *,
        replay_source_run_id: Optional[str] = None,
        publication_eligible: bool = True,
        acceptance: bool = False,
        acceptance_replay: bool = False,
    ) -> dict:
        live_acceptance = bool(acceptance and not acceptance_replay)
        isolated_acceptance = bool(acceptance or acceptance_replay)
        if acceptance_replay and replay_source_run_id is None:
            raise RunValidationError(
                "acceptance_replay_source_run_id_missing"
            )
        if acceptance or acceptance_replay:
            # Acceptance is physically nonpublishing regardless of a caller's
            # generic default.  Durable run metadata is checked below too.
            publication_eligible = False
        summary = self.control.get_run_summary(
            run_id,
            parser_version=PAGE_DOCUMENT_VERSION,
            typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
            stateful_parser_version=DISCOVERY_PARSER_VERSION,
        )
        if summary is None:
            raise RunValidationError(f"Unknown run {run_id}")
        summary_metadata = summary.get("metadata") or {}
        if isinstance(summary_metadata, str):
            try:
                summary_metadata = json.loads(summary_metadata)
            except json.JSONDecodeError as exc:
                raise RunValidationError(
                    "persistent_http_profile_invalid"
                ) from exc
        if not isinstance(summary_metadata, Mapping):
            raise RunValidationError("persistent_http_profile_invalid")
        persistent_marker = summary_metadata.get(
            "persistent_http_session"
        )
        if persistent_marker is True:
            self.control.assert_persistent_metering_reconciled(run_id)
        elif persistent_marker is not None and persistent_marker is not False:
            raise RunValidationError("persistent_http_profile_invalid")
        target_counts = summary.get("target_counts") or {}
        # 'skipped' is a target the run deliberately did not fetch — it stopped
        # at its budget and handed the target back to the queue. That is the
        # designed steady state of a budgeted crawler, not an incomplete run.
        # 'failed' mirrors the wave gate (#1102): reanimation already returned
        # every claimable aborted target to 'retry', so a remaining 'failed'
        # has no living page behind it and this run cannot act on it any more.
        # It is not "incomplete" here either — the loss is logged loudly and a
        # genuinely unhealthy run is still rejected by the traffic gates below.
        incomplete = {
            status: count
            for status, count in target_counts.items()
            if status not in {"succeeded", "skipped", "failed"}
            and int(count) > 0
        }
        if int(target_counts.get("failed") or 0) > 0:
            logger.warning(
                "Run %s finishes with %d terminally failed target(s); this "
                "run will not refetch them — each page is either "
                "dead-lettered, backed off, or already handled by another "
                "run (frontier state is authoritative)",
                run_id,
                int(target_counts.get("failed") or 0),
            )
        dataset_counts = summary.get("dataset_validation_counts") or {}
        dataset_failures = sum(
            int(count)
            for status, count in dataset_counts.items()
            if status not in {"succeeded", "skipped"}
        )
        errors = []
        warnings: dict[str, object] = {}
        traffic = summary.get("traffic_totals") or {}
        network_attempts = int(traffic.get("network_attempts") or 0)
        warm_successes = int(traffic.get("warm_http_successes") or 0)
        success_rate = traffic.get("warm_http_success_rate")
        durable_progress = int(target_counts.get("succeeded") or 0)
        cohort_work = sum(int(count or 0) for count in target_counts.values())
        unresolved_claims = bool(incomplete) or any(
            int(summary.get(metric) or 0) != 0
            for metric in ("requests_reserved", "bytes_reserved")
        )
        productive_recoverable = (
            durable_progress > 0
            and not unresolved_claims
            and int(summary.get("unprocessed_raw_count") or 0) == 0
        )
        if live_acceptance:
            errors.extend(_acceptance_summary_errors(summary))
            raw_error = _accepted_raw_audit_error(run_id, summary)
            if raw_error is not None:
                errors.append(raw_error)
            metadata = summary.get("metadata")
            cohort_anchor = (
                metadata.get("acceptance_cohort")
                if isinstance(metadata, Mapping)
                else None
            )
            if not isinstance(cohort_anchor, Mapping):
                errors.append("acceptance_cohort_missing")
            else:
                target_total = sum(
                    int(count or 0) for count in target_counts.values()
                )
                if int(cohort_anchor.get("cohort_size") or 0) != target_total:
                    errors.append("acceptance_cohort_size_mismatch")
                page_kind_total = sum(
                    int(count or 0)
                    for count in (
                        summary.get("cohort_page_kind_counts") or {}
                    ).values()
                )
                route_total = sum(
                    int(count or 0)
                    for count in (
                        summary.get("cohort_route_counts") or {}
                    ).values()
                )
                if page_kind_total != target_total:
                    errors.append("acceptance_page_kind_counts_mismatch")
                if route_total != target_total:
                    errors.append("acceptance_route_counts_mismatch")
                evidence = self.control.get_acceptance_run_evidence(run_id)
                errors.extend(
                    _acceptance_coverage_errors(cohort_anchor, evidence)
                )
        if acceptance_replay:
            replay_metadata = summary.get("metadata")
            if (
                str(summary.get("run_type") or "").casefold() != "replay"
                or int(summary.get("request_limit", -1)) != 0
                or int(summary.get("byte_limit", -1)) != 0
                or not isinstance(replay_metadata, Mapping)
                or replay_metadata.get("acceptance_replay") is not True
                or str(replay_metadata.get("execution_mode") or "").casefold()
                != "acceptance_replay_nonpublishing"
                or replay_metadata.get("publication_eligible") is not False
                or str(
                    replay_metadata.get("acceptance_replay_source_run_id") or ""
                )
                != str(replay_source_run_id)
            ):
                errors.append("acceptance_replay_profile_invalid")
            replay_raw_error = _accepted_replay_raw_audit_error(
                run_id, replay_source_run_id, summary
            )
            if replay_raw_error is not None:
                errors.append(replay_raw_error)
        if incomplete:
            errors.append(f"incomplete_targets={incomplete}")
        if dataset_failures:
            errors.append(f"failed_dataset_manifests={dataset_failures}")
        if "unvalidated_target_count" not in summary:
            errors.append("unvalidated_target_count_missing")
        elif int(summary.get("unvalidated_target_count") or 0) != 0:
            errors.append(
                "unvalidated_target_count="
                f"{int(summary['unvalidated_target_count'])}"
            )
        if (
            publication_eligible
            and str(summary.get("run_type") or "").casefold() == "current"
            and int(summary.get("promotion_pending_match_count") or 0) != 0
        ):
            pending_matches = int(summary["promotion_pending_match_count"])
            if productive_recoverable:
                warnings["promotion_pending_match_count"] = pending_matches
                logger.warning(
                    "Run %s completed durable work with %d recoverable "
                    "promotion-pending match(es)",
                    run_id,
                    pending_matches,
                )
            else:
                errors.append(
                    "promotion_pending_match_count="
                    f"{pending_matches}"
                )
        if bool(summary.get("budget_exceeded")):
            errors.append("budget_exceeded=true")
        for reservation_metric in ("requests_reserved", "bytes_reserved"):
            reservation_value = int(summary.get(reservation_metric) or 0)
            if reservation_value != 0:
                errors.append(
                    f"{reservation_metric}={reservation_value}"
                )
        request_limit = int(summary.get("request_limit") or 0)
        byte_limit = int(summary.get("byte_limit") or 0)
        requests_used = int(summary.get("requests_used") or 0)
        bytes_used = int(summary.get("bytes_used") or 0)
        production_safety_profile = (
            str(summary.get("run_type") or "").casefold()
            in {"current", "backfill"}
            and request_limit == DEFAULT_REQUEST_LIMIT
            and byte_limit == DEFAULT_BYTE_LIMIT
        )
        if production_safety_profile and (
            requests_used >= request_limit or bytes_used >= byte_limit
        ):
            errors.append("production_safety_circuit_reached")
        else:
            if requests_used > request_limit:
                errors.append("request_limit_exceeded")
            if bytes_used > byte_limit:
                errors.append("byte_limit_exceeded")
        if not isolated_acceptance and str(
            summary.get("run_type") or ""
        ).casefold() != "replay":
            if network_attempts > 0 and warm_successes == 0:
                errors.append("zero warm HTTP successes after network attempts")
            if network_attempts > 0 and success_rate is None:
                errors.append("warm_http_success_rate_missing")
            elif success_rate is not None and float(success_rate) < 0.5:
                errors.append(
                    f"warm_http_success_rate={float(success_rate):.4f}<0.5"
                )
            elif success_rate is not None and float(success_rate) < 0.95:
                if durable_progress > 0 and not unresolved_claims:
                    warnings["warm_http_success_rate"] = float(success_rate)
                    logger.warning(
                        "Run %s completed durable work with partial warm HTTP "
                        "success rate %.4f",
                        run_id,
                        float(success_rate),
                    )
                else:
                    errors.append(
                        "partial_warm_http_success_without_recoverable_progress="
                        f"{float(success_rate):.4f}"
                    )
            if (
                durable_progress <= 0
                and (network_attempts > 0 or cohort_work > 0)
            ):
                errors.append("no_durable_progress_after_claimed_work")
        if int(traffic.get("unclassified_failures") or 0) != 0:
            errors.append(
                "unclassified_failures="
                f"{int(traffic['unclassified_failures'])}"
            )
        if int(traffic.get("duplicate_fetch_violations") or 0) != 0:
            errors.append(
                "duplicate_fetch_violations="
                f"{int(traffic['duplicate_fetch_violations'])}"
            )
        sessions = summary.get("session_metrics") or {}
        # The invariant is that the browser establishes ONE clearance per
        # session and every page then rides the warm HTTP path — a regression
        # that drove the browser per page would show one attempt per page. A
        # stalled exit IP legitimately costs a re-solve on a fresh proxy, which
        # the transport bounds at MAX_CLEARANCE_SOLVE_ATTEMPTS; demanding a
        # single attempt failed a run whose only sin was surviving a bad proxy.
        if int(sessions.get("max_bootstraps_per_session") or 0) > (
            MAX_CLEARANCE_SOLVE_ATTEMPTS
        ):
            errors.append("browser_bootstrap_exceeded_per_session")
        if str(summary.get("run_type")) == "replay" and (
            int(traffic.get("network_attempts") or 0) != 0
            or int(summary.get("bytes_used") or 0) != 0
        ):
            errors.append("replay_generated_proxy_traffic")
        if str(summary.get("run_type")) == "replay":
            source_summary = (
                self.control.get_run_summary(replay_source_run_id)
                if replay_source_run_id
                else None
            )
            if (
                not acceptance_replay
                and source_summary
                and int(source_summary.get("request_limit") or 0) == 100
            ):
                errors.append("replay_source_canary_not_publication_eligible")
            source_error = (
                self._acceptance_replay_source_error(replay_source_run_id)
                if acceptance_replay
                else self._replay_source_error(replay_source_run_id)
            )
            if source_error:
                errors.append(source_error)
            elif self.control.list_replay_fetches(
                replay_source_run_id,
                parser_version=PAGE_DOCUMENT_VERSION,
                typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
                stateful_parser_version=DISCOVERY_PARSER_VERSION,
                limit=1,
            ):
                errors.append("replay_candidates_remaining")
        if not isolated_acceptance:
            if int(summary.get("female_downstream_targets") or 0) != 0:
                errors.append("female_downstream_targets_nonzero")
            if int(summary.get("unknown_gender_downstream_targets") or 0) != 0:
                errors.append("unknown_gender_downstream_targets_nonzero")
            if "unknown_gender_registry_count" not in summary:
                errors.append("unknown_gender_registry_count_missing")
            elif int(summary.get("unknown_gender_registry_count") or 0) != 0:
                errors.append(
                    "unknown_gender_registry_count="
                    f"{int(summary['unknown_gender_registry_count'])}"
                )
        if "unprocessed_raw_count" not in summary:
            errors.append("unprocessed_raw_count_missing")
        elif int(summary.get("unprocessed_raw_count") or 0) != 0:
            errors.append(
                "unprocessed_raw_count="
                f"{int(summary['unprocessed_raw_count'])}"
            )
        if not isolated_acceptance:
            if "global_unprocessed_raw_sla_overdue_count" not in summary:
                errors.append("global_unprocessed_raw_sla_overdue_count_missing")
            elif int(
                summary.get("global_unprocessed_raw_sla_overdue_count") or 0
            ) != 0:
                errors.append(
                    "global_unprocessed_raw_sla_overdue_count="
                    f"{int(summary['global_unprocessed_raw_sla_overdue_count'])}"
                )

        if not isolated_acceptance:
            crawlable_scope = summary.get("crawlable_frontier_scope_counts")
            if not isinstance(crawlable_scope, Mapping):
                errors.append("crawlable_frontier_scope_counts_missing")
            else:
                invalid_crawlable = {
                    str(status): int(count)
                    for status, count in crawlable_scope.items()
                    if status != "eligible_male" and int(count) > 0
                }
                if invalid_crawlable:
                    errors.append(
                        f"crawlable_out_of_scope_targets={invalid_crawlable}"
                    )
                if int(crawlable_scope.get("eligible_male") or 0) <= 0:
                    errors.append("crawlable_male_scope_empty")

        if publication_eligible and str(summary.get("run_type") or "").lower() != "replay":
            freshness = summary.get("publication_scope_freshness")
            freshness_label = "publication_scope"
            if not isinstance(freshness, Mapping):
                freshness = summary.get("current_scope_freshness")
                freshness_label = "current_scope"
            if not isinstance(freshness, Mapping):
                errors.append("publication_scope_freshness_missing")
            else:
                if int(freshness.get("total_targets") or 0) <= 0:
                    errors.append("publication_scope_freshness_empty")
                if not bool(freshness.get("all_within_sla")):
                    errors.append(
                        f"{freshness_label}_stale_targets="
                        f"{int(freshness.get('stale_targets') or 0)}"
                    )
        if (
            not isolated_acceptance
            and str(summary.get("run_type") or "").lower() != "replay"
        ):
            errors.extend(
                _sentinel_gate_errors(summary.get("sentinel_coverage"))
            )
        if errors:
            # Do NOT finish the run here. A finished run is terminal, so marking
            # it failed on the first validation error made every retry of this
            # task impossible: the retry re-validated cleanly and then died on
            # "run cannot finish as succeeded". The DAG's failure callback
            # aborts the run when the DAG itself gives up, which is the only
            # point at which the outcome is actually known.
            raise RunValidationError("; ".join(errors))
        if live_acceptance:
            cohort_anchor = summary["metadata"]["acceptance_cohort"]
            self.control.record_bronze_acceptance(
                run_id,
                _bronze_acceptance_marker(
                    run_id,
                    summary,
                    cohort_anchor=cohort_anchor,
                ),
            )
        elif acceptance_replay:
            source_run = self.control.get_run(replay_source_run_id)
            source_metadata = source_run.get("metadata") or {}
            source_marker = source_metadata["bronze_acceptance"]
            self.control.record_bronze_acceptance(
                run_id,
                _bronze_acceptance_replay_marker(
                    run_id,
                    summary,
                    source_run_id=str(replay_source_run_id),
                    source_marker=source_marker,
                ),
                replay=True,
            )
        run_row = self.control.get_run(run_id)
        if run_row is not None and str(run_row.get("status") or "") == "failed":
            # A lone ``clear`` of the validate task never re-runs the waves,
            # so the reanimation (#1102) must also live here: the gates above
            # have just passed, and finishing a 'failed' run raises
            # StateConflict otherwise.  start_run keeps refusing terminal
            # runs (succeeded/publication/sealed).  No targets are reopened —
            # the run is finishing right now, and reopened 'retry' rows would
            # linger in a succeeded run forever.
            self.control.start_run(run_id, reopen_targets=False)
        if warnings:
            summary["warnings"] = warnings
        self.control.finish_run(run_id, succeeded=True)
        return summary


__all__ = [
    "BACKFILL_SEASON_COHORT_RESERVATION_BYTES",
    "DEFAULT_BYTE_LIMIT",
    "DEFAULT_REQUEST_LIMIT",
    "DEFAULT_REQUEST_RESERVATION_BYTES",
    "DEFAULT_SHARD_SIZE",
    "FBrefPipeline",
    "FETCH_LEASE_SECONDS",
    "FetchWaveError",
    "LiveRunResult",
    "MIB",
    "MAX_SHARD_SIZE",
    "ParseWaveError",
    "PipelineError",
    "PipelineSettings",
    "TypedPromotionDeferred",
    "RunValidationError",
    "SENTINEL_COMPETITIONS",
    "WaveResult",
    "backfill_season_cohort_capacity",
    "frontier_target",
    "live_wave_target_capacity",
    "page_target_from_link",
    "wave_target_capacity",
]
