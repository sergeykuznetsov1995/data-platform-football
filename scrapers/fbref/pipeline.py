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
import time
import uuid
from contextlib import ExitStack
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Mapping, Optional, Sequence

from scrapers.fbref.bronze import FBrefGenericBronzeWriter
from scrapers.fbref.control import (
    BudgetExceeded,
    CompetitionRegistryEntry,
    ControlStore,
    FrontierTarget,
    SeasonRegistryEntry,
    StateConflict,
    make_control_run_id,
    make_logical_refresh_id,
)
from scrapers.fbref.control.models import CohortTarget
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
    sentinel_coverage,
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
    parse_page_document,
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
    DEFAULT_BYTE_LIMIT,
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_REQUEST_LIMIT,
    DEFAULT_REQUEST_RESERVATION_BYTES,
    DEFAULT_SHARD_SIZE,
    MAX_CLEARANCE_SOLVE_ATTEMPTS,
    MAX_SHARD_SIZE,
    MIB,
    bootstrap_reservation_for,
)
from scrapers.fbref.typed_bronze import (
    TYPED_BRONZE_PARSER_VERSION,
    FBrefTypedBronzeAdapter,
    FBrefTypedBronzeWriter,
    TypedBronzeError,
    TypedSourceContext,
    parse_match_html as parse_typed_match_html,
    parse_schedule_html as parse_typed_schedule_html,
    parse_season_stats_html as parse_typed_season_stats_html,
    typed_result_requires_persistence,
)


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

# Statuses Cloudflare returns when it no longer honours a cf_clearance for the
# warm HTTP session. They say nothing about the target page — only that this
# clearance is dead — so the wave re-solves instead of failing every remaining
# target against it.
CLEARANCE_REJECTED_STATUSES = frozenset({401, 403, 429})
# Each refresh costs one browser solve, so a source that rejects clearances
# outright must still fail the wave rather than launch browsers in a loop.
MAX_CLEARANCE_REFRESHES = 2

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class PipelineSettings:
    run_type: str = "current"
    request_limit: int = DEFAULT_REQUEST_LIMIT
    byte_limit: int = DEFAULT_BYTE_LIMIT
    shard_size: int = DEFAULT_SHARD_SIZE
    request_reservation_bytes: int = DEFAULT_REQUEST_RESERVATION_BYTES
    domain_interval_seconds: float = DEFAULT_DOMAIN_INTERVAL_SECONDS
    bootstrap_request_reservation: Optional[int] = None
    target_request_reservation: int = MAX_TARGET_HTTP_ATTEMPTS
    proxy_file: Optional[str] = None

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
        if self.run_type not in {"current", "backfill", "replay"}:
            raise ValueError("run_type must be current, backfill, or replay")
        if self.request_limit < 0 or self.byte_limit < 0:
            raise ValueError("run budgets must be non-negative")
        if not 1 <= self.shard_size <= MAX_SHARD_SIZE:
            raise ValueError("shard_size must be between 1 and 25")
        if self.request_reservation_bytes <= 0:
            raise ValueError("request_reservation_bytes must be positive")
        if self.domain_interval_seconds <= 0:
            raise ValueError("domain_interval_seconds must be positive")
        if self.bootstrap_request_reservation < 1:
            raise ValueError("bootstrap_request_reservation must be positive")
        if self.target_request_reservation != MAX_TARGET_HTTP_ATTEMPTS:
            raise ValueError(
                "target_request_reservation must cover both HTTP attempts"
            )


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
    failures: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


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


def _clearance_rejected(exc: FetchError) -> bool:
    """True when the failure means the clearance died, not that the page did."""

    return (
        exc.error_class == "http_status"
        and exc.http_status in CLEARANCE_REJECTED_STATUSES
    )


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
    ) -> None:
        self.control = control
        self.raw_store = raw_store
        self.generic_writer = generic_writer or FBrefGenericBronzeWriter()
        self.typed_adapter = typed_adapter or FBrefTypedBronzeAdapter()
        self.fetcher_factory = fetcher_factory or (
            lambda proxy_file, max_browser_requests: FBrefFetcher(
                proxy_file=proxy_file,
                max_browser_requests=max_browser_requests,
            )
        )
        self.sleep = sleep
        self.clock = clock

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

    def initialize_run(
        self,
        *,
        airflow_run_id: object,
        dag_id: object,
        settings: PipelineSettings,
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
        self.control.create_run(
            settings.run_type,
            run_id=run_id,
            request_limit=settings.request_limit,
            byte_limit=settings.byte_limit,
            metadata={
                "airflow_run_id": str(airflow_run_id),
                "dag_id": str(dag_id),
                "shard_size": settings.shard_size,
                "request_reservation_bytes": (
                    settings.request_reservation_bytes
                ),
                "target_request_reservation": (
                    settings.target_request_reservation
                ),
            },
        )
        self.control.start_run(run_id)
        return run_id

    def seed_competition_index(self) -> str:
        target = competition_index_target()
        self.control.upsert_frontier_target(frontier_target(target))
        return target.target_id

    def _replay_source_error(self, source_run_id: Optional[str]) -> Optional[str]:
        """Return a fail-closed source-run contract error, if any."""

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
        if status not in {"succeeded", "failed", "cancelled"}:
            return f"replay_source_run_not_terminal={status}"
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
        safe_limit = min(
            int(limit), self._remaining_wave_limit(run_id, settings)
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
        self.control.create_run_cohort(run_id, cohort)
        return {"seeded": len(rows), "auto_resume": True}

    def _remaining_wave_limit(
        self, run_id: str, settings: PipelineSettings
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
        byte_capacity = byte_remaining // settings.request_reservation_bytes
        request_capacity = (
            max(
                0,
                request_remaining - settings.bootstrap_request_reservation,
            )
            // settings.target_request_reservation
        )
        return min(settings.shard_size, request_capacity, byte_capacity)

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

    def fetch_wave(
        self,
        run_id: str,
        *,
        worker_id: str,
        page_kinds: Sequence[str],
        settings: PipelineSettings,
    ) -> WaveResult:
        """Fetch one bounded cohort and commit raw before control success."""

        if settings.run_type == "replay":
            raise PipelineError("Replay mode cannot execute a fetch wave")
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
            unfinished = sum(
                int(count)
                for status, count in target_counts.items()
                if status not in {"succeeded", "skipped"}
            )
            if unfinished:
                raise FetchWaveError(
                    f"Run has {unfinished} unfinished target(s) that are not claimable"
                )
            limit = self._remaining_wave_limit(run_id, settings)
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
        session_id: Optional[str] = None
        fetcher = None
        clearance_refreshes = 0
        stack = ExitStack()
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
                budget_settled = False
                try:
                    # Only exact logical-refresh crash recovery is safe. A
                    # prior recurring snapshot may predate a match final or a
                    # season rollover, so one-shot policy transitions must
                    # perform their own final network refresh.
                    frontier = self.control.get_frontier_target(
                        lease.target_id
                    ) or {}
                    recoverable = self.raw_store.has_fetch(
                        lease.logical_refresh_id
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

                    reserved_requests = settings.target_request_reservation + (
                        settings.bootstrap_request_reservation
                        if fetcher is None
                        else 0
                    )
                    reservation = self.control.reserve_budget(
                        run_id,
                        lease.logical_refresh_id,
                        attempt_id=lease.attempt_id,
                        requests=reserved_requests,
                        bytes_=settings.request_reservation_bytes,
                    )
                    self.control.bind_reservation(
                        lease, reservation.reservation_id
                    )
                    slot = self.control.reserve_domain_slot(
                        "fbref.com",
                        interval_seconds=settings.domain_interval_seconds,
                    )
                    self._wait_for_slot(slot.scheduled_at)

                    if fetcher is None:
                        session_id = self.control.open_clearance_session(
                            domain="fbref.com",
                            session_version=FETCHER_VERSION,
                            expires_at=_as_utc(self.clock()) + timedelta(hours=1),
                            run_id=run_id,
                            metadata={"worker_id": worker_id},
                        )
                        fetcher = stack.enter_context(
                            self.fetcher_factory(
                                settings.proxy_file,
                                settings.bootstrap_request_reservation,
                            )
                        )
                    response = fetcher.fetch(
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
                        session_version=session_id,
                    )
                    billed = (
                        response.provider_billed_bytes
                        if response.provider_billed_bytes is not None
                        else response.http_wire_bytes
                        + response.browser_document_bytes
                        + response.browser_asset_bytes
                        + response.browser_unobserved_bytes
                    )
                    self.control.settle_budget(
                        reservation.reservation_id,
                        requests_used=(
                            response.http_requests + response.browser_requests
                        ),
                        bytes_used=billed,
                    )
                    budget_settled = True
                    if session_id is not None:
                        self.control.record_session_metrics(
                            session_id,
                            browser_bootstrap_requests=response.browser_requests,
                            browser_bootstrap_attempts=(
                                response.browser_bootstrap_attempts
                            ),
                            browser_unobserved_bytes=(
                                response.browser_unobserved_bytes
                            ),
                            browser_document_bytes=(
                                response.browser_document_bytes
                            ),
                            browser_asset_bytes=response.browser_asset_bytes,
                            http_requests=response.http_requests,
                            http_wire_bytes=response.http_wire_bytes,
                            decoded_html_bytes=response.decoded_html_bytes,
                            compressed_raw_bytes=record.encoded_bytes,
                            provider_billed_bytes=(
                                response.provider_billed_bytes
                            ),
                        )
                    self._complete_from_record(
                        lease, record, historical=historical
                    )
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
                except BudgetExceeded as exc:
                    # The budget is a ceiling the crawler is meant to stop at,
                    # not a fault. Failing these targets made every day that
                    # spent its budget a red run — and the pages had not even
                    # been touched. Hand them back to the queue and end the wave
                    # cleanly; the next run picks them up.
                    unfetched = leases[lease_index:]
                    result.requeued_at_budget = (
                        self.control.requeue_unfetched_targets(unfetched)
                    )
                    result.budget_exhausted = True
                    logger.warning(
                        "FBref run budget exhausted (%s) — %d unfetched "
                        "target(s) returned to the queue for the next run",
                        exc,
                        result.requeued_at_budget,
                    )
                    break
                except FetchError as exc:
                    billed = (
                        exc.provider_billed_bytes
                        if exc.provider_billed_bytes is not None
                        else max(0, int(exc.wire_bytes))
                        + max(0, int(exc.browser_document_bytes))
                        + max(0, int(exc.browser_asset_bytes))
                        + max(0, int(exc.browser_unobserved_bytes))
                    )
                    if reservation is not None and not budget_settled:
                        self.control.settle_budget(
                            reservation.reservation_id,
                            requests_used=(
                                exc.http_requests + exc.browser_requests
                            ),
                            bytes_used=billed,
                        )
                    if session_id is not None:
                        self.control.record_session_metrics(
                            session_id,
                            browser_bootstrap_requests=exc.browser_requests,
                            browser_bootstrap_attempts=(
                                exc.browser_bootstrap_attempts
                            ),
                            browser_unobserved_bytes=(
                                exc.browser_unobserved_bytes
                            ),
                            browser_document_bytes=(
                                exc.browser_document_bytes
                            ),
                            browser_asset_bytes=exc.browser_asset_bytes,
                            http_requests=exc.http_requests,
                            http_wire_bytes=max(0, int(exc.wire_bytes)),
                            provider_billed_bytes=exc.provider_billed_bytes,
                        )
                    self.control.fail_fetch(
                        lease,
                        error_class=exc.error_class,
                        error_message=str(exc),
                        retry_delay_seconds=60,
                        permanent=(exc.error_class == "response_too_large"),
                        http_status=exc.http_status,
                        http_request_count=exc.http_requests,
                        http_status_history=exc.http_status_history,
                        wire_bytes=exc.wire_bytes,
                        provider_billed_bytes=exc.provider_billed_bytes,
                        latency_ms=exc.latency_ms,
                        transport_version=FETCHER_VERSION,
                        session_version=session_id,
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
                    result.failures.append(
                        f"{lease.target_id}:{exc.error_class}"
                    )
                    if (
                        _clearance_rejected(exc)
                        and fetcher is not None
                        and clearance_refreshes < MAX_CLEARANCE_REFRESHES
                    ):
                        # Cloudflare stopped honouring this clearance mid-wave
                        # (its exit IP fell out of favour). Every remaining
                        # target would 403 against the same dead session — one
                        # rejected exit IP burned a whole wave in production.
                        # Drop the session here; the next target solves again on
                        # a fresh proxy. Bounded: a source that rejects every
                        # clearance must fail the wave, not spawn browsers.
                        clearance_refreshes += 1
                        logger.warning(
                            "FBref rejected the warm clearance (HTTP %s) — "
                            "dropping the session and re-solving on a fresh "
                            "proxy (refresh %d/%d)",
                            exc.http_status,
                            clearance_refreshes,
                            MAX_CLEARANCE_REFRESHES,
                        )
                        stack.close()
                        fetcher = None
                        self.control.close_clearance_session(
                            session_id, status="failed"
                        )
                        session_id = None
                except Exception as exc:
                    if reservation is not None and not budget_settled:
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
                            None if response is None else session_id
                        ),
                    )
                    result.failures.append(
                        f"{lease.target_id}:{type(exc).__name__}"
                    )
        finally:
            stack.close()
            if session_id is not None:
                self.control.close_clearance_session(
                    session_id,
                    status="failed" if result.failures else "closed",
                )
        if result.failures:
            raise FetchWaveError("; ".join(result.failures))
        return result

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
    ) -> tuple[int, int]:
        eligible = set(self._eligible_competitions())
        seeded = 0
        skipped = 0
        seen_targets: set[str] = set()
        for link in links:
            source_ids = dict(link.source_ids)
            competition_id = source_ids.get("competition_id")
            if competition_id and str(competition_id) not in eligible:
                skipped += 1
                continue
            target = page_target_from_link(link)
            if target.target_id in seen_targets:
                continue
            seen_targets.add(target.target_id)
            prepared = frontier_target(target, historical=historical)
            if refresh_policy is not None:
                prepared = FrontierTarget(
                    target_id=prepared.target_id,
                    page_kind=prepared.page_kind,
                    canonical_url=prepared.canonical_url,
                    source_ids=prepared.source_ids,
                    refresh_policy=refresh_policy,
                    priority=prepared.priority,
                    next_fetch_at=prepared.next_fetch_at,
                    source=prepared.source,
                )
            self.control.upsert_frontier_target(
                prepared
            )
            seeded += 1
        return seeded, skipped

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
        seeded = 0
        skipped = 0
        for competition in competitions:
            if competition_eligibility(competition).value != "eligible":
                skipped += 1
                continue
            target = competition_page_target(
                competition.competition_id, competition.history_url
            )
            self.control.upsert_frontier_target(frontier_target(target))
            seeded += 1
        return seeded, skipped

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
        current_indexes = {
            index
            for index, season in enumerate(seasons)
            if current_label and season.label == current_label
        }
        if not current_indexes and seasons:
            current_indexes = {0}
        entries = [
            SeasonRegistryEntry(
                competition_id=competition_id,
                season_id=season.season_id,
                canonical_url=season.season_url,
                label=season.label,
                is_current=index in current_indexes,
                metadata={"calendar_type": season.calendar_type.value},
            )
            for index, season in enumerate(seasons)
        ]
        self.control.reconcile_seasons(snapshot_id, competition_id, entries)
        seeded = 0
        for index, season in enumerate(seasons):
            is_current = index in current_indexes
            if run_type == "current" and not is_current:
                continue
            if run_type == "backfill" and is_current:
                continue
            target = season_page_target(
                competition_id, season.season_id, season.season_url
            )
            self.control.upsert_frontier_target(
                frontier_target(target, historical=not is_current)
            )
            seeded += 1
        return seeded, 0

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
        links: list[DiscoveredPageLink] = []
        directly_seeded = 0
        directly_skipped = 0
        if record.page_kind == "season":
            parsed = parse_season_html(html, self._season_ref(record))
            if parsed.has_errors:
                raise ParseWaveError(
                    f"Schedule discovery failed for {record.target_id}"
                )
            for schedule in parsed.datasets["schedules"].records:
                links.append(
                    DiscoveredPageLink(
                        page_kind="schedule",
                        canonical_url=schedule.schedule_url,
                        source_ids={
                            "competition_id": schedule.competition_id,
                            "season_id": schedule.season_id,
                        },
                    )
                )
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
                seeded, skipped = self._seed_links(
                    [match_link],
                    historical=(
                        historical
                    ),
                    refresh_policy=(
                        None
                        if historical or match.canonical_url not in completed_urls
                        else "current_completed_once"
                    ),
                )
                directly_seeded += seeded
                directly_skipped += skipped
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
        links.extend(discovered)
        seeded, skipped = self._seed_links(links, historical=historical)
        return directly_seeded + seeded, directly_skipped + skipped

    def _persist_generic(
        self,
        run_id: str,
        html: str,
        record: RawFetchRecord,
    ):
        page = parse_page_document(
            html,
            target_id=record.target_id,
            page_kind=record.page_kind,
            content_hash=record.content_hash,
        )
        try:
            self.generic_writer.persist_page(
                page,
                canonical_url=record.canonical_url,
                run_id=run_id,
                staging_identity=record.logical_refresh_id,
            )
        except Exception as exc:
            try:
                self.control.record_dataset_manifest(
                    target_id=record.target_id,
                    content_hash=record.content_hash,
                    parser_version=PAGE_DOCUMENT_VERSION,
                    dataset="__page__",
                    availability=Availability.ERROR.value,
                    parse_status=("failed" if page.errors else "succeeded"),
                    persistence_status="failed",
                    validation_status="failed",
                    row_count=0,
                    error_class=type(exc).__name__,
                    error_message=str(exc),
                )
            except StateConflict:
                # These exact bytes already have a completed manifest from an
                # earlier parse by this exact parser; that evidence stands.
                # Recording a failure over it must never replace the error that
                # actually broke this parse — the diagnosis is what we need.
                logger.warning(
                    "Failure manifest for %s not recorded: the generic "
                    "manifest is already completed", record.target_id,
                )
            raise
        for table in page.tables:
            self.control.record_dataset_manifest(
                target_id=record.target_id,
                content_hash=record.content_hash,
                parser_version=page.parser_version,
                dataset=f"table:{table.table_id}:{table.source_location}",
                availability=table.availability.value,
                parse_status="succeeded",
                persistence_status="succeeded",
                validation_status="succeeded",
                row_count=table.row_count,
            )
        return page

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

    def _record_typed_results(
        self,
        record: RawFetchRecord,
        parsed: Mapping[str, object],
        *,
        persisted: Optional[Mapping[str, int]],
    ) -> None:
        for name, dataset in parsed.items():
            status = str(getattr(dataset.status, "value", dataset.status))
            requires_persistence = typed_result_requires_persistence(dataset)
            failed = status == "error" or (
                requires_persistence and persisted is None
            )
            was_persisted = bool(persisted is not None and name in persisted)
            self.control.record_dataset_manifest(
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
                error_message=getattr(dataset, "error_message", None),
            )

    def _record_typed_completion(
        self,
        record: RawFetchRecord,
    ) -> None:
        """Cache typed success only after every dataset result."""

        self.control.record_dataset_manifest(
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
    ) -> None:
        self.control.record_dataset_manifest(
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
            parse_status="succeeded" if succeeded else "failed",
            persistence_status="succeeded" if succeeded else "failed",
            validation_status="succeeded" if succeeded else "failed",
            row_count=(
                0
                if page is None
                else sum(table.row_count for table in page.tables)
            ),
            error_class=None if error is None else type(error).__name__,
            error_message=None if error is None else str(error),
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

    def parse_wave(
        self,
        run_id: str,
        *,
        page_kinds: Sequence[str],
        settings: PipelineSettings,
        source_run_id: Optional[str] = None,
    ) -> WaveResult:
        """Parse and persist a bounded handoff using raw storage only."""

        result = WaveResult()
        stateful_run_id = run_id
        stateful_run_type = settings.run_type
        if settings.run_type == "replay":
            source_error = self._replay_source_error(source_run_id)
            if source_error:
                raise ParseWaveError(source_error)
            source_run = self.control.get_run(source_run_id)
            stateful_run_id = str(source_run_id)
            stateful_run_type = str(source_run["run_type"])
        if source_run_id:
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
                limit=settings.shard_size,
            )
        historical = stateful_run_type == "backfill"
        for item in fetches:
            logical_refresh_id = str(item["logical_refresh_id"])
            record = None
            page = None
            observation_lease = None
            try:
                html, record = self.raw_store.load_fetch_html(
                    logical_refresh_id
                )
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
                observation_lease = self.control.claim_observation_processing(
                    logical_refresh_id=logical_refresh_id,
                    target_id=record.target_id,
                    content_hash=record.content_hash,
                    parser_version=PAGE_DOCUMENT_VERSION,
                    typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
                    stateful_parser_version=DISCOVERY_PARSER_VERSION,
                    lease_seconds=PROCESSING_LEASE_SECONDS,
                )
                if observation_lease is None:
                    continue
                page = self._persist_generic(run_id, html, record)
                typed_page = record.page_kind in {
                    "schedule",
                    "season",
                    "season_stats",
                    "match",
                }
                # One frontier lock linearizes typed output, stateful parser
                # effects, and completion against the next fetch.  Replay is
                # offline but intentionally rebuilds state from latest raw
                # when its discovery parser version changes.
                with self.control.guard_latest_content(
                    record.target_id,
                    record.content_hash,
                    record.logical_refresh_id,
                ) as is_latest:
                    if is_latest is None:
                        raise TypedPromotionDeferred(
                            "Stateful promotion deferred for active target "
                            f"{record.target_id}"
                        )
                    if is_latest:
                        if typed_page:
                            if self._typed_context(record) is None:
                                raise TypedBronzeError(
                                    "Typed page requires source "
                                    "competition_id and season_id"
                                )
                            self._persist_typed(run_id, html, record)
                            result.typed_promoted += 1
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
                    else:
                        seeded, skipped = 0, 0
                        stateful_status = "skipped"
                        typed_status = "skipped"
                        if typed_page:
                            self._record_stale_typed_observation(record)
                            result.stale_typed_observations_skipped += 1
                    self._record_page_completion(
                        record, page, succeeded=True
                    )
                    self.control.complete_observation_processing(
                        observation_lease,
                        typed_status=typed_status,
                        stateful_status=stateful_status,
                    )
                result.seeded += seeded
                result.skipped_ineligible += skipped
                result.parsed += 1
            except Exception as exc:
                if record is not None and not isinstance(
                    exc, TypedPromotionDeferred
                ):
                    try:
                        self._record_page_completion(
                            record, page, succeeded=False, error=exc
                        )
                    except Exception as manifest_exc:
                        result.failures.append(
                            f"{item['target_id']}:manifest:"
                            f"{type(manifest_exc).__name__}:{manifest_exc}"
                        )
                if observation_lease is not None:
                    try:
                        self.control.fail_observation_processing(
                            observation_lease,
                            error_class=type(exc).__name__,
                            error_message=str(exc),
                        )
                    except Exception as fence_exc:
                        result.failures.append(
                            f"{item['target_id']}:observation_fence:"
                            f"{type(fence_exc).__name__}:{fence_exc}"
                        )
                result.failures.append(
                    f"{item['target_id']}:{type(exc).__name__}:{exc}"
                )
        if result.failures:
            raise ParseWaveError("; ".join(result.failures))
        return result

    def validate_and_finish(
        self,
        run_id: str,
        *,
        replay_source_run_id: Optional[str] = None,
    ) -> dict:
        summary = self.control.get_run_summary(
            run_id,
            parser_version=PAGE_DOCUMENT_VERSION,
            typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
            stateful_parser_version=DISCOVERY_PARSER_VERSION,
        )
        if summary is None:
            raise RunValidationError(f"Unknown run {run_id}")
        target_counts = summary.get("target_counts") or {}
        # 'skipped' is a target the run deliberately did not fetch — it stopped
        # at its budget and handed the target back to the queue. That is the
        # designed steady state of a budgeted crawler, not an incomplete run.
        incomplete = {
            status: count
            for status, count in target_counts.items()
            if status not in {"succeeded", "skipped"} and int(count) > 0
        }
        dataset_counts = summary.get("dataset_validation_counts") or {}
        dataset_failures = sum(
            int(count)
            for status, count in dataset_counts.items()
            if status not in {"succeeded", "skipped"}
        )
        errors = []
        if incomplete:
            errors.append(f"incomplete_targets={incomplete}")
        if dataset_failures:
            errors.append(f"failed_dataset_manifests={dataset_failures}")
        if int(summary.get("unvalidated_target_count") or 0) != 0:
            errors.append(
                "unvalidated_target_count="
                f"{int(summary['unvalidated_target_count'])}"
            )
        if int(summary.get("promotion_pending_match_count") or 0) != 0:
            errors.append(
                "promotion_pending_match_count="
                f"{int(summary['promotion_pending_match_count'])}"
            )
        if bool(summary.get("budget_exceeded")):
            errors.append("budget_exceeded=true")
        if int(summary.get("requests_used") or 0) > int(
            summary.get("request_limit") or 0
        ):
            errors.append("request_limit_exceeded")
        if int(summary.get("bytes_used") or 0) > int(
            summary.get("byte_limit") or 0
        ):
            errors.append("byte_limit_exceeded")
        traffic = summary.get("traffic_totals") or {}
        success_rate = traffic.get("warm_http_success_rate")
        if success_rate is not None and float(success_rate) < 0.95:
            errors.append(
                f"warm_http_success_rate={float(success_rate):.4f}<0.95"
            )
        if float(traffic.get("unclassified_failure_rate") or 0.0) >= 0.005:
            errors.append(
                "unclassified_failure_rate="
                f"{float(traffic['unclassified_failure_rate']):.4f}>=0.005"
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
            source_error = self._replay_source_error(replay_source_run_id)
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
        if int(summary.get("female_downstream_targets") or 0) != 0:
            errors.append("female_downstream_targets_nonzero")
        if int(summary.get("unknown_gender_downstream_targets") or 0) != 0:
            errors.append("unknown_gender_downstream_targets_nonzero")
        if str(summary.get("run_type") or "").lower() != "replay":
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
        self.control.finish_run(run_id, succeeded=True)
        return summary


__all__ = [
    "DEFAULT_BYTE_LIMIT",
    "DEFAULT_REQUEST_LIMIT",
    "DEFAULT_REQUEST_RESERVATION_BYTES",
    "DEFAULT_SHARD_SIZE",
    "FBrefPipeline",
    "FETCH_LEASE_SECONDS",
    "FetchWaveError",
    "MIB",
    "MAX_SHARD_SIZE",
    "ParseWaveError",
    "PipelineError",
    "PipelineSettings",
    "TypedPromotionDeferred",
    "RunValidationError",
    "SENTINEL_COMPETITIONS",
    "WaveResult",
    "frontier_target",
    "page_target_from_link",
]
