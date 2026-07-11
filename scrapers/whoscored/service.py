"""Application service for the WhoScored V2 ingestion pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping, Optional

from .catalog import CatalogSeason, WhoScoredCatalog
from .domain import WhoScoredScope
from .parsers import (
    DatasetStatus,
    WhoScoredParseError,
    find_source_season_id,
    parse_calendar_months,
    parse_match_html,
    parse_preview_html,
    parse_profile_html,
    parse_schedule_json,
    parse_season_stages,
)
from .raw_store import (
    RawObjectNotFound,
    RawObjectRecord,
    RawTarget,
    WhoScoredRawStore,
    match_page_target,
    preview_page_target,
    profile_page_target,
    schedule_month_target,
    stage_page_target,
)
from .repository import (
    ManifestFailure,
    MatchCommit,
    PreviewCommit,
    PreviewFailure,
    WhoScoredRepository,
)
from .transport import (
    CachedPayload,
    FailureKind,
    TransportBudgets,
    TransportResponse,
    WhoScoredTransport,
    WhoScoredTransportError,
)


ACTIVE_SCHEDULE_CACHE_TTL = timedelta(hours=6)


@dataclass
class EntityResult:
    entity: str
    scope: str
    attempted: int = 0
    succeeded: int = 0
    skipped: int = 0
    retryable: list[str] = field(default_factory=list)
    terminal: list[str] = field(default_factory=list)
    tables: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.errors:
            return "failed"
        if self.retryable:
            return "retryable"
        return "success"

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "scope": self.scope,
            "status": self.status,
            "attempted": self.attempted,
            "succeeded": self.succeeded,
            "skipped": self.skipped,
            "retryable": list(self.retryable),
            "terminal": list(self.terminal),
            "tables": list(dict.fromkeys(self.tables)),
            "counts": dict(self.counts),
            "errors": list(self.errors),
        }


class _TargetRawCache:
    """Bind transport's string cache hook to one typed raw target."""

    def __init__(
        self,
        store: WhoScoredRawStore,
        target: RawTarget,
        *,
        allow_load: bool,
        content_type: str,
    ) -> None:
        self.backend = store
        self.target = target
        self.allow_load = allow_load
        self.content_type = content_type
        self.record: Optional[RawObjectRecord] = None

    def load(self, key: str) -> Optional[CachedPayload]:
        if not self.allow_load or key != self.target.target_id:
            return None
        try:
            content, record = self.backend.load_bytes(self.target)
        except RawObjectNotFound:
            return None
        self.record = record
        return CachedPayload(content=content, status_code=200, headers={})

    def store(self, key: str, payload: CachedPayload, sha256: str) -> None:
        if key != self.target.target_id:
            raise ValueError(f"raw cache key mismatch: {key!r}")
        record = self.backend.store_bytes(
            self.target,
            payload.content,
            content_type=self.content_type,
            charset="utf-8",
        )
        if record.content_hash != sha256:
            raise ValueError("raw store hash differs from transport hash")
        self.record = record

    @property
    def raw_uri(self) -> str:
        if self.record is None:
            raise RuntimeError("raw target has not been committed")
        return self.backend.object_uri(self.record.blob_key)


class WhoScoredIngestService:
    """Coordinate discovery, transport, parsing and logical Iceberg commits."""

    def __init__(
        self,
        scope: CatalogSeason | WhoScoredScope,
        *,
        catalog: Optional[WhoScoredCatalog] = None,
        repository: Optional[WhoScoredRepository] = None,
        transport: Optional[WhoScoredTransport] = None,
        raw_store: Optional[WhoScoredRawStore] = None,
    ) -> None:
        self.catalog = catalog or WhoScoredCatalog.from_file()
        self.catalog_season = (
            scope
            if isinstance(scope, CatalogSeason)
            else self.catalog.resolve_scope(scope.competition_id, scope.season_id)
        )
        self.scope = self.catalog_season.scope
        self.competition = self.catalog.competition(self.scope.competition_id)
        self.repository = repository or WhoScoredRepository()
        self.raw_store = raw_store or WhoScoredRawStore.from_env(optional=False)
        self.transport = transport or WhoScoredTransport(
            flaresolverr_url=os.environ.get(
                "FLARESOLVERR_URL", "http://flaresolverr:8191"
            ),
            paid_proxy_url=(
                os.environ.get("WHOSCORED_PAID_PROXY_URL", "").strip() or None
            ),
            proxy_control_url=(
                os.environ.get("WHOSCORED_PROXY_CONTROL_URL", "").strip() or None
            ),
        )
        from scrapers.utils.rate_limiter import RateLimiter

        self._rate_limiter = RateLimiter(max_requests=30, window_seconds=60)

    def _fetch(
        self,
        target: RawTarget,
        *,
        validator: Callable[[TransportResponse], Optional[bool]],
        content_type: str = "text/html",
        allow_cache: bool = True,
        cache_ttl: Optional[timedelta] = None,
    ) -> tuple[TransportResponse, str]:
        cache_is_fresh = (
            allow_cache
            and cache_ttl is not None
            and self.raw_store.is_fresh(target, max_age=cache_ttl)
        )
        cache_load_allowed = allow_cache and (
            cache_ttl is None or cache_is_fresh
        )
        adapter = _TargetRawCache(
            self.raw_store,
            target,
            allow_load=cache_load_allowed,
            content_type=content_type,
        )
        cache_present = cache_load_allowed and self.raw_store.has(target)
        previous = self.transport.raw_cache
        self.transport.raw_cache = adapter
        try:
            if not cache_present:
                self._rate_limiter.acquire()
            response = self.transport.fetch(
                target.canonical_url,
                cache_key=target.target_id,
                validator=validator,
            )
        except WhoScoredTransportError as exc:
            # Validator/content failures are persisted by the transport before
            # they are raised. Carry that raw identity to the failure manifest
            # so a later parser version can replay it without network traffic.
            if adapter.record is not None:
                exc.payload_sha256 = adapter.record.content_hash
                exc.raw_uri = adapter.raw_uri
            raise
        finally:
            self.transport.raw_cache = previous
        return response, adapter.raw_uri

    @staticmethod
    def _html_target(
        *, page_kind: str, target_id: str, url: str, source_ids: Mapping[str, str]
    ) -> RawTarget:
        return RawTarget(
            source="whoscored",
            page_kind=page_kind,
            target_id=target_id,
            canonical_url=url,
            source_ids=source_ids,
        )

    def _source_season_id(self) -> int:
        configured = self.catalog_season.source_season_id
        if configured is not None:
            return int(configured)
        existing = self.repository.latest_source_season_id(
            self.scope.competition_id, self.scope.season_id
        )
        if existing is not None:
            return existing
        if self.competition.region_id is None or self.competition.tournament_id is None:
            raise RuntimeError(f"{self.scope.spec}: WhoScored source ids are not configured")
        url = (
            "https://www.whoscored.com/Regions/"
            f"{self.competition.region_id}/Tournaments/{self.competition.tournament_id}"
        )
        target = self._html_target(
            page_kind="tournament",
            target_id=f"whoscored:tournament:{self.scope.competition_id}",
            url=url,
            source_ids={
                "competition_id": self.scope.competition_id,
                "region_id": str(self.competition.region_id),
                "tournament_id": str(self.competition.tournament_id),
            },
        )

        def validate(response: TransportResponse) -> bool:
            find_source_season_id(response.text, self.scope)
            return True

        active = self.catalog_season.end is None or self.catalog_season.end >= date.today()
        response, _ = self._fetch(
            target,
            validator=validate,
            allow_cache=True,
            cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL if active else None,
        )
        return find_source_season_id(response.text, self.scope)

    def _bound_paid_fallback(self, eligible_urls: int) -> None:
        """Tighten paid URL cardinality for the current entity batch.

        The byte ceilings remain task-wide; this additionally prevents a
        large roster/preview candidate list from turning a transient
        Cloudflare incident into one paid lease per candidate.
        """
        current = self.transport.budgets
        self.transport.budgets = TransportBudgets.for_eligible_urls(
            eligible_urls,
            max_response_bytes=current.max_response_bytes,
            max_paid_bytes_per_url=current.max_paid_bytes_per_url,
            max_paid_bytes_per_lease=current.max_paid_bytes_per_lease,
            max_paid_bytes_per_task=current.max_paid_bytes_per_task,
            max_paid_browser_bootstraps=current.max_paid_browser_bootstraps,
            lease_ttl_seconds=current.lease_ttl_seconds,
        )

    def sync_schedule(self) -> EntityResult:
        result = EntityResult("schedule", self.scope.spec, attempted=1)
        # Discovery fans out from one season page into calendars/months. A
        # broad Cloudflare incident must not turn that fan-out into the
        # transport default of twenty paid URLs; three distinct fallbacks are
        # the task-wide ceiling, and the proxy filter enforces bytes as well.
        self._bound_paid_fallback(1)
        try:
            source_season_id = self._source_season_id()
            today = date.today()
            active = self.catalog_season.end is None or self.catalog_season.end >= today
            if self.competition.region_id is None or self.competition.tournament_id is None:
                raise RuntimeError(f"{self.scope.spec}: missing region/tournament ids")
            season_target = stage_page_target(
                self.scope,
                region_id=self.competition.region_id,
                tournament_id=self.competition.tournament_id,
                source_season_id=source_season_id,
            )

            def validate_stages(response: TransportResponse) -> bool:
                parse_season_stages(
                    response.text,
                    scope=self.scope,
                    region_id=self.competition.region_id,
                    tournament_id=self.competition.tournament_id,
                    source_season_id=source_season_id,
                )
                return True

            season_response, _ = self._fetch(
                season_target,
                validator=validate_stages,
                # Active tournament pages can gain knockout/group stages.
                # A short TTL keeps same-day retries offline without freezing
                # discovery; historical seasons remain immutable raw replay.
                allow_cache=True,
                cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL if active else None,
            )
            stages = parse_season_stages(
                season_response.text,
                scope=self.scope,
                region_id=self.competition.region_id,
                tournament_id=self.competition.tournament_id,
                source_season_id=source_season_id,
            )
            stage_rows = list(stages.rows)
            schedule_by_id: dict[int, dict[str, Any]] = {}
            for stage in stage_rows:
                stage_id = int(stage["stage_id"])
                calendar_url = (
                    f"https://www.whoscored.com/Regions/{self.competition.region_id}"
                    f"/Tournaments/{self.competition.tournament_id}"
                    f"/Seasons/{source_season_id}/Stages/{stage_id}"
                )
                calendar_target = self._html_target(
                    page_kind="stage_calendar",
                    target_id=f"whoscored:calendar:{stage_id}",
                    url=calendar_url,
                    source_ids={"stage_id": str(stage_id)},
                )

                def validate_calendar(response: TransportResponse) -> bool:
                    parse_calendar_months(response.text)
                    return True

                calendar_response, _ = self._fetch(
                    calendar_target,
                    validator=validate_calendar,
                    allow_cache=True,
                    cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL if active else None,
                )
                for month in parse_calendar_months(calendar_response.text):
                    month_target = schedule_month_target(stage_id, month.year, month.month)
                    if month.month == 12:
                        next_month = date(month.year + 1, 1, 1)
                    else:
                        next_month = date(month.year, month.month + 1, 1)
                    # A month is immutable after a grace period for postponed
                    # games and corrections. Mutable months use the same short
                    # TTL, so retries are offline but changes arrive same-day.
                    closed_month = next_month + timedelta(days=7) <= today

                    def validate_month(response: TransportResponse) -> bool:
                        parse_schedule_json(
                            response.content,
                            scope=self.scope,
                            stage_id=stage_id,
                            stage=stage.get("stage"),
                        )
                        return True

                    month_response, _ = self._fetch(
                        month_target,
                        validator=validate_month,
                        content_type="application/json",
                        allow_cache=True,
                        cache_ttl=(
                            ACTIVE_SCHEDULE_CACHE_TTL
                            if active and not closed_month
                            else None
                        ),
                    )
                    parsed = parse_schedule_json(
                        month_response.content,
                        scope=self.scope,
                        stage_id=stage_id,
                        stage=stage.get("stage"),
                    )
                    for row in parsed.rows:
                        schedule_by_id[int(row["game_id"])] = dict(row)

            if not schedule_by_id:
                raise WhoScoredParseError("season schedule contains no matches")
            self.repository.write_scope_snapshot(
                table="whoscored_season_stages",
                rows=stage_rows,
                league=self.scope.competition_id,
                season=self.scope.season_id,
                entity_type="season_stages",
                distinct_key="stage_id",
            )
            self.repository.write_scope_snapshot(
                table="whoscored_schedule",
                rows=list(schedule_by_id.values()),
                league=self.scope.competition_id,
                season=self.scope.season_id,
                entity_type="schedule",
                distinct_key="game_id",
            )
            result.succeeded = len(schedule_by_id)
            result.counts["schedule"] = len(schedule_by_id)
            result.counts["season_stages"] = len(stage_rows)
            result.tables.extend(
                [
                    "iceberg.bronze.whoscored_season_stages",
                    "iceberg.bronze.whoscored_schedule",
                ]
            )
        except Exception as exc:
            result.errors.append(f"schedule: {type(exc).__name__}: {exc}")
        return result

    def sync_matches(
        self,
        *,
        match_ids: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
    ) -> EntityResult:
        result = EntityResult("matches", self.scope.spec)
        self.repository.ensure_schema()
        candidates = self.repository.list_match_candidates(
            self.scope.competition_id,
            self.scope.season_id,
            match_ids=match_ids,
            limit=limit,
        )
        result.attempted = len(candidates)
        self._bound_paid_fallback(len(candidates))
        for candidate in candidates:
            target = match_page_target(candidate.game_id)
            response: Optional[TransportResponse] = None
            raw_uri: Optional[str] = None

            def validate(response: TransportResponse) -> bool:
                parse_match_html(
                    response.text,
                    scope=self.scope,
                    game_id=candidate.game_id,
                    game=candidate.game,
                )
                return True

            try:
                response, raw_uri = self._fetch(
                    target, validator=validate, allow_cache=True
                )
                parsed = parse_match_html(
                    response.text,
                    scope=self.scope,
                    game_id=candidate.game_id,
                    game=candidate.game,
                )
                if parsed.events.status is not DatasetStatus.AVAILABLE:
                    raise WhoScoredParseError("completed match has no events")
                self.repository.commit_match(
                    MatchCommit(
                        game_id=candidate.game_id,
                        league=self.scope.competition_id,
                        season=self.scope.season_id,
                        game=candidate.game,
                        payload_sha256=response.sha256,
                        raw_uri=raw_uri,
                        events=parsed.events.rows,
                        lineups=parsed.lineups.rows,
                        lineups_available=(
                            parsed.lineups.status is DatasetStatus.AVAILABLE
                        ),
                        transport_mode=response.route.value,
                        proxy_mode=(
                            "filtered_paid"
                            if response.route.value.startswith("paid_")
                            else "none"
                        ),
                        http_status=response.status_code,
                        direct_bytes=(
                            response.wire_bytes
                            if not response.route.value.startswith("paid_")
                            else 0
                        ),
                        paid_bytes=(
                            response.wire_bytes
                            if response.route.value.startswith("paid_")
                            else 0
                        ),
                        parser_version=parsed.parser_version,
                        kickoff=candidate.kickoff,
                    )
                )
                result.succeeded += 1
                result.counts["events"] = (
                    result.counts.get("events", 0) + parsed.events.row_count
                )
                result.counts["lineups"] = (
                    result.counts.get("lineups", 0) + parsed.lineups.row_count
                )
            except WhoScoredTransportError as exc:
                if exc.kind is FailureKind.CONTENT:
                    state = "parse_failed"
                else:
                    state = (
                        "retryable"
                        if exc.retryable or exc.status_code == 404
                        else "terminal"
                    )
                retry_after = (
                    datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=6)
                    if state == "retryable"
                    else None
                )
                self.repository.record_failure(
                    ManifestFailure(
                        game_id=candidate.game_id,
                        league=self.scope.competition_id,
                        season=self.scope.season_id,
                        state=state,
                        failure_code=exc.kind.value,
                        error=str(exc)[:4000],
                        retry_after=retry_after,
                        attempt_no=1,
                        transport_mode=exc.route.value if exc.route else "none",
                        proxy_mode=(
                            "filtered_paid"
                            if exc.route and exc.route.value.startswith("paid_")
                            else "none"
                        ),
                        http_status=exc.status_code,
                        payload_sha256=getattr(exc, "payload_sha256", None),
                        raw_uri=getattr(exc, "raw_uri", None),
                    )
                )
                if state == "parse_failed":
                    result.errors.append(f"game {candidate.game_id}: {exc}")
                else:
                    target_list = (
                        result.retryable
                        if state == "retryable"
                        else result.terminal
                    )
                    target_list.append(str(candidate.game_id))
            except WhoScoredParseError as exc:
                self.repository.record_failure(
                    ManifestFailure(
                        game_id=candidate.game_id,
                        league=self.scope.competition_id,
                        season=self.scope.season_id,
                        state="parse_failed",
                        failure_code=FailureKind.CONTENT.value,
                        error=str(exc)[:4000],
                        retry_after=None,
                        attempt_no=1,
                        payload_sha256=(
                            response.sha256 if response is not None else None
                        ),
                        raw_uri=raw_uri,
                        transport_mode=(
                            response.route.value if response is not None else "none"
                        ),
                        proxy_mode=(
                            "filtered_paid"
                            if response is not None
                            and response.route.value.startswith("paid_")
                            else "none"
                        ),
                        http_status=(
                            response.status_code if response is not None else None
                        ),
                        direct_bytes=(
                            response.wire_bytes
                            if response is not None
                            and not response.route.value.startswith("paid_")
                            else 0
                        ),
                        paid_bytes=(
                            response.wire_bytes
                            if response is not None
                            and response.route.value.startswith("paid_")
                            else 0
                        ),
                    )
                )
                result.errors.append(f"game {candidate.game_id}: {exc}")
            except Exception as exc:
                result.errors.append(
                    f"game {candidate.game_id}: {type(exc).__name__}: {exc}"
                )
        if candidates or result.succeeded:
            result.tables.extend(
                [
                    "iceberg.bronze.whoscored_events",
                    "iceberg.bronze.whoscored_lineups",
                    "iceberg.bronze.whoscored_match_ingest_manifest",
                ]
            )
        return result

    def sync_previews(self, *, limit: Optional[int] = None) -> EntityResult:
        result = EntityResult("previews", self.scope.spec)
        self.repository.ensure_schema()
        candidates = self.repository.list_preview_candidates(
            self.scope.competition_id, self.scope.season_id, limit=limit
        )
        result.attempted = len(candidates)
        self._bound_paid_fallback(len(candidates))
        for candidate in candidates:
            target = preview_page_target(candidate["game_id"])

            def validate(response: TransportResponse) -> bool:
                parse_preview_html(
                    response.text,
                    scope=self.scope,
                    game_id=candidate["game_id"],
                    game=candidate["game"],
                    home_team=candidate["home_team"],
                    away_team=candidate["away_team"],
                )
                return True

            try:
                # Parser-version refreshes and due retries replay raw first;
                # a cache miss naturally reaches the source. Only a cadence
                # refresh after a successful snapshot bypasses that object so
                # mutable injury news is actually refreshed.
                response, raw_uri = self._fetch(
                    target,
                    validator=validate,
                    allow_cache=not bool(candidate["force_refresh"]),
                )
            except WhoScoredTransportError as exc:
                if exc.kind is FailureKind.CONTENT:
                    state = "parse_failed"
                else:
                    retryable_kinds = {
                        FailureKind.BUDGET,
                        FailureKind.BROWSER,
                        FailureKind.CACHE,
                        FailureKind.CLOUDFLARE,
                        FailureKind.CONFIG,
                        FailureKind.PROXY,
                        FailureKind.TIMEOUT,
                    }
                    state = (
                        "retryable"
                        if (
                            exc.retryable
                            or exc.kind in retryable_kinds
                            or exc.status_code == 404
                        )
                        else "terminal"
                    )
                attempt_no = int(candidate["attempt_no"])
                retry_after = None
                if state == "retryable":
                    delay_hours = min(6 * (2 ** min(attempt_no - 1, 3)), 48)
                    retry_after = (
                        datetime.now(timezone.utc).replace(tzinfo=None)
                        + timedelta(hours=delay_hours)
                    )
                try:
                    self.repository.record_preview_failure(
                        PreviewFailure(
                            game_id=int(candidate["game_id"]),
                            league=self.scope.competition_id,
                            season=self.scope.season_id,
                            game=str(candidate["game"]),
                            kickoff=candidate["date"],
                            state=state,
                            failure_code=exc.kind.value,
                            error=str(exc),
                            retry_after=retry_after,
                            attempt_no=attempt_no,
                            transport_mode=(
                                exc.route.value if exc.route else "none"
                            ),
                            proxy_mode=(
                                "filtered_paid"
                                if exc.route
                                and exc.route.value.startswith("paid_")
                                else "none"
                            ),
                            http_status=exc.status_code,
                            payload_sha256=getattr(
                                exc, "payload_sha256", None
                            ),
                            raw_uri=getattr(exc, "raw_uri", None),
                        )
                    )
                except Exception as manifest_exc:
                    result.errors.append(
                        f"preview {candidate['game_id']} failure manifest: "
                        f"{type(manifest_exc).__name__}: {manifest_exc}"
                    )
                else:
                    if state == "retryable":
                        result.retryable.append(str(candidate["game_id"]))
                    elif state == "terminal":
                        result.terminal.append(str(candidate["game_id"]))
                    else:
                        # Parser/content drift must fail visibly. Transport
                        # has already persisted the raw payload and never
                        # escalates validator failures to paid proxy.
                        result.errors.append(
                            f"preview {candidate['game_id']}: {exc}"
                        )
                continue

            try:
                parsed = parse_preview_html(
                    response.text,
                    scope=self.scope,
                    game_id=candidate["game_id"],
                    game=candidate["game"],
                    home_team=candidate["home_team"],
                    away_team=candidate["away_team"],
                )
                paid_route = response.route.value.startswith("paid_")
                self.repository.commit_preview(
                    PreviewCommit(
                        game_id=int(candidate["game_id"]),
                        league=self.scope.competition_id,
                        season=self.scope.season_id,
                        game=str(candidate["game"]),
                        kickoff=candidate["date"],
                        payload_sha256=response.sha256,
                        raw_uri=raw_uri,
                        missing_players=parsed.rows,
                        transport_mode=response.route.value,
                        proxy_mode="filtered_paid" if paid_route else "none",
                        http_status=response.status_code,
                        direct_bytes=0 if paid_route else response.wire_bytes,
                        paid_bytes=response.wire_bytes if paid_route else 0,
                        attempt_no=int(candidate["attempt_no"]),
                    )
                )
                result.succeeded += 1
                result.counts["missing_players"] = (
                    result.counts.get("missing_players", 0) + parsed.row_count
                )
            except WhoScoredParseError as exc:
                paid_route = response.route.value.startswith("paid_")
                try:
                    self.repository.record_preview_failure(
                        PreviewFailure(
                            game_id=int(candidate["game_id"]),
                            league=self.scope.competition_id,
                            season=self.scope.season_id,
                            game=str(candidate["game"]),
                            kickoff=candidate["date"],
                            state="parse_failed",
                            failure_code=FailureKind.CONTENT.value,
                            error=str(exc),
                            retry_after=None,
                            attempt_no=int(candidate["attempt_no"]),
                            payload_sha256=response.sha256,
                            raw_uri=raw_uri,
                            transport_mode=response.route.value,
                            proxy_mode="filtered_paid" if paid_route else "none",
                            http_status=response.status_code,
                            direct_bytes=0 if paid_route else response.wire_bytes,
                            paid_bytes=response.wire_bytes if paid_route else 0,
                        )
                    )
                except Exception as manifest_exc:
                    result.errors.append(
                        f"preview {candidate['game_id']} failure manifest: "
                        f"{type(manifest_exc).__name__}: {manifest_exc}"
                    )
                result.errors.append(f"preview {candidate['game_id']}: {exc}")
            except Exception as exc:
                result.errors.append(
                    f"preview {candidate['game_id']}: {type(exc).__name__}: {exc}"
                )
        if candidates:
            result.tables.extend(
                [
                    "iceberg.bronze.whoscored_missing_players",
                    "iceberg.bronze.whoscored_preview_ingest_manifest",
                ]
            )
        return result

    def _profile_candidate_scopes(
        self,
        selectors: Optional[Iterable[CatalogSeason | WhoScoredScope | str]],
    ) -> tuple[WhoScoredScope, ...]:
        """Resolve and deduplicate the roster scopes for one global profile run."""
        raw_selectors = (self.scope,) if selectors is None else tuple(selectors)
        resolved: list[WhoScoredScope] = []
        seen: set[str] = set()
        for selector in raw_selectors:
            if isinstance(selector, CatalogSeason):
                configured = self.catalog.resolve_scope(
                    selector.scope.competition_id, selector.scope.season_id
                )
            elif isinstance(selector, WhoScoredScope):
                configured = self.catalog.resolve_scope(
                    selector.competition_id, selector.season_id
                )
            elif isinstance(selector, str):
                configured = self.catalog.parse_scope_spec(selector)
            else:
                raise TypeError(
                    "profile candidate scopes must be catalog scopes or canonical specs"
                )
            competition = self.catalog.competition(
                configured.scope.competition_id
            )
            if not competition.whoscored_enabled:
                raise ValueError(
                    f"WhoScored is not enabled for {configured.scope.competition_id}"
                )
            if configured.scope.spec not in seen:
                seen.add(configured.scope.spec)
                resolved.append(configured.scope)
        if not resolved:
            raise ValueError("profile ingestion requires at least one candidate scope")
        return tuple(resolved)

    def sync_profiles(
        self,
        *,
        limit: int = 200,
        candidate_scopes: Optional[
            Iterable[CatalogSeason | WhoScoredScope | str]
        ] = None,
    ) -> EntityResult:
        scopes = self._profile_candidate_scopes(candidate_scopes)
        result = EntityResult(
            "profiles", ",".join(scope.spec for scope in scopes)
        )
        player_ids = self.repository.list_profile_candidates(
            scopes=scopes,
            limit=limit,
        )
        result.attempted = len(player_ids)
        self._bound_paid_fallback(len(player_ids))
        for player_id in player_ids:
            target = profile_page_target(player_id)

            def validate(response: TransportResponse) -> bool:
                parsed = parse_profile_html(response.text, player_id=player_id)
                row = parsed.rows[0]
                useful = sum(
                    row.get(key) is not None
                    for key in (
                        "date_of_birth", "height_cm", "nationality", "current_team_id"
                    )
                )
                if not row.get("name") or useful < 2:
                    raise WhoScoredParseError("profile lacks required identity fields")
                return True

            try:
                response, raw_uri = self._fetch(
                    target, validator=validate, allow_cache=True
                )
            except WhoScoredTransportError as exc:
                # Local safety ceilings and storage/proxy infrastructure
                # failures are task-scoped, not permanent facts about a
                # player.  Marking them terminal would blacklist the profile
                # forever after one capped run.
                retryable_kinds = {
                    FailureKind.BUDGET,
                    FailureKind.CACHE,
                    FailureKind.CONFIG,
                    FailureKind.PROXY,
                    FailureKind.TIMEOUT,
                }
                if exc.kind is FailureKind.CONTENT:
                    state = "parse_failed"
                else:
                    state = (
                        "retryable"
                        if exc.retryable or exc.kind in retryable_kinds
                        else "terminal"
                    )
                retry_after = (
                    datetime.now(timezone.utc).replace(tzinfo=None)
                    + timedelta(hours=24)
                    if state == "retryable"
                    else None
                )
                try:
                    self.repository.record_profile_failure(
                        player_id=player_id,
                        state=state,
                        failure_code=exc.kind.value,
                        error=str(exc),
                        retry_after=retry_after,
                        transport_mode=exc.route.value if exc.route else "none",
                        proxy_mode=(
                            "filtered_paid"
                            if exc.route and exc.route.value.startswith("paid_")
                            else "none"
                        ),
                        http_status=exc.status_code,
                        payload_sha256=getattr(exc, "payload_sha256", None),
                        raw_uri=getattr(exc, "raw_uri", None),
                    )
                except Exception as manifest_exc:
                    result.errors.append(
                        f"profile {player_id} failure manifest: "
                        f"{type(manifest_exc).__name__}: {manifest_exc}"
                    )
                else:
                    if state == "parse_failed":
                        result.errors.append(f"profile {player_id}: {exc}")
                    else:
                        target_list = (
                            result.retryable
                            if state == "retryable"
                            else result.terminal
                        )
                        target_list.append(str(player_id))
                continue

            try:
                parsed = parse_profile_html(response.text, player_id=player_id)
                profile = dict(parsed.rows[0])
                if profile.get("date_of_birth"):
                    profile["date_of_birth"] = date.fromisoformat(
                        profile["date_of_birth"]
                    )
            except (WhoScoredParseError, ValueError) as exc:
                paid_route = response.route.value.startswith("paid_")
                try:
                    self.repository.record_profile_failure(
                        player_id=player_id,
                        state="parse_failed",
                        failure_code=FailureKind.CONTENT.value,
                        error=str(exc),
                        retry_after=None,
                        transport_mode=response.route.value,
                        proxy_mode="filtered_paid" if paid_route else "none",
                        http_status=response.status_code,
                        direct_bytes=0 if paid_route else response.wire_bytes,
                        paid_bytes=response.wire_bytes if paid_route else 0,
                        payload_sha256=response.sha256,
                        raw_uri=raw_uri,
                    )
                except Exception as manifest_exc:
                    result.errors.append(
                        f"profile {player_id} failure manifest: "
                        f"{type(manifest_exc).__name__}: {manifest_exc}"
                    )
                else:
                    result.errors.append(f"profile {player_id}: {exc}")
                continue

            try:
                self.repository.commit_profile(
                    player_id=player_id,
                    profile=profile,
                    payload_sha256=response.sha256,
                    raw_uri=raw_uri,
                    transport_mode=response.route.value,
                    proxy_mode=(
                        "filtered_paid"
                        if response.route.value.startswith("paid_")
                        else "none"
                    ),
                    direct_bytes=(
                        response.wire_bytes
                        if not response.route.value.startswith("paid_")
                        else 0
                    ),
                    paid_bytes=(
                        response.wire_bytes
                        if response.route.value.startswith("paid_")
                        else 0
                    ),
                )
                result.succeeded += 1
                result.counts["player_profile"] = (
                    result.counts.get("player_profile", 0) + 1
                )
            except Exception as exc:
                result.errors.append(
                    f"profile {player_id}: {type(exc).__name__}: {exc}"
                )
        if player_ids:
            result.tables.extend(
                [
                    "iceberg.bronze.whoscored_player_profile_versions",
                    "iceberg.bronze.whoscored_profile_ingest_manifest",
                ]
            )
        return result

    def traffic_stats(self) -> dict[str, Any]:
        return self.transport.get_traffic_stats()

    def close(self) -> None:
        self.transport.close()

    def __enter__(self) -> "WhoScoredIngestService":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False
