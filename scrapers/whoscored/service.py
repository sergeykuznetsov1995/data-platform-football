"""Application service for the WhoScored V2 ingestion pipeline."""

from __future__ import annotations

import hashlib
import json
import os
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from urllib.parse import urlencode, urljoin

from .catalog import (
    CatalogSeason,
    WhoScoredCatalog,
    apply_schedule_classification,
    build_technical_exclusion_audit,
)
from .domain import SeasonFormat, TournamentEligibility, WhoScoredScope
from .detailed_feeds import (
    DETAILED_FEED_CATALOG,
    DetailedFeedFamily,
)
from .parsers import (
    MAX_PLAYER_STAGE_STAT_PAGES,
    PARSER_VERSION,
    DatasetStatus,
    MatchCentreDataAbsent,
    ParsedDataset,
    PlayerStageStatisticsPage,
    WhoScoredParseError,
    find_source_season_id,
    is_valid_match_page_without_matchcentre,
    merge_player_stage_statistics_pages,
    parse_all_regions,
    parse_calendar_months,
    parse_match_html,
    parse_preview_bundle,
    parse_profile_bundle,
    parse_schedule_bets,
    parse_schedule_incidents,
    parse_schedule_json,
    parse_season_page,
    parse_season_tables,
    parse_team_stage_statistics,
    parse_player_stage_statistics_page,
    parse_referee_stage_statistics_html,
    parse_tournament_seasons,
)
from .raw_store import (
    RawObjectNotFound,
    RawObjectRecord,
    RawTarget,
    RawTargetLockTimeout,
    WhoScoredRawStore,
    match_page_target,
    preview_page_target,
    profile_page_target,
    schedule_month_target,
    stage_page_target,
)
from .runtime_limits import SOURCE_PAGE_REQUESTS_PER_MINUTE
from .stage_feeds import (
    STAGE_TEAM_FEED_CATALOG,
    parse_stage_team_feed,
    stage_team_feed_url,
)
from .repository import (
    canonical_catalog_raw_inputs,
    canonical_catalog_rows,
    catalog_raw_provenance_sha256,
    catalog_payload_sha256,
    entity_id_payload_sha256,
    deterministic_match_not_available_batch_id,
    deterministic_preview_not_available_batch_id,
    deterministic_profile_not_available_batch_id,
    ManifestFailure,
    MatchCommit,
    MATCH_REFRESH_DAYS,
    MATCH_NOT_AVAILABLE_BATCH_ID_PREFIX,
    PREVIEW_NOT_AVAILABLE_BATCH_ID_PREFIX,
    PROFILE_NOT_AVAILABLE_BATCH_ID_PREFIX,
    PROFILE_REFRESH_DAYS,
    ProfileCommit,
    PreviewCommit,
    PreviewFailure,
    scope_write_chunk_rows_from_env,
    WhoScoredScopeRowSpool,
    WhoScoredRepository,
)
from .transport import (
    CachedPayload,
    FailureKind,
    FetchRequest,
    TransportBudgets,
    TransportContext,
    TransportResponse,
    WhoScoredTransport,
    WhoScoredTransportError,
)

_SOURCE_STAGE_HEADER_UNAVAILABLE = "WhoScored page request header is unavailable."


def _is_lower_sha256(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _is_source_stage_statistics_unavailable(exc: WhoScoredTransportError) -> bool:
    return exc.kind is FailureKind.BROWSER and _SOURCE_STAGE_HEADER_UNAVAILABLE in str(
        exc
    )


ACTIVE_SCHEDULE_CACHE_TTL = timedelta(hours=6)
MIN_INITIAL_CATALOG_TOURNAMENTS = 100
DEFAULT_STRUCTURED_REQUESTS_PER_MINUTE = 60
MAX_STRUCTURED_REQUESTS_PER_MINUTE = 60
STRUCTURED_REQUEST_BURST_SIZE = 4
STRUCTURED_PARSE_BATCH_SIZE = 8
PLAYER_STAGE_PAGINATION_BATCH_SIZE = 8
DEFAULT_CATALOG_REQUESTS_PER_MINUTE = 60
MAX_CATALOG_REQUESTS_PER_MINUTE = 60
CATALOG_REQUEST_BURST_SIZE = 4
TEAM_STAGE_STATISTICS_ENDPOINT = (
    "https://www.whoscored.com/statisticsfeed/1/getteamstatistics"
)
PLAYER_STAGE_STATISTICS_ENDPOINT = (
    "https://www.whoscored.com/statisticsfeed/1/getplayerstatistics"
)
TEAM_STAGE_STAT_TABS: tuple[tuple[str, str, str, str, str, str], ...] = (
    ("summaryteam", "all", "Rating", "", "", ""),
    ("summaryteam", "offensive", "shotsPerGame", "", "", ""),
    ("summaryteam", "defensive", "tacklePerGame", "", "", ""),
    # Current official xG tab emits these three literal boolean filters. They
    # are not interchangeable with the empty defaults of the other tabs.
    ("xg-teamstats", "summary", "xG", "false", "true", "false"),
)
# These are the five tabs emitted by the current player-statistics UI.  Keep
# category/subcategory pairs explicit: the endpoint does not accept the old
# shorthand categories (``defensive/all``, ``xg/all``).
PLAYER_STAGE_STAT_TABS: tuple[tuple[str, str, bool], ...] = (
    ("summary", "all", False),
    ("summary", "defensive", False),
    ("summary", "offensive", False),
    ("summary", "passing", False),
    ("xg-stats", "summary", True),
)
TEAM_DETAILED_STAT_TABS = tuple(
    spec for spec in DETAILED_FEED_CATALOG if spec.family is DetailedFeedFamily.TEAM
)
PLAYER_DETAILED_STAT_TABS = tuple(
    spec for spec in DETAILED_FEED_CATALOG if spec.family is DetailedFeedFamily.PLAYER
)


def structured_requests_per_minute_from_env(
    environ: Mapping[str, str] = os.environ,
) -> int:
    """Return the fail-closed per-task structured-source request ceiling."""

    raw = environ.get(
        "WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE",
        str(DEFAULT_STRUCTURED_REQUESTS_PER_MINUTE),
    )
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE must be an integer "
            f"between 1 and {MAX_STRUCTURED_REQUESTS_PER_MINUTE}"
        ) from exc
    if (
        str(raw).strip() != str(value)
        or not 1 <= value <= MAX_STRUCTURED_REQUESTS_PER_MINUTE
    ):
        raise ValueError(
            "WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE must be an integer "
            f"between 1 and {MAX_STRUCTURED_REQUESTS_PER_MINUTE}"
        )
    return value


def catalog_requests_per_minute_from_env(
    environ: Mapping[str, str] = os.environ,
) -> int:
    """Return the fail-closed catalog discovery request ceiling."""

    raw = environ.get(
        "WHOSCORED_CATALOG_REQUESTS_PER_MINUTE",
        str(DEFAULT_CATALOG_REQUESTS_PER_MINUTE),
    )
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "WHOSCORED_CATALOG_REQUESTS_PER_MINUTE must be an integer "
            f"between 1 and {MAX_CATALOG_REQUESTS_PER_MINUTE}"
        ) from exc
    if (
        str(raw).strip() != str(value)
        or not 1 <= value <= MAX_CATALOG_REQUESTS_PER_MINUTE
    ):
        raise ValueError(
            "WHOSCORED_CATALOG_REQUESTS_PER_MINUTE must be an integer "
            f"between 1 and {MAX_CATALOG_REQUESTS_PER_MINUTE}"
        )
    return value


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
    committed_batches: dict[str, list[str]] = field(default_factory=dict)
    attempted_snapshots: dict[str, dict[str, Any]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    traffic: dict[str, Any] = field(default_factory=dict)

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
            "committed_batches": {
                str(kind): list(batch_ids)
                for kind, batch_ids in self.committed_batches.items()
            },
            "attempted_snapshots": {
                str(kind): dict(snapshot)
                for kind, snapshot in self.attempted_snapshots.items()
            },
            "metadata": dict(self.metadata),
            "errors": list(self.errors),
            "traffic": dict(self.traffic),
        }


@dataclass(frozen=True)
class _ParsedFetchSpec:
    target: RawTarget
    parser: Callable[[TransportResponse], Any]
    content_type: str = "text/html"
    allow_cache: bool = True
    cache_ttl: Optional[timedelta] = None
    browser_bootstrap_url: Optional[str] = None
    page_factory: Optional[Callable[[int], _ParsedFetchSpec]] = None
    page_merger: Optional[
        Callable[[Sequence[PlayerStageStatisticsPage]], ParsedDataset]
    ] = None


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
        return CachedPayload(
            content=content,
            status_code=200,
            headers={},
            observed_at=record.fetched_at,
        )

    def store(self, key: str, payload: CachedPayload, sha256: str) -> None:
        if key != self.target.target_id:
            raise ValueError(f"raw cache key mismatch: {key!r}")
        record = self.backend.store_bytes(
            self.target,
            payload.content,
            content_type=self.content_type,
            charset="utf-8",
            fetched_at=payload.observed_at,
        )
        if record.content_hash != sha256:
            raise ValueError("raw store hash differs from transport hash")
        self.record = record

    @property
    def raw_uri(self) -> str:
        if self.record is None:
            raise RuntimeError("raw target has not been committed")
        return self.backend.object_uri(self.record.blob_key)


class _BatchRawCache:
    """Dispatch transport cache operations to independent typed raw targets."""

    def __init__(self, adapters: Sequence[_TargetRawCache]) -> None:
        self._adapters = {adapter.target.target_id: adapter for adapter in adapters}
        if len(self._adapters) != len(adapters):
            raise ValueError("batch raw targets must be unique")

    def load(self, key: str) -> Optional[CachedPayload]:
        try:
            adapter = self._adapters[key]
        except KeyError as exc:
            raise ValueError(f"unknown batch raw cache key: {key!r}") from exc
        return adapter.load(key)

    def store(self, key: str, payload: CachedPayload, sha256: str) -> None:
        try:
            adapter = self._adapters[key]
        except KeyError as exc:
            raise ValueError(f"unknown batch raw cache key: {key!r}") from exc
        adapter.store(key, payload, sha256)


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
        self.repository = repository or WhoScoredRepository()
        if catalog is None:
            try:
                discovered = self.repository.load_discovered_catalog()
                if not isinstance(discovered, WhoScoredCatalog):
                    raise TypeError("repository returned an invalid discovered catalog")
                self.catalog = discovered
            except (LookupError, TypeError, AttributeError) as exc:
                raise LookupError(
                    "WhoScored persisted discovery catalog is unavailable; "
                    "run the discovery workflow before constructing a scope service"
                ) from exc
        else:
            self.catalog = catalog
        self.catalog_season = (
            scope
            if isinstance(scope, CatalogSeason)
            else self.catalog.resolve_scope(scope.competition_id, scope.season_id)
        )
        self.scope = self.catalog_season.scope
        self.competition = self.catalog.competition(self.scope.competition_id)
        self.raw_store = raw_store or WhoScoredRawStore.from_env(optional=False)
        self.transport = transport or WhoScoredTransport(
            flaresolverr_url=os.environ.get(
                "FLARESOLVERR_URL", "http://flaresolverr:8191"
            ),
            paid_gateway_url=(
                os.environ.get("WHOSCORED_PAID_GATEWAY_URL", "").strip() or None
            ),
            context=TransportContext.from_env().request_context(scope=self.scope.spec),
        )
        from scrapers.utils.rate_limiter import RateLimiter

        self._rate_limiter = RateLimiter(
            max_requests=SOURCE_PAGE_REQUESTS_PER_MINUTE,
            window_seconds=60,
        )
        self._structured_rate_limiter = RateLimiter(
            max_requests=structured_requests_per_minute_from_env(),
            window_seconds=60,
            burst_size=STRUCTURED_REQUEST_BURST_SIZE,
        )
        self._schema_ready = False

    def _ensure_schema_once(self) -> None:
        if self._schema_ready or os.environ.get("WHOSCORED_SCHEMA_READY") == "1":
            return
        self.repository.ensure_schema()
        self._schema_ready = True

    def _fetch(
        self,
        target: RawTarget,
        *,
        validator: Callable[[TransportResponse], Optional[bool]],
        content_type: str = "text/html",
        allow_cache: bool = True,
        cache_ttl: Optional[timedelta] = None,
        browser_bootstrap_url: Optional[str] = None,
    ) -> tuple[TransportResponse, str]:
        """Singleflight one raw target, then re-check cache under the lock."""

        lock = getattr(self.raw_store, "target_lock", None)
        if callable(lock):
            try:
                with lock(target):
                    return self._fetch_unlocked(
                        target,
                        validator=validator,
                        content_type=content_type,
                        allow_cache=allow_cache,
                        cache_ttl=cache_ttl,
                        browser_bootstrap_url=browser_bootstrap_url,
                    )
            except RawTargetLockTimeout as exc:
                raise WhoScoredTransportError(
                    str(exc),
                    kind=FailureKind.CACHE,
                    url=target.canonical_url,
                    retryable=True,
                ) from exc
        # Lightweight discovery tests may inject a minimal raw-store seam.
        return self._fetch_unlocked(
            target,
            validator=validator,
            content_type=content_type,
            allow_cache=allow_cache,
            cache_ttl=cache_ttl,
            browser_bootstrap_url=browser_bootstrap_url,
        )

    def _fetch_unlocked(
        self,
        target: RawTarget,
        *,
        validator: Callable[[TransportResponse], Optional[bool]],
        content_type: str = "text/html",
        allow_cache: bool = True,
        cache_ttl: Optional[timedelta] = None,
        browser_bootstrap_url: Optional[str] = None,
    ) -> tuple[TransportResponse, str]:
        cache_is_fresh = (
            allow_cache
            and cache_ttl is not None
            and self.raw_store.is_fresh(target, max_age=cache_ttl)
        )
        cache_load_allowed = allow_cache and (cache_ttl is None or cache_is_fresh)
        adapter = _TargetRawCache(
            self.raw_store,
            target,
            allow_load=cache_load_allowed,
            content_type=content_type,
        )
        previous = self.transport.raw_cache
        self.transport.raw_cache = adapter
        try:
            response = self.transport.fetch(
                target.canonical_url,
                cache_key=target.target_id,
                validator=validator,
                before_network=self._rate_limiter.acquire,
                scope=(self.scope.spec if getattr(self, "scope", None) else "catalog"),
                entity=target.page_kind,
                browser_bootstrap_url=browser_bootstrap_url,
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

    def _fetch_parsed(
        self,
        target: RawTarget,
        *,
        parser: Callable[[TransportResponse], Any],
        content_type: str = "text/html",
        allow_cache: bool = True,
        cache_ttl: Optional[timedelta] = None,
        browser_bootstrap_url: Optional[str] = None,
    ) -> tuple[TransportResponse, str, Any]:
        """Run the parser exactly once as the transport validator."""
        holder: dict[str, Any] = {}

        def validate(response: TransportResponse) -> bool:
            try:
                holder["parsed"] = parser(response)
            except Exception as exc:
                raise WhoScoredParseError(
                    f"{target.target_id}: {type(exc).__name__}: {exc}"
                ) from exc
            return True

        response, raw_uri = self._fetch(
            target,
            validator=validate,
            content_type=content_type,
            allow_cache=allow_cache,
            cache_ttl=cache_ttl,
            browser_bootstrap_url=browser_bootstrap_url,
        )
        if "parsed" not in holder:
            raise WhoScoredParseError(
                f"validator did not parse raw target {target.target_id}"
            )
        return response, raw_uri, holder["parsed"]

    def _fetch_parsed_many(
        self, specs: Sequence[_ParsedFetchSpec]
    ) -> list[tuple[TransportResponse, str, Any]]:
        """Fetch and validate one atomic structured-feed group.

        Target locks are acquired in stable order, each raw target keeps its
        own cache object and parser validator, and no parsed result is returned
        until the transport has completed the full group.
        """

        items = tuple(specs)
        if not items:
            return []
        target_ids = [item.target.target_id for item in items]
        if len(set(target_ids)) != len(target_ids):
            raise WhoScoredParseError("structured feed targets must be unique")

        holders: list[dict[str, Any]] = [{} for _ in items]
        validators: list[Callable[[TransportResponse], bool]] = []
        for item, holder in zip(items, holders):

            def validate(
                response: TransportResponse,
                *,
                current=item,
                current_holder=holder,
            ) -> bool:
                try:
                    current_holder["parsed"] = current.parser(response)
                except Exception as exc:
                    raise WhoScoredParseError(
                        f"{current.target.target_id}: {type(exc).__name__}: {exc}"
                    ) from exc
                return True

            validators.append(validate)

        adapters: list[_TargetRawCache] = []

        def run_batch() -> list[TransportResponse]:
            nonlocal adapters
            # Freshness must be re-evaluated only after every target lock is
            # held. Another task may have completed this raw group while this
            # task was waiting; using a pre-lock snapshot would duplicate all
            # of its source requests.
            adapters = [
                _TargetRawCache(
                    self.raw_store,
                    item.target,
                    allow_load=(
                        item.allow_cache
                        and (
                            item.cache_ttl is None
                            or self.raw_store.is_fresh(
                                item.target, max_age=item.cache_ttl
                            )
                        )
                    ),
                    content_type=item.content_type,
                )
                for item in items
            ]
            previous = self.transport.raw_cache
            self.transport.raw_cache = _BatchRawCache(adapters)
            try:
                return self.transport.fetch_many(
                    [
                        FetchRequest(
                            url=item.target.canonical_url,
                            cache_key=item.target.target_id,
                            validator=validator,
                            before_network=self._structured_rate_limiter.acquire,
                            scope=(
                                self.scope.spec
                                if getattr(self, "scope", None)
                                else "catalog"
                            ),
                            entity=item.target.page_kind,
                            browser_bootstrap_url=item.browser_bootstrap_url,
                        )
                        for item, validator in zip(items, validators)
                    ]
                )
            except WhoScoredTransportError as exc:
                matching = next(
                    (
                        adapter
                        for adapter in adapters
                        if adapter.target.canonical_url == exc.url
                        or adapter.target.target_id == getattr(exc, "cache_key", None)
                    ),
                    None,
                )
                if matching is not None and matching.record is not None:
                    exc.payload_sha256 = matching.record.content_hash
                    exc.raw_uri = matching.raw_uri
                raise
            finally:
                self.transport.raw_cache = previous

        lock = getattr(self.raw_store, "target_lock", None)
        try:
            if callable(lock):
                with ExitStack() as stack:
                    for item in sorted(items, key=lambda value: value.target.target_id):
                        stack.enter_context(lock(item.target))
                    responses = run_batch()
            else:
                responses = run_batch()
        except RawTargetLockTimeout as exc:
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.CACHE,
                url=items[0].target.canonical_url,
                retryable=True,
            ) from exc

        if len(responses) != len(items):
            raise WhoScoredParseError("structured feed transport batch is incomplete")
        parsed_results: list[tuple[TransportResponse, str, Any]] = []
        for item, holder, adapter, response in zip(items, holders, adapters, responses):
            if "parsed" not in holder:
                raise WhoScoredParseError(
                    f"validator did not parse raw target {item.target.target_id}"
                )
            parsed_results.append((response, adapter.raw_uri, holder["parsed"]))
        return parsed_results

    def _complete_player_statistics_pages(
        self,
        specs: Sequence[_ParsedFetchSpec],
        results: Sequence[tuple[TransportResponse, str, Any]],
    ) -> tuple[
        list[tuple[TransportResponse, str, Any]],
        list[tuple[TransportResponse, str]],
    ]:
        """Fetch every declared player-stat page and replace page-one envelopes."""

        if len(specs) != len(results):
            raise WhoScoredParseError("structured feed result/spec count mismatch")
        completed = list(results)
        additional_raw: list[tuple[TransportResponse, str]] = []
        for index, (spec, result) in enumerate(zip(specs, results)):
            first_response, first_uri, parsed = result
            if not isinstance(parsed, PlayerStageStatisticsPage):
                continue
            if spec.page_factory is None or spec.page_merger is None:
                raise WhoScoredParseError(
                    f"{spec.target.target_id} declared pagination without a handler"
                )
            if parsed.total_pages > MAX_PLAYER_STAGE_STAT_PAGES:
                raise WhoScoredParseError(
                    f"{spec.target.target_id} pagination exceeds its safety bound"
                )
            pages = [parsed]
            for first_page in range(
                2,
                parsed.total_pages + 1,
                PLAYER_STAGE_PAGINATION_BATCH_SIZE,
            ):
                stop_page = min(
                    parsed.total_pages + 1,
                    first_page + PLAYER_STAGE_PAGINATION_BATCH_SIZE,
                )
                next_specs = [
                    spec.page_factory(page_number)
                    for page_number in range(first_page, stop_page)
                ]
                for response, uri, next_page in self._fetch_parsed_many(next_specs):
                    if not isinstance(next_page, PlayerStageStatisticsPage):
                        raise WhoScoredParseError(
                            f"{spec.target.target_id} pagination ended before its "
                            f"declared {parsed.total_pages} pages"
                        )
                    pages.append(next_page)
                    additional_raw.append((response, uri))
            completed[index] = (
                first_response,
                first_uri,
                spec.page_merger(pages),
            )
        return completed, additional_raw

    @staticmethod
    def _entity_keyed(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        keyed: list[dict[str, Any]] = []
        seen: set[str] = set()
        for source in rows:
            row = dict(source)
            payload = json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            key = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            if key in seen:
                continue
            seen.add(key)
            row["entity_key"] = key
            keyed.append(row)
        return keyed

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

    def _team_stage_statistics_spec(
        self,
        *,
        stage_id: int,
        source_season_id: int,
        active: bool,
        category: str,
        subcategory: str,
        sort_by: str,
        sort_ascending: str = "",
        inc_pens: str = "",
        against: str = "",
        detailed: bool = False,
        cache_ttl: Optional[timedelta],
        browser_bootstrap_url: str,
    ) -> _ParsedFetchSpec:
        params = {
            "category": category,
            "subcategory": subcategory,
            "statsAccumulationType": 0,
            "field": "Overall",
            "tournamentOptions": "",
            "timeOfTheGameStart": "",
            "timeOfTheGameEnd": "",
            "teamIds": "",
            "stageId": stage_id,
            "sortBy": sort_by,
            "sortAscending": sort_ascending,
            "page": "",
            "numberOfTeamsToPick": "",
            "isCurrent": str(active).lower(),
            "formation": "",
            "incPens": inc_pens,
            "against": against,
        }
        url = f"{TEAM_STAGE_STATISTICS_ENDPOINT}?{urlencode(params)}"
        family = "detailed:" if detailed else ""
        target = self._html_target(
            page_kind="team_stage_statistics",
            target_id=(
                f"whoscored:team-stats:{stage_id}:{family}{category}:{subcategory}"
            ),
            url=url,
            source_ids={
                "stage_id": str(stage_id),
                "category": category,
                "subcategory": subcategory,
            },
        )
        return _ParsedFetchSpec(
            target=target,
            parser=lambda response: parse_team_stage_statistics(
                response.content,
                scope=self.scope,
                stage_id=stage_id,
                source_season_id=source_season_id,
                source_category=category,
                source_subcategory=subcategory,
            ),
            content_type="application/json",
            cache_ttl=cache_ttl,
            browser_bootstrap_url=browser_bootstrap_url,
        )

    def _player_stage_statistics_spec(
        self,
        *,
        stage_id: int,
        source_season_id: int,
        active: bool,
        category: str,
        subcategory: str,
        sort_by: str = "Rating",
        inc_pens: bool = False,
        detailed: bool = False,
        page: int = 1,
        cache_ttl: Optional[timedelta],
        browser_bootstrap_url: str,
    ) -> _ParsedFetchSpec:
        params: dict[str, Any] = {
            "category": category,
            "subcategory": subcategory,
            "statsAccumulationType": 0,
            "isCurrent": str(active).lower(),
            "playerId": "",
            "teamIds": "",
            "matchId": "",
            "stageId": stage_id,
            "tournamentOptions": int(self.competition.tournament_id),
            "sortBy": sort_by,
            "sortAscending": "",
            "age": "",
            "ageComparisonType": "",
            "appearances": "",
            "appearancesComparisonType": "",
            "field": "Overall",
            "nationality": "",
            "positionOptions": "",
            "timeOfTheGameEnd": "",
            "timeOfTheGameStart": "",
            "isMinApp": "true",
            "page": int(page),
            "includeZeroValues": "true",
            "numberOfPlayersToPick": 5000,
        }
        if inc_pens:
            params["incPens"] = "true"
        url = f"{PLAYER_STAGE_STATISTICS_ENDPOINT}?{urlencode(params)}"
        family = "detailed:" if detailed else ""
        page_suffix = "" if int(page) == 1 else f":page:{int(page)}"
        target = self._html_target(
            page_kind="player_stage_statistics",
            target_id=(
                f"whoscored:player-stats:{stage_id}:{family}{category}:{subcategory}"
                f"{page_suffix}"
            ),
            url=url,
            source_ids={
                "stage_id": str(stage_id),
                "category": category,
                "subcategory": subcategory,
                "page": str(int(page)),
            },
        )
        return _ParsedFetchSpec(
            target=target,
            parser=lambda response: parse_player_stage_statistics_page(
                response.content,
                scope=self.scope,
                stage_id=stage_id,
                expected_page=int(page),
                source_season_id=source_season_id,
                source_category=category,
                source_subcategory=subcategory,
            ),
            content_type="application/json",
            cache_ttl=cache_ttl,
            browser_bootstrap_url=browser_bootstrap_url,
            page_factory=lambda next_page: self._player_stage_statistics_spec(
                stage_id=stage_id,
                source_season_id=source_season_id,
                active=active,
                category=category,
                subcategory=subcategory,
                sort_by=sort_by,
                inc_pens=inc_pens,
                detailed=detailed,
                page=next_page,
                cache_ttl=cache_ttl,
                browser_bootstrap_url=browser_bootstrap_url,
            ),
            page_merger=lambda pages: merge_player_stage_statistics_pages(
                pages,
                scope=self.scope,
                stage_id=stage_id,
                source_season_id=source_season_id,
                source_category=category,
                source_subcategory=subcategory,
            ),
        )

    @classmethod
    def discover_catalog(
        cls,
        *,
        as_of_date: date,
        repository: Optional[WhoScoredRepository] = None,
        transport: Optional[WhoScoredTransport] = None,
        raw_store: Optional[WhoScoredRawStore] = None,
        full_history: bool = False,
    ) -> EntityResult:
        """Discover and atomically publish the complete WhoScored catalog.

        The operation is intentionally resumable through raw targets.  It
        publishes nothing until every tournament/season/stage page required
        for the snapshot was parsed successfully.
        """
        if type(as_of_date) is not date:
            raise ValueError("as_of_date must be an exact date (not datetime)")
        as_of_iso = as_of_date.isoformat()
        result = EntityResult("catalog", "catalog", attempted=1)
        repo = repository or WhoScoredRepository()
        previous_generation: Optional[dict[str, Any]] = None
        try:
            previous_generation, previous_catalog = (
                repo.load_catalog_generation_snapshot(
                    allow_legacy_parser_for_full_history=bool(full_history)
                )
            )
        except LookupError:
            previous_catalog = None
        except Exception as exc:
            result.errors.append(f"catalog: {type(exc).__name__}: {exc}")
            return result
        store = raw_store or WhoScoredRawStore.from_env(optional=False)
        network = transport or WhoScoredTransport(
            flaresolverr_url=os.environ.get(
                "FLARESOLVERR_URL", "http://flaresolverr:8191"
            ),
            paid_gateway_url=(
                os.environ.get("WHOSCORED_PAID_GATEWAY_URL", "").strip() or None
            ),
            context=TransportContext.from_env().request_context(
                scope="catalog", entity="discovery"
            ),
        )
        bootstrap = cls.__new__(cls)
        bootstrap.repository = repo
        bootstrap.raw_store = store
        bootstrap.transport = network
        bootstrap.scope = None
        from scrapers.utils.rate_limiter import RateLimiter

        bootstrap._rate_limiter = RateLimiter(
            max_requests=catalog_requests_per_minute_from_env(),
            window_seconds=60,
            burst_size=CATALOG_REQUEST_BURST_SIZE,
        )
        raw_inputs: list[dict[str, str]] = []
        root_raw_uri = ""
        # The first atomic snapshot must always contain historical stage
        # evidence.  Making that safety property automatic keeps the scheduled
        # production DAG bootstrappable; callers cannot accidentally publish a
        # latest-season-only catalog merely by omitting a one-time CLI flag.
        # Once a successful snapshot exists, ordinary discovery remains
        # incremental unless an explicit backfill requests full history.
        full_history = bool(full_history or previous_catalog is None)
        parent_catalog_batch_id: Optional[str] = None
        parent_catalog_payload_sha256: Optional[str] = None
        parent_catalog_raw_provenance_sha256: Optional[str] = None
        parent_generation_error: Optional[str] = None
        if not full_history:
            if previous_generation is None:
                parent_generation_error = (
                    "incremental catalog requires an exact parent generation"
                )
            else:
                parent_catalog_batch_id = str(
                    previous_generation.get("catalog_batch_id") or ""
                )
                parent_catalog_payload_sha256 = str(
                    previous_generation.get("catalog_payload_sha256") or ""
                )
                parent_catalog_raw_provenance_sha256 = str(
                    previous_generation.get("catalog_raw_provenance_sha256") or ""
                )
                if (
                    not parent_catalog_batch_id
                    or not _is_lower_sha256(parent_catalog_payload_sha256)
                    or not _is_lower_sha256(parent_catalog_raw_provenance_sha256)
                ):
                    parent_generation_error = (
                        "incremental catalog parent generation is incomplete"
                    )
        previous_activity: dict[tuple[str, int], bool] = {}
        previous_season_keys: set[tuple[str, int]] = set()
        previous_season_rows: dict[tuple[str, int], dict[str, Any]] = {}
        if previous_catalog is not None:
            for item in previous_catalog.all_scopes():
                if item.source_season_id is not None:
                    key = (item.scope.competition_id, int(item.source_season_id))
                    previous_activity[key] = bool(item.is_active)
                    previous_season_keys.add(key)
            for row in previous_catalog.to_rows().get("seasons", ()):
                if (
                    row.get("competition_id")
                    and row.get("source_season_id") is not None
                ):
                    previous_season_rows[
                        (str(row["competition_id"]), int(row["source_season_id"]))
                    ] = dict(row)

        def fetch_parsed(
            target: RawTarget,
            parser: Callable[[TransportResponse], Any],
            *,
            content_type: str = "text/html",
            cache_ttl: Optional[timedelta] = None,
        ) -> tuple[TransportResponse, str, Any]:
            parsed_holder: dict[str, Any] = {}

            def validate(response: TransportResponse) -> bool:
                try:
                    parsed_holder["value"] = parser(response)
                except Exception as exc:
                    raise WhoScoredParseError(
                        f"{target.target_id}: {type(exc).__name__}: {exc}"
                    ) from exc
                return True

            response, uri = bootstrap._fetch(
                target,
                validator=validate,
                content_type=content_type,
                allow_cache=True,
                cache_ttl=cache_ttl,
            )
            raw_inputs.append(
                {
                    "target_id": target.target_id,
                    "url": target.canonical_url,
                    "raw_uri": uri,
                    "sha256": response.sha256,
                    "as_of_date": as_of_iso,
                }
            )
            # The validator is the single parse. A raw-cache implementation
            # used by a test may omit validator execution, so fail closed.
            if "value" not in parsed_holder:
                raise WhoScoredParseError(
                    f"validator did not parse discovery target {target.target_id}"
                )
            return response, uri, parsed_holder["value"]

        try:
            if parent_generation_error is not None:
                raise RuntimeError(parent_generation_error)
            identity_catalog = WhoScoredCatalog.from_file()
            competition_aliases = {
                (int(item.region_id), int(item.tournament_id)): item.competition_id
                for item in identity_catalog.competitions
                if item.region_id is not None and item.tournament_id is not None
            }
            root_target = RawTarget(
                source="whoscored",
                page_kind="all_regions",
                target_id="whoscored:catalog:all-regions",
                canonical_url="https://www.whoscored.com/",
                source_ids={"catalog": "all-regions"},
            )
            _root_response, root_raw_uri, competitions_ds = fetch_parsed(
                root_target,
                lambda response: parse_all_regions(
                    response.content,
                    competition_aliases=competition_aliases,
                ),
                cache_ttl=timedelta(hours=24),
            )
            competition_rows = [dict(row) for row in competitions_ds.rows]
            minimum_tournaments = int(
                os.environ.get(
                    "WHOSCORED_CATALOG_MIN_TOURNAMENTS",
                    str(MIN_INITIAL_CATALOG_TOURNAMENTS),
                )
            )
            if minimum_tournaments < 1:
                raise ValueError("WHOSCORED_CATALOG_MIN_TOURNAMENTS must be positive")
            if previous_catalog is None and len(competition_rows) < minimum_tournaments:
                raise WhoScoredParseError(
                    "allRegions catalog is implausibly small: "
                    f"{len(competition_rows)} tournaments, minimum={minimum_tournaments}"
                )
            if previous_catalog is not None:
                previous_competition_keys = {
                    (item.region_id, item.tournament_id)
                    for item in previous_catalog.competitions
                    if item.region_id is not None and item.tournament_id is not None
                }
                current_competition_keys = {
                    (int(row["region_id"]), int(row["tournament_id"]))
                    for row in competition_rows
                    if row.get("region_id") is not None
                    and row.get("tournament_id") is not None
                }
                missing_competitions = sorted(
                    previous_competition_keys - current_competition_keys
                )
                if missing_competitions:
                    raise WhoScoredParseError(
                        "allRegions catalog lost previously published tournaments: "
                        + ", ".join(
                            f"{region_id}/{tournament_id}"
                            for region_id, tournament_id in missing_competitions[:20]
                        )
                    )
            season_rows: list[dict[str, Any]] = []
            stage_rows: list[dict[str, Any]] = []
            classification_schedule_rows: list[dict[str, Any]] = []
            activity_expected: set[tuple[str, int]] = set()
            activity_evidence: set[tuple[str, int]] = set()
            historical_stage_expected: set[tuple[str, int]] = set()
            historical_stage_evidence: set[tuple[str, int]] = set()
            visited_season_keys: set[tuple[str, int]] = set()

            for competition in competition_rows:
                competition_id = str(competition["competition_id"])
                region_id = int(competition["region_id"])
                tournament_id = int(competition["tournament_id"])
                # Names/source metadata already give a definitive exclusion
                # for women, youth and reserve tournaments. Keep those rows in
                # the auditable catalog without spending more requests. A
                # technical override is different: fetch its current
                # season/stage evidence so the snapshot-bound duplicate audit
                # can revalidate it instead of trusting a stale registry entry.
                if str(competition.get("eligibility", "")) in {
                    TournamentEligibility.EXCLUDED_WOMEN.value,
                    TournamentEligibility.EXCLUDED_YOUTH.value,
                    TournamentEligibility.EXCLUDED_RESERVE.value,
                }:
                    continue
                tournament_url = urljoin(
                    "https://www.whoscored.com/",
                    str(
                        competition.get("tournament_url")
                        or (f"/Regions/{region_id}/Tournaments/{tournament_id}")
                    ),
                )
                tournament_target = bootstrap._html_target(
                    page_kind="tournament_seasons",
                    target_id=f"whoscored:tournament:{region_id}:{tournament_id}",
                    url=tournament_url,
                    source_ids={
                        "competition_id": competition_id,
                        "region_id": str(region_id),
                        "tournament_id": str(tournament_id),
                    },
                )
                _, _, seasons_ds = fetch_parsed(
                    tournament_target,
                    lambda response, row=competition: parse_tournament_seasons(
                        response.text, competition_row=row
                    ),
                    cache_ttl=(
                        ACTIVE_SCHEDULE_CACHE_TTL
                        if any(
                            active
                            for (
                                known_competition,
                                _,
                            ), active in previous_activity.items()
                            if known_competition == competition_id
                        )
                        else timedelta(days=7)
                    ),
                )
                tournament_seasons = [dict(row) for row in seasons_ds.rows]
                season_rows.extend(tournament_seasons)

                # ``selected`` is merely WhoScored's dropdown default.  It is
                # retained as source metadata, while activity is derived from
                # the actual fixture horizon below.
                # Keep WhoScored's dropdown order.  The canonical split-year
                # token wraps at the century boundary (``9900`` -> ``0001``),
                # so lexical ordering can put 1999/00 ahead of 2025/26 and make
                # a future-edition fallback silently select ancient fixtures.
                recognizable_seasons = [
                    row
                    for row in tournament_seasons
                    if row.get("season_id")
                    and row.get("season_format")
                    and row.get("source_season_id")
                ]
                default_seasons = [
                    row
                    for row in recognizable_seasons
                    if row.get("source_selected") is True
                ]
                activity_seasons: list[dict[str, Any]] = []
                activity_ids: set[int] = set()
                for row in (*default_seasons, *recognizable_seasons):
                    source_id = int(row["source_season_id"])
                    if source_id not in activity_ids:
                        activity_ids.add(source_id)
                        activity_seasons.append(row)
                for row in recognizable_seasons:
                    row["is_active"] = False
                requires_stage_backfill = full_history or any(
                    (competition_id, int(row["source_season_id"]))
                    not in previous_season_keys
                    for row in recognizable_seasons
                )
                ordered_seasons = list(activity_seasons)
                if requires_stage_backfill:
                    ordered_seasons.extend(
                        row
                        for row in recognizable_seasons
                        if int(row["source_season_id"]) not in activity_ids
                    )
                activity_search_complete = False
                pending_fixture_evidence: dict[tuple[str, int], dict[str, Any]] = {}
                for season in ordered_seasons:
                    source_season_id = int(season["source_season_id"])
                    activity_candidate = (
                        not activity_search_complete
                        and source_season_id in activity_ids
                    )
                    scope = WhoScoredScope(
                        competition_id,
                        str(season["season_id"]),
                        SeasonFormat.coerce(str(season["season_format"])),
                    )
                    key = (competition_id, source_season_id)
                    visited_season_keys.add(key)
                    if activity_candidate:
                        activity_expected.add(key)
                    if requires_stage_backfill:
                        historical_stage_expected.add(key)
                    previously_active = previous_activity.get(key)
                    mutable_ttl = (
                        ACTIVE_SCHEDULE_CACHE_TTL
                        if previously_active is not False
                        else timedelta(days=7)
                    )
                    season_target = stage_page_target(
                        scope,
                        region_id=region_id,
                        tournament_id=tournament_id,
                        source_season_id=source_season_id,
                    )
                    try:
                        _, _, season_page = fetch_parsed(
                            season_target,
                            lambda response, current_scope=scope, source_id=source_season_id: (
                                parse_season_page(
                                    response.text,
                                    scope=current_scope,
                                    region_id=region_id,
                                    tournament_id=tournament_id,
                                    source_season_id=source_id,
                                )
                            ),
                            cache_ttl=mutable_ttl if activity_candidate else None,
                        )
                    except WhoScoredTransportError as exc:
                        if (
                            exc.kind is not FailureKind.HTTP_STATUS
                            or exc.status_code not in {403, 404}
                        ):
                            raise
                        season.update(
                            {
                                "is_active": False,
                                "eligibility": (
                                    TournamentEligibility.SOURCE_UNAVAILABLE.value
                                ),
                                "classification_reason": (
                                    f"season_stage_page_http_{exc.status_code}"
                                ),
                            }
                        )
                        activity_expected.discard(key)
                        historical_stage_expected.discard(key)
                        continue
                    current_stages = [dict(row) for row in season_page.stages.rows]
                    stage_rows.extend(current_stages)
                    if current_stages:
                        historical_stage_evidence.add(key)
                    else:
                        season.update(
                            {
                                "is_active": False,
                                "eligibility": (
                                    TournamentEligibility.SOURCE_UNAVAILABLE.value
                                ),
                                "classification_reason": "season_stage_list_empty",
                            }
                        )
                        activity_expected.discard(key)
                        historical_stage_expected.discard(key)
                        continue

                    if not activity_candidate:
                        continue
                    activity_dates: list[date] = []
                    schedule_source_unavailable = False
                    calendar_months_observed = False
                    try:
                        for stage in current_stages:
                            stage_id = int(stage["stage_id"])
                            calendar_target = bootstrap._html_target(
                                page_kind="stage_calendar",
                                target_id=f"whoscored:calendar:{stage_id}",
                                url=urljoin(
                                    "https://www.whoscored.com/",
                                    str(
                                        stage.get("source_url")
                                        or (
                                            f"/Regions/{region_id}/Tournaments/{tournament_id}"
                                            f"/Seasons/{source_season_id}/Stages/{stage_id}"
                                        )
                                    ),
                                ),
                                source_ids={"stage_id": str(stage_id)},
                            )
                            _, _, months = fetch_parsed(
                                calendar_target,
                                lambda response: parse_calendar_months(response.text),
                                cache_ttl=mutable_ttl,
                            )
                            ordered_months = tuple(sorted(set(months)))
                            calendar_months_observed = calendar_months_observed or bool(
                                ordered_months
                            )
                            month_records: dict[
                                tuple[int, int], list[dict[str, Any]]
                            ] = {}
                            # The latest non-empty month proves the end of the
                            # fixture interval; the earliest proves its start.
                            # This distinguishes a current long qualification
                            # from a not-yet-started future edition without
                            # downloading every intervening month.
                            for month_search in (
                                reversed(ordered_months),
                                iter(ordered_months),
                            ):
                                for month in month_search:
                                    month_key = (month.year, month.month)
                                    if month_key not in month_records:
                                        month_target = schedule_month_target(
                                            stage_id, month.year, month.month
                                        )
                                        _, _, schedule_ds = fetch_parsed(
                                            month_target,
                                            lambda response, current_scope=scope, current_stage=stage_id, current_stage_row=stage: (
                                                parse_schedule_json(
                                                    response.content,
                                                    scope=current_scope,
                                                    stage_id=current_stage,
                                                    stage=current_stage_row.get(
                                                        "stage_name"
                                                    )
                                                    or current_stage_row.get("stage"),
                                                )
                                            ),
                                            content_type="application/json",
                                            cache_ttl=mutable_ttl,
                                        )
                                        schedule_records = [
                                            dict(row) for row in schedule_ds.rows
                                        ]
                                        month_records[month_key] = schedule_records
                                        classification_schedule_rows.extend(
                                            schedule_records
                                        )
                                        for record in schedule_records:
                                            value = record.get("date")
                                            try:
                                                parsed_date = (
                                                    value.date()
                                                    if isinstance(value, datetime)
                                                    else date.fromisoformat(
                                                        str(value)[:10]
                                                    )
                                                )
                                            except (TypeError, ValueError):
                                                continue
                                            activity_dates.append(parsed_date)
                                    if month_records[month_key]:
                                        break
                    except WhoScoredTransportError as exc:
                        if (
                            exc.kind is not FailureKind.HTTP_STATUS
                            or exc.status_code not in {403, 404}
                        ):
                            raise
                        activity_dates = []
                        schedule_source_unavailable = True

                    if activity_dates:
                        for (
                            pending_key,
                            pending_season,
                        ) in pending_fixture_evidence.items():
                            pending_season.update(
                                {
                                    "is_active": False,
                                    "eligibility": (
                                        TournamentEligibility.SOURCE_UNAVAILABLE.value
                                    ),
                                    "classification_reason": (
                                        "newer_season_has_no_fixture_date_evidence"
                                    ),
                                }
                            )
                            activity_expected.discard(pending_key)
                        pending_fixture_evidence.clear()
                        activity_evidence.add(key)
                        first_fixture = min(activity_dates)
                        last_fixture = max(activity_dates)
                        season["start"] = first_fixture.isoformat()
                        season["end"] = last_fixture.isoformat()
                        season["is_active"] = (
                            first_fixture - timedelta(days=45)
                            <= as_of_date
                            <= last_fixture + timedelta(days=45)
                        )
                        if season["is_active"]:
                            activity_search_complete = True
                            if not requires_stage_backfill:
                                break
                        elif last_fixture < as_of_date - timedelta(days=45):
                            # Source order is newest-first. Once the first
                            # accessible edition has ended, every following
                            # edition is historical and cannot be active.
                            activity_search_complete = True
                            if not requires_stage_backfill:
                                break
                    elif schedule_source_unavailable or not calendar_months_observed:
                        season.update(
                            {
                                "is_active": False,
                                "eligibility": (
                                    TournamentEligibility.SOURCE_UNAVAILABLE.value
                                ),
                                "classification_reason": (
                                    "season_has_no_accessible_fixture_date_evidence"
                                    if schedule_source_unavailable
                                    else "season_stage_calendars_empty"
                                ),
                            }
                        )
                        activity_expected.discard(key)
                    else:
                        # Keep the candidate unresolved until an older source
                        # season proves that this is merely a not-yet-started
                        # edition. If no fallback supplies fixture dates, the
                        # final completeness pass quarantines the catalog.
                        pending_fixture_evidence[key] = season

            if previous_catalog is not None:
                # Normal discovery stops after the active edition. Preserve
                # the last published evidence for untouched historical rows;
                # otherwise a previously explicit source_unavailable season
                # reverts to parent=included without stage evidence.
                for season in season_rows:
                    if (
                        not season.get("competition_id")
                        or season.get("source_season_id") is None
                    ):
                        continue
                    key = (
                        str(season["competition_id"]),
                        int(season["source_season_id"]),
                    )
                    previous = previous_season_rows.get(key)
                    if previous is None or key in visited_season_keys:
                        continue
                    for field in (
                        "start",
                        "end",
                        "eligibility",
                        "classification_reason",
                    ):
                        season[field] = previous.get(field)
                    if (
                        previous.get("eligibility")
                        == TournamentEligibility.SOURCE_UNAVAILABLE.value
                    ):
                        season["is_active"] = False
                    else:
                        try:
                            previous_start = (
                                date.fromisoformat(str(previous["start"])[:10])
                                if previous.get("start")
                                else None
                            )
                            previous_end = (
                                date.fromisoformat(str(previous["end"])[:10])
                                if previous.get("end")
                                else None
                            )
                        except ValueError:
                            previous_start = previous_end = None
                        season["is_active"] = bool(
                            (previous_start is not None or previous_end is not None)
                            and (
                                previous_start is None
                                or previous_start - timedelta(days=45) <= as_of_date
                            )
                            and (
                                previous_end is None
                                or as_of_date <= previous_end + timedelta(days=45)
                            )
                        )
                current_season_keys = {
                    (str(row.get("competition_id")), int(row["source_season_id"]))
                    for row in season_rows
                    if row.get("competition_id")
                    and row.get("source_season_id") is not None
                }
                missing_seasons = sorted(previous_season_keys - current_season_keys)
                if missing_seasons:
                    raise WhoScoredParseError(
                        "tournament season menus lost previously published seasons: "
                        + ", ".join(
                            f"{competition_id}/{source_season_id}"
                            for competition_id, source_season_id in missing_seasons[:20]
                        )
                    )

            if previous_catalog is not None and not full_history:
                existing_stage_rows = previous_catalog.to_rows().get("stages", ())
                stage_keys = {
                    (
                        row.get("competition_id"),
                        row.get("source_season_id"),
                        row.get("stage_id"),
                    )
                    for row in stage_rows
                }
                for row in existing_stage_rows:
                    key = (
                        row.get("competition_id"),
                        row.get("source_season_id"),
                        row.get("stage_id"),
                    )
                    if key not in stage_keys:
                        stage_rows.append(dict(row))
                        stage_keys.add(key)

            if previous_catalog is not None:
                previous_stage_keys = {
                    (
                        str(row.get("competition_id") or ""),
                        int(row["source_season_id"]),
                        int(row["stage_id"]),
                    )
                    for row in previous_catalog.to_rows().get("stages", ())
                    if row.get("competition_id")
                    and row.get("source_season_id") is not None
                    and row.get("stage_id") is not None
                }
                current_stage_keys = {
                    (
                        str(row.get("competition_id") or ""),
                        int(row["source_season_id"]),
                        int(row["stage_id"]),
                    )
                    for row in stage_rows
                    if row.get("competition_id")
                    and row.get("source_season_id") is not None
                    and row.get("stage_id") is not None
                }
                missing_stages = sorted(previous_stage_keys - current_stage_keys)
                if missing_stages:
                    raise WhoScoredParseError(
                        "season stage menus lost previously published stages: "
                        + ", ".join(
                            f"{competition_id}/{source_season_id}/{stage_id}"
                            for competition_id, source_season_id, stage_id in missing_stages[
                                :20
                            ]
                        )
                    )

            resolved_rows = apply_schedule_classification(
                {
                    "competitions": competition_rows,
                    "seasons": season_rows,
                    "stages": stage_rows,
                },
                classification_schedule_rows,
            )
            missing_activity_evidence = activity_expected - activity_evidence
            missing_stage_evidence = (
                historical_stage_expected - historical_stage_evidence
            )
            if missing_activity_evidence or missing_stage_evidence:
                resolved_seasons: list[dict[str, Any]] = []
                for source in resolved_rows["seasons"]:
                    row = dict(source)
                    key = (
                        str(row.get("competition_id") or ""),
                        int(row["source_season_id"])
                        if row.get("source_season_id") is not None
                        else -1,
                    )
                    if key in missing_activity_evidence:
                        row.update(
                            {
                                "is_active": None,
                                "eligibility": "quarantined",
                                "classification_reason": (
                                    "active_candidate_has_no_fixture_date_evidence"
                                ),
                            }
                        )
                    elif key in missing_stage_evidence:
                        row.update(
                            {
                                "eligibility": "quarantined",
                                "classification_reason": (
                                    "historical_season_has_no_stage_evidence"
                                ),
                            }
                        )
                    resolved_seasons.append(row)
                resolved_rows = {
                    **resolved_rows,
                    "seasons": tuple(resolved_seasons),
                }
                exceptional_seasons = {
                    (
                        str(row.get("competition_id") or ""),
                        int(row["source_season_id"]),
                    ): (
                        str(row.get("eligibility")),
                        str(row.get("classification_reason") or "season_unclassified"),
                    )
                    for row in resolved_seasons
                    if row.get("source_season_id") is not None
                    and row.get("eligibility")
                    in {
                        TournamentEligibility.QUARANTINED.value,
                        TournamentEligibility.SOURCE_UNAVAILABLE.value,
                    }
                }
                resolved_stages: list[dict[str, Any]] = []
                for source in resolved_rows["stages"]:
                    row = dict(source)
                    key = (
                        str(row.get("competition_id") or ""),
                        int(row["source_season_id"])
                        if row.get("source_season_id") is not None
                        else -1,
                    )
                    disposition = exceptional_seasons.get(key)
                    if disposition is not None:
                        row["eligibility"], reason = disposition
                        row["classification_reason"] = f"season:{reason}"
                    resolved_stages.append(row)
                resolved_rows = {
                    **resolved_rows,
                    "stages": tuple(resolved_stages),
                }
            resolved_rows = canonical_catalog_rows(resolved_rows)
            discovered = WhoScoredCatalog.from_rows(resolved_rows)
            eligible_without_stages = sorted(
                season.scope.spec
                for season in discovered.enabled_scopes()
                if not season.stage_ids
            )
            catalog_payload_sha256_value = catalog_payload_sha256(resolved_rows)
            technical_audit = build_technical_exclusion_audit(
                resolved_rows,
                source_snapshot_sha256=_root_response.sha256,
            )
            technical_audit_payload = json.dumps(
                technical_audit,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            technical_audit_sha256 = hashlib.sha256(technical_audit_payload).hexdigest()
            # A discovery batch represents both the parsed catalog and the
            # immutable raw observations that produced it.  Hashing only the
            # parser output makes a source refresh whose semantics are
            # unchanged reuse the old batch id while presenting different
            # ``raw_inputs_json`` to the repository.  The repository then
            # correctly rejects that ambiguous provenance as a conflict.
            # Normalize the list once and use the exact same order for both
            # identity and persistence.
            raw_inputs = list(canonical_catalog_raw_inputs(raw_inputs))
            raw_inputs_payload = json.dumps(
                raw_inputs,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            raw_inputs_sha256 = catalog_raw_provenance_sha256(raw_inputs)
            discovery_mode = "full_history" if full_history else "incremental"
            batch_identity = (
                f"{PARSER_VERSION}\0{discovery_mode}\0"
                f"{catalog_payload_sha256_value}\0{raw_inputs_sha256}\0"
                f"{technical_audit_sha256}\0{as_of_iso}\0"
                f"{parent_catalog_batch_id or ''}\0"
                f"{parent_catalog_payload_sha256 or ''}\0"
                f"{parent_catalog_raw_provenance_sha256 or ''}"
            )
            discovery_batch_id = (
                "wsc2-" + hashlib.sha256(batch_identity.encode("utf-8")).hexdigest()
            )
            technical_audit_descriptor: Optional[dict[str, Any]] = None
            result.metadata.update(
                {
                    "catalog_batch_id": discovery_batch_id,
                    "catalog_payload_sha256": catalog_payload_sha256_value,
                    "catalog_raw_provenance_sha256": raw_inputs_sha256,
                    "catalog_as_of_date": as_of_iso,
                    "catalog_source_snapshot_sha256": _root_response.sha256,
                    "technical_exclusion_audit_sha256": technical_audit_sha256,
                    "technical_exclusion_audit": technical_audit,
                    "parent_catalog_batch_id": parent_catalog_batch_id,
                    "parent_catalog_payload_sha256": (parent_catalog_payload_sha256),
                    "parent_catalog_raw_provenance_sha256": (
                        parent_catalog_raw_provenance_sha256
                    ),
                }
            )
            if hasattr(store, "store_bytes") and hasattr(store, "object_uri"):
                technical_audit_target = RawTarget(
                    source="whoscored",
                    page_kind="catalog_technical_audit",
                    target_id=(
                        f"whoscored:catalog-technical-audit:{discovery_batch_id}"
                    ),
                    canonical_url=(
                        "https://www.whoscored.com/catalog/technical-audit/"
                        f"{discovery_batch_id}"
                    ),
                    source_ids={"discovery_batch_id": discovery_batch_id},
                )
                technical_audit_record = store.store_bytes(
                    technical_audit_target,
                    technical_audit_payload,
                    content_type="application/json",
                    charset="utf-8",
                    fetcher_version=PARSER_VERSION,
                )
                technical_audit_uri = store.object_uri(technical_audit_record.blob_key)
                technical_audit_descriptor = {
                    "target_id": technical_audit_target.target_id,
                    "raw_uri": technical_audit_uri,
                    "payload_sha256": technical_audit_sha256,
                    "audit_type": technical_audit["audit_type"],
                    "source_snapshot_sha256": _root_response.sha256,
                    "candidate_count": technical_audit["candidate_count"],
                    "unresolved_candidate_count": technical_audit[
                        "unresolved_candidate_count"
                    ],
                    "as_of_date": as_of_iso,
                    "encoding": "application/json+gzip",
                }
                result.metadata["technical_exclusion_audit_uri"] = technical_audit_uri
            quarantined_by_kind: dict[str, int] = {}
            quarantined_by_reason: dict[str, int] = {}
            quarantined_competition_ids: set[str] = set()
            for quarantine in discovered.quarantined:
                kind = str(quarantine.get("record_type") or "unknown")
                reason = str(
                    quarantine.get("classification_reason") or "reason_missing"
                )
                competition_id = str(quarantine.get("competition_id") or "")
                if competition_id:
                    quarantined_competition_ids.add(competition_id)
                quarantined_by_kind[kind] = quarantined_by_kind.get(kind, 0) + 1
                quarantined_by_reason[reason] = quarantined_by_reason.get(reason, 0) + 1
            result.counts.update(
                {
                    "competitions": len(competition_rows),
                    "eligible_competitions": sum(
                        item.whoscored_enabled for item in discovered.competitions
                    ),
                    "excluded_women": sum(
                        str(row.get("eligibility")) == "excluded_women"
                        for row in resolved_rows["competitions"]
                    ),
                    "excluded_youth": sum(
                        str(row.get("eligibility")) == "excluded_youth"
                        for row in resolved_rows["competitions"]
                    ),
                    "excluded_reserve": sum(
                        str(row.get("eligibility")) == "excluded_reserve"
                        for row in resolved_rows["competitions"]
                    ),
                    "excluded_technical": sum(
                        str(row.get("eligibility")) == "excluded_technical"
                        for row in resolved_rows["competitions"]
                    ),
                    "technical_duplicate_candidates": int(
                        technical_audit["candidate_count"]
                    ),
                    "unresolved_technical_duplicate_candidates": int(
                        technical_audit["unresolved_candidate_count"]
                    ),
                    "seasons": len(season_rows),
                    "source_unavailable_seasons": sum(
                        str(row.get("eligibility")) == "source_unavailable"
                        for row in resolved_rows["seasons"]
                    ),
                    "stages": len(stage_rows),
                    "eligible_scopes": len(discovered.eligible_scopes()),
                    "active_scopes": sum(
                        item.eligibility is TournamentEligibility.INCLUDED
                        and item.is_active is True
                        for item in discovered.all_scopes()
                    ),
                    "quarantined": len(discovered.quarantined),
                    "quarantined_competitions": quarantined_by_kind.get(
                        "competition", 0
                    ),
                    "quarantined_seasons": quarantined_by_kind.get("season", 0),
                    "quarantined_stages": quarantined_by_kind.get("stage", 0),
                    "quarantined_unique_competitions": len(quarantined_competition_ids),
                    "quarantine_reasons": len(quarantined_by_reason),
                    "eligible_seasons_without_stages": len(eligible_without_stages),
                    "full_history": int(full_history),
                }
            )
            result.tables.extend(
                [
                    "iceberg.bronze.whoscored_competitions",
                    "iceberg.bronze.whoscored_seasons",
                    "iceberg.bronze.whoscored_stages",
                    "iceberg.bronze.whoscored_catalog_manifest",
                ]
            )
            if (
                discovered.quarantined
                or eligible_without_stages
                or technical_audit["unresolved_candidate_count"]
            ):
                result.succeeded = 0
                problems: list[str] = []
                if discovered.quarantined:
                    reason_summary = ", ".join(
                        f"{reason}={count}"
                        for reason, count in sorted(quarantined_by_reason.items())
                    )
                    samples = []
                    for quarantine in discovered.quarantined:
                        if len(samples) >= 10:
                            break
                        identity = "/".join(
                            str(value)
                            for value in (
                                quarantine.get("record_type"),
                                quarantine.get("competition_id"),
                                quarantine.get("source_season_id"),
                                quarantine.get("stage_id"),
                            )
                            if value not in (None, "")
                        )
                        if identity and identity not in samples:
                            samples.append(identity)
                    problems.append(
                        f"{len(discovered.quarantined)} quarantined records "
                        f"across {len(quarantined_competition_ids)} competitions "
                        f"({reason_summary}; samples: {', '.join(samples)})"
                    )
                if eligible_without_stages:
                    problems.append(
                        f"{len(eligible_without_stages)} eligible seasons without stages "
                        f"({', '.join(eligible_without_stages[:10])})"
                    )
                if technical_audit["unresolved_candidate_count"]:
                    unresolved_samples = [
                        f"{item['candidate_type']}:{item['candidate_key']}"
                        for item in technical_audit["candidates"]
                        if item["review_disposition"]
                        == "requires_versioned_source_id_review"
                    ]
                    problems.append(
                        f"{technical_audit['unresolved_candidate_count']} "
                        "unreviewed technical duplicate candidates "
                        f"({', '.join(unresolved_samples[:10])})"
                    )
                result.errors.append("catalog completeness: " + "; ".join(problems))
            else:
                persisted_raw_inputs: Sequence[Mapping[str, Any]] = raw_inputs
                persisted_raw_uri = root_raw_uri
                # A first full-history catalog has more provenance than can
                # fit in one Trino query literal (10k+ raw inputs can exceed
                # query.max-length). Persist the complete canonical list as
                # one content-addressed raw object and put only its compact,
                # hash-bound descriptor in the commit manifest. Tests may use
                # a minimal raw-store seam; production always has store_bytes.
                if hasattr(store, "store_bytes") and hasattr(store, "object_uri"):
                    provenance_target = RawTarget(
                        source="whoscored",
                        page_kind="catalog_provenance",
                        target_id=f"whoscored:catalog-provenance:{discovery_batch_id}",
                        canonical_url=(
                            "https://www.whoscored.com/catalog/provenance/"
                            f"{discovery_batch_id}"
                        ),
                        source_ids={"discovery_batch_id": discovery_batch_id},
                    )
                    provenance_record = store.store_bytes(
                        provenance_target,
                        raw_inputs_payload.encode("utf-8"),
                        content_type="application/json",
                        charset="utf-8",
                        fetcher_version=PARSER_VERSION,
                    )
                    persisted_raw_uri = store.object_uri(provenance_record.blob_key)
                    descriptors = [
                        {
                            "target_id": provenance_target.target_id,
                            "raw_uri": persisted_raw_uri,
                            "payload_sha256": raw_inputs_sha256,
                            "input_count": len(raw_inputs),
                            "as_of_date": as_of_iso,
                            "encoding": "application/json+gzip",
                        },
                    ]
                    if technical_audit_descriptor is not None:
                        descriptors.append(technical_audit_descriptor)
                    persisted_raw_inputs = tuple(
                        sorted(
                            descriptors,
                            key=lambda item: str(item.get("target_id") or ""),
                        )
                    )
                repo.persist_discovered_catalog(
                    discovered,
                    discovery_batch_id=discovery_batch_id,
                    raw_uri=persisted_raw_uri,
                    payload_sha256=catalog_payload_sha256_value,
                    raw_inputs=persisted_raw_inputs,
                    raw_provenance_sha256=raw_inputs_sha256,
                    discovery_mode=discovery_mode,
                    as_of_date=as_of_date,
                    parent_catalog_batch_id=parent_catalog_batch_id,
                    parent_catalog_payload_sha256=(parent_catalog_payload_sha256),
                    parent_catalog_raw_provenance_sha256=(
                        parent_catalog_raw_provenance_sha256
                    ),
                )
                result.succeeded = len(competition_rows)
        except Exception as exc:
            result.errors.append(f"catalog: {type(exc).__name__}: {exc}")
        finally:
            try:
                result.traffic = dict(network.get_traffic_stats())
            except (AttributeError, TypeError):
                result.traffic = {}
            if transport is None:
                network.close()
        return result

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
            raise RuntimeError(
                f"{self.scope.spec}: WhoScored source ids are not configured"
            )
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

        active = self._scope_is_active()
        response, _ = self._fetch(
            target,
            validator=validate,
            allow_cache=True,
            cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL if active else None,
        )
        return find_source_season_id(response.text, self.scope)

    def _scope_is_active(self) -> bool:
        """Use catalog evidence; never treat an undated historical scope as current."""
        if self.catalog_season.is_active is not None:
            return bool(self.catalog_season.is_active)
        today = date.today()
        if self.catalog_season.start is None and self.catalog_season.end is None:
            return False
        return (
            self.catalog_season.start is None or self.catalog_season.start <= today
        ) and (self.catalog_season.end is None or self.catalog_season.end >= today)

    def _bound_paid_fallback(self, eligible_urls: int) -> None:
        """Tighten paid URL cardinality for the current entity batch.

        The byte ceilings remain task-wide; this additionally prevents a
        large roster/preview candidate list from turning a transient
        Cloudflare incident into one paid lease per candidate.
        """
        campaign = getattr(
            getattr(self.transport, "context", None), "proxy_campaign", {}
        )
        if isinstance(campaign, Mapping) and campaign.get("proxy_allocation"):
            # A verified campaign already fixes the exact durable work-item
            # request/lease/byte ceilings. Local cardinality heuristics must
            # not silently shrink that signed allocation; proxy-filter carries
            # its remaining balance across retries and DagRun continuations.
            return
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
        result = EntityResult(
            "schedule",
            self.scope.spec,
            attempted=1,
            committed_batches={"scope": []},
        )
        self._bound_paid_fallback(2)
        scope_spools: list[WhoScoredScopeRowSpool] = []
        try:
            # Validate the memory ceiling before any source request. A typo in
            # production configuration must not consume network/proxy bytes
            # and then fail only after every stage has been parsed.
            scope_write_chunk_rows_from_env()
            source_season_id = self._source_season_id()
            today = date.today()
            active = self._scope_is_active()
            if (
                self.competition.region_id is None
                or self.competition.tournament_id is None
            ):
                raise RuntimeError(f"{self.scope.spec}: missing region/tournament ids")
            season_target = stage_page_target(
                self.scope,
                region_id=self.competition.region_id,
                tournament_id=self.competition.tournament_id,
                source_season_id=source_season_id,
            )

            season_response, season_raw_uri, season_page = self._fetch_parsed(
                season_target,
                parser=lambda response: parse_season_page(
                    response.text,
                    scope=self.scope,
                    region_id=int(self.competition.region_id),
                    tournament_id=int(self.competition.tournament_id),
                    source_season_id=source_season_id,
                ),
                cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL if active else None,
            )
            raw_uris = [season_raw_uri]
            payload_hashes = [season_response.sha256]
            stage_rows = [dict(row) for row in season_page.stages.rows]
            source_stage_ids = sorted({int(row["stage_id"]) for row in stage_rows})
            result.metadata.update(
                {
                    "source_stage_ids": source_stage_ids,
                    "source_stage_count": len(source_stage_ids),
                }
            )
            schedule_by_id: dict[int, dict[str, Any]] = {}
            incident_by_key: dict[str, dict[str, Any]] = {}
            bet_by_key: dict[str, dict[str, Any]] = {}
            season_dataset_rows: dict[str, list[Mapping[str, Any]]] = {
                "standings": [],
                "forms": [],
                "streaks": [],
                "performance": [],
            }
            season_dataset_statuses: dict[str, list[DatasetStatus]] = {
                name: [] for name in season_dataset_rows
            }
            for stage in stage_rows:
                stage_id = int(stage["stage_id"])
                # Discovery and scope ingestion share one raw target.  Bind it
                # to the same source-provided stage URL in both paths; using a
                # constructed ``/Stages/{id}`` URL here while discovery used
                # ``/Stages/{id}/Fixtures/{slug}`` made each workflow
                # quarantine the other's otherwise valid manifest as a target
                # mismatch and refetch the page direct.
                calendar_url = urljoin(
                    "https://www.whoscored.com/",
                    str(
                        stage.get("source_url")
                        or (
                            f"/Regions/{self.competition.region_id}"
                            f"/Tournaments/{self.competition.tournament_id}"
                            f"/Seasons/{source_season_id}/Stages/{stage_id}"
                        )
                    ),
                )
                calendar_target = self._html_target(
                    page_kind="stage_calendar",
                    target_id=f"whoscored:calendar:{stage_id}",
                    url=calendar_url,
                    source_ids={"stage_id": str(stage_id)},
                )

                calendar_response, calendar_raw_uri, calendar_bundle = (
                    self._fetch_parsed(
                        calendar_target,
                        parser=lambda response: (
                            parse_calendar_months(response.text),
                            parse_season_tables(
                                response.text,
                                scope=self.scope,
                                source_season_id=source_season_id,
                            ),
                        ),
                        cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL if active else None,
                    )
                )
                months, stage_tables = calendar_bundle
                for name, dataset in stage_tables.items():
                    season_dataset_rows[name].extend(dataset.rows)
                    season_dataset_statuses[name].append(dataset.status)
                raw_uris.append(calendar_raw_uri)
                payload_hashes.append(calendar_response.sha256)
                for month in months:
                    month_target = schedule_month_target(
                        stage_id, month.year, month.month
                    )
                    if month.month == 12:
                        next_month = date(month.year + 1, 1, 1)
                    else:
                        next_month = date(month.year, month.month + 1, 1)
                    # A month is immutable after a grace period for postponed
                    # games and corrections. Mutable months use the same short
                    # TTL, so retries are offline but changes arrive same-day.
                    closed_month = next_month + timedelta(days=7) <= today

                    month_response, month_raw_uri, parsed = self._fetch_parsed(
                        month_target,
                        parser=lambda response, current_stage=stage_id, stage_row=stage: (
                            parse_schedule_json(
                                response.content,
                                scope=self.scope,
                                stage_id=current_stage,
                                stage=stage_row.get("stage_name")
                                or stage_row.get("stage"),
                            )
                        ),
                        content_type="application/json",
                        cache_ttl=(
                            ACTIVE_SCHEDULE_CACHE_TTL
                            if active and not closed_month
                            else None
                        ),
                    )
                    raw_uris.append(month_raw_uri)
                    payload_hashes.append(month_response.sha256)
                    for incident in parse_schedule_incidents(parsed).rows:
                        entity_key = str(incident["entity_key"])
                        candidate_incident = dict(incident)
                        previous_incident = incident_by_key.get(entity_key)
                        if (
                            previous_incident is not None
                            and previous_incident != candidate_incident
                        ):
                            raise WhoScoredParseError(
                                f"incident {entity_key} appears with conflicting data"
                            )
                        incident_by_key[entity_key] = candidate_incident
                    for bet in parse_schedule_bets(parsed).rows:
                        entity_key = str(bet["entity_key"])
                        candidate_bet = dict(bet)
                        previous_bet = bet_by_key.get(entity_key)
                        if previous_bet is not None and previous_bet != candidate_bet:
                            raise WhoScoredParseError(
                                f"bet offer {entity_key} appears with conflicting data"
                            )
                        bet_by_key[entity_key] = candidate_bet
                    for row in parsed.rows:
                        game_id = int(row["game_id"])
                        candidate = dict(row)
                        previous = schedule_by_id.get(game_id)
                        if previous is not None and previous != candidate:
                            raise WhoScoredParseError(
                                f"game {game_id} appears with conflicting stage data"
                            )
                        schedule_by_id[game_id] = candidate

            if not schedule_by_id:
                raise WhoScoredParseError("season schedule contains no matches")

            datasets: dict[str, Sequence[Mapping[str, Any]]] = {
                "whoscored_schedule": list(schedule_by_id.values()),
                "whoscored_match_incidents": list(incident_by_key.values()),
                "whoscored_match_bets": list(bet_by_key.values()),
            }
            distinct_keys: dict[str, str] = {
                "whoscored_schedule": "game_id",
                "whoscored_match_incidents": "entity_key",
                "whoscored_match_bets": "entity_key",
            }
            source_empty: set[str] = set()
            source_unavailable: set[str] = set()
            if not incident_by_key:
                source_empty.add("whoscored_match_incidents")
            if not bet_by_key:
                source_empty.add("whoscored_match_bets")
            season_mapping = {
                "standings": "whoscored_stage_standings",
                "forms": "whoscored_stage_forms",
                "streaks": "whoscored_stage_streaks",
                "performance": "whoscored_stage_performance",
            }
            for name, table in season_mapping.items():
                rows = season_dataset_rows[name]
                statuses = season_dataset_statuses[name]
                datasets[table] = self._entity_keyed(rows)
                distinct_keys[table] = "entity_key"
                if rows:
                    continue
                if DatasetStatus.EMPTY in statuses:
                    source_empty.add(table)
                else:
                    source_unavailable.add(table)

            # Team/player feeds expose one JSON table per UI tab. Team paging
            # must mirror the browser's empty defaults; the player endpoint
            # accepts one bounded page above any plausible stage population.
            stage_stat_rows: dict[str, WhoScoredScopeRowSpool] = {}
            for table in (
                "whoscored_team_stage_stats",
                "whoscored_player_stage_stats",
                "whoscored_referee_stage_stats",
            ):
                spool = WhoScoredScopeRowSpool(
                    table=table,
                    league=self.scope.competition_id,
                    season=self.scope.season_id,
                )
                stage_stat_rows[table] = spool
                # Register each resource immediately so a later constructor
                # failure still closes every already-open SQLite file.
                scope_spools.append(spool)
            feed_states: dict[str, str] = {}
            for stage in stage_rows:
                stage_id = int(stage["stage_id"])
                stage_kickoffs = [
                    row.get("date")
                    for row in schedule_by_id.values()
                    if int(row.get("stage_id") or -1) == stage_id
                    and isinstance(row.get("date"), datetime)
                ]
                # A completed stage is immutable even when its parent season
                # is still active (cups often expose many finished groups).
                # This prevents every daily run from refetching dozens of
                # Detailed feeds for stages whose last match is long past.
                stage_mutable = active and (
                    not stage_kickoffs
                    or max(stage_kickoffs).date() + timedelta(days=7) > today
                )
                stage_stats_cache_ttl = timedelta(hours=30) if stage_mutable else None
                # Direct HTTP is attempted first.  If WhoScored returns its
                # source-specific verification shell, the transport opens this
                # ordinary stage page once and executes only allow-listed JSON
                # GETs in the same direct FlareSolverr browser context.
                statistics_bootstrap_url = (
                    f"https://www.whoscored.com/Regions/{self.competition.region_id}"
                    f"/Tournaments/{self.competition.tournament_id}"
                    f"/Seasons/{source_season_id}/Stages/{stage_id}/TeamStatistics"
                )
                structured_specs: list[_ParsedFetchSpec] = []
                # feed-state key, destination table, fail on NOT_AVAILABLE,
                # human-readable family for explicit drift errors.
                structured_contracts: list[tuple[str, str, bool, str]] = []

                for (
                    category,
                    subcategory,
                    sort_by,
                    sort_ascending,
                    inc_pens,
                    against,
                ) in TEAM_STAGE_STAT_TABS:
                    structured_specs.append(
                        self._team_stage_statistics_spec(
                            stage_id=stage_id,
                            source_season_id=source_season_id,
                            active=active,
                            category=category,
                            subcategory=subcategory,
                            sort_by=sort_by,
                            sort_ascending=sort_ascending,
                            inc_pens=inc_pens,
                            against=against,
                            cache_ttl=stage_stats_cache_ttl,
                            browser_bootstrap_url=statistics_bootstrap_url,
                        )
                    )
                    structured_contracts.append(
                        (
                            f"{stage_id}:team:{category}:{subcategory}",
                            "whoscored_team_stage_stats",
                            True,
                            f"team {category}/{subcategory}",
                        )
                    )

                for detailed_spec in TEAM_DETAILED_STAT_TABS:
                    structured_specs.append(
                        self._team_stage_statistics_spec(
                            stage_id=stage_id,
                            source_season_id=source_season_id,
                            active=active,
                            category=detailed_spec.category,
                            subcategory=detailed_spec.subcategory,
                            sort_by=detailed_spec.sort_by,
                            detailed=True,
                            cache_ttl=stage_stats_cache_ttl,
                            browser_bootstrap_url=statistics_bootstrap_url,
                        )
                    )
                    structured_contracts.append(
                        (
                            f"{stage_id}:team-detailed:"
                            f"{detailed_spec.category}:{detailed_spec.subcategory}",
                            "whoscored_team_stage_stats",
                            True,
                            "team Detailed "
                            f"{detailed_spec.category}/{detailed_spec.subcategory}",
                        )
                    )

                for category, subcategory, inc_pens in PLAYER_STAGE_STAT_TABS:
                    structured_specs.append(
                        self._player_stage_statistics_spec(
                            stage_id=stage_id,
                            source_season_id=source_season_id,
                            active=active,
                            category=category,
                            subcategory=subcategory,
                            inc_pens=inc_pens,
                            cache_ttl=stage_stats_cache_ttl,
                            browser_bootstrap_url=statistics_bootstrap_url,
                        )
                    )
                    structured_contracts.append(
                        (
                            f"{stage_id}:player:{category}:{subcategory}",
                            "whoscored_player_stage_stats",
                            True,
                            f"player {category}/{subcategory}",
                        )
                    )

                for detailed_spec in PLAYER_DETAILED_STAT_TABS:
                    structured_specs.append(
                        self._player_stage_statistics_spec(
                            stage_id=stage_id,
                            source_season_id=source_season_id,
                            active=active,
                            category=detailed_spec.category,
                            subcategory=detailed_spec.subcategory,
                            sort_by=detailed_spec.sort_by,
                            detailed=True,
                            cache_ttl=stage_stats_cache_ttl,
                            browser_bootstrap_url=statistics_bootstrap_url,
                        )
                    )
                    structured_contracts.append(
                        (
                            f"{stage_id}:player-detailed:"
                            f"{detailed_spec.category}:{detailed_spec.subcategory}",
                            "whoscored_player_stage_stats",
                            True,
                            "player Detailed "
                            f"{detailed_spec.category}/{detailed_spec.subcategory}",
                        )
                    )

                for feed_spec in STAGE_TEAM_FEED_CATALOG:
                    feed_url = stage_team_feed_url(stage_id, feed_spec.type_id)
                    feed_target = self._html_target(
                        page_kind="team_stage_feed",
                        target_id=(
                            f"whoscored:team-stage-feed:{stage_id}:{feed_spec.type_id}"
                        ),
                        url=feed_url,
                        source_ids={
                            "stage_id": str(stage_id),
                            "feed_type": str(feed_spec.type_id),
                        },
                    )
                    structured_specs.append(
                        _ParsedFetchSpec(
                            target=feed_target,
                            parser=lambda response, current_stage=stage_id, current_type=feed_spec.type_id: (
                                parse_stage_team_feed(
                                    response.content,
                                    scope=self.scope,
                                    stage_id=current_stage,
                                    feed_type=current_type,
                                    source_season_id=source_season_id,
                                )
                            ),
                            content_type="application/json",
                            cache_ttl=stage_stats_cache_ttl,
                            browser_bootstrap_url=statistics_bootstrap_url,
                        )
                    )
                    structured_contracts.append(
                        (
                            f"{stage_id}:stagestatfeed:{feed_spec.type_id}",
                            "whoscored_team_stage_stats",
                            False,
                            f"stage team feed {feed_spec.type_id}",
                        )
                    )

                # WhoScored advertises the next domestic edition before its
                # first kickoff, but the stage statistics page intentionally
                # has no Model-last-Mode token until statistics exist. This is
                # explicit source unavailability, not a browser failure and
                # not a reason to discard the already available schedule.
                if stage_kickoffs and min(stage_kickoffs).date() > today:
                    for feed_key, table, _required, _label in structured_contracts:
                        feed_states[feed_key] = DatasetStatus.NOT_AVAILABLE.value
                        source_unavailable.add(table)
                    feed_states[f"{stage_id}:referee:summary"] = (
                        DatasetStatus.NOT_AVAILABLE.value
                    )
                    source_unavailable.add("whoscored_referee_stage_stats")
                    continue

                for spool in stage_stat_rows.values():
                    spool.begin_stage()
                stage_raw_inputs: list[tuple[str, str]] = []
                stage_feed_states: dict[str, str] = {}
                try:
                    # Parse at most one FlareSolverr XHR batch at a time. Raw
                    # objects are still fetched/resumed under the same stage
                    # bootstrap and nothing becomes visible until the scope
                    # manifest is committed, but expanded rows no longer stay
                    # resident for all 68 feeds and every stage.
                    for offset in range(
                        0, len(structured_specs), STRUCTURED_PARSE_BATCH_SIZE
                    ):
                        batch_specs = structured_specs[
                            offset : offset + STRUCTURED_PARSE_BATCH_SIZE
                        ]
                        batch_contracts = structured_contracts[
                            offset : offset + STRUCTURED_PARSE_BATCH_SIZE
                        ]
                        structured_results = self._fetch_parsed_many(batch_specs)
                        (
                            structured_results,
                            paginated_raw_inputs,
                        ) = self._complete_player_statistics_pages(
                            batch_specs, structured_results
                        )
                        if len(structured_results) != len(batch_contracts):
                            raise WhoScoredParseError(
                                f"stage {stage_id} structured feed batch is incomplete"
                            )
                        for paginated_response, paginated_uri in paginated_raw_inputs:
                            stage_raw_inputs.append(
                                (paginated_uri, paginated_response.sha256)
                            )
                        for contract, (response, uri, parsed) in zip(
                            batch_contracts, structured_results
                        ):
                            feed_key, table, require_available, label = contract
                            stage_feed_states[feed_key] = parsed.status.value
                            stage_raw_inputs.append((uri, response.sha256))
                            if (
                                require_available
                                and parsed.status is DatasetStatus.NOT_AVAILABLE
                            ):
                                raise WhoScoredParseError(
                                    f"{label} statistics structure is unavailable for "
                                    f"stage {stage_id}"
                                )
                            # Explicit source unavailability for positional
                            # feeds remains visible without removing siblings.
                            stage_stat_rows[table].append_entity_rows(parsed.rows)
                        del structured_results, paginated_raw_inputs, parsed

                    referee_url = (
                        "https://www.whoscored.com/Regions/"
                        f"{self.competition.region_id}/Tournaments/"
                        f"{self.competition.tournament_id}/Seasons/"
                        f"{source_season_id}/Stages/{stage_id}/RefereeStatistics"
                    )
                    referee_target = self._html_target(
                        page_kind="referee_stage_statistics",
                        target_id=f"whoscored:referee-stats:{stage_id}",
                        url=referee_url,
                        source_ids={"stage_id": str(stage_id)},
                    )
                    response, uri, parsed = self._fetch_parsed(
                        referee_target,
                        parser=lambda response, current_stage=stage_id: (
                            parse_referee_stage_statistics_html(
                                response.text,
                                scope=self.scope,
                                stage_id=current_stage,
                                source_season_id=source_season_id,
                            )
                        ),
                        cache_ttl=stage_stats_cache_ttl,
                    )
                    referee_key = f"{stage_id}:referee:summary"
                    stage_feed_states[referee_key] = parsed.status.value
                    stage_raw_inputs.append((uri, response.sha256))
                    if parsed.status is DatasetStatus.NOT_AVAILABLE:
                        raise WhoScoredParseError(
                            "referee statistics structure is unavailable for "
                            f"stage {stage_id}"
                        )
                    stage_stat_rows["whoscored_referee_stage_stats"].append_entity_rows(
                        parsed.rows
                    )
                except WhoScoredTransportError as exc:
                    for spool in stage_stat_rows.values():
                        spool.rollback_stage()
                    # The restricted browser endpoint emits this exact error
                    # only when the source-owned stage statistics token is
                    # absent. Unrelated browser/5xx failures still fail closed.
                    if not _is_source_stage_statistics_unavailable(exc):
                        raise
                    for feed_key, table, _required, _label in structured_contracts:
                        feed_states[feed_key] = DatasetStatus.NOT_AVAILABLE.value
                        source_unavailable.add(table)
                    feed_states[f"{stage_id}:referee:summary"] = (
                        DatasetStatus.NOT_AVAILABLE.value
                    )
                    source_unavailable.add("whoscored_referee_stage_stats")
                    continue
                except Exception:
                    for spool in stage_stat_rows.values():
                        spool.rollback_stage()
                    raise
                else:
                    for spool in stage_stat_rows.values():
                        spool.commit_stage()
                    feed_states.update(stage_feed_states)
                    for uri, payload_hash in stage_raw_inputs:
                        raw_uris.append(uri)
                        payload_hashes.append(payload_hash)

            for table, rows in stage_stat_rows.items():
                datasets[table] = rows
                distinct_keys[table] = "entity_key"
                if not rows and table not in source_unavailable:
                    source_empty.add(table)

            combined_hash = hashlib.sha256(
                json.dumps(sorted(payload_hashes), separators=(",", ":")).encode(
                    "utf-8"
                )
            ).hexdigest()
            scope_batch_id = self.repository.commit_scope_bundle(
                league=self.scope.competition_id,
                season=self.scope.season_id,
                entity_group="season",
                datasets=datasets,
                distinct_keys=distinct_keys,
                payload_sha256=combined_hash,
                raw_uris=raw_uris,
                source_empty=source_empty,
                source_unavailable=source_unavailable,
                feed_states=feed_states,
            )
            result.committed_batches["scope"].append(str(scope_batch_id))
            result.succeeded = 1
            for table, rows in datasets.items():
                result.counts[table.removeprefix("whoscored_")] = len(rows)
                result.tables.append(f"iceberg.bronze.{table}")
        except Exception as exc:
            result.errors.append(f"schedule: {type(exc).__name__}: {exc}")
        finally:
            for spool in scope_spools:
                spool.close()
        return result

    def sync_matches(
        self,
        *,
        match_ids: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        force_replay: bool = False,
        historical_replay: bool = False,
        kickoff_from: Optional[datetime] = None,
    ) -> EntityResult:
        result = EntityResult(
            "matches",
            self.scope.spec,
            committed_batches={"match": [], "match_not_available": []},
        )
        self._ensure_schema_once()
        candidates = self.repository.list_match_candidates(
            self.scope.competition_id,
            self.scope.season_id,
            match_ids=match_ids,
            limit=limit,
            include_success=force_replay,
            kickoff_from=kickoff_from,
            include_exact_count=True,
        )
        result.attempted = len(candidates)
        result.attempted_snapshots["match"] = {
            "schema_version": 1,
            "count": result.attempted,
            "payload_sha256": entity_id_payload_sha256(
                int(candidate.game_id) for candidate in candidates
            ),
        }
        # Backlog visibility: how many due matches this bounded run left
        # un-fetched (``limit`` per scope). Pure observability — no gate.
        total_candidates = (
            candidates[0].exact_candidate_count
            if candidates and candidates[0].exact_candidate_count is not None
            else result.attempted
        )
        result.metadata["match_candidates"] = {
            "schema_version": 1,
            "count": int(total_candidates),
            "attempted": result.attempted,
            "remaining": max(0, int(total_candidates) - result.attempted),
        }
        self._bound_paid_fallback(len(candidates))
        pending: list[tuple[MatchCommit, Any]] = []
        for candidate in candidates:
            target = match_page_target(candidate.game_id)
            response: Optional[TransportResponse] = None
            raw_uri: Optional[str] = None
            parsed_holder: dict[str, Any] = {}

            def validate(response: TransportResponse) -> bool:
                parsed_holder["html"] = response.text
                try:
                    parsed_holder["match"] = parse_match_html(
                        response.text,
                        scope=self.scope,
                        game_id=candidate.game_id,
                        game=candidate.game,
                    )
                except MatchCentreDataAbsent:
                    parsed_holder["matchcentre_absent"] = True
                    raise
                return True

            try:
                response, raw_uri = self._fetch(
                    target,
                    validator=validate,
                    allow_cache=True,
                    # Final historical pages are immutable source evidence.
                    # Reparse persisted raw bytes regardless of age and use
                    # the source only when that object is absent or invalid.
                    cache_ttl=(
                        None
                        if historical_replay
                        else timedelta(days=MATCH_REFRESH_DAYS)
                    ),
                )
                parsed = parsed_holder["match"]
                additional = {
                    name: dataset.rows
                    for name, dataset in parsed.datasets.items()
                    if name not in {"events", "lineups"}
                }
                commit = MatchCommit(
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
                    attempt_no=int(candidate.attempt_no),
                    datasets=additional,
                    dataset_statuses={
                        name: dataset.status.value
                        for name, dataset in parsed.datasets.items()
                    },
                    is_opta=candidate.match_is_opta,
                    schedule_status=candidate.status,
                )
                try:
                    self.repository.validate_match_commit(commit)
                except ValueError as exc:
                    raise WhoScoredParseError(str(exc)) from exc
                pending.append((commit, parsed))
            except WhoScoredTransportError as exc:
                source_match_unavailable = (
                    exc.kind is FailureKind.CONTENT
                    and parsed_holder.get("matchcentre_absent") is True
                    and is_valid_match_page_without_matchcentre(
                        str(parsed_holder.get("html") or ""),
                        game_id=candidate.game_id,
                    )
                )
                if source_match_unavailable:
                    state = "not_available"
                elif exc.kind is FailureKind.CONTENT:
                    state = "parse_failed"
                else:
                    # Task-scoped safety ceilings and cache/proxy
                    # infrastructure failures are not permanent facts about
                    # a match.  In particular TransportBudgetExceeded is
                    # deliberately non-retryable inside one task, but the
                    # candidate must be eligible again in a later DagRun.
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
                attempt_no = int(candidate.attempt_no)
                retry_after = None
                if state == "retryable":
                    delay_hours = min(6 * (2 ** min(attempt_no - 1, 3)), 48)
                    retry_after = datetime.now(timezone.utc).replace(
                        tzinfo=None
                    ) + timedelta(hours=delay_hours)
                try:
                    failure_batch_id = self.repository.record_failure(
                        ManifestFailure(
                            game_id=candidate.game_id,
                            league=self.scope.competition_id,
                            season=self.scope.season_id,
                            state=state,
                            failure_code=(
                                "source_not_available"
                                if state == "not_available"
                                else exc.kind.value
                            ),
                            error=str(exc)[:4000],
                            retry_after=retry_after,
                            attempt_no=attempt_no,
                            game=candidate.game,
                            kickoff=candidate.kickoff,
                            is_opta=candidate.match_is_opta,
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
                except Exception as manifest_exc:
                    result.errors.append(
                        f"game {candidate.game_id} failure manifest: "
                        f"{type(manifest_exc).__name__}: {manifest_exc}"
                    )
                else:
                    if state == "not_available":
                        expected_failure_batch_id = (
                            deterministic_match_not_available_batch_id(
                                candidate.game_id,
                                league=self.scope.competition_id,
                                season=self.scope.season_id,
                                failure_code="source_not_available",
                                http_status=exc.status_code,
                                payload_sha256=getattr(exc, "payload_sha256", None),
                                raw_uri=getattr(exc, "raw_uri", None),
                            )
                        )
                        suffix = (
                            failure_batch_id.removeprefix(
                                MATCH_NOT_AVAILABLE_BATCH_ID_PREFIX
                            )
                            if isinstance(failure_batch_id, str)
                            else ""
                        )
                        if (
                            not isinstance(failure_batch_id, str)
                            or not failure_batch_id.startswith(
                                MATCH_NOT_AVAILABLE_BATCH_ID_PREFIX
                            )
                            or not _is_lower_sha256(suffix)
                            or failure_batch_id != expected_failure_batch_id
                        ):
                            result.errors.append(
                                f"game {candidate.game_id}: repository returned no "
                                "exact not-available outcome identity"
                            )
                        else:
                            result.committed_batches["match_not_available"].append(
                                failure_batch_id
                            )
                            result.succeeded += 1
                            result.counts["not_available"] = (
                                result.counts.get("not_available", 0) + 1
                            )
                    elif state == "parse_failed":
                        result.errors.append(f"game {candidate.game_id}: {exc}")
                    else:
                        target_list = (
                            result.retryable
                            if state == "retryable"
                            else result.terminal
                        )
                        target_list.append(str(candidate.game_id))
            except WhoScoredParseError as exc:
                try:
                    self.repository.record_failure(
                        ManifestFailure(
                            game_id=candidate.game_id,
                            league=self.scope.competition_id,
                            season=self.scope.season_id,
                            state="parse_failed",
                            failure_code=FailureKind.CONTENT.value,
                            error=str(exc)[:4000],
                            retry_after=None,
                            attempt_no=int(candidate.attempt_no),
                            game=candidate.game,
                            kickoff=candidate.kickoff,
                            is_opta=candidate.match_is_opta,
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
                except Exception as manifest_exc:
                    result.errors.append(
                        f"game {candidate.game_id} failure manifest: "
                        f"{type(manifest_exc).__name__}: {manifest_exc}"
                    )
                result.errors.append(f"game {candidate.game_id}: {exc}")
            except Exception as exc:
                result.errors.append(
                    f"game {candidate.game_id}: {type(exc).__name__}: {exc}"
                )
        if pending:
            try:
                expected_batch_ids = tuple(
                    commit.batch_id for commit, _parsed in pending
                )
                committed_batch_ids = tuple(
                    self.repository.commit_matches(
                        [commit for commit, _parsed in pending]
                    )
                )
                if committed_batch_ids != expected_batch_ids:
                    raise RuntimeError(
                        "match repository returned different committed batch ids"
                    )
            except Exception as exc:
                result.errors.append(f"match batch: {type(exc).__name__}: {exc}")
            else:
                result.committed_batches["match"].extend(committed_batch_ids)
                result.succeeded += len(pending)
                for _commit, parsed in pending:
                    for name, dataset in parsed.datasets.items():
                        result.counts[name] = (
                            result.counts.get(name, 0) + dataset.row_count
                        )
        if candidates or result.succeeded:
            result.tables.extend(
                [
                    "iceberg.bronze.whoscored_matches",
                    "iceberg.bronze.whoscored_events",
                    "iceberg.bronze.whoscored_lineups",
                    "iceberg.bronze.whoscored_substitutions",
                    "iceberg.bronze.whoscored_formations",
                    "iceberg.bronze.whoscored_team_match_stats",
                    "iceberg.bronze.whoscored_player_match_stats",
                    "iceberg.bronze.whoscored_match_ingest_manifest",
                ]
            )
        return result

    def sync_previews(
        self,
        *,
        limit: Optional[int] = None,
        match_ids: Optional[Iterable[int]] = None,
        force_replay: bool = False,
    ) -> EntityResult:
        result = EntityResult(
            "previews",
            self.scope.spec,
            committed_batches={"preview": [], "preview_not_available": []},
        )
        self._ensure_schema_once()
        candidates = self.repository.list_preview_candidates(
            self.scope.competition_id,
            self.scope.season_id,
            limit=limit,
            match_ids=match_ids,
            force_replay=force_replay,
        )
        result.attempted = len(candidates)
        result.attempted_snapshots["preview"] = {
            "schema_version": 1,
            "count": result.attempted,
            "payload_sha256": entity_id_payload_sha256(
                int(candidate["game_id"]) for candidate in candidates
            ),
        }
        self._bound_paid_fallback(len(candidates))
        pending: list[tuple[PreviewCommit, Any]] = []
        for candidate in candidates:
            target = preview_page_target(candidate["game_id"])
            parsed_holder: dict[str, Any] = {}

            def validate(response: TransportResponse) -> bool:
                parsed_holder["preview"] = parse_preview_bundle(
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
                if exc.status_code in {404, 410}:
                    state = "not_available"
                elif exc.kind is FailureKind.CONTENT:
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
                        if (exc.retryable or exc.kind in retryable_kinds)
                        else "terminal"
                    )
                attempt_no = int(candidate["attempt_no"])
                retry_after = None
                if state == "retryable":
                    delay_hours = min(6 * (2 ** min(attempt_no - 1, 3)), 48)
                    retry_after = datetime.now(timezone.utc).replace(
                        tzinfo=None
                    ) + timedelta(hours=delay_hours)
                try:
                    failure_batch_id = self.repository.record_preview_failure(
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
                            transport_mode=(exc.route.value if exc.route else "none"),
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
                except Exception as manifest_exc:
                    result.errors.append(
                        f"preview {candidate['game_id']} failure manifest: "
                        f"{type(manifest_exc).__name__}: {manifest_exc}"
                    )
                else:
                    if state == "retryable":
                        result.retryable.append(str(candidate["game_id"]))
                    elif state == "not_available":
                        expected_failure_batch_id = (
                            deterministic_preview_not_available_batch_id(
                                int(candidate["game_id"]),
                                league=self.scope.competition_id,
                                season=self.scope.season_id,
                                failure_code=exc.kind.value,
                                http_status=exc.status_code,
                                payload_sha256=getattr(exc, "payload_sha256", None),
                                raw_uri=getattr(exc, "raw_uri", None),
                            )
                        )
                        suffix = (
                            failure_batch_id.removeprefix(
                                PREVIEW_NOT_AVAILABLE_BATCH_ID_PREFIX
                            )
                            if isinstance(failure_batch_id, str)
                            else ""
                        )
                        if (
                            not isinstance(failure_batch_id, str)
                            or not failure_batch_id.startswith(
                                PREVIEW_NOT_AVAILABLE_BATCH_ID_PREFIX
                            )
                            or not _is_lower_sha256(suffix)
                            or failure_batch_id != expected_failure_batch_id
                        ):
                            result.errors.append(
                                f"preview {candidate['game_id']}: repository returned "
                                "no exact not-available outcome identity"
                            )
                        else:
                            result.committed_batches["preview_not_available"].append(
                                failure_batch_id
                            )
                            result.succeeded += 1
                            result.counts["not_available"] = (
                                result.counts.get("not_available", 0) + 1
                            )
                    elif state == "terminal":
                        result.terminal.append(str(candidate["game_id"]))
                    else:
                        # Parser/content drift must fail visibly. Transport
                        # has already persisted the raw payload and never
                        # escalates validator failures to paid proxy.
                        result.errors.append(f"preview {candidate['game_id']}: {exc}")
                continue

            try:
                parsed = parsed_holder["preview"]
                paid_route = response.route.value.startswith("paid_")
                commit = PreviewCommit(
                    game_id=int(candidate["game_id"]),
                    league=self.scope.competition_id,
                    season=self.scope.season_id,
                    game=str(candidate["game"]),
                    kickoff=candidate["date"],
                    payload_sha256=response.sha256,
                    raw_uri=raw_uri,
                    missing_players=parsed.missing_players.rows,
                    transport_mode=response.route.value,
                    proxy_mode="filtered_paid" if paid_route else "none",
                    http_status=response.status_code,
                    direct_bytes=0 if paid_route else response.wire_bytes,
                    paid_bytes=response.wire_bytes if paid_route else 0,
                    attempt_no=int(candidate["attempt_no"]),
                    datasets={
                        "preview_lineups": parsed.preview_lineups.rows,
                        "preview_sections": parsed.preview_sections.rows,
                    },
                    dataset_statuses={
                        name: dataset.status.value
                        for name, dataset in parsed.datasets.items()
                    },
                    parser_version=parsed.parser_version,
                )
                try:
                    self.repository.validate_preview_commit(commit)
                except ValueError as exc:
                    raise WhoScoredParseError(str(exc)) from exc
                pending.append((commit, parsed))
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
        if pending:
            try:
                expected_batch_ids = tuple(
                    commit.batch_id for commit, _parsed in pending
                )
                committed_batch_ids = tuple(
                    self.repository.commit_previews(
                        [commit for commit, _parsed in pending]
                    )
                )
                if committed_batch_ids != expected_batch_ids:
                    raise RuntimeError(
                        "preview repository returned different committed batch ids"
                    )
            except Exception as exc:
                result.errors.append(f"preview batch: {type(exc).__name__}: {exc}")
            else:
                result.committed_batches["preview"].extend(committed_batch_ids)
                result.succeeded += len(pending)
                for _commit, parsed in pending:
                    for name, dataset in parsed.datasets.items():
                        result.counts[name] = (
                            result.counts.get(name, 0) + dataset.row_count
                        )
        if candidates:
            result.tables.extend(
                [
                    "iceberg.bronze.whoscored_missing_players",
                    "iceberg.bronze.whoscored_preview_lineups",
                    "iceberg.bronze.whoscored_preview_sections",
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
            competition = self.catalog.competition(configured.scope.competition_id)
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
        limit: int = 500,
        candidate_scopes: Optional[
            Iterable[CatalogSeason | WhoScoredScope | str]
        ] = None,
        player_ids: Optional[Iterable[int]] = None,
    ) -> EntityResult:
        self._ensure_schema_once()
        scopes = self._profile_candidate_scopes(candidate_scopes)
        result = EntityResult(
            "profiles",
            ",".join(scope.spec for scope in scopes),
            committed_batches={"profile": [], "profile_not_available": []},
        )
        if player_ids is None:
            selected_player_ids = self.repository.list_profile_candidates(
                scopes=scopes,
                limit=limit,
            )
        else:
            raw_player_ids = list(player_ids)
            if any(
                type(value) is not int or value <= 0 for value in raw_player_ids
            ) or raw_player_ids != sorted(set(raw_player_ids)):
                raise ValueError(
                    "explicit profile player_ids must be sorted unique IDs"
                )
            selected_player_ids = list(raw_player_ids)
            if len(selected_player_ids) > int(limit):
                raise ValueError("explicit profile player_ids exceed the task limit")
        result.attempted = len(selected_player_ids)
        result.attempted_snapshots["profile"] = {
            "schema_version": 1,
            "count": result.attempted,
            "payload_sha256": entity_id_payload_sha256(selected_player_ids),
        }
        self._bound_paid_fallback(len(selected_player_ids))
        pending_commits: list[ProfileCommit] = []
        for player_id in selected_player_ids:
            target = profile_page_target(player_id)
            parsed_holder: dict[str, Any] = {}

            def validate(response: TransportResponse) -> bool:
                parsed = parse_profile_bundle(response.text, player_id=player_id)
                parsed_holder["profile"] = parsed
                if parsed.participations.status is DatasetStatus.NOT_AVAILABLE:
                    raise WhoScoredParseError(
                        "profile participation/playerStatistics structure is absent"
                    )
                row = parsed.profiles.rows[0]
                useful = sum(
                    row.get(key) is not None
                    for key in (
                        "date_of_birth",
                        "height_cm",
                        "nationality",
                        "current_team_id",
                        "shirt_number",
                        "age",
                        "positions",
                    )
                )
                # WhoScored legitimately omits biographical fields for some
                # reserve/new players while still publishing their team,
                # position and tournament participation.  Those are valid
                # profiles, not parser drift.  Requiring a name plus one
                # independent descriptor keeps challenge/error shells out
                # without rejecting sparse source records.
                if not row.get("name") or useful < 1:
                    raise WhoScoredParseError("profile lacks required identity fields")
                return True

            try:
                response, raw_uri = self._fetch(
                    target,
                    validator=validate,
                    allow_cache=True,
                    cache_ttl=timedelta(days=PROFILE_REFRESH_DAYS),
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
                if exc.status_code in {404, 410}:
                    state = "not_available"
                elif exc.kind is FailureKind.CONTENT:
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
                    failure_batch_id = self.repository.record_profile_failure(
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
                    elif state == "not_available":
                        expected_failure_batch_id = (
                            deterministic_profile_not_available_batch_id(
                                player_id,
                                failure_code=exc.kind.value,
                                http_status=exc.status_code,
                                payload_sha256=getattr(exc, "payload_sha256", None),
                                raw_uri=getattr(exc, "raw_uri", None),
                            )
                        )
                        suffix = (
                            failure_batch_id.removeprefix(
                                PROFILE_NOT_AVAILABLE_BATCH_ID_PREFIX
                            )
                            if isinstance(failure_batch_id, str)
                            else ""
                        )
                        if (
                            not isinstance(failure_batch_id, str)
                            or not failure_batch_id.startswith(
                                PROFILE_NOT_AVAILABLE_BATCH_ID_PREFIX
                            )
                            or not _is_lower_sha256(suffix)
                            or failure_batch_id != expected_failure_batch_id
                        ):
                            result.errors.append(
                                f"profile {player_id}: repository returned no exact "
                                "not-available outcome identity"
                            )
                        else:
                            result.committed_batches["profile_not_available"].append(
                                failure_batch_id
                            )
                            result.succeeded += 1
                            result.counts["not_available"] = (
                                result.counts.get("not_available", 0) + 1
                            )
                    else:
                        target_list = (
                            result.retryable
                            if state == "retryable"
                            else result.terminal
                        )
                        target_list.append(str(player_id))
                continue

            try:
                parsed = parsed_holder["profile"]
                profile = dict(parsed.profiles.rows[0])
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

            paid_route = response.route.value.startswith("paid_")
            pending_commits.append(
                ProfileCommit(
                    player_id=player_id,
                    profile=profile,
                    payload_sha256=response.sha256,
                    raw_uri=raw_uri,
                    transport_mode=response.route.value,
                    proxy_mode="filtered_paid" if paid_route else "none",
                    direct_bytes=0 if paid_route else response.wire_bytes,
                    paid_bytes=response.wire_bytes if paid_route else 0,
                    parser_version=parsed.parser_version,
                    participations=parsed.participations.rows,
                    participations_status=parsed.participations.status.value,
                )
            )
        if pending_commits:
            try:
                expected_batch_ids = tuple(
                    commit.batch_id for commit in pending_commits
                )
                committed_batch_ids = tuple(
                    self.repository.commit_profiles(pending_commits)
                )
                if committed_batch_ids != expected_batch_ids:
                    raise RuntimeError(
                        "profile repository returned different committed batch ids"
                    )
            except Exception as exc:
                result.errors.append(f"profile batch: {type(exc).__name__}: {exc}")
            else:
                result.committed_batches["profile"].extend(committed_batch_ids)
                result.succeeded += len(pending_commits)
                result.counts["player_profile_versions"] = len(pending_commits)
                result.counts["player_stage_participations"] = sum(
                    len(commit.participations) for commit in pending_commits
                )
        if selected_player_ids:
            result.tables.extend(
                [
                    "iceberg.bronze.whoscored_player_profile_versions",
                    "iceberg.bronze.whoscored_player_stage_participations",
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
