"""Application service for source-native FotMob ingestion.

The service coordinates four deliberately separate layers:

``transport -> raw object -> pure parser -> manifest-backed repository``.

It never derives a season from a calendar year, never uses a competition name
as identity, and never labels seasonless team/player/transfer snapshots as
historical.  Network work is bounded by an explicit ledger and all fan-out is
deduplicated before requests are scheduled.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence

from .catalog import (
    CatalogDiscovery,
    CatalogShapeError,
    SelectedSeasonMismatch,
    classify_competition,
    discover_competitions,
    parse_seasons,
)
from .domain import (
    CompetitionRef,
    ScopeClassification,
    ScopeDecision,
    ScopeRef,
    SeasonBundle,
    SeasonRef,
)
from .field_map import INTENTIONAL_EXCLUSIONS, classify_paths
from .parsers import (
    inventory_json_paths,
    parse_leaderboards,
    parse_season_bundle,
    parse_transfers,
)
from .planner import (
    BudgetExceeded,
    BudgetLedger,
    RunMode,
    TransportBudget,
    tombstones_after_two_absences,
    utc_run_id,
)
from .repository import (
    PARSER_VERSION,
    ManifestStatus,
    TableRows,
    TargetCommit,
    utc_now,
)
from .transport import FetchOutcome, FetchResult, FotMobTransport, canonicalize_target


MATCH_CONTENT_SECTIONS = (
    "matchFacts",
    "stats",
    "playerStats",
    "lineup",
    "shotmap",
    "h2h",
    "momentum",
)

_BLOCKING_PARSE_ISSUES = frozenset(
    {
        "invalid_match",
        "match_without_id",
        "playoff_match_without_id",
        "invalid_standing",
        "invalid_playoff_matchup",
    }
)


def _aware_datetime(value: Optional[str | datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _content_hash(values: Iterable[str]) -> str:
    material = "\0".join(sorted(str(value) for value in values)).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _completion_target_key(
    target_type: str,
    competition_id: int,
    source_season_key: Optional[str],
    plan_signature: str,
) -> str:
    """Hash a logical completion identity without normalizing source season."""

    material = _json(
        {
            "competition_id": int(competition_id),
            "plan_signature": str(plan_signature),
            "source_season_key": source_season_key,
            "target_type": str(target_type),
        }
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _event_year(value: Any, fallback: int) -> int:
    match = re.search(r"(?:19|20)\d{2}", str(value or ""))
    return int(match.group(0)) if match else fallback


def _fetch_manifest_status(fetch: FetchResult) -> ManifestStatus:
    if fetch.outcome in {FetchOutcome.SUCCESS, FetchOutcome.STALE_REPLAY}:
        return ManifestStatus.SUCCESS
    if fetch.outcome == FetchOutcome.NOT_MODIFIED:
        return ManifestStatus.NOT_MODIFIED
    if fetch.outcome == FetchOutcome.NOT_AVAILABLE:
        return ManifestStatus.NOT_AVAILABLE
    if fetch.outcome == FetchOutcome.RETRYABLE_FAILURE:
        return ManifestStatus.RETRYABLE_FAILURE
    return ManifestStatus.TERMINAL_FAILURE


@dataclass
class OperationResult:
    entity: str
    attempted: int = 0
    succeeded: int = 0
    skipped: int = 0
    not_available: int = 0
    review_required: int = 0
    tables: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    retryable: list[str] = field(default_factory=list)
    terminal: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.errors and not self.retryable and not self.terminal

    @property
    def status(self) -> str:
        if self.errors or self.terminal:
            return "failed"
        if self.retryable:
            return "retryable"
        if self.review_required:
            return "review_required"
        return "success"

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "status": self.status,
            "attempted": self.attempted,
            "succeeded": self.succeeded,
            "skipped": self.skipped,
            "not_available": self.not_available,
            "review_required": self.review_required,
            "tables": list(dict.fromkeys(self.tables)),
            "counts": dict(self.counts),
            "errors": list(self.errors),
            "retryable": list(self.retryable),
            "terminal": list(self.terminal),
            "metadata": dict(self.metadata),
        }


@dataclass
class CatalogResult:
    operation: OperationResult
    discovery: Optional[CatalogDiscovery] = None
    classifications: tuple[ScopeClassification, ...] = ()
    fetch: Optional[FetchResult] = None


@dataclass
class CompetitionDiscoveryResult:
    competition: CompetitionRef
    classification: ScopeClassification
    operation: OperationResult
    seasons: tuple[SeasonRef, ...] = ()
    selected_bundle: Optional[SeasonBundle] = None
    fetch: Optional[FetchResult] = None


@dataclass
class RunReport:
    run_id: str
    mode: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    operations: list[OperationResult] = field(default_factory=list)
    budget: dict[str, int] = field(default_factory=dict)
    transport: dict[str, Any] = field(default_factory=dict)
    exclusions: tuple[dict[str, str], ...] = INTENTIONAL_EXCLUSIONS

    @property
    def ok(self) -> bool:
        return bool(self.operations) and all(item.ok for item in self.operations)

    @property
    def status(self) -> str:
        return "success" if self.ok else "incomplete"

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "mode": self.mode,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "operations": [item.as_dict() for item in self.operations],
            "budget": dict(self.budget),
            "transport": dict(self.transport),
            "intentional_exclusions": list(self.exclusions),
        }


class FotMobIngestService:
    """Coordinate bounded discovery, parsing and canonical persistence."""

    def __init__(
        self,
        *,
        transport: FotMobTransport,
        repository: Any,
        mode: RunMode = RunMode.DAILY,
        budget: Optional[TransportBudget] = None,
        run_id: Optional[str] = None,
        max_workers: int = 4,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least one")
        self.transport = transport
        self.repository = repository
        self.mode = RunMode(mode)
        self.run_id = run_id or utc_run_id()
        self.ledger = BudgetLedger(budget or TransportBudget())
        self.max_workers = min(int(max_workers), 16)
        self._budget_lock = threading.Lock()
        self._next_build_id: Optional[str] = None
        self.repository.ensure_schema()

    def _account(self, fetch: FetchResult) -> None:
        with self._budget_lock:
            self.ledger.account_fetch(
                attempts=fetch.attempts,
                direct_bytes=fetch.direct_bytes,
                proxy_bytes=fetch.proxy_bytes,
            )

    def _fetch(
        self,
        endpoint: str,
        params: Optional[Mapping[str, Any]] = None,
        *,
        replay: bool = False,
    ) -> FetchResult:
        if replay or self.mode == RunMode.REPLAY:
            fetch = self.transport.replay_json(endpoint, params)
            self._account(fetch)
            return fetch
        with self._budget_lock:
            max_attempts = max(1, int(getattr(self.transport, "max_attempts", 1)))
            if self.ledger.remaining_requests < max_attempts:
                raise BudgetExceeded(
                    "remaining request budget cannot cover one retry-bounded "
                    f"target ({self.ledger.remaining_requests}<{max_attempts})"
                )
        fetch = self.transport.fetch_json(endpoint, params)
        self._account(fetch)
        return fetch

    def _fetch_many(
        self,
        requests: Sequence[tuple[str, str, Optional[Mapping[str, Any]]]],
        *,
        replay: bool = False,
    ) -> dict[str, FetchResult | Exception]:
        """Fetch unique targets concurrently while preserving caller keys."""

        unique: dict[str, tuple[str, Optional[Mapping[str, Any]]]] = {}
        for key, endpoint, params in requests:
            unique.setdefault(key, (endpoint, params))
        if not unique:
            return {}
        # A single transport call may consume max_attempts.  Do not launch a
        # concurrent wave that could cross the hard request ceiling.
        max_attempts = max(1, int(getattr(self.transport, "max_attempts", 1)))
        output: dict[str, FetchResult | Exception] = {}
        if not replay and self.mode != RunMode.REPLAY:
            safe_count = self.ledger.remaining_requests // max_attempts
            if safe_count <= 0:
                raise BudgetExceeded(
                    "remaining request budget cannot cover one retry-bounded target"
                )
            items = list(unique.items())
            unique = dict(items[:safe_count])
            for key, _ in items[safe_count:]:
                output[key] = BudgetExceeded(
                    "target was not scheduled because the request budget "
                    "cannot cover its bounded retries"
                )
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(unique))) as pool:
            futures = {
                pool.submit(self._fetch, endpoint, params, replay=replay): key
                for key, (endpoint, params) in unique.items()
            }
            for future in as_completed(futures):
                key = futures[future]
                try:
                    output[key] = future.result()
                except Exception as exc:  # result reports exact target failure
                    output[key] = exc
        return output

    def _commit_for_fetch(
        self,
        fetch: FetchResult,
        *,
        target_type: str,
        status: Optional[ManifestStatus] = None,
        competition_id: Optional[int] = None,
        source_season_key: Optional[str] = None,
        entity_id: Optional[str | int] = None,
        datasets: Sequence[TableRows] = (),
        expected_counts: Optional[Mapping[str, int]] = None,
        capabilities: Optional[Mapping[str, Any]] = None,
        exclusions: Sequence[Mapping[str, Any] | str] = (),
        unknown_paths: Sequence[str] = (),
        error_code: Optional[str] = None,
        error: Optional[str] = None,
    ) -> list[str]:
        manifest_status = status or _fetch_manifest_status(fetch)
        commit = TargetCommit(
            run_id=self.run_id,
            target_type=target_type,
            target_key=fetch.target_key,
            status=manifest_status,
            competition_id=(
                str(competition_id) if competition_id is not None else None
            ),
            source_season_key=source_season_key,
            entity_id=(str(entity_id) if entity_id is not None else None),
            content_hash=fetch.content_hash,
            raw_uri=fetch.raw_uri,
            fetch_outcome=fetch.status,
            http_status=fetch.http_status,
            attempts=fetch.attempts,
            retries=fetch.retries,
            cache_hit=fetch.cache_hit,
            stale=fetch.stale,
            fetched_at=_aware_datetime(fetch.fetched_at),
            direct_bytes=fetch.direct_bytes,
            proxy_bytes=fetch.proxy_bytes,
            encoded_bytes=fetch.encoded_bytes,
            decoded_bytes=fetch.decoded_bytes,
            expected_counts=expected_counts or {},
            capabilities=capabilities or {},
            exclusions=exclusions,
            unknown_paths=unknown_paths,
            error_code=error_code,
            error=error or fetch.error,
        )
        # A 304, raw replay, or byte-identical 200 may point at a batch that is
        # already logically committed.  Re-parsing remains useful to validate
        # current code and feed downstream fan-out, but appending the same
        # physical rows again only creates Iceberg files/metadata.  Reuse is
        # safe only after a successful manifest for the exact deterministic
        # batch; raw cache presence alone is insufficient after a crash.
        datasets_to_write = datasets
        if (
            datasets
            and target_type != "all_leagues"
            and manifest_status
            in {
                ManifestStatus.SUCCESS,
                ManifestStatus.NOT_MODIFIED,
            }
        ):
            previous = self.repository.latest_success(fetch.target_key)
            if previous and previous.get("batch_id") == commit.batch_id:
                datasets_to_write = ()
        return self.repository.commit(commit, datasets_to_write)

    @staticmethod
    def _record_failure(result: OperationResult, key: str, fetch: FetchResult) -> None:
        if fetch.outcome == FetchOutcome.NOT_AVAILABLE:
            result.not_available += 1
        elif fetch.outcome == FetchOutcome.RETRYABLE_FAILURE:
            result.retryable.append(key)
        else:
            result.terminal.append(key)

    @staticmethod
    def _inventory_rows(
        target_type: str,
        paths: Iterable[str],
        *,
        competition_id: Optional[int] = None,
        source_season_key: Optional[str] = None,
    ) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        coverage = classify_paths(target_type, paths)
        rows: list[dict[str, Any]] = []
        for disposition, values in (
            ("typed", coverage.typed),
            ("raw_only", coverage.raw_only),
            ("excluded", coverage.excluded),
            ("unknown", coverage.unknown),
        ):
            rows.extend(
                {
                    "target_type": target_type,
                    "competition_id": competition_id,
                    "source_season_key": source_season_key,
                    "json_path": path,
                    "disposition": disposition,
                }
                for path in values
            )
        return rows, coverage.unknown

    def discover_catalog(self) -> CatalogResult:
        result = OperationResult("competition_catalog", attempted=1)
        try:
            fetch = self._fetch("allLeagues")
        except Exception as exc:
            result.errors.append(f"allLeagues: {type(exc).__name__}: {exc}")
            return CatalogResult(result)
        if not fetch.ok:
            self._commit_for_fetch(fetch, target_type="all_leagues")
            self._record_failure(result, fetch.url, fetch)
            return CatalogResult(result, fetch=fetch)
        try:
            discovery = discover_competitions(fetch.data)
            if discovery.issues:
                raise CatalogShapeError(
                    "invalid catalog entries: "
                    + "; ".join(issue.message for issue in discovery.issues)
                )
            conflicts = {item.competition_id: item for item in discovery.conflicts}
            classifications: list[ScopeClassification] = []
            # Each validated discovery is a new catalog snapshot even when a
            # 304 reuses an older immutable body.  Source fetch time remains
            # in the manifest; ordering/tombstone logic uses observation time.
            observed = utc_now()
            rows: list[dict[str, Any]] = []
            current_ids = {item.competition_id for item in discovery.competitions}
            previous_snapshots = self.repository.previous_catalog_snapshots(2)
            tombstoned_ids = (
                tombstones_after_two_absences(
                    previous_snapshots[0], previous_snapshots[1], current_ids
                )
                if len(previous_snapshots) >= 2
                else set()
            )
            for competition in discovery.competitions:
                classification = classify_competition(competition)
                conflict = conflicts.get(competition.competition_id)
                if conflict is not None:
                    classification = ScopeClassification(
                        competition,
                        ScopeDecision.REVIEW_REQUIRED,
                        "conflicting allLeagues metadata: " + ",".join(conflict.fields),
                        "review_catalog_conflict",
                    )
                classifications.append(classification)
                rows.append(
                    {
                        **asdict(competition),
                        "competition_id": str(competition.competition_id),
                        "presentation_slug": competition.presentation_slug,
                        "scope_decision": classification.decision.value,
                        "scope_reason": classification.reason,
                        "scope_policy_rule": classification.policy_rule,
                        "has_catalog_conflict": conflict is not None,
                        "is_tombstoned": False,
                        "tombstone_reason": None,
                        "conflict_fields_json": _json(
                            conflict.fields if conflict else ()
                        ),
                        "discovery_run_id": self.run_id,
                        "discovery_date": observed.date().isoformat(),
                    }
                )
            rows.extend(
                {
                    "competition_id": str(competition_id),
                    "name": None,
                    "presentation_slug": str(competition_id),
                    "scope_decision": ScopeDecision.EXCLUDED.value,
                    "scope_reason": "absent from two consecutive complete catalogs",
                    "scope_policy_rule": "two_snapshot_tombstone",
                    "has_catalog_conflict": False,
                    "is_tombstoned": True,
                    "tombstone_reason": "two_consecutive_complete_absences",
                    "conflict_fields_json": "[]",
                    "discovery_run_id": self.run_id,
                    "discovery_date": observed.date().isoformat(),
                }
                for competition_id in sorted(tombstoned_ids)
            )
            paths = inventory_json_paths(fetch.data)
            inventory, unknown = self._inventory_rows("all_leagues", paths)
            if unknown:
                raise CatalogShapeError(
                    f"unclassified allLeagues JSON paths: {list(unknown)}"
                )
            for row in inventory:
                row["discovery_run_id"] = self.run_id
            datasets = [
                TableRows("fotmob_competitions", rows, "competitions"),
                TableRows(
                    "fotmob_field_inventory",
                    inventory,
                    "field_inventory",
                    partition_cols=("target_type",),
                ),
            ]
            paths_written = self._commit_for_fetch(
                fetch,
                target_type="all_leagues",
                datasets=datasets,
                expected_counts={"competitions": len(rows)},
                capabilities={
                    "unique_competitions": len(discovery.competitions),
                    "catalog_conflicts": len(discovery.conflicts),
                    "catalog_issues": len(discovery.issues),
                    "tombstones": len(tombstoned_ids),
                },
                unknown_paths=unknown,
            )
            result.tables.extend(paths_written)
            result.succeeded = len(discovery.competitions)
            result.review_required = sum(
                item.decision == ScopeDecision.REVIEW_REQUIRED
                for item in classifications
            )
            result.counts.update(
                {
                    "competitions": len(discovery.competitions),
                    "included": sum(
                        item.decision == ScopeDecision.INCLUDED
                        for item in classifications
                    ),
                    "excluded": sum(
                        item.decision == ScopeDecision.EXCLUDED
                        for item in classifications
                    ),
                    "review_required": result.review_required,
                    "conflicts": len(discovery.conflicts),
                    "tombstones": len(tombstoned_ids),
                    "json_paths": len(paths),
                }
            )
            return CatalogResult(
                result,
                discovery,
                tuple(classifications),
                fetch,
            )
        except Exception as exc:
            result.errors.append(f"allLeagues parse: {type(exc).__name__}: {exc}")
            self._commit_for_fetch(
                fetch,
                target_type="all_leagues",
                status=ManifestStatus.SCHEMA_DRIFT,
                error_code=type(exc).__name__,
                error=str(exc),
            )
            return CatalogResult(result, fetch=fetch)

    def discover_competition(
        self,
        classification: ScopeClassification,
        *,
        prefetched: Optional[FetchResult] = None,
    ) -> CompetitionDiscoveryResult:
        competition = classification.competition
        result = OperationResult(
            "competition_seasons",
            attempted=1,
            metadata={"competition_id": competition.competition_id},
        )
        if classification.decision != ScopeDecision.INCLUDED:
            target = canonicalize_target("leagues", {"id": competition.competition_id})
            commit = TargetCommit(
                run_id=self.run_id,
                target_type="competition_seasons",
                target_key=target.target_key,
                competition_id=str(competition.competition_id),
                status=(
                    ManifestStatus.EXCLUDED
                    if classification.decision == ScopeDecision.EXCLUDED
                    else ManifestStatus.REVIEW_REQUIRED
                ),
                exclusions=(
                    {
                        "decision": classification.decision.value,
                        "reason": classification.reason,
                        "rule": classification.policy_rule,
                    },
                ),
            )
            self.repository.record(commit)
            result.skipped = 1
            if classification.decision == ScopeDecision.REVIEW_REQUIRED:
                result.review_required = 1
            return CompetitionDiscoveryResult(competition, classification, result)

        try:
            fetch = prefetched or self._fetch(
                "leagues", {"id": competition.competition_id}
            )
        except Exception as exc:
            result.errors.append(f"competition {competition.competition_id}: {exc}")
            return CompetitionDiscoveryResult(competition, classification, result)
        if not fetch.ok:
            self._commit_for_fetch(
                fetch,
                target_type="competition_seasons",
                competition_id=competition.competition_id,
            )
            self._record_failure(result, fetch.url, fetch)
            return CompetitionDiscoveryResult(
                competition, classification, result, fetch=fetch
            )

        try:
            seasons = parse_seasons(fetch.data, competition)
            selected = next((item for item in seasons if item.is_selected), None)
            bundle = (
                parse_season_bundle(fetch.data, ScopeRef.from_season(selected))
                if selected is not None
                else None
            )
            inventory: list[dict[str, Any]] = []
            unknown: tuple[str, ...] = ()
            if bundle is not None:
                blocking = [
                    issue
                    for issue in bundle.issues
                    if issue.code in _BLOCKING_PARSE_ISSUES
                ]
                inventory, unknown = self._inventory_rows(
                    "league_season",
                    bundle.json_paths,
                    competition_id=competition.competition_id,
                    source_season_key=selected.source_season_key,
                )
                if blocking or unknown:
                    reasons = [
                        f"{issue.code}@{issue.path}: {issue.message}"
                        for issue in blocking
                    ] + [f"unknown_path:{path}" for path in unknown]
                    raise CatalogShapeError("; ".join(reasons))
                for row in inventory:
                    row["discovery_run_id"] = self.run_id
            history_by_key = {
                str(item.get("seasonName")): item
                for item in (fetch.data.get("seasons") or [])
                if isinstance(item, Mapping) and item.get("seasonName") is not None
            }
            stat_links_by_key = {
                str(item.get("Name")): item
                for item in (
                    (fetch.data.get("stats") or {}).get("seasonStatLinks") or []
                )
                if isinstance(item, Mapping) and item.get("Name") is not None
            }
            history_rows = [
                {
                    "competition_id": str(competition.competition_id),
                    "history_season_label": str(item.get("seasonName")),
                    "winner_id": (item.get("winner") or {}).get("id"),
                    "winner_name": (item.get("winner") or {}).get("name"),
                    "runner_up_id": (item.get("loser") or {}).get("id"),
                    "runner_up_name": (item.get("loser") or {}).get("name"),
                    "season_history_json": item,
                    "discovered_at": _aware_datetime(fetch.fetched_at) or utc_now(),
                    "discovery_run_id": self.run_id,
                }
                for item in (fetch.data.get("seasons") or [])
                if isinstance(item, Mapping) and item.get("seasonName") is not None
            ]
            rows = [
                {
                    **asdict(season),
                    "competition_id": str(season.competition_id),
                    "source_slug": fetch.data.get("seostr"),
                    "winner_id": (
                        (
                            history_by_key.get(season.source_season_key, {}).get(
                                "winner"
                            )
                            or {}
                        ).get("id")
                    ),
                    "winner_name": (
                        (
                            history_by_key.get(season.source_season_key, {}).get(
                                "winner"
                            )
                            or {}
                        ).get("name")
                    ),
                    "runner_up_id": (
                        (
                            history_by_key.get(season.source_season_key, {}).get(
                                "loser"
                            )
                            or {}
                        ).get("id")
                    ),
                    "runner_up_name": (
                        (
                            history_by_key.get(season.source_season_key, {}).get(
                                "loser"
                            )
                            or {}
                        ).get("name")
                    ),
                    "season_history_json": history_by_key.get(season.source_season_key),
                    "stat_link_json": stat_links_by_key.get(season.source_season_key),
                    "discovered_at": _aware_datetime(fetch.fetched_at) or utc_now(),
                    "discovery_run_id": self.run_id,
                }
                for season in seasons
            ]
            paths_written = self._commit_for_fetch(
                fetch,
                target_type="competition_seasons",
                competition_id=competition.competition_id,
                datasets=[
                    TableRows(
                        "fotmob_competition_seasons",
                        rows,
                        "competition_seasons",
                    ),
                    TableRows(
                        "fotmob_competition_season_history",
                        history_rows,
                        "competition_season_history",
                        partition_cols=("competition_id",),
                    ),
                    TableRows(
                        "fotmob_field_inventory",
                        inventory,
                        "field_inventory",
                        partition_cols=("target_type",),
                    ),
                ],
                expected_counts={
                    "competition_seasons": len(seasons),
                    "competition_season_history": len(history_rows),
                },
                capabilities={
                    "selected_season": (
                        selected.source_season_key if selected else None
                    ),
                    "json_paths": len(bundle.json_paths) if bundle else 0,
                },
                unknown_paths=unknown,
            )
            result.tables.extend(paths_written)
            result.succeeded = len(seasons)
            result.counts["seasons"] = len(seasons)
            return CompetitionDiscoveryResult(
                competition,
                classification,
                result,
                seasons,
                bundle,
                fetch,
            )
        except Exception as exc:
            result.errors.append(
                f"competition {competition.competition_id} seasons: "
                f"{type(exc).__name__}: {exc}"
            )
            self._commit_for_fetch(
                fetch,
                target_type="competition_seasons",
                status=ManifestStatus.SCHEMA_DRIFT,
                competition_id=competition.competition_id,
                error_code=type(exc).__name__,
                error=str(exc),
            )
            return CompetitionDiscoveryResult(
                competition, classification, result, fetch=fetch
            )

    def discover_competitions(
        self,
        classifications: Sequence[ScopeClassification],
    ) -> list[CompetitionDiscoveryResult]:
        """Fetch included competition roots concurrently, commit sequentially."""

        requests = [
            (
                str(item.competition.competition_id),
                "leagues",
                {"id": item.competition.competition_id},
            )
            for item in classifications
            if item.decision == ScopeDecision.INCLUDED
        ]
        try:
            fetched = self._fetch_many(requests)
        except BudgetExceeded as exc:
            fetched = {
                str(item.competition.competition_id): exc
                for item in classifications
                if item.decision == ScopeDecision.INCLUDED
            }
        output: list[CompetitionDiscoveryResult] = []
        for item in classifications:
            if item.decision != ScopeDecision.INCLUDED:
                output.append(self.discover_competition(item))
                continue
            value = fetched.get(str(item.competition.competition_id))
            if isinstance(value, FetchResult):
                output.append(self.discover_competition(item, prefetched=value))
                continue
            operation = OperationResult(
                "competition_seasons",
                attempted=1,
                errors=[
                    f"competition {item.competition.competition_id}: "
                    f"{type(value).__name__}: {value}"
                ],
                metadata={"competition_id": item.competition.competition_id},
            )
            output.append(CompetitionDiscoveryResult(item.competition, item, operation))
        return output

    def sync_season(
        self,
        competition_id: int,
        source_season_key: str,
        *,
        prefetched: Optional[FetchResult] = None,
    ) -> tuple[OperationResult, Optional[SeasonBundle]]:
        result = OperationResult(
            "season_bundle",
            attempted=1,
            metadata={
                "competition_id": int(competition_id),
                "source_season_key": source_season_key,
            },
        )
        exact = canonicalize_target(
            "leagues",
            {"id": int(competition_id), "season": source_season_key},
        )
        prefetched_source_url: Optional[str] = None
        try:
            if prefetched is None:
                previous = self.repository.latest_success(exact.target_key)
                replay_existing = previous is not None and self.mode == RunMode.BACKFILL
                fetch = self._fetch(
                    exact.canonical_url,
                    replay=replay_existing,
                )
                if replay_existing and not fetch.ok:
                    result.metadata["raw_replay_fallback"] = {
                        "outcome": fetch.status,
                        "error": fetch.error,
                    }
                    fetch = self._fetch(exact.canonical_url)
            else:
                # Discovery fetched the selected season through ``?id=``.  It
                # is byte-equivalent and saves a request, but the logical
                # season commit must still use the exact ``id+season`` target.
                # Otherwise a future selected-season change would make the
                # latest-target view hide the previous season's matches.
                prefetched_source_url = prefetched.url
                fetch = replace(
                    prefetched,
                    target_key=exact.target_key,
                    url=exact.canonical_url,
                    attempts=0,
                    retries=0,
                    cache_hit=True,
                    encoded_bytes=0,
                    direct_bytes=0,
                    proxy_bytes=0,
                )
        except Exception as exc:
            result.errors.append(f"season fetch: {type(exc).__name__}: {exc}")
            return result, None
        if not fetch.ok:
            self._commit_for_fetch(
                fetch,
                target_type="league_season",
                competition_id=competition_id,
                source_season_key=source_season_key,
            )
            self._record_failure(result, fetch.url, fetch)
            return result, None

        try:
            scope = ScopeRef(int(competition_id), str(source_season_key))
            bundle = parse_season_bundle(fetch.data, scope)
            blocking = [
                issue for issue in bundle.issues if issue.code in _BLOCKING_PARSE_ISSUES
            ]
            inventory, unknown = self._inventory_rows(
                "league_season",
                bundle.json_paths,
                competition_id=competition_id,
                source_season_key=source_season_key,
            )
            if blocking or unknown:
                reasons = [
                    f"{issue.code}@{issue.path}: {issue.message}" for issue in blocking
                ] + [f"unknown_path:{path}" for path in unknown]
                raise CatalogShapeError("; ".join(reasons))

            if prefetched_source_url is not None:
                alias_cached = getattr(self.transport, "alias_cached_json", None)
                if alias_cached is not None:
                    try:
                        alias_cached(prefetched_source_url, exact.canonical_url)
                    except Exception as exc:
                        raise CatalogShapeError(
                            "selected-season raw alias failed: "
                            f"{type(exc).__name__}: {exc}"
                        ) from exc

            category_rows = [
                {
                    "competition_id": str(competition_id),
                    "source_season_key": source_season_key,
                    **asdict(descriptor),
                }
                for descriptor in (
                    *bundle.player_categories,
                    *bundle.team_categories,
                )
            ]
            datasets = [
                TableRows("fotmob_season_stages", bundle.stages, "season_stages"),
                TableRows("fotmob_matches", bundle.matches, "matches"),
                TableRows("fotmob_standings", bundle.standings, "standings"),
                TableRows(
                    "fotmob_leaderboard_categories",
                    category_rows,
                    "leaderboard_categories",
                ),
                TableRows(
                    "fotmob_playoff_brackets",
                    bundle.playoffs,
                    "playoff_brackets",
                    partition_cols=("competition_id", "source_season_key"),
                ),
                TableRows(
                    "fotmob_season_teams",
                    bundle.teams,
                    "season_teams",
                    partition_cols=("competition_id", "source_season_key"),
                ),
                TableRows(
                    "fotmob_field_inventory",
                    inventory,
                    "field_inventory",
                    partition_cols=("target_type",),
                ),
            ]
            paths = self._commit_for_fetch(
                fetch,
                target_type="league_season",
                competition_id=competition_id,
                source_season_key=source_season_key,
                datasets=datasets,
                expected_counts={
                    "matches": int(bundle.capabilities.get("match_count", 0)),
                    "standings": int(bundle.capabilities.get("standing_count", 0)),
                    "playoff_brackets": int(
                        bundle.capabilities.get("playoff_matchup_count", 0)
                    ),
                    "season_teams": int(bundle.capabilities.get("team_count", 0)),
                    "leaderboard_categories": (
                        int(bundle.capabilities.get("player_category_count", 0))
                        + int(bundle.capabilities.get("team_category_count", 0))
                    ),
                },
                capabilities=bundle.capabilities,
                unknown_paths=unknown,
            )
            result.tables.extend(paths)
            result.succeeded = 1
            result.counts.update(
                {
                    "matches": len(bundle.matches),
                    "standings": len(bundle.standings),
                    "stages": len(bundle.stages),
                    "playoffs": len(bundle.playoffs),
                    "teams": len(bundle.teams),
                    "player_categories": len(bundle.player_categories),
                    "team_categories": len(bundle.team_categories),
                    "leaderboard_categories": len(category_rows),
                    "json_paths": len(bundle.json_paths),
                    "parse_issues": len(bundle.issues),
                }
            )
            return result, bundle
        except (
            CatalogShapeError,
            SelectedSeasonMismatch,
            TypeError,
            ValueError,
        ) as exc:
            result.errors.append(f"season parse: {type(exc).__name__}: {exc}")
            self._commit_for_fetch(
                fetch,
                target_type="league_season",
                status=ManifestStatus.SCHEMA_DRIFT,
                competition_id=competition_id,
                source_season_key=source_season_key,
                error_code=type(exc).__name__,
                error=str(exc),
            )
            return result, None

    def sync_leaderboards(self, bundle: SeasonBundle) -> OperationResult:
        result = OperationResult(
            "leaderboards",
            metadata={
                "competition_id": bundle.scope.competition_id,
                "source_season_key": bundle.scope.source_season_key,
            },
        )
        descriptors = (*bundle.player_categories, *bundle.team_categories)
        result.attempted = len(descriptors)
        seen_urls: set[tuple[str, str]] = set()
        for index, descriptor in enumerate(descriptors):
            key = (
                descriptor.participant_type,
                descriptor.fetch_all_url or f"missing:{index}",
            )
            if key in seen_urls:
                result.skipped += 1
                continue
            seen_urls.add(key)
            if not descriptor.fetch_all_url:
                target_key = hashlib.sha256(
                    (
                        f"leaderboard:{bundle.scope.identity}:"
                        f"{descriptor.participant_type}:{descriptor.name}:{index}"
                    ).encode("utf-8")
                ).hexdigest()
                self.repository.record(
                    TargetCommit(
                        run_id=self.run_id,
                        target_type="leaderboard",
                        target_key=target_key,
                        competition_id=str(bundle.scope.competition_id),
                        source_season_key=bundle.scope.source_season_key,
                        entity_id=descriptor.name,
                        status=ManifestStatus.NOT_AVAILABLE,
                        error_code="missing_fetch_all_url",
                        error="advertised leaderboard has no fetchAllUrl",
                    )
                )
                result.not_available += 1
                result.metadata["intentional_not_available"] = (
                    int(result.metadata.get("intentional_not_available", 0)) + 1
                )
                continue
            manifest_target = canonicalize_target(descriptor.fetch_all_url)
            if self.mode == RunMode.BACKFILL:
                previous = self.repository.latest_success(manifest_target.target_key)
                if (
                    previous is not None
                    and previous.get("parser_version") == PARSER_VERSION
                ):
                    result.skipped += 1
                    continue
            try:
                fetch = self._fetch(manifest_target.canonical_url)
            except Exception as exc:
                result.errors.append(
                    f"{descriptor.participant_type}/{descriptor.name}: {exc}"
                )
                continue
            if not fetch.ok:
                self._commit_for_fetch(
                    fetch,
                    target_type="leaderboard",
                    competition_id=bundle.scope.competition_id,
                    source_season_key=bundle.scope.source_season_key,
                    entity_id=descriptor.name,
                )
                self._record_failure(result, fetch.url, fetch)
                continue
            try:
                rows = parse_leaderboards(
                    fetch.data,
                    participant_type=descriptor.participant_type,
                    descriptor=descriptor,
                    scope=bundle.scope,
                )
                raw_top_lists = fetch.data.get("TopLists")
                if not isinstance(raw_top_lists, list):
                    raw_top_lists = (fetch.data.get("data") or {}).get("TopLists") or []
                expected_rows = sum(
                    len(top.get("StatList") or top.get("statList") or [])
                    for top in raw_top_lists
                    if isinstance(top, Mapping)
                )
                if len(rows) != expected_rows:
                    raise CatalogShapeError(
                        "leaderboard row completeness mismatch: "
                        f"parsed={len(rows)} expected={expected_rows}"
                    )
                inventory, unknown = self._inventory_rows(
                    "leaderboard",
                    inventory_json_paths(fetch.data),
                    competition_id=bundle.scope.competition_id,
                    source_season_key=bundle.scope.source_season_key,
                )
                if unknown:
                    raise CatalogShapeError(
                        f"unclassified leaderboard JSON paths: {list(unknown)}"
                    )
                paths = self._commit_for_fetch(
                    fetch,
                    target_type="leaderboard",
                    competition_id=bundle.scope.competition_id,
                    source_season_key=bundle.scope.source_season_key,
                    entity_id=descriptor.name,
                    datasets=[
                        TableRows("fotmob_leaderboards", rows, "leaderboards"),
                        TableRows(
                            "fotmob_field_inventory",
                            inventory,
                            "field_inventory",
                            partition_cols=("target_type",),
                        ),
                    ],
                    expected_counts={"leaderboards": expected_rows},
                    capabilities={"top_lists": len(fetch.data.get("TopLists") or [])},
                    unknown_paths=unknown,
                )
                result.tables.extend(paths)
                result.succeeded += 1
                result.counts["rows"] = result.counts.get("rows", 0) + len(rows)
            except Exception as exc:
                result.errors.append(
                    f"{descriptor.participant_type}/{descriptor.name} parse: "
                    f"{type(exc).__name__}: {exc}"
                )
                self._commit_for_fetch(
                    fetch,
                    target_type="leaderboard",
                    status=ManifestStatus.SCHEMA_DRIFT,
                    competition_id=bundle.scope.competition_id,
                    source_season_key=bundle.scope.source_season_key,
                    entity_id=descriptor.name,
                    error_code=type(exc).__name__,
                    error=str(exc),
                )
        return result

    def sync_transfers(
        self,
        competition_id: int,
        *,
        max_pages: int = 250,
        recent_only: bool = False,
    ) -> OperationResult:
        """Fetch or resume one league-filtered global transfer stream.

        Page one is conditionally revalidated on a resumed backfill.  When its
        content hash is unchanged, already committed later pages are replayed
        from raw with zero HTTP before pagination continues at the first
        missing page.  A changed first-page anchor invalidates that shortcut
        and refetches the stream, preventing page shifts from creating gaps.
        """

        stream_window = "1year" if recent_only else "all"
        stream_key = f"{int(competition_id)}|window={stream_window}"
        result = OperationResult(
            "transfer_events",
            metadata={
                "competition_id": int(competition_id),
                "stream_key": stream_key,
                "window": stream_window,
            },
        )
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        unique_ids: set[str] = set()
        expected_hits: Optional[int] = None
        fallback_year = datetime.now(timezone.utc).year
        page_one_changed = False
        resumed_pages = 0
        for page in range(1, max_pages + 1):
            params: dict[str, Any] = {
                "leagueIds": str(int(competition_id)),
                "page": page,
            }
            if recent_only:
                params["last"] = "1year"
            target = canonicalize_target("transfers", params)
            previous = (
                self.repository.latest_success(target.target_key)
                if self.mode == RunMode.BACKFILL
                else None
            )
            replay_page = bool(
                page > 1
                and not page_one_changed
                and previous is not None
                and previous.get("parser_version") == PARSER_VERSION
            )
            result.attempted += 1
            try:
                fetch = self._fetch(
                    target.canonical_url,
                    replay=replay_page,
                )
                if replay_page and not fetch.ok:
                    fetch = self._fetch(target.canonical_url)
                    replay_page = False
            except Exception as exc:
                result.errors.append(f"transfers page {page}: {exc}")
                break
            if replay_page:
                resumed_pages += 1
            if page == 1 and previous is not None:
                page_one_changed = previous.get("content_hash") != fetch.content_hash
            if not fetch.ok:
                self._commit_for_fetch(
                    fetch,
                    target_type="transfers_page",
                    competition_id=competition_id,
                    entity_id=f"{stream_window}:{page}",
                )
                self._record_failure(result, fetch.url, fetch)
                break
            payload = fetch.data
            try:
                if expected_hits is None:
                    raw_hits = payload.get("hits")
                    expected_hits = int(raw_hits) if raw_hits is not None else None
                rows = list(parse_transfers(payload))
                new_rows = [
                    row for row in rows if row["transfer_event_id"] not in unique_ids
                ]
                unique_ids.update(row["transfer_event_id"] for row in new_rows)
                for row in new_rows:
                    row["competition_id"] = str(competition_id)
                    row["event_year"] = _event_year(
                        row.get("transfer_date"), fallback_year
                    )
                inventory, unknown = self._inventory_rows(
                    "transfers",
                    inventory_json_paths(payload),
                    competition_id=competition_id,
                )
                if unknown:
                    raise CatalogShapeError(
                        f"unclassified transfer JSON paths: {list(unknown)}"
                    )
                paths = self._commit_for_fetch(
                    fetch,
                    target_type="transfers_page",
                    competition_id=competition_id,
                    entity_id=f"{stream_window}:{page}",
                    datasets=[
                        TableRows(
                            "fotmob_transfer_events",
                            new_rows,
                            "transfer_events",
                        ),
                        TableRows(
                            "fotmob_field_inventory",
                            inventory,
                            "field_inventory",
                            partition_cols=("target_type",),
                        ),
                    ],
                    expected_counts={"transfer_events": len(new_rows)},
                    capabilities={
                        "stream_key": stream_key,
                        "stream_window": stream_window,
                        "source_hits": expected_hits,
                        "source_page": page,
                        "source_page_rows": len(rows),
                        "unique_seen": len(unique_ids),
                        "page_identity_hash": _content_hash(
                            row["transfer_event_id"] for row in rows
                        ),
                    },
                    unknown_paths=unknown,
                )
                result.tables.extend(paths)
                result.succeeded += 1
                result.counts["events"] = len(unique_ids)
                if expected_hits is not None and len(unique_ids) >= expected_hits:
                    break
                if not rows:
                    break
            except Exception as exc:
                result.errors.append(
                    f"transfers page {page} parse: {type(exc).__name__}: {exc}"
                )
                self._commit_for_fetch(
                    fetch,
                    target_type="transfers_page",
                    status=ManifestStatus.SCHEMA_DRIFT,
                    competition_id=competition_id,
                    entity_id=f"{stream_window}:{page}",
                    error_code=type(exc).__name__,
                    error=str(exc),
                )
                break
        if expected_hits is None:
            result.errors.append(
                "transfer pagination incomplete: source hits is missing"
            )
        elif len(unique_ids) < expected_hits:
            result.errors.append(
                "transfer pagination incomplete: "
                f"unique={len(unique_ids)} source_hits={expected_hits}"
            )
        result.metadata["source_hits"] = expected_hits
        result.metadata["page_one_changed"] = page_one_changed
        result.metadata["resumed_raw_pages"] = resumed_pages
        return result

    @staticmethod
    def _match_payload_row(
        match: Mapping[str, Any], payload: Mapping[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        content = payload.get("content")
        if not isinstance(content, Mapping):
            content = payload
        available = {
            section: section in content and content.get(section) not in (None, {}, [])
            for section in MATCH_CONTENT_SECTIONS
        }
        # Only match facts are universally expected for a finished fixture;
        # every other source capability is explicit in the mask rather than
        # silently interpreted as parser failure.
        expected = {"matchFacts": bool(match.get("finished"))}
        missing = [
            section
            for section, required in expected.items()
            if required and not available.get(section, False)
        ]
        row = {
            "competition_id": str(match["competition_id"]),
            "source_season_key": str(match["source_season_key"]),
            "match_id": match.get("match_id"),
            "page_url": match.get("page_url"),
            "utc_time": match.get("utc_time"),
            "home_team_id": match.get("home_team_id"),
            "away_team_id": match.get("away_team_id"),
            "available_sections_json": available,
            "expected_sections_json": expected,
            "missing_expected_sections_json": missing,
            "match_facts_json": content.get("matchFacts"),
            "stats_json": content.get("stats"),
            "player_stats_json": content.get("playerStats"),
            "lineup_json": content.get("lineup"),
            "shotmap_json": content.get("shotmap"),
            "h2h_json": content.get("h2h"),
            "momentum_json": content.get("momentum"),
            "payload_json": payload,
        }
        return row, {
            "available_sections": available,
            "expected_sections": expected,
            "missing_expected_sections": missing,
        }

    def sync_match_payloads(
        self,
        bundle: SeasonBundle,
        *,
        include_unfinished: bool = False,
        limit: Optional[int] = None,
    ) -> OperationResult:
        """Fetch each needed match with exactly one ``matchDetails`` request."""

        matches = [
            match
            for match in bundle.matches
            if include_unfinished or bool(match.get("finished"))
        ]
        result = OperationResult(
            "match_payloads",
            attempted=len(matches),
            metadata={
                "competition_id": bundle.scope.competition_id,
                "source_season_key": bundle.scope.source_season_key,
            },
        )
        requests: list[tuple[str, str, Mapping[str, Any]]] = []
        by_key: dict[str, Mapping[str, Any]] = {}
        for match in matches:
            match_id = match.get("match_id")
            key = str(match_id)
            manifest_target = canonicalize_target(
                "matchDetails", {"matchId": str(match_id)}
            )
            previous = self.repository.latest_success(manifest_target.target_key)
            if previous is not None and self.mode != RunMode.REPLAY:
                result.skipped += 1
                continue
            by_key[key] = match
            requests.append((key, "matchDetails", {"matchId": str(match_id)}))
        if limit is not None:
            requests = requests[: max(0, int(limit))]
            by_key = {key: by_key[key] for key, _, _ in requests}
        try:
            fetched = self._fetch_many(requests)
        except BudgetExceeded as exc:
            result.errors.append(str(exc))
            return result
        for key, outcome in fetched.items():
            match = by_key[key]
            if isinstance(outcome, Exception):
                result.errors.append(f"match {key}: {outcome}")
                continue
            fetch = outcome
            if not fetch.ok:
                self._commit_for_fetch(
                    fetch,
                    target_type="match",
                    competition_id=bundle.scope.competition_id,
                    source_season_key=bundle.scope.source_season_key,
                    entity_id=key,
                )
                self._record_failure(result, key, fetch)
                continue
            try:
                row, coverage = self._match_payload_row(match, fetch.data)
                paths = inventory_json_paths(fetch.data)
                inventory, unknown = self._inventory_rows(
                    "match",
                    paths,
                    competition_id=bundle.scope.competition_id,
                    source_season_key=bundle.scope.source_season_key,
                )
                missing = coverage["missing_expected_sections"]
                status = (
                    ManifestStatus.SCHEMA_DRIFT
                    if missing or unknown
                    else ManifestStatus.SUCCESS
                )
                datasets = (
                    []
                    if missing or unknown
                    else [
                        TableRows("fotmob_match_payloads", [row], "match_payloads"),
                        TableRows(
                            "fotmob_field_inventory",
                            inventory,
                            "field_inventory",
                            partition_cols=("target_type",),
                        ),
                    ]
                )
                paths_written = self._commit_for_fetch(
                    fetch,
                    target_type="match",
                    status=status,
                    competition_id=bundle.scope.competition_id,
                    source_season_key=bundle.scope.source_season_key,
                    entity_id=key,
                    datasets=datasets,
                    expected_counts={"match_payloads": 1},
                    capabilities=coverage,
                    unknown_paths=unknown,
                    error_code=(
                        "missing_expected_sections"
                        if missing
                        else "unclassified_json_paths"
                        if unknown
                        else None
                    ),
                    error=(
                        f"missing sections: {missing}"
                        if missing
                        else f"unclassified paths: {list(unknown)}"
                        if unknown
                        else None
                    ),
                )
                result.tables.extend(paths_written)
                if missing or unknown:
                    result.errors.append(
                        f"match {key} incomplete: missing={missing}, "
                        f"unknown_paths={list(unknown)}"
                    )
                else:
                    result.succeeded += 1
                    result.counts["rows"] = result.counts.get("rows", 0) + 1
            except Exception as exc:
                result.errors.append(f"match {key} parse: {type(exc).__name__}: {exc}")
                self._commit_for_fetch(
                    fetch,
                    target_type="match",
                    status=ManifestStatus.SCHEMA_DRIFT,
                    competition_id=bundle.scope.competition_id,
                    source_season_key=bundle.scope.source_season_key,
                    entity_id=key,
                    error_code=type(exc).__name__,
                    error=str(exc),
                )
        return result

    @staticmethod
    def _team_rows(
        team_id: int,
        payload: Mapping[str, Any],
        observed_at: datetime,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], set[int]]:
        details = payload.get("details") or {}
        overview = payload.get("overview") or {}
        history = payload.get("history") or {}
        venue = (overview.get("venue") or {}).get("widget") or {}
        snapshot_date = observed_at.date().isoformat()
        team_row = {
            "team_id": str(team_id),
            "team_name": details.get("name") or overview.get("name"),
            "country_code": details.get("country") or details.get("countryCode"),
            "page_url": details.get("pageUrl"),
            "venue_name": venue.get("name"),
            "venue_city": venue.get("city"),
            "venue_capacity": venue.get("capacity"),
            "details_json": details,
            "overview_json": overview,
            "history_json": history,
            "snapshot_date": snapshot_date,
            "observed_at": observed_at,
        }
        squad_root = payload.get("squad") or {}
        sections = squad_root.get("squad") or []
        squad_rows: list[dict[str, Any]] = []
        player_ids: set[int] = set()
        for section_index, section in enumerate(sections):
            if not isinstance(section, Mapping):
                continue
            section_title = section.get("title") or section.get("role")
            for member_index, member in enumerate(section.get("members") or []):
                if not isinstance(member, Mapping):
                    raise CatalogShapeError(
                        f"team {team_id} squad member is not an object"
                    )
                member_id = member.get("id")
                if member_id is None:
                    raise CatalogShapeError(
                        f"team {team_id} squad member has no source id"
                    )
                role = str(section_title or "").lower()
                is_coach = "coach" in role or "manager" in role
                if not is_coach:
                    try:
                        player_ids.add(int(member_id))
                    except (TypeError, ValueError):
                        pass
                squad_rows.append(
                    {
                        "team_id": str(team_id),
                        "member_id": str(member_id),
                        "member_type": "coach" if is_coach else "player",
                        "member_name": member.get("name"),
                        "position": member.get("position"),
                        "position_description_json": member.get("positionDescription"),
                        "shirt_number": member.get("shirtNumber"),
                        "date_of_birth": member.get("dateOfBirthUTC")
                        or member.get("dateOfBirth"),
                        "height_cm": member.get("height"),
                        "country_code": member.get("ccode"),
                        "member_json": member,
                        "section": section_title,
                        "section_index": section_index,
                        "member_index": member_index,
                        "snapshot_date": snapshot_date,
                        "observed_at": observed_at,
                    }
                )
        return team_row, squad_rows, player_ids

    def sync_team_snapshots(
        self,
        bundle: SeasonBundle,
        *,
        refresh_after: timedelta = timedelta(hours=20),
        limit: Optional[int] = None,
    ) -> tuple[OperationResult, set[int]]:
        teams = list(bundle.teams)
        result = OperationResult(
            "team_snapshots",
            attempted=len(teams),
            metadata={
                "competition_id": bundle.scope.competition_id,
                "source_season_key": bundle.scope.source_season_key,
                "snapshot_semantics": "global_observed_at_not_historical",
            },
        )
        now = utc_now()
        due = []
        player_ids: set[int] = set()
        for team in teams:
            team_id = team.get("team_id")
            previous = self.repository.latest_entity_success("team", team_id)
            validated_at = (
                _aware_datetime(
                    previous.get("completed_at") or previous.get("fetched_at")
                )
                if previous
                else None
            )
            if (
                previous is not None
                and validated_at is not None
                and now - validated_at < refresh_after
                and self.mode != RunMode.REPLAY
            ):
                result.skipped += 1
                player_ids.update(self.repository.current_squad_player_ids(team_id))
            else:
                due.append(team)
        teams = due
        if limit is not None:
            teams = teams[: max(0, int(limit))]
        requests = [
            (str(team["team_id"]), "teams", {"id": str(team["team_id"])})
            for team in teams
        ]
        fetched = self._fetch_many(requests)
        for key, outcome in fetched.items():
            if isinstance(outcome, Exception):
                result.errors.append(f"team {key}: {outcome}")
                continue
            fetch = outcome
            if not fetch.ok:
                self._commit_for_fetch(
                    fetch,
                    target_type="team",
                    entity_id=key,
                )
                self._record_failure(result, key, fetch)
                continue
            try:
                observed = _aware_datetime(fetch.fetched_at) or utc_now()
                team_row, squad_rows, ids = self._team_rows(
                    int(key), fetch.data, observed
                )
                player_ids.update(ids)
                inventory, unknown = self._inventory_rows(
                    "team", inventory_json_paths(fetch.data)
                )
                if unknown:
                    raise CatalogShapeError(
                        f"unclassified team JSON paths: {list(unknown)}"
                    )
                paths = self._commit_for_fetch(
                    fetch,
                    target_type="team",
                    entity_id=key,
                    datasets=[
                        TableRows(
                            "fotmob_team_snapshots", [team_row], "team_snapshots"
                        ),
                        TableRows(
                            "fotmob_squad_snapshots",
                            squad_rows,
                            "squad_snapshots",
                        ),
                        TableRows(
                            "fotmob_field_inventory",
                            inventory,
                            "field_inventory",
                            partition_cols=("target_type",),
                        ),
                    ],
                    expected_counts={
                        "team_snapshots": 1,
                        "squad_snapshots": len(squad_rows),
                    },
                    capabilities={"squad_members": len(squad_rows)},
                    unknown_paths=unknown,
                )
                result.tables.extend(paths)
                result.succeeded += 1
                result.counts["teams"] = result.counts.get("teams", 0) + 1
                result.counts["squad_members"] = result.counts.get(
                    "squad_members", 0
                ) + len(squad_rows)
            except Exception as exc:
                result.errors.append(f"team {key} parse: {type(exc).__name__}: {exc}")
                self._commit_for_fetch(
                    fetch,
                    target_type="team",
                    status=ManifestStatus.SCHEMA_DRIFT,
                    entity_id=key,
                    error_code=type(exc).__name__,
                    error=str(exc),
                )
        return result, player_ids

    def _resolve_next_build_id(self, *, force: bool = False) -> str:
        """Resolve the rotating Next.js build once per snapshot batch."""

        if self._next_build_id and not force:
            return self._next_build_id
        if self.mode == RunMode.REPLAY:
            raise RuntimeError(
                "offline player replay needs an explicit/cached Next build id"
            )
        fetch_document = getattr(self.transport, "fetch_document", None)
        if fetch_document is None:
            raise RuntimeError("transport does not support Next build discovery")
        max_attempts = max(1, int(getattr(self.transport, "max_attempts", 1)))
        with self._budget_lock:
            if self.ledger.remaining_requests < max_attempts:
                raise BudgetExceeded(
                    "remaining request budget cannot cover Next build "
                    f"discovery retries ({self.ledger.remaining_requests}<"
                    f"{max_attempts})"
                )
        fetch = fetch_document("https://www.fotmob.com/")
        self._account(fetch)
        if not fetch.ok or not fetch.body:
            raise RuntimeError(fetch.error or "FotMob homepage is unavailable")
        text = fetch.body.decode("utf-8", errors="replace")
        match = re.search(r'"buildId"\s*:\s*"([^"\\]+)"', text)
        if match is None:
            raise RuntimeError("FotMob homepage does not contain a Next buildId")
        self._next_build_id = match.group(1)
        return self._next_build_id

    @staticmethod
    def _player_snapshot_row(
        player_id: int,
        payload: Mapping[str, Any],
        observed_at: datetime,
    ) -> dict[str, Any]:
        page_props = payload.get("pageProps") or {}
        data = page_props.get("data") if isinstance(page_props, Mapping) else None
        if not isinstance(data, Mapping):
            # Some localized Next routes retain an outer props wrapper.
            props = payload.get("props") or {}
            nested = props.get("pageProps") if isinstance(props, Mapping) else None
            data = nested.get("data") if isinstance(nested, Mapping) else None
        if not isinstance(data, Mapping):
            raise CatalogShapeError("player Next payload lacks pageProps.data")
        primary_team = data.get("primaryTeam") or {}
        main_league = data.get("mainLeague") or {}
        position = data.get("positionDescription")
        if isinstance(position, Mapping):
            position_label = (position.get("primaryPosition") or {}).get(
                "label"
            ) or position.get("label")
        else:
            position_label = position
        contract_end = data.get("contractEnd")
        if isinstance(contract_end, Mapping):
            contract_end = contract_end.get("utcTime")
        return {
            "player_id": str(data.get("id") or player_id),
            "name": data.get("name"),
            "birth_date": data.get("birthDate"),
            "is_coach": data.get("isCoach"),
            "is_captain": data.get("isCaptain"),
            "gender": data.get("gender"),
            "primary_team_id": primary_team.get("teamId"),
            "primary_team_name": primary_team.get("teamName"),
            "position_description": position_label,
            "main_league_id": main_league.get("leagueId"),
            "main_league_name": main_league.get("leagueName"),
            "contract_end": contract_end,
            "player_information_json": data.get("playerInformation"),
            "injury_information_json": data.get("injuryInformation"),
            "trophies_json": data.get("trophies"),
            "career_history_json": data.get("careerHistory"),
            "stat_seasons_json": data.get("statSeasons"),
            "first_season_stats_json": data.get("firstSeasonStats"),
            "recent_matches_json": data.get("recentMatches"),
            "market_values_json": data.get("marketValues"),
            "traits_json": data.get("traits"),
            "meta_json": data.get("meta"),
            "coach_stats_json": data.get("coachStats"),
            "next_match_json": data.get("nextMatch"),
            "payload_json": data,
            "snapshot_date": observed_at.date().isoformat(),
            "observed_at": observed_at,
        }

    def sync_player_snapshots(
        self,
        player_ids: Iterable[int],
        *,
        build_id: Optional[str] = None,
        refresh_after: timedelta = timedelta(days=7),
        limit: Optional[int] = None,
    ) -> OperationResult:
        """Refresh global player snapshots without season mislabelling.

        A rotating Next build is resolved once.  If it changes between
        resolution and the batch, all 404s are retried once under one newly
        resolved build rather than downloading the homepage per player.
        """

        ids = list(dict.fromkeys(int(value) for value in player_ids))
        result = OperationResult(
            "player_snapshots",
            attempted=len(ids),
            metadata={"snapshot_semantics": "global_observed_at_not_historical"},
        )
        now = utc_now()
        due: list[int] = []
        for player_id in ids:
            previous = self.repository.latest_entity_success("player", player_id)
            fetched_at = (
                _aware_datetime(
                    previous.get("completed_at") or previous.get("fetched_at")
                )
                if previous
                else None
            )
            if (
                previous is not None
                and fetched_at is not None
                and now - fetched_at < refresh_after
                and self.mode != RunMode.REPLAY
            ):
                result.skipped += 1
            else:
                due.append(player_id)
        due_before_limit = len(due)
        if limit is not None:
            due = due[: max(0, int(limit))]
        result.metadata["due_before_limit"] = due_before_limit
        result.metadata["deferred_by_limit"] = due_before_limit - len(due)
        if not due:
            return result
        try:
            current_build = build_id or self._resolve_next_build_id()
        except Exception as exc:
            result.errors.append(f"Next build discovery: {type(exc).__name__}: {exc}")
            return result

        def requests_for(values: Iterable[int], active_build: str):
            return [
                (
                    str(player_id),
                    f"https://www.fotmob.com/_next/data/{active_build}/players/"
                    f"{player_id}.json",
                    None,
                )
                for player_id in values
            ]

        fetched = self._fetch_many(requests_for(due, current_build))
        rotated: list[int] = []
        for key, outcome in fetched.items():
            if (
                not isinstance(outcome, Exception)
                and outcome.outcome == FetchOutcome.NOT_AVAILABLE
                and build_id is None
                and self.mode != RunMode.REPLAY
            ):
                rotated.append(int(key))
        if rotated:
            try:
                refreshed_build = self._resolve_next_build_id(force=True)
                if refreshed_build != current_build:
                    fetched.update(
                        self._fetch_many(requests_for(rotated, refreshed_build))
                    )
            except Exception as exc:
                result.errors.append(f"Next build refresh: {type(exc).__name__}: {exc}")

        for key, outcome in fetched.items():
            if isinstance(outcome, Exception):
                result.errors.append(f"player {key}: {outcome}")
                continue
            fetch = outcome
            if not fetch.ok:
                self._commit_for_fetch(fetch, target_type="player", entity_id=key)
                self._record_failure(result, key, fetch)
                continue
            try:
                observed = _aware_datetime(fetch.fetched_at) or utc_now()
                row = self._player_snapshot_row(int(key), fetch.data, observed)
                inventory, unknown = self._inventory_rows(
                    "player", inventory_json_paths(fetch.data)
                )
                if unknown:
                    raise CatalogShapeError(
                        f"unclassified player JSON paths: {list(unknown)}"
                    )
                paths = self._commit_for_fetch(
                    fetch,
                    target_type="player",
                    entity_id=key,
                    datasets=[
                        TableRows(
                            "fotmob_player_snapshots",
                            [row],
                            "player_snapshots",
                        ),
                        TableRows(
                            "fotmob_field_inventory",
                            inventory,
                            "field_inventory",
                            partition_cols=("target_type",),
                        ),
                    ],
                    expected_counts={"player_snapshots": 1},
                    capabilities={
                        "has_market_values": bool(row.get("market_values_json")),
                        "has_career_history": bool(row.get("career_history_json")),
                    },
                    unknown_paths=unknown,
                )
                result.tables.extend(paths)
                result.succeeded += 1
                result.counts["players"] = result.counts.get("players", 0) + 1
            except Exception as exc:
                result.errors.append(f"player {key} parse: {type(exc).__name__}: {exc}")
                self._commit_for_fetch(
                    fetch,
                    target_type="player",
                    status=ManifestStatus.SCHEMA_DRIFT,
                    entity_id=key,
                    error_code=type(exc).__name__,
                    error=str(exc),
                )
        return result

    @staticmethod
    def _resolved_coverage_hash(
        coverage: Optional[Mapping[str, Any]],
        counts: Optional[Mapping[str, int]],
        coverage_hash: Optional[str],
    ) -> str:
        if coverage_hash is not None:
            value = str(coverage_hash).strip()
            if not value:
                raise ValueError("coverage_hash must not be empty")
            return value
        return hashlib.sha256(
            _json(
                {
                    "coverage": dict(coverage or {}),
                    "counts": {
                        str(key): int(value) for key, value in (counts or {}).items()
                    },
                }
            ).encode("utf-8")
        ).hexdigest()

    def record_scope_completion(
        self,
        competition_id: int,
        source_season_key: str,
        *,
        plan_signature: str,
        coverage: Optional[Mapping[str, Any]] = None,
        counts: Optional[Mapping[str, int]] = None,
        coverage_hash: Optional[str] = None,
    ) -> list[str]:
        """Commit a full season-scope coverage marker after all work succeeds."""

        scope = ScopeRef(int(competition_id), source_season_key)
        signature = str(plan_signature).strip()
        if not signature:
            raise ValueError("plan_signature must not be empty")
        normalized_counts = {
            str(key): int(value) for key, value in (counts or {}).items()
        }
        resolved_hash = self._resolved_coverage_hash(
            coverage, normalized_counts, coverage_hash
        )
        commit = TargetCommit(
            run_id=self.run_id,
            target_type="scope_completion",
            target_key=_completion_target_key(
                "scope_completion",
                scope.competition_id,
                scope.source_season_key,
                signature,
            ),
            status=ManifestStatus.SUCCESS,
            competition_id=str(scope.competition_id),
            source_season_key=scope.source_season_key,
            entity_id=signature,
            content_hash=resolved_hash,
            expected_counts=normalized_counts,
            capabilities={
                "plan_signature": signature,
                "coverage": dict(coverage or {}),
                "coverage_hash": resolved_hash,
            },
            completed_at=utc_now(),
        )
        return self.repository.commit(commit)

    def record_competition_completion(
        self,
        competition_id: int,
        *,
        plan_signature: str,
        coverage: Optional[Mapping[str, Any]] = None,
        counts: Optional[Mapping[str, int]] = None,
        coverage_hash: Optional[str] = None,
    ) -> list[str]:
        """Commit a competition-wide coverage marker (for transfer streams)."""

        resolved_competition_id = int(competition_id)
        signature = str(plan_signature).strip()
        if not signature:
            raise ValueError("plan_signature must not be empty")
        normalized_counts = {
            str(key): int(value) for key, value in (counts or {}).items()
        }
        resolved_hash = self._resolved_coverage_hash(
            coverage, normalized_counts, coverage_hash
        )
        commit = TargetCommit(
            run_id=self.run_id,
            target_type="competition_completion",
            target_key=_completion_target_key(
                "competition_completion",
                resolved_competition_id,
                None,
                signature,
            ),
            status=ManifestStatus.SUCCESS,
            competition_id=str(resolved_competition_id),
            entity_id=signature,
            content_hash=resolved_hash,
            expected_counts=normalized_counts,
            capabilities={
                "plan_signature": signature,
                "coverage": dict(coverage or {}),
                "coverage_hash": resolved_hash,
            },
            completed_at=utc_now(),
        )
        return self.repository.commit(commit)

    def report(
        self, operations: Sequence[OperationResult], started_at: datetime
    ) -> RunReport:
        stats = self.transport.snapshot_stats()
        transport = asdict(stats)
        # A proxy byte in a direct-only pipeline is a correctness failure, not
        # merely a surprising metric.
        if int(transport.get("proxy_bytes", 0)) != 0:
            operations = list(operations) + [
                OperationResult(
                    "proxy_invariant",
                    errors=[f"proxy_bytes={transport['proxy_bytes']} (expected 0)"],
                )
            ]
        return RunReport(
            run_id=self.run_id,
            mode=self.mode.value,
            started_at=started_at,
            completed_at=utc_now(),
            operations=list(operations),
            budget=self.ledger.as_dict(),
            transport=transport,
        )


__all__ = [
    "CatalogResult",
    "CompetitionDiscoveryResult",
    "FotMobIngestService",
    "OperationResult",
    "RunReport",
]
