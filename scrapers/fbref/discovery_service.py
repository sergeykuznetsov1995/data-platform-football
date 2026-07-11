"""Bounded raw-first orchestration for FBref discovery pages.

The service deliberately knows nothing about Iceberg or Airflow.  It walks a
small, explicitly selected graph and commits every HTML page to ``RawPageStore``
before invoking the pure parsers from :mod:`scrapers.fbref.discovery`.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional, Sequence

from scrapers.fbref.discovery import (
    CompetitionRef,
    DiscoveryPageResult,
    MatchRef,
    SeasonRef,
    parse_competition_html,
    parse_competition_index_html,
    parse_schedule_html,
    parse_season_html,
)
from scrapers.fbref.match_parser import DatasetStatus
from scrapers.fbref.raw_store import (
    FETCHER_VERSION,
    PageTarget,
    RawPageStore,
    RawStoreError,
    competition_index_target,
    competition_page_target,
    schedule_page_target,
    season_page_target,
)


DISCOVERY_FETCHER_VERSION = f"{FETCHER_VERSION}-discovery-v1"


class DiscoveryError(RuntimeError):
    """Base error for a bounded discovery run."""


class NetworkPageBudgetExceeded(DiscoveryError):
    """The run attempted to fetch more pages than explicitly allowed."""


Loader = Callable[[str, str], Optional[str]]


def _jsonable(value):
    if is_dataclass(value):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


@dataclass
class DiscoveryRunResult:
    """Serializable result of one index or selected-graph run."""

    mode: str
    offline: bool
    competitions: List[CompetitionRef] = field(default_factory=list)
    seasons: List[SeasonRef] = field(default_factory=list)
    schedules: List[Dict[str, object]] = field(default_factory=list)
    matches: List[MatchRef] = field(default_factory=list)
    page_manifests: List[str] = field(default_factory=list)
    errors: List[Dict[str, str]] = field(default_factory=list)
    raw_hits: int = 0
    raw_writes: int = 0
    network_pages: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict:
        return _jsonable({
            "mode": self.mode,
            "offline": self.offline,
            "competitions": self.competitions,
            "seasons": self.seasons,
            "schedules": self.schedules,
            "matches": self.matches,
            "page_manifests": self.page_manifests,
            "errors": self.errors,
            "raw": {
                "hits": self.raw_hits,
                "writes": self.raw_writes,
                "network_pages": self.network_pages,
            },
        })


class FBrefDiscoveryService:
    """Walk FBref discovery targets sequentially with a hard network budget."""

    def __init__(
        self,
        raw_store: RawPageStore,
        *,
        loader: Optional[Loader] = None,
        offline: bool = False,
        max_network_pages: int = 4,
    ) -> None:
        if max_network_pages < 0:
            raise ValueError("max_network_pages must be non-negative")
        if not offline and loader is None:
            raise ValueError("A loader is required unless offline=True")
        self.raw_store = raw_store
        self.loader = loader
        self.offline = offline
        self.max_network_pages = max_network_pages
        self.raw_hits = 0
        self.raw_writes = 0
        self.network_pages = 0
        self.page_manifests: List[str] = []

    def _load_and_parse(
        self,
        target: PageTarget,
        parser: Callable[[str], DiscoveryPageResult],
    ) -> DiscoveryPageResult:
        if self.raw_store.has_page(target):
            self.raw_hits += 1
        else:
            if self.offline:
                # ``load_html`` supplies the canonical missing/corrupt error.
                self.raw_store.load_html(target)
            else:
                if self.network_pages >= self.max_network_pages:
                    raise NetworkPageBudgetExceeded(
                        "FBref discovery network page budget exceeded: "
                        f"limit={self.max_network_pages}, next={target.target_id}"
                    )
                assert self.loader is not None
                # Consume the budget before transport I/O: a timeout or a
                # rejected response still spent one source/proxy attempt.
                self.network_pages += 1
                html = self.loader(target.canonical_url, target.page_kind)
                if not html:
                    raise RawStoreError(
                        f"Loader returned no HTML for {target.target_id}"
                    )
                self.raw_store.store_html(
                    target,
                    html,
                    fetcher_version=DISCOVERY_FETCHER_VERSION,
                )
                self.raw_writes += 1

        # Always parse the committed object, never the transport response.
        html, record = self.raw_store.load_html(target)
        result = parser(html)
        manifest_key = self.raw_store.write_page_parse_manifests(record, result)
        self.page_manifests.append(manifest_key)
        return result

    @staticmethod
    def _dataset_records(
        result: DiscoveryPageResult,
        dataset: str,
    ) -> list:
        parsed = result.datasets.get(dataset)
        return [] if parsed is None else list(parsed.records)

    @staticmethod
    def _append_parse_errors(
        output: DiscoveryRunResult,
        target: PageTarget,
        result: DiscoveryPageResult,
    ) -> None:
        for name, dataset in result.datasets.items():
            if dataset.status != DatasetStatus.ERROR:
                continue
            output.errors.append({
                "target_id": target.target_id,
                "page_kind": target.page_kind,
                "dataset": name,
                "reason": dataset.reason or "parse_error",
                "error_type": dataset.error_type or "DiscoveryParseError",
                "message": dataset.error_message or "",
            })

    def _sync_counters(self, output: DiscoveryRunResult) -> None:
        output.raw_hits = self.raw_hits
        output.raw_writes = self.raw_writes
        output.network_pages = self.network_pages
        output.page_manifests = list(self.page_manifests)

    def discover_index(self) -> DiscoveryRunResult:
        output = DiscoveryRunResult(mode="index", offline=self.offline)
        target = competition_index_target()
        try:
            result = self._load_and_parse(target, parse_competition_index_html)
            output.competitions.extend(
                self._dataset_records(result, "competitions")
            )
            self._append_parse_errors(output, target, result)
        except Exception as exc:
            output.errors.append({
                "target_id": target.target_id,
                "page_kind": target.page_kind,
                "dataset": "competitions",
                "reason": "target_failed",
                "error_type": type(exc).__name__,
                "message": str(exc),
            })
        self._sync_counters(output)
        return output

    def discover_graph(
        self,
        competition_ids: Sequence[str],
        *,
        max_competitions: int = 1,
        max_seasons_per_competition: int = 1,
        season_labels: Optional[Iterable[str]] = None,
    ) -> DiscoveryRunResult:
        if max_competitions <= 0:
            raise ValueError("max_competitions must be positive")
        if max_seasons_per_competition <= 0:
            raise ValueError("max_seasons_per_competition must be positive")
        requested_ids = list(dict.fromkeys(str(item) for item in competition_ids))
        if not requested_ids:
            raise ValueError("At least one --competition-id is required")
        if len(requested_ids) > max_competitions:
            raise ValueError(
                f"Requested {len(requested_ids)} competitions, "
                f"limit is {max_competitions}"
            )

        output = DiscoveryRunResult(mode="discover", offline=self.offline)
        index_target = competition_index_target()
        try:
            index_result = self._load_and_parse(
                index_target, parse_competition_index_html
            )
            self._append_parse_errors(output, index_target, index_result)
        except Exception as exc:
            output.errors.append({
                "target_id": index_target.target_id,
                "page_kind": index_target.page_kind,
                "dataset": "competitions",
                "reason": "target_failed",
                "error_type": type(exc).__name__,
                "message": str(exc),
            })
            self._sync_counters(output)
            return output

        by_id = {
            item.competition_id: item
            for item in self._dataset_records(index_result, "competitions")
        }
        selected_labels = (
            set(str(label) for label in season_labels)
            if season_labels else None
        )

        for competition_id in requested_ids:
            competition = by_id.get(competition_id)
            if competition is None:
                output.errors.append({
                    "target_id": f"fbref:competition:{competition_id}",
                    "page_kind": "competition",
                    "dataset": "competitions",
                    "reason": "competition_not_discovered",
                    "error_type": "UnknownCompetitionError",
                    "message": f"Competition id {competition_id!r} is absent from /en/comps/",
                })
                continue

            output.competitions.append(competition)
            competition_target = competition_page_target(
                competition.competition_id,
                competition.history_url,
            )
            try:
                competition_result = self._load_and_parse(
                    competition_target,
                    lambda html, item=competition: parse_competition_html(
                        html, item
                    ),
                )
                self._append_parse_errors(
                    output, competition_target, competition_result
                )
            except Exception as exc:
                output.errors.append({
                    "target_id": competition_target.target_id,
                    "page_kind": competition_target.page_kind,
                    "dataset": "seasons",
                    "reason": "target_failed",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                })
                continue
            if competition_result.has_errors:
                continue

            seasons = self._dataset_records(competition_result, "seasons")
            if selected_labels is not None:
                seasons = [
                    season for season in seasons
                    if season.season_label in selected_labels
                ]
                missing = selected_labels - {
                    season.season_label for season in seasons
                }
                if missing:
                    output.errors.append({
                        "target_id": competition_target.target_id,
                        "page_kind": "competition",
                        "dataset": "seasons",
                        "reason": "season_not_discovered",
                        "error_type": "UnknownSeasonError",
                        "message": f"Missing requested season labels: {sorted(missing)}",
                    })
            seasons = seasons[:max_seasons_per_competition]

            for season in seasons:
                output.seasons.append(season)
                season_target = season_page_target(
                    season.competition_id,
                    season.season_id,
                    season.season_url,
                )
                try:
                    season_result = self._load_and_parse(
                        season_target,
                        lambda html, item=season: parse_season_html(html, item),
                    )
                    self._append_parse_errors(output, season_target, season_result)
                except Exception as exc:
                    output.errors.append({
                        "target_id": season_target.target_id,
                        "page_kind": season_target.page_kind,
                        "dataset": "schedules",
                        "reason": "target_failed",
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                    })
                    continue
                if season_result.has_errors:
                    continue

                for schedule in self._dataset_records(
                    season_result, "schedules"
                ):
                    schedule_target = schedule_page_target(
                        schedule.competition_id,
                        schedule.season_id,
                        schedule.schedule_url,
                    )
                    try:
                        schedule_result = self._load_and_parse(
                            schedule_target,
                            lambda html, item=season: parse_schedule_html(
                                html, item
                            ),
                        )
                        self._append_parse_errors(
                            output, schedule_target, schedule_result
                        )
                    except Exception as exc:
                        output.errors.append({
                            "target_id": schedule_target.target_id,
                            "page_kind": schedule_target.page_kind,
                            "dataset": "schedule_rows",
                            "reason": "target_failed",
                            "error_type": type(exc).__name__,
                            "message": str(exc),
                        })
                        continue

                    rows = self._dataset_records(
                        schedule_result, "schedule_rows"
                    )
                    matches = self._dataset_records(schedule_result, "matches")
                    output.schedules.append({
                        **asdict(schedule),
                        "row_count": len(rows),
                        "match_count": len(matches),
                        "status": schedule_result.status.value,
                    })
                    output.matches.extend(matches)

        # A match can appear in multiple stage blocks; keep first source order.
        output.matches = list({item.match_id: item for item in output.matches}.values())
        self._sync_counters(output)
        return output
