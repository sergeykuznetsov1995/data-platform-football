"""Pure offline parser for one stored FBref match page."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Dict, Optional, Set

import pandas as pd
from bs4 import BeautifulSoup

from scrapers.fbref.html_parser import (
    extract_tables_from_comments,
    parse_events_from_scorebox,
    parse_keeper_match_stats_tables,
    parse_lineup_table,
    parse_match_managers,
    parse_match_officials,
    parse_player_match_stats_tables,
    parse_shots_table,
    parse_team_match_stats_table,
)


MATCH_PARSER_VERSION = "fbref-match-parser-v1"
MATCH_COMPLETION_CONTRACT_VERSION = "match-v2-two-team-marker-last"


class DatasetStatus(str, Enum):
    AVAILABLE = "available"
    EMPTY = "empty"
    RESTRICTED = "restricted"
    NOT_APPLICABLE = "not_applicable"
    ERROR = "error"


@dataclass
class DatasetParseResult:
    dataset: str
    status: DatasetStatus
    frame: Optional[pd.DataFrame] = None
    reason: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    exception: Optional[Exception] = field(default=None, repr=False)

    @property
    def row_count(self) -> int:
        return 0 if self.frame is None else len(self.frame)


@dataclass
class MatchParseResult:
    parser_version: str
    parsed_at: str
    status: DatasetStatus
    datasets: Dict[str, DatasetParseResult]

    @property
    def has_errors(self) -> bool:
        return self.status == DatasetStatus.ERROR


class MatchPageParseError(RuntimeError):
    """One or more match datasets failed parsing or their contract."""


def not_applicable(dataset: str, reason: str) -> DatasetParseResult:
    return DatasetParseResult(
        dataset=dataset,
        status=DatasetStatus.NOT_APPLICABLE,
        reason=reason,
    )


def _run_parser(
    dataset: str,
    parser: Callable[[], Optional[pd.DataFrame]],
    *,
    empty_status: DatasetStatus = DatasetStatus.EMPTY,
    empty_reason: str = "parser_returned_no_rows",
    required: bool = False,
) -> DatasetParseResult:
    try:
        frame = parser()
    except Exception as exc:  # each dataset must leave an explicit manifest row
        return DatasetParseResult(
            dataset=dataset,
            status=DatasetStatus.ERROR,
            reason="parser_exception",
            error_type=type(exc).__name__,
            error_message=str(exc)[:1000],
            exception=exc,
        )
    if frame is not None and not frame.empty:
        return DatasetParseResult(
            dataset=dataset,
            status=DatasetStatus.AVAILABLE,
            frame=frame,
        )
    if required:
        return DatasetParseResult(
            dataset=dataset,
            status=DatasetStatus.ERROR,
            reason="required_dataset_contract_failed",
            error_type="MatchPlayerContractError",
            error_message="Expected two valid player summary tables",
        )
    return DatasetParseResult(
        dataset=dataset,
        status=empty_status,
        reason=empty_reason,
    )


def parse_match_html(
    html: str,
    *,
    match_id: str,
    league: str,
    season: int,
    parser_version: str = MATCH_PARSER_VERSION,
    enabled_datasets: Optional[Set[str]] = None,
    require_player_contract: bool = True,
    parser_overrides: Optional[Dict[str, Callable]] = None,
) -> MatchParseResult:
    """Parse all current match datasets without transport or proxy objects."""
    soup = BeautifulSoup(html, "html.parser")
    parsers = {
        "extract_tables": extract_tables_from_comments,
        "shot_events": parse_shots_table,
        "match_events": parse_events_from_scorebox,
        "lineups": parse_lineup_table,
        "match_team_stats": parse_team_match_stats_table,
        "match_player_stats": parse_player_match_stats_tables,
        "match_managers": parse_match_managers,
        "match_officials": parse_match_officials,
        "match_keeper_stats": parse_keeper_match_stats_tables,
    }
    if parser_overrides:
        parsers.update(parser_overrides)
    enabled = (
        set(parsers) - {"extract_tables"}
        if enabled_datasets is None else enabled_datasets
    )
    try:
        comment_tables = parsers["extract_tables"](soup)
    except Exception as exc:
        datasets = {
            name: (
                DatasetParseResult(
                    dataset=name,
                    status=DatasetStatus.ERROR,
                    reason="parser_exception",
                    error_type=type(exc).__name__,
                    error_message=str(exc)[:1000],
                    exception=exc,
                )
                if name in enabled
                else not_applicable(name, "dataset_not_requested")
            )
            for name in set(parsers) - {"extract_tables"}
        }
        soup.decompose()
        return MatchParseResult(
            parser_version=parser_version,
            parsed_at=datetime.now(timezone.utc).isoformat(),
            status=DatasetStatus.ERROR,
            datasets=datasets,
        )

    def run(name: str, callback: Callable, **kwargs) -> DatasetParseResult:
        if name not in enabled:
            return not_applicable(name, "dataset_not_requested")
        return _run_parser(name, callback, **kwargs)

    shots_restricted = soup.find(id="all_shots") is not None
    datasets = {
        "shot_events": run(
            "shot_events",
            lambda: parsers["shot_events"](soup, comment_tables),
            empty_status=(
                DatasetStatus.RESTRICTED
                if shots_restricted else DatasetStatus.EMPTY
            ),
            empty_reason=(
                "source_section_without_shots_table"
                if shots_restricted else "parser_returned_no_rows"
            ),
        ),
        "match_events": run(
            "match_events", lambda: parsers["match_events"](soup)
        ),
        "lineups": run(
            "lineups",
            lambda: parsers["lineups"](
                soup, comment_tables=comment_tables
            ),
        ),
        "match_team_stats": run(
            "match_team_stats",
            lambda: parsers["match_team_stats"](soup, comment_tables),
        ),
        "match_player_stats": run(
            "match_player_stats",
            lambda: parsers["match_player_stats"](soup, comment_tables),
            required=require_player_contract,
        ),
        "match_managers": run(
            "match_managers", lambda: parsers["match_managers"](soup)
        ),
        "match_officials": run(
            "match_officials", lambda: parsers["match_officials"](soup)
        ),
        "match_keeper_stats": run(
            "match_keeper_stats",
            lambda: parsers["match_keeper_stats"](soup, comment_tables),
        ),
    }

    for dataset in datasets.values():
        if dataset.frame is None or dataset.frame.empty:
            continue
        frame = dataset.frame
        frame["match_id"] = match_id
        frame["league"] = league
        frame["season"] = season
        if dataset.dataset == "match_player_stats":
            frame["parser_contract_version"] = MATCH_COMPLETION_CONTRACT_VERSION

    status = (
        DatasetStatus.ERROR
        if any(d.status == DatasetStatus.ERROR for d in datasets.values())
        else DatasetStatus.AVAILABLE
    )
    soup.decompose()
    return MatchParseResult(
        parser_version=parser_version,
        parsed_at=datetime.now(timezone.utc).isoformat(),
        status=status,
        datasets=datasets,
    )
