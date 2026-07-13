"""Pure offline parser for one stored FBref match page."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import re
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


MATCH_PARSER_VERSION = "fbref-match-parser-v2"
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
    source_present: bool = False,
    source_has_rows: bool = False,
    allow_empty_source: bool = False,
    missing_is_error: bool = False,
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
    # A required dataset fails the contract when the source published rows the
    # parser could not read, or published no container at all.  It must not
    # fail when the source published the container with no rows in it: FBref
    # ships empty player tables for matches it has no player data for (older
    # relegation play-offs), and calling that a parser failure would reject a
    # page whose events, team stats and officials are perfectly parseable.
    if required and (not source_present or source_has_rows):
        return DatasetParseResult(
            dataset=dataset,
            status=DatasetStatus.ERROR,
            reason="required_dataset_contract_failed",
            error_type="MatchPlayerContractError",
            error_message="Expected two valid player summary tables",
        )
    if not source_present:
        if not missing_is_error:
            return DatasetParseResult(
                dataset=dataset,
                status=DatasetStatus.NOT_APPLICABLE,
                reason="source_container_not_published",
            )
        return DatasetParseResult(
            dataset=dataset,
            status=DatasetStatus.ERROR,
            reason="source_container_missing",
            error_type="MatchPageContractError",
            error_message=(
                f"Expected source container for {dataset} is absent"
            ),
        )
    if source_has_rows:
        return DatasetParseResult(
            dataset=dataset,
            status=DatasetStatus.ERROR,
            reason="source_container_unparsed",
            error_type="MatchPageSchemaDriftError",
            error_message=(
                f"Source container for {dataset} has rows but parser returned none"
            ),
        )
    if not allow_empty_source:
        return DatasetParseResult(
            dataset=dataset,
            status=DatasetStatus.ERROR,
            reason="required_source_container_empty",
            error_type="MatchPageContractError",
            error_message=f"Source container for {dataset} is unexpectedly empty",
        )
    return DatasetParseResult(
        dataset=dataset,
        status=empty_status,
        reason=empty_reason,
    )


def _table_has_rows(table) -> bool:
    body = table.find("tbody")
    rows = (body or table).find_all("tr")
    return any(
        "thead" not in set(row.get("class") or [])
        and bool(row.get_text(" ", strip=True))
        for row in rows
    )


def _event_div_has_content(div) -> bool:
    """True when an ``event`` div carries a real event, not an empty column.

    Every FBref match page ships two empty ``div.event`` side containers
    (``id="a"`` / ``id="b"``) even when the source publishes no events at all,
    so their mere presence must not be read as unparsed event rows: that turns
    a genuine source gap into a false schema-drift error.
    """
    if div.find("a", href=lambda href: href and "/players/" in href):
        return True
    return bool(div.get_text(" ", strip=True))


def _match_source_evidence(soup, comment_tables) -> Dict[str, tuple[bool, bool]]:
    """Inventory source containers before any parser may collapse them.

    A missing container is schema drift, not proof of a zero-row dataset.  The
    writer may clear live rows only when the stored page explicitly contains
    an empty/restricted source section.
    """

    tables = list(soup.find_all("table")) + list(comment_tables.values())
    table_pairs = [
        (str(table.get("id") or ""), table)
        for table in tables
    ]

    def matching(pattern: str):
        compiled = re.compile(pattern, re.IGNORECASE)
        return [table for table_id, table in table_pairs if compiled.fullmatch(table_id)]

    shot_tables = matching(r"shots?(?:_all|_both)?")
    summary_tables = matching(r"stats_[a-f0-9]{8}_summary")
    keeper_tables = matching(r"keeper_stats_[a-f0-9]{8}")

    events_wrap = soup.find("div", id="events_wrap")
    event_divs = (events_wrap or soup).find_all(
        "div",
        class_=lambda value: value
        and "event" in str(value).casefold().split(),
    )
    legacy_events = event_divs[0] if event_divs else None
    lineup_divs = soup.find_all(
        "div",
        class_=lambda value: value and "lineup" in str(value).casefold(),
    )
    if not lineup_divs:
        lineup_divs = soup.find_all(
            "div",
            id=lambda value: value and "lineup" in str(value).casefold(),
        )
    team_stats = soup.find("div", id="team_stats")
    scorebox = soup.find("div", class_="scorebox") or soup.find(
        "div", id="scorebox"
    )
    officials_label = next(
        (
            tag
            for tag in soup.find_all("small")
            if tag.get_text(strip=True).casefold() == "officials"
        ),
        None,
    )
    officials_block = (
        officials_label.find_parent("div")
        if officials_label is not None
        else None
    )

    return {
        "shot_events": (
            bool(shot_tables or soup.find(id="all_shots")),
            any(_table_has_rows(table) for table in shot_tables),
        ),
        "match_events": (
            bool(events_wrap or legacy_events),
            any(_event_div_has_content(div) for div in event_divs),
        ),
        "lineups": (
            bool(lineup_divs),
            any(
                div.find("a", href=lambda href: href and "/players/" in href)
                is not None
                for div in lineup_divs
            ),
        ),
        "match_team_stats": (
            team_stats is not None,
            bool(team_stats and team_stats.get_text(" ", strip=True)),
        ),
        "match_player_stats": (
            len(summary_tables) == 2,
            any(_table_has_rows(table) for table in summary_tables),
        ),
        "match_managers": (
            scorebox is not None,
            bool(scorebox and scorebox.get_text(" ", strip=True)),
        ),
        "match_officials": (
            officials_block is not None,
            bool(officials_block and officials_block.find("span")),
        ),
        "match_keeper_stats": (
            len(keeper_tables) == 2,
            any(_table_has_rows(table) for table in keeper_tables),
        ),
    }


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

    source_evidence = _match_source_evidence(soup, comment_tables)

    def run(name: str, callback: Callable, **kwargs) -> DatasetParseResult:
        if name not in enabled:
            return not_applicable(name, "dataset_not_requested")
        present, has_rows = source_evidence[name]
        return _run_parser(
            name,
            callback,
            source_present=present,
            source_has_rows=has_rows,
            **kwargs,
        )

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
            allow_empty_source=True,
        ),
        "match_events": run(
            "match_events",
            lambda: parsers["match_events"](soup),
            allow_empty_source=True,
        ),
        "lineups": run(
            "lineups",
            lambda: parsers["lineups"](
                soup, comment_tables=comment_tables
            ),
            allow_empty_source=True,
        ),
        "match_team_stats": run(
            "match_team_stats",
            lambda: parsers["match_team_stats"](soup, comment_tables),
        ),
        "match_player_stats": run(
            "match_player_stats",
            lambda: parsers["match_player_stats"](soup, comment_tables),
            required=require_player_contract,
            # Empty published tables are a source gap, not a parse failure.
            allow_empty_source=True,
        ),
        "match_managers": run(
            "match_managers",
            lambda: parsers["match_managers"](soup),
            missing_is_error=True,
        ),
        "match_officials": run(
            "match_officials",
            lambda: parsers["match_officials"](soup),
            allow_empty_source=True,
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
