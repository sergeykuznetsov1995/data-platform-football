"""Offline compatibility parsing and idempotent typed Bronze persistence.

This module is deliberately downstream of raw storage.  It accepts HTML bytes
or text supplied by a caller, delegates to the existing pure FBref parsers, and
never imports a URL builder, browser, HTTP client, or fetcher.

The legacy ``league`` and ``season`` columns are retained for current
Bronze/Silver consumers.  They are compatibility projections only:

* known competition IDs map to the aliases already used by the project;
* an unknown competition remains parseable and uses its source name (or a
  stable ``FBREF-<id>`` fallback);
* source-native competition and season IDs are always persisted separately.

The compatibility map must therefore never be iterated to choose crawl scope.
Discovery/frontier data is the sole authority for which pages reach this
adapter.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Dict, Mapping, Optional, Protocol, Sequence

import pandas as pd
import pyarrow as pa
from bs4 import BeautifulSoup

from scrapers.fbref.constants import LEAGUE_IDS
from scrapers.fbref.html_parser import (
    extract_tables_from_comments,
    find_player_stats_table,
    find_schedule_table,
    find_team_stats_table,
)
from scrapers.fbref.match_parser import (
    DatasetParseResult,
    DatasetStatus,
    MatchPageParseError,
    MatchParseResult,
    parse_match_html as parse_existing_match_html,
)


TYPED_BRONZE_PARSER_VERSION = "fbref-typed-bronze-v1"

# Output compatibility only.  This mapping is intentionally derived from the
# legacy aliases instead of being duplicated as another source of crawl scope.
COMPATIBILITY_LEAGUE_BY_COMPETITION_ID: Mapping[str, str] = MappingProxyType(
    {
        str(config["comp_id"]): alias
        for alias, config in LEAGUE_IDS.items()
    }
)

MATCH_DATASET_TABLES: Mapping[str, str] = MappingProxyType(
    {
        "shot_events": "fbref_shot_events",
        "match_events": "fbref_match_events",
        "lineups": "fbref_lineups",
        "match_team_stats": "fbref_match_team_stats",
        "match_managers": "fbref_match_managers",
        "match_officials": "fbref_match_officials",
        "match_keeper_stats": "fbref_match_keeper_stats",
        # Keep legacy player data last among typed data tables. Independent
        # dataset-availability evidence is the actual final commit marker.
        "match_player_stats": "fbref_match_player_stats",
    }
)
MATCH_COMPLETION_DATASET = "match_player_stats"
MATCH_AVAILABILITY_TABLE = "fbref_dataset_availability"

SEASON_DATASET_TABLES: Mapping[str, str] = MappingProxyType(
    {
        "player_stats": "fbref_player_stats",
        "team_stats": "fbref_team_stats",
        "player_shooting": "fbref_player_shooting",
        "team_shooting": "fbref_team_shooting",
        "player_passing": "fbref_player_passing",
        "team_passing": "fbref_team_passing",
        "player_passing_types": "fbref_player_passing_types",
        "team_passing_types": "fbref_team_passing_types",
        "player_gca": "fbref_player_gca",
        "team_gca": "fbref_team_gca",
        "player_defense": "fbref_player_defense",
        "team_defense": "fbref_team_defense",
        "player_possession": "fbref_player_possession",
        "team_possession": "fbref_team_possession",
        "player_playingtime": "fbref_player_playingtime",
        "team_playingtime": "fbref_team_playingtime",
        "player_misc": "fbref_player_misc",
        "team_misc": "fbref_team_misc",
        "keeper_keeper": "fbref_keeper_keeper",
        "keeper_keeper_adv": "fbref_keeper_keeper_adv",
    }
)

# Parser routing, not discovery scope.  A route is considered only when the
# caller supplies HTML for that already-discovered source page.
SEASON_ROUTE_DATASETS: Mapping[str, Sequence[tuple[str, str]]] = (
    MappingProxyType(
        {
            "standard": (("player", "stats"), ("team", "stats")),
            "shooting": (("player", "shooting"), ("team", "shooting")),
            "passing": (("player", "passing"), ("team", "passing")),
            "passing_types": (
                ("player", "passing_types"),
                ("team", "passing_types"),
            ),
            "gca": (("player", "gca"), ("team", "gca")),
            "defense": (("player", "defense"), ("team", "defense")),
            "possession": (
                ("player", "possession"),
                ("team", "possession"),
            ),
            "playingtime": (
                ("player", "playingtime"),
                ("team", "playingtime"),
            ),
            "misc": (("player", "misc"), ("team", "misc")),
            "keepers": (("keeper", "keeper"),),
            "keepersadv": (("keeper", "keeper_adv"),),
        }
    )
)

_SPLIT_YEAR_RE = re.compile(r"^(?P<start>\d{4})[-–](?P<end>\d{4})$")
_SINGLE_YEAR_RE = re.compile(r"^\d{4}$")


class TypedBronzeError(RuntimeError):
    """Typed compatibility parsing or persistence failed."""


class TypedBronzePersistenceError(TypedBronzeError):
    """A typed dataset could not be committed safely."""


_CLEAR_TO_EMPTY_STATUSES = frozenset(
    {
        DatasetStatus.EMPTY.value,
        DatasetStatus.RESTRICTED.value,
        DatasetStatus.NOT_APPLICABLE.value,
    }
)
_DISABLED_REASONS = frozenset(
    {
        "dataset_not_requested",
        "dataset_disabled",
        "source_container_not_published",
        "typed_source_table_not_published",
    }
)
_NOT_REQUESTED_REASONS = frozenset(
    {"dataset_not_requested", "dataset_disabled"}
)


def typed_result_requires_persistence(result: DatasetParseResult) -> bool:
    """Whether a parsed result must replace live typed state.

    Explicit empty-like observations are writes too: they replace the exact
    source partition or match with zero rows. A dataset omitted by caller
    configuration remains untouched.
    """

    status = str(getattr(result.status, "value", result.status)).casefold()
    if status == DatasetStatus.AVAILABLE.value:
        return True
    return (
        status in _CLEAR_TO_EMPTY_STATUSES
        and str(result.reason or "").casefold() not in _DISABLED_REASONS
    )


def _persistence_action(result: DatasetParseResult) -> str:
    status = str(getattr(result.status, "value", result.status)).casefold()
    if status == DatasetStatus.ERROR.value:
        raise TypedBronzePersistenceError(
            f"Cannot persist parser error for {result.dataset}"
        )
    if not typed_result_requires_persistence(result):
        if (
            status in _CLEAR_TO_EMPTY_STATUSES
            and str(result.reason or "").casefold() in _DISABLED_REASONS
        ):
            return "skip"
        raise TypedBronzePersistenceError(
            f"Unknown typed status for {result.dataset}: {status!r}"
        )
    if status == DatasetStatus.AVAILABLE.value:
        if result.frame is None or result.frame.empty:
            raise TypedBronzePersistenceError(
                f"Available dataset {result.dataset} has no rows"
            )
        return "write"
    return "clear"


class _TrinoManager(Protocol):
    """Small persistence surface used by :class:`FBrefTypedBronzeWriter`."""

    def table_exists(self, schema: str, table: str) -> bool: ...

    def arrow_schema_to_trino(self, arrow_schema: pa.Schema) -> Dict[str, str]: ...

    def create_iceberg_table(
        self,
        schema: str,
        table: str,
        columns: Dict[str, str],
        partition_columns: Optional[Sequence[str]] = None,
    ) -> None: ...

    def get_table_columns(self, schema: str, table: str) -> Dict[str, str]: ...

    def add_column(
        self, schema: str, table: str, column: str, column_type: str
    ) -> None: ...

    def insert_dataframe_atomic(
        self,
        schema: str,
        table: str,
        df: pd.DataFrame,
        batch_size: int = 1000,
        delete_filter: Optional[str] = None,
        staging_id: Optional[str] = None,
    ) -> int: ...


@dataclass(frozen=True)
class TypedSourceContext:
    """Source-native identity plus a non-authoritative legacy projection."""

    source_competition_id: str
    source_season_id: str
    competition_name: Optional[str] = None
    season_label: Optional[str] = None
    compatibility_season: Optional[int] = None

    def __post_init__(self) -> None:
        if not str(self.source_competition_id).strip():
            raise ValueError("source_competition_id must not be blank")
        if not str(self.source_season_id).strip():
            raise ValueError("source_season_id must not be blank")
        if self.compatibility_season is not None:
            value = int(self.compatibility_season)
            if value < 1000 or value > 9999:
                raise ValueError("compatibility_season must be a four-digit year")

    @property
    def league(self) -> str:
        competition_id = str(self.source_competition_id)
        known = COMPATIBILITY_LEAGUE_BY_COMPETITION_ID.get(competition_id)
        if known:
            return known
        if self.competition_name and self.competition_name.strip():
            return self.competition_name.strip()
        return f"FBREF-{competition_id}"

    @property
    def season(self) -> Optional[int]:
        """Return a legacy year only when the source supplied one.

        No second year or URL is synthesized.  Opaque source season IDs remain
        ``None`` unless the registry supplies an explicit compatibility year.
        """

        if self.compatibility_season is not None:
            return int(self.compatibility_season)
        for candidate in (self.season_label, self.source_season_id):
            value = str(candidate or "").strip()
            if _SINGLE_YEAR_RE.fullmatch(value):
                return int(value)
            match = _SPLIT_YEAR_RE.fullmatch(value)
            if match:
                return int(match.group("start"))
        return None


def compatibility_league_alias(
    source_competition_id: str,
    *,
    competition_name: Optional[str] = None,
) -> str:
    """Project a discovered competition into the legacy ``league`` column."""

    return TypedSourceContext(
        source_competition_id=str(source_competition_id),
        source_season_id="compatibility-only",
        competition_name=competition_name,
    ).league


def _html_text(html: str | bytes) -> str:
    if isinstance(html, bytes):
        return html.decode("utf-8", errors="replace")
    if not isinstance(html, str):
        raise TypeError("stored HTML must be str or bytes")
    return html


def _apply_source_context(
    frame: pd.DataFrame, context: TypedSourceContext
) -> pd.DataFrame:
    # Parser frames are newly allocated and have no external owner. Enriching
    # them in place avoids copying every match dataset during offline replay.
    output = frame
    output["league"] = context.league
    # Preserve the existing BIGINT Bronze contract even when an opaque source
    # season has no honest legacy projection.  A plain all-None object column
    # would otherwise create VARCHAR on a fresh table.
    output["season"] = pd.Series(
        [context.season] * len(output),
        index=output.index,
        dtype="Int64",
    )
    output["source_competition_id"] = str(context.source_competition_id)
    output["source_season_id"] = str(context.source_season_id)
    return output


def _table_has_materialized_rows(table) -> bool:
    body = table.find("tbody")
    rows = (body or table).find_all("tr")
    return any(
        "thead" not in set(row.get("class") or [])
        and bool(row.get_text(" ", strip=True))
        for row in rows
    )


def _matching_tables(soup, comment_tables, predicate):
    tables = list(soup.find_all("table")) + list(comment_tables.values())
    return [
        table
        for table in tables
        if predicate(str(table.get("id") or "").casefold())
    ]


def _season_source_tables(
    soup,
    comment_tables,
    *,
    category: str,
    stat_type: str,
):
    source_type = "playing_time" if stat_type == "playingtime" else stat_type
    if category == "team":
        exact = {
            f"stats_squads_{source_type}_for",
            f"stats_squads_{stat_type}_for",
            f"stats_squads_{source_type}",
            f"stats_squads_{stat_type}",
        }
        if stat_type in {"stats", "standard"}:
            exact.add("stats_squads_standard_for")
        return _matching_tables(
            soup,
            comment_tables,
            lambda table_id: table_id in exact
            or (
                "squads" in table_id
                and (stat_type in table_id or source_type in table_id)
            ),
        )
    exact = {
        f"stats_{source_type}",
        f"stats_{stat_type}",
        f"stats_{source_type}_all",
        f"stats_{stat_type}_all",
    }
    if stat_type in {"stats", "standard"}:
        exact.add("stats_standard")
    return _matching_tables(
        soup,
        comment_tables,
        lambda table_id: table_id in exact,
    )


def parse_schedule_html(
    html: str | bytes,
    *,
    context: TypedSourceContext,
) -> DatasetParseResult:
    """Parse one stored schedule page without constructing or fetching a URL."""

    soup = BeautifulSoup(_html_text(html), "html.parser")
    try:
        comment_tables = extract_tables_from_comments(soup)
        frame = find_schedule_table(
            soup,
            comment_tables,
            str(context.source_season_id),
            str(context.source_competition_id),
        )
        if frame is None or frame.empty:
            schedule_tables = _matching_tables(
                soup,
                comment_tables,
                lambda table_id: "sched" in table_id,
            )
            has_schedule_table = bool(schedule_tables)
            has_schedule_rows = any(
                _table_has_materialized_rows(table)
                for table in schedule_tables
            )
            safe_empty = has_schedule_table and not has_schedule_rows
            return DatasetParseResult(
                dataset="schedule",
                status=(
                    DatasetStatus.EMPTY
                    if safe_empty
                    else DatasetStatus.ERROR
                ),
                reason=(
                    "schedule_table_empty"
                    if safe_empty
                    else (
                        "schedule_table_unparsed"
                        if has_schedule_table
                        else "schedule_table_missing"
                    )
                ),
                error_type=(
                    None
                    if safe_empty
                    else (
                        "SchedulePageSchemaDriftError"
                        if has_schedule_table
                        else "SchedulePageContractError"
                    )
                ),
                error_message=(
                    None
                    if safe_empty
                    else (
                        "Schedule table has rows but could not be parsed"
                        if has_schedule_table
                        else "No schedule table in stored HTML"
                    )
                ),
            )
        return DatasetParseResult(
            dataset="schedule",
            status=DatasetStatus.AVAILABLE,
            frame=_apply_source_context(frame, context),
        )
    except Exception as exc:
        return DatasetParseResult(
            dataset="schedule",
            status=DatasetStatus.ERROR,
            reason="parser_exception",
            error_type=type(exc).__name__,
            error_message=str(exc)[:1000],
            exception=exc,
        )
    finally:
        soup.decompose()


def parse_match_html(
    html: str | bytes,
    *,
    match_id: str,
    context: TypedSourceContext,
    enabled_datasets: Optional[set[str]] = None,
    require_player_contract: bool = True,
) -> MatchParseResult:
    """Parse a stored match page and attach both source-native identifiers."""

    if not str(match_id).strip():
        raise ValueError("match_id must not be blank")
    result = parse_existing_match_html(
        _html_text(html),
        match_id=str(match_id),
        league=context.league,
        season=context.season,
        enabled_datasets=enabled_datasets,
        require_player_contract=require_player_contract,
    )
    for dataset in result.datasets.values():
        if dataset.frame is not None and not dataset.frame.empty:
            dataset.frame = _apply_source_context(dataset.frame, context)
    return result


def _season_dataset_name(category: str, stat_type: str) -> str:
    if category == "keeper":
        return f"keeper_{stat_type}"
    return f"{category}_{stat_type}"


def parse_season_stats_html(
    html: str | bytes,
    *,
    context: TypedSourceContext,
    stat_route: str,
) -> Dict[str, DatasetParseResult]:
    """Parse stable typed datasets from one already-discovered season page."""

    normalized_route = str(stat_route).strip().casefold()
    if normalized_route == "stats":
        normalized_route = "standard"
    extracts = SEASON_ROUTE_DATASETS.get(normalized_route)
    if extracts is None:
        raise ValueError(f"Unsupported typed season stat route: {stat_route!r}")

    soup = BeautifulSoup(_html_text(html), "html.parser")
    results: Dict[str, DatasetParseResult] = {}
    try:
        comment_tables = extract_tables_from_comments(soup)
        for category, stat_type in extracts:
            dataset_name = _season_dataset_name(category, stat_type)
            try:
                if category == "team":
                    frame = find_team_stats_table(
                        soup, comment_tables, stat_type
                    )
                else:
                    frame = find_player_stats_table(
                        soup, comment_tables, stat_type
                    )
                if frame is None or frame.empty:
                    source_tables = _season_source_tables(
                        soup,
                        comment_tables,
                        category=category,
                        stat_type=stat_type,
                    )
                    has_rows = any(
                        _table_has_materialized_rows(table)
                        for table in source_tables
                    )
                    if source_tables and not has_rows:
                        results[dataset_name] = DatasetParseResult(
                            dataset=dataset_name,
                            status=DatasetStatus.EMPTY,
                            reason="typed_source_table_empty",
                        )
                    else:
                        results[dataset_name] = DatasetParseResult(
                            dataset=dataset_name,
                            status=(
                                DatasetStatus.ERROR
                                if source_tables
                                else DatasetStatus.NOT_APPLICABLE
                            ),
                            reason=(
                                "typed_source_table_unparsed"
                                if source_tables
                                else "typed_source_table_not_published"
                            ),
                            error_type=(
                                "SeasonPageSchemaDriftError"
                                if source_tables
                                else None
                            ),
                            error_message=(
                                f"Source table for {dataset_name} has rows "
                                "but parser returned none"
                                if source_tables
                                else None
                            ),
                        )
                    continue
                if category in {"player", "keeper"} and "Player" in frame:
                    frame = frame.copy()
                    frame["Player"] = frame["Player"].astype(str).str.replace(
                        r"^\d+\s*", "", regex=True
                    )
                frame["stat_type"] = stat_type
                results[dataset_name] = DatasetParseResult(
                    dataset=dataset_name,
                    status=DatasetStatus.AVAILABLE,
                    frame=_apply_source_context(frame, context),
                )
            except Exception as exc:
                results[dataset_name] = DatasetParseResult(
                    dataset=dataset_name,
                    status=DatasetStatus.ERROR,
                    reason="parser_exception",
                    error_type=type(exc).__name__,
                    error_message=str(exc)[:1000],
                    exception=exc,
                )
        return results
    finally:
        soup.decompose()


def _sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _source_partition_filter(context: TypedSourceContext) -> str:
    native = (
        f"source_competition_id = {_sql_string(context.source_competition_id)} "
        f"AND source_season_id = {_sql_string(context.source_season_id)}"
    )
    if context.season is None:
        return native
    legacy = (
        "source_competition_id IS NULL AND source_season_id IS NULL "
        f"AND league = {_sql_string(context.league)} "
        f"AND season = {int(context.season)}"
    )
    return f"(({native}) OR ({legacy}))"


def _staging_identity(*parts: object) -> str:
    seed = "\x1f".join(str(part) for part in parts)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    # UUID suffix prevents concurrent retries/shards from sharing a stage.
    return f"fbref_{digest}_{uuid.uuid4().hex[:12]}"


class FBrefTypedBronzeWriter:
    """Write existing typed tables with source-aware replace semantics."""

    def __init__(
        self,
        manager: Optional[_TrinoManager] = None,
        *,
        schema: str = "bronze",
    ) -> None:
        if manager is None:
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
        self.manager = manager
        self.schema = schema

    def _ensure_table(self, table: str, frame: pd.DataFrame) -> None:
        arrow_schema = pa.Table.from_pandas(
            frame, preserve_index=False
        ).schema
        columns = self.manager.arrow_schema_to_trino(arrow_schema)
        if not self.manager.table_exists(self.schema, table):
            partitions = [
                column
                for column in ("league", "season")
                if column in columns
            ]
            self.manager.create_iceberg_table(
                self.schema,
                table,
                columns,
                partition_columns=partitions or None,
            )
            return

        existing = {
            name.casefold()
            for name in self.manager.get_table_columns(
                self.schema, table
            )
        }
        for name, column_type in columns.items():
            if name.casefold() not in existing:
                self.manager.add_column(
                    self.schema, table, name, column_type
                )

    @staticmethod
    def _decorate(
        frame: pd.DataFrame,
        *,
        dataset: str,
        context: TypedSourceContext,
        run_id: str,
        ingested_at: datetime,
    ) -> pd.DataFrame:
        output = _apply_source_context(frame, context)
        output["_source"] = "fbref"
        output["_entity_type"] = dataset
        output["_ingested_at"] = ingested_at
        output["_batch_id"] = str(run_id)
        return output

    def _persist_frame(
        self,
        *,
        dataset: str,
        table: str,
        frame: pd.DataFrame,
        context: TypedSourceContext,
        run_id: str,
        target_identity: str,
        delete_filter: str,
        ingested_at: datetime,
    ) -> int:
        decorated = self._decorate(
            frame,
            dataset=dataset,
            context=context,
            run_id=run_id,
            ingested_at=ingested_at,
        )
        self._ensure_table(table, decorated)
        staging_id = _staging_identity(
            run_id, target_identity, dataset, table
        )
        inserted = self.manager.insert_dataframe_atomic(
            self.schema,
            table,
            decorated,
            delete_filter=delete_filter,
            staging_id=staging_id,
        )
        if inserted != len(decorated):
            raise TypedBronzePersistenceError(
                f"Row count mismatch for {table}: "
                f"inserted={inserted}, expected={len(decorated)}"
            )
        return inserted

    def _persist_empty(
        self,
        *,
        table: str,
        delete_filter: str,
        source_partition: bool = False,
    ) -> int:
        # A never-created typed table is already the desired empty state. Do
        # not invent a schema from a zero-row frame merely to record success.
        if not self.manager.table_exists(self.schema, table):
            return 0
        if source_partition:
            # Legacy typed tables predate source-native identity. The empty
            # replacement filter uses these columns, so migrate them before
            # issuing its DELETE just as the non-empty path does in
            # ``_ensure_table``.
            existing = {
                name.casefold()
                for name in self.manager.get_table_columns(
                    self.schema, table
                )
            }
            for column in (
                "source_competition_id",
                "source_season_id",
            ):
                if column.casefold() not in existing:
                    self.manager.add_column(
                        self.schema, table, column, "VARCHAR"
                    )
        deleted = self.manager.insert_dataframe_atomic(
            self.schema,
            table,
            pd.DataFrame(),
            delete_filter=delete_filter,
        )
        if deleted != 0:
            raise TypedBronzePersistenceError(
                f"Empty replacement for {table} returned {deleted} rows"
            )
        return 0

    def persist_schedule(
        self,
        parsed: DatasetParseResult,
        *,
        context: TypedSourceContext,
        run_id: str,
        target_identity: str,
    ) -> Dict[str, int]:
        action = _persistence_action(parsed)
        if action == "skip":
            return {}
        delete_filter = _source_partition_filter(context)
        if action == "clear":
            return {
                "schedule": self._persist_empty(
                    table="fbref_schedule",
                    delete_filter=delete_filter,
                    source_partition=True,
                )
            }
        assert parsed.frame is not None
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        count = self._persist_frame(
            dataset="schedule",
            table="fbref_schedule",
            frame=parsed.frame,
            context=context,
            run_id=run_id,
            target_identity=target_identity,
            delete_filter=delete_filter,
            ingested_at=ingested_at,
        )
        return {"schedule": count}

    def persist_match(
        self,
        parsed: MatchParseResult,
        *,
        match_id: str,
        context: TypedSourceContext,
        run_id: str,
        target_identity: str,
    ) -> Dict[str, int]:
        errors = [
            name
            for name, dataset in parsed.datasets.items()
            if str(getattr(dataset.status, "value", dataset.status)).casefold()
            == DatasetStatus.ERROR.value
        ]
        if parsed.has_errors or errors:
            raise MatchPageParseError(
                f"Refusing partial typed persistence; parser errors: {errors}"
            )

        actions: Dict[str, str] = {}
        for name, result in parsed.datasets.items():
            action = _persistence_action(result)
            if action == "skip":
                continue
            if name not in MATCH_DATASET_TABLES:
                raise TypedBronzePersistenceError(
                    f"No typed Bronze table mapping for {name}"
                )
            actions[name] = action

        ordered = [
            dataset
            for dataset in MATCH_DATASET_TABLES
            if dataset in actions
        ]

        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        counts: Dict[str, int] = {}
        for dataset in ordered:
            table = MATCH_DATASET_TABLES[dataset]
            delete_filter = f"match_id = {_sql_string(match_id)}"
            if actions[dataset] == "clear":
                counts[dataset] = self._persist_empty(
                    table=table,
                    delete_filter=delete_filter,
                )
            else:
                frame = parsed.datasets[dataset].frame
                assert frame is not None
                counts[dataset] = self._persist_frame(
                    dataset=dataset,
                    table=table,
                    frame=frame,
                    context=context,
                    run_id=run_id,
                    target_identity=target_identity,
                    delete_filter=delete_filter,
                    ingested_at=ingested_at,
                )

        # Independent typed completion evidence is always committed last.
        # A page that legitimately lacks match_player_stats can therefore
        # complete without deleting that table. Partial remediation updates
        # only the requested dataset keys and cannot overwrite availability
        # recorded by the full-page parser.
        availability_rows = [
            {
                "match_id": str(match_id),
                "dataset": name,
                "availability": str(
                    getattr(result.status, "value", result.status)
                ).casefold(),
                "reason": result.reason,
            }
            for name, result in parsed.datasets.items()
            if str(result.reason or "").casefold()
            not in _NOT_REQUESTED_REASONS
        ]
        if not availability_rows:
            raise TypedBronzePersistenceError(
                "Match parser produced no requested dataset availability"
            )
        availability = pd.DataFrame(availability_rows)
        dataset_filter = ", ".join(
            _sql_string(row["dataset"])
            for row in availability_rows
        )
        self._persist_frame(
            dataset="dataset_availability",
            table=MATCH_AVAILABILITY_TABLE,
            frame=availability,
            context=context,
            run_id=run_id,
            target_identity=target_identity,
            delete_filter=(
                f"match_id = {_sql_string(match_id)} "
                f"AND dataset IN ({dataset_filter})"
            ),
            ingested_at=ingested_at,
        )
        return counts

    def persist_season_stats(
        self,
        parsed: Mapping[str, DatasetParseResult],
        *,
        context: TypedSourceContext,
        run_id: str,
        target_identity: str,
    ) -> Dict[str, int]:
        parser_errors = [
            name
            for name, result in parsed.items()
            if str(getattr(result.status, "value", result.status)).casefold()
            == DatasetStatus.ERROR.value
        ]
        if parser_errors:
            raise TypedBronzePersistenceError(
                f"Refusing season persistence; parser errors: {parser_errors}"
            )
        actions: Dict[str, str] = {}
        for dataset, result in parsed.items():
            action = _persistence_action(result)
            if action == "skip":
                continue
            if dataset not in SEASON_DATASET_TABLES:
                raise TypedBronzePersistenceError(
                    f"No typed Bronze table mapping for {dataset}"
                )
            actions[dataset] = action

        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        counts: Dict[str, int] = {}
        for dataset, result in parsed.items():
            action = actions.get(dataset)
            if action is None:
                continue
            table = SEASON_DATASET_TABLES[dataset]
            delete_filter = _source_partition_filter(context)
            if action == "clear":
                counts[dataset] = self._persist_empty(
                    table=table,
                    delete_filter=delete_filter,
                    source_partition=True,
                )
            else:
                assert result.frame is not None
                counts[dataset] = self._persist_frame(
                    dataset=dataset,
                    table=table,
                    frame=result.frame,
                    context=context,
                    run_id=run_id,
                    target_identity=target_identity,
                    delete_filter=delete_filter,
                    ingested_at=ingested_at,
                )
        return counts


class FBrefTypedBronzeAdapter:
    """Convenience facade for parse-from-raw then typed Bronze persistence."""

    def __init__(
        self, writer: Optional[FBrefTypedBronzeWriter] = None
    ) -> None:
        self.writer = writer or FBrefTypedBronzeWriter()

    def ingest_schedule_html(
        self,
        html: str | bytes,
        *,
        context: TypedSourceContext,
        run_id: str,
        target_identity: str,
    ) -> tuple[DatasetParseResult, Dict[str, int]]:
        parsed = parse_schedule_html(html, context=context)
        counts = self.writer.persist_schedule(
            parsed,
            context=context,
            run_id=run_id,
            target_identity=target_identity,
        )
        return parsed, counts

    def ingest_match_html(
        self,
        html: str | bytes,
        *,
        match_id: str,
        context: TypedSourceContext,
        run_id: str,
        target_identity: str,
        enabled_datasets: Optional[set[str]] = None,
        require_player_contract: bool = True,
    ) -> tuple[MatchParseResult, Dict[str, int]]:
        parsed = parse_match_html(
            html,
            match_id=match_id,
            context=context,
            enabled_datasets=enabled_datasets,
            require_player_contract=require_player_contract,
        )
        counts = self.writer.persist_match(
            parsed,
            match_id=match_id,
            context=context,
            run_id=run_id,
            target_identity=target_identity,
        )
        return parsed, counts

    def ingest_season_stats_html(
        self,
        html: str | bytes,
        *,
        context: TypedSourceContext,
        stat_route: str,
        run_id: str,
        target_identity: str,
    ) -> tuple[Dict[str, DatasetParseResult], Dict[str, int]]:
        parsed = parse_season_stats_html(
            html, context=context, stat_route=stat_route
        )
        counts = self.writer.persist_season_stats(
            parsed,
            context=context,
            run_id=run_id,
            target_identity=target_identity,
        )
        return parsed, counts


__all__ = [
    "COMPATIBILITY_LEAGUE_BY_COMPETITION_ID",
    "FBrefTypedBronzeAdapter",
    "FBrefTypedBronzeWriter",
    "MATCH_COMPLETION_DATASET",
    "MATCH_AVAILABILITY_TABLE",
    "MATCH_DATASET_TABLES",
    "SEASON_DATASET_TABLES",
    "SEASON_ROUTE_DATASETS",
    "TYPED_BRONZE_PARSER_VERSION",
    "TypedBronzeError",
    "TypedBronzePersistenceError",
    "TypedSourceContext",
    "compatibility_league_alias",
    "parse_match_html",
    "parse_schedule_html",
    "parse_season_stats_html",
]
