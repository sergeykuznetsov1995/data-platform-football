"""Production facade for the source-native Understat ingestion."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

from scrapers.base.base_scraper import BaseScraper

from .catalog import (
    LEAGUE_BY_CANONICAL,
    PRODUCTION_LEAGUES,
    UnderstatScope,
    source_season_id_from_slug,
    season_slug,
)
from .client import UnderstatClient
from .contracts import TABLE_CONTRACTS
from .service import UnderstatSource


logger = logging.getLogger(__name__)


class UnderstatScraper(BaseScraper):
    """Native Understat scraper with legacy ``read_*`` compatibility methods.

    Production orchestration should call :meth:`scrape_scope`, which guarantees
    that every match payload is fetched once and parsed into both derived
    tables. The individual readers share a scope cache and exist for ad-hoc
    callers which used the old soccerdata-backed API.
    """

    SOURCE_NAME = "understat"
    DEFAULT_RATE_LIMIT = 30
    SUPPORTED_LEAGUES = list(PRODUCTION_LEAGUES)
    TABLE_SPECS = [
        (contract.reader_method, contract.table_name, contract.result_key)
        for contract in TABLE_CONTRACTS
    ]

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int | str]] = None,
        *,
        client: Optional[UnderstatClient] = None,
        source: Optional[UnderstatSource] = None,
        session: Any = None,
        cache_dir: Optional[str | Path] = None,
        requests_per_minute: Optional[int] = None,
        client_sleep: Any = None,
        client_monotonic: Any = None,
        client_now: Any = None,
        client_jitter: Any = None,
        **kwargs: Any,
    ):
        selected = list(leagues or PRODUCTION_LEAGUES)
        unsupported = [league for league in selected if league not in LEAGUE_BY_CANONICAL]
        if unsupported:
            logger.warning("Dropping leagues not covered by Understat: %s", unsupported)
            selected = [league for league in selected if league in LEAGUE_BY_CANONICAL]
        if not selected:
            raise ValueError(
                "No supported Understat leagues left after filtering; "
                f"supported: {list(PRODUCTION_LEAGUES)}"
            )

        base_rate_limit = kwargs.pop("rate_limit", None)
        super().__init__(
            leagues=selected,
            seasons=list(seasons or []),
            rate_limit=base_rate_limit,
            **kwargs,
        )
        if source is not None and client is not None:
            raise ValueError("Pass either source or client, not both")
        if source is None:
            if client is None:
                native_cache = cache_dir or os.getenv(
                    "UNDERSTAT_CACHE_DIR",
                    # The platform's persistent scraper-cache volume is still
                    # mounted at ~/soccerdata; this subdirectory is owned by
                    # the native client and imports no soccerdata runtime.
                    str(Path.home() / "soccerdata" / "data" / "UnderstatNative"),
                )
                client_kwargs: dict[str, Any] = {
                    "session": session,
                    "cache_dir": native_cache,
                    "requests_per_minute": (
                        requests_per_minute or base_rate_limit or self.DEFAULT_RATE_LIMIT
                    ),
                }
                optional = {
                    "sleep": client_sleep,
                    "monotonic": client_monotonic,
                    "now": client_now,
                    "jitter": client_jitter,
                }
                client_kwargs.update({key: value for key, value in optional.items() if value})
                client = UnderstatClient(**client_kwargs)
            source = UnderstatSource(client)
        self.source = source
        self.client = source.client
        self._scope_cache: dict[tuple[str, str, int], dict[str, pd.DataFrame]] = {}

    def discover_scopes(self, *, force_refresh: bool = True) -> tuple[UnderstatScope, ...]:
        return self.source.catalog.discover_scopes(force_refresh=force_refresh)

    def rolling_scopes(
        self, *, window: int = 2, probe_next: bool = True, force_refresh: bool = True
    ) -> tuple[UnderstatScope, ...]:
        return self.source.catalog.rolling_scopes(
            window=window,
            probe_next=probe_next,
            force_refresh=force_refresh,
        )

    def scrape_scope(
        self,
        league: str,
        season_slug: str,
        source_season_id: int,
        *,
        mode: str = "current",
        force_refresh: bool = False,
    ) -> dict[str, pd.DataFrame]:
        """Return all seven source tables without writer-owned metadata."""

        return self.source.scrape_scope(
            league,
            season_slug,
            source_season_id,
            mode=mode,
            force_refresh=force_refresh,
        )

    def save_to_iceberg(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        database: str = "bronze",
        replace_partitions: Optional[List[str]] = None,
        min_replace_ratio: Optional[float] = None,
        replace_guard_key: Optional[str] = None,
        natural_keys: Optional[List[str]] = None,
        batch_id: Optional[str] = None,
    ) -> str:
        """Write one manifest-fenced entity without changing shared writer APIs.

        The Understat runner owns one batch id for all seven tables.  Stamp the
        writer metadata before calling the stable ``IcebergWriter`` interface
        with ``add_metadata=False``; otherwise its per-call metadata helper
        would generate seven unrelated batch ids.  Keeping this adaptation in
        the source package also lets an Understat-only hot deploy coexist with
        an attested scheduler image whose shared writer code is immutable.
        """

        normalized_batch_id = str(batch_id or "").strip()
        if not normalized_batch_id:
            raise ValueError("Understat publication requires a non-empty batch_id")
        if df.empty:
            raise ValueError(f"Understat publication cannot write empty {table_name}")
        if natural_keys and replace_partitions:
            raise ValueError(
                "natural_keys and replace_partitions are mutually exclusive"
            )

        observed_batch_ids = set(df.get("_batch_id", pd.Series(dtype="object")).dropna())
        if observed_batch_ids and observed_batch_ids != {normalized_batch_id}:
            raise ValueError(
                f"{table_name}: frame batch ids do not match {normalized_batch_id}"
            )

        partition_spec = (
            [(column, "identity") for column in partition_cols]
            if partition_cols
            else None
        )
        delete_filter = (
            self._build_partition_delete_filter(df, replace_partitions)
            if replace_partitions
            else None
        )
        if min_replace_ratio is not None:
            self._enforce_replace_guard(
                df,
                database,
                table_name,
                delete_filter,
                min_replace_ratio,
                replace_guard_key,
            )

        prepared = self._iceberg_writer._add_metadata_columns(
            df,
            self.SOURCE_NAME,
            batch_id=normalized_batch_id,
        )
        table_path = self._iceberg_writer.write_dataframe(
            df=prepared,
            database=database,
            table=table_name,
            partition_spec=partition_spec,
            add_metadata=False,
            source=self.SOURCE_NAME,
            delete_filter=delete_filter,
            merge_keys=natural_keys,
        )
        self._stats["tables_written"].append(table_path)
        logger.info("Saved %d rows to %s", len(df), table_path)
        return table_path

    def _configured_scopes(self) -> list[tuple[str, str, int]]:
        if not self.seasons:
            raise ValueError("At least one explicit canonical season is required")
        result: list[tuple[str, str, int]] = []
        for league in self.leagues:
            for raw in self.seasons:
                if isinstance(raw, bool):
                    raise ValueError("Boolean is not a valid Understat season")
                if isinstance(raw, int):
                    source_year = raw
                    slug = season_slug(source_year)
                elif isinstance(raw, str):
                    source_year = source_season_id_from_slug(raw)
                    slug = raw
                else:
                    raise ValueError(f"Unsupported season value: {raw!r}")
                result.append((league, slug, source_year))
        return result

    def _read_table(self, table_name: str) -> Optional[pd.DataFrame]:
        frames: list[pd.DataFrame] = []
        for league, slug, source_year in self._configured_scopes():
            key = (league, slug, source_year)
            if key not in self._scope_cache:
                self._scope_cache[key] = self.scrape_scope(
                    league,
                    slug,
                    source_year,
                    mode="current",
                )
            frame = self._scope_cache[key][table_name]
            if not frame.empty:
                frames.append(frame)
        if not frames:
            return None
        result = pd.concat(frames, ignore_index=True).convert_dtypes()
        contract = next(item for item in TABLE_CONTRACTS if item.table_name == table_name)
        return self._add_metadata(result, contract.result_key)

    def read_schedule(
        self, include_matches_without_data: bool = True, force_cache: bool = False
    ) -> Optional[pd.DataFrame]:
        frame = self._read_table("understat_schedule")
        if frame is not None and not include_matches_without_data:
            frame = frame[frame["has_data"].fillna(False).astype(bool)]
        return frame

    def read_shot_events(self, match_id: Any = None) -> Optional[pd.DataFrame]:
        return self._filter_match(self._read_table("understat_shots"), match_id)

    def read_player_season_stats(self, force_cache: bool = False) -> Optional[pd.DataFrame]:
        return self._read_table("understat_players")

    def read_team_match_stats(self, force_cache: bool = False) -> Optional[pd.DataFrame]:
        return self._read_table("understat_team_match_stats")

    def read_player_match_stats(self, match_id: Any = None) -> Optional[pd.DataFrame]:
        return self._filter_match(
            self._read_table("understat_player_match_stats"), match_id
        )

    def read_player_team_season_stats(self) -> Optional[pd.DataFrame]:
        return self._read_table("understat_player_team_season_stats")

    def read_team_season_breakdowns(self) -> Optional[pd.DataFrame]:
        return self._read_table("understat_team_season_breakdowns")

    @staticmethod
    def _filter_match(frame: Optional[pd.DataFrame], match_id: Any) -> Optional[pd.DataFrame]:
        if frame is None or match_id is None:
            return frame
        match_ids = [match_id] if isinstance(match_id, (int, str)) else list(match_id)
        selected = frame[frame["game_id"].isin([int(value) for value in match_ids])]
        if selected.empty:
            raise ValueError("No matches found with the given IDs in selected scopes")
        return selected

    def scrape_all(
        self, min_replace_ratio: Optional[float] = 0.9
    ) -> dict[str, str]:
        """Reject the legacy unfenced multi-table write path.

        Native publication requires one runner-owned batch id, a pre-write
        manifest marker, scope DQ, and a seven-table physical fence.  Keeping
        the abstract method fail-loud preserves the BaseScraper interface
        without leaving an operational bypass around those guarantees.
        """

        del min_replace_ratio
        raise RuntimeError(
            "Understat scrape_all() is disabled: use the scope-aware "
            "dags/scripts/run_understat_scraper.py runner"
        )

    def close(self) -> None:
        close = getattr(self.client.session, "close", None)
        if callable(close):
            close()
        super().close()


__all__ = ["UnderstatScraper"]
