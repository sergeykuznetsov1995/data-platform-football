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
)
from .client import UnderstatClient
from .contracts import FORECAST_COLUMNS
from .service import UnderstatSource


logger = logging.getLogger(__name__)


class UnderstatScraper(BaseScraper):
    """Native Understat scraper for scope-aware production orchestration.

    :meth:`scrape_scope` guarantees that every match payload is fetched once
    and parsed into both derived tables.
    """

    SOURCE_NAME = "understat"
    DEFAULT_RATE_LIMIT = 30
    SUPPORTED_LEAGUES = list(PRODUCTION_LEAGUES)

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

    def league_snapshot(
        self, league: str, season_slug: str, source_season_id: int
    ) -> dict[str, str]:
        """#1431: one-request league fingerprints for the weekly closed check."""

        return self.source.league_snapshot(league, season_slug, source_season_id)

    def confirm_cache(self) -> None:
        self.source.confirm_cache()

    def note_quality(self, league: str, source_season_id: int, report: Any, *,
                     publication_ready: bool = True) -> None:
        """Keep pending/failed scopes daily until the existing DQ gates heal."""
        self.source.catalog.note_quality(
            league, source_season_id, report, publication_ready=publication_ready
        )

    @property
    def last_league_hashes(self) -> dict[str, str]:
        return dict(self.source.last_league_hashes)

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

        # An all-NULL forecast otherwise becomes Arrow null and Trino VARCHAR.
        # Do not coerce invalid numbers to NULL or alter unrelated source types.
        if table_name == "understat_schedule" and any(
            column in df for column in FORECAST_COLUMNS
        ):
            df = df.copy()
            for column in FORECAST_COLUMNS:
                if column in df:
                    values = pd.to_numeric(df[column], errors="raise").astype("Float64")
                    if (df[column].notna() & values.isna()).any():
                        raise ValueError(f"Invalid numeric value in {column}")
                    df[column] = values

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
