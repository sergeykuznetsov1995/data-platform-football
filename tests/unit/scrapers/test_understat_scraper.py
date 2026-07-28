"""Public facade tests for the source-native Understat scraper."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scrapers.understat import PRODUCTION_LEAGUES, TABLE_CONTRACTS, UnderstatScraper


@pytest.fixture
def base_dependencies():
    with patch("scrapers.base.base_scraper.get_rate_limiter") as rate_limiter, patch(
        "scrapers.base.base_scraper.get_retry_policy"
    ) as retry_policy, patch(
        "scrapers.base.base_scraper.get_circuit_breaker"
    ) as circuit_breaker, patch(
        "scrapers.base.base_scraper.IcebergWriter"
    ):
        rate_limiter.return_value = MagicMock()
        retry_policy.return_value = MagicMock()
        circuit_breaker.return_value = MagicMock()
        yield


def _contract_frames():
    frames = {}
    for contract in TABLE_CONTRACTS:
        row = {column: None for column in contract.required_columns}
        row.update(
            {
                "league": "ENG-Premier League",
                "season": "2526",
                "source_season_id": 2025,
                "game_id": 100,
                "has_data": True,
            }
        )
        frames[contract.table_name] = pd.DataFrame([row]).convert_dtypes()
    return frames


class _FakeSource:
    def __init__(self):
        self.calls = []
        self.client = SimpleNamespace(session=SimpleNamespace(close=lambda: None))
        self.catalog = SimpleNamespace(
            discover_scopes=lambda **kwargs: ("discovered",),
            rolling_scopes=lambda **kwargs: ("rolling",),
        )

    def scrape_scope(self, league, season_slug, source_season_id, **kwargs):
        self.calls.append((league, season_slug, source_season_id, kwargs))
        return {name: frame.copy() for name, frame in _contract_frames().items()}


@pytest.fixture
def scraper(base_dependencies):
    source = _FakeSource()
    scraper = UnderstatScraper(
        leagues=["ENG-Premier League"], seasons=["2526"], source=source
    )
    return scraper, source


def test_supported_leagues_include_all_six_and_invalid_selection_fails(base_dependencies):
    assert tuple(UnderstatScraper.SUPPORTED_LEAGUES) == PRODUCTION_LEAGUES
    assert "RUS-Premier League" in UnderstatScraper.SUPPORTED_LEAGUES
    with pytest.raises(ValueError, match="No supported Understat leagues"):
        UnderstatScraper(leagues=["USA-MLS"], seasons=["2526"], source=_FakeSource())


def test_scrape_scope_preserves_explicit_canonical_signature(scraper):
    facade, source = scraper
    frames = facade.scrape_scope(
        "ENG-Premier League", "2526", 2025, mode="history", force_refresh=True
    )
    assert tuple(frames) == tuple(contract.table_name for contract in TABLE_CONTRACTS)
    assert source.calls == [
        (
            "ENG-Premier League",
            "2526",
            2025,
            {"mode": "history", "force_refresh": True},
        )
    ]


def test_read_wrappers_share_one_seven_table_scope_fetch(scraper):
    facade, source = scraper
    frames = [getattr(facade, contract.reader_method)() for contract in TABLE_CONTRACTS]
    assert len(source.calls) == 1
    assert all(frame is not None and len(frame) == 1 for frame in frames)
    assert all("_batch_id" in frame.columns for frame in frames)
    assert facade.discover_scopes() == ("discovered",)
    assert facade.rolling_scopes() == ("rolling",)


def test_scope_writer_stamps_shared_batch_without_extending_shared_writer_api(scraper):
    facade, _source = scraper
    writer = facade._iceberg_writer
    frame = pd.DataFrame(
        {
            "league": ["ENG-Premier League"],
            "season": ["2526"],
            "_batch_id": ["scope-batch-1"],
        }
    )

    def add_metadata(value, source, batch_id=None):
        return value.assign(
            _source=source,
            _ingested_at=pd.Timestamp("2026-07-27T12:00:00Z"),
            _batch_id=batch_id,
        )

    writer._add_metadata_columns.side_effect = add_metadata
    writer.write_dataframe.return_value = "iceberg.bronze.understat_schedule"

    result = facade.save_to_iceberg(
        frame,
        "understat_schedule",
        partition_cols=["league", "season"],
        replace_partitions=["league", "season"],
        batch_id="scope-batch-1",
    )

    assert result == "iceberg.bronze.understat_schedule"
    metadata_call = writer._add_metadata_columns.call_args
    assert metadata_call.args == (frame, "understat")
    assert metadata_call.kwargs == {"batch_id": "scope-batch-1"}
    write_kwargs = writer.write_dataframe.call_args.kwargs
    assert write_kwargs["add_metadata"] is False
    assert "batch_id" not in write_kwargs
    assert write_kwargs["df"]["_batch_id"].tolist() == ["scope-batch-1"]
    assert write_kwargs["partition_spec"] == [
        ("league", "identity"),
        ("season", "identity"),
    ]
    assert write_kwargs["delete_filter"] == (
        "(league = 'ENG-Premier League' AND season = '2526')"
    )


def test_scope_writer_requires_runner_owned_batch_id(scraper):
    facade, _source = scraper
    with pytest.raises(ValueError, match="requires a non-empty batch_id"):
        facade.save_to_iceberg(
            pd.DataFrame({"league": ["ENG-Premier League"], "season": ["2526"]}),
            "understat_schedule",
        )


def test_table_specs_come_from_registry_and_legacy_writer_is_disabled(scraper):
    facade, _source = scraper
    assert facade.TABLE_SPECS == [
        (contract.reader_method, contract.table_name, contract.result_key)
        for contract in TABLE_CONTRACTS
    ]
    with pytest.raises(RuntimeError, match="scope-aware"):
        facade.scrape_all()
