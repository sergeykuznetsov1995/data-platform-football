"""Public facade tests for the source-native Understat scraper."""

import ast
from pathlib import Path
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


def test_scope_discovery_is_forwarded(scraper):
    facade, _source = scraper
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


def test_legacy_writer_is_disabled(scraper):
    facade, _source = scraper
    with pytest.raises(RuntimeError, match="scope-aware"):
        facade.scrape_all()


@pytest.mark.parametrize(
    "filename, removed",
    [
        (
            "scraper.py",
            {
                "TABLE_SPECS", "_scope_cache", "_configured_scopes", "_read_table",
                "_filter_match", "read_schedule", "read_shot_events",
                "read_player_season_stats", "read_team_match_stats",
                "read_player_match_stats", "read_player_team_season_stats",
                "read_team_season_breakdowns",
            },
        ),
        ("contracts.py", {"reader_method"}),
        ("manifest.py", {"is_published", "next_incomplete"}),
    ],
)
def test_retired_api_has_no_definitions_or_references(filename, removed):
    source = Path(__file__).resolve().parents[3] / "scrapers" / "understat" / filename
    tree = ast.parse(source.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.keyword):
            names.add(node.arg)
    assert not names & removed


@pytest.mark.parametrize("values", [[None, None], [0, 1, None], [0.125, 0.75, None]])
def test_forecast_writer_types_survive_real_arrow_preparation(scraper, values):
    from scrapers.base.iceberg_writer import IcebergWriter
    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.understat.contracts import FORECAST_COLUMNS

    facade, _ = scraper
    writer = facade._iceberg_writer
    writer._add_metadata_columns.side_effect = lambda df, source, batch_id: df.assign(_batch_id=batch_id)
    frame = pd.DataFrame({column: values for column in FORECAST_COLUMNS}).convert_dtypes()
    facade.save_to_iceberg(frame, "understat_schedule", batch_id="batch")
    prepared = writer.write_dataframe.call_args.kwargs["df"]
    arrow = IcebergWriter()._pandas_to_arrow(prepared)
    types = TrinoTableManager.__new__(TrinoTableManager).arrow_schema_to_trino(arrow.schema)
    for column in FORECAST_COLUMNS:
        assert types[column] == "DOUBLE"
        assert str(arrow.schema.field(column).type) == "double"
        assert str(prepared[column].dtype) == "Float64"
        assert arrow[column].to_pylist() == values
    assert prepared["_batch_id"].tolist() == ["batch"] * len(values)


@pytest.mark.parametrize("invalid", ["invalid", ""])
def test_forecast_writer_rejects_invalid_numbers_before_write(scraper, invalid):
    facade, _ = scraper
    with pytest.raises(ValueError):
        facade.save_to_iceberg(
            pd.DataFrame({"forecast_draw": [invalid]}),
            "understat_schedule", batch_id="batch",
        )
    facade._iceberg_writer.write_dataframe.assert_not_called()


def test_contract_declares_all_columns_without_changing_batch_versions():
    for contract in TABLE_CONTRACTS:
        assert set(contract.column_types) == set(contract.required_columns)
        assert contract.column_types["league_id"] == "varchar"
        assert contract.column_types["season"] == "varchar"
    assert TABLE_CONTRACTS[0].column_types["forecast_draw"] == "double"
