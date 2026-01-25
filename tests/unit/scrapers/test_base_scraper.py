"""
Tests for BaseScraper and related classes.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime


class TestScraperConfig:
    """Tests for ScraperConfig."""

    def test_init_empty(self):
        from scrapers.base.base_scraper import ScraperConfig
        config = ScraperConfig()
        assert config.config == {}

    def test_get_nested_key(self):
        from scrapers.base.base_scraper import ScraperConfig
        config = ScraperConfig()
        config.config = {
            'level1': {
                'level2': {
                    'value': 42
                }
            }
        }

        assert config.get('level1.level2.value') == 42
        assert config.get('level1.level2.missing', 'default') == 'default'

    def test_get_simple_key(self):
        from scrapers.base.base_scraper import ScraperConfig
        config = ScraperConfig()
        config.config = {'key': 'value'}

        assert config.get('key') == 'value'


class TestBaseScraper:
    """Tests for BaseScraper abstract class."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all scraper dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw:

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True

            mock_rp.return_value = MagicMock()
            mock_rp.return_value.execute.side_effect = lambda f, *a, **k: f(*a, **k)

            mock_cb.return_value = MagicMock()
            mock_cb.return_value.call.side_effect = lambda f, *a, **k: f(*a, **k)
            mock_cb.return_value.state = 'closed'

            mock_iw.return_value = MagicMock()
            mock_iw.return_value.write_dataframe.return_value = 'iceberg.bronze.test'

            yield {
                'rate_limiter': mock_rl,
                'retry_policy': mock_rp,
                'circuit_breaker': mock_cb,
                'iceberg_writer': mock_iw,
            }

    @pytest.fixture
    def concrete_scraper(self, mock_dependencies):
        """Create a concrete scraper for testing."""
        from scrapers.base.base_scraper import BaseScraper

        class ConcreteScraper(BaseScraper):
            SOURCE_NAME = 'test'

            def scrape_all(self):
                return {'test': 'iceberg.bronze.test'}

        return ConcreteScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024]
        )

    def test_init(self, concrete_scraper):
        """Test scraper initialization."""
        assert concrete_scraper.leagues == ['ENG-Premier League']
        assert concrete_scraper.seasons == [2024]
        assert concrete_scraper._batch_id is not None

    def test_add_metadata(self, concrete_scraper):
        """Test adding metadata to DataFrame."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        result = concrete_scraper._add_metadata(df, 'test_entity')

        assert '_source' in result.columns
        assert '_entity_type' in result.columns
        assert '_ingested_at' in result.columns
        assert '_batch_id' in result.columns
        assert result['_source'].iloc[0] == 'test'
        assert result['_entity_type'].iloc[0] == 'test_entity'

    def test_add_metadata_empty_df(self, concrete_scraper):
        """Test adding metadata to empty DataFrame."""
        df = pd.DataFrame()
        result = concrete_scraper._add_metadata(df, 'test_entity')
        assert result.empty

    def test_save_to_iceberg(self, concrete_scraper, mock_dependencies):
        """Test saving to Iceberg."""
        df = pd.DataFrame({
            'league': ['ENG-Premier League'],
            'season': [2024],
            'value': [100],
        })

        result = concrete_scraper.save_to_iceberg(
            df, 'test_table', partition_cols=['league', 'season']
        )

        assert result == 'iceberg.bronze.test'
        assert 'iceberg.bronze.test' in concrete_scraper._stats['tables_written']

    def test_save_to_iceberg_empty(self, concrete_scraper):
        """Test saving empty DataFrame."""
        df = pd.DataFrame()

        result = concrete_scraper.save_to_iceberg(df, 'test_table')

        assert 'iceberg.bronze.test_table' in result
        assert len(concrete_scraper._stats['tables_written']) == 0

    def test_get_stats(self, concrete_scraper, mock_dependencies):
        """Test getting scraper statistics."""
        stats = concrete_scraper.get_stats()

        assert 'requests' in stats
        assert 'successes' in stats
        assert 'failures' in stats
        assert 'tables_written' in stats
        assert 'circuit_breaker_state' in stats

    def test_reset_stats(self, concrete_scraper):
        """Test resetting statistics."""
        old_batch_id = concrete_scraper._batch_id
        concrete_scraper._stats['requests'] = 10

        concrete_scraper.reset_stats()

        assert concrete_scraper._stats['requests'] == 0
        assert concrete_scraper._batch_id != old_batch_id

    def test_context_manager(self, mock_dependencies):
        """Test context manager usage."""
        from scrapers.base.base_scraper import BaseScraper

        class ConcreteScraper(BaseScraper):
            SOURCE_NAME = 'test'
            closed = False

            def scrape_all(self):
                return {}

            def close(self):
                self.closed = True

        with ConcreteScraper() as scraper:
            pass

        assert scraper.closed is True


class TestSoccerdataScraper:
    """Tests for SoccerdataScraper base class."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all scraper dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw:

            mock_rl.return_value = MagicMock()
            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_iw.return_value = MagicMock()

            yield

    def test_sd_kwargs(self, mock_dependencies):
        """Test soccerdata kwargs are set correctly."""
        from scrapers.base.base_scraper import SoccerdataScraper

        class ConcreteSdScraper(SoccerdataScraper):
            SOURCE_NAME = 'test'

            def _get_reader(self):
                return MagicMock()

            def scrape_all(self):
                return {}

        scraper = ConcreteSdScraper(no_cache=True, proxy='http://proxy:8080')

        assert scraper._sd_kwargs['no_cache'] is True
        assert scraper._sd_kwargs['proxy'] == 'http://proxy:8080'
