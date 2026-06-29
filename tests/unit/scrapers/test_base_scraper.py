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

    def test_build_partition_delete_filter_raises_on_missing_columns(self):
        """replace_partitions on a frame missing the partition column must fail
        loud, not silently fall back to a dup-accumulating append (#314 p.3)."""
        from scrapers.base.base_scraper import BaseScraper

        df = pd.DataFrame({'season': [2024], 'value': [1]})  # no 'league'
        with pytest.raises(ValueError, match='missing columns'):
            BaseScraper._build_partition_delete_filter(df, ['league', 'season'])

    def test_build_partition_delete_filter_raises_on_all_null_keys(self):
        """A non-empty frame whose partition keys are all NULL must fail loud."""
        from scrapers.base.base_scraper import BaseScraper

        df = pd.DataFrame({'league': [None, None], 'season': [None, None]})
        with pytest.raises(ValueError, match='NULL'):
            BaseScraper._build_partition_delete_filter(df, ['league', 'season'])

    def test_build_partition_delete_filter_happy_path(self):
        """Valid partition keys still produce the OR-joined WHERE clause."""
        from scrapers.base.base_scraper import BaseScraper

        df = pd.DataFrame({'league': ['EPL', 'EPL'], 'season': [2024, 2024]})
        clause = BaseScraper._build_partition_delete_filter(df, ['league', 'season'])
        assert clause == "(league = 'EPL' AND season = 2024)"

    # ---- replace_partitions completeness guard (min_replace_ratio, #513) ----

    def _guard_df(self, n_players=100):
        """One (league, season) partition with ``n`` distinct player_id."""
        return pd.DataFrame({
            'league': ['EPL'] * n_players,
            'season': ['2526'] * n_players,
            'player_id': [str(i) for i in range(n_players)],
        })

    def _setup_iw(self, scraper, *, exists=True, existing_count=100):
        """Wire the mocked IcebergWriter for the guard's count query."""
        iw = scraper._iceberg_writer
        iw.catalog = 'iceberg'
        iw.table_exists.return_value = exists
        iw._get_trino_manager.return_value.execute_query.return_value = (
            [[existing_count]] if existing_count is not None else []
        )
        return iw

    def test_guard_blocks_when_new_below_ratio(self, concrete_scraper):
        """50 distinct players < 90% of 600 existing → refuse, nothing written."""
        from scrapers.base.base_scraper import ReplaceGuardError
        iw = self._setup_iw(concrete_scraper, existing_count=600)
        df = self._guard_df(50)

        with pytest.raises(ReplaceGuardError, match='refusing'):
            concrete_scraper.save_to_iceberg(
                df, 'transfermarkt_players',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=0.9, replace_guard_key='player_id',
            )
        iw.write_dataframe.assert_not_called()

    def test_guard_passes_at_exact_boundary(self, concrete_scraper):
        """90 vs 100 == exactly 90% → not below threshold → save proceeds."""
        iw = self._setup_iw(concrete_scraper, existing_count=100)
        df = self._guard_df(90)

        concrete_scraper.save_to_iceberg(
            df, 'transfermarkt_players',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
            min_replace_ratio=0.9, replace_guard_key='player_id',
        )
        iw.write_dataframe.assert_called_once()

    def test_guard_passes_when_above_ratio(self, concrete_scraper):
        iw = self._setup_iw(concrete_scraper, existing_count=100)
        df = self._guard_df(100)

        concrete_scraper.save_to_iceberg(
            df, 'transfermarkt_players',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
            min_replace_ratio=0.9, replace_guard_key='player_id',
        )
        iw.write_dataframe.assert_called_once()

    def test_guard_skipped_when_table_missing(self, concrete_scraper):
        """First run (table absent) → no count query, save proceeds."""
        iw = self._setup_iw(concrete_scraper, exists=False)
        df = self._guard_df(1)

        concrete_scraper.save_to_iceberg(
            df, 'transfermarkt_players',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
            min_replace_ratio=0.9, replace_guard_key='player_id',
        )
        iw.write_dataframe.assert_called_once()
        iw._get_trino_manager.return_value.execute_query.assert_not_called()

    def test_guard_skipped_when_existing_zero(self, concrete_scraper):
        iw = self._setup_iw(concrete_scraper, existing_count=0)
        df = self._guard_df(1)

        concrete_scraper.save_to_iceberg(
            df, 'transfermarkt_players',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
            min_replace_ratio=0.9, replace_guard_key='player_id',
        )
        iw.write_dataframe.assert_called_once()

    def test_guard_skipped_when_trino_unreachable(self, concrete_scraper):
        """table_exists raising (dead cluster) → skip; the save needs Trino too,
        so an unreachable cluster cannot wipe anything."""
        iw = self._setup_iw(concrete_scraper)
        iw.table_exists.side_effect = Exception('connection refused')
        df = self._guard_df(1)

        concrete_scraper.save_to_iceberg(
            df, 'transfermarkt_players',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
            min_replace_ratio=0.9, replace_guard_key='player_id',
        )
        iw.write_dataframe.assert_called_once()

    def test_guard_skipped_on_query_error(self, concrete_scraper):
        iw = self._setup_iw(concrete_scraper)
        iw._get_trino_manager.return_value.execute_query.side_effect = (
            Exception('boom')
        )
        df = self._guard_df(1)

        concrete_scraper.save_to_iceberg(
            df, 'transfermarkt_players',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
            min_replace_ratio=0.9, replace_guard_key='player_id',
        )
        iw.write_dataframe.assert_called_once()

    def test_guard_query_uses_distinct_key_and_delete_filter(self, concrete_scraper):
        iw = self._setup_iw(concrete_scraper, existing_count=100)
        df = self._guard_df(100)

        concrete_scraper.save_to_iceberg(
            df, 'transfermarkt_players',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
            min_replace_ratio=0.9, replace_guard_key='player_id',
        )
        sql = iw._get_trino_manager.return_value.execute_query.call_args[0][0]
        assert 'count(DISTINCT player_id)' in sql
        assert 'iceberg.bronze.transfermarkt_players' in sql
        assert "league = 'EPL'" in sql
        assert "season = '2526'" in sql

    def test_guard_counts_raw_rows_without_key(self, concrete_scraper):
        """No replace_guard_key → COUNT(*) of raw rows (not DISTINCT)."""
        from scrapers.base.base_scraper import ReplaceGuardError
        iw = self._setup_iw(concrete_scraper, existing_count=100)
        df = self._guard_df(50)  # 50 rows < 90 → block via raw-row count

        with pytest.raises(ReplaceGuardError):
            concrete_scraper.save_to_iceberg(
                df, 'matchhistory_results',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=0.9,
            )
        sql = iw._get_trino_manager.return_value.execute_query.call_args[0][0]
        assert 'count(*)' in sql
        assert 'DISTINCT' not in sql
        iw.write_dataframe.assert_not_called()

    def test_guard_no_op_without_min_replace_ratio(self, concrete_scraper):
        """Regression contract for the ~50 existing replace_partitions call
        sites: omitting min_replace_ratio runs no count query and writes."""
        iw = self._setup_iw(concrete_scraper, existing_count=1)
        df = self._guard_df(100)

        concrete_scraper.save_to_iceberg(
            df, 'transfermarkt_players',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
        )
        iw.table_exists.assert_not_called()
        iw.write_dataframe.assert_called_once()

    def test_guard_raises_without_replace_partitions(self, concrete_scraper):
        self._setup_iw(concrete_scraper)
        df = self._guard_df(100)
        with pytest.raises(ValueError, match='requires replace_partitions'):
            concrete_scraper.save_to_iceberg(
                df, 'transfermarkt_players',
                partition_cols=['league', 'season'],
                min_replace_ratio=0.9, replace_guard_key='player_id',
            )

    @pytest.mark.parametrize('bad', [0.0, 1.5, -0.1])
    def test_guard_raises_on_ratio_out_of_range(self, concrete_scraper, bad):
        self._setup_iw(concrete_scraper)
        df = self._guard_df(100)
        with pytest.raises(ValueError, match='min_replace_ratio'):
            concrete_scraper.save_to_iceberg(
                df, 'transfermarkt_players',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=bad, replace_guard_key='player_id',
            )

    def test_guard_raises_when_key_missing(self, concrete_scraper):
        self._setup_iw(concrete_scraper)
        df = self._guard_df(100)
        with pytest.raises(ValueError, match='not in DataFrame columns'):
            concrete_scraper.save_to_iceberg(
                df, 'transfermarkt_players',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=0.9, replace_guard_key='nonexistent',
            )

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
