"""
Tests for ClubEloScraper.
"""

import pytest
import pandas as pd
from datetime import date
from unittest.mock import MagicMock, patch


class TestClubEloScraper:
    """Tests for ClubEloScraper."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.fixture
    def mock_soccerdata_clubelo(self):
        """Mock soccerdata ClubElo reader.

        Mirrors the real read_by_date frame: soccerdata already translates
        ClubElo's country_level into canonical league IDs ('ENG_1' →
        'ENG-Premier League'); untranslated levels (e.g. Championship) come
        back as NaN. The Championship row must be filtered OUT by the scraper.
        """
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_by_date.return_value = pd.DataFrame({
                'team': ['Manchester City', 'Arsenal', 'Leeds'],
                'country': ['ENG', 'ENG', 'ENG'],
                'level': [1, 1, 2],
                'league': ['ENG-Premier League', 'ENG-Premier League', float('nan')],
                'elo': [2050, 1980, 1700],
                'rank': [1, 2, float('nan')],
            })

            sd.ClubElo.return_value = reader
            yield reader

    @pytest.fixture
    def scraper(self, mock_dependencies, mock_soccerdata_clubelo):
        """Create ClubEloScraper instance."""
        from scrapers.clubelo import ClubEloScraper

        return ClubEloScraper(leagues=['ENG-Premier League'])

    def test_init(self, scraper):
        """Test ClubEloScraper initialization."""
        assert scraper.SOURCE_NAME == 'clubelo'
        assert 'ENG-Premier League' in scraper.leagues

    def test_read_by_date(self, scraper, mock_soccerdata_clubelo):
        """Test reading ratings by date."""
        df = scraper.read_by_date(date(2024, 1, 15))

        assert df is not None
        assert 'team' in df.columns
        assert 'elo' in df.columns

    # ------------------------------------------------------------------
    # League scalability: filter on the CANONICAL league column soccerdata
    # already provides ('ENG_1' → 'ENG-Premier League'), not on country.
    # A country filter also kept lower-division clubs (league=NaN) — 24
    # Championship rows/day that became orphans in xref_team/fct_team_elo.
    # ------------------------------------------------------------------
    def test_read_by_date_filters_by_canonical_league(
        self, scraper, mock_soccerdata_clubelo
    ):
        """Only rows of the requested canonical leagues survive; lower-level
        clubs of the same country (league=NaN) are dropped."""
        df = scraper.read_by_date(date(2024, 1, 15))

        assert set(df['league']) == {'ENG-Premier League'}
        assert 'Leeds' not in df['team'].tolist()  # level-2, league=NaN
        assert len(df) == 2

    def test_read_by_date_warns_on_league_with_no_rows(
        self, mock_dependencies, mock_soccerdata_clubelo, caplog
    ):
        """A requested league absent from the snapshot (unmapped or not
        covered by ClubElo) must produce a WARNING, not silent 0 rows."""
        import logging

        from scrapers.clubelo import ClubEloScraper

        scraper = ClubEloScraper(leagues=['USA-Major League Soccer'])
        with caplog.at_level(logging.WARNING):
            df = scraper.read_by_date(date(2024, 1, 15))

        assert df is not None and df.empty
        assert any('USA-Major League Soccer' in r.message for r in caplog.records)

    def test_read_by_date_default(self, scraper, mock_soccerdata_clubelo):
        """Test reading ratings for today."""
        df = scraper.read_by_date()

        assert df is not None

    def test_scrape_current_ratings(self, scraper, mock_soccerdata_clubelo):
        """Test scraping current ratings; result carries rows + rating_date
        so the runner can report them without re-implementing the save."""
        result = scraper.scrape_current_ratings()

        assert 'current_ratings' in result
        assert result['rows'] == 2
        assert result['rating_date'] is not None

    # ------------------------------------------------------------------
    # #583 guard parity: the completeness guard must live on the METHOD, not
    # only inlined in the runner — the two paths had already drifted (the
    # runner armed min_replace_ratio, scrape_current_ratings did not).
    # ------------------------------------------------------------------
    def test_scrape_current_ratings_arms_replace_guard(
        self, scraper, mock_soccerdata_clubelo
    ):
        """Default call passes min_replace_ratio=0.9; force_replace disarms."""
        with patch.object(
            scraper, "save_to_iceberg", return_value="iceberg.bronze.clubelo_ratings"
        ) as mock_save:
            scraper.scrape_current_ratings()
        assert mock_save.call_args.kwargs["min_replace_ratio"] == 0.9

        with patch.object(
            scraper, "save_to_iceberg", return_value="iceberg.bronze.clubelo_ratings"
        ) as mock_save:
            scraper.scrape_current_ratings(force_replace=True)
        assert mock_save.call_args.kwargs["min_replace_ratio"] is None

    def test_scrape_historical_ratings_empty_logs_error(
        self, scraper, mock_soccerdata_clubelo, caplog
    ):
        """All per-date reads failing must NOT be silent: {} + ERROR log
        (the runner turns this into results['errors'])."""
        import logging

        with patch.object(scraper, "read_by_date", return_value=None):
            with caplog.at_level(logging.ERROR):
                result = scraper.scrape_historical_ratings(days_back=14)

        assert result == {}
        assert any(r.levelno >= logging.ERROR for r in caplog.records)

    def test_scrape_all(self, scraper, mock_soccerdata_clubelo):
        """Test full scrape."""
        result = scraper.scrape_all()

        assert isinstance(result, dict)

    # ------------------------------------------------------------------
    # Regression: scrape_historical_ratings must call save_to_iceberg with
    # replace_partitions=['rating_date'] AND a string rating_date.
    #
    # Background: rating_date arrives as a datetime. _build_partition_delete_filter
    # only quotes str values; a datetime is emitted raw → invalid SQL → the DELETE
    # fails and the writer SILENTLY falls back to plain APPEND (the 2026-05-04
    # HDFS-overflow footgun). Normalizing rating_date to ISO 'YYYY-MM-DD' makes the
    # replace-partition DELETE valid so re-runs stay idempotent.
    # ------------------------------------------------------------------
    def test_scrape_historical_ratings_uses_replace_partitions_with_str_date(
        self, scraper, mock_soccerdata_clubelo
    ):
        """scrape_historical_ratings must pass replace_partitions=['rating_date']
        and the saved frame's rating_date column must be ISO strings (not
        datetimes), so the partition-delete filter is valid SQL."""
        with patch.object(
            scraper,
            "save_to_iceberg",
            return_value="iceberg.bronze.clubelo_ratings_historical",
        ) as mock_save:
            result = scraper.scrape_historical_ratings(days_back=14)

        assert mock_save.call_count == 1
        _, kwargs = mock_save.call_args
        assert kwargs.get("table_name") == "clubelo_ratings_historical"
        assert kwargs.get("partition_cols") == ["rating_date"]
        assert kwargs.get("replace_partitions") == ["rating_date"], (
            "scrape_historical_ratings must pass replace_partitions=['rating_date'] "
            "— without it daily/repeat runs APPEND duplicate snapshots and Iceberg "
            "metadata balloons (2026-05-04 HDFS overflow)."
        )

        saved_df = kwargs.get("df")
        assert saved_df is not None and not saved_df.empty
        rating_dates = saved_df["rating_date"].tolist()
        assert all(isinstance(v, str) for v in rating_dates), (
            "rating_date must be ISO strings — a datetime makes "
            "_build_partition_delete_filter emit an unquoted predicate, the DELETE "
            "fails, and the writer silently falls back to APPEND."
        )
        # ISO format YYYY-MM-DD
        assert all(len(v) == 10 and v[4] == '-' and v[7] == '-' for v in rating_dates)

        assert result["historical_ratings"] == "iceberg.bronze.clubelo_ratings_historical"
        assert "rows" in result

    # ------------------------------------------------------------------
    # Regression (#470 bug 5): the DAILY snapshot path must match historical —
    # rating_date as an ISO date-only string + replace_partitions=['rating_date'].
    #
    # Background: scrape_current_ratings set rating_date = datetime.now() WITH
    # TIME and saved without replace_partitions, so an Airflow retry / same-day
    # rerun appended a second full snapshot (the historical path was already
    # fixed; the two branches had diverged). Precedent: #283/#314.
    # ------------------------------------------------------------------
    def test_scrape_current_ratings_uses_replace_partitions_with_str_date(
        self, scraper, mock_soccerdata_clubelo
    ):
        """scrape_current_ratings must pass replace_partitions=['rating_date']
        and a date-only ISO string, so a same-day rerun replaces (not appends)."""
        with patch.object(
            scraper,
            "save_to_iceberg",
            return_value="iceberg.bronze.clubelo_ratings",
        ) as mock_save:
            result = scraper.scrape_current_ratings()

        assert mock_save.call_count == 1
        _, kwargs = mock_save.call_args
        assert kwargs.get("table_name") == "clubelo_ratings"
        assert kwargs.get("partition_cols") == ["rating_date"]
        assert kwargs.get("replace_partitions") == ["rating_date"], (
            "scrape_current_ratings must pass replace_partitions=['rating_date'] "
            "— without it a same-day rerun/Airflow-retry APPENDS a duplicate "
            "full snapshot (#470)."
        )

        saved_df = kwargs.get("df")
        assert saved_df is not None and not saved_df.empty
        rating_dates = saved_df["rating_date"].tolist()
        assert all(isinstance(v, str) for v in rating_dates), (
            "rating_date must be a date-only ISO string (no time component), or "
            "_build_partition_delete_filter emits an invalid predicate and the "
            "writer silently falls back to APPEND."
        )
        assert all(len(v) == 10 and v[4] == '-' and v[7] == '-' for v in rating_dates)

        assert result["current_ratings"] == "iceberg.bronze.clubelo_ratings"
