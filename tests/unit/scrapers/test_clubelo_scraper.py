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
        """Mock soccerdata ClubElo reader."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_by_date.return_value = pd.DataFrame({
                'club': ['Manchester City', 'Arsenal', 'Liverpool'],
                'country': ['ENG', 'ENG', 'ENG'],
                'elo': [2050, 1980, 1950],
                'rank': [1, 2, 3],
            })
            reader.read_team_history.return_value = pd.DataFrame({
                'from': [pd.Timestamp('2024-01-01'), pd.Timestamp('2024-06-01')],
                'to': [pd.Timestamp('2024-05-31'), pd.Timestamp('2024-12-31')],
                'elo': [2000, 2050],
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

    def test_rate_limit(self, scraper):
        """Test ClubElo has permissive rate limit."""
        assert scraper.DEFAULT_RATE_LIMIT == 60

    def test_league_mapping(self, scraper):
        """Test league code mapping."""
        assert scraper.LEAGUE_MAPPING['ENG-Premier League'] == 'ENG'
        assert scraper.LEAGUE_MAPPING['ESP-La Liga'] == 'ESP'

    def test_read_by_date(self, scraper, mock_soccerdata_clubelo):
        """Test reading ratings by date."""
        df = scraper.read_by_date(date(2024, 1, 15))

        assert df is not None
        assert 'club' in df.columns
        assert 'elo' in df.columns

    def test_read_by_date_default(self, scraper, mock_soccerdata_clubelo):
        """Test reading ratings for today."""
        df = scraper.read_by_date()

        assert df is not None

    def test_read_team_history(self, scraper, mock_soccerdata_clubelo):
        """Test reading team ELO history."""
        df = scraper.read_team_history('Manchester City')

        assert df is not None
        assert 'team' in df.columns

    def test_scrape_current_ratings(self, scraper, mock_soccerdata_clubelo):
        """Test scraping current ratings."""
        result = scraper.scrape_current_ratings()

        assert 'current_ratings' in result

    def test_scrape_all(self, scraper, mock_soccerdata_clubelo):
        """Test full scrape."""
        result = scraper.scrape_all()

        assert isinstance(result, dict)

    # ------------------------------------------------------------------
    # Regression: scrape_team_histories must call save_to_iceberg with
    # replace_partitions=['team'].
    #
    # Background: read_team_history() returns the FULL ELO history every
    # run. Plain APPEND duplicates ~30k rows × N teams × N days, blowing
    # Iceberg metadata to >20 GB within weeks. The fix overwrites the
    # team partition instead of appending.
    # ------------------------------------------------------------------
    def test_scrape_team_histories_uses_replace_partitions(
        self, scraper, mock_soccerdata_clubelo
    ):
        """scrape_team_histories(['Arsenal']) must pass
        ``replace_partitions=['team']`` to ``save_to_iceberg`` so we get
        partition-replace semantics (not append-only)."""

        # Patch save_to_iceberg on the live scraper instance to capture
        # kwargs without going all the way down to Trino INSERT.
        with patch.object(
            scraper,
            "save_to_iceberg",
            return_value="iceberg.bronze.clubelo_team_history",
        ) as mock_save:
            result = scraper.scrape_team_histories(teams=["Arsenal"])

        # One team → one save call
        assert mock_save.call_count == 1, (
            f"Expected exactly one save_to_iceberg call, "
            f"got {mock_save.call_count}"
        )

        _, kwargs = mock_save.call_args
        assert kwargs.get("table_name") == "clubelo_team_history"
        assert kwargs.get("partition_cols") == ["team"]
        assert kwargs.get("replace_partitions") == ["team"], (
            "scrape_team_histories must pass replace_partitions=['team'] — "
            "without it Iceberg accumulates duplicate snapshots and metadata "
            "balloons (the bug this fix targets)."
        )

        # Sanity: returned dict points at the captured table path (+ row count)
        assert result["team_history"] == "iceberg.bronze.clubelo_team_history"
        assert "rows" in result

    def test_scrape_team_histories_batches_all_teams_in_one_save(
        self, scraper, mock_soccerdata_clubelo
    ):
        """All teams must be concatenated into ONE save_to_iceberg call —
        per-team writes are too slow (each does DELETE+INSERT and triggered
        Airflow timeout when scaled to ~50 PL clubs)."""
        with patch.object(
            scraper,
            "save_to_iceberg",
            return_value="iceberg.bronze.clubelo_team_history",
        ) as mock_save:
            scraper.scrape_team_histories(
                teams=["Arsenal", "Liverpool", "Chelsea"]
            )

        assert mock_save.call_count == 1, (
            "scrape_team_histories must batch all teams into a single save "
            "to avoid per-team DELETE+INSERT overhead."
        )
        _, kwargs = mock_save.call_args
        assert kwargs.get("replace_partitions") == ["team"]
        assert kwargs.get("partition_cols") == ["team"]

    def test_scrape_team_histories_skips_empty_dataframe(
        self, scraper, mock_soccerdata_clubelo
    ):
        """Empty read_team_history → no save call → no exception, returns {}.

        Guards against regressions where the per-team try/except swallows
        the early-return guard.
        """
        # Override read_team_history mock to return empty
        mock_soccerdata_clubelo.read_team_history.return_value = pd.DataFrame()

        with patch.object(scraper, "save_to_iceberg") as mock_save:
            result = scraper.scrape_team_histories(teams=["Arsenal"])

        assert mock_save.call_count == 0
        assert result == {}

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


class TestTopEnglishClubs:
    """Tests for predefined club lists."""

    def test_top_english_clubs(self):
        """Test top English clubs are defined."""
        from scrapers.clubelo.scraper import TOP_ENGLISH_CLUBS

        assert 'Manchester City' in TOP_ENGLISH_CLUBS
        assert 'Arsenal' in TOP_ENGLISH_CLUBS
        assert 'Liverpool' in TOP_ENGLISH_CLUBS
        assert len(TOP_ENGLISH_CLUBS) == 20
