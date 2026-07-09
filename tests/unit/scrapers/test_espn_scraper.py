"""
Tests for ESPNScraper.
"""

import json
from pathlib import Path

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_dependencies():
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


@pytest.fixture
def make_scraper(mock_dependencies):
    """Factory: build an ESPNScraper for arbitrary seasons."""
    def _make(seasons):
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            from scrapers.espn import ESPNScraper
            return ESPNScraper(leagues=['ENG-Premier League'], seasons=seasons)
    return _make


@pytest.fixture
def scraper(make_scraper):
    """Create ESPNScraper instance."""
    return make_scraper([2024])


def _per_match_sched(games):
    """Schedule frame with the columns _read_per_match consumes.

    ``games``: list of ``(game_id, game_key, date_iso)`` tuples.
    """
    return pd.DataFrame({
        'league': ['ENG-Premier League'] * len(games),
        'season': ['2425'] * len(games),
        'game': [g[1] for g in games],
        'date': [g[2] for g in games],
        'home_team': ['H'] * len(games),
        'away_team': ['A'] * len(games),
        'game_id': [g[0] for g in games],
        'league_id': ['eng.1'] * len(games),
    })


class TestESPNScraper:
    """Tests for ESPNScraper."""

    @staticmethod
    def _mock_reader():
        """A soccerdata-reader stand-in with the attributes the COVID-2021
        calendar seed touches (espn.py uses these exact names)."""
        reader = MagicMock()
        reader._selected_leagues = {'ENG-Premier League': 'eng.1'}
        reader.data_dir = Path('/tmp/espn_test')
        return reader

    def test_init(self, scraper):
        """Test ESPNScraper initialization."""
        assert scraper.SOURCE_NAME == 'espn'

    def test_standardize_schedule(self, scraper):
        """Test schedule standardization: date -> match_date rename only.
        Result columns (home_goals etc.) come from the scoreboard enrichment
        in read_schedule, NOT from renames — soccerdata never emits
        home_score, so the old mapping was dead code."""
        df = pd.DataFrame({
            'date': ['2024-08-17'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
        })

        result = scraper._standardize_schedule(df)

        assert 'match_date' in result.columns
        assert 'date' not in result.columns

    def test_read_schedule_raises_on_reader_error(self, scraper):
        """Issue #466: read_schedule must propagate reader errors instead of
        returning None — a swallowed exception leaves the runner's
        results['errors'] empty -> exit 0 -> green DAG on total failure."""
        with patch.object(scraper, '_get_reader', return_value=MagicMock()), \
             patch.object(scraper, '_execute_with_resilience',
                          side_effect=RuntimeError('CF block')):
            with pytest.raises(RuntimeError, match='CF block'):
                scraper.read_schedule()

    def test_scrape_schedule_uses_replace_partitions(self, scraper):
        """Regression #347: scrape_schedule MUST pass replace_partitions=['league',
        'season'] so daily writes replace each partition instead of appending
        (else espn_schedule accumulates ~31x duplicates in the active season)."""
        # Arrange
        mock_schedule = pd.DataFrame({
            'date': ['2024-08-17'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'home_score': [2],
            'away_score': [0],
            'league': ['ENG-Premier League'],
            'season': [2425],
        })

        # Act
        with patch.object(scraper, 'read_schedule', return_value=mock_schedule):
            with patch.object(scraper, 'save_to_iceberg',
                              return_value='iceberg.bronze.test') as mock_save:
                scraper.scrape_schedule()

        # Assert
        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs['replace_partitions'] == ['league', 'season']

    def test_scrape_schedule_arms_completeness_guard(self, scraper):
        """#583: scrape_schedule MUST arm the replace guard (min_replace_ratio
        0.9) so a partial scrape can't wipe a good espn_schedule partition."""
        # Arrange
        mock_schedule = pd.DataFrame({
            'date': ['2024-08-17'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'home_score': [2],
            'away_score': [0],
            'league': ['ENG-Premier League'],
            'season': [2425],
        })

        # Act
        with patch.object(scraper, 'read_schedule', return_value=mock_schedule):
            with patch.object(scraper, 'save_to_iceberg',
                              return_value='iceberg.bronze.test') as mock_save:
                scraper.scrape_schedule()

        # Assert
        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs['min_replace_ratio'] == 0.9

    def test_read_lineup_skips_malformed_match(self, scraper, tmp_path):
        """#713: soccerdata's bulk read_lineup aborts the whole season on one
        match with malformed JSON (KeyError 'displayName'). read_lineup must
        iterate per match and skip only the broken one — good matches still land."""
        sched = _per_match_sched([
            (101, 'g101', '2024-08-17T14:00Z'),
            (102, 'g102', '2024-08-17T14:00Z'),
            (103, 'g103', '2024-08-17T14:00Z'),
        ])
        good1 = pd.DataFrame({'player': ['A'], 'team': ['X']})
        good3 = pd.DataFrame({'player': ['B'], 'team': ['Y']})

        reader = MagicMock()
        reader.data_dir = tmp_path

        def fake_lineup(match_id=None):
            if match_id == 102:
                raise KeyError('displayName')
            return good1 if match_id == 101 else good3

        reader.read_lineup.side_effect = fake_lineup

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_execute_with_resilience', return_value=sched), \
             patch.object(scraper, '_existing_game_keys', return_value=set()), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            result = scraper.read_lineup()

        # Malformed match 102 skipped (no raise); 101 + 103 concatenated.
        assert result is not None
        assert len(result) == 2
        assert set(result['player']) == {'A', 'B'}

    def test_seed_2021_calendar_overrides_covid_anchor(self, make_scraper):
        """#817: soccerdata anchors the ESPN season calendar at July 1 of the
        start year (espn.py: 20{skey[:2]}0701). For 2020-21 (season code '2021')
        that's 2020-07-01, which falls INSIDE the COVID-extended 2019-20 PL
        season (ended 2020-07-26) — so ESPN returns the 2019-20 calendar. The
        seed must overwrite that calendar cache file with a post-COVID anchor
        (2020-08-01) so soccerdata reads the real 2020-21 dates."""
        scraper = make_scraper(['2021'])
        reader = self._mock_reader()

        scraper._seed_2021_season_calendar(reader)

        reader.get.assert_called_once()
        url = reader.get.call_args.args[0]
        seed_fp = reader.get.call_args.args[1]
        assert 'eng.1' in url
        assert 'dates=20200801' in url
        assert Path(seed_fp) == reader.data_dir / 'Schedule_eng.1_20200701.json'
        assert reader.get.call_args.kwargs['no_cache'] is True

    def test_seed_2021_calendar_noop_for_other_seasons(self, make_scraper):
        """The COVID workaround must touch ONLY season '2021' — the daily run
        (season '2526') and every other backfill season are left untouched."""
        scraper = make_scraper(['2526'])
        reader = self._mock_reader()

        scraper._seed_2021_season_calendar(reader)

        reader.get.assert_not_called()

    def test_seed_2021_calendar_runs_at_most_once(self, make_scraper):
        """read_schedule and the per-match readers each seed defensively; the
        helper must be idempotent so it re-downloads the calendar at most once
        per process."""
        scraper = make_scraper(['2021'])
        reader = self._mock_reader()

        scraper._seed_2021_season_calendar(reader)
        scraper._seed_2021_season_calendar(reader)

        reader.get.assert_called_once()

    def test_read_schedule_seeds_2021_calendar_before_read(self, make_scraper):
        """Wiring: read_schedule must seed the 2020-21 calendar before handing
        off to soccerdata — otherwise soccerdata reads the wrong (2019-20)
        calendar and the seed never takes effect."""
        scraper = make_scraper(['2021'])
        reader = self._mock_reader()

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_seed_2021_season_calendar') as mock_seed, \
             patch.object(scraper, '_execute_with_resilience',
                          return_value=pd.DataFrame()), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            scraper.read_schedule()

        mock_seed.assert_called_once_with(reader)


class TestESPNPerMatchTraffic:
    """Traffic/staleness fixes in _read_per_match: unplayed-match filter,
    schedule memoization, stale-stub re-fetch, skip-existing, rate limiting."""

    PAST = '2024-08-17T14:00Z'
    FUTURE = '2999-01-01T14:00Z'

    def _run_lineup(self, scraper, reader, sched, existing=set()):
        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_execute_with_resilience', return_value=sched), \
             patch.object(scraper, '_existing_game_keys', return_value=existing), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            return scraper.read_lineup()

    @staticmethod
    def _lineup_reader(tmp_path):
        reader = MagicMock()
        reader.data_dir = tmp_path
        reader.read_lineup.side_effect = lambda match_id=None: pd.DataFrame(
            {'player': [f'P{match_id}'], 'game': [f'g{match_id}']}
        )
        return reader

    def test_filters_unplayed_matches(self, scraper, tmp_path):
        """A Summary fetched before kickoff is cached forever (soccerdata
        MAXAGE=None) and permanently masks the real lineups — unplayed
        matches must NOT be fetched at all."""
        sched = _per_match_sched([
            (101, 'g101', self.PAST),
            (102, 'g102', self.FUTURE),
        ])
        reader = self._lineup_reader(tmp_path)

        self._run_lineup(scraper, reader, sched)

        fetched = [c.kwargs['match_id'] for c in reader.read_lineup.call_args_list]
        assert fetched == [101]

    def test_memoizes_schedule_during_loop(self, scraper, tmp_path):
        """soccerdata's per-match read_lineup re-runs read_schedule() on EVERY
        call — in a live season that re-downloads every date scoreboard with
        no_cache=True (O(matches x match days) requests). The loop must feed
        per-match calls a memoized schedule and restore the reader afterwards."""
        sched = _per_match_sched([
            (101, 'g101', self.PAST),
            (103, 'g103', self.PAST),
        ])
        reader = MagicMock()
        reader.data_dir = tmp_path
        orig_calls = []

        def orig_read_schedule(force_cache=False):
            orig_calls.append(1)
            return sched

        reader.read_schedule = orig_read_schedule

        def fake_lineup(match_id=None):
            # mimic soccerdata: every per-match call re-reads the schedule
            reader.read_schedule()
            return pd.DataFrame({'player': [f'P{match_id}']})

        reader.read_lineup.side_effect = fake_lineup

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_execute_with_resilience',
                          side_effect=lambda func: func()), \
             patch.object(scraper, '_existing_game_keys', return_value=set()), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            result = scraper.read_lineup()

        assert len(result) == 2
        # one real fetch up front; the two per-match calls hit the memo
        assert len(orig_calls) == 1
        # reader restored — later callers get live behaviour back
        assert reader.read_schedule is orig_read_schedule

    def test_refetches_stale_pre_kickoff_stub(self, scraper, tmp_path):
        """A cached Summary without rosters for an already-played match is a
        pre-kickoff stub — re-download it once (no_cache=True) so the real
        lineups/stats can ever land (poisoned-cache heal)."""
        (tmp_path / 'Summary_101.json').write_text(json.dumps(
            {'rosters': [{'homeAway': 'home'}, {'homeAway': 'away'}]}
        ))
        sched = _per_match_sched([(101, 'g101', self.PAST)])
        reader = self._lineup_reader(tmp_path)

        self._run_lineup(scraper, reader, sched)

        reader.get.assert_called_once()
        url = reader.get.call_args.args[0]
        assert 'eng.1/summary?event=101' in url
        assert reader.get.call_args.kwargs['no_cache'] is True

    def test_complete_summary_not_refetched(self, scraper, tmp_path):
        """A cached Summary that already has rosters must be served from cache
        — no re-download, no rate-limit wait."""
        (tmp_path / 'Summary_101.json').write_text(json.dumps(
            {'rosters': [{'roster': [{'athlete': {'displayName': 'A'}}]},
                         {'roster': [{'athlete': {'displayName': 'B'}}]}]}
        ))
        sched = _per_match_sched([(101, 'g101', self.PAST)])
        reader = self._lineup_reader(tmp_path)

        self._run_lineup(scraper, reader, sched)

        reader.get.assert_not_called()
        scraper._rate_limiter.acquire.assert_not_called()

    def test_rate_limits_only_new_downloads(self, scraper, tmp_path):
        """The per-match loop must pace matches whose Summary is NOT yet
        cached (a real download) and stay silent for cached ones."""
        (tmp_path / 'Summary_101.json').write_text(json.dumps(
            {'rosters': [{'roster': [{'athlete': {'displayName': 'A'}}]}]}
        ))
        sched = _per_match_sched([
            (101, 'g101', self.PAST),   # cached, complete -> no acquire
            (102, 'g102', self.PAST),   # not cached -> acquire
        ])
        reader = self._lineup_reader(tmp_path)

        self._run_lineup(scraper, reader, sched)

        assert scraper._rate_limiter.acquire.call_count == 1

    def test_skip_existing_drops_ingested_games(self, scraper, tmp_path):
        """Skip-existing (#842 pattern): games already materialised in bronze
        are not re-fetched — steady-state cost is 'new matches only'."""
        sched = _per_match_sched([
            (101, 'g101', self.PAST),
            (102, 'g102', self.PAST),
        ])
        reader = self._lineup_reader(tmp_path)

        self._run_lineup(scraper, reader, sched, existing={'g101'})

        fetched = [c.kwargs['match_id'] for c in reader.read_lineup.call_args_list]
        assert fetched == [102]

    def test_probe_failure_treats_all_as_new(self, scraper, tmp_path):
        """_existing_game_keys -> None (probe failed) must NOT skip anything:
        per-game replace semantics make a full re-scrape duplicate-safe."""
        sched = _per_match_sched([
            (101, 'g101', self.PAST),
            (102, 'g102', self.PAST),
        ])
        reader = self._lineup_reader(tmp_path)

        self._run_lineup(scraper, reader, sched, existing=None)

        assert reader.read_lineup.call_count == 2

    def test_all_existing_is_noop(self, scraper, tmp_path):
        """Every game already ingested -> None (no-op run, nothing saved)."""
        sched = _per_match_sched([(101, 'g101', self.PAST)])
        reader = self._lineup_reader(tmp_path)

        result = self._run_lineup(scraper, reader, sched, existing={'g101'})

        assert result is None
        reader.read_lineup.assert_not_called()

    def test_matchsheet_probe_requires_stats(self, scraper, tmp_path):
        """The matchsheet skip-probe must key on total_shots IS NOT NULL: a
        pre-fix stub row (venue only, no stats) doesn't count as ingested,
        so the stub gets re-scraped and healed."""
        sched = _per_match_sched([(101, 'g101', self.PAST)])
        reader = MagicMock()
        reader.data_dir = tmp_path
        reader.read_matchsheet.side_effect = lambda match_id=None: pd.DataFrame(
            {'team': ['X'], 'game': [f'g{match_id}']}
        )

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_execute_with_resilience', return_value=sched), \
             patch.object(scraper, '_existing_game_keys',
                          return_value=set()) as mock_probe, \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            scraper.read_matchsheet()

        assert mock_probe.call_args.kwargs.get('non_null_col') == 'total_shots'

    def test_skip_existing_false_scrapes_everything(self, scraper, tmp_path):
        """--force-replace path: skip_existing=False must bypass the probe and
        re-fetch every played match (full refresh)."""
        sched = _per_match_sched([(101, 'g101', self.PAST)])
        reader = self._lineup_reader(tmp_path)

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_execute_with_resilience', return_value=sched), \
             patch.object(scraper, '_existing_game_keys',
                          return_value={'g101'}) as mock_probe, \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            scraper.read_lineup(skip_existing=False)

        mock_probe.assert_not_called()
        assert reader.read_lineup.call_count == 1

    def test_network_error_logged_as_network_not_malformed(self, scraper, tmp_path, caplog):
        """soccerdata raises ConnectionError after its own 5 retries — the loop
        must skip the match with an honest 'network error' log instead of
        mislabelling it as malformed ESPN data (and must not run the
        sanitize-retry path pointlessly)."""
        sched = _per_match_sched([(101, 'g101', self.PAST)])
        reader = MagicMock()
        reader.data_dir = tmp_path
        reader.read_lineup.side_effect = ConnectionError('Could not download')

        with patch.object(scraper, '_sanitize_match_cache') as mock_sanitize:
            result = self._run_lineup(scraper, reader, sched)

        assert result is None
        mock_sanitize.assert_not_called()
        assert any('network error' in r.message for r in caplog.records)


class TestESPNScheduleEnrichment:
    """#F4: bronze.espn_schedule must carry результат — home_goals/away_goals/
    status/venue/attendance joined from the scoreboard JSONs soccerdata
    already downloaded (zero extra traffic)."""

    @staticmethod
    def _scoreboard(events):
        return json.dumps({'events': events})

    def test_read_schedule_enriches_result_columns(self, scraper, tmp_path):
        raw = pd.DataFrame({
            'league': ['ENG-Premier League'],
            'season': ['2425'],
            'game': ['2024-08-17 Arsenal-Wolves'],
            'date': ['2024-08-17T14:00Z'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'game_id': [101],
            'league_id': ['eng.1'],
        })
        reader = MagicMock()
        reader.data_dir = tmp_path
        (tmp_path / 'Schedule_eng.1_20240817.json').write_text(self._scoreboard([{
            'id': '101',
            'status': {'type': {'name': 'STATUS_FULL_TIME'}},
            'competitions': [{
                'venue': {'fullName': 'Emirates Stadium'},
                'attendance': 60000,
                'competitors': [
                    {'homeAway': 'home', 'score': '2'},
                    {'homeAway': 'away', 'score': '0'},
                ],
            }],
        }]))

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_execute_with_resilience', return_value=raw), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            df = scraper.read_schedule()

        row = df.iloc[0]
        assert row['home_goals'] == '2'
        assert row['away_goals'] == '0'
        assert row['status'] == 'STATUS_FULL_TIME'
        assert row['venue'] == 'Emirates Stadium'
        assert row['attendance'] == '60000'

    def test_missing_scoreboard_cache_leaves_nulls(self, scraper, tmp_path):
        """No cache file for the match date -> result columns exist but stay
        NULL (never crash the schedule save)."""
        raw = pd.DataFrame({
            'league': ['ENG-Premier League'],
            'season': ['2425'],
            'game': ['2024-08-17 Arsenal-Wolves'],
            'date': ['2024-08-17T14:00Z'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'game_id': [101],
            'league_id': ['eng.1'],
        })
        reader = MagicMock()
        reader.data_dir = tmp_path

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_execute_with_resilience', return_value=raw), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            df = scraper.read_schedule()

        assert 'home_goals' in df.columns
        assert df['home_goals'].isna().all()


class TestESPNMatchsheetRoster:
    """#F5: the raw ESPN roster blob duplicates espn_lineup and bloats bronze
    — drop it before the save."""

    def test_read_matchsheet_drops_roster_column(self, scraper):
        df = pd.DataFrame({
            'league': ['ENG-Premier League'],
            'season': ['2425'],
            'game': ['g101'],
            'team': ['Arsenal'],
            'roster': [[{'athlete': {'displayName': 'A'}}]],
            'total_shots': [12],
        })

        with patch.object(scraper, '_read_per_match', return_value=df), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            result = scraper.read_matchsheet()

        assert 'roster' not in result.columns
        assert result.iloc[0]['total_shots'] == '12'


class TestESPNConfigFloors:
    """#F9: espn_lineup / espn_matchsheet need wipe-floors — otherwise a stale
    or wiped per-match table passes validation silently (#466 class)."""

    def test_thresholds_present(self):
        from utils.config import MIN_ROW_THRESHOLDS

        # ~28 lineup rows x 340 matches/season and 2 matchsheet rows x 340,
        # sized as wipe-floors against the whole table (>=1 season retained).
        assert MIN_ROW_THRESHOLDS.get('espn_lineup', 0) >= 9000
        assert MIN_ROW_THRESHOLDS.get('espn_matchsheet', 0) >= 600


class TestSeedSingleYearCupCalendar:
    """#913: fifa.world serves the calendar as STAGE DICTS — the seeder must
    rewrite the cached anchor into the flat day-string list soccerdata's
    read_schedule expects, fetching content from July 1 of the tournament
    year (the soccerdata anchor formula degenerates to 2020 for skey '2026')."""

    _CAL = {
        "leagues": [{"calendar": [{
            "label": "FIFA World Cup",
            "startDate": "2026-06-11T04:00Z",
            "endDate": "2026-12-31T04:59Z",   # outer shell — must be ignored
            "entries": [
                {"label": "Group", "startDate": "2026-06-11T07:00Z",
                 "endDate": "2026-06-13T06:59Z"},
                {"label": "Final", "startDate": "2026-07-19T07:00Z",
                 "endDate": "2026-07-19T23:59Z"},
            ],
        }]}],
        "events": [],
    }

    def _reader(self, tmp_path, payload):
        import io

        reader = MagicMock()
        reader.data_dir = tmp_path
        reader._selected_leagues = {'INT-World Cup': 'fifa.world'}
        reader.seasons = ['2026']
        reader.get = MagicMock(
            return_value=io.StringIO(json.dumps(payload)))
        return reader

    def test_rewrites_stage_dict_calendar_to_day_strings(
            self, make_scraper, tmp_path):
        scraper = make_scraper(['2026'])
        reader = self._reader(tmp_path, self._CAL)
        scraper._seed_single_year_cup_calendar(reader)

        # Content fetched from July 1 of the TOURNAMENT year, not 2020.
        url = reader.get.call_args[0][0]
        assert 'dates=20260701' in url
        assert reader.get.call_args[1] == {'no_cache': True}
        # Rewritten into soccerdata's anchor path (its formula → 20200701).
        fp = tmp_path / 'Schedule_fifa.world_20200701.json'
        assert fp.exists()
        cal = json.loads(fp.read_text())['leagues'][0]['calendar']
        # 3 Group days + 1 Final day; outer Dec-31 shell ignored.
        assert cal == ['2026-06-11T12:00Z', '2026-06-12T12:00Z',
                       '2026-06-13T12:00Z', '2026-07-19T12:00Z']

    def test_noop_for_flat_club_calendar(self, make_scraper, tmp_path):
        payload = {"leagues": [{"calendar": ["2025-08-16T14:00Z"]}],
                   "events": []}
        scraper = make_scraper(['2026'])
        reader = self._reader(tmp_path, payload)
        scraper._seed_single_year_cup_calendar(reader)
        assert not (tmp_path / 'Schedule_fifa.world_20200701.json').exists()

    def test_noop_without_world_cup_league(self, make_scraper, tmp_path):
        scraper = make_scraper(['2026'])
        reader = self._reader(tmp_path, self._CAL)
        reader._selected_leagues = {'ENG-Premier League': 'eng.1'}
        scraper._seed_single_year_cup_calendar(reader)
        reader.get.assert_not_called()
