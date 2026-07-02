"""
Unit tests for FotMobScraper.

Tests scraper logic with mocked HTTP responses. The scraper uses FotMob's
public ``/api/data`` JSON endpoints (no browser / cookies).
"""

import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestFotMobScraperUnit:
    """Unit tests for FotMobScraper."""

    @pytest.fixture
    def scraper_class(self):
        """Get scraper class without instantiating."""
        from scrapers.fotmob import FotMobScraper
        return FotMobScraper

    @pytest.fixture
    def mock_scraper(self, scraper_class):
        """Create scraper with a mocked HTTP session."""
        scraper = scraper_class(
            leagues=['ENG-Premier League'],
            seasons=[2025],
        )
        scraper._session = MagicMock()
        yield scraper
        scraper.close()

    def test_init(self, scraper_class):
        """Test scraper initialization."""
        scraper = scraper_class(
            leagues=['ENG-Premier League'],
            seasons=[2025],
        )
        assert scraper.SOURCE_NAME == 'fotmob'
        assert scraper.leagues == ['ENG-Premier League']
        assert scraper.seasons == [2025]
        assert scraper.API_BASE.endswith('/api/data')
        scraper.close()

    def test_format_season(self, mock_scraper):
        """Test season formatting."""
        assert mock_scraper._format_season(2024) == '2024/2025'
        assert mock_scraper._format_season(2023) == '2023/2024'
        assert mock_scraper._format_season(2020) == '2020/2021'

    def test_league_ids(self, scraper_class):
        """Test league IDs configuration."""
        assert 'ENG-Premier League' in scraper_class.LEAGUE_IDS
        assert scraper_class.LEAGUE_IDS['ENG-Premier League'] == '47'
        assert scraper_class.LEAGUE_IDS['ESP-La Liga'] == '87'
        assert scraper_class.LEAGUE_IDS['GER-Bundesliga'] == '54'

    def test_fetch_api_json_success(self, mock_scraper):
        """Test successful API fetch."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'test'}

        mock_scraper._session.get.return_value = mock_response

        result = mock_scraper._fetch_api_json('test-endpoint', {'param': 'value'})

        assert result == {'data': 'test'}
        mock_scraper._session.get.assert_called()

    def test_fetch_api_json_absolute_url(self, mock_scraper):
        """Absolute http(s) endpoints bypass API_BASE (used for fetchAllUrl)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'TopLists': []}
        mock_scraper._session.get.return_value = mock_response

        result = mock_scraper._fetch_api_json('https://data.fotmob.com/stats/x.json')

        assert result == {'TopLists': []}
        called_url = mock_scraper._session.get.call_args[0][0]
        assert called_url == 'https://data.fotmob.com/stats/x.json'

    def test_fetch_api_json_non_200_returns_none(self, mock_scraper):
        """Non-200 responses exhaust retries and return None (no cookie path)."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_scraper._session.get.return_value = mock_response

        result = mock_scraper._fetch_api_json('test-endpoint', retry_count=2)

        assert result is None

    def test_next_data_buildid_rotation_retry(self, mock_scraper):
        """A stale buildId (404) triggers one refresh + retry with the new id."""
        with patch.object(mock_scraper, '_get_build_id',
                          side_effect=['oldbuild', 'newbuild']) as gb, \
             patch.object(mock_scraper, '_fetch_api_json',
                          side_effect=[None, {'pageProps': {'data': {'id': 1}}}]) as fj:
            res = mock_scraper._fetch_next_data_payload('/players/1')

        assert res == {'pageProps': {'data': {'id': 1}}}
        assert gb.call_count == 2                      # refreshed once
        assert 'oldbuild' in fj.call_args_list[0][0][0]
        assert 'newbuild' in fj.call_args_list[1][0][0]

    def test_league_data_cached_per_league_season(self, mock_scraper):
        """The ~65KB league payload is fetched once per (league, season) run."""
        with patch.object(mock_scraper, '_fetch_api_json',
                          return_value={'table': []}) as fj:
            d1 = mock_scraper._get_league_data('ENG-Premier League', 2025)
            d2 = mock_scraper._get_league_data('ENG-Premier League', 2025)
            mock_scraper._get_league_data('ENG-Premier League', 2024)

        assert d1 is d2
        assert fj.call_count == 2                 # second season = separate fetch

    def test_league_data_failure_not_cached(self, mock_scraper):
        """A failed league fetch must not poison the cache for later entities."""
        with patch.object(mock_scraper, '_fetch_api_json',
                          side_effect=[None, {'table': []}]) as fj:
            assert mock_scraper._get_league_data('ENG-Premier League', 2025) is None
            assert mock_scraper._get_league_data('ENG-Premier League', 2025) == {'table': []}

        assert fj.call_count == 2

    def test_fetch_api_json_404_not_retried(self, mock_scraper):
        """404 is permanent — retrying only burns rate-limit budget."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_scraper._session.get.return_value = mock_response

        assert mock_scraper._fetch_api_json('test-endpoint', retry_count=3) is None
        assert mock_scraper._session.get.call_count == 1

    def test_fetch_api_json_5xx_retried_with_backoff(self, mock_scraper):
        """Transient 5xx is retried with a pause between attempts."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_scraper._session.get.return_value = mock_response

        with patch('scrapers.fotmob.scraper.time.sleep') as sleep:
            assert mock_scraper._fetch_api_json('test-endpoint', retry_count=3) is None

        assert mock_scraper._session.get.call_count == 3
        assert sleep.call_count == 2

    def test_next_data_no_retry_when_buildid_unchanged(self, mock_scraper):
        """A miss with a fresh buildId is a genuine 404 — no duplicate request."""
        with patch.object(mock_scraper, '_get_build_id',
                          side_effect=['same', 'same']) as gb, \
             patch.object(mock_scraper, '_fetch_api_json',
                          return_value=None) as fj:
            assert mock_scraper._fetch_next_data_payload('/players/1') is None

        assert gb.call_count == 2
        fj.assert_called_once()

    def test_next_data_buildid_refresh_throttled(self, mock_scraper):
        """Repeated misses must not re-download the homepage more than 1×/min."""
        mock_scraper._build_id_verified_at = time.monotonic()
        with patch.object(mock_scraper, '_get_build_id',
                          return_value='b1') as gb, \
             patch.object(mock_scraper, '_fetch_api_json',
                          return_value=None) as fj:
            assert mock_scraper._fetch_next_data_payload('/players/1') is None

        assert gb.call_count == 1                 # no refresh within the window
        fj.assert_called_once()

    def test_fetch_match_details_uses_matchdetails_endpoint(self, mock_scraper):
        """Content comes from /api/data/matchDetails, not the _next/data payload.

        Issue #547: FotMob serves a slimmed _next/data static payload for
        archived matches (``content.stats = null``), while the matchDetails API
        still returns the full stats. The fetch must use matchDetails so a
        missed match can be re-scraped with its stats intact.
        """
        header = {'pageUrl': '/matches/everton-vs-newcastle-united/2yo9qd#4813499'}
        match_details = {'content': {
            'stats': {'Periods': {'All': {'stats': [{'key': 'BallPossesion'}]}}},
            'lineup': {'a': 1},
        }}
        with patch.object(mock_scraper, '_fetch_api_json',
                          side_effect=[header, match_details]) as fj, \
             patch.object(mock_scraper, '_fetch_next_data_payload') as nd:
            res = mock_scraper._fetch_match_details(4813499)

        assert res is not None
        assert res['content'] == match_details['content']
        assert res['content']['stats']            # the bug: null via _next/data
        assert res['page_url'] == header['pageUrl']
        nd.assert_not_called()                    # no longer uses _next/data
        endpoints = [c.args[0] for c in fj.call_args_list]
        assert 'matchDetails' in endpoints
        md_call = next(c for c in fj.call_args_list if c.args[0] == 'matchDetails')
        assert md_call.kwargs['params'] == {'matchId': '4813499'}

    def test_read_schedule_parses_data(self, mock_scraper):
        """Test schedule parsing from API response."""
        mock_data = {
            'fixtures': {
                'allMatches': [
                    {
                        'id': '123',
                        'status': {'utcTime': '2024-01-15T15:00:00Z', 'finished': True, 'scoreStr': '2 - 1'},
                        'home': {'name': 'Arsenal', 'id': '9825'},
                        'away': {'name': 'Chelsea', 'id': '8455'},
                        'round': '21',
                        'roundName': 21,
                    },
                    {
                        'id': '124',
                        'status': {'utcTime': '2024-01-16T15:00:00Z', 'finished': False},
                        'home': {'name': 'Liverpool', 'id': '8650'},
                        'away': {'name': 'Man City', 'id': '8456'},
                        'round': '21',
                        'roundName': 21,
                    },
                ]
            }
        }

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_schedule('ENG-Premier League', 2024)

            assert df is not None
            assert len(df) == 2
            assert 'match_id' in df.columns
            assert 'home_team' in df.columns
            assert 'away_team' in df.columns
            assert df.iloc[0]['home_team'] == 'Arsenal'
            assert df.iloc[0]['home_score'] == 2
            assert df.iloc[0]['is_finished'] == True

    def test_read_schedule_no_league(self, mock_scraper):
        """Test schedule with unknown league."""
        mock_scraper.leagues = []
        mock_scraper.seasons = []

        df = mock_scraper.read_schedule(None, None)

        assert df is None

    def test_read_team_stats_parses_data(self, mock_scraper):
        """Test team stats parsing from API response."""
        mock_data = {
            'table': [
                {
                    'data': {
                        'table': {
                            'all': [
                                {
                                    'id': '9825', 'name': 'Arsenal', 'idx': 1,
                                    'played': 20, 'wins': 15, 'draws': 3, 'losses': 2,
                                    'scoresStr': '45-12', 'goalConDiff': 33, 'pts': 48,
                                },
                                {
                                    'id': '8650', 'name': 'Liverpool', 'idx': 2,
                                    'played': 20, 'wins': 14, 'draws': 4, 'losses': 2,
                                    'scoresStr': '42-15', 'goalConDiff': 27, 'pts': 46,
                                },
                            ]
                        }
                    }
                }
            ]
        }

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_team_season_stats('ENG-Premier League', 2024)

            assert df is not None
            assert len(df) == 2
            assert 'team_name' in df.columns
            assert 'points' in df.columns
            assert df.iloc[0]['team_name'] == 'Arsenal'
            assert df.iloc[0]['points'] == 48

    def test_read_player_stats_parses_data(self, mock_scraper):
        """player_stats walks stats.players[].fetchAllUrl -> TopLists[0].StatList[].

        NB: FotMob misspells the player id key as 'ParticiantId' (no 2nd 'p').
        """
        league_data = {'stats': {'players': [
            {'fetchAllUrl': 'https://data.fotmob.com/stats/47/season/27110/goals.json',
             'header': 'Top scorer', 'name': 'goals', 'category': 'Top Stat'},
        ]}}
        toplist = {'TopLists': [{
            'Title': 'Top scorer', 'StatName': 'goals', 'Category': 'Top Stat',
            'StatList': [
                {'ParticiantId': 1001, 'ParticipantName': 'Erling Haaland',
                 'TeamId': 8456, 'TeamName': 'Manchester City',
                 'ParticipantCountryCode': 'NOR', 'Rank': 1, 'StatValue': 20,
                 'SubStatValue': 3.0, 'StatValueCount': 18,
                 'MatchesPlayed': 30, 'MinutesPlayed': 2600},
            ],
        }]}

        with patch.object(mock_scraper, '_get_league_data', return_value=league_data):
            with patch.object(mock_scraper, '_fetch_api_json', return_value=toplist):
                df = mock_scraper.read_player_season_stats('goals', 'ENG-Premier League', 2024)

            assert df is not None
            assert len(df) == 1
            row = df.iloc[0]
            assert row['participant_id'] == 1001
            assert row['participant_name'] == 'Erling Haaland'
            assert row['minutes_played'] == 2600
            assert row['matches_played'] == 30
            assert row['stat_name'] == 'goals'
            assert row['stat_value'] == 20

    # ---------------------------------------------------------------- #
    # New entities (issue #99)
    # ---------------------------------------------------------------- #

    @pytest.fixture
    def team_payload(self):
        """Minimal /api/data/teams payload covering profile + squad."""
        return {
            'details': {'id': 9825, 'name': 'Arsenal', 'shortName': 'Arsenal', 'country': 'ENG'},
            'overview': {
                'season': '2025/2026',
                'venue': {
                    'widget': {'name': 'Emirates Stadium',
                               'location': ['51.5549', '-0.1084'],
                               'city': 'London'},
                    # statPairs = list of [label, value] pairs (#750).
                    'statPairs': [['Surface', 'Grass'],
                                  ['Capacity', 60704],
                                  ['Opened', 2006]],
                },
                'table': [{'data': {'table': {'all': [{'id': 9825, 'idx': 2}]}}}],
                'nextMatch': {'id': 1},
                'lastMatch': {'id': 2},
            },
            'history': {'tables': {'current': [{}], 'historic': [{}, {}, {}]}},
            'squad': {
                'squad': [
                    {'title': 'coach', 'members': [
                        {'id': 111, 'name': 'Mikel Arteta', 'role': {'key': 'coach', 'fallback': 'Coach'}},
                    ]},
                    {'title': 'keepers', 'members': [
                        {'id': 562727, 'name': 'David Raya', 'shirtNumber': 1, 'positionId': 0,
                         'ccode': 'ESP', 'cname': 'Spain', 'role': {'key': 'keeper_long', 'fallback': 'Keeper'},
                         'injury': None, 'rating': 7.2, 'goals': 0, 'assists': 0, 'penalties': 0,
                         'rcards': None, 'ycards': 2, 'height': 183, 'age': 30,
                         'dateOfBirth': '1995-09-15', 'excludeFromRanking': False},
                    ]},
                ]
            },
        }

    def test_read_team_profile(self, mock_scraper, team_payload):
        """team_profile maps details/overview/history to flat columns."""
        with patch.object(mock_scraper, '_team_ids_for_league', return_value=[9825]):
            with patch.object(mock_scraper, '_get_team_data', return_value=team_payload):
                df = mock_scraper.read_team_profile('ENG-Premier League', 2025)

        assert df is not None and len(df) == 1
        row = df.iloc[0]
        assert row['team_id'] == 9825
        assert row['team_name'] == 'Arsenal'
        assert row['venue'] == 'Emirates Stadium'
        # #719: stadium coords kept raw (strings) from widget.location.
        assert row['venue_latitude'] == '51.5549'
        assert row['venue_longitude'] == '-0.1084'
        # #750: widget.city + statPairs (surface/capacity/opened), kept raw (str).
        assert row['venue_city'] == 'London'
        assert row['venue_surface'] == 'Grass'
        assert row['venue_capacity'] == '60704'
        assert row['venue_opened'] == '2006'
        assert row['overview_season'] == '2025/2026'
        assert row['overview_table_position'] == '2'
        assert row['history_seasons_count'] == '3'
        assert '"id": 1' in row['next_match']

    def test_read_team_profile_without_location(self, mock_scraper, team_payload):
        """#719: widget without 'location' → coords None (no crash)."""
        team_payload['overview']['venue']['widget'].pop('location')
        with patch.object(mock_scraper, '_team_ids_for_league', return_value=[9825]):
            with patch.object(mock_scraper, '_get_team_data', return_value=team_payload):
                df = mock_scraper.read_team_profile('ENG-Premier League', 2025)

        row = df.iloc[0]
        assert row['venue'] == 'Emirates Stadium'
        assert row['venue_latitude'] is None
        assert row['venue_longitude'] is None

    def test_read_team_profile_without_stat_pairs(self, mock_scraper, team_payload):
        """#750: venue without statPairs / city → those fields None (no crash)."""
        team_payload['overview']['venue']['widget'].pop('city')
        team_payload['overview']['venue'].pop('statPairs')
        with patch.object(mock_scraper, '_team_ids_for_league', return_value=[9825]):
            with patch.object(mock_scraper, '_get_team_data', return_value=team_payload):
                df = mock_scraper.read_team_profile('ENG-Premier League', 2025)

        row = df.iloc[0]
        assert row['venue'] == 'Emirates Stadium'
        assert row['venue_city'] is None
        assert row['venue_surface'] is None
        assert row['venue_capacity'] is None
        assert row['venue_opened'] is None

    def test_read_team_squad(self, mock_scraper, team_payload):
        """team_squad flattens member.role and section title; rcards/ycards."""
        with patch.object(mock_scraper, '_team_ids_for_league', return_value=[9825]):
            with patch.object(mock_scraper, '_get_team_data', return_value=team_payload):
                df = mock_scraper.read_team_squad('ENG-Premier League', 2025)

        assert df is not None and len(df) == 2  # coach + 1 keeper
        keeper = df[df['player_id'] == 562727].iloc[0]
        assert keeper['role'] == 'keepers'                      # section title
        assert keeper['position_label_key'] == 'keeper_long'    # member.role.key
        assert keeper['position_label_fallback'] == 'Keeper'
        assert keeper['yellow_cards'] == 2
        assert keeper['red_cards'] is None or pd.isna(keeper['red_cards'])
        assert keeper['country'] == 'Spain'
        assert keeper['height_cm'] == 183
        assert keeper['injury_text'] is None

    def test_read_team_leaderboards(self, mock_scraper):
        """team_leaderboards walks fetchAllUrl -> TopLists[0].StatList[]."""
        league_data = {'stats': {'teams': [
            {'fetchAllUrl': 'https://data.fotmob.com/stats/47/season/27110/rating_team.json',
             'header': 'FotMob rating', 'name': 'rating_team', 'category': 'general'},
        ]}}
        toplist = {'TopLists': [{
            'Title': 'FotMob rating', 'StatName': 'rating_team', 'Category': 'general',
            'StatList': [
                {'ParticipantName': 'Arsenal', 'TeamId': 9825, 'TeamColor': '#bd0510',
                 'ParticipantCountryCode': 'ENG', 'Rank': 1, 'StatValue': 7.2,
                 'SubStatValue': 0.0, 'StatValueCount': 1, 'MatchesPlayed': 37, 'MinutesPlayed': 3330},
            ],
        }]}

        with patch.object(mock_scraper, '_get_league_data', return_value=league_data):
            with patch.object(mock_scraper, '_fetch_api_json', return_value=toplist):
                df = mock_scraper.read_team_leaderboards('ENG-Premier League', 2025)

        assert df is not None and len(df) == 1
        row = df.iloc[0]
        assert row['participant_name'] == 'Arsenal'
        assert row['team_id'] == 9825
        assert row['country_code'] == 'ENG'
        assert row['stat_category_header'] == 'FotMob rating'
        assert row['stat_name'] == 'rating_team'
        assert row['team_name'] is None or pd.isna(row['team_name'])  # no TeamName key

    def test_read_transfers(self, mock_scraper):
        """transfers flattens position/transferType, picks euro estimate fee."""
        payload = {'transfers': [
            {'playerId': 1072817, 'name': 'Dominik Kother',
             'position': {'label': 'LW', 'key': 'leftwinger_short'},
             'transferDate': '2026-05-29T11:21:16Z',
             'fromClub': 'Dynamo Dresden', 'fromClubFullName': 'SG Dynamo Dresden', 'fromClubId': 8480,
             'toClub': 'MSV Duisburg', 'toClubFullName': 'MSV Duisburg', 'toClubId': 8293,
             'fee': None, 'amountEuroEstimated': None, 'marketValue': 362217,
             'onLoan': False, 'transferType': {'text': 'contract', 'localizationKey': 'contract'}},
        ]}

        with patch.object(mock_scraper, '_fetch_api_json', return_value=payload):
            df = mock_scraper.read_transfers('ENG-Premier League', 2025)

        assert df is not None and len(df) == 1
        row = df.iloc[0]
        assert row['player_id'] == 1072817
        assert row['position_label'] == 'LW'
        assert row['position_key'] == 'leftwinger_short'
        assert row['transfer_type_key'] == 'contract'
        assert row['transfer_type_text'] == 'contract'
        assert row['market_value'] == '362217'
        assert row['on_loan'] == False

    def test_read_match_details(self, mock_scraper):
        """match_details maps a finished match's _next/data content to JSON cols."""
        league_data = {'fixtures': {'allMatches': [
            {'id': 4813374, 'status': {'finished': True, 'utcTime': '2025-08-10T12:00:00Z',
                                       'scoreStr': '3 - 1', 'reason': {'long': 'Full-Time'}},
             'home': {'name': 'Liverpool', 'id': 8650},
             'away': {'name': 'Bournemouth', 'id': 8678}},
            {'id': 999, 'status': {'finished': False}},  # skipped
        ]}}
        details = {
            'page_url': '/matches/liverpool-vs-afc-bournemouth/2he69q#4813374',
            'content': {
                'lineup': {'a': 1}, 'matchFacts': {'events': [{'e': 1}], 'x': 2},
                'stats': {'s': 1}, 'playerStats': {'p': 1}, 'shotmap': {'sm': 1},
                'h2h': {'h': 1}, 'momentum': {'m': 1},
            },
        }

        with patch.object(mock_scraper, '_get_league_data', return_value=league_data):
            with patch.object(mock_scraper, '_fetch_match_details', return_value=details):
                df = mock_scraper.read_match_details('ENG-Premier League', 2025)

        assert df is not None and len(df) == 1   # only the finished match
        row = df.iloc[0]
        assert row['match_id'] == 4813374
        assert row['home_team'] == 'Liverpool'
        assert row['home_score'] == 3
        assert row['status'] == 'Full-Time'
        assert row['lineup_json'] == '{"a": 1}'
        assert row['events_json'] == '[{"e": 1}]'
        assert row['page_url'].startswith('/matches/')

    def test_read_player_details(self, mock_scraper):
        """player_details maps pageProps.data scalars + JSON columns."""
        payload = {'pageProps': {'data': {
            'id': 24011, 'name': 'David Raya', 'birthDate': '1995-09-15T00:00:00.000Z',
            'isCoach': False, 'isCaptain': False, 'gender': 'male',
            'primaryTeam': {'teamId': 9825, 'teamName': 'Arsenal'},
            'positionDescription': {'primaryPosition': {'label': 'Goalkeeper'},
                                    'positions': [{'occurences': 5}]},
            'contractEnd': {'utcTime': '2028-06-30T00:00:00.000Z', 'timezone': 'UTC'},
            'mainLeague': {'leagueId': 47, 'leagueName': 'Premier League'},
            'playerInformation': [{'title': 'Height'}], 'careerHistory': [{'team': 'x'}],
            'statSeasons': [{'s': 1}], 'marketValues': {'mv': 1}, 'trophies': [{'t': 1}],
            'recentMatches': [{'r': 1}], 'traits': {'tr': 1}, 'meta': {'me': 1},
            'coachStats': None, 'nextMatch': {'n': 1}, 'firstSeasonStats': {'f': 1},
            'injuryInformation': None,
        }}}

        with patch.object(mock_scraper, '_player_ids_for_league', return_value=[24011]):
            with patch.object(mock_scraper, '_fetch_next_data_payload', return_value=payload):
                df = mock_scraper.read_player_details('ENG-Premier League', 2025)

        assert df is not None and len(df) == 1
        row = df.iloc[0]
        assert row['player_id'] == 24011
        assert row['primary_team_id'] == 9825
        assert row['primary_team_name'] == 'Arsenal'
        assert row['main_league_id'] == 47
        # object-valued API fields extracted to clean scalars
        assert row['contract_end'] == '2028-06-30T00:00:00.000Z'
        assert row['position_description'] == 'Goalkeeper'
        assert row['career_history_json'] == '[{"team": "x"}]'
        assert row['coach_stats_json'] is None

    def test_scrape_all_combines_data(self, mock_scraper):
        """scrape_all returns table-name-keyed paths for entities with frames."""
        mock_df = pd.DataFrame({
            'a': [1], 'league': ['ENG-Premier League'], 'season': [2025],
        })

        # 3 league-level entities return frames; the 6 detail entities return None.
        with patch.object(mock_scraper, 'read_schedule', return_value=mock_df), \
             patch.object(mock_scraper, 'read_team_season_stats', return_value=mock_df), \
             patch.object(mock_scraper, 'read_player_season_stats', return_value=mock_df), \
             patch.object(mock_scraper, 'read_team_profile', return_value=None), \
             patch.object(mock_scraper, 'read_team_squad', return_value=None), \
             patch.object(mock_scraper, 'read_team_leaderboards', return_value=None), \
             patch.object(mock_scraper, 'read_transfers', return_value=None), \
             patch.object(mock_scraper, 'read_match_details', return_value=None), \
             patch.object(mock_scraper, 'read_player_details', return_value=None), \
             patch.object(mock_scraper, 'save_to_iceberg', return_value='iceberg.bronze.test'):
            results = mock_scraper.scrape_all()

        assert 'fotmob_schedule' in results
        assert 'fotmob_team_stats' in results
        assert 'fotmob_player_stats' in results

    def test_metadata_added(self, mock_scraper):
        """Test that metadata is added to DataFrames."""
        mock_data = {
            'fixtures': {
                'allMatches': [
                    {
                        'id': '123',
                        'status': {'utcTime': '2024-01-15T15:00:00Z', 'finished': True, 'scoreStr': '1 - 0'},
                        'home': {'name': 'Arsenal', 'id': '9825'},
                        'away': {'name': 'Chelsea', 'id': '8455'},
                        'round': '1',
                    },
                ]
            }
        }

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_schedule('ENG-Premier League', 2024)

            assert '_source' in df.columns
            assert '_entity_type' in df.columns
            assert '_ingested_at' in df.columns
            assert df.iloc[0]['_source'] == 'fotmob'

    def test_empty_response_handling(self, mock_scraper):
        """Test handling of empty API response."""
        with patch.object(mock_scraper, '_get_league_data', return_value=None):
            df = mock_scraper.read_schedule('ENG-Premier League', 2024)
            assert df is None

    def test_empty_matches_handling(self, mock_scraper):
        """Test handling of empty matches in response."""
        mock_data = {'fixtures': {'allMatches': []}}

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_schedule('ENG-Premier League', 2024)
            assert df is None


class TestFotMobLeagueMapping:
    """Test league ID mappings."""

    def test_major_leagues_present(self):
        """Test that all major leagues are mapped."""
        from scrapers.fotmob import FotMobScraper

        major_leagues = [
            'ENG-Premier League',
            'ESP-La Liga',
            'GER-Bundesliga',
            'ITA-Serie A',
            'FRA-Ligue 1',
        ]

        for league in major_leagues:
            assert league in FotMobScraper.LEAGUE_IDS
            assert FotMobScraper.LEAGUE_IDS[league] is not None
