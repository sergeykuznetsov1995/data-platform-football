"""
Tests for SofaScoreScraper.
"""

import pytest
from unittest.mock import MagicMock, patch


class TestSofaScoreScraper:
    """Tests for SofaScoreScraper."""

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

    @pytest.fixture
    def scraper(self, mock_dependencies):
        """Create SofaScoreScraper instance."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            from scrapers.sofascore import SofaScoreScraper
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2024])

    def test_init(self, scraper):
        """Test SofaScoreScraper initialization."""
        assert scraper.SOURCE_NAME == 'sofascore'

    def test_rate_limit(self, scraper):
        """Test SofaScore rate limit."""
        assert scraper.DEFAULT_RATE_LIMIT == 20


class TestShotmapFlatten:
    """Pure-function tests for the shotmap payload flattener (#22)."""

    def _payload(self):
        """Minimal but realistic SofaScore shotmap response."""
        return {
            'shotmap': [
                {
                    'id': 101,
                    'player': {'id': 11111, 'name': 'Player A'},
                    'teamId': 1,
                    'isHome': True,
                    'time': 12,
                    'addedTime': 0,
                    'reversedPeriodCount': 1,
                    'shotType': 'rightFoot',
                    'situation': 'open-play',
                    'bodyPart': 'rightFoot',
                    'incidentType': 'goal',
                    'goalType': 'regular',
                    'playerCoordinates': {'x': 88.5, 'y': 50.0},
                    'goalMouthCoordinates': {'x': 100.0, 'y': 52.3},
                    'xg': 0.72,
                    'xgot': 0.84,
                },
                {
                    'id': 102,
                    'player': {'id': 22222, 'name': 'Player B'},
                    'teamId': 2,
                    'isHome': False,
                    'time': 45,
                    'addedTime': 2,
                    'reversedPeriodCount': 1,
                    'shotType': 'header',
                    'situation': 'corner',
                    'bodyPart': 'head',
                    'incidentType': 'save',
                    'goalType': None,
                    'playerCoordinates': {'x': 92.1, 'y': 48.5},
                    'goalMouthCoordinates': {'x': 100.0, 'y': 51.2},
                    'xg': 0.11,
                    'xgot': None,
                },
                # Missing id → composite fallback
                {
                    'player': {'id': 33333},
                    'teamId': 1,
                    'isHome': True,
                    'time': 78,
                    'shotType': 'leftFoot',
                    'incidentType': 'miss',
                    'xg': 0.04,
                },
            ]
        }

    def test_flatten_happy_path(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        rows = SofaScoreScraper._flatten_shotmap('14023925', self._payload())
        assert len(rows) == 3

        goal = rows[0]
        assert goal['match_id'] == '14023925'
        assert goal['shot_id'] == '101'
        assert goal['player_id'] == '11111'
        assert goal['team_id'] == 1
        assert goal['is_home'] is True
        assert goal['minute'] == 12
        assert goal['period'] == 1
        assert goal['shot_type'] == 'rightFoot'
        assert goal['situation'] == 'open-play'
        assert goal['body_part'] == 'rightFoot'
        assert goal['outcome'] == 'goal'
        assert goal['goal_type'] == 'regular'
        assert goal['x'] == 88.5
        assert goal['y'] == 50.0
        assert goal['xg'] == 0.72
        assert goal['xgot'] == 0.84

    def test_flatten_missing_id_falls_back_to_composite(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        rows = SofaScoreScraper._flatten_shotmap('14023925', self._payload())
        third = rows[2]
        # composite: match-time-player-addedTime
        assert third['shot_id'] == '14023925-78-33333-0'
        assert third['player_id'] == '33333'
        assert third['outcome'] == 'miss'
        # xgot absent → None
        assert third['xgot'] is None

    def test_flatten_handles_garbage(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        # Non-dict payload, missing shotmap key, non-list shotmap.
        assert SofaScoreScraper._flatten_shotmap('1', None) == []
        assert SofaScoreScraper._flatten_shotmap('1', {}) == []
        assert SofaScoreScraper._flatten_shotmap('1', {'shotmap': 'oops'}) == []
        assert SofaScoreScraper._flatten_shotmap('1', {'shotmap': [{}]}) == [
            # Empty dict still yields a row with mostly None values + composite shot_id
            {
                'match_id': '1',
                'shot_id': '1-NA-NA-0',
                'player_id': None,
                'team_id': None,
                'is_home': None,
                'minute': None,
                'added_time': None,
                'period': None,
                'shot_type': None,
                'situation': None,
                'body_part': None,
                'outcome': None,
                'goal_type': None,
                'x': None,
                'y': None,
                'goal_x': None,
                'goal_y': None,
                'xg': None,
                'xgot': None,
            }
        ]


class TestCamelToSnake:
    """Sanity tests for the snake_case normalizer used by #21/#23/#24."""

    def test_basic(self):
        from scrapers.sofascore.scraper import _camel_to_snake
        assert _camel_to_snake('goalsPrevented') == 'goals_prevented'
        assert _camel_to_snake('accuratePass') == 'accurate_pass'
        assert _camel_to_snake('expectedAssists') == 'expected_assists'

    def test_consecutive_capitals(self):
        from scrapers.sofascore.scraper import _camel_to_snake
        # XGOnTarget keeps the leading abbreviation intact, splits at the
        # first lowercase boundary.
        assert _camel_to_snake('XGOnTarget') == 'xg_on_target'

    def test_already_snake(self):
        from scrapers.sofascore.scraper import _camel_to_snake
        assert _camel_to_snake('already_snake') == 'already_snake'


class TestEventPlayerStatsFlatten:
    """Tests for the per-(match, player) Opta stats flattener (#21)."""

    def _payload(self):
        # Mirrors the real /event/{id}/player/{pid}/statistics response
        # (verified live 2026-06-05, #301): NO `extra` block and NO
        # `statistics.position` — so is_home/captain/substitute/
        # position_specific cannot be sourced here and stay None. They are
        # back-filled from /lineups by the overlay (tested below).
        return {
            'player': {'id': 11111, 'name': 'Player A'},
            'team': {'id': 1, 'name': 'Team X'},
            'position': 'F',
            'statistics': {
                'rating': '7.8',
                'goalsPrevented': 0.42,
                'accuratePass': 35,
                'totalPass': 40,
                'expectedAssists': {'value': 0.21, 'previousValue': 0.10},
                # Pure dict without 'value' → None
                'noisyStruct': {'foo': 'bar'},
            },
        }

    def test_flatten_happy_path(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        row = SofaScoreScraper._flatten_event_player_stats(
            '14023925', '11111', self._payload(),
        )
        assert row is not None
        assert row['match_id'] == '14023925'
        assert row['player_id'] == '11111'
        assert row['team_id'] == 1
        assert row['team_name'] == 'Team X'
        # No `extra` block in the real statistics payload → these anchors
        # are None from the flattener alone; the lineup overlay fills them.
        assert row['is_home'] is None
        assert row['captain'] is None
        assert row['substitute'] is None
        assert row['position_specific'] is None
        # `position` (top-level) IS returned by the statistics endpoint.
        assert row['position'] == 'F'

        # snake_case auto-flatten
        assert row['rating'] == 7.8
        assert row['goals_prevented'] == 0.42
        assert row['accurate_pass'] == 35
        assert row['total_pass'] == 40
        # struct with `value` → unwrapped
        assert row['expected_assists'] == 0.21
        # struct without `value` → None
        assert row['noisy_struct'] is None

    def test_garbage_payload(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert SofaScoreScraper._flatten_event_player_stats('1', '1', None) is None
        # Empty payload still produces an anchor-only row (no stats).
        row = SofaScoreScraper._flatten_event_player_stats('1', '1', {})
        assert row is not None
        assert row['match_id'] == '1'
        assert row['player_id'] == '1'


class TestEventPlayerStatsFromLineups:
    """Tests for deriving per-(match, player) stats from the captured /lineups
    payload (#751 PR1). Live-verified 2026-06-22: lineups carries the full
    per-player ``statistics`` block (33 Opta metrics) + anchors, so eps no
    longer needs ~25 per-player /player/{pid}/statistics calls per match."""

    def _lineups(self):
        # Mirrors the live /event/{id}/lineups shape (APL 14023959, #751):
        # per-player `statistics` holds the rich Opta block; anchors
        # (position/substitute/captain) come straight off the entry; is_home
        # off the side. `captain` present only on the captain's entry.
        return {
            'home': {'players': [
                {'player': {'id': 11111, 'name': 'Keeper'}, 'position': 'G',
                 'substitute': False, 'captain': True,
                 'statistics': {
                     'rating': '7.8', 'accuratePass': 35, 'totalPass': 40,
                     'expectedAssists': {'value': 0.21, 'previousValue': 0.10},
                     'goalsPrevented': 0.42, 'saves': 3,
                     'noisyStruct': {'foo': 'bar'},
                 }},
            ]},
            'away': {'players': [
                {'player': {'id': 22222, 'name': 'Striker'}, 'position': 'F',
                 'substitute': True,
                 'statistics': {'rating': '6.5', 'totalShots': 2}},
            ]},
        }

    def _event(self):
        # Live /event/{id} nests the event object under "event" (proven
        # 2026-06-22, #751 PR2 — flat access returned NULL team_id in PR1).
        return {'event': {'homeTeam': {'id': 1, 'name': 'Team X'},
                          'awayTeam': {'id': 2, 'name': 'Team Y'}}}

    def test_unwraps_nested_event_for_team_mapping(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        rows = SofaScoreScraper._flatten_event_player_stats_from_lineups(
            '1', self._lineups(), {'event': {
                'homeTeam': {'id': 7, 'name': 'Nested FC'},
                'awayTeam': {'id': 8, 'name': 'Away FC'}}},
        )
        home = next(r for r in rows if r['player_id'] == '11111')
        assert home['team_id'] == 7 and home['team_name'] == 'Nested FC'

    def test_flat_event_still_maps_teams(self):
        # Defensive: a bare (un-nested) event object must still map.
        from scrapers.sofascore.scraper import SofaScoreScraper
        rows = SofaScoreScraper._flatten_event_player_stats_from_lineups(
            '1', self._lineups(),
            {'homeTeam': {'id': 3}, 'awayTeam': {'id': 4}},
        )
        home = next(r for r in rows if r['player_id'] == '11111')
        assert home['team_id'] == 3

    def test_happy_path_home_and_away(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        rows = SofaScoreScraper._flatten_event_player_stats_from_lineups(
            '14023959', self._lineups(), self._event(),
        )
        assert len(rows) == 2
        home = next(r for r in rows if r['player_id'] == '11111')
        away = next(r for r in rows if r['player_id'] == '22222')

        # Anchors — all populated directly from lineups (no overlay needed).
        assert home['match_id'] == '14023959'
        assert home['team_id'] == 1
        assert home['team_name'] == 'Team X'
        assert home['is_home'] is True
        assert home['captain'] is True
        assert home['substitute'] is False
        assert home['position'] == 'G'
        assert home['position_specific'] == 'G'

        assert away['team_id'] == 2
        assert away['is_home'] is False
        # `captain` absent on non-captain entry → False, not None.
        assert away['captain'] is False
        assert away['substitute'] is True

        # Stats auto-flattened (snake_case + struct unwrap), same rules as the
        # dedicated-endpoint flattener.
        assert home['rating'] == 7.8
        assert home['accurate_pass'] == 35
        assert home['total_pass'] == 40
        assert home['expected_assists'] == 0.21
        assert home['goals_prevented'] == 0.42
        assert home['noisy_struct'] is None
        assert away['total_shots'] == 2

    def test_no_event_payload_team_fields_none(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        rows = SofaScoreScraper._flatten_event_player_stats_from_lineups(
            '1', self._lineups(), None,
        )
        assert len(rows) == 2
        assert all(r['team_id'] is None and r['team_name'] is None for r in rows)
        # Anchors that come from lineups itself still populated.
        assert rows[0]['is_home'] in (True, False)

    def test_garbage_payload(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert SofaScoreScraper._flatten_event_player_stats_from_lineups(
            '1', None, None) == []
        assert SofaScoreScraper._flatten_event_player_stats_from_lineups(
            '1', {'home': {'players': None}, 'away': {}}, None) == []


class TestLineupOverlayLookup:
    """Tests for _build_lineup_overlay_lookup — the /lineups projection
    that back-fills is_home/captain/substitute/position_specific (#301)."""

    def _lineups(self):
        # Shape verified live 2026-06-05 (#301): `captain` present only on
        # the captain entry; `substitute` a real bool everywhere;
        # `position` the per-event line.
        return {
            'home': {'players': [
                {'player': {'id': 11111}, 'position': 'G',
                 'substitute': False, 'captain': True},
                {'player': {'id': 22222}, 'position': 'D',
                 'substitute': False},
            ]},
            'away': {'players': [
                {'player': {'id': 33333}, 'position': 'M',
                 'substitute': True},
            ]},
        }

    def test_maps_all_four_fields(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        lookup = SofaScoreScraper._build_lineup_overlay_lookup(self._lineups())

        # int id normalised to str key.
        assert set(lookup) == {'11111', '22222', '33333'}

        # home → is_home True; captain flag promoted to True.
        assert lookup['11111'] == {
            'is_home': True, 'captain': True,
            'substitute': False, 'position_specific': 'G',
        }
        # home, no captain key → captain False (not None).
        assert lookup['22222']['is_home'] is True
        assert lookup['22222']['captain'] is False
        assert lookup['22222']['substitute'] is False
        assert lookup['22222']['position_specific'] == 'D'
        # away → is_home False; bench → substitute True.
        assert lookup['33333']['is_home'] is False
        assert lookup['33333']['substitute'] is True
        assert lookup['33333']['position_specific'] == 'M'

    def test_missing_position_is_none(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        lookup = SofaScoreScraper._build_lineup_overlay_lookup(
            {'home': {'players': [{'player': {'id': 7}, 'substitute': False}]}}
        )
        assert lookup['7']['position_specific'] is None

    def test_player_without_id_skipped(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        lookup = SofaScoreScraper._build_lineup_overlay_lookup(
            {'home': {'players': [{'player': {}, 'position': 'F'}]}}
        )
        assert lookup == {}

    def test_garbage_payload(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert SofaScoreScraper._build_lineup_overlay_lookup(None) == {}
        assert SofaScoreScraper._build_lineup_overlay_lookup({}) == {}
        # Missing/empty sides and non-list players → no raise, empty.
        assert SofaScoreScraper._build_lineup_overlay_lookup(
            {'home': {}, 'away': {'players': None}}
        ) == {}


class TestApplyLineupOverlay:
    """Tests for _apply_lineup_overlay — fill-if-None in-place merge."""

    def test_fills_none_anchors(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        row = {'is_home': None, 'captain': None, 'substitute': None,
               'position_specific': None, 'rating': 7.8}
        SofaScoreScraper._apply_lineup_overlay(row, {
            'is_home': True, 'captain': False,
            'substitute': True, 'position_specific': 'M',
        })
        assert row['is_home'] is True
        assert row['captain'] is False
        assert row['substitute'] is True
        assert row['position_specific'] == 'M'
        assert row['rating'] == 7.8  # untouched

    def test_does_not_overwrite_existing(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        row = {'is_home': False, 'captain': None,
               'substitute': None, 'position_specific': None}
        SofaScoreScraper._apply_lineup_overlay(row, {
            'is_home': True, 'captain': True,
            'substitute': True, 'position_specific': 'M',
        })
        # Primary (already-set) value wins; only None anchors get filled.
        assert row['is_home'] is False
        assert row['captain'] is True

    def test_none_overlay_leaves_row_untouched(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        row = {'is_home': None, 'captain': None,
               'substitute': None, 'position_specific': None}
        SofaScoreScraper._apply_lineup_overlay(row, None)
        assert all(row[c] is None for c in row)

    def test_partial_overlay_fills_subset(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        row = {'is_home': None, 'captain': None,
               'substitute': None, 'position_specific': None}
        SofaScoreScraper._apply_lineup_overlay(
            row, {'captain': True, 'substitute': None,
                  'is_home': None, 'position_specific': None}
        )
        assert row['captain'] is True
        assert row['substitute'] is None
        assert row['is_home'] is None


class TestEventPlayerStatsOverlayWiring:
    """read_event_player_stats fetches /lineups once per match and overlays
    the four anchor fields onto each stat row (#301)."""

    def test_overlay_applied_and_lineup_fetched_once_per_match(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            scraper = SofaScoreScraper()

        # Two players in one match.
        players_by_match = {'M1': ['11111', '33333']}

        def fake_stats(event_id, player_id, max_attempts=3):
            # Real statistics payload: no `extra`, no statistics.position.
            return {
                'player': {'id': int(player_id)},
                'team': {'id': 1, 'name': 'Team X'},
                'position': 'F',
                'statistics': {'rating': '7.0'},
            }

        lineup_calls = []

        def fake_lineup(event_id, max_attempts=3):
            lineup_calls.append(event_id)
            return {
                'home': {'players': [
                    {'player': {'id': 11111}, 'position': 'G',
                     'substitute': False, 'captain': True},
                ]},
                'away': {'players': [
                    {'player': {'id': 33333}, 'position': 'M',
                     'substitute': True},
                ]},
            }

        scraper._fetch_event_player_stats_payload = fake_stats
        scraper._fetch_lineup_payload = fake_lineup

        df = scraper.read_event_player_stats(
            league='ENG-Premier League', season=2025,
            player_ids_by_match=players_by_match,
        )

        # Exactly one lineup fetch for the single match.
        assert lineup_calls == ['M1']

        rows = {r['player_id']: r for r in df.to_dict('records')}
        # Home captain.
        assert rows['11111']['is_home'] is True
        assert rows['11111']['captain'] is True
        assert rows['11111']['substitute'] is False
        assert rows['11111']['position_specific'] == 'G'
        # Away substitute.
        assert rows['33333']['is_home'] is False
        assert rows['33333']['substitute'] is True
        assert rows['33333']['position_specific'] == 'M'


class TestMatchStatsFlatten:
    """Tests for the team-level per-(match, period, stat) flattener (#25)."""

    def _payload(self):
        return {
            'statistics': [
                {
                    'period': 'ALL',
                    'groups': [
                        {
                            'groupName': 'Possession',
                            'statisticsItems': [
                                {
                                    'name': 'Ball possession',
                                    'key': 'ballPossession',
                                    'home': '55%',
                                    'away': '45%',
                                    'homeValue': 55,
                                    'awayValue': 45,
                                    'compareCode': 1,
                                    'valueType': 'percent',
                                },
                            ],
                        },
                        {
                            'groupName': 'Shots',
                            'statisticsItems': [
                                {
                                    'name': 'Total shots',
                                    'key': 'totalShotsOnGoal',
                                    'home': '14',
                                    'away': '7',
                                    'homeValue': 14,
                                    'awayValue': 7,
                                    'compareCode': 1,
                                    'valueType': 'count',
                                },
                                {
                                    'name': 'Expected goals',
                                    'key': 'expectedGoals',
                                    'home': '1.8',
                                    'away': '0.6',
                                    'homeValue': 1.8,
                                    'awayValue': 0.6,
                                    'compareCode': 1,
                                    'valueType': 'decimal',
                                },
                            ],
                        },
                    ],
                },
                {
                    'period': '1ST',
                    'groups': [
                        {
                            'groupName': 'Possession',
                            'statisticsItems': [
                                {
                                    'name': 'Ball possession',
                                    'key': 'ballPossession',
                                    'home': '58%',
                                    'away': '42%',
                                    'homeValue': 58,
                                    'awayValue': 42,
                                    'compareCode': 1,
                                    'valueType': 'percent',
                                },
                            ],
                        },
                    ],
                },
            ]
        }

    def test_flatten_happy_path(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        rows = SofaScoreScraper._flatten_match_stats('14023925', self._payload())
        # 3 in ALL (1 possession + 2 shots) + 1 in 1ST = 4
        assert len(rows) == 4

        bp_all = next(
            r for r in rows
            if r['period'] == 'ALL' and r['stat_name'] == 'Ball possession'
        )
        assert bp_all['match_id'] == '14023925'
        assert bp_all['stat_group'] == 'Possession'
        assert bp_all['stat_key'] == 'ballPossession'
        assert bp_all['home_value'] == 55.0
        assert bp_all['away_value'] == 45.0
        assert bp_all['home_text'] == '55%'
        assert bp_all['away_text'] == '45%'

        xg = next(r for r in rows if r['stat_name'] == 'Expected goals')
        assert xg['home_value'] == 1.8
        assert xg['away_value'] == 0.6

        bp_1st = next(
            r for r in rows
            if r['period'] == '1ST' and r['stat_name'] == 'Ball possession'
        )
        assert bp_1st['home_value'] == 58.0

    def test_flatten_handles_garbage(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert SofaScoreScraper._flatten_match_stats('1', None) == []
        assert SofaScoreScraper._flatten_match_stats('1', {}) == []
        assert SofaScoreScraper._flatten_match_stats('1', {'statistics': 'oops'}) == []
        # Period with no groups → no rows.
        assert SofaScoreScraper._flatten_match_stats(
            '1', {'statistics': [{'period': 'ALL'}]}
        ) == []


class TestPlayerSeasonStatsFlatten:
    """Tests for the per-(player, season) flattener (#24)."""

    def test_happy_path(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        payload = {
            'team': {'id': 1, 'name': 'Team X'},
            'statistics': {
                'rating': 7.42,
                'totalGoals': 12,
                'totalAssists': 5,
                'expectedGoals': 10.8,
                'minutesPlayed': 2800,
            },
        }
        row = SofaScoreScraper._flatten_player_season_stats(
            '11111', 17, 76986, payload,
        )
        assert row is not None
        assert row['player_id'] == '11111'
        assert row['unique_tournament_id'] == 17
        assert row['sofascore_season_id'] == 76986
        assert row['team_id'] == 1
        assert row['team_name'] == 'Team X'
        assert row['rating'] == 7.42
        assert row['total_goals'] == 12
        assert row['total_assists'] == 5
        assert row['expected_goals'] == 10.8
        assert row['minutes_played'] == 2800

    def test_garbage(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert (
            SofaScoreScraper._flatten_player_season_stats('1', 17, 1, None)
            is None
        )
        row = SofaScoreScraper._flatten_player_season_stats('1', 17, 1, {})
        assert row is not None
        assert row['player_id'] == '1'


class TestSofaScoreTournamentMap:
    """Sanity for the league → unique_tournament_id static map (#24)."""

    def test_known_leagues(self):
        from scrapers.sofascore.scraper import SOFASCORE_TOURNAMENT_MAP
        # Premier League is the canonical reference (APL probe #19).
        assert SOFASCORE_TOURNAMENT_MAP['ENG-Premier League'] == 17
        # Other Big 5 leagues should be present.
        assert 'ESP-La Liga' in SOFASCORE_TOURNAMENT_MAP
        assert 'GER-Bundesliga' in SOFASCORE_TOURNAMENT_MAP
        assert 'ITA-Serie A' in SOFASCORE_TOURNAMENT_MAP
        assert 'FRA-Ligue 1' in SOFASCORE_TOURNAMENT_MAP


class TestPlayerProfileFlatten:
    """Tests for the per-player biographical snapshot flattener (#23)."""

    def test_happy_path(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        # dateOfBirthTimestamp = 1990-01-01 00:00 UTC = 631152000
        payload = {
            'player': {
                'id': 11111,
                'name': 'John Doe',
                'shortName': 'J. Doe',
                'slug': 'john-doe',
                'position': 'F',
                'jerseyNumber': '9',
                'shirtNumber': 9,
                'height': 182,
                'preferredFoot': 'Right',
                'dateOfBirthTimestamp': 631152000,
                'nationality': 'England',
                'country': {'name': 'England', 'alpha2': 'EN'},
                'team': {'id': 1, 'name': 'Team X'},
                'retired': False,
            }
        }
        row = SofaScoreScraper._flatten_player_profile(payload)
        assert row is not None
        assert row['player_id'] == '11111'
        assert row['name'] == 'John Doe'
        assert row['short_name'] == 'J. Doe'
        assert row['slug'] == 'john-doe'
        assert row['position'] == 'F'
        assert row['shirt_number'] == 9
        assert row['height_cm'] == 182
        assert row['preferred_foot'] == 'Right'
        assert row['date_of_birth'] == '1990-01-01'
        assert row['nationality'] == 'England'
        assert row['country_code'] == 'EN'
        assert row['current_team_id'] == 1
        assert row['current_team_name'] == 'Team X'
        assert row['retired'] is False

    def test_garbage(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert SofaScoreScraper._flatten_player_profile(None) is None
        assert SofaScoreScraper._flatten_player_profile({}) is None
        # No player.id → None
        assert SofaScoreScraper._flatten_player_profile({'player': {'name': 'X'}}) is None

    def test_dob_fallback_when_timestamp_invalid(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        payload = {
            'player': {'id': 1, 'dateOfBirthTimestamp': None},
        }
        row = SofaScoreScraper._flatten_player_profile(payload)
        assert row is not None
        assert row['date_of_birth'] is None

    def test_country_fallback_for_nationality(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        payload = {
            'player': {
                'id': 1,
                'country': {'name': 'Brazil', 'alpha2': 'BR'},
            }
        }
        row = SofaScoreScraper._flatten_player_profile(payload)
        assert row['nationality'] == 'Brazil'
        assert row['country_code'] == 'BR'


class TestNoShadowedPlayerStats:
    """#470 bug 7: a no-arg read_player_season_stats(self) stub shadowed the
    real (league, season) method, and an unreachable scrape_player_stats()
    called it with no args (latent TypeError) and without replace_partitions.
    Both were removed — guard against their return."""

    def test_no_scrape_player_stats_method(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert not hasattr(SofaScoreScraper, 'scrape_player_stats')

    def test_single_read_player_season_stats_with_args(self):
        import inspect
        from scrapers.sofascore import scraper as sofascore_module
        from scrapers.sofascore.scraper import SofaScoreScraper

        src = inspect.getsource(sofascore_module)
        assert src.count('def read_player_season_stats(') == 1, (
            "exactly one read_player_season_stats definition — the no-arg stub "
            "must stay removed so it can't shadow the real method"
        )
        params = inspect.signature(
            SofaScoreScraper.read_player_season_stats
        ).parameters
        assert 'league' in params and 'season' in params


class TestReadPlayerRatingsCapture:
    """read_player_ratings sources /lineups from the Camoufox capture
    transport (#757) — the tls_requests REST path is Turnstile-blocked.
    We patch the ``_iter_lineup_payloads`` seam so no browser is needed;
    the per-side flatten + (league, season) tagging is what we assert here.
    """

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])

    def _lineups(self):
        return {
            'home': {'players': [
                {'player': {'id': 11111}, 'position': 'G',
                 'statistics': {'rating': '7.2'}},
            ]},
            'away': {'players': [
                {'player': {'id': 22222}, 'position': 'F',
                 'statistics': {'rating': '6.5'}},
            ]},
        }

    @pytest.mark.unit
    def test_builds_rows_from_captured_lineups(self):
        # Arrange
        scraper = self._scraper()
        captured = []

        def fake_iter(match_ids):
            for mid in match_ids:
                captured.append(str(mid))
                yield str(mid), self._lineups()

        scraper._iter_lineup_payloads = fake_iter

        # Act
        df = scraper.read_player_ratings(
            league='ENG-Premier League', season=2025, match_ids=['M1', 'M2'],
        )

        # Assert — one capture per match in order, 2 matches × 2 players.
        assert captured == ['M1', 'M2']
        assert len(df) == 4
        rows = {(r['match_id'], r['player_id']): r for r in df.to_dict('records')}
        assert rows[('M1', '11111')]['team_side'] == 'home'
        assert rows[('M1', '11111')]['rating'] == 7.2
        assert rows[('M1', '11111')]['position'] == 'G'
        assert rows[('M1', '22222')]['team_side'] == 'away'
        assert rows[('M1', '22222')]['rating'] == 6.5
        # 2025 -> soccerdata short slug '2526' (partition key alignment, #27).
        assert set(df['season']) == {'2526'}
        assert set(df['league']) == {'ENG-Premier League'}
        assert set(df['_entity_type']) == {'player_ratings'}

    @pytest.mark.unit
    def test_graceful_empty_when_no_lineups_captured(self):
        # Arrange — every capture misses (Turnstile not solved / proxy dead).
        scraper = self._scraper()

        def fake_iter(match_ids):
            for mid in match_ids:
                yield str(mid), None

        scraper._iter_lineup_payloads = fake_iter

        # Act
        df = scraper.read_player_ratings(
            league='ENG-Premier League', season=2025, match_ids=['M1'],
        )

        # Assert — empty frame but column contract preserved (E4.4 stub path).
        assert df.empty
        assert 'match_id' in df.columns
        assert 'rating' in df.columns


class TestReadMatchCapture:
    """read_match_capture (#751 PR1): ONE Camoufox capture pass per match →
    both player_ratings and event_player_stats frames. We patch the
    ``_iter_match_captures`` seam (no browser); per-side ratings + lineups-
    derived eps + team mapping from /event + (league, season) tagging is what
    we assert."""

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])

    def _endpoints(self):
        return {
            'lineups': {
                'home': {'players': [
                    {'player': {'id': 11111}, 'position': 'G',
                     'substitute': False, 'captain': True,
                     'statistics': {'rating': '7.2', 'totalPass': 30}},
                ]},
                'away': {'players': [
                    {'player': {'id': 22222}, 'position': 'F',
                     'substitute': False,
                     'statistics': {'rating': '6.5', 'totalShots': 3}},
                ]},
            },
            # Live /event/{id} nests the event under "event" (#751 PR2).
            'event': {'event': {'homeTeam': {'id': 1, 'name': 'Home FC'},
                                'awayTeam': {'id': 2, 'name': 'Away FC'}}},
            # #751 PR2 — same nav also captures statistics + shotmap.
            'statistics': {'statistics': [
                {'period': 'ALL', 'groups': [
                    {'groupName': 'Possession', 'statisticsItems': [
                        {'name': 'Ball possession', 'key': 'ballPossession',
                         'home': '55%', 'away': '45%',
                         'homeValue': 55, 'awayValue': 45,
                         'compareCode': 1, 'valueType': 'team'},
                    ]},
                ]},
            ]},
            'shotmap': {'shotmap': [
                {'id': 9001, 'player': {'id': 11111}, 'teamId': 1,
                 'isHome': True, 'time': 23, 'incidentType': 'goal',
                 'shotType': 'rightFoot', 'situation': 'open-play',
                 'playerCoordinates': {'x': 90, 'y': 50},
                 'goalMouthCoordinates': {'x': 100, 'y': 50}, 'xg': 0.45},
            ]},
        }

    @pytest.mark.unit
    def test_one_pass_yields_both_frames(self):
        scraper = self._scraper()
        seen = []

        def fake_iter(match_ids, **kwargs):
            for mid in match_ids:
                seen.append(str(mid))
                yield str(mid), self._endpoints()

        scraper._iter_match_captures = fake_iter

        out = scraper.read_match_capture(
            league='ENG-Premier League', season=2025, match_ids=['M1', 'M2'],
        )
        # ONE capture per match (not one per table) — the "one nav" contract.
        assert seen == ['M1', 'M2']

        ratings = out['player_ratings']
        eps = out['event_player_stats']
        assert len(ratings) == 4   # 2 matches × 2 players
        assert len(eps) == 4
        assert set(ratings['season']) == {'2526'}
        assert set(eps['season']) == {'2526'}
        assert set(eps['_entity_type']) == {'event_player_stats'}

        erows = {(r['match_id'], r['player_id']): r for r in eps.to_dict('records')}
        home = erows[('M1', '11111')]
        away = erows[('M1', '22222')]
        # team mapping from the captured /event payload.
        assert home['team_id'] == 1 and home['team_name'] == 'Home FC'
        assert away['team_id'] == 2
        # anchors straight from lineups (no overlay needed).
        assert home['is_home'] and home['captain']
        assert not away['is_home']
        assert home['total_pass'] == 30
        assert away['total_shots'] == 3

    @pytest.mark.unit
    def test_one_pass_yields_match_stats_and_shotmap(self):
        # #751 PR2 — the SAME capture pass also materialises match_stats +
        # event_shotmap from the statistics/shotmap endpoints.
        scraper = self._scraper()

        def fake_iter(match_ids, **kwargs):
            for mid in match_ids:
                yield str(mid), self._endpoints()

        scraper._iter_match_captures = fake_iter

        out = scraper.read_match_capture(
            league='ENG-Premier League', season=2025, match_ids=['M1', 'M2'],
        )

        ms = out['match_stats']
        sm = out['event_shotmap']
        assert len(ms) == 2   # 2 matches × 1 stat item
        assert len(sm) == 2   # 2 matches × 1 shot
        assert set(ms['season']) == {'2526'}
        assert set(sm['season']) == {'2526'}
        assert set(ms['_entity_type']) == {'match_stats'}
        assert set(sm['_entity_type']) == {'event_shotmap'}
        assert set(ms['stat_name']) == {'Ball possession'}
        assert set(sm['xg']) == {0.45}

    @pytest.mark.unit
    def test_requests_all_tabs_and_event(self):
        # The "one nav" contract clicks ALL deep tabs and requires event so
        # team_id is populated — assert _iter_match_captures is invoked so.
        scraper = self._scraper()
        captured_kwargs = {}

        def fake_iter(match_ids, **kwargs):
            captured_kwargs.update(kwargs)
            for mid in match_ids:
                yield str(mid), self._endpoints()

        scraper._iter_match_captures = fake_iter
        scraper.read_match_capture(
            league='ENG-Premier League', season=2025, match_ids=['M1'],
        )
        assert 'Statistics' in captured_kwargs['tabs']
        assert 'Shotmap' in captured_kwargs['tabs']
        assert 'event' in captured_kwargs['required']

    @pytest.mark.unit
    def test_graceful_empty_when_no_lineups(self):
        scraper = self._scraper()

        def fake_iter(match_ids, **kwargs):
            for mid in match_ids:
                yield str(mid), {}

        scraper._iter_match_captures = fake_iter

        out = scraper.read_match_capture(
            league='ENG-Premier League', season=2025, match_ids=['M1'],
        )
        for key in ('player_ratings', 'event_player_stats',
                    'match_stats', 'event_shotmap'):
            assert out[key].empty
            assert 'match_id' in out[key].columns


class TestCamoufoxProxy:
    """_camoufox_proxy builds a Playwright/Camoufox proxy dict from the
    configured residential proxy, splitting creds out of the URL — browsers
    reject creds embedded in the proxy URL (#757).
    """

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])

    @staticmethod
    def _proxy(**kw):
        from types import SimpleNamespace
        defaults = dict(host='1.2.3.4', port=10000, username='u', password='p',
                        masked_url='http://u:***@1.2.3.4:10000')
        defaults.update(kw)
        return SimpleNamespace(**defaults)

    @pytest.mark.unit
    def test_returns_split_creds_dict_when_proxy_has_credentials(self):
        # Arrange
        scraper = self._scraper()
        scraper._proxy_manager = MagicMock(total_count=1)
        scraper._proxy_manager.get_proxy.return_value = self._proxy()

        # Act
        out = scraper._camoufox_proxy()

        # Assert — server + creds split out of the URL.
        assert out == {
            'server': 'http://1.2.3.4:10000', 'username': 'u', 'password': 'p',
        }

    @pytest.mark.unit
    def test_omits_creds_when_proxy_has_none(self):
        # Arrange — proxy without username/password (e.g. IP-allowlisted exit).
        scraper = self._scraper()
        scraper._proxy_manager = MagicMock(total_count=1)
        scraper._proxy_manager.get_proxy.return_value = self._proxy(
            username=None, password=None,
        )

        # Act
        out = scraper._camoufox_proxy()

        # Assert — only the server key; no username/password.
        assert out == {'server': 'http://1.2.3.4:10000'}

    @pytest.mark.unit
    def test_returns_none_when_no_proxy_configured(self):
        # Arrange — no proxy manager / empty pool → capture runs proxy-less and
        # Turnstile 403s every endpoint; _camoufox_proxy signals that with None.
        scraper = self._scraper()

        scraper._proxy_manager = None
        assert scraper._camoufox_proxy() is None

        scraper._proxy_manager = MagicMock(total_count=0)
        assert scraper._camoufox_proxy() is None
        scraper._proxy_manager.get_proxy.assert_not_called()


class TestResolveMatchIdsViaCapture:
    """resolve_finished_match_ids_via_capture navigates the league page through
    Camoufox and pulls finished match_ids from the captured /events XHR (#757 B1).
    We patch the capture session so no browser is needed.
    """

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])

    @staticmethod
    def _buffer():
        return {
            "/api/v1/unique-tournament/17/season/96668/events/last/0": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 101, "status": {"type": "finished"}},
                    {"id": 102, "status": {"type": "notstarted"}},
                    {"id": 103, "status": {"type": "finished"}},
                ]}},
        }

    class _FakeCap:
        def __init__(self, buffer):
            self._buffer = buffer

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def capture_tournament(self, nav_url):
            return self._buffer

    @pytest.mark.unit
    def test_returns_finished_ids_from_capture(self):
        # Arrange
        scraper = self._scraper()
        scraper._proxy_manager = None
        fake = self._FakeCap(self._buffer())

        # Act — patch the capture session (imported lazily inside the method).
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   return_value=fake):
            out = scraper.resolve_finished_match_ids_via_capture(
                'ENG-Premier League', 2025,
            )

        # Assert — only finished events, deduped, notstarted dropped.
        assert out == ['101', '103']

    @pytest.mark.unit
    def test_returns_empty_when_no_slug_or_ut_id(self):
        # Arrange — NED-Eredivisie is in neither the ut_id nor the slug map.
        scraper = self._scraper()
        # Act / Assert — bails before opening a browser.
        assert scraper.resolve_finished_match_ids_via_capture('NED-Eredivisie', 2025) == []

    @pytest.mark.unit
    def test_returns_empty_when_capture_raises(self):
        # Arrange — a dead proxy / browser crash must degrade to [] (not raise).
        scraper = self._scraper()
        scraper._proxy_manager = None
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   side_effect=RuntimeError('browser boom')):
            out = scraper.resolve_finished_match_ids_via_capture(
                'ENG-Premier League', 2025,
            )
        assert out == []


class TestReadPlayerCapture:
    """read_player_capture (#751 PR3): ONE Camoufox nav per player → the
    player_profile frame (bio from __NEXT_DATA__). We patch the
    ``_iter_player_captures`` seam (no browser) and assert profile flattening +
    (league, season) tagging. (Season-aggregate stats are deferred to PR3b.)"""

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])

    def _capture(self, pid):
        return {'profile': {
            'id': int(pid), 'name': f'Player {pid}', 'slug': f'p-{pid}',
            'position': 'F', 'height': 185, 'preferredFoot': 'Right',
            'dateOfBirthTimestamp': 1180483200,
            'team': {'id': 30, 'name': 'Brighton'}}}

    @pytest.mark.unit
    def test_one_pass_yields_profile_frame(self):
        scraper = self._scraper()
        seen = []

        def fake_iter(player_ids):
            for pid in player_ids:
                seen.append(str(pid))
                yield str(pid), self._capture(pid)

        scraper._iter_player_captures = fake_iter
        out = scraper.read_player_capture(
            league='ENG-Premier League', season=2025, player_ids=['101', '102'])

        assert seen == ['101', '102']   # ONE capture per player ("one nav")
        prof = out['player_profile']
        assert len(prof) == 2
        assert set(prof['season']) == {'2526'}
        assert set(prof['_entity_type']) == {'player_profile'}
        prow = {r['player_id']: r for r in prof.to_dict('records')}['101']
        assert prow['height_cm'] == 185 and prow['preferred_foot'] == 'Right'
        assert prow['current_team_name'] == 'Brighton'

    @pytest.mark.unit
    def test_graceful_empty_when_nothing_captured(self):
        scraper = self._scraper()

        def fake_iter(player_ids):
            for pid in player_ids:
                yield str(pid), {'profile': None}

        scraper._iter_player_captures = fake_iter
        out = scraper.read_player_capture(
            league='ENG-Premier League', season=2025, player_ids=['101'])
        assert out['player_profile'].empty
        assert 'player_id' in out['player_profile'].columns
