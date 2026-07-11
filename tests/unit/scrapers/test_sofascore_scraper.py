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
                    # #840: extra/unknown source fields must pass through.
                    'draw': True,
                    'isOwnGoal': False,
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
        # Anchors / PK — unchanged contract.
        assert goal['match_id'] == '14023925'
        assert goal['shot_id'] == '101'
        assert goal['player_id'] == '11111'
        assert goal['team_id'] == 1
        assert goal['is_home'] is True
        # #840: source-key names (Bronze as-is), NOT the old derived names.
        assert goal['id'] == 101
        assert goal['time'] == 12
        assert goal['added_time'] == 0
        assert goal['reversed_period_count'] == 1
        assert goal['shot_type'] == 'rightFoot'
        assert goal['situation'] == 'open-play'
        assert goal['body_part'] == 'rightFoot'
        assert goal['incident_type'] == 'goal'
        assert goal['goal_type'] == 'regular'
        assert goal['player_coordinates_x'] == 88.5
        assert goal['player_coordinates_y'] == 50.0
        assert goal['goal_mouth_coordinates_x'] == 100.0
        assert goal['goal_mouth_coordinates_y'] == 52.3
        assert goal['xg'] == 0.72
        assert goal['xgot'] == 0.84
        # Unknown/extra source fields pass through automatically (the #840 point).
        assert goal['draw'] is True
        assert goal['is_own_goal'] is False
        # Frozen compatibility aliases keep fresh-bootstrap Silver SQL valid.
        assert goal['minute'] == goal['time']
        assert goal['x'] == goal['player_coordinates_x']
        assert goal['y'] == goal['player_coordinates_y']
        for dead in ('period', 'outcome', 'goal_x', 'goal_y'):
            assert dead not in goal

    def test_flatten_missing_id_falls_back_to_composite(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        rows = SofaScoreScraper._flatten_shotmap('14023925', self._payload())
        third = rows[2]
        # composite: match-time-player-addedTime
        assert third['shot_id'] == '14023925-78-33333-0'
        assert third['player_id'] == '33333'
        assert third['incident_type'] == 'miss'
        # #840: absent source key -> absent column (not a None-valued column).
        assert 'xgot' not in third
        assert 'added_time' not in third

    def test_fallback_ids_stay_unique_for_same_player_and_minute(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        shot = {"player": {"id": 7}, "time": 42, "addedTime": 0}
        rows = SofaScoreScraper._flatten_shotmap(
            "99",
            {"shotmap": [dict(shot), dict(shot)]},
        )
        assert [row["shot_id"] for row in rows] == [
            "99-42-7-0",
            "99-42-7-0-2",
        ]

    def test_flatten_handles_garbage(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        # Non-dict payload, missing shotmap key, non-list shotmap.
        assert SofaScoreScraper._flatten_shotmap('1', None) == []
        assert SofaScoreScraper._flatten_shotmap('1', {}) == []
        assert SofaScoreScraper._flatten_shotmap('1', {'shotmap': 'oops'}) == []
        # Empty rows still carry the frozen compatibility schema.
        assert SofaScoreScraper._flatten_shotmap('1', {'shotmap': [{}]}) == [
            {
                'match_id': '1',
                'shot_id': '1-NA-NA-0',
                'player_id': None,
                'team_id': None,
                'is_home': None,
                'minute': None,
                'x': None,
                'y': None,
            }
        ]


class TestAutoFlatten:
    """Unit tests for the recursive Bronze auto-flatten helper (#840)."""

    def test_scalar_coercion(self):
        from scrapers.sofascore.scraper import _auto_flatten
        out = {}
        _auto_flatten({'accuratePass': '12', 'ratingText': 'n/a', 'flag': True}, out)
        assert out['accurate_pass'] == 12       # numeric string upcast
        assert out['rating_text'] == 'n/a'      # non-numeric string kept
        assert out['flag'] is True

    def test_value_wrapper_unwrapped(self):
        from scrapers.sofascore.scraper import _auto_flatten
        out = {}
        _auto_flatten({'goals': {'value': 3, 'previousValue': 2}}, out)
        # {"value": X, ...} collapses to X — not recursed into a prefix.
        assert out['goals'] == 3
        assert 'goals_value' not in out

    def test_nested_dict_prefixed(self):
        from scrapers.sofascore.scraper import _auto_flatten
        out = {}
        _auto_flatten({'playerCoordinates': {'x': 88.5, 'y': 50.0}}, out)
        assert out['player_coordinates_x'] == 88.5
        assert out['player_coordinates_y'] == 50.0

    def test_lists_skipped(self):
        from scrapers.sofascore.scraper import _auto_flatten
        out = {}
        _auto_flatten({'tags': [1, 2, 3], 'n': 5}, out)
        assert 'tags' not in out                 # lists don't flatten into columns
        assert out['n'] == 5

    def test_skip_and_anchor_not_clobbered(self):
        from scrapers.sofascore.scraper import _auto_flatten
        out = {'team_id': 1}                      # pre-seeded anchor
        _auto_flatten(
            {'team': {'id': 999}, 'teamId': 999, 'x': 1},
            out, skip=('team',),
        )
        assert out['team_id'] == 1                # anchor preserved (teamId did not clobber)
        assert out['x'] == 1                      # skip removed only 'team', not siblings

    def test_depth_cap(self):
        from scrapers.sofascore.scraper import _auto_flatten, _MAX_FLATTEN_DEPTH
        node = {'deep_leaf': 1}
        for _ in range(_MAX_FLATTEN_DEPTH + 2):
            node = {'d': node}
        node['shallow'] = 2
        out = {}
        _auto_flatten(node, out)
        assert out['shallow'] == 2                # shallow content kept
        assert 1 not in out.values()             # too-deep leaf dropped by the cap


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
        # position_specific cannot be sourced here and stay None. Production
        # capture derives these fields directly from the unified lineups parser.
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
        # remain None in this pure legacy-payload flattener.
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


class TestFirstClassLineups:
    @pytest.mark.unit
    def test_preserves_starter_used_and_unused_bench(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        rows = SofaScoreScraper._flatten_lineup_side(
            '42',
            'home',
            {'players': [
                {'player': {'id': 1}, 'substitute': False,
                 'statistics': {'rating': 7.0}},
                {'player': {'id': 2}, 'substitute': True,
                 'statistics': {'rating': 6.2}},
                {'player': {'id': 3}, 'substitute': True},
            ]},
        )
        by_id = {row['player_id']: row for row in rows}
        assert by_id['1']['participation_status'] == 'starter'
        assert by_id['2']['participation_status'] == 'substitute_used'
        assert by_id['3']['participation_status'] == 'unused_substitute'
        assert by_id['3']['is_unused_substitute'] is True


class TestIncidentsFlatten:
    @pytest.mark.unit
    def test_goals_cards_substitutions_var_and_derived_key(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        payload = {'incidents': [
            {'id': 10, 'incidentType': 'goal', 'time': 12,
             'player': {'id': 1}},
            {'id': 11, 'incidentType': 'card', 'incidentClass': 'yellow'},
            {'id': 12, 'incidentType': 'substitution',
             'playerIn': {'id': 2}, 'playerOut': {'id': 3}},
            {'incidentType': 'varDecision', 'reason': 'offside'},
        ]}
        rows = SofaScoreScraper._flatten_incidents('42', payload)
        assert [row['incident_type'] for row in rows] == [
            'goal', 'card', 'substitution', 'varDecision',
        ]
        assert rows[0]['player_id'] == 1
        assert rows[1]['incident_class'] == 'yellow'
        assert rows[2]['player_in_id'] == 2
        assert rows[3]['incident_id'].startswith('derived-3-')
        assert SofaScoreScraper._flatten_incidents('42', payload)[3][
            'incident_id'
        ] == rows[3]['incident_id']


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
                                    # #840: SofaScore sends `home`/`away` as a
                                    # JSON *number* for count/decimal stats (not
                                    # a string) — reproduce that heterogeneity so
                                    # the str-pinning is exercised.
                                    'home': 14,
                                    'away': 7,
                                    'homeValue': 14,
                                    'awayValue': 7,
                                    'compareCode': 1,
                                    'valueType': 'count',
                                },
                                {
                                    'name': 'Expected goals',
                                    'key': 'expectedGoals',
                                    'home': 1.8,
                                    'away': 0.6,
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

        # #840: source-key names (Bronze as-is); Silver renames name->stat_name,
        # key->stat_key, home->home_text, away->away_text.
        bp_all = next(
            r for r in rows
            if r['period'] == 'ALL' and r['name'] == 'Ball possession'
        )
        assert bp_all['match_id'] == '14023925'
        assert bp_all['stat_group'] == 'Possession'
        assert bp_all['key'] == 'ballPossession'
        assert bp_all['home_value'] == 55
        assert bp_all['away_value'] == 45
        assert bp_all['home'] == '55%'
        assert bp_all['away'] == '45%'
        assert bp_all['compare_code'] == 1
        assert bp_all['value_type'] == 'percent'
        assert bp_all['stat_name'] == bp_all['name']
        assert bp_all['stat_key'] == bp_all['key']
        assert bp_all['home_text'] == bp_all['home']
        assert bp_all['away_text'] == bp_all['away']
        assert bp_all['statistic_key'] == bp_all['key']

        xg = next(r for r in rows if r['name'] == 'Expected goals')
        assert xg['home_value'] == 1.8
        assert xg['away_value'] == 0.6

        bp_1st = next(
            r for r in rows
            if r['period'] == '1ST' and r['name'] == 'Ball possession'
        )
        assert bp_1st['home_value'] == 58

        # #840: `home`/`away` are display text — ALWAYS str, even when SofaScore
        # sent a JSON number (count/decimal stats). Numeric-source values are
        # stringified ('14', '1.8'), not upcast, so the Bronze column is a stable
        # varchar the PyArrow->Iceberg writer can serialize.
        ts = next(r for r in rows if r['name'] == 'Total shots')
        assert ts['home'] == '14' and isinstance(ts['home'], str)
        assert xg['home'] == '1.8' and isinstance(xg['home'], str)
        assert all(isinstance(r['home'], str) for r in rows)
        assert all(isinstance(r['away'], str) for r in rows)

    def test_flatten_handles_garbage(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert SofaScoreScraper._flatten_match_stats('1', None) == []
        assert SofaScoreScraper._flatten_match_stats('1', {}) == []
        assert SofaScoreScraper._flatten_match_stats('1', {'statistics': 'oops'}) == []
        # Period with no groups → no rows.
        assert SofaScoreScraper._flatten_match_stats(
            '1', {'statistics': [{'period': 'ALL'}]}
        ) == []

    def test_home_away_are_iceberg_serializable(self):
        """#840 regression (found in live e2e): SofaScore's `home`/`away` are
        heterogeneous JSON — str '55%' for percent, int 14 for count, float 1.8
        for decimal. Left raw, _coerce_scalar upcast the numeric ones while the
        percent stayed str, yielding a mixed int/float/str object column that
        crashed the PyArrow->Iceberg writer ("Expected bytes, got a 'float'").
        Pinning them to str keeps the Bronze column a single-type varchar.
        """
        import pandas as pd

        from scrapers.sofascore.scraper import SofaScoreScraper

        df = pd.DataFrame(
            SofaScoreScraper._flatten_match_stats('m', self._payload())
        )
        for col in ('home', 'away'):
            types = {type(v).__name__ for v in df[col].dropna()}
            assert types == {'str'}, f"{col} has mixed types: {types}"

        # The exact prod failure path: pandas -> Arrow must not raise.
        pa = pytest.importorskip('pyarrow')
        pa.Table.from_pandas(df, preserve_index=False)


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
    """Sanity for registry-backed legacy lookup views (#24)."""

    def test_known_leagues(self):
        from scrapers.sofascore.scraper import SOFASCORE_TOURNAMENT_MAP
        # Premier League is the canonical reference (APL probe #19).
        assert SOFASCORE_TOURNAMENT_MAP['ENG-Premier League'] == 17
        # Other Big 5 leagues should be present.
        assert 'ESP-La Liga' in SOFASCORE_TOURNAMENT_MAP
        assert 'GER-Bundesliga' in SOFASCORE_TOURNAMENT_MAP
        assert 'ITA-Serie A' in SOFASCORE_TOURNAMENT_MAP
        assert 'FRA-Ligue 1' in SOFASCORE_TOURNAMENT_MAP

    def test_compatibility_views_are_derived_from_registry(self):
        from scrapers.sofascore.catalog import SofaScoreCatalog
        from scrapers.sofascore.scraper import (
            SOFASCORE_TOURNAMENT_MAP,
            SOFASCORE_TOURNAMENT_SLUG,
        )

        catalog = SofaScoreCatalog.load()
        assert SOFASCORE_TOURNAMENT_MAP == catalog.tournament_map(
            enabled_only=False,
        )
        assert SOFASCORE_TOURNAMENT_SLUG == catalog.slug_map(
            enabled_only=False,
        )

    def test_registered_season_id_precedes_capture_fallbacks(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        catalog = MagicMock()
        catalog.resolve_season_id.return_value = 76986
        cap = MagicMock()
        captured = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200,
                "challenge": False,
                "json": {"seasons": [{"year": "25/26", "id": 99999}]},
            }
        }

        with patch(
            "scrapers.sofascore.scraper._SOFASCORE_CATALOG",
            catalog,
        ):
            sid = SofaScoreScraper._resolve_target_sid(
                MagicMock(), cap, captured, 17, "25/26",
            )

        assert sid == 76986
        catalog.resolve_season_id.assert_called_once_with(17, "25/26")
        cap.fetch_api_json.assert_not_called()


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
        # Anchor.
        assert row['player_id'] == '11111'
        # #840: source-key names (Bronze as-is); Silver renames/derives.
        assert row['name'] == 'John Doe'
        assert row['short_name'] == 'J. Doe'
        assert row['slug'] == 'john-doe'
        assert row['position'] == 'F'
        assert row['jersey_number'] == 9                  # _coerce_scalar upcasts '9'->9
        assert row['shirt_number'] == 9
        assert row['height'] == 182                       # was height_cm
        assert row['preferred_foot'] == 'Right'
        assert row['date_of_birth_timestamp'] == 631152000  # raw epoch, no derive
        assert row['nationality'] == 'England'
        assert row['country_name'] == 'England'
        assert row['country_alpha2'] == 'EN'              # was country_code
        assert row['team_id'] == 1                        # was current_team_id
        assert row['team_name'] == 'Team X'               # was current_team_name
        assert row['retired'] is False
        assert row['height_cm'] == row['height']
        assert row['date_of_birth'] == '1990-01-01'
        assert row['country_code'] == row['country_alpha2']
        assert row['current_team_id'] == row['team_id']
        assert row['current_team_name'] == row['team_name']

    def test_garbage(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert SofaScoreScraper._flatten_player_profile(None) is None
        assert SofaScoreScraper._flatten_player_profile({}) is None
        # No player.id → None
        assert SofaScoreScraper._flatten_player_profile({'player': {'name': 'X'}}) is None

    def test_dob_timestamp_kept_raw(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        # #840: Bronze keeps the raw epoch; epoch->date derivation is Silver's job.
        payload = {
            'player': {'id': 1, 'dateOfBirthTimestamp': None},
        }
        row = SofaScoreScraper._flatten_player_profile(payload)
        assert row is not None
        assert row['date_of_birth_timestamp'] is None
        assert row['date_of_birth'] is None

    def test_country_kept_raw_no_fallback_in_bronze(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        # #840: nationality<-country.name fallback moved to Silver; Bronze keeps
        # the nested country block as-is (no synthetic nationality here).
        payload = {
            'player': {
                'id': 1,
                'country': {'name': 'Brazil', 'alpha2': 'BR'},
            }
        }
        row = SofaScoreScraper._flatten_player_profile(payload)
        assert row['country_name'] == 'Brazil'
        assert row['country_alpha2'] == 'BR'
        assert row['nationality'] is None
        assert row['country_code'] == 'BR'


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
            "lineups": {
                "home": {
                    "players": [
                        {
                            "player": {"id": 11111},
                            "position": "G",
                            "substitute": False,
                            "captain": True,
                            "shirtNumber": 1,
                            "statistics": {"rating": "7.2", "totalPass": 30},
                        },
                    ]
                },
                "away": {
                    "players": [
                        {
                            "player": {"id": 22222},
                            "position": "F",
                            "substitute": False,
                            "statistics": {"rating": "6.5", "totalShots": 3},
                        },
                    ]
                },
            },
            # Live /event/{id} nests the event under "event" (#751 PR2); the
            # venue block is the SofaScore nested-object shape (#753).
            'event': {'event': {'id': 14023959,
                                'homeTeam': {'id': 1, 'name': 'Home FC'},
                                'awayTeam': {'id': 2, 'name': 'Away FC'},
                                'venue': {
                                    'stadium': {'name': 'Etihad Stadium',
                                                'capacity': 55097},
                                    'city': {'name': 'Manchester'},
                                    'country': {'name': 'England',
                                                'alpha2': 'EN'},
                                    'venueCoordinates': {
                                        'latitude': 53.483056,
                                        'longitude': -2.200278}}}},
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
            'incidents': {'incidents': [
                {'id': 77, 'incidentType': 'goal', 'time': 23,
                 'player': {'id': 11111}, 'isHome': True},
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
        assert set(ratings["season"]) == {"2526"}
        assert set(eps["season"]) == {"2526"}
        assert set(eps["_entity_type"]) == {"event_player_stats"}
        assert len(out['lineups']) == 4
        assert set(out['lineups']['_entity_type']) == {'lineups'}
        assert len(out['incidents']) == 2
        assert set(out['incidents']['incident_type']) == {'goal'}
        status = out["capture_status"]
        assert status["capture_complete"].all()
        assert set(status["lineups_status"]) == {"success"}
        assert {"captain", "substitute", "shirt_number"} <= set(ratings.columns)
        assert ratings.loc[ratings["player_id"] == "11111", "captain"].all()
        assert set(ratings["shirt_number"].dropna()) == {1}

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
        assert set(ms['name']) == {'Ball possession'}   # #840 source-key name
        assert set(sm['xg']) == {0.45}

    @pytest.mark.unit
    def test_one_pass_yields_venue_frame(self):
        # #753 — the SAME capture pass materialises a venue frame from the
        # /event payload's nested venue block (one row per match).
        scraper = self._scraper()

        def fake_iter(match_ids, **kwargs):
            for mid in match_ids:
                yield str(mid), self._endpoints()

        scraper._iter_match_captures = fake_iter

        out = scraper.read_match_capture(
            league='ENG-Premier League', season=2025, match_ids=['M1', 'M2'],
        )

        ve = out['venue']
        assert len(ve) == 2   # one row per match
        assert set(ve['season']) == {'2526'}
        assert set(ve['_entity_type']) == {'venue'}
        row = ve.to_dict('records')[0]
        # #840: nested SofaScore objects auto-flatten to source-key names
        # (stadium.name -> stadium_name); Silver renames back.
        assert row['game_id'] == 14023959
        assert row['stadium_name'] == 'Etihad Stadium'
        assert row['city_name'] == 'Manchester'
        assert row['country_name'] == 'England'
        assert row['venue_coordinates_latitude'] == 53.483056
        assert row['venue_coordinates_longitude'] == -2.200278
        # Passthrough bonus fields the old fixed list dropped.
        assert row['stadium_capacity'] == 55097
        assert row['country_alpha2'] == 'EN'
        assert row['stadium'] == row['stadium_name']
        assert row['city'] == row['city_name']
        assert row['country'] == row['country_name']
        assert row['venue_latitude'] == row['venue_coordinates_latitude']
        assert row['venue_longitude'] == row['venue_coordinates_longitude']

    @pytest.mark.unit
    def test_requests_all_tabs_and_event(self):
        # The consolidated pass requests every endpoint family.
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
        assert set(captured_kwargs['required']) == {'event', 'lineups'}

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
        # venue is keyed by game_id (one row per match), not match_id.
        assert out['venue'].empty
        assert 'game_id' in out['venue'].columns
        status = out['capture_status'].iloc[0]
        assert not status['capture_complete']
        assert status['lineups_status'] == 'missing'

    @pytest.mark.unit
    def test_terminal_empty_payloads_are_manifest_complete(self):
        scraper = self._scraper()
        endpoints = {
            'event': {},
            'lineups': {'home': {}, 'away': {}},
            'statistics': {'statistics': []},
            'shotmap': {'shotmap': []},
        }
        scraper._iter_match_captures = lambda match_ids, **kwargs: iter(
            [('M1', endpoints)]
        )

        out = scraper.read_match_capture(
            league='ENG-Premier League',
            season=2025,
            match_ids=['M1'],
        )

        assert out['player_ratings'].empty
        status = out['capture_status'].iloc[0]
        assert status['capture_complete']
        assert status['shotmap_status'] == 'success'

    @pytest.mark.unit
    def test_keeps_independent_endpoints_when_lineups_miss(self):
        scraper = self._scraper()
        endpoints = self._endpoints()
        endpoints.pop("lineups")

        scraper._iter_match_captures = lambda match_ids, **kwargs: iter(
            [
                ("M1", endpoints),
            ]
        )
        out = scraper.read_match_capture(
            league="ENG-Premier League",
            season=2025,
            match_ids=["M1"],
        )

        assert out["player_ratings"].empty
        assert out["event_player_stats"].empty
        assert len(out["match_stats"]) == 1
        assert len(out["event_shotmap"]) == 1
        assert len(out["venue"]) == 1
        assert not out["capture_status"].iloc[0]["capture_complete"]


class TestFlattenEventVenue:
    """_flatten_event_venue (#753): project /event venue → one Bronze row.
    Defensive across the SofaScore nested-object shape AND the flat shape the
    issue documents; None when no usable stadium."""

    def _flat(self, payload, match_id='14023959'):
        from scrapers.sofascore.scraper import SofaScoreScraper
        return SofaScoreScraper._flatten_event_venue(match_id, payload)

    @pytest.mark.unit
    def test_extracts_nested_sofascore_shape(self):
        # #840: nested {"name": ...} objects auto-flatten to source-key names
        # (stadium.name -> stadium_name); Silver renames back.
        payload = {'event': {'id': 14023959, 'venue': {
            'stadium': {'name': 'Etihad Stadium', 'capacity': 55097},
            'city': {'name': 'Manchester'},
            'country': {'name': 'England', 'alpha2': 'EN'},
            'venueCoordinates': {'latitude': 53.483056, 'longitude': -2.200278},
        }}}
        row = self._flat(payload)
        assert row['game_id'] == 14023959
        assert row['stadium_name'] == 'Etihad Stadium'
        assert row['stadium_capacity'] == 55097          # passthrough bonus
        assert row['city_name'] == 'Manchester'
        assert row['country_name'] == 'England'
        assert row['country_alpha2'] == 'EN'
        assert row['venue_coordinates_latitude'] == 53.483056
        assert row['venue_coordinates_longitude'] == -2.200278
        assert row['stadium'] == row['stadium_name']
        assert row['city'] == row['city_name']
        assert row['country'] == row['country_name']
        assert row['venue_latitude'] == row['venue_coordinates_latitude']
        assert row['venue_longitude'] == row['venue_coordinates_longitude']

    @pytest.mark.unit
    def test_extracts_flat_issue_shape(self):
        # The flat {stadium, city, country} string form: auto-flatten keeps the
        # bare-string keys as-is (stadium/city/country) — Silver's COALESCE bridges
        # both shapes.
        payload = {'event': {'id': 99, 'venue': {
            'stadium': 'Anfield', 'city': 'Liverpool', 'country': 'England',
        }}}
        row = self._flat(payload)
        assert row['stadium'] == 'Anfield'
        assert row['city'] == 'Liverpool'
        assert row['country'] == 'England'
        assert 'venue_coordinates_latitude' not in row
        assert row['game_id'] == 99

    @pytest.mark.unit
    def test_extracts_live_amex_shape_no_coords(self):
        # Live-verified 2026-06-23 (event 14023959, American Express Stadium): the
        # real payload nests stadium/city/country objects, carries capacity, and
        # has NO venueCoordinates → coords absent, not an error.
        payload = {'event': {'id': 14023959, 'venue': {
            'name': 'American Express Stadium',
            'capacity': 31876,
            'stadium': {'name': 'American Express Stadium', 'capacity': 31876},
            'city': {'name': 'Falmer', 'country': {'name': 'England'}, 'id': 25144},
            'country': {'name': 'England', 'alpha2': 'EN', 'alpha3': 'ENG'},
            'slug': 'american-express-community-s-stadium', 'id': 2443,
        }}}
        row = self._flat(payload)
        assert row['game_id'] == 14023959
        assert row['stadium_name'] == 'American Express Stadium'
        assert row['city_name'] == 'Falmer'
        assert row['country_name'] == 'England'
        # Deeper nesting + extra fields preserved (#840 "keep everything").
        assert row['city_country_name'] == 'England'
        assert row['country_alpha3'] == 'ENG'
        assert row['slug'] == 'american-express-community-s-stadium'
        assert 'venue_coordinates_latitude' not in row

    @pytest.mark.unit
    def test_none_when_no_venue(self):
        assert self._flat({'event': {'id': 1}}) is None

    @pytest.mark.unit
    def test_none_when_no_stadium(self):
        assert self._flat(
            {'event': {'id': 1, 'venue': {'city': {'name': 'X'}}}}) is None

    @pytest.mark.unit
    def test_none_when_payload_not_dict(self):
        assert self._flat(None) is None

    @pytest.mark.unit
    def test_game_id_falls_back_to_match_id(self):
        # event payload without id → use the numeric match_id arg.
        payload = {'event': {'venue': {'stadium': {'name': 'X'}}}}
        assert self._flat(payload, match_id='555')['game_id'] == 555


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

    @pytest.mark.unit
    def test_retry_avoids_immediately_reselecting_same_proxy(self):
        scraper = self._scraper()
        first = self._proxy(host="1.1.1.1", port=10001)
        second = self._proxy(host="2.2.2.2", port=10002)
        scraper._proxy_manager = MagicMock(total_count=2)
        scraper._proxy_manager.get_proxy.side_effect = [first, first, second]

        assert scraper._camoufox_proxy()["server"] == "http://1.1.1.1:10001"
        assert scraper._camoufox_proxy()["server"] == "http://2.2.2.2:10002"

    @pytest.mark.unit
    def test_browser_result_updates_proxy_manager_health(self):
        scraper = self._scraper()
        proxy_obj = self._proxy()
        scraper._proxy_manager = MagicMock(total_count=1)
        scraper._proxy_manager.get_proxy.return_value = proxy_obj
        proxy = scraper._camoufox_proxy()

        scraper._record_camoufox_proxy_result(
            proxy,
            success=False,
            error_type="cloudflare",
        )

        scraper._proxy_manager.record_result.assert_called_once_with(
            proxy_obj,
            success=False,
            error_type="cloudflare",
        )


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
        # season 2025 -> target year '25/26' (sid 76986 in the registry); events
        # carry that season so the season-year guard keeps them (#824).
        s = {"year": "25/26", "id": 76986}
        return {
            "/api/v1/unique-tournament/17/season/76986/events/last/0": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 101, "status": {"type": "finished"}, "season": s},
                    {"id": 102, "status": {"type": "notstarted"}, "season": s},
                    {"id": 103, "status": {"type": "finished"}, "season": s},
                ]}},
        }

    class _FakeCap:
        def __init__(self, buffer):
            self._buffer = buffer

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def capture_buffer(self, nav_url):
            return self._buffer

        def paginate_tournament_season(self, ut_id, season_id, max_pages=25):
            # Real impl pages the season's events into the buffer; the fixture
            # pre-populates them, so just return the buffer unchanged (#824).
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

    @pytest.mark.unit
    def test_retries_when_a_later_finished_page_is_missing(self):
        scraper = self._scraper()
        scraper._proxy_manager = None
        season = {"year": "25/26", "id": 76986}
        base = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200,
                "challenge": False,
                "json": {"seasons": [season]},
            },
            "/api/v1/unique-tournament/17/season/76986/events/last/0": {
                "status": 200,
                "challenge": False,
                "json": {
                    "events": [
                        {"id": 101, "season": season, "status": {"type": "finished"}}
                    ],
                    "hasNextPage": True,
                },
            },
        }
        complete = {
            **base,
            "/api/v1/unique-tournament/17/season/76986/events/last/1": {
                "status": 200,
                "challenge": False,
                "json": {
                    "events": [
                        {"id": 102, "season": season, "status": {"type": "finished"}}
                    ],
                    "hasNextPage": False,
                },
            },
        }
        sessions = []

        class _Cap:
            def __init__(self, **kwargs):
                self.buffer = base if not sessions else complete
                sessions.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def capture_buffer(self, nav_url):
                return dict(self.buffer)

            def paginate_tournament_season(self, *args, **kwargs):
                return dict(self.buffer)

        with patch(
            "scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture",
            _Cap,
        ):
            out = scraper.resolve_finished_match_ids_via_capture(
                "ENG-Premier League",
                2025,
            )

        assert len(sessions) == 2
        assert out == ["101", "102"]


class TestResolveMatchIdsFromSchedule:
    @pytest.mark.unit
    def test_integer_2021_matches_2122_schedule_partition(self):
        import pandas as pd

        with (
            patch("scrapers.base.base_scraper.get_rate_limiter"),
            patch("scrapers.base.base_scraper.get_retry_policy"),
            patch("scrapers.base.base_scraper.get_circuit_breaker"),
            patch("scrapers.base.base_scraper.IcebergWriter"),
        ):
            from scrapers.sofascore.scraper import SofaScoreScraper

            scraper = SofaScoreScraper(
                leagues=["ENG-Premier League"],
                seasons=[2021],
            )
        scraper.read_schedule = MagicMock(
            return_value=pd.DataFrame(
                {
                    "game_id": [101, 99],
                    "league": [
                        "ENG-Premier League",
                        "ENG-Premier League",
                    ],
                    "season": ["2122", "2021"],
                    "status_type": ["finished", "finished"],
                }
            )
        )

        assert scraper._resolve_match_ids("ENG-Premier League", 2021) == ["101"]


class TestReadScheduleViaCapture:
    """read_schedule (#761) builds bronze.sofascore_schedule rows from a captured
    tournament page (the soccerdata reader is Turnstile-blocked). We patch the
    capture session so no browser is needed and assert the persisted schema."""

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            s = SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])
        s._proxy_manager = None
        return s

    @staticmethod
    def _buffer():
        # ut=17 (EPL) 25/26 events: one finished (round 7, scores), one
        # not-started (round 8). A 26/27 event (next season) must be filtered out
        # by the season-year guard, and a featured ut=16 event by the ut_id.
        s2526 = {"year": "25/26", "id": 76986}
        return {
            "/api/v1/unique-tournament/17/season/76986/events/last/0": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 101, "status": {"type": "finished"}, "season": s2526,
                     "homeTeam": {"name": "Arsenal"}, "awayTeam": {"name": "Chelsea"},
                     "homeScore": {"current": 2}, "awayScore": {"current": 1},
                     "startTimestamp": 1719000000, "roundInfo": {"round": 7}},
                ]}},
            "/api/v1/unique-tournament/17/season/76986/events/next/0": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 102, "status": {"type": "notstarted"}, "season": s2526,
                     "homeTeam": {"name": "Liverpool"}, "awayTeam": {"name": "Everton"},
                     "startTimestamp": 1719600000, "roundInfo": {"round": 8}},
                ]}},
            # NEXT season (26/27) on the same ut — must be dropped by the guard.
            "/api/v1/unique-tournament/17/season/96668/events/next/0": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 201, "status": {"type": "notstarted"},
                     "season": {"year": "26/27", "id": 96668},
                     "homeTeam": {"name": "Leeds"}, "awayTeam": {"name": "Sunderland"},
                     "startTimestamp": 1755000000, "roundInfo": {"round": 1}},
                ]}},
            # Featured OTHER tournament — must be filtered out by ut_id.
            "/api/v1/unique-tournament/16/season/58210/events/last/0": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 901, "status": {"type": "finished"},
                     "season": {"year": "25/26", "id": 58210},
                     "homeTeam": {"name": "X"}, "awayTeam": {"name": "Y"},
                     "homeScore": {"current": 0}, "awayScore": {"current": 0},
                     "startTimestamp": 1719000000, "roundInfo": {"round": 1}},
                ]}},
        }

    class _FakeCap:
        def __init__(self, buffer):
            self._buffer = buffer

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def capture_buffer(self, nav_url):
            return self._buffer

        def paginate_tournament_season(
            self,
            ut_id,
            season_id,
            max_pages=25,
            *,
            include_next=False,
        ):
            # Real impl pages the target season's events into the buffer; the
            # fixture pre-populates them, so return the buffer unchanged (#824).
            return self._buffer

    @pytest.mark.unit
    def test_builds_schedule_rows_with_full_schema(self):
        import pandas as pd

        scraper = self._scraper()
        fake = self._FakeCap(self._buffer())
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   return_value=fake):
            df = scraper.read_schedule()

        # Exactly the two ut=17 events (901 from ut=16 is filtered out).
        assert df is not None
        assert sorted(df['game_id'].tolist()) == [101, 102]
        # Partition labels: league passthrough + season short form '2526'.
        assert set(df['league']) == {'ENG-Premier League'}
        assert set(df['season']) == {'2526'}
        # #840: Bronze as-is — source-key names, raw types. Renames + type
        # derivations (epoch->timestamp, round->bigint) moved to the schedule
        # consumers (xref_match, team_match, shots).
        cols = set(df.columns)
        assert {'game_id', 'home_team_name', 'away_team_name',
                'home_score_current', 'start_timestamp', 'round_info_round',
                'status_type'} <= cols
        assert {'home_team', 'away_team', 'home_score', 'away_score'} <= cols
        assert not ({'date', 'round', 'week', 'game'} & cols)
        # start_timestamp stays a raw epoch int (no pandas timestamp coercion).
        finished = df[df['game_id'] == 101].iloc[0]
        assert finished['start_timestamp'] == 1719000000
        assert finished['home_team_name'] == 'Arsenal'
        assert finished['home_score_current'] == 2 and finished['away_score_current'] == 1
        # not-started event has no score → NaN in the unioned frame.
        notstarted = df[df['game_id'] == 102].iloc[0]
        assert pd.isna(notstarted['home_score_current'])

    @pytest.mark.unit
    def test_drops_next_season_events_when_page_rolled_over(self):
        # Off-season: the page serves ONLY 26/27 fixtures while our target is
        # 25/26 → the season guard drops them all → None (no partition pollution).
        scraper = self._scraper()
        buf = {
            "/api/v1/unique-tournament/17/season/96668/events/next/0": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 201, "status": {"type": "notstarted"},
                     "season": {"year": "26/27", "id": 96668},
                     "homeTeam": {"name": "Leeds"}, "awayTeam": {"name": "Sunderland"},
                     "startTimestamp": 1755000000, "roundInfo": {"round": 1}},
                ]}},
        }
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   return_value=self._FakeCap(buf)):
            assert scraper.read_schedule() is None

    @pytest.mark.unit
    def test_keeps_future_fixtures_when_last_page_is_empty(self):
        scraper = self._scraper()
        season = {"year": "25/26", "id": 76986}
        buf = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200,
                "challenge": False,
                "json": {"seasons": [season]},
            },
            "/api/v1/unique-tournament/17/season/76986/events/last/0": {
                "status": 200,
                "challenge": False,
                "json": {"events": []},
            },
            "/api/v1/unique-tournament/17/season/76986/events/next/0": {
                "status": 200,
                "challenge": False,
                "json": {
                    "events": [
                        {
                            "id": 501,
                            "season": season,
                            "status": {"type": "notstarted"},
                            "homeTeam": {"name": "A"},
                            "awayTeam": {"name": "B"},
                            "startTimestamp": 1800000000,
                        }
                    ]
                },
            },
        }
        with patch(
            "scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture",
            return_value=self._FakeCap(buf),
        ):
            df = scraper.read_schedule()
        assert df is not None
        assert df["game_id"].tolist() == [501]

    @pytest.mark.unit
    def test_returns_none_when_capture_empty(self):
        scraper = self._scraper()
        fake = self._FakeCap({})  # no events captured
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   return_value=fake):
            assert scraper.read_schedule() is None

    @pytest.mark.unit
    def test_returns_none_when_capture_raises(self):
        scraper = self._scraper()
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   side_effect=RuntimeError('browser boom')):
            assert scraper.read_schedule() is None

    @pytest.mark.unit
    def test_retries_on_fresh_proxy_after_transient_capture_failure(self):
        # #879: the top-5 weekend backfill lost whole league-seasons to ONE
        # dead proxy — a transient capture failure must retry on a fresh
        # residential exit instead of silently no-opping the unit.
        scraper = self._scraper()
        scraper._camoufox_proxy = MagicMock(
            side_effect=[{'server': 'http://dead:1'},
                         {'server': 'http://alive:2'}])
        good = self._FakeCap(self._buffer())
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   side_effect=[RuntimeError('Failed to connect to proxy'),
                                good]):
            df = scraper.read_schedule()

        assert df is not None
        assert sorted(df['game_id'].tolist()) == [101, 102]
        assert scraper._camoufox_proxy.call_count == 2  # fresh proxy per attempt

    def _sid_resolvable_buffer(self, page0_rec):
        """Target 25/26 sid resolvable from /seasons; its events page0 carries
        ``page0_rec`` — the record whose 'answered-ness' drives retry (#879)."""
        return {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200, "challenge": False, "json": {"seasons": [
                    {"year": "25/26", "id": 76986}]}},
            "/api/v1/unique-tournament/17/season/76986/events/last/0": page0_rec,
        }

    def _run_with_session_counter(self, scraper, buf):
        sessions = {'n': 0}

        class _Cap:
            def __init__(self, **kwargs):
                sessions['n'] += 1

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def capture_buffer(self, nav_url):
                return dict(buf)

            def paginate_tournament_season(
                self,
                ut_id,
                season_id,
                max_pages=25,
                *,
                include_next=False,
            ):
                return dict(buf)

        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   _Cap):
            df = scraper.read_schedule()
        return df, sessions['n']

    @pytest.mark.unit
    def test_answered_empty_season_not_retried(self):
        # The target season's OWN events page answered 200 with zero events —
        # legitimately empty (e.g. fixtures not yet published) → one attempt.
        scraper = self._scraper()
        scraper._camoufox_proxy = MagicMock(return_value=None)
        buf = self._sid_resolvable_buffer(
            {"status": 200, "challenge": False, "json": {"events": []}}
        )
        buf["/api/v1/unique-tournament/17/season/76986/events/next/0"] = {
            "status": 200,
            "challenge": False,
            "json": {"events": []},
        }

        df, sessions = self._run_with_session_counter(scraper, buf)

        assert df is None
        assert sessions == 1

    @pytest.mark.unit
    def test_terminal_404_next_direction_not_retried(self):
        # Live canary: a finished season had all 381 fixtures in /last while
        # /next/0 returned a JSON 404. That is a complete empty direction and
        # must not spend two extra ~2 MB browser warm-ups.
        scraper = self._scraper()
        scraper._camoufox_proxy = MagicMock(return_value=None)
        season = {"year": "25/26", "id": 76986}
        buf = self._sid_resolvable_buffer(
            {
                "status": 200,
                "challenge": False,
                "json": {
                    "events": [
                        {
                            "id": 101,
                            "season": season,
                            "status": {"type": "finished"},
                            "startTimestamp": 1750000000,
                            "homeTeam": {"id": 1, "name": "Home"},
                            "awayTeam": {"id": 2, "name": "Away"},
                        }
                    ],
                    "hasNextPage": False,
                },
            }
        )
        buf["/api/v1/unique-tournament/17/season/76986/events/next/0"] = {
            "status": 404,
            "challenge": False,
            "json": {"error": {"code": 404, "reason": "Not Found"}},
        }

        df, sessions = self._run_with_session_counter(scraper, buf)

        assert df is not None and df["game_id"].tolist() == [101]
        assert sessions == 1

    @pytest.mark.unit
    def test_body_read_race_record_is_retried(self):
        # #879 review (HIGH): a body-read race leaves {'status': 200,
        # 'json': None, 'challenge': None} — a transport failure, NOT a
        # legitimate empty. It must burn the retry budget, not short-circuit.
        scraper = self._scraper()
        scraper._camoufox_proxy = MagicMock(return_value=None)
        buf = self._sid_resolvable_buffer({
            "status": 200, "challenge": None, "json": None})

        df, sessions = self._run_with_session_counter(scraper, buf)

        assert df is None
        assert sessions == 3  # all attempts spent — never treated as answered

    @pytest.mark.unit
    def test_unanswered_events_page_is_retried(self):
        # sid resolved but the target season's events page never made it into
        # the buffer (challenged / dropped XHR) → retry on a fresh proxy.
        scraper = self._scraper()
        scraper._camoufox_proxy = MagicMock(return_value=None)
        buf = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200, "challenge": False, "json": {"seasons": [
                    {"year": "25/26", "id": 76986}]}},
        }

        df, sessions = self._run_with_session_counter(scraper, buf)

        assert df is None
        assert sessions == 3


class TestReadLeagueTableViaCapture:
    """read_league_table (#777) builds bronze.sofascore_league_table rows from a
    captured tournament page (the soccerdata reader is Turnstile-blocked). We
    patch the capture session so no browser is needed and assert the persisted
    schema + the season_id guard (only the target season's standings are kept)."""

    # The 16 columns of bronze.sofascore_league_table (mirrors bronze_schemas.json;
    # 'group' — WC group support, #913).
    _EXPECTED_COLS = {
        '_batch_id', '_entity_type', '_ingested_at', '_source',
        'team', 'mp', 'w', 'd', 'l', 'gf', 'ga', 'gd', 'pts', 'league', 'season',
        'group',
    }

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            s = SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])
        s._proxy_manager = None
        return s

    @staticmethod
    def _row(name, mp, w, d, losses, gf, ga, pts):
        return {
            "team": {"id": hash(name) % 1000, "name": name},
            "matches": mp,
            "wins": w,
            "draws": d,
            "losses": losses,
            "scoresFor": gf,
            "scoresAgainst": ga,
            "points": pts,
        }

    @classmethod
    def _buffer(cls):
        # ut=17 (EPL): /seasons maps 25/26 -> 76986 (target) and 26/27 -> 99999.
        # standings/total fires for the target sid (3 teams); a NEXT-season
        # standings (sid 99999) and a featured OTHER tournament (ut=16) must be
        # ignored by the (ut_id, season_id) guard.
        return {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200, "challenge": False, "json": {"seasons": [
                    {"year": "26/27", "id": 99999},
                    {"year": "25/26", "id": 76986},
                ]}},
            "/api/v1/unique-tournament/17/season/76986/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "rows": [
                        cls._row("Arsenal", 20, 15, 3, 2, 45, 18, 48),
                        cls._row("Liverpool", 20, 14, 4, 2, 42, 20, 46),
                        cls._row("Chelsea", 20, 12, 5, 3, 38, 22, 41),
                    ]},
                    {"type": "home", "rows": [cls._row("Arsenal", 10, 9, 1, 0, 28, 6, 28)]},
                ]}},
            # NEXT season standings on the same ut — must be dropped by sid guard.
            "/api/v1/unique-tournament/17/season/99999/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "rows": [
                        cls._row("Leeds", 0, 0, 0, 0, 0, 0, 0)]}]}},
            # Featured OTHER tournament — never matched (exact ut/sid path).
            "/api/v1/unique-tournament/16/season/58210/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "rows": [
                        cls._row("Real Madrid", 20, 16, 2, 2, 50, 15, 50)]}]}},
        }

    class _FakeCap:
        def __init__(self, buffer):
            self._buffer = buffer

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def capture_buffer(self, nav_url):
            return self._buffer

    @pytest.mark.unit
    def test_builds_league_table_rows_with_full_schema(self):
        scraper = self._scraper()
        fake = self._FakeCap(self._buffer())
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   return_value=fake):
            df = scraper.read_league_table()

        # Exactly the 3 target-season ut=17 rows (next-season + ut=16 dropped).
        assert df is not None
        assert sorted(df['team'].tolist()) == ['Arsenal', 'Chelsea', 'Liverpool']
        # Persisted schema matches the bronze table (no add/drop columns).
        assert set(df.columns) == self._EXPECTED_COLS
        # Partition labels: league passthrough + season short form '2526'.
        assert set(df['league']) == {'ENG-Premier League'}
        assert set(df['season']) == {'2526'}
        # gd is derived (scoresFor - scoresAgainst); counts are nullable bigint.
        arsenal = df[df['team'] == 'Arsenal'].iloc[0]
        assert arsenal['gd'] == 27 and arsenal['pts'] == 48 and arsenal['mp'] == 20
        for col in ('mp', 'w', 'd', 'l', 'gf', 'ga', 'gd', 'pts'):
            assert str(df[col].dtype) == 'Int64'

    @pytest.mark.unit
    def test_world_cup_single_year_label_and_groups(self):
        # #913 regression: the sid RESOLVE was single_year-aware but the row
        # LABEL used _season_to_short(2026) → WC rows landed under '2627'.
        # Both must be '2026'; group blocks must all survive.
        # dags.utils.medallion_config resolves only under the container
        # PYTHONPATH — stub it the way read_league_table imports it.
        import sys
        import types
        stub = types.ModuleType('dags.utils.medallion_config')
        stub.get_competition_season_format = lambda league, season: 'single_year'
        modules = {
            'dags': types.ModuleType('dags'),
            'dags.utils': types.ModuleType('dags.utils'),
            'dags.utils.medallion_config': stub,
        }
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            scraper = SofaScoreScraper(leagues=['INT-World Cup'], seasons=[2026])
        scraper._proxy_manager = None
        buf = {
            "/api/v1/unique-tournament/16/seasons": {
                "status": 200, "challenge": False, "json": {"seasons": [
                    {"year": "2026", "id": 58210},
                    {"year": "2022", "id": 41087},
                ]}},
            "/api/v1/unique-tournament/16/season/58210/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "name": "Group A", "rows": [
                        self._row("Mexico", 3, 3, 0, 0, 6, 0, 9)]},
                    {"type": "total", "name": "Group B", "rows": [
                        self._row("Canada", 3, 2, 1, 0, 5, 1, 7)]},
                ]}},
        }
        with patch.dict(sys.modules, modules), \
             patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   return_value=self._FakeCap(buf)):
            df = scraper.read_league_table()

        assert df is not None
        assert set(df['season']) == {'2026'}          # NOT '2627'
        assert set(df['league']) == {'INT-World Cup'}
        assert sorted(df['team'].tolist()) == ['Canada', 'Mexico']
        assert sorted(df['group'].tolist()) == ['Group A', 'Group B']

    @pytest.mark.unit
    def test_drops_next_season_when_page_rolled_over(self):
        # Off-season: /seasons still lists our 25/26 sid, but the standings XHR
        # that fired is for the NEXT season (99999) — the sid guard finds no
        # standings for our target sid → None (no partition pollution).
        scraper = self._scraper()
        buf = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200, "challenge": False, "json": {"seasons": [
                    {"year": "26/27", "id": 99999},
                    {"year": "25/26", "id": 76986},
                ]}},
            "/api/v1/unique-tournament/17/season/99999/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "rows": [
                        self._row("Leeds", 0, 0, 0, 0, 0, 0, 0)]}]}},
        }
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   return_value=self._FakeCap(buf)):
            assert scraper.read_league_table() is None

    @pytest.mark.unit
    def test_returns_none_when_capture_empty(self):
        scraper = self._scraper()
        fake = self._FakeCap({})  # no seasons map, no standings captured
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   return_value=fake):
            assert scraper.read_league_table() is None

    @pytest.mark.unit
    def test_returns_none_when_capture_raises(self):
        scraper = self._scraper()
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   side_effect=RuntimeError('browser boom')):
            assert scraper.read_league_table() is None


class TestReadPlayerCapture:
    """read_player_capture (#751 PR3 + PR3b): ONE Camoufox nav per player → the
    player_profile frame (bio from __NEXT_DATA__) AND the player_season_stats
    frame (exact target-season capture). We patch ``_iter_player_captures``
    seam (no browser) and assert flattening + (league, season) tagging + the
    season-guarded selection of the EPL overall out of the capture buffer."""

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])

    def _season_buffer(self, pid):
        """A Season-tab capture: /statistics/seasons map + the EPL (ut=17,
        season 76986 = '25/26') overall. Mirrors a non-transferred APL player."""
        return {
            f'/api/v1/player/{pid}/statistics/seasons': {
                'status': 200, 'challenge': False, 'json': {
                    'uniqueTournamentSeasons': [
                        {'uniqueTournament': {'id': 17, 'name': 'Premier League'},
                         'seasons': [{'year': '25/26', 'id': 76986}]}]}},
            f'/api/v1/player/{pid}/unique-tournament/17/season/76986/statistics/overall': {
                'status': 200, 'challenge': False, 'json': {
                    'team': {'id': 30, 'name': 'Brighton'},
                    'statistics': {'rating': 7.0, 'totalGoals': 3}}},
        }

    def _capture(self, pid, with_season=True):
        return {
            'profile': {
                'id': int(pid), 'name': f'Player {pid}', 'slug': f'p-{pid}',
                'position': 'F', 'height': 185, 'preferredFoot': 'Right',
                'dateOfBirthTimestamp': 1180483200,
                'team': {'id': 30, 'name': 'Brighton'}},
            'season_buffer': self._season_buffer(pid) if with_season else {},
        }

    @pytest.mark.unit
    def test_one_pass_yields_profile_and_season_frames(self):
        scraper = self._scraper()
        seen = []
        targets = []

        def fake_iter(player_ids, target_ut=None, target_year=None):
            targets.append((target_ut, target_year))
            for pid in player_ids:
                seen.append(str(pid))
                yield str(pid), self._capture(pid)

        scraper._iter_player_captures = fake_iter
        out = scraper.read_player_capture(
            league='ENG-Premier League', season=2025, player_ids=['101', '102'])

        assert seen == ["101", "102"]  # ONE capture per player ("one nav")
        assert targets == [(17, "25/26")]

        prof = out['player_profile']
        assert len(prof) == 2
        assert set(prof['season']) == {'2526'}
        assert set(prof['_entity_type']) == {'player_profile'}
        prow = {r['player_id']: r for r in prof.to_dict('records')}['101']
        # #840: Bronze source-key names (Silver renames height->height_cm etc.).
        assert prow['height'] == 185 and prow['preferred_foot'] == 'Right'
        assert prow['team_name'] == 'Brighton'

        # Season-aggregate frame: the season-guarded EPL overall, flattened.
        seas = out['player_season_stats']
        assert len(seas) == 2
        assert set(seas['season']) == {'2526'}
        assert set(seas['_entity_type']) == {'player_season_stats'}
        srow = {r['player_id']: r for r in seas.to_dict('records')}['101']
        assert srow['unique_tournament_id'] == 17
        assert srow['sofascore_season_id'] == 76986
        assert srow['team_name'] == 'Brighton'
        assert srow['rating'] == 7.0
        assert srow['total_goals'] == 3

    @pytest.mark.unit
    def test_world_cup_keeps_literal_target_year_and_partition(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        with (
            patch("scrapers.base.base_scraper.get_rate_limiter"),
            patch("scrapers.base.base_scraper.get_retry_policy"),
            patch("scrapers.base.base_scraper.get_circuit_breaker"),
            patch("scrapers.base.base_scraper.IcebergWriter"),
        ):
            scraper = SofaScoreScraper(
                leagues=["INT-World Cup"],
                seasons=[2026],
            )

        targets = []

        def fake_iter(player_ids, target_ut=None, target_year=None):
            targets.append((target_ut, target_year))
            yield "101", {
                "profile": self._capture("101")["profile"],
                "season_buffer": {},
            }

        scraper._iter_player_captures = fake_iter
        with patch(
            "scrapers.sofascore.scraper._is_single_year",
            return_value=True,
        ):
            out = scraper.read_player_capture(
                league="INT-World Cup",
                season=2026,
                player_ids=["101"],
            )

        assert targets == [(16, "2026")]
        assert set(out["player_profile"]["season"]) == {"2026"}

    @pytest.mark.unit
    def test_season_stats_empty_when_picker_missed(self):
        # Profile still lands, but the picker never surfaced the EPL overall
        # (transferred player) → player_season_stats is empty, not a crash.
        scraper = self._scraper()

        def fake_iter(player_ids, **kwargs):
            for pid in player_ids:
                yield str(pid), self._capture(pid, with_season=False)

        scraper._iter_player_captures = fake_iter
        out = scraper.read_player_capture(
            league='ENG-Premier League', season=2025, player_ids=['101'])
        assert len(out['player_profile']) == 1
        assert out['player_season_stats'].empty
        assert 'player_id' in out['player_season_stats'].columns

    @pytest.mark.unit
    def test_does_not_label_latest_stats_as_requested_season(self):
        scraper = self._scraper()
        pid = "101"
        wrong_buffer = {
            f"/api/v1/player/{pid}/statistics/seasons": {
                "status": 200,
                "challenge": False,
                "json": {
                    "uniqueTournamentSeasons": [
                        {
                            "uniqueTournament": {"id": 17},
                            "seasons": [{"year": "24/25", "id": 60000}],
                        }
                    ]
                },
            },
            f"/api/v1/player/{pid}/unique-tournament/17/season/60000/statistics/overall": {
                "status": 200,
                "challenge": False,
                "json": {"statistics": {"rating": 9.9}},
            },
        }
        scraper._iter_player_captures = lambda player_ids, **kwargs: iter(
            [
                (
                    pid,
                    {
                        "profile": self._capture(pid)["profile"],
                        "season_buffer": wrong_buffer,
                    },
                ),
            ]
        )

        out = scraper.read_player_capture(
            league="ENG-Premier League",
            season=2025,
            player_ids=[pid],
        )
        assert len(out["player_profile"]) == 1
        assert out["player_season_stats"].empty

    @pytest.mark.unit
    def test_graceful_empty_when_nothing_captured(self):
        scraper = self._scraper()

        def fake_iter(player_ids, **kwargs):
            for pid in player_ids:
                yield str(pid), {'profile': None, 'season_buffer': {}}

        scraper._iter_player_captures = fake_iter
        out = scraper.read_player_capture(
            league='ENG-Premier League', season=2025, player_ids=['101'])
        assert out['player_profile'].empty
        assert 'player_id' in out['player_profile'].columns
        assert out['player_season_stats'].empty


class TestReadLeagueTableCapture:
    """read_league_table resolves the season_id from the captured EVENTS — the
    /seasons map does NOT fire on the standings landing (live-proven #779: 0 of
    3 capture passes saw it), so the prior /seasons-only resolution returned 0
    rows even in-season. We patch the Camoufox seam with a real-shaped buffer
    (EPL 24/25) carrying events + standings but NO /seasons and assert the
    events fallback recovers the sid and flattens the table.
    """

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2024])

    def _patch_capture(self, buffer):
        fake = MagicMock()
        fake.__enter__.return_value.capture_buffer.return_value = buffer
        fake.__exit__.return_value = False
        return patch(
            'scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
            return_value=fake)

    def _buffer_no_seasons(self, sid=61627):
        """EPL 24/25 landing: events carry season {year:'24/25', id:61627};
        standings/total for that sid; NO /seasons (it never fires)."""
        return {
            f"/api/v1/unique-tournament/17/season/{sid}/events/round/1": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 1, "season": {"year": "24/25", "id": sid}}]}},
            f"/api/v1/unique-tournament/17/season/{sid}/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "rows": [
                        {"team": {"name": "Liverpool FC"}, "matches": 38, "wins": 25,
                         "draws": 9, "losses": 4, "scoresFor": 86,
                         "scoresAgainst": 41, "points": 84},
                        {"team": {"name": "Southampton"}, "matches": 38, "wins": 2,
                         "draws": 6, "losses": 30, "scoresFor": 26,
                         "scoresAgainst": 86, "points": 12}]}]}},
        }

    @pytest.mark.unit
    def test_resolves_sid_from_events_when_seasons_absent(self):
        # Arrange — the real live scenario: /seasons missing, events present.
        scraper = self._scraper()
        scraper._camoufox_proxy = lambda: None

        # Act
        with self._patch_capture(self._buffer_no_seasons()):
            df = scraper.read_league_table()

        # Assert — sid recovered from events → 2 rows, correctly labelled.
        assert df is not None and len(df) == 2
        rows = {r['team']: r for r in df.to_dict('records')}
        assert rows['Liverpool FC']['pts'] == 84
        assert rows['Liverpool FC']['gd'] == 45          # derived gf-ga
        assert set(df['season']) == {'2425'}             # 2024 -> soccerdata slug
        assert set(df['league']) == {'ENG-Premier League'}
        assert set(df['_entity_type']) == {'league_table'}

    @pytest.mark.unit
    def test_returns_none_when_target_season_not_served(self):
        # Off-season: the page serves only the NEXT season (26/27); target 24/25
        # is absent from both /seasons and events → skip (no empty overwrite).
        scraper = self._scraper()
        scraper._camoufox_proxy = lambda: None
        buf = {
            "/api/v1/unique-tournament/17/season/96668/events/round/1": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 9, "season": {"year": "26/27", "id": 96668}}]}},
            "/api/v1/unique-tournament/17/season/96668/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "rows": [
                        {"team": {"name": "X"}, "matches": 0, "wins": 0, "draws": 0,
                         "losses": 0, "scoresFor": 0, "scoresAgainst": 0,
                         "points": 0}]}]}},
        }
        with self._patch_capture(buf):
            df = scraper.read_league_table()
        assert df is None

    def _fetch_cap_cls(self, landing, fetch_map, init_counter=None):
        """Fake capture class: landing buffer + in-page fetch map (#879)."""
        class _Cap:
            def __init__(self, **kwargs):
                if init_counter is not None:
                    init_counter['n'] += 1

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def capture_buffer(self, nav_url):
                return dict(landing)

            def fetch_api_json(self, path):
                return fetch_map.get(path)

        return _Cap

    @pytest.mark.unit
    def test_historical_season_fetched_in_page(self):
        # #879: off-season/historical — the landing serves 26/27 only; the
        # target 24/25 sid comes from the in-page /seasons fetch and its table
        # from the in-page standings fetch, both keyed by EXACT sid (the same
        # season-guard as before — just no dependency on the SPA firing it).
        scraper = self._scraper()
        scraper._camoufox_proxy = lambda: None
        sid = 61627
        landing = {
            "/api/v1/unique-tournament/17/season/96668/events/round/1": {
                "status": 200, "challenge": False, "json": {"events": [
                    {"id": 9, "season": {"year": "26/27", "id": 96668}}]}},
        }
        fetch_map = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200, "challenge": False, "json": {"seasons": [
                    {"year": "26/27", "id": 96668},
                    {"year": "24/25", "id": sid}]}},
            f"/api/v1/unique-tournament/17/season/{sid}/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "rows": [
                        {"team": {"name": "Liverpool FC"}, "matches": 38,
                         "wins": 25, "draws": 9, "losses": 4, "scoresFor": 86,
                         "scoresAgainst": 41, "points": 84}]}]}},
        }

        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   self._fetch_cap_cls(landing, fetch_map)):
            df = scraper.read_league_table()

        assert df is not None and len(df) == 1
        assert df.iloc[0]['team'] == 'Liverpool FC'
        assert df.iloc[0]['pts'] == 84
        assert set(df['season']) == {'2425'}

    @pytest.mark.unit
    def test_zero_rows_with_answered_standings_not_retried(self):
        # A real 200 answer with zero rows for OUR sid is legitimately empty —
        # no retry burn (the retry budget is for transport/resolution misses).
        scraper = self._scraper()
        scraper._camoufox_proxy = MagicMock(return_value=None)
        sid = 61627
        fetch_map = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200, "challenge": False, "json": {"seasons": [
                    {"year": "24/25", "id": sid}]}},
            f"/api/v1/unique-tournament/17/season/{sid}/standings/total": {
                "status": 200, "challenge": False, "json": {"standings": [
                    {"type": "total", "rows": []}]}},
        }
        sessions = {'n': 0}

        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   self._fetch_cap_cls({}, fetch_map, init_counter=sessions)):
            df = scraper.read_league_table()

        assert df is None
        assert sessions['n'] == 1


class TestTournamentSnapshot:
    @pytest.mark.unit
    def test_world_cup_keeps_literal_snapshot_target_and_partition(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        with (
            patch("scrapers.base.base_scraper.get_rate_limiter"),
            patch("scrapers.base.base_scraper.get_retry_policy"),
            patch("scrapers.base.base_scraper.get_circuit_breaker"),
            patch("scrapers.base.base_scraper.IcebergWriter"),
        ):
            scraper = SofaScoreScraper(
                leagues=["INT-World Cup"],
                seasons=[2026],
            )
        scraper._capture_tournament_snapshot = MagicMock(
            return_value={"events": [], "standings": []},
        )

        with patch(
            "scrapers.sofascore.scraper._is_single_year",
            return_value=True,
        ):
            schedule, table = scraper.read_tournament_snapshot()

        assert schedule is None and table is None
        args = scraper._capture_tournament_snapshot.call_args.args
        assert args[2:] == ("INT-World Cup", "2026", "2026")

    @pytest.mark.parametrize(
        ("rec", "expected"),
        [
            ({"status": 204, "json": None, "challenge": None}, True),
            (
                {
                    "status": 404,
                    "json": {"error": {"code": 404}},
                    "challenge": False,
                },
                True,
            ),
            ({"status": 404, "json": None, "challenge": None}, False),
            (
                {
                    "status": 403,
                    "json": {"error": {"code": 403}},
                    "challenge": False,
                },
                False,
            ),
            ({"status": 429, "json": {}, "challenge": False}, False),
            ({"status": 503, "json": {}, "challenge": False}, False),
        ],
    )
    def test_terminal_empty_classifier(self, rec, expected):
        from scrapers.sofascore.scraper import SofaScoreScraper

        assert SofaScoreScraper._rec_terminal_empty(rec) is expected

    @pytest.mark.unit
    def test_empty_page_that_promises_more_is_incomplete(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        prefix = "/api/v1/unique-tournament/17/season/76986/events"
        buffer = {
            f"{prefix}/last/0": {
                "status": 200,
                "challenge": False,
                "json": {"events": [{"id": 1}], "hasNextPage": True},
            },
            f"{prefix}/last/1": {
                "status": 200,
                "challenge": False,
                "json": {"events": [], "hasNextPage": True},
            },
        }

        assert not SofaScoreScraper._event_direction_complete(
            buffer,
            prefix,
            "last",
        )
    @pytest.mark.unit
    def test_schedule_and_standings_share_one_browser_session(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        with (
            patch("scrapers.base.base_scraper.get_rate_limiter"),
            patch("scrapers.base.base_scraper.get_retry_policy"),
            patch("scrapers.base.base_scraper.get_circuit_breaker"),
            patch("scrapers.base.base_scraper.IcebergWriter"),
        ):
            scraper = SofaScoreScraper(
                leagues=["ENG-Premier League"],
                seasons=[2025],
            )
        season = {"year": "25/26", "id": 76986}
        buffer = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200,
                "challenge": False,
                "json": {"seasons": [season]},
            },
            "/api/v1/unique-tournament/17/season/76986/events/last/0": {
                "status": 200,
                "challenge": False,
                "json": {
                    "events": [
                        {
                            "id": 1,
                            "season": season,
                            "status": {"type": "finished"},
                            "startTimestamp": 1750000000,
                            "homeTeam": {"name": "A"},
                            "awayTeam": {"name": "B"},
                        }
                    ]
                },
            },
            "/api/v1/unique-tournament/17/season/76986/events/next/0": {
                "status": 200,
                "challenge": False,
                "json": {"events": []},
            },
            "/api/v1/unique-tournament/17/season/76986/standings/total": {
                "status": 200,
                "challenge": False,
                "json": {
                    "standings": [
                        {
                            "type": "total",
                            "rows": [
                                {
                                    "team": {"name": "A"},
                                    "matches": 1,
                                    "wins": 1,
                                    "draws": 0,
                                    "losses": 0,
                                    "scoresFor": 2,
                                    "scoresAgainst": 0,
                                    "points": 3,
                                }
                            ],
                        }
                    ]
                },
            },
        }
        sessions = []

        class _Cap:
            def __init__(self, **kwargs):
                sessions.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def capture_buffer(self, nav_url):
                return dict(buffer)

            def paginate_tournament_season(
                self,
                ut_id,
                sid,
                max_pages=25,
                *,
                include_next=False,
            ):
                return dict(buffer)

            def fetch_api_json(self, path):
                return buffer.get(path)

        with patch(
            "scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture",
            _Cap,
        ):
            schedule, table = scraper.read_tournament_snapshot()

        assert len(sessions) == 1
        assert schedule is not None and schedule["game_id"].tolist() == [1]
        assert table is not None and table["team"].tolist() == ["A"]

    @pytest.mark.unit
    def test_partial_later_page_retries_on_fresh_session(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        with (
            patch("scrapers.base.base_scraper.get_rate_limiter"),
            patch("scrapers.base.base_scraper.get_retry_policy"),
            patch("scrapers.base.base_scraper.get_circuit_breaker"),
            patch("scrapers.base.base_scraper.IcebergWriter"),
        ):
            scraper = SofaScoreScraper(
                leagues=["ENG-Premier League"],
                seasons=[2025],
            )
        scraper._proxy_manager = None
        season = {"year": "25/26", "id": 76986}
        seasons_rec = {
            "/api/v1/unique-tournament/17/seasons": {
                "status": 200,
                "challenge": False,
                "json": {"seasons": [season]},
            }
        }
        event1 = {
            "id": 1,
            "season": season,
            "status": {"type": "finished"},
            "startTimestamp": 1750000000,
            "homeTeam": {"id": 1, "name": "A"},
            "awayTeam": {"id": 2, "name": "B"},
        }
        event2 = {
            "id": 2,
            "season": season,
            "status": {"type": "finished"},
            "startTimestamp": 1750003600,
            "homeTeam": {"id": 3, "name": "C"},
            "awayTeam": {"id": 4, "name": "D"},
        }
        first = {
            **seasons_rec,
            "/api/v1/unique-tournament/17/season/76986/events/last/0": {
                "status": 200,
                "challenge": False,
                "json": {"events": [event1], "hasNextPage": True},
            },
            "/api/v1/unique-tournament/17/season/76986/events/next/0": {
                "status": 200,
                "challenge": False,
                "json": {"events": []},
            },
            # A terminal response is valid only on page zero. Page one was
            # promised by hasNextPage=True, so this remains a partial capture.
            "/api/v1/unique-tournament/17/season/76986/events/last/1": {
                "status": 404,
                "challenge": False,
                "json": {"error": {"code": 404, "reason": "Not Found"}},
            },
        }
        complete = {
            **first,
            "/api/v1/unique-tournament/17/season/76986/events/last/1": {
                "status": 200,
                "challenge": False,
                "json": {"events": [event2], "hasNextPage": False},
            },
        }
        sessions = []

        class _Cap:
            def __init__(self, **kwargs):
                self.buffer = first if not sessions else complete
                sessions.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def capture_buffer(self, nav_url):
                return dict(self.buffer)

            def paginate_tournament_season(self, *args, **kwargs):
                return dict(self.buffer)

        with patch(
            "scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture",
            _Cap,
        ):
            df = scraper.read_schedule()

        assert len(sessions) == 2
        assert df is not None and sorted(df["game_id"].tolist()) == [1, 2]


class TestMatchCaptureSessionRestart:
    """_iter_match_captures restarts the Camoufox session every ``session_max``
    matches so a full-season backfill survives the ~200-navigation Firefox
    crash that capped coverage at ~55% (#829)."""

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            s = SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2024])
        s._proxy_manager = None
        return s

    @staticmethod
    def _fakecap_cls(instances):
        class _FakeCap:
            def __init__(self, *a, **k):
                instances.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def capture_event(self, mid, tabs=(), required=(), **kwargs):
                return {"lineups": {"home": {}, "away": {}}, "event": {}}

        return _FakeCap

    @pytest.mark.unit
    def test_restarts_session_every_n_matches(self):
        scraper = self._scraper()
        instances = []
        ids = [str(i) for i in range(250)]
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   self._fakecap_cls(instances)):
            out = list(scraper._iter_match_captures(ids, session_max=100))
        # Every match yielded, in order — none dropped at a chunk boundary.
        assert [m for m, _ in out] == ids
        # 250 / 100 -> 3 browser sessions (100, 100, 50).
        assert len(instances) == 3

    @pytest.mark.unit
    def test_single_session_under_threshold(self):
        # Daily run (a handful of matches) keeps ONE session — behaviour unchanged.
        scraper = self._scraper()
        instances = []
        ids = [str(i) for i in range(5)]
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   self._fakecap_cls(instances)):
            out = list(scraper._iter_match_captures(ids, session_max=120))
        assert len(out) == 5
        assert len(instances) == 1

    @pytest.mark.unit
    def test_rotates_proxy_after_consecutive_failures(self):
        # A dead proxy must rotate immediately and retry the SAME match.
        scraper = self._scraper()
        instances = []

        class _DeadThenOk:
            def __init__(self, *a, **k):
                self.idx = len(instances) + 1
                instances.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def capture_event(self, mid, tabs=(), required=(), **kwargs):
                if self.idx == 1:
                    return {}  # dead proxy — no lineups
                return {'lineups': {'home': {}, 'away': {}}, 'event': {}}

        ids = [str(k) for k in range(20)]
        with patch(
            "scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture", _DeadThenOk
        ):
            out = list(
                scraper._iter_match_captures(ids, session_max=100, item_max_attempts=2)
            )
        # Every match_id is still yielded, in order.
        assert [m for m, _ in out] == ids
        # Rotated to (at least) a second session after the dead one.
        assert len(instances) >= 2
        # No hole: match 0 is retried on the second session before advancing.
        assert all(ep.get("lineups") is not None for _, ep in out)


class TestMatchCaptureInPageFetch:
    """#842 in-page fetch: only the session's FIRST match navigates (solves
    Turnstile); later matches pull their endpoints via same-origin fetch. A
    fetch that misses a required endpoint falls back to a full navigation for
    that match; SOFASCORE_INPAGE_FETCH=0 restores nav-per-match."""

    _GOOD = {'lineups': {'home': {}, 'away': {}}, 'event': {}}

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            s = SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2024])
        s._proxy_manager = None
        return s

    def _fakecap_cls(self, calls, fetch_results=None):
        good = self._GOOD

        class _FakeCap:
            def __init__(self, *a, **k):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def capture_event(self, mid, tabs=(), required=(), **kwargs):
                calls.append(("nav", mid))
                return good

            def fetch_event(self, mid, names=()):
                calls.append(('fetch', mid))
                if fetch_results is not None:
                    return fetch_results.get(mid, good)
                return good
        return _FakeCap

    @pytest.mark.unit
    def test_first_match_navigates_then_fetches(self):
        scraper = self._scraper()
        calls = []
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   self._fakecap_cls(calls)):
            out = list(scraper._iter_match_captures(['1', '2', '3']))
        assert [m for m, _ in out] == ['1', '2', '3']
        assert calls == [('nav', '1'), ('fetch', '2'), ('fetch', '3')]

    @pytest.mark.unit
    def test_fetch_miss_falls_back_to_navigation(self):
        # Match 2's fetch misses the required lineups (clearance expired) —
        # it re-navigates; match 3 goes back to the cheap fetch path.
        scraper = self._scraper()
        calls = []
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   self._fakecap_cls(calls, fetch_results={'2': {}})):
            out = list(scraper._iter_match_captures(['1', '2', '3']))
        assert calls == [('nav', '1'),
                         ('fetch', '2'), ('nav', '2'),
                         ('fetch', '3')]
        # The fallback navigation recovered match 2 — nothing lost.
        assert all(ep.get('lineups') for _, ep in out)
        assert (
            scraper.get_traffic_stats()["browser_fallback_navigations"] == 1
        )

    @pytest.mark.unit
    def test_kill_switch_restores_nav_per_match(self, monkeypatch):
        monkeypatch.setenv('SOFASCORE_INPAGE_FETCH', '0')
        scraper = self._scraper()
        calls = []
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   self._fakecap_cls(calls)):
            list(scraper._iter_match_captures(['1', '2']))
        assert calls == [('nav', '1'), ('nav', '2')]

    @pytest.mark.unit
    def test_cap_without_fetch_event_degrades_to_nav(self):
        # A capture layer without fetch_event (or a fetch bug) must degrade to
        # the old nav-per-match behaviour, not fail the run.
        scraper = self._scraper()
        calls = []

        class _NavOnly:
            def __init__(self, *a, **k):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def capture_event(self, mid, tabs=(), required=(), **kwargs):
                calls.append(("nav", mid))
                return {"lineups": {"home": {}, "away": {}}, "event": {}}

        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   _NavOnly):
            out = list(scraper._iter_match_captures(['1', '2']))
        assert [m for m, _ in out] == ['1', '2']
        assert calls == [('nav', '1'), ('nav', '2')]
        assert all(ep.get('lineups') for _, ep in out)

    @pytest.mark.unit
    def test_terminal_optional_404_does_not_rotate_proxy(self):
        scraper = self._scraper()
        sessions = []

        class _TerminalShotmap404:
            def __init__(self, *args, **kwargs):
                sessions.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def capture_event(self, mid, **kwargs):
                return {
                    'event': {},
                    'lineups': {'home': {}, 'away': {}},
                    'statistics': {'statistics': []},
                }

            def event_endpoint_states(self, mid, names=None):
                return {
                    'event': 'success',
                    'lineups': 'success',
                    'statistics': 'success',
                    'shotmap': 'not_available',
                }

        with patch(
            'scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
            _TerminalShotmap404,
        ):
            out = list(
                scraper._iter_match_captures(
                    ['1'],
                    required=('event', 'lineups', 'statistics', 'shotmap'),
                )
            )

        assert len(sessions) == 1
        assert out[0][1]['_endpoint_states']['shotmap'] == 'not_available'

    @pytest.mark.unit
    def test_optional_transient_miss_retries_json_without_navigation(self):
        scraper = self._scraper()
        calls = []

        class _CheapOptionalRetry:
            def __init__(self, *args, **kwargs):
                self.last_names = ()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def capture_event(self, mid, **kwargs):
                calls.append(('nav', mid))
                return self_outer._GOOD

            def fetch_event(self, mid, names=()):
                self.last_names = tuple(names)
                calls.append(('fetch', mid, self.last_names))
                if set(names) == {'statistics', 'shotmap'}:
                    return {
                        'statistics': {'statistics': []},
                        'shotmap': {'shotmap': []},
                    }
                return {'event': {}, 'lineups': {'home': {}, 'away': {}}}

            def event_endpoint_states(self, mid, names=None):
                return {
                    name: (
                        'success'
                        if name in {'event', 'lineups'}
                        or set(self.last_names) == {'statistics', 'shotmap'}
                        else 'server_error'
                    )
                    for name in names or ()
                }

        self_outer = self
        with patch(
            'scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
            _CheapOptionalRetry,
        ):
            out = list(
                scraper._iter_match_captures(
                    ['1', '2'],
                    tabs=('Lineups', 'Statistics', 'Shotmap'),
                    required=('event', 'lineups'),
                )
            )

        assert [call[:2] for call in calls] == [
            ('nav', '1'),
            ('fetch', '1'),
            ('fetch', '2'),
            ('fetch', '2'),
        ]
        assert out[1][1]['_endpoint_states']['shotmap'] == 'success'
        assert scraper.get_traffic_stats()['browser_fallback_navigations'] == 0


class TestPlayerCaptureInPageFetch:
    """#842 players: only the session's FIRST player navigates (solves
    Turnstile); later players pull /api/v1/player/{id} (+ season endpoints)
    via same-origin fetch. A fetch that misses the profile falls back to a
    full navigation; SOFASCORE_INPAGE_FETCH=0 restores nav-per-player."""

    _GOOD = {'profile': {'id': 1, 'name': 'X'}, 'season_buffer': {}}

    def _scraper(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        with patch('scrapers.base.base_scraper.get_rate_limiter'), \
             patch('scrapers.base.base_scraper.get_retry_policy'), \
             patch('scrapers.base.base_scraper.get_circuit_breaker'), \
             patch('scrapers.base.base_scraper.IcebergWriter'):
            s = SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2024])
        s._proxy_manager = None
        return s

    def _fakecap_cls(self, calls, fetch_results=None):
        good = self._GOOD

        class _FakeCap:
            def __init__(self, *a, **k):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def capture_player(self, pid, target_ut=None, target_year=None):
                calls.append(("nav", pid, target_ut, target_year))
                return good

            def fetch_player(self, pid, target_ut=None, target_year=None):
                calls.append(('fetch', pid, target_ut, target_year))
                if fetch_results is not None:
                    return fetch_results.get(pid, good)
                return good
        return _FakeCap

    @pytest.mark.unit
    def test_first_player_navigates_then_fetches_with_target(self):
        scraper = self._scraper()
        calls = []
        with patch(
            "scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture",
            self._fakecap_cls(calls),
        ):
            out = list(
                scraper._iter_player_captures(
                    ["1", "2", "3"], target_ut=17, target_year="25/26"
                )
            )
        assert [p for p, _ in out] == ["1", "2", "3"]
        # First player warms the session via navigation; the rest fetch with
        # the (ut, year) target for the precise season-stats resolution.
        assert calls == [
            ("nav", "1", 17, "25/26"),
            ("fetch", "2", 17, "25/26"),
            ("fetch", "3", 17, "25/26"),
        ]

    @pytest.mark.unit
    def test_fetch_miss_falls_back_to_navigation(self):
        scraper = self._scraper()
        calls = []
        miss = {'profile': None, 'season_buffer': {}}
        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   self._fakecap_cls(calls, fetch_results={'2': miss})):
            out = list(scraper._iter_player_captures(
                ['1', '2', '3'], target_ut=17, target_year='25/26'))
        assert [c[:2] for c in calls] == [('nav', '1'),
                                          ('fetch', '2'), ('nav', '2'),
                                          ('fetch', '3')]
        # The fallback navigation recovered player 2 — nothing lost.
        assert all(c.get('profile') for _, c in out)
        assert (
            scraper.get_traffic_stats()["browser_fallback_navigations"] == 1
        )

    @pytest.mark.unit
    def test_kill_switch_restores_nav_per_player(self, monkeypatch):
        monkeypatch.setenv('SOFASCORE_INPAGE_FETCH', '0')
        scraper = self._scraper()
        calls = []
        with patch(
            "scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture",
            self._fakecap_cls(calls),
        ):
            list(scraper._iter_player_captures(["1", "2"]))
        assert [c[:2] for c in calls] == [("nav", "1"), ("nav", "2")]

    @pytest.mark.unit
    def test_cap_without_fetch_player_degrades_to_nav(self):
        scraper = self._scraper()
        calls = []

        class _NavOnly:
            def __init__(self, *a, **k):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def capture_player(self, pid, target_ut=None, target_year=None):
                calls.append(("nav", pid))
                return {"profile": {"id": int(pid)}, "season_buffer": {}}

        with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
                   _NavOnly):
            out = list(scraper._iter_player_captures(['1', '2']))
        assert calls == [('nav', '1'), ('nav', '2')]
        assert all(c.get('profile') for _, c in out)


class TestSeasonToShort:
    """_season_to_short — shared season-token normalizer (extracted from 10
    inline copies that mapped an already-short '2526' to a nonexistent '2627')."""

    @pytest.fixture
    def season_to_short(self):
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            from scrapers.sofascore.scraper import _season_to_short
        return _season_to_short

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "token, expected",
        [
            (2024, "2425"),  # year-start int
            (2021, "2122"),  # DAG int is the 2021/22 start year
            ("2025", "2526"),  # year-start str
            ("2526", "2526"),  # already-short passthrough (old inline code -> '2627')
            ("2021", "2021"),  # explicit string keeps short 20/21 meaning
            (1999, "9900"),  # century wrap
            ("9900", "9900"),  # already-short century wrap passthrough
            ("abc", "abc"),  # non-4-digit passthrough (legacy else branch)
            ("25/26", "25/26"),  # non-digit passthrough
        ],
    )
    def test_normalizes_tokens(self, season_to_short, token, expected):
        assert season_to_short(token) == expected
