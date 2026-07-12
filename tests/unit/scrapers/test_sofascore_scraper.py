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

    @pytest.mark.parametrize(
        "proxy_kwargs",
        [
            {"proxy": "http://paid.invalid:9999"},
            {"proxy_file": "/opt/airflow/proxys.txt"},
        ],
    )
    def test_standalone_proxy_configuration_is_rejected(self, proxy_kwargs):
        from scrapers.sofascore import SofaScoreScraper

        with pytest.raises(ValueError, match="common capture engine"):
            SofaScoreScraper(
                leagues=['ENG-Premier League'],
                seasons=[2025],
                **proxy_kwargs,
            )


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
