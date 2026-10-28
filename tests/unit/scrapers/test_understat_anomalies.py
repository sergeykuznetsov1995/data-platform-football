"""Offline source-cache replay and semantic diagnostic regressions (#1438)."""
import json
from pathlib import Path

import pandas as pd
import pytest

from scrapers.understat.anomalies import diagnose_matches
from scrapers.understat.catalog import UnderstatScope
from scrapers.understat.parsers import parse_match_payload

pytestmark = pytest.mark.unit


def test_production_cache_replay_reports_31948_participation_and_30804_rosters():
    root = Path(__file__).resolve().parents[2] / 'fixtures/understat/anomalies_1438'
    frames = {'understat_shots': [], 'understat_player_match_stats': []}
    for game_id, league in ((31948, 'FRA-Ligue 1'), (30539, 'RUS-Premier League'), (30804, 'ESP-La Liga')):
        payload = json.loads((root / f'match_{game_id}.json').read_text())
        scope = UnderstatScope(league, 'offline', 1, '2627', 2026, False)
        # Cache contains match bodies only: schedule labels below are synthetic.
        row = {'game_id': game_id, 'game': 'cache replay', 'home_team': 'H', 'away_team': 'A',
               'home_team_id': 1, 'away_team_id': 2}
        shots, players = parse_match_payload(payload, scope, row)
        frames['understat_shots'].append(shots)
        frames['understat_player_match_stats'].append(players)
    frames = {key: pd.concat(parts, ignore_index=True) for key, parts in frames.items()}
    before = {key: frame.copy(deep=True) for key, frame in frames.items()}
    signals = diagnose_matches(frames)
    # The supplied 28.10 cache no longer contains the 23.09 cross-league clone.
    assert not any(signal['code'].startswith('identical_match_') for signal in signals)
    assert any(signal['code'] == 'single_match_participation' and
               signal['details']['match'][2] == '31948' for signal in signals)
    assert any(signal['code'] == 'multiple_roster_entries' and
               signal['details']['match'][2] == '30804' for signal in signals)
    shuffled = {key: frame.sample(frac=1, random_state=42) for key, frame in frames.items()}
    assert sorted(map(repr, diagnose_matches(shuffled))) == sorted(map(repr, signals))
    for key in frames:
        pd.testing.assert_frame_equal(frames[key], before[key])


def _frames():
    base = {'league': 'L', 'season': '2627', 'game_id': 1}
    schedule = [{**base, 'is_result': True, 'home_team_id': 1, 'away_team_id': 2,
                 'home_goals': 1, 'away_goals': 1}]
    shots = [{**base, 'team_id': 1, 'player_id': 10, 'shot_id': 1, 'result': 'Goal'},
             {**base, 'team_id': 1, 'player_id': 11, 'shot_id': 2, 'result': 'Own Goal'}]
    return {'understat_schedule': pd.DataFrame(schedule), 'understat_shots': pd.DataFrame(shots)}


def test_own_goal_score_duplicate_shot_and_missing_data_are_distinct():
    frames = _frames()
    assert diagnose_matches(frames) == []
    shots = frames['understat_shots']
    frames['understat_shots'] = pd.concat([shots, shots.iloc[[0]].assign(shot_id=999)])
    codes = {signal['code'] for signal in diagnose_matches(frames)}
    assert codes == {'duplicate_shot_content', 'shot_goals_score_mismatch'}
    frames['understat_shots'] = shots.iloc[:0]
    assert [signal['code'] for signal in diagnose_matches(frames)] == ['goal_score_unavailable']
    assert diagnose_matches({}) == []


def test_player_participation_is_scoped_to_team_league_season_and_other_games():
    rows = [{'league': 'L', 'season': '2627', 'game_id': game, 'team_id': 1, 'player_id': 10,
             'minutes': game} for game in (1, 2)]
    assert not any(signal['code'] == 'single_match_participation' for signal in
                   diagnose_matches({'understat_player_match_stats': pd.DataFrame(rows)}))
    rows[1]['team_id'] = 2
    assert sum(signal['code'] == 'single_match_participation' for signal in
               diagnose_matches({'understat_player_match_stats': pd.DataFrame(rows)})) == 2


def test_clone_detection_ignores_scope_ids_and_order_but_preserves_multiplicity():
    frames = _frames()
    shots = frames['understat_shots']
    clone = shots.assign(league='OTHER', game_id=2, shot_id=[100, 101])
    frames['understat_schedule'] = pd.concat([frames['understat_schedule'],
        frames['understat_schedule'].assign(league='OTHER', game_id=2)])
    frames['understat_shots'] = pd.concat([shots, clone])
    signals = diagnose_matches(frames)
    assert any(signal['code'] == 'identical_match_shots' for signal in signals)
    frames['understat_shots'] = pd.concat([shots, clone, clone.iloc[[0]].assign(shot_id=102)])
    assert not any(signal['code'] == 'identical_match_shots' for signal in diagnose_matches(frames))
    roster = pd.DataFrame([dict(league='L', season='2627', game_id=1, player_id=10),
                           dict(league='OTHER', season='2627', game_id=2, player_id=10)])
    assert any(signal['code'] == 'identical_match_roster' for signal in
               diagnose_matches({'understat_player_match_stats': roster}))


def test_same_shot_content_for_opposite_sides_is_not_a_duplicate():
    frames = _frames()
    shots = frames['understat_shots'].iloc[[0]]
    frames['understat_shots'] = pd.concat([shots, shots.assign(shot_id=9, team_id=2)])
    assert not any(signal['code'] == 'duplicate_shot_content'
                   for signal in diagnose_matches(frames))
