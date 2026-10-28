"""Offline source-cache replay and semantic diagnostic regressions (#1438)."""
import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from scrapers.understat.anomalies import diagnose_matches
from scrapers.understat.catalog import UnderstatScope
from scrapers.understat.parsers import parse_match_payload

pytestmark = pytest.mark.unit


FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures/understat/anomalies_1438'


def _season_frames():
    with gzip.open(FIXTURES / 'season.json.gz', 'rt') as stream:
        payload = json.load(stream)
    frames = {'understat_schedule': [], 'understat_shots': [], 'understat_player_match_stats': []}
    for league, entities in payload.items():
        for source, entity in (('schedule', 'understat_schedule'), ('shots', 'understat_shots'),
                               ('players', 'understat_player_match_stats')):
            data = entities[source]
            frame = pd.DataFrame(data['rows'], columns=data['columns'])
            frames[entity].append(frame.assign(league=league, season='2627'))
    return {entity: pd.concat(parts, ignore_index=True) for entity, parts in frames.items()}


def test_real_seasons_without_injection_have_no_clone_or_repeat_player_singletons():
    frames = _season_frames()
    assert frames['understat_schedule'].groupby('league').size().to_dict() == {
        'FRA-Ligue 1': 72, 'RUS-Premier League': 96}
    before = {key: frame.copy(deep=True) for key, frame in frames.items()}
    signals = diagnose_matches(frames)
    assert not any(signal['code'].startswith('identical_match_') for signal in signals)
    players = frames['understat_player_match_stats']
    counts = players.groupby(['league', 'season', 'team_id', 'player_id']).game_id.nunique()
    for signal in signals:
        if signal['code'] == 'single_match_participation':
            details = signal['details']
            league, season, _ = details['match']
            assert counts.loc[league, season, int(details['team_id']), int(details['player_id'])] == 1
    shuffled = {key: frame.sample(frac=1, random_state=42) for key, frame in frames.items()}
    assert sorted(map(repr, diagnose_matches(shuffled))) == sorted(map(repr, signals))
    for key in frames:
        pd.testing.assert_frame_equal(frames[key], before[key])


def test_real_seasons_with_injected_30539_clone_report_31948_and_foreign_players():
    frames = _season_frames()
    schedule = frames['understat_schedule'].set_index('game_id')
    source, target = schedule.loc[30539], schedule.loc[31948]
    for entity in ('understat_shots', 'understat_player_match_stats'):
        frame = frames[entity]
        clone = frame[frame.game_id == 30539].copy()
        # Match parser takes shot teams from the target schedule, but preserves
        # roster team IDs from the source body. Reproduce that source injection.
        if entity == 'understat_shots':
            clone['team_id'] = clone.team_id.map({source.home_team_id: target.home_team_id,
                                                 source.away_team_id: target.away_team_id})
        clone = clone.assign(league='FRA-Ligue 1', game_id=31948)
        frames[entity] = pd.concat([frame[frame.game_id != 31948], clone], ignore_index=True)
    signals = diagnose_matches(frames)
    expected = {('FRA-Ligue 1', '2627', '31948'), ('RUS-Premier League', '2627', '30539')}
    for code in ('identical_match_roster', 'identical_match_shots'):
        assert any(signal['code'] == code and
                   set(map(tuple, signal['details']['matches'])) == expected for signal in signals)
    players = frames['understat_player_match_stats']
    foreign = players[players.game_id == 31948]
    assert len(foreign) == 31
    singletons = {signal['details']['player_id'] for signal in signals
                  if signal['code'] == 'single_match_participation' and
                  signal['details']['match'] == ['FRA-Ligue 1', '2627', '31948']}
    assert singletons == set(foreign.player_id.astype(str))


def test_30804_cache_reports_duplicate_rosters_and_cleared_ids_are_negative_control():
    with gzip.open(FIXTURES / 'match_30804.json.gz', 'rt') as stream:
        payload = json.load(stream)
    scope = UnderstatScope('ESP-La Liga', 'offline', 1, '2627', 2026, False)
    row = {'game_id': 30804, 'game': 'cache replay', 'home_team': 'Villarreal',
           'away_team': 'Deportivo La Coruna',
           'home_team_id': int(next(iter(payload['rosters']['h'].values()))['team_id']),
           'away_team_id': int(next(iter(payload['rosters']['a'].values()))['team_id'])}
    shots, players = parse_match_payload(payload, scope, row)
    frames = {'understat_shots': shots, 'understat_player_match_stats': players}
    signals = diagnose_matches(frames)
    duplicates = [signal for signal in signals if signal['code'] == 'multiple_roster_entries']
    assert duplicates and all(signal['details']['match'] == ['ESP-La Liga', '2627', '30804']
                              for signal in duplicates)
    clean = players.copy()
    clean['roster_entry_ids'] = clean.roster_entry_ids.str.split(',').str[0]
    assert not any(signal['code'] == 'multiple_roster_entries' for signal in
                   diagnose_matches({**frames, 'understat_player_match_stats': clean}))


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
