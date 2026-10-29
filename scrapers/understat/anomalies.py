"""Read-only source diagnostics; every signal is a warning, never a gate."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Mapping

import pandas as pd


# Explicit semantic columns: source IDs for shots/rosters, scope labels, dates,
# batch metadata and schedule-derived team names must not mask a source clone.
SHOT_CONTENT = (
    'team_side', 'player_id', 'assist_player', 'xg', 'location_x', 'location_y', 'minute',
    'body_part', 'situation', 'result', 'last_action',
)
ROSTER_CONTENT = (
    'team_side', 'team_id', 'player_id', 'position', 'minutes', 'goals',
    'own_goals', 'shots', 'xg', 'assists', 'xa', 'key_passes',
    'yellow_cards', 'red_cards', 'xg_chain', 'xg_buildup',
)


def _value(value):
    return None if pd.isna(value) else str(value)


def _identity(row):
    return tuple(_value(row.get(key)) for key in ('league', 'season', 'game_id'))


def diagnose_matches(frames: Mapping[str, pd.DataFrame]) -> list[dict]:
    """Compare all supplied scopes in one pass, preserving row multiplicity.

    Singleton participation can mean a debut or transfer, not a source error.
    Missing observations are reported separately, never interpreted as zero.
    """
    signals = []

    def emit(code, **details):
        if 'match' in details:
            details['match'] = list(details['match'])
        if 'matches' in details:
            details['matches'] = [list(match) for match in details['matches']]
        signals.append({'code': code, 'details': details})

    schedule = frames.get('understat_schedule', pd.DataFrame()).to_dict('records')
    players = frames.get('understat_player_match_stats', pd.DataFrame()).to_dict('records')
    shots = frames.get('understat_shots', pd.DataFrame()).to_dict('records')
    schedules = {_identity(row): row for row in schedule}
    for shot in shots:
        game = schedules.get(_identity(shot), {})
        team = _value(shot.get('team_id'))
        shot['team_side'] = next((side for side, key in
            (('h', 'home_team_id'), ('a', 'away_team_id'))
            if team is not None and team == _value(game.get(key))), None)
    groups = {}
    for entity, rows, columns in (
        ('roster', players, ROSTER_CONTENT), ('shots', shots, SHOT_CONTENT),
    ):
        grouped = defaultdict(list)
        for row in rows:
            grouped[_identity(row)].append(row)
        groups[entity] = grouped
        signatures = defaultdict(list)
        for match, records in sorted(grouped.items(), key=repr):
            content = Counter(tuple(_value(row.get(col)) for col in columns) for row in records)
            # Sorting repr handles nullable fields without ordering None vs str.
            signature = tuple(sorted(content.items(), key=repr))
            signatures[signature].append(match)
            if entity == 'shots' and any(count > 1 for count in content.values()):
                emit('duplicate_shot_content', match=match,
                     extra_rows=sum(count - 1 for count in content.values()))
        for matches in signatures.values():
            if len(matches) > 1:
                emit(f'identical_match_{entity}', matches=matches)

    participation = defaultdict(set)
    for row in players:
        match = _identity(row)
        key = (*match[:2], _value(row.get('team_id')), _value(row.get('player_id')))
        participation[key].add(match)
        ids = str(row.get('roster_entry_ids', '')).split(',')
        if len(set(ids)) > 1:
            emit('multiple_roster_entries', match=match, player_id=key[-1], roster_entry_ids=ids)
    for key, matches in sorted(participation.items(), key=repr):
        if len(matches) == 1:
            emit('single_match_participation', match=next(iter(matches)),
                 team_id=key[-2], player_id=key[-1], explanation='possible debut or transfer')

    for row in schedule:
        if pd.isna(row.get('is_result')) or not row.get('is_result'):
            continue
        match = _identity(row)
        observed = groups['shots'].get(match, [])
        teams = [_value(row.get('home_team_id')), _value(row.get('away_team_id'))]
        score = [pd.to_numeric(row.get(key), errors='coerce')
                 for key in ('home_goals', 'away_goals')]
        if not observed or any(pd.isna(value) for value in (*teams, *score)):
            emit('goal_score_unavailable', match=match)
            continue
        goals = [0, 0]
        unknown = False
        for shot in observed:
            team = _value(shot.get('team_id'))
            if team not in teams or pd.isna(shot.get('result')):
                unknown = True
                continue
            if shot['result'] in ('Goal', 'Own Goal'):
                side = teams.index(team)
                goals[1 - side if shot['result'] == 'Own Goal' else side] += 1
        if unknown:
            emit('goal_score_unavailable', match=match)
        elif goals != [int(value) for value in score]:
            emit('shot_goals_score_mismatch', match=match, shot_goals=goals,
                 score=[int(value) for value in score])
    return signals
