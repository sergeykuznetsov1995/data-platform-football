from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dags.utils import transfermarkt_dq_contracts as dq


ENTITIES = {
    'squad_memberships',
    'player_attribute_observations',
    'player_contract_observations',
    'market_value_points',
    'transfer_events',
    'coach_profiles',
    'coach_stints',
}


def _input(**overrides):
    values = dict(
        competition_type='continental_club',
        team_type='club',
        current=True,
        expected_team_ids=('1', '2'),
        observed_team_ids=('2', '1'),
        endpoint_status_by_team={'1': 'ok', '2': 'ok'},
        entity_statuses={entity: 'ok' for entity in ENTITIES},
        fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    values.update(overrides)
    return dq.ScopeDQInput(**values)


@pytest.mark.parametrize(
    'competition_type',
    ['domestic_cup', 'continental_club', 'national_team_tournament'],
)
def test_cup_and_international_contracts_are_strict(competition_type):
    report = dq.validate_scope_capture(
        _input(competition_type=competition_type),
        expected_entities=ENTITIES,
    )
    assert report['strict'] is True
    assert report['participant_coverage'] == 1.0


def test_missing_participant_or_504_blocks_scope():
    with pytest.raises(dq.ScopeDQError, match='participant mismatch'):
        dq.validate_scope_capture(
            _input(
                observed_team_ids=('1',),
                endpoint_status_by_team={
                    '1': 'ok', '2': 'retry_exhausted',
                },
            ),
            expected_entities=ENTITIES,
        )
    with pytest.raises(dq.ScopeDQError, match='not authoritative'):
        dq.validate_scope_capture(
            _input(endpoint_status_by_team={'1': 'ok', '2': 'retry_exhausted'}),
            expected_entities=ENTITIES,
        )


def test_domestic_league_keeps_existing_ninety_percent_floor():
    expected = tuple(str(value) for value in range(10))
    observed = expected[:9]
    endpoints = {team: ('ok' if team in observed else 'retry_exhausted')
                 for team in expected}
    report = dq.validate_scope_capture(
        _input(
            competition_type='domestic_league',
            expected_team_ids=expected,
            observed_team_ids=observed,
            endpoint_status_by_team=endpoints,
        ),
        expected_entities=ENTITIES,
    )
    assert report['strict'] is False
    assert report['endpoint_coverage'] == 0.9

    with pytest.raises(dq.ScopeDQError, match='below 90%'):
        dq.validate_scope_capture(
            _input(
                competition_type='domestic_league',
                expected_team_ids=expected,
                observed_team_ids=expected[:8],
                endpoint_status_by_team={
                    team: ('ok' if team in expected[:8] else 'retry_exhausted')
                    for team in expected
                },
            ),
            expected_entities=ENTITIES,
        )


def test_non_authoritative_listing_blocks_even_with_complete_endpoints():
    with pytest.raises(dq.ScopeDQError, match='listing is not authoritative'):
        dq.validate_scope_capture(
            _input(listing_status='schema_error'),
            expected_entities=ENTITIES,
        )


def test_national_team_contracts_are_explicitly_not_applicable():
    statuses = {entity: 'ok' for entity in ENTITIES}
    statuses['player_contract_observations'] = 'not_applicable'
    report = dq.validate_scope_capture(
        _input(
            competition_type='national_team_tournament',
            team_type='national_team',
            entity_statuses=statuses,
        ),
        expected_entities=ENTITIES,
    )
    assert report['passed'] is True


def test_club_contracts_cannot_silently_be_not_applicable():
    statuses = {entity: 'ok' for entity in ENTITIES}
    statuses['player_contract_observations'] = 'not_applicable'
    with pytest.raises(dq.ScopeDQError, match='club contract'):
        dq.validate_scope_capture(
            _input(entity_statuses=statuses), expected_entities=ENTITIES,
        )


@pytest.mark.parametrize(
    'entity',
    ['squad_memberships', 'player_attribute_observations',
     'player_contract_observations'],
)
@pytest.mark.parametrize('empty_status', ['authoritative_empty', 'not_applicable'])
def test_nonempty_club_scope_requires_core_entities(entity, empty_status):
    statuses = {name: 'ok' for name in ENTITIES}
    statuses[entity] = empty_status
    with pytest.raises(dq.ScopeDQError, match='applicability contract|club contract'):
        dq.validate_scope_capture(
            _input(entity_statuses=statuses), expected_entities=ENTITIES,
        )


@pytest.mark.parametrize(
    'entity',
    ['market_value_points', 'transfer_events', 'coach_profiles', 'coach_stints'],
)
def test_optional_career_entities_require_explicit_authoritative_empty(entity):
    statuses = {name: 'ok' for name in ENTITIES}
    statuses[entity] = 'authoritative_empty'
    report = dq.validate_scope_capture(
        _input(entity_statuses=statuses), expected_entities=ENTITIES,
    )
    assert report['passed'] is True

    statuses[entity] = 'not_applicable'
    with pytest.raises(dq.ScopeDQError, match='applicability contract'):
        dq.validate_scope_capture(
            _input(entity_statuses=statuses), expected_entities=ENTITIES,
        )


def test_entity_contract_is_explicitly_competition_and_team_bound():
    contract = dq.entity_applicability_contract(
        entity='transfer_events',
        competition_type='national_team_tournament',
        team_type='national_team',
    )
    assert contract['competition_type'] == 'national_team_tournament'
    assert contract['team_type'] == 'national_team'
    assert contract['allowed_statuses'] == ['ok', 'authoritative_empty']
    assert contract['requires_authoritative_empty_evidence'] is True


def test_current_scope_freshness_is_blocking():
    with pytest.raises(dq.ScopeDQError, match='stale'):
        dq.validate_scope_capture(
            _input(fetched_at=datetime.now(timezone.utc) - timedelta(days=9)),
            expected_entities=ENTITIES,
        )


def test_bidirectional_parity_rejects_either_delta():
    assert dq.bidirectional_set_delta([('a',)], [('a',)])['passed'] is True
    report = dq.bidirectional_set_delta([('a',)], [('a',), ('b',)])
    assert report['passed'] is False
    assert report['right_only'] == [('b',)]


def _empty_listing_input(**overrides):
    statuses = {entity: 'authoritative_empty' for entity in ENTITIES}
    values = dict(
        expected_team_ids=(),
        observed_team_ids=(),
        endpoint_status_by_team={},
        entity_statuses=statuses,
        listing_status='authoritative_empty',
    )
    values.update(overrides)
    return _input(**values)


def test_authoritative_empty_listing_completes_with_empty_entities():
    report = dq.validate_scope_capture(
        _empty_listing_input(), expected_entities=ENTITIES,
    )
    assert report['passed'] is True
    assert report['participant_count'] == 0
    assert report['participant_coverage'] == 1.0


def test_authoritative_empty_listing_rejects_participant_evidence():
    with pytest.raises(dq.ScopeDQError, match='cannot carry participant'):
        dq.validate_scope_capture(
            _empty_listing_input(expected_team_ids=('1',)),
            expected_entities=ENTITIES,
        )


def test_authoritative_empty_listing_rejects_ok_entities():
    statuses = {entity: 'authoritative_empty' for entity in ENTITIES}
    statuses['squad_memberships'] = 'ok'
    with pytest.raises(dq.ScopeDQError, match='contradicts'):
        dq.validate_scope_capture(
            _empty_listing_input(entity_statuses=statuses),
            expected_entities=ENTITIES,
        )


def test_authoritative_empty_listing_keeps_current_freshness_gate():
    with pytest.raises(dq.ScopeDQError, match='stale'):
        dq.validate_scope_capture(
            _empty_listing_input(
                fetched_at=datetime.now(timezone.utc) - timedelta(days=30),
            ),
            expected_entities=ENTITIES,
        )


def test_empty_listing_contract_flips_required_nonempty_entities():
    contract = dq.entity_applicability_contract(
        entity='squad_memberships',
        competition_type='domestic_cup',
        team_type='club',
        listing_authoritative_empty=True,
    )
    assert contract['allowed_statuses'] == ['authoritative_empty']
    assert contract['minimum_rows'] == 0
    assert contract['requires_authoritative_empty_evidence'] is True

    unchanged = dq.entity_applicability_contract(
        entity='squad_memberships',
        competition_type='domestic_cup',
        team_type='club',
    )
    assert unchanged['allowed_statuses'] == ['ok']
