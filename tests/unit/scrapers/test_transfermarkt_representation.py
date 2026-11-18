from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from scrapers.transfermarkt.offline_qualification import CaptureExport
from scrapers.transfermarkt.raw_store import RawResponseStore
from scrapers.transfermarkt.representation import (
    RepresentationParseError,
    RepresentationUniverse,
    UniversePlayer,
    build_representation_dq,
    extract_representation_observations,
    parse_representation_packet,
)
from scrapers.transfermarkt.tmapi import players_url

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures' / 'transfermarkt' / 'representation'
CAPTURED = datetime(2026, 11, 8, 4, 57, 13, 167286, tzinfo=timezone.utc)
EXTRACTED = datetime(2026, 11, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)
MISSING = object()


def packet(value=123, *, player_id='1', clubs=('10',)):
    return {
        'success': True,
        'data': [{
            'id': player_id,
            'attributes': {} if value is MISSING else {'consultantAgencyId': value},
            'clubAssignments': [
                {'clubId': club, 'type': 'current', 'shirtNumber': None, 'isCaptain': False}
                for club in clubs
            ],
        }],
    }


def universe(players=('1',), captures=()):
    items = tuple(item if isinstance(item, UniversePlayer) else UniversePlayer(str(item))
                  for item in players)
    return RepresentationUniverse('registry-test', items, tuple(captures))


def capture(store, value=123, *, ids=('1',), body=None, at=CAPTURED, scope='scope-1'):
    raw_body = body if body is not None else json.dumps(packet(value)).encode()
    return store.store_attempt(
        url=players_url(ids), body=raw_body, status_code=200,
        headers={'Content-Type': 'application/json'}, fetched_at=at.isoformat(),
        cycle_id='raw-cycle', scope_id=scope, endpoint='current_players', attempt=0,
    )


def extract(store, raw, *, players=('1',), at=EXTRACTED):
    return extract_representation_observations(
        store, universe(players, (raw.capture_id,)), extracted_at=at,
    )


@pytest.mark.parametrize('value,expected', [
    (1, '1'), ('1', '1'), ('0001', '1'),
    (123456789012345678901234567890, '123456789012345678901234567890'),
])
def test_positive_agency_ids_are_assigned_and_canonical(value, expected):
    result = parse_representation_packet(packet(value), expected_ids=['1'])
    assertion = result['1']
    assert assertion.observation_status == 'assigned'
    assert assertion.status_reason == 'positive_id'
    assert assertion.representative_id == expected


@pytest.mark.parametrize('value,reason', [
    (0, 'zero'), ('0', 'zero'), ('0000', 'zero'),
    (None, 'null'), (MISSING, 'missing_field'),
])
def test_zero_null_and_missing_are_unknown_never_explicit_none(value, reason):
    assertion = parse_representation_packet(packet(value), expected_ids=['1'])['1']
    assert assertion.observation_status == 'unknown'
    assert assertion.status_reason == reason
    assert assertion.representative_id is None


@pytest.mark.parametrize('value', [
    True, False, 1.0, 0.0, -1, '+1', ' 1', '1 ', '١', '', 'abc', [], {},
])
def test_invalid_agency_values_are_errors_not_unknown_or_none(value):
    assertion = parse_representation_packet(packet(value), expected_ids=['1'])['1']
    assert assertion.observation_status == 'error'
    assert assertion.status_reason.startswith('invalid_agency_')
    assert assertion.representative_id is None


@pytest.mark.parametrize('payload,ids', [
    ({'success': False, 'data': []}, ['1']),
    ({'success': True}, ['1']),
    ({'success': True, 'data': {}}, ['1']),
    (packet(1, player_id='2'), ['1']),
    ({'success': True, 'data': [packet(1)['data'][0], packet(2)['data'][0]]}, ['1']),
    (packet(1), []),
    (packet(1), [str(index) for index in range(1, 102)]),
])
def test_packet_schema_and_request_identity_fail_closed(payload, ids):
    with pytest.raises((RepresentationParseError, ValueError)):
        parse_representation_packet(payload, expected_ids=ids)


def test_extract_preserves_raw_lineage_and_separates_extraction_clock(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    raw = capture(store, 123)
    result = extract(store, raw)
    row = result.observations[0]
    assert row.natural_key == ('1', 'consultant_agency', 'agency:123', raw.capture_id)
    assert row.group_key == ('1', 'consultant_agency', raw.capture_id)
    assert row.source_capture_id == row.raw_capture_id == raw.capture_id
    assert row.source_url == raw.url and row.source_body_hash == raw.content_hash
    assert row.captured_at == row.observed_at == raw.fetched_at
    assert row.extracted_at == EXTRACTED.isoformat()
    assert row.scope_id == 'scope-1' and row.cycle_id == 'raw-cycle'
    assert row.source_endpoint == 'current_players'
    assert row.assessment_manifest_id == result.universe.assessment_manifest_id
    assert row.run_id == result.run_id
    assert result.rows[0]['_batch_id'] == result.batch_id
    assert result.rows[0]['_ingested_at'] == result.extracted_at
    assert result.rows[0]['_source'] == 'transfermarkt'
    assert result.rows[0]['_entity_type'] == 'player_representation'


def test_identical_replay_is_deterministic_and_later_replay_does_not_refresh_source(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    raw = capture(store, 123)
    first = extract(store, raw)
    assert extract(store, raw).canonical_sha256() == first.canonical_sha256()
    later = extract(store, raw, at=EXTRACTED + timedelta(hours=2))
    assert [row.natural_key for row in later.observations] == [row.natural_key for row in first.observations]
    assert later.observations[0].captured_at == first.observations[0].captured_at
    assert later.observations[0].observed_at == first.observations[0].observed_at
    assert later.observations[0].extracted_at != first.observations[0].extracted_at
    assert later.run_id != first.run_id and later.batch_id != first.batch_id
    assert later.canonical_sha256() != first.canonical_sha256()


def test_multi_club_player_is_one_global_observation(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    raw = capture(store, 123)
    player = UniversePlayer('1', club_ids=('10', '20', '30'), scope_ids=('scope-a', 'scope-b'))
    result = extract_representation_observations(
        store, universe((player,), (raw.capture_id,)), extracted_at=EXTRACTED,
    )
    assert len(result.observations) == 1
    assert result.observations[0].player_id == '1'
    assert result.universe.players[0].club_ids == ('10', '20', '30')


def test_missing_packet_has_assessment_bound_identity_and_no_fake_raw_lineage(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    raw = capture(store, 123, ids=('1',))
    result = extract(store, raw, players=('1', '2'))
    assert Counter(row.observation_status for row in result.observations) == {
        'assigned': 1, 'missing_packet': 1,
    }
    missing = next(row for row in result.observations if row.player_id == '2')
    assert missing.source_capture_id.startswith('missing:')
    expected_suffix = hashlib.sha256(
        f'{result.universe.assessment_manifest_id}|2|consultant_agency'.encode()
    ).hexdigest()
    assert missing.source_capture_id == 'missing:' + expected_suffix
    assert missing.raw_capture_id is None and missing.source_body_hash is None
    assert missing.source_url is None and missing.captured_at is None and missing.observed_at is None
    assert missing.status_reason == 'missing_packet'
    assert build_representation_dq(result)['conclusive_coverage'] == 0.5


def test_unavailable_selected_capture_fails_assessment_without_fabricating_rows(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    missing_id = 'a' * 64
    result = extract_representation_observations(
        store, universe(('1',), (missing_id,)), extracted_at=EXTRACTED,
    )
    assert result.observations == ()
    assert result.capture_reports[0]['status'] == 'error'
    assert result.capture_reports[0]['capture_id'] == missing_id
    report = build_representation_dq(result)
    assert not report['passed']
    assert report['packet_coverage'] == 0.0


def test_malformed_known_capture_is_capture_associated_error_not_missing(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    raw = capture(store, body=b'{not-json')
    result = extract(store, raw)
    row = result.observations[0]
    assert row.observation_status == 'error'
    assert row.raw_capture_id == raw.capture_id
    assert row.source_body_hash == raw.content_hash
    assert row.captured_at == raw.fetched_at
    assert row.observation_status != 'missing_packet'
    assert not build_representation_dq(result)['passed']


def test_requested_player_omitted_from_existing_packet_is_reconciliation_error(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    raw = capture(store, ids=('1', '2'))
    result = extract(store, raw, players=('1', '2'))
    assert len(result.observations) == 2
    assert {row.observation_status for row in result.observations} == {'error'}
    assert all(row.raw_capture_id == raw.capture_id for row in result.observations)
    assert all(row.observation_status != 'missing_packet' for row in result.observations)
    assert not build_representation_dq(result)['passed']


def test_multiple_independent_representatives_do_not_inflate_player_coverage(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    result = extract(store, capture(store, 123))
    first = result.observations[0]
    second = replace(
        first, representative_key='agency:456', representative_id='456',
        evidence_type='independent_test_proof', evidence_locator='saved-proof-2',
    )
    multi = replace(result, observations=(first, second))
    report = build_representation_dq(multi)
    assert report['passed']
    assert report['expected_players'] == 1
    assert report['observation_rows'] == 2
    assert report['observed_players'] == 1
    assert report['conclusive_coverage'] == 1.0


def test_contradictory_group_and_duplicate_payload_fail_dq(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    result = extract(store, capture(store, 123))
    first = result.observations[0]
    contradictory = replace(
        first, representative_key='status:unknown', representative_id=None,
        observation_status='unknown', status_reason='zero',
    )
    assert not build_representation_dq(replace(result, observations=(first, contradictory)))['passed']
    conflicting = replace(first, status_reason='different_reason')
    assert not build_representation_dq(replace(result, observations=(first, conflicting)))['passed']


def test_explicit_none_requires_separate_typed_profile_proof(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    base = extract(store, capture(store, 123)).observations[0]
    with pytest.raises(ValueError, match='explicit_none'):
        replace(
            base, observation_status='explicit_none', representative_key='status:explicit_none',
            representative_id=None, status_reason='source_explicit_none',
        )
    profile = store.store_attempt(
        url='https://www.transfermarkt.com/player-one/profil/spieler/1',
        body=b'typed contract fixture; not parsed as source evidence', status_code=200,
        headers={'Content-Type': 'text/html'}, fetched_at=CAPTURED.isoformat(),
        cycle_id='profile-cycle', scope_id='profile-scope', endpoint='player_profile', attempt=0,
    )
    proven = replace(
        base, observation_status='explicit_none', representative_key='status:explicit_none',
        representative_id=None, status_reason='source_explicit_none',
        evidence_type='player_profile_explicit_none', evidence_locator='typed-synthetic-proof',
        source_capture_id=profile.capture_id, raw_capture_id=profile.capture_id,
        source_url=profile.url, source_body_hash=profile.content_hash,
        source_endpoint='player_profile', scope_id='profile-scope', cycle_id='profile-cycle',
        captured_at=profile.fetched_at, observed_at=profile.fetched_at,
    )
    assert proven.observation_status == 'explicit_none'
    assert proven.raw_capture_id == profile.capture_id


def test_empty_universe_is_not_automatic_success(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    result = extract_representation_observations(store, universe(()), extracted_at=EXTRACTED)
    report = build_representation_dq(result)
    assert report['expected_players'] == 0
    assert report['packet_coverage'] is None
    assert report['conclusive_coverage'] is None
    assert not report['passed']


def test_real_cache_fixtures_cover_55_clubs_and_replay_exactly():
    selection = json.loads((FIXTURES / 'selection.json').read_text())
    source = CaptureExport(FIXTURES / 'captures.json')
    roster = RepresentationUniverse.from_dict(json.loads((FIXTURES / 'universe.json').read_text()))
    first = extract_representation_observations(source, roster, extracted_at=EXTRACTED)
    second = extract_representation_observations(source, roster, extracted_at=EXTRACTED)
    assert selection['capture_count'] == len(source.records) == 11
    assert selection['player_count'] == len(roster.players) == 1032
    assert selection['club_count'] == 55
    assert sum(len(player.club_ids) > 1 for player in roster.players) == 25
    assert len(first.observations) == len(first.rows) == 1032
    assert Counter(row.observation_status for row in first.observations) == {
        'assigned': 947, 'unknown': 85,
    }
    assert len({row.natural_key for row in first.observations}) == 1032
    assert len({row.group_key for row in first.observations}) == 1032
    assert all(row.observation_status != 'explicit_none' for row in first.observations)
    assert first.canonical_sha256() == second.canonical_sha256()
    assert all(report['status'] == 'reconciled' for report in first.capture_reports)
    for row in first.observations:
        body, raw = source.load_capture(row.raw_capture_id)
        assert hashlib.sha256(body).hexdigest() == row.source_body_hash
        assert row.source_capture_id == raw.capture_id
        assert row.captured_at == row.observed_at == raw.fetched_at
        assert row.extracted_at == EXTRACTED.isoformat()
        assert str(row.player_id) in parse_qs(urlsplit(raw.url).query)['ids[]']
    report = build_representation_dq(first)
    assert report['passed'] and report['structural_passed']
    assert report['expected_players'] == report['expected_groups'] == 1032
    assert report['observed_players'] == report['observed_groups'] == 1032
    assert report['packet_coverage'] == 1.0
    assert report['verified_write_coverage'] == 0.0
    assert report['conclusive_coverage'] == pytest.approx(947 / 1032)
    assert report['fresh_conclusive_coverage'] is None
    assert report['freshness'] == 'not_evaluated'
    assert report['unresolved_players'] == 85
    later = extract_representation_observations(
        source, roster, extracted_at=EXTRACTED + timedelta(days=1),
    )
    assert [row.natural_key for row in later.observations] == [row.natural_key for row in first.observations]
    assert {row.captured_at for row in later.observations} == {row.captured_at for row in first.observations}


def test_universe_input_order_does_not_change_assessment_identity():
    first = {
        'universe_version': 'registry-order-test',
        'players': [
            {'player_id': '2', 'club_ids': ['20', '10'], 'scope_ids': ['scope-b'], 'competition_ids': []},
            {'player_id': '1', 'club_ids': ['30'], 'scope_ids': ['scope-a'], 'competition_ids': ['GB1']},
        ],
        'capture_ids': ['b' * 64, 'a' * 64],
    }
    second = {
        'universe_version': 'registry-order-test',
        'players': list(reversed(first['players'])),
        'capture_ids': list(reversed(first['capture_ids'])),
    }
    assert (RepresentationUniverse.from_dict(first).assessment_manifest_id
            == RepresentationUniverse.from_dict(second).assessment_manifest_id)


def test_same_body_in_different_captures_remains_two_observations(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    body = json.dumps(packet(123)).encode()
    first_raw = capture(store, body=body, at=CAPTURED)
    second_raw = capture(store, body=body, at=CAPTURED + timedelta(hours=1))
    result = extract_representation_observations(
        store, universe(('1',), (second_raw.capture_id, first_raw.capture_id)),
        extracted_at=EXTRACTED,
    )
    assert len(result.observations) == 2
    assert len({row.source_capture_id for row in result.observations}) == 2
    assert len({row.source_body_hash for row in result.observations}) == 1
    assert len({row.natural_key for row in result.observations}) == 2


def test_dq_freshness_uses_source_capture_time_not_extraction_time(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    result = extract(store, capture(store, 123))
    stale = build_representation_dq(result, now=EXTRACTED)
    fresh = build_representation_dq(result, now=CAPTURED + timedelta(hours=1))
    assert stale['fresh_conclusive_coverage'] == 0.0
    assert fresh['fresh_conclusive_coverage'] == 1.0
    assert stale['conclusive_coverage'] == fresh['conclusive_coverage'] == 1.0


def test_invalid_agency_is_unresolved_but_structurally_complete(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    result = extract(store, capture(store, 1.0))
    report = build_representation_dq(result)
    assert report['passed'] and report['structural_passed']
    assert report['status_counts'] == {'error': 1}
    assert report['unresolved_players'] == 1
    assert report['conclusive_coverage'] == 0.0
