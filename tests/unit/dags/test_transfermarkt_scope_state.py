from __future__ import annotations

import dataclasses
import json

import pytest

from dags.utils import transfermarkt_scope_state as state


EXPECTED = {
    'squad_memberships',
    'player_attribute_observations',
    'player_contract_observations',
    'market_value_points',
    'transfer_events',
    'coach_profiles',
    'coach_stints',
}


def _entity(name: str, *, provider_bytes: int = 10) -> state.EntityEvidence:
    return state.EntityEvidence(
        entity=name,
        applicability_status='ok',
        expected_rows=1,
        raw_rows=1,
        dedup_rows=1,
        key_hash='a' * 64,
        content_hash='b' * 64,
        dq_status='passed',
        decoded_bytes=5,
        wire_bytes=8,
        provider_metered_bytes=provider_bytes,
        requests=1,
        retries=0,
        cache_hits=0,
        duration_ms=20,
    )


def _manifest(
    scope: str,
    *,
    parent_cycle_id: str = 'parent',
    reader_revision: int = 7,
) -> state.ScopeManifest:
    from dags.utils import transfermarkt_dq_contracts as dq

    competition, edition = scope.split(':', 1)
    entities = tuple(_entity(name) for name in sorted(EXPECTED))
    capture = {
        'schema_version': 1,
        'scope_id': scope,
        'competition_id': competition,
        'edition_id': edition,
        'competition_type': 'domestic_league',
        'gender': 'men',
        'team_type': 'club',
        'age_category': 'senior',
        'listing_status': 'ok',
        'listing_source_url': 'https://example.test/listing',
        'listing_source_body_hash': 'listing-hash',
        'expected_team_ids': ['1', '2'],
        'observed_team_ids': ['1', '2'],
        'endpoint_status_by_team': {'1': 'ok', '2': 'ok'},
        'fetched_at': '2026-07-11T00:00:00+00:00',
    }
    return state.ScopeManifest(
        parent_cycle_id=parent_cycle_id,
        child_cycle_id=f'{parent_cycle_id}/{scope}',
        scope_id=scope,
        competition_id=competition,
        edition_id=edition,
        canonical_competition_id=f'canonical-{competition}',
        canonical_season=edition,
        registry_snapshot_id='registry-1',
        capture_revision='capture-1',
        parser_revision='parser-1',
        schema_revision='schema-1',
        reader_revision=reader_revision,
        entities=entities,
        dq_evidence={
            'status': 'passed',
            'registry_participant_count': 2,
            'edition_current': True,
            'scope_capture': capture,
            'entity_statuses': {
                item.entity: item.applicability_status for item in entities
            },
            'entity_contracts': dq.entity_applicability_contracts(
                entities=EXPECTED,
                competition_type='domestic_league',
                team_type='club',
            ),
            'authoritative_empty_evidence': {},
            'roster_coverage': {},
            'career_fetches_pending': 0,
            'participant_contract': {
                'passed': True,
                'competition_type': 'domestic_league',
                'strict': False,
                'participant_count': 2,
                'observed_participant_count': 2,
                'participant_coverage': 1.0,
                'endpoint_coverage': 1.0,
                'minimum_participant_coverage': 0.9,
                'fresh': True,
            },
        },
    )


def test_scope_set_is_order_independent_and_accepts_older_child_revision():
    one = _manifest(
        'GB1:2025', parent_cycle_id='batch-1', reader_revision=5,
    )
    two = _manifest(
        'CL:2025', parent_cycle_id='batch-2', reader_revision=7,
    )
    left = state.ScopeSetManifest.build(
        [one, two], expected_entities=EXPECTED, reader_revision=9,
    )
    right = state.ScopeSetManifest.build(
        [two, one], expected_entities=EXPECTED, reader_revision=9,
    )
    assert left == right
    assert left.reader_revision == 9
    assert len(left.scope_set_id) == 64

    with pytest.raises(state.ScopeManifestError, match='exceeds scope-set'):
        state.ScopeSetManifest.build(
            [one, two], expected_entities=EXPECTED, reader_revision=6,
        )


def test_scope_set_supports_512_exact_scopes_and_rejects_513():
    manifests = [_manifest(f'C{index}:2025') for index in range(512)]
    scope_set = state.ScopeSetManifest.build(
        manifests, expected_entities=EXPECTED,
    )
    assert len(scope_set.scope_digests) == state.MAX_SCOPE_SET_SIZE == 512

    with pytest.raises(state.ScopeManifestError, match='cannot exceed 512'):
        state.ScopeSetManifest.build(
            [*manifests, _manifest('OVER:2025')],
            expected_entities=EXPECTED,
        )


def test_scope_set_still_requires_one_registry_capture_parser_schema_contract():
    one = _manifest('GB1:2025')
    drifted = state.ScopeManifest(**{
        **_manifest('CL:2025').__dict__,
        'parser_revision': 'parser-drift',
    })
    with pytest.raises(state.ScopeManifestError, match='registry/capture/parser'):
        state.ScopeSetManifest.build(
            [one, drifted], expected_entities=EXPECTED,
        )


def test_scope_manifest_json_round_trip_is_strict():
    original = _manifest('FIWC:2026')
    payload = json.loads(json.dumps(original.as_dict()))
    restored = state.ScopeManifest.from_mapping(payload)
    restored.validate(EXPECTED)
    assert restored == original
    assert restored.digest == original.digest

    del payload['entities'][0]['provider_metered_bytes']
    with pytest.raises(state.ScopeManifestError, match='missing fields'):
        state.ScopeManifest.from_mapping(payload)

    without_dq = original.as_dict()
    del without_dq['dq_evidence']
    with pytest.raises(state.ScopeManifestError, match='missing fields'):
        state.ScopeManifest.from_mapping(without_dq)


def test_incomplete_scope_cannot_enter_scope_set():
    manifest = _manifest('GB1:2025')
    incomplete = state.ScopeManifest(
        **{**manifest.__dict__, 'entities': manifest.entities[:-1]}
    )
    with pytest.raises(state.ScopeManifestError, match='entity set mismatch'):
        state.ScopeSetManifest.build([incomplete], expected_entities=EXPECTED)


def test_failed_or_unclassified_empty_entity_blocks_scope():
    manifest = _manifest('GB1:2025')
    broken = state.EntityEvidence(
        **{**manifest.entities[0].__dict__, 'applicability_status': 'blocked'}
    )
    invalid = state.ScopeManifest(
        **{**manifest.__dict__, 'entities': (broken,) + manifest.entities[1:]}
    )
    with pytest.raises(state.ScopeManifestError, match='invalid terminal status'):
        invalid.validate(EXPECTED)


def test_scope_digest_binds_strict_participant_dq_evidence():
    from dags.utils import transfermarkt_dq_contracts as dq

    manifest = _manifest('CL:2025')
    strict_capture = {
        **manifest.dq_evidence['scope_capture'],
        'competition_type': 'continental_club',
    }
    strict = state.ScopeManifest(**{
        **manifest.__dict__,
        'dq_evidence': {
            **manifest.dq_evidence,
            'scope_capture': strict_capture,
            'entity_contracts': dq.entity_applicability_contracts(
                entities=EXPECTED,
                competition_type='continental_club',
                team_type='club',
            ),
            'roster_coverage': {},
            'career_fetches_pending': 0,
            'participant_contract': {
                **manifest.dq_evidence['participant_contract'],
                'competition_type': 'continental_club',
                'strict': True,
                'minimum_participant_coverage': 1.0,
            },
        },
    })
    strict.validate(EXPECTED)
    original_digest = strict.digest
    incomplete_capture = {
        **strict_capture,
        'observed_team_ids': ['1'],
        'endpoint_status_by_team': {'1': 'ok', '2': 'retry_exhausted'},
    }
    incomplete = state.ScopeManifest(**{
        **strict.__dict__,
        'dq_evidence': {
            **strict.dq_evidence,
            'scope_capture': incomplete_capture,
        },
    })
    assert incomplete.digest != original_digest
    with pytest.raises(state.ScopeManifestError, match='strict participant'):
        incomplete.validate(EXPECTED)


def _with_empty_entity(manifest, entity_name, *, proof=True):
    empty_hash = state.stable_hash([])
    entities = tuple(
        state.EntityEvidence(**{
            **item.__dict__,
            'applicability_status': 'authoritative_empty',
            'expected_rows': 0,
            'raw_rows': 0,
            'dedup_rows': 0,
            'key_hash': empty_hash,
            'content_hash': empty_hash,
        }) if item.entity == entity_name else item
        for item in manifest.entities
    )
    statuses = {
        item.entity: item.applicability_status for item in entities
    }
    empty_evidence = ({
        entity_name: {
            'kind': 'typed_fetch_state',
            'result_sha256': 'c' * 64,
        },
    } if proof else {})
    return state.ScopeManifest(**{
        **manifest.__dict__,
        'entities': entities,
        'dq_evidence': {
            **manifest.dq_evidence,
            'entity_statuses': statuses,
            'authoritative_empty_evidence': empty_evidence,
        },
    })


def test_core_applicable_entity_cannot_be_empty_in_nonempty_scope():
    manifest = _with_empty_entity(
        _manifest('GB1:2025'), 'squad_memberships',
    )
    with pytest.raises(state.ScopeManifestError, match='applicability contract'):
        manifest.validate(EXPECTED)


def test_optional_empty_requires_hash_bound_authoritative_proof():
    original = _manifest('GB1:2025')
    proven = _with_empty_entity(original, 'market_value_points')
    proven.validate(EXPECTED)
    assert proven.digest != original.digest

    unproven = _with_empty_entity(
        original, 'market_value_points', proof=False,
    )
    with pytest.raises(state.ScopeManifestError, match='evidence set'):
        unproven.validate(EXPECTED)


def test_traffic_is_totalled_across_scope_and_entity():
    report = state.aggregate_traffic([
        _manifest('GB1:2025'), _manifest('CL:2025'),
    ])
    assert report['provider_metered_bytes'] == len(EXPECTED) * 2 * 10
    assert report['requests'] == len(EXPECTED) * 2


def test_scope_control_ddl_is_additive_and_has_global_ledger():
    sql = '\n'.join(state.ddl_statements())
    assert 'CREATE TABLE IF NOT EXISTS' in sql
    assert state.SCOPE_MANIFEST_TABLE in sql
    assert state.SCOPE_SET_MANIFEST_TABLE in sql
    assert state.PROXY_LEDGER_TABLE in sql
    assert 'provider_metered_bytes bigint' in sql


def test_a_scope_cannot_understate_the_careers_it_still_owes():
    # A career fact is bought a roster window at a time, so a scope can be
    # 'complete' while holding a hundred of a league's several thousand
    # players. The count rides inside the manifest hash precisely so that it
    # cannot be edited afterwards to say the slot is fuller than it is.
    manifest = _manifest('GB1:2025')
    lying = dataclasses.replace(
        manifest,
        dq_evidence={
            **manifest.dq_evidence,
            'roster_coverage': {
                'market_value_history': {
                    'roster_size': 2859, 'selected': 100, 'pending': 2759,
                },
            },
            'career_fetches_pending': 0,
        },
    )

    with pytest.raises(state.ScopeManifestError, match='career_fetches_pending'):
        lying.validate(EXPECTED)

    honest = dataclasses.replace(
        manifest,
        dq_evidence={
            **manifest.dq_evidence,
            'roster_coverage': {
                'market_value_history': {
                    'roster_size': 2859, 'selected': 100, 'pending': 2759,
                },
            },
            'career_fetches_pending': 2759,
        },
    )
    honest.validate(EXPECTED)
    assert honest.digest != manifest.digest
