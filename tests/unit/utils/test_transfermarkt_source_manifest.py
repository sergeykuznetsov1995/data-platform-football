import pytest

from dags.utils import transfermarkt_source as source
from dags.utils import transfermarkt_source_manifest as manifest


def _entity(entity, *, status='ok', pending=0, capture='a'):
    return manifest.EntityEvidence(
        entity=entity,
        status=status,
        row_count=0 if status in {'authoritative_empty', 'not_applicable'} else 1,
        natural_key_hash='b' * 64,
        content_hash='c' * 64,
        raw_capture_ids=(capture * 64,),
        pending_count=pending,
    )


def _scope(*, partial_entity=None):
    entities = tuple(
        _entity(
            entity,
            status='partial' if entity == partial_entity else 'ok',
            pending=1 if entity == partial_entity else 0,
            capture=hex(index + 1)[2:],
        )
        for index, entity in enumerate(source.SOURCE_ENTITIES)
    )
    payload_set, captures = manifest.raw_payload_set_id(
        capture for item in entities for capture in item.raw_capture_ids
    )
    return manifest.ScopeManifest(
        parent_cycle_id='parent',
        child_cycle_id='child',
        scope_id='scope',
        competition_id='GB1',
        edition_id='2025',
        registry_snapshot_id='tm-discovery-' + 'd' * 24,
        parser_revision='raw-v1',
        schema_revision='3',
        entities=entities,
        raw_payload_set_id=payload_set,
        raw_response_count=len(captures),
        player_capture_count=1,
        player_capture_hash='e' * 64,
    )


def test_scope_is_complete_only_when_every_entity_is_terminal():
    complete = _scope()
    partial = _scope(partial_entity='market_value_points')

    assert complete.as_dict()['status'] == 'complete'
    assert partial.as_dict()['status'] == 'partial'
    assert partial.as_dict()['pending_count'] == 1


def test_scope_manifest_binds_raw_and_player_evidence():
    scope = _scope()
    assert len(scope.digest) == 64

    broken = manifest.ScopeManifest(**{
        **{field: getattr(scope, field) for field in scope.__dataclass_fields__},
        'raw_response_count': 999,
    })
    with pytest.raises(manifest.SourceManifestError, match='response count'):
        broken.validate()


def test_scope_player_capture_is_deterministic_and_conflicts_fail():
    row = manifest.ScopePlayerCapture(
        cycle_id='cycle', scope_id='scope', competition_id='GB1',
        edition_id='2025', club_id='1', player_id='2',
        raw_capture_id='a' * 64,
    )
    count, digest, rows = manifest.scope_player_evidence([row, row])
    assert count == 1
    assert len(digest) == 64
    assert rows == (row,)

    conflict = manifest.ScopePlayerCapture(
        cycle_id='cycle', scope_id='scope', competition_id='GB1',
        edition_id='2025', club_id='1', player_id='2',
        raw_capture_id='b' * 64,
    )
    with pytest.raises(manifest.SourceManifestError, match='conflicts'):
        manifest.scope_player_evidence([row, conflict])


def test_entity_cannot_claim_terminal_with_pending_debt():
    with pytest.raises(manifest.SourceManifestError, match='terminal status has debt'):
        _entity('transfer_events', pending=1).validate()
