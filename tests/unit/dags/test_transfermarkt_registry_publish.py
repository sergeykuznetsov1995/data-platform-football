from __future__ import annotations

from copy import deepcopy

import pytest

from dags.utils import transfermarkt_registry_publish as publish


SNAPSHOT_ID = 'tm-discovery-' + 'a' * 24
OLD_SNAPSHOT_ID = 'tm-discovery-' + 'b' * 24


def _manifest() -> tuple[dict, str]:
    scopes = [
        {
            'competition_id': 'GB1',
            'edition_id': '2025',
            'scope_id': 'tm-scope-gb1',
        },
    ]
    value = {
        'status': 'success',
        'dry_run': False,
        'cycle_id': 'registry-cycle-1',
        'snapshot_id': SNAPSHOT_ID,
        'snapshot_hash': '1' * 64,
        'page_count': 2,
        'source_body_hashes': ['2' * 64, '3' * 64],
        'rows': {
            'competitions': 2,
            'competition_editions': 3,
        },
        'hashes': {
            'competitions': '4' * 64,
            'competition_editions': '5' * 64,
            'crawl_scopes': publish.stable_hash(scopes),
        },
        'classification_counts': {
            'eligible': 1,
            'excluded': 1,
        },
        'blocked_competition_ids': [],
        'promotable': True,
        'crawl_scope_count': len(scopes),
        'crawl_scopes': scopes,
        'writes': [
            {
                'table': 'iceberg.bronze.transfermarkt_competitions',
                'rows': 2,
            },
            {
                'table': (
                    'iceberg.bronze.transfermarkt_competition_editions'
                ),
                'rows': 3,
            },
        ],
    }
    return value, publish.stable_hash(value)


def _state(*, revision: int = 5, new: bool = False) -> dict:
    return {
        'state_key': 'canonical',
        'registry_snapshot_id': SNAPSHOT_ID if new else OLD_SNAPSHOT_ID,
        'source_hash': MANIFEST_HASH if new else '9' * 64,
        'competition_count': 2 if new else 7,
        'edition_count': 3 if new else 11,
        'unknown_active_count': 0,
        'status': 'promoted',
        'revision': revision,
    }


def _dq(**overrides: int) -> dict[str, int]:
    value = {
        'competition_count': 2,
        'competition_distinct_count': 2,
        'edition_count': 3,
        'edition_distinct_count': 3,
        'orphan_editions': 0,
        'competitions_without_editions': 0,
        'current_edition_violations': 0,
        'season_format_mismatches': 0,
        'canonical_season_violations': 0,
        'classification_evidence_violations': 0,
        'classification_field_violations': 0,
        'unknown_active_count': 0,
        'content_mismatch_count': 0,
    }
    value.update(overrides)
    return value


MANIFEST, MANIFEST_HASH = _manifest()


class FakeExecutor:
    def __init__(self, *, readback: dict | None = None, dq: dict | None = None):
        self.statements: list[str] = []
        self.readback = readback if readback is not None else _state(
            revision=6, new=True,
        )
        self.dq = dq if dq is not None else _dq()

    def __call__(self, sql: str):
        self.statements.append(sql)
        if 'tm_registry_state:before' in sql:
            return [_state()]
        if 'tm_registry_state:rollback_readback' in sql:
            return [self.readback]
        if 'tm_registry_state:readback' in sql:
            return [self.readback]
        if 'tm_registry_dq:' in sql:
            return [self.dq]
        return []


def _publish(**kwargs):
    return publish.publish_registry(
        MANIFEST,
        manifest_hash=MANIFEST_HASH,
        snapshot_id=SNAPSHOT_ID,
        competition_count=2,
        edition_count=3,
        expected_revision=5,
        **kwargs,
    )


def test_dry_plan_is_side_effect_free_and_binds_existing_transforms():
    executor = FakeExecutor()
    result = _publish(apply=False, executor=executor)

    assert result.applied is False
    assert executor.statements == []
    assert result.plan.discovery_manifest_hash == MANIFEST_HASH
    assert result.plan.snapshot_hash == '1' * 64
    assert len(result.plan.registry_manifest_hash) == 64
    assert result.plan.promoted_revision == 6
    transforms = dict(result.plan.transform_sql)
    assert set(transforms) == {'competitions', 'competition_editions'}
    for sql in transforms.values():
        assert (
            "AND b.registry_snapshot_id = '" + SNAPSHOT_ID + "'"
        ) in sql
        assert 'ROW_NUMBER() OVER' in sql
        assert 'WHERE rn = 1' in sql


def test_removed_competitions_are_filtered_by_snapshot_and_history_is_retained():
    plan = _publish().plan
    all_sql = '\n'.join(plan.statements)

    # Dedup runs after the exact Bronze snapshot filter. An old competition
    # remains physically retained but cannot enter the new staged snapshot.
    for _, transform in plan.transform_sql:
        predicate = f"b.registry_snapshot_id = '{SNAPSHOT_ID}'"
        assert transform.count(predicate) == 1
        assert transform.index(predicate) < transform.index('\n)\nWHERE rn = 1')
    assert 'DELETE FROM iceberg.bronze' not in all_sql
    assert 'DELETE FROM iceberg.silver' not in all_sql
    assert 'WHEN MATCHED THEN UPDATE' not in '\n'.join(
        statement for statement in plan.statements
        if 'tm_registry_merge:' in statement
    )
    assert 'target.registry_snapshot_id = source.registry_snapshot_id' in all_sql


def test_unknown_manifest_blocks_before_any_sql_or_cas():
    manifest = deepcopy(MANIFEST)
    manifest['classification_counts'] = {'eligible': 1, 'unknown': 1}
    manifest['promotable'] = False
    manifest['blocked_competition_ids'] = ['UNK']
    manifest_hash = publish.stable_hash(manifest)
    executor = FakeExecutor()

    with pytest.raises(publish.RegistryManifestError, match='not promotable'):
        publish.publish_registry(
            manifest,
            manifest_hash=manifest_hash,
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=5,
            apply=True,
            executor=executor,
        )
    assert executor.statements == []


def test_partial_bronze_write_manifest_blocks_before_sql():
    manifest = deepcopy(MANIFEST)
    manifest['writes'] = manifest['writes'][:1]
    executor = FakeExecutor()

    with pytest.raises(publish.RegistryManifestError, match='incomplete'):
        publish.publish_registry(
            manifest,
            manifest_hash=publish.stable_hash(manifest),
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=5,
            apply=True,
            executor=executor,
        )
    assert executor.statements == []


def test_sql_unknown_active_count_blocks_before_target_and_cas():
    executor = FakeExecutor(dq=_dq(unknown_active_count=1))
    with pytest.raises(publish.RegistryDQError, match='unknown_active_count'):
        _publish(apply=True, executor=executor)

    assert any('tm_registry_dq:staging' in sql for sql in executor.statements)
    assert not any('tm_registry_merge:' in sql for sql in executor.statements)
    assert not any('tm_registry_state:cas' in sql for sql in executor.statements)


def test_natural_key_violation_blocks_before_cas():
    executor = FakeExecutor(dq=_dq(competition_distinct_count=1))
    with pytest.raises(publish.RegistryDQError, match='counts do not match'):
        _publish(apply=True, executor=executor)
    assert not any('tm_registry_state:cas' in sql for sql in executor.statements)


def test_initial_cas_revision_drift_blocks_before_transforms():
    class DriftExecutor(FakeExecutor):
        def __call__(self, sql: str):
            self.statements.append(sql)
            if 'tm_registry_state:before' in sql:
                return [_state(revision=4)]
            return []

    executor = DriftExecutor()
    with pytest.raises(publish.RegistryCasError, match='expected revision 5'):
        _publish(apply=True, executor=executor)
    assert len(executor.statements) == 1
    assert 'tm_registry_state:before' in executor.statements[0]


def test_zero_row_cas_is_detected_by_exact_readback():
    executor = FakeExecutor(readback=_state(revision=5, new=False))
    with pytest.raises(publish.RegistryCasError, match='readback mismatch'):
        _publish(apply=True, executor=executor)

    assert any('tm_registry_state:cas' in sql for sql in executor.statements)
    assert any('tm_registry_state:readback' in sql for sql in executor.statements)
    assert any('tm_registry_state:rollback' in sql for sql in executor.statements)
    # Cleanup is completed before CAS, so no non-essential mutation can fail
    # after the canonical pointer has moved.
    assert any('tm_registry_stage:cleanup' in sql for sql in executor.statements)


def test_post_cas_readback_failure_compensates_to_previous_pointer():
    class CompensatingExecutor(FakeExecutor):
        def __call__(self, sql: str):
            self.statements.append(sql)
            if 'tm_registry_state:before' in sql:
                return [_state()]
            if 'tm_registry_state:rollback_readback' in sql:
                restored = _state(revision=7, new=False)
                return [restored]
            if 'tm_registry_state:readback' in sql:
                broken = _state(revision=6, new=True)
                broken['source_hash'] = '0' * 64
                return [broken]
            if 'tm_registry_dq:' in sql:
                return [self.dq]
            return []

    executor = CompensatingExecutor()
    with pytest.raises(publish.RegistryCasError, match='readback mismatch'):
        _publish(apply=True, executor=executor)
    rollback = next(
        sql for sql in executor.statements
        if 'tm_registry_state:rollback */' in sql
    )
    assert "state_key = 'history:5'" in rollback
    assert 'target.revision = 6' in rollback
    assert 'revision = 7' in rollback


def test_exact_success_proves_state_and_emits_revision_safe_rollback():
    executor = FakeExecutor()
    result = _publish(apply=True, executor=executor)

    assert result.applied is True
    assert result.previous_state == publish.RegistryState.from_mapping(_state())
    assert result.promoted_state == publish.RegistryState.from_mapping(
        _state(revision=6, new=True),
    )
    assert dict(result.dq)['target.content_mismatch_count'] == 0
    cas = next(sql for sql in executor.statements if 'tm_registry_state:cas' in sql)
    assert "BIGINT '5'" in cas
    assert "BIGINT '6'" in cas
    assert MANIFEST_HASH in cas
    rollback = '\n'.join(result.plan.rollback_statements)
    assert "state_key = 'history:5'" in rollback
    assert 'target.revision = 6' in rollback
    assert 'revision = 7' in rollback
    assert 'DELETE' not in rollback
    assert 'iceberg.bronze' not in rollback


def test_dbapi_connection_is_supported_and_caller_connection_is_not_closed():
    backend = FakeExecutor()

    class Description:
        def __init__(self, name):
            self.name = name

    class Cursor:
        def __init__(self):
            self.rows = []
            self.description = []
            self.closed = False

        def execute(self, sql):
            mappings = backend(sql)
            if mappings:
                keys = tuple(mappings[0])
                self.description = [Description(key) for key in keys]
                self.rows = [tuple(item[key] for key in keys) for item in mappings]
            else:
                self.description = []
                self.rows = []

        def fetchall(self):
            return self.rows

        def close(self):
            self.closed = True

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()
            self.closed = False

        def cursor(self):
            return self.cursor_instance

        def close(self):
            self.closed = True

    connection = Connection()
    result = _publish(apply=True, connection=connection)
    assert result.applied is True
    assert connection.cursor_instance.closed is True
    assert connection.closed is False


def test_registry_manifest_hash_changes_with_expected_revision():
    first = _publish().plan
    second = publish.publish_registry(
        MANIFEST,
        manifest_hash=MANIFEST_HASH,
        snapshot_id=SNAPSHOT_ID,
        competition_count=2,
        edition_count=3,
        expected_revision=6,
    ).plan
    assert first.registry_manifest_hash != second.registry_manifest_hash


def test_manifest_hash_and_exact_counts_are_mandatory():
    with pytest.raises(publish.RegistryManifestError, match='hash mismatch'):
        publish.publish_registry(
            MANIFEST,
            manifest_hash='0' * 64,
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=5,
        )
    with pytest.raises(publish.RegistryManifestError, match='row counts'):
        publish.publish_registry(
            MANIFEST,
            manifest_hash=MANIFEST_HASH,
            snapshot_id=SNAPSHOT_ID,
            competition_count=3,
            edition_count=3,
            expected_revision=5,
        )
