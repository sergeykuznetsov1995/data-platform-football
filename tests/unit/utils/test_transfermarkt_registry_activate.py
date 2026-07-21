from types import SimpleNamespace

import pytest

from dags.utils import transfermarkt_registry_activate as activate


def _manifest():
    value = {
        'snapshot_id': 'tm-discovery-' + 'a' * 24,
        'status': 'success',
        'promotable': True,
        'blocked_competition_ids': [],
        'rows': {
            'competitions': 10,
            'competition_editions': 20,
            'competition_participants': 40,
        },
        'raw_response_set': {
            'raw_payload_set_id': 'b' * 64,
            'response_count': 12,
        },
    }
    return value, activate.stable_hash(value)


def test_plan_activates_bronze_directly_without_silver_or_slots():
    manifest, digest = _manifest()
    plan = activate.build_activation_plan(
        manifest, manifest_hash=digest, expected_revision=3,
    )

    assert plan.state.revision == 4
    sql = '\n'.join(plan.statements)
    assert 'iceberg.bronze.transfermarkt_competitions' in sql
    assert 'iceberg.bronze.transfermarkt_competition_participants' in sql
    assert 'transfermarkt_raw_responses' in sql
    assert 'iceberg.silver' not in sql
    assert 'candidate_slot' not in sql


def test_plan_rejects_unbound_or_non_raw_capture():
    manifest, digest = _manifest()
    manifest['blocked_competition_ids'] = ['UNKNOWN']
    with pytest.raises(activate.RegistryActivationError, match='block activation'):
        activate.build_activation_plan(
            manifest,
            manifest_hash=activate.stable_hash(manifest),
            expected_revision=0,
        )

    manifest, _ = _manifest()
    manifest.pop('raw_response_set')
    with pytest.raises(activate.RegistryActivationError, match='raw response-set'):
        activate.build_activation_plan(
            manifest,
            manifest_hash=activate.stable_hash(manifest),
            expected_revision=0,
        )


class _Cursor:
    def __init__(self, plan):
        self.plan = plan
        self.description = ()
        self._rows = []
        self.closed = False

    def execute(self, sql):
        if sql == self.plan.statements[1]:
            values = {
                'competition_count': 10,
                'competition_distinct_count': 10,
                'edition_count': 20,
                'edition_distinct_count': 20,
                'participant_count': 40,
                'participant_distinct_count': 40,
                'blocked_competition_count': 0,
                'orphan_edition_count': 0,
                'orphan_participant_count': 0,
                'participant_count_mismatch_count': 0,
                'raw_lineage_violation_count': 0,
            }
            self.description = tuple((name,) for name in values)
            self._rows = [tuple(values.values())]
        elif sql == self.plan.statements[3]:
            state = self.plan.state
            values = {
                'registry_snapshot_id': state.snapshot_id,
                'source_hash': state.source_hash,
                'competition_count': state.competition_count,
                'edition_count': state.edition_count,
                'participant_count': state.participant_count,
                'revision': state.revision,
                'status': state.status,
            }
            self.description = tuple((name,) for name in values)
            self._rows = [tuple(values.values())]
        else:
            self.description = ()
            self._rows = []

    def fetchall(self):
        return self._rows

    def close(self):
        self.closed = True


def test_apply_verifies_dq_and_cas_then_commits():
    manifest, digest = _manifest()
    plan = activate.build_activation_plan(
        manifest, manifest_hash=digest, expected_revision=0,
    )
    cursor = _Cursor(plan)
    connection = SimpleNamespace(
        cursor=lambda: cursor,
        commit=lambda: setattr(connection, 'committed', True),
        rollback=lambda: setattr(connection, 'rolled_back', True),
        committed=False,
        rolled_back=False,
    )

    actual = activate.apply_activation(plan, connection)

    assert actual == plan.state
    assert connection.committed is True
    assert connection.rolled_back is False
    assert cursor.closed is True
