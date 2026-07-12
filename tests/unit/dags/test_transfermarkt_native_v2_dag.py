"""Static DAG wiring tests for the gated Transfermarkt native-v2 shadow."""
from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest


def _reload():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop('dag_transform_transfermarkt_silver', None)
    sys.modules.pop('dags.dag_transform_transfermarkt_silver', None)
    return importlib.import_module('dag_transform_transfermarkt_silver')


def _task(task_id):
    from airflow.operators.python import PythonOperator

    return next((t for t in PythonOperator._instances if t.task_id == task_id), None)


def _entity(name, *, provider_bytes=10):
    from utils.transfermarkt_scope_state import EntityEvidence

    return EntityEvidence(
        entity=name,
        applicability_status='ok',
        expected_rows=1,
        raw_rows=1,
        dedup_rows=1,
        key_hash='a' * 64,
        content_hash='b' * 64,
        dq_status='passed',
        decoded_bytes=provider_bytes,
        wire_bytes=provider_bytes,
        provider_metered_bytes=provider_bytes,
        requests=1,
        retries=0,
        cache_hits=0,
        duration_ms=10,
    )


def _manifest(mod, suffix='1', **changes):
    from utils.transfermarkt_scope_state import ScopeManifest
    from utils import transfermarkt_dq_contracts as dq

    values = {
        'parent_cycle_id': 'parent-cycle',
        'child_cycle_id': f'child-{suffix}',
        'scope_id': f'scope-{suffix}',
        'competition_id': f'GB{suffix}',
        'edition_id': '2025',
        'canonical_competition_id': f'ENG-League-{suffix}',
        'canonical_season': '2526',
        'registry_snapshot_id': 'registry-1',
        'capture_revision': 'capture-v1',
        'parser_revision': 'parser-v2',
        'schema_revision': '2',
        'reader_revision': 7,
        'entities': tuple(_entity(name) for name in mod.tm_v2.NATIVE_ENTITIES),
    }
    values.update(changes)
    if 'dq_evidence' not in values:
        capture = {
            'schema_version': 1,
            'scope_id': values['scope_id'],
            'competition_id': values['competition_id'],
            'edition_id': values['edition_id'],
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
        values['dq_evidence'] = {
            'status': 'passed',
            'registry_participant_count': 2,
            'edition_current': True,
            'scope_capture': capture,
            'entity_statuses': {
                item.entity: item.applicability_status
                for item in values['entities']
            },
            'entity_contracts': dq.entity_applicability_contracts(
                entities=(item.entity for item in values['entities']),
                competition_type='domestic_league',
                team_type='club',
            ),
            'authoritative_empty_evidence': {},
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
        }
    return ScopeManifest(**values)


def _scope_set(mod, *manifests, reader_revision=None):
    from utils.transfermarkt_scope_state import ScopeSetManifest

    return ScopeSetManifest.build(
        manifests,
        expected_entities=mod.tm_v2.NATIVE_ENTITIES,
        reader_revision=reader_revision,
    )


def _scope_conf(scope_set, *, slot='b'):
    return {
        'transfermarkt_parent_cycle_id': 'parent-cycle',
        'transfermarkt_scope_set_id': scope_set.scope_set_id,
        'transfermarkt_scope_set': scope_set.as_dict(),
        'transfermarkt_reader_revision': scope_set.reader_revision,
        'transfermarkt_candidate_slot': slot,
    }


def _manifest_row(manifest, *, status='complete'):
    return (
        manifest.parent_cycle_id,
        manifest.child_cycle_id,
        manifest.scope_id,
        manifest.competition_id,
        manifest.edition_id,
        manifest.canonical_competition_id,
        manifest.canonical_season,
        manifest.registry_snapshot_id,
        manifest.capture_revision,
        manifest.parser_revision,
        manifest.schema_revision,
        manifest.reader_revision,
        json.dumps({
            'entities': [item.as_dict() for item in manifest.entities],
            'dq_evidence': manifest.dq_evidence,
        }),
        manifest.digest,
        status,
    )


def test_global_shadow_transforms_are_explicitly_unpartitioned():
    mod = _reload()
    configs = {table: parts for _, _, table, parts in mod.NATIVE_V2_TRANSFORMS}
    for table in (
        'transfermarkt_player_xref_global_v2',
        'transfermarkt_player_attributes_v2',
        'transfermarkt_market_value_points_v2',
        'transfermarkt_transfer_events_v2',
        'transfermarkt_coach_profiles_v2',
        'transfermarkt_coach_stints_v2',
    ):
        assert configs[table] == [], f'{table} accidentally season-partitioned'
    assert configs['transfermarkt_squad_memberships_v2'] == [
        'competition_id', 'edition_id',
    ]
    assert configs['transfermarkt_player_team_season_assignment_v2'] == [
        'competition_id', 'edition_id',
    ]


def test_native_gate_defaults_off_and_is_environment_switchable(monkeypatch):
    mod = _reload()
    monkeypatch.delenv('TM_NATIVE_V2_ENABLED', raising=False)
    assert mod._native_v2_enabled() is False
    monkeypatch.setenv('TM_NATIVE_V2_ENABLED', 'true')
    assert mod._native_v2_enabled() is True


def test_readiness_uses_shared_dag_utility_without_path_import_kludge():
    source = Path(__file__).resolve().parents[3] / 'dags' / 'dag_transform_transfermarkt_silver.py'
    text = source.read_text(encoding='utf-8')
    assert 'from utils import transfermarkt_native_v2 as tm_v2' in text
    assert 'spec_from_file_location' not in text


def test_shadow_gold_is_built_and_ready_marker_is_a_leaf():
    mod = _reload()
    gold = {table: parts for _, _, table, parts in mod.NATIVE_V2_GOLD_TRANSFORMS}
    assert set(gold) == {
        'dim_manager_v2', 'fct_transfer_v2',
        'fct_player_market_value_v2',
        'transfermarkt_team_season_market_value_v2',
    }
    marker = _task('tm_model_ready_v2')
    assert marker is not None
    assert 'persist_scope_set_manifest' in marker.upstream_task_ids
    assert 'validate_silver_quality' in marker.upstream_task_ids
    assert 'record_native_v2_model_manifest' in marker.upstream_task_ids
    manifest = _task('record_native_v2_model_manifest')
    assert 'validate_native_v2_gold' in manifest.upstream_task_ids
    capture = _task('capture_reader_state')
    pins = _task('pin_transform_input_snapshots')
    approval = _task('authorize_silver_writes')
    gold_approval = _task('authorize_gold_writes')
    persisted = _task('persist_scope_set_manifest')
    assert capture.upstream_task_ids == {'validate_transform_scope_set'}
    assert pins.upstream_task_ids == {'capture_reader_state'}
    assert approval.upstream_task_ids == {'pin_transform_input_snapshots'}
    assert persisted.upstream_task_ids == {'authorize_silver_writes'}
    assert gold_approval.upstream_task_ids == {'validate_native_v2_quality'}
    for task_id, _, _, _ in mod.NATIVE_V2_GOLD_TRANSFORMS:
        task = _task(f'native_v2_gold.{task_id}')
        assert 'authorize_gold_writes' in task.upstream_task_ids
    source = (
        Path(__file__).resolve().parents[3]
        / 'dags' / 'dag_transform_transfermarkt_silver.py'
    ).read_text(encoding='utf-8')
    assert "trigger_rule='none_failed_min_one_success'" in source


def test_native_scope_never_infers_a_cycle(monkeypatch):
    mod = _reload()

    from airflow.exceptions import AirflowException

    class DagRun:
        conf = {'transfermarkt_league': 'ENG-Premier League',
                'transfermarkt_season': 2025}

    with pytest.raises(AirflowException, match='parent_cycle_id'):
        mod._validate_transform_scope_set(dag_run=DagRun())


def test_shadow_gold_gate_rejects_empty_and_null_primary_keys():
    mod = _reload()
    assert mod.NATIVE_V2_GOLD_MIN_ROWS == {
        'fct_transfer_v2': 500,
        'fct_player_market_value_v2': 1000,
        'dim_manager_v2': 20,
        'transfermarkt_team_season_market_value_v2': 80,
    }
    source = Path(mod.__file__).read_text(encoding='utf-8')
    assert "def g(name: str)" in source
    for table in (
        'fct_transfer_v2', 'fct_player_market_value_v2', 'dim_manager_v2',
        'transfermarkt_team_season_market_value_v2',
    ):
        assert f"g('{table}')" in source
    assert "where=\"source = 'transfermarkt'\"" in source
    assert 'tm_native_v2_market_value_transfermarkt_branch' in source


def test_legacy_producers_target_versioned_physical_tables():
    mod = _reload()
    targets = {task: table for task, _, table in mod.SILVER_TRANSFORMS}
    assert targets['players'] == 'transfermarkt_players_legacy'
    assert targets['coaches'] == 'transfermarkt_coaches_legacy'


def test_post_cleanup_skips_legacy_silver_validation_before_querying_tables():
    mod = _reload()
    from airflow.exceptions import AirflowSkipException

    class TI:
        @staticmethod
        def xcom_pull(task_ids=None):
            assert task_ids == 'capture_reader_state'
            return {'legacy_writers_disabled_at': '2026-08-15T00:00:00Z'}

    with pytest.raises(AirflowSkipException, match='legacy Silver validation'):
        mod._validate_silver(ti=TI())
    with pytest.raises(AirflowSkipException, match='legacy Silver DQ'):
        mod._validate_silver_quality(ti=TI())


def test_scope_manifests_are_loaded_by_exact_digest_across_parents():
    mod = _reload()
    first = _manifest(
        mod, '1', parent_cycle_id='batch-1', reader_revision=5,
    )
    second = _manifest(
        mod, '2', parent_cycle_id='batch-2', reader_revision=7,
    )
    scope_set = _scope_set(mod, first, second, reader_revision=9)

    class Cursor:
        sql = []

        def execute(self, sql):
            self.sql.append(sql)

        def fetchall(self):
            return [_manifest_row(first), _manifest_row(second)]

    cursor = Cursor()
    loaded = mod._load_scope_manifests(
        cursor,
        scope_set=scope_set,
    )
    assert loaded == (first, second)
    assert 'WHERE manifest_digest IN' in cursor.sql[0]
    assert 'WHERE parent_cycle_id' not in cursor.sql[0]
    assert first.digest in cursor.sql[0]
    assert second.digest in cursor.sql[0]
    assert 'latest' not in cursor.sql[0].lower()


def test_legacy_entity_only_ops_json_cannot_reach_transform():
    mod = _reload()
    manifest = _manifest(mod)
    old_shape = json.dumps([
        item.as_dict() for item in manifest.entities
    ])
    with pytest.raises(ValueError, match='entities and dq_evidence'):
        mod._entity_json(old_shape)


def test_scope_manifest_duplicate_exact_digest_row_fails_closed():
    mod = _reload()
    manifest = _manifest(mod)
    scope_set = _scope_set(mod, manifest)

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [_manifest_row(manifest), _manifest_row(manifest)]

    with pytest.raises(Exception, match='row count differs'):
        mod._load_scope_manifests(
            Cursor(), scope_set=scope_set,
        )


def test_active_v2_explicit_child_cycle_targets_inactive_slot(monkeypatch):
    mod = _reload()
    state = mod.tm_v2.ReaderState(
        exists=True, active_version='v2', approved_cycle_id='old-cycle',
        approved_league='ENG-Premier League', approved_season=2025,
        approved_model_revision=0, active_slot='a', revision=1,
    )

    class Cursor:
        def close(self):
            pass

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(mod.tm_v2, 'connect', lambda: Conn())
    monkeypatch.setattr(mod.tm_v2, 'read_reader_state', lambda *a, **k: state)

    manifest = _manifest(mod, reader_revision=1)
    scope_set = _scope_set(mod, manifest)

    class DagRun:
        conf = {
            **_scope_conf(scope_set),
            'transfermarkt_parent_cycle_id': 'new-cycle',
        }

    captured = mod._capture_reader_state(dag_run=DagRun())
    assert captured['active_slot'] == 'a'
    assert captured['candidate_slot'] == 'b'
    assert captured['revision'] == 1
    assert captured['scope_set_id'] == scope_set.scope_set_id


def test_scope_set_conf_is_rehashed_and_rejects_content_drift():
    mod = _reload()
    manifest = _manifest(mod)
    scope_set = _scope_set(mod, manifest)
    conf = _scope_conf(scope_set)
    assert mod._parse_scope_set_conf(conf) == scope_set

    drifted = scope_set.as_dict()
    drifted['parser_revision'] = 'parser-drift'
    conf['transfermarkt_scope_set'] = drifted
    with pytest.raises(ValueError, match='does not match immutable'):
        mod._parse_scope_set_conf(conf)


def test_transform_preflight_returns_exact_scopes_and_traffic(monkeypatch):
    mod = _reload()
    first = _manifest(mod, '1')
    second = _manifest(mod, '2')
    scope_set = _scope_set(mod, first, second)

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            if f'FROM {mod.tm_planner.REGISTRY_STATE_TABLE}' in self.sql:
                return [('registry-1', 'promoted', 0)]
            if f'FROM {mod.tm_planner.COMPETITIONS_TABLE}' in self.sql:
                return [
                    (first.competition_id, first.edition_id),
                    (second.competition_id, second.edition_id),
                ]
            return [_manifest_row(first), _manifest_row(second)]

        def close(self):
            pass

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(mod.tm_v2, 'connect', lambda: Conn())

    class DagRun:
        conf = _scope_conf(scope_set)

    result = mod._validate_transform_scope_set(dag_run=DagRun())
    assert result['scope_set_id'] == scope_set.scope_set_id
    assert result['parent_cycle_id'] == 'parent-cycle'
    assert [item['scope_id'] for item in result['scopes']] == [
        first.scope_id, second.scope_id,
    ]
    assert result['traffic']['provider_metered_bytes'] == 140
    assert result['registry_coverage'] == {
        'registry_snapshot_id': 'registry-1',
        'target_scope_count': 2,
        'complete': True,
    }


def test_transform_prewrite_rejects_partial_promoted_registry_target():
    mod = _reload()
    manifest = _manifest(mod, '1')
    scope_set = _scope_set(mod, manifest)

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            if f'FROM {mod.tm_planner.REGISTRY_STATE_TABLE}' in self.sql:
                return [('registry-1', 'promoted', 0)]
            return [
                (manifest.competition_id, manifest.edition_id),
                ('CL', '2025'),
            ]

    with pytest.raises(Exception, match='complete promoted registry target'):
        mod._assert_complete_promoted_registry_target(
            Cursor(), scope_set=scope_set, manifests=(manifest,),
        )


def test_transform_treats_cross_parent_aggregate_traffic_as_metric_only():
    mod = _reload()
    per_entity = 9 * 1024 * 1024 // 7
    first = _manifest(
        mod,
        '1',
        parent_cycle_id='batch-1',
        entities=tuple(
            _entity(name, provider_bytes=per_entity)
            for name in mod.tm_v2.NATIVE_ENTITIES
        ),
    )
    second = _manifest(
        mod,
        '2',
        parent_cycle_id='batch-2',
        entities=tuple(
            _entity(name, provider_bytes=per_entity)
            for name in mod.tm_v2.NATIVE_ENTITIES
        ),
    )
    scope_set = _scope_set(mod, first, second)

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [_manifest_row(first), _manifest_row(second)]

    loaded = mod._load_scope_manifests(Cursor(), scope_set=scope_set)
    traffic = mod.tm_scope.aggregate_traffic(loaded)
    assert traffic['provider_metered_bytes'] > 15 * 1024 * 1024


def test_sql_is_scope_filtered_time_travel_pinned_and_lineage_bound():
    mod = _reload()
    scopes = [
        {
            'competition_id': 'GB1',
            'edition_id': '2025',
            'canonical_competition_id': 'ENG-Premier League',
            'canonical_season': '2526',
        },
        {
            'competition_id': 'CL',
            'edition_id': '2025',
            'canonical_competition_id': 'UEFA-Champions League',
            'canonical_season': '2526',
        },
    ]
    sql = (
        'SELECT * FROM iceberg.bronze.transfermarkt_squad_memberships '
        'JOIN iceberg.silver.xref_player ON true'
    )
    scoped = mod._scope_native_sql(
        sql, scopes, registry_snapshot_id='registry-1',
    )
    pinned = mod._pin_sql_relations(scoped, {
        'iceberg.bronze.transfermarkt_squad_memberships': 101,
        'iceberg.silver.xref_player': 202,
    })
    assert 'competition_id = \'GB1\'' in pinned
    assert 'edition_id = \'2025\'' in pinned
    assert 'competition_id = \'CL\'' in pinned
    assert (
        'iceberg.bronze.transfermarkt_squad_memberships '
        'FOR VERSION AS OF 101'
    ) in pinned
    assert 'iceberg.silver.xref_player FOR VERSION AS OF 202' in pinned

    lineage = mod._inject_model_lineage(
        pinned,
        input_snapshot_ids={'iceberg.silver.xref_player': 202},
        build_id='tm-shadow__run-1',
        scope_set_id='a' * 64,
    )
    for column in ('input_snapshot_ids', 'build_id', 'scope_set_id'):
        assert lineage.count(f'AS {column}') == 1
    with pytest.raises(RuntimeError, match='refusing duplicate'):
        mod._inject_model_lineage(
            lineage,
            input_snapshot_ids={'iceberg.silver.xref_player': 202},
            build_id='tm-shadow__run-1',
            scope_set_id='a' * 64,
        )


def test_every_native_output_is_built_once_with_candidate_slot_and_lineage_dq():
    mod = _reload()
    silver_tasks = [
        _task(f'native_v2_transforms.{task_id}')
        for task_id, _, _, _ in mod.NATIVE_V2_TRANSFORMS
    ]
    gold_tasks = [
        _task(f'native_v2_gold.{task_id}')
        for task_id, _, _, _ in mod.NATIVE_V2_GOLD_TRANSFORMS
    ]
    assert len(silver_tasks) == 12
    assert len(gold_tasks) == 4
    assert all(task is not None for task in silver_tasks + gold_tasks)
    assert all(task.op_kwargs['use_candidate_slot'] for task in silver_tasks)
    assert all(task.op_kwargs['use_candidate_slot'] for task in gold_tasks)
    source = Path(mod.__file__).read_text(encoding='utf-8')
    assert "cols=['input_snapshot_ids', 'build_id', 'scope_set_id']" in source


def test_model_manifest_receives_scope_set_and_one_pinned_snapshot_map(monkeypatch):
    mod = _reload()
    manifest = _manifest(mod)
    scope_set = _scope_set(mod, manifest)
    scope_value = {
        'mode': 'scope_set',
        'parent_cycle_id': 'parent-cycle',
        'scope_set_id': scope_set.scope_set_id,
        'reader_revision': 7,
        'scopes': [],
    }
    snapshots = {'iceberg.bronze.transfermarkt_squad_memberships': 101}
    pin_payload = {
        'scope_set_id': scope_set.scope_set_id,
        'parent_cycle_id': 'parent-cycle',
        'input_snapshot_ids': snapshots,
    }
    pins = {
        **pin_payload,
        'snapshot_set_id': mod.tm_scope.stable_hash(pin_payload),
    }
    reader = {'revision': 7, 'candidate_slot': 'b'}

    class TI:
        @staticmethod
        def xcom_pull(task_ids=None):
            return {
                'validate_transform_scope_set': scope_value,
                'pin_transform_input_snapshots': pins,
                'capture_reader_state': reader,
            }[task_ids]

    class Cursor:
        def close(self):
            pass

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    captured = {}

    def record(cur, **kwargs):
        captured.update(kwargs)
        return {'scope_set_id': kwargs['scope_set_id'], 'rows': []}

    monkeypatch.setattr(mod.tm_v2, 'connect', lambda: Conn())
    monkeypatch.setattr(mod.tm_v2, 'record_model_build_manifest', record)

    class DagRun:
        run_id = 'tm-shadow__run-1'

    result = mod._record_native_v2_model_manifest(
        ti=TI(), dag_run=DagRun(),
    )
    assert result['scope_set_id'] == scope_set.scope_set_id
    assert captured['cycle_id'] == 'parent-cycle'
    assert captured['scope_set_id'] == scope_set.scope_set_id
    assert captured['pinned_input_snapshot_ids'] == snapshots


def test_ready_marker_returns_exact_scope_set_for_cutover(monkeypatch):
    mod = _reload()
    scope = {
        'mode': 'scope_set',
        'parent_cycle_id': 'parent-cycle',
        'scope_set_id': 'a' * 64,
        'reader_revision': 7,
    }
    reader = {'revision': 7, 'candidate_slot': 'b'}

    class TI:
        @staticmethod
        def xcom_pull(task_ids=None):
            return {
                'validate_transform_scope_set': scope,
                'capture_reader_state': reader,
            }[task_ids]

    class Cursor:
        def close(self):
            pass

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    captured = {}

    def readiness(cur, cycle_id, **kwargs):
        captured.update({'cycle_id': cycle_id, **kwargs})
        return {'ready': True, 'scope_set_id': 'a' * 64}

    monkeypatch.setenv('TM_NATIVE_V2_ENABLED', 'true')
    monkeypatch.setattr(mod.tm_v2, 'connect', lambda: Conn())
    monkeypatch.setattr(
        mod.tm_v2, 'assert_reader_revision', lambda cur, revision: None,
    )
    monkeypatch.setattr(mod.tm_v2, 'readiness', readiness)
    result = mod._tm_model_ready_v2(ti=TI())
    assert result['status'] == 'scope_set_ready'
    assert result['scope_set_id'] == 'a' * 64
    assert captured == {
        'cycle_id': 'parent-cycle',
        'expected_revision': 7,
        'scope_set_id': 'a' * 64,
        'parent_cycle_id': 'parent-cycle',
        'candidate_slot_override': 'b',
    }


def test_write_approval_is_exact_and_consumed_once(tmp_path, monkeypatch):
    mod = _reload()
    from utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket

    manifest = _manifest(mod)
    scope_set = _scope_set(mod, manifest)
    scope_value = {
        'mode': 'scope_set',
        'parent_cycle_id': 'parent-cycle',
        'scope_set_id': scope_set.scope_set_id,
        'reader_revision': 7,
        'candidate_slot': 'b',
        'scope_digests': [list(item) for item in scope_set.scope_digests],
        'traffic': {'provider_metered_bytes': 70},
        'scopes': [],
    }
    reader = {
        'exists': True,
        'revision': 7,
        'candidate_slot': 'b',
        'active_slot': 'a',
        'legacy_writers_disabled_at': None,
    }
    pin_payload = {
        'scope_set_id': scope_set.scope_set_id,
        'parent_cycle_id': 'parent-cycle',
        'input_snapshot_ids': {'iceberg.bronze.transfermarkt_players': 101},
    }
    pins = {
        **pin_payload,
        'snapshot_set_id': mod.tm_scope.stable_hash(pin_payload),
    }
    expected_argv = mod._transform_write_argv(
        phase='silver',
        parent_cycle_id='parent-cycle',
        scope_set_id=scope_set.scope_set_id,
        reader_revision=7,
        candidate_slot='b',
        build_id='tm-shadow__run-1',
    )
    journal_path = (tmp_path / 'approval.json').resolve()
    packet = ApprovalPacket(
        packet_id='tm-transform-write-1',
        action='production_write',
        argv=expected_argv,
        byte_cap_bytes=0,
        byte_cap_mib=Decimal(0),
        request_limit=0,
        retry_limit=0,
        concurrency=1,
        expected_duration_seconds=1800,
        affected_tables=mod._transform_write_tables(
            phase='silver', candidate_slot='b', include_legacy=True,
        ),
        affected_files=(str(journal_path),),
        stop_conditions=('scope/revision/snapshot/DQ drift',),
        backup_commands=(('trino', '--execute', 'SELECT 1'),),
        rollback_commands=(('trino', '--execute', 'SELECT 1'),),
    )
    journal = ApprovalJournal(journal_path)
    journal.issue(
        packet,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )
    journal.approve(packet, presented_hash=packet.packet_hash)

    class TI:
        @staticmethod
        def xcom_pull(task_ids=None):
            return {
                'validate_transform_scope_set': scope_value,
                'pin_transform_input_snapshots': pins,
                'capture_reader_state': reader,
            }[task_ids]

    class DagRun:
        run_id = 'tm-shadow__run-1'
        conf = {
            'transfermarkt_silver_write_approval_packet_id': packet.packet_id,
            'transfermarkt_silver_write_approval_packet_hash': packet.packet_hash,
            'transfermarkt_approval_journal': str(journal_path),
        }

    result = mod._authorize_silver_writes(ti=TI(), dag_run=DagRun())
    assert result['status'] == 'consumed'
    assert result['scope_set_id'] == scope_set.scope_set_id
    assert journal.get(packet.packet_hash).status == 'consumed'
    assert '--snapshot-set-id' not in packet.argv
    with pytest.raises(Exception, match='not consumable'):
        mod._authorize_silver_writes(ti=TI(), dag_run=DagRun())


def test_gold_has_a_separate_one_shot_packet_and_disjoint_assets(tmp_path):
    mod = _reload()
    scope = {
        'mode': 'scope_set',
        'parent_cycle_id': 'parent-cycle',
        'scope_set_id': 'a' * 64,
        'reader_revision': 7,
    }
    reader = {
        'exists': True,
        'revision': 7,
        'candidate_slot': 'b',
        'active_slot': 'a',
        'legacy_writers_disabled_at': None,
    }
    pin_payload = {
        'scope_set_id': 'a' * 64,
        'parent_cycle_id': 'parent-cycle',
        'input_snapshot_ids': {'iceberg.bronze.transfermarkt_players': 101},
    }
    pins = {
        **pin_payload,
        'snapshot_set_id': mod.tm_scope.stable_hash(pin_payload),
    }

    class TI:
        @staticmethod
        def xcom_pull(task_ids=None):
            return {
                'validate_transform_scope_set': scope,
                'capture_reader_state': reader,
                'pin_transform_input_snapshots': pins,
            }[task_ids]

    class DagRun:
        run_id = 'tm-shadow__run-1'
        conf = {
            'transfermarkt_silver_write_approval_packet_id': 'silver-only',
            'transfermarkt_silver_write_approval_packet_hash': 'b' * 64,
            'transfermarkt_approval_journal': str(tmp_path / 'journal.json'),
        }

    with pytest.raises(Exception, match='fresh gold write approval'):
        mod._authorize_gold_writes(ti=TI(), dag_run=DagRun())

    silver = set(mod._transform_write_tables(
        phase='silver', candidate_slot='b', include_legacy=True,
    ))
    gold = set(mod._transform_write_tables(
        phase='gold', candidate_slot='b', include_legacy=False,
    ))
    assert len(silver) == 17  # 12 native + scope-set ops + 4 retained legacy
    assert len(gold) == 5  # 4 native Gold + model-manifest ops
    assert silver.isdisjoint(gold)


def test_write_gate_requires_runtime_pins_even_though_packet_argv_does_not():
    mod = _reload()
    scope = {
        'mode': 'scope_set',
        'parent_cycle_id': 'parent-cycle',
        'scope_set_id': 'a' * 64,
        'reader_revision': 7,
    }

    class TI:
        @staticmethod
        def xcom_pull(task_ids=None):
            return {
                'validate_transform_scope_set': scope,
                'capture_reader_state': {
                    'exists': True,
                    'revision': 7,
                    'candidate_slot': 'b',
                },
                'pin_transform_input_snapshots': None,
            }[task_ids]

    class DagRun:
        run_id = 'tm-shadow__run-1'
        conf = {}

    with pytest.raises(RuntimeError, match='pin_transform_input_snapshots'):
        mod._authorize_silver_writes(ti=TI(), dag_run=DagRun())
    argv = mod._transform_write_argv(
        phase='silver',
        parent_cycle_id='parent-cycle',
        scope_set_id='a' * 64,
        reader_revision=7,
        candidate_slot='b',
        build_id='tm-shadow__run-1',
    )
    assert '--snapshot-set-id' not in argv


def test_scope_set_row_is_persisted_only_after_consumed_approval(monkeypatch):
    mod = _reload()
    manifest = _manifest(mod)
    scope_set = _scope_set(mod, manifest)
    traffic = {
        'decoded_bytes': 70,
        'wire_bytes': 70,
        'provider_metered_bytes': 70,
        'requests': 7,
        'retries': 0,
        'cache_hits': 0,
        'duration_ms': 70,
    }
    scope_value = {
        'mode': 'scope_set',
        'scope_set_id': scope_set.scope_set_id,
        'registry_snapshot_id': scope_set.registry_snapshot_id,
        'capture_revision': scope_set.capture_revision,
        'parser_revision': scope_set.parser_revision,
        'schema_revision': scope_set.schema_revision,
        'reader_revision': 7,
        'scope_digests': [list(item) for item in scope_set.scope_digests],
        'traffic': traffic,
    }
    reader = {'revision': 7}

    class TI:
        @staticmethod
        def xcom_pull(task_ids=None):
            return {
                'validate_transform_scope_set': scope_value,
                'capture_reader_state': reader,
                'authorize_silver_writes': {'status': 'consumed'},
            }[task_ids]

    class Cursor:
        statements = []

        def execute(self, sql):
            self.statements.append(sql)

        def fetchall(self):
            return []

        def close(self):
            pass

    cursor = Cursor()

    class Conn:
        def cursor(self):
            return cursor

        def close(self):
            pass

    monkeypatch.setattr(mod.tm_v2, 'connect', lambda: Conn())
    monkeypatch.setattr(
        mod.tm_v2, 'assert_reader_revision', lambda cur, revision: None,
    )
    result = mod._persist_scope_set_manifest(ti=TI())
    assert result['status'] == 'persisted'
    insert = next(sql for sql in cursor.statements if 'INSERT INTO' in sql)
    assert mod.tm_scope.SCOPE_SET_MANIFEST_TABLE in insert
    assert scope_set.scope_set_id in insert
    assert "'success'" in insert
    assert 'provider_metered_bytes' in insert


def test_scope_set_persist_refuses_missing_approval_without_sql(monkeypatch):
    mod = _reload()
    scope = {'mode': 'scope_set', 'scope_set_id': 'a' * 64}

    class TI:
        @staticmethod
        def xcom_pull(task_ids=None):
            return {
                'validate_transform_scope_set': scope,
                'capture_reader_state': {'revision': 7},
                'authorize_silver_writes': None,
            }[task_ids]

    monkeypatch.setattr(
        mod.tm_v2,
        'connect',
        lambda: pytest.fail('SQL opened before approval'),
    )
    with pytest.raises(RuntimeError, match='consumed write approval'):
        mod._persist_scope_set_manifest(ti=TI())


def test_gold_legacy_producers_target_versioned_physical_tables():
    root = Path(__file__).resolve().parents[3]
    source = (root / 'dags' / 'dag_transform_fbref_gold.py').read_text(
        encoding='utf-8',
    )
    assert "'dim_manager_legacy'" in source
    assert "'fct_player_market_value_legacy'" in source
    assert "'fct_transfer_legacy_source'" in source
    assert "task_id='transfermarkt_reader_precondition'" in source
    assert "task_id='transfermarkt_reader_postcondition'" in source
    assert "task_id='transfermarkt_legacy_physical_dq'" in source
    assert '_rollback_dq_report' in source


def test_native_dependency_order_is_materialized():
    _reload()
    attrs = _task('native_v2_transforms.player_attributes_v2')
    assignment = _task('native_v2_transforms.player_team_season_assignment_v2')
    assert {
        'native_v2_transforms.player_attribute_observations_v2',
        'native_v2_transforms.player_contract_observations_v2',
        'native_v2_transforms.competition_editions_v2',
        'native_v2_transforms.player_xref_global_v2',
        'native_v2_transforms.market_value_points_v2',
    }.issubset(attrs.upstream_task_ids)
    assert {
        'native_v2_transforms.squad_memberships_v2',
        'native_v2_transforms.transfer_events_v2',
        'native_v2_transforms.competition_editions_v2',
    }.issubset(assignment.upstream_task_ids)


def test_master_has_no_environment_cutover_authority():
    root = Path(__file__).resolve().parents[3]
    source = (root / 'dags' / 'dag_master_pipeline.py').read_text(encoding='utf-8')
    assert 'TM_NATIVE_V2_CUTOVER_REQUIRED' not in source
    assert source.count('python_callable=_transfermarkt_gold_gate') == 1
    # #933: every Gold prerequisite edge is fail-closed all_success; the TM
    # gate blocks Gold by raising instead of relying on a soft trigger rule.
    assert "trigger_rule='none_failed_min_one_success'" not in source
    assert 'active TM v2 scope-set readiness' in source
    assert "trigger_dag_id='dag_transform_transfermarkt_silver'" not in source
