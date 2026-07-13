"""Production contract tests for the registry-driven Transfermarkt DAG."""

from __future__ import annotations

import importlib
import inspect
import json
from pathlib import Path
import re
from types import SimpleNamespace
import sys
from unittest.mock import MagicMock

import pytest


def _reload_dag_module():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop('dag_ingest_transfermarkt', None)
    sys.modules.pop('dags.dag_ingest_transfermarkt', None)
    return importlib.import_module('dag_ingest_transfermarkt')


@pytest.fixture
def dag_module():
    return _reload_dag_module()


def _bash_task(task_id: str):
    from airflow.operators.bash import BashOperator

    return next(
        (item for item in BashOperator._instances if item.task_id == task_id),
        None,
    )


def _params(module, **overrides):
    values = {
        'scopes': [],
        'leagues': [],
        'season': None,
        'registry_snapshot_id': '',
        'max_batch': 8,
        'approval_journal': module.APPROVAL_JOURNAL,
        'approval_bundles': {},
        'mv_transfers_limit': 100,
        'refresh_mode': 'auto',
        'coach_history_ttl_days': 28,
        'checkpoint_ttl_days': 35,
        'proxy_lease_ttl_seconds': 3600,
        'proxy_request_limit': 710,
        'proxy_retry_limit': 2,
        'entity_timeout_seconds': 3600,
    }
    values.update(overrides)
    return values


def _payload(tmp_path: Path) -> dict:
    base = tmp_path / 'cycles' / 'parent' / 'scopes' / 'GB1__2025' / 'hash'
    return {
        'parent_cycle_id': 'scheduled__2026-07-13',
        'child_cycle_id': 'tm-child-abc',
        'scope_id': 'GB1__2025',
        'competition_id': 'GB1',
        'edition_id': '2025',
        'canonical_competition_id': 'TM-GB1',
        'canonical_season': '2526',
        'competition_type': 'domestic_league',
        'season_format': 'split_year',
        'registry_snapshot_id': 'registry-1',
        'edition_record': {'current': True},
        'result_paths': {
            'base_dir': str(base),
            'capture_result': str(base / 'capture-result.json'),
            'scope_manifest': str(base / 'scope-manifest.json'),
            'entity_staging_dir': str(base / 'entities'),
        },
        'parent_ledger': {
            'parent_cycle_id': 'scheduled__2026-07-13',
            'ledger_id': 'tm-ledger-abc',
            'path': str(tmp_path / 'proxy-ledger.json'),
        },
    }


class TestDagShape:
    def test_only_one_bounded_mapped_paid_task(self, dag_module):
        task = _bash_task('run_exact_child_cycle')
        assert task is not None
        assert task.is_mapped is True
        assert task._expand_kwargs['env'].operator.task_id == 'plan_exact_scopes'
        assert task._init_kwargs['pool'] == 'transfermarkt_proxy'
        assert task._init_kwargs['pool_slots'] == 1
        assert task._init_kwargs['max_active_tis_per_dag'] == 1
        assert task._init_kwargs['retries'] == 0
        assert task._init_kwargs['do_xcom_push'] is False

    def test_exact_wrapper_receives_both_one_shot_approvals(self, dag_module):
        command = _bash_task('run_exact_child_cycle').bash_command
        assert 'run_transfermarkt_scope_cycle.py' in command
        assert '--paid-proxy-approval-packet-id' in command
        assert '--paid-proxy-approval-packet-hash' in command
        assert '--production-write-approval-packet-id' in command
        assert '--production-write-approval-packet-hash' in command
        assert '--career-window-limit "$TM_MV_TRANSFERS_LIMIT"' in command
        assert '--cycle-budget-bytes "$TM_PROVIDER_HARD_CAP_BYTES"' in command
        assert '--soft-byte-stop-bytes "$TM_PROVIDER_SOFT_STOP_BYTES"' in command
        assert '--checkpoint-ttl-days "$TM_CHECKPOINT_TTL_DAYS"' in command
        assert '--entity-timeout-seconds "$TM_ENTITY_TIMEOUT_SECONDS"' in command
        assert '--mv-transfers-limit' not in command
        assert '--hard-byte-cap' not in command
        assert '--soft-byte-cap' not in command
        assert '--as-of-date' not in command
        assert '--concurrency' not in command

    def test_every_rendered_flag_exists_in_final_scope_wrapper(self, dag_module):
        from dags.scripts import run_transfermarkt_scope_cycle as wrapper

        command = _bash_task('run_exact_child_cycle').bash_command
        rendered_flags = set(re.findall(r'(?m)^\s*(--[a-z0-9-]+)\b', command))
        parser_flags = set(wrapper._parser()._option_string_actions)
        assert rendered_flags <= parser_flags

    def test_no_static_scope_or_global_tmp_paths_remain(self, dag_module):
        source = inspect.getsource(dag_module)
        assert 'TRANSFERMARKT_LEAGUES' not in source
        assert 'TRANSFERMARKT_LEAGUES[0]' not in source
        assert '/tmp/transfermarkt_' not in source
        assert 'scrape_players_task' not in source
        assert 'competition x edition' in source

    def test_default_selectors_mean_all_due_registry_scopes(self, dag_module):
        params = dag_module.dag._dag_kwargs['params']
        assert params['scopes'].default == []
        assert params['leagues'].default == []
        assert params['season'].default is None
        assert params['max_batch'].default == 8
        assert params['max_batch']._kw['maximum'] == 8

    def test_bounded_operator_params_are_not_relaxable(self, dag_module):
        params = dag_module.dag._dag_kwargs['params']
        assert params['mv_transfers_limit']._kw['maximum'] == 100
        assert params['proxy_retry_limit']._kw['maximum'] == 400
        assert params['proxy_request_limit']._kw['maximum'] == 710
        assert params['proxy_lease_ttl_seconds']._kw['maximum'] == 3600
        assert dag_module.PROVIDER_HARD_CAP_BYTES == 15 * 1024 * 1024
        assert dag_module.PROVIDER_SOFT_STOP_BYTES == 14 * 1024 * 1024
        assert dag_module.PROXY_CONCURRENCY == 1

    def test_ingest_stops_at_scope_set_before_fresh_transform_approval(
        self, dag_module,
    ):
        source = inspect.getsource(dag_module)
        assert "trigger_dag_id='dag_transform_transfermarkt_silver'" not in source
        assert 'Silver/Gold are a second Airflow phase' in source
        params = dag_module.dag._dag_kwargs['params']
        assert 'silver_write_approval_packet_id' not in params
        assert 'gold_write_approval_packet_id' not in params


class TestPlanningGate:
    def _context(self, module, params):
        ti = MagicMock()
        ti.xcom_pull.return_value = {
            'paid_io_allowed': True,
            'revision': 7,
            'candidate_slot': 'b',
            'write_mode': 'dual',
        }
        return {
            'ti': ti,
            'dag': SimpleNamespace(dag_id='dag_ingest_transfermarkt'),
            'run_id': 'scheduled__2026-07-13',
            'ds': '2026-07-13',
            'params': params,
        }

    def test_params_scopes_and_leagues_reach_central_planner(
        self, dag_module, monkeypatch, tmp_path,
    ):
        from utils import transfermarkt_scope_planner as planner

        payload = _payload(tmp_path)
        captured = {}

        def fake_plan(params, **kwargs):
            captured['params'] = params
            captured.update(kwargs)
            return SimpleNamespace(mapped_payloads=(payload,))

        monkeypatch.setattr(planner, 'plan_transfermarkt_scopes', fake_plan)
        monkeypatch.setattr(dag_module, '_read_promoted_registry', lambda **kw: [{'x': 1}])
        monkeypatch.setenv('TM_PROXY_CONTROL_URL', 'http://proxy-filter:8890')
        params = _params(
            dag_module,
            scopes=['GB1:2025'],
            leagues=['ENG-Premier League'],
            approval_journal=str(
                Path('/opt/airflow/logs/transfermarkt-approvals/test.json')
            ),
            approval_bundles={
                'GB1__2025': {
                    'paid_proxy_packet_id': 'paid-1',
                    'paid_proxy_packet_hash': 'a' * 64,
                    'production_write_packet_id': 'write-1',
                    'production_write_packet_hash': 'b' * 64,
                },
            },
        )
        environments = dag_module._plan_exact_scopes(
            **self._context(dag_module, params),
        )

        assert captured['params']['scopes'] == ['GB1:2025']
        assert captured['params']['leagues'] == ['ENG-Premier League']
        assert captured['max_batch_size'] == 8
        assert captured['parent_cycle_id'] == 'scheduled__2026-07-13'
        assert len(environments) == 1
        env = environments[0]
        assert json.loads(env['TM_SCOPE_PAYLOAD_JSON'])['scope_id'] == 'GB1__2025'
        assert env['TM_READER_REVISION'] == '7'
        assert env['TM_PROVIDER_HARD_CAP_BYTES'] == str(15 * 1024 * 1024)
        assert env['TM_REFRESH_MODE'] == 'current'
        assert env['TM_CHECKPOINT_TTL_DAYS'] == '35'
        assert env['TM_ENTITY_TIMEOUT_SECONDS'] == '3600'
        assert env['TM_REQUIRE_METERED_PROXY'] == 'true'

    def test_downstream_approval_is_not_falsely_required_before_scope_id(
        self, dag_module,
    ):
        assert not hasattr(dag_module, '_downstream_approval_refs')

    def test_scheduled_run_fails_closed_without_exact_approval(
        self, dag_module, monkeypatch, tmp_path,
    ):
        from utils import transfermarkt_scope_planner as planner

        monkeypatch.setattr(
            planner,
            'plan_transfermarkt_scopes',
            lambda *a, **kw: SimpleNamespace(mapped_payloads=(_payload(tmp_path),)),
        )
        monkeypatch.setattr(dag_module, '_read_promoted_registry', lambda **kw: [{'x': 1}])
        monkeypatch.setenv('TM_PROXY_CONTROL_URL', 'http://proxy-filter:8890')
        with pytest.raises(Exception, match='approval bundle is required'):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, _params(dag_module)),
            )

    def test_direct_fallback_is_blocked_before_registry_read(
        self, dag_module, monkeypatch,
    ):
        monkeypatch.delenv('TM_PROXY_CONTROL_URL', raising=False)
        registry = MagicMock()
        monkeypatch.setattr(dag_module, '_read_promoted_registry', registry)
        with pytest.raises(Exception, match='direct fallback is forbidden'):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, _params(dag_module)),
            )
        registry.assert_not_called()

    def test_reused_one_shot_identity_is_rejected(self, dag_module):
        value = {
            'paid_proxy_packet_id': 'same',
            'paid_proxy_packet_hash': 'a' * 64,
            'production_write_packet_id': 'same',
            'production_write_packet_hash': 'b' * 64,
        }
        with pytest.raises(Exception, match='must be distinct'):
            dag_module._approval_bundle({'scope': value}, scope_id='scope')


class TestRegistryRead:
    def test_query_is_read_only_and_empty_registry_fails(self, dag_module, monkeypatch):
        from utils import transfermarkt_native_v2 as tm_v2

        class Cursor:
            description = [('competition_id',)]

            def execute(self, sql):
                self.sql = sql

            def fetchall(self):
                return []

            def close(self):
                pass

        cursor = Cursor()
        connection = MagicMock()
        connection.cursor.return_value = cursor
        monkeypatch.setattr(tm_v2, 'connect', lambda: connection)
        with pytest.raises(Exception, match='no exact promoted'):
            dag_module._read_promoted_registry()
        assert cursor.sql.lstrip().startswith('WITH promoted AS')
        assert not any(word in cursor.sql.upper() for word in ('INSERT ', 'DELETE ', 'UPDATE '))


def _write_manifest_and_ledger(module, tmp_path: Path, *, bad_digest=False):
    from utils import transfermarkt_native_v2 as tm_v2
    from utils import transfermarkt_dq_contracts as tm_dq
    from utils.transfermarkt_scope_state import EntityEvidence, ScopeManifest

    payload = _payload(tmp_path)
    entities = tuple(
        EntityEvidence(
            entity=name,
            applicability_status='ok',
            expected_rows=1,
            raw_rows=1,
            dedup_rows=1,
            key_hash=f'{index + 1:064x}',
            content_hash=f'{index + 11:064x}',
            dq_status='passed',
            decoded_bytes=10,
            wire_bytes=8,
            provider_metered_bytes=12,
            requests=1,
            retries=0,
            cache_hits=0,
            duration_ms=100,
        )
        for index, name in enumerate(tm_v2.NATIVE_ENTITIES)
    )
    manifest = ScopeManifest(
        parent_cycle_id=payload['parent_cycle_id'],
        child_cycle_id=payload['child_cycle_id'],
        scope_id=payload['scope_id'],
        competition_id=payload['competition_id'],
        edition_id=payload['edition_id'],
        canonical_competition_id=payload['canonical_competition_id'],
        canonical_season=payload['canonical_season'],
        registry_snapshot_id=payload['registry_snapshot_id'],
        capture_revision='capture-v1',
        parser_revision='parser-v1',
        schema_revision='1',
        reader_revision=7,
        entities=entities,
        dq_evidence={
            'status': 'passed',
            'registry_participant_count': 2,
            'edition_current': True,
            'scope_capture': {
                'schema_version': 1,
                'scope_id': payload['scope_id'],
                'competition_id': payload['competition_id'],
                'edition_id': payload['edition_id'],
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
            },
            'entity_statuses': {
                item.entity: item.applicability_status for item in entities
            },
            'entity_contracts': tm_dq.entity_applicability_contracts(
                entities=(item.entity for item in entities),
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
    manifest_path = Path(payload['result_paths']['scope_manifest'])
    manifest_path.parent.mkdir(parents=True)
    value = manifest.as_dict()
    value['manifest_digest'] = 'bad' if bad_digest else manifest.digest
    manifest_path.write_text(json.dumps(value), encoding='utf-8')
    provider_bytes = sum(item.provider_metered_bytes for item in entities)
    ledger = {
        'provider_metered_bytes': provider_bytes,
        'requests': len(entities),
        'retries': 0,
        'hard_provider_byte_budget': module.PROVIDER_HARD_CAP_BYTES,
        'soft_provider_byte_stop': module.PROVIDER_SOFT_STOP_BYTES,
    }
    Path(payload['parent_ledger']['path']).write_text(
        json.dumps(ledger), encoding='utf-8',
    )
    return payload, manifest, provider_bytes


class TestScopeSetGate:
    def test_scope_set_result_exposes_second_phase_transform_conf(
        self, dag_module, monkeypatch,
    ):
        from utils import transfermarkt_native_v2 as tm_v2
        from utils import transfermarkt_scope_planner as planner

        scope_set_id = 'a' * 64
        scope_set_manifest = {
            'scope_set_id': scope_set_id,
            'scope_digests': [['GB1__2025', 'b' * 64]],
        }
        monkeypatch.setattr(dag_module, '_build_scope_set', lambda *a, **kw: {
            'parent_cycle_id': 'parent-cycle',
            'scope_set_id': scope_set_id,
            'scope_set_manifest': scope_set_manifest,
            'scope_count': 1,
            'traffic': {'provider_metered_bytes': 1},
            'candidate_slot': 'b',
            'reader_revision': 7,
            'continuation_required': False,
            'remaining_count': 0,
            'coverage_complete': True,
        })
        monkeypatch.setattr(
            dag_module,
            '_read_promoted_registry',
            lambda **kw: [{'registry_snapshot_id': 'registry-1'}],
        )
        monkeypatch.setattr(
            dag_module, '_read_completed_scope_manifest_rows', lambda **kw: [],
        )
        monkeypatch.setattr(
            planner,
            'eligible_registry_scopes',
            lambda rows: [SimpleNamespace(scope_id='GB1__2025')],
        )
        state = tm_v2.ReaderState(
            exists=True, active_version='v2', active_slot='a', revision=7,
        )
        monkeypatch.setattr(tm_v2, 'connect', lambda: MagicMock())
        monkeypatch.setattr(tm_v2, 'read_reader_state', lambda *a, **kw: state)

        ti = MagicMock()
        ti.xcom_pull.side_effect = lambda task_ids: (
            [] if task_ids == 'plan_exact_scopes' else {
                'revision': 7, 'candidate_slot': 'b',
            }
        )
        result = dag_module._validate_scope_set(
            ti=ti, params={'scopes': ['GB1:2025'], 'leagues': []},
        )
        assert result['transform_conf'] == {
            'transfermarkt_parent_cycle_id': 'parent-cycle',
            'transfermarkt_scope_set_id': scope_set_id,
            'transfermarkt_scope_set_manifest': scope_set_manifest,
            'transfermarkt_reader_revision': 7,
            'transfermarkt_candidate_slot': 'b',
        }
        assert 'issue fresh exact Silver and Gold' in result['next_action']
        assert result['promotion_ready'] is True
        assert result['explicit_scope_selection'] is True

    def test_every_entity_manifest_and_provider_ledger_build_one_scope_set(
        self, dag_module, tmp_path,
    ):
        payload, manifest, provider_bytes = _write_manifest_and_ledger(
            dag_module, tmp_path,
        )
        planned = [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}]
        result = dag_module._build_scope_set(
            planned,
            {'revision': 7, 'candidate_slot': 'b'},
        )
        assert result['parent_cycle_id'] == payload['parent_cycle_id']
        assert len(result['scope_set_id']) == 64
        assert result['scope_count'] == 1
        assert result['continuation_required'] is False
        assert result['traffic']['provider_metered_bytes'] == provider_bytes
        assert result['scope_set_manifest']['scope_digests'] == [
            [payload['scope_id'], manifest.digest],
        ]

    def test_bounded_batches_accumulate_into_registry_complete_slot(
        self, dag_module, tmp_path,
    ):
        from utils.transfermarkt_scope_state import ScopeManifest

        payload, current, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        value = current.as_dict()
        value.update({
            'parent_cycle_id': 'scheduled__previous-batch',
            'child_cycle_id': 'tm-child-previous',
            'scope_id': 'CL__2025',
            'competition_id': 'CL',
            'canonical_competition_id': 'TM-CL',
            'reader_revision': 6,
        })
        capture = value['dq_evidence']['scope_capture']
        capture['scope_id'] = 'CL__2025'
        capture['competition_id'] = 'CL'
        previous = ScopeManifest.from_mapping(value)
        row = {
            key: getattr(previous, key)
            for key in (
                'parent_cycle_id', 'child_cycle_id', 'scope_id',
                'competition_id', 'edition_id', 'canonical_competition_id',
                'canonical_season', 'registry_snapshot_id', 'capture_revision',
                'parser_revision', 'schema_revision', 'reader_revision',
            )
        }
        row.update({
            'entity_manifest_json': json.dumps({
                'entities': [item.as_dict() for item in previous.entities],
                'dq_evidence': previous.dq_evidence,
            }),
            'manifest_digest': previous.digest,
            'status': 'complete',
        })
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
            target_scope_ids=['GB1__2025', 'CL__2025'],
            persisted_manifest_rows=[row],
        )
        assert result['coverage_complete'] is True
        assert result['scope_count'] == 2
        assert result['missing_scope_ids'] == []
        assert len(result['scope_set_manifest']['scope_digests']) == 2

    def test_partial_registry_coverage_cannot_produce_transform_scope_set(
        self, dag_module, tmp_path,
    ):
        payload, _, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
            target_scope_ids=['GB1__2025', 'CL__2025'],
        )
        assert result['coverage_complete'] is False
        assert result['scope_set_id'] is None
        assert result['scope_set_manifest'] is None
        assert result['missing_scope_ids'] == ['CL__2025']

    def test_manifest_digest_drift_blocks_silver(self, dag_module, tmp_path):
        payload, _, _ = _write_manifest_and_ledger(
            dag_module, tmp_path, bad_digest=True,
        )
        with pytest.raises(Exception, match='digest mismatch'):
            dag_module._build_scope_set(
                [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
                {'revision': 7, 'candidate_slot': 'b'},
            )

    def test_bounded_continuation_is_not_promotion_ready(
        self, dag_module, tmp_path,
    ):
        payload, _, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        payload['continuation_required'] = True
        payload['remaining_count'] = 9
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
        )
        assert result['continuation_required'] is True
        assert result['remaining_count'] == 9

    def test_unknown_or_mismatched_provider_counter_blocks_silver(
        self, dag_module, tmp_path,
    ):
        payload, _, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        ledger_path = Path(payload['parent_ledger']['path'])
        ledger = json.loads(ledger_path.read_text(encoding='utf-8'))
        ledger['provider_metered_bytes'] = -1
        ledger_path.write_text(json.dumps(ledger), encoding='utf-8')
        with pytest.raises(Exception, match='ledger disagrees'):
            dag_module._build_scope_set(
                [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
                {'revision': 7, 'candidate_slot': 'b'},
            )


class TestReaderPreflight:
    def test_native_v2_feature_flag_blocks_before_sql(self, dag_module, monkeypatch):
        from utils import transfermarkt_native_v2 as tm_v2

        monkeypatch.delenv('TM_NATIVE_V2_ENABLED', raising=False)
        connect = MagicMock()
        monkeypatch.setattr(tm_v2, 'connect', connect)
        with pytest.raises(Exception, match='must be true'):
            dag_module._preflight_reader_route_for_paid_cycle()
        connect.assert_not_called()

    def test_live_v2_route_is_pinned(self, dag_module, monkeypatch):
        from utils import transfermarkt_native_v2 as tm_v2

        monkeypatch.setenv('TM_NATIVE_V2_ENABLED', 'true')
        state = tm_v2.ReaderState(
            exists=True,
            active_version='v2',
            active_slot='a',
            revision=9,
            legacy_writers_disabled_at=None,
        )
        cursor = MagicMock()
        connection = MagicMock()
        connection.cursor.return_value = cursor
        monkeypatch.setattr(tm_v2, 'connect', lambda: connection)
        monkeypatch.setattr(tm_v2, 'read_reader_state', lambda *a, **kw: state)
        monkeypatch.setattr(
            tm_v2, 'verify_reader_views', lambda *a, **kw: {'passed': True},
        )
        result = dag_module._preflight_reader_route_for_paid_cycle()
        assert result['revision'] == 9
        assert result['candidate_slot'] == 'b'
        assert result['write_mode'] == 'dual'
        assert result['paid_io_allowed'] is True
