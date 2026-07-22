"""Production contract tests for the registry-driven Transfermarkt DAG."""

from __future__ import annotations

from datetime import timedelta
from enum import Enum
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
        'mv_transfers_limit': 500,
        'refresh_mode': 'auto',
        'coach_history_ttl_days': 28,
        'checkpoint_ttl_days': 35,
        'proxy_lease_ttl_seconds': 3600,
        'proxy_request_limit': 1610,
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
        assert 'case "$TM_APPROVAL_MODE" in' in command
        standing = command.split('standing_policy)')[1].split(';;')[0]
        one_shot = command.split('one_shot)')[1].split(';;')[0]
        fallback = command.split('*)')[1].split(';;')[0]
        for flag in (
            '--approval-journal "$TM_APPROVAL_JOURNAL"',
            '--paid-proxy-approval-packet-id "$TM_PAID_APPROVAL_PACKET_ID"',
            '--paid-proxy-approval-packet-hash "$TM_PAID_APPROVAL_PACKET_HASH"',
            '--production-write-approval-packet-id "$TM_WRITE_APPROVAL_PACKET_ID"',
            '--production-write-approval-packet-hash'
            ' "$TM_WRITE_APPROVAL_PACKET_HASH"',
        ):
            assert flag in one_shot
        assert '--standing-policy "$TM_STANDING_POLICY_PATH"' in standing
        assert (
            '--standing-policy-sha256 "$TM_STANDING_POLICY_SHA256"' in standing
        )
        assert 'approval-packet' not in standing
        assert '--approval-journal' not in standing
        assert '--standing-policy' not in one_shot
        assert 'exit 1' in fallback
        assert '--career-window-limit "$TM_MV_TRANSFERS_LIMIT"' in command
        assert '--cycle-budget-bytes "$TM_PROVIDER_HARD_CAP_BYTES"' in command
        assert '--soft-byte-stop-bytes "$TM_PROVIDER_SOFT_STOP_BYTES"' in command
        assert '--parent-byte-budget "$TM_PARENT_BYTE_BUDGET"' in command
        assert '--parent-soft-byte-stop "$TM_PARENT_SOFT_BYTE_STOP"' in command
        assert '--parent-request-limit "$TM_PARENT_REQUEST_LIMIT"' in command
        assert '--parent-retry-limit "$TM_PARENT_RETRY_LIMIT"' in command
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
        assert {'--standing-policy', '--standing-policy-sha256'} <= rendered_flags
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
        assert params['mv_transfers_limit']._kw['maximum'] == 500
        assert params['proxy_retry_limit']._kw['maximum'] == 800
        assert params['proxy_request_limit']._kw['maximum'] == 1610
        assert params['proxy_lease_ttl_seconds']._kw['maximum'] == 3600
        assert dag_module.PROVIDER_HARD_CAP_BYTES == 24 * 1024 * 1024
        assert dag_module.PROVIDER_SOFT_STOP_BYTES == 22 * 1024 * 1024
        assert dag_module.PARENT_BYTE_BUDGET == 336 * 1024 * 1024
        assert dag_module.PARENT_SOFT_BYTE_STOP == 320 * 1024 * 1024
        assert dag_module.PARENT_REQUEST_LIMIT == 8 * 1610
        assert dag_module.PARENT_RETRY_LIMIT == 8 * 800
        assert dag_module.PROXY_CONCURRENCY == 1

    def test_task_timeout_covers_every_entity_subprocess_it_supervises(
        self, dag_module,
    ):
        # A SIGKILL at the task timeout lands in the middle of paid I/O: the
        # attempt guard is not written and the entity's evidence is lost.  The
        # ceiling therefore has to be worst-case compatible, not average.
        from types import SimpleNamespace

        from dags.scripts import run_transfermarkt_scope_cycle as wrapper
        from scrapers.transfermarkt import models

        task = _bash_task('run_exact_child_cycle')
        execution_timeout = task._init_kwargs['execution_timeout']
        params = dag_module.dag._dag_kwargs['params']
        args = SimpleNamespace(
            entity_timeout_seconds=params['entity_timeout_seconds']._kw[
                'maximum'
            ],
        )
        worst_case = sum(
            wrapper._entity_timeout_seconds(args, entity)
            for entity in wrapper.ENTITY_ORDER
        )

        assert worst_case == 18_000
        assert execution_timeout.total_seconds() >= worst_case
        assert execution_timeout == timedelta(
            seconds=models.SCOPE_WALL_CLOCK_TIMEOUT_SECONDS,
        )

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
        assert captured['selection_mode'] == 'current_only'
        assert len(environments) == 1
        env = environments[0]
        assert json.loads(env['TM_SCOPE_PAYLOAD_JSON'])['scope_id'] == 'GB1__2025'
        assert env['TM_READER_REVISION'] == '7'
        assert env['TM_PROVIDER_HARD_CAP_BYTES'] == str(24 * 1024 * 1024)
        assert env['TM_PARENT_BYTE_BUDGET'] == str(336 * 1024 * 1024)
        assert env['TM_PARENT_SOFT_BYTE_STOP'] == str(320 * 1024 * 1024)
        assert env['TM_PARENT_REQUEST_LIMIT'] == str(8 * 1610)
        assert env['TM_PARENT_RETRY_LIMIT'] == str(8 * 800)
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

        monkeypatch.delenv('TM_STANDING_POLICY_ENABLED', raising=False)
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


def _write_standing_policy(tmp_path: Path, **overrides) -> tuple[Path, str]:
    from dags.scripts.run_transfermarkt_scope_cycle import required_write_tables
    from dags.utils.transfermarkt_approval import load_standing_policy

    value = {
        'policy_version': 1,
        'dag_id': 'dag_ingest_transfermarkt',
        'approved_by': 'sergeykuznetsov1995',
        'approved_at': '2026-07-14T00:00:00Z',
        'expires_at': '2100-01-01T00:00:00Z',
        'paid_proxy': {
            'byte_cap_bytes': 24 * 1024 * 1024,
            'request_limit': 1610,
            'retry_limit': 800,
            'concurrency': 1,
        },
        'production_write': {
            'byte_cap_bytes': 0,
            'request_limit': 0,
            'retry_limit': 0,
            'concurrency': 1,
        },
        'allowed_write_tables': sorted(
            required_write_tables('dual') | required_write_tables('native-only')
        ),
    }
    value.update(overrides)
    path = tmp_path / 'standing_approval_policy.json'
    path.write_text(json.dumps(value), encoding='utf-8')
    return path, load_standing_policy(path).policy_hash


class TestStandingPolicyGate:
    def _context(self, module, params, *, run_type='scheduled'):
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
            'dag_run': SimpleNamespace(run_type=run_type),
            'run_id': f'{run_type}__2026-07-13',
            'ds': '2026-07-13',
            'params': params,
        }

    def _arm(self, dag_module, monkeypatch, tmp_path):
        from utils import transfermarkt_scope_planner as planner

        payload = _payload(tmp_path)
        monkeypatch.setattr(
            planner,
            'plan_transfermarkt_scopes',
            lambda *a, **kw: SimpleNamespace(mapped_payloads=(payload,)),
        )
        monkeypatch.setattr(
            dag_module, '_read_promoted_registry', lambda **kw: [{'x': 1}],
        )
        monkeypatch.setenv('TM_PROXY_CONTROL_URL', 'http://proxy-filter:8890')
        monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
        return payload

    def _standing_params(self, dag_module, **overrides):
        values = {'proxy_retry_limit': 800}
        values.update(overrides)
        return _params(dag_module, **values)

    def test_standing_policy_scheduled_run_builds_standing_envs(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, policy_hash = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))

        environments = dag_module._plan_exact_scopes(
            **self._context(dag_module, self._standing_params(dag_module)),
        )

        assert len(environments) == 1
        env = environments[0]
        assert env['TM_APPROVAL_MODE'] == 'standing_policy'
        assert env['TM_STANDING_POLICY_PATH'] == str(policy_path)
        assert env['TM_STANDING_POLICY_SHA256'] == policy_hash
        assert 'TM_APPROVAL_JOURNAL' not in env
        assert 'TM_PAID_APPROVAL_PACKET_ID' not in env
        assert 'TM_PAID_APPROVAL_PACKET_HASH' not in env
        assert 'TM_WRITE_APPROVAL_PACKET_ID' not in env
        assert 'TM_WRITE_APPROVAL_PACKET_HASH' not in env
        assert env['TM_REQUIRE_METERED_PROXY'] == 'true'

    def test_standing_policy_env_gate_off_requires_manual_ritual(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        monkeypatch.delenv('TM_STANDING_POLICY_ENABLED', raising=False)
        policy_path, _ = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))

        with pytest.raises(
            Exception, match='exact paid/write approval bundle is required',
        ):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, self._standing_params(dag_module)),
            )

    def test_manual_run_with_explicit_scopes_requires_one_shot_bundles(
        self, dag_module, monkeypatch, tmp_path,
    ):
        # An explicit selector skips the planner's dueness filter, so a manual
        # trigger must never ride the standing grant even with the gate on.
        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, _ = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))
        params = self._standing_params(dag_module, scopes=['GB1:2025'])

        with pytest.raises(
            Exception, match='exact paid/write approval bundle is required',
        ):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, params, run_type='manual'),
            )

    def test_manual_run_with_empty_selectors_requires_one_shot_bundles(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, _ = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))

        with pytest.raises(
            Exception, match='exact paid/write approval bundle is required',
        ):
            dag_module._plan_exact_scopes(
                **self._context(
                    dag_module,
                    self._standing_params(dag_module),
                    run_type='manual',
                ),
            )

    def test_scheduled_run_with_explicit_selectors_requires_one_shot_bundles(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, _ = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))
        params = self._standing_params(
            dag_module, leagues=['ENG-Premier League'],
        )

        with pytest.raises(
            Exception, match='exact paid/write approval bundle is required',
        ):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, params),
            )

    def test_dag_run_without_run_type_is_not_scheduled(
        self, dag_module, monkeypatch, tmp_path,
    ):
        # A present dag_run whose run_type is missing must not fall through
        # to the run_id prefix: the prefix is operator-controlled input.
        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, _ = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))
        context = self._context(dag_module, self._standing_params(dag_module))
        context['dag_run'] = SimpleNamespace()
        context['run_id'] = 'scheduled__2026-07-13'

        with pytest.raises(
            Exception, match='exact paid/write approval bundle is required',
        ):
            dag_module._plan_exact_scopes(**context)

    def test_enum_run_type_is_recognized_as_scheduled(
        self, dag_module, monkeypatch, tmp_path,
    ):
        # Airflow's DagRunType is a str Enum; its member must gate the same
        # way its plain-string value does.
        class FakeRunType(str, Enum):
            SCHEDULED = 'scheduled'

        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, policy_hash = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))
        context = self._context(dag_module, self._standing_params(dag_module))
        context['dag_run'] = SimpleNamespace(run_type=FakeRunType.SCHEDULED)

        environments = dag_module._plan_exact_scopes(**context)

        assert environments[0]['TM_APPROVAL_MODE'] == 'standing_policy'
        assert environments[0]['TM_STANDING_POLICY_SHA256'] == policy_hash

    @pytest.mark.parametrize('run_type', ['backfill', 'dataset_triggered'])
    def test_non_scheduled_run_types_require_one_shot_bundles(
        self, dag_module, monkeypatch, tmp_path, run_type,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, _ = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))

        with pytest.raises(
            Exception, match='exact paid/write approval bundle is required',
        ):
            dag_module._plan_exact_scopes(
                **self._context(
                    dag_module,
                    self._standing_params(dag_module),
                    run_type=run_type,
                ),
            )

    def test_standing_policy_missing_or_invalid_file_fails_closed(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        monkeypatch.setattr(
            dag_module, 'STANDING_POLICY_PATH', str(tmp_path / 'absent.json'),
        )
        with pytest.raises(Exception, match='unreadable'):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, self._standing_params(dag_module)),
            )

        broken = tmp_path / 'broken.json'
        broken.write_text('{not json', encoding='utf-8')
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(broken))
        with pytest.raises(Exception, match='not valid JSON'):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, self._standing_params(dag_module)),
            )

    @pytest.mark.parametrize(
        ('paid_override', 'params_override'),
        [
            ({'byte_cap_bytes': 15 * 1024 * 1024}, {}),
            ({'request_limit': 1611}, {}),
            ({'retry_limit': 801}, {}),
            ({'concurrency': 2}, {}),
            ({}, {'proxy_retry_limit': 2}),
        ],
    )
    def test_standing_policy_caps_must_equal_pinned_limits(
        self, dag_module, monkeypatch, tmp_path, paid_override, params_override,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        paid = {
            'byte_cap_bytes': 24 * 1024 * 1024,
            'request_limit': 1610,
            'retry_limit': 800,
            'concurrency': 1,
        }
        paid.update(paid_override)
        policy_path, _ = _write_standing_policy(tmp_path, paid_proxy=paid)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))

        with pytest.raises(
            Exception, match='caps differ from wrapper limits|pinned',
        ):
            dag_module._plan_exact_scopes(
                **self._context(
                    dag_module,
                    self._standing_params(dag_module, **params_override),
                ),
            )

    def test_standing_policy_expired_fails_closed(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, _ = _write_standing_policy(
            tmp_path,
            approved_at='2026-01-01T00:00:00Z',
            expires_at='2026-02-01T00:00:00Z',
        )
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))

        with pytest.raises(Exception, match='expired'):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, self._standing_params(dag_module)),
            )

    def test_standing_policy_wrong_dag_id_fails_closed(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, _ = _write_standing_policy(
            tmp_path, dag_id='dag_ingest_sofascore',
        )
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))

        with pytest.raises(Exception, match='dag_id mismatch'):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, self._standing_params(dag_module)),
            )

    def test_standing_policy_missing_required_write_table_fails_closed(
        self, dag_module, monkeypatch, tmp_path,
    ):
        from dags.scripts.run_transfermarkt_scope_cycle import (
            required_write_tables,
        )

        self._arm(dag_module, monkeypatch, tmp_path)
        tables = sorted(
            (required_write_tables('dual') | required_write_tables('native-only'))
            - {'iceberg.ops.transfermarkt_scope_manifest_v2'}
        )
        policy_path, _ = _write_standing_policy(
            tmp_path, allowed_write_tables=tables,
        )
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))

        with pytest.raises(Exception, match='omits write tables'):
            dag_module._plan_exact_scopes(
                **self._context(dag_module, self._standing_params(dag_module)),
            )

    def test_explicit_bundles_take_precedence_over_standing_policy(
        self, dag_module, monkeypatch, tmp_path,
    ):
        payload = self._arm(dag_module, monkeypatch, tmp_path)
        policy_path, _ = _write_standing_policy(tmp_path)
        monkeypatch.setattr(dag_module, 'STANDING_POLICY_PATH', str(policy_path))
        params = self._standing_params(
            dag_module,
            approval_bundles={
                payload['scope_id']: {
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

        assert len(environments) == 1
        env = environments[0]
        assert env['TM_APPROVAL_MODE'] == 'one_shot'
        assert env['TM_PAID_APPROVAL_PACKET_ID'] == 'paid-1'
        assert env['TM_WRITE_APPROVAL_PACKET_HASH'] == 'b' * 64
        assert env['TM_APPROVAL_JOURNAL'] == dag_module.APPROVAL_JOURNAL
        assert 'TM_STANDING_POLICY_PATH' not in env
        assert 'TM_STANDING_POLICY_SHA256' not in env


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
    from utils.transfermarkt_scope_state import (
        CAPTURE_REVISION,
        EntityEvidence,
        ScopeManifest,
    )

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
        capture_revision=CAPTURE_REVISION,
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
        'hard_provider_byte_budget': module.PARENT_BYTE_BUDGET,
        'soft_provider_byte_stop': module.PARENT_SOFT_BYTE_STOP,
    }
    Path(payload['parent_ledger']['path']).write_text(
        json.dumps(ledger), encoding='utf-8',
    )
    return payload, manifest, provider_bytes


def _target(
    scope_id: str,
    *,
    competition_id: str,
    edition_id: str = '2025',
    current: bool = False,
    canonical_competition_id: str | None = None,
    canonical_season: str = '2526',
) -> dict:
    """One promoted-registry target row as `eligible_registry_scopes` states it."""

    return {
        'scope_id': scope_id,
        'competition_id': competition_id,
        'edition_id': edition_id,
        'current': current,
        'canonical_competition_id': (
            canonical_competition_id or f'TM-{competition_id}'
        ),
        'canonical_season': canonical_season,
        'competition_type': 'domestic_league',
        'team_type': 'club',
        'gender': 'men',
        'age_category': 'senior',
    }


def _persisted_row(
    manifest,
    *,
    scope_id: str,
    competition_id: str,
    child_cycle_id: str,
    is_fresh: bool = True,
    reader_revision: int = 6,
    registry_snapshot_id: str | None = None,
) -> dict:
    """Shape one earlier batch's committed ops row for the cumulative slot."""

    from utils.transfermarkt_scope_state import ScopeManifest

    value = manifest.as_dict()
    value.update({
        'parent_cycle_id': f'scheduled__{child_cycle_id}',
        'child_cycle_id': child_cycle_id,
        'scope_id': scope_id,
        'competition_id': competition_id,
        'canonical_competition_id': f'TM-{competition_id}',
        'reader_revision': reader_revision,
        'registry_snapshot_id': (
            registry_snapshot_id or manifest.registry_snapshot_id
        ),
    })
    capture = value['dq_evidence']['scope_capture']
    capture['scope_id'] = scope_id
    capture['competition_id'] = competition_id
    persisted = ScopeManifest.from_mapping(value)
    row = {
        key: getattr(persisted, key)
        for key in (
            'parent_cycle_id', 'child_cycle_id', 'scope_id',
            'competition_id', 'edition_id', 'canonical_competition_id',
            'canonical_season', 'registry_snapshot_id', 'capture_revision',
            'parser_revision', 'schema_revision', 'reader_revision',
        )
    }
    row.update({
        'entity_manifest_json': json.dumps({
            'entities': [item.as_dict() for item in persisted.entities],
            'dq_evidence': persisted.dq_evidence,
        }),
        'manifest_digest': persisted.digest,
        'status': 'complete',
        'is_fresh': is_fresh,
    })
    return row


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
            'registry_snapshot_id': 'registry-1',
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
            lambda rows: [
                planner.RegistryScopeTarget(
                    scope_id='GB1__2025',
                    competition_id='GB1',
                    edition_id='2025',
                    current=True,
                ),
            ],
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
        payload, current, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        row = _persisted_row(
            current, scope_id='CL__2025', competition_id='CL',
            child_cycle_id='tm-child-previous',
        )
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
            target_scopes=[
                _target('GB1__2025', competition_id='GB1'),
                _target('CL__2025', competition_id='CL'),
            ],
            persisted_manifest_rows=[row],
        )
        assert result['coverage_complete'] is True
        assert result['scope_count'] == 2
        assert result['missing_scope_ids'] == []
        assert len(result['scope_set_manifest']['scope_digests']) == 2

    def test_a_registry_rotation_does_not_wipe_the_accumulated_slot(
        self, dag_module, tmp_path,
    ):
        # A snapshot id hashes every page of the source registry, so any byte
        # that moves on the site mints a new one and discovery runs monthly.
        # While slot membership required one shared snapshot, every rotation
        # threw away everything earlier batches had bought — the target could
        # never be reached.  What a manifest captured does not change when the
        # registry is re-read.
        payload, current, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        older_snapshot = _persisted_row(
            current, scope_id='CL__2025', competition_id='CL',
            child_cycle_id='tm-child-previous',
            registry_snapshot_id='tm-discovery-two-rotations-ago',
        )
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
            target_scopes=[
                _target('GB1__2025', competition_id='GB1'),
                _target('CL__2025', competition_id='CL'),
            ],
            persisted_manifest_rows=[older_snapshot],
        )
        assert result['scope_count'] == 2
        assert result['coverage_complete'] is True
        # The slot is frozen against the snapshot THIS paid batch ran under.
        assert result['registry_snapshot_id'] == payload['registry_snapshot_id']
        assert result['registry_drift_scope_count'] == 0

    def test_a_batch_may_not_drift_from_the_capture_canon(
        self, dag_module, tmp_path,
    ):
        # A capture revision that is not the canon is exactly what orphaned the
        # two production manifests: they were written under a per-batch
        # selection hash and can never join a slot again.  Fail the cycle
        # instead of silently minting evidence no slot will ever accept.
        import dataclasses

        payload, manifest, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        drifted = dataclasses.replace(manifest, capture_revision='selection-hash')
        value = drifted.as_dict() | {'manifest_digest': drifted.digest}
        Path(payload['result_paths']['scope_manifest']).write_text(
            json.dumps(value), encoding='utf-8',
        )

        with pytest.raises(Exception, match='capture revision'):
            dag_module._build_scope_set(
                [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
                {'revision': 7, 'candidate_slot': 'b'},
            )

    def test_a_scope_the_registry_retired_is_dropped_but_named(
        self, dag_module, tmp_path,
    ):
        # A competition the registry no longer targets legitimately leaves the
        # slot — but a coverage change is never silent.
        payload, current, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        retired = _persisted_row(
            current, scope_id='OLD__2019', competition_id='OLD',
            child_cycle_id='tm-child-retired',
        )
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
            target_scopes=[_target('GB1__2025', competition_id='GB1')],
            persisted_manifest_rows=[retired],
        )
        assert result['retired_scope_ids'] == ['OLD__2019']
        assert result['registry_drift_scope_count'] == 0
        assert result['scope_count'] == 1

    def test_a_scope_the_registry_now_means_differently_must_be_recrawled(
        self, dag_module, tmp_path,
    ):
        # Snapshot identity is not evidence, but MEANING is: if the promoted
        # registry now publishes the scope under another canonical id or another
        # classification, what was captured no longer describes what is served.
        payload, current, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        renamed = _persisted_row(
            current, scope_id='CL__2025', competition_id='CL',
            child_cycle_id='tm-child-previous',
        )
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
            target_scopes=[
                _target('GB1__2025', competition_id='GB1'),
                _target(
                    'CL__2025', competition_id='CL',
                    canonical_competition_id='UEFA-Champions League',
                ),
            ],
            persisted_manifest_rows=[renamed],
        )
        assert result['scope_count'] == 1
        assert result['registry_drift_scope_ids'] == ['CL__2025']
        assert result['missing_scope_ids'] == ['CL__2025']

    def test_partial_registry_coverage_still_builds_and_reports_the_slot(
        self, dag_module, tmp_path,
    ):
        # The ~9.7k-scope target is bought eight scopes per bounded daily batch.
        # Refusing to build a slot short of the complete target meant no Silver
        # or Gold for months, so partial coverage is now reported, not refused;
        # what it blocks is the reader cutover (readiness), not the build.
        payload, _, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
            target_scopes=[
                _target('GB1__2025', competition_id='GB1'),
                _target('CL__2025', competition_id='CL'),
            ],
        )
        assert result['coverage_complete'] is False
        assert len(result['scope_set_id']) == 64
        assert result['scope_set_manifest']['scope_digests'] == [
            [payload['scope_id'], result['scope_set_manifest'][
                'scope_digests'
            ][0][1]],
        ]
        assert result['scope_count'] == 1
        assert result['coverage_target_count'] == 2
        assert result['coverage_ratio'] == 0.5
        assert result['missing_scope_count'] == 1
        assert result['missing_scope_ids'] == ['CL__2025']
        assert result['continuation_required'] is True

    def test_a_stale_current_edition_stays_in_the_slot_and_is_reported(
        self, dag_module, tmp_path,
    ):
        # A stale current edition is a quality problem, not a membership one.
        # Dropping it would let the slot shrink, and a slot that shrinks can
        # never pass the cutover monotonicity gate again — so it is kept, named,
        # and blocks the reader flip in readiness instead.
        payload, current, _ = _write_manifest_and_ledger(dag_module, tmp_path)
        old_history = _persisted_row(
            current, scope_id='CL__2025', competition_id='CL',
            child_cycle_id='tm-child-history', is_fresh=False,
        )
        stale_current = _persisted_row(
            current, scope_id='ES1__2025', competition_id='ES1',
            child_cycle_id='tm-child-stale', is_fresh=False,
        )
        result = dag_module._build_scope_set(
            [{'TM_SCOPE_PAYLOAD_JSON': json.dumps(payload)}],
            {'revision': 7, 'candidate_slot': 'b'},
            target_scopes=[
                _target('GB1__2025', competition_id='GB1', current=True),
                _target('CL__2025', competition_id='CL'),
                _target('ES1__2025', competition_id='ES1', current=True),
            ],
            persisted_manifest_rows=[old_history, stale_current],
        )
        assert result['scope_count'] == 3
        assert result['stale_current_scope_ids'] == ['ES1__2025']
        assert result['missing_scope_ids'] == []
        assert sorted(
            item[0] for item in result['scope_set_manifest']['scope_digests']
        ) == ['CL__2025', 'ES1__2025', 'GB1__2025']

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


class TestBudgetCanonSingleSource:
    def test_scheduled_parent_cycle_runs_daily(self):
        from utils.config import SCHEDULES

        assert SCHEDULES['dag_ingest_transfermarkt'] == '0 4 * * *'

    def test_every_consumer_pins_the_same_budget_canon(self, dag_module):
        """models == scope_cycle == runner == DAG == bench == prepare == policy.

        The numbers are compared through imports, so any consumer drifting
        away from the canon in scrapers/transfermarkt/models.py fails here.
        """
        import importlib.util

        from dags.scripts import run_transfermarkt_scope_cycle as wrapper
        from dags.scripts import run_transfermarkt_scraper as runner
        from dags.utils.transfermarkt_approval import load_standing_policy
        from scrapers.transfermarkt import models
        from scrapers.transfermarkt import scraper as scraper_module

        root = Path(__file__).resolve().parents[3]

        def _load(path: Path, name: str):
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module

        bench = _load(
            root / 'scripts' / 'research' / 'bench_transfermarkt_fetch.py',
            'bench_transfermarkt_fetch_canon',
        )
        prepare = _load(
            root / 'scripts' / 'prepare_transfermarkt_scope_approvals.py',
            'prepare_transfermarkt_scope_approvals_canon',
        )
        policy = load_standing_policy(
            root / 'dags' / 'configs' / 'transfermarkt'
            / 'standing_approval_policy.json',
        )

        # Scope-cycle wrapper.
        assert wrapper.HARD_BYTE_CAP == models.SCOPE_HARD_PROVIDER_BYTE_CAP
        assert wrapper.SOFT_BYTE_STOP == models.SCOPE_SOFT_PROVIDER_BYTE_STOP
        assert wrapper.PARENT_BYTE_BUDGET == (
            models.PARENT_DAILY_HARD_PROVIDER_BYTE_CAP
        )
        assert wrapper.PARENT_SOFT_BYTE_STOP == (
            models.PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP
        )
        assert wrapper.PARENT_RETRY_LIMIT == models.PARENT_RETRY_LIMIT
        assert wrapper.DEFAULT_ENTITY_LIMITS == {
            name: {
                'decoded_bytes': int(budget['decoded_mb'] * 1024 * 1024),
                'requests': int(budget['requests']),
            }
            for name, budget in models.PRODUCTION_ENTITY_BUDGETS.items()
        }
        assert sum(
            item['requests'] for item in wrapper.DEFAULT_ENTITY_LIMITS.values()
        ) == models.SCOPE_REQUEST_LIMIT

        # Entity runner and the scraper's per-operation defaults.
        assert runner.PRODUCTION_CYCLE_BUDGET_BYTES == (
            models.SCOPE_HARD_PROVIDER_BYTE_CAP
        )
        assert runner.PRODUCTION_ENTITY_BUDGETS is (
            models.PRODUCTION_ENTITY_BUDGETS
        )
        assert runner.MAX_ROSTER_WINDOW == models.MAX_ROSTER_WINDOW == 500
        assert scraper_module._DEFAULT_DECODED_BUDGET_MB == {
            name: float(budget['decoded_mb'])
            for name, budget in models.PRODUCTION_ENTITY_BUDGETS.items()
        }
        assert scraper_module._DEFAULT_REQUEST_ATTEMPT_BUDGET == {
            name: int(budget['requests'])
            for name, budget in models.PRODUCTION_ENTITY_BUDGETS.items()
        }

        # The DAG.
        assert dag_module.PROVIDER_HARD_CAP_BYTES == (
            models.SCOPE_HARD_PROVIDER_BYTE_CAP
        )
        assert dag_module.PROVIDER_SOFT_STOP_BYTES == (
            models.SCOPE_SOFT_PROVIDER_BYTE_STOP
        )
        assert dag_module.PROXY_REQUEST_LIMIT == models.SCOPE_REQUEST_LIMIT
        assert dag_module.PROXY_RETRY_LIMIT == models.SCOPE_RETRY_LIMIT
        assert dag_module.PARENT_BYTE_BUDGET == (
            models.PARENT_DAILY_HARD_PROVIDER_BYTE_CAP
        )
        assert dag_module.PARENT_SOFT_BYTE_STOP == (
            models.PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP
        )
        assert dag_module.PARENT_REQUEST_LIMIT == models.PARENT_REQUEST_LIMIT
        assert dag_module.PARENT_RETRY_LIMIT == models.PARENT_RETRY_LIMIT
        assert dag_module.MV_HISTORY_DAILY_LIMIT == models.MAX_ROSTER_WINDOW
        assert dag_module.MAX_SCOPE_BATCH == models.MAX_SCOPE_BATCH

        # Benchmark phases.
        assert bench.PRODUCTION_CYCLE_BUDGET_BYTES == (
            models.SCOPE_HARD_PROVIDER_BYTE_CAP
        )
        assert bench.PRODUCTION_PHASE_BUDGETS == {
            phase: {
                'decoded_body_bytes': int(
                    models.PRODUCTION_ENTITY_BUDGETS[entity]['decoded_mb']
                    * 1024 * 1024
                ),
                'request_attempts': int(
                    models.PRODUCTION_ENTITY_BUDGETS[entity]['requests']
                ),
            }
            for phase, entity in bench._PHASE_ENTITY.items()
        }

        # Approval preparation script.
        assert prepare.PROVIDER_HARD_CAP_BYTES == (
            models.SCOPE_HARD_PROVIDER_BYTE_CAP
        )
        assert prepare.PROVIDER_SOFT_STOP_BYTES == (
            models.SCOPE_SOFT_PROVIDER_BYTE_STOP
        )
        assert prepare.PROXY_REQUEST_LIMIT == models.SCOPE_REQUEST_LIMIT
        assert prepare.PROXY_RETRY_LIMIT == models.SCOPE_RETRY_LIMIT
        assert prepare.MV_HISTORY_DAILY_LIMIT == models.MAX_ROSTER_WINDOW

        # Committed standing policy pins the SCOPE caps enforced on the child;
        # the parent caps are pinned via argv equality in the wrapper.
        assert policy.paid_proxy.byte_cap_bytes == (
            models.SCOPE_HARD_PROVIDER_BYTE_CAP
        )
        assert policy.paid_proxy.request_limit == models.SCOPE_REQUEST_LIMIT
        assert policy.paid_proxy.retry_limit == models.SCOPE_RETRY_LIMIT

        # Derived parent limits stay derived, not hand-edited.
        assert models.PARENT_REQUEST_LIMIT == (
            models.MAX_SCOPE_BATCH * models.SCOPE_REQUEST_LIMIT
        )
        assert models.PARENT_RETRY_LIMIT == (
            models.MAX_SCOPE_BATCH * models.SCOPE_RETRY_LIMIT
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
