"""DAG wiring and behaviour tests for the #948 Bronze DQ gate."""
from __future__ import annotations

import importlib
import sys

import pytest


def _reload():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop('dag_transform_transfermarkt_silver', None)
    sys.modules.pop('dags.dag_transform_transfermarkt_silver', None)
    return importlib.import_module('dag_transform_transfermarkt_silver')


def _task(task_id):
    from airflow.operators.python import PythonOperator

    return next(
        (t for t in PythonOperator._instances if t.task_id == task_id), None,
    )


def _manual_ti(mod):
    """XComs for the explicit manual compatibility path (legacy zone)."""

    scope = {
        'mode': 'manual_single_scope',
        'parent_cycle_id': 'cycle-1',
        'scope_set_id': None,
        'registry_snapshot_id': None,
        'reader_revision': 0,
        'scopes': [{
            'scope_id': 'manual:ENG-Premier League:2025',
            'competition_id': 'ENG-Premier League',
            'edition_id': '2025',
            'canonical_competition_id': 'ENG-Premier League',
            'canonical_season': '2526',
        }],
    }
    pin_payload = {
        'scope_set_id': None,
        'parent_cycle_id': 'cycle-1',
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
                'pin_transform_input_snapshots': pins,
            }[task_ids]

    return TI()


@pytest.fixture
def gate_env(monkeypatch):
    mod = _reload()

    class Cursor:
        def close(self):
            pass

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(mod.tm_v2, 'connect', lambda: Conn())

    from utils import alerts, medallion_config
    monkeypatch.setattr(medallion_config, 'load_competitions', lambda: {
        'competitions': [{
            'id': 'ENG-Premier League',
            'seasons': [{'id': 2324}, {'id': 2425}],
        }],
    })
    monkeypatch.setattr(
        alerts, 'telegram_dq_summary', lambda report, header=None: None,
    )
    return mod


def test_gate_sits_between_pins_and_write_approval():
    _reload()
    gate = _task('validate_bronze_quality')
    approval = _task('authorize_silver_writes')
    pins = _task('pin_transform_input_snapshots')
    assert gate is not None
    assert gate.upstream_task_ids == {'pin_transform_input_snapshots'}
    assert approval.upstream_task_ids == {'validate_bronze_quality'}
    assert 'validate_bronze_quality' in pins.downstream_task_ids
    # The old direct edge must be gone: a red Bronze fails the run before
    # the one-shot write approval is consumed.
    assert 'authorize_silver_writes' not in pins.downstream_task_ids


def test_gate_raises_on_error_results(gate_env, monkeypatch):
    mod = gate_env
    from utils import transfermarkt_bronze_dq as bronze_dq

    failing = bronze_dq.BronzeCheckResult(
        name='tm_legacy_phantom_pair[transfermarkt_players]',
        kind='legacy_phantom_pair',
        severity='ERROR',
        passed=False,
        details='3 violating rows',
        value=3,
    )
    warning_only = bronze_dq.BronzeCheckResult(
        name='tm_bronze_cross_batch_duplicates[squad_memberships]',
        kind='bronze_cross_batch_duplicates',
        severity='WARNING',
        passed=False,
        details='5 violating rows',
        value=5,
    )
    monkeypatch.setattr(
        bronze_dq, 'run_bronze_dq', lambda *a, **k: [failing, warning_only],
    )

    from airflow.exceptions import AirflowException
    with pytest.raises(AirflowException, match='tm_legacy_phantom_pair'):
        mod._validate_bronze_quality(ti=_manual_ti(mod))

    # WARNING-only results never gate.
    monkeypatch.setattr(
        bronze_dq, 'run_bronze_dq', lambda *a, **k: [warning_only],
    )
    payload = mod._validate_bronze_quality(ti=_manual_ti(mod))
    assert payload['errors'] == []
    assert payload['warnings'] == [warning_only.name]


def test_gate_manual_mode_runs_legacy_zone_only(gate_env, monkeypatch):
    mod = gate_env
    from utils import transfermarkt_bronze_dq as bronze_dq

    captured = {}

    def fake_run(cur, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(bronze_dq, 'run_bronze_dq', fake_run)
    payload = mod._validate_bronze_quality(ti=_manual_ti(mod))
    assert payload['zone'] == 'legacy'
    assert captured['zone'] == 'legacy'
    assert captured.get('manifests') is None
    assert captured['registry_snapshot_id'] is None
    assert captured['pins'] == {'iceberg.bronze.transfermarkt_players': 101}
    assert captured['legacy_allowlist'] == [
        ('ENG-Premier League', '2324'), ('ENG-Premier League', '2425'),
    ]


def test_gate_publishes_coverage_counters_xcom(gate_env, monkeypatch):
    mod = gate_env
    from utils import transfermarkt_bronze_dq as bronze_dq

    coverage_value = {
        'registry_snapshot_id': 'snap-1',
        'target_scopes': 9745,
        'complete_scopes': 2,
        'complete_in_target': 2,
        'coverage_ratio': 2 / 9745,
        'extra_complete': [],
    }
    debt_value = {
        'career_fetches_pending': 4198,
        'null_evidence_manifests': 1,
        'complete_manifests': 2,
    }
    results = [
        bronze_dq.BronzeCheckResult(
            name='tm_target_scope_coverage', kind='target_scope_coverage',
            severity='WARNING', passed=True, value=coverage_value,
        ),
        bronze_dq.BronzeCheckResult(
            name='tm_career_debt', kind='career_debt',
            severity='WARNING', passed=True, value=debt_value,
        ),
    ]
    monkeypatch.setattr(bronze_dq, 'run_bronze_dq', lambda *a, **k: results)
    payload = mod._validate_bronze_quality(ti=_manual_ti(mod))
    assert payload['coverage'] == coverage_value
    assert payload['career_debt'] == debt_value
    assert payload['passed'] == 2
    assert payload['total'] == 2
    assert payload['errors'] == []
