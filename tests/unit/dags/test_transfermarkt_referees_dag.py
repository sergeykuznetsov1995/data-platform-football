from __future__ import annotations

from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from dags import dag_backfill_transfermarkt_referees as history
from dags import dag_ingest_transfermarkt_referees as current
from dags import dag_dq_transfermarkt_referees as daily

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def green_physical_dq(monkeypatch):
    from dags.utils import transfermarkt_native_v2 as native
    from dags.utils import transfermarkt_referees as writer

    monkeypatch.setattr(native, 'connect', lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(
        writer, 'run_referee_physical_dq',
        lambda connection, *, run_id=None: {'completed': True, 'status': 'GREEN', 'run_id': run_id},
    )


def task(module, task_id):
    return getattr(module, task_id)


def test_current_dag_is_paused_weekly_and_serial_with_nonmatch_runner():
    assert current.dag.dag_id == 'dag_ingest_transfermarkt_referees'
    assert current.dag._dag_kwargs['schedule'] == '0 6 * * 0'
    assert current.dag._dag_kwargs['is_paused_upon_creation'] is True
    assert current.dag._dag_kwargs['max_active_runs'] == 1
    assert (current.plan.task_id, current.capture.task_id, current.validate.task_id) == (
        'plan_referee_sweep', 'capture_and_write_referees', 'validate_referee_coverage',
    )
    command = current.capture.bash_command
    assert 'run_transfermarkt_referees.py' in command and '--mode current' in command
    assert 'spielbericht' not in command and 'match' not in command


def test_history_dag_is_manual_paused_and_separate(tmp_path):
    assert history.dag.dag_id == 'dag_backfill_transfermarkt_referees'
    assert history.dag._dag_kwargs['schedule'] is None
    assert history.dag._dag_kwargs['is_paused_upon_creation'] is True
    assert history.dag._dag_kwargs['default_args']['pool'] == 'transfermarkt_backfill_control'
    assert history.capture._init_kwargs['pool'] == 'transfermarkt_backfill_proxy'
    command = history.capture.bash_command
    assert '--mode history' in command and 'spielbericht' not in command


def test_current_plan_preserves_exact_run_scope_and_policy_environment():
    env, = current._plan_current_referees(
        run_id='scheduled__referee-test',
        params={'scopes': ['TM-XY1-2025'], 'max_scopes': 1, 'max_seconds': 60},
    )
    assert env['TM_RUN_ID'] == 'scheduled__referee-test'
    assert env['TM_DAG_ID'] == 'dag_ingest_transfermarkt_referees'
    assert json.loads(env['TM_REFEREE_SCOPES_JSON']) == ['TM-XY1-2025']
    assert env['TM_REFEREE_MAX_SCOPES'] == '1'
    assert env['TM_STANDING_POLICY_ENABLED'] == 'true'
    assert env['TM_REFEREE_STATE_DIR'] == current.STATE_DIR


def test_history_plan_uses_native_only_and_history_identity():
    env, = history._plan_referee_history(
        run_id='manual__history-test', params={'scopes': [], 'max_scopes': 100, 'max_seconds': 3600},
    )
    assert env['TM_DAG_ID'] == 'dag_backfill_transfermarkt_referees'
    assert env['TM_WRITE_MODE'] == 'native-only'
    assert env['TM_RUN_ID'] == 'manual__history-test'


def test_current_validation_rejects_excess_unresolved_and_wrong_run(tmp_path, monkeypatch):
    report = {
        'run_id': 'run-good', 'status': 'complete', 'unresolved_ratio': 0.0,
    }
    path = tmp_path / 'current-last-report.json'
    path.write_text(json.dumps(report))
    monkeypatch.setattr(current, 'STATE_DIR', str(tmp_path))
    validated = current._validate_current_referees(run_id='run-good')
    assert validated['status'] == 'complete'
    assert validated['physical_dq']['status'] == 'GREEN'
    with pytest.raises(Exception, match='another run'):
        current._validate_current_referees(run_id='run-other')
    report['unresolved_ratio'] = 0.06
    path.write_text(json.dumps(report))
    with pytest.raises(Exception, match='5%'):
        current._validate_current_referees(run_id='run-good')
    report.update(status='error', unresolved_ratio=0.05)
    path.write_text(json.dumps(report))
    assert current._validate_current_referees(run_id='run-good')['physical_dq']['status'] == 'GREEN'


def test_history_validation_reads_only_matching_run_report(tmp_path, monkeypatch):
    monkeypatch.setattr(history, 'STATE_DIR', str(tmp_path))
    report = {'run_id': 'history-good', 'status': 'complete', 'history_coverage': 1.0}
    (tmp_path / 'history-last-report.json').write_text(json.dumps(report))
    validated = history._validate_referee_history(run_id='history-good')
    assert validated['history_coverage'] == 1.0
    assert validated['physical_dq']['status'] == 'GREEN'
    report['status'] = 'error'
    (tmp_path / 'history-last-report.json').write_text(json.dumps(report))
    with pytest.raises(Exception, match='unresolved'):
        history._validate_referee_history(run_id='history-good')


def test_daily_dq_is_paused_and_persists_green_report(tmp_path, monkeypatch):
    assert daily.dag.dag_id == 'dag_dq_transfermarkt_referees'
    assert daily.dag._dag_kwargs['schedule'] == '0 1 * * *'
    assert daily.dag._dag_kwargs['is_paused_upon_creation'] is True
    monkeypatch.setattr(daily, 'STATE_DIR', tmp_path)
    logical = datetime(2026, 11, 21, tzinfo=timezone.utc)
    report = daily.run_daily_referee_dq(run_id='scheduled__dq-test', logical_date=logical)
    saved = json.loads((tmp_path / '2026-11-21.json').read_text())
    assert report['status'] == saved['status'] == 'GREEN'
    assert saved['day'] == '2026-11-21' and saved['dag_run_id'] == 'scheduled__dq-test'


def test_daily_dq_red_is_persisted_and_fails(tmp_path, monkeypatch):
    from dags.utils import transfermarkt_referees as writer
    monkeypatch.setattr(daily, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(
        writer, 'run_referee_physical_dq',
        lambda connection: {'completed': True, 'status': 'RED', 'errors': ['duplicate_natural_key']},
    )
    with pytest.raises(Exception, match='not GREEN'):
        daily.run_daily_referee_dq(run_id='scheduled__dq-red', logical_date=datetime(2026, 11, 21))
    assert json.loads((tmp_path / '2026-11-21.json').read_text())['status'] == 'RED'
