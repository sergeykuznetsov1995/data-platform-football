"""Real TERM at final flush with a durable fake catalog, then a fresh retry."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scrapers.fotmob.repository import FotMobRepository
from tests.unit.scrapers.test_fotmob_repository import ReconcileWriter, _commit, _match_dataset


WORKER = r'''
import json, os, signal, sys
from pathlib import Path
from types import SimpleNamespace
from dags.scripts import run_fotmob_scraper as runner
from scrapers.fotmob.repository import FotMobRepository
from scrapers.fotmob.service import OperationResult, RunReport
from tests.unit.scrapers.test_fotmob_repository import ReconcileWriter, _commit, _match_dataset

state = Path(sys.argv[1])
point = sys.argv[2]
writer = ReconcileWriter()
writer.rows['fotmob_ingest_manifest'] = [_commit(run_id='prior', target_key='other-target').manifest_row()]
sent = False
views = []
original_query = writer.trino.execute_query
original_write = writer.write_dataframe

def terminate():
    global sent
    if not sent:
        sent = True
        os.kill(os.getpid(), signal.SIGTERM)

def query(sql):
    if point == 'manifest_reconcile' and 'SELECT run_id, batch_id' in sql:
        terminate()
    return original_query(sql)

def write(df, **kwargs):
    path = original_write(df, **kwargs)
    if kwargs['table'] == 'fotmob_matches' and point == 'physical_response':
        terminate()
    return path

writer.trino.execute_query = query
writer.write_dataframe = write
repository = FotMobRepository(writer=writer, batch_size=50)
repository.commit(_commit(), [_match_dataset('1')])
repository.missing_current_squad_player_ids = lambda limit: []
repository.ensure_current_views = lambda: views.append('view update') or []
service = SimpleNamespace(
    repository=repository,
    cancel=lambda: None,
    sync_player_snapshots=lambda *a, **kw: OperationResult('player_snapshots', metadata={'terminal_outcomes': []}),
    report=lambda operations, started: RunReport('run-1', 'daily', started, operations=list(operations)),
)
args = runner._argument_parser().parse_args(['--mode', runner.PLAYER_COLLECTOR_MODE, '--run-id', 'run-1'])
original_run = runner._run_native
runner._run_native = lambda args: original_run(args, service=service)
previous = signal.signal(signal.SIGTERM, runner._sigterm_to_exception)
try:
    rc, report = runner._run_native_unfenced(args)
    # A second TERM cannot interrupt the diagnostic/report cleanup.
    os.kill(os.getpid(), signal.SIGTERM)
finally:
    signal.signal(signal.SIGTERM, previous)
state.write_text(json.dumps({'rc': rc, 'report': report, 'rows': writer.rows, 'views': views}, default=str))
'''


@pytest.mark.parametrize('point', ['physical_response', 'manifest_reconcile'])
def test_term_during_final_flush_records_interruption_and_fresh_attempt_passes(tmp_path, point):
    state = tmp_path / 'catalog.json'
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
    root = Path(__file__).resolve().parents[3]
    environment['PYTHONPATH'] = os.pathsep.join((str(root), str(root / 'dags')))
    result = subprocess.run(
        [sys.executable, '-c', WORKER, str(state), point],
        cwd=root, env=environment, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr
    observed = json.loads(state.read_text())
    assert observed['rc'] == 1
    assert observed['report']['complete'] is False
    assert observed['views'] == []
    assert [row['status'] for row in observed['rows']['fotmob_ingest_manifest'] if row['run_id'] == 'run-1'] == ['interrupted']

    writer = ReconcileWriter()
    writer.rows = observed['rows']
    repository = FotMobRepository(writer=writer, batch_size=50)
    repository.commit(_commit(run_id='next-attempt'), [_match_dataset('1')])
    repository.flush()
    assert len(writer.rows['fotmob_matches']) == 1
    assert [row['status'] for row in writer.rows['fotmob_ingest_manifest'] if row['run_id'] != 'prior'] == ['interrupted', 'success']
