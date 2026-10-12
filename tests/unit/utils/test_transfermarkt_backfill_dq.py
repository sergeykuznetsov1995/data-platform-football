from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from dags.utils import transfermarkt_backfill_dq as dq
from dags.utils import transfermarkt_bronze_dq as bronze_dq


class _Cursor:
    def __init__(self, *, lineage_rows=()):
        self.sql = []
        self._rows = []
        self.lineage_rows = list(lineage_rows)

    def execute(self, sql):
        self.sql.append(sql)
        if '$snapshots' in sql:
            self._rows = [(42,)]
        elif 'SELECT DISTINCT raw_capture_id' in sql:
            self._rows = list(self.lineage_rows) if not any(
                'SELECT DISTINCT raw_capture_id' in item
                for item in self.sql[:-1]
            ) else []
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)


class _RawStore:
    def __init__(self, capture_id, body):
        self.capture_id = capture_id
        self.body = body

    def load_capture(self, capture_id):
        assert capture_id == self.capture_id
        digest = hashlib.sha256(self.body).hexdigest()
        return self.body, SimpleNamespace(
            capture_id=capture_id,
            content_hash=digest,
        )


def test_snapshot_pin_is_exact_and_complete():
    cur = _Cursor()

    pins = dq.pin_iceberg_snapshots(cur)

    assert pins == {table: 42 for table in dq.BACKFILL_PIN_TABLES}
    assert len(cur.sql) == len(dq.BACKFILL_PIN_TABLES)
    assert all('$snapshots' in sql for sql in cur.sql)


def test_lineage_query_is_snapshot_and_child_cycle_pinned():
    table = dq.BACKFILL_ENTITY_TABLES[0]

    sql = dq.build_raw_lineage_sql(
        table,
        snapshot_id=71,
        child_cycle_ids=('tm-child-b', "tm-child-'a"),
    )

    assert f'{table} FOR VERSION AS OF 71' in sql
    assert "tm-child-''a" in sql
    assert 'tm-child-b' in sql


@pytest.mark.parametrize('scope_status', ('retryable_error', 'terminal_error'))
def test_partial_raw_lineage_has_exact_inventory_for_closing_states(
    scope_status,
):
    body = b'<html>raw</html>'
    capture_id = 'a' * 64
    body_hash = hashlib.sha256(body).hexdigest()
    cur = _Cursor(lineage_rows=[(
        capture_id, body_hash, 'GB1__2024', 'tm-child-one',
    )])

    result = dq.verify_raw_lineage(
        cur,
        pins={table: 42 for table in dq.BACKFILL_ENTITY_TABLES},
        child_cycle_ids=('tm-child-one',),
        raw_store=_RawStore(capture_id, body),
        attempt_envelopes=[SimpleNamespace(
            outcome_kind='response', capture_id=capture_id,
            scope_id='GB1__2024', cycle_id='tm-child-one',
        )],
        manifest_scope_cycles=(),
        scope_statuses={'GB1__2024': scope_status},
    )

    assert result['capture_count'] == 1
    assert len(result['capture_set_hash']) == 64
    assert result['partial_capture_count'] == 1
    assert result['partial_capture_inventory'] == [{
        'scope_id': 'GB1__2024',
        'child_cycle_id': 'tm-child-one',
        'table': dq.BACKFILL_ENTITY_TABLES[0],
        'scope_status': scope_status,
        'capture_count': 1,
        'capture_set_hash': dq.stable_hash([capture_id]),
    }]


def test_raw_lineage_hash_drift_fails_closed():
    capture_id = 'a' * 64
    cur = _Cursor(lineage_rows=[(
        capture_id, 'b' * 64, 'GB1__2024', 'tm-child-one',
    )])

    with pytest.raises(dq.BackfillDqError, match='differs from Bronze'):
        dq.verify_raw_lineage(
            cur,
            pins={table: 42 for table in dq.BACKFILL_ENTITY_TABLES},
            child_cycle_ids=('tm-child-one',),
            raw_store=_RawStore(capture_id, b'actual'),
            attempt_envelopes=[SimpleNamespace(
                outcome_kind='response', capture_id=capture_id,
                scope_id='GB1__2024', cycle_id='tm-child-one',
            )],
            manifest_scope_cycles=(),
            scope_statuses={'GB1__2024': 'terminal_error'},
        )


def test_batch_report_preserves_errors_and_fails_gate(monkeypatch):
    monkeypatch.setattr(
        dq.bronze_dq,
        'run_bronze_dq',
        lambda *args, **kwargs: [bronze_dq.BronzeCheckResult(
            name='broken', kind='lineage', severity='ERROR', passed=False,
            details='bad lineage',
        )],
    )
    monkeypatch.setattr(
        dq,
        'verify_raw_lineage',
        lambda *args, **kwargs: {
            'capture_count': 1,
            'capture_set_hash': 'c' * 64,
            'rows_by_table': {},
        },
    )
    pins = {table: 42 for table in dq.BACKFILL_PIN_TABLES}

    report = dq.run_backfill_batch_dq(
        _Cursor(),
        campaign_id='campaign',
        batch_id='batch',
        registry_snapshot_id='registry',
        manifests=[],
        child_cycle_ids=('child',),
        scope_bindings=(('child', 'scope', 'GB1', '2020'),),
        raw_store=object(),
        attempt_envelopes=(),
        scope_statuses={'scope': 'terminal_error'},
        pins=pins,
    )

    assert report.passed is False
    assert report.bronze_checks[0]['kind'] == 'lineage'
    assert report.as_dict()['report_hash'] == report.report_hash


def test_history_dq_reads_original_career_snapshots_after_current_replacement():
    old = ['1', '2020-01-01', 100, 'Old club', 20, 'EUR100']
    new = ['2', '2020-01-01', 200, 'Other club', 21, 'EUR200']
    receipts = []
    for snapshot, row in ((10, old), (20, new)):
        receipts.append({'snapshot_id': snapshot, 'player_ids': [row[0]],
            'physical_refs': [[row[0], f'original-{snapshot}', 1]],
            'row_count': 1, 'key_hash': dq._fingerprint_rows([row])[1],
            'scope_id': 'historical-scope', 'cycle_id': 'historical-cycle',
            'result_sha256': 'a' * 64})
    class Cursor:
        def __init__(self):
            self.sql = []
        def execute(self, sql):
            self.sql.append(sql)
        def fetchall(self):
            if 'FOR VERSION AS OF 10 ' in self.sql[-1]:
                return [old]
            if 'FOR VERSION AS OF 20 ' in self.sql[-1]:
                return [new]
            # Current replacement changed player1 and removed its old cycle.
            return [['1', '2026-10-10', 999, 'Current club', 26, 'EUR999']]
    manifest = SimpleNamespace(scope_id='historical-scope', child_cycle_id='historical-cycle',
        entities=[SimpleNamespace(entity='market_value_points', dedup_rows=2,
                  key_hash=dq._fingerprint_rows([old, new])[1])],
        dq_evidence={'historical_career_receipts': {'market_value_points': receipts}})
    cur = Cursor()
    result = dq.verify_manifest_entity_fingerprints(cur,
        pins={table: 99 for table in dq.BACKFILL_PIN_TABLES}, manifests=[manifest])
    assert result['row_count'] == 2
    assert len(cur.sql) == 2
    assert all('FOR VERSION AS OF 99' not in sql for sql in cur.sql)
    assert "_batch_id = 'original-10'" in cur.sql[0]
    assert 'cycle_id =' not in cur.sql[0]


def test_history_original_snapshot_raw_lineage_is_checked_after_current_replacement():
    body = b'original full career'
    capture_id = 'b' * 64
    body_hash = hashlib.sha256(body).hexdigest()
    receipt = {'snapshot_id': 10, 'player_ids': ['1'], 'row_count': 1,
        'physical_refs': [['1', 'original-10', 1]],
        'key_hash': 'a' * 64, 'scope_id': 'historical-scope',
        'cycle_id': 'historical-cycle', 'result_sha256': 'c' * 64}
    class Cursor:
        def __init__(self):
            self.sql = []
        def execute(self, sql):
            self.sql.append(sql)
        def fetchall(self):
            return [(capture_id, body_hash, 'historical-scope', 'historical-cycle', '1', 'original-10')] if 'FOR VERSION AS OF 10 ' in self.sql[-1] else []
    cur = Cursor()
    result = dq.verify_raw_lineage(cur,
        pins={table: 99 for table in dq.BACKFILL_PIN_TABLES},
        child_cycle_ids=['historical-cycle'], raw_store=_RawStore(capture_id, body),
        attempt_envelopes=[SimpleNamespace(outcome_kind='response', capture_id=capture_id,
             scope_id='historical-scope', cycle_id='historical-cycle')],
        manifest_scope_cycles=[('historical-scope', 'historical-cycle')],
        scope_statuses={'historical-scope': 'complete'},
        manifests=[SimpleNamespace(dq_evidence={'historical_career_receipts': {'market_value_points': [receipt]}})])
    assert result['capture_count'] == 1
    assert any('FOR VERSION AS OF 10 ' in sql for sql in cur.sql)
