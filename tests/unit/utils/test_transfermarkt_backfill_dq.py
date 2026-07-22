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
