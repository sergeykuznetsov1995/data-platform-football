"""Execute the metadata selector offline, including its window/partition bounds."""

from collections import Counter
from datetime import datetime, timezone
import json

import duckdb
import pytest
import sqlglot

from utils import maintenance_tasks as maintenance
from utils import bronze_compaction as compaction
from utils import bronze_compaction_direct as direct


class Metadata:
    def __init__(self, partitions=64, files=32, size=10, partitioned=True):
        self.db = duckdb.connect()
        self.sql = []
        self.partitioned = partitioned
        self.db.execute('CREATE TABLE files (partition VARCHAR, file_path VARCHAR, '
                        'file_size_in_bytes BIGINT, content INTEGER)')
        self.db.executemany('INSERT INTO files VALUES (?, ?, ?, ?)', [
            (f'p{p:03}', f's3://p{p:03}/{f:04}', size, 0)
            for p in range(partitions) for f in range(files)
        ])

    def cursor(self):
        owner = self

        class Cursor:
            def execute(self, sql):
                owner.sql.append(sql)
                if sql.startswith("SET SESSION"):
                    return
                if not owner.partitioned and 'CAST(partition AS JSON)' in sql:
                    raise RuntimeError("COLUMN_NOT_FOUND: 'partition'")
                sql = sql.replace('iceberg."bronze"."espn_match$files"', 'files')
                # Trino ROW->JSON is a JSON array/string key; use a string key
                # here, preserving comparison, grouping and the query itself.
                sql = sql.replace('json_format(CAST(partition AS JSON))', 'partition')
                owner.db.execute(sqlglot.transpile(sql, read='trino', write='duckdb')[0])

            def fetchall(self):
                return owner.db.fetchall()

            def close(self):
                pass

        return Cursor()

    def close(self):
        pass

    def probe(self, **kwargs):
        return maintenance._compaction_candidates(
            self, schema='bronze', table='espn_match', max_input_bytes=kwargs.pop('budget', 512 * 1024**2),
            max_files=kwargs.pop('max_files', 1024), multi_partition=True, **kwargs,
        )


def test_many_small_partitions_fill_one_portion(monkeypatch):
    metadata = Metadata()
    now = datetime(2026, 10, 29, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(compaction.time, 'time', lambda: now.timestamp())
    monkeypatch.setattr(compaction, 'nightly_deadline', lambda: now.replace(hour=5))
    monkeypatch.setattr(maintenance, '_list_tables', lambda *a: ['espn_match'])
    result = compaction.run_compaction(
        tables=['espn_match'], deadline=now.replace(hour=5),
        connection_factory=lambda: metadata, dry_run=True,
    )
    assert not result['failures']
    assert result['selected_files'] == 1024
    probe = metadata.probe()
    assert len(probe.candidates) == 1024
    assert len({path.split('/')[2] for path, _ in probe.candidates}) == 32
    assert probe.next_partition_cursor == 'p031'
    assert 'LIMIT 1024' in metadata.sql[-1]
    statements = []
    class Connection:
        def cursor(self):
            class Cursor:
                description = []
                def execute(self, sql):
                    statements.append(sql)
                def fetchall(self):
                    return []
                def close(self):
                    pass
            return Cursor()
    maintenance._compact_exact_files(Connection(), schema='bronze', table='espn_match',
                                     candidates=probe.candidates, max_files=1024)
    assert 'WHERE "$path" IN (' in statements[-1]
    assert statements[-1].count("'s3://") == 1024


@pytest.mark.parametrize('limits', [dict(max_files=5), dict(budget=50)])
def test_bounds_remove_partition_singletons(limits):
    metadata = Metadata(partitions=3, files=2)
    probe = metadata.probe(**limits)
    assert len(probe.candidates) == 4
    assert set(Counter(path.split('/')[2] for path, _ in probe.candidates).values()) == {2}
    # Even the visited singleton advances the traversal to avoid starvation.
    assert probe.next_partition_cursor == 'p002'


def test_cursor_wraps_after_disappearing_partitions_and_replenishment():
    metadata = Metadata()
    first = metadata.probe()
    metadata.db.execute("DELETE FROM files WHERE partition <= 'p031'")
    metadata.db.execute("INSERT INTO files VALUES ('p000', 's3://new/a', 10, 0), "
                        "('p000', 's3://new/b', 10, 0)")
    second = metadata.probe(partition_cursor=first.next_partition_cursor)
    assert len(second.candidates) == 1024
    assert second.next_partition_cursor == 'p063'
    metadata.db.execute("DELETE FROM files WHERE partition >= 'p032'")
    assert metadata.probe(partition_cursor=second.next_partition_cursor).next_partition_cursor == 'p000'


def test_large_partition_does_not_hold_up_later_partitions():
    metadata = Metadata(partitions=2, files=1100)
    first = metadata.probe()
    assert first.next_partition_cursor == 'p000'
    second = metadata.probe(partition_cursor=first.next_partition_cursor)
    assert second.next_partition_cursor == 'p001'
    assert all('/p001/' in path for path, _ in second.candidates)


@pytest.mark.parametrize('content', [1, 2])
def test_delete_partitions_are_excluded(content):
    metadata = Metadata(partitions=2, files=2)
    metadata.db.execute("INSERT INTO files VALUES ('p000', 's3://delete', 100, ?)", [content])
    probe = metadata.probe()
    assert len(probe.candidates) == 2
    assert probe.skipped_delete_partitions == 1
    assert all('/p001/' in path for path, _ in probe.candidates)


def test_unpartitioned_and_empty_tables():
    metadata = Metadata(partitions=1, files=4, partitioned=False)
    probe = metadata.probe()
    assert len(probe.candidates) == 4
    assert probe.next_partition_cursor == '[]'
    metadata.db.execute('DELETE FROM files')
    assert not metadata.probe().candidates


def test_direct_legacy_state_migrates_without_refunding_budget(tmp_path):
    path = tmp_path / 'state'
    old = dict(version=1, tables=['espn_match'], window='2026-10-29',
               portions=7, input_bytes=1234, queue=['espn_match'],
               partition_offsets={'espn_match': 42})
    path.write_text(json.dumps(old))
    state = direct._load(path, ['espn_match'], '2026-10-29')
    assert state == dict(version=2, tables=['espn_match'], window='2026-10-29',
                         portions=7, input_bytes=1234, queue=['espn_match'], partition_cursors={})
    assert json.loads(path.read_text()) == old
    state['partition_cursors'] = {'espn_match': 'p031'}
    direct._save(path, state)
    assert direct._load(path, ['espn_match'], '2026-10-30')['partition_cursors'] == {'espn_match': 'p031'}


def test_python_rejects_singleton_partitions(monkeypatch):
    monkeypatch.setattr(maintenance, '_run_multi_partition_probe', lambda *a, **kw: [
        ('s3://a', 10, 0, 'p1', 'p2'), ('s3://b', 10, 0, 'p2', 'p2')])
    with pytest.raises(RuntimeError, match='partition selection'):
        Metadata().probe()
