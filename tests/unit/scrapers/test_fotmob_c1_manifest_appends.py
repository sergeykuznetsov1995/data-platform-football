"""The append shortcut must produce the exact old/new row-bag difference."""
from collections import Counter

import pytest

from scrapers.fotmob import c1_inputs as c1


pytestmark = pytest.mark.unit


def snapshot(sid, parent, operation, added, deleted=None, added_rows=None, deleted_rows=None):
    return sid, parent, operation, added, deleted, added_rows, deleted_rows


class AppendManager:
    def __init__(self):
        self.snapshots = [snapshot(7, 10, "append", 1), snapshot(10, 902, "append", 1),
                          snapshot(902, 4, "overwrite", None)]
        self.schema_rows = []
        self.files = [(7, "s3://fixture/a'b", 1), (10, "s3://fixture/c", 2)]
        self.rows = [("s3://fixture/a'b", "1", "2025", "ignored", "a"),
                     ("s3://fixture/c", None, None, "match", "b"),
                     ("s3://fixture/c", None, None, "match", "b")]
        self.calls = []

    def execute_query(self, sql):
        self.calls.append(sql)
        if '$snapshots"' in sql:
            for field in ("added-data-files", "deleted-data-files", "added-records", "deleted-records"):
                assert f"'{field}'" in sql
            return iter(self.snapshots)
        if '$metadata_log_entries"' in sql:
            assert "latest_schema_id" in sql
            return iter(self.schema_rows)
        if '$files"' in sql:
            assert "content = 0" in sql
            assert "added_snapshot_id IN (7, 10)" in sql
            return iter(self.files)
        if 'SELECT "$path",' in sql:
            assert 'FOR VERSION AS OF 7' in sql
            assert "\"$path\" IN ('s3://fixture/a''b', 's3://fixture/c')" in sql
            return iter(self.rows)
        raise AssertionError("unexpected query: " + sql)


def test_append_reads_only_proven_files_and_preserves_duplicates():
    manager = AppendManager()
    result = list(c1.manifest_changes(manager, 902, 7))
    assert Counter(result) == Counter({(None, None, "match", "b"): 2})
    assert len(manager.calls) == 3


def compaction_followed_by_appends():
    manager = AppendManager()
    manager.snapshots[1:2] = [
        snapshot(10, 81, "append", 1),
        snapshot(81, 33, "replace", 4, 69, 39918, 39918),
        snapshot(33, 902, "replace", 7, 995, 189947, 189947),
    ]
    manager.schema_rows = [(sid, 1) for sid in (902, 33, 81, 7)]
    return manager


def test_compaction_prefix_keeps_only_exact_append_rows():
    manager = compaction_followed_by_appends()
    changes = list(c1.manifest_changes(manager, 902, 7))
    assert Counter(changes) == Counter({(None, None, "match", "b"): 2})
    assert len(manager.calls) == 4
    schema_query = next(sql for sql in manager.calls if '$metadata_log_entries"' in sql)
    assert "IN (7, 33, 81, 902)" in schema_query


def test_only_proven_compactions_have_an_empty_logical_difference():
    manager = compaction_followed_by_appends()
    manager.schema_rows = [(sid, 1) for sid in (902, 33, 81)]
    assert list(c1.manifest_changes(manager, 902, 81)) == []
    assert len(manager.calls) == 2


@pytest.mark.parametrize("case", [
    "missing_schema", "schema_change", "conflicting_schema", "missing_schema_id",
    "missing_rows", "unequal_rows", "missing_removed_files", "wrong_operation",
    "append_before_replace", "cycle",
])
def test_incomplete_compaction_proof_uses_the_existing_exact_comparison(monkeypatch, case):
    manager = compaction_followed_by_appends()
    if case == "missing_schema":
        manager.schema_rows.pop(1)
    elif case == "schema_change":
        manager.schema_rows[1] = (33, 2)
    elif case == "conflicting_schema":
        manager.schema_rows.append((33, 2))
    elif case == "missing_schema_id":
        manager.schema_rows[1] = (33, None)
    elif case == "missing_rows":
        manager.snapshots[2] = snapshot(81, 33, "replace", 4, 69, None, None)
    elif case == "unequal_rows":
        manager.snapshots[2] = snapshot(81, 33, "replace", 4, 69, 39918, 39917)
    elif case == "missing_removed_files":
        manager.snapshots[2] = snapshot(81, 33, "replace", 4, None, 39918, 39918)
    elif case == "wrong_operation":
        manager.snapshots[2] = snapshot(81, 33, "overwrite", 4, 69, 39918, 39918)
    elif case == "append_before_replace":
        manager.snapshots[3] = snapshot(33, 902, "append", 1)
    else:
        manager.snapshots[3] = snapshot(33, 81, "replace", 7, 995, 189947, 189947)
    def exact_comparison(_manager, old, new):
        assert (old, new) == (902, 7)
        yield ("1", "2025", "match", "from-comparison")

    monkeypatch.setattr(c1, "_compare_manifest_buckets", exact_comparison)
    assert list(c1.manifest_changes(manager, 902, 7)) == [("1", "2025", "match", "from-comparison")]


def test_compaction_schema_read_failure_stops_before_file_reads():
    manager = compaction_followed_by_appends()
    execute = manager.execute_query

    def fail_schema_read(sql):
        if '$metadata_log_entries"' in sql:
            raise RuntimeError("snapshot metadata unavailable")
        return execute(sql)

    manager.execute_query = fail_schema_read
    with pytest.raises(RuntimeError, match="snapshot metadata unavailable"):
        next(c1.manifest_changes(manager, 902, 7))
    assert len(manager.calls) == 1


@pytest.mark.parametrize("case", [
    "overwrite", "replace", "delete", "not_ancestor", "expired", "no_summary",
    "missing_file", "duplicate_file", "null_path", "wrong_row_count", "missing_data",
    "unexpected_path", "file_limit", "row_limit",
])
def test_unproven_append_uses_exact_signed_difference_before_yield(monkeypatch, case):
    manager = AppendManager()
    if case in {"overwrite", "replace", "delete"}:
        manager.snapshots[1] = snapshot(10, 902, case, 1)
    elif case == "not_ancestor":
        manager.snapshots[1] = snapshot(10, 1, "append", 1)
    elif case == "expired":
        manager.snapshots.pop()
    elif case == "no_summary":
        manager.snapshots[1] = snapshot(10, 902, "append", None)
    elif case == "missing_file":
        manager.files.pop()
    elif case == "duplicate_file":
        manager.files[1] = (10, manager.files[0][1], 2)
    elif case == "null_path":
        manager.files[1] = (10, None, 2)
    elif case == "wrong_row_count":
        manager.files[1] = (10, manager.files[1][1], 1)
    elif case == "missing_data":
        manager.rows.clear()
    elif case == "file_limit":
        monkeypatch.setattr(c1, "MAX_APPEND_FILES", 1)
    elif case == "row_limit":
        monkeypatch.setattr(c1, "MAX_APPEND_ROWS", 2)
    else:
        manager.rows[0] = ("s3://fixture/surprise", *manager.rows[0][1:])
    sentinel = ("2", "2024", "match", "from-comparison")
    monkeypatch.setattr(c1, "_compare_manifest_buckets", lambda m, old, new: iter([sentinel]))
    assert list(c1.manifest_changes(manager, 902, 7)) == [sentinel]


def test_partial_read_never_yields_changes():
    manager = AppendManager()

    def broken_rows():
        yield manager.rows[1]
        raise RuntimeError("read failed")

    source = broken_rows()
    original = manager.execute_query
    manager.execute_query = lambda sql: source if 'SELECT "$path",' in sql else original(sql)
    with pytest.raises(RuntimeError, match="read failed"):
        next(c1.manifest_changes(manager, 902, 7))
