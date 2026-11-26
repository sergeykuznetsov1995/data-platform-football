"""The append shortcut must produce the exact old/new row-bag difference."""
from collections import Counter

import pytest

from scrapers.fotmob import c1_inputs as c1


pytestmark = pytest.mark.unit


class AppendManager:
    def __init__(self):
        self.snapshots = [(7, 10, "append", 1), (10, 902, "append", 1),
                          (902, 4, "overwrite", None)]
        self.files = [(7, "s3://fixture/a'b", 1), (10, "s3://fixture/c", 2)]
        self.rows = [("s3://fixture/a'b", "1", "2025", "ignored", "a"),
                     ("s3://fixture/c", None, None, "match", "b"),
                     ("s3://fixture/c", None, None, "match", "b")]
        self.calls = []

    def execute_query(self, sql):
        self.calls.append(sql)
        if '$snapshots"' in sql:
            return iter(self.snapshots)
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


@pytest.mark.parametrize("case", [
    "overwrite", "replace", "delete", "not_ancestor", "expired", "no_summary",
    "missing_file", "duplicate_file", "null_path", "wrong_row_count", "missing_data",
    "unexpected_path", "file_limit", "row_limit",
])
def test_unproven_append_uses_exact_signed_difference_before_yield(monkeypatch, case):
    manager = AppendManager()
    if case in {"overwrite", "replace", "delete"}:
        manager.snapshots[1] = (10, 902, case, 1)
    elif case == "not_ancestor":
        manager.snapshots[1] = (10, 1, "append", 1)
    elif case == "expired":
        manager.snapshots.pop()
    elif case == "no_summary":
        manager.snapshots[1] = (10, 902, "append", None)
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
