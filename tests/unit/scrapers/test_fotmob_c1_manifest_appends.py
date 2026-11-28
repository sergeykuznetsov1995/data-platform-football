"""The append shortcut must produce the exact old/new row-bag difference."""
from collections import Counter
from contextlib import nullcontext
import json
import re
from types import SimpleNamespace

import pytest
import requests

from scrapers.fotmob import c1_inputs as c1


pytestmark = pytest.mark.unit


def snapshot(sid, parent, operation, added, deleted=None, added_rows=None, deleted_rows=None):
    return sid, parent, operation, added, deleted, added_rows, deleted_rows


class AppendManager:
    def __init__(self):
        self.snapshots = [snapshot(7, 10, "append", 1), snapshot(10, 902, "append", 1),
                          snapshot(902, 4, "overwrite", None)]
        self.schema_rows = []
        self.metadata_files = []
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
            if sql.startswith("SELECT file "):
                return [(path,) for path in self.metadata_files]
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


@pytest.mark.parametrize("other_kind", ["transfers_page", None])
def test_compaction_prefix_keeps_only_exact_append_rows(other_kind):
    manager = compaction_followed_by_appends()
    path, competition, season, _, entity = manager.rows[0]
    manager.rows[0] = (path, competition, season, other_kind, entity)
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


@pytest.mark.parametrize("empty", [False, True])
def test_rewrite_without_a_file_count_in_its_summary_is_still_a_proven_compaction(empty):
    manager = compaction_followed_by_appends()
    # Only appends need their file inventory; a rewrite proves itself by rows.
    manager.snapshots[2] = snapshot(81, 33, "replace", None, 69, 0 if empty else 39918,
                                    0 if empty else 39918)
    manager.schema_rows = [(sid, 1) for sid in (902, 33, 81, 7)]

    assert Counter(c1.manifest_changes(manager, 902, 7)) == Counter({(None, None, "match", "b"): 2})
    assert len(manager.calls) == 4


@pytest.fixture
def rotated_log(monkeypatch):
    manager = compaction_followed_by_appends()
    manager.schema_rows = [(7, 1)]
    manager.metadata_files = ["s3://fixture/metadata/v101.json", "s3://fixture/metadata/v100.json"]
    packed = {"metadata-location": manager.metadata_files[1], "metadata": {
        "schemas": [{"schema-id": 1}],
        "snapshots": [{"snapshot-id": sid, "schema-id": 1} for sid in (902, 33, 81, 7)],
    }}
    monkeypatch.setattr(c1, "_retained_manifest_metadata", lambda: packed)
    return manager, packed


def test_recent_metadata_log_can_omit_still_valid_snapshot_schemas(monkeypatch, rotated_log):
    manager, _ = rotated_log
    monkeypatch.setattr(c1, "_compare_manifest_buckets",
                        lambda *_: pytest.fail("proved compactions should skip wide comparison"))
    assert Counter(c1.manifest_changes(manager, 902, 7)) == Counter({(None, None, "match", "b"): 2})
    assert sum('$metadata_log_entries"' in sql for sql in manager.calls) == 2


@pytest.mark.parametrize("case", [
    "missing_snapshot", "different_rewrite_schema", "duplicate_snapshot", "missing_id",
    "unknown_schema", "different_table", "contradictory_sql", "absent_metadata",
])
def test_unbound_or_incomplete_catalog_schema_uses_exact_comparison(monkeypatch, rotated_log, case):
    manager, doc = rotated_log
    records = doc["metadata"]["snapshots"]
    if case == "missing_snapshot":
        records.pop()
    elif case == "different_rewrite_schema":
        records[1]["schema-id"] = 8
    elif case == "duplicate_snapshot":
        records.append(dict(records[0]))
    elif case == "missing_id":
        del records[0]["schema-id"]
    elif case == "unknown_schema":
        doc["metadata"]["schemas"] = []
    elif case == "different_table":
        doc["metadata-location"] = "s3://other/metadata/v100.json"
    elif case == "contradictory_sql":
        manager.schema_rows = [(7, 2)]
    else:
        doc.clear()
    sentinel = ("1", "2025", "match", "exact")
    monkeypatch.setattr(c1, "_compare_manifest_buckets", lambda *_: iter([sentinel]))
    assert list(c1.manifest_changes(manager, 902, 7)) == [sentinel]
    assert not any('$files"' in sql for sql in manager.calls)


def test_catalog_reads_only_manifest_with_bounded_requests(monkeypatch):
    calls = []
    docs = iter([{"defaults": {"prefix": "old"}, "overrides": {"prefix": "new prefix"}},
                 {"metadata-location": "s3://fixture/metadata/v100.json"}])

    def get(url, *, params, timeout, stream, allow_redirects):
        assert timeout == (3, 8) and stream and not allow_redirects
        calls.append((url, params))
        reply = json.dumps(next(docs)).encode()
        return nullcontext(SimpleNamespace(status_code=200, iter_content=lambda _: [reply]))

    monkeypatch.setenv("ICEBERG_REST_URI", "http://catalog.test/iceberg/")
    monkeypatch.setenv("ICEBERG_REST_WAREHOUSE", "fixture")
    monkeypatch.setattr(requests, "Session", lambda: nullcontext(SimpleNamespace(get=get)))
    assert c1._retained_manifest_metadata() == {"metadata-location": "s3://fixture/metadata/v100.json"}
    assert calls == [
        ("http://catalog.test/iceberg/v1/config", {"warehouse": "fixture"}),
        ("http://catalog.test/iceberg/v1/new%20prefix/namespaces/bronze/tables/fotmob_ingest_manifest",
         {"snapshots": "all"}),
    ]


@pytest.mark.parametrize("case", ["timeout", "redirect", "invalid_json", "large_config", "path_traversal"])
def test_unavailable_catalog_proof_is_bounded_and_read_only(monkeypatch, case):
    calls = []

    def get(url, **options):
        calls.append(url)
        if case == "timeout":
            raise requests.Timeout("fixture")
        status = 302 if case == "redirect" else 200
        bodies = {"invalid_json": b"{", "large_config": b"x" * 65_537,
                  "path_traversal": b'{"overrides":{"prefix":"../other"}}'}
        return nullcontext(SimpleNamespace(status_code=status,
                                           iter_content=lambda _: [bodies.get(case, b"{}")]))

    monkeypatch.setattr(requests, "Session", lambda: nullcontext(SimpleNamespace(get=get)))
    assert c1._retained_manifest_metadata() is None
    assert len(calls) == 1 and calls[0].endswith("/v1/config")


@pytest.mark.parametrize("case", [
    "missing_schema", "schema_change", "conflicting_schema", "missing_schema_id",
    "missing_rows", "unequal_rows", "missing_removed_files", "wrong_operation",
    "cycle",
])
def test_incomplete_compaction_proof_uses_the_existing_exact_comparison(monkeypatch, case):
    monkeypatch.setattr(c1, "_retained_manifest_metadata", lambda: None)
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
    "overwrite", "replace", "delete", "append_with_deletes", "not_ancestor", "expired", "no_summary",
    "missing_file", "duplicate_file", "null_path", "wrong_row_count", "missing_data",
    "unexpected_path",
])
def test_unproven_append_uses_exact_signed_difference_before_yield(monkeypatch, case):
    manager = AppendManager()
    if case in {"overwrite", "replace", "delete"}:
        manager.snapshots[1] = snapshot(10, 902, case, 1)
    elif case == "append_with_deletes":
        manager.snapshots[1] = snapshot(10, 902, "append", 1, 3)
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
    else:
        manager.rows[0] = ("s3://fixture/surprise", *manager.rows[0][1:])
    sentinel = ("2", "2024", "match", "from-comparison")
    monkeypatch.setattr(c1, "_compare_manifest_buckets", lambda m, old, new: iter([sentinel]))
    assert list(c1.manifest_changes(manager, 902, 7)) == [sentinel]


PROJECTION_FIELDS = "competition_id, source_season_key, target_type, entity_id"
PROJECTION = re.compile(
    r"SELECT " + PROJECTION_FIELDS + r" FROM \(SELECT " + PROJECTION_FIELDS
    + r" FROM \(SELECT \* FROM iceberg\.bronze\.fotmob_ingest_manifest FOR VERSION AS OF (\d+) "
    r"WHERE (target_type='[^']+')\) EXCEPT ALL SELECT " + PROJECTION_FIELDS
    + r" FROM \(SELECT \* FROM iceberg\.bronze\.fotmob_ingest_manifest "
    r"FOR VERSION AS OF (\d+) WHERE \2\)\) LIMIT (\d+)"
)


class ProjectionManager:
    """Answer the key-level comparison of two pinned snapshots from an oracle.

    ``versions`` holds whole synthetic snapshots per target type; the expected
    additions are the multiset difference computed independently of the SQL.
    """

    def __init__(self, snapshots, versions):
        self.snapshots = snapshots
        self.versions = versions
        # The SQL metadata log retains exactly the snapshots a rewrite proof needs.
        rewrites = [row[0] for row in snapshots if row[2] == "replace"]
        self.schema_rows = [(sid, 1) for sid in (902, *rewrites, 7)]
        self.calls = []
        self.fail_type = None

    def execute_query(self, sql):
        self.calls.append(sql)
        if '$snapshots"' in sql:
            return iter(self.snapshots)
        if '$metadata_log_entries"' in sql:
            assert "latest_schema_id" in sql
            return iter(self.schema_rows)
        match = PROJECTION.fullmatch(sql)
        assert match, "unexpected query: " + sql
        new, predicate, old, limit = match.groups()
        assert (int(old), int(new)) == (902, 7)
        target_type = predicate.split("'")[1]
        if target_type == self.fail_type:
            raise RuntimeError("synthetic expired snapshot")
        before = Counter(self.versions[902].get(target_type, ()))
        after = Counter(self.versions[7].get(target_type, ()))
        return list((after - before).elements())[: int(limit)]


def interleaved():
    """Appends on both sides of two proven compactions, with an ignored type."""
    manager = ProjectionManager(
        [snapshot(7, 10, "append", 1), snapshot(10, 81, "append", 1),
         snapshot(81, 33, "replace", 4, 69, 39918, 39918),
         snapshot(33, 20, "append", 1), snapshot(20, 902, "replace", 7, 995, 189947, 189947),
         snapshot(902, 4, "overwrite", None)],
        {
            902: {"match": [("1", "2025", "match", "a")] * 2 + [("2", "2025", "match", "gone")],
                  "league_season": [(None, None, "league_season", "x")],
                  "transfer": [("9", "2025", "transfer", "t0")]},
            7: {"match": [("1", "2025", "match", "a")] * 3
                + [("2", "2025", "match", "gone"), (None, "2025", "match", "n")],
               "league_season": [(None, None, "league_season", "x")] * 2
               + [("5", None, "league_season", "y")],
               "competition_profile": [("50", None, "competition_profile", "p")],
               "transfer": [("9", "2025", "transfer", "t0"), ("9", "2025", "transfer", "t1")]},
        },
    )
    return manager, Counter({("1", "2025", "match", "a"): 1, (None, "2025", "match", "n"): 1,
                             (None, None, "league_season", "x"): 1,
                             ("5", None, "league_season", "y"): 1,
                             ("50", None, "competition_profile", "p"): 1})


def refuse_buckets():
    return lambda *_: pytest.fail("an additive history must not run 512 bucket comparisons")


def test_interleaved_compactions_read_exact_additions_in_four_comparisons(monkeypatch):
    manager, expected = interleaved()
    monkeypatch.setattr(c1, "_compare_manifest_buckets", refuse_buckets())

    assert Counter(c1.manifest_changes(manager, 902, 7)) == expected

    comparisons = [sql for sql in manager.calls if sql.startswith("SELECT " + PROJECTION_FIELDS)]
    assert [PROJECTION.fullmatch(sql).group(2) for sql in comparisons] == [
        "target_type='match'", "target_type='league_season'",
        "target_type='competition_profile'", "target_type='competition_seasons'",
    ]
    assert not any('$files"' in sql or 'SELECT "$path",' in sql for sql in manager.calls)
    assert len(manager.calls) == 6


def test_projection_caps_every_type_at_the_overflow_sentinel():
    manager, expected = interleaved()
    original = manager.execute_query
    seen = []

    def execute_query(sql):
        rows = original(sql)
        if match := PROJECTION.fullmatch(sql):
            seen.append((len(rows), int(match.group(4))))
        return rows

    manager.execute_query = execute_query
    assert Counter(c1.manifest_changes(manager, 902, 7)) == expected
    # The cap is a truncation sentinel per type, never a share of the delta:
    # a smaller LIMIT could silently drop rows of a later type.
    assert [count for count, _ in seen] == [2, 2, 1, 0]
    assert {limit for _, limit in seen} == {c1.MAX_PROJECTION_ROWS + 1}


def test_projection_overflow_keeps_the_exact_comparison(monkeypatch):
    manager, _ = interleaved()
    monkeypatch.setattr(c1, "MAX_PROJECTION_ROWS", 3)
    sentinel = ("2", "2024", "match", "from-comparison")
    monkeypatch.setattr(c1, "_compare_manifest_buckets", lambda m, old, new: iter([sentinel]))

    assert list(c1.manifest_changes(manager, 902, 7)) == [sentinel]


@pytest.mark.parametrize("case", ["schema_change", "append_with_deletes", "append_with_deleted_rows",
                                  "rewrite_without_rows_for_its_files", "unbalanced_rewrite",
                                  "delete_between", "overwrite_between", "cycle"])
def test_unproven_interleaved_history_uses_the_exact_comparison(monkeypatch, case):
    manager, _ = interleaved()
    if case == "schema_change":
        manager.schema_rows[1] = (33, 2)
    elif case == "append_with_deletes":
        manager.snapshots[3] = snapshot(33, 20, "append", 1, 2)
    elif case == "append_with_deleted_rows":
        manager.snapshots[3] = snapshot(33, 20, "append", 1, None, 5, 1)
    elif case == "rewrite_without_rows_for_its_files":
        manager.snapshots[2] = snapshot(81, 33, "replace", 0, 69, 39918, 39918)
    elif case == "unbalanced_rewrite":
        manager.snapshots[2] = snapshot(81, 33, "replace", 4, 69, 39918, 39917)
    elif case == "delete_between":
        manager.snapshots[3] = snapshot(33, 20, "delete", None, 2, 0, 5)
    elif case == "overwrite_between":
        manager.snapshots[3] = snapshot(33, 20, "overwrite", 1)
    else:
        manager.snapshots[3] = snapshot(33, 81, "append", 1)
    sentinel = ("2", "2024", "match", "from-comparison")
    monkeypatch.setattr(c1, "_compare_manifest_buckets", lambda m, old, new: iter([sentinel]))

    assert list(c1.manifest_changes(manager, 902, 7)) == [sentinel]
    assert not any(PROJECTION.fullmatch(sql) for sql in manager.calls)


def test_projection_read_failure_propagates_without_wide_fallback(monkeypatch):
    manager, _ = interleaved()
    manager.fail_type = "league_season"
    monkeypatch.setattr(c1, "_compare_manifest_buckets", refuse_buckets())

    with pytest.raises(RuntimeError, match="synthetic expired snapshot"):
        list(c1.manifest_changes(manager, 902, 7))
    assert sum(bool(PROJECTION.fullmatch(sql)) for sql in manager.calls) == 2


@pytest.mark.parametrize("limit", ["MAX_APPEND_FILES", "MAX_APPEND_ROWS"])
def test_append_inventory_over_its_limit_reads_additions_by_projection(monkeypatch, limit):
    manager = ProjectionManager(
        [snapshot(7, 10, "append", 1, None, 2), snapshot(10, 902, "append", 1, None, 2),
         snapshot(902, 4, "overwrite", None)],
        {902: {"match": [("0", "2025", "match", "kept")]},
         7: {"match": [("0", "2025", "match", "kept"), ("1", "2025", "match", "a"),
                       ("1", "2025", "match", "a"), ("1", "2025", "match", "b")]}},
    )
    monkeypatch.setattr(c1, limit, 1)
    monkeypatch.setattr(c1, "_compare_manifest_buckets", refuse_buckets())

    assert list(c1.manifest_changes(manager, 902, 7)) == [
        ("1", "2025", "match", "a"), ("1", "2025", "match", "a"), ("1", "2025", "match", "b")]
    # No rewrite occurred, so the common-schema proof is not needed.
    assert not any('$metadata_log_entries"' in sql for sql in manager.calls)
    assert len(manager.calls) == 5


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
