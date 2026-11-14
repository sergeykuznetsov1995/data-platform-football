"""Offline regression for bounded C1 manifest comparison."""

from collections import Counter
from datetime import date, datetime
import json
import re

import pytest

from scrapers.fotmob import c1_inputs as c1


pytestmark = pytest.mark.unit
TYPES = ("match", "league_season", "competition_profile", "competition_seasons")
FIELDS = (
    "run_id", "target_type", "target_key", "competition_id", "source_season_key",
    "stage_id", "entity_id", "batch_id", "content_hash", "observation_id", "raw_uri",
    "parser_version", "status", "fetch_outcome", "http_status", "attempts", "retries",
    "cache_hit", "stale", "fetched_at", "completed_at", "retry_after", "direct_bytes",
    "proxy_bytes", "encoded_bytes", "decoded_bytes", "expected_counts_json",
    "actual_counts_json", "capabilities_json", "exclusions_json", "unknown_paths_json",
    "error_code", "error",
)
IDENTITY = ("competition_id", "source_season_key", "target_type", "entity_id")
OLD, NEW = 902, 7  # A current snapshot need not have a larger ID.
DAY = date(2026, 11, 13)
SQL_VALUE = r"(?:NULL|'(?:''|[^'])*')"
SOURCE = re.compile(
    r"\(SELECT \* FROM iceberg\.bronze\.fotmob_ingest_manifest "
    r"FOR VERSION AS OF (\d+) WHERE "
    r"(target_type=" + SQL_VALUE
    + r"(?: AND competition_id IS NOT DISTINCT FROM " + SQL_VALUE
    + r" AND source_season_key IS NOT DISTINCT FROM " + SQL_VALUE + r")?)\)"
)


def row(target_type, cid="10", season="2026/27", mid="100", **changes):
    values = dict.fromkeys(FIELDS, None)
    values.update(
        run_id="synthetic-run", target_type=target_type, target_key="synthetic-key",
        competition_id=cid, source_season_key=season, entity_id=mid,
        batch_id="synthetic-batch", content_hash="same-content-hash",
        parser_version=c1.PARSER_VERSION, status="success", attempts=1, retries=0,
        cache_hit=False, stale=False, http_status=200,
        # Wide columns stay in the equality relation, even when content_hash is
        # unchanged. Synthetic only; this is not a runtime memory benchmark.
        actual_counts_json=json.dumps({"synthetic": "x" * 8192}),
        error="synthetic detail " * 512,
    )
    values.update(changes)
    return tuple(values[field] for field in FIELDS)


def identity(record):
    return tuple(record[FIELDS.index(field)] for field in IDENTITY)


def reference(before, after):
    """Unscoped full-row multiset oracle, with projection only AFTER subtraction."""
    before = Counter(r for r in before if r[FIELDS.index("target_type")] in TYPES)
    after = Counter(r for r in after if r[FIELDS.index("target_type")] in TYPES)
    return {identity(r) for r in (before - after) | (after - before)}


def decode(value):
    return None if value == "NULL" else value[1:-1].replace("''", "'")


class ManifestManager:
    """Tiny evaluator for the supported SQL grammar, using exact bag semantics.

    Rejects unscoped heavy queries, missing time travel, key/hash-only comparison
    and all-scope UNION. The oracle above independently compares whole snapshots.
    Result iteration must finish before the caller submits another statement.
    """

    def __init__(self, before, after, *, fail_batch=None, expired=None):
        self.data = {OLD: before, NEW: after}
        self.fail_batch = fail_batch
        self.expired = expired
        self.queries = []
        self.discoveries = []
        self.batches = []
        self.completed = []
        self.pending = False

    def select(self, selection, *, scoped):
        version, predicate = int(selection[0]), selection[1]
        assert version in self.data
        if version == self.expired:
            raise RuntimeError("synthetic expired snapshot")
        pattern = r"target_type=(" + SQL_VALUE + ")"
        if scoped:
            pattern += (
                r" AND competition_id IS NOT DISTINCT FROM (" + SQL_VALUE + ")"
                r" AND source_season_key IS NOT DISTINCT FROM (" + SQL_VALUE + ")"
            )
        match = re.fullmatch(pattern, predicate)
        assert match, "comparison must have one typed, NULL-safe scope"
        values = tuple(decode(value) for value in match.groups())
        fields = ("target_type", "competition_id", "source_season_key") if scoped else ("target_type",)
        selected = [r for r in self.data[version]
                    if tuple(r[FIELDS.index(field)] for field in fields) == values]
        return version, values, selected

    def execute_query(self, sql):
        assert not self.pending, "next statement submitted before consuming the batch"
        self.queries.append(sql)
        normalized = re.sub(r"\s+", " ", sql).strip()
        selections = SOURCE.findall(normalized)
        structure = SOURCE.sub("<source>", normalized)
        if "EXCEPT" in sql:
            assert re.fullmatch(
                r"SELECT competition_id, source_season_key, target_type, entity_id "
                r"FROM \(\s*SELECT \* FROM <source> EXCEPT ALL SELECT \* FROM <source>\)",
                structure,
            ), "full-row EXCEPT ALL must run separately for each scope and direction"
            assert len(selections) == 2
            left_version, scope, left = self.select(selections[0], scoped=True)
            right_version, right_scope, right = self.select(selections[1], scoped=True)
            assert scope == right_scope
            assert {left_version, right_version} == {OLD, NEW}
            batch = (scope, left_version, right_version)
            self.batches.append(batch)
            if len(self.batches) == self.fail_batch:
                raise RuntimeError("synthetic query memory limit")
            result = [identity(r) for r in (Counter(left) - Counter(right)).elements()]
            self.pending = True

            def consume():
                yield from result
                self.completed.append(batch)
                self.pending = False

            return consume()
        if structure == (
            "SELECT competition_id, source_season_key FROM <source> "
            "UNION SELECT competition_id, source_season_key FROM <source>"
        ):
            assert len(selections) == 2
            left_version, scope, left = self.select(selections[0], scoped=False)
            right_version, right_scope, right = self.select(selections[1], scoped=False)
            assert scope == right_scope
            assert {left_version, right_version} == {OLD, NEW}
            self.discoveries.append(scope[0])
            return list(dict.fromkeys((identity(r)[0], identity(r)[1]) for r in left + right))
        return self.other_query(sql)

    def other_query(self, sql):
        raise AssertionError(f"unexpected SQL: {sql}")


def expected_batches(before, after):
    scopes = {(r[FIELDS.index("target_type")], *identity(r)[:2])
              for r in before + after if r[FIELDS.index("target_type")] in TYPES}
    return {(scope, left, right) for scope in scopes for left, right in ((OLD, NEW), (NEW, OLD))}


@pytest.mark.parametrize("target_type", TYPES)
@pytest.mark.parametrize("scenario", ["rewrite", "insert", "delete", "rollback", "duplicate", "null"])
def test_scoped_comparison_matches_whole_snapshot_multiset(target_type, scenario):
    base = row(target_type)
    extra = row(target_type, cid="20", season="2025/26", mid="200")
    if scenario == "rewrite":
        before, after = [base, base, extra], [extra, base, base]
    elif scenario == "insert":
        before, after = [base], [base, extra]
    elif scenario == "delete":
        before, after = [base, extra], [extra]
    elif scenario == "rollback":
        before = [base, extra, row(target_type, error="later evidence")]
        after = [base]
    elif scenario == "duplicate":
        before, after = [base, base, extra], [base, extra, extra]
    else:
        nulls = [row(target_type, cid=cid, season=sk, mid=None)
                 for cid, sk in ((None, None), (None, "2026/27"), ("10", None))]
        before, after = [base, *nulls, nulls[0]], [*nulls, nulls[1]]
    # An out-of-contract type must not leak into discoveries or changes.
    ignored = row("not_a_c1_target", mid="999")
    before.append(ignored)
    manager = ManifestManager(before, after)

    actual = set(c1.manifest_changes(manager, OLD, NEW))

    assert actual == reference(before, after)
    assert set(manager.batches) == expected_batches(before, after)
    assert len(manager.batches) == len(set(manager.batches))
    assert manager.completed == manager.batches
    assert manager.discoveries == list(TYPES)
    if scenario == "rewrite":
        assert actual == set()
    else:
        assert actual


@pytest.mark.parametrize("field", [field for field in FIELDS if field not in IDENTITY])
def test_changes_only_in_nonidentity_fields_remain_visible(field):
    # Keep the consumer identity fixed, varying each remaining TargetCommit
    # column independently. In particular, wide JSON/error edits need not alter
    # content_hash, timestamps, batch_id or any other convenient digest/key.
    before = [row(target_type) for target_type in TYPES]
    original = before[0][FIELDS.index(field)]
    if isinstance(original, bool):
        replacement = not original
    elif isinstance(original, int):
        replacement = original + 1
    else:
        replacement = "synthetic changed value " * 1024
    after = [row(target_type, **{field: replacement}) for target_type in TYPES]
    manager = ManifestManager(before, after)

    assert set(c1.manifest_changes(manager, OLD, NEW)) == reference(before, after)
    assert reference(before, after) == {identity(r) for r in before}


def test_equal_rewrite_uses_observed_scopes_without_fixed_bucket_fanout():
    before = [row(target_type, cid=str(cid), season=season)
              for target_type in TYPES for cid in (10, 20)
              for season in (None, "2026/27", "synthetic ' season")]
    manager = ManifestManager(before, list(reversed(before)))

    assert list(c1.manifest_changes(manager, OLD, NEW)) == []
    assert len(manager.discoveries) == 4
    assert len(manager.batches) == 2 * len(before)
    assert len(manager.queries) == 4 + 2 * len(before)
    assert manager.completed == manager.batches


def test_empty_snapshots_only_discover_scopes():
    manager = ManifestManager([], [])
    assert list(c1.manifest_changes(manager, OLD, NEW)) == []
    assert len(manager.queries) == 4
    assert manager.batches == []


@pytest.mark.parametrize("deletion", [False, True])
def test_scopes_existing_in_only_one_snapshot_are_compared(deletion):
    records = [row(target_type, cid=None, season=None, mid=None) for target_type in TYPES]
    before, after = (records, []) if deletion else ([], records)
    manager = ManifestManager(before, after)

    assert set(c1.manifest_changes(manager, OLD, NEW)) == reference(before, after)
    assert len(manager.completed) == 8
    assert set(manager.completed) == expected_batches(before, after)


def test_comparison_iterator_does_not_prefetch_batches():
    before = [row("match")]
    after = [row("match", error="changed"), row("match", mid="101")]
    manager = ManifestManager(before, after)
    changes = c1.manifest_changes(manager, OLD, NEW)
    assert manager.queries == []

    first = next(changes)
    assert len(manager.queries) == 2  # one discovery and one heavy query
    assert len(manager.batches) == 1
    assert manager.completed == []
    assert manager.pending
    assert {first, *changes} == reference(before, after)
    assert manager.completed == manager.batches


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 14, 12, tzinfo=tz)


class RefreshManager(ManifestManager):
    def __init__(self, before, after, **kwargs):
        super().__init__(before, after, **kwargs)
        self.current = {table: 900 + i for i, table in enumerate(c1.SOURCES)}
        self.current[c1.MANIFEST] = NEW
        previous = dict(self.current, **{c1.MANIFEST: OLD})
        self.files = {table: {} for table in (c1.MATCHES, c1.PAYLOADS, c1.SCOPE, c1.SEASONS)}
        self.files[c1.MATCHES] = {(str(cid), "2026/27"): "unchanged" for cid in (10, 20, 30, 40)}
        self.checkpoint = {
            "contract_version": c1.CONTRACT_VERSION,
            "parser_versions": [c1.PARSER_VERSION, c1.LEGACY_PARSER_VERSION],
            "first_day": str(DAY), "last_day": str(DAY), "snapshots": previous,
            "files": {table: c1._encoded_pairs(values) for table, values in self.files.items()},
        }
        self.writes = []
        self.required_batches = expected_batches(before, after)
        self.required_discoveries = list(TYPES)

    def other_query(self, sql):
        if sql.startswith("SELECT competition_id, source_season_key, match_id, kind, generation"):
            return [(None, None, None, "checkpoint", "old", True, json.dumps(self.checkpoint))]
        if sql.startswith(("CREATE TABLE ", "INSERT INTO ", "MERGE INTO ", "DROP TABLE ")):
            assert set(self.completed) == self.required_batches
            assert len(self.completed) == len(self.required_batches)
            assert self.discoveries == self.required_discoveries
            self.writes.append(sql)
            return []
        if sql.startswith(("SELECT DISTINCT match_id FROM ",
                           "SELECT DISTINCT competition_id, source_season_key FROM ",
                           "SELECT deadline_day, competition_id, source_season_key, match_id, kind",
                           "WITH candidates AS")):
            # No eligible calendar rows in this synthetic refresh. Real refresh
            # still computes affected closure, stages days/checkpoint and MERGEs.
            return []
        return super().other_query(sql)


def setup_refresh(monkeypatch, manager):
    monkeypatch.setattr(c1, "datetime", FrozenDatetime)
    monkeypatch.setattr(c1, "snapshots", lambda _: dict(manager.current))
    monkeypatch.setattr(c1, "partition_fingerprints", lambda _, table: manager.files[table])


@pytest.mark.parametrize("rewrite", [False, True])
def test_refresh_consumes_sequential_scopes_before_publishing(monkeypatch, rewrite):
    before = [row(target_type, cid=str(cid)) for target_type, cid in zip(TYPES, (10, 20, 30, 40))]
    after = list(reversed(before)) if rewrite else [
        row(target_type, cid=str(cid), error="changed wide evidence " * 1024)
        for target_type, cid in zip(TYPES, (10, 20, 30, 40))
    ]
    manager = RefreshManager(before, after)
    setup_refresh(monkeypatch, manager)
    staged = []
    real_stage_rows = c1._stage_rows

    def stage_rows(manager_arg, stage, rows):
        assert manager.completed == manager.batches
        assert set(manager.completed) == manager.required_batches
        rows = list(rows)
        staged.extend(rows)
        return real_stage_rows(manager_arg, stage, rows)

    monkeypatch.setattr(c1, "_stage_rows", stage_rows)
    result = c1.refresh(manager)

    assert result["seasons"] == (0 if rewrite else 4)
    assert result["rows"] == 2  # day marker and checkpoint
    assert len(manager.completed) == 8
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.writes) == 1
    checkpoint_rows = [r for r in staged if r[4] == "checkpoint"]
    assert len(checkpoint_rows) == 1
    assert json.loads(checkpoint_rows[0][-1])["snapshots"][c1.MANIFEST] == NEW


@pytest.mark.parametrize("fail_batch", [1, 2, 5, 8])
def test_refresh_batch_failure_aborts_before_any_publication(monkeypatch, fail_batch):
    before = [row(target_type, cid=str(cid)) for target_type, cid in zip(TYPES, (10, 20, 30, 40))]
    after = [row(target_type, cid=str(cid), error="changed")
             for target_type, cid in zip(TYPES, (10, 20, 30, 40))]
    manager = RefreshManager(before, after, fail_batch=fail_batch)
    setup_refresh(monkeypatch, manager)

    with pytest.raises(RuntimeError, match="synthetic query memory limit"):
        c1.refresh(manager)

    assert len(manager.batches) == fail_batch
    assert len(manager.completed) == fail_batch - 1
    assert manager.writes == []
    assert manager.checkpoint["snapshots"][c1.MANIFEST] == OLD


@pytest.mark.parametrize("expired", [OLD, NEW])
def test_refresh_missing_snapshot_fails_closed(monkeypatch, expired):
    before = [row("match")]
    manager = RefreshManager(before, before, expired=expired)
    setup_refresh(monkeypatch, manager)

    with pytest.raises(RuntimeError, match="synthetic expired snapshot"):
        c1.refresh(manager)

    assert manager.writes == []
    assert manager.batches == []
    assert len(manager.queries) == 2  # checkpoint read, then failed discovery


def test_refresh_equal_snapshot_ids_skip_manifest_work(monkeypatch):
    manager = RefreshManager([row("match")], [row("match")])
    manager.checkpoint["snapshots"][c1.MANIFEST] = NEW
    manager.required_batches = set()
    manager.required_discoveries = []
    setup_refresh(monkeypatch, manager)

    result = c1.refresh(manager)

    assert result["seasons"] == 0
    assert manager.batches == []
    assert not any("FOR VERSION AS OF" in sql for sql in manager.queries)
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.writes) == 1
