"""Unit coverage for the exact-identity SofaScore standings purge (#2469)."""

from __future__ import annotations

import importlib.util
import re
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "purge_sofascore_standings_duplicates.py"
PRE_SNAPSHOT_ID = 7_246_610_019_590_143_131
OWNED_APPEND_SNAPSHOT_ID = 6_106_183_601_553_813_582
PURGE_SNAPSHOT_ID = 8_357_721_120_601_254_242
FOREIGN_SNAPSHOT_ID = 5_127_472_990_381_682_135
CONCURRENT_SNAPSHOT_ID = 6_468_832_231_712_365_353
OTHER_QUERY_ID = "20261128_120000_00009_other"
DELETE_QUERY_ID = "20261128_120000_00001_test"
TOTAL_ROWS = 88_700


def _load_module():
    name = "purge_sofascore_standings_duplicates"
    spec = importlib.util.spec_from_file_location(name, SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load_module()


def _row_tuple(mod, row):
    return tuple(getattr(row, column) for column in mod.IDENTITY_COLUMNS)


def _partitions(mod):
    counts = {("SS-1", "2627"): 20}
    for stale in mod.STALE_ROWS:
        key = (stale.league, stale.season)
        counts[key] = counts.get(key, 0) + 2
    return counts


class FakeCursor:
    """Scripted answers for the reviewed table state; records executed SQL."""

    def __init__(self, mod):
        self.mod = mod
        self.stale_rows = {row: [_row_tuple(mod, row)] for row in mod.STALE_ROWS}
        self.keeper_rows = {row: [_row_tuple(mod, row)] for row in mod.KEEPER_ROWS}
        self.key_count_overrides: dict = {}
        self.extra_duplicates: set = set()
        self.removed_duplicates: set = set()
        self.executed: list[str] = []
        self.deleted = False
        self.rowcount = 7
        self.query_id = DELETE_QUERY_ID
        # snapshot id -> (parent id, operation, trino query id)
        self.nodes = {
            PRE_SNAPSHOT_ID: (None, "append", None),
            PURGE_SNAPSHOT_ID: (PRE_SNAPSHOT_ID, "delete", DELETE_QUERY_ID),
            CONCURRENT_SNAPSHOT_ID: (PURGE_SNAPSHOT_ID, "append", OTHER_QUERY_ID),
        }
        self.main = [PRE_SNAPSHOT_ID]
        self.after_delete = [PURGE_SNAPSHOT_ID]
        self.partition_mutation = None
        self.total_delta = len(mod.STALE_ROWS)
        self.post_delete_duplicates = False
        self.stale_survives = False
        self.fail_delete_send = False
        self.pending = None

    def duplicates(self):
        pinned = set() if self.deleted else {row.natural_key for row in self.mod.STALE_ROWS}
        extra = self.extra_duplicates | (
            {self.mod.STALE_ROWS[0].natural_key} if self.post_delete_duplicates else set()
        )
        return (pinned | extra) - self.removed_duplicates

    def key_count(self, row):
        override = self.key_count_overrides.get(row.natural_key)
        if override is not None:
            return override
        if self.deleted and not self.stale_survives:
            return 1
        return 2

    def snapshot(self, snapshot_id):
        parent_id, operation, query_id = self.nodes[snapshot_id]
        summary = {"trino_query_id": query_id} if query_id else {}
        return [
            (snapshot_id, parent_id, "2026-11-28T10:00:00.000000Z", operation, summary)
        ]

    def owned(self, sql):
        query_id = re.search(
            r"element_at\(summary, 'trino_query_id'\) = '([^']*)'", sql
        ).group(1)
        return [
            (snapshot_id, parent_id, operation)
            for snapshot_id, (parent_id, operation, owner) in sorted(
                self.nodes.items(), reverse=True
            )
            if owner == query_id
        ]

    def named_rows(self, sql):
        for row, rows in (*self.stale_rows.items(), *self.keeper_rows.items()):
            if f"team = '{row.team}'" in sql:
                if self.deleted and not self.stale_survives and row in self.stale_rows:
                    return []
                return rows
        raise AssertionError(f"unexpected named-row query: {sql}")

    def partitions(self):
        counts = dict(self.partitions_before)
        if self.deleted:
            counts = self.mod.expected_partition_counts_after(counts)
            counts.update(self.partition_mutation or {})
        return [(*partition, count) for partition, count in sorted(counts.items())]

    def fetchall(self):
        if self.pending is None:
            raise AssertionError("fetchall before execute")
        answer, self.pending = self.pending, None
        return answer()

    def execute(self, sql):
        compact = " ".join(sql.split())
        self.executed.append(compact)
        if compact.startswith("DELETE FROM"):
            self.pending = lambda: []
            if self.fail_delete_send:
                raise ConnectionError("connection reset while sending the DELETE")
            self.deleted = True
            self.main.extend(self.after_delete)
        elif "$refs" in compact:
            self.pending = lambda: self.snapshot(self.main[-1])
        elif "element_at(summary, 'trino_query_id') =" in compact:
            self.pending = lambda: self.owned(compact)
        elif "HAVING COUNT(*) > 1" in compact:
            self.pending = lambda: [(*key, 2) for key in sorted(self.duplicates())]
        elif "GROUP BY league, season" in compact:
            self.pending = self.partitions
        elif compact == f"SELECT COUNT(*) FROM {self.mod.TABLE}":
            total = TOTAL_ROWS - (self.total_delta if self.deleted else 0)
            self.pending = lambda: [(total,)]
        elif compact.startswith(f"SELECT COUNT(*) FROM {self.mod.TABLE} WHERE"):
            for row in self.mod.STALE_ROWS:
                if f"team_id = '{row.team_id}'" in compact and row.team_id in compact:
                    self.pending = lambda: [(self.key_count(row),)]
                    return
            raise AssertionError(f"unexpected key-count query: {compact}")
        elif compact.startswith('SELECT league, season, "group", team_id, team, _batch_id'):
            self.pending = lambda: self.named_rows(compact)
        else:
            raise AssertionError(f"unexpected SQL: {compact}")


@pytest.fixture()
def cursor(mod):
    fake = FakeCursor(mod)
    fake.partitions_before = _partitions(mod)
    return fake


def deletes(cursor):
    return [sql for sql in cursor.executed if sql.startswith("DELETE")]


def clean_state(cursor):
    cursor.stale_rows = {row: [] for row in cursor.mod.STALE_ROWS}
    cursor.removed_duplicates = {row.natural_key for row in cursor.mod.STALE_ROWS}
    cursor.key_count_overrides = {row.natural_key: 1 for row in cursor.mod.STALE_ROWS}


def test_allowlist_is_seven_aligned_pairs_with_distinct_keys(mod):
    assert len(mod.STALE_ROWS) == 7
    assert len({row.natural_key for row in mod.STALE_ROWS}) == 7
    for stale, keeper in zip(mod.STALE_ROWS, mod.KEEPER_ROWS):
        assert stale.natural_key == keeper.natural_key
        assert stale.group == "__total__"
        assert stale.team != keeper.team
        assert mod._parse_instant(keeper.raw_fetched_at, label="k") > mod._parse_instant(
            stale.raw_fetched_at, label="s"
        )


def test_delete_has_one_complete_identity_arm_per_stale_row(mod):
    sql = mod.render_delete_sql()
    assert sql.startswith("DELETE FROM iceberg.bronze.sofascore_league_table\nWHERE (")
    assert sql.count(" OR (") == 6
    assert "raw_fetched_at" not in sql
    for row in mod.STALE_ROWS:
        arm = mod._identity_predicate(row)
        assert arm in sql
        for fragment in (
            f"league = '{row.league}'",
            f"season = '{row.season}'",
            f'"group" = \'{row.group}\'',
            f"team_id = '{row.team_id}'",
            f"team = '{row.team}'",
            f"_batch_id = '{row.batch_id}'",
            f"_ingested_at = TIMESTAMP '{row.ingested_at}'",
            f"raw_content_hash = '{row.raw_content_hash}'",
        ):
            assert fragment in arm


def test_default_dry_run_validates_but_never_writes(mod, cursor):
    result = mod.purge(cursor)
    assert result == mod.PurgeResult(applied=False, deleted_rows=0, already_clean=False)
    assert not deletes(cursor)


@pytest.mark.parametrize(
    "changes",
    [
        {"batch_id": "00000000-0000-0000-0000-000000000000"},
        {"ingested_at": "2026-07-31 14:19:33.905386"},
        {"raw_content_hash": "0" * 64},
        {"raw_fetched_at": "2026-08-01T00:00:00+00:00"},
        {"team": "Renamed"},
    ],
)
def test_refuses_when_a_pinned_stale_row_changed(mod, cursor, changes):
    changed = replace(mod.STALE_ROWS[0], **changes)
    cursor.stale_rows[mod.STALE_ROWS[0]] = [_row_tuple(mod, changed)]
    with pytest.raises(mod.PurgeRefused, match="stale .*column"):
        mod.purge(cursor, apply=True)
    assert not deletes(cursor)


def test_refuses_when_a_pinned_row_is_short(mod, cursor):
    cursor.stale_rows[mod.STALE_ROWS[0]] = [_row_tuple(mod, mod.STALE_ROWS[0])[:5]]
    with pytest.raises(mod.PurgeRefused, match="stale .*5 columns"):
        mod.purge(cursor, apply=True)
    assert not deletes(cursor)


def test_refuses_when_a_keeper_is_missing_or_changed(mod, cursor):
    keeper = mod.KEEPER_ROWS[3]
    cursor.keeper_rows[keeper] = []
    with pytest.raises(mod.PurgeRefused, match="keeper .*expected exactly one row"):
        mod.purge(cursor, apply=True)
    cursor.keeper_rows[keeper] = [_row_tuple(mod, replace(keeper, team="Other Name"))]
    with pytest.raises(mod.PurgeRefused, match="keeper .*column team"):
        mod.purge(cursor, apply=True)
    assert not deletes(cursor)


def test_refuses_when_keeper_is_not_strictly_newer(mod, monkeypatch):
    # The pinned constants are consistent, so an inconsistent pair is modelled
    # by patching the module before the scripted table state is built.
    stale = mod.STALE_ROWS[5]
    keepers = list(mod.KEEPER_ROWS)
    keepers[5] = replace(keepers[5], raw_fetched_at=stale.raw_fetched_at)
    monkeypatch.setattr(mod, "KEEPER_ROWS", tuple(keepers))
    with pytest.raises(mod.PurgeRefused, match="not newer"):
        mod.purge(FakeCursor(mod))


def test_refuses_misaligned_keeper_set(mod, monkeypatch):
    monkeypatch.setattr(mod, "KEEPER_ROWS", mod.KEEPER_ROWS[:-1])
    with pytest.raises(mod.PurgeRefused, match="not aligned"):
        mod.purge(FakeCursor(mod), apply=True)


def test_refuses_unexpected_duplicate_keys(mod, cursor):
    cursor.extra_duplicates = {("SS-999", "2627", "__total__", "1")}
    with pytest.raises(mod.PurgeRefused, match="duplicate keys differ.*unexpected"):
        mod.purge(cursor)


def test_refuses_partially_applied_state(mod, cursor):
    gone = mod.STALE_ROWS[2]
    cursor.stale_rows[gone] = []
    cursor.key_count_overrides = {gone.natural_key: 1}
    cursor.removed_duplicates = {gone.natural_key}
    with pytest.raises(mod.PurgeRefused, match="partially applied"):
        mod.purge(cursor)


def test_partial_state_with_several_missing_rows_still_refuses_cleanly(mod, cursor):
    gone = [mod.STALE_ROWS[1], mod.STALE_ROWS[4], mod.STALE_ROWS[6]]
    for row in gone:
        cursor.stale_rows[row] = []
        cursor.removed_duplicates.add(row.natural_key)
    cursor.key_count_overrides = {row.natural_key: 1 for row in gone}
    with pytest.raises(mod.PurgeRefused, match="partially applied") as excinfo:
        mod.purge(cursor, apply=True)
    assert all(row.team in str(excinfo.value) for row in gone)
    assert not deletes(cursor)


def test_refuses_third_row_under_a_pinned_key(mod, cursor):
    third = mod.STALE_ROWS[3]
    cursor.key_count_overrides = {third.natural_key: 3}
    with pytest.raises(mod.PurgeRefused, match="holds 3 rows"):
        mod.purge(cursor, apply=True)
    assert not deletes(cursor)


def test_refuses_missing_duplicate_census_entry(mod, cursor):
    cursor.removed_duplicates = {mod.STALE_ROWS[0].natural_key}
    with pytest.raises(mod.PurgeRefused, match="duplicate keys differ.*missing"):
        mod.purge(cursor)


def test_refuses_duplicated_stale_row_before_delete(mod, cursor):
    cursor.stale_rows[mod.STALE_ROWS[1]] *= 2
    with pytest.raises(mod.PurgeRefused, match="expected exactly one row"):
        mod.purge(cursor, apply=True)
    assert not deletes(cursor)


def test_apply_uses_one_narrow_delete_and_verifies_ownership(mod, cursor):
    result = mod.purge(cursor, apply=True)
    assert len(deletes(cursor)) == 1
    assert result.applied and result.deleted_rows == 7 and not result.already_clean
    assert result.pre_snapshot_id == PRE_SNAPSHOT_ID
    assert result.purge_snapshot_id == PURGE_SNAPSHOT_ID
    assert result.delete_query_id == DELETE_QUERY_ID
    instruction = result.conditional_rollback_instruction
    assert "rollback_to_snapshot('bronze', 'sofascore_league_table'" in instruction
    assert str(PRE_SNAPSHOT_ID) in instruction
    assert str(PURGE_SNAPSHOT_ID) in instruction


def test_two_snapshot_copy_on_write_outcome_is_owned_in_either_order(mod, cursor):
    # Trino may commit rewritten files before or after the delete of the same query.
    cursor.nodes[OWNED_APPEND_SNAPSHOT_ID] = (PRE_SNAPSHOT_ID, "append", DELETE_QUERY_ID)
    cursor.nodes[PURGE_SNAPSHOT_ID] = (OWNED_APPEND_SNAPSHOT_ID, "delete", DELETE_QUERY_ID)
    assert mod.purge(cursor, apply=True).applied

    cursor = FakeCursor(mod)
    cursor.partitions_before = _partitions(mod)
    cursor.nodes[OWNED_APPEND_SNAPSHOT_ID] = (PRE_SNAPSHOT_ID, "delete", DELETE_QUERY_ID)
    cursor.nodes[PURGE_SNAPSHOT_ID] = (OWNED_APPEND_SNAPSHOT_ID, "append", DELETE_QUERY_ID)
    assert mod.purge(cursor, apply=True).applied


def test_single_overwrite_snapshot_of_the_same_query_is_owned(mod, cursor):
    cursor.nodes[PURGE_SNAPSHOT_ID] = (PRE_SNAPSHOT_ID, "overwrite", DELETE_QUERY_ID)
    assert mod.purge(cursor, apply=True).applied


def test_owned_chain_without_a_removal_is_unknown(mod, cursor):
    cursor.nodes[PURGE_SNAPSHOT_ID] = (PRE_SNAPSHOT_ID, "append", DELETE_QUERY_ID)
    with pytest.raises(mod.PostDeleteVerificationError, match="exactly one removal") as excinfo:
        mod.purge(cursor, apply=True)
    assert excinfo.value.conditional_rollback_instruction is None


def test_owned_snapshot_query_tolerates_snapshots_without_the_query_id(mod, cursor):
    # A map subscript raises on snapshots that lack the key; element_at does not.
    mod.purge(cursor, apply=True)
    owned_queries = [
        sql for sql in cursor.executed if "element_at(summary, 'trino_query_id') =" in sql
    ]
    assert len(owned_queries) == 1 and f"= '{DELETE_QUERY_ID}'" in owned_queries[0]
    assert not any("summary['trino_query_id']" in sql for sql in cursor.executed)


@pytest.mark.parametrize(
    "attribute, value",
    [
        ("rowcount", 6),
        ("rowcount", 8),
        ("after_delete", [PRE_SNAPSHOT_ID]),  # nothing committed
        ("after_delete", [CONCURRENT_SNAPSHOT_ID]),
        ("query_id", OTHER_QUERY_ID),
        ("query_id", None),
    ],
)
def test_unproven_delete_outcome_is_unknown_without_rollback(
    mod, cursor, caplog, attribute, value
):
    setattr(cursor, attribute, value)
    with pytest.raises(mod.PostDeleteVerificationError) as excinfo:
        mod.purge(cursor, apply=True)
    assert cursor.deleted
    assert excinfo.value.conditional_rollback_instruction is None
    assert "CONDITIONAL ROLLBACK" not in caplog.text


def test_foreign_snapshot_between_pre_and_purge_is_unknown(mod, cursor):
    cursor.nodes[FOREIGN_SNAPSHOT_ID] = (PRE_SNAPSHOT_ID, "append", OTHER_QUERY_ID)
    cursor.nodes[PURGE_SNAPSHOT_ID] = (FOREIGN_SNAPSHOT_ID, "delete", DELETE_QUERY_ID)
    with pytest.raises(mod.PostDeleteVerificationError, match="was not committed") as excinfo:
        mod.purge(cursor, apply=True)
    assert excinfo.value.conditional_rollback_instruction is None


def test_delete_whose_chain_does_not_reach_pre_is_unknown(mod, cursor):
    cursor.nodes[FOREIGN_SNAPSHOT_ID] = (None, "append", DELETE_QUERY_ID)
    cursor.nodes[PURGE_SNAPSHOT_ID] = (FOREIGN_SNAPSHOT_ID, "delete", DELETE_QUERY_ID)
    with pytest.raises(mod.PostDeleteVerificationError, match="does not reach"):
        mod.purge(cursor, apply=True)


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"post_delete_duplicates": True}, "duplicate natural keys remain"),
        ({"total_delta": 6}, "did not decrease by exactly"),
        ({"stale_survives": True}, "pinned stale rows remain"),
    ],
)
def test_post_check_fails_when_the_delete_left_the_reviewed_state(
    mod, cursor, changes, message
):
    for attribute, value in changes.items():
        setattr(cursor, attribute, value)
    with pytest.raises(mod.PostDeleteVerificationError, match=message) as excinfo:
        mod.purge(cursor, apply=True)
    assert excinfo.value.conditional_rollback_instruction


def test_failed_delete_send_reports_unknown_without_rollback(mod, cursor):
    cursor.fail_delete_send = True
    with pytest.raises(mod.PostDeleteVerificationError, match="may have been sent") as excinfo:
        mod.purge(cursor, apply=True)
    assert excinfo.value.conditional_rollback_instruction is None


def test_post_check_fails_when_another_partition_changed(mod, cursor):
    cursor.partition_mutation = {("SS-1", "2627"): 19}
    with pytest.raises(mod.PostDeleteVerificationError, match="outside the pinned") as excinfo:
        mod.purge(cursor, apply=True)
    assert excinfo.value.conditional_rollback_instruction


def test_post_check_fails_when_a_keeper_changed(mod, cursor):
    keeper = mod.KEEPER_ROWS[4]
    execute = cursor.execute

    def execute_then_change_keeper(sql):
        execute(sql)
        if sql.startswith("DELETE"):
            cursor.keeper_rows[keeper] = [
                _row_tuple(mod, replace(keeper, raw_content_hash="f" * 64))
            ]
    cursor.execute = execute_then_change_keeper
    with pytest.raises(mod.PostDeleteVerificationError, match="keeper .*column"):
        mod.purge(cursor, apply=True)


def test_refuses_before_delete_if_main_snapshot_changes_during_preflight(mod, cursor):
    execute = cursor.execute
    reads = {"snapshots": 0}

    def execute_with_concurrent_append(sql):
        if "$refs" in sql:
            reads["snapshots"] += 1
        execute(sql)
        if "$refs" in sql and reads["snapshots"] == 2:
            cursor.main.append(CONCURRENT_SNAPSHOT_ID)
    cursor.execute = execute_with_concurrent_append
    with pytest.raises(mod.PurgeRefused, match="changed during preflight"):
        mod.purge(cursor, apply=True)
    assert not deletes(cursor)


def test_refuses_if_main_advances_after_purge_during_final_verification(mod, cursor):
    execute = cursor.execute

    def execute_with_concurrent_append(sql):
        if "GROUP BY league, season" in sql and cursor.deleted:
            cursor.main.append(CONCURRENT_SNAPSHOT_ID)
        execute(sql)
    cursor.execute = execute_with_concurrent_append
    # Main already moved past the purge snapshot, so the guarded rollback is void.
    with pytest.raises(mod.PostDeleteVerificationError, match="advanced during final") as excinfo:
        mod.purge(cursor, apply=True)
    assert excinfo.value.conditional_rollback_instruction is None


def test_already_clean_apply_is_idempotent(mod, cursor):
    clean_state(cursor)
    result = mod.purge(cursor, apply=True)
    assert result == mod.PurgeResult(applied=True, deleted_rows=0, already_clean=True)
    assert not deletes(cursor)


def test_clean_path_refuses_when_a_keeper_is_missing(mod, cursor):
    clean_state(cursor)
    cursor.key_count_overrides[mod.STALE_ROWS[2].natural_key] = 0
    with pytest.raises(mod.PurgeRefused, match="holds 0 rows instead of the single keeper"):
        mod.purge(cursor, apply=True)


def test_clean_with_remaining_duplicates_is_refused(mod, cursor):
    clean_state(cursor)
    cursor.extra_duplicates = {("SS-999", "2627", "__total__", "1")}
    with pytest.raises(mod.PurgeRefused, match="duplicate keys remain"):
        mod.purge(cursor)


def test_owned_snapshot_query_is_scoped_to_the_cursor_query_id(mod):
    with pytest.raises(mod.PurgeRefused, match="not a Trino query id"):
        mod.load_owned_snapshots(SimpleNamespace(execute=lambda sql: None), "'; DROP TABLE x")


def test_naive_timestamp_is_treated_as_utc_and_garbage_is_refused(mod):
    aware = mod._parse_instant("2026-11-09T14:51:46.073583+00:00", label="x")
    naive = mod._parse_instant("2026-11-09T14:51:46.073583", label="x")
    assert aware == naive
    with pytest.raises(mod.PurgeRefused, match="not ISO-8601"):
        mod._parse_instant("09/11/2026 14:51", label="x")


def test_insecure_tls_is_dry_run_only(mod, monkeypatch):
    monkeypatch.setenv("TRINO_TLS_VERIFY", "false")
    assert mod._trino_tls_verify(allow_insecure=True) is False
    with pytest.raises(mod.PurgeRefused, match="forbidden for --apply"):
        mod._trino_tls_verify()
    monkeypatch.setenv("TRINO_TLS_VERIFY", "maybe")
    with pytest.raises(mod.PurgeRefused, match="must be one of"):
        mod._trino_tls_verify(allow_insecure=True)


def test_main_reports_refusal_and_unknown_outcome(mod, monkeypatch):
    connection = SimpleNamespace(cursor=lambda: None, close=lambda: None)
    monkeypatch.setattr(mod, "get_conn", lambda **kwargs: connection)
    applied = []

    def refuse(cursor, *, apply):
        applied.append(apply)
        raise mod.PurgeRefused("state changed")
    monkeypatch.setattr(mod, "purge", refuse)
    assert mod.main([]) == 2

    def unknown(cursor, *, apply):
        raise mod.PostDeleteVerificationError("lost answer")
    monkeypatch.setattr(mod, "purge", unknown)
    assert mod.main(["--apply"]) == 3
    assert applied == [False]
