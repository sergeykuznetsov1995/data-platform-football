"""Unit coverage for the exact-key SofaScore manifest purge (#999)."""

from __future__ import annotations

import importlib.util
import re
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "purge_sofascore_manifest_observations.py"
PRE_SNAPSHOT_ID = 7_246_610_019_590_143_131
PURGE_SNAPSHOT_ID = 8_357_721_120_601_254_242
CONCURRENT_SNAPSHOT_ID = 6_468_832_231_712_365_353
DELETE_QUERY_ID = "20260722_080000_00001_test"
_SAME_QUERY_ID = object()


def _load_module():
    name = "purge_sofascore_manifest_observations"
    spec = importlib.util.spec_from_file_location(name, SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load_module()


def _candidate_rows(mod):
    transport_key = mod.ManifestKey(
        "16",
        "58210",
        "season_page",
        "last:0",
        "schedule_last",
        "day-2026-07-16",
    )
    rows = []
    for key in sorted(mod.PURGE_ALLOWLIST):
        transport = key == transport_key
        rows.append(
            (
                *key.as_tuple(),
                "retryable_failure",
                "TransportError" if transport else "DeferredMaterialization",
                None if transport else f"hash-{key.target_id}-{key.freshness_key}",
                None if transport else f"raw/{key.target_id}/{key.freshness_key}",
                mod._OBSERVED_CANDIDATE_UPDATED_AT[key],
            )
        )
    return rows


def _replacement_rows(mod):
    targets = sorted(
        {(key.target_type, key.target_id, key.endpoint) for key in mod.PURGE_ALLOWLIST}
    )
    return [
        (
            target_type,
            target_id,
            endpoint,
            "day-2026-07-22",
            "legitimate_empty" if endpoint == "schedule_next" else "success",
            "2026-07-22T14:00:00+00:00",
            f"hash-{endpoint}-{target_id}",
            f"raw/{endpoint}/{target_id}.json.gz",
            404 if endpoint == "schedule_next" else 200,
            0 if endpoint == "schedule_next" else 1,
        )
        for target_type, target_id, endpoint in targets
    ]


def _mutate_replacement(rows, *, endpoint, column, value):
    mutated = list(rows)
    index = next(i for i, row in enumerate(mutated) if row[2] == endpoint)
    replacement = list(mutated[index])
    replacement[column] = value
    mutated[index] = tuple(replacement)
    return mutated


class FakeCursor:
    def __init__(
        self,
        mod,
        *,
        candidates=None,
        replacements=None,
        final_evidence=(104, 520, 5, 5, 5),
        total_before=20_000,
        total_after=19_989,
        protected_before=777,
        protected_after=777,
        remaining_after=None,
        before_snapshot_rows=None,
        after_snapshot_rows=None,
        delete_query_id=DELETE_QUERY_ID,
        delete_query_id_after_fetch=_SAME_QUERY_ID,
        delete_rowcount=11,
        delete_fetch_error=None,
        first_after_snapshot_error=None,
    ):
        self.mod = mod
        self.candidates = (
            list(_candidate_rows(mod)) if candidates is None else list(candidates)
        )
        self.replacements = (
            list(_replacement_rows(mod))
            if replacements is None
            else list(replacements)
        )
        self.final_evidence = tuple(final_evidence)
        self.total_before = total_before
        self.total_after = total_after
        self.protected_before = protected_before
        self.protected_after = protected_after
        self.remaining_after = [] if remaining_after is None else list(remaining_after)
        self.delete_query_id = delete_query_id
        self.delete_query_id_after_fetch = (
            delete_query_id
            if delete_query_id_after_fetch is _SAME_QUERY_ID
            else delete_query_id_after_fetch
        )
        self.delete_rowcount = delete_rowcount
        self.delete_fetch_error = delete_fetch_error
        self.first_after_snapshot_error = first_after_snapshot_error
        self.before_snapshot_rows = list(
            before_snapshot_rows
            or [
                (
                    PRE_SNAPSHOT_ID,
                    111,
                    "2026-07-21 11:47:42.405 UTC",
                    "replace",
                    {"trino_query_id": "older-query"},
                ),
            ]
        )
        self.after_snapshot_rows = list(
            after_snapshot_rows
            or [
                (
                    PURGE_SNAPSHOT_ID,
                    PRE_SNAPSHOT_ID,
                    "2026-07-22 08:00:00.000 UTC",
                    "delete",
                    {
                        "trino_query_id": delete_query_id,
                        "added-position-deletes": "11",
                    },
                ),
            ]
        )
        self.before_snapshot_reads = 0
        self.after_snapshot_reads = 0
        self.sql_log: list[str] = []
        self.last_sql = ""
        self.deleted = False
        self.closed = False
        self.query_id = None
        self.rowcount = -1

    def execute(self, sql):
        self.last_sql = sql
        self.sql_log.append(sql)
        if sql.lstrip().startswith("DELETE FROM"):
            self.deleted = True
            self.query_id = self.delete_query_id

    def fetchall(self):
        sql = self.last_sql.lstrip()
        if sql.startswith("SELECT source_tournament_id"):
            return self.remaining_after if self.deleted else self.candidates
        if sql.startswith("SELECT target_type"):
            return self.replacements
        if sql.startswith("WITH final_success"):
            return [self.final_evidence]
        if "AS total_rows" in sql:
            return [(self.total_after if self.deleted else self.total_before,)]
        if "AS protected_terminal_rows" in sql:
            return [(
                self.protected_after if self.deleted else self.protected_before,
            )]
        if sql.startswith("SELECT r.snapshot_id"):
            if self.deleted:
                if (
                    self.first_after_snapshot_error is not None
                    and self.after_snapshot_reads == 0
                ):
                    self.after_snapshot_reads += 1
                    raise self.first_after_snapshot_error
                index = min(
                    self.after_snapshot_reads,
                    len(self.after_snapshot_rows) - 1,
                )
                self.after_snapshot_reads += 1
                return [self.after_snapshot_rows[index]]
            index = min(
                self.before_snapshot_reads,
                len(self.before_snapshot_rows) - 1,
            )
            self.before_snapshot_reads += 1
            return [self.before_snapshot_rows[index]]
        if sql.startswith("DELETE FROM"):
            if self.delete_fetch_error is not None:
                raise self.delete_fetch_error
            self.query_id = self.delete_query_id_after_fetch
            self.rowcount = self.delete_rowcount
            return []
        raise AssertionError(f"unexpected SQL: {self.last_sql}")

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def _install_fake_trino(monkeypatch):
    captured = {"connect_calls": 0}

    class BasicAuthentication:
        def __init__(self, user, password):
            self.user = user
            self.password = password

    def connect(**kwargs):
        captured["connect_calls"] += 1
        captured["kwargs"] = kwargs
        return object()

    fake = SimpleNamespace(
        auth=SimpleNamespace(BasicAuthentication=BasicAuthentication),
        dbapi=SimpleNamespace(connect=connect),
    )
    monkeypatch.setitem(sys.modules, "trino", fake)
    return captured


def _set_tls_environment(monkeypatch, **values):
    for variable in (
        "TRINO_PASSWORD",
        "TRINO_TLS_VERIFY",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
    ):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("TRINO_PASSWORD", "secret")
    for variable, value in values.items():
        monkeypatch.setenv(variable, value)


@pytest.mark.unit
def test_allowlist_is_exact_immutable_natural_key_set(mod):
    expected = {
        ("16", "58210", "season_page", "last:0", "schedule_last", "day-2026-07-16"),
        *{
            ("16", "58210", "season_page", f"last:{page}", "schedule_last", "day-2026-07-18")
            for page in range(4)
        },
        ("16", "58210", "season", "58210", "standings_total", "day-2026-07-19"),
        *{
            ("16", "58210", "season_page", f"last:{page}", "schedule_last", "day-2026-07-19")
            for page in range(4)
        },
        ("16", "58210", "season_page", "next:0", "schedule_next", "day-2026-07-19"),
    }

    assert isinstance(mod.PURGE_ALLOWLIST, frozenset)
    assert {key.as_tuple() for key in mod.PURGE_ALLOWLIST} == expected
    assert len(mod.PURGE_ALLOWLIST) == 11
    assert mod.NATURAL_KEY_COLUMNS == (
        "source_tournament_id",
        "source_season_id",
        "target_type",
        "target_id",
        "endpoint",
        "freshness_key",
    )
    assert mod.TRANSPORT_ERROR_KEY.as_tuple() == (
        "16",
        "58210",
        "season_page",
        "last:0",
        "schedule_last",
        "day-2026-07-16",
    )


@pytest.mark.unit
def test_delete_predicate_has_one_complete_or_arm_per_exact_key(mod):
    predicate = mod._key_predicate(mod.PURGE_ALLOWLIST)
    arms = []
    for line in predicate.splitlines():
        arm = line.strip()
        if arm.startswith("OR "):
            arm = arm.removeprefix("OR ")
        if " AND " not in arm:
            continue
        pairs = re.findall(r"([a-z_]+) = '([^']*)'", arm)
        assert len(pairs) == len(mod.NATURAL_KEY_COLUMNS)
        assert tuple(column for column, _value in pairs) == mod.NATURAL_KEY_COLUMNS
        arms.append(tuple(value for _column, value in pairs))

    assert len(arms) == 11
    assert set(arms) == {key.as_tuple() for key in mod.PURGE_ALLOWLIST}


@pytest.mark.unit
def test_default_dry_run_validates_evidence_but_never_writes(mod):
    cursor = FakeCursor(mod)

    result = mod.purge(cursor)

    assert result == mod.PurgeResult(
        applied=False,
        deleted_rows=0,
        already_clean=False,
    )
    assert not any(sql.lstrip().startswith("DELETE") for sql in cursor.sql_log)
    assert not any("$snapshots" in sql for sql in cursor.sql_log)
    assert not any("AS total_rows" in sql for sql in cursor.sql_log)


@pytest.mark.unit
@pytest.mark.parametrize("mutation", ["missing", "unexpected", "duplicate"])
def test_refuses_candidate_key_mismatch(mod, mutation):
    rows = _candidate_rows(mod)
    if mutation == "missing":
        rows.pop()
    elif mutation == "unexpected":
        row = list(rows[-1])
        row[3] = "last:99"
        rows[-1] = tuple(row)
    else:
        rows.append(rows[-1])
    cursor = FakeCursor(mod, candidates=rows)

    with pytest.raises(mod.PurgeRefused, match="exact 11-key allowlist"):
        mod.purge(cursor, apply=True)

    assert not any(sql.lstrip().startswith("DELETE") for sql in cursor.sql_log)


@pytest.mark.unit
@pytest.mark.parametrize("gap", ["deferred_raw", "transport_raw", "fingerprint"])
def test_refuses_candidate_evidence_gaps(mod, gap):
    candidates = mod.load_candidates(FakeCursor(mod))
    if gap == "deferred_raw":
        index = next(
            i for i, item in enumerate(candidates)
            if item.error_type == "DeferredMaterialization"
        )
        candidates[index] = replace(candidates[index], raw_blob_key=None)
        match = "lacks committed raw lineage"
    elif gap == "transport_raw":
        index = next(
            i for i, item in enumerate(candidates)
            if item.error_type == "TransportError"
        )
        candidates[index] = replace(candidates[index], raw_content_hash="unexpected")
        match = "unexpectedly has raw lineage"
    else:
        candidates[0] = replace(candidates[0], error_type="UnknownError")
        match = "error fingerprint changed"

    with pytest.raises(mod.PurgeRefused, match=match):
        mod.validate_candidate_evidence(candidates)


@pytest.mark.unit
def test_transport_error_is_pinned_to_its_specific_key(mod):
    candidates = mod.load_candidates(FakeCursor(mod))
    transport_index = next(
        i for i, item in enumerate(candidates)
        if item.key == mod.TRANSPORT_ERROR_KEY
    )
    deferred_index = next(
        i for i, item in enumerate(candidates)
        if item.key != mod.TRANSPORT_ERROR_KEY
    )
    candidates[transport_index] = replace(
        candidates[transport_index],
        error_type="DeferredMaterialization",
        raw_content_hash="hash",
        raw_blob_key="raw/key",
    )
    candidates[deferred_index] = replace(
        candidates[deferred_index],
        error_type="TransportError",
        raw_content_hash=None,
        raw_blob_key=None,
    )

    with pytest.raises(mod.PurgeRefused, match="only TransportError"):
        mod.validate_candidate_evidence(candidates)


@pytest.mark.unit
def test_refuses_missing_or_not_strictly_newer_replacement(mod):
    replacements = _replacement_rows(mod)
    replacements = [row for row in replacements if row[1] != "last:3"]
    cursor = FakeCursor(mod, replacements=replacements)

    with pytest.raises(mod.PurgeRefused, match="both strictly newer"):
        mod.purge(cursor, apply=True)

    assert not any(sql.lstrip().startswith("DELETE") for sql in cursor.sql_log)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("updated_at", "error"),
    [
        ("2026-07-20T00:03:02.607688+00:00", "both strictly newer"),
        ("2026-07-20T00:03:02.607687+00:00", "both strictly newer"),
        ("2026-07-22T14:00:00", "not timezone-aware"),
        ("not-a-timestamp", "invalid terminal timestamp"),
    ],
)
def test_refuses_equal_older_naive_or_malformed_replacement_updated_at(
    mod,
    updated_at,
    error,
):
    replacements = _replacement_rows(mod)
    index = next(i for i, row in enumerate(replacements) if row[2] == "standings_total")
    row = list(replacements[index])
    row[5] = updated_at
    replacements[index] = tuple(row)

    with pytest.raises(mod.PurgeRefused, match=error):
        mod.purge(FakeCursor(mod, replacements=replacements))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("freshness_key", "error"),
    [
        ("day-2026-07-19", "both strictly newer"),
        ("day-2026-07-18", "both strictly newer"),
        ("final", "not canonical day-YYYY-MM-DD"),
        ("day-2026-02-31", "invalid terminal freshness_key calendar day"),
    ],
)
def test_refuses_old_equal_or_malformed_replacement_freshness(
    mod,
    freshness_key,
    error,
):
    replacements = _replacement_rows(mod)
    index = next(i for i, row in enumerate(replacements) if row[2] == "standings_total")
    row = list(replacements[index])
    row[3] = freshness_key
    replacements[index] = tuple(row)

    with pytest.raises(mod.PurgeRefused, match=error):
        mod.purge(FakeCursor(mod, replacements=replacements))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("endpoint", "status"),
    [
        ("schedule_last", "legitimate_empty"),
        ("standings_total", "legitimate_empty"),
        ("schedule_next", "not_supported"),
    ],
)
def test_refuses_endpoint_incompatible_or_not_supported_replacement_status(
    mod,
    endpoint,
    status,
):
    replacements = _replacement_rows(mod)
    index = next(i for i, row in enumerate(replacements) if row[2] == endpoint)
    row = list(replacements[index])
    row[4] = status
    replacements[index] = tuple(row)

    with pytest.raises(mod.PurgeRefused, match="not valid replacement evidence"):
        mod.purge(FakeCursor(mod, replacements=replacements))


@pytest.mark.unit
def test_terminal_query_and_model_include_required_manifest_invariant_fields(mod):
    cursor = FakeCursor(mod)

    observations = mod.load_terminal_observations(cursor)

    sql = next(
        statement
        for statement in cursor.sql_log
        if statement.lstrip().startswith("SELECT target_type")
    )
    for column in (
        "raw_content_hash",
        "raw_blob_key",
        "http_status",
        "row_count",
    ):
        assert column in sql
    schedule_next = next(
        item for item in observations if item.endpoint == "schedule_next"
    )
    assert schedule_next.raw_content_hash
    assert schedule_next.raw_blob_key
    assert schedule_next.http_status == 404
    assert schedule_next.row_count == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    ("column", "value", "error"),
    [
        (6, None, "lacks committed raw lineage"),
        (7, "", "lacks committed raw lineage"),
        (8, None, "non-empty 2xx HTTP response"),
        (8, 204, "non-empty 2xx HTTP response"),
        (8, 300, "non-empty 2xx HTTP response"),
        (8, 503, "cannot be terminal replacement"),
        (9, 0, "row_count > 0"),
        (9, -1, "row_count must be non-negative"),
        (8, "200", "http_status is not an integer"),
        (9, None, "row_count is not an integer"),
    ],
)
def test_refuses_success_replacement_manifest_invariant_gaps(
    mod,
    column,
    value,
    error,
):
    replacements = _mutate_replacement(
        _replacement_rows(mod),
        endpoint="standings_total",
        column=column,
        value=value,
    )

    with pytest.raises(mod.PurgeRefused, match=error):
        mod.purge(FakeCursor(mod, replacements=replacements))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("column", "value", "error"),
    [
        (6, None, "lacks committed raw lineage"),
        (7, "", "lacks committed raw lineage"),
        (8, 403, "cannot be terminal replacement"),
        (8, 429, "cannot be terminal replacement"),
        (8, 500, "cannot be terminal replacement"),
        (9, -1, "row_count must be non-negative"),
    ],
)
def test_idempotently_clean_path_refuses_invalid_legitimate_empty_evidence(
    mod,
    column,
    value,
    error,
):
    replacements = _mutate_replacement(
        _replacement_rows(mod),
        endpoint="schedule_next",
        column=column,
        value=value,
    )
    cursor = FakeCursor(mod, candidates=[], replacements=replacements)

    with pytest.raises(mod.PurgeRefused, match=error):
        mod.purge(cursor, apply=True)

    assert not any(sql.lstrip().startswith("DELETE") for sql in cursor.sql_log)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("http_status", "row_count"),
    [
        (None, 0),
        (200, 0),
        (204, 0),
        (404, 0),
        (404, 3),
    ],
)
def test_legitimate_empty_accepts_exact_endpoint_manifest_semantics(
    mod,
    http_status,
    row_count,
):
    replacements = _mutate_replacement(
        _replacement_rows(mod),
        endpoint="schedule_next",
        column=8,
        value=http_status,
    )
    replacements = _mutate_replacement(
        replacements,
        endpoint="schedule_next",
        column=9,
        value=row_count,
    )

    result = mod.purge(FakeCursor(mod, replacements=replacements))

    assert result.applied is False
    assert result.already_clean is False


@pytest.mark.unit
def test_refuses_incomplete_world_cup_final_evidence(mod):
    cursor = FakeCursor(mod, final_evidence=(103, 515, 5, 5, 5))

    with pytest.raises(mod.PurgeRefused, match="final event evidence changed"):
        mod.purge(cursor, apply=True)

    assert not any(sql.lstrip().startswith("DELETE") for sql in cursor.sql_log)


@pytest.mark.unit
def test_apply_snapshots_then_uses_one_narrow_delete_and_post_checks(mod, caplog):
    cursor = FakeCursor(mod)

    with caplog.at_level("WARNING"):
        result = mod.purge(cursor, apply=True)

    deletes = [sql for sql in cursor.sql_log if sql.lstrip().startswith("DELETE")]
    assert len(deletes) == 1
    delete = deletes[0]
    assert "DELETE FROM iceberg.ops.sofascore_capture_manifest" in delete
    assert "status = 'retryable_failure'" in delete
    # Every OR arm repeats all six natural-key columns, so no partition-wide
    # or status-wide predicate can accidentally be substituted.
    for column in mod.NATURAL_KEY_COLUMNS:
        assert delete.count(f"{column} =") == 11
    for key in mod.PURGE_ALLOWLIST:
        assert all(f"{column} = '{value}'" in delete for column, value in zip(
            mod.NATURAL_KEY_COLUMNS,
            key.as_tuple(),
        ))
    assert "UPDATE " not in "\n".join(cursor.sql_log)
    assert "DROP TABLE" not in "\n".join(cursor.sql_log)

    snapshot_index = next(
        i for i, sql in enumerate(cursor.sql_log) if "$snapshots" in sql
    )
    delete_index = cursor.sql_log.index(delete)
    assert snapshot_index < delete_index
    assert result.deleted_rows == 11
    assert result.pre_snapshot_id == PRE_SNAPSHOT_ID
    assert result.purge_snapshot_id == PURGE_SNAPSHOT_ID
    assert result.delete_query_id == DELETE_QUERY_ID
    assert result.delete_rowcount == 11
    assert result.snapshot_summary_count == 11
    assert "QUIESCE ALL WRITERS" in result.conditional_rollback_instruction
    assert str(PURGE_SNAPSHOT_ID) in result.conditional_rollback_instruction
    assert (
        "CALL iceberg.system.rollback_to_snapshot("
        f"'ops', 'sofascore_capture_manifest', {PRE_SNAPSHOT_ID})"
        in result.conditional_rollback_instruction
    )
    assert "CONDITIONAL ROLLBACK" in caplog.text
    snapshots = [sql for sql in cursor.sql_log if "$refs" in sql and "$snapshots" in sql]
    assert len(snapshots) == 4
    # Pre/post counts and evidence are both read.
    assert sum("AS total_rows" in sql for sql in cursor.sql_log) == 2
    assert sum("AS protected_terminal_rows" in sql for sql in cursor.sql_log) == 2
    assert sum(sql.lstrip().startswith("WITH final_success") for sql in cursor.sql_log) == 2


@pytest.mark.unit
def test_apply_fails_post_check_when_total_does_not_drop_by_exactly_11(mod):
    cursor = FakeCursor(mod, total_after=19_990)

    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="exactly 11",
    ) as exc_info:
        mod.purge(cursor, apply=True)

    assert cursor.deleted is True
    assert "QUIESCE ALL WRITERS" in (
        exc_info.value.conditional_rollback_instruction
    )


@pytest.mark.unit
def test_apply_fails_post_check_when_protected_terminal_count_changes(mod):
    cursor = FakeCursor(mod, protected_after=776)

    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="protected terminal evidence count changed",
    ):
        mod.purge(cursor, apply=True)

    assert cursor.deleted is True


@pytest.mark.unit
def test_refuses_before_delete_if_main_snapshot_changes_during_preflight(mod):
    before = [
        (
            PRE_SNAPSHOT_ID,
            111,
            "2026-07-21 11:47:42.405 UTC",
            "replace",
            {"trino_query_id": "older-query"},
        ),
        (
            CONCURRENT_SNAPSHOT_ID,
            PRE_SNAPSHOT_ID,
            "2026-07-22 07:59:59.000 UTC",
            "append",
            {"trino_query_id": "concurrent-query"},
        ),
    ]
    cursor = FakeCursor(mod, before_snapshot_rows=before)

    with pytest.raises(mod.PurgeRefused, match="changed during preflight"):
        mod.purge(cursor, apply=True)

    assert cursor.deleted is False


@pytest.mark.unit
@pytest.mark.parametrize("operation", ["append", "overwrite", "replace"])
def test_refuses_direct_child_append_or_rewrite_snapshot(mod, operation):
    cursor = FakeCursor(
        mod,
        after_snapshot_rows=[
            (
                PURGE_SNAPSHOT_ID,
                PRE_SNAPSHOT_ID,
                "2026-07-22 08:00:01.000 UTC",
                operation,
                {"trino_query_id": DELETE_QUERY_ID},
            )
        ],
    )

    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="operation is not delete",
    ) as exc_info:
        mod.purge(cursor, apply=True)

    assert cursor.deleted is True
    assert exc_info.value.conditional_rollback_instruction is None
    assert "outcome is unknown" in str(exc_info.value)


@pytest.mark.unit
def test_refuses_non_direct_snapshot_lineage_as_unknown_outcome(mod):
    cursor = FakeCursor(
        mod,
        after_snapshot_rows=[
            (
                CONCURRENT_SNAPSHOT_ID,
                123,
                "2026-07-22 08:00:01.000 UTC",
                "delete",
                {"trino_query_id": DELETE_QUERY_ID},
            )
        ],
    )

    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="not exactly one direct child",
    ) as exc_info:
        mod.purge(cursor, apply=True)

    assert exc_info.value.conditional_rollback_instruction is None
    assert "outcome is unknown" in str(exc_info.value)


@pytest.mark.unit
@pytest.mark.parametrize("snapshot_query_id", [None, "foreign-delete-query"])
def test_refuses_missing_or_foreign_delete_snapshot_query_id(
    mod,
    snapshot_query_id,
):
    summary = {} if snapshot_query_id is None else {"trino_query_id": snapshot_query_id}
    cursor = FakeCursor(
        mod,
        after_snapshot_rows=[
            (
                PURGE_SNAPSHOT_ID,
                PRE_SNAPSHOT_ID,
                "2026-07-22 08:00:00.000 UTC",
                "delete",
                summary,
            )
        ],
    )

    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="trino_query_id does not own this cursor",
    ) as exc_info:
        mod.purge(cursor, apply=True)

    assert exc_info.value.conditional_rollback_instruction is None
    assert "outcome is unknown" in str(exc_info.value)


@pytest.mark.unit
def test_refuses_cursor_query_id_missing_or_changed_after_delete_fetch(mod):
    cursor = FakeCursor(mod, delete_query_id_after_fetch="different-query")

    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="query_id is missing or changed",
    ) as exc_info:
        mod.purge(cursor, apply=True)

    assert exc_info.value.conditional_rollback_instruction is None


@pytest.mark.unit
def test_refuses_zero_delete_rowcount_before_creating_rollback_instruction(
    mod,
    caplog,
):
    cursor = FakeCursor(mod, delete_rowcount=0)

    with caplog.at_level("WARNING"):
        with pytest.raises(
            mod.PostDeleteVerificationError,
            match="rowcount=0",
        ) as exc_info:
            mod.purge(cursor, apply=True)

    assert exc_info.value.conditional_rollback_instruction is None
    assert "rollback_to_snapshot" not in caplog.text


@pytest.mark.unit
def test_position_delete_summary_count_is_auxiliary_to_dbapi_rowcount(mod):
    cursor = FakeCursor(
        mod,
        after_snapshot_rows=[
            (
                PURGE_SNAPSHOT_ID,
                PRE_SNAPSHOT_ID,
                "2026-07-22 08:00:00.000 UTC",
                "delete",
                {
                    "trino_query_id": DELETE_QUERY_ID,
                    "deleted-records": "4139",
                    "added-position-deletes": "11",
                    "rewritten-delete-files": "1900",
                },
            )
        ],
    )

    result = mod.purge(cursor, apply=True)

    assert result.delete_rowcount == 11
    assert result.snapshot_summary_count == 4139
    assert result.deleted_rows == 11


@pytest.mark.unit
@pytest.mark.parametrize(
    ("cursor_kwargs", "error_text"),
    [
        ({"delete_fetch_error": RuntimeError("fetch failed")}, "fetch failed"),
        (
            {"first_after_snapshot_error": RuntimeError("refs failed")},
            "refs failed",
        ),
    ],
)
def test_delete_or_first_snapshot_failure_reports_unknown_without_rollback(
    mod,
    cursor_kwargs,
    error_text,
):
    cursor = FakeCursor(mod, **cursor_kwargs)

    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="outcome is unknown",
    ) as exc_info:
        mod.purge(cursor, apply=True)

    assert cursor.deleted is True
    assert exc_info.value.conditional_rollback_instruction is None
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert error_text in str(exc_info.value.__cause__)


@pytest.mark.unit
def test_refuses_if_main_advances_after_purge_during_final_verification(mod):
    cursor = FakeCursor(
        mod,
        after_snapshot_rows=[
            (
                PURGE_SNAPSHOT_ID,
                PRE_SNAPSHOT_ID,
                "2026-07-22 08:00:00.000 UTC",
                "delete",
                {"trino_query_id": DELETE_QUERY_ID},
            ),
            (
                CONCURRENT_SNAPSHOT_ID,
                PURGE_SNAPSHOT_ID,
                "2026-07-22 08:00:01.000 UTC",
                "append",
                {"trino_query_id": "concurrent-query"},
            ),
        ],
    )

    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="advanced during final verification",
    ) as exc_info:
        mod.purge(cursor, apply=True)

    assert cursor.deleted is True
    assert exc_info.value.conditional_rollback_instruction is None
    assert "do not run an unconditional rollback" in str(exc_info.value)


@pytest.mark.unit
def test_already_clean_apply_is_idempotent_only_with_valid_evidence(mod):
    cursor = FakeCursor(mod, candidates=[])

    result = mod.purge(cursor, apply=True)

    assert result == mod.PurgeResult(
        applied=True,
        deleted_rows=0,
        already_clean=True,
    )
    assert not any(sql.lstrip().startswith("DELETE") for sql in cursor.sql_log)
    assert sum("$refs" in sql and "$snapshots" in sql for sql in cursor.sql_log) == 2

    missing = [row for row in _replacement_rows(mod) if row[1] != "last:2"]
    with pytest.raises(mod.PurgeRefused, match="both strictly newer"):
        mod.purge(FakeCursor(mod, candidates=[], replacements=missing), apply=True)
    with pytest.raises(mod.PurgeRefused, match="final event evidence changed"):
        mod.purge(
            FakeCursor(mod, candidates=[], final_evidence=(104, 519, 5, 5, 5)),
            apply=True,
        )


@pytest.mark.unit
def test_tls_verification_defaults_to_true(mod, monkeypatch):
    captured = _install_fake_trino(monkeypatch)
    _set_tls_environment(monkeypatch)

    mod.get_conn()

    assert captured["connect_calls"] == 1
    assert captured["kwargs"]["http_scheme"] == "https"
    assert captured["kwargs"]["verify"] is True


@pytest.mark.unit
@pytest.mark.parametrize("variable", ["REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"])
def test_tls_ca_bundle_environment_is_forwarded(mod, monkeypatch, variable):
    captured = _install_fake_trino(monkeypatch)
    _set_tls_environment(monkeypatch, **{variable: "/run/certs/trino-ca.pem"})

    mod.get_conn()

    assert captured["connect_calls"] == 1
    assert captured["kwargs"]["verify"] == "/run/certs/trino-ca.pem"


@pytest.mark.unit
def test_invalid_tls_verify_setting_fails_before_connect(mod, monkeypatch):
    captured = _install_fake_trino(monkeypatch)
    _set_tls_environment(monkeypatch, TRINO_TLS_VERIFY="sometimes")

    with pytest.raises(mod.PurgeRefused, match="TRINO_TLS_VERIFY"):
        mod.get_conn()

    assert captured["connect_calls"] == 0


@pytest.mark.unit
def test_insecure_tls_is_dry_run_only_and_warns(mod, monkeypatch, caplog):
    captured = _install_fake_trino(monkeypatch)
    _set_tls_environment(monkeypatch, TRINO_TLS_VERIFY="false")

    with pytest.raises(mod.PurgeRefused, match="forbidden for --apply"):
        mod.get_conn(allow_insecure=False)
    assert captured["connect_calls"] == 0

    with caplog.at_level("WARNING"):
        mod.get_conn(allow_insecure=True)
    assert captured["connect_calls"] == 1
    assert captured["kwargs"]["verify"] is False
    assert "read-only dry-run only" in caplog.text


@pytest.mark.unit
def test_main_rejects_insecure_apply_before_opening_connection(
    mod,
    monkeypatch,
):
    captured = _install_fake_trino(monkeypatch)
    _set_tls_environment(monkeypatch, TRINO_TLS_VERIFY="no")

    assert mod.main(["--apply"]) == 2
    assert captured["connect_calls"] == 0


@pytest.mark.unit
def test_main_reports_unknown_delete_outcome_without_rollback(
    mod,
    monkeypatch,
    caplog,
):
    cursor = FakeCursor(mod, delete_fetch_error=RuntimeError("network lost"))
    connection = FakeConnection(cursor)
    monkeypatch.setattr(mod, "get_conn", lambda **_kwargs: connection)

    with caplog.at_level("WARNING"):
        assert mod.main(["--apply"]) == 3

    assert "outcome is unknown" in caplog.text
    assert "No rollback instruction is safe" in caplog.text
    assert "ROLLBACK IS CONDITIONAL" not in caplog.text
    assert "rollback_to_snapshot" not in caplog.text
    assert cursor.closed is True
    assert connection.closed is True


@pytest.mark.unit
def test_main_closes_cursor_and_connection(mod, monkeypatch):
    cursor = FakeCursor(mod)
    connection = FakeConnection(cursor)
    monkeypatch.setattr(mod, "get_conn", lambda **_kwargs: connection)

    assert mod.main([]) == 0
    assert cursor.closed is True
    assert connection.closed is True
