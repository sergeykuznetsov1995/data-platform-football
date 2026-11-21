"""Offline C1 change detection, closure batching and publication regressions."""

from contextlib import contextmanager
from datetime import date, datetime
import json
import re
import sys
import threading
from types import SimpleNamespace

import pytest

from scrapers.fotmob import c1_inputs as c1


pytestmark = pytest.mark.unit
DAY = date(2026, 11, 13)
OLD, NEW = 902, 7
STRING = r"'(?:''|[^'])*'"
VALUE = STRING + r"|-?\d+"
PAIR_TERM = re.compile(
    r"\(source_season_key=(" + STRING + r") AND competition_id IN "
    r"\(((?:" + VALUE + r")(?:,(?:" + VALUE + r"))*)\)\)"
)
ID_PREFIX = "CAST(match_id AS varchar) IN ("
FORWARD = "SELECT DISTINCT match_id FROM "
REVERSE = "SELECT DISTINCT competition_id, source_season_key FROM "


def decode(value):
    return value[1:-1].replace("''", "'") if value.startswith("'") else value


def parse_pairs(predicate, *, physical):
    if predicate == "false":
        return set()
    assert re.fullmatch(r"\(term(?: OR term)*\)", PAIR_TERM.sub("term", predicate))
    pairs = set()
    for season, values in PAIR_TERM.findall(predicate):
        for value in re.findall(VALUE, values):
            assert bool(re.fullmatch(r"-?\d+", value)) == physical
            pairs.add((decode(value), decode(season)))
    return pairs


def parse_ids(predicate):
    assert re.fullmatch(
        re.escape(ID_PREFIX) + STRING + r"(?:," + STRING + r")*\)", predicate
    )
    return {decode(value) for value in re.findall(STRING, predicate[len(ID_PREFIX):-1])}


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 14, 12, tzinfo=tz)


class ClosureManager:
    """Evaluate closure predicates against synthetic snapshot/target relations."""

    def __init__(self, pairs=(), *, changed=(), rows=None, target=(), seed_ids=(),
                 budget=200_000, fail_batch=None, scope_decisions=None, seasons_latest=None):
        self.pairs = set(pairs)
        self.changed = set(changed)
        self.seed_ids = set(seed_ids)
        self.rows = rows or {}
        self.target = list(target)
        self.budget = budget
        self.fail_batch = fail_batch
        self.old = {table: OLD for table in c1.SOURCES}
        self.current = {table: NEW for table in c1.SOURCES}
        self.files = {table: {} for table in (c1.MATCHES, c1.PAYLOADS)}
        self.files[c1.MATCHES] = {pair: "stable" for pair in self.pairs}
        self.scope_decisions = ({cid: "included" for cid, _ in self.pairs}
                                if scope_decisions is None else dict(scope_decisions))
        self.seasons_latest = (dict.fromkeys(self.pairs, True)
                               if seasons_latest is None else dict(seasons_latest))
        checkpoint = {
            "contract_version": c1.CONTRACT_VERSION,
            "detection": 2,
            "scope_decisions": [list(row) for row in sorted(self.scope_decisions.items())],
            "seasons_latest": c1._encoded_pairs(self.seasons_latest),
            "parser_versions": [c1.PARSER_VERSION, c1.LEGACY_PARSER_VERSION],
            "first_day": str(DAY), "last_day": str(DAY), "snapshots": self.old,
            "files": {table: c1._encoded_pairs(values) for table, values in self.files.items()},
        }
        for pair in self.changed:
            self.files[c1.MATCHES][pair] = "changed"
        self.set_checkpoint(checkpoint)
        self.queries, self.closure, self.scans = [], [], []
        self.writes, self.staged = [], []
        self.existing_pairs = None

    def set_checkpoint(self, checkpoint):
        self.controls = [(None, None, None, "checkpoint", "old", True, json.dumps(checkpoint))]

    def execute_query(self, sql):
        self.queries.append(sql)
        if sql.startswith((FORWARD, REVERSE)):
            self.closure.append(sql)
            if len(sql.encode("utf-8")) >= 1_000_000:
                raise RuntimeError("QUERY_TEXT_TOO_LARGE")
            if len(self.closure) == self.fail_batch:
                raise RuntimeError("synthetic closure batch failure")
            forward = sql.startswith(FORWARD)
            selection = sql[len(FORWARD if forward else REVERSE):]
            if selection.startswith("("):
                match = re.fullmatch(
                    r"\(SELECT \* FROM iceberg\.bronze\.(\w+) "
                    r"FOR VERSION AS OF (\d+) WHERE (.*)\)", selection, re.S
                )
                assert match
                table, version, predicate = match.groups()
                version = int(version)
                assert table in (c1.MATCHES, c1.PAYLOADS)
                assert version in (OLD, NEW)
                if not forward:
                    assert (table, version) == (c1.MATCHES, NEW)
                records = self.rows.get((table, version), [])
            else:
                prefix = (f"{c1.TABLE} WHERE kind='input' "
                          f"AND deadline_day >= DATE '{DAY}' AND ")
                assert selection.startswith(prefix)
                table, version = c1.TABLE, None
                predicate = selection[len(prefix):]
                records = self.target
            pair_predicate, id_predicate = None, None
            if forward:
                pair_predicate = predicate
            elif table == c1.TABLE or predicate.startswith(ID_PREFIX):
                id_predicate = predicate
            else:
                # Accept the legacy pair + ID form so regressions fail on
                # query counts and repeated IDs, not on fake-engine parsing.
                pair_predicate, tail = predicate.rsplit(" AND " + ID_PREFIX, 1)
                id_predicate = ID_PREFIX + tail
            if pair_predicate is not None:
                assert len(pair_predicate.encode("utf-8")) <= self.budget
            if id_predicate is not None:
                if table == c1.MATCHES:
                    assert len(sql.encode("utf-8")) <= getattr(c1, "REVERSE_QUERY_BYTES", 900_000)
                else:
                    assert len(id_predicate.encode("utf-8")) <= self.budget
            pairs = (parse_pairs(pair_predicate, physical=table == c1.MATCHES)
                     if pair_predicate is not None else None)
            ids = parse_ids(id_predicate) if id_predicate is not None else None
            self.scans.append((forward, table, version, pairs, ids))
            # Keep duplicate results to exercise set union across batches.
            selected = [(cid, season, mid) for cid, season, mid in records
                        if (pairs is None or (str(cid), str(season)) in pairs)
                        and (ids is None or str(mid) in ids)]
            return [(mid,) for _, _, mid in selected] if forward else [r[:2] for r in selected]
        if sql.startswith("SELECT competition_id, source_season_key, match_id, kind, generation"):
            return list(self.controls)
        if sql.startswith("SELECT deadline_day, competition_id, source_season_key, match_id, kind"):
            prefix = (f"SELECT deadline_day, competition_id, source_season_key, match_id, kind "
                      f"FROM {c1.TABLE} WHERE kind='input' AND deadline_day >= DATE '{DAY}' AND ")
            assert sql.startswith(prefix)
            self.existing_pairs = parse_pairs(sql[len(prefix):], physical=False)
            return []
        if sql.startswith("WITH candidates AS"):
            return []  # No eligible deadline rows; publish days and checkpoint.
        if sql.startswith(("CREATE TABLE ", "INSERT INTO ", "MERGE INTO ", "DROP TABLE ")):
            self.writes.append(sql)
            if sql.startswith("MERGE INTO "):
                self.controls = [(r[1], r[2], r[3], r[4], r[12], r[13], r[14])
                                 for r in self.staged
                                 if r[13] and r[4] in ("checkpoint", "checkpoint_part")]
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


def setup_refresh(monkeypatch, manager):
    def pinned(values, pins):
        assert pins == manager.current
        return dict(values)

    monkeypatch.setattr(c1, "datetime", FrozenDatetime)
    monkeypatch.setattr(c1, "snapshots", lambda _: dict(manager.current))
    monkeypatch.setattr(c1, "partition_fingerprints", lambda _, table: manager.files[table])
    monkeypatch.setattr(c1, "scope_decisions", lambda _, pins: pinned(manager.scope_decisions, pins))
    monkeypatch.setattr(c1, "seasons_latest", lambda _, pins: pinned(manager.seasons_latest, pins))
    monkeypatch.setattr(c1, "manifest_changes", lambda *_: iter(
        (None, None, "match", mid) for mid in sorted(manager.seed_ids)
    ))
    real_stage_rows = c1._stage_rows

    def stage_rows(manager_arg, stage, rows):
        rows = list(rows)
        manager.staged = rows
        return real_stage_rows(manager_arg, stage, rows)

    monkeypatch.setattr(c1, "_stage_rows", stage_rows)


def full_set_oracle(manager):
    affected, ids, iterations = set(manager.changed), set(manager.seed_ids), []
    while True:
        known = set(affected)
        for records in [*manager.rows.values(), manager.target]:
            ids.update(mid for cid, season, mid in records if (cid, season) in affected)
        iterations.append((known, set(ids)))
        affected.update((cid, season) for cid, season, mid in
                        manager.rows.get((c1.MATCHES, NEW), [])
                        if mid in ids and (cid, season) in manager.pairs)
        affected.update((cid, season) for cid, season, mid in manager.target if mid in ids)
        if known == affected:
            return affected, ids, iterations


@pytest.mark.parametrize("reader,table,columns,rows,expected", [
    (c1.scope_decisions, c1.SCOPE, "competition_id, decision",
     [(10, "included"), (20, "excluded")], {"10": "included", "20": "excluded"}),
    (c1.seasons_latest, c1.SEASONS, "competition_id, source_season_key, is_latest",
     [(10, 2025, False), (10, 2026, True)], {("10", "2025"): False, ("10", "2026"): True}),
])
def test_semantic_state_reads_one_current_pinned_projection(reader, table, columns, rows, expected):
    pins = {source: i for i, source in enumerate(c1.SOURCES, 1)}
    queries = []

    def execute_query(sql):
        queries.append(sql)
        return rows

    assert reader(SimpleNamespace(execute_query=execute_query), pins) == expected
    assert queries == [f"SELECT {columns} FROM ({c1.current_sql(table, pins)})"]


@pytest.mark.parametrize("invalid", [None, 0, "false"])
def test_seasons_latest_refuses_ambiguous_source_flags(invalid):
    manager = SimpleNamespace(execute_query=lambda _: [(10, "2026", invalid)])
    with pytest.raises(RuntimeError, match="invalid source is_latest"):
        c1.seasons_latest(manager, {c1.SEASONS: NEW, c1.MANIFEST: OLD})


@pytest.mark.parametrize("old,new", [
    ({"10": "included"}, {"10": "excluded"}),
    ({}, {"10": "included"}),
    ({"10": "included"}, {}),
])
def test_refresh_scope_change_affects_just_that_competition(monkeypatch, old, new):
    competition = {("10", "2025"), ("10", "2026")}
    stable = {"20": "included"}
    manager = ClosureManager(competition | {("20", "2026")}, scope_decisions=stable | old)
    manager.scope_decisions = stable | new
    setup_refresh(monkeypatch, manager)

    result = c1.refresh(manager)

    assert result["seasons"] == len(competition)
    assert manager.existing_pairs == competition


@pytest.mark.parametrize("old,new,other_latest", [
    (True, False, True), (False, True, True), (None, False, True), (False, None, True),
    (True, False, False), (False, True, False),
])
def test_refresh_season_change_expands_only_if_any_latest_flips(monkeypatch, old, new, other_latest):
    pair, sibling, other = ("10", "2026"), ("10", "2025"), ("20", "2026")
    stable = {sibling: other_latest, other: True}
    manager = ClosureManager([pair, sibling, other], seasons_latest=(
        stable if old is None else {**stable, pair: old}
    ))
    manager.seasons_latest = stable if new is None else {**stable, pair: new}
    setup_refresh(monkeypatch, manager)

    result = c1.refresh(manager)

    expected = {pair} if other_latest else {pair, sibling}
    assert manager.existing_pairs == expected
    assert result["seasons"] == len(expected)


@pytest.mark.parametrize("target_type", ["competition_profile", "competition_seasons"])
def test_refresh_keeps_manifest_and_payload_changes_with_stable_semantics(monkeypatch, target_type):
    competition = {("10", "2025"), ("10", "2026")}
    payload = ("20", "2025")
    manager = ClosureManager(competition | {payload, ("20", "2026")})
    manager.files[c1.PAYLOADS][payload] = "new"
    setup_refresh(monkeypatch, manager)
    monkeypatch.setattr(c1, "manifest_changes", lambda *_: iter([("10", None, target_type, None)]))

    c1.refresh(manager)

    assert manager.existing_pairs == competition | {payload}


@pytest.mark.parametrize("detection", [None, 1])
def test_refresh_migrates_unknown_detection_in_one_full_pass(monkeypatch, detection):
    pairs = {("20", "2025"), ("10", "2026"), ("10", "2025")}
    manager = ClosureManager(pairs, seasons_latest={pair: pair[1] == "2026" for pair in pairs})
    previous = c1._read_checkpoint(manager.controls)
    for name in ("detection", "scope_decisions", "seasons_latest"):
        del previous[name]
    if detection is not None:
        previous["detection"] = detection
    for table in (c1.SCOPE, c1.SEASONS):
        previous["files"][table] = [["10", "", "old"], ["20", "", "old"]]
    manager.set_checkpoint(previous)
    setup_refresh(monkeypatch, manager)

    assert c1.refresh(manager)["seasons"] == len(pairs)
    assert manager.existing_pairs == pairs
    migrated = c1._read_checkpoint(manager.controls)
    assert migrated["detection"] == 2
    assert migrated["contract_version"] == 1
    assert migrated["scope_decisions"] == [["10", "included"], ["20", "included"]]
    assert migrated["seasons_latest"] == [
        ["10", "2025", False], ["10", "2026", True], ["20", "2025", False],
    ]
    assert set(migrated["files"]) == {c1.MATCHES, c1.PAYLOADS}
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.writes) == 1

    assert c1.refresh(manager)["seasons"] == 0
    assert manager.existing_pairs == set()
    assert c1._read_checkpoint(manager.controls) == migrated


@pytest.mark.parametrize("field,invalid", [
    ("scope_decisions", None), ("scope_decisions", {}), ("scope_decisions", ["ab"]),
    ("scope_decisions", [[10, "included"]]), ("scope_decisions", [["10", False]]),
    ("scope_decisions", [["10", "included"], ["10", "excluded"]]),
    ("seasons_latest", None), ("seasons_latest", [["10", "2025"]]),
    ("seasons_latest", [["10", 2025, True]]), ("seasons_latest", [["10", "2025", 0]]),
    ("seasons_latest", [["10", "2025", True], ["10", "2025", False]]),
])
def test_refresh_rejects_malformed_semantic_checkpoint_before_staging(monkeypatch, field, invalid):
    manager = ClosureManager()
    checkpoint = c1._read_checkpoint(manager.controls)
    if invalid is None:
        del checkpoint[field]
    else:
        checkpoint[field] = invalid
    manager.set_checkpoint(checkpoint)
    setup_refresh(monkeypatch, manager)

    with pytest.raises(RuntimeError, match=f"C1 invalid checkpoint {field}"):
        c1.refresh(manager)
    assert manager.writes == []


def test_refresh_large_id_inventory_stays_below_query_limit(monkeypatch):
    pairs = {(str(1000 + i), "2026/27") for i in range(3740)}
    seed = min(pairs)
    ids = [str(10_000_000 + i) for i in range(100_000)]
    manager = ClosureManager(pairs, changed={seed}, rows={
        (c1.PAYLOADS, OLD): [(*seed, mid) for mid in ids],
    })
    setup_refresh(monkeypatch, manager)
    unbatched = REVERSE + c1.source_sql(
        c1.MATCHES, manager.current,
        ID_PREFIX + ",".join(map(c1.literal, ids)) + ")",
    )
    assert len(unbatched.encode("utf-8")) > 1_000_000
    legacy_ids = c1._id_predicates(ids)
    pair_chunks = c1._pair_predicates(pairs, physical=True)
    assert len(legacy_ids) == 6
    assert [len(parse_pairs(sql, physical=True)) for sql in pair_chunks] == [3225, 515]
    assert len(pair_chunks) * len(legacy_ids) == 12

    result = c1.refresh(manager)

    assert result["seasons"] == 1
    assert manager.existing_pairs == {seed}
    assert max(len(sql.encode("utf-8")) for sql in manager.closure) < 1_000_000
    reverse = [scan for scan in manager.scans if not scan[0] and scan[1] == c1.MATCHES]
    assert len(reverse) == 2
    assert all(scan[3] is None for scan in reverse)
    assert set().union(*(scan[4] for scan in reverse)) == set(ids)
    assert sum(len(scan[4]) for scan in reverse) == len(ids)
    reverse_sql = [sql for sql in manager.closure
                   if sql.startswith(REVERSE + "(SELECT * FROM " + c1.PREFIX + c1.MATCHES)]
    assert len(reverse_sql) == 2
    assert all(column not in sql.split(" WHERE ", 1)[1]
               for sql in reverse_sql for column in ("competition_id", "source_season_key"))
    assert max(len(sql.encode("utf-8")) for sql in reverse_sql) <= c1.REVERSE_QUERY_BYTES
    assert len([scan for scan in manager.scans if not scan[0] and scan[1] == c1.TABLE]) == 6
    checkpoint = c1._read_checkpoint(manager.controls)
    assert checkpoint["snapshots"] == manager.current
    assert dict(checkpoint["scope_decisions"]) == manager.scope_decisions
    assert c1._decoded_pairs(checkpoint["seasons_latest"]) == manager.seasons_latest
    assert len(manager.controls) > 1
    assert all(len(row[-1].encode()) <= 32_000 for row in manager.controls)
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.writes) == 1


def chain_manager(*, fail_batch=None):
    a, b, c, d, e, f, unused = [(str(i * 10), f"season-{i}'雪") for i in range(1, 8)]
    target_only = ("90", "target'雪")
    old_id, new_id, old_payload, new_payload, bridge, next_id = [
        name + "'雪-" + "x" * 25
        for name in ("old", "new", "op", "np", "bridge", "next")
    ]
    return ClosureManager(
        {a, b, c, d, e, f, unused}, changed={a}, budget=90, fail_batch=fail_batch,
        rows={
            (c1.MATCHES, OLD): [(*a, old_id), (*a, old_id)],
            (c1.MATCHES, NEW): [(*a, new_id), (*b, old_id), (*c, new_id),
                                (*d, old_payload), (*e, new_payload), (*f, next_id)],
            (c1.PAYLOADS, OLD): [(*b, old_payload), (*b, old_payload)],
            (c1.PAYLOADS, NEW): [(*c, new_payload)],
        },
        target=[(*b, bridge), (*target_only, bridge), (*target_only, bridge),
                (*target_only, next_id)],
    )


def test_refresh_cross_batch_fixed_point_matches_full_set_oracle(monkeypatch):
    manager = chain_manager()
    setup_refresh(monkeypatch, manager)
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", manager.budget)
    expected, expected_ids, iterations = full_set_oracle(manager)
    assert len(iterations) == 4
    assert len(expected) == 7

    result = c1.refresh(manager)

    assert result["seasons"] == len(expected)
    assert manager.existing_pairs == expected
    # Every iteration revisits the FULL affected set in table/snapshot order.
    expected_forward = []
    expected_target = []
    for affected, full_ids in iterations:
        for table in (c1.MATCHES, c1.PAYLOADS):
            for version in (OLD, NEW):
                expected_forward.extend((table, version, {pair}) for pair in sorted(affected))
        expected_forward.extend((c1.TABLE, None, {pair}) for pair in sorted(affected))
        expected_target.extend(parse_ids(predicate) for predicate in c1._id_predicates(full_ids))
    assert [(t, v, pairs) for forward, t, v, pairs, _ in manager.scans if forward] == expected_forward
    assert [ids for forward, t, _, _, ids in manager.scans
            if not forward and t == c1.TABLE] == expected_target
    reverse = [scan for scan in manager.scans if not scan[0] and scan[1] == c1.MATCHES]
    searched = set()
    for _, _, _, _, ids in reverse:
        assert searched.isdisjoint(ids)
        searched.update(ids)
    assert searched == expected_ids
    assert all(scan[3] is None for scan in reverse)
    assert c1._read_checkpoint(manager.controls)["snapshots"] == manager.current


def test_refresh_reverse_source_keeps_only_canonical_pairs_in_domain(monkeypatch):
    pair = ("10", "2026/27")
    manager = ClosureManager({pair}, seed_ids={"100"}, rows={
        (c1.MATCHES, NEW): [
            (10, pair[1], 100), (20, pair[1], 100),
            (None, pair[1], 100), (10, None, 100), (10, pair[1], 100),
        ],
    })
    setup_refresh(monkeypatch, manager)

    result = c1.refresh(manager)

    assert result["seasons"] == 1
    assert manager.existing_pairs == {pair}
    reverse = [scan for scan in manager.scans if not scan[0] and scan[1] == c1.MATCHES]
    assert [(scan[3], scan[4]) for scan in reverse] == [(None, {"100"})]
    assert all(pairs == {pair} for forward, _, _, pairs, _ in manager.scans if forward)
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.writes) == 1


@pytest.mark.parametrize("mode", ["unchanged", "pairs_without_ids", "ids_without_pairs"])
def test_refresh_empty_sets_do_not_issue_empty_closure_queries(monkeypatch, mode):
    pair = ("10", "2026/27")
    manager = ClosureManager(
        () if mode == "ids_without_pairs" else {pair},
        changed={pair} if mode == "pairs_without_ids" else (),
        seed_ids={"100"} if mode == "ids_without_pairs" else (),
    )
    setup_refresh(monkeypatch, manager)

    result = c1.refresh(manager)

    assert result["seasons"] == (1 if mode == "pairs_without_ids" else 0)
    if mode == "unchanged":
        assert manager.closure == []
        assert manager.existing_pairs == set()
        assert not any("fotmob_c1_candidates__stg_" in sql for sql in manager.queries)
        assert [row[4] for row in manager.staged] == ["day", "checkpoint"]
        assert sum(sql.startswith("MERGE INTO ") for sql in manager.writes) == 1
    elif mode == "pairs_without_ids":
        assert len(manager.closure) == 5
        assert all(sql.startswith(FORWARD) for sql in manager.closure)
    else:
        assert len(manager.closure) == 1
        assert manager.closure[0].startswith(REVERSE + c1.TABLE)
    if mode != "pairs_without_ids":
        assert any(sql.endswith("AND false") for sql in manager.queries)


@pytest.mark.parametrize("physical", [False, True])
@pytest.mark.parametrize("same_season", [False, True])
def test_pair_batch_boundaries_determinism_and_escaped_bytes(monkeypatch, physical, same_season):
    pairs = [("10", "a'雪"), ("20", "a'雪" if same_season else "b'é")]
    single = [c1.pairs_sql([pair], physical=physical) for pair in pairs]
    boundary = sum(len(sql.encode("utf-8")) for sql in single) + 2
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", boundary)
    expected = [c1.pairs_sql(pairs, physical=physical)]
    assert c1._pair_predicates(pairs, physical=physical) == expected
    assert c1._pair_predicates(list(reversed(pairs)), physical=physical) == expected
    assert len(expected[0].encode("utf-8")) <= boundary

    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", boundary - 1)
    chunks = c1._pair_predicates(pairs, physical=physical)
    assert chunks == single
    assert set().union(*(parse_pairs(sql, physical=physical) for sql in chunks)) == set(pairs)
    assert c1._pair_predicates([], physical=physical) == []


def test_id_batch_boundaries_determinism_and_escaped_bytes(monkeypatch):
    ids = ["a'雪", "bé"]
    combined = ID_PREFIX + ",".join(map(c1.literal, ids)) + ")"
    boundary = len(combined.encode("utf-8"))
    assert boundary > len(combined)
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", boundary)
    assert c1._id_predicates(ids) == [combined]
    assert c1._id_predicates(list(reversed(ids))) == [combined]

    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", boundary - 1)
    chunks = c1._id_predicates(ids)
    assert chunks == [ID_PREFIX + c1.literal(mid) + ")" for mid in ids]
    assert set().union(*(parse_ids(sql) for sql in chunks)) == set(ids)
    assert c1._id_predicates([]) == []


def test_reverse_query_byte_boundary_and_determinism(monkeypatch):
    pairs = [("10", "a'雪"), ("20", "a'雪")]
    ids = ["a'雪", "bé"]
    manager = ClosureManager(pairs)
    combined = REVERSE + c1.source_sql(
        c1.MATCHES, manager.current,
        ID_PREFIX + ",".join(map(c1.literal, ids)) + ")",
    )
    boundary = len(combined.encode("utf-8"))
    assert boundary > len(combined)
    monkeypatch.setattr(c1, "REVERSE_QUERY_BYTES", boundary)
    plan = c1._reverse_matches_plan(pairs, ids, manager.current)
    assert plan == [combined]
    assert c1._reverse_matches_plan(
        list(reversed(pairs)), list(reversed(ids)), manager.current
    ) == plan
    for sql in plan:
        manager.execute_query(sql)
    assert len(manager.closure) == 1
    assert len(manager.closure[0].encode("utf-8")) == c1.REVERSE_QUERY_BYTES

    monkeypatch.setattr(c1, "REVERSE_QUERY_BYTES", boundary - 1)
    manager = ClosureManager(pairs)
    plan = c1._reverse_matches_plan(pairs, ids, manager.current)
    assert plan == [REVERSE + c1.source_sql(
        c1.MATCHES, manager.current,
        ID_PREFIX + c1.literal(mid) + ")",
    ) for mid in ids]
    assert c1._reverse_matches_plan(
        list(reversed(pairs)), list(reversed(ids)), manager.current
    ) == plan
    for sql in plan:
        manager.execute_query(sql)
    assert len(manager.closure) == 2
    assert all(len(sql.encode("utf-8")) <= c1.REVERSE_QUERY_BYTES for sql in plan)
    assert set().union(*(scan[4] for scan in manager.scans)) == set(ids)
    assert all(scan[3] is None for scan in manager.scans)
    assert c1._reverse_matches_plan([], ids, manager.current) == []
    assert c1._reverse_matches_plan(pairs, [], manager.current) == []


@pytest.mark.parametrize("kind", ["pair", "physical_pair", "id"])
def test_oversized_literal_fails_before_any_batch_is_executed(monkeypatch, kind):
    manager = ClosureManager()
    if kind == "id":
        oversized = "z'雪" * 20
        build = c1._id_predicates
        values = ["a", oversized]
        single = ID_PREFIX + c1.literal(oversized) + ")"
    else:
        physical = kind == "physical_pair"
        oversized = ("20", "z'雪" * 20)
        build = lambda values: c1._pair_predicates(values, physical=physical)
        values = [("10", "a"), oversized]
        single = c1.pairs_sql([oversized], physical=physical)
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", len(single.encode("utf-8")))
    assert build([oversized]) == [single]
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", len(single.encode("utf-8")) - 1)

    with pytest.raises(RuntimeError, match="C1 closure .* exceeds predicate limit"):
        for predicate in build(values):
            manager.execute_query(FORWARD + predicate)

    assert manager.queries == []


@pytest.mark.parametrize("kind", ["pair", "id"])
def test_refresh_oversized_literal_stops_before_closure_queries(monkeypatch, kind):
    pair = ("10", "'雪" * 30)
    manager = ClosureManager(
        {pair} if kind == "pair" else (),
        changed={pair} if kind == "pair" else (),
        seed_ids={"'雪" * 30} if kind == "id" else (),
        budget=90,
    )
    controls = list(manager.controls)
    setup_refresh(monkeypatch, manager)
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", manager.budget)

    with pytest.raises(RuntimeError, match="C1 closure .* exceeds predicate limit"):
        c1.refresh(manager)

    assert manager.closure == []
    assert manager.writes == []
    assert manager.staged == []
    assert manager.controls == controls


def test_refresh_reverse_plan_ignores_pair_predicate_size(monkeypatch):
    pairs = [("10", "z'雪" * 30)]
    ids = ["a'雪"]
    manager = ClosureManager(pairs, seed_ids=ids, budget=90)
    setup_refresh(monkeypatch, manager)
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", manager.budget)
    with pytest.raises(RuntimeError, match="C1 closure pair exceeds predicate limit"):
        c1._pair_predicates(pairs, physical=True)
    sql = REVERSE + c1.source_sql(
        c1.MATCHES, manager.current, ID_PREFIX + c1.literal(ids[0]) + ")",
    )
    monkeypatch.setattr(c1, "REVERSE_QUERY_BYTES", len(sql.encode("utf-8")))
    assert c1._reverse_matches_plan(pairs, ids, manager.current) == [sql]

    result = c1.refresh(manager)

    assert result["seasons"] == 0
    assert manager.existing_pairs == set()
    assert manager.closure[0] == sql
    assert manager.scans[0] == (False, c1.MATCHES, NEW, None, set(ids))
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.writes) == 1


def test_refresh_reverse_plan_oversized_id_prevents_execution(monkeypatch):
    pair = ("10", "a")
    ids = ["a", "z'雪" * 5]
    manager = ClosureManager([pair], seed_ids=ids, budget=90)
    controls = list(manager.controls)
    setup_refresh(monkeypatch, manager)
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", manager.budget)
    assert c1._id_predicates(ids)
    first_sql = REVERSE + c1.source_sql(
        c1.MATCHES, manager.current,
        ID_PREFIX + c1.literal(ids[0]) + ")",
    )
    monkeypatch.setattr(c1, "REVERSE_QUERY_BYTES", len(first_sql.encode("utf-8")))
    assert c1._reverse_matches_plan([pair], ids[:1], manager.current) == [first_sql]

    with pytest.raises(RuntimeError, match="C1 closure match ID exceeds predicate limit"):
        c1.refresh(manager)

    assert manager.closure == []
    assert manager.writes == []
    assert manager.staged == []
    assert manager.controls == controls


def test_refresh_reverse_plan_checks_actual_full_query_bytes(monkeypatch):
    pair = ("10", "a'雪")
    ids = ["a'雪"]
    manager = ClosureManager([pair], seed_ids=ids)
    controls = list(manager.controls)
    setup_refresh(monkeypatch, manager)
    sql, = c1._reverse_matches_plan([pair], ids, manager.current)
    monkeypatch.setattr(c1, "REVERSE_QUERY_BYTES", len(sql.encode("utf-8")) - 1)
    real_id_predicates = c1._id_predicates
    monkeypatch.setattr(c1, "_id_predicates", lambda values, ceiling=None: real_id_predicates(values))

    with pytest.raises(RuntimeError, match="C1 closure reverse MATCHES query exceeds query limit"):
        c1.refresh(manager)

    assert manager.closure == []
    assert manager.writes == []
    assert manager.staged == []
    assert manager.controls == controls


@pytest.mark.parametrize("fail_batch", [2, 6, 8, 20, 24, 50])
def test_refresh_mid_loop_batch_failure_prevents_publication(monkeypatch, fail_batch):
    manager = chain_manager(fail_batch=fail_batch)
    controls = list(manager.controls)
    setup_refresh(monkeypatch, manager)
    monkeypatch.setattr(c1, "CLOSURE_PREDICATE_BYTES", manager.budget)

    with pytest.raises(RuntimeError, match="synthetic closure batch failure"):
        c1.refresh(manager)

    assert len(manager.closure) == fail_batch
    if fail_batch in (6, 24):
        assert manager.closure[-1].startswith(
            REVERSE + "(SELECT * FROM " + c1.PREFIX + c1.MATCHES
        )
    assert manager.writes == []
    assert manager.staged == []
    assert manager.controls == controls
    assert manager.existing_pairs is None


@pytest.mark.parametrize("bootstrap", [False, True])
def test_refresh_locks_only_shared_writes_and_unbinds_released_lease(monkeypatch, bootstrap):
    pair = ("10", "2026/27")
    manager = ClosureManager([pair], changed={pair})
    setup_refresh(monkeypatch, manager)
    if bootstrap:
        monkeypatch.setattr(c1, "C1_MAX_LOCK_HOLD_SECONDS", 0.0)
    authority = SimpleNamespace(held=False)
    authority.check = lambda: pytest.fail("unexpected direct authority check") if not authority.held else None
    events = []
    manager.authority = None

    def set_write_authority(value):
        manager.authority = value

    manager.set_write_authority = set_write_authority

    @contextmanager
    def write_lease():
        assert not authority.held
        authority.held = True
        events.append(("acquire", True))
        try:
            yield authority
        finally:
            authority.held = False
            events.append(("release", False))

    real_execute = manager.execute_query

    def execute_query(sql):
        if sql == (
            f"SELECT DISTINCT competition_id, source_season_key FROM {c1.TABLE} "
            f"WHERE kind='input' AND deadline_day >= DATE '{DAY}'"
        ):
            return []
        if sql.startswith(("CREATE TABLE ", "INSERT INTO ", "MERGE INTO ", "DROP TABLE ")):
            kind = (
                "candidate" if "fotmob_c1_candidates" in sql
                else "bootstrap" if sql.startswith(f"CREATE TABLE IF NOT EXISTS {c1.TABLE} ")
                else "publication"
            )
            events.append((kind, authority.held, manager.authority is authority))
        return real_execute(sql)

    manager.execute_query = execute_query

    def snapshots(manager_arg):
        assert (manager_arg is manager) == (bootstrap or not authority.held)
        events.append(("snapshots", authority.held))
        return dict(manager_arg.current)

    monkeypatch.setattr(c1, "snapshots", snapshots)
    kwargs = {"first_day": DAY, "last_day": DAY} if bootstrap else {}
    result = c1.refresh(manager, writer_lock=write_lease, **kwargs)

    assert result["rows"] > 0
    assert not authority.held
    assert manager.authority is None
    assert sum(event[0] == "acquire" for event in events) == 1
    assert [event[1] for event in events if event[0] == "candidate"] == (
        [True, True] if bootstrap else [False, False]
    )
    publication = [event for event in events if event[0] == "publication"]
    assert publication and all(event[1] and event[2] for event in publication)
    assert [event[1] for event in events if event[0] == "snapshots"] == (
        [True, True] if bootstrap else [False, True]
    )
    if bootstrap:
        assert any(
            event[0] == "bootstrap" and event[1] and event[2] for event in events
        )


@pytest.fixture
def clock(monkeypatch):
    clock = SimpleNamespace(now=0.0, sleeps=[])

    def sleep(seconds):
        clock.sleeps.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(c1, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(c1, "_sleep", sleep)
    return clock


@pytest.mark.parametrize("mode,budget,sleeps", [
    ("retry", 900.0, [20.0]), ("exhausted", 30.0, []), ("exhausted_in_backoff", 50.0, [20.0]),
    ("lost", 900.0, []), ("busy_after_acquisition", 900.0, []),
])
def test_build_writer_lock_retries_only_busy_acquisitions(monkeypatch, clock, mode, budget, sleeps):
    from scrapers.fotmob import writer_lock as locks

    attempts = []
    lease = object()
    failure = locks.WriterLockLost("lost") if mode == "lost" else locks.WriterLockBusy("busy")

    @contextmanager
    def writer_lock(**kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1 and mode != "busy_after_acquisition":
            clock.now += kwargs["wait_seconds"]
            raise failure
        yield lease

    monkeypatch.setitem(sys.modules, "dags.scripts.run_fotmob_scraper",
                        SimpleNamespace(_WRITER_LOCK_KEY=83))
    monkeypatch.setattr(locks, "writer_lock", writer_lock)
    monkeypatch.setattr(c1, "C1_LOCK_RETRY_BUDGET_SECONDS", budget)
    lock = c1.build_writer_lock()

    if mode == "retry":
        with lock() as authority:
            assert authority is lease
    else:
        with pytest.raises(type(failure)) as caught:
            with lock():
                assert mode == "busy_after_acquisition"
                raise failure
        assert caught.value is failure
    assert attempts == [{"key": 83, "wait_seconds": 30.0, "poll_seconds": 2.0}] * (
        2 if mode == "retry" else 1
    )
    assert clock.sleeps == sleeps


@pytest.mark.parametrize("states,budget,sleeps", [
    ([True, True, False], 600.0, [60.0, 60.0]),
    ([True, True, True], 125.0, [60.0, 60.0, 5.0]),
])
def test_wait_for_wave_stops_on_idle_or_deadline(monkeypatch, clock, states, budget, sleeps):
    from scrapers.fotmob import maintenance

    observations = iter(states)
    monkeypatch.setattr(maintenance, "probe_wave", lambda: (next(observations), None))
    monkeypatch.setattr(c1, "C1_WAVE_WAIT_SECONDS", budget)

    assert c1.wait_for_wave() is None
    assert clock.sleeps == sleeps
    assert list(observations) == []


def test_wait_for_wave_propagates_probe_failure(monkeypatch, clock):
    from scrapers.fotmob import maintenance

    failure = RuntimeError("synthetic wave probe failure")

    def probe_wave():
        raise failure

    monkeypatch.setattr(maintenance, "probe_wave", probe_wave)
    with pytest.raises(RuntimeError) as caught:
        c1.wait_for_wave()
    assert caught.value is failure
    assert clock.sleeps == []


def test_hold_deadline_spans_multiple_short_queries(clock):
    calls = []

    def execute_query(sql):
        calls.append(sql)
        clock.now += c1.C1_MAX_LOCK_HOLD_SECONDS * 0.6
        return [sql]

    raw = SimpleNamespace(execute_query=execute_query, current=NEW)
    publisher = c1._HoldDeadlineManager(raw)
    assert publisher.current == raw.current
    assert publisher.execute_query("first") == ["first"]
    with pytest.raises(c1.C1LockHoldOverrun):
        publisher.execute_query("second")
    with pytest.raises(c1.C1LockHoldOverrun):
        publisher.execute_query("third")
    assert calls == ["first", "second"]


@pytest.mark.parametrize("has_cancel", [False, True])
def test_fired_deadline_fences_queries_when_cancellation_is_unavailable(has_cancel):
    def cancel():
        raise RuntimeError("synthetic cancellation failure")

    raw = SimpleNamespace(execute_query=lambda _: pytest.fail("query after deadline"))
    if has_cancel:
        raw.cancel_active_query = cancel
    publisher = c1._HoldDeadlineManager(raw)
    publisher._expire()
    with pytest.raises(c1.C1LockHoldOverrun):
        publisher.execute_query("fenced")


@pytest.mark.parametrize("slow_statement,cancelled_query", [
    ("CREATE TABLE ", True), ("INSERT INTO ", False), ("MERGE INTO ", False),
])
def test_refresh_overrun_cancels_query_drops_stage_and_releases_lease(
    monkeypatch, slow_statement, cancelled_query,
):
    manager = ClosureManager()
    setup_refresh(monkeypatch, manager)
    monkeypatch.setattr(c1, "C1_MAX_LOCK_HOLD_SECONDS", 0.1)
    cancelled = threading.Event()
    released = threading.Event()
    cancel_finished = threading.Event()
    events = []
    authority = object()
    failure = RuntimeError("synthetic query cancelled")

    @contextmanager
    def lock():
        events.append("acquire")
        try:
            yield authority
        finally:
            events.append("release")
            released.set()

    def cancel_active_query():
        events.append("cancel")
        cancelled.set()
        # The statement can finish while cancellation I/O is still outstanding.
        if cancelled_query and not released.wait(5):
            events.append("cancellation blocked release")
        cancel_finished.set()

    raw_execute_query = manager.execute_query

    def execute_query(sql):
        result = raw_execute_query(sql)
        if sql.startswith(slow_statement):
            assert cancelled.wait(5), "the in-flight statement must receive cancellation"
            if cancelled_query:
                raise failure
        if sql.startswith(f"DROP TABLE IF EXISTS {c1.PREFIX}fotmob_c1_inputs__stg_"):
            events.append("drop")
            raise RuntimeError("synthetic cleanup failure")
        return result

    manager.execute_query = execute_query
    manager.cancel_active_query = cancel_active_query
    manager.set_write_authority = lambda value: events.append(("bind", value))
    with pytest.raises(RuntimeError if cancelled_query else c1.C1LockHoldOverrun) as caught:
        c1.refresh(manager, writer_lock=lock)
    if cancelled_query:
        assert caught.value is failure
    assert cancel_finished.wait(5)
    assert events == ["acquire", ("bind", authority), "cancel", "drop", ("bind", None), "release"]
    assert manager.writes[-1].startswith(f"DROP TABLE IF EXISTS {c1.PREFIX}fotmob_c1_inputs__stg_")
    if slow_statement != "MERGE INTO ":
        assert not any(sql.startswith("MERGE INTO ") for sql in manager.writes)
