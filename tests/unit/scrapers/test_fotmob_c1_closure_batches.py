"""Offline C1 closure query batching regressions."""

from datetime import date, datetime
import json
import re

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
                 budget=200_000, fail_batch=None):
        self.pairs = set(pairs)
        self.changed = set(changed)
        self.seed_ids = set(seed_ids)
        self.rows = rows or {}
        self.target = list(target)
        self.budget = budget
        self.fail_batch = fail_batch
        self.old = {table: OLD for table in c1.SOURCES}
        self.current = {table: NEW for table in c1.SOURCES}
        self.files = {table: {} for table in (c1.MATCHES, c1.PAYLOADS, c1.SCOPE, c1.SEASONS)}
        self.files[c1.MATCHES] = {pair: "stable" for pair in self.pairs}
        checkpoint = {
            "contract_version": c1.CONTRACT_VERSION,
            "parser_versions": [c1.PARSER_VERSION, c1.LEGACY_PARSER_VERSION],
            "first_day": str(DAY), "last_day": str(DAY), "snapshots": self.old,
            "files": {table: c1._encoded_pairs(values) for table, values in self.files.items()},
        }
        for pair in self.changed:
            self.files[c1.MATCHES][pair] = "changed"
        self.controls = [(None, None, None, "checkpoint", "old", True, json.dumps(checkpoint))]
        self.queries, self.closure, self.scans = [], [], []
        self.writes, self.staged = [], []
        self.existing_pairs = None

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
                                 for r in self.staged if r[4] in ("checkpoint", "checkpoint_part")]
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


def setup_refresh(monkeypatch, manager):
    monkeypatch.setattr(c1, "datetime", FrozenDatetime)
    monkeypatch.setattr(c1, "snapshots", lambda _: dict(manager.current))
    monkeypatch.setattr(c1, "partition_fingerprints", lambda _, table: manager.files[table])
    monkeypatch.setattr(c1, "manifest_changes", lambda *_: iter(
        (None, None, "match", mid) for mid in sorted(manager.seed_ids)
    ))
    real_stage_rows = c1._stage_rows

    def stage_rows(manager_arg, stage, rows):
        rows = list(rows)
        manager.staged.extend(rows)
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
    assert c1._read_checkpoint(manager.controls)["snapshots"] == manager.current
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
    from contextlib import contextmanager
    from types import SimpleNamespace

    pair = ("10", "2026/27")
    manager = ClosureManager([pair], changed={pair})
    setup_refresh(monkeypatch, manager)
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
