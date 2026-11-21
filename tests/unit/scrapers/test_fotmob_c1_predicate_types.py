"""Offline refresh regression with schema-checked, synthetic snapshot reads."""

import json
import re
from datetime import date, datetime

import pytest

from scrapers.fotmob import c1_inputs as c1


DAY = date(2026, 11, 13)
PAIR = ("10", "2026/27")
OLD_RELATED = ("20", "2026/27")
NEW_RELATED = ("30", "2026/27")
STRING = r"'(?:''|[^'])*'"
SOURCE = re.compile(
    r"\(SELECT \* FROM iceberg\.bronze\.(\w+) FOR VERSION AS OF (\d+) WHERE "
)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 14, 12, tzinfo=tz)


def _predicate_end(sql, start):
    # Find the source subquery's closing parenthesis, respecting SQL strings.
    depth = 1
    for token in re.finditer(STRING + r"|[()]", sql[start:]):
        if token.group() == "(":
            depth += 1
        elif token.group() == ")":
            depth -= 1
            if depth == 0:
                return start + token.start()
    raise AssertionError("unclosed source selection")


def _typed_pairs(predicate, table):
    """Type-check physical predicates before using them to select fake rows."""
    assert not re.search(r"CAST\(\s*(competition_id|source_season_key)\b", predicate, re.I)
    expected = r"-?\d+" if table == c1.MATCHES else STRING
    for values in re.findall(r"\bcompetition_id IN \(([^)]*)\)", predicate):
        assert re.fullmatch(expected + r"(?:\s*,\s*" + expected + r")*", values), (
            f"TYPE_MISMATCH: {table}.competition_id IN ({values})"
        )
    for value in re.findall(r"\bsource_season_key\s*=\s*(" + STRING + r"|[^\s)]+)", predicate):
        assert re.fullmatch(STRING, value), f"TYPE_MISMATCH: {table}.source_season_key"
    pairs = set()
    for season, values in re.findall(
        r"source_season_key=(" + STRING + r") AND competition_id IN \(([^)]*)\)", predicate
    ):
        pairs.update((cid.strip().strip("'"), season[1:-1]) for cid in values.split(","))
    return pairs


class SyntheticManager:
    def __init__(self, changed):
        self.changed = changed
        self.old = {table: 100 + index for index, table in enumerate(c1.SOURCES)}
        self.current = dict(self.old)
        if changed:
            self.current[changed] += 100
        self.files = {
            c1.MATCHES: {pair: "stable" for pair in (PAIR, OLD_RELATED, NEW_RELATED)},
            c1.PAYLOADS: {PAIR: "stable"},
        }
        self.scope_decisions = {PAIR[0]: "included"}
        self.seasons_latest = {PAIR: True}
        self.checkpoint = {
            "contract_version": c1.CONTRACT_VERSION,
            "detection": 2,
            "scope_decisions": [list(row) for row in self.scope_decisions.items()],
            "seasons_latest": c1._encoded_pairs(self.seasons_latest),
            "parser_versions": [c1.PARSER_VERSION, c1.LEGACY_PARSER_VERSION],
            "first_day": str(DAY), "last_day": str(DAY), "snapshots": self.old,
            "files": {table: c1._encoded_pairs(values) for table, values in self.files.items()},
        }
        if changed:
            self.files[changed][PAIR] = "changed"
        self.queries = []
        self.scans = []
        self.target_pairs = []

    def records(self, table, version):
        rows = []
        if table == c1.MATCHES:
            # Shared IDs can return rows outside the fingerprint pair domain;
            # production closure, rather than this fake engine, must filter them.
            rows = [(*OLD_RELATED, "101"), (*NEW_RELATED, "102"),
                    ("40", PAIR[1], "101"), (None, PAIR[1], "102")]
        if table == self.changed:
            rows.append((*PAIR, "101" if version == self.old[table] else "102"))
        else:
            rows.append((*PAIR, "100"))
        return rows

    def execute_query(self, sql):
        self.queries.append(sql)
        selections = []
        for match in SOURCE.finditer(sql):
            table, version = match.group(1), int(match.group(2))
            assert version in (self.old[table], self.current[table])
            predicate = sql[match.end():_predicate_end(sql, match.end())]
            pairs = _typed_pairs(predicate, table)
            selections.append((table, version, pairs, predicate))
            self.scans.append((table, version, pairs, predicate))
        if f"FROM {c1.TABLE} WHERE" in sql:
            self.target_pairs.append(_typed_pairs(sql.split(" WHERE ", 1)[1], c1.TABLE))

        if sql.startswith("SELECT competition_id, source_season_key, match_id, kind, generation"):
            return [(None, None, None, "checkpoint", "old", True, json.dumps(self.checkpoint))]
        if "$history" in sql:
            table = re.search(r'"(\w+)\$history"', sql).group(1)
            return [(self.current[table],)]
        if "$files" in sql:
            table = re.search(r'"(\w+)\$files"', sql).group(1)
            return [(*pair, value) for pair, value in self.files[table].items()]
        if sql.startswith("SELECT competition_id, decision FROM ("):
            assert selections == [
                (c1.SCOPE, self.current[c1.SCOPE], set(), "true"),
                (c1.MANIFEST, self.current[c1.MANIFEST], set(), "target_type='competition_profile'"),
            ]
            return list(self.scope_decisions.items())
        if sql.startswith("SELECT competition_id, source_season_key, is_latest FROM ("):
            assert selections == [
                (c1.SEASONS, self.current[c1.SEASONS], set(), "true"),
                (c1.MANIFEST, self.current[c1.MANIFEST], set(), "target_type='competition_seasons'"),
            ]
            return [(*pair, latest) for pair, latest in self.seasons_latest.items()]
        if sql.startswith("SELECT DISTINCT match_id FROM ("):
            assert len(selections) == 1
            table, version, pairs, _ = selections[0]
            assert pairs
            return [(mid,) for cid, season, mid in self.records(table, version)
                    if (cid, season) in pairs]
        if sql.startswith("SELECT DISTINCT competition_id, source_season_key FROM ("):
            assert len(selections) == 1
            table, version, pairs, predicate = selections[0]
            assert table == c1.MATCHES
            assert version == self.current[c1.MATCHES]
            assert not pairs
            values = re.fullmatch(
                r"CAST\(match_id AS varchar\) IN \((" + STRING + r"(?:," + STRING + r")*)\)",
                predicate,
            )
            assert values
            ids = set(re.findall(STRING, values.group(1)))
            return [(cid, season) for cid, season, mid in self.records(table, version)
                    if c1.literal(mid) in ids]
        if sql.startswith(("SELECT DISTINCT match_id FROM " + c1.TABLE,
                           "SELECT DISTINCT competition_id, source_season_key FROM " + c1.TABLE,
                           "SELECT deadline_day, competition_id, source_season_key, match_id, kind")):
            return []
        if sql.startswith("WITH candidates AS"):
            return [(DAY, *PAIR, "102", FrozenDatetime(2026, 11, 13, 12),
                     None, 0, 0, 0, None, 0)]
        if sql.startswith(("CREATE TABLE ", "INSERT INTO ", "DROP TABLE IF EXISTS ", "MERGE INTO ")):
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


def setup_physical_refresh(monkeypatch, changed):
    monkeypatch.setattr(c1, "datetime", FrozenDatetime)
    monkeypatch.setattr(c1, "scope_decisions", lambda manager, _: manager.scope_decisions)
    monkeypatch.setattr(c1, "seasons_latest", lambda manager, _: manager.seasons_latest)
    return SyntheticManager(changed)


@pytest.mark.unit
@pytest.mark.parametrize("changed", [c1.PAYLOADS, c1.MATCHES])
def test_refresh_changed_physical_pairs_use_table_types_and_both_snapshots(monkeypatch, changed):
    manager = setup_physical_refresh(monkeypatch, changed)
    result = c1.refresh(manager)

    # Each old/new association expands closure, including one absent from the
    # current changed partition. Numeric payload IN literals fail in the fake
    # engine before rows are returned (the original implementation regresses).
    expected_pairs = {PAIR, OLD_RELATED, NEW_RELATED}
    assert result["seasons"] == 3
    assert result["rows"] == 3  # input, day marker, checkpoint
    for table in (c1.MATCHES, c1.PAYLOADS):
        for version in (manager.old[table], manager.current[table]):
            assert any(t == table and v == version and pairs == {PAIR}
                       for t, v, pairs, _ in manager.scans)
            assert any(t == table and v == version and pairs == expected_pairs
                       for t, v, pairs, _ in manager.scans)
    assert {PAIR} in manager.target_pairs
    assert expected_pairs in manager.target_pairs
    reverse = [(table, version, pairs) for table, version, pairs, predicate in manager.scans
               if predicate.startswith("CAST(match_id AS varchar) IN (")]
    assert reverse
    assert all(table == c1.MATCHES and version == manager.current[c1.MATCHES] and not pairs
               for table, version, pairs in reverse)
    # The checker also visits MATCHES predicates inside candidates/current_sql
    # (including the nonempty batch test) and inputs_sql, not just closure SQL.
    assert any(sql.startswith("CREATE TABLE ") and " AS WITH inc AS" in sql
               for sql in manager.queries)
    assert any(sql.startswith("WITH candidates AS") for sql in manager.queries)
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.queries) == 1
    assert sum("$history" in sql for sql in manager.queries) == 2 * len(c1.SOURCES)


@pytest.mark.unit
def test_refresh_unchanged_inputs_skips_physical_closure(monkeypatch):
    manager = setup_physical_refresh(monkeypatch, None)

    result = c1.refresh(manager)

    assert result["seasons"] == 0
    assert result["rows"] == 2  # day marker and checkpoint still publish
    assert manager.scans == []
    assert not any(" AS WITH inc AS" in sql for sql in manager.queries)
    assert not any(sql.startswith("WITH candidates AS") for sql in manager.queries)
    assert any(sql.endswith("AND false") for sql in manager.queries)
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.queries) == 1
    assert sum("$history" in sql for sql in manager.queries) == 2 * len(c1.SOURCES)


@pytest.mark.unit
def test_refresh_pinned_scope_change_uses_typed_pair_predicates(monkeypatch):
    monkeypatch.setattr(c1, "datetime", FrozenDatetime)
    manager = SyntheticManager(c1.MATCHES)
    # The scope entry for OLD_RELATED disappeared while PAIR's match file changed.
    manager.checkpoint["scope_decisions"].append([OLD_RELATED[0], "included"])
    manager.current[c1.SCOPE] += 100

    result = c1.refresh(manager)

    for table in (c1.SCOPE, c1.SEASONS):
        assert manager.scans.count((table, manager.current[table], set(), "true")) == 1
    initial_pairs = {PAIR, OLD_RELATED}
    for table in (c1.MATCHES, c1.PAYLOADS):
        assert any(t == table and pairs == initial_pairs for t, _, pairs, _ in manager.scans)
    assert initial_pairs in manager.target_pairs
    assert result["seasons"] == 3  # Shared match 102 also pulls in NEW_RELATED.


@pytest.mark.unit
@pytest.mark.parametrize("table, predicate", [
    (c1.MATCHES, "source_season_key='2026/27' AND competition_id IN ('10')"),
    (c1.PAYLOADS, "source_season_key='2026/27' AND competition_id IN (10)"),
    (c1.TABLE, "source_season_key='2026/27' AND competition_id IN (10)"),
    (c1.PAYLOADS, "source_season_key=2026 AND competition_id IN ('10')"),
])
def test_synthetic_engine_rejects_incompatible_predicate_types(table, predicate):
    manager = SyntheticManager(c1.PAYLOADS)
    if table == c1.TABLE:
        sql = f"SELECT DISTINCT match_id FROM {table} WHERE {predicate}"
    else:
        sql = "SELECT DISTINCT match_id FROM " + c1.source_sql(table, manager.current, predicate)
    with pytest.raises(AssertionError, match="TYPE_MISMATCH"):
        manager.execute_query(sql)
