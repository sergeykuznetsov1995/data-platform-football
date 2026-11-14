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
            c1.SCOPE: {("10", ""): "stable"},
            c1.SEASONS: {("10", ""): "stable"},
        }
        self.checkpoint = {
            "contract_version": c1.CONTRACT_VERSION,
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
            rows = [(*OLD_RELATED, "101"), (*NEW_RELATED, "102")]
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
            ids = re.search(r"CAST\(match_id AS varchar\) IN \(([^)]*)\)", predicate).group(1)
            ids = set(re.findall(STRING, ids))
            return [(cid, season) for cid, season, mid in self.records(table, version)
                    if (cid, season) in pairs and c1.literal(mid) in ids]
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


@pytest.mark.unit
@pytest.mark.parametrize("changed", [c1.PAYLOADS, c1.MATCHES])
def test_refresh_changed_physical_pairs_use_table_types_and_both_snapshots(monkeypatch, changed):
    monkeypatch.setattr(c1, "datetime", FrozenDatetime)
    manager = SyntheticManager(changed)

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
    # The checker also visits MATCHES predicates inside candidates/current_sql
    # (including the nonempty batch test) and inputs_sql, not just closure SQL.
    assert any(sql.startswith("CREATE TABLE ") and " AS WITH inc AS" in sql
               for sql in manager.queries)
    assert any(sql.startswith("WITH candidates AS") for sql in manager.queries)
    assert sum(sql.startswith("MERGE INTO ") for sql in manager.queries) == 1
    assert sum("$history" in sql for sql in manager.queries) == 2 * len(c1.SOURCES)


@pytest.mark.unit
def test_refresh_unchanged_inputs_skips_physical_closure(monkeypatch):
    monkeypatch.setattr(c1, "datetime", FrozenDatetime)
    manager = SyntheticManager(None)

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
