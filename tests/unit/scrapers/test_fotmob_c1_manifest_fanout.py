"""Offline C1 manifest fan-out performance contract regression."""

from collections import Counter, defaultdict
import re
import zlib

import pytest

from scrapers.fotmob import c1_inputs as c1


pytestmark = pytest.mark.unit
TYPES = ("match", "league_season", "competition_profile", "competition_seasons")
OLD, NEW = 902, 7
ROUTE = "mod(crc32(to_utf8(coalesce(target_key, ''))), BIGINT '64')"
SOURCE = re.compile(
    r"\(SELECT \* FROM iceberg\.bronze\.fotmob_ingest_manifest "
    r"FOR VERSION AS OF (\d+) WHERE target_type='([a-z_]+)'"
    r"(?: AND " + re.escape(ROUTE) + r"=BIGINT '(\d+)')?\)"
)


def covering_keys():
    """Small deterministic fixture covering every bucket, with real CRC32."""
    keys = {}
    for i in range(4096):
        key = f"synthetic-target-{i}"
        keys.setdefault(zlib.crc32(key.encode("utf-8")) % 64, key)
        if len(keys) == 64:
            break
    assert set(keys) == set(range(64))
    return [keys[bucket] for bucket in range(64)]


def oracle(before, after):
    # Deliberately unbucketed: whole physical rows decide equality and retain
    # duplicate counts. The skinny fixtures avoid a runtime/memory benchmark.
    old, new = Counter(before), Counter(after)
    return Counter(r[:4] for r in ((old - new) + (new - old)).elements())


class FanoutManager:
    """Evaluate the actual generated SQL with pre-indexed synthetic full rows.

    The strict grammar checks both pinned sources, partition pruning, routing
    functions/types and full-row bag subtraction before projecting identities.
    Indexing only speeds up this offline evaluator; production still scans.
    """

    def __init__(self, before, after):
        self.groups = {}
        for version, rows in ((OLD, before), (NEW, after)):
            groups = defaultdict(Counter)
            for record in rows:
                key = record[4]
                bucket = zlib.crc32((key or "").encode("utf-8")) % 64
                groups[(record[2], bucket)][record] += 1
            self.groups[version] = groups
        self.queries = []
        self.comparisons = []
        self.discoveries = []

    def execute_query(self, sql):
        self.queries.append(sql)
        if '$snapshots"' in sql:
            return []  # No append proof: exercise the general comparison.
        normalized = re.sub(r"\s+", " ", sql).strip()
        sources = SOURCE.findall(normalized)
        assert len(sources) == 2, "both pinned bucket sources are required"
        shape = SOURCE.sub("<source>", normalized)
        (lv, lt, lb), (rv, rt, rb) = sources
        assert {int(lv), int(rv)} == {OLD, NEW}
        assert lt == rt and lt in TYPES
        if "EXCEPT" in normalized:
            assert re.fullmatch(
                r"SELECT competition_id, source_season_key, target_type, entity_id "
                r"FROM \(\s*SELECT \* FROM <source> EXCEPT ALL SELECT \* FROM <source>\)",
                shape,
            )
            assert lb == rb and lb != "" and 0 <= int(lb) < 64
            self.comparisons.append((lt, int(lb), int(lv), int(rv)))
            left = self.groups[int(lv)].get((lt, int(lb)), Counter())
            right = self.groups[int(rv)].get((rt, int(rb)), Counter())
            return [r[:4] for r in (left - right).elements()]
        assert shape == f"SELECT {ROUTE} FROM <source> UNION SELECT {ROUTE} FROM <source>"
        assert lb == rb == ""
        buckets = {bucket for version in (OLD, NEW)
                   for target, bucket in self.groups[version] if target == lt}
        assert len(buckets) <= 64
        self.discoveries.append((lt, len(buckets)))
        return [(bucket,) for bucket in sorted(buckets)]


@pytest.mark.parametrize("pair_count", [64, 4096])
@pytest.mark.parametrize("rewrite", [False, True])
def test_manifest_query_count_independent_of_pair_cardinality(pair_count, rewrite):
    keys = covering_keys()
    # 4096 distinct competition/season pairs PER TYPE, including match alone.
    # Only a few short columns: wide-column semantics live in the memory tests.
    before = [
        (str(i), f"synthetic-season-{i}", target, str(i), keys[i % 64],
         "same-content-hash", "original evidence")
        for target in TYPES for i in range(pair_count)
    ]
    before.extend([before[0], before[0]])
    after = list(reversed(before))  # Compaction changes layout, not the row bag.
    if not rewrite:
        after.pop()  # Delete one duplicate, preserving the remaining copies.
        # Same routing key and content_hash; only nonidentity evidence changes.
        after[0] = (*after[0][:-1], "changed evidence")
        after.append(after[pair_count])
    manager = FanoutManager(before, after)

    actual = Counter(c1.manifest_changes(manager, OLD, NEW))

    assert actual == oracle(before, after)
    assert bool(actual) is not rewrite
    assert manager.discoveries == [(target, 64) for target in TYPES]
    # Both the small and thousand-pair cases have the SAME fixed upper bound,
    # even for an unchanged snapshot rewrite: no content_hash shortcut allowed.
    assert len(manager.comparisons) == 512
    assert len(manager.queries) == 517
    assert set(manager.comparisons) == {
        (target, bucket, left, right)
        for target in TYPES for bucket in range(64)
        for left, right in ((OLD, NEW), (NEW, OLD))
    }
