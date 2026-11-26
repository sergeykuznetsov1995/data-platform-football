"""Incremental, atomic publication of the eight frozen C1 inputs (#1896).

No source HTTP. Checkpoints are committed in the same Iceberg MERGE as inputs.
Bootstrap is explicit; normal runs refuse a missing checkpoint. Source snapshots
are pinned, so failed/partial work never becomes a complete measurement.
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from scrapers.fotmob.repository import LEGACY_PARSER_VERSION, PARSER_VERSION

PREFIX = "iceberg.bronze."
TABLE = PREFIX + "fotmob_c1_inputs"
MATCHES = "fotmob_matches"
PAYLOADS = "fotmob_match_payloads"
MANIFEST = "fotmob_ingest_manifest"
SCOPE = "fotmob_competition_scope_observations"
SEASONS = "fotmob_competition_seasons"
SOURCES = (MATCHES, PAYLOADS, MANIFEST, SCOPE, SEASONS)
CONTROL_DAY = date(1970, 1, 1)
CONTRACT_VERSION = 1
C1_LOCK_WAIT_SECONDS = 30.0
C1_LOCK_POLL_SECONDS = 2.0
C1_LOCK_RETRY_SLEEP_SECONDS = 20.0
C1_LOCK_RETRY_BUDGET_SECONDS = 600.0
C1_MAX_LOCK_HOLD_SECONDS = 300.0
C1_WAVE_WAIT_SECONDS = 600.0
C1_WAVE_POLL_SECONDS = 60.0
C1_WAVE_PROBE_ATTEMPTS = 3
C1_REFRESH_ATTEMPTS = 3
_sleep = time.sleep
COLUMNS = (
    "deadline_day", "competition_id", "source_season_key", "match_id", "kind",
    "deadline", "first_ingested", "na_runs", "na_spread_s", "success_count",
    "first_ft", "saw_unfinished", "generation", "complete", "checkpoint_json", "published_at",
)
DDL = """(
    deadline_day DATE, competition_id VARCHAR, source_season_key VARCHAR,
    match_id VARCHAR, kind VARCHAR, deadline TIMESTAMP(6),
    first_ingested TIMESTAMP(6), na_runs BIGINT, na_spread_s BIGINT,
    success_count BIGINT, first_ft TIMESTAMP(6), saw_unfinished BIGINT,
    generation VARCHAR, complete BOOLEAN, checkpoint_json VARCHAR, published_at TIMESTAMP(6)
) WITH (partitioning = ARRAY['deadline_day'])"""


def literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return "TIMESTAMP '" + value.isoformat(sep=" ") + "'"
    if isinstance(value, date):
        return "DATE '" + value.isoformat() + "'"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def pairs_sql(pairs, *, physical=False):
    if not pairs:
        return "false"
    # One OR term per season key, not per pair: thousands of pairs (bootstrap)
    # exceed Trino's compiler limit; literal IN lists keep partition pruning.
    by_season = {}
    for cid, sk in sorted(pairs):
        by_season.setdefault(sk, []).append(literal(int(cid) if physical else cid))
    return "(" + " OR ".join(
        f"(source_season_key={literal(sk)} AND competition_id IN ({','.join(cids)}))"
        for sk, cids in by_season.items()
    ) + ")"


CLOSURE_PREDICATE_BYTES = 200_000
REVERSE_QUERY_BYTES = 900_000


def _pair_predicates(pairs, *, physical=False):
    predicates, chunk, size = [], [], 0
    for pair in sorted(pairs):
        pair_size = len(pairs_sql([pair], physical=physical).encode("utf-8"))
        if pair_size > CLOSURE_PREDICATE_BYTES:
            raise RuntimeError("C1 closure pair exceeds predicate limit")
        # Separate season terms need two more bytes than standalone predicates.
        # Grouping competitions within a season can only reduce this bound.
        if chunk and size + 2 + pair_size > CLOSURE_PREDICATE_BYTES:
            predicates.append(pairs_sql(chunk, physical=physical))
            chunk, size = [], 0
        size += pair_size + (2 if chunk else 0)
        chunk.append(pair)
    if chunk:
        predicates.append(pairs_sql(chunk, physical=physical))
    return predicates


def _id_predicates(ids, ceiling=None):
    if ceiling is None:
        ceiling = CLOSURE_PREDICATE_BYTES
    prefix = "CAST(match_id AS varchar) IN ("
    overhead = len(prefix.encode("utf-8")) + 1
    predicates, chunk, size = [], [], overhead
    for mid in sorted(ids):
        value = literal(mid)
        value_size = len(value.encode("utf-8"))
        if overhead + value_size > ceiling:
            raise RuntimeError("C1 closure match ID exceeds predicate limit")
        if chunk and size + 1 + value_size > ceiling:
            predicates.append(prefix + ",".join(chunk) + ")")
            chunk, size = [], overhead
        size += value_size + (1 if chunk else 0)
        chunk.append(value)
    if chunk:
        predicates.append(prefix + ",".join(chunk) + ")")
    return predicates


def source_sql(table, snapshots, predicate="true"):
    return (f"(SELECT * FROM {PREFIX}{table} FOR VERSION AS OF "
            f"{int(snapshots[table])} WHERE {predicate})")


def _reverse_matches_plan(all_pairs, ids, snapshots):
    queries = []
    if not all_pairs or not ids:
        return queries
    prefix = "SELECT DISTINCT competition_id, source_season_key FROM "
    overhead = len((prefix + source_sql(MATCHES, snapshots, "")).encode("utf-8"))
    for id_predicate in _id_predicates(ids, ceiling=REVERSE_QUERY_BYTES - overhead):
        sql = prefix + source_sql(MATCHES, snapshots, id_predicate)
        if len(sql.encode("utf-8")) > REVERSE_QUERY_BYTES:
            raise RuntimeError("C1 closure reverse MATCHES query exceeds query limit")
        queries.append(sql)
    return queries


def current_sql(table, snapshots, predicate="true"):
    """Same selection as repository current views, before any day restriction.

    Matches are restricted to whole season partitions, including the physical
    nonempty-batch test. Manifest ranking retains its full logical identities.
    """
    types, keys = {
        MATCHES: ("league_season", "competition_id, source_season_key, match_id"),
        SCOPE: ("competition_profile", "competition_id"),
        SEASONS: ("competition_seasons", "competition_id, source_season_key"),
    }[table]
    raw = source_sql(table, snapshots, predicate)
    manifest = source_sql(MANIFEST, snapshots, f"target_type='{types}'")
    eligible = (
        f"((parser_version={literal(PARSER_VERSION)} AND status IN "
        "('success','not_modified','not_available')) OR "
        f"(parser_version={literal(LEGACY_PARSER_VERSION)} AND status IN "
        "('success','not_modified')))"
    )
    if table == MATCHES:
        committed = f"""SELECT batch_id, parser_version FROM (
            SELECT batch_id, parser_version, status, row_number() OVER (
                PARTITION BY target_type, competition_id, source_season_key
                ORDER BY CASE WHEN parser_version={literal(PARSER_VERSION)}
                    THEN 1 ELSE 0 END DESC, completed_at DESC, batch_id DESC
            ) AS rn FROM {manifest} WHERE {eligible}
            AND (status='not_available' OR batch_id IN (
                SELECT _target_batch_id FROM {raw}
                GROUP BY _target_batch_id HAVING count(*) > 0)))
            WHERE rn=1 AND status IN ('success','not_modified')"""
    else:
        committed = f"""SELECT DISTINCT batch_id, parser_version FROM {manifest}
            WHERE parser_version IN ({literal(PARSER_VERSION)},
                {literal(LEGACY_PARSER_VERSION)})
            AND status IN ('success','not_modified')"""
    partition = ", ".join("r." + key.strip() for key in keys.split(","))
    return f"""SELECT * FROM (
        SELECT r.*, row_number() OVER (PARTITION BY {partition}
            ORDER BY CASE WHEN c.parser_version={literal(PARSER_VERSION)}
                THEN 1 ELSE 0 END DESC, r._observed_at DESC,
                r._ingested_at DESC, r._target_batch_id DESC) AS c1_rn
        FROM {raw} r JOIN ({committed}) c ON c.batch_id=r._target_batch_id
    ) WHERE c1_rn=1"""


def candidates_sql(pairs, snapshots, first_day, last_day=None):
    pairs = {pairs} if isinstance(pairs, tuple) else set(pairs)
    competitions = "competition_id IN (" + ",".join(literal(cid) for cid in sorted({p[0] for p in pairs})) + ")" if pairs else "false"
    matches = current_sql(MATCHES, snapshots, pairs_sql(pairs, physical=True))
    scope = current_sql(SCOPE, snapshots, competitions)
    seasons = current_sql(SEASONS, snapshots, competitions)
    upper_bound = (f"AND deadline < TIMESTAMP '{last_day + timedelta(days=1)} 00:00:00'"
                   if last_day is not None else "")
    return f"""WITH inc AS ({scope}), seasons AS ({seasons}), calendar AS ({matches}),
    md AS (
        SELECT m.competition_id, m.source_season_key, m.match_id, CAST(from_iso8601_timestamp(m.utc_time)
            AT TIME ZONE 'UTC' AS timestamp(6)) + interval '26' hour AS deadline
        FROM calendar m WHERE m.cancelled=false
        AND EXISTS (SELECT 1 FROM inc WHERE decision='included'
            AND inc.competition_id=CAST(m.competition_id AS varchar))
        AND (m.finished OR NOT EXISTS (
            SELECT 1 FROM seasons s WHERE NOT s.is_latest
            AND s.source_season_key=m.source_season_key AND s.competition_id=CAST(m.competition_id AS varchar)
            AND EXISTS (SELECT 1 FROM seasons latest WHERE latest.is_latest
                AND latest.competition_id=s.competition_id)))
    ), candidates AS (
        SELECT *, CAST(deadline AS date) AS deadline_day FROM md
        WHERE deadline >= TIMESTAMP '{first_day} 00:00:00'
          {upper_bound}
    ) SELECT * FROM candidates"""


def inputs_sql(pairs, snapshots, first_day, last_day, *, candidates_table=None):
    """Read frozen evidence for selected candidates, retaining global ID history.

    Production persists candidates once before this query. Calendar history is
    restricted to whole related seasons, including all shared-ID associations.
    """
    pairs = {pairs} if isinstance(pairs, tuple) else set(pairs)
    candidates = candidates_table or "(" + candidates_sql(pairs, snapshots, first_day, last_day) + ")"
    payloads = source_sql(PAYLOADS, snapshots)
    manifest = source_sql(MANIFEST, snapshots, "target_type='match'")
    raw = source_sql(MATCHES, snapshots, pairs_sql(pairs, physical=True))
    return f"""WITH candidates AS (SELECT * FROM {candidates}), p AS (
        SELECT match_id, min(_ingested_at) AS fi FROM {payloads}
        WHERE match_id IN (SELECT match_id FROM candidates) GROUP BY match_id
    ), h AS (
        SELECT entity_id AS mid,
            count(DISTINCT CASE WHEN status='not_available'
                AND error_code='source_data_not_found' THEN run_id END) AS na_runs,
            date_diff('second', min(CASE WHEN status='not_available'
                AND error_code='source_data_not_found' THEN completed_at END),
                max(CASE WHEN status='not_available'
                AND error_code='source_data_not_found' THEN completed_at END)) AS na_spread,
            count_if(status='success') AS succ
        FROM {manifest} WHERE entity_id IN (
            SELECT CAST(match_id AS varchar) FROM candidates) GROUP BY entity_id
    ), snap AS (
        SELECT md.match_id, md.deadline_day,
            min(CASE WHEN s.finished THEN s._observed_at END) AS first_ft,
            count_if(NOT s.finished AND s._observed_at >= md.deadline - interval '23' hour
                AND s._observed_at < md.deadline) AS saw_unfinished
        FROM {raw} s JOIN candidates md ON s.match_id=md.match_id
        WHERE s._ingested_at >= CAST(md.deadline_day AS timestamp(6)) - interval '4' day
        AND s.utc_time >= to_iso8601(CAST(md.deadline_day AS timestamp(6)) - interval '3' day)
        GROUP BY md.match_id, md.deadline_day
    ) SELECT md.deadline_day, CAST(md.competition_id AS varchar), md.source_season_key, CAST(md.match_id AS varchar),
        md.deadline, p.fi, coalesce(h.na_runs,0), coalesce(h.na_spread,0),
        coalesce(h.succ,0), snap.first_ft, coalesce(snap.saw_unfinished,0)
    FROM candidates md LEFT JOIN p ON p.match_id=md.match_id
    LEFT JOIN h ON h.mid=CAST(md.match_id AS varchar)
    LEFT JOIN snap ON snap.match_id=md.match_id AND snap.deadline_day=md.deadline_day"""


def read_sql(first_day, last_day=None, *, include_day=False):
    """One partition-pruned scan, including explicit empty/missing-day handling."""
    first_day = date.fromisoformat(str(first_day))
    last_day = date.fromisoformat(str(last_day or first_day))
    if last_day < first_day:
        raise ValueError("invalid C1 day range")
    day_column = "d, " if include_day else ""
    return f"""WITH scanned AS (
        SELECT * FROM {TABLE}
        WHERE deadline_day BETWEEN DATE '{first_day}' AND DATE '{last_day}'
    ), checked AS (
        SELECT d, i.*,
            count_if(kind='day' AND complete
                AND published_at >= CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6)) - interval '2' hour) OVER (PARTITION BY d) AS markers,
            count_if(complete IS DISTINCT FROM true) OVER (PARTITION BY d) AS incomplete
        FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', interval '1' day)) AS days(d)
        LEFT JOIN scanned i ON i.deadline_day=d
    ) SELECT {day_column}IF(markers=1 AND incomplete=0, match_id,
        CAST(CAST(concat('C1 incomplete day ', CAST(d AS varchar)) AS bigint) AS varchar)),
        deadline, first_ingested, na_runs, na_spread_s, success_count,
        first_ft, saw_unfinished FROM checked"""


def snapshots(manager):
    result = {}
    for table in SOURCES:
        rows = manager.execute_query(
            f'SELECT snapshot_id FROM {PREFIX}"{table}$history" '
            'ORDER BY made_current_at DESC LIMIT 1'
        )
        if not rows:
            raise RuntimeError(f"C1 source has no snapshot: {table}")
        result[table] = int(rows[0][0])
    return result


def partition_fingerprints(manager, table):
    season = "partition.source_season_key" if table in (MATCHES, PAYLOADS) else "''"
    rows = manager.execute_query(
        f"SELECT partition.competition_id, {season}, "
        "to_hex(checksum(ROW(file_path, file_size_in_bytes, record_count, content))) "
        f'FROM {PREFIX}"{table}$files" '
        f"GROUP BY partition.competition_id, {season}"
    )
    return {(str(cid), str(sk)): str(value) for cid, sk, value in rows}


def scope_decisions(manager, snapshots):
    """Current scope decision per competition at the pinned snapshot."""
    rows = manager.execute_query(
        f"SELECT competition_id, decision FROM ({current_sql(SCOPE, snapshots)})"
    )
    return {str(cid): str(decision) for cid, decision in rows}


def seasons_latest(manager, snapshots):
    """Current is_latest flag per (competition, season) at the pinned snapshot."""
    rows = manager.execute_query(
        "SELECT competition_id, source_season_key, is_latest "
        f"FROM ({current_sql(SEASONS, snapshots)})"
    )
    flags = {}
    for cid, sk, latest in rows:
        if type(latest) is not bool:
            raise RuntimeError("C1 invalid source is_latest flag")
        flags[(str(cid), str(sk))] = latest
    return flags


def changed_pairs(previous, current):
    return {key for key in previous.keys() | current.keys()
            if previous.get(key) != current.get(key)}


def _encoded_pairs(values):
    return [[*key, value] for key, value in sorted(values.items())]


def _decoded_pairs(values):
    return {(cid, sk): value for cid, sk, value in values}


MANIFEST_BUCKETS = 64
MANIFEST_TARGET_TYPES = ("match", "league_season", "competition_profile", "competition_seasons")
MAX_APPEND_FILES = 256
MAX_APPEND_ROWS = 50_000
# Trino crc32(varbinary) returns an unsigned, nonnegative BIGINT. One VARCHAR
# key avoids tuple/JSON encodings and delimiter ambiguity. NULL and empty keys
# deliberately share a bucket; routing collisions never establish equality.
MANIFEST_BUCKET_SQL = (
    "mod(crc32(to_utf8(coalesce(target_key, ''))), "
    f"BIGINT '{MANIFEST_BUCKETS}')"
)


def manifest_changes_sql(old, new, target_type, bucket):
    """One direction of an exact full-row bag difference in one hash bucket."""
    predicate = (
        f"target_type={literal(target_type)} "
        f"AND {MANIFEST_BUCKET_SQL}=BIGINT '{int(bucket)}'"
    )
    before = source_sql(MANIFEST, {MANIFEST: old}, predicate)
    after = source_sql(MANIFEST, {MANIFEST: new}, predicate)
    return f"""SELECT competition_id, source_season_key, target_type, entity_id FROM (
        SELECT * FROM {after} EXCEPT ALL SELECT * FROM {before})"""


def _compare_manifest_buckets(manager, old, new):
    """Consume at most 512 comparisons plus four bounded-key discoveries.

    64 buckets per type bound sequential fan-out independently of competition/
    season cardinality while splitting full-row comparison state. Discover the
    union of occupied buckets in BOTH pinned snapshots (at most 64 BIGINTs).
    Equal rows always route together; a changed key is compared in both its old
    and new buckets. EXCEPT ALL, never the routing hash, decides equality and
    preserves multiplicity, deletions, rollback and wide-column-only edits.

    target_type retains partition pruning. Buckets trade repeated scans of each
    type for smaller comparison state; NULL/empty keys and other skew can still
    exceed 2GB, and the fixed query bound does not guarantee the task deadline.
    Runtime validation is required. Propagate every failure, including expired
    snapshots, without retry or publication; refresh exhausts this iterator
    before any staging or checkpoint writes.
    """
    for target_type in MANIFEST_TARGET_TYPES:
        predicate = f"target_type={literal(target_type)}"
        before = source_sql(MANIFEST, {MANIFEST: old}, predicate)
        after = source_sql(MANIFEST, {MANIFEST: new}, predicate)
        # Materialize at most 64 keys, finishing discovery before comparisons.
        buckets = sorted(manager.execute_query(
            f"SELECT {MANIFEST_BUCKET_SQL} FROM {before} "
            f"UNION SELECT {MANIFEST_BUCKET_SQL} FROM {after}"
        ))
        for (bucket,) in buckets:
            for left, right in ((old, new), (new, old)):
                yield from manager.execute_query(
                    manifest_changes_sql(left, right, target_type, bucket)
                )


def _appended_manifest_rows(manager, old, new):
    """Return exact additions on a short append-only ancestry, or None to compare.

    Snapshot IDs are opaque. Walk parent links, then corroborate the per-snapshot
    file inventory and each file's row count before returning any changes.
    Maintenance can replace files while we read live metadata; a missing file
    always sends us back to the full comparison of the two pinned snapshots.
    """
    history = manager.execute_query(
        f"SELECT snapshot_id, parent_id, operation, "
        f"try_cast(element_at(summary, 'added-data-files') AS bigint) "
        f'FROM {PREFIX}"{MANIFEST}$snapshots" ORDER BY committed_at DESC LIMIT 1024'
    )
    ancestry = {sid: (parent, operation, added) for sid, parent, operation, added in history}
    if old not in ancestry:
        return None
    files_per_snapshot = {}
    snapshot = new
    while snapshot != old:
        if snapshot not in ancestry or snapshot in files_per_snapshot:
            return None
        parent, operation, added = ancestry[snapshot]
        if operation != "append" or added is None or added < 1:
            return None
        files_per_snapshot[snapshot] = added
        snapshot = parent
    file_count = sum(files_per_snapshot.values())
    if not file_count or file_count > MAX_APPEND_FILES:
        return None

    versions = ", ".join(str(int(sid)) for sid in sorted(files_per_snapshot))
    files = list(manager.execute_query(
        f'SELECT added_snapshot_id, file_path, record_count FROM {PREFIX}"{MANIFEST}$files" '
        f"WHERE content = 0 AND added_snapshot_id IN ({versions}) LIMIT {file_count + 1}"
    ))
    if Counter(sid for sid, _, _ in files) != files_per_snapshot:
        return None
    counts = {path: count for _, path, count in files}
    if len(counts) != file_count or None in counts or any(
        count is None or count < 0 for count in counts.values()
    ):
        return None
    row_count = sum(counts.values())
    if row_count > MAX_APPEND_ROWS:
        return None
    paths = ", ".join(literal(path) for path in sorted(counts))
    rows = manager.execute_query(
        f'SELECT "$path", competition_id, source_season_key, target_type, entity_id '
        f'FROM {PREFIX}{MANIFEST} FOR VERSION AS OF {int(new)} '
        f'WHERE "$path" IN ({paths}) LIMIT {row_count + 1}'
    )
    observed = Counter()
    changes = []
    for path, competition, season, target_type, entity in rows:
        observed[path] += 1
        if target_type in MANIFEST_TARGET_TYPES:
            changes.append((competition, season, target_type, entity))
    return changes if observed == Counter(counts) else None


def manifest_changes(manager, old, new):
    appended = _appended_manifest_rows(manager, old, new)
    if appended is None:
        yield from _compare_manifest_buckets(manager, old, new)
    else:
        yield from appended


def merge_sql(stage):
    keys = ("deadline_day", "competition_id", "source_season_key", "match_id", "kind")
    return f"""MERGE INTO {TABLE} t USING {stage} s ON
        {" AND ".join(f't.{key} IS NOT DISTINCT FROM s.{key}' for key in keys)}
        WHEN MATCHED AND NOT s.complete THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            {', '.join(f'{column}=s.{column}' for column in COLUMNS)}
        WHEN NOT MATCHED AND s.complete THEN INSERT ({', '.join(COLUMNS)})
            VALUES ({', '.join('s.' + column for column in COLUMNS)})"""


def _stage_rows(manager, stage, rows):
    chunk = []
    size = 0
    for row in rows:
        row = (*row, datetime.now(timezone.utc).replace(tzinfo=None))
        sql = "(" + ",".join(literal(value) for value in row) + ")"
        if len(sql.encode()) > 200_000:
            raise RuntimeError("C1 checkpoint/row exceeds staging limit")
        if chunk and size + len(sql.encode()) > 200_000:
            manager.execute_query(f"INSERT INTO {stage} VALUES " + ",".join(chunk))
            chunk, size = [], 0
        chunk.append(sql)
        size += len(sql.encode()) + 1
    if chunk:
        manager.execute_query(f"INSERT INTO {stage} VALUES " + ",".join(chunk))


def _checkpoint_header(checkpoint):
    """Keep unbounded inventories in the checksummed parts of a split checkpoint."""
    return {k: v for k, v in checkpoint.items()
            if k not in ("files", "scope_decisions", "seasons_latest")}


def _checkpoint_payloads(checkpoint_json, generation):
    """Keep legacy small checkpoints; bound SQL literals for larger inventories.

    json.dumps uses ASCII escapes, so 32k characters remain below the staging
    ceiling even after SQL quote escaping. Header and parts share one MERGE.
    """
    if len(checkpoint_json.encode()) <= 32_000:
        return [(None, "checkpoint", checkpoint_json)]
    parts = [checkpoint_json[i:i + 32_000]
             for i in range(0, len(checkpoint_json), 32_000)]
    header = _checkpoint_header(json.loads(checkpoint_json))
    header["checkpoint_storage"] = {"version": 1, "parts": len(parts), "sha256": generation}
    return [(None, "checkpoint", json.dumps(header, sort_keys=True)),
            *[(str(i), "checkpoint_part", part) for i, part in enumerate(parts)]]


def _validated_checkpoint(checkpoint):
    """Each semantic key must have one typed value before a delta can be trusted."""
    if checkpoint.get("detection") == 2:
        for name, types in (
            ("scope_decisions", (str, str)),
            ("seasons_latest", (str, str, bool)),
        ):
            rows = checkpoint.get(name)
            if (not isinstance(rows, list)
                    or any(not isinstance(row, list) or tuple(map(type, row)) != types
                           for row in rows)
                    or len({tuple(row[:-1]) for row in rows}) != len(rows)):
                raise RuntimeError(f"C1 invalid checkpoint {name}")
    return checkpoint


def _read_checkpoint(rows):
    """Validate one control-partition snapshot before consuming any inventory."""
    headers = [row for row in rows if row[3] == "checkpoint"]
    if len(headers) != 1:
        raise RuntimeError("C1 requires an approved bootstrap")
    cid, season, mid, _, generation, complete, payload = headers[0]
    if (cid, season, mid) != (None, None, None) or complete is not True:
        raise RuntimeError("C1 incomplete checkpoint")
    header = json.loads(payload)
    parts = [row for row in rows if row[3] == "checkpoint_part"]
    storage = header.get("checkpoint_storage")
    if storage is None:
        if parts:
            raise RuntimeError("C1 unexpected checkpoint parts")
        return _validated_checkpoint(header)
    if (not isinstance(storage, dict) or storage.get("version") != 1
            or type(storage.get("parts")) is not int or storage["parts"] < 1
            or len(parts) != storage["parts"] or storage.get("sha256") != generation):
        raise RuntimeError("C1 incomplete checkpoint parts")
    indexed = {}
    for cid, season, mid, _, part_generation, complete, payload in parts:
        if (cid is not None or season is not None or mid in indexed
                or part_generation != generation or complete is not True
                or not isinstance(payload, str)):
            raise RuntimeError("C1 invalid checkpoint part")
        indexed[mid] = payload
    if set(indexed) != {str(i) for i in range(len(parts))}:
        raise RuntimeError("C1 invalid checkpoint part indexes")
    payload = "".join(indexed[str(i)] for i in range(len(parts)))
    if hashlib.sha256(payload.encode()).hexdigest() != generation:
        raise RuntimeError("C1 checkpoint checksum mismatch")
    checkpoint = json.loads(payload)
    if _checkpoint_header(checkpoint) != {
        k: v for k, v in header.items() if k != "checkpoint_storage"
    }:
        raise RuntimeError("C1 checkpoint header mismatch")
    return _validated_checkpoint(checkpoint)


def build_writer_lock():
    """Retry acquisition on the wave's lock. A granted lease is never replayed."""
    from dags.scripts.run_fotmob_scraper import _WRITER_LOCK_KEY
    from scrapers.fotmob.writer_lock import WriterLockBusy, WriterLockLost, writer_lock

    @contextmanager
    def acquire():
        deadline = time.monotonic() + C1_LOCK_RETRY_BUDGET_SECONDS
        acquired = False
        while True:
            try:
                with writer_lock(
                    key=_WRITER_LOCK_KEY, wait_seconds=C1_LOCK_WAIT_SECONDS,
                    poll_seconds=C1_LOCK_POLL_SECONDS,
                ) as lease:
                    acquired = True
                    yield lease
                return
            except WriterLockLost:
                raise
            except WriterLockBusy:
                if acquired or time.monotonic() >= deadline:
                    raise
                _sleep(C1_LOCK_RETRY_SLEEP_SECONDS)
                if time.monotonic() >= deadline:
                    raise

    return acquire


def wait_for_wave():
    """Give a running wave a bounded head start before opening Trino.

    The probe is best effort: the writer lock, not this courtesy wait, guards
    correctness, so an infra failure (a live run saw transient docker DNS take
    down name resolution for the isolated metadb) must not fail the refresh.
    Retry the probe briefly, then proceed without waiting; acquisition of the
    lock itself stays fail-closed.
    """
    from scrapers.fotmob.maintenance import probe_wave

    deadline = time.monotonic() + C1_WAVE_WAIT_SECONDS
    failures = 0
    while time.monotonic() < deadline:
        try:
            running, _ = probe_wave()
        except Exception:
            failures += 1
            if failures >= C1_WAVE_PROBE_ATTEMPTS:
                return
            _sleep(min(C1_WAVE_POLL_SECONDS, deadline - time.monotonic()))
            continue
        if not running:
            return
        remaining = deadline - time.monotonic()
        if remaining > 0:
            _sleep(min(C1_WAVE_POLL_SECONDS, remaining))


class C1LockHoldOverrun(RuntimeError):
    """Publication held the writer lock longer than C1_MAX_LOCK_HOLD_SECONDS."""


class _HoldDeadlineManager:
    """Fence every publication statement against the same monotonic lease deadline."""

    def __init__(self, manager):
        self._manager = manager
        self._deadline = time.monotonic() + C1_MAX_LOCK_HOLD_SECONDS
        self._fired = threading.Event()

    def __getattr__(self, name):
        return getattr(self._manager, name)

    def remaining(self):
        remaining = self._deadline - time.monotonic()
        if self._fired.is_set() or remaining <= 0:
            raise C1LockHoldOverrun("C1 publication exceeded its writer lock hold deadline")
        return remaining

    def _expire(self):
        self._fired.set()
        try:
            cancel = getattr(self._manager, "cancel_active_query", None)
            if cancel is not None:
                cancel()
        except Exception:
            pass  # Best effort. The deadline still fences the next statement.

    def execute_query(self, sql):
        timer = threading.Timer(self.remaining(), self._expire)
        timer.daemon = True
        timer.start()
        try:
            result = self._manager.execute_query(sql)
        finally:
            # Cancellation I/O is best effort and may outlive the statement.
            # Releasing the writer lease must never wait for the timer thread.
            timer.cancel()
        self.remaining()
        return result


@contextmanager
def _write_lease(manager, writer_lock):
    if writer_lock is None:
        yield
        return
    with writer_lock() as authority:
        manager.set_write_authority(authority)
        try:
            yield
        finally:
            manager.set_write_authority(None)


def refresh(manager, *, first_day=None, last_day=None, writer_lock=None):
    """Pin reads first and lease only shared writes; first_day is bootstrap only.

    A missing/expired previous snapshot fails closed. Rebootstrap in an approved
    window rather than guessing which commits/partitions were lost. Bootstrap
    keeps one lease for the whole operation; the normal refresh leases only the
    final publication after all reads and candidate staging have completed.
    """
    bootstrap = first_day is not None
    if bootstrap and writer_lock is not None:
        with _write_lease(manager, writer_lock):
            return refresh(
                manager, first_day=first_day, last_day=last_day,
            )
    if bootstrap:
        first_day = date.fromisoformat(str(first_day))
        last_day = date.fromisoformat(str(last_day))
        if first_day <= CONTROL_DAY or last_day < first_day:
            raise ValueError("invalid C1 bootstrap range")
        manager.execute_query(f"CREATE TABLE IF NOT EXISTS {TABLE} {DDL}")
    controls = manager.execute_query(
        f"SELECT competition_id, source_season_key, match_id, kind, generation, complete, "
        f"checkpoint_json FROM {TABLE} WHERE deadline_day=DATE '{CONTROL_DAY}' "
        "AND kind IN ('checkpoint','checkpoint_part')"
    )
    previous = None if bootstrap else _read_checkpoint(controls)
    if previous is not None and (
        previous.get("contract_version") != CONTRACT_VERSION
        or previous.get("parser_versions") != [PARSER_VERSION, LEGACY_PARSER_VERSION]
    ):
        raise RuntimeError("C1 contract changed; approved rebootstrap required")
    if not bootstrap:
        first_day = date.fromisoformat(previous["first_day"])
        last_day = max(date.fromisoformat(previous["last_day"]),
                       datetime.now(timezone.utc).date() - timedelta(days=1))
    before = snapshots(manager)
    semantic = {
        "scope": scope_decisions(manager, before),
        "seasons": seasons_latest(manager, before),
    }
    files = {table: partition_fingerprints(manager, table)
             for table in (MATCHES, PAYLOADS)}
    all_pairs = set(files[MATCHES])
    # A legacy checkpoint gets one full pass while retaining its evidence for
    # deletions and shared-ID closure. The same MERGE upgrades its detector.
    has_semantics = previous is not None and previous.get("detection") == 2
    affected = set() if has_semantics else set(all_pairs)
    if previous is not None:
        for table in (MATCHES, PAYLOADS):
            affected |= changed_pairs(_decoded_pairs(previous["files"][table]), files[table])
        changed_competitions = set()
        ids = set()
        if before[MANIFEST] != previous["snapshots"][MANIFEST]:
            for cid, sk, target_type, mid in manifest_changes(
                manager, previous["snapshots"][MANIFEST], before[MANIFEST]
            ):
                if target_type in ("competition_profile", "competition_seasons"):
                    changed_competitions.add(str(cid))
                elif target_type == "match":
                    ids.add(str(mid))
                    if cid is not None and sk is not None:
                        affected.add((str(cid), str(sk)))
                else:
                    affected.add((str(cid), str(sk)))
        if has_semantics:
            changed_competitions |= changed_pairs(
                dict(previous["scope_decisions"]), semantic["scope"]
            )
            old_seasons = _decoded_pairs(previous["seasons_latest"])
            affected |= changed_pairs(old_seasons, semantic["seasons"])
            # Unfinished-match candidacy also depends on the existence of any
            # latest season in the competition, including outside all_pairs.
            old_latest = {cid for (cid, _), latest in old_seasons.items() if latest}
            new_latest = {cid for (cid, _), latest in semantic["seasons"].items() if latest}
            changed_competitions |= old_latest ^ new_latest
        affected |= {pair for pair in all_pairs if pair[0] in changed_competitions}
        # Pool legacy evidence across natural keys. Find IDs in changed physical
        # seasons in both snapshots, then every season containing them. Deleted
        # evidence may have no published input in its own physical season.
        # The pinned current ID-to-pair relation is fixed; each newly found ID
        # needs only one reverse source search as the closure grows.
        reverse_searched_ids = set()
        while True:
            known = set(affected)
            if affected:
                for table in (MATCHES, PAYLOADS):
                    predicates = _pair_predicates(affected, physical=table == MATCHES)
                    for version in (previous["snapshots"], before):
                        for predicate in predicates:
                            ids |= {str(row[0]) for row in manager.execute_query(
                                f"SELECT DISTINCT match_id FROM {source_sql(table, version, predicate)}"
                            )}
                for predicate in _pair_predicates(affected):
                    ids |= {str(row[0]) for row in manager.execute_query(
                        f"SELECT DISTINCT match_id FROM {TABLE} WHERE kind='input' "
                        f"AND deadline_day >= DATE '{first_day}' "
                        f"AND {predicate}"
                    )}
            if ids:
                id_predicates = _id_predicates(ids)
                new_ids = ids - reverse_searched_ids
                reverse_matches = _reverse_matches_plan(all_pairs, new_ids, before)
                for sql in reverse_matches:
                    for row in manager.execute_query(sql):
                        pair = (str(row[0]), str(row[1]))
                        if pair in all_pairs:
                            affected.add(pair)
                reverse_searched_ids.update(new_ids)
                for id_predicate in id_predicates:
                    affected |= {(str(row[0]), str(row[1])) for row in manager.execute_query(
                        f"SELECT DISTINCT competition_id, source_season_key FROM {TABLE} "
                        f"WHERE kind='input' AND deadline_day >= DATE '{first_day}' "
                        f"AND {id_predicate}"
                    )}
            if affected == known:
                break
    else:
        affected |= {(str(row[0]), str(row[1])) for row in manager.execute_query(
            f"SELECT DISTINCT competition_id, source_season_key FROM {TABLE} "
            f"WHERE kind='input' AND deadline_day >= DATE '{first_day}'"
        )}
    checkpoint = {
        "contract_version": CONTRACT_VERSION,
        "detection": 2,
        "scope_decisions": [list(row) for row in sorted(semantic["scope"].items())],
        "seasons_latest": _encoded_pairs(semantic["seasons"]),
        "parser_versions": [PARSER_VERSION, LEGACY_PARSER_VERSION],
        "first_day": str(first_day), "last_day": str(last_day), "snapshots": before,
        "files": {table: _encoded_pairs(values) for table, values in files.items()},
    }
    checkpoint_json = json.dumps(checkpoint, sort_keys=True)
    generation = hashlib.sha256(checkpoint_json.encode()).hexdigest()
    replacements = {}
    # Cache future eligible matches too. A new deadline day only needs its
    # completeness marker; its inputs were populated on the last source commit.
    if affected:
        candidates_stage = PREFIX + "fotmob_c1_candidates__stg_" + uuid4().hex
        manager.execute_query(
            f"CREATE TABLE {candidates_stage} AS "
            + candidates_sql(affected, before, first_day)
        )
        try:
            records = manager.execute_query(inputs_sql(
                affected, before, first_day, None, candidates_table=candidates_stage
            ))
        finally:
            manager.execute_query(f"DROP TABLE IF EXISTS {candidates_stage}")
        for record in records:
            day, cid, sk, mid, *inputs = record
            day = date.fromisoformat(str(day))
            key = (day, str(cid), str(sk), str(mid), "input")
            if key in replacements:
                raise RuntimeError("C1 duplicate natural key")
            replacements[key] = (*key, *inputs, generation, True, None)
    existing = manager.execute_query(
        f"SELECT deadline_day, competition_id, source_season_key, match_id, kind FROM {TABLE} "
        f"WHERE kind='input' AND deadline_day >= DATE '{first_day}' "
        f"AND {pairs_sql(affected)}"
    )
    for record in existing:
        key = (date.fromisoformat(str(record[0])), *record[1:])
        if key not in replacements:
            replacements[key] = (*key, *([None] * 7), generation, False, None)
    day = first_day
    while day <= last_day:
        key = (day, None, None, None, "day")
        replacements[key] = (*key, *([None] * 7), generation, True, None)
        day += timedelta(days=1)
    for mid, kind, payload in _checkpoint_payloads(checkpoint_json, generation):
        key = (CONTROL_DAY, None, None, mid, kind)
        replacements[key] = (*key, *([None] * 7), generation, True, payload)
    # Shrinking inventories/rebootstrap must not leave previous parts behind.
    # Tombstones are published atomically with the new header, inputs and parts.
    for cid, season, mid, kind, *_ in controls:
        key = (CONTROL_DAY, cid, season, mid, kind)
        if key not in replacements:
            replacements[key] = (*key, *([None] * 7), generation, False, None)
    stage = PREFIX + "fotmob_c1_inputs__stg_" + uuid4().hex
    with _write_lease(manager, writer_lock):
        publisher = manager if bootstrap else _HoldDeadlineManager(manager)
        created = published = False
        try:
            publisher.execute_query(f"CREATE TABLE {stage} {DDL}")
            created = True
            _stage_rows(publisher, stage, replacements.values())
            # Metadata was read outside time travel; reject a mixed file inventory.
            # Source commits during a lost lease can never advance our checkpoint.
            if snapshots(publisher) != before:
                raise RuntimeError("C1 source snapshots changed during refresh")
            publisher.execute_query(merge_sql(stage))
            published = True
        finally:
            # A cancelled CREATE may already have made the stage visible.
            # Preserve the publication error if raw cleanup also fails.
            if created or not bootstrap:
                try:
                    manager.execute_query(f"DROP TABLE IF EXISTS {stage}")
                except Exception:
                    if published or bootstrap:
                        raise
        if not bootstrap:
            publisher.remaining()
    return {"generation": generation, "seasons": len(affected), "rows": len(replacements)}


def run_refresh(**context):
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    if conf:
        raise ValueError("C1 bootstrap is manual and requires an approved window")
    wait_for_wave()
    from scrapers.fotmob.trino_instrumentation import FotMobTrinoTableManager

    # Day/checkpoint rows have nullable MERGE keys matched with IS NOT DISTINCT
    # FROM; disable dynamic filtering for these C1 joins.
    manager = FotMobTrinoTableManager(
        source="fotmob:c1-inputs",
        session_properties={
            "query_max_run_time": "300s",
            "query_max_execution_time": "300s",
            "enable_dynamic_filtering": "false",
        },
    )
    # Saturated wave lanes can invalidate the pinned source snapshots while
    # this refresh politely waits for the publication lease; the failed attempt
    # publishes nothing, so retry from freshly pinned snapshots instead of
    # burning the whole slot. The task deadline still bounds the loop.
    attempts = C1_REFRESH_ATTEMPTS
    while True:
        attempts -= 1
        try:
            return refresh(manager, writer_lock=build_writer_lock())
        except RuntimeError as error:
            if attempts <= 0 or "snapshots changed" not in str(error):
                raise
