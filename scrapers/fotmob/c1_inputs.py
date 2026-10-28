"""Incremental, atomic publication of the eight frozen C1 inputs (#1896).

No source HTTP. Checkpoints are committed in the same Iceberg MERGE as inputs.
Bootstrap is explicit; normal runs refuse a missing checkpoint. Source snapshots
are pinned, so failed/partial work never becomes a complete measurement.
"""
from __future__ import annotations

import hashlib
import json
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
    return "(" + " OR ".join(
        f"(competition_id={literal(int(cid) if physical else cid)} AND source_season_key={literal(sk)})"
        for cid, sk in sorted(pairs)
    ) + ")"


def source_sql(table, snapshots, predicate="true"):
    return (f"(SELECT * FROM {PREFIX}{table} FOR VERSION AS OF "
            f"{int(snapshots[table])} WHERE {predicate})")


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


def changed_pairs(previous, current):
    return {key for key in previous.keys() | current.keys()
            if previous.get(key) != current.get(key)}


def _encoded_pairs(values):
    return [[*key, value] for key, value in sorted(values.items())]


def _decoded_pairs(values):
    return {(cid, sk): value for cid, sk, value in values}


def manifest_changes_sql(old, new):
    # EXCEPT in both directions handles rollback/deletion as well as append;
    # checkpoints never depend on wall-clock timestamps or completed_at order.
    predicate = "target_type IN ('match','league_season','competition_profile','competition_seasons')"
    before = source_sql(MANIFEST, {MANIFEST: old}, predicate)
    after = source_sql(MANIFEST, {MANIFEST: new}, predicate)
    return f"""SELECT competition_id, source_season_key, target_type, entity_id FROM (
        (SELECT * FROM {after} EXCEPT ALL SELECT * FROM {before})
        UNION ALL (SELECT * FROM {before} EXCEPT ALL SELECT * FROM {after}))"""


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


def refresh(manager, *, first_day=None, last_day=None):
    """Call under the FotMob writer lease; first_day is explicit bootstrap only.

    A missing/expired previous snapshot fails closed. Rebootstrap in an approved
    window rather than guessing which commits/partitions were lost.
    """
    bootstrap = first_day is not None
    if bootstrap:
        first_day = date.fromisoformat(str(first_day))
        last_day = date.fromisoformat(str(last_day))
        if first_day <= CONTROL_DAY or last_day < first_day:
            raise ValueError("invalid C1 bootstrap range")
        manager.execute_query(f"CREATE TABLE IF NOT EXISTS {TABLE} {DDL}")
    checkpoints = manager.execute_query(
        f"SELECT checkpoint_json FROM {TABLE} WHERE deadline_day=DATE '{CONTROL_DAY}' "
        "AND kind='checkpoint' AND complete"
    )
    if not bootstrap and len(checkpoints) != 1:
        raise RuntimeError("C1 requires an approved bootstrap")
    previous = json.loads(checkpoints[0][0]) if checkpoints and not bootstrap else None
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
    files = {table: partition_fingerprints(manager, table)
             for table in (MATCHES, PAYLOADS, SCOPE, SEASONS)}
    all_pairs = set(files[MATCHES])
    affected = set(all_pairs) if previous is None else set()
    if previous is not None:
        for table in (MATCHES, PAYLOADS):
            affected |= changed_pairs(_decoded_pairs(previous["files"][table]), files[table])
        changed_competitions = set()
        ids = set()
        if before[MANIFEST] != previous["snapshots"][MANIFEST]:
            for cid, sk, target_type, mid in manager.execute_query(manifest_changes_sql(
                previous["snapshots"][MANIFEST], before[MANIFEST]
            )):
                if target_type in ("competition_profile", "competition_seasons"):
                    changed_competitions.add(str(cid))
                elif target_type == "match":
                    ids.add(str(mid))
                    if cid is not None and sk is not None:
                        affected.add((str(cid), str(sk)))
                else:
                    affected.add((str(cid), str(sk)))
        for table in (SCOPE, SEASONS):
            changed_competitions |= {pair[0] for pair in changed_pairs(
                _decoded_pairs(previous["files"][table]), files[table]
            )}
        affected |= {pair for pair in all_pairs if pair[0] in changed_competitions}
        # Pool legacy evidence across natural keys. Find IDs in changed physical
        # seasons, then every season containing them; deleted keys are read back
        # from the previously published inputs below as well.
        while True:
            known = set(affected)
            if affected:
                for table in (MATCHES, PAYLOADS):
                    ids |= {str(row[0]) for row in manager.execute_query(
                        f"SELECT DISTINCT match_id FROM {source_sql(table, before, pairs_sql(affected, physical=True))}"
                    )}
                ids |= {str(row[0]) for row in manager.execute_query(
                    f"SELECT DISTINCT match_id FROM {TABLE} WHERE kind='input' "
                    f"AND deadline_day >= DATE '{first_day}' "
                    f"AND {pairs_sql(affected)}"
                )}
            if ids:
                id_predicate = "CAST(match_id AS varchar) IN (" + ",".join(map(literal, sorted(ids))) + ")"
                affected |= {(str(row[0]), str(row[1])) for row in manager.execute_query(
                    "SELECT DISTINCT competition_id, source_season_key FROM "
                    + source_sql(MATCHES, before, pairs_sql(all_pairs, physical=True) + " AND " + id_predicate)
                )}
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
    key = (CONTROL_DAY, None, None, None, "checkpoint")
    replacements[key] = (*key, *([None] * 7), generation, True, checkpoint_json)
    stage = PREFIX + "fotmob_c1_inputs__stg_" + uuid4().hex
    manager.execute_query(f"CREATE TABLE {stage} {DDL}")
    try:
        _stage_rows(manager, stage, replacements.values())
        # Metadata was read outside time travel; reject a mixed file inventory.
        # Source commits during a lost lease can never advance our checkpoint.
        if snapshots(manager) != before:
            raise RuntimeError("C1 source snapshots changed during refresh")
        manager.execute_query(merge_sql(stage))
    finally:
        manager.execute_query(f"DROP TABLE IF EXISTS {stage}")
    return {"generation": generation, "seasons": len(affected), "rows": len(replacements)}


def run_refresh(**context):
    from scrapers.fotmob.maintenance import writer_lock
    from scrapers.fotmob.trino_instrumentation import FotMobTrinoTableManager

    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    if conf:
        raise ValueError("C1 bootstrap is manual and requires an approved window")
    manager = FotMobTrinoTableManager(
        source="fotmob:c1-inputs",
        session_properties={"query_max_run_time": "300s", "query_max_execution_time": "300s"},
    )
    with writer_lock() as authority:
        manager.set_write_authority(authority)
        return refresh(manager)
