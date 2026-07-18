"""Bounded semantic DQ for immutable WhoScored backfill populations."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator, Mapping, Sequence

from scrapers.whoscored.runtime_contract import require_production_runtime_class


DQ_STAGE_TABLE = "whoscored_backfill_dq_population"
DQ_STAGE_QUALIFIED = f"iceberg.bronze.{DQ_STAGE_TABLE}"
FINGERPRINT_SHARD_COUNT = 4_096
DQ_STAGE_RETENTION_DAYS = 90
DQ_STAGE_CLEANUP_LIMIT = 100
DQ_STAGE_ARROW_BATCH_ROWS = 100_000
_IDENTITY_SEPARATOR = "\x1f"
_EMPTY_RELATION_SHA256 = hashlib.sha256(b"").hexdigest()
_STAGE_COLUMNS = (
    "population_sha256",
    "row_kind",
    "league",
    "season",
    "game_id",
    "preview_required",
    "player_id",
    "row_sha256",
    "scope_relation_sha256",
    "match_relation_sha256",
    "player_relation_sha256",
    "expected_scopes",
    "expected_matches",
    "expected_previews",
    "expected_players",
    "plan_id",
    "checkpoint_sha256",
    "staged_at",
)


class FrozenDQError(RuntimeError):
    """The frozen population or a Trino DQ response violated its contract."""


def _is_sha256(value: object) -> bool:
    token = str(value or "")
    return len(token) == 64 and all(char in "0123456789abcdef" for char in token)


def _sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _identity_sha256(parts: Sequence[object]) -> str:
    values = [str(value) for value in parts]
    if any(_IDENTITY_SEPARATOR in value for value in values):
        raise FrozenDQError("frozen DQ identity contains a reserved separator")
    payload = _IDENTITY_SEPARATOR.join(values).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _relation_sha256(row_hashes: Iterable[str]) -> str:
    """Hash identity-ordered rows via bounded, portable 12-bit shards."""

    shard_hashers = [hashlib.sha256() for _ in range(FINGERPRINT_SHARD_COUNT)]
    shard_counts = [0] * FINGERPRINT_SHARD_COUNT
    for value in row_hashes:
        if not _is_sha256(value):
            raise FrozenDQError("frozen DQ row hash is invalid")
        shard_id = int(value[:3], 16)
        shard_hashers[shard_id].update(value.encode("ascii"))
        shard_counts[shard_id] += 1
    root = hashlib.sha256()
    for shard_id, count in enumerate(shard_counts):
        if not count:
            continue
        digest = shard_hashers[shard_id].hexdigest()
        root.update(f"{shard_id}:{count}:{digest}\n".encode("ascii"))
    return root.hexdigest()


def _stage_table_ddl() -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {DQ_STAGE_QUALIFIED} (
            population_sha256 VARCHAR,
            row_kind VARCHAR,
            league VARCHAR,
            season VARCHAR,
            game_id BIGINT,
            preview_required BOOLEAN,
            player_id BIGINT,
            row_sha256 VARCHAR,
            scope_relation_sha256 VARCHAR,
            match_relation_sha256 VARCHAR,
            player_relation_sha256 VARCHAR,
            expected_scopes BIGINT,
            expected_matches BIGINT,
            expected_previews BIGINT,
            expected_players BIGINT,
            plan_id VARCHAR,
            checkpoint_sha256 VARCHAR,
            staged_at TIMESTAMP(6)
        ) WITH (partitioning = ARRAY['population_sha256'])
    """


def _stage_table_migrations() -> tuple[str, ...]:
    """Idempotent additive upgrades for a table created by an earlier deploy."""

    return (
        f"ALTER TABLE {DQ_STAGE_QUALIFIED} ADD COLUMN IF NOT EXISTS "
        "scope_relation_sha256 VARCHAR",
        f"ALTER TABLE {DQ_STAGE_QUALIFIED} ADD COLUMN IF NOT EXISTS "
        "expected_scopes BIGINT",
    )


def _stage_population_marker(population: Mapping[str, Any]) -> dict[str, Any]:
    """Validate key order and compute bounded exact relation fingerprints."""

    population_sha = str(population.get("population_sha256") or "")
    plan_id = str(population.get("plan_id") or "")
    checkpoint = population.get("checkpoint")
    checkpoint_sha = (
        str(checkpoint.get("sha256") or "")
        if isinstance(checkpoint, Mapping)
        else ""
    )
    if not all(
        _is_sha256(value) for value in (population_sha, plan_id, checkpoint_sha)
    ):
        raise FrozenDQError("frozen DQ population identity is invalid")
    scope_stages = population.get("scope_stages")
    matches = population.get("matches")
    player_ids = population.get("player_ids")
    if (
        not isinstance(scope_stages, list)
        or not isinstance(matches, list)
        or not isinstance(player_ids, list)
    ):
        raise FrozenDQError("frozen DQ population keys are invalid")

    preview_count = 0
    scope_keys: set[tuple[str, str]] = set()

    def scope_hashes() -> Iterator[str]:
        previous: tuple[str, str] | None = None
        for item in scope_stages:
            if not isinstance(item, Mapping):
                raise FrozenDQError("frozen DQ scope key is invalid")
            league = str(item.get("league") or "")
            season = str(item.get("season") or "")
            stage_ids = item.get("stage_ids")
            key = (league, season)
            if (
                not all(key)
                or item.get("scope") != f"{league}={season}"
                or not isinstance(stage_ids, list)
                or not stage_ids
                or any(type(value) is not int or value <= 0 for value in stage_ids)
                or stage_ids != sorted(set(stage_ids))
                or (previous is not None and key <= previous)
            ):
                raise FrozenDQError(
                    "frozen DQ scope keys are not unique and identity-ordered"
                )
            previous = key
            scope_keys.add(key)
            yield _identity_sha256(("scope", league, season))

    def match_hashes() -> Iterator[str]:
        nonlocal preview_count
        previous: tuple[str, str, int] | None = None
        for item in matches:
            if not isinstance(item, Mapping):
                raise FrozenDQError("frozen DQ match key is invalid")
            league = str(item.get("league") or "")
            season = str(item.get("season") or "")
            game_id = item.get("game_id")
            preview = item.get("preview_required")
            if (
                not league
                or not season
                or type(game_id) is not int
                or game_id <= 0
                or type(preview) is not bool
                or (league, season) not in scope_keys
            ):
                raise FrozenDQError("frozen DQ match key is invalid")
            key = (league, season, game_id)
            if previous is not None and key <= previous:
                raise FrozenDQError(
                    "frozen DQ match keys are not unique and identity-ordered"
                )
            previous = key
            preview_count += int(preview)
            yield _identity_sha256(
                ("match", league, season, game_id, "1" if preview else "0")
            )

    def player_hashes() -> Iterator[str]:
        previous = 0
        for value in player_ids:
            if type(value) is not int or value <= previous:
                raise FrozenDQError(
                    "frozen DQ player keys are not unique and identity-ordered"
                )
            previous = value
            yield _identity_sha256(("player", value))

    scope_relation_sha = _relation_sha256(scope_hashes())
    match_relation_sha = _relation_sha256(match_hashes())
    player_relation_sha = _relation_sha256(player_hashes())
    expected_counts = {
        "scopes": len(scope_stages),
        "matches": len(matches),
        "previews": preview_count,
        "players": len(player_ids),
    }
    counts = population.get("counts")
    if not isinstance(counts, Mapping) or any(
        type(counts.get(name)) is not int or counts[name] != expected
        for name, expected in expected_counts.items()
    ):
        raise FrozenDQError("frozen DQ population counts do not match its keys")
    return {
        "population_sha256": population_sha,
        "plan_id": plan_id,
        "checkpoint_sha256": checkpoint_sha,
        "scope_relation_sha256": scope_relation_sha,
        "match_relation_sha256": match_relation_sha,
        "player_relation_sha256": player_relation_sha,
        "expected_scopes": expected_counts["scopes"],
        "expected_matches": expected_counts["matches"],
        "expected_previews": expected_counts["previews"],
        "expected_players": expected_counts["players"],
        "expected_rows": 1 + len(scope_stages) + len(matches) + len(player_ids),
    }


def _stage_arrow_batches(
    population: Mapping[str, Any], marker: Mapping[str, Any]
) -> Iterator[Any]:
    """Yield bounded Arrow batches; the caller commits all in one snapshot."""

    import pyarrow as pa

    schema = pa.schema(
        (
            ("population_sha256", pa.string()),
            ("row_kind", pa.string()),
            ("league", pa.string()),
            ("season", pa.string()),
            ("game_id", pa.int64()),
            ("preview_required", pa.bool_()),
            ("player_id", pa.int64()),
            ("row_sha256", pa.string()),
            ("scope_relation_sha256", pa.string()),
            ("match_relation_sha256", pa.string()),
            ("player_relation_sha256", pa.string()),
            ("expected_scopes", pa.int64()),
            ("expected_matches", pa.int64()),
            ("expected_previews", pa.int64()),
            ("expected_players", pa.int64()),
            ("plan_id", pa.string()),
            ("checkpoint_sha256", pa.string()),
            ("staged_at", pa.timestamp("us")),
        )
    )
    staged_at = datetime.now(timezone.utc).replace(tzinfo=None)
    yield pa.Table.from_pylist(
        [
            {
                **{name: None for name in _STAGE_COLUMNS},
                "population_sha256": marker["population_sha256"],
                "row_kind": "marker",
                "scope_relation_sha256": marker["scope_relation_sha256"],
                "match_relation_sha256": marker["match_relation_sha256"],
                "player_relation_sha256": marker["player_relation_sha256"],
                "expected_scopes": marker["expected_scopes"],
                "expected_matches": marker["expected_matches"],
                "expected_previews": marker["expected_previews"],
                "expected_players": marker["expected_players"],
                "plan_id": marker["plan_id"],
                "checkpoint_sha256": marker["checkpoint_sha256"],
                "staged_at": staged_at,
            }
        ],
        schema=schema,
    )

    scopes = population["scope_stages"]
    for offset in range(0, len(scopes), DQ_STAGE_ARROW_BATCH_ROWS):
        chunk = scopes[offset : offset + DQ_STAGE_ARROW_BATCH_ROWS]
        count = len(chunk)
        leagues = [str(item["league"]) for item in chunk]
        seasons = [str(item["season"]) for item in chunk]
        yield pa.Table.from_arrays(
            (
                pa.array([marker["population_sha256"]] * count, pa.string()),
                pa.array(["scope"] * count, pa.string()),
                pa.array(leagues, pa.string()),
                pa.array(seasons, pa.string()),
                pa.nulls(count, pa.int64()),
                pa.nulls(count, pa.bool_()),
                pa.nulls(count, pa.int64()),
                pa.array(
                    [
                        _identity_sha256(("scope", league, season))
                        for league, season in zip(leagues, seasons)
                    ],
                    pa.string(),
                ),
                *[pa.nulls(count, pa.string()) for _ in range(3)],
                *[pa.nulls(count, pa.int64()) for _ in range(4)],
                *[pa.nulls(count, pa.string()) for _ in range(2)],
                pa.array([staged_at] * count, pa.timestamp("us")),
            ),
            schema=schema,
        )

    matches = population["matches"]
    for offset in range(0, len(matches), DQ_STAGE_ARROW_BATCH_ROWS):
        chunk = matches[offset : offset + DQ_STAGE_ARROW_BATCH_ROWS]
        count = len(chunk)
        leagues = [str(item["league"]) for item in chunk]
        seasons = [str(item["season"]) for item in chunk]
        game_ids = [int(item["game_id"]) for item in chunk]
        previews = [bool(item["preview_required"]) for item in chunk]
        row_hashes = [
            _identity_sha256(
                (
                    "match",
                    league,
                    season,
                    game_id,
                    "1" if preview else "0",
                )
            )
            for league, season, game_id, preview in zip(
                leagues, seasons, game_ids, previews
            )
        ]
        yield pa.Table.from_arrays(
            (
                pa.array([marker["population_sha256"]] * count, pa.string()),
                pa.array(["match"] * count, pa.string()),
                pa.array(leagues, pa.string()),
                pa.array(seasons, pa.string()),
                pa.array(game_ids, pa.int64()),
                pa.array(previews, pa.bool_()),
                pa.nulls(count, pa.int64()),
                pa.array(row_hashes, pa.string()),
                *[pa.nulls(count, pa.string()) for _ in range(3)],
                *[pa.nulls(count, pa.int64()) for _ in range(4)],
                *[pa.nulls(count, pa.string()) for _ in range(2)],
                pa.array([staged_at] * count, pa.timestamp("us")),
            ),
            schema=schema,
        )

    player_ids = population["player_ids"]
    for offset in range(0, len(player_ids), DQ_STAGE_ARROW_BATCH_ROWS):
        chunk = player_ids[offset : offset + DQ_STAGE_ARROW_BATCH_ROWS]
        count = len(chunk)
        yield pa.Table.from_arrays(
            (
                pa.array([marker["population_sha256"]] * count, pa.string()),
                pa.array(["player"] * count, pa.string()),
                *[pa.nulls(count, pa.string()) for _ in range(2)],
                pa.nulls(count, pa.int64()),
                pa.nulls(count, pa.bool_()),
                pa.array(chunk, pa.int64()),
                pa.array(
                    [_identity_sha256(("player", value)) for value in chunk],
                    pa.string(),
                ),
                *[pa.nulls(count, pa.string()) for _ in range(3)],
                *[pa.nulls(count, pa.int64()) for _ in range(4)],
                *[pa.nulls(count, pa.string()) for _ in range(2)],
                pa.array([staged_at] * count, pa.timestamp("us")),
            ),
            schema=schema,
        )


def stage_frozen_population(
    population: Mapping[str, Any], *, writer: Any | None = None
) -> dict[str, Any]:
    """Materialise one immutable frozen-key partition, or prove it exists.

    An existing population partition is never repaired automatically.  Its
    marker must match the receipt-derived artifact exactly; the subsequent
    set-based proof detects any row-level corruption against that marker.
    """

    require_production_runtime_class(operation="WhoScored frozen DQ persistence")
    from scrapers.base.iceberg_writer import IcebergWriter

    marker = _stage_population_marker(population)
    writer = writer or IcebergWriter(catalog="iceberg")
    trino = writer._get_trino_manager()
    trino._execute(_stage_table_ddl())
    for query in _stage_table_migrations():
        trino._execute(query)
    columns = {str(value).lower() for value in trino.get_table_columns("bronze", DQ_STAGE_TABLE)}
    if columns != set(_STAGE_COLUMNS):
        raise FrozenDQError(
            "frozen DQ stage table has a conflicting column contract"
        )
    try:
        writer.require_exact_identity_partition(
            database="bronze",
            table=DQ_STAGE_TABLE,
            partition_column="population_sha256",
        )
    except RuntimeError as exc:
        raise FrozenDQError(
            "frozen DQ stage table has a conflicting partition contract"
        ) from exc

    population_sql = _sql_string(marker["population_sha256"])
    existing = trino.execute_query(
        f"""
        SELECT COUNT(*),
               COUNT_IF(row_kind='marker'),
               MAX(plan_id), MAX(checkpoint_sha256),
               MAX(scope_relation_sha256), MAX(match_relation_sha256),
               MAX(player_relation_sha256),
               MAX(expected_scopes),
               MAX(expected_matches), MAX(expected_previews),
               MAX(expected_players)
        FROM {DQ_STAGE_QUALIFIED}
        WHERE population_sha256={population_sql}
        """
    )
    row = existing[0] if existing else (0,) * 11
    existing_rows = int(row[0] or 0)
    if existing_rows:
        actual_marker = (
            int(row[1] or 0),
            str(row[2] or ""),
            str(row[3] or ""),
            str(row[4] or ""),
            str(row[5] or ""),
            str(row[6] or ""),
            int(row[7] or 0),
            int(row[8] or 0),
            int(row[9] or 0),
            int(row[10] or 0),
            existing_rows,
        )
        expected_marker = (
            1,
            marker["plan_id"],
            marker["checkpoint_sha256"],
            marker["scope_relation_sha256"],
            marker["match_relation_sha256"],
            marker["player_relation_sha256"],
            marker["expected_scopes"],
            marker["expected_matches"],
            marker["expected_previews"],
            marker["expected_players"],
            marker["expected_rows"],
        )
        if actual_marker != expected_marker:
            raise FrozenDQError(
                "existing frozen DQ stage partition conflicts with its artifact"
            )
    else:
        writer.replace_identity_partition_arrow_batches(
            _stage_arrow_batches(population, marker),
            database="bronze",
            table=DQ_STAGE_TABLE,
            partition_column="population_sha256",
            partition_value=marker["population_sha256"],
        )

    try:
        snapshot_id = writer.current_snapshot_id(
            database="bronze", table=DQ_STAGE_TABLE
        )
    except RuntimeError as exc:
        raise FrozenDQError(
            "frozen DQ stage has no committed Iceberg snapshot"
        ) from exc
    return {
        **marker,
        "table": DQ_STAGE_QUALIFIED,
        "snapshot_id": snapshot_id,
    }


def _merge(
    counters: dict[str, int], names: Sequence[str], row: Sequence[Any]
) -> None:
    if len(row) != len(names):
        raise FrozenDQError(
            f"historical DQ returned {len(row)} counters, expected {len(names)}"
        )
    for name, value in zip(names, row):
        counters[name] = counters.get(name, 0) + int(value or 0)


def _staged_table_source(relation: Mapping[str, Any]) -> str:
    table = str(relation.get("table") or "")
    snapshot_id = relation.get("snapshot_id")
    if table != DQ_STAGE_QUALIFIED or type(snapshot_id) is not int or snapshot_id <= 0:
        raise FrozenDQError("frozen DQ staged relation reference is invalid")
    return f"{table} FOR VERSION AS OF {snapshot_id}"


def staged_scope_relation_sql(relation: Mapping[str, Any]) -> str:
    """Return the exact snapshot-pinned scope-key relation for set joins."""

    source = _staged_table_source(relation)
    population_sha256 = relation.get("population_sha256")
    if not _is_sha256(population_sha256):
        raise FrozenDQError("frozen DQ staged scope identity is invalid")
    return (
        f"SELECT league,season FROM {source} "
        f"WHERE population_sha256={_sql_string(population_sha256)} "
        "AND row_kind='scope'"
    )


def _validate_staged_relation(cur: Any, relation: Mapping[str, Any]) -> None:
    """Prove the snapshot-pinned rows equal the content-addressed artifact."""

    source = _staged_table_source(relation)
    population_sql = _sql_string(relation.get("population_sha256"))
    expected_values = {
        "rows": relation.get("expected_rows"),
        "scopes": relation.get("expected_scopes"),
        "matches": relation.get("expected_matches"),
        "previews": relation.get("expected_previews"),
        "players": relation.get("expected_players"),
    }
    if any(
        type(value) is not int or value < 0
        for value in expected_values.values()
    ):
        raise FrozenDQError("frozen DQ staged relation counts are invalid")
    expected = {key: int(value) for key, value in expected_values.items()}
    for name in (
        "population_sha256",
        "plan_id",
        "checkpoint_sha256",
        "scope_relation_sha256",
        "match_relation_sha256",
        "player_relation_sha256",
    ):
        if not _is_sha256(relation.get(name)):
            raise FrozenDQError("frozen DQ staged relation identity is invalid")
    marker_predicate = " AND ".join(
        (
            f"plan_id={_sql_string(relation.get('plan_id'))}",
            "checkpoint_sha256="
            + _sql_string(relation.get("checkpoint_sha256")),
            "scope_relation_sha256="
            + _sql_string(relation.get("scope_relation_sha256")),
            "match_relation_sha256="
            + _sql_string(relation.get("match_relation_sha256")),
            "player_relation_sha256="
            + _sql_string(relation.get("player_relation_sha256")),
            f"expected_scopes={expected['scopes']}",
            f"expected_matches={expected['matches']}",
            f"expected_previews={expected['previews']}",
            f"expected_players={expected['players']}",
        )
    )
    cur.execute(
        f"""
        WITH staged AS (
            SELECT * FROM {source}
            WHERE population_sha256={population_sql}
        ),
        scope_shards AS (
            SELECT CAST(from_base(SUBSTR(row_sha256,1,3),16) AS INTEGER) shard_id,
                   COUNT(*) row_count,
                   LOWER(TO_HEX(SHA256(TO_UTF8(ARRAY_JOIN(
                       ARRAY_AGG(row_sha256 ORDER BY league,season), ''
                   ))))) shard_sha256
            FROM staged WHERE row_kind='scope' GROUP BY 1
        ),
        match_shards AS (
            SELECT CAST(from_base(SUBSTR(row_sha256,1,3),16) AS INTEGER) shard_id,
                   COUNT(*) row_count,
                   LOWER(TO_HEX(SHA256(TO_UTF8(ARRAY_JOIN(
                       ARRAY_AGG(row_sha256 ORDER BY league,season,game_id), ''
                   ))))) shard_sha256
            FROM staged WHERE row_kind='match' GROUP BY 1
        ),
        player_shards AS (
            SELECT CAST(from_base(SUBSTR(row_sha256,1,3),16) AS INTEGER) shard_id,
                   COUNT(*) row_count,
                   LOWER(TO_HEX(SHA256(TO_UTF8(ARRAY_JOIN(
                       ARRAY_AGG(row_sha256 ORDER BY player_id), ''
                   ))))) shard_sha256
            FROM staged WHERE row_kind='player' GROUP BY 1
        ),
        scope_relation AS (
            SELECT COALESCE(LOWER(TO_HEX(SHA256(TO_UTF8(ARRAY_JOIN(ARRAY_AGG(
                CAST(shard_id AS VARCHAR)||':'||CAST(row_count AS VARCHAR)||':'||
                shard_sha256||CHR(10) ORDER BY shard_id
            ), ''))))), '{_EMPTY_RELATION_SHA256}') relation_sha256
            FROM scope_shards
        ),
        match_relation AS (
            SELECT COALESCE(LOWER(TO_HEX(SHA256(TO_UTF8(ARRAY_JOIN(ARRAY_AGG(
                CAST(shard_id AS VARCHAR)||':'||CAST(row_count AS VARCHAR)||':'||
                shard_sha256||CHR(10) ORDER BY shard_id
            ), ''))))), '{_EMPTY_RELATION_SHA256}') relation_sha256
            FROM match_shards
        ),
        player_relation AS (
            SELECT COALESCE(LOWER(TO_HEX(SHA256(TO_UTF8(ARRAY_JOIN(ARRAY_AGG(
                CAST(shard_id AS VARCHAR)||':'||CAST(row_count AS VARCHAR)||':'||
                shard_sha256||CHR(10) ORDER BY shard_id
            ), ''))))), '{_EMPTY_RELATION_SHA256}') relation_sha256
            FROM player_shards
        )
        SELECT
            COUNT(*),
            COUNT_IF(row_kind='marker'),
            COUNT_IF(row_kind='marker' AND {marker_predicate}),
            COUNT_IF(row_kind='scope'),
            COUNT_IF(row_kind='match'),
            COUNT_IF(row_kind='match' AND preview_required),
            COUNT_IF(row_kind='player'),
            COUNT_IF(
                staged_at IS NULL OR
                row_kind NOT IN ('marker','scope','match','player') OR
                (row_kind='marker' AND (
                    league IS NOT NULL OR season IS NOT NULL OR game_id IS NOT NULL OR
                    preview_required IS NOT NULL OR player_id IS NOT NULL OR
                    row_sha256 IS NOT NULL
                )) OR
                (row_kind='scope' AND (
                    league IS NULL OR league='' OR season IS NULL OR season='' OR
                    game_id IS NOT NULL OR preview_required IS NOT NULL OR
                    player_id IS NOT NULL OR row_sha256 IS NULL OR
                    plan_id IS NOT NULL OR checkpoint_sha256 IS NOT NULL OR
                    scope_relation_sha256 IS NOT NULL OR
                    match_relation_sha256 IS NOT NULL OR
                    player_relation_sha256 IS NOT NULL OR
                    expected_scopes IS NOT NULL OR expected_matches IS NOT NULL OR
                    expected_previews IS NOT NULL OR expected_players IS NOT NULL
                )) OR
                (row_kind='match' AND (
                    league IS NULL OR league='' OR season IS NULL OR season='' OR
                    game_id IS NULL OR game_id<=0 OR preview_required IS NULL OR
                    player_id IS NOT NULL OR row_sha256 IS NULL OR
                    plan_id IS NOT NULL OR checkpoint_sha256 IS NOT NULL OR
                    scope_relation_sha256 IS NOT NULL OR
                    match_relation_sha256 IS NOT NULL OR
                    player_relation_sha256 IS NOT NULL OR
                    expected_scopes IS NOT NULL OR
                    expected_matches IS NOT NULL OR expected_previews IS NOT NULL OR
                    expected_players IS NOT NULL
                )) OR
                (row_kind='player' AND (
                    league IS NOT NULL OR season IS NOT NULL OR game_id IS NOT NULL OR
                    preview_required IS NOT NULL OR player_id IS NULL OR player_id<=0 OR
                    row_sha256 IS NULL OR plan_id IS NOT NULL OR
                    checkpoint_sha256 IS NOT NULL OR
                    scope_relation_sha256 IS NOT NULL OR
                    match_relation_sha256 IS NOT NULL OR
                    player_relation_sha256 IS NOT NULL OR expected_matches IS NOT NULL OR
                    expected_scopes IS NOT NULL OR
                    expected_previews IS NOT NULL OR expected_players IS NOT NULL
                ))
            ),
            COUNT_IF(row_kind='scope' AND row_sha256 IS DISTINCT FROM LOWER(TO_HEX(
                SHA256(TO_UTF8('scope'||CHR(31)||league||CHR(31)||season))
            ))),
            COUNT_IF(row_kind='match' AND row_sha256 IS DISTINCT FROM LOWER(TO_HEX(
                SHA256(TO_UTF8('match'||CHR(31)||league||CHR(31)||season||CHR(31)||
                    CAST(game_id AS VARCHAR)||CHR(31)||
                    CASE WHEN preview_required THEN '1' ELSE '0' END))
            ))),
            COUNT_IF(row_kind='player' AND row_sha256 IS DISTINCT FROM LOWER(TO_HEX(
                SHA256(TO_UTF8('player'||CHR(31)||CAST(player_id AS VARCHAR)))
            ))),
            (SELECT COUNT(*)-COUNT(DISTINCT ROW(league,season))
             FROM staged WHERE row_kind='scope'),
            (SELECT COUNT(*)-COUNT(DISTINCT ROW(league,season,game_id))
             FROM staged WHERE row_kind='match'),
            (SELECT COUNT(*)-COUNT(DISTINCT player_id)
             FROM staged WHERE row_kind='player'),
            (SELECT relation_sha256 FROM scope_relation),
            (SELECT relation_sha256 FROM match_relation),
            (SELECT relation_sha256 FROM player_relation)
        FROM staged
        """
    )
    rows = cur.fetchall()
    if len(rows) != 1 or len(rows[0]) != 17:
        raise FrozenDQError("frozen DQ stage proof returned an invalid row")
    row = rows[0]
    actual = {
        "rows": int(row[0] or 0),
        "marker_rows": int(row[1] or 0),
        "valid_marker_rows": int(row[2] or 0),
        "scopes": int(row[3] or 0),
        "matches": int(row[4] or 0),
        "previews": int(row[5] or 0),
        "players": int(row[6] or 0),
        "invalid_shapes": int(row[7] or 0),
        "invalid_scope_hashes": int(row[8] or 0),
        "invalid_match_hashes": int(row[9] or 0),
        "invalid_player_hashes": int(row[10] or 0),
        "duplicate_scope_keys": int(row[11] or 0),
        "duplicate_match_keys": int(row[12] or 0),
        "duplicate_player_keys": int(row[13] or 0),
        "scope_relation_sha256": str(row[14] or ""),
        "match_relation_sha256": str(row[15] or ""),
        "player_relation_sha256": str(row[16] or ""),
    }
    failures = {
        key: value
        for key, value in actual.items()
        if (
            key in {"rows", "scopes", "matches", "previews", "players"}
            and value != expected[key]
        )
        or (
            key in {"marker_rows", "valid_marker_rows"} and value != 1
        )
        or (
            key
            in {
                "invalid_shapes",
                "invalid_scope_hashes",
                "invalid_match_hashes",
                "invalid_player_hashes",
                "duplicate_scope_keys",
                "duplicate_match_keys",
                "duplicate_player_keys",
            }
            and value != 0
        )
        or (
            key == "scope_relation_sha256"
            and value != relation.get("scope_relation_sha256")
        )
        or (
            key == "match_relation_sha256"
            and value != relation.get("match_relation_sha256")
        )
        or (
            key == "player_relation_sha256"
            and value != relation.get("player_relation_sha256")
        )
    }
    if failures:
        raise FrozenDQError(f"frozen DQ staged relation proof failed: {failures}")


_CORE_NAMES = (
    "completed_matches",
    "missing_frozen_schedule_matches",
    "uncovered_completed_matches",
    "parse_failed_matches",
    "retryable_matches",
    "terminal_matches",
    "unproven_not_available_matches",
    "required_previews",
    "uncovered_previews",
    "parse_failed_previews",
    "retryable_previews",
    "terminal_previews",
    "event_game_mismatches",
    "lineup_game_mismatches",
    "incomplete_final_opta_games",
    "uncovered_incident_summaries",
    "uncovered_bet_matches",
    "incomplete_match_snapshots",
    "invalid_event_identity_rows",
    "duplicate_source_event_ids",
    "duplicate_team_event_ids",
    "inconsistent_match_dataset_states",
    "incomplete_preview_snapshots",
    "inconsistent_preview_dataset_states",
)


def _run_match_core(
    cur: Any,
    relation: Mapping[str, Any],
    *,
    parser_sql: str,
    availability_sql: str,
) -> Sequence[Any]:
    """Run four fixed-size query families below Trino's 150-stage ceiling."""

    source = _staged_table_source(relation)
    population_sql = _sql_string(relation.get("population_sha256"))
    groups: tuple[tuple[tuple[str, ...], str], ...] = (
        (
            (*_CORE_NAMES[:12], *_CORE_NAMES[22:]),
            f"""
            /* whoscored_frozen_coverage */
            WITH frozen AS (
                SELECT league,season,game_id,preview_required FROM {source}
                WHERE population_sha256={population_sql} AND row_kind='match'
            ), schedule AS (
                SELECT * FROM (
                    SELECT s.*,ROW_NUMBER() OVER (
                        PARTITION BY s.league,s.season,s.game_id
                        ORDER BY s._ingested_at DESC
                    ) rn
                    FROM iceberg.bronze.whoscored_schedule_current s
                    JOIN frozen f ON f.league=s.league AND f.season=s.season
                     AND f.game_id=CAST(s.game_id AS BIGINT)
                ) WHERE rn=1
            ), latest_match AS (
                SELECT * FROM (
                    SELECT m.*,ROW_NUMBER() OVER (
                        PARTITION BY m.league,m.season,m.game_id
                        ORDER BY COALESCE(
                            m.completed_at,m.fetched_at,m._ingested_at
                        ) DESC,COALESCE(m.batch_id,'') DESC
                    ) rn
                    FROM iceberg.bronze.whoscored_match_ingest_manifest m
                    JOIN frozen f ON f.league=m.league AND f.season=m.season
                     AND f.game_id=CAST(m.game_id AS BIGINT)
                ) WHERE rn=1
            ), valid_match AS (
                SELECT * FROM latest_match
                WHERE parser_version={parser_sql}
                  AND availability_version={availability_sql} AND (
                    (state='success' AND batch_id LIKE 'ws2-%'
                     AND raw_uri IS NOT NULL AND payload_sha256 IS NOT NULL)
                    OR (state='not_available' AND failure_code IS NOT NULL
                     AND (raw_uri IS NOT NULL OR http_status IN (404,410)))
                  )
            ), latest_preview AS (
                SELECT * FROM (
                    SELECT p.*,ROW_NUMBER() OVER (
                        PARTITION BY p.league,p.season,p.game_id
                        ORDER BY COALESCE(
                            p.completed_at,p.fetched_at,p._ingested_at
                        ) DESC,COALESCE(p.batch_id,'') DESC
                    ) rn
                    FROM iceberg.bronze.whoscored_preview_ingest_manifest p
                    JOIN frozen f ON f.preview_required
                     AND f.league=p.league AND f.season=p.season
                     AND f.game_id=CAST(p.game_id AS BIGINT)
                ) WHERE rn=1
            ), valid_preview AS (
                SELECT * FROM latest_preview WHERE parser_version={parser_sql} AND (
                    (state='success' AND batch_id LIKE 'wsp2-%'
                     AND raw_uri IS NOT NULL AND payload_sha256 IS NOT NULL)
                    OR (state='not_available'
                     AND availability_version={availability_sql}
                     AND failure_code IS NOT NULL
                     AND (raw_uri IS NOT NULL OR http_status IN (404,410)))
                )
            ), success_preview AS (
                SELECT * FROM valid_preview WHERE state='success'
            )
            SELECT
                (SELECT COUNT(*) FROM frozen),
                (SELECT COUNT(*) FROM frozen f LEFT JOIN schedule s
                 ON s.league=f.league AND s.season=f.season
                AND CAST(s.game_id AS BIGINT)=f.game_id WHERE s.game_id IS NULL),
                (SELECT COUNT(*) FROM frozen f LEFT JOIN valid_match m
                 ON m.league=f.league AND m.season=f.season
                AND CAST(m.game_id AS BIGINT)=f.game_id WHERE m.game_id IS NULL),
                (SELECT COUNT(*) FROM latest_match WHERE state='parse_failed'),
                (SELECT COUNT(*) FROM latest_match WHERE state='retryable'),
                (SELECT COUNT(*) FROM latest_match WHERE state='terminal'),
                (SELECT COUNT(*) FROM latest_match WHERE state='not_available' AND (
                    parser_version IS DISTINCT FROM {parser_sql}
                    OR availability_version IS DISTINCT FROM {availability_sql}
                    OR failure_code IS NULL OR (raw_uri IS NULL AND (
                        http_status IS NULL OR http_status NOT IN (404,410)
                    ))
                )),
                (SELECT COUNT(*) FROM frozen WHERE preview_required),
                (SELECT COUNT(*) FROM frozen f LEFT JOIN valid_preview p
                 ON f.preview_required AND p.league=f.league AND p.season=f.season
                AND CAST(p.game_id AS BIGINT)=f.game_id
                 WHERE f.preview_required AND p.game_id IS NULL),
                (SELECT COUNT(*) FROM latest_preview WHERE state='parse_failed'),
                (SELECT COUNT(*) FROM latest_preview WHERE state='retryable'),
                (SELECT COUNT(*) FROM latest_preview WHERE state='terminal'),
                (SELECT COUNT(*) FROM success_preview
                 WHERE dataset_statuses_json IS NULL
                    OR COALESCE(json_size(
                        TRY(json_parse(dataset_statuses_json)),'$'
                    ),-1)<>3
                    OR COALESCE(json_extract_scalar(
                        TRY(json_parse(dataset_statuses_json)),'$.missing_players'
                    ),'') NOT IN ('available','empty','not_available')
                    OR COALESCE(json_extract_scalar(
                        TRY(json_parse(dataset_statuses_json)),'$.preview_lineups'
                    ),'') NOT IN ('available','empty','not_available')
                    OR COALESCE(json_extract_scalar(
                        TRY(json_parse(dataset_statuses_json)),'$.preview_sections'
                    ),'') NOT IN ('available','empty','not_available')),
                (SELECT COUNT(*) FROM success_preview p WHERE EXISTS (
                    SELECT 1 FROM (VALUES
                        ('missing_players'),('preview_lineups'),('preview_sections')
                    ) AS d(name) WHERE (
                        COALESCE(json_extract_scalar(
                            TRY(json_parse(p.dataset_statuses_json)),'$.'||d.name
                        ),'')='available' AND COALESCE(TRY_CAST(
                            json_extract_scalar(
                                TRY(json_parse(p.entity_counts_json)),'$.'||d.name
                            ) AS BIGINT
                        ),-1)<=0
                    ) OR (
                        COALESCE(json_extract_scalar(
                            TRY(json_parse(p.dataset_statuses_json)),'$.'||d.name
                        ),'') IN ('empty','not_available') AND COALESCE(TRY_CAST(
                            json_extract_scalar(
                                TRY(json_parse(p.entity_counts_json)),'$.'||d.name
                            ) AS BIGINT
                        ),-1)<>0
                    )
                ))
            """,
        ),
        (
            (
                "event_game_mismatches",
                "incomplete_final_opta_games",
                "invalid_event_identity_rows",
                "duplicate_source_event_ids",
                "duplicate_team_event_ids",
            ),
            f"""
            /* whoscored_frozen_events */
            WITH frozen AS (
                SELECT league,season,game_id FROM {source}
                WHERE population_sha256={population_sql} AND row_kind='match'
            ), success_match AS (
                SELECT * FROM (
                    SELECT m.*,ROW_NUMBER() OVER (
                        PARTITION BY m.league,m.season,m.game_id
                        ORDER BY COALESCE(
                            m.completed_at,m.fetched_at,m._ingested_at
                        ) DESC,COALESCE(m.batch_id,'') DESC
                    ) rn
                    FROM iceberg.bronze.whoscored_match_ingest_manifest m
                    JOIN frozen f ON f.league=m.league AND f.season=m.season
                     AND f.game_id=CAST(m.game_id AS BIGINT)
                ) WHERE rn=1 AND state='success' AND parser_version={parser_sql}
                  AND availability_version={availability_sql}
                  AND batch_id LIKE 'ws2-%' AND raw_uri IS NOT NULL
                  AND payload_sha256 IS NOT NULL
            ), event_rows AS (
                SELECT e.*,
                       COUNT(*) OVER (PARTITION BY e.league,e.season,e.game_id,
                           e.source_event_id) source_duplicate_count,
                       COUNT(*) OVER (PARTITION BY e.league,e.season,e.game_id,
                           e.team_id,e.team_event_id) team_duplicate_count
                FROM iceberg.bronze.whoscored_events_current e
                JOIN success_match m ON m.league=e.league AND m.season=e.season
                 AND CAST(m.game_id AS BIGINT)=CAST(e.game_id AS BIGINT)
                 AND m.batch_id=e._game_batch_id
            ), events_by_game AS (
                SELECT m.league,m.season,m.game_id,COUNT(e.game_id) rows_count,
                       MAX(COALESCE(e.expanded_minute,e.minute)) max_minute,
                       COUNT_IF(e.game_id IS NOT NULL AND (
                           e.source_event_id IS NULL OR e.source_event_id<=0
                           OR e.team_event_id IS NULL OR e.team_event_id<=0
                           OR TRY_CAST(TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json,'$.id'
                           )) AS DOUBLE) AS BIGINT) IS DISTINCT FROM COALESCE(
                               e.opta_event_id,e.source_event_id
                           ) OR TRY_CAST(TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json,'$.eventId'
                           )) AS DOUBLE) AS BIGINT) IS DISTINCT FROM e.team_event_id
                           OR TRY_CAST(TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json,'$.relatedEventId'
                           )) AS DOUBLE) AS BIGINT) IS DISTINCT FROM
                               e.related_team_event_id
                       )) invalid_identity_rows,
                       COUNT(DISTINCT IF(
                           e.source_duplicate_count>1,
                           COALESCE(CAST(e.source_event_id AS VARCHAR),'<null>'),
                           NULL
                       )) duplicate_source_ids,
                       COUNT(DISTINCT IF(e.team_duplicate_count>1,
                           COALESCE(CAST(e.team_id AS VARCHAR),'<null>')||':'||
                           COALESCE(CAST(e.team_event_id AS VARCHAR),'<null>'),NULL
                       )) duplicate_team_ids
                FROM success_match m LEFT JOIN event_rows e
                  ON e.league=m.league AND e.season=m.season
                 AND CAST(e.game_id AS BIGINT)=CAST(m.game_id AS BIGINT)
                GROUP BY 1,2,3
            ), match_rows AS (
                SELECT m.league,m.season,m.game_id,
                       MAX(h.expanded_max_minute) expanded_max_minute
                FROM success_match m
                LEFT JOIN iceberg.bronze.whoscored_matches_current h
                  ON h.league=m.league AND h.season=m.season
                 AND CAST(h.game_id AS BIGINT)=CAST(m.game_id AS BIGINT)
                 AND h._game_batch_id=m.batch_id
                GROUP BY 1,2,3
            )
            SELECT
                (SELECT COUNT(*) FROM success_match m JOIN events_by_game e
                 ON e.league=m.league AND e.season=m.season
                AND e.game_id=m.game_id WHERE e.rows_count<>m.events_count),
                (SELECT COUNT(*) FROM success_match m
                 JOIN events_by_game e ON e.league=m.league AND e.season=m.season
                  AND e.game_id=m.game_id
                 JOIN match_rows h ON h.league=m.league AND h.season=m.season
                  AND h.game_id=m.game_id
                 WHERE m.is_final=TRUE AND m.is_opta=TRUE AND (
                    COALESCE(json_extract_scalar(
                        TRY(json_parse(m.dataset_statuses_json)),'$.events'
                    ),'')<>'available'
                    OR e.rows_count<GREATEST(20,COALESCE(h.expanded_max_minute,90))
                    OR COALESCE(e.max_minute,-1)<GREATEST(
                        1,COALESCE(h.expanded_max_minute,90)-15
                    )
                 )),
                (SELECT COALESCE(SUM(invalid_identity_rows),0) FROM events_by_game),
                (SELECT COALESCE(SUM(duplicate_source_ids),0) FROM events_by_game),
                (SELECT COALESCE(SUM(duplicate_team_ids),0) FROM events_by_game)
            """,
        ),
        (
            (
                "lineup_game_mismatches",
                "incomplete_match_snapshots",
                "inconsistent_match_dataset_states",
            ),
            f"""
            /* whoscored_frozen_match_snapshots */
            WITH frozen AS (
                SELECT league,season,game_id FROM {source}
                WHERE population_sha256={population_sql} AND row_kind='match'
            ), success_match AS (
                SELECT * FROM (
                    SELECT m.*,ROW_NUMBER() OVER (
                        PARTITION BY m.league,m.season,m.game_id
                        ORDER BY COALESCE(
                            m.completed_at,m.fetched_at,m._ingested_at
                        ) DESC,COALESCE(m.batch_id,'') DESC
                    ) rn
                    FROM iceberg.bronze.whoscored_match_ingest_manifest m
                    JOIN frozen f ON f.league=m.league AND f.season=m.season
                     AND f.game_id=CAST(m.game_id AS BIGINT)
                ) WHERE rn=1 AND state='success' AND parser_version={parser_sql}
                  AND availability_version={availability_sql}
                  AND batch_id LIKE 'ws2-%' AND raw_uri IS NOT NULL
                  AND payload_sha256 IS NOT NULL
            ), lineups_by_game AS (
                SELECT m.league,m.season,m.game_id,COUNT(l.game_id) rows_count
                FROM success_match m
                LEFT JOIN iceberg.bronze.whoscored_lineups_current l
                  ON l.league=m.league AND l.season=m.season
                 AND CAST(l.game_id AS BIGINT)=CAST(m.game_id AS BIGINT)
                 AND l._game_batch_id=m.batch_id GROUP BY 1,2,3
            )
            SELECT
                (SELECT COUNT(*) FROM success_match m JOIN lineups_by_game l
                 ON l.league=m.league AND l.season=m.season
                AND l.game_id=m.game_id WHERE l.rows_count<>m.lineups_count),
                (SELECT COUNT(*) FROM success_match
                 WHERE dataset_statuses_json IS NULL OR COALESCE(json_size(
                    TRY(json_parse(dataset_statuses_json)),'$'
                 ),-1)<>7
                 OR COALESCE(json_extract_scalar(
                    TRY(json_parse(dataset_statuses_json)),'$.matches'
                 ),'') NOT IN ('available','empty','not_available')
                 OR COALESCE(json_extract_scalar(
                    TRY(json_parse(dataset_statuses_json)),'$.events'
                 ),'') NOT IN ('available','empty','not_available')
                 OR COALESCE(json_extract_scalar(
                    TRY(json_parse(dataset_statuses_json)),'$.lineups'
                 ),'') NOT IN ('available','empty','not_available')
                 OR COALESCE(json_extract_scalar(
                    TRY(json_parse(dataset_statuses_json)),'$.substitutions'
                 ),'') NOT IN ('available','empty','not_available')
                 OR COALESCE(json_extract_scalar(
                    TRY(json_parse(dataset_statuses_json)),'$.formations'
                 ),'') NOT IN ('available','empty','not_available')
                 OR COALESCE(json_extract_scalar(
                    TRY(json_parse(dataset_statuses_json)),'$.team_match_stats'
                 ),'') NOT IN ('available','empty','not_available')
                 OR COALESCE(json_extract_scalar(
                    TRY(json_parse(dataset_statuses_json)),'$.player_match_stats'
                 ),'') NOT IN ('available','empty','not_available')),
                (SELECT COUNT(*) FROM success_match m WHERE
                    COALESCE(json_extract_scalar(
                        TRY(json_parse(m.dataset_statuses_json)),'$.matches'
                    ),'')<>'available'
                    OR COALESCE(TRY_CAST(json_extract_scalar(
                        TRY(json_parse(m.entity_counts_json)),'$.matches'
                    ) AS BIGINT),-1)<>1
                    OR EXISTS (SELECT 1 FROM (VALUES
                        ('events'),('lineups'),('substitutions'),('formations'),
                        ('team_match_stats'),('player_match_stats')
                    ) AS d(name) WHERE (
                        COALESCE(json_extract_scalar(
                            TRY(json_parse(m.dataset_statuses_json)),'$.'||d.name
                        ),'')='available' AND COALESCE(TRY_CAST(
                            json_extract_scalar(
                                TRY(json_parse(m.entity_counts_json)),'$.'||d.name
                            ) AS BIGINT
                        ),-1)<=0
                    ) OR (
                        COALESCE(json_extract_scalar(
                            TRY(json_parse(m.dataset_statuses_json)),'$.'||d.name
                        ),'') IN ('empty','not_available') AND COALESCE(TRY_CAST(
                            json_extract_scalar(
                                TRY(json_parse(m.entity_counts_json)),'$.'||d.name
                            ) AS BIGINT
                        ),-1)<>0
                    ))
                )
            """,
        ),
        (
            ("uncovered_incident_summaries", "uncovered_bet_matches"),
            f"""
            /* whoscored_frozen_schedule_extras */
            WITH frozen AS (
                SELECT league,season,game_id FROM {source}
                WHERE population_sha256={population_sql} AND row_kind='match'
            ), schedule AS (
                SELECT * FROM (
                    SELECT s.*,ROW_NUMBER() OVER (
                        PARTITION BY s.league,s.season,s.game_id
                        ORDER BY s._ingested_at DESC
                    ) rn
                    FROM iceberg.bronze.whoscored_schedule_current s
                    JOIN frozen f ON f.league=s.league AND f.season=s.season
                     AND f.game_id=CAST(s.game_id AS BIGINT)
                ) WHERE rn=1
            ), incidents_by_game AS (
                SELECT f.league,f.season,f.game_id,COUNT(i.game_id) rows_count
                FROM frozen f
                LEFT JOIN iceberg.bronze.whoscored_match_incidents_current i
                  ON i.league=f.league AND i.season=f.season
                 AND CAST(i.game_id AS BIGINT)=f.game_id GROUP BY 1,2,3
            ), bets_by_game AS (
                SELECT f.league,f.season,f.game_id,COUNT(b.game_id) rows_count
                FROM frozen f
                LEFT JOIN iceberg.bronze.whoscored_match_bets_current b
                  ON b.league=f.league AND b.season=f.season
                 AND CAST(b.game_id AS BIGINT)=f.game_id GROUP BY 1,2,3
            )
            SELECT
                (SELECT COUNT(*) FROM schedule s JOIN incidents_by_game i
                 ON i.league=s.league AND i.season=s.season
                AND i.game_id=CAST(s.game_id AS BIGINT)
                 WHERE s.has_incidents_summary=TRUE AND i.rows_count=0),
                (SELECT COUNT(*) FROM schedule s JOIN bets_by_game b
                 ON b.league=s.league AND b.season=s.season
                AND b.game_id=CAST(s.game_id AS BIGINT)
                 WHERE COALESCE(json_size(TRY(json_parse(s.bets)),'$'),0)>0
                   AND b.rows_count=0)
            """,
        ),
    )
    counters: dict[str, int] = {}
    for names, query in groups:
        cur.execute(query)
        rows = cur.fetchall()
        if len(rows) != 1:
            raise FrozenDQError("frozen match DQ family returned no row")
        _merge(counters, names, rows[0])
    return tuple(counters.get(name, 0) for name in _CORE_NAMES)


def _dataset_parity_batch(
    cur: Any,
    *,
    relation: Mapping[str, Any],
    preview_only: bool,
    dataset_tables: Mapping[str, str],
    manifest_table: str,
    batch_column: str,
    batch_prefix: str,
    parser_sql: str,
    availability_sql: str | None,
) -> dict[str, dict[str, int]]:
    """Prove totals and every manifest owner without cardinality-sized SQL."""

    source = _staged_table_source(relation)
    population_sql = _sql_string(relation.get("population_sha256"))
    preview_filter = " AND preview_required" if preview_only else ""
    availability_filter = (
        f"AND availability_version={availability_sql}"
        if availability_sql is not None
        else ""
    )
    result: dict[str, dict[str, int]] = {}
    for entity, table in dataset_tables.items():
        cur.execute(
            f"""
            /* whoscored_frozen_owner_parity:{table} */
            WITH frozen AS (
                SELECT league,season,game_id,preview_required FROM {source}
                WHERE population_sha256={population_sql} AND row_kind='match'
                {preview_filter}
            ), latest AS (
                SELECT * FROM (
                    SELECT m.*,ROW_NUMBER() OVER (
                        PARTITION BY m.league,m.season,m.game_id
                        ORDER BY COALESCE(
                            m.completed_at,m.fetched_at,m._ingested_at
                        ) DESC,COALESCE(m.batch_id,'') DESC
                    ) rn
                    FROM iceberg.bronze.{manifest_table} m
                    JOIN frozen f ON f.league=m.league AND f.season=m.season
                     AND f.game_id=CAST(m.game_id AS BIGINT)
                ) WHERE rn=1 AND state='success' AND parser_version={parser_sql}
                  {availability_filter} AND batch_id LIKE '{batch_prefix}-%'
                  AND raw_uri IS NOT NULL AND payload_sha256 IS NOT NULL
            ), owners AS (
                SELECT league,season,CAST(game_id AS BIGINT) game_id,batch_id,
                       COALESCE(TRY_CAST(json_extract_scalar(
                           TRY(json_parse(entity_counts_json)),'$.{entity}'
                       ) AS BIGINT),0) expected_rows
                FROM latest
            ), physical AS (
                SELECT m.league,m.season,m.game_id,m.batch_id,
                       COUNT(d.game_id) actual_rows
                FROM owners m LEFT JOIN iceberg.bronze.{table} d
                  ON m.league=d.league AND m.season=d.season
                 AND m.game_id=CAST(d.game_id AS BIGINT)
                 AND m.batch_id=d.{batch_column}
                GROUP BY 1,2,3,4
            ), current_rows AS (
                SELECT m.league,m.season,m.game_id,m.batch_id,
                       COUNT(d.game_id) actual_rows
                FROM owners m LEFT JOIN iceberg.bronze.{table}_current d
                  ON m.league=d.league AND m.season=d.season
                 AND m.game_id=CAST(d.game_id AS BIGINT)
                 AND m.batch_id=d.{batch_column}
                GROUP BY 1,2,3,4
            )
            SELECT COALESCE(SUM(m.expected_rows),0),
                   COALESCE(SUM(p.actual_rows),0),
                   COALESCE(SUM(c.actual_rows),0),
                   COUNT_IF(m.expected_rows<>COALESCE(p.actual_rows,0)
                         OR m.expected_rows<>COALESCE(c.actual_rows,0))
            FROM owners m
            LEFT JOIN physical p ON p.league=m.league AND p.season=m.season
             AND p.game_id=m.game_id AND p.batch_id=m.batch_id
            LEFT JOIN current_rows c ON c.league=m.league AND c.season=m.season
             AND c.game_id=m.game_id AND c.batch_id=m.batch_id
            """
        )
        rows = cur.fetchall()
        if len(rows) != 1 or len(rows[0]) != 4:
            raise FrozenDQError(
                "frozen dataset owner parity DQ returned an invalid row"
            )
        result[table] = {
            key: int(value or 0)
            for key, value in zip(
                ("manifest", "physical", "current", "owner_mismatches"),
                rows[0],
            )
        }
    return result


def frozen_historical_integrity(
    cur: Any,
    population: Mapping[str, Any],
    *,
    parser_version: str,
    availability_version: str,
    match_dataset_tables: Mapping[str, str],
    preview_dataset_tables: Mapping[str, str],
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """Return exact semantic counters and 12-table parity for frozen keys."""

    require_production_runtime_class(operation="WhoScored frozen DQ validation")
    relation = population.get("staged_relation")
    if not isinstance(relation, Mapping):
        raise FrozenDQError("frozen DQ requires a staged relation reference")
    parser_sql = _sql_string(parser_version)
    availability_sql = _sql_string(availability_version)
    counters: dict[str, int] = {}
    parity: dict[str, dict[str, int]] = {}

    _validate_staged_relation(cur, relation)
    _merge(
        counters,
        _CORE_NAMES,
        _run_match_core(
            cur,
            relation,
            parser_sql=parser_sql,
            availability_sql=availability_sql,
        ),
    )
    parity.update(
        _dataset_parity_batch(
            cur,
            relation=relation,
            preview_only=False,
            dataset_tables=match_dataset_tables,
            manifest_table="whoscored_match_ingest_manifest",
            batch_column="_game_batch_id",
            batch_prefix="ws2",
            parser_sql=parser_sql,
            availability_sql=availability_sql,
        )
    )
    if int(relation.get("expected_previews") or 0):
        parity.update(
            _dataset_parity_batch(
                cur,
                relation=relation,
                preview_only=True,
                dataset_tables=preview_dataset_tables,
                manifest_table="whoscored_preview_ingest_manifest",
                batch_column="_preview_batch_id",
                batch_prefix="wsp2",
                parser_sql=parser_sql,
                availability_sql=None,
            )
        )

    for table in (*match_dataset_tables.values(), *preview_dataset_tables.values()):
        parity.setdefault(
            table,
            {
                "manifest": 0,
                "physical": 0,
                "current": 0,
                "owner_mismatches": 0,
            },
        )

    profile_names = (
        "frozen_profile_players",
        "uncovered_eligible_profiles",
        "parse_failed_profiles",
        "retryable_profiles",
        "terminal_profiles",
        "unproven_not_available_profiles",
    )
    profile_parity = {
        "whoscored_player_profile_versions": {
            "manifest": 0,
            "physical": 0,
            "current": 0,
            "owner_mismatches": 0,
        },
        "whoscored_player_stage_participations": {
            "manifest": 0,
            "physical": 0,
            "current": 0,
            "owner_mismatches": 0,
        },
    }
    if int(relation.get("expected_players") or 0):
        source = _staged_table_source(relation)
        population_sql = _sql_string(relation.get("population_sha256"))
        cur.execute(
            f"""
            WITH frozen AS (
                SELECT player_id FROM {source}
                WHERE population_sha256={population_sql} AND row_kind='player'
            ),
            latest AS (
                SELECT * FROM (
                    SELECT m.*,ROW_NUMBER() OVER (
                        PARTITION BY m.player_id ORDER BY COALESCE(
                            m.completed_at,m.fetched_at,m._ingested_at
                        ) DESC,COALESCE(m._profile_batch_id,'') DESC
                    ) rn
                    FROM iceberg.bronze.whoscored_profile_ingest_manifest m
                    JOIN frozen f ON f.player_id=CAST(m.player_id AS BIGINT)
                ) WHERE rn=1
            ), valid AS (
                SELECT * FROM latest WHERE parser_version={parser_sql} AND (
                    (state='success' AND _profile_batch_id LIKE 'wspr2-%'
                     AND raw_uri IS NOT NULL AND payload_sha256 IS NOT NULL)
                    OR (state='not_available'
                     AND availability_version={availability_sql}
                     AND failure_code IS NOT NULL
                     AND (raw_uri IS NOT NULL OR http_status IN (404,410)))
                )
            ), latest_success AS (
                SELECT * FROM (
                    SELECT m.*,ROW_NUMBER() OVER (
                        PARTITION BY m.player_id ORDER BY COALESCE(
                            m.completed_at,m.fetched_at,m._ingested_at
                        ) DESC,COALESCE(m._profile_batch_id,'') DESC
                    ) rn
                    FROM iceberg.bronze.whoscored_profile_ingest_manifest m
                    JOIN frozen f ON f.player_id=CAST(m.player_id AS BIGINT)
                    WHERE m.state='success' AND m.parser_version={parser_sql}
                      AND m._profile_batch_id LIKE 'wspr2-%'
                      AND m.raw_uri IS NOT NULL AND m.payload_sha256 IS NOT NULL
                ) WHERE rn=1
            )
            SELECT
                (SELECT COUNT(*) FROM frozen),
                (SELECT COUNT(*) FROM frozen f LEFT JOIN valid p
                 ON p.player_id=f.player_id WHERE p.player_id IS NULL),
                (SELECT COUNT(*) FROM latest WHERE state='parse_failed'),
                (SELECT COUNT(*) FROM latest WHERE state='retryable'),
                (SELECT COUNT(*) FROM latest WHERE state='terminal'),
                (SELECT COUNT(*) FROM latest WHERE state='not_available' AND (
                    parser_version IS DISTINCT FROM {parser_sql}
                    OR availability_version IS DISTINCT FROM {availability_sql}
                    OR failure_code IS NULL OR (raw_uri IS NULL AND (
                        http_status IS NULL OR http_status NOT IN (404,410)
                    ))
                )),
                (SELECT COUNT(*) FROM latest_success),
                (SELECT COUNT(*)
                 FROM iceberg.bronze.whoscored_player_profile_versions p
                 JOIN latest_success m ON m.player_id=p.player_id
                  AND m._profile_batch_id=p._profile_batch_id),
                (SELECT COUNT(*)
                 FROM iceberg.silver.whoscored_player_profile_current p
                 JOIN frozen f ON f.player_id=CAST(p.player_id AS BIGINT)),
                (SELECT COALESCE(SUM(participations_count),0)
                 FROM latest_success),
                (SELECT COUNT(*)
                 FROM iceberg.bronze.whoscored_player_stage_participations p
                 JOIN latest_success m ON m.player_id=p.player_id
                  AND m._profile_batch_id=p._profile_batch_id),
                (SELECT COUNT(*)
                 FROM iceberg.bronze.whoscored_player_stage_participations_current p
                 JOIN frozen f ON f.player_id=CAST(p.player_id AS BIGINT))
            """
        )
        rows = cur.fetchall()
        if len(rows) != 1 or len(rows[0]) != len(profile_names) + 6:
            raise FrozenDQError(
                "frozen profile DQ returned an invalid counter row"
            )
        row = rows[0]
        _merge(counters, profile_names, row[: len(profile_names)])
        parity_row = row[len(profile_names) :]
        for table, values_row in (
            ("whoscored_player_profile_versions", parity_row[:3]),
            ("whoscored_player_stage_participations", parity_row[3:]),
        ):
            for key, value in zip(
                ("manifest", "physical", "current"), values_row
            ):
                profile_parity[table][key] += int(value or 0)
        profile_owner_specs = (
            (
                "whoscored_player_profile_versions",
                "CAST(1 AS BIGINT)",
                "iceberg.bronze.whoscored_player_profile_versions",
                "iceberg.silver.whoscored_player_profile_current",
                False,
            ),
            (
                "whoscored_player_stage_participations",
                "COALESCE(CAST(participations_count AS BIGINT),0)",
                "iceberg.bronze.whoscored_player_stage_participations",
                "iceberg.bronze.whoscored_player_stage_participations_current",
                True,
            ),
        )
        for table, expected_expr, physical_table, current_table, current_has_batch in (
            profile_owner_specs
        ):
            current_batch_join = (
                "AND m._profile_batch_id=c._profile_batch_id"
                if current_has_batch
                else ""
            )
            cur.execute(
                f"""
                /* whoscored_frozen_profile_owner_parity:{table} */
                WITH frozen AS (
                    SELECT player_id FROM {source}
                    WHERE population_sha256={population_sql} AND row_kind='player'
                ), latest_success AS (
                    SELECT * FROM (
                        SELECT m.*,ROW_NUMBER() OVER (
                            PARTITION BY m.player_id ORDER BY COALESCE(
                                m.completed_at,m.fetched_at,m._ingested_at
                            ) DESC,COALESCE(m._profile_batch_id,'') DESC
                        ) rn
                        FROM iceberg.bronze.whoscored_profile_ingest_manifest m
                        JOIN frozen f ON f.player_id=CAST(m.player_id AS BIGINT)
                        WHERE m.state='success' AND m.parser_version={parser_sql}
                          AND m._profile_batch_id LIKE 'wspr2-%'
                          AND m.raw_uri IS NOT NULL
                          AND m.payload_sha256 IS NOT NULL
                    ) WHERE rn=1
                ), owners AS (
                    SELECT CAST(player_id AS BIGINT) player_id,_profile_batch_id,
                           {expected_expr} expected_rows FROM latest_success
                ), physical AS (
                    SELECT m.player_id,m._profile_batch_id,COUNT(p.player_id) rows_count
                    FROM owners m LEFT JOIN {physical_table} p
                      ON m.player_id=CAST(p.player_id AS BIGINT)
                     AND m._profile_batch_id=p._profile_batch_id GROUP BY 1,2
                ), current_rows AS (
                    SELECT m.player_id,m._profile_batch_id,COUNT(c.player_id) rows_count
                    FROM owners m LEFT JOIN {current_table} c
                      ON m.player_id=CAST(c.player_id AS BIGINT)
                     {current_batch_join} GROUP BY 1,2
                )
                SELECT COUNT_IF(m.expected_rows<>COALESCE(p.rows_count,0)
                             OR m.expected_rows<>COALESCE(c.rows_count,0))
                FROM owners m
                LEFT JOIN physical p ON p.player_id=m.player_id
                 AND p._profile_batch_id=m._profile_batch_id
                LEFT JOIN current_rows c ON c.player_id=m.player_id
                 AND c._profile_batch_id=m._profile_batch_id
                """
            )
            values = cur.fetchall()
            if len(values) != 1 or len(values[0]) != 1:
                raise FrozenDQError(
                    "frozen profile owner parity returned an invalid row"
                )
            profile_parity[table]["owner_mismatches"] = int(
                values[0][0] or 0
            )
    parity.update(profile_parity)

    for name in (*_CORE_NAMES, *profile_names):
        counters.setdefault(name, 0)
    counters["failed_previews"] = sum(
        counters[name]
        for name in (
            "parse_failed_previews",
            "retryable_previews",
            "terminal_previews",
        )
    )
    counters["failed_profiles"] = sum(
        counters[name]
        for name in (
            "parse_failed_profiles",
            "retryable_profiles",
            "terminal_profiles",
        )
    )
    return counters, parity


def cleanup_staged_frozen_populations(
    cur: Any,
    *,
    keep_population_sha256: str | None = None,
    retention_days: int = DQ_STAGE_RETENTION_DAYS,
) -> int:
    """Delete at most 100 expired identity partitions.

    A successful backfill keeps its just-validated population while pruning
    older partitions.  Scheduled maintenance passes no population so expiry
    still advances when no backfill reaches its success-only cleanup path.
    """

    require_production_runtime_class(operation="WhoScored frozen DQ cleanup")
    if (
        (keep_population_sha256 is not None and not _is_sha256(keep_population_sha256))
        or type(retention_days) is not int
        or not 1 <= retention_days <= 365
    ):
        raise FrozenDQError("frozen DQ retention policy is invalid")
    keep_filter = ""
    if keep_population_sha256 is not None:
        keep_filter = (
            "WHERE population_sha256<>"
            + _sql_string(keep_population_sha256)
        )
    cur.execute(
        f"""
        SELECT population_sha256
        FROM {DQ_STAGE_QUALIFIED}
        {keep_filter}
        GROUP BY population_sha256
        HAVING MAX(staged_at) < CAST(
            CURRENT_TIMESTAMP - INTERVAL '{retention_days}' DAY AS TIMESTAMP
        )
        ORDER BY MAX(staged_at), population_sha256
        LIMIT {DQ_STAGE_CLEANUP_LIMIT}
        """
    )
    values = [str(row[0]) for row in cur.fetchall()]
    if not values:
        return 0
    if any(not _is_sha256(value) for value in values):
        raise FrozenDQError("frozen DQ cleanup selected an invalid partition")
    cur.execute(
        f"DELETE FROM {DQ_STAGE_QUALIFIED} WHERE population_sha256 IN ("
        + ",".join(_sql_string(value) for value in values)
        + ")"
    )
    return len(values)
