"""Pure selection of SofaScore raw-replay scope plans (#1367).

Pure selection turns already-captured endpoint manifest records into
deterministic replay plans for committed raw blobs.  The adapter functions at
the bottom of this module additionally read scope keys and records from the
fixed ``iceberg.ops.sofascore_capture_manifest`` table and resolve a scope
through the tournament catalog.  Worker/DAG wiring deliberately stays out.

A record is *replayable* when its raw payload is readable: ``http_status`` is
a 2xx other than 204 and both ``raw_content_hash`` and ``raw_blob_key`` are
non-empty.  Everything else is excluded from replay work.

Two modes exist:

``fields``
    Re-parse paid field endpoints (``lineups``, ``standings_total``,
    ``rounds``, ``cup_trees``, ``participants``, ``referee_profile``) with any
    manifest status, so an old ``success`` capture is replayed after a parser
    upgrade.

``drain``
    Re-parse deferred work: any endpoint whose last observation ended as
    ``retryable_failure`` while the raw payload itself is replayable
    (generic HTTP 200 deferred records).

Selection is deterministic: scopes are ordered by
``(source_tournament_id, source_season_id, freshness_key)``, targets inside a
scope by ``(target_type, target_id)`` and records inside a target by
endpoint.  Caps drop whole scopes or whole targets, never a partial endpoint
set of one target.
"""

from __future__ import annotations

from typing import (
    Callable,
    Iterable,
    Mapping,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
)

from .manifest import EndpointManifest, ManifestStatus
from .partition_keys import canonical_league

__all__ = [
    "FIELDS_BLOCKING_ENDPOINTS",
    "FIELDS_ENDPOINTS",
    "MODES",
    "MODE_DRAIN",
    "MODE_FIELDS",
    "REPLAY_MANIFEST_TABLE",
    "ReplayCatalogProtocol",
    "ReplayManagerProtocol",
    "ReplayPlanningError",
    "ReplayScopeKey",
    "ReplayScopePlan",
    "ReplaySelection",
    "ResolvedReplayScope",
    "list_replay_scope_keys",
    "load_replay_scope_records",
    "replay_eligible_scope_pairs",
    "resolve_replay_scope",
    "select_replay_scope_plans",
    "validate_replay_scope_pair",
]

MODE_FIELDS = "fields"
MODE_DRAIN = "drain"
MODES = (MODE_FIELDS, MODE_DRAIN)

FIELDS_ENDPOINTS = frozenset(
    {
        "lineups",
        "standings_total",
        "rounds",
        "cup_trees",
        "participants",
        "referee_profile",
    }
)

# Fields-mode endpoints whose stored ``schema_error`` blocks the scope's
# season materialization (#1367): the deployed parser deterministically
# re-rejects the same raw bytes, so ``materialize_season_partition`` can
# never complete for that (tournament, season, freshness) triple.
# ``lineups`` never enters the season plan and ``referee_profile`` has an
# explicit rejected-profile carve-out, so a stored schema_error on either
# does NOT block the scope (Astra design adjudication 2026-11-22).
FIELDS_BLOCKING_ENDPOINTS = (
    "cup_trees",
    "participants",
    "rounds",
    "standings_total",
)

# Registry season formats that can name a Bronze ``season`` partition label
# through the replay overlay; ``unknown`` cannot (review r1 F1).
_REPLAYABLE_SEASON_FORMATS = ("single_year", "split_year")

SKIP_NON_2XX = "non_2xx"
SKIP_MISSING_LINEAGE = "missing_lineage"
SKIP_OTHER_STATUS = "other_status"
SKIP_REASONS = (SKIP_NON_2XX, SKIP_MISSING_LINEAGE, SKIP_OTHER_STATUS)


class ReplayPlanningError(ValueError):
    """Invalid replay selection arguments (mode, caps or exact_scope)."""


ReplayScopeKey = NamedTuple(
    "ReplayScopeKey",
    source_tournament_id=str,
    source_season_id=str,
    freshness_key=str,
)

ReplayScopePlan = NamedTuple(
    "ReplayScopePlan",
    scope=ReplayScopeKey,
    records=tuple[EndpointManifest, ...],
    mode=str,
)


class ReplaySelection(tuple):
    """Tuple of :class:`ReplayScopePlan` plus pre-cap skip diagnostics.

    ``skipped`` counts records excluded by the replayability/status filters
    before caps are applied.  Records dropped by ``scope_cap``/``target_cap``
    or narrowed away by ``exact_scope`` and the mode's endpoint universe are
    intentionally not counted.
    """

    def __new__(cls, plans, skipped) -> "ReplaySelection":
        selection = super().__new__(cls, plans)
        counts = {reason: 0 for reason in SKIP_REASONS}
        for reason, count in dict(skipped).items():
            if reason in counts:
                counts[reason] = int(count)
        selection._skipped = counts
        return selection

    @property
    def skipped(self) -> dict[str, int]:
        return dict(self._skipped)


def _validate_positive_int(value: object, name: str) -> int:
    if not isinstance(value, int) or value <= 0:
        raise ReplayPlanningError(
            f"{name} must be a positive integer, got {value!r}"
        )
    return value


def _normalize_exact_scope(exact_scope: object) -> Optional[tuple[str, str]]:
    if exact_scope is None:
        return None
    if isinstance(exact_scope, (str, bytes)) or not isinstance(
        exact_scope, Sequence
    ):
        raise ReplayPlanningError(
            "exact_scope must be a (source_tournament_id, source_season_id) pair"
        )
    pair = tuple(exact_scope)
    if len(pair) != 2:
        raise ReplayPlanningError(
            "exact_scope must contain exactly "
            "(source_tournament_id, source_season_id), "
            f"got {len(pair)} values"
        )
    normalized = []
    for value in pair:
        if not isinstance(value, (str, int)):
            raise ReplayPlanningError(
                f"exact_scope ids must be str or int, got {value!r}"
            )
        text = str(value).strip()
        if not text:
            raise ReplayPlanningError("exact_scope ids must not be empty")
        normalized.append(text)
    return (normalized[0], normalized[1])


def _is_replayable_http(record: EndpointManifest) -> bool:
    status = record.http_status
    return status is not None and 200 <= status < 300 and status != 204


def _has_raw_lineage(record: EndpointManifest) -> bool:
    return bool((record.raw_content_hash or "").strip()) and bool(
        (record.raw_blob_key or "").strip()
    )


def _skip_reason(record: EndpointManifest, mode: str) -> Optional[str]:
    if not _is_replayable_http(record):
        return SKIP_NON_2XX
    if not _has_raw_lineage(record):
        return SKIP_MISSING_LINEAGE
    if mode == MODE_DRAIN and record.status != ManifestStatus.RETRYABLE_FAILURE:
        return SKIP_OTHER_STATUS
    return None


def _record_sort_key(record: EndpointManifest) -> tuple[str, str, str, str]:
    return (record.key.endpoint, record.run_id, record.task_id, record.updated_at)


def select_replay_scope_plans(
    records: Iterable[EndpointManifest],
    *,
    mode: str,
    scope_cap: int = 1,
    target_cap: int = 25,
    exact_scope: Optional[Sequence[object]] = None,
) -> tuple[ReplayScopePlan, ...]:
    """Select deterministic raw-replay plans from manifest records.

    mode ``fields`` replays paid field endpoints with any status (including
    old ``success``); mode ``drain`` replays any endpoint that ended as
    ``retryable_failure`` with a replayable 2xx raw payload.  ``exact_scope``
    filters records to one ``(source_tournament_id, source_season_id)``
    before grouping.  ``scope_cap`` limits the number of scopes and
    ``target_cap`` the number of ``(target_type, target_id)`` groups per
    scope; targets are always selected whole, so a cap never splits a
    target's endpoint set.

    The returned tuple subclass exposes a ``skipped`` property counting the
    records excluded before caps, by reason (``non_2xx``,
    ``missing_lineage``, ``other_status``).

    Raises :class:`ReplayPlanningError` for an unknown mode, non-positive or
    non-integer caps, or a malformed ``exact_scope``.
    """
    if mode not in MODES:
        raise ReplayPlanningError(f"mode must be one of {MODES}, got {mode!r}")
    scope_cap = _validate_positive_int(scope_cap, "scope_cap")
    target_cap = _validate_positive_int(target_cap, "target_cap")
    wanted_scope = _normalize_exact_scope(exact_scope)

    skipped = {reason: 0 for reason in SKIP_REASONS}
    by_scope: dict[tuple[str, str, str], list[EndpointManifest]] = {}

    for record in records:
        key = record.key
        if wanted_scope is not None and (
            key.source_tournament_id,
            key.source_season_id,
        ) != wanted_scope:
            continue
        if mode == MODE_FIELDS and key.endpoint not in FIELDS_ENDPOINTS:
            continue
        reason = _skip_reason(record, mode)
        if reason is not None:
            skipped[reason] += 1
            continue
        by_scope.setdefault(
            (key.source_tournament_id, key.source_season_id, key.freshness_key),
            [],
        ).append(record)

    plans: list[ReplayScopePlan] = []
    for scope_values in sorted(by_scope)[:scope_cap]:
        by_target: dict[tuple[str, str], list[EndpointManifest]] = {}
        for record in by_scope[scope_values]:
            by_target.setdefault(
                (record.key.target_type, record.key.target_id), []
            ).append(record)
        selected: list[EndpointManifest] = []
        for target in sorted(by_target)[:target_cap]:
            selected.extend(sorted(by_target[target], key=_record_sort_key))
        plans.append(
            ReplayScopePlan(
                scope=ReplayScopeKey(*scope_values),
                records=tuple(selected),
                mode=mode,
            )
        )
    return ReplaySelection(plans, skipped)


REPLAY_MANIFEST_TABLE = "iceberg.ops.sofascore_capture_manifest"

_HTTP_REPLAYABLE_SQL = (
    '"http_status" >= 200',
    '"http_status" < 300',
    '"http_status" <> 204',
)
_RAW_LINEAGE_SQL = (
    "COALESCE(TRIM(\"raw_content_hash\"), '') <> ''",
    "COALESCE(TRIM(\"raw_blob_key\"), '') <> ''",
)
_SCOPE_ORDER_SQL = (
    'ORDER BY m."source_tournament_id", m."source_season_id", '
    'm."freshness_key"'
)
_RECORD_ORDER_SQL = (
    'ORDER BY "target_type", "target_id", "endpoint", '
    '"run_id", "task_id", "updated_at"'
)
_FIELDS_ENDPOINTS_SQL = '"endpoint" IN ({})'.format(
    ", ".join(f"'{endpoint}'" for endpoint in sorted(FIELDS_ENDPOINTS))
)
_DRAIN_STATUS_SQL = "\"status\" = 'retryable_failure'"

# Suppression subquery for fields mode (#1367): a scope whose stored
# ``schema_error`` on a blocking season endpoint cannot complete
# materialization — the deployed parser deterministically re-rejects the
# same committed raw.  Correlated on the full scope triple so one blocked
# freshness key never suppresses its sibling captures of the same
# (tournament, season).
_FIELDS_BLOCKED_SQL = (
    "NOT EXISTS (SELECT 1 FROM {table} b WHERE {correlation} "
    "b.\"endpoint\" IN ({endpoints}) "
    "AND b.\"status\" = 'schema_error' AND {replayable})"
).format(
    table=REPLAY_MANIFEST_TABLE,
    correlation=" ".join(
        f'b."{column}" = m."{column}" AND'
        for column in (
            "source_tournament_id",
            "source_season_id",
            "freshness_key",
        )
    ),
    endpoints=", ".join(
        f"'{endpoint}'" for endpoint in FIELDS_BLOCKING_ENDPOINTS
    ),
    replayable=" AND ".join(
        (
            'b."http_status" >= 200',
            'b."http_status" < 300',
            'b."http_status" <> 204',
            "COALESCE(TRIM(b.\"raw_content_hash\"), '') <> ''",
            "COALESCE(TRIM(b.\"raw_blob_key\"), '') <> ''",
        )
    ),
)


class ReplayManagerProtocol(Protocol):
    def _execute(
        self,
        sql: str,
        fetch: bool = False,
        params: Optional[tuple] = None,
    ): ...


class ReplayCatalogProtocol(Protocol):
    def tournament(self, unique_tournament_id: int) -> object: ...

    def tournament_map(self) -> Mapping[str, int]: ...


ResolvedReplayScope = NamedTuple(
    "ResolvedReplayScope",
    source_tournament_id=str,
    source_season_id=str,
    freshness_key=str,
    league=str,
    canonical_season=str,
)
# The five-field shape is the module's public contract (review r1/r2 F4):
# registry metadata the phases need beyond it — the season's format — is
# carried alongside the scope by the worker, never inside it.


def _mode_filter_sql(mode: str) -> str:
    if mode == MODE_FIELDS:
        return _FIELDS_ENDPOINTS_SQL
    return _DRAIN_STATUS_SQL


def _normalize_scope_key(scope: object) -> ReplayScopeKey:
    if isinstance(scope, ReplayScopeKey):
        values: Sequence[object] = tuple(scope)
    elif isinstance(scope, (str, bytes)) or not isinstance(scope, Sequence):
        raise ReplayPlanningError(
            "scope must be a (source_tournament_id, source_season_id, "
            "freshness_key) triple"
        )
    else:
        values = tuple(scope)
    if len(values) != 3:
        raise ReplayPlanningError(
            "scope must contain exactly (source_tournament_id, "
            f"source_season_id, freshness_key), got {len(values)} values"
        )
    normalized = []
    for value in values:
        if not isinstance(value, (str, int)):
            raise ReplayPlanningError(
                f"scope ids must be str or int, got {value!r}"
            )
        text = str(value).strip()
        if not text:
            raise ReplayPlanningError("scope ids must not be empty")
        normalized.append(text)
    return ReplayScopeKey(*normalized)


def _manifest_row_codec() -> tuple[
    tuple[str, ...], Callable[[Sequence[object]], EndpointManifest]
]:
    from .adapters import MANIFEST_COLUMNS, manifest_from_row

    return MANIFEST_COLUMNS, manifest_from_row


def _scope_int_id(value: str, name: str) -> int:
    try:
        return int(value.strip())
    except ValueError as exc:
        raise ReplayPlanningError(
            f"{name} must be a numeric source identifier, got {value!r}"
        ) from exc


def _season_slug(canonical_season: object) -> Optional[int]:
    """The canonical season as a partition slug, or None.

    Conversion — not ``str.isdigit()`` — decides (review r2 F5): digits
    like superscript two pass ``isdigit()`` but cannot become the overlay's
    integer season id or a partition label.
    """
    if not isinstance(canonical_season, str):
        return None
    try:
        return int(canonical_season)
    except ValueError:
        return None


def _replayable_registry_season(season: object) -> bool:
    """True when a registry season can resolve AND enter a scope overlay.

    One predicate shared by resolution, pair enumeration and exact-scope
    validation (review r1 F1): a replayable season needs a canonical season
    convertible to a numeric slug (the overlay medallion entry and the
    Bronze ``season`` partition are numeric), and a season format that
    names a partition label (``single_year``/``split_year``).
    """
    if _season_slug(getattr(season, "canonical_season", None)) is None:
        return False
    return getattr(season, "season_format", None) in _REPLAYABLE_SEASON_FORMATS


def replay_eligible_scope_pairs(
    catalog: ReplayCatalogProtocol,
) -> tuple[tuple[str, str], ...]:
    """(source_tournament_id, source_season_id) pairs the registry resolves.

    Enumeration mirrors :func:`resolve_replay_scope` exactly: only
    capture-allowed tournaments with a canonical id, and only seasons whose
    canonical season and replayable season format are present
    (:func:`_replayable_registry_season`).  A ``season_id`` named by more
    than one season of its tournament is refused by the resolver as
    ambiguous, so only THOSE ids are skipped — uniquely named sibling
    seasons of the same tournament stay eligible (review r2 F1).
    The pair set is the SQL-side resolvability filter for automatic
    planning — scopes outside it (unknown tournaments such as EURO with no
    registry seasons, disabled tournaments) can never resolve in a worker.
    """
    tournaments = getattr(catalog, "tournaments", None)
    if tournaments is None:
        raise ReplayPlanningError(
            "catalog does not expose a tournaments sequence for replay "
            "scope enumeration"
        )
    pairs: set[tuple[str, str]] = set()
    for tournament in tournaments:
        if not getattr(tournament, "capture_allowed", False):
            continue
        canonical_id = getattr(tournament, "canonical_id", None)
        if not isinstance(canonical_id, str) or not canonical_id.strip():
            continue
        source_id = getattr(tournament, "unique_tournament_id", None)
        if isinstance(source_id, bool) or not isinstance(source_id, int):
            continue
        seasons = tuple(getattr(tournament, "seasons", ()) or ())
        id_counts: dict[object, int] = {}
        for season in seasons:
            season_id = getattr(season, "season_id", None)
            id_counts[season_id] = id_counts.get(season_id, 0) + 1
        for season in seasons:
            season_id = getattr(season, "season_id", None)
            if isinstance(season_id, bool) or not isinstance(season_id, int):
                continue
            if id_counts[season_id] > 1:
                continue
            if not _replayable_registry_season(season):
                continue
            pairs.add((str(source_id), str(season_id)))
    return tuple(sorted(pairs))


def validate_replay_scope_pair(
    exact_scope: Sequence[object], catalog: ReplayCatalogProtocol
) -> tuple[str, str]:
    """Validate an operator-supplied exact scope pair loudly.

    Raises :class:`ReplayPlanningError` naming the exact reason (unknown
    tournament, unknown season, missing canonical metadata, ineligible
    season format, or registry-disabled tournament) so an exact replay
    request fails in the planner instead of dying in a worker with a
    generic resolution error.
    """
    wanted = _normalize_exact_scope(exact_scope)
    tournament_id = _scope_int_id(wanted[0], "source_tournament_id")
    season_id = _scope_int_id(wanted[1], "source_season_id")
    prefix = f"exact replay scope ({wanted[0]!r}, {wanted[1]!r})"
    try:
        tournament = catalog.tournament(tournament_id)
    except (ValueError, KeyError, TypeError) as exc:
        raise ReplayPlanningError(
            f"{prefix} is not replay-eligible: unknown tournament: {exc}"
        ) from exc
    matches = [
        season for season in tournament.seasons if season.season_id == season_id
    ]
    if not matches:
        raise ReplayPlanningError(
            f"{prefix} is not replay-eligible: season {wanted[1]!r} is "
            "unknown for the source tournament"
        )
    if len(matches) > 1:
        raise ReplayPlanningError(
            f"{prefix} is not replay-eligible: season {wanted[1]!r} is "
            "ambiguous for the source tournament"
        )
    if not _replayable_registry_season(matches[0]):
        canonical_season = getattr(matches[0], "canonical_season", None)
        reason = (
            "season has no numeric canonical season"
            if _season_slug(canonical_season) is None
            else "season has no replayable season format"
        )
        raise ReplayPlanningError(f"{prefix} is not replay-eligible: {reason}")
    canonical_id = getattr(tournament, "canonical_id", None)
    if not isinstance(canonical_id, str) or not canonical_id.strip():
        raise ReplayPlanningError(
            f"{prefix} is not replay-eligible: tournament has no canonical id"
        )
    if not getattr(tournament, "capture_allowed", False):
        raise ReplayPlanningError(
            f"{prefix} is not replay-eligible: tournament is not "
            "capture-allowed by the registry"
        )
    return wanted


def list_replay_scope_keys(
    manager: ReplayManagerProtocol,
    *,
    mode: str,
    exact_scope: Optional[Sequence[object]] = None,
    limit: int = 1,
    catalog: Optional[ReplayCatalogProtocol] = None,
    include_blocked: bool = False,
    blocked_only: bool = False,
) -> tuple[ReplayScopeKey, ...]:
    """List replayable scope keys from the fixed manifest table.

    ``fields`` selects scopes with at least one paid field endpoint whose raw
    payload is replayable (2xx other than 204 with committed raw lineage,
    any manifest status); ``drain`` selects scopes with any endpoint whose
    last observation is ``retryable_failure`` under the same raw conditions.
    Keys come back ordered by ``(source_tournament_id, source_season_id,
    freshness_key)`` capped by the SQL ``LIMIT``.  ``exact_scope`` values are
    bound as SQL parameters; the table identifier is the fixed module
    constant and never caller input.

    ``catalog`` filters to scopes the registry can resolve.  Automatic
    planning passes it to admit only capture-allowed
    ``(source_tournament_id, source_season_id)`` pairs (bound as two ``?``
    parameters per pair, decimal strings); an empty pair set short-circuits
    to no candidates without touching the manifest.  An ``exact_scope``
    under a catalog is validated eagerly and loudly instead of receiving the
    pair filter.  In ``fields`` mode a scope triple with a stored
    ``schema_error`` on a blocking season endpoint is suppressed unless
    ``include_blocked`` opts in (the retry path after a parser fix); the
    deployed parser deterministically re-rejects that committed raw, so its
    season materialization can never complete (#1367).  ``blocked_only``
    (fields mode, review r1 F3) inverts the same predicate to list exactly
    the blocked triples — the planner's skip diagnostic — instead of every
    candidate a second unrestricted query could sweep in.
    """
    if mode not in MODES:
        raise ReplayPlanningError(f"mode must be one of {MODES}, got {mode!r}")
    limit = _validate_positive_int(limit, "limit")
    if blocked_only and include_blocked:
        raise ReplayPlanningError(
            "blocked_only and include_blocked are mutually exclusive"
        )
    if blocked_only and mode != MODE_FIELDS:
        raise ReplayPlanningError(
            "blocked_only is only meaningful for the fields mode"
        )
    wanted_scope = _normalize_exact_scope(exact_scope)
    pair_filter = False
    if catalog is not None:
        if wanted_scope is None:
            pair_filter = True
        else:
            wanted_scope = validate_replay_scope_pair(wanted_scope, catalog)
    clauses = [
        *_HTTP_REPLAYABLE_SQL,
        *_RAW_LINEAGE_SQL,
        _mode_filter_sql(mode),
    ]
    params: list[str] = []
    if wanted_scope is not None:
        clauses.extend(
            [
                'm."source_tournament_id" = ?',
                'm."source_season_id" = ?',
            ]
        )
        params.extend(wanted_scope)
    elif pair_filter:
        pairs = replay_eligible_scope_pairs(catalog)
        if not pairs:
            return ()
        clauses.append(
            '(m."source_tournament_id", m."source_season_id") IN ({})'.format(
                ", ".join("(?, ?)" for _ in pairs)
            )
        )
        params.extend(
            value for pair in pairs for value in pair
        )
    if mode == MODE_FIELDS and (blocked_only or not include_blocked):
        predicate = _FIELDS_BLOCKED_SQL
        if blocked_only:
            predicate = predicate.replace("NOT EXISTS", "EXISTS", 1)
        clauses.append(predicate)
    sql = (
        'SELECT DISTINCT m."source_tournament_id", m."source_season_id", '
        'm."freshness_key" '
        f"FROM {REPLAY_MANIFEST_TABLE} m "
        f"WHERE {' AND '.join(clauses)} "
        f"{_SCOPE_ORDER_SQL} "
        f"LIMIT {limit}"
    )
    rows = manager._execute(sql, fetch=True, params=tuple(params))
    return tuple(
        ReplayScopeKey(*(str(value) for value in row)) for row in (rows or [])
    )


def load_replay_scope_records(
    manager: ReplayManagerProtocol,
    scope: Sequence[object],
    *,
    mode: str,
) -> tuple[EndpointManifest, ...]:
    """Load one exact scope key's replayable manifest records.

    Filters by the exact ``(source_tournament_id, source_season_id,
    freshness_key)`` triple plus the same replayability/status conditions per
    ``mode`` as :func:`list_replay_scope_keys`, orders rows deterministically
    by target and endpoint, and converts each row through
    ``manifest_from_row``.  The adapter never trims a scope's target set:
    capping stays the responsibility of ``select_replay_scope_plans``.
    """
    if mode not in MODES:
        raise ReplayPlanningError(f"mode must be one of {MODES}, got {mode!r}")
    scope_key = _normalize_scope_key(scope)
    columns, manifest_from_row = _manifest_row_codec()
    select_columns = ", ".join(f'"{column}"' for column in columns)
    clauses = [
        '"source_tournament_id" = ?',
        '"source_season_id" = ?',
        '"freshness_key" = ?',
        *_HTTP_REPLAYABLE_SQL,
        *_RAW_LINEAGE_SQL,
        _mode_filter_sql(mode),
    ]
    sql = (
        f"SELECT {select_columns} "
        f"FROM {REPLAY_MANIFEST_TABLE} "
        f"WHERE {' AND '.join(clauses)} "
        f"{_RECORD_ORDER_SQL}"
    )
    rows = manager._execute(sql, fetch=True, params=tuple(scope_key))
    return tuple(manifest_from_row(row) for row in (rows or []))


def resolve_replay_scope(
    key: Sequence[object], catalog: ReplayCatalogProtocol
) -> ResolvedReplayScope:
    """Resolve a scope key to canonical league/season via the catalog.

    The tournament is looked up by ``int(source_tournament_id)`` and the
    season by exact ``season_id`` membership in ``tournament.seasons`` —
    never by date and never by the first alias.  Unknown tournaments, unknown
    or ambiguous seasons, missing ``canonical_id``/``canonical_season``,
    a season format that cannot name a Bronze season label
    (``single_year``/``split_year``), a canonical season the overlay cannot
    convert to a partition slug and unknown legacy league keys all raise
    :class:`ReplayPlanningError`.
    """
    scope = _normalize_scope_key(key)
    tournament_id = _scope_int_id(
        scope.source_tournament_id, "source_tournament_id"
    )
    season_id = _scope_int_id(scope.source_season_id, "source_season_id")
    try:
        tournament = catalog.tournament(tournament_id)
    except (ValueError, KeyError, TypeError) as exc:
        raise ReplayPlanningError(
            f"unknown source tournament {scope.source_tournament_id!r}: {exc}"
        ) from exc
    matches = [
        season for season in tournament.seasons if season.season_id == season_id
    ]
    if not matches:
        raise ReplayPlanningError(
            f"season {scope.source_season_id!r} is unknown for source "
            f"tournament {scope.source_tournament_id!r}"
        )
    if len(matches) > 1:
        raise ReplayPlanningError(
            f"season {scope.source_season_id!r} is ambiguous for source "
            f"tournament {scope.source_tournament_id!r}"
        )
    canonical_season = getattr(matches[0], "canonical_season", None)
    if not isinstance(canonical_season, str) or not canonical_season.strip():
        raise ReplayPlanningError(
            f"season {scope.source_season_id!r} has no canonical season"
        )
    season_format = getattr(matches[0], "season_format", None)
    if season_format not in _REPLAYABLE_SEASON_FORMATS:
        raise ReplayPlanningError(
            f"season {scope.source_season_id!r} of source tournament "
            f"{scope.source_tournament_id!r} has no replayable season format "
            f"(got {season_format!r})"
        )
    if _season_slug(canonical_season) is None:
        raise ReplayPlanningError(
            f"season {scope.source_season_id!r} of source tournament "
            f"{scope.source_tournament_id!r} has a non-numeric canonical "
            f"season {canonical_season!r}; the replay overlay and the Bronze "
            "season partition need a numeric slug"
        )
    canonical_id = getattr(tournament, "canonical_id", None)
    if not isinstance(canonical_id, str) or not canonical_id.strip():
        raise ReplayPlanningError(
            f"source tournament {scope.source_tournament_id!r} has no "
            "canonical id"
        )
    try:
        league = canonical_league(
            canonical_id.strip(), catalog.tournament_map()
        )
    except ValueError as exc:
        raise ReplayPlanningError(
            f"cannot map source tournament {scope.source_tournament_id!r} "
            f"to a canonical league: {exc}"
        ) from exc
    return ResolvedReplayScope(
        source_tournament_id=scope.source_tournament_id,
        source_season_id=scope.source_season_id,
        freshness_key=scope.freshness_key,
        league=league,
        canonical_season=canonical_season.strip(),
    )
