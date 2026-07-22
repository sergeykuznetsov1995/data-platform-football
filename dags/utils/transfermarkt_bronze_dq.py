"""Cross-table Bronze DQ for the native Transfermarkt pipeline (issue #948).

Pure ``build_*_sql`` functions plus a cursor-driven runner.  The module has
no Airflow or Trino imports: every builder returns one Trino SQL string and
:func:`run_bronze_dq` only needs a DB-API cursor, so the whole surface is
unit-testable without a live warehouse.  Table specs are re-declared here
(the DAG module cannot be imported from ``utils``); unit tests assert their
equality with ``dag_transform_transfermarkt_silver._NATIVE_BRONZE_SCOPE_COLUMNS``
and ``utils.transfermarkt_native_v2.TABLE_CONTRACTS``.

Zones
-----
- ``full``      — the whole-Bronze sweep (weekly/ad-hoc); scope-set presence
                  runs only when ``manifests`` are supplied; pins are
                  optional (an unpinned run reflects the current live state).
- ``scope_set`` — the production transform gate; ``manifests`` are required
                  and every native Bronze relation plus the promoted-registry
                  Silver relations must be time-travel pinned.  Heavy checks
                  (intra-batch conflicts/duplicates, membership orphans,
                  cross-batch duplicates, partial scope) are restricted to
                  the exact scope set via chunked
                  ``(competition_id, edition_id) IN (...)`` predicates, so
                  the per-transform cost stays bounded at full target scale
                  (heap-heavy GROUP BY/anti-join sides shrink to the scope
                  set); the NULL-cohort counters and the legacy checks are
                  skipped here — the periodic ``full`` sweep covers them.
                  Light global checks stay global: promoted-snapshot
                  presence, phantom scope (anti-join with a small registry
                  build side), manifest presence, coverage and career debt.
- ``legacy``    — only the legacy ``(league, season)`` checks; used by the
                  explicit ``manual_single_scope`` compatibility path which
                  writes no native tables.  This is the ROLLBACK path: it
                  must keep working while the native contour (including the
                  promoted registry) is broken.  An unavailable or non-green
                  registry therefore degrades the phantom check to the
                  curated YAML allowlist at WARNING severity and adds an
                  explicit ``tm_registry_unavailable_degraded`` WARNING —
                  integrity in degraded mode is observability, not a gate.

Pinned vs live reads
--------------------
Time-travel pins apply to the native and legacy Bronze relations and the
promoted-registry Silver relations.  The ops relations
(``REGISTRY_STATE_TABLE``, ``SCOPE_MANIFEST_TABLE``) are deliberately read
live: the state row is a verifiable pointer (re-checked here for
``promoted`` + ``unknown_active_count = 0``) and scope manifests are
digest-verified upstream by the DAG preflight before this gate runs.

Severity contract
-----------------
ERROR results gate the transform (the caller raises); WARNING results are
observability-only and must never gate.

- Intra-batch duplicates are split by payload: a duplicate group whose
  semantic payload projection CONFLICTS inside one batch (two different
  truths from one crawl) is ERROR (``tm_*_intra_batch_conflicts``);
  identical repeats of the same row are WARNING-only dirt
  (``tm_*_intra_batch_duplicates``) — the deterministic Silver dedup
  collapses them without information loss.
- ``tm_bronze_cross_batch_duplicates[*]`` counts identical contract-PK rows
  across ``_batch_id`` values: after the carry-forward fix (issue #948,
  phase F5, ``observed_at`` is carried forward on idempotent re-crawls)
  identical rows appended by repeated runs are NORMAL, so the metric stays
  observational forever — the meaningful uniqueness grain is the post-dedup
  Silver model, which has its own blocking gates.  For relations whose
  contract PK embeds the crawl scope (memberships and both observation
  tables) the metric only reads scoped rows: pre-native NULL-scope cohort
  rows have no contract identity (their season lives outside the PK) and
  are already counted by ``tm_bronze_legacy_cohort[*]``.
- ``tm_bronze_membership_orphans[coach_stints]`` is WARNING (not ERROR):
  coach stints are scraped from a club's full coach-history page while
  coach profiles are only fetched for current/recent coaches, so a stint
  referencing a historical coach without a profile row is a consequence of
  the bounded crawl policy, not corruption.  Referential strictness stays
  ERROR for attribute/contract observations and market-value/transfer
  facts against same-scope memberships.
- ``tm_scope_set_bronze_presence`` vs append-only Bronze: once a scope has
  rows, a later manifest transition ``ok -> authoritative_empty`` makes the
  presence check permanently red — Bronze never deletes, so clearing it is
  an EXPECTED manual scenario (operator DELETE of the stale scoped rows
  after verifying the terminal-empty evidence).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, List, Mapping, Sequence

from utils.transfermarkt_scope_state import (
    REGISTRY_STATE_TABLE,
    SCOPE_COMPLETION_STATUS,
    SCOPE_MANIFEST_TABLE,
)

# Non-slotted promoted-registry relations (values mirror
# utils.transfermarkt_scope_planner.COMPETITIONS_TABLE / EDITIONS_TABLE;
# equality is asserted by unit tests).
COMPETITIONS_REGISTRY_TABLE = 'iceberg.silver.transfermarkt_competitions_v2'
EDITIONS_REGISTRY_TABLE = 'iceberg.silver.transfermarkt_competition_editions_v2'

COMPETITIONS_BRONZE_TABLE = 'iceberg.bronze.transfermarkt_competitions'
EDITIONS_BRONZE_TABLE = 'iceberg.bronze.transfermarkt_competition_editions'
MEMBERSHIPS_BRONZE_TABLE = 'iceberg.bronze.transfermarkt_squad_memberships'
COACH_PROFILES_BRONZE_TABLE = 'iceberg.bronze.transfermarkt_coach_profiles'
COACH_STINTS_BRONZE_TABLE = 'iceberg.bronze.transfermarkt_coach_stints'

ZONES = ('full', 'scope_set', 'legacy')

# One scope-set IN-predicate chunk.  Bounded so a full-target scope set
# (up to MAX_SCOPE_SET_SIZE = 16384 scopes) never produces one gigantic
# query text; each heavy check runs one query per chunk and sums.
SCOPE_PREDICATE_CHUNK_SIZE = 1000

# Scope-column kind per native Bronze relation.  Must stay identical to the
# DAG's ``_NATIVE_BRONZE_SCOPE_COLUMNS`` (asserted by unit tests).
NATIVE_BRONZE_SCOPE_COLUMNS = {
    'iceberg.bronze.transfermarkt_competitions': 'competition',
    'iceberg.bronze.transfermarkt_competition_editions': 'edition',
    'iceberg.bronze.transfermarkt_squad_memberships': 'edition',
    'iceberg.bronze.transfermarkt_player_attribute_observations': 'edition',
    'iceberg.bronze.transfermarkt_player_contract_observations': 'edition',
    'iceberg.bronze.transfermarkt_market_value_points': 'source_edition',
    'iceberg.bronze.transfermarkt_transfer_events': 'source_edition',
    'iceberg.bronze.transfermarkt_coach_profiles': 'source_edition',
    'iceberg.bronze.transfermarkt_coach_stints': 'source_edition',
}

# Contract natural keys per native Bronze relation.  Must stay identical to
# the bronze-layer ``TABLE_CONTRACTS`` in ``utils.transfermarkt_native_v2``
# (asserted by unit tests).
NATIVE_BRONZE_KEYS = {
    'iceberg.bronze.transfermarkt_competitions': ('competition_id',),
    'iceberg.bronze.transfermarkt_competition_editions': (
        'competition_id', 'edition_id',
    ),
    'iceberg.bronze.transfermarkt_squad_memberships': (
        'competition_id', 'edition_id', 'club_id', 'player_id',
    ),
    'iceberg.bronze.transfermarkt_player_attribute_observations': (
        'competition_id', 'edition_id', 'club_id', 'player_id', 'observed_at',
    ),
    'iceberg.bronze.transfermarkt_player_contract_observations': (
        'competition_id', 'edition_id', 'team_id', 'player_id', 'observed_at',
    ),
    'iceberg.bronze.transfermarkt_market_value_points': (
        'player_id', 'mv_date',
    ),
    'iceberg.bronze.transfermarkt_transfer_events': ('transfer_id',),
    'iceberg.bronze.transfermarkt_coach_profiles': ('coach_id',),
    'iceberg.bronze.transfermarkt_coach_stints': (
        'club_id', 'coach_id', 'appointed_date', 'left_date',
    ),
}

# Entity name (as used in scope manifests) -> scoped Bronze entity relation.
# Order mirrors ``utils.transfermarkt_native_v2.NATIVE_ENTITIES``.
ENTITY_BRONZE_TABLES = {
    'squad_memberships': 'iceberg.bronze.transfermarkt_squad_memberships',
    'player_attribute_observations': (
        'iceberg.bronze.transfermarkt_player_attribute_observations'
    ),
    'player_contract_observations': (
        'iceberg.bronze.transfermarkt_player_contract_observations'
    ),
    'market_value_points': 'iceberg.bronze.transfermarkt_market_value_points',
    'transfer_events': 'iceberg.bronze.transfermarkt_transfer_events',
    'coach_profiles': 'iceberg.bronze.transfermarkt_coach_profiles',
    'coach_stints': 'iceberg.bronze.transfermarkt_coach_stints',
}

# Legacy compatibility relations and their natural keys.  For players and
# coaches the physical Bronze grain includes the club: a mid-season transfer
# legitimately yields one row per club within the same (league, season) and
# batch (live 2026-07-14: all 1733 player duplicate groups under the
# club-less key were multi-club).  This matches the legacy coaches
# replace_keys (league, season, current_club_id).
LEGACY_BRONZE_KEYS = {
    'iceberg.bronze.transfermarkt_players': (
        'player_id', 'league', 'season', 'current_club_id',
    ),
    'iceberg.bronze.transfermarkt_market_value_history': (
        'player_id', 'mv_date', 'league', 'season',
    ),
    'iceberg.bronze.transfermarkt_transfers': (
        'player_id', 'transfer_date', 'from_club_id', 'to_club_id',
        'league', 'season',
    ),
    'iceberg.bronze.transfermarkt_coaches': (
        'coach_id', 'league', 'season', 'current_club_id',
    ),
}

# Semantic payload projections (contract columns minus keys and ingest
# lineage).  A duplicate group whose payload projection has more than one
# distinct value inside one batch is a CONFLICT (ERROR); identical repeats
# are WARNING-only (see the module docstring).
NATIVE_PAYLOAD_COLUMNS = {
    'iceberg.bronze.transfermarkt_competitions': (
        'slug', 'name', 'country', 'confederation', 'competition_type',
        'gender', 'team_type', 'age_category', 'season_format', 'active',
        'canonical_competition_id', 'classification_status',
        'classification_evidence',
    ),
    'iceberg.bronze.transfermarkt_competition_editions': (
        'edition_label', 'canonical_season', 'season_format', 'start_date',
        'end_date', 'active', 'current', 'participant_count',
        'participant_hash',
    ),
    # observed_at is deliberately NOT payload for memberships: two fetches
    # of the same squad page within one batch legitimately carry different
    # capture timestamps — that is repetition, not two truths.
    'iceberg.bronze.transfermarkt_squad_memberships': (
        'league', 'season', 'club_slug', 'club_name', 'player_slug',
        'player_name',
    ),
    'iceberg.bronze.transfermarkt_player_attribute_observations': (
        'player_slug', 'name', 'position', 'dob', 'age', 'height_cm',
        'foot', 'nationality', 'contract_until', 'market_value_eur',
        'league', 'season', 'club_name',
    ),
    'iceberg.bronze.transfermarkt_player_contract_observations': (
        'team_name', 'contract_until', 'applicability_status',
    ),
    'iceberg.bronze.transfermarkt_market_value_points': (
        'value_eur', 'club_name', 'age', 'mv_raw',
    ),
    'iceberg.bronze.transfermarkt_transfer_events': (
        'player_id', 'transfer_date', 'event_season', 'from_club_id',
        'from_club_name', 'to_club_id', 'to_club_name', 'fee_text',
        'fee_eur', 'market_value_eur', 'is_upcoming',
    ),
    'iceberg.bronze.transfermarkt_coach_profiles': (
        'coach_slug', 'name', 'dob', 'nationality',
    ),
    'iceberg.bronze.transfermarkt_coach_stints': (
        'club_name', 'coach_slug', 'name', 'role',
    ),
}
LEGACY_PAYLOAD_COLUMNS = {
    'iceberg.bronze.transfermarkt_players': (
        'player_slug', 'name', 'position', 'dob', 'age', 'height_cm',
        'foot', 'nationality', 'contract_until', 'market_value_eur',
        'market_value_last_update', 'current_club_name',
    ),
    'iceberg.bronze.transfermarkt_market_value_history': (
        'value_eur', 'club_name', 'age', 'mv_raw',
    ),
    'iceberg.bronze.transfermarkt_transfers': (
        'from_club_name', 'to_club_name', 'fee_text', 'fee_eur',
        'market_value_eur', 'is_upcoming',
    ),
    'iceberg.bronze.transfermarkt_coaches': (
        'coach_slug', 'name', 'role', 'dob', 'nationality',
        'current_club_name',
    ),
}

# Registry relations carry ``registry_snapshot_id`` as part of their
# physical identity: one Bronze table legally stores many snapshots.
_SNAPSHOT_KEYED_TABLES = (COMPETITIONS_BRONZE_TABLE, EDITIONS_BRONZE_TABLE)

# Scoped observation relations whose (scope, club/team, player) identity must
# exist in same-scope squad memberships: own column -> memberships column.
_MEMBERSHIP_CLUB_COLUMNS = {
    'iceberg.bronze.transfermarkt_player_attribute_observations': (
        'club_id', 'club_id',
    ),
    'iceberg.bronze.transfermarkt_player_contract_observations': (
        'team_id', 'club_id',
    ),
}
_MEMBERSHIP_PLAYER_TABLES = (
    'iceberg.bronze.transfermarkt_market_value_points',
    'iceberg.bronze.transfermarkt_transfer_events',
)


@dataclass
class BronzeCheckResult:
    """Shape-compatible with ``utils.data_quality.CheckResult``."""

    name: str
    kind: str
    severity: str
    passed: bool
    details: str = ''
    value: Any = None
    error: str | None = None


@dataclass
class BronzeDqReport:
    """Duck-type compatible with ``utils.data_quality.RunReport``."""

    results: List[BronzeCheckResult] = field(default_factory=list)

    @property
    def errors(self) -> List[BronzeCheckResult]:
        return [
            r for r in self.results if not r.passed and r.severity == 'ERROR'
        ]

    @property
    def warnings(self) -> List[BronzeCheckResult]:
        return [
            r for r in self.results if not r.passed and r.severity == 'WARNING'
        ]

    @property
    def passed(self) -> List[BronzeCheckResult]:
        return [r for r in self.results if r.passed]

    def summary(self) -> str:
        return (
            f'{len(self.passed)}/{len(self.results)} passed, '
            f'{len(self.errors)} ERRORs, {len(self.warnings)} WARNINGs'
        )


def _sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _pinned(table: str, pins: Mapping[str, Any] | None) -> str:
    if pins and table in pins:
        snapshot_id = int(pins[table])
        if snapshot_id <= 0:
            raise ValueError(f'invalid pinned snapshot for {table}')
        return f'{table} FOR VERSION AS OF {snapshot_id}'
    return table


def _display(table: str) -> str:
    name = table.rsplit('.', 1)[-1]
    prefix = 'transfermarkt_'
    if table in NATIVE_BRONZE_SCOPE_COLUMNS and name.startswith(prefix):
        return name[len(prefix):]
    return name


def _entity_scope_columns(table: str) -> tuple[str, str]:
    kind = NATIVE_BRONZE_SCOPE_COLUMNS.get(table)
    if kind == 'edition':
        return 'competition_id', 'edition_id'
    if kind == 'source_edition':
        return 'source_competition_id', 'source_edition_id'
    raise ValueError(f'{table}: not an edition-scoped native Bronze relation')


def _scope_pairs_predicate(
    table: str,
    scope_pairs: Sequence[Sequence[str]],
    *,
    alias: str = '',
) -> str:
    """``(competition_id, edition_id) IN (...)`` for one scope-set chunk."""

    if not scope_pairs:
        raise ValueError('scope predicate requires at least one scope pair')
    comp, ed = _entity_scope_columns(table)
    prefix = f'{alias}.' if alias else ''
    values = ', '.join(
        f'({_sql_literal(pair[0])}, {_sql_literal(pair[1])})'
        for pair in scope_pairs
    )
    return f'({prefix}{comp}, {prefix}{ed}) IN ({values})'


def _chunked(
    items: Sequence[Any], size: int,
) -> Iterable[Sequence[Any]]:
    for index in range(0, len(items), max(1, int(size))):
        yield items[index:index + max(1, int(size))]


# ---------------------------------------------------------------------------
# SQL builders (pure; unit-testable without Trino)
# ---------------------------------------------------------------------------

def build_phantom_scope_sql(
    table: str,
    *,
    registry_snapshot_id: str,
    pins: Mapping[str, Any] | None = None,
    child_cycle_ids: Sequence[str] | None = None,
) -> str:
    """Scoped rows whose (competition, edition) is not in the promoted registry."""

    comp, ed = _entity_scope_columns(table)
    scoped = ""
    if child_cycle_ids is not None:
        if not child_cycle_ids:
            raise ValueError('child-cycle predicate requires at least one cycle')
        scoped = "\n  AND b.cycle_id IN (" + ", ".join(
            _sql_literal(item) for item in child_cycle_ids
        ) + ")"
    return f"""SELECT COUNT(*)
FROM {_pinned(table, pins)} b
WHERE b.{comp} IS NOT NULL
  AND b.{ed} IS NOT NULL
  {scoped}
  AND NOT EXISTS (
      SELECT 1
      FROM {_pinned(EDITIONS_REGISTRY_TABLE, pins)} r
      WHERE r.registry_snapshot_id = {_sql_literal(registry_snapshot_id)}
        AND r.competition_id = b.{comp}
        AND r.edition_id = b.{ed}
  )"""


def build_scope_ownership_sql(
    table: str,
    *,
    pins: Mapping[str, Any] | None,
    scope_bindings: Sequence[Sequence[str]],
) -> str:
    """Rows owned by a child cycle must match its exact frozen scope identity."""

    if not scope_bindings:
        raise ValueError('scope ownership requires at least one binding')
    comp, ed = _entity_scope_columns(table)
    values = ', '.join(
        '(' + ', '.join(_sql_literal(value) for value in binding) + ')'
        for binding in scope_bindings
    )
    return f"""WITH expected (
    child_cycle_id, scope_id, competition_id, edition_id
) AS (VALUES {values})
SELECT COUNT(*)
FROM {_pinned(table, pins)} b
JOIN expected e ON b.cycle_id = e.child_cycle_id
WHERE COALESCE(b.scope_id, '') <> e.scope_id
   OR COALESCE(CAST(b.{comp} AS varchar), '') <> e.competition_id
   OR COALESCE(CAST(b.{ed} AS varchar), '') <> e.edition_id"""


def build_registry_expected_counts_sql(registry_snapshot_id: str) -> str:
    return f"""SELECT DISTINCT competition_count, edition_count
FROM {REGISTRY_STATE_TABLE}
WHERE (state_key = 'canonical' OR regexp_like(state_key, '^history:[0-9]+$'))
  AND registry_snapshot_id = {_sql_literal(registry_snapshot_id)}
  AND status = 'promoted'
  AND unknown_active_count = 0"""


def build_promoted_snapshot_count_sql(
    kind: str,
    *,
    registry_snapshot_id: str,
    pins: Mapping[str, Any] | None = None,
) -> str:
    """Distinct promoted-snapshot registry keys actually present in Bronze."""

    snapshot = _sql_literal(registry_snapshot_id)
    if kind == 'competitions':
        return (
            'SELECT COUNT(DISTINCT competition_id)\n'
            f'FROM {_pinned(COMPETITIONS_BRONZE_TABLE, pins)}\n'
            f'WHERE registry_snapshot_id = {snapshot}'
        )
    if kind == 'competition_editions':
        return f"""SELECT COUNT(*)
FROM (
    SELECT DISTINCT competition_id, edition_id
    FROM {_pinned(EDITIONS_BRONZE_TABLE, pins)}
    WHERE registry_snapshot_id = {snapshot}
)"""
    raise ValueError(f'unknown promoted-snapshot kind: {kind!r}')


def build_partial_scope_sql(
    table: str,
    *,
    pins: Mapping[str, Any] | None = None,
    scope_pairs: Sequence[Sequence[str]] | None = None,
) -> str:
    """Half-scoped rows, plus NULL-scope rows that still claim a crawl scope.

    With ``scope_pairs`` (scope_set zone) only half-scoped rows whose
    non-NULL half touches the scope set are counted; the mislabelled
    NULL-scope branch cannot be attributed to any scope and is checked by
    the ``full`` sweep only.
    """

    comp, ed = _entity_scope_columns(table)
    if scope_pairs is not None:
        if not scope_pairs:
            raise ValueError('scope predicate requires at least one scope pair')
        comps = ', '.join(sorted({
            _sql_literal(pair[0]) for pair in scope_pairs
        }))
        eds = ', '.join(sorted({
            _sql_literal(pair[1]) for pair in scope_pairs
        }))
        return f"""SELECT COUNT(*)
FROM {_pinned(table, pins)} b
WHERE ((b.{comp} IS NULL) <> (b.{ed} IS NULL))
  AND (b.{comp} IN ({comps}) OR b.{ed} IN ({eds}))"""
    return f"""SELECT COUNT(*)
FROM {_pinned(table, pins)} b
WHERE ((b.{comp} IS NULL) <> (b.{ed} IS NULL))
   OR (b.{comp} IS NULL AND b.{ed} IS NULL
       AND (COALESCE(b.scope_id, '') <> '' OR COALESCE(b.cycle_id, '') <> ''))"""


def _duplicate_split_sql(
    table: str,
    group_columns: Sequence[str],
    payload_columns: Sequence[str],
    pins: Mapping[str, Any] | None,
    where: str | None = None,
) -> str:
    """Two-column result: (conflicting groups, identical extra rows)."""

    group = ', '.join(group_columns)
    payload = ', '.join(payload_columns)
    where_clause = f'\n    WHERE {where}' if where else ''
    return f"""SELECT
    COALESCE(SUM(CASE WHEN payload_variants > 1 THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN payload_variants <= 1 THEN cnt - 1 ELSE 0 END), 0)
FROM (
    SELECT COUNT(*) AS cnt,
           COUNT(DISTINCT ROW({payload})) AS payload_variants
    FROM {_pinned(table, pins)}{where_clause}
    GROUP BY {group}
    HAVING COUNT(*) > 1
)"""


def build_intra_batch_duplicates_sql(
    table: str,
    *,
    pins: Mapping[str, Any] | None = None,
    scope_pairs: Sequence[Sequence[str]] | None = None,
) -> str:
    """Split intra-batch duplicates: conflicting groups vs identical repeats."""

    key = NATIVE_BRONZE_KEYS.get(table)
    if key is None:
        raise ValueError(f'{table}: not a native Bronze relation')
    group = [*key, '_batch_id']
    if table in _SNAPSHOT_KEYED_TABLES:
        group.append('registry_snapshot_id')
    where = (
        _scope_pairs_predicate(table, scope_pairs)
        if scope_pairs is not None else None
    )
    return _duplicate_split_sql(
        table, group, NATIVE_PAYLOAD_COLUMNS[table], pins, where=where,
    )


def build_cross_batch_duplicates_sql(
    table: str,
    *,
    pins: Mapping[str, Any] | None = None,
    scope_pairs: Sequence[Sequence[str]] | None = None,
) -> str:
    """WARNING-only: contract-PK rows repeated across batches (see docstring).

    Counts one violation per extra batch that re-emits an existing key
    (``SUM(distinct batches - 1)`` over multi-batch groups), so intra-batch
    repeats — already measured by the intra-batch split — are never
    double-counted here.
    """

    key = NATIVE_BRONZE_KEYS.get(table)
    if key is None:
        raise ValueError(f'{table}: not a native Bronze relation')
    group = list(key)
    if table in _SNAPSHOT_KEYED_TABLES:
        group.append('registry_snapshot_id')
    if scope_pairs is not None:
        where = _scope_pairs_predicate(table, scope_pairs)
    elif (
        NATIVE_BRONZE_SCOPE_COLUMNS.get(table) == 'edition'
        and table not in _SNAPSHOT_KEYED_TABLES
    ):
        # The contract PK embeds the crawl scope; NULL-scope cohort rows have
        # no such identity and are counted by tm_bronze_legacy_cohort instead.
        where = 'competition_id IS NOT NULL AND edition_id IS NOT NULL'
    else:
        where = None
    columns = ', '.join(group)
    where_clause = f'\n    WHERE {where}' if where else ''
    return f"""SELECT COALESCE(SUM(batches - 1), 0)
FROM (
    SELECT COUNT(DISTINCT _batch_id) AS batches
    FROM {_pinned(table, pins)}{where_clause}
    GROUP BY {columns}
    HAVING COUNT(DISTINCT _batch_id) > 1
)"""


def build_legacy_intra_batch_duplicates_sql(
    table: str,
    *,
    pins: Mapping[str, Any] | None = None,
) -> str:
    """Split legacy intra-batch duplicates: conflicts vs identical repeats."""

    key = LEGACY_BRONZE_KEYS.get(table)
    if key is None:
        raise ValueError(f'{table}: not a legacy Bronze relation')
    return _duplicate_split_sql(
        table, [*key, '_batch_id'], LEGACY_PAYLOAD_COLUMNS[table], pins,
    )


def build_legacy_phantom_sql(
    table: str,
    *,
    legacy_allowlist: Iterable[Sequence[str]],
    registry_snapshot_id: str | None,
    pins: Mapping[str, Any] | None = None,
) -> str:
    """Legacy rows whose (league, season) matches neither allowlist branch.

    The allowlist is the union of (a) curated ``competitions.yaml`` pairs,
    supplied by the caller, and (b) the promoted registry's canonical pairs
    (``COALESCE(canonical_competition_id, 'TM-' || competition_id)`` ×
    ``canonical_season``), resolved as a SQL branch.  With
    ``registry_snapshot_id=None`` (degraded rollback mode, see the module
    docstring) the registry branch is omitted and only the curated YAML
    pairs remain.
    """

    if table not in LEGACY_BRONZE_KEYS:
        raise ValueError(f'{table}: not a legacy Bronze relation')
    pairs = sorted({
        (str(league), str(season)) for league, season in legacy_allowlist
    })
    if not pairs and registry_snapshot_id is None:
        raise ValueError('legacy phantom check requires an allowlist branch')
    branches = []
    if pairs:
        values = ', '.join(
            f'({_sql_literal(league)}, {_sql_literal(season)})'
            for league, season in pairs
        )
        branches.append(f"""NOT EXISTS (
      SELECT 1
      FROM (VALUES {values}) AS allowed(league, season)
      WHERE allowed.league = b.league
        AND allowed.season = b.season
  )""")
    if registry_snapshot_id is not None:
        branches.append(f"""NOT EXISTS (
      SELECT 1
      FROM {_pinned(COMPETITIONS_REGISTRY_TABLE, pins)} c
      JOIN {_pinned(EDITIONS_REGISTRY_TABLE, pins)} e
        ON e.competition_id = c.competition_id
       AND e.registry_snapshot_id = c.registry_snapshot_id
      WHERE c.registry_snapshot_id = {_sql_literal(registry_snapshot_id)}
        AND COALESCE(c.canonical_competition_id, 'TM-' || c.competition_id)
            = b.league
        AND e.canonical_season = b.season
  )""")
    predicate = '\n  AND '.join(branches)
    return f'SELECT COUNT(*)\nFROM {_pinned(table, pins)} b\nWHERE {predicate}'


def build_membership_orphans_sql(
    table: str,
    *,
    pins: Mapping[str, Any] | None = None,
    scope_pairs: Sequence[Sequence[str]] | None = None,
) -> str:
    """Scoped entity rows without their same-scope membership/profile parent."""

    memberships = _pinned(MEMBERSHIPS_BRONZE_TABLE, pins)
    comp, ed = _entity_scope_columns(table)
    if scope_pairs is not None:
        # The IN-predicate implies both scope columns are non-NULL.
        scoped = _scope_pairs_predicate(table, scope_pairs, alias='b')
    else:
        scoped = f'b.{comp} IS NOT NULL\n  AND b.{ed} IS NOT NULL'
    if table in _MEMBERSHIP_CLUB_COLUMNS:
        own_club, member_club = _MEMBERSHIP_CLUB_COLUMNS[table]
        exists = f"""SELECT 1
      FROM {memberships} m
      WHERE m.competition_id = b.{comp}
        AND m.edition_id = b.{ed}
        AND m.{member_club} = b.{own_club}
        AND m.player_id = b.player_id"""
    elif table in _MEMBERSHIP_PLAYER_TABLES:
        exists = f"""SELECT 1
      FROM {memberships} m
      WHERE m.competition_id = b.{comp}
        AND m.edition_id = b.{ed}
        AND m.player_id = b.player_id"""
    elif table == COACH_STINTS_BRONZE_TABLE:
        exists = f"""SELECT 1
      FROM {_pinned(COACH_PROFILES_BRONZE_TABLE, pins)} p
      WHERE p.coach_id = b.coach_id"""
    else:
        raise ValueError(f'{table}: no membership-orphan contract')
    return f"""SELECT COUNT(*)
FROM {_pinned(table, pins)} b
WHERE {scoped}
  AND NOT EXISTS (
      {exists}
  )"""


def build_scope_pair_counts_sql(
    table: str,
    *,
    pins: Mapping[str, Any] | None = None,
) -> str:
    """Per-(competition, edition) row counts for one scoped entity relation."""

    comp, ed = _entity_scope_columns(table)
    return f"""SELECT {comp}, {ed}, COUNT(*)
FROM {_pinned(table, pins)}
WHERE {comp} IS NOT NULL AND {ed} IS NOT NULL
GROUP BY {comp}, {ed}"""


def build_null_scope_cohort_sql(
    table: str,
    *,
    pins: Mapping[str, Any] | None = None,
) -> str:
    comp, ed = _entity_scope_columns(table)
    return (
        f'SELECT COUNT(*)\nFROM {_pinned(table, pins)}\n'
        f'WHERE {comp} IS NULL AND {ed} IS NULL'
    )


def build_registry_target_sql(registry_snapshot_id: str) -> str:
    """Full eligible senior-men target of one promoted registry snapshot.

    The predicate mirrors ``_assert_complete_promoted_registry_target`` in
    ``dag_transform_transfermarkt_silver`` and
    ``transfermarkt_scope_planner.eligible_registry_scopes``.  Kept free of
    Airflow imports so phase F3 can reuse it standalone.
    """

    return f"""SELECT c.competition_id, e.edition_id
FROM {COMPETITIONS_REGISTRY_TABLE} c
JOIN {EDITIONS_REGISTRY_TABLE} e
  ON e.competition_id = c.competition_id
 AND e.registry_snapshot_id = c.registry_snapshot_id
WHERE c.registry_snapshot_id = {_sql_literal(registry_snapshot_id)}
  AND c.active = true
  AND c.classification_status = 'eligible'
  AND c.gender = 'men'
  AND c.age_category = 'senior'
  AND c.team_type IN ('club', 'national_team')
  AND c.competition_type IN (
      'domestic_league', 'domestic_cup', 'continental_club',
      'national_team_tournament'
  )
  AND e.active = true
ORDER BY c.competition_id, e.edition_id"""


def build_complete_scope_pairs_sql() -> str:
    """Scopes with a latest complete manifest (CTE form mirrors
    ``last_complete_scope`` in ``build_promoted_registry_query``)."""

    return f"""SELECT competition_id, edition_id
FROM (
    SELECT competition_id, edition_id, ROW_NUMBER() OVER (
        PARTITION BY competition_id, edition_id
        ORDER BY committed_at DESC
    ) AS rn
    FROM {SCOPE_MANIFEST_TABLE}
    WHERE status = {_sql_literal(SCOPE_COMPLETION_STATUS)}
)
WHERE rn = 1"""


def build_career_debt_sql() -> str:
    """Career-fetch debt over latest complete manifests + NULL-evidence count."""

    return f"""SELECT COALESCE(SUM(pending), 0),
       COUNT_IF(pending IS NULL),
       COUNT(*)
FROM (
    SELECT TRY_CAST(json_extract_scalar(
        entity_manifest_json, '$.dq_evidence.career_fetches_pending'
    ) AS bigint) AS pending
    FROM (
        SELECT entity_manifest_json, ROW_NUMBER() OVER (
            PARTITION BY competition_id, edition_id
            ORDER BY committed_at DESC
        ) AS rn
        FROM {SCOPE_MANIFEST_TABLE}
        WHERE status = {_sql_literal(SCOPE_COMPLETION_STATUS)}
    )
    WHERE rn = 1
)"""


# ---------------------------------------------------------------------------
# Cursor-driven helpers
# ---------------------------------------------------------------------------

def _scalar(cur: Any, sql: str) -> int:
    cur.execute(sql)
    rows = list(cur.fetchall())
    if len(rows) != 1 or len(rows[0]) < 1:
        raise ValueError('scalar DQ query must return exactly one row')
    return int(rows[0][0])


def _pairs(cur: Any, sql: str) -> set[tuple[str, str]]:
    cur.execute(sql)
    return {(str(row[0]), str(row[1])) for row in cur.fetchall()}


def resolve_promoted_snapshot(cur: Any) -> str:
    """Return the single green promoted canonical snapshot, or fail closed."""

    cur.execute(
        'SELECT registry_snapshot_id, status, unknown_active_count\n'
        f"FROM {REGISTRY_STATE_TABLE}\nWHERE state_key = 'canonical'"
    )
    rows = list(cur.fetchall())
    if len(rows) != 1:
        raise ValueError(
            'promoted registry must have exactly one canonical state row'
        )
    snapshot, status, unknown_active = rows[0]
    if str(status) != 'promoted' or int(unknown_active) != 0:
        raise ValueError(
            'canonical registry state is not a green promoted snapshot'
        )
    return str(snapshot)


def target_coverage_report(cur: Any, *, registry_snapshot_id: str) -> dict[str, Any]:
    """Target vs latest-complete scope coverage; no Airflow dependencies."""

    targets = _pairs(cur, build_registry_target_sql(registry_snapshot_id))
    complete = _pairs(cur, build_complete_scope_pairs_sql())
    complete_in_target = targets & complete
    extra = sorted(complete - targets)
    return {
        'registry_snapshot_id': str(registry_snapshot_id),
        'target_scopes': len(targets),
        'complete_scopes': len(complete),
        'complete_in_target': len(complete_in_target),
        'coverage_ratio': (
            len(complete_in_target) / len(targets) if targets else 0.0
        ),
        'extra_complete': [list(pair) for pair in extra],
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _clip(items: Sequence[str], limit: int = 5) -> str:
    shown = '; '.join(items[:limit])
    hidden = len(items) - limit
    return shown + (f' (+{hidden} more)' if hidden > 0 else '')


def run_bronze_dq(
    cur: Any,
    *,
    registry_snapshot_id: str | None,
    pins: Mapping[str, Any] | None = None,
    zone: str = 'full',
    manifests: Sequence[Any] | None = None,
    scope_bindings: Sequence[Sequence[str]] | None = None,
    legacy_allowlist: Iterable[Sequence[str]] = (),
) -> list[BronzeCheckResult]:
    """Execute the cross-table Bronze DQ suite and return every result.

    ``manifests`` is a sequence of scope-manifest objects exposing
    ``competition_id``, ``edition_id`` and ``entities`` (each entity with
    ``entity``, ``applicability_status`` and ``dedup_rows``) —
    ``utils.transfermarkt_scope_state.ScopeManifest`` satisfies this.
    """

    if zone not in ZONES:
        raise ValueError(f'unknown Bronze DQ zone: {zone!r}')
    pins = dict(pins or {})
    if zone == 'scope_set':
        if not scope_bindings and manifests:
            scope_bindings = tuple(
                (
                    str(item.child_cycle_id),
                    str(item.scope_id),
                    str(item.competition_id),
                    str(item.edition_id),
                )
                for item in manifests
            )
        if not scope_bindings:
            raise ValueError('scope_set zone requires exact scope bindings')
        required = (
            *NATIVE_BRONZE_SCOPE_COLUMNS,
            COMPETITIONS_REGISTRY_TABLE,
            EDITIONS_REGISTRY_TABLE,
        )
        missing = sorted(table for table in required if table not in pins)
        if missing:
            raise ValueError(
                'scope_set zone requires pinned snapshots for: '
                + ', '.join(missing)
            )
    allowlist = tuple(
        (str(league), str(season)) for league, season in legacy_allowlist
    )

    results: list[BronzeCheckResult] = []

    def run(name: str, kind: str, severity: str, fn) -> None:
        try:
            passed, details, value = fn()
        except Exception as exc:  # fail closed at declared severity
            results.append(BronzeCheckResult(
                name=name, kind=kind, severity=severity,
                passed=False, error=str(exc),
            ))
        else:
            results.append(BronzeCheckResult(
                name=name, kind=kind, severity=severity,
                passed=passed, details=details, value=value,
            ))

    def zero_violations(sql: str):
        def check():
            count = _scalar(cur, sql)
            return count == 0, f'{count} violating rows', count
        return check

    def summed_zero_violations(sqls: Sequence[str]):
        def check():
            total = sum(_scalar(cur, sql) for sql in sqls)
            return total == 0, f'{total} violating rows', total
        return check

    def run_intra_batch_split(
        prefix: str, table: str, sqls: Sequence[str],
    ) -> None:
        """Split queries -> ERROR (conflicts) + WARNING (identical) results."""

        display = _display(table)
        try:
            conflicts = identical = 0
            for sql in sqls:
                cur.execute(sql)
                rows = list(cur.fetchall())
                if len(rows) != 1 or len(rows[0]) < 2:
                    raise ValueError(
                        'intra-batch split query must return one two-column row'
                    )
                conflicts += int(rows[0][0])
                identical += int(rows[0][1])
        except Exception as exc:  # fail closed at the gating severity
            results.append(BronzeCheckResult(
                name=f'{prefix}_intra_batch_conflicts[{display}]',
                kind=f'{prefix.removeprefix("tm_")}_intra_batch_conflicts',
                severity='ERROR', passed=False, error=str(exc),
            ))
            results.append(BronzeCheckResult(
                name=f'{prefix}_intra_batch_duplicates[{display}]',
                kind=f'{prefix.removeprefix("tm_")}_intra_batch_duplicates',
                severity='WARNING', passed=False, error=str(exc),
            ))
            return
        results.append(BronzeCheckResult(
            name=f'{prefix}_intra_batch_conflicts[{display}]',
            kind=f'{prefix.removeprefix("tm_")}_intra_batch_conflicts',
            severity='ERROR', passed=conflicts == 0,
            details=f'{conflicts} conflicting duplicate groups',
            value=conflicts,
        ))
        results.append(BronzeCheckResult(
            name=f'{prefix}_intra_batch_duplicates[{display}]',
            kind=f'{prefix.removeprefix("tm_")}_intra_batch_duplicates',
            severity='WARNING', passed=identical == 0,
            details=f'{identical} identical duplicate rows',
            value=identical,
        ))

    def run_legacy_checks(
        *, registry_snapshot: str | None, phantom_severity: str,
    ) -> None:
        for table in LEGACY_BRONZE_KEYS:
            run(
                f'tm_legacy_phantom_pair[{_display(table)}]',
                'legacy_phantom_pair', phantom_severity,
                zero_violations(build_legacy_phantom_sql(
                    table,
                    legacy_allowlist=allowlist,
                    registry_snapshot_id=registry_snapshot,
                    pins=pins,
                )),
            )
        for table in LEGACY_BRONZE_KEYS:
            run_intra_batch_split(
                'tm_legacy', table,
                [build_legacy_intra_batch_duplicates_sql(table, pins=pins)],
            )

    if zone == 'legacy':
        # The rollback path degrades instead of dying when the native
        # contour (promoted registry) is broken — see the module docstring.
        degraded_reason: str | None = None
        legacy_snapshot: str | None = (
            str(registry_snapshot_id) if registry_snapshot_id else None
        )
        if legacy_snapshot is None:
            try:
                legacy_snapshot = resolve_promoted_snapshot(cur)
            except Exception as exc:
                degraded_reason = str(exc)
        if degraded_reason is not None:
            results.append(BronzeCheckResult(
                name='tm_registry_unavailable_degraded',
                kind='registry_unavailable_degraded',
                severity='WARNING', passed=False,
                details=(
                    'promoted registry is unavailable; the legacy phantom '
                    'check degraded to the curated YAML allowlist only: '
                    + degraded_reason
                ),
            ))
        run_legacy_checks(
            registry_snapshot=legacy_snapshot,
            phantom_severity=(
                'ERROR' if degraded_reason is None else 'WARNING'
            ),
        )
        return results

    snapshot = (
        str(registry_snapshot_id)
        if registry_snapshot_id
        else resolve_promoted_snapshot(cur)
    )

    scope_pairs: tuple[tuple[str, str], ...] | None = None
    scope_chunks: list[Sequence[tuple[str, str]]] = []
    if zone == 'scope_set':
        normalised_bindings = tuple(sorted({
            tuple(str(value) for value in item) for item in scope_bindings
        }))
        if any(len(item) != 4 or not all(item) for item in normalised_bindings):
            raise ValueError('scope binding must be child/scope/competition/edition')
        scope_pairs = tuple(sorted({
            (item[2], item[3]) for item in normalised_bindings
        }))
        scope_chunks = list(_chunked(scope_pairs, SCOPE_PREDICATE_CHUNK_SIZE))
        binding_chunks = list(_chunked(
            normalised_bindings, SCOPE_PREDICATE_CHUNK_SIZE,
        ))
    else:
        binding_chunks = []

    def entity_sqls(build_fn, table: str) -> list[str]:
        """One full-sweep query, or one query per scope-set chunk."""

        if scope_pairs is None:
            return [build_fn(table, pins=pins)]
        return [
            build_fn(table, pins=pins, scope_pairs=chunk)
            for chunk in scope_chunks
        ]

    entity_tables = tuple(ENTITY_BRONZE_TABLES.values())

    # -- ERROR: promoted registry snapshot is fully present in Bronze --
    def snapshot_presence(kind: str, index: int):
        def check():
            cur.execute(build_registry_expected_counts_sql(snapshot))
            rows = list(cur.fetchall())
            if len(rows) != 1:
                raise ValueError(
                    'promoted registry state row is missing or duplicated'
                )
            expected = int(rows[0][index])
            actual = _scalar(cur, build_promoted_snapshot_count_sql(
                kind, registry_snapshot_id=snapshot, pins=pins,
            ))
            return (
                expected > 0 and actual == expected,
                f'bronze={actual} registry={expected}',
                {'bronze': actual, 'registry': expected},
            )
        return check

    for index, kind in enumerate(('competitions', 'competition_editions')):
        run(
            f'tm_bronze_promoted_snapshot_present[{kind}]',
            'bronze_promoted_snapshot_present', 'ERROR',
            snapshot_presence(kind, index),
        )

    # -- ERROR: scoped rows must reference promoted registry editions.
    # A frozen backfill batch only gates its own scope set.  Daily ingest may
    # legitimately publish a scope from a newer registry snapshot while a long
    # campaign is still draining the old one; a global anti-join here would
    # falsely block that campaign.
    for table in entity_tables:
        run(
            f'tm_bronze_phantom_scope[{_display(table)}]',
            'bronze_phantom_scope', 'ERROR',
            summed_zero_violations([
                build_phantom_scope_sql(
                    table,
                    registry_snapshot_id=snapshot,
                    pins=pins,
                    child_cycle_ids=tuple(item[0] for item in chunk),
                )
                for chunk in binding_chunks
            ]) if scope_pairs is not None else zero_violations(
                build_phantom_scope_sql(
                    table, registry_snapshot_id=snapshot, pins=pins,
                )
            ),
        )
        if scope_pairs is not None:
            run(
                f'tm_bronze_scope_ownership[{_display(table)}]',
                'bronze_scope_ownership', 'ERROR',
                summed_zero_violations([
                    build_scope_ownership_sql(
                        table, pins=pins, scope_bindings=chunk,
                    )
                    for chunk in binding_chunks
                ]),
            )

    # -- ERROR: no half-scoped rows / mislabelled NULL-scope rows --
    for table in entity_tables:
        run(
            f'tm_bronze_partial_scope[{_display(table)}]',
            'bronze_partial_scope', 'ERROR',
            summed_zero_violations(
                entity_sqls(build_partial_scope_sql, table)
            ),
        )

    # -- one batch never emits two different truths for one natural key:
    # conflicting payload -> ERROR; identical repeats -> WARNING.  The two
    # registry relations are snapshot-small and stay unfiltered even in
    # scope_set mode.
    for table in NATIVE_BRONZE_KEYS:
        if table in _SNAPSHOT_KEYED_TABLES:
            sqls = [build_intra_batch_duplicates_sql(table, pins=pins)]
        else:
            sqls = entity_sqls(build_intra_batch_duplicates_sql, table)
        run_intra_batch_split('tm_bronze', table, sqls)

    # -- scoped facts resolve to same-scope memberships/profiles.
    # coach_stints is WARNING-only: profiles are fetched for
    # current/recent coaches while stints cover the club's full history
    # (see the module docstring).
    for table in (
        *_MEMBERSHIP_CLUB_COLUMNS,
        *_MEMBERSHIP_PLAYER_TABLES,
        COACH_STINTS_BRONZE_TABLE,
    ):
        severity = (
            'WARNING' if table == COACH_STINTS_BRONZE_TABLE else 'ERROR'
        )
        run(
            f'tm_bronze_membership_orphans[{_display(table)}]',
            'bronze_membership_orphans', severity,
            summed_zero_violations(
                entity_sqls(build_membership_orphans_sql, table)
            ),
        )

    # -- ERROR: legacy (league, season) must be curated or canonical.
    # Full-sweep only: the legacy tables are outside the scope-set write
    # path and are covered by the periodic full zone.
    if zone == 'full':
        run_legacy_checks(registry_snapshot=snapshot, phantom_severity='ERROR')

    # -- per-scope row counts, shared by presence + unmanifested checks -----
    pair_counts: dict[str, dict[tuple[str, str], int]] = {}
    pair_counts_error: str | None = None
    try:
        for entity, table in ENTITY_BRONZE_TABLES.items():
            cur.execute(build_scope_pair_counts_sql(table, pins=pins))
            pair_counts[entity] = {
                (str(row[0]), str(row[1])): int(row[2])
                for row in cur.fetchall()
            }
    except Exception as exc:
        pair_counts_error = str(exc)

    # -- ERROR: pinned Bronze presence must match every scope manifest ------
    if manifests is not None:
        def scope_presence():
            if pair_counts_error is not None:
                raise ValueError(pair_counts_error)
            violations: list[str] = []
            for manifest in manifests:
                scope_key = (
                    str(manifest.competition_id), str(manifest.edition_id),
                )
                label = '/'.join(scope_key)
                for evidence in manifest.entities:
                    entity = str(evidence.entity)
                    if entity not in pair_counts:
                        raise ValueError(
                            f'{label}: unknown manifest entity {entity!r}'
                        )
                    count = pair_counts[entity].get(scope_key, 0)
                    status = str(evidence.applicability_status)
                    if status == 'ok':
                        if int(evidence.dedup_rows) > 0 and count == 0:
                            violations.append(
                                f'{label}:{entity}: manifest has '
                                f'{evidence.dedup_rows} rows, Bronze has none'
                            )
                    elif status in ('authoritative_empty', 'not_applicable'):
                        if count != 0:
                            violations.append(
                                f'{label}:{entity}: terminal-empty scope has '
                                f'{count} Bronze rows'
                            )
                    else:
                        violations.append(
                            f'{label}:{entity}: unknown applicability '
                            f'status {status!r}'
                        )
            return (
                not violations,
                _clip(violations) if violations else 'all scopes present',
                len(violations),
            )
        run(
            'tm_scope_set_bronze_presence',
            'scope_set_bronze_presence', 'ERROR',
            scope_presence,
        )

    # -- WARNING: observability-only (never gates) ---------------------------
    for table in NATIVE_BRONZE_KEYS:
        if table in _SNAPSHOT_KEYED_TABLES:
            sqls = [build_cross_batch_duplicates_sql(table, pins=pins)]
        else:
            sqls = entity_sqls(build_cross_batch_duplicates_sql, table)
        run(
            f'tm_bronze_cross_batch_duplicates[{_display(table)}]',
            'bronze_cross_batch_duplicates', 'WARNING',
            summed_zero_violations(sqls),
        )

    if zone == 'full':
        # NULL-cohort counters are full-sweep observability; the scope_set
        # zone never touches the pre-native cohort.
        def cohort_counter(table: str):
            def check():
                count = _scalar(
                    cur, build_null_scope_cohort_sql(table, pins=pins),
                )
                return True, f'{count} pre-native NULL-scope rows', count
            return check

        for table in entity_tables:
            run(
                f'tm_bronze_legacy_cohort[{_display(table)}]',
                'bronze_legacy_cohort', 'WARNING',
                cohort_counter(table),
            )

    def unmanifested_scopes():
        if pair_counts_error is not None:
            raise ValueError(pair_counts_error)
        scoped = {
            pair for counts in pair_counts.values() for pair in counts
        }
        complete = _pairs(cur, build_complete_scope_pairs_sql())
        extra = sorted(scoped - complete)
        labels = ['/'.join(pair) for pair in extra]
        return (
            not extra,
            _clip(labels) if labels else 'every scoped pair has a complete manifest',
            [list(pair) for pair in extra],
        )
    run(
        'tm_bronze_unmanifested_scopes',
        'bronze_unmanifested_scopes', 'WARNING',
        unmanifested_scopes,
    )

    def coverage():
        report = target_coverage_report(cur, registry_snapshot_id=snapshot)
        details = (
            f"complete {report['complete_in_target']}/{report['target_scopes']}"
            f" target scopes (ratio={report['coverage_ratio']:.4f}),"
            f" extra_complete={len(report['extra_complete'])}"
        )
        return not report['extra_complete'], details, report
    run('tm_target_scope_coverage', 'target_scope_coverage', 'WARNING', coverage)

    def career_debt():
        cur.execute(build_career_debt_sql())
        rows = list(cur.fetchall())
        if len(rows) != 1:
            raise ValueError('career debt query must return exactly one row')
        pending, null_evidence, manifest_count = (
            int(rows[0][0]), int(rows[0][1]), int(rows[0][2]),
        )
        value = {
            'career_fetches_pending': pending,
            'null_evidence_manifests': null_evidence,
            'complete_manifests': manifest_count,
        }
        details = (
            f'{pending} pending career fetches across {manifest_count} '
            f'complete manifests; {null_evidence} without evidence'
        )
        return True, details, value
    run('tm_career_debt', 'career_debt', 'WARNING', career_debt)

    return results
