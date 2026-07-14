"""Unit tests for the #948 cross-table Bronze Transfermarkt DQ module."""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from utils import transfermarkt_bronze_dq as dq


MEMBERSHIPS = 'iceberg.bronze.transfermarkt_squad_memberships'
ATTR_OBS = 'iceberg.bronze.transfermarkt_player_attribute_observations'
CONTRACT_OBS = 'iceberg.bronze.transfermarkt_player_contract_observations'
MV_POINTS = 'iceberg.bronze.transfermarkt_market_value_points'
TRANSFER_EVENTS = 'iceberg.bronze.transfermarkt_transfer_events'
COACH_PROFILES = 'iceberg.bronze.transfermarkt_coach_profiles'
COACH_STINTS = 'iceberg.bronze.transfermarkt_coach_stints'
COMPETITIONS = 'iceberg.bronze.transfermarkt_competitions'
EDITIONS = 'iceberg.bronze.transfermarkt_competition_editions'


def _default_rows(sql: str):
    """Green-Bronze responder for every runner query."""
    if 'competition_count' in sql:
        return [(781, 10253)]
    if 'registry_snapshot_id, status, unknown_active_count' in sql:
        return [('snap-1', 'promoted', 0)]
    if 'career_fetches_pending' in sql:
        return [(0, 0, 0)]
    if "classification_status = 'eligible'" in sql:
        return []
    if 'ROW_NUMBER() OVER' in sql:
        return []
    if 'payload_variants' in sql:
        # Intra-batch split queries return (conflicts, identical extras).
        return [(0, 0)]
    if 'COUNT(DISTINCT _batch_id) > 1' in sql:
        # Cross-batch queries return a single repeated-appearance sum.
        return [(0,)]
    if 'COUNT(DISTINCT competition_id)' in sql:
        return [(781,)]
    if 'SELECT DISTINCT competition_id, edition_id' in sql:
        return [(10253,)]
    if 'GROUP BY' in sql:
        return []
    return [(0,)]


class StubCursor:
    """Dispatches canned rows; overrides are (predicate-or-substring, rows)."""

    def __init__(self, overrides=()):
        self.overrides = list(overrides)
        self.sql_log = []
        self._rows = []

    def execute(self, sql):
        self.sql_log.append(sql)
        for match, rows in self.overrides:
            hit = match(sql) if callable(match) else (match in sql)
            if hit:
                self._rows = rows
                return
        self._rows = _default_rows(sql)

    def fetchall(self):
        return self._rows


def _pair_counts_override(table, rows):
    return (
        lambda sql, t=table: (
            f'FROM {t}\n' in sql and 'GROUP BY' in sql and 'HAVING' not in sql
        ),
        rows,
    )


def _full_pins():
    return {
        table: index + 1
        for index, table in enumerate((
            *dq.NATIVE_BRONZE_SCOPE_COLUMNS,
            dq.COMPETITIONS_REGISTRY_TABLE,
            dq.EDITIONS_REGISTRY_TABLE,
        ))
    }


# ---------------------------------------------------------------------------
# SQL builders
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('table, comp, ed', [
    (MEMBERSHIPS, 'competition_id', 'edition_id'),
    (ATTR_OBS, 'competition_id', 'edition_id'),
    (CONTRACT_OBS, 'competition_id', 'edition_id'),
    (MV_POINTS, 'source_competition_id', 'source_edition_id'),
    (TRANSFER_EVENTS, 'source_competition_id', 'source_edition_id'),
    (COACH_PROFILES, 'source_competition_id', 'source_edition_id'),
    (COACH_STINTS, 'source_competition_id', 'source_edition_id'),
])
def test_phantom_sql_uses_promoted_snapshot_and_correct_scope_columns(
    table, comp, ed,
):
    sql = dq.build_phantom_scope_sql(table, registry_snapshot_id='snap-1')
    assert 'NOT EXISTS' in sql
    assert dq.EDITIONS_REGISTRY_TABLE in sql
    assert "r.registry_snapshot_id = 'snap-1'" in sql
    assert f'r.competition_id = b.{comp}' in sql
    assert f'r.edition_id = b.{ed}' in sql


def test_phantom_sql_ignores_null_scope_rows():
    sql = dq.build_phantom_scope_sql(MV_POINTS, registry_snapshot_id='snap-1')
    assert 'b.source_competition_id IS NOT NULL' in sql
    assert 'b.source_edition_id IS NOT NULL' in sql
    sql = dq.build_phantom_scope_sql(MEMBERSHIPS, registry_snapshot_id='snap-1')
    assert 'b.competition_id IS NOT NULL' in sql
    assert 'b.edition_id IS NOT NULL' in sql


def test_intra_batch_duplicate_sql_appends_batch_and_snapshot_keys():
    comp_sql = dq.build_intra_batch_duplicates_sql(COMPETITIONS)
    assert 'GROUP BY competition_id, _batch_id, registry_snapshot_id' in comp_sql
    ed_sql = dq.build_intra_batch_duplicates_sql(EDITIONS)
    assert (
        'GROUP BY competition_id, edition_id, _batch_id, registry_snapshot_id'
        in ed_sql
    )
    member_sql = dq.build_intra_batch_duplicates_sql(MEMBERSHIPS)
    assert (
        'GROUP BY competition_id, edition_id, club_id, player_id, _batch_id'
        in member_sql
    )
    assert 'registry_snapshot_id' not in member_sql
    # The split query measures payload conflicts inside each duplicate group.
    assert 'COUNT(DISTINCT ROW(' in member_sql
    # Legacy players/coaches physical grain includes the club (mid-season
    # transfers produce one row per club within one league-season).
    legacy_sql = dq.build_legacy_intra_batch_duplicates_sql(
        'iceberg.bronze.transfermarkt_players',
    )
    assert (
        'GROUP BY player_id, league, season, current_club_id, _batch_id'
        in legacy_sql
    )
    coaches_sql = dq.build_legacy_intra_batch_duplicates_sql(
        'iceberg.bronze.transfermarkt_coaches',
    )
    assert (
        'GROUP BY coach_id, league, season, current_club_id, _batch_id'
        in coaches_sql
    )


def test_intra_batch_conflicts_gate_only_on_conflicting_payload():
    mv_table = 'iceberg.bronze.transfermarkt_market_value_history'
    split_hit = (
        lambda sql, t=mv_table: f'FROM {t}\n' in sql and '_batch_id' in sql
    )
    cur = StubCursor(overrides=[(split_hit, [(3, 41)])])
    results = dq.run_bronze_dq(
        cur, registry_snapshot_id='snap-1', zone='legacy', legacy_allowlist=[],
    )
    conflicts = next(
        r for r in results
        if r.name == (
            'tm_legacy_intra_batch_conflicts'
            '[transfermarkt_market_value_history]'
        )
    )
    identical = next(
        r for r in results
        if r.name == (
            'tm_legacy_intra_batch_duplicates'
            '[transfermarkt_market_value_history]'
        )
    )
    assert conflicts.severity == 'ERROR'
    assert not conflicts.passed and conflicts.value == 3
    assert identical.severity == 'WARNING'
    assert not identical.passed and identical.value == 41
    # Only the conflicting groups gate the run.
    report = dq.BronzeDqReport(results)
    assert [r.name for r in report.errors] == [conflicts.name]

    # Identical-only repeats never gate: ERROR passes, WARNING fires.
    cur = StubCursor(overrides=[(split_hit, [(0, 5)])])
    results = dq.run_bronze_dq(
        cur, registry_snapshot_id='snap-1', zone='legacy', legacy_allowlist=[],
    )
    assert dq.BronzeDqReport(results).errors == []
    identical = next(
        r for r in results
        if r.name == (
            'tm_legacy_intra_batch_duplicates'
            '[transfermarkt_market_value_history]'
        )
    )
    assert not identical.passed and identical.value == 5


def test_cross_batch_duplicates_are_warning_only():
    cur = StubCursor(overrides=[(
        lambda sql: 'COUNT(DISTINCT _batch_id) > 1' in sql,
        [(5,)],
    )])
    results = dq.run_bronze_dq(
        cur,
        registry_snapshot_id='snap-1',
        zone='full',
        legacy_allowlist=[('ENG-Premier League', '2324')],
    )
    cross = [
        r for r in results
        if r.name.startswith('tm_bronze_cross_batch_duplicates')
    ]
    assert len(cross) == len(dq.NATIVE_BRONZE_KEYS)
    assert all(r.severity == 'WARNING' and not r.passed for r in cross)
    report = dq.BronzeDqReport(results)
    # Cross-batch duplicates never gate: they surface as warnings only.
    assert report.errors == []
    assert {r.name for r in report.warnings} >= {r.name for r in cross}


def test_cross_batch_metric_reads_scoped_rows_for_scope_keyed_tables():
    # Contract PK embeds the crawl scope -> the NULL-scope cohort has no
    # contract identity and must not pollute the metric (it is counted by
    # tm_bronze_legacy_cohort instead).
    for table in (MEMBERSHIPS, ATTR_OBS, CONTRACT_OBS):
        sql = dq.build_cross_batch_duplicates_sql(table)
        assert 'WHERE competition_id IS NOT NULL AND edition_id IS NOT NULL' in sql
    # Global contract keys keep reading the whole table.
    for table in (MV_POINTS, TRANSFER_EVENTS, COACH_PROFILES, COACH_STINTS,
                  COMPETITIONS, EDITIONS):
        assert 'IS NOT NULL' not in dq.build_cross_batch_duplicates_sql(table)
    # Only groups spanning >1 batch count: intra-batch repeats are already
    # measured by the intra-batch split and must never be double-counted.
    for table in dq.NATIVE_BRONZE_KEYS:
        sql = dq.build_cross_batch_duplicates_sql(table)
        assert 'HAVING COUNT(DISTINCT _batch_id) > 1' in sql
        assert 'SUM(batches - 1)' in sql
        assert 'HAVING COUNT(*) > 1' not in sql


def test_coach_stint_orphans_are_warning_only():
    cur = StubCursor(overrides=[(
        lambda sql: 'p.coach_id = b.coach_id' in sql, [(206,)],
    )])
    results = dq.run_bronze_dq(
        cur, registry_snapshot_id='snap-1', zone='full', legacy_allowlist=[],
    )
    orphans = {
        r.name: r for r in results
        if r.name.startswith('tm_bronze_membership_orphans')
    }
    stints = orphans.pop('tm_bronze_membership_orphans[coach_stints]')
    # Historical stints without a fetched profile are a crawl-policy gap,
    # not corruption: observable, never gating.
    assert stints.severity == 'WARNING'
    assert not stints.passed and stints.value == 206
    assert all(r.severity == 'ERROR' for r in orphans.values())
    assert len(orphans) == 4
    assert dq.BronzeDqReport(results).errors == []


def test_partial_scope_rows_fail_closed():
    sql = dq.build_partial_scope_sql(MEMBERSHIPS)
    assert '(b.competition_id IS NULL) <> (b.edition_id IS NULL)' in sql
    assert "COALESCE(b.scope_id, '') <> ''" in sql
    assert "COALESCE(b.cycle_id, '') <> ''" in sql

    cur = StubCursor(overrides=[(
        lambda s: "COALESCE(b.scope_id, '')" in s, [(2,)],
    )])
    results = dq.run_bronze_dq(
        cur, registry_snapshot_id='snap-1', zone='full', legacy_allowlist=[],
    )
    partial = [
        r for r in results if r.name.startswith('tm_bronze_partial_scope')
    ]
    assert len(partial) == len(dq.ENTITY_BRONZE_TABLES)
    assert all(r.severity == 'ERROR' and not r.passed for r in partial)
    assert dq.BronzeDqReport(results).errors


def test_legacy_allowlist_unions_yaml_and_registry_pairs():
    sql = dq.build_legacy_phantom_sql(
        'iceberg.bronze.transfermarkt_players',
        legacy_allowlist=[
            ('ENG-Premier League', '2324'), ('ESP-La Liga', '2425'),
        ],
        registry_snapshot_id='snap-1',
    )
    # Curated YAML pairs are legal via the static VALUES branch.
    assert "('ENG-Premier League', '2324')" in sql
    assert "('ESP-La Liga', '2425')" in sql
    # TM-2DVB×'2324' is in neither branch: not curated, and the registry
    # branch only admits canonical_season values ('2024'/'2025' for 2DVB).
    assert 'TM-2DVB' not in sql
    # The registry branch admits promoted canonical pairs (TM-2DVB×'2024').
    assert (
        "COALESCE(c.canonical_competition_id, 'TM-' || c.competition_id)"
        in sql
    )
    assert 'e.canonical_season = b.season' in sql
    assert "c.registry_snapshot_id = 'snap-1'" in sql
    assert dq.COMPETITIONS_REGISTRY_TABLE in sql
    assert dq.EDITIONS_REGISTRY_TABLE in sql
    # Phantom = matches NEITHER branch (the two NOT EXISTS are AND-ed).
    assert sql.count('NOT EXISTS') == 2
    assert '\n  AND NOT EXISTS' in sql


def test_target_coverage_report_math():
    targets = [
        ('2DVB', '2023'), ('ES1', '2024'), ('GB1', '2024'), ('GB1', '2025'),
    ]
    complete = [('2DVB', '2023'), ('GB1', '2024'), ('XX', '1999')]
    cur = StubCursor(overrides=[
        ("classification_status = 'eligible'", targets),
        ('ROW_NUMBER() OVER', complete),
    ])
    report = dq.target_coverage_report(cur, registry_snapshot_id='snap-1')
    assert report['registry_snapshot_id'] == 'snap-1'
    assert report['target_scopes'] == 4
    assert report['complete_scopes'] == 3
    assert report['complete_in_target'] == 2
    assert report['coverage_ratio'] == pytest.approx(0.5)
    assert report['extra_complete'] == [['XX', '1999']]


def _evidence(entity, status='ok', dedup_rows=1):
    return SimpleNamespace(
        entity=entity, applicability_status=status, dedup_rows=dedup_rows,
    )


def test_scope_set_presence_honors_authoritative_empty_and_not_applicable():
    manifest = SimpleNamespace(
        competition_id='2DVB',
        edition_id='2023',
        entities=(
            _evidence('squad_memberships', dedup_rows=10),
            _evidence('player_attribute_observations', dedup_rows=5),
            _evidence('player_contract_observations', dedup_rows=0),
            _evidence('market_value_points', status='authoritative_empty',
                      dedup_rows=0),
            _evidence('transfer_events', status='not_applicable',
                      dedup_rows=0),
            _evidence('coach_profiles', status='authoritative_empty',
                      dedup_rows=0),
            _evidence('coach_stints', dedup_rows=2),
        ),
    )
    cur = StubCursor(overrides=[
        _pair_counts_override(MEMBERSHIPS, [('2DVB', '2023', 10)]),
        # authoritative_empty entity with Bronze rows -> violation
        _pair_counts_override(MV_POINTS, [('2DVB', '2023', 3)]),
        _pair_counts_override(COACH_STINTS, [('2DVB', '2023', 2)]),
        # attr_obs left absent -> ok-with-rows manifest becomes a violation
    ])
    results = dq.run_bronze_dq(
        cur,
        registry_snapshot_id='snap-1',
        zone='full',
        manifests=[manifest],
        legacy_allowlist=[],
    )
    presence = next(
        r for r in results if r.name == 'tm_scope_set_bronze_presence'
    )
    assert presence.severity == 'ERROR'
    assert not presence.passed
    assert presence.value == 2
    assert 'player_attribute_observations' in presence.details
    assert 'market_value_points' in presence.details
    # not_applicable / authoritative_empty with zero Bronze rows pass, as
    # does an ok entity whose manifest recorded zero rows.
    assert 'transfer_events' not in presence.details
    assert 'coach_profiles' not in presence.details
    assert 'player_contract_observations' not in presence.details


def test_pinned_relation_is_required_in_scope_set_mode():
    manifest = SimpleNamespace(
        competition_id='2DVB',
        edition_id='2023',
        entities=(_evidence('squad_memberships', dedup_rows=1),),
    )
    pins = {
        table: index + 1
        for index, table in enumerate((
            *dq.NATIVE_BRONZE_SCOPE_COLUMNS,
            dq.COMPETITIONS_REGISTRY_TABLE,
            dq.EDITIONS_REGISTRY_TABLE,
        ))
    }
    del pins[dq.EDITIONS_REGISTRY_TABLE]
    with pytest.raises(ValueError, match='requires pinned snapshots'):
        dq.run_bronze_dq(
            StubCursor(),
            registry_snapshot_id='snap-1',
            pins=pins,
            zone='scope_set',
            manifests=[manifest],
            legacy_allowlist=[],
        )
    with pytest.raises(ValueError, match='requires scope manifests'):
        dq.run_bronze_dq(
            StubCursor(),
            registry_snapshot_id='snap-1',
            pins={**pins, dq.EDITIONS_REGISTRY_TABLE: 99},
            zone='scope_set',
            manifests=None,
            legacy_allowlist=[],
        )


def test_run_bronze_dq_rejects_unknown_zone_or_table():
    with pytest.raises(ValueError, match='unknown Bronze DQ zone'):
        dq.run_bronze_dq(
            StubCursor(), registry_snapshot_id='snap-1', zone='bronze',
        )
    with pytest.raises(ValueError, match='not an edition-scoped'):
        dq.build_phantom_scope_sql(
            COMPETITIONS, registry_snapshot_id='snap-1',
        )
    with pytest.raises(ValueError, match='not a native Bronze relation'):
        dq.build_intra_batch_duplicates_sql('iceberg.bronze.nope')
    with pytest.raises(ValueError, match='not a legacy Bronze relation'):
        dq.build_legacy_phantom_sql(
            MEMBERSHIPS, legacy_allowlist=[], registry_snapshot_id='snap-1',
        )
    with pytest.raises(ValueError, match='no membership-orphan contract'):
        dq.build_membership_orphans_sql(MEMBERSHIPS)
    with pytest.raises(ValueError, match='unknown promoted-snapshot kind'):
        dq.build_promoted_snapshot_count_sql(
            'players', registry_snapshot_id='snap-1',
        )
    # Degraded mode without any curated pairs has no allowlist left at all.
    with pytest.raises(ValueError, match='requires an allowlist branch'):
        dq.build_legacy_phantom_sql(
            'iceberg.bronze.transfermarkt_players',
            legacy_allowlist=[],
            registry_snapshot_id=None,
        )
    with pytest.raises(ValueError, match='at least one scope pair'):
        dq.build_intra_batch_duplicates_sql(MEMBERSHIPS, scope_pairs=[])


def _dag_scope_columns():
    source = (
        Path(__file__).resolve().parents[3]
        / 'dags' / 'dag_transform_transfermarkt_silver.py'
    ).read_text(encoding='utf-8')
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and any(
            getattr(target, 'id', None) == '_NATIVE_BRONZE_SCOPE_COLUMNS'
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError('_NATIVE_BRONZE_SCOPE_COLUMNS not found in the DAG')


def test_scope_columns_match_dag_map():
    assert dq.NATIVE_BRONZE_SCOPE_COLUMNS == _dag_scope_columns()

    from utils import transfermarkt_native_v2 as tm_v2
    bronze_keys = {
        contract.output_table: tuple(contract.key_columns)
        for contract in tm_v2.TABLE_CONTRACTS
        if contract.layer == 'bronze'
    }
    assert dq.NATIVE_BRONZE_KEYS == bronze_keys
    assert tuple(dq.ENTITY_BRONZE_TABLES) == tm_v2.NATIVE_ENTITIES

    from utils import transfermarkt_scope_planner as tm_planner
    assert dq.COMPETITIONS_REGISTRY_TABLE == tm_planner.COMPETITIONS_TABLE
    assert dq.EDITIONS_REGISTRY_TABLE == tm_planner.EDITIONS_TABLE


# ---------------------------------------------------------------------------
# scope_set zone cost profile
# ---------------------------------------------------------------------------

def test_scope_set_zone_is_scope_filtered_and_skips_full_sweeps():
    manifest = SimpleNamespace(
        competition_id='2DVB',
        edition_id='2023',
        entities=(_evidence('squad_memberships', dedup_rows=1),),
    )
    cur = StubCursor()
    results = dq.run_bronze_dq(
        cur,
        registry_snapshot_id='snap-1',
        pins=_full_pins(),
        zone='scope_set',
        manifests=[manifest],
        legacy_allowlist=[('ENG-Premier League', '2324')],
    )
    names = [r.name for r in results]
    # Legacy and NULL-cohort checks are full-sweep only.
    assert not [n for n in names if n.startswith('tm_legacy_')]
    assert not [n for n in names if n.startswith('tm_bronze_legacy_cohort')]
    # Light global checks stay present.
    for expected in (
        'tm_bronze_promoted_snapshot_present[competitions]',
        'tm_bronze_phantom_scope[squad_memberships]',
        'tm_scope_set_bronze_presence',
        'tm_bronze_unmanifested_scopes',
        'tm_target_scope_coverage',
        'tm_career_debt',
    ):
        assert expected in names

    # Heavy checks carry the scope-set predicate (source_-prefixed for the
    # source-scoped relations).
    member_intra = [
        s for s in cur.sql_log
        if 'payload_variants' in s and 'transfermarkt_squad_memberships' in s
    ]
    assert member_intra and all(
        "(competition_id, edition_id) IN (('2DVB', '2023'))" in s
        for s in member_intra
    )
    mv_intra = [
        s for s in cur.sql_log
        if 'payload_variants' in s and 'transfermarkt_market_value_points' in s
    ]
    assert mv_intra and all(
        "(source_competition_id, source_edition_id) IN (('2DVB', '2023'))"
        in s for s in mv_intra
    )
    partial = [s for s in cur.sql_log if 'IS NULL) <> (' in s]
    assert len(partial) == len(dq.ENTITY_BRONZE_TABLES)
    assert all(
        "IN ('2DVB')" in s and "IN ('2023')" in s for s in partial
    )
    orphans = [
        s for s in cur.sql_log
        if 'NOT EXISTS' in s
        and ('m.player_id = b.player_id' in s or 'p.coach_id = b.coach_id' in s)
    ]
    assert len(orphans) == 5
    assert all(") IN (('2DVB', '2023'))" in s for s in orphans)
    cross = [s for s in cur.sql_log if 'COUNT(DISTINCT _batch_id) > 1' in s]
    entity_cross = [s for s in cross if 'registry_snapshot_id' not in s]
    assert entity_cross and all(
        ") IN (('2DVB', '2023'))" in s for s in entity_cross
    )


def test_scope_set_predicates_are_chunked(monkeypatch):
    monkeypatch.setattr(dq, 'SCOPE_PREDICATE_CHUNK_SIZE', 1)
    manifests = [
        SimpleNamespace(competition_id='2DVB', edition_id='2023', entities=()),
        SimpleNamespace(competition_id='2DVB', edition_id='2024', entities=()),
    ]

    def mv_split_hit(sql):
        return (
            'payload_variants' in sql
            and 'transfermarkt_market_value_points' in sql
        )

    cur = StubCursor(overrides=[(mv_split_hit, [(1, 2)])])
    results = dq.run_bronze_dq(
        cur,
        registry_snapshot_id='snap-1',
        pins=_full_pins(),
        zone='scope_set',
        manifests=manifests,
        legacy_allowlist=[],
    )
    mv_sqls = [s for s in cur.sql_log if mv_split_hit(s)]
    assert len(mv_sqls) == 2  # one bounded query per chunk
    assert any("IN (('2DVB', '2023'))" in s for s in mv_sqls)
    assert any("IN (('2DVB', '2024'))" in s for s in mv_sqls)
    conflicts = next(
        r for r in results
        if r.name == 'tm_bronze_intra_batch_conflicts[market_value_points]'
    )
    identical = next(
        r for r in results
        if r.name == 'tm_bronze_intra_batch_duplicates[market_value_points]'
    )
    # Chunk results are summed component-wise.
    assert conflicts.value == 2
    assert identical.value == 4


# ---------------------------------------------------------------------------
# legacy zone rollback degradation
# ---------------------------------------------------------------------------

def test_legacy_zone_degrades_when_registry_unavailable():
    cur = StubCursor(overrides=[
        (
            'registry_snapshot_id, status, unknown_active_count',
            [('snap-1', 'pending', 0)],
        ),
        (lambda sql: 'FROM (VALUES' in sql, [(7,)]),
    ])
    results = dq.run_bronze_dq(
        cur,
        registry_snapshot_id=None,
        zone='legacy',
        legacy_allowlist=[('ENG-Premier League', '2324')],
    )
    degraded = next(
        r for r in results if r.name == 'tm_registry_unavailable_degraded'
    )
    assert degraded.severity == 'WARNING'
    assert not degraded.passed
    assert 'degraded to the curated YAML allowlist' in degraded.details
    phantoms = [
        r for r in results if r.name.startswith('tm_legacy_phantom_pair')
    ]
    assert len(phantoms) == 4
    # Degraded phantoms observe (7 rows each) but never gate the rollback.
    assert all(
        r.severity == 'WARNING' and not r.passed and r.value == 7
        for r in phantoms
    )
    assert dq.BronzeDqReport(results).errors == []
    phantom_sqls = [s for s in cur.sql_log if 'FROM (VALUES' in s]
    assert phantom_sqls
    assert all(
        dq.COMPETITIONS_REGISTRY_TABLE not in s for s in phantom_sqls
    )


def test_legacy_zone_green_registry_keeps_error_union():
    cur = StubCursor()
    results = dq.run_bronze_dq(
        cur,
        registry_snapshot_id=None,
        zone='legacy',
        legacy_allowlist=[('ENG-Premier League', '2324')],
    )
    names = [r.name for r in results]
    assert 'tm_registry_unavailable_degraded' not in names
    phantoms = [
        r for r in results if r.name.startswith('tm_legacy_phantom_pair')
    ]
    assert len(phantoms) == 4
    assert all(r.severity == 'ERROR' for r in phantoms)
    phantom_sqls = [s for s in cur.sql_log if 'FROM (VALUES' in s]
    assert phantom_sqls
    assert all(dq.COMPETITIONS_REGISTRY_TABLE in s for s in phantom_sqls)
    assert all("= 'snap-1'" in s for s in phantom_sqls)


def test_resolve_promoted_snapshot_fail_closed():
    assert dq.resolve_promoted_snapshot(StubCursor()) == 'snap-1'
    state_key = 'registry_snapshot_id, status, unknown_active_count'
    for rows, match in (
        ([('snap-1', 'pending', 0)], 'not a green promoted'),
        ([('snap-1', 'promoted', 3)], 'not a green promoted'),
        ([], 'exactly one canonical state row'),
        (
            [('a', 'promoted', 0), ('b', 'promoted', 0)],
            'exactly one canonical state row',
        ),
    ):
        cur = StubCursor(overrides=[(state_key, rows)])
        with pytest.raises(ValueError, match=match):
            dq.resolve_promoted_snapshot(cur)


# ---------------------------------------------------------------------------
# sync with the DAG gate and the scraper frames
# ---------------------------------------------------------------------------

def test_registry_target_predicate_mirrors_dag_gate():
    sql = dq.build_registry_target_sql('snap-1')
    start = sql.index('AND c.active = true')
    end = sql.index('AND e.active = true') + len('AND e.active = true')
    predicate = sql[start:end]
    dag_source = (
        Path(__file__).resolve().parents[3]
        / 'dags' / 'dag_transform_transfermarkt_silver.py'
    ).read_text(encoding='utf-8')
    # The eligible-target predicate must stay verbatim-identical to
    # _assert_complete_promoted_registry_target in the DAG.
    assert predicate in dag_source


def test_payload_columns_match_scraper_frames():
    from scrapers.transfermarkt import scraper as tm_scraper

    lineage = (
        set(tm_scraper._SCOPE_LINEAGE_COLUMNS)
        | set(tm_scraper._METADATA_COLUMNS)
    )
    native_frames = {
        MEMBERSHIPS: tm_scraper.SQUAD_MEMBERSHIP_COLUMNS,
        ATTR_OBS: tm_scraper.PLAYER_ATTRIBUTE_OBSERVATION_COLUMNS,
        CONTRACT_OBS: tm_scraper.PLAYER_CONTRACT_OBSERVATION_COLUMNS,
        MV_POINTS: tm_scraper.MARKET_VALUE_POINT_COLUMNS,
        TRANSFER_EVENTS: tm_scraper.TRANSFER_EVENT_COLUMNS,
        COACH_PROFILES: tm_scraper.COACH_PROFILE_COLUMNS,
        COACH_STINTS: tm_scraper.COACH_STINT_COLUMNS,
    }
    # observed_at is a deliberate exclusion: repeat fetches of one squad
    # page inside a batch are repetition, not two truths.
    deliberate_exclusions = {MEMBERSHIPS: {'observed_at'}}
    for table, frame in native_frames.items():
        keys = set(dq.NATIVE_BRONZE_KEYS[table])
        assert keys <= set(frame), table
        semantic = set(frame) - lineage - keys
        expected = semantic - deliberate_exclusions.get(table, set())
        assert set(dq.NATIVE_PAYLOAD_COLUMNS[table]) == expected, table

    legacy_frames = {
        'iceberg.bronze.transfermarkt_players': (
            tm_scraper.LEGACY_PLAYER_COLUMNS
        ),
        'iceberg.bronze.transfermarkt_market_value_history': (
            tm_scraper.LEGACY_MV_COLUMNS
        ),
        'iceberg.bronze.transfermarkt_transfers': (
            tm_scraper.LEGACY_TRANSFER_COLUMNS
        ),
        'iceberg.bronze.transfermarkt_coaches': (
            tm_scraper.LEGACY_COACH_COLUMNS
        ),
    }
    for table, frame in legacy_frames.items():
        keys = set(dq.LEGACY_BRONZE_KEYS[table])
        assert keys <= set(frame), table
        semantic = set(frame) - lineage - keys
        assert set(dq.LEGACY_PAYLOAD_COLUMNS[table]) == semantic, table
