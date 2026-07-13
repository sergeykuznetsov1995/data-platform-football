from __future__ import annotations

from dataclasses import replace
import json
from datetime import datetime, timedelta, timezone

import pytest

from dags.utils import transfermarkt_scope_planner as planner
from scrapers.transfermarkt.registry import (
    AgeCategory,
    ClassificationEvidence,
    CompetitionRecord,
    CompetitionType,
    EditionRecord,
    EvidenceOrigin,
    Gender,
    SeasonFormat,
    TeamType,
    canonical_season,
    deterministic_scope_id,
)


NOW = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)


def _competition(
    competition_id: str,
    *,
    season_format: SeasonFormat = SeasonFormat.SPLIT_YEAR,
    status: str = 'eligible',
) -> CompetitionRecord:
    values = {
        'competition_type': CompetitionType.DOMESTIC_LEAGUE,
        'gender': Gender.MEN,
        'team_type': TeamType.CLUB,
        'age_category': AgeCategory.SENIOR,
        'season_format': season_format,
    }
    if status == 'unknown':
        values['gender'] = Gender.UNKNOWN
    evidence = ClassificationEvidence(
        source_field='structured_metadata',
        source_value='test fixture',
        source_url=f'https://example.test/{competition_id}',
        origin=EvidenceOrigin.STRUCTURED,
        competition_type=values['competition_type'],
        gender=(None if status == 'unknown' else values['gender']),
        team_type=values['team_type'],
        age_category=values['age_category'],
        season_format=values['season_format'],
    )
    return CompetitionRecord(
        competition_id=competition_id,
        slug=competition_id.lower(),
        name=f'Competition {competition_id}',
        country='Test',
        confederation='Test confederation',
        competition_type=values['competition_type'],
        gender=values['gender'],
        team_type=values['team_type'],
        age_category=values['age_category'],
        season_format=values['season_format'],
        active=True,
        source_url=f'https://example.test/{competition_id}',
        discovered_at=NOW,
        canonical_competition_id=f'CAN-{competition_id}',
        evidence=(evidence,),
        registry_snapshot_id='registry-1',
        source_body_hash=f'hash-{competition_id}',
        aliases=(f'league-{competition_id}',),
    )


def _edition(
    competition_id: str,
    edition_id: str,
    *,
    season_format: SeasonFormat = SeasonFormat.SPLIT_YEAR,
    current: bool = True,
) -> EditionRecord:
    return EditionRecord(
        competition_id=competition_id,
        edition_id=edition_id,
        edition_label=edition_id,
        canonical_season=canonical_season(edition_id, season_format),
        season_format=season_format,
        start_date=None,
        end_date=None,
        active=True,
        current=current,
        participant_count=20,
        participant_hash=f'participants-{competition_id}-{edition_id}',
        source_url=f'https://example.test/{competition_id}/{edition_id}',
        discovered_at=NOW,
        registry_snapshot_id='registry-1',
        source_body_hash=f'hash-{competition_id}-{edition_id}',
    )


def _joined_row(
    competition: CompetitionRecord,
    edition: EditionRecord,
    *,
    last_success_at: datetime | None = None,
    career_fetches_pending: int = 0,
) -> dict:
    return {
        'competition_id': competition.competition_id,
        'slug': competition.slug,
        'name': competition.name,
        'country': competition.country,
        'confederation': competition.confederation,
        'competition_type': competition.competition_type.value,
        'gender': competition.gender.value,
        'team_type': competition.team_type.value,
        'age_category': competition.age_category.value,
        'competition_season_format': competition.season_format.value,
        'competition_active': competition.active,
        'competition_source_url': competition.source_url,
        'competition_discovered_at': competition.discovered_at.isoformat(),
        'canonical_competition_id': competition.canonical_competition_id,
        'classification_status': competition.classification_status.value,
        'classification_evidence': json.dumps([
            item.as_dict() for item in competition.evidence
        ]),
        'competition_source_body_hash': competition.source_body_hash,
        'competition_parser_revision': competition.parser_revision,
        'competition_schema_revision': competition.schema_revision,
        'edition_id': edition.edition_id,
        'edition_label': edition.edition_label,
        'canonical_season': edition.canonical_season,
        'edition_season_format': edition.season_format.value,
        'start_date': edition.start_date,
        'end_date': edition.end_date,
        'edition_active': edition.active,
        'is_current': edition.current,
        'participant_count': edition.participant_count,
        'participant_hash': edition.participant_hash,
        'edition_source_url': edition.source_url,
        'edition_discovered_at': edition.discovered_at.isoformat(),
        'edition_source_body_hash': edition.source_body_hash,
        'edition_parser_revision': edition.parser_revision,
        'edition_schema_revision': edition.schema_revision,
        'registry_snapshot_id': edition.registry_snapshot_id,
        'last_success_at': (
            last_success_at.isoformat() if last_success_at else None
        ),
        'career_fetches_pending': career_fetches_pending,
    }


def test_legacy_leagues_uses_every_value_and_registry_season_semantics():
    epl = _competition('GB1')
    world_cup = _competition('FIWC', season_format=SeasonFormat.SINGLE_YEAR)
    plan = planner.plan_transfermarkt_scopes(
        {'leagues': ['GB1', 'FIWC'], 'season': 2026},
        parent_cycle_id='scheduled__2026-07-11',
        competitions=[epl, world_cup],
        editions=[
            _edition('GB1', '2026'),
            _edition('FIWC', '2026', season_format=SeasonFormat.SINGLE_YEAR),
        ],
        now=NOW,
    )
    assert [item['competition_id'] for item in plan.mapped_payloads] == [
        'GB1', 'FIWC',
    ]
    assert [item['canonical_season'] for item in plan.mapped_payloads] == [
        '2627', '2026',
    ]


def test_exact_scopes_take_precedence_and_duplicates_are_removed():
    epl = _competition('GB1')
    ucl = _competition('CL')
    plan = planner.plan_transfermarkt_scopes(
        {
            'scopes': [
                {'competition_id': 'CL', 'edition_id': '2025'},
                'GB1:2025',
                'CL:2025',
            ],
            'leagues': ['does-not-exist'],
            'season': 1999,
        },
        parent_cycle_id='manual__exact',
        competitions=[epl, ucl],
        editions=[_edition('GB1', '2025'), _edition('CL', '2025')],
        now=NOW,
    )
    assert [
        (item['competition_id'], item['edition_id'])
        for item in plan.mapped_payloads
    ] == [('CL', '2025'), ('GB1', '2025')]


def test_a_calendar_league_scope_names_the_source_edition_not_its_season():
    # The source offsets a calendar league's saison_id from the season it names:
    # edition '2024' is season 2025, and season 2024 is edition '2023'.
    league = _competition('2DVB', season_format=SeasonFormat.SINGLE_YEAR)
    editions = [
        replace(
            _edition('2DVB', edition_id, season_format=SeasonFormat.SINGLE_YEAR),
            edition_label=season,
            canonical_season=season,
        )
        for edition_id, season in (('2023', '2024'), ('2024', '2025'))
    ]
    plan = planner.plan_transfermarkt_scopes(
        {'scopes': ['2DVB:2024']},
        parent_cycle_id='manual__calendar',
        competitions=[league],
        editions=editions,
        now=NOW,
    )
    assert [
        (item['edition_id'], item['canonical_season'])
        for item in plan.mapped_payloads
    ] == [('2024', '2025')]


def test_unknown_active_classification_blocks_empty_and_explicit_plans():
    unknown = _competition('UNK', status='unknown')
    edition = _edition('UNK', '2025')
    with pytest.raises(
        planner.ScopePlanningError,
        match='active registry classifications block crawl: UNK',
    ):
        planner.plan_transfermarkt_scopes(
            {},
            parent_cycle_id='scheduled__blocked',
            competitions=[unknown],
            editions=[edition],
            now=NOW,
        )
    with pytest.raises(planner.ScopePlanningError, match='classification blocks'):
        planner.plan_transfermarkt_scopes(
            {'scopes': ['UNK:2025']},
            parent_cycle_id='manual__blocked',
            competitions=[unknown],
            editions=[edition],
            now=NOW,
        )


def test_empty_params_selects_due_oldest_first_and_caps_batch_at_eight():
    rows = []
    for index in range(10):
        competition = _competition(f'C{index:02d}')
        edition = _edition(competition.competition_id, '2025')
        last_success = None
        if index == 0:
            last_success = NOW - timedelta(days=30)
        elif index == 1:
            last_success = NOW - timedelta(days=10)
        rows.append(_joined_row(
            competition, edition, last_success_at=last_success,
        ))

    plan = planner.plan_transfermarkt_scopes(
        {},
        parent_cycle_id='scheduled__due',
        registry_rows=rows,
        now=NOW,
    )
    assert plan.total_selected_count == 10
    assert len(plan.mapped_payloads) == 8
    assert plan.continuation_required is True
    assert plan.remaining_count == 2
    # Never-served identities are ordered first and deterministically.
    assert [item['competition_id'] for item in plan.mapped_payloads[:3]] == [
        'C02', 'C03', 'C04',
    ]


def test_full_slot_target_is_registry_complete_not_limited_to_one_batch():
    rows = [
        _joined_row(
            _competition(f'C{index:02d}'),
            _edition(f'C{index:02d}', '2025'),
        )
        for index in range(12)
    ]
    targets = planner.eligible_registry_scopes(rows)
    assert len(targets) == 12
    assert targets[0].scope_id == deterministic_scope_id('C00', '2025')
    assert targets[-1].scope_id == deterministic_scope_id('C11', '2025')
    assert all(item.current for item in targets)


def test_historical_success_is_immutable_and_current_fresh_scope_is_not_due():
    historical_competition = _competition('HIST')
    current_competition = _competition('CURR')
    rows = [
        _joined_row(
            historical_competition,
            _edition('HIST', '2024', current=False),
            last_success_at=NOW - timedelta(days=100),
        ),
        _joined_row(
            current_competition,
            _edition('CURR', '2025'),
            last_success_at=NOW - timedelta(days=1),
        ),
    ]
    plan = planner.plan_transfermarkt_scopes(
        {}, parent_cycle_id='scheduled__nothing-due', registry_rows=rows, now=NOW,
    )
    assert plan.total_selected_count == 0
    assert plan.mapped_payloads == ()


def test_completed_ops_scope_is_not_replanned_due_to_status_drift():
    from dags.scripts import run_transfermarkt_scope_cycle as writer
    from dags.utils import transfermarkt_scope_state as scope_state

    assert planner.SCOPE_COMPLETION_STATUS == writer.SCOPE_COMPLETION_STATUS
    assert planner.SCOPE_COMPLETION_STATUS == scope_state.SCOPE_COMPLETION_STATUS
    query = planner.build_promoted_registry_query()
    assert (
        f"WHERE status = '{scope_state.SCOPE_COMPLETION_STATUS}'" in query
    )
    # ``committed_at`` is a naive timestamp(6) written in UTC, and the planner
    # refuses a timestamp that states no zone — so the read must state it.  The
    # first scope ever completed is what turns this from theory into a failure:
    # until then every last_success_at is NULL.
    assert "with_timezone(committed_at, 'UTC')" in query
    # A scope that still owes career fetches is not finished with, whatever its
    # manifest was called — so the planner must read the debt, not just the date.
    assert "career_fetches_pending" in query

    competition = _competition('DONE')
    plan = planner.plan_transfermarkt_scopes(
        {},
        parent_cycle_id='scheduled__completed-is-not-due',
        registry_rows=[_joined_row(
            competition,
            _edition('DONE', '2024', current=False),
            last_success_at=NOW - timedelta(days=365),
        )],
        now=NOW,
    )
    assert plan.mapped_payloads == ()


def test_payloads_are_json_serializable_content_addressed_and_share_one_ledger():
    competitions = [_competition('GB1'), _competition('CL')]
    editions = [_edition('GB1', '2025'), _edition('CL', '2025')]
    kwargs = {
        'params': {'scopes': ['GB1:2025', 'CL:2025']},
        'parent_cycle_id': 'manual__same-parent',
        'competitions': competitions,
        'editions': editions,
        'now': NOW,
    }
    first = planner.plan_transfermarkt_scopes(**kwargs)
    second = planner.plan_transfermarkt_scopes(**kwargs)
    assert first == second
    json.dumps(first.as_dict())
    ledgers = {item['parent_ledger']['path'] for item in first.mapped_payloads}
    assert ledgers == {first.parent_ledger.path}
    assert first.parent_ledger.path.startswith(planner.RESULT_ROOT + '/cycles/')
    result_dirs = {
        item['result_paths']['base_dir'] for item in first.mapped_payloads
    }
    assert len(result_dirs) == 2
    assert all(path.startswith(planner.RESULT_ROOT) for path in result_dirs)
    assert len({item['child_cycle_id'] for item in first.mapped_payloads}) == 2


def test_promoted_registry_query_is_read_only_and_escapes_snapshot_literal():
    sql = planner.build_promoted_registry_query(
        registry_snapshot_id="registry'one",
    )
    assert planner.REGISTRY_STATE_TABLE in sql
    assert planner.SCOPE_MANIFEST_TABLE in sql
    assert planner.COMPETITIONS_TABLE in sql
    assert planner.EDITIONS_TABLE in sql
    assert "registry''one" in sql
    assert "status = 'promoted'" in sql
    assert 'unknown_active_count = 0' in sql
    assert 'SELECT' in sql
    assert 'INSERT ' not in sql
    assert 'UPDATE ' not in sql
    assert 'DELETE ' not in sql


@pytest.mark.parametrize('batch_size', [0, 9, True])
def test_batch_size_cannot_bypass_global_bound(batch_size):
    with pytest.raises(planner.ScopePlanningError, match='between 1 and 8'):
        planner.plan_transfermarkt_scopes(
            {'scopes': ['GB1:2025']},
            parent_cycle_id='manual__invalid-batch',
            competitions=[_competition('GB1')],
            editions=[_edition('GB1', '2025')],
            max_batch_size=batch_size,
            now=NOW,
        )


def test_a_scope_that_still_owes_careers_is_asked_for_again():
    # One cycle buys at most a window of a roster's careers. A historical
    # edition is never re-planned once it has a complete manifest, so the
    # careers it still owed would never be bought at all — the league would sit
    # in the slot with a hundred of its players' histories and no one the wiser.
    competition = _competition('DONE')
    edition = _edition('DONE', '2024', current=False)

    settled = planner.plan_transfermarkt_scopes(
        {},
        parent_cycle_id='scheduled__settled',
        registry_rows=[_joined_row(
            competition, edition,
            last_success_at=NOW - timedelta(days=1),
            career_fetches_pending=0,
        )],
        now=NOW,
    )
    assert settled.mapped_payloads == ()

    owing = planner.plan_transfermarkt_scopes(
        {},
        parent_cycle_id='scheduled__still-owing',
        registry_rows=[_joined_row(
            competition, edition,
            last_success_at=NOW - timedelta(days=1),
            career_fetches_pending=2099,
        )],
        now=NOW,
    )
    assert [
        (item['competition_id'], item['edition_id'])
        for item in owing.mapped_payloads
    ] == [('DONE', '2024')]
