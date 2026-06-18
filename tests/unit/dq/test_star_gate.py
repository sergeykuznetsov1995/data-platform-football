"""
Unit tests for ``utils.gold_tasks.build_star_gate_checks`` (issue #432).

The final star-schema DQ gate: design-grain PK uniqueness, league/season FK
for every star fact, missing fct->dim soft-FKs with orphan-rate thresholds,
and grain sanity. Pure attribute inspection — no Trino, the builder is called
directly (pattern: tests/unit/dq/test_e3_dq.py).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


def _build():
    from utils.gold_tasks import build_star_gate_checks
    return build_star_gate_checks()


def _by_name(checks, name):
    matches = [c for c in checks if c.name == name]
    assert len(matches) == 1, f"expected exactly one check named {name!r}, got {len(matches)}"
    return matches[0]


@pytest.mark.unit
class TestStarGateComposition:
    def test_total_check_count(self):
        """Contract: 7 star_pk + 28 league/season FK (14 facts × 2) +
        20 dim-FK + 2 NULL-coverage + 3 grain = 60.

        #431 added fct_team_elo pointwise (PK + team_id->dim_team FK). #430
        added the three player-money facts: fct_player_salary joined
        _STAR_FACT_TABLES (replacing fct_player_market_value, which dropped
        league/season), salary / market_value / fifa_rating each gained a
        pointwise star_pk (+3), and salary / fifa_rating each gained a
        dim_player FK (+2). Pointwise facts gain no league/season FK.
        """
        assert len(_build()) == 60

    def test_names_unique(self):
        checks = _build()
        names = [c.name for c in checks]
        assert len(names) == len(set(names))

    def test_names_disjoint_from_e1_5_builder(self):
        """The gate is appended to the same registry as the E1.5 post-cutover
        checks — name collisions would make the Telegram summary ambiguous."""
        from utils.xref_dq import build_e1_5_post_cutover_checks
        gate = {c.name for c in _build()}
        e1_5 = {c.name for c in build_e1_5_post_cutover_checks()}
        assert gate.isdisjoint(e1_5)

    def test_where_and_condition_guard_safe(self):
        """No where/condition param may carry the sequences the SQL-injection
        guard rejects — otherwise the check fails closed at runtime."""
        for chk in _build():
            for key in ('where', 'condition'):
                val = chk.params.get(key)
                if val:
                    assert ';' not in val, chk.name
                    assert '--' not in val, chk.name
                    assert '/*' not in val, chk.name


@pytest.mark.unit
class TestStarGatePk:
    def test_fct_shot_design_pk(self):
        chk = _by_name(_build(), 'star_pk[fct_shot(match_id,shot_id)]')
        assert chk.kind == 'no_duplicates'
        assert chk.params['pk'] == ['match_id', 'shot_id']
        assert chk.severity == 'ERROR'

    def test_fct_match_odds_design_pk(self):
        chk = _by_name(
            _build(),
            'star_pk[fct_match_odds(match_id,bookmaker,market,is_closing)]',
        )
        assert chk.params['pk'] == ['match_id', 'bookmaker', 'market', 'is_closing']
        assert chk.severity == 'ERROR'

    def test_fct_lineup_design_pk_scoped_to_resolved(self):
        """Design PK (match_id, player_id) — but only over resolved rows:
        ESPN rows carry NULL player_id and would collapse into false dups."""
        chk = _by_name(_build(), 'star_pk[fct_lineup(match_id,player_id) resolved]')
        assert chk.params['pk'] == ['match_id', 'player_id']
        assert chk.params['where'] == 'player_id IS NOT NULL'

    def test_fct_team_elo_design_pk(self):
        """issue #431: one row per team per date (team_id, elo_date)."""
        chk = _by_name(_build(), 'star_pk[fct_team_elo(team_id,elo_date)]')
        assert chk.kind == 'no_duplicates'
        assert chk.params['pk'] == ['team_id', 'elo_date']
        assert chk.severity == 'ERROR'

    def test_player_money_design_pks(self):
        """issue #430: design-grain PK for the three player-money facts."""
        cases = {
            'star_pk[fct_player_salary(player_id,league,season)]':
                ['player_id', 'league', 'season'],
            'star_pk[fct_player_market_value(player_id_canonical,valuation_date,source)]':
                ['player_id_canonical', 'valuation_date', 'source'],
            'star_pk[fct_player_fifa_rating(player_id,fifa_edition)]':
                ['player_id', 'fifa_edition'],
        }
        for name, pk in cases.items():
            chk = _by_name(_build(), name)
            assert chk.kind == 'no_duplicates'
            assert chk.params['pk'] == pk


@pytest.mark.unit
class TestStarGateLeagueSeasonFk:
    def test_every_star_fact_has_league_and_season_fk(self):
        from utils.gold_tasks import _STAR_FACT_TABLES
        checks = _build()
        for table in _STAR_FACT_TABLES:
            for key, parent in (('league', 'gold.dim_competition'),
                                ('season', 'gold.dim_season')):
                found = [
                    c for c in checks
                    if c.kind == 'ref_integrity'
                    and c.params['child'] == table
                    and c.params['key'] == key
                    and c.params['parent'] == parent
                ]
                assert len(found) == 1, f"missing {table}.{key} -> {parent}"
                assert found[0].severity == 'ERROR'

    def test_star_fact_list_is_14_facts(self):
        from utils.gold_tasks import _STAR_FACT_TABLES
        assert len(_STAR_FACT_TABLES) == 14
        assert len(set(_STAR_FACT_TABLES)) == 14
        assert all(t.startswith('gold.fct_') for t in _STAR_FACT_TABLES)


@pytest.mark.unit
class TestStarGateDimFk:
    def test_rate_checks_carry_valid_thresholds(self):
        """Every WARNING-severity ref_integrity in the gate must use rate
        mode (that is the point of #432) with warn_rate <= error_rate."""
        for chk in _build():
            if chk.kind != 'ref_integrity' or chk.severity != 'WARNING':
                continue
            warn = chk.params['warn_rate']
            err = chk.params['error_rate']
            assert warn is not None, f"{chk.name}: WARNING FK without warn_rate"
            if err is not None:
                assert warn <= err, chk.name

    def test_error_fks_have_no_rate(self):
        """ERROR-severity FKs are zero-tolerance by construction — they must
        use the legacy first-orphan-fails mode."""
        for chk in _build():
            if chk.kind == 'ref_integrity' and chk.severity == 'ERROR':
                assert chk.params['warn_rate'] is None, chk.name

    def test_team_match_team_fks_are_error(self):
        checks = [
            c for c in _build()
            if c.kind == 'ref_integrity'
            and c.params['child'] == 'gold.fct_team_match'
            and c.params['parent'] == 'gold.dim_team'
        ]
        assert {c.params['key'] for c in checks} == {'team_id', 'opponent_id'}
        assert all(c.severity == 'ERROR' for c in checks)

    def test_fct_team_elo_team_fk(self):
        """issue #431: fct_team_elo.team_id -> dim_team, WARNING rate-mode
        (ClubElo names absent from team_aliases.yaml fall back to 'ce_' orphans
        kept by design)."""
        checks = [
            c for c in _build()
            if c.kind == 'ref_integrity'
            and c.params['child'] == 'gold.fct_team_elo'
            and c.params['parent'] == 'gold.dim_team'
        ]
        assert len(checks) == 1
        chk = checks[0]
        assert chk.params['key'] == 'team_id'
        assert chk.severity == 'WARNING'
        assert chk.params['warn_rate'] <= chk.params['error_rate']

    def test_player_money_dim_player_fks(self):
        """issue #430: salary / fifa_rating keep orphan ids ('cap_' / 'sf_'),
        so the dim_player FK is WARNING rate-mode, not zero-tolerance ERROR."""
        for child in ('gold.fct_player_salary', 'gold.fct_player_fifa_rating'):
            checks = [
                c for c in _build()
                if c.kind == 'ref_integrity'
                and c.params['child'] == child
                and c.params['parent'] == 'gold.dim_player'
            ]
            assert len(checks) == 1, child
            chk = checks[0]
            assert chk.params['key'] == 'player_id'
            assert chk.severity == 'WARNING'
            assert chk.params['warn_rate'] <= chk.params['error_rate']

    def test_player_facts_point_at_dim_player(self):
        """The design FK is player -> dim_player (NOT dim_player_attributes,
        which keeps its separate pre-existing checks in the main registry)."""
        expected_children = {
            'gold.fct_shot', 'gold.fct_lineup', 'gold.fct_player_match',
            'gold.fct_player_unavailable', 'gold.fct_match_timeline',
            'gold.fct_player_season_stats', 'gold.fct_keeper_season_stats',
            'gold.fct_player_market_value',
            'gold.fct_player_salary', 'gold.fct_player_fifa_rating',
        }
        children = {
            c.params['child'] for c in _build()
            if c.kind == 'ref_integrity' and c.params['parent'] == 'gold.dim_player'
        }
        assert children == expected_children

    def test_null_share_coverage_checks(self):
        checks = [c for c in _build() if c.kind == 'coverage']
        by_table = {c.params['table']: c for c in checks}
        assert set(by_table) == {'gold.fct_shot', 'gold.fct_lineup'}
        # fct_lineup thresholds sit under the live 64.9% non-NULL baseline —
        # anything tighter would fire permanently on healthy data.
        lineup = by_table['gold.fct_lineup']
        assert lineup.params['warn_threshold'] <= 0.62


@pytest.mark.unit
class TestStarGateGrain:
    def test_team_match_exactly_two_rows_per_match(self):
        chk = _by_name(_build(), 'star_grain[fct_team_match=2rows/match]')
        assert chk.kind == 'row_count'
        assert chk.params['max_rows'] == 0
        assert 'HAVING COUNT(*) <> 2' in chk.params['where']
        assert chk.severity == 'ERROR'

    def test_seasonal_team_counts(self):
        for name, table in (
            ('star_grain[fct_standings~20teams/league-season]',
             'gold.fct_standings'),
            ('star_grain[fct_team_season_stats~20teams/league-season]',
             'gold.fct_team_season_stats'),
        ):
            chk = _by_name(_build(), name)
            assert chk.kind == 'row_count'
            assert chk.params['table'] == table
            assert chk.params['max_rows'] == 0
            assert 'NOT BETWEEN 18 AND 24' in chk.params['where']
            assert chk.severity == 'WARNING'
