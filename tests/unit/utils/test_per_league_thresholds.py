"""
Per-league DQ floors (#920 Phase 2) — equivalence and scaling tests.

The equivalence contract: PER_LEAGUE_FLOOR_BASES scales the SAME calibrated
constants the flat MIN_ROW_THRESHOLDS carried before #920 Phase 2, and for a
20-team club league (the APL calibration basis) the scaled floor must equal
the old constant EXACTLY — integer arithmetic, no tolerance. Anything else
would be a silent recalibration of prod gates, not a refactor.

Runs on the host: utils.config performs no YAML IO at import; the floor
helpers lazy-import medallion_config, so tests point MEDALLION_CONFIG_DIR at
the real shipped configs/medallion/.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)


@pytest.fixture
def real_config(monkeypatch):
    """Point the lazy medallion_config import at the shipped YAML."""
    monkeypatch.setenv(
        "MEDALLION_CONFIG_DIR", str(PROJECT_ROOT / "configs" / "medallion")
    )
    import importlib

    from utils import medallion_config

    importlib.reload(medallion_config)
    medallion_config.reset_cache()
    yield
    medallion_config.reset_cache()


# The flat per-league constants from dags/utils/config.py @ 080d308 (the last
# pre-per-league master). If a base in PER_LEAGUE_FLOOR_BASES drifts from
# these, the prod gates were recalibrated — do that consciously, not here.
_MASTER_LITERALS = {
    'whoscored_schedule': 340,
    'espn_schedule': 340,
    'understat_schedule': 340,
    'understat_team_match_stats': 340,
    'understat_shots': 8000,
    'understat_player_match_stats': 10_000,
    'understat_players': 450,
    'sofifa_players': 450,
    'sofifa_teams': 18,
    'sofifa_team_ratings': 18,
    'sofifa_leagues': 1,
}


@pytest.mark.unit
class TestEquivalenceWithMasterConstants:

    def test_bases_cover_exactly_the_master_league_aware_keys(self):
        from utils.config import PER_LEAGUE_FLOOR_BASES
        assert set(PER_LEAGUE_FLOOR_BASES) == set(_MASTER_LITERALS)

    @pytest.mark.parametrize("key, literal", sorted(_MASTER_LITERALS.items()))
    def test_20_team_club_league_reduces_to_master_constant(
            self, real_config, key, literal):
        from utils.config import get_min_row_threshold
        assert get_min_row_threshold(key, 'ENG-Premier League') == literal

    def test_min_row_thresholds_fallback_values_unchanged(self):
        # The whole-table fallback dict must stay byte-compatible with master
        # (same keys minus the 5 consumer-less legacy ones, same values) —
        # it is monkeypatched by existing tests and read by validate_table
        # for every call without a league scope.
        from utils.config import MIN_ROW_THRESHOLDS
        assert MIN_ROW_THRESHOLDS == {
            'whoscored_schedule': 2040,   # 340 * 6 WHOSCORED_LEAGUES
            'whoscored_events': 20_000_000,
            'whoscored_player_profile': 300,
            'espn_schedule': 340,
            'espn_lineup': 9000,
            'espn_matchsheet': 620,
            'understat_schedule': 340,
            'understat_players': 450,
            'understat_shots': 8000,
            'understat_team_match_stats': 340,
            'understat_player_match_stats': 10_000,
            'sofifa_players': 450,
            'sofifa_teams': 18,
            'sofifa_team_ratings': 18,
            'sofifa_versions': 15,
            'sofifa_leagues': 1,
            'sofifa_player_ratings': 450,
        }


@pytest.mark.unit
class TestTournamentAndShrunkLeagueScaling:

    def test_world_cup_schedule_floor_sane(self, real_config):
        # 48 teams, 104 scheduled matches: the floor must sit under the full
        # fixture list (schedule tables carry it from the first scrape) but
        # far above zero.
        from utils.config import get_min_row_threshold
        floor = get_min_row_threshold('whoscored_schedule', 'INT-World Cup')
        assert floor == 340 * 104 // 380 == 93
        assert 0 < floor <= 104
        assert get_min_row_threshold('espn_schedule', 'INT-World Cup') == 93

    def test_18_team_league_scaling(self, real_config):
        from utils.config import get_min_row_threshold
        assert get_min_row_threshold(
            'espn_schedule', 'GER-Bundesliga') == 340 * 306 // 380 == 273
        assert get_min_row_threshold(
            'understat_shots', 'GER-Bundesliga') == 8000 * 306 // 380 == 6442
        assert get_min_row_threshold('sofifa_teams', 'GER-Bundesliga') == 16
        # 'league' unit must NOT scale — 1 * 18 // 20 == 0 would silently
        # pass an empty sofifa_leagues partition.
        assert get_min_row_threshold('sofifa_leagues', 'GER-Bundesliga') == 1

    def test_every_floor_at_least_one_for_in_scope_leagues(self, real_config):
        from utils.config import PER_LEAGUE_FLOOR_BASES, get_min_row_threshold
        from utils.medallion_config import get_in_scope_competitions
        for league in get_in_scope_competitions():
            for key in PER_LEAGUE_FLOOR_BASES:
                assert get_min_row_threshold(key, league) >= 1, (key, league)


@pytest.mark.unit
class TestFailClosed:

    def test_unknown_threshold_key_raises_keyerror(self, real_config):
        from utils.config import get_min_row_threshold
        with pytest.raises(KeyError):
            get_min_row_threshold('nope_table', 'ENG-Premier League')

    def test_unknown_league_raises(self, real_config):
        from utils.config import get_min_row_threshold
        from utils.medallion_config import MedallionConfigError
        with pytest.raises(MedallionConfigError, match="not found"):
            get_min_row_threshold('espn_schedule', 'XX-Nope')

    def test_stub_competition_raises(self, real_config):
        # UEFA-Champions League is a catalog stub (seasons: []) — a floor of
        # 0 would silently pass an empty table, so it must raise instead.
        from utils.config import get_min_row_threshold
        from utils.medallion_config import MedallionConfigError
        with pytest.raises(MedallionConfigError, match="no seasons"):
            get_min_row_threshold('espn_schedule', 'UEFA-Champions League')

    def test_unknown_unit_raises(self):
        from utils.config import scale_floor_for_league
        with pytest.raises(ValueError, match="unknown floor unit"):
            scale_floor_for_league('percent', 10, 'ENG-Premier League')
