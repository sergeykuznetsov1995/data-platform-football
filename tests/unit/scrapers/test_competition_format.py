"""
Shared competition-format helpers for scrapers (#920 Phase 3).

The scrapers' season-format branches used to hardcode ``league ==
'INT-World Cup'`` — onboarding the next tournament silently fetched the
club-formula page (the wrong-season class of the 2026-07-09 WC bronze
incident). These tests pin the shared helpers AND the repo-relative
CONFIG_DIR fallback: on a host without MEDALLION_CONFIG_DIR the lookups must
resolve against the shipped configs/medallion/, not silently return the club
default.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def reload_medallion(monkeypatch):
    """Reload dags.utils.medallion_config under a controlled env.

    The scraper helpers import the ``dags.utils.medallion_config`` module
    object (namespace-package path), whose CONFIG_DIR is evaluated at import
    time — reload to re-resolve it. Teardown restores a clean default state
    so later tests in the session never see a poisoned CONFIG_DIR.
    """
    import dags.utils.medallion_config as mc

    def _reload(env=None):
        if env is None:
            monkeypatch.delenv('MEDALLION_CONFIG_DIR', raising=False)
        else:
            monkeypatch.setenv('MEDALLION_CONFIG_DIR', env)
        importlib.reload(mc)
        mc.reset_cache()
        return mc

    yield _reload
    monkeypatch.undo()
    importlib.reload(mc)
    mc.reset_cache()


@pytest.mark.unit
class TestConfigDirFallback:
    def test_no_env_resolves_repo_relative(self, reload_medallion):
        mc = reload_medallion(env=None)
        # Host has no /opt/airflow — must land on <repo>/configs/medallion.
        assert mc.CONFIG_DIR.name == 'medallion'
        assert (mc.CONFIG_DIR / 'competitions.yaml').is_file()

    def test_env_still_wins(self, reload_medallion, tmp_path):
        mc = reload_medallion(env=str(tmp_path))
        assert str(mc.CONFIG_DIR) == str(tmp_path)


@pytest.mark.unit
class TestIsSingleYear:
    def test_wc_true_without_env(self, reload_medallion):
        # The load-bearing change: before the repo-relative fallback this
        # returned False on every host run (except-branch club default).
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_single_year
        assert is_single_year('INT-World Cup', 2026) is True

    def test_club_league_false(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_single_year
        assert is_single_year('ENG-Premier League', 2025) is False

    def test_unknown_league_false(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_single_year
        assert is_single_year('XX-Nope', 2026) is False

    def test_none_league_false(self):
        from scrapers.utils.competition_format import is_single_year
        assert is_single_year(None, 2026) is False

    def test_broken_env_falls_back_false_without_raise(self, reload_medallion):
        reload_medallion(env='/nonexistent-medallion-920')
        from scrapers.utils.competition_format import is_single_year
        assert is_single_year('INT-World Cup', 2026) is False


@pytest.mark.unit
class TestIsSingleYearCompetition:
    def test_wc_true(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_single_year_competition
        assert is_single_year_competition('INT-World Cup') is True

    def test_club_false(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_single_year_competition
        assert is_single_year_competition('ENG-Premier League') is False


@pytest.mark.unit
class TestIsGroupKnockout:
    def test_wc_true(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_group_knockout
        assert is_group_knockout('INT-World Cup') is True

    def test_club_false(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_group_knockout
        assert is_group_knockout('ENG-Premier League') is False

    def test_unknown_false(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_group_knockout
        assert is_group_knockout('XX-Nope') is False
