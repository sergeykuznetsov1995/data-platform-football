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

    def test_unlisted_tournament_season_stays_single_year(
            self, reload_medallion):
        # Historical backfill (WC 2022 — only the 2026 edition is configured):
        # falling back to the club form would silently fetch the wrong-edition
        # page, the exact class the old literal was immune to.
        reload_medallion(env=None)
        from scrapers.utils.competition_format import is_single_year
        assert is_single_year('INT-World Cup', 2022) is True

    def test_broken_env_club_league_falls_back_false(self, reload_medallion):
        reload_medallion(env='/nonexistent-medallion-920')
        from scrapers.utils.competition_format import is_single_year
        assert is_single_year('ENG-Premier League', 2025) is False

    def test_broken_env_tournament_raises_not_club_fallback(
            self, reload_medallion):
        # A silent club fallback for an INT-* league IS the wrong-season /
        # wrong-partition incident class — must raise, never return False.
        reload_medallion(env='/nonexistent-medallion-920')
        from scrapers.utils.competition_format import is_single_year
        with pytest.raises(RuntimeError, match='INT-'):
            is_single_year('INT-World Cup', 2026)


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
class TestSeasonFormatConsumers:
    """The two format_season implementations must follow season_format from
    competitions.yaml, not a league-name literal (#920 Phase 3)."""

    def test_fbref_format_season_single_year(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.fbref.url_builder import format_season
        assert format_season(2026, 'INT-World Cup') == '2026'
        assert format_season(2024, 'ENG-Premier League') == '2024-2025'
        assert format_season(2024) == '2024-2025'   # no league -> club form

    def test_fotmob_format_season_single_year(self, reload_medallion):
        reload_medallion(env=None)
        from scrapers.fotmob.scraper import FotMobScraper
        s = FotMobScraper.__new__(FotMobScraper)   # no network in __init__ path
        assert s._format_season(2026, 'INT-World Cup') == '2026'
        assert s._format_season(2024, 'ENG-Premier League') == '2024/2025'
        assert s._format_season(2024) == '2024/2025'

    def test_onboarded_tournaments_get_single_year_form(self, reload_medallion):
        # The Phase-3 acceptance case: Euro 2028 must NOT get the club
        # '2028-2029' page (the exact silent wrong-edition failure that
        # required onboarding to be code, not config).
        reload_medallion(env=None)
        from scrapers.fbref.url_builder import format_season, get_schedule_url
        assert format_season(2028, 'INT-European Championship') == '2028'
        assert '/676/2028/' in get_schedule_url('INT-European Championship', 2028)
        from scrapers.utils.competition_format import (
            is_group_knockout, is_single_year, is_single_year_competition,
        )
        assert is_single_year_competition('INT-Africa Cup of Nations') is True
        # Copa América: season_format resolvable even while the season is
        # inert (no start/end until CONMEBOL announces).
        assert is_single_year('INT-Copa America', 2028) is True
        assert is_group_knockout('INT-European Championship') is True


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
