"""#1474: explicit daily denominator of WhoScored."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from scrapers.whoscored.catalog import (
    DEFAULT_TOURNAMENT_OVERRIDES,
    classify_tournament,
)
from scrapers.whoscored.denominator import (
    CLASS_A_TOURNAMENT_IDS,
    EXCLUDED_TOURNAMENT_IDS,
    PROBE_SCOPE_SPECS,
    denominator_scopes,
    missing_probe_specs,
)
from scrapers.whoscored.domain import TournamentEligibility

TODAY = date(2026, 9, 26)


def _season(competition_id, season_id, *, start=None, end=None, active=False):
    return SimpleNamespace(
        scope=SimpleNamespace(
            competition_id=competition_id,
            season_id=season_id,
            spec=f"{competition_id}={season_id}",
        ),
        start=start,
        end=end,
        active=active,
    )


class _Catalog:
    def __init__(self, tournaments, seasons):
        self._tournaments = tournaments
        self._seasons = seasons

    def enabled_scopes(self):
        return tuple(self._seasons)

    def active_scopes(self, *, on=None):
        assert on == TODAY
        return tuple(season for season in self._seasons if season.active)

    def competition(self, competition_id):
        return SimpleNamespace(tournament_id=self._tournaments[competition_id])


def _specs(catalog):
    return [season.scope.spec for season in denominator_scopes(catalog, on=TODAY)]


def test_class_a_is_the_22_tournaments_of_review_c2():
    assert len(CLASS_A_TOURNAMENT_IDS) == 22
    assert CLASS_A_TOURNAMENT_IDS == {
        2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 17, 18, 20, 21, 22, 29, 30, 36, 77, 85,
        95, 721,
    }  # fmt: skip
    assert EXCLUDED_TOURNAMENT_IDS == {27, 57, 613, 635}
    assert not CLASS_A_TOURNAMENT_IDS & EXCLUDED_TOURNAMENT_IDS
    assert len(PROBE_SCOPE_SPECS) == 11


def test_current_and_just_finished_season_of_a_class_a_tournament():
    catalog = _Catalog(
        {"ENG-Premier League": 2},
        [
            _season("ENG-Premier League", "2425"),
            _season(
                "ENG-Premier League",
                "2526",
                start=date(2025, 8, 15),
                end=date(2026, 5, 24),
            ),
            _season(
                "ENG-Premier League",
                "2627",
                start=date(2026, 8, 21),
                end=date(2027, 5, 30),
                active=True,
            ),
        ],
    )

    assert _specs(catalog) == ["ENG-Premier League=2526", "ENG-Premier League=2627"]


def test_new_season_inherits_the_class_without_a_code_change():
    catalog = _Catalog(
        {"WS-233-85": 85},
        [
            _season("WS-233-85", "2025"),
            _season("WS-233-85", "2026", end=date(2026, 9, 1)),
            _season("WS-233-85", "2027", start=date(2026, 9, 20), active=True),
        ],
    )

    assert _specs(catalog) == ["WS-233-85=2026", "WS-233-85=2027"]


def test_between_seasons_only_the_latest_finished_season_is_kept():
    catalog = _Catalog(
        {"INT-World Cup": 36},
        [
            _season("INT-World Cup", "2022", end=date(2022, 12, 18)),
            _season(
                "INT-World Cup", "2026", start=date(2026, 6, 11), end=date(2026, 7, 19)
            ),
        ],
    )

    assert _specs(catalog) == ["INT-World Cup=2026"]


def test_not_yet_started_inactive_season_is_not_finished():
    catalog = _Catalog(
        {"WS-250-12": 12},
        [
            _season("WS-250-12", "2526", end=date(2026, 5, 30)),
            _season("WS-250-12", "2627", start=date(2026, 10, 1)),
        ],
    )

    assert _specs(catalog) == ["WS-250-12=2526"]


def test_outside_class_a_friendlies_and_unknown_tournaments_are_dropped():
    catalog = _Catalog(
        {"WS-247-57": 57, "WS-247-27": 27, "WS-9-999": 999},
        [
            _season("WS-247-57", "2026", active=True),
            _season("WS-247-27", "2026", active=True),
            _season("WS-9-999", "2627", active=True),
        ],
    )

    assert _specs(catalog) == []


def test_probe_scopes_join_the_daily_run_when_the_catalog_knows_them():
    catalog = _Catalog(
        {"WS-206-63": 63, "WS-265-105": 105},
        [
            _season("WS-206-63", "2526", end=date(2026, 6, 20)),
            _season("WS-206-63", "2627", active=True),
            _season("WS-265-105", "2026", active=True),
        ],
    )

    assert _specs(catalog) == ["WS-206-63=2526", "WS-265-105=2026"]
    assert set(missing_probe_specs(catalog)) == set(PROBE_SCOPE_SPECS) - {
        "WS-206-63=2526",
        "WS-265-105=2026",
    }


def test_toulon_is_excluded_as_youth_despite_male_source_sex():
    assert 203 in {item.tournament_id for item in DEFAULT_TOURNAMENT_OVERRIDES}

    decision = classify_tournament(
        tournament_id=203,
        tournament_name="Toulon Tournament",
        region_name="International",
        source_sex=1,
    )

    assert decision.eligibility is TournamentEligibility.EXCLUDED_YOUTH
    assert decision.reason.startswith("explicit_override:audited youth")
