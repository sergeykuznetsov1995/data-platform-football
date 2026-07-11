"""Contract tests for the versioned SofaScore discovery registry."""

from __future__ import annotations

from copy import deepcopy

import pytest

from scrapers.sofascore.catalog import CatalogError, SofaScoreCatalog


def _season(
    season_id: int = 76986,
    *,
    year: str = "25/26",
    season_format: str = "split_year",
    canonical_season: str | None = "2526",
) -> dict:
    return {
        "season_id": season_id,
        "name": f"Season {year}",
        "year": year,
        "season_format": season_format,
        "canonical_season": canonical_season,
    }


def _tournament(
    source_id: int = 17,
    *,
    canonical_id: str | None = "ENG-Premier League",
    enabled: bool = True,
    seasons: list[dict] | None = None,
) -> dict:
    return {
        "unique_tournament_id": source_id,
        "name": "Premier League",
        "slug": "premier-league",
        "category": {"id": 1, "name": "England", "slug": "england"},
        "sport_slug": "football",
        "page_path": "football/england/premier-league",
        "canonical_id": canonical_id,
        "enabled": enabled,
        "seasons": [_season()] if seasons is None else seasons,
    }


def _document(*tournaments: dict) -> dict:
    return {"schema_version": 1, "tournaments": list(tournaments)}


@pytest.mark.unit
def test_shipped_registry_preserves_current_activation_and_legacy_views():
    catalog = SofaScoreCatalog.load()

    assert catalog.enabled_competition_ids() == (
        "ENG-Premier League",
        "INT-World Cup",
    )
    assert catalog.tournament_map()["ENG-Premier League"] == 17
    assert catalog.slug_map()["INT-World Cup"] == (
        "football/world/world-championship"
    )
    assert catalog.resolve_season_id(17, "25/26") == 76986
    assert catalog.resolve_season_id(17, "2526") == 76986
    assert catalog.resolve_season_id(16, "2026") == 58210


@pytest.mark.unit
def test_disabled_tournament_is_resolvable_but_not_active():
    catalog = SofaScoreCatalog.from_mapping(
        _document(_tournament(enabled=False))
    )

    assert catalog.enabled_competition_ids() == ()
    assert catalog.tournament_map() == {"ENG-Premier League": 17}
    assert catalog.tournament_map(enabled_only=True) == {}


@pytest.mark.unit
def test_unknown_season_is_retained_but_not_activatable():
    catalog = SofaScoreCatalog.from_mapping(_document(_tournament(
        seasons=[_season(
            year="Apertura 2026",
            season_format="unknown",
            canonical_season=None,
        )],
    )))

    season = catalog.resolve_source_season(17, "Apertura 2026")
    assert season is not None
    assert season.activatable is False
    assert season.canonical_season is None


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate, expected",
    [
        (lambda doc: doc["tournaments"].append(
            deepcopy(doc["tournaments"][0])
        ), "duplicate unique_tournament_id"),
        (lambda doc: doc["tournaments"].append({
            **deepcopy(doc["tournaments"][0]),
            "unique_tournament_id": 99,
        }), "duplicate canonical_id"),
        (lambda doc: doc["tournaments"][0]["seasons"].append(
            deepcopy(doc["tournaments"][0]["seasons"][0])
        ), "duplicate season_id"),
        (lambda doc: doc["tournaments"][0]["seasons"].append({
            **deepcopy(doc["tournaments"][0]["seasons"][0]),
            "season_id": 99999,
            "year": "2025/2026",
        }), "ambiguous canonical_season"),
        (lambda doc: doc["tournaments"][0].update({
            "canonical_id": None,
            "enabled": True,
        }), "enabled without canonical_id"),
    ],
)
def test_registry_conflicts_fail_closed(mutate, expected):
    document = _document(_tournament())
    mutate(document)

    with pytest.raises(CatalogError, match=expected):
        SofaScoreCatalog.from_mapping(document)


@pytest.mark.unit
def test_page_path_must_be_derived_from_source_slugs():
    tournament = _tournament()
    tournament["page_path"] = "football/spain/laliga"

    with pytest.raises(CatalogError, match="page_path must equal"):
        SofaScoreCatalog.from_mapping(_document(tournament))
