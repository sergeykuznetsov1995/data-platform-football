"""Network-free ESPN soccer catalog discovery tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers.espn.discovery import (
    CatalogCandidate,
    CatalogSnapshot,
    DiscoveryChangeKind,
    diff_catalogs,
    discover_catalog,
    load_catalog_snapshot,
    parse_competition_detail,
    parse_soccer_dropdown,
    quarantine_new_editions,
    save_catalog_snapshot,
)
from scrapers.espn.models import AgeClass, CapabilityState, Gender
from scrapers.espn.registry import DEFAULT_REGISTRY_PATH, load_registry


FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "espn" / "catalog_2026-07-31.json"
RAW_DROPDOWN_FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "espn" / "dropdown_2026-07-31.json"
)


def _dropdown() -> dict:
    return {
        "groups": [
            {
                "name": "topCompetitions",
                "title": "Top Competitions",
                "columns": [
                    {
                        "teams": [
                            {
                                "n": "English Premier League",
                                "id": "700",
                                "lk": [
                                    {
                                        "t": "schedule",
                                        "u": "https://www.espn.com/soccer/schedule/_/league/eng.1",
                                    }
                                ],
                            },
                            {
                                "n": "FIFA World Cup",
                                "id": "606",
                                "lk": [
                                    {"t": "index", "u": "https://www.espn.com/soccer/worldcup/"},
                                    {
                                        "t": "schedule",
                                        "u": "https://www.espn.com/soccer/schedule/_/league/fifa.world",
                                    },
                                ],
                            },
                        ]
                    }
                ],
            },
            {
                "name": "Europe",
                "title": "Europe",
                "columns": [
                    {
                        "teams": [
                            {
                                "n": "English Premier League",
                                "id": "700",
                                "lk": [
                                    {
                                        "t": "index",
                                        "u": "https://www.espn.com/soccer/league/_/name/eng.1",
                                    }
                                ],
                            }
                        ]
                    }
                ],
            },
        ]
    }


def _detail(*, gender: str = "MALE", season_year: int = 2026) -> dict:
    return {
        "id": "700",
        "slug": "eng.1",
        "name": "English Premier League",
        "gender": gender,
        "genderEvidence": "competition detail metadata",
        "season": {
            "year": season_year,
            "displayName": f"{season_year}-{str(season_year + 1)[-2:]} English Premier League",
            "startDate": f"{season_year}-06-01T04:00Z",
            "endDate": f"{season_year + 1}-06-01T03:59Z",
        },
        "capabilities": {
            "schedule": "proven",
            "lineup": "partial",
            "matchsheet": "unknown",
        },
    }


@pytest.mark.unit
def test_dropdown_retains_every_occurrence_and_never_infers_gender_or_age() -> None:
    rows = parse_soccer_dropdown(_dropdown())

    assert len(rows) == 3
    assert [row.slug for row in rows] == ["eng.1", "fifa.world", "eng.1"]
    assert rows[0].gender is Gender.UNKNOWN
    assert rows[0].age_class is AgeClass.UNKNOWN


@pytest.mark.unit
def test_native_dropdown_endpoint_retains_all_direct_league_rows() -> None:
    payload = {
        "leagues": [
            {"id": "606", "slug": "fifa.world", "name": "FIFA World Cup"},
            {"slug": "fifa.wwcq.ply", "name": "Women's World Cup Qualifying Playoff"},
        ]
    }

    rows = parse_soccer_dropdown(payload)

    assert [(row.espn_id, row.slug) for row in rows] == [
        (606, "fifa.world"),
        (None, "fifa.wwcq.ply"),
    ]
    assert all(row.group == "ESPN soccer dropdown" for row in rows)
    assert all(row.gender is Gender.UNKNOWN for row in rows)


@pytest.mark.unit
def test_detail_metadata_is_only_source_of_gender_edition_and_capabilities() -> None:
    detail = parse_competition_detail(_detail())

    assert detail.gender is Gender.MALE
    assert detail.source_season_year == 2026
    assert detail.capabilities.schedule is CapabilityState.PROVEN
    assert detail.capabilities.lineup is CapabilityState.PARTIAL
    assert detail.capabilities.matchsheet is CapabilityState.UNKNOWN
    assert detail.gender_evidence == ("competition detail metadata",)


@pytest.mark.unit
def test_detail_without_numeric_id_remains_an_unpromotable_candidate() -> None:
    payload = _detail(gender="FEMALE")
    payload.pop("id")
    payload["slug"] = "fifa.wwcq.ply"
    payload["name"] = "FIFA Women's World Cup Qualifying - Playoff Tournament"

    detail = parse_competition_detail(payload)

    assert detail.espn_id is None
    assert detail.gender is Gender.FEMALE


@pytest.mark.unit
def test_discovery_enriches_every_row_without_promoting_or_guessing_age() -> None:
    before = DEFAULT_REGISTRY_PATH.read_bytes()
    snapshot = discover_catalog(
        _dropdown(),
        details_by_slug={"eng.1": _detail()},
        captured_at="2026-07-31T12:00:00Z",
    )

    assert len(snapshot.candidates) == 3
    assert snapshot.candidates[0].gender is Gender.MALE
    assert snapshot.candidates[0].age_class is AgeClass.UNKNOWN
    assert snapshot.candidates[1].gender is Gender.UNKNOWN
    assert DEFAULT_REGISTRY_PATH.read_bytes() == before


@pytest.mark.unit
def test_candidate_snapshot_persists_every_row_separately_from_registry(tmp_path) -> None:
    snapshot = discover_catalog(
        _dropdown(),
        details_by_slug={"eng.1": _detail()},
        captured_at="2026-07-31T12:00:00Z",
    )
    target = tmp_path / "candidate-snapshot.json"

    save_catalog_snapshot(snapshot, target)

    assert load_catalog_snapshot(target) == snapshot
    assert len(json.loads(target.read_text(encoding="utf-8"))["candidates"]) == 3


@pytest.mark.unit
def test_saved_catalog_fixture_retains_all_220_discovery_rows() -> None:
    raw_dropdown = json.loads(RAW_DROPDOWN_FIXTURE.read_text(encoding="utf-8"))
    parsed_dropdown = parse_soccer_dropdown(raw_dropdown)
    document = json.loads(FIXTURE.read_text(encoding="utf-8"))
    snapshot = CatalogSnapshot.from_dict(document)

    assert len(parsed_dropdown) == 220
    assert len({row.slug for row in parsed_dropdown}) == 220
    assert snapshot.captured_at.startswith("2026-07-31T")
    assert len(snapshot.candidates) == 220
    assert len({row.slug for row in snapshot.candidates}) == 220
    assert [row.slug for row in snapshot.candidates] == [
        row.slug for row in parsed_dropdown
    ]
    assert any(row.gender is Gender.FEMALE for row in snapshot.candidates)


@pytest.mark.unit
def test_every_female_fixture_candidate_is_rejected_by_manual_promotion() -> None:
    from scrapers.espn.registry import RegistryError, promote_candidate

    snapshot = CatalogSnapshot.from_dict(
        json.loads(FIXTURE.read_text(encoding="utf-8"))
    )
    female = [row for row in snapshot.candidates if row.gender is Gender.FEMALE]

    assert female
    for candidate in female:
        with pytest.raises(RegistryError, match="explicit MALE"):
            promote_candidate(
                {"schema_version": 1, "registry_version": "test", "as_of": "2026-07-31", "competitions": []},
                candidate,
                age_class=AgeClass.SENIOR,
                age_class_evidence=("manual",),
                legacy_league="TEST",
            )


@pytest.mark.unit
def test_catalog_diff_reports_added_removed_and_changed_fields() -> None:
    base = CatalogCandidate.from_dict(
        {
            "espn_id": 700,
            "slug": "eng.1",
            "name": "English Premier League",
            "group": "Europe",
            "source_order": 1,
            "gender": "MALE",
            "age_class": "UNKNOWN",
            "source_season_year": 2025,
            "edition_display_name": "2025-26 English Premier League",
            "start_date": "2025-06-01",
            "end_date": "2026-06-01",
            "capabilities": {"schedule": "proven", "lineup": "partial", "matchsheet": "unknown"},
            "gender_evidence": ["detail.gender=MALE"],
        }
    )
    changed = CatalogCandidate.from_dict(
        {
            **base.to_dict(),
            "slug": "eng.premier",
            "gender": "FEMALE",
            "source_season_year": 2026,
            "capabilities": {"schedule": "proven", "lineup": "proven", "matchsheet": "partial"},
        }
    )
    removed = CatalogCandidate.from_dict({**base.to_dict(), "espn_id": 606, "slug": "fifa.world"})
    added = CatalogCandidate.from_dict({**base.to_dict(), "espn_id": 775, "slug": "uefa.champions"})
    previous = CatalogSnapshot("2026-07-24T00:00:00Z", (base, removed))
    current = CatalogSnapshot("2026-07-31T00:00:00Z", (changed, added))

    diff = diff_catalogs(previous, current)

    kinds = {change.kind for change in diff.changes}
    assert DiscoveryChangeKind.ADDED in kinds
    assert DiscoveryChangeKind.REMOVED in kinds
    assert DiscoveryChangeKind.SLUG in kinds
    assert DiscoveryChangeKind.GENDER in kinds
    assert DiscoveryChangeKind.CURRENT_EDITION in kinds
    assert DiscoveryChangeKind.CAPABILITIES in kinds


@pytest.mark.unit
def test_catalog_diff_detects_edition_window_change_without_year_change() -> None:
    before = CatalogCandidate.from_dict(
        {
            "espn_id": 700,
            "slug": "eng.1",
            "name": "English Premier League",
            "group": "Europe",
            "source_order": 1,
            "gender": "MALE",
            "age_class": "UNKNOWN",
            "source_season_year": 2026,
            "edition_display_name": "2026-27 English Premier League",
            "start_date": "2026-06-01",
            "end_date": "2027-06-01",
            "capabilities": {"schedule": "proven", "lineup": "partial", "matchsheet": "unknown"},
        }
    )
    after = CatalogCandidate.from_dict({**before.to_dict(), "end_date": "2027-07-01"})

    diff = diff_catalogs(
        CatalogSnapshot("2026-07-24T00:00:00Z", (before,)),
        CatalogSnapshot("2026-07-31T00:00:00Z", (after,)),
    )

    assert [change.kind for change in diff.changes] == [
        DiscoveryChangeKind.CURRENT_EDITION
    ]


@pytest.mark.unit
def test_new_source_season_is_quarantined_until_registry_is_manually_updated() -> None:
    registry = load_registry(DEFAULT_REGISTRY_PATH)
    current = CatalogCandidate.from_dict(
        {
            "espn_id": 700,
            "slug": "eng.1",
            "name": "English Premier League",
            "group": "Europe",
            "source_order": 1,
            "gender": "MALE",
            "age_class": "UNKNOWN",
            "source_season_year": 2027,
            "edition_display_name": "2027-28 English Premier League",
            "start_date": "2027-06-01",
            "end_date": "2028-06-01",
            "capabilities": {"schedule": "proven", "lineup": "proven", "matchsheet": "proven"},
            "gender_evidence": ["detail.gender=MALE"],
        }
    )

    quarantined = quarantine_new_editions(CatalogSnapshot("2027-07-31T00:00:00Z", (current,)), registry)

    assert quarantined == {"700:2027"}
    assert registry.by_slug["eng.1"].current_edition.source_season_year == 2026
