"""History ordering keeps the full adult registry and visible unknown debt."""

import pytest

from scrapers.fbref.history import campaign_rows, progress_by_year


def adult(cid):
    return {
        "competition_id": str(cid),
        "name": "National League",
        "gender": "male",
        "classification": "league",
    }


def season(cid, sid, **kwargs):
    return {
        "competition_id": str(cid),
        "season_id": sid,
        "present": True,
        "lifecycle_state": "present",
        "is_current": False,
        "canonical_url": f"https://fbref.com/en/{cid}/{sid}",
        **kwargs,
    }


def test_complete_112_registry_by_year_including_missing_editions_and_current_owned():
    adults = [adult(cid) for cid in range(1000, 1112)]
    excluded = [
        dict(adult(2001), gender="female"),
        dict(adult(2002), name="Under-21 Cup"),
    ]
    seasons = [
        season(1000, "2026-2027", is_current=True),
        season(1001, "2026"),
        season(1001, "2017-2018"),
        season(1000, "2016-2017"),
        season(2002, "2026"),
    ]
    rows = campaign_rows(adults + excluded, seasons)
    summary = progress_by_year(rows)
    assert set(summary) == {str(year) for year in range(2016, 2027)}
    assert all(sum(summary[str(year)].values()) == 112 for year in range(2017, 2027))
    assert summary["2026"] == {"current_owned": 1, "pending": 1, "missing": 110}
    assert [r["year"] for r in rows] == sorted([r["year"] for r in rows], reverse=True)
    assert rows[-1]["season_id"] == "2016-2017"


def test_superseded_and_missing_registry_bytes_never_become_fetch_candidates():
    rows = campaign_rows(
        [adult(1000)],
        [
            season(1000, "2024-2025", canonical_url="https://fbref.com/#superseded:x"),
            season(1000, "2025-2026", present=False),
        ],
    )
    assert all(
        row["state"] == "missing"
        for row in rows
        if not row["season_id"].startswith("missing:")
    )


def test_source_catalog_range_proves_unavailable_but_absence_alone_stays_missing():
    discontinued = dict(
        adult(1000),
        metadata={
            "first_season": "1930",
            "last_season": "2010-2011",
            "current_scope_lifecycle": "discontinued",
        },
    )
    rows = campaign_rows([discontinued, adult(1001)], [])
    assert all(
        row["state"]
        == ("unavailable" if row["competition_id"] == "1000" else "missing")
        for row in rows
    )


def test_periodic_catalog_proves_gap_years_without_inventing_annual_editions():
    competition = dict(
        adult(1000), metadata={"first_season": "1930", "last_season": "2026"}
    )
    editions = [
        season(1000, "2026", is_current=True),
        season(1000, "2022"),
        season(1000, "2018"),
    ]
    catalog = {
        "1000": {"snapshot_id": "catalog-proof", "editions": ["2026", "2022", "2018"]}
    }
    rows = campaign_rows([competition], editions, catalog)
    assert {r["year"] for r in rows if r["state"] in {"pending", "current_owned"}} == {
        2026,
        2022,
        2018,
    }
    assert all(
        r["state"] == "unavailable" for r in rows if r["year"] not in {2026, 2022, 2018}
    )


def test_available_catalog_edition_missing_from_registry_remains_a_barrier():
    competition = adult(1000)
    rows = campaign_rows(
        [competition],
        [season(1000, "2026"), season(1000, "2022")],
        {"1000": {"editions": ["2026", "2025", "2022"], "snapshot_id": "proof"}},
    )
    assert next(r for r in rows if r["year"] == 2025)["state"] == "missing"


@pytest.mark.parametrize("current", [False, True])
def test_proven_superseded_alias_does_not_create_duplicate_campaign_debt(current):
    editions = [
        season(1000, "2026", canonical_url="https://fbref.com/#superseded:2026"),
        season(1000, "2026-2027", is_current=current),
        season(1000, "2025-2026"),
    ]
    proof = {
        "1000": {
            "snapshot_id": "proof",
            "editions": ["2026-2027", "2025-2026"],
            "aliases": {"2026": "2026-2027"},
        }
    }
    rows = campaign_rows([adult(1000)], editions, proof)
    assert not any(row["season_id"] == "2026" for row in rows)
    assert next(row for row in rows if row["season_id"] == "2026-2027")["state"] == (
        "current_owned" if current else "pending"
    )
    unknown = campaign_rows([adult(1000)], editions)
    assert (
        next(row for row in unknown if row["season_id"] == "2026")["state"] == "missing"
    )
