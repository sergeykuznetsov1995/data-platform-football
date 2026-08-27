from __future__ import annotations

import hashlib
import json

import pytest
import yaml

from scrapers.sofascore.all_mens_campaign import (
    CampaignScopeError,
    load_exact_scope,
    render_scope_overlays,
)
from scrapers.sofascore.catalog import SofaScoreCatalog
from scrapers.sofascore.workload_plan import WorkloadPlanError, team_count_band
from utils.medallion_config import _validate_competitions_schema


def _snapshot(
    *,
    team_count=20,
    gender="male",
    metadata_status="ready",
    season_metadata_status=None,
    kind="league",
):
    document = {
        "schema_version": 1,
        "candidate_count": 1,
        "policy_id": "unit-policy",
        "campaign_id": "unit-campaign",
        "tournaments": [{
            "unique_tournament_id": 17,
            "capture_key": "SS-17",
            "name": "Premier League",
            "slug": "premier-league",
            "page_path": "football/england/premier-league",
            "category": {"id": 1, "name": "England", "slug": "england"},
            "kind": kind,
            "metadata_status": metadata_status,
            "classification": {
                "sport": "football",
                "gender": gender,
                "age_group": "unknown",
                "team_level": "unknown",
                "status": "review_required",
                "exclusion_reasons": [],
                "evidence": [{
                    "type": "source_field",
                    "endpoint": "/unique-tournament/17",
                    "field": "gender",
                    "value": "M",
                }],
            },
            "eligibility_review": {
                "status": "approved",
                "confirmed": {
                    "age_group": "adult",
                    "team_level": "first_team",
                    "professional": True,
                },
                "reviewed_by": "unit-owner",
                "reviewed_at": "2026-08-21",
            },
            "seasons": [{
                "source_season_id": 76986,
                "source_name": "Premier League 25/26",
                "canonical_season": "2526",
                "season_format": "split_year",
                "start_year": 2025,
                "team_count": team_count,
                "metadata_status": season_metadata_status or metadata_status,
                "team_count_evidence": {
                    "type": "source_team_ids",
                    "endpoint": "/unique-tournament/17/season/76986/teams",
                    "count": team_count,
                    "team_ids_sha256": "a" * 64,
                } if team_count is not None else None,
            }],
        }],
    }
    raw = json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    document["snapshot_id"] = hashlib.sha256(raw).hexdigest()
    return document


@pytest.mark.unit
def test_exact_scope_is_bound_to_one_pinned_ready_tournament_season(tmp_path):
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")

    scope = load_exact_scope(snapshot, tournament_id=17, source_season_id=76986)

    assert scope["capture_key"] == "SS-17"
    assert scope["campaign_id"] == "unit-campaign"
    assert scope["canonical_season"] == "2526"
    assert scope["team_count"] == 20
    assert len(scope["scope_digest"]) == 64


@pytest.mark.unit
@pytest.mark.parametrize(
    "change, expected",
    [
        ({"snapshot_id": "0" * 64}, "snapshot digest"),
        ({"gender": "female"}, "confirmed male"),
        ({"team_count": None}, "team_count"),
        ({"metadata_status": "pending"}, "metadata_status"),
    ],
)
def test_exact_scope_fails_closed_before_paid_capture(tmp_path, change, expected):
    document = _snapshot()
    if "snapshot_id" in change:
        document["snapshot_id"] = change["snapshot_id"]
    else:
        tournament = document["tournaments"][0]
        if "gender" in change:
            tournament["classification"]["gender"] = change["gender"]
        if "team_count" in change:
            tournament["seasons"][0]["team_count"] = change["team_count"]
        if "metadata_status" in change:
            tournament["metadata_status"] = change["metadata_status"]
        unsigned = dict(document)
        unsigned.pop("snapshot_id")
        document["snapshot_id"] = hashlib.sha256(json.dumps(
            unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(CampaignScopeError, match=expected):
        load_exact_scope(snapshot, tournament_id=17, source_season_id=76986)


@pytest.mark.unit
def test_scope_overlays_are_single_tournament_and_catalog_valid(tmp_path):
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")
    scope = load_exact_scope(snapshot, tournament_id=17, source_season_id=76986)

    paths = render_scope_overlays(scope, tmp_path / "scope")

    catalog = SofaScoreCatalog.load(paths.registry_path)
    assert [item.canonical_id for item in catalog.tournaments] == ["SS-17"]
    assert catalog.tournaments[0].capture_allowed is True
    competitions = yaml.safe_load(paths.competitions_path.read_text())
    assert [item["id"] for item in competitions["competitions"]] == ["SS-17"]
    season = competitions["competitions"][0]["seasons"][0]
    assert season["id"] == 2526
    assert season["team_count"] == 20


def _pending_snapshot(tmp_path, **kwargs):
    document = _snapshot(
        team_count=None, season_metadata_status="pending", **kwargs
    )
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps(document), encoding="utf-8")
    return snapshot


@pytest.mark.unit
def test_pending_season_is_refused_unless_the_refresh_lane_allows_it(tmp_path):
    # The history campaign only ever pays for a season whose team pages were
    # fetched; the refresh lane (F2) plans matches from Bronze evidence and is
    # the only caller allowed to take a pending season.
    snapshot = _pending_snapshot(tmp_path)

    with pytest.raises(CampaignScopeError, match="metadata_status must be ready"):
        load_exact_scope(snapshot, tournament_id=17, source_season_id=76986)

    scope = load_exact_scope(
        snapshot,
        tournament_id=17,
        source_season_id=76986,
        allow_pending_season=True,
    )

    assert scope["capture_key"] == "SS-17"
    assert scope["canonical_season"] == "2526"
    assert scope["team_count"] is None
    assert scope["team_count_evidence"] is None
    assert len(scope["scope_digest"]) == 64


@pytest.mark.unit
def test_excluded_season_stays_refused_even_for_the_refresh_lane(tmp_path):
    document = _snapshot(team_count=None, season_metadata_status="excluded")
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(CampaignScopeError, match="metadata_status must be ready"):
        load_exact_scope(
            snapshot,
            tournament_id=17,
            source_season_id=76986,
            allow_pending_season=True,
        )


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["league", "cup"])
def test_pending_season_overlays_carry_no_team_count_and_fail_closed_on_pages(
    tmp_path, kind
):
    snapshot = _pending_snapshot(tmp_path, kind=kind)
    scope = load_exact_scope(
        snapshot,
        tournament_id=17,
        source_season_id=76986,
        allow_pending_season=True,
    )

    paths = render_scope_overlays(scope, tmp_path / "scope")

    # The registry overlay keeps the source season id (the match phase needs
    # it for its specs) but claims no team count it never measured.
    catalog = SofaScoreCatalog.load(paths.registry_path)
    season = catalog.resolve_source_season(17, "2526")
    assert season.season_id == 76986
    assert season.team_count is None
    registry_season = json.loads(paths.registry_path.read_text())[
        "tournaments"
    ][0]["seasons"][0]
    assert "team_count" not in registry_season
    assert "team_count_evidence" not in registry_season
    # The medallion overlay must still load (season_format drives the Bronze
    # partition label), yet its team count must not map onto any measured
    # band: page-evidence planning of a pending season fails closed.
    competitions = yaml.safe_load(paths.competitions_path.read_text())
    _validate_competitions_schema(competitions)
    season_config = competitions["competitions"][0]["seasons"][0]
    assert season_config["season_format"] == "split_year"
    with pytest.raises(WorkloadPlanError, match="outside the measured"):
        team_count_band(season_config["team_count"])
