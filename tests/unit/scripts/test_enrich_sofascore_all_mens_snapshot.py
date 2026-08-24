from __future__ import annotations

import hashlib
import json
from copy import deepcopy

import pytest

from scripts import enrich_sofascore_all_mens_snapshot as enrichment
from scripts.enrich_sofascore_all_mens_snapshot import (
    SnapshotEnrichmentError,
    _SnapshotCheckpoint,
    enrich_snapshot,
)


def _snapshot():
    document = {
        "schema_version": 1,
        "candidate_count": 1,
        "policy_id": "test",
        "campaign_id": "campaign-test",
        "tournaments": [{
            "unique_tournament_id": 17,
            "capture_key": "SS-17",
            "name": "Premier League",
            "slug": "premier-league",
            "page_path": "football/england/premier-league",
            "category": {"id": 1, "name": "England", "slug": "england"},
            "kind": "league",
            "classification": {
                "sport": "football",
                "gender": "unknown",
                "age_group": "unknown",
                "team_level": "unknown",
                "status": "unknown",
            },
            "eligibility_review": {
                "status": "approved",
                "confirmed": {
                    "age_group": "adult",
                    "team_level": "first_team",
                    "professional": True,
                },
                "reviewed_by": "unit-owner",
            },
            "metadata_status": "pending",
            "seasons": [
                {
                    "source_season_id": 76986,
                    "source_name": "Premier League 25/26",
                    "canonical_season": "2526",
                    "season_format": "split_year",
                    "start_year": 2025,
                    "team_count": None,
                    "metadata_status": "pending",
                    "team_count_evidence": None,
                },
                {
                    "source_season_id": 61627,
                    "source_name": "Premier League 24/25",
                    "canonical_season": "2425",
                    "season_format": "split_year",
                    "start_year": 2024,
                    "team_count": None,
                    "metadata_status": "pending",
                    "team_count_evidence": None,
                },
            ],
        }],
    }
    document["snapshot_id"] = hashlib.sha256(json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    return document


class Client:
    def __init__(self, *, gender="M"):
        self.gender = gender
        self.calls = []

    def get_json(self, path):
        self.calls.append(path)
        if path == "/unique-tournament/17":
            return {"uniqueTournament": {
                "id": 17,
                "name": "Premier League",
                "slug": "premier-league",
                "gender": self.gender,
                "category": {
                    "id": 1,
                    "name": "England",
                    "slug": "england",
                    "sport": {"slug": "football"},
                },
            }}
        return {"teams": [{"id": value} for value in range(1, 21)]}


class UnavailableTeamClient(Client):
    def get_json(self, path):
        if path == "/unique-tournament/17/season/76986/teams":
            self.calls.append(path)
            return {"teams": []}
        return super().get_json(path)


def test_enrichment_confirms_gender_and_only_requested_wave_team_count():
    client = Client()
    enriched, report = enrich_snapshot(_snapshot(), client, wave_start_year=2025)

    tournament = enriched["tournaments"][0]
    assert tournament["classification"]["gender"] == "male"
    assert tournament["metadata_status"] == "ready"
    assert tournament["seasons"][0]["team_count"] == 20
    assert tournament["seasons"][0]["metadata_status"] == "ready"
    assert tournament["seasons"][1]["metadata_status"] == "pending"
    assert report["ready_tournaments"] == 1
    assert report["ready_wave_scopes"] == 1
    assert enriched["snapshot_id"] != _snapshot()["snapshot_id"]


def test_source_female_candidate_is_excluded_without_fetching_teams():
    client = Client(gender="F")
    enriched, report = enrich_snapshot(_snapshot(), client, wave_start_year=2025)

    tournament = enriched["tournaments"][0]
    assert tournament["metadata_status"] == "excluded"
    assert all(item["metadata_status"] == "excluded" for item in tournament["seasons"])
    assert report["excluded_tournaments"] == 1
    assert client.calls == ["/unique-tournament/17"]


def test_unavailable_team_list_excludes_only_that_season_and_continues():
    client = UnavailableTeamClient()

    enriched, report = enrich_snapshot(_snapshot(), client, wave_start_year=2025)

    tournament = enriched["tournaments"][0]
    season = tournament["seasons"][0]
    assert tournament["metadata_status"] == "ready"
    assert season["metadata_status"] == "excluded"
    assert season["team_count"] is None
    assert season["team_count_evidence"] == {
        "type": "source_team_ids_unavailable",
        "endpoint": "/unique-tournament/17/season/76986/teams",
        "reason": "schema_error",
    }
    assert report["excluded_wave_scopes"] == 1


def test_resume_does_not_refetch_ready_tournament_or_scope():
    client = Client()
    first, _ = enrich_snapshot(_snapshot(), client, wave_start_year=2025)
    resumed_client = Client()

    second, report = enrich_snapshot(first, resumed_client, wave_start_year=2025)

    assert second == first
    assert resumed_client.calls == []
    assert report["source_requests"] == 0


def _revision(snapshot, marker):
    revised = deepcopy(snapshot)
    revised["test_marker"] = marker
    revised["snapshot_id"] = hashlib.sha256(json.dumps(
        {key: value for key, value in revised.items() if key != "snapshot_id"},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()).hexdigest()
    return revised


def test_checkpoint_rejects_a_concurrent_snapshot_revision(tmp_path):
    output = tmp_path / "snapshot.json"
    initial = _snapshot()
    output.write_text(json.dumps(initial))
    checkpoint = _SnapshotCheckpoint(
        output,
        source_path=output,
        expected_snapshot_id=initial["snapshot_id"],
    )
    first = _revision(initial, "first")
    checkpoint.write(first)
    concurrent = _revision(first, "concurrent")
    output.write_text(json.dumps(concurrent))

    with pytest.raises(SnapshotEnrichmentError, match="changed after planning"):
        checkpoint.write(_revision(first, "ours"))

    assert json.loads(output.read_text())["test_marker"] == "concurrent"


def test_expected_revision_mismatch_fails_before_browser_creation(
    tmp_path, monkeypatch
):
    snapshot = tmp_path / "snapshot.json"
    policy = tmp_path / "policy.json"
    report = tmp_path / "report.json"
    snapshot.write_text(json.dumps(_snapshot()))
    policy.write_text("{}")
    browser_created = False

    def fail_if_created(**_kwargs):
        nonlocal browser_created
        browser_created = True
        raise AssertionError("browser must not be created")

    monkeypatch.setattr(enrichment, "LeaseBrowserSofaScoreClient", fail_if_created)
    monkeypatch.setattr(enrichment, "validate_campaign_snapshot", lambda *_: None)

    result = enrichment.main([
        "--snapshot", str(snapshot),
        "--policy", str(policy),
        "--output", str(snapshot),
        "--report", str(report),
        "--expected-snapshot-id", "0" * 64,
        "--dag-id", "operator_sofascore_all_mens_metadata",
        "--run-id", "manual__test",
        "--task-id", "metadata_wave_2025",
        "--budget-cap-bytes", "1000",
        "--control-url", "http://proxy-filter:8899",
    ])

    assert result == 1
    assert browser_created is False
    assert "changed after planning" in report.read_text()
