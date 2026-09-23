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


def _sign(document):
    document["snapshot_id"] = hashlib.sha256(json.dumps(
        {key: value for key, value in document.items() if key != "snapshot_id"},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()).hexdigest()
    return document


def _after_wave_snapshot():
    """Snapshot as the 2025 metadata wave left it: 76986 excluded by schema_error."""

    document = _snapshot()
    tournament = document["tournaments"][0]
    tournament["classification"]["gender"] = "male"
    tournament["metadata_status"] = "ready"
    season = tournament["seasons"][0]
    season["metadata_status"] = "excluded"
    season["team_count"] = None
    season["team_count_evidence"] = {
        "type": "source_team_ids_unavailable",
        "endpoint": "/unique-tournament/17/season/76986/teams",
        "reason": "schema_error",
    }
    return _sign(document)


def test_next_wave_retries_schema_error_season():
    client = Client()

    enriched, report = enrich_snapshot(
        _after_wave_snapshot(), client, wave_start_year=2024
    )

    tournament = enriched["tournaments"][0]
    retried = tournament["seasons"][0]
    assert retried["metadata_status"] == "ready"
    assert retried["team_count"] == 20
    assert retried["team_count_evidence"]["type"] == "source_team_ids"
    assert "/unique-tournament/17/season/76986/teams" in client.calls
    assert report["retried_schema_error_scopes"] == 1
    assert report["recovered_schema_error_scopes"] == 1
    assert tournament["seasons"][1]["metadata_status"] == "ready"


def test_repeated_schema_error_keeps_evidence_and_stays_excluded():
    client = UnavailableTeamClient()
    before = _after_wave_snapshot()["tournaments"][0]["seasons"][0]

    enriched, report = enrich_snapshot(
        _after_wave_snapshot(), client, wave_start_year=2024
    )

    season = enriched["tournaments"][0]["seasons"][0]
    assert "/unique-tournament/17/season/76986/teams" in client.calls
    assert season["metadata_status"] == "excluded"
    assert season["team_count"] is None
    assert season["team_count_evidence"] == before["team_count_evidence"]
    assert report["retried_schema_error_scopes"] == 1
    assert report["recovered_schema_error_scopes"] == 0


def test_wave_does_not_retry_the_season_it_excluded():
    client = Client()

    enriched, report = enrich_snapshot(
        _after_wave_snapshot(), client, wave_start_year=2025
    )

    assert enriched["tournaments"][0]["seasons"][0]["metadata_status"] == "excluded"
    assert client.calls == []
    assert report["source_requests"] == 0


def test_tournament_excluded_seasons_are_never_retried():
    document = _after_wave_snapshot()
    tournament = document["tournaments"][0]
    tournament["metadata_status"] = "excluded"
    for season in tournament["seasons"]:
        season["metadata_status"] = "excluded"
    tournament["seasons"][1]["team_count_evidence"] = {
        "type": "source_team_ids_unavailable",
        "endpoint": "/unique-tournament/17/season/61627/teams",
        "reason": "schema_error",
    }
    client = Client()

    _, report = enrich_snapshot(_sign(document), client, wave_start_year=2024)

    assert client.calls == []
    assert report["source_requests"] == 0
    assert report.get("retried_schema_error_scopes", 0) == 0


def test_retry_lifts_a_tournament_with_no_pending_season_in_the_wave():
    """The live shape: the wave year is already done, only the schema_error stays."""

    document = _after_wave_snapshot()
    done = document["tournaments"][0]["seasons"][1]
    done["metadata_status"] = "ready"
    done["team_count"] = 20
    done["team_count_evidence"] = {
        "type": "source_team_ids",
        "endpoint": "/unique-tournament/17/season/61627/teams",
        "count": 20,
        "team_ids_sha256": "0" * 64,
    }
    client = Client()

    enriched, report = enrich_snapshot(_sign(document), client, wave_start_year=2024)

    retried = enriched["tournaments"][0]["seasons"][0]
    assert retried["metadata_status"] == "ready"
    assert client.calls == ["/unique-tournament/17/season/76986/teams"]
    assert report["retried_schema_error_scopes"] == 1
    assert report["recovered_schema_error_scopes"] == 1


@pytest.mark.parametrize("tournament_class", ["esoccer", "student"])
def test_tournament_outside_the_denominator_queues_is_never_fetched(tournament_class):
    from scrapers.sofascore.denominator import (
        CLASS_PRIORITY,
        Denominator,
        DenominatorRow,
    )

    denominator = Denominator(rows={17: DenominatorRow(
        tournament_id=17, capture_key="SS-17", name="x",
        tournament_class=tournament_class,
        queue_priority=CLASS_PRIORITY[tournament_class], basis="test",
    )})
    client = Client()
    snapshot = _snapshot()

    enriched, _ = enrich_snapshot(
        snapshot, client, wave_start_year=2025, denominator=denominator
    )

    assert client.calls == []
    assert enriched["tournaments"] == snapshot["tournaments"]


def _priority_denominator(classes):
    from scrapers.sofascore.denominator import (
        CLASS_PRIORITY,
        Denominator,
        DenominatorRow,
    )

    return Denominator(rows={
        tournament_id: DenominatorRow(
            tournament_id=tournament_id, capture_key=f"SS-{tournament_id}",
            name="x", tournament_class=tournament_class,
            queue_priority=CLASS_PRIORITY[tournament_class], basis="test",
        )
        for tournament_id, tournament_class in classes.items()
    })


def _priority_snapshot():
    def season(tournament_id, year, status="pending"):
        return {
            "source_season_id": tournament_id * 10000 + year,
            "start_year": year,
            "metadata_status": status,
            "team_count": None,
            "team_count_evidence": None,
        }

    def tournament(tournament_id, seasons, status="ready"):
        return {
            "unique_tournament_id": tournament_id,
            "metadata_status": status,
            "seasons": seasons,
        }

    document = {
        "schema_version": 1,
        "candidate_count": 4,
        "policy_id": "test",
        "campaign_id": "campaign-test",
        "tournaments": [
            # disputed (priority 9): its running season still ranks last.
            tournament(9, [season(9, 2026)]),
            # core: newest 2025 ready, deeper seasons pending.
            tournament(1, [
                season(1, 2025, "ready"), season(1, 2024), season(1, 2023),
                season(1, -1),
            ]),
            # core: running 2026 season pending.
            tournament(2, [season(2, 2026), season(2, 2024)], status="pending"),
            # esoccer (priority 0): never selected.
            tournament(5, [season(5, 2026)]),
        ],
    }
    return _sign(document)


def test_priority_selection_orders_running_core_then_deeper_core_then_disputed():
    denominator = _priority_denominator(
        {1: "core", 2: "core", 9: "youth", 5: "esoccer"}
    )

    picked = enrichment.select_priority_seasons(
        _priority_snapshot(), denominator, 10
    )

    assert picked == [
        (2, 22026, 2026),   # core, running season
        (1, 12024, 2024),   # core, deeper 2024 (tid 1 before tid 2)
        (2, 22024, 2024),
        (1, 12023, 2023),   # core, deeper 2023
        (9, 92026, 2026),   # disputed last
    ]


def test_priority_selection_cuts_by_seasons_not_tournaments():
    denominator = _priority_denominator(
        {1: "core", 2: "core", 9: "youth", 5: "esoccer"}
    )

    picked = enrichment.select_priority_seasons(
        _priority_snapshot(), denominator, 2
    )

    assert picked == [(2, 22026, 2026), (1, 12024, 2024)]


def test_priority_enrichment_fetches_only_selected_seasons():
    denominator = _priority_denominator({17: "core"})

    class SeasonClient(Client):
        pass

    client = SeasonClient()
    snapshot = _snapshot()

    enriched, report = enrich_snapshot(
        snapshot, client, wave_start_year=1999, denominator=denominator,
        select="priority", max_seasons=1,
    )

    assert client.calls == [
        "/unique-tournament/17",
        "/unique-tournament/17/season/76986/teams",
    ]
    seasons = enriched["tournaments"][0]["seasons"]
    assert [item["metadata_status"] for item in seasons] == ["ready", "pending"]
    assert report["selected_seasons"] == 1
    assert report["ready_wave_scopes"] == 1
    assert report["source_requests"] == 2


def test_priority_enrichment_does_not_retry_schema_error_seasons():
    denominator = _priority_denominator({17: "core"})
    document = _after_wave_snapshot()
    client = Client()

    enriched, report = enrich_snapshot(
        _sign(document), client, wave_start_year=2024, denominator=denominator,
        select="priority", max_seasons=5,
    )

    assert "/unique-tournament/17/season/76986/teams" not in client.calls
    assert report["retried_schema_error_scopes"] == 0


def test_priority_main_writes_selection_into_the_report(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.json"
    policy = tmp_path / "policy.json"
    report = tmp_path / "report.json"
    snapshot.write_text(json.dumps(_snapshot()))
    policy.write_text("{}")

    class FakeBrowser(Client):
        stats = {"paid_proxy_bytes": 0}

        def __init__(self, **_kwargs):
            super().__init__()

        def close(self):
            pass

    monkeypatch.setattr(enrichment, "LeaseBrowserSofaScoreClient", FakeBrowser)
    monkeypatch.setattr(enrichment, "validate_campaign_snapshot", lambda *_: None)
    monkeypatch.setattr(
        enrichment, "load_denominator",
        lambda *_: _priority_denominator({17: "core"}),
    )

    result = enrichment.main([
        "--snapshot", str(snapshot),
        "--policy", str(policy),
        "--output", str(snapshot),
        "--report", str(report),
        "--expected-snapshot-id", _snapshot()["snapshot_id"],
        "--dag-id", "dag_refresh_sofascore_all_mens",
        "--run-id", "scheduled__test",
        "--task-id", "enrich_season_metadata",
        "--budget-cap-bytes", "1000",
        "--control-url", "http://proxy-filter:8899",
        "--select", "priority",
        "--max-seasons", "2",
    ])

    written = json.loads(report.read_text())
    assert result == 0
    assert written["select"] == "priority"
    assert written["max_seasons"] == 2
    assert written["selected_seasons"] == 2
    assert written["ready_wave_scopes"] == 2
    assert written["wave_start_year"] is None


class MissingSeasonClient(Client):
    """HTTP 404 on the running season's team index, like the real client."""

    def get_json(self, path):
        if path == "/unique-tournament/17/season/76986/teams":
            from scrapers.sofascore.discovery import DiscoveryHTTPError

            self.calls.append(path)
            raise DiscoveryHTTPError(
                f"metered browser request failed: HTTP 404 {path}", status_code=404
            )
        return super().get_json(path)


def test_priority_queue_moves_past_a_season_the_source_does_not_have():
    # Astra r2: an entity-level 4xx must not abort the pass, or the priority
    # queue would select the same season first on every run forever.
    denominator = _priority_denominator({17: "core"})

    first, report = enrich_snapshot(
        _snapshot(), MissingSeasonClient(), wave_start_year=1999,
        denominator=denominator, select="priority", max_seasons=5,
    )

    seasons = first["tournaments"][0]["seasons"]
    assert seasons[0]["metadata_status"] == "excluded"
    assert seasons[0]["team_count_evidence"]["reason"] == "schema_error"
    assert seasons[1]["metadata_status"] == "ready"
    assert report["ready_wave_scopes"] == 1

    client = MissingSeasonClient()
    _second, again = enrich_snapshot(
        first, client, wave_start_year=1999,
        denominator=denominator, select="priority", max_seasons=5,
    )

    assert client.calls == []
    assert again["selected_seasons"] == 0


@pytest.mark.parametrize("status", [403, 429, 500, None])
def test_blocking_or_transient_errors_still_abort_the_pass(status):
    from scrapers.sofascore.discovery import DiscoveryHTTPError

    class BlockedClient(Client):
        def get_json(self, path):
            if "/season/" in path:
                raise DiscoveryHTTPError("blocked", status_code=status)
            return super().get_json(path)

    with pytest.raises(DiscoveryHTTPError):
        enrich_snapshot(
            _snapshot(), BlockedClient(), wave_start_year=1999,
            denominator=_priority_denominator({17: "core"}),
            select="priority", max_seasons=5,
        )


def test_missing_tournament_identity_is_skipped_not_fatal():
    from scrapers.sofascore.discovery import DiscoveryHTTPError

    class GoneClient(Client):
        def get_json(self, path):
            if path == "/unique-tournament/17":
                self.calls.append(path)
                raise DiscoveryHTTPError("HTTP 404", status_code=404)
            return super().get_json(path)

    snapshot = _snapshot()
    client = GoneClient()

    enriched, report = enrich_snapshot(
        snapshot, client, wave_start_year=1999,
        denominator=_priority_denominator({17: "core"}),
        select="priority", max_seasons=5,
    )

    assert client.calls == ["/unique-tournament/17"]
    assert report["unavailable_tournaments"] == 1
    tournament = enriched["tournaments"][0]
    assert tournament["metadata_status"] == "pending"
    assert tournament["identity_unavailable"]["status_code"] == 404
    assert tournament["seasons"] == snapshot["tournaments"][0]["seasons"]


def test_unavailable_identity_cannot_hold_the_head_of_the_queue():
    # Astra r3: with max_seasons=1 a gone tournament ahead of a healthy one
    # must not stall every following run.
    from scrapers.sofascore.discovery import DiscoveryHTTPError

    gone = deepcopy(_snapshot()["tournaments"][0])
    gone["unique_tournament_id"] = 5
    for season in gone["seasons"]:
        season["source_season_id"] += 1
    document = _snapshot()
    document["tournaments"].insert(0, gone)
    document["candidate_count"] = 2

    class GoneFirstClient(Client):
        def get_json(self, path):
            if path == "/unique-tournament/5":
                self.calls.append(path)
                raise DiscoveryHTTPError("HTTP 404", status_code=404)
            return super().get_json(path)

    denominator = _priority_denominator({5: "core", 17: "core"})
    state = _sign(document)
    ready = []
    for _run in range(3):
        state, report = enrich_snapshot(
            state, GoneFirstClient(), wave_start_year=1999,
            denominator=denominator, select="priority", max_seasons=1,
        )
        ready.append(sum(
            season["metadata_status"] == "ready"
            for item in state["tournaments"] for season in item["seasons"]
        ))

    # Run 1 meets the gone tournament first; runs 2 and 3 move past it.
    assert ready == [0, 1, 2]
