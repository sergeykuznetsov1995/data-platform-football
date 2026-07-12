from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from jsonschema import Draft202012Validator

from scrapers.sofascore.capture_engine import HttpPayload, ProviderBudgetToken
from scrapers.sofascore.workload_plan import (
    MATCH_WORKLOAD_CLASS,
    PLAYER_WORKLOAD_CLASS,
    match_workload_class,
    player_workload_class,
    production_season_shape,
    season_shape_digest,
    season_workload_class,
)
from scripts.research import bench_sofascore_paid_canary as canary


CAP = 100_000


@pytest.fixture
def cohort():
    return canary.load_fixed_cohort()


@pytest.fixture
def complete_world_cup_cohort(tmp_path, monkeypatch):
    payload = json.loads(canary.DEFAULT_WORLD_CUP_COHORT_PATH.read_text())
    payload["player_ids"] = [str(9_000_000 + index) for index in range(50)]
    payload.pop("player_collection_blocker", None)
    path = tmp_path / "complete-world-cup-cohort.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    monkeypatch.setattr(canary, "DEFAULT_WORLD_CUP_COHORT_PATH", path)
    return path


def test_experimental_engine_and_proxy_policy_ids_are_identical(cohort):
    from scripts.proxy_filter.budget import experimental_canary_policy_id

    policy = canary._experimental_policy(CAP, cohort, MATCH_WORKLOAD_CLASS)

    assert policy.artifact_id == experimental_canary_policy_id(CAP)


@pytest.mark.parametrize(
    "workload_class",
    [MATCH_WORKLOAD_CLASS, PLAYER_WORKLOAD_CLASS],
)
def test_canary_and_production_use_identical_tournament_warm_anchor(
    workload_class,
):
    from dags.scripts.run_sofascore_scraper import _tournament_canonical_url

    tournament = SimpleNamespace(
        unique_tournament_id=17,
        slug="premier-league",
    )
    catalog = SimpleNamespace(competition=lambda _league: tournament)
    with patch(
        "scrapers.sofascore.catalog.SofaScoreCatalog.load",
        return_value=catalog,
    ):
        production = _tournament_canonical_url("ENG-Premier League", 17)

    assert canary.canonical_anchor(workload_class) == production


def _request_counts(workload_class: str) -> dict[str, int]:
    if workload_class in {
        canary.EPL_MATCH_CLASS,
        canary.WORLD_CUP_MATCH_CLASS,
    }:
        return {name: 25 for name in canary.EVENT_PATHS}
    if workload_class in {
        canary.EPL_PLAYER_CLASS,
        canary.WORLD_CUP_PLAYER_CLASS,
    }:
        return {name: 50 for name in canary.PLAYER_PATHS}
    return {name: 1 for name in canary.SEASON_ENDPOINTS}


def _sample(
    cohort,
    workload_class: str,
    run_id: str,
    mode: str,
    *,
    exit_index: int = 0,
) -> dict:
    counts = _request_counts(workload_class)
    planned = sum(counts.values())
    if mode == "cold":
        observations = {
            endpoint: [10] * count for endpoint, count in counts.items()
        }
        raw_writes = planned
        request_count = planned
    elif mode == "single_endpoint_resume":
        endpoint = next(iter(counts))
        observations = {endpoint: [17]}
        raw_writes = 1
        request_count = 1
    else:
        observations = {}
        raw_writes = 0
        request_count = 0
    endpoint_bytes = {
        endpoint: sum(values) for endpoint, values in observations.items()
    }
    total = sum(endpoint_bytes.values())
    networkless = mode in {"no_op", "offline_replay"}
    is_match = workload_class in {
        canary.EPL_MATCH_CLASS,
        canary.WORLD_CUP_MATCH_CLASS,
    }
    is_player = workload_class in {
        canary.EPL_PLAYER_CLASS,
        canary.WORLD_CUP_PLAYER_CLASS,
    }
    return {
        "run_id": run_id,
        "workload_class": workload_class,
        "source_tournament_id": canary._class_tournament_id(workload_class),
        "units": canary._class_units(workload_class),
        "budget_eligible": mode == "cold",
        "cohort": canary._class_cohort_name(workload_class),
        "mode": mode,
        "proxy_exit_hash": (
            None
            if networkless
            else hashlib.sha256(f"exit-{exit_index}".encode()).hexdigest()
        ),
        "total_provider_bytes": total,
        "endpoint_provider_bytes": endpoint_bytes,
        "endpoint_request_provider_bytes": observations,
        "lease_count": 0 if networkless else 1,
        "network_request_count": request_count,
        "allocation_bytes": 0 if networkless else CAP,
        "metrics": {
            "browser_sessions": 0 if networkless else 1,
            "cache_hit_rate": 1.0 if networkless else 0.0,
            "endpoint_completeness": 1.0,
            "matches_per_second": 1.0,
            "completed_matches": 25 if is_match else 0,
            "completed_players": 50 if is_player else 0,
            "navigations": 0 if networkless else request_count + 1,
            "p50_duration_ms": 1,
            "p95_duration_ms": 2,
            "players_per_second": 2.0,
            "provider_down_bytes": total // 2,
            "provider_total_bytes": total,
            "provider_up_bytes": total - total // 2,
            "replay_hit_rate": 1.0 if mode == "offline_replay" else 0.0,
            "request_count": request_count,
            "source_request_count": (
                0 if networkless else request_count + 2
            ),
        },
        "evidence": {
            "cohort_sha256": cohort.digest,
            "runtime_fingerprint_digest": canary.runtime_fingerprint()["digest"],
            "experimental_cap_bytes": CAP,
            "planned_endpoints": planned,
            "raw_payload_count": planned,
            "raw_payload_write_count": raw_writes,
            "successful_event_bases": 25 if is_match else 0,
            "successful_player_profiles": 50 if is_player else 0,
            "endpoint_status_counts": {
                endpoint: {"success": count}
                for endpoint, count in counts.items()
            },
            "season_plan_complete": (
                True
                if workload_class in {
                    canary.EPL_SEASON_CLASS,
                    canary.WORLD_CUP_SEASON_CLASS,
                }
                else None
            ),
            "transport_source": "none" if networkless else "sofascore_canary",
        },
    }


def _candidate(
    tmp_path: Path,
    cohort,
    *,
    short_class: str | None = None,
    short_count: int = 19,
) -> Path:
    artifact = tmp_path / "candidate.json"
    payload = canary._artifact_template(cohort, CAP)
    for class_name in canary.REQUIRED_WORKLOAD_CLASSES:
        class_cohort = canary._cohort_for_workload_class(cohort, class_name)
        count = short_count if class_name == short_class else 20
        samples = [
            _sample(
                class_cohort,
                class_name,
                f"{class_name}-cold-{index}",
                "cold",
                exit_index=index % 5,
            )
            for index in range(count)
        ]
        payload["workload_classes"][class_name]["samples"] = samples
        payload["workload_classes"][class_name]["hard_task_bytes"] = max(
            sample["total_provider_bytes"] for sample in samples
        )
        for mode in canary.BENCHMARK_ONLY_MODES:
            payload["benchmark_samples"].append(
                _sample(
                    class_cohort,
                    class_name,
                    f"{class_name}-{mode}",
                    mode,
                    exit_index=4,
                )
            )
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return artifact


def test_shipped_candidate_has_exact_required_v2_classes_and_shapes():
    payload = json.loads(canary.DEFAULT_ARTIFACT_PATH.read_text())
    schema = json.loads(
        (
            canary.ROOT
            / "configs/sofascore/proxy_budget_canary.schema.json"
        ).read_text()
    )
    Draft202012Validator(schema).validate(payload)
    assert payload == canary._artifact_template(
        canary.load_fixed_cohort(),
        payload["experimental_hard_cap_bytes"],
    )
    assert payload["schema_version"] == 2
    assert payload["verified"] is False
    assert set(payload["workload_classes"]) == set(
        canary.REQUIRED_WORKLOAD_CLASSES
    )
    registry = json.loads(
        (canary.ROOT / "configs" / "sofascore" / "tournaments.json").read_text()
    )
    enabled_ids = {
        item["unique_tournament_id"]
        for item in registry["tournaments"]
        if item["enabled"]
    }
    assert enabled_ids == {16, 17}
    for tournament_id in enabled_ids:
        assert match_workload_class(tournament_id) in payload["workload_classes"]
        assert player_workload_class(tournament_id) in payload["workload_classes"]
        assert payload["workload_classes"][match_workload_class(tournament_id)][
            "source_tournament_id"
        ] == tournament_id
        assert payload["workload_classes"][player_workload_class(tournament_id)][
            "source_tournament_id"
        ] == tournament_id
    world_player = payload["workload_classes"][canary.WORLD_CUP_PLAYER_CLASS]
    assert world_player["collection_blocker"] == ""
    assert world_player["cohort"] == "fixed_world_cup_50_players"
    epl = production_season_shape(
        17,
        season_format="split_year",
        max_pages_per_direction=50,
    )
    world_cup = production_season_shape(
        16,
        season_format="calendar_year",
        max_pages_per_direction=50,
    )
    for tournament_id, shape in ((17, epl), (16, world_cup)):
        name = season_workload_class(tournament_id, shape)
        stored = payload["workload_classes"][name]
        assert stored["shape"] == shape
        assert stored["shape_digest"] == season_shape_digest(shape)
        assert set(stored["required_endpoints"]) == set(canary.SEASON_ENDPOINTS)


def test_fixed_cohort_splits_into_exact_match_and_player_classes(cohort):
    specs = canary.build_fixed_specs(cohort)
    assert len(specs) == 225
    assert len(canary._class_specs(cohort, MATCH_WORKLOAD_CLASS)) == 125
    assert len(canary._class_specs(cohort, PLAYER_WORKLOAD_CLASS)) == 100


def test_bootstrap_requires_cap_and_never_authorizes_production(tmp_path, cohort):
    artifact = tmp_path / "candidate.json"
    with pytest.raises(canary.CanaryPolicyError, match="positive integer"):
        canary.bootstrap_artifact(
            artifact,
            cohort=cohort,
            experimental_cap_bytes=0,
        )
    payload = canary.bootstrap_artifact(
        artifact,
        cohort=cohort,
        experimental_cap_bytes=CAP,
    )
    assert payload["schema_version"] == 2
    assert payload["verified"] is False
    assert payload["bootstrap_authorizes_production"] is False
    assert all(
        value["samples"] == []
        for value in payload["workload_classes"].values()
    )
    assert payload["runtime_fingerprint"] == canary.runtime_fingerprint()


def test_bootstrap_never_relabels_existing_unfingerprinted_evidence(
    tmp_path, cohort
):
    artifact = tmp_path / "legacy-candidate.json"
    payload = canary._artifact_template(cohort, CAP)
    payload["workload_classes"][MATCH_WORKLOAD_CLASS]["samples"] = [
        _sample(cohort, MATCH_WORKLOAD_CLASS, "old-sample", "cold")
    ]
    del payload["runtime_fingerprint"]
    artifact.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(canary.CanaryPolicyError, match="new empty candidate"):
        canary.bootstrap_artifact(
            artifact,
            cohort=cohort,
            experimental_cap_bytes=CAP,
        )


def test_artifact_and_sample_reject_another_runtime_fingerprint(cohort):
    payload = canary._artifact_template(cohort, CAP)
    payload["runtime_fingerprint"]["digest"] = "0" * 64
    with pytest.raises(canary.CanaryPolicyError, match="does not match"):
        canary.validate_artifact(payload, cohort=cohort, require_verifiable=False)

    sample = _sample(cohort, MATCH_WORKLOAD_CLASS, "stale-runtime", "cold")
    sample["evidence"]["runtime_fingerprint_digest"] = "0" * 64
    with pytest.raises(canary.CanaryPolicyError, match="runtime_fingerprint_digest"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_each_class_needs_twenty_cold_runs(
    tmp_path, cohort, complete_world_cup_cohort
):
    artifact = _candidate(
        tmp_path,
        cohort,
        short_class=canary.WORLD_CUP_SEASON_CLASS,
    )
    payload = json.loads(artifact.read_text())
    with pytest.raises(canary.CanaryPolicyError, match="20 cold runs"):
        canary.validate_artifact(payload, cohort=cohort, require_verifiable=True)


def test_each_class_needs_five_distinct_exits(
    tmp_path, cohort, complete_world_cup_cohort
):
    artifact = _candidate(tmp_path, cohort)
    payload = json.loads(artifact.read_text())
    samples = payload["workload_classes"][MATCH_WORKLOAD_CLASS]["samples"]
    for sample in samples:
        sample["proxy_exit_hash"] = hashlib.sha256(b"one-exit").hexdigest()
    with pytest.raises(canary.CanaryPolicyError, match="5 distinct exit hashes"):
        canary.validate_artifact(payload, cohort=cohort, require_verifiable=True)


def test_noop_and_offline_replay_have_zero_lease_network_and_allocation(cohort):
    for mode in ("no_op", "offline_replay"):
        sample = _sample(cohort, MATCH_WORKLOAD_CLASS, mode, mode)
        canary.validate_sample(sample, cohort=cohort, cap=CAP)
        assert sample["lease_count"] == 0
        assert sample["network_request_count"] == 0
        assert sample["allocation_bytes"] == 0
        assert sample["total_provider_bytes"] == 0
        assert sample["proxy_exit_hash"] is None


def test_networkless_sample_with_hidden_allocation_fails_closed(cohort):
    sample = _sample(cohort, MATCH_WORKLOAD_CLASS, "noop", "no_op")
    sample["allocation_bytes"] = 1
    with pytest.raises(canary.CanaryPolicyError, match="zero-network"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_cold_sample_rejects_extra_navigation_or_missing_source_count(cohort):
    sample = _sample(cohort, MATCH_WORKLOAD_CLASS, "cold", "cold")
    sample["metrics"]["navigations"] = 2
    with pytest.raises(canary.CanaryPolicyError, match="acceptance"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)

    sample = _sample(cohort, MATCH_WORKLOAD_CLASS, "cold", "cold")
    del sample["metrics"]["source_request_count"]
    with pytest.raises(canary.CanaryPolicyError, match="missing production"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_cold_sample_rejects_not_supported_required_match_endpoint(cohort):
    sample = _sample(cohort, MATCH_WORKLOAD_CLASS, "cold", "cold")
    sample["evidence"]["endpoint_status_counts"]["lineups"] = {
        "not_supported": 25
    }

    with pytest.raises(canary.CanaryPolicyError, match="not_supported"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_collection_rejects_not_supported_required_match_endpoint(cohort):
    spec = next(
        spec
        for spec in canary._class_specs(cohort, MATCH_WORKLOAD_CLASS)
        if spec.key.endpoint == "lineups"
    )
    raw = SimpleNamespace(http_status=404, content_hash="hash", blob_key="blob")
    manifest = SimpleNamespace(
        key=spec.key,
        status=canary.ManifestStatus.NOT_SUPPORTED,
        raw_content_hash="hash",
        raw_blob_key="blob",
    )
    raw_store = SimpleNamespace(load_bytes=lambda _target: (b'{"error":404}', raw))

    with pytest.raises(canary.CanaryPolicyError, match="disallowed status"):
        canary._result_evidence(
            (spec,),
            (SimpleNamespace(manifest=manifest),),
            raw_store,
            workload_class=MATCH_WORKLOAD_CLASS,
        )


def test_cold_season_sample_requires_complete_final_plan(cohort):
    sample = _sample(cohort, canary.EPL_SEASON_CLASS, "cold", "cold")
    sample["evidence"]["season_plan_complete"] = False

    with pytest.raises(canary.CanaryPolicyError, match="acceptance"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_cold_season_sample_allows_absent_optional_referee_requests(cohort):
    sample = _sample(cohort, canary.EPL_SEASON_CLASS, "cold", "cold")
    removed = sample["endpoint_request_provider_bytes"].pop("referee_profile")
    removed_bytes = sample["endpoint_provider_bytes"].pop("referee_profile")
    sample["evidence"]["endpoint_status_counts"].pop("referee_profile")
    removed_count = len(removed)
    sample["evidence"]["planned_endpoints"] -= removed_count
    sample["evidence"]["raw_payload_count"] -= removed_count
    sample["evidence"]["raw_payload_write_count"] -= removed_count
    sample["total_provider_bytes"] -= removed_bytes
    metrics = sample["metrics"]
    metrics["request_count"] -= removed_count
    metrics["source_request_count"] -= removed_count
    metrics["navigations"] -= removed_count
    metrics["provider_total_bytes"] -= removed_bytes
    metrics["provider_up_bytes"] = metrics["provider_total_bytes"] - metrics["provider_down_bytes"]
    sample["network_request_count"] -= removed_count

    canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_cold_season_sample_rejects_not_supported_schedule(cohort):
    sample = _sample(cohort, canary.EPL_SEASON_CLASS, "cold", "cold")
    sample["evidence"]["endpoint_status_counts"]["schedule_last"] = {
        "not_supported": 1
    }

    with pytest.raises(canary.CanaryPolicyError, match="not_supported"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


@pytest.mark.parametrize(
    "plan",
    [
        SimpleNamespace(
            complete=False,
            pending_keys=("schedule",),
            player_universe_evidence_gaps=(),
        ),
        SimpleNamespace(
            complete=False,
            pending_keys=(),
            player_universe_evidence_gaps=("participants payload is empty",),
        ),
    ],
)
def test_collection_rejects_incomplete_season_plan(plan):
    with pytest.raises(canary.CanaryPolicyError, match="season canary plan is incomplete"):
        canary._require_complete_season_plan(plan)


def test_recording_transport_keeps_only_endpoint_byte_deltas():
    class Wrapped:
        def __enter__(self):
            return self

        def provider_snapshot(self):
            return {"provider_total_bytes": 0, "provider_budget_bytes": CAP}

        def request(self, url, *, provider_budget):
            return HttpPayload(200, b"{}", provider_bytes=37)

        def __exit__(self, *args):
            return False

    transport = canary.RecordingCanaryTransport(Wrapped(), CAP)
    token = ProviderBudgetToken("run", "event", "secret-token", CAP)
    with transport:
        transport.request("https://example.invalid", provider_budget=token)
    assert transport.request_observations == {"event": [37]}
    assert "secret-token" not in repr(transport.request_observations)


def test_collection_resumes_only_the_incomplete_class(tmp_path, cohort, monkeypatch):
    artifact = _candidate(
        tmp_path,
        cohort,
        short_class=MATCH_WORKLOAD_CLASS,
        short_count=19,
    )
    calls = []
    final_sample = _sample(
        cohort,
        MATCH_WORKLOAD_CLASS,
        "match-final",
        "cold",
        exit_index=4,
    )

    def fake_cold(*args, workload_class, **kwargs):
        calls.append(workload_class)
        return SimpleNamespace(sample=final_sample, workload_class=workload_class)

    monkeypatch.setattr(canary, "execute_cold_run", fake_cold)
    result = canary.collect_canary(
        artifact_path=artifact,
        experimental_cap_bytes=CAP,
        target_cold_runs=20,
        workspace=tmp_path / "work",
    )
    assert calls == [MATCH_WORKLOAD_CLASS]
    assert result["workload_classes"][MATCH_WORKLOAD_CLASS]["cold_samples"] == 20
    assert result["production_authorized"] is False


def test_collection_reports_blocked_class_and_continues_later_classes(
    tmp_path, cohort, monkeypatch
):
    artifact_template = canary._artifact_template

    def blocked_template(*args, **kwargs):
        payload = artifact_template(*args, **kwargs)
        payload["workload_classes"][canary.WORLD_CUP_PLAYER_CLASS][
            "collection_blocker"
        ] = "fixture source evidence is intentionally missing"
        return payload

    monkeypatch.setattr(canary, "_artifact_template", blocked_template)
    artifact = _candidate(
        tmp_path,
        cohort,
        short_class=canary.EPL_SEASON_CLASS,
        short_count=19,
    )
    calls = []
    final_sample = _sample(
        cohort,
        canary.EPL_SEASON_CLASS,
        "epl-season-final",
        "cold",
        exit_index=4,
    )

    def fake_cold(*args, workload_class, **kwargs):
        calls.append(workload_class)
        return SimpleNamespace(sample=final_sample, workload_class=workload_class)

    monkeypatch.setattr(canary, "execute_cold_run", fake_cold)
    result = canary.collect_canary(
        artifact_path=artifact,
        experimental_cap_bytes=CAP,
        target_cold_runs=20,
        workspace=tmp_path / "work",
    )

    assert calls == [canary.EPL_SEASON_CLASS]
    assert result["workload_classes"][canary.EPL_SEASON_CLASS][
        "cold_samples"
    ] == 20
    assert result["blocked_workload_classes"] == {
        canary.WORLD_CUP_PLAYER_CLASS: json.loads(artifact.read_text())[
            "workload_classes"
        ][canary.WORLD_CUP_PLAYER_CLASS]["collection_blocker"]
    }
    with pytest.raises(canary.CanaryPolicyError, match="collection blocker"):
        canary.validate_artifact(
            json.loads(artifact.read_text()),
            cohort=cohort,
            require_verifiable=True,
        )


def test_verify_atomically_changes_only_verified_after_all_classes(
    tmp_path, cohort, complete_world_cup_cohort
):
    artifact = _candidate(tmp_path, cohort)
    before = json.loads(artifact.read_text())
    result = canary.verify_artifact(artifact)
    after = json.loads(artifact.read_text())
    expected = copy.deepcopy(before)
    expected["verified"] = True
    assert after == expected
    assert result["status"] == "verified"
    assert set(result["workload_classes"]) == set(canary.REQUIRED_WORKLOAD_CLASSES)
    assert all(
        value["sample_count"] == 20
        for value in result["workload_classes"].values()
    )


def test_verify_failure_does_not_rewrite_candidate(
    tmp_path, cohort, complete_world_cup_cohort
):
    artifact = _candidate(
        tmp_path,
        cohort,
        short_class=canary.EPL_SEASON_CLASS,
    )
    before = artifact.read_bytes()
    with pytest.raises(canary.CanaryPolicyError, match="20 cold runs"):
        canary.verify_artifact(artifact)
    assert artifact.read_bytes() == before


def test_verify_rejects_raw_ip_without_promotion(tmp_path, cohort):
    artifact = _candidate(tmp_path, cohort)
    payload = json.loads(artifact.read_text())
    payload["diagnostic"] = "exit=203.0.113.7"
    artifact.write_text(json.dumps(payload))
    before = artifact.read_bytes()
    with pytest.raises(canary.CanaryPolicyError, match="raw IP"):
        canary.verify_artifact(artifact)
    assert artifact.read_bytes() == before
