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
    MIN_MEASURED_TOURNAMENTS_FOR_TRANSFER,
    match_workload_class,
    player_workload_class,
    production_match_shape,
    production_player_shape,
    production_season_shape,
    season_workload_class,
    workload_shape_digest,
)
from scripts.research import bench_sofascore_paid_canary as canary


CAP = 100_000
MATCH_CLASS = match_workload_class()
PLAYER_CLASS = player_workload_class()
EPL_SEASON_SHAPE = production_season_shape(
    season_format="split_year",
    team_count_band="16_20",
    max_pages_per_direction=50,
)
WORLD_CUP_SEASON_SHAPE = production_season_shape(
    season_format="calendar_year",
    team_count_band="33_48",
    max_pages_per_direction=50,
)
EPL_SEASON_CLASS = season_workload_class(EPL_SEASON_SHAPE)
WORLD_CUP_SEASON_CLASS = season_workload_class(WORLD_CUP_SEASON_SHAPE)
# The paid canary measures these two tournaments; every other enabled league is
# authorized through the by-shape transfer rule, never through its own samples.
MEASURED_TOURNAMENT_IDS = {16, 17}


@pytest.fixture
def cohort():
    return canary.load_fixed_cohort()


def _spec(workload_class: str):
    return canary.CLASS_MANIFEST.spec(workload_class)


def _target(workload_class: str, tournament_id: int):
    return _spec(workload_class).target_for(tournament_id)


def _target_cohort(workload_class: str, tournament_id: int):
    return canary.load_target_cohort(_target(workload_class, tournament_id))


def test_experimental_engine_and_proxy_policy_ids_are_identical():
    from scripts.proxy_filter.budget import experimental_canary_policy_id

    policy = canary._experimental_policy(CAP, _spec(MATCH_CLASS))

    assert policy.artifact_id == experimental_canary_policy_id(CAP)
    assert policy.workload_class == MATCH_CLASS


@pytest.mark.parametrize("workload_class", [MATCH_CLASS, PLAYER_CLASS])
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

    assert _target(workload_class, 17).canonical_url == production


def _request_counts(workload_class: str) -> dict[str, int]:
    scope = _spec(workload_class).scope
    if scope == "match":
        return {name: 25 for name in canary.EVENT_PATHS}
    if scope == "player":
        return {name: 50 for name in canary.PLAYER_PATHS}
    return {name: 1 for name in canary.SEASON_ENDPOINTS}


def _sample(
    workload_class: str,
    run_id: str,
    mode: str,
    *,
    tournament_id: int,
    exit_index: int = 0,
) -> dict:
    spec = _spec(workload_class)
    target = spec.target_for(tournament_id)
    cohort = _target_cohort(workload_class, tournament_id)
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
    is_match = spec.scope == "match"
    is_player = spec.scope == "player"
    return {
        "run_id": run_id,
        "workload_class": workload_class,
        "source_tournament_id": tournament_id,
        "units": spec.max_units,
        "budget_eligible": mode == "cold",
        "cohort": target.cohort_name,
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
            "season_plan_complete": True if spec.scope == "season" else None,
            "transport_source": "none" if networkless else "sofascore_canary",
        },
    }


def _cold_samples(
    workload_class: str,
    count: int,
    *,
    tournaments: list[int] | None = None,
) -> list[dict]:
    """Round-robin cold samples across the class' measured tournaments."""

    spec = _spec(workload_class)
    ids = tournaments or [
        target.source_tournament_id for target in spec.targets
    ]
    return [
        _sample(
            workload_class,
            f"{workload_class}-cold-{index}",
            "cold",
            tournament_id=ids[index % len(ids)],
            exit_index=index % 5,
        )
        for index in range(count)
    ]


def _candidate(
    tmp_path: Path,
    cohort,
    *,
    short_class: str | None = None,
    short_count: int = 19,
    single_tournament_class: str | None = None,
    skewed_class: str | None = None,
) -> Path:
    artifact = tmp_path / "candidate.json"
    payload = canary._artifact_template(cohort, CAP)
    for class_name in canary.REQUIRED_WORKLOAD_CLASSES:
        spec = _spec(class_name)
        count = short_count if class_name == short_class else 20
        if class_name == single_tournament_class:
            samples = _cold_samples(
                class_name,
                count,
                tournaments=[spec.targets[0].source_tournament_id],
            )
        elif class_name == skewed_class:
            first, second = (
                spec.targets[0].source_tournament_id,
                spec.targets[-1].source_tournament_id,
            )
            samples = _cold_samples(
                class_name,
                count,
                tournaments=[first] * 11 + [second] * 9,
            )
        else:
            samples = _cold_samples(class_name, count)
        payload["workload_classes"][class_name]["samples"] = samples
        payload["workload_classes"][class_name]["hard_task_bytes"] = max(
            sample["total_provider_bytes"] for sample in samples
        )
        for mode in canary.BENCHMARK_ONLY_MODES:
            payload["benchmark_samples"].append(
                _sample(
                    class_name,
                    f"{class_name}-{mode}",
                    mode,
                    tournament_id=spec.targets[0].source_tournament_id,
                    exit_index=4,
                )
            )
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return artifact


def test_workload_classes_are_derived_from_the_class_manifest():
    manifest = canary.CLASS_MANIFEST

    assert canary.REQUIRED_WORKLOAD_CLASSES == tuple(sorted(manifest.classes))
    assert set(manifest.classes) == {
        MATCH_CLASS,
        PLAYER_CLASS,
        EPL_SEASON_CLASS,
        WORLD_CUP_SEASON_CLASS,
    }
    assert _spec(MATCH_CLASS).shape == production_match_shape()
    assert _spec(PLAYER_CLASS).shape == production_player_shape()
    assert _spec(EPL_SEASON_CLASS).shape == EPL_SEASON_SHAPE
    assert _spec(WORLD_CUP_SEASON_CLASS).shape == WORLD_CUP_SEASON_SHAPE
    # match/player are one merged shape measured on both tournaments; a season
    # shape is bound to the league sizes/formats it was actually measured on.
    assert _spec(MATCH_CLASS).measured_tournament_ids == (16, 17)
    assert _spec(PLAYER_CLASS).measured_tournament_ids == (16, 17)
    # #951+: t8 (La Liga) is the second measured tournament of the split_year
    # season shape, so the class can transfer to the unmeasured club leagues.
    assert _spec(EPL_SEASON_CLASS).measured_tournament_ids == (8, 17)
    assert _spec(WORLD_CUP_SEASON_CLASS).measured_tournament_ids == (16,)
    assert _target(EPL_SEASON_CLASS, 17).representative_season_id == 76986
    assert _target(EPL_SEASON_CLASS, 8).representative_season_id == 77559
    assert _target(WORLD_CUP_SEASON_CLASS, 16).representative_season_id == 58210
    for spec in manifest.classes.values():
        for target in spec.targets:
            cohort = canary.load_target_cohort(target)
            assert cohort.source_tournament_id == target.source_tournament_id


def test_manifest_names_are_recomputed_and_never_drawn(tmp_path):
    payload = json.loads(canary.DEFAULT_CLASS_MANIFEST_PATH.read_text())
    for entry in payload["classes"]:
        entry["name"] = "match_batch_25_deadbeefdeadbeef"
        if entry["scope"] == "season" and entry["shape_params"][
            "team_count_band"
        ] == "16_20":
            entry["shape_params"]["team_count_band"] = "21_32"
    path = tmp_path / "classes.json"
    path.write_text(json.dumps(payload))
    # The cohorts live next to the shipped manifest.
    for name in ("proxy_canary_cohort.json", "proxy_canary_cohort_world_cup.json"):
        (tmp_path / name).write_text((canary.CONFIG_DIR / name).read_text())

    manifest = canary.load_class_manifest(path)

    rebanded = season_workload_class(
        production_season_shape(
            season_format="split_year",
            team_count_band="21_32",
            max_pages_per_direction=50,
        )
    )
    assert rebanded in manifest.classes
    assert EPL_SEASON_CLASS not in manifest.classes
    assert "match_batch_25_deadbeefdeadbeef" not in manifest.classes
    assert manifest.classes[rebanded].shape_digest == workload_shape_digest(
        manifest.classes[rebanded].shape
    )


def test_manifest_rejects_two_classes_with_the_same_shape(tmp_path):
    payload = json.loads(canary.DEFAULT_CLASS_MANIFEST_PATH.read_text())
    duplicate = copy.deepcopy(payload["classes"][0])
    duplicate["key"] = "match_again"
    payload["classes"].append(duplicate)
    path = tmp_path / "classes.json"
    path.write_text(json.dumps(payload))

    with pytest.raises(canary.CanaryPolicyError, match="same match shape"):
        canary.load_class_manifest(path)


def test_manifest_rejects_a_season_class_without_shape_parameters(tmp_path):
    payload = json.loads(canary.DEFAULT_CLASS_MANIFEST_PATH.read_text())
    for entry in payload["classes"]:
        if entry["scope"] == "season":
            del entry["shape_params"]
    path = tmp_path / "classes.json"
    path.write_text(json.dumps(payload))

    with pytest.raises(canary.CanaryPolicyError, match="shape parameters"):
        canary.load_class_manifest(path)


def test_shipped_candidate_has_exact_required_v3_classes_and_shapes():
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
    assert payload["schema_version"] == 3
    assert payload["verified"] is False
    assert payload["class_manifest_sha256"] == canary.CLASS_MANIFEST.digest
    assert payload["requirements"][
        "minimum_distinct_tournaments_for_transfer"
    ] == MIN_MEASURED_TOURNAMENTS_FOR_TRANSFER
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
    # Wave 1 (#951): the top-5 club leagues plus the two measured tournaments,
    # extended with the Russian Premier League (t203) by the owner's decision.
    assert enabled_ids == {8, 16, 17, 23, 34, 35, 203}
    # Only t16/t17 carry paid canary samples; the other enabled leagues rely on
    # the by-shape transfer rule, which needs at least two measured tournaments.
    assert len(MEASURED_TOURNAMENT_IDS) >= MIN_MEASURED_TOURNAMENTS_FOR_TRANSFER
    assert MEASURED_TOURNAMENT_IDS <= enabled_ids
    for name in (MATCH_CLASS, PLAYER_CLASS):
        stored = payload["workload_classes"][name]
        assert stored["measured_tournament_ids"] == sorted(MEASURED_TOURNAMENT_IDS)
        assert set(stored["cohorts"]) == {
            str(value) for value in MEASURED_TOURNAMENT_IDS
        }
        assert "source_tournament_id" not in stored
        assert "source_tournament_id" not in stored["shape"]
    for name, shape in (
        (EPL_SEASON_CLASS, EPL_SEASON_SHAPE),
        (WORLD_CUP_SEASON_CLASS, WORLD_CUP_SEASON_SHAPE),
    ):
        stored = payload["workload_classes"][name]
        assert stored["shape"] == shape
        assert stored["shape_digest"] == workload_shape_digest(shape)
        assert set(stored["required_endpoints"]) == set(canary.SEASON_ENDPOINTS)
    assert payload["workload_classes"][EPL_SEASON_CLASS][
        "representative_season_ids"
    ] == {"17": 76986, "8": 77559}


def test_fixed_cohort_splits_into_exact_match_and_player_classes(cohort):
    specs = canary.build_fixed_specs(cohort)
    assert len(specs) == 225
    assert len(canary._class_specs(_spec(MATCH_CLASS), cohort)) == 125
    assert len(canary._class_specs(_spec(PLAYER_CLASS), cohort)) == 100


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
    assert payload["schema_version"] == 3
    assert payload["collector_version"] == "sofascore-paid-canary-v3"
    assert payload["verified"] is False
    assert payload["bootstrap_authorizes_production"] is False
    assert payload["class_manifest_sha256"] == canary.CLASS_MANIFEST.digest
    assert set(payload["workload_classes"]) == set(
        canary.REQUIRED_WORKLOAD_CLASSES
    )
    assert all(
        value["samples"] == []
        for value in payload["workload_classes"].values()
    )
    assert payload["runtime_fingerprint"] == canary.runtime_fingerprint()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("measured_tournament_ids", [17]),
        ("shape_digest", "0" * 64),
        ("cohorts", {"17": {"name": "x", "cohort_sha256": "0" * 64}}),
        ("max_units", 24),
    ],
)
def test_bootstrap_rejects_a_rewritten_immutable_class_field(
    tmp_path, cohort, field, value
):
    artifact = tmp_path / "candidate.json"
    payload = canary._artifact_template(cohort, CAP)
    payload["workload_classes"][MATCH_CLASS][field] = value
    artifact.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(canary.CanaryPolicyError, match="shape changed"):
        canary.bootstrap_artifact(
            artifact,
            cohort=cohort,
            experimental_cap_bytes=CAP,
        )


def test_bootstrap_never_relabels_existing_unfingerprinted_evidence(
    tmp_path, cohort
):
    artifact = tmp_path / "legacy-candidate.json"
    payload = canary._artifact_template(cohort, CAP)
    payload["workload_classes"][MATCH_CLASS]["samples"] = [
        _sample(MATCH_CLASS, "old-sample", "cold", tournament_id=17)
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

    sample = _sample(MATCH_CLASS, "stale-runtime", "cold", tournament_id=17)
    sample["evidence"]["runtime_fingerprint_digest"] = "0" * 64
    with pytest.raises(canary.CanaryPolicyError, match="runtime_fingerprint_digest"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_sample_of_an_unmeasured_tournament_is_rejected(cohort):
    # 23 (Serie A) transfers into the split_year shape but is never a measured
    # target of the class (measured: t17 EPL + t8 La Liga since #951+).
    sample = _sample(EPL_SEASON_CLASS, "foreign", "cold", tournament_id=17)
    sample["source_tournament_id"] = 23

    with pytest.raises(canary.CanaryPolicyError, match="not a measured target"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_each_class_needs_twenty_cold_runs(tmp_path, cohort):
    artifact = _candidate(tmp_path, cohort, short_class=WORLD_CUP_SEASON_CLASS)
    payload = json.loads(artifact.read_text())
    with pytest.raises(canary.CanaryPolicyError, match="20 cold runs"):
        canary.validate_artifact(payload, cohort=cohort, require_verifiable=True)


def test_each_class_needs_five_distinct_exits(tmp_path, cohort):
    artifact = _candidate(tmp_path, cohort)
    payload = json.loads(artifact.read_text())
    for sample in payload["workload_classes"][MATCH_CLASS]["samples"]:
        sample["proxy_exit_hash"] = hashlib.sha256(b"one-exit").hexdigest()
    with pytest.raises(canary.CanaryPolicyError, match="5 distinct exit hashes"):
        canary.validate_artifact(payload, cohort=cohort, require_verifiable=True)


def test_verify_rejects_a_class_measured_on_one_of_its_tournaments(
    tmp_path, cohort
):
    artifact = _candidate(tmp_path, cohort, single_tournament_class=PLAYER_CLASS)
    payload = json.loads(artifact.read_text())

    with pytest.raises(canary.CanaryPolicyError, match="no cold sample"):
        canary.validate_artifact(payload, cohort=cohort, require_verifiable=True)


def test_verify_rejects_cold_samples_skewed_below_the_even_floor(
    tmp_path, cohort
):
    artifact = _candidate(tmp_path, cohort, skewed_class=MATCH_CLASS)
    payload = json.loads(artifact.read_text())

    with pytest.raises(canary.CanaryPolicyError, match="skewed"):
        canary.validate_artifact(payload, cohort=cohort, require_verifiable=True)


def test_noop_and_offline_replay_have_zero_lease_network_and_allocation(cohort):
    for mode in ("no_op", "offline_replay"):
        sample = _sample(MATCH_CLASS, mode, mode, tournament_id=17)
        canary.validate_sample(sample, cohort=cohort, cap=CAP)
        assert sample["lease_count"] == 0
        assert sample["network_request_count"] == 0
        assert sample["allocation_bytes"] == 0
        assert sample["total_provider_bytes"] == 0
        assert sample["proxy_exit_hash"] is None


def test_networkless_sample_with_hidden_allocation_fails_closed(cohort):
    sample = _sample(MATCH_CLASS, "noop", "no_op", tournament_id=17)
    sample["allocation_bytes"] = 1
    with pytest.raises(canary.CanaryPolicyError, match="zero-network"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_cold_sample_rejects_extra_navigation_or_missing_source_count(cohort):
    sample = _sample(MATCH_CLASS, "cold", "cold", tournament_id=17)
    sample["metrics"]["navigations"] = 2
    with pytest.raises(canary.CanaryPolicyError, match="acceptance"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)

    sample = _sample(MATCH_CLASS, "cold", "cold", tournament_id=17)
    del sample["metrics"]["source_request_count"]
    with pytest.raises(canary.CanaryPolicyError, match="missing production"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_cold_sample_rejects_not_supported_required_match_endpoint(cohort):
    sample = _sample(MATCH_CLASS, "cold", "cold", tournament_id=17)
    sample["evidence"]["endpoint_status_counts"]["lineups"] = {
        "not_supported": 25
    }

    with pytest.raises(canary.CanaryPolicyError, match="not_supported"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_collection_rejects_not_supported_required_match_endpoint(cohort):
    spec = next(
        spec
        for spec in canary._class_specs(_spec(MATCH_CLASS), cohort)
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
            scope="match",
        )


def test_cold_season_sample_requires_complete_final_plan(cohort):
    sample = _sample(EPL_SEASON_CLASS, "cold", "cold", tournament_id=17)
    sample["evidence"]["season_plan_complete"] = False

    with pytest.raises(canary.CanaryPolicyError, match="acceptance"):
        canary.validate_sample(sample, cohort=cohort, cap=CAP)


def test_cold_season_sample_allows_absent_optional_referee_requests(cohort):
    sample = _sample(EPL_SEASON_CLASS, "cold", "cold", tournament_id=17)
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


def _drop_referee(sample: dict) -> dict:
    """Strip the optional dynamic ``referee_profile`` request from a season sample."""

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
    metrics["provider_up_bytes"] = (
        metrics["provider_total_bytes"] - metrics["provider_down_bytes"]
    )
    sample["network_request_count"] -= removed_count
    return sample


def test_verify_accepts_a_season_class_without_optional_referee_requests(
    tmp_path, cohort
):
    # A season cold sample may legitimately omit the dynamic referee endpoint;
    # verify must promote it instead of dead-locking the class forever.
    artifact = _candidate(tmp_path, cohort)
    payload = json.loads(artifact.read_text())
    season = payload["workload_classes"][EPL_SEASON_CLASS]
    for sample in season["samples"]:
        _drop_referee(sample)
    season["hard_task_bytes"] = max(
        sample["total_provider_bytes"] for sample in season["samples"]
    )
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    result = canary.verify_artifact(artifact)

    assert result["status"] == "verified"
    assert result["workload_classes"][EPL_SEASON_CLASS]["sample_count"] == 20


def test_extend_scenario_collects_and_verifies_above_the_minimum_floor(
    tmp_path, cohort, monkeypatch
):
    # extend carries 20 samples on the first target and adds a second target;
    # collection tops the second up to the even floor (MIN // 2 = 10), leaving
    # 20/10.  collect and verify must agree on that fixed floor - a len(samples)
    # floor of 30 // 2 = 15 would reject a class collect declared complete.
    artifact = _candidate(tmp_path, cohort)
    payload = json.loads(artifact.read_text())
    samples = _cold_samples(MATCH_CLASS, 30, tournaments=[16] * 20 + [17] * 10)
    payload["workload_classes"][MATCH_CLASS]["samples"] = samples
    payload["workload_classes"][MATCH_CLASS]["hard_task_bytes"] = max(
        sample["total_provider_bytes"] for sample in samples
    )
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    calls: list[tuple[str, int]] = []
    monkeypatch.setattr(canary, "execute_cold_run", _fake_cold_run(calls))
    result = canary.collect_canary(
        artifact_path=artifact,
        experimental_cap_bytes=CAP,
        target_cold_runs=20,
        workspace=tmp_path / "work",
    )

    # 20/10 already clears both the total (>= 20) and the even floor (>= 10).
    assert calls == []
    assert result["workload_classes"][MATCH_CLASS][
        "cold_samples_by_tournament"
    ] == {"16": 20, "17": 10}

    verified = canary.verify_artifact(artifact)
    assert verified["status"] == "verified"
    assert verified["workload_classes"][MATCH_CLASS]["sample_count"] == 30


def test_cold_season_sample_rejects_not_supported_schedule(cohort):
    sample = _sample(EPL_SEASON_CLASS, "cold", "cold", tournament_id=17)
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


def _fake_cold_run(calls: list[tuple[str, int]]):
    def fake_cold(class_spec, target, **_kwargs):
        calls.append((class_spec.name, target.source_tournament_id))
        sample = _sample(
            class_spec.name,
            f"{class_spec.name}-resumed-{len(calls)}",
            "cold",
            tournament_id=target.source_tournament_id,
            exit_index=4,
        )
        return SimpleNamespace(
            sample=sample,
            class_spec=class_spec,
            target=target,
        )

    return fake_cold


def test_collection_resumes_only_the_incomplete_class(tmp_path, cohort, monkeypatch):
    artifact = _candidate(
        tmp_path,
        cohort,
        short_class=MATCH_CLASS,
        short_count=19,
    )
    calls: list[tuple[str, int]] = []
    monkeypatch.setattr(canary, "execute_cold_run", _fake_cold_run(calls))

    result = canary.collect_canary(
        artifact_path=artifact,
        experimental_cap_bytes=CAP,
        target_cold_runs=20,
        workspace=tmp_path / "work",
    )

    # 19 round-robin samples leave tournament 17 one below the even floor.
    assert calls == [(MATCH_CLASS, 17)]
    summary = result["workload_classes"][MATCH_CLASS]
    assert summary["cold_samples"] == 20
    assert summary["cold_samples_by_tournament"] == {"16": 10, "17": 10}
    assert result["production_authorized"] is False


def test_collection_rebalances_a_target_below_the_even_floor(
    tmp_path, cohort, monkeypatch
):
    artifact = _candidate(tmp_path, cohort, skewed_class=MATCH_CLASS)
    calls: list[tuple[str, int]] = []
    monkeypatch.setattr(canary, "execute_cold_run", _fake_cold_run(calls))

    result = canary.collect_canary(
        artifact_path=artifact,
        experimental_cap_bytes=CAP,
        target_cold_runs=20,
        workspace=tmp_path / "work",
    )

    # 11/9 already reaches 20 samples, but tournament 17 is below floor 10.
    assert calls == [(MATCH_CLASS, 17)]
    assert result["workload_classes"][MATCH_CLASS][
        "cold_samples_by_tournament"
    ] == {"16": 11, "17": 10}


def test_collection_reports_blocked_class_and_continues_later_classes(
    tmp_path, cohort, monkeypatch
):
    artifact_template = canary._artifact_template

    def blocked_template(*args, **kwargs):
        payload = artifact_template(*args, **kwargs)
        payload["workload_classes"][PLAYER_CLASS]["collection_blocker"] = (
            "fixture source evidence is intentionally missing"
        )
        return payload

    monkeypatch.setattr(canary, "_artifact_template", blocked_template)
    artifact = _candidate(
        tmp_path,
        cohort,
        short_class=EPL_SEASON_CLASS,
        short_count=19,
    )
    calls: list[tuple[str, int]] = []
    monkeypatch.setattr(canary, "execute_cold_run", _fake_cold_run(calls))

    result = canary.collect_canary(
        artifact_path=artifact,
        experimental_cap_bytes=CAP,
        target_cold_runs=20,
        workspace=tmp_path / "work",
    )

    assert calls == [(EPL_SEASON_CLASS, 17)]
    assert result["workload_classes"][EPL_SEASON_CLASS]["cold_samples"] == 20
    assert result["blocked_workload_classes"] == {
        PLAYER_CLASS: "fixture source evidence is intentionally missing"
    }
    with pytest.raises(canary.CanaryPolicyError, match="collection blocker"):
        canary.validate_artifact(
            json.loads(artifact.read_text()),
            cohort=cohort,
            require_verifiable=True,
        )


def test_verify_atomically_changes_only_verified_after_all_classes(
    tmp_path, cohort
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


def test_verify_failure_does_not_rewrite_candidate(tmp_path, cohort):
    artifact = _candidate(tmp_path, cohort, short_class=EPL_SEASON_CLASS)
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


def test_extend_opens_an_unverified_candidate_and_carries_cold_samples(
    tmp_path, cohort
):
    artifact = _candidate(tmp_path, cohort)
    canary.verify_artifact(artifact)
    verified = json.loads(artifact.read_text())
    destination = tmp_path / "next-candidate.json"

    result = canary.extend_artifact(artifact, destination=destination)

    extended = json.loads(destination.read_text())
    assert result["status"] == "extended_unverified"
    assert result["production_authorized"] is False
    assert result["carried_cold_samples"] == {
        name: 20 for name in canary.REQUIRED_WORKLOAD_CLASSES
    }
    assert extended["verified"] is False
    assert json.loads(artifact.read_text()) == verified
    for name in canary.REQUIRED_WORKLOAD_CLASSES:
        assert (
            extended["workload_classes"][name]["samples"]
            == verified["workload_classes"][name]["samples"]
        )
        assert (
            extended["workload_classes"][name]["hard_task_bytes"]
            == verified["workload_classes"][name]["hard_task_bytes"]
        )
    canary.validate_artifact(extended, cohort=cohort, require_verifiable=True)


def test_extend_rejects_samples_from_another_runtime_fingerprint(
    tmp_path, cohort
):
    artifact = _candidate(tmp_path, cohort)
    canary.verify_artifact(artifact)
    payload = json.loads(artifact.read_text())
    payload["runtime_fingerprint"]["digest"] = "0" * 64
    artifact.write_text(json.dumps(payload))
    destination = tmp_path / "next-candidate.json"

    with pytest.raises(canary.CanaryPolicyError, match="runtime fingerprint"):
        canary.extend_artifact(artifact, destination=destination)
    assert not destination.exists()


def test_extend_refuses_an_unverified_source(tmp_path, cohort):
    artifact = _candidate(tmp_path, cohort)

    with pytest.raises(canary.CanaryPolicyError, match="needs a verified artifact"):
        canary.extend_artifact(artifact, destination=tmp_path / "next.json")
