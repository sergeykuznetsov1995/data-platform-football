"""Pure policy tests for the FBref Bronze acceptance cohort."""

from __future__ import annotations

import copy
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dags.utils import fbref_bronze_acceptance_tasks as acceptance


def _candidate(
    target_id: str,
    page_kind: str,
    *,
    competition_id: str | None = "9",
    season_id: str | None = "2025-2026",
    is_current: bool | None = True,
    route: str | None = None,
    evidence_class: str | None = None,
) -> dict:
    source_ids = {}
    if competition_id is not None:
        source_ids["competition_id"] = competition_id
    if season_id is not None:
        source_ids["season_id"] = season_id
    if route is not None:
        source_ids["stat_route"] = route
    return {
        "target_id": target_id,
        "page_kind": page_kind,
        "canonical_url": f"https://fbref.com/en/{target_id}",
        "source_ids": source_ids,
        "refresh_policy": (
            "historical_once" if is_current is False else "daily"
        ),
        "state": "queued",
        "gender": None if page_kind == "competition_index" else "male",
        "competition_id": competition_id,
        "season_id": season_id,
        "is_current": is_current,
        "evidence_class": evidence_class,
    }


def _current_candidates() -> list[dict]:
    rows = [
        _candidate(
            "index",
            "competition_index",
            competition_id=None,
            season_id=None,
            is_current=None,
        ),
        _candidate("competition", "competition", season_id=None),
        _candidate("season", "season"),
        _candidate("schedule", "schedule"),
        _candidate("squad", "squad"),
        _candidate("matchlog", "matchlog"),
        _candidate(
            "player-populated",
            "player",
            evidence_class="populated_player",
        ),
        _candidate(
            "player-empty", "player", evidence_class="empty_player"
        ),
        _candidate("match-full", "match", evidence_class="full_match"),
        _candidate(
            "match-sparse", "match", evidence_class="sparse_match"
        ),
    ]
    rows.extend(
        _candidate(f"stats-{route}", "season_stats", route=route)
        for route in acceptance.SEASON_STAT_ROUTES
    )
    return rows


@pytest.mark.unit
def test_current_cohort_is_deterministic_and_covers_every_required_slot():
    candidates = _current_candidates()

    first = acceptance.select_acceptance_cohort(
        candidates, scope="current"
    )
    second = acceptance.select_acceptance_cohort(
        list(reversed(candidates)), scope="CURRENT"
    )

    assert first == second
    assert first["cohort_size"] == 15
    assert len(first["target_ids"]) == len(set(first["target_ids"]))
    assert set(item["slot"] for item in first["members"]) == set(
        acceptance._required_slots("current")
    )
    assert first["season"] is None
    assert len(first["cohort_hash"]) == 64


@pytest.mark.unit
def test_selector_fails_closed_without_explicit_empty_player_evidence():
    candidates = [
        row
        for row in _current_candidates()
        if row["evidence_class"] != "empty_player"
    ]

    with pytest.raises(
        acceptance.FBrefAcceptanceError, match="player_empty"
    ):
        acceptance.select_acceptance_cohort(candidates, scope="current")


@pytest.mark.unit
def test_selector_rejects_duplicate_or_non_male_candidates():
    candidates = _current_candidates()
    duplicate = [*candidates, copy.deepcopy(candidates[-1])]
    with pytest.raises(
        acceptance.FBrefAcceptanceError, match="duplicate target IDs"
    ):
        acceptance.select_acceptance_cohort(duplicate, scope="current")

    female = copy.deepcopy(candidates)
    female[-1]["gender"] = "female"
    with pytest.raises(acceptance.FBrefAcceptanceError, match="male scope"):
        acceptance.select_acceptance_cohort(female, scope="current")


@pytest.mark.unit
def test_current_selector_rejects_historical_season_evidence():
    candidates = _current_candidates()
    candidates[-1]["is_current"] = False

    with pytest.raises(
        acceptance.FBrefAcceptanceError, match="non-current seasons"
    ):
        acceptance.select_acceptance_cohort(candidates, scope="current")


def _history_candidates(competition_id: str, season_id: str) -> list[dict]:
    return [
        {
            **row,
            "target_id": f"{competition_id}-{season_id}-{row['target_id']}",
            "canonical_url": (
                "https://fbref.com/en/"
                f"{competition_id}/{season_id}/{row['target_id']}"
            ),
            "competition_id": competition_id,
            "season_id": season_id,
            "is_current": False,
            "refresh_policy": "historical_once",
            "source_ids": {
                **row["source_ids"],
                "competition_id": competition_id,
                "season_id": season_id,
            },
        }
        for row in _current_candidates()
        if row["page_kind"] not in {"competition_index", "competition"}
    ]


@pytest.mark.unit
def test_history_cohort_uses_exactly_one_complete_male_season():
    later = _history_candidates("20", "2023-2024")
    earlier = _history_candidates("8", "2021-2022")

    result = acceptance.select_acceptance_cohort(
        [*later, *earlier], scope="history"
    )

    assert result["cohort_size"] == 13
    assert result["season"] == {
        "competition_id": "20",
        "season_id": "2023-2024",
    }
    assert {
        (item["competition_id"], item["season_id"])
        for item in result["members"]
    } == {("20", "2023-2024")}


@pytest.mark.unit
def test_history_does_not_combine_incomplete_seasons():
    one = [
        row
        for row in _history_candidates("8", "2021-2022")
        if row["evidence_class"] != "empty_player"
    ]
    two = [
        row
        for row in _history_candidates("9", "2020-2021")
        if row["evidence_class"] != "full_match"
    ]

    with pytest.raises(
        acceptance.FBrefAcceptanceError, match="No one male historical season"
    ):
        acceptance.select_acceptance_cohort([*one, *two], scope="history")


@pytest.mark.unit
def test_dataset_gate_accepts_explained_empty_and_rejects_silent_empty():
    accepted = {
        "datasets": [
            {
                "availability": "available",
                "parse_status": "succeeded",
                "persistence_status": "succeeded",
                "validation_status": "succeeded",
                "row_count": 2,
                "empty_reason": None,
            },
            {
                "availability": "empty",
                "parse_status": "skipped",
                "persistence_status": "skipped",
                "validation_status": "succeeded",
                "row_count": 0,
                "empty_reason": "source_has_no_rows",
            },
        ]
    }
    acceptance._validate_dataset_evidence(accepted)

    accepted["datasets"][1]["empty_reason"] = None
    with pytest.raises(
        acceptance.FBrefAcceptanceError, match="invalid dataset manifest"
    ):
        acceptance._validate_dataset_evidence(accepted)

    accepted["datasets"][1].update(
        availability="unknown", empty_reason="unclassified", row_count=3
    )
    with pytest.raises(
        acceptance.FBrefAcceptanceError, match="invalid dataset manifest"
    ):
        acceptance._validate_dataset_evidence(accepted)


@pytest.mark.unit
def test_prepare_persists_exact_hash_routes_kinds_and_coverage(monkeypatch):
    candidates = _current_candidates()
    selected = acceptance.select_acceptance_cohort(
        candidates, scope="current"
    )
    control = SimpleNamespace(
        list_acceptance_candidates=MagicMock(return_value=candidates)
    )
    pipeline = SimpleNamespace(seed_acceptance_cohort=MagicMock())
    pipeline.seed_acceptance_cohort.return_value = {
        "target_ids": selected["target_ids"],
        "cohort_size": selected["cohort_size"],
        "cohort_sha256": selected["cohort_hash"],
    }
    monkeypatch.setattr(acceptance, "_control_store", lambda: control)
    monkeypatch.setattr(acceptance, "_pipeline", lambda: pipeline)
    monkeypatch.setattr(
        acceptance, "_control_run_id", lambda **_: "control-run"
    )

    result = acceptance.prepare_fbref_acceptance_cohort(
        airflow_run_id="manual__accept", dag_id="dag", scope="current"
    )

    assert result == selected
    kwargs = pipeline.seed_acceptance_cohort.call_args.kwargs
    assert kwargs["run_id"] == "control-run"
    assert kwargs["target_ids"] == selected["target_ids"]
    # 'standings' is intentionally excluded from required coverage (FBref serves
    # no standalone standings page; 0 targets in the frontier) while remaining in
    # the ACCEPTANCE_PAGE_KINDS candidate universe (#949).
    assert kwargs["required_page_kinds"] == tuple(
        kind for kind in acceptance.ACCEPTANCE_PAGE_KINDS if kind != "standings"
    )
    assert kwargs["required_routes"] == acceptance.SEASON_STAT_ROUTES
    assert kwargs["coverage_slots"] == {
        item["slot"]: item["target_id"] for item in selected["members"]
    }


@pytest.mark.unit
def test_live_wrapper_has_one_exact_paid_batch(monkeypatch):
    import utils.fbref_pipeline_tasks as pipeline_tasks

    run = MagicMock(return_value={"ok": True})
    monkeypatch.setattr(pipeline_tasks, "run_fbref_live_waves", run)

    result = acceptance.run_fbref_acceptance_live_wave(
        airflow_run_id="manual__accept", dag_id="dag", scope="history"
    )

    assert result == {"ok": True}
    kwargs = run.call_args.kwargs
    assert kwargs["run_type"] == "backfill"
    assert kwargs["request_limit"] == 100
    assert kwargs["byte_limit_mb"] == 50
    assert kwargs["shard_size"] == 25
    assert kwargs["max_batches"] == 1


@pytest.mark.unit
def test_replay_initializer_and_parser_are_physically_zero_network(monkeypatch):
    import utils.fbref_pipeline_tasks as pipeline_tasks

    pipeline = SimpleNamespace(
        initialize_acceptance_replay_run=MagicMock(return_value="replay-run")
    )
    monkeypatch.setattr(acceptance, "_pipeline", lambda: pipeline)
    source_id = "11111111-1111-4111-8111-111111111111"

    result = acceptance.initialize_fbref_acceptance_replay_run(
        airflow_run_id="manual__replay",
        dag_id="dag_replay_fbref_bronze",
        source_control_run_id=source_id,
    )

    assert result == "replay-run"
    settings = pipeline.initialize_acceptance_replay_run.call_args.kwargs[
        "settings"
    ]
    assert settings.run_type == "replay"
    assert settings.request_limit == settings.byte_limit == 0
    assert settings.shard_size == 25

    parse = MagicMock(return_value={"parsed": 0})
    monkeypatch.setattr(pipeline_tasks, "parse_fbref_wave", parse)
    assert acceptance.parse_fbref_acceptance_replay(
        airflow_run_id="manual__replay",
        dag_id="dag_replay_fbref_bronze",
        source_control_run_id=source_id,
    ) == {"parsed": 0}
    kwargs = parse.call_args.kwargs
    assert kwargs["acceptance_replay"] is True
    assert kwargs["request_limit"] == kwargs["byte_limit_mb"] == 0
    assert kwargs["shard_size"] == 25


@pytest.mark.unit
def test_replay_readiness_uses_read_only_raw_and_trino_probes(monkeypatch):
    import utils.alerts as alerts
    from scrapers.base import trino_manager
    from scrapers.fbref import raw_store, readiness

    monkeypatch.setenv("FBREF_RAW_STORE_URI", "s3://football/raw/fbref")
    monkeypatch.setattr(
        alerts,
        "validate_alert_environment",
        MagicMock(return_value={"status": "ready"}),
    )
    control = SimpleNamespace(
        validate_migrations=MagicMock(return_value={"read_only": True})
    )
    monkeypatch.setattr(acceptance, "_control_store", lambda: control)
    raw = object()
    monkeypatch.setattr(
        raw_store.RawPageStore,
        "from_uri",
        MagicMock(return_value=raw),
    )
    raw_read = MagicMock(return_value={"probe": "read_only_root_listing"})
    trino_read = MagicMock(
        return_value={"probe": "read_only_bronze_schema_select"}
    )
    monkeypatch.setattr(readiness, "check_raw_store_read_access", raw_read)
    monkeypatch.setattr(readiness, "check_trino_read_access", trino_read)
    trino = object()
    monkeypatch.setattr(
        trino_manager, "TrinoTableManager", MagicMock(return_value=trino)
    )
    raw_mutating = MagicMock(side_effect=AssertionError("must not write"))
    trino_mutating = MagicMock(side_effect=AssertionError("must not write"))
    monkeypatch.setattr(readiness, "check_raw_store_roundtrip", raw_mutating)
    monkeypatch.setattr(readiness, "check_trino_roundtrip", trino_mutating)

    result = acceptance.validate_fbref_acceptance_replay_readiness()

    assert result["publication_eligible"] is False
    assert result["request_limit"] == result["byte_limit_mb"] == 0
    assert result["dependencies"]["proxy_meter"] == {
        "status": "not_required"
    }
    raw_read.assert_called_once_with(raw)
    trino_read.assert_called_once_with(trino)
    raw_mutating.assert_not_called()
    trino_mutating.assert_not_called()


@pytest.mark.unit
def test_acceptance_lock_is_bounded_past_the_dag_timeout(monkeypatch):
    import utils.fbref_pipeline_tasks as pipeline_tasks

    acquire = MagicMock(return_value={"acquired": True})
    monkeypatch.setattr(
        pipeline_tasks, "acquire_fbref_publication_lock", acquire
    )

    result = acceptance.acquire_fbref_acceptance_publication_lock(
        airflow_run_id="manual__accept", dag_id="dag_accept_fbref_bronze"
    )

    assert result == {"acquired": True}
    assert acceptance.ACCEPTANCE_PUBLICATION_LOCK_TTL_SECONDS == 4 * 60 * 60
    assert acceptance.ACCEPTANCE_PUBLICATION_LOCK_TTL_SECONDS < 8 * 24 * 60 * 60
    acquire.assert_called_once_with(
        airflow_run_id="manual__accept",
        dag_id="dag_accept_fbref_bronze",
        ttl_seconds=acceptance.ACCEPTANCE_PUBLICATION_LOCK_TTL_SECONDS,
    )


@pytest.mark.unit
def test_airflow_parse_bridge_passes_acceptance_replay_to_core(monkeypatch):
    import utils.fbref_pipeline_tasks as pipeline_tasks

    pipeline = SimpleNamespace(
        parse_wave=MagicMock(
            return_value=SimpleNamespace(as_dict=lambda: {"parsed": 0})
        )
    )
    monkeypatch.setattr(pipeline_tasks, "_pipeline", lambda: pipeline)
    monkeypatch.setattr(
        pipeline_tasks, "_control_run_id", lambda **_: "processing-run"
    )

    result = pipeline_tasks.parse_fbref_wave(
        airflow_run_id="manual__replay",
        dag_id="dag_replay_fbref_bronze",
        page_kinds=["match"],
        run_type="replay",
        source_control_run_id="11111111-1111-4111-8111-111111111111",
        request_limit=0,
        byte_limit_mb=0,
        shard_size=25,
        acceptance_replay=True,
    )

    assert result == {"parsed": 0}
    assert pipeline.parse_wave.call_args.kwargs["acceptance_replay"] is True


def _strict_evidence(*, replay: bool = False) -> dict:
    return {
        "summary": {
            "request_limit": 0 if replay else 100,
            "byte_limit": 0 if replay else 50 * 1024 * 1024,
            "requests_used": 0 if replay else 1,
            "bytes_used": 0 if replay else 100,
            "traffic_totals": {"network_attempts": 0 if replay else 1},
        },
        "targets": [
            {
                "target_id": "target-1",
                "status": "succeeded",
                "page_kind": "season",
                "source_ids": {
                    "competition_id": "9",
                    "season_id": "2025-2026",
                },
                "http_status": 200,
                "raw_manifest_key": "raw/manifest.json",
                "content_hash": "a" * 64,
            }
        ],
        "datasets": [
            {
                "availability": "available",
                "target_id": "target-1",
                "parse_status": "succeeded",
                "persistence_status": "succeeded",
                "validation_status": "succeeded",
                "row_count": 1,
                "dataset": "typed:player_stats",
                "empty_reason": None,
            },
            {
                "availability": "available",
                "target_id": "target-1",
                "parse_status": "succeeded",
                "persistence_status": "succeeded",
                "validation_status": "succeeded",
                "row_count": 1,
                "dataset": "typed:team_stats",
                "empty_reason": None,
            },
            {
                "availability": "available",
                "target_id": "target-1",
                "parse_status": "succeeded",
                "persistence_status": "succeeded",
                "validation_status": "succeeded",
                "row_count": 0,
                "dataset": "typed:__complete__",
                "empty_reason": None,
            }
        ],
    }


@pytest.mark.unit
@pytest.mark.parametrize("replay", [False, True])
def test_strict_task_core_handoff_finishes_only_after_evidence(
    monkeypatch, replay
):
    evidence = _strict_evidence(replay=replay)
    control = SimpleNamespace(
        get_acceptance_run_evidence=MagicMock(return_value=evidence)
    )
    pipeline = SimpleNamespace(
        validate_and_finish=MagicMock(return_value={"status": "running"})
    )
    monkeypatch.setattr(acceptance, "_control_store", lambda: control)
    monkeypatch.setattr(acceptance, "_pipeline", lambda: pipeline)
    monkeypatch.setattr(
        acceptance, "_control_run_id", lambda **_: "processing-run"
    )
    expected = None
    if not replay:
        target_ids = ["target-1"]
        expected = {
            "cohort_hash": hashlib.sha256(
                json.dumps(
                    target_ids, ensure_ascii=True, separators=(",", ":")
                ).encode("ascii")
            ).hexdigest(),
            "target_ids": target_ids,
            "members": [{"slot": "season", "target_id": "target-1"}],
        }

    result = acceptance.validate_fbref_acceptance_run(
        airflow_run_id="manual__run",
        dag_id="acceptance-dag",
        expected_cohort=expected,
        source_control_run_id=("source-run" if replay else None),
        replay=replay,
    )

    assert result["control_run_id"] == "processing-run"
    kwargs = pipeline.validate_and_finish.call_args.kwargs
    assert kwargs["publication_eligible"] is False
    assert kwargs["acceptance"] is True
    assert kwargs["acceptance_replay"] is replay
    assert kwargs["replay_source_run_id"] == (
        "source-run" if replay else None
    )


@pytest.mark.unit
def test_replay_gate_rejects_missing_zero_traffic_counters(monkeypatch):
    evidence = _strict_evidence(replay=True)
    del evidence["summary"]["traffic_totals"]["network_attempts"]
    control = SimpleNamespace(
        get_acceptance_run_evidence=MagicMock(return_value=evidence)
    )
    pipeline = SimpleNamespace(validate_and_finish=MagicMock())
    monkeypatch.setattr(acceptance, "_control_store", lambda: control)
    monkeypatch.setattr(acceptance, "_pipeline", lambda: pipeline)
    monkeypatch.setattr(
        acceptance, "_control_run_id", lambda **_: "processing-run"
    )

    with pytest.raises(
        acceptance.FBrefAcceptanceError, match="not zero-network"
    ):
        acceptance.validate_fbref_acceptance_run(
            airflow_run_id="manual__replay",
            dag_id="dag_replay_fbref_bronze",
            source_control_run_id="source-run",
            replay=True,
        )

    pipeline.validate_and_finish.assert_not_called()


@pytest.mark.unit
def test_fresh_season_gate_requires_route_tables_and_completion():
    evidence = _strict_evidence()
    expected = {
        "members": [{"slot": "season", "target_id": "target-1"}]
    }

    acceptance._validate_fresh_coverage(evidence, expected)

    evidence["datasets"] = [
        item
        for item in evidence["datasets"]
        if item["dataset"] != "typed:team_stats"
    ]
    with pytest.raises(
        acceptance.FBrefAcceptanceError,
        match="season:typed_datasets_incomplete",
    ):
        acceptance._validate_fresh_coverage(evidence, expected)
