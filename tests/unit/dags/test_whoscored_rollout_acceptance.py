"""Production rollout acceptance contract for WhoScored."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pytest

from dags.scripts import whoscored_rollout_acceptance as acceptance


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _ranked_scopes(catalog_count: int = 163) -> list[str]:
    return [f"scope-{index:04d}" for index in range(catalog_count)]


def _scope_list_hash(scopes) -> str:
    return hashlib.sha256(("\n".join(scopes) + "\n").encode("utf-8")).hexdigest()


def _scope_plan(wave_id: str = "wave-20", *, catalog_count: int = 163):
    limit = acceptance.WAVE_LIMITS[wave_id]
    selected = min(limit, catalog_count)
    deferred = catalog_count - selected
    selected_scopes = _ranked_scopes(catalog_count)[:selected]
    return {
        "rollout_id": "rollout-954",
        "wave_id": wave_id,
        "max_scopes": limit,
        "require_full_active": wave_id == "wave-all",
        "active_scopes": selected_scopes,
        "active_scope_count": selected,
        "active_scopes_sha256": _scope_list_hash(selected_scopes),
        "catalog_active_scope_count": catalog_count,
        "catalog_active_scopes_sha256": _scope_list_hash(
            sorted(_ranked_scopes(catalog_count))
        ),
        "deferred_scope_count": deferred,
        "deferred_scopes_sha256": _digest(f"deferred:{wave_id}"),
        "cohort_sha256": _digest(f"cohort:{wave_id}"),
        "workload_sha256": _digest(f"workload:{wave_id}"),
        "ranked_scope_ids_sha256": _scope_list_hash(_ranked_scopes(catalog_count)),
        "ranked_workload_sha256": _digest("ranked-workload"),
        "runtime_sha256": _digest("code"),
        "classifier_sha256": _digest("classifier"),
        "promotion_acceptance_sha256": (
            acceptance.ROLLOUT_GENESIS_PROOF_SHA256
            if wave_id == "wave-20"
            else _digest(f"promotion:{wave_id}")
        ),
        "promotion_terminal_receipt_sha256": (
            acceptance.ROLLOUT_GENESIS_PROOF_SHA256
            if wave_id == "wave-20"
            else _digest(f"terminal:{wave_id}")
        ),
    }


def _scope_dq(scopes):
    return [
        {
            "scope": scope,
            "paid_proxy_bytes": 0,
            "expected_scope_batches": 1,
            "exact_scope_manifests": 1,
            "exact_scope_dataset_mismatches": 0,
            "expected_match_batches": 1,
            "exact_match_manifests": 1,
            "exact_match_dataset_mismatches": 0,
            "expected_match_not_available_batches": 0,
            "exact_match_not_available_manifests": 0,
            "exact_match_not_available_physical_rows": 0,
            "exact_match_outcome_count": 1,
            "exact_match_outcome_duplicates": 0,
            "exact_match_outcome_payload_sha256": _digest(f"match:{scope}"),
            "expected_preview_batches": 1,
            "exact_preview_manifests": 1,
            "exact_preview_dataset_mismatches": 0,
            "expected_preview_not_available_batches": 0,
            "exact_preview_not_available_manifests": 0,
            "exact_preview_not_available_physical_rows": 0,
            "exact_preview_outcome_count": 1,
            "exact_preview_outcome_duplicates": 0,
            "exact_preview_outcome_payload_sha256": _digest(f"preview:{scope}"),
            "schedule_rows": 2,
            "schedule_games": 2,
            "manifest_event_rows": 3,
            "current_event_rows": 3,
            "manifest_lineup_rows": 4,
            "current_lineup_rows": 4,
            "manifest_match_rows": 1,
            "current_match_rows": 1,
            "manifest_substitution_rows": 2,
            "current_substitution_rows": 2,
            "manifest_formation_rows": 2,
            "current_formation_rows": 2,
            "manifest_team_stat_rows": 2,
            "current_team_stat_rows": 2,
            "manifest_player_stat_rows": 4,
            "current_player_stat_rows": 4,
            "duplicate_source_event_ids": 0,
            "duplicate_team_event_ids": 0,
            "scope_manifest_mismatches": 0,
            "manifest_missing_player_rows": 1,
            "current_missing_player_rows": 1,
            "manifest_preview_lineup_rows": 2,
            "current_preview_lineup_rows": 2,
            "manifest_preview_section_rows": 3,
            "current_preview_section_rows": 3,
        }
        for scope in scopes
    ]


def _terminal_task_states(count: int):
    return [
        {
            "task_id": "validate_active_scope",
            "map_index": index,
            "state": "success",
        }
        for index in range(count)
    ] + [{"task_id": "final_success_gate", "map_index": -1, "state": "success"}]


def _runtime(*, code_tree_sha256: str | None = None):
    return {
        "status": "success",
        "transport_policy": "direct_then_paid",
        "direct_only": False,
        "campaign_id": "campaign-954",
        "approval_id": "approval-954",
        "approval_sha256": _digest("approval-954"),
        "runtime_contract": {
            "parser_version": acceptance.REQUIRED_PARSER_VERSION,
            "manifest_sha256": _digest("manifest"),
            "code_tree_sha256": code_tree_sha256 or _digest("code"),
        },
    }


def _catalog_dq():
    return {"status": "success", "active_scopes": 163, "quarantined": 0}


def _profile_dq():
    return {
        "status": "success",
        "expected_profile_batches": 2,
        "exact_profile_manifests": 2,
        "exact_profile_row_mismatches": 0,
        "exact_profile_participation_mismatches": 0,
        "expected_profile_not_available_batches": 0,
        "exact_profile_not_available_manifests": 0,
        "exact_profile_not_available_physical_rows": 0,
        "exact_profile_outcome_count": 2,
        "exact_profile_outcome_duplicates": 0,
        "exact_profile_outcome_payload_sha256": _digest("profile-outcomes"),
        "roster_players": 2,
        "current_profile_manifests": 2,
        "current_profile_rows": 2,
        "manifest_participation_rows": 4,
        "current_participation_rows": 4,
        "uncovered_profiles": 0,
        "stale_profiles": 0,
    }


def _traffic_dq():
    return {
        "schema_version": 1,
        "paid_proxy_bytes": 123,
        "reported_paid_proxy_bytes": 123,
        "request_ledger_paid_proxy_bytes": 123,
        "durable_paid_proxy_bytes": 123,
        "campaign_paid_proxy_bytes": 123,
        "artifact_sha256": _digest("traffic"),
        "artifact_bytes": 42,
    }


def _receipt_key(receipt):
    payload = json.dumps(
        receipt,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    rollout_id = receipt["scope"]["rollout_id"]
    return f"{acceptance.receipts_prefix(rollout_id)}/{digest}.json"


def _build(
    *,
    day: int,
    wave_id: str = "wave-20",
    records=(),
    previous=None,
    runtime=None,
    scope_plan=None,
    daily_slo=None,
    alert=None,
    scope_dq=None,
    terminal_task_states=None,
):
    logical_date = datetime(2026, 7, day, 10, tzinfo=timezone.utc)
    resolved_runtime = runtime or _runtime()
    resolved_plan = dict(scope_plan or _scope_plan(wave_id))
    if scope_plan is None:
        resolved_plan["runtime_sha256"] = resolved_runtime["runtime_contract"][
            "code_tree_sha256"
        ]
    if scope_plan is None and wave_id != "wave-20":
        prior_wave = acceptance.WAVE_ORDER[acceptance.WAVE_ORDER.index(wave_id) - 1]
        prior = [
            (key, receipt)
            for key, receipt in records
            if receipt["scope"]["wave_id"] == prior_wave
            and receipt["wave_accepted"] is True
        ]
        if prior:
            terminal_digest = prior[-1][0].rsplit("/", 1)[-1].removesuffix(".json")
            resolved_plan["promotion_terminal_receipt_sha256"] = terminal_digest
            resolved_plan["promotion_acceptance_sha256"] = _digest(
                f"proof:{terminal_digest}"
            )
    selected_count = resolved_plan["active_scope_count"]
    return acceptance.build_success_receipt(
        run_id=f"scheduled__{logical_date.isoformat()}",
        logical_date=logical_date,
        scope_plan=resolved_plan,
        runtime_preflight=resolved_runtime,
        catalog_dq=_catalog_dq(),
        profile_dq=_profile_dq(),
        traffic_dq=_traffic_dq(),
        daily_slo=daily_slo or {"status": "success", "p95_hours": 2.0, "samples": 20},
        alert_preflight=alert
        or {
            "status": "delivered",
            "campaign_id": "campaign-954",
            "approval_id": "approval-954",
            "approval_sha256": _digest("approval-954"),
            "transport_policy": "direct_then_paid",
        },
        scope_dq=scope_dq or _scope_dq(resolved_plan["active_scopes"]),
        terminal_task_states=terminal_task_states
        or _terminal_task_states(selected_count),
        previous_terminal_run=previous,
        existing_records=records,
    )


def _record(receipt):
    return (_receipt_key(receipt), receipt)


def _accepted_records_through(source_wave_id: str):
    records = []
    day = 16
    terminal = None
    source_index = acceptance.WAVE_ORDER.index(source_wave_id)
    for wave_id in acceptance.WAVE_ORDER[: source_index + 1]:
        first = _build(day=day, wave_id=wave_id, records=records)
        records.append(_record(first))
        terminal = _build(
            day=day + 1,
            wave_id=wave_id,
            records=records,
            previous={
                "run_id": first["run_id"],
                "state": "success",
                "logical_date": datetime(2026, 7, day, 10, tzinfo=timezone.utc),
            },
        )
        records.append(_record(terminal))
        day += 2
    assert terminal is not None
    return records, terminal


def _rehash_receipt(receipt):
    forged = json.loads(json.dumps(receipt))
    forged["identity_sha256"] = acceptance._sha256_json(
        acceptance._identity_payload(forged)
    )
    return _record(forged)


@pytest.mark.unit
def test_only_empty_scheduler_created_run_is_countable():
    exact = {
        "run_id": "scheduled__2026-07-22T10:00:00+00:00",
        "run_type": "scheduled",
        "external_trigger": False,
        "conf": {},
    }
    assert acceptance.is_countable_scheduled_run(**exact) is True
    assert (
        acceptance.is_countable_scheduled_run(**{**exact, "conf": {"wave": 20}})
        is False
    )
    assert (
        acceptance.is_countable_scheduled_run(
            **{**exact, "run_id": "manual__smoke", "run_type": "manual"}
        )
        is False
    )
    assert (
        acceptance.is_countable_scheduled_run(**{**exact, "external_trigger": True})
        is False
    )
    assert (
        acceptance.is_countable_scheduled_run(
            **{**exact, "conf": type("ConfMapping", (dict,), {})()}
        )
        is False
    )


@pytest.mark.unit
def test_public_evidence_replay_matches_receipt_authority_and_green_witnesses():
    scope_plan = _scope_plan()
    kwargs = {
        "scope_plan": scope_plan,
        "runtime_preflight": _runtime(),
        "catalog_dq": _catalog_dq(),
        "profile_dq": _profile_dq(),
        "traffic_dq": _traffic_dq(),
        "daily_slo": {"status": "success", "p95_hours": 2.0, "samples": 20},
        "alert_preflight": {
            "status": "delivered",
            "campaign_id": "campaign-954",
            "approval_id": "approval-954",
            "approval_sha256": _digest("approval-954"),
            "transport_policy": "direct_then_paid",
        },
        "scope_dq": _scope_dq(scope_plan["active_scopes"]),
        "terminal_task_states": _terminal_task_states(scope_plan["active_scope_count"]),
    }
    normalized = acceptance.normalized_run_evidence(**kwargs)
    receipt = _build(day=1)

    assert normalized == {
        field: receipt[field] for field in ("scope", "release", "evidence")
    }
    assert acceptance.run_evidence_sha256(**kwargs) == acceptance._sha256_json(
        normalized
    )


@pytest.mark.unit
def test_public_idempotency_evidence_binds_exact_green_counters():
    scopes = _scope_dq(("scope-0001", "scope-0002"))
    witness = acceptance.idempotency_evidence(
        scope_dq=scopes,
        profile_dq=_profile_dq(),
    )

    assert witness["schema_version"] == 1
    assert witness["status"] == "green"
    assert witness["scope"] == {
        "scope_count": 2,
        "exact_manifest_pair_count": 10,
        "duplicate_counter_count": 8,
        "physical_current_pair_count": 22,
        "zero_mismatch_counter_count": 12,
        "violation_count": 0,
        "evidence_sha256": witness["scope"]["evidence_sha256"],
    }
    assert witness["profile"] == {
        "exact_manifest_pair_count": 2,
        "duplicate_counter_count": 1,
        "physical_current_pair_count": 2,
        "zero_mismatch_counter_count": 5,
        "violation_count": 0,
        "evidence_sha256": witness["profile"]["evidence_sha256"],
    }
    assert all(
        len(section["evidence_sha256"]) == 64
        for section in (witness["scope"], witness["profile"])
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("target", "field"),
    (
        ("scope", "exact_match_manifests"),
        ("scope", "exact_match_outcome_duplicates"),
        ("scope", "current_event_rows"),
        ("profile", "exact_profile_manifests"),
        ("profile", "exact_profile_outcome_duplicates"),
        ("profile", "current_profile_rows"),
    ),
)
def test_idempotency_evidence_rejects_non_green_dq_counter(target, field):
    scopes = _scope_dq(("scope-0001",))
    profile = _profile_dq()
    selected = scopes[0] if target == "scope" else profile
    selected[field] = selected[field] + 1

    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="idempotency evidence is not green",
    ):
        acceptance.idempotency_evidence(scope_dq=scopes, profile_dq=profile)


@pytest.mark.unit
def test_rehashed_receipt_with_non_green_idempotency_is_rejected():
    receipt = _build(day=22)
    receipt["evidence"]["idempotency"]["scope"]["violation_count"] = 1

    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="idempotency witness is not green",
    ):
        acceptance.validated_receipts([_record(receipt)])


@pytest.mark.unit
def test_rehashed_receipt_cannot_forge_genesis_or_promotion_terminal_authority():
    first = _build(day=1)
    first_record = _record(first)
    second = _build(
        day=2,
        records=[first_record],
        previous={
            "run_id": first["run_id"],
            "state": "success",
            "logical_date": datetime(2026, 7, 1, 10, tzinfo=timezone.utc),
        },
    )
    forged_genesis = json.loads(json.dumps(second))
    forged_genesis["scope"]["promotion_acceptance_sha256"] = _digest("forged")
    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="genesis authority",
    ):
        acceptance.validated_receipts([_rehash_receipt(forged_genesis)])

    second_record = _record(second)
    promoted = _build(
        day=3,
        wave_id="wave-70",
        records=[first_record, second_record],
        previous={
            "run_id": second["run_id"],
            "state": "success",
            "logical_date": datetime(2026, 7, 2, 10, tzinfo=timezone.utc),
        },
    )
    forged_promoted = json.loads(json.dumps(promoted))
    forged_promoted["scope"]["promotion_terminal_receipt_sha256"] = _digest(
        "wrong-terminal"
    )
    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="authorized prior terminal",
    ):
        acceptance.validated_receipts([_rehash_receipt(forged_promoted)])


@pytest.mark.unit
@pytest.mark.parametrize(
    ("source_wave_id", "expected_run_count"),
    [("wave-20", 2), ("wave-70", 4)],
)
def test_public_promotion_replay_selects_exact_chain_and_legacy_digest(
    source_wave_id, expected_run_count
):
    records, terminal = _accepted_records_through(source_wave_id)
    terminal_digest = _receipt_key(terminal).rsplit("/", 1)[-1].removesuffix(".json")
    if source_wave_id == "wave-70":
        extra = _build(day=20, wave_id="wave-all", records=records)
        records.append(_record(extra))

    result = acceptance.promotion_acceptance_evidence(
        reversed(records),
        rollout_id="rollout-954",
        source_wave_id=source_wave_id,
        expected_terminal_receipt_sha256=terminal_digest,
    )
    proof = {
        field: result[field]
        for field in (
            "schema_version",
            "rollout_id",
            "source_wave_id",
            "source_cohort_sha256",
            "runtime_sha256",
            "classifier_sha256",
            "release",
            "receipt_sha256s",
            "terminal_receipt_sha256",
        )
    }

    assert result["promotion_acceptance_sha256"] == acceptance._sha256_json(proof)
    assert result["terminal_receipt_sha256"] == terminal_digest
    assert len(result["receipt_sha256s"]) == expected_run_count
    assert len(result["terminal_runs"]) == expected_run_count
    assert [run["logical_date"] for run in result["terminal_runs"]] == sorted(
        run["logical_date"] for run in result["terminal_runs"]
    )


@pytest.mark.unit
def test_public_promotion_replay_requires_exact_live_terminal_and_complete_chain():
    records, terminal = _accepted_records_through("wave-70")
    terminal_digest = _receipt_key(terminal).rsplit("/", 1)[-1].removesuffix(".json")
    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="terminal receipt is absent",
    ):
        acceptance.promotion_acceptance_evidence(
            records,
            rollout_id="rollout-954",
            source_wave_id="wave-70",
            expected_terminal_receipt_sha256=_digest("forged-terminal"),
        )

    missing_predecessor = records[1:]
    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="first success",
    ):
        acceptance.promotion_acceptance_evidence(
            missing_predecessor,
            rollout_id="rollout-954",
            source_wave_id="wave-70",
            expected_terminal_receipt_sha256=terminal_digest,
        )


@pytest.mark.unit
def test_two_adjacent_scheduled_successes_accept_one_wave():
    first = _build(day=22)
    first_record = _record(first)
    second = _build(
        day=23,
        records=[first_record],
        previous={
            "run_id": first["run_id"],
            "state": "success",
            "logical_date": datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
        },
    )

    assert first["consecutive_successes"] == 1
    assert first["wave_accepted"] is False
    assert second["consecutive_successes"] == 2
    assert second["wave_accepted"] is True


@pytest.mark.unit
@pytest.mark.parametrize(
    ("state", "previous_date", "runtime"),
    [
        ("failed", datetime(2026, 7, 22, 10, tzinfo=timezone.utc), None),
        ("success", datetime(2026, 7, 21, 10, tzinfo=timezone.utc), None),
        (
            "success",
            datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
            _runtime(code_tree_sha256="f" * 64),
        ),
    ],
)
def test_failure_missed_day_or_release_change_resets_streak(
    state, previous_date, runtime
):
    first = _build(day=22)
    second = _build(
        day=23,
        records=[_record(first)],
        previous={
            "run_id": first["run_id"],
            "state": state,
            "logical_date": previous_date,
        },
        runtime=runtime,
    )

    assert second["consecutive_successes"] == 1
    assert second["wave_accepted"] is False


@pytest.mark.unit
def test_changed_signed_cohort_resets_same_wave_streak():
    first = _build(day=22)
    plan = _scope_plan("wave-20")
    plan["cohort_sha256"] = _digest("changed-cohort")
    second = _build(
        day=23,
        records=[_record(first)],
        previous={
            "run_id": first["run_id"],
            "state": "success",
            "logical_date": datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
        },
        scope_plan=plan,
    )

    assert second["consecutive_successes"] == 1
    assert second["wave_accepted"] is False


@pytest.mark.unit
def test_next_wave_requires_hash_link_to_accepted_previous_wave():
    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="requires accepted wave-20",
    ):
        _build(day=24, wave_id="wave-70")

    first = _build(day=22)
    accepted_20 = _build(
        day=23,
        records=[_record(first)],
        previous={
            "run_id": first["run_id"],
            "state": "success",
            "logical_date": datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
        },
    )
    wave_70 = _build(
        day=24,
        wave_id="wave-70",
        records=[_record(first), _record(accepted_20)],
    )

    assert wave_70["prior_wave_receipt_sha256"] == _receipt_key(accepted_20).rsplit(
        "/", 1
    )[-1].removesuffix(".json")


@pytest.mark.unit
def test_complete_three_wave_chain_reports_accepted():
    records = []
    day = 16
    prior_accepted = None
    for wave_id in acceptance.WAVE_ORDER:
        first = _build(day=day, wave_id=wave_id, records=records)
        records.append(_record(first))
        second = _build(
            day=day + 1,
            wave_id=wave_id,
            records=records,
            previous={
                "run_id": first["run_id"],
                "state": "success",
                "logical_date": datetime(2026, 7, day, 10, tzinfo=timezone.utc),
            },
        )
        records.append(_record(second))
        prior_accepted = second
        day += 2

    assert prior_accepted is not None
    assert prior_accepted["scope"]["wave_id"] == "wave-all"
    assert prior_accepted["scope"]["deferred_scope_count"] == 0
    status = acceptance.rollout_acceptance_status(records, rollout_id="rollout-954")
    terminal_runs = status.pop("terminal_runs")
    assert status == {
        "schema_version": 1,
        "status": "accepted",
        "rollout_id": "rollout-954",
        "accepted_waves": list(acceptance.WAVE_ORDER),
        "missing_waves": [],
        "release": _runtime()["runtime_contract"],
        "catalog": {
            "active_scope_count": 163,
            "active_scopes_sha256": _scope_list_hash(sorted(_ranked_scopes())),
        },
        "authority": {
            field: prior_accepted["scope"][field]
            for field in (
                "rollout_id",
                "wave_id",
                "max_scopes",
                "require_full_active",
                "cohort_sha256",
                "ranked_scope_ids_sha256",
                "runtime_sha256",
                "classifier_sha256",
                "promotion_acceptance_sha256",
                "promotion_terminal_receipt_sha256",
            )
        },
        "final_wave_receipt_sha256": _receipt_key(prior_accepted)
        .rsplit("/", 1)[-1]
        .removesuffix(".json"),
    }
    assert len(terminal_runs) == 6
    assert all(run["task_states"]["count"] >= 21 for run in terminal_runs)
    assert all(
        run["scope_plan_sha256"]
        == acceptance._sha256_json(
            next(
                receipt["scope"]
                for _key, receipt in records
                if receipt["run_id"] == run["run_id"]
            )
        )
        for run in terminal_runs
    )
    assert all(
        run["evidence_sha256"]
        == acceptance._sha256_json(
            {
                field: next(
                    receipt[field]
                    for _key, receipt in records
                    if receipt["run_id"] == run["run_id"]
                )
                for field in ("scope", "release", "evidence")
            }
        )
        for run in terminal_runs
    )
    assert all(
        run["idempotency"]
        == next(
            receipt["evidence"]["idempotency"]
            for _key, receipt in records
            if receipt["run_id"] == run["run_id"]
        )
        for run in terminal_runs
    )
    assert [run["logical_date"] for run in terminal_runs] == sorted(
        run["logical_date"] for run in terminal_runs
    )


@pytest.mark.unit
def test_full_wave_rejects_any_deferred_scope():
    plan = _scope_plan("wave-all")
    plan["active_scope_count"] -= 1
    plan["deferred_scope_count"] = 1
    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError, match="exact cumulative wave"
    ):
        _build(day=22, wave_id="wave-all", scope_plan=plan)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("daily_slo", "alert", "runtime", "message"),
    [
        (
            {"status": "warming_up"},
            None,
            None,
            "rolling daily SLO is not green",
        ),
        (
            None,
            {"status": "not_required", "transport_policy": "direct_only"},
            None,
            "paid alert delivery is not green",
        ),
        (
            None,
            None,
            {
                **_runtime(),
                "runtime_contract": {
                    **_runtime()["runtime_contract"],
                    "parser_version": "whoscored-parser-v7",
                },
            },
            "not running parser-v8",
        ),
    ],
)
def test_non_green_slo_alert_or_parser_cannot_create_receipt(
    daily_slo, alert, runtime, message
):
    with pytest.raises(acceptance.WhoScoredRolloutAcceptanceError, match=message):
        _build(day=22, daily_slo=daily_slo, alert=alert, runtime=runtime)


@pytest.mark.unit
def test_foreign_paid_alert_receipt_is_rejected():
    alert = {
        "status": "delivered",
        "campaign_id": "campaign-foreign",
        "approval_id": "approval-954",
        "approval_sha256": _digest("approval-954"),
        "transport_policy": "direct_then_paid",
    }
    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="paid alert delivery is not green",
    ):
        _build(day=22, alert=alert)


@pytest.mark.unit
def test_mapped_scope_dq_must_match_unique_selected_order():
    plan = _scope_plan("wave-20")
    wrong = _scope_dq(plan["active_scopes"])
    wrong[1]["scope"] = wrong[0]["scope"]

    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="mapped scope DQ evidence is not exact",
    ):
        _build(day=22, scope_plan=plan, scope_dq=wrong)


@pytest.mark.unit
def test_wave_prefix_cannot_change_inside_one_frozen_ranking():
    first = _build(day=16)
    accepted_20 = _build(
        day=17,
        records=[_record(first)],
        previous={
            "run_id": first["run_id"],
            "state": "success",
            "logical_date": datetime(2026, 7, 16, 10, tzinfo=timezone.utc),
        },
    )
    plan = _scope_plan("wave-70")
    plan["active_scopes"][0] = "scope-outside-frozen-prefix"
    plan["active_scopes_sha256"] = _scope_list_hash(plan["active_scopes"])

    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="requires accepted wave-20",
    ):
        _build(
            day=18,
            wave_id="wave-70",
            scope_plan=plan,
            records=[_record(first), _record(accepted_20)],
        )


@pytest.mark.unit
def test_next_wave_cannot_predate_accepted_prior_wave():
    first = _build(day=20)
    accepted_20 = _build(
        day=21,
        records=[_record(first)],
        previous={
            "run_id": first["run_id"],
            "state": "success",
            "logical_date": datetime(2026, 7, 20, 10, tzinfo=timezone.utc),
        },
    )

    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="requires accepted wave-20",
    ):
        _build(
            day=19,
            wave_id="wave-70",
            records=[_record(first), _record(accepted_20)],
        )


@pytest.mark.unit
def test_full_wave_reveals_exact_rank_and_catalog_hashes():
    plan = _scope_plan("wave-all")
    plan["ranked_scope_ids_sha256"] = "f" * 64

    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="rank/catalog preimage",
    ):
        _build(day=22, wave_id="wave-all", scope_plan=plan)


@pytest.mark.unit
def test_durable_receipt_first_write_and_retry_are_identical():
    class Store:
        def __init__(self):
            self.values = {}

        def iter_content_addressed_json(self, prefix):
            return iter(
                sorted(
                    (key, value)
                    for key, value in self.values.items()
                    if key.startswith(f"{prefix}/")
                )
            )

        def put_content_addressed_json(self, prefix, value):
            payload = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            digest = hashlib.sha256(payload).hexdigest()
            key = f"{prefix}/{digest}.json"
            self.values[key] = dict(value)
            return {
                "uri": self.object_uri(key),
                "key": key,
                "sha256": digest,
                "bytes": len(payload),
            }

        def read_content_addressed_json(self, key, *, expected_sha256, expected_bytes):
            assert key.endswith(f"{expected_sha256}.json")
            value = self.values[key]
            payload = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            assert len(payload) == expected_bytes
            return dict(value)

        @staticmethod
        def object_uri(key):
            return f"s3://ops/{key}"

    store = Store()
    logical_date = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    plan = _scope_plan("wave-20")
    kwargs = {
        "ops_store": store,
        "run_id": f"scheduled__{logical_date.isoformat()}",
        "logical_date": logical_date,
        "scope_plan": plan,
        "runtime_preflight": _runtime(),
        "catalog_dq": _catalog_dq(),
        "scope_dq": _scope_dq(plan["active_scopes"]),
        "profile_dq": _profile_dq(),
        "traffic_dq": _traffic_dq(),
        "daily_slo": {"status": "success", "p95_hours": 2.0, "samples": 20},
        "alert_preflight": {
            "status": "delivered",
            "campaign_id": "campaign-954",
            "approval_id": "approval-954",
            "approval_sha256": _digest("approval-954"),
            "transport_policy": "direct_then_paid",
        },
        "terminal_task_states": _terminal_task_states(20),
        "previous_terminal_run": None,
    }

    first = acceptance.record_success_receipt(**kwargs)
    retry = acceptance.record_success_receipt(**kwargs)

    assert first == retry
    assert first["status"] == "pending"
    assert len(store.values) == 1


@pytest.mark.unit
def test_content_address_tamper_is_rejected():
    receipt = _build(day=22)
    bad_key = f"{acceptance.receipts_prefix('rollout-954')}/{'f' * 64}.json"
    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="content-address mismatch",
    ):
        acceptance.validated_receipts([(bad_key, receipt)])


@pytest.mark.unit
def test_content_addressed_receipt_must_be_directly_under_rollout_prefix():
    receipt = _build(day=22)
    nested_key = _receipt_key(receipt).replace("/receipts/", "/receipts/nested/")

    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="content-address mismatch",
    ):
        acceptance.validated_receipts([(nested_key, receipt)])


@pytest.mark.unit
def test_rehashed_receipt_cannot_detach_scope_dq_from_frozen_wave():
    receipt = _build(day=22)
    receipt["evidence"]["scope_dq"]["scopes_sha256"] = _digest("other-scopes")

    with pytest.raises(
        acceptance.WhoScoredRolloutAcceptanceError,
        match="scope DQ witness differs from its frozen wave",
    ):
        acceptance.validated_receipts([_record(receipt)])
