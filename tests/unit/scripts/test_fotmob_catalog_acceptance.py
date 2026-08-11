from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from scrapers.fotmob.catalog_contract import build_catalog_contract
from scripts import fotmob_catalog_acceptance as acceptance


NOW = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)


def _contract_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "as_dict"):
        return dict(value.as_dict())
    if hasattr(value, "to_dict"):
        return dict(value.to_dict())
    if is_dataclass(value):
        return asdict(value)
    return dict(value)


def _contract(
    *,
    scopes=((47, "2025/2026"),),
    entities=("season",),
    entity_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _contract_dict(
        build_catalog_contract(
            catalog_batch_id="catalog-batch-1",
            catalog_content_hash="a" * 64,
            classifier_version="fotmob-men-v1",
            included_ids=[47],
            scopes=scopes,
            entities=entities,
            entity_policy=entity_policy or {},
        )
    )


def _included_decision(competition_id: int = 47) -> dict[str, Any]:
    return {
        "competition_id": competition_id,
        "catalog_name": "Premier League",
        "profile_name": "Premier League",
        "source_gender": "male",
        "source_age_group": "adult",
        "source_type": "league",
        "probe_status": "success",
        "decision": "included",
        "reason": "structurally confirmed adult men's competition",
        "policy_rule": "include_structural_male_adult",
        "classifier_version": "fotmob-men-v1",
        "profile_target_key": f"leagues?id={competition_id}",
        "profile_content_hash": "b" * 64,
    }


def _excluded_decision(competition_id: int = 88) -> dict[str, Any]:
    return {
        "competition_id": competition_id,
        "catalog_name": "Women's League",
        "profile_name": "Women's League",
        "source_gender": "female",
        "source_age_group": "adult",
        "source_type": "league",
        "probe_status": "success",
        "decision": "excluded",
        "reason": "women/female competition",
        "policy_rule": "exclude_female",
        "classifier_version": "fotmob-men-v1",
        "profile_target_key": f"leagues?id={competition_id}",
        "profile_content_hash": "c" * 64,
    }


def _attempt(
    contract: dict[str, Any],
    *,
    outcome: str = "success",
    last_attempt_at: datetime | None = None,
    reason: str = "scope completed",
    next_retry_at: datetime | None = None,
    attempt_count: int = 1,
    attempt_identities: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "competition_id": 47,
        "source_season_key": "2025/2026",
        "plan_signature": contract["plan_signature"],
        "attempt_count": attempt_count,
        "last_attempt_at": (last_attempt_at or NOW - timedelta(hours=1)).isoformat(),
        "next_retry_at": next_retry_at.isoformat() if next_retry_at else None,
        "outcome": outcome,
        "reason": reason,
        "attempt_identities": list(attempt_identities or []),
    }


def _report(*, lane: str = "current") -> dict[str, Any]:
    contract = _contract()
    return {
        "run_id": "run-1",
        "mode": "refresh" if lane == "current" else "backfill",
        "status": "success",
        "complete": True,
        "completed_at": NOW.isoformat(),
        "budget": {"proxy_bytes": 0},
        "selection": {
            "scope_lane": lane,
            "catalog_contract": contract,
            "entities": ["season"],
            "catalog_ids": [47, 88],
            "catalog_decisions": [_included_decision(), _excluded_decision()],
            "scope_plan_signature": contract["plan_signature"],
            "planned_scopes": ["47=2025/2026"],
            "scope_attempts": [_attempt(contract)],
            "completed_transfer_competition_ids": [],
            "transfer_plan_signature": None,
            "deferrals": [],
        },
    }


def _errors(report: dict[str, Any]) -> tuple[str, ...]:
    return acceptance.validate_report(report, now=NOW).errors


def test_validate_report_accepts_exact_dynamic_current_catalog_evidence() -> None:
    result = acceptance.validate_report(_report(), now=NOW)

    assert result.ok is True
    assert result["ok"] is True
    assert result.errors == ()
    assert result.catalog_count == 2
    assert result.included_count == 1
    assert result.scope_count == 1


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("included_count", 2),
        ("included_ids_sha256", "0" * 64),
        ("scope_count", 2),
        ("scope_sha256", "0" * 64),
        ("plan_signature", "fmplan1-tampered"),
    ],
)
def test_validate_report_recomputes_every_contract_count_hash_and_signature(
    field: str, replacement: object
) -> None:
    report = _report()
    report["selection"]["catalog_contract"][field] = replacement

    errors = _errors(report)

    assert any("catalog contract" in error for error in errors)


@pytest.mark.parametrize("mutation", ["duplicate", "missing"])
def test_validate_report_requires_exactly_one_decision_for_every_catalog_id(
    mutation: str,
) -> None:
    report = _report()
    if mutation == "duplicate":
        report["selection"]["catalog_decisions"].append(_included_decision())
    else:
        report["selection"]["catalog_decisions"].pop()

    errors = _errors(report)

    assert any("exactly one decision" in error for error in errors)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("probe_status", "pending"),
        ("source_gender", "female"),
        ("source_age_group", "U19"),
        ("source_type", None),
        ("policy_rule", "include_by_id"),
        ("profile_target_key", None),
        ("profile_content_hash", None),
        ("classifier_version", "fotmob-men-v0"),
    ],
)
def test_validate_report_requires_successful_structural_male_evidence(
    field: str, replacement: object
) -> None:
    report = _report()
    report["selection"]["catalog_decisions"][0][field] = replacement

    errors = _errors(report)

    assert any("included competition 47" in error for error in errors)


@pytest.mark.parametrize("source_type", ["league", "friendly"])
def test_validate_report_rejects_excluded_structural_adult_male_evidence(
    source_type: str,
) -> None:
    report = _report()
    excluded = report["selection"]["catalog_decisions"][1]
    excluded.update(
        {
            "catalog_name": "Senior Competition",
            "profile_name": "Club Friendlies" if source_type == "friendly" else "Senior Competition",
            "source_gender": "male",
            "source_age_group": "adult",
            "source_type": source_type,
        }
    )

    errors = _errors(report)

    assert any(
        "excluded competition 88" in error and "adult male" in error
        for error in errors
    )


def test_validate_report_keeps_structural_adult_male_friendlies_included() -> None:
    report = _report()
    included = report["selection"]["catalog_decisions"][0]
    included.update(
        {
            "catalog_name": "Club Friendlies",
            "profile_name": "Club Friendlies",
            "source_type": "friendly",
            "reason": "structurally confirmed adult men's friendly",
        }
    )

    assert acceptance.validate_report(report, now=NOW).ok is True


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("catalog_name", "Super Women's Cup"),
        ("profile_name", "National U-19 League"),
        ("profile_name", "Premier League U‑19"),
        ("profile_name", "Premier League Reserves"),
        ("profile_name", "Premier League Ｒｅｓｅｒｖｅｓ"),
        ("source_type", "exhibition"),
        ("profile_name", "Legends—Show"),
    ],
)
def test_validate_report_rejects_planned_female_youth_reserve_or_show_evidence(
    field: str, replacement: str
) -> None:
    report = _report()
    report["selection"]["catalog_decisions"][0][field] = replacement

    errors = _errors(report)

    assert any("forbidden structural signal" in error for error in errors)


def test_validate_report_rejects_scope_outside_included_contract_ids() -> None:
    report = _report()
    report["selection"]["planned_scopes"] = ["88=2025/2026"]

    errors = _errors(report)

    assert any("planned scope" in error and "included" in error for error in errors)


def test_validate_report_requires_current_terminal_evidence_within_72_hours() -> None:
    report = _report()
    contract = report["selection"]["catalog_contract"]
    report["selection"]["scope_attempts"] = [
        _attempt(contract, last_attempt_at=NOW - timedelta(hours=72, seconds=1))
    ]

    errors = _errors(report)

    assert any("older than 72 hours" in error for error in errors)


def test_validate_report_rejects_generic_history_retry_as_incomplete() -> None:
    report = _report(lane="history")
    contract = report["selection"]["catalog_contract"]
    report["status"] = "partial_success"
    report["complete"] = False
    report["selection"]["scope_attempts"] = [
        _attempt(
            contract,
            outcome="retryable",
            reason="upstream returned 503",
            next_retry_at=NOW + timedelta(hours=1),
        )
    ]

    errors = _errors(report)

    assert any("retryable scope" in error for error in errors)
    assert any("budget or deadline deferral" in error for error in errors)


@pytest.mark.parametrize("kind", ["budget", "deadline"])
def test_validate_report_accepts_only_explicit_authorized_partial_deferral(
    kind: str,
) -> None:
    report = _report(lane="history")
    contract = report["selection"]["catalog_contract"]
    reason = (
        "request budget deferred scope"
        if kind == "budget"
        else "run deadline deferred scope"
    )
    report["status"] = "partial_success"
    report["complete"] = False
    report["selection"]["scope_attempts"] = [
        _attempt(
            contract,
            outcome="deferred",
            reason=reason,
            next_retry_at=NOW + timedelta(minutes=1),
        )
    ]
    report["selection"]["deferrals"] = [
        {
            "kind": kind,
            "target_type": "scope",
            "targets": ["47=2025/2026"],
            "reason": reason,
        }
    ]

    assert acceptance.validate_report(report, now=NOW).ok is True


@pytest.mark.parametrize(
    ("reason", "next_retry_at"),
    [("", NOW + timedelta(hours=1)), ("upstream returned 503", None)],
)
def test_validate_report_rejects_retry_without_explicit_reason_and_due_time(
    reason: str, next_retry_at: datetime | None
) -> None:
    report = _report(lane="history")
    contract = report["selection"]["catalog_contract"]
    report["selection"]["scope_attempts"] = [
        _attempt(
            contract,
            outcome="retryable",
            reason=reason,
            next_retry_at=next_retry_at,
        )
    ]

    errors = _errors(report)

    assert any("retryable scope" in error for error in errors)


def test_continuous_lane_tolerates_scheduled_retry_but_campaign_does_not() -> None:
    """Одно и то же доказательство отвечает на два разных вопроса.

    Кампания (канарейка, допуск) спрашивает «обязательство закрыто целиком?» —
    там незакрытый повтор недопустим. Непрерывная полоса спрашивает «этот ран
    честен?» — она обходит ~450 скоупов под временным бюджетом, и запланированный
    повтор для неё штатное состояние. Структурные проверки одинаковы в обоих
    режимах.
    """

    report = _report()
    contract = report["selection"]["catalog_contract"]
    report["status"] = "partial_success"
    report["complete"] = False
    report["selection"]["scope_attempts"] = [
        _attempt(
            contract,
            outcome="retryable",
            reason="scope 47=2025/2026 incomplete; outstanding={}",
            next_retry_at=NOW + timedelta(minutes=15),
        )
    ]

    campaign = acceptance.validate_report(report, now=NOW)
    continuous = acceptance.validate_report(
        report, now=NOW, require_full_completion=False
    )

    assert campaign.ok is False
    assert any("cannot be accepted" in error for error in campaign.errors)
    assert continuous.ok is True, continuous.errors


def test_continuous_lane_accepts_partial_success_without_deferrals() -> None:
    """Неполнота бывает не только от бюджета.

    Раннер отдаёт partial_success и без бюджетной отсрочки — достаточно любого
    незакрытого хвоста. Если приёмка продолжает требовать бюджетное
    доказательство, гейт раннера и приёмка расходятся в трактовке одного слова,
    и ран падает уже на validate_data.
    """

    report = _report()
    report["status"] = "partial_success"
    report["complete"] = False

    campaign = acceptance.validate_report(report, now=NOW)
    continuous = acceptance.validate_report(
        report, now=NOW, require_full_completion=False
    )

    assert campaign.ok is False
    assert any("deferral evidence" in error for error in campaign.errors)
    assert continuous.ok is True, continuous.errors


def test_continuous_lane_accepts_unfinished_transfer_stream() -> None:
    """Недобранный поток трансферов — хвост следующего окна, а не отказ полосы.

    Раннер такой случай отсрочкой не оформляет: он кладёт
    `competition N transfer stream incomplete` в retryable операции и идёт
    дальше, поэтому у кампании это отказ, а у непрерывной полосы — норма.
    """

    policy = {
        "transfer_policy": {
            "window": "1year",
            "pagination": "unique_hits",
            "completion_scope": "included_ids",
            "completion_signature": "catalog_contract",
        }
    }
    contract = _contract(entities=("season", "transfers"), entity_policy=policy)
    report = _report()
    selection = report["selection"]
    selection["catalog_contract"] = contract
    selection["scope_plan_signature"] = contract["plan_signature"]
    selection["entities"] = ["season", "transfers"]
    selection["transfer_plan_signature"] = contract["plan_signature"]
    selection["completed_transfer_competition_ids"] = []
    selection["scope_attempts"] = [_attempt(contract)]
    report["status"] = "partial_success"
    report["complete"] = False

    campaign = acceptance.validate_report(report, now=NOW)
    continuous = acceptance.validate_report(
        report, now=NOW, require_full_completion=False
    )

    assert campaign.ok is False
    assert any("transfer completion" in error for error in campaign.errors)
    assert continuous.ok is True, continuous.errors


def test_continuous_lane_reds_a_stalled_lane_with_nothing_planned() -> None:
    """Пустой план + одни повторы в журнале = застой, и это должно краснеть.

    Если источник лёг и все скоупы ушли в повтор с бэкоффом, следующий ран
    планирует пустой список: своих исходов у него нет, гейт раннера молчит,
    отчёт получается `success`. Без этой проверки контур сутки стоял бы под
    зелёными ранами — ровно та слепота, которую чинит вся эта работа.
    """

    contract = _contract(scopes=((47, "2025/2026"), (47, "2024/2025")))
    report = _report()
    selection = report["selection"]
    selection["catalog_contract"] = contract
    selection["scope_plan_signature"] = contract["plan_signature"]
    selection["planned_scopes"] = []
    selection["completed_scopes"] = []
    stalled = dict(
        _attempt(
            contract,
            outcome="retryable",
            reason="scope 47=2025/2026 incomplete; outstanding={}",
            last_attempt_at=NOW - timedelta(hours=3),
            next_retry_at=NOW + timedelta(hours=3),
        )
    )
    selection["scope_attempts"] = [stalled]

    result = acceptance.validate_report(
        report, now=NOW, require_full_completion=False
    )

    assert result.ok is False
    assert any("lane is stalled" in error for error in result.errors), result.errors


def test_continuous_lane_ignores_retry_left_by_a_previous_run() -> None:
    """Ран, закрывший всё, что планировал, не краснеет из-за чужого повтора.

    `selection.scope_attempts` — это перечитанная карта состояний по подписи
    плана, то есть история: скоуп, отложенный прошлым раном на 6 часов, лежит в
    ней и в отчёте следующего рана. Если считать это «незакрытым обязательством
    текущего рана», лучший из возможных ранов оказывается красным, а silver
    так и не стартует.
    """

    contract = _contract(scopes=((47, "2025/2026"), (47, "2024/2025")))
    report = _report()
    selection = report["selection"]
    selection["catalog_contract"] = contract
    selection["scope_plan_signature"] = contract["plan_signature"]
    selection["planned_scopes"] = ["47=2025/2026"]
    stale_retry = dict(
        _attempt(
            contract,
            outcome="retryable",
            reason="scope 47=2024/2025 incomplete; outstanding={}",
            last_attempt_at=NOW - timedelta(hours=2),
            next_retry_at=NOW + timedelta(hours=4),
        )
    )
    stale_retry["source_season_key"] = "2024/2025"
    selection["scope_attempts"] = [_attempt(contract), stale_retry]

    campaign = acceptance.validate_report(report, now=NOW)
    continuous = acceptance.validate_report(
        report, now=NOW, require_full_completion=False
    )

    assert campaign.ok is False
    assert continuous.ok is True, continuous.errors


@pytest.mark.parametrize(
    ("outcome", "reason", "next_retry_at", "message"),
    [
        ("retryable", "", NOW + timedelta(minutes=15), "must record an explicit reason"),
        (
            "retryable",
            "scope 47=2025/2026 incomplete; outstanding={}",
            NOW - timedelta(hours=2),
            "next_retry_at must follow its attempt",
        ),
        ("terminal", "", None, "must record an explicit reason"),
    ],
)
def test_continuous_lane_keeps_structural_checks_on_retry_evidence(
    outcome: str, reason: str, next_retry_at: datetime | None, message: str
) -> None:
    """Послабление снимает вопрос о ПОЛНОТЕ, но не о качестве доказательства.

    Непрерывная полоса перестаёт краснеть из-за незакрытого каталога, однако
    доказательство обязано оставаться честным: у повтора есть причина и срок,
    и срок наступает после самой попытки.
    """

    report = _report()
    contract = report["selection"]["catalog_contract"]
    report["status"] = "partial_success"
    report["complete"] = False
    report["selection"]["scope_attempts"] = [
        _attempt(
            contract,
            outcome=outcome,
            reason=reason,
            next_retry_at=next_retry_at,
        )
    ]

    result = acceptance.validate_report(
        report, now=NOW, require_full_completion=False
    )

    assert result.ok is False
    assert any(message in error for error in result.errors), result.errors


def test_validate_report_accepts_source_gap_only_with_two_attempt_identities() -> None:
    report = _report()
    contract = report["selection"]["catalog_contract"]
    report["selection"]["scope_attempts"] = [
        _attempt(
            contract,
            outcome="source_gap",
            reason="two successful fetches lacked an advertised finished match",
            attempt_count=2,
            attempt_identities=["attempt-1", "attempt-2"],
        )
    ]

    assert acceptance.validate_report(report, now=NOW).ok is True

    report["selection"]["scope_attempts"][0]["attempt_identities"] = ["attempt-1"]
    errors = _errors(report)
    assert any("source_gap" in error and "two" in error for error in errors)


def test_validate_report_rejects_terminal_schema_or_commit_failure() -> None:
    report = _report()
    contract = report["selection"]["catalog_contract"]
    report["status"] = "partial_success"
    report["complete"] = False
    report["selection"]["scope_attempts"] = [
        _attempt(contract, outcome="terminal", reason="schema drift")
    ]

    assert any("hard failure" in error for error in _errors(report))


def test_validate_report_rejects_any_proxy_bytes() -> None:
    report = _report()
    report["budget"]["proxy_bytes"] = 1

    errors = _errors(report)

    assert any("proxy bytes must be zero" in error for error in errors)


def test_validate_report_rejects_scope_attempt_bound_to_another_plan() -> None:
    report = _report()
    report["selection"]["scope_attempts"][0]["plan_signature"] = "fmplan1-other"

    errors = _errors(report)

    assert any("plan signature" in error for error in errors)


def test_validate_report_rejects_selection_signature_different_from_contract() -> None:
    report = _report()
    report["selection"]["scope_plan_signature"] = "fmplan1-other"

    errors = _errors(report)

    assert any("selection.scope_plan_signature" in error for error in errors)


@pytest.mark.parametrize(
    ("status", "complete"),
    [
        ("success", False),
        ("partial_success", True),
        ("partial_success", False),
    ],
)
def test_validate_report_enforces_status_complete_and_partial_evidence_shape(
    status: str, complete: bool
) -> None:
    report = _report()
    report["status"] = status
    report["complete"] = complete

    errors = _errors(report)

    assert any(
        "status/complete" in error or "budget or deadline deferral" in error
        for error in errors
    )


def test_validate_report_binds_transfer_completion_to_catalog_contract() -> None:
    report = _report()
    transfer_policy = {
        "window": "1year",
        "pagination": "unique_hits",
        "completion_scope": "included_ids",
        "completion_signature": "catalog_contract",
    }
    contract = _contract(
        entities=("season", "transfers"),
        entity_policy={"transfer_policy": transfer_policy},
    )
    report["selection"].update(
        {
            "catalog_contract": contract,
            "entities": ["season", "transfers"],
            "scope_plan_signature": contract["plan_signature"],
            "scope_attempts": [_attempt(contract)],
            "completed_transfer_competition_ids": [47],
            "transfer_plan_signature": contract["plan_signature"],
        }
    )

    assert acceptance.validate_report(report, now=NOW).ok is True

    report["selection"]["completed_transfer_competition_ids"] = []
    errors = _errors(report)
    assert any("transfer completion" in error for error in errors)

    report["selection"]["completed_transfer_competition_ids"] = [47]
    report["selection"]["transfer_plan_signature"] = "fmplan1-separate"
    errors = _errors(report)
    assert any("transfer plan signature" in error for error in errors)


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("catalog_contract", "included_ids"), 47),
        (("catalog_contract", "scopes"), 47),
        (("scope_attempts", "attempt_identities"), [{"not": "hashable"}, "two"]),
    ],
)
def test_validate_report_fails_closed_for_malformed_nested_evidence(
    path: tuple[str, str], replacement: object
) -> None:
    report = _report()
    parent, field = path
    if parent == "catalog_contract":
        report["selection"][parent][field] = replacement
    else:
        report["selection"][parent][0][field] = replacement
        report["selection"][parent][0]["outcome"] = "source_gap"
        report["selection"][parent][0]["attempt_count"] = 2

    result = acceptance.validate_report(report, now=NOW)

    assert result.ok is False
    assert result.errors


def test_cli_reads_one_report_and_writes_versioned_acceptance(tmp_path) -> None:
    report_path = tmp_path / "runner.json"
    output_path = tmp_path / "acceptance.json"
    report_path.write_text(json.dumps(_report(lane="history")), encoding="utf-8")

    return_code = acceptance.main(
        ["--report", str(report_path), "--output", str(output_path)]
    )

    assert return_code == 0
    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "schema": "fotmob-catalog-acceptance-v1",
        "ok": True,
        "errors": [],
        "catalog_count": 2,
        "included_count": 1,
        "scope_count": 1,
    }
