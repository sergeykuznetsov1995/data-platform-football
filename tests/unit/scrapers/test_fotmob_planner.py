import pytest

from scrapers.fotmob.domain import (
    CompetitionRef,
    ScopeClassification,
    ScopeDecision,
    SeasonRef,
)
from scrapers.fotmob.planner import (
    BudgetExceeded,
    BudgetLedger,
    RunMode,
    TransportBudget,
    deterministic_plan_signature,
    plan_seasons,
    tombstones_after_two_absences,
)


def _classified(competition_id, decision=ScopeDecision.INCLUDED):
    return ScopeClassification(
        CompetitionRef(competition_id, f"Competition {competition_id}"),
        decision,
        "test",
        "test_rule",
    )


def test_backfill_prioritizes_required_competitions_and_preserves_exact_seasons():
    plan = plan_seasons(
        [_classified(289), _classified(999)],
        [
            SeasonRef(999, "2025/2026", is_latest=True, source_order=0),
            SeasonRef(289, "2017/2019", source_order=2),
            SeasonRef(289, "2017/2018", source_order=1),
        ],
        mode=RunMode.BACKFILL,
    )
    assert [item.identity for item in plan] == [
        (289, "2017/2018"),
        (289, "2017/2019"),
        (999, "2025/2026"),
    ]


def test_daily_only_plans_source_selected_or_latest_seasons():
    plan = plan_seasons(
        [_classified(47)],
        [
            SeasonRef(47, "2025/2026", is_selected=True, source_order=0),
            SeasonRef(47, "2024/2025", source_order=1),
        ],
        mode=RunMode.DAILY,
    )
    assert [item.source_season_key for item in plan] == ["2025/2026"]


def test_excluded_and_review_required_competitions_stay_out_of_ingest_plan():
    plan = plan_seasons(
        [
            _classified(1, ScopeDecision.EXCLUDED),
            _classified(2, ScopeDecision.REVIEW_REQUIRED),
        ],
        [SeasonRef(1, "2025"), SeasonRef(2, "2025")],
        mode=RunMode.BACKFILL,
    )
    assert plan == []


def test_backfill_skips_only_the_exact_successful_source_scope():
    plan = plan_seasons(
        [_classified(289)],
        [SeasonRef(289, "2017/2019"), SeasonRef(289, "2017/2018")],
        mode=RunMode.BACKFILL,
        previously_successful={(289, "2017/2019")},
    )
    assert [item.identity for item in plan] == [(289, "2017/2018")]


def test_tombstone_requires_two_consecutive_complete_absences():
    # 42 disappeared only now; 47 has been absent twice.
    assert tombstones_after_two_absences(
        previous_snapshot_ids={42},
        snapshot_before_previous_ids={42, 47, 63},
        current_snapshot_ids={63},
    ) == {47}


def test_budget_enforces_proxy_zero_and_request_ceiling():
    ledger = BudgetLedger(
        TransportBudget(max_requests=1, max_direct_bytes=100, max_proxy_bytes=0)
    )
    ledger.reserve_request()
    ledger.account_bytes(direct_bytes=100, proxy_bytes=0)
    with pytest.raises(BudgetExceeded, match="request budget"):
        ledger.reserve_request()
    with pytest.raises(BudgetExceeded, match="proxy-byte invariant"):
        ledger.account_bytes(direct_bytes=0, proxy_bytes=1)


def test_plan_signature_is_order_independent_and_policy_sensitive():
    first = deterministic_plan_signature(
        ["players", "matches", "players"],
        {"include_unfinished": False},
    )
    second = deterministic_plan_signature(
        ["MATCHES", "players"],
        {"include_unfinished": False},
    )

    assert first == second
    assert first.startswith("fmplan1-")
    assert first != deterministic_plan_signature(
        ["matches", "players"],
        {"include_unfinished": True},
    )
    with pytest.raises(ValueError, match="at least one"):
        deterministic_plan_signature([])
