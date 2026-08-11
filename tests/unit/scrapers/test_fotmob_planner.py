from datetime import datetime, timedelta

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
    ScopeAttemptState,
    ScopeLane,
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


def test_backfill_uses_source_order_not_a_hardcoded_competition_allowlist():
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
        (999, "2025/2026"),
        (289, "2017/2018"),
        (289, "2017/2019"),
    ]
    assert all(item.reason != "mandatory_acceptance_sentinel" for item in plan)


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


def test_automatic_current_and_history_lanes_are_disjoint():
    seasons = [
        SeasonRef(47, "2025/2026", is_selected=True, source_order=0),
        SeasonRef(47, "2024/2025", source_order=1),
    ]
    current = plan_seasons(
        [_classified(47)], seasons, mode=RunMode.DAILY, lane=ScopeLane.CURRENT
    )
    history = plan_seasons(
        [_classified(47)], seasons, mode=RunMode.BACKFILL, lane=ScopeLane.HISTORY
    )

    assert [item.identity for item in current] == [(47, "2025/2026")]
    assert [item.identity for item in history] == [(47, "2024/2025")]


def test_retryable_scope_not_due_does_not_starve_later_ready_scopes():
    now = datetime(2026, 8, 8, 10)
    attempts = {
        (47, "2025/2026"): ScopeAttemptState(
            competition_id=47,
            source_season_key="2025/2026",
            plan_signature="fmplan1-test",
            attempt_count=1,
            last_attempt_at=now - timedelta(minutes=5),
            next_retry_at=now + timedelta(minutes=10),
            outcome="retryable",
            reason="HTTP 503",
        )
    }
    plan = plan_seasons(
        [_classified(47), _classified(48), _classified(49)],
        [
            SeasonRef(47, "2025/2026", is_selected=True),
            SeasonRef(48, "2025/2026", is_selected=True),
            SeasonRef(49, "2025/2026", is_selected=True),
        ],
        mode=RunMode.DAILY,
        lane=ScopeLane.CURRENT,
        attempt_states=attempts,
        now=now,
    )

    assert [item.competition_id for item in plan] == [48, 49]


def _terminal_attempt(now, age):
    return {
        (47, "2025/2026"): ScopeAttemptState(
            competition_id=47,
            source_season_key="2025/2026",
            plan_signature="fmplan1-test",
            attempt_count=1,
            last_attempt_at=now - age,
            next_retry_at=None,
            outcome="terminal",
            reason="scope completion commit failed: TrinoUserError",
        )
    }


def test_fresh_terminal_scope_stays_out_of_the_plan():
    now = datetime(2026, 8, 8, 10)
    plan = plan_seasons(
        [_classified(47), _classified(48)],
        [
            SeasonRef(47, "2025/2026", is_selected=True),
            SeasonRef(48, "2025/2026", is_selected=True),
        ],
        mode=RunMode.DAILY,
        lane=ScopeLane.CURRENT,
        attempt_states=_terminal_attempt(now, timedelta(hours=23)),
        now=now,
    )

    assert [item.competition_id for item in plan] == [48]


def test_terminal_scope_returns_to_the_plan_after_ttl():
    """Разовая ошибка коммита не должна вычёркивать скоуп из обхода навсегда.

    Под стабильной журнальной подписью карта состояний переживает раны, поэтому
    вечное исключение превратилось бы в тихую дыру в покрытии: ран зелёный,
    скоуп не собирается больше никогда и об этом никто не узнает.
    """

    now = datetime(2026, 8, 8, 10)
    plan = plan_seasons(
        [_classified(47), _classified(48)],
        [
            SeasonRef(47, "2025/2026", is_selected=True),
            SeasonRef(48, "2025/2026", is_selected=True),
        ],
        mode=RunMode.DAILY,
        lane=ScopeLane.CURRENT,
        attempt_states=_terminal_attempt(now, timedelta(hours=25)),
        now=now,
    )

    # 48 впереди по справедливости обхода: его не пробовали ни разу
    # (last_attempt = datetime.min), а 47 пробовали сутки назад.
    assert [item.competition_id for item in plan] == [48, 47]


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
