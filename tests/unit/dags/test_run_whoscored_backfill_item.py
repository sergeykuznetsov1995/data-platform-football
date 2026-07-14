"""Exact candidate-freeze contracts for one immutable backfill item."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from dags.scripts.run_whoscored_backfill_item import (
    _filtered_candidates,
    _schedule_request_accounting,
    _source_request_attempts,
)
from dags.scripts.run_whoscored_scraper import RunnerScope


class _Repository:
    def __init__(self, ids):
        self.ids = ids
        self.calls = []

    def list_completed_match_candidates(self, league, season, *, match_ids=None):
        self.calls.append((league, season, match_ids))
        selected = (
            self.ids
            if match_ids is None
            else [value for value in self.ids if value in set(match_ids)]
        )
        return [
            SimpleNamespace(game_id=value, kickoff=datetime(2025, 8, 1))
            for value in selected
        ]


@pytest.mark.unit
def test_freeze_uses_all_completed_policy_even_for_prior_successes():
    repository = _Repository([3, 1, 2])
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("INT-World Cup=2026")

    frozen = _filtered_candidates(service, {"selector": {}}, scope)

    assert frozen == [1, 2, 3]
    assert repository.calls == [("INT-World Cup", "2026", None)]


@pytest.mark.unit
def test_explicit_game_ids_must_freeze_exactly_or_fail():
    repository = _Repository([1])
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("INT-World Cup=2026")

    with pytest.raises(RuntimeError, match="not completed matches"):
        _filtered_candidates(
            service,
            {"selector": {"game_ids": [1, 2]}},
            scope,
        )


@pytest.mark.unit
def test_schedule_accounting_uses_observed_multi_stage_cardinality_conservatively():
    item = {
        "kind": "schedule",
        "catalog_stage_ids": [23752],
        "estimated_request_units": 70,
    }
    source_stage_ids = list(range(23752, 23765))
    result = SimpleNamespace(
        metadata={
            "source_stage_ids": source_stage_ids,
            "source_stage_count": len(source_stage_ids),
        }
    )
    traffic = {
        "route_requests": {
            "raw_cache": 100,
            "direct_http": 700,
            "direct_flaresolverr": 100,
        },
        "failures": {"cloudflare": 7},
    }

    accounting = _schedule_request_accounting(item, result, traffic)

    assert _source_request_attempts(traffic) == 807
    assert accounting == {
        "source_stage_ids": source_stage_ids,
        "source_request_attempts": 807,
        "estimated_request_units": 70,
        "actual_request_units": 910,
    }
    retry_heavy = _schedule_request_accounting(
        item,
        result,
        {"route_requests": {"direct_http": 1000}, "failures": {}},
    )
    assert retry_heavy["actual_request_units"] == 1000


@pytest.mark.unit
def test_schedule_accounting_rejects_missing_stage_evidence():
    item = {
        "kind": "schedule",
        "catalog_stage_ids": [23752],
        "estimated_request_units": 70,
    }

    with pytest.raises(RuntimeError, match="source-stage metadata"):
        _schedule_request_accounting(
            item,
            SimpleNamespace(metadata={}),
            {"route_requests": {}, "failures": {}},
        )
