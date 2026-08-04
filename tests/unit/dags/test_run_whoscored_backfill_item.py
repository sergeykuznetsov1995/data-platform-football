"""Exact candidate-freeze contracts for one immutable backfill item."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from dags.scripts.run_whoscored_backfill_item import (
    _filtered_candidates,
    _frozen_profile_ids,
    _parser,
    _probe_outcome,
    _schedule_request_accounting,
    _source_request_attempts,
    _validate_transport_args,
    main,
)
from dags.scripts.run_whoscored_scraper import RunnerScope


class _Repository:
    def __init__(self, ids, non_opta=(), ingest_states=None):
        self.ids = ids
        self.non_opta = set(non_opta)
        self.ingest_states = dict(ingest_states or {})
        self.calls = []
        self.state_calls = []

    def list_completed_match_candidates(self, league, season, *, match_ids=None):
        self.calls.append((league, season, match_ids))
        selected = (
            self.ids
            if match_ids is None
            else [value for value in self.ids if value in set(match_ids)]
        )
        return [
            SimpleNamespace(
                game_id=value,
                kickoff=datetime(2025, 8, 1),
                match_is_opta=value not in self.non_opta,
            )
            for value in selected
        ]

    def list_match_ingest_states(self, league, season, *, match_ids):
        self.state_calls.append((league, season, tuple(match_ids)))
        return {
            game_id: self.ingest_states[game_id]
            for game_id in match_ids
            if game_id in self.ingest_states
        }


@pytest.mark.unit
def test_freeze_uses_all_completed_policy_even_for_prior_successes():
    repository = _Repository([3, 1, 2])
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("INT-World Cup=2026")

    frozen, non_opta = _filtered_candidates(service, {"selector": {}}, scope)

    assert frozen == [1, 2, 3]
    assert non_opta == []
    assert repository.calls == [("INT-World Cup", "2026", None)]


@pytest.mark.unit
def test_freeze_reports_non_opta_flags_without_filtering_any_candidate():
    repository = _Repository([3, 1, 2], non_opta=[2, 3])
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("INT-World Cup=2026")

    frozen, non_opta = _filtered_candidates(service, {"selector": {}}, scope)

    assert frozen == [1, 2, 3]
    assert non_opta == [2, 3]


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


def _probe_item(game_ids, candidate_game_ids):
    return {"game_ids": game_ids, "candidate_game_ids": candidate_game_ids}


@pytest.mark.unit
def test_probe_outcome_defers_every_remaining_candidate_on_stub_verdicts():
    candidates = list(range(1, 27))
    repository = _Repository(
        candidates,
        ingest_states={1: "not_available", 26: "not_available"},
    )
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("WS-252-26=2526")

    outcome = _probe_outcome(service, scope, _probe_item([1, 26], candidates))

    assert outcome == {
        "game_ids": [1, 26],
        "not_available_game_ids": [1, 26],
        "known_data_game_ids": [],
        "deferred_game_ids": list(range(2, 26)),
    }
    # No candidate is ever lost: probes and deferral partition the freeze.
    assert sorted(
        outcome["game_ids"] + outcome["deferred_game_ids"]
    ) == candidates
    # The manifest is consulted for the whole freeze, not only the probes.
    assert repository.state_calls == [("WS-252-26", "2526", tuple(candidates))]


@pytest.mark.unit
def test_probe_outcome_defers_nothing_when_the_flag_lied():
    candidates = list(range(1, 27))
    repository = _Repository(
        candidates,
        ingest_states={1: "success", 26: "not_available"},
    )
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("WS-252-26=2526")

    outcome = _probe_outcome(service, scope, _probe_item([1, 26], candidates))

    assert outcome == {
        "game_ids": [1, 26],
        "not_available_game_ids": [26],
        "known_data_game_ids": [],
        "deferred_game_ids": [],
    }


@pytest.mark.unit
def test_probe_outcome_treats_prior_manifest_success_as_deferral_veto():
    # Stub verdicts on both probes are outweighed by a success row another
    # candidate already earned: real data exists, defer nothing.
    candidates = list(range(1, 27))
    repository = _Repository(
        candidates,
        ingest_states={
            1: "not_available",
            26: "not_available",
            7: "success",
        },
    )
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("WS-252-26=2526")

    outcome = _probe_outcome(service, scope, _probe_item([1, 26], candidates))

    assert outcome["not_available_game_ids"] == [1, 26]
    assert outcome["known_data_game_ids"] == [7]
    assert outcome["deferred_game_ids"] == []


@pytest.mark.unit
def test_probe_outcome_ignores_parse_failed_rows_as_data_evidence():
    # parse_failed is written for any CONTENT failure without the two-marker
    # not-available proof, including shell pages with no data at all
    # (WS-100-129: 79 such rows) — it must not veto the deferral, and
    # retryable/terminal rows prove nothing either.
    candidates = list(range(1, 27))
    repository = _Repository(
        candidates,
        ingest_states={
            1: "not_available",
            26: "not_available",
            9: "parse_failed",
            11: "retryable",
            13: "terminal",
        },
    )
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("WS-252-26=2526")

    outcome = _probe_outcome(service, scope, _probe_item([1, 26], candidates))

    assert outcome["not_available_game_ids"] == [1, 26]
    assert outcome["known_data_game_ids"] == []
    assert outcome["deferred_game_ids"] == list(range(2, 26))


@pytest.mark.unit
def test_probe_outcome_requires_a_manifest_verdict_for_every_probe():
    repository = _Repository([1, 2, 3], ingest_states={1: "not_available"})
    service = SimpleNamespace(repository=repository)
    scope = RunnerScope.parse("WS-252-26=2526")

    with pytest.raises(RuntimeError, match="no ingest manifest verdict"):
        _probe_outcome(service, scope, _probe_item([1, 3], [1, 2, 3]))


class _RosterRepository:
    def __init__(self, player_ids):
        self.player_ids = player_ids

    def list_roster_player_ids(self, *, scopes):
        assert len(scopes) == 1
        return list(self.player_ids)


def _roster_service(player_ids):
    return SimpleNamespace(
        repository=_RosterRepository(player_ids),
        scope=SimpleNamespace(spec="FRA-Ligue 1=2526"),
    )


@pytest.mark.unit
def test_roster_freeze_drops_placeholder_ids_instead_of_failing_the_scope():
    frozen = _frozen_profile_ids(_roster_service([0, 11, 7, 7]))

    assert frozen == [7, 11]


@pytest.mark.unit
def test_roster_freeze_still_fails_when_nothing_but_placeholders_remains():
    with pytest.raises(RuntimeError, match="only placeholder player_ids"):
        _frozen_profile_ids(_roster_service([0, -3]))


@pytest.mark.unit
def test_roster_freeze_keeps_an_empty_roster_empty():
    assert _frozen_profile_ids(_roster_service([])) == []


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


@pytest.mark.unit
def test_backfill_worker_rejects_invalid_plan_before_runtime_projection(
    monkeypatch, tmp_path
):
    from scrapers.whoscored import runtime_contract

    observed = []

    def validate(**kwargs):
        observed.append(kwargs)

    monkeypatch.setattr(runtime_contract, "validate_runtime_contract", validate)
    output = tmp_path / "worker.json"

    rc = main(
        [
            "--queue-id",
            "queue",
            "--plan-id",
            "a" * 64,
            "--work-item",
            "not-valid-base64",
            "--output",
            str(output),
        ]
    )

    assert rc == 1
    assert observed == []


@pytest.mark.unit
def test_backfill_worker_binds_paid_selector_to_decoded_work_item():
    parser = _parser()
    common = [
        "--queue-id",
        "queue",
        "--plan-id",
        "a" * 64,
        "--work-item",
        "encoded",
        "--output",
        "/tmp/result.json",
        "--transport-policy",
        "direct_then_paid",
        "--proxy-approval-path",
        "/run/approval.json",
        "--proxy-approval-id",
        "approval-1",
        "--proxy-approval-sha256",
        "b" * 64,
    ]
    args = parser.parse_args(common + ["--proxy-work-item-id", "wrong-item"])
    with pytest.raises(SystemExit):
        _validate_transport_args(parser, args, work_item_id="immutable-item")

    args = parser.parse_args(
        common + ["--proxy-allocation-id", "attacker-allocation"]
    )
    with pytest.raises(SystemExit):
        _validate_transport_args(parser, args, work_item_id="immutable-item")

    args = parser.parse_args(common)
    _validate_transport_args(parser, args, work_item_id="immutable-item")
    assert args.proxy_work_item_id == "immutable-item"
    assert args.expected_proxy_work_item_id == "immutable-item"
